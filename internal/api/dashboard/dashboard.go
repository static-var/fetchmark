// Package dashboard serves a small, read-only operator UI backed by the
// Prometheus metrics the rest of Fetchmark already exposes. Design
// priorities (in order): no JS build step, no cookie machinery, no
// information leakage.
//
// Layout choices:
//   - Server-rendered html/template. We deliberately avoid templ's
//     codegen step so the repo builds with plain `go build` — switching
//     to templ later is a cosmetic change.
//   - HTMX from CDN polls /dashboard/partials/* every few seconds.
//   - Basic Auth credentials are required to mount the dashboard at
//     all; a handler is never registered when unset (defense in depth).
package dashboard

import (
	"context"
	"crypto/subtle"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Deps bundles what the dashboard needs. Gatherer is usually
// prometheus.DefaultGatherer; SearxngURL is shown on the header so
// operators can sanity-check which backend they're talking to.
type Deps struct {
	Gatherer   prometheus.Gatherer
	SearxngURL string
	RedisURL   string
	Version    string
}

// Mount attaches the dashboard to the provided chi-compatible mux. User
// and password must both be non-empty; when either is missing the
// dashboard is not mounted at all so there is no unauthenticated
// surface to probe.
func Mount(mux interface {
	Get(pattern string, h http.HandlerFunc)
	Handle(pattern string, h http.Handler)
}, user, password string, d Deps) {
	if user == "" || password == "" {
		return
	}
	auth := basicAuth(user, password)

	mux.Handle("/dashboard", auth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		renderIndex(w, d)
	})))
	mux.Handle("/dashboard/", auth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		renderIndex(w, d)
	})))
	mux.Handle("/dashboard/partials/counters", auth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		renderCounters(w, d)
	})))
	mux.Handle("/dashboard/partials/engines", auth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		renderEngines(w, d)
	})))
}

func basicAuth(wantUser, wantPass string) func(http.Handler) http.Handler {
	wu := []byte(wantUser)
	wp := []byte(wantPass)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			u, p, ok := r.BasicAuth()
			if !ok ||
				subtle.ConstantTimeCompare([]byte(u), wu) != 1 ||
				subtle.ConstantTimeCompare([]byte(p), wp) != 1 {
				w.Header().Set("WWW-Authenticate", `Basic realm="fetchmark"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

var indexTmpl = template.Must(template.New("index").Parse(`<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Fetchmark — Ops</title>
<script src="https://unpkg.com/htmx.org@1.9.12"></script>
<style>
 body { font: 14px/1.4 -apple-system, system-ui, sans-serif; max-width: 960px; margin: 24px auto; padding: 0 16px; color:#1a1a1a; }
 h1   { font-size: 20px; margin: 0 0 4px; }
 h2   { font-size: 14px; margin: 24px 0 8px; color:#555; text-transform:uppercase; letter-spacing: .06em; }
 .hdr { display:flex; justify-content: space-between; align-items:baseline; margin-bottom: 16px; }
 .k   { color:#666; }
 table{ border-collapse: collapse; width:100%; }
 td,th{ border-bottom:1px solid #eee; padding:6px 8px; text-align:left; }
 .ok    { color:#107c10; }
 .warn  { color:#b75a00; }
 .err   { color:#c50f1f; }
</style>
</head>
<body>
  <div class="hdr">
    <div>
      <h1>Fetchmark</h1>
      <div class="k">version {{.Version}} · searxng: <code>{{.SearxngURL}}</code> · redis: <code>{{.RedisURL}}</code></div>
    </div>
  </div>

  <h2>Live counters</h2>
  <div id="counters" hx-get="/dashboard/partials/counters" hx-trigger="load, every 5s">loading…</div>

  <h2>SearXNG engine health</h2>
  <div id="engines" hx-get="/dashboard/partials/engines" hx-trigger="load, every 10s">loading…</div>

  <h2>Scrape window</h2>
  <div class="k">Counters are cumulative since process start; divide deltas across two polls to derive a rate.</div>
</body>
</html>`))

func renderIndex(w http.ResponseWriter, d Deps) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = indexTmpl.Execute(w, d)
}

// renderCounters walks the live Prometheus registry and picks out the
// fetchmark_* counters/histograms the operator cares about. Writing raw
// html (not a template per cell) keeps this ~40 LOC and avoids a second
// template file.
func renderCounters(w http.ResponseWriter, d Deps) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	mfs, err := d.Gatherer.Gather()
	if err != nil {
		fmt.Fprintf(w, `<p class="err">gather failed: %s</p>`, template.HTMLEscapeString(err.Error()))
		return
	}
	rows := []counterRow{}
	for _, mf := range mfs {
		name := mf.GetName()
		if !strings.HasPrefix(name, "fetchmark_") {
			continue
		}
		for _, m := range mf.GetMetric() {
			label := labelSetString(m.GetLabel())
			val := numericValue(mf.GetType(), m)
			if val < 0 {
				continue
			}
			rows = append(rows, counterRow{Name: name, Labels: label, Value: val})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Name == rows[j].Name {
			return rows[i].Labels < rows[j].Labels
		}
		return rows[i].Name < rows[j].Name
	})
	fmt.Fprint(w, `<table><thead><tr><th>metric</th><th>labels</th><th>value</th></tr></thead><tbody>`)
	for _, r := range rows {
		fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td>%.0f</td></tr>`,
			template.HTMLEscapeString(r.Name),
			template.HTMLEscapeString(r.Labels),
			r.Value)
	}
	fmt.Fprint(w, `</tbody></table>`)
	fmt.Fprintf(w, `<p class="k">last update %s</p>`, time.Now().Format(time.RFC3339))
}

// renderEngines extracts just the SearXNG engine gauge so operators can
// spot unhealthy engines without wading through the full metrics dump.
func renderEngines(w http.ResponseWriter, d Deps) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	mfs, err := d.Gatherer.Gather()
	if err != nil {
		fmt.Fprintf(w, `<p class="err">gather failed: %s</p>`, template.HTMLEscapeString(err.Error()))
		return
	}
	type eng struct {
		Name      string
		Unhealthy bool
	}
	var engines []eng
	for _, mf := range mfs {
		if mf.GetName() != "fetchmark_searxng_engine_unresponsive" {
			continue
		}
		for _, m := range mf.GetMetric() {
			name := ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "engine" {
					name = l.GetValue()
				}
			}
			engines = append(engines, eng{Name: name, Unhealthy: m.GetGauge().GetValue() > 0})
		}
	}
	if len(engines) == 0 {
		fmt.Fprint(w, `<p class="k">no engine data yet — run a query first</p>`)
		return
	}
	sort.Slice(engines, func(i, j int) bool { return engines[i].Name < engines[j].Name })
	fmt.Fprint(w, `<table><thead><tr><th>engine</th><th>status</th></tr></thead><tbody>`)
	for _, e := range engines {
		cls, txt := "ok", "healthy"
		if e.Unhealthy {
			cls, txt = "err", "unresponsive"
		}
		fmt.Fprintf(w, `<tr><td>%s</td><td class="%s">%s</td></tr>`,
			template.HTMLEscapeString(e.Name), cls, txt)
	}
	fmt.Fprint(w, `</tbody></table>`)
}

type counterRow struct {
	Name   string
	Labels string
	Value  float64
}

func labelSetString(labels []*dto.LabelPair) string {
	if len(labels) == 0 {
		return "—"
	}
	parts := make([]string, 0, len(labels))
	for _, l := range labels {
		parts = append(parts, l.GetName()+"="+l.GetValue())
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func numericValue(kind dto.MetricType, m *dto.Metric) float64 {
	switch kind {
	case dto.MetricType_COUNTER:
		return m.GetCounter().GetValue()
	case dto.MetricType_GAUGE:
		return m.GetGauge().GetValue()
	case dto.MetricType_HISTOGRAM:
		return float64(m.GetHistogram().GetSampleCount())
	case dto.MetricType_SUMMARY:
		return float64(m.GetSummary().GetSampleCount())
	default:
		return -1
	}
}

// compile-time: make sure we never ship with dashboard imports going
// stale against the metrics package.
var _ context.Context = context.TODO()
