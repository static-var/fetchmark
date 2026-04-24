package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/staticvar/fetchmark/internal/obs"
)

// Metrics records the HTTP request duration histogram in obs. It must be
// registered after chi has populated its RouteContext so RoutePattern()
// returns the matched template (keeping label cardinality bounded);
// registering it as the last middleware achieves this.
func Metrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &metricsRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		obs.HTTPRequestDuration.
			WithLabelValues(r.Method, route, strconv.Itoa(rec.status/100)+"xx").
			Observe(time.Since(start).Seconds())
	})
}

type metricsRecorder struct {
	http.ResponseWriter
	status int
}

func (m *metricsRecorder) WriteHeader(code int) {
	m.status = code
	m.ResponseWriter.WriteHeader(code)
}
