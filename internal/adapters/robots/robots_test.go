package robots

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestChecker_AllowDisallow(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		if got := r.Header.Get("User-Agent"); got != "Fetchmark" {
			t.Errorf("robots User-Agent = %q", got)
		}
		_, _ = w.Write([]byte("User-agent: Fetchmark\nDisallow: /private\n"))
	}))
	t.Cleanup(srv.Close)

	c := New(srv.Client(), time.Hour, 0)
	ok, err := c.Allowed(context.Background(), "Fetchmark", srv.URL+"/public/page")
	if err != nil || !ok {
		t.Fatalf("public allowed err=%v ok=%v", err, ok)
	}
	ok, _ = c.Allowed(context.Background(), "Fetchmark", srv.URL+"/private/page")
	if ok {
		t.Fatal("private should be disallowed")
	}
	// Second check must hit cache (still 1 upstream request).
	_, _ = c.Allowed(context.Background(), "Fetchmark", srv.URL+"/another")
	if n := atomic.LoadInt64(&hits); n != 1 {
		t.Fatalf("expected 1 upstream hit, got %d", n)
	}
}

func TestEvaluatePolicyUsesExactProductTokenAndPathQuery(t *testing.T) {
	body := []byte("User-agent: Fetchmark\nDisallow: /search?private=\n\nUser-agent: Fetch\nDisallow: /\n")
	allowed, err := EvaluatePolicy(body, "Fetchmark/1 (+mailto:operator@example.org)", "https://example.org/search?public=1")
	if err != nil || !allowed {
		t.Fatalf("public query allowed=%v error=%v", allowed, err)
	}
	allowed, err = EvaluatePolicy(body, "Fetchmark/1 (+mailto:operator@example.org)", "https://example.org/search?private=1")
	if err != nil || allowed {
		t.Fatalf("private query allowed=%v error=%v", allowed, err)
	}
	for name, body := range map[string][]byte{
		"oversized": make([]byte, MaxEvidencePolicyBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := EvaluatePolicy(body, "Fetchmark/1", "https://example.org/"); err == nil {
				t.Fatal("invalid policy input accepted")
			}
		})
	}
	if _, err := EvaluatePolicy(nil, "", "https://example.org/"); err == nil {
		t.Fatal("empty product token accepted")
	}
	if decision := New(http.DefaultClient, time.Hour, 512<<10).Evaluate(context.Background(), "", "https://example.org/"); decision.Err == nil || decision.Allowed {
		t.Fatalf("checker empty product token decision = %#v", decision)
	}
	if _, err := EvaluatePolicy(nil, "Fetchmark/1", "relative"); err == nil {
		t.Fatal("relative URL accepted")
	}
}

func TestEvaluatePolicyRFC9309BOMEncodingGroupsAndAllowTie(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		url       string
		wantAllow bool
	}{
		{
			name: "BOM does not create prefix agent match",
			body: "\ufeffUser-agent: Fetch\nAllow: /\n\nUser-agent: *\nDisallow: /private\n",
			url:  "https://example.org/private", wantAllow: false,
		},
		{
			name: "percent encoded unreserved octet is decoded",
			body: "User-agent: *\nDisallow: /private/~user\n",
			url:  "https://example.org/private/%7euser", wantAllow: false,
		},
		{
			name: "equal specificity allow wins",
			body: "User-agent: Fetchmark\nDisallow: /same\nAllow: /same\n",
			url:  "https://example.org/same", wantAllow: true,
		},
		{
			name: "equal specificity allow wins in reverse order",
			body: "User-agent: Fetchmark\nAllow: /same\nDisallow: /same\n",
			url:  "https://example.org/same", wantAllow: true,
		},
		{
			name: "matching groups are combined",
			body: "User-agent: Fetchmark\nDisallow: /one\n\nUser-agent: Fetchmark\nDisallow: /two\n",
			url:  "https://example.org/two", wantAllow: false,
		},
		{
			name: "exact group suppresses wildcard",
			body: "User-agent: *\nDisallow: /\n\nUser-agent: Fetchmark\nAllow: /public\n",
			url:  "https://example.org/other", wantAllow: true,
		},
		{
			name: "wildcard and end anchor",
			body: "User-agent: *\nDisallow: /private/*/exact$\n",
			url:  "https://example.org/private/one/exact", wantAllow: false,
		},
		{
			name: "end anchor excludes suffix",
			body: "User-agent: *\nDisallow: /private/exact$\n",
			url:  "https://example.org/private/exact/suffix", wantAllow: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			allowed, err := EvaluatePolicy([]byte(test.body), "Fetchmark/1", test.url)
			if err != nil || allowed != test.wantAllow {
				t.Fatalf("allowed=%v want=%v error=%v", allowed, test.wantAllow, err)
			}
		})
	}
	if _, err := EvaluatePolicy([]byte{0xff}, "Fetchmark/1", "https://example.org/"); err == nil {
		t.Fatal("invalid UTF-8 policy accepted")
	}
}

func TestEvaluatePolicyResourceLimitsFailClosed(t *testing.T) {
	longTarget := "https://example.org/" + strings.Repeat("a", maxPolicyRequestTargetBytes)
	allowed, err := EvaluatePolicy(nil, "Fetchmark/1", longTarget)
	if err != nil || allowed {
		t.Fatalf("oversized request target allowed=%v error=%v", allowed, err)
	}

	var oversizedPolicy strings.Builder
	oversizedPolicy.WriteString("User-agent: Fetchmark\n")
	for index := 0; index <= maxPolicyRules; index++ {
		fmt.Fprintf(&oversizedPolicy, "Disallow: /%05d\n", index)
	}
	if oversizedPolicy.Len() > MaxEvidencePolicyBytes {
		t.Fatalf("rule-limit fixture exceeds policy byte limit: %d", oversizedPolicy.Len())
	}
	if _, err := EvaluatePolicy([]byte(oversizedPolicy.String()), "Fetchmark/1", "https://example.org/"); err == nil || !strings.Contains(err.Error(), "rule limit") {
		t.Fatalf("rule-limit error = %v", err)
	}

	target := "/" + strings.Repeat("a", maxPolicyRequestTargetBytes-1)
	rules := make([]policyRule, 0, 256)
	for index := 0; index < cap(rules); index++ {
		pattern := fmt.Sprintf("/*never-%03d", index)
		rules = append(rules, policyRule{pattern: pattern, specificity: policySpecificity(pattern)})
	}
	resourceIntensive := &policy{groups: []policyGroup{{agents: []string{"fetchmark"}, rules: rules}}}
	if resourceIntensive.allowed("fetchmark", target) {
		t.Fatal("matcher allowed after exhausting its explicit work budget")
	}
}

func TestChecker_UserAgentGroupsMatchExactProductToken(t *testing.T) {
	const userAgent = "Fetchmark/0.1 (+https://github.com/staticvar/fetchmark)"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != userAgent {
			t.Errorf("robots User-Agent = %q, want %q", got, userAgent)
		}
		_, _ = w.Write([]byte("User-agent: Fetch\nDisallow: /\n\nUser-agent: *\nDisallow: /w/\n"))
	}))
	t.Cleanup(srv.Close)

	c := New(srv.Client(), time.Hour, 0)
	decision := c.Evaluate(context.Background(), userAgent, srv.URL+"/wiki/Article")
	if decision.Err != nil || !decision.Allowed || !decision.Authoritative {
		t.Fatalf("Fetchmark must not inherit the distinct Fetch product-token group: %+v", decision)
	}
}

func TestChecker_4xxAllowsAll(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	c := New(srv.Client(), time.Hour, 0)
	ok, err := c.Allowed(context.Background(), "Fetchmark", srv.URL+"/anywhere")
	if err != nil || !ok {
		t.Fatalf("4xx should allow all, err=%v ok=%v", err, ok)
	}
}

func TestChecker_CoalescesConcurrentColdFetchesPerOrigin(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte("User-agent: Fetchmark\nDisallow: /private\n"))
	}))
	t.Cleanup(srv.Close)

	c := New(srv.Client(), time.Hour, 0)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := c.Allowed(context.Background(), "Fetchmark", srv.URL+"/public")
			if err != nil || !ok {
				t.Errorf("Allowed err=%v ok=%v", err, ok)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt64(&hits); got != 1 {
		t.Fatalf("robots fetches = %d, want 1", got)
	}
}

func TestChecker_FetchReportsOversizedRobots(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("User-agent: Fetchmark\nDisallow: /private\n" + strings.Repeat("x", 64)))
	}))
	t.Cleanup(srv.Close)

	c := New(srv.Client(), time.Hour, 16)
	data, err := c.fetch(context.Background(), srv.URL, "Fetchmark")
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("err = %v, want too-large error", err)
	}
	if data != nil {
		t.Fatalf("oversized robots data should not be parsed: %+v", data)
	}
}

func TestChecker_FetchFailureFailsClosedAndIsNotCached(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			http.Error(w, "temporary", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("User-agent: Fetchmark\nDisallow: /private\n"))
	}))
	t.Cleanup(srv.Close)
	c := New(srv.Client(), time.Hour, 0)

	if ok, err := c.Allowed(context.Background(), "Fetchmark", srv.URL+"/private"); ok || err == nil {
		t.Fatalf("first transient failure should fail closed, ok=%v err=%v", ok, err)
	}
	if ok, _ := c.Allowed(context.Background(), "Fetchmark", srv.URL+"/private"); ok {
		t.Fatal("second request should retry robots and observe disallow")
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("robots hits = %d, want 2", got)
	}
}

func TestChecker_UnreachableFailsClosed(t *testing.T) {
	c := New(&http.Client{Timeout: 100 * time.Millisecond}, time.Hour, 0)
	// Port 1 is reserved; connect refuses fast.
	ok, err := c.Allowed(context.Background(), "Fetchmark", "http://127.0.0.1:1/page")
	if err == nil || ok {
		t.Fatalf("unreachable robots.txt should fail closed, err=%v ok=%v", err, ok)
	}
	if !c.IsDisallowed(context.Background(), "Fetchmark", "http://127.0.0.1:1/page") {
		t.Fatal("IsDisallowed failed open on unreachable robots.txt")
	}
}

func TestChecker_EvaluatePreservesRobotsUncertaintyForRetention(t *testing.T) {
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("User-agent: Fetchmark\nDisallow: /private\n"))
	}))
	t.Cleanup(srv.Close)
	c := New(srv.Client(), time.Hour, 0)

	decision := c.Evaluate(context.Background(), "Fetchmark", srv.URL+"/private")
	if decision.Allowed || !decision.Authoritative || decision.Err == nil {
		t.Fatalf("transient decision = %+v", decision)
	}
	decision = c.Evaluate(context.Background(), "Fetchmark", srv.URL+"/private")
	if decision.Allowed || !decision.Authoritative || decision.Err != nil {
		t.Fatalf("authoritative decision = %+v", decision)
	}
}

func TestChecker_SweepExpiredRemovesOldEntries(t *testing.T) {
	c := New(http.DefaultClient, time.Minute, 0)
	now := time.Now()
	c.cache["http://expired.example"] = entry{fetched: now.Add(-2 * time.Minute)}
	c.cache["http://fresh.example"] = entry{fetched: now.Add(-30 * time.Second)}

	c.sweepExpired(now)

	if _, ok := c.cache["http://expired.example"]; ok {
		t.Fatal("expired cache entry was not removed")
	}
	if _, ok := c.cache["http://fresh.example"]; !ok {
		t.Fatal("fresh cache entry was removed")
	}
}
