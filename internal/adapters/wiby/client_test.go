package wiby

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestClientSearchBatchMapsOfficialResultsAndAttribution(t *testing.T) {
	var request *http.Request
	client := mustNew(t, testOptions(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		request = req.Clone(req.Context())
		return jsonResponse(http.StatusOK, `[
			{"URL":"https://example.com/one","Title":" First result ","Snippet":" useful  snippet ","Description":"long description"},
			{"URL":"http://example.org/two","Title":"Second result","Snippet":"","Description":"description fallback"}
		]`), nil
	})))

	batch, err := client.SearchBatch(context.Background(), search.Query{Q: " open  search ", MaxResults: 50})
	if err != nil {
		t.Fatal(err)
	}
	if request == nil || request.Method != http.MethodGet || request.URL.String() != "https://wiby.me/json/?p=0&q=open+search" {
		t.Fatalf("request = %v", request)
	}
	if request.Header.Get("Accept") != "application/json" || !strings.Contains(request.Header.Get("User-Agent"), "FetchmarkBot") {
		t.Fatalf("headers = %v", request.Header)
	}
	if batch.Provider != "wiby" || batch.Instance != "wiby.me" || batch.Status != search.BatchHealthy || len(batch.Hits) != 2 {
		t.Fatalf("batch = %+v", batch)
	}
	first := batch.Hits[0]
	if first.Title != "First result" || first.Snippet != "useful snippet" || first.Metadata["source"] != "Wiby" || first.Metadata["attribution_url"] != "https://wiby.me/" || first.Metadata["wiby_description"] != "long description" {
		t.Fatalf("first hit = %+v", first)
	}
	second := batch.Hits[1]
	if second.Snippet != "description fallback" || len(second.Engines) != 1 || second.Engines[0] != "wiby" {
		t.Fatalf("second hit = %+v", second)
	}
}

func TestClientCapsResultsAtTwelve(t *testing.T) {
	var body strings.Builder
	body.WriteByte('[')
	for index := 0; index < 13; index++ {
		if index > 0 {
			body.WriteByte(',')
		}
		body.WriteString(`{"URL":"https://example.com/`)
		body.WriteString(string(rune('a' + index)))
		body.WriteString(`","Title":"result"}`)
	}
	body.WriteByte(']')
	options := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, body.String()), nil
	}))
	options.MaxResults = 100
	client := mustNew(t, options)

	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "many", MaxResults: 100})
	if err != nil || len(batch.Hits) != 12 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestClientClassifiesAuthoritativeEmptyAndPartialMalformedRows(t *testing.T) {
	emptyClient := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `[]`), nil
	})))
	empty, err := emptyClient.SearchBatch(context.Background(), search.Query{Q: "none"})
	if err != nil || empty.Status != search.BatchAuthoritativeEmpty || len(empty.Diagnostics) != 0 {
		t.Fatalf("empty batch=%+v err=%v", empty, err)
	}

	partialClient := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `[
			{"URL":"javascript:alert(1)","Title":"bad"},
			{"URL":"https://example.com/good","Title":"good"}
		]`), nil
	})))
	partial, err := partialClient.SearchBatch(context.Background(), search.Query{Q: "mixed"})
	if err != nil || partial.Status != search.BatchPartial || len(partial.Hits) != 1 || len(partial.Diagnostics) != 1 || partial.Diagnostics[0].Reason != "malformed_results" {
		t.Fatalf("partial batch=%+v err=%v", partial, err)
	}
}

func TestClientRejectsNullAndAllInvalidPayloadsAsFailed(t *testing.T) {
	for _, body := range []string{`null`, `[{"URL":"javascript:alert(1)","Title":"bad"}]`} {
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, body), nil
		})))
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "invalid"})
		if err == nil || batch.Status != search.BatchFailed || len(batch.Diagnostics) != 1 {
			t.Fatalf("body=%s batch=%+v err=%v", body, batch, err)
		}
	}
}

func TestClientDomainFiltersMatchMwmblGrammar(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `[
			{"URL":"https://www.go.dev/doc/tutorial","Title":"keep path"},
			{"URL":"https://go.dev/blog/release","Title":"wrong path"},
			{"URL":"https://docs.example.com/guide/start","Title":"excluded wildcard"}
		]`), nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{
		Q: "domain grammar", IncludeDomains: []string{"go.dev/doc", "*.example.com/guide"},
		ExcludeDomains: []string{"www.docs.example.com/guide"},
	})
	if err != nil || len(batch.Hits) != 1 || batch.Hits[0].URL != "https://www.go.dev/doc/tutorial" {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestClientRejectsUnsupportedControlsBeforeNetwork(t *testing.T) {
	tests := []struct {
		name    string
		query   search.Query
		control string
	}{
		{name: "engines", query: search.Query{Engines: []string{"wiby"}}, control: "engines"},
		{name: "categories", query: search.Query{Categories: []string{"general"}}, control: "categories"},
		{name: "language", query: search.Query{Language: "en"}, control: "language"},
		{name: "time range", query: search.Query{TimeRange: "day"}, control: "time_range"},
		{name: "safe search disabled", query: search.Query{SafeSearch: intPointer(0)}, control: "safesearch"},
		{name: "safe search strict", query: search.Query{SafeSearch: intPointer(2)}, control: "safesearch"},
		{name: "exact match", query: search.Query{ExactMatch: true}, control: "exact_match"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return jsonResponse(http.StatusOK, `[]`), nil
			})))
			test.query.Q = "birds"
			batch, err := client.SearchBatch(context.Background(), test.query)
			var unsupported *search.UnsupportedControlError
			if !errors.As(err, &unsupported) || unsupported.Control != test.control || batch.Status != search.BatchFailed || calls.Load() != 0 {
				t.Fatalf("batch=%+v err=%v calls=%d", batch, err, calls.Load())
			}
		})
	}

	var supportedRequest *http.Request
	client := mustNew(t, testOptions(roundTripFunc(func(request *http.Request) (*http.Response, error) {
		supportedRequest = request.Clone(request.Context())
		return jsonResponse(http.StatusOK, `[]`), nil
	})))
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "safe", SafeSearch: intPointer(1)}); err != nil {
		t.Fatalf("safe-search moderate should be supported: %v", err)
	}
	if supportedRequest == nil || supportedRequest.URL.Query().Has("nsfw") {
		t.Fatalf("request must retain Wiby's filtered default: %v", supportedRequest)
	}
}

func TestClientHonorsRetryAfterAndConservativeBlockCooldown(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				response := jsonResponse(status, `{}`)
				if status == http.StatusTooManyRequests {
					response.Header.Set("Retry-After", "3")
				}
				return response, nil
			})))
			client.now = func() time.Time { return time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) }
			wantDelay := defaultBlockBackoff
			if status == http.StatusTooManyRequests {
				wantDelay = 3 * time.Second
			}
			for range 2 {
				batch, err := client.SearchBatch(context.Background(), search.Query{Q: "limited"})
				var statusErr *StatusError
				if !errors.As(err, &statusErr) || statusErr.RetryAfter != wantDelay || batch.Diagnostics[0].RetryAfter != wantDelay {
					t.Fatalf("batch=%+v err=%v", batch, err)
				}
			}
			if calls.Load() != 1 {
				t.Fatalf("HTTP calls = %d, want cooldown to suppress second request", calls.Load())
			}
		})
	}
}

func TestClientHugeRetryAfterClamps(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		response := jsonResponse(http.StatusTooManyRequests, `{}`)
		response.Header.Set("Retry-After", "9223372036854775807")
		return response, nil
	})))
	client.now = func() time.Time { return time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) }
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "limited"})
	var statusErr *StatusError
	if !errors.As(err, &statusErr) || statusErr.RetryAfter != maxRetryAfter || batch.Diagnostics[0].RetryAfter != maxRetryAfter {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestClientHardCapsRateBurstAndConcurrency(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(http.StatusOK, `[]`), nil
	})))
	if client.budget.Burst() != 1 || client.budget.MaxConcurrency() != 1 {
		t.Fatalf("burst=%d concurrency=%d", client.budget.Burst(), client.budget.MaxConcurrency())
	}
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "first"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	batch, err := client.SearchBatch(ctx, search.Query{Q: "second"})
	if err == nil || batch.Diagnostics[0].Reason != "canceled" || calls.Load() != 1 {
		t.Fatalf("batch=%+v err=%v calls=%d", batch, err, calls.Load())
	}
}

func TestNewRequiresOfficialEndpointContactAndPositiveBudgets(t *testing.T) {
	valid := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `[]`), nil
	}))
	tests := []struct {
		name   string
		mutate func(*Options)
	}{
		{name: "wrong endpoint", mutate: func(o *Options) { o.Endpoint = "https://example.com/json/" }},
		{name: "wrong official path", mutate: func(o *Options) { o.Endpoint = "https://wiby.me/" }},
		{name: "missing contact", mutate: func(o *Options) { o.UserAgent = "FetchmarkBot" }},
		{name: "missing client", mutate: func(o *Options) { o.HTTPClient = nil }},
		{name: "missing results", mutate: func(o *Options) { o.MaxResults = 0 }},
		{name: "missing body bound", mutate: func(o *Options) { o.MaxBodyBytes = 0 }},
		{name: "missing rate", mutate: func(o *Options) { o.RatePerSecond = 0 }},
		{name: "missing burst", mutate: func(o *Options) { o.Burst = 0 }},
		{name: "missing concurrency", mutate: func(o *Options) { o.MaxConcurrency = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.mutate(&options)
			if _, err := New(options); err == nil {
				t.Fatal("New succeeded")
			}
		})
	}
}

func TestClientRejectsOversizedAndMalformedResponses(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "oversized", body: strings.Repeat("x", 33)},
		{name: "malformed", body: `{not-json}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return jsonResponse(http.StatusOK, test.body), nil
			}))
			if test.name == "oversized" {
				options.MaxBodyBytes = 32
			}
			client := mustNew(t, options)
			batch, err := client.SearchBatch(context.Background(), search.Query{Q: "bad response"})
			if err == nil || batch.Status != search.BatchFailed || len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != test.name {
				t.Fatalf("batch=%+v err=%v", batch, err)
			}
		})
	}
}

func intPointer(value int) *int { return &value }

func testOptions(transport http.RoundTripper) Options {
	return Options{
		HTTPClient: &http.Client{Transport: transport},
		UserAgent:  "FetchmarkBot/0.1 (https://github.com/staticvar/fetchmark)",
		MaxResults: 12, MaxBodyBytes: 1 << 20, RatePerSecond: 100, Burst: 100, MaxConcurrency: 100,
	}
}

func mustNew(t *testing.T, options Options) *Client {
	t.Helper()
	client, err := New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return client
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
