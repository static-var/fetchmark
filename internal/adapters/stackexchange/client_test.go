package stackexchange

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type readErrorBody struct{}

func (readErrorBody) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (readErrorBody) Close() error             { return nil }

func TestClientSearchBatchMapsFixedSimilarRequestAndAttribution(t *testing.T) {
	var request *http.Request
	client := mustNew(t, Options{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			request = req.Clone(req.Context())
			return jsonResponse(http.StatusOK, `{"items":[{"tags":["go","error-handling"],"owner":{"user_id":42,"display_name":"Ada &amp; Co"},"is_answered":true,"answer_count":3,"score":9,"last_activity_date":1784304000,"creation_date":1784217600,"question_id":123,"accepted_answer_id":456,"content_license":"CC BY-SA 4.0","body":"<p>Use <code>Unwrap</code> so errors.Is can inspect the cause.</p>","title":"How to use errors.Is &amp; errors.As?"}],"has_more":false,"quota_max":300,"quota_remaining":299}`), nil
		})},
		UserAgent:  "FetchmarkBot/0.1 (https://github.com/staticvar/fetchmark)",
		MaxResults: 50, MaxBodyBytes: 1 << 20, RatePerSecond: 100, Burst: 100, MaxConcurrency: 100,
	})
	client.now = func() time.Time { return time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) }

	moderate := 1
	batch, err := client.SearchBatch(context.Background(), search.Query{
		Q: "How should Go errors wrap?", Language: "en", TimeRange: "month",
		SafeSearch: &moderate, MaxResults: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	if request == nil || request.URL.Scheme != "https" || request.URL.Host != "api.stackexchange.com" || request.URL.Path != "/2.3/similar" {
		t.Fatalf("request URL = %v", request)
	}
	query := request.URL.Query()
	if query.Get("site") != "stackoverflow" || query.Get("title") != "How should Go errors wrap?" || query.Get("sort") != "relevance" || query.Get("order") != "desc" || query.Get("pagesize") != "10" || query.Get("filter") != "withbody" {
		t.Fatalf("query = %v", query)
	}
	if query.Get("fromdate") != "1781740800" || query.Get("todate") != "1784332800" || query.Get("key") != "" || query.Get("access_token") != "" {
		t.Fatalf("bounded/auth query = %v", query)
	}
	if batch.Provider != "stackexchange" || batch.Instance != "api.stackexchange.com" || batch.Status != search.BatchHealthy || len(batch.Hits) != 1 {
		t.Fatalf("batch = %+v", batch)
	}
	hit := batch.Hits[0]
	if hit.URL != "https://stackoverflow.com/questions/123" || hit.Title != "How to use errors.Is & errors.As?" || hit.Snippet != "go, error-handling · score 9 · 3 answers" {
		t.Fatalf("hit = %+v", hit)
	}
	if hit.Metadata["source"] != "Stack Overflow" || hit.Metadata["attribution_url"] != "https://stackoverflow.com/" || hit.Metadata["license"] != "CC BY-SA 4.0" || hit.Metadata["owner"] != "Ada & Co" || hit.Metadata["accepted_answer_id"] != "456" {
		t.Fatalf("metadata = %+v", hit.Metadata)
	}
	if hit.PublishedAt == nil || hit.PublishedAt.Unix() != 1784217600 {
		t.Fatalf("published = %v", hit.PublishedAt)
	}
	if hit.ProviderDocument == nil || string(hit.ProviderDocument.HTML) != "<p>Use <code>Unwrap</code> so errors.Is can inspect the cause.</p>" || hit.ProviderDocument.Author != "Ada & Co" || hit.ProviderDocument.SiteName != "Stack Overflow" {
		t.Fatalf("provider document = %+v", hit.ProviderDocument)
	}
}

func TestClientDoesNotExposeProviderDocumentWithoutContentLicense(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"items":[{"question_id":1,"title":"Unlicensed body","body":"<p>content</p>"}],"quota_remaining":10}`), nil
	})))
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "test"})
	if err != nil || len(batch.Hits) != 1 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	if batch.Hits[0].ProviderDocument == nil || len(batch.Hits[0].ProviderDocument.HTML) != 0 {
		t.Fatalf("unlicensed provider no-fetch marker = %+v", batch.Hits[0].ProviderDocument)
	}
}

func TestClientBackoffAndQuotaExhaustionOpenCooldown(t *testing.T) {
	t.Run("backoff with usable results", func(t *testing.T) {
		var calls atomic.Int32
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return jsonResponse(http.StatusOK, `{"items":[{"question_id":1,"title":"Useful"}],"backoff":12,"quota_remaining":10}`), nil
		})))
		now := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
		client.now = func() time.Time { return now }
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "first"})
		if err != nil || len(batch.Hits) != 1 || batch.Status != search.BatchHealthy {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
		batch, err = client.SearchBatch(context.Background(), search.Query{Q: "different"})
		var statusErr *StatusError
		if !errors.As(err, &statusErr) || batch.Diagnostics[0].Reason != "cooldown" || batch.Diagnostics[0].RetryAfter != 12*time.Second || calls.Load() != 1 {
			t.Fatalf("batch=%+v err=%v calls=%d", batch, err, calls.Load())
		}
	})

	t.Run("quota exhausted", func(t *testing.T) {
		var calls atomic.Int32
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return jsonResponse(http.StatusOK, `{"items":[],"quota_max":300,"quota_remaining":0}`), nil
		})))
		client.now = func() time.Time { return time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) }
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "first"})
		if err != nil || batch.Status != search.BatchAuthoritativeEmpty || len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Reason != "quota_exhausted" {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
		if _, err := client.SearchBatch(context.Background(), search.Query{Q: "different"}); err == nil || calls.Load() != 1 {
			t.Fatalf("second err=%v calls=%d", err, calls.Load())
		}
	})
}

func TestClientSuppressesSemanticallyIdenticalRequestsForOneMinute(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(http.StatusOK, `{"items":[],"quota_remaining":100}`), nil
	})))
	now := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	client.now = func() time.Time { return now }
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "  Kotlin   Flow  ", TimeRange: "day"}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(31 * time.Second)
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "Kotlin Flow", TimeRange: "day"})
	if err == nil || batch.Diagnostics[0].Reason != "duplicate_cooldown" || batch.Diagnostics[0].RetryAfter != 29*time.Second || calls.Load() != 1 {
		t.Fatalf("batch=%+v err=%v calls=%d", batch, err, calls.Load())
	}
	now = now.Add(29 * time.Second)
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "Kotlin Flow", TimeRange: "day"}); err != nil || calls.Load() != 2 {
		t.Fatalf("after cooldown err=%v calls=%d", err, calls.Load())
	}
}

func TestClientRejectsDuplicateBeforeSpendingRateCapacity(t *testing.T) {
	var calls atomic.Int32
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return jsonResponse(http.StatusOK, `{"items":[],"quota_remaining":100}`), nil
	})))
	client.now = func() time.Time { return time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) }
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "same"}); err != nil {
		t.Fatal(err)
	}
	// Refill the one-token hard-capped provider budget. The duplicate must not
	// spend it, leaving the distinct request immediately admissible.
	time.Sleep(1100 * time.Millisecond)
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "same"}); err == nil {
		t.Fatal("duplicate request should be suppressed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := client.SearchBatch(ctx, search.Query{Q: "different"}); err != nil {
		t.Fatalf("distinct request did not receive preserved rate token: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("provider calls=%d, want 2", calls.Load())
	}
}

func TestClientDuplicateReservationHandlesConcurrencyCancellationAndAmbiguousNetwork(t *testing.T) {
	t.Run("concurrent identical", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		var calls atomic.Int32
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			close(started)
			<-release
			return jsonResponse(http.StatusOK, `{"items":[],"quota_remaining":100}`), nil
		})))
		results := make(chan error, 2)
		go func() {
			_, err := client.SearchBatch(context.Background(), search.Query{Q: "same"})
			results <- err
		}()
		<-started
		go func() {
			_, err := client.SearchBatch(context.Background(), search.Query{Q: "same"})
			results <- err
		}()
		close(release)
		first, second := <-results, <-results
		if (first == nil) == (second == nil) || calls.Load() != 1 {
			t.Fatalf("errors=(%v,%v) calls=%d", first, second, calls.Load())
		}
	})

	t.Run("canceled before dispatch releases reservation", func(t *testing.T) {
		var calls atomic.Int32
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return jsonResponse(http.StatusOK, `{"items":[],"quota_remaining":100}`), nil
		})))
		if _, err := client.SearchBatch(context.Background(), search.Query{Q: "first"}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		if _, err := client.SearchBatch(ctx, search.Query{Q: "second"}); err == nil {
			t.Fatal("rate-wait cancellation should fail")
		}
		time.Sleep(1100 * time.Millisecond)
		if _, err := client.SearchBatch(context.Background(), search.Query{Q: "second"}); err != nil {
			t.Fatalf("released reservation still suppressed retry: %v", err)
		}
		if calls.Load() != 2 {
			t.Fatalf("provider calls=%d, want 2", calls.Load())
		}
	})

	t.Run("ambiguous network failure remains reserved", func(t *testing.T) {
		var calls atomic.Int32
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return nil, errors.New("ambiguous network failure")
		})))
		if _, err := client.SearchBatch(context.Background(), search.Query{Q: "same"}); err == nil {
			t.Fatal("network failure should fail")
		}
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "same"})
		if err == nil || batch.Diagnostics[0].Reason != "duplicate_cooldown" || calls.Load() != 1 {
			t.Fatalf("batch=%+v err=%v calls=%d", batch, err, calls.Load())
		}
	})
}

func TestQueryReservationDuplicateWindowStartsAtDispatch(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"items":[]}`), nil
	})))
	now := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	client.now = func() time.Time { return now }
	key := sha256.Sum256([]byte("query"))
	if retryAfter := client.reserveQuery(key); retryAfter != 0 {
		t.Fatalf("initial reservation retry_after=%v", retryAfter)
	}
	now = now.Add(5 * time.Second)
	if err := client.commitQueryDispatch(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	now = now.Add(59 * time.Second)
	if retryAfter := client.reserveQuery(key); retryAfter != time.Second {
		t.Fatalf("retry_after=%v at 59 seconds after dispatch, want 1s", retryAfter)
	}
	now = now.Add(time.Second)
	if retryAfter := client.reserveQuery(key); retryAfter != 0 {
		t.Fatalf("retry_after=%v at 60 seconds after dispatch", retryAfter)
	}
}

func TestClientDuplicateWindowUsesPostAdmissionDispatchTime(t *testing.T) {
	base := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	var nowUnix atomic.Int64
	nowUnix.Store(base.Unix())
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"items":[],"quota_remaining":100}`), nil
	})))
	client.now = func() time.Time { return time.Unix(nowUnix.Load(), 0).UTC() }
	if _, err := client.SearchBatch(context.Background(), search.Query{Q: "first"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := client.SearchBatch(context.Background(), search.Query{Q: "second"})
		done <- err
	}()
	// The second request is now waiting for the one-request/second token. Move
	// the provider clock before admission so its dispatch stamp is observably
	// later than its pending reservation.
	time.Sleep(100 * time.Millisecond)
	nowUnix.Store(base.Add(5 * time.Second).Unix())
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	nowUnix.Store(base.Add(64 * time.Second).Unix())
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "second"})
	if err == nil || batch.Diagnostics[0].Reason != "duplicate_cooldown" || batch.Diagnostics[0].RetryAfter != time.Second {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestCanceledContextReleasesPendingReservationBeforeDispatch(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"items":[]}`), nil
	})))
	client.now = func() time.Time { return time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) }
	key := sha256.Sum256([]byte("query"))
	if retryAfter := client.reserveQuery(key); retryAfter != 0 {
		t.Fatalf("initial reservation retry_after=%v", retryAfter)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.commitQueryDispatch(ctx, key); !errors.Is(err, context.Canceled) {
		t.Fatalf("dispatch error=%v", err)
	}
	if retryAfter := client.reserveQuery(key); retryAfter != 0 {
		t.Fatalf("canceled pending reservation remained for %v", retryAfter)
	}
}

func TestClientRejectsUnsupportedControlsAndUnsafeConfiguration(t *testing.T) {
	strict := 2
	queries := []search.Query{
		{Q: "x", Engines: []string{"stackoverflow"}},
		{Q: "x", Categories: []string{"developer"}},
		{Q: "x", Language: "fr"},
		{Q: "x", SafeSearch: &strict},
		{Q: "x", ExactMatch: true},
	}
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("unsupported control reached network")
		return nil, nil
	})))
	for _, query := range queries {
		batch, err := client.SearchBatch(context.Background(), query)
		var unsupported *search.UnsupportedControlError
		if !errors.As(err, &unsupported) || batch.Status != search.BatchFailed || batch.Diagnostics[0].Reason != "unsupported_control" {
			t.Fatalf("query=%+v batch=%+v err=%v", query, batch, err)
		}
	}

	for _, options := range []Options{
		{Endpoint: "https://evil.example/2.3/similar", HTTPClient: &http.Client{}, UserAgent: "Fetchmark/1 (https://example.com)", MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1},
		{HTTPClient: &http.Client{}, UserAgent: "curl/1", MaxResults: 10, MaxBodyBytes: 1024, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1},
	} {
		if _, err := New(options); err == nil {
			t.Fatalf("New accepted unsafe options: %+v", options)
		}
	}
}

func TestClientClassifiesPartialMalformedOversizedAndHTTPBackoff(t *testing.T) {
	t.Run("partial malformed rows", func(t *testing.T) {
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{"items":[{"question_id":1,"title":"Useful"},{"question_id":0,"title":"Invalid"}],"quota_remaining":10}`), nil
		})))
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "test"})
		if err != nil || batch.Status != search.BatchPartial || len(batch.Hits) != 1 || batch.Diagnostics[len(batch.Diagnostics)-1].Reason != "malformed_results" {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
	})

	t.Run("missing items", func(t *testing.T) {
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{"quota_remaining":10}`), nil
		})))
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "test"})
		if err == nil || batch.Diagnostics[0].Reason != "malformed" {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
	})

	t.Run("oversized", func(t *testing.T) {
		options := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			return jsonResponse(http.StatusOK, strings.Repeat("x", 65)), nil
		}))
		options.MaxBodyBytes = 64
		client := mustNew(t, options)
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "test"})
		if err == nil || batch.Diagnostics[0].Reason != "oversized" {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
	})

	t.Run("HTTP backoff", func(t *testing.T) {
		var calls atomic.Int32
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return jsonResponse(http.StatusBadGateway, `{"backoff":7,"error_name":"throttle_violation"}`), nil
		})))
		client.now = func() time.Time { return time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) }
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "test"})
		var statusErr *StatusError
		if !errors.As(err, &statusErr) || statusErr.RetryAfter != 7*time.Second || batch.Diagnostics[0].Reason != "http_502" {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
		if _, err := client.SearchBatch(context.Background(), search.Query{Q: "different"}); err == nil || calls.Load() != 1 {
			t.Fatalf("cooldown err=%v calls=%d", err, calls.Load())
		}
	})
}

func TestClientPreservesSchedulingEvidenceBeforeBodyAndWrapperValidation(t *testing.T) {
	now := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)

	t.Run("oversized 429 with Retry-After", func(t *testing.T) {
		var calls atomic.Int32
		options := testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			response := jsonResponse(http.StatusTooManyRequests, strings.Repeat("x", 65))
			response.Header.Set("Retry-After", "13")
			return response, nil
		}))
		options.MaxBodyBytes = 64
		client := mustNew(t, options)
		client.now = func() time.Time { return now }
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "first"})
		if err == nil || batch.Diagnostics[0].Reason != "oversized" || batch.Diagnostics[0].RetryAfter != 13*time.Second {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
		if _, err := client.SearchBatch(context.Background(), search.Query{Q: "different"}); err == nil || calls.Load() != 1 {
			t.Fatalf("cooldown err=%v calls=%d", err, calls.Load())
		}
	})

	t.Run("read-error 503 with Retry-After date", func(t *testing.T) {
		var calls atomic.Int32
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header:     http.Header{"Retry-After": []string{now.Add(17 * time.Second).Format(http.TimeFormat)}},
				Body:       readErrorBody{},
			}, nil
		})))
		client.now = func() time.Time { return now }
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "first"})
		if err == nil || batch.Diagnostics[0].Reason != "body_read" || batch.Diagnostics[0].RetryAfter != 17*time.Second {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
		if _, err := client.SearchBatch(context.Background(), search.Query{Q: "different"}); err == nil || calls.Load() != 1 {
			t.Fatalf("cooldown err=%v calls=%d", err, calls.Load())
		}
	})

	t.Run("read-error 200 remains retryable", func(t *testing.T) {
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: readErrorBody{}}, nil
		})))
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "first"})
		if err == nil || batch.Diagnostics[0].Reason != "body_read" || !batch.Diagnostics[0].Retryable {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
	})

	t.Run("longer JSON backoff wins", func(t *testing.T) {
		client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
			response := jsonResponse(http.StatusBadGateway, `{"items":[],"backoff":7}`)
			response.Header.Set("Retry-After", "2")
			return response, nil
		})))
		client.now = func() time.Time { return now }
		batch, err := client.SearchBatch(context.Background(), search.Query{Q: "first"})
		var statusErr *StatusError
		if !errors.As(err, &statusErr) || statusErr.RetryAfter != 7*time.Second || batch.Diagnostics[0].RetryAfter != 7*time.Second {
			t.Fatalf("batch=%+v err=%v", batch, err)
		}
	})

	for name, body := range map[string]string{
		"backoff with missing items":    `{"backoff":12,"quota_remaining":10}`,
		"quota zero with missing items": `{"quota_remaining":0}`,
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return jsonResponse(http.StatusOK, body), nil
			})))
			client.now = func() time.Time { return now }
			batch, err := client.SearchBatch(context.Background(), search.Query{Q: "first"})
			if err == nil || batch.Diagnostics[0].Reason != "malformed" {
				t.Fatalf("batch=%+v err=%v", batch, err)
			}
			if _, err := client.SearchBatch(context.Background(), search.Query{Q: "different"}); err == nil || calls.Load() != 1 {
				t.Fatalf("cooldown err=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestClientRejectsProviderRedirects(t *testing.T) {
	for name, location := range map[string]string{
		"different host": "https://example.com/not-stackexchange",
		"different path": "https://api.stackexchange.com/2.3/search",
	} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
				if calls.Add(1) > 1 {
					t.Fatal("redirect reached a second request")
				}
				response := jsonResponse(http.StatusFound, "")
				response.Header.Set("Location", location)
				return response, nil
			})))
			batch, err := client.SearchBatch(context.Background(), search.Query{Q: "test"})
			if err == nil || batch.Diagnostics[0].Reason != "network" || calls.Load() != 1 {
				t.Fatalf("batch=%+v err=%v calls=%d", batch, err, calls.Load())
			}
		})
	}
}

func TestClientOmitsUnsafeProviderTimestampsAndRemainsSerializable(t *testing.T) {
	client := mustNew(t, testOptions(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"items":[{"question_id":1,"title":"Extreme","creation_date":9223372036854775807,"last_activity_date":9223372036854775807},{"question_id":2,"title":"Pre epoch","creation_date":-1,"last_activity_date":-1},{"question_id":3,"title":"Future","creation_date":1784505600,"last_activity_date":1784505600}],"quota_remaining":10}`), nil
	})))
	client.now = func() time.Time { return time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC) }
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "test"})
	if err != nil || batch.Status != search.BatchPartial || len(batch.Hits) != 3 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	for _, hit := range batch.Hits {
		if hit.PublishedAt != nil || hit.Metadata["last_activity_unix"] != "" {
			t.Fatalf("unsafe timestamp retained: %+v", hit)
		}
	}
	if _, err := json.Marshal(batch.Hits); err != nil {
		t.Fatalf("serialize hits: %v", err)
	}
}

func TestClientCapsProviderConcurrency(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var active, maximum atomic.Int32
	options := testOptions(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		current := active.Add(1)
		for {
			prior := maximum.Load()
			if current <= prior || maximum.CompareAndSwap(prior, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
			active.Add(-1)
			return jsonResponse(http.StatusOK, `{"items":[],"quota_remaining":10}`), nil
		case <-req.Context().Done():
			active.Add(-1)
			return nil, req.Context().Err()
		}
	}))
	options.MaxConcurrency = 9
	options.Burst = 9
	options.RatePerSecond = 100
	client := mustNew(t, options)
	var wg sync.WaitGroup
	for _, query := range []string{"first", "second"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = client.SearchBatch(context.Background(), search.Query{Q: query})
		}()
	}
	<-started
	select {
	case <-started:
		t.Fatal("second provider request started concurrently")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrency=%d", maximum.Load())
	}
}

func testOptions(transport http.RoundTripper) Options {
	return Options{
		HTTPClient: &http.Client{Transport: transport}, UserAgent: "FetchmarkBot/0.1 (https://github.com/staticvar/fetchmark)",
		MaxResults: 10, MaxBodyBytes: 1 << 20, RatePerSecond: 100, Burst: 10, MaxConcurrency: 10,
	}
}

func mustNew(t *testing.T, options Options) *Client {
	t.Helper()
	client, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
