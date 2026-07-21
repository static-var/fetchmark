package scrapling

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/core/search"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestClientSearchBatchMapsPartialResultsAndChallengeEvidence(t *testing.T) {
	var received searchRequest
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Path != searchPath {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Fatal(err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{
			"provider":"scrapling",
			"status":"partial",
			"results":[
				{"url":"https://example.com/first","title":" First result ","snippet":" useful snippet ","engine":"google","rank":1},
				{"url":"https://example.org/second","title":"Second result","engine":"duckduckgo","rank":2}
			],
			"diagnostics":[{
				"source":"brave","reason":"result_selector_miss","retryable":false,"retry_after_ms":0,
				"fallback":{"format":"cleaned_dom","content":"<main><h1>Parser changed</h1></main>","truncated":false}
			}]
		}`)),
		}, nil
	})}

	client, err := New(Options{
		Endpoint: "http://scrapling:8080", HTTPClient: httpClient, AllowInsecureHTTP: true,
		MaxResults: 10, MaxBodyBytes: 1 << 20, RatePerSecond: 10, Burst: 1, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := client.SearchBatch(context.Background(), search.Query{
		Q: " open search ", Engines: []string{"google", "duckduckgo", "brave"}, MaxResults: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	if received.Query != "open search" || received.MaxResults != 2 || len(received.Engines) != 3 {
		t.Fatalf("request body = %+v", received)
	}
	if batch.Provider != "scrapling" || batch.Instance == "" || batch.Status != search.BatchPartial || len(batch.Hits) != 2 {
		t.Fatalf("batch = %+v", batch)
	}
	if batch.Hits[0].Title != "First result" || batch.Hits[0].Snippet != "useful snippet" || batch.Hits[0].Engines[0] != "google" || batch.Hits[0].Metadata["scrapling_rank"] != "1" {
		t.Fatalf("first hit = %+v", batch.Hits[0])
	}
	if len(batch.Diagnostics) != 1 || batch.Diagnostics[0].Source != "brave" || batch.Diagnostics[0].Reason != "result_selector_miss" || batch.Diagnostics[0].Retryable || batch.Diagnostics[0].RetryAfter != 0 {
		t.Fatalf("diagnostics = %+v", batch.Diagnostics)
	}
	if batch.Diagnostics[0].Fallback == nil || batch.Diagnostics[0].Fallback.Format != "cleaned_dom" || !strings.Contains(batch.Diagnostics[0].Fallback.Content, "Parser changed") {
		t.Fatalf("fallback = %+v", batch.Diagnostics[0].Fallback)
	}
}

func TestClientMapsDegradedEmptyWithoutConvertingItToAuthoritative(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
				"provider":"scrapling","status":"degraded_empty","results":[],
				"diagnostics":[{"source":"google","reason":"challenge","retryable":true,"retry_after_ms":60000}]
			}`)),
			Header: http.Header{"Content-Type": []string{"application/json"}},
		}, nil
	})}
	client, err := New(Options{
		Endpoint: "http://scrapling:8080", HTTPClient: httpClient, AllowInsecureHTTP: true,
		MaxResults: 20, MaxBodyBytes: 1 << 20, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "query", Engines: []string{"google"}})
	if err != nil || batch.Status != search.BatchDegradedEmpty || len(batch.Hits) != 0 || len(batch.Diagnostics) != 1 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestNewPinsOriginAndCapsBrowserBudgets(t *testing.T) {
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("unexpected transport call")
	})
	client, err := New(Options{
		Endpoint: "https://scrapling.internal", HTTPClient: &http.Client{Transport: base},
		MaxResults: 100, MaxBodyBytes: 1 << 20, RatePerSecond: 100, Burst: 10, MaxConcurrency: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.maxResults != maxProviderResults || client.budget.Burst() != 4 || client.budget.MaxConcurrency() != 4 {
		t.Fatalf("caps = results %d burst %d concurrency %d", client.maxResults, client.budget.Burst(), client.budget.MaxConcurrency())
	}
	request, err := http.NewRequest(http.MethodPost, "https://other.internal/v1/search", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.httpClient.Transport.RoundTrip(request); err == nil || !strings.Contains(err.Error(), "outside configured origin") {
		t.Fatalf("off-origin error = %v", err)
	}
	for _, endpoint := range []string{
		"http://scrapling.internal", "https://user:secret@scrapling.internal", "https://scrapling.internal/path", "https://scrapling.internal?token=x",
	} {
		_, err := New(Options{
			Endpoint: endpoint, HTTPClient: &http.Client{Transport: base},
			MaxResults: 20, MaxBodyBytes: 1 << 20, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1,
		})
		if err == nil {
			t.Fatalf("New accepted endpoint %q", endpoint)
		}
	}
}

func TestClientRejectsDomainControlsItCannotSearchAuthoritatively(t *testing.T) {
	var calls int
	client, err := New(Options{
		Endpoint: "http://scrapling:8080", AllowInsecureHTTP: true,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return nil, errors.New("unexpected transport call")
		})},
		MaxResults: 20, MaxBodyBytes: 1 << 20, RatePerSecond: 1, Burst: 1, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := client.SearchBatch(context.Background(), search.Query{Q: "query", IncludeDomains: []string{"example.com"}})
	var unsupported *search.UnsupportedControlError
	if !errors.As(err, &unsupported) || unsupported.Control != "include_domains" || batch.Status != search.BatchFailed || calls != 0 {
		t.Fatalf("batch=%+v err=%v calls=%d", batch, err, calls)
	}
}
