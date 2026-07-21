package pipeline

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/bleveindex"
	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	artifactfs "github.com/staticvar/fetchmark/internal/adapters/localartifact"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
)

type conditionalSequenceFetcher struct {
	mu        sync.Mutex
	responses []fetcher.Result
	requests  []fetcher.Request
	before    func(int, fetcher.Request)
}

func (fetcherSequence *conditionalSequenceFetcher) Fetch(_ context.Context, request fetcher.Request) fetcher.Result {
	fetcherSequence.mu.Lock()
	index := len(fetcherSequence.requests)
	fetcherSequence.requests = append(fetcherSequence.requests, request)
	before := fetcherSequence.before
	response := fetcherSequence.responses[index]
	fetcherSequence.mu.Unlock()
	if before != nil {
		before(index, request)
	}
	response.URL = request.URL
	if response.FinalURL == "" {
		response.FinalURL = request.URL
	}
	if response.UAUsed == "" {
		response.UAUsed = request.UserAgent
	}
	return response
}

type countingContentExtractor struct{ calls atomic.Int64 }

func (extractor *countingContentExtractor) Extract(raw []byte, rawURL string) (*model.Content, error) {
	extractor.calls.Add(1)
	return &model.Content{URL: rawURL, Title: "Retained", MainText: string(raw), Markdown: string(raw)}, nil
}

type failingContentExtractor struct{}

func (failingContentExtractor) Extract([]byte, string) (*model.Content, error) {
	return nil, errors.New("extract failed")
}

func TestPipelineConditional304ReusesStoredSourceAndRefreshesIndex(t *testing.T) {
	const rawURL = "https://example.com/revalidated"
	base := time.Now().UTC().Add(-time.Hour)
	fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{
		{Status: 200, Body: []byte("immutable retained source"), ContentType: "text/html", ETag: `W/"one"`,
			ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
		{Status: 304, NotModified: true, ETag: `W/"one"`, ObservedAt: base.Add(time.Minute),
			RobotsAllowed: true, RobotsAuthoritative: true},
	}}
	extractor := &countingContentExtractor{}
	pipeline, index, artifacts := personalPipeline(t, fetches, extractor)
	options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
	first := pipeline.Parse(context.Background(), options)
	second := pipeline.Parse(context.Background(), options)
	if len(first) != 1 || first[0].Content == nil || len(second) != 1 || second[0].Content == nil || second[0].Content.MainText != "immutable retained source" || !second[0].FromCache {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	if extractor.calls.Load() != 2 {
		t.Fatalf("extractor calls = %d", extractor.calls.Load())
	}
	if len(fetches.requests) != 2 || fetches.requests[1].IfNoneMatch != `W/"one"` {
		t.Fatalf("requests = %+v", fetches.requests)
	}
	if count, err := index.Count(); err != nil || count != 1 {
		t.Fatalf("index count=%d err=%v", count, err)
	}
	current, ok, err := artifacts.Current(context.Background(), rawURL)
	if err != nil || !ok || current.ObservedAt != base.Add(time.Minute) || string(current.Body) != "immutable retained source" {
		t.Fatalf("current=%+v ok=%v err=%v", current, ok, err)
	}
}

func TestPipelineConditional304NoIndexRevokesArtifactAndIndex(t *testing.T) {
	const rawURL = "https://example.com/revoked-on-304"
	base := time.Now().UTC().Add(-time.Hour)
	fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{
		{Status: 200, Body: []byte("source later revoked"), ContentType: "text/html", ETag: `"one"`,
			ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
		{Status: 304, NotModified: true, ETag: `"one"`, XRobotsTag: []string{"noindex"},
			ObservedAt: base.Add(time.Minute), RobotsAllowed: true, RobotsAuthoritative: true},
	}}
	pipeline, index, artifacts := personalPipeline(t, fetches, &countingContentExtractor{})
	options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
	pipeline.Parse(context.Background(), options)
	result := pipeline.Parse(context.Background(), options)
	if len(result) != 1 || result[0].Content != nil || result[0].Unsupported != "noindex" {
		t.Fatalf("result = %+v", result)
	}
	if count, err := index.Count(); err != nil || count != 0 {
		t.Fatalf("index count=%d err=%v", count, err)
	}
	if current, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok || current.Body != nil {
		t.Fatalf("current=%+v ok=%v err=%v", current, ok, err)
	}
}

func TestPipelineConditional304NoArchiveRevokesArtifactAndIndex(t *testing.T) {
	const rawURL = "https://example.com/noarchive-on-304"
	base := time.Now().UTC().Add(-time.Hour)
	fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{
		{Status: 200, Body: []byte("source later made non-archivable"), ContentType: "text/html", ETag: `"one"`,
			ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
		{Status: 304, NotModified: true, ETag: `"one"`, XRobotsTag: []string{"noarchive"},
			ObservedAt: base.Add(time.Minute), RobotsAllowed: true, RobotsAuthoritative: true},
	}}
	pipeline, index, artifacts := personalPipeline(t, fetches, &countingContentExtractor{})
	options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
	pipeline.Parse(context.Background(), options)
	result := pipeline.Parse(context.Background(), options)
	if len(result) != 1 || result[0].Content != nil || result[0].Unsupported != "noarchive" {
		t.Fatalf("result = %+v", result)
	}
	if count, err := index.Count(); err != nil || count != 0 {
		t.Fatalf("index count=%d err=%v", count, err)
	}
	if current, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok || current.Body != nil {
		t.Fatalf("current=%+v ok=%v err=%v", current, ok, err)
	}
}

func TestPipelineDefinitiveChangedResponseRevokesPriorPersistentState(t *testing.T) {
	tests := []struct {
		name     string
		response fetcher.Result
	}{
		{name: "terminal non html 404", response: fetcher.Result{Status: 404, Unsupported: fetcher.ReasonNonHTML, RobotsAllowed: true, RobotsAuthoritative: true}},
		{name: "terminal non html 410", response: fetcher.Result{Status: 410, Unsupported: fetcher.ReasonNonHTML, RobotsAllowed: true, RobotsAuthoritative: true}},
		{name: "changed non html", response: fetcher.Result{Status: 200, Unsupported: fetcher.ReasonNonHTML, ContentType: "application/pdf", RobotsAllowed: true, RobotsAuthoritative: true}},
		{name: "changed oversized", response: fetcher.Result{Status: 200, Unsupported: fetcher.ReasonTooLarge, ContentType: "text/html", RobotsAllowed: true, RobotsAuthoritative: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const rawURL = "https://example.com/changed"
			base := time.Now().UTC().Add(-time.Hour)
			seed := fetcher.Result{Status: 200, Body: []byte("prior searchable source"), ContentType: "text/html", ETag: `"one"`, ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true}
			test.response.ObservedAt = base.Add(time.Minute)
			fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{seed, test.response}}
			pipeline, index, artifacts := personalPipeline(t, fetches, &countingContentExtractor{})
			options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
			pipeline.Parse(context.Background(), options)
			pipeline.Parse(context.Background(), options)
			if count, err := index.Count(); err != nil || count != 0 {
				t.Fatalf("index count=%d err=%v", count, err)
			}
			if current, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok || current.Body != nil {
				t.Fatalf("current=%+v ok=%v err=%v", current, ok, err)
			}
		})
	}
}

func TestPipelineTerminalStatusEmitsOneSpecificHeaderTombstone(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		directive   string
		disposition localcorpus.IndexingDisposition
	}{
		{name: "404 noindex", status: 404, directive: "noindex", disposition: localcorpus.DispositionNoIndexHeader},
		{name: "410 noarchive", status: 410, directive: "noarchive", disposition: localcorpus.DispositionNoArchiveHeader},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const rawURL = "https://example.com/terminal-header"
			corpus := &recordingCorpus{}
			fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{{
				Status: test.status, Body: []byte("<html>gone</html>"), ContentType: "text/html",
				XRobotsTag: []string{test.directive}, RobotsAllowed: true, RobotsAuthoritative: true,
			}}}
			pipeline := &Pipeline{
				Fetcher: fetches, Extractor: &countingContentExtractor{}, LocalCorpus: corpus,
				LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModePersonal, MaxAge: 24 * time.Hour},
			}
			pipeline.Parse(context.Background(), Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"})
			if len(corpus.reconciled) != 1 || corpus.reconciled[0].IndexingDisposition != test.disposition {
				t.Fatalf("reconciled = %+v", corpus.reconciled)
			}
		})
	}
}

func TestPipelineHeaderRevocationSurvivesExtractionFailure(t *testing.T) {
	tests := []struct {
		directive   string
		disposition localcorpus.IndexingDisposition
	}{
		{directive: "noindex", disposition: localcorpus.DispositionNoIndexHeader},
		{directive: "noarchive", disposition: localcorpus.DispositionNoArchiveHeader},
	}
	for _, test := range tests {
		t.Run(test.directive, func(t *testing.T) {
			const rawURL = "https://example.com/header-extract-failure"
			corpus := &recordingCorpus{}
			pipeline := &Pipeline{
				Fetcher: &conditionalSequenceFetcher{responses: []fetcher.Result{{
					Status: 200, Body: []byte("<html>unextractable</html>"), ContentType: "text/html",
					XRobotsTag: []string{test.directive}, RobotsAllowed: true, RobotsAuthoritative: true,
				}}},
				Extractor: failingContentExtractor{}, LocalCorpus: corpus,
				LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModePersonal, MaxAge: 24 * time.Hour},
			}
			pipeline.Parse(context.Background(), Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"})
			if len(corpus.reconciled) != 1 || corpus.reconciled[0].IndexingDisposition != test.disposition {
				t.Fatalf("reconciled = %+v", corpus.reconciled)
			}
		})
	}
}

func TestPipelineRealFetcherPlainTextTerminalStatusRevokesPersistentState(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusGone} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "text/plain")
				writer.WriteHeader(status)
				_, _ = writer.Write([]byte("gone"))
			}))
			t.Cleanup(server.Close)
			base := time.Now().UTC().Add(-time.Hour)
			seedFetcher := &conditionalSequenceFetcher{responses: []fetcher.Result{{
				Status: 200, Body: []byte("complete prior representation"), ContentType: "text/html", ETag: `"one"`,
				ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true,
			}}}
			pipeline, index, artifacts := personalPipeline(t, seedFetcher, &countingContentExtractor{})
			options := Options{URLs: []string{server.URL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
			pipeline.Parse(context.Background(), options)
			realFetcher, err := fetcher.New(fetcher.Options{
				Policy: egress.DefaultInternal(), Budgets: fetcher.Budgets{
					MaxBodyBytes: 1 << 20, MaxDecompressedBytes: 1 << 20, Retries: 0,
				},
				DefaultUA: "Fetchmark-Test/1",
			})
			if err != nil {
				t.Fatal(err)
			}
			pipeline.Fetcher = realFetcher
			result := pipeline.Parse(context.Background(), options)
			if len(result) != 1 || result[0].Unsupported != "fetch_failed" {
				t.Fatalf("result = %+v", result)
			}
			if count, err := index.Count(); err != nil || count != 0 {
				t.Fatalf("index count=%d err=%v", count, err)
			}
			if current, ok, err := artifacts.Current(context.Background(), server.URL); err != nil || ok || current.Body != nil {
				t.Fatalf("current=%+v ok=%v err=%v", current, ok, err)
			}
		})
	}
}

func TestPipelineTransientFailurePreservesPriorPersistentState(t *testing.T) {
	const rawURL = "https://example.com/transient"
	base := time.Now().UTC().Add(-time.Hour)
	fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{
		{Status: 200, Body: []byte("prior searchable source"), ContentType: "text/html", ETag: `"one"`, ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
		{Status: 503, Err: errors.New("temporary failure"), ObservedAt: base.Add(time.Minute), RobotsAllowed: true, RobotsAuthoritative: true},
	}}
	pipeline, index, artifacts := personalPipeline(t, fetches, &countingContentExtractor{})
	options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
	pipeline.Parse(context.Background(), options)
	pipeline.Parse(context.Background(), options)
	if count, err := index.Count(); err != nil || count != 1 {
		t.Fatalf("index count=%d err=%v", count, err)
	}
	if current, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || !ok || current.Body == nil {
		t.Fatalf("current=%+v ok=%v err=%v", current, ok, err)
	}
}

func TestPipelineTransientFailureHeadersPreservePriorPersistentState(t *testing.T) {
	for _, directive := range []string{"noindex", "noarchive"} {
		t.Run(directive, func(t *testing.T) {
			const rawURL = "https://example.com/transient-header"
			base := time.Now().UTC().Add(-time.Hour)
			fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{
				{Status: 200, Body: []byte("prior searchable source"), ContentType: "text/html", ETag: `"one"`, ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
				{Status: 503, Err: errors.New("temporary failure"), XRobotsTag: []string{directive}, ObservedAt: base.Add(time.Minute), RobotsAllowed: true, RobotsAuthoritative: true},
			}}
			pipeline, index, artifacts := personalPipeline(t, fetches, &countingContentExtractor{})
			options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
			pipeline.Parse(context.Background(), options)
			pipeline.Parse(context.Background(), options)
			if count, err := index.Count(); err != nil || count != 1 {
				t.Fatalf("index count=%d err=%v", count, err)
			}
			if current, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || !ok || current.Body == nil {
				t.Fatalf("current=%+v ok=%v err=%v", current, ok, err)
			}
		})
	}
}

func TestPipelinePartialRequestBudgetExhaustionPreservesPriorPersistentState(t *testing.T) {
	const rawURL = "https://example.com/request-budget"
	base := time.Now().UTC().Add(-time.Hour)
	requestBudgetResponse := fetcher.Result{
		Status: 200, Unsupported: fetcher.ReasonRequestBudget, ObservedAt: base.Add(time.Minute),
		RobotsAllowed: true, RobotsAuthoritative: true,
	}
	fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{
		{Status: 200, Body: []byte("prior searchable source"), ContentType: "text/html", ETag: `"one"`, ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
		requestBudgetResponse,
	}}
	pipeline, index, artifacts := personalPipeline(t, fetches, &countingContentExtractor{})
	pipeline.MaxArtifactBodyBytes = 64
	pipeline.MaxArtifactDecompressedBytes = 64
	options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
	pipeline.Parse(context.Background(), options)

	budget := &requestBudget{maxSource: 64, source: 60}
	ctx := context.WithValue(context.Background(), requestBudgetKey{}, budget)
	result := model.SearchResult{URL: rawURL}
	pipeline.fetchAndExtract(ctx, options, &result, false)

	if len(fetches.requests) != 2 || fetches.requests[1].MaxTotalBytes != 4 {
		t.Fatalf("requests = %+v, want second partial request budget of 4 bytes", fetches.requests)
	}
	if result.Unsupported != fetcher.ReasonRequestBudget {
		t.Fatalf("unsupported = %q, want %q", result.Unsupported, fetcher.ReasonRequestBudget)
	}
	if count, err := index.Count(); err != nil || count != 1 {
		t.Fatalf("index count=%d err=%v", count, err)
	}
	if current, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || !ok || string(current.Body) != "prior searchable source" {
		t.Fatalf("current=%+v ok=%v err=%v", current, ok, err)
	}
}

func TestPipelineNon200SuccessDoesNotEnterOrReplacePersistentState(t *testing.T) {
	const rawURL = "https://example.com/partial-response"
	base := time.Now().UTC().Add(-time.Hour)
	tests := []struct {
		name     string
		response fetcher.Result
	}{
		{name: "empty 204", response: fetcher.Result{Status: 204}},
		{name: "empty 205", response: fetcher.Result{Status: 205}},
		{name: "unsolicited 206", response: fetcher.Result{Status: 206, Body: []byte("unsolicited partial representation"), ContentType: "text/html"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.response.ObservedAt = base.Add(time.Minute)
			test.response.RobotsAllowed = true
			test.response.RobotsAuthoritative = true
			t.Run("cold", func(t *testing.T) {
				pipeline, index, artifacts := personalPipeline(t, &conditionalSequenceFetcher{responses: []fetcher.Result{test.response}}, &countingContentExtractor{})
				result := pipeline.Parse(context.Background(), Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"})
				if len(result) != 1 || result[0].Content != nil || result[0].Unsupported != "fetch_failed" {
					t.Fatalf("result = %+v", result)
				}
				if count, err := index.Count(); err != nil || count != 0 {
					t.Fatalf("index count=%d err=%v", count, err)
				}
				if current, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok || current.Body != nil {
					t.Fatalf("current=%+v ok=%v err=%v", current, ok, err)
				}
			})
			t.Run("replacement", func(t *testing.T) {
				fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{
					{Status: 200, Body: []byte("complete prior representation"), ContentType: "text/html", ETag: `"one"`, ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
					test.response,
				}}
				pipeline, index, artifacts := personalPipeline(t, fetches, &countingContentExtractor{})
				options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
				pipeline.Parse(context.Background(), options)
				result := pipeline.Parse(context.Background(), options)
				if len(result) != 1 || result[0].Content != nil || result[0].Unsupported != "fetch_failed" {
					t.Fatalf("result = %+v", result)
				}
				if count, err := index.Count(); err != nil || count != 1 {
					t.Fatalf("index count=%d err=%v", count, err)
				}
				if current, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || !ok || string(current.Body) != "complete prior representation" {
					t.Fatalf("current=%+v ok=%v err=%v", current, ok, err)
				}
			})
		})
	}
}

func TestPipelineDefinitiveChangeRevokesNonConditionalPersistentState(t *testing.T) {
	tests := []struct {
		name string
		seed fetcher.Result
	}{
		{name: "validatorless", seed: fetcher.Result{Status: 200, Body: []byte("validatorless source"), ContentType: "text/html", RobotsAllowed: true, RobotsAuthoritative: true}},
		{name: "redirected", seed: fetcher.Result{Status: 200, Body: []byte("redirected source"), ContentType: "text/html", ETag: `"one"`, FinalURL: "https://redirected.example/final", RobotsAllowed: true, RobotsAuthoritative: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const rawURL = "https://example.com/non-conditional"
			base := time.Now().UTC().Add(-time.Hour)
			test.seed.ObservedAt = base
			fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{
				test.seed,
				{Status: 404, ObservedAt: base.Add(time.Minute), RobotsAllowed: true, RobotsAuthoritative: true},
			}}
			pipeline, index, artifacts := personalPipeline(t, fetches, &countingContentExtractor{})
			options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
			pipeline.Parse(context.Background(), options)
			pipeline.Parse(context.Background(), options)
			if count, err := index.Count(); err != nil || count != 0 {
				t.Fatalf("index count=%d err=%v", count, err)
			}
			if current, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok || current.Body != nil {
				t.Fatalf("current=%+v ok=%v err=%v", current, ok, err)
			}
		})
	}
}

func TestPipelineDefinitiveChangeRevokesMissingOrCorruptArtifactProjection(t *testing.T) {
	for _, mutation := range []string{"missing", "corrupt"} {
		t.Run(mutation, func(t *testing.T) {
			const rawURL = "https://example.com/damaged-artifact"
			base := time.Now().UTC().Add(-time.Hour)
			artifactPath := filepath.Join(t.TempDir(), "artifacts")
			artifacts, err := artifactfs.Open(artifactfs.Options{Path: artifactPath, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = artifacts.Close() })
			index := openPipelineIndex(t)
			fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{
				{Status: 200, Body: []byte("source to damage"), ContentType: "text/html", ETag: `"one"`, ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
				{Status: 404, ObservedAt: base.Add(time.Minute), RobotsAllowed: true, RobotsAuthoritative: true},
			}}
			pipeline := &Pipeline{Fetcher: fetches, Extractor: &countingContentExtractor{}, LocalCorpus: index, LocalArtifacts: artifacts, LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModePersonal, MaxAge: 24 * time.Hour}}
			options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
			pipeline.Parse(context.Background(), options)
			current, ok, err := artifacts.Current(context.Background(), rawURL)
			if err != nil || !ok {
				t.Fatalf("seed current=%+v ok=%v err=%v", current, ok, err)
			}
			bodyPath := filepath.Join(artifactPath, "objects", artifactfs.URLID(rawURL), current.ContentHash+".body")
			if mutation == "missing" {
				if err := os.Remove(bodyPath); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(bodyPath, []byte("corrupt"), 0o600); err != nil {
				t.Fatal(err)
			}
			pipeline.Parse(context.Background(), options)
			if count, err := index.Count(); err != nil || count != 0 {
				t.Fatalf("index count=%d err=%v", count, err)
			}
		})
	}
}

func TestPipeline304AfterArtifactRevocationRetriesOnceWithoutValidators(t *testing.T) {
	const rawURL = "https://example.com/raced-revalidation"
	base := time.Now().UTC().Add(-time.Hour)
	artifactsPath := filepath.Join(t.TempDir(), "artifacts")
	artifacts, err := artifactfs.Open(artifactfs.Options{Path: artifactsPath, MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = artifacts.Close() })
	index := openPipelineIndex(t)
	fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{
		{Status: 200, Body: []byte("initial source"), ContentType: "text/html", ETag: `"one"`, ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
		{Status: 304, NotModified: true, ETag: `"one"`, ObservedAt: base.Add(time.Minute), RobotsAllowed: true, RobotsAuthoritative: true},
		{Status: 200, Body: []byte("fresh unconditional source"), ContentType: "text/html", ETag: `"two"`, ObservedAt: base.Add(2 * time.Minute), RobotsAllowed: true, RobotsAuthoritative: true},
	}}
	fetches.before = func(callIndex int, _ fetcher.Request) {
		if callIndex == 1 {
			if err := artifacts.Revoke(context.Background(), rawURL, localcorpus.DispositionTakedown, base.Add(90*time.Second)); err != nil {
				t.Errorf("revoke: %v", err)
			}
			if err := pipelineIndexTakedown(context.Background(), index, rawURL, base.Add(90*time.Second)); err != nil {
				t.Errorf("index takedown: %v", err)
			}
		}
	}
	pipeline := &Pipeline{
		Fetcher: fetches, Extractor: &countingContentExtractor{}, LocalCorpus: index, LocalArtifacts: artifacts,
		LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModePersonal, MaxAge: 24 * time.Hour},
	}
	options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
	pipeline.Parse(context.Background(), options)
	result := pipeline.Parse(context.Background(), options)
	if len(result) != 1 || result[0].Content == nil || result[0].Content.MainText != "fresh unconditional source" {
		t.Fatalf("result = %+v", result)
	}
	if len(fetches.requests) != 3 || fetches.requests[1].IfNoneMatch == "" || fetches.requests[2].IfNoneMatch != "" || fetches.requests[2].IfModifiedSince != "" {
		t.Fatalf("requests = %+v", fetches.requests)
	}
	if count, err := index.Count(); err != nil || count != 0 {
		t.Fatalf("sticky takedown index count=%d err=%v", count, err)
	}
}

func pipelineIndexTakedown(ctx context.Context, index *bleveindex.Index, rawURL string, observedAt time.Time) error {
	return index.Reconcile(ctx, localcorpus.Document{
		URL: rawURL, FetchedAt: observedAt, IndexingDisposition: localcorpus.DispositionTakedown,
	})
}

func TestColdFetchUsesSameRawContentHashInArtifactAndIndex(t *testing.T) {
	const rawURL = "https://example.com/hash-agreement"
	const body = "retained metadata source"
	fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{{
		Status: 200, Body: []byte(body), ContentType: "text/html", ETag: `"one"`,
		ObservedAt: time.Now().UTC(), RobotsAllowed: true, RobotsAuthoritative: true,
	}}}
	pipeline, index, artifacts := personalPipeline(t, fetches, &countingContentExtractor{})
	result := pipeline.Parse(context.Background(), Options{
		URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1",
	})
	if len(result) != 1 || result[0].Content == nil {
		t.Fatalf("result = %+v", result)
	}
	artifact, ok, err := artifacts.Current(context.Background(), rawURL)
	if err != nil || !ok {
		t.Fatalf("artifact ok=%t err=%v", ok, err)
	}
	batch, err := index.SearchBatch(context.Background(), search.Query{Q: "retained metadata", MaxResults: 1})
	if err != nil || len(batch.Hits) != 1 {
		t.Fatalf("index batch=%+v err=%v", batch, err)
	}
	if got := batch.Hits[0].Metadata["content_hash"]; got == "" || got != artifact.ContentHash {
		t.Fatalf("index content hash=%q artifact content hash=%q", got, artifact.ContentHash)
	}
}

func TestArchiveAutomaticallyRetainsDistinctSourcesWhileIndexStaysCurrentOnly(t *testing.T) {
	const rawURL = "https://example.com/archive-flywheel"
	base := time.Now().UTC().Add(-time.Hour)
	fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{
		{Status: 200, Body: []byte("historic alpha only"), ContentType: "text/html", ETag: `"one"`, ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
		{Status: 200, Body: []byte("current beta only"), ContentType: "text/html", ETag: `"two"`, ObservedAt: base.Add(time.Minute), RobotsAllowed: true, RobotsAuthoritative: true},
	}}
	index := openPipelineIndex(t)
	artifacts, err := artifactfs.Open(artifactfs.Options{
		Path: filepath.Join(t.TempDir(), "archive"), MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10,
		MaxEntries: 10, Archive: true, MaxVersions: 10, MaxVersionsPerURL: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = artifacts.Close() })
	pipeline := &Pipeline{
		Fetcher: fetches, Extractor: &countingContentExtractor{}, LocalCorpus: index, LocalArtifacts: artifacts,
		LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModeArchive},
	}
	options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
	for attempt := 0; attempt < 2; attempt++ {
		result := pipeline.Parse(context.Background(), options)
		if len(result) != 1 || result[0].Content == nil {
			t.Fatalf("attempt %d result=%+v", attempt, result)
		}
	}
	history, ok, err := artifacts.History(context.Background(), rawURL)
	if err != nil || !ok || len(history.Versions) != 2 {
		t.Fatalf("history=%+v ok=%t err=%v", history, ok, err)
	}
	current, ok, err := artifacts.Current(context.Background(), rawURL)
	if err != nil || !ok || string(current.Body) != "current beta only" || current.ExpiresAt != nil {
		t.Fatalf("current=%+v ok=%t err=%v", current, ok, err)
	}
	oldBatch, err := index.SearchBatch(context.Background(), search.Query{Q: "historic alpha", MaxResults: 10})
	if err != nil || len(oldBatch.Hits) != 0 {
		t.Fatalf("historical index batch=%+v err=%v", oldBatch, err)
	}
	currentBatch, err := index.SearchBatch(context.Background(), search.Query{Q: "current beta", MaxResults: 10})
	if err != nil || len(currentBatch.Hits) != 1 || currentBatch.Hits[0].URL != rawURL {
		t.Fatalf("current index batch=%+v err=%v", currentBatch, err)
	}
}

func TestArchiveConditional304ReusesStoredSourceWithoutAddingVersion(t *testing.T) {
	const rawURL = "https://example.com/archive-revalidated"
	const lastModified = "Fri, 18 Jul 2025 10:00:00 GMT"
	base := time.Now().UTC().Add(-time.Hour)
	fetches := &conditionalSequenceFetcher{responses: []fetcher.Result{
		{Status: 200, Body: []byte("archive retained source"), ContentType: "text/html", ETag: `W/"one"`, LastModified: lastModified,
			ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
		{Status: 304, NotModified: true, ETag: `W/"one"`, LastModified: lastModified,
			ObservedAt: base.Add(time.Minute), RobotsAllowed: true, RobotsAuthoritative: true},
		{Status: 200, Body: []byte("unexpected unconditional source"), ContentType: "text/html", ETag: `W/"two"`,
			ObservedAt: base.Add(2 * time.Minute), RobotsAllowed: true, RobotsAuthoritative: true},
	}}
	index := openPipelineIndex(t)
	artifacts, err := artifactfs.Open(artifactfs.Options{
		Path: filepath.Join(t.TempDir(), "archive"), MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10,
		MaxEntries: 10, Archive: true, MaxVersions: 10, MaxVersionsPerURL: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = artifacts.Close() })
	pipeline := &Pipeline{
		Fetcher: fetches, Extractor: &countingContentExtractor{}, LocalCorpus: index, LocalArtifacts: artifacts,
		LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModeArchive},
	}
	options := Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true, UserAgent: "Fetchmark-Test/1"}
	first := pipeline.Parse(context.Background(), options)
	second := pipeline.Parse(context.Background(), options)
	if len(first) != 1 || first[0].Content == nil || len(second) != 1 || second[0].Content == nil {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	if len(fetches.requests) != 2 || fetches.requests[1].IfNoneMatch != `W/"one"` || fetches.requests[1].IfModifiedSince != lastModified {
		t.Fatalf("requests=%+v", fetches.requests)
	}
	if second[0].Content.MainText != "archive retained source" || !second[0].FromCache {
		t.Fatalf("second=%+v", second)
	}
	history, ok, err := artifacts.History(context.Background(), rawURL)
	if err != nil || !ok || len(history.Versions) != 1 {
		t.Fatalf("history=%+v ok=%t err=%v", history, ok, err)
	}
	current, ok, err := artifacts.Current(context.Background(), rawURL)
	if err != nil || !ok || current.ObservedAt != base.Add(time.Minute) || current.ExpiresAt != nil || string(current.Body) != "archive retained source" {
		t.Fatalf("current=%+v ok=%t err=%v", current, ok, err)
	}
	batch, err := index.SearchBatch(context.Background(), search.Query{Q: "archive retained", MaxResults: 10})
	if err != nil || len(batch.Hits) != 1 || batch.Hits[0].URL != rawURL {
		t.Fatalf("index batch=%+v err=%v", batch, err)
	}
	indexedAt, err := time.Parse(time.RFC3339, batch.Hits[0].Metadata["fetched_at"])
	if err != nil || !indexedAt.Equal(base.Add(time.Minute).Truncate(time.Second)) {
		t.Fatalf("indexed fetched_at=%q parsed=%v err=%v", batch.Hits[0].Metadata["fetched_at"], indexedAt, err)
	}
}

func personalPipeline(t *testing.T, fetches Fetcher, extractor Extractor) (*Pipeline, *bleveindex.Index, *artifactfs.Store) {
	t.Helper()
	index := openPipelineIndex(t)
	artifacts, err := artifactfs.Open(artifactfs.Options{
		Path: filepath.Join(t.TempDir(), "artifacts"), MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = artifacts.Close() })
	return &Pipeline{
		Fetcher: fetches, Extractor: extractor, LocalCorpus: index, LocalArtifacts: artifacts,
		LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModePersonal, MaxAge: 24 * time.Hour},
	}, index, artifacts
}

func openPipelineIndex(t *testing.T) *bleveindex.Index {
	t.Helper()
	index, err := bleveindex.Open(bleveindex.Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	return index
}
