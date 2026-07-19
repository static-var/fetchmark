package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/bleveindex"
	"github.com/staticvar/fetchmark/internal/adapters/extractor"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/model"
)

type retentionRenderer struct{ raw []byte }

func (renderer retentionRenderer) Render(context.Context, string) ([]byte, error) {
	return append([]byte(nil), renderer.raw...), nil
}

type recordingCorpus struct {
	mu         sync.Mutex
	reconciled []localcorpus.Document
	deleted    []string
}

func (corpus *recordingCorpus) Reconcile(_ context.Context, document localcorpus.Document) error {
	corpus.mu.Lock()
	defer corpus.mu.Unlock()
	corpus.reconciled = append(corpus.reconciled, document)
	return nil
}

func (corpus *recordingCorpus) Delete(_ context.Context, rawURL string) error {
	corpus.mu.Lock()
	defer corpus.mu.Unlock()
	corpus.deleted = append(corpus.deleted, rawURL)
	return nil
}

func TestLocalDocumentUsesRawArtifactContentHash(t *testing.T) {
	raw := []byte("<html><body>raw retained representation</body></html>")
	headings := []string{"Overview"}
	outboundLinks := []string{"https://example.com/reference"}
	fetched := fetcher.Result{Body: raw, ContentType: "text/html", ObservedAt: time.Now().UTC()}
	document := localDocument(
		&model.SearchResult{URL: "https://example.com/hash", Engines: []string{"wikipedia"}},
		&model.Content{MainText: "raw retained representation", Headings: headings, OutboundLinks: outboundLinks},
		fetched, localcorpus.DispositionPermitted,
	)
	want := sha256.Sum256(raw)
	if document.ContentHash != hex.EncodeToString(want[:]) {
		t.Fatalf("content hash = %q, want %x", document.ContentHash, want)
	}
	if len(document.Headings) != 1 || document.Headings[0] != "Overview" {
		t.Fatalf("headings = %#v", document.Headings)
	}
	if len(document.OutboundLinks) != 1 || document.OutboundLinks[0] != "https://example.com/reference" {
		t.Fatalf("outbound links = %#v", document.OutboundLinks)
	}
	headings[0] = "mutated"
	outboundLinks[0] = "https://example.com/mutated"
	if document.Headings[0] != "Overview" || document.OutboundLinks[0] != "https://example.com/reference" {
		t.Fatalf("document metadata aliases extractor-owned slices: %+v", document)
	}
}

func TestRedirectedContentUsesEffectiveURLForMetadataButPreservesPublicURL(t *testing.T) {
	const requestedURL = "https://old.example/old"
	const effectiveURL = "https://new.example/dir/page"
	body := `<html><head><title>Redirected Article</title></head><body><article>` +
		`<h1>Redirected Article</h1><p>` + strings.Repeat("substantial redirected article text ", 30) +
		`<a href="../next#details">Next page</a></p></article></body></html>`
	corpus := &recordingCorpus{}
	pipeline := &Pipeline{
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{requestedURL: {
			Status: 200, FinalURL: effectiveURL, Body: []byte(body), ContentType: "text/html",
			UAUsed: "Fetchmark/0.1", RobotsAllowed: true, RobotsAuthoritative: true,
		}}},
		Extractor: extractor.New(true), LocalCorpus: corpus,
	}
	results := pipeline.Parse(context.Background(), Options{URLs: []string{requestedURL}, MaxResults: 1})
	if len(results) != 1 || results[0].Content == nil || results[0].Content.URL != requestedURL {
		t.Fatalf("results = %+v", results)
	}
	if len(corpus.reconciled) != 1 {
		t.Fatalf("reconciled = %+v", corpus.reconciled)
	}
	want := "https://new.example/next"
	if links := corpus.reconciled[0].OutboundLinks; len(links) != 1 || links[0] != want {
		t.Fatalf("outbound links = %#v, want %q; cleaned HTML = %q", links, want, results[0].Content.CleanedHTML)
	}
}

func TestColdFetchFeedsLocalCorpusOnlyWithAffirmativePermission(t *testing.T) {
	const rawURL = "https://example.com/article"
	tests := []struct {
		name            string
		response        fetcher.Result
		wantDisposition localcorpus.IndexingDisposition
	}{
		{
			name: "permitted", response: fetcher.Result{Status: 200, ContentType: "text/html", Body: []byte("permitted body"),
				UAUsed: "Fetchmark/0.1", RobotsAllowed: true, RobotsAuthoritative: true}, wantDisposition: localcorpus.DispositionPermitted,
		},
		{
			name: "header noindex", response: fetcher.Result{Status: 200, Body: []byte("header denied"), XRobotsTag: []string{"noindex"},
				UAUsed: "Fetchmark/0.1", RobotsAllowed: true, RobotsAuthoritative: true}, wantDisposition: localcorpus.DispositionNoIndexHeader,
		},
		{
			name: "metadata noindex", response: fetcher.Result{Status: 200, Body: []byte(`<meta name="robots" content="noindex"><p>denied</p>`),
				UAUsed: "Fetchmark/0.1", RobotsAllowed: true, RobotsAuthoritative: true}, wantDisposition: localcorpus.DispositionNoIndexMetadata,
		},
		{
			name: "robots uncertain", response: fetcher.Result{Status: 200, Body: []byte("uncertain"),
				UAUsed: "Fetchmark/0.1", RobotsAllowed: true, RobotsAuthoritative: false}, wantDisposition: localcorpus.DispositionUnknown,
		},
		{
			name: "robots blocked", response: fetcher.Result{Status: 200, Unsupported: fetcher.ReasonRobots,
				UAUsed: "Fetchmark/0.1", RobotsAllowed: false, RobotsAuthoritative: true}, wantDisposition: localcorpus.DispositionRobotsBlocked,
		},
		{
			name: "unsupported body with header noindex", response: fetcher.Result{Status: 200, Unsupported: fetcher.ReasonNonHTML,
				XRobotsTag: []string{"noindex"}, UAUsed: "Fetchmark/0.1", RobotsAllowed: true, RobotsAuthoritative: true},
			wantDisposition: localcorpus.DispositionNoIndexHeader,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			corpus := &recordingCorpus{}
			pipeline := &Pipeline{
				Fetcher: stubFetcher{resp: map[string]fetcher.Result{rawURL: test.response}}, Extractor: stubExtractor{}, LocalCorpus: corpus,
			}
			results := pipeline.Parse(context.Background(), Options{URLs: []string{rawURL}, MaxResults: 1})
			if len(results) != 1 {
				t.Fatalf("results = %+v", results)
			}
			if len(corpus.reconciled) != 1 || corpus.reconciled[0].IndexingDisposition != test.wantDisposition {
				t.Fatalf("reconciled = %+v", corpus.reconciled)
			}
			if test.wantDisposition == localcorpus.DispositionPermitted && corpus.reconciled[0].Body == "" {
				t.Fatalf("permitted document = %+v", corpus.reconciled[0])
			}
			if test.wantDisposition != localcorpus.DispositionPermitted && corpus.reconciled[0].Body != "" {
				t.Fatalf("tombstone retained body = %+v", corpus.reconciled[0])
			}
		})
	}
}

type orderedRetentionFetcher struct {
	started chan struct{}
	release chan struct{}
	calls   atomic.Int64
	base    time.Time
}

type sequenceRetentionFetcher struct {
	responses []fetcher.Result
	next      atomic.Int64
}

func (sequence *sequenceRetentionFetcher) Fetch(_ context.Context, request fetcher.Request) fetcher.Result {
	index := int(sequence.next.Add(1) - 1)
	response := sequence.responses[index]
	response.URL = request.URL
	return response
}

func (ordered *orderedRetentionFetcher) Fetch(_ context.Context, request fetcher.Request) fetcher.Result {
	if ordered.calls.Add(1) == 1 {
		close(ordered.started)
		<-ordered.release
		return fetcherResult(request.URL, ordered.base, nil)
	}
	return fetcherResult(request.URL, ordered.base.Add(time.Minute), []string{"noindex"})
}

func fetcherResult(rawURL string, observedAt time.Time, xRobotsTag []string) fetcher.Result {
	return fetcher.Result{
		URL: rawURL, Status: 200, Body: []byte("ordered retention body"), ObservedAt: observedAt,
		XRobotsTag: xRobotsTag, UAUsed: "Fetchmark/0.1", RobotsAllowed: true, RobotsAuthoritative: true,
	}
}

func TestOlderPermittedFetchFinishingLastCannotUndoNewerNoIndex(t *testing.T) {
	index, err := bleveindex.Open(bleveindex.Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	ordered := &orderedRetentionFetcher{
		started: make(chan struct{}), release: make(chan struct{}), base: time.Now().UTC().Add(-time.Hour),
	}
	pipeline := &Pipeline{Fetcher: ordered, Extractor: stubExtractor{}, LocalCorpus: index}
	const rawURL = "https://example.com/ordered-pipeline"
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		pipeline.Parse(context.Background(), Options{URLs: []string{rawURL}, MaxResults: 1})
	}()
	<-ordered.started
	pipeline.Parse(context.Background(), Options{URLs: []string{rawURL}, MaxResults: 1})
	close(ordered.release)
	<-firstDone

	count, err := index.Count()
	if err != nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestUnsupportedResponseHeaderNoIndexRevokesPriorContent(t *testing.T) {
	index, err := bleveindex.Open(bleveindex.Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	base := time.Now().UTC().Add(-time.Hour)
	sequence := &sequenceRetentionFetcher{responses: []fetcher.Result{
		fetcherResult("", base, nil),
		{
			Status: 200, Unsupported: fetcher.ReasonTooLarge, ObservedAt: base.Add(time.Minute),
			XRobotsTag: []string{"noindex"}, UAUsed: "Fetchmark/0.1", RobotsAllowed: true, RobotsAuthoritative: true,
		},
	}}
	pipeline := &Pipeline{Fetcher: sequence, Extractor: stubExtractor{}, LocalCorpus: index}
	const rawURL = "https://example.com/unsupported-noindex"
	pipeline.Parse(context.Background(), Options{URLs: []string{rawURL}, MaxResults: 1})
	count, err := index.Count()
	if err != nil || count != 1 {
		t.Fatalf("initial count=%d err=%v", count, err)
	}
	pipeline.Parse(context.Background(), Options{URLs: []string{rawURL}, MaxResults: 1})
	count, err = index.Count()
	if err != nil || count != 0 {
		t.Fatalf("revoked count=%d err=%v", count, err)
	}
}

func TestRendererOnlyRevokesOnExplicitMetadataRetentionDirective(t *testing.T) {
	const rawURL = "https://example.com/rendered"
	for _, test := range []struct {
		name string
		raw  string
		want localcorpus.IndexingDisposition
	}{
		{name: "absence does not purge", raw: "<html><body>rendered content</body></html>"},
		{name: "noindex revokes", raw: `<meta name="robots" content="noindex"><p>rendered</p>`, want: localcorpus.DispositionNoIndexMetadata},
		{name: "noarchive revokes", raw: `<meta name="robots" content="noarchive"><p>rendered</p>`, want: localcorpus.DispositionNoArchiveMetadata},
	} {
		t.Run(test.name, func(t *testing.T) {
			corpus := &recordingCorpus{}
			pipeline := &Pipeline{Renderer: retentionRenderer{raw: []byte(test.raw)}, Extractor: stubExtractor{}, LocalCorpus: corpus}
			result := &model.SearchResult{URL: rawURL}
			if _, err := pipeline.renderAndExtract(context.Background(), Options{UserAgent: "Fetchmark/0.1"}, result, "unused", true); err != nil {
				t.Fatal(err)
			}
			if (len(corpus.reconciled) == 1) != (test.want != "") {
				t.Fatalf("reconciled = %+v", corpus.reconciled)
			}
			if test.want != "" && corpus.reconciled[0].IndexingDisposition != test.want {
				t.Fatalf("tombstone = %+v", corpus.reconciled[0])
			}
		})
	}
}
