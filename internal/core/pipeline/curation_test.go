package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/bleveindex"
	cacheadapter "github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	artifactfs "github.com/staticvar/fetchmark/internal/adapters/localartifact"
	"github.com/staticvar/fetchmark/internal/core/localartifact"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/model"
	"github.com/staticvar/fetchmark/internal/core/search"
)

func TestAdmitCuratedFreshFetchPersistsWithoutExpiryAndBypassesCache(t *testing.T) {
	const rawURL = "https://example.com/curated"
	var calls atomic.Int64
	responseCache := cacheadapter.New(nil, time.Hour)
	t.Cleanup(responseCache.Close)
	cached, err := json.Marshal(model.Content{URL: rawURL, MainText: "stale cache body"})
	if err != nil {
		t.Fatal(err)
	}
	if err := responseCache.Set(context.Background(), cacheadapter.ArtifactKey(rawURL), cached); err != nil {
		t.Fatal(err)
	}
	pipeline, index, artifacts := curatedPipeline(t, stubFetcher{calls: &calls, resp: map[string]fetcher.Result{rawURL: {
		Status: 200, FinalURL: rawURL, Body: []byte("fresh curated source"), ContentType: "text/html",
		UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true,
	}}}, stubExtractor{})
	pipeline.Cache = responseCache

	mutations := pipeline.AdmitCurated(context.Background(), []string{rawURL})
	if len(mutations) != 1 || mutations[0].Status != CuratedStatusAdmitted || mutations[0].Reason != "" {
		t.Fatalf("mutations = %+v", mutations)
	}
	if calls.Load() != 1 {
		t.Fatalf("fetch calls = %d, want fresh fetch", calls.Load())
	}
	batch, err := index.SearchBatch(context.Background(), search.Query{Q: "fresh curated", MaxResults: 1})
	if err != nil || len(batch.Hits) != 1 {
		t.Fatalf("index batch=%+v err=%v", batch, err)
	}
	artifact, ok, err := artifacts.Current(context.Background(), rawURL)
	if err != nil || !ok || string(artifact.Body) != "fresh curated source" || artifact.ExpiresAt != nil {
		t.Fatalf("artifact=%+v ok=%t err=%v", artifact, ok, err)
	}
}

type focusedRequestCapture struct {
	request fetcher.Request
	result  fetcher.Result
}

type outboundLinkExtractor struct {
	links []string
}

func (extractor outboundLinkExtractor) Extract(raw []byte, pageURL string) (*model.Content, error) {
	return &model.Content{
		URL: pageURL, Title: "T", MainText: string(raw), OutboundLinks: append([]string(nil), extractor.links...),
	}, nil
}

func (capture *focusedRequestCapture) Fetch(_ context.Context, request fetcher.Request) fetcher.Result {
	capture.request = request
	result := capture.result
	result.URL = request.URL
	return result
}

func TestFocusedAdmissionEnablesFetcherRedirectBoundaryOnlyForCrawlerPath(t *testing.T) {
	const rawURL = "https://example.com/docs/page"
	capture := &focusedRequestCapture{result: fetcher.Result{
		Status: 200, FinalURL: rawURL, Body: []byte("focused body"), ContentType: "text/html",
		UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true,
	}}
	pipeline, _, _ := curatedPipeline(t, capture, stubExtractor{})
	mutations := pipeline.AdmitCuratedFocused(context.Background(), FocusedAdmission{
		URL: rawURL, AllowedPathPrefixes: []string{"/docs/"},
	})
	if len(mutations) != 1 || mutations[0].Status != CuratedStatusAdmitted {
		t.Fatalf("focused mutations = %+v", mutations)
	}
	if capture.request.FocusedRedirectScope == nil || len(capture.request.FocusedRedirectScope.AllowedPathPrefixes) != 1 {
		t.Fatal("focused admission did not enable strict redirect scope")
	}

	mutations = pipeline.AdmitCurated(context.Background(), []string{rawURL})
	if len(mutations) != 1 || mutations[0].Status != CuratedStatusAdmitted {
		t.Fatalf("ordinary mutations = %+v", mutations)
	}
	if capture.request.FocusedRedirectScope != nil {
		t.Fatal("ordinary curated admission unexpectedly enabled crawler redirect scope")
	}
}

func TestFocusedAdmissionReturnsBoundedOutboundLinkSnapshotOnlyWhenRequested(t *testing.T) {
	const rawURL = "https://example.com/docs/page"
	links := make([]string, 0, MaxFocusedOutboundLinks+5)
	for index := 0; index < MaxFocusedOutboundLinks+5; index++ {
		links = append(links, "https://reference.example/item/"+strconv.Itoa(index))
	}
	pipeline, _, _ := curatedPipeline(t, stubFetcher{resp: map[string]fetcher.Result{rawURL: {
		Status: 200, FinalURL: rawURL, Body: []byte("focused body"), ContentType: "text/html",
		UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true,
	}}}, outboundLinkExtractor{links: links})

	mutations := pipeline.AdmitCuratedFocused(context.Background(), FocusedAdmission{
		URL: rawURL, AllowedPathPrefixes: []string{"/docs/"}, MaxOutboundLinks: 3,
	})
	if len(mutations) != 1 || mutations[0].Status != CuratedStatusAdmitted || mutations[0].OutboundLinks == nil {
		t.Fatalf("focused mutations = %+v", mutations)
	}
	got := *mutations[0].OutboundLinks
	if len(got) != 3 || got[0] != links[0] || got[2] != links[2] {
		t.Fatalf("outbound links = %#v", got)
	}

	mutations = pipeline.AdmitCuratedFocused(context.Background(), FocusedAdmission{
		URL: rawURL, AllowedPathPrefixes: []string{"/docs/"}, MaxOutboundLinks: MaxFocusedOutboundLinks,
	})
	if mutations[0].OutboundLinks == nil || len(*mutations[0].OutboundLinks) != MaxFocusedOutboundLinks {
		t.Fatalf("capped outbound links = %+v", mutations[0].OutboundLinks)
	}
}

func TestFocusedAdmissionPreservesOmittedAndAuthoritativeEmptyLinkSnapshots(t *testing.T) {
	const rawURL = "https://example.com/docs/page"
	pipeline, _, _ := curatedPipeline(t, stubFetcher{resp: map[string]fetcher.Result{rawURL: {
		Status: 200, FinalURL: rawURL, Body: []byte("focused body"), ContentType: "text/html",
		UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true,
	}}}, outboundLinkExtractor{})

	withoutSnapshot := pipeline.AdmitCuratedFocused(context.Background(), FocusedAdmission{
		URL: rawURL, AllowedPathPrefixes: []string{"/docs/"}, MaxOutboundLinks: 0,
	})
	if len(withoutSnapshot) != 1 || withoutSnapshot[0].OutboundLinks != nil {
		t.Fatalf("zero-cap focused result = %+v", withoutSnapshot)
	}
	withEmptySnapshot := pipeline.AdmitCuratedFocused(context.Background(), FocusedAdmission{
		URL: rawURL, AllowedPathPrefixes: []string{"/docs/"}, MaxOutboundLinks: 4,
	})
	if len(withEmptySnapshot) != 1 || withEmptySnapshot[0].OutboundLinks == nil || *withEmptySnapshot[0].OutboundLinks == nil || len(*withEmptySnapshot[0].OutboundLinks) != 0 {
		t.Fatalf("authoritative-empty focused result = %+v", withEmptySnapshot)
	}
	wire, err := json.Marshal(map[string]any{"count": len(withEmptySnapshot), "results": withEmptySnapshot})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(wire), `"outbound_links":[]`) {
		t.Fatalf("authoritative-empty wire result = %s", wire)
	}
}

func TestFocusedAdmissionHonorsPageLevelNoFollow(t *testing.T) {
	tests := []struct {
		name   string
		header []string
		body   string
	}{
		{name: "header", header: []string{"nofollow"}, body: "focused body"},
		{name: "metadata", body: `<html><head><meta name="robots" content="nofollow"></head><body>focused body</body></html>`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const rawURL = "https://example.com/docs/page"
			pipeline, _, _ := curatedPipeline(t, stubFetcher{resp: map[string]fetcher.Result{rawURL: {
				Status: 200, FinalURL: rawURL, Body: []byte(test.body), ContentType: "text/html",
				UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true,
				XRobotsTag: test.header,
			}}}, outboundLinkExtractor{links: []string{"https://example.com/docs/next"}})
			mutations := pipeline.AdmitCuratedFocused(context.Background(), FocusedAdmission{
				URL: rawURL, AllowedPathPrefixes: []string{"/docs/"}, MaxOutboundLinks: 4,
			})
			if len(mutations) != 1 || mutations[0].Status != CuratedStatusAdmitted || mutations[0].OutboundLinks == nil || len(*mutations[0].OutboundLinks) != 0 {
				t.Fatalf("mutations=%+v", mutations)
			}
		})
	}
}

func TestOutboundLinksRemainAbsentFromOrdinaryAndRejectedAdmissions(t *testing.T) {
	const admittedURL = "https://example.com/docs/admitted"
	const rejectedURL = "https://example.com/docs/rejected"
	pipeline, _, _ := curatedPipeline(t, stubFetcher{resp: map[string]fetcher.Result{
		admittedURL: {
			Status: 200, FinalURL: admittedURL, Body: []byte("ordinary body"), ContentType: "text/html",
			UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true,
		},
		rejectedURL: {
			Status: 200, FinalURL: rejectedURL, Unsupported: fetcher.ReasonRobots,
			UAUsed: "Fetchmark-Curator/1", RobotsAllowed: false, RobotsAuthoritative: true,
		},
	}}, outboundLinkExtractor{links: []string{"https://reference.example/"}})

	ordinary := pipeline.AdmitCurated(context.Background(), []string{admittedURL})
	if len(ordinary) != 1 || ordinary[0].Status != CuratedStatusAdmitted || ordinary[0].OutboundLinks != nil {
		t.Fatalf("ordinary admission = %+v", ordinary)
	}
	rejected := pipeline.AdmitCuratedFocused(context.Background(), FocusedAdmission{
		URL: rejectedURL, AllowedPathPrefixes: []string{"/docs/"}, MaxOutboundLinks: 4,
	})
	if len(rejected) != 1 || rejected[0].Status != CuratedStatusRejected || rejected[0].OutboundLinks != nil {
		t.Fatalf("rejected focused admission = %+v", rejected)
	}
}

func TestOrdinaryParseCannotAdmitPermittedContentInCuratedMode(t *testing.T) {
	const rawURL = "https://example.com/ordinary"
	pipeline, index, artifacts := curatedPipeline(t, stubFetcher{resp: map[string]fetcher.Result{rawURL: {
		Status: 200, FinalURL: rawURL, Body: []byte("ordinary response"), ContentType: "text/html",
		UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true,
	}}}, stubExtractor{})
	results := pipeline.Parse(context.Background(), Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true})
	if len(results) != 1 || results[0].Content == nil {
		t.Fatalf("results = %+v", results)
	}
	if count, err := index.Count(); err != nil || count != 0 {
		t.Fatalf("index count=%d err=%v", count, err)
	}
	if _, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok {
		t.Fatalf("artifact ok=%t err=%v", ok, err)
	}
}

func TestOrdinaryDeniedURLsCannotConsumeCuratedTombstoneCapacity(t *testing.T) {
	const deniedURL = "https://attacker.example/denied"
	const admittedURL = "https://example.com/admitted"
	index, err := bleveindex.Open(bleveindex.Options{InMemory: true, MaxDocumentBytes: 64 << 10, MaxDocuments: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	artifacts := openCuratedArtifacts(t)
	pipeline := &Pipeline{
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{
			deniedURL: {
				Status: 200, FinalURL: deniedURL, Unsupported: fetcher.ReasonRobots,
				UAUsed: "Fetchmark-Curator/1", RobotsAllowed: false, RobotsAuthoritative: true,
			},
			admittedURL: {
				Status: 200, FinalURL: admittedURL, Body: []byte("legitimate curated content"), ContentType: "text/html",
				UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true,
			},
		}},
		Extractor: stubExtractor{}, LocalCorpus: index, LocalArtifacts: artifacts,
		LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModeCurated}, RobotsUserAgent: "Fetchmark-Curator/1",
	}
	pipeline.Parse(context.Background(), Options{URLs: []string{deniedURL}, MaxResults: 1, RespectRobots: true})
	mutations := pipeline.AdmitCurated(context.Background(), []string{admittedURL})
	if len(mutations) != 1 || mutations[0].Status != CuratedStatusAdmitted {
		t.Fatalf("mutations = %+v", mutations)
	}
}

func TestCuratedAdmissionDoesNotUseExtractedContentOutputBudget(t *testing.T) {
	const rawURL = "https://example.com/bodyless-output"
	pipeline, index, _ := curatedPipeline(t, stubFetcher{resp: map[string]fetcher.Result{rawURL: {
		Status: 200, FinalURL: rawURL, Body: []byte("content larger than one output byte"), ContentType: "text/html",
		UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true,
	}}}, stubExtractor{})
	pipeline.MaxRequestOutputBytes = 1
	pipeline.MaxRequestSourceBytes = 1 << 20
	mutations := pipeline.AdmitCurated(context.Background(), []string{rawURL})
	if len(mutations) != 1 || mutations[0].Status != CuratedStatusAdmitted {
		t.Fatalf("mutations = %+v", mutations)
	}
	if count, err := index.Count(); err != nil || count != 1 {
		t.Fatalf("index count=%d err=%v", count, err)
	}
}

func TestCuratedAdmissionPersistsOnlyExplicitSafetyAssertion(t *testing.T) {
	const safeURL = "https://example.com/operator-reviewed"
	const defaultURL = "https://example.com/unclassified"
	pipeline, index, artifacts := curatedPipeline(t, stubFetcher{resp: map[string]fetcher.Result{
		safeURL: {
			Status: 200, FinalURL: safeURL, Body: []byte("operator reviewed safety corpus"), ContentType: "text/html",
			UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true,
		},
		defaultURL: {
			Status: 200, FinalURL: defaultURL, Body: []byte("default unclassified corpus"), ContentType: "text/html",
			UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true,
		},
	}}, stubExtractor{})
	if got := pipeline.AdmitCuratedClassified(context.Background(), []string{safeURL}, localcorpus.SafetySafe); len(got) != 1 || got[0].Status != CuratedStatusAdmitted {
		t.Fatalf("classified admission = %+v", got)
	}
	if got := pipeline.AdmitCurated(context.Background(), []string{defaultURL}); len(got) != 1 || got[0].Status != CuratedStatusAdmitted {
		t.Fatalf("default admission = %+v", got)
	}
	safeArtifact, ok, err := artifacts.Current(context.Background(), safeURL)
	if err != nil || !ok || safeArtifact.SafetyClassification != localcorpus.SafetySafe {
		t.Fatalf("safe artifact=%+v ok=%t err=%v", safeArtifact, ok, err)
	}
	defaultArtifact, ok, err := artifacts.Current(context.Background(), defaultURL)
	if err != nil || !ok || defaultArtifact.SafetyClassification != localcorpus.SafetyUnclassified {
		t.Fatalf("default artifact=%+v ok=%t err=%v", defaultArtifact, ok, err)
	}
	strict := 2
	batch, err := index.SearchBatch(context.Background(), search.Query{Q: "safety corpus", SafeSearch: &strict, MaxResults: 10})
	if err != nil || len(batch.Hits) != 1 || batch.Hits[0].URL != safeURL {
		t.Fatalf("strict batch=%+v err=%v", batch, err)
	}
	if got := pipeline.AdmitCuratedClassified(context.Background(), []string{safeURL}, "guessed"); len(got) != 1 || got[0].Reason != CuratedReasonInvalidSafety {
		t.Fatalf("invalid classification admission = %+v", got)
	}
}

func TestOrdinaryPolicyObservationStillRevokesExistingCuratedContent(t *testing.T) {
	const rawURL = "https://example.com/policy-change"
	sequence := &sequenceRetentionFetcher{responses: []fetcher.Result{
		{Status: 200, FinalURL: rawURL, Body: []byte("initial curated body"), ContentType: "text/html", RobotsAllowed: true, RobotsAuthoritative: true},
		{Status: 200, FinalURL: rawURL, Body: []byte("now denied"), ContentType: "text/html", XRobotsTag: []string{"noindex"}, RobotsAllowed: true, RobotsAuthoritative: true},
	}}
	pipeline, index, artifacts := curatedPipeline(t, sequence, stubExtractor{})
	if got := pipeline.AdmitCurated(context.Background(), []string{rawURL}); len(got) != 1 || got[0].Status != CuratedStatusAdmitted {
		t.Fatalf("admission = %+v", got)
	}
	pipeline.Parse(context.Background(), Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true})
	if count, err := index.Count(); err != nil || count != 0 {
		t.Fatalf("index count=%d err=%v", count, err)
	}
	if _, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok {
		t.Fatalf("artifact ok=%t err=%v", ok, err)
	}
}

func TestOrdinaryPolicyObservationRevokesArtifactOnlyCuratedState(t *testing.T) {
	const rawURL = "https://example.com/artifact-only-policy-change"
	base := time.Now().UTC().Add(-time.Minute)
	index, err := bleveindex.Open(bleveindex.Options{InMemory: true, MaxDocumentBytes: 64 << 10, MaxDocuments: 10, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = index.Close() })
	artifacts := openCuratedArtifacts(t)
	if err := artifacts.Put(context.Background(), localartifact.Version{
		URL: rawURL, Body: []byte("artifact survived an index rebuild"), FetchedAt: base, ObservedAt: base,
		ValidatedAt: base, IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatal(err)
	}
	pipeline := &Pipeline{
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{rawURL: {
			Status: 200, FinalURL: rawURL, Body: []byte("now denied"), ContentType: "text/html",
			XRobotsTag: []string{"noindex"}, UAUsed: "Fetchmark-Curator/1",
			RobotsAllowed: true, RobotsAuthoritative: true, ObservedAt: base.Add(time.Second),
		}}},
		Extractor: stubExtractor{}, LocalCorpus: index, LocalArtifacts: artifacts,
		LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModeCurated}, RobotsUserAgent: "Fetchmark-Curator/1",
	}

	pipeline.Parse(context.Background(), Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true})
	if count, err := index.Count(); err != nil || count != 0 {
		t.Fatalf("index count=%d err=%v", count, err)
	}
	if _, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok {
		t.Fatalf("artifact ok=%t err=%v", ok, err)
	}
}

func TestNewerOrdinaryDenialCannotRacePastOlderCuratedAdmission(t *testing.T) {
	const rawURL = "https://example.com/concurrent-policy-change"
	base := time.Now().UTC().Add(-time.Minute)
	controlled := &orderedCuratedFetcher{
		firstStarted:  make(chan struct{}),
		releaseFirst:  make(chan struct{}),
		secondStarted: make(chan struct{}),
		responses: []fetcher.Result{
			{
				Status: 200, FinalURL: rawURL, Body: []byte("older permitted body"), ContentType: "text/html",
				UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true, ObservedAt: base,
			},
			{
				Status: 200, FinalURL: rawURL, Body: []byte("newer denied body"), ContentType: "text/html",
				XRobotsTag: []string{"noindex"}, UAUsed: "Fetchmark-Curator/1",
				RobotsAllowed: true, RobotsAuthoritative: true, ObservedAt: base.Add(time.Second),
			},
		},
	}
	pipeline, index, artifacts := curatedPipeline(t, controlled, stubExtractor{})
	admissionDone := make(chan []CuratedMutationResult, 1)
	go func() {
		admissionDone <- pipeline.AdmitCurated(context.Background(), []string{rawURL})
	}()
	<-controlled.firstStarted

	ordinaryDone := make(chan struct{})
	go func() {
		pipeline.Parse(context.Background(), Options{URLs: []string{rawURL}, MaxResults: 1, RespectRobots: true})
		close(ordinaryDone)
	}()
	select {
	case <-controlled.secondStarted:
		close(controlled.releaseFirst)
		t.Fatal("newer denial fetched before the older admission lifecycle completed")
	case <-time.After(100 * time.Millisecond):
	}
	close(controlled.releaseFirst)

	select {
	case admission := <-admissionDone:
		if len(admission) != 1 || admission[0].Status != CuratedStatusAdmitted {
			t.Fatalf("admission = %+v", admission)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("admission did not complete")
	}
	select {
	case <-ordinaryDone:
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary denial did not complete")
	}
	if count, err := index.Count(); err != nil || count != 0 {
		t.Fatalf("index count=%d err=%v", count, err)
	}
	if _, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok {
		t.Fatalf("artifact ok=%t err=%v", ok, err)
	}
	pipeline.curatedMutationLocks.mu.Lock()
	lockEntries := len(pipeline.curatedMutationLocks.entries)
	pipeline.curatedMutationLocks.mu.Unlock()
	if lockEntries != 0 {
		t.Fatalf("curated mutation lock entries=%d, want 0", lockEntries)
	}
}

func TestAdmitCuratedRejectsNonPermittedRetrievals(t *testing.T) {
	const rawURL = "https://example.com/rejected"
	tests := []struct {
		name       string
		response   fetcher.Result
		wantReason string
	}{
		{name: "robots unknown", response: fetcher.Result{Status: 200, Body: []byte("body"), RobotsAllowed: true}, wantReason: CuratedReasonRobotsUnknown},
		{name: "robots blocked", response: fetcher.Result{Status: 200, Unsupported: fetcher.ReasonRobots, RobotsAllowed: false, RobotsAuthoritative: true}, wantReason: CuratedReasonRobotsBlocked},
		{name: "header noindex", response: fetcher.Result{Status: 200, Body: []byte("body"), XRobotsTag: []string{"noindex"}, RobotsAllowed: true, RobotsAuthoritative: true}, wantReason: CuratedReasonNoIndex},
		{name: "header noarchive", response: fetcher.Result{Status: 200, Body: []byte("body"), XRobotsTag: []string{"noarchive"}, RobotsAllowed: true, RobotsAuthoritative: true}, wantReason: CuratedReasonNoArchive},
		{name: "metadata noindex", response: fetcher.Result{Status: 200, Body: []byte(`<meta name="robots" content="noindex"><p>body</p>`), RobotsAllowed: true, RobotsAuthoritative: true}, wantReason: CuratedReasonNoIndex},
		{name: "metadata noarchive", response: fetcher.Result{Status: 200, Body: []byte(`<meta name="robots" content="noarchive"><p>body</p>`), RobotsAllowed: true, RobotsAuthoritative: true}, wantReason: CuratedReasonNoArchive},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := test.response
			response.FinalURL = rawURL
			response.ContentType = "text/html"
			response.UAUsed = "Fetchmark-Curator/1"
			pipeline, index, artifacts := curatedPipeline(t, stubFetcher{resp: map[string]fetcher.Result{rawURL: response}}, stubExtractor{})
			mutations := pipeline.AdmitCurated(context.Background(), []string{rawURL})
			if len(mutations) != 1 || mutations[0].Status != CuratedStatusRejected || mutations[0].Reason != test.wantReason {
				t.Fatalf("mutations = %+v", mutations)
			}
			if count, err := index.Count(); err != nil || count != 0 {
				t.Fatalf("index count=%d err=%v", count, err)
			}
			if _, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok {
				t.Fatalf("artifact ok=%t err=%v", ok, err)
			}
		})
	}
}

func TestCuratedTakedownIsStickyAgainstReadmission(t *testing.T) {
	const rawURL = "https://example.com/takedown"
	base := time.Now().UTC()
	sequence := &sequenceRetentionFetcher{responses: []fetcher.Result{
		{Status: 200, FinalURL: rawURL, Body: []byte("first admitted body"), ContentType: "text/html", ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
		{Status: 200, FinalURL: rawURL, Body: []byte("later attempted body"), ContentType: "text/html", ObservedAt: base.Add(time.Hour), RobotsAllowed: true, RobotsAuthoritative: true},
	}}
	pipeline, index, artifacts := curatedPipeline(t, sequence, stubExtractor{})
	if got := pipeline.AdmitCurated(context.Background(), []string{rawURL}); len(got) != 1 || got[0].Status != CuratedStatusAdmitted {
		t.Fatalf("initial admission = %+v", got)
	}
	if got := pipeline.TakedownCurated(context.Background(), []string{rawURL}); len(got) != 1 || got[0].Status != CuratedStatusTakenDown {
		t.Fatalf("takedown = %+v", got)
	}
	if got := pipeline.AdmitCurated(context.Background(), []string{rawURL}); len(got) != 1 || got[0].Status != CuratedStatusFailed || got[0].Reason != CuratedReasonStorageFailed {
		t.Fatalf("readmission = %+v", got)
	}
	if count, err := index.Count(); err != nil || count != 0 {
		t.Fatalf("index count=%d err=%v", count, err)
	}
	if _, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok {
		t.Fatalf("artifact ok=%t err=%v", ok, err)
	}
}

type recordingFailCorpus struct {
	mu        sync.Mutex
	documents []localcorpus.Document
	fail      bool
	failAll   bool
}

func (corpus *recordingFailCorpus) Reconcile(_ context.Context, document localcorpus.Document) error {
	corpus.mu.Lock()
	defer corpus.mu.Unlock()
	corpus.documents = append(corpus.documents, document)
	if corpus.failAll || (corpus.fail && document.IndexingDisposition == localcorpus.DispositionPermitted) {
		return errors.New("index failed")
	}
	return nil
}

func (corpus *recordingFailCorpus) Delete(context.Context, string) error { return nil }

func TestCuratedIndexFailureRollsBackArtifactWithUnknownTombstone(t *testing.T) {
	const rawURL = "https://example.com/rollback"
	artifacts := openCuratedArtifacts(t)
	corpus := &recordingFailCorpus{fail: true}
	pipeline := &Pipeline{
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{rawURL: {
			Status: 200, FinalURL: rawURL, Body: []byte("rollback body"), ContentType: "text/html",
			UAUsed: "Fetchmark-Curator/1", RobotsAllowed: true, RobotsAuthoritative: true,
		}}},
		Extractor: stubExtractor{}, LocalCorpus: corpus, LocalArtifacts: artifacts,
		LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModeCurated}, RobotsUserAgent: "Fetchmark-Curator/1",
	}
	mutations := pipeline.AdmitCurated(context.Background(), []string{rawURL})
	if len(mutations) != 1 || mutations[0].Status != CuratedStatusFailed || mutations[0].Reason != CuratedReasonStorageFailed {
		t.Fatalf("mutations = %+v", mutations)
	}
	corpus.mu.Lock()
	documents := append([]localcorpus.Document(nil), corpus.documents...)
	corpus.mu.Unlock()
	if len(documents) != 2 || documents[0].IndexingDisposition != localcorpus.DispositionPermitted || documents[1].IndexingDisposition != localcorpus.DispositionUnknown {
		t.Fatalf("documents = %+v", documents)
	}
	if _, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok {
		t.Fatalf("artifact ok=%t err=%v", ok, err)
	}
}

func TestFailedSafetyDowngradeQuarantinesStaleLocalProjection(t *testing.T) {
	const rawURL = "https://example.com/safety-downgrade"
	base := time.Now().UTC().Add(-time.Minute)
	sequence := &sequenceRetentionFetcher{responses: []fetcher.Result{
		{Status: 200, FinalURL: rawURL, Body: []byte("same reviewed body"), ContentType: "text/html", ObservedAt: base, RobotsAllowed: true, RobotsAuthoritative: true},
		{Status: 200, FinalURL: rawURL, Body: []byte("same reviewed body"), ContentType: "text/html", ObservedAt: base.Add(time.Second), RobotsAllowed: true, RobotsAuthoritative: true},
	}}
	pipeline, index, _ := curatedPipeline(t, sequence, stubExtractor{})
	if got := pipeline.AdmitCuratedClassified(context.Background(), []string{rawURL}, localcorpus.SafetySafe); len(got) != 1 || got[0].Status != CuratedStatusAdmitted {
		t.Fatalf("safe admission = %+v", got)
	}
	pipeline.LocalCorpus = &recordingFailCorpus{failAll: true}
	if got := pipeline.AdmitCuratedClassified(context.Background(), []string{rawURL}, localcorpus.SafetyUnsafe); len(got) != 1 || got[0].Status != CuratedStatusFailed {
		t.Fatalf("unsafe reclassification = %+v", got)
	}
	if !pipeline.localCorpusQuarantined.Load() {
		t.Fatal("local corpus was not quarantined after failed compensation")
	}
	strict := 2
	direct, err := index.SearchBatch(context.Background(), search.Query{Q: "reviewed body", SafeSearch: &strict, MaxResults: 10})
	if err != nil || len(direct.Hits) != 1 {
		t.Fatalf("stale direct projection=%+v err=%v", direct, err)
	}
	pipeline.Searcher = &countingSearch{}
	hits, err := pipeline.searchCandidates(context.Background(), Options{Query: "reviewed body", SafeSearch: &strict}, 10)
	if err != nil || len(hits) != 0 {
		t.Fatalf("quarantined hits=%+v err=%v", hits, err)
	}
}

func TestCuratedTakedownIndexFailureStillPurgesArtifactAndReportsFailure(t *testing.T) {
	const rawURL = "https://example.com/takedown-order"
	artifacts := openCuratedArtifacts(t)
	observedAt := time.Now().UTC().Add(-time.Minute)
	if err := artifacts.Put(context.Background(), localartifact.Version{
		URL: rawURL, EffectiveURL: rawURL, Body: []byte("retained source"), MIME: "text/html",
		PolicyAgent: "Fetchmark-Curator/1", FetchedAt: observedAt, ObservedAt: observedAt,
		ValidatedAt: observedAt, IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatal(err)
	}
	pipeline := &Pipeline{
		LocalCorpus: &recordingFailCorpus{failAll: true}, LocalArtifacts: artifacts,
		LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModeCurated},
	}
	mutations := pipeline.TakedownCurated(context.Background(), []string{rawURL})
	if len(mutations) != 1 || mutations[0].Status != CuratedStatusFailed || mutations[0].Reason != CuratedReasonTakedownFailed {
		t.Fatalf("mutations = %+v", mutations)
	}
	artifact, ok, err := artifacts.Current(context.Background(), rawURL)
	if err != nil || ok || artifact.Body != nil {
		t.Fatalf("artifact=%+v ok=%t err=%v", artifact, ok, err)
	}
}

func TestCuratedPolicyRevocationFailureIsNotReportedAsRejection(t *testing.T) {
	const rawURL = "https://example.com/noindex-failure"
	artifacts := openCuratedArtifacts(t)
	observedAt := time.Now().UTC().Add(-time.Minute)
	if err := artifacts.Put(context.Background(), localartifact.Version{
		URL: rawURL, EffectiveURL: rawURL, Body: []byte("previous source"), MIME: "text/html",
		PolicyAgent: "Fetchmark-Curator/1", FetchedAt: observedAt, ObservedAt: observedAt,
		ValidatedAt: observedAt, IndexingDisposition: localcorpus.DispositionPermitted,
	}); err != nil {
		t.Fatal(err)
	}
	pipeline := &Pipeline{
		Fetcher: stubFetcher{resp: map[string]fetcher.Result{rawURL: {
			Status: 200, FinalURL: rawURL, Body: []byte("denied"), ContentType: "text/html",
			XRobotsTag: []string{"noindex"}, UAUsed: "Fetchmark-Curator/1",
			RobotsAllowed: true, RobotsAuthoritative: true,
		}}},
		Extractor: stubExtractor{}, LocalCorpus: &recordingFailCorpus{failAll: true}, LocalArtifacts: artifacts,
		LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModeCurated}, RobotsUserAgent: "Fetchmark-Curator/1",
	}
	mutations := pipeline.AdmitCurated(context.Background(), []string{rawURL})
	if len(mutations) != 1 || mutations[0].Status != CuratedStatusFailed || mutations[0].Reason != CuratedReasonStorageFailed {
		t.Fatalf("mutations = %+v", mutations)
	}
	if _, ok, err := artifacts.Current(context.Background(), rawURL); err != nil || ok {
		t.Fatalf("artifact ok=%t err=%v", ok, err)
	}
}

type orderedCuratedFetcher struct {
	mu            sync.Mutex
	calls         int
	firstStarted  chan struct{}
	releaseFirst  chan struct{}
	secondStarted chan struct{}
	responses     []fetcher.Result
}

func (fetches *orderedCuratedFetcher) Fetch(ctx context.Context, _ fetcher.Request) fetcher.Result {
	fetches.mu.Lock()
	call := fetches.calls
	fetches.calls++
	response := fetches.responses[call]
	fetches.mu.Unlock()
	if call == 0 {
		close(fetches.firstStarted)
		select {
		case <-fetches.releaseFirst:
		case <-ctx.Done():
			response.Err = ctx.Err()
		}
	} else if call == 1 {
		close(fetches.secondStarted)
	}
	return response
}

func curatedPipeline(t *testing.T, fetches Fetcher, extractor Extractor) (*Pipeline, interface {
	SearchBatch(context.Context, search.Query) (search.SearchBatch, error)
	Count() (uint64, error)
}, *artifactfs.Store) {
	t.Helper()
	index := openPipelineIndex(t)
	artifacts := openCuratedArtifacts(t)
	return &Pipeline{
		Fetcher: fetches, Extractor: extractor, LocalCorpus: index, LocalArtifacts: artifacts, LocalSearcher: index,
		LocalRetentionPolicy: localcorpus.Policy{Mode: localcorpus.ModeCurated}, RobotsUserAgent: "Fetchmark-Curator/1",
	}, index, artifacts
}

func openCuratedArtifacts(t *testing.T) *artifactfs.Store {
	t.Helper()
	artifacts, err := artifactfs.Open(artifactfs.Options{
		Path: filepath.Join(t.TempDir(), "artifacts"), MaxBytes: 1 << 20, MaxArtifactBytes: 64 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = artifacts.Close() })
	return artifacts
}
