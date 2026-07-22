package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/staticvar/fetchmark/internal/adapters/bleveindex"
	artifactfs "github.com/staticvar/fetchmark/internal/adapters/localartifact"
	"github.com/staticvar/fetchmark/internal/adapters/openpackindex"
	"github.com/staticvar/fetchmark/internal/adapters/searchbudget"
	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/discovery"
	"github.com/staticvar/fetchmark/internal/core/federation"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/pipeline"
	"github.com/staticvar/fetchmark/internal/core/search"
)

type stubSearcher struct{}

func (stubSearcher) Search(context.Context, search.Query) ([]search.Hit, error) { return nil, nil }

type expirySweepEvent struct {
	store string
	now   time.Time
}

type recordingExpirySweeper struct {
	store  string
	events chan<- expirySweepEvent
	err    error
}

func (sweeper recordingExpirySweeper) Sweep(ctx context.Context, now time.Time) (int, error) {
	select {
	case sweeper.events <- expirySweepEvent{store: sweeper.store, now: now}:
		return 1, sweeper.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func TestParseRedisOptionsRejectsMalformedURL(t *testing.T) {
	if _, err := parseRedisOptions("://not-a-redis-url"); err == nil {
		t.Fatal("expected malformed Redis URL to fail")
	}
}

func TestBindLocalPersistenceDoesNotBoxTypedNilPointers(t *testing.T) {
	pipe := &pipeline.Pipeline{}
	bindLocalPersistence(pipe, (*bleveindex.Index)(nil), (*artifactfs.Store)(nil))
	if pipe.LocalCorpus != nil || pipe.LocalSearcher != nil || pipe.LocalArtifacts != nil {
		t.Fatalf("disabled local persistence became non-nil: writer=%v searcher=%v artifacts=%v", pipe.LocalCorpus, pipe.LocalSearcher, pipe.LocalArtifacts)
	}

	index := &bleveindex.Index{}
	artifacts := &artifactfs.Store{}
	bindLocalPersistence(pipe, index, artifacts)
	if pipe.LocalCorpus != index || pipe.LocalSearcher != index || pipe.LocalArtifacts != artifacts {
		t.Fatalf("enabled local persistence was not bound")
	}
}

func TestParseRedisOptionsAcceptsRedisURL(t *testing.T) {
	opts, err := parseRedisOptions("redis://localhost:6379/2")
	if err != nil {
		t.Fatalf("parseRedisOptions: %v", err)
	}
	if opts.Addr != "localhost:6379" || opts.DB != 2 {
		t.Fatalf("options = Addr %q DB %d", opts.Addr, opts.DB)
	}
}

func TestBuildDiscoveryPlannerRoutesBuiltInNativeSources(t *testing.T) {
	cfg := discoveryTestConfig()
	planner, primary, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	if primary == nil {
		t.Fatal("primary discovery source is nil")
	}
	got := make([]string, 0)
	for _, source := range planner.Sources(search.Query{Q: "peer reviewed paper"}) {
		got = append(got, source.ID)
	}
	want := []string{"searxng-default", "searxng-open", "crossref-research", "searxng-research"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("source plan = %v, want %v", got, want)
	}
}

func TestBuildDiscoveryPlannerFromSpecDoesNotRereadConfigurationFile(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryPackFile = filepath.Join(t.TempDir(), "must-not-be-read.json")
	spec, err := discovery.DefaultSpec()
	if err != nil {
		t.Fatalf("DefaultSpec: %v", err)
	}
	planner, primary, err := buildDiscoveryPlannerFromSpec(cfg, stubSearcher{}, &http.Client{}, nil, spec)
	if err != nil {
		t.Fatalf("buildDiscoveryPlannerFromSpec: %v", err)
	}
	if planner == nil || primary == nil {
		t.Fatalf("planner=%v primary=%v", planner, primary)
	}
}

func TestBuildDiscoveryPlannerBindsOptInNativeMwmblLane(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "mwmbl")
	planner, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	sources := planner.Sources(search.Query{Q: "independent web search"})
	found := false
	for _, source := range sources {
		if source.ID == "mwmbl-general" {
			found = source.ProviderID == "mwmbl" && source.ProviderKind == "mwmbl" && reflect.DeepEqual(source.Variants, []string{"original", "concept"})
		}
	}
	if !found {
		t.Fatalf("Mwmbl lane missing or malformed: %+v", sources)
	}
}

func TestBuildDiscoveryPlannerBindsOptInNativeWibyLane(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "wiby")
	planner, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	sources := planner.Sources(search.Query{Q: "independent personal web pages"})
	found := false
	for _, source := range sources {
		if source.ID == "wiby-general" {
			found = source.ProviderID == "wiby" && source.ProviderKind == "wiby" && reflect.DeepEqual(source.Variants, []string{"original"})
		}
	}
	if !found {
		t.Fatalf("Wiby lane missing or malformed: %+v", sources)
	}
}

func TestBuildDiscoveryPlannerBindsOptInNativeStackExchangeLane(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "stackexchange")
	planner, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	sources := planner.Sources(search.Query{Q: "How do Kubernetes readiness probes work?"})
	found := false
	for _, source := range sources {
		if source.ID == "stackexchange-developer" {
			found = source.ProviderID == "stackexchange" && source.ProviderKind == "stackexchange" && reflect.DeepEqual(source.Variants, []string{"original"})
		}
	}
	if !found {
		t.Fatalf("Stack Exchange lane missing or malformed: %+v", sources)
	}
}

func TestBuildDiscoveryPlannerBindsOptInNativeGitHubLane(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "github")
	planner, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	sources := planner.Sources(search.Query{Q: "How do Kubernetes readiness probes work?"})
	found := false
	for _, source := range sources {
		if source.ID == "github-developer" {
			found = source.ProviderID == "github" && source.ProviderKind == "github" && reflect.DeepEqual(source.Variants, []string{"original"})
		}
	}
	if !found {
		t.Fatalf("GitHub lane missing or malformed: %+v", sources)
	}
}

func TestBuildDiscoveryPlannerBindsOptInNativeArxivLane(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "arxiv")
	planner, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	sources := planner.Sources(search.Query{Q: "peer reviewed information retrieval paper"})
	found := false
	for _, source := range sources {
		if source.ID == "arxiv-research" {
			found = source.ProviderID == "arxiv" && source.ProviderKind == "arxiv" && reflect.DeepEqual(source.Variants, []string{"original", "exact", "freshness"})
		}
	}
	if !found {
		t.Fatalf("arXiv lane missing or malformed: %+v", sources)
	}
}

func TestBuildDiscoveryPlannerBindsOptInNativePubMedLane(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "pubmed")
	cfg.PubMedEmail = "operator@example.test"
	planner, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	sources := planner.Sources(search.Query{Q: "longitudinal circadian biomarker study"})
	found := false
	for _, source := range sources {
		if source.ID == "pubmed-research" {
			found = source.ProviderID == "pubmed" && source.ProviderKind == "pubmed" && reflect.DeepEqual(source.Variants, []string{"original", "freshness"})
		}
	}
	if !found {
		t.Fatalf("PubMed lane missing or malformed: %+v", sources)
	}
}

func TestBuildDiscoveryPlannerRequiresPubMedOperatorEmail(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "pubmed")
	if _, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{}); err == nil || !strings.Contains(err.Error(), "plain valid operator email") {
		t.Fatalf("buildDiscoveryPlanner error = %v", err)
	}
}

func TestBuildDiscoveryPlannerBindsOptInNativeYaCyLane(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "yacy")
	cfg.YaCyURL = "http://yacy:8090"
	cfg.YaCyResource = "local"
	cfg.YaCyAllowInsecureHTTP = true
	planner, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	sources := planner.Sources(search.Query{Q: "obscure independent web page"})
	found := false
	for _, source := range sources {
		if source.ID == "yacy-general" {
			found = source.ProviderID == "yacy" && source.ProviderKind == "yacy" && reflect.DeepEqual(source.Variants, []string{"original"})
		}
	}
	if !found {
		t.Fatalf("YaCy lane missing or malformed: %+v", sources)
	}
}

func TestBuildDiscoveryPlannerRequiresExplicitYaCyHTTPOptIn(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "yacy")
	cfg.YaCyURL = "http://yacy:8090"
	cfg.YaCyResource = "local"
	if _, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{}); err == nil || !strings.Contains(err.Error(), "insecure HTTP is explicitly enabled") {
		t.Fatalf("buildDiscoveryPlanner error = %v", err)
	}
}

func TestBuildDiscoveryPlannerBindsOptInScraplingLane(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "scrapling")
	cfg.ScraplingURL = "http://scrapling:8080"
	cfg.ScraplingAllowInsecureHTTP = true
	planner, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	sources := planner.Sources(search.Query{Q: "latest runtime release"})
	for _, source := range sources {
		if source.ID == "scrapling-general" && source.ProviderID == "scrapling" && source.ProviderKind == "scrapling" && reflect.DeepEqual(source.Engines, []string{"google", "duckduckgo"}) {
			return
		}
	}
	t.Fatalf("Scrapling lane missing or malformed: %+v", sources)
}

func TestBuildDiscoveryPlannerRequiresExplicitScraplingHTTPOptIn(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "scrapling")
	cfg.ScraplingURL = "http://scrapling:8080"
	if _, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{}); err == nil || !strings.Contains(err.Error(), "insecure HTTP is explicitly enabled") {
		t.Fatalf("buildDiscoveryPlanner error = %v", err)
	}
}

func TestBuildDiscoveryPlannerAllowsExplicitNonSearxPrimaryWithoutSearxClient(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = []string{"wikipedia"}
	cfg.DiscoveryPrimarySource = "wikipedia"
	planner, primary, err := buildDiscoveryPlanner(cfg, nil, &http.Client{})
	if err != nil {
		t.Fatalf("buildDiscoveryPlanner: %v", err)
	}
	if planner == nil || primary == nil {
		t.Fatalf("planner=%v primary=%v", planner, primary)
	}
	sources := planner.Sources(search.Query{Q: "what is a tidal bore"})
	if len(sources) != 1 || sources[0].ProviderID != "wikipedia" {
		t.Fatalf("sources = %+v", sources)
	}
}

func TestBuildDiscoveryPlannerRequiresSearxClientOnlyWhenEnabled(t *testing.T) {
	cfg := discoveryTestConfig()
	if _, _, err := buildDiscoveryPlanner(cfg, nil, &http.Client{}); err == nil || !strings.Contains(err.Error(), "SearXNG client is required") {
		t.Fatalf("buildDiscoveryPlanner error = %v", err)
	}
}

func TestSearxReadinessRequiredOnlyForConfiguredPrimary(t *testing.T) {
	cfg := discoveryTestConfig()
	if !searxReadinessRequired(cfg) {
		t.Fatal("default SearXNG primary must remain a hard readiness dependency")
	}
	cfg.DiscoveryPrimarySource = "wikipedia"
	if searxReadinessRequired(cfg) {
		t.Fatal("secondary opportunistic SearXNG lane must not fail global readiness")
	}
	cfg.DiscoveryEnabledSources = []string{"wikipedia"}
	if searxReadinessRequired(cfg) {
		t.Fatal("disabled SearXNG must not be a readiness dependency")
	}
}

func TestBuildDiscoveryPlannerKeepsEnabledSearxOpportunisticBehindNativePrimary(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryPrimarySource = "wikipedia"
	planner, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	sources := planner.Sources(search.Query{Q: "tidal bores"})
	if len(sources) < 2 || sources[0].ProviderID != "wikipedia" || sources[1].ProviderID != "searxng" {
		t.Fatalf("source plan = %+v, want native primary before opportunistic SearXNG", sources)
	}
}

func TestBuildDiscoveryPlannerRejectsUnsplittableCacheBudget(t *testing.T) {
	cfg := discoveryTestConfig()
	cfg.DiscoveryCacheEntries = 2
	if _, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{}); err == nil {
		t.Fatal("buildDiscoveryPlanner accepted fewer cache entries than enabled sources")
	}
}

func TestBuildDiscoveryPlannerOpensOptInFreshFeedIndex(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	snapshotPath := filepath.Join(t.TempDir(), "feed-index.json")
	snapshot := map[string]any{
		"version": 1, "generated_at": now.Add(-time.Minute).Format(time.RFC3339),
		"sources": []map[string]any{{
			"id": "python", "feed_url": "https://blog.python.org/rss.xml", "topics": []string{"python"},
			"license": "CC-BY-NC-SA-3.0", "license_url": "https://creativecommons.org/licenses/by-nc-sa/3.0/",
			"fetched_at": now.Add(-time.Minute).Format(time.RFC3339), "feed_robots_observed_at": now.Add(-time.Minute).Format(time.RFC3339),
			"feed_robots_allowed": true, "feed_noindex": false, "feed_x_robots_noindex": false,
			"min_poll_interval_seconds": 3600, "max_items_per_fetch": 50,
			"documents": []map[string]any{{
				"url": "https://blog.python.org/2026/06/python-3146-31314/", "title": "Python 3.14.6 and 3.13.14 are now available", "summary": "A pair of bug fix releases.",
				"published_at": now.Add(-24 * time.Hour).Format(time.RFC3339), "observed_at": now.Add(-time.Minute).Format(time.RFC3339),
				"robots_observed_at": now.Add(-time.Minute).Format(time.RFC3339), "robots_allowed": true, "noindex": false, "x_robots_noindex": false,
			}},
		}},
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(snapshotPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "feedindex")
	cfg.FeedIndexFile = snapshotPath
	planner, primary, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	closer, ok := primary.(io.Closer)
	if !ok {
		t.Fatal("primary searcher does not own feed-index lifecycle")
	}
	t.Cleanup(func() { _ = closer.Close() })

	sources := planner.Sources(search.Query{Q: "latest stable Python release", TimeRange: "day"})
	var feedSource search.Searcher
	for _, source := range sources {
		if source.ID == "official-fresh-feeds" {
			feedSource = source.Searcher
			if source.Weight != 0.96 {
				t.Fatalf("feed lane weight = %v", source.Weight)
			}
		}
	}
	if feedSource == nil {
		t.Fatalf("sources = %+v", sources)
	}
	hits, err := feedSource.Search(context.Background(), search.Query{Q: "latest stable Python release", MaxResults: 5})
	if err != nil || len(hits) != 1 || hits[0].Metadata["feed_source"] != "python" {
		t.Fatalf("feed hits = %+v err=%v", hits, err)
	}
}

func TestBuildDiscoveryPlannerOpensOptInOfficialDocIndex(t *testing.T) {
	fixturePath, err := filepath.Abs(filepath.Join("..", "..", "internal", "adapters", "docindex", "testdata", "official-docs.json"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot map[string]any
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
	snapshot["generated_at"] = now
	for _, rawSource := range snapshot["sources"].([]any) {
		source := rawSource.(map[string]any)
		source["observed_at"] = now
		source["robots_observed_at"] = now
		for _, rawDocument := range source["documents"].([]any) {
			document := rawDocument.(map[string]any)
			document["observed_at"] = now
			document["robots_observed_at"] = now
		}
	}
	raw, err = json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshotPath := filepath.Join(t.TempDir(), "official-docs.json")
	if err := os.WriteFile(snapshotPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := discoveryTestConfig()
	cfg.DiscoveryEnabledSources = append(cfg.DiscoveryEnabledSources, "docindex")
	cfg.OfficialDocIndexFile = snapshotPath
	planner, primary, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{})
	if err != nil {
		t.Fatal(err)
	}
	closer, ok := primary.(io.Closer)
	if !ok {
		t.Fatal("primary searcher does not own official-doc-index lifecycle")
	}
	t.Cleanup(func() { _ = closer.Close() })

	sources := planner.Sources(search.Query{Q: "Kotlin coroutine API documentation"})
	var docSource search.Searcher
	for _, source := range sources {
		if source.ID == "official-developer-docs" {
			docSource = source.Searcher
			if source.Weight != 1.0 || len(source.Variants) != 1 || source.Variants[0] != "original" {
				t.Fatalf("official docs lane = %+v", source)
			}
		}
	}
	if docSource == nil {
		t.Fatalf("sources = %+v", sources)
	}
	moderate := 1
	hits, err := docSource.Search(context.Background(), search.Query{
		Q: "Kotlin coroutine cancellation", MaxResults: 5, Language: "en", SafeSearch: &moderate,
	})
	if err != nil || len(hits) == 0 || hits[0].Metadata["document_source"] != "kotlin" {
		t.Fatalf("official docs hits = %+v err=%v", hits, err)
	}
}

func TestBuildDiscoveryPlannerOpensExplicitSignedPackSource(t *testing.T) {
	fixture := installDiscoveryPackFixture(t)
	cfg := discoveryTestConfig()
	cfg.DiscoveryPackFile = fixture.specPath
	cfg.OpenPackRegistryFile = fixture.registryPath
	cfg.DiscoveryEnabledPacks = []string{"developer"}
	cfg.DiscoveryEnabledSources = []string{"openpack-developer"}
	cfg.DiscoveryPrimarySource = "openpack-developer"

	planner, primary, err := buildDiscoveryPlanner(cfg, nil, &http.Client{})
	if err != nil {
		t.Fatalf("buildDiscoveryPlanner: %v", err)
	}
	closer, ok := primary.(io.Closer)
	if !ok {
		t.Fatal("primary searcher does not own open-pack lifecycle")
	}
	t.Cleanup(func() {
		if err := closer.Close(); err != nil {
			t.Errorf("close discovery sources: %v", err)
		}
	})

	sources := planner.Sources(search.Query{Q: "golang API documentation"})
	if len(sources) != 1 || sources[0].ID != "openpack-developer-lane" {
		t.Fatalf("sources = %+v", sources)
	}
	if _, ok := sources[0].Searcher.(*searchbudget.Searcher); !ok {
		t.Fatalf("open-pack source is %T, want direct budget wrapper without discovery response cache", sources[0].Searcher)
	}
	hits, err := sources[0].Searcher.Search(context.Background(), search.Query{Q: "open pack metadata", MaxResults: 5})
	if err != nil {
		t.Fatalf("open-pack search: %v", err)
	}
	if len(hits) != 1 || hits[0].URL != "https://docs.example.com/open-pack" || hits[0].Metadata["source_id"] != "openpack-developer" {
		t.Fatalf("open-pack hits = %+v", hits)
	}
}

func TestBuildDiscoveryPlannerUsesProjectionCountForVersionTwoDeltaBinding(t *testing.T) {
	fixture := installDiscoveryPackFixtureAtRevision(t, 2)
	mutateDiscoveryRegistryToVersionTwoDelta(t, fixture.registryPath, 3, 1)
	cfg := discoveryTestConfig()
	cfg.DiscoveryPackFile = fixture.specPath
	cfg.OpenPackRegistryFile = fixture.registryPath
	cfg.DiscoveryEnabledPacks = []string{"developer"}
	cfg.DiscoveryEnabledSources = []string{"openpack-developer"}
	cfg.DiscoveryPrimarySource = "openpack-developer"

	planner, primary, err := buildDiscoveryPlanner(cfg, nil, &http.Client{})
	if err != nil {
		t.Fatalf("buildDiscoveryPlanner(v2 delta): %v", err)
	}
	closer, ok := primary.(io.Closer)
	if !ok {
		t.Fatal("primary searcher does not own open-pack lifecycle")
	}
	t.Cleanup(func() {
		if err := closer.Close(); err != nil {
			t.Errorf("close discovery sources: %v", err)
		}
	})

	sources := planner.Sources(search.Query{Q: "open pack metadata"})
	if len(sources) != 1 {
		t.Fatalf("sources = %+v", sources)
	}
	hits, err := sources[0].Searcher.Search(context.Background(), search.Query{Q: "open pack metadata", MaxResults: 5})
	if err != nil || len(hits) != 1 || hits[0].URL != "https://docs.example.com/open-pack" {
		t.Fatalf("v2 delta projection search = %+v, %v", hits, err)
	}
}

func TestBuildDiscoveryPlannerRequiresRegistryForEnabledOpenPack(t *testing.T) {
	fixture := installDiscoveryPackFixture(t)
	cfg := discoveryTestConfig()
	cfg.DiscoveryPackFile = fixture.specPath
	cfg.DiscoveryEnabledPacks = []string{"developer"}
	cfg.DiscoveryEnabledSources = []string{"searxng", "openpack-developer"}
	cfg.OpenPackRegistryFile = ""
	if _, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{}); err == nil || !strings.Contains(err.Error(), "FM_OPEN_PACK_REGISTRY_FILE is required") {
		t.Fatalf("buildDiscoveryPlanner error = %v", err)
	}
}

func TestBuildDiscoveryPlannerRejectsWritableOpenPackRegistry(t *testing.T) {
	fixture := installDiscoveryPackFixture(t)
	if err := os.Chmod(fixture.registryPath, 0o666); err != nil {
		t.Fatal(err)
	}
	cfg := discoveryTestConfig()
	cfg.DiscoveryPackFile = fixture.specPath
	cfg.OpenPackRegistryFile = fixture.registryPath
	cfg.DiscoveryEnabledPacks = []string{"developer"}
	cfg.DiscoveryEnabledSources = []string{"searxng", "openpack-developer"}
	if _, _, err := buildDiscoveryPlanner(cfg, stubSearcher{}, &http.Client{}); err == nil || !strings.Contains(err.Error(), "must not be writable by group or world") {
		t.Fatalf("buildDiscoveryPlanner error = %v", err)
	}
}

func TestBuildDiscoveryPlannerBindsExplicitTrustedFederationSource(t *testing.T) {
	fixture := writeFederationFixture(t)
	cfg := discoveryTestConfig()
	cfg.DiscoveryPackFile = fixture.specPath
	cfg.DiscoveryEnabledPacks = []string{"peer-developer"}
	cfg.DiscoveryEnabledSources = []string{"peer-one"}
	cfg.DiscoveryPrimarySource = "peer-one"
	cfg.FederationIdentityFile = fixture.identityPath
	cfg.FederationTrustRegistryFile = fixture.registryPath
	runtime, err := loadFederationRuntime(cfg)
	if err != nil {
		t.Fatalf("loadFederationRuntime: %v", err)
	}
	planner, primary, err := buildDiscoveryPlannerWithFederation(cfg, nil, &http.Client{}, runtime)
	if err != nil {
		t.Fatalf("buildDiscoveryPlannerWithFederation: %v", err)
	}
	if primary == nil {
		t.Fatal("primary discovery source is nil")
	}
	sources := planner.Sources(search.Query{Q: "golang federation documentation"})
	if len(sources) != 1 || sources[0].ID != "peer-one-original" || sources[0].ProviderID != "peer-one" || sources[0].ProviderKind != "federation" {
		t.Fatalf("federation sources = %+v", sources)
	}
}

func TestBuildDiscoveryPlannerRequiresConfiguredFederationBinding(t *testing.T) {
	fixture := writeFederationFixture(t)
	cfg := discoveryTestConfig()
	cfg.DiscoveryPackFile = fixture.specPath
	cfg.DiscoveryEnabledPacks = []string{"peer-developer"}
	cfg.DiscoveryEnabledSources = []string{"searxng", "peer-one"}
	if _, _, err := buildDiscoveryPlannerWithFederation(cfg, stubSearcher{}, &http.Client{}, nil); err == nil || !strings.Contains(err.Error(), "FM_FEDERATION_IDENTITY_FILE") {
		t.Fatalf("missing runtime error = %v", err)
	}
	runtime, err := loadFederationRuntime(config.Config{
		FederationIdentityFile: fixture.identityPath, FederationTrustRegistryFile: fixture.registryPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	missingSpec := filepath.Join(t.TempDir(), "missing-peer.json")
	raw := `{"version":1,"sources":[{"id":"searxng","kind":"searxng","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1},{"id":"peer-two","kind":"federation","weight":1,"max_results":10,"timeout_ms":2000,"max_concurrency":1,"rate_per_second":1,"burst":1}],"packs":[{"id":"peer-developer","intents":["developer"],"sources":[{"id":"peer-two-original","source":"peer-two","weight":1,"variants":["original"]}]}]}`
	if err := os.WriteFile(missingSpec, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.DiscoveryPackFile = missingSpec
	cfg.DiscoveryEnabledSources = []string{"searxng", "peer-two"}
	if _, _, err := buildDiscoveryPlannerWithFederation(cfg, stubSearcher{}, &http.Client{}, runtime); err == nil || !strings.Contains(err.Error(), "no federation trust binding") {
		t.Fatalf("missing trust binding error = %v", err)
	}
}

func TestSplitDiscoveryCacheOptionsBoundsAggregateCapacity(t *testing.T) {
	cfg := discoveryTestConfig()
	options := splitDiscoveryCacheOptions(cfg, 3)
	if options.MaxEntries*3 > cfg.DiscoveryCacheEntries || options.MaxBytes*3 > cfg.DiscoveryCacheBytes || options.MaxInflight*3 > cfg.DiscoveryMaxInflight {
		t.Fatalf("split options exceed aggregate config: %+v", options)
	}
	if options.MaxEntryBytes > options.MaxBytes {
		t.Fatalf("entry budget %d exceeds cache budget %d", options.MaxEntryBytes, options.MaxBytes)
	}
}

func TestLocalExpiryCoordinatorUsesOneTimestampAndContinuesAfterFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time, 1)
	events := make(chan expirySweepEvent, 2)
	done := make(chan struct{})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		defer close(done)
		runLocalExpirySweeper(ctx, ticks, logger, []namedExpirySweeper{
			{store: localExpiryStoreIndex, sweeper: recordingExpirySweeper{store: localExpiryStoreIndex, events: events, err: errors.New("index unavailable")}},
			{store: localExpiryStoreArtifact, sweeper: recordingExpirySweeper{store: localExpiryStoreArtifact, events: events}},
		})
	}()
	wantNow := time.Date(2026, 7, 18, 12, 0, 0, 0, time.FixedZone("test", 5*60*60+30*60))
	ticks <- wantNow
	first := <-events
	second := <-events
	if first.store != localExpiryStoreIndex || second.store != localExpiryStoreArtifact {
		t.Fatalf("sweep order = %q, %q", first.store, second.store)
	}
	if !first.now.Equal(wantNow) || !second.now.Equal(wantNow) || first.now.Location() != time.UTC || second.now.Location() != time.UTC {
		t.Fatalf("sweep times = %v, %v; want UTC %v", first.now, second.now, wantNow.UTC())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expiry coordinator did not stop after cancellation")
	}
}

func TestDiscoveryProviderUserAgentUsesValidatedContact(t *testing.T) {
	got, err := discoveryProviderUserAgent("ExampleBot/1.0", "ops@example.com")
	if err != nil || got != "ExampleBot/1.0 (contact: mailto:ops@example.com)" {
		t.Fatalf("user agent = %q err=%v", got, err)
	}
	if _, err := discoveryProviderUserAgent("ExampleBot/1.0", "none"); err == nil {
		t.Fatal("invalid contact was accepted")
	}
	if _, err := discoveryProviderUserAgent("ExampleBot/1.0", "not@"); err == nil {
		t.Fatal("malformed email contact was accepted")
	}
}

func discoveryTestConfig() config.Config {
	return config.Config{
		UserAgent:                   "Fetchmark/0.1 (+https://github.com/staticvar/fetchmark)",
		DiscoveryEnabledPacks:       []string{"general-open", "developer", "research", "knowledge", "fresh"},
		DiscoveryEnabledSources:     []string{"searxng", "wikipedia", "crossref"},
		DiscoveryPrimarySource:      "searxng",
		DiscoveryProviderMaxBody:    2 << 20,
		DiscoveryCacheTTL:           30 * time.Second,
		DiscoveryCacheStaleTTL:      2 * time.Minute,
		DiscoveryRefreshTimeout:     30 * time.Second,
		DiscoveryCacheEntries:       256,
		DiscoveryCacheBytes:         16 << 20,
		DiscoveryCacheMaxEntryBytes: 1 << 20,
		DiscoveryMaxInflight:        16,
	}
}

type discoveryPackFixture struct {
	specPath     string
	registryPath string
}

type federationFixture struct {
	identityPath string
	registryPath string
	specPath     string
}

func writeFederationFixture(t *testing.T) federationFixture {
	t.Helper()
	root := t.TempDir()
	localPublic, localPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	remotePublic, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	identityRaw, err := json.Marshal(map[string]any{
		"version": federation.IdentityVersion, "identity_id": "local-node",
		"key_id": federation.KeyID(localPublic), "ed25519_private_key": base64.StdEncoding.EncodeToString(localPrivate),
	})
	if err != nil {
		t.Fatal(err)
	}
	registryRaw, err := json.Marshal(map[string]any{
		"version": federation.TrustRegistryVersion,
		"identities": []map[string]any{{
			"id": "remote-node", "key_id": federation.KeyID(remotePublic),
			"ed25519_public_key": base64.StdEncoding.EncodeToString(remotePublic),
		}},
		"peers": []map[string]any{{
			"id": "peer-one", "identity_id": "remote-node", "base_origin": "https://peer.example",
			"allow_private_network": false, "allow_inbound": true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	identityPath := filepath.Join(root, "identity.json")
	registryPath := filepath.Join(root, "trust.json")
	specPath := filepath.Join(root, "discovery.json")
	for path, raw := range map[string][]byte{identityPath: identityRaw, registryPath: registryRaw} {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	spec := `{"version":1,"sources":[{"id":"searxng","kind":"searxng","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1},{"id":"peer-one","kind":"federation","weight":0.8,"max_results":10,"timeout_ms":2000,"max_concurrency":1,"rate_per_second":1,"burst":1}],"packs":[{"id":"peer-developer","intents":["developer"],"sources":[{"id":"peer-one-original","source":"peer-one","weight":1,"variants":["original"]}]}]}`
	if err := os.WriteFile(specPath, []byte(spec), 0o600); err != nil {
		t.Fatal(err)
	}
	return federationFixture{identityPath: identityPath, registryPath: registryPath, specPath: specPath}
}

func installDiscoveryPackFixture(t *testing.T) discoveryPackFixture {
	return installDiscoveryPackFixtureAtRevision(t, 1)
}

func installDiscoveryPackFixtureAtRevision(t *testing.T, revision uint64) discoveryPackFixture {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	record := indexpack.Record{
		Operation: indexpack.OperationUpsert, URL: "https://docs.example.com/open-pack", Title: "Open pack metadata",
		SalientSketch: "signed local discovery", Language: "en", FetchedAt: now.Add(-time.Hour).Format(time.RFC3339),
		Provenance: []indexpack.Provenance{{
			Source: "fixture", SourceURI: "https://example.com/source", RetrievedAt: now.Add(-2 * time.Hour).Format(time.RFC3339), RightsNotice: "fixture rights",
		}},
	}
	var ndjson bytes.Buffer
	if err := json.NewEncoder(&ndjson).Encode(record); err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	encoder, err := zstd.NewWriter(&compressed, zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encoder.Write(ndjson.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	compressedDigest := indexpack.ShardDigest(compressed.Bytes())
	manifest := indexpack.Manifest{
		Version: indexpack.Version, Kind: indexpack.KindSnapshot, PackID: "developer-en", Revision: revision,
		CreatedAt: now.Add(-time.Hour).Format(time.RFC3339), ExpiresAt: now.Add(24 * time.Hour).Format(time.RFC3339), SigningKeyID: indexpack.KeyID(publicKey),
		Publisher: indexpack.Publisher{Name: "Fixture Publisher", ContactURI: "mailto:operator@example.com", TakedownURI: "https://example.com/takedown", RightsNotice: "fixture rights"},
		Policy:    indexpack.Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"}, Languages: []string{"en"}, RecordCount: 1,
		Shards: []indexpack.Shard{{
			Path: indexpack.ShardPath(compressedDigest), Compression: "zstd", SHA256: compressedDigest,
			CompressedSizeBytes: uint64(compressed.Len()), UncompressedSHA256: indexpack.ShardDigest(ndjson.Bytes()), UncompressedSizeBytes: uint64(ndjson.Len()), RecordCount: 1,
		}},
		Build: indexpack.Build{
			Generator: "fixture", GeneratorVersion: "1", Analyzer: "unicode-lexical", AnalyzerVersion: "1",
			PolicySHA256: strings.Repeat("1", 64), ExclusionsSHA256: strings.Repeat("2", 64),
			Inputs: []indexpack.BuildInput{{Name: "fixture", URI: "https://example.com/input", RetrievedAt: now.Add(-2 * time.Hour).Format(time.RFC3339), SHA256: strings.Repeat("3", 64), RightsNotice: "fixture rights"}},
		},
	}
	manifestRaw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := indexpack.SignManifest(manifestRaw, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	signatureRaw, err := indexpack.EncodeSignature(signature)
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	bundle := filepath.Join(base, "bundle")
	root := filepath.Join(base, "open-packs")
	if err := os.MkdirAll(filepath.Join(bundle, filepath.Dir(manifest.Shards[0].Path)), 0o700); err != nil {
		t.Fatal(err)
	}
	for path, data := range map[string][]byte{
		filepath.Join(bundle, openpackindex.ManifestFilename):  manifestRaw,
		filepath.Join(bundle, openpackindex.SignatureFilename): signatureRaw,
		filepath.Join(bundle, manifest.Shards[0].Path):         compressed.Bytes(),
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	digest := indexpack.ManifestDigest(manifestRaw)
	acceptance := indexpack.Acceptance{
		Now: now, ExpectedPackID: manifest.PackID, ExpectedManifestSHA256: digest,
		ExpectedKeyID: indexpack.KeyID(publicKey), MinimumRevision: manifest.Revision, ExpectedRevision: manifest.Revision,
		ExpectedRecordCount: manifest.RecordCount, ExpectedCreatedAt: manifest.CreatedAt, ExpectedExpiresAt: manifest.ExpiresAt,
	}
	installed, err := openpackindex.Install(context.Background(), openpackindex.InstallOptions{
		BundleDir: bundle, Root: root, TrustedKeys: map[string]ed25519.PublicKey{indexpack.KeyID(publicKey): publicKey}, Acceptance: acceptance,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := map[string]any{
		"version": 1,
		"publishers": []any{map[string]any{
			"id": "fixture", "key_id": indexpack.KeyID(publicKey), "ed25519_public_key": base64.StdEncoding.EncodeToString(publicKey),
		}},
		"bindings": []any{map[string]any{
			"source_id": "openpack-developer", "pack_id": manifest.PackID, "publisher_id": "fixture",
			"manifest_sha256": digest, "revision": manifest.Revision, "record_count": manifest.RecordCount,
			"created_at": manifest.CreatedAt, "expires_at": manifest.ExpiresAt, "installed_path": installed.Path,
		}},
	}
	registryRaw, err := json.Marshal(registry)
	if err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(base, "registry.json")
	if err := os.WriteFile(registryPath, registryRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	spec := `{"version":1,"sources":[{"id":"searxng","kind":"searxng","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1},{"id":"openpack-developer","kind":"openpack","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":100,"burst":10}],"packs":[{"id":"developer","intents":["developer"],"sources":[{"id":"openpack-developer-lane","source":"openpack-developer","weight":1,"variants":["original"]}]}]}`
	specPath := filepath.Join(base, "discovery.json")
	if err := os.WriteFile(specPath, []byte(spec), 0o600); err != nil {
		t.Fatal(err)
	}
	return discoveryPackFixture{specPath: specPath, registryPath: registryPath}
}

func mutateDiscoveryRegistryToVersionTwoDelta(t *testing.T, path string, operationCount, projectionCount uint64) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	document["version"] = float64(2)
	bindings, ok := document["bindings"].([]any)
	if !ok || len(bindings) != 1 {
		t.Fatalf("unexpected bindings: %#v", document["bindings"])
	}
	binding, ok := bindings[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected binding: %#v", bindings[0])
	}
	delete(binding, "record_count")
	binding["kind"] = string(indexpack.KindDelta)
	binding["parent_manifest_sha256"] = strings.Repeat("b", 64)
	binding["manifest_record_count"] = operationCount
	binding["projection_record_count"] = projectionCount
	updated, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, updated, 0o600); err != nil {
		t.Fatal(err)
	}
}
