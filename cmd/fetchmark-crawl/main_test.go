package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/corpusclient"
	"github.com/staticvar/fetchmark/internal/adapters/crawlfrontier"
	"github.com/staticvar/fetchmark/internal/core/focusedcrawl"
)

func TestDryRunValidatesAndPlansWithoutStateOrNetworkConfiguration(t *testing.T) {
	configPath := writeTestConfig(t)
	statePath := filepath.Join(t.TempDir(), "must-not-exist.db")
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-config", configPath, "-state", statePath, "-dry-run"}, &stdout, &stderr, func(string) string { return "" })
	if code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatalf("dry-run created state: %v", err)
	}
	var summary map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if summary["planned"] != float64(2) || summary["planned_sources"] != float64(0) || summary["mode"] != "dry_run" {
		t.Fatalf("summary=%v", summary)
	}
}

func TestDiscoveryCapsAndParserBridge(t *testing.T) {
	config, err := focusedcrawl.DecodeConfig(bytes.NewBufferString(`{
  "version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":10,
  "max_discovery_page_memberships":1000,"max_discovery_source_edges":1000,
  "jobs":[{"id":"docs","seeds":[],"allowed_domains":["example.org"],"allowed_path_prefixes":["/docs/"],
    "max_urls_per_run":1,"max_source_polls_per_run":1,"refresh_interval":"1h","rejection_recheck_interval":"24h","min_host_interval":"1s","max_attempts":2,
    "discovery_sources":[{"url":"https://example.org/feeds/root.xml","kind":"sitemap","allowed_source_path_prefixes":["/feeds/"],
      "poll_interval":"1h","error_recheck_interval":"1h","max_compressed_bytes":4096,"max_decompressed_bytes":8192,
      "max_entries":17,"max_child_sources":3,"max_depth":2}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	caps := discoveryCaps(config)
	if caps.maxPagesPerSource != 17 || caps.maxChildrenPerSource != 3 || caps.maxSourceDepth != 2 || caps.maxWireBytes != 4096 || caps.maxDecompressedBytes != 8192 {
		t.Fatalf("caps=%+v", caps)
	}
	document, err := parseDiscovery(
		[]byte(`<urlset><url><loc>https://example.org/docs/a</loc></url></urlset>`),
		"https://example.org/feeds/root.xml", focusedcrawl.SourceKindSitemap,
		focusedcrawl.DiscoveryLimits{MaxDocumentBytes: 4096, MaxEntries: 10, MaxURLBytes: 2048, MaxXMLDepth: 16},
	)
	if err != nil {
		t.Fatal(err)
	}
	if document.Kind != focusedcrawl.SourceKindSitemap || len(document.Pages) != 1 || document.Pages[0].URL != "https://example.org/docs/a" || len(document.Sources) != 0 {
		t.Fatalf("document=%+v", document)
	}
}

func TestFrontierBridgeCommitsDiscoveredPage(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	frontier, err := crawlfrontier.Open(filepath.Join(t.TempDir(), "frontier.db"), crawlfrontier.Options{
		MaxEntries: 10, MaxSources: 10, MaxPagesPerSource: 10, MaxChildrenPerSource: 10,
		MaxMemberships: 10, MaxSourceEdges: 10, MaxSourceDepth: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer frontier.Close()
	bridge := frontierBridge{frontier}
	if err := bridge.SyncDiscoverySources([]focusedcrawl.SourceFrontierSeed{{
		Key: "root", JobID: "docs", URL: "https://example.org/feeds/root.xml", Kind: focusedcrawl.SourceKindSitemap, RootKey: "root",
	}}, now); err != nil {
		t.Fatal(err)
	}
	sources, err := bridge.LeaseDueDiscoverySourcesJob("docs", now, 1, time.Minute)
	if err != nil || len(sources) != 1 {
		t.Fatalf("sources=%+v error=%v", sources, err)
	}
	if err := bridge.ApplyDiscoverySnapshot(focusedcrawl.SourceSnapshot{
		SourceKey: "root", LeaseGeneration: sources[0].LeaseGeneration,
		Pages:      []focusedcrawl.DiscoveredPage{{Key: "page", URL: "https://example.org/docs/a"}},
		Validators: focusedcrawl.SourceValidators{ETag: `"v1"`}, NextEligible: now.Add(time.Hour),
	}, now); err != nil {
		t.Fatal(err)
	}
	pages, err := bridge.LeaseDueJob("docs", now, 1, time.Minute)
	if err != nil || len(pages) != 1 || pages[0].Key != "page" || pages[0].URL != "https://example.org/docs/a" {
		t.Fatalf("pages=%+v error=%v", pages, err)
	}
}

func TestOnceRunPersistsRefreshScheduleAndDoesNotRepeatImmediately(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.URL.Path != "/admin/corpus/focused-admissions" || request.Header.Get("X-API-Key") != "crawler-key" {
			t.Errorf("request=%s key=%q", request.URL.Path, request.Header.Get("X-API-Key"))
		}
		var body struct {
			URL                 string   `json:"url"`
			AllowedPathPrefixes []string `json:"allowed_path_prefixes"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body.URL == "" || len(body.AllowedPathPrefixes) != 1 || body.AllowedPathPrefixes[0] != "/docs/" {
			t.Errorf("admission=%+v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"count":1,"results":[{"url":"`+body.URL+`","status":"admitted"}]}`)
	}))
	defer server.Close()

	configPath := writeTestConfig(t)
	statePath := filepath.Join(t.TempDir(), "frontier.db")
	environment := func(key string) string {
		switch key {
		case "FM_CRAWLER_FETCHMARK_URL":
			return server.URL
		case "FM_CRAWLER_ADMIN_API_KEY":
			return "crawler-key"
		}
		return ""
	}
	for runIndex := 0; runIndex < 2; runIndex++ {
		var stdout, stderr bytes.Buffer
		code := run(context.Background(), []string{"-config", configPath, "-state", statePath, "-once"}, &stdout, &stderr, environment)
		if code != 0 {
			t.Fatalf("run %d code=%d stderr=%s", runIndex, code, stderr.String())
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d want one per seed only on first run", calls.Load())
	}
}

func TestOnceSynchronizesLinkPolicyBeforeDiscoverySourceFailure(t *testing.T) {
	configRaw := []byte(`{
  "version":3,
  "contact_uri":"mailto:operator@example.org",
  "max_frontier_urls":10,
  "max_discovery_sources":10,
  "max_discovery_page_memberships":10,
  "max_discovery_source_edges":10,
  "max_link_edges":100,
  "jobs":[{
    "id":"docs",
    "seeds":["https://localhost/docs/root"],
    "allowed_domains":["localhost"],
    "allowed_path_prefixes":["/docs/"],
    "max_urls_per_run":2,
    "max_source_polls_per_run":1,
    "refresh_interval":"24h",
    "rejection_recheck_interval":"168h",
    "min_host_interval":"1s",
    "max_attempts":2,
    "link_expansion":{"max_links_per_page":1,"max_pages_per_run":1},
    "discovery_sources":[{
      "url":"https://localhost/docs/feed.xml",
      "kind":"feed",
      "allowed_source_path_prefixes":["/docs/"],
      "poll_interval":"1h",
      "error_recheck_interval":"1h",
      "max_compressed_bytes":4096,
      "max_decompressed_bytes":8192,
      "max_entries":10,
      "max_child_sources":0,
      "max_depth":0
    }]
  }]
}`)
	config, err := focusedcrawl.DecodeConfig(bytes.NewReader(configRaw))
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "crawl.json")
	if err := os.WriteFile(configPath, configRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(t.TempDir(), "frontier.db")
	frontier, err := crawlfrontier.Open(statePath, crawlfrontier.Options{
		MaxEntries: 10, MaxSources: 10, MaxPagesPerSource: 10, MaxChildrenPerSource: 10,
		MaxMemberships: 10, MaxSourceEdges: 10, MaxLinkEdges: 100, MaxLinksPerPage: 64, MaxSourceDepth: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	seed := config.Seeds()[0]
	if err := frontier.SyncSeeds([]crawlfrontier.Seed{{Key: seed.Key, JobID: seed.JobID, URL: seed.URL}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := frontier.SyncLinkPolicies([]crawlfrontier.LinkPolicy{{JobID: seed.JobID, MaxLinksPerPage: 64}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	leased, err := frontier.LeaseDueJob(seed.JobID, time.Now().UTC(), 1, time.Minute)
	if err != nil || len(leased) != 1 {
		t.Fatalf("leased=%+v error=%v", leased, err)
	}
	if _, err := frontier.CompleteWithLinks(crawlfrontier.LinkCompletion{
		Key: seed.Key, LeaseGeneration: leased[0].LeaseGeneration, Outcome: crawlfrontier.StateSucceeded,
		NextEligible: time.Now().UTC().Add(time.Hour), Links: []crawlfrontier.Seed{{
			Key: "old-child", JobID: seed.JobID, URL: "https://localhost/docs/old-child",
		}},
	}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	source := config.SourceSeeds()[0]
	if err := frontier.SyncDiscoverySources([]crawlfrontier.DiscoverySourceSeed{{
		Key: source.Key, JobID: source.JobID, URL: "https://localhost/docs/conflicting-feed.xml",
		Kind: crawlfrontier.DiscoverySourceFeed, RootKey: source.Key,
	}}, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := frontier.Close(); err != nil {
		t.Fatal(err)
	}

	environment := func(key string) string {
		switch key {
		case "FM_CRAWLER_FETCHMARK_URL":
			return "http://localhost:1"
		case "FM_CRAWLER_ADMIN_API_KEY":
			return "crawler-key"
		}
		return ""
	}
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"-config", configPath, "-state", statePath, "-once", "-timeout", "1s"}, &stdout, &stderr, environment); code != 1 {
		t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "sync discovery sources") {
		t.Fatalf("stderr=%s", stderr.String())
	}

	maxLinkEdges, maxLinksPerPage := config.LinkFrontierOpenCaps()
	frontier, err = crawlfrontier.Open(statePath, crawlfrontier.Options{
		MaxEntries: 10, MaxSources: 10, MaxPagesPerSource: 10, MaxChildrenPerSource: 10,
		MaxMemberships: 10, MaxSourceEdges: 10, MaxLinkEdges: maxLinkEdges, MaxLinksPerPage: maxLinksPerPage, MaxSourceDepth: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer frontier.Close()
	leased, err = frontier.LeaseDueJob(seed.JobID, time.Now().UTC().Add(time.Hour), 10, time.Minute)
	if err != nil || len(leased) != 1 || leased[0].Key != seed.Key || !leased[0].CanExpand {
		t.Fatalf("post-failure leases=%+v error=%v", leased, err)
	}
	_, err = frontier.CompleteWithLinks(crawlfrontier.LinkCompletion{
		Key: seed.Key, LeaseGeneration: leased[0].LeaseGeneration, Outcome: crawlfrontier.StateSucceeded,
		NextEligible: time.Now().UTC().Add(2 * time.Hour), Links: []crawlfrontier.Seed{
			{Key: "a", JobID: seed.JobID, URL: "https://localhost/docs/a"},
			{Key: "b", JobID: seed.JobID, URL: "https://localhost/docs/b"},
		},
	}, time.Now().UTC().Add(time.Hour))
	if !errors.Is(err, crawlfrontier.ErrInvalidTransition) {
		t.Fatalf("narrowed policy completion error=%v", err)
	}
}

func TestRunRequiresExactlyOneModeAndAbsolutePaths(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"-config", "relative.json", "-dry-run"},
		{"-config", "/tmp/config.json", "-dry-run", "-once"},
	} {
		var stdout, stderr bytes.Buffer
		if code := run(context.Background(), args, &stdout, &stderr, func(string) string { return "" }); code != 2 {
			t.Fatalf("args=%v code=%d stderr=%s", args, code, stderr.String())
		}
	}
}

func TestOnceRejectsInvalidEndpointBeforeCreatingFrontier(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "frontier.db")
	environment := func(key string) string {
		switch key {
		case "FM_CRAWLER_FETCHMARK_URL":
			return "https://user:secret@fetchmark.example"
		case "FM_CRAWLER_ADMIN_API_KEY":
			return "crawler-key"
		}
		return ""
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"-config", writeTestConfig(t), "-state", statePath, "-once"}, &stdout, &stderr, environment)
	if code != 2 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatalf("invalid endpoint created state: %v", err)
	}
}

func TestPermanentAdmissionErrorsStopTheRun(t *testing.T) {
	err := scheduledAdmissionError{cause: &corpusclient.AdmissionError{Class: corpusclient.FailurePermanent}}
	if !err.Fatal() || err.Temporary() {
		t.Fatalf("fatal=%t temporary=%t", err.Fatal(), err.Temporary())
	}
}

func writeTestConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "crawl.json")
	raw := []byte(`{
  "version":3,
  "contact_uri":"mailto:operator@example.org",
  "max_frontier_urls":100,
  "max_discovery_sources":0,
  "jobs":[{
    "id":"docs",
    "seeds":["https://example.org/docs/a","https://other.org/docs/b"],
    "allowed_domains":["example.org","other.org"],
    "allowed_path_prefixes":["/docs/"],
    "max_urls_per_run":10,
    "refresh_interval":"24h",
    "rejection_recheck_interval":"168h",
    "min_host_interval":"1s",
    "max_attempts":4
  }]
}`)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
