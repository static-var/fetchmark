package focusedcrawl

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestCrawlerExampleUsesValidVersionThreeConfig(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	file, err := os.Open(filepath.Join(filepath.Dir(currentFile), "..", "..", "..", "deploy", "crawler.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	config, err := DecodeConfig(file)
	if err != nil {
		t.Fatal(err)
	}
	if config.Version != 3 || len(config.SourceSeeds()) != 1 {
		t.Fatalf("config version=%d sources=%+v", config.Version, config.SourceSeeds())
	}
}

func TestCrawlerPackTemplatesUseValidVersionThreeConfigs(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	packDir := filepath.Join(filepath.Dir(currentFile), "..", "..", "..", "deploy", "crawler-packs")
	entries, err := os.ReadDir(packDir)
	if err != nil {
		t.Fatal(err)
	}
	validated := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".example.json") {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			file, err := os.Open(filepath.Join(packDir, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			config, err := DecodeConfig(file)
			if err != nil {
				t.Fatal(err)
			}
			if config.Version != 3 || len(config.Jobs) != 1 || config.Jobs[0].LinkExpansion == nil {
				t.Fatalf("config=%+v", config)
			}
		})
		validated++
	}
	if validated != 4 {
		t.Fatalf("validated %d pack templates, want 4", validated)
	}
}

func validV3Config(sourceOverrides string) string {
	return fmt.Sprintf(`{
  "version": 3,
  "contact_uri": "https://operator.example/crawler",
  "max_frontier_urls": 100,
  "max_discovery_sources": 100,
  "max_discovery_page_memberships": 1000,
  "max_discovery_source_edges": 1000,
  "max_link_edges": 1000,
  "jobs": [{
    "id": "docs",
    "seeds": [],
    "allowed_domains": ["example.org"],
    "allowed_path_prefixes": ["/docs/"],
    "max_urls_per_run": 20,
    "max_source_polls_per_run": 4,
    "refresh_interval": "24h",
    "rejection_recheck_interval": "168h",
    "min_host_interval": "2s",
    "max_attempts": 4,
    "link_expansion": {
      "max_links_per_page": 32,
      "max_pages_per_run": 20
    },
    "discovery_sources": [{
      "url": "https://EXAMPLE.org:443/sitemaps/docs.xml?b=2&a=1#fragment",
      "kind": "sitemap",
      "allowed_source_path_prefixes": ["/sitemaps/"],
      "poll_interval": "6h",
      "error_recheck_interval": "1h",
      "max_compressed_bytes": 2097152,
      "max_decompressed_bytes": 8388608,
      "max_entries": 10000,
      "max_child_sources": 128,
      "max_depth": 2%s
    }]
  }]
}`, sourceOverrides)
}

func TestDecodeConfigV3ProducesCanonicalBoundedDiscoverySources(t *testing.T) {
	config, err := DecodeConfig(strings.NewReader(validV3Config("")))
	if err != nil {
		t.Fatal(err)
	}
	if config.Version != 3 || config.ContactURI != "https://operator.example/crawler" || config.MaxDiscoverySources != 100 || config.MaxLinkEdges != 1000 {
		t.Fatalf("config=%+v", config)
	}
	if len(config.Jobs) != 1 || len(config.Jobs[0].DiscoverySources) != 1 {
		t.Fatalf("jobs=%+v", config.Jobs)
	}
	source := config.Jobs[0].DiscoverySources[0]
	if source.URL != "https://example.org/sitemaps/docs.xml?a=1&b=2" {
		t.Fatalf("source URL=%q", source.URL)
	}
	if source.Kind != SourceKindSitemap || source.PollInterval.Duration != 6*time.Hour || source.ErrorRecheckInterval.Duration != time.Hour {
		t.Fatalf("source=%+v", source)
	}
	if expansion := config.Jobs[0].LinkExpansion; expansion == nil || expansion.MaxLinksPerPage != 32 || expansion.MaxPagesPerRun != 20 {
		t.Fatalf("link expansion=%+v", expansion)
	}
	if len(config.Seeds()) != 0 {
		t.Fatalf("seeds=%+v", config.Seeds())
	}
}

func TestLinkFrontierOpenCapsPermitPolicyReconciliation(t *testing.T) {
	tests := []struct {
		name      string
		maxEdges  int
		wantEdges int
	}{
		{name: "disabled expansion uses format ceiling", maxEdges: 0, wantEdges: maxLinkEdges},
		{name: "positive storage cap remains authoritative", maxEdges: 10, wantEdges: 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := Config{MaxLinkEdges: test.maxEdges, Jobs: []Job{{
				LinkExpansion: &LinkExpansion{MaxLinksPerPage: 1},
			}}}
			gotEdges, gotPerPage := config.LinkFrontierOpenCaps()
			if gotEdges != test.wantEdges || gotPerPage != maxOutboundLinksPerPage {
				t.Fatalf("LinkFrontierOpenCaps() = (%d, %d), want (%d, %d)", gotEdges, gotPerPage, test.wantEdges, maxOutboundLinksPerPage)
			}
		})
	}
}

func TestConfigSourceSeedsAndRootSourceAreDeterministicDefensiveCopies(t *testing.T) {
	config, err := DecodeConfig(strings.NewReader(validV3Config("")))
	if err != nil {
		t.Fatal(err)
	}
	seeds := config.SourceSeeds()
	if len(seeds) != 1 || seeds[0].Key == "" || seeds[0].JobID != "docs" ||
		seeds[0].URL != "https://example.org/sitemaps/docs.xml?a=1&b=2" || seeds[0].Kind != SourceKindSitemap {
		t.Fatalf("source seeds=%+v", seeds)
	}
	if seeds[0].Key == configKey("docs", seeds[0].URL) {
		t.Fatal("source key collided with the page-seed key namespace")
	}
	secondConfig, err := DecodeConfig(strings.NewReader(validV3Config("")))
	if err != nil || secondConfig.SourceSeeds()[0].Key != seeds[0].Key {
		t.Fatalf("source key changed across decodes: second=%+v error=%v", secondConfig.SourceSeeds(), err)
	}
	root, ok := config.RootSource(seeds[0].Key)
	if !ok || root.Seed != seeds[0] || len(root.Config.AllowedSourcePathPrefixes) != 1 {
		t.Fatalf("root=%+v ok=%t", root, ok)
	}

	firstKey := seeds[0].Key
	seeds[0].Key = "mutated"
	root.Config.AllowedSourcePathPrefixes[0] = "/mutated/"
	again := config.SourceSeeds()
	againRoot, ok := config.RootSource(firstKey)
	if !ok || again[0].Key != firstKey || againRoot.Config.AllowedSourcePathPrefixes[0] != "/sitemaps/" {
		t.Fatalf("config exposed mutable source state: seeds=%+v root=%+v", again, againRoot)
	}
}

func TestDecodeConfigV3ValidatesDiscoveryPolicyAndBounds(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "missing contact", body: strings.Replace(validV3Config(""), "  \"contact_uri\": \"https://operator.example/crawler\",\n", "", 1)},
		{name: "insecure contact", body: strings.Replace(validV3Config(""), "https://operator.example/crawler", "http://operator.example/crawler", 1)},
		{name: "credentialed contact", body: strings.Replace(validV3Config(""), "https://operator.example/crawler", "https://user:secret@operator.example/crawler", 1)},
		{name: "ambiguous contact path", body: strings.Replace(validV3Config(""), "https://operator.example/crawler", "https://operator.example/a/../crawler", 1)},
		{name: "multiple mailto recipients", body: strings.Replace(validV3Config(""), "https://operator.example/crawler", "mailto:a@example.org,b@example.org", 1)},
		{name: "multiple mailto ats", body: strings.Replace(validV3Config(""), "https://operator.example/crawler", "mailto:a@example.org@example.net", 1)},
		{name: "encoded mailto", body: strings.Replace(validV3Config(""), "https://operator.example/crawler", "mailto:a%40example.org", 1)},
		{name: "zero source cap", body: strings.Replace(validV3Config(""), `"max_discovery_sources": 100`, `"max_discovery_sources": 0`, 1)},
		{name: "negative source cap", body: strings.Replace(validV3Config(""), `"max_discovery_sources": 100`, `"max_discovery_sources": -1`, 1)},
		{name: "global source cap", body: strings.Replace(validV3Config(""), `"max_discovery_sources": 100`, `"max_discovery_sources": 100001`, 1)},
		{name: "zero page membership cap", body: strings.Replace(validV3Config(""), `"max_discovery_page_memberships": 1000`, `"max_discovery_page_memberships": 0`, 1)},
		{name: "page membership cap", body: strings.Replace(validV3Config(""), `"max_discovery_page_memberships": 1000`, `"max_discovery_page_memberships": 1000001`, 1)},
		{name: "zero source edge cap", body: strings.Replace(validV3Config(""), `"max_discovery_source_edges": 1000`, `"max_discovery_source_edges": 0`, 1)},
		{name: "source edge cap", body: strings.Replace(validV3Config(""), `"max_discovery_source_edges": 1000`, `"max_discovery_source_edges": 1000001`, 1)},
		{name: "zero link edge cap", body: strings.Replace(validV3Config(""), `"max_link_edges": 1000`, `"max_link_edges": 0`, 1)},
		{name: "link edge cap", body: strings.Replace(validV3Config(""), `"max_link_edges": 1000`, `"max_link_edges": 1000001`, 1)},
		{name: "zero per-page links", body: strings.Replace(validV3Config(""), `"max_links_per_page": 32`, `"max_links_per_page": 0`, 1)},
		{name: "per-page link cap", body: strings.Replace(validV3Config(""), `"max_links_per_page": 32`, `"max_links_per_page": 65`, 1)},
		{name: "zero link pages per run", body: strings.Replace(validV3Config(""), `"max_pages_per_run": 20`, `"max_pages_per_run": 0`, 1)},
		{name: "link pages exceed admission run", body: strings.Replace(validV3Config(""), `"max_pages_per_run": 20`, `"max_pages_per_run": 21`, 1)},
		{name: "source outside domain", body: strings.Replace(validV3Config(""), "https://EXAMPLE.org:443/sitemaps/", "https://other.example/sitemaps/", 1)},
		{name: "source outside source path", body: strings.Replace(validV3Config(""), "/sitemaps/docs.xml", "/outside/docs.xml", 1)},
		{name: "ambiguous source path", body: strings.Replace(validV3Config(""), "/sitemaps/docs.xml", "/sitemaps%2fdocs.xml", 1)},
		{name: "ambiguous source prefix", body: strings.Replace(validV3Config(""), `"/sitemaps/"`, `"/sitemaps/%2fprivate/"`, 1)},
		{name: "invalid source kind", body: strings.Replace(validV3Config(""), `"kind": "sitemap"`, `"kind": "html"`, 1)},
		{name: "zero source polls", body: strings.Replace(validV3Config(""), `"max_source_polls_per_run": 4`, `"max_source_polls_per_run": 0`, 1)},
		{name: "too many source polls", body: strings.Replace(validV3Config(""), `"max_source_polls_per_run": 4`, `"max_source_polls_per_run": 65`, 1)},
		{name: "fast source poll", body: strings.Replace(validV3Config(""), `"poll_interval": "6h"`, `"poll_interval": "4m59s"`, 1)},
		{name: "slow source poll", body: strings.Replace(validV3Config(""), `"poll_interval": "6h"`, `"poll_interval": "721h"`, 1)},
		{name: "fast source error recheck", body: strings.Replace(validV3Config(""), `"error_recheck_interval": "1h"`, `"error_recheck_interval": "4m59s"`, 1)},
		{name: "slow source error recheck", body: strings.Replace(validV3Config(""), `"error_recheck_interval": "1h"`, `"error_recheck_interval": "25h"`, 1)},
		{name: "compressed cap", body: strings.Replace(validV3Config(""), `"max_compressed_bytes": 2097152`, `"max_compressed_bytes": 4194305`, 1)},
		{name: "zero compressed bytes", body: strings.Replace(validV3Config(""), `"max_compressed_bytes": 2097152`, `"max_compressed_bytes": 0`, 1)},
		{name: "decompressed cap", body: strings.Replace(validV3Config(""), `"max_decompressed_bytes": 8388608`, `"max_decompressed_bytes": 16777217`, 1)},
		{name: "zero decompressed bytes", body: strings.Replace(validV3Config(""), `"max_decompressed_bytes": 8388608`, `"max_decompressed_bytes": 0`, 1)},
		{name: "entry cap", body: strings.Replace(validV3Config(""), `"max_entries": 10000`, `"max_entries": 10001`, 1)},
		{name: "zero entries", body: strings.Replace(validV3Config(""), `"max_entries": 10000`, `"max_entries": 0`, 1)},
		{name: "child cap", body: strings.Replace(validV3Config(""), `"max_child_sources": 128`, `"max_child_sources": 257`, 1)},
		{name: "negative children", body: strings.Replace(validV3Config(""), `"max_child_sources": 128`, `"max_child_sources": -1`, 1)},
		{name: "depth cap", body: strings.Replace(validV3Config(""), `"max_depth": 2`, `"max_depth": 5`, 1)},
		{name: "negative depth", body: strings.Replace(validV3Config(""), `"max_depth": 2`, `"max_depth": -1`, 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeConfig(strings.NewReader(test.body)); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestDecodeConfigRejectsVersionOneAndJobsWithoutInputs(t *testing.T) {
	versionOne := strings.Replace(validV3Config(""), `"version": 3`, `"version": 1`, 1)
	if _, err := DecodeConfig(strings.NewReader(versionOne)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("version 1 error=%v", err)
	}

	emptyJob := `{
  "version":3, "contact_uri":"mailto:operator@example.org",
  "max_frontier_urls":100, "max_discovery_sources":0,
  "jobs":[{
    "id":"empty", "seeds":[], "allowed_domains":["example.org"],
    "allowed_path_prefixes":["/"], "max_urls_per_run":1,
    "refresh_interval":"1h", "rejection_recheck_interval":"24h",
    "min_host_interval":"1s", "max_attempts":2
  }]
}`
	if _, err := DecodeConfig(strings.NewReader(emptyJob)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty job error=%v", err)
	}
}

func TestConfigRejectsMoreThanMaximumRootSources(t *testing.T) {
	sources := make([]DiscoverySource, maxRootDiscoverySourcesPerJob+1)
	for index := range sources {
		sources[index] = DiscoverySource{
			URL: "https://example.org/sitemaps/" + fmt.Sprint(index) + ".xml", Kind: SourceKindSitemap,
			AllowedSourcePathPrefixes: []string{"/sitemaps/"}, PollInterval: Duration{time.Hour},
			ErrorRecheckInterval: Duration{time.Hour}, MaxCompressedBytes: 1024,
			MaxDecompressedBytes: 2048, MaxEntries: 10, MaxChildSources: 2, MaxDepth: 1,
		}
	}
	config := Config{
		Version: 3, ContactURI: "mailto:operator@example.org", MaxFrontierURLs: 100,
		MaxDiscoverySources: len(sources), MaxDiscoveryPageMemberships: 1000, MaxDiscoverySourceEdges: 1000, Jobs: []Job{{
			ID: "docs", AllowedDomains: []string{"example.org"}, AllowedPathPrefixes: []string{"/docs/"},
			MaxURLsPerRun: 1, MaxSourcePollsPerRun: 1, RefreshInterval: Duration{time.Hour},
			RejectionRecheckInterval: Duration{24 * time.Hour}, MinHostInterval: Duration{time.Second},
			MaxAttempts: 2, DiscoverySources: sources,
		}},
	}
	if err := config.validate(); err == nil {
		t.Fatal("oversized configured root list was accepted")
	}
}

func TestDecodeConfigV3RejectsDuplicateCanonicalSourcesAcrossJobs(t *testing.T) {
	first := validV3Config("")
	secondJob := `,
  {
    "id":"other", "seeds":["https://other.example/docs/"],
    "allowed_domains":["other.example","example.org"], "allowed_path_prefixes":["/docs/"],
    "max_urls_per_run":1, "max_source_polls_per_run":1,
    "refresh_interval":"1h", "rejection_recheck_interval":"24h", "min_host_interval":"1s", "max_attempts":2,
    "discovery_sources":[{
      "url":"https://example.org:443/sitemaps/docs.xml?b=2&a=1", "kind":"auto",
      "allowed_source_path_prefixes":["/sitemaps/"], "poll_interval":"1h", "error_recheck_interval":"1h",
      "max_compressed_bytes":1024, "max_decompressed_bytes":2048, "max_entries":10,
      "max_child_sources":2, "max_depth":1
    }]
  }`
	body := strings.Replace(first, "\n  }]\n}", "\n  }"+secondJob+"]\n}", 1)
	if _, err := DecodeConfig(strings.NewReader(body)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error=%v", err)
	}
}

func TestDiscoveryLocalLimitsStayBelowSitemapProtocolMaxima(t *testing.T) {
	if maxSourceEntries >= sitemapProtocolMaxEntries {
		t.Fatalf("local entry cap %d must be below protocol maximum %d", maxSourceEntries, sitemapProtocolMaxEntries)
	}
	if maxSourceDecompressedBytes >= sitemapProtocolMaxDecompressedBytes {
		t.Fatalf("local decompressed cap %d must be below protocol maximum %d", maxSourceDecompressedBytes, sitemapProtocolMaxDecompressedBytes)
	}
}

func TestDecodeConfigProducesCanonicalScopedSeeds(t *testing.T) {
	config, err := DecodeConfig(strings.NewReader(`{
  "version": 3,
  "contact_uri": "mailto:operator@example.org",
  "max_frontier_urls": 100,
  "max_discovery_sources": 0,
  "jobs": [{
    "id": "docs",
    "seeds": ["https://EXAMPLE.org:443/docs/start?utm_source=seed#top"],
    "allowed_domains": ["example.org"],
    "allowed_path_prefixes": ["/docs/"],
    "denied_path_prefixes": ["/docs/private/"],
    "denied_urls": ["https://example.org/docs/removed"],
    "max_urls_per_run": 20,
    "refresh_interval": "24h",
    "rejection_recheck_interval": "168h",
    "min_host_interval": "2s",
    "max_attempts": 4
  }]
}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(config.Jobs) != 1 || config.Jobs[0].RefreshInterval.Duration != 24*time.Hour {
		t.Fatalf("config=%+v", config)
	}
	seeds := config.Seeds()
	if len(seeds) != 1 || seeds[0].URL != "https://example.org/docs/start" || seeds[0].JobID != "docs" || seeds[0].Key == "" {
		t.Fatalf("seeds=%+v", seeds)
	}
	if !config.Jobs[0].Allows("https://example.org/docs/page") {
		t.Fatal("expected scoped page to be allowed")
	}
	for _, denied := range []string{
		"https://example.org/docs/private/secret",
		"https://example.org/docs/removed",
		"https://example.org/outside",
		"https://other.example/docs/page",
	} {
		if config.Jobs[0].Allows(denied) {
			t.Fatalf("unexpected allow for %q", denied)
		}
	}
}

func TestDecodeConfigRejectsUnsafeAmbiguousAndUnboundedJobs(t *testing.T) {
	validJob := `{
    "id":"docs", "seeds":["https://example.org/docs/"],
    "allowed_domains":["example.org"], "allowed_path_prefixes":["/docs/"],
    "max_urls_per_run":20, "refresh_interval":"24h",
    "rejection_recheck_interval":"168h", "min_host_interval":"2s", "max_attempts":4
  }`
	tests := []struct {
		name string
		body string
		want error
	}{
		{name: "unknown field", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"extra":true,"jobs":[` + validJob + `]}`, want: ErrInvalidConfig},
		{name: "trailing json", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"jobs":[` + validJob + `]} {}`, want: ErrInvalidConfig},
		{name: "credentialed seed", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"jobs":[` + strings.Replace(validJob, "https://example.org/docs/", "https://user:secret@example.org/docs/", 1) + `]}`, want: ErrInvalidConfig},
		{name: "wildcard domain", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"jobs":[` + strings.Replace(validJob, `"example.org"]`, `"*.example.org"]`, 1) + `]}`, want: ErrInvalidConfig},
		{name: "unicode domain", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"jobs":[` + strings.ReplaceAll(validJob, "example.org", "éxample.org") + `]}`, want: ErrInvalidConfig},
		{name: "trailing dot domain", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"jobs":[` + strings.ReplaceAll(validJob, "example.org", "example.org.") + `]}`, want: ErrInvalidConfig},
		{name: "noncanonical port", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"jobs":[` + strings.Replace(validJob, "example.org/docs/", "example.org:0443/docs/", 1) + `]}`, want: ErrInvalidConfig},
		{name: "encoded separator prefix", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"jobs":[` + strings.Replace(validJob, `"/docs/"]`, `"/docs/%2fprivate/"]`, 1) + `]}`, want: ErrInvalidConfig},
		{name: "dot segment prefix", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"jobs":[` + strings.Replace(validJob, `"/docs/"]`, `"/docs/../private/"]`, 1) + `]}`, want: ErrInvalidConfig},
		{name: "invalid utf8 path", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"jobs":[` + strings.Replace(validJob, "/docs/\"],", "/docs/%FF\"],", 1) + `]}`, want: ErrInvalidConfig},
		{name: "seed outside scope", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"jobs":[` + strings.Replace(validJob, "/docs/\"],\n    \"allowed_domains", "/other/\"],\n    \"allowed_domains", 1) + `]}`, want: ErrInvalidConfig},
		{name: "zero cap", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":0,"max_discovery_sources":0,"jobs":[` + validJob + `]}`, want: ErrInvalidConfig},
		{name: "too-fast host", body: `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"jobs":[` + strings.Replace(validJob, `"2s"`, `"100ms"`, 1) + `]}`, want: ErrInvalidConfig},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeConfig(strings.NewReader(test.body))
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want %v", err, test.want)
			}
		})
	}
}

func TestDecodeConfigRejectsDuplicateCanonicalSeedsAcrossJobs(t *testing.T) {
	_, err := DecodeConfig(strings.NewReader(`{
  "version":3, "contact_uri":"mailto:operator@example.org", "max_frontier_urls":100, "max_discovery_sources":0, "jobs":[
    {"id":"one","seeds":["https://example.org/docs?a=1&utm_source=x"],"allowed_domains":["example.org"],"allowed_path_prefixes":["/"],"max_urls_per_run":1,"refresh_interval":"1h","rejection_recheck_interval":"24h","min_host_interval":"1s","max_attempts":2},
    {"id":"two","seeds":["https://EXAMPLE.org:443/docs?a=1#x"],"allowed_domains":["example.org"],"allowed_path_prefixes":["/"],"max_urls_per_run":1,"refresh_interval":"1h","rejection_recheck_interval":"24h","min_host_interval":"1s","max_attempts":2}
  ]
}`))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error=%v", err)
	}
}

func TestDecodeConfigRejectsInputBeyondBoundInsteadOfAcceptingTruncatedJSON(t *testing.T) {
	prefix := `{"version":3,"contact_uri":"mailto:operator@example.org","max_frontier_urls":100,"max_discovery_sources":0,"jobs":[]}`
	_, err := DecodeConfig(strings.NewReader(prefix + strings.Repeat(" ", maxConfigBytes-len(prefix)+1)))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("error=%v", err)
	}
}

func TestJobValidationMatchesFocusedAdmissionRequestBounds(t *testing.T) {
	base := func() Job {
		return Job{
			ID: "docs", SeedURLs: []string{"https://example.org/docs/start"}, AllowedDomains: []string{"example.org"},
			AllowedPathPrefixes: []string{"/docs/"}, MaxURLsPerRun: 1, RefreshInterval: Duration{time.Hour},
			RejectionRecheckInterval: Duration{24 * time.Hour}, MinHostInterval: Duration{time.Second}, MaxAttempts: 2,
		}
	}
	t.Run("too many prefixes", func(t *testing.T) {
		job := base()
		job.AllowedPathPrefixes = make([]string, maxPolicyItems+1)
		for index := range job.AllowedPathPrefixes {
			job.AllowedPathPrefixes[index] = "/docs/" + strings.Repeat("x", index+1) + "/"
		}
		if err := job.validate(); err == nil {
			t.Fatal("oversized prefix list was accepted")
		}
	})
	t.Run("oversized prefix", func(t *testing.T) {
		job := base()
		job.AllowedPathPrefixes = []string{"/" + strings.Repeat("x", maxPathPrefixBytes)}
		if err := job.validate(); err == nil {
			t.Fatal("oversized prefix was accepted")
		}
	})
	t.Run("duplicate denied URL", func(t *testing.T) {
		job := base()
		job.DeniedURLs = []string{"https://example.org/removed#one", "https://EXAMPLE.org:443/removed#two"}
		if err := job.validate(); err == nil {
			t.Fatal("duplicate denied URL was accepted")
		}
	})
	t.Run("encoded path cannot bypass denied prefix", func(t *testing.T) {
		job := base()
		job.SeedURLs = []string{"https://example.org/docs/%73ecret/page"}
		job.DeniedPathPrefixes = []string{"/docs/secret/"}
		if err := job.validate(); err == nil {
			t.Fatal("encoded denied seed was accepted")
		}
	})
	t.Run("encoded path cannot bypass exact denied URL", func(t *testing.T) {
		job := base()
		job.SeedURLs = []string{"https://example.org/docs/%73ecret"}
		job.DeniedURLs = []string{"https://example.org/docs/secret"}
		if err := job.validate(); err == nil {
			t.Fatal("encoded exact-denied seed was accepted")
		}
	})
	t.Run("maximum future URL budget", func(t *testing.T) {
		job := base()
		prefix := "https://example.org/docs/"
		job.SeedURLs = []string{prefix + strings.Repeat("x", maxWebURLBytes-len(prefix))}
		if err := job.validate(); err != nil {
			t.Fatalf("maximum URL was rejected: %v", err)
		}
	})
	t.Run("source-only aggregate request budget", func(t *testing.T) {
		job := base()
		job.SeedURLs = nil
		job.MaxSourcePollsPerRun = 1
		job.DiscoverySources = []DiscoverySource{{
			URL: "https://example.org/sitemaps/docs.xml", Kind: SourceKindSitemap,
			AllowedSourcePathPrefixes: []string{"/sitemaps/"}, PollInterval: Duration{time.Hour},
			ErrorRecheckInterval: Duration{time.Hour}, MaxCompressedBytes: 1024,
			MaxDecompressedBytes: 2048, MaxEntries: 10, MaxChildSources: 2, MaxDepth: 1,
		}}
		for index := 0; index < maxPolicyItems-1; index++ {
			job.AllowedPathPrefixes = append(job.AllowedPathPrefixes, "/allow/"+strings.Repeat("a", 1980)+string(rune('A'+index%26))+strings.Repeat("b", index/26)+"/")
		}
		for index := 0; index < maxPolicyItems; index++ {
			job.DeniedPathPrefixes = append(job.DeniedPathPrefixes, "/deny/"+strings.Repeat("c", 1980)+string(rune('A'+index%26))+strings.Repeat("d", index/26)+"/")
		}
		if err := job.validate(); err == nil || !strings.Contains(err.Error(), "request") && !strings.Contains(err.Error(), "policy exceeds") {
			t.Fatalf("aggregate oversized policy error = %v", err)
		}
	})
}
