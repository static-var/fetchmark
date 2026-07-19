package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("FM_API_KEYS", "k1, k2 ,")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.ListenAddr != ":8080" {
		t.Errorf("ListenAddr default = %q", c.ListenAddr)
	}
	if len(c.APIKeys) != 2 || c.APIKeys[0] != "k1" || c.APIKeys[1] != "k2" {
		t.Errorf("APIKeys cleaned = %#v", c.APIKeys)
	}
	if c.DashboardEnabled() {
		t.Error("dashboard should be disabled when creds unset")
	}
	if !c.RespectRobots {
		t.Error("RespectRobots should default to true")
	}
	if c.SummarizeMaxTokensCap != 4096 {
		t.Errorf("SummarizeMaxTokensCap default = %d", c.SummarizeMaxTokensCap)
	}
	if c.SummarizeMaxTimeout.String() != "2m0s" {
		t.Errorf("SummarizeMaxTimeout default = %v", c.SummarizeMaxTimeout)
	}
	if c.SummarizeMaxInstructionsLen != 4000 {
		t.Errorf("SummarizeMaxInstructionsLen default = %d", c.SummarizeMaxInstructionsLen)
	}
	if c.SummarizeAllowModelOverride {
		t.Error("SummarizeAllowModelOverride should default to false")
	}
	if c.SummarizeAllowProviderOverride {
		t.Error("SummarizeAllowProviderOverride should default to false")
	}
	if c.SummarizeAllowThinkingOverride {
		t.Error("SummarizeAllowThinkingOverride should default to false")
	}
	if c.ArtifactConcurrency != 3 {
		t.Errorf("ArtifactConcurrency default = %d", c.ArtifactConcurrency)
	}
	if c.MaxRequestSourceBytes != 64<<20 || c.MaxRequestOutputBytes != 128<<20 {
		t.Errorf("request byte budgets = (%d, %d)", c.MaxRequestSourceBytes, c.MaxRequestOutputBytes)
	}
	if c.MemoryCacheEntries != 512 || c.MemoryCacheBytes != 128<<20 || c.CacheMaxValueBytes != 8<<20 {
		t.Errorf("memory cache limits = (%d, %d, %d)", c.MemoryCacheEntries, c.MemoryCacheBytes, c.CacheMaxValueBytes)
	}
	if c.LocalCorpusMode != "disabled" || c.LocalCorpusMaxAge != 30*24*time.Hour || c.LocalExpirySweepInterval != 15*time.Minute || c.LocalIndexPath != "" || c.LocalArtifactPath != "" || c.LocalIndexMaxDocumentBytes != 2<<20 || c.LocalIndexMaxBytes != 1<<30 || c.LocalIndexMaxDocuments != 100000 || c.LocalArtifactMaxBytes != 1<<30 || c.LocalArtifactMaxValueBytes != 20<<20 || c.LocalArtifactMaxEntries != 100000 || c.LocalArchiveMaxVersions != 100000 || c.LocalArchiveMaxVersionsPerURL != 16 {
		t.Errorf("local corpus defaults = %+v", c)
	}
	if c.DiscoveryCacheTTL != 30*time.Second || c.DiscoveryCacheStaleTTL != 2*time.Minute || c.DiscoveryRefreshTimeout != 30*time.Second {
		t.Errorf("discovery cache durations = (%v, %v, %v)", c.DiscoveryCacheTTL, c.DiscoveryCacheStaleTTL, c.DiscoveryRefreshTimeout)
	}
	if c.DiscoveryCacheEntries != 256 || c.DiscoveryCacheBytes != 16<<20 || c.DiscoveryCacheMaxEntryBytes != 1<<20 || c.DiscoveryMaxInflight != 16 {
		t.Errorf("discovery cache limits = (%d, %d, %d, %d)", c.DiscoveryCacheEntries, c.DiscoveryCacheBytes, c.DiscoveryCacheMaxEntryBytes, c.DiscoveryMaxInflight)
	}
	if c.DiscoveryProviderMaxBody != 2<<20 || c.CrossrefMailto != "" || c.PubMedEmail != "" || c.YaCyURL != "" || c.YaCyResource != "local" || c.YaCyAllowInsecureHTTP {
		t.Errorf("native discovery defaults = (%d, %q, %q, %q, %q, %t)", c.DiscoveryProviderMaxBody, c.CrossrefMailto, c.PubMedEmail, c.YaCyURL, c.YaCyResource, c.YaCyAllowInsecureHTTP)
	}
	if c.OpenPackRegistryFile != "" {
		t.Errorf("OpenPackRegistryFile default = %q", c.OpenPackRegistryFile)
	}
	if c.FederationIdentityFile != "" || c.FederationTrustRegistryFile != "" || c.FederationListenAddr != "" {
		t.Errorf("federation should default disabled: (%q, %q, %q)", c.FederationIdentityFile, c.FederationTrustRegistryFile, c.FederationListenAddr)
	}
	if c.CompatAnswerProvider != "" || c.CompatAnswerMaxTokens != 512 || c.CompatAnswerTimeout != 30*time.Second {
		t.Errorf("compat answer defaults = (%q, %d, %v)", c.CompatAnswerProvider, c.CompatAnswerMaxTokens, c.CompatAnswerTimeout)
	}
	if c.AdvancedSearchConcurrency != 4 {
		t.Errorf("AdvancedSearchConcurrency default = %d", c.AdvancedSearchConcurrency)
	}
	if c.DiscoveryPackFile != "" {
		t.Errorf("DiscoveryPackFile default = %q", c.DiscoveryPackFile)
	}
	if c.DiscoveryPrimarySource != "searxng" {
		t.Errorf("DiscoveryPrimarySource default = %q", c.DiscoveryPrimarySource)
	}
	wantPacks := []string{"general-open", "developer", "research", "knowledge", "fresh"}
	if len(c.DiscoveryEnabledPacks) != len(wantPacks) {
		t.Fatalf("DiscoveryEnabledPacks = %v", c.DiscoveryEnabledPacks)
	}
	for i := range wantPacks {
		if c.DiscoveryEnabledPacks[i] != wantPacks[i] {
			t.Fatalf("DiscoveryEnabledPacks = %v", c.DiscoveryEnabledPacks)
		}
	}
	wantSources := []string{"searxng", "wikipedia", "crossref"}
	if len(c.DiscoveryEnabledSources) != len(wantSources) {
		t.Fatalf("DiscoveryEnabledSources = %v", c.DiscoveryEnabledSources)
	}
	for i := range wantSources {
		if c.DiscoveryEnabledSources[i] != wantSources[i] {
			t.Fatalf("DiscoveryEnabledSources = %v", c.DiscoveryEnabledSources)
		}
	}
}

func TestLoadRequiresYaCyEndpointWhenSourceIsEnabled(t *testing.T) {
	t.Setenv("FM_DISCOVERY_ENABLED_SOURCES", "yacy")
	t.Setenv("FM_DISCOVERY_PRIMARY_SOURCE", "yacy")
	t.Setenv("FM_SEARXNG_URL", "")
	if _, err := Load(); err == nil || err.Error() != "FM_YACY_URL is required when the yacy discovery source is enabled" {
		t.Fatalf("Load error = %v", err)
	}
}

func TestLoadRejectsInvalidYaCyResourceWhenSourceIsEnabled(t *testing.T) {
	t.Setenv("FM_DISCOVERY_ENABLED_SOURCES", "yacy")
	t.Setenv("FM_DISCOVERY_PRIMARY_SOURCE", "yacy")
	t.Setenv("FM_SEARXNG_URL", "")
	t.Setenv("FM_YACY_URL", "https://yacy.example")
	t.Setenv("FM_YACY_RESOURCE", "remote")
	if _, err := Load(); err == nil || err.Error() != "FM_YACY_RESOURCE must be local or global" {
		t.Fatalf("Load error = %v", err)
	}
}

func TestLoad_ValidatesOpenPackRegistryPath(t *testing.T) {
	t.Setenv("FM_OPEN_PACK_REGISTRY_FILE", "relative/open-packs.json")
	if _, err := Load(); err == nil || err.Error() != "FM_OPEN_PACK_REGISTRY_FILE must be absolute when set" {
		t.Fatalf("Load error = %v", err)
	}
}

func TestLoad_ValidatesFederationConfiguration(t *testing.T) {
	t.Run("relative identity", func(t *testing.T) {
		t.Setenv("FM_FEDERATION_IDENTITY_FILE", "identity.json")
		t.Setenv("FM_FEDERATION_TRUST_REGISTRY_FILE", "/etc/fetchmark/trust.json")
		if _, err := Load(); err == nil || err.Error() != "FM_FEDERATION_IDENTITY_FILE must be absolute when set" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("relative registry", func(t *testing.T) {
		t.Setenv("FM_FEDERATION_IDENTITY_FILE", "/etc/fetchmark/identity.json")
		t.Setenv("FM_FEDERATION_TRUST_REGISTRY_FILE", "trust.json")
		if _, err := Load(); err == nil || err.Error() != "FM_FEDERATION_TRUST_REGISTRY_FILE must be absolute when set" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("paired files", func(t *testing.T) {
		t.Setenv("FM_FEDERATION_IDENTITY_FILE", "/etc/fetchmark/identity.json")
		if _, err := Load(); err == nil || err.Error() != "FM_FEDERATION_IDENTITY_FILE and FM_FEDERATION_TRUST_REGISTRY_FILE must be configured together" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("distinct files", func(t *testing.T) {
		t.Setenv("FM_FEDERATION_IDENTITY_FILE", "/etc/fetchmark/federation.json")
		t.Setenv("FM_FEDERATION_TRUST_REGISTRY_FILE", "/etc/fetchmark/federation.json")
		if _, err := Load(); err == nil || err.Error() != "federation identity and trust registry files must be distinct" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("inbound requires files", func(t *testing.T) {
		t.Setenv("FM_FEDERATION_LISTEN_ADDR", "127.0.0.1:8082")
		if _, err := Load(); err == nil || err.Error() != "federation identity and trust registry files are required when FM_FEDERATION_LISTEN_ADDR is set" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("inbound requires curated mode", func(t *testing.T) {
		t.Setenv("FM_FEDERATION_IDENTITY_FILE", "/etc/fetchmark/identity.json")
		t.Setenv("FM_FEDERATION_TRUST_REGISTRY_FILE", "/etc/fetchmark/trust.json")
		t.Setenv("FM_FEDERATION_LISTEN_ADDR", "127.0.0.1:8082")
		if _, err := Load(); err == nil || err.Error() != "FM_FEDERATION_LISTEN_ADDR requires FM_LOCAL_CORPUS_MODE=curated" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("inbound requires separate listener", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "curated")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/artifacts")
		t.Setenv("FM_ADMIN_API_KEYS", "curator-key")
		t.Setenv("FM_FEDERATION_IDENTITY_FILE", "/etc/fetchmark/identity.json")
		t.Setenv("FM_FEDERATION_TRUST_REGISTRY_FILE", "/etc/fetchmark/trust.json")
		t.Setenv("FM_FEDERATION_LISTEN_ADDR", ":8080")
		if _, err := Load(); err == nil || err.Error() != "FM_FEDERATION_LISTEN_ADDR must differ from FM_LISTEN_ADDR" {
			t.Fatalf("Load error = %v", err)
		}
	})
	for _, listener := range []string{"0.0.0.0:8080", "[::]:8080", "127.0.0.1:8080", "127.0.0.1:08080", "[::ffff:127.0.0.1]:8080"} {
		listener := listener
		t.Run("inbound rejects overlapping listener "+listener, func(t *testing.T) {
			t.Setenv("FM_LOCAL_CORPUS_MODE", "curated")
			t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
			t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/artifacts")
			t.Setenv("FM_ADMIN_API_KEYS", "curator-key")
			t.Setenv("FM_FEDERATION_IDENTITY_FILE", "/etc/fetchmark/identity.json")
			t.Setenv("FM_FEDERATION_TRUST_REGISTRY_FILE", "/etc/fetchmark/trust.json")
			t.Setenv("FM_LISTEN_ADDR", "127.0.0.1:8080")
			if strings.Contains(listener, "0.0.0.0") || strings.Contains(listener, "[::]:") {
				t.Setenv("FM_LISTEN_ADDR", ":8080")
			}
			t.Setenv("FM_FEDERATION_LISTEN_ADDR", listener)
			if _, err := Load(); err == nil || err.Error() != "FM_FEDERATION_LISTEN_ADDR must differ from FM_LISTEN_ADDR" {
				t.Fatalf("Load error = %v", err)
			}
		})
	}
	t.Run("outbound identity is opt in without listener", func(t *testing.T) {
		t.Setenv("FM_FEDERATION_IDENTITY_FILE", "/etc/fetchmark/identity.json")
		t.Setenv("FM_FEDERATION_TRUST_REGISTRY_FILE", "/etc/fetchmark/trust.json")
		configuration, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if configuration.FederationListenAddr != "" {
			t.Fatalf("FederationListenAddr = %q", configuration.FederationListenAddr)
		}
	})
}

func TestLoad_ValidatesLocalIndexConfiguration(t *testing.T) {
	t.Run("relative path", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "personal")
		t.Setenv("FM_LOCAL_INDEX_PATH", "relative/index")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_INDEX_PATH must be absolute when set" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("document budget", func(t *testing.T) {
		t.Setenv("FM_LOCAL_INDEX_MAX_DOCUMENT_BYTES", "0")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_INDEX_MAX_DOCUMENT_BYTES must be > 0" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("unknown mode", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "forever-ish")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_CORPUS_MODE must be one of disabled, ephemeral, personal, curated, archive" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("disabled rejects path", func(t *testing.T) {
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		if _, err := Load(); err == nil || err.Error() != "local corpus paths must be empty when FM_LOCAL_CORPUS_MODE is disabled or ephemeral" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("ephemeral rejects path", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "ephemeral")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		if _, err := Load(); err == nil || err.Error() != "local corpus paths must be empty when FM_LOCAL_CORPUS_MODE is disabled or ephemeral" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("personal requires paths", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "personal")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_INDEX_PATH and FM_LOCAL_ARTIFACT_PATH are required in personal mode" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("curated requires paths", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "curated")
		t.Setenv("FM_ADMIN_API_KEYS", "curator-key")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_INDEX_PATH and FM_LOCAL_ARTIFACT_PATH are required in curated mode" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("curated requires an admin API key", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "curated")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/artifacts")
		t.Setenv("FM_ADMIN_API_KEYS", "")
		if _, err := Load(); err == nil || err.Error() != "at least one FM_ADMIN_API_KEYS value is required in curated mode" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("curated requires robots enforcement", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "curated")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/artifacts")
		t.Setenv("FM_ADMIN_API_KEYS", "curator-key")
		t.Setenv("FM_RESPECT_ROBOTS", "false")
		if _, err := Load(); err == nil || err.Error() != "FM_RESPECT_ROBOTS must be true in curated mode" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("curated paths must not overlap", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "curated")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/corpus")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/corpus/artifacts")
		t.Setenv("FM_ADMIN_API_KEYS", "curator-key")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_INDEX_PATH and FM_LOCAL_ARTIFACT_PATH must not overlap" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("curated is selectable with safeguards", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "curated")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/artifacts")
		t.Setenv("FM_ADMIN_API_KEYS", "curator-key")
		configuration, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if configuration.LocalCorpusMode != "curated" {
			t.Fatalf("LocalCorpusMode = %q", configuration.LocalCorpusMode)
		}
	})
	t.Run("archive requires paths", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "archive")
		t.Setenv("FM_ADMIN_API_KEYS", "archive-admin")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_INDEX_PATH and FM_LOCAL_ARTIFACT_PATH are required in archive mode" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("archive requires an admin API key", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "archive")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/artifacts")
		if _, err := Load(); err == nil || err.Error() != "at least one FM_ADMIN_API_KEYS value is required in archive mode" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("archive requires robots enforcement", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "archive")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/artifacts")
		t.Setenv("FM_ADMIN_API_KEYS", "archive-admin")
		t.Setenv("FM_RESPECT_ROBOTS", "false")
		if _, err := Load(); err == nil || err.Error() != "FM_RESPECT_ROBOTS must be true in archive mode" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("archive requires aggregate version capacity", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "archive")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/artifacts")
		t.Setenv("FM_ADMIN_API_KEYS", "archive-admin")
		t.Setenv("FM_LOCAL_ARCHIVE_MAX_VERSIONS", "0")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_ARCHIVE_MAX_VERSIONS must be > 0 in archive mode" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("archive requires bounded per-URL history", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "archive")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/artifacts")
		t.Setenv("FM_ADMIN_API_KEYS", "archive-admin")
		t.Setenv("FM_LOCAL_ARCHIVE_MAX_VERSIONS", "10")
		t.Setenv("FM_LOCAL_ARCHIVE_MAX_VERSIONS_PER_URL", "11")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_ARCHIVE_MAX_VERSIONS_PER_URL must be between 2 and FM_LOCAL_ARCHIVE_MAX_VERSIONS" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("archive is selectable with safeguards", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "archive")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/artifacts")
		t.Setenv("FM_ADMIN_API_KEYS", "archive-admin")
		configuration, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if configuration.LocalCorpusMode != "archive" {
			t.Fatalf("LocalCorpusMode = %q", configuration.LocalCorpusMode)
		}
	})
	t.Run("personal requires positive max age", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "personal")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/artifacts")
		t.Setenv("FM_LOCAL_CORPUS_MAX_AGE", "0s")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_CORPUS_MAX_AGE must be > 0 in personal mode" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("personal requires positive expiry sweep interval", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "personal")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/artifacts")
		t.Setenv("FM_LOCAL_EXPIRY_SWEEP_INTERVAL", "0s")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_EXPIRY_SWEEP_INTERVAL must be > 0 in personal mode" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("artifact path must be absolute", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "personal")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/index")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "relative/artifacts")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_ARTIFACT_PATH must be absolute when set" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("persistent paths must not overlap", func(t *testing.T) {
		t.Setenv("FM_LOCAL_CORPUS_MODE", "personal")
		t.Setenv("FM_LOCAL_INDEX_PATH", "/var/lib/fetchmark/corpus")
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", "/var/lib/fetchmark/corpus/artifacts")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_INDEX_PATH and FM_LOCAL_ARTIFACT_PATH must not overlap" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("persistent path aliases must not overlap", func(t *testing.T) {
		parent := t.TempDir()
		indexPath := filepath.Join(parent, "index")
		if err := os.MkdirAll(indexPath, 0o700); err != nil {
			t.Fatal(err)
		}
		alias := filepath.Join(parent, "artifact-alias")
		if err := os.Symlink(indexPath, alias); err != nil {
			t.Fatal(err)
		}
		t.Setenv("FM_LOCAL_CORPUS_MODE", "personal")
		t.Setenv("FM_LOCAL_INDEX_PATH", indexPath)
		t.Setenv("FM_LOCAL_ARTIFACT_PATH", alias)
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_INDEX_PATH and FM_LOCAL_ARTIFACT_PATH must not overlap" {
			t.Fatalf("Load error = %v", err)
		}
	})
	t.Run("artifact value budget", func(t *testing.T) {
		t.Setenv("FM_LOCAL_ARTIFACT_MAX_BYTES", "1024")
		t.Setenv("FM_LOCAL_ARTIFACT_MAX_VALUE_BYTES", "1025")
		if _, err := Load(); err == nil || err.Error() != "FM_LOCAL_ARTIFACT_MAX_VALUE_BYTES must be <= FM_LOCAL_ARTIFACT_MAX_BYTES" {
			t.Fatalf("Load error = %v", err)
		}
	})
}

func TestLoad_CleansDiscoveryPackLists(t *testing.T) {
	t.Setenv("FM_DISCOVERY_ENABLED_PACKS", " research, knowledge ,, ")
	t.Setenv("FM_DISCOVERY_ENABLED_SOURCES", " searxng, crossref ,, ")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.DiscoveryEnabledPacks) != 2 || c.DiscoveryEnabledPacks[0] != "research" || c.DiscoveryEnabledPacks[1] != "knowledge" {
		t.Fatalf("packs = %v", c.DiscoveryEnabledPacks)
	}
	if len(c.DiscoveryEnabledSources) != 2 || c.DiscoveryEnabledSources[0] != "searxng" || c.DiscoveryEnabledSources[1] != "crossref" {
		t.Fatalf("sources = %v", c.DiscoveryEnabledSources)
	}
}

func TestLoad_AllowsSearxNGDisabledWithoutSearxConfiguration(t *testing.T) {
	t.Setenv("FM_DISCOVERY_ENABLED_SOURCES", "wikipedia,crossref")
	t.Setenv("FM_DISCOVERY_PRIMARY_SOURCE", "wikipedia")
	t.Setenv("FM_SEARXNG_URL", "")
	t.Setenv("FM_SEARXNG_URLS", "")
	t.Setenv("FM_SEARXNG_COOLDOWN", "0s")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(c.SearxngURLs) != 0 {
		t.Fatalf("SearxngURLs = %v, want empty when SearXNG is disabled", c.SearxngURLs)
	}
	if c.DiscoveryPrimarySource != "wikipedia" {
		t.Fatalf("DiscoveryPrimarySource = %q", c.DiscoveryPrimarySource)
	}
}

func TestLoad_RejectsDisabledDiscoveryPrimary(t *testing.T) {
	t.Setenv("FM_DISCOVERY_ENABLED_SOURCES", "wikipedia")
	t.Setenv("FM_DISCOVERY_PRIMARY_SOURCE", "crossref")
	if _, err := Load(); err == nil || err.Error() != "FM_DISCOVERY_PRIMARY_SOURCE must name an enabled discovery source" {
		t.Fatalf("Load error = %v", err)
	}
}

func TestLoad_AdvancedSearchConcurrencyBounds(t *testing.T) {
	for _, value := range []string{"0", "17"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("FM_ADVANCED_SEARCH_CONCURRENCY", value)
			_, err := Load()
			if err == nil || err.Error() != "FM_ADVANCED_SEARCH_CONCURRENCY must be between 1 and 16" {
				t.Fatalf("Load error = %v", err)
			}
		})
	}
}

func TestLoad_InvalidDiscoveryCacheLimits(t *testing.T) {
	tests := []struct {
		env  string
		want string
	}{
		{env: "FM_DISCOVERY_CACHE_TTL", want: "discovery cache durations must be > 0"},
		{env: "FM_DISCOVERY_CACHE_STALE_TTL", want: "discovery cache durations must be > 0"},
		{env: "FM_DISCOVERY_REFRESH_TIMEOUT", want: "discovery cache durations must be > 0"},
		{env: "FM_DISCOVERY_CACHE_ENTRIES", want: "discovery cache limits must be > 0"},
		{env: "FM_DISCOVERY_CACHE_BYTES", want: "discovery cache limits must be > 0"},
		{env: "FM_DISCOVERY_CACHE_MAX_ENTRY_BYTES", want: "discovery cache limits must be > 0"},
		{env: "FM_DISCOVERY_MAX_INFLIGHT", want: "discovery cache limits must be > 0"},
		{env: "FM_DISCOVERY_PROVIDER_MAX_BODY", want: "FM_DISCOVERY_PROVIDER_MAX_BODY must be > 0"},
	}
	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			t.Setenv(tt.env, "0")
			_, err := Load()
			if err == nil || err.Error() != tt.want {
				t.Fatalf("Load error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestLoad_RejectsDiscoveryEntryLargerThanCache(t *testing.T) {
	t.Setenv("FM_DISCOVERY_CACHE_BYTES", "1024")
	t.Setenv("FM_DISCOVERY_CACHE_MAX_ENTRY_BYTES", "1025")
	_, err := Load()
	if err == nil || err.Error() != "FM_DISCOVERY_CACHE_MAX_ENTRY_BYTES must be <= FM_DISCOVERY_CACHE_BYTES" {
		t.Fatalf("Load error = %v", err)
	}
}

func TestLoad_InvalidSummarizeCaps(t *testing.T) {
	cases := []struct {
		name string
		env  string
		val  string
		want string
	}{
		{"max tokens", "FM_SUMMARIZE_MAX_TOKENS_CAP", "0", "FM_SUMMARIZE_MAX_TOKENS_CAP must be > 0"},
		{"timeout", "FM_SUMMARIZE_MAX_TIMEOUT", "0s", "FM_SUMMARIZE_MAX_TIMEOUT must be > 0"},
		{"instructions", "FM_SUMMARIZE_MAX_INSTRUCTIONS_LEN", "-1", "FM_SUMMARIZE_MAX_INSTRUCTIONS_LEN must be >= 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.env, tc.val)
			_, err := Load()
			if err == nil || err.Error() != tc.want {
				t.Fatalf("Load error = %v want %q", err, tc.want)
			}
		})
	}
}

func TestLoad_InvalidCompatibilityAnswerCaps(t *testing.T) {
	cases := []struct {
		env, value string
	}{
		{"FM_COMPAT_ANSWER_MAX_TOKENS", "4097"},
		{"FM_COMPAT_ANSWER_TIMEOUT", "121s"},
	}
	for _, tc := range cases {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv(tc.env, tc.value)
			_, err := Load()
			if err == nil || err.Error() != "compatibility answers are capped at 4096 tokens and 2 minutes" {
				t.Fatalf("Load error = %v", err)
			}
		})
	}
}

func TestLoad_DisabledRendererDoesNotConsumeSourceBudget(t *testing.T) {
	t.Setenv("FM_RENDERER_URL", "")
	t.Setenv("FM_RENDERER_MAX_BODY", "1073741824")
	if _, err := Load(); err != nil {
		t.Fatalf("disabled renderer should not affect source budget validation: %v", err)
	}
}

func TestLoad_SourceBudgetIncludesLargerPlainBodyLimit(t *testing.T) {
	t.Setenv("FM_ARTIFACT_CONCURRENCY", "2")
	t.Setenv("FM_MAX_BODY_BYTES", "32")
	t.Setenv("FM_MAX_DECOMPRESSED_BYTES", "16")
	t.Setenv("FM_MAX_REQUEST_SOURCE_BYTES", "63")
	if _, err := Load(); err == nil {
		t.Fatal("expected source budget below 2 * max body bytes to fail")
	}
}

func TestLoad_RendererRequiresEgressProxy(t *testing.T) {
	t.Setenv("FM_RENDERER_URL", "http://browserless:3000/content")
	t.Setenv("FM_RENDERER_EGRESS_PROXY_URL", "")
	if _, err := Load(); err == nil {
		t.Fatal("expected renderer without enforced egress proxy to fail")
	}
}

func TestLoad_InvalidMemoryLimits(t *testing.T) {
	cases := []struct {
		env, value string
	}{
		{"FM_ARTIFACT_CONCURRENCY", "0"},
		{"FM_MAX_REQUEST_SOURCE_BYTES", "0"},
		{"FM_MAX_REQUEST_OUTPUT_BYTES", "0"},
		{"FM_MEMORY_CACHE_ENTRIES", "0"},
		{"FM_MEMORY_CACHE_BYTES", "0"},
		{"FM_CACHE_MAX_VALUE_BYTES", "0"},
	}
	for _, tc := range cases {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv(tc.env, tc.value)
			if _, err := Load(); err == nil {
				t.Fatalf("expected %s=%s to fail validation", tc.env, tc.value)
			}
		})
	}
}

func TestLoad_InvalidResults(t *testing.T) {
	t.Setenv("FM_MAX_RESULTS", "100")
	t.Setenv("FM_RESULTS_CAP", "50")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when MaxResults > ResultsCap")
	}
}

func TestDashboardEnabled(t *testing.T) {
	t.Setenv("FM_DASHBOARD_USER", "admin")
	t.Setenv("FM_DASHBOARD_PASSWORD", "secret")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !c.DashboardEnabled() {
		t.Error("dashboard should be enabled when creds set")
	}
}

func TestLoad_SearxngCooldownDefault(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.SearxngCooldown.String() != "30s" {
		t.Fatalf("SearxngCooldown default = %v", c.SearxngCooldown)
	}
}

func TestLoad_SearxngCooldownRejectsZero(t *testing.T) {
	t.Setenv("FM_SEARXNG_COOLDOWN", "0s")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when FM_SEARXNG_COOLDOWN=0")
	}
}
