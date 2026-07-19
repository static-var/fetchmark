package evaluationmanifest

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/discovery"
)

func TestBuildProducesStableNonSecretResolvedConfiguration(t *testing.T) {
	spec, err := discovery.DefaultSpec()
	if err != nil {
		t.Fatalf("DefaultSpec: %v", err)
	}
	cfg := config.Config{
		APIKeys: []string{"secret-api-key"}, AdminAPIKeys: []string{"secret-admin"},
		SearxngURL: "http://private-searx:8080", SearxngURLs: []string{"http://operator:guessable@private-searx:8080/base?token=guessable"},
		RedisURL: "redis://secret@private-redis:6379/0", ProxyURL: "http://operator:guessable@secret-proxy:8080?token=guessable",
		DiscoveryEnabledPacks:   []string{"general-open", "developer"},
		DiscoveryEnabledSources: []string{"mwmbl", "wiby"}, DiscoveryPrimarySource: "mwmbl",
		DiscoveryCacheTTL: 30 * time.Second, DiscoveryCacheStaleTTL: 2 * time.Minute,
		DiscoveryRefreshTimeout: 15 * time.Second, DiscoveryCacheEntries: 256,
		DiscoveryCacheBytes: 16 << 20, DiscoveryCacheMaxEntryBytes: 1 << 20,
		DiscoveryMaxInflight: 16, AdvancedSearchConcurrency: 4,
		DiscoveryProviderMaxBody: 2 << 20, SearxngCooldown: 30 * time.Second,
		FetchConcurrency: 10, PerHostConcurrency: 2, FetchTimeout: 8 * time.Second,
		HeaderTimeout: 5 * time.Second, FetchRetries: 2, MaxBodyBytes: 5 << 20,
		MaxDecompressedBytes: 20 << 20, MaxRedirects: 5,
		AllowedMIME: []string{"text/html", "application/xhtml+xml"}, RespectRobots: true,
		UserAgent: "secret-contact@example.com", UserAgentsPool: []string{"private-agent"},
		Contact:       "another-secret-contact@example.com",
		HostAllowlist: []string{"private.example"}, HostDenylist: []string{"denied.example"},
		CacheTTL: time.Hour, MemoryCacheEntries: 512, MemoryCacheBytes: 128 << 20,
		CacheMaxValueBytes: 8 << 20, LocalCorpusMode: "personal", LocalCorpusMaxAge: 720 * time.Hour,
		LocalExpirySweepInterval: 15 * time.Minute,
		LocalIndexPath:           "/private/index", LocalArtifactPath: "/private/artifacts",
		LocalIndexMaxDocumentBytes: 2 << 20, LocalIndexMaxBytes: 1 << 30, LocalIndexMaxDocuments: 100000,
		LocalArtifactMaxBytes: 1 << 30, LocalArtifactMaxValueBytes: 20 << 20, LocalArtifactMaxEntries: 100000,
		LocalArchiveMaxVersions: 100000, LocalArchiveMaxVersionsPerURL: 16,
		ArtifactConcurrency: 3, MaxRequestSourceBytes: 64 << 20, MaxRequestOutputBytes: 128 << 20,
		MaxResults: 10, ResultsCap: 50, RendererURL: "http://operator:guessable@private-renderer:3000/render?token=guessable",
		RendererEgressProxyURL: "http://operator:guessable@secret-renderer-proxy:8080?token=guessable", RateLimitPerSec: 5, RateLimitBurst: 20,
		RendererAuto: true, RendererTimeout: 20 * time.Second, RendererMaxBody: 10 << 20,
		RendererToken: "secret-renderer-token", FederationIdentityFile: "/private/identity.json",
		FederationTrustRegistryFile: "/private/trust.json", OpenPackRegistryFile: "/private/packs.json",
		CrossrefMailto: "secret-contact@example.com", PubMedEmail: "pubmed-secret@example.com",
		YaCyURL: "http://private-yacy:8090", YaCyResource: "global", YaCyAllowInsecureHTTP: true,
	}

	manifest, raw, digest, err := Build(cfg, spec)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if manifest.SchemaVersion != 1 || len(raw) == 0 || len(digest) != 64 {
		t.Fatalf("manifest=%+v bytes=%d digest=%q", manifest, len(raw), digest)
	}
	if manifest.Discovery.PrimarySource != "mwmbl" || len(manifest.Discovery.Registry.Sources) != 2 || len(manifest.Discovery.Registry.Packs) != 2 {
		t.Fatalf("resolved discovery = %+v", manifest.Discovery)
	}
	if manifest.Discovery.Registry.Sources[0].ID != "mwmbl" || manifest.Discovery.Registry.Sources[1].ID != "wiby" {
		t.Fatalf("source order changed: %+v", manifest.Discovery.Registry.Sources)
	}
	if !manifest.Retrieval.RespectRobots || manifest.Persistence.Mode != "personal" || !manifest.Renderer.Enabled || !manifest.Renderer.EgressProxyConfigured {
		t.Fatalf("runtime controls missing: %+v", manifest)
	}
	for _, forbidden := range []string{
		"secret", "private-searx", "private-yacy", "private-redis", "private-agent", "private.example",
		"denied.example", "/private/", "example.com", "http://", "redis://",
	} {
		if bytes.Contains(raw, []byte(forbidden)) {
			t.Fatalf("manifest leaked %q: %s", forbidden, raw)
		}
	}
	if manifest.NetworkPolicy.HostPolicySHA256 == "" || manifest.NetworkPolicy.SearxngPoolSHA256 == "" || manifest.NetworkPolicy.ProxySHA256 == "" {
		t.Fatalf("redacted policy identity missing: %+v", manifest.NetworkPolicy)
	}
	if !manifest.NetworkPolicy.ProxyCredentialsConfigured || manifest.NetworkPolicy.ProxyQueryParameterCount != 1 || manifest.NetworkPolicy.SearxngCredentialedInstanceCount != 1 || manifest.NetworkPolicy.SearxngQueryParameterCount != 1 {
		t.Fatalf("redacted endpoint shape missing: %+v", manifest.NetworkPolicy)
	}
	if !manifest.NetworkPolicy.ProviderContactConfigured || !manifest.NetworkPolicy.CrossrefMailtoConfigured || !manifest.NetworkPolicy.PubMedEmailConfigured || manifest.NetworkPolicy.UserAgentPoolCount != 1 {
		t.Fatalf("redacted provider identity shape missing: %+v", manifest.NetworkPolicy)
	}
	if manifest.NetworkPolicy.YaCyEndpointSHA256 == "" || manifest.NetworkPolicy.YaCyResource != "global" || !manifest.NetworkPolicy.YaCyAllowInsecureHTTP {
		t.Fatalf("redacted YaCy configuration missing: %+v", manifest.NetworkPolicy)
	}
	if manifest.Renderer.EndpointSHA256 == "" || manifest.Renderer.EgressProxySHA256 == "" || !manifest.Renderer.EndpointCredentialsConfigured || !manifest.Renderer.EgressCredentialsConfigured || !manifest.Renderer.TokenConfigured {
		t.Fatalf("redacted renderer identity missing: %+v", manifest.Renderer)
	}
	if !strings.Contains(strings.Join(manifest.StateLimitations, ","), "local_corpus_contents_not_snapshotted") {
		t.Fatalf("dynamic state limitation missing: %v", manifest.StateLimitations)
	}

	_, rawAgain, digestAgain, err := Build(cfg, spec)
	if err != nil || !bytes.Equal(raw, rawAgain) || digest != digestAgain {
		t.Fatalf("manifest is not deterministic: err=%v digest=%q/%q", err, digest, digestAgain)
	}

	changedSecrets := cfg
	changedSecrets.SearxngURLs = []string{"http://different:password@private-searx:8080/base?token=different"}
	changedSecrets.ProxyURL = "http://different:password@secret-proxy:8080?token=different"
	changedSecrets.RendererURL = "http://different:password@private-renderer:3000/render?token=different"
	changedSecrets.RendererEgressProxyURL = "http://different:password@secret-renderer-proxy:8080?token=different"
	changedSecrets.RendererToken = "different-renderer-token"
	changedSecrets.UserAgent = "different-contact@example.net"
	changedSecrets.UserAgentsPool = []string{"different-private-agent"}
	changedSecrets.Contact = "different-contact@example.net"
	changedSecrets.CrossrefMailto = "different-contact@example.net"
	changedSecrets.PubMedEmail = "different-pubmed-contact@example.net"
	_, changedRaw, changedDigest, err := Build(changedSecrets, spec)
	if err != nil || !bytes.Equal(raw, changedRaw) || digest != changedDigest {
		t.Fatalf("credential/contact changes became offline verifiers: err=%v digest=%q/%q", err, digest, changedDigest)
	}
}

func TestBuildRedactsPrivateSourceAndLaneBindings(t *testing.T) {
	spec, err := discovery.LoadSpec(strings.NewReader(`{
  "version":1,
  "sources":[
    {"id":"peer-secret-name","kind":"federation","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1},
    {"id":"private-pack-name","kind":"openpack","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1},
    {"id":"wikipedia","kind":"wikipedia","weight":1,"max_results":10,"timeout_ms":1000,"max_concurrency":1,"rate_per_second":1,"burst":1}
  ],
  "packs":[
    {"id":"peer-secret-pack","always":true,"sources":[
      {"id":"peer-secret-lane","source":"peer-secret-name","weight":1,"variants":["original"]},
      {"id":"private-pack-lane","source":"private-pack-name","weight":1,"variants":["original"]}
    ]},
    {"id":"private-pack-1","intents":["knowledge"],"sources":[
      {"id":"wikipedia-public","source":"wikipedia","weight":1,"variants":["original"]}
    ]},
    {"id":"peer-secret-pack-two","intents":["developer"],"sources":[
      {"id":"peer-secret-lane-two","source":"peer-secret-name","weight":1,"variants":["original"]}
    ]}
  ]
}`))
	if err != nil {
		t.Fatalf("LoadSpec: %v", err)
	}
	manifest, raw, _, err := Build(config.Config{
		DiscoveryEnabledPacks: []string{"peer-secret-pack", "private-pack-1", "peer-secret-pack-two"}, DiscoveryEnabledSources: []string{"peer-secret-name", "private-pack-name", "wikipedia"},
		DiscoveryPrimarySource: "peer-secret-name",
	}, spec)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for _, private := range []string{"peer-secret-pack", "peer-secret-pack-two", "peer-secret-name", "peer-secret-lane", "peer-secret-lane-two", "private-pack-name", "private-pack-lane"} {
		if bytes.Contains(raw, []byte(private)) {
			t.Fatalf("manifest leaked private binding %q: %s", private, raw)
		}
	}
	if manifest.Discovery.PrimarySource != "federation-1" || manifest.Discovery.PrivateBindingSHA256 == "" {
		t.Fatalf("redacted discovery = %+v", manifest.Discovery)
	}
	if len(manifest.Discovery.EnabledPacks) != 3 || manifest.Discovery.EnabledPacks[0] != "private-pack-2" || manifest.Discovery.EnabledPacks[1] != "private-pack-1" || manifest.Discovery.EnabledPacks[2] != "private-pack-3" || manifest.Discovery.Registry.Packs[0].ID != "private-pack-2" {
		t.Fatalf("redacted packs = %+v", manifest.Discovery)
	}
	if got := manifest.Discovery.Registry.Packs[0].Sources; got[0].ID != "federation-1" || got[0].LaneID != "federation-1-lane-1" || got[1].ID != "openpack-1" {
		t.Fatalf("redacted lanes = %+v", got)
	}
	laneIDs := make(map[string]struct{})
	for _, pack := range manifest.Discovery.Registry.Packs {
		for _, lane := range pack.Sources {
			if _, duplicate := laneIDs[lane.LaneID]; duplicate {
				t.Fatalf("duplicate redacted lane id %q", lane.LaneID)
			}
			laneIDs[lane.LaneID] = struct{}{}
		}
	}
	registryRaw, err := json.Marshal(manifest.Discovery.Registry)
	if err != nil {
		t.Fatalf("Marshal registry: %v", err)
	}
	if _, err := discovery.LoadSpec(bytes.NewReader(registryRaw)); err != nil {
		t.Fatalf("redacted registry is not reloadable: %v\n%s", err, registryRaw)
	}
}

func TestBuildRejectsUnknownEnabledConfiguration(t *testing.T) {
	spec, err := discovery.DefaultSpec()
	if err != nil {
		t.Fatalf("DefaultSpec: %v", err)
	}
	_, _, _, err = Build(config.Config{
		DiscoveryEnabledPacks:   []string{"missing-pack"},
		DiscoveryEnabledSources: []string{"mwmbl"},
		DiscoveryPrimarySource:  "mwmbl",
	}, spec)
	if err == nil || !strings.Contains(err.Error(), "missing-pack") {
		t.Fatalf("Build error = %v", err)
	}
}
