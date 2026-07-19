// Package evaluationmanifest builds a bounded, non-secret snapshot of the
// resolved controls that materially affect Fetchmark evaluation results.
package evaluationmanifest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/discovery"
)

const (
	HeaderName       = "X-Fetchmark-Configuration-SHA256"
	MaxManifestBytes = 1 << 20
)

type Manifest struct {
	SchemaVersion    int               `json:"schema_version"`
	Discovery        DiscoveryManifest `json:"discovery"`
	Retrieval        RetrievalManifest `json:"retrieval"`
	ContentCache     CacheManifest     `json:"content_cache"`
	Persistence      Persistence       `json:"persistence"`
	Renderer         RendererManifest  `json:"renderer"`
	NetworkPolicy    NetworkPolicy     `json:"network_policy"`
	StateLimitations []string          `json:"state_limitations,omitempty"`
}

type DiscoveryManifest struct {
	PrimarySource         string                 `json:"primary_source"`
	EnabledSources        []string               `json:"enabled_sources"`
	EnabledPacks          []string               `json:"enabled_packs"`
	Registry              discovery.RegistrySpec `json:"registry"`
	AdvancedConcurrency   int                    `json:"advanced_concurrency"`
	ProviderMaxBodyBytes  int64                  `json:"provider_max_body_bytes"`
	SearxngCooldownMS     int64                  `json:"searxng_cooldown_ms"`
	CacheFreshTTLMS       int64                  `json:"cache_fresh_ttl_ms"`
	CacheStaleTTLMS       int64                  `json:"cache_stale_ttl_ms"`
	CacheRefreshTimeoutMS int64                  `json:"cache_refresh_timeout_ms"`
	CacheEntries          int                    `json:"cache_entries"`
	CacheBytes            int64                  `json:"cache_bytes"`
	CacheMaxEntryBytes    int64                  `json:"cache_max_entry_bytes"`
	CacheMaxInflight      int                    `json:"cache_max_inflight"`
	PrivateBindingSHA256  string                 `json:"private_binding_sha256,omitempty"`
}

type RetrievalManifest struct {
	FetchConcurrency      int      `json:"fetch_concurrency"`
	PerHostConcurrency    int      `json:"per_host_concurrency"`
	FetchTimeoutMS        int64    `json:"fetch_timeout_ms"`
	HeaderTimeoutMS       int64    `json:"header_timeout_ms"`
	FetchRetries          int      `json:"fetch_retries"`
	MaxBodyBytes          int64    `json:"max_body_bytes"`
	MaxDecompressedBytes  int64    `json:"max_decompressed_bytes"`
	MaxRedirects          int      `json:"max_redirects"`
	AllowedMIME           []string `json:"allowed_mime"`
	RespectRobots         bool     `json:"respect_robots"`
	ArtifactConcurrency   int      `json:"artifact_concurrency"`
	MaxRequestSourceBytes int64    `json:"max_request_source_bytes"`
	MaxRequestOutputBytes int64    `json:"max_request_output_bytes"`
	DefaultMaxResults     int      `json:"default_max_results"`
	ResultsCap            int      `json:"results_cap"`
}

type CacheManifest struct {
	TTLMS         int64 `json:"ttl_ms"`
	Entries       int   `json:"entries"`
	Bytes         int64 `json:"bytes"`
	MaxValueBytes int64 `json:"max_value_bytes"`
}

type Persistence struct {
	Mode                     string `json:"mode"`
	MaxAgeMS                 int64  `json:"max_age_ms"`
	ExpirySweepIntervalMS    int64  `json:"expiry_sweep_interval_ms"`
	IndexMaxDocumentBytes    int    `json:"index_max_document_bytes"`
	IndexMaxBytes            int64  `json:"index_max_bytes"`
	IndexMaxDocuments        int    `json:"index_max_documents"`
	ArtifactMaxBytes         int64  `json:"artifact_max_bytes"`
	ArtifactMaxValueBytes    int64  `json:"artifact_max_value_bytes"`
	ArtifactMaxEntries       int    `json:"artifact_max_entries"`
	ArchiveMaxVersions       int    `json:"archive_max_versions"`
	ArchiveMaxVersionsPerURL int    `json:"archive_max_versions_per_url"`
}

type RendererManifest struct {
	Enabled                       bool   `json:"enabled"`
	Auto                          bool   `json:"auto"`
	TimeoutMS                     int64  `json:"timeout_ms"`
	MaxBodyBytes                  int64  `json:"max_body_bytes"`
	TokenConfigured               bool   `json:"token_configured"`
	EgressProxyConfigured         bool   `json:"egress_proxy_configured"`
	EndpointSHA256                string `json:"endpoint_sha256,omitempty"`
	EndpointCredentialsConfigured bool   `json:"endpoint_credentials_configured"`
	EndpointQueryParameterCount   int    `json:"endpoint_query_parameter_count"`
	EgressProxySHA256             string `json:"egress_proxy_sha256,omitempty"`
	EgressCredentialsConfigured   bool   `json:"egress_credentials_configured"`
	EgressQueryParameterCount     int    `json:"egress_query_parameter_count"`
}

type NetworkPolicy struct {
	ProxyConfigured                  bool    `json:"proxy_configured"`
	ProxySHA256                      string  `json:"proxy_sha256,omitempty"`
	ProxyCredentialsConfigured       bool    `json:"proxy_credentials_configured"`
	ProxyQueryParameterCount         int     `json:"proxy_query_parameter_count"`
	RequestRatePerSecond             float64 `json:"request_rate_per_second"`
	RequestRateBurst                 int     `json:"request_rate_burst"`
	SearxngInstanceCount             int     `json:"searxng_instance_count"`
	SearxngPoolSHA256                string  `json:"searxng_pool_sha256,omitempty"`
	SearxngCredentialedInstanceCount int     `json:"searxng_credentialed_instance_count"`
	SearxngQueryParameterCount       int     `json:"searxng_query_parameter_count"`
	HostPolicySHA256                 string  `json:"host_policy_sha256,omitempty"`
	UserAgentConfigured              bool    `json:"user_agent_configured"`
	UserAgentPoolCount               int     `json:"user_agent_pool_count"`
	ProviderContactConfigured        bool    `json:"provider_contact_configured"`
	CrossrefMailtoConfigured         bool    `json:"crossref_mailto_configured"`
	PubMedEmailConfigured            bool    `json:"pubmed_email_configured"`
	YaCyEndpointSHA256               string  `json:"yacy_endpoint_sha256,omitempty"`
	YaCyResource                     string  `json:"yacy_resource,omitempty"`
	YaCyAllowInsecureHTTP            bool    `json:"yacy_allow_insecure_http,omitempty"`
}

type endpointPolicy struct {
	SHA256              string
	CredentialCount     int
	QueryParameterCount int
}

// Build resolves the enabled subset of a validated discovery registry and
// emits deterministic JSON plus its lowercase SHA-256. Raw endpoints,
// credentials, contacts, peer identities, and filesystem paths are excluded.
func Build(cfg config.Config, spec discovery.RegistrySpec) (Manifest, []byte, string, error) {
	resolved, enabledSources, enabledPacks, sourceKinds, privateBindingSHA256, err := resolveRegistry(cfg, spec)
	if err != nil {
		return Manifest{}, nil, "", err
	}
	primarySource := cfg.DiscoveryPrimarySource
	for index, id := range unique(cfg.DiscoveryEnabledSources) {
		if id == cfg.DiscoveryPrimarySource {
			primarySource = enabledSources[index]
			break
		}
	}
	rendererEndpoint := redactEndpointPolicy("renderer-endpoint", cfg.RendererURL)
	rendererEgress := redactEndpointPolicy("renderer-egress-proxy", cfg.RendererEgressProxyURL)
	fetchProxy := redactEndpointPolicy("fetch-proxy", cfg.ProxyURL)
	searxngPool := redactEndpointPolicy("searxng", cfg.SearxngURLs...)
	yacyEndpoint := redactEndpointPolicy("yacy", cfg.YaCyURL)
	yacyResource := ""
	yacyAllowInsecureHTTP := false
	if strings.TrimSpace(cfg.YaCyURL) != "" {
		yacyResource = strings.TrimSpace(cfg.YaCyResource)
		yacyAllowInsecureHTTP = cfg.YaCyAllowInsecureHTTP
	}
	manifest := Manifest{
		SchemaVersion: 1,
		Discovery: DiscoveryManifest{
			PrimarySource: primarySource, EnabledSources: enabledSources,
			EnabledPacks: enabledPacks, Registry: resolved,
			AdvancedConcurrency: cfg.AdvancedSearchConcurrency, ProviderMaxBodyBytes: cfg.DiscoveryProviderMaxBody,
			SearxngCooldownMS: cfg.SearxngCooldown.Milliseconds(), CacheFreshTTLMS: cfg.DiscoveryCacheTTL.Milliseconds(),
			CacheStaleTTLMS: cfg.DiscoveryCacheStaleTTL.Milliseconds(), CacheRefreshTimeoutMS: cfg.DiscoveryRefreshTimeout.Milliseconds(),
			CacheEntries: cfg.DiscoveryCacheEntries, CacheBytes: cfg.DiscoveryCacheBytes,
			CacheMaxEntryBytes: cfg.DiscoveryCacheMaxEntryBytes, CacheMaxInflight: cfg.DiscoveryMaxInflight,
			PrivateBindingSHA256: privateBindingSHA256,
		},
		Retrieval: RetrievalManifest{
			FetchConcurrency: cfg.FetchConcurrency, PerHostConcurrency: cfg.PerHostConcurrency,
			FetchTimeoutMS: cfg.FetchTimeout.Milliseconds(), HeaderTimeoutMS: cfg.HeaderTimeout.Milliseconds(),
			FetchRetries: cfg.FetchRetries, MaxBodyBytes: cfg.MaxBodyBytes, MaxDecompressedBytes: cfg.MaxDecompressedBytes,
			MaxRedirects: cfg.MaxRedirects, AllowedMIME: append([]string(nil), cfg.AllowedMIME...), RespectRobots: cfg.RespectRobots,
			ArtifactConcurrency: cfg.ArtifactConcurrency, MaxRequestSourceBytes: cfg.MaxRequestSourceBytes,
			MaxRequestOutputBytes: cfg.MaxRequestOutputBytes, DefaultMaxResults: cfg.MaxResults, ResultsCap: cfg.ResultsCap,
		},
		ContentCache: CacheManifest{
			TTLMS: cfg.CacheTTL.Milliseconds(), Entries: cfg.MemoryCacheEntries,
			Bytes: cfg.MemoryCacheBytes, MaxValueBytes: cfg.CacheMaxValueBytes,
		},
		Persistence: Persistence{
			Mode: cfg.LocalCorpusMode, MaxAgeMS: cfg.LocalCorpusMaxAge.Milliseconds(),
			ExpirySweepIntervalMS: cfg.LocalExpirySweepInterval.Milliseconds(),
			IndexMaxDocumentBytes: cfg.LocalIndexMaxDocumentBytes, IndexMaxBytes: cfg.LocalIndexMaxBytes,
			IndexMaxDocuments: cfg.LocalIndexMaxDocuments, ArtifactMaxBytes: cfg.LocalArtifactMaxBytes,
			ArtifactMaxValueBytes: cfg.LocalArtifactMaxValueBytes, ArtifactMaxEntries: cfg.LocalArtifactMaxEntries,
			ArchiveMaxVersions: cfg.LocalArchiveMaxVersions, ArchiveMaxVersionsPerURL: cfg.LocalArchiveMaxVersionsPerURL,
		},
		Renderer: RendererManifest{
			Enabled: strings.TrimSpace(cfg.RendererURL) != "", Auto: cfg.RendererAuto,
			TimeoutMS: cfg.RendererTimeout.Milliseconds(), MaxBodyBytes: cfg.RendererMaxBody,
			TokenConfigured:       strings.TrimSpace(cfg.RendererToken) != "",
			EgressProxyConfigured: strings.TrimSpace(cfg.RendererEgressProxyURL) != "",
			EndpointSHA256:        rendererEndpoint.SHA256, EndpointCredentialsConfigured: rendererEndpoint.CredentialCount > 0,
			EndpointQueryParameterCount: rendererEndpoint.QueryParameterCount,
			EgressProxySHA256:           rendererEgress.SHA256, EgressCredentialsConfigured: rendererEgress.CredentialCount > 0,
			EgressQueryParameterCount: rendererEgress.QueryParameterCount,
		},
		NetworkPolicy: NetworkPolicy{
			ProxyConfigured: strings.TrimSpace(cfg.ProxyURL) != "", RequestRatePerSecond: cfg.RateLimitPerSec,
			RequestRateBurst: cfg.RateLimitBurst, SearxngInstanceCount: len(cfg.SearxngURLs),
			ProxySHA256: fetchProxy.SHA256, ProxyCredentialsConfigured: fetchProxy.CredentialCount > 0,
			ProxyQueryParameterCount: fetchProxy.QueryParameterCount,
			SearxngPoolSHA256:        searxngPool.SHA256, SearxngCredentialedInstanceCount: searxngPool.CredentialCount,
			SearxngQueryParameterCount: searxngPool.QueryParameterCount,
			HostPolicySHA256:           hashPolicy("hosts", cfg.HostAllowlist, cfg.HostDenylist),
			UserAgentConfigured:        strings.TrimSpace(cfg.UserAgent) != "", UserAgentPoolCount: len(cfg.UserAgentsPool),
			ProviderContactConfigured: strings.TrimSpace(cfg.Contact) != "", CrossrefMailtoConfigured: strings.TrimSpace(cfg.CrossrefMailto) != "",
			PubMedEmailConfigured: strings.TrimSpace(cfg.PubMedEmail) != "",
			YaCyEndpointSHA256:    yacyEndpoint.SHA256, YaCyResource: yacyResource,
			YaCyAllowInsecureHTTP: yacyAllowInsecureHTTP,
		},
		StateLimitations: stateLimitations(cfg, sourceKinds),
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return Manifest{}, nil, "", fmt.Errorf("evaluation manifest: encode: %w", err)
	}
	if len(raw) > MaxManifestBytes {
		return Manifest{}, nil, "", fmt.Errorf("evaluation manifest exceeds %d bytes", MaxManifestBytes)
	}
	digest := sha256.Sum256(raw)
	return manifest, raw, hex.EncodeToString(digest[:]), nil
}

func resolveRegistry(cfg config.Config, spec discovery.RegistrySpec) (discovery.RegistrySpec, []string, []string, map[string]string, string, error) {
	sourceByID := make(map[string]discovery.SourceSpec, len(spec.Sources))
	for _, source := range spec.Sources {
		sourceByID[source.ID] = source
	}
	enabledSources := unique(cfg.DiscoveryEnabledSources)
	enabledSet := make(map[string]struct{}, len(enabledSources))
	resolvedSources := make([]discovery.SourceSpec, 0, len(enabledSources))
	sourceKinds := make(map[string]string, len(enabledSources))
	for _, id := range enabledSources {
		source, exists := sourceByID[id]
		if !exists {
			return discovery.RegistrySpec{}, nil, nil, nil, "", fmt.Errorf("evaluation manifest: enabled source %q is not defined", id)
		}
		enabledSet[id] = struct{}{}
		sourceKinds[id] = source.Kind
	}
	if _, exists := enabledSet[cfg.DiscoveryPrimarySource]; !exists {
		return discovery.RegistrySpec{}, nil, nil, nil, "", errors.New("evaluation manifest: primary source is not enabled")
	}

	aliases := make(map[string]string, len(enabledSources))
	kindCounts := make(map[string]int, 2)
	privateBindings := make([]string, 0)
	manifestSources := make([]string, 0, len(enabledSources))
	for _, id := range enabledSources {
		source := sourceByID[id]
		alias := id
		if source.Kind == "federation" || source.Kind == "openpack" {
			kindCounts[source.Kind]++
			alias = fmt.Sprintf("%s-%d", source.Kind, kindCounts[source.Kind])
			privateBindings = append(privateBindings, "source", source.Kind, id)
		}
		aliases[id] = alias
		source.ID = alias
		resolvedSources = append(resolvedSources, source)
		manifestSources = append(manifestSources, alias)
	}

	packByID := make(map[string]discovery.Pack, len(spec.Packs))
	for _, pack := range spec.Packs {
		packByID[pack.ID] = pack
	}
	enabledPacks := unique(cfg.DiscoveryEnabledPacks)
	resolvedPacks := make([]discovery.Pack, 0, len(enabledPacks))
	manifestPacks := make([]string, 0, len(enabledPacks))
	privatePacks := make(map[string]bool, len(enabledPacks))
	reservedPackIDs := make(map[string]struct{}, len(enabledPacks))
	for _, id := range enabledPacks {
		pack, exists := packByID[id]
		if !exists {
			return discovery.RegistrySpec{}, nil, nil, nil, "", fmt.Errorf("evaluation manifest: enabled pack %q is not defined", id)
		}
		for _, lane := range pack.Sources {
			kind := sourceKinds[lane.ID]
			if _, enabled := enabledSet[lane.ID]; enabled && (kind == "federation" || kind == "openpack") {
				privatePacks[id] = true
				break
			}
		}
		if !privatePacks[id] {
			reservedPackIDs[id] = struct{}{}
		}
	}
	usedPackIDs := make(map[string]struct{}, len(enabledPacks))
	reservedLaneIDs := make(map[string]struct{})
	for _, id := range enabledPacks {
		for _, lane := range packByID[id].Sources {
			kind := sourceKinds[lane.ID]
			if _, enabled := enabledSet[lane.ID]; enabled && kind != "federation" && kind != "openpack" {
				reservedLaneIDs[lane.LaneID] = struct{}{}
			}
		}
	}
	usedLaneIDs := make(map[string]struct{})
	privateLaneCounts := make(map[string]int)
	privatePackCount := 0
	for _, id := range enabledPacks {
		pack, exists := packByID[id]
		if !exists {
			return discovery.RegistrySpec{}, nil, nil, nil, "", fmt.Errorf("evaluation manifest: enabled pack %q is not defined", id)
		}
		packID := pack.ID
		if privatePacks[id] {
			for {
				privatePackCount++
				candidate := fmt.Sprintf("private-pack-%d", privatePackCount)
				if _, reserved := reservedPackIDs[candidate]; reserved {
					continue
				}
				if _, used := usedPackIDs[candidate]; used {
					continue
				}
				packID = candidate
				break
			}
			privateBindings = append(privateBindings, "pack", pack.ID)
		}
		usedPackIDs[packID] = struct{}{}
		manifestPacks = append(manifestPacks, packID)
		resolved := discovery.Pack{ID: packID, Always: pack.Always, Intents: append([]discovery.Intent(nil), pack.Intents...)}
		for _, lane := range pack.Sources {
			if _, enabled := enabledSet[lane.ID]; !enabled {
				continue
			}
			cloned := lane
			cloned.ID = aliases[lane.ID]
			if sourceKinds[lane.ID] == "federation" || sourceKinds[lane.ID] == "openpack" {
				if lane.LaneID != "" {
					privateBindings = append(privateBindings, "lane", pack.ID, lane.LaneID)
					for {
						privateLaneCounts[cloned.ID]++
						candidate := fmt.Sprintf("%s-lane-%d", cloned.ID, privateLaneCounts[cloned.ID])
						if _, reserved := reservedLaneIDs[candidate]; reserved {
							continue
						}
						if _, used := usedLaneIDs[candidate]; used {
							continue
						}
						cloned.LaneID = candidate
						break
					}
				}
			}
			usedLaneIDs[cloned.LaneID] = struct{}{}
			cloned.Variants = append([]string(nil), lane.Variants...)
			cloned.Engines = append([]string(nil), lane.Engines...)
			cloned.Categories = append([]string(nil), lane.Categories...)
			resolved.Sources = append(resolved.Sources, cloned)
		}
		resolvedPacks = append(resolvedPacks, resolved)
	}
	return discovery.RegistrySpec{Version: spec.Version, Sources: resolvedSources, Packs: resolvedPacks}, manifestSources, manifestPacks, sourceKinds, hashPolicy("private-bindings", privateBindings), nil
}

func unique(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func hashPolicy(label string, groups ...[]string) string {
	nonempty := false
	for _, group := range groups {
		for _, value := range group {
			if strings.TrimSpace(value) != "" {
				nonempty = true
				break
			}
		}
		if nonempty {
			break
		}
	}
	if !nonempty {
		return ""
	}
	raw, _ := json.Marshal(struct {
		Label  string     `json:"label"`
		Groups [][]string `json:"groups"`
	}{Label: label, Groups: groups})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// redactEndpointPolicy fingerprints routing identity without retaining a
// verifier for credentials. URL userinfo and query values are omitted; their
// presence is represented only by counts/booleans in the manifest.
func redactEndpointPolicy(label string, values ...string) endpointPolicy {
	redacted := make([]string, 0, len(values))
	policy := endpointPolicy{}
	for _, raw := range values {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			redacted = append(redacted, "configured-unparseable")
			continue
		}
		if parsed.User != nil {
			policy.CredentialCount++
			parsed.User = nil
		}
		policy.QueryParameterCount += len(parsed.Query())
		parsed.RawQuery = ""
		parsed.ForceQuery = false
		parsed.Fragment = ""
		parsed.RawFragment = ""
		redacted = append(redacted, parsed.String())
	}
	policy.SHA256 = hashPolicy(label, redacted)
	return policy
}

func stateLimitations(cfg config.Config, sourceKinds map[string]string) []string {
	limitations := []string{
		"content_cache_contents_not_snapshotted",
		"live_upstream_state_not_snapshotted",
		"provider_health_state_not_snapshotted",
	}
	if cfg.LocalCorpusMode != "" && cfg.LocalCorpusMode != "disabled" {
		limitations = append(limitations, "local_corpus_contents_not_snapshotted")
	}
	for _, kind := range sourceKinds {
		switch kind {
		case "searxng":
			limitations = append(limitations, "searxng_engine_state_not_snapshotted")
		case "openpack":
			limitations = append(limitations, "open_pack_binding_not_snapshotted")
		case "federation":
			limitations = append(limitations, "federation_peer_index_state_not_snapshotted")
		case "yacy":
			limitations = append(limitations, "yacy_index_state_not_snapshotted")
		}
	}
	sort.Strings(limitations)
	return unique(limitations)
}
