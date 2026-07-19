package config

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/staticvar/fetchmark/internal/core/localcorpus"
)

// Config captures the full runtime configuration for Fetchmark.
// All values are sourced from environment variables to keep the
// deployment story 12-factor friendly.
type Config struct {
	ListenAddr string `env:"FM_LISTEN_ADDR"    envDefault:":8080"`
	LogLevel   string `env:"FM_LOG_LEVEL"      envDefault:"info"`

	APIKeys      []string `env:"FM_API_KEYS"       envSeparator:","`
	AdminAPIKeys []string `env:"FM_ADMIN_API_KEYS" envSeparator:","`

	SearxngURL  string   `env:"FM_SEARXNG_URL"  envDefault:"http://searxng:8080"`
	SearxngURLs []string `env:"FM_SEARXNG_URLS" envSeparator:","`
	// SearxngCooldown is the base backoff for transport and response-quality
	// circuits. Repeated failures grow exponentially and Retry-After may extend
	// it. Non-positive values are rejected to preserve source rate limits.
	SearxngCooldown time.Duration `env:"FM_SEARXNG_COOLDOWN" envDefault:"30s"`
	RedisURL        string        `env:"FM_REDIS_URL"        envDefault:"redis://redis:6379/0"`

	// Discovery cache is a bounded process-local decorator around the provider
	// pool. StaleTTL is an additional stale-while-revalidate window after TTL.
	DiscoveryCacheTTL           time.Duration `env:"FM_DISCOVERY_CACHE_TTL"             envDefault:"30s"`
	DiscoveryCacheStaleTTL      time.Duration `env:"FM_DISCOVERY_CACHE_STALE_TTL"       envDefault:"2m"`
	DiscoveryRefreshTimeout     time.Duration `env:"FM_DISCOVERY_REFRESH_TIMEOUT"       envDefault:"30s"`
	DiscoveryCacheEntries       int           `env:"FM_DISCOVERY_CACHE_ENTRIES"         envDefault:"256"`
	DiscoveryCacheBytes         int64         `env:"FM_DISCOVERY_CACHE_BYTES"           envDefault:"16777216"`
	DiscoveryCacheMaxEntryBytes int64         `env:"FM_DISCOVERY_CACHE_MAX_ENTRY_BYTES" envDefault:"1048576"`
	DiscoveryMaxInflight        int           `env:"FM_DISCOVERY_MAX_INFLIGHT"          envDefault:"16"`
	AdvancedSearchConcurrency   int           `env:"FM_ADVANCED_SEARCH_CONCURRENCY"    envDefault:"4"`
	DiscoveryPackFile           string        `env:"FM_DISCOVERY_PACK_FILE"`
	DiscoveryEnabledPacks       []string      `env:"FM_DISCOVERY_ENABLED_PACKS"   envDefault:"general-open,developer,research,knowledge,fresh" envSeparator:","`
	DiscoveryEnabledSources     []string      `env:"FM_DISCOVERY_ENABLED_SOURCES" envDefault:"searxng,wikipedia,crossref" envSeparator:","`
	DiscoveryPrimarySource      string        `env:"FM_DISCOVERY_PRIMARY_SOURCE"  envDefault:"searxng"`
	DiscoveryProviderMaxBody    int64         `env:"FM_DISCOVERY_PROVIDER_MAX_BODY" envDefault:"2097152"`
	OpenPackRegistryFile        string        `env:"FM_OPEN_PACK_REGISTRY_FILE"`
	// Federation remains disabled unless an operator supplies a dedicated
	// identity and trust registry. ListenAddr enables only the separate inbound
	// URL-discovery listener; outbound peer sources are enabled explicitly in a
	// discovery pack and source allowlist.
	FederationIdentityFile      string        `env:"FM_FEDERATION_IDENTITY_FILE"`
	FederationTrustRegistryFile string        `env:"FM_FEDERATION_TRUST_REGISTRY_FILE"`
	FederationListenAddr        string        `env:"FM_FEDERATION_LISTEN_ADDR"`
	CrossrefMailto              string        `env:"FM_CROSSREF_MAILTO"`
	PubMedEmail                 string        `env:"FM_PUBMED_EMAIL"`
	YaCyURL                     string        `env:"FM_YACY_URL"`
	YaCyResource                string        `env:"FM_YACY_RESOURCE" envDefault:"local"`
	YaCyAllowInsecureHTTP       bool          `env:"FM_YACY_ALLOW_INSECURE_HTTP" envDefault:"false"`
	CompatAnswerProvider        string        `env:"FM_COMPAT_ANSWER_PROVIDER"`
	CompatAnswerMaxTokens       int           `env:"FM_COMPAT_ANSWER_MAX_TOKENS" envDefault:"512"`
	CompatAnswerTimeout         time.Duration `env:"FM_COMPAT_ANSWER_TIMEOUT"    envDefault:"30s"`

	FetchConcurrency     int           `env:"FM_FETCH_CONCURRENCY"      envDefault:"10"`
	PerHostConcurrency   int           `env:"FM_PER_HOST_CONCURRENCY"   envDefault:"2"`
	FetchTimeout         time.Duration `env:"FM_FETCH_TIMEOUT"          envDefault:"8s"`
	HeaderTimeout        time.Duration `env:"FM_HEADER_TIMEOUT"         envDefault:"5s"`
	FetchRetries         int           `env:"FM_FETCH_RETRIES"          envDefault:"2"`
	MaxBodyBytes         int64         `env:"FM_MAX_BODY_BYTES"         envDefault:"5242880"`  // 5 MiB
	MaxDecompressedBytes int64         `env:"FM_MAX_DECOMPRESSED_BYTES" envDefault:"20971520"` // 20 MiB
	MaxRedirects         int           `env:"FM_MAX_REDIRECTS"          envDefault:"5"`
	AllowedMIME          []string      `env:"FM_ALLOWED_MIME"           envDefault:"text/html,application/xhtml+xml" envSeparator:","`

	ProxyURL       string   `env:"FM_PROXY_URL"`
	RespectRobots  bool     `env:"FM_RESPECT_ROBOTS"  envDefault:"true"`
	UserAgent      string   `env:"FM_USER_AGENT"      envDefault:"Fetchmark/0.1 (+https://github.com/staticvar/fetchmark)"`
	UserAgentsPool []string `env:"FM_USER_AGENTS"     envSeparator:","`
	Contact        string   `env:"FM_CONTACT"`

	HostAllowlist []string `env:"FM_HOST_ALLOWLIST" envSeparator:","`
	HostDenylist  []string `env:"FM_HOST_DENYLIST"  envSeparator:","`

	CacheTTL           time.Duration `env:"FM_CACHE_TTL"             envDefault:"1h"`
	MemoryCacheEntries int           `env:"FM_MEMORY_CACHE_ENTRIES"  envDefault:"512"`
	MemoryCacheBytes   int64         `env:"FM_MEMORY_CACHE_BYTES"    envDefault:"134217728"` // 128 MiB
	CacheMaxValueBytes int64         `env:"FM_CACHE_MAX_VALUE_BYTES" envDefault:"8388608"`   // 8 MiB

	// LocalCorpusMode makes retention intent explicit. Disabled is the default;
	// ephemeral is process-local; personal expires automatically; curated only
	// accepts explicit ingestion plus revocations; archive keeps admitted
	// versions until an explicit removal policy runs.
	LocalCorpusMode   string        `env:"FM_LOCAL_CORPUS_MODE" envDefault:"disabled"`
	LocalCorpusMaxAge time.Duration `env:"FM_LOCAL_CORPUS_MAX_AGE" envDefault:"720h"`
	// LocalExpirySweepInterval bounds how long expired personal data remains
	// physically present after it stops being readable/searchable.
	LocalExpirySweepInterval time.Duration `env:"FM_LOCAL_EXPIRY_SWEEP_INTERVAL" envDefault:"15m"`
	// LocalIndexPath is required only for persistent modes. Ephemeral mode uses
	// a memory index and therefore rejects a path rather than silently writing.
	LocalIndexPath                string `env:"FM_LOCAL_INDEX_PATH"`
	LocalIndexMaxDocumentBytes    int    `env:"FM_LOCAL_INDEX_MAX_DOCUMENT_BYTES" envDefault:"2097152"`
	LocalIndexMaxBytes            int64  `env:"FM_LOCAL_INDEX_MAX_BYTES" envDefault:"1073741824"`
	LocalIndexMaxDocuments        int    `env:"FM_LOCAL_INDEX_MAX_DOCUMENTS" envDefault:"100000"`
	LocalArtifactPath             string `env:"FM_LOCAL_ARTIFACT_PATH"`
	LocalArtifactMaxBytes         int64  `env:"FM_LOCAL_ARTIFACT_MAX_BYTES" envDefault:"1073741824"`
	LocalArtifactMaxValueBytes    int64  `env:"FM_LOCAL_ARTIFACT_MAX_VALUE_BYTES" envDefault:"20971520"`
	LocalArtifactMaxEntries       int    `env:"FM_LOCAL_ARTIFACT_MAX_ENTRIES" envDefault:"100000"`
	LocalArchiveMaxVersions       int    `env:"FM_LOCAL_ARCHIVE_MAX_VERSIONS" envDefault:"100000"`
	LocalArchiveMaxVersionsPerURL int    `env:"FM_LOCAL_ARCHIVE_MAX_VERSIONS_PER_URL" envDefault:"16"`

	ArtifactConcurrency   int   `env:"FM_ARTIFACT_CONCURRENCY"     envDefault:"3"`
	MaxRequestSourceBytes int64 `env:"FM_MAX_REQUEST_SOURCE_BYTES" envDefault:"67108864"`  // 64 MiB
	MaxRequestOutputBytes int64 `env:"FM_MAX_REQUEST_OUTPUT_BYTES" envDefault:"134217728"` // 128 MiB

	MaxResults int `env:"FM_MAX_RESULTS" envDefault:"10"`
	ResultsCap int `env:"FM_RESULTS_CAP" envDefault:"50"`

	// Per-API-key rate limits. Rate is requests per second sustained;
	// Burst is the token bucket capacity. A Rate of 0 disables limiting.
	RateLimitPerSec float64 `env:"FM_RATE_LIMIT_PER_SEC" envDefault:"5"`
	RateLimitBurst  int     `env:"FM_RATE_LIMIT_BURST"   envDefault:"20"`

	DashboardUser     string `env:"FM_DASHBOARD_USER"`
	DashboardPassword string `env:"FM_DASHBOARD_PASSWORD"`

	// Headless renderer integration. When RendererURL is empty the
	// feature is disabled and render=true requests degrade to the plain
	// fetch path. RendererAuto toggles automatic retry when the
	// extractor flags a page as js_required.
	RendererURL             string        `env:"FM_RENDERER_URL"`
	RendererAuto            bool          `env:"FM_RENDERER_AUTO"     envDefault:"false"`
	RendererTimeout         time.Duration `env:"FM_RENDERER_TIMEOUT"  envDefault:"20s"`
	RendererMaxBody         int64         `env:"FM_RENDERER_MAX_BODY" envDefault:"10485760"` // 10 MiB
	RendererToken           string        `env:"FM_RENDERER_TOKEN"`
	RendererEgressProxyURL  string        `env:"FM_RENDERER_EGRESS_PROXY_URL"`
	RendererProxyListenAddr string        `env:"FM_RENDERER_PROXY_LISTEN_ADDR" envDefault:"127.0.0.1:8081"`

	// /v1/summarize runtime configuration. Env vars bootstrap one or
	// two profiles (one per provider kind) at boot; admin PUT calls
	// can add or update profiles at runtime but those mutations are
	// process-local. Leaving FM_SUMMARIZE_*_MODEL empty for a given
	// kind skips that profile entirely.
	SummarizeDefaultProvider string `env:"FM_SUMMARIZE_DEFAULT_PROVIDER"`

	SummarizeMaxTokensCap          int           `env:"FM_SUMMARIZE_MAX_TOKENS_CAP"       envDefault:"4096"`
	SummarizeMaxTimeout            time.Duration `env:"FM_SUMMARIZE_MAX_TIMEOUT"          envDefault:"120s"`
	SummarizeMaxInstructionsLen    int           `env:"FM_SUMMARIZE_MAX_INSTRUCTIONS_LEN" envDefault:"4000"`
	SummarizeAllowModelOverride    bool          `env:"FM_SUMMARIZE_ALLOW_MODEL_OVERRIDE" envDefault:"false"`
	SummarizeAllowProviderOverride bool          `env:"FM_SUMMARIZE_ALLOW_PROVIDER_OVERRIDE" envDefault:"false"`
	SummarizeAllowThinkingOverride bool          `env:"FM_SUMMARIZE_ALLOW_THINKING_OVERRIDE" envDefault:"false"`

	SummarizeOpenAIBaseURL     string        `env:"FM_SUMMARIZE_OPENAI_BASE_URL"`
	SummarizeOpenAIAPIKey      string        `env:"FM_SUMMARIZE_OPENAI_API_KEY"`
	SummarizeOpenAIModel       string        `env:"FM_SUMMARIZE_OPENAI_MODEL"`
	SummarizeOpenAIMaxTokens   int           `env:"FM_SUMMARIZE_OPENAI_MAX_TOKENS"   envDefault:"1024"`
	SummarizeOpenAITimeout     time.Duration `env:"FM_SUMMARIZE_OPENAI_TIMEOUT"      envDefault:"60s"`
	SummarizeOpenAIThinking    bool          `env:"FM_SUMMARIZE_OPENAI_THINKING"     envDefault:"false"`
	SummarizeOpenAIThinkEffort string        `env:"FM_SUMMARIZE_OPENAI_THINK_EFFORT"`

	SummarizeAnthropicBaseURL     string        `env:"FM_SUMMARIZE_ANTHROPIC_BASE_URL"`
	SummarizeAnthropicAPIKey      string        `env:"FM_SUMMARIZE_ANTHROPIC_API_KEY"`
	SummarizeAnthropicModel       string        `env:"FM_SUMMARIZE_ANTHROPIC_MODEL"`
	SummarizeAnthropicMaxTokens   int           `env:"FM_SUMMARIZE_ANTHROPIC_MAX_TOKENS"  envDefault:"1024"`
	SummarizeAnthropicTimeout     time.Duration `env:"FM_SUMMARIZE_ANTHROPIC_TIMEOUT"     envDefault:"60s"`
	SummarizeAnthropicThinking    bool          `env:"FM_SUMMARIZE_ANTHROPIC_THINKING"    envDefault:"false"`
	SummarizeAnthropicThinkBudget int           `env:"FM_SUMMARIZE_ANTHROPIC_THINK_BUDGET" envDefault:"0"`
}

// Load reads configuration from the environment, applies defaults,
// trims whitespace around list entries and validates the result.
func Load() (Config, error) {
	var c Config
	if err := env.Parse(&c); err != nil {
		return Config{}, err
	}
	c.APIKeys = cleanList(c.APIKeys)
	c.AdminAPIKeys = cleanList(c.AdminAPIKeys)
	c.AllowedMIME = cleanList(c.AllowedMIME)
	c.UserAgentsPool = cleanList(c.UserAgentsPool)
	c.HostAllowlist = cleanList(c.HostAllowlist)
	c.HostDenylist = cleanList(c.HostDenylist)
	c.SearxngURLs = cleanList(c.SearxngURLs)
	c.DiscoveryEnabledPacks = cleanList(c.DiscoveryEnabledPacks)
	c.DiscoveryEnabledSources = cleanList(c.DiscoveryEnabledSources)
	c.DiscoveryPrimarySource = strings.TrimSpace(c.DiscoveryPrimarySource)
	c.YaCyURL = strings.TrimSpace(c.YaCyURL)
	c.YaCyResource = strings.ToLower(strings.TrimSpace(c.YaCyResource))
	// FM_SEARXNG_URLS wins when set; otherwise fall back to the single
	// FM_SEARXNG_URL so existing deployments keep working unchanged. Discard
	// env defaults entirely when SearXNG is not enabled: they must not turn an
	// optional source back into a runtime dependency.
	if listContains(c.DiscoveryEnabledSources, "searxng") {
		if len(c.SearxngURLs) == 0 && c.SearxngURL != "" {
			c.SearxngURLs = []string{c.SearxngURL}
		}
	} else {
		c.SearxngURL = ""
		c.SearxngURLs = nil
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c *Config) validate() error {
	if c.AdvancedSearchConcurrency < 1 || c.AdvancedSearchConcurrency > 16 {
		return errors.New("FM_ADVANCED_SEARCH_CONCURRENCY must be between 1 and 16")
	}
	if c.FetchConcurrency <= 0 {
		return errors.New("FM_FETCH_CONCURRENCY must be > 0")
	}
	if c.PerHostConcurrency <= 0 {
		return errors.New("FM_PER_HOST_CONCURRENCY must be > 0")
	}
	if c.MaxBodyBytes <= 0 || c.MaxDecompressedBytes <= 0 {
		return errors.New("body byte budgets must be > 0")
	}
	if c.MaxResults <= 0 || c.ResultsCap <= 0 || c.MaxResults > c.ResultsCap {
		return errors.New("FM_MAX_RESULTS must be >0 and <= FM_RESULTS_CAP")
	}
	if c.ArtifactConcurrency <= 0 {
		return errors.New("FM_ARTIFACT_CONCURRENCY must be > 0")
	}
	if c.MaxRequestSourceBytes <= 0 || c.MaxRequestOutputBytes <= 0 {
		return errors.New("request byte budgets must be > 0")
	}
	maxArtifactSource := c.MaxDecompressedBytes
	if c.MaxBodyBytes > maxArtifactSource {
		maxArtifactSource = c.MaxBodyBytes
	}
	if c.RendererURL != "" && c.RendererMaxBody > maxArtifactSource {
		maxArtifactSource = c.RendererMaxBody
	}
	if int64(c.ArtifactConcurrency)*maxArtifactSource > c.MaxRequestSourceBytes {
		return errors.New("FM_MAX_REQUEST_SOURCE_BYTES must cover one full source claim per artifact worker")
	}
	if c.MemoryCacheEntries <= 0 || c.MemoryCacheBytes <= 0 || c.CacheMaxValueBytes <= 0 {
		return errors.New("memory cache limits must be > 0")
	}
	if c.CacheMaxValueBytes > c.MemoryCacheBytes {
		return errors.New("FM_CACHE_MAX_VALUE_BYTES must be <= FM_MEMORY_CACHE_BYTES")
	}
	if c.LocalIndexMaxDocumentBytes <= 0 {
		return errors.New("FM_LOCAL_INDEX_MAX_DOCUMENT_BYTES must be > 0")
	}
	if c.LocalIndexMaxBytes <= 0 || c.LocalIndexMaxDocuments <= 0 {
		return errors.New("local index aggregate budgets must be > 0")
	}
	if int64(c.LocalIndexMaxDocumentBytes) > c.LocalIndexMaxBytes {
		return errors.New("FM_LOCAL_INDEX_MAX_DOCUMENT_BYTES must be <= FM_LOCAL_INDEX_MAX_BYTES")
	}
	if c.LocalArtifactMaxBytes <= 0 || c.LocalArtifactMaxValueBytes <= 0 {
		return errors.New("local artifact byte budgets must be > 0")
	}
	if c.LocalArtifactMaxEntries <= 0 {
		return errors.New("FM_LOCAL_ARTIFACT_MAX_ENTRIES must be > 0")
	}
	if c.LocalArtifactMaxValueBytes > c.LocalArtifactMaxBytes {
		return errors.New("FM_LOCAL_ARTIFACT_MAX_VALUE_BYTES must be <= FM_LOCAL_ARTIFACT_MAX_BYTES")
	}
	mode, err := localcorpus.ParseRetentionMode(c.LocalCorpusMode)
	if err != nil {
		return errors.New("FM_LOCAL_CORPUS_MODE must be one of disabled, ephemeral, personal, curated, archive")
	}
	c.LocalCorpusMode = string(mode)
	if c.LocalIndexPath != "" && !filepath.IsAbs(c.LocalIndexPath) {
		return errors.New("FM_LOCAL_INDEX_PATH must be absolute when set")
	}
	if c.LocalArtifactPath != "" && !filepath.IsAbs(c.LocalArtifactPath) {
		return errors.New("FM_LOCAL_ARTIFACT_PATH must be absolute when set")
	}
	switch mode {
	case localcorpus.ModeDisabled, localcorpus.ModeEphemeral:
		if c.LocalIndexPath != "" || c.LocalArtifactPath != "" {
			return errors.New("local corpus paths must be empty when FM_LOCAL_CORPUS_MODE is disabled or ephemeral")
		}
	case localcorpus.ModePersonal:
		if c.LocalIndexPath == "" || c.LocalArtifactPath == "" {
			return errors.New("FM_LOCAL_INDEX_PATH and FM_LOCAL_ARTIFACT_PATH are required in personal mode")
		}
		if pathsOverlap(c.LocalIndexPath, c.LocalArtifactPath) || resolvedPathsOverlap(c.LocalIndexPath, c.LocalArtifactPath) {
			return errors.New("FM_LOCAL_INDEX_PATH and FM_LOCAL_ARTIFACT_PATH must not overlap")
		}
	case localcorpus.ModeCurated:
		if c.LocalIndexPath == "" || c.LocalArtifactPath == "" {
			return errors.New("FM_LOCAL_INDEX_PATH and FM_LOCAL_ARTIFACT_PATH are required in curated mode")
		}
		if pathsOverlap(c.LocalIndexPath, c.LocalArtifactPath) || resolvedPathsOverlap(c.LocalIndexPath, c.LocalArtifactPath) {
			return errors.New("FM_LOCAL_INDEX_PATH and FM_LOCAL_ARTIFACT_PATH must not overlap")
		}
		if len(c.AdminAPIKeys) == 0 {
			return errors.New("at least one FM_ADMIN_API_KEYS value is required in curated mode")
		}
		if !c.RespectRobots {
			return errors.New("FM_RESPECT_ROBOTS must be true in curated mode")
		}
	case localcorpus.ModeArchive:
		if c.LocalIndexPath == "" || c.LocalArtifactPath == "" {
			return errors.New("FM_LOCAL_INDEX_PATH and FM_LOCAL_ARTIFACT_PATH are required in archive mode")
		}
		if pathsOverlap(c.LocalIndexPath, c.LocalArtifactPath) || resolvedPathsOverlap(c.LocalIndexPath, c.LocalArtifactPath) {
			return errors.New("FM_LOCAL_INDEX_PATH and FM_LOCAL_ARTIFACT_PATH must not overlap")
		}
		if len(c.AdminAPIKeys) == 0 {
			return errors.New("at least one FM_ADMIN_API_KEYS value is required in archive mode")
		}
		if !c.RespectRobots {
			return errors.New("FM_RESPECT_ROBOTS must be true in archive mode")
		}
		if c.LocalArchiveMaxVersions <= 0 {
			return errors.New("FM_LOCAL_ARCHIVE_MAX_VERSIONS must be > 0 in archive mode")
		}
		if c.LocalArchiveMaxVersionsPerURL < 2 || c.LocalArchiveMaxVersionsPerURL > c.LocalArchiveMaxVersions {
			return errors.New("FM_LOCAL_ARCHIVE_MAX_VERSIONS_PER_URL must be between 2 and FM_LOCAL_ARCHIVE_MAX_VERSIONS")
		}
	}
	if mode == localcorpus.ModePersonal && c.LocalCorpusMaxAge <= 0 {
		return errors.New("FM_LOCAL_CORPUS_MAX_AGE must be > 0 in personal mode")
	}
	if mode == localcorpus.ModePersonal && c.LocalExpirySweepInterval <= 0 {
		return errors.New("FM_LOCAL_EXPIRY_SWEEP_INTERVAL must be > 0 in personal mode")
	}
	if len(c.DiscoveryEnabledSources) == 0 {
		return errors.New("at least one FM_DISCOVERY_ENABLED_SOURCES value is required")
	}
	if c.DiscoveryPrimarySource == "" || !listContains(c.DiscoveryEnabledSources, c.DiscoveryPrimarySource) {
		return errors.New("FM_DISCOVERY_PRIMARY_SOURCE must name an enabled discovery source")
	}
	if listContains(c.DiscoveryEnabledSources, "yacy") {
		if c.YaCyURL == "" {
			return errors.New("FM_YACY_URL is required when the yacy discovery source is enabled")
		}
		if c.YaCyResource != "local" && c.YaCyResource != "global" {
			return errors.New("FM_YACY_RESOURCE must be local or global")
		}
	}
	if listContains(c.DiscoveryEnabledSources, "searxng") {
		if len(c.SearxngURLs) == 0 {
			return errors.New("at least one SearXNG URL must be configured (FM_SEARXNG_URL or FM_SEARXNG_URLS)")
		}
		if c.SearxngCooldown <= 0 {
			return errors.New("FM_SEARXNG_COOLDOWN must be > 0")
		}
	}
	if c.DiscoveryCacheTTL <= 0 || c.DiscoveryCacheStaleTTL <= 0 || c.DiscoveryRefreshTimeout <= 0 {
		return errors.New("discovery cache durations must be > 0")
	}
	if c.DiscoveryCacheEntries <= 0 || c.DiscoveryCacheBytes <= 0 || c.DiscoveryCacheMaxEntryBytes <= 0 || c.DiscoveryMaxInflight <= 0 {
		return errors.New("discovery cache limits must be > 0")
	}
	if c.DiscoveryProviderMaxBody <= 0 {
		return errors.New("FM_DISCOVERY_PROVIDER_MAX_BODY must be > 0")
	}
	if c.OpenPackRegistryFile != "" && !filepath.IsAbs(c.OpenPackRegistryFile) {
		return errors.New("FM_OPEN_PACK_REGISTRY_FILE must be absolute when set")
	}
	if c.FederationIdentityFile != "" && !filepath.IsAbs(c.FederationIdentityFile) {
		return errors.New("FM_FEDERATION_IDENTITY_FILE must be absolute when set")
	}
	if c.FederationTrustRegistryFile != "" && !filepath.IsAbs(c.FederationTrustRegistryFile) {
		return errors.New("FM_FEDERATION_TRUST_REGISTRY_FILE must be absolute when set")
	}
	if (c.FederationIdentityFile == "") != (c.FederationTrustRegistryFile == "") {
		return errors.New("FM_FEDERATION_IDENTITY_FILE and FM_FEDERATION_TRUST_REGISTRY_FILE must be configured together")
	}
	if c.FederationIdentityFile != "" && filepath.Clean(c.FederationIdentityFile) == filepath.Clean(c.FederationTrustRegistryFile) {
		return errors.New("federation identity and trust registry files must be distinct")
	}
	if strings.TrimSpace(c.FederationListenAddr) != c.FederationListenAddr {
		return errors.New("FM_FEDERATION_LISTEN_ADDR must not have leading or trailing whitespace")
	}
	if c.FederationListenAddr != "" {
		if c.FederationIdentityFile == "" {
			return errors.New("federation identity and trust registry files are required when FM_FEDERATION_LISTEN_ADDR is set")
		}
		if mode != localcorpus.ModeCurated {
			return errors.New("FM_FEDERATION_LISTEN_ADDR requires FM_LOCAL_CORPUS_MODE=curated")
		}
		if listenerAddressesOverlap(c.FederationListenAddr, c.ListenAddr) {
			return errors.New("FM_FEDERATION_LISTEN_ADDR must differ from FM_LISTEN_ADDR")
		}
	}
	if c.DiscoveryCacheMaxEntryBytes > c.DiscoveryCacheBytes {
		return errors.New("FM_DISCOVERY_CACHE_MAX_ENTRY_BYTES must be <= FM_DISCOVERY_CACHE_BYTES")
	}
	if c.CompatAnswerMaxTokens <= 0 || c.CompatAnswerTimeout <= 0 {
		return errors.New("compatibility answer token and timeout limits must be > 0")
	}
	if c.CompatAnswerMaxTokens > 4096 || c.CompatAnswerTimeout > 2*time.Minute {
		return errors.New("compatibility answers are capped at 4096 tokens and 2 minutes")
	}
	if c.RendererURL != "" && c.RendererEgressProxyURL == "" {
		return errors.New("FM_RENDERER_EGRESS_PROXY_URL is required when FM_RENDERER_URL is set")
	}
	if c.SummarizeMaxTokensCap <= 0 {
		return errors.New("FM_SUMMARIZE_MAX_TOKENS_CAP must be > 0")
	}
	if c.SummarizeMaxTimeout <= 0 {
		return errors.New("FM_SUMMARIZE_MAX_TIMEOUT must be > 0")
	}
	if c.SummarizeMaxInstructionsLen < 0 {
		return errors.New("FM_SUMMARIZE_MAX_INSTRUCTIONS_LEN must be >= 0")
	}
	return nil
}

func listenerAddressesOverlap(left, right string) bool {
	leftHost, leftPort, leftErr := net.SplitHostPort(left)
	rightHost, rightPort, rightErr := net.SplitHostPort(right)
	if leftErr != nil || rightErr != nil {
		return left == right
	}
	if !sameListenerPort(leftPort, rightPort) {
		return false
	}
	leftHost = strings.Trim(strings.ToLower(leftHost), "[]")
	rightHost = strings.Trim(strings.ToLower(rightHost), "[]")
	if wildcardListenerHost(leftHost) || wildcardListenerHost(rightHost) {
		return true
	}
	leftIP, rightIP := net.ParseIP(leftHost), net.ParseIP(rightHost)
	if leftIP != nil && rightIP != nil {
		return leftIP.Equal(rightIP)
	}
	if loopbackListenerHost(leftHost, leftIP) && loopbackListenerHost(rightHost, rightIP) {
		return true
	}
	return leftHost == rightHost
}

func sameListenerPort(left, right string) bool {
	if left == right {
		return true
	}
	leftNumber, leftErr := strconv.ParseUint(left, 10, 16)
	rightNumber, rightErr := strconv.ParseUint(right, 10, 16)
	return leftErr == nil && rightErr == nil && leftNumber == rightNumber
}

func wildcardListenerHost(host string) bool {
	if host == "" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsUnspecified()
}

func loopbackListenerHost(host string, ip net.IP) bool {
	return host == "localhost" || (ip != nil && ip.IsLoopback())
}

func pathsOverlap(left, right string) bool {
	contains := func(parent, child string) bool {
		relative, err := filepath.Rel(filepath.Clean(parent), filepath.Clean(child))
		if err != nil {
			return false
		}
		return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
	}
	return contains(left, right) || contains(right, left)
}

func resolvedPathsOverlap(left, right string) bool {
	resolvedLeft, leftErr := resolveExistingPath(left)
	resolvedRight, rightErr := resolveExistingPath(right)
	return leftErr == nil && rightErr == nil && pathsOverlap(resolvedLeft, resolvedRight)
}

func resolveExistingPath(path string) (string, error) {
	path = filepath.Clean(path)
	tail := make([]string, 0, 4)
	current := path
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for index := len(tail) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, tail[index])
			}
			return filepath.Clean(resolved), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		tail = append(tail, filepath.Base(current))
		current = parent
	}
}

// DashboardEnabled reports whether the ops dashboard should be mounted.
// Disabled-by-default keeps unauthenticated exposure impossible when the
// operator forgets to set credentials.
func (c Config) DashboardEnabled() bool {
	return c.DashboardUser != "" && c.DashboardPassword != ""
}

func cleanList(in []string) []string {
	out := in[:0]
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func listContains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
