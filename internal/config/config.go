package config

import (
	"errors"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
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
	// SearxngCooldown is how long a SearXNG instance is skipped after a
	// transport error or 5xx. Non-positive values are rejected so a
	// misconfigured env var cannot accidentally disable failover.
	SearxngCooldown time.Duration `env:"FM_SEARXNG_COOLDOWN" envDefault:"30s"`
	RedisURL        string        `env:"FM_REDIS_URL"        envDefault:"redis://redis:6379/0"`

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

	CacheTTL   time.Duration `env:"FM_CACHE_TTL"    envDefault:"1h"`
	MaxResults int           `env:"FM_MAX_RESULTS"  envDefault:"10"`
	ResultsCap int           `env:"FM_RESULTS_CAP"  envDefault:"50"`

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
	RendererURL     string        `env:"FM_RENDERER_URL"`
	RendererAuto    bool          `env:"FM_RENDERER_AUTO"     envDefault:"false"`
	RendererTimeout time.Duration `env:"FM_RENDERER_TIMEOUT"  envDefault:"20s"`
	RendererMaxBody int64         `env:"FM_RENDERER_MAX_BODY" envDefault:"10485760"` // 10 MiB
	RendererToken   string        `env:"FM_RENDERER_TOKEN"`

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
	// FM_SEARXNG_URLS wins when set; otherwise fall back to the single
	// FM_SEARXNG_URL so existing deployments keep working unchanged.
	if len(c.SearxngURLs) == 0 && c.SearxngURL != "" {
		c.SearxngURLs = []string{c.SearxngURL}
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) validate() error {
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
	if len(c.SearxngURLs) == 0 {
		return errors.New("at least one SearXNG URL must be configured (FM_SEARXNG_URL or FM_SEARXNG_URLS)")
	}
	if c.SearxngCooldown <= 0 {
		return errors.New("FM_SEARXNG_COOLDOWN must be > 0")
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
