package summarizer

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// ProviderConfig is a single named upstream. Multiple profiles can
// exist in the registry at once; callers pick one by name via the
// default or a request-scoped override.
type ProviderConfig struct {
	Name    string        `json:"name"`
	Kind    Kind          `json:"kind"`
	BaseURL string        `json:"base_url"`
	APIKey  string        `json:"-"`
	Model   string        `json:"model"`
	Timeout time.Duration `json:"timeout"`

	// Optional defaults applied to every request unless the caller
	// overrides them in Request.
	MaxTokens   int     `json:"max_tokens,omitempty"`
	Temperature float64 `json:"temperature,omitempty"`

	// Thinking defaults; a per-request Request.Thinking wins.
	Thinking Thinking `json:"thinking,omitempty"`
}

// Validate enforces the invariants the registry and handler rely on.
// Callable on config-write (admin PUT) so we reject bad overrides
// before persisting them.
func (p ProviderConfig) Validate() error {
	if strings.TrimSpace(p.Name) == "" {
		return errors.New("name required")
	}
	switch p.Kind {
	case KindOpenAI, KindAnthropic:
	default:
		return fmt.Errorf("kind must be %q or %q", KindOpenAI, KindAnthropic)
	}
	if strings.TrimSpace(p.Model) == "" {
		return errors.New("model required")
	}
	if p.BaseURL != "" {
		u, err := url.Parse(p.BaseURL)
		if err != nil {
			return fmt.Errorf("base_url: %w", err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return errors.New("base_url: scheme must be http or https")
		}
		if u.Host == "" {
			return errors.New("base_url: host required")
		}
	}
	if p.Timeout < 0 {
		return errors.New("timeout must be >= 0")
	}
	if p.Thinking.Enabled && p.Thinking.BudgetTokens > 0 && p.Thinking.BudgetTokens < 1024 {
		return errors.New("thinking.budget_tokens must be >= 1024")
	}
	return nil
}

// Redact returns a copy with the API key replaced by a suffix-only
// hint. Used for admin GET responses so secrets never leave the
// process unmasked.
func (p ProviderConfig) Redact() ProviderConfig {
	p.APIKey = maskKey(p.APIKey)
	return p
}

func maskKey(k string) string {
	if k == "" {
		return ""
	}
	if len(k) <= 4 {
		return "***"
	}
	return "***" + k[len(k)-4:]
}
