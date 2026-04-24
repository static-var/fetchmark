// Package summarizer wraps the OpenAI and Anthropic SDKs behind a
// single narrow Provider interface so /v1/summarize can switch
// upstreams without leaking SDK types into the API layer.
//
// Providers are stateless; per-request overrides flow through Request
// so the HTTP handler can let callers pick a model or reasoning budget
// without reconfiguring the process. The registry (see registry.go)
// owns the lifecycle and provides the configured defaults.
package summarizer

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Kind enumerates the wire protocols Fetchmark can speak.
type Kind string

const (
	// KindOpenAI targets the OpenAI /v1/chat/completions shape. This
	// also covers Azure OpenAI, Groq, Together, SubSandwich, and any
	// other proxy that re-implements that wire format.
	KindOpenAI Kind = "openai"
	// KindAnthropic targets Anthropic's /v1/messages shape.
	KindAnthropic Kind = "anthropic"
)

// Thinking carries the caller's reasoning preference. Providers map
// this to their native knob (Anthropic thinking.budget, OpenAI
// reasoning_effort) and degrade gracefully when the upstream model
// does not support the knob at all.
type Thinking struct {
	// Enabled toggles native reasoning support. When false the
	// provider omits the knob entirely so older models still work.
	Enabled bool `json:"enabled,omitempty"`
	// BudgetTokens caps reasoning tokens (Anthropic); ignored by
	// OpenAI which uses Effort instead. Must be >= 1024 when set.
	BudgetTokens int `json:"budget_tokens,omitempty"`
	// Effort is one of "low", "medium", "high" and is forwarded to
	// OpenAI's reasoning_effort. Empty means "do not set".
	Effort string `json:"effort,omitempty"`
}

// Request is the wire-independent summarize call. SystemPrompt is
// optional; most callers only need UserPrompt.
type Request struct {
	Model        string
	SystemPrompt string
	UserPrompt   string
	MaxTokens    int
	Temperature  float64
	Thinking     Thinking
}

// Usage is the normalized token accounting. ReasoningTokens is 0 when
// the upstream did not bill reasoning separately.
type Usage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	ReasoningTokens  int64 `json:"reasoning_tokens,omitempty"`
	TotalTokens      int64 `json:"total_tokens"`
}

// Response is what the Provider returns. Raw reasoning/thinking text is
// intentionally dropped: callers have no way to verify or sanitize it
// and surfacing it encourages prompt-injection leaks.
type Response struct {
	Summary string
	Model   string
	Kind    Kind
	Usage   Usage
}

// Provider is the narrow port the summarize handler binds to.
type Provider interface {
	Kind() Kind
	Name() string
	Summarize(ctx context.Context, req Request) (Response, error)
}

// Validate sanity-checks a Request before it hits the network. Errors
// surface as 400s from the handler.
func (r Request) Validate() error {
	if strings.TrimSpace(r.Model) == "" {
		return errors.New("model required")
	}
	if strings.TrimSpace(r.UserPrompt) == "" {
		return errors.New("user prompt required")
	}
	if r.MaxTokens < 0 {
		return errors.New("max_tokens must be >= 0")
	}
	if r.Thinking.Enabled && r.Thinking.BudgetTokens > 0 && r.Thinking.BudgetTokens < 1024 {
		return fmt.Errorf("thinking.budget_tokens must be >= 1024 (got %d)", r.Thinking.BudgetTokens)
	}
	return nil
}
