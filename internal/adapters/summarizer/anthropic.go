package summarizer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
)

// AnthropicProvider speaks /v1/messages. Extended-thinking blocks are
// counted in Usage but never surfaced as text.
type AnthropicProvider struct {
	cfg    ProviderConfig
	client anthropic.Client
}

// NewAnthropicProvider mirrors NewOpenAIProvider: the transport is the
// caller's business so egress policy can be centralized.
func NewAnthropicProvider(cfg ProviderConfig, httpClient *http.Client) *AnthropicProvider {
	opts := []option.RequestOption{option.WithMaxRetries(0)}
	if cfg.APIKey != "" {
		opts = append(opts, option.WithAPIKey(cfg.APIKey))
	}
	if cfg.BaseURL != "" {
		b := cfg.BaseURL
		if !strings.HasSuffix(b, "/") {
			b += "/"
		}
		opts = append(opts, option.WithBaseURL(b))
	}
	if httpClient != nil {
		opts = append(opts, option.WithHTTPClient(httpClient))
	}
	return &AnthropicProvider{cfg: cfg, client: anthropic.NewClient(opts...)}
}

// Kind implements Provider.
func (p *AnthropicProvider) Kind() Kind { return KindAnthropic }

// Name implements Provider.
func (p *AnthropicProvider) Name() string { return p.cfg.Name }

// Summarize implements Provider.
func (p *AnthropicProvider) Summarize(ctx context.Context, req Request) (Response, error) {
	if err := req.Validate(); err != nil {
		return Response{}, fmt.Errorf("anthropic: %w", err)
	}

	// Anthropic requires a non-zero MaxTokens; pick a generous default
	// when the caller didn't supply one so the call doesn't 400.
	maxTokens := int64(req.MaxTokens)
	if maxTokens <= 0 {
		maxTokens = 1024
	}

	params := anthropic.MessageNewParams{
		Model:     anthropic.Model(req.Model),
		MaxTokens: maxTokens,
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(req.UserPrompt)),
		},
	}
	if s := strings.TrimSpace(req.SystemPrompt); s != "" {
		params.System = []anthropic.TextBlockParam{{Text: s}}
	}
	if req.Temperature > 0 {
		params.Temperature = anthropic.Float(req.Temperature)
	}
	if req.Thinking.Enabled && req.Thinking.BudgetTokens >= 1024 {
		params.Thinking = anthropic.ThinkingConfigParamOfEnabled(int64(req.Thinking.BudgetTokens))
	}

	resp, err := p.client.Messages.New(ctx, params)
	if err != nil {
		return Response{}, classifyErr("anthropic", err)
	}

	var buf strings.Builder
	var reasoningTokens int64 // placeholder: anthropic doesn't bill separately today
	for _, block := range resp.Content {
		switch block.Type {
		case "text":
			buf.WriteString(block.Text)
		case "thinking", "redacted_thinking":
			// Intentionally skipped: do not surface raw reasoning.
			_ = reasoningTokens
		}
	}
	text := strings.TrimSpace(buf.String())
	if text == "" {
		return Response{}, errors.New("anthropic: empty completion content")
	}

	return Response{
		Summary: text,
		Model:   string(resp.Model),
		Kind:    KindAnthropic,
		Usage: Usage{
			PromptTokens:     resp.Usage.InputTokens,
			CompletionTokens: resp.Usage.OutputTokens,
			TotalTokens:      resp.Usage.InputTokens + resp.Usage.OutputTokens,
		},
	}, nil
}
