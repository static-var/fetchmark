package summarizer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/packages/param"
	"github.com/openai/openai-go/shared"
)

// OpenAIProvider speaks /v1/chat/completions. Works against real
// OpenAI, Azure OpenAI, SubSandwich, Groq, Together, Ollama's OpenAI
// shim, and any other server that implements the same wire format.
type OpenAIProvider struct {
	cfg    ProviderConfig
	client openai.Client
}

// NewOpenAIProvider builds a client with a caller-supplied HTTP
// transport. The transport is expected to be policy-aware (TLS opts,
// trusted-upstream dial rules) so this constructor does not try to
// second-guess what localhost or private IPs mean — that belongs in
// the transport.
func NewOpenAIProvider(cfg ProviderConfig, httpClient *http.Client) *OpenAIProvider {
	opts := []option.RequestOption{option.WithMaxRetries(0)}
	if cfg.APIKey != "" {
		opts = append(opts, option.WithAPIKey(cfg.APIKey))
	} else {
		// Some compatibility proxies (SubSandwich in local mode, Ollama)
		// accept an empty token but the SDK still sets the header. Pass
		// a placeholder so the library doesn't panic on init.
		opts = append(opts, option.WithAPIKey("placeholder"))
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
	return &OpenAIProvider{cfg: cfg, client: openai.NewClient(opts...)}
}

// Kind implements Provider.
func (p *OpenAIProvider) Kind() Kind { return KindOpenAI }

// Name implements Provider.
func (p *OpenAIProvider) Name() string { return p.cfg.Name }

// Summarize implements Provider.
func (p *OpenAIProvider) Summarize(ctx context.Context, req Request) (Response, error) {
	if err := req.Validate(); err != nil {
		return Response{}, fmt.Errorf("openai: %w", err)
	}

	msgs := make([]openai.ChatCompletionMessageParamUnion, 0, 2)
	if s := strings.TrimSpace(req.SystemPrompt); s != "" {
		msgs = append(msgs, openai.SystemMessage(s))
	}
	msgs = append(msgs, openai.UserMessage(req.UserPrompt))

	params := openai.ChatCompletionNewParams{
		Model:    shared.ChatModel(req.Model),
		Messages: msgs,
	}
	if req.MaxTokens > 0 {
		params.MaxCompletionTokens = param.NewOpt[int64](int64(req.MaxTokens))
	}
	if req.Temperature > 0 {
		params.Temperature = param.NewOpt[float64](req.Temperature)
	}
	if req.Thinking.Enabled && req.Thinking.Effort != "" {
		switch strings.ToLower(req.Thinking.Effort) {
		case "low":
			params.ReasoningEffort = shared.ReasoningEffortLow
		case "medium":
			params.ReasoningEffort = shared.ReasoningEffortMedium
		case "high":
			params.ReasoningEffort = shared.ReasoningEffortHigh
		}
	}

	resp, err := p.client.Chat.Completions.New(ctx, params)
	if err != nil {
		return Response{}, classifyErr("openai", err)
	}
	if len(resp.Choices) == 0 {
		return Response{}, errors.New("openai: no choices in response")
	}
	text := strings.TrimSpace(resp.Choices[0].Message.Content)
	if text == "" {
		return Response{}, errors.New("openai: empty completion content")
	}

	out := Response{
		Summary: text,
		Model:   string(resp.Model),
		Kind:    KindOpenAI,
		Usage: Usage{
			PromptTokens:     resp.Usage.PromptTokens,
			CompletionTokens: resp.Usage.CompletionTokens,
			TotalTokens:      resp.Usage.TotalTokens,
			ReasoningTokens:  resp.Usage.CompletionTokensDetails.ReasoningTokens,
		},
	}
	if out.Model == "" {
		out.Model = req.Model
	}
	return out, nil
}
