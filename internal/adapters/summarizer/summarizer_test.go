package summarizer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// openAIResp is the minimum chat.completions shape we need.
type openAIResp struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Model   string         `json:"model"`
	Choices []openAIChoice `json:"choices"`
	Usage   openAIUsage    `json:"usage"`
}

type openAIChoice struct {
	Index        int           `json:"index"`
	Message      openAIMessage `json:"message"`
	FinishReason string        `json:"finish_reason"`
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIUsage struct {
	PromptTokens            int64                  `json:"prompt_tokens"`
	CompletionTokens        int64                  `json:"completion_tokens"`
	TotalTokens             int64                  `json:"total_tokens"`
	CompletionTokensDetails openAICompletionTokens `json:"completion_tokens_details"`
}

type openAICompletionTokens struct {
	ReasoningTokens int64 `json:"reasoning_tokens"`
}

func TestOpenAIProvider_Summarize_OK(t *testing.T) {
	var seen struct {
		Authorization string
		Body          map[string]any
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		seen.Authorization = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&seen.Body)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(openAIResp{
			ID:    "cmpl-1",
			Model: "glm-5.1",
			Choices: []openAIChoice{{
				Index:        0,
				Message:      openAIMessage{Role: "assistant", Content: "summary body"},
				FinishReason: "stop",
			}},
			Usage: openAIUsage{
				PromptTokens:            10,
				CompletionTokens:        20,
				TotalTokens:             30,
				CompletionTokensDetails: openAICompletionTokens{ReasoningTokens: 5},
			},
		})
	}))
	defer srv.Close()

	p := NewOpenAIProvider(ProviderConfig{
		Name: "test", Kind: KindOpenAI, BaseURL: srv.URL + "/v1/", APIKey: "sk-abc123", Model: "glm-5.1",
	}, srv.Client())

	resp, err := p.Summarize(context.Background(), Request{
		Model:      "glm-5.1",
		UserPrompt: "summarize this",
		MaxTokens:  128,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.Summary != "summary body" {
		t.Errorf("summary: got %q", resp.Summary)
	}
	if resp.Model != "glm-5.1" {
		t.Errorf("model: got %q", resp.Model)
	}
	if resp.Usage.PromptTokens != 10 || resp.Usage.CompletionTokens != 20 || resp.Usage.ReasoningTokens != 5 {
		t.Errorf("usage: %+v", resp.Usage)
	}
	if !strings.HasPrefix(seen.Authorization, "Bearer sk-abc123") {
		t.Errorf("auth header: %q", seen.Authorization)
	}
	if got, _ := seen.Body["model"].(string); got != "glm-5.1" {
		t.Errorf("model in body: %v", seen.Body["model"])
	}
}

func TestOpenAIProvider_Summarize_EmptyChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(openAIResp{ID: "c", Model: "m"})
	}))
	defer srv.Close()

	p := NewOpenAIProvider(ProviderConfig{
		Name: "t", Kind: KindOpenAI, BaseURL: srv.URL + "/v1/", Model: "m",
	}, srv.Client())
	_, err := p.Summarize(context.Background(), Request{Model: "m", UserPrompt: "x"})
	if err == nil {
		t.Fatal("expected error for empty choices")
	}
}

func TestOpenAIProvider_Summarize_Classified(t *testing.T) {
	cases := []struct {
		name string
		code int
		want string
	}{
		{"auth", http.StatusUnauthorized, "auth"},
		{"rate", http.StatusTooManyRequests, "rate_limit"},
		{"upstream", http.StatusBadGateway, "upstream"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, `{"error":{"message":"nope"}}`, tc.code)
			}))
			defer srv.Close()
			p := NewOpenAIProvider(ProviderConfig{
				Name: "t", Kind: KindOpenAI, BaseURL: srv.URL + "/v1/", APIKey: "k", Model: "m",
			}, srv.Client())
			_, err := p.Summarize(context.Background(), Request{Model: "m", UserPrompt: "x"})
			if err == nil {
				t.Fatal("expected error")
			}
			var ce *ClassifiedError
			if !errors.As(err, &ce) {
				t.Fatalf("want ClassifiedError, got %T: %v", err, err)
			}
			if ce.Kind != tc.want {
				t.Errorf("kind: got %q want %q", ce.Kind, tc.want)
			}
		})
	}
}

// Anthropic mock — minimum messages response.
type anthroResp struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Role    string          `json:"role"`
	Model   string          `json:"model"`
	Content []anthroBlock   `json:"content"`
	Usage   anthroRespUsage `json:"usage"`
}

type anthroBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

type anthroRespUsage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
}

func TestAnthropicProvider_Summarize_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages" {
			t.Errorf("path: %s", r.URL.Path)
		}
		if r.Header.Get("x-api-key") != "ak-xyz" {
			t.Errorf("x-api-key: %q", r.Header.Get("x-api-key"))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(anthroResp{
			ID: "msg_1", Type: "message", Role: "assistant", Model: "claude-3-5-sonnet",
			Content: []anthroBlock{
				{Type: "thinking", Text: "internal reasoning"},
				{Type: "text", Text: "final answer"},
			},
			Usage: anthroRespUsage{InputTokens: 42, OutputTokens: 7},
		})
	}))
	defer srv.Close()

	p := NewAnthropicProvider(ProviderConfig{
		Name: "claude", Kind: KindAnthropic, BaseURL: srv.URL, APIKey: "ak-xyz", Model: "claude-3-5-sonnet",
	}, srv.Client())

	resp, err := p.Summarize(context.Background(), Request{
		Model:      "claude-3-5-sonnet",
		UserPrompt: "summarize",
		MaxTokens:  256,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.Summary != "final answer" {
		t.Errorf("summary got %q; must skip thinking block", resp.Summary)
	}
	if resp.Usage.PromptTokens != 42 || resp.Usage.CompletionTokens != 7 || resp.Usage.TotalTokens != 49 {
		t.Errorf("usage: %+v", resp.Usage)
	}
}

func TestRegistry_ResolveAndDefault(t *testing.T) {
	reg := NewRegistry(DefaultFactory, http.DefaultClient)
	if !reg.Empty() {
		t.Fatal("registry should start empty")
	}
	if err := reg.Set(ProviderConfig{Name: "a", Kind: KindOpenAI, Model: "m"}); err != nil {
		t.Fatalf("set a: %v", err)
	}
	if err := reg.Set(ProviderConfig{Name: "b", Kind: KindAnthropic, Model: "m"}); err != nil {
		t.Fatalf("set b: %v", err)
	}
	if reg.DefaultName() != "a" {
		t.Errorf("default should be first inserted: %q", reg.DefaultName())
	}
	if err := reg.SetDefault("b"); err != nil {
		t.Fatal(err)
	}
	if reg.DefaultName() != "b" {
		t.Errorf("default: %q", reg.DefaultName())
	}
	p, err := reg.Resolve("")
	if err != nil || p == nil || p.Name() != "b" {
		t.Errorf("resolve default: %v %v", p, err)
	}
	if _, err := reg.Resolve("nope"); !errors.Is(err, ErrProviderNotFound) {
		t.Errorf("resolve unknown: %v", err)
	}
}

func TestRegistry_MergeWithExistingPreservesKey(t *testing.T) {
	reg := NewRegistry(DefaultFactory, http.DefaultClient)
	orig := ProviderConfig{Name: "x", Kind: KindOpenAI, BaseURL: "https://api.example/v1/", APIKey: "secret-0000", Model: "m"}
	if err := reg.Set(orig); err != nil {
		t.Fatal(err)
	}
	overlay := ProviderConfig{Name: "x", Model: "m-new"}
	merged, err := reg.MergeWithExisting(overlay)
	if err != nil {
		t.Fatal(err)
	}
	if merged.APIKey != "secret-0000" {
		t.Errorf("apikey should survive merge: %q", merged.APIKey)
	}
	if merged.Model != "m-new" {
		t.Errorf("model should be overridden: %q", merged.Model)
	}
	if merged.Kind != KindOpenAI {
		t.Errorf("kind should be preserved: %q", merged.Kind)
	}
}

func TestProviderConfig_ValidateRejectsBadScheme(t *testing.T) {
	cfg := ProviderConfig{Name: "x", Kind: KindOpenAI, Model: "m", BaseURL: "file:///etc/passwd"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for file:// base URL")
	}
}

func TestOpenAIProvider_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(200 * time.Millisecond):
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}))
	defer srv.Close()
	p := NewOpenAIProvider(ProviderConfig{Name: "t", Kind: KindOpenAI, BaseURL: srv.URL + "/v1/", Model: "m"}, srv.Client())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := p.Summarize(ctx, Request{Model: "m", UserPrompt: "x"})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	var ce *ClassifiedError
	if !errors.As(err, &ce) {
		t.Fatalf("want classified, got %T", err)
	}
	if ce.Kind != "timeout" {
		t.Errorf("kind: %q", ce.Kind)
	}
}
