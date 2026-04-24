package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/staticvar/fetchmark/internal/adapters/summarizer"
)

// adminProviderBody is the wire shape for PUT /admin/summarize/providers.
// Keys absent from the JSON leave existing values intact (merge-on-write
// semantics so admins can tweak Model without re-typing the API key).
type adminProviderBody struct {
	Name        string              `json:"name"`
	Kind        string              `json:"kind,omitempty"`
	BaseURL     string              `json:"base_url,omitempty"`
	APIKey      string              `json:"api_key,omitempty"`
	Model       string              `json:"model,omitempty"`
	TimeoutMS   int                 `json:"timeout_ms,omitempty"`
	MaxTokens   int                 `json:"max_tokens,omitempty"`
	Temperature float64             `json:"temperature,omitempty"`
	Thinking    *adminThinkingBody  `json:"thinking,omitempty"`
}

type adminThinkingBody struct {
	Enabled      bool   `json:"enabled"`
	BudgetTokens int    `json:"budget_tokens,omitempty"`
	Effort       string `json:"effort,omitempty"`
}

func adminSummarizeGet(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if d.Summarizers == nil {
			writeJSON(w, http.StatusOK, map[string]any{"providers": []any{}, "default": ""})
			return
		}
		cfgs, def := d.Summarizers.Snapshot()
		writeJSON(w, http.StatusOK, map[string]any{
			"providers": cfgs,
			"default":   def,
		})
	}
}

func adminSummarizeProviderPut(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if d.Summarizers == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "summarize_disabled"})
			return
		}
		var body adminProviderBody
		dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "detail": err.Error()})
			return
		}
		if strings.TrimSpace(body.Name) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name required"})
			return
		}

		overlay := summarizer.ProviderConfig{
			Name:        body.Name,
			Kind:        summarizer.Kind(body.Kind),
			BaseURL:     body.BaseURL,
			APIKey:      body.APIKey,
			Model:       body.Model,
			Timeout:     time.Duration(body.TimeoutMS) * time.Millisecond,
			MaxTokens:   body.MaxTokens,
			Temperature: body.Temperature,
		}
		if body.Thinking != nil {
			overlay.Thinking = summarizer.Thinking{
				Enabled:      body.Thinking.Enabled,
				BudgetTokens: body.Thinking.BudgetTokens,
				Effort:       body.Thinking.Effort,
			}
		}
		merged, err := d.Summarizers.MergeWithExisting(overlay)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_config", "detail": err.Error()})
			return
		}
		if err := d.Summarizers.Set(merged); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_config", "detail": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"provider": merged.Redact(),
			"default":  d.Summarizers.DefaultName(),
		})
	}
}

func adminSummarizeProviderDelete(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if d.Summarizers == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "summarize_disabled"})
			return
		}
		name := chi.URLParam(r, "name")
		d.Summarizers.Delete(name)
		writeJSON(w, http.StatusOK, map[string]any{
			"deleted": name,
			"default": d.Summarizers.DefaultName(),
		})
	}
}

func adminSummarizeDefaultPut(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if d.Summarizers == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "summarize_disabled"})
			return
		}
		var body struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20)).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
			return
		}
		if err := d.Summarizers.SetDefault(body.Name); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"default": d.Summarizers.DefaultName()})
	}
}
