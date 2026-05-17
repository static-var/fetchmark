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
// Optional fields are pointers so omission is distinguishable from an
// explicit zero: omitting `max_tokens` preserves the prior value;
// sending `"max_tokens": 0` clears it. Required identity fields
// (Name/Kind/BaseURL/Model) are plain strings.
type adminProviderBody struct {
	Name        string             `json:"name"`
	Kind        string             `json:"kind,omitempty"`
	BaseURL     string             `json:"base_url,omitempty"`
	APIKey      string             `json:"api_key,omitempty"`
	Model       string             `json:"model,omitempty"`
	TimeoutMS   *int               `json:"timeout_ms,omitempty"`
	MaxTokens   *int               `json:"max_tokens,omitempty"`
	Temperature *float64           `json:"temperature,omitempty"`
	Thinking    *adminThinkingBody `json:"thinking,omitempty"`
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
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "detail": err.Error()})
			return
		}
		if strings.TrimSpace(body.Name) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name required"})
			return
		}

		// Look up the existing config so omitted numeric fields keep
		// their prior values. Registry.Config returns a redacted copy,
		// but we only need the non-secret fields here — APIKey is
		// preserved separately inside MergeWithExisting.
		prior, _ := d.Summarizers.Config(body.Name)

		overlay := summarizer.ProviderConfig{
			Name:    body.Name,
			Kind:    summarizer.Kind(body.Kind),
			BaseURL: body.BaseURL,
			APIKey:  body.APIKey,
			Model:   body.Model,
		}
		if body.TimeoutMS != nil {
			overlay.Timeout = time.Duration(*body.TimeoutMS) * time.Millisecond
		} else {
			overlay.Timeout = prior.Timeout
		}
		if body.MaxTokens != nil {
			overlay.MaxTokens = *body.MaxTokens
		} else {
			overlay.MaxTokens = prior.MaxTokens
		}
		if body.Temperature != nil {
			overlay.Temperature = *body.Temperature
		} else {
			overlay.Temperature = prior.Temperature
		}
		if body.Thinking != nil {
			overlay.Thinking = summarizer.Thinking{
				Enabled:      body.Thinking.Enabled,
				BudgetTokens: body.Thinking.BudgetTokens,
				Effort:       body.Thinking.Effort,
			}
		} else {
			overlay.Thinking = prior.Thinking
		}
		merged, err := d.Summarizers.MergeWithExisting(overlay)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_config", "detail": err.Error()})
			return
		}
		if err := validateSummarizeEffectiveCaps(merged.MaxTokens, merged.Timeout, merged.Thinking, d.Config); err != nil {
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
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
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
