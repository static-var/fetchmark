package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/pipeline"
)

type corpusMutationRequest struct {
	URLs                 []string        `json:"urls"`
	SafetyClassification json.RawMessage `json:"safety_classification,omitempty"`
}

type focusedCorpusAdmissionRequest struct {
	URL                 string   `json:"url"`
	AllowedPathPrefixes []string `json:"allowed_path_prefixes"`
	DeniedPathPrefixes  []string `json:"denied_path_prefixes,omitempty"`
	DeniedURLs          []string `json:"denied_urls,omitempty"`
	MaxOutboundLinks    int      `json:"max_outbound_links,omitempty"`
}

func adminCorpusAdmission(d Deps) http.HandlerFunc {
	return adminCorpusMutation(d, "admit")
}

func adminCorpusFocusedAdmission(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if d.Config.LocalCorpusMode != string(localcorpus.ModeCurated) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "curated_mode_required"})
			return
		}
		if d.CorpusCurator == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "curated_corpus_not_ready"})
			return
		}
		var request focusedCorpusAdmissionRequest
		if err := decodeCorpusMutation(w, r, &request); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
			return
		}
		admission, err := validateFocusedCorpusAdmission(request)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		results := d.CorpusCurator.AdmitCuratedFocused(r.Context(), admission)
		writeJSONBounded(w, http.StatusOK, map[string]any{"count": len(results), "results": results}, d.Config.MaxRequestOutputBytes)
	}
}

func adminCorpusTakedown(d Deps) http.HandlerFunc {
	return adminCorpusMutation(d, "takedown")
}

func adminCorpusMutation(d Deps, operation string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mode := d.Config.LocalCorpusMode
		if operation == "admit" && mode != string(localcorpus.ModeCurated) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "curated_mode_required"})
			return
		}
		if operation == "takedown" && mode != string(localcorpus.ModeCurated) && mode != string(localcorpus.ModeArchive) {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "persistent_mode_required"})
			return
		}
		if d.CorpusCurator == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "curated_corpus_not_ready"})
			return
		}
		var request corpusMutationRequest
		if err := decodeCorpusMutation(w, r, &request); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
			return
		}
		urls, err := validateCorpusMutationURLs(request.URLs, d.Config.ResultsCap)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		var results []pipeline.CuratedMutationResult
		if operation == "takedown" {
			if len(request.SafetyClassification) != 0 {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "safety_classification_not_allowed"})
				return
			}
			results = d.CorpusCurator.TakedownCurated(r.Context(), urls)
		} else {
			classification, err := decodeSafetyClassification(request.SafetyClassification)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_safety_classification"})
				return
			}
			results = d.CorpusCurator.AdmitCuratedClassified(r.Context(), urls, classification)
		}
		writeJSONBounded(w, http.StatusOK, map[string]any{"count": len(results), "results": results}, d.Config.MaxRequestOutputBytes)
	}
}

func validateFocusedCorpusAdmission(request focusedCorpusAdmissionRequest) (pipeline.FocusedAdmission, error) {
	if request.MaxOutboundLinks < 0 || request.MaxOutboundLinks > pipeline.MaxFocusedOutboundLinks {
		return pipeline.FocusedAdmission{}, errors.New("invalid_max_outbound_links")
	}
	urls, err := validateCorpusMutationURLs([]string{request.URL}, 1)
	if err != nil {
		return pipeline.FocusedAdmission{}, err
	}
	urls[0], err = canonicalFocusedAdmissionURL(urls[0])
	if err != nil {
		return pipeline.FocusedAdmission{}, errors.New("invalid_url")
	}
	allowed, err := validateFocusedPathPrefixes(request.AllowedPathPrefixes, true)
	if err != nil {
		return pipeline.FocusedAdmission{}, err
	}
	denied, err := validateFocusedPathPrefixes(request.DeniedPathPrefixes, false)
	if err != nil {
		return pipeline.FocusedAdmission{}, err
	}
	deniedURLs := []string(nil)
	if len(request.DeniedURLs) > 0 {
		deniedURLs, err = validateCorpusMutationURLs(request.DeniedURLs, 256)
		if err != nil {
			return pipeline.FocusedAdmission{}, err
		}
		for index := range deniedURLs {
			deniedURLs[index], err = canonicalFocusedAdmissionURL(deniedURLs[index])
			if err != nil {
				return pipeline.FocusedAdmission{}, errors.New("invalid_url")
			}
		}
	}
	parsed, err := url.Parse(urls[0])
	if err != nil {
		return pipeline.FocusedAdmission{}, errors.New("invalid_url")
	}
	escapedPath := parsed.EscapedPath()
	if escapedPath == "" {
		escapedPath = "/"
	}
	for _, deniedURL := range deniedURLs {
		if urls[0] == deniedURL {
			return pipeline.FocusedAdmission{}, errors.New("url_outside_scope")
		}
	}
	for _, prefix := range denied {
		if strings.HasPrefix(escapedPath, prefix) {
			return pipeline.FocusedAdmission{}, errors.New("url_outside_scope")
		}
	}
	withinAllowed := false
	for _, prefix := range allowed {
		if strings.HasPrefix(escapedPath, prefix) {
			withinAllowed = true
			break
		}
	}
	if !withinAllowed {
		return pipeline.FocusedAdmission{}, errors.New("url_outside_scope")
	}
	return pipeline.FocusedAdmission{
		URL: urls[0], AllowedPathPrefixes: allowed, DeniedPathPrefixes: denied, DeniedURLs: deniedURLs,
		MaxOutboundLinks: request.MaxOutboundLinks,
	}, nil
}

func validateFocusedPathPrefixes(prefixes []string, required bool) ([]string, error) {
	if required && len(prefixes) == 0 {
		return nil, errors.New("allowed_path_prefixes_required")
	}
	if len(prefixes) > 256 {
		return nil, errors.New("too_many_path_prefixes")
	}
	out := make([]string, 0, len(prefixes))
	seen := make(map[string]struct{}, len(prefixes))
	for _, prefix := range prefixes {
		lower := strings.ToLower(prefix)
		if len(prefix) > 2048 || prefix == "" || !strings.HasPrefix(prefix, "/") ||
			strings.ContainsAny(prefix, "?#\\%") || strings.Contains(lower, "%2f") || strings.Contains(lower, "%5c") {
			return nil, errors.New("invalid_path_prefix")
		}
		parsed, err := url.PathUnescape(prefix)
		if err != nil {
			return nil, errors.New("invalid_path_prefix")
		}
		if !utf8.ValidString(parsed) {
			return nil, errors.New("invalid_path_prefix")
		}
		for _, segment := range strings.Split(parsed, "/") {
			if segment == "." || segment == ".." {
				return nil, errors.New("invalid_path_prefix")
			}
		}
		if _, duplicate := seen[parsed]; duplicate {
			return nil, errors.New("duplicate_path_prefix")
		}
		seen[parsed] = struct{}{}
		out = append(out, parsed)
	}
	return out, nil
}

func canonicalFocusedAdmissionURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil {
		return "", errors.New("invalid URL")
	}
	escaped := strings.ToLower(parsed.EscapedPath())
	if strings.Contains(escaped, "%2f") || strings.Contains(escaped, "%5c") || strings.Contains(parsed.Path, `\`) {
		return "", errors.New("ambiguous path")
	}
	pathValue := parsed.Path
	if pathValue == "" {
		pathValue = "/"
	}
	if !utf8.ValidString(pathValue) {
		return "", errors.New("ambiguous path")
	}
	for _, segment := range strings.Split(pathValue, "/") {
		if segment == "." || segment == ".." {
			return "", errors.New("ambiguous path")
		}
	}
	parsed.Path = pathValue
	parsed.RawPath = ""
	return cache.CanonicalURL(parsed.String())
}

func decodeSafetyClassification(raw json.RawMessage) (localcorpus.SafetyClassification, error) {
	if len(raw) == 0 {
		return localcorpus.SafetyUnclassified, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", errors.New("safety classification must not be null")
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", err
	}
	if strings.TrimSpace(value) == "" {
		return "", errors.New("safety classification must not be empty")
	}
	return localcorpus.ParseSafetyClassification(value)
}

func decodeCorpusMutation(w http.ResponseWriter, r *http.Request, destination any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("request must contain exactly one JSON value")
	}
	return nil
}

func validateCorpusMutationURLs(rawURLs []string, limit int) ([]string, error) {
	if len(rawURLs) == 0 {
		return nil, errors.New("urls_required")
	}
	if limit > 0 && len(rawURLs) > limit {
		return nil, errors.New("too_many_urls")
	}
	canonicalURLs := make([]string, 0, len(rawURLs))
	seen := make(map[string]struct{}, len(rawURLs))
	for _, rawURL := range rawURLs {
		if len(rawURL) > 2048 {
			return nil, errors.New("invalid_url")
		}
		parsed, err := url.Parse(strings.TrimSpace(rawURL))
		if err != nil || parsed.User != nil || parsed.Hostname() == "" ||
			(!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) {
			return nil, errors.New("invalid_url")
		}
		canonical, err := cache.CanonicalURL(parsed.String())
		if err != nil {
			return nil, errors.New("invalid_url")
		}
		if _, duplicate := seen[canonical]; duplicate {
			return nil, errors.New("duplicate_url")
		}
		seen[canonical] = struct{}{}
		canonicalURLs = append(canonicalURLs, canonical)
	}
	return canonicalURLs, nil
}
