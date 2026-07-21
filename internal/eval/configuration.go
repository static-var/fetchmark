package eval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/buildidentity"
	"github.com/staticvar/fetchmark/internal/evaluationmanifest"
)

// FetchConfigurationManifest retrieves the authenticated non-secret manifest
// paired with a native /v1/search endpoint and verifies its exact digest. It
// refuses redirects so an API key cannot be forwarded to another origin.
func FetchConfigurationManifest(ctx context.Context, client *http.Client, searchEndpoint, apiKey string) ([]byte, string, error) {
	configurationEndpoint, err := deriveConfigurationEndpoint(searchEndpoint)
	if err != nil {
		return nil, "", err
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	strictClient := *client
	strictClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, configurationEndpoint, nil)
	if err != nil {
		return nil, "", fmt.Errorf("eval: build configuration request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Fetchmark-Eval/1")
	if key := strings.TrimSpace(apiKey); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	response, err := strictClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("eval: fetch configuration manifest: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("eval: configuration endpoint returned HTTP %d", response.StatusCode)
	}
	headerValues := response.Header.Values(evaluationmanifest.HeaderName)
	if len(headerValues) != 1 {
		return nil, "", errors.New("eval: configuration response requires exactly one digest header")
	}
	digest, ok := buildidentity.Parse(headerValues[0])
	if !ok {
		return nil, "", errors.New("eval: configuration response has invalid digest header")
	}
	limited := &io.LimitedReader{R: response.Body, N: evaluationmanifest.MaxManifestBytes + 1}
	raw, err := io.ReadAll(limited)
	if err != nil {
		return nil, "", fmt.Errorf("eval: read configuration manifest: %w", err)
	}
	if limited.N == 0 {
		return nil, "", fmt.Errorf("eval: configuration manifest exceeds %d bytes", evaluationmanifest.MaxManifestBytes)
	}
	var header struct {
		SchemaVersion int `json:"schema_version"`
	}
	if err := json.Unmarshal(raw, &header); err != nil || header.SchemaVersion != 1 {
		return nil, "", errors.New("eval: configuration manifest is not supported v1 JSON")
	}
	computed := sha256.Sum256(raw)
	if hex.EncodeToString(computed[:]) != digest {
		return nil, "", errors.New("eval: configuration manifest digest mismatch")
	}
	return raw, digest, nil
}

func deriveConfigurationEndpoint(raw string) (string, error) {
	validated, err := validateEndpoint(raw)
	if err != nil {
		return "", err
	}
	parsed, err := url.Parse(validated)
	if err != nil {
		return "", errors.New("eval: invalid search endpoint")
	}
	const suffix = "/v1/search"
	if !strings.HasSuffix(parsed.Path, suffix) {
		return "", errors.New("eval: configuration manifest requires a native /v1/search endpoint")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, suffix) + "/v1/evaluation/configuration"
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}
