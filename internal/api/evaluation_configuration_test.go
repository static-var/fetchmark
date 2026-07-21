package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/evaluationmanifest"
)

func TestEvaluationConfigurationIsAuthenticatedAndBindsSearchResponses(t *testing.T) {
	manifest := []byte(`{"schema_version":1,"discovery":{"primary_source":"mwmbl"}}`)
	sum := sha256.Sum256(manifest)
	digest := hex.EncodeToString(sum[:])
	router := NewRouter(Deps{
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Config: config.Config{
			APIKeys: []string{"k1"}, ResultsCap: 50, MaxRequestOutputBytes: 1 << 20,
		},
		Pipeline:                   &fakePipeline{},
		EvaluationConfiguration:    manifest,
		EvaluationConfigurationSHA: digest,
	})

	unauthorized := httptest.NewRequest(http.MethodGet, "/v1/evaluation/configuration", nil)
	unauthorizedRecorder := httptest.NewRecorder()
	router.ServeHTTP(unauthorizedRecorder, unauthorized)
	if unauthorizedRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorizedRecorder.Code)
	}

	request := httptest.NewRequest(http.MethodGet, "/v1/evaluation/configuration", nil)
	request.Header.Set("Authorization", "Bearer k1")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || recorder.Body.String() != string(manifest) {
		t.Fatalf("status=%d body=%q", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get(evaluationmanifest.HeaderName); got != digest {
		t.Fatalf("manifest digest header = %q", got)
	}

	searchRequest := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"x"}`))
	searchRequest.Header.Set("Authorization", "Bearer k1")
	searchRecorder := httptest.NewRecorder()
	router.ServeHTTP(searchRecorder, searchRequest)
	if got := searchRecorder.Header().Get(evaluationmanifest.HeaderName); got != digest {
		t.Fatalf("search digest header = %q", got)
	}
}

func TestEvaluationConfigurationRejectsInvalidEvidenceWithoutAdvertisingIt(t *testing.T) {
	valid := []byte(`{"schema_version":1}`)
	validSum := sha256.Sum256(valid)
	for name, evidence := range map[string]struct {
		raw    []byte
		digest string
	}{
		"missing":      {},
		"invalid json": {raw: []byte(`{"schema_version":`)},
		"invalid hash": {raw: valid, digest: "not-a-digest"},
		"mismatched":   {raw: valid, digest: strings.Repeat("a", 64)},
		"oversized":    {raw: append([]byte(`{"schema_version":1,"padding":"`), append(bytes.Repeat([]byte("x"), evaluationmanifest.MaxManifestBytes), []byte(`"}`)...)...), digest: hex.EncodeToString(validSum[:])},
	} {
		t.Run(name, func(t *testing.T) {
			router := NewRouter(Deps{
				Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
				Config:   config.Config{APIKeys: []string{"k1"}, ResultsCap: 50, MaxRequestOutputBytes: 1 << 20},
				Pipeline: &fakePipeline{}, EvaluationConfiguration: evidence.raw, EvaluationConfigurationSHA: evidence.digest,
			})
			request := httptest.NewRequest(http.MethodGet, "/v1/evaluation/configuration", nil)
			request.Header.Set("Authorization", "Bearer k1")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get(evaluationmanifest.HeaderName) != "" {
				t.Fatalf("status=%d header=%q body=%q", recorder.Code, recorder.Header().Get(evaluationmanifest.HeaderName), recorder.Body.String())
			}

			searchRequest := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"query":"x"}`))
			searchRequest.Header.Set("Authorization", "Bearer k1")
			searchRecorder := httptest.NewRecorder()
			router.ServeHTTP(searchRecorder, searchRequest)
			if got := searchRecorder.Header().Get(evaluationmanifest.HeaderName); got != "" {
				t.Fatalf("invalid evidence advertised on search: %q", got)
			}
		})
	}
}
