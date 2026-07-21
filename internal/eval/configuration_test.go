package eval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/staticvar/fetchmark/internal/evaluationmanifest"
)

func TestFetchConfigurationManifestDerivesEndpointAndVerifiesDigest(t *testing.T) {
	manifest := []byte(`{"schema_version":1,"discovery":{"primary_source":"mwmbl"}}`)
	sum := sha256.Sum256(manifest)
	digest := hex.EncodeToString(sum[:])
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/prefix/v1/evaluation/configuration" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("authorization = %q", request.Header.Get("Authorization"))
		}
		writer.Header().Set(evaluationmanifest.HeaderName, digest)
		_, _ = writer.Write(manifest)
	}))
	t.Cleanup(server.Close)

	raw, gotDigest, err := FetchConfigurationManifest(context.Background(), server.Client(), server.URL+"/prefix/v1/search", "test-key")
	if err != nil {
		t.Fatalf("FetchConfigurationManifest: %v", err)
	}
	if string(raw) != string(manifest) || gotDigest != digest {
		t.Fatalf("raw=%q digest=%q", raw, gotDigest)
	}
}

func TestFetchConfigurationManifestRejectsDigestMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set(evaluationmanifest.HeaderName, strings.Repeat("0", 64))
		_, _ = writer.Write([]byte(`{"schema_version":1}`))
	}))
	t.Cleanup(server.Close)
	if _, _, err := FetchConfigurationManifest(context.Background(), server.Client(), server.URL+"/v1/search", ""); err == nil {
		t.Fatal("digest mismatch was accepted")
	}
}
