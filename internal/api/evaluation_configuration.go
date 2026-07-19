package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/staticvar/fetchmark/internal/buildidentity"
	"github.com/staticvar/fetchmark/internal/evaluationmanifest"
)

func evaluationConfigurationEvidence(d Deps) ([]byte, string, bool) {
	if len(d.EvaluationConfiguration) == 0 || len(d.EvaluationConfiguration) > evaluationmanifest.MaxManifestBytes || !json.Valid(d.EvaluationConfiguration) {
		return nil, "", false
	}
	digest, ok := buildidentity.Parse(d.EvaluationConfigurationSHA)
	if !ok {
		return nil, "", false
	}
	computed := sha256.Sum256(d.EvaluationConfiguration)
	if hex.EncodeToString(computed[:]) != digest {
		return nil, "", false
	}
	return d.EvaluationConfiguration, digest, true
}

func evaluationConfigurationHandler(d Deps) http.HandlerFunc {
	return func(writer http.ResponseWriter, _ *http.Request) {
		raw, digest, ok := evaluationConfigurationEvidence(d)
		if !ok {
			writeJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "evaluation configuration unavailable"})
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set(evaluationmanifest.HeaderName, digest)
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(raw)
	}
}
