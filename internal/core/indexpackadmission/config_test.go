package indexpackadmission

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

const validConfig = `{"version":1,"product_token":"FetchmarkPackEvidence","contact_uri":"mailto:operator@example.org","evidence_validity_hours":12,"min_host_interval_ms":1000,"global_concurrency":4,"request_timeout_seconds":20,"rights":{"allowed_fields":["url_metadata"],"basis":"url_metadata_policy","evidence_uri":"https://commoncrawl.org/terms-of-use","rights_notice":"URL metadata only under the publisher policy"}}`

func TestDecodeConfigAndConstructEvidence(t *testing.T) {
	config, digest, err := DecodeConfig([]byte(validConfig))
	if err != nil {
		t.Fatal(err)
	}
	wantConfigDigest := sha256.Sum256([]byte(validConfig))
	if digest != hex.EncodeToString(wantConfigDigest[:]) {
		t.Fatalf("config digest = %q", digest)
	}
	userAgent, err := config.UserAgent("1.2.3")
	if err != nil || userAgent != "FetchmarkPackEvidence/1.2.3 (+mailto:operator@example.org)" {
		t.Fatalf("user agent = %q, %v", userAgent, err)
	}
	evidence := []byte("reviewed URL metadata policy\n")
	observed := time.Date(2026, 7, 19, 12, 0, 0, 987, time.FixedZone("offset", 2*60*60))
	decision, err := config.RightsDecision(evidence, observed)
	if err != nil {
		t.Fatal(err)
	}
	wantEvidenceDigest := sha256.Sum256(evidence)
	if decision.Outcome != "permitted" || decision.AllowedFields[0] != "url_metadata" ||
		decision.EvidenceSHA256 != hex.EncodeToString(wantEvidenceDigest[:]) ||
		decision.ObservedAt != "2026-07-19T10:00:00Z" || decision.ValidUntil != "2026-07-19T22:00:00Z" {
		t.Fatalf("rights decision = %#v", decision)
	}
}

func TestDecodeConfigRejectsAmbiguousOrUnsafePolicy(t *testing.T) {
	tests := map[string]string{
		"unknown field":      strings.Replace(validConfig, `"version":1`, `"version":1,"unknown":true`, 1),
		"duplicate field":    strings.Replace(validConfig, `"version":1`, `"version":1,"version":1`, 1),
		"bad token":          strings.Replace(validConfig, "FetchmarkPackEvidence", "Fetchmark/Pack", 1),
		"HTTP contact":       strings.Replace(validConfig, "mailto:operator@example.org", "http://example.org/contact", 1),
		"private contact":    strings.Replace(validConfig, "mailto:operator@example.org", "https://127.0.0.1/contact", 1),
		"contact query":      strings.Replace(validConfig, "mailto:operator@example.org", "https://example.org/contact?x=1", 1),
		"too long validity":  strings.Replace(validConfig, `"evidence_validity_hours":12`, `"evidence_validity_hours":25`, 1),
		"too fast host":      strings.Replace(validConfig, `"min_host_interval_ms":1000`, `"min_host_interval_ms":999`, 1),
		"too slow host":      strings.Replace(validConfig, `"min_host_interval_ms":1000`, `"min_host_interval_ms":3600001`, 1),
		"zero concurrency":   strings.Replace(validConfig, `"global_concurrency":4`, `"global_concurrency":0`, 1),
		"high concurrency":   strings.Replace(validConfig, `"global_concurrency":4`, `"global_concurrency":33`, 1),
		"short timeout":      strings.Replace(validConfig, `"request_timeout_seconds":20`, `"request_timeout_seconds":0`, 1),
		"long timeout":       strings.Replace(validConfig, `"request_timeout_seconds":20`, `"request_timeout_seconds":121`, 1),
		"wrong fields":       strings.Replace(validConfig, `["url_metadata"]`, `["url_metadata","body"]`, 1),
		"bad basis":          strings.Replace(validConfig, "url_metadata_policy", "crawl_inclusion", 1),
		"noncanonical URI":   strings.Replace(validConfig, "https://commoncrawl.org/terms-of-use", "https://COMMONCRAWL.org/terms-of-use", 1),
		"private rights URI": strings.Replace(validConfig, "https://commoncrawl.org/terms-of-use", "https://127.0.0.1/terms", 1),
		"whitespace notice":  strings.Replace(validConfig, "URL metadata only under the publisher policy", " URL metadata only under the publisher policy", 1),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeConfig([]byte(raw)); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	if _, _, err := DecodeConfig(nil); err == nil {
		t.Fatal("empty config accepted")
	}
}

func TestUserAgentAndRightsDecisionBounds(t *testing.T) {
	config, _, err := DecodeConfig([]byte(validConfig))
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"", "bad version", strings.Repeat("x", 65)} {
		if _, err := config.UserAgent(version); err == nil {
			t.Fatalf("tool version %q accepted", version)
		}
	}
	if _, err := config.RightsDecision(nil, time.Now()); err == nil {
		t.Fatal("empty rights evidence accepted")
	}
	if _, err := config.RightsDecision([]byte("evidence"), time.Time{}); err == nil {
		t.Fatal("zero observation time accepted")
	}
}
