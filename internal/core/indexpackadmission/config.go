// Package indexpackadmission defines the strict operator policy shared by the
// separate open-pack evidence collector. It performs no network access and
// never infers rights from crawl inclusion or page content.
package indexpackadmission

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/mail"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/core/canonicalurl"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

const (
	ConfigVersion          = 1
	MaxConfigBytes         = 1 << 20
	MaxRightsEvidenceBytes = 4 << 20
	MaxCollectorRecords    = 10_000
	MaxValidityHours       = 24
	MinHostIntervalMS      = 1_000
	MaxHostIntervalMS      = 3_600_000
	MaxGlobalConcurrency   = 32
	MinRequestTimeoutSecs  = 1
	MaxRequestTimeoutSecs  = 120
	NoIndexParserVersion   = "fetchmark-noindex-v2"
)

var (
	productTokenPattern = regexp.MustCompile(`^[A-Za-z_-]{1,64}$`)
	toolVersionPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
)

type Config struct {
	Version               int          `json:"version"`
	ProductToken          string       `json:"product_token"`
	ContactURI            string       `json:"contact_uri"`
	EvidenceValidityHours uint64       `json:"evidence_validity_hours"`
	MinHostIntervalMillis uint64       `json:"min_host_interval_ms"`
	GlobalConcurrency     uint64       `json:"global_concurrency"`
	RequestTimeoutSeconds uint64       `json:"request_timeout_seconds"`
	Rights                RightsPolicy `json:"rights"`
}

type RightsPolicy struct {
	AllowedFields []string `json:"allowed_fields"`
	Basis         string   `json:"basis"`
	EvidenceURI   string   `json:"evidence_uri"`
	RightsNotice  string   `json:"rights_notice"`
}

// DecodeConfig strictly decodes the exact collector policy and returns the
// SHA-256 that a future evidence report must bind.
func DecodeConfig(raw []byte) (Config, string, error) {
	if len(raw) == 0 || len(raw) > MaxConfigBytes {
		return Config{}, "", fmt.Errorf("index pack admission config: size must be 1..%d bytes", MaxConfigBytes)
	}
	var config Config
	if err := indexpack.DecodeStrictJSON(raw, &config); err != nil {
		return Config{}, "", fmt.Errorf("index pack admission config: %w", err)
	}
	if err := config.validate(); err != nil {
		return Config{}, "", fmt.Errorf("index pack admission config: %w", err)
	}
	digest := sha256.Sum256(raw)
	return config, hex.EncodeToString(digest[:]), nil
}

func (config Config) validate() error {
	if config.Version != ConfigVersion {
		return fmt.Errorf("version must be %d", ConfigVersion)
	}
	if !productTokenPattern.MatchString(config.ProductToken) {
		return errors.New("product_token must be an RFC 9309 product token of at most 64 bytes")
	}
	if err := validateContactURI(config.ContactURI); err != nil {
		return err
	}
	if config.EvidenceValidityHours == 0 || config.EvidenceValidityHours > MaxValidityHours {
		return fmt.Errorf("evidence_validity_hours must be 1..%d", MaxValidityHours)
	}
	if config.MinHostIntervalMillis < MinHostIntervalMS || config.MinHostIntervalMillis > MaxHostIntervalMS {
		return fmt.Errorf("min_host_interval_ms must be %d..%d", MinHostIntervalMS, MaxHostIntervalMS)
	}
	if config.GlobalConcurrency == 0 || config.GlobalConcurrency > MaxGlobalConcurrency {
		return fmt.Errorf("global_concurrency must be 1..%d", MaxGlobalConcurrency)
	}
	if config.RequestTimeoutSeconds < MinRequestTimeoutSecs || config.RequestTimeoutSeconds > MaxRequestTimeoutSecs {
		return fmt.Errorf("request_timeout_seconds must be %d..%d", MinRequestTimeoutSecs, MaxRequestTimeoutSecs)
	}
	if len(config.Rights.AllowedFields) != 1 || config.Rights.AllowedFields[0] != "url_metadata" {
		return errors.New(`rights.allowed_fields must be exactly ["url_metadata"]`)
	}
	switch config.Rights.Basis {
	case "explicit_license", "publisher_permission", "public_domain", "url_metadata_policy":
	default:
		return errors.New("rights.basis is unsupported")
	}
	canonical, err := canonicalurl.V1(config.Rights.EvidenceURI)
	if err != nil || canonical != config.Rights.EvidenceURI || !isPackPublicURL(canonical) {
		return errors.New("rights.evidence_uri must be a canonical public HTTP(S) URL")
	}
	notice := config.Rights.RightsNotice
	if strings.TrimSpace(notice) != notice || notice == "" || len(notice) > 2_048 || strings.ContainsAny(notice, "\x00\r\n") {
		return errors.New("rights.rights_notice must be 1..2048 bytes without surrounding whitespace or control data")
	}
	return nil
}

// UserAgent constructs the stable contact-bearing HTTP identification string
// whose leading product token is used for RFC 9309 group matching.
func (config Config) UserAgent(toolVersion string) (string, error) {
	if err := config.validate(); err != nil {
		return "", err
	}
	if !toolVersionPattern.MatchString(toolVersion) {
		return "", errors.New("tool version is invalid")
	}
	value := config.ProductToken + "/" + toolVersion + " (+" + config.ContactURI + ")"
	if len(value) > 256 {
		return "", errors.New("constructed user agent exceeds 256 bytes")
	}
	return value, nil
}

// RightsDecision binds the exact local operator evidence bytes to a bounded
// current assertion. It does not interpret those bytes or make a legal claim.
func (config Config) RightsDecision(evidence []byte, observedAt time.Time) (indexpackselection.RightsDecision, error) {
	if err := config.validate(); err != nil {
		return indexpackselection.RightsDecision{}, err
	}
	if len(evidence) == 0 || len(evidence) > MaxRightsEvidenceBytes {
		return indexpackselection.RightsDecision{}, fmt.Errorf("rights evidence size must be 1..%d bytes", MaxRightsEvidenceBytes)
	}
	if observedAt.IsZero() {
		return indexpackselection.RightsDecision{}, errors.New("rights observation time is required")
	}
	observedAt = observedAt.UTC().Truncate(time.Second)
	digest := sha256.Sum256(evidence)
	return indexpackselection.RightsDecision{
		Outcome: "permitted", AllowedFields: []string{"url_metadata"}, Basis: config.Rights.Basis,
		EvidenceURI: config.Rights.EvidenceURI, EvidenceSHA256: hex.EncodeToString(digest[:]),
		ObservedAt:   observedAt.Format(time.RFC3339),
		ValidUntil:   observedAt.Add(time.Duration(config.EvidenceValidityHours) * time.Hour).Format(time.RFC3339),
		RightsNotice: config.Rights.RightsNotice,
	}, nil
}

func validateContactURI(raw string) error {
	if strings.TrimSpace(raw) != raw || raw == "" || len(raw) > 512 || strings.ContainsAny(raw, "\x00\r\n") {
		return errors.New("contact_uri is invalid")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return errors.New("contact_uri must be a mailto URI or canonical HTTPS URL without query or fragment")
	}
	switch parsed.Scheme {
	case "mailto":
		if parsed.Opaque == "" || parsed.Host != "" || parsed.User != nil {
			return errors.New("contact_uri mailto address is invalid")
		}
		address, err := mail.ParseAddress(parsed.Opaque)
		if err != nil || address.Address != parsed.Opaque {
			return errors.New("contact_uri mailto address is invalid")
		}
	case "https":
		canonical, err := canonicalurl.V1(raw)
		if err != nil || canonical != raw || !isPackPublicURL(canonical) {
			return errors.New("contact_uri HTTPS URL must be canonical")
		}
	default:
		return errors.New("contact_uri must use mailto or HTTPS")
	}
	return nil
}
