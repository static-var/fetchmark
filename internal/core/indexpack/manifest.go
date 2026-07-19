// Package indexpack defines Fetchmark's bounded, signed open-index pack
// interchange contract. Packs contain discovery metadata only; page bodies are
// deliberately outside this schema.
package indexpack

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/core/canonicalurl"
)

const (
	// Version is the original enriched-metadata pack contract retained for
	// existing publishers and consumers.
	Version = 1
	// VersionURLMetadata requires URL-only upserts. The local projection already
	// derives host and path terms from canonical URLs, so publishers need not
	// mislabel URL-derived tokens as licensed titles, headings, or anchors.
	VersionURLMetadata = 2

	MaxManifestBytes          = 1 << 20
	MaxManifestShards         = 4_096
	MaxPackRecords            = 10_000_000
	MaxCompressedShardBytes   = 256 << 20
	MaxUncompressedShardBytes = 512 << 20
	MaxShardRecords           = 1_000_000

	// RecommendedInstallRecordLimit is the conservative first-consumer cap.
	// It is intentionally lower than the neutral format's hard limit.
	RecommendedInstallRecordLimit = 10_000
)

const (
	maxPackIDBytes       = 128
	maxPathBytes         = 512
	maxPublisherName     = 256
	maxURIBytes          = 2_048
	maxRightsNoticeBytes = 4_096
	maxGeneratorBytes    = 256
	maxBuildInputs       = 128
	maxInputNameBytes    = 256
	maxLanguages         = 128
)

var (
	ErrInvalidManifest = errors.New("index pack: invalid manifest")
	packIDPattern      = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	languagePattern    = regexp.MustCompile(`^[a-z]{2,8}(?:-[a-z0-9]{1,8})*$`)
	hexDigestPattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type Kind string

const (
	KindSnapshot Kind = "snapshot"
	KindDelta    Kind = "delta"
)

type Manifest struct {
	Version              int       `json:"version"`
	Kind                 Kind      `json:"kind"`
	PackID               string    `json:"pack_id"`
	Revision             uint64    `json:"revision"`
	CreatedAt            string    `json:"created_at"`
	ExpiresAt            string    `json:"expires_at"`
	ParentManifestSHA256 string    `json:"parent_manifest_sha256,omitempty"`
	SigningKeyID         string    `json:"signing_key_id"`
	Publisher            Publisher `json:"publisher"`
	Policy               Policy    `json:"policy"`
	Languages            []string  `json:"languages"`
	RecordCount          uint64    `json:"record_count"`
	Shards               []Shard   `json:"shards"`
	Build                Build     `json:"build"`
}

type Publisher struct {
	Name         string `json:"name"`
	ContactURI   string `json:"contact_uri"`
	TakedownURI  string `json:"takedown_uri"`
	RightsNotice string `json:"rights_notice"`
}

type Build struct {
	Generator        string       `json:"generator"`
	GeneratorVersion string       `json:"generator_version"`
	Analyzer         string       `json:"analyzer"`
	AnalyzerVersion  string       `json:"analyzer_version"`
	PolicySHA256     string       `json:"policy_sha256"`
	ExclusionsSHA256 string       `json:"exclusions_sha256"`
	CandidateSHA256  string       `json:"candidate_sha256,omitempty"`
	Inputs           []BuildInput `json:"inputs"`
}

type Policy struct {
	Robots           string `json:"robots"`
	NoIndex          string `json:"noindex"`
	BodyDistribution string `json:"body_distribution"`
}

type BuildInput struct {
	Name         string `json:"name"`
	URI          string `json:"uri"`
	RetrievedAt  string `json:"retrieved_at"`
	SHA256       string `json:"sha256"`
	RightsNotice string `json:"rights_notice"`
}

type Shard struct {
	Path                  string `json:"path"`
	Compression           string `json:"compression"`
	SHA256                string `json:"sha256"`
	CompressedSizeBytes   uint64 `json:"compressed_size_bytes"`
	UncompressedSHA256    string `json:"uncompressed_sha256"`
	UncompressedSizeBytes uint64 `json:"uncompressed_size_bytes"`
	RecordCount           uint64 `json:"record_count"`
}

type VerifiedManifest struct {
	Manifest Manifest
	Digest   string
	KeyID    string
}

func EncodeManifest(manifest Manifest) ([]byte, error) {
	if err := manifest.validate(); err != nil {
		return nil, invalidManifest(err)
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, invalidManifest(err)
	}
	if len(raw) > MaxManifestBytes {
		return nil, invalidManifest(fmt.Errorf("encoded manifest exceeds %d bytes", MaxManifestBytes))
	}
	return raw, nil
}

func DecodeManifest(raw []byte) (Manifest, error) {
	if len(raw) == 0 || len(raw) > MaxManifestBytes {
		return Manifest{}, invalidManifest(fmt.Errorf("manifest size must be 1..%d bytes", MaxManifestBytes))
	}
	var manifest Manifest
	if err := DecodeStrictJSON(raw, &manifest); err != nil {
		return Manifest{}, invalidManifest(err)
	}
	if err := validateManifestVersionedFields(raw, manifest.Version); err != nil {
		return Manifest{}, invalidManifest(err)
	}
	if err := manifest.validate(); err != nil {
		return Manifest{}, invalidManifest(err)
	}
	return manifest, nil
}

func validateManifestVersionedFields(raw []byte, version int) error {
	var envelope struct {
		Build map[string]json.RawMessage `json:"build"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return err
	}
	_, candidatePresent := envelope.Build["candidate_sha256"]
	switch version {
	case Version:
		if candidatePresent {
			return errors.New("candidate_sha256 is not supported by manifest version 1")
		}
	case VersionURLMetadata:
		if !candidatePresent {
			return errors.New("candidate_sha256 is required by manifest version 2")
		}
	}
	return nil
}

func Digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func ManifestDigest(manifestBytes []byte) string {
	return Digest(manifestBytes)
}

func ShardDigest(shardBytes []byte) string {
	return Digest(shardBytes)
}

func ShardPath(digest string) string {
	return "shards/sha256/" + digest + ".ndjson.zst"
}

func (manifest Manifest) validate() error {
	if manifest.Version != Version && manifest.Version != VersionURLMetadata {
		return fmt.Errorf("version must be %d or %d", Version, VersionURLMetadata)
	}
	if manifest.Kind != KindSnapshot && manifest.Kind != KindDelta {
		return errors.New("kind must be snapshot or delta")
	}
	if len(manifest.PackID) > maxPackIDBytes || !packIDPattern.MatchString(manifest.PackID) {
		return errors.New("pack_id is invalid")
	}
	if err := validateTimestamp(manifest.CreatedAt); err != nil {
		return fmt.Errorf("created_at: %w", err)
	}
	if manifest.Revision < 1 {
		return errors.New("revision must be positive")
	}
	if err := validateTimestamp(manifest.ExpiresAt); err != nil {
		return fmt.Errorf("expires_at: %w", err)
	}
	created, _ := time.Parse(time.RFC3339, manifest.CreatedAt)
	expires, _ := time.Parse(time.RFC3339, manifest.ExpiresAt)
	if !expires.After(created) || expires.Sub(created) > 365*24*time.Hour {
		return errors.New("expires_at must be after created_at and no more than 365 days later")
	}
	if err := validateDigest(manifest.SigningKeyID); err != nil {
		return fmt.Errorf("signing_key_id: %w", err)
	}
	switch manifest.Kind {
	case KindSnapshot:
		if manifest.ParentManifestSHA256 != "" {
			return errors.New("snapshot must not declare parent_manifest_sha256")
		}
	case KindDelta:
		if manifest.Revision < 2 {
			return errors.New("delta revision must be at least 2")
		}
		if err := validateDigest(manifest.ParentManifestSHA256); err != nil {
			return fmt.Errorf("parent_manifest_sha256: %w", err)
		}
	}
	if err := manifest.Publisher.validate(); err != nil {
		return fmt.Errorf("publisher: %w", err)
	}
	if err := manifest.Policy.validate(); err != nil {
		return fmt.Errorf("policy: %w", err)
	}
	if len(manifest.Languages) < 1 || len(manifest.Languages) > maxLanguages {
		return fmt.Errorf("languages must contain 1..%d entries", maxLanguages)
	}
	previousLanguage := ""
	for index, language := range manifest.Languages {
		if !languagePattern.MatchString(language) {
			return fmt.Errorf("languages[%d] is invalid", index)
		}
		if language <= previousLanguage {
			return errors.New("languages must be sorted and unique")
		}
		previousLanguage = language
	}
	if manifest.RecordCount < 1 || manifest.RecordCount > MaxPackRecords {
		return fmt.Errorf("record_count must be 1..%d", MaxPackRecords)
	}
	if len(manifest.Shards) < 1 || len(manifest.Shards) > MaxManifestShards {
		return fmt.Errorf("shards must contain 1..%d entries", MaxManifestShards)
	}
	var recordCount uint64
	paths := make(map[string]struct{}, len(manifest.Shards))
	digests := make(map[string]struct{}, len(manifest.Shards))
	previousPath := ""
	for index, shard := range manifest.Shards {
		if err := shard.validate(); err != nil {
			return fmt.Errorf("shards[%d]: %w", index, err)
		}
		if shard.Path <= previousPath {
			return errors.New("shards must be sorted by path and unique")
		}
		previousPath = shard.Path
		if _, duplicate := paths[shard.Path]; duplicate {
			return fmt.Errorf("duplicate shard path %q", shard.Path)
		}
		paths[shard.Path] = struct{}{}
		if _, duplicate := digests[shard.SHA256]; duplicate {
			return fmt.Errorf("duplicate shard digest %q", shard.SHA256)
		}
		digests[shard.SHA256] = struct{}{}
		if recordCount > MaxPackRecords-shard.RecordCount {
			return errors.New("shard record counts overflow pack bound")
		}
		recordCount += shard.RecordCount
	}
	if recordCount != manifest.RecordCount {
		return fmt.Errorf("record_count is %d but shards declare %d", manifest.RecordCount, recordCount)
	}
	if err := manifest.Build.validate(manifest.CreatedAt, manifest.Version); err != nil {
		return fmt.Errorf("build: %w", err)
	}
	return nil
}

func (publisher Publisher) validate() error {
	if err := boundedRequired("name", publisher.Name, maxPublisherName); err != nil {
		return err
	}
	if err := validateContactURI(publisher.ContactURI); err != nil {
		return fmt.Errorf("contact_uri: %w", err)
	}
	if err := validateCanonicalHTTPURL(publisher.TakedownURI); err != nil {
		return fmt.Errorf("takedown_uri: %w", err)
	}
	return boundedRequired("rights_notice", publisher.RightsNotice, maxRightsNoticeBytes)
}

func (policy Policy) validate() error {
	if policy.Robots != "rfc9309" {
		return errors.New(`robots must be "rfc9309"`)
	}
	if policy.NoIndex != "exclude" {
		return errors.New(`noindex must be "exclude"`)
	}
	if policy.BodyDistribution != "forbidden" {
		return errors.New(`body_distribution must be "forbidden"`)
	}
	return nil
}

func (build Build) validate(createdAt string, manifestVersion int) error {
	if err := boundedRequired("generator", build.Generator, maxGeneratorBytes); err != nil {
		return err
	}
	if err := boundedRequired("generator_version", build.GeneratorVersion, maxGeneratorBytes); err != nil {
		return err
	}
	if err := boundedRequired("analyzer", build.Analyzer, maxGeneratorBytes); err != nil {
		return err
	}
	if err := boundedRequired("analyzer_version", build.AnalyzerVersion, maxGeneratorBytes); err != nil {
		return err
	}
	if err := validateDigest(build.PolicySHA256); err != nil {
		return fmt.Errorf("policy_sha256: %w", err)
	}
	if err := validateDigest(build.ExclusionsSHA256); err != nil {
		return fmt.Errorf("exclusions_sha256: %w", err)
	}
	switch manifestVersion {
	case Version:
		if build.CandidateSHA256 != "" {
			return errors.New("candidate_sha256 is not supported by manifest version 1")
		}
	case VersionURLMetadata:
		if err := validateDigest(build.CandidateSHA256); err != nil {
			return fmt.Errorf("candidate_sha256: %w", err)
		}
	default:
		return errors.New("manifest version is invalid")
	}
	if len(build.Inputs) < 1 || len(build.Inputs) > maxBuildInputs {
		return fmt.Errorf("inputs must contain 1..%d entries", maxBuildInputs)
	}
	names := make(map[string]struct{}, len(build.Inputs))
	previousName := ""
	for index, input := range build.Inputs {
		if err := input.validate(createdAt); err != nil {
			return fmt.Errorf("inputs[%d]: %w", index, err)
		}
		if _, duplicate := names[input.Name]; duplicate {
			return fmt.Errorf("duplicate input name %q", input.Name)
		}
		if input.Name <= previousName {
			return errors.New("inputs must be sorted by name and unique")
		}
		names[input.Name] = struct{}{}
		previousName = input.Name
	}
	return nil
}

func (input BuildInput) validate(createdAt string) error {
	if err := boundedRequired("name", input.Name, maxInputNameBytes); err != nil {
		return err
	}
	if err := validateCanonicalHTTPURL(input.URI); err != nil {
		return fmt.Errorf("uri: %w", err)
	}
	if err := validateTimestamp(input.RetrievedAt); err != nil {
		return fmt.Errorf("retrieved_at: %w", err)
	}
	retrieved, _ := time.Parse(time.RFC3339, input.RetrievedAt)
	created, _ := time.Parse(time.RFC3339, createdAt)
	if retrieved.After(created) {
		return errors.New("retrieved_at must not be after manifest created_at")
	}
	if err := validateDigest(input.SHA256); err != nil {
		return fmt.Errorf("sha256: %w", err)
	}
	return boundedRequired("rights_notice", input.RightsNotice, maxRightsNoticeBytes)
}

func (shard Shard) validate() error {
	if shard.Compression != "zstd" {
		return errors.New(`compression must be "zstd"`)
	}
	if err := validateDigest(shard.SHA256); err != nil {
		return fmt.Errorf("sha256: %w", err)
	}
	if len(shard.Path) == 0 || len(shard.Path) > maxPathBytes || !utf8.ValidString(shard.Path) {
		return fmt.Errorf("path must be 1..%d UTF-8 bytes", maxPathBytes)
	}
	if path.Clean(shard.Path) != shard.Path || path.IsAbs(shard.Path) || strings.Contains(shard.Path, "\\") {
		return errors.New("path must be a clean relative slash path")
	}
	if shard.Path != ShardPath(shard.SHA256) {
		return errors.New("path must be content-addressed from sha256")
	}
	if shard.CompressedSizeBytes < 1 || shard.CompressedSizeBytes > MaxCompressedShardBytes {
		return fmt.Errorf("compressed_size_bytes must be 1..%d", MaxCompressedShardBytes)
	}
	if err := validateDigest(shard.UncompressedSHA256); err != nil {
		return fmt.Errorf("uncompressed_sha256: %w", err)
	}
	if shard.UncompressedSizeBytes < 1 || shard.UncompressedSizeBytes > MaxUncompressedShardBytes {
		return fmt.Errorf("uncompressed_size_bytes must be 1..%d", MaxUncompressedShardBytes)
	}
	if shard.RecordCount < 1 || shard.RecordCount > MaxShardRecords {
		return fmt.Errorf("record_count must be 1..%d", MaxShardRecords)
	}
	return nil
}

func validateDigest(value string) error {
	if !hexDigestPattern.MatchString(value) {
		return errors.New("must be 64 lowercase hexadecimal characters")
	}
	return nil
}

func validateTimestamp(value string) error {
	if value == "" || len(value) > len("2006-01-02T15:04:05Z") {
		return errors.New("must be second-precision RFC 3339 UTC")
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil || parsed.Format(time.RFC3339) != value || parsed.Location() != time.UTC {
		return errors.New("must be second-precision RFC 3339 UTC")
	}
	if parsed.Year() < 1970 || parsed.Year() > 2200 {
		return errors.New("year must be 1970..2200")
	}
	return nil
}

func validateCanonicalHTTPURL(value string) error {
	if len(value) == 0 || len(value) > maxURIBytes || !utf8.ValidString(value) {
		return fmt.Errorf("must be 1..%d UTF-8 bytes", maxURIBytes)
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return errors.New("must be a valid URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("scheme must be http or https")
	}
	if parsed.User != nil || parsed.Hostname() == "" {
		return errors.New("userinfo is forbidden and host is required")
	}
	canonical, err := canonicalurl.V1(value)
	if err != nil || canonical != value {
		return errors.New("must be a canonical HTTP(S) URL")
	}
	return nil
}

func validateContactURI(value string) error {
	if len(value) == 0 || len(value) > maxURIBytes || !utf8.ValidString(value) {
		return fmt.Errorf("must be 1..%d UTF-8 bytes", maxURIBytes)
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return errors.New("must be a valid URI")
	}
	if parsed.Scheme == "mailto" {
		if parsed.Opaque == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
			return errors.New("mailto URI must contain one address without query or fragment")
		}
		address, err := mail.ParseAddress(parsed.Opaque)
		if err != nil || address.Address != parsed.Opaque {
			return errors.New("mailto URI must contain one bare address")
		}
		return nil
	}
	return validateCanonicalHTTPURL(value)
}

func boundedRequired(name, value string, maximum int) error {
	if strings.TrimSpace(value) == "" || len(value) > maximum || !utf8.ValidString(value) {
		return fmt.Errorf("%s must be non-blank UTF-8 of at most %d bytes", name, maximum)
	}
	return nil
}

func invalidManifest(err error) error {
	return fmt.Errorf("%w: %v", ErrInvalidManifest, err)
}
