// Package indexpackselection turns an exact Common Crawl URL-index export
// plus explicit live indexing-admission evidence into bounded, metadata-only
// open-index-pack records. It never fetches WARC data or copies page content.
package indexpackselection

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/core/canonicalurl"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

const (
	SpecVersion                      = 2
	ReportVersion                    = 2
	ExclusionsVersion                = 1
	AdmissionVersion                 = 1
	MaxSpecBytes                     = 1 << 20
	MaxExclusionsBytes               = 4 << 20
	MaxCandidateBytes                = 32 << 10
	MaxCandidates             uint64 = 50_000_000
	MaxPermissionAgeHours            = 24
	MaxBuildTimestampSkew            = 15 * time.Minute
	MinimumCompletionValidity        = time.Minute
	maxWARCRecordBytes        uint64 = 64 << 20
)

const (
	ProfileLightweight     = "lightweight-v1"
	ProfileFormatPrototype = "format-prototype-v1"
)

var (
	ErrInvalidSpec       = errors.New("index pack selection: invalid spec")
	ErrInvalidExclusions = errors.New("index pack selection: invalid exclusions")
	ErrInvalidCandidate  = errors.New("index pack selection: invalid candidate")
	packIDPattern        = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	languagePattern      = regexp.MustCompile(`^[a-z]{2,8}(?:-[a-z0-9]{1,8})*$`)
	sha256Pattern        = regexp.MustCompile(`^[0-9a-f]{64}$`)
	sha1Base32Pattern    = regexp.MustCompile(`^(?:sha1:)?[A-Z2-7]{32}$`)
)

// Spec is the exact, hash-bound selection policy used for a build. Inputs
// identify the immutable Common Crawl source artifacts, while CandidateSHA256
// binds the separate admission-enriched JSONL stream consumed by Select.
type Spec struct {
	Version               int                    `json:"version"`
	Profile               string                 `json:"profile"`
	PackID                string                 `json:"pack_id"`
	Revision              uint64                 `json:"revision"`
	CreatedAt             string                 `json:"created_at"`
	ExpiresAt             string                 `json:"expires_at"`
	Publisher             indexpack.Publisher    `json:"publisher"`
	Languages             []string               `json:"languages"`
	MaxRecords            uint64                 `json:"max_records"`
	MaxRecordsPerHost     uint64                 `json:"max_records_per_host"`
	MaxPermissionAgeHours uint64                 `json:"max_permission_age_hours"`
	RobotsUserAgent       string                 `json:"robots_user_agent"`
	CandidateSHA256       string                 `json:"candidate_sha256"`
	Inputs                []indexpack.BuildInput `json:"inputs"`
}

// Exclusions is an exact, signed-by-digest opt-out/takedown input. HostSuffixes
// exclude both the named host and every subdomain.
type Exclusions struct {
	Version      int      `json:"version"`
	URLs         []string `json:"urls"`
	HostSuffixes []string `json:"host_suffixes"`
}

// Candidate mirrors the documented Common Crawl CDXJ fields. Admission is
// publisher-produced evidence from a separate live RFC 9309/noindex pass.
// Unknown fields fail closed so the export contract cannot drift silently.
type Candidate struct {
	URLKey       string    `json:"urlkey,omitempty"`
	Timestamp    string    `json:"timestamp"`
	URL          string    `json:"url"`
	MIME         string    `json:"mime"`
	MIMEDetected string    `json:"mime-detected"`
	Status       string    `json:"status"`
	Digest       string    `json:"digest"`
	Length       string    `json:"length"`
	Offset       string    `json:"offset"`
	Filename     string    `json:"filename"`
	Languages    string    `json:"languages"`
	Encoding     string    `json:"encoding,omitempty"`
	Admission    Admission `json:"fetchmark_admission"`
}

// CandidatePreflight is the capture metadata shared by live evidence
// collection and final selection. It deliberately excludes admission policy,
// which can only be evaluated after the live evidence pass.
type CandidatePreflight struct {
	CanonicalURL string
	Language     string
	CapturedAt   time.Time
	Length       uint64
}

type Admission struct {
	Version  int                 `json:"version"`
	Robots   RobotsObservation   `json:"robots"`
	Indexing IndexingObservation `json:"indexing"`
	Rights   RightsDecision      `json:"rights"`
}

type RobotsObservation struct {
	UserAgent  string `json:"user_agent"`
	RobotsURI  string `json:"robots_uri"`
	CheckedAt  string `json:"checked_at"`
	ValidUntil string `json:"valid_until"`
	Outcome    string `json:"outcome"`
	BodySHA256 string `json:"body_sha256"`
}

type IndexingObservation struct {
	CheckedAt            string `json:"checked_at"`
	ValidUntil           string `json:"valid_until"`
	FinalURL             string `json:"final_url"`
	Outcome              string `json:"outcome"`
	HeadersSHA256        string `json:"headers_sha256"`
	RepresentationSHA256 string `json:"representation_sha256"`
	ParserVersion        string `json:"parser_version"`
}

type RightsDecision struct {
	Outcome        string   `json:"outcome"`
	AllowedFields  []string `json:"allowed_fields"`
	Basis          string   `json:"basis"`
	EvidenceURI    string   `json:"evidence_uri"`
	EvidenceSHA256 string   `json:"evidence_sha256"`
	ObservedAt     string   `json:"observed_at"`
	ValidUntil     string   `json:"valid_until"`
	RightsNotice   string   `json:"rights_notice"`
}

type Report struct {
	Version              int               `json:"version"`
	PolicySHA256         string            `json:"policy_sha256"`
	ExclusionsSHA256     string            `json:"exclusions_sha256"`
	CandidateSHA256      string            `json:"candidate_sha256"`
	Candidates           uint64            `json:"candidates"`
	Accepted             uint64            `json:"accepted"`
	UniqueHosts          uint64            `json:"unique_hosts"`
	Rejected             uint64            `json:"rejected"`
	RejectedByReason     map[string]uint64 `json:"rejected_by_reason"`
	AcceptedByLanguage   map[string]uint64 `json:"accepted_by_language"`
	PermissionOldestAge  uint64            `json:"permission_oldest_age_hours"`
	PermissionValidUntil string            `json:"permission_valid_until"`
}

// DecodeSpec strictly decodes and validates a build specification and returns
// the digest of its exact bytes for Manifest.Build.PolicySHA256.
func DecodeSpec(raw []byte) (Spec, string, error) {
	if len(raw) == 0 || len(raw) > MaxSpecBytes {
		return Spec{}, "", invalid(ErrInvalidSpec, fmt.Errorf("size must be 1..%d bytes", MaxSpecBytes))
	}
	var spec Spec
	if err := decodeStrict(raw, &spec); err != nil {
		return Spec{}, "", invalid(ErrInvalidSpec, err)
	}
	if err := spec.validate(); err != nil {
		return Spec{}, "", invalid(ErrInvalidSpec, err)
	}
	return spec, digest(raw), nil
}

// DecodeExclusions strictly decodes canonical, sorted exclusion inputs and
// returns the digest of their exact bytes for Manifest.Build.ExclusionsSHA256.
func DecodeExclusions(raw []byte) (Exclusions, string, error) {
	if len(raw) == 0 || len(raw) > MaxExclusionsBytes {
		return Exclusions{}, "", invalid(ErrInvalidExclusions, fmt.Errorf("size must be 1..%d bytes", MaxExclusionsBytes))
	}
	var exclusions Exclusions
	if err := decodeStrict(raw, &exclusions); err != nil {
		return Exclusions{}, "", invalid(ErrInvalidExclusions, err)
	}
	if err := exclusions.validate(); err != nil {
		return Exclusions{}, "", invalid(ErrInvalidExclusions, err)
	}
	return exclusions, digest(raw), nil
}

// Select strictly decodes the exact policy and exclusion bytes, validates every
// candidate and the exact input digest, applies permission, exclusion,
// diversity, and capacity gates, then emits accepted records in input order.
// emit must stage its effects: a late digest mismatch or malformed line makes
// the complete selection invalid.
func Select(
	ctx context.Context,
	buildTime time.Time,
	specRaw []byte,
	exclusionsRaw []byte,
	input io.Reader,
	emit func(indexpack.Record) error,
) (Report, error) {
	if ctx == nil || buildTime.IsZero() || input == nil || emit == nil {
		return Report{}, errors.New("index pack selection: context, build time, input, and emit are required")
	}
	spec, policySHA256, err := DecodeSpec(specRaw)
	if err != nil {
		return Report{}, err
	}
	exclusions, exclusionsSHA256, err := DecodeExclusions(exclusionsRaw)
	if err != nil {
		return Report{}, err
	}

	report := Report{
		Version: ReportVersion, PolicySHA256: policySHA256, ExclusionsSHA256: exclusionsSHA256,
		RejectedByReason: make(map[string]uint64), AcceptedByLanguage: make(map[string]uint64),
	}
	allowedLanguages := make(map[string]struct{}, len(spec.Languages))
	for _, language := range spec.Languages {
		allowedLanguages[language] = struct{}{}
	}
	excludedURLs := make(map[string]struct{}, len(exclusions.URLs))
	for _, candidateURL := range exclusions.URLs {
		excludedURLs[candidateURL] = struct{}{}
	}
	hostCounts := make(map[string]uint64)
	seenURLs := make(map[string]struct{})
	hasher := sha256.New()
	var permissionValidUntil time.Time
	scanner := bufio.NewScanner(io.TeeReader(input, hasher))
	scanner.Buffer(make([]byte, 64<<10), MaxCandidateBytes+1)
	createdAt, _ := time.Parse(time.RFC3339, spec.CreatedAt)
	buildTime = buildTime.UTC()
	if delta := buildTime.Sub(createdAt); delta < -MaxBuildTimestampSkew || delta > MaxBuildTimestampSkew {
		return Report{}, invalid(ErrInvalidSpec, fmt.Errorf("created_at must be within %s of build execution", MaxBuildTimestampSkew))
	}
	evidenceTime := buildTime
	if createdAt.After(evidenceTime) {
		evidenceTime = createdAt
	}

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		report.Candidates++
		if report.Candidates > MaxCandidates {
			return Report{}, invalid(ErrInvalidCandidate, fmt.Errorf("candidate count exceeds %d", MaxCandidates))
		}
		line := scanner.Bytes()
		if len(line) == 0 || len(line) > MaxCandidateBytes {
			return Report{}, invalid(ErrInvalidCandidate, fmt.Errorf("candidate %d has invalid line size", report.Candidates))
		}
		var candidate Candidate
		if err := decodeStrict(line, &candidate); err != nil {
			return Report{}, invalid(ErrInvalidCandidate, fmt.Errorf("candidate %d: %w", report.Candidates, err))
		}
		selected, reason, checkedAge, err := candidate.record(spec, createdAt, buildTime, evidenceTime, allowedLanguages)
		if err != nil {
			return Report{}, invalid(ErrInvalidCandidate, fmt.Errorf("candidate %d: %w", report.Candidates, err))
		}
		if checkedAge > report.PermissionOldestAge {
			report.PermissionOldestAge = checkedAge
		}
		if reason == "" {
			host := hostOf(selected.URL)
			switch {
			case excludedURL(excludedURLs, selected.URL):
				reason = "excluded_url"
			case excludedHost(exclusions.HostSuffixes, host):
				reason = "excluded_host"
			case alreadySeen(seenURLs, selected.URL):
				reason = "duplicate_url"
			case hostCounts[host] >= spec.MaxRecordsPerHost:
				reason = "host_limit"
			case report.Accepted >= spec.MaxRecords:
				reason = "pack_capacity"
			}
		}
		if reason != "" {
			report.Rejected++
			report.RejectedByReason[reason]++
			continue
		}
		seenURLs[selected.URL] = struct{}{}
		host := hostOf(selected.URL)
		hostCounts[host]++
		validUntil := admissionValidUntil(candidate.Admission)
		if permissionValidUntil.IsZero() || validUntil.Before(permissionValidUntil) {
			permissionValidUntil = validUntil
			report.PermissionValidUntil = validUntil.UTC().Format(time.RFC3339)
		}
		if err := emit(selected); err != nil {
			return Report{}, fmt.Errorf("index pack selection: emit candidate %d: %w", report.Candidates, err)
		}
		report.Accepted++
		report.AcceptedByLanguage[selected.Language]++
	}
	if err := scanner.Err(); err != nil {
		return Report{}, invalid(ErrInvalidCandidate, fmt.Errorf("scan: %w", err))
	}
	report.CandidateSHA256 = hex.EncodeToString(hasher.Sum(nil))
	if report.CandidateSHA256 != spec.CandidateSHA256 {
		return Report{}, invalid(ErrInvalidCandidate, errors.New("candidate input sha256 does not match spec"))
	}
	if report.Accepted == 0 {
		return Report{}, invalid(ErrInvalidCandidate, errors.New("selection accepted no records"))
	}
	report.UniqueHosts = uint64(len(hostCounts))
	return report, nil
}

// ValidateCompletion prevents a publisher from activating a pack after any
// admitted robots, indexing, or rights decision has expired. Historical
// signature verification deliberately does not call this wall-clock gate.
func ValidateCompletion(report Report, completedAt time.Time) error {
	if completedAt.IsZero() || report.PermissionValidUntil == "" {
		return errors.New("index pack selection: completion time and permission validity are required")
	}
	validUntil, err := time.Parse(time.RFC3339, report.PermissionValidUntil)
	if err != nil || validUntil.UTC().Format(time.RFC3339) != report.PermissionValidUntil || !validUntil.After(completedAt.UTC().Add(MinimumCompletionValidity)) {
		return errors.New("index pack selection: admitted evidence expired before build completion")
	}
	return nil
}

func (spec Spec) validate() error {
	if spec.Version != SpecVersion {
		return fmt.Errorf("version must be %d", SpecVersion)
	}
	switch spec.Profile {
	case ProfileLightweight:
		if spec.MaxRecords > indexpack.RecommendedInstallRecordLimit {
			return fmt.Errorf("%s max_records must not exceed %d", ProfileLightweight, indexpack.RecommendedInstallRecordLimit)
		}
	case ProfileFormatPrototype:
		if spec.MaxRecords > 5_000_000 {
			return errors.New("format-prototype-v1 max_records must not exceed 5000000")
		}
	default:
		return errors.New("profile is invalid")
	}
	if !packIDPattern.MatchString(spec.PackID) || spec.Revision == 0 {
		return errors.New("pack_id or revision is invalid")
	}
	created, err := time.Parse(time.RFC3339, spec.CreatedAt)
	if err != nil || created.UTC().Format(time.RFC3339) != spec.CreatedAt {
		return errors.New("created_at must be canonical RFC3339 UTC")
	}
	expires, err := time.Parse(time.RFC3339, spec.ExpiresAt)
	if err != nil || expires.UTC().Format(time.RFC3339) != spec.ExpiresAt || !expires.After(created) || expires.Sub(created) > 365*24*time.Hour {
		return errors.New("expires_at must be canonical, after created_at, and within 365 days")
	}
	if spec.MaxRecords == 0 || spec.MaxRecords > indexpack.MaxPackRecords || spec.MaxRecordsPerHost == 0 || spec.MaxRecordsPerHost > spec.MaxRecords {
		return errors.New("record and per-host limits are invalid")
	}
	if spec.MaxPermissionAgeHours == 0 || spec.MaxPermissionAgeHours > MaxPermissionAgeHours {
		return fmt.Errorf("max_permission_age_hours must be 1..%d", MaxPermissionAgeHours)
	}
	if strings.TrimSpace(spec.RobotsUserAgent) != spec.RobotsUserAgent || spec.RobotsUserAgent == "" || len(spec.RobotsUserAgent) > 256 || strings.ContainsAny(spec.RobotsUserAgent, "\x00\r\n") {
		return errors.New("robots_user_agent is invalid")
	}
	if len(spec.Languages) == 0 || len(spec.Languages) > 128 {
		return errors.New("languages must contain 1..128 entries")
	}
	previous := ""
	for _, language := range spec.Languages {
		if !languagePattern.MatchString(language) || language <= previous {
			return errors.New("languages must be valid, sorted, and unique")
		}
		previous = language
	}
	if !sha256Pattern.MatchString(spec.CandidateSHA256) {
		return errors.New("candidate_sha256 must be a lowercase SHA-256 digest")
	}
	if len(spec.Inputs) != 1 {
		return errors.New("inputs must contain exactly one source artifact")
	}
	if spec.Inputs[0].SHA256 == spec.CandidateSHA256 {
		return errors.New("source input and builder candidate digests must differ")
	}
	validationDigest := strings.Repeat("a", 64)
	if _, err := indexpack.EncodeManifest(indexpack.Manifest{
		Version: indexpack.VersionURLMetadata, Kind: indexpack.KindSnapshot, PackID: spec.PackID, Revision: spec.Revision,
		CreatedAt: spec.CreatedAt, ExpiresAt: spec.ExpiresAt, SigningKeyID: validationDigest,
		Publisher: spec.Publisher,
		Policy:    indexpack.Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"},
		Languages: spec.Languages, RecordCount: 1,
		Shards: []indexpack.Shard{{
			Path: indexpack.ShardPath(validationDigest), Compression: "zstd", SHA256: validationDigest,
			CompressedSizeBytes: 1, UncompressedSHA256: validationDigest, UncompressedSizeBytes: 1, RecordCount: 1,
		}},
		Build: indexpack.Build{
			Generator: "fetchmark-url-metadata", GeneratorVersion: "1", Analyzer: "url-terms", AnalyzerVersion: "1",
			PolicySHA256: validationDigest, ExclusionsSHA256: validationDigest,
			CandidateSHA256: validationDigest, Inputs: spec.Inputs,
		},
	}); err != nil {
		return fmt.Errorf("manifest metadata: %w", err)
	}
	return nil
}

func (exclusions Exclusions) validate() error {
	if exclusions.Version != ExclusionsVersion {
		return fmt.Errorf("version must be %d", ExclusionsVersion)
	}
	if len(exclusions.URLs)+len(exclusions.HostSuffixes) > 100_000 {
		return errors.New("exclusions contain more than 100000 entries")
	}
	previous := ""
	for _, rawURL := range exclusions.URLs {
		canonical, err := canonicalurl.V1(rawURL)
		if err != nil || canonical != rawURL || rawURL <= previous {
			return errors.New("excluded URLs must be canonical, sorted, and unique")
		}
		previous = rawURL
	}
	previous = ""
	for _, host := range exclusions.HostSuffixes {
		if !validPublicHost(host) || host <= previous {
			return errors.New("excluded host suffixes must be public, lowercase, sorted, and unique")
		}
		previous = host
	}
	return nil
}

func (candidate Candidate) record(spec Spec, createdAt, buildTime, requiredUntil time.Time, allowedLanguages map[string]struct{}) (indexpack.Record, string, uint64, error) {
	preflight, reason, err := PreflightCandidate(candidate, createdAt)
	if err != nil || reason != "" {
		return indexpack.Record{}, reason, 0, err
	}
	if _, allowed := allowedLanguages[preflight.Language]; !allowed {
		return indexpack.Record{}, "language_not_allowed", 0, nil
	}
	ageHours, reason, err := candidate.Admission.validate(spec, preflight.CanonicalURL, preflight.CapturedAt, buildTime, requiredUntil)
	if err != nil || reason != "" {
		return indexpack.Record{}, reason, ageHours, err
	}
	freshness := freshnessScore(preflight.CapturedAt, createdAt)
	record := indexpack.Record{
		Operation: indexpack.OperationUpsert, URL: preflight.CanonicalURL,
		Language: preflight.Language, FetchedAt: preflight.CapturedAt.UTC().Format(time.RFC3339), FreshnessScore: freshness,
		Provenance: []indexpack.Provenance{{
			Source: spec.Inputs[0].Name, SourceURI: "https://data.commoncrawl.org/" + candidate.Filename,
			RetrievedAt: preflight.CapturedAt.UTC().Format(time.RFC3339), RightsNotice: candidate.Admission.Rights.RightsNotice,
		}},
	}
	if err := indexpack.ValidateRecord(record, indexpack.VersionURLMetadata, indexpack.KindSnapshot); err != nil {
		return indexpack.Record{}, "", ageHours, fmt.Errorf("selected record is invalid: %w", err)
	}
	return record, "", ageHours, nil
}

// PreflightCandidate applies every capture-side rule that can be evaluated
// before network access. A candidate that passes can still be excluded by a
// build's language, takedown, host-limit, or live-admission policy.
func PreflightCandidate(candidate Candidate, notAfter time.Time) (CandidatePreflight, string, error) {
	if notAfter.IsZero() {
		return CandidatePreflight{}, "", errors.New("preflight cutoff is required")
	}
	if candidate.Status != "200" {
		return CandidatePreflight{}, "status_not_200", nil
	}
	if !htmlMIME(candidate.MIME) || !htmlMIME(candidate.MIMEDetected) {
		return CandidatePreflight{}, "mime_not_html", nil
	}
	if !sha1Base32Pattern.MatchString(candidate.Digest) {
		return CandidatePreflight{}, "", errors.New("digest must be Common Crawl SHA-1 base32")
	}
	length, err := parseBoundedDecimal(candidate.Length, maxWARCRecordBytes)
	if err != nil || length == 0 {
		return CandidatePreflight{}, "", errors.New("length is invalid")
	}
	_, err = parseBoundedDecimal(candidate.Offset, ^uint64(0)-length)
	if err != nil {
		return CandidatePreflight{}, "", errors.New("offset is invalid")
	}
	if !ValidCommonCrawlWARCPath(candidate.Filename) {
		return CandidatePreflight{}, "", errors.New("filename is not a Common Crawl WARC path")
	}
	capturedAt, err := time.Parse("20060102150405", candidate.Timestamp)
	if err != nil || capturedAt.After(notAfter) {
		return CandidatePreflight{}, "capture_after_build", nil
	}
	language := primaryLanguage(candidate.Languages)
	if !languagePattern.MatchString(language) {
		return CandidatePreflight{}, "language_invalid", nil
	}
	rawURL, err := url.Parse(candidate.URL)
	if err != nil {
		return CandidatePreflight{}, "", errors.New("url is invalid")
	}
	if rawURL.RawQuery != "" || rawURL.ForceQuery {
		return CandidatePreflight{}, "query_not_allowed", nil
	}
	canonical, err := canonicalurl.V1(candidate.URL)
	if err != nil {
		return CandidatePreflight{}, "", errors.New("url is invalid")
	}
	parsed, err := url.Parse(canonical)
	if err != nil || parsed.User != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || !validPublicHost(parsed.Hostname()) {
		return CandidatePreflight{}, "non_public_url", nil
	}
	if parsed.RawQuery != "" {
		return CandidatePreflight{}, "query_not_allowed", nil
	}
	return CandidatePreflight{CanonicalURL: canonical, Language: language, CapturedAt: capturedAt, Length: length}, "", nil
}

func (admission Admission) validate(spec Spec, canonical string, capturedAt, buildTime, requiredUntil time.Time) (uint64, string, error) {
	if admission.Version != AdmissionVersion {
		return 0, "permission_not_authoritative", nil
	}
	if admission.Robots.UserAgent != spec.RobotsUserAgent || admission.Robots.Outcome != "allowed" {
		return 0, "robots_not_allowed", nil
	}
	if !sha256Pattern.MatchString(admission.Robots.BodySHA256) {
		return 0, "", errors.New("robots body_sha256 is invalid")
	}
	page, _ := url.Parse(canonical)
	robotsURI, err := url.Parse(admission.Robots.RobotsURI)
	expectedRobotsURI := page.Scheme + "://" + page.Host + "/robots.txt"
	if err != nil || admission.Robots.RobotsURI != expectedRobotsURI || robotsURI.User != nil || robotsURI.Scheme != page.Scheme || robotsURI.Host != page.Host ||
		robotsURI.EscapedPath() != "/robots.txt" || robotsURI.RawQuery != "" || robotsURI.ForceQuery || robotsURI.Fragment != "" {
		return 0, "", errors.New("robots_uri must be the canonical URL origin robots.txt")
	}
	robotsAge, valid := observationWindow(admission.Robots.CheckedAt, admission.Robots.ValidUntil, capturedAt, buildTime, requiredUntil, spec.MaxPermissionAgeHours)
	if !valid {
		return robotsAge, "permission_stale", nil
	}
	if admission.Indexing.Outcome != "indexable" || admission.Indexing.ParserVersion == "" || len(admission.Indexing.ParserVersion) > 128 ||
		!sha256Pattern.MatchString(admission.Indexing.HeadersSHA256) || !sha256Pattern.MatchString(admission.Indexing.RepresentationSHA256) {
		if admission.Indexing.Outcome != "indexable" {
			return robotsAge, "indexing_not_permitted", nil
		}
		return robotsAge, "", errors.New("indexing evidence is invalid")
	}
	indexingURL, err := url.Parse(admission.Indexing.FinalURL)
	if err != nil || indexingURL.RawQuery != "" || indexingURL.ForceQuery {
		return robotsAge, "indexing_redirect_mismatch", nil
	}
	finalURL, err := canonicalurl.V1(admission.Indexing.FinalURL)
	if err != nil || finalURL != admission.Indexing.FinalURL || finalURL != canonical {
		return robotsAge, "indexing_redirect_mismatch", nil
	}
	indexingAge, valid := observationWindow(admission.Indexing.CheckedAt, admission.Indexing.ValidUntil, capturedAt, buildTime, requiredUntil, spec.MaxPermissionAgeHours)
	if !valid {
		return max(robotsAge, indexingAge), "permission_stale", nil
	}
	age := max(robotsAge, indexingAge)
	if admission.Rights.Outcome != "permitted" {
		return age, "rights_not_permitted", nil
	}
	if len(admission.Rights.AllowedFields) != 1 || admission.Rights.AllowedFields[0] != "url_metadata" {
		return age, "rights_fields_not_permitted", nil
	}
	switch admission.Rights.Basis {
	case "explicit_license", "publisher_permission", "public_domain", "url_metadata_policy":
	default:
		return age, "", errors.New("rights basis is invalid")
	}
	if !sha256Pattern.MatchString(admission.Rights.EvidenceSHA256) || strings.TrimSpace(admission.Rights.RightsNotice) == "" || len(admission.Rights.RightsNotice) > 2_048 {
		return age, "", errors.New("rights evidence is invalid")
	}
	evidenceURI, err := canonicalurl.V1(admission.Rights.EvidenceURI)
	if err != nil || evidenceURI != admission.Rights.EvidenceURI {
		return age, "", errors.New("rights evidence_uri must be canonical HTTP(S)")
	}
	rightsAge, valid := observationWindow(admission.Rights.ObservedAt, admission.Rights.ValidUntil, capturedAt, buildTime, requiredUntil, spec.MaxPermissionAgeHours)
	if !valid {
		return max(age, rightsAge), "rights_stale", nil
	}
	return max(age, rightsAge), "", nil
}

func observationWindow(checkedRaw, validRaw string, capturedAt, buildTime, requiredUntil time.Time, maximumAgeHours uint64) (uint64, bool) {
	checkedAt, checkedErr := time.Parse(time.RFC3339, checkedRaw)
	validUntil, validErr := time.Parse(time.RFC3339, validRaw)
	var ageHours uint64
	if checkedErr == nil && !checkedAt.After(buildTime) {
		ageHours = uint64(buildTime.Sub(checkedAt) / time.Hour)
	}
	if checkedErr != nil || validErr != nil || checkedAt.UTC().Format(time.RFC3339) != checkedRaw || validUntil.UTC().Format(time.RFC3339) != validRaw ||
		checkedAt.Before(capturedAt) || checkedAt.After(buildTime) || !validUntil.After(requiredUntil) ||
		!validUntil.After(checkedAt) || validUntil.Sub(checkedAt) > time.Duration(MaxPermissionAgeHours)*time.Hour {
		return ageHours, false
	}
	age := buildTime.Sub(checkedAt)
	return ageHours, age <= time.Duration(maximumAgeHours)*time.Hour
}

func admissionValidUntil(admission Admission) time.Time {
	values := []string{admission.Robots.ValidUntil, admission.Indexing.ValidUntil, admission.Rights.ValidUntil}
	earliest, _ := time.Parse(time.RFC3339, values[0])
	for _, raw := range values[1:] {
		candidate, _ := time.Parse(time.RFC3339, raw)
		if candidate.Before(earliest) {
			earliest = candidate
		}
	}
	return earliest
}

func decodeStrict(raw []byte, target any) error {
	return indexpack.DecodeStrictJSON(raw, target)
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func invalid(kind error, err error) error { return fmt.Errorf("%w: %v", kind, err) }

func htmlMIME(value string) bool {
	mediaType := strings.ToLower(strings.TrimSpace(strings.SplitN(value, ";", 2)[0]))
	return mediaType == "text/html" || mediaType == "application/xhtml+xml"
}

func parseBoundedDecimal(raw string, maximum uint64) (uint64, error) {
	if raw == "" || strings.TrimLeft(raw, "0") != raw && raw != "0" {
		return 0, errors.New("decimal is not canonical")
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value > maximum {
		return 0, errors.New("decimal is outside bound")
	}
	return value, nil
}

// ValidCommonCrawlWARCPath identifies the exact relative WARC artifact path
// shape accepted by both the canonical producer and its independent verifier.
func ValidCommonCrawlWARCPath(value string) bool {
	if !strings.HasPrefix(value, "crawl-data/CC-MAIN-") || !strings.HasSuffix(value, ".warc.gz") ||
		path.Clean(value) != value || path.IsAbs(value) {
		return false
	}
	for _, character := range value {
		if character == '/' || character == '.' || character == '-' || character == '_' ||
			character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' {
			continue
		}
		return false
	}
	return true
}

func primaryLanguage(value string) string {
	value = strings.ToLower(strings.TrimSpace(strings.SplitN(value, ",", 2)[0]))
	return value
}

func validPublicHost(host string) bool {
	if host == "" || host != strings.TrimSpace(host) || host != strings.ToLower(host) || strings.HasSuffix(host, ".") ||
		len(host) > 253 || net.ParseIP(host) != nil || !strings.Contains(host, ".") || strings.ContainsAny(host, " /\\@") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if character != '-' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
				return false
			}
		}
	}
	for _, suffix := range []string{".localhost", ".local", ".internal", ".home", ".lan", ".home.arpa", ".test", ".invalid", ".example", ".onion"} {
		if host == strings.TrimPrefix(suffix, ".") || strings.HasSuffix(host, suffix) {
			return false
		}
	}
	return true
}

func freshnessScore(capturedAt, createdAt time.Time) uint16 {
	age := createdAt.Sub(capturedAt)
	if age <= 0 {
		return 10_000
	}
	const yearHours = 365 * 24
	ageHours := uint64(age / time.Hour)
	if ageHours >= yearHours {
		return 0
	}
	return uint16(10_000 - ageHours*10_000/yearHours)
}

func hostOf(rawURL string) string {
	parsed, _ := url.Parse(rawURL)
	return strings.ToLower(parsed.Hostname())
}

func excludedURL(exclusions map[string]struct{}, rawURL string) bool {
	_, excluded := exclusions[rawURL]
	return excluded
}

func excludedHost(suffixes []string, host string) bool {
	for _, suffix := range suffixes {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return true
		}
	}
	return false
}

func alreadySeen(seen map[string]struct{}, rawURL string) bool {
	_, exists := seen[rawURL]
	return exists
}
