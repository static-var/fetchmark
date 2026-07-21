package indexpack

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxRecordBytes       = 256 << 10
	maxTitleBytes        = 1_024
	maxHeadings          = 64
	maxHeadingBytes      = 512
	maxAnchorTerms       = 128
	maxAnchorTermBytes   = 128
	maxSalientSketch     = 4_096
	maxProvenanceEntries = 16
	maxSourceNameBytes   = 256
	maxRecordRightsBytes = 2_048
	maxScore             = 10_000
)

var ErrInvalidShard = errors.New("index pack: invalid shard")

type Operation string

const (
	OperationUpsert    Operation = "upsert"
	OperationTombstone Operation = "tombstone"
)

type Record struct {
	Operation      Operation    `json:"operation"`
	URL            string       `json:"url"`
	Title          string       `json:"title,omitempty"`
	Headings       []string     `json:"headings,omitempty"`
	AnchorTerms    []string     `json:"anchor_terms,omitempty"`
	SalientSketch  string       `json:"salient_sketch,omitempty"`
	Language       string       `json:"language,omitempty"`
	PublishedAt    string       `json:"published_at,omitempty"`
	FetchedAt      string       `json:"fetched_at,omitempty"`
	ContentSHA256  string       `json:"content_sha256,omitempty"`
	AuthorityScore uint16       `json:"authority_score,omitempty"`
	FreshnessScore uint16       `json:"freshness_score,omitempty"`
	Provenance     []Provenance `json:"provenance,omitempty"`
}

type Provenance struct {
	Source       string `json:"source"`
	SourceURI    string `json:"source_uri"`
	RetrievedAt  string `json:"retrieved_at"`
	RightsNotice string `json:"rights_notice"`
}

// ValidateRecord validates the semantic record fields before a producer emits
// them. Stream decoders additionally enforce exact JSON shape, duplicate URLs,
// counts, and byte digests.
func ValidateRecord(record Record, version int, kind Kind) error {
	if version != Version && version != VersionURLMetadata {
		return invalidShard(errors.New("manifest version is unsupported"))
	}
	if kind != KindSnapshot && kind != KindDelta {
		return invalidShard(errors.New("kind must be snapshot or delta"))
	}
	if err := record.validate(version, kind); err != nil {
		return invalidShard(err)
	}
	return nil
}

// VerifyCompressedShard verifies the content-addressed compressed bytes. It
// deliberately does not decompress; transport and zstd handling belong to an
// adapter with its own resource controls.
func VerifyCompressedShard(reader io.Reader, descriptor Shard) error {
	if err := descriptor.validate(); err != nil {
		return invalidShard(fmt.Errorf("descriptor: %w", err))
	}
	hasher := sha256.New()
	n, err := io.Copy(hasher, io.LimitReader(reader, int64(descriptor.CompressedSizeBytes)+1))
	if err != nil {
		return invalidShard(fmt.Errorf("read compressed shard: %w", err))
	}
	if uint64(n) != descriptor.CompressedSizeBytes {
		return invalidShard(fmt.Errorf("compressed size is %d bytes, expected %d", n, descriptor.CompressedSizeBytes))
	}
	if hex.EncodeToString(hasher.Sum(nil)) != descriptor.SHA256 {
		return invalidShard(errors.New("compressed sha256 digest mismatch"))
	}
	return nil
}

// ScanRecordStream accepts already-decompressed NDJSON bytes and validates them
// incrementally without buffering a shard. The visitor must stage side effects
// and discard them when ScanRecordStream returns an error: the final size and
// digest can only be authenticated after every record has been visited.
func ScanRecordStream(ctx context.Context, reader io.Reader, kind Kind, descriptor Shard, visit func(Record) error) error {
	return ScanRecordStreamVersion(ctx, reader, Version, kind, descriptor, visit)
}

// ScanRecordStreamVersion applies the record semantics bound by the signed
// manifest version. Version 1 requires publisher-supplied lexical metadata;
// version 2 requires URL-only records for rights-conservative packs.
func ScanRecordStreamVersion(ctx context.Context, reader io.Reader, version int, kind Kind, descriptor Shard, visit func(Record) error) error {
	if version != Version && version != VersionURLMetadata {
		return invalidShard(errors.New("manifest version is unsupported"))
	}
	if kind != KindSnapshot && kind != KindDelta {
		return invalidShard(errors.New("kind must be snapshot or delta"))
	}
	if err := descriptor.validate(); err != nil {
		return invalidShard(fmt.Errorf("descriptor: %w", err))
	}
	if ctx == nil {
		return invalidShard(errors.New("context is required"))
	}
	if visit == nil {
		return invalidShard(errors.New("record visitor is required"))
	}
	digest := &streamDigest{hasher: sha256.New()}
	limited := io.LimitReader(reader, int64(descriptor.UncompressedSizeBytes)+1)
	scanner := bufio.NewScanner(io.TeeReader(contextReader{ctx: ctx, reader: limited}, digest))
	scanner.Buffer(make([]byte, 64<<10), MaxRecordBytes+1)
	seenURLs := make(map[string]struct{}, descriptor.RecordCount)
	var recordCount uint64
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Bytes()
		if len(line) == 0 {
			return invalidShard(fmt.Errorf("record %d is empty", recordCount+1))
		}
		if len(line) > MaxRecordBytes {
			return invalidShard(fmt.Errorf("record %d exceeds %d bytes", recordCount+1, MaxRecordBytes))
		}
		var record Record
		if err := DecodeStrictJSON(line, &record); err != nil {
			return invalidShard(fmt.Errorf("record %d: %w", recordCount+1, err))
		}
		if err := record.validate(version, kind); err != nil {
			return invalidShard(fmt.Errorf("record %d: %w", recordCount+1, err))
		}
		if record.Operation == OperationTombstone {
			hasMetadata, err := tombstoneHasMetadataFields(line)
			if err != nil {
				return invalidShard(fmt.Errorf("record %d: %w", recordCount+1, err))
			}
			if hasMetadata {
				return invalidShard(fmt.Errorf("record %d: tombstone must contain only operation and url", recordCount+1))
			}
		}
		if _, duplicate := seenURLs[record.URL]; duplicate {
			return invalidShard(fmt.Errorf("record %d duplicates URL %q", recordCount+1, record.URL))
		}
		seenURLs[record.URL] = struct{}{}
		recordCount++
		if recordCount > descriptor.RecordCount {
			return invalidShard(errors.New("record count exceeds descriptor"))
		}
		if err := visit(record); err != nil {
			return fmt.Errorf("visit record %d: %w", recordCount, err)
		}
	}
	if err := scanner.Err(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return invalidShard(fmt.Errorf("scan: %w", err))
	}
	if digest.size != descriptor.UncompressedSizeBytes {
		return invalidShard(fmt.Errorf("uncompressed size is %d bytes, expected %d", digest.size, descriptor.UncompressedSizeBytes))
	}
	if digest.lastByte != '\n' {
		return invalidShard(errors.New("NDJSON shard must end with a newline"))
	}
	if hex.EncodeToString(digest.hasher.Sum(nil)) != descriptor.UncompressedSHA256 {
		return invalidShard(errors.New("uncompressed sha256 digest mismatch"))
	}
	if recordCount != descriptor.RecordCount {
		return invalidShard(fmt.Errorf("record count is %d, expected %d", recordCount, descriptor.RecordCount))
	}
	return nil
}

// VerifyRecordStream is a small-data convenience wrapper. Installers should
// use ScanRecordStream so one shard is never accumulated in memory.
func VerifyRecordStream(reader io.Reader, kind Kind, descriptor Shard) ([]Record, error) {
	return VerifyRecordStreamVersion(reader, Version, kind, descriptor)
}

func VerifyRecordStreamVersion(reader io.Reader, version int, kind Kind, descriptor Shard) ([]Record, error) {
	if err := descriptor.validate(); err != nil {
		return nil, invalidShard(fmt.Errorf("descriptor: %w", err))
	}
	if descriptor.RecordCount > RecommendedInstallRecordLimit {
		return nil, invalidShard(fmt.Errorf("buffered helper is limited to %d records; use ScanRecordStream", RecommendedInstallRecordLimit))
	}
	records := make([]Record, 0, descriptor.RecordCount)
	err := ScanRecordStreamVersion(context.Background(), reader, version, kind, descriptor, func(record Record) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(buffer)
}

type streamDigest struct {
	hasher   hashWriter
	size     uint64
	lastByte byte
}

type hashWriter interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
}

func (digest *streamDigest) Write(buffer []byte) (int, error) {
	written, err := digest.hasher.Write(buffer)
	digest.size += uint64(written)
	if written > 0 {
		digest.lastByte = buffer[written-1]
	}
	return written, err
}

func (record Record) validate(version int, kind Kind) error {
	if err := validateCanonicalHTTPURL(record.URL); err != nil {
		return fmt.Errorf("url: %w", err)
	}
	if version == VersionURLMetadata {
		parsed, err := url.Parse(record.URL)
		if err != nil || parsed.RawQuery != "" || parsed.ForceQuery {
			return errors.New("URL-metadata records must not contain a query")
		}
	}
	switch record.Operation {
	case OperationTombstone:
		if kind != KindDelta {
			return errors.New("tombstone is allowed only in delta packs")
		}
		return nil
	case OperationUpsert:
	default:
		return errors.New("operation must be upsert or tombstone")
	}
	if err := boundedOptional("title", record.Title, maxTitleBytes); err != nil {
		return err
	}
	if len(record.Headings) > maxHeadings {
		return fmt.Errorf("headings must contain at most %d entries", maxHeadings)
	}
	if err := validateUniqueStrings("headings", record.Headings, maxHeadingBytes); err != nil {
		return err
	}
	if len(record.AnchorTerms) > maxAnchorTerms {
		return fmt.Errorf("anchor_terms must contain at most %d entries", maxAnchorTerms)
	}
	if err := validateUniqueStrings("anchor_terms", record.AnchorTerms, maxAnchorTermBytes); err != nil {
		return err
	}
	if err := boundedOptional("salient_sketch", record.SalientSketch, maxSalientSketch); err != nil {
		return err
	}
	if version == Version && strings.TrimSpace(record.Title) == "" && len(record.Headings) == 0 && len(record.AnchorTerms) == 0 && strings.TrimSpace(record.SalientSketch) == "" {
		return errors.New("upsert requires title, headings, anchor_terms, or salient_sketch")
	}
	if version == VersionURLMetadata && (record.Title != "" || len(record.Headings) != 0 || len(record.AnchorTerms) != 0 || record.SalientSketch != "" ||
		record.PublishedAt != "" || record.ContentSHA256 != "" || record.AuthorityScore != 0) {
		return errors.New("URL-metadata upsert contains enriched or page-derived fields")
	}
	if !languagePattern.MatchString(record.Language) {
		return errors.New("language is invalid")
	}
	if record.PublishedAt != "" {
		if err := validateTimestamp(record.PublishedAt); err != nil {
			return fmt.Errorf("published_at: %w", err)
		}
	}
	if err := validateTimestamp(record.FetchedAt); err != nil {
		return fmt.Errorf("fetched_at: %w", err)
	}
	if record.PublishedAt != "" {
		published, _ := time.Parse(time.RFC3339, record.PublishedAt)
		fetched, _ := time.Parse(time.RFC3339, record.FetchedAt)
		if published.After(fetched) {
			return errors.New("published_at must not be after fetched_at")
		}
	}
	if record.ContentSHA256 != "" {
		if err := validateDigest(record.ContentSHA256); err != nil {
			return fmt.Errorf("content_sha256: %w", err)
		}
	}
	if record.AuthorityScore > maxScore || record.FreshnessScore > maxScore {
		return fmt.Errorf("scores must be 0..%d", maxScore)
	}
	if len(record.Provenance) < 1 || len(record.Provenance) > maxProvenanceEntries {
		return fmt.Errorf("provenance must contain 1..%d entries", maxProvenanceEntries)
	}
	sources := make(map[string]struct{}, len(record.Provenance))
	for index, provenance := range record.Provenance {
		if err := provenance.validate(record.FetchedAt); err != nil {
			return fmt.Errorf("provenance[%d]: %w", index, err)
		}
		key := provenance.Source + "\x00" + provenance.SourceURI
		if _, duplicate := sources[key]; duplicate {
			return fmt.Errorf("duplicate provenance source %q", provenance.Source)
		}
		sources[key] = struct{}{}
	}
	return nil
}

func tombstoneHasMetadataFields(raw []byte) (bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return false, err
	}
	for field := range fields {
		if field != "operation" && field != "url" {
			return true, nil
		}
	}
	return false, nil
}

func (provenance Provenance) validate(fetchedAt string) error {
	if err := boundedRequired("source", provenance.Source, maxSourceNameBytes); err != nil {
		return err
	}
	if err := validateCanonicalHTTPURL(provenance.SourceURI); err != nil {
		return fmt.Errorf("source_uri: %w", err)
	}
	if err := validateTimestamp(provenance.RetrievedAt); err != nil {
		return fmt.Errorf("retrieved_at: %w", err)
	}
	retrieved, _ := time.Parse(time.RFC3339, provenance.RetrievedAt)
	fetched, _ := time.Parse(time.RFC3339, fetchedAt)
	if retrieved.After(fetched) {
		return errors.New("retrieved_at must not be after fetched_at")
	}
	return boundedRequired("rights_notice", provenance.RightsNotice, maxRecordRightsBytes)
}

func validateUniqueStrings(name string, values []string, maxBytes int) error {
	seen := make(map[string]struct{}, len(values))
	for index, value := range values {
		if err := boundedRequired(fmt.Sprintf("%s[%d]", name, index), value, maxBytes); err != nil {
			return err
		}
		if _, duplicate := seen[value]; duplicate {
			return fmt.Errorf("%s contains duplicate %q", name, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func boundedOptional(name, value string, maximum int) error {
	if len(value) > maximum || !utf8.ValidString(value) {
		return fmt.Errorf("%s must be UTF-8 of at most %d bytes", name, maximum)
	}
	return nil
}

func invalidShard(err error) error {
	return fmt.Errorf("%w: %v", ErrInvalidShard, err)
}
