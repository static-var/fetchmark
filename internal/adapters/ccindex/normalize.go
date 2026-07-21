// Package ccindex normalizes bounded Common Crawl CDX index exports into an
// exact, versioned metadata stream for the separate publisher evidence stage.
// It performs no network access and does not infer robots, indexing, or rights
// permission from historical crawl inclusion.
package ccindex

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

const (
	NormalizedVersion             = 1
	ReportVersion                 = 1
	FormatCDXAPIJSON              = "cdx-api-json-v1"
	FormatCDXJHeader              = "cdxj-header-v1"
	FormatURLIndexParquet         = "url-index-parquet-flat-v1"
	MaxRawLineBytes               = 64 << 10
	MaxNormalizedLineBytes        = 32 << 10
	MaxRecords             uint64 = 50_000_000
	maxURLKeyBytes                = 8 << 10
	maxURLBytes                   = 16 << 10
	maxMIMEBytes                  = 256
	maxDigestBytes                = 128
	maxFilenameBytes              = 4 << 10
	maxLanguagesBytes             = 512
	maxEncodingBytes              = 128
)

type Options struct {
	Format string
}

// Candidate is a lossless normalized CDX metadata row. It intentionally has
// no admission field: a separate live collector must add current robots,
// indexing, and rights evidence before indexpackselection can consume it.
type Candidate struct {
	Version      int    `json:"version"`
	URLKey       string `json:"urlkey"`
	Timestamp    string `json:"timestamp"`
	URL          string `json:"url"`
	MIME         string `json:"mime"`
	MIMEDetected string `json:"mime-detected,omitempty"`
	Status       string `json:"status"`
	Digest       string `json:"digest"`
	Length       string `json:"length"`
	Offset       string `json:"offset"`
	Filename     string `json:"filename"`
	Languages    string `json:"languages,omitempty"`
	Encoding     string `json:"encoding,omitempty"`
}

type Report struct {
	Version      int                          `json:"version"`
	Format       string                       `json:"format"`
	Records      uint64                       `json:"records"`
	InputBytes   uint64                       `json:"input_bytes"`
	OutputBytes  uint64                       `json:"output_bytes"`
	InputSHA256  string                       `json:"input_sha256"`
	OutputSHA256 string                       `json:"output_sha256"`
	Selection    *ParquetSelectionReport      `json:"selection,omitempty"`
	Parts        *ParquetPartsSelectionReport `json:"parts,omitempty"`
}

// DecodeCandidate strictly decodes one normalized-v1 JSON object for the
// separate live-admission stage. It does not accept a raw CDX object and does
// not add or infer permission evidence.
func DecodeCandidate(raw []byte) (Candidate, error) {
	if len(raw) == 0 || len(raw) > MaxNormalizedLineBytes {
		return Candidate{}, fmt.Errorf("Common Crawl normalized candidate: size must be 1..%d bytes", MaxNormalizedLineBytes)
	}
	var candidate Candidate
	if err := indexpack.DecodeStrictJSON(raw, &candidate); err != nil {
		return Candidate{}, fmt.Errorf("Common Crawl normalized candidate: %w", err)
	}
	if err := candidate.validate(); err != nil {
		return Candidate{}, fmt.Errorf("Common Crawl normalized candidate: %w", err)
	}
	return candidate, nil
}

type apiRecord struct {
	URLKey       string `json:"urlkey"`
	Timestamp    string `json:"timestamp"`
	URL          string `json:"url"`
	MIME         string `json:"mime"`
	MIMEDetected string `json:"mime-detected"`
	Status       string `json:"status"`
	Digest       string `json:"digest"`
	Length       string `json:"length"`
	Offset       string `json:"offset"`
	Filename     string `json:"filename"`
	Languages    string `json:"languages"`
	Encoding     string `json:"encoding,omitempty"`
}

type cdxjPayload struct {
	URL          string `json:"url"`
	MIME         string `json:"mime"`
	MIMEDetected string `json:"mime-detected"`
	Status       string `json:"status"`
	Digest       string `json:"digest"`
	Length       string `json:"length"`
	Offset       string `json:"offset"`
	Filename     string `json:"filename"`
	Languages    string `json:"languages"`
	Encoding     string `json:"encoding,omitempty"`
}

type countingHash struct {
	writer io.Writer
	hash   hashWriter
	bytes  uint64
}

type hashWriter interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
}

func (writer *countingHash) Write(raw []byte) (int, error) {
	written, err := writer.writer.Write(raw)
	if written > 0 {
		_, _ = writer.hash.Write(raw[:written])
		writer.bytes += uint64(written)
	}
	return written, err
}

// Normalize strictly decodes every source row and emits deterministic JSONL.
// A malformed or unknown field invalidates the whole artifact; callers must
// stage output until the returned report has been accepted.
func Normalize(ctx context.Context, input io.Reader, output io.Writer, options Options) (Report, error) {
	if ctx == nil || input == nil || output == nil {
		return Report{}, errors.New("Common Crawl index normalizer: context, input, and output are required")
	}
	if options.Format != FormatCDXAPIJSON && options.Format != FormatCDXJHeader {
		return Report{}, fmt.Errorf("Common Crawl index normalizer: unsupported format %q", options.Format)
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	inputDigest := sha256.New()
	inputCounter := &countingHash{writer: io.Discard, hash: inputDigest}
	scanner := bufio.NewScanner(io.TeeReader(contextReader{ctx: ctx, reader: input}, inputCounter))
	scanner.Buffer(make([]byte, 64<<10), MaxRawLineBytes+1)
	outputCounter := &countingHash{writer: output, hash: sha256.New()}
	report := Report{Version: ReportVersion, Format: options.Format}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		report.Records++
		if report.Records > MaxRecords {
			return Report{}, fmt.Errorf("Common Crawl index normalizer: record count exceeds %d", MaxRecords)
		}
		line := scanner.Bytes()
		if len(line) == 0 || len(line) > MaxRawLineBytes {
			return Report{}, fmt.Errorf("Common Crawl index normalizer: line %d has invalid size", report.Records)
		}
		candidate, err := decodeLine(line, options.Format)
		if err != nil {
			return Report{}, fmt.Errorf("Common Crawl index normalizer: line %d: %w", report.Records, err)
		}
		raw, err := json.Marshal(candidate)
		if err != nil {
			return Report{}, err
		}
		raw = append(raw, '\n')
		if len(raw) > MaxNormalizedLineBytes {
			return Report{}, fmt.Errorf("Common Crawl index normalizer: line %d exceeds normalized size limit", report.Records)
		}
		written, err := outputCounter.Write(raw)
		if err != nil {
			return Report{}, fmt.Errorf("Common Crawl index normalizer: write line %d: %w", report.Records, err)
		}
		if written != len(raw) {
			return Report{}, io.ErrShortWrite
		}
	}
	if err := scanner.Err(); err != nil {
		return Report{}, fmt.Errorf("Common Crawl index normalizer: scan: %w", err)
	}
	// A reader or writer may cancel the context while completing the final
	// record. Do not report a complete artifact merely because Scanner reached
	// EOF before it needed another context-aware read.
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	if report.Records == 0 {
		return Report{}, errors.New("Common Crawl index normalizer: input contains no records")
	}
	report.InputBytes = inputCounter.bytes
	report.OutputBytes = outputCounter.bytes
	report.InputSHA256 = hex.EncodeToString(inputDigest.Sum(nil))
	report.OutputSHA256 = hex.EncodeToString(outputCounter.hash.Sum(nil))
	return report, nil
}

func decodeLine(line []byte, format string) (Candidate, error) {
	var source apiRecord
	switch format {
	case FormatCDXAPIJSON:
		if err := indexpack.DecodeStrictJSON(line, &source); err != nil {
			return Candidate{}, err
		}
	case FormatCDXJHeader:
		first := strings.IndexByte(string(line), ' ')
		if first <= 0 {
			return Candidate{}, errors.New("CDXJ header is missing URL key")
		}
		secondRelative := strings.IndexByte(string(line[first+1:]), ' ')
		if secondRelative <= 0 {
			return Candidate{}, errors.New("CDXJ header is missing timestamp")
		}
		second := first + 1 + secondRelative
		var payload cdxjPayload
		if err := indexpack.DecodeStrictJSON(line[second+1:], &payload); err != nil {
			return Candidate{}, err
		}
		source = apiRecord{
			URLKey: string(line[:first]), Timestamp: string(line[first+1 : second]),
			URL: payload.URL, MIME: payload.MIME, MIMEDetected: payload.MIMEDetected,
			Status: payload.Status, Digest: payload.Digest, Length: payload.Length,
			Offset: payload.Offset, Filename: payload.Filename, Languages: payload.Languages, Encoding: payload.Encoding,
		}
	}
	candidate := Candidate{
		Version: NormalizedVersion, URLKey: source.URLKey, Timestamp: source.Timestamp,
		URL: source.URL, MIME: source.MIME, MIMEDetected: source.MIMEDetected,
		Status: source.Status, Digest: source.Digest, Length: source.Length,
		Offset: source.Offset, Filename: source.Filename, Languages: source.Languages, Encoding: source.Encoding,
	}
	normalizedDigest, err := normalizeDigest(candidate.Digest)
	if err != nil {
		return Candidate{}, err
	}
	candidate.Digest = normalizedDigest
	if err := candidate.validate(); err != nil {
		return Candidate{}, err
	}
	return candidate, nil
}

func (candidate Candidate) validate() error {
	if candidate.Version != NormalizedVersion {
		return errors.New("normalized candidate version is invalid")
	}
	fields := []struct{ name, value string }{
		{"urlkey", candidate.URLKey}, {"url", candidate.URL}, {"mime", candidate.MIME},
		{"status", candidate.Status},
		{"digest", candidate.Digest}, {"length", candidate.Length}, {"offset", candidate.Offset},
		{"filename", candidate.Filename},
	}
	for _, field := range fields {
		if field.value == "" || !utf8.ValidString(field.value) || strings.ContainsAny(field.value, "\x00\r\n") {
			return fmt.Errorf("%s is missing or contains control data", field.name)
		}
	}
	if len(candidate.URLKey) > maxURLKeyBytes || len(candidate.URL) > maxURLBytes ||
		len(candidate.MIME) > maxMIMEBytes || len(candidate.MIMEDetected) > maxMIMEBytes ||
		len(candidate.Digest) > maxDigestBytes || len(candidate.Filename) > maxFilenameBytes ||
		len(candidate.Languages) > maxLanguagesBytes || len(candidate.Encoding) > maxEncodingBytes ||
		strings.ContainsAny(candidate.MIMEDetected, "\x00\r\n") || !utf8.ValidString(candidate.MIMEDetected) ||
		strings.ContainsAny(candidate.Languages, "\x00\r\n") || !utf8.ValidString(candidate.Languages) ||
		strings.ContainsAny(candidate.Encoding, "\x00\r\n") || !utf8.ValidString(candidate.Encoding) {
		return errors.New("normalized candidate field exceeds its byte limit")
	}
	if len(candidate.Timestamp) != 14 {
		return errors.New("timestamp must contain 14 decimal digits")
	}
	if _, err := time.Parse("20060102150405", candidate.Timestamp); err != nil {
		return errors.New("timestamp is invalid")
	}
	if len(candidate.Status) != 3 || !decimal(candidate.Status) {
		return errors.New("status must contain three decimal digits")
	}
	if _, err := parseCanonicalUint(candidate.Length); err != nil {
		return fmt.Errorf("length: %w", err)
	}
	if _, err := parseCanonicalUint(candidate.Offset); err != nil {
		return fmt.Errorf("offset: %w", err)
	}
	return nil
}

func normalizeDigest(raw string) (string, error) {
	value := strings.TrimPrefix(raw, "sha1:")
	if len(value) != 32 {
		return "", errors.New("digest must be a Common Crawl SHA-1 base32 value")
	}
	for _, character := range value {
		if (character < 'A' || character > 'Z') && (character < '2' || character > '7') {
			return "", errors.New("digest must be a Common Crawl SHA-1 base32 value")
		}
	}
	return value, nil
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

func parseCanonicalUint(raw string) (uint64, error) {
	if !decimal(raw) || (len(raw) > 1 && raw[0] == '0') {
		return 0, errors.New("must be a canonical unsigned decimal")
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, errors.New("is outside uint64")
	}
	return value, nil
}

func decimal(raw string) bool {
	if raw == "" {
		return false
	}
	for _, character := range raw {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}
