package indexpackadmission

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/core/canonicalurl"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"golang.org/x/net/html/charset"
)

const (
	ResponseEvidenceVersion = 1
	MaxRepresentationBytes  = 4 << 20
	MaxXRobotsTagValues     = 32
	MaxXRobotsTagBytes      = 64 << 10
	MaxMetadataRobotsValues = 64
	MaxMetadataRobotsBytes  = 64 << 10
)

// IndexingInput is the bounded live representation passed to the pure evidence
// evaluator. Representation is hashed and parsed but never copied into the
// returned archive evidence.
type IndexingInput struct {
	Status         int
	FinalURL       string
	ContentType    string
	XRobotsTag     []string
	Representation []byte
	// Complete must be set only after the bounded HTTP reader reached a clean
	// EOF without hitting its byte limit or a decoding/content-length error.
	Complete   bool
	UserAgent  string
	ObservedAt time.Time
	Validity   time.Duration
}

// ResponseEvidence is the canonical audit subset whose exact JSON bytes are
// bound by IndexingObservation.HeadersSHA256.
type ResponseEvidence struct {
	Version        int      `json:"version"`
	Status         int      `json:"status"`
	FinalURL       string   `json:"final_url"`
	ContentType    string   `json:"content_type"`
	XRobotsTag     []string `json:"x_robots_tag"`
	MetadataRobots []string `json:"metadata_robots"`
	Disposition    string   `json:"disposition"`
	ParserVersion  string   `json:"parser_version"`
}

// EvaluateIndexing creates deterministic builder evidence without retaining
// the page representation. The caller remains responsible for public egress,
// robots permission, pacing, and matching FinalURL to the normalized row.
func EvaluateIndexing(input IndexingInput) (indexpackselection.IndexingObservation, ResponseEvidence, error) {
	if !input.Complete {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, errors.New("index pack admission: representation is incomplete")
	}
	if input.Status < 100 || input.Status > 599 {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, errors.New("index pack admission: HTTP status is invalid")
	}
	canonical, err := canonicalurl.V1(input.FinalURL)
	if err != nil || canonical != input.FinalURL || !isPackPublicURL(canonical) {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, errors.New("index pack admission: final URL must be canonical public HTTP(S)")
	}
	if len(input.ContentType) > 256 || !utf8.ValidString(input.ContentType) || strings.ContainsAny(input.ContentType, "\x00\r\n") {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, errors.New("index pack admission: content type is invalid")
	}
	if len(input.XRobotsTag) > MaxXRobotsTagValues {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, errors.New("index pack admission: too many X-Robots-Tag values")
	}
	headerBytes := 0
	for _, value := range input.XRobotsTag {
		headerBytes += len(value)
		if !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return indexpackselection.IndexingObservation{}, ResponseEvidence{}, errors.New("index pack admission: X-Robots-Tag contains control data")
		}
	}
	if headerBytes > MaxXRobotsTagBytes {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, errors.New("index pack admission: X-Robots-Tag exceeds byte limit")
	}
	if len(input.Representation) > MaxRepresentationBytes {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, fmt.Errorf("index pack admission: representation exceeds %d bytes", MaxRepresentationBytes)
	}
	if strings.TrimSpace(input.UserAgent) != input.UserAgent || input.UserAgent == "" || len(input.UserAgent) > 256 || strings.ContainsAny(input.UserAgent, "\x00\r\n") {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, errors.New("index pack admission: user agent is invalid")
	}
	if input.ObservedAt.IsZero() || input.Validity <= 0 || input.Validity > MaxValidityHours*time.Hour {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, errors.New("index pack admission: observation time or validity is invalid")
	}
	observedAt := input.ObservedAt.UTC().Truncate(time.Second)
	disposition := localcorpus.DispositionPermitted
	var metadataValues []string
	if input.Status == 200 && htmlContentType(input.ContentType) {
		decoded, err := decodeHTMLRepresentation(input.Representation, input.ContentType)
		if err != nil {
			return indexpackselection.IndexingObservation{}, ResponseEvidence{}, fmt.Errorf("index pack admission: decode HTML representation: %w", err)
		}
		disposition, metadataValues, err = localcorpus.EvaluateIndexabilityEvidenceStrict(input.XRobotsTag, decoded, input.UserAgent)
		if err != nil {
			return indexpackselection.IndexingObservation{}, ResponseEvidence{}, fmt.Errorf("index pack admission: parse HTML metadata: %w", err)
		}
	} else if directivesOnlyNoIndex(input.XRobotsTag, input.UserAgent) {
		disposition = localcorpus.DispositionNoIndexHeader
	}
	if len(metadataValues) > MaxMetadataRobotsValues {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, errors.New("index pack admission: too many applicable robots metadata values")
	}
	metadataBytes := 0
	for _, value := range metadataValues {
		metadataBytes += len(value)
		if !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return indexpackselection.IndexingObservation{}, ResponseEvidence{}, errors.New("index pack admission: robots metadata contains control data")
		}
	}
	if metadataBytes > MaxMetadataRobotsBytes {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, errors.New("index pack admission: robots metadata exceeds byte limit")
	}
	outcome := "indexable"
	if input.Status != 200 {
		outcome = "status_not_200"
	} else if !htmlContentType(input.ContentType) {
		outcome = "non_html"
	} else if disposition == localcorpus.DispositionNoIndexHeader || disposition == localcorpus.DispositionNoIndexMetadata {
		outcome = "noindex"
	}
	evidence := ResponseEvidence{
		Version: ResponseEvidenceVersion, Status: input.Status, FinalURL: canonical,
		ContentType: input.ContentType, XRobotsTag: append([]string(nil), input.XRobotsTag...),
		MetadataRobots: append([]string(nil), metadataValues...),
		Disposition:    string(disposition), ParserVersion: NoIndexParserVersion,
	}
	evidenceRaw, err := json.Marshal(evidence)
	if err != nil {
		return indexpackselection.IndexingObservation{}, ResponseEvidence{}, err
	}
	headerDigest := sha256.Sum256(evidenceRaw)
	representationDigest := sha256.Sum256(input.Representation)
	return indexpackselection.IndexingObservation{
		CheckedAt:  observedAt.Format(time.RFC3339),
		ValidUntil: observedAt.Add(input.Validity).Format(time.RFC3339),
		FinalURL:   canonical, Outcome: outcome,
		HeadersSHA256:        hex.EncodeToString(headerDigest[:]),
		RepresentationSHA256: hex.EncodeToString(representationDigest[:]),
		ParserVersion:        NoIndexParserVersion,
	}, evidence, nil
}

// ValidateDerivedIndexableEvidence validates the canonical body-free evidence
// retained for an affirmative indexing decision and returns the digest of its
// canonical JSON representation. It independently replays header and derived
// metadata directives, but cannot prove metadata extraction without the page
// representation that the collector deliberately does not retain.
func ValidateDerivedIndexableEvidence(evidence ResponseEvidence, userAgent string) (string, error) {
	canonical, err := canonicalurl.V1(evidence.FinalURL)
	if evidence.Version != ResponseEvidenceVersion || evidence.Status != 200 || err != nil || canonical != evidence.FinalURL || !isPackPublicURL(canonical) ||
		evidence.ParserVersion != NoIndexParserVersion || !htmlContentType(evidence.ContentType) ||
		strings.TrimSpace(userAgent) != userAgent || userAgent == "" || len(userAgent) > 256 || strings.ContainsAny(userAgent, "\x00\r\n") {
		return "", errors.New("index pack admission: derived response evidence header is invalid")
	}
	if err := validateDerivedDirectiveValues(evidence.XRobotsTag, MaxXRobotsTagValues, MaxXRobotsTagBytes, "X-Robots-Tag"); err != nil {
		return "", err
	}
	if err := validateDerivedDirectiveValues(evidence.MetadataRobots, MaxMetadataRobotsValues, MaxMetadataRobotsBytes, "robots metadata"); err != nil {
		return "", err
	}
	disposition := localcorpus.EvaluateDerivedIndexabilityEvidence(evidence.XRobotsTag, evidence.MetadataRobots, userAgent)
	if disposition != localcorpus.DispositionPermitted || evidence.Disposition != string(disposition) {
		return "", errors.New("index pack admission: derived response evidence is not indexable")
	}
	raw, err := json.Marshal(evidence)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func validateDerivedDirectiveValues(values []string, maximumCount, maximumBytes int, label string) error {
	if len(values) > maximumCount {
		return fmt.Errorf("index pack admission: too many %s values", label)
	}
	total := 0
	for _, value := range values {
		total += len(value)
		if !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("index pack admission: %s contains control data", label)
		}
	}
	if total > maximumBytes {
		return fmt.Errorf("index pack admission: %s exceeds byte limit", label)
	}
	return nil
}

func decodeHTMLRepresentation(raw []byte, contentType string) ([]byte, error) {
	_, parameters, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, err
	}
	if label, declared := parameters["charset"]; declared {
		if strings.TrimSpace(label) == "" {
			return nil, errors.New("declared charset is empty")
		}
		if encoding, _ := charset.Lookup(label); encoding == nil {
			return nil, fmt.Errorf("unsupported charset %q", label)
		}
	}
	_, selectedCharset, _ := charset.DetermineEncoding(raw, contentType)
	if selectedCharset == "utf-8" && !utf8.Valid(raw) {
		return nil, errors.New("UTF-8 representation contains malformed byte sequences")
	}
	reader, err := charset.NewReader(bytes.NewReader(raw), contentType)
	if err != nil {
		return nil, err
	}
	decoded, err := io.ReadAll(io.LimitReader(reader, MaxRepresentationBytes+1))
	if err != nil {
		return nil, err
	}
	if len(decoded) > MaxRepresentationBytes {
		return nil, errors.New("decoded representation exceeds byte limit")
	}
	if !utf8.Valid(decoded) {
		return nil, errors.New("decoded representation is not valid UTF-8")
	}
	// WHATWG-compatible decoders replace malformed byte sequences with U+FFFD
	// instead of reporting an error. Publication requires affirmative evidence,
	// so even a potentially literal replacement rune is conservatively treated
	// as decoding uncertainty.
	if bytes.Contains(decoded, []byte("\uFFFD")) {
		return nil, errors.New("decoded representation contains replacement characters")
	}
	return decoded, nil
}

// Non-HTML responses cannot carry applicable HTML metadata. Preserve an
// explicit header noindex disposition in their audit evidence without parsing
// an unrelated representation.
func directivesOnlyNoIndex(values []string, userAgent string) bool {
	disposition, _, err := localcorpus.EvaluateIndexabilityEvidenceStrict(values, nil, userAgent)
	return err == nil && disposition == localcorpus.DispositionNoIndexHeader
}

func htmlContentType(raw string) bool {
	mediaType, _, err := mime.ParseMediaType(raw)
	if err != nil {
		return false
	}
	return strings.EqualFold(mediaType, "text/html") || strings.EqualFold(mediaType, "application/xhtml+xml")
}

func isPackPublicURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") {
		return false
	}
	host := parsed.Hostname()
	if host == "" || host != strings.ToLower(host) || strings.HasSuffix(host, ".") || len(host) > 253 ||
		net.ParseIP(host) != nil || !strings.Contains(host, ".") || strings.ContainsAny(host, " /\\@") {
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
