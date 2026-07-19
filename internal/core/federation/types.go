// Package federation defines the cryptographic wire contract and immutable
// trust registry for Fetchmark's private, allowlisted federation protocol. It
// deliberately performs no filesystem, HTTP, configuration, or replay-cache
// operations.
package federation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/core/canonicalurl"
)

const (
	ProtocolVersion = 1
	SearchMethod    = "POST"
	SearchPath      = "/federation/v1/index/search"

	MaxRequestBytes  = 32 << 10
	MaxResponseBytes = 128 << 10
	MaxResults       = 20
	MaxQueryBytes    = 1 << 10
	MaxURLBytes      = 4 << 10
	NonceBytes       = 16

	DefaultClockSkew    = 60 * time.Second
	MaxAllowedClockSkew = 10 * time.Minute

	maxJSONDepth = 16
)

var (
	ErrInvalidRequest   = errors.New("federation: invalid request")
	ErrInvalidResponse  = errors.New("federation: invalid response")
	ErrInvalidSignature = errors.New("federation: invalid signature")

	idPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
	digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// SearchRequest is intentionally small. Resource, latency, and provider policy
// remain local to the receiving peer rather than becoming remote controls.
type SearchRequest struct {
	Query string `json:"query"`
	Limit int    `json:"limit"`
}

// SearchResult reveals only a canonical URL. This first private-federation
// contract does not exchange title, snippet, body, score, or provenance.
type SearchResult struct {
	URL string `json:"url"`
}

type SearchResponse struct {
	Results []SearchResult `json:"results"`
}

// SignedRequest is the v1 wire envelope. Body is retained as exact JSON bytes
// so BodySHA256 binds the bytes received rather than a decoded approximation.
type SignedRequest struct {
	Version    int             `json:"version"`
	Sender     string          `json:"sender"`
	Audience   string          `json:"audience"`
	KeyID      string          `json:"key_id"`
	Method     string          `json:"method"`
	Path       string          `json:"path"`
	Timestamp  string          `json:"timestamp"`
	Nonce      string          `json:"nonce"`
	BodySHA256 string          `json:"body_sha256"`
	Body       json.RawMessage `json:"body"`
	Signature  string          `json:"signature"`
}

// SignedResponse cryptographically binds the response to both the exact
// request bytes and its nonce, preventing cross-request substitution.
type SignedResponse struct {
	Version       int             `json:"version"`
	Sender        string          `json:"sender"`
	Audience      string          `json:"audience"`
	KeyID         string          `json:"key_id"`
	Method        string          `json:"method"`
	Path          string          `json:"path"`
	Timestamp     string          `json:"timestamp"`
	Nonce         string          `json:"nonce"`
	Status        int             `json:"status"`
	RequestNonce  string          `json:"request_nonce"`
	RequestSHA256 string          `json:"request_sha256"`
	BodySHA256    string          `json:"body_sha256"`
	Body          json.RawMessage `json:"body"`
	Signature     string          `json:"signature"`
}

// Verification identifies the expected transport-independent peer context.
// Replay prevention remains the caller's responsibility: retain accepted
// sender/nonce pairs for at least the configured clock-skew window.
type Verification struct {
	Sender       string
	Audience     string
	Now          time.Time
	MaxClockSkew time.Duration
}

// VerifiedRequest contains only values validated against the signature and
// caller-supplied context. Body and nonce accessors return defensive copies.
type VerifiedRequest struct {
	Envelope SignedRequest
	Search   SearchRequest
	Digest   string
	nonce    []byte
}

func (verified VerifiedRequest) NonceBytes() []byte { return cloneBytes(verified.nonce) }

// VerifiedResponse contains only values validated against both its signature
// and the exact request that prompted it.
type VerifiedResponse struct {
	Envelope SignedResponse
	Search   SearchResponse
	Digest   string
	nonce    []byte
}

func (verified VerifiedResponse) NonceBytes() []byte { return cloneBytes(verified.nonce) }

func EncodeSearchRequest(request SearchRequest) ([]byte, error) {
	if err := validateSearchRequest(request); err != nil {
		return nil, invalidRequest(err)
	}
	return json.Marshal(request)
}

func DecodeSearchRequest(raw []byte) (SearchRequest, error) {
	var request SearchRequest
	if err := decodeStrictJSON(raw, &request); err != nil {
		return SearchRequest{}, invalidRequest(err)
	}
	if err := validateSearchRequest(request); err != nil {
		return SearchRequest{}, invalidRequest(err)
	}
	canonical, _ := json.Marshal(request)
	if string(canonical) != string(raw) {
		return SearchRequest{}, invalidRequest(errors.New("body must use canonical JSON encoding"))
	}
	return request, nil
}

func EncodeSearchResponse(response SearchResponse) ([]byte, error) {
	if response.Results == nil {
		response.Results = []SearchResult{}
	}
	if err := validateSearchResponse(response); err != nil {
		return nil, invalidResponse(err)
	}
	return json.Marshal(response)
}

func DecodeSearchResponse(raw []byte) (SearchResponse, error) {
	var response SearchResponse
	if err := decodeStrictJSON(raw, &response); err != nil {
		return SearchResponse{}, invalidResponse(err)
	}
	if err := validateSearchResponse(response); err != nil {
		return SearchResponse{}, invalidResponse(err)
	}
	canonical, _ := json.Marshal(response)
	if string(canonical) != string(raw) {
		return SearchResponse{}, invalidResponse(errors.New("body must use canonical JSON encoding"))
	}
	return response, nil
}

func validateSearchRequest(request SearchRequest) error {
	if !utf8.ValidString(request.Query) || len(request.Query) == 0 || len(request.Query) > MaxQueryBytes {
		return fmt.Errorf("query must be valid UTF-8 and 1..%d bytes", MaxQueryBytes)
	}
	if strings.TrimSpace(request.Query) == "" {
		return errors.New("query must contain non-space text")
	}
	for _, character := range request.Query {
		if unicode.IsControl(character) {
			return errors.New("query must not contain control characters")
		}
	}
	if request.Limit < 1 || request.Limit > MaxResults {
		return fmt.Errorf("limit must be 1..%d", MaxResults)
	}
	return nil
}

func validateSearchResponse(response SearchResponse) error {
	if response.Results == nil {
		return errors.New("results must be an array")
	}
	if len(response.Results) > MaxResults {
		return fmt.Errorf("results must contain at most %d entries", MaxResults)
	}
	seen := make(map[string]struct{}, len(response.Results))
	for index, result := range response.Results {
		if len(result.URL) == 0 || len(result.URL) > MaxURLBytes || !utf8.ValidString(result.URL) {
			return fmt.Errorf("results[%d].url must be valid UTF-8 and 1..%d bytes", index, MaxURLBytes)
		}
		parsed, err := url.Parse(result.URL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
			return fmt.Errorf("results[%d].url must be an HTTP(S) URL without user information", index)
		}
		canonical, err := canonicalurl.V1(result.URL)
		if err != nil || canonical != result.URL {
			return fmt.Errorf("results[%d].url must use canonical URL v%d", index, canonicalurl.VersionV1)
		}
		if _, duplicate := seen[result.URL]; duplicate {
			return fmt.Errorf("results[%d].url is duplicated", index)
		}
		seen[result.URL] = struct{}{}
	}
	return nil
}

func parseCanonicalTimestamp(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil || parsed.Nanosecond() != 0 || parsed.Location() != time.UTC || parsed.Format(time.RFC3339) != value {
		return time.Time{}, errors.New("must be canonical UTC RFC3339 with whole seconds")
	}
	return parsed, nil
}

func canonicalTimestamp(value time.Time) (string, error) {
	if value.IsZero() || value.Location() != time.UTC || value.Nanosecond() != 0 {
		return "", errors.New("timestamp must be UTC with whole seconds")
	}
	return value.Format(time.RFC3339), nil
}

func validateVerification(verification Verification, timestamp string) error {
	if err := validateID("expected sender", verification.Sender); err != nil {
		return err
	}
	if err := validateID("expected audience", verification.Audience); err != nil {
		return err
	}
	if verification.Now.IsZero() {
		return errors.New("verification time is required")
	}
	skew := verification.MaxClockSkew
	if skew == 0 {
		skew = DefaultClockSkew
	}
	if skew < time.Second || skew > MaxAllowedClockSkew {
		return fmt.Errorf("clock skew must be 1s..%s", MaxAllowedClockSkew)
	}
	signedAt, err := parseCanonicalTimestamp(timestamp)
	if err != nil {
		return fmt.Errorf("timestamp: %w", err)
	}
	delta := verification.Now.Sub(signedAt)
	if delta < 0 {
		delta = -delta
	}
	if delta > skew {
		return errors.New("timestamp is outside the accepted clock-skew window")
	}
	return nil
}

func validateID(name, value string) error {
	if !idPattern.MatchString(value) {
		return fmt.Errorf("%s must match %s", name, idPattern)
	}
	return nil
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func validateDigest(name, value string) error {
	if !digestPattern.MatchString(value) {
		return fmt.Errorf("%s must be a lowercase SHA-256 digest", name)
	}
	return nil
}

func invalidRequest(err error) error   { return fmt.Errorf("%w: %v", ErrInvalidRequest, err) }
func invalidResponse(err error) error  { return fmt.Errorf("%w: %v", ErrInvalidResponse, err) }
func invalidSignature(err error) error { return fmt.Errorf("%w: %v", ErrInvalidSignature, err) }
func cloneBytes(value []byte) []byte   { return append([]byte(nil), value...) }
