// Package localcorpus defines the privacy-sensitive write contract for an
// optional local discovery index. It deliberately requires an affirmative
// indexing disposition; an omitted or unknown value is never treated as
// consent to persist content.
package localcorpus

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"golang.org/x/net/html"
)

// RetentionMode defines who owns corpus admission and how long data lives.
// The zero value is disabled so adding a corpus dependency never opts an
// operator into persistence accidentally.
type RetentionMode string

const (
	ModeDisabled  RetentionMode = "disabled"
	ModeEphemeral RetentionMode = "ephemeral"
	ModePersonal  RetentionMode = "personal"
	ModeCurated   RetentionMode = "curated"
	ModeArchive   RetentionMode = "archive"

	DefaultPersonalMaxAge = 30 * 24 * time.Hour
)

// ParseRetentionMode accepts only the documented privacy modes.
func ParseRetentionMode(value string) (RetentionMode, error) {
	mode := RetentionMode(strings.ToLower(strings.TrimSpace(value)))
	if mode == "" {
		mode = ModeDisabled
	}
	switch mode {
	case ModeDisabled, ModeEphemeral, ModePersonal, ModeCurated, ModeArchive:
		return mode, nil
	default:
		return "", fmt.Errorf("unknown local corpus retention mode %q", value)
	}
}

// Policy applies automatic request-path retention. Curated mode deliberately
// declines new permitted content because admission belongs to an
// operator-controlled ingestion path, while still accepting revocations for
// content already present. Archive means no age-based expiry; immutable
// version history is owned by the separate artifact store.
type Policy struct {
	Mode   RetentionMode
	MaxAge time.Duration
}

// ApplyAutomatic returns the document to reconcile and whether the request
// path is allowed to do so. Callers must not interpret false as permission to
// delete or replace existing state.
func (policy Policy) ApplyAutomatic(document Document) (Document, bool) {
	mode := policy.Mode
	if mode == "" {
		mode = ModeDisabled
	}
	if mode == ModeDisabled {
		return document, false
	}
	if document.IndexingDisposition != DispositionPermitted {
		document.ExpiresAt = nil
		return document, true
	}
	if mode == ModeCurated {
		return document, false
	}
	if mode == ModePersonal {
		maxAge := policy.MaxAge
		if maxAge <= 0 {
			maxAge = DefaultPersonalMaxAge
		}
		observedAt := document.FetchedAt.UTC()
		if observedAt.IsZero() {
			observedAt = time.Now().UTC()
		}
		expiresAt := observedAt.Add(maxAge)
		document.ExpiresAt = &expiresAt
	} else {
		document.ExpiresAt = nil
	}
	return document, true
}

// ApplyExplicit applies operator-controlled corpus admission. Only curated
// mode accepts new permitted content through this path, and curated documents
// do not expire automatically. Enabled modes still accept non-permitted
// observations so an explicit ingestion path cannot preserve stale content
// after a robots, noindex, noarchive, or takedown decision.
func (policy Policy) ApplyExplicit(document Document) (Document, bool) {
	mode := policy.Mode
	if mode == "" {
		mode = ModeDisabled
	}
	if mode == ModeDisabled {
		return document, false
	}
	if document.IndexingDisposition != DispositionPermitted {
		document.ExpiresAt = nil
		return document, true
	}
	if mode != ModeCurated {
		return document, false
	}
	document.ExpiresAt = nil
	return document, true
}

// IndexingDisposition records why a document may or may not be retained.
type IndexingDisposition string

const (
	DispositionPermitted         IndexingDisposition = "permitted"
	DispositionRobotsBlocked     IndexingDisposition = "robots_blocked"
	DispositionNoIndexHeader     IndexingDisposition = "noindex_header"
	DispositionNoIndexMetadata   IndexingDisposition = "noindex_metadata"
	DispositionNoArchiveHeader   IndexingDisposition = "noarchive_header"
	DispositionNoArchiveMetadata IndexingDisposition = "noarchive_metadata"
	DispositionTakedown          IndexingDisposition = "takedown"
	DispositionUnknown           IndexingDisposition = "unknown"
)

// SafetyClassification is an explicit operator/source assertion, never an
// inference from page text. Empty and unclassified are both fail-closed for
// moderate/strict safe-search queries.
type SafetyClassification string

const (
	SafetyUnclassified SafetyClassification = "unclassified"
	SafetySafe         SafetyClassification = "safe"
	SafetyUnsafe       SafetyClassification = "unsafe"
)

func ParseSafetyClassification(value string) (SafetyClassification, error) {
	classification := SafetyClassification(strings.ToLower(strings.TrimSpace(value)))
	if classification == "" {
		classification = SafetyUnclassified
	}
	switch classification {
	case SafetyUnclassified, SafetySafe, SafetyUnsafe:
		return classification, nil
	default:
		return "", fmt.Errorf("unknown safety classification %q", value)
	}
}

// Document is the provider-independent subset retained by the discovery
// index. Full immutable artifacts and URL-version pointers remain separate so
// this lightweight index can be deleted or rebuilt without losing cache data.
type Document struct {
	URL                  string
	Title                string
	Headings             []string
	Body                 string
	Language             string
	Author               string
	PublishedAt          *time.Time
	FetchedAt            time.Time
	ExpiresAt            *time.Time
	ContentHash          string
	OutboundLinks        []string
	Provenance           []string
	MIME                 string
	ExtractionStatus     string
	SafetyClassification SafetyClassification
	IndexingDisposition  IndexingDisposition
}

// Writer reconciles timestamped observations into an opt-in local corpus.
// Non-permitted documents are tombstones, not errors, so a newer noindex or
// robots decision cannot be lost behind a stale permitted fetch.
type Writer interface {
	Reconcile(context.Context, Document) error
	Delete(context.Context, string) error
}

// EvaluateDisposition combines every retention control known for one
// retrieval. Permission is affirmative only when robots.txt was evaluated
// authoritatively and neither an applicable response header nor HTML metadata
// contains noindex (or its equivalent "none" directive).
func EvaluateDisposition(robotsAllowed, robotsAuthoritative bool, xRobotsTag []string, rawHTML []byte, userAgent string) IndexingDisposition {
	if robotsAuthoritative && !robotsAllowed {
		return DispositionRobotsBlocked
	}
	metadata, metadataErr := inspectApplicableMetadataStrict(rawHTML, userAgent)
	if directivesContainNoIndex(xRobotsTag, userAgent, true) {
		return DispositionNoIndexHeader
	}
	if metadata.noIndex {
		return DispositionNoIndexMetadata
	}
	if directivesContainNoArchive(xRobotsTag, userAgent, true) {
		return DispositionNoArchiveHeader
	}
	if metadata.noArchive {
		return DispositionNoArchiveMetadata
	}
	if metadataErr != nil {
		return DispositionUnknown
	}
	if !robotsAuthoritative {
		return DispositionUnknown
	}
	if !robotsAllowed {
		return DispositionUnknown
	}
	return DispositionPermitted
}

// EvaluateIndexability applies only noindex (including the equivalent none
// directive) from X-Robots-Tag and HTML robots metadata. It deliberately does
// not treat noarchive as noindex, which lets metadata-only pack publication
// avoid retaining a representation while still honoring indexing controls.
func EvaluateIndexability(xRobotsTag []string, rawHTML []byte, userAgent string) IndexingDisposition {
	disposition, _, err := EvaluateIndexabilityEvidenceStrict(xRobotsTag, rawHTML, userAgent)
	if err != nil {
		return DispositionUnknown
	}
	return disposition
}

// EvaluateIndexabilityEvidence also returns the exact content values from
// applicable HTML robots meta elements. Callers can retain these bounded
// derived controls without retaining the complete page representation.
func EvaluateIndexabilityEvidence(xRobotsTag []string, rawHTML []byte, userAgent string) (IndexingDisposition, []string) {
	disposition, values, err := EvaluateIndexabilityEvidenceStrict(xRobotsTag, rawHTML, userAgent)
	if err != nil {
		return DispositionUnknown, nil
	}
	return disposition, values
}

// EvaluateIndexabilityEvidenceStrict is the fail-closed form used when a
// positive indexing decision will authorize publication. A malformed HTML
// representation is uncertainty, not evidence that robots metadata is absent.
func EvaluateIndexabilityEvidenceStrict(xRobotsTag []string, rawHTML []byte, userAgent string) (IndexingDisposition, []string, error) {
	metadata, err := inspectApplicableMetadataStrict(rawHTML, userAgent)
	if err != nil {
		return DispositionUnknown, nil, err
	}
	if directivesContainNoIndex(xRobotsTag, userAgent, true) {
		return DispositionNoIndexHeader, metadata.values, nil
	}
	if metadata.noIndex {
		return DispositionNoIndexMetadata, metadata.values, nil
	}
	return DispositionPermitted, metadata.values, nil
}

// EvaluateDerivedIndexabilityEvidence applies the same directive semantics to
// the bounded, already-derived controls retained by an admission collector.
// Metadata values must be the exact content values from applicable robots meta
// elements; the function cannot prove that extraction without the page bytes.
func EvaluateDerivedIndexabilityEvidence(xRobotsTag, metadataRobots []string, userAgent string) IndexingDisposition {
	if directivesContainNoIndex(xRobotsTag, userAgent, true) {
		return DispositionNoIndexHeader
	}
	if directivesContainNoIndex(metadataRobots, "", false) {
		return DispositionNoIndexMetadata
	}
	return DispositionPermitted
}

// AllowsLinkFollowing applies applicable X-Robots-Tag and HTML robots metadata
// to crawl discovery. It is independent of retention: an indexable page may
// still ask crawlers not to follow its links.
func AllowsLinkFollowing(xRobotsTag []string, rawHTML []byte, userAgent string) bool {
	if directivesContain(xRobotsTag, userAgent, true, "nofollow", "none") {
		return false
	}
	blocked, err := metadataContainsDirectiveStrict(rawHTML, userAgent, "nofollow", "none")
	return err == nil && !blocked
}

func metadataContainsDirective(rawHTML []byte, userAgent string, targets ...string) bool {
	contains, _ := metadataContainsDirectiveStrict(rawHTML, userAgent, targets...)
	return contains
}

func metadataContainsDirectiveStrict(rawHTML []byte, userAgent string, targets ...string) (bool, error) {
	if len(rawHTML) == 0 {
		return false, nil
	}
	document, err := html.Parse(bytes.NewReader(rawHTML))
	if err != nil {
		return false, err
	}
	agent := agentToken(userAgent)
	var visit func(*html.Node) bool
	visit = func(node *html.Node) bool {
		if node.Type == html.ElementNode && strings.EqualFold(node.Data, "meta") {
			var name, content string
			for _, attribute := range node.Attr {
				switch strings.ToLower(attribute.Key) {
				case "name":
					name = strings.ToLower(strings.TrimSpace(attribute.Val))
				case "content":
					content = attribute.Val
				}
			}
			if (name == "robots" || agent != "" && name == agent) && directivesContain([]string{content}, "", false, targets...) {
				return true
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			if visit(child) {
				return true
			}
		}
		return false
	}
	return visit(document), nil
}

func metadataContainsNoIndex(rawHTML []byte, userAgent string) bool {
	return inspectApplicableMetadata(rawHTML, userAgent).noIndex
}

type applicableMetadata struct {
	values    []string
	noIndex   bool
	noArchive bool
}

func inspectApplicableMetadata(rawHTML []byte, userAgent string) applicableMetadata {
	evidence, _ := inspectApplicableMetadataStrict(rawHTML, userAgent)
	return evidence
}

func inspectApplicableMetadataStrict(rawHTML []byte, userAgent string) (applicableMetadata, error) {
	if len(rawHTML) == 0 {
		return applicableMetadata{}, nil
	}
	document, err := html.Parse(bytes.NewReader(rawHTML))
	if err != nil {
		return applicableMetadata{}, err
	}
	agent := agentToken(userAgent)
	var evidence applicableMetadata
	var visit func(*html.Node)
	visit = func(node *html.Node) {
		if node.Type == html.ElementNode && strings.EqualFold(node.Data, "meta") {
			var name, content string
			for _, attribute := range node.Attr {
				switch strings.ToLower(attribute.Key) {
				case "name":
					name = strings.ToLower(strings.TrimSpace(attribute.Val))
				case "content":
					content = attribute.Val
				}
			}
			if name == "robots" || (agent != "" && name == agent) {
				evidence.values = append(evidence.values, content)
				if directivesContainNoIndex([]string{content}, "", false) {
					evidence.noIndex = true
				}
				if directivesContainNoArchive([]string{content}, "", false) {
					evidence.noArchive = true
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			visit(child)
		}
	}
	visit(document)
	return evidence, nil
}

func metadataDisposition(rawHTML []byte, userAgent string) IndexingDisposition {
	evidence := inspectApplicableMetadata(rawHTML, userAgent)
	if evidence.noIndex {
		return DispositionNoIndexMetadata
	}
	if evidence.noArchive {
		return DispositionNoArchiveMetadata
	}
	return ""
}

func directivesContainNoIndex(values []string, userAgent string, allowScopes bool) bool {
	return directivesContain(values, userAgent, allowScopes, "noindex", "none")
}

func directivesContainNoArchive(values []string, userAgent string, allowScopes bool) bool {
	return directivesContain(values, userAgent, allowScopes, "noarchive", "none")
}

func directivesContain(values []string, userAgent string, allowScopes bool, targets ...string) bool {
	agent := agentToken(userAgent)
	for _, value := range values {
		scope := ""
		for _, segment := range strings.Split(value, ",") {
			segment = strings.TrimSpace(segment)
			if allowScopes {
				if before, after, found := strings.Cut(segment, ":"); found {
					candidate := strings.ToLower(strings.TrimSpace(before))
					if candidate != "" && !strings.ContainsAny(candidate, " \t") {
						scope = candidate
						segment = strings.TrimSpace(after)
					}
				}
			}
			if scope != "" && scope != "*" && scope != agent {
				continue
			}
			if containsDirective(segment, targets...) {
				return true
			}
		}
	}
	return false
}

func containsNoIndexDirective(value string) bool {
	return containsDirective(value, "noindex", "none")
}

func containsDirective(value string, targets ...string) bool {
	for _, directive := range strings.FieldsFunc(strings.ToLower(value), func(r rune) bool {
		return r == ',' || r == ';' || r == ' ' || r == '\t' || r == '\r' || r == '\n'
	}) {
		for _, target := range targets {
			if directive == target {
				return true
			}
		}
	}
	return false
}

func agentToken(userAgent string) string {
	token := strings.ToLower(strings.TrimSpace(userAgent))
	if index := strings.IndexAny(token, "/ \t"); index >= 0 {
		token = token[:index]
	}
	return token
}
