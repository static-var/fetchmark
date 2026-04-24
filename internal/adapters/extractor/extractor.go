// Package extractor turns raw HTML bytes into a structured Content
// (metadata + plain text + cleaned HTML + Markdown) using go-trafilatura
// as the primary reader-view algorithm.
//
// It also applies a deterministic heuristic to flag pages whose primary
// content depends on JavaScript — v1 does not render JS, so we surface
// that signal rather than silently returning an empty extraction.
package extractor

import (
	"bytes"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/staticvar/fetchmark/internal/core/model"

	htmltomarkdown "github.com/JohannesKaufmann/html-to-markdown/v2"
	"github.com/markusmobius/go-trafilatura"
	"golang.org/x/net/html"
)

// Extractor runs the readability + MD pipeline.
type Extractor struct {
	fallback bool
}

// New constructs an Extractor. enableFallback enables trafilatura's
// Readability/DomDistiller fallbacks (slower, better recall).
func New(enableFallback bool) *Extractor {
	return &Extractor{fallback: enableFallback}
}

// ReasonJSRequired is attached to Content.UnsupportedReason when the
// page likely needs JS to render primary content.
const ReasonJSRequired = "js_required"

const (
	jsHeuristicMinText      = 200
	jsHeuristicScriptRatio  = 0.5
	jsHeuristicMinHTMLBytes = 1024
)

var scriptRE = regexp.MustCompile(`(?is)<script\b[^>]*>.*?</script\s*>`)

// Extract runs the pipeline. pageURL is used to resolve relative links
// when building metadata; may be empty.
func (e *Extractor) Extract(rawHTML []byte, pageURL string) (*model.Content, error) {
	if len(rawHTML) == 0 {
		return nil, fmt.Errorf("extractor: empty html")
	}

	var opts trafilatura.Options
	opts.EnableFallback = e.fallback
	opts.IncludeLinks = true
	opts.IncludeImages = false
	if pageURL != "" {
		if u, err := url.Parse(pageURL); err == nil {
			opts.OriginalURL = u
		}
	}

	result, err := trafilatura.Extract(bytes.NewReader(rawHTML), opts)
	if err != nil {
		// Trafilatura returns "text and comments are not long enough"
		// when it couldn't identify primary content. That is a
		// non-fatal outcome for us — the JS-required heuristic may
		// still fire, and callers can decide what to do with empty
		// content.
		if strings.Contains(err.Error(), "not long enough") {
			c := &model.Content{URL: pageURL}
			if looksJSRequired(rawHTML, "") {
				c.UnsupportedReason = ReasonJSRequired
			}
			return c, nil
		}
		return nil, fmt.Errorf("extractor: trafilatura: %w", err)
	}

	c := &model.Content{
		URL:         pageURL,
		Title:       result.Metadata.Title,
		Author:      result.Metadata.Author,
		SiteName:    result.Metadata.Sitename,
		Description: result.Metadata.Description,
		Language:    result.Metadata.Language,
		MainText:    strings.TrimSpace(result.ContentText),
	}
	if !result.Metadata.Date.IsZero() {
		c.PublishedAt = &result.Metadata.Date
	}

	if result.ContentNode != nil {
		var buf bytes.Buffer
		if err := html.Render(&buf, result.ContentNode); err == nil {
			c.CleanedHTML = buf.String()
		}
		if md, ok := safeConvertMarkdown(result.ContentNode); ok {
			c.Markdown = strings.TrimSpace(md)
		}
	}

	if looksJSRequired(rawHTML, c.MainText) {
		c.UnsupportedReason = ReasonJSRequired
	}

	return c, nil
}

// safeConvertMarkdown wraps html-to-markdown/v2's ConvertNode in a
// defer/recover because the upstream "collapse" pass has been observed
// to panic with "index out of range" on some real-world pages
// (see collapse/collapse.go:125). A panic on one page must not fail
// the whole batch — we just return (_, false) and the caller falls
// back to the cleaned HTML + plain text we already have.
func safeConvertMarkdown(node *html.Node) (md string, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			md = ""
			ok = false
		}
	}()
	b, err := htmltomarkdown.ConvertNode(node)
	if err != nil {
		return "", false
	}
	return string(b), true
}

// looksJSRequired implements the heuristic documented in the plan:
// main text <200 chars AND <script> byte share >50% of raw bytes,
// for pages large enough that the ratio is meaningful.
func looksJSRequired(raw []byte, mainText string) bool {
	if len(raw) < jsHeuristicMinHTMLBytes {
		return false
	}
	if len(strings.TrimSpace(mainText)) >= jsHeuristicMinText {
		return false
	}
	var scriptBytes int
	for _, m := range scriptRE.FindAllIndex(raw, -1) {
		scriptBytes += m[1] - m[0]
	}
	ratio := float64(scriptBytes) / float64(len(raw))
	return ratio > jsHeuristicScriptRatio
}
