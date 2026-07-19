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
	"unicode"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
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

	// Extracted list metadata is internal indexing input. Keep the combined
	// retained count (640) comfortably below Bleve's 4096 list-value limit,
	// and bound individual values before parsing or normalization.
	maxMetadataHeadings      = 128
	maxMetadataOutboundLinks = 512
	maxMetadataHeadingBytes  = 512
	maxMetadataURLBytes      = 2048
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
			// go-trafilatura currently joins relative hrefs to OriginalURL.Path
			// as though that path were a directory. Supplying the containing
			// directory preserves RFC 3986 resolution for ordinary page URLs;
			// Content.URL below still retains the full caller-visible page URL.
			u = trafilaturaBaseURL(u)
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
		c.Headings, c.OutboundLinks = collectContentMetadata(result.ContentNode, pageURL)

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

func trafilaturaBaseURL(pageURL *url.URL) *url.URL {
	if pageURL == nil {
		return nil
	}
	base := *pageURL
	base.RawQuery = ""
	base.ForceQuery = false
	base.Fragment = ""
	base.RawFragment = ""
	if !strings.HasSuffix(base.Path, "/") {
		if separator := strings.LastIndex(base.Path, "/"); separator >= 0 {
			base.Path = base.Path[:separator+1]
		} else {
			base.Path = "/"
		}
		base.RawPath = ""
	}
	if base.Path == "" {
		base.Path = "/"
	}
	return &base
}

// collectContentMetadata projects a bounded amount of indexing metadata from
// the reader-view content tree. It deliberately ignores the raw document so
// navigation and other discarded boilerplate do not enter the local corpus.
func collectContentMetadata(root *html.Node, pageURL string) ([]string, []string) {
	var baseURL *url.URL
	if len(pageURL) <= maxMetadataURLBytes {
		if parsed, err := url.Parse(pageURL); err == nil && isWebURL(parsed) {
			baseURL = parsed
		}
	}

	headings := make([]string, 0, 16)
	links := make([]string, 0, 32)
	seenLinks := make(map[string]struct{})

	var walk func(*html.Node) bool
	walk = func(node *html.Node) bool {
		if node.Type == html.ElementNode {
			if isNonVisibleNode(node) {
				return false
			}
			if len(headings) < maxMetadataHeadings && isHeadingElement(node.Data) {
				if text, ok := normalizedVisibleText(node); ok && text != "" {
					headings = append(headings, text)
				}
			}

			if len(links) < maxMetadataOutboundLinks && node.Data == "a" {
				if link, ok := resolvedWebLink(node, baseURL); ok && !anchorNoFollow(node) {
					if _, duplicate := seenLinks[link]; !duplicate {
						seenLinks[link] = struct{}{}
						links = append(links, link)
					}
				}
			}
		}

		if len(headings) == maxMetadataHeadings && len(links) == maxMetadataOutboundLinks {
			return true
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			if walk(child) {
				return true
			}
		}
		return false
	}
	walk(root)
	return headings, links
}

func anchorNoFollow(node *html.Node) bool {
	if node == nil {
		return false
	}
	for _, attribute := range node.Attr {
		if strings.EqualFold(attribute.Key, "rel") {
			for _, value := range strings.Fields(strings.ToLower(attribute.Val)) {
				if value == "nofollow" {
					return true
				}
			}
		}
	}
	return false
}

func isHeadingElement(name string) bool {
	return len(name) == 2 && name[0] == 'h' && name[1] >= '1' && name[1] <= '6'
}

func normalizedVisibleText(root *html.Node) (string, bool) {
	var text strings.Builder
	text.Grow(maxMetadataHeadingBytes)
	seenText := false
	pendingSpace := false
	oversized := false

	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if oversized {
			return
		}
		if node.Type == html.ElementNode && isNonVisibleNode(node) {
			return
		}
		if node.Type == html.TextNode {
			for _, r := range node.Data {
				if unicode.IsSpace(r) {
					pendingSpace = seenText
					continue
				}
				extraBytes := utf8.RuneLen(r)
				if pendingSpace {
					extraBytes++
				}
				if text.Len()+extraBytes > maxMetadataHeadingBytes {
					oversized = true
					return
				}
				if pendingSpace {
					text.WriteByte(' ')
				}
				text.WriteRune(r)
				seenText = true
				pendingSpace = false
			}
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	if oversized {
		return "", false
	}
	return text.String(), true
}

func isNonVisibleNode(node *html.Node) bool {
	switch node.Data {
	case "script", "style", "template", "noscript":
		return true
	}
	for _, attribute := range node.Attr {
		if attribute.Key == "hidden" ||
			(attribute.Key == "aria-hidden" && strings.EqualFold(strings.TrimSpace(attribute.Val), "true")) {
			return true
		}
	}
	return false
}

func resolvedWebLink(node *html.Node, baseURL *url.URL) (string, bool) {
	var rawHref string
	for _, attribute := range node.Attr {
		if attribute.Key == "href" {
			rawHref = strings.TrimSpace(attribute.Val)
			break
		}
	}
	if rawHref == "" || len(rawHref) > maxMetadataURLBytes {
		return "", false
	}

	parsed, err := url.Parse(rawHref)
	if err != nil {
		return "", false
	}
	if !parsed.IsAbs() {
		if baseURL == nil {
			return "", false
		}
		parsed = baseURL.ResolveReference(parsed)
	}
	if !isWebURL(parsed) {
		return "", false
	}
	parsed.Fragment = ""
	parsed.RawFragment = ""
	resolved, err := cache.CanonicalURL(parsed.String())
	if err != nil || resolved == "" || len(resolved) > maxMetadataURLBytes {
		return "", false
	}
	return resolved, true
}

func isWebURL(candidate *url.URL) bool {
	return candidate != nil && candidate.Hostname() != "" && candidate.User == nil &&
		(strings.EqualFold(candidate.Scheme, "http") || strings.EqualFold(candidate.Scheme, "https"))
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
