// Package crawler contains the bounded discovery primitives used by the
// optional Fetchmark crawler. It deliberately has no network or storage access.
package crawler

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
)

var (
	ErrDocumentTooLarge    = errors.New("crawler: discovery document exceeds byte limit")
	ErrEntryLimit          = errors.New("crawler: discovery document exceeds entry limit")
	ErrURLTooLarge         = errors.New("crawler: discovered URL exceeds byte limit")
	ErrNestingTooDeep      = errors.New("crawler: discovery XML exceeds nesting limit")
	ErrLinkLimit           = errors.New("crawler: Atom entry exceeds link limit")
	ErrMalformedDocument   = errors.New("crawler: malformed discovery document")
	ErrUnsupportedDocument = errors.New("crawler: unsupported discovery document")
)

const maxAtomLinksPerEntry = 32

type DiscoveryKind string

const (
	KindAuto    DiscoveryKind = "auto"
	KindSitemap DiscoveryKind = "sitemap"
	KindFeed    DiscoveryKind = "feed"
)

type Limits struct {
	MaxDocumentBytes int
	MaxEntries       int
	MaxURLBytes      int
	MaxDepth         int
}

type DiscoveredPage struct {
	URL          string
	LastModified *time.Time
}

type DiscoveredSource struct {
	URL  string
	Kind DiscoveryKind
}

type DiscoveryDocument struct {
	Kind    DiscoveryKind
	Pages   []DiscoveredPage
	Sources []DiscoveredSource
}

type xmlLocation struct {
	Location     string `xml:"loc"`
	LastModified string `xml:"lastmod"`
}

type xmlSitemapDocument struct {
	URLs     []xmlLocation `xml:"url"`
	Sitemaps []xmlLocation `xml:"sitemap"`
}

type xmlRSSDocument struct {
	Items []struct {
		Link string `xml:"link"`
	} `xml:"channel>item"`
}

type xmlAtomLink struct {
	Href string `xml:"href,attr"`
	Rel  string `xml:"rel,attr"`
}

type xmlAtomDocument struct {
	Base    string `xml:"http://www.w3.org/XML/1998/namespace base,attr"`
	Entries []struct {
		Base  string        `xml:"http://www.w3.org/XML/1998/namespace base,attr"`
		Links []xmlAtomLink `xml:"link"`
	} `xml:"entry"`
}

// ParseDiscoveryDocument parses one already-fetched sitemap, RSS document, or
// Atom document. Fetching, decompression, content-type checks, and robots policy
// remain the responsibility of the crawler runner.
func ParseDiscoveryDocument(raw []byte, sourceURL string, requested DiscoveryKind, limits Limits) (DiscoveryDocument, error) {
	if limits.MaxDocumentBytes <= 0 || len(raw) > limits.MaxDocumentBytes {
		return DiscoveryDocument{}, ErrDocumentTooLarge
	}
	if limits.MaxEntries <= 0 {
		return DiscoveryDocument{}, ErrEntryLimit
	}
	if limits.MaxURLBytes <= 0 {
		return DiscoveryDocument{}, ErrURLTooLarge
	}
	if limits.MaxDepth <= 0 {
		limits.MaxDepth = 128
	}
	if len(sourceURL) > limits.MaxURLBytes {
		return DiscoveryDocument{}, ErrURLTooLarge
	}

	base, err := parseWebURL(sourceURL)
	if err != nil {
		return DiscoveryDocument{}, fmt.Errorf("crawler: invalid discovery source URL: %w", err)
	}
	root, err := inspectXML(raw, limits.MaxDepth, limits.MaxEntries)
	if err != nil {
		return DiscoveryDocument{}, err
	}

	detected, err := kindForRoot(root)
	if err != nil {
		return DiscoveryDocument{}, err
	}
	if requested != KindAuto && requested != detected {
		return DiscoveryDocument{}, fmt.Errorf("%w: expected %s, found %s", ErrUnsupportedDocument, requested, detected)
	}

	builder := discoveryBuilder{
		base: base, limits: limits, seen: make(map[string]struct{}),
		document: DiscoveryDocument{Kind: detected},
	}
	switch strings.ToLower(root) {
	case "urlset", "sitemapindex":
		var document xmlSitemapDocument
		if err := xml.Unmarshal(raw, &document); err != nil {
			return DiscoveryDocument{}, fmt.Errorf("%w: %v", ErrMalformedDocument, err)
		}
		if strings.EqualFold(root, "urlset") {
			for _, entry := range document.URLs {
				if err := builder.countRecord(); err != nil {
					return DiscoveryDocument{}, err
				}
				if err := builder.addPage(entry.Location, base, parseLastModified(entry.LastModified)); err != nil {
					return DiscoveryDocument{}, err
				}
			}
		} else {
			for _, entry := range document.Sitemaps {
				if err := builder.countRecord(); err != nil {
					return DiscoveryDocument{}, err
				}
				if err := builder.addSource(entry.Location, base); err != nil {
					return DiscoveryDocument{}, err
				}
			}
		}
	case "rss":
		var document xmlRSSDocument
		if err := xml.Unmarshal(raw, &document); err != nil {
			return DiscoveryDocument{}, fmt.Errorf("%w: %v", ErrMalformedDocument, err)
		}
		for _, item := range document.Items {
			if err := builder.countRecord(); err != nil {
				return DiscoveryDocument{}, err
			}
			if err := builder.addPage(item.Link, base, nil); err != nil {
				return DiscoveryDocument{}, err
			}
		}
	case "feed":
		var document xmlAtomDocument
		if err := xml.Unmarshal(raw, &document); err != nil {
			return DiscoveryDocument{}, fmt.Errorf("%w: %v", ErrMalformedDocument, err)
		}
		feedBase, err := resolveOptionalBase(base, document.Base)
		if err != nil {
			return DiscoveryDocument{}, fmt.Errorf("crawler: invalid Atom base URL: %w", err)
		}
		for _, entry := range document.Entries {
			if err := builder.countRecord(); err != nil {
				return DiscoveryDocument{}, err
			}
			entryBase, err := resolveOptionalBase(feedBase, entry.Base)
			if err != nil {
				continue
			}
			for _, link := range entry.Links {
				if link.Rel != "" && !strings.EqualFold(link.Rel, "alternate") {
					continue
				}
				if err := builder.addPage(link.Href, entryBase, nil); err != nil {
					return DiscoveryDocument{}, err
				}
				break
			}
		}
	}
	return builder.document, nil
}

type discoveryBuilder struct {
	base     *url.URL
	limits   Limits
	entries  int
	seen     map[string]struct{}
	document DiscoveryDocument
}

func (b *discoveryBuilder) countRecord() error {
	b.entries++
	if b.entries > b.limits.MaxEntries {
		return ErrEntryLimit
	}
	return nil
}

func (b *discoveryBuilder) addPage(raw string, base *url.URL, modified *time.Time) error {
	canonical, accepted, err := b.canonical(raw, base)
	if err != nil || !accepted {
		return err
	}
	if _, exists := b.seen["page\x00"+canonical]; exists {
		return nil
	}
	b.seen["page\x00"+canonical] = struct{}{}
	b.document.Pages = append(b.document.Pages, DiscoveredPage{URL: canonical, LastModified: modified})
	return nil
}

func (b *discoveryBuilder) addSource(raw string, base *url.URL) error {
	canonical, accepted, err := b.canonical(raw, base)
	if err != nil || !accepted {
		return err
	}
	if _, exists := b.seen["source\x00"+canonical]; exists {
		return nil
	}
	b.seen["source\x00"+canonical] = struct{}{}
	b.document.Sources = append(b.document.Sources, DiscoveredSource{URL: canonical, Kind: KindSitemap})
	return nil
}

func (b *discoveryBuilder) canonical(raw string, base *url.URL) (string, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", false, nil
	}
	if len(raw) > b.limits.MaxURLBytes {
		return "", false, ErrURLTooLarge
	}
	reference, err := url.Parse(raw)
	if err != nil {
		return "", false, nil
	}
	resolved := base.ResolveReference(reference)
	if resolved.Scheme != "http" && resolved.Scheme != "https" || resolved.Host == "" || resolved.User != nil {
		return "", false, nil
	}
	if len(resolved.String()) > b.limits.MaxURLBytes {
		return "", false, ErrURLTooLarge
	}
	canonical, err := cache.CanonicalURL(resolved.String())
	if err != nil {
		return "", false, nil
	}
	return canonical, true, nil
}

func kindForRoot(root string) (DiscoveryKind, error) {
	switch strings.ToLower(root) {
	case "urlset", "sitemapindex":
		return KindSitemap, nil
	case "rss", "feed":
		return KindFeed, nil
	default:
		return "", fmt.Errorf("%w: root element %q", ErrUnsupportedDocument, root)
	}
}

func inspectXML(raw []byte, maxDepth, maxEntries int) (string, error) {
	decoder := xml.NewDecoder(bytes.NewReader(raw))
	decoder.Strict = true
	depth := 0
	root := ""
	records := 0
	atomEntryDepth := 0
	atomLinks := 0
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("%w: %v", ErrMalformedDocument, err)
		}
		switch token := token.(type) {
		case xml.StartElement:
			depth++
			if depth > maxDepth {
				return "", ErrNestingTooDeep
			}
			if root == "" {
				root = token.Name.Local
			}
			rootKind := strings.ToLower(root)
			name := strings.ToLower(token.Name.Local)
			isRecord := depth > 1 && ((rootKind == "urlset" && name == "url") ||
				(rootKind == "sitemapindex" && name == "sitemap") ||
				(rootKind == "rss" && name == "item") || (rootKind == "feed" && name == "entry"))
			if isRecord {
				records++
				if records > maxEntries {
					return "", ErrEntryLimit
				}
			}
			if rootKind == "feed" && name == "entry" {
				atomEntryDepth = depth
				atomLinks = 0
			} else if atomEntryDepth > 0 && name == "link" {
				atomLinks++
				if atomLinks > maxAtomLinksPerEntry {
					return "", ErrLinkLimit
				}
			}
		case xml.EndElement:
			if atomEntryDepth == depth && strings.EqualFold(token.Name.Local, "entry") {
				atomEntryDepth = 0
				atomLinks = 0
			}
			depth--
		}
	}
	if root == "" || depth != 0 {
		return "", ErrMalformedDocument
	}
	return root, nil
}

func parseWebURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return nil, errors.New("URL must be absolute HTTP(S) without credentials")
	}
	return parsed, nil
}

func resolveOptionalBase(base *url.URL, raw string) (*url.URL, error) {
	if strings.TrimSpace(raw) == "" {
		return base, nil
	}
	reference, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, err
	}
	resolved := base.ResolveReference(reference)
	if resolved.Scheme != "http" && resolved.Scheme != "https" || resolved.Host == "" || resolved.User != nil {
		return nil, errors.New("base must resolve to HTTP(S) without credentials")
	}
	return resolved, nil
}

func parseLastModified(raw string) *time.Time {
	value := strings.TrimSpace(raw)
	for _, layout := range []string{time.RFC3339, "2006-01-02"} {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			utc := parsed.UTC()
			return &utc
		}
	}
	return nil
}
