package crawler

import (
	"errors"
	"strings"
	"testing"
)

func TestParseDiscoveryDocumentExtractsCanonicalSitemapPages(t *testing.T) {
	raw := []byte(`<?xml version="1.0"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://EXAMPLE.com:443/docs/a?utm_source=feed#part</loc><lastmod>2026-07-17</lastmod></url>
  <url><loc>https://example.com/docs/a</loc></url>
  <url><loc>https://example.com/docs/b</loc></url>
</urlset>`)
	document, err := ParseDiscoveryDocument(raw, "https://example.com/sitemap.xml", KindSitemap, Limits{MaxDocumentBytes: 1 << 20, MaxEntries: 10, MaxURLBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	if document.Kind != KindSitemap || len(document.Pages) != 2 || document.Pages[0].URL != "https://example.com/docs/a" || document.Pages[1].URL != "https://example.com/docs/b" {
		t.Fatalf("pages=%+v", document.Pages)
	}
	if document.Pages[0].LastModified == nil || document.Pages[0].LastModified.Format("2006-01-02") != "2026-07-17" {
		t.Fatalf("last modified=%v", document.Pages[0].LastModified)
	}
	if len(document.Sources) != 0 {
		t.Fatalf("sources=%+v", document.Sources)
	}
}

func TestParseDiscoveryDocumentExtractsSitemapIndexSources(t *testing.T) {
	raw := []byte(`<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>/sitemaps/one.xml.gz</loc></sitemap>
  <sitemap><loc>https://example.com/sitemaps/two.xml</loc></sitemap>
</sitemapindex>`)
	document, err := ParseDiscoveryDocument(raw, "https://example.com/root.xml", KindAuto, Limits{MaxDocumentBytes: 1 << 20, MaxEntries: 10, MaxURLBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	if len(document.Sources) != 2 || document.Sources[0].URL != "https://example.com/sitemaps/one.xml.gz" || document.Sources[0].Kind != KindSitemap || document.Sources[1].URL != "https://example.com/sitemaps/two.xml" {
		t.Fatalf("sources=%+v", document.Sources)
	}
}

func TestParseDiscoveryDocumentExtractsRSSAndAtomLinks(t *testing.T) {
	tests := []struct {
		name   string
		source string
		raw    string
		want   []string
	}{
		{
			name: "rss", source: "https://example.com/feed.xml",
			raw: `<rss version="2.0"><channel>
  <item><link>https://example.com/posts/one</link></item>
  <item><link>/posts/two#fragment</link></item>
</channel></rss>`,
			want: []string{"https://example.com/posts/one", "https://example.com/posts/two"},
		},
		{
			name: "atom", source: "https://example.com/feeds/atom.xml",
			raw: `<feed xmlns="http://www.w3.org/2005/Atom" xml:base="https://example.com/">
  <entry><link rel="self" href="/feeds/entry-one"/><link rel="alternate" href="posts/one"/></entry>
  <entry><link href="posts/two"/></entry>
</feed>`,
			want: []string{"https://example.com/posts/one", "https://example.com/posts/two"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document, err := ParseDiscoveryDocument([]byte(test.raw), test.source, KindFeed, Limits{MaxDocumentBytes: 1 << 20, MaxEntries: 10, MaxURLBytes: 2048})
			if err != nil {
				t.Fatal(err)
			}
			if len(document.Pages) != len(test.want) {
				t.Fatalf("pages=%+v", document.Pages)
			}
			for index, want := range test.want {
				if document.Pages[index].URL != want {
					t.Fatalf("pages[%d]=%q want %q", index, document.Pages[index].URL, want)
				}
			}
		})
	}
}

func TestParseDiscoveryDocumentFailsClosedOnBoundsAndMalformedXML(t *testing.T) {
	tests := []struct {
		name   string
		raw    string
		limits Limits
		want   error
	}{
		{name: "document bytes", raw: `<urlset/>`, limits: Limits{MaxDocumentBytes: 4, MaxEntries: 10, MaxURLBytes: 2048}, want: ErrDocumentTooLarge},
		{name: "entries", raw: `<urlset><url><loc>https://example.com/a</loc></url><url><loc>https://example.com/b</loc></url></urlset>`, limits: Limits{MaxDocumentBytes: 1 << 20, MaxEntries: 1, MaxURLBytes: 2048}, want: ErrEntryLimit},
		{name: "url bytes", raw: `<urlset><url><loc>https://example.com/too-long</loc></url></urlset>`, limits: Limits{MaxDocumentBytes: 1 << 20, MaxEntries: 10, MaxURLBytes: 8}, want: ErrURLTooLarge},
		{name: "nesting", raw: `<urlset><a><b><c/></b></a></urlset>`, limits: Limits{MaxDocumentBytes: 1 << 20, MaxEntries: 10, MaxURLBytes: 2048, MaxDepth: 3}, want: ErrNestingTooDeep},
		{name: "malformed", raw: `<urlset><url>`, limits: Limits{MaxDocumentBytes: 1 << 20, MaxEntries: 10, MaxURLBytes: 2048}, want: ErrMalformedDocument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseDiscoveryDocument([]byte(test.raw), "https://example.com/source.xml", KindAuto, test.limits)
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want %v", err, test.want)
			}
		})
	}
}

func TestParseDiscoveryDocumentEntryLimitCountsInvalidAndEmptyRecords(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
		kind DiscoveryKind
	}{
		{name: "sitemap", raw: `<urlset><url><loc></loc></url><url><loc>ftp://example.com/no</loc></url></urlset>`, kind: KindSitemap},
		{name: "rss", raw: `<rss><channel><item><link></link></item><item><link>mailto:no@example.com</link></item></channel></rss>`, kind: KindFeed},
		{name: "atom", raw: `<feed xmlns="http://www.w3.org/2005/Atom"><entry></entry><entry><link rel="self" href="https://example.com/no"/></entry></feed>`, kind: KindFeed},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseDiscoveryDocument([]byte(test.raw), "https://example.com/source.xml", test.kind, Limits{
				MaxDocumentBytes: 1 << 20, MaxEntries: 1, MaxURLBytes: 2048,
			})
			if !errors.Is(err, ErrEntryLimit) {
				t.Fatalf("error = %v, want %v", err, ErrEntryLimit)
			}
		})
	}
}

func TestParseDiscoveryDocumentRejectsAtomLinkFanoutBeforeUnmarshal(t *testing.T) {
	raw := `<feed xmlns="http://www.w3.org/2005/Atom"><entry>` + strings.Repeat(`<link rel="self" href="https://example.com/no"/>`, maxAtomLinksPerEntry+1) + `</entry></feed>`
	_, err := ParseDiscoveryDocument([]byte(raw), "https://example.com/feed.xml", KindFeed, Limits{
		MaxDocumentBytes: 1 << 20, MaxEntries: 10, MaxURLBytes: 2048,
	})
	if !errors.Is(err, ErrLinkLimit) {
		t.Fatalf("error = %v, want %v", err, ErrLinkLimit)
	}
}

func TestParseDiscoveryDocumentRejectsNonWebAndCredentialedURLs(t *testing.T) {
	raw := []byte(`<urlset>
  <url><loc>file:///etc/passwd</loc></url>
  <url><loc>https://user:secret@example.com/private</loc></url>
  <url><loc>https://example.com/allowed</loc></url>
</urlset>`)
	document, err := ParseDiscoveryDocument(raw, "https://example.com/sitemap.xml", KindAuto, Limits{MaxDocumentBytes: 1 << 20, MaxEntries: 10, MaxURLBytes: 2048})
	if err != nil {
		t.Fatal(err)
	}
	if len(document.Pages) != 1 || document.Pages[0].URL != "https://example.com/allowed" {
		t.Fatalf("pages=%+v", document.Pages)
	}
}
