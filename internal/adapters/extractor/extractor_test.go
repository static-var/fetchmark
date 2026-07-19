package extractor

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/net/html"
)

const sampleArticle = `<!DOCTYPE html><html lang="en"><head>
<meta charset="utf-8">
<meta property="og:title" content="The Test Article">
<meta name="author" content="Jane Doe">
<meta property="og:site_name" content="Example News">
<title>The Test Article</title></head>
<body>
<header><nav>Home | About</nav></header>
<article>
<h1>The Test Article</h1>
<p class="byline">By Jane Doe</p>
<p>This is the first paragraph of an article that is long enough to be
considered primary content. It discusses multiple relevant topics and
includes a <a href="https://example.com/other">useful link</a>.</p>
<p>A second paragraph reinforces the main content so trafilatura
recognises this as the extraction target and not boilerplate. It needs
more than a handful of words so the heuristic fires correctly.</p>
<p>A third paragraph adds enough bulk that the extractor keeps the
article block. Modern readability algorithms require several paragraphs
to avoid false negatives on thin pages.</p>
</article>
<footer><p>Copyright stuff</p></footer>
</body></html>`

func TestExtract_BasicArticle(t *testing.T) {
	ex := New(true)
	c, err := ex.Extract([]byte(sampleArticle), "https://example.com/news/1")
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if c.Title == "" || !strings.Contains(c.Title, "Test Article") {
		t.Fatalf("title = %q", c.Title)
	}
	if !strings.Contains(c.MainText, "first paragraph") {
		t.Fatalf("main text missing content: %q", c.MainText)
	}
	if !strings.Contains(c.Markdown, "first paragraph") {
		t.Fatalf("markdown missing content: %q", c.Markdown)
	}
	if c.CleanedHTML == "" {
		t.Fatal("cleaned html is empty")
	}
	if c.UnsupportedReason != "" {
		t.Fatalf("unexpected unsupported: %q", c.UnsupportedReason)
	}
	if want := []string{"The Test Article"}; !reflect.DeepEqual(c.Headings, want) {
		t.Fatalf("headings = %#v, want %#v", c.Headings, want)
	}
	if want := []string{"https://example.com/other"}; !reflect.DeepEqual(c.OutboundLinks, want) {
		t.Fatalf("outbound links = %#v, want %#v", c.OutboundLinks, want)
	}

	encoded, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal content: %v", err)
	}
	if strings.Contains(string(encoded), "headings") || strings.Contains(string(encoded), "outbound_links") {
		t.Fatalf("internal metadata leaked into public JSON: %s", encoded)
	}
}

func TestCollectContentMetadata_OrderResolutionFilteringAndDedupe(t *testing.T) {
	node := parseMetadataFixture(t, `<article>
		<h2>  First <span> heading </span><span hidden> hidden text</span><span aria-hidden="true"> aria-hidden text</span> </h2>
		<p>
			<a href="../docs?q=go#install">relative</a>
			<a href="https://other.example/path#one">absolute</a>
			<a href="https://OTHER.example:443/path#two">canonical duplicate</a>
			<a href="//cdn.example/asset#download">scheme relative</a>
			<a href="https://[2001:DB8::1]:443/page#section">IPv6</a>
			<a href="../docs?q=go#other">duplicate after fragment removal</a>
			<a href="https://user:secret@example.com/private">userinfo</a>
			<a href="mailto:test@example.com">email</a>
			<a href="javascript:alert(1)">script</a>
			<a href="%zz">malformed</a>
			<a href="/do-not-follow" rel="external NOFOLLOW">nofollow</a>
		</p>
		<section hidden><h3>Hidden ancestor heading</h3><a href="/hidden">hidden link</a></section>
		<section aria-hidden="true"><h3>ARIA-hidden ancestor heading</h3><a href="/aria-hidden">ARIA-hidden link</a></section>
		<h6>Second
		 heading<script>not visible</script></h6>
	</article>`)

	headings, links := collectContentMetadata(node, "https://example.com/news/page")

	if want := []string{"First heading", "Second heading"}; !reflect.DeepEqual(headings, want) {
		t.Fatalf("headings = %#v, want %#v", headings, want)
	}
	if want := []string{
		"https://example.com/docs?q=go",
		"https://other.example/path",
		"https://cdn.example/asset",
		"https://[2001:db8::1]/page",
	}; !reflect.DeepEqual(links, want) {
		t.Fatalf("links = %#v, want %#v", links, want)
	}
}

func TestCollectContentMetadata_BoundsCountsAndEntrySizes(t *testing.T) {
	var fixture strings.Builder
	fixture.WriteString("<article>")
	fixture.WriteString("<h2>" + strings.Repeat("h", maxMetadataHeadingBytes+1) + "</h2>")
	fixture.WriteString(`<a href="https://example.com/` + strings.Repeat("u", maxMetadataURLBytes+1) + `">oversized</a>`)
	for i := 0; i < maxMetadataHeadings+5; i++ {
		fixture.WriteString("<h3>Heading " + strconv.Itoa(i) + "</h3>")
	}
	for i := 0; i < maxMetadataOutboundLinks+5; i++ {
		fixture.WriteString(`<a href="/link/` + strconv.Itoa(i) + `#fragment">Link</a>`)
	}
	fixture.WriteString("</article>")

	headings, links := collectContentMetadata(parseMetadataFixture(t, fixture.String()), "https://example.com/base")

	if len(headings) != maxMetadataHeadings {
		t.Fatalf("heading count = %d, want cap %d", len(headings), maxMetadataHeadings)
	}
	if headings[0] != "Heading 0" || headings[len(headings)-1] != "Heading "+strconv.Itoa(maxMetadataHeadings-1) {
		t.Fatalf("unexpected bounded headings: first=%q last=%q", headings[0], headings[len(headings)-1])
	}
	if len(links) != maxMetadataOutboundLinks {
		t.Fatalf("link count = %d, want cap %d", len(links), maxMetadataOutboundLinks)
	}
	if links[0] != "https://example.com/link/0" || links[len(links)-1] != "https://example.com/link/"+strconv.Itoa(maxMetadataOutboundLinks-1) {
		t.Fatalf("unexpected bounded links: first=%q last=%q", links[0], links[len(links)-1])
	}
}

func parseMetadataFixture(t *testing.T, fixture string) *html.Node {
	t.Helper()
	node, err := html.Parse(strings.NewReader(fixture))
	if err != nil {
		t.Fatalf("parse metadata fixture: %v", err)
	}
	return node
}

func TestExtract_JSRequiredHeuristic(t *testing.T) {
	// Large HTML where scripts dominate and there is no meaningful text.
	body := "<html><body><div id='root'></div>" +
		strings.Repeat("<script>"+strings.Repeat("x", 200)+"</script>", 20) +
		"</body></html>"
	ex := New(false)
	c, err := ex.Extract([]byte(body), "https://example.com/spa")
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if c.UnsupportedReason != ReasonJSRequired {
		t.Fatalf("expected js_required, got %q (text=%q)", c.UnsupportedReason, c.MainText)
	}
}

func TestExtract_EmptyReturnsError(t *testing.T) {
	if _, err := New(false).Extract(nil, ""); err == nil {
		t.Fatal("expected error for empty html")
	}
}
