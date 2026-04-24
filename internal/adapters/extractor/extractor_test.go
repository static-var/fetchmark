package extractor

import (
	"strings"
	"testing"
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
