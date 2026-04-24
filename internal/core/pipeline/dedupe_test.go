package pipeline

import (
	"testing"

	"github.com/staticvar/fetchmark/internal/core/model"
)

func TestJaccard_IdenticalSets(t *testing.T) {
	a := shingleSet("the quick brown fox jumps over the lazy dog", 3)
	b := shingleSet("the quick brown fox jumps over the lazy dog", 3)
	if got := jaccard(a, b); got != 1.0 {
		t.Fatalf("identical Jaccard = %v, want 1.0", got)
	}
}

func TestJaccard_Disjoint(t *testing.T) {
	a := shingleSet("red green blue yellow", 3)
	b := shingleSet("mercury venus earth mars", 3)
	if got := jaccard(a, b); got != 0 {
		t.Fatalf("disjoint Jaccard = %v, want 0", got)
	}
}

func TestDedupeNearDuplicates_DropsReposts(t *testing.T) {
	// Two syndicated copies with tiny edits + one unrelated article.
	article := "This post explains how BM25 ranking works over a corpus " +
		"of documents. It covers term frequency saturation, inverse " +
		"document frequency, and length normalization with practical " +
		"worked examples drawn from common search pipelines."
	// Realistic syndicated repost: identical body plus a short credit line.
	syndicated := article + " Reprinted with permission."
	unrelated := "A beginner's guide to sourdough starters covering flour, " +
		"hydration, and bulk fermentation for home bakers exploring " +
		"fermented breads for the first time."

	in := []model.SearchResult{
		{URL: "https://a/x", Score: 0.4, Content: &model.Content{MainText: article}},
		{URL: "https://b/x", Score: 0.9, Content: &model.Content{MainText: syndicated}},
		{URL: "https://c/y", Score: 0.7, Content: &model.Content{MainText: unrelated}},
	}
	out := dedupeNearDuplicates(in)
	if len(out) != 2 {
		t.Fatalf("expected 2 results after near-dup dedupe, got %d", len(out))
	}
	// Higher-scoring duplicate (b) should survive over a.
	seen := map[string]bool{}
	for _, r := range out {
		seen[r.URL] = true
	}
	if seen["https://a/x"] {
		t.Errorf("lower-scoring duplicate kept; survivors=%v", seen)
	}
	if !seen["https://b/x"] || !seen["https://c/y"] {
		t.Errorf("expected b and c to survive; survivors=%v", seen)
	}
}

func TestDedupeNearDuplicates_PassesThroughWithoutContent(t *testing.T) {
	in := []model.SearchResult{{URL: "https://a"}, {URL: "https://b"}}
	out := dedupeNearDuplicates(in)
	if len(out) != 2 {
		t.Fatalf("results without Content.MainText should be kept; got %d", len(out))
	}
}

func TestShingleSet_ShortText(t *testing.T) {
	s := shingleSet("hi there", 3)
	if len(s) != 2 {
		t.Fatalf("expected fallback unigram set of size 2, got %d", len(s))
	}
}

// CJK bodies have no whitespace word boundaries, so the word-3-gram
// shingler collapses to a near-empty set and Jaccard refuses to group
// obvious duplicates. These tests pin the CJK branch: identical bodies
// dedupe, unrelated bodies don't, and the set is dense enough for
// Jaccard to be meaningful.
func TestShingleSet_CJKUsesCharBigrams(t *testing.T) {
// 30-rune Chinese passage — word 3-grams would yield at most 1
// shingle (the whole string); char bi-grams should yield 29.
cn := "机器学习是人工智能的一个分支它研究计算机如何模拟学习"
s := shingleSet(cn, shingleSize)
if len(s) < 10 {
t.Fatalf("CJK body should produce dense char shingles; got %d", len(s))
}
}

func TestDedupeNearDuplicates_CJKIdenticalBodiesCollapse(t *testing.T) {
body := "机器学习是人工智能的一个分支它研究计算机如何模拟学习过程使系统能够从数据中学习"
in := []model.SearchResult{
{URL: "https://a/", Score: 2, Content: &model.Content{MainText: body}},
{URL: "https://b/", Score: 1, Content: &model.Content{MainText: body}},
}
out := dedupeNearDuplicates(in)
if len(out) != 1 || out[0].URL != "https://a/" {
t.Fatalf("expected identical CJK bodies to collapse to higher-score a; got %+v", out)
}
}

func TestDedupeNearDuplicates_CJKUnrelatedBodiesSurvive(t *testing.T) {
a := "机器学习是人工智能的一个分支它研究计算机如何模拟学习过程"
b := "气候变化是当前全球面临的最严重的环境问题之一需要各国共同努力"
in := []model.SearchResult{
{URL: "https://a/", Content: &model.Content{MainText: a}},
{URL: "https://b/", Content: &model.Content{MainText: b}},
}
out := dedupeNearDuplicates(in)
if len(out) != 2 {
t.Fatalf("unrelated CJK bodies should both survive; got %d", len(out))
}
}

func TestIsCJKDominant_MixedAsciiPunctuation(t *testing.T) {
// Mostly-Chinese sentence with routine ASCII punctuation — must
// still take the CJK path (CJK ratio by non-punct rune count).
if !isCJKDominant("机器学习, is 人工智能的一个分支.") {
t.Fatal("mixed CJK-dominant body should trip isCJKDominant")
}
// ASCII-dominant body with a stray Chinese character must NOT
// flip the heuristic.
if isCJKDominant("the quick brown fox jumps over 猫") {
t.Fatal("ASCII-dominant body should not trip isCJKDominant")
}
}

// Near-duplicate realism check: a base CJK article plus a short
// appended credit/boilerplate tail should still collapse against the
// base under the existing 0.85 threshold. This guards against the
// char-bigram branch being too strict for real-world reposts.
func TestDedupeNearDuplicates_CJKWithAppendedTailCollapses(t *testing.T) {
base := "机器学习是人工智能的一个分支它研究计算机如何模拟或实现人类的学习行为以获取新的知识或技能重新组织已有的知识结构使之不断改善自身的性能它是人工智能的核心"
withTail := base + "来源新浪科技编辑张三"
in := []model.SearchResult{
{URL: "https://a/", Score: 2, Content: &model.Content{MainText: base}},
{URL: "https://b/", Score: 1, Content: &model.Content{MainText: withTail}},
}
out := dedupeNearDuplicates(in)
if len(out) != 1 {
t.Fatalf("base + short appended tail should collapse; got %d survivors (Jaccard too strict)", len(out))
}
if out[0].URL != "https://a/" {
t.Fatalf("expected higher-score a to survive; got %s", out[0].URL)
}
}

// Hangul coverage: the CJK branch relies on unicode.Is(Hangul) and
// char bi-grams. Identical Korean bodies must collapse.
func TestDedupeNearDuplicates_HangulIdenticalBodiesCollapse(t *testing.T) {
body := "기계 학습은 인공 지능의 한 분야로 컴퓨터가 학습할 수 있도록 하는 알고리즘과 기술을 개발하는 분야이다"
in := []model.SearchResult{
{URL: "https://a/", Score: 2, Content: &model.Content{MainText: body}},
{URL: "https://b/", Score: 1, Content: &model.Content{MainText: body}},
}
out := dedupeNearDuplicates(in)
if len(out) != 1 || out[0].URL != "https://a/" {
t.Fatalf("identical Hangul bodies should collapse to higher-score a; got %+v", out)
}
}
