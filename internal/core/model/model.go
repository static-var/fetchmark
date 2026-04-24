// Package model holds shared domain types that cross layer boundaries.
// Keep this package free of adapter- or framework-specific imports.
package model

import "time"

// SearchResult is the canonical representation of a single search hit
// after it has been extracted (possibly) and scored (possibly). Adapters
// return lighter-weight variants which are promoted into this shape by
// the core pipeline.
type SearchResult struct {
	URL         string            `json:"url"`
	Title       string            `json:"title,omitempty"`
	Snippet     string            `json:"snippet,omitempty"`
	Engines     []string          `json:"engines,omitempty"`
	PublishedAt *time.Time        `json:"published_at,omitempty"`
	Author      string            `json:"author,omitempty"`
	Markdown    string            `json:"markdown,omitempty"`
	HTML        string            `json:"html,omitempty"`
	Content     *Content          `json:"content,omitempty"`
	Score       float64           `json:"score,omitempty"`
	FromCache   bool              `json:"from_cache,omitempty"`
	FetchMS     int64             `json:"fetch_ms,omitempty"`
	Unsupported string            `json:"unsupported_reason,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// Content is the structured extraction output: headline, body text, and
// auxiliary assets.
type Content struct {
	Title       string     `json:"title,omitempty"`
	Byline      string     `json:"byline,omitempty"`
	Text        string     `json:"text,omitempty"`
	Links       []string   `json:"links,omitempty"`
	Images      []string   `json:"images,omitempty"`
	PublishedAt *time.Time `json:"published_at,omitempty"`
	Language    string     `json:"language,omitempty"`
}
