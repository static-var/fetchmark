// Package search declares the domain-facing search contract. Adapters
// like SearXNG live outside this package and implement Searcher.
package search

import (
	"context"
	"time"
)

// Query describes a user's search intent.
type Query struct {
	Q              string
	Engines        []string
	Categories     []string
	Language       string
	TimeRange      string
	SafeSearch     *int
	IncludeDomains []string
	ExcludeDomains []string
	ExactMatch     bool
	MaxResults     int
}

// Hit is the minimal per-result data returned by a Searcher, prior to
// any URL fetching or content extraction.
type Hit struct {
	URL         string
	Title       string
	Snippet     string
	Engines     []string
	PublishedAt *time.Time
	Metadata    map[string]string
}

// Searcher returns a ranked list of URLs matching a query. Implementations
// must be safe for concurrent use.
type Searcher interface {
	Search(ctx context.Context, q Query) ([]Hit, error)
}
