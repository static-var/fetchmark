// Package searchbudget applies process-wide rate and concurrency limits to a
// Searcher whose upstream does not enforce declarative source budgets itself.
package searchbudget

import (
	"context"
	"errors"

	"github.com/staticvar/fetchmark/internal/adapters/providerbudget"
	"github.com/staticvar/fetchmark/internal/core/search"
)

type Options struct {
	RatePerSecond  float64
	Burst          int
	MaxConcurrency int
}

type Searcher struct {
	inner search.Searcher
	batch search.BatchSearcher
	gate  *providerbudget.Gate
}

var _ search.Searcher = (*Searcher)(nil)
var _ search.BatchSearcher = (*Searcher)(nil)

func New(inner search.Searcher, options Options) (*Searcher, error) {
	if inner == nil {
		return nil, errors.New("searchbudget: inner searcher is required")
	}
	if options.RatePerSecond <= 0 || options.Burst < 1 || options.MaxConcurrency < 1 {
		return nil, errors.New("searchbudget: rate, burst, and concurrency must be positive")
	}
	gate, err := providerbudget.New(options.RatePerSecond, options.Burst, options.MaxConcurrency)
	if err != nil {
		return nil, err
	}
	budgeted := &Searcher{inner: inner, gate: gate}
	if batch, ok := inner.(search.BatchSearcher); ok {
		budgeted.batch = batch
	}
	return budgeted, nil
}

func (s *Searcher) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	batch, err := s.SearchBatch(ctx, q)
	if err != nil {
		return nil, err
	}
	return batch.Hits, nil
}

func (s *Searcher) SearchBatch(ctx context.Context, q search.Query) (search.SearchBatch, error) {
	if err := ctx.Err(); err != nil {
		return search.SearchBatch{Status: search.BatchFailed}, err
	}
	release, err := s.gate.Acquire(ctx)
	if err != nil {
		return search.SearchBatch{Status: search.BatchFailed}, err
	}
	defer release()
	if s.batch != nil {
		return s.batch.SearchBatch(ctx, q)
	}
	hits, err := s.inner.Search(ctx, q)
	if err != nil {
		return search.SearchBatch{Status: search.BatchFailed}, err
	}
	status := search.BatchAuthoritativeEmpty
	if len(hits) > 0 {
		status = search.BatchHealthy
	}
	return search.SearchBatch{Hits: hits, Provider: "legacy", Status: status}, nil
}
