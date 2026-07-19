package searchbudget

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
)

type searchFunc func(context.Context, search.Query) ([]search.Hit, error)

func (f searchFunc) Search(ctx context.Context, q search.Query) ([]search.Hit, error) {
	return f(ctx, q)
}

func TestSearcherBoundsConcurrency(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var active, maximum atomic.Int32
	inner := searchFunc(func(ctx context.Context, _ search.Query) ([]search.Hit, error) {
		current := active.Add(1)
		for {
			prior := maximum.Load()
			if current <= prior || maximum.CompareAndSwap(prior, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
			active.Add(-1)
			return nil, nil
		case <-ctx.Done():
			active.Add(-1)
			return nil, ctx.Err()
		}
	})
	budgeted, err := New(inner, Options{RatePerSecond: 100, Burst: 2, MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = budgeted.SearchBatch(context.Background(), search.Query{Q: "test"})
		}()
	}
	<-started
	select {
	case <-started:
		t.Fatal("second request bypassed provider concurrency budget")
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrency = %d", maximum.Load())
	}
}

func TestSearcherCancellationDuringRateWaitSkipsProvider(t *testing.T) {
	var calls atomic.Int32
	budgeted, err := New(searchFunc(func(context.Context, search.Query) ([]search.Hit, error) {
		calls.Add(1)
		return nil, nil
	}), Options{RatePerSecond: 0.01, Burst: 1, MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := budgeted.SearchBatch(context.Background(), search.Query{Q: "first"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := budgeted.SearchBatch(ctx, search.Query{Q: "second"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("provider calls = %d", calls.Load())
	}
}
