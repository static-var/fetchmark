// Package providerbudget coordinates process-wide provider concurrency and
// request-rate admission at the actual upstream call boundary.
package providerbudget

import (
	"context"
	"errors"

	"golang.org/x/time/rate"
)

// Gate admits at most maxConcurrency operations and spends a rate token only
// after an operation owns a concurrency slot. This prevents queued operations
// from reserving future tokens and then bursting when the slot opens.
type Gate struct {
	limiter *rate.Limiter
	sem     chan struct{}
}

// New returns a process-local gate with strict positive budgets.
func New(ratePerSecond float64, burst, maxConcurrency int) (*Gate, error) {
	if ratePerSecond <= 0 || burst < 1 || maxConcurrency < 1 {
		return nil, errors.New("providerbudget: rate, burst, and concurrency must be positive")
	}
	return &Gate{
		limiter: rate.NewLimiter(rate.Limit(ratePerSecond), burst),
		sem:     make(chan struct{}, maxConcurrency),
	}, nil
}

// AcquireSlot waits only for provider concurrency. Callers with cooldown state
// can recheck it before spending rate capacity.
func (g *Gate) AcquireSlot(ctx context.Context) (func(), error) {
	select {
	case g.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return func() { <-g.sem }, nil
}

// WaitRate spends one provider rate token after concurrency admission.
func (g *Gate) WaitRate(ctx context.Context) error { return g.limiter.Wait(ctx) }

// Acquire waits for concurrency first, then rate admission. The returned
// function must be called exactly once when the provider operation finishes.
func (g *Gate) Acquire(ctx context.Context) (func(), error) {
	release, err := g.AcquireSlot(ctx)
	if err != nil {
		return nil, err
	}
	if err := g.limiter.Wait(ctx); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// Burst reports the maximum rate burst configured for diagnostics and tests.
func (g *Gate) Burst() int { return g.limiter.Burst() }

// MaxConcurrency reports the provider concurrency capacity.
func (g *Gate) MaxConcurrency() int { return cap(g.sem) }
