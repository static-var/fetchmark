package providerbudget

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAcquireDoesNotSpendRateTokenBeforeConcurrencyAdmission(t *testing.T) {
	gate, err := New(1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	gate.sem <- struct{}{}
	defer func() { <-gate.sem }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	release, err := gate.Acquire(ctx)
	if release != nil {
		t.Fatal("release must be nil when admission fails")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if !gate.limiter.Allow() {
		t.Fatal("concurrency rejection consumed the provider's next rate token")
	}
}

func TestAcquireReleasesConcurrencyAfterRateWaitFailure(t *testing.T) {
	gate, err := New(1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !gate.limiter.Allow() {
		t.Fatal("consume initial token")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	release, err := gate.Acquire(ctx)
	if release != nil {
		t.Fatal("release must be nil when rate admission fails")
	}
	if err == nil {
		t.Fatal("rate admission should fail when its wait exceeds the deadline")
	}
	select {
	case gate.sem <- struct{}{}:
		<-gate.sem
	default:
		t.Fatal("failed rate admission leaked a concurrency slot")
	}
}

func TestNewRejectsInvalidBudgets(t *testing.T) {
	for _, values := range [][3]float64{{0, 1, 1}, {1, 0, 1}, {1, 1, 0}} {
		if _, err := New(values[0], int(values[1]), int(values[2])); err == nil {
			t.Fatalf("New(%v) should fail", values)
		}
	}
}
