package searxng

import (
	"testing"
	"time"
)

func requireEventually(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if ok() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not satisfied within %v", timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
