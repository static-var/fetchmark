package scrapling

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/search"
)

// TestLiveSidecar is an explicit integration probe. It is skipped by ordinary
// test runs so upstream availability never makes the deterministic suite flaky.
func TestLiveSidecar(t *testing.T) {
	endpoint := os.Getenv("FM_TEST_SCRAPLING_URL")
	if endpoint == "" {
		t.Skip("set FM_TEST_SCRAPLING_URL to run the live sidecar probe")
	}
	client, err := New(Options{
		Endpoint: endpoint, HTTPClient: &http.Client{Timeout: 2 * time.Minute},
		AllowInsecureHTTP: true, MaxResults: 12, MaxBodyBytes: 1 << 20,
		RatePerSecond: 1, Burst: 1, MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	batch, err := client.SearchBatch(ctx, search.Query{
		Q: "latest Unicode Standard changes", Engines: []string{"google", "duckduckgo", "brave"}, MaxResults: 12,
	})
	if err != nil {
		t.Fatal(err)
	}
	if (batch.Status != search.BatchHealthy && batch.Status != search.BatchPartial) || len(batch.Hits) == 0 {
		t.Fatalf("live batch = %+v", batch)
	}
	for _, hit := range batch.Hits {
		if hit.URL == "" || hit.Title == "" || len(hit.Engines) != 1 {
			t.Fatalf("invalid live hit = %+v", hit)
		}
	}
}
