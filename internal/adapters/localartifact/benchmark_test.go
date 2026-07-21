package localartifact

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	coreartifact "github.com/staticvar/fetchmark/internal/core/localartifact"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
)

var localArtifactBenchmarkSizes = []int{100, 1000}

func BenchmarkStorePut(b *testing.B) {
	for _, corpusSize := range localArtifactBenchmarkSizes {
		b.Run(fmt.Sprintf("urls=%d", corpusSize), func(b *testing.B) {
			store := benchmarkStore(b, corpusSize)
			base := time.Date(2026, time.July, 18, 0, 0, 0, 0, time.UTC)
			const target = "https://benchmark.example/target"
			if err := store.Put(context.Background(), benchmarkVersion(target, "baseline", base)); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				body := fmt.Sprintf("replacement-%d", index%2)
				if err := store.Put(context.Background(), benchmarkVersion(target, body, base.Add(time.Duration(index+1)*time.Second))); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStoreRevalidate(b *testing.B) {
	for _, corpusSize := range localArtifactBenchmarkSizes {
		b.Run(fmt.Sprintf("urls=%d", corpusSize), func(b *testing.B) {
			store := benchmarkStore(b, corpusSize)
			base := time.Date(2026, time.July, 18, 0, 0, 0, 0, time.UTC)
			const target = "https://benchmark.example/target"
			version := benchmarkVersion(target, "stable", base)
			version.ETag = `"baseline"`
			if err := store.Put(context.Background(), version); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				observedAt := base.Add(time.Duration(index+1) * time.Second)
				if err := store.Revalidate(context.Background(), coreartifact.Observation{
					URL: target, ObservedAt: observedAt, ValidatedAt: observedAt,
					ETag: fmt.Sprintf(`"etag-%d"`, index),
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStoreRevoke(b *testing.B) {
	for _, corpusSize := range localArtifactBenchmarkSizes {
		b.Run(fmt.Sprintf("urls=%d", corpusSize), func(b *testing.B) {
			store := benchmarkStore(b, corpusSize)
			base := time.Date(2026, time.July, 18, 0, 0, 0, 0, time.UTC)
			const target = "https://benchmark.example/target"
			if err := store.Put(context.Background(), benchmarkVersion(target, "private", base)); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := store.Revoke(
					context.Background(), target, localcorpus.DispositionTakedown,
					base.Add(time.Duration(index+1)*time.Second),
				); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStoreReopenRepair(b *testing.B) {
	for _, corpusSize := range localArtifactBenchmarkSizes {
		b.Run(fmt.Sprintf("urls=%d", corpusSize), func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "artifacts")
			options := benchmarkStoreOptions(path, corpusSize)
			store, err := Open(options)
			if err != nil {
				b.Fatal(err)
			}
			populateBenchmarkStore(b, store, corpusSize)
			if err := store.Close(); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				store, err = Open(options)
				if err != nil {
					b.Fatal(err)
				}
				if err := store.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkStore(b *testing.B, corpusSize int) *Store {
	b.Helper()
	store, err := Open(benchmarkStoreOptions(filepath.Join(b.TempDir(), "artifacts"), corpusSize))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = store.Close() })
	populateBenchmarkStore(b, store, corpusSize)
	return store
}

func benchmarkStoreOptions(path string, corpusSize int) Options {
	return Options{
		Path: path, MaxBytes: 1 << 30, MaxArtifactBytes: 1 << 20,
		MaxEntries: corpusSize + 10,
	}
}

func populateBenchmarkStore(b *testing.B, store *Store, corpusSize int) {
	b.Helper()
	base := time.Date(2026, time.July, 17, 0, 0, 0, 0, time.UTC)
	store.mu.Lock()
	defer store.mu.Unlock()
	for index := 0; index < corpusSize; index++ {
		canonical := fmt.Sprintf("https://benchmark-%06d.example/document", index)
		body := []byte(fmt.Sprintf("benchmark body %06d", index))
		contentHash := hashBytes(body)
		observedAt := base.Add(time.Duration(index) * time.Second)
		if err := ensureRealDirectory(store.objectDir(canonical), 0o700); err != nil {
			b.Fatal(err)
		}
		if err := atomicWriteRoot(store.rootFS, store.bodyRelative(canonical, contentHash), body); err != nil {
			b.Fatal(err)
		}
		if err := store.writeState(canonical, pointerState{
			SchemaVersion: storeSchemaVersion, URL: canonical, EffectiveURL: canonical, ContentHash: contentHash,
			MIME: "text/html", PolicyAgent: "Fetchmark-Benchmark", FetchedAt: observedAt,
			ObservedAt: observedAt, ValidatedAt: observedAt, SafetyClassification: localcorpus.SafetyUnclassified,
			IndexingDisposition: localcorpus.DispositionPermitted,
		}); err != nil {
			b.Fatal(err)
		}
	}
	if err := store.refreshUsageLocked(); err != nil {
		b.Fatal(err)
	}
}

func benchmarkVersion(rawURL, body string, observedAt time.Time) coreartifact.Version {
	return coreartifact.Version{
		URL: rawURL, EffectiveURL: rawURL, Body: []byte(body), MIME: "text/html",
		PolicyAgent: "Fetchmark-Benchmark", FetchedAt: observedAt, ObservedAt: observedAt,
		ValidatedAt: observedAt, SafetyClassification: localcorpus.SafetyUnclassified,
		IndexingDisposition: localcorpus.DispositionPermitted,
	}
}
