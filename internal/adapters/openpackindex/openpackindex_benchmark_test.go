package openpackindex

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	benchmarkOpenPackRecords10K = 10_000
	benchmarkOpenPackProvider   = "benchmark-open-pack"
)

var (
	benchmarkOpenPackCreatedAt = time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	benchmarkOpenPackExpiresAt = time.Date(2027, time.June, 30, 23, 59, 59, 0, time.UTC)
	benchmarkOpenPackNow       = time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC)
)

type benchmarkOpenPackFixture struct {
	bundle            string
	manifest          indexpack.Manifest
	manifestSHA256    string
	publicKey         ed25519.PublicKey
	acceptance        indexpack.Acceptance
	compressedBytes   uint64
	uncompressedBytes uint64
}

func BenchmarkOpenPackInstall10K(b *testing.B) {
	b.StopTimer()
	fixture := newBenchmarkOpenPackFixture10K(b)
	roots := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()

	var logicalBytes, diskBytes uint64
	var diskSizeAvailable bool
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		root, err := os.MkdirTemp(roots, "install-")
		if err != nil {
			b.Fatalf("create install root: %v", err)
		}
		b.StartTimer()
		installed, err := Install(context.Background(), fixture.installOptions(root))
		b.StopTimer()
		if err != nil {
			b.Fatalf("Install: %v", err)
		}
		if installed.RecordCount != benchmarkOpenPackRecords10K || installed.ManifestSHA256 != fixture.manifestSHA256 {
			b.Fatalf("Install identity = %#v", installed)
		}
		logicalBytes, diskBytes, diskSizeAvailable = benchmarkDirectorySize(b, installed.Path)
	}
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(benchmarkOpenPackRecords10K*b.N)/elapsed.Seconds(), "records/s")
	}
	b.ReportMetric(float64(fixture.compressedBytes), "compressed_B/op")
	b.ReportMetric(float64(fixture.uncompressedBytes), "uncompressed_B/op")
	if b.N > 0 {
		b.ReportMetric(float64(logicalBytes), "installed_logical_B/op")
		if diskSizeAvailable {
			b.ReportMetric(float64(diskBytes), "installed_disk_B/op")
		}
	}
}

func BenchmarkOpenPackColdOpen10K(b *testing.B) {
	b.StopTimer()
	fixture := newBenchmarkOpenPackFixture10K(b)
	installed := benchmarkInstallOpenPack10K(b, fixture)
	options := fixture.openOptions(installed)
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		index, err := Open(options)
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		if err := index.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
}

func BenchmarkOpenPackSearch10K(b *testing.B) {
	b.StopTimer()
	fixture := newBenchmarkOpenPackFixture10K(b)
	installed := benchmarkInstallOpenPack10K(b, fixture)
	index, err := Open(fixture.openOptions(installed))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = index.Close() })
	queries := []search.Query{
		{Q: "titlebenchmark", MaxResults: 5},
		{Q: "headingbenchmark", MaxResults: 5},
		{Q: "anchorbenchmark", MaxResults: 5},
		{Q: "sketchbenchmark", MaxResults: 5},
		{Q: "commonbenchmark", IncludeDomains: []string{"domain042.example.com"}, MaxResults: 5},
		{Q: "commonbenchmark", Language: "fr", MaxResults: 5},
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()

	for iteration := 0; iteration < b.N; iteration++ {
		for _, query := range queries {
			batch, err := index.SearchBatch(context.Background(), query)
			if err != nil {
				b.Fatalf("SearchBatch(%q): %v", query.Q, err)
			}
			if len(batch.Hits) == 0 {
				b.Fatalf("SearchBatch(%q) returned no hits", query.Q)
			}
			if batch.Provider != benchmarkOpenPackProvider || batch.Instance != fixture.manifest.PackID+"@1" {
				b.Fatalf("SearchBatch identity = %#v", batch)
			}
			if batch.Hits[0].Metadata["source_id"] != benchmarkOpenPackProvider {
				b.Fatalf("SearchBatch source identity = %#v", batch.Hits[0].Metadata)
			}
		}
	}
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(b.N*len(queries))/elapsed.Seconds(), "queries/s")
	}
}

func newBenchmarkOpenPackFixture10K(b *testing.B) benchmarkOpenPackFixture {
	b.Helper()
	seed := sha256.Sum256([]byte("fetchmark-open-pack-10k-benchmark-ed25519-v1"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	publicKey := privateKey.Public().(ed25519.PublicKey)

	var records bytes.Buffer
	encoder := json.NewEncoder(&records)
	encoder.SetEscapeHTML(false)
	for recordNumber := 0; recordNumber < benchmarkOpenPackRecords10K; recordNumber++ {
		host := fmt.Sprintf("domain%03d.example.com", recordNumber%250)
		language := "en"
		if recordNumber%2 == 1 {
			language = "fr"
		}
		canonicalURL := fmt.Sprintf("https://%s/docs/record-%05d", host, recordNumber)
		record := indexpack.Record{
			Operation:      indexpack.OperationUpsert,
			URL:            canonicalURL,
			Title:          fmt.Sprintf("titlebenchmark open pack document %05d", recordNumber),
			Headings:       []string{"headingbenchmark discovery topic", fmt.Sprintf("section%03d", recordNumber%100)},
			AnchorTerms:    []string{"anchorbenchmark", fmt.Sprintf("reference%03d", recordNumber%100)},
			SalientSketch:  fmt.Sprintf("sketchbenchmark commonbenchmark record %05d", recordNumber),
			Language:       language,
			PublishedAt:    "2026-06-15T00:00:00Z",
			FetchedAt:      "2026-06-30T12:00:00Z",
			ContentSHA256:  indexpack.Digest([]byte(fmt.Sprintf("benchmark-content-%05d", recordNumber))),
			AuthorityScore: uint16(100 + recordNumber%900),
			FreshnessScore: uint16(200 + recordNumber%800),
			Provenance: []indexpack.Provenance{{
				Source: "benchmark-source", SourceURI: "https://source.example.com/open-pack",
				RetrievedAt: "2026-06-30T12:00:00Z", RightsNotice: "benchmark fixture metadata",
			}},
		}
		if err := encoder.Encode(record); err != nil {
			b.Fatalf("encode record %d: %v", recordNumber, err)
		}
	}

	var compressed bytes.Buffer
	zstdEncoder, err := zstd.NewWriter(&compressed, zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true))
	if err != nil {
		b.Fatalf("initialize zstd encoder: %v", err)
	}
	if _, err := zstdEncoder.Write(records.Bytes()); err != nil {
		b.Fatalf("compress records: %v", err)
	}
	if err := zstdEncoder.Close(); err != nil {
		b.Fatalf("close zstd encoder: %v", err)
	}

	compressedRaw := compressed.Bytes()
	recordsRaw := records.Bytes()
	compressedSHA256 := indexpack.ShardDigest(compressedRaw)
	manifest := indexpack.Manifest{
		Version: indexpack.Version, Kind: indexpack.KindSnapshot, PackID: "benchmark-developer-en-fr", Revision: 1,
		CreatedAt: benchmarkOpenPackCreatedAt.Format(time.RFC3339), ExpiresAt: benchmarkOpenPackExpiresAt.Format(time.RFC3339),
		SigningKeyID: indexpack.KeyID(publicKey),
		Publisher: indexpack.Publisher{
			Name: "Fetchmark Benchmark", ContactURI: "mailto:benchmark@example.com",
			TakedownURI: "https://source.example.com/takedown", RightsNotice: "synthetic benchmark metadata",
		},
		Policy:      indexpack.Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"},
		Languages:   []string{"en", "fr"},
		RecordCount: benchmarkOpenPackRecords10K,
		Shards: []indexpack.Shard{{
			Path: indexpack.ShardPath(compressedSHA256), Compression: "zstd", SHA256: compressedSHA256,
			CompressedSizeBytes: uint64(len(compressedRaw)), UncompressedSHA256: indexpack.ShardDigest(recordsRaw),
			UncompressedSizeBytes: uint64(len(recordsRaw)), RecordCount: benchmarkOpenPackRecords10K,
		}},
		Build: indexpack.Build{
			Generator: "fetchmark-benchmark", GeneratorVersion: "1", Analyzer: "unicode-lexical", AnalyzerVersion: "1",
			PolicySHA256: indexpack.Digest([]byte("benchmark-policy-v1")), ExclusionsSHA256: indexpack.Digest([]byte("benchmark-exclusions-v1")),
			Inputs: []indexpack.BuildInput{{
				Name: "synthetic-10k", URI: "https://source.example.com/open-pack", RetrievedAt: "2026-06-30T12:00:00Z",
				SHA256: indexpack.Digest(recordsRaw), RightsNotice: "synthetic benchmark metadata",
			}},
		},
	}
	manifestRaw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		b.Fatalf("EncodeManifest: %v", err)
	}
	signature, err := indexpack.SignManifest(manifestRaw, privateKey)
	if err != nil {
		b.Fatalf("SignManifest: %v", err)
	}
	signatureRaw, err := indexpack.EncodeSignature(signature)
	if err != nil {
		b.Fatalf("EncodeSignature: %v", err)
	}

	bundle := filepath.Join(b.TempDir(), "bundle")
	if err := os.Mkdir(bundle, 0o700); err != nil {
		b.Fatalf("create bundle: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundle, ManifestFilename), manifestRaw, 0o600); err != nil {
		b.Fatalf("write manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundle, SignatureFilename), signatureRaw, 0o600); err != nil {
		b.Fatalf("write signature: %v", err)
	}
	shardPath := filepath.Join(bundle, filepath.FromSlash(manifest.Shards[0].Path))
	if err := os.MkdirAll(filepath.Dir(shardPath), 0o700); err != nil {
		b.Fatalf("create shard directory: %v", err)
	}
	if err := os.WriteFile(shardPath, compressedRaw, 0o600); err != nil {
		b.Fatalf("write shard: %v", err)
	}

	manifestSHA256 := indexpack.ManifestDigest(manifestRaw)
	return benchmarkOpenPackFixture{
		bundle: bundle, manifest: manifest, manifestSHA256: manifestSHA256, publicKey: publicKey,
		acceptance: indexpack.Acceptance{
			Now: benchmarkOpenPackNow, ExpectedPackID: manifest.PackID, ExpectedManifestSHA256: manifestSHA256,
			ExpectedKeyID: indexpack.KeyID(publicKey), MinimumRevision: 1, ExpectedRevision: manifest.Revision,
			ExpectedRecordCount: manifest.RecordCount, ExpectedCreatedAt: manifest.CreatedAt, ExpectedExpiresAt: manifest.ExpiresAt,
		},
		compressedBytes: uint64(len(compressedRaw)), uncompressedBytes: uint64(len(recordsRaw)),
	}
}

func (fixture benchmarkOpenPackFixture) installOptions(root string) InstallOptions {
	return InstallOptions{
		BundleDir: fixture.bundle, Root: root,
		TrustedKeys: map[string]ed25519.PublicKey{indexpack.KeyID(fixture.publicKey): fixture.publicKey},
		Acceptance:  fixture.acceptance,
	}
}

func (fixture benchmarkOpenPackFixture) openOptions(installed Installed) OpenOptions {
	return OpenOptions{
		Path: installed.Path, ExpectedManifestSHA256: fixture.manifestSHA256,
		ExpectedPackID: fixture.manifest.PackID, ExpectedRevision: fixture.manifest.Revision,
		ExpectedRecordCount: benchmarkOpenPackRecords10K, ExpectedKeyID: fixture.acceptance.ExpectedKeyID,
		ExpectedCreatedAt: fixture.manifest.CreatedAt, ExpectedExpiresAt: fixture.manifest.ExpiresAt,
		Now: fixture.acceptance.Now, ProviderID: benchmarkOpenPackProvider,
	}
}

func benchmarkInstallOpenPack10K(b *testing.B, fixture benchmarkOpenPackFixture) Installed {
	b.Helper()
	root := filepath.Join(b.TempDir(), "root")
	installed, err := Install(context.Background(), fixture.installOptions(root))
	if err != nil {
		b.Fatalf("Install fixture: %v", err)
	}
	return installed
}

func benchmarkDirectorySize(b *testing.B, root string) (logical, disk uint64, diskAvailable bool) {
	b.Helper()
	diskAvailable = true
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			logical += uint64(info.Size())
		}
		blocks, ok := allocatedFilesystemBlocks(info)
		if !ok {
			diskAvailable = false
			return nil
		}
		disk += blocks * 512
		return nil
	})
	if err != nil {
		b.Fatalf("measure installed projection: %v", err)
	}
	return logical, disk, diskAvailable
}

func allocatedFilesystemBlocks(info fs.FileInfo) (uint64, bool) {
	value := reflect.ValueOf(info.Sys())
	if !value.IsValid() {
		return 0, false
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return 0, false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return 0, false
	}
	blocks := value.FieldByName("Blocks")
	if !blocks.IsValid() {
		return 0, false
	}
	switch blocks.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if blocks.Int() < 0 {
			return 0, false
		}
		return uint64(blocks.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return blocks.Uint(), true
	default:
		return 0, false
	}
}
