package openpackindex

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const maxOptInScaleRecords = 100_000

type scaleFixture struct {
	bundle             string
	root               string
	manifest           indexpack.Manifest
	manifestSHA256     string
	publicKey          ed25519.PublicKey
	acceptance         indexpack.Acceptance
	compressedBytes    uint64
	uncompressedBytes  uint64
	compressedSHA256   string
	uncompressedSHA256 string
	domains            int
}

type scaleResult struct {
	Schema                 int     `json:"schema"`
	Records                uint64  `json:"records"`
	Languages              int     `json:"languages"`
	Domains                int     `json:"domains"`
	CompressedBytes        uint64  `json:"compressed_bytes"`
	UncompressedBytes      uint64  `json:"uncompressed_bytes"`
	ManifestSHA256         string  `json:"manifest_sha256"`
	CompressedSHA256       string  `json:"compressed_sha256"`
	UncompressedSHA256     string  `json:"uncompressed_sha256"`
	ProjectionLogicalBytes uint64  `json:"projection_logical_bytes"`
	ProjectionDiskBytes    uint64  `json:"projection_disk_bytes,omitempty"`
	InstallMilliseconds    float64 `json:"install_ms"`
	InstallRecordsPerSec   float64 `json:"install_records_per_second"`
	InstallAllocatedBytes  uint64  `json:"install_allocated_bytes"`
	ColdOpenMilliseconds   float64 `json:"cold_open_ms"`
	Searches               int     `json:"searches"`
	SearchP50Milliseconds  float64 `json:"search_p50_ms"`
	SearchP95Milliseconds  float64 `json:"search_p95_ms"`
	CgroupMemoryLimitBytes uint64  `json:"cgroup_memory_limit_bytes,omitempty"`
	CgroupMemoryPeakBytes  uint64  `json:"cgroup_memory_peak_bytes,omitempty"`
	GoVersion              string  `json:"go_version"`
	GOOS                   string  `json:"goos"`
	GOARCH                 string  `json:"goarch"`
	GOMAXPROCS             int     `json:"gomaxprocs"`
}

// TestOpenPackScaleProfile is an opt-in, deterministic scale gate. Production
// Install and Open retain MaxInstallRecords; only this same-package test passes
// a larger unexported policy so the exact streaming/project/reopen/search path
// can be measured before changing any public limit.
func TestOpenPackScaleProfile(t *testing.T) {
	rawCount := strings.TrimSpace(os.Getenv("FM_OPENPACK_SCALE_RECORDS"))
	if rawCount == "" {
		t.Skip("set FM_OPENPACK_SCALE_RECORDS to run the opt-in scale profile")
	}
	records, err := strconv.ParseUint(rawCount, 10, 64)
	if err != nil || records < MaxInstallRecords || records > maxOptInScaleRecords {
		t.Fatalf("FM_OPENPACK_SCALE_RECORDS must be %d..%d", MaxInstallRecords, maxOptInScaleRecords)
	}

	fixture := newScaleFixture(t, records)
	options := InstallOptions{
		BundleDir: fixture.bundle, Root: fixture.root,
		TrustedKeys:        map[string]ed25519.PublicKey{indexpack.KeyID(fixture.publicKey): fixture.publicKey},
		Acceptance:         fixture.acceptance,
		MaxProjectionBytes: MaxProjectionBytes,
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	installStarted := time.Now()
	policy := productionInstallPolicy()
	policy.maxRecords = records
	installed, err := install(
		context.Background(), options, os.RemoveAll, verifyClosedProjection,
		policy,
	)
	installDuration := time.Since(installStarted)
	if err != nil {
		t.Fatalf("install varied scale fixture: %v", err)
	}
	runtime.ReadMemStats(&after)
	if installed.RecordCount != records || installed.ProjectionBytes == 0 || installed.ProjectionBytes > MaxProjectionBytes {
		t.Fatalf("installed identity/size = %#v", installed)
	}

	logicalBytes, diskBytes, diskAvailable := scaleDirectorySize(t, installed.Path)
	if logicalBytes != installed.ProjectionBytes {
		t.Fatalf("reported projection bytes %d differ from measured %d", installed.ProjectionBytes, logicalBytes)
	}
	openOptions := OpenOptions{
		Path: installed.Path, ExpectedManifestSHA256: fixture.manifestSHA256,
		ExpectedPackID: fixture.manifest.PackID, ExpectedRevision: fixture.manifest.Revision,
		ExpectedRecordCount: records, ExpectedKeyID: fixture.acceptance.ExpectedKeyID,
		ExpectedCreatedAt: fixture.manifest.CreatedAt, ExpectedExpiresAt: fixture.manifest.ExpiresAt,
		Now: fixture.acceptance.Now, ProviderID: "scale-open-pack",
	}
	openStarted := time.Now()
	index, err := open(openOptions, records)
	coldOpenDuration := time.Since(openStarted)
	if err != nil {
		t.Fatalf("reopen scale projection: %v", err)
	}
	defer index.Close()

	queries := scaleQueries()
	latencies := make([]time.Duration, 0, len(queries)*4)
	for iteration := 0; iteration < 4; iteration++ {
		for _, query := range queries {
			started := time.Now()
			batch, err := index.SearchBatch(context.Background(), query)
			latencies = append(latencies, time.Since(started))
			if err != nil {
				t.Fatalf("search %q: %v", query.Q, err)
			}
			if len(batch.Hits) == 0 || batch.Provider != "scale-open-pack" {
				t.Fatalf("search %q returned invalid batch %#v", query.Q, batch)
			}
		}
	}
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	cgroupLimit, cgroupPeak := scaleCgroupMemoryEvidence(t)

	result := scaleResult{
		Schema: 1, Records: records, Languages: len(scaleLanguages), Domains: fixture.domains,
		CompressedBytes: fixture.compressedBytes, UncompressedBytes: fixture.uncompressedBytes,
		ManifestSHA256: fixture.manifestSHA256, CompressedSHA256: fixture.compressedSHA256,
		UncompressedSHA256:     fixture.uncompressedSHA256,
		ProjectionLogicalBytes: installed.ProjectionBytes,
		InstallMilliseconds:    milliseconds(installDuration),
		InstallRecordsPerSec:   float64(records) / installDuration.Seconds(),
		InstallAllocatedBytes:  after.TotalAlloc - before.TotalAlloc,
		ColdOpenMilliseconds:   milliseconds(coldOpenDuration),
		Searches:               len(latencies),
		SearchP50Milliseconds:  milliseconds(percentileDuration(latencies, 50)),
		SearchP95Milliseconds:  milliseconds(percentileDuration(latencies, 95)),
		CgroupMemoryLimitBytes: cgroupLimit,
		CgroupMemoryPeakBytes:  cgroupPeak,
		GoVersion:              runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
	if diskAvailable {
		result.ProjectionDiskBytes = diskBytes
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("FETCHMARK_OPENPACK_SCALE_RESULT=%s\n", raw)
}

const scaleDomainCount = 8_192

var (
	scaleLanguages = []string{"de", "en", "es", "fr", "it", "ja", "nl", "pt"}
	scaleVerticals = []string{"developer", "research", "knowledge", "fresh", "government", "culture", "science", "smallweb"}
	scaleTopics    = []string{
		"compiler", "database", "network", "security", "climate", "biology", "history", "mathematics",
		"robotics", "language", "archives", "governance", "medicine", "astronomy", "literature", "geography",
	}
	scaleQualifiers = []string{
		"practical", "foundational", "distributed", "verified", "regional", "historical", "experimental", "accessible",
		"resilient", "independent", "structured", "comparative", "sustainable", "public", "technical", "open",
	}
)

type countingHash struct {
	hash  hash.Hash
	bytes uint64
}

func (writer *countingHash) Write(raw []byte) (int, error) {
	written, err := writer.hash.Write(raw)
	writer.bytes += uint64(written)
	return written, err
}

func (writer *countingHash) digest() string { return hex.EncodeToString(writer.hash.Sum(nil)) }

func newScaleFixture(t *testing.T, records uint64) scaleFixture {
	t.Helper()
	base := t.TempDir()
	bundle := filepath.Join(base, "bundle")
	root := filepath.Join(base, "projection")
	if err := os.Mkdir(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	temporaryShard := filepath.Join(bundle, "building.ndjson.zst")
	file, err := os.OpenFile(temporaryShard, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	compressedDigest := &countingHash{hash: sha256.New()}
	encoder, err := zstd.NewWriter(io.MultiWriter(file, compressedDigest), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(true))
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	uncompressedDigest := &countingHash{hash: sha256.New()}
	jsonEncoder := json.NewEncoder(io.MultiWriter(encoder, uncompressedDigest))
	jsonEncoder.SetEscapeHTML(false)
	domains := make(map[string]struct{}, scaleDomainCount)
	for recordNumber := uint64(0); recordNumber < records; recordNumber++ {
		record := scaleRecord(recordNumber)
		parsed, parseErr := url.Parse(record.URL)
		if parseErr != nil || parsed.Hostname() == "" {
			t.Fatalf("parse generated record %d URL: %v", recordNumber, parseErr)
		}
		domains[parsed.Hostname()] = struct{}{}
		if err := jsonEncoder.Encode(record); err != nil {
			_ = encoder.Close()
			_ = file.Close()
			t.Fatalf("encode record %d: %v", recordNumber, err)
		}
	}
	if err := encoder.Close(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if len(domains) != scaleDomainCount {
		t.Fatalf("generated domains = %d, want %d", len(domains), scaleDomainCount)
	}

	shardRelative := indexpack.ShardPath(compressedDigest.digest())
	shardPath := filepath.Join(bundle, filepath.FromSlash(shardRelative))
	if err := os.MkdirAll(filepath.Dir(shardPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporaryShard, shardPath); err != nil {
		t.Fatal(err)
	}
	seed := sha256.Sum256([]byte("fetchmark-varied-open-pack-scale-v1"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	publicKey := privateKey.Public().(ed25519.PublicKey)
	manifest := indexpack.Manifest{
		Version: indexpack.Version, Kind: indexpack.KindSnapshot, PackID: "varied-scale-open-v1", Revision: 1,
		CreatedAt: "2026-07-01T00:00:00Z", ExpiresAt: "2027-06-30T23:59:59Z", SigningKeyID: indexpack.KeyID(publicKey),
		Publisher: indexpack.Publisher{
			Name: "Fetchmark deterministic scale fixture", ContactURI: "mailto:benchmark@example.com",
			TakedownURI: "https://benchmark.example/takedown", RightsNotice: "synthetic benchmark metadata",
		},
		Policy:    indexpack.Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"},
		Languages: append([]string(nil), scaleLanguages...), RecordCount: records,
		Shards: []indexpack.Shard{{
			Path: shardRelative, Compression: "zstd", SHA256: compressedDigest.digest(),
			CompressedSizeBytes: compressedDigest.bytes, UncompressedSHA256: uncompressedDigest.digest(),
			UncompressedSizeBytes: uncompressedDigest.bytes, RecordCount: records,
		}},
		Build: indexpack.Build{
			Generator: "fetchmark-scale-test", GeneratorVersion: "1", Analyzer: "unicode-lexical", AnalyzerVersion: "1",
			PolicySHA256: indexpack.Digest([]byte("scale-policy-v1")), ExclusionsSHA256: indexpack.Digest([]byte("scale-exclusions-v1")),
			Inputs: []indexpack.BuildInput{{
				Name: "deterministic-varied-metadata", URI: "https://benchmark.example/varied-v1",
				RetrievedAt: "2026-06-30T12:00:00Z", SHA256: uncompressedDigest.digest(), RightsNotice: "synthetic benchmark metadata",
			}},
		},
	}
	manifestRaw, err := indexpack.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := indexpack.SignManifest(manifestRaw, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	signatureRaw, err := indexpack.EncodeSignature(signature)
	if err != nil {
		t.Fatal(err)
	}
	for path, raw := range map[string][]byte{
		filepath.Join(bundle, ManifestFilename):  manifestRaw,
		filepath.Join(bundle, SignatureFilename): signatureRaw,
	} {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifestSHA256 := indexpack.ManifestDigest(manifestRaw)
	return scaleFixture{
		bundle: bundle, root: root, manifest: manifest, manifestSHA256: manifestSHA256, publicKey: publicKey,
		acceptance: indexpack.Acceptance{
			Now: time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC), ExpectedPackID: manifest.PackID,
			ExpectedManifestSHA256: manifestSHA256, ExpectedKeyID: indexpack.KeyID(publicKey), MinimumRevision: 1,
			ExpectedRevision: manifest.Revision, ExpectedRecordCount: records,
			ExpectedCreatedAt: manifest.CreatedAt, ExpectedExpiresAt: manifest.ExpiresAt,
		},
		compressedBytes: compressedDigest.bytes, uncompressedBytes: uncompressedDigest.bytes,
		compressedSHA256: compressedDigest.digest(), uncompressedSHA256: uncompressedDigest.digest(), domains: len(domains),
	}
}

func scaleRecord(number uint64) indexpack.Record {
	language := scaleLanguages[number%uint64(len(scaleLanguages))]
	vertical := scaleVerticals[(number/3)%uint64(len(scaleVerticals))]
	topic := scaleTopics[(number*7+number/17)%uint64(len(scaleTopics))]
	qualifier := scaleQualifiers[(number*11+number/29)%uint64(len(scaleQualifiers))]
	host := fmt.Sprintf("site-%04d.example", number%scaleDomainCount)
	fetched := time.Date(2026, time.June, 30, int(number%24), int(number%60), 0, 0, time.UTC)
	record := indexpack.Record{
		Operation: indexpack.OperationUpsert,
		URL:       fmt.Sprintf("https://%s/%s/%s/%02d/document-%06d", host, language, topic, number%97, number),
		Title:     fmt.Sprintf("%s %s %s guide %06d", qualifier, vertical, topic, number),
		Headings: []string{
			fmt.Sprintf("%s methods and evidence", topic),
			fmt.Sprintf("%s collection %03d", qualifier, number%997),
		},
		AnchorTerms: []string{
			fmt.Sprintf("%s reference", topic),
			fmt.Sprintf("%s-%s", vertical, language),
			fmt.Sprintf("series-%04d", number%4096),
		},
		SalientSketch: fmt.Sprintf(
			"A %s %s note about %s, maintained for open discovery. Dataset group %04d records regional and technical context without distributing page bodies.",
			qualifier, vertical, topic, number%2048,
		),
		Language: language, FetchedAt: fetched.Format(time.RFC3339),
		ContentSHA256:  indexpack.Digest([]byte(fmt.Sprintf("varied-scale-content-%d", number))),
		AuthorityScore: uint16(100 + number%900), FreshnessScore: uint16(200 + number%800),
		Provenance: []indexpack.Provenance{{
			Source: "synthetic-" + vertical, SourceURI: "https://benchmark.example/source/" + vertical,
			RetrievedAt: "2026-06-29T00:00:00Z", RightsNotice: "synthetic benchmark metadata",
		}},
	}
	if number%5 != 0 {
		record.PublishedAt = fetched.Add(-time.Duration(24+number%720) * time.Hour).Format(time.RFC3339)
	}
	if number%7 == 0 {
		record.Headings = append(record.Headings, fmt.Sprintf("case study %05d", number%10000))
	}
	return record
}

func scaleQueries() []search.Query {
	queries := make([]search.Query, 0, len(scaleTopics)+len(scaleLanguages))
	for _, topic := range scaleTopics {
		queries = append(queries, search.Query{Q: topic, MaxResults: 10})
	}
	for index, language := range scaleLanguages {
		queries = append(queries, search.Query{Q: scaleTopics[index], Language: language, MaxResults: 10})
	}
	return queries
}

func scaleDirectorySize(t *testing.T, root string) (logical, disk uint64, diskAvailable bool) {
	t.Helper()
	diskAvailable = true
	if err := filepath.WalkDir(root, func(_ string, entry os.DirEntry, walkErr error) error {
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
	}); err != nil {
		t.Fatal(err)
	}
	return logical, disk, diskAvailable
}

func percentileDuration(sorted []time.Duration, percentile int) time.Duration {
	index := (len(sorted)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	return sorted[index-1]
}

func milliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

func scaleCgroupMemoryEvidence(t *testing.T) (uint64, uint64) {
	t.Helper()
	expectedRaw := strings.TrimSpace(os.Getenv("FM_OPENPACK_SCALE_EXPECT_CGROUP_MEMORY_BYTES"))
	if expectedRaw == "" {
		limit, _ := readCgroupUint("/sys/fs/cgroup/memory.max")
		peak, _ := readCgroupUint("/sys/fs/cgroup/memory.peak")
		return limit, peak
	}
	expected, err := strconv.ParseUint(expectedRaw, 10, 64)
	if err != nil || expected == 0 {
		t.Fatal("FM_OPENPACK_SCALE_EXPECT_CGROUP_MEMORY_BYTES must be a positive integer")
	}
	limit, err := readCgroupUint("/sys/fs/cgroup/memory.max")
	if err != nil {
		t.Fatalf("read required cgroup memory limit: %v", err)
	}
	peak, err := readCgroupUint("/sys/fs/cgroup/memory.peak")
	if err != nil {
		t.Fatalf("read required cgroup peak memory: %v", err)
	}
	if err := validateScaleCgroupMemoryEvidence(expected, limit, peak); err != nil {
		t.Fatal(err)
	}
	return limit, peak
}

func TestValidateScaleCgroupMemoryEvidence(t *testing.T) {
	tests := []struct {
		name                  string
		expected, limit, peak uint64
		wantError             bool
	}{
		{name: "valid", expected: 512, limit: 512, peak: 400},
		{name: "zero expected", limit: 512, peak: 400, wantError: true},
		{name: "limit mismatch", expected: 512, limit: 1024, peak: 400, wantError: true},
		{name: "zero peak", expected: 512, limit: 512, wantError: true},
		{name: "peak above limit", expected: 512, limit: 512, peak: 513, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateScaleCgroupMemoryEvidence(test.expected, test.limit, test.peak)
			if (err != nil) != test.wantError {
				t.Fatalf("validateScaleCgroupMemoryEvidence = %v, wantError %v", err, test.wantError)
			}
		})
	}
}

func validateScaleCgroupMemoryEvidence(expected, limit, peak uint64) error {
	if expected == 0 {
		return errors.New("expected cgroup memory limit must be positive")
	}
	if limit != expected {
		return fmt.Errorf("cgroup memory limit = %d, want %d", limit, expected)
	}
	if peak == 0 || peak > limit {
		return fmt.Errorf("cgroup peak memory = %d, want 1..%d", peak, limit)
	}
	return nil
}

func readCgroupUint(path string) (uint64, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	value := strings.TrimSpace(string(raw))
	if value == "" || value == "max" {
		return 0, fmt.Errorf("%s is not a finite integer", path)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}
