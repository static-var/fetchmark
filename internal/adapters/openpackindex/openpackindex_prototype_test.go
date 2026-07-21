package openpackindex

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
)

const (
	prototypeMinRecords        = 100_000
	prototypeMaxRecords        = 1_000_000
	prototypeShards            = 4
	prototypeCompressedBytes   = 256 << 20
	prototypeUncompressedBytes = 1 << 30
	prototypeProjectionBytes   = 3 << 30
)

type prototypeShardEvidence struct {
	Path               string `json:"path"`
	Records            uint64 `json:"records"`
	CompressedBytes    uint64 `json:"compressed_bytes"`
	UncompressedBytes  uint64 `json:"uncompressed_bytes"`
	CompressedSHA256   string `json:"compressed_sha256"`
	UncompressedSHA256 string `json:"uncompressed_sha256"`
}

type prototypeFixture struct {
	bundle            string
	root              string
	manifest          indexpack.Manifest
	manifestSHA256    string
	publicKey         ed25519.PublicKey
	acceptance        indexpack.Acceptance
	compressedBytes   uint64
	uncompressedBytes uint64
	domains           int
	shards            []prototypeShardEvidence
}

type prototypeResult struct {
	Schema                  int                      `json:"schema"`
	Records                 uint64                   `json:"records"`
	Languages               int                      `json:"languages"`
	Domains                 int                      `json:"domains"`
	ManifestSHA256          string                   `json:"manifest_sha256"`
	Shards                  []prototypeShardEvidence `json:"shards"`
	CompressedBytes         uint64                   `json:"compressed_bytes"`
	UncompressedBytes       uint64                   `json:"uncompressed_bytes"`
	ProjectionLogicalBytes  uint64                   `json:"projection_logical_bytes"`
	ProjectionDiskBytes     uint64                   `json:"projection_disk_bytes,omitempty"`
	InstallMilliseconds     float64                  `json:"install_ms"`
	InstallRecordsPerSec    float64                  `json:"install_records_per_second"`
	InstallAllocatedBytes   uint64                   `json:"install_allocated_bytes"`
	ColdOpenMilliseconds    float64                  `json:"cold_open_ms"`
	Searches                int                      `json:"searches"`
	SearchP50Milliseconds   float64                  `json:"search_p50_ms"`
	SearchP95Milliseconds   float64                  `json:"search_p95_ms"`
	PolicyMaxShards         int                      `json:"policy_max_shards"`
	PolicyMaxRecords        uint64                   `json:"policy_max_records"`
	PolicyCompressedBytes   uint64                   `json:"policy_compressed_bytes"`
	PolicyUncompressedBytes uint64                   `json:"policy_uncompressed_bytes"`
	PolicyProjectionBytes   uint64                   `json:"policy_projection_bytes"`
	CgroupMemoryLimitBytes  uint64                   `json:"cgroup_memory_limit_bytes"`
	CgroupMemoryPeakBytes   uint64                   `json:"cgroup_memory_peak_bytes"`
	GoVersion               string                   `json:"go_version"`
	GOOS                    string                   `json:"goos"`
	GOARCH                  string                   `json:"goarch"`
	GOMAXPROCS              int                      `json:"gomaxprocs"`
}

// TestOpenPackMillionPrototype is a separate opt-in prototype. Its larger
// unexported policy cannot be selected by Install, Open, or fetchmark-pack.
func TestOpenPackMillionPrototype(t *testing.T) {
	rawCount := strings.TrimSpace(os.Getenv("FM_OPENPACK_PROTOTYPE_RECORDS"))
	if rawCount == "" {
		t.Skip("set FM_OPENPACK_PROTOTYPE_RECORDS to run the opt-in million-record prototype")
	}
	records, err := strconv.ParseUint(rawCount, 10, 64)
	if err != nil || records < prototypeMinRecords || records > prototypeMaxRecords {
		t.Fatalf("FM_OPENPACK_PROTOTYPE_RECORDS must be %d..%d", prototypeMinRecords, prototypeMaxRecords)
	}

	fixture := newPrototypeFixture(t, records)
	policy := installPolicy{
		maxShards: prototypeShards, maxRecords: records,
		maxCompressedBytes: prototypeCompressedBytes, maxUncompressedBytes: prototypeUncompressedBytes,
		maxProjectionBytes: prototypeProjectionBytes, maxDecoderMemoryBytes: indexpack.MaxUncompressedShardBytes,
	}
	if err := policy.validate(); err != nil {
		t.Fatalf("prototype install policy: %v", err)
	}
	options := InstallOptions{
		BundleDir: fixture.bundle, Root: fixture.root,
		TrustedKeys: map[string]ed25519.PublicKey{indexpack.KeyID(fixture.publicKey): fixture.publicKey},
		Acceptance:  fixture.acceptance, MaxProjectionBytes: policy.maxProjectionBytes,
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	installed, err := install(context.Background(), options, os.RemoveAll, verifyClosedProjection, policy)
	installDuration := time.Since(started)
	if err != nil {
		t.Fatalf("install million-record prototype: %v", err)
	}
	runtime.ReadMemStats(&after)
	if installed.RecordCount != records || installed.ProjectionBytes == 0 || installed.ProjectionBytes > policy.maxProjectionBytes {
		t.Fatalf("installed prototype identity/size = %#v", installed)
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
		Now: fixture.acceptance.Now, ProviderID: "prototype-open-pack",
	}
	openStarted := time.Now()
	index, err := open(openOptions, records)
	coldOpenDuration := time.Since(openStarted)
	if err != nil {
		t.Fatalf("reopen million-record prototype: %v", err)
	}
	defer index.Close()

	queries := scaleQueries()
	latencies := make([]time.Duration, 0, len(queries)*4)
	for iteration := 0; iteration < 4; iteration++ {
		for _, query := range queries {
			searchStarted := time.Now()
			batch, err := index.SearchBatch(context.Background(), query)
			latencies = append(latencies, time.Since(searchStarted))
			if err != nil {
				t.Fatalf("prototype search %q: %v", query.Q, err)
			}
			if len(batch.Hits) == 0 || batch.Provider != "prototype-open-pack" {
				t.Fatalf("prototype search %q returned invalid batch %#v", query.Q, batch)
			}
		}
	}
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	cgroupLimit, cgroupPeak := scaleCgroupMemoryEvidence(t)

	result := prototypeResult{
		Schema: 1, Records: records, Languages: len(scaleLanguages), Domains: fixture.domains,
		ManifestSHA256: fixture.manifestSHA256, Shards: fixture.shards,
		CompressedBytes: fixture.compressedBytes, UncompressedBytes: fixture.uncompressedBytes,
		ProjectionLogicalBytes: installed.ProjectionBytes,
		InstallMilliseconds:    milliseconds(installDuration), InstallRecordsPerSec: float64(records) / installDuration.Seconds(),
		InstallAllocatedBytes: after.TotalAlloc - before.TotalAlloc, ColdOpenMilliseconds: milliseconds(coldOpenDuration),
		Searches: len(latencies), SearchP50Milliseconds: milliseconds(percentileDuration(latencies, 50)),
		SearchP95Milliseconds: milliseconds(percentileDuration(latencies, 95)),
		PolicyMaxShards:       policy.maxShards, PolicyMaxRecords: policy.maxRecords,
		PolicyCompressedBytes: policy.maxCompressedBytes, PolicyUncompressedBytes: policy.maxUncompressedBytes,
		PolicyProjectionBytes:  policy.maxProjectionBytes,
		CgroupMemoryLimitBytes: cgroupLimit, CgroupMemoryPeakBytes: cgroupPeak,
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
	if diskAvailable {
		result.ProjectionDiskBytes = diskBytes
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("FETCHMARK_OPENPACK_PROTOTYPE_RESULT=%s\n", raw)
}

func newPrototypeFixture(t *testing.T, records uint64) prototypeFixture {
	t.Helper()
	base := t.TempDir()
	bundle := filepath.Join(base, "bundle")
	root := filepath.Join(base, "projection")
	if err := os.Mkdir(bundle, 0o700); err != nil {
		t.Fatal(err)
	}
	domains := make(map[string]struct{}, scaleDomainCount)
	descriptors := make([]indexpack.Shard, 0, prototypeShards)
	evidenceByPath := make(map[string]prototypeShardEvidence, prototypeShards)
	var compressedTotal, uncompressedTotal uint64
	start := uint64(0)
	for shardNumber := 0; shardNumber < prototypeShards; shardNumber++ {
		count := records / prototypeShards
		if uint64(shardNumber) < records%prototypeShards {
			count++
		}
		descriptor, evidence := writePrototypeShard(t, bundle, shardNumber, start, count, domains)
		descriptors = append(descriptors, descriptor)
		evidenceByPath[descriptor.Path] = evidence
		compressedTotal += descriptor.CompressedSizeBytes
		uncompressedTotal += descriptor.UncompressedSizeBytes
		start += count
	}
	if start != records || len(domains) != scaleDomainCount {
		t.Fatalf("prototype fixture records/domains = %d/%d, want %d/%d", start, len(domains), records, scaleDomainCount)
	}
	sort.Slice(descriptors, func(left, right int) bool { return descriptors[left].Path < descriptors[right].Path })
	shardEvidence := make([]prototypeShardEvidence, 0, len(descriptors))
	aggregateInput := sha256.New()
	for _, descriptor := range descriptors {
		evidence := evidenceByPath[descriptor.Path]
		shardEvidence = append(shardEvidence, evidence)
		_, _ = fmt.Fprintf(aggregateInput, "%s %s %s\n", evidence.Path, evidence.CompressedSHA256, evidence.UncompressedSHA256)
	}

	seed := sha256.Sum256([]byte("fetchmark-million-open-pack-prototype-v1"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	publicKey := privateKey.Public().(ed25519.PublicKey)
	manifest := indexpack.Manifest{
		Version: indexpack.Version, Kind: indexpack.KindSnapshot, PackID: "varied-million-prototype-v1", Revision: 1,
		CreatedAt: "2026-07-01T00:00:00Z", ExpiresAt: "2027-06-30T23:59:59Z", SigningKeyID: indexpack.KeyID(publicKey),
		Publisher: indexpack.Publisher{
			Name: "Fetchmark million-record prototype", ContactURI: "mailto:benchmark@example.com",
			TakedownURI: "https://benchmark.example/takedown", RightsNotice: "synthetic benchmark metadata",
		},
		Policy:    indexpack.Policy{Robots: "rfc9309", NoIndex: "exclude", BodyDistribution: "forbidden"},
		Languages: append([]string(nil), scaleLanguages...), RecordCount: records, Shards: descriptors,
		Build: indexpack.Build{
			Generator: "fetchmark-million-prototype", GeneratorVersion: "1", Analyzer: "unicode-lexical", AnalyzerVersion: "1",
			PolicySHA256:     indexpack.Digest([]byte("million-prototype-policy-v1")),
			ExclusionsSHA256: indexpack.Digest([]byte("million-prototype-exclusions-v1")),
			Inputs: []indexpack.BuildInput{{
				Name: "deterministic-four-shard-metadata", URI: "https://benchmark.example/million-prototype-v1",
				RetrievedAt: "2026-06-30T12:00:00Z", SHA256: hex.EncodeToString(aggregateInput.Sum(nil)),
				RightsNotice: "synthetic benchmark metadata",
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
	return prototypeFixture{
		bundle: bundle, root: root, manifest: manifest, manifestSHA256: manifestSHA256,
		publicKey: publicKey, compressedBytes: compressedTotal, uncompressedBytes: uncompressedTotal,
		domains: len(domains), shards: shardEvidence,
		acceptance: indexpack.Acceptance{
			Now: time.Date(2026, time.July, 18, 12, 0, 0, 0, time.UTC), ExpectedPackID: manifest.PackID,
			ExpectedManifestSHA256: manifestSHA256, ExpectedKeyID: indexpack.KeyID(publicKey), MinimumRevision: 1,
			ExpectedRevision: manifest.Revision, ExpectedRecordCount: records,
			ExpectedCreatedAt: manifest.CreatedAt, ExpectedExpiresAt: manifest.ExpiresAt,
		},
	}
}

func writePrototypeShard(
	t *testing.T,
	bundle string,
	shardNumber int,
	start, count uint64,
	domains map[string]struct{},
) (indexpack.Shard, prototypeShardEvidence) {
	t.Helper()
	temporary := filepath.Join(bundle, fmt.Sprintf("building-%02d.ndjson.zst", shardNumber))
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
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
	for number := start; number < start+count; number++ {
		record := scaleRecord(number)
		parsed, parseErr := url.Parse(record.URL)
		if parseErr != nil || parsed.Hostname() == "" {
			t.Fatalf("parse generated record %d URL: %v", number, parseErr)
		}
		domains[parsed.Hostname()] = struct{}{}
		if err := jsonEncoder.Encode(record); err != nil {
			_ = encoder.Close()
			_ = file.Close()
			t.Fatalf("encode prototype record %d: %v", number, err)
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
	compressedSHA256 := compressedDigest.digest()
	uncompressedSHA256 := uncompressedDigest.digest()
	relative := indexpack.ShardPath(compressedSHA256)
	destination := filepath.Join(bundle, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, destination); err != nil {
		t.Fatal(err)
	}
	descriptor := indexpack.Shard{
		Path: relative, Compression: "zstd", SHA256: compressedSHA256,
		CompressedSizeBytes: compressedDigest.bytes, UncompressedSHA256: uncompressedSHA256,
		UncompressedSizeBytes: uncompressedDigest.bytes, RecordCount: count,
	}
	return descriptor, prototypeShardEvidence{
		Path: relative, Records: count, CompressedBytes: compressedDigest.bytes, UncompressedBytes: uncompressedDigest.bytes,
		CompressedSHA256: compressedSHA256, UncompressedSHA256: uncompressedSHA256,
	}
}
