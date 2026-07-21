package bleveindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/search"
)

const (
	localCorpusScaleRecordsEnvironment = "FM_LOCALCORPUS_SCALE_RECORDS"
	localCorpusScaleMemoryEnvironment  = "FM_LOCALCORPUS_SCALE_EXPECT_CGROUP_MEMORY_BYTES"
	localCorpusScaleDomainCount        = 2_048
)

type localCorpusScaleResult struct {
	Schema                 int     `json:"schema"`
	Records                int     `json:"records"`
	Languages              int     `json:"languages"`
	Domains                int     `json:"domains"`
	DocumentLogicalBytes   uint64  `json:"document_logical_bytes"`
	IndexLogicalBytes      uint64  `json:"index_logical_bytes"`
	IndexDiskBytes         uint64  `json:"index_disk_bytes,omitempty"`
	InitialOpenMillis      float64 `json:"initial_open_ms"`
	IndexMillis            float64 `json:"index_ms"`
	IndexRecordsPerSecond  float64 `json:"index_records_per_second"`
	CloseMillis            float64 `json:"close_ms"`
	ReopenMillis           float64 `json:"reopen_ms"`
	FirstSearchMillis      float64 `json:"first_search_ms"`
	Searches               int     `json:"searches"`
	SearchP50Millis        float64 `json:"search_p50_ms"`
	SearchP95Millis        float64 `json:"search_p95_ms"`
	CgroupMemoryLimitBytes uint64  `json:"cgroup_memory_limit_bytes,omitempty"`
	CgroupMemoryPeakBytes  uint64  `json:"cgroup_memory_peak_bytes,omitempty"`
	GoVersion              string  `json:"go_version"`
	GOOS                   string  `json:"goos"`
	GOARCH                 string  `json:"goarch"`
	GOMAXPROCS             int     `json:"gomaxprocs"`
}

// TestLocalCorpusScaleProfile is an opt-in, deterministic measurement of the
// production persistent path. It deliberately uses Reconcile one document at
// a time: the benchmark must not hide request-path write costs behind a private
// bulk loader. Ordinary tests skip it, and public index limits remain unchanged.
func TestLocalCorpusScaleProfile(t *testing.T) {
	rawRecords := strings.TrimSpace(os.Getenv(localCorpusScaleRecordsEnvironment))
	records, err := parseLocalCorpusScaleRecords(rawRecords)
	if err != nil {
		t.Fatal(err)
	}
	if records == 0 {
		t.Skip("set FM_LOCALCORPUS_SCALE_RECORDS to 10000, 50000, or 100000")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Minute)
	defer cancel()
	path := filepath.Join(t.TempDir(), "local-corpus.bleve")
	options := Options{
		Path: path, MaxDocumentBytes: defaultMaxDocumentSize,
		MaxBytes: defaultMaxIndexBytes, MaxDocuments: records,
	}

	openStarted := time.Now()
	initialIndex, err := Open(options)
	initialOpenDuration := time.Since(openStarted)
	if err != nil {
		t.Fatalf("create persistent index: %v", err)
	}
	t.Cleanup(func() { _ = initialIndex.Close() })

	var documentLogicalBytes uint64
	indexStarted := time.Now()
	for recordNumber := 0; recordNumber < records; recordNumber++ {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		document := localCorpusScaleDocument(recordNumber)
		documentLogicalBytes += uint64(documentSize(document))
		if err := initialIndex.Reconcile(ctx, document); err != nil {
			t.Fatalf("index record %d: %v", recordNumber, err)
		}
	}
	indexDuration := time.Since(indexStarted)
	if count, err := initialIndex.Count(); err != nil || count != uint64(records) {
		t.Fatalf("count after indexing = %d, %v; want %d", count, err, records)
	}

	closeStarted := time.Now()
	if err := initialIndex.Close(); err != nil {
		t.Fatalf("close populated index: %v", err)
	}
	closeDuration := time.Since(closeStarted)
	indexLogicalBytes, indexDiskBytes, diskAvailable := localCorpusScaleDirectorySize(t, path)
	if indexLogicalBytes == 0 {
		t.Fatal("persistent index has zero logical bytes")
	}

	reopenStarted := time.Now()
	reopenedIndex, err := Open(options)
	reopenDuration := time.Since(reopenStarted)
	if err != nil {
		t.Fatalf("reopen populated index: %v", err)
	}
	t.Cleanup(func() { _ = reopenedIndex.Close() })
	if count, err := reopenedIndex.Count(); err != nil || count != uint64(records) {
		t.Fatalf("count after reopen = %d, %v; want %d", count, err, records)
	}

	queries := localCorpusScaleQueries()
	var firstSearchDuration time.Duration
	for queryNumber, query := range queries {
		started := time.Now()
		batch, err := reopenedIndex.SearchBatch(ctx, query)
		duration := time.Since(started)
		if queryNumber == 0 {
			firstSearchDuration = duration
		}
		validateLocalCorpusScaleBatch(t, query, batch, err)
	}
	latencies := make([]time.Duration, 0, len(queries)*4)
	for iteration := 0; iteration < 4; iteration++ {
		for _, query := range queries {
			started := time.Now()
			batch, err := reopenedIndex.SearchBatch(ctx, query)
			latencies = append(latencies, time.Since(started))
			validateLocalCorpusScaleBatch(t, query, batch, err)
		}
	}
	if err := reopenedIndex.Close(); err != nil {
		t.Fatalf("close reopened index: %v", err)
	}
	sort.Slice(latencies, func(left, right int) bool { return latencies[left] < latencies[right] })
	cgroupLimit, cgroupPeak := localCorpusScaleCgroupMemoryEvidence(t)

	result := localCorpusScaleResult{
		Schema: 1, Records: records, Languages: len(localCorpusScaleLanguages), Domains: localCorpusScaleDomainCount,
		DocumentLogicalBytes: documentLogicalBytes, IndexLogicalBytes: indexLogicalBytes,
		InitialOpenMillis:      localCorpusScaleMilliseconds(initialOpenDuration),
		IndexMillis:            localCorpusScaleMilliseconds(indexDuration),
		IndexRecordsPerSecond:  float64(records) / indexDuration.Seconds(),
		CloseMillis:            localCorpusScaleMilliseconds(closeDuration),
		ReopenMillis:           localCorpusScaleMilliseconds(reopenDuration),
		FirstSearchMillis:      localCorpusScaleMilliseconds(firstSearchDuration),
		Searches:               len(latencies),
		SearchP50Millis:        localCorpusScaleMilliseconds(localCorpusScalePercentile(latencies, 50)),
		SearchP95Millis:        localCorpusScaleMilliseconds(localCorpusScalePercentile(latencies, 95)),
		CgroupMemoryLimitBytes: cgroupLimit, CgroupMemoryPeakBytes: cgroupPeak,
		GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
	if diskAvailable {
		result.IndexDiskBytes = indexDiskBytes
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("FETCHMARK_LOCALCORPUS_SCALE_RESULT=%s\n", raw)
}

func TestParseLocalCorpusScaleRecords(t *testing.T) {
	tests := []struct {
		raw       string
		want      int
		wantError bool
	}{
		{raw: "10000", want: 10_000},
		{raw: "50000", want: 50_000},
		{raw: "100000", want: 100_000},
		{raw: ""},
		{raw: "9999", wantError: true},
		{raw: "10001", wantError: true},
		{raw: "100001", wantError: true},
		{raw: "ten-thousand", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.raw, func(t *testing.T) {
			got, err := parseLocalCorpusScaleRecords(test.raw)
			if (err != nil) != test.wantError {
				t.Fatalf("parseLocalCorpusScaleRecords(%q) error = %v, wantError %v", test.raw, err, test.wantError)
			}
			if got != test.want {
				t.Fatalf("parseLocalCorpusScaleRecords(%q) = %d, want %d", test.raw, got, test.want)
			}
		})
	}
}

func parseLocalCorpusScaleRecords(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	records, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be 10000, 50000, or 100000", localCorpusScaleRecordsEnvironment)
	}
	switch records {
	case 10_000, 50_000, 100_000:
		return records, nil
	default:
		return 0, fmt.Errorf("%s must be 10000, 50000, or 100000", localCorpusScaleRecordsEnvironment)
	}
}

func TestValidateLocalCorpusScaleCgroupMemoryEvidence(t *testing.T) {
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
			err := validateLocalCorpusScaleCgroupMemoryEvidence(test.expected, test.limit, test.peak)
			if (err != nil) != test.wantError {
				t.Fatalf("validateLocalCorpusScaleCgroupMemoryEvidence = %v, wantError %v", err, test.wantError)
			}
		})
	}
}

var (
	localCorpusScaleLanguages  = []string{"de", "en", "es", "fr", "it", "ja", "nl", "pt"}
	localCorpusScaleVerticals  = []string{"developer", "research", "knowledge", "fresh", "government", "culture", "science", "smallweb"}
	localCorpusScaleTopics     = []string{"compiler", "database", "network", "security", "climate", "biology", "history", "mathematics", "robotics", "language", "archives", "governance", "medicine", "astronomy", "literature", "geography"}
	localCorpusScaleQualifiers = []string{"practical", "foundational", "distributed", "verified", "regional", "historical", "experimental", "accessible", "resilient", "independent", "structured", "comparative", "sustainable", "public", "technical", "open"}
)

func localCorpusScaleDocument(number int) localcorpus.Document {
	language := localCorpusScaleLanguages[number%len(localCorpusScaleLanguages)]
	vertical := localCorpusScaleVerticals[(number/3)%len(localCorpusScaleVerticals)]
	topic := localCorpusScaleTopics[(number*7+number/17)%len(localCorpusScaleTopics)]
	qualifier := localCorpusScaleQualifiers[(number*11+number/29)%len(localCorpusScaleQualifiers)]
	host := fmt.Sprintf("site-%04d.example", number%localCorpusScaleDomainCount)
	fetchedAt := time.Date(2026, time.June, 30, number%24, number%60, 0, 0, time.UTC)
	publishedAt := fetchedAt.Add(-time.Duration(24+number%720) * time.Hour)
	body := fmt.Sprintf(
		"A %s %s guide to %s for independent web discovery. The document records verified methods, regional context, implementation notes, operational constraints, and reproducible evidence for dataset group %04d. It remains intentionally lexical and CPU-only so a self-hosted Fetchmark node can retrieve it without a model or paid search service.",
		qualifier, vertical, topic, number%2048,
	)
	contentHash := sha256.Sum256([]byte(body))
	return localcorpus.Document{
		URL:      fmt.Sprintf("https://%s/%s/%s/%02d/document-%06d", host, language, topic, number%97, number),
		Title:    fmt.Sprintf("%s %s %s guide %06d", qualifier, vertical, topic, number),
		Headings: []string{fmt.Sprintf("%s methods and evidence", topic), fmt.Sprintf("%s collection %03d", qualifier, number%997)},
		Body:     body, Language: language, Author: fmt.Sprintf("Open Corpus Author %03d", number%256),
		PublishedAt: &publishedAt, FetchedAt: fetchedAt,
		ContentHash: hex.EncodeToString(contentHash[:]),
		OutboundLinks: []string{
			fmt.Sprintf("https://reference-%03d.example/%s", number%512, topic),
			fmt.Sprintf("https://archive-%03d.example/%s", number%256, vertical),
		},
		Provenance: []string{"synthetic-local-corpus-scale", "operator-permitted"},
		MIME:       "text/html", ExtractionStatus: "extracted",
		SafetyClassification: localcorpus.SafetySafe,
		IndexingDisposition:  localcorpus.DispositionPermitted,
	}
}

func localCorpusScaleQueries() []search.Query {
	safe := 1
	queries := make([]search.Query, 0, len(localCorpusScaleTopics)+len(localCorpusScaleLanguages))
	for _, topic := range localCorpusScaleTopics {
		queries = append(queries, search.Query{Q: topic, SafeSearch: &safe, MaxResults: 10})
	}
	for index, language := range localCorpusScaleLanguages {
		queries = append(queries, search.Query{Q: localCorpusScaleTopics[index], Language: language, SafeSearch: &safe, MaxResults: 10})
	}
	return queries
}

func validateLocalCorpusScaleBatch(t *testing.T, query search.Query, batch search.SearchBatch, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("search %q: %v", query.Q, err)
	}
	if len(batch.Hits) == 0 || batch.Provider != providerID || batch.Status != search.BatchHealthy {
		t.Fatalf("search %q returned invalid batch %#v", query.Q, batch)
	}
}

func localCorpusScaleDirectorySize(t *testing.T, root string) (logical, disk uint64, diskAvailable bool) {
	t.Helper()
	diskAvailable = true
	if err := filepath.WalkDir(root, func(_ string, entry fs.DirEntry, walkErr error) error {
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
		blocks, ok := localCorpusAllocatedFilesystemBlocks(info)
		if !ok {
			diskAvailable = false
			return nil
		}
		disk += blocks * 512
		return nil
	}); err != nil {
		t.Fatalf("measure persistent index: %v", err)
	}
	return logical, disk, diskAvailable
}

func localCorpusAllocatedFilesystemBlocks(info fs.FileInfo) (uint64, bool) {
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

func localCorpusScalePercentile(sorted []time.Duration, percentile int) time.Duration {
	index := (len(sorted)*percentile + 99) / 100
	if index < 1 {
		index = 1
	}
	return sorted[index-1]
}

func localCorpusScaleMilliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

func localCorpusScaleCgroupMemoryEvidence(t *testing.T) (uint64, uint64) {
	t.Helper()
	expectedRaw := strings.TrimSpace(os.Getenv(localCorpusScaleMemoryEnvironment))
	if expectedRaw == "" {
		limit, _ := readLocalCorpusScaleCgroupUint("/sys/fs/cgroup/memory.max")
		peak, _ := readLocalCorpusScaleCgroupUint("/sys/fs/cgroup/memory.peak")
		return limit, peak
	}
	expected, err := strconv.ParseUint(expectedRaw, 10, 64)
	if err != nil || expected == 0 {
		t.Fatalf("%s must be a positive integer", localCorpusScaleMemoryEnvironment)
	}
	limit, err := readLocalCorpusScaleCgroupUint("/sys/fs/cgroup/memory.max")
	if err != nil {
		t.Fatalf("read required cgroup memory limit: %v", err)
	}
	peak, err := readLocalCorpusScaleCgroupUint("/sys/fs/cgroup/memory.peak")
	if err != nil {
		t.Fatalf("read required cgroup peak memory: %v", err)
	}
	if err := validateLocalCorpusScaleCgroupMemoryEvidence(expected, limit, peak); err != nil {
		t.Fatal(err)
	}
	return limit, peak
}

func validateLocalCorpusScaleCgroupMemoryEvidence(expected, limit, peak uint64) error {
	if expected == 0 {
		return fmt.Errorf("expected cgroup memory limit must be positive")
	}
	if limit != expected {
		return fmt.Errorf("cgroup memory limit = %d, want %d", limit, expected)
	}
	if peak == 0 || peak > limit {
		return fmt.Errorf("cgroup peak memory = %d, want 1..%d", peak, limit)
	}
	return nil
}

func readLocalCorpusScaleCgroupUint(path string) (uint64, error) {
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
