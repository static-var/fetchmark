package ccindex

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/staticvar/fetchmark/internal/core/indexpackadmission"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

const (
	FormatURLIndexParquetSelection             = "url-index-parquet-selection-v1"
	ParquetSelectionAlgorithm                  = "sha256-canonical-url-per-host-v1"
	MaxParquetSelectionRecords                 = indexpackadmission.MaxCollectorRecords
	MaxParquetPrototypeSelectionRecords uint64 = 5_000_000
	ParquetSelectionProfileCollector           = "collector-v1"
	ParquetSelectionProfilePrototype           = indexpackselection.ProfileFormatPrototype
	maxParquetSelectionLanguages               = 64
	parquetSelectionHashDomain                 = "fetchmark-common-crawl-url-selection-v1\n"
)

type ParquetSelectionOptions struct {
	Profile    string
	MaxRecords uint64
	Languages  []string
	NotAfter   time.Time
}

type ParquetSelectionReport struct {
	Algorithm           string            `json:"algorithm"`
	Profile             string            `json:"profile"`
	MaxRecords          uint64            `json:"max_records"`
	MaxRecordsPerHost   uint64            `json:"max_records_per_host"`
	Languages           []string          `json:"languages"`
	NotAfter            string            `json:"not_after"`
	SourceRecords       uint64            `json:"source_records"`
	EligibleRecords     uint64            `json:"eligible_records"`
	SelectedRecords     uint64            `json:"selected_records"`
	EligibleNotSelected uint64            `json:"eligible_not_selected"`
	Rejections          map[string]uint64 `json:"rejections"`
}

type parquetSampleItem struct {
	score     [sha256.Size]byte
	host      string
	candidate Candidate
	index     int
}

type parquetSampleHeap []*parquetSampleItem

func (values parquetSampleHeap) Len() int { return len(values) }
func (values parquetSampleHeap) Less(left, right int) bool {
	return parquetSampleLess(values[right], values[left])
}
func (values parquetSampleHeap) Swap(left, right int) {
	values[left], values[right] = values[right], values[left]
	values[left].index = left
	values[right].index = right
}
func (values *parquetSampleHeap) Push(value any) {
	item := value.(*parquetSampleItem)
	item.index = len(*values)
	*values = append(*values, item)
}
func (values *parquetSampleHeap) Pop() any {
	old := *values
	last := old[len(old)-1]
	old[len(old)-1] = nil
	last.index = -1
	*values = old[:len(old)-1]
	return last
}

// SelectParquet scans one exact local flat URL Index Parquet part, applies the
// same capture-side eligibility rules as the live evidence collector, and
// emits at most one deterministic normalized-v1 candidate per host. It reads
// and hashes the complete input but retains only MaxRecords candidates.
func SelectParquet(ctx context.Context, input io.ReaderAt, size int64, output io.Writer, options ParquetSelectionOptions) (report Report, returnErr error) {
	if ctx == nil || input == nil || output == nil {
		return Report{}, errors.New("Common Crawl URL Index Parquet selector: context, input, and output are required")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			report = Report{}
			returnErr = fmt.Errorf("Common Crawl URL Index Parquet selector: reader panic: %v", recovered)
		}
	}()
	options, err := ValidateParquetSelectionOptions(options)
	if err != nil {
		return Report{}, err
	}
	prepared, err := prepareParquetInput(ctx, input, size)
	if err != nil {
		return Report{}, err
	}
	selection := &ParquetSelectionReport{
		Algorithm: ParquetSelectionAlgorithm, Profile: options.Profile, MaxRecords: options.MaxRecords, MaxRecordsPerHost: 1,
		Languages: append([]string(nil), options.Languages...), NotAfter: options.NotAfter.Format(time.RFC3339),
		SourceRecords: uint64(prepared.file.NumRows()), Rejections: make(map[string]uint64),
	}
	allowedLanguages := make(map[string]struct{}, len(options.Languages))
	for _, language := range options.Languages {
		allowedLanguages[language] = struct{}{}
	}
	selected := make(parquetSampleHeap, 0, int(options.MaxRecords))
	selectedHosts := make(map[string]*parquetSampleItem, int(options.MaxRecords))
	consider := func(candidate Candidate, err error) error {
		if err != nil {
			selection.Rejections["invalid_normalized_metadata"]++
			return nil
		}
		preflight, reason, err := indexpackselection.PreflightCandidate(selectionCandidate(candidate), options.NotAfter)
		if err != nil {
			selection.Rejections["invalid_capture_metadata"]++
			return nil
		}
		if reason != "" {
			selection.Rejections[reason]++
			return nil
		}
		if preflight.CanonicalURL != candidate.URL {
			selection.Rejections["noncanonical_url"]++
			return nil
		}
		if _, allowed := allowedLanguages[preflight.Language]; !allowed {
			selection.Rejections["language_not_selected"]++
			return nil
		}
		parsed, err := url.Parse(preflight.CanonicalURL)
		if err != nil || parsed.Hostname() == "" {
			selection.Rejections["invalid_capture_metadata"]++
			return nil
		}
		selection.EligibleRecords++
		item := &parquetSampleItem{
			score: sha256.Sum256([]byte(parquetSelectionHashDomain + preflight.CanonicalURL)),
			host:  strings.ToLower(parsed.Hostname()), candidate: candidate, index: -1,
		}
		if existing := selectedHosts[item.host]; existing != nil {
			if parquetSampleLess(item, existing) {
				existing.score = item.score
				existing.candidate = item.candidate
				heap.Fix(&selected, existing.index)
			}
			return nil
		}
		if uint64(selected.Len()) < options.MaxRecords {
			heap.Push(&selected, item)
			selectedHosts[item.host] = item
			return nil
		}
		if parquetSampleLess(item, selected[0]) {
			discarded := heap.Pop(&selected).(*parquetSampleItem)
			delete(selectedHosts, discarded.host)
			heap.Push(&selected, item)
			selectedHosts[item.host] = item
		}
		return nil
	}
	switch prepared.timestampEncoding {
	case parquetTimestampMillis:
		err = visitParquetRows(ctx, prepared.file, func(_ uint64, value urlIndexParquetMillisRow) error {
			candidate, candidateErr := value.candidate()
			return consider(candidate, candidateErr)
		})
	case parquetTimestampINT96:
		err = visitParquetRows(ctx, prepared.file, func(_ uint64, value urlIndexParquetINT96Row) error {
			candidate, candidateErr := value.candidate()
			return consider(candidate, candidateErr)
		})
	default:
		err = errors.New("Common Crawl URL Index Parquet selector: timestamp encoding is unsupported")
	}
	if err != nil {
		return Report{}, err
	}
	if uint64(prepared.file.NumRows()) != selection.SourceRecords {
		return Report{}, errors.New("Common Crawl URL Index Parquet selector: source record count changed")
	}
	if selected.Len() == 0 {
		return Report{}, errors.New("Common Crawl URL Index Parquet selector: no eligible records selected")
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	items := make([]*parquetSampleItem, len(selected))
	copy(items, selected)
	sort.Slice(items, func(left, right int) bool { return parquetSampleLess(items[left], items[right]) })
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	outputCounter := &countingHash{writer: output, hash: sha256.New()}
	report = Report{Version: ReportVersion, Format: FormatURLIndexParquetSelection, Selection: selection}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		report.Records++
		if err := writeParquetCandidate(outputCounter, item.candidate, report.Records); err != nil {
			return Report{}, err
		}
	}
	selection.SelectedRecords = report.Records
	selection.EligibleNotSelected = selection.EligibleRecords - selection.SelectedRecords
	if err := verifyPreparedParquetInput(ctx, input, size, prepared); err != nil {
		return Report{}, err
	}
	report.InputBytes = prepared.inputBytes
	report.OutputBytes = outputCounter.bytes
	report.InputSHA256 = prepared.inputSHA256
	report.OutputSHA256 = hex.EncodeToString(outputCounter.hash.Sum(nil))
	return report, nil
}

// ValidateParquetSelectionOptions validates and defensively copies the
// deterministic selector's complete operator policy.
func ValidateParquetSelectionOptions(options ParquetSelectionOptions) (ParquetSelectionOptions, error) {
	if options.Profile == "" {
		options.Profile = ParquetSelectionProfileCollector
	}
	maximum := uint64(MaxParquetSelectionRecords)
	switch options.Profile {
	case ParquetSelectionProfileCollector:
	case ParquetSelectionProfilePrototype:
		maximum = MaxParquetPrototypeSelectionRecords
	default:
		return ParquetSelectionOptions{}, errors.New("Common Crawl URL Index Parquet selector: profile is invalid")
	}
	if options.MaxRecords == 0 || options.MaxRecords > maximum {
		return ParquetSelectionOptions{}, fmt.Errorf("Common Crawl URL Index Parquet selector: %s max records must be 1..%d", options.Profile, maximum)
	}
	if options.NotAfter.IsZero() || !options.NotAfter.Equal(options.NotAfter.UTC().Truncate(time.Second)) || options.NotAfter.Location() != time.UTC {
		return ParquetSelectionOptions{}, errors.New("Common Crawl URL Index Parquet selector: not-after must be an exact UTC second")
	}
	if len(options.Languages) == 0 || len(options.Languages) > maxParquetSelectionLanguages {
		return ParquetSelectionOptions{}, fmt.Errorf("Common Crawl URL Index Parquet selector: languages must contain 1..%d values", maxParquetSelectionLanguages)
	}
	for index, language := range options.Languages {
		if !validSelectionLanguage(language) || (index > 0 && options.Languages[index-1] >= language) {
			return ParquetSelectionOptions{}, errors.New("Common Crawl URL Index Parquet selector: languages must be canonical, sorted, and unique")
		}
	}
	options.Languages = append([]string(nil), options.Languages...)
	return options, nil
}

func validSelectionLanguage(language string) bool {
	parts := strings.Split(language, "-")
	if len(parts[0]) < 2 || len(parts[0]) > 8 {
		return false
	}
	for index, part := range parts {
		if part == "" || (index > 0 && len(part) > 8) {
			return false
		}
		for _, character := range part {
			if (character < 'a' || character > 'z') && (index == 0 || character < '0' || character > '9') {
				return false
			}
		}
	}
	return true
}

func selectionCandidate(candidate Candidate) indexpackselection.Candidate {
	return indexpackselection.Candidate{
		URLKey: candidate.URLKey, Timestamp: candidate.Timestamp, URL: candidate.URL,
		MIME: candidate.MIME, MIMEDetected: candidate.MIMEDetected, Status: candidate.Status,
		Digest: candidate.Digest, Length: candidate.Length, Offset: candidate.Offset,
		Filename: candidate.Filename, Languages: candidate.Languages, Encoding: candidate.Encoding,
	}
}

func parquetSampleLess(left, right *parquetSampleItem) bool {
	if comparison := bytes.Compare(left.score[:], right.score[:]); comparison != 0 {
		return comparison < 0
	}
	if left.candidate.URL != right.candidate.URL {
		return left.candidate.URL < right.candidate.URL
	}
	if left.candidate.Timestamp != right.candidate.Timestamp {
		return left.candidate.Timestamp > right.candidate.Timestamp
	}
	if left.candidate.Filename != right.candidate.Filename {
		return left.candidate.Filename < right.candidate.Filename
	}
	if left.candidate.Offset != right.candidate.Offset {
		return left.candidate.Offset < right.candidate.Offset
	}
	for _, values := range [][2]string{
		{left.candidate.URLKey, right.candidate.URLKey},
		{left.candidate.MIME, right.candidate.MIME},
		{left.candidate.MIMEDetected, right.candidate.MIMEDetected},
		{left.candidate.Status, right.candidate.Status},
		{left.candidate.Digest, right.candidate.Digest},
		{left.candidate.Length, right.candidate.Length},
		{left.candidate.Languages, right.candidate.Languages},
		{left.candidate.Encoding, right.candidate.Encoding},
	} {
		if values[0] != values[1] {
			return values[0] < values[1]
		}
	}
	return false
}

func visitParquetRows[T any](ctx context.Context, file *parquet.File, visit func(uint64, T) error) error {
	var record uint64
	for rowGroupNumber, rowGroup := range file.RowGroups() {
		if err := ctx.Err(); err != nil {
			return err
		}
		reader := parquet.NewGenericRowGroupReader[T](rowGroup)
		rows := make([]T, parquetBatchRows)
		for {
			count, readErr := reader.Read(rows)
			for index := 0; index < count; index++ {
				if err := ctx.Err(); err != nil {
					_ = reader.Close()
					return err
				}
				record++
				if err := visit(record, rows[index]); err != nil {
					_ = reader.Close()
					return err
				}
				var zero T
				rows[index] = zero
			}
			if readErr != nil {
				if !errors.Is(readErr, io.EOF) {
					_ = reader.Close()
					return fmt.Errorf("Common Crawl URL Index Parquet: read row group %d: %w", rowGroupNumber+1, readErr)
				}
				break
			}
			if count == 0 {
				_ = reader.Close()
				return fmt.Errorf("Common Crawl URL Index Parquet: row group %d returned no rows without EOF", rowGroupNumber+1)
			}
		}
		if err := reader.Close(); err != nil {
			return fmt.Errorf("Common Crawl URL Index Parquet: close row group %d: %w", rowGroupNumber+1, err)
		}
	}
	if record != uint64(file.NumRows()) {
		return errors.New("Common Crawl URL Index Parquet: decoded record count does not match metadata")
	}
	return nil
}
