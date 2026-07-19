package ccindex

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

const (
	FormatURLIndexParquetPartsSelection = "url-index-parquet-parts-selection-v1"
	MaxParquetSelectionInputs           = 1_024
	MaxParquetCollectorSourceRecords    = uint64(indexpackselection.MaxCandidates)
	MaxParquetPrototypeSourceRecords    = 2_000_000_000
	parquetPartsHashDomain              = "fetchmark-common-crawl-parquet-input-set-v1\n"
)

type ParquetSelectionInput struct {
	Reader io.ReaderAt
	Size   int64
}

type preparedParquetSelectionInput struct {
	input    ParquetSelectionInput
	prepared preparedParquetInput
}

type ParquetSelectionPartReport struct {
	SHA256 string `json:"sha256"`
	Bytes  uint64 `json:"bytes"`
	Rows   uint64 `json:"rows"`
}

type ParquetPartsSelectionReport struct {
	Algorithm           string                       `json:"algorithm"`
	Profile             string                       `json:"profile"`
	MaxRecords          uint64                       `json:"max_records"`
	MaxRecordsPerHost   uint64                       `json:"max_records_per_host"`
	MaxSourceRecords    uint64                       `json:"max_source_records"`
	Languages           []string                     `json:"languages"`
	NotAfter            string                       `json:"not_after"`
	InputCount          uint64                       `json:"input_count"`
	Inputs              []ParquetSelectionPartReport `json:"inputs"`
	SourceRecords       uint64                       `json:"source_records"`
	EligibleRecords     uint64                       `json:"eligible_records"`
	SelectedRecords     uint64                       `json:"selected_records"`
	EligibleNotSelected uint64                       `json:"eligible_not_selected"`
	Rejections          map[string]uint64            `json:"rejections"`
}

// SelectParquetParts applies one deterministic global selection to multiple
// exact local Common Crawl flat URL Index Parquet parts. Every part is fully
// hashed, schema-checked, decoded, and rehashed before any output is emitted.
// The caller must stage output until the final report succeeds because every
// input is rehashed once more after output and before caller-side activation.
func SelectParquetParts(ctx context.Context, inputs []ParquetSelectionInput, output io.Writer, options ParquetSelectionOptions) (Report, error) {
	if ctx == nil || output == nil || len(inputs) < 2 || len(inputs) > MaxParquetSelectionInputs {
		return Report{}, fmt.Errorf("Common Crawl URL Index Parquet multi-selector: context, output, and 2..%d inputs are required", MaxParquetSelectionInputs)
	}
	options, err := ValidateParquetSelectionOptions(options)
	if err != nil {
		return Report{}, err
	}
	parts := &ParquetPartsSelectionReport{
		Algorithm: ParquetSelectionAlgorithm, Profile: options.Profile, MaxRecords: options.MaxRecords, MaxRecordsPerHost: 1,
		MaxSourceRecords: maxParquetSelectionSourceRecords(options.Profile),
		Languages:        append([]string(nil), options.Languages...), NotAfter: options.NotAfter.Format(time.RFC3339),
		InputCount: uint64(len(inputs)), Rejections: make(map[string]uint64),
	}
	allowedLanguages := make(map[string]struct{}, len(options.Languages))
	for _, language := range options.Languages {
		allowedLanguages[language] = struct{}{}
	}
	selected := make(parquetSampleHeap, 0, boundedSelectionCapacity(options.MaxRecords))
	selectedHosts := make(map[string]*parquetSampleItem, boundedSelectionCapacity(options.MaxRecords))
	seenInputs := make(map[string]struct{}, len(inputs))
	preparedInputs := make([]preparedParquetSelectionInput, 0, len(inputs))
	for inputIndex, input := range inputs {
		if input.Reader == nil {
			return Report{}, fmt.Errorf("Common Crawl URL Index Parquet multi-selector: input %d is required", inputIndex+1)
		}
		prepared, err := prepareParquetInput(ctx, input.Reader, input.Size)
		if err != nil {
			return Report{}, fmt.Errorf("Common Crawl URL Index Parquet multi-selector: input %d: %w", inputIndex+1, err)
		}
		if _, duplicate := seenInputs[prepared.inputSHA256]; duplicate {
			return Report{}, errors.New("Common Crawl URL Index Parquet multi-selector: duplicate exact input")
		}
		seenInputs[prepared.inputSHA256] = struct{}{}
		rows := uint64(prepared.file.NumRows())
		if parts.SourceRecords > ^uint64(0)-rows {
			return Report{}, errors.New("Common Crawl URL Index Parquet multi-selector: source record count overflow")
		}
		parts.SourceRecords += rows
		if parts.SourceRecords > parts.MaxSourceRecords {
			return Report{}, fmt.Errorf("Common Crawl URL Index Parquet multi-selector: %s source records exceed %d", options.Profile, parts.MaxSourceRecords)
		}
		consider := func(candidate Candidate, candidateErr error) error {
			if candidateErr != nil {
				parts.Rejections["invalid_normalized_metadata"]++
				return nil
			}
			return considerParquetPartCandidate(candidate, parts, allowedLanguages, &selected, selectedHosts, options)
		}
		switch prepared.timestampEncoding {
		case parquetTimestampMillis:
			err = visitParquetRows(ctx, prepared.file, func(_ uint64, row urlIndexParquetMillisRow) error {
				candidate, candidateErr := row.candidate()
				return consider(candidate, candidateErr)
			})
		case parquetTimestampINT96:
			err = visitParquetRows(ctx, prepared.file, func(_ uint64, row urlIndexParquetINT96Row) error {
				candidate, candidateErr := row.candidate()
				return consider(candidate, candidateErr)
			})
		default:
			err = errors.New("timestamp encoding is unsupported")
		}
		if err != nil {
			return Report{}, fmt.Errorf("Common Crawl URL Index Parquet multi-selector: input %d: %w", inputIndex+1, err)
		}
		preparedInputs = append(preparedInputs, preparedParquetSelectionInput{input: input, prepared: prepared})
		parts.Inputs = append(parts.Inputs, ParquetSelectionPartReport{SHA256: prepared.inputSHA256, Bytes: prepared.inputBytes, Rows: rows})
	}
	if err := verifyPreparedParquetSelectionInputs(ctx, preparedInputs); err != nil {
		return Report{}, err
	}
	if selected.Len() == 0 {
		return Report{}, errors.New("Common Crawl URL Index Parquet multi-selector: no eligible records selected")
	}
	sort.Slice(parts.Inputs, func(left, right int) bool { return parts.Inputs[left].SHA256 < parts.Inputs[right].SHA256 })
	items := make([]*parquetSampleItem, len(selected))
	copy(items, selected)
	sort.Slice(items, func(left, right int) bool { return parquetSampleLess(items[left], items[right]) })
	outputCounter := &countingHash{writer: output, hash: sha256.New()}
	report := Report{Version: ReportVersion, Format: FormatURLIndexParquetPartsSelection, Parts: parts}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return Report{}, err
		}
		report.Records++
		if err := writeParquetCandidate(outputCounter, item.candidate, report.Records); err != nil {
			return Report{}, err
		}
	}
	if err := verifyPreparedParquetSelectionInputs(ctx, preparedInputs); err != nil {
		return Report{}, err
	}
	parts.SelectedRecords = report.Records
	parts.EligibleNotSelected = parts.EligibleRecords - parts.SelectedRecords
	inputSetRaw, err := json.Marshal(parts.Inputs)
	if err != nil {
		return Report{}, err
	}
	inputSetDigest := sha256.Sum256(append([]byte(parquetPartsHashDomain), inputSetRaw...))
	report.InputSHA256 = hex.EncodeToString(inputSetDigest[:])
	report.OutputSHA256 = hex.EncodeToString(outputCounter.hash.Sum(nil))
	report.OutputBytes = outputCounter.bytes
	for _, descriptor := range parts.Inputs {
		if report.InputBytes > ^uint64(0)-descriptor.Bytes {
			return Report{}, errors.New("Common Crawl URL Index Parquet multi-selector: input byte count overflow")
		}
		report.InputBytes += descriptor.Bytes
	}
	return report, nil
}

func verifyPreparedParquetSelectionInputs(ctx context.Context, inputs []preparedParquetSelectionInput) error {
	for index, input := range inputs {
		if err := verifyPreparedParquetInput(ctx, input.input.Reader, input.input.Size, input.prepared); err != nil {
			return fmt.Errorf("Common Crawl URL Index Parquet multi-selector: input %d: %w", index+1, err)
		}
	}
	return nil
}

func considerParquetPartCandidate(candidate Candidate, parts *ParquetPartsSelectionReport, allowedLanguages map[string]struct{}, selected *parquetSampleHeap, selectedHosts map[string]*parquetSampleItem, options ParquetSelectionOptions) error {
	preflight, reason, err := indexpackselection.PreflightCandidate(selectionCandidate(candidate), options.NotAfter)
	if err != nil {
		parts.Rejections["invalid_capture_metadata"]++
		return nil
	}
	if reason != "" {
		parts.Rejections[reason]++
		return nil
	}
	if preflight.CanonicalURL != candidate.URL {
		parts.Rejections["noncanonical_url"]++
		return nil
	}
	if _, allowed := allowedLanguages[preflight.Language]; !allowed {
		parts.Rejections["language_not_selected"]++
		return nil
	}
	parsed, err := url.Parse(preflight.CanonicalURL)
	if err != nil || parsed.Hostname() == "" {
		parts.Rejections["invalid_capture_metadata"]++
		return nil
	}
	parts.EligibleRecords++
	item := &parquetSampleItem{
		score: sha256.Sum256([]byte(parquetSelectionHashDomain + preflight.CanonicalURL)),
		host:  strings.ToLower(parsed.Hostname()), candidate: candidate, index: -1,
	}
	if existing := selectedHosts[item.host]; existing != nil {
		if parquetSampleLess(item, existing) {
			existing.score = item.score
			existing.candidate = item.candidate
			heap.Fix(selected, existing.index)
		}
		return nil
	}
	if uint64(selected.Len()) < options.MaxRecords {
		heap.Push(selected, item)
		selectedHosts[item.host] = item
		return nil
	}
	if parquetSampleLess(item, (*selected)[0]) {
		discarded := heap.Pop(selected).(*parquetSampleItem)
		delete(selectedHosts, discarded.host)
		heap.Push(selected, item)
		selectedHosts[item.host] = item
	}
	return nil
}

func boundedSelectionCapacity(maximum uint64) int {
	maxInt := uint64(^uint(0) >> 1)
	if maximum > maxInt {
		return int(maxInt)
	}
	return int(maximum)
}

func maxParquetSelectionSourceRecords(profile string) uint64 {
	if profile == ParquetSelectionProfilePrototype {
		return MaxParquetPrototypeSourceRecords
	}
	return MaxParquetCollectorSourceRecords
}
