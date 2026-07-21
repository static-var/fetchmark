package ccindex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

const (
	MaxParquetInputBytes       int64 = 4 << 30
	MaxParquetFooterBytes      int64 = 16 << 20
	maxParquetColumns                = 256
	maxParquetRowGroups              = 100_000
	maxParquetPageHeaderBytes  int64 = 64 << 10
	maxParquetPageBytes        int64 = 64 << 20
	maxParquetColumnChunkBytes int64 = 512 << 20
	maxParquetPagesPerChunk          = 1_000_000
	maxParquetProjectedChunks        = 100_000
	maxParquetTotalPages             = 5_000_000
	maxParquetThriftBlobBytes        = 1 << 20
	maxParquetThriftCollection       = 100_000
	maxParquetThriftAggregate        = 250_000
	parquetBatchRows                 = 256
	unixEpochJulianDay         int64 = 2_440_588
	nanosecondsPerDay          int64 = 86_400_000_000_000
)

type urlIndexParquetMillisRow struct {
	URLKey           string  `parquet:"url_surtkey"`
	URL              string  `parquet:"url"`
	FetchTime        int64   `parquet:"fetch_time,timestamp(millisecond)"`
	FetchStatus      int16   `parquet:"fetch_status"`
	ContentDigest    *string `parquet:"content_digest"`
	ContentMIME      *string `parquet:"content_mime_type"`
	ContentDetected  *string `parquet:"content_mime_detected"`
	ContentCharset   *string `parquet:"content_charset"`
	ContentLanguages *string `parquet:"content_languages"`
	WARCFilename     string  `parquet:"warc_filename"`
	WARCOffset       int32   `parquet:"warc_record_offset"`
	WARCLength       int32   `parquet:"warc_record_length"`
}

type urlIndexParquetINT96Row struct {
	URLKey           string           `parquet:"url_surtkey"`
	URL              string           `parquet:"url"`
	FetchTime        deprecated.Int96 `parquet:"fetch_time"`
	FetchStatus      int16            `parquet:"fetch_status"`
	ContentDigest    *string          `parquet:"content_digest"`
	ContentMIME      *string          `parquet:"content_mime_type"`
	ContentDetected  *string          `parquet:"content_mime_detected"`
	ContentCharset   *string          `parquet:"content_charset"`
	ContentLanguages *string          `parquet:"content_languages"`
	WARCFilename     string           `parquet:"warc_filename"`
	WARCOffset       int32            `parquet:"warc_record_offset"`
	WARCLength       int32            `parquet:"warc_record_length"`
}

type parquetSchemaField struct {
	name         string
	typeNames    []string
	definition   int
	allowMissing bool
}

var urlIndexParquetSchema = []parquetSchemaField{
	{name: "url_surtkey", typeNames: []string{parquet.String().Type().String()}},
	{name: "url", typeNames: []string{parquet.String().Type().String()}},
	{name: "fetch_time", typeNames: []string{parquet.Timestamp(parquet.Millisecond).Type().String(), parquet.Int96Type.String()}},
	{name: "fetch_status", typeNames: []string{parquet.Int(16).Type().String()}},
	{name: "content_digest", typeNames: []string{parquet.String().Type().String()}, definition: 1},
	{name: "content_mime_type", typeNames: []string{parquet.String().Type().String()}, definition: 1},
	{name: "content_mime_detected", typeNames: []string{parquet.String().Type().String()}, definition: 1, allowMissing: true},
	{name: "content_charset", typeNames: []string{parquet.String().Type().String()}, definition: 1, allowMissing: true},
	{name: "content_languages", typeNames: []string{parquet.String().Type().String()}, definition: 1, allowMissing: true},
	{name: "warc_filename", typeNames: []string{parquet.String().Type().String()}},
	{name: "warc_record_offset", typeNames: []string{parquet.Int(32).Type().String(), parquet.Int32Type.String()}},
	{name: "warc_record_length", typeNames: []string{parquet.Int(32).Type().String(), parquet.Int32Type.String()}},
}

type parquetTimestampEncoding uint8

const (
	parquetTimestampMillis parquetTimestampEncoding = iota + 1
	parquetTimestampINT96
)

type parquetCandidateRow interface {
	candidate() (Candidate, error)
}

type parquetFileSnapshot struct {
	size    int64
	mode    os.FileMode
	modTime time.Time
}

type preparedParquetInput struct {
	file              *parquet.File
	timestampEncoding parquetTimestampEncoding
	snapshot          *parquetFileSnapshot
	inputBytes        uint64
	inputSHA256       string
}

type parquetChunkPreflight struct {
	chunk    *format.ColumnChunk
	start    int64
	end      int64
	rowCount int64
	rowGroup int
	column   int
}

type thriftReaderDelegate interface {
	thrift.Reader
}

type boundedThriftReader struct {
	thriftReaderDelegate
	maxBlobBytes       int
	maxCollection      int32
	maxAggregate       int64
	aggregateEntries   int64
	directPayloadBytes int
}

// NormalizeParquet streams one exact local Common Crawl flat URL Index
// Parquet file into normalized-v1 JSONL. It performs no network access and
// accepts extra source columns, but rejects missing or changed core columns.
// The caller owns input and output and must stage the output until this report
// succeeds.
func NormalizeParquet(ctx context.Context, input io.ReaderAt, size int64, output io.Writer) (report Report, returnErr error) {
	if ctx == nil || input == nil || output == nil {
		return Report{}, errors.New("Common Crawl URL Index Parquet normalizer: context, input, and output are required")
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			report = Report{}
			returnErr = fmt.Errorf("Common Crawl URL Index Parquet normalizer: reader panic: %v", recovered)
		}
	}()
	prepared, err := prepareParquetInput(ctx, input, size)
	if err != nil {
		return Report{}, err
	}
	outputCounter := &countingHash{writer: output, hash: sha256.New()}
	report = Report{Version: ReportVersion, Format: FormatURLIndexParquet}
	switch prepared.timestampEncoding {
	case parquetTimestampMillis:
		err = streamParquetRows[urlIndexParquetMillisRow](ctx, prepared.file, outputCounter, &report)
	case parquetTimestampINT96:
		err = streamParquetRows[urlIndexParquetINT96Row](ctx, prepared.file, outputCounter, &report)
	default:
		err = errors.New("Common Crawl URL Index Parquet normalizer: timestamp encoding is unsupported")
	}
	if err != nil {
		return Report{}, err
	}
	if report.Records == 0 || report.Records != uint64(prepared.file.NumRows()) {
		return Report{}, errors.New("Common Crawl URL Index Parquet normalizer: decoded record count does not match metadata")
	}
	if err := verifyPreparedParquetInput(ctx, input, size, prepared); err != nil {
		return Report{}, err
	}
	report.InputBytes = prepared.inputBytes
	report.OutputBytes = outputCounter.bytes
	report.InputSHA256 = prepared.inputSHA256
	report.OutputSHA256 = hex.EncodeToString(outputCounter.hash.Sum(nil))
	return report, nil
}

func streamParquetRows[T parquetCandidateRow](ctx context.Context, file *parquet.File, output *countingHash, report *Report) error {
	return visitParquetRows(ctx, file, func(_ uint64, row T) error {
		report.Records++
		candidate, err := row.candidate()
		if err != nil {
			return fmt.Errorf("Common Crawl URL Index Parquet normalizer: row %d: %w", report.Records, err)
		}
		return writeParquetCandidate(output, candidate, report.Records)
	})
}

func prepareParquetInput(ctx context.Context, input io.ReaderAt, size int64) (preparedParquetInput, error) {
	if err := ctx.Err(); err != nil {
		return preparedParquetInput{}, err
	}
	if size < 12 || size > MaxParquetInputBytes {
		return preparedParquetInput{}, fmt.Errorf("Common Crawl URL Index Parquet: input size must be 12..%d bytes", MaxParquetInputBytes)
	}
	before, err := snapshotParquetInput(input)
	if err != nil {
		return preparedParquetInput{}, err
	}
	if before != nil && before.size != size {
		return preparedParquetInput{}, errors.New("Common Crawl URL Index Parquet: declared size does not match input")
	}
	if err := validateParquetEnvelope(input, size); err != nil {
		return preparedParquetInput{}, err
	}
	if err := validateParquetFooterThrift(ctx, input, size); err != nil {
		return preparedParquetInput{}, err
	}
	inputBytes, inputDigest, err := hashParquetInput(ctx, input, size)
	if err != nil {
		return preparedParquetInput{}, err
	}
	file, err := parquet.OpenFile(
		input, size, parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true),
		parquet.FileReadMode(parquet.ReadModeSync), parquet.ReadBufferSize(64<<10),
	)
	if err != nil {
		return preparedParquetInput{}, fmt.Errorf("Common Crawl URL Index Parquet: open: %w", err)
	}
	encoding, err := validateURLIndexParquetFile(ctx, input, size, file)
	if err != nil {
		return preparedParquetInput{}, err
	}
	return preparedParquetInput{file: file, timestampEncoding: encoding, snapshot: before, inputBytes: inputBytes, inputSHA256: inputDigest}, nil
}

func verifyPreparedParquetInput(ctx context.Context, input io.ReaderAt, size int64, prepared preparedParquetInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	secondBytes, secondDigest, err := hashParquetInput(ctx, input, size)
	if err != nil {
		return err
	}
	if prepared.inputSHA256 != secondDigest || prepared.inputBytes != secondBytes {
		return errors.New("Common Crawl URL Index Parquet: input changed while processing")
	}
	return verifyParquetSnapshot(input, prepared.snapshot)
}

func validateParquetEnvelope(input io.ReaderAt, size int64) error {
	var header [4]byte
	if _, err := input.ReadAt(header[:], 0); err != nil {
		return fmt.Errorf("Common Crawl URL Index Parquet normalizer: read header: %w", err)
	}
	if string(header[:]) != "PAR1" {
		return errors.New("Common Crawl URL Index Parquet normalizer: plaintext PAR1 header is required")
	}
	var footer [8]byte
	if _, err := input.ReadAt(footer[:], size-8); err != nil {
		return fmt.Errorf("Common Crawl URL Index Parquet normalizer: read footer: %w", err)
	}
	if string(footer[4:]) != "PAR1" {
		return errors.New("Common Crawl URL Index Parquet normalizer: plaintext PAR1 footer is required")
	}
	footerBytes := int64(binary.LittleEndian.Uint32(footer[:4]))
	if footerBytes <= 0 || footerBytes > MaxParquetFooterBytes || footerBytes > size-12 {
		return fmt.Errorf("Common Crawl URL Index Parquet normalizer: footer size must be 1..%d bytes within input", MaxParquetFooterBytes)
	}
	return nil
}

func validateParquetFooterThrift(ctx context.Context, input io.ReaderAt, size int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var trailer [8]byte
	if _, err := input.ReadAt(trailer[:], size-8); err != nil {
		return fmt.Errorf("Common Crawl URL Index Parquet normalizer: reread footer: %w", err)
	}
	footerBytes := int64(binary.LittleEndian.Uint32(trailer[:4]))
	if footerBytes <= 0 || footerBytes > MaxParquetFooterBytes || footerBytes > size-12 {
		return errors.New("Common Crawl URL Index Parquet normalizer: footer range is invalid")
	}
	raw := make([]byte, footerBytes)
	reader := contextReader{ctx: ctx, reader: io.NewSectionReader(input, size-8-footerBytes, footerBytes)}
	if _, err := io.ReadFull(reader, raw); err != nil {
		return fmt.Errorf("Common Crawl URL Index Parquet normalizer: read bounded footer: %w", err)
	}
	protocol := new(thrift.CompactProtocol)
	consumed, err := validateCompactThriftStructure(ctx, bytes.NewReader(raw), len(raw))
	if err != nil {
		return fmt.Errorf("Common Crawl URL Index Parquet normalizer: bounded footer structure: %w", err)
	}
	if consumed != len(raw) {
		return fmt.Errorf("Common Crawl URL Index Parquet normalizer: footer structural scan consumed %d of %d bytes", consumed, len(raw))
	}
	bounded := newBoundedThriftReader(protocol.NewReader(bytes.NewReader(raw)), len(raw))
	var metadata format.FileMetaData
	decoder := thrift.NewDecoder(bounded)
	decoder.SetStrict(true)
	if err := decoder.Decode(&metadata); err != nil {
		return fmt.Errorf("Common Crawl URL Index Parquet normalizer: bounded footer decode: %w", err)
	}
	return nil
}

func newBoundedThriftReader(reader thrift.Reader, payloadBudget int) *boundedThriftReader {
	blobLimit := maxParquetThriftBlobBytes
	if payloadBudget < blobLimit {
		blobLimit = payloadBudget
	}
	collectionLimit := int32(maxParquetThriftCollection)
	if scaled := payloadBudget / 4; scaled < int(collectionLimit) {
		collectionLimit = int32(scaled)
	}
	aggregateLimit := int64(maxParquetThriftAggregate)
	if scaled := int64(payloadBudget / 2); scaled < aggregateLimit {
		aggregateLimit = scaled
	}
	return &boundedThriftReader{
		thriftReaderDelegate: reader, maxBlobBytes: blobLimit,
		maxCollection: collectionLimit, maxAggregate: aggregateLimit,
	}
}

func (reader *boundedThriftReader) BytesRead() int {
	return reader.thriftReaderDelegate.BytesRead() + reader.directPayloadBytes
}

func (reader *boundedThriftReader) ReadBytes() ([]byte, error) {
	length, err := reader.thriftReaderDelegate.ReadLength()
	if err != nil {
		return nil, err
	}
	if length < 0 || length > reader.maxBlobBytes || reader.directPayloadBytes > reader.maxBlobBytes-length {
		return nil, fmt.Errorf("Parquet Thrift byte value exceeds bounded payload limit %d", reader.maxBlobBytes)
	}
	raw := make([]byte, length)
	if _, err := io.ReadFull(reader.thriftReaderDelegate.Reader(), raw); err != nil {
		return nil, err
	}
	reader.directPayloadBytes += length
	return raw, nil
}

func (reader *boundedThriftReader) ReadString() (string, error) {
	raw, err := reader.ReadBytes()
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (reader *boundedThriftReader) ReadLength() (int, error) {
	length, err := reader.thriftReaderDelegate.ReadLength()
	if err != nil {
		return 0, err
	}
	if length < 0 || length > reader.maxBlobBytes {
		return 0, fmt.Errorf("Parquet Thrift length exceeds bounded payload limit %d", reader.maxBlobBytes)
	}
	return length, nil
}

func (reader *boundedThriftReader) ReadMessage() (thrift.Message, error) {
	return thrift.Message{}, errors.New("Parquet Thrift messages are unsupported")
}

func (reader *boundedThriftReader) ReadList() (thrift.List, error) {
	list, err := reader.thriftReaderDelegate.ReadList()
	if err != nil {
		return thrift.List{}, err
	}
	if err := reader.accountCollection(list.Size); err != nil {
		return thrift.List{}, err
	}
	return list, nil
}

func (reader *boundedThriftReader) ReadSet() (thrift.Set, error) {
	set, err := reader.thriftReaderDelegate.ReadSet()
	if err != nil {
		return thrift.Set{}, err
	}
	if err := reader.accountCollection(set.Size); err != nil {
		return thrift.Set{}, err
	}
	return set, nil
}

func (reader *boundedThriftReader) ReadMap() (thrift.Map, error) {
	value, err := reader.thriftReaderDelegate.ReadMap()
	if err != nil {
		return thrift.Map{}, err
	}
	if err := reader.accountCollection(value.Size); err != nil {
		return thrift.Map{}, err
	}
	return value, nil
}

func (reader *boundedThriftReader) accountCollection(size int32) error {
	if size < 0 || size > reader.maxCollection || int64(size) > reader.maxAggregate-reader.aggregateEntries {
		return fmt.Errorf("Parquet Thrift collection exceeds bounded entry limits (%d per collection, %d aggregate)", reader.maxCollection, reader.maxAggregate)
	}
	reader.aggregateEntries += int64(size)
	return nil
}

func validateURLIndexParquetFile(ctx context.Context, input io.ReaderAt, size int64, file *parquet.File) (parquetTimestampEncoding, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if file == nil || file.Schema() == nil {
		return 0, errors.New("Common Crawl URL Index Parquet normalizer: schema is missing")
	}
	columns := file.Schema().Columns()
	if len(columns) == 0 || len(columns) > maxParquetColumns {
		return 0, fmt.Errorf("Common Crawl URL Index Parquet normalizer: leaf column count must be 1..%d", maxParquetColumns)
	}
	rowGroups := file.RowGroups()
	if len(rowGroups) == 0 || len(rowGroups) > maxParquetRowGroups {
		return 0, fmt.Errorf("Common Crawl URL Index Parquet normalizer: row group count must be 1..%d", maxParquetRowGroups)
	}
	if file.NumRows() <= 0 || uint64(file.NumRows()) > MaxRecords {
		return 0, fmt.Errorf("Common Crawl URL Index Parquet normalizer: record count must be 1..%d", MaxRecords)
	}
	dataEnd, err := parquetDataEnd(input, size)
	if err != nil {
		return 0, err
	}
	indexes := make(map[string]int, len(urlIndexParquetSchema))
	var timestampEncoding parquetTimestampEncoding
	for _, expected := range urlIndexParquetSchema {
		leaf, ok := file.Schema().Lookup(expected.name)
		if !ok {
			if expected.allowMissing {
				continue
			}
			return 0, fmt.Errorf("Common Crawl URL Index Parquet normalizer: required column %q is missing", expected.name)
		}
		typeName := leaf.Node.Type().String()
		if leaf.MaxRepetitionLevel != 0 || leaf.MaxDefinitionLevel != expected.definition || !containsString(expected.typeNames, typeName) {
			return 0, fmt.Errorf(
				"Common Crawl URL Index Parquet normalizer: column %q has incompatible type or repetition: type=%q definition=%d repetition=%d",
				expected.name, typeName, leaf.MaxDefinitionLevel, leaf.MaxRepetitionLevel,
			)
		}
		if expected.name == "fetch_time" {
			switch typeName {
			case parquet.Timestamp(parquet.Millisecond).Type().String():
				timestampEncoding = parquetTimestampMillis
			case parquet.Int96Type.String():
				timestampEncoding = parquetTimestampINT96
			}
		}
		indexes[expected.name] = leaf.ColumnIndex
	}
	if timestampEncoding == 0 {
		return 0, errors.New("Common Crawl URL Index Parquet normalizer: fetch_time encoding is unsupported")
	}
	metadata := file.Metadata()
	if metadata == nil || len(metadata.RowGroups) != len(rowGroups) {
		return 0, errors.New("Common Crawl URL Index Parquet normalizer: row group metadata is inconsistent")
	}
	var rowCount uint64
	chunks := make([]parquetChunkPreflight, 0, min(len(metadata.RowGroups)*len(indexes), maxParquetProjectedChunks))
	for rowGroupIndex := range metadata.RowGroups {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		group := &metadata.RowGroups[rowGroupIndex]
		if group.NumRows <= 0 || uint64(group.NumRows) > MaxRecords-rowCount || len(group.Columns) != len(columns) {
			return 0, fmt.Errorf("Common Crawl URL Index Parquet normalizer: row group %d metadata is invalid", rowGroupIndex+1)
		}
		rowCount += uint64(group.NumRows)
		for _, columnIndex := range indexes {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			if len(chunks) >= maxParquetProjectedChunks {
				return 0, fmt.Errorf("Common Crawl URL Index Parquet normalizer: projected column chunk count exceeds %d", maxParquetProjectedChunks)
			}
			chunk := &group.Columns[columnIndex]
			preflight, err := parquetColumnChunkPreflight(dataEnd, chunk, group.NumRows)
			if err != nil {
				return 0, fmt.Errorf("Common Crawl URL Index Parquet normalizer: row group %d column %d: %w", rowGroupIndex+1, columnIndex+1, err)
			}
			preflight.rowGroup = rowGroupIndex + 1
			preflight.column = columnIndex + 1
			chunks = append(chunks, preflight)
		}
	}
	if rowCount != uint64(file.NumRows()) {
		return 0, errors.New("Common Crawl URL Index Parquet normalizer: row count sum does not match file metadata")
	}
	if err := validateParquetChunkRanges(ctx, chunks, dataEnd); err != nil {
		return 0, err
	}
	pageCount := 0
	for _, chunk := range chunks {
		var err error
		pageCount, err = validateParquetColumnChunk(ctx, input, chunk, pageCount)
		if err != nil {
			return 0, fmt.Errorf("Common Crawl URL Index Parquet normalizer: row group %d column %d: %w", chunk.rowGroup, chunk.column, err)
		}
	}
	return timestampEncoding, nil
}

func parquetDataEnd(input io.ReaderAt, size int64) (int64, error) {
	var footer [8]byte
	if _, err := input.ReadAt(footer[:], size-8); err != nil {
		return 0, fmt.Errorf("Common Crawl URL Index Parquet normalizer: reread footer: %w", err)
	}
	footerBytes := int64(binary.LittleEndian.Uint32(footer[:4]))
	dataEnd := size - 8 - footerBytes
	if dataEnd < 4 || dataEnd >= size-8 {
		return 0, errors.New("Common Crawl URL Index Parquet normalizer: footer range is invalid")
	}
	return dataEnd, nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func parquetColumnChunkPreflight(dataEnd int64, chunk *format.ColumnChunk, rowCount int64) (parquetChunkPreflight, error) {
	if chunk == nil || chunk.FilePath != "" || len(chunk.EncryptedColumnMetadata) != 0 || chunk.CryptoMetadata.EncryptionWithFooterKey != nil || chunk.CryptoMetadata.EncryptionWithColumnKey != nil {
		return parquetChunkPreflight{}, errors.New("external or encrypted column chunks are unsupported")
	}
	metadata := &chunk.MetaData
	if metadata.NumValues != rowCount || metadata.TotalCompressedSize <= 0 || metadata.TotalCompressedSize > maxParquetColumnChunkBytes || metadata.TotalUncompressedSize <= 0 || metadata.TotalUncompressedSize > maxParquetColumnChunkBytes {
		return parquetChunkPreflight{}, errors.New("column chunk values or sizes are outside bounds")
	}
	start := metadata.DataPageOffset
	if metadata.DictionaryPageOffset > 0 && metadata.DictionaryPageOffset < start {
		start = metadata.DictionaryPageOffset
	}
	if start < 4 || start > dataEnd || metadata.TotalCompressedSize > dataEnd-start {
		return parquetChunkPreflight{}, errors.New("column chunk range is outside input")
	}
	return parquetChunkPreflight{chunk: chunk, start: start, end: start + metadata.TotalCompressedSize, rowCount: rowCount}, nil
}

func validateParquetChunkRanges(ctx context.Context, chunks []parquetChunkPreflight, dataEnd int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sort.Slice(chunks, func(left, right int) bool {
		if chunks[left].start == chunks[right].start {
			return chunks[left].end < chunks[right].end
		}
		return chunks[left].start < chunks[right].start
	})
	var previousEnd, total int64
	for index, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if chunk.start < 4 || chunk.end <= chunk.start || chunk.end > dataEnd {
			return errors.New("Common Crawl URL Index Parquet normalizer: projected column chunk range is outside input")
		}
		if index > 0 && chunk.start < previousEnd {
			return errors.New("Common Crawl URL Index Parquet normalizer: projected column chunk ranges overlap")
		}
		width := chunk.end - chunk.start
		if total > dataEnd-4-width {
			return errors.New("Common Crawl URL Index Parquet normalizer: projected column chunk bytes exceed data section")
		}
		total += width
		previousEnd = chunk.end
	}
	return nil
}

func validateParquetColumnChunk(ctx context.Context, input io.ReaderAt, preflight parquetChunkPreflight, totalPages int) (int, error) {
	if err := ctx.Err(); err != nil {
		return totalPages, err
	}
	metadata := &preflight.chunk.MetaData
	var consumed, values int64
	pageCount := 0
	dictionarySeen := false
	dataSeen := false
	for consumed < metadata.TotalCompressedSize {
		if err := ctx.Err(); err != nil {
			return totalPages, err
		}
		pageCount++
		if pageCount > maxParquetPagesPerChunk {
			return totalPages, errors.New("column chunk page count exceeds limit")
		}
		totalPages++
		if totalPages > maxParquetTotalPages {
			return totalPages, errors.New("projected Parquet page count exceeds limit")
		}
		header, headerBytes, err := decodeBoundedParquetPageHeader(ctx, input, preflight.start+consumed, metadata.TotalCompressedSize-consumed)
		if err != nil {
			return totalPages, err
		}
		if headerBytes <= 0 || header.CompressedPageSize <= 0 || header.UncompressedPageSize <= 0 || int64(header.CompressedPageSize) > maxParquetPageBytes || int64(header.UncompressedPageSize) > maxParquetPageBytes {
			return totalPages, errors.New("page header or body size is outside bounds")
		}
		pageBytes := headerBytes + int64(header.CompressedPageSize)
		if pageBytes > metadata.TotalCompressedSize-consumed {
			return totalPages, errors.New("page exceeds column chunk range")
		}
		switch header.Type {
		case format.DataPage:
			if !header.DataPageHeader.Valid || header.DataPageHeader.V.NumValues < 0 || int64(header.DataPageHeader.V.NumValues) > metadata.NumValues-values {
				return totalPages, errors.New("data page header is invalid")
			}
			values += int64(header.DataPageHeader.V.NumValues)
			dataSeen = true
		case format.DataPageV2:
			if !header.DataPageHeaderV2.Valid || header.DataPageHeaderV2.V.NumValues < 0 || int64(header.DataPageHeaderV2.V.NumValues) > metadata.NumValues-values {
				return totalPages, errors.New("data page v2 header is invalid")
			}
			values += int64(header.DataPageHeaderV2.V.NumValues)
			dataSeen = true
		case format.DictionaryPage:
			dictionaryValues := int64(header.DictionaryPageHeader.V.NumValues)
			if dictionarySeen || dataSeen || !header.DictionaryPageHeader.Valid || dictionaryValues <= 0 || dictionaryValues > metadata.NumValues || dictionaryValues > int64(header.UncompressedPageSize)/4 {
				return totalPages, errors.New("dictionary page cardinality is outside bounds")
			}
			dictionarySeen = true
		case format.IndexPage:
			if !header.IndexPageHeader.Valid {
				return totalPages, errors.New("index page header is invalid")
			}
		default:
			return totalPages, errors.New("page type is unsupported")
		}
		consumed += pageBytes
	}
	if consumed != metadata.TotalCompressedSize || values != metadata.NumValues {
		return totalPages, errors.New("column chunk page totals do not match metadata")
	}
	return totalPages, nil
}

func decodeBoundedParquetPageHeader(ctx context.Context, input io.ReaderAt, offset, remaining int64) (format.PageHeader, int64, error) {
	if err := ctx.Err(); err != nil {
		return format.PageHeader{}, 0, err
	}
	limit := min(remaining, maxParquetPageHeaderBytes)
	if limit <= 0 {
		return format.PageHeader{}, 0, errors.New("page header range is empty")
	}
	protocol := new(thrift.CompactProtocol)
	scanReader := contextReader{ctx: ctx, reader: io.NewSectionReader(input, offset, limit)}
	headerBytes, err := validateCompactThriftStructure(ctx, scanReader, int(limit))
	if err != nil {
		return format.PageHeader{}, 0, fmt.Errorf("scan page header: %w", err)
	}
	if headerBytes <= 0 || int64(headerBytes) > limit {
		return format.PageHeader{}, 0, errors.New("page header size is outside bounds")
	}
	raw := make([]byte, headerBytes)
	if _, err := io.ReadFull(contextReader{ctx: ctx, reader: io.NewSectionReader(input, offset, int64(headerBytes))}, raw); err != nil {
		return format.PageHeader{}, 0, fmt.Errorf("reread page header: %w", err)
	}
	bounded := newBoundedThriftReader(protocol.NewReader(bytes.NewReader(raw)), len(raw))
	var header format.PageHeader
	decoder := thrift.NewDecoder(bounded)
	decoder.SetStrict(true)
	if err := decoder.Decode(&header); err != nil {
		return format.PageHeader{}, 0, fmt.Errorf("decode page header: %w", err)
	}
	return header, int64(headerBytes), nil
}

func (row urlIndexParquetMillisRow) candidate() (Candidate, error) {
	timestamp, err := decodeMillisTimestamp(row.FetchTime)
	if err != nil {
		return Candidate{}, err
	}
	return parquetCandidate(
		timestamp, row.URLKey, row.URL, row.FetchStatus, row.ContentDigest, row.ContentMIME,
		row.ContentDetected, row.ContentCharset, row.ContentLanguages, row.WARCFilename, row.WARCOffset, row.WARCLength,
	)
}

func (row urlIndexParquetINT96Row) candidate() (Candidate, error) {
	timestamp, err := decodeINT96Timestamp(row.FetchTime)
	if err != nil {
		return Candidate{}, err
	}
	return parquetCandidate(
		timestamp, row.URLKey, row.URL, row.FetchStatus, row.ContentDigest, row.ContentMIME,
		row.ContentDetected, row.ContentCharset, row.ContentLanguages, row.WARCFilename, row.WARCOffset, row.WARCLength,
	)
}

func parquetCandidate(
	timestamp, urlKey, rawURL string,
	status int16,
	digest, mime, detected, charset, languages *string,
	filename string,
	offset, length int32,
) (Candidate, error) {
	if status < 100 || status > 999 {
		return Candidate{}, errors.New("fetch_status must contain a three-digit HTTP status")
	}
	if offset < 0 || length < 0 {
		return Candidate{}, errors.New("WARC offset and length must be non-negative")
	}
	candidate := Candidate{
		Version: NormalizedVersion, URLKey: urlKey, Timestamp: timestamp,
		URL: rawURL, MIME: pointerValue(mime), MIMEDetected: pointerValue(detected),
		Status: strconv.Itoa(int(status)), Digest: pointerValue(digest),
		Length: strconv.FormatInt(int64(length), 10), Offset: strconv.FormatInt(int64(offset), 10),
		Filename: filename, Languages: pointerValue(languages), Encoding: pointerValue(charset),
	}
	var err error
	candidate.Digest, err = normalizeDigest(candidate.Digest)
	if err != nil {
		return Candidate{}, err
	}
	if err := candidate.validate(); err != nil {
		return Candidate{}, err
	}
	return candidate, nil
}

func decodeMillisTimestamp(milliseconds int64) (string, error) {
	if milliseconds%1_000 != 0 {
		return "", errors.New("fetch_time TIMESTAMP_MILLIS must be a whole UTC second")
	}
	valueTime := time.Unix(milliseconds/1_000, 0).UTC()
	if valueTime.Year() < 1 || valueTime.Year() > 9999 {
		return "", errors.New("fetch_time TIMESTAMP_MILLIS year is outside normalized timestamp range")
	}
	return valueTime.Format("20060102150405"), nil
}

func decodeINT96Timestamp(value deprecated.Int96) (string, error) {
	nanoseconds := uint64(value[1])<<32 | uint64(value[0])
	if nanoseconds >= uint64(nanosecondsPerDay) || nanoseconds%uint64(time.Second) != 0 {
		return "", errors.New("fetch_time INT96 must be a whole UTC second within one day")
	}
	days := int64(value[2]) - unixEpochJulianDay
	seconds := days*86_400 + int64(nanoseconds/uint64(time.Second))
	valueTime := time.Unix(seconds, 0).UTC()
	if valueTime.Year() < 1 || valueTime.Year() > 9999 {
		return "", errors.New("fetch_time INT96 year is outside normalized timestamp range")
	}
	return valueTime.Format("20060102150405"), nil
}

func writeParquetCandidate(output *countingHash, candidate Candidate, record uint64) error {
	raw, err := json.Marshal(candidate)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if len(raw) > MaxNormalizedLineBytes {
		return fmt.Errorf("Common Crawl URL Index Parquet normalizer: row %d exceeds normalized size limit", record)
	}
	written, err := output.Write(raw)
	if err != nil {
		return fmt.Errorf("Common Crawl URL Index Parquet normalizer: write row %d: %w", record, err)
	}
	if written != len(raw) {
		return io.ErrShortWrite
	}
	return nil
}

func hashParquetInput(ctx context.Context, input io.ReaderAt, size int64) (uint64, string, error) {
	digest := sha256.New()
	counter := &countingHash{writer: io.Discard, hash: digest}
	buffer := make([]byte, 64<<10)
	if _, err := io.CopyBuffer(counter, contextReader{ctx: ctx, reader: io.NewSectionReader(input, 0, size)}, buffer); err != nil {
		return 0, "", fmt.Errorf("Common Crawl URL Index Parquet normalizer: hash input: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return 0, "", err
	}
	if counter.bytes != uint64(size) {
		return 0, "", errors.New("Common Crawl URL Index Parquet normalizer: input ended before declared size")
	}
	return counter.bytes, hex.EncodeToString(digest.Sum(nil)), nil
}

func snapshotParquetInput(input io.ReaderAt) (*parquetFileSnapshot, error) {
	statter, ok := input.(interface{ Stat() (os.FileInfo, error) })
	if !ok {
		return nil, nil
	}
	info, err := statter.Stat()
	if err != nil {
		return nil, fmt.Errorf("Common Crawl URL Index Parquet normalizer: stat input: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("Common Crawl URL Index Parquet normalizer: input must be a regular file")
	}
	return &parquetFileSnapshot{size: info.Size(), mode: info.Mode(), modTime: info.ModTime()}, nil
}

func verifyParquetSnapshot(input io.ReaderAt, before *parquetFileSnapshot) error {
	if before == nil {
		return nil
	}
	after, err := snapshotParquetInput(input)
	if err != nil {
		return err
	}
	if after == nil || after.size != before.size || after.mode != before.mode || !after.modTime.Equal(before.modTime) {
		return errors.New("Common Crawl URL Index Parquet normalizer: input metadata changed while normalizing")
	}
	return nil
}

func pointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
