package ccindex

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
	"github.com/staticvar/fetchmark/internal/core/indexpackselection"
)

const apiFixture = `{"urlkey":"org,commoncrawl)/get-started","timestamp":"20251014220259","url":"https://www.commoncrawl.org/get-started","mime":"text/html","mime-detected":"text/html","status":"200","digest":"D4IRZZ6NS7QW37BB2ODQPCUGG7ISRFGV","length":"12675","offset":"686242195","filename":"crawl-data/CC-MAIN-2025-43/segments/fixture/warc/CC-MAIN-20251014214924-00000.warc.gz","languages":"eng","encoding":"UTF-8"}`

func TestNormalizeAPIAndCDXJProduceIdenticalDeterministicArtifact(t *testing.T) {
	apiInput := []byte(apiFixture + "\n")
	cdxjInput := []byte(`org,commoncrawl)/get-started 20251014220259 {"url":"https://www.commoncrawl.org/get-started","mime":"text/html","mime-detected":"text/html","status":"200","digest":"D4IRZZ6NS7QW37BB2ODQPCUGG7ISRFGV","length":"12675","offset":"686242195","filename":"crawl-data/CC-MAIN-2025-43/segments/fixture/warc/CC-MAIN-20251014214924-00000.warc.gz","languages":"eng","encoding":"UTF-8"}` + "\n")
	var apiOutput, cdxjOutput bytes.Buffer
	apiReport, err := Normalize(context.Background(), bytes.NewReader(apiInput), &apiOutput, Options{Format: FormatCDXAPIJSON})
	if err != nil {
		t.Fatal(err)
	}
	cdxjReport, err := Normalize(context.Background(), bytes.NewReader(cdxjInput), &cdxjOutput, Options{Format: FormatCDXJHeader})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(apiOutput.Bytes(), cdxjOutput.Bytes()) || !strings.Contains(apiOutput.String(), `"version":1`) {
		t.Fatalf("normalized outputs differ:\napi=%s\ncdxj=%s", apiOutput.Bytes(), cdxjOutput.Bytes())
	}
	outputDigest := sha256.Sum256(apiOutput.Bytes())
	if apiReport.Records != 1 || apiReport.InputBytes != uint64(len(apiInput)) || apiReport.OutputBytes != uint64(apiOutput.Len()) || apiReport.OutputSHA256 != hex.EncodeToString(outputDigest[:]) {
		t.Fatalf("API report = %#v", apiReport)
	}
	if cdxjReport.Records != 1 || cdxjReport.OutputSHA256 != apiReport.OutputSHA256 || cdxjReport.InputSHA256 == apiReport.InputSHA256 {
		t.Fatalf("CDXJ report = %#v", cdxjReport)
	}
	var buildCandidate indexpackselection.Candidate
	if err := indexpack.DecodeStrictJSON(bytes.TrimSpace(apiOutput.Bytes()), &buildCandidate); err == nil {
		t.Fatal("normalized metadata bypassed the required live-admission enrichment boundary")
	}
}

func TestNormalizeURLIndexParquetProducesIdenticalDeterministicArtifact(t *testing.T) {
	apiInput := []byte(apiFixture + "\n")
	parquetInput := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{validURLIndexParquetFixtureRow()})
	var apiOutput, parquetOutput bytes.Buffer
	apiReport, err := Normalize(context.Background(), bytes.NewReader(apiInput), &apiOutput, Options{Format: FormatCDXAPIJSON})
	if err != nil {
		t.Fatal(err)
	}
	parquetReport, err := NormalizeParquet(context.Background(), bytes.NewReader(parquetInput), int64(len(parquetInput)), &parquetOutput)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(apiOutput.Bytes(), parquetOutput.Bytes()) {
		t.Fatalf("normalized outputs differ:\napi=%s\nparquet=%s", apiOutput.Bytes(), parquetOutput.Bytes())
	}
	inputDigest := sha256.Sum256(parquetInput)
	if parquetReport.Format != FormatURLIndexParquet || parquetReport.Records != 1 || parquetReport.InputBytes != uint64(len(parquetInput)) || parquetReport.InputSHA256 != hex.EncodeToString(inputDigest[:]) || parquetReport.OutputSHA256 != apiReport.OutputSHA256 {
		t.Fatalf("Parquet report = %#v", parquetReport)
	}
}

func TestNormalizeURLIndexParquetStreamsMultipleRowGroupsInPhysicalOrder(t *testing.T) {
	first := validURLIndexParquetFixtureRow()
	second := first
	second.URLSurtKey = "org,example)/second"
	second.URL = "https://example.org/second"
	second.FetchTime = time.Date(2025, 10, 15, 1, 2, 3, 0, time.UTC).UnixMilli()
	second.WARCRecordOffset++
	input := urlIndexParquetMultiGroupFixture(t, first, second)
	var output bytes.Buffer
	report, err := NormalizeParquet(context.Background(), bytes.NewReader(input), int64(len(input)), &output)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(output.Bytes()), []byte{'\n'})
	if report.Records != 2 || len(lines) != 2 {
		t.Fatalf("report=%#v output=%q", report, output.String())
	}
	firstCandidate, err := DecodeCandidate(lines[0])
	if err != nil {
		t.Fatal(err)
	}
	secondCandidate, err := DecodeCandidate(lines[1])
	if err != nil {
		t.Fatal(err)
	}
	if firstCandidate.URL != first.URL || secondCandidate.URL != second.URL || secondCandidate.Timestamp != "20251015010203" {
		t.Fatalf("physical order changed: first=%#v second=%#v", firstCandidate, secondCandidate)
	}
}

func TestNormalizeURLIndexParquetAllowsMissingEvolutionColumns(t *testing.T) {
	row := validURLIndexParquetFixtureRow()
	input := parquetFixture(t, []urlIndexParquetMinimalRow{{
		URLSurtKey: row.URLSurtKey, URL: row.URL, FetchTime: row.FetchTime, FetchStatus: row.FetchStatus,
		ContentDigest: row.ContentDigest, ContentMIMEType: row.ContentMIMEType,
		WARCFilename: row.WARCFilename, WARCRecordOffset: row.WARCRecordOffset, WARCRecordLength: row.WARCRecordLength,
	}})
	var output bytes.Buffer
	if _, err := NormalizeParquet(context.Background(), bytes.NewReader(input), int64(len(input)), &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "mime-detected") || strings.Contains(output.String(), "languages") || strings.Contains(output.String(), "encoding") {
		t.Fatalf("missing evolution columns were invented: %s", output.String())
	}
}

func TestNormalizeURLIndexParquetAcceptsSparkPhysicalWARCIntegers(t *testing.T) {
	row := validURLIndexParquetFixtureRow()
	schema := parquet.NewSchema("url_index", parquet.Group{
		"url_surtkey":        parquet.String(),
		"url":                parquet.String(),
		"fetch_time":         parquet.Timestamp(parquet.Millisecond),
		"fetch_status":       parquet.Int(16),
		"content_digest":     parquet.Optional(parquet.String()),
		"content_mime_type":  parquet.Optional(parquet.String()),
		"warc_filename":      parquet.String(),
		"warc_record_offset": parquet.Leaf(parquet.Int32Type),
		"warc_record_length": parquet.Leaf(parquet.Int32Type),
	})
	input := parquetAnyFixture(t, schema, map[string]any{
		"url_surtkey": row.URLSurtKey, "url": row.URL, "fetch_time": row.FetchTime,
		"fetch_status": row.FetchStatus, "content_digest": *row.ContentDigest,
		"content_mime_type": *row.ContentMIMEType, "warc_filename": row.WARCFilename,
		"warc_record_offset": row.WARCRecordOffset, "warc_record_length": row.WARCRecordLength,
	})
	var output bytes.Buffer
	if _, err := NormalizeParquet(context.Background(), bytes.NewReader(input), int64(len(input)), &output); err != nil {
		t.Fatal(err)
	}
	candidate, err := DecodeCandidate(bytes.TrimSpace(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Offset != strconv.FormatInt(int64(row.WARCRecordOffset), 10) || candidate.Length != strconv.FormatInt(int64(row.WARCRecordLength), 10) {
		t.Fatalf("candidate=%#v", candidate)
	}
}

func TestNormalizeURLIndexParquetRejectsSchemaDriftAndUnusableRows(t *testing.T) {
	valid := validURLIndexParquetFixtureRow()
	missingRequired := parquetFixture(t, []urlIndexParquetMissingURLKeyRow{{
		URL: valid.URL, FetchTime: valid.FetchTime, FetchStatus: valid.FetchStatus,
		ContentDigest: valid.ContentDigest, ContentMIMEType: valid.ContentMIMEType,
		WARCFilename: valid.WARCFilename, WARCRecordOffset: valid.WARCRecordOffset, WARCRecordLength: valid.WARCRecordLength,
	}})
	wrongTimestamp := parquetFixture(t, []urlIndexParquetLogicalTimestampRow{{
		URLSurtKey: valid.URLSurtKey, URL: valid.URL, FetchTime: time.Date(2025, 10, 14, 22, 2, 59, 0, time.UTC), FetchStatus: valid.FetchStatus,
		ContentDigest: valid.ContentDigest, ContentMIMEType: valid.ContentMIMEType,
		WARCFilename: valid.WARCFilename, WARCRecordOffset: valid.WARCRecordOffset, WARCRecordLength: valid.WARCRecordLength,
	}})
	nullDigest := valid
	nullDigest.ContentDigest = nil
	subsecond := valid
	subsecond.FetchTime = time.Date(2025, 10, 14, 22, 2, 59, int(time.Millisecond), time.UTC).UnixMilli()
	for name, input := range map[string][]byte{
		"missing required column": missingRequired,
		"logical timestamp":       wrongTimestamp,
		"null required metadata":  urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{nullDigest}),
		"subsecond timestamp":     urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{subsecond}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeParquet(context.Background(), bytes.NewReader(input), int64(len(input)), io.Discard); err == nil {
				t.Fatal("invalid Parquet input accepted")
			}
		})
	}
}

func TestNormalizeURLIndexParquetAcceptsLegacyINT96CaptureTime(t *testing.T) {
	row := validURLIndexParquetFixtureRow()
	input := parquetFixture(t, []urlIndexParquetLegacyRow{{
		URLSurtKey: row.URLSurtKey, URL: row.URL,
		FetchTime: int96Timestamp(time.Date(2025, 10, 14, 22, 2, 59, 0, time.UTC)), FetchStatus: row.FetchStatus,
		ContentDigest: row.ContentDigest, ContentMIMEType: row.ContentMIMEType,
		WARCFilename: row.WARCFilename, WARCRecordOffset: row.WARCRecordOffset, WARCRecordLength: row.WARCRecordLength,
	}})
	var output bytes.Buffer
	if _, err := NormalizeParquet(context.Background(), bytes.NewReader(input), int64(len(input)), &output); err != nil {
		t.Fatal(err)
	}
	candidate, err := DecodeCandidate(bytes.TrimSpace(output.Bytes()))
	if err != nil || candidate.Timestamp != "20251014220259" {
		t.Fatalf("legacy candidate=%#v error=%v", candidate, err)
	}
}

func TestNormalizeURLIndexParquetRejectsMalformedBoundedAndCanceledInputs(t *testing.T) {
	footerBomb := make([]byte, 12)
	copy(footerBomb[:4], "PAR1")
	binary.LittleEndian.PutUint32(footerBomb[4:8], uint32(MaxParquetFooterBytes+1))
	copy(footerBomb[8:], "PAR1")
	for name, input := range map[string][]byte{
		"short":       []byte("PAR1"),
		"wrong magic": []byte("NOPE\x00\x00\x00\x00PAR1"),
		"footer bomb": footerBomb,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeParquet(context.Background(), bytes.NewReader(input), int64(len(input)), io.Discard); err == nil {
				t.Fatal("malformed Parquet input accepted")
			}
		})
	}
	valid := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{validURLIndexParquetFixtureRow()})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NormalizeParquet(ctx, bytes.NewReader(valid), int64(len(valid)), io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}
	if _, err := NormalizeParquet(context.Background(), bytes.NewReader(valid), int64(len(valid)), errorWriter{}); err == nil {
		t.Fatal("writer error ignored")
	}
}

func TestNormalizeURLIndexParquetPreflightsPageHeaders(t *testing.T) {
	raw := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{validURLIndexParquetFixtureRow()})
	file, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		t.Fatal(err)
	}
	leaf, ok := file.Schema().Lookup("url_surtkey")
	if !ok {
		t.Fatal("fixture URL key column missing")
	}
	metadata := &file.Metadata().RowGroups[0].Columns[leaf.ColumnIndex].MetaData
	pageOffset := metadata.DataPageOffset
	if metadata.DictionaryPageOffset > 0 && metadata.DictionaryPageOffset < pageOffset {
		pageOffset = metadata.DictionaryPageOffset
	}
	corrupt := append([]byte(nil), raw...)
	corrupt[pageOffset] ^= 0xff
	if _, err := NormalizeParquet(context.Background(), bytes.NewReader(corrupt), int64(len(corrupt)), io.Discard); err == nil {
		t.Fatal("corrupt page header reached the Parquet decoder")
	}
}

func TestURLIndexParquetPreflightRejectsOverlappingProjectedChunks(t *testing.T) {
	chunks := []parquetChunkPreflight{
		{start: 4, end: 104},
		{start: 103, end: 203},
	}
	if err := validateParquetChunkRanges(context.Background(), chunks, 256); err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("overlapping chunk error = %v", err)
	}
}

func TestURLIndexParquetPreflightBoundsDictionaryCardinality(t *testing.T) {
	header := format.PageHeader{
		Type:                 format.DictionaryPage,
		CompressedPageSize:   1,
		UncompressedPageSize: 4,
	}
	header.DictionaryPageHeader.Valid = true
	header.DictionaryPageHeader.V = format.DictionaryPageHeader{NumValues: math.MaxInt32, Encoding: format.Plain}
	var encoded bytes.Buffer
	protocol := new(thrift.CompactProtocol)
	if err := thrift.NewEncoder(protocol.NewWriter(&encoded)).Encode(&header); err != nil {
		t.Fatal(err)
	}
	encoded.WriteByte(0)
	chunk := &format.ColumnChunk{}
	chunk.MetaData.NumValues = 1
	chunk.MetaData.TotalCompressedSize = int64(encoded.Len())
	chunk.MetaData.TotalUncompressedSize = int64(encoded.Len())
	chunk.MetaData.DataPageOffset = 4
	chunk.MetaData.DictionaryPageOffset = 4
	input := append(make([]byte, 4), encoded.Bytes()...)
	preflight, err := parquetColumnChunkPreflight(int64(len(input)), chunk, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validateParquetColumnChunk(context.Background(), bytes.NewReader(input), preflight, 0); err == nil || !strings.Contains(err.Error(), "dictionary") {
		t.Fatalf("dictionary cardinality error = %v", err)
	}
}

func TestURLIndexParquetPagePreflightHonorsCancellation(t *testing.T) {
	raw := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{validURLIndexParquetFixtureRow()})
	file, err := parquet.OpenFile(bytes.NewReader(raw), int64(len(raw)), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		t.Fatal(err)
	}
	leaf, ok := file.Schema().Lookup("url_surtkey")
	if !ok {
		t.Fatal("fixture URL key column missing")
	}
	chunk := &file.Metadata().RowGroups[0].Columns[leaf.ColumnIndex]
	preflight, err := parquetColumnChunkPreflight(int64(len(raw)-8), chunk, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	reader := &cancelingParquetReaderAt{ReaderAt: bytes.NewReader(raw), cancel: cancel, offset: preflight.start}
	if _, err := validateParquetColumnChunk(ctx, reader, preflight, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("page preflight cancellation error = %v", err)
	}
}

type cancelingParquetReaderAt struct {
	io.ReaderAt
	cancel context.CancelFunc
	offset int64
}

func (reader *cancelingParquetReaderAt) ReadAt(buffer []byte, offset int64) (int, error) {
	count, err := reader.ReaderAt.ReadAt(buffer, offset)
	if offset >= reader.offset {
		reader.cancel()
	}
	return count, err
}

func TestURLIndexParquetBoundedThriftReaderRejectsAllocationDeclarations(t *testing.T) {
	protocol := new(thrift.CompactProtocol)
	t.Run("collection", func(t *testing.T) {
		fixture := parquetThriftCollectionFixture{Values: make([]int32, maxParquetThriftCollection+1)}
		var encoded bytes.Buffer
		if err := thrift.NewEncoder(protocol.NewWriter(&encoded)).Encode(&fixture); err != nil {
			t.Fatal(err)
		}
		bounded := newBoundedThriftReader(protocol.NewReader(bytes.NewReader(encoded.Bytes())), encoded.Len())
		var decoded parquetThriftCollectionFixture
		if err := thrift.NewDecoder(bounded).Decode(&decoded); err == nil || !strings.Contains(err.Error(), "collection") {
			t.Fatalf("collection declaration error = %v", err)
		}
	})
	t.Run("blob", func(t *testing.T) {
		fixture := parquetThriftBlobFixture{Value: bytes.Repeat([]byte{'x'}, maxParquetThriftBlobBytes+1)}
		var encoded bytes.Buffer
		if err := thrift.NewEncoder(protocol.NewWriter(&encoded)).Encode(&fixture); err != nil {
			t.Fatal(err)
		}
		bounded := newBoundedThriftReader(protocol.NewReader(bytes.NewReader(encoded.Bytes())), encoded.Len())
		var decoded parquetThriftBlobFixture
		if err := thrift.NewDecoder(bounded).Decode(&decoded); err == nil || !strings.Contains(err.Error(), "payload") {
			t.Fatalf("blob declaration error = %v", err)
		}
	})
}

func TestNormalizeURLIndexParquetRejectsFooterCollectionBeforeOpen(t *testing.T) {
	metadata := format.FileMetaData{
		Version: 1,
		Schema:  make(thrift.Slice[format.SchemaElement], maxParquetThriftCollection+1),
		NumRows: 1,
	}
	protocol := new(thrift.CompactProtocol)
	var footer bytes.Buffer
	if err := thrift.NewEncoder(protocol.NewWriter(&footer)).Encode(&metadata); err != nil {
		t.Fatal(err)
	}
	if footer.Len() > int(MaxParquetFooterBytes) {
		t.Fatalf("hostile fixture footer unexpectedly exceeds envelope: %d", footer.Len())
	}
	var input bytes.Buffer
	input.WriteString("PAR1")
	input.Write(footer.Bytes())
	var trailer [8]byte
	binary.LittleEndian.PutUint32(trailer[:4], uint32(footer.Len()))
	copy(trailer[4:], "PAR1")
	input.Write(trailer[:])
	if _, err := NormalizeParquet(context.Background(), bytes.NewReader(input.Bytes()), int64(input.Len()), io.Discard); err == nil || !strings.Contains(err.Error(), "bounded footer") || !strings.Contains(err.Error(), "collection") {
		t.Fatalf("hostile footer error = %v", err)
	}
}

func TestURLIndexParquetStructuralScanRejectsNestedBombs(t *testing.T) {
	t.Run("struct", func(t *testing.T) {
		raw := bytes.Repeat([]byte{0x1c}, maxParquetThriftDepth+1) // field 1, STRUCT
		raw = append(raw, bytes.Repeat([]byte{0x00}, maxParquetThriftDepth+2)...)
		if _, err := validateCompactThriftStructure(context.Background(), bytes.NewReader(raw), 1024); err == nil || !strings.Contains(err.Error(), "nesting depth") {
			t.Fatalf("nested struct error = %v", err)
		}
	})
	t.Run("list", func(t *testing.T) {
		raw := []byte{0x19}                                                       // field 1, LIST
		raw = append(raw, bytes.Repeat([]byte{0x19}, maxParquetThriftDepth+1)...) // one LIST element of type LIST
		raw = append(raw, 0x15, 0x00, 0x00)                                       // one I32 zero, then top-level STOP
		if _, err := validateCompactThriftStructure(context.Background(), bytes.NewReader(raw), 1024); err == nil || !strings.Contains(err.Error(), "nesting depth") {
			t.Fatalf("nested list error = %v", err)
		}
	})
	t.Run("tokens", func(t *testing.T) {
		raw := bytes.Repeat([]byte{0x15, 0x00}, 201) // field 1, I32 zero
		raw = append(raw, 0x00)
		if _, err := validateCompactThriftStructure(context.Background(), bytes.NewReader(raw), 100); err == nil || !strings.Contains(err.Error(), "token count") {
			t.Fatalf("field token error = %v", err)
		}
	})
}

func TestNormalizeURLIndexParquetRejectsKnownFieldTypeConfusionBeforeOpen(t *testing.T) {
	payload := bytes.Repeat([]byte{0x1c}, maxParquetThriftDepth) // nested STRUCT field bytes hidden inside BINARY
	footer := []byte{0x18}                                       // FileMetaData.Version with wrong BINARY type
	footer = binary.AppendUvarint(footer, uint64(len(payload)))
	footer = append(footer, payload...)
	footer = append(footer, 0x00)
	var input bytes.Buffer
	input.WriteString("PAR1")
	input.Write(footer)
	var trailer [8]byte
	binary.LittleEndian.PutUint32(trailer[:4], uint32(len(footer)))
	copy(trailer[4:], "PAR1")
	input.Write(trailer[:])
	if _, err := NormalizeParquet(context.Background(), bytes.NewReader(input.Bytes()), int64(input.Len()), io.Discard); err == nil || !strings.Contains(err.Error(), "bounded footer decode") || !strings.Contains(err.Error(), "type") {
		t.Fatalf("known-field type confusion error = %v", err)
	}
}

type parquetThriftCollectionFixture struct {
	Values []int32 `thrift:"1,required"`
}

type parquetThriftBlobFixture struct {
	Value []byte `thrift:"1,required"`
}

func TestNormalizeURLIndexParquetRejectsInputChangedBetweenHashPasses(t *testing.T) {
	raw := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{validURLIndexParquetFixtureRow()})
	reader := &changingParquetReaderAt{raw: raw}
	if _, err := NormalizeParquet(context.Background(), reader, int64(len(raw)), io.Discard); err == nil || !strings.Contains(err.Error(), "input changed") {
		t.Fatalf("changed input error = %v", err)
	}
}

type changingParquetReaderAt struct {
	raw       []byte
	fullReads int
}

func (reader *changingParquetReaderAt) ReadAt(buffer []byte, offset int64) (int, error) {
	if offset < 0 || offset >= int64(len(reader.raw)) {
		return 0, io.EOF
	}
	count := copy(buffer, reader.raw[offset:])
	if offset == 0 && len(buffer) == len(reader.raw) {
		reader.fullReads++
		if reader.fullReads == 2 {
			buffer[count-1] ^= 1
		}
	}
	if count != len(buffer) {
		return count, io.EOF
	}
	return count, nil
}

func TestDecodeINT96Timestamp(t *testing.T) {
	known := int96Timestamp(time.Date(2026, 7, 19, 14, 15, 16, 0, time.UTC))
	if got, err := decodeINT96Timestamp(known); err != nil || got != "20260719141516" {
		t.Fatalf("known timestamp = %q, %v", got, err)
	}
	dayOverflow := uint64(nanosecondsPerDay)
	for name, value := range map[string]deprecated.Int96{
		"day overflow": {uint32(dayOverflow), uint32(dayOverflow >> 32), uint32(unixEpochJulianDay)},
		"ancient year": {0, 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeINT96Timestamp(value); err == nil {
				t.Fatal("invalid INT96 timestamp accepted")
			}
		})
	}
}

func TestDecodeMillisTimestamp(t *testing.T) {
	known := time.Date(2026, 7, 19, 14, 15, 16, 0, time.UTC).UnixMilli()
	if got, err := decodeMillisTimestamp(known); err != nil || got != "20260719141516" {
		t.Fatalf("known timestamp = %q, %v", got, err)
	}
	if _, err := decodeMillisTimestamp(known + 1); err == nil {
		t.Fatal("subsecond timestamp accepted")
	}
}

type urlIndexParquetFixtureRow struct {
	URLSurtKey          string  `parquet:"url_surtkey"`
	URL                 string  `parquet:"url"`
	FetchTime           int64   `parquet:"fetch_time,timestamp(millisecond)"`
	FetchStatus         int16   `parquet:"fetch_status"`
	ContentDigest       *string `parquet:"content_digest"`
	ContentMIMEType     *string `parquet:"content_mime_type"`
	ContentMIMEDetected *string `parquet:"content_mime_detected"`
	ContentCharset      *string `parquet:"content_charset"`
	ContentLanguages    *string `parquet:"content_languages"`
	WARCFilename        string  `parquet:"warc_filename"`
	WARCRecordOffset    int32   `parquet:"warc_record_offset"`
	WARCRecordLength    int32   `parquet:"warc_record_length"`
	ExtraColumn         *string `parquet:"future_optional_column"`
}

type urlIndexParquetMinimalRow struct {
	URLSurtKey       string  `parquet:"url_surtkey"`
	URL              string  `parquet:"url"`
	FetchTime        int64   `parquet:"fetch_time,timestamp(millisecond)"`
	FetchStatus      int16   `parquet:"fetch_status"`
	ContentDigest    *string `parquet:"content_digest"`
	ContentMIMEType  *string `parquet:"content_mime_type"`
	WARCFilename     string  `parquet:"warc_filename"`
	WARCRecordOffset int32   `parquet:"warc_record_offset"`
	WARCRecordLength int32   `parquet:"warc_record_length"`
}

type urlIndexParquetMissingURLKeyRow struct {
	URL              string  `parquet:"url"`
	FetchTime        int64   `parquet:"fetch_time,timestamp(millisecond)"`
	FetchStatus      int16   `parquet:"fetch_status"`
	ContentDigest    *string `parquet:"content_digest"`
	ContentMIMEType  *string `parquet:"content_mime_type"`
	WARCFilename     string  `parquet:"warc_filename"`
	WARCRecordOffset int32   `parquet:"warc_record_offset"`
	WARCRecordLength int32   `parquet:"warc_record_length"`
}

type urlIndexParquetLegacyRow struct {
	URLSurtKey       string           `parquet:"url_surtkey"`
	URL              string           `parquet:"url"`
	FetchTime        deprecated.Int96 `parquet:"fetch_time"`
	FetchStatus      int16            `parquet:"fetch_status"`
	ContentDigest    *string          `parquet:"content_digest"`
	ContentMIMEType  *string          `parquet:"content_mime_type"`
	WARCFilename     string           `parquet:"warc_filename"`
	WARCRecordOffset int32            `parquet:"warc_record_offset"`
	WARCRecordLength int32            `parquet:"warc_record_length"`
}

type urlIndexParquetLogicalTimestampRow struct {
	URLSurtKey       string    `parquet:"url_surtkey"`
	URL              string    `parquet:"url"`
	FetchTime        time.Time `parquet:"fetch_time,timestamp(microsecond)"`
	FetchStatus      int16     `parquet:"fetch_status"`
	ContentDigest    *string   `parquet:"content_digest"`
	ContentMIMEType  *string   `parquet:"content_mime_type"`
	WARCFilename     string    `parquet:"warc_filename"`
	WARCRecordOffset int32     `parquet:"warc_record_offset"`
	WARCRecordLength int32     `parquet:"warc_record_length"`
}

func urlIndexParquetFixture(t *testing.T, rows []urlIndexParquetFixtureRow) []byte {
	return parquetFixture(t, rows)
}

func parquetFixture[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := parquet.NewGenericWriter[T](&buffer)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func parquetAnyFixture(t *testing.T, schema *parquet.Schema, row map[string]any) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := parquet.NewGenericWriter[any](&buffer, schema)
	if _, err := writer.Write([]any{row}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func urlIndexParquetMultiGroupFixture(t *testing.T, first, second urlIndexParquetFixtureRow) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := parquet.NewGenericWriter[urlIndexParquetFixtureRow](&buffer)
	if _, err := writer.Write([]urlIndexParquetFixtureRow{first}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]urlIndexParquetFixtureRow{second}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func validURLIndexParquetFixtureRow() urlIndexParquetFixtureRow {
	return urlIndexParquetFixtureRow{
		URLSurtKey:          "org,commoncrawl)/get-started",
		URL:                 "https://www.commoncrawl.org/get-started",
		FetchTime:           time.Date(2025, 10, 14, 22, 2, 59, 0, time.UTC).UnixMilli(),
		FetchStatus:         200,
		ContentDigest:       stringPointer("D4IRZZ6NS7QW37BB2ODQPCUGG7ISRFGV"),
		ContentMIMEType:     stringPointer("text/html"),
		ContentMIMEDetected: stringPointer("text/html"),
		ContentCharset:      stringPointer("UTF-8"),
		ContentLanguages:    stringPointer("eng"),
		WARCFilename:        "crawl-data/CC-MAIN-2025-43/segments/fixture/warc/CC-MAIN-20251014214924-00000.warc.gz",
		WARCRecordOffset:    686242195,
		WARCRecordLength:    12675,
		ExtraColumn:         stringPointer("ignored schema evolution"),
	}
}

func int96Timestamp(value time.Time) deprecated.Int96 {
	value = value.UTC()
	dayStart := time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
	nanosOfDay := uint64(value.Sub(dayStart))
	julianDay := uint32(dayStart.Unix()/86400 + 2_440_588)
	return deprecated.Int96{uint32(nanosOfDay), uint32(nanosOfDay >> 32), julianDay}
}

func stringPointer(value string) *string { return &value }

func TestNormalizeAcceptsOfficialMinimalRawCDXJShape(t *testing.T) {
	input := []byte(`org,example)/ 20140101000000 {"url":"http://example.org/","mime":"text/html","status":"200","digest":"sha1:D4IRZZ6NS7QW37BB2ODQPCUGG7ISRFGV","length":"1234","offset":"5678","filename":"crawl-data/CC-MAIN-2014-01/segments/fixture/warc/CC-MAIN-20140101000000-00000.warc.gz"}` + "\n")
	var output bytes.Buffer
	report, err := Normalize(context.Background(), bytes.NewReader(input), &output, Options{Format: FormatCDXJHeader})
	if err != nil {
		t.Fatal(err)
	}
	if report.Records != 1 || strings.Contains(output.String(), "sha1:") || strings.Contains(output.String(), "mime-detected") || strings.Contains(output.String(), "languages") {
		t.Fatalf("minimal normalized output=%q report=%#v", output.String(), report)
	}
	if !strings.Contains(output.String(), `"digest":"D4IRZZ6NS7QW37BB2ODQPCUGG7ISRFGV"`) {
		t.Fatalf("digest was not normalized: %s", output.String())
	}
}

func TestDecodeCandidateStrictlyAcceptsOnlyNormalizedV1(t *testing.T) {
	var normalized bytes.Buffer
	if _, err := Normalize(context.Background(), strings.NewReader(apiFixture+"\n"), &normalized, Options{Format: FormatCDXAPIJSON}); err != nil {
		t.Fatal(err)
	}
	candidate, err := DecodeCandidate(bytes.TrimSpace(normalized.Bytes()))
	if err != nil || candidate.Version != NormalizedVersion || candidate.URL != "https://www.commoncrawl.org/get-started" {
		t.Fatalf("decoded candidate=%#v error=%v", candidate, err)
	}
	for name, raw := range map[string][]byte{
		"raw CDX":       []byte(apiFixture),
		"unknown field": bytes.Replace(bytes.TrimSpace(normalized.Bytes()), []byte(`"version":1`), []byte(`"version":1,"unknown":true`), 1),
		"wrong version": bytes.Replace(bytes.TrimSpace(normalized.Bytes()), []byte(`"version":1`), []byte(`"version":2`), 1),
		"empty":         nil,
		"oversized":     bytes.Repeat([]byte("x"), MaxNormalizedLineBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeCandidate(raw); err == nil {
				t.Fatal("invalid normalized candidate accepted")
			}
		})
	}
}

func TestNormalizeRejectsSchemaDriftAmbiguityAndMalformedRows(t *testing.T) {
	tests := map[string]string{
		"unknown field":    strings.Replace(apiFixture, `"encoding":"UTF-8"`, `"encoding":"UTF-8","new-field":true`, 1),
		"duplicate field":  strings.Replace(apiFixture, `"status":"200"`, `"status":"200","status":"200"`, 1),
		"null field":       strings.Replace(apiFixture, `"status":"200"`, `"status":null`, 1),
		"leading zero":     strings.Replace(apiFixture, `"length":"12675"`, `"length":"012675"`, 1),
		"bad timestamp":    strings.Replace(apiFixture, `"timestamp":"20251014220259"`, `"timestamp":"20251314220259"`, 1),
		"embedded newline": strings.Replace(apiFixture, `"languages":"eng"`, `"languages":"eng\nfra"`, 1),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			var output bytes.Buffer
			if _, err := Normalize(context.Background(), strings.NewReader(raw+"\n"), &output, Options{Format: FormatCDXAPIJSON}); err == nil {
				t.Fatal("invalid row accepted")
			}
		})
	}
	for name, raw := range map[string]string{
		"missing header":    `{}`,
		"missing payload":   `org,example)/ 20251014220259 `,
		"header whitespace": `org,example)/ bad {}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Normalize(context.Background(), strings.NewReader(raw+"\n"), io.Discard, Options{Format: FormatCDXJHeader}); err == nil {
				t.Fatal("invalid CDXJ row accepted")
			}
		})
	}
}

func TestNormalizeFailsOnEmptyOversizedCanceledAndWriterErrors(t *testing.T) {
	if _, err := Normalize(context.Background(), strings.NewReader(""), io.Discard, Options{Format: FormatCDXAPIJSON}); err == nil {
		t.Fatal("empty input accepted")
	}
	if _, err := Normalize(context.Background(), strings.NewReader(strings.Repeat("x", MaxRawLineBytes+1)), io.Discard, Options{Format: FormatCDXAPIJSON}); err == nil {
		t.Fatal("oversized input accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Normalize(ctx, strings.NewReader(apiFixture+"\n"), io.Discard, Options{Format: FormatCDXAPIJSON}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}
	if _, err := Normalize(context.Background(), strings.NewReader(apiFixture+"\n"), errorWriter{}, Options{Format: FormatCDXAPIJSON}); err == nil {
		t.Fatal("writer error ignored")
	}
	finalCtx, finalCancel := context.WithCancel(context.Background())
	input := &readOnceWithEOF{raw: []byte(apiFixture + "\n")}
	if _, err := Normalize(finalCtx, input, cancelingWriter{cancel: finalCancel}, Options{Format: FormatCDXAPIJSON}); !errors.Is(err, context.Canceled) {
		t.Fatalf("final-write cancellation error = %v", err)
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

type readOnceWithEOF struct {
	raw  []byte
	done bool
}

func (reader *readOnceWithEOF) Read(buffer []byte) (int, error) {
	if reader.done {
		return 0, io.EOF
	}
	reader.done = true
	return copy(buffer, reader.raw), io.EOF
}

type cancelingWriter struct{ cancel context.CancelFunc }

func (writer cancelingWriter) Write(raw []byte) (int, error) {
	writer.cancel()
	return len(raw), nil
}
