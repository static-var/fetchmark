package ccindex

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSelectParquetPartsIsInputOrderIndependentAndGloballyHostDiverse(t *testing.T) {
	base := validURLIndexParquetFixtureRow()
	part := func(values ...struct{ host, path string }) []byte {
		rows := make([]urlIndexParquetFixtureRow, 0, len(values))
		for index, value := range values {
			row := base
			row.URLSurtKey = value.host + ")/" + value.path
			row.URL = "https://" + value.host + "/" + value.path
			row.WARCRecordOffset += int32(index + 1)
			rows = append(rows, row)
		}
		return urlIndexParquetFixture(t, rows)
	}
	first := part(struct{ host, path string }{"a.example.org", "older"}, struct{ host, path string }{"b.example.org", "page"}, struct{ host, path string }{"c.example.org", "page"})
	second := part(struct{ host, path string }{"a.example.org", "newer"}, struct{ host, path string }{"d.example.org", "page"})
	options := ParquetSelectionOptions{Profile: ParquetSelectionProfilePrototype, MaxRecords: 3, Languages: []string{"eng"}, NotAfter: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	inputs := func(left, right []byte) []ParquetSelectionInput {
		return []ParquetSelectionInput{{Reader: bytes.NewReader(left), Size: int64(len(left))}, {Reader: bytes.NewReader(right), Size: int64(len(right))}}
	}
	var forward bytes.Buffer
	forwardReport, err := SelectParquetParts(context.Background(), inputs(first, second), &forward, options)
	if err != nil {
		t.Fatal(err)
	}
	var reverse bytes.Buffer
	reverseReport, err := SelectParquetParts(context.Background(), inputs(second, first), &reverse, options)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(forward.Bytes(), reverse.Bytes()) || forwardReport.OutputSHA256 != reverseReport.OutputSHA256 || forwardReport.InputSHA256 != reverseReport.InputSHA256 {
		t.Fatalf("input order affected multi-part selection:\nforward=%s\nreverse=%s", forward.Bytes(), reverse.Bytes())
	}
	if !reflect.DeepEqual(forwardReport, reverseReport) {
		t.Fatalf("input order affected report:\nforward=%#v\nreverse=%#v", forwardReport, reverseReport)
	}
	if forwardReport.Format != FormatURLIndexParquetPartsSelection || forwardReport.Records != 3 || forwardReport.Parts == nil {
		t.Fatalf("report = %#v", forwardReport)
	}
	parts := forwardReport.Parts
	if parts.Profile != ParquetSelectionProfilePrototype || parts.MaxSourceRecords != MaxParquetPrototypeSourceRecords || parts.InputCount != 2 || parts.SourceRecords != 5 || parts.EligibleRecords != 5 || parts.SelectedRecords != 3 || parts.EligibleNotSelected != 2 || len(parts.Inputs) != 2 {
		t.Fatalf("parts report = %#v", parts)
	}
	hosts := make([]string, 0, 3)
	for _, line := range bytes.Split(bytes.TrimSpace(forward.Bytes()), []byte{'\n'}) {
		candidate, err := DecodeCandidate(line)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := url.Parse(candidate.URL)
		if err != nil {
			t.Fatal(err)
		}
		hosts = append(hosts, parsed.Hostname())
	}
	slices.Sort(hosts)
	if len(hosts) != 3 || hosts[0] == hosts[1] || hosts[1] == hosts[2] {
		t.Fatalf("selected hosts = %v", hosts)
	}
}

func TestSelectParquetPartsRejectsDuplicateInputs(t *testing.T) {
	input := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{validURLIndexParquetFixtureRow()})
	options := ParquetSelectionOptions{MaxRecords: 1, Languages: []string{"eng"}, NotAfter: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	parts := []ParquetSelectionInput{{Reader: bytes.NewReader(input), Size: int64(len(input))}, {Reader: bytes.NewReader(input), Size: int64(len(input))}}
	if _, err := SelectParquetParts(context.Background(), parts, &bytes.Buffer{}, options); err == nil {
		t.Fatal("duplicate exact input accepted")
	}
}

func TestSelectParquetPartsEmitsNothingWhenAnyInputChanges(t *testing.T) {
	first := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{validURLIndexParquetFixtureRow()})
	secondRow := validURLIndexParquetFixtureRow()
	secondRow.URLSurtKey = "org,example)/page"
	secondRow.URL = "https://example.org/page"
	second := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{secondRow})
	inputs := []ParquetSelectionInput{
		{Reader: bytes.NewReader(first), Size: int64(len(first))},
		{Reader: &changingParquetReaderAt{raw: second}, Size: int64(len(second))},
	}
	options := ParquetSelectionOptions{MaxRecords: 2, Languages: []string{"eng"}, NotAfter: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	var output bytes.Buffer
	if _, err := SelectParquetParts(context.Background(), inputs, &output, options); err == nil || !strings.Contains(err.Error(), "input changed") {
		t.Fatalf("changed input error = %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("wrote %d bytes before all inputs were verified", output.Len())
	}
}

func TestSelectParquetPartsRechecksEarlierInputsAfterLaterScans(t *testing.T) {
	first := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{validURLIndexParquetFixtureRow()})
	secondRow := validURLIndexParquetFixtureRow()
	secondRow.URLSurtKey = "org,example)/page"
	secondRow.URL = "https://example.org/page"
	second := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{secondRow})
	firstReader := bytes.NewReader(first)
	secondReader := &callbackReaderAt{ReaderAt: bytes.NewReader(second), callback: func() { first[len(first)-1] ^= 1 }}
	inputs := []ParquetSelectionInput{
		{Reader: firstReader, Size: int64(len(first))},
		{Reader: secondReader, Size: int64(len(second))},
	}
	options := ParquetSelectionOptions{MaxRecords: 2, Languages: []string{"eng"}, NotAfter: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	var output bytes.Buffer
	if _, err := SelectParquetParts(context.Background(), inputs, &output, options); err == nil || !strings.Contains(err.Error(), "input changed") {
		t.Fatalf("changed earlier input error = %v", err)
	}
	if output.Len() != 0 {
		t.Fatalf("wrote %d bytes before all inputs were reverified", output.Len())
	}
}

func TestSelectParquetPartsRechecksInputsAfterOutput(t *testing.T) {
	first := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{validURLIndexParquetFixtureRow()})
	secondRow := validURLIndexParquetFixtureRow()
	secondRow.URLSurtKey = "org,example)/page"
	secondRow.URL = "https://example.org/page"
	second := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{secondRow})
	inputs := []ParquetSelectionInput{
		{Reader: bytes.NewReader(first), Size: int64(len(first))},
		{Reader: bytes.NewReader(second), Size: int64(len(second))},
	}
	output := &callbackWriter{callback: func() { first[len(first)-1] ^= 1 }}
	options := ParquetSelectionOptions{MaxRecords: 2, Languages: []string{"eng"}, NotAfter: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	if _, err := SelectParquetParts(context.Background(), inputs, output, options); err == nil || !strings.Contains(err.Error(), "input changed") {
		t.Fatalf("post-output input change error = %v", err)
	}
	if output.Len() == 0 {
		t.Fatal("test did not reach staged output before mutation")
	}
}

type callbackReaderAt struct {
	io.ReaderAt
	once     sync.Once
	callback func()
}

func (reader *callbackReaderAt) ReadAt(buffer []byte, offset int64) (int, error) {
	reader.once.Do(reader.callback)
	return reader.ReaderAt.ReadAt(buffer, offset)
}

type callbackWriter struct {
	bytes.Buffer
	once     sync.Once
	callback func()
}

func (writer *callbackWriter) Write(buffer []byte) (int, error) {
	writer.once.Do(writer.callback)
	return writer.Buffer.Write(buffer)
}

func TestParquetSelectionPrototypeRequiresExplicitProfile(t *testing.T) {
	if got := maxParquetSelectionSourceRecords(ParquetSelectionProfileCollector); got != MaxParquetCollectorSourceRecords {
		t.Fatalf("collector source-row ceiling = %d", got)
	}
	if got := maxParquetSelectionSourceRecords(ParquetSelectionProfilePrototype); got != MaxParquetPrototypeSourceRecords {
		t.Fatalf("prototype source-row ceiling = %d", got)
	}
	base := ParquetSelectionOptions{MaxRecords: MaxParquetSelectionRecords + 1, Languages: []string{"eng"}, NotAfter: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	if _, err := ValidateParquetSelectionOptions(base); err == nil {
		t.Fatal("ordinary collector profile accepted a prototype-sized selection")
	}
	base.Profile = ParquetSelectionProfilePrototype
	validated, err := ValidateParquetSelectionOptions(base)
	if err != nil {
		t.Fatal(err)
	}
	if validated.Profile != ParquetSelectionProfilePrototype {
		t.Fatalf("profile = %q", validated.Profile)
	}
	base.MaxRecords = MaxParquetPrototypeSelectionRecords + 1
	if _, err := ValidateParquetSelectionOptions(base); err == nil {
		t.Fatal("prototype profile exceeded its explicit ceiling")
	}
}
