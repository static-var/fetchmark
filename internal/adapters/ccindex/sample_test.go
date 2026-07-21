package ccindex

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"slices"
	"testing"
	"time"
)

func TestSelectURLIndexParquetIsDeterministicBoundedAndHostDiverse(t *testing.T) {
	base := validURLIndexParquetFixtureRow()
	sameHost := base
	sameHost.URLSurtKey = "org,commoncrawl)/about"
	sameHost.URL = "https://www.commoncrawl.org/about"
	sameHost.WARCRecordOffset++
	secondHost := base
	secondHost.URLSurtKey = "org,example)/guide"
	secondHost.URL = "https://example.org/guide"
	secondHost.WARCRecordOffset += 2
	thirdHost := base
	thirdHost.URLSurtKey = "net,example)/reference"
	thirdHost.URL = "https://example.net/reference"
	thirdHost.WARCRecordOffset += 3
	query := base
	query.URLSurtKey = "org,query)/search?term=open"
	query.URL = "https://query.org/search?term=open"
	query.WARCRecordOffset += 4
	nonHTML := base
	nonHTML.URLSurtKey = "org,pdf)/paper"
	nonHTML.URL = "https://pdf.org/paper"
	nonHTML.ContentMIMEType = stringPointer("application/pdf")
	nonHTML.ContentMIMEDetected = stringPointer("application/pdf")
	nonHTML.WARCRecordOffset += 5
	otherLanguage := base
	otherLanguage.URLSurtKey = "fr,example)/manuel"
	otherLanguage.URL = "https://example.fr/manuel"
	otherLanguage.ContentLanguages = stringPointer("fra")
	otherLanguage.WARCRecordOffset += 6
	nullDigest := base
	nullDigest.URLSurtKey = "org,invalid)/missing-digest"
	nullDigest.URL = "https://invalid.org/missing-digest"
	nullDigest.ContentDigest = nil
	nullDigest.WARCRecordOffset += 7
	rows := []urlIndexParquetFixtureRow{base, sameHost, secondHost, thirdHost, query, nonHTML, otherLanguage, nullDigest}
	input := urlIndexParquetFixture(t, rows)
	options := ParquetSelectionOptions{
		MaxRecords: 2,
		Languages:  []string{"eng"},
		NotAfter:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	var first bytes.Buffer
	firstReport, err := SelectParquet(context.Background(), bytes.NewReader(input), int64(len(input)), &first, options)
	if err != nil {
		t.Fatal(err)
	}
	var second bytes.Buffer
	secondReport, err := SelectParquet(context.Background(), bytes.NewReader(input), int64(len(input)), &second, options)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) || firstReport.OutputSHA256 != secondReport.OutputSHA256 {
		t.Fatalf("selection is not deterministic:\nfirst=%s\nsecond=%s", first.Bytes(), second.Bytes())
	}
	lines := bytes.Split(bytes.TrimSpace(first.Bytes()), []byte{'\n'})
	if len(lines) != 2 || firstReport.Records != 2 || firstReport.Selection == nil {
		t.Fatalf("report=%#v output=%q", firstReport, first.String())
	}
	hosts := make(map[string]struct{}, len(lines))
	for _, line := range lines {
		candidate, err := DecodeCandidate(line)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := url.Parse(candidate.URL)
		if err != nil {
			t.Fatal(err)
		}
		hosts[parsed.Hostname()] = struct{}{}
	}
	if len(hosts) != 2 {
		t.Fatalf("selected hosts = %v", hosts)
	}
	selection := firstReport.Selection
	if selection.Algorithm != ParquetSelectionAlgorithm || selection.SourceRecords != uint64(len(rows)) || selection.EligibleRecords != 4 || selection.SelectedRecords != 2 || selection.EligibleNotSelected != 2 {
		t.Fatalf("selection report = %#v", selection)
	}
	for reason, expected := range map[string]uint64{
		"invalid_normalized_metadata": 1,
		"language_not_selected":       1,
		"mime_not_html":               1,
		"query_not_allowed":           1,
	} {
		if selection.Rejections[reason] != expected {
			t.Fatalf("rejections[%q]=%d, want %d (%v)", reason, selection.Rejections[reason], expected, selection.Rejections)
		}
	}
}

func TestSelectURLIndexParquetDoesNotDependOnPhysicalRowOrder(t *testing.T) {
	base := validURLIndexParquetFixtureRow()
	rows := make([]urlIndexParquetFixtureRow, 0, 6)
	for index, host := range []string{"a.example.org", "b.example.org", "c.example.org", "d.example.org", "e.example.org", "f.example.org"} {
		row := base
		row.URLSurtKey = host + ")/page"
		row.URL = "https://" + host + "/page"
		row.WARCRecordOffset += int32(index + 1)
		rows = append(rows, row)
	}
	options := ParquetSelectionOptions{MaxRecords: 3, Languages: []string{"eng"}, NotAfter: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	var forward bytes.Buffer
	forwardInput := urlIndexParquetFixture(t, rows)
	forwardReport, err := SelectParquet(context.Background(), bytes.NewReader(forwardInput), int64(len(forwardInput)), &forward, options)
	if err != nil {
		t.Fatal(err)
	}
	slices.Reverse(rows)
	var reverse bytes.Buffer
	reverseInput := urlIndexParquetFixture(t, rows)
	reverseReport, err := SelectParquet(context.Background(), bytes.NewReader(reverseInput), int64(len(reverseInput)), &reverse, options)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(forward.Bytes(), reverse.Bytes()) || forwardReport.OutputSHA256 != reverseReport.OutputSHA256 || forwardReport.InputSHA256 == reverseReport.InputSHA256 {
		t.Fatalf("physical order affected selection:\nforward=%s\nreverse=%s", forward.Bytes(), reverse.Bytes())
	}
}

func TestSelectURLIndexParquetTotallyOrdersEquivalentCaptureLocations(t *testing.T) {
	first := validURLIndexParquetFixtureRow()
	second := first
	first.ContentDigest = stringPointer("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	second.ContentDigest = stringPointer("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
	options := ParquetSelectionOptions{
		MaxRecords: 1,
		Languages:  []string{"eng"},
		NotAfter:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	var forward bytes.Buffer
	forwardInput := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{first, second})
	if _, err := SelectParquet(context.Background(), bytes.NewReader(forwardInput), int64(len(forwardInput)), &forward, options); err != nil {
		t.Fatal(err)
	}
	var reverse bytes.Buffer
	reverseInput := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{second, first})
	if _, err := SelectParquet(context.Background(), bytes.NewReader(reverseInput), int64(len(reverseInput)), &reverse, options); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(forward.Bytes(), reverse.Bytes()) {
		t.Fatalf("equivalent capture order affected selection:\nforward=%s\nreverse=%s", forward.Bytes(), reverse.Bytes())
	}
	candidate, err := DecodeCandidate(bytes.TrimSpace(forward.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Digest != *first.ContentDigest {
		t.Fatalf("selected digest=%q, want %q", candidate.Digest, *first.ContentDigest)
	}
}

func TestSelectURLIndexParquetRejectsInvalidOptionsAndWriterFailure(t *testing.T) {
	input := urlIndexParquetFixture(t, []urlIndexParquetFixtureRow{validURLIndexParquetFixtureRow()})
	for name, options := range map[string]ParquetSelectionOptions{
		"zero records":     {Languages: []string{"eng"}, NotAfter: time.Now().UTC()},
		"too many records": {MaxRecords: MaxParquetSelectionRecords + 1, Languages: []string{"eng"}, NotAfter: time.Now().UTC()},
		"languages":        {MaxRecords: 1, Languages: []string{"eng", "eng"}, NotAfter: time.Now().UTC()},
		"cutoff":           {MaxRecords: 1, Languages: []string{"eng"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := SelectParquet(context.Background(), bytes.NewReader(input), int64(len(input)), io.Discard, options); err == nil {
				t.Fatal("invalid options accepted")
			}
		})
	}
	options := ParquetSelectionOptions{MaxRecords: 1, Languages: []string{"eng"}, NotAfter: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	if _, err := SelectParquet(context.Background(), bytes.NewReader(input), int64(len(input)), errorWriter{}, options); err == nil {
		t.Fatal("writer failure ignored")
	}
}
