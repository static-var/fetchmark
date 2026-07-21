package indexpackselection

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/staticvar/fetchmark/internal/core/canonicalurl"
	"github.com/staticvar/fetchmark/internal/core/indexpack"
)

func TestSelectMetadataOnlyCandidatesDeterministically(t *testing.T) {
	created := time.Date(2026, time.July, 19, 0, 0, 0, 0, time.UTC)
	candidates := []Candidate{
		candidate("https://docs.example.org/guides/open-search", "eng", "200", created.Add(-time.Hour)),
		candidate("https://DOCS.example.org/guides/open-search?utm_source=duplicate", "eng", "200", created.Add(-time.Hour)),
		candidate("https://query.example.org/guides/open-search?token=private", "eng", "200", created.Add(-time.Hour)),
		candidate("https://child.optout.org/index", "eng", "200", created.Add(-time.Hour)),
		candidate("https://stale.example.org/index", "eng", "200", created.Add(-25*time.Hour)),
		candidate("https://missing.example.org/index", "eng", "404", created.Add(-time.Hour)),
		candidate("https://wissen.example.de/handbuch/suche", "deu", "200", created.Add(-time.Hour)),
		candidate("https://wissen.example.de/handbuch/zweite", "deu", "200", created.Add(-time.Hour)),
	}
	input := encodeCandidates(t, candidates)
	specRaw := selectionSpec(t, created, input, 2, 1)
	exclusionsRaw := []byte(`{"version":1,"urls":[],"host_suffixes":["optout.org"]}`)

	var records []indexpack.Record
	report, err := Select(context.Background(), created, specRaw, exclusionsRaw, bytes.NewReader(input), func(record indexpack.Record) error {
		records = append(records, record)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Candidates != 8 || report.Accepted != 2 || report.Rejected != 6 || report.UniqueHosts != 2 {
		t.Fatalf("report counts = %#v", report)
	}
	inputDigest := sha256.Sum256(input)
	if report.Version != ReportVersion || report.CandidateSHA256 != hex.EncodeToString(inputDigest[:]) {
		t.Fatalf("candidate binding = %#v", report)
	}
	wantReasons := map[string]uint64{
		"query_not_allowed": 2, "excluded_host": 1, "permission_stale": 1, "status_not_200": 1, "host_limit": 1,
	}
	if !equalCounts(report.RejectedByReason, wantReasons) || report.AcceptedByLanguage["eng"] != 1 || report.AcceptedByLanguage["deu"] != 1 {
		t.Fatalf("report breakdown = %#v", report)
	}
	if len(records) != 2 || records[0].URL != "https://docs.example.org/guides/open-search" || records[1].URL != "https://wissen.example.de/handbuch/suche" {
		t.Fatalf("records = %#v", records)
	}
	if len(records[0].AnchorTerms) != 0 || records[0].Title != "" || len(records[0].Headings) != 0 || records[0].SalientSketch != "" {
		t.Fatalf("metadata-only record contains enriched fields = %#v", records[0])
	}
	if records[0].Provenance[0].SourceURI != "https://data.commoncrawl.org/"+candidates[0].Filename || records[0].AuthorityScore != 0 {
		t.Fatalf("provenance/authority = %#v", records[0])
	}

	var second []indexpack.Record
	secondReport, err := Select(context.Background(), created, specRaw, exclusionsRaw, bytes.NewReader(input), func(record indexpack.Record) error {
		second = append(second, record)
		return nil
	})
	if err != nil || fmt.Sprint(records) != fmt.Sprint(second) || fmt.Sprint(report) != fmt.Sprint(secondReport) {
		t.Fatalf("second selection differs: err=%v records=%#v report=%#v", err, second, secondReport)
	}
}

func TestSelectFailsClosedOnLateDigestMismatchAndMalformedInput(t *testing.T) {
	created := time.Date(2026, time.July, 19, 0, 0, 0, 0, time.UTC)
	input := encodeCandidates(t, []Candidate{candidate("https://docs.example.org/a", "eng", "200", created.Add(-time.Hour))})
	specRaw := selectionSpec(t, created, []byte("different"), 10, 10)
	exclusionsRaw := []byte(`{"version":1,"urls":[],"host_suffixes":[]}`)
	emitted := 0
	_, err := Select(context.Background(), created, specRaw, exclusionsRaw, bytes.NewReader(input), func(indexpack.Record) error {
		emitted++
		return nil
	})
	if !errors.Is(err, ErrInvalidCandidate) || emitted != 1 || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("digest mismatch err=%v emitted=%d", err, emitted)
	}

	bad := append(append([]byte(nil), input...), []byte(`{"unknown":true}`+"\n")...)
	actualSpecRaw := selectionSpec(t, created, bad, 10, 10)
	_, err = Select(context.Background(), created, actualSpecRaw, exclusionsRaw, bytes.NewReader(bad), func(indexpack.Record) error { return nil })
	if !errors.Is(err, ErrInvalidCandidate) || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown-field error = %v", err)
	}

	longInput := encodeCandidates(t, []Candidate{candidate("https://docs.example.org/"+strings.Repeat("a", 3_000), "eng", "200", created.Add(-time.Hour))})
	longSpecRaw := selectionSpec(t, created, longInput, 10, 10)
	_, err = Select(context.Background(), created, longSpecRaw, exclusionsRaw, bytes.NewReader(longInput), func(indexpack.Record) error { return nil })
	if !errors.Is(err, ErrInvalidCandidate) || !strings.Contains(err.Error(), "selected record is invalid") {
		t.Fatalf("oversized selected-record error = %v", err)
	}
}

func TestPreflightCandidateSharesCaptureEligibilityWithSelection(t *testing.T) {
	cutoff := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
	valid := candidate("https://docs.example.org/page", "eng", "200", cutoff.Add(-time.Hour))
	valid.Timestamp = cutoff.Add(-time.Hour).Format("20060102150405")
	preflight, reason, err := PreflightCandidate(valid, cutoff)
	if err != nil || reason != "" || preflight.CanonicalURL != valid.URL || preflight.Language != "eng" || preflight.Length != 12345 {
		t.Fatalf("preflight=%#v reason=%q error=%v", preflight, reason, err)
	}
	tests := []struct {
		name       string
		mutate     func(*Candidate)
		wantReason string
		wantError  bool
	}{
		{name: "status", mutate: func(value *Candidate) { value.Status = "204" }, wantReason: "status_not_200"},
		{name: "capture MIME", mutate: func(value *Candidate) { value.MIME = "application/pdf" }, wantReason: "mime_not_html"},
		{name: "detected MIME", mutate: func(value *Candidate) { value.MIMEDetected = "text/plain" }, wantReason: "mime_not_html"},
		{name: "zero length", mutate: func(value *Candidate) { value.Length = "0" }, wantError: true},
		{name: "oversized length", mutate: func(value *Candidate) { value.Length = "67108865" }, wantError: true},
		{name: "invalid offset", mutate: func(value *Candidate) { value.Offset = "-1" }, wantError: true},
		{name: "invalid WARC path", mutate: func(value *Candidate) { value.Filename = "elsewhere/file.warc.gz" }, wantError: true},
		{name: "future capture", mutate: func(value *Candidate) { value.Timestamp = cutoff.Add(time.Second).Format("20060102150405") }, wantReason: "capture_after_build"},
		{name: "invalid language", mutate: func(value *Candidate) { value.Languages = "unknown_language" }, wantReason: "language_invalid"},
		{name: "userinfo", mutate: func(value *Candidate) { value.URL = "https://user:secret@docs.example.org/page" }, wantReason: "non_public_url"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid
			test.mutate(&value)
			_, reason, err := PreflightCandidate(value, cutoff)
			if (err != nil) != test.wantError || reason != test.wantReason {
				t.Fatalf("reason=%q error=%v", reason, err)
			}
		})
	}
}

func TestDecodeSpecAndExclusionsRejectAmbiguousOrUnsafeInputs(t *testing.T) {
	created := time.Date(2026, time.July, 19, 0, 0, 0, 0, time.UTC)
	input := encodeCandidates(t, []Candidate{candidate("https://docs.example.org/a", "eng", "200", created.Add(-time.Hour))})
	validSpec := selectionSpec(t, created, input, 10, 2)
	candidateDigest := sha256.Sum256(input)
	for name, raw := range map[string][]byte{
		"unknown":                        bytes.Replace(validSpec, []byte(`"version":2`), []byte(`"version":2,"unknown":true`), 1),
		"duplicate":                      bytes.Replace(validSpec, []byte(`"version":2`), []byte(`"version":2,"version":2`), 1),
		"bad input":                      bytes.Replace(validSpec, []byte(`"rights_notice":"Common Crawl terms; URL metadata only"`), []byte(`"rights_notice":""`), 1),
		"conflated source and candidate": bytes.Replace(validSpec, []byte(strings.Repeat("5", 64)), []byte(hex.EncodeToString(candidateDigest[:])), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeSpec(raw); !errors.Is(err, ErrInvalidSpec) {
				t.Fatalf("DecodeSpec = %v", err)
			}
		})
	}
	for name, raw := range map[string][]byte{
		"noncanonical URL":  []byte(`{"version":1,"urls":["https://EXAMPLE.org/a"],"host_suffixes":[]}`),
		"unsorted URLs":     []byte(`{"version":1,"urls":["https://z.example.org/","https://a.example.org/"],"host_suffixes":[]}`),
		"private host":      []byte(`{"version":1,"urls":[],"host_suffixes":["localhost"]}`),
		"bad host label":    []byte(`{"version":1,"urls":[],"host_suffixes":["bad..example.org"]}`),
		"host whitespace":   []byte(`{"version":1,"urls":[],"host_suffixes":[" optout.org"]}`),
		"host trailing dot": []byte(`{"version":1,"urls":[],"host_suffixes":["optout.org."]}`),
		"home arpa":         []byte(`{"version":1,"urls":[],"host_suffixes":["router.home.arpa"]}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := DecodeExclusions(raw); !errors.Is(err, ErrInvalidExclusions) {
				t.Fatalf("DecodeExclusions = %v", err)
			}
		})
	}
}

func TestValidCommonCrawlWARCPath(t *testing.T) {
	for path, want := range map[string]bool{
		"crawl-data/CC-MAIN-2026-25/segments/fixture/warc/CC-MAIN-20260701000000-fixture.warc.gz": true,
		"crawl-data/CC-MAIN-2026-25/../forged.warc.gz":                                            false,
		"crawl-data/CC-MAIN-2026-25/%2e%2e/forged.warc.gz":                                        false,
		"crawl-data//CC-MAIN-2026-25/forged.warc.gz":                                              false,
		"crawl-data/not-a-cc-main-artifact.warc.gz":                                               false,
		"/crawl-data/CC-MAIN-2026-25/forged.warc.gz":                                              false,
		`crawl-data\CC-MAIN-2026-25\forged.warc.gz`:                                               false,
	} {
		if got := ValidCommonCrawlWARCPath(path); got != want {
			t.Errorf("ValidCommonCrawlWARCPath(%q) = %t, want %t", path, got, want)
		}
	}
}

func TestSelectKeepsRobotsIndexingAndRightsGatesSeparate(t *testing.T) {
	created := time.Date(2026, time.July, 19, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		mutate func(*Candidate)
		reason string
	}{
		{name: "robots unreachable", reason: "robots_not_allowed", mutate: func(candidate *Candidate) {
			candidate.Admission.Robots.Outcome = "unreachable"
		}},
		{name: "noindex", reason: "indexing_not_permitted", mutate: func(candidate *Candidate) {
			candidate.Admission.Indexing.Outcome = "noindex"
		}},
		{name: "redirect mismatch", reason: "indexing_redirect_mismatch", mutate: func(candidate *Candidate) {
			candidate.Admission.Indexing.FinalURL = "https://other.example.org/a"
		}},
		{name: "rights unknown", reason: "rights_not_permitted", mutate: func(candidate *Candidate) {
			candidate.Admission.Rights.Outcome = "unknown"
		}},
		{name: "rights field scope", reason: "rights_fields_not_permitted", mutate: func(candidate *Candidate) {
			candidate.Admission.Rights.AllowedFields = []string{"title", "url_metadata"}
		}},
		{name: "rights stale", reason: "rights_stale", mutate: func(candidate *Candidate) {
			candidate.Admission.Rights.ValidUntil = created.Add(-time.Second).Format(time.RFC3339)
		}},
		{name: "noncanonical final URL", reason: "indexing_redirect_mismatch", mutate: func(candidate *Candidate) {
			candidate.Admission.Indexing.FinalURL += "?utm_source=evidence"
		}},
		{name: "trailing dot candidate", reason: "non_public_url", mutate: func(candidate *Candidate) {
			candidate.URL = "https://bad.example.org./a"
		}},
		{name: "home arpa candidate", reason: "non_public_url", mutate: func(candidate *Candidate) {
			candidate.URL = "https://router.home.arpa/a"
		}},
		{name: "validity ends at build", reason: "permission_stale", mutate: func(candidate *Candidate) {
			candidate.Admission.Robots.ValidUntil = created.Format(time.RFC3339)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			good := candidate("https://good.example.org/a", "eng", "200", created.Add(-time.Hour))
			bad := candidate("https://bad.example.org/a", "eng", "200", created.Add(-time.Hour))
			test.mutate(&bad)
			input := encodeCandidates(t, []Candidate{good, bad})
			specRaw := selectionSpec(t, created, input, 10, 10)
			exclusionsRaw := []byte(`{"version":1,"urls":[],"host_suffixes":[]}`)
			report, err := Select(context.Background(), created, specRaw, exclusionsRaw, bytes.NewReader(input), func(indexpack.Record) error { return nil })
			if err != nil || report.Accepted != 1 || report.RejectedByReason[test.reason] != 1 {
				t.Fatalf("Select report=%#v err=%v", report, err)
			}
		})
	}
}

func TestSelectBindsAllEvidenceToActualBuildTime(t *testing.T) {
	created := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
	buildTime := created.Add(10 * time.Minute)
	tests := []struct {
		name   string
		reason string
		mutate func(*Candidate)
	}{
		{name: "robots", reason: "permission_stale", mutate: func(candidate *Candidate) {
			candidate.Admission.Robots.ValidUntil = created.Add(time.Minute).Format(time.RFC3339)
		}},
		{name: "indexing", reason: "permission_stale", mutate: func(candidate *Candidate) {
			candidate.Admission.Indexing.ValidUntil = created.Add(time.Minute).Format(time.RFC3339)
		}},
		{name: "rights", reason: "rights_stale", mutate: func(candidate *Candidate) {
			candidate.Admission.Rights.ValidUntil = created.Add(time.Minute).Format(time.RFC3339)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			good := candidate("https://good.example.org/a", "eng", "200", created.Add(-time.Hour))
			bad := candidate("https://bad.example.org/a", "eng", "200", created.Add(-time.Hour))
			test.mutate(&bad)
			input := encodeCandidates(t, []Candidate{good, bad})
			report, err := Select(context.Background(), buildTime, selectionSpec(t, created, input, 10, 10), []byte(`{"version":1,"urls":[],"host_suffixes":[]}`), bytes.NewReader(input), func(indexpack.Record) error { return nil })
			if err != nil || report.Accepted != 1 || report.RejectedByReason[test.reason] != 1 {
				t.Fatalf("Select report=%#v err=%v", report, err)
			}
		})
	}

	futureCreated := buildTime.Add(10 * time.Minute)
	good := candidate("https://good.example.org/a", "eng", "200", buildTime.Add(-time.Hour))
	future := candidate("https://future.example.org/a", "eng", "200", buildTime.Add(time.Minute))
	input := encodeCandidates(t, []Candidate{good, future})
	report, err := Select(context.Background(), buildTime, selectionSpec(t, futureCreated, input, 10, 10), []byte(`{"version":1,"urls":[],"host_suffixes":[]}`), bytes.NewReader(input), func(indexpack.Record) error { return nil })
	if err != nil || report.Accepted != 1 || report.RejectedByReason["permission_stale"] != 1 {
		t.Fatalf("future evidence report=%#v err=%v", report, err)
	}

	input = encodeCandidates(t, []Candidate{candidate("https://good.example.org/a", "eng", "200", created.Add(-time.Hour))})
	_, err = Select(context.Background(), created.Add(MaxBuildTimestampSkew+time.Second), selectionSpec(t, created, input, 10, 10), []byte(`{"version":1,"urls":[],"host_suffixes":[]}`), bytes.NewReader(input), func(indexpack.Record) error { return nil })
	if !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("backdated spec error = %v", err)
	}
}

func TestValidateCompletionRequiresUnexpiredEvidence(t *testing.T) {
	report := Report{PermissionValidUntil: "2026-07-19T12:01:01Z"}
	if err := ValidateCompletion(report, time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	report.PermissionValidUntil = "2026-07-19T12:01:00Z"
	if err := ValidateCompletion(report, time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("completion without the activation safety interval was accepted")
	}
}

func candidate(rawURL, language, status string, checkedAt time.Time) Candidate {
	canonical, err := canonicalurl.V1(rawURL)
	if err != nil {
		panic(err)
	}
	parsed, err := url.Parse(canonical)
	if err != nil {
		panic(err)
	}
	return Candidate{
		URLKey: "org,example)/", Timestamp: "20260701000000", URL: rawURL,
		MIME: "text/html", MIMEDetected: "text/html", Status: status,
		Digest: strings.Repeat("A", 32), Length: "12345", Offset: "67890",
		Filename:  "crawl-data/CC-MAIN-2026-26/segments/fixture/warc/CC-MAIN-20260701000000-fixture.warc.gz",
		Languages: language, Encoding: "UTF-8",
		Admission: Admission{Version: 1,
			Robots: RobotsObservation{
				UserAgent: "Fetchmark-PackBuilder/1", RobotsURI: parsed.Scheme + "://" + parsed.Host + "/robots.txt",
				CheckedAt: checkedAt.UTC().Format(time.RFC3339), ValidUntil: checkedAt.Add(24 * time.Hour).UTC().Format(time.RFC3339),
				Outcome: "allowed", BodySHA256: strings.Repeat("1", 64),
			},
			Indexing: IndexingObservation{
				CheckedAt: checkedAt.UTC().Format(time.RFC3339), ValidUntil: checkedAt.Add(24 * time.Hour).UTC().Format(time.RFC3339),
				FinalURL: canonical, Outcome: "indexable", HeadersSHA256: strings.Repeat("2", 64),
				RepresentationSHA256: strings.Repeat("3", 64), ParserVersion: "fetchmark-noindex-v1",
			},
			Rights: RightsDecision{
				Outcome: "permitted", AllowedFields: []string{"url_metadata"}, Basis: "url_metadata_policy",
				EvidenceURI: "https://commoncrawl.org/terms-of-use", EvidenceSHA256: strings.Repeat("4", 64),
				ObservedAt: checkedAt.UTC().Format(time.RFC3339), ValidUntil: checkedAt.Add(24 * time.Hour).UTC().Format(time.RFC3339),
				RightsNotice: "URL metadata only under publisher policy; third-party rights remain applicable",
			},
		},
	}
}

func encodeCandidates(t *testing.T, candidates []Candidate) []byte {
	t.Helper()
	var output bytes.Buffer
	encoder := json.NewEncoder(&output)
	encoder.SetEscapeHTML(false)
	for _, candidate := range candidates {
		if err := encoder.Encode(candidate); err != nil {
			t.Fatal(err)
		}
	}
	return output.Bytes()
}

func selectionSpec(t *testing.T, created time.Time, input []byte, maxRecords, maxPerHost uint64) []byte {
	t.Helper()
	inputDigest := sha256.Sum256(input)
	spec := Spec{
		Version: SpecVersion, Profile: ProfileLightweight, PackID: "commoncrawl-url-en-de", Revision: 1,
		CreatedAt: created.UTC().Format(time.RFC3339), ExpiresAt: created.Add(30 * 24 * time.Hour).UTC().Format(time.RFC3339),
		Publisher: indexpack.Publisher{
			Name: "Fetchmark test publisher", ContactURI: "mailto:publisher@example.org",
			TakedownURI: "https://publisher.example.org/takedown", RightsNotice: "URL-derived metadata only; no page bodies",
		},
		Languages: []string{"deu", "eng"}, MaxRecords: maxRecords, MaxRecordsPerHost: maxPerHost, MaxPermissionAgeHours: 24,
		RobotsUserAgent: "Fetchmark-PackBuilder/1", CandidateSHA256: hex.EncodeToString(inputDigest[:]),
		Inputs: []indexpack.BuildInput{{
			Name: "CC-MAIN-2026-26 URL Index export", URI: "https://index.commoncrawl.org/CC-MAIN-2026-26-index",
			RetrievedAt: created.Add(-time.Hour).UTC().Format(time.RFC3339), SHA256: strings.Repeat("5", 64),
			RightsNotice: "Common Crawl terms; URL metadata only",
		}},
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func equalCounts(left, right map[string]uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range right {
		if left[key] != value {
			return false
		}
	}
	return true
}
