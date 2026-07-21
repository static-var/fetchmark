package indexpackadmission

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf16"
)

func TestEvaluateIndexingProducesDeterministicBodyFreeEvidence(t *testing.T) {
	observed := time.Date(2026, 7, 19, 12, 0, 0, 999, time.FixedZone("offset", 2*60*60))
	input := IndexingInput{
		Status: 200, FinalURL: "https://example.org/docs/page", ContentType: "text/html; charset=utf-8",
		XRobotsTag: []string{"noarchive"}, Representation: []byte(`<html><head><meta name="robots" content="index"></head><body>private body</body></html>`),
		Complete:  true,
		UserAgent: "FetchmarkPackEvidence/1 (+mailto:operator@example.org)", ObservedAt: observed, Validity: 12 * time.Hour,
	}
	observation, evidence, err := EvaluateIndexing(input)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	headerDigest := sha256.Sum256(raw)
	bodyDigest := sha256.Sum256(input.Representation)
	if observation.Outcome != "indexable" || observation.CheckedAt != "2026-07-19T10:00:00Z" || observation.ValidUntil != "2026-07-19T22:00:00Z" ||
		observation.HeadersSHA256 != hex.EncodeToString(headerDigest[:]) || observation.RepresentationSHA256 != hex.EncodeToString(bodyDigest[:]) ||
		evidence.Disposition != "permitted" || len(evidence.MetadataRobots) != 1 || evidence.MetadataRobots[0] != "index" || strings.Contains(string(raw), "private body") {
		t.Fatalf("observation=%#v evidence=%s", observation, raw)
	}
	again, againEvidence, err := EvaluateIndexing(input)
	if err != nil || again != observation || !equalResponseEvidence(againEvidence, evidence) {
		t.Fatalf("non-deterministic result observation=%#v evidence=%#v err=%v", again, againEvidence, err)
	}
}

func TestValidateDerivedIndexableEvidenceReplaysRetainedDirectives(t *testing.T) {
	input := IndexingInput{
		Status: 200, FinalURL: "https://example.org/page", ContentType: "text/html; charset=utf-8",
		XRobotsTag: []string{"index"}, Representation: []byte(`<meta name="robots" content="index">`),
		Complete: true, UserAgent: "FetchmarkBot/1", ObservedAt: time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC), Validity: time.Hour,
	}
	observation, evidence, err := EvaluateIndexing(input)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := ValidateDerivedIndexableEvidence(evidence, input.UserAgent)
	if err != nil || digest != observation.HeadersSHA256 {
		t.Fatalf("digest=%q observation=%q error=%v", digest, observation.HeadersSHA256, err)
	}
	evidence.XRobotsTag = []string{"noindex"}
	if _, err := ValidateDerivedIndexableEvidence(evidence, input.UserAgent); err == nil {
		t.Fatal("retained header noindex was accepted")
	}
	evidence.XRobotsTag = nil
	evidence.MetadataRobots = []string{"none"}
	if _, err := ValidateDerivedIndexableEvidence(evidence, input.UserAgent); err == nil {
		t.Fatal("retained metadata noindex was accepted")
	}
}

func TestEvaluateIndexingHonorsApplicableNoIndexAcrossChannels(t *testing.T) {
	base := IndexingInput{
		Status: 200, FinalURL: "https://example.org/", ContentType: "text/html", Representation: []byte(`<p>ok</p>`),
		Complete:  true,
		UserAgent: "FetchmarkPackEvidence/1", ObservedAt: time.Now(), Validity: time.Hour,
	}
	tests := []struct {
		name        string
		header      []string
		body        string
		want        string
		disposition string
	}{
		{name: "unscoped header", header: []string{"noindex"}, want: "noindex", disposition: "noindex_header"},
		{name: "matching header", header: []string{"FetchmarkPackEvidence: noindex"}, want: "noindex", disposition: "noindex_header"},
		{name: "other agent", header: []string{"OtherBot: noindex"}, want: "indexable", disposition: "permitted"},
		{name: "metadata", body: `<meta name="robots" content="none">`, want: "noindex", disposition: "noindex_metadata"},
		{name: "noarchive only", header: []string{"noarchive"}, want: "indexable", disposition: "permitted"},
		{name: "metadata wins after noarchive", header: []string{"noarchive"}, body: `<meta name="robots" content="noindex">`, want: "noindex", disposition: "noindex_metadata"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := base
			input.XRobotsTag = test.header
			if test.body != "" {
				input.Representation = []byte(test.body)
			}
			observation, evidence, err := EvaluateIndexing(input)
			if err != nil || observation.Outcome != test.want || evidence.Disposition != test.disposition {
				t.Fatalf("observation=%#v evidence=%#v error=%v", observation, evidence, err)
			}
		})
	}
}

func TestEvaluateIndexingRejectsUnboundedOrAmbiguousInput(t *testing.T) {
	valid := IndexingInput{
		Status: 200, FinalURL: "https://example.org/", ContentType: "text/html", Representation: []byte("ok"),
		Complete:  true,
		UserAgent: "FetchmarkPackEvidence/1", ObservedAt: time.Now(), Validity: time.Hour,
	}
	tests := map[string]func(*IndexingInput){
		"incomplete":   func(input *IndexingInput) { input.Complete = false },
		"status":       func(input *IndexingInput) { input.Status = 0 },
		"URL":          func(input *IndexingInput) { input.FinalURL = "http://127.0.0.1/" },
		"content type": func(input *IndexingInput) { input.ContentType = "text/html\nX: y" },
		"header count": func(input *IndexingInput) { input.XRobotsTag = make([]string, MaxXRobotsTagValues+1) },
		"header bytes": func(input *IndexingInput) { input.XRobotsTag = []string{strings.Repeat("x", MaxXRobotsTagBytes+1)} },
		"metadata count": func(input *IndexingInput) {
			input.Representation = []byte(strings.Repeat(`<meta name="robots" content="index">`, MaxMetadataRobotsValues+1))
		},
		"metadata bytes": func(input *IndexingInput) {
			input.Representation = []byte(`<meta name="robots" content="` + strings.Repeat("x", MaxMetadataRobotsBytes+1) + `">`)
		},
		"representation": func(input *IndexingInput) { input.Representation = make([]byte, MaxRepresentationBytes+1) },
		"user agent":     func(input *IndexingInput) { input.UserAgent = "" },
		"time":           func(input *IndexingInput) { input.ObservedAt = time.Time{} },
		"validity":       func(input *IndexingInput) { input.Validity = 25 * time.Hour },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			input := valid
			mutate(&input)
			if _, _, err := EvaluateIndexing(input); err == nil {
				t.Fatal("invalid indexing input accepted")
			}
		})
	}
}

func TestEvaluateIndexingFailsClosedOnHTMLParserOrCharsetUncertainty(t *testing.T) {
	base := IndexingInput{
		Status: 200, FinalURL: "https://example.org/", ContentType: "text/html", Representation: []byte("<p>ok</p>"),
		Complete: true, UserAgent: "FetchmarkPackEvidence/1", ObservedAt: time.Now(), Validity: time.Hour,
	}
	t.Run("parser depth error", func(t *testing.T) {
		input := base
		input.Representation = []byte(strings.Repeat("<div>", 513) + `<meta name="robots" content="noindex">` + strings.Repeat("</div>", 513))
		if _, _, err := EvaluateIndexing(input); err == nil || !strings.Contains(err.Error(), "parse HTML metadata") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("unsupported charset", func(t *testing.T) {
		input := base
		input.ContentType = "text/html; charset=x-fetchmark-unknown"
		if _, _, err := EvaluateIndexing(input); err == nil || !strings.Contains(err.Error(), "decode HTML representation") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("UTF-16 BOM noindex", func(t *testing.T) {
		input := base
		input.Representation = utf16LE(`<meta name="robots" content="noindex">`)
		observation, evidence, err := EvaluateIndexing(input)
		if err != nil || observation.Outcome != "noindex" || evidence.Disposition != "noindex_metadata" {
			t.Fatalf("observation=%#v evidence=%#v error=%v", observation, evidence, err)
		}
	})
	t.Run("truncated UTF-16", func(t *testing.T) {
		input := base
		input.Representation = append(utf16LE(`<meta name="robots" content="noindex">`), 0)
		if _, _, err := EvaluateIndexing(input); err == nil || !strings.Contains(err.Error(), "decode HTML representation") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("declared non UTF-8 noindex", func(t *testing.T) {
		input := base
		input.ContentType = "text/html; charset=windows-1252"
		input.Representation = append([]byte(`<title>caf`), append([]byte{0xe9}, []byte(`</title><meta name="robots" content="noindex">`)...)...)
		observation, evidence, err := EvaluateIndexing(input)
		if err != nil || observation.Outcome != "noindex" || evidence.Disposition != "noindex_metadata" {
			t.Fatalf("observation=%#v evidence=%#v error=%v", observation, evidence, err)
		}
	})
	t.Run("malformed declared UTF-8", func(t *testing.T) {
		input := base
		input.ContentType = "text/html; charset=utf-8"
		input.Representation = []byte{'<', 'p', '>', 0xff, '<', '/', 'p', '>'}
		if _, _, err := EvaluateIndexing(input); err == nil || !strings.Contains(err.Error(), "malformed byte sequences") {
			t.Fatalf("error = %v", err)
		}
	})
}

func utf16LE(value string) []byte {
	raw := []byte{0xff, 0xfe}
	for _, unit := range utf16.Encode([]rune(value)) {
		raw = append(raw, byte(unit), byte(unit>>8))
	}
	return raw
}

func equalResponseEvidence(left, right ResponseEvidence) bool {
	leftRaw, _ := json.Marshal(left)
	rightRaw, _ := json.Marshal(right)
	return string(leftRaw) == string(rightRaw)
}
