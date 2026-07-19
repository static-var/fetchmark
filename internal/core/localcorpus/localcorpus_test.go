package localcorpus

import (
	"strings"
	"testing"
)

func TestEvaluateDispositionFailsClosedOnRobotsUncertainty(t *testing.T) {
	tests := []struct {
		name          string
		allowed       bool
		authoritative bool
		want          IndexingDisposition
	}{
		{name: "unknown policy", allowed: true, authoritative: false, want: DispositionUnknown},
		{name: "blocked", allowed: false, authoritative: true, want: DispositionRobotsBlocked},
		{name: "permitted", allowed: true, authoritative: true, want: DispositionPermitted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := EvaluateDisposition(test.allowed, test.authoritative, nil, []byte("<html></html>"), "Fetchmark/0.1"); got != test.want {
				t.Fatalf("disposition = %q, want %q", got, test.want)
			}
		})
	}
}

func TestEvaluateDispositionHonorsApplicableXRobotsTag(t *testing.T) {
	tests := []struct {
		name   string
		header []string
		want   IndexingDisposition
	}{
		{name: "unqualified", header: []string{"noindex, nofollow"}, want: DispositionNoIndexHeader},
		{name: "matching agent", header: []string{"Fetchmark: noindex"}, want: DispositionNoIndexHeader},
		{name: "wildcard", header: []string{"*: none"}, want: DispositionNoIndexHeader},
		{name: "unqualified noarchive", header: []string{"noarchive"}, want: DispositionNoArchiveHeader},
		{name: "matching agent noarchive", header: []string{"Fetchmark: noarchive"}, want: DispositionNoArchiveHeader},
		{name: "other agent", header: []string{"googlebot: noindex"}, want: DispositionPermitted},
		{name: "other agent noarchive", header: []string{"googlebot: noarchive"}, want: DispositionPermitted},
		{name: "scoped continuation", header: []string{"googlebot: noarchive, noindex"}, want: DispositionPermitted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := EvaluateDisposition(true, true, test.header, nil, "Fetchmark/0.1"); got != test.want {
				t.Fatalf("disposition = %q, want %q", got, test.want)
			}
		})
	}
}

func TestNoIndexRevokesEvenWhenRobotsStateIsUnknown(t *testing.T) {
	if got := EvaluateDisposition(true, false, []string{"noindex"}, nil, "Fetchmark/0.1"); got != DispositionNoIndexHeader {
		t.Fatalf("disposition = %q", got)
	}
	if got := EvaluateDisposition(true, false, nil, []byte(`<meta name="robots" content="noindex">`), "Fetchmark/0.1"); got != DispositionNoIndexMetadata {
		t.Fatalf("disposition = %q", got)
	}
}

func TestEvaluateIndexabilityIgnoresNoArchiveButNoIndexStillWins(t *testing.T) {
	if got := EvaluateIndexability([]string{"noarchive"}, []byte(`<meta name="robots" content="index">`), "Fetchmark/1"); got != DispositionPermitted {
		t.Fatalf("noarchive indexability = %q", got)
	}
	if got := EvaluateIndexability([]string{"noarchive"}, []byte(`<meta name="robots" content="noindex">`), "Fetchmark/1"); got != DispositionNoIndexMetadata {
		t.Fatalf("metadata noindex after header noarchive = %q", got)
	}
	if got := EvaluateDisposition(true, true, []string{"noarchive"}, []byte(`<meta name="robots" content="noindex">`), "Fetchmark/1"); got != DispositionNoIndexMetadata {
		t.Fatalf("combined disposition = %q", got)
	}
	disposition, values := EvaluateIndexabilityEvidence(
		[]string{"OtherBot: noindex"},
		[]byte(`<meta name="robots" content="index"><meta name="fetchmark" content="noindex"><meta name="otherbot" content="none">`),
		"Fetchmark/1",
	)
	if disposition != DispositionNoIndexMetadata || len(values) != 2 || values[0] != "index" || values[1] != "noindex" {
		t.Fatalf("indexability evidence = %q, %#v", disposition, values)
	}
}

func TestEvaluateDispositionHonorsRobotsMetadata(t *testing.T) {
	tests := []struct {
		name string
		html string
		want IndexingDisposition
	}{
		{name: "robots noindex", html: `<meta name="robots" content="max-snippet:50, NOINDEX">`, want: DispositionNoIndexMetadata},
		{name: "robots noarchive", html: `<meta name="robots" content="noarchive">`, want: DispositionNoArchiveMetadata},
		{name: "matching agent noarchive", html: `<meta name="fetchmark" content="noarchive">`, want: DispositionNoArchiveMetadata},
		{name: "other agent noarchive", html: `<meta name="googlebot" content="noarchive">`, want: DispositionPermitted},
		{name: "noindex wins over noarchive", html: `<meta name="robots" content="noarchive"><meta name="robots" content="noindex">`, want: DispositionNoIndexMetadata},
		{name: "robots none", html: `<meta content="none" name="ROBOTS">`, want: DispositionNoIndexMetadata},
		{name: "matching agent", html: `<meta name="fetchmark" content="noindex">`, want: DispositionNoIndexMetadata},
		{name: "other agent", html: `<meta name="googlebot" content="noindex">`, want: DispositionPermitted},
		{name: "substring is not directive", html: `<meta name="robots" content="not-noindexing">`, want: DispositionPermitted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := EvaluateDisposition(true, true, nil, []byte(test.html), "Fetchmark/0.1"); got != test.want {
				t.Fatalf("disposition = %q, want %q", got, test.want)
			}
		})
	}
}

func TestAllowsLinkFollowingHonorsApplicableHeaderAndMetadata(t *testing.T) {
	tests := []struct {
		name   string
		header []string
		html   string
		want   bool
	}{
		{name: "default", want: true},
		{name: "header", header: []string{"nofollow"}, want: false},
		{name: "header none", header: []string{"Fetchmark: none"}, want: false},
		{name: "other header agent", header: []string{"googlebot: nofollow"}, want: true},
		{name: "metadata", html: `<meta name="robots" content="nofollow">`, want: false},
		{name: "metadata none", html: `<meta name="fetchmark" content="none">`, want: false},
		{name: "other metadata agent", html: `<meta name="googlebot" content="nofollow">`, want: true},
		{name: "substring", html: `<meta name="robots" content="not-nofollowing">`, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := AllowsLinkFollowing(test.header, []byte(test.html), "Fetchmark/0.1"); got != test.want {
				t.Fatalf("allows = %t, want %t", got, test.want)
			}
		})
	}
}

func TestHTMLControlParsingFailsClosedOnMalformedTrees(t *testing.T) {
	malformed := []byte(strings.Repeat("<div>", 513) + `<meta name="robots" content="index,follow">` + strings.Repeat("</div>", 513))
	if got := EvaluateDisposition(true, true, nil, malformed, "Fetchmark/1"); got != DispositionUnknown {
		t.Fatalf("disposition = %q, want unknown", got)
	}
	if got := EvaluateIndexability(nil, malformed, "Fetchmark/1"); got != DispositionUnknown {
		t.Fatalf("indexability = %q, want unknown", got)
	}
	if AllowsLinkFollowing(nil, malformed, "Fetchmark/1") {
		t.Fatal("malformed metadata permitted link following")
	}
}
