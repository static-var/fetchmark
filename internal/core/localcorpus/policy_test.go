package localcorpus

import (
	"testing"
	"time"
)

func TestParseRetentionMode(t *testing.T) {
	for _, mode := range []RetentionMode{ModeDisabled, ModeEphemeral, ModePersonal, ModeCurated, ModeArchive} {
		got, err := ParseRetentionMode(string(mode))
		if err != nil || got != mode {
			t.Fatalf("ParseRetentionMode(%q) = %q, %v", mode, got, err)
		}
	}
	if _, err := ParseRetentionMode("forever-ish"); err == nil {
		t.Fatal("unknown retention mode was accepted")
	}
}

func TestParseSafetyClassification(t *testing.T) {
	for input, want := range map[string]SafetyClassification{
		"": SafetyUnclassified, " UNCLASSIFIED ": SafetyUnclassified, "safe": SafetySafe, "unsafe": SafetyUnsafe,
	} {
		got, err := ParseSafetyClassification(input)
		if err != nil || got != want {
			t.Fatalf("ParseSafetyClassification(%q)=%q err=%v, want %q", input, got, err, want)
		}
	}
	if _, err := ParseSafetyClassification("probably"); err == nil {
		t.Fatal("unknown safety classification was accepted")
	}
}

func TestPolicyAppliesModeSpecificAutomaticRetention(t *testing.T) {
	observedAt := time.Date(2026, time.July, 18, 10, 0, 0, 0, time.UTC)
	document := Document{
		URL: "https://example.com/policy", Body: "permitted", FetchedAt: observedAt,
		IndexingDisposition: DispositionPermitted,
	}
	for _, test := range []struct {
		name        string
		policy      Policy
		wantRetain  bool
		wantExpires *time.Time
	}{
		{name: "disabled", policy: Policy{Mode: ModeDisabled}},
		{name: "ephemeral", policy: Policy{Mode: ModeEphemeral}, wantRetain: true},
		{name: "personal", policy: Policy{Mode: ModePersonal, MaxAge: 24 * time.Hour}, wantRetain: true, wantExpires: timePointer(observedAt.Add(24 * time.Hour))},
		{name: "curated", policy: Policy{Mode: ModeCurated}},
		{name: "archive", policy: Policy{Mode: ModeArchive}, wantRetain: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, retain := test.policy.ApplyAutomatic(document)
			if retain != test.wantRetain {
				t.Fatalf("retain = %v, want %v", retain, test.wantRetain)
			}
			if !equalTimes(got.ExpiresAt, test.wantExpires) {
				t.Fatalf("expires = %v, want %v", got.ExpiresAt, test.wantExpires)
			}
		})
	}
}

func TestCuratedPolicyStillAppliesRevocations(t *testing.T) {
	document := Document{
		URL: "https://example.com/curated", FetchedAt: time.Now().UTC(),
		IndexingDisposition: DispositionNoIndexHeader,
	}
	got, retain := (Policy{Mode: ModeCurated}).ApplyAutomatic(document)
	if !retain || got.IndexingDisposition != DispositionNoIndexHeader {
		t.Fatalf("curated revocation = %+v, retain=%v", got, retain)
	}
}

func TestPolicyAppliesExplicitAdmissionOnlyInCuratedMode(t *testing.T) {
	document := Document{
		URL: "https://example.com/curated", FetchedAt: time.Now().UTC(),
		IndexingDisposition: DispositionPermitted,
	}
	expired := document.FetchedAt.Add(time.Hour)
	document.ExpiresAt = &expired

	for _, test := range []struct {
		mode       RetentionMode
		wantRetain bool
	}{
		{mode: ModeDisabled},
		{mode: ModeEphemeral},
		{mode: ModePersonal},
		{mode: ModeCurated, wantRetain: true},
		{mode: ModeArchive},
	} {
		t.Run(string(test.mode), func(t *testing.T) {
			got, retain := (Policy{Mode: test.mode}).ApplyExplicit(document)
			if retain != test.wantRetain {
				t.Fatalf("retain = %v, want %v", retain, test.wantRetain)
			}
			if retain && got.ExpiresAt != nil {
				t.Fatalf("curated explicit admission expires at %v", got.ExpiresAt)
			}
		})
	}
}

func TestExplicitPolicyStillAppliesRevocations(t *testing.T) {
	document := Document{
		URL: "https://example.com/curated", FetchedAt: time.Now().UTC(),
		IndexingDisposition: DispositionNoIndexHeader,
	}
	for _, mode := range []RetentionMode{ModeEphemeral, ModePersonal, ModeCurated, ModeArchive} {
		t.Run(string(mode), func(t *testing.T) {
			got, retain := (Policy{Mode: mode}).ApplyExplicit(document)
			if !retain || got.IndexingDisposition != DispositionNoIndexHeader || got.ExpiresAt != nil {
				t.Fatalf("explicit revocation = %+v, retain=%v", got, retain)
			}
		})
	}
}

func timePointer(value time.Time) *time.Time { return &value }

func equalTimes(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}
