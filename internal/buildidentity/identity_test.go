package buildidentity

import (
	"strings"
	"testing"
)

func TestParseCanonicalizesSHA256(t *testing.T) {
	want := strings.Repeat("a1", 32)
	got, ok := Parse(strings.ToUpper(want))
	if !ok || got != want {
		t.Fatalf("Parse() = %q, %v; want %q, true", got, ok, want)
	}
	for _, raw := range []string{"", "abc", strings.Repeat("g", 64), strings.Repeat("a", 65)} {
		if got, ok := Parse(raw); ok || got != "" {
			t.Fatalf("Parse(%q) = %q, %v; want empty, false", raw, got, ok)
		}
	}
}

func TestCurrentExecutableSHA256IsCanonical(t *testing.T) {
	got, err := CurrentExecutableSHA256()
	if err != nil {
		t.Fatalf("CurrentExecutableSHA256: %v", err)
	}
	if canonical, ok := Parse(got); !ok || canonical != got {
		t.Fatalf("CurrentExecutableSHA256() = %q, want canonical SHA-256", got)
	}
}
