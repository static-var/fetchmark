package retryafter

import (
	"net/http"
	"testing"
	"time"
)

func TestParseBoundsDeltaSecondsBeforeDurationConversion(t *testing.T) {
	maximum := 24 * time.Hour
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "empty", value: "", want: 0},
		{name: "zero", value: "0", want: 0},
		{name: "seconds", value: " 60 ", want: time.Minute},
		{name: "maximum", value: "86400", want: maximum},
		{name: "above maximum", value: "86401", want: maximum},
		{name: "max int64", value: "9223372036854775807", want: maximum},
		{name: "integer overflow", value: "999999999999999999999999999999999999", want: maximum},
		{name: "negative is invalid", value: "-1", want: 0},
		{name: "explicit plus is invalid", value: "+1", want: 0},
		{name: "non decimal is invalid", value: "1.5", want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Parse(test.value, now, maximum); got != test.want {
				t.Fatalf("Parse(%q) = %v, want %v", test.value, got, test.want)
			}
		})
	}
}

func TestParseHonorsSubsecondMaximumWithoutRoundingUp(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	maximum := 1500 * time.Millisecond
	if got := Parse("1", now, maximum); got != time.Second {
		t.Fatalf("one second = %v", got)
	}
	if got := Parse("2", now, maximum); got != maximum {
		t.Fatalf("bounded two seconds = %v", got)
	}
}

func TestParseBoundsHTTPDateAndRejectsInvalidMaximum(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	if got := Parse(now.Add(30*time.Minute).Format(http.TimeFormat), now, time.Hour); got != 30*time.Minute {
		t.Fatalf("future date = %v", got)
	}
	if got := Parse(now.Add(2*time.Hour).Format(http.TimeFormat), now, time.Hour); got != time.Hour {
		t.Fatalf("bounded date = %v", got)
	}
	for _, value := range []string{now.Format(http.TimeFormat), now.Add(-time.Second).Format(http.TimeFormat), "invalid"} {
		if got := Parse(value, now, time.Hour); got != 0 {
			t.Fatalf("Parse(%q) = %v, want 0", value, got)
		}
	}
	if got := Parse("60", now, 0); got != 0 {
		t.Fatalf("zero maximum = %v", got)
	}
}
