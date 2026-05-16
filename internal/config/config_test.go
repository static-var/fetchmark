package config

import (
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("FM_API_KEYS", "k1, k2 ,")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.ListenAddr != ":8080" {
		t.Errorf("ListenAddr default = %q", c.ListenAddr)
	}
	if len(c.APIKeys) != 2 || c.APIKeys[0] != "k1" || c.APIKeys[1] != "k2" {
		t.Errorf("APIKeys cleaned = %#v", c.APIKeys)
	}
	if c.DashboardEnabled() {
		t.Error("dashboard should be disabled when creds unset")
	}
	if !c.RespectRobots {
		t.Error("RespectRobots should default to true")
	}
	if c.SummarizeMaxTokensCap != 4096 {
		t.Errorf("SummarizeMaxTokensCap default = %d", c.SummarizeMaxTokensCap)
	}
	if c.SummarizeMaxTimeout.String() != "2m0s" {
		t.Errorf("SummarizeMaxTimeout default = %v", c.SummarizeMaxTimeout)
	}
	if c.SummarizeMaxInstructionsLen != 4000 {
		t.Errorf("SummarizeMaxInstructionsLen default = %d", c.SummarizeMaxInstructionsLen)
	}
	if c.SummarizeAllowModelOverride {
		t.Error("SummarizeAllowModelOverride should default to false")
	}
	if c.SummarizeAllowProviderOverride {
		t.Error("SummarizeAllowProviderOverride should default to false")
	}
	if c.SummarizeAllowThinkingOverride {
		t.Error("SummarizeAllowThinkingOverride should default to false")
	}
}

func TestLoad_InvalidSummarizeCaps(t *testing.T) {
	cases := []struct {
		name string
		env  string
		val  string
		want string
	}{
		{"max tokens", "FM_SUMMARIZE_MAX_TOKENS_CAP", "0", "FM_SUMMARIZE_MAX_TOKENS_CAP must be > 0"},
		{"timeout", "FM_SUMMARIZE_MAX_TIMEOUT", "0s", "FM_SUMMARIZE_MAX_TIMEOUT must be > 0"},
		{"instructions", "FM_SUMMARIZE_MAX_INSTRUCTIONS_LEN", "-1", "FM_SUMMARIZE_MAX_INSTRUCTIONS_LEN must be >= 0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.env, tc.val)
			_, err := Load()
			if err == nil || err.Error() != tc.want {
				t.Fatalf("Load error = %v want %q", err, tc.want)
			}
		})
	}
}

func TestLoad_InvalidResults(t *testing.T) {
	t.Setenv("FM_MAX_RESULTS", "100")
	t.Setenv("FM_RESULTS_CAP", "50")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when MaxResults > ResultsCap")
	}
}

func TestDashboardEnabled(t *testing.T) {
	t.Setenv("FM_DASHBOARD_USER", "admin")
	t.Setenv("FM_DASHBOARD_PASSWORD", "secret")
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !c.DashboardEnabled() {
		t.Error("dashboard should be enabled when creds set")
	}
}

func TestLoad_SearxngCooldownDefault(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.SearxngCooldown.String() != "30s" {
		t.Fatalf("SearxngCooldown default = %v", c.SearxngCooldown)
	}
}

func TestLoad_SearxngCooldownRejectsZero(t *testing.T) {
	t.Setenv("FM_SEARXNG_COOLDOWN", "0s")
	if _, err := Load(); err == nil {
		t.Fatal("expected error when FM_SEARXNG_COOLDOWN=0")
	}
}
