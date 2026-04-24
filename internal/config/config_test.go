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
