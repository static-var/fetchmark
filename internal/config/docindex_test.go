package config

import "testing"

func TestDocIndexRequiresAbsoluteSnapshotWhenEnabled(t *testing.T) {
	t.Setenv("FM_DISCOVERY_ENABLED_SOURCES", "docindex")
	t.Setenv("FM_DISCOVERY_PRIMARY_SOURCE", "docindex")
	t.Setenv("FM_SEARXNG_URL", "")
	if _, err := Load(); err == nil || err.Error() != "FM_OFFICIAL_DOC_INDEX_FILE is required when the docindex discovery source is enabled" {
		t.Fatalf("Load error = %v", err)
	}
	t.Setenv("FM_OFFICIAL_DOC_INDEX_FILE", "relative/official-docs.json")
	if _, err := Load(); err == nil || err.Error() != "FM_OFFICIAL_DOC_INDEX_FILE must be absolute when set" {
		t.Fatalf("Load relative error = %v", err)
	}
}
