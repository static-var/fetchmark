package eval

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

func TestWriteLabelUIIsSelfContainedBlindAndRunBound(t *testing.T) {
	records := labeledRecordsFixture()
	records[0].Results[0].Title = `One </script><script>alert("x")</script>`

	var output bytes.Buffer
	if err := WriteLabelUI(&output, records); err != nil {
		t.Fatalf("WriteLabelUI: %v", err)
	}
	html := output.String()
	for _, required := range []string{
		"<!doctype html>",
		"Content-Security-Policy",
		"Export draft",
		"Export completed labels",
		"Import draft",
		"data-label-payload=",
	} {
		if !strings.Contains(html, required) {
			t.Fatalf("label UI missing %q", required)
		}
	}
	if strings.Contains(html, `<script>alert("x")</script>`) {
		t.Fatal("untrusted title was embedded as executable HTML")
	}
	if strings.Contains(html, "searxng") || strings.Contains(html, "crossref") || strings.Contains(html, "wikipedia") {
		t.Fatal("provider identity leaked into the blind UI")
	}

	match := regexp.MustCompile(`data-label-payload="([A-Za-z0-9+/=]+)"`).FindStringSubmatch(html)
	if len(match) != 2 {
		t.Fatal("encoded label payload was not found")
	}
	raw, err := base64.StdEncoding.DecodeString(match[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var payload struct {
		SchemaVersion int                  `json:"schema_version"`
		RunID         string               `json:"run_id"`
		Rows          []labelTemplateEntry `json:"rows"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode payload JSON: %v", err)
	}
	if payload.SchemaVersion != 1 || payload.RunID != "run-a" || len(payload.Rows) != 3 {
		t.Fatalf("payload = %+v", payload)
	}
	if payload.Rows[0].URL != "https://one.example/" || payload.Rows[2].URL != "https://three.example/" {
		t.Fatalf("row order changed: %+v", payload.Rows)
	}
	if payload.Rows[0].Relevance != nil {
		t.Fatalf("new UI contained a completed grade: %+v", payload.Rows[0])
	}
	if strings.Contains(string(raw), `"sources"`) || strings.Contains(string(raw), `"provider"`) {
		t.Fatalf("blind payload leaked provenance: %s", raw)
	}
}

func TestWriteLabelUIRejectsInvalidInputs(t *testing.T) {
	if err := WriteLabelUI(nil, labeledRecordsFixture()); err == nil {
		t.Fatal("nil writer was accepted")
	}
	var output bytes.Buffer
	if err := WriteLabelUI(&output, nil); err == nil {
		t.Fatal("empty run was accepted")
	}
}

func TestWriteLabelUIUsesRovingRailAndAnnouncesResultChanges(t *testing.T) {
	var output bytes.Buffer
	if err := WriteLabelUI(&output, labeledRecordsFixture()); err != nil {
		t.Fatalf("WriteLabelUI: %v", err)
	}
	html := output.String()
	for _, required := range []string{
		`href="#workspace"`,
		`id="workspace" tabindex="-1"`,
		`id="result-announcement" aria-live="polite" aria-atomic="true"`,
		`button.tabIndex = index === current ? 0 : -1;`,
		`cell.tabIndex = index === current ? 0 : -1;`,
		`elements.announcement.textContent = "Result "`,
		`row.schema_version,`,
		`row.intent,`,
		`row.query,`,
		`row.rank`,
	} {
		if !strings.Contains(html, required) {
			t.Fatalf("label UI missing accessibility or binding behavior %q", required)
		}
	}
	if strings.Contains(html, `rows.map(row => [row.case_id, row.url])`) {
		t.Fatal("browser recovery remained bound to only case and URL")
	}
}
