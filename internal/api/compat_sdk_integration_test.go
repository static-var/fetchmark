//go:build compat_sdk

package api

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"net/http/httptest"

	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/model"
)

func TestOfficialPythonSDKCompatibility(t *testing.T) {
	python := os.Getenv("FETCHMARK_COMPAT_SDK_PYTHON")
	if python == "" {
		t.Skip("set FETCHMARK_COMPAT_SDK_PYTHON to a Python environment with the pinned official SDKs")
	}
	if _, err := os.Stat(python); err != nil {
		t.Fatalf("compatibility SDK Python: %v", err)
	}

	pipe := &fakePipeline{results: []model.SearchResult{{
		URL:      "https://example.com/open-discovery",
		Title:    "Open discovery",
		Snippet:  "A deterministic compatibility result.",
		Markdown: "# Open discovery\n\nA deterministic compatibility result.",
		Content: &model.Content{
			MainText: "Open discovery\nA deterministic compatibility result.",
			Markdown: "# Open discovery\n\nA deterministic compatibility result.",
		},
	}}}
	server := httptest.NewServer(compatibilityRouter(config.Config{
		APIKeys:               []string{"sdk-test-key"},
		MaxResults:            10,
		ResultsCap:            50,
		RespectRobots:         true,
		MaxRequestOutputBytes: 1 << 20,
	}, pipe))
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	versionTest := exec.CommandContext(ctx, python, filepath.Join("testdata", "compat_sdk_smoke_test.py"))
	if output, err := versionTest.CombinedOutput(); err != nil {
		t.Fatalf("compatibility SDK version regression failed: %v\n%s", err, output)
	}
	script := filepath.Join("testdata", "compat_sdk_smoke.py")
	command := exec.CommandContext(ctx, python, script, server.URL, "sdk-test-key")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("official SDK compatibility smoke failed: %v\n%s", err, output)
	}
	t.Logf("%s", output)
}
