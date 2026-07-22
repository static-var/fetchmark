package config

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func TestComposeFilesExposeEveryRuntimeVariable(t *testing.T) {
	configSource, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	tagPattern := regexp.MustCompile(`env:"(FM_[A-Z0-9_]+)"`)
	var variables []string
	for _, match := range tagPattern.FindAllSubmatch(configSource, -1) {
		variables = append(variables, string(match[1]))
	}
	sort.Strings(variables)

	for _, path := range []string{"../../deploy/docker-compose.yml", "../../deploy/docker-compose.external.yml"} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(body)
		var missing []string
		for _, variable := range variables {
			if !strings.Contains(text, variable+":") {
				missing = append(missing, variable)
			}
		}
		if len(missing) > 0 {
			t.Errorf("%s missing runtime variables: %s", path, strings.Join(missing, ", "))
		}
	}
}

func TestComposeMountsOfficialDocIndexDirectoryReadOnly(t *testing.T) {
	const mount = `${FM_OFFICIAL_DOC_INDEX_HOST_DIR:-./official-docs}:/etc/fetchmark/official-docs:ro`
	for _, path := range []string{"../../deploy/docker-compose.yml", "../../deploy/docker-compose.external.yml"} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), mount) {
			t.Errorf("%s missing read-only official-doc-index mount", path)
		}
	}
	example, err := os.ReadFile("../../.env.example")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(example), "FM_OFFICIAL_DOC_INDEX_HOST_DIR=") || !strings.Contains(string(example), "FM_OFFICIAL_DOC_INDEX_FILE=") {
		t.Fatal(".env.example does not document both host mount and container snapshot path")
	}
}

func TestComposeKeepsAutomaticBrowserRenderingOptIn(t *testing.T) {
	body, err := os.ReadFile("../../deploy/docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `FM_RENDERER_AUTO: "${FM_RENDERER_AUTO:-false}"`) {
		t.Fatal("docker-compose.yml must default FM_RENDERER_AUTO to false")
	}
}

func TestComposeDocumentsCoupledRendererDeadlines(t *testing.T) {
	compose, err := os.ReadFile("../../deploy/docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	environment, err := os.ReadFile("../../.env.example")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"FM_RENDERER_TIMEOUT=20s",
		"SCRAPLING_RENDER_TIMEOUT_SECONDS=19",
		"Change both renderer deadlines together",
	} {
		if !strings.Contains(string(environment), required) {
			t.Fatalf(".env.example missing %q", required)
		}
	}
	if !strings.Contains(string(compose), "must remain below FM_RENDERER_TIMEOUT") {
		t.Fatal("docker-compose.yml must document the sidecar timeout relationship")
	}
}
