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
