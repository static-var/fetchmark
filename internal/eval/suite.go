// Package eval provides deterministic query-suite validation and an opt-in
// live evaluator for Fetchmark's HTTP API. Unit tests never contact the web.
package eval

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// Intent is a stable top-level evaluation category.
type Intent string

const (
	IntentGeneral   Intent = "general"
	IntentFresh     Intent = "fresh"
	IntentLongTail  Intent = "long_tail"
	IntentDeveloper Intent = "developer"
	IntentResearch  Intent = "research"
	IntentKnowledge Intent = "knowledge"
)

var (
	caseIDPattern = regexp.MustCompile(`^(general|fresh|long_tail|developer|research|knowledge)-[0-9]{3}$`)
	tagPattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
)

// AllIntents returns the canonical order used by validation and reporting.
func AllIntents() []Intent {
	return []Intent{IntentGeneral, IntentFresh, IntentLongTail, IntentDeveloper, IntentResearch, IntentKnowledge}
}

// Case is one stable query fixture. ExpectedDomains is an optional relevance
// proxy, not a requirement that every good result come from those domains.
type Case struct {
	ID                 string   `json:"id"`
	Intent             Intent   `json:"intent"`
	Query              string   `json:"query"`
	Tags               []string `json:"tags"`
	MaxResults         int      `json:"max_results"`
	SearchDepth        string   `json:"search_depth"`
	Engines            []string `json:"engines,omitempty"`
	Categories         []string `json:"categories,omitempty"`
	Language           string   `json:"language,omitempty"`
	TimeRange          string   `json:"time_range,omitempty"`
	ExpectedDomains    []string `json:"expected_domains,omitempty"`
	FreshnessSensitive bool     `json:"freshness_sensitive,omitempty"`
}

// Suite is a validated ordered collection of cases.
type Suite struct {
	Cases []Case
}

// LoadSuite decodes and validates JSONL while preserving file order.
func LoadSuite(r io.Reader) (Suite, error) {
	if r == nil {
		return Suite{}, errors.New("eval: nil suite reader")
	}
	var suite Suite
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	for line := 1; scanner.Scan(); line++ {
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(raw))
		dec.DisallowUnknownFields()
		var c Case
		if err := dec.Decode(&c); err != nil {
			return Suite{}, fmt.Errorf("eval: line %d: %w", line, err)
		}
		var extra any
		if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
			if err == nil {
				return Suite{}, fmt.Errorf("eval: line %d: multiple JSON values", line)
			}
			return Suite{}, fmt.Errorf("eval: line %d: trailing JSON: %w", line, err)
		}
		suite.Cases = append(suite.Cases, c)
	}
	if err := scanner.Err(); err != nil {
		return Suite{}, fmt.Errorf("eval: read suite: %w", err)
	}
	if err := suite.Validate(); err != nil {
		return Suite{}, err
	}
	return suite, nil
}

// Validate enforces stable IDs and bounded request controls.
func (s Suite) Validate() error {
	if len(s.Cases) == 0 {
		return errors.New("eval: suite is empty")
	}
	if len(s.Cases) > 1000 {
		return fmt.Errorf("eval: suite has %d cases, maximum is 1000", len(s.Cases))
	}
	seen := make(map[string]struct{}, len(s.Cases))
	validIntents := make(map[Intent]struct{}, len(AllIntents()))
	for _, intent := range AllIntents() {
		validIntents[intent] = struct{}{}
	}
	for i, c := range s.Cases {
		position := i + 1
		if !caseIDPattern.MatchString(c.ID) {
			return fmt.Errorf("eval: case %d has invalid id %q", position, c.ID)
		}
		if _, ok := seen[c.ID]; ok {
			return fmt.Errorf("eval: duplicate case id %q", c.ID)
		}
		seen[c.ID] = struct{}{}
		if _, ok := validIntents[c.Intent]; !ok {
			return fmt.Errorf("eval: case %q has invalid intent %q", c.ID, c.Intent)
		}
		if !strings.HasPrefix(c.ID, string(c.Intent)+"-") {
			return fmt.Errorf("eval: case %q does not match intent %q", c.ID, c.Intent)
		}
		if strings.TrimSpace(c.Query) == "" {
			return fmt.Errorf("eval: case %q has empty query", c.ID)
		}
		if c.MaxResults < 1 || c.MaxResults > 50 {
			return fmt.Errorf("eval: case %q max_results must be 1..50", c.ID)
		}
		if c.SearchDepth != "basic" && c.SearchDepth != "advanced" {
			return fmt.Errorf("eval: case %q search_depth must be basic or advanced", c.ID)
		}
		if len(c.Tags) == 0 {
			return fmt.Errorf("eval: case %q requires at least one tag", c.ID)
		}
		tags := make(map[string]struct{}, len(c.Tags))
		for _, tag := range c.Tags {
			if !tagPattern.MatchString(tag) {
				return fmt.Errorf("eval: case %q has invalid tag %q", c.ID, tag)
			}
			if _, ok := tags[tag]; ok {
				return fmt.Errorf("eval: case %q repeats tag %q", c.ID, tag)
			}
			tags[tag] = struct{}{}
		}
		for _, domain := range c.ExpectedDomains {
			if !validExpectedDomain(domain) {
				return fmt.Errorf("eval: case %q has invalid expected domain %q", c.ID, domain)
			}
		}
	}
	return nil
}

func validExpectedDomain(domain string) bool {
	domain = strings.TrimSpace(domain)
	return domain != "" && domain == strings.ToLower(domain) && !strings.ContainsAny(domain, "/:@ ?#") && strings.Contains(domain, ".")
}

// IntentCounts returns a fresh count map for reporting and tests.
func (s Suite) IntentCounts() map[Intent]int {
	out := make(map[Intent]int, len(AllIntents()))
	for _, c := range s.Cases {
		out[c.Intent]++
	}
	return out
}
