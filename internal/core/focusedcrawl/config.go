// Package focusedcrawl defines the policy and scheduler model for Fetchmark's
// optional, separately operated focused-ingestion worker.
package focusedcrawl

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/mail"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
)

var ErrInvalidConfig = errors.New("focused crawl: invalid config")

const (
	configVersion      = 3
	maxConfigBytes     = 4 << 20
	maxFrontierEntries = 1_000_000
	maxJobs            = 1_000
	maxURLsPerRun      = 10_000
	maxDuration        = 365 * 24 * time.Hour
	maxPolicyItems     = 256
	maxPathPrefixBytes = 2048
	maxWebURLBytes     = 2048
	maxAdmissionBytes  = 900 << 10

	maxContactURIBytes                  = 2048
	maxDiscoverySources                 = 100_000
	maxDiscoveryRelationships           = 1_000_000
	maxLinkEdges                        = 1_000_000
	maxOutboundLinksPerPage             = 64
	maxLinkSnapshotPagesPerRun          = 1_000
	maxRootDiscoverySourcesPerJob       = 64
	maxSourcePollsPerRun                = 64
	minSourcePollInterval               = 5 * time.Minute
	maxSourcePollInterval               = 30 * 24 * time.Hour
	minSourceErrorRecheckInterval       = 5 * time.Minute
	maxSourceErrorRecheckInterval       = 24 * time.Hour
	maxSourceCompressedBytes      int64 = 4 << 20
	maxSourceDecompressedBytes    int64 = 16 << 20
	maxSourceEntries                    = 10_000
	maxSourceChildSources               = 256
	maxSourceDepth                      = 4

	sitemapProtocolMaxEntries                 = 50_000
	sitemapProtocolMaxDecompressedBytes int64 = 50 << 20
)

var jobIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(raw []byte) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return errors.New("duration must be a string")
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

type Config struct {
	Version                     int    `json:"version"`
	ContactURI                  string `json:"contact_uri"`
	MaxFrontierURLs             int    `json:"max_frontier_urls"`
	MaxDiscoverySources         int    `json:"max_discovery_sources"`
	MaxDiscoveryPageMemberships int    `json:"max_discovery_page_memberships"`
	MaxDiscoverySourceEdges     int    `json:"max_discovery_source_edges"`
	MaxLinkEdges                int    `json:"max_link_edges"`
	Jobs                        []Job  `json:"jobs"`

	sourceSeeds []SourceSeed
	rootSources map[string]RootSource
}

// SourceKind selects the discovery-document parser for a configured source.
type SourceKind string

const (
	SourceKindAuto    SourceKind = "auto"
	SourceKindSitemap SourceKind = "sitemap"
	SourceKindFeed    SourceKind = "feed"
)

// DiscoverySource is the bounded, canonicalized configuration for one root
// sitemap, RSS, or Atom source.
type DiscoverySource struct {
	URL                       string     `json:"url"`
	Kind                      SourceKind `json:"kind"`
	AllowedSourcePathPrefixes []string   `json:"allowed_source_path_prefixes"`
	PollInterval              Duration   `json:"poll_interval"`
	ErrorRecheckInterval      Duration   `json:"error_recheck_interval"`
	MaxCompressedBytes        int64      `json:"max_compressed_bytes"`
	MaxDecompressedBytes      int64      `json:"max_decompressed_bytes"`
	MaxEntries                int        `json:"max_entries"`
	MaxChildSources           int        `json:"max_child_sources"`
	MaxDepth                  int        `json:"max_depth"`
}

// SourceSeed is the immutable identity needed to synchronize configured root
// sources into the future persistent source frontier.
type SourceSeed struct {
	Key   string
	JobID string
	URL   string
	Kind  SourceKind
}

// RootSource joins a configured source identity to its polling policy.
type RootSource struct {
	Seed   SourceSeed
	Config DiscoverySource
}

// LinkExpansion enables one-hop discovery from independently configured or
// source-owned pages. Link-derived pages are admitted and refreshed, but do
// not recursively publish another link snapshot unless they also gain an
// explicit seed or live sitemap/feed ownership.
type LinkExpansion struct {
	MaxLinksPerPage int `json:"max_links_per_page"`
	MaxPagesPerRun  int `json:"max_pages_per_run"`
}

type Job struct {
	ID                       string            `json:"id"`
	SeedURLs                 []string          `json:"seeds"`
	AllowedDomains           []string          `json:"allowed_domains"`
	AllowedPathPrefixes      []string          `json:"allowed_path_prefixes"`
	DeniedPathPrefixes       []string          `json:"denied_path_prefixes,omitempty"`
	DeniedURLs               []string          `json:"denied_urls,omitempty"`
	IncludeSubdomains        bool              `json:"include_subdomains,omitempty"`
	MaxURLsPerRun            int               `json:"max_urls_per_run"`
	MaxSourcePollsPerRun     int               `json:"max_source_polls_per_run"`
	RefreshInterval          Duration          `json:"refresh_interval"`
	RejectionRecheckInterval Duration          `json:"rejection_recheck_interval"`
	MinHostInterval          Duration          `json:"min_host_interval"`
	MaxAttempts              int               `json:"max_attempts"`
	DiscoverySources         []DiscoverySource `json:"discovery_sources,omitempty"`
	LinkExpansion            *LinkExpansion    `json:"link_expansion,omitempty"`

	seeds         []Seed
	deniedURLSet  map[string]struct{}
	allowedDomain map[string]struct{}
}

type Seed struct {
	Key   string
	JobID string
	URL   string
}

func DecodeConfig(reader io.Reader) (Config, error) {
	raw, err := io.ReadAll(io.LimitReader(reader, maxConfigBytes+1))
	if err != nil {
		return Config{}, invalid(fmt.Errorf("read configuration: %w", err))
	}
	if len(raw) > maxConfigBytes {
		return Config{}, invalid(errors.New("configuration exceeds 4 MiB"))
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, invalid(err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Config{}, invalid(errors.New("configuration must contain exactly one JSON value"))
	}
	if err := config.validate(); err != nil {
		return Config{}, invalid(err)
	}
	return config, nil
}

func (c *Config) validate() error {
	c.sourceSeeds = nil
	c.rootSources = make(map[string]RootSource)
	if c.Version != configVersion {
		return fmt.Errorf("version must be %d", configVersion)
	}
	contactURI, err := canonicalContactURI(c.ContactURI)
	if err != nil {
		return fmt.Errorf("contact_uri: %w", err)
	}
	c.ContactURI = contactURI
	if c.MaxFrontierURLs < 1 || c.MaxFrontierURLs > maxFrontierEntries {
		return fmt.Errorf("max_frontier_urls must be 1..%d", maxFrontierEntries)
	}
	if c.MaxDiscoverySources < 0 || c.MaxDiscoverySources > maxDiscoverySources {
		return fmt.Errorf("max_discovery_sources must be 0..%d", maxDiscoverySources)
	}
	if c.MaxDiscoveryPageMemberships < 0 || c.MaxDiscoveryPageMemberships > maxDiscoveryRelationships {
		return fmt.Errorf("max_discovery_page_memberships must be 0..%d", maxDiscoveryRelationships)
	}
	if c.MaxDiscoverySourceEdges < 0 || c.MaxDiscoverySourceEdges > maxDiscoveryRelationships {
		return fmt.Errorf("max_discovery_source_edges must be 0..%d", maxDiscoveryRelationships)
	}
	if c.MaxLinkEdges < 0 || c.MaxLinkEdges > maxLinkEdges {
		return fmt.Errorf("max_link_edges must be 0..%d", maxLinkEdges)
	}
	if len(c.Jobs) < 1 || len(c.Jobs) > maxJobs {
		return fmt.Errorf("jobs must contain 1..%d entries", maxJobs)
	}
	jobIDs := make(map[string]struct{}, len(c.Jobs))
	seedURLs := make(map[string]string)
	sourceURLs := make(map[string]string)
	seedCount := 0
	sourceCount := 0
	linkExpansionJobs := 0
	for index := range c.Jobs {
		job := &c.Jobs[index]
		if !jobIDPattern.MatchString(job.ID) {
			return fmt.Errorf("job %d has invalid id", index)
		}
		if _, duplicate := jobIDs[job.ID]; duplicate {
			return fmt.Errorf("duplicate job id %q", job.ID)
		}
		jobIDs[job.ID] = struct{}{}
		if err := job.validate(); err != nil {
			return fmt.Errorf("job %q: %w", job.ID, err)
		}
		if job.LinkExpansion != nil {
			linkExpansionJobs++
		}
		if len(job.SeedURLs) == 0 && len(job.DiscoverySources) == 0 {
			return fmt.Errorf("job %q: at least one seed or discovery source is required", job.ID)
		}
		for _, seed := range job.seeds {
			if previous, duplicate := seedURLs[seed.URL]; duplicate {
				return fmt.Errorf("seed %q is duplicated across jobs %q and %q", seed.URL, previous, job.ID)
			}
			seedURLs[seed.URL] = job.ID
			seedCount++
		}
		for _, source := range job.DiscoverySources {
			if previous, duplicate := sourceURLs[source.URL]; duplicate {
				return fmt.Errorf("discovery source %q is duplicated across jobs %q and %q", source.URL, previous, job.ID)
			}
			sourceURLs[source.URL] = job.ID
			sourceCount++
			seed := SourceSeed{
				Key:   sourceConfigKey(job.ID, source.URL),
				JobID: job.ID,
				URL:   source.URL,
				Kind:  source.Kind,
			}
			c.sourceSeeds = append(c.sourceSeeds, seed)
			c.rootSources[seed.Key] = RootSource{Seed: seed, Config: cloneDiscoverySource(source)}
		}
	}
	if seedCount > c.MaxFrontierURLs {
		return errors.New("configured seeds exceed max_frontier_urls")
	}
	if sourceCount > c.MaxDiscoverySources {
		return errors.New("configured discovery sources exceed max_discovery_sources")
	}
	if sourceCount > 0 && (c.MaxDiscoveryPageMemberships == 0 || c.MaxDiscoverySourceEdges == 0) {
		return errors.New("discovery relationship caps must be positive when discovery sources are configured")
	}
	if linkExpansionJobs > 0 && c.MaxLinkEdges == 0 {
		return errors.New("max_link_edges must be positive when link expansion is configured")
	}
	return nil
}

func (j *Job) validate() error {
	j.seeds = nil
	if len(j.AllowedDomains) == 0 {
		return errors.New("allowed_domains are required")
	}
	j.allowedDomain = make(map[string]struct{}, len(j.AllowedDomains))
	for index, raw := range j.AllowedDomains {
		domain := strings.ToLower(strings.TrimSpace(raw))
		if domain == "" || strings.ContainsAny(domain, "*:/@") || net.ParseIP(domain) != nil || !validASCIIDNSName(domain) {
			return fmt.Errorf("allowed_domains[%d] must be an explicit DNS hostname", index)
		}
		if parsed, err := url.Parse("https://" + domain); err != nil || parsed.Hostname() != domain {
			return fmt.Errorf("allowed_domains[%d] is invalid", index)
		}
		j.AllowedDomains[index] = domain
		j.allowedDomain[domain] = struct{}{}
	}
	if err := validatePathPrefixes("allowed_path_prefixes", j.AllowedPathPrefixes, true); err != nil {
		return err
	}
	if err := validatePathPrefixes("denied_path_prefixes", j.DeniedPathPrefixes, false); err != nil {
		return err
	}
	if j.MaxURLsPerRun < 1 || j.MaxURLsPerRun > maxURLsPerRun {
		return fmt.Errorf("max_urls_per_run must be 1..%d", maxURLsPerRun)
	}
	if len(j.DiscoverySources) == 0 {
		if j.MaxSourcePollsPerRun != 0 {
			return errors.New("max_source_polls_per_run must be 0 when discovery_sources is empty")
		}
	} else if j.MaxSourcePollsPerRun < 1 || j.MaxSourcePollsPerRun > maxSourcePollsPerRun {
		return fmt.Errorf("max_source_polls_per_run must be 1..%d when discovery_sources is configured", maxSourcePollsPerRun)
	}
	if j.RefreshInterval.Duration < 5*time.Minute || j.RefreshInterval.Duration > maxDuration {
		return errors.New("refresh_interval must be between 5m and 8760h")
	}
	if j.RejectionRecheckInterval.Duration < 5*time.Minute || j.RejectionRecheckInterval.Duration > maxDuration {
		return errors.New("rejection_recheck_interval must be between 5m and 8760h")
	}
	if j.MinHostInterval.Duration < time.Second || j.MinHostInterval.Duration > time.Hour {
		return errors.New("min_host_interval must be between 1s and 1h")
	}
	if j.MaxAttempts < 1 || j.MaxAttempts > 20 {
		return errors.New("max_attempts must be 1..20")
	}
	if j.LinkExpansion != nil {
		if j.LinkExpansion.MaxLinksPerPage < 1 || j.LinkExpansion.MaxLinksPerPage > maxOutboundLinksPerPage {
			return fmt.Errorf("link_expansion.max_links_per_page must be 1..%d", maxOutboundLinksPerPage)
		}
		maxPages := min(j.MaxURLsPerRun, maxLinkSnapshotPagesPerRun)
		if j.LinkExpansion.MaxPagesPerRun < 1 || j.LinkExpansion.MaxPagesPerRun > maxPages {
			return fmt.Errorf("link_expansion.max_pages_per_run must be 1..%d", maxPages)
		}
	}
	if len(j.DeniedURLs) > maxPolicyItems {
		return fmt.Errorf("denied_urls must contain at most %d entries", maxPolicyItems)
	}

	j.deniedURLSet = make(map[string]struct{}, len(j.DeniedURLs))
	for index, raw := range j.DeniedURLs {
		canonical, err := canonicalWebURL(raw)
		if err != nil {
			return fmt.Errorf("denied_urls[%d]: %w", index, err)
		}
		if _, duplicate := j.deniedURLSet[canonical]; duplicate {
			return fmt.Errorf("denied_urls[%d] is a canonical duplicate", index)
		}
		j.DeniedURLs[index] = canonical
		j.deniedURLSet[canonical] = struct{}{}
	}
	if err := validateAdmissionPolicySize(*j); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(j.SeedURLs))
	for index, raw := range j.SeedURLs {
		canonical, err := canonicalWebURL(raw)
		if err != nil {
			return fmt.Errorf("seeds[%d]: %w", index, err)
		}
		if _, duplicate := seen[canonical]; duplicate {
			return fmt.Errorf("seeds[%d] is a canonical duplicate", index)
		}
		if !j.Allows(canonical) {
			return fmt.Errorf("seeds[%d] is outside the job scope", index)
		}
		seen[canonical] = struct{}{}
		j.SeedURLs[index] = canonical
		j.seeds = append(j.seeds, Seed{Key: configKey(j.ID, canonical), JobID: j.ID, URL: canonical})
	}
	if err := j.validateDiscoverySources(); err != nil {
		return err
	}
	return nil
}

func (j *Job) validateDiscoverySources() error {
	if len(j.DiscoverySources) > maxRootDiscoverySourcesPerJob {
		return fmt.Errorf("discovery_sources must contain at most %d entries", maxRootDiscoverySourcesPerJob)
	}
	seen := make(map[string]struct{}, len(j.DiscoverySources))
	for index := range j.DiscoverySources {
		source := &j.DiscoverySources[index]
		canonical, err := canonicalWebURL(source.URL)
		if err != nil {
			return fmt.Errorf("discovery_sources[%d].url: %w", index, err)
		}
		if _, duplicate := seen[canonical]; duplicate {
			return fmt.Errorf("discovery_sources[%d].url is a canonical duplicate", index)
		}
		parsed, err := url.Parse(canonical)
		if err != nil || !j.domainAllowed(strings.ToLower(parsed.Hostname())) {
			return fmt.Errorf("discovery_sources[%d].url is outside the job domain scope", index)
		}
		if err := validatePathPrefixes(
			fmt.Sprintf("discovery_sources[%d].allowed_source_path_prefixes", index),
			source.AllowedSourcePathPrefixes,
			true,
		); err != nil {
			return err
		}
		path := parsed.Path
		if path == "" {
			path = "/"
		}
		if !pathMatchesAnyPrefix(path, source.AllowedSourcePathPrefixes) {
			return fmt.Errorf("discovery_sources[%d].url is outside allowed_source_path_prefixes", index)
		}
		switch source.Kind {
		case SourceKindAuto, SourceKindSitemap, SourceKindFeed:
		default:
			return fmt.Errorf("discovery_sources[%d].kind must be auto, sitemap, or feed", index)
		}
		if source.PollInterval.Duration < minSourcePollInterval || source.PollInterval.Duration > maxSourcePollInterval {
			return fmt.Errorf("discovery_sources[%d].poll_interval must be between 5m and 720h", index)
		}
		if source.ErrorRecheckInterval.Duration < minSourceErrorRecheckInterval || source.ErrorRecheckInterval.Duration > maxSourceErrorRecheckInterval {
			return fmt.Errorf("discovery_sources[%d].error_recheck_interval must be between 5m and 24h", index)
		}
		if source.MaxCompressedBytes < 1 || source.MaxCompressedBytes > maxSourceCompressedBytes {
			return fmt.Errorf("discovery_sources[%d].max_compressed_bytes must be 1..%d", index, maxSourceCompressedBytes)
		}
		if source.MaxDecompressedBytes < 1 || source.MaxDecompressedBytes > maxSourceDecompressedBytes {
			return fmt.Errorf("discovery_sources[%d].max_decompressed_bytes must be 1..%d", index, maxSourceDecompressedBytes)
		}
		if source.MaxEntries < 1 || source.MaxEntries > maxSourceEntries {
			return fmt.Errorf("discovery_sources[%d].max_entries must be 1..%d", index, maxSourceEntries)
		}
		if source.MaxChildSources < 0 || source.MaxChildSources > maxSourceChildSources {
			return fmt.Errorf("discovery_sources[%d].max_child_sources must be 0..%d", index, maxSourceChildSources)
		}
		if source.MaxDepth < 0 || source.MaxDepth > maxSourceDepth {
			return fmt.Errorf("discovery_sources[%d].max_depth must be 0..%d", index, maxSourceDepth)
		}
		source.URL = canonical
		seen[canonical] = struct{}{}
	}
	return nil
}

func pathMatchesAnyPrefix(path string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func validatePathPrefixes(field string, prefixes []string, required bool) error {
	if required && len(prefixes) == 0 {
		return fmt.Errorf("%s is required", field)
	}
	if len(prefixes) > maxPolicyItems {
		return fmt.Errorf("%s must contain at most %d entries", field, maxPolicyItems)
	}
	seen := make(map[string]struct{}, len(prefixes))
	for index, prefix := range prefixes {
		lower := strings.ToLower(prefix)
		if len(prefix) > maxPathPrefixBytes || prefix == "" || !strings.HasPrefix(prefix, "/") || strings.ContainsAny(prefix, "?#\\%") ||
			strings.Contains(lower, "%2f") || strings.Contains(lower, "%5c") {
			return fmt.Errorf("%s[%d] must be an absolute URL path prefix", field, index)
		}
		decoded, err := url.PathUnescape(prefix)
		if err != nil {
			return fmt.Errorf("%s[%d] must be an unambiguous URL path prefix", field, index)
		}
		if !utf8.ValidString(decoded) {
			return fmt.Errorf("%s[%d] must be valid UTF-8", field, index)
		}
		for _, segment := range strings.Split(decoded, "/") {
			if segment == "." || segment == ".." {
				return fmt.Errorf("%s[%d] must not contain dot segments", field, index)
			}
		}
		if _, duplicate := seen[decoded]; duplicate {
			return fmt.Errorf("%s[%d] is duplicated", field, index)
		}
		seen[decoded] = struct{}{}
		prefixes[index] = decoded
	}
	return nil
}

func validateAdmissionPolicySize(job Job) error {
	// encoding/json can expand one input byte to six bytes. Reserve that
	// worst-case encoding for every future URL accepted by the API's 2 KiB cap,
	// rather than validating only the explicit seed URLs present at startup.
	worstCaseURL := strings.Repeat("\x00", maxWebURLBytes)
	raw, err := json.Marshal(struct {
		URL                 string   `json:"url"`
		AllowedPathPrefixes []string `json:"allowed_path_prefixes"`
		DeniedPathPrefixes  []string `json:"denied_path_prefixes,omitempty"`
		DeniedURLs          []string `json:"denied_urls,omitempty"`
	}{
		URL: worstCaseURL, AllowedPathPrefixes: job.AllowedPathPrefixes,
		DeniedPathPrefixes: job.DeniedPathPrefixes, DeniedURLs: job.DeniedURLs,
	})
	if err != nil {
		return errors.New("focused admission policy cannot be encoded")
	}
	if len(raw) > maxAdmissionBytes {
		return fmt.Errorf("focused admission policy exceeds %d bytes", maxAdmissionBytes)
	}
	return nil
}

func (j Job) Allows(raw string) bool {
	canonical, err := canonicalWebURL(raw)
	if err != nil {
		return false
	}
	if _, denied := j.deniedURLSet[canonical]; denied {
		return false
	}
	parsed, err := url.Parse(canonical)
	if err != nil || !j.domainAllowed(strings.ToLower(parsed.Hostname())) {
		return false
	}
	path := parsed.Path
	if path == "" {
		path = "/"
	}
	for _, prefix := range j.DeniedPathPrefixes {
		if strings.HasPrefix(path, prefix) {
			return false
		}
	}
	for _, prefix := range j.AllowedPathPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func (j Job) domainAllowed(host string) bool {
	if _, allowed := j.allowedDomain[host]; allowed {
		return true
	}
	if !j.IncludeSubdomains {
		return false
	}
	for domain := range j.allowedDomain {
		if strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}

func (c Config) Seeds() []Seed {
	var seeds []Seed
	for _, job := range c.Jobs {
		seeds = append(seeds, job.seeds...)
	}
	return seeds
}

// SourceSeeds returns configured root identities in deterministic job/source
// order without exposing configuration-owned storage.
func (c Config) SourceSeeds() []SourceSeed {
	return append([]SourceSeed(nil), c.sourceSeeds...)
}

// RootSource returns a defensive copy of one configured root source.
func (c Config) RootSource(key string) (RootSource, bool) {
	root, ok := c.rootSources[key]
	if !ok {
		return RootSource{}, false
	}
	root.Config = cloneDiscoverySource(root.Config)
	return root, true
}

func cloneDiscoverySource(source DiscoverySource) DiscoverySource {
	source.AllowedSourcePathPrefixes = append([]string(nil), source.AllowedSourcePathPrefixes...)
	return source
}

func configKey(jobID, canonicalURL string) string {
	hash := sha256.Sum256([]byte(jobID + "\x00" + canonicalURL))
	return hex.EncodeToString(hash[:])
}

func sourceConfigKey(jobID, canonicalURL string) string {
	hash := sha256.Sum256([]byte(jobID + "\x00source\x00" + canonicalURL))
	return hex.EncodeToString(hash[:])
}

func (c Config) Job(id string) (Job, bool) {
	for _, job := range c.Jobs {
		if job.ID == id {
			return job, true
		}
	}
	return Job{}, false
}

// LinkFrontierOpenCaps returns storage-validation ceilings, not the active
// per-job expansion policy. A disabled or narrowed policy must be able to open
// and transactionally deactivate state written under the previous policy.
// An explicitly positive global edge cap remains authoritative, so lowering it
// below retained state fails closed instead of deleting relationships.
func (c Config) LinkFrontierOpenCaps() (maxEdges, maxPerPage int) {
	maxEdges = c.MaxLinkEdges
	if maxEdges == 0 {
		maxEdges = maxLinkEdges
	}
	return maxEdges, maxOutboundLinksPerPage
}

func canonicalContactURI(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("is required")
	}
	if len(raw) > maxContactURIBytes || strings.ContainsAny(raw, "\r\n") {
		return "", fmt.Errorf("must be at most %d bytes without control characters", maxContactURIBytes)
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Fragment != "" {
		return "", errors.New("must be an absolute HTTPS or mailto URI without a fragment")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https":
		canonical, err := canonicalWebURL(raw)
		if err != nil || !strings.HasPrefix(canonical, "https://") {
			return "", errors.New("must be an absolute HTTPS or mailto URI without credentials")
		}
		return canonical, nil
	case "mailto":
		address := parsed.Opaque
		parsedAddress, addressErr := mail.ParseAddress(address)
		local, domain, found := strings.Cut(address, "@")
		if parsed.Host != "" || parsed.User != nil || parsed.RawQuery != "" || strings.Contains(address, "%") ||
			!found || local == "" || strings.Contains(domain, "@") || !validASCIIDNSName(strings.ToLower(domain)) ||
			addressErr != nil || parsedAddress.Name != "" || parsedAddress.Address != address {
			return "", errors.New("must contain a direct mailto address without query parameters")
		}
		return "mailto:" + local + "@" + strings.ToLower(domain), nil
	default:
		return "", errors.New("must use HTTPS or mailto")
	}
}

func canonicalWebURL(raw string) (string, error) {
	if len(raw) > maxWebURLBytes {
		return "", fmt.Errorf("URL exceeds %d bytes", maxWebURLBytes)
	}
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.User != nil || parsed.Hostname() == "" ||
		(!strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https")) {
		return "", errors.New("URL must be absolute HTTP(S) without credentials")
	}
	host := parsed.Hostname()
	if net.ParseIP(host) == nil && !validASCIIDNSName(host) {
		return "", errors.New("URL host must be a canonical ASCII DNS name")
	}
	if port := parsed.Port(); port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 || strconv.Itoa(portNumber) != port {
			return "", errors.New("URL port must be canonical")
		}
	}
	pathValue, ok := unambiguousWebPath(parsed)
	if !ok {
		return "", errors.New("URL path must not contain encoded separators, backslashes, or dot segments")
	}
	parsed.Path = pathValue
	parsed.RawPath = ""
	return cache.CanonicalURL(parsed.String())
}

func unambiguousWebPath(parsed *url.URL) (string, bool) {
	if parsed == nil {
		return "", false
	}
	escaped := strings.ToLower(parsed.EscapedPath())
	if strings.Contains(escaped, "%2f") || strings.Contains(escaped, "%5c") || strings.Contains(parsed.Path, `\`) {
		return "", false
	}
	pathValue := parsed.Path
	if pathValue == "" {
		pathValue = "/"
	}
	if !utf8.ValidString(pathValue) {
		return "", false
	}
	for _, segment := range strings.Split(pathValue, "/") {
		if segment == "." || segment == ".." {
			return "", false
		}
	}
	return pathValue, true
}

func validASCIIDNSName(host string) bool {
	if host == "" || len(host) > 253 || strings.HasSuffix(host, ".") {
		return false
	}
	for _, char := range host {
		if char > 127 {
			return false
		}
	}
	for _, label := range strings.Split(strings.ToLower(host), ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
				return false
			}
		}
	}
	return true
}

func invalid(err error) error {
	return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
}
