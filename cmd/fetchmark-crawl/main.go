// Command fetchmark-crawl runs Fetchmark's optional, separately operated
// focused-ingestion worker. It never opens Fetchmark's corpus or artifact
// stores; live work crosses the authenticated URL-only admission boundary.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/staticvar/fetchmark/internal/adapters/corpusclient"
	"github.com/staticvar/fetchmark/internal/adapters/crawlfrontier"
	"github.com/staticvar/fetchmark/internal/adapters/crawlsource"
	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/robots"
	"github.com/staticvar/fetchmark/internal/core/focusedcrawl"
	"github.com/staticvar/fetchmark/internal/crawler"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	defaultTimeout := 90 * time.Second
	if raw := strings.TrimSpace(getenv("FM_CRAWLER_HTTP_TIMEOUT")); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed < time.Second || parsed > 10*time.Minute {
			fmt.Fprintln(stderr, "fetchmark-crawl: FM_CRAWLER_HTTP_TIMEOUT must be between 1s and 10m")
			return 2
		}
		defaultTimeout = parsed
	}

	flags := flag.NewFlagSet("fetchmark-crawl", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", getenv("FM_CRAWLER_CONFIG_FILE"), "absolute path to the strict crawl job JSON")
	statePath := flags.String("state", getenv("FM_CRAWLER_STATE_PATH"), "absolute path to the separate crawler frontier database")
	fetchmarkURL := flags.String("fetchmark-url", getenv("FM_CRAWLER_FETCHMARK_URL"), "fixed Fetchmark API origin")
	dryRun := flags.Bool("dry-run", false, "validate and print the seed plan without network or state access")
	once := flags.Bool("once", false, "execute one bounded live admission run")
	timeout := flags.Duration("timeout", defaultTimeout, "timeout for each source or Fetchmark HTTP request")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || *dryRun == *once {
		fmt.Fprintln(stderr, "fetchmark-crawl: choose exactly one of -dry-run or -once")
		return 2
	}
	if !filepath.IsAbs(*configPath) {
		fmt.Fprintln(stderr, "fetchmark-crawl: -config must be an absolute path")
		return 2
	}
	if *timeout < time.Second || *timeout > 10*time.Minute {
		fmt.Fprintln(stderr, "fetchmark-crawl: -timeout must be between 1s and 10m")
		return 2
	}

	configFile, err := openRegularFile(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-crawl: open config: %v\n", err)
		return 1
	}
	config, decodeErr := focusedcrawl.DecodeConfig(configFile)
	closeErr := configFile.Close()
	if decodeErr != nil {
		fmt.Fprintf(stderr, "fetchmark-crawl: %v\n", decodeErr)
		return 1
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "fetchmark-crawl: close config: %v\n", closeErr)
		return 1
	}

	if *dryRun {
		return writeJSON(stdout, stderr, map[string]any{
			"mode": "dry_run", "config_version": config.Version,
			"max_frontier_urls":              config.MaxFrontierURLs,
			"max_discovery_sources":          config.MaxDiscoverySources,
			"max_discovery_page_memberships": config.MaxDiscoveryPageMemberships,
			"max_discovery_source_edges":     config.MaxDiscoverySourceEdges,
			"max_link_edges":                 config.MaxLinkEdges,
			"jobs":                           len(config.Jobs),
			"planned":                        focusedcrawl.DryRun(config).Planned, "planned_sources": len(config.SourceSeeds()),
			"planned_link_expansion_jobs": linkExpansionJobCount(config),
		})
	}

	if !filepath.IsAbs(*statePath) {
		fmt.Fprintln(stderr, "fetchmark-crawl: -state must be an absolute path with -once")
		return 2
	}
	apiKey := getenv("FM_CRAWLER_ADMIN_API_KEY")
	if strings.TrimSpace(*fetchmarkURL) == "" || apiKey == "" {
		fmt.Fprintln(stderr, "fetchmark-crawl: Fetchmark URL and FM_CRAWLER_ADMIN_API_KEY are required with -once")
		return 2
	}

	client, err := corpusclient.New(corpusclient.Options{
		BaseURL: *fetchmarkURL, APIKey: apiKey, Timeout: *timeout,
		HTTPClient: &http.Client{Timeout: *timeout},
	})
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-crawl: configure admission client: %v\n", err)
		return 2
	}
	frontierCaps := discoveryCaps(config)
	maxLinkEdges, maxLinksPerPage := config.LinkFrontierOpenCaps()
	frontier, err := crawlfrontier.Open(*statePath, crawlfrontier.Options{
		MaxEntries: config.MaxFrontierURLs, MaxSources: config.MaxDiscoverySources,
		MaxPagesPerSource: frontierCaps.maxPagesPerSource, MaxChildrenPerSource: frontierCaps.maxChildrenPerSource,
		MaxMemberships: config.MaxDiscoveryPageMemberships, MaxSourceEdges: config.MaxDiscoverySourceEdges,
		MaxLinkEdges: maxLinkEdges, MaxLinksPerPage: maxLinksPerPage,
		MaxSourceDepth: frontierCaps.maxSourceDepth,
	})
	if err != nil {
		fmt.Fprintf(stderr, "fetchmark-crawl: open frontier: %v\n", err)
		return 1
	}

	bridge := frontierBridge{frontier}
	if err := focusedcrawl.SyncPlan(config, bridge, time.Now().UTC()); err != nil {
		_ = frontier.Close()
		fmt.Fprintf(stderr, "fetchmark-crawl: synchronize page/link policy: %v\n", err)
		return 1
	}
	var sourceSummary focusedcrawl.SourceSummary
	var runErr error
	if len(config.SourceSeeds()) > 0 {
		policy := egress.DefaultExternal()
		policy.ResponseHeaderTimeout = *timeout
		robotsChecker := robots.New(policy.HTTPClient(*timeout), 24*time.Hour, 512<<10)
		sourceClient, sourceErr := crawlsource.New(crawlsource.Options{
			Policy: policy, Robots: robotsChecker, UserAgent: crawlerUserAgent(config.ContactURI), Timeout: *timeout,
			MaxWireBytes: frontierCaps.maxWireBytes, MaxDecompressedBytes: frontierCaps.maxDecompressedBytes,
		})
		if sourceErr != nil {
			_ = frontier.Close()
			fmt.Fprintf(stderr, "fetchmark-crawl: configure discovery source client: %v\n", sourceErr)
			return 2
		}
		sourceSummary, runErr = (focusedcrawl.SourceRunner{
			Config: config, Frontier: bridge, Fetcher: sourceFetchBridge{sourceClient}, Parse: parseDiscovery,
			LeaseTTL: 5 * time.Minute,
		}).Run(ctx)
	} else {
		sourceSummary, runErr = (focusedcrawl.SourceRunner{Config: config, Frontier: bridge}).Run(ctx)
	}
	var summary focusedcrawl.Summary
	if runErr == nil {
		summary, runErr = (focusedcrawl.Runner{
			Config: config, Frontier: bridge, Admissions: admissionBridge{client}, LeaseTTL: 5 * time.Minute,
		}).Run(ctx)
	}
	closeErr = frontier.Close()
	if code := writeJSON(stdout, stderr, map[string]any{
		"mode": "once", "source_summary": sourceSummary, "admission_summary": summary,
	}); code != 0 {
		return code
	}
	if runErr != nil {
		fmt.Fprintf(stderr, "fetchmark-crawl: run incomplete: %v\n", runErr)
		return 1
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "fetchmark-crawl: close frontier: %v\n", closeErr)
		return 1
	}
	return 0
}

type crawlerCaps struct {
	maxPagesPerSource    int
	maxChildrenPerSource int
	maxSourceDepth       int
	maxWireBytes         int64
	maxDecompressedBytes int64
}

func discoveryCaps(config focusedcrawl.Config) crawlerCaps {
	caps := crawlerCaps{maxPagesPerSource: 1, maxChildrenPerSource: 1, maxSourceDepth: 1, maxWireBytes: 1, maxDecompressedBytes: 1}
	for _, job := range config.Jobs {
		for _, source := range job.DiscoverySources {
			caps.maxPagesPerSource = max(caps.maxPagesPerSource, source.MaxEntries)
			caps.maxChildrenPerSource = max(caps.maxChildrenPerSource, source.MaxChildSources)
			caps.maxSourceDepth = max(caps.maxSourceDepth, source.MaxDepth)
			caps.maxWireBytes = max(caps.maxWireBytes, source.MaxCompressedBytes)
			caps.maxDecompressedBytes = max(caps.maxDecompressedBytes, source.MaxDecompressedBytes)
		}
	}
	return caps
}

func linkExpansionJobCount(config focusedcrawl.Config) int {
	count := 0
	for _, job := range config.Jobs {
		if job.LinkExpansion != nil {
			count++
		}
	}
	return count
}

func crawlerUserAgent(contactURI string) string {
	return "FetchmarkCrawler/" + version + " (+" + contactURI + ")"
}

func openRegularFile(path string) (*os.File, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("path is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		_ = file.Close()
		return nil, errors.New("config path changed while opening")
	}
	return file, nil
}

func writeJSON(stdout, stderr io.Writer, value any) int {
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		fmt.Fprintf(stderr, "fetchmark-crawl: write summary: %v\n", err)
		return 1
	}
	return 0
}

type frontierBridge struct{ frontier *crawlfrontier.Frontier }

func (bridge frontierBridge) SyncSeeds(seeds []focusedcrawl.Seed, now time.Time) error {
	converted := make([]crawlfrontier.Seed, 0, len(seeds))
	for _, seed := range seeds {
		converted = append(converted, crawlfrontier.Seed{Key: seed.Key, JobID: seed.JobID, URL: seed.URL})
	}
	return bridge.frontier.SyncSeeds(converted, now)
}

func (bridge frontierBridge) SyncLinkPolicies(policies []focusedcrawl.LinkPolicy, now time.Time) error {
	converted := make([]crawlfrontier.LinkPolicy, 0, len(policies))
	for _, policy := range policies {
		converted = append(converted, crawlfrontier.LinkPolicy{JobID: policy.JobID, MaxLinksPerPage: policy.MaxLinksPerPage})
	}
	return bridge.frontier.SyncLinkPolicies(converted, now)
}

func (bridge frontierBridge) LeaseDueJob(jobID string, now time.Time, limit int, leaseTTL time.Duration) ([]focusedcrawl.FrontierEntry, error) {
	entries, err := bridge.frontier.LeaseDueJob(jobID, now, limit, leaseTTL)
	if err != nil {
		return nil, err
	}
	converted := make([]focusedcrawl.FrontierEntry, 0, len(entries))
	for _, entry := range entries {
		converted = append(converted, focusedcrawl.FrontierEntry{
			Key: entry.Key, JobID: entry.JobID, URL: entry.URL, State: focusedcrawl.EntryState(entry.State),
			Attempts: entry.Attempts, NextEligible: entry.NextEligible, LeaseUntil: entry.LeaseUntil,
			LeaseGeneration: entry.LeaseGeneration, LastReason: entry.LastReason, CanExpand: entry.CanExpand,
		})
	}
	return converted, nil
}

func (bridge frontierBridge) Complete(key string, leaseGeneration uint64, state focusedcrawl.EntryState, next time.Time, reason string) error {
	return bridge.frontier.Complete(key, leaseGeneration, crawlfrontier.State(state), next, reason)
}

func (bridge frontierBridge) CompleteWithLinks(completion focusedcrawl.LinkCompletion, now time.Time) (focusedcrawl.LinkMutationCounts, error) {
	links := make([]crawlfrontier.Seed, 0, len(completion.Links))
	for _, link := range completion.Links {
		links = append(links, crawlfrontier.Seed{Key: link.Key, JobID: link.JobID, URL: link.URL})
	}
	counts, err := bridge.frontier.CompleteWithLinks(crawlfrontier.LinkCompletion{
		Key: completion.Key, LeaseGeneration: completion.LeaseGeneration,
		Outcome: crawlfrontier.State(completion.Outcome), NextEligible: completion.NextEligible,
		Reason: completion.Reason, Links: links,
	}, now)
	return focusedcrawl.LinkMutationCounts{
		Reported: counts.Reported, AppliedEdges: counts.AppliedEdges, AddedEdges: counts.AddedEdges,
		RemovedEdges: counts.RemovedEdges, Created: counts.Created, Reactivated: counts.Reactivated,
	}, err
}

func (bridge frontierBridge) ReserveHost(authority string, now time.Time, interval time.Duration) (time.Duration, error) {
	return bridge.frontier.ReserveHost(authority, now, interval)
}

func (bridge frontierBridge) Counts() (map[focusedcrawl.EntryState]int, error) {
	counts, err := bridge.frontier.Counts()
	if err != nil {
		return nil, err
	}
	return map[focusedcrawl.EntryState]int{
		focusedcrawl.StateQueued: counts.Queued, focusedcrawl.StateLeased: counts.Leased,
		focusedcrawl.StateSucceeded: counts.Succeeded, focusedcrawl.StateRejected: counts.Rejected,
		focusedcrawl.StateRetry: counts.Retry, focusedcrawl.StateDisabled: counts.Disabled,
	}, nil
}

func (bridge frontierBridge) SyncDiscoverySources(seeds []focusedcrawl.SourceFrontierSeed, now time.Time) error {
	converted := make([]crawlfrontier.DiscoverySourceSeed, 0, len(seeds))
	for _, seed := range seeds {
		converted = append(converted, crawlfrontier.DiscoverySourceSeed{
			Key: seed.Key, JobID: seed.JobID, URL: seed.URL, Kind: crawlfrontier.DiscoverySourceKind(seed.Kind), RootKey: seed.RootKey,
		})
	}
	return bridge.frontier.SyncDiscoverySources(converted, now)
}

func (bridge frontierBridge) LeaseDueDiscoverySourcesJob(jobID string, now time.Time, limit int, leaseTTL time.Duration) ([]focusedcrawl.SourceFrontierEntry, error) {
	entries, err := bridge.frontier.LeaseDueDiscoverySourcesJob(jobID, now, limit, leaseTTL)
	if err != nil {
		return nil, err
	}
	converted := make([]focusedcrawl.SourceFrontierEntry, 0, len(entries))
	for _, entry := range entries {
		converted = append(converted, focusedcrawl.SourceFrontierEntry{
			Key: entry.Key, JobID: entry.JobID, URL: entry.URL, Kind: focusedcrawl.SourceKind(entry.Kind),
			RootKey: entry.RootKey, Depth: entry.Depth, Attempts: entry.Attempts,
			LeaseGeneration: entry.LeaseGeneration, ETag: entry.ETag, LastModified: entry.LastModified,
		})
	}
	return converted, nil
}

func (bridge frontierBridge) ApplyDiscoverySnapshot(snapshot focusedcrawl.SourceSnapshot, observedAt time.Time) error {
	pages := make([]crawlfrontier.DiscoveredPage, 0, len(snapshot.Pages))
	for _, page := range snapshot.Pages {
		pages = append(pages, crawlfrontier.DiscoveredPage{Key: page.Key, URL: page.URL})
	}
	children := make([]crawlfrontier.DiscoverySourceSeed, 0, len(snapshot.Children))
	for _, child := range snapshot.Children {
		children = append(children, crawlfrontier.DiscoverySourceSeed{
			Key: child.Key, JobID: child.JobID, URL: child.URL, Kind: crawlfrontier.DiscoverySourceKind(child.Kind),
			RootKey: child.RootKey, ParentKey: child.ParentKey, Depth: child.Depth,
		})
	}
	return bridge.frontier.ApplyDiscoverySnapshot(crawlfrontier.DiscoverySnapshot{
		SourceKey: snapshot.SourceKey, LeaseGeneration: snapshot.LeaseGeneration,
		Pages: pages, Children: children,
		Validators:   crawlfrontier.SourceValidators{ETag: snapshot.Validators.ETag, LastModified: snapshot.Validators.LastModified},
		NextEligible: snapshot.NextEligible,
	}, observedAt)
}

func (bridge frontierBridge) PreserveDiscoverySnapshot(key string, generation uint64, validators focusedcrawl.SourceValidators, nextEligible time.Time) error {
	return bridge.frontier.PreserveDiscoverySnapshot(key, generation, crawlfrontier.SourceValidators{
		ETag: validators.ETag, LastModified: validators.LastModified,
	}, nextEligible)
}

func (bridge frontierBridge) CompleteDiscoverySource(key string, generation uint64, nextEligible time.Time, reason string) error {
	return bridge.frontier.CompleteDiscoverySource(key, generation, crawlfrontier.StateRetry, nextEligible, reason)
}

type sourceFetchBridge struct{ client *crawlsource.Client }

func (bridge sourceFetchBridge) Fetch(ctx context.Context, request focusedcrawl.SourceFetchRequest) (focusedcrawl.SourceFetchResult, error) {
	result, err := bridge.client.Fetch(ctx, crawlsource.Request{
		URL: request.URL, AllowedPathPrefixes: request.AllowedPathPrefixes,
		IfNoneMatch: request.IfNoneMatch, IfModifiedSince: request.IfModifiedSince,
		MaxWireBytes: request.MaxWireBytes, MaxDecompressedBytes: request.MaxDecompressedBytes,
	})
	return focusedcrawl.SourceFetchResult{
		Status: result.Status, Body: result.Body, FinalURL: result.FinalURL,
		ETag: result.ETag, LastModified: result.LastModified, NotModified: result.NotModified,
		FreshUntil: result.Freshness.FreshUntil, NoStore: result.Freshness.NoStore, RetryAfter: result.RetryAfter,
	}, err
}

func parseDiscovery(raw []byte, sourceURL string, requested focusedcrawl.SourceKind, limits focusedcrawl.DiscoveryLimits) (focusedcrawl.ParsedDiscoveryDocument, error) {
	document, err := crawler.ParseDiscoveryDocument(raw, sourceURL, crawler.DiscoveryKind(requested), crawler.Limits{
		MaxDocumentBytes: limits.MaxDocumentBytes, MaxEntries: limits.MaxEntries,
		MaxURLBytes: limits.MaxURLBytes, MaxDepth: limits.MaxXMLDepth,
	})
	if err != nil {
		return focusedcrawl.ParsedDiscoveryDocument{}, err
	}
	converted := focusedcrawl.ParsedDiscoveryDocument{
		Kind:    focusedcrawl.SourceKind(document.Kind),
		Pages:   make([]focusedcrawl.ParsedDiscoveryPage, 0, len(document.Pages)),
		Sources: make([]focusedcrawl.ParsedDiscoverySource, 0, len(document.Sources)),
	}
	for _, page := range document.Pages {
		converted.Pages = append(converted.Pages, focusedcrawl.ParsedDiscoveryPage{URL: page.URL})
	}
	for _, source := range document.Sources {
		converted.Sources = append(converted.Sources, focusedcrawl.ParsedDiscoverySource{
			URL: source.URL, Kind: focusedcrawl.SourceKind(source.Kind),
		})
	}
	return converted, nil
}

type admissionBridge struct{ client *corpusclient.Client }

func (bridge admissionBridge) Admit(ctx context.Context, request focusedcrawl.AdmissionRequest) (focusedcrawl.AdmissionResult, error) {
	response, err := bridge.client.Admit(ctx, corpusclient.FocusedAdmission{
		URL: request.URL, AllowedPathPrefixes: request.AllowedPathPrefixes,
		DeniedPathPrefixes: request.DeniedPathPrefixes, DeniedURLs: request.DeniedURLs,
		MaxOutboundLinks: request.MaxOutboundLinks,
	})
	if err != nil {
		var admissionErr *corpusclient.AdmissionError
		if errors.As(err, &admissionErr) {
			return focusedcrawl.AdmissionResult{}, scheduledAdmissionError{admissionErr}
		}
		return focusedcrawl.AdmissionResult{}, err
	}
	if response.Count != 1 || len(response.Results) != 1 {
		return focusedcrawl.AdmissionResult{}, scheduledAdmissionError{&corpusclient.AdmissionError{Class: corpusclient.FailurePermanent}}
	}
	result := response.Results[0]
	if result.URL != request.URL {
		return focusedcrawl.AdmissionResult{}, scheduledAdmissionError{&corpusclient.AdmissionError{Class: corpusclient.FailurePermanent}}
	}
	return focusedcrawl.AdmissionResult{
		URL: result.URL, Status: string(result.Status), Reason: result.Reason, OutboundLinks: result.OutboundLinks,
	}, nil
}

type scheduledAdmissionError struct{ cause *corpusclient.AdmissionError }

func (err scheduledAdmissionError) Error() string { return err.cause.Error() }
func (err scheduledAdmissionError) Unwrap() error { return err.cause }
func (err scheduledAdmissionError) Fatal() bool {
	return err.cause.Class == corpusclient.FailureFatal || err.cause.Class == corpusclient.FailurePermanent
}
func (err scheduledAdmissionError) Temporary() bool {
	return err.cause.Class == corpusclient.FailureTransient
}
func (err scheduledAdmissionError) RetryAfter() time.Duration { return err.cause.RetryAfter }
