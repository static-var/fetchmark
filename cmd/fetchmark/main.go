package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/mail"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/staticvar/fetchmark/internal/adapters/arxiv"
	"github.com/staticvar/fetchmark/internal/adapters/bleveindex"
	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/crossref"
	"github.com/staticvar/fetchmark/internal/adapters/discoverycache"
	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/egressproxy"
	"github.com/staticvar/fetchmark/internal/adapters/extractor"
	"github.com/staticvar/fetchmark/internal/adapters/federationconfigfile"
	"github.com/staticvar/fetchmark/internal/adapters/federationpeer"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	githubadapter "github.com/staticvar/fetchmark/internal/adapters/github"
	artifactfs "github.com/staticvar/fetchmark/internal/adapters/localartifact"
	"github.com/staticvar/fetchmark/internal/adapters/mwmbl"
	"github.com/staticvar/fetchmark/internal/adapters/openpackindex"
	"github.com/staticvar/fetchmark/internal/adapters/openpackregistryfile"
	"github.com/staticvar/fetchmark/internal/adapters/pubmed"
	"github.com/staticvar/fetchmark/internal/adapters/renderer"
	"github.com/staticvar/fetchmark/internal/adapters/robots"
	"github.com/staticvar/fetchmark/internal/adapters/searchbudget"
	"github.com/staticvar/fetchmark/internal/adapters/searxng"
	"github.com/staticvar/fetchmark/internal/adapters/stackexchange"
	"github.com/staticvar/fetchmark/internal/adapters/summarizer"
	"github.com/staticvar/fetchmark/internal/adapters/wiby"
	"github.com/staticvar/fetchmark/internal/adapters/wikipedia"
	"github.com/staticvar/fetchmark/internal/adapters/yacy"
	"github.com/staticvar/fetchmark/internal/api"
	"github.com/staticvar/fetchmark/internal/buildidentity"
	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/discovery"
	"github.com/staticvar/fetchmark/internal/core/federation"
	"github.com/staticvar/fetchmark/internal/core/localcorpus"
	"github.com/staticvar/fetchmark/internal/core/openpackregistry"
	"github.com/staticvar/fetchmark/internal/core/pipeline"
	"github.com/staticvar/fetchmark/internal/core/rank"
	"github.com/staticvar/fetchmark/internal/core/search"
	"github.com/staticvar/fetchmark/internal/evaluationmanifest"
	"github.com/staticvar/fetchmark/internal/federationapi"
	"github.com/staticvar/fetchmark/internal/obs"
)

var version = "dev"

const (
	localExpiryStoreIndex    = "index"
	localExpiryStoreArtifact = "artifact"
)

type expirySweeper interface {
	Sweep(context.Context, time.Time) (int, error)
}

type namedExpirySweeper struct {
	store   string
	sweeper expirySweeper
}

func bindLocalPersistence(pipe *pipeline.Pipeline, index *bleveindex.Index, artifacts *artifactfs.Store) {
	pipe.LocalCorpus = nil
	pipe.LocalSearcher = nil
	pipe.LocalArtifacts = nil
	if index != nil {
		pipe.LocalCorpus = index
		pipe.LocalSearcher = index
	}
	if artifacts != nil {
		pipe.LocalArtifacts = artifacts
	}
}

func runLocalExpirySweeper(ctx context.Context, ticks <-chan time.Time, log *slog.Logger, sweepers []namedExpirySweeper) {
	for {
		select {
		case <-ctx.Done():
			return
		case now, ok := <-ticks:
			if !ok {
				return
			}
			now = now.UTC()
			for _, named := range sweepers {
				removed, err := named.sweeper.Sweep(ctx, now)
				if removed > 0 {
					obs.LocalExpirySweepRemovedTotal.WithLabelValues(named.store).Add(float64(removed))
				}
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					obs.LocalExpirySweepTotal.WithLabelValues(named.store, "error").Inc()
					log.Warn("local expiry sweep failed", "store", named.store, "err", err)
					continue
				}
				obs.LocalExpirySweepTotal.WithLabelValues(named.store, "ok").Inc()
			}
		}
	}
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	buildSHA256, buildIdentityErr := buildidentity.CurrentExecutableSHA256()
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)
	evaluationSpec, err := loadDiscoverySpec(cfg.DiscoveryPackFile)
	if err != nil {
		return err
	}
	_, evaluationConfiguration, evaluationConfigurationSHA, err := evaluationmanifest.Build(cfg, evaluationSpec)
	if err != nil {
		return fmt.Errorf("build evaluation configuration: %w", err)
	}
	log.Info("evaluation configuration resolved", "sha256", evaluationConfigurationSHA)
	if buildIdentityErr != nil {
		log.Warn("build identity unavailable", "error", buildIdentityErr)
	}
	federationRuntime, err := loadFederationRuntime(cfg)
	if err != nil {
		return err
	}
	var localIndex *bleveindex.Index
	var localArtifacts *artifactfs.Store
	localMode, err := localcorpus.ParseRetentionMode(cfg.LocalCorpusMode)
	if err != nil {
		return err
	}
	if localMode == localcorpus.ModeEphemeral {
		localIndex, err = bleveindex.Open(bleveindex.Options{
			InMemory: true, MaxDocumentBytes: cfg.LocalIndexMaxDocumentBytes,
			MaxBytes: cfg.LocalIndexMaxBytes, MaxDocuments: cfg.LocalIndexMaxDocuments,
		})
	} else if localMode != localcorpus.ModeDisabled {
		localIndex, err = bleveindex.Open(bleveindex.Options{
			Path: cfg.LocalIndexPath, MaxDocumentBytes: cfg.LocalIndexMaxDocumentBytes,
			MaxBytes: cfg.LocalIndexMaxBytes, MaxDocuments: cfg.LocalIndexMaxDocuments,
		})
	}
	if err != nil {
		return fmt.Errorf("open local index: %w", err)
	}
	if localIndex != nil {
		defer func() {
			if err := localIndex.Close(); err != nil {
				log.Warn("local index close failed", "err", err)
			}
		}()
		log.Info("local corpus enabled", "mode", localMode, "path", cfg.LocalIndexPath)
	}
	if localMode == localcorpus.ModePersonal || localMode == localcorpus.ModeCurated || localMode == localcorpus.ModeArchive {
		localArtifacts, err = artifactfs.Open(artifactfs.Options{
			Path: cfg.LocalArtifactPath, MaxBytes: cfg.LocalArtifactMaxBytes,
			MaxArtifactBytes: cfg.LocalArtifactMaxValueBytes, MaxEntries: cfg.LocalArtifactMaxEntries,
			Archive:     localMode == localcorpus.ModeArchive,
			MaxVersions: cfg.LocalArchiveMaxVersions, MaxVersionsPerURL: cfg.LocalArchiveMaxVersionsPerURL,
		})
		if err != nil {
			return fmt.Errorf("open local artifact store: %w", err)
		}
		defer func() {
			if err := localArtifacts.Close(); err != nil {
				log.Warn("local artifact store close failed", "err", err)
			}
		}()
		log.Info("local artifacts enabled", "mode", localMode, "path", cfg.LocalArtifactPath)
	}

	// SearXNG is optional. Construct its trusted-internal client only when the
	// operator explicitly enables that source, so SearX-free installations do
	// not validate, dial, or report readiness against it.
	var sx *searxng.MultiClient
	var searxSearcher search.Searcher
	if containsString(cfg.DiscoveryEnabledSources, "searxng") {
		internal := egress.DefaultInternal().HTTPClient(10 * time.Second)
		sx, err = searxng.NewMultiWithCooldown(cfg.SearxngURLs, internal, cfg.SearxngCooldown)
		if err != nil {
			return err
		}
		searxSearcher = sx
	}

	// External fetch path: every outbound user-URL fetch flows through
	// egress.DefaultExternal, which refuses private IPs, CGNAT, and
	// cross-scheme downgrades on redirect (see egress package doc).
	external := egress.DefaultExternal()
	external.HostAllowlist = cfg.HostAllowlist
	external.HostDenylist = cfg.HostDenylist
	external.MaxRedirects = cfg.MaxRedirects
	external.DialTimeout = cfg.HeaderTimeout
	external.ResponseHeaderTimeout = cfg.HeaderTimeout

	// Native discovery providers use a separate SSRF-safe client so content
	// host allow/deny lists cannot accidentally change fixed provider APIs.
	providerExternal := egress.DefaultExternal()
	providerExternal.DialTimeout = cfg.HeaderTimeout
	providerExternal.ResponseHeaderTimeout = cfg.HeaderTimeout
	discoveryPlanner, primaryDiscovery, err := buildDiscoveryPlannerFromSpec(cfg, searxSearcher, providerExternal.HTTPClient(10*time.Second), federationRuntime, evaluationSpec)
	if err != nil {
		return err
	}
	if closer, ok := primaryDiscovery.(io.Closer); ok {
		defer func() {
			if err := closer.Close(); err != nil {
				log.Warn("discovery source close failed", "err", err)
			}
		}()
	}
	rchk := robots.New(external.HTTPClient(5*time.Second), time.Hour, 0)
	fx, err := fetcher.New(fetcher.Options{
		Policy: external,
		Budgets: fetcher.Budgets{
			MaxBodyBytes:         cfg.MaxBodyBytes,
			MaxDecompressedBytes: cfg.MaxDecompressedBytes,
			MaxRedirects:         cfg.MaxRedirects,
			HeaderTimeout:        cfg.HeaderTimeout,
			FetchTimeout:         cfg.FetchTimeout,
			PerHostConcurrency:   cfg.PerHostConcurrency,
			GlobalConcurrency:    cfg.FetchConcurrency,
			Retries:              cfg.FetchRetries,
			AllowedMIME:          cfg.AllowedMIME,
		},
		Robots:        rchk,
		DefaultUA:     cfg.UserAgent,
		UserAgentPool: cfg.UserAgentsPool,
		RespectRobots: cfg.RespectRobots,
	})
	if err != nil {
		return err
	}

	// Cache: Redis when reachable, in-memory fallback otherwise so the
	// binary remains usable without a backing store in dev.
	var rdb *redis.Client
	opt, err := parseRedisOptions(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("invalid FM_REDIS_URL: %w", err)
	}
	rdb = redis.NewClient(opt)
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if cerr := rdb.Ping(pingCtx).Err(); cerr != nil {
		log.Warn("redis unreachable; falling back to in-memory cache", "err", cerr)
		if err := rdb.Close(); err != nil {
			log.Warn("redis close failed", "err", err)
		}
		rdb = nil
	}
	c := cache.NewWithMemoryLimits(rdb, cfg.CacheTTL, cache.MemoryLimits{
		MaxEntries:    cfg.MemoryCacheEntries,
		MaxBytes:      cfg.MemoryCacheBytes,
		MaxValueBytes: cfg.CacheMaxValueBytes,
	})
	defer c.Close()
	if rdb != nil {
		defer func() {
			if err := rdb.Close(); err != nil {
				log.Warn("redis close failed", "err", err)
			}
		}()
	}

	pipe := &pipeline.Pipeline{
		Searcher:                  primaryDiscovery,
		DiscoveryPlanner:          discoveryPlanner,
		AdvancedSearchConcurrency: cfg.AdvancedSearchConcurrency,
		Fetcher:                   fx,
		Extractor:                 extractor.New(true),
		Cache:                     c,
		Ranker:                    rank.New(),
		LocalRetentionPolicy:      localcorpus.Policy{Mode: localMode, MaxAge: cfg.LocalCorpusMaxAge},
		Robots:                    rchk,
		RobotsUserAgent:           cfg.UserAgent,
		// Initial validation is defense in depth. Renderer redirects and
		// subresources are enforced by the connection-time egress proxy.
		EgressValidate:               external.Validate,
		ArtifactConcurrency:          cfg.ArtifactConcurrency,
		MaxArtifactBodyBytes:         cfg.MaxBodyBytes,
		MaxArtifactDecompressedBytes: cfg.MaxDecompressedBytes,
		MaxRendererSourceBytes:       cfg.RendererMaxBody,
		MaxRequestSourceBytes:        cfg.MaxRequestSourceBytes,
		MaxRequestOutputBytes:        cfg.MaxRequestOutputBytes,
	}
	bindLocalPersistence(pipe, localIndex, localArtifacts)

	// Optional headless renderer. A dedicated HTTP client is used so
	// the renderer's timeout and body budgets are isolated from the
	// outbound user-URL fetch path.
	var rendererProxySrv *http.Server
	if cfg.RendererURL != "" {
		rend, rerr := renderer.NewHTTP(renderer.Options{
			Endpoint:       cfg.RendererURL,
			Timeout:        cfg.RendererTimeout,
			MaxBody:        cfg.RendererMaxBody,
			Token:          cfg.RendererToken,
			EgressProxyURL: cfg.RendererEgressProxyURL,
			UserAgent:      cfg.UserAgent,
		})
		if rerr != nil {
			return rerr
		}
		pipe.Renderer = rend
		pipe.RendererAuto = cfg.RendererAuto
		pipe.RendererTimeout = cfg.RendererTimeout
		if cfg.RendererProxyListenAddr != "" {
			rendererProxySrv = &http.Server{
				Addr:              cfg.RendererProxyListenAddr,
				Handler:           egressproxy.New(external),
				ReadHeaderTimeout: 5 * time.Second,
				IdleTimeout:       60 * time.Second,
			}
		}
		log.Info("headless renderer enabled", "endpoint", cfg.RendererURL, "auto", cfg.RendererAuto, "egress_proxy", cfg.RendererEgressProxyURL)
	}

	handler := api.NewRouter(api.Deps{
		Log:                        log,
		Config:                     cfg,
		Pipeline:                   pipe,
		Version:                    version,
		BuildSHA256:                buildSHA256,
		EvaluationConfiguration:    evaluationConfiguration,
		EvaluationConfigurationSHA: evaluationConfigurationSHA,
		Redis:                      rdb,
		Summarizers:                buildSummarizerRegistry(cfg, log),
		CorpusCurator:              pipe,
		ReadyCheck: func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if sx != nil && searxReadinessRequired(cfg) {
				if err := sx.Ping(ctx); err != nil {
					return err
				}
			}
			if rdb != nil {
				if err := rdb.Ping(ctx).Err(); err != nil {
					return err
				}
			}
			return nil
		},
	})

	writeTimeout := 90 * time.Second
	if needed := cfg.SummarizeMaxTimeout + 10*time.Second; needed > writeTimeout {
		writeTimeout = needed
	}
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       60 * time.Second,
	}
	var federationSrv *http.Server
	if cfg.FederationListenAddr != "" {
		// 16 trusted peers * 2 requests/second * five minutes plus bursts is
		// below this bound. Admission gates run before reservation as a second
		// defense against rejected requests consuming shared replay state.
		replay, replayErr := federationapi.NewReplayCache(16384, 5*time.Minute)
		if replayErr != nil {
			return fmt.Errorf("configure federation replay protection: %w", replayErr)
		}
		federationHandler, handlerErr := federationapi.NewHandler(federationapi.HandlerOptions{
			Identity: federationRuntime.Identity, TrustRegistry: federationRuntime.TrustRegistry,
			Searcher: localIndex, ReplayCache: replay,
		})
		if handlerErr != nil {
			return fmt.Errorf("configure federation listener: %w", handlerErr)
		}
		federationSrv = &http.Server{
			Addr: cfg.FederationListenAddr, Handler: federationHandler,
			ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
			WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10,
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	var expiryTicker *time.Ticker
	var expirySweepers sync.WaitGroup
	if localMode == localcorpus.ModePersonal {
		expiryTicker = time.NewTicker(cfg.LocalExpirySweepInterval)
		expirySweepers.Add(1)
		go func() {
			defer expirySweepers.Done()
			runLocalExpirySweeper(ctx, expiryTicker.C, log, []namedExpirySweeper{
				{store: localExpiryStoreIndex, sweeper: localIndex},
				{store: localExpiryStoreArtifact, sweeper: localArtifacts},
			})
		}()
		log.Info("local expiry sweeper enabled", "interval", cfg.LocalExpirySweepInterval)
	}
	// This defer was registered after the store closers, so it cancels and
	// joins the sweeper before either persistent adapter is closed.
	defer func() {
		stop()
		if expiryTicker != nil {
			expiryTicker.Stop()
		}
		expirySweepers.Wait()
	}()

	errCh := make(chan error, 3)
	go func() {
		log.Info("starting", "addr", cfg.ListenAddr, "dashboard", cfg.DashboardEnabled(), "version", version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	if rendererProxySrv != nil {
		go func() {
			log.Info("starting renderer egress proxy", "addr", rendererProxySrv.Addr)
			if err := rendererProxySrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("renderer egress proxy: %w", err)
			}
		}()
	}
	if federationSrv != nil {
		go func() {
			log.Info("starting private federation listener", "addr", federationSrv.Addr)
			if err := federationSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- fmt.Errorf("federation listener: %w", err)
			}
		}()
	}

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var shutdownErrors []error
	if federationSrv != nil {
		if err := federationSrv.Shutdown(shutdownCtx); err != nil {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("federation listener shutdown: %w", err))
		}
	}
	if rendererProxySrv != nil {
		if err := rendererProxySrv.Shutdown(shutdownCtx); err != nil {
			shutdownErrors = append(shutdownErrors, fmt.Errorf("renderer egress proxy shutdown: %w", err))
		}
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		shutdownErrors = append(shutdownErrors, fmt.Errorf("public server shutdown: %w", err))
	}
	return errors.Join(shutdownErrors...)
}

func buildDiscoveryPlanner(cfg config.Config, searx search.Searcher, providerHTTP *http.Client) (registry *discovery.Registry, primary search.Searcher, err error) {
	return buildDiscoveryPlannerWithFederation(cfg, searx, providerHTTP, nil)
}

type configuredFederation struct {
	Identity      federation.Identity
	TrustRegistry federation.TrustRegistry
}

func loadFederationRuntime(cfg config.Config) (*configuredFederation, error) {
	if cfg.FederationIdentityFile == "" && cfg.FederationTrustRegistryFile == "" {
		return nil, nil
	}
	identity, err := federationconfigfile.LoadIdentity(cfg.FederationIdentityFile)
	if err != nil {
		return nil, fmt.Errorf("load federation identity: %w", err)
	}
	registry, err := federationconfigfile.LoadTrustRegistry(cfg.FederationTrustRegistryFile)
	if err != nil {
		return nil, fmt.Errorf("load federation trust registry: %w", err)
	}
	return &configuredFederation{Identity: identity, TrustRegistry: registry}, nil
}

func buildDiscoveryPlannerWithFederation(cfg config.Config, searx search.Searcher, providerHTTP *http.Client, federationRuntime *configuredFederation) (registry *discovery.Registry, primary search.Searcher, err error) {
	spec, err := loadDiscoverySpec(cfg.DiscoveryPackFile)
	if err != nil {
		return nil, nil, err
	}
	return buildDiscoveryPlannerFromSpec(cfg, searx, providerHTTP, federationRuntime, spec)
}

func buildDiscoveryPlannerFromSpec(cfg config.Config, searx search.Searcher, providerHTTP *http.Client, federationRuntime *configuredFederation, spec discovery.RegistrySpec) (registry *discovery.Registry, primary search.Searcher, err error) {
	var closers []io.Closer
	defer func() {
		if err != nil {
			_ = closeDiscoverySources(closers)
		}
	}()
	specByID := make(map[string]discovery.SourceSpec, len(spec.Sources))
	for _, source := range spec.Sources {
		specByID[source.ID] = source
	}
	uniqueEnabled := uniqueStrings(cfg.DiscoveryEnabledSources)
	if len(uniqueEnabled) == 0 {
		return nil, nil, errors.New("configure discovery: at least one source must be enabled")
	}
	if cfg.DiscoveryCacheEntries < len(uniqueEnabled) || cfg.DiscoveryCacheBytes < int64(len(uniqueEnabled)) || cfg.DiscoveryMaxInflight < len(uniqueEnabled) {
		return nil, nil, errors.New("configure discovery: cache entry, byte, and inflight budgets must each cover every enabled source")
	}
	cacheOptions := splitDiscoveryCacheOptions(cfg, len(uniqueEnabled))
	providerUserAgent, err := discoveryProviderUserAgent(cfg.UserAgent, cfg.Contact)
	if err != nil {
		return nil, nil, err
	}
	adapters := make(map[string]search.Searcher, len(uniqueEnabled))
	var packRegistry *openpackregistry.Registry
	startupNow := time.Now().UTC()
	for _, id := range uniqueEnabled {
		sourceSpec, exists := specByID[id]
		if !exists {
			return nil, nil, fmt.Errorf("configure discovery: enabled source %q is not defined", id)
		}
		var adapter search.Searcher
		cacheAdapter := true
		switch sourceSpec.Kind {
		case "searxng":
			if id != "searxng" {
				return nil, nil, fmt.Errorf("configure discovery: primary SearXNG source must be named searxng")
			}
			if searx == nil {
				return nil, nil, errors.New("configure discovery: SearXNG client is required when source searxng is enabled")
			}
			adapter, err = searchbudget.New(searx, searchbudget.Options{
				RatePerSecond: sourceSpec.RatePerSecond, Burst: sourceSpec.Burst,
				MaxConcurrency: sourceSpec.MaxConcurrency,
			})
		case "wikipedia":
			adapter, err = wikipedia.New(wikipedia.Options{
				HTTPClient: providerHTTP, UserAgent: providerUserAgent, MaxResults: sourceSpec.MaxResults,
				MaxBodyBytes: cfg.DiscoveryProviderMaxBody, RatePerSecond: sourceSpec.RatePerSecond,
				Burst: sourceSpec.Burst, MaxConcurrency: sourceSpec.MaxConcurrency,
			})
		case "crossref":
			adapter, err = crossref.New(crossref.Options{
				HTTPClient: providerHTTP, UserAgent: providerUserAgent, Mailto: cfg.CrossrefMailto,
				MaxResults: sourceSpec.MaxResults, MaxBodyBytes: cfg.DiscoveryProviderMaxBody,
				RatePerSecond: sourceSpec.RatePerSecond, Burst: sourceSpec.Burst,
				MaxConcurrency: sourceSpec.MaxConcurrency,
			})
		case "arxiv":
			if id != "arxiv" {
				return nil, nil, errors.New("configure discovery: native arXiv source must be named arxiv")
			}
			adapter, err = arxiv.New(arxiv.Options{
				HTTPClient: providerHTTP, UserAgent: providerUserAgent, MaxResults: sourceSpec.MaxResults,
				MaxBodyBytes: cfg.DiscoveryProviderMaxBody, RatePerSecond: sourceSpec.RatePerSecond,
				Burst: sourceSpec.Burst, MaxConcurrency: sourceSpec.MaxConcurrency,
			})
		case "mwmbl":
			if id != "mwmbl" {
				return nil, nil, errors.New("configure discovery: native Mwmbl source must be named mwmbl")
			}
			adapter, err = mwmbl.New(mwmbl.Options{
				HTTPClient: providerHTTP, UserAgent: providerUserAgent, MaxResults: sourceSpec.MaxResults,
				MaxBodyBytes: cfg.DiscoveryProviderMaxBody, RatePerSecond: sourceSpec.RatePerSecond,
				Burst: sourceSpec.Burst, MaxConcurrency: sourceSpec.MaxConcurrency,
			})
		case "wiby":
			if id != "wiby" {
				return nil, nil, errors.New("configure discovery: native Wiby source must be named wiby")
			}
			adapter, err = wiby.New(wiby.Options{
				HTTPClient: providerHTTP, UserAgent: providerUserAgent, MaxResults: sourceSpec.MaxResults,
				MaxBodyBytes: cfg.DiscoveryProviderMaxBody, RatePerSecond: sourceSpec.RatePerSecond,
				Burst: sourceSpec.Burst, MaxConcurrency: sourceSpec.MaxConcurrency,
			})
		case "stackexchange":
			if id != "stackexchange" {
				return nil, nil, errors.New("configure discovery: native Stack Exchange source must be named stackexchange")
			}
			adapter, err = stackexchange.New(stackexchange.Options{
				HTTPClient: providerHTTP, UserAgent: providerUserAgent, MaxResults: sourceSpec.MaxResults,
				MaxBodyBytes: cfg.DiscoveryProviderMaxBody, RatePerSecond: sourceSpec.RatePerSecond,
				Burst: sourceSpec.Burst, MaxConcurrency: sourceSpec.MaxConcurrency,
			})
		case "github":
			if id != "github" {
				return nil, nil, errors.New("configure discovery: native GitHub source must be named github")
			}
			adapter, err = githubadapter.New(githubadapter.Options{
				HTTPClient: providerHTTP, UserAgent: providerUserAgent, MaxResults: sourceSpec.MaxResults,
				MaxBodyBytes: cfg.DiscoveryProviderMaxBody, RatePerSecond: sourceSpec.RatePerSecond,
				Burst: sourceSpec.Burst, MaxConcurrency: sourceSpec.MaxConcurrency,
			})
		case "pubmed":
			if id != "pubmed" {
				return nil, nil, errors.New("configure discovery: native PubMed source must be named pubmed")
			}
			adapter, err = pubmed.New(pubmed.Options{
				HTTPClient: providerHTTP, UserAgent: providerUserAgent, Email: cfg.PubMedEmail,
				MaxResults: sourceSpec.MaxResults, MaxBodyBytes: cfg.DiscoveryProviderMaxBody,
				RatePerSecond: sourceSpec.RatePerSecond, Burst: sourceSpec.Burst,
				MaxConcurrency: sourceSpec.MaxConcurrency,
			})
		case "yacy":
			if id != "yacy" {
				return nil, nil, errors.New("configure discovery: native YaCy source must be named yacy")
			}
			yacyHTTP, clientErr := configuredYaCyHTTPClient(cfg.YaCyURL, time.Duration(sourceSpec.TimeoutMS)*time.Millisecond)
			if clientErr != nil {
				return nil, nil, fmt.Errorf("configure discovery source %q: %w", id, clientErr)
			}
			adapter, err = yacy.New(yacy.Options{
				Endpoint: cfg.YaCyURL, HTTPClient: yacyHTTP, UserAgent: providerUserAgent,
				Resource: cfg.YaCyResource, AllowInsecureHTTP: cfg.YaCyAllowInsecureHTTP,
				MaxResults: sourceSpec.MaxResults, MaxBodyBytes: cfg.DiscoveryProviderMaxBody,
				RatePerSecond: sourceSpec.RatePerSecond, Burst: sourceSpec.Burst,
				MaxConcurrency: sourceSpec.MaxConcurrency,
			})
		case "openpack":
			cacheAdapter = false
			if packRegistry == nil {
				loaded, loadErr := loadOpenPackRegistry(cfg.OpenPackRegistryFile)
				if loadErr != nil {
					return nil, nil, fmt.Errorf("configure discovery source %q: %w", id, loadErr)
				}
				packRegistry = &loaded
			}
			binding, found := packRegistry.Lookup(id)
			if !found {
				return nil, nil, fmt.Errorf("configure discovery source %q: no open-pack registry binding", id)
			}
			acceptance := binding.Acceptance(startupNow)
			opened, openErr := openpackindex.Open(openpackindex.OpenOptions{
				Path: binding.InstalledPath, ExpectedManifestSHA256: binding.ManifestSHA256,
				ExpectedPackID: binding.PackID, ExpectedRevision: binding.Revision,
				ExpectedRecordCount: binding.RecordCount, ExpectedKeyID: acceptance.ExpectedKeyID,
				ExpectedCreatedAt: binding.CreatedAt, ExpectedExpiresAt: binding.ExpiresAt,
				Now: startupNow, ProviderID: id,
			})
			if openErr != nil {
				return nil, nil, fmt.Errorf("configure discovery source %q: %w", id, openErr)
			}
			closers = append(closers, opened)
			adapter, err = searchbudget.New(opened, searchbudget.Options{
				RatePerSecond: sourceSpec.RatePerSecond, Burst: sourceSpec.Burst,
				MaxConcurrency: sourceSpec.MaxConcurrency,
			})
		case "federation":
			if federationRuntime == nil {
				return nil, nil, fmt.Errorf("configure discovery source %q: FM_FEDERATION_IDENTITY_FILE and FM_FEDERATION_TRUST_REGISTRY_FILE are required", id)
			}
			peer, found := federationRuntime.TrustRegistry.Lookup(id)
			if !found {
				return nil, nil, fmt.Errorf("configure discovery source %q: no federation trust binding", id)
			}
			peerTimeout := time.Duration(sourceSpec.TimeoutMS) * time.Millisecond
			if peerTimeout > time.Minute {
				peerTimeout = time.Minute
			}
			peerClient, peerErr := federationpeer.New(federationpeer.Options{
				Identity: federationRuntime.Identity, Peer: peer, Timeout: peerTimeout,
			})
			if peerErr != nil {
				return nil, nil, fmt.Errorf("configure discovery source %q: %w", id, peerErr)
			}
			adapter, err = searchbudget.New(peerClient, searchbudget.Options{
				RatePerSecond: sourceSpec.RatePerSecond, Burst: sourceSpec.Burst,
				MaxConcurrency: sourceSpec.MaxConcurrency,
			})
		default:
			return nil, nil, fmt.Errorf("configure discovery: unsupported source kind %q", sourceSpec.Kind)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("configure discovery source %q: %w", id, err)
		}
		if cacheAdapter {
			cached, cacheErr := discoverycache.New(adapter, cacheOptions)
			if cacheErr != nil {
				return nil, nil, fmt.Errorf("configure discovery cache for %q: %w", id, cacheErr)
			}
			adapter = cached
		}
		adapters[id] = adapter
	}
	registry, err = discovery.NewRegistryFromSpec(spec, adapters, cfg.DiscoveryPrimarySource, cfg.DiscoveryEnabledPacks, cfg.DiscoveryEnabledSources)
	if err != nil {
		return nil, nil, fmt.Errorf("configure discovery registry: %w", err)
	}
	primary, exists := adapters[cfg.DiscoveryPrimarySource]
	if !exists {
		return nil, nil, fmt.Errorf("configure discovery: primary source %q is not enabled", cfg.DiscoveryPrimarySource)
	}
	if len(closers) > 0 {
		primary = &managedDiscoverySearcher{Searcher: primary, providerID: cfg.DiscoveryPrimarySource, closers: closers}
	}
	return registry, primary, nil
}

func configuredYaCyHTTPClient(rawEndpoint string, timeout time.Duration) (*http.Client, error) {
	parsed, err := url.Parse(rawEndpoint)
	if err != nil || parsed.Hostname() == "" {
		return nil, errors.New("YaCy endpoint must contain a hostname")
	}
	policy := egress.DefaultInternal()
	policy.HostAllowlist = []string{parsed.Hostname()}
	policy.MaxRedirects = 0
	policy.DialTimeout = min(timeout, 10*time.Second)
	policy.ResponseHeaderTimeout = timeout
	return policy.HTTPClient(timeout), nil
}

type managedDiscoverySearcher struct {
	search.Searcher
	providerID string
	closers    []io.Closer
}

func (managed *managedDiscoverySearcher) SearchBatch(ctx context.Context, query search.Query) (search.SearchBatch, error) {
	if batchSearcher, ok := managed.Searcher.(search.BatchSearcher); ok {
		return batchSearcher.SearchBatch(ctx, query)
	}
	started := time.Now()
	hits, err := managed.Searcher.Search(ctx, query)
	if err != nil {
		return search.SearchBatch{Provider: managed.providerID, Instance: managed.providerID, Status: search.BatchFailed, Duration: time.Since(started)}, err
	}
	status := search.BatchHealthy
	if len(hits) == 0 {
		status = search.BatchAuthoritativeEmpty
	}
	return search.SearchBatch{Hits: hits, Provider: managed.providerID, Instance: managed.providerID, Status: status, Duration: time.Since(started)}, nil
}

func (managed *managedDiscoverySearcher) Close() error {
	return closeDiscoverySources(managed.closers)
}

func closeDiscoverySources(closers []io.Closer) error {
	errorsSeen := make([]error, 0, len(closers))
	for index := len(closers) - 1; index >= 0; index-- {
		if err := closers[index].Close(); err != nil {
			errorsSeen = append(errorsSeen, err)
		}
	}
	return errors.Join(errorsSeen...)
}

func loadOpenPackRegistry(path string) (openpackregistry.Registry, error) {
	if strings.TrimSpace(path) == "" {
		return openpackregistry.Registry{}, errors.New("FM_OPEN_PACK_REGISTRY_FILE is required for enabled openpack sources")
	}
	loaded, err := openpackregistryfile.Load(path)
	if err != nil {
		return openpackregistry.Registry{}, fmt.Errorf("load open pack registry: %w", err)
	}
	return loaded, nil
}

func discoveryProviderUserAgent(userAgent, contact string) (string, error) {
	userAgent = strings.TrimSpace(userAgent)
	contact = strings.TrimSpace(contact)
	if contact == "" {
		return userAgent, nil
	}
	if len(contact) > 256 || strings.ContainsAny(contact, "\r\n") || !validDiscoveryContact(contact) {
		return "", errors.New("configure discovery: FM_CONTACT must be an email, mailto URL, or HTTP(S) contact URL")
	}
	if validPlainEmail(contact) {
		contact = "mailto:" + contact
	}
	return userAgent + " (contact: " + contact + ")", nil
}

func validDiscoveryContact(contact string) bool {
	parsed, err := url.Parse(contact)
	if err == nil && (parsed.Scheme == "https" || parsed.Scheme == "http") && parsed.Hostname() != "" {
		return true
	}
	if err == nil && parsed.Scheme == "mailto" && parsed.RawQuery == "" {
		address := parsed.Opaque
		if address == "" {
			address = strings.TrimPrefix(parsed.Path, "/")
		}
		return validPlainEmail(address)
	}
	return validPlainEmail(contact)
}

func validPlainEmail(value string) bool {
	address, err := mail.ParseAddress(value)
	return err == nil && strings.EqualFold(address.Address, value)
}

func loadDiscoverySpec(path string) (discovery.RegistrySpec, error) {
	if strings.TrimSpace(path) == "" {
		spec, err := discovery.DefaultSpec()
		if err != nil {
			return discovery.RegistrySpec{}, fmt.Errorf("load built-in discovery packs: %w", err)
		}
		return spec, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return discovery.RegistrySpec{}, fmt.Errorf("open discovery pack file: %w", err)
	}
	defer file.Close()
	spec, err := discovery.LoadSpec(file)
	if err != nil {
		return discovery.RegistrySpec{}, fmt.Errorf("load discovery pack file: %w", err)
	}
	return spec, nil
}

func splitDiscoveryCacheOptions(cfg config.Config, providers int) discoverycache.Options {
	if providers < 1 {
		providers = 1
	}
	maxEntries := max(1, cfg.DiscoveryCacheEntries/providers)
	maxBytes := max(int64(1), cfg.DiscoveryCacheBytes/int64(providers))
	maxEntryBytes := min(cfg.DiscoveryCacheMaxEntryBytes, maxBytes)
	maxInflight := max(1, cfg.DiscoveryMaxInflight/providers)
	return discoverycache.Options{
		FreshTTL: cfg.DiscoveryCacheTTL, StaleTTL: cfg.DiscoveryCacheStaleTTL,
		RefreshTimeout: cfg.DiscoveryRefreshTimeout, MaxEntries: maxEntries,
		MaxBytes: maxBytes, MaxEntryBytes: maxEntryBytes, MaxInflight: maxInflight,
	}
}

func uniqueStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func searxReadinessRequired(cfg config.Config) bool {
	return cfg.DiscoveryPrimarySource == "searxng" && containsString(cfg.DiscoveryEnabledSources, "searxng")
}

func parseRedisOptions(raw string) (*redis.Options, error) {
	return redis.ParseURL(raw)
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// buildSummarizerRegistry seeds the summarize registry from env vars.
// Providers with an empty model are skipped entirely so operators can
// enable one upstream without being forced to set both. A dedicated
// HTTP client is used so the LLM path is insulated from the Fetcher's
// egress policy — LLM endpoints are admin-configured trusted upstreams
// and may legitimately be on localhost (SubSandwich) during testing.
func buildSummarizerRegistry(cfg config.Config, log *slog.Logger) *summarizer.Registry {
	trusted := &http.Client{Timeout: 0} // per-call deadlines are set in the handler
	reg := summarizer.NewRegistry(summarizer.DefaultFactory, trusted)

	if cfg.SummarizeOpenAIModel != "" {
		oa := summarizer.ProviderConfig{
			Name:      "openai",
			Kind:      summarizer.KindOpenAI,
			BaseURL:   cfg.SummarizeOpenAIBaseURL,
			APIKey:    cfg.SummarizeOpenAIAPIKey,
			Model:     cfg.SummarizeOpenAIModel,
			Timeout:   cfg.SummarizeOpenAITimeout,
			MaxTokens: cfg.SummarizeOpenAIMaxTokens,
			Thinking: summarizer.Thinking{
				Enabled: cfg.SummarizeOpenAIThinking,
				Effort:  cfg.SummarizeOpenAIThinkEffort,
			},
		}
		if err := reg.Set(oa); err != nil {
			log.Warn("summarizer: openai profile rejected", "err", err)
		} else {
			log.Info("summarizer: openai profile configured", "model", oa.Model, "base_url", oa.BaseURL)
		}
	}
	if cfg.SummarizeAnthropicModel != "" {
		an := summarizer.ProviderConfig{
			Name:      "anthropic",
			Kind:      summarizer.KindAnthropic,
			BaseURL:   cfg.SummarizeAnthropicBaseURL,
			APIKey:    cfg.SummarizeAnthropicAPIKey,
			Model:     cfg.SummarizeAnthropicModel,
			Timeout:   cfg.SummarizeAnthropicTimeout,
			MaxTokens: cfg.SummarizeAnthropicMaxTokens,
			Thinking: summarizer.Thinking{
				Enabled:      cfg.SummarizeAnthropicThinking,
				BudgetTokens: cfg.SummarizeAnthropicThinkBudget,
			},
		}
		if err := reg.Set(an); err != nil {
			log.Warn("summarizer: anthropic profile rejected", "err", err)
		} else {
			log.Info("summarizer: anthropic profile configured", "model", an.Model, "base_url", an.BaseURL)
		}
	}
	if d := strings.TrimSpace(cfg.SummarizeDefaultProvider); d != "" {
		if err := reg.SetDefault(d); err != nil {
			log.Warn("summarizer: default provider not registered", "name", d, "err", err)
		}
	}
	return reg
}
