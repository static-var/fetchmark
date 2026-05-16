package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/extractor"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	"github.com/staticvar/fetchmark/internal/adapters/renderer"
	"github.com/staticvar/fetchmark/internal/adapters/robots"
	"github.com/staticvar/fetchmark/internal/adapters/searxng"
	"github.com/staticvar/fetchmark/internal/adapters/summarizer"
	"github.com/staticvar/fetchmark/internal/api"
	"github.com/staticvar/fetchmark/internal/config"
	"github.com/staticvar/fetchmark/internal/core/pipeline"
	"github.com/staticvar/fetchmark/internal/core/rank"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := newLogger(cfg.LogLevel)

	// SearXNG lives on a trusted internal network (docker-compose) or a
	// user-trusted URL; use the internal egress policy so private IPs
	// are permitted. External user-URL fetches will receive their own
	// DefaultExternal client in P2.
	internal := egress.DefaultInternal().HTTPClient(10 * time.Second)
	sx, err := searxng.NewMultiWithCooldown(cfg.SearxngURLs, internal, cfg.SearxngCooldown)
	if err != nil {
		return err
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
	if opt, perr := redis.ParseURL(cfg.RedisURL); perr == nil {
		rdb = redis.NewClient(opt)
		if cerr := rdb.Ping(context.Background()).Err(); cerr != nil {
			log.Warn("redis unreachable; falling back to in-memory cache", "err", cerr)
			if err := rdb.Close(); err != nil {
				log.Warn("redis close failed", "err", err)
			}
			rdb = nil
		}
	}
	c := cache.New(rdb, cfg.CacheTTL)
	defer c.Close()
	if rdb != nil {
		defer func() {
			if err := rdb.Close(); err != nil {
				log.Warn("redis close failed", "err", err)
			}
		}()
	}

	pipe := &pipeline.Pipeline{
		Searcher:  sx,
		Fetcher:   fx,
		Extractor: extractor.New(true),
		Cache:     c,
		Ranker:    rank.New(),
		// Close the SSRF gap on the render path: the fetcher's
		// DialControl doesn't run when the renderer is called
		// directly, so the pipeline validates the URL itself.
		EgressValidate: external.Validate,
	}

	// Optional headless renderer. A dedicated HTTP client is used so
	// the renderer's timeout and body budgets are isolated from the
	// outbound user-URL fetch path.
	if cfg.RendererURL != "" {
		rend, rerr := renderer.NewHTTP(renderer.Options{
			Endpoint: cfg.RendererURL,
			Timeout:  cfg.RendererTimeout,
			MaxBody:  cfg.RendererMaxBody,
			Token:    cfg.RendererToken,
		})
		if rerr != nil {
			return rerr
		}
		pipe.Renderer = rend
		pipe.RendererAuto = cfg.RendererAuto
		pipe.RendererTimeout = cfg.RendererTimeout
		log.Info("headless renderer enabled", "endpoint", cfg.RendererURL, "auto", cfg.RendererAuto)
	}

	handler := api.NewRouter(api.Deps{
		Log:         log,
		Config:      cfg,
		Pipeline:    pipe,
		Redis:       rdb,
		Summarizers: buildSummarizerRegistry(cfg, log),
		ReadyCheck: func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := sx.Ping(ctx); err != nil {
				return err
			}
			if rdb != nil {
				if err := rdb.Ping(ctx).Err(); err != nil {
					return err
				}
			}
			return nil
		},
	})

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      90 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Info("starting", "addr", cfg.ListenAddr, "dashboard", cfg.DashboardEnabled())
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
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
