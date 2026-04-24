package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/staticvar/fetchmark/internal/adapters/cache"
	"github.com/staticvar/fetchmark/internal/adapters/egress"
	"github.com/staticvar/fetchmark/internal/adapters/extractor"
	"github.com/staticvar/fetchmark/internal/adapters/fetcher"
	"github.com/staticvar/fetchmark/internal/adapters/robots"
	"github.com/staticvar/fetchmark/internal/adapters/searxng"
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
	sx, err := searxng.New(cfg.SearxngURL, internal)
	if err != nil {
		return err
	}

	// External fetch path: every outbound user-URL fetch flows through
	// egress.DefaultExternal, which refuses private IPs, CGNAT, and
	// cross-scheme downgrades on redirect (see egress package doc).
	external := egress.DefaultExternal()
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
			rdb = nil
		}
	}
	c := cache.New(rdb, cfg.CacheTTL)

	pipe := &pipeline.Pipeline{
		Searcher:  sx,
		Fetcher:   fx,
		Extractor: extractor.New(true),
		Cache:     c,
		Ranker:    rank.New(),
	}

	handler := api.NewRouter(api.Deps{
		Log:      log,
		Config:   cfg,
		Pipeline: pipe,
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
