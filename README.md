# Fetchmark

**A self-hostable, Docker-first search API. A drop-in alternative to Tavily and Exa.**

Fetchmark glues together a SearXNG meta-search, a parallel fetch+extract
pipeline, and a BM25 re-ranker into one small Go binary. Point it at a query,
get back ranked results with clean Markdown, structured JSON, and cleaned
HTML — ready for RAG, LLM context, or downstream processing.

[![Go](https://img.shields.io/badge/go-1.22-00ADD8)](go.mod)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-ready-2496ED)](deploy/docker-compose.yml)

---

## Why Fetchmark

- **You own the stack.** No per-query pricing, no rate caps, no terms-of-service
  risk. Run it on your laptop, in a homelab, or in your VPC.
- **Small and boring.** Single Go binary. Redis + SearXNG are the only
  runtime dependencies. Distroless image, ~25 MB.
- **Batteries included.** SSRF-safe egress, per-key rate limits, on-disk
  artifact cache with cross-instance stampede protection, Prometheus
  metrics, a read-only ops dashboard, OpenAPI spec.
- **LLM-friendly output.** Clean Markdown, structured metadata
  (author, published_at, main_text), and a `js_required` flag so you
  know when a page needs a headless renderer.

---

## Quickstart

```bash
git clone https://github.com/staticvar/fetchmark && cd fetchmark
cp .env.example .env
printf 'FM_API_KEYS=%s\n' "$(openssl rand -hex 32)" > .env
printf 'FM_ADMIN_API_KEYS=\n' >> .env
docker compose -f deploy/docker-compose.yml up -d --build
```

Fetchmark listens on `:8080`. Redis and a bundled SearXNG come up alongside.
Use the generated key from `.env` for requests:

```bash
export FM_API_KEY=$(grep '^FM_API_KEYS=' .env | cut -d= -f2)
```

Generate replacement API keys with `openssl rand -hex 32`.

```bash
# Health
curl -s localhost:8080/healthz

# Search — meta-search + fetch + extract + rank
curl -s -X POST localhost:8080/v1/search \
  -H "Authorization: Bearer $FM_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"BM25 ranking algorithm","max_results":5}' | jq

# Parse arbitrary URLs — skip search, get clean content
curl -s -X POST localhost:8080/v1/parse \
  -H "Authorization: Bearer $FM_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"urls":["https://example.com"],"query":"example"}' | jq
```

A successful response contains `results[]` with `title`, `markdown`,
`cleaned_html`, `main_text`, `author`, `published_at`, BM25 `score`, and a
`from_cache` flag.

---

## Endpoints

| Method | Path              | Purpose                                              |
| ------ | ----------------- | ---------------------------------------------------- |
| POST   | `/v1/search`      | SearXNG query → parallel fetch → extract → BM25 rank |
| POST   | `/v1/parse`       | Fetch + extract arbitrary URLs; ranks if `query` set |
| POST   | `/v1/summarize`   | Parse + LLM summary (OpenAI-/Anthropic-compatible)   |
| GET    | `/healthz`        | Liveness probe                                       |
| GET    | `/readyz`         | Deep-check: SearXNG + Redis reachable                |
| GET    | `/metrics`        | Prometheus exposition                                |
| GET    | `/dashboard/`     | Read-only ops view (Basic Auth, opt-in)              |

See [`docs/openapi.yaml`](docs/openapi.yaml) for the full contract.

### Request knobs

Both `/v1/search` and `/v1/parse` accept:

```jsonc
{
  "query": "...",              // required on /v1/search
  "urls":  ["..."],            // required on /v1/parse
  "max_results": 10,           // caps returned list
  "engines": ["google","duckduckgo"],
  "formats": ["markdown","json","html"],
  "render":  false,            // force headless render path (opt-in)
  "respect_robots": true,
  "timeout_ms": 8000,
  "proxy_url": "http://..."    // admin API key required
}
```

### Authentication

```
Authorization: Bearer <key>
```

or the equivalent `X-API-Key: <key>` header. Generate API keys with
`openssl rand -hex 32` and set them in `FM_API_KEYS` before starting the
compose stack. Admin keys are optional; leave `FM_ADMIN_API_KEYS=` empty unless
you need admin-only fields (`proxy_url`, dashboard access). If you do enable
admin access, set `FM_ADMIN_API_KEYS` explicitly to one or more generated keys.

---

## Features

| Feature                                 | Status |
| --------------------------------------- | :----: |
| SearXNG meta-search                     |   ✅   |
| Multi-SearXNG failover (round-robin)    |   ✅   |
| Parallel fetch (per-host concurrency)   |   ✅   |
| SSRF / egress policy (private IP block) |   ✅   |
| Robots.txt + User-Agent pool            |   ✅   |
| Markdown / JSON / cleaned-HTML output   |   ✅   |
| BM25 re-ranking with engine diversity   |   ✅   |
| Exact-SHA + Jaccard near-dup dedupe     |   ✅   |
| CJK-aware shingling for near-dup        |   ✅   |
| Two-layer cache (Redis + in-memory)     |   ✅   |
| Cross-instance stampede lock            |   ✅   |
| Per-key rate limiting                   |   ✅   |
| Prometheus metrics + request IDs        |   ✅   |
| Ops dashboard (HTMX, read-only)         |   ✅   |
| Headless rendering (opt-in)             |   ✅   |
| Proxy URL passthrough (admin)           |   ✅   |
| LLM summarisation endpoint              |  🧪 (stub) |
| SSE streaming                           |  ⏳    |

---

## Configuration

All config is environment-driven. Copy `.env.example` and edit.

| Variable                     | Default                  | Purpose                               |
| ---------------------------- | ------------------------ | ------------------------------------- |
| `FM_API_KEYS`                | required                 | Comma-separated allowed API keys      |
| `FM_ADMIN_API_KEYS`          | empty                    | Optional admin-only keys; set explicitly when used |
| `FM_SEARXNG_URL`             | `http://searxng:8080`    | Single SearXNG instance               |
| `FM_SEARXNG_URLS`            | _(unset)_                | Comma-separated list for failover     |
| `FM_SEARXNG_COOLDOWN`        | `30s`                    | Skip window after an instance fails   |
| `FM_REDIS_URL`               | `redis://redis:6379/0`   | Cache + rate-limit state              |
| `FM_RATE_PER_SEC` / `_BURST` | `5` / `20`               | Default per-key token bucket          |
| `FM_RENDERER_URL`            | _(unset)_                | Enable headless render path           |
| `FM_RENDERER_AUTO`           | `false`                  | Auto-upgrade js_required pages        |
| `FM_DASHBOARD_USER` / `_PASSWORD` | _(unset)_           | Enable `/dashboard/` when both set    |
| `FM_LOG_LEVEL`               | `info`                   | `debug` / `info` / `warn` / `error`   |

Full list: [`internal/config/config.go`](internal/config/config.go).

### External SearXNG

If you already run SearXNG, point at it:

```bash
export FM_SEARXNG_URL=https://my.searxng.example
docker compose -f deploy/docker-compose.external.yml up -d --build
```

### Headless rendering (optional)

JS-heavy pages (SPAs, lazy-loaded content) can be sent through a headless
browser. Enable the `render` profile:

```bash
FM_RENDERER_URL=http://chromium:3000/content \
FM_RENDERER_TOKEN=$(openssl rand -hex 32) \
docker compose -f deploy/docker-compose.yml --profile render up -d --build
```

Then pass `"render": true` on `/v1/parse`, or set `FM_RENDERER_AUTO=true` to
auto-upgrade any page the extractor flags as `js_required`.

---

## Architecture

```
┌────────────┐  ┌──────────┐   ┌─────────────┐    ┌────────────┐
│  /v1/*     │─▶│ Pipeline │──▶│   Fetcher   │───▶│  Targets   │
│  handlers  │  │          │   │  (egress,   │    │ (internet) │
└────────────┘  │  ┌────┐  │   │   proxy,    │    └────────────┘
                │  │rank│  │   │   robots)   │
                │  └────┘  │   └─────────────┘
                │  ┌────┐  │   ┌─────────────┐    ┌────────────┐
                │  │dedu│  │   │  Extractor  │    │  SearXNG   │
                │  └────┘  │   │ (markdown)  │    │ (1..N      │
                │  ┌────┐  │   └─────────────┘    │  failover) │
                │  │cach│◀─┼───▶┌───────────┐     └────────────┘
                │  └────┘  │    │   Redis   │
                └──────────┘    │ (cache +  │
                                │ ratelim + │
                                │  lock)    │
                                └───────────┘
```

Layered along the classic ports & adapters pattern:

- `internal/core/pipeline` — orchestration
- `internal/core/rank`, `.../search`, `.../model` — domain
- `internal/adapters/{searxng,fetcher,extractor,renderer,cache,egress,robots}` — IO
- `internal/api` — presentation (handlers, middleware, dashboard)

Package docs live under [`docs/`](docs/).

---

## Development

```bash
make test       # go test -race ./...
make build      # binary at bin/fetchmark
make run        # run locally against env vars
make docker     # build distroless image
```

The full suite runs in <10s and is race-enabled. Add new behaviour with a
test first; see [`internal/core/pipeline/*_test.go`](internal/core/pipeline)
for patterns (stub adapters, table-driven cases).

---

## Operations

- **Metrics.** Prometheus exposition at `/metrics`. Key series:
  `fetchmark_fetch_outcome_total`, `fetchmark_extract_outcome_total`,
  `fetchmark_cache_events_total`, `fetchmark_searxng_instance_up`,
  `fetchmark_renderer_outcome_total`, HTTP latency histograms.
- **Dashboard.** Set `FM_DASHBOARD_USER` + `FM_DASHBOARD_PASSWORD` to enable
  `/dashboard/`. Shows recent requests, cache hit-rate, SearXNG instance
  health, and per-engine outcomes. Read-only; no mutating actions.
- **Logs.** Structured JSON via `log/slog`, with a request ID on every line.
- **Graceful shutdown.** `SIGTERM` drains in-flight requests, flushes
  metrics, and closes Redis.

---

## Security

- SSRF-hardened egress policy (private/link-local/loopback blocked by
  default, redirect chain revalidated, scheme-downgrade refused).
- Body + decompressed size caps enforced pre-read.
- Admin-only `proxy_url` passthrough: non-admin keys get 403.
- API keys are compared with a constant-time check.
- MIT-licensed; zero telemetry, zero phone-home.

Found a security issue? Please open a **private** security advisory on the
repository.

---

## Roadmap

Live backlog: [`docs/tasks.md`](docs/tasks.md). Open items:

- SSE streaming of search results (deferred pending demand)
- SimHash/MinHash dedupe (declined while result cap stays ≤ 50)
- Cross-instance summarizer config replication via Redis pub/sub (deferred;
  env vars remain authoritative, per-instance admin overrides are in-process)

### Summarize — configuration

The `/v1/summarize` endpoint accepts a `url` and calls the configured LLM
through either the OpenAI or Anthropic wire format. Any proxy that speaks
the OpenAI Chat Completions shape works (SubSandwich, Groq, Together,
Azure OpenAI, etc.). Configure via env:

```bash
# Required to enable the endpoint (at least one provider):
FM_SUMMARIZE_OPENAI_BASE_URL=http://localhost:4141/v1/
FM_SUMMARIZE_OPENAI_API_KEY=placeholder       # sent literally; use your key otherwise
FM_SUMMARIZE_OPENAI_MODEL=glm-5.1
FM_SUMMARIZE_OPENAI_TIMEOUT=60s
FM_SUMMARIZE_OPENAI_MAX_TOKENS=1024
FM_SUMMARIZE_OPENAI_THINKING=false            # set true + THINK_EFFORT for o-series
FM_SUMMARIZE_OPENAI_THINK_EFFORT=medium       # low|medium|high

FM_SUMMARIZE_ANTHROPIC_BASE_URL=https://api.anthropic.com/
FM_SUMMARIZE_ANTHROPIC_API_KEY=sk-ant-...
FM_SUMMARIZE_ANTHROPIC_MODEL=claude-3-5-sonnet-latest
FM_SUMMARIZE_ANTHROPIC_MAX_TOKENS=1024
FM_SUMMARIZE_ANTHROPIC_THINKING=false
FM_SUMMARIZE_ANTHROPIC_THINK_BUDGET=2048      # >= 1024 required by Anthropic

FM_SUMMARIZE_DEFAULT_PROVIDER=openai          # "openai" or "anthropic"
```

Per-request overrides (`provider`, `model`, `max_tokens`, `temperature`,
`instructions`, `thinking`, `timeout_ms`) are supported on the POST body.
For non-admin API keys, `provider`, `model`, and `thinking` overrides are
disabled unless explicitly allowed by config. `max_tokens`, `timeout_ms`,
`instructions`, and `thinking.budget_tokens` caps apply to all callers,
including admins. Admins can also upsert providers at runtime via
`/admin/summarize/providers` (see the OpenAPI spec); those overrides live
in-process and revert to env on restart.

**Docker compose + SubSandwich:** when running Fetchmark in the compose
stack and SubSandwich on the host, point the base URL at
`http://host.docker.internal:4141/v1/`.

---

## License

[MIT](LICENSE).
