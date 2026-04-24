# Fetchmark

A self-hostable, Docker-first alternative to Tavily/Exa. Powered by
SearXNG, with parallel readability extraction (Markdown / JSON /
cleaned-HTML) and BM25 re-ranking.

> **Status:** v1 scope complete. See `docs/openapi.yaml` for the API
> contract and `docs/tasks.md` for the live backlog.

## Quickstart (bundled SearXNG + Redis)

```
cp .env.example .env
docker compose -f deploy/docker-compose.yml up --build
```

Fetchmark listens on `:8080`. All `/v1/*` endpoints require an API key:

```
curl -sS http://localhost:8080/healthz
curl -sS -H "X-API-Key: dev-key" -X POST http://localhost:8080/v1/search -d '{}'
```

## External SearXNG

```
export FM_SEARXNG_URL=https://my.searxng.example
docker compose -f deploy/docker-compose.external.yml up --build
```

## Development

```
make test       # race-enabled unit tests
make build      # binary at bin/fetchmark
make run        # run locally
make docker     # build distroless image
```

## Endpoints (v1 scope)

| Method | Path           | Status       | Notes                                   |
| ------ | -------------- | ------------ | --------------------------------------- |
| POST   | `/v1/search`   | ready        | Search + parallel fetch + extract + BM25 |
| POST   | `/v1/parse`    | ready        | Parse-only; ranks when `query` is set   |
| POST   | `/v1/summarize`| **501**      | Designed; LLM wiring deferred to v2     |
| GET    | `/healthz`     | ready        | Liveness                                |
| GET    | `/readyz`      | ready        | SearXNG + Redis reachability            |
| GET    | `/metrics`     | ready        | Prometheus exposition                   |
| GET    | `/dashboard`   | opt-in       | Basic-Auth; off unless `FM_DASHBOARD_*` set |
