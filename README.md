# Fetchmark

A self-hostable, Docker-first alternative to Tavily/Exa. Powered by
SearXNG, with parallel readability extraction (Markdown / JSON /
cleaned-HTML) and BM25 re-ranking.

> **Status:** early development. See `docs/tasks.md` for the live backlog.

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
| POST   | `/v1/search`   | stubbed      | Full pipeline lands in P1–P4            |
| POST   | `/v1/parse`    | stubbed      | Parse-only endpoint, lands in P5        |
| POST   | `/v1/summarize`| **501**      | Designed; LLM wiring deferred to v2     |
| GET    | `/healthz`     | ready        | Liveness                                |
| GET    | `/readyz`      | ready        | Dependency reachability                 |
| GET    | `/metrics`     | ready        | Prometheus exposition                   |
