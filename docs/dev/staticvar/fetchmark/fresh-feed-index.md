# Fresh feed discovery

Fetchmark can use an operator-built RSS/Atom metadata snapshot as an optional
fresh-discovery lane. This is a focused source pack, not a crawler: the API
process opens one immutable, process-owned, non-writable JSON file, builds small in-memory Bleve indexes,
and performs only CPU-local lexical searches. It never polls feeds or pages on
the query path.

Enable it explicitly:

```sh
export FM_DISCOVERY_ENABLED_SOURCES=searxng,wikipedia,crossref,feedindex
export FM_FEED_INDEX_FILE=/absolute/path/to/feed-index.json
```

The built-in `fresh` pack assigns the lane weight `0.96`. Existing source ranks
therefore remain ahead of an equally ranked feed candidate while relevant feed
results can still enter the top ten through weighted RRF. Non-fresh intents do
not plan this lane. Queries that do not contain one of a source's declared
topics receive an authoritative empty response from the adapter.

## Snapshot contract

[`deploy/feed-index.example.json`](../../../../deploy/feed-index.example.json)
is a synthetic, offline schema fixture. Its timestamps are illustrative; copy
it and regenerate all evidence timestamps rather than enabling it directly.
The loader rejects unknown fields and fails closed unless all of these hold:

- the file is absolute, regular, at most 2 MiB, generated within seven days,
  and contains at most 32 sources and 3,200 documents;
- feed, license, and document URLs are HTTPS, contain no credentials or
  fragments, and are not local/private IP literals or local hostnames;
- every source declares a license identifier and license URL, a minimum poll
  interval of at least five minutes, at most 100 items per fetch, and explicit
  feed-level robots/noindex observations;
- every document has fresh observation timestamps, an explicit affirmative
  robots decision, and explicit negative HTML and X-Robots-Tag noindex
  observations; missing evidence is rejection, not permission;
- published metadata is no more than 400 days old and no more than 24 hours in
  the future.

DNS rebinding cannot occur while loading or searching because the adapter does
not resolve or fetch snapshot URLs. If a returned URL is later selected for
live extraction, Fetchmark's normal SSRF-safe resolver, redirect checks,
robots.txt handling, noindex enforcement, rate limits, and extraction budgets
still apply. A collector must apply the same redirect and DNS checks while
creating the snapshot.

The source-level license applies to every admitted document in that source.
Split feeds with mixed licenses into separate source records, or omit entries
whose reuse status is unclear. Store only titles and short summaries permitted
by that license; the snapshot format is not a page-body archive.

## Reproducible update boundary

The existing separate focused-ingestion worker already knows how to discover
RSS/Atom entries under explicit domain, path, rate, robots, noindex, noarchive,
retention, and takedown policies. Snapshot production remains outside the API
process so deployments can schedule and audit it independently. A valid
producer must write a new file beside the active one and atomically replace the
configured path only after completing all observations; restart Fetchmark to
load the new immutable snapshot.

The adapter unit tests construct the fixture at a fixed clock and cover topic
routing, lexical freshness ranking, private-address rejection, poll budgets,
licenses, robots, HTML noindex, and X-Robots-Tag noindex. They require no
network access:

```sh
go test ./internal/adapters/feedindex
```

The July 2026 fixed-suite experiment used assistant-generated provisional
judgments at the user's request. It is useful tuning evidence, not independent
human ground truth. Keep later human labels and run bindings separate.
