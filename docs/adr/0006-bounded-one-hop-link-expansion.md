# ADR 0006: Bounded one-hop link expansion

Status: accepted

## Context

Configured seeds, sitemaps, RSS, and Atom provide useful focused discovery but
can omit closely related pages. Fetchmark already extracts a bounded ordered
set of canonical reader-view links while the API process performs the fresh
SSRF, robots, noindex/noarchive, extraction, retention, and takedown checks.
Moving page bodies or crawling logic into the separate worker would duplicate
that security boundary. Treating discovered links as explicit seeds would lose
provenance and make operator policy changes impossible to reconcile safely.

The feature must not turn focused ingestion into a recursive crawler, silently
activate third-party targets, or let a partial run-level budget replace a
previous complete snapshot.

## Decision

Crawler configuration version 3 adds a separate `max_link_edges` lifetime cap
and an optional per-job `link_expansion` block with `max_links_per_page` and
`max_pages_per_run`. The page budget decides which directly owned parents ask
for a complete snapshot during one run. It does not truncate a snapshot after
retrieval.

The existing focused-admission request may ask for `max_outbound_links` in the
range `0..64`. A successful admitted response then includes `outbound_links`:
omission means no snapshot was requested, while a present empty array is an
authoritative empty snapshot. Ordinary admissions, failures, and rejections do
not expose link metadata. The field never contains bodies, snippets, headings,
anchor text, or safety assertions. The fixed-origin client revalidates count,
uniqueness, canonical HTTP(S) form, URL length, and absence of credentials.

The crawler requests a snapshot only for a page independently owned by an
explicit seed or a currently reachable sitemap/feed membership. Link-only
children use the normal admission and refresh path but cannot publish their own
snapshot. A later explicit or source ownership can make that same page an
eligible parent. Every returned URL is rechecked against the job's explicit
deny-first domain/path policy. Self-links, per-anchor `rel=nofollow`, and
applicable `X-Robots-Tag` or HTML robots `nofollow` directives are excluded.

Frontier schema v6 adds exact parent-to-child and child-to-parent indexes plus
persisted per-job link policy. A transactional v5-to-v6 migration preserves
page/source scheduling, source snapshots, lease generations, host pacing, and
authority references before advancing the schema marker.

`CompleteWithLinks` verifies the current lease generation and direct ownership,
then exact-replaces the link snapshot, creates/reactivates affected children,
updates both indexes, transitions the parent, and reconciles scheduling in one
bbolt transaction. Stale leases, identity conflicts, entry exhaustion, edge
exhaustion, and corruption roll back the whole mutation. An admitted snapshot
replaces links, including with empty. An authoritative policy rejection clears
links. Transport, protocol, storage, cancellation, and exhausted-attempt
failures use ordinary completion and preserve the previous snapshot.

Historical edges become inactive when their parent loses direct ownership and
reactivate if direct ownership returns. Shared children survive while another
direct owner or active link parent remains. Pruning cannot leave dangling link
relationships.

Developer, research, news, and knowledge “packs” are complete version-3 example
files under `deploy/crawler-packs/`. They contain only `example.invalid`
targets. Fetchmark never merges or enables them automatically; operators must
copy, edit, and review real endpoints, robots policy, terms, licenses, paths,
poll intervals, and budgets.

## Consequences

- Focused instances gain useful local recall without a paid search API or
  per-query cost.
- Expansion remains one hop and separately operated; ordinary API requests and
  installations do no crawler work.
- Fetchmark's API process remains the only page-fetch and retention authority.
- Complete snapshot and reverse-index semantics cost additional bounded
  frontier state, covered by an independent global edge cap.
- Reader-view and nofollow filtering intentionally trade recall for a safer,
  less boilerplate-driven first implementation.
- Recursive crawling, cross-origin policy inference, compiled third-party
  target presets, WebSub, and whole-web coverage remain out of scope.
