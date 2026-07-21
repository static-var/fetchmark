# Focused-ingestion templates

These strict version-3 configurations are operator-owned starting points, not
built-in crawler targets. Copy a template, replace every `example.invalid`
origin and the contact URI, then review the target's robots policy, terms,
license, paths, polling interval, and budgets before enabling `-once`.

The named templates describe workload shapes only:

- `developer`: documentation seed plus sitemap, with conservative one-hop links.
- `research`: Atom/RSS publication feed into an allowlisted paper path.
- `news`: a frequently polled feed with a low per-run page budget.
- `knowledge`: a sitemap-led topic corpus with slow refreshes.

Fetchmark never activates these files automatically. Link expansion remains
one hop: only explicit seeds or pages currently owned by a sitemap/feed may
publish a link snapshot. Every discovered URL still passes the job's explicit
domain/path policy and the normal SSRF, robots, noindex, noarchive, extraction,
retention, and takedown controls.
