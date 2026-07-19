# Common Crawl URL Index real-corpus selection — 2026-07-19

This is the first complete real-corpus run of Fetchmark's bounded offline URL
Index selector. It proves that one exact current Common Crawl Parquet part can
be fully hashed, schema-checked, page-preflighted, decoded, filtered, and
reduced to a deterministic evidence-collector input. It does not prove current
robots/indexing permission, redistribution rights, pack publication, or
commercial-index coverage parity.

## Pinned source

- Latest crawl at run time: `CC-MAIN-2026-25`.
- Official path manifest:
  `https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-25/cc-index-table.paths.gz`
- Path-manifest SHA-256:
  `4813551cd94d27104119b0d27f651feccdebf01fa0b5dcec322f8ad64e37202a`
- Selected WARC URL Index part:
  `cc-index/table/cc-main/warc/crawl=CC-MAIN-2026-25/subset=warc/part-00000-b13edba3-e431-43c6-8915-a9f1c955272b.c000.gz.parquet`
- HTTPS URI:
  `https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2026-25/subset=warc/part-00000-b13edba3-e431-43c6-8915-a9f1c955272b.c000.gz.parquet`
- Content length: `554,884,713` bytes.
- ETag: `b11056134d1eba62df2142f10f4a2e2f-9`.
- Last-Modified: `2026-06-19T07:52:58Z`.
- Complete downloaded-object SHA-256:
  `587124b0e999954ed06be5252442993231b6e65ac1ce973e8d36217980fdf25f`.

Common Crawl's current documentation identifies this bulk analytical dataset
as the URL Index and the official June 2026 crawl announcement records the
`CC-MAIN-2026-25` release. The terms still require an operator review: crawl
availability alone is not a license for underlying third-party content.

## Command

```sh
fetchmark-pack-build select-parquet \
  -input /publisher/raw/cc-main-2026-25-warc-part-00000.parquet \
  -out /publisher/work/cc-main-2026-25-selected-100.jsonl \
  -max-records 100 \
  -languages eng \
  -not-after 2026-06-19T00:00:00Z
```

The cutoff is an explicit operator policy for this pinned crawl, not a clock-
derived default. The selector's domain-separated canonical-URL hash ranking
allows at most one selected row per host. It reads the complete input and
rehashes it after decoding before atomically activating the output.

## Result

| Measure | First run | Repeat run |
| --- | ---: | ---: |
| Wall time | 21.26 s | 20.72 s |
| Source rows | 6,890,137 | 6,890,137 |
| Eligible rows | 411,537 | 411,537 |
| Selected rows | 100 | 100 |
| Eligible but not selected | 411,437 | 411,437 |
| Output bytes | 52,134 | 52,134 |
| Output SHA-256 | `dd73cadfa8c8b071ac0349ced2708dae15ab074c4b5bd4b7d595cb4022d068cd` | same |

The two independently activated outputs were byte-identical under `cmp` and
their complete reports were identical apart from output path.

Rejection accounting from both runs:

| Reason | Rows |
| --- | ---: |
| `language_not_selected` | 5,513,488 |
| `query_not_allowed` | 802,939 |
| `language_invalid` | 112,128 |
| `mime_not_html` | 49,998 |
| `non_public_url` | 45 |
| `noncanonical_url` | 2 |

The first attempt also exposed a real schema-compatibility issue: this Spark
output uses plain physical `INT32` for WARC offsets and lengths, while the
initial fixture used the signed-32 logical annotation. Fetchmark now accepts
only those two explicit compatible encodings, has a custom-schema regression
test for the physical form, and continues to reject other type or repetition
changes.

## Remaining gate

These 100 rows contain historical capture metadata only. Before any pack can
be described as publishable, a real operator must provide a working contact,
review the exact URL-metadata rights basis and takedown policy, run the separate
public-only evidence collector, inspect its rejection outcomes, build twice
within the evidence window, and pass independent `verify-run` consumer
activation. Page bodies remain outside the proposed pack.

The subsequent
[two-part real-corpus run](common-crawl-url-index-multipart-selection-20260719.md)
proved global selection and repeatability across adjacent raw parts and exposed
the much lower unique-host density that governs million-row planning.
