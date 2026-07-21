# Common Crawl URL Index multi-part real-corpus selection — 2026-07-19

This is the first retained run of Fetchmark's direct multi-part Parquet
selector against two exact adjacent WARC URL Index parts. It proves global
one-per-host selection, complete input binding, and input-order-independent
output on real Common Crawl metadata. It does not claim a million selected
records, current robots/noindex permission, redistribution rights, pack
publication, or commercial-index parity.

## Pinned inputs

- Crawl: `CC-MAIN-2026-25`.
- Official path manifest SHA-256:
  `4813551cd94d27104119b0d27f651feccdebf01fa0b5dcec322f8ad64e37202a`.
- `subset=warc/part-00000-...parquet`: 554,884,713 bytes, 6,890,137
  rows, SHA-256
  `587124b0e999954ed06be5252442993231b6e65ac1ce973e8d36217980fdf25f`.
- `subset=warc/part-00001-...parquet`: 543,397,863 bytes, 6,543,291
  rows, SHA-256
  `20a65fb8a008de9667d090e6df843ad2f65775e0746dc2a13adaf3eba3e2ca05`.
- Aggregate input identity:
  `4021971da5029e6a8f9a5eaa8dc3beaa9a1a4653787505faafb1a4cb98b79898`.
- Exact builder executable: 31,801,314 bytes, SHA-256
  `cea464b84901eb8d273c7f49c96d5cddfc92925495a3856f1878a1d6b4b4679e`.

The path manifest and both parts came from the official
`data.commoncrawl.org` origin. The second part reported content length
543,397,863, ETag `e199b5ada411bbe2e5e45a0e2b38b491-9`, and
Last-Modified `2026-06-19T07:48:46Z` before download. Complete local SHA-256,
not ETag, is the artifact identity.

## Command and repeat

```sh
fetchmark-pack-build select-parquet-parts \
  -input /publisher/raw/part-00000.parquet \
  -input /publisher/raw/part-00001.parquet \
  -out /publisher/work/selected-forward.jsonl \
  -profile format-prototype-v1 \
  -max-records 1000000 \
  -languages eng \
  -not-after 2026-06-19T00:00:00Z
```

The repeat used the same command with the two `-input` values reversed and a
fresh output path. `cmp` succeeded. Both outputs contained exactly 66,576
lines and 30,515,106 bytes with SHA-256
`6fb2f0be14c5de460af8c5206f9d8be8fc92f461a22a79a803efbe7b075a4e31`.
The complete reports were identical; wall times were 48.44 and 47.60 seconds.

The exact schema-versioned run records are retained in
[`artifacts/common-crawl-url-index-multipart-selection-20260719.jsonl`](artifacts/common-crawl-url-index-multipart-selection-20260719.jsonl).

## Corpus-shape result

| Measure | Count |
| --- | ---: |
| Source rows | 13,433,428 |
| Capture-eligible rows | 3,556,637 |
| Globally selected unique hosts | 66,576 |
| Eligible but not selected | 3,490,061 |
| Eligible / source | 26.4760% |
| Selected / eligible | 1.8719% |
| Selected / source | 0.4956% |
| Source rows per selected host | 201.78 |

Rejections were 6,787,120 unselected-language rows, 2,707,258 query URLs,
213,455 invalid-language rows, 168,878 non-HTML rows, 76 non-public URLs, and
4 noncanonical URLs.

The one-million maximum did not bind. Adjacent URL Index parts contain many
captures per host, so the one-per-host policy—not heap capacity—limited this
run. At the observed 201.78 source rows per selected host, one million hosts
would require roughly 202 million source rows and five million roughly 1.01
billion. This observation motivated the explicit prototype-only two-billion
source-row ceiling. The ordinary collector profile remains capped at 50
million source rows and 10,000 selected rows.

## Remaining gate

This run proves the multi-part selection machinery and gives an evidence-based
scale estimate. It does not justify retaining or publishing all 66,576 rows.
The next gate is a larger quality-aware part selection approaching one million
unique hosts, followed by an operator-reviewed current robots/noindex and
URL-metadata rights strategy that does not weaken the 10,000-row lightweight
path. Targeted partition/range retrieval is needed to avoid blindly
downloading hundreds of gigabytes.
