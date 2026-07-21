# Common Crawl URL Index Parquet metadata probe — 2026-07-19

This bounded live probe checked the direct Parquet reader's initial resource
and schema assumptions against one official Common Crawl WARC URL Index part.
It used HTTP range requests for the header/footer metadata and did not download
or normalize the complete object.

## Exact object

- Crawl: `CC-MAIN-2024-22`
- Published path source:
  `crawl-data/CC-MAIN-2024-22/cc-index-table.paths.gz`
- Object:
  `cc-index/table/cc-main/warc/crawl=CC-MAIN-2024-22/subset=warc/part-00000-4dd72944-e9c0-41a1-9026-dfd2d0615bf2.c000.gz.parquet`
- HTTPS URI:
  `https://data.commoncrawl.org/cc-index/table/cc-main/warc/crawl=CC-MAIN-2024-22/subset=warc/part-00000-4dd72944-e9c0-41a1-9026-dfd2d0615bf2.c000.gz.parquet`
- `Content-Length`: `1,607,042,706` bytes
- ETag: `9ef011e085f00a093d8a069642e07f0d-24`
- Last-Modified: `2024-05-31T14:07:21Z`
- Parquet footer length: `57,995` bytes

## Parsed metadata

| Measure | Observed |
| --- | ---: |
| Rows | 20,522,545 |
| Row groups | 12 |
| Leaf columns | 30 |
| `fetch_time` | `TIMESTAMP(isAdjustedToUTC=true,unit=MILLIS)` |
| `fetch_time` definition level | 0 |
| Largest compressed column chunk | 40,569,429 bytes |
| Largest uncompressed column chunk | 192,343,532 bytes |

The physical timestamp matches Common Crawl's current official converter
setting, `spark.sql.parquet.outputTimestampType=TIMESTAMP_MILLIS`. The reader
therefore treats this as the primary encoding. Explicit legacy `INT96` support
is separate and never depends on Parquet library coercion.

The observed object is within the reader's 4-GiB input, 16-MiB footer,
50-million-row, 100,000-row-group, 256-column, and 512-MiB column-chunk limits.
The 4-GiB input ceiling retains headroom because this first measured part is
already about 1.50 GiB.

## Not proved by this probe

- No complete Parquet object was downloaded or decoded.
- Column pages and nullable row values were not inspected.
- No normalized candidate, live evidence, signed pack, repeat build, or public
  consumer activation was produced.
- One part does not establish the maximum size or schema across every crawl.

The digest-pinned full-object selection gate was subsequently completed against
a current `CC-MAIN-2026-25` part and is recorded in the
[real-corpus selection run](common-crawl-url-index-selection-20260719.md). The
separate live collector and repeatable build/verification run remain pending.
