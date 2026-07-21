# ADR 0008: private trusted index federation

- Status: Accepted
- Date: 2026-07-18

## Context

Fetchmark can already fuse provider-aware discovery lanes and improve an
operator-owned local corpus through permitted live retrieval. Some operators
may want a small private mesh to share discovery coverage without routing web
traffic through another machine or depending on a hosted search service.

Federation changes the privacy and trust boundary: a query is disclosed to a
peer, peer results can be malicious, request replay can consume resources, and
ordinary API authentication does not establish a node identity. A peer must
not become an arbitrary proxy, recursively query other peers, export retained
page bodies, or bypass Fetchmark's live retrieval policy.

## Decision

The first federation protocol is an optional, private, allowlisted exchange of
compact index candidates. It is disabled by default and is not part of basic
search or vendor-compatibility routes unless an operator explicitly adds a
peer source to an advanced discovery pack.

### Export boundary

Inbound federation uses a separate listener with one endpoint:

```text
POST /federation/v1/index/search
```

The handler receives only a curated local lexical index. It cannot access the
pipeline, fetcher, discovery planner, SearXNG, another peer, artifacts, or a
crawler. Personal, ephemeral, archive, disabled, and absent local-corpus modes
cannot serve federation results.

Responses contain only canonical URLs in rank order. The receiver exports only
records explicitly classified as safe by the curated local index. Responses
omit title, snippet, body, Markdown, HTML, passages, headings, links, author,
score, provenance history, timestamps, content hashes, safety metadata, and
arbitrary metadata. Publisher-derived fields may be considered later only with
an explicit rights and privacy policy.

Received candidates remain untrusted discovery hints. The requesting node
still performs its ordinary SSRF checks, RFC 9309 handling,
noindex/X-Robots-Tag enforcement, live fetch, extraction, deduplication, and
reranking before returning content.

### Identity and message integrity

Each node has a dedicated Ed25519 identity independent of API keys and pack
publisher keys. Operators exchange public keys and exact peer bindings out of
band. Requests and responses are signed over domain-separated messages that
bind the protocol version, sender, audience, method and exact path, timestamp,
nonce, status where applicable, request identity, and SHA-256 of the exact
body bytes.

Requests are accepted only within 60 seconds of the local clock and reserve a
peer/nonce tuple for five minutes in a bounded replay table. Duplicate nonces
fail. A full replay table fails closed. The first implementation is
single-process; a restart retains only the narrow timestamp window. Multiple
inbound replicas require shared atomic replay state before they can be
advertised as supported.

Protocol-v1 success responses are signed and bound to the exact request.
Rejections use a small, generic, unsigned JSON error and disclose no peer,
query, URL, nonce, signature, key, or implementation detail. Authenticated
machine-readable error envelopes and retry metadata require a later protocol
revision; clients must treat every non-200 response as untrusted scheduling
information rather than a signed peer assertion.

Identity and trust files use the same hardened local-file boundary as signed
pack registries: clean absolute paths, no arbitrary symlink components,
effective-user ownership, safe parent directories, stable reads, and strict
write permissions. Private identity material requires mode 0600 and is never
included in errors or logs.

### Transport and budgets

Peer origins are exact HTTPS origins with no credentials, path, query, or
fragment. TLS verification is mandatory and redirects are refused. Private
network destinations require an explicit per-binding operator opt-in; this
supports a private mesh without making a peer or the operator's Mac an exit
node. There is no insecure-TLS switch.

Initial protocol caps are:

- 32 KiB strict JSON request and 128 KiB strict JSON response;
- query at most 1,024 UTF-8 bytes;
- at most 20 results, with no remote domain filters or provider controls;
- inbound two requests/second per peer, burst four, and four global searches;
- one concurrent local search per peer, sixteen concurrent signature checks,
  and a 16,384-entry five-minute replay table sized above the 16-peer maximum;
- two-second local search budget and five-second server read/write headers;
- no compression, retry loop, pagination, passages, or answers.

Existing discovery source configuration continues to own outbound result,
timeout, rate, burst, concurrency, and source-weight budgets. Healthy peer
results may use the ordinary bounded discovery cache because final content is
always revalidated live.

### Privacy and operations

Enabling a peer source discloses search text and the requested result limit to
that trusted peer. Fetchmark does not forward caller API keys, client addresses,
cookies, headers, local scores, or retention metadata. Metrics and logs use
only fixed outcomes and trusted peer IDs; queries, URLs, nonces, signatures,
and bodies never become labels.

Operators must put the separate listener behind a TLS reverse proxy or an
encrypted private ingress, firewall it to expected peers, exchange keys out of
band, and remove a binding to revoke a peer. Public discovery, automatic peer
discovery, reputation, Sybil resistance, threshold trust, and transparent key
rotation remain future work.

## Consequences

- Federation improves coverage by sharing owned index results, not bandwidth
  or arbitrary egress.
- The smallest useful response is privacy- and licensing-conservative, but it
  gives local ranking less context than a commercial search response.
- Ed25519 authenticates a configured peer and exact message; it does not prove
  that a result is accurate, safe, fresh, licensed, or non-spam.
- The curated-only export rule prevents automatically retained personal or
  archive data from silently becoming shared infrastructure.
- Separate listeners and dependencies make recursive search and proxy behavior
  structurally unavailable to the inbound handler.
- The feature requires no paid API, hosted control plane, embeddings, local
  model, public proxy list, CAPTCHA mechanism, or broad crawler.

## Explicit non-goals

- Arbitrary HTTP proxying, peer-assisted fetching, exit-node behavior, or
  forwarding a request to another peer.
- Public/open federation, automatic peer discovery, anonymous access, or
  commercial-index parity claims.
- Bodies, cached artifacts, passages, titles, snippets, embeddings, or model
  output in the first protocol.
- Reusing vendor/API keys, pack signing keys, or remotely discovered keys as
  federation identities.
- Insecure TLS, redirects, compression, retries, streaming, pagination, or
  multi-replica replay coordination in the first slice.
