# Private trusted index federation

Fetchmark federation is an optional exchange of discovery URLs between
operators who already trust one another. It federates explicitly curated local
indexes; it is not an HTTP proxy, a remote page fetcher, a content-sharing
network, or automatic public peer discovery. It is disabled unless an operator
configures secure identity/trust files and explicitly adds a `federation`
source to a source pack.

The protocol and threat boundary are fixed by
[ADR 0008](../../../adr/0008-private-trusted-index-federation.md).

## Disclosure and retention boundary

An outbound request discloses the exact search query and requested result limit
to the selected peer. A successful peer response contains only up to 20
canonical HTTP(S) URLs. It never contains titles, snippets, passages, bodies,
scores, local provenance, link graphs, or the peer's internal document IDs.

The inbound listener searches only `FM_LOCAL_CORPUS_MODE=curated` data and
forces safe search. Consequently only documents explicitly admitted with the
operator safety assertion `safe` can leave the node. Ordinary searches do not
populate curated mode. Robots, X-Robots-Tag, HTML noindex/noarchive, takedown,
expiry, and local revocation rules continue to control what is searchable.

A receiving node treats peer output as discovery hints only. Fetchmark performs
its normal live canonicalization, SSRF checks, RFC 9309 policy, fetch,
noindex/X-Robots-Tag checks, extraction, deduplication, and ranking before a URL
can become a public result. Federation never authorizes retention or bypasses a
site's current policy.

Public native results identify this lane only as provider `federation`, lane
`federation`. Trusted peer binding IDs and peer-specific source-pack lane IDs
remain private routing data and are not serialized in result metadata or typed
provenance. Tavily, Exa, and Brave compatibility responses omit Fetchmark
provenance entirely.

## Transport topology

The outbound adapter requires an exact HTTPS origin, rejects private/special
addresses at validation and dial time for public-peer bindings, disables
redirects, compression, ambient proxies, cookies, and retries, and verifies the
signed response against the exact signed request. Private or special-use
network origins require the explicit `allow_private_network` trust flag. That
flag deliberately permits any resolved address for the exact configured TLS
hostname; hostname allowlisting, normal certificate verification, signed
response verification, and redirect refusal still apply.

`FM_FEDERATION_LISTEN_ADDR` starts a separate plain-HTTP server containing only:

```text
POST /federation/v1/index/search
```

Place that listener behind an operator-managed private ingress which terminates
valid TLS, forwards only this path, preserves request bytes, and is reachable
only by the intended peers. Apply connection, header, body, request-rate, and
request-time limits at that ingress as the first pre-authentication boundary.
Do not publish it as the public Fetchmark API. The
Compose files pass through configuration but intentionally mount no key files,
publish no federation port, and configure no TLS ingress.

Requests and responses use strict canonical JSON envelopes, Ed25519 signatures,
SHA-256 body binding, 16-byte random nonces, whole-second UTC timestamps, exact
sender/audience matching, and exact request/response binding. The listener
allows 60 seconds of clock skew, keeps up to 16,384 entries in a five-minute
replay cache, caps each trusted peer at 2 requests/second with burst 4 and one
concurrent search, caps signature verification at 16 concurrent requests, and
permits four concurrent local searches across peers with a two-second search
timeout. These listener limits are fixed conservative defaults in the first
implementation.

## Generate a node identity

Build the binaries and create the identity in a new operator-owned directory:

```bash
make build
install -d -m 700 /absolute/path/to/fetchmark-federation
./bin/fetchmark-peer keygen \
  -identity-id node-a \
  -out /absolute/path/to/fetchmark-federation/identity.json \
  > /absolute/path/to/fetchmark-federation/public-identity.json
```

`fetchmark-peer` refuses relative or unclean paths and refuses to overwrite an
existing file. The private identity is created as mode `0600`; the command
prints only the shareable public identity entry. Transfer that public entry to
the peer through an authenticated out-of-band channel. Never copy or log the
private identity file.

The server also refuses a private identity that is not process-owned mode
`0600`. The trust registry must be process-owned and not group/world writable.
The two files must use distinct absolute paths.

## Configure one peer

Create a strict trust registry containing the remote public entry and one exact
peer binding. All booleans are required:

```json
{
  "version": 1,
  "identities": [
    {
      "id": "node-b",
      "key_id": "64-lowercase-hex-key-id-from-node-b",
      "ed25519_public_key": "canonical-padded-base64-public-key-from-node-b"
    }
  ],
  "peers": [
    {
      "id": "peer-b",
      "identity_id": "node-b",
      "base_origin": "https://peer-b.example",
      "allow_private_network": false,
      "allow_inbound": true
    }
  ]
}
```

`allow_inbound` grants that remote identity permission to query this node. Set
it to `false` for an outbound-only relationship. `allow_private_network` permits
the exact TLS hostname to resolve to private/special-use addresses; it does not
permit private result URLs, redirects, invalid TLS, or a different hostname.
Each identity is bound exactly once, and a registry contains at most 16
identities and 16 peer bindings.

Enable outbound discovery with a custom source-pack file. The federation source
ID must exactly match the peer binding ID (`peer-b` here), and the lane must use
only the `original` variant because the remote contract has no language,
time-range, domain, category, engine, or exact-match controls:

```json
{
  "version": 1,
  "sources": [
    {
      "id": "searxng",
      "kind": "searxng",
      "weight": 1,
      "max_results": 50,
      "timeout_ms": 30000,
      "max_concurrency": 4,
      "rate_per_second": 10,
      "burst": 4
    },
    {
      "id": "peer-b",
      "kind": "federation",
      "weight": 0.8,
      "max_results": 10,
      "timeout_ms": 2000,
      "max_concurrency": 1,
      "rate_per_second": 1,
      "burst": 1
    }
  ],
  "packs": [
    {
      "id": "general-open",
      "always": true,
      "sources": [
        {"id": "searxng-default", "source": "searxng", "variants": ["original"]},
        {"id": "peer-b-original", "source": "peer-b", "variants": ["original"]}
      ]
    }
  ]
}
```

Then configure the process. The listener is optional for an outbound-only node:

```bash
export FM_FEDERATION_IDENTITY_FILE=/absolute/path/to/fetchmark-federation/identity.json
export FM_FEDERATION_TRUST_REGISTRY_FILE=/absolute/path/to/fetchmark-federation/trust.json
export FM_DISCOVERY_PACK_FILE=/absolute/path/to/fetchmark-federation/discovery.json
export FM_DISCOVERY_ENABLED_PACKS=general-open
export FM_DISCOVERY_ENABLED_SOURCES=searxng,peer-b

# Inbound only: curated mode and a listener distinct from FM_LISTEN_ADDR.
export FM_LOCAL_CORPUS_MODE=curated
export FM_LOCAL_INDEX_PATH=/absolute/path/to/fetchmark-data/index
export FM_LOCAL_ARTIFACT_PATH=/absolute/path/to/fetchmark-data/artifacts
export FM_ADMIN_API_KEYS=replace-with-a-generated-admin-key
export FM_RESPECT_ROBOTS=true
export FM_FEDERATION_LISTEN_ADDR=127.0.0.1:8082
```

For the bundled Compose file, add an operator-owned override rather than
publishing the listener directly:

```yaml
services:
  fetchmark:
    volumes:
      - ./federation:/run/secrets/fetchmark-federation:ro
    environment:
      FM_FEDERATION_IDENTITY_FILE: /run/secrets/fetchmark-federation/identity.json
      FM_FEDERATION_TRUST_REGISTRY_FILE: /run/secrets/fetchmark-federation/trust.json
      FM_DISCOVERY_PACK_FILE: /run/secrets/fetchmark-federation/discovery.json
      FM_DISCOVERY_ENABLED_PACKS: general-open
      FM_DISCOVERY_ENABLED_SOURCES: searxng,peer-b
      FM_LOCAL_CORPUS_MODE: curated
      FM_LOCAL_INDEX_PATH: /var/lib/fetchmark/index
      FM_LOCAL_ARTIFACT_PATH: /var/lib/fetchmark/artifacts
      FM_ADMIN_API_KEYS: "${FM_ADMIN_API_KEYS:?set FM_ADMIN_API_KEYS}"
      FM_RESPECT_ROBOTS: "true"
      FM_FEDERATION_LISTEN_ADDR: 0.0.0.0:8082
```

On Linux, the mounted identity must be owned by the image's nonroot UID/GID
`65532:65532` and mode `0600`; trust and discovery files must not be
group/world writable. Add a private TLS-ingress service to the same Compose
network and forward only the exact route to `fetchmark:8082`. Do not add a host
`ports` mapping for 8082.

An enabled federation lane containing the `original` variant participates in
both basic and advanced search, so the caller's query text is disclosed to that
explicitly trusted peer. Omit `original` from the peer lane to keep it out of
basic search. A caller's safe-search control is accepted because the remote
result set is always stricter. Other unsupported controls fail only that
federation lane, allowing compatible local/open lanes to continue.

## Operations, revocation, and rotation

Monitor:

- `fetchmark_federation_request_total{direction,peer,outcome}`
- `fetchmark_federation_request_duration_seconds{direction,peer}`

Peer labels come only from the bounded operator trust registry; traffic which
cannot authenticate aggregates under `untrusted`. Queries, URLs, identity IDs,
nonces, signatures, and raw error text are never metric labels. Generic unsigned
error bodies intentionally do not reveal which verification step failed.

To revoke a peer, remove its identity and binding from the trust registry,
remove/disable its discovery source where applicable, and restart Fetchmark.
The first version loads identity and trust files only at startup. Key rotation
is likewise an authenticated out-of-band exchange followed by coordinated file
replacement and restart; there is no automatic key discovery, overlapping-key
rotation, or in-process reload yet.

Operators remain responsible for peer consent, query-data handling, retention,
logging, takedown contact, TLS certificate lifecycle, clock synchronization,
and private-ingress access controls. This private allowlist is not a claim of
commercial-index parity and must not be exposed as anonymous public federation.
