# Vendor compatibility

Fetchmark exposes explicit compatibility prefixes so Tavily and Exa can both
retain their `POST /search` path without ambiguous body sniffing. Configure the
vendor client with the base URL shown below; use a key from `FM_API_KEYS`.

| Client | Fetchmark base URL | Authentication |
| --- | --- | --- |
| Tavily | `http://host:8080/compat/tavily` | `Authorization: Bearer <key>` |
| Exa | `http://host:8080/compat/exa` | `x-api-key: <key>` or Bearer |
| Brave | `http://host:8080/compat/brave` | `X-Subscription-Token: <key>` |

The goal is protocol compatibility for an honest open-discovery subset, not a
claim that a self-hosted node has the same proprietary index or semantic
models. Unknown fields and controls whose semantics cannot be preserved are
rejected rather than silently ignored.

Compatibility response bodies do not gain Fetchmark-specific attribution
fields. When a returned page contains Wiby, arXiv, Stack Overflow, or PubMed
results, Fetchmark uses HTTP `Link` relations to retain required source evidence and,
for Stack Overflow content, exact license evidence. arXiv's HTTP relation is
only its requested acknowledgment; CC0 identity stays scoped to native result
metadata and does not license compatibility responses, papers, or PDFs.
PubMed relations identify NLM as the source and expose its terms/disclaimer;
they do not license abstracts or linked articles.
Applications displaying Stack Exchange API results must still visibly identify
Stack Overflow as their source; the compatibility envelope cannot enforce UI
rendering on their behalf.

## Tavily `POST /search`

| Field | Status | Fetchmark behavior |
| --- | --- | --- |
| `query`, `max_results` | Supported | `max_results` is 1–20 and also subject to `FM_RESULTS_CAP`. |
| `search_depth` | Supported | `basic`, `advanced`, `fast`, and `ultra-fast` map to Fetchmark retrieval breadth. |
| `chunks_per_source` | Supported | Up to three locally selected passages. |
| `topic` | Partial | `general` and `news`; `finance` is rejected. |
| `time_range` | Partial | Day, month, and year; week and explicit date windows are rejected. |
| domain filters, `exact_match`, `safe_search` | Supported | Mapped to the canonical search request. |
| `include_raw_content` | Supported | Boolean/`markdown` or `text`. |
| `include_answer: true` / `basic` | Supported | Deterministic extractive answer with `[n]` citations; no model required. |
| `include_answer: advanced` | Optional | Uses only the summarizer profile named by `FM_COMPAT_ANSWER_PROVIDER`; returns 503 when absent. |
| images, favicon, country, auto parameters | Unsupported | Rejected with a Tavily-shaped 400 response. |
| usage | Supported | Reports zero Fetchmark credits because Fetchmark has no per-query toll. An explicitly configured answer provider may have separate operator costs. |

## Exa `POST /search` and `POST /contents`

| Field | Status | Fetchmark behavior |
| --- | --- | --- |
| `query`, `numResults`, domain filters | Supported | `numResults` is 1–100 and subject to `FM_RESULTS_CAP`. |
| `type` | Partial | Omitted or `auto`; proprietary semantic/deep modes are rejected. |
| `contents.text` | Supported | Boolean or `{maxCharacters}`. |
| `contents.highlights` | Partial | Boolean or `{maxCharacters}` uses search-query chunks; a distinct `query` is rejected. |
| `/contents` `urls` | Supported | Fetches and extracts absolute HTTP(S) URLs, subject to `FM_RESULTS_CAP` (default 50). Highlights require `{query,...}`. |
| `/contents` `ids` | Partial | Accepted only when each ID is itself an absolute HTTP(S) URL (`id=url` in search results). |
| per-item statuses | Supported | Reports `success` with `cached`/`crawled`, or an item-level extraction error. |
| category/date/context/moderation/schema/stream/summary/subpages/extras | Unsupported | Rejected explicitly. |

Exa error responses follow the current public Exa error envelope. Except for
rate limits, errors contain a non-empty `requestId`, an `error` message, and a
machine-readable `tag`:

| Condition | HTTP | Tag / shape |
| --- | --- | --- |
| Missing, empty, or invalid key | 401 | `INVALID_API_KEY` |
| Malformed JSON, missing required fields, unsupported fields, or invalid parameter values | 400 | `INVALID_REQUEST_BODY` |
| Conflicting controls, such as both `ids` and `urls`, or a highlights query that conflicts with the search query | 400 | `INVALID_REQUEST` |
| A non-HTTP(S), relative, or otherwise invalid content URL/ID | 400 | `INVALID_URLS` |
| `numResults` outside the supported Exa range of 1–100 | 400 | `INVALID_NUM_RESULTS` |
| A valid `numResults` above this instance's `FM_RESULTS_CAP` | 400 | `NUM_RESULTS_EXCEEDED` |
| Rate limit exceeded | 429 | Exactly `{"error":"rate limit exceeded"}`; no `requestId` or `tag` |

Fetchmark additionally returns HTTP 507 with tag `RESPONSE_TOO_LARGE` when the
operator's response-byte budget cannot contain the result. This is an explicit
Fetchmark extension, not an Exa status or tag. Clients that exhaustively model
Exa's published statuses must handle it as an extension.

## Brave `GET /res/v1/web/search`

| Parameter | Status | Fetchmark behavior |
| --- | --- | --- |
| `q`, `count`, `offset` | Supported | Count is 1–20; offset is 0–9; the requested page must fit `FM_RESULTS_CAP`. |
| `search_lang` | Supported | Valid Brave language values map to the canonical control; default is `en`. |
| `safesearch` | Supported | `off`, `moderate`, or `strict`; default is `moderate`. |
| `freshness` | Partial | `pd`, `pm`, and `py`; week/custom date ranges are rejected. |
| `spellcheck` | Accepted | Syntax is validated; Fetchmark does not rewrite the query. |
| country/UI locale/decorations/goggles/result filters/extra snippets/summary | Unsupported | Rejected explicitly. |

The native arXiv, PubMed, and YaCy lanes cannot contribute to Brave requests.
Their upstream contracts cannot preserve Brave's default `search_lang=en` and
`safesearch=moderate` semantics, so those lanes return typed unsupported-control
outcomes rather than silently weakening the request. Other configured discovery
lanes continue independently; a configuration containing only these sources
therefore cannot serve Brave search.

Compatibility behavior follows the public vendor contracts as of this
implementation: [Tavily Search](https://docs.tavily.com/documentation/api-reference/endpoint/search),
[Exa Search](https://exa.ai/docs/reference/search),
[Exa Contents](https://exa.ai/docs/reference/get-contents),
[Exa Error Codes](https://exa.ai/docs/reference/error-codes), and
[Brave Web Search](https://api-dashboard.search.brave.com/api-reference/web/search/get).

## Official client smoke test

An opt-in loopback test exercises Fetchmark's real router with pinned current
clients. It makes no request to a vendor service and needs no vendor credential.

| Surface | Pinned client | Covered call |
| --- | --- | --- |
| Tavily | `tavily-python==0.7.26` | `TavilyClient.search` with `api_base_url` |
| Exa | `exa-py==2.16.0` | `Exa.search` and `Exa.get_contents` with `base_url` |
| Brave | Official Python `requests` pattern | `GET /res/v1/web/search` with `X-Subscription-Token` |

Run it in an isolated environment:

```sh
python3 -m venv /tmp/fetchmark-compat-sdk
/tmp/fetchmark-compat-sdk/bin/python -m pip install \
  -r internal/api/testdata/compat-sdk-requirements.txt
make compat-sdk-check \
  COMPAT_SDK_PYTHON=/tmp/fetchmark-compat-sdk/bin/python
```

The test is excluded from ordinary Go runs because installing third-party SDKs
is an explicit integration action. It rejects installed SDK versions that do
not exactly match the requirements file before issuing a client request. The
pinned versions are a verified snapshot,
not a claim that future SDK releases will remain wire-compatible without a
rerun and deliberate version update.

## Optional advanced answers

`include_answer: "advanced"` never selects a paid provider implicitly. To
enable it, configure any existing local/OpenAI-compatible or Anthropic-compatible
summarizer profile and name it explicitly:

```dotenv
FM_SUMMARIZE_OPENAI_BASE_URL=http://host.docker.internal:11434/v1/
FM_SUMMARIZE_OPENAI_API_KEY=local
FM_SUMMARIZE_OPENAI_MODEL=qwen3:8b
FM_COMPAT_ANSWER_PROVIDER=openai
FM_COMPAT_ANSWER_MAX_TOKENS=512
FM_COMPAT_ANSWER_TIMEOUT=30s
```

The provider receives a bounded set of top results as untrusted source data and
is instructed to answer only from those results with inline citations.
