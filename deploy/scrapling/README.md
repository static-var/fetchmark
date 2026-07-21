# Fetchmark Scrapling sidecar

This service uses Scrapling 0.4.11 with one persistent, headless Chromium
profile and a bounded asynchronous page pool. `POST /v1/search` accepts only a
query, a compiled engine allowlist, and a bounded result count. Google and
DuckDuckGo are the default engines and are visited concurrently.

Using the default lane discloses each eligible query to Google and DuckDuckGo.
Fetchmark rejects controls this sidecar cannot preserve instead of silently
weakening them.

`POST /v1/render` returns bounded rendered HTML for Fetchmark's existing
renderer adapter. It remains disabled unless `SCRAPLING_EGRESS_PROXY_URL` is
configured; Compose points it at Fetchmark's connection-time egress proxy so
redirects and browser subresources retain SSRF controls. Fetchmark remains
responsible for robots.txt, noindex, MIME, extraction, and byte budgets.
Fetchmark calls this fallback when ordinary extraction reports `js_required`
or returns metadata without a usable body; plain extracted text is returned as
valid Markdown without invoking the browser again.
Challenge pages are reported as degraded diagnostics rather than empty success.
Zero parsed anchors are also degraded because selector drift is not proof of an
authoritative empty result set. Parser drift additionally returns a bounded,
sanitized DOM diagnostic with active content and form values removed.
The persistent profile is guarded by an exclusive volume lease; after an
unclean stop, startup removes only Chromium's stale singleton lock files and
preserves cookies and preferences.

Scrapling is BSD-3-Clause licensed. Operators remain responsible for the terms
and policies of each configured search engine.
