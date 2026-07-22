#!/usr/bin/env python3
"""A bounded, headless Scrapling SERP discovery sidecar for Fetchmark."""

from __future__ import annotations

import asyncio
import fcntl
import html
import json
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


PROVIDER = "scrapling"
SEARCH_PATH = "/v1/search"
RENDER_PATH = "/v1/render"
MAX_REQUEST_BYTES = 16 * 1024
MAX_QUERY_BYTES = 2048
MAX_RESULTS = 20
MAX_RENDERED_HTML_BYTES = 10 * 1024 * 1024
MAX_CLEANED_DOM_BYTES = 64 * 1024
MAX_CLEANED_DOM_INPUT_BYTES = 512 * 1024
# Increment when an engine selector contract changes so logs identify the parser generation.
PARSER_VERSION = 1
SUPPORTED_ENGINES = ("google", "duckduckgo", "brave")
ENGINE_HOSTS = {
    "google": ("google.com", "google.co.in", "accounts.google.com", "support.google.com"),
    "duckduckgo": ("duckduckgo.com",),
    "brave": ("search.brave.com",),
}
CHALLENGE = re.compile(
    r"sorry/index|unusual traffic|our systems have detected|not a robot|captcha|"
    r"select all squares containing a duck|unfortunately, bots use duckduckgo too|"
    r"verifying you.re not a bot|drag the slider",
    re.IGNORECASE,
)


@dataclass
class EngineOutcome:
    candidates: list[dict[str, str]] = field(default_factory=list)
    challenged: bool = False
    error_reason: str = ""
    fallback: dict[str, object] | None = None


_DOM_ALLOWED_TAGS = {
    "main", "article", "section", "nav", "header", "footer",
    "h1", "h2", "h3", "h4", "h5", "h6", "p", "br", "hr",
    "ul", "ol", "li", "dl", "dt", "dd", "blockquote", "pre", "code",
    "table", "thead", "tbody", "tfoot", "tr", "th", "td",
    "a", "strong", "b", "em", "i", "time", "figure", "figcaption",
}
_DOM_SUPPRESSED_TAGS = {
    "script", "style", "svg", "canvas", "iframe", "object", "embed",
    "form", "input", "button", "select", "option", "textarea",
    "template", "noscript",
}
_DOM_VOID_TAGS = {"br", "hr", "input", "embed"}


class _CleanDOMParser(HTMLParser):
    def __init__(self, maximum_bytes: int):
        super().__init__(convert_charrefs=True)
        self.maximum_bytes = max(64, maximum_bytes)
        self.parts = ['<main data-fetchmark-fallback="cleaned-dom">']
        self.byte_count = len(self.parts[0].encode("utf-8"))
        self.suppressed_depth = 0
        self.truncated = False

    def _append(self, value: str) -> None:
        if self.truncated or not value:
            return
        encoded = value.encode("utf-8")
        if self.byte_count + len(encoded) + len(b"</main>") > self.maximum_bytes:
            self.truncated = True
            return
        self.parts.append(value)
        self.byte_count += len(encoded)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attr_map = {name.lower(): value or "" for name, value in attrs}
        hidden = "hidden" in attr_map or attr_map.get("aria-hidden", "").strip().lower() == "true"
        if self.suppressed_depth:
            if tag not in _DOM_VOID_TAGS:
                self.suppressed_depth += 1
            return
        if tag in _DOM_SUPPRESSED_TAGS or hidden:
            if tag not in _DOM_VOID_TAGS:
                self.suppressed_depth = 1
            return
        if tag not in _DOM_ALLOWED_TAGS:
            return
        retained: list[str] = []
        for name in ("href", "title", "aria-label", "datetime"):
            value = " ".join(attr_map.get(name, "").split())
            if not value or len(value.encode("utf-8")) > 2048:
                continue
            if name == "href":
                parsed = urlparse(value)
                if parsed.scheme and parsed.scheme not in ("http", "https"):
                    continue
                if not parsed.scheme and not value.startswith(("/", "?", "#")):
                    continue
            retained.append(f' {name}="{html.escape(value, quote=True)}"')
        suffix = "/" if tag in _DOM_VOID_TAGS else ""
        self._append(f"<{tag}{''.join(retained)}{suffix}>")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self.suppressed_depth:
            self.suppressed_depth -= 1
            return
        if tag in _DOM_ALLOWED_TAGS and tag not in _DOM_VOID_TAGS:
            self._append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        if self.suppressed_depth:
            return
        value = " ".join(data.split())
        if value:
            self._append(html.escape(value, quote=False))

    def result(self) -> dict[str, object]:
        return {
            "format": "cleaned_dom",
            "content": "".join(self.parts) + "</main>",
            "truncated": self.truncated,
        }


def clean_dom_fallback(raw_html: object, maximum_bytes: int = MAX_CLEANED_DOM_BYTES) -> dict[str, object]:
    """Return bounded inert markup suitable for explicit parser-failure diagnostics."""
    if not isinstance(raw_html, str):
        raw_html = ""
    encoded = raw_html.encode("utf-8", errors="replace")
    input_truncated = len(encoded) > MAX_CLEANED_DOM_INPUT_BYTES
    if input_truncated:
        raw_html = encoded[:MAX_CLEANED_DOM_INPUT_BYTES].decode("utf-8", errors="ignore")
    parser = _CleanDOMParser(maximum_bytes)
    parser.feed(raw_html)
    parser.close()
    result = parser.result()
    result["truncated"] = bool(result["truncated"] or input_truncated)
    return result


def _useful_dom_fallback(raw_html: object) -> dict[str, object] | None:
    fallback = clean_dom_fallback(raw_html)
    if fallback["content"] == '<main data-fetchmark-fallback="cleaned-dom"></main>':
        return None
    return fallback


def _clean_text(value: object, maximum: int) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = " ".join(value.split())
    if not cleaned or len(cleaned.encode("utf-8")) > maximum or "\x00" in cleaned:
        return ""
    return cleaned


def _engine_host(engine: str, hostname: str) -> bool:
    hostname = hostname.lower().rstrip(".")
    return any(hostname == host or hostname.endswith("." + host) for host in ENGINE_HOSTS[engine])


def _unwrap_redirect(engine: str, raw_url: str) -> str:
    parsed = urlparse(raw_url)
    if not parsed.hostname or not _engine_host(engine, parsed.hostname):
        return raw_url
    values = parse_qs(parsed.query)
    if engine == "google" and parsed.path == "/url":
        return (values.get("q") or values.get("url") or [raw_url])[0]
    if engine == "duckduckgo" and values.get("uddg"):
        return values["uddg"][0]
    return raw_url


def _canonical_result_url(engine: str, raw_url: object) -> str:
    if not isinstance(raw_url, str) or len(raw_url) > 8192:
        return ""
    parsed = urlparse(_unwrap_redirect(engine, raw_url.strip()))
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
        return ""
    if _engine_host(engine, parsed.hostname):
        return ""
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path or "/", parsed.params, parsed.query, ""))


def normalize_candidates(engine: str, raw: list[dict[str, object]], limit: int) -> list[dict[str, object]]:
    """Validate browser-evaluated rows without trusting page-controlled data."""
    if engine not in SUPPORTED_ENGINES:
        return []
    results: list[dict[str, object]] = []
    seen: set[str] = set()
    for candidate in raw[:200]:
        url = _canonical_result_url(engine, candidate.get("url"))
        title = _clean_text(candidate.get("title"), 1024)
        if not url or not title or url in seen:
            continue
        seen.add(url)
        results.append(
            {
                "url": url,
                "title": title,
                "snippet": _clean_text(candidate.get("snippet"), 4096),
                "engine": engine,
                "rank": len(results) + 1,
            }
        )
        if len(results) == limit:
            break
    return results


class SearchService:
    def __init__(self, fetch_engine: Callable[[str, str], EngineOutcome], max_workers: int = len(SUPPORTED_ENGINES)):
        self._fetch_engine = fetch_engine
        self._executor = ThreadPoolExecutor(max_workers=max(1, min(max_workers, len(SUPPORTED_ENGINES))))

    def search(self, query: str, engines: list[str], max_results: int) -> dict[str, object]:
        per_engine: list[list[dict[str, object]]] = []
        diagnostics: list[dict[str, object]] = []
        futures = [self._executor.submit(self._fetch_engine, engine, query) for engine in engines]
        for engine, future in zip(engines, futures):
            try:
                outcome = future.result()
            except Exception as error:
                print(f"scrapling engine task failed: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
                outcome = EngineOutcome(error_reason="network")
            normalized = normalize_candidates(engine, outcome.candidates, max_results)
            per_engine.append(normalized)
            if outcome.challenged:
                diagnostics.append(
                    {
                        "source": engine,
                        "reason": "challenge",
                        "retryable": True,
                        "retry_after_ms": 60_000,
                    }
                )
            elif outcome.error_reason:
                retryable = outcome.error_reason not in {
                    "configuration",
                    "malformed",
                    "result_selector_miss",
                    "snippet_selector_miss",
                }
                diagnostic: dict[str, object] = {
                        "source": engine,
                        "reason": outcome.error_reason,
                        "retryable": retryable,
                        "retry_after_ms": 5_000 if retryable else 0,
                }
                if outcome.fallback is not None:
                    diagnostic["fallback"] = outcome.fallback
                diagnostics.append(diagnostic)
            elif not normalized:
                diagnostics.append(
                    {
                        "source": engine,
                        "reason": "malformed_results" if outcome.candidates else "zero_results",
                        "retryable": True,
                        "retry_after_ms": 5_000,
                    }
                )

        results: list[dict[str, object]] = []
        seen: set[str] = set()
        for rank in range(max_results):
            for engine_results in per_engine:
                if rank >= len(engine_results):
                    continue
                candidate = engine_results[rank]
                url = str(candidate["url"])
                if url in seen:
                    continue
                seen.add(url)
                results.append(candidate)
                if len(results) == max_results:
                    break
            if len(results) == max_results:
                break

        if results:
            status = "partial" if diagnostics else "healthy"
        else:
            status = "degraded_empty" if diagnostics else "authoritative_empty"
        return {
            "provider": PROVIDER,
            "status": status,
            "results": results,
            "diagnostics": diagnostics,
        }


class ProfileLease:
    """Provides exclusive ownership and removes only stale Chromium lock files."""

    def __init__(self, profile_dir: str):
        self._profile = Path(profile_dir)
        self._handle = None

    def __enter__(self) -> "ProfileLease":
        self._profile.mkdir(parents=True, exist_ok=True)
        lease_path = self._profile.parent / ".fetchmark-scrapling.lock"
        self._handle = lease_path.open("a+")
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self._handle.close()
            self._handle = None
            raise RuntimeError("Scrapling profile is already owned by another process") from error
        try:
            for name in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
                candidate = self._profile / name
                try:
                    candidate.unlink()
                except FileNotFoundError:
                    pass
        except BaseException:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
            self._handle = None
            raise
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self._handle is not None:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
            self._handle = None


ENGINE_JAVASCRIPT = {
    "google": """() => Array.from(document.querySelectorAll('h3')).map(heading => {
        const anchor = heading.closest('a');
        if (!anchor) return null;
        const result = heading.closest('.MjjYud, .g, [data-hveid]') || anchor.parentElement;
        const snippet = result && result.querySelector('.VwiC3b, [data-sncf], [data-content-feature="1"]');
        return {
          url: anchor.href,
          title: heading.innerText || heading.textContent || '',
          snippet: snippet ? (snippet.innerText || snippet.textContent || '') : ''
        };
      }).filter(Boolean)""",
    "duckduckgo": """() => {
        let anchors = Array.from(document.querySelectorAll('a[data-testid="result-title-a"], a.result__a'));
        if (!anchors.length) anchors = Array.from(document.querySelectorAll('a[href]'));
        return anchors.map(anchor => {
          const result = anchor.closest('article[data-testid="result"], .result') || anchor.parentElement;
          const snippet = result && result.querySelector('[data-result="snippet"], [data-testid="result-snippet"], .result__snippet');
          return {
            url: anchor.href,
            title: anchor.innerText || anchor.textContent || '',
            snippet: snippet ? (snippet.innerText || snippet.textContent || '') : ''
          };
        });
      }""",
    "brave": """() => Array.from(document.querySelectorAll('.result-content > a[href], a.l1[href]')).map(anchor => {
        const lines = (anchor.innerText || anchor.textContent || '').split('\\n').map(line => line.trim()).filter(Boolean);
        const result = anchor.closest('.snippet, .result') || anchor.parentElement;
        const snippet = result && result.querySelector('.content, .snippet-description, [data-type="description"], .snippet-content');
        return {
          url: anchor.href,
          title: lines.length ? lines[lines.length - 1] : '',
          snippet: snippet ? (snippet.innerText || snippet.textContent || '') : ''
        };
      })""",
}


class ScraplingBrowser:
    """One persistent headless browser profile with bounded concurrent pages."""

    def __init__(
        self,
        profile_dir: str,
        locale: str,
        timezone_id: str,
        max_pages: int = 4,
        discovery_pages: int = 2,
        egress_proxy_url: str = "",
    ):
        self._profile_dir = profile_dir
        self._locale = locale
        self._timezone_id = timezone_id
        self._max_pages = max(2, max_pages)
        self._discovery_pages = max(1, min(discovery_pages, self._max_pages - 1))
        self._render_pages = self._max_pages - self._discovery_pages
        self._egress_proxy_url = egress_proxy_url
        self._session_context = None
        self._session = None
        self._profile_lease = None
        self._discovery_slots: asyncio.Semaphore | None = None
        self._render_slots: asyncio.Semaphore | None = None

    async def __aenter__(self) -> "ScraplingBrowser":
        from scrapling.fetchers import AsyncStealthySession

        self._profile_lease = ProfileLease(self._profile_dir)
        self._profile_lease.__enter__()
        self._session_context = AsyncStealthySession(
            max_pages=self._max_pages,
            headless=True,
            user_data_dir=self._profile_dir,
            locale=self._locale,
            timezone_id=self._timezone_id,
            proxy=self._egress_proxy_url or None,
            disable_resources=False,
            block_webrtc=True,
            hide_canvas=True,
            allow_webgl=True,
            solve_cloudflare=False,
        )
        try:
            self._session = await self._session_context.__aenter__()
            self._discovery_slots = asyncio.Semaphore(self._discovery_pages)
            self._render_slots = asyncio.Semaphore(self._render_pages)
        except BaseException:
            self._profile_lease.__exit__(*sys.exc_info())
            self._profile_lease = None
            raise
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        try:
            if self._session_context is not None:
                await self._session_context.__aexit__(exc_type, exc, traceback)
        finally:
            if self._profile_lease is not None:
                self._profile_lease.__exit__(exc_type, exc, traceback)
                self._profile_lease = None

    async def fetch_engine(self, engine: str, query: str) -> EngineOutcome:
        if engine not in SUPPORTED_ENGINES or self._session is None or self._discovery_slots is None:
            return EngineOutcome(error_reason="configuration")
        urls = {
            "google": "https://www.google.com/search?" + urlencode({"q": query, "hl": "en", "num": "20"}),
            "duckduckgo": "https://duckduckgo.com/?" + urlencode({"q": query, "ia": "web"}),
            "brave": "https://search.brave.com/search?" + urlencode({"q": query, "source": "web"}),
        }
        captured: dict[str, object] = {"candidates": []}

        async def capture_page(browser_page) -> None:
            try:
                selector = {
                    "google": "h3",
                    "duckduckgo": 'a[data-testid="result-title-a"], a.result__a',
                    "brave": ".result-content > a[href], a.l1[href]",
                }[engine]
                await browser_page.locator(selector).first.wait_for(state="attached", timeout=1_500)
            except Exception:
                pass
            captured["candidates"] = await browser_page.evaluate(
                ENGINE_JAVASCRIPT[engine], isolated_context=False
            )

        try:
            async with self._discovery_slots:
                page = await self._session.fetch(
                    urls[engine],
                    google_search=False,
                    retries=0,
                    wait=0,
                    timeout=30_000,
                    network_idle=False,
                    solve_cloudflare=False,
                    page_action=capture_page,
                )
        except Exception as error:
            print(f"scrapling engine {engine} failed: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
            return EngineOutcome(error_reason="network")
        combined = f"{getattr(page, 'url', '')}\n{page.get_all_text(strip=True)}"
        if CHALLENGE.search(combined):
            return EngineOutcome(challenged=True)
        if getattr(page, "status", 0) >= 400:
            return EngineOutcome(error_reason="http_error")
        raw = captured.get("candidates")
        if not isinstance(raw, list):
            return EngineOutcome(error_reason="malformed")
        candidates = [item for item in raw if isinstance(item, dict)]
        if not candidates:
            _report_parser_error(engine, "result_selector_miss", getattr(page, "url", ""), candidates)
            return EngineOutcome(
                error_reason="result_selector_miss",
                fallback=_useful_dom_fallback(getattr(page, "html_content", "")),
            )
        snippet_count = sum(bool(_clean_text(candidate.get("snippet"), 4096)) for candidate in candidates)
        if snippet_count == 0:
            _report_parser_error(engine, "snippet_selector_miss", getattr(page, "url", ""), candidates)
            return EngineOutcome(
                candidates=candidates,
                error_reason="snippet_selector_miss",
                fallback=_useful_dom_fallback(getattr(page, "html_content", "")),
            )
        return EngineOutcome(candidates=candidates)

    async def render_url(self, url: str) -> str:
        if self._session is None or self._render_slots is None:
            raise RuntimeError("Scrapling browser is not ready")
        if not self._egress_proxy_url:
            raise RuntimeError("Scrapling rendering requires an enforced egress proxy")
        async with self._render_slots:
            page = await self._session.fetch(
                url,
                google_search=False,
                retries=0,
                wait=250,
                timeout=30_000,
                network_idle=False,
                solve_cloudflare=False,
            )
        rendered = getattr(page, "html_content", "")
        if not isinstance(rendered, str) or not rendered.strip():
            raise RuntimeError("Scrapling renderer returned empty HTML")
        return rendered


def _report_parser_error(engine: str, reason: str, raw_page_url: object, candidates: list[dict[str, object]]) -> None:
    parsed = urlparse(raw_page_url if isinstance(raw_page_url, str) else "")
    page = ""
    if parsed.scheme in ("http", "https") and parsed.hostname:
        page = urlunparse((parsed.scheme, parsed.hostname, parsed.path or "/", "", "", ""))
    snippet_count = sum(bool(_clean_text(candidate.get("snippet"), 4096)) for candidate in candidates)
    print(
        json.dumps(
            {
                "event": "scrapling_parser_error",
                "parser_version": PARSER_VERSION,
                "engine": engine,
                "reason": reason,
                "page": page,
                "candidate_count": len(candidates),
                "snippet_count": snippet_count,
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
        file=sys.stderr,
        flush=True,
    )


class BrowserWorker:
    """Owns one asynchronous Playwright session on a dedicated event-loop thread."""

    def __init__(self, browser_factory: Callable[[], ScraplingBrowser], render_timeout_seconds: float = 19.0):
        if render_timeout_seconds <= 0:
            raise ValueError("render timeout must be positive")
        self._browser_factory = browser_factory
        self._render_timeout_seconds = render_timeout_seconds
        self._ready = threading.Event()
        self._startup_error: BaseException | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._browser: ScraplingBrowser | None = None
        self._browser_context = None
        self._thread = threading.Thread(target=self._run, name="scrapling-browser", daemon=True)

    def __enter__(self) -> "BrowserWorker":
        self._thread.start()
        self._ready.wait()
        if self._startup_error is not None:
            raise RuntimeError("unable to start Scrapling browser") from self._startup_error
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if self._thread.is_alive() and self._loop is not None:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=10)

    def fetch_engine(self, engine: str, query: str) -> EngineOutcome:
        if self._loop is None or self._browser is None:
            return EngineOutcome(error_reason="configuration")
        future = asyncio.run_coroutine_threadsafe(self._browser.fetch_engine(engine, query), self._loop)
        return future.result()

    def render_url(self, url: str) -> str:
        if self._loop is None or self._browser is None:
            raise RuntimeError("Scrapling browser is not ready")
        future = asyncio.run_coroutine_threadsafe(self._browser.render_url(url), self._loop)
        try:
            return future.result(timeout=self._render_timeout_seconds)
        except FutureTimeoutError as error:
            future.cancel()
            raise TimeoutError("Scrapling render timed out") from error

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        try:
            self._browser_context = self._browser_factory()
            self._browser = loop.run_until_complete(self._browser_context.__aenter__())
            self._ready.set()
            loop.run_forever()
        except BaseException as error:
            self._startup_error = error
            self._ready.set()
        finally:
            if self._browser_context is not None and self._browser is not None:
                try:
                    loop.run_until_complete(self._browser_context.__aexit__(None, None, None))
                except Exception as error:
                    print(f"scrapling browser shutdown failed: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()


class SidecarHandler(BaseHTTPRequestHandler):
    service: SearchService
    render_url: Callable[[str], str]
    request_slots = threading.BoundedSemaphore(8)

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(5)

    def do_GET(self) -> None:
        if self.path != "/healthz":
            self._write_json(404, {"error": "not_found"})
            return
        self._write_json(200, {"status": "ok", "provider": PROVIDER})

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path not in (SEARCH_PATH, RENDER_PATH):
            self._write_json(404, {"error": "not_found"})
            return
        if not self.request_slots.acquire(blocking=False):
            self._write_json(429, {"error": "busy"}, {"Retry-After": "1"})
            return
        try:
            if path == SEARCH_PATH:
                request = self._read_request()
                response = self.service.search(request[0], request[1], request[2])
                self._write_json(200, response)
            else:
                rendered = type(self).render_url(self._read_render_request())
                if len(rendered.encode("utf-8")) > MAX_RENDERED_HTML_BYTES:
                    raise ValueError("rendered HTML exceeds bound")
                self._write_json(200, {"html": rendered})
        except ValueError as error:
            self._write_json(400, {"error": "invalid_request", "detail": str(error)})
        except Exception as error:
            print(f"scrapling request failed: {type(error).__name__}", file=sys.stderr, flush=True)
            self._write_json(502, {"error": "browser_failed"})
        finally:
            self.request_slots.release()

    def _read_request(self) -> tuple[str, list[str], int]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as error:
            raise ValueError("invalid content length") from error
        if length < 1 or length > MAX_REQUEST_BYTES:
            raise ValueError("request body exceeds bound")
        try:
            payload = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError("request body must be JSON") from error
        if not isinstance(payload, dict) or set(payload) - {"query", "engines", "max_results"}:
            raise ValueError("unknown or invalid request fields")
        query = _clean_text(payload.get("query"), MAX_QUERY_BYTES)
        if not query:
            raise ValueError("query is required")
        raw_engines = payload.get("engines", list(SUPPORTED_ENGINES))
        if not isinstance(raw_engines, list) or not raw_engines:
            raise ValueError("engines must be a non-empty list")
        engines: list[str] = []
        for raw_engine in raw_engines:
            if not isinstance(raw_engine, str) or raw_engine not in SUPPORTED_ENGINES:
                raise ValueError("unsupported engine")
            if raw_engine not in engines:
                engines.append(raw_engine)
        max_results = payload.get("max_results", MAX_RESULTS)
        if not isinstance(max_results, int) or isinstance(max_results, bool) or max_results < 1:
            raise ValueError("max_results must be a positive integer")
        return query, engines, min(max_results, MAX_RESULTS)

    def _read_render_request(self) -> str:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as error:
            raise ValueError("invalid content length") from error
        if length < 1 or length > MAX_REQUEST_BYTES:
            raise ValueError("request body exceeds bound")
        try:
            payload = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError("request body must be JSON") from error
        if not isinstance(payload, dict) or set(payload) != {"url"}:
            raise ValueError("render request requires only url")
        raw_url = payload.get("url")
        if not isinstance(raw_url, str) or len(raw_url) > 8192 or raw_url.strip() != raw_url:
            raise ValueError("invalid render URL")
        parsed = urlparse(raw_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("render URL must be an http(s) URL without credentials")
        return raw_url

    def _write_json(self, status: int, payload: object, headers: dict[str, str] | None = None) -> None:
        body = (json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    host = os.environ.get("SCRAPLING_LISTEN_HOST", "0.0.0.0")
    port = int(os.environ.get("SCRAPLING_LISTEN_PORT", "8080"))
    profile_dir = os.environ.get("SCRAPLING_PROFILE_DIR", "/data/profile")
    locale = os.environ.get("SCRAPLING_LOCALE", "en-US")
    timezone_id = os.environ.get("SCRAPLING_TIMEZONE", "UTC")
    max_pages = max(2, min(int(os.environ.get("SCRAPLING_MAX_PAGES", "4")), 16))
    discovery_pages = max(1, min(int(os.environ.get("SCRAPLING_DISCOVERY_PAGES", "2")), max_pages - 1))
    request_concurrency = max(1, min(int(os.environ.get("SCRAPLING_REQUEST_CONCURRENCY", "8")), 64))
    render_timeout_seconds = max(1.0, min(float(os.environ.get("SCRAPLING_RENDER_TIMEOUT_SECONDS", "19")), 120.0))
    egress_proxy_url = os.environ.get("SCRAPLING_EGRESS_PROXY_URL", "")
    browser_factory = lambda: ScraplingBrowser(
        profile_dir,
        locale,
        timezone_id,
        max_pages=max_pages,
        discovery_pages=discovery_pages,
        egress_proxy_url=egress_proxy_url,
    )
    with BrowserWorker(browser_factory, render_timeout_seconds=render_timeout_seconds) as browser:
        SidecarHandler.service = SearchService(browser.fetch_engine, max_workers=discovery_pages)
        SidecarHandler.render_url = browser.render_url
        SidecarHandler.request_slots = threading.BoundedSemaphore(request_concurrency)
        server = ThreadingHTTPServer((host, port), SidecarHandler)
        server.daemon_threads = True
        try:
            server.serve_forever(poll_interval=0.5)
        finally:
            server.server_close()


if __name__ == "__main__":
    main()
