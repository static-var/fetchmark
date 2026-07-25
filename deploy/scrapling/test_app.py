import asyncio
import io
import json
import time
import threading
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from app import (
    BrowserWorker,
    ENGINE_JAVASCRIPT,
    EngineOutcome,
    ProfileLease,
    ScraplingBrowser,
    SearchService,
    SidecarHandler,
    clean_dom_fallback,
    normalize_candidates,
)


class ScraplingSidecarTest(unittest.TestCase):
    def test_search_request_defaults_to_duckduckgo_and_brave(self):
        payload = json.dumps({"query": "default engines"}).encode("utf-8")
        handler = object.__new__(SidecarHandler)
        handler.headers = {"Content-Length": str(len(payload))}
        handler.rfile = io.BytesIO(payload)

        query, engines, max_results = handler._read_request()

        self.assertEqual(query, "default engines")
        self.assertEqual(engines, ["duckduckgo", "brave"])
        self.assertEqual(max_results, 20)

    def test_search_service_fetches_engines_concurrently(self):
        both_started = threading.Barrier(2)

        def fetch_engine(engine, query):
            both_started.wait(timeout=1)
            time.sleep(0.05)
            return EngineOutcome(candidates=[{
                "url": f"https://{engine}.example/result",
                "title": f"{engine} result",
            }])

        started = time.monotonic()
        response = SearchService(fetch_engine, max_workers=2).search(
            "parallel query", ["google", "duckduckgo"], 10
        )
        elapsed = time.monotonic() - started

        self.assertEqual(response["status"], "healthy")
        self.assertEqual(len(response["results"]), 2)
        self.assertLess(elapsed, 0.5)

    def test_clean_dom_fallback_is_bounded_and_removes_active_or_private_markup(self):
        fallback = clean_dom_fallback(
            """<!doctype html><html><body>
            <script>steal()</script><style>.x{display:none}</style>
            <form><input name="secret" value="do-not-return"><button>Send</button></form>
            <main onclick="steal()"><h1>Search parser changed</h1>
            <a href="https://example.com/result" aria-label="Useful result">Result</a>
            <a href="javascript:steal()">Bad link</a></main>
            </body></html>""",
            maximum_bytes=512,
        )

        self.assertEqual(fallback["format"], "cleaned_dom")
        self.assertFalse(fallback["truncated"])
        content = fallback["content"]
        self.assertIn("Search parser changed", content)
        self.assertIn('href="https://example.com/result"', content)
        self.assertIn('aria-label="Useful result"', content)
        self.assertNotIn("script", content)
        self.assertNotIn("style", content)
        self.assertNotIn("form", content)
        self.assertNotIn("input", content)
        self.assertNotIn("do-not-return", content)
        self.assertNotIn("onclick", content)
        self.assertNotIn("javascript:", content)

    def test_render_endpoint_returns_html_for_the_generic_fetchmark_renderer(self):
        SidecarHandler.service = SearchService(lambda engine, query: EngineOutcome())
        SidecarHandler.render_url = lambda url: f"<html><body>rendered {url}</body></html>"
        handler = object.__new__(SidecarHandler)
        handler.path = "/v1/render?launch=ignored-browserless-options"
        handler._read_render_request = lambda: "https://example.com/app"
        responses = []
        handler._write_json = lambda status, payload, headers=None: responses.append((status, payload))

        handler.do_POST()

        self.assertEqual(
            responses,
            [(200, {"html": "<html><body>rendered https://example.com/app</body></html>"})],
        )

    def test_result_selector_miss_is_reported_without_query_or_dom(self):
        class BrowserPage:
            async def evaluate(self, script, isolated_context=False):
                return []

        class Page:
            url = "https://www.google.com/search?q=private+query"
            status = 200
            html_content = """<html><body><main><h1>Changed results page</h1>
                <script>ignore()</script></main></body></html>"""

            def get_all_text(self, strip=True):
                return "ordinary search page body"

        class Session:
            async def fetch(self, url, page_action, **kwargs):
                await page_action(BrowserPage())
                return Page()

        browser = ScraplingBrowser("unused", "en-US", "UTC")
        browser._session = Session()
        browser._discovery_slots = asyncio.Semaphore(2)
        logs = io.StringIO()

        with redirect_stderr(logs):
            outcome = asyncio.run(browser.fetch_engine("google", "private query"))

        self.assertEqual(outcome.error_reason, "result_selector_miss")
        self.assertEqual(outcome.fallback["format"], "cleaned_dom")
        self.assertIn("Changed results page", outcome.fallback["content"])
        self.assertNotIn("script", outcome.fallback["content"])
        event = json.loads(logs.getvalue())
        self.assertEqual(event["event"], "scrapling_parser_error")
        self.assertEqual(event["reason"], "result_selector_miss")
        self.assertEqual(event["engine"], "google")
        self.assertEqual(event["page"], "https://www.google.com/search")
        self.assertEqual(event["candidate_count"], 0)
        self.assertNotIn("private query", logs.getvalue())
        self.assertNotIn("ordinary search page body", logs.getvalue())

    def test_snippet_selector_miss_keeps_results_and_marks_batch_partial(self):
        candidates = [{"url": "https://example.com/result", "title": "Result title", "snippet": ""}]

        class BrowserPage:
            async def evaluate(self, script, isolated_context=False):
                return candidates

        class Page:
            url = "https://duckduckgo.com/?q=private+query"
            status = 200

            def get_all_text(self, strip=True):
                return "ordinary search page body"

        class Session:
            async def fetch(self, url, page_action, **kwargs):
                await page_action(BrowserPage())
                return Page()

        browser = ScraplingBrowser("unused", "en-US", "UTC")
        browser._session = Session()
        browser._discovery_slots = asyncio.Semaphore(2)
        logs = io.StringIO()
        with redirect_stderr(logs):
            outcome = asyncio.run(browser.fetch_engine("duckduckgo", "private query"))
        response = SearchService(lambda engine, query: outcome).search("private query", ["duckduckgo"], 10)

        self.assertEqual(outcome.error_reason, "snippet_selector_miss")
        self.assertEqual(response["status"], "partial")
        self.assertEqual(len(response["results"]), 1)
        self.assertEqual(
            response["diagnostics"],
            [{"source": "duckduckgo", "reason": "snippet_selector_miss", "retryable": False, "retry_after_ms": 0}],
        )

    def test_engine_extractors_return_serp_snippets(self):
        from playwright.sync_api import sync_playwright

        fixtures = {
            "google": (
                """<div class="MjjYud"><a href="https://example.com/google"><h3>Google title</h3></a>"
                "<div class="VwiC3b">Google result snippet with useful context.</div></div>""",
                "Google result snippet with useful context.",
            ),
            "duckduckgo": (
                """<article data-testid="result"><a data-testid="result-title-a" href="https://example.com/ddg">"
                "Duck title</a><div data-result="snippet">Duck result snippet with useful context.</div></article>""",
                "Duck result snippet with useful context.",
            ),
            "brave": (
                """<div class="snippet"><div class="result-content"><a href="https://example.com/brave">"
                "<span>example.com</span><span>Brave title</span></a>"
                "<div class="content">Brave result snippet with useful context.</div></div></div>""",
                "Brave result snippet with useful context.",
            ),
        }

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page()
                for engine, (markup, expected) in fixtures.items():
                    with self.subTest(engine=engine):
                        page.set_content(markup)
                        candidates = page.evaluate(ENGINE_JAVASCRIPT[engine])
                        normalized = normalize_candidates(engine, candidates, 10)
                        self.assertEqual(normalized[0]["snippet"], expected)
            finally:
                browser.close()

    def test_normalize_candidates_rejects_engine_navigation_and_deduplicates(self):
        candidates = normalize_candidates(
            "brave",
            [
                {"url": "https://search.brave.com/settings", "title": "Settings"},
                {"url": "https://example.com/article#section", "title": " Article title "},
                {"url": "https://example.com/article", "title": "duplicate"},
                {"url": "javascript:alert(1)", "title": "bad"},
            ],
            10,
        )

        self.assertEqual(
            candidates,
            [{"url": "https://example.com/article", "title": "Article title", "snippet": "", "engine": "brave", "rank": 1}],
        )

    def test_search_service_keeps_results_when_one_engine_is_challenged(self):
        outcomes = {
            "google": EngineOutcome(
                candidates=[{"url": "https://example.com/g", "title": "Google result"}]
            ),
            "duckduckgo": EngineOutcome(
                candidates=[{"url": "https://example.org/d", "title": "Duck result"}]
            ),
            "brave": EngineOutcome(challenged=True),
        }
        service = SearchService(lambda engine, query: outcomes[engine])

        response = service.search("fresh result", ["google", "duckduckgo", "brave"], 10)

        self.assertEqual(response["status"], "partial")
        self.assertEqual([result["engine"] for result in response["results"]], ["google", "duckduckgo"])
        self.assertEqual(
            response["diagnostics"],
            [{"source": "brave", "reason": "challenge", "retryable": True, "retry_after_ms": 60000}],
        )

    def test_search_service_reports_challenged_empty_as_degraded(self):
        service = SearchService(lambda engine, query: EngineOutcome(challenged=True))

        response = service.search("fresh result", ["google"], 10)

        self.assertEqual(response["status"], "degraded_empty")
        self.assertEqual(response["results"], [])

    def test_search_service_never_treats_selector_empty_as_authoritative(self):
        service = SearchService(lambda engine, query: EngineOutcome())

        response = service.search("fresh result", ["google"], 10)

        self.assertEqual(response["status"], "degraded_empty")
        self.assertEqual(response["diagnostics"][0]["reason"], "zero_results")

    def test_search_service_degrades_all_invalid_browser_rows(self):
        service = SearchService(
            lambda engine, query: EngineOutcome(candidates=[{"url": "javascript:alert(1)", "title": "bad"}])
        )

        response = service.search("fresh result", ["google"], 10)

        self.assertEqual(response["status"], "degraded_empty")
        self.assertEqual(response["diagnostics"][0]["reason"], "malformed_results")

    def test_browser_worker_keeps_session_calls_on_owner_thread(self):
        owner = []

        class FakeBrowser:
            async def __aenter__(self):
                owner.append(threading.get_ident())
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return None

            async def fetch_engine(self, engine, query):
                self.assert_owner()
                return EngineOutcome(candidates=[{"url": "https://example.com/", "title": query}])

            def assert_owner(self):
                if threading.get_ident() != owner[0]:
                    raise AssertionError("browser called from a non-owner thread")

        with BrowserWorker(FakeBrowser) as worker:
            outcome = worker.fetch_engine("google", "owner thread")

        self.assertNotEqual(owner[0], threading.get_ident())
        self.assertEqual(outcome.candidates[0]["title"], "owner thread")

    def test_browser_worker_cancels_render_when_operation_deadline_expires(self):
        started = threading.Event()
        cancelled = threading.Event()

        class FakeBrowser:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return None

            async def render_url(self, url):
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

        with BrowserWorker(FakeBrowser, render_timeout_seconds=0.05) as worker:
            with self.assertRaisesRegex(TimeoutError, "timed out"):
                worker.render_url("https://example.com/slow")
            self.assertTrue(started.is_set())
            self.assertTrue(cancelled.wait(timeout=1), "render coroutine was not cancelled")

    def test_browser_worker_runs_multiple_pages_concurrently_on_one_async_session(self):
        lock = threading.Lock()
        active = 0
        maximum_active = 0

        class FakeBrowser:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return None

            async def fetch_engine(self, engine, query):
                nonlocal active, maximum_active
                with lock:
                    active += 1
                    maximum_active = max(maximum_active, active)
                await asyncio.sleep(0.05)
                with lock:
                    active -= 1
                return EngineOutcome(candidates=[{"url": f"https://{engine}.example/", "title": query}])

        with BrowserWorker(FakeBrowser) as worker:
            outcomes = []
            threads = [
                threading.Thread(target=lambda e=engine: outcomes.append(worker.fetch_engine(e, "parallel")))
                for engine in ("google", "duckduckgo")
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        self.assertEqual(len(outcomes), 2)
        self.assertEqual(maximum_active, 2)

    def test_profile_lease_removes_only_chromium_singleton_files(self):
        with tempfile.TemporaryDirectory() as root:
            profile = Path(root) / "profile"
            profile.mkdir()
            (profile / "SingletonLock").symlink_to("old-container-19")
            (profile / "Preferences").write_text("keep", encoding="utf-8")
            self.assertTrue((profile / "SingletonLock").is_symlink())

            with ProfileLease(str(profile)):
                self.assertFalse((profile / "SingletonLock").is_symlink())
                self.assertEqual((profile / "Preferences").read_text(encoding="utf-8"), "keep")


if __name__ == "__main__":
    unittest.main()
