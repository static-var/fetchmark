import importlib.metadata
import json
import sys
from pathlib import Path

import requests
from exa_py import Exa
from tavily import TavilyClient


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def expected_sdk_versions() -> dict[str, str]:
    requirements = Path(__file__).with_name("compat-sdk-requirements.txt")
    expected: dict[str, str] = {}
    for raw_line in requirements.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        package, separator, version = line.partition("==")
        require(bool(separator and package and version), f"unversioned compatibility requirement: {line}")
        expected[package] = version
    require(set(expected) == {"tavily-python", "exa-py"}, "compatibility requirements changed unexpectedly")
    return expected


def verify_sdk_versions(installed: dict[str, str]) -> dict[str, str]:
    expected = expected_sdk_versions()
    for package, version in expected.items():
        require(installed.get(package) == version, f"{package} version {installed.get(package)!r}; expected {version}")
    return installed


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: compat_sdk_smoke.py ROOT_URL API_KEY")
    root_url, api_key = sys.argv[1:]
    expected_url = "https://example.com/open-discovery"
    installed = verify_sdk_versions({
        "tavily-python": importlib.metadata.version("tavily-python"),
        "exa-py": importlib.metadata.version("exa-py"),
    })

    tavily = TavilyClient(
        api_key=api_key,
        api_base_url=f"{root_url}/compat/tavily",
    )
    tavily_response = tavily.search(
        query="open discovery",
        max_results=2,
        include_raw_content="markdown",
        include_usage=True,
    )
    require(tavily_response["query"] == "open discovery", "Tavily query changed")
    require(len(tavily_response["results"]) == 1, "Tavily result count changed")
    require(tavily_response["results"][0]["url"] == expected_url, "Tavily URL changed")
    require(tavily_response["results"][0]["raw_content"].startswith("# Open discovery"), "Tavily raw content missing")
    require(tavily_response["usage"]["credits"] == 0, "Tavily usage must remain zero-cost")

    exa = Exa(api_key=api_key, base_url=f"{root_url}/compat/exa")
    exa_search = exa.search(
        "open discovery",
        num_results=2,
        contents={"text": True},
    )
    require(len(exa_search.results) == 1, "Exa result count changed")
    require(exa_search.results[0].url == expected_url, "Exa URL changed")
    require(exa_search.results[0].text.startswith("Open discovery"), "Exa search text missing")

    exa_contents = exa.get_contents([expected_url], text=True)
    require(len(exa_contents.results) == 1, "Exa contents result count changed")
    require(exa_contents.results[0].url == expected_url, "Exa contents URL changed")
    require(exa_contents.results[0].text.startswith("Open discovery"), "Exa contents text missing")

    brave_response = requests.get(
        f"{root_url}/compat/brave/res/v1/web/search",
        params={"q": "open discovery", "count": 1},
        headers={
            "Accept": "application/json",
            "X-Subscription-Token": api_key,
        },
        timeout=5,
    )
    brave_response.raise_for_status()
    brave_payload = brave_response.json()
    require(brave_payload["type"] == "search", "Brave response type changed")
    require(len(brave_payload["web"]["results"]) == 1, "Brave result count changed")
    require(brave_payload["web"]["results"][0]["url"] == expected_url, "Brave URL changed")

    print(json.dumps({
        "brave_documented_http": "passed",
        "tavily_python": installed["tavily-python"],
        "exa_py": installed["exa-py"],
        "tavily_search": "passed",
        "exa_search": "passed",
        "exa_contents": "passed",
    }, sort_keys=True))


if __name__ == "__main__":
    main()
