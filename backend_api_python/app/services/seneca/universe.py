"""
Small-cap universe bootstrapper.

Scrapes Finviz screener for stocks with float 1-15M, price $3-20.
Run once to populate universe.json, then use the list in scanner.py.

Usage (inside the container):
    docker exec quantdinger-backend python -c "
    from dotenv import load_dotenv; load_dotenv('/app/.env')
    from app.services.seneca.universe import build_universe
    tickers = build_universe()
    print(f'{len(tickers)} tickers written')
    "
"""

import json
import re
import time
import urllib.request
from pathlib import Path

_CACHE_DIR = Path("/app/data/alpaca_cache")
_UNIVERSE_PATH = _CACHE_DIR / "small_cap_universe.json"

# Finviz screener: price $3-20, float <15M, US equities, sorted by volume desc
# fa_float_u15 = float under 15M; sh_price_3to20 = price $3-20
_FINVIZ_URL = (
    "https://finviz.com/screener.ashx?v=111"
    "&f=fa_float_u15,sh_price_3to20,geo_usa"
    "&o=-volume&r={start}"
)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; seneca-universe/1.0)",
    "Accept": "text/html",
}


def _fetch_page(start: int) -> list:
    url = _FINVIZ_URL.format(start=start)
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="ignore")
        # Finviz new layout: tickers are in data-boxover-ticker attributes
        tickers = re.findall(r'data-boxover-ticker="([A-Z]{1,6})"', html)
        return list(dict.fromkeys(tickers))  # deduplicate while preserving order
    except Exception as e:
        print(f"  page {start}: error {e}")
        return []


def build_universe(max_pages: int = 10) -> list:
    """Scrape Finviz and write universe.json. Returns ticker list."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    all_tickers = []

    for page in range(max_pages):
        start = page * 20 + 1
        tickers = _fetch_page(start)
        if not tickers:
            print(f"  page {start}: empty — stopping")
            break
        new = [t for t in tickers if t not in all_tickers]
        all_tickers.extend(new)
        print(f"  page {start}: +{len(new)} tickers ({len(all_tickers)} total)")
        if len(new) == 0:
            break
        time.sleep(1.5)  # gentle rate limit

    _UNIVERSE_PATH.write_text(json.dumps(sorted(set(all_tickers)), indent=2))
    print(f"Written {len(all_tickers)} tickers → {_UNIVERSE_PATH}")
    return all_tickers


def load_universe() -> list:
    """Load universe from JSON cache, or return empty list if not built yet."""
    if _UNIVERSE_PATH.exists():
        try:
            return json.loads(_UNIVERSE_PATH.read_text())
        except Exception:
            pass
    return []
