"""
Seneca Selection Layer — Layer 1 scanner.

screen(date, live) -> list[Candidate]

Dual data path (same rule engine):
  live=True          -> Alpaca screener endpoints (pre-market scan)
  date="2025-06-01"  -> Alpaca historical bars over curated small-cap universe

Float data: Finnhub API (if FINNHUB_API_KEY set) or Finviz scrape.
            Cached at ALPACA_CACHE_DIR/float_cache.json; stale = 30 days.

Known precision limitations (see scanner-spec.md):
  1. Catalyst proxy is gap% + RVOL only — no news/filing signal.
  2. Float snapshots can be stale for recent gappers.
  3. Historical pre-market volume uses total daily volume as proxy;
     true 4am-9:30am volume requires full intraday bars for each universe ticker.
  4. Universe coverage is the recall ceiling — upgrade to Polygon grouped-daily
     for full-market historical scan when budget allows.
"""

import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date as date_type
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from app.services.seneca.levels import quick_levels

logger = logging.getLogger(__name__)

# ── Criteria ──────────────────────────────────────────────────────────────────
PRICE_MIN = 3.0
PRICE_MAX = 20.0
FLOAT_MIN_M = 1.0
FLOAT_MAX_M = 15.0
GAP_MIN_PCT = 5.0
PREMARKET_VOL_MIN = 1_000_000
RVOL_MIN = 2.0
FLOAT_STALE_DAYS = 30

# ── Paths ─────────────────────────────────────────────────────────────────────
_CACHE_DIR = Path(os.getenv("ALPACA_CACHE_DIR", "/app/data/alpaca_cache"))
_FLOAT_CACHE = _CACHE_DIR / "float_cache.json"

# ── Alpaca endpoints ──────────────────────────────────────────────────────────
_ALPACA_DATA = "https://data.alpaca.markets"
_TOP_MOVERS_URL = f"{_ALPACA_DATA}/v1beta1/screener/stocks/movers"
_MOST_ACTIVES_URL = f"{_ALPACA_DATA}/v1beta1/screener/stocks/most-actives"
_SNAPSHOTS_URL = f"{_ALPACA_DATA}/v2/stocks/snapshots"

# ── Small-cap universe for historical screening ───────────────────────────────
# Broad list — not hand-picked winners. Represents the search space.
# Known limitation: universe coverage is the recall ceiling. Full-market
# historical scan requires Polygon grouped-daily (see Future Work in dev-tips).
SMALL_CAP_UNIVERSE = sorted(set([
    # Crypto miners
    "MARA", "RIOT", "CLSK", "CIFR", "HUT", "BITF",
    # EV / clean energy
    "NKLA", "RIDE", "GOEV", "MULN", "IDEX", "SOLO", "WKHS",
    # Biotech — the most frequent gap-and-go source
    "AGEN", "ADTX", "AKER", "ALDX", "AMPIO", "ANIX", "APDN",
    "ATXI", "AYRO", "AZRX", "BCRX", "BHVN", "BLRX", "CBAY",
    "CBMG", "CDXS", "CKPT", "CLRB", "CNSP", "CODX", "COEP",
    "CRBP", "CRTX", "CYCN", "DARE", "DVAX", "EDSA", "ENLV",
    "EPIX", "ESPR", "EWTX", "EYEG", "FATE", "FBIO", "FCSC",
    "FREQ", "FUSN", "GNPX", "GOVX", "HGEN", "HOOK", "HRTX",
    "IBRX", "IDYA", "IMRN", "IMTX", "INFI", "INVO", "IPIX",
    "JAGX", "KDMN", "KMDA", "KRYS", "KTRA", "KYMR", "LCTX",
    "LQDA", "LXRX", "MCRB", "MNKD", "MNPR", "MTEM", "MYND",
    "NBRV", "NEOS", "NKTR", "NLSP", "NRXP", "NVAX", "OBSV",
    "OCGN", "OCUL", "OGEN", "ONCT", "ONVO", "ORTX", "PHAT",
    "PHVS", "PIRS", "PLSE", "PMVP", "PRAX", "PRVB", "PTGX",
    "RAPT", "RCEL", "RCKT", "RLAY", "RLMD", "RLYB", "RNAZ",
    "RPHM", "RPTX", "RUBY", "RZLT", "SAGE", "SANA", "SCPH",
    "SDGR", "SELB", "SEEL", "SENS", "SESN", "SIGA", "SINT",
    "SKYE", "SLDB", "SLRN", "SMMT", "SNDL", "SPRO", "SRRA",
    "STOK", "SURF", "SYRS", "TCON", "TELA", "TLRY", "TNXP",
    "TRIL", "TSHA", "TTOO", "TVTX", "TYME", "TYRA", "UAVS",
    "URGN", "VBIV", "VCEL", "VCNX", "VCYT", "VKTX", "VNRX",
    "VRAY", "VRDN", "VRNA", "VTGN", "VTAK", "VXRT", "WINT",
    "XAIR", "XBIO", "XELA", "XENE", "XERS", "XFOR", "XOMA",
    "ZIOP", "ZYME",
    # Meme / retail
    "GME", "AMC", "SPCE", "CLOV", "WISH", "EXPR", "NOK", "BB",
    # Tech small-caps
    "VUZI", "KOPN", "KOSS", "AEYE",
    # Finance / fintech
    "SOFI", "CURO",
    # Energy / mining
    "CDEV", "AMMO", "TELL", "AMPE",
    # Consumer
    "PRPL", "LOVE",
]))


# ── Candidate dataclass ───────────────────────────────────────────────────────

@dataclass
class Candidate:
    ticker: str
    gap_pct: float
    rvol: float
    premarket_vol: int
    float_shares: int
    price: float
    score: float
    score_components: dict = field(default_factory=dict)
    float_stale: bool = False
    # Context Layer (Layer 2) — quick levels, no extra API call
    prev_high: float = 0.0
    prev_low: float = 0.0
    whole_dollar_above: float = 0.0
    whole_dollar_below: float = 0.0

    def to_dict(self) -> dict:
        return {
            "ticker": self.ticker,
            "gap_pct": round(self.gap_pct, 2),
            "rvol": round(self.rvol, 2),
            "premarket_vol": self.premarket_vol,
            "float_shares": self.float_shares,
            "price": round(self.price, 2),
            "score": round(self.score, 4),
            "score_components": self.score_components,
            "float_stale": self.float_stale,
            "prev_high": round(self.prev_high, 2),
            "prev_low": round(self.prev_low, 2),
            "whole_dollar_above": self.whole_dollar_above,
            "whole_dollar_below": self.whole_dollar_below,
        }


# ── Scoring ───────────────────────────────────────────────────────────────────

def _float_tier(float_shares: int) -> float:
    m = float_shares / 1_000_000
    if m <= 3:
        return 3.0
    if m <= 7:
        return 2.0
    return 1.0


def _score(gap_pct: float, rvol: float, float_shares: int) -> tuple:
    tier = _float_tier(float_shares)
    raw = gap_pct * rvol * tier
    comps = {
        "gap_pct": round(gap_pct, 2),
        "rvol": round(rvol, 2),
        "float_tier": tier,
        "raw": round(raw, 4),
    }
    return raw, comps


# ── Alpaca helpers ────────────────────────────────────────────────────────────

def _alpaca_headers() -> dict:
    return {
        "APCA-API-KEY-ID": os.environ.get("ALPACA_API_KEY", ""),
        "APCA-API-SECRET-KEY": os.environ.get("ALPACA_SECRET_KEY", ""),
        "Accept": "application/json",
    }


def _alpaca_get(url: str, params: Optional[dict] = None) -> dict:
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=_alpaca_headers())
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


# ── Float cache ───────────────────────────────────────────────────────────────

def _load_float_cache() -> dict:
    if not _FLOAT_CACHE.exists():
        return {}
    try:
        return json.loads(_FLOAT_CACHE.read_text())
    except Exception:
        return {}


def _save_float_cache(cache: dict) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _FLOAT_CACHE.write_text(json.dumps(cache, indent=2))


def _is_stale(entry: dict) -> bool:
    fetched = entry.get("fetched_at", "")
    if not fetched:
        return True
    try:
        d = datetime.strptime(fetched, "%Y-%m-%d").date()
        return (date_type.today() - d).days > FLOAT_STALE_DAYS
    except Exception:
        return True


def _parse_si(val: str) -> Optional[int]:
    val = val.strip().upper().replace(",", "")
    try:
        if val.endswith("B"):
            return int(float(val[:-1]) * 1_000_000_000)
        if val.endswith("M"):
            return int(float(val[:-1]) * 1_000_000)
        if val.endswith("K"):
            return int(float(val[:-1]) * 1_000)
        return int(float(val))
    except Exception:
        return None


def _fetch_float_finnhub(ticker: str) -> Optional[int]:
    key = os.environ.get("FINNHUB_API_KEY", "")
    if not key:
        return None
    url = f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={key}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
        shares_out = float(data.get("shareOutstanding") or 0)
        if shares_out > 0:
            return int(shares_out * 1_000_000)  # Finnhub returns millions
    except Exception as e:
        logger.debug("Finnhub float failed for %s: %s", ticker, e)
    return None


def _fetch_float_finviz(ticker: str) -> Optional[int]:
    url = f"https://finviz.com/quote.ashx?t={ticker}&ty=c&ta=1&p=d"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; seneca-scanner/1.0)",
        "Accept": "text/html",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=4) as r:
            html = r.read().decode("utf-8", errors="ignore")
        # Finviz wraps the value in <b>...</b> after the Float label
        m = re.search(r"Float</div></td>.*?<b>([^<]+)</b>", html, re.DOTALL)
        if not m:
            # fallback: older Finviz layout
            m = re.search(r"Float\s*<[^>]*>([^<]+)<", html)
        if m:
            return _parse_si(m.group(1).strip())
    except Exception as e:
        logger.debug("Finviz float failed for %s: %s", ticker, e)
    return None


def _get_float(ticker: str, cache: dict) -> tuple:
    """Returns (float_shares, is_stale). Checks cache; fetches on miss/stale.
    Caches both found AND not-found results so failed lookups aren't retried
    every request (avoids repeated 4s Finviz timeouts for unlisted tickers).
    """
    entry = cache.get(ticker, {})
    if not _is_stale(entry) and "float_shares" in entry:
        shares = int(entry["float_shares"])
        return shares, shares == 0  # 0 means not found (stale=True)

    shares = _fetch_float_finnhub(ticker) or _fetch_float_finviz(ticker)
    cache[ticker] = {
        "float_shares": shares or 0,
        "fetched_at": date_type.today().isoformat(),
    }
    return (shares or 0), not shares


# ── Parallel float fetch ──────────────────────────────────────────────────────

def _fetch_floats_parallel(symbols: list, cache: dict) -> dict:
    """
    Returns {sym: (float_shares, is_stale)} for all symbols.
    Cache hits are returned immediately; misses are fetched in parallel (8 threads).
    Updates cache in-place for any new fetches.
    """
    results = {}
    to_fetch = []

    for sym in symbols:
        entry = cache.get(sym, {})
        if not _is_stale(entry) and "float_shares" in entry:
            results[sym] = (int(entry["float_shares"]), False)
        else:
            to_fetch.append(sym)

    if not to_fetch:
        return results

    def _fetch_one(sym):
        return sym, _fetch_float_finnhub(sym) or _fetch_float_finviz(sym)

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_one, sym): sym for sym in to_fetch}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                _, shares = future.result(timeout=20)
            except Exception as e:
                logger.debug("Parallel float failed for %s: %s", sym, e)
                shares = None

            # Cache both hits and misses — avoids repeated slow fetches for
            # tickers not listed on Finviz (e.g. OTC/pink-sheet stocks)
            cache[sym] = {
                "float_shares": shares or 0,
                "fetched_at": date_type.today().isoformat(),
            }
            results[sym] = (shares or 0, not shares)

    return results


# ── Live path ─────────────────────────────────────────────────────────────────

def _live_screen() -> list:
    """Pre-market scan using Alpaca screener endpoints."""
    gainers: list = []

    try:
        data = _alpaca_get(_TOP_MOVERS_URL, {"top": 50})
        gainers = data.get("gainers", [])
    except Exception as e:
        logger.warning("top-movers fetch failed: %s", e)

    if not gainers:
        try:
            data = _alpaca_get(_MOST_ACTIVES_URL, {"top": 50, "by": "volume"})
            gainers = data.get("most_actives", [])
        except Exception as e:
            logger.warning("most-actives fallback failed: %s", e)
            return []

    symbols = [g["symbol"] for g in gainers if g.get("symbol")]
    if not symbols:
        return []

    # Enrich with snapshot data (price, prev-day volume for RVOL)
    snap_by_symbol = {}
    try:
        for i in range(0, len(symbols), 50):
            chunk = symbols[i : i + 50]
            data = _alpaca_get(_SNAPSHOTS_URL, {
                "symbols": ",".join(chunk),
                "feed": "iex",
            })
            snap_by_symbol.update(data)
    except Exception as e:
        logger.warning("snapshots fetch failed (continuing without): %s", e)

    # Pre-market detection
    in_pm = _is_premarket()
    elapsed_pm = _pm_elapsed_minutes() if in_pm else 390
    session_start = _today_session_start_utc() if in_pm else None

    if in_pm:
        logger.info("Pre-market mode: %d min elapsed since 4am ET", elapsed_pm)

    # Pass 1 — filter on all non-float criteria, collect survivors
    pre_candidates = []
    for g in gainers:
        sym = g.get("symbol", "")
        if not sym:
            continue

        gap_pct = float(g.get("percent_change") or 0)
        if gap_pct < GAP_MIN_PCT:
            continue

        snap = snap_by_symbol.get(sym, {}) or {}
        daily = snap.get("dailyBar") or {}
        prev = snap.get("prevDailyBar") or {}
        latest_trade = snap.get("latestTrade") or {}

        price = float(
            latest_trade.get("p") or daily.get("c") or daily.get("o") or g.get("price") or 0
        )
        if not PRICE_MIN <= price <= PRICE_MAX:
            continue

        prev_vol = float(prev.get("v") or 0)

        if in_pm:
            # Only count volume if dailyBar is from today's session
            # (if the stock hasn't traded yet today, dailyBar is yesterday's — skip)
            pm_active = False
            bar_t = daily.get("t", "")
            if bar_t:
                try:
                    bar_dt = datetime.strptime(bar_t, "%Y-%m-%dT%H:%M:%SZ").replace(
                        tzinfo=timezone.utc)
                    pm_active = bar_dt >= session_start
                except Exception:
                    pass
            if not pm_active:
                continue  # not trading pre-market yet today
            today_vol = int(daily.get("v") or 0)
            # Normalize RVOL: at this PM rate, what's the full-session equivalent?
            rvol = (today_vol / elapsed_pm * 390) / prev_vol if prev_vol > 0 else 1.0
        else:
            today_vol = int(daily.get("v") or 0)
            rvol = (today_vol / prev_vol) if prev_vol > 0 else 1.0

        if today_vol < PREMARKET_VOL_MIN:
            continue

        if rvol < RVOL_MIN:
            continue

        pre_candidates.append({
            "sym": sym, "gap_pct": gap_pct, "rvol": rvol,
            "price": price, "today_vol": today_vol,
            "prev_high": float(prev.get("h") or 0),
            "prev_low":  float(prev.get("l") or 0),
        })

    if not pre_candidates:
        return []

    # Pass 2 — fetch all floats in parallel (cache hits are free)
    float_cache = _load_float_cache()
    float_data = _fetch_floats_parallel([c["sym"] for c in pre_candidates], float_cache)

    # Pass 3 — apply float filter, score, build Candidate list
    candidates = []
    for item in pre_candidates:
        sym = item["sym"]
        float_shares, stale = float_data.get(sym, (0, True))

        if float_shares > 0:
            float_m = float_shares / 1_000_000
            if not FLOAT_MIN_M <= float_m <= FLOAT_MAX_M:
                continue

        sc, comps = _score(item["gap_pct"], item["rvol"], float_shares or 5_000_000)
        lvls = quick_levels(item["price"], item["prev_high"], item["prev_low"])
        candidates.append(Candidate(
            ticker=sym,
            gap_pct=item["gap_pct"],
            rvol=round(item["rvol"], 2),
            premarket_vol=item["today_vol"],
            float_shares=float_shares,
            price=item["price"],
            score=sc,
            score_components=comps,
            float_stale=stale,
            prev_high=lvls["prev_high"],
            prev_low=lvls["prev_low"],
            whole_dollar_above=lvls["whole_dollar_above"],
            whole_dollar_below=lvls["whole_dollar_below"],
        ))

    _save_float_cache(float_cache)
    return sorted(candidates, key=lambda c: c.score, reverse=True)


# ── Historical path ───────────────────────────────────────────────────────────

def _historical_screen(date_str: str) -> list:
    """
    Reconstruct candidates for a given date using Alpaca daily + intraday bars.

    Pre-market volume approximation: total daily volume is used as proxy.
    True 4am-9:30am volume would require per-ticker intraday pulls for the
    full universe — cost-prohibitive on free tier for >200 symbols.
    """
    try:
        target = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        logger.error("Invalid date: %s (expected YYYY-MM-DD)", date_str)
        return []

    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    except ImportError:
        logger.error("alpaca-py not installed — run: pip install alpaca-py")
        return []

    api_key = os.environ.get("ALPACA_API_KEY", "")
    secret = os.environ.get("ALPACA_SECRET_KEY", "")
    if not api_key or not secret:
        logger.error("ALPACA_API_KEY / ALPACA_SECRET_KEY not set")
        return []

    client = StockHistoricalDataClient(api_key=api_key, secret_key=secret)
    start_window = target - timedelta(days=35)   # 35 calendar days ≈ 25 trading days
    end_window = target + timedelta(days=1)

    # Prefer the dynamic universe (built via universe.py) over the static fallback
    from app.services.seneca.universe import load_universe
    dynamic = load_universe()
    universe = dynamic if dynamic else list(SMALL_CAP_UNIVERSE)

    # Batch-fetch daily bars for the full universe
    all_bars: dict = {}
    for i in range(0, len(universe), 50):
        chunk = universe[i : i + 50]
        try:
            req = StockBarsRequest(
                symbol_or_symbols=chunk,
                timeframe=TimeFrame(1, TimeFrameUnit.Day),
                start=start_window,
                end=end_window,
                feed="iex",
            )
            data = client.get_stock_bars(req).data
            all_bars.update(data)
        except Exception as e:
            logger.warning("Daily bars failed for chunk %s...: %s", chunk[:2], e)
        time.sleep(0.3)

    float_cache = _load_float_cache()
    pre_candidates = []

    for sym, bars in all_bars.items():
        date_bars = sorted(bars, key=lambda b: b.timestamp)

        # Locate target date bar
        target_idx = next(
            (i for i, b in enumerate(date_bars) if b.timestamp.date() == target.date()),
            None,
        )
        if target_idx is None or target_idx == 0:
            continue

        today_bar = date_bars[target_idx]
        prev_bar = date_bars[target_idx - 1]

        price = float(today_bar.open or 0)
        if not PRICE_MIN <= price <= PRICE_MAX:
            continue

        prev_close = float(prev_bar.close or 0)
        if prev_close <= 0:
            continue

        gap_pct = (price - prev_close) / prev_close * 100
        if gap_pct < GAP_MIN_PCT:
            continue

        prior = date_bars[max(0, target_idx - 20) : target_idx]
        if len(prior) < 5:
            continue
        avg_vol = sum(float(b.volume or 0) for b in prior) / len(prior)
        today_vol = float(today_bar.volume or 0)
        rvol = today_vol / avg_vol if avg_vol > 0 else 1.0
        if rvol < RVOL_MIN:
            continue

        if today_vol < PREMARKET_VOL_MIN:
            continue

        pre_candidates.append({
            "sym": sym,
            "gap_pct": gap_pct,
            "rvol": rvol,
            "price": price,
            "premarket_vol": int(today_vol),
            "prev_high": float(prev_bar.high or 0),
            "prev_low":  float(prev_bar.low  or 0),
        })

    candidates = []
    for item in pre_candidates:
        sym = item["sym"]
        float_shares, stale = _get_float(sym, float_cache)
        # Historical mode: float is today's snapshot, not the target date's.
        # Apply as a score weight rather than a hard cut — large-float stocks
        # score lower; we still surface them so coverage/precision can be measured.
        # (Precision Killer #2 — see scanner-spec.md)
        float_for_scoring = float_shares if float_shares > 0 else 5_000_000

        sc, comps = _score(item["gap_pct"], item["rvol"], float_for_scoring)
        comps["float_note"] = "historical_soft" if float_shares > FLOAT_MAX_M * 1_000_000 else "ok"
        lvls = quick_levels(item["price"], item["prev_high"], item["prev_low"])
        candidates.append(Candidate(
            ticker=sym,
            gap_pct=item["gap_pct"],
            rvol=round(item["rvol"], 2),
            premarket_vol=item["premarket_vol"],
            float_shares=float_shares,
            price=item["price"],
            score=sc,
            score_components=comps,
            float_stale=stale,
            prev_high=lvls["prev_high"],
            prev_low=lvls["prev_low"],
            whole_dollar_above=lvls["whole_dollar_above"],
            whole_dollar_below=lvls["whole_dollar_below"],
        ))

    _save_float_cache(float_cache)
    return sorted(candidates, key=lambda c: c.score, reverse=True)


# ── In-memory cache ───────────────────────────────────────────────────────────
# Key: date_str (YYYY-MM-DD) → (fetched_at: float, results: list)
# Market-closed data is immutable for that date → never expires mid-session.
# Live (market open) data expires after 2 min so scanner refreshes naturally.

import time as _time

_SCAN_CACHE: dict = {}
_LIVE_TTL  = 120   # seconds — match the frontend auto-refresh interval
_HIST_TTL  = 3600  # seconds — historical data is static; keep for 1h


def _last_trading_day_str() -> str:
    from datetime import date, timedelta
    d = date.today()
    if d.weekday() == 5:
        d -= timedelta(days=1)
    elif d.weekday() == 6:
        d -= timedelta(days=2)
    return d.isoformat()


def _market_is_open() -> bool:
    """True only during regular US market hours (9:30–16:00 ET, weekdays)."""
    from datetime import datetime, timezone, timedelta
    now = datetime.now(tz=timezone.utc)
    if now.weekday() >= 5:
        return False
    # rough ET offset check (good enough for cache TTL decisions)
    off = -4 if (3 < now.month < 11) else -5
    et_hour  = now.hour + off
    et_min   = now.minute
    et_total = et_hour * 60 + et_min
    return 9 * 60 + 30 <= et_total < 16 * 60


def _is_premarket() -> bool:
    """True during 4:00–9:29am ET on weekdays."""
    from datetime import datetime, timezone
    now = datetime.now(tz=timezone.utc)
    if now.weekday() >= 5:
        return False
    off = -4 if (3 < now.month < 11) else -5
    et_total = (now.hour + off) * 60 + now.minute
    return 4 * 60 <= et_total < 9 * 60 + 30


def _pm_elapsed_minutes() -> int:
    """Minutes since 4:00am ET (capped at 330 = 5.5 h of pre-market)."""
    from datetime import datetime, timezone
    now = datetime.now(tz=timezone.utc)
    off = -4 if (3 < now.month < 11) else -5
    et_total = (now.hour + off) * 60 + now.minute
    return max(1, min(et_total - 4 * 60, 330))


def _today_session_start_utc() -> "datetime":
    """
    Alpaca labels the daily bar at 4am UTC (= midnight ET in EDT).
    Today's session bar will have t >= this timestamp.
    """
    from datetime import datetime, timezone
    now = datetime.now(tz=timezone.utc)
    off = -4 if (3 < now.month < 11) else -5
    return datetime(now.year, now.month, now.day, -off, 0, tzinfo=timezone.utc)


# ── Public API ────────────────────────────────────────────────────────────────

def screen(date: Optional[str] = None, live: bool = False) -> list:
    """
    screen(date=None, live=True)   -> live pre-market scan (Alpaca screener)
    screen(date="2025-06-01")      -> historical reconstruction for that date
    """
    is_live = live or date is None
    cache_key = date or _last_trading_day_str()
    ttl = _LIVE_TTL if (is_live and _market_is_open()) else _HIST_TTL

    cached = _SCAN_CACHE.get(cache_key)
    if cached and (_time.time() - cached[0]) < ttl:
        logger.debug("scanner cache hit: %s (age %.0fs)", cache_key, _time.time() - cached[0])
        return cached[1]

    results = _live_screen() if is_live else _historical_screen(cache_key)
    _SCAN_CACHE[cache_key] = (_time.time(), results)
    return results
