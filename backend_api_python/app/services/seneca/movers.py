"""
Seneca movers service — live top gainers, losers, most actives.

Strategy (two paths, chosen by market state):

  MARKET HOURS (9:30am–4pm ET):
    Alpaca screener resets at open → symbols + prices are current.
    Pass through screener data directly.

  PRE-MARKET (4am–9:30am ET):
    The Alpaca screener still shows yesterday's symbols (it resets at 9:30).
    Symbol discovery: Finviz screener (real-time pre-market coverage).
    Data enrichment: Alpaca snapshots for prices / prev-close / volume.
    Ranking: recomputed from snapshot data (change_pct = current vs prev close).

Screener behaviour (from Alpaca docs):
  - percent_change = (current_price / previous_eod_close) - 1
  - For stocks: the endpoint resets at market open. Until then it shows the
    previous session's movers.
"""
import json
import logging
import os
import re
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_ALPACA_DATA = "https://data.alpaca.markets"
_TOP_MOVERS_URL   = f"{_ALPACA_DATA}/v1beta1/screener/stocks/movers"
_MOST_ACTIVES_URL = f"{_ALPACA_DATA}/v1beta1/screener/stocks/most-actives"
_SNAPSHOTS_URL    = f"{_ALPACA_DATA}/v2/stocks/snapshots"
_BARS_URL          = f"{_ALPACA_DATA}/v2/stocks/bars"

# Finviz screener: pre-market gainers — current volume > 100 sh, price > $1,
# sorted by % change descending.  This catches stocks moving NOW, which the
# stale Alpaca screener misses before 9:30am.
_FINVIZ_GAINERS_URL = (
    "https://finviz.com/screener.ashx?v=111"
    "&f=sh_curvol_o100,sh_price_o1"
    "&o=-change&r={start}"
)
_FINVIZ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; seneca-movers/1.0)",
    "Accept": "text/html",
}


def _headers() -> dict:
    return {
        "APCA-API-KEY-ID":     os.environ.get("ALPACA_API_KEY", ""),
        "APCA-API-SECRET-KEY": os.environ.get("ALPACA_SECRET_KEY", ""),
        "Accept": "application/json",
    }


def _get(url: str, params: dict = None) -> dict:
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=_headers())
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


# ── Market clock helpers ────────────────────────────────────────────────────

def _et_now() -> datetime:
    """Current datetime in US Eastern Time."""
    off = -4  # EDT (Mar–Nov); EST = -5
    return datetime.now(tz=timezone.utc) + timedelta(hours=off)


def _is_premarket() -> bool:
    """True 4:00–9:29am ET, Mon–Fri."""
    now = _et_now()
    if now.weekday() >= 5:
        return False
    return 4 * 60 <= now.hour * 60 + now.minute < 9 * 60 + 30


def _is_market_hours() -> bool:
    """True 9:30am–4:00pm ET, Mon–Fri."""
    now = _et_now()
    if now.weekday() >= 5:
        return False
    return 9 * 60 + 30 <= now.hour * 60 + now.minute < 16 * 60


# ── Cached universe (built from Alpaca assets, refreshed weekly) ───────────

_UNIVERSE_CACHE_PATH = Path("/app/data/alpaca_cache/movers_universe.json")


def _build_universe_from_alpaca() -> list:
    """
    Fetch ALL tradeable US equities from Alpaca and cache the symbol list.

    Runs once per week (on cache miss / stale).  The universe is the set of
    every stock we *could* scan during pre-market — no price / volume filter
    at build time.  Filtering happens at scan time (we only keep stocks with
    today's pre-market data).
    """
    import urllib.request as _ur
    api_key = os.environ.get("ALPACA_API_KEY", "")
    secret  = os.environ.get("ALPACA_SECRET_KEY", "")
    if not api_key:
        logger.warning("No Alpaca API key — universe build skipped")
        return []
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret,
        "Accept": "application/json",
    }
    all_symbols = []

    for exchange in ("NASDAQ", "NYSE"):
        try:
            url = (
                "https://paper-api.alpaca.markets/v2/assets"
                "?status=active&asset_class=us_equity"
                f"&exchange={exchange}"
            )
            req = _ur.Request(url, headers=headers)
            with _ur.urlopen(req, timeout=20) as r:
                data = json.loads(r.read())
            # Alpaca returns a raw JSON array, not {assets: [...]}
            if isinstance(data, list):
                assets = data
            elif isinstance(data, dict):
                assets = data.get("assets", [])
            else:
                logger.warning("Unexpected assets response type for %s: %s", exchange, type(data))
                continue
            count = sum(1 for a in assets if a.get("tradable"))
            all_symbols.extend(a["symbol"] for a in assets if a.get("tradable"))
            logger.info("Fetched %s: %d assets (%d tradable), total symbols: %d",
                        exchange, len(assets), count, len(all_symbols))
        except Exception as e:
            logger.warning("%s assets failed: %s", exchange, e)

    # Deduplicate and cache
    symbols = list(dict.fromkeys(all_symbols))
    _UNIVERSE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _UNIVERSE_CACHE_PATH.write_text(json.dumps({
        "built_at": datetime.now(tz=timezone.utc).isoformat(),
        "count": len(symbols),
        "symbols": symbols,
    }))
    logger.info("Movers universe built: %d symbols cached", len(symbols))
    return symbols


def _load_cached_universe() -> list:
    """Load cached universe. Returns empty list if cache missing or stale (>7 days)."""
    if not _UNIVERSE_CACHE_PATH.exists():
        return []
    try:
        data = json.loads(_UNIVERSE_CACHE_PATH.read_text())
        built = datetime.fromisoformat(data["built_at"])
        if (datetime.now(tz=timezone.utc) - built).days > 7:
            logger.info("Movers universe cache stale (>7d), will rebuild")
            return []
        return data.get("symbols", [])
    except Exception:
        return []


def _get_premarket_universe() -> list:
    """
    Return the symbol universe for pre-market scanning.

    Priority (fast to slow):
      1. Cached Alpaca assets universe (comprehensive, NASDAQ+NYSE, ~4000 symbols)
      2. On cache miss: build from scratch (slow, happens once per week)
      3. Fallback: Finviz scraper + screener symbols (fastest but incomplete)
    """
    # Try cached universe first
    cached = _load_cached_universe()
    if cached:
        return cached

    # Try to build
    try:
        return _build_universe_from_alpaca()
    except Exception as e:
        logger.warning("Universe build failed, using fallback: %s", e)

    # Fallback: Finviz + screener
    symbols = _scrape_finviz_gainers(max_symbols=80)
    try:
        data = _get(_TOP_MOVERS_URL, {"top": 30})
        for m in data.get("gainers", []):
            if m.get("symbol") and m["symbol"] not in symbols:
                symbols.append(m["symbol"])
        for m in data.get("losers", []):
            if m.get("symbol") and m["symbol"] not in symbols:
                symbols.append(m["symbol"])
    except Exception:
        pass
    try:
        data = _get(_MOST_ACTIVES_URL, {"top": 30, "by": "volume"})
        for m in data.get("most_actives", []):
            if m.get("symbol") and m["symbol"] not in symbols:
                symbols.append(m["symbol"])
    except Exception:
        pass
    return symbols


# ── Pre-market cumulative volume ────────────────────────────────────────────

def _fetch_premarket_volumes(symbols: list) -> dict:
    """
    Fetch today's cumulative pre-market volume for a list of symbols.

    Uses the Alpaca bars API (1-min bars from 4am UTC = midnight ET).
    Returns {SYM: total_volume}.

    IEX limitation: only covers 8am ET onwards (~18% of consolidated volume).
    """
    vol_map = {}
    if not symbols:
        return vol_map

    # 4am UTC = midnight ET (EDT).  This catches all of today's bars.
    today_4am = datetime.now(tz=timezone.utc).replace(
        hour=4, minute=0, second=0, microsecond=0
    )
    start_str = today_4am.strftime("%Y-%m-%dT%H:%M:%SZ")

    for i in range(0, len(symbols), 50):
        chunk = symbols[i:i + 50]
        try:
            data = _get(_BARS_URL, {
                "symbols": ",".join(chunk),
                "timeframe": "1Min",
                "start": start_str,
                "limit": "1000",
                "feed": "iex",
                "sort": "asc",
            })
            bars = data.get("bars", {})
            # Multi-symbol: {"bars": {"SYM1": [...], "SYM2": [...]}}
            # Single-symbol: {"bars": [...]}
            if isinstance(bars, dict):
                for sym, sym_bars in bars.items():
                    vol_map[sym] = sum(int(b.get("v") or 0) for b in sym_bars)
            elif isinstance(bars, list):
                for b in bars:
                    sym = b.get("S") or b.get("symbol", "")
                    v = int(b.get("v") or 0)
                    vol_map[sym] = vol_map.get(sym, 0) + v
        except Exception as e:
            logger.warning("premarket volume fetch failed for chunk %s: %s",
                           chunk[:2], e)
    return vol_map


# ── Finviz scraper (pre-market symbol discovery) ────────────────────────────

def _scrape_finviz_gainers(max_symbols: int = 60) -> list:
    """
    Scrape Finviz screener for current top % gainers.

    Uses the Finviz screener with pre-market-friendly filters:
    current volume > 100 shares, price > $1, sorted by % change desc.
    Returns a list of unique ticker symbols.
    """
    all_symbols = []
    for page in range(3):  # up to 3 pages × 20 = 60 symbols
        start = page * 20 + 1
        url = _FINVIZ_GAINERS_URL.format(start=start)
        try:
            req = urllib.request.Request(url, headers=_FINVIZ_HEADERS)
            with urllib.request.urlopen(req, timeout=12) as r:
                html = r.read().decode("utf-8", errors="ignore")
            # Finviz tickers appear in data-boxover-ticker attributes
            tickers = re.findall(r'data-boxover-ticker="([A-Z]{1,6})"', html)
            tickers = list(dict.fromkeys(tickers))  # dedup, preserve order
            new = [t for t in tickers if t not in all_symbols]
            if not new:
                break
            all_symbols.extend(new)
        except Exception as e:
            logger.warning("Finviz gainers page %d failed: %s", page, e)
            break

    logger.info("Finviz gainers: %d symbols scraped", len(all_symbols))
    return all_symbols[:max_symbols]


def _fetch_snapshots(symbols: list) -> dict:
    """
    Fetch current snapshots for a list of symbols. Returns {SYM: snapshot_dict}.
    IEX only — SIP returns 403 on the free plan.
    """
    snap_map = {}
    if not symbols:
        return snap_map
    for i in range(0, len(symbols), 50):
        chunk = symbols[i:i + 50]
        try:
            data = _get(_SNAPSHOTS_URL, {"symbols": ",".join(chunk), "feed": "iex"})
            snap_map.update(data)
        except Exception as e:
            logger.warning("snapshot fetch failed for chunk %s: %s", chunk[:2], e)
    return snap_map


def _format_row(item: dict, rank: int) -> dict:
    """Format a screener result item into a standard cockpit row."""
    price = item.get("price")
    pct   = item.get("percent_change")
    vol   = item.get("volume")
    return {
        "ticker":     item.get("symbol", ""),
        "price":      round(float(price), 2) if price is not None else 0.0,
        "change_pct": round(float(pct), 2) if pct is not None else 0.0,
        "volume":     int(vol) if vol is not None else 0,
        "rank":       rank,
    }


def _today_str_utc() -> str:
    """Today's date string in UTC, e.g. '2026-06-16'."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


def _snapshot_is_fresh(snap: dict) -> bool:
    """Return True if the snapshot has trade or minute-bar data from today."""
    today = _today_str_utc()
    # Check minuteBar first (most reliable pre-market indicator)
    mb = snap.get("minuteBar") or {}
    if mb.get("t", "").startswith(today):
        return True
    # Check latestTrade
    lt = snap.get("latestTrade") or {}
    if lt.get("t", "").startswith(today):
        return True
    return False


def _enrich_prices(rows: list, snap_map: dict) -> list:
    """
    Fill in price / change_pct from snapshots for rows where the screener
    didn't provide them (e.g. most-actives returns no price data).

    Price priority:
      1. latestTrade.p        — actual trade (most reliable)
      2. latestQuote midpoint  — only when spread is reasonable (< 5×)
      3. dailyBar.c            — yesterday's close (stale but sane)
    """
    for r in rows:
        sym = r["ticker"]
        snap = snap_map.get(sym, {}) or {}
        if r["price"] <= 0:
            lt = snap.get("latestTrade") or {}
            lq = snap.get("latestQuote") or {}
            db = snap.get("dailyBar") or {}
            trade_p = float(lt.get("p") or 0)
            bp = float(lq.get("bp") or 0)
            ap = float(lq.get("ap") or 0)
            if trade_p > 0:
                r["price"] = round(trade_p, 2)
            elif bp > 0 and ap > 0 and max(ap, bp) / min(ap, bp) < 5:
                r["price"] = round((bp + ap) / 2, 2)
            elif bp > 0:
                r["price"] = round(bp, 2)
            elif ap > 0:
                r["price"] = round(ap, 2)
            elif float(db.get("c") or 0) > 0:
                r["price"] = round(float(db["c"]), 2)
        if r["change_pct"] == 0.0 and r["price"] > 0:
            prev = snap.get("prevDailyBar") or {}
            prev_close = float(prev.get("c") or 0)
            if prev_close > 0:
                r["change_pct"] = round((r["price"] - prev_close) / prev_close * 100, 2)
    return rows


def _build_from_snapshots(symbols: list, snap_map: dict, top: int,
                          vol_map: dict = None) -> tuple:
    """
    Build gainers, losers, actives lists from snapshot data.

    For each symbol with snapshot data:
      - price from latestTrade.p (if today) or dailyBar.c (yesterday's close)
      - change_pct = (price - prevDailyBar.c) / prevDailyBar.c * 100
      - volume from vol_map (today's cumulative pre-market) if provided,
        otherwise from dailyBar.v

    Sorts fresh symbols (today's data) first, then stale symbols.
    Most Actives sorted by volume desc.
    Returns (gainers, losers, actives).
    """
    if vol_map is None:
        vol_map = {}
    rows = []
    today = _today_str_utc()

    for sym in symbols:
        if not sym:
            continue
        snap = snap_map.get(sym, {}) or {}
        lt = snap.get("latestTrade") or {}
        db = snap.get("dailyBar") or {}
        pd = snap.get("prevDailyBar") or {}
        lq = snap.get("latestQuote") or {}

        # Price: prefer today's trade, fall back to yesterday's close
        lt_is_fresh = lt.get("t", "").startswith(today)
        trade_p = float(lt.get("p") or 0)
        if lt_is_fresh and trade_p > 0:
            price = trade_p
        else:
            # Stale trade — use yesterday's close as fallback
            bp = float(lq.get("bp") or 0)
            ap = float(lq.get("ap") or 0)
            if bp > 0 and ap > 0 and max(ap, bp) / min(ap, bp) < 5:
                price = (bp + ap) / 2
            elif bp > 0:
                price = bp
            elif trade_p > 0:
                price = trade_p
            else:
                price = float(db.get("c") or 0)

        if price < 0.005:  # sub-cent — untradeable or bad data
            continue

        # Filter out warrants / rights / units (noise in losers)
        if "." in sym and any(
            sym.endswith(s) for s in (".WS", ".RT", ".RW", ".U", ".UN")
        ):
            continue

        prev_close = float(pd.get("c") or 0)
        if prev_close > 0:
            change_pct = round((price - prev_close) / prev_close * 100, 2)
        else:
            change_pct = 0.0

        fresh = _snapshot_is_fresh(snap)

        # Volume: prefer today's cumulative pre-market, fall back to dailyBar.v
        if sym in vol_map and vol_map[sym] > 0:
            volume = vol_map[sym]
        else:
            volume = int(db.get("v") or 0)

        # Filter out untraded junk: no volume AND no fresh data today
        if volume <= 0 and not fresh:
            continue

        rows.append({
            "ticker":     sym,
            "price":      round(price, 2),
            "change_pct": change_pct,
            "volume":     volume,
            "fresh":      fresh,
        })

    # Sort: fresh first (by change_pct desc), then stale (by change_pct desc)
    fresh_rows = [r for r in rows if r["fresh"]]
    stale_rows = [r for r in rows if not r["fresh"]]
    fresh_rows.sort(key=lambda r: r["change_pct"], reverse=True)
    stale_rows.sort(key=lambda r: r["change_pct"], reverse=True)

    # Deduplicate by ticker (keep first occurrence — fresh_rows entries win)
    seen = set()
    all_sorted = []
    for r in fresh_rows + stale_rows:
        if r["ticker"] not in seen:
            seen.add(r["ticker"])
            all_sorted.append(r)

    # Gainers: top N by change_pct desc.  Copy rows — the same stock may also
    # appear in losers / actives, and we must not leak rank assignments across lists.
    gainers = [dict(r) for r in all_sorted[:top]]
    for i, r in enumerate(gainers):
        r["rank"] = i + 1
        r.pop("fresh", None)

    # Losers: fresh first (by change_pct asc), then stale (by change_pct asc)
    fresh_by_loss = sorted(fresh_rows, key=lambda r: r["change_pct"])
    stale_by_loss = sorted(stale_rows, key=lambda r: r["change_pct"])
    seen_loss = set()
    loss_sorted = []
    for r in fresh_by_loss + stale_by_loss:
        if r["ticker"] not in seen_loss:
            seen_loss.add(r["ticker"])
            loss_sorted.append(r)
    losers = [dict(r) for r in loss_sorted[:top]]
    for i, r in enumerate(losers):
        r["rank"] = i + 1
        r.pop("fresh", None)

    # Most actives: fresh first (by volume desc), then stale (by volume desc)
    fresh_by_vol = sorted(fresh_rows, key=lambda r: r["volume"], reverse=True)
    stale_by_vol = sorted(stale_rows, key=lambda r: r["volume"], reverse=True)
    seen_vol = set()
    vol_sorted = []
    for r in fresh_by_vol + stale_by_vol:
        if r["ticker"] not in seen_vol:
            seen_vol.add(r["ticker"])
            vol_sorted.append(r)
    by_vol = [dict(r) for r in vol_sorted[:top]]
    for i, r in enumerate(by_vol):
        r["rank"] = i + 1
        r.pop("fresh", None)

    return gainers, losers, by_vol


# ── Public API ──────────────────────────────────────────────────────────────

# Result cache for pre-market scans (expensive — 8k+ symbols, ~20s)
_RESULT_CACHE: dict = {}  # {cache_key: (timestamp, result_dict)}


def get_movers(top: int = 20) -> dict:
    """
    Returns {top_gainers, top_losers, most_actives, fetched_at}.

    Two paths, chosen by market state:

      MARKET HOURS (9:30am–4pm ET):
        Alpaca screener data directly — symbols + prices are current.

      PRE-MARKET / AFTER-HOURS:
        Broad Alpaca-universe scan with snapshots — the screener shows
        yesterday's symbols before 9:30, so we scan all NASDAQ+NYSE stocks
        for today's pre-market activity.

    Pre-market results are cached for 60s (the scan is expensive — 8k+ symbols).
    """
    import time as _time

    in_pm = _is_premarket()

    # ── Pre-market: check cache first ──
    cache_key = f"pm_{top}"
    if in_pm and cache_key in _RESULT_CACHE:
        ts, cached = _RESULT_CACHE[cache_key]
        if _time.time() - ts < 60:
            # Update fetched_at to reflect this request time
            cached["fetched_at"] = datetime.now(tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            return cached

    # ── Market hours: Alpaca screener directly ──
    if not in_pm:
        gainers, losers, actives = [], [], []

        try:
            data = _get(_TOP_MOVERS_URL, {"top": top})
            for i, m in enumerate(data.get("gainers", [])):
                gainers.append(_format_row(m, i + 1))
            for i, m in enumerate(data.get("losers", [])):
                losers.append(_format_row(m, i + 1))
        except Exception as e:
            logger.warning("movers fetch failed: %s", e)

        try:
            data = _get(_MOST_ACTIVES_URL, {"top": top, "by": "volume"})
            for i, m in enumerate(data.get("most_actives", [])):
                actives.append(_format_row(m, i + 1))
        except Exception as e:
            logger.warning("most-actives fetch failed: %s", e)

        if actives:
            active_symbols = [a["ticker"] for a in actives if a["ticker"]]
            snap_map = _fetch_snapshots(active_symbols)
            actives = _enrich_prices(actives, snap_map)

    # ── Pre-market: broad universe scan with Alpaca snapshots ──
    else:
        logger.info("PRE-MARKET: broad universe scan")
        # 1. Get symbol universe (cached Alpaca assets, or Finviz + screener fallback)
        pm_universe = _get_premarket_universe()
        logger.info("PRE-MARKET universe size: %d symbols", len(pm_universe))

        # 2. Parallel snapshot fetch
        snap_map = {}
        batch_size = 50
        batches = [
            pm_universe[i:i + batch_size]
            for i in range(0, len(pm_universe), batch_size)
        ]

        def _fetch_batch(symbols):
            return _fetch_snapshots(symbols)

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_fetch_batch, b): b for b in batches}
            for future in as_completed(futures):
                try:
                    snap_map.update(future.result(timeout=20))
                except Exception as e:
                    logger.warning("Snapshot batch failed: %s", e)

        # 3. First pass: identify fresh symbols (trading today)
        fresh_symbols = []
        for sym in pm_universe:
            snap = snap_map.get(sym, {}) or {}
            if _snapshot_is_fresh(snap):
                fresh_symbols.append(sym)

        # 4. Fetch today's cumulative pre-market volume for fresh symbols only
        pm_vol_map = _fetch_premarket_volumes(fresh_symbols)

        # 5. Build ranked lists — Most Actives uses today's cumulative volume
        gainers, losers, actives = _build_from_snapshots(
            pm_universe, snap_map, top, vol_map=pm_vol_map
        )

    fetched_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(
        "MOVERS mode=%s gainers(first5)=%s actives(first3)=%s",
        "premarket" if in_pm else "market",
        [(g["ticker"], g["price"], g["change_pct"]) for g in gainers[:5]],
        [(a["ticker"], a["price"], a["volume"]) for a in actives[:3]],
    )

    result = {
        "top_gainers":  gainers,
        "top_losers":   losers,
        "most_actives": actives,
        "fetched_at":   fetched_at,
    }

    # Cache pre-market results (expensive scan)
    if in_pm:
        _RESULT_CACHE[cache_key] = (_time.time(), result.copy())

    return result
