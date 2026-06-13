"""
Seneca Context Layer — Layer 2.

get_context(ticker, date_str, live) -> dict

  candles:         list[{timestamp_ms, open, high, low, close, volume, vwap}]
                   1-min bars from 4am ET. vwap is cumulative from 9:30am open;
                   null for pre-market bars.
  levels:          list[{price, label, tier, side, color, dash}]
                   Tier 1 — structural: PDH, PDC, PDL, PM High/Low, OR High/Low
                   Tier 2 — recent memory: 5-day swing H/L
                   Tier 3 — psychological: round-dollar levels (deduped against structural)
  bias:            {direction, label, score, color, reasons}
  session_open_ms: 9:30am ET in ms (frontend uses this to draw the market-open divider)
  current_price:   latest close from intraday bars
"""

import logging
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.services.seneca.patterns import detect_patterns

logger = logging.getLogger(__name__)

# ── Colour palette ────────────────────────────────────────────────────────────
_COLORS = {
    ("resistance", 1): "#ef5350",
    ("resistance", 2): "#ff7043",
    ("resistance", 3): "#ffca28",
    ("support",    1): "#26a69a",
    ("support",    2): "#66bb6a",
    ("support",    3): "#a5d6a7",
    ("neutral",    1): "#888888",
}

def _color(side: str, tier: int) -> str:
    return _COLORS.get((side, tier), "#90a4ae")


# ── ET timezone helpers ───────────────────────────────────────────────────────

def _is_edt(dt: datetime) -> bool:
    m = dt.month
    return 3 < m < 11 or (m == 3 and dt.day >= 8) or (m == 11 and dt.day < 1)

def _et_offset(dt: datetime) -> int:
    """UTC offset hours for US/Eastern: -4 (EDT) or -5 (EST)."""
    return -4 if _is_edt(dt) else -5

def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def _session_times(target: datetime):
    """
    Returns (premarket_start_utc, session_open_utc) for target date (UTC midnight).
    Pre-market: 4:00am ET.  Session open: 9:30am ET.
    """
    off = _et_offset(target)
    # off is negative, so hour - off gives UTC hour
    pm_start = target.replace(
        hour=4 - off, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    s_open = target.replace(
        hour=9 - off, minute=30, second=0, microsecond=0, tzinfo=timezone.utc
    )
    return pm_start, s_open


# ── Level builder ─────────────────────────────────────────────────────────────

def _level(price: float, label: str, tier: int, side: str, dash: bool = False) -> dict:
    return {
        "price": round(price, 4),
        "label": label,
        "tier":  tier,
        "side":  side,
        "color": _color(side, tier),
        "dash":  dash,
    }

def _side(level_price: float, current: float) -> str:
    if level_price > current:
        return "resistance"
    if level_price < current:
        return "support"
    return "neutral"

def _near(p: float, prices: list, tol: float = 0.05) -> bool:
    return any(abs(p - x) < tol for x in prices)


# ── Swing high/low (5-day) ────────────────────────────────────────────────────

def _swing_levels(daily_bars, n: int = 5):
    bars = daily_bars[-n:] if len(daily_bars) >= n else daily_bars
    if not bars:
        return 0.0, 0.0
    return max(float(b.high) for b in bars), min(float(b.low) for b in bars)


# ── Round-dollar levels ───────────────────────────────────────────────────────

def _round_dollars(current: float, structural: list) -> list:
    """
    $1 round levels within ±15% of current price.
    Skipped if within $0.08 of any structural level (avoid label clutter).
    """
    lo = current * 0.85
    hi = current * 1.15
    levels = []
    for n in range(math.floor(lo), math.ceil(hi) + 1):
        p = float(n)
        if lo <= p <= hi and not _near(p, structural, tol=0.08):
            levels.append(_level(p, f"${n}", 3, _side(p, current)))
    return levels


# ── VWAP series ───────────────────────────────────────────────────────────────

def _vwap_series(bars_sorted: list, session_open: datetime) -> dict:
    """
    Returns {timestamp_ms: vwap_or_None} for each bar.
    VWAP accumulates from session open; pre-market bars get None.
    """
    open_ms = _to_ms(session_open)
    cum_tpv = cum_vol = 0.0
    result = {}
    for b in bars_sorted:
        ts_ms = _to_ms(b.timestamp)
        if ts_ms < open_ms:
            result[ts_ms] = None
            continue
        tp = (float(b.high) + float(b.low) + float(b.close)) / 3
        vol = float(b.volume or 0)
        cum_tpv += tp * vol
        cum_vol  += vol
        result[ts_ms] = round(cum_tpv / cum_vol, 4) if cum_vol > 0 else None
    return result


# ── Bias ──────────────────────────────────────────────────────────────────────

def _bias(price: float, vwap, pdh: float, pdc: float, pdl: float,
          pm_high: float, pm_low: float) -> dict:
    score = 0
    reasons = []

    if vwap:
        if price > vwap:
            score += 2; reasons.append(f"Above VWAP (${vwap:.2f})")
        else:
            score -= 2; reasons.append(f"Below VWAP (${vwap:.2f})")

    if pdh:
        if price > pdh:
            score += 2; reasons.append(f"Above PDH (${pdh:.2f}) — breakout territory")
        elif pdc and price > pdc:
            score += 1; reasons.append(f"Gapped up, approaching PDH (${pdh:.2f})")
        elif pdc and price < pdc:
            score -= 1; reasons.append("Gap filling — below prev close")

    if pdl and price < pdl:
        score -= 2; reasons.append(f"Below PDL (${pdl:.2f}) — breakdown")

    if pm_high:
        if price >= pm_high * 0.99:
            reasons.append("Holding pre-market high")
        elif price < pm_high * 0.94:
            reasons.append("Fading from pre-market high")

    if score >= 3:
        direction, label, color = "bullish",      "Strong bullish bias",    "#26a69a"
    elif score >= 1:
        direction, label, color = "bullish_lean", "Bullish lean",           "#66bb6a"
    elif score <= -3:
        direction, label, color = "bearish",      "Bearish bias",           "#ef5350"
    elif score <= -1:
        direction, label, color = "bearish_lean", "Bearish lean",           "#ff7043"
    else:
        direction, label, color = "neutral",      "Neutral — key test ahead", "#ffa726"

    return {"direction": direction, "label": label,
            "score": score, "color": color, "reasons": reasons}


# ── Daily-chart path ─────────────────────────────────────────────────────────

def _get_context_daily(ticker: str, client, target: datetime) -> dict:
    """6-month daily bars for the daily chart pane. No VWAP, no pre-market."""
    from alpaca.data.requests import StockBarsRequest
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

    start = target - timedelta(days=185)
    daily_all = []
    try:
        req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame(1, TimeFrameUnit.Day),
            start=start,
            end=target + timedelta(days=1),
            feed="iex",
        )
        daily_all = sorted(
            client.get_stock_bars(req).data.get(ticker, []),
            key=lambda b: b.timestamp,
        )
    except Exception as e:
        logger.warning("Daily bars (chart) failed for %s: %s", ticker, e)
        return {}

    if not daily_all:
        return {}

    current = float(daily_all[-1].close or 0)
    prev_bars = daily_all[:-1]
    pdh = pdc = pdl = swing_h = swing_l = 0.0
    if prev_bars:
        prev = prev_bars[-1]
        pdh, pdc, pdl = float(prev.high), float(prev.close), float(prev.low)
        swing_h, swing_l = _swing_levels(prev_bars, n=5)

    candles = [
        {
            "timestamp": _to_ms(b.timestamp),
            "open":   round(float(b.open  or 0), 4),
            "high":   round(float(b.high  or 0), 4),
            "low":    round(float(b.low   or 0), 4),
            "close":  round(float(b.close or 0), 4),
            "volume": int(b.volume or 0),
            "vwap":   None,
        }
        for b in daily_all
    ]

    levels, structural = [], []
    def add(lvl):
        levels.append(lvl); structural.append(lvl["price"])
    if pdh: add(_level(pdh, "PDH", 1, _side(pdh, current)))
    if pdc: add(_level(pdc, "PDC", 1, _side(pdc, current), dash=True))
    if pdl: add(_level(pdl, "PDL", 1, _side(pdl, current)))
    if swing_h and not _near(swing_h, structural):
        add(_level(swing_h, "5D High", 2, _side(swing_h, current), dash=True))
    if swing_l and not _near(swing_l, structural):
        add(_level(swing_l, "5D Low",  2, _side(swing_l, current), dash=True))
    levels.extend(_round_dollars(current, structural))
    levels.sort(key=lambda l: l["price"], reverse=True)

    return {
        "ticker":          ticker,
        "date":            target.date().isoformat(),
        "timeframe":       "1d",
        "candles":         candles,
        "levels":          levels,
        "bias":            _bias(current, None, pdh, pdc, pdl, 0.0, 0.0),
        "session_open_ms": 0,
        "current_price":   current,
        "patterns":       [],
    }


# ── In-memory cache ───────────────────────────────────────────────────────────
import time as _time

_CTX_CACHE: dict = {}
_LIVE_TTL  = 300   # 5 min during market hours (intraday bars change slowly)
_HIST_TTL  = 7200  # 2 h for historical / market-closed data


def _market_is_open() -> bool:
    now = datetime.now(tz=timezone.utc)
    if now.weekday() >= 5:
        return False
    off = -4 if (3 < now.month < 11) else -5
    et_total = (now.hour + off) * 60 + now.minute
    return 9 * 60 + 30 <= et_total < 16 * 60


# ── Public entry ──────────────────────────────────────────────────────────────

def get_context(ticker: str, date_str: Optional[str] = None,
                live: bool = False, timeframe: str = "1m") -> dict:
    try:
        from alpaca.data.historical import StockHistoricalDataClient
        from alpaca.data.requests import StockBarsRequest
        from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    except ImportError:
        logger.error("alpaca-py not installed")
        return {}

    api_key = os.environ.get("ALPACA_API_KEY", "")
    secret  = os.environ.get("ALPACA_SECRET_KEY", "")
    if not api_key or not secret:
        logger.error("Alpaca credentials not set")
        return {}

    # ── Cache check (before any Alpaca call) ──────────────────────────────────
    is_live = live or not date_str
    ttl = _LIVE_TTL if (is_live and _market_is_open()) else _HIST_TTL
    cache_key = (ticker, date_str or "live", timeframe)
    cached = _CTX_CACHE.get(cache_key)
    if cached and (_time.time() - cached[0]) < ttl:
        logger.debug("context cache hit: %s %s %s", ticker, date_str, timeframe)
        return cached[1]

    if live or not date_str:
        now_utc = datetime.now(tz=timezone.utc)
        wd = now_utc.weekday()
        if wd == 5:    # Saturday → Friday
            now_utc -= timedelta(days=1)
        elif wd == 6:  # Sunday → Friday
            now_utc -= timedelta(days=2)
        target = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        try:
            target = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error("Invalid date: %s", date_str)
            return {}

    client = StockHistoricalDataClient(api_key=api_key, secret_key=secret)

    # Daily timeframe takes a completely different path
    if timeframe == "1d":
        return _get_context_daily(ticker, client, target)

    tf_minutes = 5 if timeframe == "5m" else 1
    pm_start, session_open = _session_times(target)
    session_open_ms = _to_ms(session_open)

    # ── Intraday bars (4am ET – end of day) ───────────────────────────────────
    intraday = []
    try:
        req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame(tf_minutes, TimeFrameUnit.Minute),
            start=pm_start,
            end=target + timedelta(days=1),
            feed="iex",
        )
        raw = client.get_stock_bars(req).data.get(ticker, [])
        intraday = sorted(
            [b for b in raw if b.timestamp.date() == target.date()],
            key=lambda b: b.timestamp,
        )
    except Exception as e:
        logger.warning("Intraday bars failed for %s: %s", ticker, e)
        return {}

    if not intraday:
        logger.warning("No intraday data for %s on %s", ticker, target.date())
        return {}

    # ── Daily bars (last 15 calendar days → ~10 trading days) ────────────────
    pdh = pdc = pdl = swing_h = swing_l = 0.0
    try:
        dreq = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame(1, TimeFrameUnit.Day),
            start=target - timedelta(days=15),
            end=target,
            feed="iex",
        )
        daily = sorted(
            client.get_stock_bars(dreq).data.get(ticker, []),
            key=lambda b: b.timestamp,
        )
        if daily:
            prev = daily[-1]
            pdh, pdc, pdl = float(prev.high), float(prev.close), float(prev.low)
        if len(daily) >= 2:
            swing_h, swing_l = _swing_levels(daily[:-1], n=5)
    except Exception as e:
        logger.warning("Daily bars failed for %s: %s", ticker, e)

    # ── Pre-market H/L ────────────────────────────────────────────────────────
    pm_bars = [b for b in intraday if b.timestamp < session_open]
    pm_high = max((float(b.high) for b in pm_bars), default=0.0) if pm_bars else 0.0
    pm_low  = min((float(b.low)  for b in pm_bars), default=0.0) if pm_bars else 0.0

    # ── Opening Range (9:30 – 9:35 ET) ───────────────────────────────────────
    or_end   = session_open + timedelta(minutes=5)
    or_bars  = [b for b in intraday if session_open <= b.timestamp < or_end]
    or_high  = max((float(b.high) for b in or_bars), default=0.0) if or_bars else 0.0
    or_low   = min((float(b.low)  for b in or_bars), default=0.0) if or_bars else 0.0

    # ── VWAP series ───────────────────────────────────────────────────────────
    vmap = _vwap_series(intraday, session_open)
    latest_vwap = next((v for v in reversed(list(vmap.values())) if v is not None), None)

    # ── Current price ─────────────────────────────────────────────────────────
    current = float(intraday[-1].close or 0)

    # ── Candles ───────────────────────────────────────────────────────────────
    candles = [
        {
            "timestamp": _to_ms(b.timestamp),
            "open":   round(float(b.open  or 0), 4),
            "high":   round(float(b.high  or 0), 4),
            "low":    round(float(b.low   or 0), 4),
            "close":  round(float(b.close or 0), 4),
            "volume": int(b.volume or 0),
            "vwap":   vmap.get(_to_ms(b.timestamp)),
        }
        for b in intraday
    ]

    # ── Levels ────────────────────────────────────────────────────────────────
    levels = []
    structural = []

    def add(lvl):
        levels.append(lvl)
        structural.append(lvl["price"])

    if pdh:   add(_level(pdh, "PDH",    1, _side(pdh, current)))
    if pdc:   add(_level(pdc, "PDC",    1, _side(pdc, current), dash=True))
    if pdl:   add(_level(pdl, "PDL",    1, _side(pdl, current)))
    if pm_high: add(_level(pm_high, "PM High", 1, _side(pm_high, current), dash=True))
    if pm_low:  add(_level(pm_low,  "PM Low",  1, _side(pm_low,  current), dash=True))
    if or_high and not _near(or_high, structural):
        add(_level(or_high, "OR High", 1, _side(or_high, current), dash=True))
    if or_low and not _near(or_low, structural):
        add(_level(or_low, "OR Low", 1, _side(or_low, current), dash=True))
    if swing_h and not _near(swing_h, structural):
        add(_level(swing_h, "5D High", 2, _side(swing_h, current), dash=True))
    if swing_l and not _near(swing_l, structural):
        add(_level(swing_l, "5D Low",  2, _side(swing_l, current), dash=True))

    levels.extend(_round_dollars(current, structural))
    levels.sort(key=lambda l: l["price"], reverse=True)

    result = {
        "ticker":          ticker,
        "date":            target.date().isoformat(),
        "timeframe":       timeframe,
        "candles":         candles,
        "levels":          levels,
        "bias":            _bias(current, latest_vwap, pdh, pdc, pdl, pm_high, pm_low),
        "session_open_ms": session_open_ms,
        "current_price":   current,
        "patterns":        detect_patterns(candles, timeframe, live),
    }
    _CTX_CACHE[cache_key] = (_time.time(), result)
    return result
