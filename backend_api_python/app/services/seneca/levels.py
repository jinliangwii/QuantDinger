"""
Seneca Context Layer — Layer 2.

Provides two levels of S/R data:

  quick_levels(price, prev_bar)  ->  dict
      No API call. Computed from data already in hand (prev daily bar + price).
      Returns: prev_high, prev_low, whole_dollar_above, whole_dollar_below.

  get_levels(ticker, date, live) ->  dict
      Fetches intraday bars from Alpaca (1-min, extended hours).
      Returns all quick_levels PLUS vwap, premarket_high, premarket_low.

All prices in USD. Times in US/Eastern implicitly; bar timestamps are UTC.
"""

import logging
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Pre-market window: 4:00 AM – 9:30 AM ET  (as UTC offsets)
# ET = UTC-5 (EST) or UTC-4 (EDT). Use UTC-4 (EDT) for summer, -5 for winter.
# We detect by checking if the target date falls in EDT range.
_EDT_START = (3, 8)   # 2nd Sunday of March  (month, week-of-month approx)
_EDT_END   = (11, 1)  # 1st Sunday of November


def _is_edt(dt: datetime) -> bool:
    """True if dt falls in US Eastern Daylight Time (UTC-4)."""
    m = dt.month
    return 3 < m < 11 or (m == 3 and dt.day >= 8) or (m == 11 and dt.day < 1)


def _et_offset(dt: datetime) -> int:
    """UTC offset for US/Eastern: -4 (EDT) or -5 (EST)."""
    return -4 if _is_edt(dt) else -5


def _premarket_utc(date_dt: datetime) -> tuple:
    """Return (start_utc, end_utc) for the pre-market window on date_dt."""
    offset = _et_offset(date_dt)
    # 4:00 AM ET → UTC
    start_et_h = 4
    end_et_h   = 9
    end_et_m   = 30
    start_utc = date_dt.replace(
        hour=start_et_h - offset, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
    )
    end_utc = date_dt.replace(
        hour=end_et_h - offset, minute=end_et_m, second=0, microsecond=0, tzinfo=timezone.utc
    )
    return start_utc, end_utc


# ── Quick levels (no API call) ────────────────────────────────────────────────

def quick_levels(price: float, prev_high: float, prev_low: float) -> dict:
    """
    Compute levels that require no additional Alpaca fetch.

    prev_high / prev_low should come from the Alpaca snapshot prevDailyBar
    (live path) or the previous bar in the daily bar series (historical path).
    """
    below = math.floor(price)
    above = math.ceil(price)
    # Avoid returning the price itself as a whole-dollar level
    if below == price:
        below -= 1
    if above == price:
        above += 1

    return {
        "prev_high": round(prev_high, 4),
        "prev_low":  round(prev_low, 4),
        "whole_dollar_above": float(above),
        "whole_dollar_below": float(below),
    }


# ── Full levels (intraday Alpaca fetch) ───────────────────────────────────────

def get_levels(ticker: str, date_str: Optional[str] = None, live: bool = False) -> dict:
    """
    Returns all context levels for ticker on the given date (or today if live).

    Keys:
        prev_high, prev_low           from prevDailyBar
        premarket_high, premarket_low from 4am-9:30am 1-min bars
        vwap                          from 4am to latest available bar
        whole_dollar_above/below      computed from latest price
        price                         latest available price

    Returns empty dict on any data failure.
    """
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

    # Resolve target date
    if live or not date_str:
        target = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        try:
            target = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            logger.error("Invalid date: %s", date_str)
            return {}

    client = StockHistoricalDataClient(api_key=api_key, secret_key=secret)

    # ── Fetch intraday bars (4am–end of session, extended hours) ─────────────
    premarket_start, session_open = _premarket_utc(target)
    fetch_end = target + timedelta(days=1)

    try:
        req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame(1, TimeFrameUnit.Minute),
            start=premarket_start,
            end=fetch_end,
            feed="iex",
        )
        bar_data = client.get_stock_bars(req).data
        bars = bar_data.get(ticker, [])
    except Exception as e:
        logger.warning("Intraday bars failed for %s: %s", ticker, e)
        bars = []

    # ── Fetch previous day's daily bar ───────────────────────────────────────
    prev_high = prev_low = 0.0
    try:
        daily_req = StockBarsRequest(
            symbol_or_symbols=ticker,
            timeframe=TimeFrame(1, TimeFrameUnit.Day),
            start=target - timedelta(days=7),
            end=target,
            feed="iex",
        )
        daily_data = client.get_stock_bars(daily_req).data
        daily_bars = sorted(daily_data.get(ticker, []), key=lambda b: b.timestamp)
        if daily_bars:
            prev = daily_bars[-1]
            prev_high = float(prev.high or 0)
            prev_low  = float(prev.low  or 0)
    except Exception as e:
        logger.warning("Daily bars failed for %s: %s", ticker, e)

    if not bars:
        return {}

    # Filter to target date only
    bars = [b for b in bars if b.timestamp.date() == target.date()]
    if not bars:
        return {}

    bars_sorted = sorted(bars, key=lambda b: b.timestamp)

    # ── VWAP (cumulative from session start) ──────────────────────────────────
    cum_tpv = 0.0  # typical_price × volume
    cum_vol  = 0.0
    for b in bars_sorted:
        tp  = (float(b.high) + float(b.low) + float(b.close)) / 3
        vol = float(b.volume or 0)
        cum_tpv += tp * vol
        cum_vol  += vol
    vwap = (cum_tpv / cum_vol) if cum_vol > 0 else 0.0

    # ── Pre-market H/L (4am – 9:30am ET) ─────────────────────────────────────
    pm_bars = [b for b in bars_sorted if b.timestamp < session_open]
    if pm_bars:
        premarket_high = max(float(b.high) for b in pm_bars)
        premarket_low  = min(float(b.low)  for b in pm_bars)
    else:
        premarket_high = premarket_low = 0.0

    # ── Latest price ──────────────────────────────────────────────────────────
    latest_price = float(bars_sorted[-1].close or 0)

    quick = quick_levels(latest_price, prev_high, prev_low) if prev_high else {
        "prev_high": 0.0,
        "prev_low":  0.0,
        "whole_dollar_above": float(math.ceil(latest_price)),
        "whole_dollar_below": float(math.floor(latest_price)),
    }

    return {
        **quick,
        "vwap":            round(vwap, 4),
        "premarket_high":  round(premarket_high, 4),
        "premarket_low":   round(premarket_low,  4),
        "price":           round(latest_price,   4),
    }
