"""
Polygon.io-backed US stock data source.

For all timeframes Polygon's REST API provides real-time and historical bars
with broader coverage than Alpaca's free IEX feed (pre-market data from 4am ET).
Daily/weekly bars also served by Polygon for single-source consistency.

Bars are cached as Parquet per (symbol, timeframe) under POLYGON_CACHE_DIR to
minimize API calls on repeated backtests. Cache is append-only: only missing
tail is re-fetched on each access.

Requires env var: POLYGON_API_KEY
Docs: https://polygon.io/docs/stocks/getting-started
"""

import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.data_sources.us_stock import USStockDataSource
from app.utils.logger import get_logger

logger = get_logger(__name__)

_POLYGON_BASE = "https://api.polygon.io"

# Map QuantDinger timeframe → (multiplier, timespan)
_TF_MAP = {
    "1m":  (1, "minute"),
    "5m":  (5, "minute"),
    "15m": (15, "minute"),
    "30m": (30, "minute"),
    "1H":  (1, "hour"),
    "4H":  (4, "hour"),
    "1D":  (1, "day"),
    "1W":  (1, "week"),
}

# Approximate trading bars per calendar day per timeframe (for range estimation)
_BARS_PER_DAY = {"1m": 390, "5m": 78, "15m": 26, "30m": 13, "1H": 7, "4H": 2, "1D": 1, "1W": 1 / 5}

CACHE_DIR = Path(os.getenv("POLYGON_CACHE_DIR", "/app/data/polygon_cache"))


class PolygonUSStockDataSource(USStockDataSource):
    """US stock data source backed by Polygon.io."""

    name = "USStock/Polygon"

    def __init__(self):
        super().__init__()
        self._api_key = os.environ["POLYGON_API_KEY"]
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get_kline(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        before_time: Optional[int] = None,
        after_time: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        tf = self._normalize_timeframe(timeframe)
        if tf not in _TF_MAP:
            logger.warning(f"Polygon: unsupported timeframe {tf} — falling back to yfinance")
            return super().get_kline(symbol, timeframe, limit, before_time, after_time)

        try:
            bars = self._get_bars(symbol, tf, limit, before_time, after_time)
            if bars:
                bars.sort(key=lambda x: x["time"])
                return self.filter_and_limit(
                    bars, limit, before_time, after_time, truncate=(after_time is None)
                )
            logger.warning(f"Polygon returned no bars for {symbol} {tf} — falling back")
        except Exception as exc:
            logger.warning(f"Polygon error for {symbol} {tf}: {exc} — falling back to yfinance")

        return super().get_kline(symbol, timeframe, limit, before_time, after_time)

    # ------------------------------------------------------------------
    # Snapshot — used by Seneca scanner
    # ------------------------------------------------------------------

    def get_snapshots(self, tickers: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch current snapshots for a list of tickers.

        Uses Polygon's snapshot endpoint (v2/snapshot/locale/us/markets/stocks/tickers).
        Returns a list of dicts with keys matching what the scanner expects:
          ticker, price, change_pct, today_vol, prev_close, prev_high, prev_low,
          today_open, today_high, today_low, updated_ns.

        Docs: https://polygon.io/docs/stocks/get_v2_snapshot_locale_us_markets_stocks_tickers
        """
        results: List[Dict[str, Any]] = []
        # Polygon accepts comma-separated tickers
        tickers_str = ",".join(t.upper() for t in tickers)
        url = f"{_POLYGON_BASE}/v2/snapshot/locale/us/markets/stocks/tickers?tickers={urllib.parse.quote(tickers_str)}&apiKey={self._api_key}"

        try:
            data = self._get_json(url)
            for t in data.get("tickers", []):
                results.append(self._parse_snapshot(t))
        except Exception as exc:
            logger.warning(f"Polygon snapshot batch failed: {exc}")

        return results

    def get_all_snapshots(self) -> List[Dict[str, Any]]:
        """
        Fetch snapshots for ALL US stocks (no ticker filter).

        Returns same shape as get_snapshots(). Used by scanner for full-market screen.
        """
        url = f"{_POLYGON_BASE}/v2/snapshot/locale/us/markets/stocks/tickers?apiKey={self._api_key}"
        results: List[Dict[str, Any]] = []
        try:
            data = self._get_json(url)
            for t in data.get("tickers", []):
                results.append(self._parse_snapshot(t))
        except Exception as exc:
            logger.warning(f"Polygon all-snapshots failed: {exc}")
        return results

    def get_grouped_daily(self, target_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch grouped daily bars for ALL US stocks on a given date.

        This is the Polygon equivalent of a full-market historical scan —
        one API call returns open/high/low/close/volume for every stock that
        traded on that day. Used by the historical scanner path.

        Docs: https://polygon.io/docs/stocks/get_v2_aggs_grouped_locale_us_market_stocks__date
        """
        if target_date is None:
            target_date = date.today().isoformat()
        url = (
            f"{_POLYGON_BASE}/v2/aggs/grouped/locale/us/market/stocks/{target_date}"
            f"?adjusted=true&apiKey={self._api_key}"
        )
        results: List[Dict[str, Any]] = []
        try:
            data = self._get_json(url)
            for r in data.get("results", []):
                results.append({
                    "ticker": r.get("T", ""),
                    "open":   float(r.get("o", 0)),
                    "high":   float(r.get("h", 0)),
                    "low":    float(r.get("l", 0)),
                    "close":  float(r.get("c", 0)),
                    "volume": float(r.get("v", 0)),
                    "vwap":   float(r.get("vw", 0)) if r.get("vw") else None,
                    "n":      int(r.get("n", 0)),
                })
        except Exception as exc:
            logger.warning(f"Polygon grouped daily {target_date} failed: {exc}")
        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_bars(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        before_time: Optional[int],
        after_time: Optional[int],
    ) -> List[Dict[str, Any]]:
        import pandas as pd

        mult, span = _TF_MAP[timeframe]
        cache_file = self._cache_path(symbol, timeframe)

        # Determine the window we need
        end_dt = (
            datetime.fromtimestamp(before_time, tz=timezone.utc)
            if before_time
            else datetime.now(tz=timezone.utc)
        )
        bars_per_day = _BARS_PER_DAY.get(timeframe, 1)
        days_needed = max(10, int(limit / max(bars_per_day, 0.01) * 7 / 5) + 14)
        start_dt = end_dt - timedelta(days=days_needed)
        if after_time:
            floor = datetime.fromtimestamp(after_time, tz=timezone.utc)
            if floor < start_dt:
                start_dt = floor

        # Load existing cache
        cached_df: Optional[object] = None
        if cache_file.exists():
            try:
                cached_df = pd.read_parquet(cache_file)
            except Exception as exc:
                logger.warning(f"Cache read failed ({cache_file}): {exc} — re-fetching")
                cached_df = None

        # Decide what date range to fetch from Polygon
        fetch_start = start_dt
        if cached_df is not None and not cached_df.empty:
            latest_cached = cached_df.index.max()
            if hasattr(latest_cached, "tzinfo") and latest_cached.tzinfo is None:
                latest_cached = latest_cached.tz_localize("UTC")
            elif hasattr(latest_cached, "tzinfo") and latest_cached.tzinfo is not None:
                latest_cached = latest_cached.tz_convert("UTC")
            if latest_cached.date() >= start_dt.date():
                fetch_start = latest_cached + timedelta(minutes=1)

        # Fetch missing tail from Polygon
        if fetch_start < end_dt:
            new_df = self._polygon_fetch(symbol, mult, span, fetch_start, end_dt)
            if new_df is not None and not new_df.empty:
                if cached_df is not None and not cached_df.empty:
                    cached_df = pd.concat([cached_df, new_df])
                    cached_df = cached_df[~cached_df.index.duplicated(keep="last")]
                    cached_df.sort_index(inplace=True)
                else:
                    cached_df = new_df
                # Persist updated cache
                try:
                    cache_file.parent.mkdir(parents=True, exist_ok=True)
                    cached_df.to_parquet(cache_file)
                except Exception as exc:
                    logger.warning(f"Cache write failed ({cache_file}): {exc}")

        if cached_df is None or cached_df.empty:
            return []

        # Slice to the requested window
        start_ts = start_dt.timestamp()
        end_ts = end_dt.timestamp()
        bars: List[Dict[str, Any]] = []
        for ts, row in cached_df.iterrows():
            ts_unix = int(ts.timestamp()) if hasattr(ts, "timestamp") else int(ts)
            if ts_unix < start_ts or ts_unix > end_ts:
                continue
            bars.append(
                self.format_kline(
                    timestamp=ts_unix,
                    open_price=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row.get("volume", 0)),
                )
            )
        return bars

    def _polygon_fetch(
        self,
        symbol: str,
        multiplier: int,
        timespan: str,
        start: datetime,
        end: datetime,
    ):
        """
        Fetch bars from Polygon aggregates endpoint. Returns tz-aware UTC-indexed DataFrame or None.

        Docs: https://polygon.io/docs/stocks/get_v2_aggs_ticker__stocksticker__range__multiplier___timespan___from___to
        """
        import pandas as pd

        from_ts = start.strftime("%Y-%m-%d")
        to_ts = end.strftime("%Y-%m-%d")

        # Push start back to 4am ET (9am UTC) to catch pre-market bars
        fetch_start = start.replace(hour=9, minute=0, second=0, microsecond=0)
        if fetch_start > start:
            fetch_start = start
        from_str = fetch_start.strftime("%Y-%m-%d")

        url = (
            f"{_POLYGON_BASE}/v2/aggs/ticker/{symbol.upper()}/range/{multiplier}/{timespan}"
            f"/{from_str}/{to_ts}?adjusted=true&sort=asc&limit=50000&apiKey={self._api_key}"
        )

        try:
            data = self._get_json(url)
            results = data.get("results", [])
            if not results:
                return None

            rows = []
            for r in results:
                # Polygon returns timestamp in Unix milliseconds
                ts_ms = r.get("t", 0)
                ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                rows.append({
                    "timestamp": ts,
                    "open":   float(r.get("o", 0)),
                    "high":   float(r.get("h", 0)),
                    "low":    float(r.get("l", 0)),
                    "close":  float(r.get("c", 0)),
                    "volume": float(r.get("v", 0)),
                })

            df = pd.DataFrame(rows)
            df.set_index("timestamp", inplace=True)
            df.index = pd.to_datetime(df.index, utc=True)
            df.sort_index(inplace=True)
            return df[["open", "high", "low", "close", "volume"]]

        except Exception as exc:
            logger.warning(f"Polygon API call failed {symbol} {multiplier}/{timespan}: {exc}")
            return None

    def _get_json(self, url: str, timeout: int = 20) -> dict:
        """Simple HTTP GET returning parsed JSON."""
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
        return json.loads(body) if body else {}

    def _parse_snapshot(self, ticker_data: dict) -> Dict[str, Any]:
        """
        Parse one ticker from Polygon's snapshot response into the scanner-friendly shape.
        """
        session = ticker_data.get("session", {}) or {}
        prev = ticker_data.get("prevDay", {}) or {}
        day = ticker_data.get("day", {}) or {}

        prev_close = float(prev.get("c", 0) or 0)
        price = float(day.get("c", 0) or session.get("p", 0) or 0)
        change_pct = round((price - prev_close) / prev_close, 4) if prev_close else 0.0

        return {
            "ticker":       ticker_data.get("ticker", ""),
            "price":        price,
            "change_pct":   change_pct,
            "today_vol":    int(day.get("v", 0) or session.get("v", 0) or 0),
            "prev_close":   prev_close,
            "prev_high":    float(prev.get("h", 0) or 0),
            "prev_low":     float(prev.get("l", 0) or 0),
            "today_open":   float(day.get("o", 0) or 0),
            "today_high":   float(day.get("h", 0) or 0),
            "today_low":    float(day.get("l", 0) or 0),
            "updated_ns":   ticker_data.get("updated", 0),
        }

    def _cache_path(self, symbol: str, timeframe: str) -> Path:
        return CACHE_DIR / symbol.upper() / f"{timeframe}.parquet"
