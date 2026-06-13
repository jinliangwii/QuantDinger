"""
Seneca movers service — live top gainers, losers, most actives from Alpaca screener.
"""
import json
import logging
import os
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

_ALPACA_DATA = "https://data.alpaca.markets"
_TOP_MOVERS_URL   = f"{_ALPACA_DATA}/v1beta1/screener/stocks/movers"
_MOST_ACTIVES_URL = f"{_ALPACA_DATA}/v1beta1/screener/stocks/most-actives"


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


def _fmt(m: dict, rank: int) -> dict:
    return {
        "rank":       rank,
        "ticker":     m.get("symbol", ""),
        "price":      round(float(m.get("price") or 0), 2),
        "change_pct": round(float(m.get("percent_change") or 0), 2),
        "volume":     int(m.get("volume") or 0),
    }


def get_movers(top: int = 20) -> dict:
    """
    Returns {top_gainers, top_losers, most_actives}.
    Each row: {rank, ticker, price, change_pct, volume}.
    """
    gainers, losers, actives = [], [], []

    try:
        data    = _get(_TOP_MOVERS_URL, {"top": top})
        gainers = [_fmt(m, i + 1) for i, m in enumerate(data.get("gainers", []))]
        losers  = [_fmt(m, i + 1) for i, m in enumerate(data.get("losers",  []))]
    except Exception as e:
        logger.warning("movers fetch failed: %s", e)

    try:
        data    = _get(_MOST_ACTIVES_URL, {"top": top, "by": "volume"})
        actives = [_fmt(m, i + 1) for i, m in enumerate(data.get("most_actives", []))]
    except Exception as e:
        logger.warning("most-actives fetch failed: %s", e)

    return {"top_gainers": gainers, "top_losers": losers, "most_actives": actives}
