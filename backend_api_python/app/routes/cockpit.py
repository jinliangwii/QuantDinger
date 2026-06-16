"""
Cockpit API routes — Seneca pre-market watchlist surface.

GET /api/cockpit/watchlist                                        -> live ranked candidates
GET /api/cockpit/watchlist/history?date=YYYY-MM-DD               -> historical candidates
GET /api/cockpit/movers?top=20                                    -> top gainers/losers/most-actives
GET /api/cockpit/levels/<ticker>?date=YYYY-MM-DD                 -> quick S/R levels (legacy)
GET /api/cockpit/context/<ticker>?date=&timeframe=1m|5m|1d       -> candles + levels + bias
"""

from datetime import date, datetime, timedelta, timezone

from flask import jsonify, request
from app.openapi.blueprint import HumanBlueprint as Blueprint
from app.utils.logger import get_logger


def _utc_now_iso() -> str:
    """Current UTC time as ISO 8601 string — consumed by frontend to show ET timestamp."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _last_trading_day() -> str:
    """Return today's date if weekday, else roll back to the most recent Friday."""
    d = date.today()
    if d.weekday() == 5:   # Saturday
        d -= timedelta(days=1)
    elif d.weekday() == 6:  # Sunday
        d -= timedelta(days=2)
    return d.isoformat()


def _market_status() -> str:
    return "closed" if date.today().weekday() >= 5 else "live"

logger = get_logger(__name__)

cockpit_blp = Blueprint("cockpit", __name__)


@cockpit_blp.route("/watchlist", methods=["GET"])
def get_watchlist():
    """Live pre-market watchlist — ranked candidates from Alpaca screener."""
    try:
        from app.services.seneca.scanner import screen
        candidates = screen(live=True)
        return jsonify({
            "success": True,
            "data": {
                "live": True,
                "market_status": _market_status(),
                "data_date": _last_trading_day(),
                "candidates": [c.to_dict() for c in candidates],
                "count": len(candidates),
                "fetched_at": _utc_now_iso(),
            },
        })
    except Exception as e:
        logger.error("Cockpit watchlist error: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@cockpit_blp.route("/watchlist/history", methods=["GET"])
def get_watchlist_history():
    """Historical candidate reconstruction for a given date."""
    date_str = (request.args.get("date") or "").strip()
    if not date_str:
        return jsonify({
            "success": False,
            "error": "date parameter required (YYYY-MM-DD)",
        }), 400
    try:
        from app.services.seneca.scanner import screen
        candidates = screen(date=date_str, live=False)
        return jsonify({
            "success": True,
            "data": {
                "live": False,
                "date": date_str,
                "candidates": [c.to_dict() for c in candidates],
                "count": len(candidates),
                "fetched_at": _utc_now_iso(),
            },
        })
    except Exception as e:
        logger.error("Cockpit history error: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@cockpit_blp.route("/movers", methods=["GET"])
def get_movers():
    """Live top gainers, losers, and most-actives from Alpaca screener."""
    try:
        top = int(request.args.get("top", 20))
    except ValueError:
        top = 20
    try:
        from app.services.seneca.movers import get_movers as _get_movers
        data = _get_movers(top=top)
        return jsonify({"success": True, "data": data})
    except Exception as e:
        logger.error("Cockpit movers error: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@cockpit_blp.route("/levels/<ticker>", methods=["GET"])
def get_levels(ticker: str):
    """
    Full S/R levels for a ticker — VWAP, pre-market H/L, prev-day H/L, whole-dollar.

    Query params:
        date  (optional) YYYY-MM-DD — omit for live/today
    """
    ticker = ticker.upper().strip()
    if not ticker:
        return jsonify({"success": False, "error": "ticker required"}), 400

    date_str = (request.args.get("date") or "").strip() or None
    live = date_str is None

    try:
        from app.services.seneca.levels import get_levels as _get_levels
        levels = _get_levels(ticker, date_str=date_str, live=live)
        if not levels:
            return jsonify({
                "success": False,
                "error": f"No data available for {ticker}",
            }), 404
        return jsonify({
            "success": True,
            "data": {
                "ticker": ticker,
                "date": date_str or _last_trading_day(),
                "live": live,
                **levels,
            },
        })
    except Exception as e:
        logger.error("Cockpit levels error for %s: %s", ticker, e)
        return jsonify({"success": False, "error": str(e)}), 500


@cockpit_blp.route("/context/<ticker>", methods=["GET"])
def get_context(ticker: str):
    """
    Full chart context: 1-min candles (4am ET) + structured S/R levels + bias.

    Query params:
        date  (optional) YYYY-MM-DD — omit for live/today
    """
    ticker = ticker.upper().strip()
    if not ticker:
        return jsonify({"success": False, "error": "ticker required"}), 400

    date_str  = (request.args.get("date")      or "").strip() or None
    timeframe = (request.args.get("timeframe") or "1m").strip()
    if timeframe not in ("1m", "5m", "1d"):
        timeframe = "1m"
    live = date_str is None

    try:
        from app.services.seneca.context import get_context as _get_context
        ctx = _get_context(ticker, date_str=date_str, live=live, timeframe=timeframe)
        if not ctx:
            return jsonify({
                "success": False,
                "error": f"No context data available for {ticker}",
            }), 404
        ctx["fetched_at"] = _utc_now_iso()
        return jsonify({"success": True, "data": ctx})
    except Exception as e:
        logger.error("Cockpit context error for %s: %s", ticker, e)
        return jsonify({"success": False, "error": str(e)}), 500
