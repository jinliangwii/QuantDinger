"""
Cockpit API routes — Seneca pre-market watchlist surface.

GET /api/cockpit/watchlist                      -> live ranked candidates
GET /api/cockpit/watchlist/history?date=YYYY-MM-DD  -> historical candidates
"""

from datetime import date, timedelta

from flask import jsonify, request
from app.openapi.blueprint import HumanBlueprint as Blueprint
from app.utils.logger import get_logger


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
            },
        })
    except Exception as e:
        logger.error("Cockpit history error: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500
