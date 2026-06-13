"""
Pattern Recognition — Layer 3 of the five-layer Seneca architecture.

Detects chart patterns (bull flags, breakouts, etc.) from candle data.
Pure functions — no API calls, no I/O. Same code for backtest and live.

Bull flag detection thresholds are proxy values derived from YC's conceptual
criteria. They are NOT values YC explicitly stated; they are the Mneme
backtesting proposal's operationalisation of his visual pattern recognition.
"""

import logging

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────

BULL_FLAG = {
    "pole_lookback": 20,          # candles to scan for pole formation
    "pole_min_pct": 3.0,          # minimum pole move (%)
    "min_flag_bars": 3,           # minimum consolidation candles after pole
    "max_vol_contraction": 0.70,  # flag avg vol / pole avg vol ≤ this
    "breakout_vol_mult": 1.5,     # breakout vol > flag_avg_vol * this
    "stop_buffer": 0.99,          # stop = flag_low * this
    "time_stop_bars": 30,         # max hold (bars) before time-stop
    "scan_window": 60,            # how many recent candles to examine
    "max_patterns": 1,            # only return the best one
}


# ── Public API ─────────────────────────────────────────────────────────────────

def detect_patterns(candles, timeframe="1m", live=False):
    """
    Entry point. Returns a list of detected pattern dicts.

    Args:
        candles: list of dicts with keys {timestamp, open, high, low, close,
                 volume, vwap}
        timeframe: "1m" | "5m" | "1d"
        live: True for live/today data (affects status reporting)

    Returns:
        list of pattern dicts (empty list if none found)
    """
    if timeframe == "1d":
        return []  # bull flags require intraday granularity

    if not candles or len(candles) < BULL_FLAG["pole_lookback"] + BULL_FLAG["min_flag_bars"]:
        return []

    try:
        patterns = _detect_bull_flags(candles, live)
        return patterns
    except Exception:
        logger.exception("pattern detection failed")
        return []


# ── Bull flag detection ────────────────────────────────────────────────────────

def _detect_bull_flags(candles, live):
    """
    Scan for bull flag patterns in the most recent candles.

    Algorithm (pole → flag → breakout):
      1. Find a sharp upward move (pole) within a sliding 20-candle window
      2. Confirm the pole had above-average volume
      3. Check subsequent candles for a consolidating flag (tight range,
         declining volume, holding above pole low)
      4. Check if the most recent candle confirms a breakout
    """
    scan = candles[-BULL_FLAG["scan_window"]:] if len(candles) > BULL_FLAG["scan_window"] else candles
    n = len(scan)

    window_avg_vol = sum(c["volume"] for c in scan) / n if n > 0 else 0
    if window_avg_vol <= 0:
        return []

    best = None  # track the highest-confidence pattern

    # Slide a window across the scan range looking for pole formations
    for pole_end_idx in range(BULL_FLAG["pole_lookback"] - 1, n - BULL_FLAG["min_flag_bars"] - 1):
        pole_start_idx = max(0, pole_end_idx - BULL_FLAG["pole_lookback"] + 1)
        pole_candles = scan[pole_start_idx:pole_end_idx + 1]

        if len(pole_candles) < 3:
            continue

        pole_low  = min(c["low"]  for c in pole_candles)
        pole_high = max(c["high"] for c in pole_candles)
        pole_low_idx  = _argmin(c["low"]  for c in pole_candles)
        pole_high_idx = _argmax(c["high"] for c in pole_candles)

        if pole_low <= 0:
            continue

        pole_move_pct = ((pole_high - pole_low) / pole_low) * 100
        if pole_move_pct < BULL_FLAG["pole_min_pct"]:
            continue

        # Pole must have above-average volume
        pole_avg_vol = sum(c["volume"] for c in pole_candles) / len(pole_candles)
        if pole_avg_vol <= window_avg_vol:
            continue

        # ── Flag consolidation ──
        # Split post-pole candles into flag body + breakout candidate.
        # The breakout candle is the last candle; flag stats must come from
        # the consolidation candles BEFORE it, so flag_high isn't inflated by
        # the breakout bar itself.
        post_pole = scan[pole_end_idx + 1:]

        if len(post_pole) < BULL_FLAG["min_flag_bars"] + 1:
            # Not enough bars for flag + breakout check
            continue

        flag_candles = post_pole[:-1]       # consolidation body
        breakout_candle = post_pole[-1]     # potential breakout bar

        if len(flag_candles) < BULL_FLAG["min_flag_bars"]:
            continue

        flag_low  = min(c["low"]  for c in flag_candles)
        flag_high = max(c["high"] for c in flag_candles)

        # Flag must hold above pole low (trend still intact)
        if flag_low <= pole_low:
            continue

        # Volume contraction: flag volume must be notably lower than pole volume
        flag_avg_vol = sum(c["volume"] for c in flag_candles) / len(flag_candles)
        if flag_avg_vol <= 0:
            continue

        vol_contraction = flag_avg_vol / pole_avg_vol
        if vol_contraction > BULL_FLAG["max_vol_contraction"]:
            continue

        # ── Breakout check ──
        breakout_confirmed = (
            breakout_candle["close"] > flag_high
            and breakout_candle["volume"] > BULL_FLAG["breakout_vol_mult"] * flag_avg_vol
        )

        # ── Entry / Stop / Target ──
        entry_price  = round(flag_high, 4)
        stop_price   = round(flag_low * BULL_FLAG["stop_buffer"], 4)
        pole_height  = pole_high - pole_low
        target_price = round(entry_price + pole_height, 4)

        risk   = round(entry_price - stop_price, 4)
        reward = round(target_price - entry_price, 4)
        rr_ratio = round(reward / risk, 2) if risk > 0 else 0

        # ── Status ──
        if breakout_confirmed:
            status = "confirmed"
        else:
            status = "forming"

        # ── Confidence ──
        confidence = _calc_confidence(
            pole_move_pct, vol_contraction, breakout_confirmed, len(flag_candles)
        )

        pattern = {
            "type": "bull_flag",
            "status": status,
            "pole": {
                "start_timestamp_ms": pole_candles[pole_low_idx]["timestamp"],
                "end_timestamp_ms":   pole_candles[pole_high_idx]["timestamp"],
                "high":      round(pole_high, 4),
                "low":       round(pole_low, 4),
                "height":    round(pole_height, 4),
                "move_pct":  round(pole_move_pct, 2),
                "avg_volume": int(pole_avg_vol),
            },
            "flag": {
                "start_timestamp_ms": flag_candles[0]["timestamp"],
                "end_timestamp_ms":   flag_candles[-1]["timestamp"],
                "high":      round(flag_high, 4),
                "low":       round(flag_low, 4),
                "height":    round(flag_high - flag_low, 4),
                "avg_volume": int(flag_avg_vol),
                "bar_count": len(flag_candles),
            },
            "breakout": {
                "timestamp_ms": breakout_candle["timestamp"],
                "price":   round(breakout_candle["close"], 4),
                "volume":  int(breakout_candle["volume"]),
                "confirmed": breakout_confirmed,
            },
            "entry": {
                "price":     entry_price,
                "rationale": "Break above flag high",
                "side":      "long",
            },
            "stop": {
                "price":     stop_price,
                "rationale": "Below flag low (99% buffer)",
            },
            "target": {
                "price":     target_price,
                "rationale": "Measured move: entry + pole height",
            },
            "time_stop_bars": BULL_FLAG["time_stop_bars"],
            "risk_reward": {
                "risk":   risk,
                "reward": reward,
                "ratio":  rr_ratio,
            },
            "confidence": confidence,
            "reasons": _build_reasons(
                pole_move_pct, vol_contraction, breakout_confirmed, confidence
            ),
        }

        # Keep the highest-confidence pattern
        if best is None or confidence > best["confidence"]:
            best = pattern

    if best:
        return [best]
    return []


# ── Helpers ────────────────────────────────────────────────────────────────────

def _calc_confidence(pole_move_pct, vol_contraction, breakout_confirmed, flag_bars):
    """
    Weighted 0–100 confidence score.

    Weights favour strong poles and confirmed breakouts; forming flags with
    shallow poles get penalised.
    """
    # Pole strength (25%): 3% → 40pts, 10%+ → 100pts
    pole_score = min(100, max(0, (pole_move_pct - 1.0) / 9.0 * 100))

    # Volume contraction (25%): more contraction → higher score
    vol_score = max(0, min(100, (1.0 - vol_contraction) / 0.7 * 100))

    # Breakout confirmation (25%): 100 if confirmed, 30 if still forming
    breakout_score = 100 if breakout_confirmed else 30

    # Flag bar count (15%): 3 bars → 40pts, 10+ bars → 100pts
    length_score = min(100, max(0, (flag_bars / 10.0) * 100))

    # Flag tightness proxy via bar count (10%): longer consolidation that
    # stays within a tight range suggests a healthy flag
    tightness = min(100, max(0, (flag_bars / 7.0) * 100))

    return round(
          pole_score     * 0.25
        + vol_score      * 0.25
        + breakout_score * 0.25
        + length_score   * 0.15
        + tightness      * 0.10
    )


def _build_reasons(pole_move_pct, vol_contraction, breakout_confirmed, confidence):
    """Human-readable reasons for the pattern detection."""
    reasons = []

    if pole_move_pct >= 5:
        reasons.append(f"Strong pole: +{pole_move_pct:.1f}%")
    else:
        reasons.append(f"Pole: +{pole_move_pct:.1f}%")

    contraction_pct = round((1 - vol_contraction) * 100)
    if contraction_pct >= 50:
        reasons.append(f"Sharp volume contraction: -{contraction_pct}%")
    else:
        reasons.append(f"Volume contracting: -{contraction_pct}%")

    if breakout_confirmed:
        reasons.append("Breakout confirmed — close above flag + volume surge")
    else:
        reasons.append("Flag forming — awaiting breakout")

    if confidence >= 70:
        reasons.append(f"High confidence: {confidence}/100")
    elif confidence >= 40:
        reasons.append(f"Moderate confidence: {confidence}/100")
    else:
        reasons.append(f"Low confidence: {confidence}/100")

    return reasons


def _argmin(iterable):
    """Return the index of the minimum value."""
    best_i, best_v = 0, None
    for i, v in enumerate(iterable):
        if best_v is None or v < best_v:
            best_i, best_v = i, v
    return best_i


def _argmax(iterable):
    """Return the index of the maximum value."""
    best_i, best_v = 0, None
    for i, v in enumerate(iterable):
        if best_v is None or v > best_v:
            best_i, best_v = i, v
    return best_i
