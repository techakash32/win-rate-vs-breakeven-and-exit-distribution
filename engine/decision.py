# engine/decision.py
"""
The gate decision for ONE closed bar, as a pure function of the bar history.
Used by the live Pipeline AND the backtest, so both run identical logic
(volume profile window, Bucket A/B, optional C veto, approximated-VP guard,
cooldown).
"""
from dataclasses import dataclass
from datetime import datetime, timezone
import logging

from config import settings
from features.volume_profile import compute_volume_profile
from gate.buckets import BucketSignal, Dir
from gate.confirmation_gate import (
    evaluate_bucket_b, confirmation_gate,
)

log = logging.getLogger(__name__)


_SWEEP_DIAG = {"total_bars": 0, "sweep_signals": 0, "filtered_short_long_only": 0}
_FUNDING_DIAG = {"bars_with_snapshot": 0, "extreme_signals": 0, "missing_snapshot": 0}
_DECIDE_DIAG = {"called": 0, "atr_reject": 0, "vp_none": 0, "other_reject": 0}


def reset_sweep_diagnostics():
    for key in _SWEEP_DIAG:
        _SWEEP_DIAG[key] = 0
    for key in _FUNDING_DIAG:
        _FUNDING_DIAG[key] = 0
    for key in _DECIDE_DIAG:
        _DECIDE_DIAG[key] = 0


def sweep_diagnostics_snapshot():
    return dict(_SWEEP_DIAG)


# Backwards-compatible helper names from the absorption iteration.
reset_absorption_diagnostics = reset_sweep_diagnostics
absorption_diagnostics_snapshot = sweep_diagnostics_snapshot


def decision_diagnostics_snapshot():
    return dict(_DECIDE_DIAG)


def funding_diagnostics_snapshot():
    return dict(_FUNDING_DIAG)


def _evaluate_funding_fade(history: list):
    empty = {"delta_ratio": None, "compression_ratio": None, "distance_to_level_pct": None}
    if not history:
        return BucketSignal("A", Dir.NEUTRAL, 0.0, "no bars"), empty
    funding = history[-1].get("funding_snapshot")
    if not funding:
        _FUNDING_DIAG["missing_snapshot"] += 1
        return BucketSignal("A", Dir.NEUTRAL, 0.0, "no funding data"), empty
    _FUNDING_DIAG["bars_with_snapshot"] += 1
    pct = float(funding["pct"])
    prior = float(funding["prior_pct"])
    rate = float(funding["rate"])
    previous_funding = history[-2].get("funding_snapshot") if len(history) > 1 else None
    is_new_event = (previous_funding is None or
                    previous_funding.get("time_ms") != funding.get("time_ms"))
    if (is_new_event and pct > settings.FUNDING_EXTREME_HIGH_PCT
            and prior <= settings.FUNDING_EXTREME_HIGH_PCT):
        _FUNDING_DIAG["extreme_signals"] += 1
        return BucketSignal("A", Dir.SHORT, 0.8,
                            f"funding {rate:.4%} in {pct:.0f}th pct (crowded long)"), empty
    if (is_new_event and pct < settings.FUNDING_EXTREME_LOW_PCT
            and prior >= settings.FUNDING_EXTREME_LOW_PCT):
        _FUNDING_DIAG["extreme_signals"] += 1
        return BucketSignal("A", Dir.LONG, 0.8,
                            f"funding {rate:.4%} in {pct:.0f}th pct (crowded short)"), empty
    return BucketSignal("A", Dir.NEUTRAL, 0.0,
                        f"funding {rate:.4%} at {pct:.0f}th pct (not extreme)"), empty


def _evaluate_donchian_trend(history: list):
    empty = {"delta_ratio": None, "compression_ratio": None, "distance_to_level_pct": None}
    if getattr(settings, "TREND_LONDON_ONLY", False):
        if not history:
            return BucketSignal("A", Dir.NEUTRAL, 0.0, "no bars"), empty
        # A signal is known at the bar close and fills at the next bar open.
        # Use that entry timestamp for the session rule to avoid a one-bar shift.
        entry_ts_ms = int(history[-1].get("open_time", 0)) + settings.INTERVAL_MS
        if entry_ts_ms <= 0:
            return BucketSignal("A", Dir.NEUTRAL, 0.0, "missing entry timestamp"), empty
        hour_utc = datetime.fromtimestamp(entry_ts_ms / 1000, timezone.utc).hour
        if not (8 <= hour_utc < 16):
            return BucketSignal("A", Dir.NEUTRAL, 0.0,
                                f"outside London entry session ({hour_utc:02d}:xx UTC)"), empty
    n = settings.TREND_DONCHIAN_LOOKBACK
    atr_window = settings.TREND_ATR_WINDOW
    if len(history) < max(n + 1, atr_window):
        return BucketSignal("A", Dir.NEUTRAL, 0.0, "trend warmup"), empty
    if any(bar.get("synthetic") for bar in history[-(n + 1):]):
        return BucketSignal("A", Dir.NEUTRAL, 0.0, "synthetic bar in Donchian lookback"), empty
    previous = history[-(n + 1):-1]
    high_n = max(bar["high"] for bar in previous)
    low_n = min(bar["low"] for bar in previous)
    atr = sum(bar["high"] - bar["low"] for bar in history[-atr_window:]) / atr_window
    if atr <= 0:
        return BucketSignal("A", Dir.NEUTRAL, 0.0, "no ATR"), empty
    last = history[-1]
    if last["close"] > high_n:
        return BucketSignal("A", Dir.LONG, 0.8,
                            f"breakout above {n}-bar high {high_n:.0f}"), empty
    if last["close"] < low_n:
        return BucketSignal("A", Dir.SHORT, 0.8,
                            f"breakdown below {n}-bar low {low_n:.0f}"), empty
    return BucketSignal("A", Dir.NEUTRAL, 0.0, f"inside {n}-bar range"), empty


def evaluate_bucket_a(history: list, vp, close: float):
    """Detect a failed swing breakout and return the reversal direction."""
    strategy = getattr(settings, "ACTIVE_STRATEGY", "sweep")
    if strategy == "funding_fade":
        return _evaluate_funding_fade(history)
    if strategy == "trend":
        return _evaluate_donchian_trend(history)
    empty = {"delta_ratio": None, "compression_ratio": None, "distance_to_level_pct": None}
    lookback_bars = settings.SWEEP_LOOKBACK_BARS
    if len(history) < lookback_bars + 5:
        return BucketSignal("A", Dir.NEUTRAL, 0.0, "warmup"), empty

    if any(b.get("synthetic") for b in history[-(lookback_bars + 3):]):
        return BucketSignal("A", Dir.NEUTRAL, 0.0, "synthetic bar in sweep lookback"), empty

    min_pen = settings.SWEEP_MIN_PENETRATION_PCT
    if lookback_bars < 1 or min_pen < 0:
        raise ValueError("SWEEP_LOOKBACK_BARS must be positive and SWEEP_MIN_PENETRATION_PCT nonnegative")

    # Exclude the current sweep window and use bars ending three bars ago.
    lookback = history[-(lookback_bars + 3):-3]
    if len(lookback) < lookback_bars - 2:
        return BucketSignal("A", Dir.NEUTRAL, 0.0, "insufficient lookback"), empty
    swing_high = max(b["high"] for b in lookback)
    swing_low = min(b["low"] for b in lookback)
    last = history[-1]
    _SWEEP_DIAG["total_bars"] += 1

    # Bearish failed breakout: wick above swing high, then close back below.
    if last["high"] > swing_high * (1 + min_pen) and close < swing_high:
        _SWEEP_DIAG["sweep_signals"] += 1
        if getattr(settings, "SWEEP_LONG_ONLY", False):
            _SWEEP_DIAG["filtered_short_long_only"] += 1
            return BucketSignal("A", Dir.NEUTRAL, 0.0,
                                "short sweep filtered (long-only test)"), empty
        return BucketSignal("A", Dir.SHORT, 0.8,
                            f"bearish sweep of {swing_high:.0f} (wick {last['high']:.0f})"), empty

    # Bullish failed breakdown: wick below swing low, then close back above.
    if last["low"] < swing_low * (1 - min_pen) and close > swing_low:
        _SWEEP_DIAG["sweep_signals"] += 1
        return BucketSignal("A", Dir.LONG, 0.8,
                            f"bullish sweep of {swing_low:.0f} (wick {last['low']:.0f})"), empty

    return BucketSignal("A", Dir.NEUTRAL, 0.0, "no sweep"), empty


# Backwards-compatible function name for callers from the absorption iteration.
evaluate_absorption_a = evaluate_bucket_a


@dataclass
class Decision:
    warmup: bool = False
    warmup_have: int = 0
    vp: object = None
    vp_bars_n: int = 0
    approx: int = 0
    a: BucketSignal = None
    b: BucketSignal = None
    c_sig: BucketSignal = None
    allowed: bool = False
    direction: Dir | None = None
    reason: str = ""
    suppressed: bool = False
    a_strength: float | None = None
    b_strength: float | None = None
    delta_ratio: float | None = None
    compression_ratio: float | None = None
    distance_to_level_pct: float | None = None


NEUTRAL_C = BucketSignal("C", Dir.NEUTRAL, 0.0, "not wired")


class Decider:
    """Holds the only state the gate has: the post-signal cooldown counter."""

    def __init__(self):
        self.cooldown = 0

    def decide(self, hist: list, close: float, bucket_c=None) -> Decision:
        """
        hist: finalized bars, the just-closed bar LAST.
        bucket_c: (BucketSignal, veto_fn | None) or None.
        """
        _DECIDE_DIAG["called"] += 1
        if settings.ATR_FILTER_ENABLED:
            atr = _atr_pct(hist, settings.ATR_WINDOW_BARS)
            if atr is not None and atr < settings.ATR_MIN_PCT:
                _DECIDE_DIAG["atr_reject"] += 1
                return Decision(allowed=False, reason=f"low ATR ({atr:.2f}%)", warmup=False)

        vp_bars = hist[:-1] if settings.VP_EXCLUDE_CURRENT else hist
        vp_bars = vp_bars[-settings.VP_WINDOW_BARS:]
        if len(vp_bars) < settings.WARMUP_BARS:
            _DECIDE_DIAG["other_reject"] += 1
            return Decision(warmup=True, warmup_have=len(vp_bars))
        
        # vp = compute_volume_profile(vp_bars, settings.VP_VA_PERCENT)
        if settings.ACTIVE_STRATEGY == "trend":
            vp = None                              # Donchian doesn't use VP
        else:
            vp = compute_volume_profile(vp_bars, settings.VP_VA_PERCENT)
        if vp is None:
            _DECIDE_DIAG["vp_none"] += 1
        approx = sum(1 for b in vp_bars if b.get("approx_vp") or b.get("synthetic"))

        a, absorption = evaluate_bucket_a(hist, vp, close)
        b = evaluate_bucket_b(hist, window=settings.AGGR_WINDOW_BARS)
        c_sig, veto_fn = bucket_c if bucket_c else (NEUTRAL_C, None)

        allowed, direction, reason = confirmation_gate(
            a, b, c_sig, veto_fn=veto_fn, close=close,
            min_aggr_strength=settings.MIN_AGGRESSION_STRENGTH,
            strategy=getattr(settings, "ACTIVE_STRATEGY", "sweep"),
        )

        # Never trade on a volume profile that contains approximated bars.
        approx_frac = approx / len(vp_bars) if vp_bars else 0.0
        if allowed and approx_frac > settings.GATE_MAX_APPROX_VP_FRACTION + 1e-9:
            allowed = False
            reason = (f"STAND ASIDE - VP not real yet ({approx}/{len(vp_bars)} bars "
                      f"approximated; would be {direction.value})")
            direction = None

        suppressed = False
        if self.cooldown > 0:
            self.cooldown -= 1
            if allowed:
                suppressed = True
                allowed = False
                reason = (f"COOLDOWN ({self.cooldown} bars left) - "
                          f"would be {direction.value} but suppressed")
        if not allowed and a.direction != Dir.NEUTRAL:
            log.info("%s | A=%s B=%s C=%s -> %s",
                     f"[{a.bucket} {a.direction.value} {a.strength:.2f}]",
                     a.direction.value,
                     b.direction.value if not getattr(b, "disabled", False) else "OFF",
                     c_sig.direction.value,
                     reason)
        if allowed:
            self.cooldown = settings.COOLDOWN_BARS

        return Decision(
            vp=vp, vp_bars_n=len(vp_bars), approx=approx, a=a, b=b, c_sig=c_sig,
            allowed=allowed, direction=direction, reason=reason, suppressed=suppressed,
            a_strength=a.strength, b_strength=b.strength,
            delta_ratio=absorption["delta_ratio"],
            compression_ratio=absorption["compression_ratio"],
            distance_to_level_pct=absorption["distance_to_level_pct"],
        )

def _atr_pct(history: list, window: int = 14):
    """Mean high-low range as a percentage of close over recent bars."""
    if len(history) < window:
        return None
    ranges = [(b["high"] - b["low"]) / b["close"] * 100
              for b in history[-window:] if b.get("close", 0) > 0]
    return sum(ranges) / len(ranges) if ranges else None
