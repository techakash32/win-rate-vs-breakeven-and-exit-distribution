# engine/decision.py
"""
The gate decision for ONE closed bar, as a pure function of the bar history.
Used by the live Pipeline AND the backtest, so both run identical logic
(volume profile window, Bucket A/B, optional C veto, approximated-VP guard,
cooldown).
"""
from dataclasses import dataclass

from config import settings
from features.volume_profile import compute_volume_profile
from gate.buckets import BucketSignal, Dir
from gate.confirmation_gate import (
    evaluate_bucket_a, evaluate_bucket_b, confirmation_gate,
)


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
        vp_bars = hist[:-1] if settings.VP_EXCLUDE_CURRENT else hist
        vp_bars = vp_bars[-settings.VP_WINDOW_BARS:]
        if len(vp_bars) < settings.WARMUP_BARS:
            return Decision(warmup=True, warmup_have=len(vp_bars))

        vp = compute_volume_profile(vp_bars, settings.VP_VA_PERCENT)
        approx = sum(1 for b in vp_bars if b.get("approx_vp"))

        a = evaluate_bucket_a(vp, close)
        b = evaluate_bucket_b(hist, settings.AGGR_WINDOW_BARS,
                              settings.AGGR_RATIO_THRESHOLD)
        c_sig, veto_fn = bucket_c if bucket_c else (NEUTRAL_C, None)

        allowed, direction, reason = confirmation_gate(
            a, b, c_sig, veto_fn=veto_fn, close=close,
            min_aggr_strength=settings.MIN_AGGRESSION_STRENGTH,
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
        if allowed:
            self.cooldown = settings.COOLDOWN_BARS

        return Decision(
            vp=vp, vp_bars_n=len(vp_bars), approx=approx, a=a, b=b, c_sig=c_sig,
            allowed=allowed, direction=direction, reason=reason, suppressed=suppressed,
        )