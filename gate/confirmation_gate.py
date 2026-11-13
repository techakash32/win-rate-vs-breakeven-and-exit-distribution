from gate.buckets import BucketSignal, Dir
from config import settings


def evaluate_bucket_a(vp, close):
    """Bucket A structure/auction context: position relative to value area."""
    if vp is None:
        return BucketSignal("A", Dir.NEUTRAL, 0.0, "no VP (warmup)")
    width = vp.vah - vp.val
    if width <= 0:
        return BucketSignal("A", Dir.NEUTRAL, 0.0, "degenerate VA")
    if close > vp.vah and close > vp.vwap:
        strength = min(1.0, (close - vp.vah) / width)
        return BucketSignal("A", Dir.LONG, strength,
                            f"close {close:.1f} > VAH {vp.vah:.1f}, above VWAP {vp.vwap:.1f}")
    if close < vp.val and close < vp.vwap:
        strength = min(1.0, (vp.val - close) / width)
        return BucketSignal("A", Dir.SHORT, strength,
                            f"close {close:.1f} < VAL {vp.val:.1f}, below VWAP {vp.vwap:.1f}")
    return BucketSignal("A", Dir.NEUTRAL, 0.0,
                        f"close {close:.1f} inside VA [{vp.val:.1f}, {vp.vah:.1f}]")


def evaluate_bucket_b(history, window=5, threshold=None):
    """Bucket B aggression/participation from recent buy/sell volume."""
    if not getattr(settings, "BUCKET_B_ENABLED", True):
        return BucketSignal("B", Dir.NEUTRAL, 0.0, "disabled", disabled=True)
    strategy = getattr(settings, "ACTIVE_STRATEGY", "sweep")
    if threshold is None:
        threshold = (settings.AGGR_RATIO_THRESHOLD_SWEEP if strategy == "sweep"
                     else settings.AGGR_RATIO_THRESHOLD)
    if len(history) < window:
        return BucketSignal("B", Dir.NEUTRAL, 0.0, f"warmup ({len(history)}/{window})")
    recent = history[-window:]
    buy = sum(b["buy_vol"] for b in recent)
    sell = sum(b["sell_vol"] for b in recent)
    total = buy + sell
    if total <= 0:
        return BucketSignal("B", Dir.NEUTRAL, 0.0, "no volume")
    ratio = buy / total
    delta = buy - sell
    if delta > 0 and ratio >= threshold - 1e-9:
        strength = round(min(1.0, (ratio - 0.5) * 4), 6)
        return BucketSignal("B", Dir.LONG, strength,
                            f"delta_sum {delta:+.2f}, buy_ratio {ratio:.2f}")
    if delta < 0 and ratio <= 1.0 - threshold + 1e-9:
        strength = round(min(1.0, (0.5 - ratio) * 4), 6)
        return BucketSignal("B", Dir.SHORT, strength,
                            f"delta_sum {delta:+.2f}, buy_ratio {ratio:.2f}")
    return BucketSignal("B", Dir.NEUTRAL, 0.0,
                        f"delta_sum {delta:+.2f}, buy_ratio {ratio:.2f} (no conviction, need "
                        f">={threshold:.2f} or <={1.0-threshold:.2f})")


def confirmation_gate(a, b, c, veto_fn=None, close=None,
                      min_aggr_strength=0.4, strategy="sweep"):
    """Apply strategy-specific A/B confirmation and retain C as a veto."""
    if strategy not in ("sweep", "absorption", "funding_fade", "trend"):
        raise ValueError(
            f"Unsupported ACTIVE_STRATEGY {strategy!r}; use 'sweep', 'absorption', 'funding_fade', or 'trend'."
        )
    if a.direction == Dir.NEUTRAL:
        return False, None, f"STAND ASIDE - A neutral ({a.reason})"
    if not getattr(b, "disabled", False):
        if strategy == "sweep":
            if b.direction == Dir.NEUTRAL:
                return False, None, f"STAND ASIDE - B neutral ({b.reason})"
            if b.direction == a.direction:
                return False, None, (
                    f"STAND ASIDE - no rejection (A=B={a.direction.value}, "
                    f"B strength {b.strength:.2f})"
                )
        elif strategy in ("absorption", "trend"):
            if b.direction != a.direction:
                return False, None, f"STAND ASIDE - A={a.direction.value} B={b.direction.value} disagree"
        elif strategy == "funding_fade":
            b_mode = getattr(settings, "FUNDING_B_MODE", "confirm")
            if b_mode not in ("confirm", "oppose"):
                raise ValueError("FUNDING_B_MODE must be 'confirm' or 'oppose'")
            if b_mode == "confirm" and b.direction != a.direction:
                return False, None, f"STAND ASIDE - funding A={a.direction.value} B={b.direction.value} disagree"
            if b_mode == "oppose" and (b.direction == Dir.NEUTRAL or b.direction == a.direction):
                return False, None, f"STAND ASIDE - funding A={a.direction.value} requires opposing B"
        if strategy not in ("sweep", "funding_fade") and b.strength + 1e-9 < min_aggr_strength:
            return False, None, f"STAND ASIDE - B weak ({b.strength:.2f} < {min_aggr_strength})"
    if c.direction != Dir.NEUTRAL and c.direction != a.direction:
        return False, None, f"STAND ASIDE - C veto ({c.direction.value} vs {a.direction.value})"
    if veto_fn is not None and close is not None:
        vetoed, veto_reason = veto_fn(a.direction, close)
        if vetoed:
            return False, None, f"STAND ASIDE - C wall veto: {veto_reason}"
    return True, a.direction, (f"TRADE {a.direction.value} - A: {a.reason} | "
                               f"B: {b.reason} | C: {c.reason}")
