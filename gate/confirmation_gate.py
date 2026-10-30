# gate/confirmation_gate.py
from gate.buckets import BucketSignal, Dir


def evaluate_bucket_a(vp, close):
    """
    Bucket A - Structure / Auction Context.
    LONG  if close > VAH and close > VWAP.
    SHORT if close < VAL and close < VWAP.
    NEUTRAL if inside the Value Area.
    Strength = distance outside VA, normalized by VA width.
    """
    if vp is None:
        return BucketSignal("A", Dir.NEUTRAL, 0.0, "no VP (warmup)")
    width = vp.vah - vp.val
    if width <= 0:
        return BucketSignal("A", Dir.NEUTRAL, 0.0, "degenerate VA")

    if close > vp.vah and close > vp.vwap:
        s = min(1.0, (close - vp.vah) / width)
        return BucketSignal(
            "A", Dir.LONG, s,
            f"close {close:.1f} > VAH {vp.vah:.1f}, above VWAP {vp.vwap:.1f}",
        )
    if close < vp.val and close < vp.vwap:
        s = min(1.0, (vp.val - close) / width)
        return BucketSignal(
            "A", Dir.SHORT, s,
            f"close {close:.1f} < VAL {vp.val:.1f}, below VWAP {vp.vwap:.1f}",
        )
    return BucketSignal(
        "A", Dir.NEUTRAL, 0.0,
        f"close {close:.1f} inside VA [{vp.val:.1f}, {vp.vah:.1f}]",
    )


def evaluate_bucket_b(history, window=5, ratio_threshold=0.60):
    """
    Bucket B - Aggression / Participation.
    LONG  if recent delta positive AND buy_ratio >= ratio_threshold.
    SHORT if recent delta negative AND buy_ratio <= 1 - ratio_threshold.
    Strength = |ratio - 0.5| * 4 (so threshold 0.60 <=> strength 0.40).
    """
    if len(history) < window:
        return BucketSignal("B", Dir.NEUTRAL, 0.0, f"warmup ({len(history)}/{window})")
    recent = history[-window:]
    buy = sum(b["buy_vol"] for b in recent)
    sell = sum(b["sell_vol"] for b in recent)
    total = buy + sell
    if total <= 0:
        return BucketSignal("B", Dir.NEUTRAL, 0.0, "no volume")
    ratio = buy / total
    dsum = buy - sell
    if dsum > 0 and ratio >= ratio_threshold - 1e-9:
        s = round(min(1.0, (ratio - 0.5) * 4), 6)
        return BucketSignal("B", Dir.LONG, s, f"delta_sum {dsum:+.2f}, buy_ratio {ratio:.2f}")
    if dsum < 0 and ratio <= 1.0 - ratio_threshold + 1e-9:
        s = round(min(1.0, (0.5 - ratio) * 4), 6)
        return BucketSignal("B", Dir.SHORT, s, f"delta_sum {dsum:+.2f}, buy_ratio {ratio:.2f}")
    return BucketSignal(
        "B", Dir.NEUTRAL, 0.0,
        f"delta_sum {dsum:+.2f}, buy_ratio {ratio:.2f} (no conviction, need "
        f">={ratio_threshold:.2f} or <={1.0 - ratio_threshold:.2f})",
    )


def confirmation_gate(a, b, c, veto_fn=None, close=None, min_aggr_strength=0.4):
    """
    Deterministic 3-bucket gate.
    veto_fn(direction, close) -> (vetoed: bool, reason: str)   [Bucket C walls]
    Returns (allowed: bool, direction: Dir | None, reason: str).
    """
    if a.direction == Dir.NEUTRAL:
        return False, None, f"STAND ASIDE - A neutral ({a.reason})"
    if b.direction != a.direction:
        return False, None, (
            f"STAND ASIDE - A={a.direction.value} B={b.direction.value} disagree"
        )
    if b.strength + 1e-9 < min_aggr_strength:
        return False, None, (
            f"STAND ASIDE - B weak ({b.strength:.2f} < {min_aggr_strength})"
        )
    if c.direction != Dir.NEUTRAL and c.direction != a.direction:
        return False, None, (
            f"STAND ASIDE - C veto ({c.direction.value} vs {a.direction.value})"
        )

    # Dealer-wall proximity veto (Bucket C)
    if veto_fn is not None and close is not None:
        vetoed, veto_reason = veto_fn(a.direction, close)
        if vetoed:
            return False, None, f"STAND ASIDE - C wall veto: {veto_reason}"

    return True, a.direction, (
        f"TRADE {a.direction.value} - A: {a.reason} | "
        f"B: {b.reason} | C: {c.reason}"
    )