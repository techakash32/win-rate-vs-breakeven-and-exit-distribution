# features/volume_profile.py
from dataclasses import dataclass


@dataclass
class VolumeProfileSnapshot:
    poc: float
    vah: float
    val: float
    total_volume: float
    vwap: float


def compute_volume_profile(bars, va_percent=0.70):
    """Aggregate price-level volume across bars; expand from POC to va_percent."""
    if not bars:
        return None

    agg: dict[float, float] = {}
    for bar in bars:
        for price, vol in bar["price_volumes"].items():
            agg[price] = agg.get(price, 0.0) + vol
    if not agg:
        return None

    total = sum(agg.values())
    if total <= 0:
        return None

    poc = max(agg, key=agg.get)
    prices = sorted(agg.keys())
    lo = hi = prices.index(poc)
    running = agg[poc]
    target = total * va_percent

    while running < target and (lo > 0 or hi < len(prices) - 1):
        lo_vol = agg[prices[lo - 1]] if lo > 0 else -1.0
        hi_vol = agg[prices[hi + 1]] if hi < len(prices) - 1 else -1.0
        if lo_vol >= hi_vol:
            lo -= 1
            running += agg[prices[lo]]
        else:
            hi += 1
            running += agg[prices[hi]]

    total_pv = 0.0
    total_v = 0.0
    for bar in bars:
        if bar["volume"] > 0 and bar["high"] > 0:
            tp = (bar["high"] + bar["low"] + bar["close"]) / 3.0
            total_pv += tp * bar["volume"]
            total_v += bar["volume"]
    vwap = total_pv / total_v if total_v > 0 else 0.0

    return VolumeProfileSnapshot(
        poc=poc, vah=prices[hi], val=prices[lo], total_volume=total, vwap=vwap,
    )