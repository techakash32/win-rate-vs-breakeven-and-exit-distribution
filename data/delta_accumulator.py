# data/delta_accumulator.py
from dataclasses import dataclass

from config import settings


@dataclass
class BarBucket:
    open_time: int = 0
    buy_vol: float = 0.0
    sell_vol: float = 0.0
    trade_count: int = 0

    @property
    def volume(self) -> float:
        return self.buy_vol + self.sell_vol

    @property
    def delta(self) -> float:
        return self.buy_vol - self.sell_vol


class DeltaAccumulator:
    """
    Per-bar buy/sell volume accumulator. Trades are bucketed by the bar their
    execution timestamp falls in. finalize() snapshots a bar and updates CVD.

    Safeguards:
    - Trades for an already-finalized bar are dropped and counted
      (late_trades) instead of creating a bucket that is never finalized.
    - Stale unfinalized buckets (e.g. after a reconnect gap) are discarded.
    - Bars flagged partial (joined mid-bar) can be kept out of CVD.
    """

    def __init__(self, interval_ms: int | None = None):
        self.interval_ms = interval_ms or settings.INTERVAL_MS
        self._buckets: dict[int, BarBucket] = {}
        self._cvd = 0.0
        self._last_finalized = -1
        self.late_trades = 0
        self.discarded_buckets = 0

    def add_trade(self, trade: dict) -> None:
        key = (trade["time"] // self.interval_ms) * self.interval_ms
        if key <= self._last_finalized:
            self.late_trades += 1
            return
        b = self._buckets.get(key)
        if b is None:
            b = self._buckets[key] = BarBucket(open_time=key)
        if trade["is_buyer_maker"]:
            b.sell_vol += trade["qty"]
        else:
            b.buy_vol += trade["qty"]
        b.trade_count += 1

    def finalize(self, open_time: int, include_in_cvd: bool = True):
        b = self._buckets.pop(open_time, None) or BarBucket(open_time=open_time)

        stale = [k for k in self._buckets if k < open_time]
        for k in stale:
            del self._buckets[k]
        self.discarded_buckets += len(stale)

        self._last_finalized = max(self._last_finalized, open_time)
        if include_in_cvd:
            self._cvd += b.delta
        return b, self._cvd

    def cvd(self) -> float:
        return self._cvd

    def stats(self) -> dict:
        return {
            "late_trades": self.late_trades,
            "discarded_buckets": self.discarded_buckets,
            "open_buckets": len(self._buckets),
        }