# data/bar_aggregator.py
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field

from config import settings

log = logging.getLogger(__name__)


@dataclass
class BarBucket:
    open_time: int = 0
    buy_vol: float = 0.0
    sell_vol: float = 0.0
    trade_count: int = 0
    price_volumes: dict = field(default_factory=dict)

    @property
    def volume(self) -> float:
        return self.buy_vol + self.sell_vol

    @property
    def delta(self) -> float:
        return self.buy_vol - self.sell_vol

    def add_trade(self, price, qty, is_buyer_maker, bucket_size) -> None:
        level = round(round(price / bucket_size) * bucket_size, 8)
        self.price_volumes[level] = self.price_volumes.get(level, 0.0) + qty
        self.trade_count += 1
        if is_buyer_maker:      # buyer is maker => aggressor was the SELLER
            self.sell_vol += qty
        else:
            self.buy_vol += qty


class BarAggregator:
    """
    Aggregates aggTrades into bars and keeps a rolling history of finalized,
    COMPLETE bars. Each bar holds buy/sell volume, delta, OHLC (from the
    kline, which is authoritative) and per-price-level volume.

    Safeguards carried over from Step 2:
    - trades for an already-finalized bar are dropped and counted (late_trades)
    - stale unfinalized buckets are discarded
    - partial bars (joined mid-bar / reconnect) are NOT added to history or CVD
    """

    def __init__(self, interval_ms=None, bucket_size=None, history_len=240):
        self.interval_ms = interval_ms or settings.INTERVAL_MS
        self.bucket_size = bucket_size or settings.VP_BUCKET_SIZE
        self._buckets: dict[int, BarBucket] = {}
        self.history_len = history_len
        self._history: deque = deque(maxlen=history_len)
        self._cvd = 0.0
        self._last_finalized = -1
        self.late_trades = 0
        self.discarded_buckets = 0
        self.partial_bars = 0

    # ---- trades / bars ---------------------------------------------------

    def add_trade(self, trade: dict) -> None:
        key = (trade["time"] // self.interval_ms) * self.interval_ms
        if key <= self._last_finalized:
            self.late_trades += 1
            return
        b = self._buckets.get(key)
        if b is None:
            b = self._buckets[key] = BarBucket(open_time=key)
        b.add_trade(trade["price"], trade["qty"],
                    trade["is_buyer_maker"], self.bucket_size)

    def finalize(self, candle: dict, partial: bool = False) -> dict:
        """Snapshot the bar that `candle` closed. Returns the bar dict."""
        open_time = candle["open_time"]
        b = self._buckets.pop(open_time, None) or BarBucket(open_time=open_time)

        stale = [k for k in self._buckets if k < open_time]
        for k in stale:
            del self._buckets[k]
        self.discarded_buckets += len(stale)
        self._last_finalized = max(self._last_finalized, open_time)

        vol_err = 0.0
        if candle["volume"] > 0:
            vol_err = abs(b.volume - candle["volume"]) / candle["volume"] * 100.0

        bar = {
            "open_time": open_time,
            "buy_vol": b.buy_vol,
            "sell_vol": b.sell_vol,
            "volume": b.volume,
            "delta": b.delta,
            "trade_count": b.trade_count,
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "kline_volume": candle["volume"],
            "vol_err_pct": vol_err,
            "partial": partial,
            "price_volumes": dict(b.price_volumes),
        }

        if partial:
            self.partial_bars += 1
            bar["cvd"] = self._cvd
            return bar

        if self._history:
            expected = self._history[-1]["open_time"] + self.interval_ms
            if open_time != expected:
                missed = (open_time - expected) // self.interval_ms
                log.warning("Gap in bar history: %d bar(s) missing before %d",
                            missed, open_time)
        self._cvd += b.delta
        bar["cvd"] = self._cvd
        self._history.append(bar)
        return bar

    def insert_bars(self, bars: list) -> int:
        """
        Append already-complete bars (e.g. from REST) that are newer than the
        current history. Used to heal gaps. Returns how many were added.
        """
        added = 0
        for bar in sorted(bars, key=lambda b: b["open_time"]):
            if self._history and bar["open_time"] <= self._history[-1]["open_time"]:
                continue
            bar = dict(bar)
            self._cvd += bar["delta"]
            bar["cvd"] = self._cvd
            self._history.append(bar)
            self._last_finalized = max(self._last_finalized, bar["open_time"])
            added += 1
        return added

    def replace_bar(self, open_time: int, new_bar: dict) -> bool:
        """Swap in a better copy of an existing bar (e.g. approximated -> real).
        Keeps the running CVD value of the bar it replaces."""
        for i, old in enumerate(self._history):
            if old["open_time"] == open_time:
                nb = dict(new_bar)
                nb["cvd"] = old.get("cvd", self._cvd)
                self._history[i] = nb
                return True
        return False

    # ---- accessors -------------------------------------------------------

    def history(self) -> list:
        return list(self._history)

    def cvd(self) -> float:
        return self._cvd

    def size(self) -> int:
        return len(self._history)

    def stats(self) -> dict:
        return {
            "bars": len(self._history),
            "partial_bars": self.partial_bars,
            "late_trades": self.late_trades,
            "discarded_buckets": self.discarded_buckets,
            "open_buckets": len(self._buckets),
        }

    # ---- persistence -----------------------------------------------------

    def save(self, path: str) -> None:
        """Atomic write: a crash mid-save can't leave a half-written file."""
        folder = os.path.dirname(path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for bar in self._history:
                f.write(json.dumps(bar) + "\n")
        os.replace(tmp, path)

    def load(self, path: str, now_ms: int | None = None, max_gap_bars: int | None = None) -> int:
        """Restore saved history. Returns bars restored (0 if none / too stale)."""
        if not os.path.exists(path):
            return 0
        if max_gap_bars is None:
            max_gap_bars = settings.MAX_RESTORE_GAP_BARS
        if now_ms is None:
            now_ms = int(time.time() * 1000)

        bars = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    bar = json.loads(line)
                    # JSON turns float keys into strings: coerce them back
                    bar["price_volumes"] = {
                        float(k): v for k, v in bar.get("price_volumes", {}).items()
                    }
                except Exception:
                    continue
                bars.append(bar)

        if not bars:
            return 0
        bars = bars[-self._history.maxlen:]

        age_bars = (now_ms - bars[-1]["open_time"]) / self.interval_ms
        if age_bars > max_gap_bars:
            log.warning(
                "Saved history is stale (last bar %.0f bars ago > %d). Starting fresh.",
                age_bars, max_gap_bars,
            )
            return 0

        self._history.clear()
        self._history.extend(bars)
        self._cvd = bars[-1].get("cvd", 0.0)
        self._last_finalized = bars[-1]["open_time"]
        return len(self._history)