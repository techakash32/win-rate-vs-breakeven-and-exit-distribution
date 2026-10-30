# engine/pipeline.py
"""Shared live pipeline: REST preload -> trades -> bars -> VP -> buckets -> gate.
verify_step3 runs it bare; verify_step4 subclasses it and adds overlays."""
import asyncio
import json
import logging
import os
import time

from data.ws_manager import BinanceMarketStream
from data.bar_aggregator import BarAggregator
from engine.decision import Decider
from gate.buckets import BucketSignal, Dir
from scripts import preload_history as ph
from config import settings

log = logging.getLogger("pipeline")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    )


class Pipeline:
    """REST preload -> trades -> bars -> volume profile -> buckets A/B/C -> gate."""

    def __init__(self, stream: BinanceMarketStream):
        assert settings.WARMUP_BARS <= settings.VP_WINDOW_BARS, \
            "WARMUP_BARS must be <= VP_WINDOW_BARS"
        self.stream = stream
        self.agg = BarAggregator(
            bucket_size=settings.VP_BUCKET_SIZE,
            # +1 so the profile can exclude the bar that just closed
            history_len=max(settings.VP_WINDOW_BARS, settings.AGGR_WINDOW_BARS) + 1,
        )
        self.decider = Decider()
        self._upgrading = False
        self.pending: set[asyncio.Task] = set()

    # ---- startup: disk, then REST preload ----

    def restore(self) -> int:
        needed = settings.WARMUP_BARS + (1 if settings.VP_EXCLUDE_CURRENT else 0)
        n = self.agg.load(settings.BAR_HISTORY_PATH)
        if n >= needed:
            log.info("Restored %d bars from disk.", n)
            return n

        if not settings.PRELOAD_ENABLED:
            log.info("History has %d/%d bars; live warmup will fill the rest.", n, needed)
            return n

        log.info("History only %d/%d bars - preloading from REST...", n, needed)
        try:
            count, path = ph.preload(
                settings.SYMBOLS[0], settings.INTERVAL,
                settings.VP_BUCKET_SIZE, keep=self.agg.history_len,
            )
            n = self.agg.load(path)
            log.info("Preloaded %d bars from REST (volume profile APPROXIMATED "
                     "until live bars replace them).", n)
        except Exception as exc:
            log.warning("Preload failed (%s) - falling back to live warmup.", exc)
        return n

    # ---- stream callbacks ----

    async def on_trade(self, t: dict) -> None:
        self.agg.add_trade(t)

    async def on_candle(self, c: dict) -> None:
        # Separate task: short grace for in-flight trades without blocking
        # the message loop.
        task = asyncio.create_task(self._finalize_after_grace(c))
        self.pending.add(task)
        task.add_done_callback(self.pending.discard)

    async def _finalize_after_grace(self, c: dict) -> None:
        await asyncio.sleep(settings.FINALIZE_GRACE_S)
        first = self.stream.session_first_trade_ms
        partial = first is None or first > c["open_time"] + 1500
        healed = await self._heal_gap(c, partial)
        self.process_candle(c, partial, healed)
        self._schedule_upgrade()

    def _schedule_upgrade(self) -> None:
        """Keep trying to replace approximated bars with real ones in the background."""
        if not (settings.UPGRADE_APPROX_BARS and settings.PRELOAD_ENABLED) or self._upgrading:
            return
        targets = [b["open_time"] for b in self.agg.history() if b.get("approx_vp")][-3:]
        if not targets:
            return
        self._upgrading = True
        task = asyncio.create_task(self._upgrade(targets))
        self.pending.add(task)
        task.add_done_callback(self.pending.discard)

    async def _upgrade(self, targets: list) -> None:
        try:
            for ot in targets:
                bars = await asyncio.to_thread(
                    ph.fetch_bars, settings.SYMBOLS[0], settings.INTERVAL,
                    settings.VP_BUCKET_SIZE, ot, ot, int(time.time() * 1000),
                )
                if bars and not bars[0].get("approx_vp") and self.agg.replace_bar(ot, bars[0]):
                    log.info("Upgraded bar %d to a real volume profile.", ot)
        except Exception as exc:
            log.debug("approx-bar upgrade failed: %s", exc)
        finally:
            self._upgrading = False

    async def _heal_gap(self, c: dict, partial: bool) -> bool:
        """
        Fill bars missing between the history and this candle from REST
        (restart mid-bar, reconnect gap). For a partial bar the REST copy of
        that bar itself is used, since our live copy is incomplete.
        Returns True if the candle's own bar is now in history.
        """
        if not settings.PRELOAD_ENABLED:
            return False
        hist = self.agg.history()
        if not hist:
            return False
        step = self.agg.interval_ms
        expected = hist[-1]["open_time"] + step
        end = c["open_time"] if partial else c["open_time"] - step
        if end < expected:
            return False
        try:
            bars = await asyncio.to_thread(
                ph.fetch_bars, settings.SYMBOLS[0], settings.INTERVAL,
                settings.VP_BUCKET_SIZE, expected, end, c["close_time"] + 1,
            )
            added = self.agg.insert_bars(bars)
            n_real = sum(1 for b in bars if not b.get("approx_vp"))
            log.info("Healed %d missing bar(s) from REST (%d with real volume profile).",
                     added, n_real)
        except Exception as exc:
            log.warning("Gap heal failed (%s).", exc)
            return False
        hist = self.agg.history()
        return bool(hist) and hist[-1]["open_time"] == c["open_time"]

    # ---- extension hooks (overridden in Step 4) ----

    @property
    def cooldown(self) -> int:
        return self.decider.cooldown

    def bucket_c(self, close: float):
        """Returns (BucketSignal for C, veto_fn or None)."""
        return BucketSignal("C", Dir.NEUTRAL, 0.0, "not wired (step 3)"), None

    def on_skipped_bar(self, c: dict) -> None:
        """Called for a partial bar that did not enter history."""

    def on_decision(self, c, bar, a, b, c_sig, allowed, direction, reason) -> None:
        """Called after every gate decision (post cooldown)."""

    # ---- core logic (synchronous, easy to test) ----

    def process_candle(self, c: dict, partial: bool, healed: bool = False) -> dict | None:
        self.agg.finalize(c, partial=partial)   # partial bars never enter history

        if partial and not healed:
            log.info("PARTIAL bar skipped (joined mid-bar). History %d bars.",
                     self.agg.size())
            self.on_skipped_bar(c)
            return None

        self.agg.save(settings.BAR_HISTORY_PATH)
        hist = self.agg.history()
        bar = hist[-1]

        close = c["close"]
        d = self.decider.decide(hist, close, self.bucket_c(close))
        if d.warmup:
            log.info("WARMUP %d/%d", d.warmup_have, settings.WARMUP_BARS)
            return None

        vp, a, b, c_sig = d.vp, d.a, d.b, d.c_sig
        allowed, direction, reason = d.allowed, d.direction, d.reason
        suppressed, approx = d.suppressed, d.approx
        vp_bars = range(d.vp_bars_n)          # only its length is used below

        header = (
            f"[A {a.direction.value} {a.strength:.2f}] "
            f"[B {b.direction.value} {b.strength:.2f}] "
            f"[C {c_sig.direction.value if c_sig.active else 'inactive'}]"
        )
        diag = "n/a"
        if vp:
            diag = (f"VA[{vp.val:.1f}-{vp.vah:.1f}] POC={vp.poc:.1f} "
                    f"VWAP={vp.vwap:.1f} close={close:.1f} "
                    f"approxVP={approx}/{len(vp_bars)}")
        extra = "" if allowed else f" | B: {b.reason}"
        log.info("%s -> %s | %s%s", header, reason, diag, extra)
        self.on_decision(c, bar, a, b, c_sig, allowed, direction, reason)

        decision = {
            "ts_ms": int(time.time() * 1000),
            "bar_open_time": c["open_time"],
            "close": close,
            "vp": ({"poc": vp.poc, "vah": vp.vah, "val": vp.val, "vwap": vp.vwap}
                   if vp else None),
            "vp_bars": len(vp_bars),
            "vp_approx_bars": approx,
            "a": {"dir": a.direction.value, "strength": a.strength, "reason": a.reason},
            "b": {"dir": b.direction.value, "strength": b.strength, "reason": b.reason},
            "c": {"dir": c_sig.direction.value, "active": c_sig.active,
                  "reason": c_sig.reason, "levels": c_sig.extra},
            "allowed": allowed,
            "suppressed_by_cooldown": suppressed,
            "direction": direction.value if (direction and allowed) else None,
            "reason": reason,
            "bar_vol_err_pct": bar.get("vol_err_pct", 0.0),
        }
        folder = os.path.dirname(settings.DECISIONS_LOG_PATH)
        if folder:
            os.makedirs(folder, exist_ok=True)
        with open(settings.DECISIONS_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(decision) + "\n")
        return decision




# Backwards-compatible name
Step3Runner = Pipeline