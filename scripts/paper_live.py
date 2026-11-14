#!/usr/bin/env python3
"""
Live paper trading for the 4-symbol London trend strategy.

Uses the SAME decision + paper-trader logic as the backtest. Only difference
is the data source: live Binance 4h WebSocket instead of historical klines.

Runs 4-8 weeks to validate that live execution matches backtest assumptions.

Usage:
    python -m scripts.paper_live
"""
import asyncio
import json
import logging
import os
from collections import deque
from datetime import datetime, timezone

from config import settings
from data.ws_manager import BinanceMarketStream  # kline + trade, works for both
from engine.decision import evaluate_bucket_a
from execution.paper_trader import PaperTrader

log = logging.getLogger("paper_live")

# The 4 approved symbols from the 12/12 backtest
SYMBOLS = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "ADAUSDT"]
INTERVAL = "4h"

# London session: signals only fire on bars OPENING at 08:00 or 12:00 UTC.
# Those bars CLOSE at 12:00 and 16:00 UTC, and their next-bar-open fills happen
# at 12:00 and 16:00 UTC. No other bars are eligible.
LONDON_OPEN_HOURS = {8, 12}

SIGNALS_LOG = "logs/paper_live/signals.jsonl"
DIVERGENCE_LOG = "logs/paper_live/divergence.jsonl"


class LiveTrendRunner:
    """One runner per symbol. Independent state, independent PaperTrader."""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.history = deque(maxlen=200)
        self.trader = PaperTrader(
            persist=True,
            state_path=f"logs/paper_live/state_{symbol}.json",
        )
        self.pending_signal_info = None  # to track fill divergence
        self.stats = {
            "bars": 0,
            "london_rejects": 0,
            "no_breakout": 0,
            "signals": 0,
            "fills": 0,
        }

    async def on_candle(self, c: dict):
        if not c.get("is_closed", True):
            return

        self.stats["bars"] += 1

        # Build the bar dict — same fields as backtest
        bar = {
            "open_time": c["open_time"],
            "open": c["open"],
            "high": c["high"],
            "low": c["low"],
            "close": c["close"],
        }
        self.history.append(bar)

        if len(self.history) < 50:
            log.info("%s warmup %d/50", self.symbol, len(self.history))
            return

        # If there was a pending signal from the previous bar, this bar's
        # open is the live fill price. Compare to what backtest assumed.
        if self.pending_signal_info is not None:
            assumed = self.pending_signal_info["signal_close"]
            actual = c["open"]
            divergence_pct = (actual - assumed) / assumed * 100
            with open(DIVERGENCE_LOG, "a") as f:
                f.write(json.dumps({
                    "ts": c["open_time"],
                    "symbol": self.symbol,
                    "signal_ts": self.pending_signal_info["signal_ts"],
                    "assumed_fill": assumed,
                    "actual_fill": actual,
                    "divergence_pct": round(divergence_pct, 5),
                }) + "\n")
            self.stats["fills"] += 1
            self.pending_signal_info = None

        # Decide whether to fire a signal on THIS bar
        hour_utc = datetime.fromtimestamp(
            c["open_time"] / 1000, tz=timezone.utc
        ).hour

        allowed = False
        direction = None
        reason = ""

        if hour_utc in LONDON_OPEN_HOURS:
            a = evaluate_bucket_a(list(self.history), vp=None, close=c["close"])
            if a.direction.value != "NEUTRAL":
                allowed = True
                direction = a.direction
                reason = a.reason
                self.stats["signals"] += 1
                log.info(
                    "%s SIGNAL %s @ %.2f — %s",
                    self.symbol, a.direction.value, c["close"], reason[:100],
                )
                # Remember signal for divergence tracking
                self.pending_signal_info = {
                    "signal_ts": c["open_time"],
                    "signal_close": c["close"],
                    "direction": a.direction.value,
                }
                with open(SIGNALS_LOG, "a") as f:
                    f.write(json.dumps({
                        "ts": c["open_time"],
                        "symbol": self.symbol,
                        "direction": a.direction.value,
                        "close": c["close"],
                        "reason": reason,
                    }) + "\n")
            else:
                self.stats["no_breakout"] += 1
        else:
            self.stats["london_rejects"] += 1

        # Feed the bar to the paper trader (same as backtest simulate())
        self.trader.on_bar(bar, allowed, direction, reason)

    async def run(self):
        stream = BinanceMarketStream([self.symbol], INTERVAL)
        stream.on_candle(self.on_candle)
        try:
            await stream.run()
        finally:
            log.info("%s FINAL STATS %s", self.symbol, self.stats)


async def main():
    os.makedirs("logs/paper_live", exist_ok=True)

    runners = [LiveTrendRunner(s) for s in SYMBOLS]
    tasks = [asyncio.create_task(r.run()) for r in runners]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")