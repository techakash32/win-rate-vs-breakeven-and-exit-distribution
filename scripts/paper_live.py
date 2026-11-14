#!/usr/bin/env python3
"""Paper-only live runner for the four-symbol 4h London trend configuration.

The decision path is the same Decider used by backtest_step5a, and orders are
simulated only through PaperTrader. This script never submits exchange orders.

Run from the repository root after setting the documented environment vars:
    python -m scripts.paper_live
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timezone

from config import settings
from data.ws_manager import BinanceMarketStream
from engine.decision import Decider
from execution.paper_trader import PaperTrader

log = logging.getLogger("paper_live")

EXPECTED_SYMBOLS = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "ADAUSDT"]
INTERVAL = "4h"
PRELOAD_BARS = 60
HISTORY_LIMIT = 200
LOG_DIR = "logs/paper_live"
SIGNALS_LOG = os.path.join(LOG_DIR, "signals.jsonl")


def validate_config() -> None:
    """Fail closed rather than silently paper-trading a different profile."""
    problems = []
    if getattr(settings, "ACTIVE_STRATEGY", None) != "trend":
        problems.append("ACTIVE_STRATEGY must be 'trend'")
    if getattr(settings, "INTERVAL", None) != INTERVAL:
        problems.append("BACKTEST_INTERVAL must resolve to '4h'")
    if list(getattr(settings, "SYMBOLS", [])) != EXPECTED_SYMBOLS:
        problems.append(f"SYMBOLS must be {EXPECTED_SYMBOLS!r}")
    if getattr(settings, "BUCKET_B_ENABLED", True):
        problems.append("BUCKET_B_ENABLED must be false for this trend profile")
    if not getattr(settings, "TREND_LONDON_ONLY", False):
        problems.append("TREND_LONDON_ONLY must be true")
    if abs(float(getattr(settings, "PAPER_SLIPPAGE_PCT", -1)) - 0.04) > 1e-9:
        problems.append("PAPER_SLIPPAGE_PCT must be 0.04 (percent units)")
    if problems:
        raise RuntimeError("Invalid paper-live configuration: " + "; ".join(problems))
    log.info(
        "Validated profile: trend, %s, London-only, slippage %.4f%%, symbols=%s",
        settings.INTERVAL, settings.PAPER_SLIPPAGE_PCT, settings.SYMBOLS,
    )


def fetch_recent_closed_bars(symbol: str, limit: int = PRELOAD_BARS) -> list[dict]:
    """Download recent closed USD-M klines, including volume fields for the gate."""
    params = urllib.parse.urlencode({
        "symbol": symbol.upper(), "interval": INTERVAL, "limit": limit,
    })
    url = f"{settings.REST_BASE}/fapi/v1/klines?{params}"
    request = urllib.request.Request(url, headers={"User-Agent": "paper-live/1.0"})
    with urllib.request.urlopen(request, timeout=settings.REST_TIMEOUT_S) as response:
        raw = json.loads(response.read())

    now_ms = int(time.time() * 1000)
    bars = []
    for k in raw:
        open_time, close_time = int(k[0]), int(k[6])
        if close_time >= now_ms:
            continue
        bars.append({
            "symbol": symbol.upper(),
            "interval": INTERVAL,
            "open_time": open_time,
            "open": float(k[1]),
            "high": float(k[2]),
            "low": float(k[3]),
            "close": float(k[4]),
            "volume": float(k[5]),
            "close_time": close_time,
            "trades": int(k[8]),
            "quote_volume": float(k[7]),
        })
    return bars


class LiveTrendRunner:
    """Independent warmup, decision state, and PaperTrader per symbol."""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.history: deque[dict] = deque(maxlen=HISTORY_LIMIT)
        self.decider = Decider()
        self.trader = PaperTrader(
            persist=True,
            state_path=os.path.join(LOG_DIR, f"state_{symbol}.json"),
            symbol=symbol,
            initial_equity=settings.PAPER_START_EQUITY / len(settings.SYMBOLS),
        )
        self.stats = {"bars": 0, "duplicates": 0, "signals": 0, "warmup": 0}

    def _remember_decision_state(self, bars: list[dict]) -> None:
        """Replay gate history for warmup/cooldown without creating old entries."""
        for i, bar in enumerate(bars):
            start = max(0, i - HISTORY_LIMIT + 1)
            self.decider.decide(bars[start:i + 1], bar["close"])

    async def preload(self) -> None:
        try:
            bars = await asyncio.to_thread(fetch_recent_closed_bars, self.symbol)
        except Exception as exc:
            log.warning("%s preload failed (%s); continuing with WebSocket warmup",
                        self.symbol, exc)
            return
        if not bars:
            log.warning("%s preload returned no closed bars; continuing with WebSocket warmup",
                        self.symbol)
            return

        for bar in bars:
            self.history.append(bar)

        # Keep the Decider's state consistent with the immediately preceding
        # history. These historical decisions warm the gate only; they do not
        # create retroactive paper entries.
        self._remember_decision_state(bars)

        # Restore/reconcile any persisted position or pending entry with bars
        # newer than its last processed timestamp. manage() never creates a
        # new entry and preserves PaperTrader's normal stop/exit behavior.
        for bar in bars:
            if bar["open_time"] > self.trader._last_ts:
                self.trader.manage(bar)

        # _history is intentionally not persisted by PaperTrader. Seed it so
        # the next live trend entry has its ATR lookback after a process restart.
        if len(self.trader._history) < settings.TREND_ATR_WINDOW:
            self.trader._history = [
                dict(bar) for bar in bars[-max(settings.TREND_ATR_WINDOW, 15):]
            ]
        self.trader._save()

        last = bars[-1]
        last_utc = datetime.fromtimestamp(last["open_time"] / 1000, timezone.utc)
        log.info("%s preload complete: fetched=%d, history=%d, trader_history=%d, "
                 "last_bar_open=%s UTC", self.symbol, len(bars), len(self.history),
                 len(self.trader._history), last_utc.strftime("%Y-%m-%d %H:%M"))

    async def on_candle(self, candle: dict) -> None:
        # BinanceMarketStream emits closed candles only.
        bar = {
            "symbol": self.symbol,
            "interval": candle.get("interval", INTERVAL),
            "open_time": int(candle["open_time"]),
            "open": float(candle["open"]),
            "high": float(candle["high"]),
            "low": float(candle["low"]),
            "close": float(candle["close"]),
            "volume": float(candle.get("volume", 0.0)),
            "trades": int(candle.get("trades", 0)),
            "quote_volume": float(candle.get("quote_volume", 0.0)),
        }
        if self.history and bar["open_time"] <= self.history[-1]["open_time"]:
            self.stats["duplicates"] += 1
            return

        self.history.append(bar)
        self.stats["bars"] += 1
        history = list(self.history)
        decision = self.decider.decide(history, bar["close"])

        if decision.warmup:
            self.stats["warmup"] += 1
            log.info("%s gate warmup: %d VP bars", self.symbol, decision.warmup_have)

        allowed = bool(decision.allowed)
        direction = decision.direction if allowed else None
        reason = decision.reason or ""
        if allowed:
            self.stats["signals"] += 1
            log.info("%s PAPER SIGNAL %s close=%.8g reason=%s",
                     self.symbol, direction.value, bar["close"], reason)
            record = {
                "ts": bar["open_time"], "symbol": self.symbol,
                "direction": direction.value, "close": bar["close"],
                "reason": reason,
            }
            with open(SIGNALS_LOG, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")

        # Mirrors backtest_step5a.simulate(): process the bar, then queue its
        # decision for a next-bar-open fill through PaperTrader.
        self.trader.on_bar(
            bar, allowed, direction, reason,
            decision.a_strength, decision.b_strength, decision.delta_ratio,
            decision.compression_ratio, decision.distance_to_level_pct,
        )

    async def run(self) -> None:
        stream = BinanceMarketStream([self.symbol], INTERVAL, include_trades=False)
        stream.on_candle(self.on_candle)
        try:
            await stream.run()
        finally:
            log.info("%s final paper-live stats: %s", self.symbol, self.stats)


async def main() -> None:
    validate_config()
    os.makedirs(LOG_DIR, exist_ok=True)
    runners = [LiveTrendRunner(symbol) for symbol in settings.SYMBOLS]

    log.info("Preloading up to %d closed %s bars for %d symbols",
             PRELOAD_BARS, INTERVAL, len(runners))
    await asyncio.gather(*(runner.preload() for runner in runners))
    log.info("Preload finished; starting closed-candle WebSocket streams")

    tasks = [asyncio.create_task(runner.run()) for runner in runners]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
