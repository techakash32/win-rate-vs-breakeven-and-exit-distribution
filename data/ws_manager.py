# data/ws_manager.py
import asyncio
import json
import logging
import os
import time
from typing import Awaitable, Callable

import websockets

from config import settings

log = logging.getLogger(__name__)

CandleCallback = Callable[[dict], Awaitable[None]]
TradeCallback = Callable[[dict], Awaitable[None]]


class BinanceMarketStream:
    """
    Single combined WebSocket for klines (+ optionally aggTrades).
    One connection keeps both feeds in a single delivery order, so by the time
    a kline-close arrives, the bar's aggTrades have (almost always) been
    delivered already. The consumer should still allow a short grace period.

    - Emits only CLOSED candles, each (symbol, open_time) at most once.
    - Reconnects with exponential backoff.
    - session_first_trade_ms: timestamp of the first trade seen since the
      latest (re)connect. A bar that opened before this is only partially
      covered by trade data.
    """

    def __init__(self, symbols: list[str], interval: str, include_trades: bool = True):
        self.symbols = [s.lower() for s in symbols]
        self.interval = interval
        streams = []
        for s in self.symbols:
            streams.append(f"{s}@kline_{interval}")
            if include_trades:
                streams.append(f"{s}@aggTrade")
        self.url = settings.WS_BASE + "/".join(streams)

        self._candle_cbs: list[CandleCallback] = []
        self._trade_cbs: list[TradeCallback] = []
        self._running = False
        self._raw_file = None
        self._last_closed: dict[str, int] = {}
        self.session_first_trade_ms: int | None = None
        self._stats = {
            "messages_received": 0,
            "closed_candles": 0,
            "open_updates_skipped": 0,
            "duplicate_closed_skipped": 0,
            "trades": 0,
            "reconnects": 0,
            "started_at": None,
        }

    def on_candle(self, fn: CandleCallback) -> None:
        self._candle_cbs.append(fn)

    def on_trade(self, fn: TradeCallback) -> None:
        self._trade_cbs.append(fn)

    async def run(self) -> None:
        self._running = True
        self._stats["started_at"] = time.time()
        delay = settings.RECONNECT_BASE_DELAY
        self._open_raw_log()

        try:
            while self._running:
                try:
                    log.info("Connecting to %s", self.url)
                    connect_kwargs = {}
                    host = getattr(settings, "CONNECT_HOST", None)
                    if host:
                        connect_kwargs["host"] = host

                    async with websockets.connect(
                        self.url,
                        ping_interval=20,
                        ping_timeout=20,
                        close_timeout=5,
                        open_timeout=15,
                        max_queue=4096,
                        **connect_kwargs,
                    ) as ws:
                        log.info("Connected. Awaiting candles + trades...")
                        delay = settings.RECONNECT_BASE_DELAY
                        self.session_first_trade_ms = None
                        async for raw in ws:
                            if not self._running:
                                break
                            await self._handle_message(raw)

                    if self._running:
                        self._stats["reconnects"] += 1
                        log.warning(
                            "Stream closed by server. Reconnecting in %.1fs...", delay
                        )
                        await asyncio.sleep(delay)

                except asyncio.CancelledError:
                    log.info("Stream cancelled by caller.")
                    raise
                except Exception as exc:
                    if not self._running:
                        break
                    self._stats["reconnects"] += 1
                    log.warning(
                        "Connection error (%s). Reconnecting in %.1fs...", exc, delay
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, settings.RECONNECT_MAX_DELAY)
        finally:
            self._close_raw_log()

    def stop(self) -> None:
        self._running = False

    def stats(self) -> dict:
        return dict(self._stats)

    # ---- raw log ---------------------------------------------------------

    def _open_raw_log(self) -> None:
        if not settings.LOG_RAW or self._raw_file:
            return
        os.makedirs(os.path.dirname(settings.RAW_LOG_PATH), exist_ok=True)
        self._raw_file = open(settings.RAW_LOG_PATH, "a", encoding="utf-8", buffering=1)

    def _close_raw_log(self) -> None:
        if self._raw_file:
            self._raw_file.close()
            self._raw_file = None

    # ---- message handling ------------------------------------------------

    async def _handle_message(self, raw) -> None:
        self._stats["messages_received"] += 1
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")

        if self._raw_file:
            self._raw_file.write(raw.rstrip() + "\n")

        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Non-JSON frame ignored: %r", raw[:100])
            return

        data = msg.get("data")
        if not data:
            return  # subscription ACK or control frame

        e = data.get("e")
        if e == "kline":
            await self._handle_kline(data["k"])
        elif e == "aggTrade":
            await self._handle_agg_trade(data)

    async def _handle_kline(self, k: dict) -> None:
        if not k["x"]:
            self._stats["open_updates_skipped"] += 1
            return

        if self._last_closed.get(k["s"]) == k["t"]:
            self._stats["duplicate_closed_skipped"] += 1
            return
        self._last_closed[k["s"]] = k["t"]

        candle = {
            "symbol": k["s"],
            "interval": k["i"],
            "open_time": k["t"],
            "close_time": k["T"],
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"]),
            "trades": k["n"],
            "quote_volume": float(k["q"]),
        }
        self._stats["closed_candles"] += 1
        for cb in self._candle_cbs:
            try:
                await cb(candle)
            except Exception:
                log.exception("Candle callback failed")

    async def _handle_agg_trade(self, t: dict) -> None:
        self._stats["trades"] += 1
        if self.session_first_trade_ms is None:
            self.session_first_trade_ms = t["T"]

        trade = {
            "symbol": t["s"],
            "price": float(t["p"]),
            "qty": float(t["q"]),
            "time": t["T"],
            # m=True  => buyer is maker  => aggressor was the SELLER
            # m=False => seller is maker => aggressor was the BUYER
            "is_buyer_maker": t["m"],
            "agg_id": t["a"],
        }
        for cb in self._trade_cbs:
            try:
                await cb(trade)
            except Exception:
                log.exception("Trade callback failed")


def BinanceKlineStream(symbols: list[str], interval: str) -> BinanceMarketStream:
    """Step 1 compatibility: klines only, no trades."""
    return BinanceMarketStream(symbols, interval, include_trades=False)