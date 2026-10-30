# scripts/verify_step2.py
import asyncio
import logging
import signal

from data.ws_manager import BinanceMarketStream
from data.delta_accumulator import DeltaAccumulator
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("verify2")


async def main() -> None:
    stream = BinanceMarketStream(settings.SYMBOLS, settings.INTERVAL)
    acc = DeltaAccumulator()
    pending: set[asyncio.Task] = set()

    async def on_trade(t: dict) -> None:
        acc.add_trade(t)

    async def finalize_and_log(c: dict) -> None:
        # Short grace so aggTrades still in flight for this bar are counted.
        # Runs as a separate task so the message loop is never blocked.
        await asyncio.sleep(settings.FINALIZE_GRACE_S)

        first = stream.session_first_trade_ms
        partial = first is None or first > c["open_time"] + 1500
        bucket, cvd = acc.finalize(c["open_time"], include_in_cvd=not partial)

        vol_err = 0.0
        if c["volume"] > 0:
            vol_err = abs(bucket.volume - c["volume"]) / c["volume"] * 100.0

        log.info(
            "CLOSED %s %s O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f n=%d | "
            "buy=%.4f sell=%.4f delta=%+.4f cvd=%+.4f aggTrades=%d dVol=%.3f%%%s",
            c["symbol"], c["interval"],
            c["open"], c["high"], c["low"], c["close"],
            c["volume"], c["trades"],
            bucket.buy_vol, bucket.sell_vol,
            bucket.delta, cvd,
            bucket.trade_count, vol_err,
            "  PARTIAL (joined mid-bar, ignore dVol, not in cvd)" if partial else "",
        )

    async def on_candle(c: dict) -> None:
        task = asyncio.create_task(finalize_and_log(c))
        pending.add(task)
        task.add_done_callback(pending.discard)

    stream.on_trade(on_trade)
    stream.on_candle(on_candle)

    run_task = asyncio.create_task(stream.run())

    def shutdown() -> None:
        log.info("Shutdown requested.")
        stream.stop()
        run_task.cancel()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass  # Windows: Ctrl+C falls through to KeyboardInterrupt

    try:
        await run_task
    except asyncio.CancelledError:
        pass
    finally:
        for t in list(pending):
            t.cancel()
        log.info("Stats: %s", stream.stats())
        log.info("Accumulator: %s", acc.stats())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user.")