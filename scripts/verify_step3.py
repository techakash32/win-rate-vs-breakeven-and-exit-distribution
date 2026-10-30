# scripts/verify_step3.py
"""Step 3 baseline: the shared pipeline with NO overlays (no GEX, LLM or paper trader)."""
import asyncio
import logging
import signal

from data.ws_manager import BinanceMarketStream
from engine.pipeline import Pipeline, configure_logging
from config import settings

log = logging.getLogger("verify3")


async def main() -> None:
    stream = BinanceMarketStream(settings.SYMBOLS, settings.INTERVAL)
    runner = Pipeline(stream)
    await asyncio.to_thread(runner.restore)     # blocking REST call off the loop

    stream.on_trade(runner.on_trade)
    stream.on_candle(runner.on_candle)

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
        for t in list(runner.pending):
            t.cancel()
        await asyncio.gather(*runner.pending, return_exceptions=True)
        log.info("Stats: %s", stream.stats())
        log.info("Aggregator: %s", runner.agg.stats())


if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user.")