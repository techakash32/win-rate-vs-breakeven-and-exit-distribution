# scripts/verify_step1.py
import asyncio
import logging
import signal

from data.ws_manager import BinanceKlineStream
from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("verify")


async def main() -> None:
    stream = BinanceKlineStream(settings.SYMBOLS, settings.INTERVAL)

    async def on_candle(c: dict) -> None:
        log.info(
            "CLOSED  %s  %s  O=%.2f H=%.2f L=%.2f C=%.2f  V=%.4f  trades=%d",
            c["symbol"], c["interval"],
            c["open"], c["high"], c["low"], c["close"],
            c["volume"], c["trades"],
        )

    stream.on_candle(on_candle)

    # Run the stream as a task so shutdown can cancel it immediately,
    # even if the socket goes quiet.
    task = asyncio.create_task(stream.run())

    def shutdown() -> None:
        log.info("Shutdown requested.")
        stream.stop()
        task.cancel()

    # Linux/macOS signal handling. Windows doesn't support
    # add_signal_handler; there Ctrl+C falls through to KeyboardInterrupt.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass

    try:
        await task
    except asyncio.CancelledError:
        pass  # expected on shutdown
    finally:
        log.info("Stats: %s", stream.stats())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass  # Windows Ctrl+C path