# scripts/verify_step4.py
"""Step 4: shared pipeline + Bucket C (GEX audit), Groq narrator, paper trader."""
import asyncio
import logging
import signal

from data.ws_manager import BinanceMarketStream
from data.gex_cache import GexCache
from data.deribit_gex import DeribitGexCache
from engine.pipeline import Pipeline, configure_logging
from execution.paper_trader import PaperTrader
from llm.interpreter import GroqInterpreter
from config import settings

try:                                  # optional: read keys from a .env file
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

log = logging.getLogger("verify4")


class Step4Runner(Pipeline):
    """Pipeline + overlays. Everything else is inherited unchanged."""

    def __init__(self, stream, gex, interpreter: GroqInterpreter,
                 trader: PaperTrader):
        super().__init__(stream)
        self.gex = gex
        self.interpreter = interpreter
        self.trader = trader

    def bucket_c(self, close):
        return self.gex.snapshot(close), self.gex.veto

    @staticmethod
    def _bar(c: dict) -> dict:
        return {"open_time": c["open_time"], "open": c["open"],
                "high": c["high"], "low": c["low"], "close": c["close"]}

    def on_skipped_bar(self, c):
        # a partial bar still has authoritative kline OHLC: keep managing the position
        self.trader.manage(self._bar(c))

    def on_decision(self, c, bar, a, b, c_sig, allowed, direction, reason):
        status = self.trader.on_bar(self._bar(c), allowed, direction, reason)

        # Narrate only trades that were actually taken: TRADE -> PAPER SIGNAL -> LLM
        # are 1:1:1; ignored signals log PAPER SKIP and cost no LLM budget.
        if status == "QUEUED":
            queued = self.interpreter.submit({
                "decision": direction.value,
                "close": c["close"],
                "structure": {"direction": a.direction.value,
                              "strength": round(a.strength, 3), "reason": a.reason},
                "aggression": {"direction": b.direction.value,
                               "strength": round(b.strength, 3), "reason": b.reason},
                "dealer": {"direction": c_sig.direction.value,
                           "active": c_sig.active, "reason": c_sig.reason},
            })
            if not queued:
                log.info("LLM disabled (no GROQ_API_KEY) - no rationale for this trade.")


def make_gex():
    if settings.GEX_SOURCE == "deribit":
        return DeribitGexCache()
    return GexCache()


def log_effective_rr() -> None:
    """Fees are part of the risk: show what the configured R:R really nets."""
    stop = settings.PAPER_STOP_PCT
    tk, mk = settings.PAPER_FEE_TAKER_PCT, settings.PAPER_FEE_MAKER_PCT
    lose = -(1.0 + (tk + tk) / stop)
    win = settings.PAPER_TARGET_R_MULTIPLE - (tk + mk) / stop
    log.info("Paper R:R: stop %.2f%% / target %.1fR gross -> net %+.2fR on a stop, "
             "%+.2fR on a target (fees = %.0f%%-%.0f%% of 1R).",
             stop, settings.PAPER_TARGET_R_MULTIPLE, lose, win,
             (tk + mk) / stop * 100, (tk + tk) / stop * 100)


async def main() -> None:
    stream = BinanceMarketStream(settings.SYMBOLS, settings.INTERVAL)
    gex = make_gex()
    interpreter = GroqInterpreter()
    trader = PaperTrader()
    runner = Step4Runner(stream, gex, interpreter, trader)

    log_effective_rr()
    if gex.enabled():
        log.info("Bucket C: source %s.%s", gex.symbol,
                 "" if settings.GEX_SOURCE == "deribit" else
                 " (FlashAlpha levels are not BTC-comparable: veto stays INACTIVE.)")
    else:
        log.info("Bucket C: INACTIVE (%s).", gex.disabled_reason())
    log.info("LLM narrator: %s", "enabled" if interpreter.enabled()
             else "disabled (GROQ_API_KEY not set)")

    await asyncio.to_thread(runner.restore)
    trader.reconcile(runner.agg.history())      # resolve any position left from last run

    stream.on_trade(runner.on_trade)
    stream.on_candle(runner.on_candle)

    run_task = asyncio.create_task(stream.run())
    gex_task = asyncio.create_task(gex.run())
    llm_task = asyncio.create_task(interpreter.run())

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
        stream.stop()
        tasks = [gex_task, llm_task, *runner.pending]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)   # let cancellation finish
        if trader.position is not None:
            log.info("Open paper position at exit (saved, resumes on restart): %s @ %.2f",
                     trader.position["side"], trader.position["entry"])
        log.info("Stats: %s", stream.stats())
        log.info("Aggregator: %s", runner.agg.stats())
        log.info("GEX: %s", gex.stats())
        log.info("Interpreter: %s", interpreter.stats())
        log.info("Equity: %.2f | closed trades this run: %d", trader.equity, len(trader.trades))


if __name__ == "__main__":
    configure_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user.")