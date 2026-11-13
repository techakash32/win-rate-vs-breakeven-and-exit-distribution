# execution/paper_trader.py
import json
import logging
import math
import os

from config import settings
from gate.buckets import Dir

log = logging.getLogger(__name__)


class PaperTrader:
    """
    One-position-at-a-time paper trader.

    - A gate signal on bar N becomes a PENDING entry; it fills at bar N+1's
      OPEN (you cannot trade the close you just observed). The entry bar's own
      range is then checked against the stop/target.
    - Existing strategies use a fixed % stop and fixed R target. Trend mode
      sizes the stop at 2x ATR, has no fixed target, and trails the stop after
      each completed bar (the updated stop is active from the next bar).
    - Fees: entry and STOP/EOD/STALE exits are taker (market); the TARGET exit
      is maker (resting limit, assumed to fill when touched - optimistic).
    - If a bar touches both stop and target, the STOP is assumed first and the
      trade is flagged ambiguous_bar=True.
    - Equity, the open position and any pending entry are saved to disk after
      every change, so a restart resumes them. reconcile() replays bars missed
      during downtime and force-closes a zombie older than PAPER_STALE_BARS.
    - TIME_STOP: after PAPER_MAX_HOLD_BARS bars the trade is closed at the bar
      close (frees the system; a stalled trade can't lock it out for hours).
    - Optional breakeven stop (PAPER_BREAKEVEN_AT_R), off by default.
    Not modelled: slippage beyond the next-open fill, partial fills.
    """

    def __init__(self, state_path=None, persist=True, log_trades=True,
                 initial_equity=None, symbol=None, equity=None):
        if equity is not None and initial_equity is not None:
            if not math.isclose(float(equity), float(initial_equity)):
                raise ValueError(
                    f"equity={equity} and initial_equity={initial_equity} disagree"
                )
        resolved_equity = equity if equity is not None else initial_equity
        if resolved_equity is None:
            resolved_equity = settings.PAPER_START_EQUITY
        try:
            resolved_equity = float(resolved_equity)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"initial equity must be finite and > 0, got {resolved_equity!r}") from exc
        if not math.isfinite(resolved_equity) or resolved_equity <= 0:
            raise ValueError(f"initial equity must be finite and > 0, got {resolved_equity!r}")

        # persist=False / log_trades=False: used by the backtest (no disk I/O)
        self.persist = persist
        self.log_trades = log_trades
        self.state_path = state_path or settings.PAPER_STATE_PATH
        self.initial_equity = resolved_equity
        self.equity = resolved_equity
        self.symbol = symbol
        self.position = None
        self.pending = None
        self.trades = []
        self.last_bar = None
        self._last_ts = -1
        self._history = []
        if persist:
            self._load()

    # ---- persistence ----

    def _save(self) -> None:
        if not self.persist:
            return
        folder = os.path.dirname(self.state_path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"equity": self.equity, "position": self.position,
                       "pending": self.pending, "last_ts": self._last_ts}, f)
        os.replace(tmp, self.state_path)

    def _load(self) -> None:
        try:
            with open(self.state_path, encoding="utf-8") as f:
                d = json.load(f)
        except (OSError, ValueError):
            return
        self.equity = float(d.get("equity", self.equity))
        self.position = d.get("position")
        self.pending = d.get("pending")
        self._last_ts = int(d.get("last_ts", -1))
        if self.position or self.pending:
            log.warning("Paper state restored: equity=%.2f position=%s pending=%s",
                        self.equity,
                        self.position["side"] if self.position else None,
                        self.pending["side"] if self.pending else None)

    # ---- public API ----

    def manage(self, bar: dict) -> None:
        """Advance with a finished bar (fill pending / manage position); no new signals."""
        self._advance(bar)

    def on_bar(self, bar: dict, allowed: bool, direction, reason: str,
               a_strength=None, b_strength=None, delta_ratio=None,
               compression_ratio=None, distance_to_level_pct=None):
        """
        Advance with the bar that just closed, then handle the gate decision.
        Returns "QUEUED" (entry will fill at the next bar's open), "SKIPPED"
        (signal ignored because a position/entry is already live), or None
        (no signal). Every allowed signal is therefore either QUEUED or logged
        as PAPER SKIP - never silent.
        """
        self._advance(bar)
        if not (allowed and direction is not None):
            return None
        if self.position is not None or self.pending is not None:
            live = self.position or self.pending
            log.info("PAPER SKIP - %s already %s (%s), signal %s ignored",
                     "position" if self.position else "entry",
                     "open" if self.position else "pending", live["side"], direction.value)
            return "SKIPPED"
        self.pending = {
            "side": direction.value, "signal_ts": bar["open_time"],
            "signal_close": bar["close"], "reason": reason,
            "a_strength": a_strength, "b_strength": b_strength,
            "delta_ratio": delta_ratio,
            "compression_ratio": compression_ratio,
            "distance_to_level_pct": distance_to_level_pct,
        }
        log.info("PAPER SIGNAL %s on close %.2f - entry at next bar open",
                 direction.value, bar["close"])
        self._save()
        return "QUEUED"

    def reconcile(self, bars: list) -> None:
        """
        After a restart: replay bars newer than the last one this trader saw
        (hits stop/target that occurred during downtime), then force-close a
        position that is older than PAPER_STALE_BARS.
        """
        if self.position is None and self.pending is None:
            return
        step = settings.INTERVAL_MS
        todo = [b for b in bars if b["open_time"] > self._last_ts]
        if todo and self._last_ts > 0 and todo[0]["open_time"] - self._last_ts > step:
            log.warning("Paper reconcile: history does not cover the whole downtime; "
                        "stop/target hits before %d cannot be replayed.", todo[0]["open_time"])
        for b in todo:
            self._advance({
                "open_time": b["open_time"], "open": b.get("open", b["close"]),
                "high": b["high"], "low": b["low"], "close": b["close"],
            })
        if self.position is not None and bars:
            latest = bars[-1]
            age = (latest["open_time"] - self.position["entry_ts"]) / step
            if age > settings.PAPER_STALE_BARS:
                log.warning("Paper position is %.0f bars old after restart - closing it.", age)
                self._close(latest, latest["close"], "STALE_ON_RESTART")

    def force_close(self, reason="EOD") -> None:
        if self.position is not None and self.last_bar is not None:
            self._close(self.last_bar, self.last_bar["close"], reason)

    # ---- internals ----

    def _advance(self, bar: dict) -> None:
        if bar["open_time"] <= self._last_ts:
            return                                   # already processed this bar
        self.last_bar = bar
        self._last_ts = bar["open_time"]

        if self.pending is not None:
            p, self.pending = self.pending, None
            fresh = bar["open_time"] - p["signal_ts"] <= 3 * settings.INTERVAL_MS
            if fresh and self.position is None:
                self._open(bar, p)
            else:
                log.warning("PAPER pending %s entry dropped (missed its bar).", p["side"])

        if self.position is not None:
            self._manage(bar)
        self._history.append(bar)
        if len(self._history) > max(200, settings.TREND_ATR_WINDOW + 1):
            del self._history[:-max(200, settings.TREND_ATR_WINDOW + 1)]
        self._save()

    def _open(self, bar: dict, pend: dict) -> None:
        side = pend["side"]
        entry_quoted = bar.get("open", bar["close"])
        sign = 1 if side == "LONG" else -1
        entry = entry_quoted * (1 + sign * settings.PAPER_SLIPPAGE_PCT / 100.0)
        risk_cash = self.equity * (settings.PAPER_RISK_PER_TRADE_PCT / 100.0)
        trend_mode = getattr(settings, "ACTIVE_STRATEGY", "sweep") == "trend"
        atr = self._average_range(self._history, settings.TREND_ATR_WINDOW) if trend_mode else None
        stop_dist = (settings.TREND_ATR_MULTIPLE * atr if trend_mode and atr is not None
                     else entry * (settings.PAPER_STOP_PCT / 100.0) if not trend_mode else 0.0)
        if entry <= 0 or stop_dist <= 0 or risk_cash <= 0:
            return
        size = risk_cash / stop_dist
        stop = entry - sign * stop_dist
        target = (None if trend_mode or settings.PAPER_TARGET_R_MULTIPLE <= 0 else
                  entry + sign * stop_dist * settings.PAPER_TARGET_R_MULTIPLE)

        self.position = {
            "side": side, "entry": entry, "stop": stop, "stop_initial": stop,
            "target": target, "size": size, "risk_cash": risk_cash,
            "signal_ts": pend["signal_ts"], "signal_close": pend["signal_close"],
            "a_strength": pend.get("a_strength"), "b_strength": pend.get("b_strength"),
            "delta_ratio": pend.get("delta_ratio"),
            "compression_ratio": pend.get("compression_ratio"),
            "distance_to_level_pct": pend.get("distance_to_level_pct"),
            "entry_slippage_vs_signal": (entry - pend["signal_close"]) * sign,
            "entry_ts": bar["open_time"], "entry_reason": pend["reason"],
            "mfe": entry, "mae": entry,
            "symbol": self.symbol,
            "atr_entry": atr,
        }
        log.info("PAPER OPEN %s @ %.2f (next-bar open; signal close %.2f) "
                 "stop=%.2f target=%s size=%.4f",
                 side, entry, pend["signal_close"], stop,
                 f"{target:.2f}" if target is not None else "TRAIL", size)

    def _manage(self, bar: dict) -> None:
        p = self.position
        high, low = bar["high"], bar["low"]
        if p["side"] == "LONG":
            p["mfe"] = max(p["mfe"], high)
            p["mae"] = min(p["mae"], low)
            stop_hit, target_hit = low <= p["stop"], (p.get("target") is not None and high >= p["target"])
        else:
            p["mfe"] = min(p["mfe"], low)
            p["mae"] = max(p["mae"], high)
            stop_hit, target_hit = high >= p["stop"], (p.get("target") is not None and low <= p["target"])

        if stop_hit:
            self._close(bar, p["stop"], "STOP", ambiguous=target_hit)
            return
        if target_hit:
            self._close(bar, p["target"], "TARGET")
            return

        # still open after this bar: time stop, then breakeven (affects NEXT bar only)
        held = (bar["open_time"] - p["entry_ts"]) // settings.INTERVAL_MS + 1
        if settings.PAPER_MAX_HOLD_BARS and held >= settings.PAPER_MAX_HOLD_BARS:
            self._close(bar, bar["close"], "TIME_STOP")
            return
        be = settings.PAPER_BREAKEVEN_AT_R
        if be:
            risk = abs(p["entry"] - p.get("stop_initial", p["stop"]))
            if p["side"] == "LONG" and p["mfe"] >= p["entry"] + be * risk:
                p["stop"] = max(p["stop"], p["entry"])
            elif p["side"] == "SHORT" and p["mfe"] <= p["entry"] - be * risk:
                p["stop"] = min(p["stop"], p["entry"])

        # Trail from this completed bar for the NEXT bar only; do not use its
        # high/low to retroactively trigger a stop that was not active yet.
        if getattr(settings, "ACTIVE_STRATEGY", "sweep") == "trend":
            atr = self._average_range(self._history + [bar], settings.TREND_ATR_WINDOW)
            if atr is not None and atr > 0:
                multiple = settings.TREND_ATR_MULTIPLE
                if p["side"] == "LONG":
                    p["stop"] = max(p["stop"], high - multiple * atr)
                else:
                    p["stop"] = min(p["stop"], low + multiple * atr)

    @staticmethod
    def _average_range(bars, window):
        recent = bars[-window:]
        if len(recent) < window:
            return None
        return sum(float(b["high"]) - float(b["low"]) for b in recent) / window

    def _close(self, bar: dict, exit_price: float, exit_reason: str,
               ambiguous: bool = False) -> None:
        p = self.position
        if exit_reason in ("STOP", "TIME_STOP", "STALE_ON_RESTART"):
            sign_exit = -1 if p["side"] == "LONG" else 1
            exit_price *= 1 + sign_exit * settings.PAPER_SLIPPAGE_PCT / 100.0
        sign = 1 if p["side"] == "LONG" else -1
        gross = (exit_price - p["entry"]) * sign * p["size"]
        exit_rate = (settings.PAPER_FEE_MAKER_PCT if exit_reason == "TARGET"
                     else settings.PAPER_FEE_TAKER_PCT)
        fees = (p["entry"] * p["size"] * settings.PAPER_FEE_TAKER_PCT / 100.0
                + exit_price * p["size"] * exit_rate / 100.0)
        pnl = gross - fees
        self.equity += pnl
        trade = {
            **p,
            "exit": exit_price, "exit_ts": bar["open_time"], "exit_reason": exit_reason,
            "bars_held": (bar["open_time"] - p["entry_ts"]) // settings.INTERVAL_MS + 1,
            "ambiguous_bar": ambiguous,
            "fee_model": {"entry": "taker", "exit": "maker" if exit_reason == "TARGET" else "taker"},
            "gross_pnl": gross, "fees": fees, "pnl": pnl,
            "r_multiple": pnl / p["risk_cash"] if p["risk_cash"] else 0.0,
            "equity_after": self.equity,
        }
        self.trades.append(trade)
        self._log(trade)
        log.info("PAPER CLOSE %s @ %.2f (%s%s) pnl=%+.2f (fees %.2f, %+.2fR) equity=%.2f",
                 p["side"], exit_price, exit_reason,
                 ", both stop+target touched -> stop assumed" if ambiguous else "",
                 pnl, fees, trade["r_multiple"], self.equity)
        self.position = None
        self._save()

    def _log(self, trade: dict) -> None:
        if not self.log_trades:
            return
        folder = os.path.dirname(settings.PAPER_TRADES_LOG)
        if folder:
            os.makedirs(folder, exist_ok=True)
        with open(settings.PAPER_TRADES_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(trade) + "\n")
