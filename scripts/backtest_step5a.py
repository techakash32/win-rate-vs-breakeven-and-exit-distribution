# scripts/backtest_step5a.py
"""
Backtest replay of the live gate + the live PaperTrader.

It reuses engine.decision.Decider (the exact gate logic the live pipeline runs)
and execution.paper_trader.PaperTrader (the exact entry/exit/fee logic), so a
backtest trade is a live trade replayed on history. Bucket C is not included
(no historical Deribit GEX).

Examples
  python -u -m scripts.backtest_step5a --vp approx --days 30
  python -u -m scripts.backtest_step5a --vp real --days 14 --end-date 2026-10-26
  python -u -m scripts.backtest_step5a --vp real --days 14 --grid
  python -u -m scripts.backtest_step5a --vp real --days 14 --flip     # diagnostic: inverted signals
  python -u -m scripts.backtest_step5a --vp real --days 14 --end-date 2026-10-26 \
          --stop 0.6 --target-r 1.5 --hold 90 --cooldown 15 --tag wide   # no settings.py edits needed

NEW in this version
  * avg_gross_r / avg_fee_r (+ bootstrap CI of gross R): separates "no entry edge"
    from "edge eaten by fees".
  * fee_to_stop_pct, avg_r_by_exit, and a plain-text "diagnosis" for ALL and TEST.
  * CLI overrides --stop --target-r --hold --cooldown --vp-window --min-strength
    so you can test exits/cooldown without editing settings.py.
  * --tag NAME: outputs go to summary_NAME.json / trades_NAME.jsonl, so runs never
    overwrite each other (flip runs get "_flipped" automatically).

Missing archive days (HTTP 404) are skipped. Bars on either side of a gap are
never joined: each contiguous stretch is replayed on its own.

--vp approx spreads each bar's volume uniformly over its range, which widens the
value area, so Bucket A behaves differently from live. Fine for a first look;
confirm with --vp real before trusting anything.
"""
import argparse
import json
import logging
import math
import os
import random
import statistics
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from itertools import product

from config import settings
from data import historical
from engine.decision import Decider
from execution.paper_trader import PaperTrader
from gate.buckets import Dir

log = logging.getLogger("backtest")
STEP = settings.INTERVAL_MS


# ---------------------------------------------------------------- helpers

@contextmanager
def override(**kw):
    """Temporarily change settings values (restored on exit)."""
    missing = object()
    old = {k: getattr(settings, k, missing) for k in kw}
    for k, v in kw.items():
        setattr(settings, k, v)
    try:
        yield
    finally:
        for k, v in old.items():
            if v is missing:
                delattr(settings, k)
            else:
                setattr(settings, k, v)


def gate_params(vp_window: int, min_strength: float) -> dict:
    threshold_name = ("AGGR_RATIO_THRESHOLD_SWEEP" if settings.ACTIVE_STRATEGY == "sweep"
                      else "AGGR_RATIO_THRESHOLD")
    return dict(
        VP_WINDOW_BARS=vp_window, WARMUP_BARS=vp_window,
        MIN_AGGRESSION_STRENGTH=min_strength,
        **{threshold_name: 0.5 + min_strength / 4.0},
    )


def exit_params(stop: float, target_r: float, hold: int) -> dict:
    return dict(PAPER_STOP_PCT=stop, PAPER_TARGET_R_MULTIPLE=target_r, PAPER_MAX_HOLD_BARS=hold)


def split_segments(bars: list, max_gap_steps: int = 2) -> list:
    """Contiguous runs of bars. A hole bigger than max_gap_steps starts a new run."""
    if not bars:
        return []
    segs = [[bars[0]]]
    for prev, b in zip(bars, bars[1:]):
        if b["open_time"] - prev["open_time"] > max_gap_steps * STEP:
            segs.append([])
        segs[-1].append(b)
    return segs


def run_gate(segments: list) -> list:
    """Gate decision for every bar. Returns, per segment, [(allowed, Dir|None)]."""
    hist_len = max(settings.VP_WINDOW_BARS, settings.AGGR_WINDOW_BARS,
                   settings.ABSORPTION_DELTA_BASELINE_BARS,
                   settings.ABSORPTION_RANGE_BASELINE_BARS + 3,
                   settings.TREND_DONCHIAN_LOOKBACK + 1,
                   settings.TREND_ATR_WINDOW) + 1
    out = []
    for seg in segments:
        decider = Decider()                    # fresh warmup + cooldown per stretch
        sigs = []
        for i, bar in enumerate(seg):
            if bar.get("synthetic"):
                sigs.append((False, None, None, None, None, None, None))
                continue
            d = decider.decide(seg[max(0, i - hist_len + 1): i + 1], bar["close"])
            sigs.append((d.allowed, d.direction, d.a_strength, d.b_strength,
                         d.delta_ratio, d.compression_ratio, d.distance_to_level_pct)
                        if not d.warmup else (False, None, None, None, None, None, None))
        out.append(sigs)
    return out


def flip_signals(sigs: list) -> list:
    inv = {Dir.LONG: Dir.SHORT, Dir.SHORT: Dir.LONG}
    return [[(x[0], inv.get(x[1], x[1]), *x[2:]) for x in seg] for seg in sigs]


def simulate(segments: list, sigs: list) -> list:
    """Replay signals through one independent trader per symbol/time stretch."""
    trades = []
    sleeve_equity = settings.PAPER_START_EQUITY / max(1, len(settings.SYMBOLS))
    equity_by_symbol = {symbol: sleeve_equity for symbol in settings.SYMBOLS}
    for seg, seg_sigs in zip(segments, sigs):
        symbol = seg[0].get("symbol") if seg else None
        seed = equity_by_symbol.get(symbol, sleeve_equity)
        trader = PaperTrader(persist=False, log_trades=False,
                             initial_equity=seed, symbol=symbol)
        assert math.isclose(trader.equity, seed), (
            f"PaperTrader sizing base mismatch for {symbol}: "
            f"{trader.equity} != requested sleeve equity {seed}"
        )
        for bar, signal in zip(seg, seg_sigs):
            if bar.get("synthetic"):
                continue  # synthetic OHLC must not create entries or simulated fills/exits
            allowed, direction = signal[:2]
            metadata = list(signal[2:7]) + [None] * max(0, 5 - len(signal[2:7]))
            trader.on_bar(bar, allowed, direction, "bt", *metadata)
        trades.extend(trader.trades)
        if symbol is not None:
            equity_by_symbol[symbol] = trader.equity
    trades.sort(key=lambda t: t["exit_ts"])
    return trades


def count_signals(segments: list, sigs: list, lo_ts=None, hi_ts=None) -> int:
    n = 0
    for seg, seg_sigs in zip(segments, sigs):
        for bar, signal in zip(seg, seg_sigs):
            allowed, _d = signal[:2]
            if allowed and (lo_ts is None or bar["open_time"] >= lo_ts) \
                    and (hi_ts is None or bar["open_time"] < hi_ts):
                n += 1
    return n


def _excursions_r(t: dict):
    risk = abs(t["entry"] - t.get("stop_initial", t["stop"]))
    if risk <= 0:
        return None
    if t["side"] == "LONG":
        return (t["mfe"] - t["entry"]) / risk, (t["entry"] - t["mae"]) / risk
    return (t["entry"] - t["mfe"]) / risk, (t["mae"] - t["entry"]) / risk


def _boot_ci(values: list, seed: int = 1, reps: int = 2000):
    """95% bootstrap CI of the mean. Returns [lo, hi] or None if too few values."""
    n = len(values)
    if n < 10:
        return None
    rng = random.Random(seed)
    means = sorted(sum(rng.choices(values, k=n)) / n for _ in range(reps))
    lo = means[int(reps * 0.025)]
    hi = means[int(reps * 0.975) - 1]
    return [round(lo, 3), round(hi, 3)]


def diagnose(m: dict) -> str:
    """Plain-text reading of net vs gross R. Facts only, no prediction."""
    if not m or m.get("trades", 0) < 30:
        return "too few trades (<30) to say anything"
    ci = m.get("avg_gross_r_95ci_bootstrap")
    if ci is None:
        return "too few trades to bootstrap"
    lo, hi = ci
    g, net, fee = m["avg_gross_r"], m["avg_r"], m["avg_fee_r"]
    if hi < 0:
        return (f"gross R {g:+.3f} is significantly NEGATIVE (CI {lo:+.3f}..{hi:+.3f}): "
                f"entries lose before fees. Fees add {fee:.3f}R/trade on top.")
    if lo > 0 and net <= 0:
        return (f"gross R {g:+.3f} is significantly positive (CI {lo:+.3f}..{hi:+.3f}) but fees "
                f"({fee:.3f}R/trade) eat it: net {net:+.3f}R. Cut trade count or fee/stop ratio.")
    if lo > 0:
        return f"gross {g:+.3f}R, net {net:+.3f}R after {fee:.3f}R fees: positive. Confirm out-of-sample."
    return (f"gross R {g:+.3f} (CI {lo:+.3f}..{hi:+.3f}) is indistinguishable from zero: no evidence "
            f"of entry edge. Fees of {fee:.3f}R/trade decide the net result ({net:+.3f}R).")


def stratify(trades: list, field: str, bins: list) -> dict:
    out = {}
    for lo, hi in zip(bins, bins[1:]):
        bucket = [t for t in trades if t.get(field) is not None and lo <= t[field] < hi]
        if bucket:
            rs = [t["r_multiple"] for t in bucket]
            out[f"{lo:.2f}-{hi:.2f}"] = {"n": len(rs), "win_rate": round(sum(r > 0 for r in rs) / len(rs) * 100, 1),
                                         "avg_r": round(sum(rs) / len(rs), 3)}
    return out


def signal_decay(trades: list) -> dict:
    """Summarize trade duration for winners versus non-winners."""
    wins = [t["bars_held"] for t in trades if t.get("r_multiple", 0) > 0 and t.get("bars_held") is not None]
    losses = [t["bars_held"] for t in trades if t.get("r_multiple", 0) <= 0 and t.get("bars_held") is not None]
    return {
        "avg_win_bars": round(sum(wins) / len(wins), 1) if wins else None,
        "avg_loss_bars": round(sum(losses) / len(losses), 1) if losses else None,
        "pct_wins_before_30_bars": round(sum(b <= 30 for b in wins) / len(wins) * 100, 1) if wins else None,
    }


def bucket_a_breakdown(trades: list) -> dict:
    """Summarize Bucket A sweep trades by reversal direction."""
    result = {"n": len(trades), "by_direction": {}}
    for side in ("LONG", "SHORT"):
        bucket = [t for t in trades if t.get("side") == side]
        if not bucket:
            continue
        rs = [t["r_multiple"] for t in bucket]
        gross_rs = [t.get("gross_pnl", 0) / t.get("risk_cash", 1) for t in bucket]
        result["by_direction"][side] = {
            "n": len(bucket),
            "win_rate": round(sum(r > 0 for r in rs) / len(rs) * 100, 1),
            "avg_r": round(sum(rs) / len(rs), 3),
            "avg_gross_r": round(sum(gross_rs) / len(gross_rs), 3),
        }
    return result


# Backwards-compatible name for the old summary API.
absorption_breakdown = bucket_a_breakdown


def regime_analysis(bars: list, trades: list) -> dict:
    import datetime as dt
    by_atr = {k: [] for k in ("quiet", "normal", "volatile")}
    by_hour = {k: [] for k in ("asia_00_08", "london_08_16", "ny_16_00")}
    by_trend = {k: [] for k in ("bull", "bear")}
    by_symbol = {}
    for b in sorted(bars, key=lambda item: item["open_time"]):
        by_symbol.setdefault(b.get("symbol"), []).append(b)
    indexed = {(sym, b["open_time"]): i for sym, series in by_symbol.items()
               for i, b in enumerate(series)}
    closes = {sym: [b["close"] for b in series] for sym, series in by_symbol.items()}
    for t in trades:
        hour = dt.datetime.fromtimestamp(t["entry_ts"] / 1000, dt.timezone.utc).hour
        by_hour[("asia_00_08" if hour < 8 else "london_08_16" if hour < 16 else "ny_16_00")].append(t)
        sym = t.get("symbol")
        i = indexed.get((sym, t["entry_ts"]))
        if i is None: continue
        bar = by_symbol[sym][i]
        pct = (bar["high"] - bar["low"]) / bar["close"] * 100 if bar["close"] else 0
        by_atr[("quiet" if pct < .10 else "normal" if pct < .25 else "volatile")].append(t)
        sym_closes = closes.get(sym, [])
        if i >= 199 and len(sym_closes) > i:
            ema = sum(sym_closes[i-199:i+1]) / 200
            by_trend["bull" if sym_closes[i] > ema else "bear"].append(t)
    def summary(groups):
        return {k: {"n":len(v), "win_rate":round(sum(x["r_multiple"] > 0 for x in v)/len(v)*100,1), "avg_r":round(sum(x["r_multiple"] for x in v)/len(v),3)} for k,v in groups.items() if v}
    return {"by_atr":summary(by_atr), "by_hour":summary(by_hour), "by_trend":summary(by_trend)}


def walk_forward(segments: list, folds: int = 4) -> dict:
    """Evaluate calendar-time folds across symbols, preserving each series' warmup history."""
    bars = sorted((b for seg in segments for b in seg), key=lambda b: b["open_time"])
    if len(bars) < folds + 1:
        return {"folds": [], "positive_folds": 0, "total_folds": 0,
                "avg_across_folds": 0, "is_robust": False}
    start_ts, end_ts = bars[0]["open_time"], bars[-1]["open_time"] + STEP
    span = end_ts - start_ts
    results = []
    for k in range(folds):
        lo = start_ts + span * (k + 1) // (folds + 1)
        hi = start_ts + span * (k + 2) // (folds + 1)
        test = [b for b in bars if lo <= b["open_time"] < hi]
        if not test:
            continue
        parts = []
        for seg in segments:
            prefix = [b for b in seg if b["open_time"] < hi]
            if prefix:
                parts.extend(split_segments(prefix))
        sigs = run_gate(parts)
        trades = simulate(parts, sigs)
        fold_trades = [t for t in trades if lo <= t["entry_ts"] < hi]
        fold_days = (hi - lo) / 86_400_000
        m = metrics(fold_trades, fold_days, 0, len(test))
        results.append({"fold": k + 1, "test_start": lo, "test_end": hi,
                        "trades": m.get("trades", 0), "win_rate": m.get("win_rate_pct"),
                        "avg_r": m.get("avg_r"),
                        "ci_lo": (m.get("avg_r_95ci_bootstrap") or [None, None])[0],
                        "ci_hi": (m.get("avg_r_95ci_bootstrap") or [None, None])[1]})
    pos=sum(1 for r in results if (r["avg_r"] or 0)>0)
    return {"folds":results,"positive_folds":pos,"total_folds":len(results),"avg_across_folds":round(sum((r["avg_r"] or 0) for r in results)/len(results),3) if results else 0,"is_robust":pos>=max(3,len(results)-1)}


def permutation_test(segments: list, signals: list, n_perm: int = 500, seed: int = 42) -> dict:
    rng = random.Random(seed)
    n_signals = sum(1 for seg in signals for x in seg if x[0])
    if n_signals < 30: return {"error":"not enough signals for permutation"}
    real = simulate(segments, signals); real_avg = sum(t["r_multiple"] for t in real)/len(real) if real else 0
    pool=[(si,i) for si,seg in enumerate(segments) for i in range(settings.WARMUP_BARS,len(seg)-1)]
    if not pool: return {"error":"no eligible bars for permutation"}
    avgs=[]
    for _ in range(n_perm):
        chosen=set(rng.sample(pool,min(n_signals,len(pool))))
        random_sigs=[]
        for si,seg in enumerate(segments):
            random_sigs.append([((si,i) in chosen,rng.choice((Dir.LONG,Dir.SHORT)) if (si,i) in chosen else None,None,None) for i in range(len(seg))])
        tr=simulate(segments,random_sigs)
        if tr: avgs.append(sum(t["r_multiple"] for t in tr)/len(tr))
    avgs.sort(); n=len(avgs); pct=sum(x<real_avg for x in avgs)/n*100 if n else 0
    return {"real_avg_r":round(real_avg,3),"shuffled_p5":round(avgs[int(.05*n)],3) if n else None,"shuffled_p50":round(avgs[int(.5*n)],3) if n else None,"shuffled_p95":round(avgs[min(n-1,int(.95*n))],3) if n else None,"real_percentile":round(pct,1),"edge_is_real":pct>95}


def metrics(trades: list, n_days: float, n_signals: int, n_bars: int, rng_seed=1) -> dict:
    n = len(trades)
    out = {"trades": n, "signals": n_signals,
           "signals_per_day": round(n_signals / n_days, 2) if n_days else None,
           "trades_per_day": round(n / n_days, 2) if n_days else None,
           "signal_frequency_pct": round(n_signals / n_bars * 100, 3) if n_bars else None}
    if n == 0:
        return out
    r = [t["r_multiple"] for t in trades]
    wins = [x for x in r if x > 0]
    losses = [x for x in r if x <= 0]
    avg = sum(r) / n
    avg_w = sum(wins) / len(wins) if wins else 0.0
    avg_l = sum(losses) / len(losses) if losses else 0.0
    be = abs(avg_l) / (avg_w + abs(avg_l)) * 100 if wins and losses else None

    ci = _boot_ci(r, rng_seed)

    # gross (before fees) vs fees, both in R
    gross_r = [t["gross_pnl"] / t["risk_cash"] for t in trades if t.get("risk_cash")]
    fee_r = [t["fees"] / t["risk_cash"] for t in trades if t.get("risk_cash")]
    gci = _boot_ci(gross_r, rng_seed + 1)

    # equity compounded from R: each trade risks PAPER_RISK_PER_TRADE_PCT of equity
    risk_frac = settings.PAPER_RISK_PER_TRADE_PCT / 100.0
    eq, peak, max_dd, streak, max_streak = settings.PAPER_START_EQUITY, 0.0, 0.0, 0, 0
    peak = eq
    for x in r:
        eq *= 1.0 + risk_frac * x
        peak = max(peak, eq)
        max_dd = max(max_dd, (peak - eq) / peak)
        streak = streak + 1 if x <= 0 else 0
        max_streak = max(max_streak, streak)

    exits: dict = {}
    by_exit: dict = {}
    for t in trades:
        exits[t["exit_reason"]] = exits.get(t["exit_reason"], 0) + 1
        by_exit.setdefault(t["exit_reason"], []).append(t["r_multiple"])
    avg_r_by_exit = {k: round(sum(v) / len(v), 3) for k, v in by_exit.items()}
    by_side = {}
    for side in ("LONG", "SHORT"):
        rs = [t["r_multiple"] for t in trades if t["side"] == side]
        if rs:
            by_side[side] = {"n": len(rs), "avg_r": round(sum(rs) / len(rs), 3)}
    exc = [e for e in (_excursions_r(t) for t in trades) if e]
    gross_w, gross_l = sum(wins), abs(sum(losses))

    out.update({
        "win_rate_pct": round(len(wins) / n * 100, 1),
        "breakeven_win_rate_pct": round(be, 1) if be is not None else None,
        "avg_r": round(avg, 3), "median_r": round(statistics.median(r), 3),
        "avg_win_r": round(avg_w, 3), "avg_loss_r": round(avg_l, 3),
        "total_r": round(sum(r), 2),
        "avg_r_95ci_bootstrap": ci,
        "edge_distinguishable_from_zero": (ci[0] > 0) if ci else None,
        "avg_gross_r": round(sum(gross_r) / len(gross_r), 3) if gross_r else None,
        "avg_fee_r": round(sum(fee_r) / len(fee_r), 3) if fee_r else None,
        "avg_gross_r_95ci_bootstrap": gci,
        "fee_to_stop_pct": (None if settings.ACTIVE_STRATEGY == "trend" else
                            round((2 * settings.PAPER_FEE_TAKER_PCT) / settings.PAPER_STOP_PCT * 100, 1)
                            if settings.PAPER_STOP_PCT else None),
        "profit_factor": round(gross_w / gross_l, 2) if gross_l else None,
        "exit_distribution": exits,
        "avg_r_by_exit": avg_r_by_exit,
        "avg_bars_held": round(sum((t["exit_ts"] - t["entry_ts"]) / STEP + 1 for t in trades) / n, 1),
        "avg_mfe_r": round(sum(e[0] for e in exc) / len(exc), 2) if exc else None,
        "avg_mae_r": round(sum(e[1] for e in exc) / len(exc), 2) if exc else None,
        "pct_trades_reaching_+1R": round(sum(1 for e in exc if e[0] >= 1.0) / len(exc) * 100, 1) if exc else None,
        "ambiguous_bars": sum(1 for t in trades if t.get("ambiguous_bar")),
        "by_side": by_side,
        "max_drawdown_pct": round(max_dd * 100, 2),
        "max_consecutive_losses": max_streak,
        "final_equity": round(eq, 2),
    })
    return out


def apply_portfolio_equity(m: dict, trades: list, initial_equity=None) -> dict:
    """Replace single-stream compounding with realized cash PnL on basket capital."""
    equity = peak = float(initial_equity if initial_equity is not None else settings.PAPER_START_EQUITY)
    max_dd = 0.0
    by_ts = {}
    for trade in trades:
        by_ts[trade["exit_ts"]] = by_ts.get(trade["exit_ts"], 0.0) + trade.get("pnl", 0.0)
    for timestamp in sorted(by_ts):
        equity += by_ts[timestamp]
        peak = max(peak, equity)
        if peak > 0:
            max_dd = max(max_dd, (peak - equity) / peak)
    if m:
        m["final_equity"] = round(equity, 2)
        m["max_drawdown_pct"] = round(max_dd * 100, 2)
    return m


def random_baseline(segments: list, n_signals: int, reps: int, seed: int = 7) -> dict:
    """Same exits/fees, but entries on random bars in random directions."""
    if n_signals == 0:
        return {}
    rng = random.Random(seed)
    first = settings.WARMUP_BARS
    pool = [(si, i) for si, seg in enumerate(segments) for i in range(first, len(seg) - 1)]
    if not pool:
        return {}
    k = min(n_signals, len(pool))
    avgs = []
    for _ in range(reps):
        chosen = set(rng.sample(pool, k))
        sigs = [[((si, i) in chosen, rng.choice((Dir.LONG, Dir.SHORT)) if (si, i) in chosen else None)
                 for i in range(len(seg))] for si, seg in enumerate(segments)]
        tr = simulate(segments, sigs)
        if tr:
            avgs.append(sum(t["r_multiple"] for t in tr) / len(tr))
    avgs.sort()
    if not avgs:
        return {}
    return {"reps": len(avgs), "mean_avg_r": round(sum(avgs) / len(avgs), 3),
            "p5": round(avgs[int(0.05 * len(avgs))], 3),
            "p95": round(avgs[min(len(avgs) - 1, int(0.95 * len(avgs)))], 3),
            "_sorted": avgs}


def move_stats(segments: list, stop_pct: float, target_r: float) -> dict:
    """Unconditional excursion stats: how often does price even travel the target distance?"""
    if target_r <= 0:
        return {"target_pct": None, "note": "No fixed target; trend exits use an ATR trailing stop."}
    tgt = stop_pct * target_r
    out = {"target_pct": round(tgt, 3)}
    for h in (30, 60):
        both = []
        for seg in segments:
            for i in range(len(seg) - h - 1):
                o = seg[i + 1]["open"]
                if o <= 0:
                    continue
                win = seg[i + 1: i + 1 + h]
                both.append((max(b["high"] for b in win) - o) / o * 100)
                both.append((o - min(b["low"] for b in win)) / o * 100)
        if not both:
            continue
        both.sort()
        q = lambda p: round(both[int(p * (len(both) - 1))], 3)
        out[f"{h}bars"] = {
            "max_favorable_move_pct": {"p50": q(.5), "p75": q(.75), "p90": q(.9)},
            "P(move >= target)": round(sum(1 for x in both if x >= tgt) / len(both), 3),
        }
    return out


def split_trades(trades: list, cut_ts: int):
    return ([t for t in trades if t["entry_ts"] < cut_ts],
            [t for t in trades if t["entry_ts"] >= cut_ts])


def validate_bars(bars: list, interval: str, requested_days: list, missing_days: list) -> dict:
    """Fail fast on wrong bar density or unexplained timestamp gaps."""
    intervals = historical.INTERVAL_MS_MAP
    if interval not in intervals:
        raise SystemExit(f"Unsupported interval {interval!r}; cannot validate data.")
    step = intervals[interval]
    expected_ppd = 86_400_000 / step
    missing = {d.isoformat() if hasattr(d, "isoformat") else str(d) for d in missing_days}
    if not bars:
        raise SystemExit(f"DATA MISMATCH: no bars loaded for {interval} backtest.")
    first_ts = min(bar["open_time"] for bar in bars)
    last_ts = max(bar["open_time"] for bar in bars)
    first_day = datetime.fromtimestamp(first_ts / 1000, timezone.utc).date()
    last_day = datetime.fromtimestamp(last_ts / 1000, timezone.utc).date()
    observed_days = (last_day - first_day).days + 1
    bars_per_day = len(bars) / max(1, observed_days)
    requested_count = max(1, len(requested_days))
    coverage_pct = observed_days / requested_count * 100
    consistent = abs(bars_per_day - expected_ppd) <= expected_ppd * 0.05
    if not consistent:
        raise SystemExit(
            f"DATA MISMATCH: interval={interval} expects about {expected_ppd:.0f} bars/day "
            f"over {observed_days} observed days, got {bars_per_day:.2f}. "
            "Refusing to backtest inconsistent data."
        )
    gaps = []
    known_gaps = 0
    for prev, current in zip(bars, bars[1:]):
        gap_ms = current["open_time"] - prev["open_time"]
        if gap_ms <= 2 * step:
            continue
        prev_day = datetime.fromtimestamp(prev["open_time"] / 1000, timezone.utc).date()
        curr_day = datetime.fromtimestamp(current["open_time"] / 1000, timezone.utc).date()
        intervening = {(prev_day + timedelta(days=n)).isoformat()
                       for n in range(1, (curr_day - prev_day).days)}
        if intervening and intervening.issubset(missing):
            known_gaps += 1
        else:
            gaps.append({"at": current["open_time"], "intervals": round(gap_ms / step, 2)})
    if gaps:
        first = gaps[0]
        raise SystemExit(f"DATA GAP: {first['intervals']}× interval at {first['at']}; refusing to backtest.")
    synthetic_bars = sum(bool(b.get("synthetic")) for b in bars)
    synthetic_days = sorted({
        datetime.fromtimestamp(b["open_time"] / 1000, timezone.utc).date().isoformat()
        for b in bars if b.get("synthetic")
    })
    return {"interval": interval, "interval_consistent": True,
            "expected_bars_per_day": round(expected_ppd, 1),
            "bars_per_day": round(bars_per_day, 2),
            "observed_days": observed_days,
            "first_day": first_day.isoformat(),
            "last_day": last_day.isoformat(),
            "requested_days": len(requested_days),
            "requested_window_coverage_pct": round(coverage_pct, 1),
            "gaps_count": known_gaps,
            "synthetic_bars": synthetic_bars,
            "synthetic_days": synthetic_days,
            "known_missing_days": sorted(missing)}


def go_no_go_gate(result: dict) -> dict:
    """Require positive, statistically supported net edge before authorizing Step 5b."""
    all_m = result.get("all", {})
    test_m = result.get("test", {})
    wf = result.get("walk_forward", {})
    perm = result.get("permutation_test", {})
    gci = all_m.get("avg_gross_r_95ci_bootstrap")
    mfe, mae = all_m.get("avg_mfe_r"), all_m.get("avg_mae_r")
    checks = {
        "data_interval_consistent": result.get("data_validation", {}).get("interval_consistent") is True,
        "no_synthetic_bars": result.get("data_validation", {}).get("synthetic_bars", 0) == 0,
        "sample_adequate": all_m.get("trades", 0) >= 250,
        "gross_edge_positive": bool(
            all_m.get("avg_gross_r") is not None and all_m["avg_gross_r"] > 0
            and gci and gci[0] is not None and gci[0] > 0
        ),
        "gross_ci_excludes_zero": bool(gci and gci[0] is not None and gci[0] > 0),
        "net_edge_positive": all_m.get("avg_r") is not None and all_m["avg_r"] > 0.0,
        "walk_forward_robust": wf.get("positive_folds", 0) >= 3,
        "permutation_significant": perm.get("edge_is_real", False) is True,
        "test_partition_positive": test_m.get("avg_r") is not None and test_m["avg_r"] > 0.0,
        "profit_factor_adequate": all_m.get("profit_factor") is not None and all_m["profit_factor"] >= 1.20,
        "drawdown_acceptable": all_m.get("max_drawdown_pct") is not None and all_m["max_drawdown_pct"] < 15.0,
        "mfe_mae_gap": (mfe is not None and mae is not None and mfe - mae > 0.10),
    }
    passed = sum(checks.values())
    synthetic_present = result.get("data_validation", {}).get("synthetic_bars", 0) > 0
    required_checks = (
        "data_interval_consistent", "no_synthetic_bars", "sample_adequate",
        "gross_edge_positive", "net_edge_positive", "walk_forward_robust",
        "permutation_significant", "test_partition_positive",
        "profit_factor_adequate", "drawdown_acceptable",
    )
    authorized = all(checks[name] for name in required_checks) and not synthetic_present
    verdict = ("NO-GO — synthetic bars present; rerun on observed data before Step 5b" if synthetic_present else
               "GO — eligible for Step 5b paper trading" if authorized else
               "NO-GO — required evidence criteria failed; do not proceed to Step 5b")
    print("\n" + "=" * 60)
    print(f"GO/NO-GO GATE: {passed}/{len(checks)} checks passed")
    print("=" * 60)
    for name, ok in checks.items():
        print(f"  {'PASS' if ok else 'FAIL'} {name}")
    print(verdict)
    print("=" * 60)
    return {"checks": checks, "passed": passed, "total": len(checks),
            "required_checks": list(required_checks),
            "authorized": authorized, "verdict": verdict}


# ---------------------------------------------------------------- modes

def single_run(segments, days, split, reps, flip=False):
    flat = sorted((b for seg in segments for b in seg), key=lambda b: b["open_time"])
    n_bars = len(flat)
    cut = flat[int(n_bars * split)]["open_time"]
    sigs = run_gate(segments)
    if flip:
        sigs = flip_signals(sigs)
    trades = simulate(segments, sigs)
    train, test = split_trades(trades, cut)

    n_all = count_signals(segments, sigs)
    n_train = count_signals(segments, sigs, hi_ts=cut)
    n_test = count_signals(segments, sigs, lo_ts=cut)
    bars_train = sum(1 for b in flat if b["open_time"] < cut)

    m_all = metrics(trades, days, n_all, n_bars)
    m_train = metrics(train, days * split, n_train, bars_train)
    m_test = metrics(test, days * (1 - split), n_test, n_bars - bars_train)
    apply_portfolio_equity(m_all, trades)
    apply_portfolio_equity(m_train, train)
    apply_portfolio_equity(m_test, test)
    per_symbol = {}
    symbol_count = max(1, len(settings.SYMBOLS))
    for symbol in settings.SYMBOLS:
        symbol_bars = [bar for bar in flat if bar.get("symbol") == symbol]
        symbol_trades = [trade for trade in trades if trade.get("symbol") == symbol]
        if symbol_bars:
            first = datetime.fromtimestamp(min(b["open_time"] for b in symbol_bars) / 1000,
                                           timezone.utc).date()
            last = datetime.fromtimestamp(max(b["open_time"] for b in symbol_bars) / 1000,
                                          timezone.utc).date()
            symbol_days = max(1, (last - first).days + 1)
        else:
            symbol_days = 0
        symbol_signals = sum(
            1 for seg, seg_sigs in zip(segments, sigs)
            for bar, signal in zip(seg, seg_sigs)
            if bar.get("symbol") == symbol and signal[0]
        )
        sm = metrics(symbol_trades, symbol_days, symbol_signals, len(symbol_bars),
                     rng_seed=sum(ord(char) for char in symbol))
        sm["observed_days"] = symbol_days
        apply_portfolio_equity(sm, symbol_trades, settings.PAPER_START_EQUITY / symbol_count)
        per_symbol[symbol] = sm

    res = {
        "config": {"strategy": settings.ACTIVE_STRATEGY,
                   "bucket_a_detector": ("donchian_breakout" if settings.ACTIVE_STRATEGY == "trend"
                                          else "funding_extreme_fade" if settings.ACTIVE_STRATEGY == "funding_fade"
                                          else "liquidity_sweep"),
                   "symbols": list(settings.SYMBOLS),
                   "sweep_long_only": settings.SWEEP_LONG_ONLY,
                   "interval": settings.INTERVAL,
                   "vp_window": settings.VP_WINDOW_BARS,
                   "b_ratio_threshold": (settings.AGGR_RATIO_THRESHOLD_SWEEP
                                         if settings.ACTIVE_STRATEGY == "sweep"
                                         else settings.AGGR_RATIO_THRESHOLD),
                   "b_min_strength": (None if settings.ACTIVE_STRATEGY == "sweep"
                                      else settings.MIN_AGGRESSION_STRENGTH),
                   "stop_pct": settings.PAPER_STOP_PCT, "target_r": settings.PAPER_TARGET_R_MULTIPLE,
                   "max_hold_bars": settings.PAPER_MAX_HOLD_BARS, "cooldown": settings.COOLDOWN_BARS,
                   "risk_per_trade_pct": settings.PAPER_RISK_PER_TRADE_PCT,
                   "slippage_pct": settings.PAPER_SLIPPAGE_PCT,
                   "sweep_lookback_bars": settings.SWEEP_LOOKBACK_BARS,
                   "sweep_min_penetration_pct": settings.SWEEP_MIN_PENETRATION_PCT,
                   "trend_donchian_lookback": settings.TREND_DONCHIAN_LOOKBACK,
                   "trend_atr_window": settings.TREND_ATR_WINDOW,
                   "trend_atr_multiple": settings.TREND_ATR_MULTIPLE,
                   "trend_london_only": settings.TREND_LONDON_ONLY,
                   "stop_model": "2x_ATR_trailing" if settings.ACTIVE_STRATEGY == "trend" else "fixed_percent",
                   "target_model": "none_trailing_exit" if settings.ACTIVE_STRATEGY == "trend" else "fixed_R",
                   "bucket_b_enabled": settings.BUCKET_B_ENABLED,
                   "fee_taker_pct": settings.PAPER_FEE_TAKER_PCT,
                   "fee_maker_pct": settings.PAPER_FEE_MAKER_PCT},
        "flipped_signals": flip,
        "bars": n_bars, "segments": len(segments), "days": round(days, 1),
        "diagnosis": {"all": diagnose(m_all), "test": diagnose(m_test)},
        "all": m_all,
        "train": m_train,
        "test": m_test,
        "per_symbol": per_symbol,
        "move_stats": move_stats(segments, settings.PAPER_STOP_PCT, settings.PAPER_TARGET_R_MULTIPLE),
    }
    res["all"]["by_a_strength"] = stratify(trades, "a_strength", [0.0, 0.3, 0.5, 0.7, 1.01])
    res["all"]["by_b_strength"] = stratify(trades, "b_strength", [0.4, 0.5, 0.6, 0.7, 1.01])
    res["all"]["signal_decay"] = signal_decay(trades)
    res["all"]["bucket_a_breakdown"] = bucket_a_breakdown(trades)
    res["regimes"] = regime_analysis(flat, trades)
    if reps and n_all:
        base = random_baseline(segments, n_all, reps)
        if base:
            s = base.pop("_sorted")
            gate_avg = res["all"].get("avg_r")
            if gate_avg is not None:
                base["gate_avg_r_percentile_vs_random"] = round(
                    sum(1 for x in s if x < gate_avg) / len(s) * 100, 1)
            res["random_entry_baseline"] = base
    if flip:
        res["flip_caveat"] = ("Inverting signals AFTER seeing the result is a diagnostic, not a "
                              "strategy: it is selected with hindsight, and fees still apply to "
                              "both sides. Only the TEST partition counts.")
    return res, trades


def grid_run(segments, days, split):
    flat = sorted((b for seg in segments for b in seg), key=lambda b: b["open_time"])
    n_bars = len(flat)
    cut = flat[int(n_bars * split)]["open_time"]
    bars_train = sum(1 for b in flat if b["open_time"] < cut)
    gate_grid = list(product((20, 30, 60), (0.35, 0.40, 0.50)))
    exit_grid = list(product((0.20, 0.25, 0.30), (2.0, 3.0), (30, 60)))
    rows = []
    for vp, ms in gate_grid:
        with override(**gate_params(vp, ms)):
            sigs = run_gate(segments)
            n_train = count_signals(segments, sigs, hi_ts=cut)
            n_test = count_signals(segments, sigs, lo_ts=cut)
            for stop, tr_r, hold in exit_grid:
                with override(**exit_params(stop, tr_r, hold)):
                    trades = simulate(segments, sigs)
                train, test = split_trades(trades, cut)
                if len(train) < 30:
                    continue
                rows.append({
                    "vp_window": vp, "min_strength": ms, "stop": stop, "target_r": tr_r, "hold": hold,
                    "train": metrics(train, days * split, n_train, bars_train),
                    "test": metrics(test, days * (1 - split), n_test, n_bars - bars_train),
                    "user_proposed": (vp == 30 and ms == 0.40 and stop == 0.25 and tr_r == 3.0 and hold == 60),
                    "current_default": (vp == 30 and ms == 0.40 and stop == 0.30 and tr_r == 2.0 and hold == 30),
                })
        log.info("grid: gate config vp=%d strength=%.2f done", vp, ms)
    rows.sort(key=lambda r: r["train"]["avg_r"], reverse=True)
    return {"combos_tried": len(gate_grid) * len(exit_grid), "combos_with_>=30_train_trades": len(rows),
            "top_by_train": rows[:8],
            "reference": [r for r in rows if r["user_proposed"] or r["current_default"]],
            "warning": ("The best train combo is picked from many tries, so its train number is "
                        "biased upward by selection. Only the TEST column is an honest estimate, "
                        "and only if its bootstrap CI excludes 0.")}


# ---------------------------------------------------------------- main

def fmt(d, indent=0):
    pad = "  " * indent
    for k, v in d.items():
        if isinstance(v, dict):
            print(f"{pad}{k}:")
            fmt(v, indent + 1)
        else:
            print(f"{pad}{k}: {v}")


def build_overrides(a, guard: float) -> dict:
    """CLI overrides -> settings overrides (applied for the whole run)."""
    ov = {"GATE_MAX_APPROX_VP_FRACTION": guard}
    if a.vp_window is not None or a.min_strength is not None:
        vp = a.vp_window if a.vp_window is not None else settings.VP_WINDOW_BARS
        ms = a.min_strength if a.min_strength is not None else settings.MIN_AGGRESSION_STRENGTH
        ov.update(gate_params(vp, ms))
    if a.stop is not None:
        ov["PAPER_STOP_PCT"] = a.stop
    if a.target_r is not None:
        ov["PAPER_TARGET_R_MULTIPLE"] = a.target_r
    if a.hold is not None:
        ov["PAPER_MAX_HOLD_BARS"] = a.hold
    if a.cooldown is not None:
        ov["COOLDOWN_BARS"] = a.cooldown
    slippage = getattr(a, "slippage", None)
    if slippage is not None:
        if slippage < 0:
            raise ValueError("--slippage must be nonnegative")
        ov["PAPER_SLIPPAGE_PCT"] = slippage
    ov["SWEEP_LONG_ONLY"] = a.long_only
    ov["TREND_LONDON_ONLY"] = a.london_only
    return ov


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--end-date", type=date.fromisoformat, default=None, metavar="YYYY-MM-DD",
                    help="last COMPLETE UTC day to include (default: yesterday)")
    ap.add_argument("--vp", choices=("real", "approx"), default="approx")
    ap.add_argument("--split", type=float, default=0.7, help="train fraction (by time)")
    ap.add_argument("--grid", action="store_true", help="parameter sweep with train/test split")
    ap.add_argument("--flip", action="store_true", help="diagnostic: invert every signal's direction")
    ap.add_argument("--long-only", action=argparse.BooleanOptionalAction,
                    default=settings.SWEEP_LONG_ONLY,
                    help="test bullish sweeps only (--no-long-only restores both directions)")
    ap.add_argument("--london-only", action=argparse.BooleanOptionalAction,
                    default=getattr(settings, "TREND_LONDON_ONLY", False),
                    help="restrict trend entries to 08:00-16:00 UTC")
    ap.add_argument("--random-reps", type=int, default=100)
    ap.add_argument("--out", default="logs/backtest")
    ap.add_argument("--keep-zips", action="store_true")
    ap.add_argument("--fill-gaps", action=argparse.BooleanOptionalAction, default=settings.FILL_GAPS,
                    help="opt in to synthetic bars for aligned data gaps")
    ap.add_argument("--gap-mode", choices=("replay", "flat"), default=settings.FILL_GAP_MODE)
    ap.add_argument("--gap-max-bars", type=int, default=settings.FILL_GAP_MAX_BARS)
    ap.add_argument("--funding-file", default="logs/funding_rates.jsonl",
                    help="Funding-rate JSONL cache (used by funding_fade strategy)")
    ap.add_argument("--funding-lookback-events", type=int, default=settings.FUNDING_LOOKBACK_EVENTS,
                    help="Prior funding events used for rolling percentile ranks")
    # overrides (do not touch settings.py)
    ap.add_argument("--stop", type=float, default=None, help="PAPER_STOP_PCT override, e.g. 0.6")
    ap.add_argument("--target-r", type=float, default=None, help="PAPER_TARGET_R_MULTIPLE override")
    ap.add_argument("--hold", type=int, default=None, help="PAPER_MAX_HOLD_BARS override")
    ap.add_argument("--cooldown", type=int, default=None, help="COOLDOWN_BARS override")
    ap.add_argument("--slippage", type=float, default=None,
                    help="PAPER_SLIPPAGE_PCT override, e.g. 0.04")
    ap.add_argument("--vp-window", type=int, default=None, help="VP_WINDOW_BARS (and WARMUP) override")
    ap.add_argument("--min-strength", type=float, default=None, help="MIN_AGGRESSION_STRENGTH override")
    ap.add_argument("--tag", default="", help="suffix for output files so runs don't overwrite each other")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("execution.paper_trader").setLevel(logging.WARNING)

    print(f"backtest_step5a: symbols={','.join(settings.SYMBOLS)} interval={settings.INTERVAL} "
          f"vp={a.vp} days={a.days} end_date={a.end_date or 'yesterday'} "
          f"long_only={a.long_only} "
          f"grid={a.grid} flip={a.flip} tag={a.tag or '-'} out={a.out}", flush=True)
    today = datetime.now(timezone.utc).date()
    if a.end_date is not None and a.end_date >= today:
        raise SystemExit(f"--end-date must be before today (UTC {today}): "
                         "Binance publishes a day's aggTrades file the next day.")
    os.makedirs(a.out, exist_ok=True)
    days = historical.completed_days(a.days, a.end_date)
    log.info("Loading %d day(s) %s..%s, volume profile = %s", len(days), days[0], days[-1], a.vp)
    t0 = time.time()
    funding_metadata = {"enabled": settings.ACTIVE_STRATEGY == "funding_fade",
                        "file": a.funding_file, "events": 0, "percentile_events": 0,
                        "lookback_events": a.funding_lookback_events}
    funding_lookup = {}
    if settings.ACTIVE_STRATEGY == "funding_fade":
        if a.funding_lookback_events < 1:
            raise SystemExit("--funding-lookback-events must be positive")
        funding_events = historical.load_funding_rates(a.funding_file)
        funding_lookup = historical.build_funding_percentile_lookup(
            funding_events, lookback_events=a.funding_lookback_events
        )
        funding_metadata.update(events=len(funding_events), percentile_events=len(funding_lookup))
        if not funding_events:
            log.warning("No funding-rate events loaded from %s; funding_fade will produce no entries.",
                        a.funding_file)
        elif not funding_lookup:
            log.warning("Funding cache has %d event(s), fewer than lookback %d; no percentile snapshots available.",
                        len(funding_events), a.funding_lookback_events)
    segments, validation_by_symbol, all_missing = [], {}, []
    all_bars = []
    for symbol in settings.SYMBOLS:
        # Each instrument gets its own indicator history and contiguous segments.
        bars, missing = historical.load_bars(
            a.vp, symbol, days, settings.VP_BUCKET_SIZE,
            os.path.join(a.out, "bars"), a.keep_zips,
            interval=settings.INTERVAL, fill_gaps_enabled=a.fill_gaps,
            gap_mode=a.gap_mode, gap_max_bars=a.gap_max_bars,
        )
        if missing:
            log.warning("%s: skipped %d day(s) with no archive file: %s", symbol, len(missing),
                        ", ".join(str(d) for d in missing))
        if len(bars) < 500:
            raise SystemExit(f"{symbol}: only {len(bars)} bars loaded - cannot include it in the basket.")
        for bar in bars:
            bar["symbol"] = symbol
        if funding_lookup:
            historical.attach_funding_snapshots(bars, funding_lookup)
        validation_by_symbol[symbol] = validate_bars(bars, settings.INTERVAL, days, missing)
        v = validation_by_symbol[symbol]
        if v["requested_window_coverage_pct"] < 99.0:
            window_start = days[0] if days else date.fromisoformat(v["first_day"])
            start_day = date.fromisoformat(v["first_day"])
            log.warning(
                "%s: history starts %s (%d days after window start %s), covers %.1f%% of window",
                symbol, v["first_day"], (start_day - window_start).days,
                window_start, v["requested_window_coverage_pct"],
            )
        all_missing.extend(f"{symbol}:{d}" for d in missing)
        all_bars.extend(bars)
        segments.extend(split_segments(bars))
    all_bars.sort(key=lambda bar: (bar["open_time"], bar.get("symbol", "")))
    data_validation = {
        "interval": settings.INTERVAL,
        "interval_consistent": all(v["interval_consistent"] for v in validation_by_symbol.values()),
        "symbols": validation_by_symbol,
        "synthetic_bars": sum(v["synthetic_bars"] for v in validation_by_symbol.values()),
        "synthetic_days": sorted({day for v in validation_by_symbol.values() for day in v["synthetic_days"]}),
        "gaps_count": sum(v["gaps_count"] for v in validation_by_symbol.values()),
        "known_missing_days": all_missing,
    }
    n_days = len(days)
    log.info("%d bars across %d symbols (%.1f requested days) in %d stretches; loaded in %.0fs",
             len(all_bars), len(settings.SYMBOLS), n_days, len(segments), time.time() - t0)

    # An all-approximated profile would be blocked by the live guard; the backtest says so explicitly.
    guard = 1.0 if a.vp == "approx" else settings.GATE_MAX_APPROX_VP_FRACTION
    suffix = "".join([("_flipped" if a.flip else ""), (f"_{a.tag}" if a.tag else "")])
    with override(**build_overrides(a, guard)):
        if a.grid:
            result = grid_run(segments, n_days, a.split)
            name = f"grid{suffix}.json"
        else:
            from engine.decision import (
                reset_sweep_diagnostics, sweep_diagnostics_snapshot,
                decision_diagnostics_snapshot, funding_diagnostics_snapshot,
            )
            reset_sweep_diagnostics()
            result, trades = single_run(segments, n_days, a.split, a.random_reps, a.flip)
            result["sweep_filter_funnel"] = sweep_diagnostics_snapshot()
            result["funding_filter_diagnostics"] = funding_diagnostics_snapshot()
            result["decision_filter_diagnostics"] = decision_diagnostics_snapshot()
            if not a.flip:
                result["walk_forward"] = walk_forward(segments, folds=4)
                if a.random_reps > 0:
                    result["permutation_test"] = permutation_test(segments, run_gate(segments), n_perm=500)
            with open(os.path.join(a.out, f"trades{suffix}.jsonl"), "w", encoding="utf-8") as f:
                for t in trades:
                    f.write(json.dumps(t) + "\n")
            name = f"summary{suffix}.json"
    result["vp_source"] = a.vp
    result["symbols"] = list(settings.SYMBOLS)
    result["interval"] = settings.INTERVAL
    result["sweep_long_only"] = a.long_only
    result["funding_data"] = funding_metadata
    result["data_validation"] = data_validation
    result["days_requested"] = [str(d) for d in days]
    result["days_missing"] = all_missing
    if data_validation.get("synthetic_bars", 0):
        result["synthetic_data_caveat"] = (
            "Synthetic bars are fabricated continuity, not observed market data. "
            "Metrics may be affected around these dates; the go/no-go gate blocks Step 5b. "
            "Repeat validation on fully observed data before treating results as headline evidence."
        )
    if a.vp == "approx":
        result["caveat"] = ("Volume profile is approximated for every bar; Bucket A is biased "
                            "toward NEUTRAL vs live. Re-run with --vp real before trusting this.")
    result["go_no_go"] = go_no_go_gate(result)
    with open(os.path.join(a.out, name), "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(f"\n===== BACKTEST ({a.vp} VP, {n_days:.1f} days of data) -> {os.path.join(a.out, name)} =====")
    fmt(result)


if __name__ == "__main__":
    main()
