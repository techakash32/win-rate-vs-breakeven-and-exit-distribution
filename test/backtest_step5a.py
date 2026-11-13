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
import os
import random
import statistics
import sys
import time
from contextlib import contextmanager
from datetime import date, datetime, timezone
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
    old = {k: getattr(settings, k) for k in kw}
    for k, v in kw.items():
        setattr(settings, k, v)
    try:
        yield
    finally:
        for k, v in old.items():
            setattr(settings, k, v)


def gate_params(vp_window: int, min_strength: float) -> dict:
    return dict(
        VP_WINDOW_BARS=vp_window, WARMUP_BARS=vp_window,
        MIN_AGGRESSION_STRENGTH=min_strength,
        AGGR_RATIO_THRESHOLD=0.5 + min_strength / 4.0,   # keep B consistent with the gate
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
    hist_len = max(settings.VP_WINDOW_BARS, settings.AGGR_WINDOW_BARS) + 1
    out = []
    for seg in segments:
        decider = Decider()                    # fresh warmup + cooldown per stretch
        sigs = []
        for i, bar in enumerate(seg):
            d = decider.decide(seg[max(0, i - hist_len + 1): i + 1], bar["close"])
            sigs.append((d.allowed, d.direction) if not d.warmup else (False, None))
        out.append(sigs)
    return out


def flip_signals(sigs: list) -> list:
    inv = {Dir.LONG: Dir.SHORT, Dir.SHORT: Dir.LONG}
    return [[(a, inv.get(d, d)) for a, d in seg] for seg in sigs]


def simulate(segments: list, sigs: list) -> list:
    """Replay signals through the live PaperTrader, one fresh trader per stretch."""
    trades = []
    for seg, seg_sigs in zip(segments, sigs):
        trader = PaperTrader(persist=False, log_trades=False)   # no disk state is read or written
        for bar, (allowed, direction) in zip(seg, seg_sigs):
            trader.on_bar(bar, allowed, direction, "bt")
        trades.extend(trader.trades)
    trades.sort(key=lambda t: t["exit_ts"])
    return trades


def count_signals(segments: list, sigs: list, lo_ts=None, hi_ts=None) -> int:
    n = 0
    for seg, seg_sigs in zip(segments, sigs):
        for bar, (allowed, _d) in zip(seg, seg_sigs):
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
        "fee_to_stop_pct": round((2 * settings.PAPER_FEE_TAKER_PCT) / settings.PAPER_STOP_PCT * 100, 1)
        if settings.PAPER_STOP_PCT else None,
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


# ---------------------------------------------------------------- modes

def single_run(segments, days, split, reps, flip=False):
    flat = [b for seg in segments for b in seg]
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

    res = {
        "config": {"vp_window": settings.VP_WINDOW_BARS, "min_strength": settings.MIN_AGGRESSION_STRENGTH,
                   "stop_pct": settings.PAPER_STOP_PCT, "target_r": settings.PAPER_TARGET_R_MULTIPLE,
                   "max_hold_bars": settings.PAPER_MAX_HOLD_BARS, "cooldown": settings.COOLDOWN_BARS,
                   "fee_taker_pct": settings.PAPER_FEE_TAKER_PCT,
                   "fee_maker_pct": settings.PAPER_FEE_MAKER_PCT},
        "flipped_signals": flip,
        "bars": n_bars, "segments": len(segments), "days": round(days, 1),
        "diagnosis": {"all": diagnose(m_all), "test": diagnose(m_test)},
        "all": m_all,
        "train": m_train,
        "test": m_test,
        "move_stats": move_stats(segments, settings.PAPER_STOP_PCT, settings.PAPER_TARGET_R_MULTIPLE),
    }
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
    flat = [b for seg in segments for b in seg]
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
    ap.add_argument("--random-reps", type=int, default=100)
    ap.add_argument("--out", default="logs/backtest")
    ap.add_argument("--keep-zips", action="store_true")
    # overrides (do not touch settings.py)
    ap.add_argument("--stop", type=float, default=None, help="PAPER_STOP_PCT override, e.g. 0.6")
    ap.add_argument("--target-r", type=float, default=None, help="PAPER_TARGET_R_MULTIPLE override")
    ap.add_argument("--hold", type=int, default=None, help="PAPER_MAX_HOLD_BARS override")
    ap.add_argument("--cooldown", type=int, default=None, help="COOLDOWN_BARS override")
    ap.add_argument("--vp-window", type=int, default=None, help="VP_WINDOW_BARS (and WARMUP) override")
    ap.add_argument("--min-strength", type=float, default=None, help="MIN_AGGRESSION_STRENGTH override")
    ap.add_argument("--tag", default="", help="suffix for output files so runs don't overwrite each other")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("execution.paper_trader").setLevel(logging.WARNING)

    print(f"backtest_step5a: vp={a.vp} days={a.days} end_date={a.end_date or 'yesterday'} "
          f"grid={a.grid} flip={a.flip} tag={a.tag or '-'} out={a.out}", flush=True)
    today = datetime.now(timezone.utc).date()
    if a.end_date is not None and a.end_date >= today:
        raise SystemExit(f"--end-date must be before today (UTC {today}): "
                         "Binance publishes a day's aggTrades file the next day.")
    os.makedirs(a.out, exist_ok=True)
    days = historical.completed_days(a.days, a.end_date)
    log.info("Loading %d day(s) %s..%s, volume profile = %s", len(days), days[0], days[-1], a.vp)
    t0 = time.time()
    # The bar cache is keyed by bucket size, so VP_BUCKET_SIZE must stay at its settings value here.
    bars, missing = historical.load_bars(a.vp, settings.SYMBOLS[0], days, settings.VP_BUCKET_SIZE,
                                         os.path.join(a.out, "bars"), a.keep_zips)
    if missing:
        log.warning("Skipped %d day(s) with no archive file: %s", len(missing),
                    ", ".join(str(d) for d in missing))
    if len(bars) < 500:
        raise SystemExit(f"Only {len(bars)} bars loaded - nothing to test.")
    segments = split_segments(bars)
    n_days = len(bars) * STEP / 86_400_000
    log.info("%d bars (%.1f days of data) in %d contiguous stretch(es), loaded in %.0fs",
             len(bars), n_days, len(segments), time.time() - t0)

    # An all-approximated profile would be blocked by the live guard; the backtest says so explicitly.
    guard = 1.0 if a.vp == "approx" else settings.GATE_MAX_APPROX_VP_FRACTION
    suffix = "".join([("_flipped" if a.flip else ""), (f"_{a.tag}" if a.tag else "")])
    with override(**build_overrides(a, guard)):
        if a.grid:
            result = grid_run(segments, n_days, a.split)
            name = f"grid{suffix}.json"
        else:
            result, trades = single_run(segments, n_days, a.split, a.random_reps, a.flip)
            with open(os.path.join(a.out, f"trades{suffix}.jsonl"), "w", encoding="utf-8") as f:
                for t in trades:
                    f.write(json.dumps(t) + "\n")
            name = f"summary{suffix}.json"
    result["vp_source"] = a.vp
    result["days_requested"] = [str(d) for d in days]
    result["days_missing"] = [str(d) for d in missing]
    if a.vp == "approx":
        result["caveat"] = ("Volume profile is approximated for every bar; Bucket A is biased "
                            "toward NEUTRAL vs live. Re-run with --vp real before trusting this.")
    with open(os.path.join(a.out, name), "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(f"\n===== BACKTEST ({a.vp} VP, {n_days:.1f} days of data) -> {os.path.join(a.out, name)} =====")
    fmt(result)


if __name__ == "__main__":
    main()