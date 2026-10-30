# scripts/preload_history.py
"""
Preload recent CLOSED 1m bars from Binance USD-S-M REST so the gate has a full
history at t=0. Also used at runtime to heal gaps (restart / reconnect).

Klines give exact OHLC, volume and taker-buy volume. Per-price volume comes
from REAL aggTrades (/fapi/v1/aggTrades, paginated and paced); a bar is accepted
as real only if its trade volume matches the kline volume. Anything that can't
be built from real trades falls back to spreading volume uniformly over
[low, high] and is flagged "approx_vp": True. The gate refuses to trade while
approximated bars are in the volume profile (GATE_MAX_APPROX_VP_FRACTION).
"""
import json
import logging
import os
import time
import urllib.parse
import urllib.request

from config import settings
from data.bar_aggregator import BarBucket

log = logging.getLogger(__name__)

MAX_LIMIT = 1500


def _get_json(path: str, params: dict | None = None):
    url = settings.REST_BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "confirmation-engine/1.0"})
    with urllib.request.urlopen(req, timeout=settings.REST_TIMEOUT_S) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get_json_retry(path: str, params: dict | None = None):
    """_get_json with exponential backoff, so a brief network drop doesn't
    leave a bar approximated."""
    last = None
    for attempt in range(settings.PRELOAD_TRADE_RETRIES + 1):
        try:
            return _get_json(path, params)
        except Exception as exc:                    # network / HTTP error
            last = exc
            if attempt < settings.PRELOAD_TRADE_RETRIES:
                time.sleep(min(0.5 * 2 ** attempt, 4.0))
    raise last


def server_time_ms() -> int:
    """Binance server time (immune to local clock skew); falls back to local."""
    try:
        return int(_get_json("/fapi/v1/time")["serverTime"])
    except Exception:
        return int(time.time() * 1000)


def fetch_klines(symbol, interval, limit=MAX_LIMIT, start_ms=None, end_ms=None):
    params = {
        "symbol": symbol.upper(),
        "interval": interval,
        "limit": max(1, min(int(limit), MAX_LIMIT)),
    }
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    return _get_json("/fapi/v1/klines", params)


def approximate_price_volumes(low, high, volume, bucket_size) -> dict:
    """Spread `volume` uniformly over the price buckets covering [low, high]."""
    if low <= 0 or high <= 0 or volume <= 0:
        return {}
    lo_b = round(low / bucket_size) * bucket_size
    hi_b = round(high / bucket_size) * bucket_size
    if hi_b < lo_b:
        lo_b, hi_b = hi_b, lo_b
    n = int(round((hi_b - lo_b) / bucket_size)) + 1
    per_bucket = volume / n
    return {round(lo_b + i * bucket_size, 8): per_bucket for i in range(n)}


def kline_to_bar(row, bucket_size) -> dict:
    volume = float(row[5])
    buy_vol = float(row[9])                 # taker buy base volume (exact)
    sell_vol = max(0.0, volume - buy_vol)
    high, low, close = float(row[2]), float(row[3]), float(row[4])
    return {
        "open_time": int(row[0]),
        "buy_vol": buy_vol,
        "sell_vol": sell_vol,
        "volume": volume,
        "delta": buy_vol - sell_vol,
        "trade_count": int(row[8]),
        "open": float(row[1]),
        "high": high,
        "low": low,
        "close": close,
        "kline_volume": volume,
        "vol_err_pct": 0.0,
        "partial": False,
        "approx_vp": True,
        "price_volumes": approximate_price_volumes(low, high, volume, bucket_size),
    }


def closed_bars(rows, bucket_size, now_ms) -> list:
    """Convert REST rows to bars, dropping any bar that is still forming."""
    return [kline_to_bar(r, bucket_size) for r in rows if int(r[6]) < now_ms]


def fetch_agg_trades(symbol, start_ms, end_ms, budget):
    """
    Paginate /fapi/v1/aggTrades over [start_ms, end_ms] (each request window
    < 1 h). `budget` = {"calls": remaining}; decremented in place.
    Returns raw rows, de-duplicated by aggregate id.
    """
    trades, seen = [], set()
    cursor = start_ms
    while cursor <= end_ms and budget["calls"] > 0:
        win_end = min(end_ms, cursor + 3_599_000)
        rows = _get_json_retry("/fapi/v1/aggTrades", {
            "symbol": symbol.upper(), "startTime": int(cursor),
            "endTime": int(win_end), "limit": 1000,
        })
        budget["calls"] -= 1
        new = 0
        for r in rows:
            if r["a"] not in seen:
                seen.add(r["a"])
                trades.append(r)
                new += 1
        if len(rows) < 1000:
            cursor = win_end + 1                    # window exhausted
        else:
            last_t = int(rows[-1]["T"])
            cursor = last_t if new else last_t + 1  # page full: continue from last ms
        time.sleep(settings.PRELOAD_TRADE_PAGE_DELAY_S)
    return trades


def fetch_agg_trades_recent_first(symbol, start_ms, end_ms, chunk_ms=None):
    """
    Fetch aggTrades one BAR at a time from NEWEST to oldest under a shared
    call budget, so if the budget runs out only the oldest bars are missing
    (a bar cut off mid-way fails the volume check and falls back to the
    approximation).
    """
    chunk_ms = chunk_ms or settings.INTERVAL_MS
    budget = {"calls": settings.PRELOAD_MAX_TRADE_CALLS}
    out = []
    chunk_end = end_ms
    while chunk_end >= start_ms and budget["calls"] > 0:
        chunk_start = max(start_ms, chunk_end - chunk_ms + 1)
        out.extend(fetch_agg_trades(symbol, chunk_start, chunk_end, budget))
        chunk_end = chunk_start - 1
    if chunk_end >= start_ms:
        log.warning("aggTrades call cap hit; bars before %d use the approximation.",
                    chunk_end + 1)
    return out


def real_bars(rows, trades, bucket_size) -> list:
    """
    Build bars from kline rows + real aggTrades. A bar whose trade volume does
    not match the kline volume (missing trades) falls back to the approximation.
    """
    buckets = {}
    step = settings.INTERVAL_MS
    for t in trades:
        key = (int(t["T"]) // step) * step
        b = buckets.get(key)
        if b is None:
            b = buckets[key] = BarBucket(open_time=key)
        b.add_trade(float(t["p"]), float(t["q"]), bool(t["m"]), bucket_size)

    bars = []
    for r in rows:
        bar = kline_to_bar(r, bucket_size)            # approximate baseline
        b = buckets.get(bar["open_time"])
        if b is not None and bar["volume"] > 0:
            err = abs(b.volume - bar["volume"]) / bar["volume"] * 100.0
            if err <= settings.PRELOAD_REAL_MAX_VOL_ERR_PCT:
                bar.update({
                    "buy_vol": b.buy_vol, "sell_vol": b.sell_vol, "volume": b.volume,
                    "delta": b.delta, "trade_count": b.trade_count,
                    "price_volumes": dict(b.price_volumes),
                    "approx_vp": False, "vol_err_pct": err,
                })
        bars.append(bar)
    return bars


def bars_from_rows(rows, symbol, bucket_size, now_ms) -> list:
    """Closed bars from kline rows; real per-price volume when possible."""
    rows = [r for r in rows if int(r[6]) < now_ms]
    if not rows:
        return []
    if settings.PRELOAD_REAL_TRADES:
        try:
            start = int(rows[0][0])
            end = int(rows[-1][6])
            n_min = len(rows)
            log.info("Fetching real aggTrades for %d bar(s) (paced; may take a minute)...", n_min)
            trades = fetch_agg_trades_recent_first(symbol, start, end)
            bars = real_bars(rows, trades, bucket_size)
            n_real = sum(1 for b in bars if not b["approx_vp"])
            log.info("Real volume profile for %d/%d bar(s) (%d trades).",
                     n_real, len(bars), len(trades))
            return bars
        except Exception as exc:
            log.warning("Real aggTrades fetch failed (%s); using approximation.", exc)
    return [kline_to_bar(r, bucket_size) for r in rows]


def fetch_bars(symbol, interval, bucket_size, start_ms, end_ms, now_ms) -> list:
    """Closed bars in [start_ms, end_ms] (used to heal gaps)."""
    step = settings.INTERVAL_MS
    limit = int((end_ms - start_ms) // step) + 1
    rows = fetch_klines(symbol, interval, limit, start_ms, end_ms)
    return bars_from_rows(rows, symbol, bucket_size, now_ms)


def preload(symbol, interval, bucket_size, keep, out_path=None):
    """Fetch the last `keep` closed bars and write them to the history file."""
    out_path = out_path or settings.BAR_HISTORY_PATH
    rows = fetch_klines(symbol, interval, limit=min(MAX_LIMIT, keep + 5))
    if not rows:
        raise RuntimeError("REST returned no klines")

    now_ms = server_time_ms()
    closed = [r for r in rows if int(r[6]) < now_ms][-keep:]
    bars = bars_from_rows(closed, symbol, bucket_size, now_ms)
    if not bars:
        raise RuntimeError("REST returned no closed klines")

    cvd = 0.0
    for bar in bars:
        cvd += bar["delta"]
        bar["cvd"] = cvd

    folder = os.path.dirname(out_path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for bar in bars:
            f.write(json.dumps(bar) + "\n")
    os.replace(tmp, out_path)
    return len(bars), out_path


if __name__ == "__main__":
    keep = max(settings.VP_WINDOW_BARS, settings.AGGR_WINDOW_BARS) + 1
    n, path = preload(
        symbol=settings.SYMBOLS[0],
        interval=settings.INTERVAL,
        bucket_size=settings.VP_BUCKET_SIZE,
        keep=keep,
    )
    print(f"Preloaded {n} bars -> {path}")