# data/historical.py
"""
Historical bars for the backtest, in the SAME dict format the live pipeline uses.

Two sources:
  * Binance Vision daily aggTrades (futures/um) -> REAL per-price volume, exact
    buy/sell split, exact OHLC. Past days only (today's file doesn't exist yet).
    Files are large (hundreds of MB per day): each day is downloaded, reduced to
    1-minute bars, cached as JSONL, and the zip deleted unless keep_zips=True.
  * REST klines -> exact OHLC / volume / taker-buy, but volume profile is the
    uniform approximation (approx_vp=True). Fast, but Bucket A then differs from
    what the live system sees.
"""
import io
import json
import logging
import os
import urllib.request
import zipfile
from datetime import date, datetime, timedelta, timezone

from config import settings
from scripts import preload_history as ph

log = logging.getLogger(__name__)

VISION = ("https://data.binance.vision/data/futures/um/daily/aggTrades/"
          "{sym}/{sym}-aggTrades-{day}.zip")


# ---------- REST klines (approximate volume profile) ----------

def fetch_klines_range(symbol, interval, start_ms, end_ms, delay=0.15) -> list:
    rows, cursor = [], int(start_ms)
    while cursor < end_ms:
        batch = ph.fetch_klines(symbol, interval, limit=1500, start_ms=cursor, end_ms=end_ms)
        if not batch:
            break
        rows.extend(batch)
        cursor = int(batch[-1][6]) + 1
        if len(batch) < 1500:
            break
        import time
        time.sleep(delay)
    return rows


def klines_to_bars(rows, bucket_size) -> list:
    return [ph.kline_to_bar(r, bucket_size) for r in rows]


# ---------- Binance Vision aggTrades (real volume profile) ----------

def _download(url: str, dest: str) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "confirmation-engine/1.0"})
    tmp = dest + ".part"
    with urllib.request.urlopen(req, timeout=60) as resp, open(tmp, "wb") as f:
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)
    os.replace(tmp, dest)


def parse_aggtrades_zip(zip_path: str, bucket_size: float, step: int) -> list:
    """Stream a Vision aggTrades zip into 1-minute bars (no full extraction)."""
    acc: dict[int, dict] = {}
    with zipfile.ZipFile(zip_path) as z:
        with z.open(z.namelist()[0]) as raw:
            for line in io.TextIOWrapper(raw, encoding="utf-8"):
                p = line.split(",")
                if len(p) < 7:
                    continue
                try:
                    price, qty, t = float(p[1]), float(p[2]), int(p[5])
                except ValueError:
                    continue                      # header row
                if t > 10 ** 14:                  # microseconds -> ms
                    t //= 1000
                key = t - t % step
                b = acc.get(key)
                if b is None:
                    b = acc[key] = {"open_time": key, "open": price, "high": price,
                                    "low": price, "close": price, "buy": 0.0,
                                    "sell": 0.0, "n": 0, "pv": {}}
                if price > b["high"]:
                    b["high"] = price
                elif price < b["low"]:
                    b["low"] = price
                b["close"] = price
                level = round(price / bucket_size) * bucket_size
                b["pv"][level] = b["pv"].get(level, 0.0) + qty
                if p[6].lstrip()[:1] in ("t", "T"):   # buyer is maker -> aggressor SELL
                    b["sell"] += qty
                else:
                    b["buy"] += qty
                b["n"] += 1

    bars = []
    for key in sorted(acc):
        b = acc[key]
        vol = b["buy"] + b["sell"]
        bars.append({
            "open_time": key, "open": b["open"], "high": b["high"], "low": b["low"],
            "close": b["close"], "buy_vol": b["buy"], "sell_vol": b["sell"],
            "volume": vol, "delta": b["buy"] - b["sell"], "trade_count": b["n"],
            "kline_volume": vol, "vol_err_pct": 0.0, "partial": False,
            "approx_vp": False,
            "price_volumes": {round(k, 8): v for k, v in b["pv"].items()},
        })
    return bars


def vision_day_bars(symbol: str, day: date, bucket_size: float, cache_dir: str,
                    keep_zips: bool = False) -> list:
    os.makedirs(cache_dir, exist_ok=True)
    cache = os.path.join(cache_dir, f"{symbol}-{day}-b{bucket_size:g}.jsonl")
    if os.path.exists(cache):
        out = []
        with open(cache, encoding="utf-8") as f:
            for line in f:
                bar = json.loads(line)
                bar["price_volumes"] = {float(k): v for k, v in bar["price_volumes"].items()}
                out.append(bar)
        return out

    zip_path = os.path.join(cache_dir, f"{symbol}-aggTrades-{day}.zip")
    if not os.path.exists(zip_path):
        url = VISION.format(sym=symbol, day=day)
        log.info("Downloading %s ...", url)
        _download(url, zip_path)
    log.info("Parsing %s ...", zip_path)
    bars = parse_aggtrades_zip(zip_path, bucket_size, settings.INTERVAL_MS)

    tmp = cache + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for b in bars:
            f.write(json.dumps(b) + "\n")
    os.replace(tmp, cache)
    if not keep_zips:
        os.remove(zip_path)
    return bars


def completed_days(n_days: int, end_day: date | None = None) -> list:
    """The last n COMPLETE UTC days (today is excluded: its file isn't published)."""
    end_day = end_day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    return [end_day - timedelta(days=i) for i in range(n_days - 1, -1, -1)]


def load_bars(source: str, symbol: str, days: list, bucket_size: float,
              cache_dir: str, keep_zips: bool = False) -> list:
    if source == "real":
        bars = []
        for d in days:
            day_bars = vision_day_bars(symbol, d, bucket_size, cache_dir, keep_zips)
            log.info("%s: %d bars", d, len(day_bars))
            bars.extend(day_bars)
        return bars

    start = datetime.combine(days[0], datetime.min.time(), tzinfo=timezone.utc)
    end = datetime.combine(days[-1] + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    rows = fetch_klines_range(symbol, settings.INTERVAL,
                              int(start.timestamp() * 1000), int(end.timestamp() * 1000) - 1)
    return klines_to_bars(rows, bucket_size)