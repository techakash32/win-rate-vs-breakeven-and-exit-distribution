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
import bisect
import io
import json
import logging
import math
import os
import urllib.error
import urllib.request
import zipfile
from datetime import date, datetime, timedelta, timezone

from config import settings
from scripts import preload_history as ph

log = logging.getLogger(__name__)

VISION = ("https://data.binance.vision/data/futures/um/daily/aggTrades/"
          "{sym}/{sym}-aggTrades-{day}.zip")
INTERVAL_MS_MAP = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


def _make_synthetic(template: dict, ts: int, ohlc: tuple[float, float, float, float],
                    volume: float = 0.0, shift: float = 0.0) -> dict:
    """Clone a bar while marking synthetic OHLC and neutralizing trade-flow fields."""
    b = dict(template)
    o, h, l, c = ohlc
    b["open_time"] = ts
    if "close_time" in template:
        b["close_time"] = ts + (template["close_time"] - template["open_time"])
    b["open"], b["high"], b["low"], b["close"] = o, max(h, o, c), min(l, o, c), c
    for key in ("volume", "quote_volume", "taker_buy_base", "taker_buy_quote", "kline_volume"):
        if key in b:
            b[key] = volume
    src_total = float(template.get("volume", 0.0))
    buy_share = (float(template.get("buy_vol", 0.0)) / src_total) if src_total > 0 else 0.5
    if "buy_vol" in b:
        b["buy_vol"] = volume * buy_share
    if "sell_vol" in b:
        b["sell_vol"] = volume * (1.0 - buy_share)
    if "delta" in b:
        b["delta"] = 0.0
    for key in ("trade_count", "num_trades"):
        if key in b:
            b[key] = 0
    pv = template.get("price_volumes", {})
    b["price_volumes"] = ({round(float(k) + shift, 8): v for k, v in pv.items()}
                           if volume and pv else {})
    b["approx_vp"] = template.get("approx_vp", False)
    b["synthetic"] = True
    return b


def _fill_flat(prev: dict, n: int, interval_ms: int) -> list[dict]:
    p = float(prev["close"])
    return [_make_synthetic(prev, prev["open_time"] + k * interval_ms,
                            (p, p, p, p), volume=0.0) for k in range(1, n + 1)]


def _fill_replay(bars_so_far: list[dict], prev: dict, n: int, interval_ms: int) -> list[dict]:
    """Repeat recent OHLC shapes, shifted around previous close; bars remain tagged synthetic."""
    tail = bars_so_far[-n:]
    if not tail:
        return _fill_flat(prev, n, interval_ms)
    shift = float(prev["close"]) - float(tail[0]["open"])
    out = []
    for i in range(n):
        src = tail[i % len(tail)]
        o, h, l, c = (float(src[k]) + shift for k in ("open", "high", "low", "close"))
        out.append(_make_synthetic(src, prev["open_time"] + (i + 1) * interval_ms,
                                   (o, h, l, c), volume=float(src.get("volume", 0.0)), shift=shift))
    return out


def fill_gaps(bars: list[dict], interval_ms: int, max_fill_bars: int = 288,
              mode: str = "replay", logger=None) -> list[dict]:
    """Optionally fill aligned timestamp holes; refuse oversized/unexpected modes."""
    if mode not in ("flat", "replay"):
        raise ValueError("gap fill mode must be 'flat' or 'replay'")
    if len(bars) < 2:
        return bars
    out = [bars[0]]
    for prev, cur in zip(bars, bars[1:]):
        diff = cur["open_time"] - prev["open_time"]
        if diff <= interval_ms:
            out.append(cur)
            continue
        if diff % interval_ms:
            raise ValueError(f"Misaligned data gap ({diff} ms) after {prev['open_time']}; cannot fill safely.")
        n_missing = diff // interval_ms - 1
        if n_missing > max_fill_bars:
            raise ValueError(f"Gap of {n_missing} bars exceeds max_fill_bars={max_fill_bars}; refusing to synthesize.")
        filler = (_fill_flat(prev, n_missing, interval_ms) if mode == "flat"
                  else _fill_replay(out, prev, n_missing, interval_ms))
        if logger:
            logger.warning("Filled %d synthetic bars (%s) after ts=%d", n_missing, mode, prev["open_time"])
        out.extend(filler)
        out.append(cur)
    return out


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
    """Raises FileNotFoundError if the archive has no file for that day (HTTP 404)."""
    req = urllib.request.Request(url, headers={"User-Agent": "confirmation-engine/1.0"})
    tmp = dest + ".part"
    try:
        with urllib.request.urlopen(req, timeout=60) as resp, open(tmp, "wb") as f:
            while True:
                chunk = resp.read(1 << 20)
                if not chunk:
                    break
                f.write(chunk)
    except urllib.error.HTTPError as exc:
        if os.path.exists(tmp):
            os.remove(tmp)
        if exc.code == 404:
            raise FileNotFoundError(url) from exc
        raise
    os.replace(tmp, dest)


def parse_aggtrades_zip(zip_path: str, bucket_size: float, step: int) -> list:
    """Stream a Vision aggTrades zip into bars of size ``step`` ms (no full extraction)."""
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


def _has_internal_gap(bars: list[dict], interval_ms: int) -> bool:
    """Return true when a daily series skips one or more expected bar opens."""
    ordered = sorted(bars, key=lambda b: b["open_time"])
    return any(cur["open_time"] - prev["open_time"] > interval_ms
               for prev, cur in zip(ordered, ordered[1:]))


def vision_day_bars(symbol: str, day: date, bucket_size: float, cache_dir: str,
                    keep_zips: bool = False, interval_ms: int | None = None):
    """Bars for one UTC day, or None if the archive has no file for it."""
    interval_ms = interval_ms or settings.INTERVAL_MS
    os.makedirs(cache_dir, exist_ok=True)
    cache = os.path.join(cache_dir, f"{symbol}-{day}-b{bucket_size:g}-i{interval_ms}.jsonl")
    if os.path.exists(cache):
        out = []
        with open(cache, encoding="utf-8") as f:
            for line in f:
                bar = json.loads(line)
                bar["price_volumes"] = {float(k): v for k, v in bar["price_volumes"].items()}
                out.append(bar)
        if not _has_internal_gap(out, interval_ms):
            return out
        log.warning("Cached bars for %s contain an internal gap; discarding cache and refetching.", day)
        os.remove(cache)

    zip_path = os.path.join(cache_dir, f"{symbol}-aggTrades-{day}.zip")
    if os.path.exists(zip_path):
        # A cache with a gap may have come from an incomplete prior archive too.
        os.remove(zip_path)
    if not os.path.exists(zip_path):
        url = VISION.format(sym=symbol, day=day)
        log.info("Downloading %s ...", url)
        try:
            _download(url, zip_path)
        except FileNotFoundError:
            log.warning("Archive has no file for %s (404) - skipping that day.", day)
            return None
    log.info("Parsing %s ...", zip_path)
    bars = parse_aggtrades_zip(zip_path, bucket_size, interval_ms)
    if _has_internal_gap(bars, interval_ms):
        log.warning("Fresh Binance archive for %s still has an internal gap; gap validation/filling will handle it.", day)

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
              cache_dir: str, keep_zips: bool = False, interval: str | None = None,
              fill_gaps_enabled: bool = False, gap_mode: str = "replay",
              gap_max_bars: int = 288):
    """Returns (bars, missing_days). Missing days leave a time gap in `bars`."""
    interval = interval or settings.INTERVAL
    if interval not in INTERVAL_MS_MAP:
        raise ValueError(f"Unsupported interval {interval!r}; supported: {', '.join(INTERVAL_MS_MAP)}")
    interval_ms = INTERVAL_MS_MAP[interval]
    if source == "real":
        bars, missing = [], []
        for d in days:
            day_bars = vision_day_bars(symbol, d, bucket_size, cache_dir, keep_zips,
                                       interval_ms=interval_ms)
            if day_bars is None:
                missing.append(d)
                continue
            log.info("%s: %d bars", d, len(day_bars))
            bars.extend(day_bars)
        if fill_gaps_enabled:
            bars = fill_gaps(bars, interval_ms, gap_max_bars, gap_mode, log)
        return bars, missing

    start = datetime.combine(days[0], datetime.min.time(), tzinfo=timezone.utc)
    end = datetime.combine(days[-1] + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    rows = fetch_klines_range(symbol, interval,
                              int(start.timestamp() * 1000), int(end.timestamp() * 1000) - 1)
    bars = klines_to_bars(rows, bucket_size)
    if fill_gaps_enabled:
        bars = fill_gaps(bars, interval_ms, gap_max_bars, gap_mode, log)
    return bars, []


def load_funding_rates(path: str = "logs/funding_rates.jsonl") -> list[tuple[int, float]]:
    """Load valid funding rates as a sorted list of (event_time_ms, rate)."""
    events = []
    if not os.path.exists(path):
        return events
    with open(path, encoding="utf-8") as f:
        for line_number, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            try:
                timestamp, rate = int(obj["time_ms"]), float(obj["rate"])
            except (KeyError, TypeError, ValueError, OverflowError) as exc:
                raise ValueError(f"Invalid funding record at {path}:{line_number}") from exc
            if not math.isfinite(rate):
                raise ValueError(f"Non-finite funding rate at {path}:{line_number}")
            events.append((timestamp, rate))
    events.sort(key=lambda item: item[0])
    return events


def build_funding_percentile_lookup(
    funding_events: list[tuple[int, float]], lookback_events: int = 270,
) -> dict[int, dict]:
    """Compute each event's trailing percentile using prior events only.

    The current event is excluded from its own window. ``prior_pct`` is the
    preceding event's percentile, enabling fresh crossings of extreme bands.
    """
    if lookback_events < 1:
        raise ValueError("lookback_events must be positive")
    events = sorted(funding_events, key=lambda item: item[0])
    result = {}
    rates = [rate for _, rate in events]
    for i, (timestamp, rate) in enumerate(events):
        if i < lookback_events:
            continue
        window = rates[i - lookback_events:i]
        pct = sum(1 for prior_rate in window if prior_rate < rate) / len(window) * 100.0
        previous_timestamp = events[i - 1][0]
        prior_pct = result.get(previous_timestamp, {}).get("pct", 50.0)
        result[timestamp] = {"pct": pct, "prior_pct": prior_pct, "rate": rate}
    return result


def attach_funding_snapshots(bars: list[dict], funding_lookup: dict[int, dict]) -> None:
    """Attach the latest published funding event at or before each bar open."""
    funding_times = sorted(funding_lookup)
    for bar in bars:
        idx = bisect.bisect_right(funding_times, bar["open_time"]) - 1
        if idx >= 0:
            event_time = funding_times[idx]
            bar["funding_snapshot"] = {**funding_lookup[event_time], "time_ms": event_time}
        else:
            bar["funding_snapshot"] = None
