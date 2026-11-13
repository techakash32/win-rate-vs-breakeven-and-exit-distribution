
"""
Download funding rate history from Binance USD-M futures.

Funding rates are typically published every 8 hours.
Downloads historical funding events with pagination and saves JSONL.

Designed for approximately 1100-1200 days of backtesting data.
"""

import json
import logging
import math
import os
import time
import urllib.parse
import urllib.request
import urllib.error

from typing import Optional

log = logging.getLogger(__name__)

BINANCE_FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"

LIMIT = 1000
MAX_RETRIES = 5
REQUEST_TIMEOUT = 30
REQUEST_DELAY = 0.15


def safe_float(value, default=None) -> Optional[float]:
    """Safely convert API values to finite floats."""
    try:
        if value is None or value == "":
            return default

        result = float(value)

        if not math.isfinite(result):
            return default

        return result

    except (ValueError, TypeError, OverflowError):
        return default


def fetch_funding_range(
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict]:
    """
    Fetch funding events in [start_ms, end_ms].
    Automatically paginates through Binance API.
    """

    if start_ms < 0 or end_ms <= start_ms:
        raise ValueError("Invalid timestamp range")

    symbol = symbol.upper()

    all_events = []
    seen_times = set()
    cursor = start_ms

    while cursor <= end_ms:

        params = urllib.parse.urlencode({
            "symbol": symbol,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": LIMIT,
        })

        url = f"{BINANCE_FUNDING_URL}?{params}"

        batch = None

        for attempt in range(MAX_RETRIES):
            try:
                req = urllib.request.Request(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                )

                with urllib.request.urlopen(
                    req,
                    timeout=REQUEST_TIMEOUT,
                ) as resp:
                    batch = json.loads(resp.read().decode("utf-8"))

                if not isinstance(batch, list):
                    raise RuntimeError(
                        f"Unexpected Binance API response: {batch}"
                    )

                break

            except urllib.error.HTTPError as exc:
                if exc.code == 429 or exc.code >= 500:
                    wait = min(2 ** attempt, 30)
                    log.warning(
                        "Binance HTTP %s. Retrying in %s seconds",
                        exc.code,
                        wait,
                    )
                    time.sleep(wait)
                    continue
                raise

            except (
                urllib.error.URLError,
                TimeoutError,
                json.JSONDecodeError,
            ) as exc:
                wait = min(2 ** attempt, 30)
                log.warning(
                    "Request failed: %s. Retrying in %s seconds",
                    exc,
                    wait,
                )
                time.sleep(wait)

        if batch is None:
            raise RuntimeError(
                f"Failed to fetch funding history at timestamp {cursor}"
            )

        if not batch:
            break

        batch.sort(key=lambda x: int(x["fundingTime"]))

        for ev in batch:
            timestamp = int(ev["fundingTime"])

            if start_ms <= timestamp <= end_ms:
                if timestamp not in seen_times:
                    all_events.append(ev)
                    seen_times.add(timestamp)

        last_timestamp = int(batch[-1]["fundingTime"])

        if last_timestamp < cursor:
            raise RuntimeError("Binance pagination moved backwards")

        if last_timestamp >= end_ms:
            break

        # Advance beyond the final event to avoid duplicate pagination.
        cursor = last_timestamp + 1

        time.sleep(REQUEST_DELAY)

    all_events.sort(key=lambda x: int(x["fundingTime"]))

    return all_events


def build_funding_cache(
    symbol: str,
    start_ms: int,
    end_ms: int,
    out_path: str = "logs/funding_rates.jsonl",
) -> int:
    """
    Download and save funding rates.

    Missing mark prices are saved as null, not zero.
    Returns the number of valid funding events.
    """

    directory = os.path.dirname(out_path)

    if directory:
        os.makedirs(directory, exist_ok=True)

    events = fetch_funding_range(symbol, start_ms, end_ms)

    missing_mark_prices = 0
    invalid_rates = 0
    written = 0

    with open(out_path, "w", encoding="utf-8") as f:

        for ev in events:

            timestamp = int(ev["fundingTime"])
            rate = safe_float(ev.get("fundingRate"))

            # Funding rate is essential. Do not save invalid events.
            if rate is None:
                invalid_rates += 1
                continue

            mark_price = safe_float(ev.get("markPrice"))

            if mark_price is None:
                missing_mark_prices += 1

            record = {
                "time_ms": timestamp,
                "rate": rate,
                "mark_price": mark_price,
            }

            f.write(json.dumps(record) + "\n")
            written += 1

    log.info(
        "Funding cache saved: %s events | Missing mark prices: %s | "
        "Invalid rates: %s",
        written,
        missing_mark_prices,
        invalid_rates,
    )

    return written


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Download Binance USD-M funding history"
    )

    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--days", type=int, default=1100)
    parser.add_argument(
        "--output",
        default="logs/funding_rates.jsonl",
    )

    args = parser.parse_args()

    end_ms = int(time.time() * 1000)
    start_ms = end_ms - args.days * 86400 * 1000

    count = build_funding_cache(
        args.symbol,
        start_ms,
        end_ms,
        args.output,
    )

    expected = args.days * 3

    print("\n========== FUNDING DOWNLOAD ==========")
    print(f"Symbol:          {args.symbol}")
    print(f"Days requested:  {args.days}")
    print(f"Events received: {count}")
    print(f"Expected approx: {expected}")
    print(f"Output:          {args.output}")
    print("======================================")
