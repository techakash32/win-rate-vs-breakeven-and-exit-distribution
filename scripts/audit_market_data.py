from pathlib import Path
from collections import Counter
import json, csv, re
from datetime import datetime, timezone, timedelta

ROOT = Path("logs/backtest/bars")
OUT = Path("logs/backtest/data_audit")
OUT.mkdir(parents=True, exist_ok=True)

results = []
daily = {}

for p in sorted(ROOT.glob("BTCUSDT-*-b10-i*.jsonl")):
    m = re.search(r"BTCUSDT-(\d{4}-\d{2}-\d{2})-b10-i(\d+)\.jsonl$", p.name)
    if not m:
        continue

    day = m.group(1)
    interval = int(m.group(2))
    expected = 86400000 // interval
    timestamps = []
    errors = 0

    with p.open(encoding="utf-8-sig") as f:
        for line in f:
            try:
                obj = json.loads(line)
                t = next(
                    obj[k] for k in
                    ("open_time", "openTime", "timestamp", "time", "t")
                    if k in obj
                )
                t = int(float(t))
                if t < 10**11:
                    t *= 1000
                timestamps.append(t)
            except Exception:
                errors += 1

    unique = sorted(set(timestamps))
    duplicates = len(timestamps) - len(unique)

    gaps = [
        (b-a)//interval - 1
        for a,b in zip(unique, unique[1:])
        if b-a > interval and (b-a) % interval == 0
    ]

    invalid_steps = sum(
        1 for a,b in zip(unique, unique[1:])
        if (b-a) % interval != 0
    )

    missing_inside = sum(gaps)
    missing_expected = max(0, expected-len(unique))

    record = {
        "date": day,
        "interval_ms": interval,
        "bars": len(timestamps),
        "unique_bars": len(unique),
        "expected_bars": expected,
        "missing_expected": missing_expected,
        "missing_inside_gaps": missing_inside,
        "duplicates": duplicates,
        "invalid_timestamp_steps": invalid_steps,
        "parse_errors": errors,
        "first_utc": datetime.fromtimestamp(unique[0]/1000, timezone.utc).isoformat() if unique else "",
        "last_utc": datetime.fromtimestamp(unique[-1]/1000, timezone.utc).isoformat() if unique else "",
    }

    results.append(record)
    daily.setdefault(day, {})[interval] = record

with open(OUT/"corrected_bar_audit.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=results[0].keys() if results else [])
    if results:
        w.writeheader()
        w.writerows(results)

print("\n========== CORRECTED BAR AUDIT ==========")

for r in results:
    status = "OK" if (
        r["unique_bars"] == r["expected_bars"]
        and r["duplicates"] == 0
        and r["parse_errors"] == 0
        and r["invalid_timestamp_steps"] == 0
    ) else "CHECK"

    print(
        f'{status} {r["date"]} | {r["interval_ms"]//60000}m'
        f' | bars={r["unique_bars"]}/{r["expected_bars"]}'
        f' | missing={r["missing_expected"]}'
        f' | duplicates={r["duplicates"]}'
        f' | errors={r["parse_errors"]}'
    )

print("\n========== MISSING DATE CHECK ==========")

dates = sorted(daily)
if dates:
    start = datetime.strptime(dates[0], "%Y-%m-%d").date()
    end = datetime.strptime(dates[-1], "%Y-%m-%d").date()
    d = start
    while d <= end:
        key = d.isoformat()
        if key not in daily:
            print("MISSING ENTIRE DATE:", key)
        d += timedelta(days=1)

print("\n========== SUMMARY ==========")
print("Daily bar files:", len(results))
print("Unique dates:", len(daily))
print("Audit saved:", OUT/"corrected_bar_audit.csv")
