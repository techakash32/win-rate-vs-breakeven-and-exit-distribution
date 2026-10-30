# data/gex_cache.py
"""
Bucket C - dealer-positioning overlay (veto only, never a trigger).

IMPORTANT limits of this bucket:
1. FlashAlpha covers US equities/ETFs (e.g. SPY), not BTC. SPY walls are in
   SPY dollars (~hundreds), BTC trades in the tens of thousands, so a wall
   "within 0.3% of close" is meaningless across the two. The wall veto
   therefore refuses to compare levels whose price scale doesn't match the
   traded instrument; with SPY it never fires, and Bucket C is informational.
   It becomes live only if GEX_SYMBOL is swapped for a BTC-denominated source.
2. FlashAlpha's FREE plan allows 5 requests/day and one refresh costs 2, so
   this cache is disk-backed, refreshes rarely, and counts calls per UTC day.
"""
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

from config import settings
from gate.buckets import BucketSignal, Dir

log = logging.getLogger(__name__)

CALLS_PER_REFRESH = 2   # fa.gex() + fa.exposure_levels()


def _num(x):
    """float(x) or None (FlashAlpha levels are nullable)."""
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _next_utc_midnight() -> float:
    now = datetime.now(timezone.utc)
    nxt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return nxt.timestamp() + 86400


class GexCache:
    def __init__(self, symbol=None):
        self.symbol = symbol or settings.GEX_SYMBOL
        self._fa = None
        self._data: dict = {}            # reduced snapshot (see _reduce)
        self._fetched_at = 0.0
        self._day = _utc_day()
        self._calls_today = 0
        self._next_try = 0.0
        self._last_error = None
        self._disabled_reason = None
        self._warned = False
        self._load_disk()

    # ---- availability ----

    def enabled(self) -> bool:
        if not settings.GEX_ENABLED:
            self._disabled_reason = "GEX_ENABLED = False"
        elif not os.environ.get("FLASHALPHA_API_KEY"):
            self._disabled_reason = "FLASHALPHA_API_KEY not set"
        else:
            self._disabled_reason = None
        return self._disabled_reason is None

    def disabled_reason(self):
        self.enabled()
        return self._disabled_reason

    # ---- disk cache ----

    def _load_disk(self) -> None:
        path = settings.GEX_CACHE_PATH
        try:
            with open(path, encoding="utf-8") as f:
                d = json.load(f)
            if d.get("symbol") != self.symbol:
                return
            self._data = d.get("data", {})
            self._fetched_at = float(d.get("fetched_at", 0.0))
            if d.get("day") == _utc_day():
                self._calls_today = int(d.get("calls_today", 0))
        except (OSError, ValueError):
            pass

    def _save_disk(self) -> None:
        path = settings.GEX_CACHE_PATH
        folder = os.path.dirname(path)
        if folder:
            os.makedirs(folder, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({
                "symbol": self.symbol, "fetched_at": self._fetched_at,
                "day": self._day, "calls_today": self._calls_today,
                "data": self._data,
            }, f)
        os.replace(tmp, path)

    # ---- fetching ----

    def _client(self):
        if self._fa is None:
            from flashalpha import FlashAlpha        # lazy: optional dependency
            self._fa = FlashAlpha(os.environ["FLASHALPHA_API_KEY"])
        return self._fa

    def _roll_day(self) -> None:
        today = _utc_day()
        if today != self._day:
            self._day = today
            self._calls_today = 0

    def _stale(self) -> bool:
        return (time.time() - self._fetched_at) >= settings.GEX_REFRESH_SECONDS

    def _budget_ok(self) -> bool:
        self._roll_day()
        return self._calls_today + CALLS_PER_REFRESH <= settings.GEX_MAX_CALLS_PER_DAY

    def _fetch_sync(self) -> dict:
        fa = self._client()
        self._calls_today += 1
        gex = fa.gex(self.symbol)
        self._calls_today += 1
        levels = fa.exposure_levels(self.symbol)
        return self._reduce(gex, levels)

    @staticmethod
    def _reduce(gex: dict, levels_resp: dict) -> dict:
        """Keep only the few scalars we need (gex() also returns per-strike data)."""
        lv = (levels_resp or {}).get("levels", {}) or {}
        net_gex = _num(gex.get("net_gex"))
        regime = gex.get("regime")
        if not regime and net_gex is not None:
            regime = "positive_gamma" if net_gex >= 0 else "negative_gamma"
        flip = _num(lv.get("gamma_flip", gex.get("gamma_flip")))
        status = lv.get("gamma_flip_status")
        if status not in (None, "available"):
            flip = None                      # withheld by the provider
        return {
            "net_gex": net_gex,
            "regime": regime or "unknown",
            "gamma_flip": flip,
            "gamma_flip_status": status,
            "call_wall": _num(lv.get("call_wall")),
            "put_wall": _num(lv.get("put_wall")),
        }

    async def refresh(self) -> bool:
        if not self.enabled():
            return False
        if not self._budget_ok():
            log.info("GEX refresh skipped: daily FlashAlpha budget used (%d/%d calls).",
                     self._calls_today, settings.GEX_MAX_CALLS_PER_DAY)
            self._next_try = _next_utc_midnight()
            return False
        try:
            self._data = await asyncio.to_thread(self._fetch_sync)
            self._fetched_at = time.time()
            self._last_error = None
            self._save_disk()
            log.info("GEX refreshed (%s): %s", self.symbol, self._describe())
            return True
        except Exception as exc:
            self._last_error = f"{type(exc).__name__}: {exc}"
            self._save_disk()                # persist the call counter
            kind = type(exc).__name__
            if kind in ("TierRestrictedError", "AuthenticationError"):
                # not a transient failure: retrying only burns quota
                self._next_try = time.time() + 86400
                log.warning("GEX disabled for 24h (%s: your FlashAlpha plan can't access %s).",
                            kind, self.symbol)
            elif kind == "RateLimitError":
                self._next_try = _next_utc_midnight()
                log.warning("GEX rate-limited (%s); next try after UTC midnight.", exc)
            else:
                self._next_try = time.time() + 1800
                log.warning("GEX refresh failed: %s (keeping previous snapshot; "
                            "retry in 30 min)", self._last_error)
            return False

    async def run(self) -> None:
        while True:
            if self.enabled() and self._stale() and time.time() >= self._next_try:
                await self.refresh()
            await asyncio.sleep(settings.GEX_CHECK_SECONDS)

    # ---- bucket output ----

    def _describe(self) -> str:
        d = self._data
        if not d:
            return f"{self.symbol} GEX unavailable"
        parts = [self.symbol]
        if d.get("net_gex") is not None:
            parts.append(f"net_gex={d['net_gex']:,.0f}")
        parts.append(f"regime={d.get('regime', 'unknown')}")
        if d.get("gamma_flip") is not None:
            parts.append(f"flip={d['gamma_flip']:.2f}")
        else:
            parts.append(f"flip=withheld({d.get('gamma_flip_status') or 'n/a'})")
        if d.get("call_wall") is not None:
            parts.append(f"call_wall={d['call_wall']:.2f}")
        if d.get("put_wall") is not None:
            parts.append(f"put_wall={d['put_wall']:.2f}")
        return "; ".join(parts)

    @staticmethod
    def _comparable(level, close) -> bool:
        """Is `level` on the same price scale as `close`? (SPY ~650 vs BTC ~80,000: no)"""
        return bool(level) and bool(close) and 0.5 <= level / close <= 2.0

    def age_seconds(self):
        return (time.time() - self._fetched_at) if self._fetched_at else None

    def is_stale(self) -> bool:
        age = self.age_seconds()
        return age is None or age > settings.GEX_STALE_FACTOR * settings.GEX_REFRESH_SECONDS

    def inactive_reason(self, close=None):
        """Why Bucket C cannot veto right now, or None if it can."""
        if not self._data:
            return f"disabled ({self._disabled_reason})" if not self.enabled() else "no GEX data yet"
        if self.is_stale():
            return f"GEX stale (age={self.age_seconds() / 3600:.1f}h)"
        if close is not None:
            walls = [w for w in (self._data.get("call_wall"), self._data.get("put_wall")) if w]
            if not any(self._comparable(w, close) for w in walls):
                return (f"wall veto n/a - {self.symbol} price scale != traded "
                        f"instrument (close {close:.0f})")
        return None

    def snapshot(self, close=None) -> BucketSignal:
        """Bucket C signal. Always NEUTRAL (C vetoes, never confirms); `active`
        says whether the veto can actually act right now."""
        why = self.inactive_reason(close)
        if why and not self._warned and "scale" in why:
            self._warned = True
            log.warning("Bucket C INACTIVE: %s. The system is effectively a "
                        "2-bucket gate + GEX audit overlay until a BTC-scale "
                        "source is wired.", why)
        if why:
            reason = f"INACTIVE: {why}"
            if self._data:
                reason += f" | {self._describe()}"
            return BucketSignal("C", Dir.NEUTRAL, 0.0, reason, active=False)
        return BucketSignal("C", Dir.NEUTRAL, 0.0,
                            f"{self._describe()} [age {self.age_seconds() / 3600:.1f}h]",
                            active=True)

    def veto(self, direction, close, proximity_pct=None):
        """
        (vetoed, reason). Vetoes only trades heading INTO a nearby dealer wall
        while dealers are long gamma. If it cannot act (no data / stale /
        wrong price scale) the reason says INACTIVE, never "no wall".
        """
        why = self.inactive_reason(close)
        if why:
            return False, f"INACTIVE: {why}"

        d = self._data
        prox = proximity_pct or settings.GEX_WALL_PROXIMITY_PCT
        net_gex = d.get("net_gex")
        if net_gex is not None and net_gex < 0:
            return False, "negative gamma - amplifying, no veto"

        call_wall, put_wall = d.get("call_wall"), d.get("put_wall")
        if direction == Dir.LONG and self._comparable(call_wall, close):
            dist = (call_wall - close) / close * 100.0
            if 0 <= dist <= prox:
                return True, (f"call wall {call_wall:.2f} is {dist:.2f}% above "
                              f"close {close:.2f} (positive gamma caps upside)")
        if direction == Dir.SHORT and self._comparable(put_wall, close):
            dist = (close - put_wall) / close * 100.0
            if 0 <= dist <= prox:
                return True, (f"put wall {put_wall:.2f} is {dist:.2f}% below "
                              f"close {close:.2f} (positive gamma caps downside)")
        return False, "checked: no wall within proximity"

    def stats(self) -> dict:
        return {
            "symbol": self.symbol, "calls_today": self._calls_today,
            "age_s": round(time.time() - self._fetched_at) if self._fetched_at else None,
            "last_error": self._last_error,
        }