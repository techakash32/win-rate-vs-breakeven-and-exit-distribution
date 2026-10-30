# data/deribit_gex.py
"""
Bucket C - BTC-native dealer overlay from Deribit options (public API, no key).

Deribit's book summary gives, per option: open_interest (in BTC), mark_iv (%),
underlying_price (forward) - but NOT greeks, strike or expiry. So strike/expiry
are parsed from the instrument name (e.g. BTC-28AUG26-80000-C) and gamma is
computed with Black-Scholes (r = 0) from mark_iv.

ASSUMPTION (stated, not measured): dealers are net long calls / short puts, so
call gamma counts positive and put gamma negative. Real dealer positioning is
not observable from open interest alone; treat levels as a heuristic.

Outputs (all in USD of BTC price):
  net_gex      dollar gamma per 1% move at spot (sign gives the regime)
  gamma_flip   spot level where net gex crosses zero (None if no crossing)
  call_wall    strike ABOVE spot with the largest call gex  (resistance-like)
  put_wall     strike BELOW spot with the largest put gex   (support-like)
"""
import asyncio
import json
import logging
import math
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from config import settings
from gate.buckets import BucketSignal, Dir

log = logging.getLogger(__name__)

YEAR_S = 365.0 * 86400.0


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def bs_gamma(f: float, k: float, t: float, iv: float) -> float:
    """Black-Scholes gamma (r=0) per $1 move of the underlying."""
    if f <= 0 or k <= 0 or t <= 0 or iv <= 0:
        return 0.0
    sd = iv * math.sqrt(t)
    d1 = (math.log(f / k) + 0.5 * sd * sd) / sd
    return _norm_pdf(d1) / (f * sd)


def parse_instrument(name: str):
    """'BTC-28AUG26-80000-C' -> (expiry_ts_utc, strike, 'C'|'P') or None."""
    try:
        _cur, exp, strike, kind = name.split("-")
        dt = datetime.strptime(exp.upper(), "%d%b%y").replace(
            hour=8, tzinfo=timezone.utc)                    # Deribit expires 08:00 UTC
        return dt.timestamp(), float(strike.replace("d", ".")), kind.upper()
    except (ValueError, TypeError):
        return None


def load_chain(summaries: list, now_ts: float, max_dte_days: float) -> list:
    """Keep options with usable OI / IV / expiry; returns plain tuples."""
    chain = []
    for s in summaries:
        parsed = parse_instrument(s.get("instrument_name", ""))
        if not parsed:
            continue
        exp_ts, strike, kind = parsed
        t_years = (exp_ts - now_ts) / YEAR_S
        oi = s.get("open_interest") or 0.0
        iv = (s.get("mark_iv") or 0.0) / 100.0
        fwd = s.get("underlying_price") or 0.0
        if t_years <= 0 or (exp_ts - now_ts) / 86400.0 > max_dte_days:
            continue
        if oi <= 0 or iv <= 0 or fwd <= 0 or kind not in ("C", "P"):
            continue
        chain.append((strike, kind, t_years, iv, fwd, float(oi)))
    return chain


def _net_gex_at(chain: list, spot0: float, spot: float) -> float:
    """Net dollar gamma per 1% move if spot were `spot` (forwards scale with spot)."""
    scale = spot / spot0
    total = 0.0
    for strike, kind, t, iv, fwd, oi in chain:
        g = bs_gamma(fwd * scale, strike, t, iv)
        gex = g * oi * spot * spot * 0.01
        total += gex if kind == "C" else -gex
    return total


def compute_gex(summaries: list, spot: float, now_ts: float,
                max_dte_days: float = 45.0) -> dict:
    chain = load_chain(summaries, now_ts, max_dte_days)
    if not chain or spot <= 0:
        return {}

    call_gex: dict[float, float] = {}
    put_gex: dict[float, float] = {}
    for strike, kind, t, iv, fwd, oi in chain:
        gex = bs_gamma(fwd, strike, t, iv) * oi * spot * spot * 0.01
        book = call_gex if kind == "C" else put_gex
        book[strike] = book.get(strike, 0.0) + gex

    net = sum(call_gex.values()) - sum(put_gex.values())

    above = {k: v for k, v in call_gex.items() if k > spot}
    below = {k: v for k, v in put_gex.items() if k < spot}
    call_wall = max(above, key=above.get) if above else None
    put_wall = max(below, key=below.get) if below else None

    # gamma flip: scan +-20% around spot for the zero crossing nearest to spot
    flip, prev_s, prev_v = None, None, None
    best = None
    for i in range(-40, 41):
        s_i = spot * (1.0 + i * 0.005)
        v_i = _net_gex_at(chain, spot, s_i)
        if prev_v is not None and (prev_v < 0) != (v_i < 0):
            x = prev_s + (s_i - prev_s) * (0.0 - prev_v) / (v_i - prev_v)
            if best is None or abs(x - spot) < abs(best - spot):
                best = x
        prev_s, prev_v = s_i, v_i
    flip = best

    return {
        "spot": spot, "net_gex": net,
        "regime": "positive_gamma" if net >= 0 else "negative_gamma",
        "gamma_flip": flip, "call_wall": call_wall, "put_wall": put_wall,
        "n_options": len(chain),
    }


class DeribitGexCache:
    """Same interface as GexCache so the runner can use either."""

    def __init__(self, currency=None):
        self.currency = currency or settings.DERIBIT_CURRENCY
        self.symbol = f"{self.currency}-Deribit"
        self._data: dict = {}
        self._fetched_at = 0.0
        self._failures = 0
        self._next_try = 0.0
        self._last_error = None

    # ---- availability (no key needed) ----

    def enabled(self) -> bool:
        return bool(settings.GEX_ENABLED)

    def disabled_reason(self):
        return None if settings.GEX_ENABLED else "GEX_ENABLED = False"

    # ---- fetching ----

    @staticmethod
    def _get(path: str, params: dict):
        url = f"{settings.DERIBIT_BASE}{path}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"User-Agent": "confirmation-engine/1.0"})
        with urllib.request.urlopen(req, timeout=settings.DERIBIT_TIMEOUT_S) as r:
            body = json.loads(r.read().decode("utf-8"))
        if "result" not in body:
            raise RuntimeError(f"Deribit error: {body.get('error')}")
        return body["result"]

    def _fetch_sync(self) -> dict:
        summaries = self._get("/public/get_book_summary_by_currency",
                              {"currency": self.currency, "kind": "option"})
        spot = float(self._get("/public/get_index_price",
                               {"index_name": f"{self.currency.lower()}_usd"})["index_price"])
        data = compute_gex(summaries, spot, time.time(), settings.DERIBIT_MAX_DTE_DAYS)
        if not data:
            raise RuntimeError("no usable options in Deribit response")
        return data

    async def refresh(self) -> bool:
        if not self.enabled():
            return False
        try:
            self._data = await asyncio.to_thread(self._fetch_sync)
            self._fetched_at = time.time()
            self._failures = 0
            self._last_error = None
            log.info("Deribit GEX: %s", self._describe())
            return True
        except Exception as exc:
            self._failures += 1
            self._last_error = f"{type(exc).__name__}: {exc}"
            backoff = min(60 * 2 ** (self._failures - 1), 1800)
            self._next_try = time.time() + backoff
            log.warning("Deribit GEX refresh failed (%s); retry in %ds, keeping the "
                        "previous snapshot until it goes stale.", self._last_error, backoff)
            return False

    async def run(self) -> None:
        while True:
            if self.enabled() and time.time() >= self._next_try \
                    and (time.time() - self._fetched_at) >= settings.DERIBIT_REFRESH_SECONDS:
                await self.refresh()
            await asyncio.sleep(5)

    # ---- bucket output ----

    def age_seconds(self):
        return (time.time() - self._fetched_at) if self._fetched_at else None

    def is_stale(self) -> bool:
        age = self.age_seconds()
        return age is None or age > settings.GEX_STALE_FACTOR * settings.DERIBIT_REFRESH_SECONDS

    def _describe(self) -> str:
        d = self._data
        if not d:
            return f"{self.symbol} GEX unavailable"
        f = d.get("gamma_flip")
        return (f"{self.symbol} spot={d['spot']:.0f} net_gex={d['net_gex']:+.3e} "
                f"regime={d['regime']} "
                f"flip={'%.0f' % f if f else 'none'} "
                f"call_wall={'%.0f' % d['call_wall'] if d.get('call_wall') else 'none'} "
                f"put_wall={'%.0f' % d['put_wall'] if d.get('put_wall') else 'none'}")

    def _levels(self, close=None) -> dict:
        """Structured levels + % distance from `close`, for later replay/analysis."""
        d = self._data
        out = {"net_gex": d.get("net_gex"), "regime": d.get("regime"),
               "flip": d.get("gamma_flip"), "call_wall": d.get("call_wall"),
               "put_wall": d.get("put_wall"), "spot": d.get("spot")}
        if close:
            for key in ("flip", "call_wall", "put_wall"):
                v = out[key]
                out[f"dist_{key}_pct"] = round((v - close) / close * 100.0, 3) if v else None
        return out

    def inactive_reason(self, close=None):
        if not self._data:
            return "no GEX data yet" + (f" (last error: {self._last_error})" if self._last_error else "")
        if self.is_stale():
            return f"GEX stale (age={self.age_seconds() / 60:.0f}min)"
        return None

    def snapshot(self, close=None) -> BucketSignal:
        """Always NEUTRAL (C vetoes, never confirms); `active` = veto can act now."""
        why = self.inactive_reason(close)
        if why:
            reason = f"INACTIVE: {why}"
            if self._data:
                reason += f" | {self._describe()}"
            return BucketSignal("C", Dir.NEUTRAL, 0.0, reason, active=False,
                                extra=self._levels(close) if self._data else None)
        return BucketSignal("C", Dir.NEUTRAL, 0.0,
                            f"{self._describe()} [age {self.age_seconds():.0f}s]",
                            active=True, extra=self._levels(close))

    def veto(self, direction, close, proximity_pct=None):
        why = self.inactive_reason(close)
        if why:
            return False, f"INACTIVE: {why}"

        d = self._data
        prox = proximity_pct or settings.GEX_WALL_PROXIMITY_PCT
        if d["net_gex"] < 0:
            return False, "negative gamma - amplifying, no veto"

        call_wall, put_wall = d.get("call_wall"), d.get("put_wall")
        if direction == Dir.LONG and call_wall:
            dist = (call_wall - close) / close * 100.0
            if 0 <= dist <= prox:
                return True, (f"call wall {call_wall:.0f} is {dist:.2f}% above close "
                              f"{close:.0f} (positive gamma caps upside)")
        if direction == Dir.SHORT and put_wall:
            dist = (close - put_wall) / close * 100.0
            if 0 <= dist <= prox:
                return True, (f"put wall {put_wall:.0f} is {dist:.2f}% below close "
                              f"{close:.0f} (positive gamma caps downside)")
        return False, "checked: no wall within proximity"

    def stats(self) -> dict:
        return {"symbol": self.symbol, "failures": self._failures,
                "age_s": round(self.age_seconds()) if self._fetched_at else None,
                "last_error": self._last_error}