"""
Configuration for the multi-symbol Confirmation Engine and backtest.

Timeframe-driven. Change INTERVAL and everything scales (parameters, fee ratio,
target/stop distances). Venue selection controls fees. Fee override for
diagnostics is at the BOTTOM so it is not overwritten.

Defaults below are for 1h bars; strategy-specific trend mode replaces fixed-R
exits with ATR sizing and a trailing stop.
"""
import os
import math


# =====================================================================
# Instrument and timeframe
# =====================================================================

_DEFAULT_SYMBOLS = "BTCUSDT,ETHUSDT,XRPUSDT,ADAUSDT"
SYMBOLS = [s.strip().upper() for s in os.environ.get("SYMBOLS", _DEFAULT_SYMBOLS).split(",") if s.strip()]
if not SYMBOLS:
    raise ValueError("SYMBOLS must contain at least one comma-separated symbol")
INTERVAL = os.environ.get("BACKTEST_INTERVAL", "4h")

_UNIT_MS = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
INTERVAL_MS = int(INTERVAL[:-1]) * _UNIT_MS[INTERVAL[-1]]

# Timeframe-scaled parameters. Each entry sets:
#   stop_pct:   stop distance as % of price
#   target_r:   target multiple of stop (2.0 = 1:2, 3.0 = 1:3)
#   hold:       max bars held
#   lookback:   sweep swing lookback (in bars of this timeframe)
#   penetration: minimum wick-through-swing to count as a sweep
#
# The stop/target pairs are calibrated so P(target hit) is achievable on each
# timeframe. Daily bars support 1:3 R:R at realistic win rates; lower
# timeframes do not.
_PARAMS = {
    "5m":  {"stop_pct": 0.6, "target_r": 1.0, "hold": 45, "lookback": 20, "penetration": 0.001},
    "15m": {"stop_pct": 1.0, "target_r": 1.5, "hold": 32, "lookback": 20, "penetration": 0.002},
    "1h":  {"stop_pct": 2.0, "target_r": 3.0, "hold": 24, "lookback": 20, "penetration": 0.005},
    # 0.0075 is a 0.75% fractional price penetration, between 1h and 4h defaults.
    "2h":  {"stop_pct": 3.0, "target_r": 3.0, "hold": 18, "lookback": 20, "penetration": 0.0075},
    "4h":  {"stop_pct": 4.0, "target_r": 3.0, "hold": 12, "lookback": 20, "penetration": 0.010},
    "1d":  {"stop_pct": 5.0, "target_r": 3.0, "hold":  5, "lookback": 20, "penetration": 0.020},
}
if INTERVAL not in _PARAMS:
    raise ValueError(f"Unsupported BACKTEST_INTERVAL {INTERVAL!r}; supported: {', '.join(_PARAMS)}")
_p = _PARAMS[INTERVAL]

PAPER_STOP_PCT = _p["stop_pct"]
PAPER_TARGET_R_MULTIPLE = _p["target_r"]
PAPER_MAX_HOLD_BARS = _p["hold"]
SWEEP_LOOKBACK_BARS = _p["lookback"]
SWEEP_MIN_PENETRATION_PCT = _p["penetration"]


# =====================================================================
# Venue fees (choose exchange)
# =====================================================================

VENUE = os.environ.get("VENUE", "binance").strip().lower()

_VENUES = {
    # spot-maker-spot-taker typical USD-M futures round trip; adjust if you
    # have a VIP tier or special arrangement. Slippage is the observed or
    # assumed round-trip friction for next-bar open entries.
    "binance":       {"taker": 0.050, "maker": 0.020, "slippage": 0.015},
    "bybit":         {"taker": 0.055, "maker": 0.020, "slippage": 0.020},
    "okx":           {"taker": 0.050, "maker": 0.020, "slippage": 0.025},
    "binance_vip":   {"taker": 0.020, "maker": 0.020, "slippage": 0.010},  # $50M+ 30d volume
    "bybit_vip":     {"taker": 0.030, "maker": 0.015, "slippage": 0.015},  # $5M+ 30d volume
    "zero_fee":      {"taker": 0.000, "maker": 0.000, "slippage": 0.000},  # diagnostic only
}
_v = _VENUES.get(VENUE, _VENUES["binance"])

PAPER_FEE_TAKER_PCT = _v["taker"]
PAPER_FEE_MAKER_PCT = _v["maker"]
PAPER_SLIPPAGE_PCT = float(os.environ.get("PAPER_SLIPPAGE_PCT", "0.04"))
if not math.isfinite(PAPER_SLIPPAGE_PCT) or PAPER_SLIPPAGE_PCT < 0:
    raise ValueError("PAPER_SLIPPAGE_PCT must be finite and nonnegative")


# =====================================================================
# Strategy selection
# =====================================================================

ACTIVE_STRATEGY = os.environ.get("ACTIVE_STRATEGY", "trend").strip().lower()
SWEEP_LONG_ONLY = os.environ.get("SWEEP_LONG_ONLY", "false").strip().lower() in {
    "1", "true", "yes", "on"
}
FUNDING_LOOKBACK_EVENTS = int(os.environ.get("FUNDING_LOOKBACK_EVENTS", "270"))
FUNDING_EXTREME_HIGH_PCT = float(os.environ.get("FUNDING_EXTREME_HIGH_PCT", "95"))
FUNDING_EXTREME_LOW_PCT = float(os.environ.get("FUNDING_EXTREME_LOW_PCT", "5"))
# With Bucket B enabled, choose "confirm" (same direction) or "oppose".
FUNDING_B_MODE = os.environ.get("FUNDING_B_MODE", "confirm").strip().lower()
TREND_DONCHIAN_LOOKBACK = int(os.environ.get("TREND_DONCHIAN_LOOKBACK", "40"))
TREND_ATR_WINDOW = int(os.environ.get("TREND_ATR_WINDOW", "14"))
TREND_ATR_MULTIPLE = float(os.environ.get("TREND_ATR_MULTIPLE", "2.0"))
TREND_LONDON_ONLY = os.environ.get("TREND_LONDON_ONLY", "true").strip().lower() in {
    "1", "true", "yes", "on"
}
if ACTIVE_STRATEGY == "trend":
    PAPER_STOP_PCT = 2.0  # used only as fallback outside ATR stop sizing
    PAPER_TARGET_R_MULTIPLE = 0.0
    PAPER_MAX_HOLD_BARS = 90

BUCKET_B_ENABLED = os.environ.get(
    "BUCKET_B_ENABLED", "False"
).strip().lower() in {"1", "true", "yes", "on"}


# =====================================================================
# WebSocket
# =====================================================================

WS_BASE = "wss://fstream.binance.com/market/stream?streams="
CONNECT_HOST = None                        # use DNS for current Binance endpoint IP
RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 30.0
LOG_RAW = True
RAW_LOG_PATH = "logs/raw_market.jsonl"
FINALIZE_GRACE_S = 0.5


# =====================================================================
# Volume Profile / Buckets
# =====================================================================

VP_BUCKET_SIZE = 10.0
VP_WINDOW_BARS = 30
VP_VA_PERCENT = 0.70
VP_EXCLUDE_CURRENT = True


# =====================================================================
# Bucket B (delta direction)
# =====================================================================

AGGR_WINDOW_BARS = 5
MIN_AGGRESSION_STRENGTH = 0.40
AGGR_RATIO_THRESHOLD = 0.60                  # for absorption (agreement)
AGGR_RATIO_THRESHOLD_SWEEP = 0.52            # for sweep (rejection)


# =====================================================================
# Gate
# =====================================================================

WARMUP_BARS = 30
COOLDOWN_BARS = 5
if ACTIVE_STRATEGY == "trend":
    COOLDOWN_BARS = int(os.environ.get("COOLDOWN_BARS", "0"))


# =====================================================================
# Persistence
# =====================================================================

BAR_HISTORY_PATH = "logs/bar_history.jsonl"
DECISIONS_LOG_PATH = "logs/decisions/gate.jsonl"
MAX_RESTORE_GAP_BARS = 10


# =====================================================================
# Preload / gap healing
# =====================================================================

PRELOAD_ENABLED = True
REST_BASE = "https://fapi.binance.com"
REST_TIMEOUT_S = 15
PRELOAD_REAL_TRADES = True
PRELOAD_TRADE_PAGE_DELAY_S = 0.55
PRELOAD_MAX_TRADE_CALLS = 150
PRELOAD_TRADE_RETRIES = 3
UPGRADE_APPROX_BARS = True
PRELOAD_REAL_MAX_VOL_ERR_PCT = 1.0
GATE_MAX_APPROX_VP_FRACTION = 0.0


# =====================================================================
# Bucket C (Deribit GEX)
# =====================================================================

GEX_ENABLED = True
GEX_SOURCE = "deribit"
DERIBIT_BASE = "https://www.deribit.com/api/v2"
DERIBIT_CURRENCY = "BTC"
DERIBIT_REFRESH_SECONDS = 300
DERIBIT_MAX_DTE_DAYS = 45
DERIBIT_TIMEOUT_S = 15
GEX_WALL_PROXIMITY_PCT = 0.30

# Compatibility aliases retained for existing GEX polling/cache components.
GEX_SYMBOL = "BTCUSDT"
GEX_REFRESH_SECONDS = DERIBIT_REFRESH_SECONDS
GEX_MAX_CALLS_PER_DAY = 4
GEX_CHECK_SECONDS = DERIBIT_REFRESH_SECONDS
GEX_CACHE_PATH = "logs/gex_cache.json"
GEX_STALE_FACTOR = 2.0


# =====================================================================
# Groq LLM (optional narrative layer)
# =====================================================================

GROQ_MODEL = "openai/gpt-oss-120b"
GROQ_REASONING_EFFORT = "low"
GROQ_MAX_COMPLETION_TOKENS = 800
GROQ_EST_TOKENS_PER_CALL = 1100
GROQ_MAX_RPM = 25
GROQ_MAX_RPD = 900
GROQ_MAX_TPM = 7000
GROQ_MAX_TOKENS_PER_DAY = 180000
GROQ_QUEUE_MAX = 50
GROQ_TIMEOUT_SECONDS = 30


# =====================================================================
# Paper execution
# =====================================================================

PAPER_START_EQUITY = 10000.0
PAPER_RISK_PER_TRADE_PCT = 0.5
PAPER_STALE_BARS = 60
PAPER_STATE_PATH = "logs/paper/state.json"
PAPER_TRADES_LOG = "logs/paper/trades.jsonl"
LLM_LOG_PATH = "logs/llm/rationales.jsonl"
PAPER_BREAKEVEN_AT_R = 0.0


# =====================================================================
# Regime filter (off by default; test separately if needed)
# =====================================================================

ATR_FILTER_ENABLED = False
ATR_MIN_PCT = 0.15
ATR_WINDOW_BARS = 14


# =====================================================================
# Absorption params (kept for reference; ACTIVE_STRATEGY controls which runs)
# =====================================================================

ABSORPTION_LEVEL_TOLERANCE = 0.003
ABSORPTION_DELTA_BASELINE_BARS = 60
ABSORPTION_DELTA_RATIO_MIN = 1.5
ABSORPTION_DELTA_RATIO_MAX = 2.0
ABSORPTION_RANGE_BASELINE_BARS = 20
ABSORPTION_COMPRESSION_MIN = 0.0
ABSORPTION_COMPRESSION_MAX = 0.6


# =====================================================================
# Historical gap filling (backtests only)
# =====================================================================

FILL_GAPS = bool(int(os.getenv("FILL_GAPS", "0")))
FILL_GAP_MODE = os.getenv("FILL_GAP_MODE", "replay")
FILL_GAP_MAX_BARS = int(os.getenv("FILL_GAP_MAX_BARS", "288"))


# =====================================================================
# DIAGNOSTIC FEE OVERRIDE — must be at the very bottom
# =====================================================================
# This is the only place PAPER_FEE_* is allowed to be overridden. Everything
# above defines the real values; this block lets test runs temporarily use
# alternative fees for what-if analysis. If FEE_OVERRIDE is not set, this
# block does nothing and the values from the VENUE block above remain.

if os.environ.get("FEE_OVERRIDE", "false").lower() == "true":
    PAPER_FEE_TAKER_PCT = float(os.environ.get("FEE_TAKER", str(PAPER_FEE_TAKER_PCT)))
    PAPER_FEE_MAKER_PCT = float(os.environ.get("FEE_MAKER", str(PAPER_FEE_MAKER_PCT)))
