# config/settings.py
SYMBOLS = ["BTCUSDT"]
INTERVAL = "1m"

# Bar length in ms, derived from INTERVAL (must match what the kline stream uses)
_UNIT_MS = {"m": 60_000, "h": 3_600_000}
INTERVAL_MS = int(INTERVAL[:-1]) * _UNIT_MS[INTERVAL[-1]]

# Binance USD-S-M futures combined stream endpoint.
# kline and aggTrade both live on the /market route since the 2026-04-23 split.
WS_BASE = "wss://fstream.binance.com/market/stream?streams="

# Optional fixed IP to connect to (TLS/SNI and Host header still use
# fstream.binance.com). Set to None to use normal DNS.
CONNECT_HOST = "16.76.169.211"

# Reconnection behavior
RECONNECT_BASE_DELAY = 1.0     # seconds
RECONNECT_MAX_DELAY = 30.0     # seconds

# Raw payload logging (for replay / backtesting later).
# Every aggTrade is logged, so this file grows fast.
LOG_RAW = True
RAW_LOG_PATH = "logs/raw_market.jsonl"

# After a kline closes, wait this long before snapshotting the bar's delta,
# so any aggTrade still in flight for that minute is included.
FINALIZE_GRACE_S = 0.5

# ---- Step 3: Volume Profile / Buckets ----
VP_BUCKET_SIZE = 10.0        # $ per price bucket for BTC
VP_WINDOW_BARS = 30          # rolling VP window (use 240 in prod; 30 for first test)
VP_VA_PERCENT = 0.70
# Build the profile from bars BEFORE the one that just closed, so the bar being
# judged doesn't widen the very value area it is compared against.
VP_EXCLUDE_CURRENT = True

# ---- Bucket B ----
AGGR_WINDOW_BARS = 5
MIN_AGGRESSION_STRENGTH = 0.40
# Bucket B direction threshold. strength = (ratio - 0.5) * 4, so a strength of
# 0.40 means ratio 0.60. Keep this consistent with MIN_AGGRESSION_STRENGTH
# (0.5 + MIN_AGGRESSION_STRENGTH / 4) so B never prints a direction the gate
# then rejects as "weak".
AGGR_RATIO_THRESHOLD = 0.60

# ---- Gate ----
WARMUP_BARS = 30             # must be <= VP_WINDOW_BARS
COOLDOWN_BARS = 5            # after a TRADE, minimum bars before another

# ---- Persistence ----
BAR_HISTORY_PATH = "logs/bar_history.jsonl"
DECISIONS_LOG_PATH = "logs/decisions/gate.jsonl"
# On restart, saved history older than this many bars is discarded
# (stale bars would be stitched onto a different market).
MAX_RESTORE_GAP_BARS = 10

# ---- Preload / gap healing (REST) ----
# Fetch recent closed 1m klines from Binance REST at startup (and to fill gaps)
# so warmup disappears. REST gives exact taker-buy volume but NO per-price
# volume, so preloaded bars use an approximate (uniform) volume profile.
PRELOAD_ENABLED = True
REST_BASE = "https://fapi.binance.com"
REST_TIMEOUT_S = 15
# Build preloaded/healed bars from REAL aggTrades (REST) instead of spreading
# volume uniformly. aggTrades costs weight 20/call (limit 2400/min), so calls
# are paced; newest minutes are fetched first so a call cap only costs the
# OLDEST bars their real profile (those fall back to the approximation).
PRELOAD_REAL_TRADES = True
PRELOAD_TRADE_PAGE_DELAY_S = 0.55
PRELOAD_MAX_TRADE_CALLS = 150
PRELOAD_TRADE_RETRIES = 3            # per-request retries with backoff before a bar falls back
# While live, keep trying to replace approximated bars in the history with real ones.
UPGRADE_APPROX_BARS = True
PRELOAD_REAL_MAX_VOL_ERR_PCT = 1.0   # real bar accepted only if its volume matches the kline within this %
# Never trade while the volume profile contains approximated bars.
# 0.0 = none allowed (observation only until the profile is real); 1.0 = ignore.
GATE_MAX_APPROX_VP_FRACTION = 0.0

# ---- Step 4: Bucket C (GEX) ----
GEX_ENABLED = True
GEX_SOURCE = "deribit"          # "deribit" (BTC-native, no key) or "flashalpha" (SPY; not BTC-comparable)
GEX_SYMBOL = "SPY"              # only used when GEX_SOURCE = "flashalpha"
GEX_WALL_PROXIMITY_PCT = 0.30   # 0.30% = "near wall" for the veto
# FlashAlpha FREE plan = 5 requests/day, and one refresh costs 2 calls
# (gex + exposure_levels). So: at most 2 refreshes/day, cached on disk.
GEX_REFRESH_SECONDS = 43200     # refresh when the cached snapshot is older than this
GEX_MAX_CALLS_PER_DAY = 4       # leave 1 call of headroom under the free quota
GEX_CHECK_SECONDS = 300         # how often to check whether a refresh is due
GEX_CACHE_PATH = "logs/gex_cache.json"
GEX_STALE_FACTOR = 2.0          # snapshot older than FACTOR x refresh => treated as inactive

# ---- Step 4: Groq interpreter ----
GROQ_MODEL = "openai/gpt-oss-120b"
GROQ_REASONING_EFFORT = "low"   # gpt-oss is a reasoning model: keep thinking cheap
GROQ_MAX_COMPLETION_TOKENS = 800  # must cover reasoning + the JSON answer
GROQ_EST_TOKENS_PER_CALL = 1100   # pre-check: ~250 prompt + up to 800 completion
GROQ_MAX_RPM = 25               # free tier is 30; leave headroom
GROQ_MAX_RPD = 900              # free tier is 1000; leave headroom
GROQ_MAX_TPM = 7000             # free tier is 8000
GROQ_MAX_TOKENS_PER_DAY = 180000
GROQ_QUEUE_MAX = 50             # drop oldest if interpreter falls behind
GROQ_TIMEOUT_SECONDS = 30

# ---- Step 4: Paper execution ----
PAPER_START_EQUITY = 10000.0
PAPER_RISK_PER_TRADE_PCT = 0.5  # 0.5% of equity per trade
PAPER_STOP_PCT = 0.30           # 0.30% adverse move = stop
PAPER_TARGET_R_MULTIPLE = 2.0   # 2R target
PAPER_FEE_TAKER_PCT = 0.05      # market orders: entry at next open, STOP, EOD/stale closes
PAPER_FEE_MAKER_PCT = 0.02      # resting limit: the TARGET exit (assumes it fills when touched)
PAPER_STALE_BARS = 60           # open position older than this at restart => STALE_ON_RESTART
PAPER_STATE_PATH = "logs/paper/state.json"   # equity + open/pending position, survives restarts
PAPER_TRADES_LOG = "logs/paper/trades.jsonl"
LLM_LOG_PATH = "logs/llm/rationales.jsonl"

# ---- Deribit GEX (Bucket C, BTC-native) ----
DERIBIT_BASE = "https://www.deribit.com/api/v2"
DERIBIT_CURRENCY = "BTC"
DERIBIT_REFRESH_SECONDS = 300     # public endpoint, no key
DERIBIT_MAX_DTE_DAYS = 45         # ignore long-dated options (negligible gamma, distort walls)
DERIBIT_TIMEOUT_S = 15

# ---- Paper trader: exits ----
PAPER_MAX_HOLD_BARS = 30          # TIME_STOP: close at the bar close after this many bars (0 = off)
PAPER_BREAKEVEN_AT_R = 0.0        # move stop to entry once +N R is reached (0 = off; applies from the NEXT bar)