# scripts/dashboard.py
import json
import os
import glob
import time
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from config import settings

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

STATE_DIR = "logs/paper_live"
TRADES_LOG = "logs/paper/trades.jsonl"
SIGNALS_LOG = f"{STATE_DIR}/signals.jsonl"
STARTING_EQUITY_PER_SYMBOL = 10000.0

def get_latest_data():
    total_equity = 0.0
    initial_equity = len(settings.SYMBOLS) * STARTING_EQUITY_PER_SYMBOL
    positions = []
    
    # Read state files for each symbol in the configured list
    for symbol in settings.SYMBOLS:
        file_path = os.path.join(STATE_DIR, f"state_{symbol}.json")
        if os.path.exists(file_path):
            try:
                with open(file_path) as fh:
                    data = json.load(fh)
                    eq = data.get("equity", STARTING_EQUITY_PER_SYMBOL)
                    if eq == 0: eq = STARTING_EQUITY_PER_SYMBOL # Failsafe
                    total_equity += eq
                    
                    if data.get("position"):
                        pos = data["position"]
                        pos["symbol"] = symbol
                        positions.append(pos)
            except Exception:
                # If file is corrupted, assume starting equity
                total_equity += STARTING_EQUITY_PER_SYMBOL
        else:
            # If file doesn't exist yet, assume starting equity
            total_equity += STARTING_EQUITY_PER_SYMBOL

    # Read closed trades for P&L and curve
    trades = []
    if os.path.exists(TRADES_LOG):
        with open(TRADES_LOG, "r") as f:
            for line in f:
                if line.strip():
                    trades.append(json.loads(line))
    
    # Read signals
    signals = []
    if os.path.exists(SIGNALS_LOG):
        with open(SIGNALS_LOG, "r") as f:
            for line in f:
                if line.strip():
                    signals.append(json.loads(line))
                    
    # Sort signals by time (newest first)
    signals = sorted(signals, key=lambda x: x.get("ts", 0), reverse=True)[:10]

    # Calculate true Portfolio Equity Curve
    # Start from initial equity and add/subtract PnL from each closed trade chronologically
    curve = []
    running_equity = initial_equity
    now = int(time.time() * 1000)
    
    if trades:
        trades_sorted = sorted(trades, key=lambda x: x["exit_ts"])
        for t in trades_sorted:
            running_equity += t.get("pnl", 0)
            curve.append({"ts": t["exit_ts"], "equity": running_equity})
    else:
        # Flat line if no trades yet
        for i in range(24, -1, -1):
            curve.append({"ts": now - (i * 3600 * 1000), "equity": initial_equity})

    return {
        "summary": {
            "total_equity": total_equity,
            "initial_equity": initial_equity,
            "total_pnl": total_equity - initial_equity,
            "total_trades": len(trades),
            "win_rate": (len([t for t in trades if t.get("pnl", 0) > 0]) / len(trades) * 100) if trades else 0
        },
        "positions": positions,
        "signals": signals,
        "equity_curve": curve
    }

@app.get("/api/dashboard")
def dashboard_data():
    return get_latest_data()

@app.get("/")
def serve_index():
    return FileResponse("dashboard/index.html")