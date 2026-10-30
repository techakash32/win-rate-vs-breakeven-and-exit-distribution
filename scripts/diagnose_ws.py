
import asyncio
import websockets

URLS = {
    "Candle only":
        "wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m",
    "Aggregate trades only":
        "wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade",
    "Both streams":
        "wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m/btcusdt@aggTrade",
}

async def test_connection(name, url):
    print(f"\nTesting: {name}")

    try:
        async with websockets.connect(url, open_timeout=15) as ws:
            print("CONNECTED")
            message = await asyncio.wait_for(ws.recv(), timeout=10)
            print("MESSAGE RECEIVED:", message[:300])

    except Exception as exc:
        print(f"FAILED: {type(exc).__name__}: {exc}")

async def main():
    for name, url in URLS.items():
        await test_connection(name, url)

if __name__ == "__main__":
    asyncio.run(main())
