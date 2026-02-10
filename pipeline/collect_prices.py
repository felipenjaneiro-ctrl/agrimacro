"""
AgriMacro v3.2 - Price Collector
Collects OHLCV data from Yahoo Finance (primary source)
IBKR provides real-time data when available (via collect_ibkr.py)
"""
import time

COMMODITIES = {
    "ZC": {"yahoo": "ZC=F", "name": "Corn"},
    "ZS": {"yahoo": "ZS=F", "name": "Soybeans"},
    "ZW": {"yahoo": "ZW=F", "name": "Wheat CBOT"},
    "KE": {"yahoo": "KE=F", "name": "Wheat KC"},
    "ZM": {"yahoo": "ZM=F", "name": "Soybean Meal"},
    "ZL": {"yahoo": "ZL=F", "name": "Soybean Oil"},
    "SB": {"yahoo": "SB=F", "name": "Sugar"},
    "KC": {"yahoo": "KC=F", "name": "Coffee"},
    "CT": {"yahoo": "CT=F", "name": "Cotton"},
    "CC": {"yahoo": "CC=F", "name": "Cocoa"},
    "OJ": {"yahoo": "OJ=F", "name": "Orange Juice"},
    "LE": {"yahoo": "LE=F", "name": "Live Cattle"},
    "GF": {"yahoo": "GF=F", "name": "Feeder Cattle"},
    "HE": {"yahoo": "HE=F", "name": "Lean Hogs"},
    "CL": {"yahoo": "CL=F", "name": "Crude Oil"},
    "NG": {"yahoo": "NG=F", "name": "Natural Gas"},
    "GC": {"yahoo": "GC=F", "name": "Gold"},
    "SI": {"yahoo": "SI=F", "name": "Silver"},
    "DX": {"yahoo": "DX=F", "name": "Dollar Index"},
}

def fetch_yahoo(symbol: str, days: int = 1500) -> list:
    """Fetch OHLCV data from Yahoo Finance"""
    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="5y")
        if df.empty:
            return []
        df = df.tail(days)
        records = []
        for date, row in df.iterrows():
            records.append({
                "date": date.strftime("%Y-%m-%d"),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": int(row["Volume"])
            })
        return records
    except Exception as e:
        print(f"  Yahoo error for {symbol}: {e}")
        return []

def collect_all_prices() -> dict:
    """Collect prices for all commodities from Yahoo Finance"""
    result = {}
    for sym, info in COMMODITIES.items():
        print(f"  Collecting {sym} ({info['name']})...")
        data = fetch_yahoo(info["yahoo"])
        if data:
            result[sym] = data
            print(f"    OK: {len(data)} candles")
        else:
            print(f"    FAILED: No data")
        time.sleep(0.5)
    return result

if __name__ == "__main__":
    prices = collect_all_prices()
    print(f"\nCollected {len(prices)} commodities")
    for sym, data in prices.items():
        if data:
            print(f"  {sym}: {len(data)} candles, last: {data[-1]['date']}")
