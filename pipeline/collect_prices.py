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
    "RB": {"yahoo": "RB=F", "name": "Gasoline RBOB"},
    "HO": {"yahoo": "HO=F", "name": "Heating Oil"},
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


def main(skip_symbols: set = None):
    """
    Coleta precos via Yahoo Finance como fallback.
    skip_symbols: conjunto de simbolos ja coletados pelo IBKR
    que NAO serao sobrescritos pelo Yahoo.
    """
    import json
    from pathlib import Path

    skip_symbols = skip_symbols or set()

    ph_path = Path(__file__).parent.parent / "agrimacro-dash" / "public" / "data" / "processed" / "price_history.json"
    raw_path = Path(__file__).parent.parent / "agrimacro-dash" / "public" / "data" / "raw" / "price_history.json"

    # Carregar price_history.json existente (pode ter dados do IBKR)
    existing = {}
    if ph_path.exists():
        try:
            with open(ph_path, encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass

    # Coletar do Yahoo apenas gaps
    yahoo_data = {}
    for sym, info in COMMODITIES.items():
        if sym in skip_symbols:
            print(f"  [SKIP] {sym} -- ja coletado pelo IBKR")
            continue
        print(f"  Collecting {sym} ({info['name']}) from Yahoo...")
        data = fetch_yahoo(info["yahoo"])
        if data:
            yahoo_data[sym] = data
            print(f"    OK: {len(data)} candles")
        else:
            print(f"    FAILED: No data from Yahoo")
        time.sleep(0.5)

    # Merge: IBKR tem prioridade sobre Yahoo
    merged = dict(existing)
    for sym, data in yahoo_data.items():
        if sym not in skip_symbols:
            merged[sym] = data

    # Salvar processed
    with open(ph_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=1)

    # Salvar raw tambem
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(merged, f)

    print(f"  Yahoo fallback: {len(yahoo_data)} coletados, {len(skip_symbols)} pulados (IBKR)")
    return yahoo_data


if __name__ == "__main__":
    prices = collect_all_prices()
    print(f"\nCollected {len(prices)} commodities")
    for sym, data in prices.items():
        if data:
            print(f"  {sym}: {len(data)} candles, last: {data[-1]['date']}")
