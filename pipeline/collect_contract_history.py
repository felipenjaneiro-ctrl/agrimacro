"""
AgriMacro v3.2 - Contract History Collector
Collects historical OHLCV for individual futures contracts from Yahoo Finance
"""
import json
import time
import os
from datetime import datetime, timedelta

def fetch_yahoo_contract(yahoo_symbol: str, days: int = 365) -> list:
    """Fetch historical data for a specific contract from Yahoo Finance"""
    import requests
    end = datetime.now()
    start = end - timedelta(days=days)
    p1 = int(start.timestamp())
    p2 = int(end.timestamp())
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_symbol}"
    params = {"period1": p1, "period2": p2, "interval": "1d"}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        data = r.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return []
        ts = result[0].get("timestamp", [])
        quote = result[0].get("indicators", {}).get("quote", [{}])[0]
        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        vols = quote.get("volume", [])
        bars = []
        for i, t in enumerate(ts):
            c = closes[i] if i < len(closes) else None
            if c is None:
                continue
            bars.append({
                "date": datetime.fromtimestamp(t).strftime("%Y-%m-%d"),
                "open": round(opens[i], 4) if opens[i] else c,
                "high": round(highs[i], 4) if highs[i] else c,
                "low": round(lows[i], 4) if lows[i] else c,
                "close": round(c, 4),
                "volume": int(vols[i]) if vols[i] else 0
            })
        return bars
    except Exception as e:
        print(f"    Error fetching {yahoo_symbol}: {e}")
        return []

def collect_contract_history():
    """Collect history for all contracts listed in futures_contracts.json"""
    base = os.path.dirname(os.path.abspath(__file__))
    fc_path = os.path.join(base, "..", "agrimacro-dash", "public", "data", "processed", "futures_contracts.json")
    out_path = os.path.join(base, "..", "agrimacro-dash", "public", "data", "processed", "contract_history.json")
    
    with open(fc_path, "r") as f:
        fc = json.load(f)
    
    commodities = fc.get("commodities", {})
    result = {}
    total = 0
    
    for sym, data in commodities.items():
        contracts = data.get("contracts", [])
        # Only fetch contracts with volume > 0 (actively traded)
        active = [c for c in contracts if c.get("volume", 0) > 0]
        if not active:
            active = contracts[:3]  # fallback: first 3
        
        for ct in active:
            contract_name = ct["contract"]
            yahoo_sym = ct.get("yahoo_symbol", "")
            if not yahoo_sym:
                continue
            
            print(f"  {contract_name} ({yahoo_sym})...", end=" ", flush=True)
            bars = fetch_yahoo_contract(yahoo_sym, days=365)
            if bars:
                result[contract_name] = {
                    "symbol": contract_name,
                    "commodity": sym,
                    "yahoo_symbol": yahoo_sym,
                    "expiry_label": ct.get("expiry_label", ""),
                    "bars": bars
                }
                print(f"OK ({len(bars)} bars)")
                total += 1
            else:
                print("NO DATA")
            
            time.sleep(0.3)  # rate limit
    
    output = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "contract_count": total,
        "contracts": result
    }
    
    with open(out_path, "w") as f:
        json.dump(output, f, indent=1)
    
    print(f"\nSaved {total} contracts to contract_history.json")
    return output

if __name__ == "__main__":
    collect_contract_history()
