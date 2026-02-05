import os, sys, json, time
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.commodities import COMMODITIES

def fetch_stooq(symbol, years=5):
    import requests
    end = datetime.now()
    start = end - timedelta(days=years*365)
    url = f"https://stooq.com/q/d/l/?s={symbol}&d1={start.strftime('%Y%m%d')}&d2={end.strftime('%Y%m%d')}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        if "No data" in r.text or len(r.text.strip()) < 50:
            return None
        df = pd.read_csv(StringIO(r.text))
        df.columns = [c.strip().lower() for c in df.columns]
        if 'date' not in df.columns or 'close' not in df.columns:
            return None
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        if 'volume' not in df.columns:
            df['volume'] = 0
        df = df.dropna(subset=['close'])
        if df.empty:
            return None
        return df[['date','open','high','low','close','volume']]
    except Exception as e:
        print(f"    [STOOQ ERRO] {e}")
        return None

def fetch_yahoo(symbol, years=5):
    try:
        import yfinance as yf
    except ImportError:
        print("    [YAHOO] yfinance nao instalado")
        return None
    try:
        df = yf.Ticker(symbol).history(period=f"{years}y")
        if df.empty:
            return None
        df = df.reset_index()
        df.columns = [c.strip().lower() for c in df.columns]
        if 'date' not in df.columns:
            return None
        if hasattr(df['date'].dt, 'tz') and df['date'].dt.tz is not None:
            df['date'] = df['date'].dt.tz_localize(None)
        if 'volume' not in df.columns:
            df['volume'] = 0
        df = df.dropna(subset=['close'])
        if df.empty:
            return None
        return df[['date','open','high','low','close','volume']]
    except Exception as e:
        print(f"    [YAHOO ERRO] {e}")
        return None

def collect_one(code, years=5):
    cfg = COMMODITIES.get(code)
    if not cfg:
        return None, "unknown"
    print(f"  [{code}] Stooq...", end=" ")
    df = fetch_stooq(cfg['stooq'], years)
    if df is not None and len(df) >= 10:
        print(f"OK ({len(df)} rows)")
        return df, "stooq"
    print("FALHOU")
    print(f"  [{code}] Yahoo...", end=" ")
    df = fetch_yahoo(cfg['yahoo'], years)
    if df is not None and len(df) >= 10:
        print(f"OK ({len(df)} rows)")
        return df, "yahoo"
    print("FALHOU")
    print(f"  [{code}] MISSING")
    return None, "missing"

def collect_all(years=5, output_dir="data/raw"):
    os.makedirs(output_dir, exist_ok=True)
    log = {"timestamp": datetime.now().isoformat(), "total": len(COMMODITIES), "success": 0, "failed": 0, "details": {}}
    all_prices = {}
    print("=" * 50)
    print("AGRIMACRO - GATE 2: COLETA DE PRECOS")
    print("=" * 50)
    for code in COMMODITIES:
        df, source = collect_one(code, years)
        if df is not None:
            records = []
            for _, row in df.iterrows():
                records.append({
                    "date": row['date'].strftime('%Y-%m-%d'),
                    "open": round(float(row['open']), 4) if pd.notna(row['open']) else None,
                    "high": round(float(row['high']), 4) if pd.notna(row['high']) else None,
                    "low": round(float(row['low']), 4) if pd.notna(row['low']) else None,
                    "close": round(float(row['close']), 4),
                    "volume": int(row['volume']) if pd.notna(row['volume']) else 0
                })
            all_prices[code] = records
            log["success"] += 1
            log["details"][code] = {"status": "ok", "source": source, "rows": len(records), "last_close": records[-1]["close"]}
        else:
            log["failed"] += 1
            log["details"][code] = {"status": "missing", "source": "none", "rows": 0}
        time.sleep(0.5)
    with open(os.path.join(output_dir, "price_history.json"), "w") as f:
        json.dump(all_prices, f)
    with open(os.path.join(output_dir, "collection_log.json"), "w") as f:
        json.dump(log, f, indent=2)
    print(f"\nRESULTADO: {log['success']}/{log['total']} OK | {log['failed']} MISSING")
    for code, d in log["details"].items():
        s = "Y" if d["status"]=="ok" else "X"
        print(f"  {s} {code:4s} | {d.get('source',''):6s} | {d.get('rows',0):5d} rows | {d.get('last_close','')}")
    return log

if __name__ == "__main__":
    collect_all(years=5)
