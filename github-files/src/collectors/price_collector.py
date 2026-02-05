"""
AgriMacro v2.0 - Gate 2: Price Collector
Coleta OHLCV de Stooq (primário) com fallback Yahoo Finance

Regras:
- Stooq falha -> Yahoo
- Ambos falham -> status "missing", pipeline NÃO trava
- ZERO MOCK
"""

import os
import sys
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

# Adicionar raiz ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.commodities import COMMODITIES


def fetch_stooq(symbol_stooq, years=5):
    """
    Busca CSV do Stooq.
    Retorna DataFrame com [date, open, high, low, close, volume] ou None.
    """
    import requests

    end = datetime.now()
    start = end - timedelta(days=years * 365)

    url = (
        f"https://stooq.com/q/d/l/"
        f"?s={symbol_stooq}"
        f"&d1={start.strftime('%Y%m%d')}"
        f"&d2={end.strftime('%Y%m%d')}"
    )

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

        # Remover linhas sem close
        df = df.dropna(subset=['close'])

        if df.empty:
            return None

        return df[['date', 'open', 'high', 'low', 'close', 'volume']]

    except Exception as e:
        print(f"    [STOOQ ERRO] {e}")
        return None


def fetch_yahoo(symbol_yahoo, years=5):
    """
    Busca do Yahoo Finance via yfinance.
    Retorna DataFrame com [date, open, high, low, close, volume] ou None.
    """
    try:
        import yfinance as yf
    except ImportError:
        print("    [YAHOO] yfinance não instalado. pip install yfinance")
        return None

    try:
        period = f"{years}y"
        ticker = yf.Ticker(symbol_yahoo)
        df = ticker.history(period=period)

        if df.empty:
            return None

        df = df.reset_index()
        df.columns = [c.strip().lower() for c in df.columns]

        if 'date' not in df.columns:
            return None

        # Remover timezone
        if hasattr(df['date'].dt, 'tz') and df['date'].dt.tz is not None:
            df['date'] = df['date'].dt.tz_localize(None)

        if 'volume' not in df.columns:
            df['volume'] = 0

        df = df.dropna(subset=['close'])

        if df.empty:
            return None

        return df[['date', 'open', 'high', 'low', 'close', 'volume']]

    except Exception as e:
        print(f"    [YAHOO ERRO] {e}")
        return None


def collect_one(code, years=5):
    """
    Coleta uma commodity com fallback.
    Retorna (DataFrame, fonte) ou (None, "missing").
    """
    cfg = COMMODITIES.get(code)
    if not cfg:
        return None, "unknown"

    # 1) Tentar Stooq
    print(f"  [{code}] Stooq ({cfg['stooq']})...", end=" ")
    df = fetch_stooq(cfg['stooq'], years)
    if df is not None and len(df) >= 10:
        print(f"OK ({len(df)} rows)")
        return df, "stooq"
    print("FALHOU")

    # 2) Fallback Yahoo
    print(f"  [{code}] Yahoo ({cfg['yahoo']})...", end=" ")
    df = fetch_yahoo(cfg['yahoo'], years)
    if df is not None and len(df) >= 10:
        print(f"OK ({len(df)} rows)")
        return df, "yahoo"
    print("FALHOU")

    # 3) Ambos falharam
    print(f"  [{code}] MISSING")
    return None, "missing"


def collect_all(years=5, output_dir="data/raw"):
    """
    Coleta todas as 21 commodities.
    Salva price_history.json e collection_log.json.
    """
    os.makedirs(output_dir, exist_ok=True)

    log = {
        "timestamp": datetime.now().isoformat(),
        "total": len(COMMODITIES),
        "success": 0,
        "failed": 0,
        "details": {}
    }

    all_prices = {}

    print("=" * 50)
    print("AGRIMACRO - GATE 2: COLETA DE PRECOS")
    print("=" * 50)

    for code in COMMODITIES:
        df, source = collect_one(code, years)

        if df is not None:
            # Converter para lista de dicts
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
            log["details"][code] = {
                "status": "ok",
                "source": source,
                "rows": len(records),
                "first": records[0]["date"],
                "last": records[-1]["date"],
                "last_close": records[-1]["close"]
            }
        else:
            log["failed"] += 1
            log["details"][code] = {
                "status": "missing",
                "source": "none",
                "rows": 0
            }

        # Pausa entre requests para não sobrecarregar
        time.sleep(0.5)

    # Salvar
    price_file = os.path.join(output_dir, "price_history.json")
    with open(price_file, "w") as f:
        json.dump(all_prices, f)
    print(f"\nSalvo: {price_file}")

    log_file = os.path.join(output_dir, "collection_log.json")
    with open(log_file, "w") as f:
        json.dump(log, f, indent=2)
    print(f"Salvo: {log_file}")

    # Resumo
    print("\n" + "=" * 50)
    print(f"RESULTADO: {log['success']}/{log['total']} OK | {log['failed']} MISSING")
    print("=" * 50)

    for code, detail in log["details"].items():
        status = "✅" if detail["status"] == "ok" else "❌"
        source = detail.get("source", "")
        rows = detail.get("rows", 0)
        last = detail.get("last_close", "")
        print(f"  {status} {code:4s} | {source:6s} | {rows:5d} rows | last: {last}")

    return log


if __name__ == "__main__":
    collect_all(years=5, output_dir="data/raw")
