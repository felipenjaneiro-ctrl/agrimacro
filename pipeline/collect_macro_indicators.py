#!/usr/bin/env python3
"""
collect_macro_indicators.py - AgriMacro Macro Indicators Collector
==================================================================
Collects S&P 500, VIX, and US 10Y Treasury yield via yfinance.
Output: macro_indicators.json
"""

import json
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
ROOT_DIR = SCRIPT_DIR.parent

OUTPUT_CANDIDATES = [
    ROOT_DIR / "agrimacro-dash" / "public" / "data" / "processed",
    SCRIPT_DIR / "public" / "data" / "processed",
    SCRIPT_DIR,
]
OUTPUT_DIR = next((c for c in OUTPUT_CANDIDATES if c.exists()), SCRIPT_DIR)
OUTPUT_FILE = OUTPUT_DIR / "macro_indicators.json"


def _pct(cur, prev):
    if prev and prev != 0:
        return round((cur - prev) / prev * 100, 2)
    return None


def _fetch_ticker(symbol, period="1mo"):
    """Fetch recent history for a Yahoo Finance ticker. Returns DataFrame or None."""
    try:
        import yfinance as yf
        tk = yf.Ticker(symbol)
        df = tk.history(period=period)
        if df.empty:
            return None
        return df
    except Exception as e:
        print(f"    yfinance error for {symbol}: {e}")
        return None


def collect_sp500():
    """Collect S&P 500 data."""
    df = _fetch_ticker("^GSPC")
    if df is None or len(df) < 2:
        return None
    last = df.iloc[-1]
    prev = df.iloc[-2]
    price = round(float(last["Close"]), 2)
    change_pct = _pct(float(last["Close"]), float(prev["Close"]))
    # Week change: compare to 5 trading days ago if available
    week_idx = min(5, len(df) - 1)
    week_prev = df.iloc[-1 - week_idx]
    change_week_pct = _pct(float(last["Close"]), float(week_prev["Close"]))
    return {
        "price": price,
        "change_pct": change_pct,
        "change_week_pct": change_week_pct,
        "date": df.index[-1].strftime("%Y-%m-%d"),
    }


def collect_vix():
    """Collect VIX data."""
    df = _fetch_ticker("^VIX")
    if df is None or len(df) < 2:
        return None
    last = df.iloc[-1]
    prev = df.iloc[-2]
    value = round(float(last["Close"]), 2)
    change_pct = _pct(float(last["Close"]), float(prev["Close"]))
    if value < 15:
        level = "baixo"
    elif value <= 25:
        level = "normal"
    elif value <= 35:
        level = "elevado"
    else:
        level = "extremo"
    return {
        "value": value,
        "change_pct": change_pct,
        "level": level,
        "date": df.index[-1].strftime("%Y-%m-%d"),
    }


def collect_treasury_10y():
    """Collect US 10Y Treasury yield."""
    df = _fetch_ticker("^TNX")
    if df is None or len(df) < 2:
        return None
    last = df.iloc[-1]
    prev = df.iloc[-2]
    yld = round(float(last["Close"]), 3)
    prev_yld = round(float(prev["Close"]), 3)
    change_bps = round((yld - prev_yld) * 100, 1)
    # Direction based on 5-day trend
    week_idx = min(5, len(df) - 1)
    week_prev_yld = round(float(df.iloc[-1 - week_idx]["Close"]), 3)
    if yld > week_prev_yld + 0.03:
        direction = "subindo"
    elif yld < week_prev_yld - 0.03:
        direction = "caindo"
    else:
        direction = "estavel"
    return {
        "yield_pct": yld,
        "change_bps": change_bps,
        "direction": direction,
        "date": df.index[-1].strftime("%Y-%m-%d"),
    }


def main():
    print("  collect_macro_indicators: starting...")
    is_fallback = False
    sp = collect_sp500()
    vix = collect_vix()
    ty = collect_treasury_10y()

    if sp is None and vix is None and ty is None:
        is_fallback = True

    ok_count = sum(1 for x in [sp, vix, ty] if x is not None)
    print(f"    collected: {ok_count}/3 indicators")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "is_fallback": is_fallback,
        "source": "Yahoo Finance (yfinance)",
        "sp500": sp,
        "vix": vix,
        "treasury_10y": ty,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"    saved to {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    main()
