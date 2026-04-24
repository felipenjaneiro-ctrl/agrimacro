"""
AgriMacro v3.0 - Seasonality Processor
Calculates seasonal patterns with SMOOTHED 5-year average
+ Multi-window monthly returns (full vs modern periods)
"""
import json
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config.commodities import COMMODITIES

# ── Per-commodity analysis windows ──
# Each entry: [(label, start_year, end_year), ...]
SEASON_WINDOWS = {
    "ZS": [("full", 1990, 2024), ("modern", 2005, 2024)],
    "ZC": [("full", 1990, 2024), ("modern", 2005, 2024)],
    "ZW": [("full", 1990, 2024), ("modern", 2010, 2024)],
    "KE": [("full", 1990, 2024), ("modern", 2010, 2024)],
    "ZM": [("full", 1990, 2024), ("modern", 2005, 2024)],
    "ZL": [("full", 1990, 2024), ("modern", 2005, 2024)],
    "CC": [("full", 2000, 2024), ("post_covid", 2020, 2024)],
    "KC": [("full", 2000, 2024), ("modern", 2010, 2024)],
    "SB": [("full", 1990, 2024), ("modern", 2005, 2024)],
    "CT": [("full", 1990, 2024), ("modern", 2010, 2024)],
    "OJ": [("full", 1990, 2024), ("modern", 2010, 2024)],
    "LE": [("full", 1990, 2024), ("modern", 2010, 2024)],
    "GF": [("full", 1990, 2024), ("modern", 2010, 2024)],
    "HE": [("full", 1990, 2024), ("modern", 2010, 2024)],
    "CL": [("full", 2000, 2024), ("modern", 2010, 2024)],
    "NG": [("full", 2000, 2024), ("modern", 2010, 2024)],
    "GC": [("full", 2000, 2024), ("modern", 2010, 2024)],
    "SI": [("full", 2000, 2024), ("modern", 2010, 2024)],
    "DX": [("full", 2000, 2024), ("modern", 2010, 2024)],
}

# Cache dir for long-term history
LONG_CACHE = Path(__file__).parent / "cache" / "long_history"


def fetch_long_history(symbol):
    """Download long-term daily history from Yahoo Finance (max period)."""
    cache_file = LONG_CACHE / f"{symbol}_long.json"
    # Use cache if fresh (< 7 days old)
    if cache_file.exists():
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        if age < 7 * 86400:
            with open(cache_file, encoding="utf-8") as f:
                return json.load(f)

    yahoo_sym = COMMODITIES.get(symbol, {}).get("yahoo")
    if not yahoo_sym:
        return []

    try:
        import yfinance as yf
        df = yf.Ticker(yahoo_sym).history(period="max")
        if df.empty:
            return []
        df = df.reset_index()
        if hasattr(df["Date"].dt, "tz") and df["Date"].dt.tz is not None:
            df["Date"] = df["Date"].dt.tz_localize(None)
        bars = [{"date": row["Date"].strftime("%Y-%m-%d"), "close": float(row["Close"])}
                for _, row in df.iterrows() if row["Close"] > 0]
        # Save cache
        LONG_CACHE.mkdir(parents=True, exist_ok=True)
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(bars, f)
        return bars
    except Exception as e:
        print(f"    [{symbol}] Yahoo long history error: {e}")
        return []


def calc_monthly_returns(bars, start_year, end_year):
    """Calculate average monthly return (%) for each month within a year range.

    Returns list of 12 floats (Jan..Dec) representing mean monthly return %.
    """
    # Group closes by year-month
    monthly_closes = defaultdict(list)
    for b in bars:
        try:
            dt = datetime.strptime(b["date"][:10], "%Y-%m-%d")
            if dt.year < start_year or dt.year > end_year:
                continue
            monthly_closes[(dt.year, dt.month)].append(b["close"])
        except Exception:
            continue

    # Get month-end close for each year-month
    month_end = {}
    for (y, m), closes in sorted(monthly_closes.items()):
        month_end[(y, m)] = closes[-1]  # last trading day of month

    # Calculate monthly returns
    returns_by_month = defaultdict(list)  # month → list of returns
    for (y, m), close in month_end.items():
        # Previous month
        prev_m = m - 1 if m > 1 else 12
        prev_y = y if m > 1 else y - 1
        if (prev_y, prev_m) in month_end:
            prev_close = month_end[(prev_y, prev_m)]
            if prev_close > 0:
                ret = (close - prev_close) / prev_close * 100
                returns_by_month[m].append(ret)

    # Average per month (1-indexed → 0-indexed list)
    result = []
    for month in range(1, 13):
        rets = returns_by_month.get(month, [])
        avg = round(sum(rets) / len(rets), 2) if rets else 0.0
        result.append(avg)

    return result

def smooth_series(data: list, window: int = 7) -> list:
    """Apply rolling average to smooth the series"""
    if len(data) < window:
        return data
    
    smoothed = []
    for i in range(len(data)):
        start = max(0, i - window // 2)
        end = min(len(data), i + window // 2 + 1)
        values = [d["close"] for d in data[start:end]]
        avg = sum(values) / len(values)
        smoothed.append({"day": data[i]["day"], "close": round(avg, 4)})
    
    return smoothed

def process_seasonality(price_file: Path) -> dict:
    """Process seasonality from price history"""
    with open(price_file) as f:
        prices = json.load(f)
    
    result = {}
    current_year = datetime.now().year
    years_to_include = [str(y) for y in range(current_year - 4, current_year + 1)]  # Last 5 years
    
    for symbol, candles in prices.items():
        if not candles:
            continue
        
        # Group by year and day-of-year
        by_year = defaultdict(list)
        by_day = defaultdict(list)  # For calculating average
        
        for candle in candles:
            try:
                date = datetime.strptime(candle["date"], "%Y-%m-%d")
                year_str = str(date.year)
                day_of_year = date.timetuple().tm_yday
                
                point = {"day": day_of_year, "close": candle["close"], "date": candle["date"]}
                
                if year_str in years_to_include:
                    by_year[year_str].append(point)
                
                # Collect all years for average (last 5 years only)
                if date.year >= current_year - 4:
                    by_day[day_of_year].append(candle["close"])
                    
            except Exception:
                continue
        
        # Calculate average per day (smoothed)
        avg_raw = []
        for day in sorted(by_day.keys()):
            values = by_day[day]
            if values:
                avg_raw.append({"day": day, "close": round(sum(values) / len(values), 4)})
        
        # Apply smoothing to average (window=7 for weekly smoothing)
        avg_smoothed = smooth_series(avg_raw, window=7)
        
        # Determine current year series (partial year)
        current_series = by_year.get(str(current_year), [])
        
        # Build years list in order
        years_list = [y for y in years_to_include if y in by_year]
        if str(current_year) in years_list:
            years_list.remove(str(current_year))
            years_list.append("current")
        years_list.append("average")
        
        # Build series dict
        series = {}
        for y in years_to_include:
            if y in by_year and y != str(current_year):
                # Sort by day
                series[y] = sorted(by_year[y], key=lambda x: x["day"])
        
        # Current year as "current"
        if current_series:
            series["current"] = sorted(current_series, key=lambda x: x["day"])
        
        # Smoothed average
        series["average"] = avg_smoothed
        
        result[symbol] = {
            "symbol": symbol,
            "status": "OK",
            "years": years_list,
            "series": series
        }

    # ── Multi-window monthly returns from long-term history ──
    for symbol in list(result.keys()):
        if symbol.startswith("_"):
            continue
        windows = SEASON_WINDOWS.get(symbol)
        if not windows:
            continue

        print(f"  [{symbol}] Fetching long-term history...", end=" ")
        bars = fetch_long_history(symbol)
        if not bars or len(bars) < 250:
            print(f"skip ({len(bars)} bars)")
            continue

        first_year = int(bars[0]["date"][:4])
        last_year = int(bars[-1]["date"][:4])
        print(f"OK ({len(bars)} bars, {first_year}-{last_year})")

        for label, start_y, end_y in windows:
            returns = calc_monthly_returns(bars, start_y, end_y)
            actual_start = max(start_y, first_year)
            actual_end = min(end_y, last_year)
            n_years = actual_end - actual_start + 1 if actual_end >= actual_start else 0

            if label == "full":
                result[symbol]["monthly_returns"] = returns
                result[symbol]["window_full"] = f"{actual_start}-{actual_end}"
                result[symbol]["n_years_full"] = n_years
            else:
                result[symbol][f"monthly_returns_{label}"] = returns
                result[symbol][f"window_{label}"] = f"{actual_start}-{actual_end}"
                result[symbol][f"n_years_{label}"] = n_years

    return result

if __name__ == "__main__":
    if len(sys.argv) > 1:
        result = process_seasonality(Path(sys.argv[1]))
    else:
        result = process_seasonality(Path("../agrimacro-dash/public/data/processed/price_history.json"))

    out_path = Path(__file__).parent.parent / "agrimacro-dash" / "public" / "data" / "processed" / "seasonality.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"Processed {len(result)} commodities")
    for sym, data in result.items():
        avg_len = len(data["series"].get("average", []))
        curr_len = len(data["series"].get("current", []))
        mr = data.get("monthly_returns")
        mr_tag = f", full={data.get('window_full','?')}" if mr else ""
        print(f"  {sym}: years={data['years']}, avg={avg_len} pts, current={curr_len} pts{mr_tag}")
    print(f"Saved to {out_path}")
