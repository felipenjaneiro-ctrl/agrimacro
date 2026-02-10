"""
AgriMacro v3.0 - Seasonality Processor
Calculates seasonal patterns with SMOOTHED 5-year average
"""
import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict

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
    
    return result

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        result = process_seasonality(Path(sys.argv[1]))
    else:
        # Default path
        result = process_seasonality(Path("../agrimacro-dash/public/data/raw/price_history.json"))
    
    print(f"Processed {len(result)} commodities")
    for sym, data in result.items():
        avg_len = len(data["series"].get("average", []))
        curr_len = len(data["series"].get("current", []))
        print(f"  {sym}: years={data['years']}, avg={avg_len} pts, current={curr_len} pts")
