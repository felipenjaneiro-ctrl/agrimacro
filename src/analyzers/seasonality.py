import os, sys, json
import numpy as np
from datetime import datetime

def analyze_seasonality(symbol, prices, years=5):
    current_year = datetime.now().year
    past_years = list(range(current_year - years, current_year))
    by_year = {}
    for p in prices:
        try:
            dt = datetime.strptime(p["date"], "%Y-%m-%d")
            yr = dt.year
            doy = dt.timetuple().tm_yday
            if yr not in by_year:
                by_year[yr] = []
            by_year[yr].append({"day": doy, "close": p["close"], "date": p["date"]})
        except:
            continue
    series = {}
    for yr in past_years:
        if yr in by_year:
            series[str(yr)] = sorted(by_year[yr], key=lambda x: x["day"])
    if current_year in by_year:
        series["current"] = sorted(by_year[current_year], key=lambda x: x["day"])
    day_vals = {}
    for yr in past_years:
        if yr in by_year:
            for pt in by_year[yr]:
                d = pt["day"]
                if d not in day_vals:
                    day_vals[d] = []
                day_vals[d].append(pt["close"])
    avg = []
    for d in sorted(day_vals.keys()):
        avg.append({"day": d, "close": round(np.mean(day_vals[d]), 4)})
    series["average"] = avg
    stats = None
    if "current" in series and avg:
        last = series["current"][-1]
        last_day = last["day"]
        avg_val = None
        for a in avg:
            if a["day"] == last_day:
                avg_val = a["close"]
                break
        if avg_val and avg_val > 0:
            dev = round(((last["close"] / avg_val) - 1) * 100, 2)
            stats = {"current_price": last["close"], "avg_price": avg_val, "deviation_pct": dev}
    return {"symbol": symbol, "status": "ok", "years": list(series.keys()), "series": series, "stats": stats}

if __name__ == "__main__":
    print("=" * 50)
    print("AGRIMACRO - GATE 3: SAZONALIDADE")
    print("=" * 50)
    with open("data/raw/price_history.json") as f:
        ph = json.load(f)
    os.makedirs("data/processed", exist_ok=True)
    results = {}
    for symbol, prices in ph.items():
        r = analyze_seasonality(symbol, prices)
        results[symbol] = r
        s = r.get("stats")
        if s:
            arrow = "^" if s["deviation_pct"] > 0 else "v"
            print(f"  {symbol:4s} | Atual: {s['current_price']:10.2f} | Media: {s['avg_price']:10.2f} | {arrow} {s['deviation_pct']:+.1f}%")
        else:
            print(f"  {symbol:4s} | Sem dados suficientes")
    with open("data/processed/seasonality.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Salvo: data/processed/seasonality.json")
