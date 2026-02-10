"""
AgriMacro v3.2 - Calendar of Market Events
Generates calendar.json with upcoming events that impact commodity volatility:
- WASDE reports (USDA schedule)
- USDA Crop Progress, Export Sales, Grain Stocks
- CFTC COT release (every Friday)
- FOMC meetings
- Futures/Options expirations (from futures_contracts.json)
- EIA weekly petroleum
"""
import json, os
from datetime import datetime, timedelta, date

OUT = os.path.join(os.path.dirname(__file__), "..", "agrimacro-dash", "public", "data", "processed", "calendar.json")
CONTRACTS_PATH = os.path.join(os.path.dirname(__file__), "..", "agrimacro-dash", "public", "data", "processed", "futures_contracts.json")

# ── Fixed 2026 dates ──────────────────────────────────────────────
WASDE_2026 = [
    "2026-01-12","2026-02-10","2026-03-10","2026-04-09",
    "2026-05-12","2026-06-11","2026-07-10","2026-08-12",
    "2026-09-11","2026-10-09","2026-11-10","2026-12-09"
]

USDA_PROSPECTIVE_PLANTINGS = ["2026-03-31"]
USDA_ACREAGE = ["2026-06-30"]
USDA_GRAIN_STOCKS = ["2026-01-12","2026-03-31","2026-06-30","2026-09-30"]

FOMC_2026 = [
    "2026-01-28","2026-03-18","2026-05-06","2026-06-17",
    "2026-07-29","2026-09-16","2026-11-04","2026-12-16"
]

# Recurring weekly events (day_of_week: 0=Mon ... 6=Sun)
WEEKLY = [
    {"name": "USDA Crop Progress", "dow": 0, "category": "usda", "season_only": True, "season_start": 4, "season_end": 11},
    {"name": "USDA Export Sales", "dow": 3, "category": "usda", "season_only": False},
    {"name": "EIA Petroleum Status", "dow": 2, "category": "eia", "season_only": False},
    {"name": "CFTC COT Release", "dow": 4, "category": "cftc", "season_only": False},
]

# CME Futures last trade dates (approx rules by commodity)
# Format: {symbol_prefix: {month_code: day_rule}}
CME_EXPIRY_RULES = {
    "ZC": "15th_prior_business",  # Business day prior to 15th
    "ZS": "15th_prior_business",
    "ZW": "15th_prior_business",
    "ZM": "15th_prior_business",
    "ZL": "15th_prior_business",
    "KE": "15th_prior_business",
    "CT": "17th_prior_business",
    "KC": "last_business",
    "SB": "last_business",
    "CC": "last_business",
    "OJ": "14th_prior_business",
    "HE": "10th_business",
    "LE": "last_business",
    "GF": "last_thursday",
    "CL": "25th_prior_3business",
    "NG": "3rd_prior_last_business",
    "GC": "3rd_last_business",
    "SI": "3rd_last_business",
}

MONTH_CODES = {"F":1,"G":2,"H":3,"J":4,"K":5,"M":6,"N":7,"Q":8,"U":9,"V":10,"X":11,"Z":12}

def generate_weekly_events(start: date, end: date) -> list:
    events = []
    d = start
    while d <= end:
        for w in WEEKLY:
            if d.weekday() == w["dow"]:
                if w.get("season_only") and not (w["season_start"] <= d.month <= w["season_end"]):
                    continue
                events.append({
                    "date": d.isoformat(),
                    "name": w["name"],
                    "category": w["category"],
                    "impact": "medium",
                    "recurring": True
                })
        d += timedelta(days=1)
    return events

def generate_fixed_events() -> list:
    events = []
    for d in WASDE_2026:
        events.append({"date": d, "name": "WASDE Report", "category": "usda", "impact": "high", "recurring": False})
    for d in USDA_PROSPECTIVE_PLANTINGS:
        events.append({"date": d, "name": "USDA Prospective Plantings", "category": "usda", "impact": "high", "recurring": False})
    for d in USDA_ACREAGE:
        events.append({"date": d, "name": "USDA Acreage Report", "category": "usda", "impact": "high", "recurring": False})
    for d in USDA_GRAIN_STOCKS:
        events.append({"date": d, "name": "USDA Grain Stocks", "category": "usda", "impact": "high", "recurring": False})
    for d in FOMC_2026:
        events.append({"date": d, "name": "FOMC Decision", "category": "macro", "impact": "high", "recurring": False})
    return events

def parse_contract_expirations() -> list:
    events = []
    if not os.path.exists(CONTRACTS_PATH):
        return events
    try:
        with open(CONTRACTS_PATH, "r") as f:
            data = json.load(f)
        contracts = data if isinstance(data, list) else data.get("contracts", [])
        seen = set()
        for c in contracts:
            sym = c.get("local_symbol") or c.get("symbol","")
            exp = c.get("lastTradeDateISO") or c.get("expiry","")
            if not exp or not sym:
                continue
            key = f"{sym}_{exp}"
            if key in seen:
                continue
            seen.add(key)
            events.append({
                "date": exp[:10],
                "name": f"Vencimento {sym}",
                "category": "expiry",
                "impact": "medium",
                "recurring": False
            })
    except Exception as e:
        print(f"  [WARN] Could not parse contracts: {e}")
    return events

def main():
    today = date.today()
    start = today - timedelta(days=7)
    end = today + timedelta(days=90)

    events = []
    events.extend(generate_fixed_events())
    events.extend(generate_weekly_events(start, end))
    events.extend(parse_contract_expirations())

    # Filter to relevant window and sort
    events = [e for e in events if start.isoformat() <= e["date"] <= end.isoformat()]
    events.sort(key=lambda x: x["date"])

    # Mark upcoming
    today_str = today.isoformat()
    for e in events:
        e["upcoming"] = e["date"] >= today_str

    output = {
        "generated_at": datetime.now().isoformat(),
        "range": {"start": start.isoformat(), "end": end.isoformat()},
        "total_events": len(events),
        "events": events
    }

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"  [OK] Calendar: {len(events)} events ({sum(1 for e in events if e['upcoming'])} upcoming)")

if __name__ == "__main__":
    main()