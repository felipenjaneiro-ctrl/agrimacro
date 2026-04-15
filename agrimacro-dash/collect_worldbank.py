#!/usr/bin/env python3
"""Collect World Bank commodity price data."""
import json, requests
from datetime import datetime
from pathlib import Path

OUT = Path(__file__).parent / "public" / "data" / "processed" / "worldbank.json"

INDICATORS = {
    "CORN": "COMMODITY.MAIZE", "SOYBEANS": "COMMODITY.SOYBEANS",
    "WHEAT": "COMMODITY.WHEAT.US.HRW", "SUGAR": "COMMODITY.SUGAR.WLD",
    "COTTON": "COMMODITY.COTTON.A.INDX", "COCOA": "COMMODITY.COCOA",
    "COFFEE": "COMMODITY.COFFEE.OTHER",
}

# Alternative: use the commodity markets data
ALT_URL = "https://api.worldbank.org/v2/en/indicator/{ind}?format=json&mrv=24&per_page=50"

def main():
    out = {"updated_at": datetime.now().isoformat(), "source": "World Bank Commodity Prices", "series": {}}
    for name, indicator in INDICATORS.items():
        try:
            url = ALT_URL.format(ind=indicator)
            r = requests.get(url, timeout=15)
            if r.ok:
                data = r.json()
                records_raw = data[1] if isinstance(data, list) and len(data) > 1 else []
                records = []
                for rec in (records_raw or []):
                    val = rec.get("value")
                    if val is not None:
                        records.append({
                            "date": rec.get("date", "?"),
                            "value": float(val),
                            "unit": "$/mt",
                        })
                out["series"][name] = sorted(records, key=lambda x: x["date"])
                print(f"  {name}: {len(records)} data points")
            else:
                out["series"][name] = []
                print(f"  {name}: HTTP {r.status_code}")
        except Exception as e:
            out["series"][name] = []
            print(f"  {name}: {e}")

    total = sum(len(v) for v in out["series"].values())
    if total > 0:
        print(f"[OK] worldbank: {total} total data points")
    else:
        out["status"] = "unavailable"
        print("[WARN] worldbank: no data collected")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2, default=str))

if __name__ == "__main__":
    main()
