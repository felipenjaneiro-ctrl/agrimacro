#!/usr/bin/env python3
"""Collect USDA NASS QuickStats production data."""
import json, os, requests
from datetime import datetime
from pathlib import Path

OUT = Path(__file__).parent / "public" / "data" / "processed" / "usda_nass.json"
API_KEY = os.environ.get("USDA_QUICKSTATS_API_KEY", "")
BASE = "https://quickstats.nass.usda.gov/api/api_GET/"

QUERIES = [
    {"commodity": "CORN", "sector": "CROPS", "stat": "PRODUCTION"},
    {"commodity": "SOYBEANS", "sector": "CROPS", "stat": "PRODUCTION"},
    {"commodity": "WHEAT", "sector": "CROPS", "stat": "PRODUCTION"},
    {"commodity": "CATTLE", "sector": "ANIMALS & PRODUCTS", "stat": "INVENTORY"},
]

def main():
    out = {"updated_at": datetime.now().isoformat(), "source": "USDA NASS QuickStats", "commodities": {}}
    if not API_KEY:
        out["status"] = "no_api_key"
        out["error"] = "Set USDA_QUICKSTATS_API_KEY env var"
        print("[WARN] usda_nass: no API key")
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(out, indent=2))
        return

    for q in QUERIES:
        try:
            params = {
                "key": API_KEY,
                "commodity_desc": q["commodity"],
                "source_desc": "SURVEY",
                "sector_desc": q["sector"],
                "statisticcat_desc": q["stat"],
                "year__GE": "2020",
                "agg_level_desc": "STATE",
                "format": "json",
            }
            r = requests.get(BASE, params=params, timeout=20)
            if r.ok:
                data = r.json().get("data", [])
                records = []
                for d in data[:200]:
                    val = d.get("Value", "").replace(",", "")
                    try:
                        val = float(val)
                    except:
                        continue
                    records.append({
                        "state": d.get("state_name", "?"),
                        "year": d.get("year", "?"),
                        "value": val,
                        "unit": d.get("unit_desc", "?"),
                    })
                out["commodities"][q["commodity"]] = records
                print(f"  {q['commodity']}: {len(records)} records")
            else:
                out["commodities"][q["commodity"]] = []
                print(f"  {q['commodity']}: HTTP {r.status_code}")
        except Exception as e:
            out["commodities"][q["commodity"]] = []
            print(f"  {q['commodity']}: {e}")

    total = sum(len(v) for v in out["commodities"].values())
    print(f"[OK] usda_nass: {total} total records")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2, default=str))

if __name__ == "__main__":
    main()
