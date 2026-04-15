#!/usr/bin/env python3
"""Collect IMF Primary Commodity Prices (Pink Sheet)."""
import json, requests
from datetime import datetime
from pathlib import Path

OUT = Path(__file__).parent / "public" / "data" / "processed" / "imf_pink.json"

# IMF DataMapper commodity codes
COMMODITIES = {
    "CORN": "PMAIZMT", "SOYBEANS": "PSOYB", "WHEAT": "PWHEAMT",
    "SUGAR": "PSUGAISA", "COTTON": "PCOTTIND", "COCOA": "PCOCO",
    "COFFEE": "PCOFFOTM", "CRUDE_OIL": "POILBRE", "GOLD": "PGOLD", "SILVER": "PSILVER",
}

def main():
    out = {"updated_at": datetime.now().isoformat(), "source": "IMF Primary Commodity Prices", "commodities": {}}
    try:
        r = requests.get("https://www.imf.org/external/datamapper/api/v1/PCOMM", timeout=20)
        if r.ok:
            data = r.json()
            values = data.get("values", {}).get("PCOMM", {})
            for name, code in COMMODITIES.items():
                series = values.get(code, {})
                records = []
                for date_key, val in sorted(series.items())[-24:]:
                    try:
                        records.append({"date": date_key, "price": float(val), "unit": "index"})
                    except (ValueError, TypeError):
                        continue
                out["commodities"][name] = records
                if records:
                    print(f"  {name}: {len(records)} months, latest={records[-1]['price']}")
                else:
                    print(f"  {name}: no data for code {code}")
            total = sum(len(v) for v in out["commodities"].values())
            print(f"[OK] imf_pink: {total} data points")
        else:
            out["status"] = "unavailable"
            out["error"] = f"HTTP {r.status_code}"
            print(f"[WARN] imf_pink: HTTP {r.status_code}")
    except Exception as e:
        out["status"] = "unavailable"
        out["error"] = str(e)[:200]
        print(f"[WARN] imf_pink: {e}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2, default=str))

if __name__ == "__main__":
    main()
