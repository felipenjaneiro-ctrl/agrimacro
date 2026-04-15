#!/usr/bin/env python3
"""Collect FAO GIEWS / FPMA commodity prices."""
import json, requests
from datetime import datetime
from pathlib import Path

OUT = Path(__file__).parent / "public" / "data" / "processed" / "fao_giews.json"

COUNTRIES = {"Brazil": "21", "Argentina": "9", "United States": "231", "Nigeria": "159", "India": "100"}
COMMODITIES = {"Maize": "56", "Wheat": "15", "Rice": "27"}

def main():
    out = {"updated_at": datetime.now().isoformat(), "source": "FAO FPMA", "prices": []}
    try:
        r = requests.get("https://fpma.fao.org/api/v1/commodity_prices/", params={"format": "json"}, timeout=20)
        if r.ok:
            data = r.json()
            results = data.get("results", data) if isinstance(data, dict) else data
            if isinstance(results, list):
                for rec in results[:500]:
                    out["prices"].append({
                        "country": rec.get("country", rec.get("country_name", "?")),
                        "commodity": rec.get("commodity", rec.get("commodity_name", "?")),
                        "date": rec.get("date", rec.get("month", "?")),
                        "price": rec.get("price", rec.get("value", None)),
                        "unit": rec.get("unit", rec.get("um", "?")),
                        "currency": rec.get("currency", rec.get("cur", "?")),
                    })
            print(f"[OK] fao_giews: {len(out['prices'])} prices from FPMA")
        else:
            raise Exception(f"FPMA HTTP {r.status_code}")
    except Exception as e:
        print(f"[WARN] FPMA failed: {e}, trying FAOSTAT...")
        try:
            url = "https://fenixservices.fao.org/faostat/api/v1/en/data/PP"
            params = {"area": "21,9,231,159,100", "item": "56,15,27", "year": "2023,2024,2025", "show_flags": "false"}
            r2 = requests.get(url, params=params, timeout=20)
            if r2.ok:
                data2 = r2.json().get("data", [])
                for rec in data2[:300]:
                    out["prices"].append({
                        "country": rec.get("Area", "?"),
                        "commodity": rec.get("Item", "?"),
                        "date": rec.get("Year", "?"),
                        "price": rec.get("Value"),
                        "unit": rec.get("Unit", "?"),
                        "currency": "USD",
                    })
                print(f"[OK] fao_giews FAOSTAT fallback: {len(out['prices'])} prices")
            else:
                out["status"] = "unavailable"
                print(f"[WARN] FAOSTAT also failed: {r2.status_code}")
        except Exception as e2:
            out["status"] = "unavailable"
            out["error"] = str(e2)[:200]
            print(f"[WARN] fao_giews: both sources failed")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2, default=str))

if __name__ == "__main__":
    main()
