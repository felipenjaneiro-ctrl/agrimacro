"""
AgriMacro v3.1 - Physical Market Collector
Collects USDA cash prices for US markets.
International markets marked as unavailable (no API).
"""
import json
import requests
from datetime import datetime
from pathlib import Path

USDA_KEY = "BA43C01C-A885-3616-9774-EFF03A68F06A"
USDA_BASE = "https://quickstats.nass.usda.gov/api/api_GET/"

CASH_MAP = {
    "CORN": {"stat": "PRICE RECEIVED", "filter": "CORN, GRAIN - PRICE RECEIVED, MEASURED IN $ / BU",
             "futures_sym": "ZC", "futures_unit": "c/bu", "cash_mult": 100, "label": "US Corn #2 Yellow"},
    "SOYBEANS": {"stat": "PRICE RECEIVED", "filter": "SOYBEANS - PRICE RECEIVED, MEASURED IN $ / BU",
                 "futures_sym": "ZS", "futures_unit": "c/bu", "cash_mult": 100, "label": "US Soybeans #2"},
    "WHEAT": {"stat": "PRICE RECEIVED", "filter": "WHEAT - PRICE RECEIVED, MEASURED IN $ / BU",
              "futures_sym": "ZW", "futures_unit": "c/bu", "cash_mult": 100, "label": "US Wheat SRW"},
    "COTTON": {"stat": "PRICE RECEIVED", "filter": "COTTON, UPLAND - PRICE RECEIVED, MEASURED IN $ / LB",
               "futures_sym": "CT", "futures_unit": "c/lb", "cash_mult": 100, "label": "US Cotton Upland"},
    "CATTLE": {"stat": "PRICE RECEIVED", "filter": "CATTLE, STEERS & HEIFERS, GE 500 LBS - PRICE RECEIVED, MEASURED IN $ / CWT",
               "futures_sym": "LE", "futures_unit": "c/lb", "cash_mult": 1, "label": "US Steers & Heifers 500+"},
    "HOGS": {"stat": "PRICE RECEIVED", "filter": "HOGS, BARROWS & GILTS - PRICE RECEIVED, MEASURED IN $ / CWT",
             "futures_sym": "HE", "futures_unit": "c/lb", "cash_mult": 1, "label": "US Barrows & Gilts"},
}

MONTHS = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}


def fetch_cash_prices(commodity, cfg):
    params = {"key": USDA_KEY, "commodity_desc": commodity, "statisticcat_desc": cfg["stat"],
              "year__GE": "2024", "agg_level_desc": "NATIONAL", "format": "JSON"}
    try:
        r = requests.get(USDA_BASE, params=params, timeout=30)
        if r.status_code != 200:
            return None
        rows = r.json().get("data", [])
        filtered = [row for row in rows if cfg["filter"] in row.get("short_desc", "")]
        if not filtered:
            filtered = rows
        records = []
        for row in filtered:
            period = row.get("reference_period_desc", "")
            if period in MONTHS:
                val = row.get("Value", "").replace(",", "")
                try:
                    records.append({"year": int(row["year"]), "month": MONTHS[period], "value": float(val)})
                except:
                    pass
        records.sort(key=lambda x: (x["year"], x["month"]))
        return records if records else None
    except Exception as e:
        print(f"  Error fetching {commodity}: {e}")
        return None


def collect_physical(price_file=None):
    result = {"timestamp": datetime.now().isoformat(), "us_cash": {}, "international": {}}

    futures_prices = {}
    if price_file and Path(price_file).exists():
        with open(price_file, encoding="utf-8") as f:
            ph = json.load(f)
        for sym, data in ph.items():
            candles = data if isinstance(data, list) else data.get("candles", [])
            if candles:
                futures_prices[sym] = candles[-1]["close"]

    for commodity, cfg in CASH_MAP.items():
        print(f"  {commodity}: Fetching USDA cash prices...")
        records = fetch_cash_prices(commodity, cfg)
        if not records:
            print(f"    No data")
            continue

        latest = records[-1]
        cash_in_futures_units = latest["value"] * cfg["cash_mult"]

        futures_price = futures_prices.get(cfg["futures_sym"], 0)
        basis = None
        basis_pct = None
        if futures_price > 0 and cash_in_futures_units > 0:
            basis = round(cash_in_futures_units - futures_price, 2)
            basis_pct = round((basis / futures_price) * 100, 1)

        trend = None
        if len(records) >= 2:
            prev = records[-2]["value"]
            if prev > 0:
                pct = ((latest["value"] - prev) / prev) * 100
                trend = f"{pct:+.1f}% m/m"

        result["us_cash"][cfg["futures_sym"]] = {
            "commodity": commodity,
            "label": cfg["label"],
            "cash_price": round(cash_in_futures_units, 2),
            "cash_unit": cfg["futures_unit"],
            "futures_price": round(futures_price, 2) if futures_price else None,
            "basis": basis,
            "basis_pct": basis_pct,
            "period": f"{latest['year']}-{latest['month']:02d}",
            "trend": trend,
            "source": "USDA NASS",
            "history": [{"period": f"{r['year']}-{r['month']:02d}", "value": round(r["value"] * cfg["cash_mult"], 2)} for r in records[-12:]],
        }
        print(f"    Cash: {cash_in_futures_units:.1f} {cfg['futures_unit']} | Futures: {futures_price:.1f} | Basis: {basis}")

    return result


if __name__ == "__main__":
    price_file = Path("../agrimacro-dash/public/data/raw/price_history.json")
    result = collect_physical(price_file)
    print(f"\nUS Cash prices: {len(result['us_cash'])} commodities")
    for sym, data in result['us_cash'].items():
        print(f"  {sym}: {data['label']} = {data['cash_price']} {data['cash_unit']} | Basis: {data['basis']} ({data['basis_pct']}%)")
