#!/usr/bin/env python3
"""
collect_usda_psd_csv.py - AgriMacro USDA PSD Bulk CSV Collector
================================================================
Replaces collect_usda_fas.py which depended on the now-broken /OpenData/api.
Downloads bulk CSVs directly from apps.fas.usda.gov/psdonline/downloads/
No API key required. Run monthly after WASDE release.

Output: usda_fas.json (compatible with existing dashboard)
"""

import csv
import io
import json
import os
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
ENV_FILE = SCRIPT_DIR.parent / ".env" if (SCRIPT_DIR.parent / ".env").exists() else SCRIPT_DIR / ".env"

# Try multiple output paths
OUTPUT_CANDIDATES = [
    SCRIPT_DIR.parent / "agrimacro-dash" / "public" / "data" / "processed",
    SCRIPT_DIR / "public" / "data" / "processed",
    SCRIPT_DIR,
]
OUTPUT_DIR = None
for c in OUTPUT_CANDIDATES:
    if c.exists():
        OUTPUT_DIR = c
        break
if OUTPUT_DIR is None:
    OUTPUT_DIR = SCRIPT_DIR

OUTPUT_FILE = OUTPUT_DIR / "usda_fas.json"
PSD_STOCKS_FILE = OUTPUT_DIR / "psd_ending_stocks.json"

# ---------------------------------------------------------------------------
# Bulk CSV URLs (no API key needed)
# ---------------------------------------------------------------------------
CSV_ZIPS = {
    "grains_pulses": "https://apps.fas.usda.gov/psdonline/downloads/psd_grains_pulses_csv.zip",
    "oilseeds": "https://apps.fas.usda.gov/psdonline/downloads/psd_oilseeds_csv.zip",
    "livestock": "https://apps.fas.usda.gov/psdonline/downloads/psd_livestock_csv.zip",
    "cotton": "https://apps.fas.usda.gov/psdonline/downloads/psd_cotton_csv.zip",
    "sugar": "https://apps.fas.usda.gov/psdonline/downloads/psd_sugar_csv.zip",
    "coffee": "https://apps.fas.usda.gov/psdonline/downloads/psd_coffee_csv.zip",
}

# ---------------------------------------------------------------------------
# Commodity mapping: PSD code -> AgriMacro key + ticker
# ---------------------------------------------------------------------------
COMMODITIES = {
    "0440000": {"key": "corn", "name": "Corn", "ticker": "ZC"},
    "2222000": {"key": "soybeans", "name": "Soybeans", "ticker": "ZS"},
    "0410000": {"key": "wheat", "name": "Wheat", "ticker": "ZW"},
    "4232000": {"key": "soybean_meal", "name": "Soybean Meal", "ticker": "ZM"},
    "4234000": {"key": "soybean_oil", "name": "Soybean Oil", "ticker": "ZL"},
    "2631000": {"key": "cotton", "name": "Cotton", "ticker": "CT"},
    "0612000": {"key": "sugar", "name": "Sugar, Centrifugal", "ticker": "SB"},
    "0422110": {"key": "rice", "name": "Rice, Milled", "ticker": "ZR"},
    "0111000": {"key": "beef", "name": "Beef and Veal", "ticker": "LE"},
    "0112000": {"key": "pork", "name": "Pork", "ticker": "HE"},
    "0711000": {"key": "coffee", "name": "Coffee, Green", "ticker": "KC"},
}

PSD_ATTRIBUTES = [
    "Production", "Beginning Stocks", "Ending Stocks",
    "Domestic Consumption", "Total Supply", "Total Distribution",
    "Imports", "Exports", "Total Use",
    "Feed Dom. Consumption", "FSI Consumption", "Crush",
]

KEY_COUNTRIES = {
    "US": "United States",
    "BR": "Brazil",
    "AR": "Argentina",
    "CH": "China",
    "IN": "India",
    "EU": "European Union",
}

WORLD_COUNTRY_CODE = "WD"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log(msg, ok=False, warn=False, err=False):
    if ok:
        icon = "[OK]"
    elif warn:
        icon = "[!!]"
    elif err:
        icon = "[XX]"
    else:
        icon = "[..]"
    print(f"  {icon} {msg}")


# ---------------------------------------------------------------------------
# Download + parse CSV
# ---------------------------------------------------------------------------
import urllib.request
import ssl


def download_zip(url, label):
    """Download ZIP and return list of CSV rows (list of dicts)."""
    print(f"\n  Downloading {label}...")
    ctx = ssl.create_default_context()
    headers = {"User-Agent": "AgriMacro/3.3"}

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=60, context=ctx) as resp:
            data = resp.read()
            size_mb = len(data) / 1024 / 1024
            log(f"{label}: {size_mb:.1f} MB downloaded", ok=True)
    except Exception as e:
        log(f"{label}: download failed - {e}", err=True)
        return []

    rows = []
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for name in zf.namelist():
                if name.endswith(".csv"):
                    log(f"Parsing {name}...")
                    with zf.open(name) as csvfile:
                        text = io.TextIOWrapper(csvfile, encoding="utf-8-sig")
                        reader = csv.DictReader(text)
                        for row in reader:
                            rows.append(row)
                    log(f"{len(rows):,} rows loaded from {name}", ok=True)
    except Exception as e:
        log(f"{label}: ZIP parse failed - {e}", err=True)

    return rows


def filter_rows(all_rows):
    """Filter rows for our commodities, world + key countries, recent years."""
    current_year = datetime.now().year
    min_year = current_year - 6  # 6 years of history

    psd_codes = set(COMMODITIES.keys())
    country_codes = set(KEY_COUNTRIES.keys()) | {WORLD_COUNTRY_CODE}

    filtered = []
    for row in all_rows:
        cc = row.get("Commodity_Code", "").strip()
        country = row.get("Country_Code", "").strip()

        if cc not in psd_codes:
            continue
        if country not in country_codes:
            continue

        try:
            year = int(row.get("Market_Year", 0))
        except (ValueError, TypeError):
            continue

        if year < min_year:
            continue

        filtered.append(row)

    return filtered


# ---------------------------------------------------------------------------
# Build PSD data structures
# ---------------------------------------------------------------------------
def build_psd_world(filtered_rows):
    """Build world S&D for each commodity (compatible with old usda_fas.json)."""
    current_year = datetime.now().year
    results = {}

    for psd_code, cfg in COMMODITIES.items():
        key = cfg["key"]
        commodity_rows = [
            r for r in filtered_rows
            if r.get("Commodity_Code", "").strip() == psd_code
            and r.get("Country_Code", "").strip() == WORLD_COUNTRY_CODE
        ]

        if not commodity_rows:
            continue

        # Group by year
        yearly = {}
        unit = ""
        for row in commodity_rows:
            try:
                year = int(row.get("Market_Year", 0))
            except (ValueError, TypeError):
                continue

            attr = row.get("Attribute_Description", "").strip()
            try:
                val = float(row.get("Value", 0) or 0)
            except (ValueError, TypeError):
                val = 0.0

            unit = row.get("Unit_Description", unit).strip()

            if attr and any(a.lower() in attr.lower() for a in PSD_ATTRIBUTES):
                if str(year) not in yearly:
                    yearly[str(year)] = {}
                yearly[str(year)][attr] = val

        if not yearly:
            continue

        # Find latest year
        ly = str(max(int(y) for y in yearly.keys()))
        latest = yearly[ly]

        # Ending stocks
        es = None
        for attr, val in latest.items():
            if "ending" in attr.lower() and "stock" in attr.lower():
                es = val
                break

        # Total use
        tu = None
        for attr, val in latest.items():
            if any(x in attr.lower() for x in ["total use", "domestic consumption", "total distribution"]):
                if tu is None or val > tu:
                    tu = val

        # Stocks-to-use ratio
        stu = round((es / tu) * 100, 2) if es and tu and tu > 0 else None
        is_tight = stu is not None and stu < 15

        # YoY change
        py = str(int(ly) - 1)
        es_prev = None
        if py in yearly:
            for attr, val in yearly[py].items():
                if "ending" in attr.lower() and "stock" in attr.lower():
                    es_prev = val
                    break

        es_yoy = None
        if es is not None and es_prev and es_prev > 0:
            es_yoy = round(((es - es_prev) / es_prev) * 100, 2)

        results[key] = {
            "commodity": cfg["name"],
            "psd_code": psd_code,
            "unit": unit,
            "latest_year": int(ly),
            "yearly_data": {y: d for y, d in sorted(yearly.items())},
            "analysis": {
                "ending_stocks": es,
                "total_use": tu,
                "stocks_to_use_pct": stu,
                "is_tight": is_tight,
                "ending_stocks_yoy_pct": es_yoy,
                "stocks_shrinking": es_yoy is not None and es_yoy < 0,
                "previous_ending_stocks": es_prev,
            },
        }
        status = "TIGHT" if is_tight else "OK"
        log(f"{cfg['name']}: ES={es:,.0f} S/U={stu}% YoY={es_yoy}% [{status}]" if es and stu else f"{cfg['name']}: data loaded", ok=True)

    return results


def build_psd_countries(filtered_rows):
    """Build S&D for key countries."""
    current_year = datetime.now().year
    results = {}

    for psd_code, cfg in COMMODITIES.items():
        key = cfg["key"]
        country_data = {}

        for cc, cname in KEY_COUNTRIES.items():
            country_rows = [
                r for r in filtered_rows
                if r.get("Commodity_Code", "").strip() == psd_code
                and r.get("Country_Code", "").strip() == cc
            ]

            if not country_rows:
                continue

            yearly = {}
            for row in country_rows:
                try:
                    year = int(row.get("Market_Year", 0))
                except (ValueError, TypeError):
                    continue

                attr = row.get("Attribute_Description", "").strip()
                try:
                    val = float(row.get("Value", 0) or 0)
                except (ValueError, TypeError):
                    val = 0.0

                if attr and any(a.lower() in attr.lower() for a in PSD_ATTRIBUTES):
                    if str(year) not in yearly:
                        yearly[str(year)] = {}
                    yearly[str(year)][attr] = val

            if yearly:
                ly = str(max(int(y) for y in yearly.keys()))
                country_data[cc] = {
                    "country": cname,
                    "latest_year": int(ly),
                    "data": yearly,
                }

        if country_data:
            results[key] = country_data

    return results


def build_psd_ending_stocks(psd_world, psd_countries=None):
    """Build psd_ending_stocks.json compatible format.
    Uses psd_world (WD aggregates) when available, falls back to US data
    from psd_countries since bulk CSVs often lack world aggregates."""
    stocks = {}

    for psd_code, cfg in COMMODITIES.items():
        key = cfg["key"]
        ticker = cfg["ticker"]

        # Fonte primaria: psd_world (agregados mundiais)
        data = psd_world.get(key)
        source_label = "USDA PSD Online (World)"

        # Fallback: psd_countries US data (bulk CSVs nao incluem WD)
        if not data and psd_countries:
            us_data = psd_countries.get(key, {}).get("US")
            if us_data:
                data = {
                    "yearly_data": us_data.get("data", {}),
                    "unit": "(1000 MT)",
                }
                source_label = "USDA PSD Online (US)"
                log(f"{cfg['name']}: usando dados US (WD indisponivel no bulk CSV)", warn=True)

        if not data:
            continue

        yearly = data.get("yearly_data", {})
        es_history = []

        for year_str in sorted(yearly.keys()):
            year_data = yearly[year_str]
            es_val = None
            for attr, val in year_data.items():
                if "ending" in attr.lower() and "stock" in attr.lower():
                    es_val = val
                    break
            if es_val is not None:
                es_history.append({"year": int(year_str), "value": es_val})

        if not es_history:
            continue

        current = es_history[-1]["value"]
        recent = [h["value"] for h in es_history[-6:-1]]  # last 5 years excluding current
        avg_5y = round(sum(recent) / len(recent), 1) if recent else 0
        deviation = round(((current - avg_5y) / avg_5y) * 100, 1) if avg_5y > 0 else 0

        stocks[ticker] = {
            "current": current,
            "avg_5y": avg_5y,
            "deviation": deviation,
            "unit": data.get("unit", ""),
            "year": es_history[-1]["year"],
            "history": es_history[-6:],  # last 6 years
            "source": source_label,
        }

    return stocks


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("USDA PSD Bulk CSV Collector - AgriMacro v3.3")
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Fonte: apps.fas.usda.gov/psdonline/downloads/")
    print(f"Output: {OUTPUT_DIR}")
    print("=" * 60)

    # Download all ZIPs
    all_rows = []
    for label, url in CSV_ZIPS.items():
        rows = download_zip(url, label)
        all_rows.extend(rows)
        time.sleep(1)  # polite delay

    if not all_rows:
        log("Nenhum dado baixado!", err=True)
        sys.exit(1)

    log(f"Total bruto: {len(all_rows):,} rows", ok=True)

    # Filter
    filtered = filter_rows(all_rows)
    log(f"Filtrado: {len(filtered):,} rows (commodities AgriMacro, anos recentes)", ok=True)

    # Build structures
    print("\n" + "=" * 40)
    print("PSD World Supply & Demand")
    print("=" * 40)
    psd_world = build_psd_world(filtered)

    print("\n" + "=" * 40)
    print("PSD Key Countries")
    print("=" * 40)
    psd_countries = build_psd_countries(filtered)
    for key, countries in psd_countries.items():
        log(f"{key}: {', '.join(countries.keys())}", ok=True)

    # Analysis summary
    summary = []
    for key, psd in psd_world.items():
        a = psd.get("analysis", {})
        line = f"{psd['commodity']}: "
        if a.get("stocks_to_use_pct") is not None:
            line += f"stocks/use={a['stocks_to_use_pct']}%"
            if a["is_tight"]:
                line += " >> TIGHT"
        if a.get("ending_stocks_yoy_pct") is not None:
            d = "DOWN" if a["ending_stocks_yoy_pct"] < 0 else "UP"
            line += f", ending stocks YoY {d} {abs(a['ending_stocks_yoy_pct'])}%"
        summary.append(line)

    # Save usda_fas.json
    result = {
        "metadata": {
            "source": "USDA PSD Online (bulk CSV downloads)",
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "version": "3.3-csv",
            "method": "Bulk CSV from apps.fas.usda.gov/psdonline/downloads/",
            "note": "API /OpenData/api returned HTTP 500 since Feb 2026. Using direct CSV downloads.",
            "sections": {
                "psd_world": len(psd_world),
                "psd_countries": len(psd_countries),
            },
        },
        "export_sales": {},
        "psd_world": psd_world,
        "psd_countries": psd_countries,
        "analysis_summary": summary,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    size_kb = OUTPUT_FILE.stat().st_size / 1024
    log(f"Salvo: {OUTPUT_FILE} ({size_kb:.1f} KB)", ok=True)

    # Save psd_ending_stocks.json
    stocks = build_psd_ending_stocks(psd_world, psd_countries)
    stocks_out = {
        "timestamp": datetime.now().isoformat(),
        "source": "USDA PSD Online (bulk CSV)",
        "commodities": stocks,
    }
    with open(PSD_STOCKS_FILE, "w", encoding="utf-8") as f:
        json.dump(stocks_out, f, indent=2, ensure_ascii=False)
    size_kb2 = PSD_STOCKS_FILE.stat().st_size / 1024
    log(f"Salvo: {PSD_STOCKS_FILE} ({size_kb2:.1f} KB)", ok=True)

    # Final report
    print(f"\n{'=' * 60}")
    print(f"[OK] Coleta finalizada!")
    print(f"   PSD World: {len(psd_world)} commodities")
    print(f"   PSD Countries: {len(psd_countries)} commodities x paises")
    print(f"   Ending Stocks: {len(stocks)} tickers")
    print(f"\n   Resumo:")
    for line in summary:
        print(f"   {line}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
