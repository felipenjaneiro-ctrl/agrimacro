"""
collect_livestock_psd.py — Coleta dados PSD de proteína animal (USDA FAS bulk CSV)
Fonte: https://apps.fas.usda.gov/psdonline/downloads/psd_livestock_csv.zip
"""
import json, os, requests, io, zipfile, csv
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).parent.parent
OUT = BASE / "agrimacro-dash" / "public" / "data" / "processed" / "livestock_psd.json"

BULK_URL = "https://apps.fas.usda.gov/psdonline/downloads/psd_livestock_csv.zip"

# Commodity codes no CSV de livestock
COMMODITIES = {
    "0111000": {"sym": "LE", "name": "Beef & Veal"},
    "0113000": {"sym": "HE", "name": "Pork"},
    "0115000": {"sym": "PO", "name": "Poultry (Chicken)"},
}

# Attribute IDs relevantes
ATTRIBUTES = {
    20:  "beginning_stocks",
    28:  "production",
    57:  "imports",
    88:  "exports",
    125: "consumption",
    176: "ending_stocks",
}

# Country codes
COUNTRIES = {
    "US": "usa",
    "BR": "brazil",
    "CH": "china",
}


def fetch_livestock_bulk():
    """Baixa bulk CSV de livestock do USDA PSD."""
    r = requests.get(BULK_URL, timeout=60)
    r.raise_for_status()
    return zipfile.ZipFile(io.BytesIO(r.content))


def parse_livestock_data(zf):
    """Parseia CSV e extrai dados relevantes por commodity/country/year."""
    results = {}

    fname = [f for f in zf.namelist() if f.endswith(".csv")][0]
    with zf.open(fname) as f:
        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))

        for row in reader:
            comm_code = row.get("Commodity_Code", "").strip()
            if comm_code not in COMMODITIES:
                continue

            country_code = row.get("Country_Code", "").strip()
            attr_id = int(row.get("Attribute_ID", 0))
            year = int(row.get("Market_Year", 0))
            value_str = row.get("Value", "").strip()

            if not value_str or year < 2015:
                continue

            attr = ATTRIBUTES.get(attr_id)
            if not attr:
                continue

            region = COUNTRIES.get(country_code)
            if not region:
                continue

            sym = COMMODITIES[comm_code]["sym"]
            if sym not in results:
                results[sym] = {
                    "name": COMMODITIES[comm_code]["name"],
                    "usa": {},
                    "brazil": {},
                    "china": {},
                }

            if year not in results[sym][region]:
                results[sym][region][year] = {}
            results[sym][region][year][attr] = float(value_str)

    return results


def enrich_with_summaries(data):
    """Calcula summaries (current, avg_5y, deviation) para cada atributo."""
    for sym, d in data.items():
        for region in ["usa", "brazil", "china"]:
            region_data = d.get(region, {})
            if not region_data:
                continue

            years = sorted(k for k in region_data if isinstance(k, int))
            if len(years) < 2:
                continue

            summaries = {}
            for attr in ATTRIBUTES.values():
                series = [
                    region_data[y][attr]
                    for y in years
                    if attr in region_data.get(y, {})
                ]
                if len(series) < 3:
                    continue

                current = series[-1]
                prev_vals = series[-6:-1] if len(series) > 1 else [current]
                avg_5y = sum(prev_vals) / len(prev_vals)
                deviation = ((current - avg_5y) / avg_5y * 100) if avg_5y != 0 else 0

                # Anos correspondentes
                attr_years = [
                    y for y in years if attr in region_data.get(y, {})
                ]

                summaries[attr] = {
                    "current": round(current, 1),
                    "current_year": attr_years[-1] if attr_years else None,
                    "avg_5y": round(avg_5y, 1),
                    "deviation_pct": round(deviation, 1),
                    "history": [
                        {"year": attr_years[-(min(6, len(series))) + i], "value": round(series[-(min(6, len(series))) + i], 1)}
                        for i in range(min(6, len(series)))
                    ],
                    "unit": "1000 MT CWE",
                }

            d[region]["summaries"] = summaries

    return data


def main():
    print("Coletando dados PSD de prote\u00edna animal (livestock)...")

    zf = fetch_livestock_bulk()
    print(f"[OK] Bulk CSV baixado ({BULK_URL.split('/')[-1]})")

    data = parse_livestock_data(zf)
    data = enrich_with_summaries(data)

    output = {
        "generated_at": datetime.now().isoformat(),
        "source": "USDA FAS PSD Livestock Bulk CSV",
        "url": BULK_URL,
        "commodities": data,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"[OK] {len(data)} commodities salvas \u2192 livestock_psd.json")

    for sym, d in data.items():
        for region in ["usa", "brazil", "china"]:
            summ = d.get(region, {}).get("summaries", {})
            prod = summ.get("production", {})
            exp = summ.get("exports", {})
            if prod:
                print(
                    f"  {sym}/{region}: prod={prod.get('current','?')} "
                    f"(vs 5A: {prod.get('deviation_pct',0):+.1f}%) "
                    f"exp={exp.get('current','?')} 1000MT"
                )


if __name__ == "__main__":
    main()
