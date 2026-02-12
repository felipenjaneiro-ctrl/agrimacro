import requests, zipfile, io, csv, json
from pathlib import Path
from datetime import datetime

OUT = Path(r"C:\Users\felip\OneDrive\Área de Trabalho\agrimacro\agrimacro-dash\public\data\processed")
BASE = "https://apps.fas.usda.gov/psdonline/downloads"

PSD_SOURCES = {
    "CT": {"file": "psd_cotton_csv.zip", "commodity": "Cotton", "attr": "Ending Stocks", "country": "United States"},
    "ZM": {"file": "psd_oilseeds_csv.zip", "commodity": "Meal, Soybean", "attr": "Ending Stocks", "country": "United States"},
    "ZL": {"file": "psd_oilseeds_csv.zip", "commodity": "Oil, Soybean", "attr": "Ending Stocks", "country": "United States"},
    "SB": {"file": "psd_sugar_csv.zip", "commodity": "Sugar, Centrifugal", "attr": "Ending Stocks", "country": "United States"},
    "KC": {"file": "psd_coffee_csv.zip", "commodity": "Coffee, Green", "attr": "Ending Stocks", "country": "United States"},
    "CC": {"file": "psd_alldata_csv.zip", "commodity": "Cocoa Beans", "attr": "Ending Stocks", "country": "United States"},
    "OJ": {"file": "psd_alldata_csv.zip", "commodity": "Juice, Orange", "attr": "Ending Stocks", "country": "United States"},
}

cache = {}

def download_and_parse(filename):
    if filename in cache:
        return cache[filename]
    url = f"{BASE}/{filename}"
    print(f"  Baixando {filename}...")
    r = requests.get(url, timeout=120)
    if r.status_code != 200:
        print(f"    ERRO: [{r.status_code}]")
        return []
    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
    print(f"    CSV: {csv_name} ({len(r.content)/1024:.0f} KB)")
    with z.open(csv_name) as f:
        text = io.TextIOWrapper(f, encoding="utf-8-sig")
        reader = csv.DictReader(text)
        rows = list(reader)
    print(f"    Registros: {len(rows)}")
    cache[filename] = rows
    return rows

def extract_ending_stocks(rows, commodity_search, attr_search, country_search):
    results = []
    for row in rows:
        commodity = row.get("Commodity_Description", "")
        attr = row.get("Attribute_Description", "")
        country = row.get("Country_Name", "")
        if (commodity_search.lower() in commodity.lower() and
            attr_search.lower() in attr.lower() and
            country_search.lower() in country.lower()):
            year = row.get("Market_Year", "")
            value = row.get("Value", "").replace(",", "")
            unit = row.get("Unit_Description", "")
            try:
                val = float(value)
                results.append({"year": int(year) if year else 0, "value": val, "unit": unit,
                                "commodity": commodity, "country": country})
            except (ValueError, TypeError):
                pass
    results.sort(key=lambda x: x["year"])
    return results

def main():
    print("=" * 60)
    print("USDA PSD Online - Coletor de Ending Stocks v2")
    print("=" * 60)
    psd_data = {}
    
    # For alldata, first find exact commodity names
    for symbol, cfg in PSD_SOURCES.items():
        filename = cfg["file"]
        print(f"\n{symbol}: {cfg['commodity']} ({cfg['country']})")
        rows = download_and_parse(filename)
        if not rows:
            continue
        
        # If alldata, show matching commodities for debug
        if filename == "psd_alldata_csv.zip":
            search = cfg["commodity"].split(",")[0].lower()
            matches = sorted(set(r.get("Commodity_Description","") for r in rows if search in r.get("Commodity_Description","").lower()))
            print(f"    Commodities matching '{search}': {matches[:8]}")
        
        stocks = extract_ending_stocks(rows, cfg["commodity"], cfg["attr"], cfg["country"])
        
        if not stocks:
            # Try broader search
            broad = cfg["commodity"].split(",")[0]
            print(f"    Tentando busca ampla: '{broad}'...")
            stocks = extract_ending_stocks(rows, broad, cfg["attr"], cfg["country"])
        
        if stocks:
            recent = [s for s in stocks if s["year"] >= datetime.now().year - 6]
            latest = recent[-1] if recent else stocks[-1]
            avg_5y = recent[:-1] if len(recent) > 1 else recent
            avg_val = sum(s["value"] for s in avg_5y) / len(avg_5y) if avg_5y else 0
            deviation = ((latest["value"] - avg_val) / avg_val) * 100 if avg_val > 0 else 0
            
            psd_data[symbol] = {
                "current": latest["value"],
                "avg_5y": round(avg_val, 2),
                "deviation": round(deviation, 1),
                "unit": latest["unit"],
                "year": latest["year"],
                "history": [{"year": s["year"], "value": s["value"]} for s in recent],
                "source": "USDA PSD Online"
            }
            print(f"    OK: {latest['value']:,.0f} {latest['unit']} ({latest['year']}) | Desvio: {deviation:+.1f}%")
        else:
            print(f"    SEM DADOS")
    
    out_file = OUT / "psd_ending_stocks.json"
    output = {"timestamp": datetime.now().isoformat(), "source": "USDA PSD Online (bulk CSV)", "commodities": psd_data}
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\n[SAVED] {out_file}")
    print(f"\nRESUMO: {len(psd_data)}/{len(PSD_SOURCES)} commodities")
    for sym, d in psd_data.items():
        print(f"  {sym:4s} | {d['current']:>10,.0f} {d['unit']:25s} | Desvio: {d['deviation']:+6.1f}%")

if __name__ == "__main__":
    main()
