"""
collect_usda_fas.py — USDA FAS: Export Sales (ESR) + Production Supply Distribution (PSD)
AgriMacro v3.2

API Key required — reads USDA_FAS_KEY from .env
Base URL: https://apps.fas.usda.gov/OpenData/api/

Output: usda_fas.json
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Load .env
# ---------------------------------------------------------------------------

def load_env():
    env_paths = [
        Path(os.path.expanduser("~")) / "OneDrive" / "READET~1" / "agrimacro" / ".env",
        Path(os.path.expanduser("~")) / "OneDrive" / "Área de Trabalho" / "agrimacro" / ".env",
        Path("..") / ".env",
        Path(".") / ".env",
    ]
    for p in env_paths:
        if p.exists():
            with open(p, "r", encoding="utf-8-sig") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, val = line.split("=", 1)
                        os.environ[key.strip()] = val.strip()
            print(f"✅ .env carregado: {p}")
            return True
    print("❌ .env não encontrado")
    return False

load_env()

API_KEY = os.environ.get("USDA_FAS_KEY", "")
if not API_KEY:
    print("❌ USDA_FAS_KEY não encontrada no .env")
    sys.exit(1)

print(f"🔑 API Key: {API_KEY[:6]}...{API_KEY[-4:]}")

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

BASE_URL = "https://apps.fas.usda.gov/OpenData/api"

COMMODITIES = {
    "corn": {"name": "Corn", "esr_code": None, "psd_code": "0440000", "esr_search": "corn"},
    "soybeans": {"name": "Soybeans", "esr_code": None, "psd_code": "2222000", "esr_search": "soybeans"},
    "wheat": {"name": "Wheat", "esr_code": None, "psd_code": "0410000", "esr_search": "wheat"},
    "soybean_meal": {"name": "Soybean Meal", "esr_code": None, "psd_code": "4232000", "esr_search": "soybean meal"},
    "soybean_oil": {"name": "Soybean Oil", "esr_code": None, "psd_code": "4234000", "esr_search": "soybean oil"},
    "cotton": {"name": "Cotton", "esr_code": None, "psd_code": "2631000", "esr_search": "cotton"},
    "sugar": {"name": "Sugar", "esr_code": None, "psd_code": "0612000", "esr_search": "sugar"},
    "rice": {"name": "Rice", "esr_code": None, "psd_code": "0422110", "esr_search": "rice"},
    "beef": {"name": "Beef", "esr_code": None, "psd_code": "0111000", "esr_search": "beef"},
    "pork": {"name": "Pork", "esr_code": None, "psd_code": "0112000", "esr_search": "pork"},
    "coffee": {"name": "Coffee, Green", "esr_code": None, "psd_code": "0711000", "esr_search": "coffee"},
}

PSD_ATTRIBUTES_OF_INTEREST = [
    "Production", "Beginning Stocks", "Ending Stocks", "Domestic Consumption",
    "Total Supply", "Total Distribution", "Imports", "Exports", "Total Use",
    "Feed Dom. Consumption", "FSI Consumption", "Crush",
]

KEY_PSD_COUNTRIES = {"US": "United States", "BR": "Brazil", "AR": "Argentina", "CH": "China"}

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

import urllib.request
import urllib.error
import ssl


def fetch_json(url, retries=3, delay=2.0):
    headers = {
        "User-Agent": "AgriMacro/3.2",
        "Accept": "application/json",
        "API_KEY": API_KEY,
    }
    ctx = ssl.create_default_context()

    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")[:200]
            except:
                pass
            print(f"  HTTP {e.code} em {url}")
            if body:
                print(f"    Response: {body}")
            if e.code == 404:
                return None
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
        except Exception as e:
            print(f"  Erro: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    return None


# ---------------------------------------------------------------------------
# ESR — Export Sales
# ---------------------------------------------------------------------------

def get_esr_commodity_codes():
    print("[ESR] Buscando lista de commodities...")
    data = fetch_json(f"{BASE_URL}/esr/commodities")
    if not data:
        print("  ❌ Falha")
        return {}

    code_map = {}
    for item in data:
        name_lower = str(item.get("commodityName", "") or item.get("CommodityName", "")).lower()
        code = item.get("commodityCode") or item.get("CommodityCode")
        for key, cfg in COMMODITIES.items():
            if cfg["esr_search"] in name_lower and key not in code_map:
                code_map[key] = {"code": code, "name": name_lower.title()}
    print(f"  ✅ {len(code_map)} commodities mapeadas")
    for k, v in code_map.items():
        print(f"     {k}: {v['code']} ({v['name']})")
    return code_map


def get_esr_countries():
    print("[ESR] Buscando países...")
    data = fetch_json(f"{BASE_URL}/esr/countries")
    if not data:
        return {}
    m = {}
    for item in data:
        code = item.get("countryCode") or item.get("CountryCode")
        name = item.get("countryName") or item.get("CountryName", "")
        if code:
            m[code] = name
    print(f"  ✅ {len(m)} países")
    return m


def collect_esr_data(commodity_codes, country_map):
    current_year = datetime.now().year
    years_to_try = [current_year, current_year - 1]
    results = {}

    for key, code_info in commodity_codes.items():
        code = code_info["code"]
        print(f"\n[ESR] {COMMODITIES[key]['name']} (code={code})...")

        for year in years_to_try:
            url = f"{BASE_URL}/esr/exports/commodityCode/{code}/allCountries/marketYear/{year}"
            data = fetch_json(url)

            if data and len(data) > 0:
                print(f"  ✅ {year}: {len(data)} registros")
                weekly_totals = {}
                country_totals = {}

                for r in data:
                    week = r.get("weekEndingDate") or r.get("WeekEndingDate", "")
                    cc = r.get("countryCode") or r.get("CountryCode")
                    cn = country_map.get(cc, f"Code_{cc}")
                    ns = (r.get("netSalesCurrent") or r.get("NetSalesCurrent", 0)) or 0
                    ex = (r.get("exports") or r.get("Exports", 0)) or 0
                    os_val = (r.get("outstandingSales") or r.get("OutstandingSales", 0)) or 0

                    weekly_totals.setdefault(week, {"net_sales": 0, "exports": 0, "outstanding": 0})
                    weekly_totals[week]["net_sales"] += ns
                    weekly_totals[week]["exports"] += ex
                    weekly_totals[week]["outstanding"] += os_val

                    country_totals.setdefault(cn, {"net_sales": 0, "exports": 0, "outstanding": 0})
                    country_totals[cn]["net_sales"] += ns
                    country_totals[cn]["exports"] += ex
                    country_totals[cn]["outstanding"] += os_val

                sorted_weeks = sorted(weekly_totals.keys(), reverse=True)[:8]
                top_buyers = sorted(country_totals.items(), key=lambda x: abs(x[1]["net_sales"]), reverse=True)[:10]

                china_data = None
                for cname, cdata in country_totals.items():
                    if "china" in cname.lower():
                        china_data = {"country": cname, **cdata}
                        break

                results[key] = {
                    "commodity": COMMODITIES[key]["name"],
                    "market_year": year,
                    "total_records": len(data),
                    "latest_week": sorted_weeks[0] if sorted_weeks else None,
                    "recent_weekly": {w: weekly_totals[w] for w in sorted_weeks},
                    "top_buyers": [{"country": n, **v} for n, v in top_buyers],
                    "china": china_data,
                }
                break
            else:
                print(f"  ⚠️ {year}: sem dados")

        if key not in results:
            print(f"  ❌ Sem dados ESR")

    return results


# ---------------------------------------------------------------------------
# PSD
# ---------------------------------------------------------------------------

def collect_psd_data():
    current_year = datetime.now().year
    years = [current_year, current_year - 1, current_year - 2]
    results = {}

    for key, cfg in COMMODITIES.items():
        psd_code = cfg["psd_code"]
        if not psd_code:
            continue
        print(f"\n[PSD] {cfg['name']} (code={psd_code})...")
        yearly = {}

        for year in years:
            data = fetch_json(f"{BASE_URL}/psd/commodity/{psd_code}/world/year/{year}")
            if data and len(data) > 0:
                print(f"  ✅ {year}: {len(data)} atributos")
                attrs = {}
                unit = ""
                for r in data:
                    an = r.get("attributeDescription") or r.get("AttributeDescription", "")
                    val = r.get("value") or r.get("Value", 0)
                    unit = r.get("unitDescription") or r.get("UnitDescription", unit)
                    if an and (an in PSD_ATTRIBUTES_OF_INTEREST or any(a.lower() in an.lower() for a in PSD_ATTRIBUTES_OF_INTEREST)):
                        attrs[an] = val
                yearly[str(year)] = attrs
            else:
                print(f"  ⚠️ {year}: sem dados")

        if yearly:
            ly = str(max(int(y) for y in yearly.keys()))
            latest = yearly[ly]
            es = None
            tu = None
            for an, val in latest.items():
                if "ending" in an.lower() and "stock" in an.lower():
                    es = val
                if any(x in an.lower() for x in ["total use", "domestic consumption", "total distribution"]):
                    if tu is None or val > tu:
                        tu = val

            stu = round((es / tu) * 100, 2) if es and tu and tu > 0 else None

            py = str(int(ly) - 1)
            es_yoy = None
            if py in yearly:
                for an, val in yearly[py].items():
                    if "ending" in an.lower() and "stock" in an.lower():
                        if es and val and val > 0:
                            es_yoy = round(((es - val) / val) * 100, 2)
                        break

            results[key] = {
                "commodity": cfg["name"],
                "psd_code": psd_code,
                "years": yearly,
                "latest_year": ly,
                "analysis": {
                    "ending_stocks": es,
                    "stocks_to_use_pct": stu,
                    "ending_stocks_yoy_pct": es_yoy,
                    "is_tight": stu is not None and stu < 15,
                    "stocks_shrinking": es_yoy is not None and es_yoy < 0,
                },
            }

    return results


def collect_psd_country_data():
    current_year = datetime.now().year
    years = [current_year, current_year - 1]
    key_commodities = ["corn", "soybeans", "wheat", "cotton", "sugar", "coffee", "beef"]
    results = {}

    for cc, cn in KEY_PSD_COUNTRIES.items():
        print(f"\n[PSD Country] {cn} ({cc})...")
        results[cc] = {"name": cn, "commodities": {}}

        for key in key_commodities:
            cfg = COMMODITIES.get(key)
            if not cfg or not cfg["psd_code"]:
                continue
            for year in years:
                data = fetch_json(f"{BASE_URL}/psd/commodity/{cfg['psd_code']}/country/{cc}/year/{year}")
                if data and len(data) > 0:
                    attrs = {}
                    for r in data:
                        an = r.get("attributeDescription") or r.get("AttributeDescription", "")
                        val = r.get("value") or r.get("Value", 0)
                        if an and (an in PSD_ATTRIBUTES_OF_INTEREST or any(a.lower() in an.lower() for a in PSD_ATTRIBUTES_OF_INTEREST)):
                            attrs[an] = val
                    if attrs:
                        results[cc]["commodities"].setdefault(key, {})[str(year)] = attrs
                        break
            time.sleep(0.3)

    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("USDA FAS Collector — ESR + PSD")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Base URL: {BASE_URL}")
    print("=" * 60)

    possible_paths = [
        Path(os.path.expanduser("~")) / "OneDrive" / "READET~1" / "agrimacro" / "agrimacro-dash" / "public" / "data" / "processed",
        Path(os.path.expanduser("~")) / "OneDrive" / "Área de Trabalho" / "agrimacro" / "agrimacro-dash" / "public" / "data" / "processed",
    ]
    output_dir = Path(".")
    for p in possible_paths:
        if p.exists():
            output_dir = p
            break
    print(f"📁 Output: {output_dir}")
    output_file = output_dir / "usda_fas.json"

    if "--diagnose" in sys.argv:
        print("\n🔍 DIAGNÓSTICO")
        for name, path in [("ESR regions", "/esr/regions"), ("PSD commodities", "/psd/commodities")]:
            url = f"{BASE_URL}{path}"
            print(f"  [{name}] {url}")
            data = fetch_json(url, retries=1)
            print(f"    {'✅ OK — ' + str(len(data)) + ' registros' if data else '❌ Falhou'}")
        sys.exit(0)

    # Teste rápido
    print("\n🔍 Teste rápido...")
    test = fetch_json(f"{BASE_URL}/esr/regions", retries=1)
    if not test:
        print("❌ API não respondeu. Verifique USDA_FAS_KEY no .env")
        result = {
            "metadata": {"source": "USDA FAS", "collected_at": datetime.now(timezone.utc).isoformat(), "error": "Auth failed"},
            "export_sales": {}, "psd_world": {}, "psd_countries": {}, "analysis_summary": ["❌ Falhou"],
        }
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        sys.exit(1)
    print("✅ API acessível!\n")

    # Coleta
    print("=" * 40)
    print("FASE 1: ESR — Export Sales Semanais")
    print("=" * 40)
    esr_codes = get_esr_commodity_codes()
    countries = get_esr_countries()
    esr_data = collect_esr_data(esr_codes, countries)

    print("\n" + "=" * 40)
    print("FASE 2: PSD — World Supply & Demand")
    print("=" * 40)
    psd_world = collect_psd_data()

    print("\n" + "=" * 40)
    print("FASE 3: PSD — S&D por País")
    print("=" * 40)
    psd_countries = collect_psd_country_data()

    # Análise
    summary = []
    for key, psd in psd_world.items():
        a = psd.get("analysis", {})
        line = f"{psd['commodity']}: "
        if a.get("stocks_to_use_pct") is not None:
            line += f"stocks/use={a['stocks_to_use_pct']}%"
            if a["is_tight"]:
                line += " ⚠️ TIGHT"
        if a.get("ending_stocks_yoy_pct") is not None:
            d = "↓" if a["ending_stocks_yoy_pct"] < 0 else "↑"
            line += f", ending stocks YoY {d}{abs(a['ending_stocks_yoy_pct'])}%"
            if a["stocks_shrinking"]:
                line += " 📉"
        summary.append(line)
    for key, esr in esr_data.items():
        if esr.get("china"):
            c = esr["china"]
            summary.append(f"{esr['commodity']} ESR: China net={c.get('net_sales',0):,.0f} outstanding={c.get('outstanding',0):,.0f}")

    result = {
        "metadata": {
            "source": "USDA FAS — apps.fas.usda.gov/OpenData",
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "version": "3.2",
            "sections": {"esr": len(esr_data), "psd_world": len(psd_world), "psd_countries": len(psd_countries)},
        },
        "export_sales": esr_data,
        "psd_world": psd_world,
        "psd_countries": psd_countries,
        "analysis_summary": summary,
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    size = output_file.stat().st_size / 1024
    print(f"\n{'='*60}")
    print(f"✅ Salvo: {output_file} ({size:.1f} KB)")
    print(f"   ESR: {len(esr_data)} | PSD World: {len(psd_world)} | PSD Countries: {len(psd_countries)}")
    for line in summary:
        print(f"   {line}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
