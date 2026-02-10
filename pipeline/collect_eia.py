"""
collect_eia.py — EIA: Energy Information Administration
AgriMacro v3.2

Coleta dados de energia relevantes para commodities agrícolas:
  - Estoques petróleo (Weekly Petroleum Status)
  - Etanol produção semanal (demanda milho)
  - Etanol estoques
  - Diesel preço retail (custo produção ag)
  - WTI forecast (STEO)

API Key: lê EIA_API_KEY do .env
Base URL: https://api.eia.gov/v2/

Output: eia_data.json
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

API_KEY = os.environ.get("EIA_API_KEY", "")
if not API_KEY:
    print("❌ EIA_API_KEY não encontrada no .env")
    sys.exit(1)

print(f"🔑 EIA Key: {API_KEY[:6]}...{API_KEY[-4:]}")

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

BASE_URL = "https://api.eia.gov/v2"

# Séries relevantes para Agro
# Formato: (nome, route, params)
SERIES = [
    {
        "id": "crude_stocks",
        "name": "Crude Oil Stocks (Weekly)",
        "route": "/petroleum/stoc/wstk/data/",
        "params": {
            "frequency": "weekly",
            "data[0]": "value",
            "facets[product][]": "EPC0",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 12,
        },
    },
    {
        "id": "ethanol_production",
        "name": "Ethanol Production (Weekly)",
        "route": "/petroleum/sum/sndw/data/",
        "params": {
            "frequency": "weekly",
            "data[0]": "value",
            "facets[series][]": "W_EPOOXE_YOP_NUS_MBBLD",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 12,
        },
    },
    {
        "id": "ethanol_stocks",
        "name": "Ethanol Stocks (Weekly)",
        "route": "/petroleum/sum/sndw/data/",
        "params": {
            "frequency": "weekly",
            "data[0]": "value",
            "facets[series][]": "W_EPOOXE_SAE_NUS_MBBL",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 12,
        },
    },
    {
        "id": "diesel_retail",
        "name": "Diesel Retail Price (Weekly)",
        "route": "/petroleum/pri/gnd/data/",
        "params": {
            "frequency": "weekly",
            "data[0]": "value",
            "facets[series][]": "EMD_EPD2D_PTE_NUS_DPG",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 12,
        },
    },
    {
        "id": "gasoline_retail",
        "name": "Gasoline Retail Price (Weekly)",
        "route": "/petroleum/pri/gnd/data/",
        "params": {
            "frequency": "weekly",
            "data[0]": "value",
            "facets[series][]": "EMM_EPMR_PTE_NUS_DPG",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 12,
        },
    },
    {
        "id": "wti_spot",
        "name": "WTI Spot Price (Weekly)",
        "route": "/petroleum/pri/spt/data/",
        "params": {
            "frequency": "weekly",
            "data[0]": "value",
            "facets[series][]": "RWTC",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 12,
        },
    },
    {
        "id": "natural_gas_spot",
        "name": "Natural Gas Henry Hub Spot (Monthly)",
        "route": "/natural-gas/pri/sum/data/",
        "params": {
            "frequency": "monthly",
            "data[0]": "value",
            "facets[series][]": "RNGWHHD",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 6,
        },
    },
]

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

import urllib.request
import urllib.error
import urllib.parse
import ssl


def fetch_eia(route, params, retries=3, delay=2.0):
    """Fetch from EIA API v2."""
    # Add API key
    all_params = dict(params)
    all_params["api_key"] = API_KEY

    # Build URL with query string
    query = urllib.parse.urlencode(all_params, doseq=True)
    url = f"{BASE_URL}{route}?{query}"

    ctx = ssl.create_default_context()

    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "AgriMacro/3.2",
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")[:300]
            except:
                pass
            print(f"  HTTP {e.code}")
            if body:
                print(f"    {body[:200]}")
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
# Coleta
# ---------------------------------------------------------------------------

def collect_all_series():
    """Coleta todas as séries definidas."""
    results = {}
    ok_count = 0

    for series in SERIES:
        sid = series["id"]
        name = series["name"]
        print(f"\n[EIA] {name}...")

        data = fetch_eia(series["route"], series["params"])

        if data and "response" in data and "data" in data["response"]:
            records = data["response"]["data"]
            print(f"  ✅ {len(records)} registros")

            # Extrair valores
            values = []
            for r in records:
                period = r.get("period", "")
                value = r.get("value")
                unit = r.get("units") or r.get("unit", "")
                desc = r.get("series-description") or r.get("seriesDescription", "")

                if value is not None:
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        pass

                values.append({
                    "period": period,
                    "value": value,
                    "unit": unit,
                })

            # Calcular variações
            latest_val = values[0]["value"] if values and values[0]["value"] is not None else None
            prev_val = values[1]["value"] if len(values) > 1 and values[1]["value"] is not None else None
            month_ago_val = values[3]["value"] if len(values) > 3 and values[3]["value"] is not None else None

            wow_change = None
            mom_change = None
            if latest_val is not None and prev_val is not None and prev_val != 0:
                wow_change = round(((latest_val - prev_val) / prev_val) * 100, 2)
            if latest_val is not None and month_ago_val is not None and month_ago_val != 0:
                mom_change = round(((latest_val - month_ago_val) / month_ago_val) * 100, 2)

            results[sid] = {
                "name": name,
                "latest_period": values[0]["period"] if values else None,
                "latest_value": latest_val,
                "unit": values[0]["unit"] if values else "",
                "wow_change_pct": wow_change,
                "mom_change_pct": mom_change,
                "history": values,
            }
            ok_count += 1
        else:
            print(f"  ❌ Sem dados")
            # Tentar mostrar o erro
            if data and "error" in data:
                print(f"    Erro: {data['error']}")
            results[sid] = {
                "name": name,
                "latest_period": None,
                "latest_value": None,
                "error": "No data returned",
            }

    return results, ok_count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("EIA Collector — Energy Data for Ag")
    print(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Base URL: {BASE_URL}")
    print("=" * 60)

    # Output path
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
    output_file = output_dir / "eia_data.json"

    # Diagnóstico
    if "--diagnose" in sys.argv:
        print("\n🔍 Testando API...")
        test = fetch_eia("/petroleum/pri/spt/data/", {
            "frequency": "weekly",
            "data[0]": "value",
            "facets[series][]": "RWTC",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 1,
        })
        if test and "response" in test:
            print("✅ EIA API funcionando!")
        else:
            print("❌ EIA API falhou")
        sys.exit(0)

    # Coleta
    series_data, ok_count = collect_all_series()

    # Resumo analítico
    summary = []
    if "wti_spot" in series_data and series_data["wti_spot"].get("latest_value"):
        wti = series_data["wti_spot"]
        line = f"WTI: ${wti['latest_value']:.2f}/bbl"
        if wti.get("wow_change_pct") is not None:
            d = "↑" if wti["wow_change_pct"] > 0 else "↓"
            line += f" ({d}{abs(wti['wow_change_pct'])}% WoW)"
        summary.append(line)

    if "diesel_retail" in series_data and series_data["diesel_retail"].get("latest_value"):
        diesel = series_data["diesel_retail"]
        line = f"Diesel: ${diesel['latest_value']:.3f}/gal"
        if diesel.get("mom_change_pct") is not None:
            d = "↑" if diesel["mom_change_pct"] > 0 else "↓"
            line += f" ({d}{abs(diesel['mom_change_pct'])}% MoM)"
        summary.append(line)

    if "ethanol_production" in series_data and series_data["ethanol_production"].get("latest_value"):
        eth = series_data["ethanol_production"]
        line = f"Ethanol prod: {eth['latest_value']} {eth.get('unit', 'MBbl/d')}"
        if eth.get("wow_change_pct") is not None:
            d = "↑" if eth["wow_change_pct"] > 0 else "↓"
            line += f" ({d}{abs(eth['wow_change_pct'])}% WoW)"
        summary.append(line)

    if "crude_stocks" in series_data and series_data["crude_stocks"].get("latest_value"):
        crude = series_data["crude_stocks"]
        line = f"Crude stocks: {crude['latest_value']:,.0f} {crude.get('unit', 'MBbl')}"
        if crude.get("wow_change_pct") is not None:
            d = "↑" if crude["wow_change_pct"] > 0 else "↓"
            line += f" ({d}{abs(crude['wow_change_pct'])}% WoW)"
        summary.append(line)

    # Output
    result = {
        "metadata": {
            "source": "EIA — api.eia.gov/v2",
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "version": "3.2",
            "series_ok": ok_count,
            "series_total": len(SERIES),
        },
        "series": series_data,
        "analysis_summary": summary,
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    size = output_file.stat().st_size / 1024
    print(f"\n{'='*60}")
    print(f"✅ Salvo: {output_file} ({size:.1f} KB)")
    print(f"   Séries OK: {ok_count}/{len(SERIES)}")
    for line in summary:
        print(f"   {line}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
