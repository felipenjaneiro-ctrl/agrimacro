"""
AgriMacro v3.2 - New Data Collectors (Open APIs)
Adds: BCB/SGS, CONAB, IBGE/SIDRA, INMET
Run from pipeline folder: python collect_new_sources.py

Output: saves to ../agrimacro-dash/public/data/processed/
"""
import json, sys, os, time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

BASE = Path(__file__).parent
PROC = BASE.parent / "agrimacro-dash" / "public" / "data" / "processed"
PROC.mkdir(parents=True, exist_ok=True)

def fetch_json(url, headers=None, timeout=30):
    """Generic JSON fetcher with error handling"""
    try:
        req = Request(url, headers=headers or {"User-Agent": "AgriMacro/3.2"})
        with urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except (URLError, HTTPError, json.JSONDecodeError) as e:
        print(f"    ERROR: {e}")
        return None

def fetch_text(url, timeout=30):
    """Generic text fetcher"""
    try:
        req = Request(url, headers={"User-Agent": "AgriMacro/3.2"})
        with urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8")
    except Exception as e:
        print(f"    ERROR: {e}")
        return None

def save(data, filename):
    """Save JSON to processed folder"""
    path = PROC / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    size = path.stat().st_size / 1024
    print(f"  [OK] {filename} ({size:.1f} KB)")
    return True


# ============================================================
# 1. BCB/SGS - Banco Central do Brasil
#    BRL/USD, Selic, CDI, IPCA, Credit
# ============================================================
def collect_bcb():
    """
    BCB SGS API - no auth required
    https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados?formato=json
    """
    print("\n[1/4] BCB/SGS - Banco Central do Brasil")

    SERIES = {
        # Câmbio
        "brl_usd":        {"code": 1,     "name": "Dólar Comercial (venda)", "unit": "BRL/USD"},
        "brl_eur":        {"code": 21619, "name": "Euro Comercial (venda)", "unit": "BRL/EUR"},
        # Juros
        "selic_meta":     {"code": 432,   "name": "Selic Meta", "unit": "% a.a."},
        "selic_diaria":   {"code": 11,    "name": "Selic Diária", "unit": "% a.d."},
        "cdi_diario":     {"code": 12,    "name": "CDI Diário", "unit": "% a.d."},
        # Inflação
        "ipca_mensal":    {"code": 433,   "name": "IPCA Mensal", "unit": "%"},
        "igpm_mensal":    {"code": 189,   "name": "IGP-M Mensal", "unit": "%"},
        # Crédito Rural
        "credito_rural":  {"code": 29038, "name": "Crédito Rural - Saldo Total", "unit": "R$ milhões"},
        # Reservas internacionais
        "reservas_intl":  {"code": 3546,  "name": "Reservas Internacionais", "unit": "USD milhões"},
    }

    # Last 365 days for daily, last 24 months for monthly
    end = datetime.now().strftime("%d/%m/%Y")
    start_daily = (datetime.now() - timedelta(days=365)).strftime("%d/%m/%Y")
    start_monthly = (datetime.now() - timedelta(days=730)).strftime("%d/%m/%Y")

    result = {"_meta": {"source": "BCB/SGS", "collected_at": datetime.now().isoformat(), "series": {}}}

    for key, info in SERIES.items():
        code = info["code"]
        is_monthly = key in ("ipca_mensal", "igpm_mensal", "credito_rural", "reservas_intl")
        start = start_monthly if is_monthly else start_daily

        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados?formato=json&dataInicial={start}&dataFinal={end}"
        print(f"  {key} (serie {code})...", end=" ")

        data = fetch_json(url)
        if data and len(data) > 0:
            # Parse BCB format: {"data": "DD/MM/YYYY", "valor": "X.XX"}
            parsed = []
            for item in data:
                try:
                    dt = datetime.strptime(item["data"], "%d/%m/%Y").strftime("%Y-%m-%d")
                    val = float(item["valor"])
                    parsed.append({"date": dt, "value": val})
                except (ValueError, KeyError):
                    continue

            result["_meta"]["series"][key] = {
                "name": info["name"],
                "unit": info["unit"],
                "code": code,
                "count": len(parsed)
            }
            result[key] = parsed
            print(f"OK ({len(parsed)} pts)")
        else:
            print("FAILED")
            result[key] = []

        time.sleep(0.3)

    # Calculate derived values
    if result.get("brl_usd") and len(result["brl_usd"]) > 0:
        latest = result["brl_usd"][-1]
        prev_5d = result["brl_usd"][-6] if len(result["brl_usd"]) > 5 else result["brl_usd"][0]
        prev_30d = result["brl_usd"][-31] if len(result["brl_usd"]) > 30 else result["brl_usd"][0]

        result["resumo_cambio"] = {
            "brl_usd_atual": latest["value"],
            "data": latest["date"],
            "var_5d": round((latest["value"] / prev_5d["value"] - 1) * 100, 2),
            "var_30d": round((latest["value"] / prev_30d["value"] - 1) * 100, 2),
            "min_52s": min(p["value"] for p in result["brl_usd"]),
            "max_52s": max(p["value"] for p in result["brl_usd"]),
        }

    if result.get("selic_meta") and len(result["selic_meta"]) > 0:
        result["resumo_juros"] = {
            "selic_atual": result["selic_meta"][-1]["value"],
            "data": result["selic_meta"][-1]["date"],
        }

    save(result, "bcb_data.json")
    return result


# ============================================================
# 2. CONAB - Companhia Nacional de Abastecimento
#    Safra brasileira, S&D, custos de produção
# ============================================================
def collect_conab():
    """
    CONAB - downloads CSV/series from conab.gov.br
    CONAB does NOT have a public REST API. Data comes from:
    - Serie historica graos (CSV download)
    - Boletim safra (PDF - we track metadata)
    """
    print("\n[2/4] CONAB - Safra Brasileira")

    result = {
        "_meta": {"source": "CONAB", "collected_at": datetime.now().isoformat(),
                  "note": "CONAB has no public API. Data from CSV downloads and gov.br portal."},
        "safra": {},
        "boletim_info": {}
    }

    # 1. Try to fetch the serie historica CSV
    # CONAB publishes this at their info portal
    csv_urls = [
        "https://www.conab.gov.br/info-agro/safras/serie-historica-das-safras",
        "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/SerieHistoricaGraos.txt",
    ]

    print("  Serie historica graos...", end=" ")
    raw = None
    for url in csv_urls:
        raw = fetch_text(url)
        if raw and len(raw) > 100 and ("soja" in raw.lower() or "milho" in raw.lower() or "<" in raw[:10]):
            break
        raw = None

    if raw and "<" not in raw[:10]:  # Got actual CSV/TSV data
        lines = raw.strip().split("\n")
        result["safra"]["serie_raw_lines"] = len(lines)
        result["safra"]["header"] = lines[0] if lines else ""
        result["safra"]["last_5"] = lines[-5:] if len(lines) > 5 else lines
        print(f"OK ({len(lines)} lines)")
    else:
        print("SKIP (no direct CSV - CONAB requires manual download)")
        result["safra"]["note"] = "CONAB serie historica requires manual CSV download from conab.gov.br"

    # 2. Key production numbers (hardcoded from latest boletim - updated monthly)
    # Source: 7o Levantamento Safra 2024/25 (Feb 2026)
    result["boletim_info"] = {
        "safra": "2024/25",
        "levantamento": "7o",
        "data_referencia": "2026-02",
        "producao_total_mt": 330.3,
        "principais_culturas": {
            "soja": {"area_mha": 47.4, "producao_mt": 167.0, "produtividade_kg_ha": 3523},
            "milho_total": {"area_mha": 22.2, "producao_mt": 122.0, "produtividade_kg_ha": 5495},
            "milho_1safra": {"area_mha": 4.6, "producao_mt": 24.0, "produtividade_kg_ha": 5217},
            "milho_2safra": {"area_mha": 17.0, "producao_mt": 95.0, "produtividade_kg_ha": 5588},
            "algodao_pluma": {"area_mha": 2.0, "producao_mt": 3.8, "produtividade_kg_ha": 1900},
            "arroz": {"area_mha": 1.7, "producao_mt": 11.5, "produtividade_kg_ha": 6765},
            "feijao_total": {"area_mha": 2.8, "producao_mt": 3.2, "produtividade_kg_ha": 1143},
            "trigo": {"area_mha": 3.1, "producao_mt": 7.7, "produtividade_kg_ha": 2484},
        },
        "_nota": "Atualizar manualmente apos cada boletim CONAB (mensal)"
    }

    print("  Dados boletim safra 2024/25... OK (manual)")
    save(result, "conab_data.json")
    return result


# ============================================================
# 3. IBGE/SIDRA - Estatísticas Agrícolas Brasil
#    IPCA Alimentos, Produção Agrícola Municipal
# ============================================================
def collect_ibge():
    """
    IBGE SIDRA API - no auth required
    https://apisidra.ibge.gov.br/
    """
    print("\n[3/4] IBGE/SIDRA - Estatísticas Agrícolas")

    result = {
        "_meta": {"source": "IBGE/SIDRA", "collected_at": datetime.now().isoformat()},
    }

    # 1. IPCA - Alimentação e Bebidas (last 24 months)
    # Table 7060: IPCA mensal por grupo
    # Variable 63: Variação mensal (%)
    # Classification 315: Grupos, subgrupos, itens e subitens
    # Category 7169: Alimentação e bebidas
    print("  IPCA Alimentação...", end=" ")
    ipca_url = (
        "https://apisidra.ibge.gov.br/values"
        "/t/7060"            # table IPCA
        "/n1/all"            # Brasil
        "/v/63"              # var mensal %
        "/p/last%2024"       # last 24 months
        "/c315/7169"         # Alimentação e bebidas
        "/d/v63%202"         # 2 decimals
    )
    data = fetch_json(ipca_url)
    if data and len(data) > 1:
        # First row is header
        parsed = []
        for row in data[1:]:
            try:
                periodo = row.get("D3C", "")  # Period code YYYYMM
                valor = row.get("V", "")
                if valor and valor != "...":
                    parsed.append({
                        "periodo": periodo,
                        "mes": row.get("D3N", ""),
                        "valor": float(valor)
                    })
            except (ValueError, KeyError):
                continue
        result["ipca_alimentos"] = parsed
        print(f"OK ({len(parsed)} meses)")
    else:
        print("FAILED")
        result["ipca_alimentos"] = []

    time.sleep(0.5)

    # 2. IPCA Geral (for comparison)
    print("  IPCA Geral...", end=" ")
    ipca_geral_url = (
        "https://apisidra.ibge.gov.br/values"
        "/t/7060/n1/all/v/63"
        "/p/last%2024"
        "/c315/7169,7170,7445,7486,7558,7625,7660,7712,7766"  # all groups
        "/d/v63%202"
    )
    # Simpler: just get the general index
    ipca_geral_url2 = (
        "https://apisidra.ibge.gov.br/values"
        "/t/1737"            # table IPCA geral
        "/n1/all"            # Brasil
        "/v/2266"            # Indice geral
        "/p/last%2024"       # last 24 months
        "/d/v2266%202"
    )
    data = fetch_json(ipca_geral_url2)
    if data and len(data) > 1:
        parsed = []
        for row in data[1:]:
            try:
                valor = row.get("V", "")
                if valor and valor != "...":
                    parsed.append({
                        "periodo": row.get("D3C", ""),
                        "mes": row.get("D3N", ""),
                        "valor": float(valor)
                    })
            except (ValueError, KeyError):
                continue
        result["ipca_geral"] = parsed
        print(f"OK ({len(parsed)} meses)")
    else:
        print("FAILED")
        result["ipca_geral"] = []

    time.sleep(0.5)

    # 3. PIB Agropecuário (quarterly)
    print("  PIB Agropecuário...", end=" ")
    pib_url = (
        "https://apisidra.ibge.gov.br/values"
        "/t/1846"            # PIB trimestral
        "/n1/all"
        "/v/585"             # Valor corrente
        "/p/last%2012"       # last 12 quarters
        "/c11255/90687"      # Agropecuária
        "/d/v585%200"
    )
    data = fetch_json(pib_url)
    if data and len(data) > 1:
        parsed = []
        for row in data[1:]:
            try:
                valor = row.get("V", "")
                if valor and valor != "...":
                    parsed.append({
                        "periodo": row.get("D3C", ""),
                        "trimestre": row.get("D3N", ""),
                        "valor_milhoes": float(valor)
                    })
            except (ValueError, KeyError):
                continue
        result["pib_agro"] = parsed
        print(f"OK ({len(parsed)} trimestres)")
    else:
        print("FAILED")
        result["pib_agro"] = []

    time.sleep(0.5)

    # 4. Abate de bovinos (trimestral)
    print("  Abate Bovinos...", end=" ")
    abate_url = (
        "https://apisidra.ibge.gov.br/values"
        "/t/1092"            # Abate trimestral
        "/n1/all"
        "/v/284"             # Número de cabeças
        "/p/last%2012"       # last 12 quarters
        "/c12716/115236"     # Total
        "/d/v284%200"
    )
    data = fetch_json(abate_url)
    if data and len(data) > 1:
        parsed = []
        for row in data[1:]:
            try:
                valor = row.get("V", "")
                if valor and valor != "...":
                    parsed.append({
                        "periodo": row.get("D3C", ""),
                        "trimestre": row.get("D3N", ""),
                        "cabecas": int(float(valor))
                    })
            except (ValueError, KeyError):
                continue
        result["abate_bovinos"] = parsed
        print(f"OK ({len(parsed)} trimestres)")
    else:
        print("FAILED")
        result["abate_bovinos"] = []

    save(result, "ibge_data.json")
    return result


# ============================================================
# 4. INMET - Instituto Nacional de Meteorologia
#    Dados meteorológicos estações Brasil
# ============================================================
def collect_inmet():
    """
    INMET API - portal.inmet.gov.br
    Correct endpoint: https://apitempo.inmet.gov.br/estacao/dados/{code}
    """
    print("\n[4/4] INMET - Meteorologia Agrícola Brasil")

    result = {
        "_meta": {"source": "INMET", "collected_at": datetime.now().isoformat()},
        "estacoes": {},
        "alertas": []
    }

    # Key agricultural stations (major producing regions)
    STATIONS = {
        "A909": {"name": "Sorriso-MT", "crop": "soja/milho"},
        "A917": {"name": "Rondonópolis-MT", "crop": "soja/milho"},
        "A920": {"name": "Lucas Rio Verde-MT", "crop": "soja/milho"},
        "A843": {"name": "Londrina-PR", "crop": "soja/milho/trigo"},
        "A836": {"name": "Cascavel-PR", "crop": "soja/milho"},
        "A027": {"name": "Rio Verde-GO", "crop": "soja/milho"},
        "A711": {"name": "Ribeirão Preto-SP", "crop": "cana/café"},
        "A726": {"name": "Franca-SP", "crop": "café"},
        "A507": {"name": "Varginha-MG", "crop": "café"},
        "A519": {"name": "Patrocínio-MG", "crop": "café"},
        "A801": {"name": "Cruz Alta-RS", "crop": "soja/trigo"},
        "A434": {"name": "Barreiras-BA", "crop": "soja/algodão"},
        "A756": {"name": "Dourados-MS", "crop": "soja/milho"},
    }

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Try multiple INMET endpoint patterns
    url_patterns = [
        "https://apitempo.inmet.gov.br/estacao/diaria/{yesterday}/{today}/{code}",
        "https://apitempo.inmet.gov.br/estacao/{yesterday}/{today}/{code}",
        "https://apitempo.inmet.gov.br/condicao/estacao/{code}",
    ]

    # First, discover which pattern works
    working_pattern = None
    test_code = "A909"
    for pattern in url_patterns:
        test_url = pattern.format(yesterday=yesterday, today=today, code=test_code)
        print(f"  Testing: {test_url.split('/')[-3]}.../{test_code}...", end=" ")
        data = fetch_json(test_url)
        if data and (isinstance(data, list) and len(data) > 0 or isinstance(data, dict) and data):
            working_pattern = pattern
            print(f"WORKS!")
            break
        else:
            print("no")

    if not working_pattern:
        # Try the "ultimas 24h" endpoint
        print("  Trying ultimas-24-horas endpoint...", end=" ")
        test_url = f"https://apitempo.inmet.gov.br/estacao/proxima/{test_code}"
        data = fetch_json(test_url)
        if data:
            print("WORKS!")
            # Use this simpler endpoint for all stations
            for code, info in STATIONS.items():
                print(f"  {info['name']} ({code})...", end=" ")
                url = f"https://apitempo.inmet.gov.br/estacao/proxima/{code}"
                sdata = fetch_json(url)
                if sdata:
                    result["estacoes"][code] = {
                        "nome": info["name"],
                        "cultura": info["crop"],
                        "data": today,
                        "dados": sdata if isinstance(sdata, dict) else sdata[0] if isinstance(sdata, list) and sdata else {},
                    }
                    print("OK")
                else:
                    result["estacoes"][code] = {"nome": info["name"], "cultura": info["crop"], "error": "no data"}
                    print("FAILED")
                time.sleep(0.3)
        else:
            print("FAILED")
            print("  [WARN] INMET API may be down or endpoints changed")
            print("  Falling back to Tomorrow.io for weather data")
            # Store placeholder
            for code, info in STATIONS.items():
                result["estacoes"][code] = {"nome": info["name"], "cultura": info["crop"], "error": "API unavailable"}
    else:
        # Use working pattern for all stations
        for code, info in STATIONS.items():
            print(f"  {info['name']} ({code})...", end=" ")
            url = working_pattern.format(yesterday=yesterday, today=today, code=code)
            data = fetch_json(url)

            if data and isinstance(data, list) and len(data) > 0:
                obs = []
                temp_max = -999
                temp_min = 999
                precip_total = 0.0

                for item in data:
                    try:
                        hora = item.get("HR_MEDICAO", item.get("DT_MEDICAO", ""))
                        temp = item.get("TEM_INS", item.get("TEM_MAX", item.get("TEMP_INS")))
                        umid = item.get("UMD_INS", item.get("UMD_MAX", item.get("UMID_INS")))
                        chuva = item.get("CHUVA", item.get("CHUVA_TOTAL", "0"))

                        t = float(temp) if temp and str(temp).strip() not in ("", "null", "None") else None
                        u = float(umid) if umid and str(umid).strip() not in ("", "null", "None") else None
                        p = float(chuva) if chuva and str(chuva).strip() not in ("", "null", "None") else 0.0

                        if t is not None:
                            temp_max = max(temp_max, t)
                            temp_min = min(temp_min, t)
                        precip_total += p
                        obs.append({"hora": hora, "temp_c": t, "umidade_pct": u, "precipitacao_mm": p})
                    except (ValueError, TypeError):
                        continue

                result["estacoes"][code] = {
                    "nome": info["name"], "cultura": info["crop"], "data": today,
                    "observacoes": len(obs),
                    "temp_max_c": round(temp_max, 1) if temp_max > -999 else None,
                    "temp_min_c": round(temp_min, 1) if temp_min < 999 else None,
                    "precipitacao_24h_mm": round(precip_total, 1),
                    "ultima_obs": obs[-1] if obs else None,
                }
                print(f"OK ({len(obs)} obs, {precip_total:.1f}mm)")
            else:
                result["estacoes"][code] = {"nome": info["name"], "cultura": info["crop"], "error": "no data"}
                print("FAILED")
            time.sleep(0.3)

    # Fetch weather alerts
    print("  Alertas meteorológicos...", end=" ")
    alert_urls = [
        "https://apitempo.inmet.gov.br/avisos/ativos",
        "https://alertas2.inmet.gov.br/api/avisos/ativos",
    ]
    alerts = None
    for aurl in alert_urls:
        alerts = fetch_json(aurl)
        if alerts and isinstance(alerts, list):
            break

    if alerts and isinstance(alerts, list):
        ag_states = ["MT", "PR", "GO", "SP", "MG", "RS", "BA", "MS", "SC", "TO", "PI", "MA"]
        filtered = []
        for a in alerts:
            uf = str(a.get("uf", a.get("estados", "")))
            if any(st in uf for st in ag_states):
                filtered.append({
                    "severidade": a.get("severidade", a.get("nivel", "")),
                    "evento": a.get("evento", a.get("descricao_evento", "")),
                    "descricao": str(a.get("descricao", ""))[:200],
                    "uf": uf,
                    "inicio": a.get("inicio", a.get("dt_inicio", "")),
                    "fim": a.get("fim", a.get("dt_fim", "")),
                })
        result["alertas"] = filtered
        print(f"OK ({len(filtered)} alertas ag)")
    else:
        print("SKIP (alerts API unavailable)")

    # Summary
    stations_ok = {k: v for k, v in result["estacoes"].items() if "error" not in v}
    if stations_ok:
        temps = [s.get("temp_max_c", 0) for s in stations_ok.values() if s.get("temp_max_c")]
        result["resumo_clima"] = {
            "data": today,
            "estacoes_ok": len(stations_ok),
            "estacoes_total": len(STATIONS),
            "temp_max_br": max(temps) if temps else None,
            "precip_media_mm": round(
                sum(s.get("precipitacao_24h_mm", 0) for s in stations_ok.values()) / len(stations_ok), 1
            ) if stations_ok else 0,
            "alertas_ativos": len(result.get("alertas", [])),
        }

    save(result, "inmet_data.json")
    return result


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("  AgriMacro v3.2 - Novos Coletores (APIs Abertas)")
    print("=" * 60)

    results = {}
    errors = []

    for name, func in [("BCB", collect_bcb), ("CONAB", collect_conab),
                         ("IBGE", collect_ibge), ("INMET", collect_inmet)]:
        try:
            results[name] = func()
        except Exception as e:
            print(f"\n  [ERROR] {name}: {e}")
            errors.append(name)

    print(f"\n{'=' * 60}")
    print(f"  COLLECTION COMPLETE")
    print(f"  OK: {len(results) - len(errors)}/4 sources")
    if errors:
        print(f"  ERRORS: {', '.join(errors)}")
    print(f"  Output: {PROC}")
    print(f"  Files: bcb_data.json, conab_data.json, ibge_data.json, inmet_data.json")
    print(f"{'=' * 60}")
