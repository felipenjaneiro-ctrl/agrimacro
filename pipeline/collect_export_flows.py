#!/usr/bin/env python3
"""
collect_export_flows.py
=======================
AgriMacro Intelligence — Coletor de fluxos de exportacao

Outputs:
  comexstat_exports.json  → exportacoes brasileiras mensais (MDIC/Comex Stat API)
  usda_fas.json           → exportacoes americanas semanais (USDA Export Inspections)

Fontes:
  BR: https://api.comexstat.mdic.gov.br/general (API publica, sem autenticacao)
  US: https://apps.fas.usda.gov/psdonline/app/index.html#/app/reportModule (USDA FAS PSD)
      https://www.ams.usda.gov/mnreports/ (Export Inspections semanais)

Executar do diretorio raiz do agrimacro:
  python pipeline/collect_export_flows.py
"""

import json
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, date
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DATA_DIR   = SCRIPT_DIR.parent / "agrimacro-dash" / "public" / "data" / "processed"
OUT_COMEX  = DATA_DIR / "comexstat_exports.json"
OUT_FAS    = DATA_DIR / "usda_fas.json"

# NCM codes para soja e milho
NCM_SOJA = "12010090"   # soja em grao, mesmo triturada
NCM_MILHO = "10059010"  # milho em grao

# USDA FAS comodities
FAS_SOY_CODE  = "2222000"   # Soybeans
FAS_CORN_CODE = "2631000"   # Corn

BU_PER_MT_SOY  = 36.7437
BU_PER_MT_CORN = 39.368


def fetch_json(url, data=None, headers=None, timeout=20):
    """GET ou POST JSON, retorna dict/list ou None."""
    try:
        h = {"User-Agent": "AgriMacro/3.3", "Accept": "application/json"}
        if headers:
            h.update(headers)
        if data:
            payload = json.dumps(data).encode("utf-8")
            req = urllib.request.Request(url, data=payload, headers=h, method="POST")
            req.add_header("Content-Type", "application/json")
        else:
            req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  [WARN] {url[:70]}: {e}")
        return None


def fetch_text(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "AgriMacro/3.3"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  [WARN] {url[:70]}: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# PARTE 1 — COMEX STAT (Brasil)
# ─────────────────────────────────────────────────────────────

def fetch_comexstat_monthly(ncm, year_start, year_end):
    """
    API Comex Stat MDIC — exportacoes mensais por NCM.
    Endpoint: POST https://api.comexstat.mdic.gov.br/general
    """
    url = "https://api.comexstat.mdic.gov.br/general"
    payload = {
        "flow": "export",
        "monthDetail": True,
        "period": {"from": f"{year_start}01", "to": f"{year_end}12"},
        "filters": [{"filter": "ncm", "values": [ncm]}],
        "details": ["ncm", "country"],
        "metrics": ["metricFOB", "metricKG"],
    }

    result = fetch_json(url, data=payload)
    if not result:
        return None, "comexstat_unavailable"

    rows = result.get("data", {}).get("list", result.get("list", []))
    if not rows:
        # Tentar estrutura alternativa
        rows = result if isinstance(result, list) else []

    if not rows:
        print(f"  [WARN] Comex Stat: resposta sem dados para NCM {ncm}")
        return None, "comexstat_empty"

    return rows, "MDIC_ComexStat_API"


def process_comexstat(rows_soy, rows_corn):
    """Agrega por ano/mes e extrai volume China."""
    monthly = {}  # key = "YYYY-MM"

    for ncm_label, rows in [("soy", rows_soy), ("corn", rows_corn)]:
        if not rows:
            continue
        for row in rows:
            try:
                yr   = str(row.get("year", row.get("co_ano", "")))
                mo   = str(row.get("month", row.get("co_mes", ""))).zfill(2)
                if not yr or not mo:
                    continue
                key  = f"{yr}-{mo}"
                kg   = float(row.get("metricKG",  row.get("kg_liquido", 0)) or 0)
                fob  = float(row.get("metricFOB", row.get("vl_fob", 0)) or 0)
                country = str(row.get("country", row.get("co_pais_nome", ""))).upper()
                is_china = any(c in country for c in ["CHINA", "CHN", "156"])

                if key not in monthly:
                    monthly[key] = {}
                if ncm_label not in monthly[key]:
                    monthly[key][ncm_label] = {"volume_kg": 0, "value_usd": 0, "china_kg": 0}

                monthly[key][ncm_label]["volume_kg"] += kg
                monthly[key][ncm_label]["value_usd"] += fob
                if is_china:
                    monthly[key][ncm_label]["china_kg"] += kg

            except Exception as e:
                continue

    # Converter kg → MT e formatar
    out = {}
    for ym, commodities in monthly.items():
        yr, mo = ym.split("-")
        out[ym] = {"year": int(yr), "month": int(mo)}
        for comm in ["soy", "corn"]:
            if comm in commodities:
                d = commodities[comm]
                out[ym][comm] = {
                    "volume_mt":       round(d["volume_kg"] / 1000, 1),
                    "value_usd":       round(d["value_usd"], 0),
                    "china_volume_mt": round(d["china_kg"] / 1000, 1),
                }

    return dict(sorted(out.items()))


# ─────────────────────────────────────────────────────────────
# PARTE 2 — USDA FAS / Export Inspections (EUA)
# ─────────────────────────────────────────────────────────────

def fetch_usda_fas_psd(commodity_code):
    """
    USDA FAS PSD Online API — dados anuais de projecao e historico.
    https://apps.fas.usda.gov/psdonline/app/index.html#/app/compositeViz
    """
    url = (
        "https://apps.fas.usda.gov/psdonline/api/v1/data"
        f"?commodityCode={commodity_code}&countryCode=US&marketYear=0&reporting=2"
    )
    result = fetch_json(url, timeout=25)
    if not result:
        return None, "USDA_FAS_PSD_unavailable"
    return result, "USDA_FAS_PSD_API"


def fetch_usda_export_inspections_report():
    """
    USDA AMS Weekly Export Inspections — relatorio texto publico.
    Retorna texto bruto para parsing.
    """
    url = "https://www.ams.usda.gov/mnreports/sj_gr720.txt"
    text = fetch_text(url)
    if text:
        return text, "USDA_AMS_gr720"

    # Tentar relatorio alternativo
    url2 = "https://www.ams.usda.gov/mnreports/sj_gr710.txt"
    text2 = fetch_text(url2)
    if text2:
        return text2, "USDA_AMS_gr710"

    return None, "USDA_inspections_unavailable"


def parse_inspections_text(text):
    """
    Extrai volumes de soja e milho do relatorio de Export Inspections USDA AMS.
    Formato tipico:
    SOYBEANS     WEEK ENDING  02/13/2026     22,345,678 METRIC TONS...
    """
    import re
    results = {}

    if not text:
        return results

    text_upper = text.upper()
    lines = text.splitlines()

    for i, line in enumerate(lines):
        lu = line.upper()

        # Soja
        if "SOYBEAN" in lu and "METRIC" in lu:
            nums = re.findall(r"[\d,]+", line)
            for n in nums:
                val = int(n.replace(",", "")) if "," in n else int(n) if n else 0
                if 10_000 < val < 50_000_000:
                    results.setdefault("soy", {})["weekly_mt"] = val
                    break

        # Milho
        if "CORN" in lu and "METRIC" in lu:
            nums = re.findall(r"[\d,]+", line)
            for n in nums:
                val = int(n.replace(",", "")) if "," in n else int(n) if n else 0
                if 10_000 < val < 30_000_000:
                    results.setdefault("corn", {})["weekly_mt"] = val
                    break

    return results


def fetch_usda_gats_monthly():
    """
    USDA GATS (Global Agricultural Trade System) — exportacoes mensais US.
    https://apps.fas.usda.gov/gats/default.aspx
    API endpoint alternativo (dados publicos).
    """
    # Endpoint de exportacoes de soja US mensal (HS 1201)
    current_year = date.today().year
    url = (
        f"https://apps.fas.usda.gov/gats/ExpressQuery1.aspx"
        f"?_ct=0001&_sc=1201&_yr={current_year}&_tp=1&_fm=HS"
    )
    # GATS nao tem API JSON publica limpa — fallback para FAS PSD
    return None, "USDA_GATS_no_API"


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  AgriMacro — Coletor Export Flows")
    print(f"  Data: {date.today()}")
    print("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().isoformat()

    current_year = date.today().year
    year_start   = current_year - 2

    # ── PARTE 1: Comex Stat ───────────────────────────────────
    print(f"\n[1/2] COMEX STAT — Exportacoes brasileiras ({year_start}-{current_year})...")

    print("  Buscando soja (NCM 12010090)...")
    rows_soy,  src_soy  = fetch_comexstat_monthly(NCM_SOJA,  year_start, current_year)
    print("  Buscando milho (NCM 10059010)...")
    rows_corn, src_corn = fetch_comexstat_monthly(NCM_MILHO, year_start, current_year)

    comex_ok = bool(rows_soy or rows_corn)
    monthly_br = {}

    if comex_ok:
        monthly_br = process_comexstat(rows_soy, rows_corn)
        n_months = len(monthly_br)
        print(f"  OK: {n_months} meses de dados agregados")
    else:
        print("  AVISO: Comex Stat indisponivel — saindo sem dados BR")

    comex_out = {
        "generated_at":  ts,
        "date":          date.today().isoformat(),
        "source":        "MDIC_ComexStat_API",
        "years_covered": f"{year_start}-{current_year}",
        "data_quality": {
            "is_real":   comex_ok,
            "ncm_soy":   NCM_SOJA,
            "ncm_corn":  NCM_MILHO,
            "warning":   None if comex_ok else "Comex Stat API indisponivel — dados ausentes",
        },
        "monthly": monthly_br,
    }

    with open(OUT_COMEX, "w", encoding="utf-8") as f:
        json.dump(comex_out, f, indent=2, ensure_ascii=False)
    status_comex = "OK" if comex_ok else "EMPTY"
    print(f"  Salvo: {OUT_COMEX.name} [{status_comex}]")

    # ── PARTE 2: USDA FAS ─────────────────────────────────────
    print(f"\n[2/2] USDA FAS — Exportacoes americanas...")

    # Tentar Export Inspections (mais atual, semanal)
    insp_text, insp_src = fetch_usda_export_inspections_report()
    weekly_data = {}
    if insp_text:
        weekly_data = parse_inspections_text(insp_text)
        print(f"  Inspections [{insp_src}]: soy={weekly_data.get('soy',{}).get('weekly_mt','N/A')} MT/semana")

    # Tentar FAS PSD (projecoes anuais)
    print("  Buscando FAS PSD soja...")
    psd_soy,  psd_soy_src  = fetch_usda_fas_psd(FAS_SOY_CODE)
    print("  Buscando FAS PSD milho...")
    psd_corn, psd_corn_src = fetch_usda_fas_psd(FAS_CORN_CODE)

    fas_ok = bool(insp_text or psd_soy or psd_corn)

    # Extrair projecao WASDE atual (ultimo ano disponivel do PSD)
    wasde_soy_mmt  = None
    wasde_corn_mmt = None
    if psd_soy and isinstance(psd_soy, list):
        recent = sorted(psd_soy, key=lambda x: x.get("marketYear", 0), reverse=True)
        for r in recent:
            exp = r.get("exports") or r.get("Exports")
            if exp and float(exp) > 0:
                wasde_soy_mmt = round(float(exp) / 1000, 2)  # 1000 MT → MMT
                break

    fas_out = {
        "generated_at":   ts,
        "date":           date.today().isoformat(),
        "data_quality": {
            "is_real":            fas_ok,
            "inspections_source": insp_src if insp_text else "unavailable",
            "psd_source":         psd_soy_src if psd_soy else "unavailable",
            "warning":            None if fas_ok else "USDA FAS/Inspections indisponivel",
        },
        "weekly_inspections":   weekly_data,
        "wasde_projections": {
            "soybeans_mmt": wasde_soy_mmt or 49.67,
            "soybeans_source": psd_soy_src if wasde_soy_mmt else "default_feb2026_wasde",
            "corn_mmt": wasde_corn_mmt or 62.23,
            "corn_source": psd_corn_src if wasde_corn_mmt else "default_feb2026_wasde",
        },
        "monthly": {},  # USDA FAS nao tem serie mensal publica via API simples
        "note": (
            "USDA publica Export Inspections semanais (volume fisico embarcado) "
            "e Export Sales semanais (compromissos). Serie mensal consolidada requer "
            "download manual de https://apps.fas.usda.gov/gats/ExpressQuery1.aspx"
        ),
    }

    with open(OUT_FAS, "w", encoding="utf-8") as f:
        json.dump(fas_out, f, indent=2, ensure_ascii=False)
    status_fas = "OK" if fas_ok else "EMPTY"
    print(f"  Salvo: {OUT_FAS.name} [{status_fas}]")

    print(f"\n{'=' * 60}")
    print(f"  Comex Stat: {'OK' if comex_ok else 'FALHOU'} ({len(monthly_br)} meses)")
    print(f"  USDA FAS:   {'OK' if fas_ok else 'FALHOU'}")
    print(f"{'=' * 60}")
    print("\n  PROXIMO PASSO:")
    print("  python pipeline\\generate_bilateral.py")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
