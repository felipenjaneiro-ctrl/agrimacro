#!/usr/bin/env python3
"""
collect_usda_freight.py
=======================
AgriMacro Intelligence — Coletor de fretes USDA GTR + rotas oceanicas

Outputs:
  usda_gtr.json            → gulf_basis_cents_bu, barge_freight_usd_mt, ocean_gulf_shanghai_usd_mt
  usda_brazil_transport.json → santos_shanghai_usd_mt

Fontes (em ordem de prioridade):
  1. USDA AMS GTR API (basis + barge)
  2. Baltic Dry Index via Quandl/FRED (BDI proxy para fretes oceanicos)
  3. Estimativas historicas calibradas com BDI recente

Executar do diretorio raiz do agrimacro:
  python pipeline/collect_usda_freight.py
"""

import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, date, timedelta
from pathlib import Path

SCRIPT_DIR  = Path(__file__).parent
DATA_DIR    = SCRIPT_DIR.parent / "agrimacro-dash" / "public" / "data" / "processed"
OUT_GTR     = DATA_DIR / "usda_gtr.json"
OUT_BRTRANS = DATA_DIR / "usda_brazil_transport.json"

# ── Constantes de calibragem (regressao historica 2020-2025) ─────────────────
# Barge IL→Gulf: correlacao fraca com BDI, mais influenciada por nivel Mississippi
# Ocean Gulf→Shanghai: Panamax BDI × coeficiente + constante
# Ocean Santos→Shanghai: rota mais curta (~14% menos que Gulf)
OCEAN_GULF_COEF   = 0.020   # USD/mt por ponto BDI
OCEAN_GULF_CONST  = 16.00    # USD/mt base
OCEAN_SANTOS_RATIO = 0.693  # Santos = 69.3% do custo Gulf (rota ~30% mais curta)
BARGE_DEFAULT      = 25.0   # USD/mt barge IL→Gulf (media historica)
GULF_BASIS_DEFAULT = 50.0   # c/bu (media historica out-of-harvest)

# ── Valores de fallback documentados ─────────────────────────────────────────
FALLBACK_BDI   = 1450       # pts (media 52 semanas recente)
FALLBACK_BASIS = 50.0       # c/bu Gulf CIF basis
FALLBACK_BARGE = 25.0       # USD/mt
FALLBACK_OCEAN_GULF   = 48.0
FALLBACK_OCEAN_SANTOS = 33.0


def fetch_url(url, timeout=15):
    """GET simples, retorna bytes ou None."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "AgriMacro/3.3"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except Exception as e:
        print(f"  [WARN] fetch {url[:60]}: {e}")
        return None


def get_bdi_fred():
    """Baltic Dry Index via FRED API (serie BDIYINDEX)."""
    url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BDIYINDEX"
    raw = fetch_url(url)
    if not raw:
        return None, "FRED_unavailable"
    lines = raw.decode("utf-8").strip().splitlines()
    for line in reversed(lines):
        parts = line.split(",")
        if len(parts) == 2 and parts[1].strip() != ".":
            try:
                return float(parts[1]), f"FRED_{parts[0]}"
            except:
                pass
    return None, "FRED_parse_error"


def get_bdi_from_cot_json():
    """Tenta ler BDI do grain_ratios.json (ja coletado pelo engine)."""
    try:
        path = DATA_DIR / "grain_ratios.json"
        with open(path, encoding="utf-8") as f:
            gr = json.load(f)
        bdi = gr.get("arbitrage", {}).get("spread_delivered_china", {}).get("bdi_used")
        if bdi and float(bdi) > 0:
            return float(bdi), "grain_ratios_cache"
    except:
        pass
    return None, "grain_ratios_unavailable"


def get_usda_ams_basis():
    """
    USDA AMS GTR — Gulf Basis via arquivo de texto publico.
    URL: https://www.ams.usda.gov/mnreports/sj_gr850.txt
    Retorna (basis_cents_bu, source) ou (None, reason).
    """
    url = "https://www.ams.usda.gov/mnreports/sj_gr850.txt"
    raw = fetch_url(url)
    if not raw:
        return None, "USDA_AMS_unavailable"

    text = raw.decode("latin-1", errors="replace")
    # O relatorio GTR tem linhas como:
    # "GULF CIF PREMIUM/DISCOUNT  56  55  58  52"
    # Procurar por valores de basis/CIF premium
    import re
    for line in text.splitlines():
        lu = line.upper()
        if "GULF" in lu and ("CIF" in lu or "BASIS" in lu or "PREMIUM" in lu):
            nums = re.findall(r"[-+]?\d+\.?\d*", line)
            floats = [float(n) for n in nums if -200 < float(n) < 200]
            if floats:
                basis = floats[0]
                print(f"  AMS GTR basis encontrado: {basis:.1f} c/bu")
                return basis, f"USDA_AMS_{date.today()}"

    return None, "USDA_AMS_parse_failed"


def get_barge_freight_usda():
    """
    USDA AMS — Tarifa de barcaca IL→Gulf.
    Tenta o relatorio de fretes fluviais (GR811).
    """
    url = "https://www.ams.usda.gov/mnreports/sj_gr811.txt"
    raw = fetch_url(url)
    if not raw:
        return None, "USDA_barge_unavailable"

    import re
    text = raw.decode("latin-1", errors="replace")
    for line in text.splitlines():
        lu = line.upper()
        if any(kw in lu for kw in ["BARGE", "RIVER", "MEMPHIS", "NEW ORLEANS", "CAIRO"]):
            nums = re.findall(r"\d+\.?\d*", line)
            floats = [float(n) for n in nums if 5 < float(n) < 120]
            if floats:
                barge = floats[0]
                print(f"  AMS barge freight: {barge:.1f} USD/mt")
                return barge, f"USDA_AMS_barge_{date.today()}"

    return None, "USDA_barge_parse_failed"


def calc_ocean_freight(bdi, route="gulf"):
    """Calcula frete oceanico Panamax via regressao calibrada com BDI."""
    if route == "gulf":
        return round(bdi * OCEAN_GULF_COEF + OCEAN_GULF_CONST, 1)
    elif route == "santos":
        gulf = calc_ocean_freight(bdi, "gulf")
        return round(gulf * OCEAN_SANTOS_RATIO, 1)
    return None


def main():
    print("=" * 60)
    print("  AgriMacro — Coletor USDA Freight")
    print(f"  Data: {date.today()}")
    print("=" * 60)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().isoformat()
    sources_used = []

    # 1. BDI ──────────────────────────────────────────────────────────────────
    print("\n[1/4] Baltic Dry Index...")
    bdi, bdi_src = get_bdi_fred()
    if bdi:
        print(f"  BDI (FRED): {bdi:.0f} pts [{bdi_src}]")
        sources_used.append(bdi_src)
    else:
        bdi, bdi_src = get_bdi_from_cot_json()
        if bdi:
            print(f"  BDI (cache): {bdi:.0f} pts [{bdi_src}]")
            sources_used.append(bdi_src)
        else:
            bdi = FALLBACK_BDI
            bdi_src = f"fallback_historical_avg_{FALLBACK_BDI}"
            print(f"  BDI (fallback): {bdi:.0f} pts")
            sources_used.append(bdi_src)

    # 2. Gulf Basis ───────────────────────────────────────────────────────────
    print("\n[2/4] USDA AMS Gulf Basis...")
    gulf_basis, basis_src = get_usda_ams_basis()
    if gulf_basis:
        sources_used.append(basis_src)
    else:
        gulf_basis = GULF_BASIS_DEFAULT
        basis_src = f"hardcoded_historical_avg_{GULF_BASIS_DEFAULT}"
        print(f"  Gulf basis (fallback): {gulf_basis:.1f} c/bu")
        sources_used.append(basis_src)

    # 3. Barge Freight ────────────────────────────────────────────────────────
    print("\n[3/4] USDA AMS Barge Freight...")
    barge, barge_src = get_barge_freight_usda()
    if barge:
        sources_used.append(barge_src)
    else:
        barge = BARGE_DEFAULT
        barge_src = f"hardcoded_historical_avg_{BARGE_DEFAULT}"
        print(f"  Barge freight (fallback): {barge:.1f} USD/mt")
        sources_used.append(barge_src)

    # 4. Ocean Freight ────────────────────────────────────────────────────────
    print("\n[4/4] Calculando fretes oceanicos via BDI...")
    ocean_gulf   = calc_ocean_freight(bdi, "gulf")
    ocean_santos = calc_ocean_freight(bdi, "santos")
    ocean_src    = f"BDI_regression_coef={OCEAN_GULF_COEF}"
    print(f"  Gulf→Shanghai:   ${ocean_gulf:.1f}/mt  (BDI={bdi:.0f} × {OCEAN_GULF_COEF} + {OCEAN_GULF_CONST})")
    print(f"  Santos→Shanghai: ${ocean_santos:.1f}/mt (Gulf × {OCEAN_SANTOS_RATIO:.3f})")
    sources_used.append(ocean_src)

    # ── Salvar usda_gtr.json ──────────────────────────────────────────────────
    gtr_out = {
        "generated_at":             ts,
        "date":                     date.today().isoformat(),
        "bdi_pts":                  round(bdi),
        "bdi_source":               bdi_src,
        "gulf_basis_cents_bu":      round(gulf_basis, 1),
        "gulf_basis_source":        basis_src,
        "barge_freight_usd_mt":     round(barge, 1),
        "barge_source":             barge_src,
        "ocean_gulf_shanghai_usd_mt": ocean_gulf,
        "ocean_source":             ocean_src,
        "data_quality": {
            "bdi_is_real":         "fallback" not in bdi_src,
            "basis_is_real":       "hardcoded" not in basis_src,
            "barge_is_real":       "hardcoded" not in barge_src,
            "ocean_is_estimated":  True,
            "ocean_method":        f"BDI × {OCEAN_GULF_COEF} + {OCEAN_GULF_CONST} (regressao calibrada 2020-2025)",
            "ocean_precision":     "±15% — usar apenas como referencia",
            "warning":             None if "hardcoded" not in basis_src else
                                   "gulf_basis e barge usam valores historicos — USDA AMS indisponivel",
        },
        "sources": sources_used,
    }

    with open(OUT_GTR, "w", encoding="utf-8") as f:
        json.dump(gtr_out, f, indent=2, ensure_ascii=False)
    print(f"\n  Salvo: {OUT_GTR.name}")

    # ── Salvar usda_brazil_transport.json ─────────────────────────────────────
    br_trans_out = {
        "generated_at":              ts,
        "date":                      date.today().isoformat(),
        "santos_shanghai_usd_mt":    ocean_santos,
        "source":                    ocean_src,
        "bdi_pts":                   round(bdi),
        "bdi_source":                bdi_src,
        "methodology":               f"Santos→Shanghai = Gulf→Shanghai × {OCEAN_SANTOS_RATIO} (rota ~30% mais curta)",
        "data_quality": {
            "is_estimated":  True,
            "precision":     "±15%",
            "warning":       "Frete Santos calculado via regressao BDI — sem fonte direta Baltic/Platts",
        },
    }

    with open(OUT_BRTRANS, "w", encoding="utf-8") as f:
        json.dump(br_trans_out, f, indent=2, ensure_ascii=False)
    print(f"  Salvo: {OUT_BRTRANS.name}")

    # ── Resumo ────────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  RESULTADO:")
    print(f"  BDI:             {bdi:.0f} pts [{bdi_src}]")
    print(f"  Gulf basis:      {gulf_basis:.1f} c/bu [{basis_src}]")
    print(f"  Barge IL→Gulf:   {barge:.1f} USD/mt [{barge_src}]")
    print(f"  Ocean Gulf→SHA:  {ocean_gulf:.1f} USD/mt (BDI-based)")
    print(f"  Ocean Santos→SHA:{ocean_santos:.1f} USD/mt (BDI-based)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
