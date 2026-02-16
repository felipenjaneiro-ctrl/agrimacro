#!/usr/bin/env python3
"""
AgriMacro Intelligence — Bilateral Indicators Pipeline
Runs all 3 proprietary indicators and outputs combined JSON.

Output: bilateral_indicators.json → consumed by dashboard, PDF report, and video generator.

Usage:
  python generate_bilateral.py                    # uses default data dir
  python generate_bilateral.py --data-dir ./data  # custom data dir
"""

import json
import os
import sys
from datetime import datetime, date
from pathlib import Path

# Add bilateral indicators to path
SCRIPT_DIR = Path(__file__).parent
BILATERAL_DIR = SCRIPT_DIR.parent / "bilateral" / "indicators"
sys.path.insert(0, str(BILATERAL_DIR))

# Import indicators
try:
    from landed_cost_shanghai import calculate_landed_cost_spread, to_dashboard_card as lcs_card, to_report_text as lcs_report
    from export_race_tracker import calculate_export_race, to_dashboard_card as ert_card, to_report_text as ert_report
    from brazil_competitiveness_index import calculate_bci, to_dashboard_card as bci_card, to_report_text as bci_report
    print("[BILATERAL] All 3 indicator modules imported OK")
except ImportError as e:
    print(f"[BILATERAL] Import error: {e}")
    print(f"[BILATERAL] Looking in: {BILATERAL_DIR}")
    sys.exit(1)

# ============================================================
# DATA LOADING HELPERS
# ============================================================

def load_json(filepath):
    """Safely load JSON file, return empty dict on failure."""
    try:
        with open(filepath, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"  [WARN] {filepath}: {e}")
        return {}


def get_data_dir():
    """Find the processed data directory."""
    # Check command line arg
    if "--data-dir" in sys.argv:
        idx = sys.argv.index("--data-dir")
        if idx + 1 < len(sys.argv):
            return Path(sys.argv[idx + 1])
    
    # Default: agrimacro-dash/public/data/processed/
    candidates = [
        SCRIPT_DIR.parent / "agrimacro-dash" / "public" / "data" / "processed",
        SCRIPT_DIR / ".." / "agrimacro-dash" / "public" / "data" / "processed",
        Path("agrimacro-dash/public/data/processed"),
    ]
    for c in candidates:
        if c.exists():
            return c.resolve()
    
    print("[BILATERAL] No data directory found, using current dir")
    return Path(".")


# ============================================================
# INDICATOR 1: LANDED COST SPREAD SHANGHAI
# ============================================================

def run_lcs(data_dir: Path) -> dict:
    """Calculate Landed Cost Spread Shanghai from available data."""
    print("\n[1/3] LANDED COST SPREAD SHANGHAI")
    
    # Load data sources
    bcb = load_json(data_dir / "bcb_data.json")
    physical_intl = load_json(data_dir / "physical_intl.json")
    futures = load_json(data_dir / "futures_contracts.json")
    
    # Extract PTAX
    ptax = 5.50  # default
    if bcb:
        brl_data = bcb.get("brl_usd", {})
        if isinstance(brl_data, dict) and "data" in brl_data:
            series = brl_data["data"]
            if series:
                last = series[-1] if isinstance(series, list) else series
                ptax = float(last.get("valor", last.get("value", 5.50)))
        elif isinstance(brl_data, list) and brl_data:
            ptax = float(brl_data[-1].get("valor", brl_data[-1].get("value", 5.50)))
    print(f"  PTAX: R$ {ptax:.2f}")
    
    # Extract CBOT soybeans
    cbot = 1000.0  # default ¢/bu
    if futures:
        contracts = futures if isinstance(futures, list) else futures.get("contracts", [])
        for c in contracts:
            sym = c.get("symbol", c.get("ticker", ""))
            if sym.startswith("ZS") and c.get("settlement"):
                cbot = float(c["settlement"]) * 100  # $/bu → ¢/bu
                break
    print(f"  CBOT: {cbot:.1f}¢/bu")
    
    # Extract CEPEA soy price
    cepea_soja = 0.0
    if physical_intl:
        br_data = physical_intl.get("brazil", physical_intl.get("br", {}))
        if isinstance(br_data, dict):
            for key in ["soja_paranagua", "soja", "soybean"]:
                if key in br_data:
                    val = br_data[key]
                    if isinstance(val, dict):
                        cepea_soja = float(val.get("price", val.get("value", 0)))
                    else:
                        cepea_soja = float(val)
                    break
    print(f"  CEPEA Soja: R$ {cepea_soja:.2f}/sc")
    
    # Load IMEA data if available
    imea = load_json(data_dir / "imea_soja.json")
    imea_price = 0.0
    imea_frete = 0.0
    imea_premio = 0.0
    imea_comercializacao = 0.0
    if imea:
        imea_price = float(imea.get("preco_mt_rs_sc", 0))
        imea_frete = float(imea.get("frete_sorriso_miritituba_rs_t", 0))
        imea_premio = float(imea.get("premio_santos_cents_bu", 0))
        imea_comercializacao = float(imea.get("comercializacao_safra_atual_pct", 0))
    
    # Default freight estimates (will be replaced by GTR/Brazil Transport when available)
    gulf_basis = 50.0       # ¢/bu
    barge_freight = 25.0    # US$/mt
    ocean_gulf = 48.0       # US$/mt
    ocean_santos = 33.0     # US$/mt
    
    # Load USDA GTR if available
    gtr = load_json(data_dir / "usda_gtr.json")
    if gtr:
        gulf_basis = float(gtr.get("gulf_basis_cents_bu", gulf_basis))
        barge_freight = float(gtr.get("barge_freight_usd_mt", barge_freight))
        ocean_gulf = float(gtr.get("ocean_gulf_shanghai_usd_mt", ocean_gulf))
    
    # Load Brazil transport if available
    br_transport = load_json(data_dir / "usda_brazil_transport.json")
    if br_transport:
        ocean_santos = float(br_transport.get("santos_shanghai_usd_mt", ocean_santos))
    
    try:
        lcs = calculate_landed_cost_spread(
            cbot_cents_bu=cbot,
            gulf_basis_cents_bu=gulf_basis,
            barge_freight_usd_mt=barge_freight,
            ocean_gulf_shanghai=ocean_gulf,
            ptax=ptax,
            ocean_santos_shanghai=ocean_santos,
            cepea_paranagua_rs_sc=cepea_soja if cepea_soja > 0 else 0,
            imea_mt_rs_sc=imea_price if cepea_soja == 0 else 0,
            frete_interior_rs_mt=imea_frete * 1.3 if cepea_soja == 0 else 0,
            premio_santos_cents_bu=imea_premio if cepea_soja == 0 else 0,
            imea_comercializacao_pct=imea_comercializacao,
        )
        
        card = lcs_card(lcs)
        report = lcs_report(lcs)
        
        print(f"  ✓ LCS: ${lcs.spread:+.2f}/mt → {lcs.competitive_origin} competitive")
        
        return {
            "status": "OK",
            "spread_usd_mt": round(lcs.spread, 2),
            "spread_pct": round(lcs.spread_pct, 2),
            "competitive_origin": lcs.competitive_origin,
            "us_landed": lcs.us_landed_shanghai,
            "br_landed": lcs.br_landed_shanghai,
            "us_fob": lcs.us_fob,
            "br_fob": lcs.br_fob,
            "us_ocean": lcs.us_ocean,
            "br_ocean": lcs.br_ocean,
            "fob_spread": round(lcs.fob_spread, 2),
            "ocean_advantage": round(lcs.ocean_advantage, 2),
            "ptax": ptax,
            "cbot_cents_bu": cbot,
            "dashboard_card": card,
            "report_text": report,
        }
    except Exception as e:
        print(f"  ✗ LCS Error: {e}")
        return {"status": "ERROR", "error": str(e)}


# ============================================================
# INDICATOR 2: EXPORT RACE TRACKER
# ============================================================

def run_ert(data_dir: Path) -> dict:
    """Calculate Export Race Tracker from available data."""
    print("\n[2/3] EXPORT RACE TRACKER")
    
    # Load Comex Stat data
    comex = load_json(data_dir / "comexstat_exports.json")
    
    # Load USDA export data
    usda_fas = load_json(data_dir / "usda_fas.json")
    
    # If neither source available, use representative estimates
    # based on WASDE pace and seasonal patterns
    today = date.today()
    
    # Determine marketing year
    if today.month >= 9:
        my = f"{today.year}/{str(today.year + 1)[2:]}"
    else:
        my = f"{today.year - 1}/{str(today.year)[2:]}"
    
    # Try to build monthly data from Comex Stat
    br_monthly = []
    if comex and "data" in comex:
        ncm_data = comex["data"].get("1201", {})  # soybeans
        for m in ncm_data.get("monthly", []):
            br_monthly.append({
                "year": m.get("year", today.year),
                "month": m.get("month", 1),
                "volume_mt": m.get("volume_kg", 0) / 1000,
                "value_usd": m.get("value_usd", 0),
                "china_volume_mt": 0,  # would need destination breakout
            })
    
    # Try to build US data from USDA FAS
    us_monthly = []
    if usda_fas:
        soy_data = usda_fas.get("soybeans", usda_fas.get("soybean", {}))
        for m in soy_data.get("monthly_inspections", soy_data.get("monthly", [])):
            us_monthly.append({
                "year": m.get("year", today.year),
                "month": m.get("month", 1),
                "volume_mt": m.get("volume_mt", m.get("volume", 0)),
                "value_usd": 0,
                "china_volume_mt": m.get("china_mt", 0),
            })
    
    # If no real data, use seasonal estimates based on WASDE
    if not br_monthly:
        print("  [WARN] No Comex Stat data — using WASDE-based seasonal estimates")
        # Build seasonal proxy from WASDE target
        wasde_br = 105.5  # MMT
        seasonal_pct = {2: 3, 3: 12, 4: 16, 5: 15, 6: 12, 7: 10, 8: 8, 9: 6, 10: 5, 11: 5, 12: 4, 1: 4}
        
        m = 2  # BR MY starts Feb
        yr = today.year - 1 if today.month < 2 else today.year
        while True:
            if m > today.month and yr >= today.year:
                break
            vol = wasde_br * seasonal_pct.get(m, 0) / 100 * 1_000_000
            br_monthly.append({
                "year": yr, "month": m,
                "volume_mt": vol, "value_usd": 0,
                "china_volume_mt": vol * 0.72,  # ~72% to China
            })
            m += 1
            if m > 12:
                m = 1
                yr += 1
    
    if not us_monthly:
        print("  [WARN] No USDA FAS data — using WASDE-based seasonal estimates")
        wasde_us = 49.67
        seasonal_pct = {9: 8, 10: 15, 11: 14, 12: 10, 1: 8, 2: 6, 3: 5, 4: 5, 5: 6, 6: 8, 7: 8, 8: 7}
        
        m = 9
        yr = today.year - 1
        while True:
            if m > today.month and yr >= today.year:
                break
            vol = wasde_us * seasonal_pct.get(m, 0) / 100 * 1_000_000
            us_monthly.append({
                "year": yr, "month": m,
                "volume_mt": vol, "value_usd": 0,
                "china_volume_mt": vol * 0.60,
            })
            m += 1
            if m > 12:
                m = 1
                yr += 1
    
    try:
        race = calculate_export_race(
            commodity="soybeans",
            marketing_year=my,
            us_monthly=us_monthly,
            br_monthly=br_monthly,
            current_month=today.month,
        )
        
        card = ert_card(race)
        report = ert_report(race)
        
        print(f"  ✓ ERT: Leader={race.leader} | BR {race.br_market_share_pct:.1f}% vs US {race.us_market_share_pct:.1f}%")
        
        return {
            "status": "OK",
            "commodity": "soybeans",
            "marketing_year": my,
            "leader": race.leader,
            "lead_pace_pct": race.lead_pace_pct,
            "us_ytd_mmt": race.us.ytd_volume_mmt,
            "br_ytd_mmt": race.br.ytd_volume_mmt,
            "us_pace_pct": race.us.pace_pct,
            "br_pace_pct": race.br.pace_pct,
            "br_market_share_pct": race.br_market_share_pct,
            "us_market_share_pct": race.us_market_share_pct,
            "share_shift_pp": race.share_shift_pp,
            "us_pace_signal": race.pace_signal_us,
            "br_pace_signal": race.pace_signal_br,
            "china_br_share_pct": race.br_china_share_pct,
            "china_us_share_pct": race.us_china_share_pct,
            "data_source": "real" if comex else "wasde_seasonal_estimate",
            "dashboard_card": card,
            "report_text": report,
        }
    except Exception as e:
        print(f"  ✗ ERT Error: {e}")
        return {"status": "ERROR", "error": str(e)}


# ============================================================
# INDICATOR 3: BRAZIL COMPETITIVENESS INDEX
# ============================================================

def run_bci(data_dir: Path, lcs_result: dict = None) -> dict:
    """Calculate Brazil Competitiveness Index from available data."""
    print("\n[3/3] BRAZIL COMPETITIVENESS INDEX")
    
    # Load data
    bcb = load_json(data_dir / "bcb_data.json")
    physical_intl = load_json(data_dir / "physical_intl.json")
    futures = load_json(data_dir / "futures_contracts.json")
    imea = load_json(data_dir / "imea_soja.json")
    
    # PTAX
    ptax = 5.50
    if bcb:
        brl_data = bcb.get("brl_usd", {})
        if isinstance(brl_data, dict) and "data" in brl_data:
            series = brl_data["data"]
            if series and isinstance(series, list):
                ptax = float(series[-1].get("valor", series[-1].get("value", 5.50)))
        elif isinstance(brl_data, list) and brl_data:
            ptax = float(brl_data[-1].get("valor", brl_data[-1].get("value", 5.50)))
    
    # CBOT
    cbot = 1000.0
    if futures:
        contracts = futures if isinstance(futures, list) else futures.get("contracts", [])
        for c in contracts:
            sym = c.get("symbol", c.get("ticker", ""))
            if sym.startswith("ZS") and c.get("settlement"):
                cbot = float(c["settlement"]) * 100
                break
    
    # Reuse LCS results if available
    santos_premium = -40.0
    gulf_basis = 50.0
    br_freight = 65.0
    us_freight = 73.0
    santos_fob = 400.0
    gulf_fob = 420.0
    
    if lcs_result and lcs_result.get("status") == "OK":
        santos_fob = lcs_result.get("br_fob", santos_fob)
        gulf_fob = lcs_result.get("us_fob", gulf_fob)
        br_freight = lcs_result.get("br_ocean", 33) + 32  # ocean + interior estimate
        us_freight = lcs_result.get("us_ocean", 48) + 25   # ocean + barge estimate
    
    # IMEA
    imea_comercializacao = 0
    if imea:
        imea_comercializacao = float(imea.get("comercializacao_safra_atual_pct", 0))
        santos_premium = float(imea.get("premio_santos_cents_bu", santos_premium))
    
    # Seasonal average selling pace (approximate by month)
    month_avg_pace = {
        1: 5, 2: 15, 3: 30, 4: 45, 5: 55, 6: 65,
        7: 72, 8: 78, 9: 82, 10: 86, 11: 90, 12: 93,
    }
    seasonal_avg = month_avg_pace.get(date.today().month, 50)
    
    # Load previous BCI for trend
    prev_bilateral = load_json(data_dir / "bilateral_indicators.json")
    prev_bci = 0
    if prev_bilateral:
        prev_bci = prev_bilateral.get("bci", {}).get("bci_score", 0)
    
    try:
        bci = calculate_bci(
            ptax=ptax,
            santos_premium_cents_bu=santos_premium,
            gulf_basis_cents_bu=gulf_basis,
            br_total_freight_usd_mt=br_freight,
            us_total_freight_usd_mt=us_freight,
            imea_comercializacao_pct=imea_comercializacao,
            seasonal_avg_comercializacao=seasonal_avg,
            santos_fob_usd_mt=santos_fob,
            gulf_fob_usd_mt=gulf_fob,
            br_crush_margin_usd_mt=45,  # default until crush data available
            us_crush_margin_usd_mt=40,
            cbot_cents_bu=cbot,
            previous_bci=prev_bci,
        )
        
        card = bci_card(bci)
        report = bci_report(bci)
        
        print(f"  ✓ BCI: {bci.bci_score:.1f}/100 [{bci.bci_signal}]")
        print(f"    Strongest: {bci.strongest_component}")
        print(f"    Weakest:   {bci.weakest_component}")
        
        return {
            "status": "OK",
            "bci_score": bci.bci_score,
            "bci_signal": bci.bci_signal,
            "bci_trend": bci.bci_trend,
            "strongest": bci.strongest_component,
            "weakest": bci.weakest_component,
            "components": [
                {
                    "name": c.name,
                    "score": c.score,
                    "weight_pct": int(c.weight * 100),
                    "signal": c.signal,
                    "raw_value": round(c.raw_value, 2),
                }
                for c in bci.components
            ],
            "ptax": ptax,
            "cbot_cents_bu": cbot,
            "dashboard_card": card,
            "report_text": report,
        }
    except Exception as e:
        print(f"  ✗ BCI Error: {e}")
        return {"status": "ERROR", "error": str(e)}


# ============================================================
# VIDEO NARRATION HELPER
# ============================================================

def generate_video_narration(lcs: dict, ert: dict, bci: dict, lang: str = "en") -> dict:
    """Generate video narration scenes for bilateral indicators."""
    
    if lang == "pt":
        return _narration_pt(lcs, ert, bci)
    return _narration_en(lcs, ert, bci)


def _narration_en(lcs: dict, ert: dict, bci: dict) -> dict:
    """English narration for US audience."""
    scenes = []
    
    # LCS Scene
    if lcs.get("status") == "OK":
        origin = lcs["competitive_origin"]
        spread = abs(lcs["spread_usd_mt"])
        if origin == "BR":
            hook = f"Brazil is currently {spread:.0f} dollars per ton cheaper to deliver soybeans to Shanghai."
            detail = (f"US Gulf landed cost is {lcs['us_landed']:.0f} dollars per ton, "
                     f"while Brazil Santos comes in at {lcs['br_landed']:.0f}. "
                     f"The ocean freight advantage alone saves Brazil {lcs['ocean_advantage']:.0f} dollars per ton "
                     f"on the shorter Santos to Shanghai route.")
        else:
            hook = f"The US is currently {spread:.0f} dollars per ton cheaper to deliver soybeans to Shanghai."
            detail = (f"Gulf landed cost is {lcs['us_landed']:.0f} versus "
                     f"{lcs['br_landed']:.0f} from Santos. "
                     f"Despite Brazil's ocean freight advantage, US FOB prices are winning right now.")
        
        scenes.append({
            "scene_type": "bilateral_lcs",
            "title": "Landed Cost Spread Shanghai",
            "narration": f"{hook} {detail}",
            "data_points": {
                "us_landed": lcs["us_landed"],
                "br_landed": lcs["br_landed"],
                "spread": lcs["spread_usd_mt"],
                "winner": origin,
            }
        })
    
    # ERT Scene
    if ert.get("status") == "OK":
        leader = ert["leader"]
        br_share = ert["br_market_share_pct"]
        shift = ert["share_shift_pp"]
        
        scenes.append({
            "scene_type": "bilateral_ert",
            "title": "Export Race Tracker",
            "narration": (
                f"In the soybean export race, Brazil holds {br_share:.0f} percent "
                f"of combined US-Brazil shipments this marketing year. "
                f"That's {abs(shift):.1f} percentage points "
                f"{'above' if shift > 0 else 'below'} the five-year average. "
                f"Brazil has shipped {ert['br_ytd_mmt']:.1f} million tons versus "
                f"{ert['us_ytd_mmt']:.1f} from the US. "
                f"{'Brazil' if leader == 'BR' else 'The US'} is leading the pace race "
                f"by {ert['lead_pace_pct']:.1f} percentage points versus WASDE targets."
            ),
            "data_points": {
                "br_ytd": ert["br_ytd_mmt"],
                "us_ytd": ert["us_ytd_mmt"],
                "br_share": br_share,
                "leader": leader,
            }
        })
    
    # BCI Scene
    if bci.get("status") == "OK":
        score = bci["bci_score"]
        signal = bci["bci_signal"]
        strongest = bci["strongest"]
        weakest = bci["weakest"]
        
        signal_text = {
            "STRONG": "highly competitive",
            "MODERATE": "competitive",
            "NEUTRAL": "in neutral territory",
            "WEAK": "under pressure",
            "VERY_WEAK": "significantly uncompetitive",
        }
        
        scenes.append({
            "scene_type": "bilateral_bci",
            "title": "Brazil Competitiveness Index",
            "narration": (
                f"Our proprietary Brazil Competitiveness Index stands at {score:.0f} out of 100, "
                f"putting Brazil {signal_text.get(signal, 'in mixed territory')} for soybean exports. "
                f"The strongest driver right now is {strongest}, "
                f"while {weakest} is the weakest component. "
                f"{'The real exchange rate is the dominant factor driving Brazilian competitiveness.' if 'FX' in strongest else ''}"
                f"{'Farmers in Mato Grosso are holding back sales, tightening available supply.' if 'Selling' in weakest else ''}"
            ),
            "data_points": {
                "score": score,
                "signal": signal,
                "components": bci.get("components", []),
            }
        })
    
    return {"lang": "en", "scenes": scenes}


def _narration_pt(lcs: dict, ert: dict, bci: dict) -> dict:
    """Portuguese narration for BR audience."""
    scenes = []
    
    if lcs.get("status") == "OK":
        origin = lcs["competitive_origin"]
        spread = abs(lcs["spread_usd_mt"])
        if origin == "BR":
            text = (f"O custo de entrega de soja brasileira em Shanghai está {spread:.0f} dólares "
                   f"por tonelada mais barato que a rota americana pelo Golfo do México. "
                   f"O custo desembarcado americano é {lcs['us_landed']:.0f} dólares, "
                   f"contra {lcs['br_landed']:.0f} pelo Santos. "
                   f"A vantagem do frete marítimo pela rota mais curta de Santos economiza "
                   f"{lcs['ocean_advantage']:.0f} dólares por tonelada.")
        else:
            text = (f"Atenção: os Estados Unidos estão {spread:.0f} dólares por tonelada "
                   f"mais baratos que o Brasil para entregar soja em Shanghai. "
                   f"O custo pelo Golfo é {lcs['us_landed']:.0f} contra {lcs['br_landed']:.0f} pelo Santos.")
        
        scenes.append({
            "scene_type": "bilateral_lcs",
            "title": "Custo Desembarcado Shanghai",
            "narration": text,
            "data_points": {
                "us_landed": lcs["us_landed"],
                "br_landed": lcs["br_landed"],
                "spread": lcs["spread_usd_mt"],
            }
        })
    
    if ert.get("status") == "OK":
        scenes.append({
            "scene_type": "bilateral_ert",
            "title": "Corrida de Exportação",
            "narration": (
                f"Na corrida de exportação de soja, o Brasil detém {ert['br_market_share_pct']:.0f} "
                f"por cento do total exportado entre Brasil e Estados Unidos nesta safra. "
                f"São {ert['br_ytd_mmt']:.1f} milhões de toneladas brasileiras "
                f"contra {ert['us_ytd_mmt']:.1f} dos americanos. "
                f"O market share brasileiro está {abs(ert['share_shift_pp']):.1f} pontos "
                f"{'acima' if ert['share_shift_pp'] > 0 else 'abaixo'} da média de cinco anos."
            ),
            "data_points": {
                "br_ytd": ert["br_ytd_mmt"],
                "us_ytd": ert["us_ytd_mmt"],
                "br_share": ert["br_market_share_pct"],
            }
        })
    
    if bci.get("status") == "OK":
        signal_pt = {
            "STRONG": "altamente competitivo",
            "MODERATE": "competitivo",
            "NEUTRAL": "neutro",
            "WEAK": "perdendo competitividade",
            "VERY_WEAK": "não competitivo",
        }
        scenes.append({
            "scene_type": "bilateral_bci",
            "title": "Índice de Competitividade Brasil",
            "narration": (
                f"Nosso índice proprietário de competitividade brasileira marca "
                f"{bci['bci_score']:.0f} de 100, nível {signal_pt.get(bci['bci_signal'], 'misto')}. "
                f"O fator mais forte é {bci['strongest']}, "
                f"e o mais fraco é {bci['weakest']}."
            ),
            "data_points": {
                "score": bci["bci_score"],
                "signal": bci["bci_signal"],
            }
        })
    
    return {"lang": "pt", "scenes": scenes}


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 60)
    print("  AgriMacro — Bilateral Indicators Pipeline")
    print("=" * 60)
    
    data_dir = get_data_dir()
    print(f"  Data dir: {data_dir}")
    print(f"  Date: {date.today().isoformat()}")
    
    # Run indicators
    lcs = run_lcs(data_dir)
    ert = run_ert(data_dir)
    bci = run_bci(data_dir, lcs_result=lcs)
    
    # Generate narrations
    narration_en = generate_video_narration(lcs, ert, bci, lang="en")
    narration_pt = generate_video_narration(lcs, ert, bci, lang="pt")
    
    # Combine output
    output = {
        "generated_at": datetime.now().isoformat(),
        "date": date.today().isoformat(),
        "version": "1.0",
        
        # Indicators
        "lcs": lcs,
        "ert": ert,
        "bci": bci,
        
        # Summary for quick dashboard access
        "summary": {
            "lcs_spread": lcs.get("spread_usd_mt", 0) if lcs.get("status") == "OK" else None,
            "lcs_origin": lcs.get("competitive_origin") if lcs.get("status") == "OK" else None,
            "ert_leader": ert.get("leader") if ert.get("status") == "OK" else None,
            "ert_br_share": ert.get("br_market_share_pct") if ert.get("status") == "OK" else None,
            "bci_score": bci.get("bci_score") if bci.get("status") == "OK" else None,
            "bci_signal": bci.get("bci_signal") if bci.get("status") == "OK" else None,
        },
        
        # Video narrations
        "video": {
            "en": narration_en,
            "pt": narration_pt,
        },
        
        # Report text blocks
        "report_blocks": {
            "lcs": lcs.get("report_text", ""),
            "ert": ert.get("report_text", ""),
            "bci": bci.get("report_text", ""),
        },
    }
    
    # Save
    output_path = data_dir / "bilateral_indicators.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    size_kb = output_path.stat().st_size / 1024
    
    # Summary
    ok = sum(1 for x in [lcs, ert, bci] if x.get("status") == "OK")
    
    print(f"\n{'=' * 60}")
    print(f"  BILATERAL PIPELINE COMPLETE: {ok}/3 indicators")
    print(f"  Output: {output_path} ({size_kb:.1f} KB)")
    print(f"{'=' * 60}")
    
    if lcs.get("status") == "OK":
        print(f"  LCS: ${lcs['spread_usd_mt']:+.2f}/mt → {lcs['competitive_origin']}")
    if ert.get("status") == "OK":
        print(f"  ERT: {ert['leader']} leads | BR {ert['br_market_share_pct']:.1f}% share")
    if bci.get("status") == "OK":
        print(f"  BCI: {bci['bci_score']:.1f}/100 [{bci['bci_signal']}]")
    
    print(f"\n  Video: {len(narration_en['scenes'])} EN scenes + {len(narration_pt['scenes'])} PT scenes")
    print(f"  Report: 3 text blocks ready")
    
    return 0 if ok > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
