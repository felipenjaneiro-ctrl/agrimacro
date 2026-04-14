#!/usr/bin/env python3
"""
intelligence_engine.py - AgriMacro Daily Intelligence Frame
============================================================
Aggregates ALL collector outputs into a single intelligence_frame.json
that tells the day's market narrative.

Reads every processed JSON, scores each commodity from -3 to +3,
generates alerts, market narrative, and a "film entry" for the day.

Output: intelligence_frame.json
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
ROOT_DIR = SCRIPT_DIR.parent

DATA_DIR_CANDIDATES = [
    ROOT_DIR / "agrimacro-dash" / "public" / "data" / "processed",
    SCRIPT_DIR / "public" / "data" / "processed",
]
DATA_DIR = next((c for c in DATA_DIR_CANDIDATES if c.exists()), SCRIPT_DIR)
OUTPUT_FILE = DATA_DIR / "intelligence_frame.json"

# The 19 commodities AgriMacro covers
ALL_SYMS = [
    "ZC", "ZS", "ZW", "KE", "ZM", "ZL",       # Grains
    "LE", "GF", "HE",                            # Livestock
    "CT", "SB", "KC", "CC",                      # Softs
    "CL", "NG",                                   # Energy
    "GC", "SI",                                    # Metals
]

NAMES = {
    "ZC": "Milho", "ZS": "Soja", "ZW": "Trigo CBOT", "KE": "Trigo KC",
    "ZM": "Farelo Soja", "ZL": "Oleo Soja", "LE": "Boi Gordo",
    "GF": "Feeder Cattle", "HE": "Suino Magro", "CT": "Algodao",
    "SB": "Acucar", "KC": "Cafe", "CC": "Cacau", "CL": "Petroleo WTI",
    "NG": "Gas Natural", "GC": "Ouro", "SI": "Prata",
}


# ---------------------------------------------------------------------------
# Load helpers
# ---------------------------------------------------------------------------
def load_json(filename):
    p = DATA_DIR / filename
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def safe_get(d, *keys, default=None):
    """Nested dict access: safe_get(d, 'a', 'b', 'c') -> d['a']['b']['c']"""
    for k in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(k, default)
        if d is None:
            return default
    return d


# ---------------------------------------------------------------------------
# Load all data sources
# ---------------------------------------------------------------------------
def load_all():
    data = {}
    data["cot"] = load_json("cot.json")
    data["psd"] = load_json("psd_ending_stocks.json")
    data["stocks"] = load_json("stocks_watch.json")
    data["drought"] = load_json("drought_monitor.json")
    data["fertilizer"] = load_json("fertilizer_prices.json")
    data["crop_progress"] = load_json("crop_progress.json")
    data["export_activity"] = load_json("export_activity.json")
    data["season"] = load_json("seasonality.json")
    data["prices"] = load_json("price_history.json")
    data["macro"] = load_json("macro_indicators.json")
    data["spreads"] = load_json("spreads.json")
    data["parities"] = load_json("parities.json")
    data["correlations"] = load_json("correlations.json")
    data["weather"] = load_json("weather_agro.json")
    data["intel_synthesis"] = load_json("intel_synthesis.json")
    data["eia"] = load_json("eia_data.json")
    data["news"] = load_json("news.json")
    data["calendar"] = load_json("calendar.json")
    data["conab"] = load_json("conab_data.json")
    data["livestock_weekly"] = load_json("livestock_weekly.json")
    data["livestock_psd"] = load_json("livestock_psd.json")
    data["physical_br"] = load_json("physical_br.json")
    data["bilateral"] = load_json("bilateral_indicators.json")
    return data


# ---------------------------------------------------------------------------
# Score a single commodity (-3 to +3)
# ---------------------------------------------------------------------------
def score_commodity(sym, data):
    score = 0
    signals = []
    bull_factors = []
    bear_factors = []
    key_number = None
    lag_factor = None

    # ── COT (weight 2) ──
    cot_comm = safe_get(data["cot"], "commodities", sym, default={})
    dis = cot_comm.get("disaggregated", {})
    # Use disaggregated cot_index_52w first, then cot_index
    ci_52w = dis.get("cot_index_52w")
    ci_156w = dis.get("cot_index") or safe_get(dis, "delta_analysis", "cot_index")

    if ci_52w is not None:
        if ci_52w >= 85:
            score -= 2
            signals.append("COT_EXTREME_BEARISH")
            bear_factors.append(f"COT 52w={ci_52w:.0f}/100 - especuladores sobreposicionados")
        elif ci_52w >= 70:
            score -= 1
            signals.append("COT_BEARISH")
            bear_factors.append(f"COT 52w={ci_52w:.0f}/100 - alto")
        elif ci_52w <= 15:
            score += 2
            signals.append("COT_EXTREME_BULLISH")
            bull_factors.append(f"COT 52w={ci_52w:.0f}/100 - extremo vendido")
        elif ci_52w <= 30:
            score += 1
            signals.append("COT_BULLISH")
            bull_factors.append(f"COT 52w={ci_52w:.0f}/100 - baixo")

    # COT reversal signal
    da = dis.get("delta_analysis", {})
    rev_score = da.get("reversal_score", 0)
    if rev_score >= 70:
        sig = da.get("signals", [{}])[0] if da.get("signals") else {}
        if sig.get("type") == "REVERSAL_BEARISH":
            signals.append("COT_REVERSAL_DOWN")
            bear_factors.append(f"Reversao COT prob {rev_score}%")
        elif sig.get("type") == "REVERSAL_BULLISH":
            signals.append("COT_REVERSAL_UP")
            bull_factors.append(f"Reversao COT prob {rev_score}%")

    # ── PSD Ending Stocks (weight 1) ──
    psd_sym = safe_get(data["psd"], "commodities", sym, default={})
    dev = psd_sym.get("deviation")
    if dev is not None:
        if dev > 30:
            score -= 1
            signals.append("STOCKS_FOLGADO")
            bear_factors.append(f"Estoques +{dev:.1f}% vs media 5A")
        elif dev > 15:
            signals.append("STOCKS_ACIMA_MEDIA")
            bear_factors.append(f"Estoques +{dev:.1f}%")
        elif dev < -20:
            score += 1
            signals.append("STOCKS_APERTADO")
            bull_factors.append(f"Estoques {dev:.1f}% vs media 5A")
        elif dev < -10:
            signals.append("STOCKS_ABAIXO_MEDIA")
            bull_factors.append(f"Estoques {dev:.1f}%")
        key_number = f"Estoques desvio {dev:+.1f}%"

    # ── Stocks Watch state ──
    sw = safe_get(data["stocks"], "commodities", sym, default={})
    state = sw.get("state", "")
    if "APERTO" in state and "STOCKS_APERTADO" not in signals:
        score += 1
        signals.append("WATCH_APERTO")
        bull_factors.append(f"Stocks watch: {state}")
    elif "EXCESSO" in state and "STOCKS_FOLGADO" not in signals:
        score -= 1
        signals.append("WATCH_EXCESSO")
        bear_factors.append(f"Stocks watch: {state}")

    # ── Drought (ZW, KE, ZC) ──
    if sym in ("ZW", "KE") and data["drought"]:
        nat = safe_get(data["drought"], "national", default={})
        d3_pct = nat.get("d3_pct", 0) + nat.get("d4_pct", 0)
        d2_pct = nat.get("d2_pct", 0) + d3_pct
        sp_reg = safe_get(data["drought"], "regions", "southern_plains", default={})
        sp_d3 = sp_reg.get("d3_plus_pct", 0)
        sp_signal = sp_reg.get("signal", "")

        if sp_d3 > 20 or sp_signal == "CRITICO":
            score += 2
            signals.append("DROUGHT_CRITICAL")
            bull_factors.append(f"Southern Plains D3+={sp_d3:.1f}% - seca critica")
            key_number = f"Southern Plains {sp_d3:.1f}% D3+"
        elif d2_pct > 25:
            score += 1
            signals.append("DROUGHT_ELEVATED")
            bull_factors.append(f"D2+ nacional {d2_pct:.1f}%")

    if sym == "ZC" and data["drought"]:
        cb_reg = safe_get(data["drought"], "regions", "corn_belt", default={})
        cb_d2 = cb_reg.get("d2_plus_pct", 0)
        cb_signal = cb_reg.get("signal", "")
        if cb_d2 > 30 or cb_signal == "CRITICO":
            score += 2
            signals.append("DROUGHT_CRITICAL_CORNBELT")
            bull_factors.append(f"Corn Belt D2+={cb_d2:.1f}% - seca critica")
        elif cb_d2 > 15 or cb_signal == "ALERTA":
            score += 1
            signals.append("DROUGHT_ALERT_CORNBELT")
            bull_factors.append(f"Corn Belt D2+={cb_d2:.1f}%")

    # ── Crop Progress (ZC, ZS mainly) ──
    if data["crop_progress"] and not safe_get(data["crop_progress"], "is_fallback"):
        crop_map = {"ZC": "CORN", "ZS": "SOYBEANS", "ZW": "WHEAT_WINTER", "CT": "COTTON"}
        crop_key = crop_map.get(sym)
        if crop_key:
            crop_nat = safe_get(data["crop_progress"], "crops", crop_key, "national", default={})
            planted = crop_nat.get("planted")
            if planted is not None:
                # Simple heuristic: <15% in April = late
                month = datetime.now().month
                if month in (4, 5) and sym in ("ZC", "ZS") and planted < 15:
                    score += 1
                    signals.append("PLANTIO_ATRASADO")
                    bull_factors.append(f"Plantio apenas {planted}% em {month}/meses")
                    key_number = f"Plantio {planted}%"

    # ── Fertilizer (lag 6-12m, no score change — informational) ──
    if data["fertilizer"] and not safe_get(data["fertilizer"], "is_fallback"):
        cost_signal = safe_get(data["fertilizer"], "cost_impact", "signal", default="")
        avg_yoy = safe_get(data["fertilizer"], "cost_impact", "avg_yoy_pct", default=0)
        if cost_signal == "PRESSAO CUSTO" and sym in ("ZC", "ZS", "ZW", "CT"):
            signals.append("FERTILIZER_LAG_6M")
            lag_factor = f"Fertilizante +{avg_yoy:.0f}% YoY -> custo plantio 2026/27 em 6-12m"

    # ── Seasonality (weight 1) ──
    if data["season"]:
        season_sym = safe_get(data["season"], sym, default={})
        monthly = season_sym.get("monthly_returns", [])
        month_idx = datetime.now().month - 1  # 0-indexed
        if monthly and len(monthly) > month_idx:
            avg_ret = monthly[month_idx]
            if isinstance(avg_ret, dict):
                avg_ret = avg_ret.get("avg", 0)
            if isinstance(avg_ret, (int, float)):
                if avg_ret > 2:
                    score += 1
                    signals.append("SEASONAL_POSITIVE")
                    bull_factors.append(f"Sazonalidade {avg_ret:+.1f}% media historica")
                elif avg_ret < -2:
                    score -= 1
                    signals.append("SEASONAL_NEGATIVE")
                    bear_factors.append(f"Sazonalidade {avg_ret:+.1f}% media historica")

    # ── Composite signals from correlations engine ──
    if data["correlations"]:
        comp_sigs = safe_get(data["correlations"], "composite_signals", default=[])
        for cs in comp_sigs:
            if cs.get("asset") == sym:
                sig = cs.get("signal", "")
                conf = cs.get("confidence", 0)
                if sig == "BULLISH" and conf >= 0.6:
                    score += 1
                    signals.append("COMPOSITE_BULLISH")
                    for f in cs.get("factors_bull", []):
                        bull_factors.append(f"Composite: {f}")
                elif sig == "BEARISH" and conf >= 0.6:
                    score -= 1
                    signals.append("COMPOSITE_BEARISH")
                    for f in cs.get("factors_bear", []):
                        bear_factors.append(f"Composite: {f}")

    # ── Spread extremes ──
    if data["spreads"]:
        spread_syms_map = {
            "ZS": ["soy_crush"],
            "ZC": ["corn_soy_ratio", "corn_meal"],
            "ZW": ["wheat_premium"],
            "ZL": ["soy_oil_petro"],
            "LE": ["cattle_corn_ratio"],
            "HE": ["hog_corn_ratio"],
        }
        for sp_key in spread_syms_map.get(sym, []):
            sp = safe_get(data["spreads"], "spreads", sp_key, default={})
            z = sp.get("zscore_1y", 0)
            if abs(z) >= 2.5:
                signals.append(f"SPREAD_EXTREME_{sp_key.upper()}")
                msg = f"Spread {sp.get('name','?')} z={z:+.2f}"
                if z > 0:
                    bull_factors.append(msg)
                else:
                    bear_factors.append(msg)

    # Clamp score to [-3, +3]
    score = max(-3, min(3, score))

    return {
        "score": score,
        "signals": signals,
        "bull_factors": bull_factors,
        "bear_factors": bear_factors,
        "key_number": key_number,
        "lag_factor": lag_factor,
    }


# ---------------------------------------------------------------------------
# Generate alerts
# ---------------------------------------------------------------------------
def generate_alerts(data, by_commodity):
    alerts = []

    # COT extremes
    if data["cot"]:
        for sym in ALL_SYMS:
            dis = safe_get(data["cot"], "commodities", sym, "disaggregated", default={})
            ci = dis.get("cot_index")
            ci_52 = dis.get("cot_index_52w")
            if ci is not None and ci >= 90:
                alerts.append({
                    "priority": "HIGH",
                    "commodity": sym,
                    "type": "COT",
                    "message": f"{NAMES.get(sym,sym)} COT {ci:.0f}/100 - extremo comprado",
                    "data_source": "cot",
                    "lag": None,
                })
            elif ci is not None and ci <= 10:
                alerts.append({
                    "priority": "HIGH",
                    "commodity": sym,
                    "type": "COT",
                    "message": f"{NAMES.get(sym,sym)} COT {ci:.0f}/100 - extremo vendido",
                    "data_source": "cot",
                    "lag": None,
                })

    # Drought
    if data["drought"] and not safe_get(data["drought"], "is_fallback"):
        for alert in safe_get(data["drought"], "crop_alerts", default=[]):
            msg = alert.get("message", "")
            if "CRITICO" in msg or "ALERTA" in msg:
                alerts.append({
                    "priority": "HIGH" if "CRITICO" in msg else "MEDIUM",
                    "commodity": "ZW" if "TRIGO" in alert.get("crop", "") else "ZC",
                    "type": "DROUGHT",
                    "message": msg,
                    "data_source": "drought_monitor",
                    "lag": None,
                })

    # Fertilizer cost pressure
    if data["fertilizer"] and not safe_get(data["fertilizer"], "is_fallback"):
        cost_sig = safe_get(data["fertilizer"], "cost_impact", "signal", default="")
        detail = safe_get(data["fertilizer"], "cost_impact", "detail", default="")
        avg_yoy = safe_get(data["fertilizer"], "cost_impact", "avg_yoy_pct", default=0)
        if cost_sig == "PRESSAO CUSTO":
            alerts.append({
                "priority": "MEDIUM",
                "commodity": "ALL_GRAINS",
                "type": "FERTILIZER",
                "message": f"Fertilizantes +{avg_yoy:.0f}% YoY ({detail})",
                "data_source": "fertilizer_prices",
                "lag": "6-12 meses",
            })

    # Crop progress delays
    if data["crop_progress"] and not safe_get(data["crop_progress"], "is_fallback"):
        for crop_key, sym in [("CORN", "ZC"), ("SOYBEANS", "ZS"), ("COTTON", "CT")]:
            planted = safe_get(data["crop_progress"], "crops", crop_key, "national", "planted")
            if planted is not None and planted < 10 and datetime.now().month in (4, 5):
                alerts.append({
                    "priority": "LOW",
                    "commodity": sym,
                    "type": "CROP_PROGRESS",
                    "message": f"{NAMES.get(sym,sym)} plantio {planted}% - inicio de temporada",
                    "data_source": "crop_progress",
                    "lag": None,
                })

    # Spread extremes
    if data["spreads"]:
        for key, sp in safe_get(data["spreads"], "spreads", default={}).items():
            z = sp.get("zscore_1y", 0)
            if abs(z) >= 2.5:
                alerts.append({
                    "priority": "MEDIUM",
                    "commodity": key,
                    "type": "SPREAD",
                    "message": f"{sp.get('name','?')}: z={z:+.2f} ({sp.get('regime','')})",
                    "data_source": "spreads",
                    "lag": None,
                })

    # PSD stocks extreme deviations
    if data["psd"]:
        for sym in ALL_SYMS:
            dev = safe_get(data["psd"], "commodities", sym, "deviation")
            if dev is not None and abs(dev) > 30:
                direction = "acima" if dev > 0 else "abaixo"
                alerts.append({
                    "priority": "MEDIUM",
                    "commodity": sym,
                    "type": "STOCKS",
                    "message": f"{NAMES.get(sym,sym)} estoques {dev:+.1f}% {direction} da media 5A",
                    "data_source": "psd_ending_stocks",
                    "lag": None,
                })

    # Composite signals from correlations
    if data["correlations"]:
        for cs in safe_get(data["correlations"], "composite_signals", default=[]):
            if cs.get("confidence", 0) >= 0.7:
                alerts.append({
                    "priority": "MEDIUM",
                    "commodity": cs.get("asset", "?"),
                    "type": "COMPOSITE",
                    "message": f"{cs['asset']}: {cs['signal']} (conf {cs['confidence']*100:.0f}%, {cs.get('sources_count',0)} fontes)",
                    "data_source": "correlations",
                    "lag": None,
                })

    # Sort: HIGH first, then MEDIUM, then LOW
    priority_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    alerts.sort(key=lambda a: priority_order.get(a["priority"], 3))

    return alerts


# ---------------------------------------------------------------------------
# Macro frame
# ---------------------------------------------------------------------------
def build_macro_frame(data):
    frame = {}

    # DXY
    if data["prices"] and "DX" in data["prices"]:
        dx = data["prices"]["DX"]
        if dx:
            last = dx[-1]["close"]
            prev = dx[-2]["close"] if len(dx) > 1 else last
            chg = (last / prev - 1) * 100 if prev else 0
            signal = "FORTE" if last > 103 else "FRACO" if last < 97 else "NEUTRO"
            frame["dxy"] = {
                "value": round(last, 2),
                "change_pct": round(chg, 2),
                "signal": signal,
                "impact": "Pressao baixista commodities" if signal == "FORTE" else "Suporte commodities" if signal == "FRACO" else "Neutro",
            }

    # Oil (CL)
    if data["prices"] and "CL" in data["prices"]:
        cl = data["prices"]["CL"]
        if cl:
            last = cl[-1]["close"]
            prev5 = cl[-6]["close"] if len(cl) > 5 else last
            chg5d = (last / prev5 - 1) * 100 if prev5 else 0
            signal = "SPIKE" if chg5d > 5 else "QUEDA" if chg5d < -5 else "NEUTRO"
            frame["oil"] = {
                "value": round(last, 2),
                "change_5d_pct": round(chg5d, 2),
                "signal": signal,
                "impact": "Fertilizantes +lag 6m" if signal == "SPIKE" else "Alivio custo" if signal == "QUEDA" else "Neutro",
            }

    # VIX
    macro = data["macro"] or {}
    vix = macro.get("vix", {})
    if vix and vix.get("value"):
        frame["vix"] = {
            "value": vix["value"],
            "level": vix.get("level", "?"),
            "impact": "Risk-off — pressao commodities" if vix["value"] > 25 else "Normal",
        }

    # Fertilizer index
    if data["fertilizer"] and not safe_get(data["fertilizer"], "is_fallback"):
        ci = safe_get(data["fertilizer"], "cost_impact", default={})
        frame["fertilizer_index"] = {
            "yoy_change": ci.get("avg_yoy_pct", 0),
            "signal": ci.get("signal", "?"),
        }

    return frame


# ---------------------------------------------------------------------------
# Market narrative
# ---------------------------------------------------------------------------
def generate_narrative(alerts, macro_frame, by_commodity):
    parts = []

    # Macro context
    oil = macro_frame.get("oil", {})
    if oil.get("signal") == "SPIKE":
        parts.append(f"Petroleo em spike (CL ${oil['value']}) pressionando custos")

    dxy = macro_frame.get("dxy", {})
    if dxy.get("signal") == "FORTE":
        parts.append(f"Dolar forte (DXY {dxy['value']}) pesando sobre commodities")

    # Top HIGH alerts
    high_alerts = [a for a in alerts if a["priority"] == "HIGH"]
    for alert in high_alerts[:3]:
        parts.append(alert["message"])

    # If no high alerts, use top scored commodities
    if not high_alerts:
        sorted_comms = sorted(by_commodity.items(), key=lambda x: abs(x[1]["score"]), reverse=True)
        for sym, info in sorted_comms[:2]:
            if abs(info["score"]) >= 2:
                direction = "altista" if info["score"] > 0 else "baixista"
                parts.append(f"{NAMES.get(sym,sym)} com vies {direction} (score {info['score']:+d})")

    if not parts:
        parts.append("Mercado sem sinais extremos hoje — monitorar posicoes")

    return ". ".join(parts) + "."


# ---------------------------------------------------------------------------
# Film entry (daily log)
# ---------------------------------------------------------------------------
def generate_film_entry(alerts, macro_frame, by_commodity, data):
    today = datetime.now().strftime("%Y-%m-%d")

    # One-liner: top 3 events
    events = []
    high_alerts = [a for a in alerts if a["priority"] == "HIGH"]
    for a in high_alerts[:3]:
        events.append(a["message"])

    # Add macro events
    oil = macro_frame.get("oil", {})
    if oil.get("signal") == "SPIKE":
        events.append(f"CL spike ${oil['value']} ({oil.get('change_5d_pct',0):+.1f}% 5d)")

    fert = macro_frame.get("fertilizer_index", {})
    if fert.get("signal") == "PRESSAO CUSTO":
        events.append(f"Fertilizantes +{fert.get('yoy_change',0):.0f}% YoY")

    # Key commodity moves from prices
    if data["prices"]:
        big_moves = []
        for sym in ["ZC", "ZS", "ZW", "CL", "LE", "GF", "KC", "CC"]:
            bars = (data["prices"] or {}).get(sym, [])
            if len(bars) >= 2:
                last = bars[-1]["close"]
                prev = bars[-2]["close"]
                chg = (last / prev - 1) * 100 if prev else 0
                if abs(chg) > 2:
                    big_moves.append((sym, chg, last))
        big_moves.sort(key=lambda x: abs(x[1]), reverse=True)
        for sym, chg, px in big_moves[:2]:
            events.append(f"{NAMES.get(sym,sym)} {chg:+.1f}% (${px:.2f})")

    # Compose one-liner
    one_liner = " + ".join(events[:3]) if events else "Dia sem eventos significativos"

    # Key events list (more detailed)
    key_events = []
    for a in alerts[:5]:
        key_events.append(f"[{a['priority']}] {a['message']}")

    # Add COT summary
    cot_extremes = []
    if data["cot"]:
        for sym in ALL_SYMS:
            ci = safe_get(data["cot"], "commodities", sym, "disaggregated", "cot_index")
            if ci is not None and (ci >= 85 or ci <= 15):
                cot_extremes.append(f"{sym} {ci:.0f}/100")
    if cot_extremes:
        key_events.append(f"COT extremos: {', '.join(cot_extremes)}")

    return {
        "date": today,
        "one_liner": one_liner,
        "key_events": key_events[:8],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("  intelligence_engine: starting...")

    data = load_all()
    loaded = sum(1 for v in data.values() if v is not None)
    print(f"    Loaded {loaded}/{len(data)} data sources")

    # Score each commodity
    by_commodity = {}
    for sym in ALL_SYMS:
        result = score_commodity(sym, data)
        by_commodity[sym] = result

    # Print scores
    scored = [(sym, info["score"]) for sym, info in by_commodity.items()]
    scored.sort(key=lambda x: x[1])
    print("    Scores:")
    for sym, sc in scored:
        sigs = by_commodity[sym]["signals"]
        print(f"      {sym:>3}: {sc:+d}  {', '.join(sigs[:4])}")

    # Generate alerts
    alerts = generate_alerts(data, by_commodity)
    high_count = sum(1 for a in alerts if a["priority"] == "HIGH")
    med_count = sum(1 for a in alerts if a["priority"] == "MEDIUM")
    print(f"    Alerts: {high_count} HIGH, {med_count} MEDIUM, {len(alerts) - high_count - med_count} LOW")

    # Macro frame
    macro_frame = build_macro_frame(data)
    print(f"    Macro frame: {list(macro_frame.keys())}")

    # Narrative
    narrative = generate_narrative(alerts, macro_frame, by_commodity)
    print(f"    Narrative: {narrative[:120]}...")

    # Film entry
    film_entry = generate_film_entry(alerts, macro_frame, by_commodity, data)
    print(f"    Film entry: {film_entry['one_liner'][:100]}...")

    # Assemble output
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "date": datetime.now().strftime("%Y-%m-%d"),
        "market_narrative": narrative,
        "alerts": alerts,
        "by_commodity": by_commodity,
        "macro_frame": macro_frame,
        "film_entry": film_entry,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"  intelligence_engine: {len(alerts)} alerts, {len(by_commodity)} commodities scored")
    print(f"  Saved to {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    main()
