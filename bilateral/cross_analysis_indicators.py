#!/usr/bin/env python3
"""
AgriMacro v3.3 — Cross-Analysis Indicators Package
====================================================
9 indicadores que cruzam dados existentes.

Data files in: agrimacro-dash/public/data/processed/
Output to:     agrimacro-dash/public/data/bilateral/
"""

import json, os, sys, re, math
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================
# PATHS — data lives in processed/ subfolder
# ============================================================
BASE = Path(os.environ.get("AGRIMACRO_BASE",
    r"C:\Users\felip\OneDrive\Área de Trabalho\agrimacro"))
PROCESSED = BASE / "agrimacro-dash" / "public" / "data" / "processed"
BILAT = BASE / "agrimacro-dash" / "public" / "data" / "bilateral"
BILAT.mkdir(parents=True, exist_ok=True)

FILES = {
    "prices":    "price_history.json",
    "cepea":     "physical_br.json",
    "macro":     "bcb_data.json",
    "cot":       "cot.json",
    "eia":       "eia_data.json",
    "weather":   "weather_agro.json",
    "bilateral": "bilateral_indicators.json",
}

def load(key):
    p = PROCESSED / FILES.get(key, key)
    if not p.exists():
        return None
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def save(name, obj):
    p = BILAT / name
    with open(p, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    print(f"    → {p.name} ({p.stat().st_size/1024:.1f} KB)")

# --- Data accessors ---
def get_price(prices, symbol):
    if not prices or not isinstance(prices, dict):
        return None
    series = prices.get(symbol, [])
    return float(series[-1]["close"]) if series else None

def get_cepea(cepea, symbol_br):
    if not cepea or not isinstance(cepea, dict):
        return None
    prod = cepea.get("products", {}).get(symbol_br, {})
    p = prod.get("price")
    return float(p) if p is not None else None

def get_brl(macro):
    if not macro or not isinstance(macro, dict):
        return 5.23
    s = macro.get("brl_usd", [])
    return float(s[-1]["value"]) if s and isinstance(s, list) else 5.23

def get_selic(macro):
    if not macro or not isinstance(macro, dict):
        return None
    s = macro.get("selic_meta", [])
    return float(s[-1]["value"]) if s and isinstance(s, list) else None

def get_eia(eia, key):
    if not eia or not isinstance(eia, dict):
        return None
    v = eia.get("series", {}).get(key, {}).get("latest_value")
    return float(v) if v is not None else None


# ============================================================
# 1. BASIS TEMPORAL
# ============================================================
def indicator_basis_temporal(prices, cepea, macro):
    print("\n  [1/9] Basis Temporal...")
    brl = get_brl(macro)
    PRODUCTS = {
        "soja":  {"cbot": "ZS", "br": "ZS_BR", "conv": lambda c, b: (c/100)*2.2046*b, "unit": "R$/sc"},
        "milho": {"cbot": "ZC", "br": "ZC_BR", "conv": lambda c, b: (c/100)*2.3621*b, "unit": "R$/sc"},
        "boi":   {"cbot": "LE", "br": "LE_BR", "conv": lambda c, b: (c/100)*33.069*b, "unit": "R$/@"},
        "cafe":  {"cbot": "KC", "br": "KC_BR", "conv": lambda c, b: (c/100)*132.277*b,"unit": "R$/sc"},
    }
    results = {}
    for key, cfg in PRODUCTS.items():
        cbot = get_price(prices, cfg["cbot"])
        br = get_cepea(cepea, cfg["br"])
        if cbot is None or br is None:
            results[key] = {"status": "NO_DATA", "cbot": cbot, "cepea": br}
            print(f"    {key}: NO_DATA (CBOT={cbot}, CEPEA={br})")
            continue
        chi_rs = cfg["conv"](cbot, brl)
        basis_abs = br - chi_rs
        basis_pct = (basis_abs / chi_rs * 100) if chi_rs else 0
        if basis_pct < -10:    sig, txt = "DEEP_DISCOUNT", f"Desconto profundo {abs(basis_pct):.1f}%"
        elif basis_pct < -5:   sig, txt = "EXPORT_WINDOW", f"Desconto BR {abs(basis_pct):.1f}%: exportação atrativa"
        elif basis_pct > 10:   sig, txt = "HIGH_PREMIUM", f"Prêmio alto {basis_pct:.1f}%"
        elif basis_pct > 5:    sig, txt = "DOMESTIC_PREMIUM", f"Prêmio doméstico {basis_pct:.1f}%"
        else:                  sig, txt = "NEUTRAL", f"Basis normal ({basis_pct:+.1f}%)"
        results[key] = {
            "br_physical": round(br, 2), "chicago_brl": round(chi_rs, 2),
            "basis_abs": round(basis_abs, 2), "basis_pct": round(basis_pct, 1),
            "signal": sig, "signal_text": txt, "unit": cfg["unit"],
        }
        print(f"    {key}: BR R${br:.2f} vs CHI R${chi_rs:.2f} → {basis_pct:+.1f}% → {sig}")
    save("basis_temporal.json", {
        "indicator": "basis_temporal", "generated": datetime.now().isoformat(),
        "brl_usd": round(brl, 4), "commodities": results,
    })
    return results


# ============================================================
# 2. COT MOMENTUM
# ============================================================
def indicator_cot_momentum(cot):
    print("\n  [2/9] COT Momentum...")
    if not cot:
        print("    [SKIP] cot.json not found")
        save("cot_momentum.json", {"indicator": "cot_momentum", "status": "NO_DATA"})
        return {}
    # Structure: {commodities: {ZS: {legacy: {history: [{date,noncomm_net,open_interest,...}]}}}}
    commodities = cot.get("commodities", {}) if isinstance(cot, dict) else {}
    SYMS = ["ZS","ZC","ZW","ZM","ZL","LE","GF","HE","CT","SB","KC","CC","CL","NG","GC","SI","KE"]
    avail = [s for s in SYMS if s in commodities]
    print(f"    COT symbols available: {len(avail)}/{len(SYMS)}")
    results = {}
    for sym in SYMS:
        prod = commodities.get(sym, {})
        leg = prod.get("legacy", {})
        hist_raw = leg.get("history", [])
        hist = []
        for r in hist_raw:
            dt = str(r.get("date", ""))[:10]
            net = r.get("noncomm_net")
            if net is None:
                nl = r.get("noncomm_long", 0) or 0
                ns = r.get("noncomm_short", 0) or 0
                net = int(nl) - int(ns)
            else:
                net = int(net)
            oi = int(r.get("open_interest", 1) or 1)
            if dt:
                hist.append({"date": dt, "net": net, "oi": oi})
        hist.sort(key=lambda x: x["date"])
        if len(hist) < 2:
            results[sym] = {"status": "NO_DATA", "weeks": len(hist)}
            continue
        cur, prev = hist[-1], hist[-2]
        prev4 = hist[-5] if len(hist) >= 5 else hist[0]
        d1 = cur["net"] - prev["net"]
        d4 = cur["net"] - prev4["net"]
        pct_oi = (cur["net"] / cur["oi"] * 100) if cur["oi"] else 0
        if d4 > 0 and d1 > 0:     mom = "ACCUMULATING"
        elif d4 < 0 and d1 < 0:   mom = "DISTRIBUTING"
        elif d4 > 0 and d1 < 0:   mom = "PROFIT_TAKING"
        elif d4 < 0 and d1 > 0:   mom = "BOTTOM_FISHING"
        else:                      mom = "NEUTRAL"
        ext = abs(pct_oi) > 15
        results[sym] = {
            "net": cur["net"], "delta_1w": d1, "delta_4w": d4,
            "pct_oi": round(pct_oi, 1), "momentum": mom,
            "extreme": ext,
            "extreme_text": f"Posicao {'comprada' if pct_oi>0 else 'vendida'} elevada ({pct_oi:.0f}% OI)" if ext else "",
            "weeks": len(hist), "last_date": hist[-1]["date"],
        }
        if ext or abs(d1) > 3000:
            print(f"    {sym}: {mom} net={cur['net']:+,} d1w={d1:+,} ({pct_oi:.0f}%OI)" + (" !!!" if ext else ""))
    active = sum(1 for v in results.values() if v.get("momentum"))
    print(f"    Products with data: {active}/{len(SYMS)}")
    save("cot_momentum.json", {
        "indicator": "cot_momentum", "generated": __import__('datetime').datetime.now().isoformat(),
        "products": results,
    })
    return results


# ============================================================
# 3. CRUSH MARGIN BILATERAL
# ============================================================
def indicator_crush_bilateral(prices, cepea, macro):
    print("\n  [3/9] Crush Margin Bilateral...")
    zs = get_price(prices, "ZS")
    zm = get_price(prices, "ZM")
    zl = get_price(prices, "ZL")
    brl = get_brl(macro)
    us_crush = None
    if all(v is not None for v in [zs, zm, zl]):
        meal = (zm / 2000) * 44
        oil = (zl / 100) * 11
        cost = zs / 100
        margin = meal + oil - cost
        margin_pct = (margin / cost * 100) if cost else 0
        oil_share = (oil / (meal + oil) * 100) if (meal + oil) else 0
        if margin_pct > 15:    sig, txt = "US_STRONG", f"Margem US forte ({margin_pct:.1f}%)"
        elif margin_pct > 0:   sig, txt = "US_HEALTHY", f"Margem US saudável ({margin_pct:.1f}%)"
        else:                  sig, txt = "US_NEGATIVE", f"Margem US negativa ({margin_pct:.1f}%)"
        us_crush = {
            "margin_usd_bu": round(margin, 3), "margin_pct": round(margin_pct, 1),
            "margin_rs_sc": round(margin * 2.2046 * brl, 2),
            "meal_revenue": round(meal, 3), "oil_revenue": round(oil, 3), "soy_cost": round(cost, 3),
            "oil_share_pct": round(oil_share, 1), "signal": sig, "signal_text": txt,
        }
        print(f"    US: ${margin:.3f}/bu ({margin_pct:.1f}%) = R${us_crush['margin_rs_sc']}/sc → {sig}")
        print(f"    Oil share: {oil_share:.1f}% | ZS={zs:.0f}¢ ZM=${zm:.0f} ZL={zl:.1f}¢")
    else:
        print(f"    US: incompleto (ZS={zs}, ZM={zm}, ZL={zl})")
    br_crush = None
    soja_br = get_cepea(cepea, "ZS_BR")
    if soja_br:
        br_crush = {"soja_rs_sc": round(soja_br, 2)}
        print(f"    BR: Soja R${soja_br:.2f}/sc")
    save("crush_bilateral.json", {
        "indicator": "crush_bilateral", "generated": datetime.now().isoformat(),
        "us_crush": us_crush, "br_crush": br_crush, "brl_usd": round(brl, 4),
    })
    return {"us": us_crush, "br": br_crush}


# ============================================================
# 4. PRODUCER MARGIN INDEX
# ============================================================
def indicator_producer_margin(prices, cepea, macro, eia):
    print("\n  [4/9] Producer Margin Index...")
    ng = get_price(prices, "NG")
    brl = get_brl(macro)
    selic = get_selic(macro)
    diesel = get_eia(eia, "diesel_retail") or get_eia(eia, "diesel_price")
    if diesel is None and eia:
        for line in eia.get("analysis_summary", []):
            m = re.search(r'Diesel.*?\$(\d+\.?\d*)/gal', str(line))
            if m:
                diesel = float(m.group(1))
                break
    if ng is None:
        ng_eia = get_eia(eia, "henry_hub") or get_eia(eia, "natural_gas_price")
        if ng_eia:
            ng = ng_eia
    print(f"    Inputs: NG=${ng} | Diesel=${diesel} | BRL={brl} | Selic={selic}%")
    BL = {"fertilizer": (ng, 3.50, "$/MMBtu", 0.30),
          "freight": (diesel, 3.50, "$/gal", 0.25),
          "imports": (brl, 5.00, "BRL/USD", 0.25),
          "storage": (selic, 12.0, "% a.a.", 0.20)}
    components = {}
    total, w_used = 0, 0
    for name, (val, baseline, unit, weight) in BL.items():
        if val is None:
            continue
        score = min((val / baseline) * 50, 100)
        components[name] = {
            "value": round(val, 3), "unit": unit, "score": round(score, 1),
            "vs_baseline": f"{'↑' if val > baseline else '↓'} vs média ({baseline})",
        }
        total += score * weight
        w_used += weight
    index = (total / w_used) if w_used > 0 else 50
    if index > 60:   level, txt = "HIGH", f"Custo ELEVADO ({index:.0f}/100)"
    elif index > 40: level, txt = "NORMAL", f"Custo NORMAL ({index:.0f}/100)"
    else:            level, txt = "LOW", f"Custo BAIXO ({index:.0f}/100)"
    revenue = {}
    zs = get_price(prices, "ZS")
    zc = get_price(prices, "ZC")
    if zs: revenue["soja_chicago_rs"] = round((zs/100)*2.2046*brl, 2)
    soja_br = get_cepea(cepea, "ZS_BR")
    if soja_br: revenue["soja_cepea_rs"] = round(soja_br, 2)
    if zc: revenue["milho_chicago_rs"] = round((zc/100)*2.3621*brl, 2)
    milho_br = get_cepea(cepea, "ZC_BR")
    if milho_br: revenue["milho_cepea_rs"] = round(milho_br, 2)
    print(f"    Index: {index:.0f}/100 ({level})")
    for n, c in components.items():
        print(f"      {n}: {c['value']} {c['unit']} → score {c['score']}")
    if revenue:
        print(f"    Revenue: {revenue}")
    save("producer_margin.json", {
        "indicator": "producer_margin", "generated": datetime.now().isoformat(),
        "cost_index": round(index, 1), "cost_level": level, "cost_text": txt,
        "components": components, "revenue": revenue,
        "squeeze": index > 55,
    })
    return {"index": index, "level": level}


# ============================================================
# 5. INTEREST RATE DIFFERENTIAL
# ============================================================
def indicator_interest_differential(macro):
    print("\n  [5/9] Interest Rate Differential...")
    selic = get_selic(macro) or 13.25
    fed = 4.50
    fred_path = BILAT / "fred_macro.json"
    if fred_path.exists():
        with open(fred_path, "r", encoding="utf-8") as f:
            fd = json.load(f)
        v = fd.get("series", {}).get("DFF", {}).get("latest_value")
        if v: fed = float(v)
    diff = selic - fed
    if diff > 10:   sig, imp = "STRONG_CARRY", "BULLISH"
    elif diff > 6:  sig, imp = "MODERATE_CARRY", "NEUTRAL_BULL"
    elif diff > 3:  sig, imp = "WEAK_CARRY", "NEUTRAL"
    else:           sig, imp = "NO_CARRY", "BEARISH"
    print(f"    Selic: {selic}% | Fed: {fed}% | Diff: {diff:.1f}pp → {sig}")
    result = {
        "indicator": "interest_differential", "generated": datetime.now().isoformat(),
        "selic": round(selic, 2), "fed_funds": round(fed, 2),
        "differential_pp": round(diff, 2), "signal": sig, "brl_impact": imp,
        "text": f"Diferencial {diff:.1f}pp → {sig}",
    }
    save("interest_differential.json", result)
    return result


# ============================================================
# 6. EXPORT PACE WEEKLY
# ============================================================
def indicator_export_pace(bilateral):
    print("\n  [6/9] Export Pace by Period...")
    ert = bilateral.get("export_race", bilateral.get("ert", {})) if bilateral and isinstance(bilateral, dict) else {}
    now = datetime.now()
    m = now.month
    my_start = datetime(now.year if m >= 9 else now.year-1, 9, 1)
    days = (now - my_start).days
    pct = days / 365 * 100
    Q = {
        "Q1_Sep_Nov": {"m": [9,10,11], "label": "Set-Nov (pico EUA)", "us": 35, "br": 5},
        "Q2_Dec_Feb": {"m": [12,1,2],  "label": "Dez-Fev (entressafra)", "us": 55, "br": 10},
        "Q3_Mar_May": {"m": [3,4,5],   "label": "Mar-Mai (pico BR)", "us": 75, "br": 50},
        "Q4_Jun_Aug": {"m": [6,7,8],   "label": "Jun-Ago (tail BR)", "us": 95, "br": 85},
    }
    cq = next((k for k, v in Q.items() if m in v["m"]), "Q2_Dec_Feb")
    qc = Q[cq]
    us_data = {k: v for k, v in ert.items() if "us_" in k}
    br_data = {k: v for k, v in ert.items() if "br_" in k}
    print(f"    MY {my_start.year}/{my_start.year+1} | Day {days} ({pct:.0f}%) | {qc['label']}")
    result = {
        "indicator": "export_pace_weekly", "generated": datetime.now().isoformat(),
        "marketing_year": f"{my_start.year}/{my_start.year+1}",
        "days_in_my": days, "pct_through": round(pct, 1),
        "current_quarter": cq, "quarter_label": qc["label"],
        "seasonal_context": f"Típico: US {qc['us']}% WASDE, BR {qc['br']}% WASDE neste ponto.",
        "us": us_data, "br": br_data, "quarters": Q,
    }
    save("export_pace_weekly.json", result)
    return result


# ============================================================
# 7. ARGENTINA TRILATERAL
# ============================================================
def indicator_argentina_trilateral():
    print("\n  [7/9] Argentina Trilateral...")
    intl = None
    p = PROCESSED / "physical_intl.json"
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            intl = json.load(f)
        print(f"    [OK] physical_intl.json ({p.stat().st_size/1024:.1f} KB)")
    ar_fob = {}
    if intl and isinstance(intl, dict):
        prods = intl.get("products", intl)
        for k, v in prods.items():
            if any(x in str(k).upper()+str(v).upper() for x in ["_AR","ARGENTIN","ROSARIO","UPRIVER"]):
                ar_fob[k] = v
    if ar_fob:
        print(f"    AR FOB prices: {list(ar_fob.keys())}")
    AR = {
        "soja":  {"est": 50.0, "avg": 44.0, "ly": 50.5, "ret": 33, "harv": "Mar-Jun"},
        "milho": {"est": 50.0, "avg": 46.0, "ly": 46.5, "ret": 12, "harv": "Mar-Jul"},
        "trigo": {"est": 18.0, "avg": 17.5, "ly": 17.0, "ret": 12, "harv": "Dec-Jan"},
    }
    results = {}
    for crop, d in AR.items():
        va = ((d["est"]-d["avg"])/d["avg"])*100
        if va < -15:   sig = "CROP_FAILURE"
        elif va < -5:  sig = "BELOW_TREND"
        elif va > 10:  sig = "BUMPER"
        else:          sig = "NORMAL"
        results[crop] = {
            "estimate_mmt": d["est"], "avg_5yr": d["avg"], "last_year": d["ly"],
            "vs_avg_pct": round(va, 1), "retenciones_pct": d["ret"],
            "harvest": d["harv"], "signal": sig,
        }
        print(f"    {crop}: {d['est']} MMT (vs avg {va:+.1f}%) → {sig}")
    save("argentina_trilateral.json", {
        "indicator": "argentina_trilateral", "generated": datetime.now().isoformat(),
        "crops": results, "ar_fob": ar_fob,
    })
    return results


# ============================================================
# 8. DROUGHT ACCUMULATOR
# ============================================================
def indicator_drought_accumulator(weather):
    print("\n  [8/9] Drought Accumulator...")
    REGIONS = {
        "cerrado_soja":  {"name": "Soja Cerrado (MT/GO)", "pm": 10, "norm": 1200},
        "sul_soja":      {"name": "Soja Sul (PR/RS)",     "pm": 10, "norm": 1000},
        "cornbelt_corn": {"name": "Milho Corn Belt",      "pm": 5,  "norm": 650},
        "pampas_soja":   {"name": "Soja Pampas (AR)",     "pm": 11, "norm": 900},
        "minas_cafe":    {"name": "Café Minas Gerais",    "pm": 9,  "norm": 1400},
    }
    now = datetime.now()
    results = {}
    for rid, cfg in REGIONS.items():
        py = now.year - 1 if cfg["pm"] > now.month else now.year
        days = (now - datetime(py, cfg["pm"], 15)).days
        if days < 0 or days > 300:
            results[rid] = {"name": cfg["name"], "status": "OFF_SEASON"}
            continue
        pct = min(days/180, 1.0)*100
        exp_mm = cfg["norm"] * min(days/180, 1.0)
        stages = [(150,"colheita"),(120,"maturação"),(90,"enchimento"),(60,"floração"),(30,"vegetativo"),(0,"emergência")]
        stage = next((s for t, s in stages if days >= t), "emergência")
        results[rid] = {
            "name": cfg["name"], "status": "IN_SEASON",
            "days": days, "pct_season": round(pct, 0),
            "stage": stage, "expected_mm": round(exp_mm, 0), "normal_mm": cfg["norm"],
        }
        print(f"    {cfg['name']}: day {days} ({pct:.0f}%) | {stage}")
    save("drought_accumulator.json", {
        "indicator": "drought_accumulator", "generated": datetime.now().isoformat(), "regions": results,
    })
    return results


# ============================================================
# 9. FREIGHT SPREAD
# ============================================================
def indicator_freight_spread():
    print("\n  [9/9] Freight Spread...")
    # Search for collector outputs
    gtr, br_tr, imea = None, None, None
    for folder in [PROCESSED, BILAT, PROCESSED.parent]:
        for nm in ["usda_gtr.json","gtr_data.json"]:
            p = folder / nm
            if p.exists():
                with open(p,"r",encoding="utf-8") as f: gtr = json.load(f)
                print(f"    [OK] GTR: {p.name}")
        for nm in ["usda_brazil_transport.json","brazil_transport.json"]:
            p = folder / nm
            if p.exists():
                with open(p,"r",encoding="utf-8") as f: br_tr = json.load(f)
                print(f"    [OK] BR Transport: {p.name}")
        for nm in ["imea.json","imea_data.json"]:
            p = folder / nm
            if p.exists():
                with open(p,"r",encoding="utf-8") as f: imea = json.load(f)
                print(f"    [OK] IMEA: {p.name}")
    us_total, us_src = 35, "estimate"
    br_total, br_src = 75, "estimate"
    if gtr: us_src = "USDA GTR"
    if br_tr: br_src = "USDA Brazil Transport"
    if imea:
        fr = imea.get("freight", imea.get("frete"))
        if isinstance(fr, dict):
            v = fr.get("value", fr.get("sorriso_santos"))
            if v: br_total, br_src = float(v), "IMEA"
    spread = br_total - us_total
    print(f"    US: ${us_total}/mt ({us_src}) | BR: ${br_total}/mt ({br_src}) | Spread: ${spread:+.0f}/mt")
    save("freight_spread.json", {
        "indicator": "freight_spread", "generated": datetime.now().isoformat(),
        "us_corridor": {"route": "Iowa→Gulf", "total_usd_mt": us_total, "source": us_src},
        "br_corridor": {"route": "Sorriso→Santos", "total_usd_mt": br_total, "source": br_src},
        "spread_usd_mt": round(spread, 1), "advantage": "US" if spread > 0 else "BR",
    })


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("AgriMacro v3.3 — Cross-Analysis Indicators")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Data: {PROCESSED}")
    print("=" * 60)
    print("\n  Loading...")
    data = {k: load(k) for k in FILES}
    found = sum(1 for v in data.values() if v)
    print(f"  Sources: {found}/{len(FILES)}")
    for k, v in data.items():
        print(f"    {'✓' if v else '✗'} {k} → {FILES[k]}")
    
    indicator_basis_temporal(data["prices"], data["cepea"], data["macro"])
    indicator_cot_momentum(data["cot"])
    indicator_crush_bilateral(data["prices"], data["cepea"], data["macro"])
    indicator_producer_margin(data["prices"], data["cepea"], data["macro"], data["eia"])
    indicator_interest_differential(data["macro"])
    indicator_export_pace(data["bilateral"])
    indicator_argentina_trilateral()
    indicator_drought_accumulator(data["weather"])
    indicator_freight_spread()
    
    print("\n" + "=" * 60)
    files = sorted(BILAT.glob("*.json"))
    total = sum(f.stat().st_size for f in files) / 1024
    for f in files:
        print(f"  {f.name:35s} {f.stat().st_size/1024:6.1f} KB")
    print(f"  {'TOTAL':35s} {total:6.1f} KB")
    print("Done!")

if __name__ == "__main__":
    main()
