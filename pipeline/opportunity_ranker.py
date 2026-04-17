"""
opportunity_ranker.py — Ranking multi-fator de oportunidades AgriMacro

Cruza 6 fontes de dados reais e gera um score composto por commodity:
  1. COT Positioning (cot.json)
  2. IV / Volatilidade (options_chain.json)
  3. Estoques vs média (stocks_watch.json)
  4. Sazonalidade (seasonality.json)
  5. Clima / Weather (weather_agro.json)
  6. DNA Composite (commodity_dna.json)

Output: pipeline/opportunity_ranking.json
"""

import json
import os
from datetime import datetime

BASE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(BASE, "..", "agrimacro-dash", "public", "data", "processed")
OUT  = os.path.join(BASE, "opportunity_ranking.json")
OUT_PUBLIC = os.path.join(DATA, "opportunity_ranking.json")

ALL_SYMS = ["ZC","ZS","ZW","KE","ZM","ZL","LE","GF","HE","KC","CC","SB","CT","CL","NG","GC","SI"]

# Commodity → weather regions mapping
WEATHER_MAP = {
    "ZC": ["corn_belt","cerrado_mt"],
    "ZS": ["corn_belt","cerrado_mt","sul_pr_rs","pampas_arg"],
    "ZW": ["corn_belt","pampas_arg"],
    "KE": ["corn_belt"],
    "ZM": ["corn_belt","cerrado_mt","pampas_arg"],
    "ZL": ["corn_belt","cerrado_mt","pampas_arg"],
    "CT": ["delta_ms","cerrado_mt"],
    "KC": ["minas_cafe"],
    "SB": ["cerrado_mt","sul_pr_rs"],
    "CC": [],
    "LE": [], "GF": [], "HE": [],
    "CL": [], "NG": [],
    "GC": [], "SI": [],
}

DROUGHT_MM_7D = 5  # <5mm em 7 dias = seca ativa


def load(filename):
    path = os.path.join(DATA, filename)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return None


def score_cot(cot_data, sym):
    """COT extremo = oportunidade. Index >85 ou <15 = +30pts, 70-85/15-30 = +15pts."""
    if not cot_data:
        return 0, None
    comm = cot_data.get("commodities", {}).get(sym, {})
    dis = comm.get("disaggregated", {})
    idx = dis.get("cot_index")
    if idx is None:
        return 0, None
    detail = f"COT={idx:.0f}"
    if idx >= 85 or idx <= 15:
        return 30, f"{detail} EXTREMO"
    if idx >= 70 or idx <= 30:
        return 15, f"{detail} elevado"
    return 5, f"{detail} neutro"


def score_iv(options_data, sym):
    """IV alta = vender premium favorável. IV>40% = +25pts, 25-40% = +15pts, <20% = 0."""
    if not options_data:
        return 0, None
    und = options_data.get("underlyings", {}).get(sym, {})
    ivr = und.get("iv_rank", {})
    iv = ivr.get("current_iv")
    if iv is None:
        return 0, None
    iv_pct = iv * 100
    rank = ivr.get("rank_52w")
    rank_str = f" Rank={rank:.0f}%" if rank is not None else ""
    detail = f"IV={iv_pct:.0f}%{rank_str}"
    if iv_pct >= 40:
        return 25, f"{detail} ALTA"
    if iv_pct >= 25:
        return 15, f"{detail} moderada"
    if iv_pct >= 20:
        return 8, f"{detail} OK"
    return 0, f"{detail} baixa"


def score_stocks(stocks_data, sym):
    """Estoques abaixo da média = bullish (oportunidade). Desvio significativo = +20pts."""
    if not stocks_data:
        return 0, None
    comm = stocks_data.get("commodities", {}).get(sym, {})
    current = comm.get("stock_current")
    avg = comm.get("stock_avg")
    if current is None or avg is None or avg == 0:
        return 0, None
    dev_pct = ((current - avg) / avg) * 100
    detail = f"Estoques {dev_pct:+.0f}% vs média"
    if abs(dev_pct) >= 20:
        return 20, detail
    if abs(dev_pct) >= 10:
        return 12, detail
    return 4, detail


def score_seasonality(season_data, sym):
    """Sazonalidade forte no mês = +15pts. Desvio >10% = significativo."""
    if not season_data:
        return 0, None
    sym_data = season_data.get(sym, {})
    monthly = sym_data.get("monthly_returns")
    if not monthly:
        return 0, None
    month_idx = datetime.now().month - 1
    val = monthly[month_idx] if month_idx < len(monthly) else None
    if val is None:
        return 0, None
    avg = val if isinstance(val, (int, float)) else val.get("avg") if isinstance(val, dict) else None
    if avg is None:
        return 0, None
    detail = f"Saz {avg:+.1f}%"
    if abs(avg) >= 3:
        return 15, detail
    if abs(avg) >= 1.5:
        return 8, detail
    return 3, detail


def score_weather(weather_data, sym):
    """Seca ativa em região produtora = +20pts. Alerta ativo = +10pts."""
    if not weather_data:
        return 0, None
    regions = weather_data.get("regions", {})
    relevant = WEATHER_MAP.get(sym, [])
    if not relevant:
        return 0, None
    total_score = 0
    details = []
    for rk in relevant:
        reg = regions.get(rk, {})
        alerts = reg.get("alerts", [])
        forecast = reg.get("forecast_15d", [])
        precip_7d = sum(f.get("precip_mm", 0) for f in forecast[:7])
        if precip_7d < DROUGHT_MM_7D:
            total_score += 20
            details.append(f"Seca ativa {reg.get('label', rk)} ({precip_7d:.0f}mm/7d)")
        elif alerts:
            total_score += 10
            details.append(f"Alerta {reg.get('label', rk)}")
    return min(total_score, 25), "; ".join(details) if details else None


def score_dna(dna_data, sym):
    """DNA composite signal: BULLISH/BEARISH forte = oportunidade direcional clara = +15pts."""
    if not dna_data:
        return 0, None
    comm = dna_data.get("commodities", {}).get(sym, {})
    signal = comm.get("composite_signal")
    if not signal:
        return 0, None
    drivers = comm.get("drivers_ranked", [])
    top = drivers[0] if drivers else {}
    top_str = f" (#1: {top.get('driver', '?')})" if top else ""
    detail = f"DNA={signal}{top_str}"
    if signal in ("BULLISH", "BEARISH"):
        return 15, detail
    if signal == "MIXED":
        return 5, detail
    return 0, detail


def detect_confluences(factors):
    """Detecta confluências triplas — 3+ fatores com score alto apontando mesma direção."""
    high_count = sum(1 for s, _ in factors if s >= 15)
    return high_count >= 3


def main():
    cot = load("cot.json")
    options = load("options_chain.json")
    stocks = load("stocks_watch.json")
    season = load("seasonality.json")
    weather = load("weather_agro.json")
    dna = load("commodity_dna.json")

    rankings = []

    for sym in ALL_SYMS:
        factors = []
        factor_details = {}

        s_cot, d_cot = score_cot(cot, sym)
        factors.append((s_cot, "COT"))
        if d_cot:
            factor_details["cot"] = d_cot

        s_iv, d_iv = score_iv(options, sym)
        factors.append((s_iv, "IV"))
        if d_iv:
            factor_details["iv"] = d_iv

        s_stk, d_stk = score_stocks(stocks, sym)
        factors.append((s_stk, "Estoques"))
        if d_stk:
            factor_details["stocks"] = d_stk

        s_saz, d_saz = score_seasonality(season, sym)
        factors.append((s_saz, "Sazonalidade"))
        if d_saz:
            factor_details["seasonality"] = d_saz

        s_wx, d_wx = score_weather(weather, sym)
        factors.append((s_wx, "Weather"))
        if d_wx:
            factor_details["weather"] = d_wx

        s_dna, d_dna = score_dna(dna, sym)
        factors.append((s_dna, "DNA"))
        if d_dna:
            factor_details["dna"] = d_dna

        total = sum(s for s, _ in factors)
        max_possible = 130  # 30+25+20+15+25+15
        pct = (total / max_possible) * 100

        # Confluence detection
        confluence = detect_confluences(factors)
        confluence_drivers = [label for s, label in factors if s >= 15]

        entry = {
            "sym": sym,
            "score": total,
            "max": max_possible,
            "pct": round(pct, 1),
            "factors": factor_details,
            "confluence": confluence,
            "confluence_drivers": confluence_drivers if confluence else [],
        }

        if confluence:
            entry["alert"] = f"Triple confluence ({', '.join(confluence_drivers)}) \u2014 alta convicção"

        rankings.append(entry)

    # Sort by score descending
    rankings.sort(key=lambda x: x["score"], reverse=True)

    # Grade assignment
    for r in rankings:
        pct = r["pct"]
        if pct >= 60:
            r["grade"] = "A"
        elif pct >= 45:
            r["grade"] = "B"
        elif pct >= 30:
            r["grade"] = "C"
        elif pct >= 15:
            r["grade"] = "D"
        else:
            r["grade"] = "F"

    top3 = [r["sym"] for r in rankings[:3]]
    avoid = [r["sym"] for r in rankings if r["pct"] < 15]

    output = {
        "generated_at": datetime.now().isoformat(),
        "total_ranked": len(rankings),
        "max_score": 130,
        "top3": top3,
        "avoid": avoid,
        "rankings": rankings,
    }

    for path in (OUT, OUT_PUBLIC):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

    # Console output
    print(f"\n{'='*70}")
    print(f"  AGRIMACRO — OPPORTUNITY RANKER")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*70}\n")

    print(f"  {'#':<4} {'SYM':<5} {'SCORE':>5} {'PCT':>6} {'GRD':>4}  {'FATORES'}")
    print(f"  {'-'*65}")

    for i, r in enumerate(rankings):
        marker = " ***" if r.get("confluence") else ""
        factors_str = " | ".join(f"{v}" for v in r["factors"].values())
        print(f"  {i+1:<4} {r['sym']:<5} {r['score']:>5} {r['pct']:>5.1f}% {r['grade']:>4}  {factors_str}{marker}")
        if r.get("alert"):
            print(f"       \u26a0  {r['alert']}")

    print(f"\n  TOP 3: {', '.join(top3)}")
    if avoid:
        print(f"  EVITAR: {', '.join(avoid)}")
    print(f"\n  Output: {OUT}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
