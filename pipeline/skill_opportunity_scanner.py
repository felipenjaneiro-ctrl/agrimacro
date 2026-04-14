#!/usr/bin/env python3
"""
AgriMacro — Opportunity Scanner (Daily Consolidated)

Combines all skill engines into one ranked list:
  - Entry timing score (8 factors)
  - Pre-trade checklist (10 filters GO/NO-GO)
  - Fundamentals bonus/penalty (PSD stocks, spreads z-score)
  - Position sizing feasibility
  - Capital regime (VEGA awareness)

Produces final ranked list: "What should I trade today?"

Run:
  python pipeline/skill_opportunity_scanner.py
"""

import json
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent.parent
PROC = BASE / "agrimacro-dash" / "public" / "data" / "processed"
OUT = BASE / "pipeline" / "opportunity_scan.json"

SYMS = ["ZC", "ZS", "ZW", "KE", "ZM", "ZL",
        "LE", "GF", "HE", "SB", "KC", "CT", "CC",
        "CL", "NG", "GC", "SI"]

NAMES = {
    "ZC": "Milho", "ZS": "Soja", "ZW": "Trigo", "KE": "Trigo KC",
    "ZM": "Farelo", "ZL": "Oleo Soja", "LE": "Boi Gordo", "GF": "Feeder",
    "HE": "Suino", "SB": "Acucar", "KC": "Cafe", "CT": "Algodao",
    "CC": "Cacau", "CL": "Petroleo", "NG": "Gas Nat", "GC": "Ouro",
    "SI": "Prata",
}

SECTORS = {
    "ZC": "Graos", "ZS": "Graos", "ZW": "Graos", "KE": "Graos",
    "ZM": "Graos", "ZL": "Graos",
    "LE": "Pecuaria", "GF": "Pecuaria", "HE": "Pecuaria",
    "SB": "Softs", "KC": "Softs", "CT": "Softs", "CC": "Softs",
    "CL": "Energia", "NG": "Energia",
    "GC": "Metais", "SI": "Metais",
}

GRAINS = {"ZC", "ZS", "ZW", "KE", "ZM", "ZL"}

TIER_1 = {"SI", "KE"}
TIER_2 = {"HE", "ZW", "ZC", "SB", "GC"}

MARGIN_PER = {
    "CL": 3500, "SI": 8000, "GF": 2500, "ZL": 1500, "CC": 3000,
    "ZC": 1200, "ZS": 2500, "ZW": 1800, "ZM": 1500, "SB": 1800,
    "KC": 4000, "LE": 2000, "HE": 1500, "NG": 5000, "GC": 5000,
    "KE": 1500, "CT": 1200,
}


def jload(path):
    for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            with open(path, encoding=enc) as f:
                return json.load(f)
        except Exception:
            continue
    return {}


def get_forward_shape(sym, contract_hist):
    contracts = contract_hist.get("contracts", {})
    items = [(n, c["bars"][-1]["close"]) for n, c in contracts.items()
             if c.get("commodity") == sym and c.get("bars") and c["bars"][-1].get("close", 0) > 0]
    if len(items) < 2:
        return "UNKNOWN", 0
    items.sort()
    diff = ((items[-1][1] - items[0][1]) / items[0][1]) * 100
    if diff < -3:
        return "STRONG_BACK", round(diff, 1)
    elif diff < -1:
        return "MILD_BACK", round(diff, 1)
    elif diff > 3:
        return "CONTANGO", round(diff, 1)
    return "FLAT", round(diff, 1)


def scan_opportunity(sym, direction, data):
    """Score one underlying+direction. Returns score dict."""
    opts = data["options"]
    cot = data["cot"]
    seas = data["seas"]
    ch = data["contract_hist"]
    cross = data["cross"]
    skill = data["skill"]
    psd = data["psd"]
    sw = data["sw"]
    spreads = data["spreads"]
    port_state = data["port_state"]

    cur_month = datetime.now().month
    cur_day = datetime.now().day
    score = 0
    max_score = 0
    notes = []
    blockers = []

    # ── B1. Forward Curve (mandatory) ──
    shape, diff = get_forward_shape(sym, ch)
    if direction == "PUT" and shape == "STRONG_BACK":
        blockers.append(f"Curva STRONG BACKWARDATION ({diff:+.1f}%)")
    elif direction == "CALL" and shape == "CONTANGO" and diff > 10:
        blockers.append(f"Curva CONTANGO forte ({diff:+.1f}%)")

    # ── B2. IV mandatory ──
    und = opts.get("underlyings", {}).get(sym, {})
    ivr = und.get("iv_rank", {})
    cur_iv = ivr.get("current_iv")
    if cur_iv is not None and cur_iv * 100 < 15:
        blockers.append(f"IV={cur_iv*100:.0f}% — insuficiente para premium selling")

    # If any blocker, return blocked
    if blockers:
        return {
            "sym": sym, "name": NAMES.get(sym, sym), "direction": direction,
            "sector": SECTORS.get(sym, "?"),
            "score": 0, "max_score": 100, "pct": 0,
            "blocked": True, "blockers": blockers, "notes": [],
            "grade": "X",
        }

    # ── S1. Tier (0-15) ──
    max_score += 15
    if sym in TIER_1:
        score += 15; notes.append(("Tier 1", "+15"))
    elif sym in TIER_2:
        score += 10; notes.append(("Tier 2", "+10"))
    else:
        score += 5; notes.append(("Tier 3", "+5"))

    # ── S2. IV Level (0-15) ──
    max_score += 15
    if cur_iv is not None:
        iv_pct = cur_iv * 100
        if iv_pct >= 50:
            score += 15; notes.append((f"IV {iv_pct:.0f}%", "+15 (alta)"))
        elif iv_pct >= 35:
            score += 12; notes.append((f"IV {iv_pct:.0f}%", "+12"))
        elif iv_pct >= 25:
            score += 8; notes.append((f"IV {iv_pct:.0f}%", "+8"))
        elif iv_pct >= 20:
            score += 5; notes.append((f"IV {iv_pct:.0f}%", "+5"))
        else:
            notes.append((f"IV {iv_pct:.0f}%", "+0 (baixa)"))

    # ── S3. COT alignment (0-10) ──
    max_score += 10
    dis = cot.get("commodities", {}).get(sym, {}).get("disaggregated", {})
    cot_idx = dis.get("cot_index")
    if cot_idx is not None:
        if direction == "PUT" and cot_idx < 25:
            score += 10; notes.append((f"COT idx={cot_idx:.0f}", "+10 (shorts crowded, PUT safe)"))
        elif direction == "CALL" and cot_idx > 75:
            score += 10; notes.append((f"COT idx={cot_idx:.0f}", "+10 (longs crowded, CALL safe)"))
        elif direction == "PUT" and cot_idx > 85:
            score -= 5; notes.append((f"COT idx={cot_idx:.0f}", "-5 (crowded long vs PUT)"))
        elif direction == "CALL" and cot_idx < 15:
            score -= 5; notes.append((f"COT idx={cot_idx:.0f}", "-5 (crowded short vs CALL)"))
        else:
            score += 5; notes.append((f"COT idx={cot_idx:.0f}", "+5 (neutro)"))

    # ── S4. Seasonality (0-10) ──
    max_score += 10
    s_data = seas.get(sym, {}).get("monthly_returns", [])
    if s_data and cur_month <= len(s_data):
        mr = s_data[cur_month - 1]
        val = mr if isinstance(mr, (int, float)) else mr.get("avg", 0) if isinstance(mr, dict) else 0
        if direction == "PUT" and val > 1.5:
            score += 10; notes.append((f"Season {val:+.1f}%", "+10 (sobe, PUT safe)"))
        elif direction == "CALL" and val < -1.5:
            score += 10; notes.append((f"Season {val:+.1f}%", "+10 (cai, CALL safe)"))
        elif direction == "PUT" and val < -2:
            score -= 3; notes.append((f"Season {val:+.1f}%", "-3 (cai forte, PUT arriscado)"))
        elif direction == "CALL" and val > 2:
            score -= 3; notes.append((f"Season {val:+.1f}%", "-3 (sobe forte, CALL arriscado)"))
        else:
            score += 5; notes.append((f"Season {val:+.1f}%", "+5 (neutro)"))

    # ── S5. Predictability (0-10) ──
    max_score += 10
    pred = cross.get("underlying_predictability", {}).get(sym, {})
    clean_wr = pred.get("clean_wr", 50)
    if clean_wr >= 70:
        score += 10; notes.append((f"Clean WR={clean_wr:.0f}%", "+10"))
    elif clean_wr >= 55:
        score += 6; notes.append((f"Clean WR={clean_wr:.0f}%", "+6"))
    else:
        score += 2; notes.append((f"Clean WR={clean_wr:.0f}%", "+2 (fraco)"))

    # ── S6. Trade history net (0-10) ──
    max_score += 10
    best_unds = {u["sym"]: u for u in skill.get("best_underlyings", [])}
    net_hist = best_unds.get(sym, {}).get("net_total", 0)
    if net_hist > 50000:
        score += 10; notes.append((f"Net hist ${net_hist:,.0f}", "+10"))
    elif net_hist > 0:
        score += 6; notes.append((f"Net hist ${net_hist:,.0f}", "+6"))
    elif net_hist > -50000:
        score += 0; notes.append((f"Net hist ${net_hist:,.0f}", "+0"))
    else:
        score -= 5; notes.append((f"Net hist ${net_hist:,.0f}", "-5 (historico ruim)"))

    # ── S7. DTE availability (0-5) ──
    max_score += 5
    exps = und.get("expirations", {})
    best_dte = None
    for ek, ed in exps.items():
        dte = ed.get("days_to_exp", 0)
        if 25 <= dte <= 70:
            if best_dte is None or abs(dte - 45) < abs(best_dte - 45):
                best_dte = dte
    if best_dte:
        score += 5; notes.append((f"DTE={best_dte}d", "+5"))
    elif exps:
        score += 2; notes.append(("DTE fora janela", "+2"))
    else:
        notes.append(("Sem chain", "+0"))

    # ── S8. Month bonus (0-5) ──
    max_score += 5
    best_months = {11: 5, 5: 5, 7: 4, 10: 4, 8: 3, 2: 2}
    worst_months = {9: -3, 3: -2, 6: -1}
    month_pts = best_months.get(cur_month, 0) + worst_months.get(cur_month, 0)
    month_pts = max(0, min(5, month_pts))
    score += month_pts
    if month_pts > 0:
        notes.append((f"Mes bonus", f"+{month_pts}"))

    # ── F1. Fundamentals: PSD ending stocks deviation (bonus/penalty) ──
    max_score += 10
    psd_sym = psd.get("commodities", {}).get(sym, {})
    deviation = psd_sym.get("deviation")
    sw_sym = sw.get("commodities", {}).get(sym, {})
    stock_state = sw_sym.get("state", "")

    if deviation is not None:
        if direction == "PUT":
            # PUT selling = bearish bias. High stocks (surplus) = favorable
            if deviation > 20:
                score += 10
                notes.append((f"Estoques +{deviation:.0f}% vs 5y avg", "+10 (excedente favorece PUT)"))
            elif deviation > 0:
                score += 5
                notes.append((f"Estoques +{deviation:.0f}% vs 5y avg", "+5"))
            elif deviation < -15:
                score -= 5
                notes.append((f"Estoques {deviation:.0f}% vs 5y avg", "-5 (deficit, PUT arriscado)"))
            else:
                score += 2
                notes.append((f"Estoques {deviation:.0f}% vs 5y avg", "+2"))
        elif direction == "CALL":
            # CALL selling = bullish bias. Low stocks (deficit) = favorable
            if deviation < -15:
                score += 10
                notes.append((f"Estoques {deviation:.0f}% vs 5y avg", "+10 (deficit favorece CALL)"))
            elif deviation < 0:
                score += 5
                notes.append((f"Estoques {deviation:.0f}% vs 5y avg", "+5"))
            elif deviation > 20:
                score -= 5
                notes.append((f"Estoques +{deviation:.0f}% vs 5y avg", "-5 (excedente, CALL arriscado)"))
            else:
                score += 2
                notes.append((f"Estoques +{deviation:.0f}% vs 5y avg", "+2"))
    else:
        score += 3
        notes.append(("Estoques N/A", "+3 (neutro)"))

    # ── F2. Spread z-score relevance ──
    max_score += 5
    spread_map = spreads.get("spreads", {})
    spread_hints = {
        "ZC": ["zc_zm", "zc_zs"], "ZS": ["soy_crush", "zc_zs"],
        "ZW": ["ke_zw", "feed_wheat"], "KE": ["ke_zw"],
        "ZL": ["soy_crush", "zl_cl"], "ZM": ["soy_crush", "zc_zm"],
        "LE": ["feedlot", "cattle_crush"], "GF": ["feedlot"],
        "CL": ["zl_cl"], "SB": [], "KC": [], "CC": [],
    }
    relevant_spreads = spread_hints.get(sym, [])
    if relevant_spreads:
        max_z = 0
        for sp_name in relevant_spreads:
            sp = spread_map.get(sp_name, {})
            z = sp.get("zscore_1y")
            if z is not None and abs(z) > abs(max_z):
                max_z = z
        if abs(max_z) >= 2:
            score += 5
            notes.append((f"Spread z={max_z:.1f}", "+5 (extremo, oportunidade)"))
        elif abs(max_z) >= 1:
            score += 3
            notes.append((f"Spread z={max_z:.1f}", "+3"))
        else:
            score += 1
            notes.append((f"Spread z={max_z:.1f}", "+1"))
    else:
        score += 2
        notes.append(("No relevant spread", "+2"))

    # ── WASDE penalty for grains ──
    if sym in GRAINS and 8 <= cur_day <= 14:
        score -= 5
        notes.append(("WASDE window", "-5 (dia 8-14, aguardar report)"))

    # ── Capital availability penalty ──
    active_pct = port_state.get("active_pct", 0)
    regime_limit = port_state.get("regime_limit", 60)
    if active_pct > regime_limit:
        score -= 8
        notes.append((f"Capital {active_pct:.0f}%>{regime_limit}%", "-8 (sem margem)"))

    # ── Grade ──
    pct = (score / max_score * 100) if max_score > 0 else 0
    if pct >= 75:
        grade = "A"
    elif pct >= 60:
        grade = "B"
    elif pct >= 45:
        grade = "C"
    elif pct >= 30:
        grade = "D"
    else:
        grade = "F"

    return {
        "sym": sym, "name": NAMES.get(sym, sym), "direction": direction,
        "sector": SECTORS.get(sym, "?"),
        "score": score, "max_score": max_score, "pct": round(pct, 1),
        "blocked": False, "blockers": [],
        "notes": notes, "grade": grade,
    }


def main():
    print("=" * 65)
    print("OPPORTUNITY SCANNER — Consolidated Daily Ranking")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')} ({datetime.now().strftime('%A')})")
    print("=" * 65)

    # Load everything
    data = {
        "options": jload(PROC / "options_chain.json"),
        "cot": jload(PROC / "cot.json"),
        "seas": jload(PROC / "seasonality.json"),
        "contract_hist": jload(PROC / "contract_history.json"),
        "cross": jload(BASE / "pipeline" / "cross_analysis.json"),
        "skill": jload(BASE / "pipeline" / "trade_skill_base.json"),
        "psd": jload(PROC / "psd_ending_stocks.json"),
        "sw": jload(PROC / "stocks_watch.json"),
        "spreads": jload(PROC / "spreads.json"),
    }

    # Portfolio state
    port = jload(PROC / "ibkr_portfolio.json")
    net_liq = float(port.get("summary", {}).get("NetLiquidation", 0))
    total_margin = 0
    active_syms = set()
    for p in port.get("positions", []):
        if p.get("sec_type") in ("FOP", "FUT") and p.get("position", 0) < 0:
            sym_p = p["symbol"]
            active_syms.add(sym_p)
            total_margin += abs(int(p["position"])) * MARGIN_PER.get(sym_p, 2000)

    active_pct = (total_margin / net_liq * 100) if net_liq > 0 else 0

    # Detect VEGA regime
    regime_limit = 60
    for s in active_syms:
        u = data["options"].get("underlyings", {}).get(s, {})
        iv = u.get("iv_rank", {}).get("current_iv")
        if iv and iv * 100 >= 40:
            regime_limit = 65
            break

    data["port_state"] = {
        "active_pct": active_pct,
        "regime_limit": regime_limit,
        "active_syms": list(active_syms),
        "net_liq": net_liq,
    }

    print(f"\n  Capital: {active_pct:.1f}% em uso (limite {'VEGA ' if regime_limit==65 else ''}{regime_limit}%)")
    print(f"  Posicoes ativas: {', '.join(sorted(active_syms))}")

    # Scan all
    results = []
    for sym in SYMS:
        for direction in ["PUT", "CALL"]:
            r = scan_opportunity(sym, direction, data)
            results.append(r)

    # Separate
    blocked = [r for r in results if r["blocked"]]
    active = sorted([r for r in results if not r["blocked"]], key=lambda r: r["score"], reverse=True)
    puts = [r for r in active if r["direction"] == "PUT"]
    calls = [r for r in active if r["direction"] == "CALL"]

    # Print PUTs
    print(f"\n  {'='*63}")
    print(f"  TOP PUTS")
    print(f"  {'='*63}")
    for i, r in enumerate(puts[:5]):
        notes_str = " | ".join(f"{n[0]}:{n[1]}" for n in r["notes"][:3])
        print(f"  {i+1}. [{r['grade']}] {r['sym']:>4} ({r['name']:>10}) "
              f"| {r['score']:>3}/{r['max_score']} ({r['pct']:>5.1f}%) "
              f"| {notes_str}")

    # Print CALLs
    print(f"\n  {'='*63}")
    print(f"  TOP CALLS")
    print(f"  {'='*63}")
    for i, r in enumerate(calls[:5]):
        notes_str = " | ".join(f"{n[0]}:{n[1]}" for n in r["notes"][:3])
        print(f"  {i+1}. [{r['grade']}] {r['sym']:>4} ({r['name']:>10}) "
              f"| {r['score']:>3}/{r['max_score']} ({r['pct']:>5.1f}%) "
              f"| {notes_str}")

    # Blocked
    if blocked:
        print(f"\n  {'='*63}")
        print(f"  BLOQUEADOS ({len(blocked)})")
        print(f"  {'='*63}")
        for r in blocked:
            print(f"  [X] {r['sym']:>4} {r['direction']}: {'; '.join(r['blockers'])}")

    # Best overall
    print(f"\n  {'='*63}")
    print(f"  MELHOR OPORTUNIDADE DO DIA")
    print(f"  {'='*63}")
    if active:
        best = active[0]
        print(f"\n  >>> {best['sym']} ({best['name']}) {best['direction']} — "
              f"Grade {best['grade']} ({best['score']}/{best['max_score']}, {best['pct']:.0f}%)")
        print(f"\n  Detalhes:")
        for name, pts in best["notes"]:
            print(f"    {pts:>6}  {name}")
        print(f"\n  Proximo passo:")
        print(f"    python pipeline/skill_pretrade_checklist.py {best['sym']} {best['direction']}")
        print(f"    python pipeline/skill_position_sizing.py {best['sym']} {best['direction']} {best['score']}")

    # Save
    output = {
        "generated_at": datetime.now().isoformat(),
        "regime": "VEGA" if regime_limit == 65 else "NORMAL",
        "active_pct": round(active_pct, 1),
        "best_opportunity": {
            "sym": active[0]["sym"], "direction": active[0]["direction"],
            "grade": active[0]["grade"], "score": active[0]["score"],
            "max_score": active[0]["max_score"],
        } if active else None,
        "top_puts": [{
            "sym": r["sym"], "direction": r["direction"], "grade": r["grade"],
            "score": r["score"], "pct": r["pct"],
        } for r in puts[:5]],
        "top_calls": [{
            "sym": r["sym"], "direction": r["direction"], "grade": r["grade"],
            "score": r["score"], "pct": r["pct"],
        } for r in calls[:5]],
        "blocked": [{"sym": r["sym"], "direction": r["direction"],
                     "blockers": r["blockers"]} for r in blocked],
    }
    with open(OUT, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n  [SAVED] {OUT}")


if __name__ == "__main__":
    main()
