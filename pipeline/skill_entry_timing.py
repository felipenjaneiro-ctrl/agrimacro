#!/usr/bin/env python3
"""
AgriMacro — Entry Timing Scanner

Scores every underlying for entry today using the validated checklist:
  1. Underlying tier (SI, KE, HE, ZW, ZC, SB, GC = tier 1-2)
  2. Month favorable (Nov, May, Jul, Oct = best)
  3. Net credit positive (mandatory — checked at execution, flag here)
  4. Forward curve (mandatory — no strong backwardation for PUT selling)
  5. IV Rank / IV level
  6. Structure 22x22 available
  7. DTE 30-60 days
  8. COT not extreme against
  9. Seasonality aligned
  10. Predictability score from cross_analysis

Outputs ranked opportunities and blocks.

Usage:
  python pipeline/skill_entry_timing.py              # Full scan
  python pipeline/skill_entry_timing.py SI PUT        # Score specific
  python pipeline/skill_entry_timing.py KE PUT
  python pipeline/skill_entry_timing.py GF CALL
"""

import json
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent.parent
PROC = BASE / "agrimacro-dash" / "public" / "data" / "processed"
OUT = BASE / "pipeline" / "entry_timing.json"

SYMS = ["ZC", "ZS", "ZW", "KE", "ZM", "ZL",
        "LE", "GF", "HE", "SB", "KC", "CT", "CC",
        "CL", "NG", "GC", "SI"]

NAMES = {
    "ZC": "Milho", "ZS": "Soja", "ZW": "Trigo CBOT", "KE": "Trigo KC",
    "ZM": "Farelo", "ZL": "Oleo Soja", "LE": "Boi Gordo", "GF": "Feeder",
    "HE": "Suino", "SB": "Acucar", "KC": "Cafe", "CT": "Algodao",
    "CC": "Cacau", "CL": "Petroleo", "NG": "Gas Nat", "GC": "Ouro", "SI": "Prata",
}

TIER_1 = {"SI", "KE"}
TIER_2 = {"HE", "ZW", "ZC", "SB", "GC"}
TIER_3 = {"ZS", "ZM", "CC", "CL", "LE", "GF", "CT", "ZL", "OJ"}
TIER_AVOID = {"KC", "NG"}

BEST_MONTHS = {11: 3, 5: 3, 7: 2, 10: 2, 8: 2, 2: 1, 1: 1}  # month: bonus
WORST_MONTHS = {9: -2, 3: -1, 6: -1}

# Grains for WASDE check
GRAINS = {"ZC", "ZS", "ZW", "KE", "ZM", "ZL"}


def jload(path):
    for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            with open(path, encoding=enc) as f:
                return json.load(f)
        except Exception:
            continue
    return {}


def get_forward_curve(contracts_data):
    """Build forward curve shape per commodity from contract_history."""
    contracts = contracts_data.get("contracts", {})
    by_comm = defaultdict(list)
    for name, c in contracts.items():
        comm = c.get("commodity", "")
        bars = c.get("bars", [])
        if bars and bars[-1].get("close", 0) > 0:
            by_comm[comm].append((name, bars[-1]["close"]))

    curves = {}
    for comm, cs in by_comm.items():
        cs.sort(key=lambda x: x[0])
        if len(cs) < 2:
            curves[comm] = {"shape": "UNKNOWN", "diff_pct": 0}
            continue
        front = cs[0][1]
        back = cs[-1][1]
        diff = ((back - front) / front) * 100
        if diff < -3:
            shape = "STRONG_BACKWARDATION"
        elif diff < -1:
            shape = "MILD_BACKWARDATION"
        elif diff > 3:
            shape = "CONTANGO"
        else:
            shape = "FLAT"
        curves[comm] = {"shape": shape, "diff_pct": round(diff, 1),
                        "front": cs[0][0], "back": cs[-1][0]}
    return curves


def score_entry(sym, direction, options, cot_data, seasonality,
                curves, cross_data, skill_data, cur_month, cur_day):
    """
    Score an entry opportunity. Returns (score, max_score, details, blocked, block_reason).
    direction: "PUT" or "CALL"
    """
    score = 0
    max_score = 0
    details = []
    blocked = False
    block_reason = ""

    # ── 1. Underlying Tier (0-3) ──
    max_score += 3
    if sym in TIER_1:
        score += 3
        details.append({"check": "Underlying Tier", "result": "TIER 1", "pts": 3, "max": 3})
    elif sym in TIER_2:
        score += 2
        details.append({"check": "Underlying Tier", "result": "TIER 2", "pts": 2, "max": 3})
    elif sym in TIER_3:
        score += 1
        details.append({"check": "Underlying Tier", "result": "TIER 3", "pts": 1, "max": 3})
    elif sym in TIER_AVOID:
        details.append({"check": "Underlying Tier", "result": "AVOID", "pts": 0, "max": 3})
    else:
        details.append({"check": "Underlying Tier", "result": "UNKNOWN", "pts": 0, "max": 3})

    # ── 2. Month Score (0-3) ──
    max_score += 3
    month_pts = BEST_MONTHS.get(cur_month, 0) + WORST_MONTHS.get(cur_month, 0)
    month_pts = max(0, min(3, month_pts))
    score += month_pts
    month_label = datetime.now().strftime("%b")
    details.append({"check": f"Mes ({month_label})", "result": f"{month_pts}/3",
                    "pts": month_pts, "max": 3})

    # ── 3. Forward Curve (MANDATORY — blocks if wrong) ──
    curve = curves.get(sym, {})
    curve_shape = curve.get("shape", "UNKNOWN")
    diff_pct = curve.get("diff_pct", 0)

    if direction == "PUT":
        # For PUT selling: strong backwardation = BLOCKED (market stressed, puts risky)
        if curve_shape == "STRONG_BACKWARDATION":
            blocked = True
            block_reason = f"Curva em STRONG BACKWARDATION ({diff_pct:+.1f}%) — mercado estressado, PUT selling bloqueado"
            details.append({"check": "Curva Forward", "result": f"BLOCKED ({curve_shape} {diff_pct:+.1f}%)",
                            "pts": 0, "max": 0, "mandatory": True})
        else:
            details.append({"check": "Curva Forward", "result": f"OK ({curve_shape} {diff_pct:+.1f}%)",
                            "pts": 0, "max": 0, "mandatory": True})
    elif direction == "CALL":
        # For CALL selling: contango = fine, backwardation = actually OK for calls
        details.append({"check": "Curva Forward", "result": f"OK ({curve_shape} {diff_pct:+.1f}%)",
                        "pts": 0, "max": 0, "mandatory": True})

    # ── 4. IV Level (0-3) ──
    max_score += 3
    opt = options.get("underlyings", {}).get(sym, {})
    ivr = opt.get("iv_rank", {})
    cur_iv = ivr.get("current_iv")

    if cur_iv is not None:
        iv_pct = cur_iv * 100
        if iv_pct >= 50:
            iv_pts = 3
            iv_label = f"ALTA ({iv_pct:.0f}%)"
        elif iv_pct >= 30:
            iv_pts = 2
            iv_label = f"MODERADA ({iv_pct:.0f}%)"
        elif iv_pct >= 20:
            iv_pts = 1
            iv_label = f"BAIXA ({iv_pct:.0f}%)"
        else:
            iv_pts = 0
            iv_label = f"MUITO BAIXA ({iv_pct:.0f}%) — premium insuficiente"
        score += iv_pts
        details.append({"check": "IV Level", "result": iv_label, "pts": iv_pts, "max": 3})
    else:
        details.append({"check": "IV Level", "result": "N/A", "pts": 0, "max": 3})

    # ── 5. DTE Available (0-2) ──
    max_score += 2
    exps = opt.get("expirations", {})
    dte_ok = False
    best_dte = None
    for exp_key, exp_data in exps.items():
        dte = exp_data.get("days_to_exp", 0)
        if 25 <= dte <= 70:
            dte_ok = True
            if best_dte is None or abs(dte - 45) < abs(best_dte - 45):
                best_dte = dte

    if dte_ok:
        score += 2
        details.append({"check": "DTE 30-60d", "result": f"OK (best={best_dte}d)", "pts": 2, "max": 2})
    elif exps:
        dtes = [e.get("days_to_exp", 0) for e in exps.values()]
        closest = min(dtes, key=lambda d: abs(d - 45)) if dtes else 0
        score += 1
        details.append({"check": "DTE 30-60d", "result": f"MARGINAL (closest={closest}d)", "pts": 1, "max": 2})
    else:
        details.append({"check": "DTE 30-60d", "result": "No chain data", "pts": 0, "max": 2})

    # ── 6. COT Not Extreme Against (0-2) ──
    max_score += 2
    cot_sym = cot_data.get("commodities", {}).get(sym, {})
    dis = cot_sym.get("disaggregated", {})
    cot_idx = dis.get("cot_index")

    if cot_idx is not None:
        if direction == "PUT":
            # For PUT selling (bearish bias): COT crowded long (>85) = against us
            if cot_idx > 85:
                details.append({"check": "COT vs Direction", "result": f"AGAINST (idx={cot_idx:.0f}, crowded long vs PUT sell)",
                                "pts": 0, "max": 2})
            elif cot_idx < 25:
                score += 2
                details.append({"check": "COT vs Direction", "result": f"ALIGNED (idx={cot_idx:.0f}, shorts crowded = reversal up)",
                                "pts": 2, "max": 2})
            else:
                score += 1
                details.append({"check": "COT vs Direction", "result": f"NEUTRAL (idx={cot_idx:.0f})",
                                "pts": 1, "max": 2})
        elif direction == "CALL":
            if cot_idx < 15:
                details.append({"check": "COT vs Direction", "result": f"AGAINST (idx={cot_idx:.0f}, crowded short vs CALL sell)",
                                "pts": 0, "max": 2})
            elif cot_idx > 75:
                score += 2
                details.append({"check": "COT vs Direction", "result": f"ALIGNED (idx={cot_idx:.0f}, longs crowded = reversal down)",
                                "pts": 2, "max": 2})
            else:
                score += 1
                details.append({"check": "COT vs Direction", "result": f"NEUTRAL (idx={cot_idx:.0f})",
                                "pts": 1, "max": 2})
    else:
        details.append({"check": "COT vs Direction", "result": "N/A", "pts": 0, "max": 2})

    # ── 7. Seasonality Aligned (0-2) ──
    max_score += 2
    seas = seasonality.get(sym, {}).get("monthly_returns", [])
    if seas and cur_month <= len(seas):
        mr = seas[cur_month - 1]
        val = mr if isinstance(mr, (int, float)) else mr.get("avg", 0) if isinstance(mr, dict) else 0

        if direction == "PUT":
            # PUT selling profits when price stays flat or goes up
            if val > 1.0:
                score += 2
                details.append({"check": "Sazonalidade", "result": f"ALIGNED (avg={val:+.2f}%, historicamente sobe)", "pts": 2, "max": 2})
            elif val > -0.5:
                score += 1
                details.append({"check": "Sazonalidade", "result": f"NEUTRAL (avg={val:+.2f}%)", "pts": 1, "max": 2})
            else:
                details.append({"check": "Sazonalidade", "result": f"AGAINST (avg={val:+.2f}%, historicamente cai)", "pts": 0, "max": 2})
        elif direction == "CALL":
            if val < -1.0:
                score += 2
                details.append({"check": "Sazonalidade", "result": f"ALIGNED (avg={val:+.2f}%, historicamente cai)", "pts": 2, "max": 2})
            elif val < 0.5:
                score += 1
                details.append({"check": "Sazonalidade", "result": f"NEUTRAL (avg={val:+.2f}%)", "pts": 1, "max": 2})
            else:
                details.append({"check": "Sazonalidade", "result": f"AGAINST (avg={val:+.2f}%, historicamente sobe)", "pts": 0, "max": 2})
    else:
        details.append({"check": "Sazonalidade", "result": "N/A", "pts": 0, "max": 2})

    # ── 8. Predictability from History (0-2) ──
    max_score += 2
    pred = cross_data.get("underlying_predictability", {}).get(sym, {})
    clean_wr = pred.get("clean_wr", 0)
    if clean_wr >= 70:
        score += 2
        details.append({"check": "Historico (clean WR)", "result": f"FORTE ({clean_wr:.0f}%)", "pts": 2, "max": 2})
    elif clean_wr >= 55:
        score += 1
        details.append({"check": "Historico (clean WR)", "result": f"OK ({clean_wr:.0f}%)", "pts": 1, "max": 2})
    elif pred:
        details.append({"check": "Historico (clean WR)", "result": f"FRACO ({clean_wr:.0f}%)", "pts": 0, "max": 2})
    else:
        details.append({"check": "Historico (clean WR)", "result": "N/A", "pts": 0, "max": 2})

    # ── WASDE warning for grains ──
    if sym in GRAINS and 8 <= cur_day <= 14:
        details.append({"check": "WASDE Warning", "result": "JANELA WASDE — considerar esperar pos-report",
                        "pts": 0, "max": 0, "warning": True})

    # ── Grade ──
    pct = (score / max_score * 100) if max_score > 0 else 0
    if pct >= 75:
        grade = "A"
    elif pct >= 55:
        grade = "B"
    elif pct >= 35:
        grade = "C"
    else:
        grade = "D"

    if blocked:
        grade = "X"

    return {
        "sym": sym,
        "name": NAMES.get(sym, sym),
        "direction": direction,
        "score": score,
        "max_score": max_score,
        "pct": round(pct, 1),
        "grade": grade,
        "blocked": blocked,
        "block_reason": block_reason,
        "details": details,
    }


def main():
    print("=" * 60)
    print("ENTRY TIMING SCANNER")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')} ({datetime.now().strftime('%A')})")
    print("=" * 60)

    # Load all data
    options = jload(PROC / "options_chain.json")
    cot_data = jload(PROC / "cot.json")
    seasonality = jload(PROC / "seasonality.json")
    contract_hist = jload(PROC / "contract_history.json")
    cross_data = jload(BASE / "pipeline" / "cross_analysis.json")
    skill_data = jload(BASE / "pipeline" / "trade_skill_base.json")

    curves = get_forward_curve(contract_hist)
    cur_month = datetime.now().month
    cur_day = datetime.now().day

    # Check if specific underlying requested
    args = sys.argv[1:]
    if len(args) >= 2:
        sym = args[0].upper()
        direction = args[1].upper()
        if sym not in SYMS:
            print(f"[ERR] Unknown underlying: {sym}")
            return
        if direction not in ("PUT", "CALL"):
            print(f"[ERR] Direction must be PUT or CALL")
            return

        result = score_entry(sym, direction, options, cot_data, seasonality,
                             curves, cross_data, skill_data, cur_month, cur_day)
        print(f"\n  {sym} {direction}: Grade {result['grade']} | Score {result['score']}/{result['max_score']} ({result['pct']:.0f}%)")
        if result["blocked"]:
            print(f"  ** BLOCKED: {result['block_reason']}")
        for d in result["details"]:
            warn = " !!" if d.get("warning") else ""
            mandatory = " [MANDATORY]" if d.get("mandatory") else ""
            print(f"    {'[X]' if d['pts']==d['max'] and d['max']>0 else '[ ]' if d['max']>0 else '[i]'} "
                  f"{d['check']}: {d['result']}{mandatory}{warn}")
        return

    # Full scan — score all underlyings for both PUT and CALL
    all_results = []
    for sym in SYMS:
        for direction in ["PUT", "CALL"]:
            result = score_entry(sym, direction, options, cot_data, seasonality,
                                 curves, cross_data, skill_data, cur_month, cur_day)
            all_results.append(result)

    # Separate blocked vs go
    blocked = [r for r in all_results if r["blocked"]]
    active = [r for r in all_results if not r["blocked"]]
    active.sort(key=lambda r: r["pct"], reverse=True)

    # Print PUT ranking
    puts = [r for r in active if r["direction"] == "PUT"]
    calls = [r for r in active if r["direction"] == "CALL"]

    print(f"\n{'='*60}")
    print("RANKING — PUT SELLING (hoje)")
    print(f"{'='*60}")
    for i, r in enumerate(puts):
        grade_clr = {"A": "***", "B": "** ", "C": "*  ", "D": "   "}.get(r["grade"], "   ")
        print(f"  {i+1:>2}. {grade_clr} {r['sym']:>4} ({r['name']:>10}): "
              f"Grade {r['grade']} | {r['score']:>2}/{r['max_score']} ({r['pct']:>5.1f}%)")

    print(f"\n{'='*60}")
    print("RANKING — CALL SELLING (hoje)")
    print(f"{'='*60}")
    for i, r in enumerate(calls):
        grade_clr = {"A": "***", "B": "** ", "C": "*  ", "D": "   "}.get(r["grade"], "   ")
        print(f"  {i+1:>2}. {grade_clr} {r['sym']:>4} ({r['name']:>10}): "
              f"Grade {r['grade']} | {r['score']:>2}/{r['max_score']} ({r['pct']:>5.1f}%)")

    # Blocked
    if blocked:
        print(f"\n{'='*60}")
        print("BLOQUEADOS (curva desfavoravel)")
        print(f"{'='*60}")
        for r in blocked:
            print(f"  {r['sym']:>4} {r['direction']}: {r['block_reason']}")

    # Best opportunity
    if active:
        best = active[0]
        print(f"\n{'='*60}")
        print(f"MELHOR OPORTUNIDADE: {best['sym']} {best['direction']} — Grade {best['grade']} ({best['score']}/{best['max_score']})")
        print(f"{'='*60}")
        for d in best["details"]:
            warn = " !!" if d.get("warning") else ""
            mandatory = " [MANDATORY]" if d.get("mandatory") else ""
            check_mark = "[X]" if d["pts"] == d["max"] and d["max"] > 0 else "[ ]" if d["max"] > 0 else "[i]"
            print(f"  {check_mark} {d['check']}: {d['result']}{mandatory}{warn}")

    # Save
    output = {
        "generated_at": datetime.now().isoformat(),
        "scan_date": datetime.now().strftime("%Y-%m-%d"),
        "scan_month": datetime.now().strftime("%b"),
        "total_scanned": len(all_results),
        "blocked_count": len(blocked),
        "best_opportunity": {
            "sym": best["sym"], "direction": best["direction"],
            "grade": best["grade"], "score": best["score"],
            "max_score": best["max_score"], "pct": best["pct"],
        } if active else None,
        "put_ranking": [{
            "sym": r["sym"], "name": r["name"], "grade": r["grade"],
            "score": r["score"], "max_score": r["max_score"], "pct": r["pct"],
        } for r in puts],
        "call_ranking": [{
            "sym": r["sym"], "name": r["name"], "grade": r["grade"],
            "score": r["score"], "max_score": r["max_score"], "pct": r["pct"],
        } for r in calls],
        "blocked": [{
            "sym": r["sym"], "direction": r["direction"],
            "reason": r["block_reason"],
        } for r in blocked],
    }
    with open(OUT, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n[SAVED] {OUT}")


if __name__ == "__main__":
    main()
