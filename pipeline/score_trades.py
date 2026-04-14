#!/usr/bin/env python3
"""
AgriMacro — Trade Scoring Engine

Scores each historical spread against entry_scoring filters
and validated_rules from trade_skill_base.json.

Outputs:
  1. Score per spread (how many filters aligned)
  2. Rules violated per spread
  3. Top 5 best and worst entries
  4. Updated trade_skill_base.json with scored_entries section

Run:
  python pipeline/score_trades.py
"""

import json
import sys
import os
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_trade_skill import (
    CSV_DIR, OUTPUT_PATH, load_all_trades, resolve_underlying,
    spread_key, parse_fop_symbol, KNOWN_ROOTS, UNDERLYING_NAMES,
)

BASE = Path(__file__).parent.parent

# ── WASDE dates (approximate — usually around 10th-12th of each month) ──
def is_wasde_window(date_str, window_days=2):
    """Check if date is within window_days of typical WASDE release (10th-12th)."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return 8 <= dt.day <= 14
    except Exception:
        return False

# Grain symbols affected by WASDE
GRAIN_SYMS = {"ZC", "ZS", "ZW", "ZM", "ZL", "KE"}

# Correlated groups (R11: max 3 correlated simultaneously)
CORRELATION_GROUPS = {
    "grains": {"ZC", "ZS", "ZW", "ZM", "ZL", "KE"},
    "livestock": {"LE", "GF", "HE"},
    "energy": {"CL", "NG"},
    "metals": {"GC", "SI"},
    "softs": {"KC", "CC", "SB", "CT"},
}


def score_spread(spread, all_spreads, skill_data):
    """
    Score a single spread against entry_scoring filters and validated_rules.
    Returns: (score, max_score, filters_hit, rules_violated)
    """
    sym = spread["sym"]  # e.g. "ZL Q5", "SI JUN24"
    und = spread["underlying"]
    year = spread["year"]
    structure = spread.get("structure", "")
    net = spread["net"]
    entry_month = spread.get("entry_month")  # 1-12
    entry_date = spread.get("entry_date", "")
    entry_weekday = spread.get("entry_weekday")  # 0=Mon, 4=Fri

    seasonality = skill_data.get("seasonality_edge", {}).get("patterns", [])
    best_und = {u["sym"]: u for u in skill_data.get("best_underlyings", [])}
    filters = skill_data.get("entry_scoring", {}).get("filters", [])

    score = 0
    max_score = 0
    filters_hit = []
    mandatory_fail = False

    for filt in filters:
        name = filt["name"]
        weight = filt["weight"]
        mandatory = filt.get("mandatory", False)

        if mandatory:
            # Mandatory filters are pass/fail, not scored
            # R01: Forward curve — we can't retroactively check this from CSVs
            # Mark as UNKNOWN
            filters_hit.append({"filter": name, "result": "UNKNOWN", "mandatory": True})
            continue

        max_score += weight
        hit = False

        # "IV Rank > 50%" — can't check retroactively without IV history at entry time
        if "IV Rank" in name:
            filters_hit.append({"filter": name, "result": "UNKNOWN", "weight": weight})
            continue

        # "Sazonalidade alinhada"
        if "Sazonalidade" in name and entry_month:
            for pat in seasonality:
                if pat["sym"] == und and entry_month in pat["months"]:
                    # Check direction alignment
                    is_selling = "SELL" in structure
                    if ("sell" in pat["direction"] and is_selling) or \
                       ("buy" in pat["direction"] and not is_selling):
                        hit = True
                        break
            if hit:
                score += weight
                filters_hit.append({"filter": name, "result": "ALIGNED", "weight": weight})
            else:
                filters_hit.append({"filter": name, "result": "NOT_ALIGNED", "weight": weight})
            continue

        # "COT nao extremo contra" — can't check retroactively
        if "COT" in name:
            filters_hit.append({"filter": name, "result": "UNKNOWN", "weight": weight})
            continue

        # "Fundamental IA validado" — can't check retroactively
        if "Fundamental" in name:
            filters_hit.append({"filter": name, "result": "UNKNOWN", "weight": weight})
            continue

        # "MA50/200 suporte proximo" — can't check retroactively
        if "MA50" in name:
            filters_hit.append({"filter": name, "result": "UNKNOWN", "weight": weight})
            continue

        # "EMA9 virando" — can't check retroactively
        if "EMA9" in name:
            filters_hit.append({"filter": name, "result": "UNKNOWN", "weight": weight})
            continue

        # "Underlying no historico positivo"
        if "historico positivo" in name.lower() or "hist" in name.lower():
            if und in best_und and best_und[und].get("net_total", 0) > 0:
                hit = True
                score += weight
                filters_hit.append({"filter": name, "result": "POSITIVE_HISTORY", "weight": weight})
            else:
                filters_hit.append({"filter": name, "result": "NEGATIVE_HISTORY", "weight": weight})
            continue

        # Fallback
        filters_hit.append({"filter": name, "result": "UNKNOWN", "weight": weight})

    # ── Rule violations ──
    rules_violated = []
    rules = skill_data.get("validated_rules", [])

    for rule in rules:
        rid = rule["id"]
        severity = rule["severity"]

        # R03: WASDE day risk for grains
        if rid == "R03" and und in GRAIN_SYMS:
            if entry_date and is_wasde_window(entry_date):
                rules_violated.append({
                    "rule": rid,
                    "severity": severity,
                    "detail": f"Entry {entry_date} within WASDE window for {und}",
                })

        # R06: Max loss > 2x credit
        if rid == "R06" and net < 0:
            # Estimate: if this is premium selling, typical credit ~ avg_credit from best_und
            avg_credit = best_und.get(und, {}).get("biggest_win", 10000)
            if avg_credit > 0 and abs(net) > 2 * avg_credit:
                rules_violated.append({
                    "rule": rid,
                    "severity": severity,
                    "detail": f"Loss ${abs(net):,.0f} > 2x biggest_win ${avg_credit:,.0f}",
                })

        # R07: Multiple re-entries (high trade count on a losing spread)
        if rid == "R07" and net < -10000 and spread["trades"] > 6:
            rules_violated.append({
                "rule": rid,
                "severity": severity,
                "detail": f"{spread['trades']} trades on losing spread (possible re-entries after stop)",
            })

        # R11: Correlated underlyings
        if rid == "R11":
            for group_name, group_syms in CORRELATION_GROUPS.items():
                if und in group_syms:
                    # Count other losing spreads in same group, same period
                    correlated_losses = [
                        s for s in all_spreads
                        if s["underlying"] in group_syms
                        and s["underlying"] != und
                        and s["year"] == year
                        and s["net"] < -5000
                    ]
                    if len(correlated_losses) >= 2:
                        others = ", ".join(set(s["underlying"] for s in correlated_losses[:3]))
                        rules_violated.append({
                            "rule": rid,
                            "severity": severity,
                            "detail": f"Correlated losses in {group_name}: {und} + {others}",
                        })
                    break

        # R12: Friday afternoon entry
        if rid == "R12" and entry_weekday == 4:
            rules_violated.append({
                "rule": rid,
                "severity": severity,
                "detail": f"Entry on Friday ({entry_date})",
            })

    return score, max_score, filters_hit, rules_violated


def main():
    print("=" * 60)
    print("TRADE SCORING ENGINE — Retroactive Analysis")
    print("=" * 60)

    # Load skill base
    if not OUTPUT_PATH.exists():
        print("[ERR] trade_skill_base.json not found. Run build_trade_skill.py first.")
        return
    skill_data = json.load(open(OUTPUT_PATH))

    # Load raw trades
    trades = load_all_trades()
    if not trades:
        print("[ERR] No trades found.")
        return

    # Filter to commodity only
    commodity_cats = {"Options On Futures", "Futures"}
    trades = [t for t in trades if t["asset_category"] in commodity_cats]
    print(f"  Commodity trades: {len(trades)}")

    # Group by spread (underlying + expiry)
    spreads = defaultdict(lambda: {
        "trades": 0, "net": 0, "pnl": 0, "comm": 0,
        "underlying": "", "year": "", "structure": "",
        "entry_date": "", "entry_month": None, "entry_weekday": None,
        "calls_sold": 0, "calls_bought": 0,
        "puts_sold": 0, "puts_bought": 0,
        "is_futures": False,
    })

    for t in trades:
        skey = t["spread_key"]
        s = spreads[skey]
        s["trades"] += 1
        s["net"] += t["net_pnl"]
        s["pnl"] += t["realized_pnl"]
        s["comm"] += t["commission"]
        s["underlying"] = t["underlying"]

        year = t["year"]
        if not s["year"] or year > s["year"]:
            s["year"] = year

        # Track earliest date as entry
        if not s["entry_date"] or t["date"] < s["entry_date"]:
            s["entry_date"] = t["date"]
            try:
                dt = datetime.strptime(t["date"], "%Y-%m-%d")
                s["entry_month"] = dt.month
                s["entry_weekday"] = dt.weekday()
            except Exception:
                pass

        # Structure
        right = t.get("right")
        qty = t["quantity"]
        cat = t["asset_category"]
        if cat == "Futures":
            s["is_futures"] = True
        elif right == "C":
            if qty < 0:
                s["calls_sold"] += abs(int(qty))
            elif qty > 0:
                s["calls_bought"] += int(qty)
        elif right == "P":
            if qty < 0:
                s["puts_sold"] += abs(int(qty))
            elif qty > 0:
                s["puts_bought"] += int(qty)

    # Build structure strings
    spread_list = []
    for skey, s in spreads.items():
        if s["is_futures"]:
            struct = "FUT"
        else:
            parts = []
            if s["calls_sold"] or s["calls_bought"]:
                parts.append(f"SELL C={s['calls_sold']} BUY C={s['calls_bought']}")
            if s["puts_sold"] or s["puts_bought"]:
                parts.append(f"SELL P={s['puts_sold']} BUY P={s['puts_bought']}")
            struct = " | ".join(parts) if parts else "UNKNOWN"
        s["structure"] = struct
        s["sym"] = skey
        spread_list.append(s)

    print(f"  Spreads: {len(spread_list)}")

    # ── Score each spread ──
    print(f"\n  Scoring {len(spread_list)} spreads...")
    scored = []
    total_violations = 0

    for s in spread_list:
        score, max_score, filters_hit, rules_violated = score_spread(
            s, spread_list, skill_data
        )
        total_violations += len(rules_violated)
        scored.append({
            "sym": s["sym"],
            "underlying": s["underlying"],
            "net": round(s["net"], 2),
            "trades": s["trades"],
            "year": s["year"],
            "entry_date": s["entry_date"],
            "structure": s["structure"],
            "score": score,
            "max_score": max_score,
            "filters": filters_hit,
            "rules_violated": rules_violated,
            "violation_count": len(rules_violated),
        })

    # ── Sort by net P&L ──
    by_pnl = sorted(scored, key=lambda x: x["net"])
    best_5 = list(reversed(by_pnl[-5:]))
    worst_5 = by_pnl[:5]

    # ── Sort by most violations ──
    by_violations = sorted(scored, key=lambda x: x["violation_count"], reverse=True)
    most_violations = [s for s in by_violations if s["violation_count"] > 0][:10]

    # ── Rule violation summary ──
    rule_counts = defaultdict(int)
    rule_severity_counts = defaultdict(int)
    for s in scored:
        for v in s["rules_violated"]:
            rule_counts[v["rule"]] += 1
            rule_severity_counts[v["severity"]] += 1

    # ── Print Report ──
    print(f"\n{'='*60}")
    print(f"SCORING REPORT")
    print(f"{'='*60}")

    print(f"\n--- TOP 5 BEST ENTRIES ---")
    for i, s in enumerate(best_5):
        rules_str = ", ".join(v["rule"] for v in s["rules_violated"]) or "none"
        print(f"  {i+1}. {s['sym']:>14} | ${s['net']:>12,.2f} | {s['trades']:>3}t | "
              f"score={s['score']}/{s['max_score']} | "
              f"violations: {rules_str} | {s['structure']}")

    print(f"\n--- TOP 5 WORST ENTRIES ---")
    for i, s in enumerate(worst_5):
        rules_str = ", ".join(v["rule"] for v in s["rules_violated"]) or "none"
        print(f"  {i+1}. {s['sym']:>14} | ${s['net']:>12,.2f} | {s['trades']:>3}t | "
              f"score={s['score']}/{s['max_score']} | "
              f"violations: {rules_str} | {s['structure']}")
        for v in s["rules_violated"]:
            print(f"       [{v['severity']}] {v['rule']}: {v['detail']}")

    print(f"\n--- RULE VIOLATION SUMMARY ---")
    rules_map = {r["id"]: r for r in skill_data.get("validated_rules", [])}
    for rid, count in sorted(rule_counts.items(), key=lambda x: x[1], reverse=True):
        r = rules_map.get(rid, {})
        print(f"  {rid} ({r.get('severity','?'):>9}): {count:>3}x — {r.get('rule','')[:70]}")

    print(f"\n  Total violations: {total_violations} across {len(most_violations)} spreads")
    print(f"  HARD_STOP: {rule_severity_counts.get('HARD_STOP', 0)}")
    print(f"  STRONG:    {rule_severity_counts.get('STRONG', 0)}")
    print(f"  MODERATE:  {rule_severity_counts.get('MODERATE', 0)}")

    print(f"\n--- MOST VIOLATED SPREADS ---")
    for s in most_violations[:5]:
        rules_str = ", ".join(f"{v['rule']}({v['severity'][:1]})" for v in s["rules_violated"])
        print(f"  {s['sym']:>14} | ${s['net']:>12,.2f} | violations: {rules_str}")

    # ── Correlation analysis ──
    print(f"\n--- CORRELATION EXPOSURE (R11) ---")
    for group_name, group_syms in CORRELATION_GROUPS.items():
        group_spreads = [s for s in scored if s["underlying"] in group_syms and abs(s["net"]) > 1000]
        if not group_spreads:
            continue
        total_net = sum(s["net"] for s in group_spreads)
        losers = [s for s in group_spreads if s["net"] < 0]
        winners = [s for s in group_spreads if s["net"] > 0]
        print(f"  {group_name:>10}: {len(group_spreads)} spreads | "
              f"Net: ${total_net:>12,.2f} | "
              f"{len(winners)}W / {len(losers)}L")

    # ── Save scored entries to skill base ──
    skill_data["scored_entries"] = {
        "generated_at": datetime.now().isoformat(),
        "total_spreads_scored": len(scored),
        "total_violations": total_violations,
        "violation_summary": dict(rule_counts),
        "best_5": [{
            "sym": s["sym"],
            "net": s["net"],
            "trades": s["trades"],
            "year": s["year"],
            "score": s["score"],
            "violations": [v["rule"] for v in s["rules_violated"]],
            "structure": s["structure"],
        } for s in best_5],
        "worst_5": [{
            "sym": s["sym"],
            "net": s["net"],
            "trades": s["trades"],
            "year": s["year"],
            "score": s["score"],
            "violations": [v["rule"] for v in s["rules_violated"]],
            "violation_details": [
                f"[{v['severity']}] {v['rule']}: {v['detail']}"
                for v in s["rules_violated"]
            ],
            "structure": s["structure"],
        } for s in worst_5],
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(skill_data, f, indent=2, default=str)
    print(f"\n[SAVED] scored_entries added to {OUTPUT_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
