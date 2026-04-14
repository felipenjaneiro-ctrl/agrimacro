#!/usr/bin/env python3
"""
AgriMacro — Cross Analysis: Cycles x Scores x Seasonality

Joins cycle data (rolls, duration) with scoring data (rule violations,
entry filters) and seasonality alignment to answer:

1. Win rate by roll count — does rolling correlate with losing?
2. Win rate by entry grade — does score predict outcome?
3. Best indicator combinations (highest clean rate)
4. Most predictable underlyings
5. Best entry months

Run:
  python pipeline/cross_analysis.py
"""

import json
import sys
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from build_trade_skill import CSV_DIR, load_all_trades, spread_key, KNOWN_ROOTS, UNDERLYING_NAMES
from backtest_entries import load_commodity_trades, detect_cycles, detect_rolls

BASE = Path(__file__).parent.parent
SKILL_PATH = BASE / "pipeline" / "trade_skill_base.json"
CYCLE_PATH = BASE / "pipeline" / "cycle_analysis.json"
OUTPUT_PATH = BASE / "pipeline" / "cross_analysis.json"

MONTH_NAMES = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# Seasonality patterns from skill base
def load_seasonality():
    skill = json.load(open(SKILL_PATH))
    return skill.get("seasonality_edge", {}).get("patterns", [])


def is_season_aligned(und, month, structure, patterns):
    """Check if entry aligns with known seasonality pattern."""
    for pat in patterns:
        if pat["sym"] == und and month in pat["months"]:
            is_selling = "sell=" in structure.lower() and "sell=0" not in structure.lower()
            direction = pat["direction"]
            if "sell" in direction and is_selling:
                return True
            if "buy" in direction and not is_selling:
                return True
    return False


def grade_cycle(cycle, patterns, best_unds):
    """
    Grade a cycle A/B/C/D/F based on available indicators:
    - Seasonality aligned (+2)
    - Underlying historically profitable (+1)
    - No rolls (+2)
    - Duration 20-60 days (+1)
    - Not on correlation group loss year (+1)
    Max = 7
    """
    score = 0
    indicators = []

    und = cycle["und"]
    month = cycle["entry_month"]
    structure = cycle["structure"]
    rolls = cycle["rolls"]
    duration = cycle["duration_days"]

    # Seasonality
    if is_season_aligned(und, month, structure, patterns):
        score += 2
        indicators.append("SEASON")

    # Underlying history
    if und in best_unds and best_unds[und] > 0:
        score += 1
        indicators.append("UND_POS")

    # No rolls
    if rolls == 0:
        score += 2
        indicators.append("CLEAN")

    # Duration sweet spot
    if 20 <= duration <= 60:
        score += 1
        indicators.append("DUR_OK")

    # Short duration (< 15 days) is a warning
    if duration < 7:
        score -= 1
        indicators.append("DUR_SHORT")

    # Grade
    if score >= 6:
        grade = "A"
    elif score >= 4:
        grade = "B"
    elif score >= 2:
        grade = "C"
    elif score >= 0:
        grade = "D"
    else:
        grade = "F"

    return grade, score, indicators


def main():
    print("=" * 60)
    print("CROSS ANALYSIS — Cycles x Scores x Seasonality")
    print("=" * 60)

    # Load data
    patterns = load_seasonality()
    skill = json.load(open(SKILL_PATH))
    best_unds = {u["sym"]: u["net_total"] for u in skill.get("best_underlyings", [])}

    # Load and build cycles
    trades = load_commodity_trades()
    trades = [t for t in trades if t["und"] in KNOWN_ROOTS]
    print(f"  {len(trades)} commodity trades")

    cycles = detect_cycles(trades)
    cycles = detect_rolls(cycles)
    print(f"  {len(cycles)} cycles detected")

    # Grade each cycle
    for c in cycles:
        grade, score, indicators = grade_cycle(c, patterns, best_unds)
        c["grade"] = grade
        c["score"] = score
        c["indicators"] = indicators
        c["is_winner"] = c["net"] > 0
        c["is_season"] = "SEASON" in indicators
        c["is_clean"] = c["rolls"] == 0

    # ════════════════════════════════════════════════════
    # 1. WIN RATE BY ROLL COUNT
    # ════════════════════════════════════════════════════
    roll_buckets = {"0": [], "1": [], "2+": []}
    for c in cycles:
        if c["rolls"] == 0:
            roll_buckets["0"].append(c)
        elif c["rolls"] == 1:
            roll_buckets["1"].append(c)
        else:
            roll_buckets["2+"].append(c)

    print(f"\n{'='*60}")
    print("1. WIN RATE BY ROLL COUNT")
    print(f"{'='*60}")
    roll_stats = {}
    for bucket, cs in roll_buckets.items():
        if not cs:
            continue
        wins = [c for c in cs if c["is_winner"]]
        total_net = sum(c["net"] for c in cs)
        avg_net = total_net / len(cs)
        wr = len(wins) / len(cs) * 100
        avg_dur = sum(c["duration_days"] for c in cs) / len(cs)
        roll_stats[bucket] = {
            "count": len(cs), "winners": len(wins),
            "win_rate": round(wr, 1), "avg_net": round(avg_net, 2),
            "total_net": round(total_net, 2), "avg_duration": round(avg_dur, 1),
        }
        print(f"  {bucket:>3} rolls: {len(cs):>3} cycles | WR={wr:>5.1f}% | "
              f"avg=${avg_net:>10,.2f} | total=${total_net:>12,.2f} | dur={avg_dur:.0f}d")

    # ════════════════════════════════════════════════════
    # 2. WIN RATE BY ENTRY GRADE
    # ════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print("2. WIN RATE BY ENTRY GRADE")
    print(f"{'='*60}")
    grade_stats = {}
    for grade in ["A", "B", "C", "D", "F"]:
        cs = [c for c in cycles if c["grade"] == grade]
        if not cs:
            continue
        wins = [c for c in cs if c["is_winner"]]
        total_net = sum(c["net"] for c in cs)
        avg_net = total_net / len(cs)
        wr = len(wins) / len(cs) * 100
        clean_pct = len([c for c in cs if c["is_clean"]]) / len(cs) * 100
        grade_stats[grade] = {
            "count": len(cs), "winners": len(wins),
            "win_rate": round(wr, 1), "avg_net": round(avg_net, 2),
            "total_net": round(total_net, 2),
            "clean_pct": round(clean_pct, 1),
        }
        print(f"  Grade {grade}: {len(cs):>3} cycles | WR={wr:>5.1f}% | "
              f"avg=${avg_net:>10,.2f} | total=${total_net:>12,.2f} | "
              f"clean={clean_pct:.0f}%")

    # ════════════════════════════════════════════════════
    # 3. BEST INDICATOR COMBINATIONS
    # ════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print("3. BEST INDICATOR COMBINATIONS (clean rate & WR)")
    print(f"{'='*60}")

    # Test all combinations of indicators
    indicator_combos = [
        ("SEASON only", lambda c: c["is_season"]),
        ("CLEAN only", lambda c: c["is_clean"]),
        ("UND_POS only", lambda c: "UND_POS" in c["indicators"]),
        ("SEASON + CLEAN", lambda c: c["is_season"] and c["is_clean"]),
        ("SEASON + UND_POS", lambda c: c["is_season"] and "UND_POS" in c["indicators"]),
        ("CLEAN + UND_POS", lambda c: c["is_clean"] and "UND_POS" in c["indicators"]),
        ("SEASON + CLEAN + UND_POS", lambda c: c["is_season"] and c["is_clean"] and "UND_POS" in c["indicators"]),
        ("DUR_OK + CLEAN", lambda c: "DUR_OK" in c["indicators"] and c["is_clean"]),
        ("ALL (season+clean+und+dur)", lambda c: all(i in c["indicators"] for i in ["SEASON", "CLEAN", "UND_POS", "DUR_OK"])),
    ]

    combo_stats = {}
    for name, filt in indicator_combos:
        cs = [c for c in cycles if filt(c)]
        if len(cs) < 3:
            continue
        wins = [c for c in cs if c["is_winner"]]
        total_net = sum(c["net"] for c in cs)
        wr = len(wins) / len(cs) * 100
        avg_net = total_net / len(cs)
        combo_stats[name] = {
            "count": len(cs), "win_rate": round(wr, 1),
            "avg_net": round(avg_net, 2), "total_net": round(total_net, 2),
        }
        print(f"  {name:>35}: {len(cs):>3} cycles | WR={wr:>5.1f}% | "
              f"avg=${avg_net:>10,.2f} | total=${total_net:>12,.2f}")

    # ════════════════════════════════════════════════════
    # 4. MOST PREDICTABLE UNDERLYINGS
    # ════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print("4. MOST PREDICTABLE UNDERLYINGS")
    print(f"{'='*60}")

    und_stats = {}
    by_und = defaultdict(list)
    for c in cycles:
        by_und[c["und"]].append(c)

    for und in sorted(by_und.keys()):
        cs = by_und[und]
        if len(cs) < 3:
            continue
        wins = [c for c in cs if c["is_winner"]]
        clean = [c for c in cs if c["is_clean"]]
        clean_wins = [c for c in cs if c["is_clean"] and c["is_winner"]]
        season = [c for c in cs if c["is_season"]]
        season_wins = [c for c in cs if c["is_season"] and c["is_winner"]]
        total_net = sum(c["net"] for c in cs)
        clean_net = sum(c["net"] for c in clean)
        avg_dur = sum(c["duration_days"] for c in cs) / len(cs)

        wr = len(wins) / len(cs) * 100
        clean_wr = len(clean_wins) / len(clean) * 100 if clean else 0
        season_wr = len(season_wins) / len(season) * 100 if season else 0

        # Predictability score = clean WR * consistency
        consistency = 1 - (max(abs(c["net"]) for c in cs) / (sum(abs(c["net"]) for c in cs) + 1))
        predictability = clean_wr * consistency / 100

        und_stats[und] = {
            "name": UNDERLYING_NAMES.get(und, und),
            "cycles": len(cs),
            "win_rate": round(wr, 1),
            "clean_pct": round(len(clean) / len(cs) * 100, 1),
            "clean_wr": round(clean_wr, 1),
            "season_wr": round(season_wr, 1) if season else None,
            "total_net": round(total_net, 2),
            "clean_net": round(clean_net, 2),
            "avg_duration": round(avg_dur, 1),
            "predictability": round(predictability, 3),
        }

    # Sort by predictability
    sorted_unds = sorted(und_stats.items(), key=lambda x: x[1]["predictability"], reverse=True)
    for und, s in sorted_unds:
        season_str = f"season_WR={s['season_wr']:.0f}%" if s["season_wr"] is not None else "no_season"
        print(f"  {und:>4} ({s['name']:>14}): {s['cycles']:>2} cycles | "
              f"WR={s['win_rate']:>5.1f}% | clean_WR={s['clean_wr']:>5.1f}% | "
              f"{season_str:>16} | "
              f"net=${s['total_net']:>12,.2f} | pred={s['predictability']:.3f}")

    # ════════════════════════════════════════════════════
    # 5. BEST ENTRY MONTHS
    # ════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print("5. BEST ENTRY MONTHS")
    print(f"{'='*60}")

    month_stats = {}
    by_month = defaultdict(list)
    for c in cycles:
        by_month[c["entry_month"]].append(c)

    for m in range(1, 13):
        cs = by_month[m]
        if not cs:
            continue
        wins = [c for c in cs if c["is_winner"]]
        clean = [c for c in cs if c["is_clean"]]
        clean_wins = [c for c in cs if c["is_clean"] and c["is_winner"]]
        total_net = sum(c["net"] for c in cs)
        clean_net = sum(c["net"] for c in clean)
        wr = len(wins) / len(cs) * 100
        clean_wr = len(clean_wins) / len(clean) * 100 if clean else 0
        clean_pct = len(clean) / len(cs) * 100

        month_stats[MONTH_NAMES[m]] = {
            "cycles": len(cs),
            "win_rate": round(wr, 1),
            "clean_pct": round(clean_pct, 1),
            "clean_wr": round(clean_wr, 1),
            "total_net": round(total_net, 2),
            "clean_net": round(clean_net, 2),
            "avg_grade": round(sum(c["score"] for c in cs) / len(cs), 1),
        }

    # Sort by clean WR
    sorted_months = sorted(month_stats.items(), key=lambda x: x[1]["clean_wr"], reverse=True)
    for month, s in sorted_months:
        print(f"  {month:>3}: {s['cycles']:>3} cycles | WR={s['win_rate']:>5.1f}% | "
              f"clean={s['clean_pct']:>5.1f}% | clean_WR={s['clean_wr']:>5.1f}% | "
              f"net=${s['total_net']:>12,.2f} | grade_avg={s['avg_grade']:.1f}")

    # ════════════════════════════════════════════════════
    # SAVE
    # ════════════════════════════════════════════════════
    output = {
        "generated_at": datetime.now().isoformat(),
        "total_cycles": len(cycles),
        "win_rate_by_rolls": roll_stats,
        "win_rate_by_grade": grade_stats,
        "indicator_combinations": combo_stats,
        "underlying_predictability": und_stats,
        "best_entry_months": month_stats,
        "key_findings": {
            "roll_impact": f"0 rolls: WR={roll_stats.get('0', {}).get('win_rate', 0)}% vs 1 roll: WR={roll_stats.get('1', {}).get('win_rate', 0)}%",
            "best_combo": max(combo_stats.items(), key=lambda x: x[1]["win_rate"])[0] if combo_stats else "N/A",
            "best_combo_wr": max(combo_stats.values(), key=lambda x: x["win_rate"])["win_rate"] if combo_stats else 0,
            "most_predictable": sorted_unds[0][0] if sorted_unds else "N/A",
            "best_month": sorted_months[0][0] if sorted_months else "N/A",
            "best_month_clean_wr": sorted_months[0][1]["clean_wr"] if sorted_months else 0,
        },
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n{'='*60}")
    print("KEY FINDINGS")
    print(f"{'='*60}")
    kf = output["key_findings"]
    print(f"  Roll impact: {kf['roll_impact']}")
    print(f"  Best combo: {kf['best_combo']} (WR={kf['best_combo_wr']}%)")
    print(f"  Most predictable: {kf['most_predictable']}")
    print(f"  Best month: {kf['best_month']} (clean_WR={kf['best_month_clean_wr']}%)")
    print(f"\n[SAVED] {OUTPUT_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
