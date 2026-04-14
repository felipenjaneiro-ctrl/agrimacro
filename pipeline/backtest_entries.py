#!/usr/bin/env python3
"""
AgriMacro — Cycle Analysis & Roll Detection

Detects trade cycles (open → close) per underlying+expiry,
counts rolls (close one expiry → open next in same underlying),
and analyzes which underlyings/months produce the cleanest trades.

A "cycle" = all trades in one underlying+expiry from first open to last close.
A "roll"  = closing one expiry and opening the next within ROLL_WINDOW days.

Output: pipeline/cycle_analysis.json

Run:
  python pipeline/backtest_entries.py
"""

import csv
import json
import sys
import os
from collections import defaultdict
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

BASE = Path(__file__).parent.parent
CSV_DIR = BASE / "agrimacro-dash" / "public" / "data" / "ibkr_history"
OUTPUT_PATH = BASE / "pipeline" / "cycle_analysis.json"
SKILL_PATH = BASE / "pipeline" / "trade_skill_base.json"

ROLL_WINDOW = 3  # days: if close exp A and open exp B within this window → roll

KNOWN_ROOTS = [
    "ZC", "ZS", "ZW", "ZM", "ZL", "KE",
    "LE", "GF", "HE",
    "SB", "KC", "CT", "CC", "OJ",
    "CL", "NG",
    "GC", "SI",
    "DX",
]

UNDERLYING_NAMES = {
    "CL": "Crude Oil", "NG": "Natural Gas", "GC": "Gold", "SI": "Silver",
    "ZS": "Soybeans", "ZC": "Corn", "ZW": "Wheat", "ZM": "Soy Meal",
    "ZL": "Soy Oil", "KC": "Coffee", "CC": "Cocoa", "SB": "Sugar",
    "CT": "Cotton", "LE": "Live Cattle", "GF": "Feeder Cattle",
    "HE": "Lean Hogs", "KE": "KC Wheat", "OJ": "Orange Juice",
}


def resolve_underlying(symbol):
    parts = symbol.strip().split()
    first = parts[0] if parts else symbol.strip()
    for root in sorted(KNOWN_ROOTS, key=len, reverse=True):
        if first == root or first.startswith(root):
            return root
    return first[:2] if len(first) >= 2 else first


def parse_date(s):
    for fmt in ("%Y-%m-%d, %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    return None


def load_commodity_trades():
    """Load all commodity trades from CSVs, return sorted by date."""
    all_trades = []
    csv_files = sorted(CSV_DIR.glob("*.csv"))

    for fpath in csv_files:
        with open(fpath, 'r', encoding='utf-8-sig') as f:
            content = f.read()

        lines = content.split('\n')
        header_cols = None
        col_idx = {}

        for line in lines:
            if not line.startswith('Trades,'):
                continue
            row = next(csv.reader(StringIO(line)))
            if len(row) < 3:
                continue

            if row[1] == "Header":
                header_cols = row
                col_idx = {col.strip(): j for j, col in enumerate(row)}
                continue

            if row[1] != "Data" or not header_cols:
                continue

            disc_j = col_idx.get("DataDiscriminator")
            if disc_j is not None and disc_j < len(row) and row[disc_j].strip() != "Order":
                continue

            cat_j = col_idx.get("Asset Category")
            cat = row[cat_j].strip() if cat_j is not None and cat_j < len(row) else ""
            if cat not in ("Options On Futures", "Futures"):
                continue

            sym_j = col_idx.get("Symbol")
            sym = row[sym_j].strip() if sym_j is not None and sym_j < len(row) else ""
            if not sym:
                continue

            dt_j = col_idx.get("Date/Time")
            dt_str = row[dt_j].strip() if dt_j is not None and dt_j < len(row) else ""
            dt = parse_date(dt_str)
            if not dt:
                continue

            qty_j = col_idx.get("Quantity")
            rpnl_j = col_idx.get("Realized P/L") or col_idx.get("Realized P&L")
            comm_j = col_idx.get("Comm/Fee")
            code_j = col_idx.get("Code")

            def pnum(j):
                try:
                    return float(row[j].strip().replace(",", "").replace('"', '')) if j is not None and j < len(row) else 0
                except ValueError:
                    return 0

            qty = pnum(qty_j)
            rpnl = pnum(rpnl_j)
            comm = pnum(comm_j)
            code = row[code_j].strip() if code_j is not None and code_j < len(row) else ""

            und = resolve_underlying(sym)
            parts = sym.split()
            exp = parts[1] if len(parts) >= 2 else sym[len(und):]
            right = parts[3] if len(parts) >= 4 else (parts[2] if len(parts) == 3 and parts[2] in ("C", "P") else None)

            is_open = "O" in code
            is_close = "C" in code or "Ep" in code

            all_trades.append({
                "und": und,
                "exp": exp,
                "sym": sym,
                "cat": cat,
                "dt": dt,
                "date": dt.strftime("%Y-%m-%d"),
                "qty": qty,
                "rpnl": rpnl,
                "comm": comm,
                "net": rpnl + comm,
                "code": code,
                "is_open": is_open,
                "is_close": is_close,
                "right": right,
            })

    all_trades.sort(key=lambda t: t["dt"])
    return all_trades


def detect_cycles(trades):
    """
    Group trades into cycles per underlying+expiry.
    A cycle = all trades sharing the same (und, exp).
    """
    by_und_exp = defaultdict(list)
    for t in trades:
        key = (t["und"], t["exp"])
        by_und_exp[key].append(t)

    cycles = []
    for (und, exp), ts in by_und_exp.items():
        ts_sorted = sorted(ts, key=lambda t: t["dt"])
        first_date = ts_sorted[0]["dt"]
        last_date = ts_sorted[-1]["dt"]
        duration = (last_date - first_date).days

        total_net = sum(t["net"] for t in ts_sorted)
        total_pnl = sum(t["rpnl"] for t in ts_sorted)
        total_comm = sum(t["comm"] for t in ts_sorted)
        trade_count = len(ts_sorted)

        # Structure: count calls/puts sold/bought
        cs = cb = ps = pb = fs = fb = 0
        is_fut = False
        for t in ts_sorted:
            if t["cat"] == "Futures":
                is_fut = True
                if t["qty"] < 0:
                    fs += abs(int(t["qty"]))
                else:
                    fb += int(t["qty"])
            elif t["right"] == "C":
                if t["qty"] < 0:
                    cs += abs(int(t["qty"]))
                else:
                    cb += int(t["qty"])
            elif t["right"] == "P":
                if t["qty"] < 0:
                    ps += abs(int(t["qty"]))
                else:
                    pb += int(t["qty"])

        if is_fut:
            structure = f"FUT SELL={fs} BUY={fb}"
        else:
            parts = []
            if cs or cb:
                parts.append(f"C: sell={cs} buy={cb}")
            if ps or pb:
                parts.append(f"P: sell={ps} buy={pb}")
            structure = " | ".join(parts) if parts else "?"

        cycles.append({
            "und": und,
            "exp": exp,
            "key": f"{und} {exp}",
            "entry_date": first_date.strftime("%Y-%m-%d"),
            "exit_date": last_date.strftime("%Y-%m-%d"),
            "entry_month": first_date.month,
            "entry_year": first_date.year,
            "duration_days": duration,
            "trades": trade_count,
            "net": round(total_net, 2),
            "pnl_gross": round(total_pnl, 2),
            "comm": round(total_comm, 2),
            "structure": structure,
            "is_futures": is_fut,
        })

    return cycles


def detect_rolls(cycles):
    """
    Detect rolls: when one cycle in an underlying closes and another opens
    within ROLL_WINDOW days.

    Returns cycles with roll_count annotated.
    """
    # Group cycles by underlying
    by_und = defaultdict(list)
    for c in cycles:
        by_und[c["und"]].append(c)

    # For each underlying, sort by entry_date and detect rolls
    for und, und_cycles in by_und.items():
        und_cycles.sort(key=lambda c: c["entry_date"])

        for c in und_cycles:
            c["rolls"] = 0
            c["rolled_from"] = []
            c["rolled_to"] = []

        # Compare every pair: if cycle A exit is near cycle B entry → roll
        for i, ca in enumerate(und_cycles):
            exit_a = datetime.strptime(ca["exit_date"], "%Y-%m-%d")
            for j, cb in enumerate(und_cycles):
                if i == j or ca["exp"] == cb["exp"]:
                    continue
                entry_b = datetime.strptime(cb["entry_date"], "%Y-%m-%d")

                gap = (entry_b - exit_a).days
                if 0 <= gap <= ROLL_WINDOW:
                    ca["rolled_to"].append(cb["key"])
                    cb["rolled_from"].append(ca["key"])
                    ca["rolls"] += 1

    return cycles


def main():
    print("=" * 60)
    print("CYCLE ANALYSIS & ROLL DETECTION")
    print("=" * 60)

    trades = load_commodity_trades()
    print(f"  Loaded {len(trades)} commodity trades")

    # Filter to known roots only
    trades = [t for t in trades if t["und"] in KNOWN_ROOTS]
    print(f"  Known underlyings: {len(trades)} trades")

    cycles = detect_cycles(trades)
    print(f"  Detected {len(cycles)} cycles (und+exp)")

    cycles = detect_rolls(cycles)
    rolled = [c for c in cycles if c["rolls"] > 0]
    print(f"  Rolls detected: {len(rolled)} cycles have rolls")

    # ── Aggregation ──
    by_und = defaultdict(list)
    for c in cycles:
        by_und[c["und"]].append(c)

    # Roll distribution
    roll_dist = {"0": 0, "1": 0, "2-3": 0, "4+": 0}
    for c in cycles:
        r = c["rolls"]
        if r == 0:
            roll_dist["0"] += 1
        elif r == 1:
            roll_dist["1"] += 1
        elif r <= 3:
            roll_dist["2-3"] += 1
        else:
            roll_dist["4+"] += 1

    # ── Per-underlying summary ──
    und_summary = {}
    for und, cs in sorted(by_und.items()):
        total = len(cs)
        no_roll = [c for c in cs if c["rolls"] == 0]
        one_roll = [c for c in cs if c["rolls"] == 1]
        multi_roll = [c for c in cs if c["rolls"] >= 2]
        total_net = sum(c["net"] for c in cs)
        avg_dur = sum(c["duration_days"] for c in cs) / total if total else 0
        clean_net = sum(c["net"] for c in no_roll)
        rolled_net = sum(c["net"] for c in cs if c["rolls"] > 0)

        und_summary[und] = {
            "name": UNDERLYING_NAMES.get(und, und),
            "total_cycles": total,
            "no_roll": len(no_roll),
            "one_roll": len(one_roll),
            "multi_roll": len(multi_roll),
            "total_net": round(total_net, 2),
            "clean_net": round(clean_net, 2),
            "rolled_net": round(rolled_net, 2),
            "avg_duration_days": round(avg_dur, 1),
            "clean_pct": round(len(no_roll) / total * 100, 1) if total else 0,
        }

    # ── Per-month analysis (entry month → no-roll success) ──
    month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    by_month = defaultdict(lambda: {"total": 0, "no_roll": 0, "net": 0, "clean_net": 0})
    for c in cycles:
        m = c["entry_month"]
        by_month[m]["total"] += 1
        by_month[m]["net"] += c["net"]
        if c["rolls"] == 0:
            by_month[m]["no_roll"] += 1
            by_month[m]["clean_net"] += c["net"]

    month_summary = {}
    for m in range(1, 13):
        d = by_month[m]
        month_summary[month_names[m]] = {
            "total_cycles": d["total"],
            "no_roll": d["no_roll"],
            "clean_pct": round(d["no_roll"] / d["total"] * 100, 1) if d["total"] else 0,
            "total_net": round(d["net"], 2),
            "clean_net": round(d["clean_net"], 2),
        }

    # ── Duration analysis by roll count ──
    dur_by_rolls = defaultdict(list)
    net_by_rolls = defaultdict(list)
    for c in cycles:
        bucket = "0" if c["rolls"] == 0 else "1" if c["rolls"] == 1 else "2-3" if c["rolls"] <= 3 else "4+"
        dur_by_rolls[bucket].append(c["duration_days"])
        net_by_rolls[bucket].append(c["net"])

    duration_analysis = {}
    for bucket in ["0", "1", "2-3", "4+"]:
        durs = dur_by_rolls.get(bucket, [])
        nets = net_by_rolls.get(bucket, [])
        if durs:
            duration_analysis[bucket] = {
                "count": len(durs),
                "avg_duration": round(sum(durs) / len(durs), 1),
                "avg_net": round(sum(nets) / len(nets), 2),
                "total_net": round(sum(nets), 2),
                "win_rate": round(len([n for n in nets if n > 0]) / len(nets) * 100, 1),
            }

    # ── Build output ──
    output = {
        "generated_at": datetime.now().isoformat(),
        "total_cycles": len(cycles),
        "total_trades": len(trades),
        "roll_distribution": roll_dist,
        "duration_by_rolls": duration_analysis,
        "by_underlying": und_summary,
        "by_entry_month": month_summary,
        "worst_rolled": sorted(
            [{"key": c["key"], "net": c["net"], "rolls": c["rolls"],
              "trades": c["trades"], "duration": c["duration_days"],
              "structure": c["structure"]}
             for c in cycles if c["rolls"] >= 2],
            key=lambda x: x["net"]
        )[:10],
        "best_clean": sorted(
            [{"key": c["key"], "net": c["net"], "trades": c["trades"],
              "duration": c["duration_days"], "structure": c["structure"],
              "entry": c["entry_date"]}
             for c in cycles if c["rolls"] == 0 and c["net"] > 0],
            key=lambda x: x["net"], reverse=True
        )[:10],
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)

    # ── Report ──
    print(f"\n{'='*60}")
    print(f"CYCLE ANALYSIS REPORT")
    print(f"{'='*60}")

    print(f"\n--- CYCLES PER UNDERLYING ---")
    for und in sorted(und_summary, key=lambda u: und_summary[u]["total_cycles"], reverse=True):
        s = und_summary[und]
        print(f"  {und:>4} ({s['name']:>14}): {s['total_cycles']:>3} ciclos | "
              f"clean={s['no_roll']:>2} ({s['clean_pct']:>5.1f}%) | "
              f"1roll={s['one_roll']:>2} | 2+roll={s['multi_roll']:>2} | "
              f"net=${s['total_net']:>12,.2f} | clean_net=${s['clean_net']:>12,.2f}")

    print(f"\n--- ROLL DISTRIBUTION ---")
    for bucket, count in roll_dist.items():
        pct = count / len(cycles) * 100 if cycles else 0
        da = duration_analysis.get(bucket, {})
        avg_d = da.get("avg_duration", 0)
        avg_n = da.get("avg_net", 0)
        wr = da.get("win_rate", 0)
        print(f"  {bucket:>4} rolls: {count:>3} ciclos ({pct:>5.1f}%) | "
              f"avg_dur={avg_d:>5.1f}d | avg_net=${avg_n:>10,.2f} | WR={wr:.0f}%")

    print(f"\n--- UNDERLYINGS WITH MOST CLEAN CYCLES (no roll) ---")
    clean_sorted = sorted(und_summary.items(), key=lambda x: x[1]["no_roll"], reverse=True)
    for und, s in clean_sorted[:8]:
        print(f"  {und:>4}: {s['no_roll']:>2} clean cycles | "
              f"clean_net=${s['clean_net']:>12,.2f} | "
              f"avg_dur={s['avg_duration_days']:.0f}d")

    print(f"\n--- BEST ENTRY MONTHS (most clean cycles) ---")
    month_sorted = sorted(month_summary.items(), key=lambda x: x[1]["no_roll"], reverse=True)
    for month, s in month_sorted:
        if s["total_cycles"] == 0:
            continue
        print(f"  {month:>3}: {s['no_roll']:>2} clean / {s['total_cycles']:>2} total ({s['clean_pct']:>5.1f}%) | "
              f"clean_net=${s['clean_net']:>12,.2f}")

    print(f"\n--- TOP 5 BEST CLEAN ENTRIES (no roll, profitable) ---")
    for i, c in enumerate(output["best_clean"][:5]):
        print(f"  {i+1}. {c['key']:>14} | ${c['net']:>12,.2f} | {c['trades']}t | "
              f"{c['duration']}d | {c['structure']} | entry={c['entry']}")

    print(f"\n--- TOP 5 WORST ROLLED ENTRIES (2+ rolls) ---")
    for i, c in enumerate(output["worst_rolled"][:5]):
        print(f"  {i+1}. {c['key']:>14} | ${c['net']:>12,.2f} | {c['rolls']} rolls | "
              f"{c['trades']}t | {c['duration']}d | {c['structure']}")

    print(f"\n[SAVED] {OUTPUT_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
