#!/usr/bin/env python3
"""
AgriMacro — Build Trade Skill Base from IBKR Statements

Reads IBKR activity statement CSVs from:
  agrimacro-dash/public/data/ibkr_history/*.csv

Generates:
  pipeline/trade_skill_base.json

IBKR CSV format (Trades section):
  Trades,Header,DataDiscriminator,Asset Category,Currency,[Account,]Symbol,
  Date/Time,Quantity,T. Price,C. Price,Proceeds,Comm/Fee,Basis,
  Realized P/L,MTM P/L,Code

Run:
  python pipeline/build_trade_skill.py

Re-run anytime to update with new statements.
"""

import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from io import StringIO

BASE = Path(__file__).parent.parent
CSV_DIR = BASE / "agrimacro-dash" / "public" / "data" / "ibkr_history"
OUTPUT_PATH = BASE / "pipeline" / "trade_skill_base.json"

# ── Underlying resolution ──────────────────────────────────────────────────

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
    "HE": "Lean Hogs", "DX": "Dollar Index", "KE": "KC Wheat",
    "OJ": "Orange Juice",
}


def resolve_underlying(symbol):
    """
    Map IBKR symbol to canonical underlying root.
    Handles:
      - FOP: "CC JAN26 5000 P" -> "CC"
      - FUT: "CCK5" -> "CC"
      - Simple: "CL" -> "CL"
    """
    sym = symbol.strip()

    # FOP format: "CC JAN26 5000 P" — take first token
    if " " in sym:
        first = sym.split()[0]
        # First token might be like "OZCN6" (option on ZC) — strip O prefix
        if first.startswith("O") and len(first) >= 3:
            candidate = first[1:3]
            if candidate in KNOWN_ROOTS:
                return candidate
        # Otherwise check 2-char prefix
        for root in KNOWN_ROOTS:
            if first == root or first.startswith(root):
                return root
        return first[:2]

    # Futures format: "CCK5", "ZSN6", "CLF25"
    for root in sorted(KNOWN_ROOTS, key=len, reverse=True):
        if sym.startswith(root):
            return root

    return sym[:2] if len(sym) >= 2 else sym


def parse_fop_symbol(symbol):
    """
    Parse FOP symbol into components.
    "CC JAN26 5000 P" -> {und: "CC", exp: "JAN26", strike: "5000", right: "P"}
    "CCK5" (futures) -> {und: "CC", exp: "K5", strike: None, right: None}
    """
    sym = symbol.strip()
    parts = sym.split()
    if len(parts) >= 4:
        return {
            "und": resolve_underlying(sym),
            "exp": parts[1],
            "strike": parts[2],
            "right": parts[3],  # "C" or "P"
        }
    elif len(parts) == 3:
        # Some formats: "CC JAN26 P" without strike?
        return {
            "und": resolve_underlying(sym),
            "exp": parts[1],
            "strike": parts[2] if parts[2][0].isdigit() else None,
            "right": parts[2] if parts[2] in ("C", "P") else None,
        }
    else:
        return {
            "und": resolve_underlying(sym),
            "exp": sym[len(resolve_underlying(sym)):] if len(sym) > 2 else "",
            "strike": None,
            "right": None,
        }


def spread_key(symbol):
    """
    Group key for spread = underlying + expiry.
    "CC JAN26 5000 P" -> "CC JAN26"
    "CC JAN26 5200 P" -> "CC JAN26"
    "CCK5" -> "CC K5"
    """
    p = parse_fop_symbol(symbol)
    return f"{p['und']} {p['exp']}"


def resolve_contract(symbol):
    """
    Extract contract code for compact display.
    "CC JAN26 5000 P" -> "CCF6" (JAN=F, 26=6)
    "CCK5" -> "CCK5"
    """
    sym = symbol.strip()

    if " " in sym:
        parts = sym.split()
        root = resolve_underlying(sym)
        month_str = parts[1] if len(parts) >= 2 else ""
        month_codes = {
            "JAN": "F", "FEB": "G", "MAR": "H", "APR": "J", "MAY": "K",
            "JUN": "M", "JUL": "N", "AUG": "Q", "SEP": "U", "OCT": "V",
            "NOV": "X", "DEC": "Z",
        }
        mc = month_codes.get(month_str[:3].upper(), "?")
        yr = month_str[-1] if month_str and month_str[-1].isdigit() else "?"
        return f"{root}{mc}{yr}"

    # Futures: already compact like "CCK5"
    return sym


# ── CSV Parser ──────────────────────────────────────────────────────────────

def parse_ibkr_csv(filepath):
    """
    Parse an IBKR annual activity statement CSV.
    Looks for 'Trades,Header,...' rows, reads column names,
    then reads 'Trades,Data,Order,...' rows.
    """
    trades = []

    with open(filepath, 'r', encoding='utf-8-sig') as f:
        content = f.read()

    lines = content.split('\n')

    # Phase 1: find ALL Trades Header rows and build column maps
    # (may differ between asset categories or if Account column is present)
    header_cols = None
    col_idx = {}

    for i, line in enumerate(lines):
        if not line.startswith('Trades,'):
            continue

        # Use csv reader for proper quote handling
        row = next(csv.reader(StringIO(line)))

        if len(row) < 3:
            continue

        section = row[0]   # "Trades"
        rowtype = row[1]   # "Header", "Data", "SubTotal", "Total"

        if rowtype == "Header":
            header_cols = row
            col_idx = {}
            for j, col in enumerate(row):
                col_idx[col.strip()] = j
            continue

        if rowtype != "Data" or not header_cols:
            continue

        # DataDiscriminator
        disc_j = col_idx.get("DataDiscriminator")
        if disc_j is not None and disc_j < len(row):
            disc = row[disc_j].strip()
            if disc != "Order":
                continue  # skip SubTotal, Total, etc.

        # Asset Category
        cat_j = col_idx.get("Asset Category")
        cat = row[cat_j].strip() if cat_j is not None and cat_j < len(row) else ""

        # We want: Options On Futures, Futures, Equity and Index Options, Stocks
        # (include all for complete P&L picture)

        # Symbol
        sym_j = col_idx.get("Symbol")
        if sym_j is None:
            continue
        symbol = row[sym_j].strip() if sym_j < len(row) else ""
        if not symbol:
            continue

        # Date/Time
        dt_j = col_idx.get("Date/Time")
        dt_str = row[dt_j].strip() if dt_j is not None and dt_j < len(row) else ""

        # Realized P/L
        rpnl_j = col_idx.get("Realized P/L")
        if rpnl_j is None:
            # Try alternate names
            rpnl_j = col_idx.get("Realized P&L")
        rpnl_str = row[rpnl_j].strip() if rpnl_j is not None and rpnl_j < len(row) else "0"

        # Comm/Fee
        comm_j = col_idx.get("Comm/Fee")
        comm_str = row[comm_j].strip() if comm_j is not None and comm_j < len(row) else "0"

        # Proceeds
        proc_j = col_idx.get("Proceeds")
        proc_str = row[proc_j].strip() if proc_j is not None and proc_j < len(row) else "0"

        # Quantity
        qty_j = col_idx.get("Quantity")
        qty_str = row[qty_j].strip() if qty_j is not None and qty_j < len(row) else "0"

        # Parse numbers
        def parse_num(s):
            try:
                return float(s.replace(",", "").replace('"', '')) if s else 0
            except ValueError:
                return 0

        rpnl = parse_num(rpnl_str)
        comm = parse_num(comm_str)
        proc = parse_num(proc_str)
        qty = parse_num(qty_str)

        # Parse date
        trade_date = None
        for fmt in ("%Y-%m-%d, %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d", "%m/%d/%Y"):
            try:
                trade_date = datetime.strptime(dt_str.strip(), fmt)
                break
            except ValueError:
                continue

        underlying = resolve_underlying(symbol)
        contract = resolve_contract(symbol)
        fop = parse_fop_symbol(symbol)
        skey = spread_key(symbol)
        year = str(trade_date.year) if trade_date else "unknown"

        trades.append({
            "symbol": symbol,
            "underlying": underlying,
            "contract": contract,
            "spread_key": skey,
            "right": fop.get("right"),  # "C" or "P" or None
            "asset_category": cat,
            "date": trade_date.strftime("%Y-%m-%d") if trade_date else dt_str,
            "year": year,
            "quantity": qty,
            "proceeds": proc,
            "realized_pnl": rpnl,
            "commission": comm,
            "net_pnl": rpnl + comm,  # comm is already negative
        })

    return trades


# ── Load all CSVs ───────────────────────────────────────────────────────────

def load_all_trades():
    """Load all trades from all CSVs in the history directory."""
    if not CSV_DIR.exists():
        print(f"[WARN] CSV directory not found: {CSV_DIR}")
        return []

    csv_files = sorted(CSV_DIR.glob("*.csv"))
    if not csv_files:
        print(f"[WARN] No CSV files found in {CSV_DIR}")
        return []

    all_trades = []
    for f in csv_files:
        print(f"  Parsing {f.name}...")
        trades = parse_ibkr_csv(f)
        # Count by category
        cats = defaultdict(int)
        for t in trades:
            cats[t["asset_category"]] += 1
        cat_str = ", ".join(f"{c}:{n}" for c, n in sorted(cats.items()))
        print(f"    -> {len(trades)} trades ({cat_str})")
        all_trades.extend(trades)

    # Dedupe by (symbol, date, realized_pnl, quantity)
    seen = set()
    unique = []
    for t in all_trades:
        key = (t["symbol"], t["date"], t["realized_pnl"], t["quantity"])
        if key not in seen:
            seen.add(key)
            unique.append(t)

    print(f"\n  Total: {len(unique)} unique trades from {len(csv_files)} files")
    return unique


# ── Aggregation ─────────────────────────────────────────────────────────────

def build_skill_data(all_trades):
    """Aggregate trades into the skill base structure."""

    # Filter to commodity trades only (Futures + Options On Futures)
    commodity_cats = {"Options On Futures", "Futures"}
    trades = [t for t in all_trades if t["asset_category"] in commodity_cats]
    other = len(all_trades) - len(trades)
    print(f"\n  Commodity trades: {len(trades)} (excluded {other} stocks/equity/bonds/forex)")

    # Per-underlying aggregation
    by_underlying = defaultdict(lambda: {
        "trades": 0, "winners": 0, "losers": 0,
        "gross_pnl": 0, "commission": 0, "net_pnl": 0,
        "biggest_win": 0, "biggest_loss": 0,
    })

    by_year = defaultdict(lambda: {"count": 0, "net": 0, "winners": 0, "losers": 0})

    # Per-spread for worst positions (grouped by underlying + expiry)
    by_spread = defaultdict(lambda: {
        "trades": 0, "net": 0, "pnl": 0, "comm": 0,
        "year": "", "underlying": "",
        "calls_sold": 0, "calls_bought": 0,
        "puts_sold": 0, "puts_bought": 0,
    })

    total_trades = 0
    total_net = 0
    total_comm = 0
    total_winners = 0
    total_losers = 0

    for t in trades:
        und = t["underlying"]
        net = t["net_pnl"]
        rpnl = t["realized_pnl"]
        comm = t["commission"]
        year = t["year"]
        contract = t["contract"]

        by_underlying[und]["trades"] += 1
        by_underlying[und]["gross_pnl"] += rpnl
        by_underlying[und]["commission"] += comm
        by_underlying[und]["net_pnl"] += net
        if net > 0:
            by_underlying[und]["winners"] += 1
        elif net < 0:
            by_underlying[und]["losers"] += 1
        by_underlying[und]["biggest_win"] = max(by_underlying[und]["biggest_win"], net)
        by_underlying[und]["biggest_loss"] = min(by_underlying[und]["biggest_loss"], net)

        by_year[year]["count"] += 1
        by_year[year]["net"] += net
        if net > 0:
            by_year[year]["winners"] += 1
        elif net < 0:
            by_year[year]["losers"] += 1

        skey = t["spread_key"]
        by_spread[skey]["trades"] += 1
        by_spread[skey]["net"] += net
        by_spread[skey]["pnl"] += rpnl
        by_spread[skey]["comm"] += comm
        if not by_spread[skey]["year"] or year > by_spread[skey]["year"]:
            by_spread[skey]["year"] = year
        by_spread[skey]["underlying"] = und

        # Track structure: calls/puts sold/bought
        right = t.get("right")
        qty = t["quantity"]
        cat = t["asset_category"]
        if cat == "Futures":
            by_spread[skey]["is_futures"] = True
            if qty < 0:
                by_spread[skey]["fut_sold"] = by_spread[skey].get("fut_sold", 0) + abs(int(qty))
            elif qty > 0:
                by_spread[skey]["fut_bought"] = by_spread[skey].get("fut_bought", 0) + int(qty)
        elif right == "C":
            if qty < 0:
                by_spread[skey]["calls_sold"] += abs(int(qty))
            elif qty > 0:
                by_spread[skey]["calls_bought"] += int(qty)
        elif right == "P":
            if qty < 0:
                by_spread[skey]["puts_sold"] += abs(int(qty))
            elif qty > 0:
                by_spread[skey]["puts_bought"] += int(qty)

        total_trades += 1
        total_net += net
        total_comm += comm
        if net > 0:
            total_winners += 1
        elif net < 0:
            total_losers += 1

    # ── Best underlyings (sorted by net P&L) ──
    best = []
    for und, d in sorted(by_underlying.items(), key=lambda x: x[1]["net_pnl"], reverse=True):
        total = d["winners"] + d["losers"]
        wr = d["winners"] / total if total > 0 else 0
        best.append({
            "sym": und,
            "name": UNDERLYING_NAMES.get(und, und),
            "net_total": round(d["net_pnl"], 2),
            "trades": d["trades"],
            "winners": d["winners"],
            "losers": d["losers"],
            "win_rate": round(wr, 3),
            "gross_pnl": round(d["gross_pnl"], 2),
            "commission": round(d["commission"], 2),
            "biggest_win": round(d["biggest_win"], 2),
            "biggest_loss": round(d["biggest_loss"], 2),
        })

    # ── Worst positions (spreads with biggest losses, grouped by und+expiry) ──
    worst = []
    for skey, data in sorted(by_spread.items(), key=lambda x: x[1]["net"]):
        if data["net"] < -5000:
            und = data["underlying"]
            exp = skey.replace(und + " ", "") if skey.startswith(und) else skey
            # Build structure string
            if data.get("is_futures"):
                fs = data.get("fut_sold", 0)
                fb = data.get("fut_bought", 0)
                structure = f"FUT SELL={fs} BUY={fb}"
                # Append options if mixed
                if data["calls_sold"] or data["calls_bought"] or data["puts_sold"] or data["puts_bought"]:
                    structure += (
                        f" + SELL C={data['calls_sold']} BUY C={data['calls_bought']} | "
                        f"SELL P={data['puts_sold']} BUY P={data['puts_bought']}"
                    )
            else:
                structure = (
                    f"SELL C={data['calls_sold']} BUY C={data['calls_bought']} | "
                    f"SELL P={data['puts_sold']} BUY P={data['puts_bought']}"
                )

            worst.append({
                "sym": skey,
                "net_total": round(data["net"], 2),
                "pnl_gross": round(data["pnl"], 2),
                "commissions": round(data["comm"], 2),
                "trades": data["trades"],
                "year": data["year"],
                "structure": structure,
                "lesson": f"Spread {und} {exp}: net ${round(data['net'], 0):,.0f} com {data['trades']} trades",
            })
    worst = worst[:15]

    # ── Load existing skill base to preserve manual content ──
    existing = {}
    if OUTPUT_PATH.exists():
        try:
            existing = json.load(open(OUTPUT_PATH))
        except Exception:
            pass

    # Merge lessons from existing worst_positions (preserve manual annotations)
    existing_worst = {w["sym"]: w for w in existing.get("worst_positions", [])}
    for w in worst:
        if w["sym"] in existing_worst:
            ew = existing_worst[w["sym"]]
            if ew.get("lesson") and not ew["lesson"].startswith("Spread "):
                w["lesson"] = ew["lesson"]

    win_rate = total_winners / total_trades if total_trades > 0 else 0

    skill_data = {
        "version": "2.0",
        "updated_at": datetime.now().strftime("%Y-%m-%d"),
        "description": "Base de conhecimento do trader Felipe. Gerado automaticamente a partir de statements IBKR reais. Lessons e rules sao preservados entre atualizacoes.",
        "data_source": f"{len(list(CSV_DIR.glob('*.csv')))} CSVs de {CSV_DIR.name}/",

        "trader_profile": existing.get("trader_profile", {
            "name": "Felipe",
            "instrument_preference": "FOP (futures options on commodities)",
            "style": "premium seller — vende vol, coleta theta",
            "avg_dte_entry": "45-90 dias",
            "preferred_delta": "0.15-0.30 (OTM)",
            "max_positions_simultaneous": 12,
            "risk_per_trade_pct": 2.5,
            "broker": "IBKR TWS",
            "session": "Pre-market scan 6:30 AM ET, execucao 9:00-10:30 AM ET",
            "edge": "Sazonalidade + COT extremos + IV rank alto = entrada ideal para venda de premium",
        }),

        "historical_performance": {
            "period": f"{min(by_year.keys(), default='?')} a {max(by_year.keys(), default='?')}",
            "total_trades": total_trades,
            "winners": total_winners,
            "losers": total_losers,
            "win_rate": round(win_rate, 3),
            "total_pnl_net": round(total_net, 2),
            "total_commissions": round(abs(total_comm), 2),
            "by_year": {
                year: {
                    "trades": d["count"],
                    "net": round(d["net"], 2),
                    "winners": d["winners"],
                    "losers": d["losers"],
                }
                for year, d in sorted(by_year.items())
            },
        },

        "best_underlyings": best,

        "worst_positions": worst,
        "methodology_note": "worst_positions agrupado por underlying+vencimento (spread completo, nao perna individual). net_total = soma de todas as opcoes abertas e fechadas no mesmo contrato.",

        # Preserve manual sections from existing file
        "validated_rules": existing.get("validated_rules", []),
        "seasonality_edge": existing.get("seasonality_edge", {}),
        "entry_scoring": existing.get("entry_scoring", {}),

        "hard_stops": existing.get("hard_stops", [
            "Futuros direcionais sem estrutura de opcoes",
            "Rolagem sem credito liquido",
            "Ignorar estrutura da curva de futuros",
        ]),
    }

    return skill_data


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("BUILD TRADE SKILL BASE — from IBKR Statements")
    print("=" * 60)

    CSV_DIR.mkdir(parents=True, exist_ok=True)

    trades = load_all_trades()

    if not trades:
        print("\n[INFO] No trades found. Place IBKR activity statement CSVs in:")
        print(f"  {CSV_DIR}")
        if OUTPUT_PATH.exists():
            d = json.load(open(OUTPUT_PATH))
            total = d.get("historical_performance", {}).get("total_trades", 0)
            net = d.get("historical_performance", {}).get("total_pnl_net", 0)
            print(f"\n  Existing: {total} trades, net ${net:,.2f}")
        return

    skill_data = build_skill_data(trades)

    OUTPUT_PATH.write_text(json.dumps(skill_data, indent=2, default=str))
    print(f"\n[SAVED] {OUTPUT_PATH}")

    # ── Summary ──
    perf = skill_data["historical_performance"]
    by_year = perf.get("by_year", {})

    print(f"\n{'='*60}")
    print(f"TOTAL: {perf['total_trades']} trades | Net: ${perf['total_pnl_net']:,.2f} | "
          f"WR: {perf['win_rate']*100:.1f}%")
    print(f"Comissoes: ${perf['total_commissions']:,.2f}")

    print(f"\nPor ano:")
    for year, d in sorted(by_year.items()):
        icon = "+" if d["net"] > 0 else "-"
        wr = d["winners"] / (d["winners"] + d["losers"]) * 100 if (d["winners"] + d["losers"]) > 0 else 0
        print(f"  {icon} {year}: ${d['net']:>12,.2f} ({d['trades']} trades, "
              f"{d['winners']}W/{d['losers']}L, WR={wr:.0f}%)")

    print(f"\nBest underlyings:")
    for u in skill_data["best_underlyings"][:10]:
        print(f"  {u['sym']:>4}: ${u['net_total']:>12,.2f} | {u['trades']:>4} trades | "
              f"WR={u['win_rate']*100:.0f}%")

    print(f"\nWorst positions (spread-level):")
    for u in skill_data["worst_positions"][:8]:
        print(f"  {u['sym']:>12}: ${u['net_total']:>12,.2f} | {u['trades']:>3} trades | {u['year']} | {u['structure']}")

    print(f"{'='*60}")


if __name__ == "__main__":
    main()
