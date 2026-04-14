#!/usr/bin/env python3
"""
AgriMacro — Trade Journal

Records every trade with full market context at time of entry.
Captures: price, IV, COT, seasonality, forward curve, VIX, portfolio state.

Usage:
  python pipeline/skill_trade_journal.py open SI PUT         # Record new trade
  python pipeline/skill_trade_journal.py open ZW CALL        # Record new trade
  python pipeline/skill_trade_journal.py close SI PUT        # Close existing trade
  python pipeline/skill_trade_journal.py show                # Show all open trades
  python pipeline/skill_trade_journal.py stats               # Performance stats
  python pipeline/skill_trade_journal.py show SI             # Show SI trades

Called programmatically from ibkr_orders.py after placeOrder():
  from skill_trade_journal import open_trade
  open_trade(underlying="SI", direction="PUT", legs=[], tese="...", dte_at_open=42)
"""

import json
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent.parent
PROC = BASE / "agrimacro-dash" / "public" / "data" / "processed"
JOURNAL_PATH = BASE / "pipeline" / "trade_journal.json"

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


def jload(path):
    for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            with open(path, encoding=enc) as f:
                return json.load(f)
        except Exception:
            continue
    return {}


def load_journal():
    if JOURNAL_PATH.exists():
        return json.loads(JOURNAL_PATH.read_text())
    return {"trades": [], "version": "1.0"}


def save_journal(journal):
    JOURNAL_PATH.write_text(json.dumps(journal, indent=2, default=str))


def capture_context(sym):
    """
    Capture full market context for a trade at this moment.
    Returns dict with all available data, [WARN] for missing fields.
    """
    options = jload(PROC / "options_chain.json")
    cot_data = jload(PROC / "cot.json")
    seasonality = jload(PROC / "seasonality.json")
    contract_hist = jload(PROC / "contract_history.json")
    prices = jload(PROC / "price_history.json")
    macro = jload(PROC / "macro_indicators.json")
    portfolio = jload(PROC / "ibkr_portfolio.json")
    spreads = jload(PROC / "spreads.json")

    ctx = {}
    warns = []

    # ── Price ──
    bars = prices.get(sym, [])
    if isinstance(bars, dict):
        bars = bars.get("history", [])
    if bars:
        last = bars[-1]
        ctx["price"] = last.get("close")
        ctx["price_date"] = last.get("date")
        if len(bars) >= 6:
            ctx["momentum_5d"] = round(((bars[-1]["close"] - bars[-6]["close"]) / bars[-6]["close"]) * 100, 2)
        if len(bars) >= 21:
            ctx["momentum_20d"] = round(((bars[-1]["close"] - bars[-21]["close"]) / bars[-21]["close"]) * 100, 2)
    else:
        ctx["price"] = None
        warns.append("[WARN] price_history nao disponivel para " + sym)

    # ── IV / Options ──
    und = options.get("underlyings", {}).get(sym, {})
    ivr = und.get("iv_rank", {})
    ctx["iv"] = ivr.get("current_iv")
    ctx["iv_rank_52w"] = ivr.get("rank_52w")
    ctx["iv_high_52w"] = ivr.get("iv_high_52w")
    ctx["iv_low_52w"] = ivr.get("iv_low_52w")
    ctx["iv_history_days"] = ivr.get("history_days", 0)

    skew = und.get("skew", {})
    ctx["skew_pct"] = skew.get("skew_pct")
    ctx["put_25d_iv"] = skew.get("put_25d_iv")
    ctx["call_25d_iv"] = skew.get("call_25d_iv")

    term = und.get("term_structure", {})
    ctx["term_structure"] = term.get("structure")
    ctx["term_points"] = term.get("points", [])

    if ctx["iv"] is None:
        warns.append("[WARN] IV nao disponivel — options chain sem dados para " + sym)
    if ctx["iv_rank_52w"] is None:
        warns.append("[WARN] IV Rank 52w nao disponivel — historico em construcao (" + str(ctx["iv_history_days"]) + " dias)")
    if ctx["skew_pct"] is None:
        warns.append("[WARN] Skew nao disponivel — sem 25-delta puts/calls para " + sym)

    # ── COT ──
    dis = cot_data.get("commodities", {}).get(sym, {}).get("disaggregated", {})
    ctx["cot_index"] = dis.get("cot_index")
    ctx["cot_index_52w"] = dis.get("cot_index_52w")
    ctx["cot_index_26w"] = dis.get("cot_index_26w")

    if ctx["cot_index"] is None:
        leg = cot_data.get("commodities", {}).get(sym, {}).get("legacy", {})
        if leg.get("latest"):
            ctx["cot_legacy_net"] = leg["latest"].get("noncomm_net")
        warns.append("[WARN] COT disaggregated nao disponivel para " + sym)

    # ── Seasonality ──
    cur_month = datetime.now().month
    seas = seasonality.get(sym, {}).get("monthly_returns", [])
    if seas and cur_month <= len(seas):
        mr = seas[cur_month - 1]
        ctx["seasonality_month"] = datetime.now().strftime("%b")
        ctx["seasonality_avg"] = mr if isinstance(mr, (int, float)) else mr.get("avg", 0) if isinstance(mr, dict) else 0
        ctx["seasonality_positive_pct"] = mr.get("positive_pct") if isinstance(mr, dict) else None
    else:
        ctx["seasonality_month"] = datetime.now().strftime("%b")
        ctx["seasonality_avg"] = None
        warns.append("[WARN] Sazonalidade nao disponivel para " + sym)

    # ── Forward Curve ──
    contracts = contract_hist.get("contracts", {})
    items = [(n, c["bars"][-1]["close"]) for n, c in contracts.items()
             if c.get("commodity") == sym and c.get("bars") and c["bars"][-1].get("close", 0) > 0]
    if len(items) >= 2:
        items.sort()
        diff = ((items[-1][1] - items[0][1]) / items[0][1]) * 100
        if diff < -3:
            shape = "STRONG_BACKWARDATION"
        elif diff < -1:
            shape = "MILD_BACKWARDATION"
        elif diff > 3:
            shape = "CONTANGO"
        else:
            shape = "FLAT"
        ctx["fwd_curve_shape"] = shape
        ctx["fwd_curve_diff_pct"] = round(diff, 1)
        ctx["fwd_curve_front"] = {"contract": items[0][0], "price": items[0][1]}
        ctx["fwd_curve_back"] = {"contract": items[-1][0], "price": items[-1][1]}
    else:
        ctx["fwd_curve_shape"] = None
        warns.append("[WARN] Forward curve nao disponivel — poucos contratos para " + sym)

    # ── Macro ──
    vix = macro.get("vix", {})
    ctx["vix"] = vix.get("value")
    ctx["vix_level"] = vix.get("level")
    sp = macro.get("sp500", {})
    ctx["sp500"] = sp.get("value")
    t10 = macro.get("treasury_10y", {})
    ctx["treasury_10y"] = t10.get("value")

    if ctx["vix"] is None:
        warns.append("[WARN] VIX nao disponivel")

    # ── Portfolio Snapshot ──
    summ = portfolio.get("summary", {})
    ctx["net_liq"] = float(summ.get("NetLiquidation", 0))
    ctx["buying_power"] = float(summ.get("BuyingPower", 0))
    ctx["cash"] = float(summ.get("TotalCashValue", 0))
    ctx["portfolio_generated"] = portfolio.get("generated_at", "?")

    # Active positions count
    active = set()
    for p in portfolio.get("positions", []):
        if p.get("sec_type") in ("FOP", "FUT") and p.get("position", 0) != 0:
            active.add(p["symbol"])
    ctx["active_positions"] = sorted(active)
    ctx["active_count"] = len(active)

    # ── Spreads involving this sym ──
    spread_hints = {
        "ZC": ["zc_zm", "zc_zs"], "ZS": ["soy_crush", "zc_zs"],
        "ZW": ["ke_zw", "feed_wheat"], "KE": ["ke_zw"],
        "ZL": ["soy_crush", "zl_cl"], "ZM": ["soy_crush", "zc_zm"],
        "LE": ["feedlot", "cattle_crush"], "GF": ["feedlot"],
        "CL": ["zl_cl"], "SB": [], "KC": [], "CC": [],
    }
    relevant = spread_hints.get(sym, [])
    ctx["spreads"] = {}
    for sp_name in relevant:
        sp = spreads.get("spreads", {}).get(sp_name, {})
        if sp.get("zscore_1y") is not None:
            ctx["spreads"][sp_name] = {
                "zscore": sp["zscore_1y"],
                "regime": sp.get("regime"),
                "current": sp.get("current"),
            }

    ctx["warnings"] = warns
    ctx["warnings_count"] = len(warns)
    ctx["captured_at"] = datetime.now().isoformat()

    return ctx


def open_trade(underlying, direction, legs=None, tese="", dte_at_open=None):
    """Record a new trade with full context snapshot."""
    journal = load_journal()

    ctx = capture_context(underlying)

    trade_id = f"{underlying}_{direction}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    entry = {
        "id": trade_id,
        "underlying": underlying,
        "name": NAMES.get(underlying, underlying),
        "sector": SECTORS.get(underlying, "?"),
        "direction": direction,
        "status": "OPEN",
        "opened_at": datetime.now().isoformat(),
        "closed_at": None,
        "dte_at_open": dte_at_open,
        "tese": tese,
        "legs": legs or [],
        "context_at_open": ctx,
        "context_at_close": None,
        "pnl": None,
        "notes": [],
    }

    journal["trades"].append(entry)
    save_journal(journal)

    print(f"\n  TRADE REGISTRADO: {trade_id}")
    print(f"  {underlying} ({NAMES.get(underlying,'')}) {direction}")
    print(f"  Contexto capturado: {len(ctx) - 2} campos ({ctx['warnings_count']} warnings)")

    if ctx["warnings"]:
        print(f"\n  Warnings:")
        for w in ctx["warnings"]:
            print(f"    {w}")

    return entry


def close_trade(underlying, direction, pnl=None, notes=""):
    """Close an open trade and capture exit context."""
    journal = load_journal()

    # Find most recent open trade matching
    found = None
    for t in reversed(journal["trades"]):
        if (t["underlying"] == underlying and t["direction"] == direction
                and t["status"] == "OPEN"):
            found = t
            break

    if not found:
        print(f"  [ERR] Nenhum trade OPEN encontrado para {underlying} {direction}")
        return None

    ctx = capture_context(underlying)
    found["status"] = "CLOSED"
    found["closed_at"] = datetime.now().isoformat()
    found["context_at_close"] = ctx
    found["pnl"] = pnl
    if notes:
        found["notes"].append(notes)

    # Calculate context deltas
    open_ctx = found.get("context_at_open", {})
    if open_ctx.get("price") and ctx.get("price"):
        price_change = ((ctx["price"] - open_ctx["price"]) / open_ctx["price"]) * 100
        found["price_change_pct"] = round(price_change, 2)
    if open_ctx.get("iv") and ctx.get("iv"):
        iv_change = (ctx["iv"] - open_ctx["iv"]) * 100
        found["iv_change_pct"] = round(iv_change, 1)

    save_journal(journal)
    print(f"\n  TRADE FECHADO: {found['id']}")
    print(f"  P&L: ${pnl:,.2f}" if pnl is not None else "  P&L: nao informado")
    return found


def show_trades(filter_sym=None):
    """Show all trades (or filtered by underlying)."""
    journal = load_journal()

    trades = journal.get("trades", [])
    if filter_sym:
        trades = [t for t in trades if t["underlying"] == filter_sym.upper()]

    if not trades:
        print(f"\n  Nenhum trade no journal{' para ' + filter_sym if filter_sym else ''}.")
        return

    open_trades = [t for t in trades if t["status"] == "OPEN"]
    closed_trades = [t for t in trades if t["status"] == "CLOSED"]

    if open_trades:
        print(f"\n  {'='*60}")
        print(f"  TRADES ABERTOS ({len(open_trades)})")
        print(f"  {'='*60}")
        for t in open_trades:
            ctx = t.get("context_at_open", {})
            iv_str = f"IV={ctx['iv']*100:.0f}%" if ctx.get("iv") else "IV=?"
            price_str = f"${ctx['price']:.2f}" if ctx.get("price") else "$?"
            cot_str = f"COT={ctx['cot_index']:.0f}" if ctx.get("cot_index") else "COT=?"
            warns = ctx.get("warnings_count", 0)

            print(f"\n  {t['id']}")
            print(f"    {t['underlying']} ({t['name']}) {t['direction']} | DTE={t.get('dte_at_open','?')}")
            print(f"    Opened: {t['opened_at'][:16]}")
            print(f"    Context: price={price_str} | {iv_str} | {cot_str} | "
                  f"VIX={ctx.get('vix','?')} | curve={ctx.get('fwd_curve_shape','?')}")
            if t.get("tese"):
                print(f"    Tese: {t['tese']}")
            if warns:
                print(f"    [{warns} warning(s) no contexto]")

    if closed_trades:
        print(f"\n  {'='*60}")
        print(f"  TRADES FECHADOS ({len(closed_trades)})")
        print(f"  {'='*60}")
        for t in closed_trades[-5:]:  # last 5
            pnl_str = f"${t['pnl']:,.2f}" if t.get("pnl") is not None else "N/A"
            print(f"  {t['id']} | P&L={pnl_str} | {t.get('opened_at','?')[:10]} -> {t.get('closed_at','?')[:10]}")


def show_stats():
    """Show journal statistics."""
    journal = load_journal()
    trades = journal.get("trades", [])

    if not trades:
        print("\n  Journal vazio.")
        return

    open_t = [t for t in trades if t["status"] == "OPEN"]
    closed_t = [t for t in trades if t["status"] == "CLOSED"]
    with_pnl = [t for t in closed_t if t.get("pnl") is not None]

    print(f"\n  {'='*60}")
    print(f"  JOURNAL STATS")
    print(f"  {'='*60}")
    print(f"  Total trades:  {len(trades)}")
    print(f"  Open:          {len(open_t)}")
    print(f"  Closed:        {len(closed_t)}")

    if with_pnl:
        total_pnl = sum(t["pnl"] for t in with_pnl)
        winners = [t for t in with_pnl if t["pnl"] > 0]
        losers = [t for t in with_pnl if t["pnl"] < 0]
        wr = len(winners) / len(with_pnl) * 100 if with_pnl else 0
        print(f"  With P&L:      {len(with_pnl)}")
        print(f"  Total P&L:     ${total_pnl:,.2f}")
        print(f"  Win Rate:      {wr:.0f}%")
        if winners:
            print(f"  Avg Winner:    ${sum(t['pnl'] for t in winners)/len(winners):,.2f}")
        if losers:
            print(f"  Avg Loser:     ${sum(t['pnl'] for t in losers)/len(losers):,.2f}")

    # Context quality stats
    all_warns = sum(t.get("context_at_open", {}).get("warnings_count", 0) for t in trades)
    all_fields = len(trades) * 20  # ~20 context fields per trade
    print(f"\n  Context warnings: {all_warns} across {len(trades)} trades")

    # By underlying
    from collections import Counter
    und_counts = Counter(t["underlying"] for t in trades)
    print(f"\n  By underlying:")
    for und, count in und_counts.most_common():
        print(f"    {und}: {count} trades")


def main():
    print("=" * 60)
    print("TRADE JOURNAL")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    args = sys.argv[1:]

    if not args:
        show_trades()
        return

    cmd = args[0].lower()

    if cmd == "open" and len(args) >= 3:
        sym = args[1].upper()
        direction = args[2].upper()
        tese = " ".join(args[3:]) if len(args) > 3 else ""
        entry = open_trade(sym, direction, tese=tese)

        # Print captured context detail
        if entry:
            ctx = entry["context_at_open"]
            print(f"\n  CONTEXTO CAPTURADO:")
            print(f"    Preco:         ${ctx['price']:.2f}" if ctx.get("price") else "    Preco:         [WARN] ausente")
            print(f"    Momentum 5d:   {ctx.get('momentum_5d', '?')}%")
            print(f"    Momentum 20d:  {ctx.get('momentum_20d', '?')}%")
            print(f"    IV:            {ctx['iv']*100:.1f}%" if ctx.get("iv") else "    IV:            [WARN] ausente")
            print(f"    IV Rank 52w:   {ctx['iv_rank_52w']:.0f}%" if ctx.get("iv_rank_52w") else "    IV Rank 52w:   [WARN] em construcao")
            print(f"    Skew:          {ctx['skew_pct']:+.1f}%" if ctx.get("skew_pct") else "    Skew:          [WARN] ausente")
            print(f"    Term Struct:   {ctx.get('term_structure', '?')}")
            print(f"    COT Index:     {ctx['cot_index']:.0f}" if ctx.get("cot_index") else "    COT Index:     [WARN] ausente")
            print(f"    Sazonalidade:  {ctx.get('seasonality_month','?')} avg={ctx.get('seasonality_avg','?')}%")
            print(f"    Curva Forward: {ctx.get('fwd_curve_shape', '?')} ({ctx.get('fwd_curve_diff_pct', '?')}%)")
            print(f"    VIX:           {ctx.get('vix', '?')} ({ctx.get('vix_level', '?')})")
            print(f"    Net Liq:       ${ctx.get('net_liq', 0):,.0f}")
            print(f"    Posicoes:      {ctx.get('active_count', '?')} ({', '.join(ctx.get('active_positions', []))})")
            if ctx.get("spreads"):
                print(f"    Spreads:")
                for sp_name, sp_data in ctx["spreads"].items():
                    print(f"      {sp_name}: z={sp_data['zscore']:.2f} ({sp_data['regime']})")

    elif cmd == "close" and len(args) >= 3:
        sym = args[1].upper()
        direction = args[2].upper()
        pnl = float(args[3]) if len(args) > 3 else None
        notes = " ".join(args[4:]) if len(args) > 4 else ""
        close_trade(sym, direction, pnl=pnl, notes=notes)

    elif cmd == "show":
        filter_sym = args[1].upper() if len(args) > 1 else None
        show_trades(filter_sym)

    elif cmd == "stats":
        show_stats()

    else:
        print("  Uso: skill_trade_journal.py [open|close|show|stats] [SYM] [DIR] [...]")


if __name__ == "__main__":
    main()
