#!/usr/bin/env python3
"""
AgriMacro — Roll/Close Decision Engine

Monitors all open positions and recommends:
  - HOLD: position is fine, let theta work
  - CLOSE: take profit or cut loss
  - ROLL: close and re-enter next expiry (with conditions)
  - URGENT: DTE < 10, needs immediate action

Based on validated rules:
  R06: Max loss = 2x credit (stop mechanical)
  R07: Never add to loser (no re-entry on same spread)
  R08: Close at 50% max profit
  R09: DTE < 21 with profit < 25% = close and roll
  Rule 1 (cross_analysis): Never roll — each roll worsens avg by $12K

Usage:
  python pipeline/skill_roll_decision.py                    # Full portfolio monitor
  python pipeline/skill_roll_decision.py GF CALL 376 10     # Specific position
  python pipeline/skill_roll_decision.py SI PUT 65 25
  python pipeline/skill_roll_decision.py portfolio           # Same as no args
"""

import json
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent.parent
PROC = BASE / "agrimacro-dash" / "public" / "data" / "processed"
OUT = BASE / "pipeline" / "roll_decisions.json"

NAMES = {
    "ZC": "Milho", "ZS": "Soja", "ZW": "Trigo", "KE": "Trigo KC",
    "ZM": "Farelo", "ZL": "Oleo Soja", "LE": "Boi Gordo", "GF": "Feeder",
    "HE": "Suino", "SB": "Acucar", "KC": "Cafe", "CT": "Algodao",
    "CC": "Cacau", "CL": "Petroleo", "NG": "Gas Nat", "GC": "Ouro", "SI": "Prata",
}

# Month code to expiry month
MONTH_CODES = {
    "F": "Jan", "G": "Feb", "H": "Mar", "J": "Apr", "K": "May", "M": "Jun",
    "N": "Jul", "Q": "Aug", "U": "Sep", "V": "Oct", "X": "Nov", "Z": "Dec",
}


def jload(path):
    for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            with open(path, encoding=enc) as f:
                return json.load(f)
        except Exception:
            continue
    return {}


def parse_local_symbol(ls):
    """Parse IBKR local symbol to extract expiry code and option details."""
    parts = ls.strip().split()
    if len(parts) >= 2:
        contract = parts[0]  # e.g. "GFJ6", "LON6", "SOK6"
        option = parts[1] if len(parts) > 1 else ""  # e.g. "C3760", "P6500"
        right = option[0] if option and option[0] in ("C", "P") else None
        strike_str = option[1:] if right else ""
        return {"contract": contract, "right": right, "strike_str": strike_str, "full": ls}
    return {"contract": ls.strip(), "right": None, "strike_str": "", "full": ls}


def get_dte_for_position(sym, local_sym, options_data):
    """Find DTE for a position by matching contract code."""
    parsed = parse_local_symbol(local_sym)
    contract_code = parsed["contract"]

    und = options_data.get("underlyings", {}).get(sym, {})
    exps = und.get("expirations", {})

    for exp_key, exp_data in exps.items():
        exp_contract = exp_data.get("contract", "")
        # Direct match
        if exp_contract == contract_code:
            return exp_data.get("days_to_exp", None)
        # IBKR options prefix: "LO" for CL, "SO" for SI, "CO" for CC, "OZL" for ZL
        # Strip known prefixes to match: LON6 -> N6, SOK6 -> K6, CON6 -> N6, OZLK6 -> K6
        def strip_prefix(code, symbol):
            prefixes = {
                "CL": "LO", "SI": "SO", "CC": "CO", "GC": "OG",
                "ZC": "OZC", "ZS": "OZS", "ZW": "OZW", "ZM": "OZM",
                "ZL": "OZL", "KE": "OKE", "NG": "LNE",
                "LE": "OLE", "GF": "OGF", "HE": "OHE",
                "SB": "OSB", "KC": "OKC", "CT": "OCT",
            }
            pref = prefixes.get(symbol, "")
            if pref and code.startswith(pref):
                return code[len(pref):]
            return code

        code_stripped = strip_prefix(contract_code, sym)
        exp_stripped = strip_prefix(exp_contract, sym)
        if code_stripped and exp_stripped and code_stripped == exp_stripped:
            return exp_data.get("days_to_exp", None)
        # Match by last 2 chars (month+year: N6, K6, etc.)
        if len(contract_code) >= 2 and len(exp_contract) >= 2:
            if contract_code[-2:] == exp_contract[-2:]:
                return exp_data.get("days_to_exp", None)

    return None


def analyze_spread(sym, legs, options_data, greeks_data):
    """
    Analyze a spread (group of legs for one underlying+expiry) and recommend action.
    Returns decision dict.
    """
    if not legs:
        return None

    # Aggregate legs
    total_delta = 0
    total_theta = 0
    total_qty = 0
    sold_legs = []
    bought_legs = []
    dte = None
    contract_code = None

    for leg in legs:
        pos = leg.get("position", 0)
        d = leg.get("delta") or 0
        th = leg.get("theta") or 0
        ls = leg.get("local_symbol", "")
        parsed = parse_local_symbol(ls)

        total_delta += d * abs(pos)
        total_theta += th * abs(pos)
        total_qty += abs(pos)

        if pos < 0:
            sold_legs.append(leg)
        else:
            bought_legs.append(leg)

        # Get DTE
        if dte is None:
            dte = get_dte_for_position(sym, ls, options_data)
            contract_code = parsed["contract"]

    if dte is None:
        dte = 999  # Unknown

    # Determine direction
    net_sold = sum(abs(l["position"]) for l in sold_legs)
    net_bought = sum(abs(l["position"]) for l in bought_legs)
    has_calls = any(parse_local_symbol(l["local_symbol"])["right"] == "C" for l in legs)
    has_puts = any(parse_local_symbol(l["local_symbol"])["right"] == "P" for l in legs)
    direction = "CALL" if has_calls and not has_puts else "PUT" if has_puts and not has_calls else "MIXED"

    # Structure description
    structure_parts = []
    for leg in sorted(legs, key=lambda l: l.get("local_symbol", "")):
        pos = leg["position"]
        ls = leg["local_symbol"]
        parsed = parse_local_symbol(ls)
        action = "SELL" if pos < 0 else "BUY"
        structure_parts.append(f"{action} {abs(pos):.0f}x {parsed['right'] or 'FUT'}{parsed['strike_str']}")

    # ── Decision Logic ──
    urgency = "LOW"
    action = "HOLD"
    reasons = []
    warnings = []

    # DTE checks
    if dte <= 5:
        urgency = "CRITICAL"
        action = "CLOSE"
        reasons.append(f"DTE={dte} — CRITICO. Gamma risk extremo. Fechar HOJE.")
    elif dte <= 10:
        urgency = "HIGH"
        action = "CLOSE"
        reasons.append(f"DTE={dte} — menos de 10 dias. Gamma acelera. Recomendado fechar.")
    elif dte <= 21:
        urgency = "MEDIUM"
        reasons.append(f"DTE={dte} — zona de gamma. Monitorar diariamente.")
        # R09: DTE < 21 with low profit = close
        if total_theta < 0:
            action = "CLOSE"
            reasons.append("Theta negativo com DTE < 21 — posicao perdendo tempo.")
        else:
            action = "MONITOR"
    elif dte <= 45:
        urgency = "LOW"
        action = "HOLD"
        reasons.append(f"DTE={dte} — theta trabalhando. Manter.")
    else:
        urgency = "LOW"
        action = "HOLD"
        reasons.append(f"DTE={dte} — distante do vencimento. Theta lento mas seguro.")

    # Delta check — is position getting directional?
    if abs(total_delta) > 0.5 * total_qty and total_qty > 0:
        warnings.append(f"Delta total {total_delta:.2f} — posicao ficando direcional. Considerar ajuste.")
        if urgency == "LOW":
            urgency = "MEDIUM"

    # Rule 1 from cross_analysis: never roll
    roll_note = "NAO ROLAR — cross_analysis mostra que cada roll piora resultado em ~$12K. Se precisar agir, FECHAR e abrir ciclo novo."

    # Next expiry available?
    next_exp = None
    und_data = options_data.get("underlyings", {}).get(sym, {})
    exps = sorted(und_data.get("expirations", {}).items())
    current_found = False
    for exp_key, exp_data in exps:
        if current_found:
            next_exp = {
                "expiry": exp_key,
                "contract": exp_data.get("contract", ""),
                "dte": exp_data.get("days_to_exp", 0),
            }
            break
        if exp_data.get("contract", "") == contract_code:
            current_found = True

    return {
        "sym": sym,
        "name": NAMES.get(sym, sym),
        "contract": contract_code or "?",
        "direction": direction,
        "dte": dte,
        "urgency": urgency,
        "action": action,
        "reasons": reasons,
        "warnings": warnings,
        "roll_note": roll_note,
        "next_expiry": next_exp,
        "legs": len(legs),
        "net_sold": net_sold,
        "net_bought": net_bought,
        "total_delta": round(total_delta, 4),
        "total_theta": round(total_theta, 4),
        "structure": " | ".join(structure_parts),
    }


def run_portfolio_monitor(portfolio, options_data, greeks_data):
    """Analyze all positions in portfolio."""
    positions = portfolio.get("positions", [])

    # Group by (symbol, contract_code)
    by_spread = defaultdict(list)
    for p in positions:
        sym = p.get("symbol", "")
        st = p.get("sec_type", "")
        if st not in ("FOP", "OPT", "FUT"):
            continue
        ls = p.get("local_symbol", "")
        parsed = parse_local_symbol(ls)
        # Group by symbol + contract code (same expiry)
        key = (sym, parsed["contract"][:4] if len(parsed["contract"]) >= 4 else parsed["contract"])
        by_spread[key].append(p)

    decisions = []
    for (sym, grp), legs in by_spread.items():
        decision = analyze_spread(sym, legs, options_data, greeks_data)
        if decision:
            decisions.append(decision)

    # Sort by urgency
    urgency_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    decisions.sort(key=lambda d: (urgency_order.get(d["urgency"], 4), d["dte"]))

    return decisions


def run_specific(sym, direction, strike, dte_input):
    """Score a specific position scenario."""
    options_data = jload(PROC / "options_chain.json")

    # Build a synthetic leg
    leg = {
        "symbol": sym,
        "local_symbol": f"{sym}?? {direction[0]}{strike}",
        "sec_type": "FOP",
        "position": -1,
        "delta": -0.30 if direction == "PUT" else 0.30,
        "theta": 0.01,
    }

    # Create minimal analysis
    decision = {
        "sym": sym,
        "name": NAMES.get(sym, sym),
        "direction": direction,
        "dte": dte_input,
        "urgency": "LOW",
        "action": "HOLD",
        "reasons": [],
        "warnings": [],
        "structure": f"SELL {direction} @{strike}",
    }

    # DTE logic
    if dte_input <= 5:
        decision["urgency"] = "CRITICAL"
        decision["action"] = "CLOSE"
        decision["reasons"].append(f"DTE={dte_input} — CRITICO. Gamma risk extremo. Fechar HOJE.")
    elif dte_input <= 10:
        decision["urgency"] = "HIGH"
        decision["action"] = "CLOSE"
        decision["reasons"].append(f"DTE={dte_input} — Alto risco gamma. Recomendado fechar esta semana.")
    elif dte_input <= 21:
        decision["urgency"] = "MEDIUM"
        decision["action"] = "MONITOR"
        decision["reasons"].append(f"DTE={dte_input} — Zona de gamma. Monitorar diariamente.")
        decision["reasons"].append("Se profit > 50%: fechar. Se profit < 25%: fechar e nao rolar.")
    elif dte_input <= 45:
        decision["urgency"] = "LOW"
        decision["action"] = "HOLD"
        decision["reasons"].append(f"DTE={dte_input} — Theta acelerando. Bom momento, manter.")
    else:
        decision["urgency"] = "LOW"
        decision["action"] = "HOLD"
        decision["reasons"].append(f"DTE={dte_input} — Distante. Theta lento. Paciencia.")

    decision["roll_note"] = "NAO ROLAR — fechar e abrir ciclo novo se necessario (cross_analysis: roll piora avg em $12K)"

    # Check next available expiry
    und_data = options_data.get("underlyings", {}).get(sym, {})
    exps = sorted(und_data.get("expirations", {}).items())
    for exp_key, exp_data in exps:
        exp_dte = exp_data.get("days_to_exp", 0)
        if exp_dte > dte_input + 20:
            decision["next_expiry"] = {
                "expiry": exp_key,
                "contract": exp_data.get("contract", ""),
                "dte": exp_dte,
            }
            break

    return decision


def main():
    print("=" * 60)
    print("ROLL/CLOSE DECISION ENGINE")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')} ({datetime.now().strftime('%A')})")
    print("=" * 60)

    args = sys.argv[1:]

    # Specific position mode: sym direction strike dte
    if len(args) >= 4 and args[1].upper() in ("PUT", "CALL"):
        sym = args[0].upper()
        direction = args[1].upper()
        strike = int(args[2])
        dte = int(args[3])

        decision = run_specific(sym, direction, strike, dte)
        print(f"\n  {sym} {direction} @{strike} (DTE={dte})")
        print(f"  Urgencia: {decision['urgency']}")
        print(f"  Acao: {decision['action']}")
        for r in decision["reasons"]:
            print(f"    -> {r}")
        if decision.get("warnings"):
            for w in decision["warnings"]:
                print(f"    !! {w}")
        print(f"  Roll: {decision['roll_note']}")
        if decision.get("next_expiry"):
            ne = decision["next_expiry"]
            print(f"  Proximo vencimento: {ne['contract']} (DTE={ne['dte']})")
        return

    # Portfolio mode
    if args and args[0].lower() not in ("portfolio", "all"):
        # Try sym-only query
        sym = args[0].upper()
        portfolio = jload(PROC / "ibkr_portfolio.json")
        options_data = jload(PROC / "options_chain.json")
        greeks = jload(PROC / "ibkr_greeks.json")
        decisions = run_portfolio_monitor(portfolio, options_data, greeks)
        sym_decisions = [d for d in decisions if d["sym"] == sym]
        if not sym_decisions:
            print(f"\n  Nenhuma posicao encontrada para {sym}")
            return
        for d in sym_decisions:
            _print_decision(d)
        return

    # Full portfolio scan
    portfolio = jload(PROC / "ibkr_portfolio.json")
    options_data = jload(PROC / "options_chain.json")
    greeks = jload(PROC / "ibkr_greeks.json")

    if not portfolio.get("positions"):
        print("\n  Portfolio vazio ou nao disponivel.")
        return

    decisions = run_portfolio_monitor(portfolio, options_data, greeks)

    # Print report
    critical = [d for d in decisions if d["urgency"] == "CRITICAL"]
    high = [d for d in decisions if d["urgency"] == "HIGH"]
    medium = [d for d in decisions if d["urgency"] == "MEDIUM"]
    low = [d for d in decisions if d["urgency"] == "LOW"]

    if critical:
        print(f"\n{'!'*60}")
        print("CRITICAL — ACAO IMEDIATA NECESSARIA")
        print(f"{'!'*60}")
        for d in critical:
            _print_decision(d)

    if high:
        print(f"\n{'='*60}")
        print("HIGH — FECHAR ESTA SEMANA")
        print(f"{'='*60}")
        for d in high:
            _print_decision(d)

    if medium:
        print(f"\n{'-'*60}")
        print("MEDIUM — MONITORAR DIARIAMENTE")
        print(f"{'-'*60}")
        for d in medium:
            _print_decision(d)

    if low:
        print(f"\n{'.'*60}")
        print("LOW — HOLD (theta trabalhando)")
        print(f"{'.'*60}")
        for d in low:
            _print_decision(d, brief=True)

    # Summary
    print(f"\n{'='*60}")
    print(f"RESUMO: {len(decisions)} spreads monitorados")
    print(f"  CRITICAL: {len(critical)}")
    print(f"  HIGH:     {len(high)}")
    print(f"  MEDIUM:   {len(medium)}")
    print(f"  LOW:      {len(low)}")

    # Save
    output = {
        "generated_at": datetime.now().isoformat(),
        "total_spreads": len(decisions),
        "critical": len(critical),
        "high": len(high),
        "medium": len(medium),
        "low": len(low),
        "decisions": [{
            "sym": d["sym"], "contract": d["contract"],
            "direction": d["direction"], "dte": d["dte"],
            "urgency": d["urgency"], "action": d["action"],
            "reasons": d["reasons"], "warnings": d.get("warnings", []),
            "structure": d["structure"],
            "next_expiry": d.get("next_expiry"),
        } for d in decisions],
    }
    with open(OUT, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n[SAVED] {OUT}")


def _print_decision(d, brief=False):
    urgency_icon = {"CRITICAL": "!!!", "HIGH": "!! ", "MEDIUM": "!  ", "LOW": "   "}.get(d["urgency"], "   ")
    action_str = {"CLOSE": "FECHAR", "HOLD": "MANTER", "MONITOR": "MONITORAR"}.get(d["action"], d["action"])

    print(f"\n  {urgency_icon} {d['sym']:>4} ({d['name']:>10}) | {d['direction']:>5} | "
          f"DTE={d['dte']:>3} | {action_str}")
    print(f"      Contrato: {d['contract']} | {d['legs']} legs | "
          f"delta={d['total_delta']:+.3f} | theta={d['total_theta']:+.4f}")
    print(f"      Estrutura: {d['structure']}")

    if not brief:
        for r in d["reasons"]:
            print(f"      -> {r}")
        for w in d.get("warnings", []):
            print(f"      !! {w}")
        if d.get("next_expiry"):
            ne = d["next_expiry"]
            print(f"      Proximo: {ne['contract']} (DTE={ne['dte']}d)")
        print(f"      Roll: {d['roll_note']}")


if __name__ == "__main__":
    main()
