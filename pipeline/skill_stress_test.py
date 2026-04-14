#!/usr/bin/env python3
"""
AgriMacro — Portfolio Stress Test & Vulnerability Scanner

Analyzes each active position under stress scenarios:
  - Price shock: +/-5%, +/-10%, +/-15%
  - IV crush / spike: +/-30%
  - Correlation cascade: correlated moves
  - Theta decay acceleration (DTE < 21)
  - Delta drift beyond neutral

Identifies most vulnerable position and overall portfolio risk.

Run:
  python pipeline/skill_stress_test.py
"""

import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent.parent
PROC = BASE / "agrimacro-dash" / "public" / "data" / "processed"
OUT = BASE / "pipeline" / "stress_test.json"

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

MULTIPLIERS = {
    "CL": 1000, "SI": 5000, "GF": 500, "ZL": 600, "CC": 10,
    "ZC": 50, "ZS": 50, "ZW": 50, "ZM": 100, "SB": 1120,
    "KC": 375, "CT": 500, "LE": 400, "HE": 400, "NG": 10000,
    "GC": 100, "KE": 50,
}

MARGIN_PER = {
    "CL": 3500, "SI": 8000, "GF": 2500, "ZL": 1500, "CC": 3000,
    "ZC": 1200, "ZS": 2500, "ZW": 1800, "ZM": 1500, "SB": 1800,
    "KC": 4000, "LE": 2000, "HE": 1500, "NG": 5000, "GC": 5000,
    "KE": 1500, "CT": 1200,
}

# Known correlation pairs (from correlation matrix)
CORR_PAIRS = {
    "CL": [("ZL", 0.40), ("NG", 0.31)],
    "ZL": [("CL", 0.40), ("ZS", 0.35)],
    "SI": [("GC", 0.80)],
    "GC": [("SI", 0.80)],
    "GF": [("LE", 0.60), ("ZC", -0.40)],
    "CC": [("KC", 0.25)],
}


def jload(path):
    for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            with open(path, encoding=enc) as f:
                return json.load(f)
        except Exception:
            continue
    return {}


def estimate_pnl_shock(delta, gamma, vega, theta, price_shock_pct, iv_shock_pct, und_price, mult, qty_net, days=0):
    """
    Estimate P&L under a combined price + IV shock using Taylor expansion.
    delta/gamma/vega/theta are per-contract.
    qty_net = net position (positive=long, negative=short).
    """
    if und_price <= 0:
        return 0

    dS = und_price * price_shock_pct / 100
    pnl_delta = delta * dS * abs(qty_net) * mult
    pnl_gamma = 0.5 * gamma * dS * dS * abs(qty_net) * mult
    pnl_vega = vega * iv_shock_pct * abs(qty_net) * mult  # vega per 1% IV change
    pnl_theta = theta * days * abs(qty_net) * mult

    # For short positions, signs flip
    if qty_net < 0:
        pnl_delta = -pnl_delta
        pnl_gamma = -pnl_gamma

    return pnl_delta + pnl_gamma + pnl_vega + pnl_theta


def main():
    print("=" * 65)
    print("STRESS TEST — Portfolio Vulnerability Analysis")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 65)

    portfolio = jload(PROC / "ibkr_portfolio.json")
    options = jload(PROC / "options_chain.json")
    prices = jload(PROC / "price_history.json")
    theta_cal = jload(BASE / "pipeline" / "theta_calendar.json")

    net_liq = float(portfolio.get("summary", {}).get("NetLiquidation", 0))

    # Build position aggregates per underlying
    positions = defaultdict(lambda: {
        "delta": 0, "gamma": 0, "theta": 0, "vega": 0,
        "sold": 0, "bought": 0, "legs": 0,
        "margin_est": 0, "und_price": 0, "iv": 0,
        "dte": None, "contract": "", "direction": "",
    })

    for p in portfolio.get("positions", []):
        sym = p.get("symbol", "")
        if p.get("sec_type") not in ("FOP", "OPT", "FUT"):
            continue

        pos = p.get("position", 0)
        ls = p.get("local_symbol", "")
        contract = ls.split()[0] if " " in ls else ls
        grp = contract

        key = (sym, grp)
        d = positions[key]
        # Greeks must be SIGN-AWARE: sold positions flip the sign
        # pos > 0 = long, pos < 0 = short
        # delta per contract * position = net delta contribution
        # For puts: delta is negative per contract, sold put = positive delta contribution
        d["delta"] += (p.get("delta") or 0) * pos
        d["gamma"] += (p.get("gamma") or 0) * pos   # gamma: long = +, short = -
        d["theta"] += (p.get("theta") or 0) * pos   # theta: long puts = -, short puts = +
        d["vega"] += (p.get("vega") or 0) * pos     # vega: long = +, short = -
        d["legs"] += 1
        if pos < 0:
            d["sold"] += abs(int(pos))
            d["margin_est"] += abs(int(pos)) * MARGIN_PER.get(sym, 2000)
        else:
            d["bought"] += int(pos)

        if p.get("und_price"):
            d["und_price"] = p["und_price"]
        if p.get("iv"):
            d["iv"] = p["iv"]

    # Get und_price from price history if missing
    for (sym, grp), d in positions.items():
        if d["und_price"] <= 0:
            bars = prices.get(sym, [])
            if isinstance(bars, dict):
                bars = bars.get("history", [])
            if bars:
                d["und_price"] = bars[-1].get("close", 0)

    # Get DTE from theta calendar
    for t in theta_cal.get("timeline", []):
        key = (t["sym"], t["contract"])
        if key in positions and t.get("dte"):
            positions[key]["dte"] = t["dte"]

    # Determine option type from local_symbol (C or P), not from delta
    for (sym, grp), d in positions.items():
        has_calls = False
        has_puts = False
        for p in portfolio.get("positions", []):
            if p.get("symbol") != sym or p.get("sec_type") not in ("FOP", "OPT"):
                continue
            ls = p.get("local_symbol", "")
            parts = ls.split()
            if len(parts) >= 2:
                right_part = parts[1]
                if right_part.startswith("C"):
                    has_calls = True
                elif right_part.startswith("P"):
                    has_puts = True
        if has_calls and not has_puts:
            d["direction"] = "CALL_SPREAD"
        elif has_puts and not has_calls:
            d["direction"] = "PUT_SPREAD"
        elif has_calls and has_puts:
            d["direction"] = "MIXED"
        else:
            d["direction"] = "FUT" if d.get("sold", 0) > 0 or d.get("bought", 0) > 0 else "UNKNOWN"

    # ── Price momentum (5d) for each active underlying ──
    momentum = {}
    for (sym, _) in positions:
        if sym in momentum:
            continue
        bars = prices.get(sym, [])
        if isinstance(bars, dict):
            bars = bars.get("history", [])
        if len(bars) >= 6:
            momentum[sym] = {
                "5d": round(((bars[-1]["close"] - bars[-6]["close"]) / bars[-6]["close"]) * 100, 1),
                "20d": round(((bars[-1]["close"] - bars[-21]["close"]) / bars[-21]["close"]) * 100, 1) if len(bars) >= 21 else None,
            }

    # ════════════════════════════════════════════════════
    # STRESS SCENARIOS
    # ════════════════════════════════════════════════════
    scenarios = [
        {"name": "Price -5%",  "price": -5, "iv": 5,  "days": 0},
        {"name": "Price -10%", "price": -10, "iv": 10, "days": 0},
        {"name": "Price -15%", "price": -15, "iv": 15, "days": 0},
        {"name": "Price +5%",  "price": 5,  "iv": -3, "days": 0},
        {"name": "Price +10%", "price": 10, "iv": -5, "days": 0},
        {"name": "Price +15%", "price": 15, "iv": -8, "days": 0},
        {"name": "IV Spike +30%", "price": 0, "iv": 30, "days": 0},
        {"name": "IV Crush -30%", "price": 0, "iv": -30, "days": 0},
        {"name": "7-day theta",   "price": 0, "iv": 0,  "days": 7},
    ]

    print(f"\n  {'='*63}")
    print(f"  PER-POSITION STRESS")
    print(f"  {'='*63}")

    all_stress = {}
    vulnerability_scores = {}

    for (sym, grp), d in sorted(positions.items()):
        mult = MULTIPLIERS.get(sym, 100)
        und_price = d["und_price"]
        if und_price <= 0:
            continue

        # Per-contract averages
        n = max(d["sold"], 1)
        avg_delta = d["delta"] / n if n else 0
        avg_gamma = d["gamma"] / n if n else 0
        avg_vega = d["vega"] / n if n else 0
        avg_theta = d["theta"] / n if n else 0
        net_qty = d["bought"] - d["sold"]  # net direction

        mom = momentum.get(sym, {})
        mom_str = f"5d={mom.get('5d', '?')}% 20d={mom.get('20d', '?')}%"

        print(f"\n  {sym:>4} {grp:>8} | {d['sold']} sold / {d['bought']} bought | "
              f"delta={d['delta']:+.1f} | theta={d['theta']:+.2f} | "
              f"DTE={d['dte'] or '?'} | und=${und_price:.2f} | mom: {mom_str}")

        worst_loss = 0
        worst_scenario = ""
        stress_results = []

        for sc in scenarios:
            pnl = estimate_pnl_shock(
                d["delta"], d["gamma"], d["vega"], d["theta"],
                sc["price"], sc["iv"], und_price, 1,  # mult already in greeks
                1, sc["days"]
            )
            # Simplified: use delta*move as primary estimate
            price_move = und_price * sc["price"] / 100
            est_pnl = d["delta"] * price_move + d["vega"] * sc["iv"] * 100 + d["theta"] * sc["days"]

            stress_results.append({"scenario": sc["name"], "est_pnl": round(est_pnl, 0)})

            if est_pnl < worst_loss:
                worst_loss = est_pnl
                worst_scenario = sc["name"]

            loss_str = f"${est_pnl:>+10,.0f}" if est_pnl != 0 else f"{'$0':>11}"
            print(f"    {sc['name']:>16}: {loss_str}")

        # Vulnerability score: worst loss as % of net_liq
        vuln_pct = abs(worst_loss) / net_liq * 100 if net_liq > 0 else 0
        all_stress[(sym, grp)] = {
            "worst_loss": worst_loss,
            "worst_scenario": worst_scenario,
            "vuln_pct": round(vuln_pct, 2),
            "results": stress_results,
        }
        vulnerability_scores[(sym, grp)] = vuln_pct

        if vuln_pct >= 3:
            print(f"    >>> VULNERAVEL: worst case ${worst_loss:,.0f} ({vuln_pct:.1f}% do capital) em {worst_scenario}")
        elif vuln_pct >= 1:
            print(f"    >>> Moderado: worst ${worst_loss:,.0f} ({vuln_pct:.1f}%) em {worst_scenario}")

    # ════════════════════════════════════════════════════
    # CORRELATION CASCADE
    # ════════════════════════════════════════════════════
    print(f"\n  {'='*63}")
    print(f"  CORRELACAO CASCADE (-10% simultaneous)")
    print(f"  {'='*63}")

    active_syms = set(sym for (sym, _) in positions)
    cascade_loss = 0
    for (sym, grp), d in positions.items():
        if d["und_price"] <= 0:
            continue
        move = d["und_price"] * -0.10
        loss = d["delta"] * move
        cascade_loss += loss

    cascade_pct = abs(cascade_loss) / net_liq * 100 if net_liq > 0 and cascade_loss < 0 else 0
    print(f"  Se TODOS os underlyings caem 10% simultaneamente:")
    print(f"  Perda estimada: ${cascade_loss:,.0f} ({cascade_pct:.1f}% do capital)")
    if cascade_pct > 10:
        print(f"  >>> RISCO ALTO: perda > 10% do capital em cenario de panico")
    elif cascade_pct > 5:
        print(f"  >>> RISCO MODERADO: ativa drawdown protocol nivel 1 (reduzir sizing 25%)")

    # ════════════════════════════════════════════════════
    # MOST VULNERABLE POSITION
    # ════════════════════════════════════════════════════
    print(f"\n  {'='*63}")
    print(f"  POSICAO MAIS VULNERAVEL")
    print(f"  {'='*63}")

    if vulnerability_scores:
        worst_key = max(vulnerability_scores, key=vulnerability_scores.get)
        worst_sym, worst_grp = worst_key
        worst_data = all_stress[worst_key]
        pos_data = positions[worst_key]
        mom_w = momentum.get(worst_sym, {})

        print(f"\n  >>> {worst_sym} {worst_grp}")
        print(f"      Worst case: ${worst_data['worst_loss']:,.0f} ({worst_data['vuln_pct']:.1f}% do capital)")
        print(f"      Cenario: {worst_data['worst_scenario']}")
        print(f"      Delta: {pos_data['delta']:+.1f} | Sold: {pos_data['sold']} | Margin: ${pos_data['margin_est']:,.0f}")
        print(f"      Momentum: 5d={mom_w.get('5d','?')}% | 20d={mom_w.get('20d','?')}%")
        print(f"      DTE: {pos_data['dte'] or '?'}")

        # Risk factors
        risk_factors = []
        if mom_w.get("5d") and abs(mom_w["5d"]) > 8:
            risk_factors.append(f"Momentum 5d forte ({mom_w['5d']:+.1f}%) — move pode continuar")
        if pos_data.get("dte") and pos_data["dte"] <= 15:
            risk_factors.append(f"DTE={pos_data['dte']} — gamma risk elevado")
        if abs(pos_data["delta"]) > 20:
            risk_factors.append(f"Delta drift {pos_data['delta']:+.1f} — posicao direcional")
        if pos_data.get("iv") and pos_data["iv"] > 0.5:
            risk_factors.append(f"IV={pos_data['iv']*100:.0f}% — alta volatilidade amplifica moves")

        if risk_factors:
            print(f"\n      Fatores de risco:")
            for rf in risk_factors:
                print(f"        ! {rf}")

        # Recommendation
        print(f"\n      Recomendacao:")
        if worst_data["vuln_pct"] >= 5:
            print(f"        REDUZIR: fechar 50% da posicao para limitar exposicao")
        elif worst_data["vuln_pct"] >= 2:
            print(f"        MONITORAR: definir stop loss em 2x o credito recebido")
        else:
            print(f"        OK: risco dentro dos parametros aceitaveis")

    # ════════════════════════════════════════════════════
    # PORTFOLIO RISK SUMMARY
    # ════════════════════════════════════════════════════
    print(f"\n  {'='*63}")
    print(f"  RESUMO DE RISCO DO PORTFOLIO")
    print(f"  {'='*63}")

    total_delta = sum(d["delta"] for d in positions.values())
    total_theta = sum(d["theta"] for d in positions.values())
    total_vega = sum(d["vega"] for d in positions.values())
    total_margin = sum(d["margin_est"] for d in positions.values())

    print(f"  Net Liq:        ${net_liq:,.0f}")
    print(f"  Total Margin:   ${total_margin:,.0f} ({total_margin/net_liq*100:.1f}%)")
    print(f"  Portfolio Delta: {total_delta:+.1f}")
    print(f"  Portfolio Theta: ${total_theta:+.2f}/dia")
    print(f"  Portfolio Vega:  {total_vega:+.2f}")
    print(f"  Cascade -10%:   ${cascade_loss:,.0f} ({cascade_pct:.1f}%)")
    print(f"  Posicoes:        {len(positions)}")

    # Risk assessment
    risk_level = "LOW"
    risk_notes = []
    if cascade_pct > 10:
        risk_level = "HIGH"
        risk_notes.append("Cascade loss > 10% — concentracao excessiva")
    elif cascade_pct > 5:
        risk_level = "MEDIUM"
        risk_notes.append("Cascade loss 5-10% — monitorar correlacoes")

    if total_margin / net_liq > 0.65:
        risk_level = "HIGH" if risk_level != "HIGH" else risk_level
        risk_notes.append(f"Margem {total_margin/net_liq*100:.0f}% > 65% — overallocated")

    if abs(total_delta) > 50:
        risk_notes.append(f"Portfolio delta {total_delta:+.1f} — direcional, nao neutro")

    worst_vuln = max(vulnerability_scores.values()) if vulnerability_scores else 0
    if worst_vuln > 5:
        risk_level = "HIGH"
        risk_notes.append(f"Posicao individual com {worst_vuln:.1f}% risk — concentrado")

    print(f"\n  Risk Level: {risk_level}")
    for rn in risk_notes:
        print(f"    ! {rn}")

    # Save
    output = {
        "generated_at": datetime.now().isoformat(),
        "net_liq": net_liq,
        "risk_level": risk_level,
        "risk_notes": risk_notes,
        "portfolio_greeks": {
            "delta": round(total_delta, 2),
            "theta": round(total_theta, 4),
            "vega": round(total_vega, 4),
        },
        "cascade_10pct": {
            "loss": round(cascade_loss, 0),
            "pct_of_capital": round(cascade_pct, 1),
        },
        "most_vulnerable": {
            "sym": worst_key[0] if vulnerability_scores else None,
            "contract": worst_key[1] if vulnerability_scores else None,
            "worst_loss": round(worst_data["worst_loss"], 0) if vulnerability_scores else 0,
            "vuln_pct": round(worst_data["vuln_pct"], 1) if vulnerability_scores else 0,
            "scenario": worst_data["worst_scenario"] if vulnerability_scores else None,
        },
        "positions": {
            f"{sym} {grp}": {
                "worst_loss": round(s["worst_loss"], 0),
                "vuln_pct": s["vuln_pct"],
                "worst_scenario": s["worst_scenario"],
            }
            for (sym, grp), s in all_stress.items()
        },
    }
    with open(OUT, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n  [SAVED] {OUT}")


if __name__ == "__main__":
    main()
