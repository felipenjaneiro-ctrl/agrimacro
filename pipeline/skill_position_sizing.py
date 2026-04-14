#!/usr/bin/env python3
"""
AgriMacro — Position Sizing Engine

Calculates proper position size for a new trade given:
  - Current portfolio (capital usage, sector exposure, correlations)
  - Capital management rules (60/25/15 split, 2.5% risk, 15% per underlying)
  - Entry score (higher score = more confidence = larger size allowed)
  - Correlation group limits (max 30% per sector)

Usage:
  python pipeline/skill_position_sizing.py                    # Portfolio capital summary
  python pipeline/skill_position_sizing.py SI PUT 10          # Size SI PUT with score=10
  python pipeline/skill_position_sizing.py ZW CALL 9          # Size ZW CALL with score=9
  python pipeline/skill_position_sizing.py KE PUT 7           # Size KE PUT with score=7
"""

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent.parent
PROC = BASE / "agrimacro-dash" / "public" / "data" / "processed"
OUT = BASE / "pipeline" / "position_sizing.json"

NAMES = {
    "ZC": "Milho", "ZS": "Soja", "ZW": "Trigo CBOT", "KE": "Trigo KC",
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

CORRELATION_GROUPS = {
    "Graos": {"ZC", "ZS", "ZW", "KE", "ZM", "ZL"},
    "Pecuaria": {"LE", "GF", "HE"},
    "Energia": {"CL", "NG"},
    "Metais": {"GC", "SI"},
    "Softs": {"SB", "KC", "CT", "CC"},
}

# Estimated SPAN margin per sold contract in a spread structure
MARGIN_PER_CONTRACT = {
    "CL": 3500, "SI": 8000, "GF": 2500, "ZL": 1500, "CC": 3000,
    "ZC": 1200, "ZS": 2500, "ZW": 1800, "ZM": 1500, "SB": 1800,
    "KC": 4000, "LE": 2000, "HE": 1500, "NG": 5000, "GC": 5000,
    "KE": 1500, "CT": 1200,
}

# Multipliers for premium calculation
OPT_MULTIPLIERS = {
    "CL": 1000, "SI": 5000, "GF": 500, "ZL": 600, "CC": 10,
    "ZC": 50, "ZS": 50, "ZW": 50, "ZM": 100, "SB": 1120,
    "KC": 375, "CT": 500, "LE": 400, "HE": 400, "NG": 10000,
    "GC": 100, "KE": 50,
}

# Sizing adjustment factors
SIZING_ADJUSTMENTS = {
    "CC": 0.50,   # Cocoa: vol extrema, sizing 50%
    "NG": 0.75,   # Nat Gas: intraday 10%+ moves
    "KC": 0.75,   # Coffee: frost risk spikes
}


def jload(path):
    for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            with open(path, encoding=enc) as f:
                return json.load(f)
        except Exception:
            continue
    return {}


def get_portfolio_state(portfolio):
    """Calculate current portfolio capital allocation."""
    summ = portfolio.get("summary", {})
    net_liq = float(summ.get("NetLiquidation", 0))
    buying_power = float(summ.get("BuyingPower", 0))
    cash = float(summ.get("TotalCashValue", 0))
    gross_pos = float(summ.get("GrossPositionValue", 0))

    positions = portfolio.get("positions", [])

    # Group by symbol — count sold contracts as margin proxy
    by_sym = defaultdict(lambda: {"legs": 0, "sold": 0, "bought": 0, "margin_est": 0})
    by_sector = defaultdict(lambda: {"syms": set(), "margin_est": 0})

    for p in positions:
        sym = p.get("symbol", "")
        st = p.get("sec_type", "")
        if st not in ("FOP", "OPT", "FUT"):
            continue
        pos = p.get("position", 0)
        by_sym[sym]["legs"] += 1
        margin_per = MARGIN_PER_CONTRACT.get(sym, 2000)

        if pos < 0:
            by_sym[sym]["sold"] += abs(int(pos))
            by_sym[sym]["margin_est"] += abs(int(pos)) * margin_per
        else:
            by_sym[sym]["bought"] += int(pos)

        sector = SECTORS.get(sym, "Other")
        by_sector[sector]["syms"].add(sym)
        if pos < 0:
            by_sector[sector]["margin_est"] += abs(int(pos)) * margin_per

    total_margin_est = sum(d["margin_est"] for d in by_sym.values())
    active_pct = (total_margin_est / net_liq * 100) if net_liq > 0 else 0
    positions_count = len([s for s in by_sym if by_sym[s]["legs"] > 0])

    return {
        "net_liq": net_liq,
        "buying_power": buying_power,
        "cash": cash,
        "gross_pos": gross_pos,
        "total_margin_est": total_margin_est,
        "active_pct": round(active_pct, 1),
        "positions_count": positions_count,
        "by_sym": dict(by_sym),
        "by_sector": {k: {"syms": list(v["syms"]), "margin_est": v["margin_est"]}
                      for k, v in by_sector.items()},
    }


def detect_regime(state, options_data):
    """
    Detect capital regime: NORMAL (60% limit) or VEGA (65% limit).
    VEGA regime activates when:
      - At least one active underlying has IV >= 40%
      - OR VIX-equivalent conditions (high vol environment)
    This is based on capital_management.vega_opportunity rules.
    """
    regime = "NORMAL"
    regime_limit = 60
    regime_reasons = []
    high_iv_positions = []

    for sym, data in state["by_sym"].items():
        if data["sold"] == 0:
            continue  # no short exposure

        und = options_data.get("underlyings", {}).get(sym, {})
        ivr = und.get("iv_rank", {})
        cur_iv = ivr.get("current_iv")

        if cur_iv is not None:
            iv_pct = cur_iv * 100
            if iv_pct >= 40:
                high_iv_positions.append({"sym": sym, "iv": iv_pct})

    if high_iv_positions:
        regime = "VEGA"
        regime_limit = 65
        iv_strs = [f"{p['sym']} IV={p['iv']:.0f}%" for p in high_iv_positions]
        regime_reasons.append(f"Regime VEGA ativo: {', '.join(iv_strs)}")
        regime_reasons.append("Limite expandido de 60% para 65% — posicoes ativas em IV alta justificam premium collection adicional")
        regime_reasons.append("Condicao: pelo menos 1 underlying com IV >= 40% no portfolio")

    return {
        "regime": regime,
        "limit_pct": regime_limit,
        "reasons": regime_reasons,
        "high_iv_positions": high_iv_positions,
    }


def calculate_sizing(sym, direction, entry_score, state, skill_data, regime_info=None):
    """
    Calculate position size for a new trade.
    Returns sizing recommendation with all checks.
    """
    net_liq = state["net_liq"]
    cm = skill_data.get("capital_management", {})
    active_trades = cm.get("active_trades", {})

    # Capital limits — regime-aware
    base_max_pct = cm.get("active_trades_max_pct", 60)
    if regime_info and regime_info["regime"] == "VEGA":
        max_active_pct = regime_info["limit_pct"]  # 65%
    else:
        max_active_pct = base_max_pct  # 60%

    max_per_und_pct = active_trades.get("max_per_underlying_pct", 15)
    max_per_sector_pct = active_trades.get("max_per_sector_pct", 30)
    max_positions = active_trades.get("max_simultaneous_positions", 12)
    risk_per_trade_pct = skill_data.get("trader_profile", {}).get("risk_per_trade_pct", 2.5)

    max_active_usd = net_liq * max_active_pct / 100
    max_per_und_usd = net_liq * max_per_und_pct / 100
    max_per_sector_usd = net_liq * max_per_sector_pct / 100
    max_risk_per_trade = net_liq * risk_per_trade_pct / 100

    # Current usage
    current_margin = state["total_margin_est"]
    current_active_pct = state["active_pct"]
    available_margin = max_active_usd - current_margin

    # Per-underlying usage
    sym_data = state["by_sym"].get(sym, {"margin_est": 0})
    current_und_margin = sym_data["margin_est"]
    available_und = max_per_und_usd - current_und_margin

    # Per-sector usage
    sector = SECTORS.get(sym, "Other")
    sector_data = state["by_sector"].get(sector, {"margin_est": 0})
    current_sector_margin = sector_data["margin_est"]
    available_sector = max_per_sector_usd - current_sector_margin

    # Effective available = minimum of all limits
    effective_available = max(0, min(available_margin, available_und, available_sector))

    # Margin per contract
    margin_per = MARGIN_PER_CONTRACT.get(sym, 2000)

    # Sizing adjustment for volatile underlyings
    adj = SIZING_ADJUSTMENTS.get(sym, 1.0)

    # Score-based sizing multiplier
    # Score 10+ = full size, 8-9 = 80%, 6-7 = 60%, <6 = 40%
    if entry_score >= 10:
        score_mult = 1.0
        score_label = "FULL SIZE (score >= 10)"
    elif entry_score >= 8:
        score_mult = 0.80
        score_label = "80% SIZE (score 8-9)"
    elif entry_score >= 6:
        score_mult = 0.60
        score_label = "60% SIZE (score 6-7)"
    else:
        score_mult = 0.40
        score_label = "40% SIZE (score < 6)"

    # Max contracts based on risk
    max_by_risk = int(max_risk_per_trade / margin_per) if margin_per > 0 else 0
    max_by_available = int(effective_available / margin_per) if margin_per > 0 else 0

    # Final sizing
    raw_contracts = min(max_by_risk, max_by_available)
    adjusted_contracts = max(0, int(raw_contracts * adj * score_mult))

    # For 22x22 butterfly structure: sold contracts = adjusted_contracts
    # Total structure: sold + bought = 2x (approximately)
    butterfly_sold = adjusted_contracts
    butterfly_total = butterfly_sold * 2  # approx

    est_margin = butterfly_sold * margin_per
    est_risk = est_margin  # max loss ~ margin for defined risk spreads

    # ── Violations check ──
    violations = []
    warnings = []

    # Position count limit
    if state["positions_count"] >= max_positions:
        violations.append(f"LIMITE DE POSICOES: {state['positions_count']}/{max_positions} (maximo atingido)")

    # Active margin limit — regime-aware messaging
    regime_name = regime_info["regime"] if regime_info else "NORMAL"
    if current_active_pct >= max_active_pct:
        over_pct = current_active_pct - max_active_pct
        if regime_name == "VEGA":
            # In VEGA regime, being over is less severe — provide context
            warnings.append(
                f"CAPITAL ACIMA DO LIMITE VEGA: {current_active_pct:.1f}% em uso (limite VEGA={max_active_pct}%, normal=60%). "
                f"Excede em {over_pct:.1f}pp. Fechar 1 posicao para liberar margem antes de nova entrada."
            )
            # Still a violation but with better context
            violations.append(
                f"CAPITAL: {current_active_pct:.1f}% > {max_active_pct}% (regime {regime_name}). "
                f"Liberar ~${over_pct * net_liq / 100:,.0f} fechando posicao existente."
            )
        else:
            violations.append(f"LIMITE DE CAPITAL: {current_active_pct:.1f}% em uso (max={max_active_pct}%)")

    # Per-underlying check
    if current_und_margin > 0:
        new_und_pct = (current_und_margin + est_margin) / net_liq * 100
        if new_und_pct > max_per_und_pct:
            violations.append(f"LIMITE {sym}: ja tem ${current_und_margin:,.0f} alocado. "
                              f"Novo total seria {new_und_pct:.1f}% (max={max_per_und_pct}%)")

    # Correlation / sector check
    sector_syms = state["by_sector"].get(sector, {}).get("syms", [])
    sector_count = len(sector_syms)
    new_sector_pct = (current_sector_margin + est_margin) / net_liq * 100 if net_liq > 0 else 0

    if new_sector_pct > max_per_sector_pct:
        violations.append(f"LIMITE SETOR {sector}: {new_sector_pct:.1f}% (max={max_per_sector_pct}%). "
                          f"Ja tem: {', '.join(sector_syms)}")
    elif sector_count >= 3:
        warnings.append(f"CORRELACAO: {sector} ja tem {sector_count} underlyings ({', '.join(sector_syms)}). "
                        f"R11 recomenda max 3 correlacionados.")

    # Sizing adjustment warning
    if adj < 1.0:
        warnings.append(f"SIZING REDUZIDO: {sym} usa fator {adj:.0%} (vol extrema)")

    # Score too low
    if entry_score < 6:
        warnings.append(f"SCORE BAIXO: {entry_score}/17 — considerar nao entrar")

    return {
        "sym": sym,
        "name": NAMES.get(sym, sym),
        "direction": direction,
        "sector": sector,
        "entry_score": entry_score,
        "score_label": score_label,
        "score_mult": score_mult,
        "vol_adj": adj,
        "regime": regime_name,
        "regime_limit": max_active_pct,

        # Sizing result
        "recommended_contracts": adjusted_contracts,
        "butterfly_sold": butterfly_sold,
        "margin_per_contract": margin_per,
        "est_margin": est_margin,
        "est_risk": round(max_risk_per_trade, 0),
        "multiplier": OPT_MULTIPLIERS.get(sym, 100),

        # Limits
        "max_by_risk": max_by_risk,
        "max_by_available": max_by_available,
        "raw_before_adj": raw_contracts,

        # Capital state
        "net_liq": net_liq,
        "current_active_pct": current_active_pct,
        "max_active_pct": max_active_pct,
        "available_margin": round(available_margin, 0),
        "available_und": round(available_und, 0),
        "available_sector": round(available_sector, 0),
        "effective_available": round(effective_available, 0),

        # Existing exposure
        "existing_und_margin": current_und_margin,
        "existing_sector_margin": current_sector_margin,
        "sector_syms": sector_syms,
        "positions_count": state["positions_count"],

        # Checks
        "violations": violations,
        "warnings": warnings,
        "can_trade": len(violations) == 0 and adjusted_contracts > 0,
    }


def main():
    print("=" * 60)
    print("POSITION SIZING ENGINE")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    portfolio = jload(PROC / "ibkr_portfolio.json")
    skill_data = jload(BASE / "pipeline" / "trade_skill_base.json")
    options_data = jload(PROC / "options_chain.json")

    if not portfolio.get("positions"):
        print("\n  Portfolio nao disponivel.")
        return

    state = get_portfolio_state(portfolio)
    regime_info = detect_regime(state, options_data)

    args = sys.argv[1:]

    # No args: show capital summary
    if not args or (len(args) == 1 and args[0].lower() == "summary"):
        _print_capital_summary(state, skill_data, regime_info)
        return

    # Specific sizing: SYM DIRECTION SCORE
    if len(args) >= 3:
        sym = args[0].upper()
        direction = args[1].upper()
        score = int(args[2])

        if sym not in NAMES:
            print(f"  [ERR] Unknown underlying: {sym}")
            return
        if direction not in ("PUT", "CALL"):
            print(f"  [ERR] Direction must be PUT or CALL")
            return

        sizing = calculate_sizing(sym, direction, score, state, skill_data, regime_info)
        _print_sizing(sizing, state)

        # Save
        output = {
            "generated_at": datetime.now().isoformat(),
            "portfolio_state": {
                "net_liq": state["net_liq"],
                "active_pct": state["active_pct"],
                "positions": state["positions_count"],
            },
            "sizing": sizing,
        }
        with open(OUT, "w") as f:
            json.dump(output, f, indent=2, default=str)
        print(f"\n  [SAVED] {OUT}")
        return

    # Default: summary
    _print_capital_summary(state, skill_data, regime_info)


def _print_capital_summary(state, skill_data, regime_info=None):
    cm = skill_data.get("capital_management", {})
    base_max = cm.get("active_trades_max_pct", 60)

    regime_name = regime_info["regime"] if regime_info else "NORMAL"
    regime_limit = regime_info["limit_pct"] if regime_info else base_max
    max_active_usd = state["net_liq"] * regime_limit / 100
    available = max(0, max_active_usd - state["total_margin_est"])
    over = state["total_margin_est"] - max_active_usd

    print(f"\n  CAPITAL SUMMARY")
    print(f"  {'='*50}")

    # Regime display
    if regime_name == "VEGA":
        print(f"  Regime:            VEGA (limite expandido {base_max}% -> {regime_limit}%)")
        for r in regime_info.get("reasons", []):
            print(f"    -> {r}")
    else:
        print(f"  Regime:            NORMAL (limite {regime_limit}%)")

    print(f"  {'='*50}")
    print(f"  Net Liquidation:   ${state['net_liq']:>12,.2f}")
    print(f"  Buying Power:      ${state['buying_power']:>12,.2f}")
    print(f"  Cash:              ${state['cash']:>12,.2f}")
    print(f"  Gross Position:    ${state['gross_pos']:>12,.2f}")
    print(f"  Est. Margin Used:  ${state['total_margin_est']:>12,.2f} ({state['active_pct']:.1f}%)")
    print(f"  Limite ({regime_name} {regime_limit}%): ${max_active_usd:>12,.2f}")

    if over > 0:
        print(f"  Excesso:           ${over:>12,.2f} ({state['active_pct'] - regime_limit:.1f}pp acima)")
        print(f"  Para liberar:      Fechar ~${over:,.0f} em margem (~1 posicao)")
    else:
        print(f"  Disponivel:        ${available:>12,.2f}")

    print(f"  Positions:         {state['positions_count']}/12")

    print(f"\n  PER UNDERLYING:")
    for sym in sorted(state["by_sym"]):
        d = state["by_sym"][sym]
        pct = d["margin_est"] / state["net_liq"] * 100 if state["net_liq"] > 0 else 0
        print(f"    {sym:>4}: margin~${d['margin_est']:>10,.0f} ({pct:>5.1f}%) | "
              f"sold={d['sold']:>3} bought={d['bought']:>3}")

    print(f"\n  PER SECTOR:")
    for sector in sorted(state["by_sector"]):
        d = state["by_sector"][sector]
        pct = d["margin_est"] / state["net_liq"] * 100 if state["net_liq"] > 0 else 0
        syms_str = ", ".join(sorted(d["syms"]))
        print(f"    {sector:>10}: margin~${d['margin_est']:>10,.0f} ({pct:>5.1f}%) | {syms_str}")


def _print_sizing(sizing, state):
    sym = sizing["sym"]
    print(f"\n  {'='*55}")
    print(f"  SIZING: {sym} ({sizing['name']}) {sizing['direction']} — Score {sizing['entry_score']}")
    print(f"  {'='*55}")

    # Violations first
    if sizing["violations"]:
        print(f"\n  {'!'*50}")
        print(f"  VIOLACOES (trade BLOQUEADO):")
        for v in sizing["violations"]:
            print(f"    X {v}")
        print(f"  {'!'*50}")

    # Warnings
    if sizing["warnings"]:
        print(f"\n  AVISOS:")
        for w in sizing["warnings"]:
            print(f"    ! {w}")

    # Result
    can = "SIM" if sizing["can_trade"] else "NAO"
    print(f"\n  Pode entrar? {can}")

    if sizing["can_trade"]:
        print(f"\n  RECOMENDACAO:")
        print(f"    Contratos vendidos:  {sizing['recommended_contracts']}")
        print(f"    Estrutura 22x22:     ~{sizing['butterfly_sold']} sold / ~{sizing['butterfly_sold']} bought")
        print(f"    Margem estimada:     ${sizing['est_margin']:,.0f}")
        print(f"    Risco maximo:        ${sizing['est_risk']:,.0f} (2.5% do capital)")
        print(f"    Multiplicador:       {sizing['multiplier']}")

    print(f"\n  CALCULO:")
    print(f"    Score sizing:        {sizing['score_label']}")
    print(f"    Fator vol:           {sizing['vol_adj']:.0%}")
    print(f"    Max por risco:       {sizing['max_by_risk']} contratos")
    print(f"    Max por capital:     {sizing['max_by_available']} contratos")
    print(f"    Raw (pre-ajuste):    {sizing['raw_before_adj']} contratos")
    print(f"    Final (ajustado):    {sizing['recommended_contracts']} contratos")

    print(f"\n  LIMITES ({sizing['regime']} regime, limite={sizing['regime_limit']}%):")
    print(f"    Capital ativo:       {sizing['current_active_pct']:.1f}% / {sizing['regime_limit']}%")
    print(f"    Disponivel total:    ${sizing['available_margin']:,.0f}")
    print(f"    Disponivel {sym}:     ${sizing['available_und']:,.0f} (max 15%)")
    print(f"    Disponivel {sizing['sector']}:  ${sizing['available_sector']:,.0f} (max 30%)")

    # Existing exposure
    if sizing["existing_und_margin"] > 0:
        print(f"\n  EXPOSICAO EXISTENTE:")
        print(f"    {sym}: ${sizing['existing_und_margin']:,.0f} em margem")
    if sizing["sector_syms"]:
        print(f"    Setor {sizing['sector']}: {', '.join(sizing['sector_syms'])}")


if __name__ == "__main__":
    main()
