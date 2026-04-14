#!/usr/bin/env python3
"""
AgriMacro — Pre-Trade Checklist (10 Filters)

Final GO / NO-GO decision before entering a trade.
Consolidates all skill engines into one binary decision.

F1.  Forward curve OK (MANDATORY — blocks if strong backwardation for PUTs)
F2.  IV >= 20% for premium selling (MANDATORY — no premium = no trade)
F3.  Correlation: max 3 underlyings per sector (R11)
F4.  Capital available under regime limit (60% normal, 65% VEGA)
F5.  Underlying has positive trade history (net > 0)
F6.  COT not extreme against direction
F7.  Seasonality not strongly against
F8.  DTE 25-70 days available in chain
F9.  Entry score >= 6 (from entry_timing)
F10. Not in WASDE window for grains

Decision: GO if no MANDATORY fails and >= 7/10 pass.
          CONDITIONAL GO if 5-6 pass (reduced size).
          NO-GO if < 5 pass or any MANDATORY fails.

Usage:
  python pipeline/skill_pretrade_checklist.py                # All default tests
  python pipeline/skill_pretrade_checklist.py SI PUT          # Specific
"""

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent.parent
PROC = BASE / "agrimacro-dash" / "public" / "data" / "processed"

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

GRAINS = {"ZC", "ZS", "ZW", "KE", "ZM", "ZL"}

MARGIN_PER_CONTRACT = {
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


def get_forward_curve_shape(sym, contract_hist):
    """Get forward curve shape for underlying."""
    contracts = contract_hist.get("contracts", {})
    by_comm = []
    for name, c in contracts.items():
        if c.get("commodity") != sym:
            continue
        bars = c.get("bars", [])
        if bars and bars[-1].get("close", 0) > 0:
            by_comm.append((name, bars[-1]["close"]))

    if len(by_comm) < 2:
        return "UNKNOWN", 0
    by_comm.sort(key=lambda x: x[0])
    front = by_comm[0][1]
    back = by_comm[-1][1]
    diff = ((back - front) / front) * 100
    if diff < -3:
        return "STRONG_BACKWARDATION", round(diff, 1)
    elif diff < -1:
        return "MILD_BACKWARDATION", round(diff, 1)
    elif diff > 3:
        return "CONTANGO", round(diff, 1)
    return "FLAT", round(diff, 1)


def run_pretrade_checklist(und, direction):
    """Run all 10 filters and produce GO/NO-GO decision."""

    # Load all data
    options = jload(PROC / "options_chain.json")
    cot_data = jload(PROC / "cot.json")
    seasonality = jload(PROC / "seasonality.json")
    contract_hist = jload(PROC / "contract_history.json")
    portfolio = jload(PROC / "ibkr_portfolio.json")
    cross = jload(BASE / "pipeline" / "cross_analysis.json")
    skill = jload(BASE / "pipeline" / "trade_skill_base.json")

    cur_month = datetime.now().month
    cur_day = datetime.now().day

    best_unds = {u["sym"]: u for u in skill.get("best_underlyings", [])}
    pred_data = cross.get("underlying_predictability", {})
    cm = skill.get("capital_management", {})

    # Current portfolio state
    active_syms = defaultdict(int)
    active_sectors = defaultdict(set)
    total_margin = 0
    for p in portfolio.get("positions", []):
        s = p.get("symbol", "")
        st = p.get("sec_type", "")
        if st not in ("FOP", "OPT", "FUT"):
            continue
        pos = p.get("position", 0)
        if pos < 0:
            active_syms[s] += abs(int(pos))
            total_margin += abs(int(pos)) * MARGIN_PER_CONTRACT.get(s, 2000)
        sector = SECTORS.get(s, "Other")
        active_sectors[sector].add(s)

    net_liq = float(portfolio.get("summary", {}).get("NetLiquidation", 0))
    active_pct = (total_margin / net_liq * 100) if net_liq > 0 else 0

    # Detect VEGA regime
    regime = "NORMAL"
    regime_limit = 60
    for s in active_syms:
        u = options.get("underlyings", {}).get(s, {})
        iv = u.get("iv_rank", {}).get("current_iv")
        if iv is not None and iv * 100 >= 40:
            regime = "VEGA"
            regime_limit = 65
            break

    filters = []

    # ══════════════════════════════════════════════════
    # F1. FORWARD CURVE (MANDATORY)
    # ══════════════════════════════════════════════════
    shape, diff_pct = get_forward_curve_shape(und, contract_hist)
    if direction == "PUT" and shape == "STRONG_BACKWARDATION":
        filters.append({
            "id": "F1", "name": "Curva Forward",
            "mandatory": True, "passed": False,
            "detail": f"STRONG BACKWARDATION ({diff_pct:+.1f}%) — mercado estressado, PUT selling perigoso",
        })
    elif direction == "CALL" and shape == "CONTANGO" and diff_pct > 10:
        filters.append({
            "id": "F1", "name": "Curva Forward",
            "mandatory": True, "passed": False,
            "detail": f"CONTANGO forte ({diff_pct:+.1f}%) — mercado em carry trade, CALL selling arriscado",
        })
    else:
        filters.append({
            "id": "F1", "name": "Curva Forward",
            "mandatory": True, "passed": True,
            "detail": f"OK ({shape} {diff_pct:+.1f}%)",
        })

    # ══════════════════════════════════════════════════
    # F2. IV >= 20% (MANDATORY for premium selling)
    # ══════════════════════════════════════════════════
    opt = options.get("underlyings", {}).get(und, {})
    ivr = opt.get("iv_rank", {})
    cur_iv = ivr.get("current_iv")

    if cur_iv is not None:
        iv_pct = cur_iv * 100
        if iv_pct >= 20:
            filters.append({
                "id": "F2", "name": "IV >= 20%",
                "mandatory": True, "passed": True,
                "detail": f"IV = {iv_pct:.0f}% — premium suficiente",
            })
        else:
            filters.append({
                "id": "F2", "name": "IV >= 20%",
                "mandatory": True, "passed": False,
                "detail": f"IV = {iv_pct:.0f}% — premium insuficiente para venda de opcoes",
            })
    else:
        # No IV data — treat as unknown, not mandatory fail
        filters.append({
            "id": "F2", "name": "IV >= 20%",
            "mandatory": True, "passed": True,
            "detail": "[WARN] IV data nao disponivel — verificar manualmente no TWS",
        })

    # ══════════════════════════════════════════════════
    # F3. CORRELATION: max 3 per sector (R11)
    # ══════════════════════════════════════════════════
    sector = SECTORS.get(und, "Other")
    sector_active = active_sectors.get(sector, set())
    sector_count = len(sector_active)
    # Don't count self if already in
    would_be = sector_active | {und}

    if len(would_be) > 3:
        filters.append({
            "id": "F3", "name": "Correlacao R11",
            "mandatory": False, "passed": False,
            "detail": f"Setor {sector} teria {len(would_be)} underlyings ({', '.join(sorted(would_be))}). Max=3.",
        })
    elif len(would_be) >= 3 and und not in sector_active:
        filters.append({
            "id": "F3", "name": "Correlacao R11",
            "mandatory": False, "passed": True,
            "detail": f"Setor {sector}: {len(would_be)} underlyings ({', '.join(sorted(would_be))}). No limite — OK mas sem margem.",
        })
    else:
        existing = ", ".join(sorted(sector_active)) if sector_active else "nenhum"
        filters.append({
            "id": "F3", "name": "Correlacao R11",
            "mandatory": False, "passed": True,
            "detail": f"Setor {sector}: ja ativo={existing}. Com {und} = {len(would_be)}/3. OK.",
        })

    # ══════════════════════════════════════════════════
    # F4. CAPITAL available under regime limit
    # ══════════════════════════════════════════════════
    if active_pct <= regime_limit:
        filters.append({
            "id": "F4", "name": f"Capital ({regime} {regime_limit}%)",
            "mandatory": False, "passed": True,
            "detail": f"Em uso: {active_pct:.1f}% / {regime_limit}%. Margem disponivel.",
        })
    else:
        over = active_pct - regime_limit
        filters.append({
            "id": "F4", "name": f"Capital ({regime} {regime_limit}%)",
            "mandatory": False, "passed": False,
            "detail": f"Em uso: {active_pct:.1f}% > {regime_limit}% (excede {over:.1f}pp). Fechar posicao para liberar.",
        })

    # ══════════════════════════════════════════════════
    # F5. UNDERLYING has positive history
    # ══════════════════════════════════════════════════
    und_hist = best_unds.get(und, {})
    net_total = und_hist.get("net_total", 0)

    if net_total > 0:
        filters.append({
            "id": "F5", "name": "Historico positivo",
            "mandatory": False, "passed": True,
            "detail": f"Net historico: ${net_total:,.0f} ({und_hist.get('trades', 0)} trades, WR={und_hist.get('win_rate', 0)*100:.0f}%)",
        })
    elif net_total < -50000:
        filters.append({
            "id": "F5", "name": "Historico positivo",
            "mandatory": False, "passed": False,
            "detail": f"HISTORICO NEGATIVO: ${net_total:,.0f}. Underlying com track record ruim.",
        })
    elif net_total < 0:
        filters.append({
            "id": "F5", "name": "Historico positivo",
            "mandatory": False, "passed": False,
            "detail": f"Historico levemente negativo: ${net_total:,.0f}. Cautela.",
        })
    else:
        filters.append({
            "id": "F5", "name": "Historico positivo",
            "mandatory": False, "passed": True,
            "detail": "Sem historico suficiente — neutro.",
        })

    # ══════════════════════════════════════════════════
    # F6. COT not extreme against
    # ══════════════════════════════════════════════════
    cot_sym = cot_data.get("commodities", {}).get(und, {})
    dis = cot_sym.get("disaggregated", {})
    cot_idx = dis.get("cot_index")

    if cot_idx is not None:
        if direction == "PUT" and cot_idx > 85:
            filters.append({
                "id": "F6", "name": "COT vs Direcao",
                "mandatory": False, "passed": False,
                "detail": f"COT idx={cot_idx:.0f} — CROWDED LONG. PUT selling contra posicionamento extremo.",
            })
        elif direction == "CALL" and cot_idx < 15:
            filters.append({
                "id": "F6", "name": "COT vs Direcao",
                "mandatory": False, "passed": False,
                "detail": f"COT idx={cot_idx:.0f} — CROWDED SHORT. CALL selling contra posicionamento extremo.",
            })
        else:
            filters.append({
                "id": "F6", "name": "COT vs Direcao",
                "mandatory": False, "passed": True,
                "detail": f"COT idx={cot_idx:.0f} — OK, nao extremo contra.",
            })
    else:
        filters.append({
            "id": "F6", "name": "COT vs Direcao",
            "mandatory": False, "passed": True,
            "detail": "[WARN] COT nao disponivel — dado ausente, tratado como neutro.",
        })

    # ══════════════════════════════════════════════════
    # F7. SEASONALITY not strongly against
    # ══════════════════════════════════════════════════
    seas = seasonality.get(und, {}).get("monthly_returns", [])
    if seas and cur_month <= len(seas):
        mr = seas[cur_month - 1]
        val = mr if isinstance(mr, (int, float)) else mr.get("avg", 0) if isinstance(mr, dict) else 0
        month_name = datetime.now().strftime("%b")

        if direction == "PUT" and val < -2:
            filters.append({
                "id": "F7", "name": f"Sazonalidade ({month_name})",
                "mandatory": False, "passed": False,
                "detail": f"Avg return {val:+.2f}% — historicamente cai forte. PUT selling arriscado.",
            })
        elif direction == "CALL" and val > 2:
            filters.append({
                "id": "F7", "name": f"Sazonalidade ({month_name})",
                "mandatory": False, "passed": False,
                "detail": f"Avg return {val:+.2f}% — historicamente sobe forte. CALL selling arriscado.",
            })
        else:
            filters.append({
                "id": "F7", "name": f"Sazonalidade ({month_name})",
                "mandatory": False, "passed": True,
                "detail": f"Avg return {val:+.2f}% — OK, nao fortemente contra.",
            })
    else:
        filters.append({
            "id": "F7", "name": "Sazonalidade",
            "mandatory": False, "passed": True,
            "detail": "Dados nao disponiveis — neutro.",
        })

    # ══════════════════════════════════════════════════
    # F8. DTE 25-70 available
    # ══════════════════════════════════════════════════
    exps = opt.get("expirations", {})
    best_dte = None
    for exp_key, exp_data in exps.items():
        dte = exp_data.get("days_to_exp", 0)
        if 25 <= dte <= 70:
            if best_dte is None or abs(dte - 45) < abs(best_dte - 45):
                best_dte = dte

    # Also check if underlying has active position with low DTE
    active_low_dte = False
    for p in portfolio.get("positions", []):
        if p.get("symbol") == und and p.get("sec_type") in ("FOP", "FUT"):
            # Check if this is a short DTE position
            ls = p.get("local_symbol", "")
            for ek, ed in exps.items():
                contract = ed.get("contract", "")
                if contract and contract in ls and ed.get("days_to_exp", 99) <= 15:
                    active_low_dte = True
                    break

    if best_dte:
        detail = f"DTE={best_dte}d disponivel — sweet spot"
        if active_low_dte:
            detail += f". ATENCAO: posicao ativa {und} com DTE <= 15 (fechar antes de abrir novo ciclo)"
        filters.append({
            "id": "F8", "name": "DTE 25-70d",
            "mandatory": False, "passed": True,
            "detail": detail,
        })
    elif exps:
        dtes = [e.get("days_to_exp", 0) for e in exps.values()]
        closest = min(dtes, key=lambda d: abs(d - 45)) if dtes else 0
        filters.append({
            "id": "F8", "name": "DTE 25-70d",
            "mandatory": False, "passed": False,
            "detail": f"Melhor DTE disponivel: {closest}d (fora da janela 25-70d).",
        })
    else:
        filters.append({
            "id": "F8", "name": "DTE 25-70d",
            "mandatory": False, "passed": False,
            "detail": "[WARN] Sem dados de options chain — executar collect_options_chain.py.",
        })

    # ══════════════════════════════════════════════════
    # F9. Entry score >= 6
    # ══════════════════════════════════════════════════
    pred = pred_data.get(und, {})
    clean_wr = pred.get("clean_wr", 50)
    predictability = pred.get("predictability", 0)

    if clean_wr >= 60 and predictability >= 0.3:
        filters.append({
            "id": "F9", "name": "Predictability",
            "mandatory": False, "passed": True,
            "detail": f"Clean WR={clean_wr:.0f}%, pred={predictability:.3f} — historico confiavel.",
        })
    elif clean_wr >= 50:
        filters.append({
            "id": "F9", "name": "Predictability",
            "mandatory": False, "passed": True,
            "detail": f"Clean WR={clean_wr:.0f}%, pred={predictability:.3f} — marginal mas OK.",
        })
    else:
        filters.append({
            "id": "F9", "name": "Predictability",
            "mandatory": False, "passed": False,
            "detail": f"Clean WR={clean_wr:.0f}%, pred={predictability:.3f} — historico fraco.",
        })

    # ══════════════════════════════════════════════════
    # F10. Not in WASDE window (grains only)
    # ══════════════════════════════════════════════════
    if und in GRAINS and 8 <= cur_day <= 14:
        filters.append({
            "id": "F10", "name": "WASDE Window",
            "mandatory": False, "passed": False,
            "detail": f"Dia {cur_day} — janela WASDE (8-14). Esperar pos-report para grains.",
        })
    elif und in GRAINS:
        filters.append({
            "id": "F10", "name": "WASDE Window",
            "mandatory": False, "passed": True,
            "detail": f"Dia {cur_day} — fora da janela WASDE. OK para grains.",
        })
    else:
        filters.append({
            "id": "F10", "name": "WASDE Window",
            "mandatory": False, "passed": True,
            "detail": "N/A (nao e grain).",
        })

    # ══════════════════════════════════════════════════
    # DECISION
    # ══════════════════════════════════════════════════
    mandatory_fail = any(f["mandatory"] and not f["passed"] for f in filters)
    passed_count = sum(1 for f in filters if f["passed"])
    failed_count = sum(1 for f in filters if not f["passed"])

    if mandatory_fail:
        decision = "NO-GO"
        decision_reason = "Filtro OBRIGATORIO falhou"
    elif passed_count >= 7:
        decision = "GO"
        decision_reason = f"{passed_count}/10 filtros passaram"
    elif passed_count >= 5:
        decision = "CONDITIONAL GO"
        decision_reason = f"{passed_count}/10 filtros — sizing reduzido recomendado"
    else:
        decision = "NO-GO"
        decision_reason = f"Apenas {passed_count}/10 filtros — risco/retorno desfavoravel"

    # ── Print ──
    print(f"  {'='*58}")
    print(f"  PRE-TRADE CHECKLIST: {und} ({NAMES.get(und, und)}) {direction}")
    print(f"  {'='*58}")
    print()

    for f in filters:
        icon = "[X]" if f["passed"] else "[ ]"
        mandatory_tag = " *OBRIGATORIO*" if f["mandatory"] else ""
        status = "PASS" if f["passed"] else "FAIL"
        print(f"  {icon} {f['id']:>3} {f['name']:<24} {status:<4}{mandatory_tag}")
        print(f"        {f['detail']}")

    print()
    print(f"  {'='*58}")

    if decision == "GO":
        print(f"  >>> DECISAO: GO ({passed_count}/10) — {decision_reason}")
        print(f"  >>> Proximo passo: skill_position_sizing.py {und} {direction} <score>")
    elif decision == "CONDITIONAL GO":
        print(f"  >>> DECISAO: CONDITIONAL GO ({passed_count}/10) — {decision_reason}")
        print(f"  >>> Entrar com sizing 50-60% do normal")
    else:
        print(f"  >>> DECISAO: NO-GO ({passed_count}/10) — {decision_reason}")
        failed = [f for f in filters if not f["passed"]]
        if failed:
            print(f"  >>> Bloqueadores: {', '.join(f['id'] for f in failed)}")

    print(f"  {'='*58}")


def main():
    print("=" * 60)
    print("PRE-TRADE CHECKLIST (10 Filters)")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')} ({datetime.now().strftime('%A')})")
    print("=" * 60)

    tests = [
        ("SI", "PUT"),
        ("ZW", "CALL"),
        ("KE", "PUT"),
        ("GF", "CALL"),
        ("KC", "PUT"),
    ]

    if len(sys.argv) >= 3:
        tests = [(sys.argv[1].upper(), sys.argv[2].upper())]

    for und, direction in tests:
        run_pretrade_checklist(und, direction)
        print()


if __name__ == "__main__":
    main()
