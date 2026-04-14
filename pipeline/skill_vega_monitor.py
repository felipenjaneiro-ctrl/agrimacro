#!/usr/bin/env python3
"""
AgriMacro — Vega Monitor & Strategic Reserve Deploy Scanner

Monitors:
  - VIX level (panic threshold: 30+)
  - IV Rank per underlying (opportunity: 90%+, or IV > 50% absolute)
  - Strategic reserve availability ($340K in T-Bills)
  - Active vega exposure in portfolio

Identifies Vega play opportunities when conditions align:
  1. VIX > 30 OR underlying IV Rank > 90% OR IV > 50%
  2. Event catalyst identifiable
  3. At least 2 entry checklist filters confirmed
  4. Reserve available for deployment

Run:
  python pipeline/skill_vega_monitor.py
"""

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent.parent
PROC = BASE / "agrimacro-dash" / "public" / "data" / "processed"
OUT = BASE / "pipeline" / "vega_monitor.json"

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

MARGIN_PER = {
    "CL": 3500, "SI": 8000, "GF": 2500, "ZL": 1500, "CC": 3000,
    "ZC": 1200, "ZS": 2500, "ZW": 1800, "ZM": 1500, "SB": 1800,
    "KC": 4000, "LE": 2000, "HE": 1500, "NG": 5000, "GC": 5000,
    "KE": 1500, "CT": 1200,
}

# IV thresholds for vega opportunity
IV_HIGH_THRESHOLD = 50    # IV >= 50% = high premium opportunity
IV_EXTREME_THRESHOLD = 80  # IV >= 80% = extreme opportunity (rare)
VIX_PANIC_THRESHOLD = 30
VIX_ELEVATED_THRESHOLD = 25


def jload(path):
    for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            with open(path, encoding=enc) as f:
                return json.load(f)
        except Exception:
            continue
    return {}


def get_forward_shape(sym, ch):
    contracts = ch.get("contracts", {})
    items = [(n, c["bars"][-1]["close"]) for n, c in contracts.items()
             if c.get("commodity") == sym and c.get("bars") and c["bars"][-1].get("close", 0) > 0]
    if len(items) < 2:
        return "UNKNOWN", 0
    items.sort()
    diff = ((items[-1][1] - items[0][1]) / items[0][1]) * 100
    if diff < -3: return "STRONG_BACK", round(diff, 1)
    elif diff < -1: return "MILD_BACK", round(diff, 1)
    elif diff > 3: return "CONTANGO", round(diff, 1)
    return "FLAT", round(diff, 1)


def run_vega_monitor():
    print("=" * 65)
    print("VEGA MONITOR — Strategic Reserve Deploy Scanner")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 65)

    # Load data
    macro = jload(PROC / "macro_indicators.json")
    options = jload(PROC / "options_chain.json")
    portfolio = jload(PROC / "ibkr_portfolio.json")
    skill = jload(BASE / "pipeline" / "trade_skill_base.json")
    cross = jload(BASE / "pipeline" / "cross_analysis.json")
    contract_hist = jload(PROC / "contract_history.json")
    cot = jload(PROC / "cot.json")

    cm = skill.get("capital_management", {})
    reserve_config = cm.get("strategic_reserve", {})
    vega_config = cm.get("vega_opportunity", {})
    net_liq = float(portfolio.get("summary", {}).get("NetLiquidation", 0))

    # ── VIX Status ──
    vix_data = macro.get("vix", {})
    vix_value = vix_data.get("value", 0)
    vix_change = vix_data.get("change_pct", 0)
    vix_date = vix_data.get("date", "?")

    if vix_value >= VIX_PANIC_THRESHOLD:
        vix_level = "PANIC"
    elif vix_value >= VIX_ELEVATED_THRESHOLD:
        vix_level = "ELEVATED"
    elif vix_value >= 20:
        vix_level = "CAUTIOUS"
    else:
        vix_level = "NORMAL"

    print(f"\n  VIX: {vix_value:.1f} ({vix_change:+.1f}%) — {vix_level} [{vix_date}]")

    # ── Strategic Reserve ──
    reserve_target = reserve_config.get("target_usd", 337500)
    reserve_current = reserve_config.get("current_value", 340000)
    max_deploy_per_event = vega_config.get("max_deploy_per_event", 170000)
    reserve_yield = reserve_config.get("yield_approx", "?")

    # Check if reserve has been partially deployed (estimate from portfolio cash)
    cash = float(portfolio.get("summary", {}).get("TotalCashValue", 0))
    # Reserve available = min(reserve_current, cash that isn't operational)
    operational_target = net_liq * cm.get("operational_cash_pct", 15) / 100
    reserve_available = max(0, reserve_current - max(0, operational_target - cash))
    # Cap at configured max
    reserve_available = min(reserve_available, reserve_current)
    deployable = min(reserve_available, max_deploy_per_event)

    print(f"\n  RESERVA ESTRATEGICA")
    print(f"  Target:      ${reserve_target:,.0f}")
    print(f"  Atual:       ${reserve_current:,.0f} ({reserve_yield})")
    print(f"  Disponivel:  ${reserve_available:,.0f}")
    print(f"  Max deploy:  ${deployable:,.0f} por evento")

    # ── Current Vega Exposure ──
    total_vega = 0
    active_syms = set()
    for p in portfolio.get("positions", []):
        if p.get("sec_type") in ("FOP", "FUT"):
            v = p.get("vega") or 0
            pos = abs(p.get("position", 0))
            total_vega += v * pos
            if p.get("position", 0) != 0:
                active_syms.add(p["symbol"])

    print(f"\n  VEGA EXPOSURE ATUAL")
    print(f"  Portfolio vega: {total_vega:+.2f}")
    print(f"  Posicoes ativas: {', '.join(sorted(active_syms))}")

    # ── Scan all underlyings for IV opportunities ──
    print(f"\n  {'='*60}")
    print(f"  IV SCAN — Todas as commodities")
    print(f"  {'='*60}")

    iv_opportunities = []
    all_ivs = []

    for sym in sorted(options.get("underlyings", {}).keys()):
        und = options["underlyings"][sym]
        ivr = und.get("iv_rank", {})
        cur_iv = ivr.get("current_iv")
        rank_52w = ivr.get("rank_52w")
        skew = und.get("skew", {}).get("skew_pct")
        term = und.get("term_structure", {}).get("structure")

        if cur_iv is None:
            continue

        iv_pct = cur_iv * 100
        all_ivs.append({"sym": sym, "iv": iv_pct, "rank": rank_52w, "skew": skew, "term": term})

        # Classification
        if iv_pct >= IV_EXTREME_THRESHOLD:
            level = "EXTREME"
        elif iv_pct >= IV_HIGH_THRESHOLD:
            level = "HIGH"
        elif iv_pct >= 35:
            level = "ELEVATED"
        elif iv_pct >= 20:
            level = "NORMAL"
        else:
            level = "LOW"

        icon = "!!!" if level == "EXTREME" else "!! " if level == "HIGH" else "!  " if level == "ELEVATED" else "   "
        rank_str = f"Rank={rank_52w:.0f}%" if rank_52w is not None else "Rank=building"
        skew_str = f"Skew={skew:+.1f}%" if skew is not None else ""
        print(f"  {icon} {sym:>4} ({NAMES.get(sym,''):>10}): IV={iv_pct:>5.1f}% | "
              f"{rank_str:>16} | {level:>8} | {term or '?':>15} {skew_str}")

    # ── Identify Vega Play Opportunities ──
    print(f"\n  {'='*60}")
    print(f"  OPORTUNIDADES DE VEGA PLAY")
    print(f"  {'='*60}")

    vega_opportunities = []
    best_unds = {u["sym"]: u for u in skill.get("best_underlyings", [])}
    pred_data = cross.get("underlying_predictability", {})

    for iv_item in sorted(all_ivs, key=lambda x: x["iv"], reverse=True):
        sym = iv_item["sym"]
        iv_pct = iv_item["iv"]
        rank = iv_item["rank"]
        term = iv_item["term"]

        # Must have high IV
        if iv_pct < 35:
            continue

        # Check forward curve
        shape, diff = get_forward_shape(sym, contract_hist)

        # Score the opportunity
        opp_score = 0
        opp_notes = []
        opp_blockers = []

        # IV level score
        if iv_pct >= IV_EXTREME_THRESHOLD:
            opp_score += 30
            opp_notes.append(f"IV EXTREMA {iv_pct:.0f}% (+30)")
        elif iv_pct >= IV_HIGH_THRESHOLD:
            opp_score += 20
            opp_notes.append(f"IV ALTA {iv_pct:.0f}% (+20)")
        elif iv_pct >= 35:
            opp_score += 10
            opp_notes.append(f"IV ELEVADA {iv_pct:.0f}% (+10)")

        # VIX amplifier
        if vix_value >= VIX_PANIC_THRESHOLD:
            opp_score += 15
            opp_notes.append(f"VIX PANIC {vix_value:.0f} (+15)")
        elif vix_value >= VIX_ELEVATED_THRESHOLD:
            opp_score += 8
            opp_notes.append(f"VIX ELEVATED {vix_value:.0f} (+8)")

        # Trade history
        net_hist = best_unds.get(sym, {}).get("net_total", 0)
        if net_hist > 50000:
            opp_score += 10
            opp_notes.append(f"Historico +${net_hist:,.0f} (+10)")
        elif net_hist > 0:
            opp_score += 5
            opp_notes.append(f"Historico +${net_hist:,.0f} (+5)")
        elif net_hist < -50000:
            opp_score -= 10
            opp_notes.append(f"Historico ${net_hist:,.0f} (-10)")

        # Predictability
        pred = pred_data.get(sym, {})
        clean_wr = pred.get("clean_wr", 50)
        if clean_wr >= 65:
            opp_score += 5
            opp_notes.append(f"Clean WR={clean_wr:.0f}% (+5)")

        # COT alignment for direction
        dis = cot.get("commodities", {}).get(sym, {}).get("disaggregated", {})
        cot_idx = dis.get("cot_index")

        # Determine best direction for selling
        direction = "PUT"  # default for high IV = sell puts
        if cot_idx is not None:
            if cot_idx > 80:
                direction = "CALL"  # crowded long = sell calls
                opp_score += 5
                opp_notes.append(f"COT={cot_idx:.0f} crowded long -> SELL CALL (+5)")
            elif cot_idx < 20:
                direction = "PUT"
                opp_score += 5
                opp_notes.append(f"COT={cot_idx:.0f} crowded short -> SELL PUT (+5)")

        # Forward curve check (blocker)
        if direction == "PUT" and shape == "STRONG_BACK":
            opp_blockers.append(f"Curva BACKWARDATION ({diff:+.1f}%) bloqueia PUT selling")
            direction = "CALL"  # flip to call if put blocked
        if direction == "CALL" and shape == "CONTANGO" and diff > 10:
            opp_blockers.append(f"Curva CONTANGO forte ({diff:+.1f}%) bloqueia CALL selling")

        # Already active?
        already_active = sym in active_syms
        if already_active:
            opp_score -= 5
            opp_notes.append(f"Ja ativo no portfolio (-5)")

        # Margin estimate
        margin_needed = 10 * MARGIN_PER.get(sym, 3000)  # ~10 contracts as base size
        can_deploy = deployable >= margin_needed

        if opp_blockers:
            continue  # Skip blocked opportunities

        if opp_score >= 15:  # Minimum threshold for vega opportunity
            vega_opportunities.append({
                "sym": sym,
                "name": NAMES.get(sym, sym),
                "direction": direction,
                "iv_pct": round(iv_pct, 1),
                "rank_52w": rank,
                "score": opp_score,
                "notes": opp_notes,
                "margin_est": margin_needed,
                "can_deploy": can_deploy,
                "already_active": already_active,
                "curve": f"{shape} ({diff:+.1f}%)",
                "term_structure": term,
            })

    # Sort by score
    vega_opportunities.sort(key=lambda x: x["score"], reverse=True)

    if not vega_opportunities:
        print(f"\n  Nenhuma oportunidade de vega play hoje.")
        print(f"  Condicoes: IV < 35% em todos os underlyings e VIX < 25")
    else:
        for i, opp in enumerate(vega_opportunities):
            active_tag = " [JA ATIVO]" if opp["already_active"] else ""
            deploy_tag = " [DEPLOY OK]" if opp["can_deploy"] else " [SEM RESERVA]"
            print(f"\n  {i+1}. {opp['sym']:>4} ({opp['name']:>10}) {opp['direction']} — "
                  f"Score {opp['score']} | IV={opp['iv_pct']:.0f}%{active_tag}{deploy_tag}")
            print(f"     Curva: {opp['curve']} | Term: {opp['term_structure'] or '?'}")
            print(f"     Margem ~${opp['margin_est']:,.0f} (10 contratos base)")
            for n in opp["notes"]:
                print(f"       {n}")

    # ── Alerts ──
    alerts = []
    if vix_value >= VIX_PANIC_THRESHOLD:
        alerts.append(f"VIX em PANICO ({vix_value:.0f}) — considerar deploy de reserva estrategica")
    if vix_value >= VIX_ELEVATED_THRESHOLD:
        alerts.append(f"VIX ELEVADO ({vix_value:.0f}) — monitorar para possivel deploy")

    high_iv_count = sum(1 for iv in all_ivs if iv["iv"] >= IV_HIGH_THRESHOLD)
    if high_iv_count >= 3:
        alerts.append(f"{high_iv_count} underlyings com IV >= {IV_HIGH_THRESHOLD}% — ambiente de alta vol generalizada")
    elif high_iv_count >= 1:
        extreme = [iv for iv in all_ivs if iv["iv"] >= IV_HIGH_THRESHOLD]
        extreme_strs = [iv["sym"] + "(" + str(round(iv["iv"])) + "%)" for iv in extreme]
        alerts.append("IV alta em: " + ", ".join(extreme_strs))

    if reserve_available < reserve_target * 0.5:
        alerts.append(f"Reserva abaixo de 50% do target (${reserve_available:,.0f} / ${reserve_target:,.0f})")

    # ── Summary ──
    print(f"\n  {'='*60}")
    print(f"  RESUMO VEGA MONITOR")
    print(f"  {'='*60}")
    print(f"  VIX:              {vix_value:.1f} ({vix_level})")
    print(f"  IV > 50%:         {sum(1 for iv in all_ivs if iv['iv'] >= 50)}/{len(all_ivs)} underlyings")
    print(f"  IV > 35%:         {sum(1 for iv in all_ivs if iv['iv'] >= 35)}/{len(all_ivs)} underlyings")
    print(f"  Oportunidades:    {len(vega_opportunities)}")
    print(f"  Reserva deploy:   ${deployable:,.0f}")
    print(f"  Portfolio vega:   {total_vega:+.2f}")

    # Save
    output = {
        "generated_at": datetime.now().isoformat(),
        "vix": {"value": vix_value, "level": vix_level, "change_pct": vix_change},
        "iv_scan": {
            "above_50": sum(1 for iv in all_ivs if iv["iv"] >= 50),
            "above_35": sum(1 for iv in all_ivs if iv["iv"] >= 35),
            "total": len(all_ivs),
            "highest": all_ivs[0] if all_ivs else None,
        },
        "reserve": {
            "target": reserve_target,
            "current": reserve_current,
            "available": round(reserve_available),
            "deployable": round(deployable),
        },
        "portfolio_vega": round(total_vega, 2),
        "opportunities": [{
            "sym": o["sym"], "direction": o["direction"],
            "iv_pct": o["iv_pct"], "score": o["score"],
            "notes": o["notes"], "can_deploy": o["can_deploy"],
            "already_active": o["already_active"],
        } for o in vega_opportunities],
        "alerts": alerts,
        "action_required": len(vega_opportunities) > 0,
    }

    out_path = OUT
    out_path.write_text(json.dumps(output, indent=2, default=str))
    print(f"\n  [SAVED] {out_path}")

    if alerts:
        print(f"\n  ALERTAS ATIVOS:")
        for a in alerts:
            print(f"    -> {a}")

    return output


if __name__ == "__main__":
    run_vega_monitor()
