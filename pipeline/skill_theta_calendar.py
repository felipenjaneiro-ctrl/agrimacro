#!/usr/bin/env python3
"""
AgriMacro — Theta Calendar

Visual timeline of all positions with:
  - DTE countdown and theta phase (harvest / monitor / close / urgent)
  - WASDE windows marked for grains
  - Decision windows (when to act)
  - Next trade suggestions based on expiry cascade

Run:
  python pipeline/skill_theta_calendar.py
"""

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

BASE = Path(__file__).parent.parent
PROC = BASE / "agrimacro-dash" / "public" / "data" / "processed"
OUT = BASE / "pipeline" / "theta_calendar.json"

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

# WASDE release dates 2026 (approximate — typically 9-12th)
WASDE_DATES = [
    "2026-01-12", "2026-02-10", "2026-03-10", "2026-04-09",
    "2026-05-12", "2026-06-11", "2026-07-10", "2026-08-12",
    "2026-09-11", "2026-10-09", "2026-11-10", "2026-12-09",
]

MARGIN_PER = {
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


def get_dte_map(options_data):
    """Build map of contract code -> DTE from options chain."""
    dte_map = {}
    for sym, und in options_data.get("underlyings", {}).items():
        for exp_key, exp_data in und.get("expirations", {}).items():
            contract = exp_data.get("contract", "")
            dte = exp_data.get("days_to_exp", 0)
            if contract:
                dte_map[contract] = dte
                # Also store with symbol prefix for matching
                dte_map[(sym, contract)] = dte
    return dte_map


def resolve_dte(sym, contract_code, dte_map):
    """Resolve DTE for a position contract code."""
    # Direct match
    if contract_code in dte_map:
        return dte_map[contract_code]
    if (sym, contract_code) in dte_map:
        return dte_map[(sym, contract_code)]

    # Strip known IBKR option prefixes to get month+year suffix
    # OZLK6 -> K6, OZLM6 -> M6, LON6 -> N6, SOK6 -> K6, etc.
    prefixes = {
        "CL": "LO", "SI": "SO", "CC": "CO", "GC": "OG",
        "ZC": "OZC", "ZS": "OZS", "ZW": "OZW", "ZM": "OZM",
        "ZL": "OZL", "KE": "OKE", "NG": "LNE",
        "LE": "OLE", "GF": "OGF", "HE": "OHE",
        "SB": "OSB", "KC": "OKC", "CT": "OCT",
    }
    pref = prefixes.get(sym, "")
    stripped = contract_code
    if pref and contract_code.startswith(pref):
        stripped = contract_code[len(pref):]

    # Match stripped code against chain contracts
    for key, dte in dte_map.items():
        if isinstance(key, tuple) and key[0] == sym:
            chain_contract = key[1]
            chain_stripped = chain_contract
            if pref and chain_contract.startswith(pref):
                chain_stripped = chain_contract[len(pref):]
            # Also strip symbol prefix from chain: ZLK6 -> K6
            if chain_contract.startswith(sym):
                chain_stripped = chain_contract[len(sym):]
            if stripped == chain_stripped:
                return dte

    # Fallback: match last 2 chars (month+year)
    suffix = contract_code[-2:] if len(contract_code) >= 2 else ""
    if suffix and len(suffix) == 2 and suffix[0].isalpha():
        for key, dte in dte_map.items():
            if isinstance(key, tuple) and key[0] == sym and key[1].endswith(suffix):
                return dte
    return None


def theta_phase(dte):
    """Classify position into theta phase."""
    if dte is None:
        return "UNKNOWN", "?"
    if dte <= 5:
        return "CRITICAL", "Fechar HOJE — gamma extremo"
    elif dte <= 10:
        return "CLOSE", "Fechar esta semana"
    elif dte <= 14:
        return "CLOSE_TARGET", "Fechar se >= 50% profit"
    elif dte <= 21:
        return "DECISION", "Zona de decisao — monitorar target 50%"
    elif dte <= 30:
        return "HARVEST", "Theta acelerando — coleta maxima"
    elif dte <= 45:
        return "PRODUCTIVE", "Theta trabalhando — fase produtiva"
    elif dte <= 60:
        return "BUILDING", "Theta lento mas construindo"
    else:
        return "DISTANT", "Distante — paciencia"


def upcoming_wasde(n=3):
    """Get next N WASDE dates from today."""
    today = datetime.now().strftime("%Y-%m-%d")
    upcoming = [d for d in WASDE_DATES if d >= today]
    return upcoming[:n]


def run_theta_calendar():
    print("=" * 65)
    print("THETA CALENDAR — Position Timeline & Decision Windows")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')} ({datetime.now().strftime('%A')})")
    print("=" * 65)

    portfolio = jload(PROC / "ibkr_portfolio.json")
    options = jload(PROC / "options_chain.json")
    today = datetime.now()

    dte_map = get_dte_map(options)

    # Group positions by spread (sym + contract group)
    spreads = defaultdict(lambda: {
        "legs": [], "sold": 0, "bought": 0, "delta": 0, "theta": 0,
        "contract": "", "sym": "",
    })

    for p in portfolio.get("positions", []):
        sym = p.get("symbol", "")
        st = p.get("sec_type", "")
        if st not in ("FOP", "OPT", "FUT"):
            continue
        ls = p.get("local_symbol", "")
        contract = ls.split()[0] if " " in ls else ls
        # Group key: full contract code (preserves digit for DTE matching)
        grp = contract

        key = (sym, grp)
        spreads[key]["legs"].append(p)
        spreads[key]["sym"] = sym
        spreads[key]["contract"] = grp

        pos = p.get("position", 0)
        d = p.get("delta") or 0
        th = p.get("theta") or 0
        if pos < 0:
            spreads[key]["sold"] += abs(int(pos))
        else:
            spreads[key]["bought"] += int(pos)
        # Greeks SIGN-AWARE: pos > 0 = long, pos < 0 = short
        spreads[key]["delta"] += d * pos
        spreads[key]["theta"] += th * pos

    # Resolve DTEs and build timeline
    timeline = []
    for (sym, grp), data in spreads.items():
        dte = resolve_dte(sym, grp, dte_map)
        phase, phase_desc = theta_phase(dte)
        margin_est = data["sold"] * MARGIN_PER.get(sym, 2000)

        # Expiry date estimate
        expiry_date = None
        if dte is not None:
            expiry_date = (today + timedelta(days=dte)).strftime("%Y-%m-%d")

        # Check WASDE overlap for grains
        wasde_conflict = None
        if sym in GRAINS and dte is not None:
            for wd in WASDE_DATES:
                try:
                    wd_dt = datetime.strptime(wd, "%Y-%m-%d")
                    days_to_wasde = (wd_dt - today).days
                    if 0 <= days_to_wasde <= dte:
                        wasde_conflict = wd
                        break
                except Exception:
                    pass

        # Next expiry for new cycle suggestion
        next_exp = None
        und_exps = options.get("underlyings", {}).get(sym, {}).get("expirations", {})
        for ek, ed in sorted(und_exps.items()):
            ed_dte = ed.get("days_to_exp", 0)
            if dte is not None and ed_dte > dte + 15 and ed_dte >= 30:
                next_exp = {"contract": ed.get("contract", ""), "dte": ed_dte}
                break

        timeline.append({
            "sym": sym,
            "name": NAMES.get(sym, sym),
            "sector": SECTORS.get(sym, "?"),
            "contract": grp,
            "legs": len(data["legs"]),
            "sold": data["sold"],
            "bought": data["bought"],
            "dte": dte,
            "expiry_date": expiry_date,
            "phase": phase,
            "phase_desc": phase_desc,
            "delta": round(data["delta"], 2),
            "theta": round(data["theta"], 4),
            "margin_est": margin_est,
            "wasde_conflict": wasde_conflict,
            "next_expiry": next_exp,
        })

    # Sort by DTE (soonest first, unknowns last)
    timeline.sort(key=lambda t: t["dte"] if t["dte"] is not None else 9999)

    # ── Print Timeline ──
    print(f"\n  {'='*63}")
    print(f"  TIMELINE (sorted by DTE)")
    print(f"  {'='*63}")

    phase_icons = {
        "CRITICAL": "!!!",
        "CLOSE": "!! ",
        "CLOSE_TARGET": "!T ",
        "DECISION": " ! ",
        "HARVEST": " $ ",
        "PRODUCTIVE": " . ",
        "BUILDING": " ~ ",
        "DISTANT": "   ",
        "UNKNOWN": " ? ",
    }

    total_margin = 0
    total_theta = 0
    decision_window = []
    action_needed = []

    for t in timeline:
        icon = phase_icons.get(t["phase"], "   ")
        dte_str = f"{t['dte']:>3}d" if t["dte"] is not None else "  ?d"
        exp_str = t["expiry_date"] or "?"
        wasde_str = f"  !! WASDE {t['wasde_conflict']}" if t["wasde_conflict"] else ""

        print(f"\n  {icon} {t['sym']:>4} ({t['name']:>10}) | {t['contract']:>6} | "
              f"DTE={dte_str} | {t['phase']:>12} | exp={exp_str}")
        print(f"       {t['sold']:>3} sold / {t['bought']:>3} bought | {t['legs']} legs | "
              f"delta={t['delta']:+.2f} | theta={t['theta']:+.4f} | "
              f"margin~${t['margin_est']:,.0f}")
        print(f"       {t['phase_desc']}{wasde_str}")

        if t["next_expiry"]:
            ne = t["next_expiry"]
            print(f"       Proximo ciclo: {ne['contract']} (DTE={ne['dte']}d)")

        total_margin += t["margin_est"]
        total_theta += t["theta"]

        if t["phase"] in ("DECISION", "CLOSE_TARGET", "CLOSE", "CRITICAL"):
            decision_window.append(t)
        if t["phase"] in ("CLOSE", "CRITICAL"):
            action_needed.append(t)

    # ── Decision Windows ──
    if decision_window:
        print(f"\n  {'='*63}")
        print(f"  JANELAS DE DECISAO (acao necessaria)")
        print(f"  {'='*63}")
        for t in decision_window:
            print(f"  {t['sym']:>4} {t['contract']}: DTE={t['dte']}d — {t['phase_desc']}")
            if t["phase"] in ("CLOSE", "CRITICAL"):
                print(f"        ACAO: Fechar posicao. Se quiser continuar, analisar proximo ciclo.")
            elif t["phase"] == "CLOSE_TARGET":
                print(f"        ACAO: Checar P&L. Se >= 50% max profit -> FECHAR (R08)")
            elif t["phase"] == "DECISION":
                print(f"        ACAO: Monitorar diariamente. Target = 50% profit.")

    # ── WASDE Calendar ──
    wasde_upcoming = upcoming_wasde(3)
    print(f"\n  {'='*63}")
    print(f"  CALENDARIO WASDE (proximos)")
    print(f"  {'='*63}")
    for wd in wasde_upcoming:
        wd_dt = datetime.strptime(wd, "%Y-%m-%d")
        days = (wd_dt - today).days
        grain_positions = [t for t in timeline if t["sym"] in GRAINS and t["dte"] is not None]
        affected = [t["sym"] + " " + t["contract"] for t in grain_positions]
        affected_str = f" — afeta: {', '.join(affected)}" if affected else " — sem posicoes grain ativas"
        print(f"  {wd} ({wd_dt.strftime('%a')}) — em {days} dias{affected_str}")

    # ── Capital Rotation Suggestion ──
    print(f"\n  {'='*63}")
    print(f"  SUGESTAO DE ROTACAO")
    print(f"  {'='*63}")

    # Find positions that free capital soonest
    closeable = [t for t in timeline if t["phase"] in ("CLOSE", "CRITICAL", "CLOSE_TARGET", "DECISION")]
    productive = [t for t in timeline if t["phase"] in ("HARVEST", "PRODUCTIVE")]

    if closeable:
        t = closeable[0]
        freed = t["margin_est"]
        print(f"  1. Fechar {t['sym']} {t['contract']} (DTE={t['dte']}d) → libera ~${freed:,.0f}")
        if t.get("next_expiry"):
            ne = t["next_expiry"]
            print(f"     -> Novo ciclo disponivel: {ne['contract']} (DTE={ne['dte']}d)")
            print(f"     -> Executar: python pipeline/skill_pretrade_checklist.py {t['sym']} CALL")
            print(f"                  python pipeline/skill_pretrade_checklist.py {t['sym']} PUT")

        # What could we enter with freed capital?
        print(f"\n  2. Com ${freed:,.0f} liberado, analisar:")
        print(f"     -> python pipeline/skill_entry_timing.py (scan completo)")
        print(f"     -> python pipeline/skill_pretrade_checklist.py SI PUT (top oportunidade)")
    elif productive:
        print(f"  Todas as posicoes estao em fase produtiva. Nenhuma acao urgente.")
        print(f"  Proximo ponto de decisao: {productive[0]['sym']} {productive[0]['contract']} em ~{max(0,(productive[0]['dte'] or 30)-21)} dias")
    else:
        print(f"  Portfolio distante de janelas de decisao.")

    # ── Summary ──
    net_liq = float(portfolio.get("summary", {}).get("NetLiquidation", 0))
    active_pct = (total_margin / net_liq * 100) if net_liq > 0 else 0

    print(f"\n  {'='*63}")
    print(f"  RESUMO")
    print(f"  {'='*63}")
    print(f"  Posicoes: {len(timeline)}")
    print(f"  Margem total: ${total_margin:,.0f} ({active_pct:.1f}% do capital)")
    print(f"  Theta diario: ${total_theta:,.2f}")
    print(f"  Na janela de decisao: {len(decision_window)}")
    print(f"  Acao imediata: {len(action_needed)}")

    # ── Save ──
    out_path = OUT
    output = {
        "generated_at": datetime.now().isoformat(),
        "scan_date": today.strftime("%Y-%m-%d"),
        "positions": len(timeline),
        "total_margin": total_margin,
        "total_theta_daily": round(total_theta, 4),
        "in_decision_window": len(decision_window),
        "action_needed": len(action_needed),
        "timeline": [{
            "sym": t["sym"], "contract": t["contract"],
            "dte": t["dte"], "phase": t["phase"],
            "expiry_date": t["expiry_date"],
            "sold": t["sold"], "bought": t["bought"],
            "delta": t["delta"], "theta": t["theta"],
            "margin_est": t["margin_est"],
            "wasde_conflict": t["wasde_conflict"],
            "next_expiry": t["next_expiry"],
        } for t in timeline],
        "wasde_upcoming": wasde_upcoming,
        "decision_window": [{
            "sym": t["sym"], "contract": t["contract"],
            "dte": t["dte"], "phase": t["phase"],
            "action": t["phase_desc"],
        } for t in decision_window],
    }

    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\n  [SAVED] {out_path}")

    return output


if __name__ == "__main__":
    run_theta_calendar()
