#!/usr/bin/env python3
"""
AgriMacro — Commodity DNA Generator

For each of the 18 commodities, builds a "DNA profile" that ranks
the key drivers by current signal strength. Combines:
  - COT positioning (cot_index, 52w)
  - Spreads/ratios (z-score, regime)
  - Seasonality (current month avg return)
  - Options (IV, IV rank, skew, term structure)
  - Correlations (strongest peers)
  - Price momentum (20d, 60d, 200d)
  - Trade history (predictability, clean WR)

Output: pipeline/commodity_dna.json
        + copy to agrimacro-dash/public/data/processed/commodity_dna.json

Run:
  python pipeline/commodity_dna.py
"""

import json
import math
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent.parent
PROC = BASE / "agrimacro-dash" / "public" / "data" / "processed"
OUT_PIPELINE = BASE / "pipeline" / "commodity_dna.json"
OUT_DASH = PROC / "commodity_dna.json"

SYMS = ["ZC", "ZS", "ZW", "KE", "ZM", "ZL",
        "LE", "GF", "HE",
        "SB", "KC", "CT", "CC",
        "CL", "NG",
        "GC", "SI",
        "DX"]

NAMES = {
    "ZC": "Milho", "ZS": "Soja", "ZW": "Trigo CBOT", "KE": "Trigo KC",
    "ZM": "Farelo de Soja", "ZL": "Oleo de Soja",
    "LE": "Boi Gordo", "GF": "Feeder Cattle", "HE": "Suino",
    "SB": "Acucar", "KC": "Cafe", "CT": "Algodao", "CC": "Cacau",
    "CL": "Petroleo", "NG": "Gas Natural",
    "GC": "Ouro", "SI": "Prata",
    "DX": "Dolar Index",
}

SECTORS = {
    "ZC": "Graos", "ZS": "Graos", "ZW": "Graos", "KE": "Graos",
    "ZM": "Graos", "ZL": "Graos",
    "LE": "Pecuaria", "GF": "Pecuaria", "HE": "Pecuaria",
    "SB": "Softs", "KC": "Softs", "CT": "Softs", "CC": "Softs",
    "CL": "Energia", "NG": "Energia",
    "GC": "Metais", "SI": "Metais",
    "DX": "FX",
}


def jload(path):
    for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            with open(path, encoding=enc) as f:
                return json.load(f)
        except Exception:
            continue
    return {}


def safe(v, default=None):
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return default
    return v


def compute_momentum(bars, window):
    """Compute price momentum over window days."""
    if not bars or len(bars) < window + 1:
        return None
    current = bars[-1].get("close", 0)
    past = bars[-(window + 1)].get("close", 0)
    if past <= 0:
        return None
    return round(((current - past) / past) * 100, 2)


def signal_from_value(value, thresholds, labels):
    """Map value to signal label based on thresholds."""
    for thresh, label in zip(thresholds, labels):
        if value <= thresh:
            return label
    return labels[-1]


def build_dna():
    print("=" * 60)
    print("COMMODITY DNA — Building profiles for all underlyings")
    print("=" * 60)

    # Load all data sources
    prices = jload(PROC / "price_history.json")
    cot = jload(PROC / "cot.json")
    spreads = jload(PROC / "spreads.json")
    seasonality = jload(PROC / "seasonality.json")
    options = jload(PROC / "options_chain.json")
    correlations = jload(PROC / "correlations.json")
    cross = jload(BASE / "pipeline" / "cross_analysis.json")
    skill = jload(BASE / "pipeline" / "trade_skill_base.json")

    cur_month = datetime.now().month  # 1-12
    month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    # Pre-index
    pred_data = cross.get("underlying_predictability", {})
    best_unds = {u["sym"]: u for u in skill.get("best_underlyings", [])}
    spread_map = spreads.get("spreads", {})
    corr_matrix = correlations.get("matrix", {})

    # Map spreads to commodities
    spread_involves = defaultdict(list)
    spread_commodity_hints = {
        "soy_crush": ["ZS", "ZM", "ZL"],
        "ke_zw": ["KE", "ZW"],
        "zl_cl": ["ZL", "CL"],
        "feedlot": ["GF", "ZC", "LE"],
        "zc_zm": ["ZC", "ZM"],
        "zc_zs": ["ZC", "ZS"],
        "cattle_crush": ["LE", "GF", "HE"],
        "feed_wheat": ["ZW", "ZC"],
    }
    for sp_name, syms in spread_commodity_hints.items():
        if sp_name in spread_map:
            for s in syms:
                spread_involves[s].append(sp_name)

    commodities = {}

    for sym in SYMS:
        drivers = []

        # ── 1. COT Positioning ──
        cot_data = cot.get("commodities", {}).get(sym, {})
        dis = cot_data.get("disaggregated", {})
        leg = cot_data.get("legacy", {})

        cot_idx = safe(dis.get("cot_index"))
        cot_52w = safe(dis.get("cot_index_52w"))

        if cot_idx is not None:
            if cot_idx >= 80:
                signal = "CROWDED LONG"
                strength = 3
            elif cot_idx <= 20:
                signal = "CROWDED SHORT"
                strength = 3
            elif cot_idx >= 65:
                signal = "Longs acumulando"
                strength = 2
            elif cot_idx <= 35:
                signal = "Shorts acumulando"
                strength = 2
            else:
                signal = "Neutro"
                strength = 1

            drivers.append({
                "driver": "COT Positioning",
                "value": f"Index={cot_idx:.0f}/100 (52w={cot_52w:.0f})" if cot_52w else f"Index={cot_idx:.0f}/100",
                "signal": signal,
                "strength": strength,
                "direction": "BEARISH" if cot_idx >= 80 else "BULLISH" if cot_idx <= 20 else "NEUTRAL",
            })

        # ── 2. Spreads / Ratios ──
        for sp_name in spread_involves.get(sym, []):
            sp = spread_map.get(sp_name, {})
            zscore = safe(sp.get("zscore_1y"))
            regime = sp.get("regime", "")
            if zscore is not None:
                abs_z = abs(zscore)
                if abs_z >= 2.5:
                    strength = 3
                elif abs_z >= 1.5:
                    strength = 2
                else:
                    strength = 1

                direction = "BULLISH" if zscore > 0 else "BEARISH" if zscore < 0 else "NEUTRAL"
                drivers.append({
                    "driver": f"Spread: {sp.get('name', sp_name)}",
                    "value": f"Z={zscore:.2f} ({regime})",
                    "signal": sp.get("signal_now", regime),
                    "strength": strength,
                    "direction": direction,
                })

        # ── 3. Seasonality ──
        seas = seasonality.get(sym, {})
        monthly = seas.get("monthly_returns", [])
        if monthly and cur_month <= len(monthly):
            mr = monthly[cur_month - 1]
            val = mr if isinstance(mr, (int, float)) else mr.get("avg", 0) if isinstance(mr, dict) else 0
            pos_pct = mr.get("positive_pct") if isinstance(mr, dict) else None

            if abs(val) >= 3:
                strength = 3
            elif abs(val) >= 1.5:
                strength = 2
            else:
                strength = 1

            direction = "BULLISH" if val > 0.5 else "BEARISH" if val < -0.5 else "NEUTRAL"
            pct_str = f", {pos_pct:.0f}% positivo" if pos_pct else ""
            drivers.append({
                "driver": f"Sazonalidade ({month_names[cur_month]})",
                "value": f"{val:+.2f}%{pct_str}",
                "signal": f"Historicamente {'alta' if val > 0 else 'queda'} em {month_names[cur_month]}",
                "strength": strength,
                "direction": direction,
            })

        # ── 4. Options: IV, Skew, Term Structure ──
        opt = options.get("underlyings", {}).get(sym, {})
        ivr = opt.get("iv_rank", {})
        skew = opt.get("skew", {})
        term = opt.get("term_structure", {})

        cur_iv = safe(ivr.get("current_iv"))
        if cur_iv is not None:
            iv_pct = cur_iv * 100
            if iv_pct >= 50:
                signal = f"IV ALTA ({iv_pct:.0f}%) — premium selling favoravel"
                strength = 3
                direction = "SELL_VOL"
            elif iv_pct >= 30:
                signal = f"IV MODERADA ({iv_pct:.0f}%)"
                strength = 2
                direction = "NEUTRAL"
            else:
                signal = f"IV BAIXA ({iv_pct:.0f}%) — evitar venda de premium"
                strength = 2
                direction = "BUY_VOL"

            drivers.append({
                "driver": "IV / Volatilidade",
                "value": f"IV={iv_pct:.1f}%",
                "signal": signal,
                "strength": strength,
                "direction": direction,
            })

        skew_pct = safe(skew.get("skew_pct"))
        if skew_pct is not None and abs(skew_pct) > 5:
            drivers.append({
                "driver": "Skew (put vs call IV)",
                "value": f"{skew_pct:+.1f}%",
                "signal": "Puts mais caros — demanda por protecao" if skew_pct > 0 else "Calls mais caros — demanda por upside",
                "strength": 2 if abs(skew_pct) > 10 else 1,
                "direction": "BEARISH" if skew_pct > 10 else "BULLISH" if skew_pct < -10 else "NEUTRAL",
            })

        term_struct = term.get("structure")
        if term_struct and term_struct != "FLAT":
            drivers.append({
                "driver": "Estrutura de termo IV",
                "value": term_struct,
                "signal": "Stress no curto prazo" if term_struct == "BACKWARDATION" else "Mercado calmo, IV sobe com prazo",
                "strength": 2,
                "direction": "BEARISH" if term_struct == "BACKWARDATION" else "NEUTRAL",
            })

        # ── 5. Price Momentum ──
        bars = prices.get(sym, [])
        if isinstance(bars, dict):
            bars = bars.get("history", [])

        mom_20 = compute_momentum(bars, 20)
        mom_60 = compute_momentum(bars, 60)
        mom_200 = compute_momentum(bars, 200)

        if mom_20 is not None:
            if abs(mom_20) >= 5:
                strength = 3
            elif abs(mom_20) >= 2:
                strength = 2
            else:
                strength = 1
            direction = "BULLISH" if mom_20 > 1 else "BEARISH" if mom_20 < -1 else "NEUTRAL"
            drivers.append({
                "driver": "Momentum (20d)",
                "value": f"{mom_20:+.1f}%",
                "signal": f"{'Alta' if mom_20 > 0 else 'Queda'} de {abs(mom_20):.1f}% em 20 dias",
                "strength": strength,
                "direction": direction,
            })

        # ── 6. Correlations ──
        sym_corr = corr_matrix.get(sym, {})
        if sym_corr:
            top_corr = sorted(sym_corr.items(), key=lambda x: abs(x[1]), reverse=True)[:3]
            corr_strs = [f"{k}({v:+.2f})" for k, v in top_corr if k != sym]
            if corr_strs:
                drivers.append({
                    "driver": "Correlacoes principais",
                    "value": ", ".join(corr_strs[:3]),
                    "signal": "Monitorar pares correlacionados",
                    "strength": 1,
                    "direction": "INFO",
                })

        # ── 7. Trade History (from cross analysis) ──
        pred = pred_data.get(sym, {})
        trade_perf = best_unds.get(sym, {})
        if pred:
            clean_wr = pred.get("clean_wr", 0)
            net = pred.get("total_net", 0)
            cycles = pred.get("cycles", 0)
            if cycles >= 3:
                if clean_wr >= 70:
                    signal = f"Historico forte: {clean_wr:.0f}% WR, net ${net:,.0f}"
                    strength = 3
                elif clean_wr >= 55:
                    signal = f"Historico ok: {clean_wr:.0f}% WR, net ${net:,.0f}"
                    strength = 2
                else:
                    signal = f"Historico fraco: {clean_wr:.0f}% WR, net ${net:,.0f}"
                    strength = 1

                drivers.append({
                    "driver": "Historico de trades",
                    "value": f"WR={clean_wr:.0f}% ({cycles} ciclos)",
                    "signal": signal,
                    "strength": strength,
                    "direction": "BULLISH" if net > 0 else "BEARISH",
                })

        # ── Sort drivers by strength ──
        drivers.sort(key=lambda d: d["strength"], reverse=True)

        # ── Composite signal ──
        bullish = sum(1 for d in drivers if d["direction"] == "BULLISH")
        bearish = sum(1 for d in drivers if d["direction"] == "BEARISH")
        strong_bull = sum(1 for d in drivers if d["direction"] == "BULLISH" and d["strength"] >= 2)
        strong_bear = sum(1 for d in drivers if d["direction"] == "BEARISH" and d["strength"] >= 2)

        if strong_bull >= 2 and strong_bear == 0:
            composite = "STRONG BULLISH"
        elif strong_bear >= 2 and strong_bull == 0:
            composite = "STRONG BEARISH"
        elif bullish > bearish + 1:
            composite = "BULLISH"
        elif bearish > bullish + 1:
            composite = "BEARISH"
        else:
            composite = "MIXED"

        last_price = bars[-1]["close"] if bars else None

        commodities[sym] = {
            "name": NAMES.get(sym, sym),
            "sector": SECTORS.get(sym, "Other"),
            "price": last_price,
            "composite_signal": composite,
            "bullish_drivers": bullish,
            "bearish_drivers": bearish,
            "total_drivers": len(drivers),
            "drivers_ranked": drivers,
            "momentum": {
                "20d": mom_20,
                "60d": mom_60,
                "200d": mom_200,
            },
        }

        # Log
        top_driver = drivers[0]["driver"] if drivers else "none"
        top_signal = drivers[0]["signal"][:50] if drivers else ""
        print(f"  {sym:>4} ({NAMES.get(sym,''):>14}): {composite:>16} | "
              f"{len(drivers)} drivers | top: {top_driver} — {top_signal}")

    # ── Build output ──
    output = {
        "generated_at": datetime.now().isoformat(),
        "description": "Commodity DNA — perfil de drivers rankeados por forca de sinal para cada commodity",
        "month": month_names[cur_month],
        "commodities": commodities,
    }

    # Save both locations
    for path in [OUT_PIPELINE, OUT_DASH]:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(output, f, indent=2, default=str)

    print(f"\n[SAVED] {OUT_PIPELINE}")
    print(f"[SAVED] {OUT_DASH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    build_dna()
