#!/usr/bin/env python3
"""
collect_correlations.py - AgriMacro Multidimensional Correlation Engine
=======================================================================
Calculates Pearson correlation matrix (252-day daily returns),
lagged correlations, causal chains, and integrates COT, fundamentals,
macro, climate, and sentiment data for composite signal generation.

Inputs:
  price_history.json         -> price correlations
  cot.json                   -> Managed Money positioning
  grain_ratios.json          -> COT Index, STU, scorecards
  psd_ending_stocks.json     -> ending stocks
  macro_indicators.json      -> VIX, S&P 500, 10Y
  fedwatch.json              -> Fed cut probability
  bcb_data.json              -> BRL/USD
  weather_agro.json          -> ENSO status, climate alerts
  google_trends.json         -> search sentiment, spikes

Output: correlations.json
"""

import json
import math
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
ROOT_DIR = SCRIPT_DIR.parent
DATA_BASE = ROOT_DIR / "agrimacro-dash" / "public" / "data"
RAW_PATH = DATA_BASE / "raw" / "price_history.json"
PROC_PATH = DATA_BASE / "processed" / "price_history.json"
PROC = DATA_BASE / "processed"

OUTPUT_DIR = PROC if PROC.exists() else SCRIPT_DIR
OUTPUT_FILE = OUTPUT_DIR / "correlations.json"

SYMBOLS = [
    "ZC", "ZS", "ZW", "KE", "ZM", "ZL",
    "SB", "KC", "CT", "CC",
    "LE", "GF", "HE",
    "CL", "NG",
    "GC", "SI",
    "DX",
]

WINDOW = 252
LAGS = [1, 5, 10, 20]

# COT symbols to track (must exist in cot.json)
COT_SYMS = ["ZS", "ZC", "ZW", "CL", "GC", "SI", "LE", "CT", "KC", "SB"]

# Grains with STU data in grain_ratios
GRAIN_SYMS = {"ZC": "corn", "ZS": "soy", "ZW": "wheat"}

CAUSAL_CHAINS = [
    {
        "id": "energia_inflacao",
        "name": "Energia \u2192 Infla\u00e7\u00e3o \u2192 Metais",
        "description": "Petr\u00f3leo puxa custos e infla\u00e7\u00e3o percebida, fortalece d\u00f3lar e pressiona metais",
        "links": [("CL","NG",0,"positiva"),("CL","DX",5,"negativa"),("DX","GC",10,"negativa"),("GC","SI",0,"positiva")],
    },
    {
        "id": "crush_proteina",
        "name": "Complexo Soja \u2192 Prote\u00edna Animal",
        "description": "Soja se decompoe em farelo+oleo; farelo vs milho compete na ra\u00e7\u00e3o",
        "links": [("ZS","ZM",0,"positiva"),("ZS","ZL",0,"positiva"),("ZM","ZC",0,"positiva"),("ZC","LE",20,"negativa"),("ZC","HE",20,"negativa")],
    },
    {
        "id": "energia_acucar_etanol",
        "name": "Energia \u2192 A\u00e7\u00facar / Biodiesel",
        "description": "Petr\u00f3leo alto favorece etanol (SB sobe) e biodiesel (\u00f3leo de soja sobe)",
        "links": [("CL","SB",5,"positiva"),("CL","ZL",5,"positiva")],
    },
    {
        "id": "macro_risco",
        "name": "D\u00f3lar \u2192 Commodities (Risco)",
        "description": "D\u00f3lar forte historicamente pressiona commodities precificadas em USD",
        "links": [("DX","ZS",0,"negativa"),("DX","ZC",0,"negativa"),("DX","GC",0,"negativa"),("DX","CL",0,"negativa"),("DX","SB",0,"negativa")],
    },
    {
        "id": "trigo_spread",
        "name": "Trigo KC vs CBOT",
        "description": "Pr\u00eamio de prote\u00edna: trigo duro (KC) vs mole (CBOT)",
        "links": [("KE","ZW",0,"positiva"),("ZW","ZC",0,"positiva")],
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _load_json(path):
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def load_prices():
    series = {}
    for path in [PROC_PATH, RAW_PATH]:
        if not path.exists():
            continue
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for sym in SYMBOLS:
            if sym in series:
                continue
            records = data.get(sym)
            if not records:
                continue
            if isinstance(records, dict):
                records = records.get("candles", [])
            if records:
                series[sym] = records
    return series


def to_returns(records, n=None):
    if n:
        records = records[-n:]
    returns = {}
    for i in range(1, len(records)):
        prev_close = records[i - 1].get("close", 0)
        cur_close = records[i].get("close", 0)
        if prev_close and prev_close != 0:
            returns[records[i]["date"]] = (cur_close - prev_close) / prev_close
    return returns


def pearson(x_vals, y_vals):
    n = len(x_vals)
    if n < 30:
        return None
    sx = sum(x_vals)
    sy = sum(y_vals)
    sxx = sum(v * v for v in x_vals)
    syy = sum(v * v for v in y_vals)
    sxy = sum(a * b for a, b in zip(x_vals, y_vals))
    denom = math.sqrt((n * sxx - sx * sx) * (n * syy - sy * sy))
    if denom == 0:
        return None
    return round((n * sxy - sx * sy) / denom, 4)


def correlate(ret_a, ret_b, lag=0):
    dates_a = sorted(ret_a.keys())
    dates_b = sorted(ret_b.keys())
    if lag == 0:
        common = sorted(set(dates_a) & set(dates_b))
        if len(common) < 30:
            return None
        return pearson([ret_a[d] for d in common], [ret_b[d] for d in common])
    date_to_idx_b = {d: i for i, d in enumerate(dates_b)}
    x, y = [], []
    for d in dates_a:
        if d not in date_to_idx_b:
            continue
        idx_b = date_to_idx_b[d] + lag
        if 0 <= idx_b < len(dates_b):
            x.append(ret_a[d])
            y.append(ret_b[dates_b[idx_b]])
    if len(x) < 30:
        return None
    return pearson(x, y)


def direction_signal(records):
    if not records or len(records) < 20:
        return "sem dados"
    last = records[-1].get("close", 0)
    prev20 = records[-20].get("close", 0)
    if not prev20:
        return "sem dados"
    pct = (last - prev20) / prev20 * 100
    if pct > 3:
        return "subindo"
    elif pct < -3:
        return "caindo"
    return "estavel"


# ---------------------------------------------------------------------------
# COT: correlate Managed Money net with future returns
# ---------------------------------------------------------------------------
def build_cot_returns(cot_data, returns):
    """Correlate weekly COT MM net changes with price returns at lags 5/10/20d."""
    results = {}
    if not cot_data or "commodities" not in cot_data:
        return results

    for sym in COT_SYMS:
        if sym not in returns:
            continue
        comm = cot_data["commodities"].get(sym)
        if not comm:
            continue
        # Prefer disaggregated
        report = comm.get("disaggregated") or comm.get("legacy")
        if not report or not report.get("history"):
            continue

        history = report["history"]
        # Build weekly MM net changes as {date: delta}
        mm_key = "managed_money_net" if "managed_money_net" in (history[0] or {}) else "noncomm_net"
        mm_changes = {}
        for i in range(1, len(history)):
            cur_val = history[i].get(mm_key)
            prev_val = history[i - 1].get(mm_key)
            dt = history[i].get("date")
            if cur_val is not None and prev_val is not None and dt:
                mm_changes[dt] = cur_val - prev_val

        if len(mm_changes) < 20:
            continue

        # Correlate COT change with future price returns at lags
        dates_ret = sorted(returns[sym].keys())
        date_to_idx = {d: i for i, d in enumerate(dates_ret)}

        for lag in [5, 10, 20]:
            x_vals, y_vals = [], []
            for cot_date, delta in mm_changes.items():
                if cot_date not in date_to_idx:
                    continue
                idx = date_to_idx[cot_date] + lag
                if 0 <= idx < len(dates_ret):
                    x_vals.append(delta)
                    y_vals.append(returns[sym][dates_ret[idx]])
            c = pearson(x_vals, y_vals)
            if c is not None:
                results[f"COT_MM_{sym}_leads_price_{lag}d"] = c

    return results


# ---------------------------------------------------------------------------
# Fundamental signals per commodity
# ---------------------------------------------------------------------------
def build_fundamental_signals(cot_data, grain_ratios, psd_data):
    signals = {}

    for sym in COT_SYMS:
        entry = {}

        # COT data
        if cot_data and "commodities" in cot_data:
            comm = cot_data["commodities"].get(sym)
            if comm:
                report = comm.get("disaggregated") or comm.get("legacy")
                if report and report.get("latest"):
                    latest = report["latest"]
                    mm_key = "managed_money_net" if "managed_money_net" in latest else "noncomm_net"
                    entry["cot_mm_net"] = latest.get(mm_key)
                    entry["cot_open_interest"] = latest.get("open_interest")
                    entry["cot_report_date"] = latest.get("date")

        # COT Index + STU from grain_ratios (grains only)
        grain = GRAIN_SYMS.get(sym)
        if grain and grain_ratios:
            snap = grain_ratios.get("current_snapshot", {})
            cot_snap = snap.get("cot", {}).get(grain, {})
            if cot_snap:
                entry["cot_index"] = cot_snap.get("cot_index")
                entry["cot_signal"] = cot_snap.get("signal")
            stu = snap.get("stu", {}).get(grain, {})
            if stu:
                entry["stu_current"] = stu.get("current")
                entry["stu_z_score"] = stu.get("z")
            ratios = snap.get("ratios", {})
            if grain == "corn":
                cs = ratios.get("corn_soy", {})
                if cs:
                    entry["corn_soy_ratio"] = cs.get("current")
            margins = snap.get("margins", {}).get(grain, {})
            if margins:
                entry["margin_vs_cop"] = margins.get("margin")

        # PSD ending stocks (non-grain commodities)
        if psd_data and not grain:
            comms = psd_data.get("commodities", {})
            psd_item = comms.get(sym)
            if psd_item:
                entry["ending_stocks"] = psd_item.get("current")
                entry["stocks_avg_5y"] = psd_item.get("avg_5y")
                entry["stocks_deviation_pct"] = psd_item.get("deviation")

        if entry:
            signals[sym] = entry

    return signals


# ---------------------------------------------------------------------------
# Macro correlations
# ---------------------------------------------------------------------------
def build_macro_correlations(returns, prices, macro_ind, fedwatch, bcb_data):
    results = {}

    # VIX vs major commodities (use macro_indicators for current level, not correlation)
    # For correlation we need VIX history — not available as daily series
    # Instead, report current VIX level and its implied impact
    if macro_ind and macro_ind.get("vix"):
        vix = macro_ind["vix"]
        results["vix_current"] = vix.get("value")
        results["vix_level"] = vix.get("level")
        results["vix_change_pct"] = vix.get("change_pct")

    # DX vs commodities (already in price matrix, extract key pairs)
    if "DX" in returns:
        for sym in ["ZS", "ZC", "ZW", "GC", "CL", "SB"]:
            if sym in returns:
                c = correlate(returns["DX"], returns[sym], lag=0)
                if c is not None:
                    results[f"DX_vs_{sym}"] = c

    # BRL/USD vs ZS in BRL (competitividade exportadora)
    if bcb_data and bcb_data.get("brl_usd") and "ZS" in prices:
        brl_series = bcb_data["brl_usd"]
        zs_records = prices["ZS"]
        # Build BRL-denominated soy price returns
        brl_map = {r["date"]: r["value"] for r in brl_series if r.get("value")}
        zs_brl_returns = {}
        for i in range(1, len(zs_records)):
            d = zs_records[i]["date"]
            d_prev = zs_records[i - 1]["date"]
            brl_cur = brl_map.get(d)
            brl_prev = brl_map.get(d_prev)
            zs_cur = zs_records[i].get("close", 0)
            zs_prev = zs_records[i - 1].get("close", 0)
            if brl_cur and brl_prev and zs_prev and zs_cur:
                price_brl_cur = zs_cur * brl_cur
                price_brl_prev = zs_prev * brl_prev
                if price_brl_prev > 0:
                    zs_brl_returns[d] = (price_brl_cur - price_brl_prev) / price_brl_prev
        # Correlate BRL/USD returns with ZS returns
        brl_returns = {}
        for i in range(1, len(brl_series)):
            d = brl_series[i]["date"]
            prev = brl_series[i - 1].get("value")
            cur = brl_series[i].get("value")
            if prev and cur and prev > 0:
                brl_returns[d] = (cur - prev) / prev
        if "ZS" in returns and len(brl_returns) >= 30:
            c = correlate(brl_returns, returns["ZS"], lag=0)
            if c is not None:
                results["BRL_vs_ZS_USD"] = c
        # BRL vs ZS in BRL terms
        if len(zs_brl_returns) >= 30 and len(brl_returns) >= 30:
            c = correlate(brl_returns, zs_brl_returns, lag=0)
            if c is not None:
                results["BRL_vs_ZS_BRL"] = c
        # Latest BRL
        if brl_series:
            results["brl_usd_latest"] = brl_series[-1].get("value")

    # Fed cut probability vs GC/SI
    if fedwatch and fedwatch.get("probabilities"):
        probs = fedwatch["probabilities"]
        results["fed_cut_prob"] = probs.get("cut_25bps")
        results["fed_hold_prob"] = probs.get("hold")
        results["fed_hike_prob"] = probs.get("hike_25bps")
        results["fed_expectation"] = fedwatch.get("market_expectation")
        results["fed_next_meeting"] = fedwatch.get("next_meeting")

    # S&P 500 info
    if macro_ind and macro_ind.get("sp500"):
        sp = macro_ind["sp500"]
        results["sp500_change_pct"] = sp.get("change_pct")
        results["sp500_change_week_pct"] = sp.get("change_week_pct")

    # Treasury 10Y
    if macro_ind and macro_ind.get("treasury_10y"):
        ty = macro_ind["treasury_10y"]
        results["treasury_10y_yield"] = ty.get("yield_pct")
        results["treasury_10y_direction"] = ty.get("direction")

    return results


# ---------------------------------------------------------------------------
# Sentiment & Climate signals
# ---------------------------------------------------------------------------
def build_sentiment_signals(weather_data, google_trends):
    signals = {}

    # ENSO status
    if weather_data:
        enso = weather_data.get("enso")
        if enso:
            signals["enso_status"] = enso.get("status")
            signals["enso_oni"] = enso.get("oni_value")

        # Active climate alerts count
        regions = weather_data.get("regions", {})
        total_alerts = 0
        alert_details = []
        for rk, region in regions.items():
            alerts = region.get("alerts", [])
            total_alerts += len(alerts)
            for a in alerts:
                alert_details.append({"region": region.get("label", rk), "type": a.get("type"), "severity": a.get("severity")})
        signals["climate_alerts_active"] = total_alerts
        if alert_details:
            signals["climate_alert_details"] = alert_details

    # Google Trends
    if google_trends and not google_trends.get("is_fallback"):
        trends = google_trends.get("trends", {})
        spikes = google_trends.get("spikes", [])
        signals["google_spike_terms"] = spikes
        signals["google_summary"] = google_trends.get("summary", "")

        # Top 5 trending terms
        sorted_terms = sorted(trends.items(), key=lambda x: x[1].get("current", 0), reverse=True)
        signals["google_top5"] = [
            {"term": t, "current": d.get("current"), "direction": d.get("direction")}
            for t, d in sorted_terms[:5]
        ]

    return signals


# ---------------------------------------------------------------------------
# Composite signals
# ---------------------------------------------------------------------------
def build_composite_signals(prices, returns, fundamental, macro_corr, sentiment):
    """Generate composite bullish/bearish signals when >=3 data sources agree."""
    composites = []

    for sym in ["ZS", "ZC", "ZW", "GC", "SI", "CL", "SB"]:
        factors_bull = []
        factors_bear = []
        sources_count = 0

        fund = fundamental.get(sym, {})

        # 1. Price trend
        if sym in prices:
            trend = direction_signal(prices[sym])
            if trend == "subindo":
                factors_bull.append("Tend\u00eancia de pre\u00e7o altista (20d)")
            elif trend == "caindo":
                factors_bear.append("Tend\u00eancia de pre\u00e7o baixista (20d)")
            if trend != "sem dados":
                sources_count += 1

        # 2. COT positioning
        cot_idx = fund.get("cot_index")
        cot_sig = fund.get("cot_signal")
        if cot_sig:
            sources_count += 1
            if cot_sig == "BULL":
                factors_bull.append(f"COT Index baixo ({cot_idx:.0f}/100) \u2014 especuladores subposicionados")
            elif cot_sig == "BEAR":
                factors_bear.append(f"COT Index alto ({cot_idx:.0f}/100) \u2014 especuladores sobreposicionados")

        # 3. STU (grains only)
        stu_z = fund.get("stu_z_score")
        if stu_z is not None:
            sources_count += 1
            if stu_z < -1:
                factors_bull.append(f"Estoques apertados (STU z={stu_z:.1f})")
            elif stu_z > 1:
                factors_bear.append(f"Estoques folgados (STU z={stu_z:.1f})")

        # 4. Margin vs COP (grains only)
        margin = fund.get("margin_vs_cop")
        if margin is not None:
            sources_count += 1
            if margin < 0:
                factors_bull.append("Pre\u00e7o abaixo custo produ\u00e7\u00e3o (suporte hist\u00f3rico)")
            elif margin > 0:
                factors_bear.append("Pre\u00e7o acima custo produ\u00e7\u00e3o")

        # 5. DX correlation impact
        dx_corr = macro_corr.get(f"DX_vs_{sym}")
        dx_trend = direction_signal(prices.get("DX", []))
        if dx_corr is not None and dx_trend != "sem dados":
            sources_count += 1
            if dx_trend == "subindo" and dx_corr < -0.2:
                factors_bear.append(f"D\u00f3lar subindo (corr {dx_corr:+.2f})")
            elif dx_trend == "caindo" and dx_corr < -0.2:
                factors_bull.append(f"D\u00f3lar caindo (corr {dx_corr:+.2f})")

        # 6. BRL weakness (for ZS competitiveness)
        if sym == "ZS":
            brl = macro_corr.get("brl_usd_latest")
            if brl is not None:
                sources_count += 1
                if brl > 5.5:
                    factors_bull.append(f"BRL fraco (R${brl:.2f}) \u2014 exporta\u00e7\u00e3o BR competitiva")
                elif brl < 4.8:
                    factors_bear.append(f"BRL forte (R${brl:.2f}) \u2014 press\u00e3o exportadora")

        # 7. VIX regime
        vix_level = macro_corr.get("vix_level")
        if vix_level:
            sources_count += 1
            if vix_level in ("elevado", "extremo"):
                # High VIX = risk-off, bad for ag, good for gold
                if sym in ("GC", "SI"):
                    factors_bull.append(f"VIX {vix_level} \u2014 demanda por porto seguro")
                else:
                    factors_bear.append(f"VIX {vix_level} \u2014 risk-off pressiona commodities")

        # 8. Fed policy impact (metals)
        if sym in ("GC", "SI"):
            cut_prob = macro_corr.get("fed_cut_prob")
            if cut_prob is not None:
                sources_count += 1
                if cut_prob > 40:
                    factors_bull.append(f"Probabilidade de corte Fed {cut_prob:.0f}%")
                elif cut_prob < 10:
                    fed_hike = macro_corr.get("fed_hike_prob", 0)
                    if fed_hike and fed_hike > 20:
                        factors_bear.append(f"Probabilidade de alta Fed {fed_hike:.0f}%")

        # 9. ENSO impact (grains, sugar, coffee)
        if sym in ("ZS", "ZC", "ZW", "SB", "KC"):
            enso = sentiment.get("enso_status")
            if enso and enso not in ("N/A", "Neutro", "neutral"):
                sources_count += 1
                if "ni" in enso.lower() and "la" in enso.lower():
                    factors_bull.append(f"La Ni\u00f1a ativa \u2014 risco de seca Am\u00e9rica do Sul")
                elif "ni" in enso.lower():
                    factors_bear.append(f"El Ni\u00f1o ativo \u2014 safra favorecida historicamente")

        # 10. Google Trends spikes
        spike_terms = sentiment.get("google_spike_terms", [])
        sym_terms = {
            "ZS": ["soja", "soybean price", "preco soja"],
            "ZC": ["milho", "corn price", "preco milho"],
            "ZW": ["trigo", "wheat price"],
            "GC": ["commodities"],
            "SB": ["commodities"],
            "CL": ["commodities"],
        }
        relevant_spikes = [t for t in spike_terms if t in sym_terms.get(sym, [])]
        if relevant_spikes:
            sources_count += 1
            factors_bull.append(f"Spike de busca: {', '.join(relevant_spikes)}")

        # Skip if insufficient data
        if sources_count < 3:
            continue

        # Compute signal
        bull = len(factors_bull)
        bear = len(factors_bear)
        total = bull + bear
        if total == 0:
            continue

        if bull > bear * 1.5:
            signal = "BULLISH"
            confidence = round(bull / total, 2)
        elif bear > bull * 1.5:
            signal = "BEARISH"
            confidence = round(bear / total, 2)
        else:
            signal = "NEUTRO"
            confidence = round(0.5, 2)

        composites.append({
            "asset": sym,
            "signal": signal,
            "confidence": confidence,
            "sources_count": sources_count,
            "factors_bull": factors_bull,
            "factors_bear": factors_bear,
        })

    return composites


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("  collect_correlations: starting...")

    # --- Load all data sources ---
    prices = load_prices()
    available = [s for s in SYMBOLS if s in prices]
    print(f"    Prices: {len(available)}/{len(SYMBOLS)} symbols")

    cot_data = _load_json(PROC / "cot.json")
    grain_ratios = _load_json(PROC / "grain_ratios.json")
    psd_data = _load_json(PROC / "psd_ending_stocks.json")
    macro_ind = _load_json(PROC / "macro_indicators.json")
    fedwatch = _load_json(PROC / "fedwatch.json")
    bcb_data = _load_json(PROC / "bcb_data.json")
    weather_data = _load_json(PROC / "weather_agro.json")
    google_trends = _load_json(PROC / "google_trends.json")

    sources_loaded = sum(1 for x in [cot_data, grain_ratios, psd_data, macro_ind, fedwatch, bcb_data, weather_data, google_trends] if x)
    print(f"    Auxiliary sources: {sources_loaded}/8 loaded")

    if len(available) < 5:
        print("    ERROR: insufficient price data")
        fallback = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "is_fallback": True,
            "error": f"Only {len(available)} symbols available",
            "matrix": {}, "lagged": {}, "causal_chains": [], "strongest_pairs": [],
            "fundamental_signals": {}, "macro_correlations": {}, "sentiment_signals": {}, "composite_signals": [],
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(fallback, f, indent=2, ensure_ascii=False)
        return fallback

    # --- Price returns ---
    returns = {}
    for sym in available:
        ret = to_returns(prices[sym], WINDOW + 1)
        if len(ret) >= 30:
            returns[sym] = ret
    syms = sorted(returns.keys())
    print(f"    {len(syms)} symbols with sufficient return data")

    # --- 1. Correlation matrix (lag 0) ---
    matrix = {}
    for a in syms:
        row = {}
        for b in syms:
            if a == b:
                continue
            c = correlate(returns[a], returns[b], lag=0)
            if c is not None:
                row[b] = c
        matrix[a] = row

    # --- 2. Lagged correlations ---
    lagged = {}
    lag_pairs = set()
    for chain in CAUSAL_CHAINS:
        for src, tgt, _, _ in chain["links"]:
            if src in returns and tgt in returns:
                lag_pairs.add((src, tgt))
    all_pairs = []
    for a in syms:
        for b in syms:
            if a < b and b in matrix.get(a, {}):
                all_pairs.append((a, b, abs(matrix[a][b])))
    all_pairs.sort(key=lambda x: x[2], reverse=True)
    for a, b, _ in all_pairs[:15]:
        lag_pairs.add((a, b))
    for src, tgt in lag_pairs:
        if src not in returns or tgt not in returns:
            continue
        for lag in LAGS:
            c = correlate(returns[src], returns[tgt], lag=lag)
            if c is not None:
                lagged[f"{src}_leads_{tgt}_{lag}d"] = c

    # --- 2b. COT vs price lagged correlations ---
    cot_lagged = build_cot_returns(cot_data, returns)
    lagged.update(cot_lagged)
    if cot_lagged:
        print(f"    COT-price lagged pairs: {len(cot_lagged)}")

    # --- 3. Causal chains ---
    chains_out = []
    for chain in CAUSAL_CHAINS:
        correlations = {}
        nodes_used = set()
        for src, tgt, lag, _ in chain["links"]:
            if src not in returns or tgt not in returns:
                continue
            c = correlate(returns[src], returns[tgt], lag=lag)
            if c is not None:
                key = f"{src}\u2192{tgt}" + (f" (lag {lag}d)" if lag > 0 else "")
                correlations[key] = c
                nodes_used.add(src)
                nodes_used.add(tgt)
        lead_sym = chain["links"][0][0]
        tail_sym = chain["links"][-1][1]
        lead_dir = direction_signal(prices.get(lead_sym, []))
        expected_tail = chain["links"][-1][3]
        if lead_dir == "subindo":
            signal = f"{lead_sym} subindo \u2192 press\u00e3o {'baixista' if expected_tail=='negativa' else 'altista'} em {tail_sym} historicamente"
        elif lead_dir == "caindo":
            signal = f"{lead_sym} caindo \u2192 press\u00e3o {'altista' if expected_tail=='negativa' else 'baixista'} em {tail_sym} historicamente"
        else:
            signal = f"{lead_sym} est\u00e1vel \u2192 sem sinal direcional claro"
        chains_out.append({
            "id": chain["id"], "name": chain["name"], "description": chain["description"],
            "nodes": sorted(nodes_used), "correlations": correlations, "current_signal": signal,
        })

    # --- 4. Strongest pairs ---
    strongest = []
    for a, b, absval in all_pairs[:20]:
        val = matrix[a][b]
        strongest.append({
            "pair": f"{a}-{b}", "correlation": val,
            "direction": "positiva" if val > 0 else "negativa",
            "strength": "forte" if absval > 0.7 else "moderada" if absval > 0.4 else "fraca",
        })

    # --- 5. Fundamental signals ---
    fundamental = build_fundamental_signals(cot_data, grain_ratios, psd_data)
    print(f"    Fundamental signals: {len(fundamental)} assets")

    # --- 6. Macro correlations ---
    macro_corr = build_macro_correlations(returns, prices, macro_ind, fedwatch, bcb_data)
    print(f"    Macro correlations: {len(macro_corr)} entries")

    # --- 7. Sentiment signals ---
    sentiment = build_sentiment_signals(weather_data, google_trends)
    print(f"    Sentiment signals: {len(sentiment)} entries")

    # --- 8. Composite signals ---
    composites = build_composite_signals(prices, returns, fundamental, macro_corr, sentiment)
    print(f"    Composite signals: {len(composites)} assets")

    # --- Output ---
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "is_fallback": False,
        "source": "Multidimensional: prices + COT + fundamentals + macro + sentiment",
        "window_days": WINDOW,
        "symbols_count": len(syms),
        "auxiliary_sources": sources_loaded,
        "matrix": matrix,
        "lagged": lagged,
        "causal_chains": chains_out,
        "strongest_pairs": strongest,
        "fundamental_signals": fundamental,
        "macro_correlations": macro_corr,
        "sentiment_signals": sentiment,
        "composite_signals": composites,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"    Matrix: {len(syms)}x{len(syms)}, Lagged: {len(lagged)}")
    print(f"    Top 10 pares:")
    for p in strongest[:10]:
        print(f"      {p['pair']:8s}  {p['correlation']:+.4f}  ({p['direction']}, {p['strength']})")
    if composites:
        print(f"    Composite signals:")
        for c in composites:
            print(f"      {c['asset']:4s}  {c['signal']:8s}  conf={c['confidence']:.2f}  ({c['sources_count']} fontes)")
            for f in c["factors_bull"]:
                print(f"        + {f}")
            for f in c["factors_bear"]:
                print(f"        - {f}")
    print(f"  Saved to {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    main()
