#!/usr/bin/env python3
"""
generate_intel_synthesis.py - AgriMacro Daily Intelligence Synthesis
====================================================================
Reads correlations.json (multidimensional) + all available JSONs and
generates a prioritized daily synthesis like a senior analyst would.

Output: intel_synthesis.json
"""

import json
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
ROOT_DIR = SCRIPT_DIR.parent
PROC = ROOT_DIR / "agrimacro-dash" / "public" / "data" / "processed"
OUTPUT_FILE = PROC / "intel_synthesis.json" if PROC.exists() else SCRIPT_DIR / "intel_synthesis.json"

FRIENDLY = {
    "ZC": "Milho", "ZS": "Soja", "ZW": "Trigo CBOT", "KE": "Trigo KC",
    "ZM": "Farelo Soja", "ZL": "\u00d3leo Soja", "SB": "A\u00e7\u00facar",
    "KC": "Caf\u00e9", "CT": "Algod\u00e3o", "CC": "Cacau", "OJ": "Suco Laranja",
    "LE": "Boi Gordo", "GF": "Feeder Cattle", "HE": "Su\u00edno",
    "CL": "Petr\u00f3leo", "NG": "G\u00e1s Natural",
    "GC": "Ouro", "SI": "Prata", "DX": "D\u00f3lar Index",
}


def _load(name):
    p = PROC / name
    if not p.exists():
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _sig(category, priority, title, detail, source):
    return {"category": category, "priority": priority, "title": title, "detail": detail, "source": source}


def main():
    print("  generate_intel_synthesis: starting...")

    corr = _load("correlations.json")
    spreads = _load("spreads.json")
    macro = _load("macro_indicators.json")
    fedwatch = _load("fedwatch.json")
    weather = _load("weather_agro.json")
    gtrends = _load("google_trends.json")
    crop = _load("crop_progress.json")
    prices_raw = _load("price_history.json")

    sources_loaded = sum(1 for x in [corr, spreads, macro, fedwatch, weather, gtrends, crop] if x)
    print(f"    Sources loaded: {sources_loaded}/7")

    if sources_loaded == 0:
        fallback = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "is_fallback": True, "error": "No data sources available",
            "priority_high": [], "priority_medium": [], "priority_low": [],
            "causal_chains_active": [], "summary": "", "signal_count": {"high": 0, "medium": 0, "low": 0},
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(fallback, f, indent=2, ensure_ascii=False)
        return fallback

    signals = []

    # ===================================================================
    # 1. COMPOSITE SIGNALS (from correlations.json)
    # ===================================================================
    if corr and corr.get("composite_signals"):
        for cs in corr["composite_signals"]:
            asset = cs["asset"]
            name = FRIENDLY.get(asset, asset)
            sig = cs["signal"]
            conf = cs["confidence"]
            n_src = cs["sources_count"]

            if sig == "BULLISH":
                prio = "high" if conf >= 0.7 else "medium"
                factors_str = "; ".join(cs.get("factors_bull", [])[:3])
                signals.append(_sig(
                    "composite", prio,
                    f"{name} ({asset}): sinal BULLISH (conf. {conf:.0%}, {n_src} fontes)",
                    factors_str, "correlations.json"
                ))
            elif sig == "BEARISH":
                prio = "high" if conf >= 0.7 else "medium"
                factors_str = "; ".join(cs.get("factors_bear", [])[:3])
                signals.append(_sig(
                    "composite", prio,
                    f"{name} ({asset}): sinal BEARISH (conf. {conf:.0%}, {n_src} fontes)",
                    factors_str, "correlations.json"
                ))

    # ===================================================================
    # 2. MACRO ALERTS
    # ===================================================================
    if macro:
        vix = macro.get("vix")
        if vix and vix.get("value"):
            v = vix["value"]
            ch = vix.get("change_pct", 0) or 0
            if v > 35:
                signals.append(_sig("macro", "high",
                    f"VIX em n\u00edvel EXTREMO: {v:.1f} ({ch:+.1f}%)",
                    "Mercado em p\u00e2nico. Risk-off severo afeta commodities.", "macro_indicators.json"))
            elif v > 25:
                signals.append(_sig("macro", "medium",
                    f"VIX ELEVADO: {v:.1f} ({ch:+.1f}%)",
                    "Volatilidade acima da m\u00e9dia. Aten\u00e7\u00e3o a commodities e metais.", "macro_indicators.json"))

            # VIX subindo forte = risk-off
            if ch and ch > 10:
                signals.append(_sig("macro", "high",
                    f"VIX disparou {ch:+.1f}% no dia",
                    "Explos\u00e3o de volatilidade sinaliza evento de risco.", "macro_indicators.json"))

        sp = macro.get("sp500")
        if sp and sp.get("change_pct") is not None:
            sp_ch = sp["change_pct"]
            if sp_ch < -2:
                signals.append(_sig("macro", "high",
                    f"S&P 500 caiu {sp_ch:.1f}% no dia",
                    "Sell-off amplo. Risk-off transversal.", "macro_indicators.json"))
            elif sp_ch < -1:
                signals.append(_sig("macro", "medium",
                    f"S&P 500 caiu {sp_ch:.1f}% no dia",
                    "Press\u00e3o vendedora no mercado acion\u00e1rio.", "macro_indicators.json"))

        ty = macro.get("treasury_10y")
        if ty and ty.get("change_bps") is not None:
            bps = ty["change_bps"]
            if abs(bps) > 10:
                signals.append(_sig("macro", "medium",
                    f"Juros 10Y EUA: {ty.get('yield_pct',0):.3f}% ({bps:+.1f} bps)",
                    f"Movimento forte de juros. Dire\u00e7\u00e3o: {ty.get('direction','?')}.", "macro_indicators.json"))

    # Fed
    if fedwatch and fedwatch.get("probabilities"):
        probs = fedwatch["probabilities"]
        cut = probs.get("cut_25bps", 0) or 0
        hike = probs.get("hike_25bps", 0) or 0
        if cut > 50:
            signals.append(_sig("macro", "high",
                f"Fed: {cut:.0f}% de probabilidade de CORTE na pr\u00f3xima reuni\u00e3o ({fedwatch.get('next_meeting','')})",
                "Corte de juros favorece ouro, prata e commodities.", "fedwatch.json"))
        elif cut > 30:
            signals.append(_sig("macro", "medium",
                f"Fed: {cut:.0f}% de probabilidade de corte ({fedwatch.get('next_meeting','')})",
                "Mercado come\u00e7a a precificar afrouxamento.", "fedwatch.json"))
        if hike > 30:
            signals.append(_sig("macro", "medium",
                f"Fed: {hike:.0f}% de probabilidade de ALTA ({fedwatch.get('next_meeting','')})",
                "Risco de aperto monet\u00e1rio adicional.", "fedwatch.json"))

    # ===================================================================
    # 3. CAUSAL CHAINS (price-triggered)
    # ===================================================================
    chains_active = []
    if corr and corr.get("causal_chains") and prices_raw:
        for chain in corr["causal_chains"]:
            sig_text = chain.get("current_signal", "")
            # Only include if there's a directional signal
            if "subindo" in sig_text or "caindo" in sig_text:
                chains_active.append({
                    "id": chain["id"],
                    "name": chain["name"],
                    "signal": sig_text,
                    "top_correlation": max(chain.get("correlations", {}).values(), default=0),
                })
                prio = "medium" if abs(max(chain.get("correlations", {}).values(), default=0)) > 0.3 else "low"
                signals.append(_sig("causal", prio,
                    f"Cadeia {chain['name']} ativa",
                    sig_text, "correlations.json"))

    # ===================================================================
    # 4. SPREADS EXTREMOS
    # ===================================================================
    if spreads and spreads.get("spreads"):
        spread_friendly = {
            "soy_crush": "Margem de Esmagamento (Soja)",
            "ke_zw": "Pr\u00eamio Trigo Duro vs Mole",
            "zl_cl": "\u00d3leo Soja vs Petr\u00f3leo",
            "feedlot": "Margem Confinamento",
            "zc_zm": "Milho vs Farelo",
            "zc_zs": "Soja vs Milho (Plantio)",
        }
        for key, sp in spreads["spreads"].items():
            regime = sp.get("regime", "")
            zscore = sp.get("zscore_1y", 0)
            trend = sp.get("trend", "")
            name = spread_friendly.get(key, sp.get("name", key))

            if regime == "EXTREMO":
                direction = "sobrevalorizado" if zscore > 0 else "subvalorizado"
                signals.append(_sig("spread", "high",
                    f"Spread EXTREMO: {name} (z={zscore:+.2f})",
                    f"{direction}, {trend.lower()}. Oportunidade de revers\u00e3o ou confirma\u00e7\u00e3o.", "spreads.json"))
            elif regime == "DISSON\u00c2NCIA" or regime == "DISSONÂNCIA":
                signals.append(_sig("spread", "medium",
                    f"Spread em Disson\u00e2ncia: {name} (z={zscore:+.2f})",
                    f"Tend\u00eancia: {trend.lower()}.", "spreads.json"))

    # ===================================================================
    # 5. CLIMA
    # ===================================================================
    if weather and weather.get("regions"):
        for rk, region in weather["regions"].items():
            alerts = region.get("alerts", [])
            for alert in alerts:
                sev = alert.get("severity", "")
                if sev in ("ALTA", "CRITICA"):
                    signals.append(_sig("clima", "high",
                        f"Alerta clim\u00e1tico {sev}: {region.get('label', rk)}",
                        alert.get("message", ""), "weather_agro.json"))
                elif sev == "MEDIA":
                    signals.append(_sig("clima", "medium",
                        f"Alerta clim\u00e1tico: {region.get('label', rk)}",
                        alert.get("message", ""), "weather_agro.json"))

        enso = weather.get("enso", {})
        if enso and enso.get("status") and enso["status"] not in ("N/A", "Neutro", "neutral"):
            signals.append(_sig("clima", "medium",
                f"ENSO: {enso['status']} (ONI={enso.get('oni_value','?')})",
                "Impacto potencial em safras de gr\u00e3os e caf\u00e9.", "weather_agro.json"))

    # ===================================================================
    # 6. GOOGLE TRENDS SPIKES
    # ===================================================================
    if gtrends and not gtrends.get("is_fallback"):
        spikes = gtrends.get("spikes", [])
        if spikes:
            signals.append(_sig("sentimento", "medium",
                f"Spike de busca Google: {', '.join(spikes)}",
                "Interesse p\u00fablico elevado pode antecipar movimento de pre\u00e7o.", "google_trends.json"))

    # ===================================================================
    # 7. CROP PROGRESS
    # ===================================================================
    if crop and not crop.get("is_fallback") and crop.get("crops"):
        for crop_key, crop_data in crop["crops"].items():
            nat = crop_data.get("national", {})
            planted = nat.get("planted")
            if planted is not None:
                crop_name = {"CORN": "Milho", "SOYBEANS": "Soja", "WHEAT_WINTER": "Trigo Inverno", "COTTON": "Algod\u00e3o"}.get(crop_key, crop_key)
                week = crop_data.get("week_ending", "")
                if planted < 10 and crop_key in ("CORN", "SOYBEANS"):
                    signals.append(_sig("safra", "low",
                        f"Plantio {crop_name} EUA: {planted:.0f}% ({week})",
                        "In\u00edcio de temporada. Monitorar ritmo nas pr\u00f3ximas semanas.", "crop_progress.json"))
                elif planted > 80:
                    signals.append(_sig("safra", "low",
                        f"Plantio {crop_name} EUA: {planted:.0f}% conclu\u00eddo ({week})",
                        "Plantio quase finalizado.", "crop_progress.json"))

            harvested = nat.get("harvested")
            if harvested is not None and harvested > 0:
                crop_name = {"CORN": "Milho", "SOYBEANS": "Soja", "WHEAT_WINTER": "Trigo Inverno", "COTTON": "Algod\u00e3o"}.get(crop_key, crop_key)
                signals.append(_sig("safra", "low",
                    f"Colheita {crop_name} EUA: {harvested:.0f}%",
                    f"Semana: {crop_data.get('week_ending','')}", "crop_progress.json"))

    # ===================================================================
    # CLASSIFY & BUILD SUMMARY
    # ===================================================================
    high = [s for s in signals if s["priority"] == "high"]
    medium = [s for s in signals if s["priority"] == "medium"]
    low = [s for s in signals if s["priority"] == "low"]

    # Build summary paragraph
    summary_parts = []

    # Lead with composite signals
    bearish = [s for s in (corr or {}).get("composite_signals", []) if s.get("signal") == "BEARISH"]
    bullish = [s for s in (corr or {}).get("composite_signals", []) if s.get("signal") == "BULLISH"]
    if bearish:
        names = ", ".join(FRIENDLY.get(s["asset"], s["asset"]) for s in bearish)
        summary_parts.append(f"Sinais BEARISH multifatoriais em {names} \u2014 estoques folgados, especuladores sobreposicionados e VIX elevado convergem para press\u00e3o baixista.")
    if bullish:
        names = ", ".join(FRIENDLY.get(s["asset"], s["asset"]) for s in bullish)
        summary_parts.append(f"Sinais BULLISH em {names} com m\u00faltiplas fontes confirmando.")

    # Macro context
    if macro and macro.get("vix", {}).get("level") in ("elevado", "extremo"):
        vval = macro["vix"]["value"]
        summary_parts.append(f"VIX em {vval:.1f} ({macro['vix']['level']}) refor\u00e7a ambiente de aten\u00e7\u00e3o para risk-off.")

    # Spreads
    extremos = [s for s in signals if s["category"] == "spread" and s["priority"] == "high"]
    if extremos:
        spread_names = [s["title"].split(":")[1].strip().split("(")[0].strip() for s in extremos]
        summary_parts.append(f"Spreads em regime EXTREMO: {', '.join(spread_names)} \u2014 monitorar para oportunidades.")

    # Chains
    if chains_active:
        chain_names = [c["name"] for c in chains_active[:2]]
        summary_parts.append(f"Cadeias causais ativas: {', '.join(chain_names)}.")

    # Climate
    climate_sigs = [s for s in signals if s["category"] == "clima" and s["priority"] in ("high", "medium")]
    if climate_sigs:
        summary_parts.append(f"{len(climate_sigs)} alerta(s) clim\u00e1tico(s) ativo(s) em regi\u00f5es produtoras.")

    summary = " ".join(summary_parts) if summary_parts else "Sem sinais relevantes no momento."

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "is_fallback": False,
        "priority_high": high,
        "priority_medium": medium,
        "priority_low": low,
        "causal_chains_active": chains_active,
        "summary": summary,
        "signal_count": {"high": len(high), "medium": len(medium), "low": len(low)},
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"    Signals: {len(high)} high, {len(medium)} medium, {len(low)} low")
    print(f"    Chains active: {len(chains_active)}")
    print(f"    Summary: {summary[:120]}...")
    print(f"  Saved to {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    main()
