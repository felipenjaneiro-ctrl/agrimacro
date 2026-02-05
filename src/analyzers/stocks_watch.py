import os, sys, json
from datetime import datetime

def market_state(stock_vs_avg, curve_spread, mm_percentile):
    tight = 0
    excess = 0
    factors = []
    if stock_vs_avg is not None:
        if stock_vs_avg < -10:
            tight += 1
            factors.append(f"Estoque {stock_vs_avg:.1f}% abaixo da media")
        elif stock_vs_avg > 10:
            excess += 1
            factors.append(f"Estoque {stock_vs_avg:.1f}% acima da media")
    if curve_spread is not None:
        if curve_spread < -0.5:
            tight += 1
            factors.append(f"Curva invertida ({curve_spread:.2f}%)")
        elif curve_spread > 0.5:
            excess += 1
            factors.append(f"Curva contango ({curve_spread:.2f}%)")
    if mm_percentile is not None:
        if mm_percentile < 20:
            tight += 1
            factors.append(f"MM muito vendido (P={mm_percentile:.0f})")
        elif mm_percentile > 80:
            excess += 1
            factors.append(f"MM muito comprado (P={mm_percentile:.0f})")
    if tight >= 3:
        state = "APERTO_FORTE"
    elif tight >= 2:
        state = "APERTO_MODERADO"
    elif excess >= 3:
        state = "EXCESSO_FORTE"
    elif excess >= 2:
        state = "EXCESSO_MODERADO"
    elif tight == 1:
        state = "NEUTRO_VIES_APERTO"
    elif excess == 1:
        state = "NEUTRO_VIES_EXCESSO"
    else:
        state = "NEUTRO"
    return {"state": state, "tight": tight, "excess": excess, "factors": factors}

def build_watch(seasonality_data, spreads_data):
    results = {"timestamp": datetime.now().isoformat(), "commodities": {}}
    for symbol, sdata in seasonality_data.items():
        stats = sdata.get("stats") or {}
        dev = stats.get("deviation_pct")
        stock_proxy = None
        if dev is not None:
            stock_proxy = dev
        curve_spread = None
        mm_pct = None
        ms = market_state(stock_proxy, curve_spread, mm_pct)
        results["commodities"][symbol] = {
            "symbol": symbol,
            "price_vs_avg": dev,
            "state": ms["state"],
            "factors": ms["factors"],
            "data_available": {"stock_proxy": dev is not None, "curve": False, "cot": False}
        }
    return results

if __name__ == "__main__":
    print("=" * 50)
    print("AGRIMACRO - GATE 3: STOCKS WATCH")
    print("=" * 50)
    with open("data/processed/seasonality.json") as f:
        seas = json.load(f)
    spreads = {}
    if os.path.exists("data/processed/spreads.json"):
        with open("data/processed/spreads.json") as f:
            spreads = json.load(f)
    results = build_watch(seas, spreads)
    os.makedirs("data/processed", exist_ok=True)
    with open("data/processed/stocks_watch.json", "w") as f:
        json.dump(results, f, indent=2)
    below = []
    above = []
    neutral = []
    for sym, d in results["commodities"].items():
        st = d["state"]
        dev = d.get("price_vs_avg")
        tag = f"{sym} ({dev:+.1f}%)" if dev else sym
        if "APERTO" in st:
            below.append(tag)
        elif "EXCESSO" in st:
            above.append(tag)
        else:
            neutral.append(tag)
    print(f"\n  ABAIXO DA MEDIA (potencial aperto): {len(below)}")
    for b in below:
        print(f"    v {b}")
    print(f"\n  ACIMA DA MEDIA (potencial excesso): {len(above)}")
    for a in above:
        print(f"    ^ {a}")
    print(f"\n  NEUTRO: {len(neutral)}")
    for n in neutral:
        print(f"    - {n}")
