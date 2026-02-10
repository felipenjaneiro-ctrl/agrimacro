"""
AgriMacro v3.0 - Spreads Processor
Calculates key commodity spreads with z-scores and regime detection
"""
import json
from datetime import datetime
from pathlib import Path
import statistics

# Spread definitions
SPREADS = {
    "soy_crush": {
        "name": "Soy Crush Margin",
        "formula": "ZM*0.022 + ZL*0.11 - ZS/100",  # Approximate crush margin
        "components": ["ZM", "ZL", "ZS"],
        "unit": "USD/bu",
        "description": "Margem de esmagamento de soja. Z-score elevado = margem acima do normal."
    },
    "ke_zw": {
        "name": "KC-CBOT Wheat Spread",
        "formula": "KE - ZW",
        "components": ["KE", "ZW"],
        "unit": "cents/bu",
        "description": "Prêmio de proteína HRW vs SRW. Positivo = mercado paga por qualidade."
    },
    "zl_cl": {
        "name": "Soybean Oil / Crude Ratio",
        "formula": "ZL / CL",
        "components": ["ZL", "CL"],
        "unit": "ratio",
        "description": "Dinâmica de biodiesel. Alto = óleo de soja relativamente caro vs petróleo."
    },
    "feedlot": {
        "name": "Feedlot Margin",
        "formula": "LE*6 - GF - ZC*0.5",  # Simplified feedlot margin
        "components": ["LE", "GF", "ZC"],
        "unit": "USD/cwt",
        "description": "Margem do confinamento. Positiva = lucro para confinadores."
    },
    "zc_zm": {
        "name": "Corn/Meal Ratio",
        "formula": "ZC / ZM",
        "components": ["ZC", "ZM"],
        "unit": "ratio",
        "description": "Competição na ração animal. Baixo = milho mais competitivo que farelo."
    },
    "zc_zs": {
        "name": "Corn/Soy Ratio",
        "formula": "ZC / ZS",
        "components": ["ZC", "ZS"],
        "unit": "ratio",
        "description": "Decisão de plantio. <0.4 favorece soja, >0.45 favorece milho."
    },
}

def calculate_spread(prices: dict, spread_key: str, spread_def: dict) -> dict:
    """Calculate a single spread with historical z-score"""
    components = spread_def["components"]
    
    # Check all components exist
    for comp in components:
        if comp not in prices or not prices[comp]:
            return None
    
    # Get minimum length across all components
    min_len = min(len(prices[comp]) for comp in components)
    if min_len < 30:
        return None
    
    # Calculate spread values for each date
    spread_values = []
    
    # Align by date (use last N days)
    for i in range(min_len):
        idx = -(min_len - i)
        try:
            values = {comp: prices[comp][idx]["close"] for comp in components}
            date = prices[components[0]][idx]["date"]
            
            # Calculate based on formula
            if spread_key == "soy_crush":
                # Crush margin: meal value + oil value - soybean cost
                val = values["ZM"] * 0.022 + values["ZL"] * 0.11 - values["ZS"] / 100
            elif spread_key == "ke_zw":
                val = values["KE"] - values["ZW"]
            elif spread_key == "zl_cl":
                val = values["ZL"] / values["CL"] if values["CL"] > 0 else 0
            elif spread_key == "feedlot":
                # Simplified: (cattle price * 6) - feeder cost - feed cost
                val = values["LE"] * 6 - values["GF"] - values["ZC"] * 0.5
            elif spread_key == "zc_zm":
                val = values["ZC"] / values["ZM"] if values["ZM"] > 0 else 0
            elif spread_key == "zc_zs":
                val = values["ZC"] / values["ZS"] if values["ZS"] > 0 else 0
            else:
                continue
            
            spread_values.append({"date": date, "value": val})
        except Exception:
            continue
    
    if len(spread_values) < 30:
        return None
    
    # Calculate statistics
    all_vals = [sv["value"] for sv in spread_values]
    current = all_vals[-1]
    
    # 1-year lookback (252 trading days)
    lookback_1y = all_vals[-252:] if len(all_vals) >= 252 else all_vals
    mean_1y = statistics.mean(lookback_1y)
    std_1y = statistics.stdev(lookback_1y) if len(lookback_1y) > 1 else 1
    zscore_1y = (current - mean_1y) / std_1y if std_1y > 0 else 0
    
    # Percentile
    sorted_vals = sorted(lookback_1y)
    percentile = (sorted_vals.index(min(sorted_vals, key=lambda x: abs(x - current))) + 1) / len(sorted_vals) * 100
    
    # Regime detection
    if abs(zscore_1y) > 2:
        regime = "EXTREMO"
    elif abs(zscore_1y) > 1:
        regime = "DISSONÂNCIA" if zscore_1y > 0 else "COMPRESSÃO"
    else:
        regime = "NORMAL"
    
    # Trend (last 5 days vs last 20 days)
    if len(all_vals) >= 20:
        recent = statistics.mean(all_vals[-5:])
        older = statistics.mean(all_vals[-20:-5])
        trend_pct = ((recent - older) / abs(older)) * 100 if older != 0 else 0
        if trend_pct > 5:
            trend = "SUBINDO"
        elif trend_pct < -5:
            trend = "CAINDO"
        else:
            trend = "LATERAL"
    else:
        trend = "INDEFINIDO"
        trend_pct = 0
    
    return {
        "name": spread_def["name"],
        "unit": spread_def["unit"],
        "description": spread_def["description"],
        "current": round(current, 4),
        "mean_1y": round(mean_1y, 4),
        "std_1y": round(std_1y, 4),
        "zscore_1y": round(zscore_1y, 2),
        "percentile": round(percentile, 0),
        "regime": regime,
        "trend": trend,
        "trend_pct": round(trend_pct, 1),
        "points": len(spread_values),
        "history": spread_values[-60:]  # Last 60 days for charting
    }

def process_spreads(price_file: Path) -> dict:
    """Process all spreads"""
    with open(price_file) as f:
        prices = json.load(f)
    
    result = {
        "timestamp": datetime.now().isoformat(),
        "spreads": {}
    }
    
    for spread_key, spread_def in SPREADS.items():
        spread_data = calculate_spread(prices, spread_key, spread_def)
        if spread_data:
            result["spreads"][spread_key] = spread_data
    
    return result

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        result = process_spreads(Path(sys.argv[1]))
    else:
        result = process_spreads(Path("../agrimacro-dash/public/data/raw/price_history.json"))
    
    print(f"Processed {len(result['spreads'])} spreads:")
    for key, data in result["spreads"].items():
        print(f"  {key}: {data['current']:.4f} {data['unit']} | Z={data['zscore_1y']:+.2f} | {data['regime']} | {data['trend']}")
