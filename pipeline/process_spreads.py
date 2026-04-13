"""
AgriMacro v3.0 - Spreads Processor
Calculates key commodity spreads with z-scores and regime detection
"""
import json
from datetime import datetime, timedelta
from pathlib import Path
import statistics
from utils import calculate_crush_spread

FUTURES_PATH = Path(__file__).parent.parent / "agrimacro-dash" / "public" / "data" / "processed" / "futures_contracts.json"

# Month code → month number mapping
MONTH_CODES = {"F":1,"G":2,"H":3,"J":4,"K":5,"M":6,"N":7,"Q":8,"U":9,"V":10,"X":11,"Z":12}


def load_futures():
    """Load futures_contracts.json for forward curve access."""
    try:
        with open(FUTURES_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def get_futures_price(futures_data, sym, months_ahead):
    """Return close of the contract closest to (today + months_ahead months).

    Scans the forward curve in futures_contracts.json and picks the contract
    whose expiry is nearest to the target date, only considering contracts
    that expire after today.
    """
    contracts = futures_data.get("commodities", {}).get(sym, {}).get("contracts", [])
    if not contracts:
        return None, None

    now = datetime.now()
    target = now + timedelta(days=30 * months_ahead)

    best_close = None
    best_label = None
    best_diff = float("inf")

    for c in contracts:
        close = c.get("close")
        if not close or close <= 0:
            continue
        # Parse expiry from month_code + year
        mc = c.get("month_code", "")
        yr = c.get("year", "")
        if mc not in MONTH_CODES or not yr:
            continue
        try:
            exp_month = MONTH_CODES[mc]
            exp_year = int(yr)
            # Use 15th of the month as approximate expiry
            exp_date = datetime(exp_year, exp_month, 15)
            if exp_date <= now:
                continue
            diff = abs((exp_date - target).days)
            if diff < best_diff:
                best_diff = diff
                best_close = float(close)
                best_label = c.get("contract", f"{sym}{mc}{yr[-2:]}")
        except Exception:
            continue

    return best_close, best_label

# Spread definitions
SPREADS = {
    "soy_crush": {
        "name": "Soy Crush Margin",
        "formula": "(ZM*44/2000) + (ZL*11/100) - ZS/100",  # CME Board Crush (44lb meal + 11lb oil yield per bushel)
        "components": ["ZM", "ZL", "ZS"],
        "unit": "USD/bu",
        "description": "Margem de esmagamento de soja. Z-score elevado = margem acima do normal.",
        "category": "graos",
    },
    "ke_zw": {
        "name": "KC-CBOT Wheat Spread",
        "formula": "KE - ZW",
        "components": ["KE", "ZW"],
        "unit": "cents/bu",
        "description": "Pr\u00eamio de prote\u00edna HRW vs SRW. Positivo = mercado paga por qualidade.",
        "category": "graos",
    },
    "zl_cl": {
        "name": "Soybean Oil / Crude Ratio",
        "formula": "ZL / CL",
        "components": ["ZL", "CL"],
        "unit": "ratio",
        "description": "Din\u00e2mica de biodiesel. Alto = \u00f3leo de soja relativamente caro vs petr\u00f3leo.",
        "category": "energia",
    },
    "feedlot": {
        "name": "Feedlot Margin",
        "formula": "LE*10 - GF*7.5 - (ZC/100)*50",  # 1000lb steer, 750lb calf, 50bu corn
        "components": ["LE", "GF", "ZC"],
        "unit": "USD/head",
        "description": "Margem do confinamento por cabe\u00e7a (1000lb). Positiva = lucro para confinadores.",
        "category": "pecuaria",
    },
    "zc_zm": {
        "name": "Corn/Meal Ratio",
        "formula": "ZC / ZM",
        "components": ["ZC", "ZM"],
        "unit": "ratio",
        "description": "Competi\u00e7\u00e3o na ra\u00e7\u00e3o animal. Baixo = milho mais competitivo que farelo.",
        "category": "graos",
    },
    "zc_zs": {
        "name": "Corn/Soy Ratio",
        "formula": "ZC / ZS",
        "components": ["ZC", "ZS"],
        "unit": "ratio",
        "description": "Decis\u00e3o de plantio. <0.4 favorece soja, >0.45 favorece milho.",
        "category": "graos",
    },
    "cattle_crush": {
        "name": "Cattle Crush Margin",
        "formula": "LE - (GF * 0.70) - (ZC * 0.0046)",
        "components": ["LE", "GF", "ZC"],
        "unit": "USD/cwt",
        "description": "Margem bruta do confinamento por cwt. Alto = confinamento lucr\u00e1vel.",
        "category": "pecuaria",
    },
    "feed_wheat": {
        "name": "Feed Wheat Ratio (ZW/ZC)",
        "formula": "ZW / ZC",
        "components": ["ZW", "ZC"],
        "unit": "ratio",
        "description": "Trigo vs milho na ra\u00e7\u00e3o. <1.15 = trigo compete com milho.",
        "category": "graos",
    },
}

# Category mapping for existing spreads
SPREAD_CATEGORIES = {
    "soy_crush": "graos",
    "ke_zw": "graos",
    "zl_cl": "energia",
    "feedlot": "pecuaria",
    "zc_zm": "graos",
    "zc_zs": "graos",
    "cattle_crush": "pecuaria",
    "feed_wheat": "graos",
}

# Interpretation texts
INTERPRETATIONS = {
    "soy_crush": "Margem que a ind\u00fastria tem para comprar soja em gr\u00e3o e vender farelo + \u00f3leo. Quando sobe, ind\u00fastria compra mais soja \u2192 suporte de pre\u00e7o.",
    "ke_zw": "Pr\u00eamio pago pelo trigo duro (proteico) sobre o mole. Sobe quando p\u00e3es artesanais e massas demandam mais prote\u00edna.",
    "zl_cl": "Quantos litros de \u00f3leo de soja equivalem a um barril de petr\u00f3leo. Alto = biodiesel com \u00f3leo vegetal se torna econ\u00f4mico.",
    "feedlot": "Lucro estimado de comprar bezerro (GF), engordar com milho (ZC) e vender como boi gordo (LE). Positivo = expans\u00e3o do rebanho. Negativo = abate precoce.",
    "zc_zm": "Custo relativo do milho vs farelo de soja como fonte de energia na ra\u00e7\u00e3o. Baixo = milho barato, pecu\u00e1ria usa mais milho.",
    "zc_zs": "A rela\u00e7\u00e3o mais importante para o produtor americano: define o que plantar. Abaixo de 2.3 = economicamente vantajoso plantar milho. Acima de 2.5 = mais lucrativo plantar soja.",
    "cattle_crush": "Margem bruta do confinamento: pre\u00e7o do boi gordo menos custo do bezerro e ra\u00e7\u00e3o. Alto = confinamento lucrativo = mais demanda futura de milho.",
    "feed_wheat": "Competi\u00e7\u00e3o entre trigo e milho na ra\u00e7\u00e3o animal. Abaixo de 1.15 = trigo barato, compete com milho. Acima de 1.30 = milho mais competitivo.",
}

WATCH_IF = {
    "soy_crush": "Queda r\u00e1pida = esmagamento parando \u2192 baixista para ZS",
    "ke_zw": "Invers\u00e3o (KC < CBOT) = raridade hist\u00f3rica, oportunidade de arbitragem",
    "zl_cl": "Se subir acima de 0.85, refinarias americanas preferem biodiesel de soja \u2192 alta demanda para ZL e ZS",
    "feedlot": "Margem negativa por 3+ meses = redu\u00e7\u00e3o de oferta bovina em 18-24 meses \u2192 alta estrutural de LE",
    "zc_zm": "Quando cai abaixo de 1.2, integrados de frango e su\u00edno mudam formula\u00e7\u00e3o \u2192 mais demanda para ZC",
    "zc_zs": "Relat\u00f3rio de acreagem USDA em 30/junho confirmar\u00e1 se a inten\u00e7\u00e3o virou plantio real",
    "cattle_crush": "Margem acima de $15/cwt por 2+ meses = pecuaristas expandem rebanho \u2192 mais demanda GF",
    "feed_wheat": "Abaixo de 1.10 por 2+ semanas = formuladores trocam milho por trigo \u2192 press\u00e3o em ZC",
}

def generate_signal_now(spread_key, value, zscore, regime, trend_pct):
    """Gera texto contextual baseado no estado atual."""
    direction = "subindo" if trend_pct > 1 else "caindo" if trend_pct < -1 else "est\u00e1vel"

    signals = {
        "soy_crush": {
            "extreme_high": f"Margem no pico ({zscore:+.1f}\u03c3). Ind\u00fastria pagando muito pela soja. Aten\u00e7\u00e3o: revers\u00e3o poss\u00edvel.",
            "high": f"Margem acima do normal ({direction}). Suporte para ZS via demanda industrial.",
            "normal": "Margem dentro do normal. Esmagamento saud\u00e1vel sem sinal direcional.",
            "low": "Margem pressionada. Ind\u00fastria pode reduzir compras de ZS.",
        },
        "feedlot": {
            "extreme_high": "Confinamento extremamente lucrativo. Expans\u00e3o de rebanho prov\u00e1vel \u2192 alta futura de GF.",
            "high": f"Margem boa ({direction}). Pecuaristas comprando GF \u2192 suporte para feeder.",
            "normal": "Margem normal. Ciclo pecu\u00e1rio em equil\u00edbrio.",
            "low": "Margem comprimida. Risco de abate precoce \u2192 bearish LE m\u00e9dio prazo.",
        },
        "cattle_crush": {
            "extreme_high": "Margem de confinamento no pico. Forte incentivo para engorda \u2192 demanda de milho sobe.",
            "high": f"Confinamento lucrativo ({direction}). Demanda por GF e ZC sustentada.",
            "normal": "Margem normal de confinamento. Sem press\u00e3o excepcional.",
            "low": "Confinamento no preju\u00edzo. Abate precoce poss\u00edvel \u2192 press\u00e3o em LE curto prazo.",
        },
        "zc_zs": {
            "extreme_high": f"Ratio extremo ({value:.4f}). Milho relativamente caro vs soja \u2192 produtores migram para milho.",
            "high": f"Ratio acima do normal ({direction}). Soja mais atrativa para plantio.",
            "normal": "Ratio equilibrado. Sem vantagem clara de plantio.",
            "low": "Milho barato vs soja. Produtores tendem a plantar mais milho.",
        },
        "ke_zw": {
            "extreme_high": f"Pr\u00eamio de prote\u00edna extremo ({direction}). Trigo duro muito demandado.",
            "high": f"Pr\u00eamio HRW acima do normal. Demanda por qualidade forte.",
            "normal": "Spread KC-CBOT dentro do padr\u00e3o sazonal.",
            "low": "Pr\u00eamio de prote\u00edna comprimido. Excesso de trigo duro ou falta de mole.",
        },
        "zl_cl": {
            "extreme_high": f"\u00d3leo de soja extremamente caro vs petr\u00f3leo. Biodiesel perde competitividade.",
            "high": f"Ratio {direction}. \u00d3leo de soja caro \u2014 biodiesel no limite.",
            "normal": "Paridade \u00f3leo/petr\u00f3leo normal. Biodiesel competitivo.",
            "low": "Petr\u00f3leo relativamente caro. Biodiesel de soja ganha espa\u00e7o.",
        },
        "zc_zm": {
            "extreme_high": "Milho muito caro vs farelo. Formuladores trocam milho por farelo na ra\u00e7\u00e3o.",
            "high": f"Milho perdendo competitividade na ra\u00e7\u00e3o ({direction}).",
            "normal": "Equil\u00edbrio milho/farelo normal na formula\u00e7\u00e3o.",
            "low": "Milho barato vs farelo \u2192 mais demanda por milho na ra\u00e7\u00e3o.",
        },
        "feed_wheat": {
            "extreme_high": "Trigo muito caro vs milho. Milho domina formula\u00e7\u00e3o de ra\u00e7\u00e3o.",
            "high": f"Trigo perdendo espa\u00e7o na ra\u00e7\u00e3o ({direction}). Milho mais competitivo.",
            "normal": "Equil\u00edbrio trigo/milho na formula\u00e7\u00e3o.",
            "low": "Trigo barato, compete com milho na ra\u00e7\u00e3o \u2192 press\u00e3o em ZC.",
        },
    }

    level = ("extreme_high" if zscore >= 2.0 else
             "high" if zscore >= 1.0 else
             "low" if zscore <= -1.0 else "normal")

    return signals.get(spread_key, {}).get(level,
        f"Z-score {zscore:+.2f}. Posi\u00e7\u00e3o {direction} vs m\u00e9dia hist\u00f3rica.")

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
                # CME Board Crush: 44 lbs meal + 11 lbs oil per bushel
                val = calculate_crush_spread(values["ZM"], values["ZL"], values["ZS"])
            elif spread_key == "ke_zw":
                val = values["KE"] - values["ZW"]
            elif spread_key == "zl_cl":
                val = values["ZL"] / values["CL"] if values["CL"] > 0 else 0
            elif spread_key == "feedlot":
                # 1000lb steer (10 cwt) out, 750lb calf (7.5 cwt) in, 50 bu corn
                val = values["LE"] * 10 - values["GF"] * 7.5 - (values["ZC"] / 100) * 50
            elif spread_key == "zc_zm":
                val = values["ZC"] / values["ZM"] if values["ZM"] > 0 else 0
            elif spread_key == "zc_zs":
                val = values["ZC"] / values["ZS"] if values["ZS"] > 0 else 0
            elif spread_key == "cattle_crush":
                val = values["LE"] - (values["GF"] * 0.70) - (values["ZC"] * 0.0046)
            elif spread_key == "feed_wheat":
                val = values["ZW"] / values["ZC"] if values["ZC"] > 0 else 0
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
    
    signal_now = generate_signal_now(spread_key, current, round(zscore_1y, 2), regime, trend_pct)

    return {
        "name": spread_def["name"],
        "unit": spread_def["unit"],
        "description": spread_def["description"],
        "category": spread_def.get("category") or SPREAD_CATEGORIES.get(spread_key, "outros"),
        "current": round(current, 4),
        "mean_1y": round(mean_1y, 4),
        "std_1y": round(std_1y, 4),
        "zscore_1y": round(zscore_1y, 2),
        "percentile": round(percentile, 0),
        "regime": regime,
        "trend": trend,
        "trend_pct": round(trend_pct, 1),
        "interpretation": INTERPRETATIONS.get(spread_key, ""),
        "signal_now": signal_now,
        "watch_if": WATCH_IF.get(spread_key, ""),
        "points": len(spread_values),
        "history": spread_values[-60:]  # Last 60 days for charting
    }

def apply_feedlot_cycle(spread_data, futures_data):
    """Override feedlot current value with cycle-corrected forward prices.

    Real feedlot cycle:
      - Buy calf TODAY:         GF contract +1 month
      - Feed for 150-180 days:  ZC contract +4 months
      - Sell finished cattle:   LE contract +6 months

    Formula: LE_exit * 10 - GF_entry * 7.5 - ZC_feed * 50
    (1000 lb steer out, 750 lb calf in, ~50 bu corn consumed)
    """
    gf_entry, gf_label = get_futures_price(futures_data, "GF", 1)
    zc_feed, zc_label = get_futures_price(futures_data, "ZC", 4)
    le_exit, le_label = get_futures_price(futures_data, "LE", 6)

    if not all([gf_entry, zc_feed, le_exit]):
        return  # keep front-month fallback

    # LE, GF in USD/cwt; ZC in cents/bu → convert to USD/bu (/100)
    # 1000 lb steer = 10 cwt out, 750 lb calf = 7.5 cwt in, 50 bu corn
    feedlot_margin = (le_exit * 10) - (gf_entry * 7.5) - (zc_feed / 100 * 50)

    spread_data["current"] = round(feedlot_margin, 2)
    spread_data["contracts"] = {
        "gf": f"GF +1m @ {gf_entry:.2f} ({gf_label})",
        "zc": f"ZC +4m @ {zc_feed:.2f} ({zc_label})",
        "le": f"LE +6m @ {le_exit:.2f} ({le_label})",
    }
    spread_data["method"] = "cycle_corrected"
    # Recalculate z-score with the new current value
    if spread_data.get("std_1y") and spread_data["std_1y"] > 0:
        spread_data["zscore_1y"] = round(
            (feedlot_margin - spread_data["mean_1y"]) / spread_data["std_1y"], 2
        )


def apply_crush_forward(spread_data, futures_data):
    """Add forward crush margin (+2 months) alongside the spot value.

    Industry locks in crush 30-60 days ahead, so the +2m forward
    better reflects real-world margins.  Uses same CME Board Crush
    formula: (ZM*44/2000) + (ZL*11/100) - (ZS/100).
    """
    zs_price, zs_label = get_futures_price(futures_data, "ZS", 2)
    zm_price, zm_label = get_futures_price(futures_data, "ZM", 2)
    zl_price, zl_label = get_futures_price(futures_data, "ZL", 2)

    if not all([zs_price, zm_price, zl_price]):
        return

    crush_fwd = (zm_price * 44 / 2000) + (zl_price * 11 / 100) - (zs_price / 100)

    spread_data["value_forward"] = round(crush_fwd, 4)
    spread_data["contracts"] = {
        "zs": f"ZS +2m @ {zs_price:.2f} ({zs_label})",
        "zm": f"ZM +2m @ {zm_price:.2f} ({zm_label})",
        "zl": f"ZL +2m @ {zl_price:.2f} ({zl_label})",
    }
    spread_data["method"] = "forward_2m"


def apply_zlcl_term_structure(spread_data, futures_data):
    """Add forward ZL/CL ratios at 3m and 6m to detect contango/backwardation divergence."""
    zl_3m, zl_3m_label = get_futures_price(futures_data, "ZL", 3)
    cl_3m, cl_3m_label = get_futures_price(futures_data, "CL", 3)
    zl_6m, zl_6m_label = get_futures_price(futures_data, "ZL", 6)
    cl_6m, cl_6m_label = get_futures_price(futures_data, "CL", 6)

    spot = spread_data.get("current")

    if zl_3m and cl_3m and cl_3m > 0:
        ratio_3m = zl_3m / cl_3m
        spread_data["value_3m"] = round(ratio_3m, 4)
        spread_data["contracts_3m"] = {
            "zl": f"ZL +3m @ {zl_3m:.2f} ({zl_3m_label})",
            "cl": f"CL +3m @ {cl_3m:.2f} ({cl_3m_label})",
        }
        if spot and spot > 0:
            diff = ratio_3m - spot
            spread_data["term_structure"] = round(diff, 4)
            if diff > 0.02:
                spread_data["term_note"] = "Paridade biodiesel melhorando no forward"
            elif diff < -0.02:
                spread_data["term_note"] = "Paridade biodiesel piorando no forward"
            else:
                spread_data["term_note"] = "Curva est\u00e1vel"

    if zl_6m and cl_6m and cl_6m > 0:
        spread_data["value_6m"] = round(zl_6m / cl_6m, 4)
        spread_data["contracts_6m"] = {
            "zl": f"ZL +6m @ {zl_6m:.2f} ({zl_6m_label})",
            "cl": f"CL +6m @ {cl_6m:.2f} ({cl_6m_label})",
        }


def process_spreads(price_file: Path) -> dict:
    """Process all spreads"""
    with open(price_file) as f:
        prices = json.load(f)

    futures_data = load_futures()

    result = {
        "timestamp": datetime.now().isoformat(),
        "spreads": {}
    }

    for spread_key, spread_def in SPREADS.items():
        spread_data = calculate_spread(prices, spread_key, spread_def)
        if spread_data:
            result["spreads"][spread_key] = spread_data

    # Apply forward-curve overrides
    if futures_data:
        if "feedlot" in result["spreads"]:
            apply_feedlot_cycle(result["spreads"]["feedlot"], futures_data)
        if "soy_crush" in result["spreads"]:
            apply_crush_forward(result["spreads"]["soy_crush"], futures_data)
        if "zl_cl" in result["spreads"]:
            apply_zlcl_term_structure(result["spreads"]["zl_cl"], futures_data)

    # Lag metadata (supply-chain delay between signal and market impact)
    if "feedlot" in result["spreads"]:
        result["spreads"]["feedlot"]["lag_months"] = 6
        result["spreads"]["feedlot"]["lag_note"] = (
            "Impacto na oferta bovina em 12-18 meses. "
            "Crush negativo hoje \u2192 menos abate em 2027 Q1."
        )
    if "cattle_crush" in result["spreads"]:
        result["spreads"]["cattle_crush"]["lag_months"] = 12
        result["spreads"]["cattle_crush"]["lag_note"] = (
            "Margem de confinamento impacta decis\u00e3o de engorda. "
            "Margem baixa hoje \u2192 menor oferta LE em 12-18 meses."
        )
    if "soy_crush" in result["spreads"]:
        result["spreads"]["soy_crush"]["lag_months"] = 1
        result["spreads"]["soy_crush"]["lag_note"] = (
            "Margem de esmagamento impacta demanda de ZS em 30-60 dias. "
            "Crush alto \u2192 ind\u00fastria compra mais soja."
        )

    return result

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        result = process_spreads(Path(sys.argv[1]))
    else:
        result = process_spreads(Path("../agrimacro-dash/public/data/raw/price_history.json"))

    # Save to spreads.json
    out_path = Path(__file__).parent.parent / "agrimacro-dash" / "public" / "data" / "processed" / "spreads.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"Processed {len(result['spreads'])} spreads:")
    for key, data in result["spreads"].items():
        print(f"  {key}: {data['current']:.4f} {data['unit']} | Z={data['zscore_1y']:+.2f} | {data['regime']} | {data['trend']}")
    print(f"Saved to {out_path}")
