import json, os, math, requests
from pathlib import Path
from datetime import datetime, timedelta

BASE = Path(__file__).parent.parent
PRICES = BASE / "agrimacro-dash/public/data/processed/price_history.json"
FUTURES = BASE / "agrimacro-dash/public/data/processed/futures_contracts.json"
PHYSICAL = BASE / "agrimacro-dash/public/data/processed/physical_br.json"
EIA_JSON = BASE / "agrimacro-dash/public/data/processed/eia_data.json"
OUT = BASE / "agrimacro-dash/public/data/processed/parities.json"

def load_prices():
    with open(PRICES, encoding="utf-8") as f:
        return json.load(f)

def load_futures():
    try:
        with open(FUTURES, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def get_new_crop(futures, sym, month_code):
    """Retorna close do primeiro contrato new-crop (month_code) disponível.
    Ex: get_new_crop(futures, "ZC", "Z") -> ZCZ26 close
        get_new_crop(futures, "ZS", "X") -> ZSX26 close
    """
    contracts = futures.get("commodities", {}).get(sym, {}).get("contracts", [])
    for c in contracts:
        if c.get("month_code") == month_code and c.get("close") and c["close"] > 0:
            return c["close"], c.get("contract", "")
    return None, None

def get_last(prices, sym):
    """Retorna último preço de um símbolo."""
    bars = prices.get(sym, [])
    if isinstance(bars, dict):
        bars = bars.get("bars", [])
    return bars[-1]["close"] if bars else None

def zscore(series, window=52):
    """Z-score de uma série usando janela de N períodos."""
    if len(series) < 10:
        return 0.0
    s = series[-window:] if len(series) >= window else series
    mean = sum(s) / len(s)
    std = math.sqrt(sum((x - mean)**2 for x in s) / len(s))
    return round((s[-1] - mean) / std, 2) if std > 0 else 0.0

def trend(series, days=7):
    """Tendência: diferença % entre último e N dias atrás."""
    if len(series) < days + 1:
        return 0.0
    return round((series[-1] - series[-days-1]) / series[-days-1] * 100, 2)

def get_bars(prices, sym):
    """Retorna lista de bars de um símbolo."""
    data = prices.get(sym, [])
    if isinstance(data, dict):
        return data.get("bars", [])
    return data if isinstance(data, list) else []

def calc_ratio_series(prices, sym_a, sym_b, divisor_a=1, divisor_b=1):
    """Calcula série de ratio entre dois símbolos alinhados por data."""
    bars_a = get_bars(prices, sym_a)
    bars_b = get_bars(prices, sym_b)
    # Alinhar por data
    dates_a = {b["date"]: b["close"] for b in bars_a}
    dates_b = {b["date"]: b["close"] for b in bars_b}
    common = sorted(set(dates_a) & set(dates_b))
    series = [(dates_a[d]/divisor_a) / (dates_b[d]/divisor_b)
              for d in common if dates_b[d] != 0]
    return series, common

def main():
    prices = load_prices()
    futures = load_futures()

    parities = {}
    generated_at = datetime.now().isoformat()

    # ─────────────────────────────────────────────
    # 1. ZC/ZS RATIO — Decisão de acreagem Corn Belt
    # Usa contratos NEW CROP: ZCZ (milho dezembro) / ZSX (soja novembro)
    # da curva de futuros em futures_contracts.json
    # Zona crítica: < 2.3 = favorece milho | > 2.5 = favorece soja
    # ─────────────────────────────────────────────
    zc_nc, zc_label = get_new_crop(futures, "ZC", "Z")  # dezembro
    zs_nc, zs_label = get_new_crop(futures, "ZS", "X")  # novembro
    # fallback para front-month se futures indisponível
    zc = zc_nc or get_last(prices, "ZC")
    zs = zs_nc or get_last(prices, "ZS")
    nc_source = "new-crop" if (zc_nc and zs_nc) else "front-month (fallback)"
    if zc and zs:
        ratio = zc / zs
        series, _ = calc_ratio_series(prices, "ZC", "ZS")
        z = zscore(series)
        t7 = trend(series, 7)
        t30 = trend(series, 30)

        if ratio < 2.30:
            signal = "FAVORECE MILHO"
            color = "#00C878"
        elif ratio < 2.50:
            signal = "NEUTRO"
            color = "#DCB432"
        else:
            signal = "FAVORECE SOJA"
            color = "#DC3C3C"

        parities["zc_zs"] = {
            "name": "Ratio Milho/Soja (New Crop)",
            "description": f"Acreagem Corn Belt via {zc_label or 'ZC'}/{zs_label or 'ZS'}. <2.3 = milho, >2.5 = soja",
            "value": round(ratio, 4),
            "unit": "ratio",
            "z_score": z,
            "trend_7d": t7,
            "trend_30d": t30,
            "signal": signal,
            "signal_color": color,
            "threshold_low": 2.30,
            "threshold_high": 2.50,
            "category": "acreagem",
            "source": nc_source,
            "contracts": {"corn": zc_label, "soy": zs_label}
        }

    # ─────────────────────────────────────────────
    # 2. ZC/ZW RATIO — Milho vs Trigo nas bordas
    # ─────────────────────────────────────────────
    zw = get_last(prices, "ZW")
    if zc and zw:
        ratio = zc / zw
        series, _ = calc_ratio_series(prices, "ZC", "ZW")
        z = zscore(series)
        t7 = trend(series, 7)

        if ratio < 0.75:
            signal = "FAVORECE TRIGO"
            color = "#DC3C3C"
        elif ratio < 0.90:
            signal = "NEUTRO"
            color = "#DCB432"
        else:
            signal = "FAVORECE MILHO"
            color = "#00C878"

        parities["zc_zw"] = {
            "name": "Ratio Milho/Trigo",
            "description": "Competição nas bordas do Corn Belt (Kansas/Oklahoma)",
            "value": round(ratio, 4),
            "unit": "ratio",
            "z_score": z,
            "trend_7d": t7,
            "trend_30d": trend(series, 30),
            "signal": signal,
            "signal_color": color,
            "threshold_low": 0.75,
            "threshold_high": 0.90,
            "category": "acreagem"
        }

    # ─────────────────────────────────────────────
    # 3. CL vs ZL — Paridade Biodiesel
    # ZL teórico = CL × 0.0071 (paridade energética)
    # ─────────────────────────────────────────────
    cl = get_last(prices, "CL")
    zl = get_last(prices, "ZL")
    if cl and zl:
        zl_teorico = cl * 0.0071
        premium = ((zl / 100) - zl_teorico) / zl_teorico * 100

        # Série histórica do spread
        bars_cl = get_bars(prices, "CL")
        bars_zl = get_bars(prices, "ZL")
        dates_cl = {b["date"]: b["close"] for b in bars_cl}
        dates_zl = {b["date"]: b["close"] for b in bars_zl}
        common = sorted(set(dates_cl) & set(dates_zl))
        spread_series = [(dates_zl[d]/100) - dates_cl[d]*0.0071
                         for d in common]
        z = zscore(spread_series)
        t7 = trend(spread_series, 7)

        if premium > 5:
            signal = "ZL PREMIUM \u2014 Biodiesel suporta"
            color = "#00C878"
        elif premium > -5:
            signal = "NA PARIDADE"
            color = "#DCB432"
        else:
            signal = "ZL DESCONTO \u2014 Press\u00e3o baixista"
            color = "#DC3C3C"

        parities["cl_zl_biodiesel"] = {
            "name": "Paridade CL/ZL Biodiesel",
            "description": "ZL te\u00f3rico baseado em CL \u00d7 0.0071. Premium = suporte, Desconto = press\u00e3o",
            "value": round(premium, 2),
            "unit": "%",
            "zl_atual": round(zl/100, 4),
            "zl_teorico": round(zl_teorico, 4),
            "cl_atual": round(cl, 2),
            "z_score": z,
            "trend_7d": t7,
            "signal": signal,
            "signal_color": color,
            "threshold_low": -5.0,
            "threshold_high": 5.0,
            "category": "energia",
            "lag_months": 6,
            "lag_note": (
                "CL alto hoje \u2192 ureia/am\u00f4nia sobe em 6-12 meses "
                "\u2192 custo plantio aumenta na safra seguinte."
            )
        }

    # ─────────────────────────────────────────────
    # 4. DXY vs Basket Grãos — Correlação inversa
    # ─────────────────────────────────────────────
    dx = get_last(prices, "DX")
    if dx and zc and zs and zw:
        basket = (zc + zs/10 + zw) / 3

        bars_dx = get_bars(prices, "DX")
        dates_dx = {b["date"]: b["close"] for b in bars_dx}
        dates_zc = {b["date"]: b["close"]
                    for b in get_bars(prices, "ZC")}
        dates_zs = {b["date"]: b["close"]
                    for b in get_bars(prices, "ZS")}
        dates_zw = {b["date"]: b["close"]
                    for b in get_bars(prices, "ZW")}

        common = sorted(
            set(dates_dx) & set(dates_zc) & set(dates_zs) & set(dates_zw)
        )[-252:]

        if len(common) > 20:
            dx_series = [dates_dx[d] for d in common]
            grain_series = [(dates_zc[d] + dates_zs[d]/10 + dates_zw[d])/3
                            for d in common]

            n = len(dx_series)
            dx_mean = sum(dx_series)/n
            g_mean = sum(grain_series)/n
            cov = sum((dx_series[i]-dx_mean)*(grain_series[i]-g_mean)
                      for i in range(n))/n
            dx_std = math.sqrt(sum((x-dx_mean)**2 for x in dx_series)/n)
            g_std = math.sqrt(sum((x-g_mean)**2 for x in grain_series)/n)
            corr = round(cov/(dx_std*g_std), 3) if dx_std*g_std > 0 else 0

            dx_7d = trend([dates_dx[d] for d in common], 7)

            if dx_7d < -0.5:
                signal = "DXY CAINDO \u2014 Suporte para gr\u00e3os"
                color = "#00C878"
            elif dx_7d > 0.5:
                signal = "DXY SUBINDO \u2014 Press\u00e3o em gr\u00e3os"
                color = "#DC3C3C"
            else:
                signal = "DXY EST\u00c1VEL"
                color = "#DCB432"

            parities["dxy_grains"] = {
                "name": "DXY vs Basket Gr\u00e3os",
                "description": "Correla\u00e7\u00e3o inversa hist\u00f3rica: DXY -1% \u2248 gr\u00e3os +0.5-1%",
                "value": corr,
                "unit": "correla\u00e7\u00e3o",
                "dxy_atual": round(dx, 2),
                "dxy_7d_pct": dx_7d,
                "basket_atual": round(basket, 2),
                "signal": signal,
                "signal_color": color,
                "category": "macro"
            }

    # ─────────────────────────────────────────────
    # 5. BRL vs Competitividade Exportação (SB/ZS/ZC)
    # ─────────────────────────────────────────────
    try:
        with open(BASE / "agrimacro-dash/public/data/processed/bcb_data.json", encoding="utf-8") as f:
            bcb = json.load(f)
        brl_series = bcb.get("brl_usd", [])
        brl = brl_series[-1]["value"] if brl_series else None
    except:
        brl = None

    if brl and zs:
        # ── Basis real: FOB Paranagu\u00e1 vs Chicago ──
        # Carrega pre\u00e7o f\u00edsico BR (R$/saca 60kg)
        fob_paranagua_brl = None
        try:
            with open(PHYSICAL, encoding="utf-8") as f:
                phys_br = json.load(f)
            fob_paranagua_brl = phys_br.get("products", {}).get("ZS_BR", {}).get("price")
        except Exception:
            pass

        # Chicago em USD/bu (ZS em cents/bu)
        zs_chicago_usd = zs / 100

        if fob_paranagua_brl and brl > 0:
            # 1 saca 60kg = 60/27.216 = 2.2046 bushels
            BU_PER_SACA = 60 / 27.216
            fob_paranagua_usd = (fob_paranagua_brl / brl) / BU_PER_SACA

            basis = fob_paranagua_usd - zs_chicago_usd

            if basis < -0.50:
                signal = "BRASIL MUITO COMPETITIVO"
                color = "#00C878"
            elif basis < -0.10:
                signal = "BRASIL COMPETITIVO"
                color = "#00C878"
            elif basis <= 0.10:
                signal = "PARIDADE"
                color = "#DCB432"
            else:
                signal = "BRASIL CARO"
                color = "#DC3C3C"

            parities["brl_competitiveness"] = {
                "name": "Competitividade BR vs Chicago (Basis Real)",
                "description": "FOB Paranagu\u00e1 convertido vs ZS CBOT. Negativo = Brasil mais barato",
                "value": round(basis, 4),
                "unit": "USD/bu (FOB Paranagu\u00e1 - Chicago)",
                "fob_paranagua_brl": round(fob_paranagua_brl, 2),
                "fob_paranagua_usd": round(fob_paranagua_usd, 4),
                "zs_chicago_usd": round(zs_chicago_usd, 4),
                "brl_usd": round(brl, 4),
                "z_score": 0.0,
                "signal": signal,
                "signal_color": color,
                "category": "macro",
                "interpretation": f"FOB Paranagu\u00e1 {'abaixo' if basis < 0 else 'acima'} de Chicago em {abs(basis):.4f} USD/bu"
            }
        else:
            # Fallback: sem f\u00edsico BR, manter indicador cambial simples
            zs_brl = zs * brl / 100
            bars_zs = get_bars(prices, "ZS")
            zs_brl_series = [b["close"] * brl / 100 for b in bars_zs[-52:]]
            z = zscore(zs_brl_series)

            if brl > 5.20:
                signal = "BRL FRACO \u2014 BR competitivo"
                color = "#00C878"
            elif brl > 4.80:
                signal = "BRL NEUTRO"
                color = "#DCB432"
            else:
                signal = "BRL FORTE \u2014 BR menos competitivo"
                color = "#DC3C3C"

            parities["brl_competitiveness"] = {
                "name": "Competitividade BR (BRL/USD \u00d7 ZS)",
                "description": "Fallback cambial \u2014 f\u00edsico BR indispon\u00edvel",
                "value": round(brl, 4),
                "unit": "BRL/USD",
                "zs_em_brl": round(zs_brl, 2),
                "z_score": z,
                "signal": signal,
                "signal_color": color,
                "threshold_low": 4.80,
                "threshold_high": 5.20,
                "category": "macro",
                "is_fallback": True
            }

    # ─────────────────────────────────────────────
    # 6. ZM/ZS — Valor relativo Farelo/Soja
    # (crush value split)
    # ─────────────────────────────────────────────
    zm = get_last(prices, "ZM")
    if zm and zs:
        # ZM em $/ton, ZS em cents/bu → ZS em $/ton = ZS*36.744/100
        zs_ton = zs * 36.744 / 100
        meal_share = (zm / zs_ton) * 100

        series_zm = [b["close"] for b in get_bars(prices, "ZM")]
        series_zs = [b["close"]*36.744/100
                     for b in get_bars(prices, "ZS")]
        if len(series_zm) == len(series_zs) and len(series_zm) > 10:
            ratio_series = [series_zm[i]/series_zs[i]*100
                            for i in range(len(series_zm))
                            if series_zs[i] > 0]
            z = zscore(ratio_series)
            t7 = trend(ratio_series, 7)
        else:
            z = 0.0
            t7 = 0.0

        if meal_share > 65:
            signal = "FARELO DOMINANTE \u2014 Esmagamento atrativo"
            color = "#00C878"
        elif meal_share > 55:
            signal = "EQUILIBRADO"
            color = "#DCB432"
        else:
            signal = "\u00d3LEO DOMINANTE"
            color = "#DCB432"

        parities["zm_zs_ratio"] = {
            "name": "Farelo/Soja \u2014 Split de Valor",
            "description": "% do valor da soja representado pelo farelo. Alto = crush lucrativo",
            "value": round(meal_share, 1),
            "unit": "%",
            "zm_atual": round(zm, 2),
            "zs_ton": round(zs_ton, 2),
            "z_score": z,
            "trend_7d": t7,
            "signal": signal,
            "signal_color": color,
            "category": "crush"
        }

    # ─────────────────────────────────────────────
    # 7. Gasolina vs Etanol — Blend Wall EUA
    # Etanol competitivo quando < $0.90 da gasolina
    # ─────────────────────────────────────────────
    try:
        with open(EIA_JSON, encoding="utf-8") as f:
            eia = json.load(f)
        gas_price = None
        series = eia.get("series", {})
        gas_retail = series.get("gasoline_retail", {})
        if gas_retail:
            gas_price = gas_retail.get("latest_value")
        if not gas_price:
            diesel_retail = series.get("diesel_retail", {})
            gas_price = diesel_retail.get("latest_value")
    except:
        gas_price = None

    try:
        with open(PHYSICAL, encoding="utf-8") as f:
            phys = json.load(f)
        products = phys.get("products", {})
        eth_br = products.get("ETH_BR", {})
        ethanol_brl = eth_br.get("price")
    except:
        ethanol_brl = None

    if gas_price:
        ethanol_usd = None
        if ethanol_brl and brl:
            try:
                ethanol_usd = float(ethanol_brl) / float(brl) * 3.785
            except:
                pass

        if ethanol_usd:
            spread = gas_price - ethanol_usd
            if spread > 0.90:
                signal = "ETANOL MUITO COMPETITIVO"
                color = "#00C878"
            elif spread > 0.30:
                signal = "ETANOL COMPETITIVO"
                color = "#00C878"
            elif spread > 0:
                signal = "PARIDADE"
                color = "#DCB432"
            else:
                signal = "GASOLINA MAIS BARATA"
                color = "#DC3C3C"

            parities["gasoline_ethanol"] = {
                "name": "Gasolina vs Etanol (Blend Wall)",
                "description": "Spread >$0.90 = etanol muito competitivo, suporte para ZC via demanda",
                "value": round(spread, 3),
                "unit": "$/gal\u00e3o",
                "gas_price": round(gas_price, 3),
                "ethanol_usd": round(ethanol_usd, 3),
                "ethanol_brl": round(float(ethanol_brl), 4) if ethanol_brl else None,
                "signal": signal,
                "signal_color": color,
                "threshold_low": 0.30,
                "threshold_high": 0.90,
                "category": "energia"
            }

    # ─────────────────────────────────────────────
    # 8. Etanol vs Açúcar — Paridade Usina Brasil
    # ─────────────────────────────────────────────
    try:
        eth_val = None
        sug_val = None
        if isinstance(phys, dict):
            products = phys.get("products", {})
            eth_val = products.get("ETH_BR", {}).get("price")
            sug_val = products.get("SB_BR", {}).get("price")

        if eth_val and sug_val and brl:
            eth_float = float(eth_val)
            sug_float = float(sug_val)

            # Paridade: 1 saca açúcar (50kg) vs equivalente etanol
            # 1 ton cana → 82kg açúcar OU 85L etanol
            # Paridade: açúcar favorável quando preço relativo > 1.15
            ratio_eth_sug = (eth_float * 100) / sug_float

            if ratio_eth_sug > 1.20:
                signal = "ETANOL FAVOR\u00c1VEL \u2014 Usinas produzem mais etanol"
                color = "#00C878"
            elif ratio_eth_sug > 0.85:
                signal = "EQUILIBRADO \u2014 Mix neutro"
                color = "#DCB432"
            else:
                signal = "A\u00c7\u00daCAR FAVOR\u00c1VEL \u2014 Usinas produzem mais a\u00e7\u00facar"
                color = "#DC3C3C"

            parities["ethanol_sugar_brazil"] = {
                "name": "Paridade Etanol/A\u00e7\u00facar (Usinas BR)",
                "description": "Define mix produ\u00e7\u00e3o usinas. >1.20 = mais etanol, <0.85 = mais a\u00e7\u00facar",
                "value": round(ratio_eth_sug, 3),
                "unit": "ratio",
                "ethanol_brl_litro": round(eth_float, 4),
                "sugar_brl_saca": round(sug_float, 2),
                "signal": signal,
                "signal_color": color,
                "threshold_low": 0.85,
                "threshold_high": 1.20,
                "category": "softs"
            }
    except Exception as e:
        print(f"[WARN] Ethanol/Sugar parity: {e}")

    # ─────────────────────────────────────────────
    # 9. NG vs Ureia — Custo Fertilizante (lag 90d)
    # Usando EIA NG + estimativa ureia via fórmula
    # ─────────────────────────────────────────────
    ng = get_last(prices, "NG")
    if ng:
        # Fórmula: Ureia spot ≈ NG × 6.5 + $120 (margem+outros)
        urea_est = ng * 6.5 + 120

        # Custo de nitrogênio por bushel de milho
        # ZC precisa ~120 lbs N/acre, yield 183 bu/acre
        # N cost per bu = (ureia_$/ton × 0.46 N content) × 120/2000 / 183
        n_cost_per_bu = (urea_est * 0.46 * 120/2000) / 183

        # Horizonte de impacto: lag 90 dias
        impact_date = (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d")

        if urea_est < 350:
            signal = "FERTILIZANTE BARATO \u2014 CMP ZC menor"
            color = "#00C878"
        elif urea_est < 500:
            signal = "FERTILIZANTE NORMAL"
            color = "#DCB432"
        else:
            signal = "FERTILIZANTE CARO \u2014 CMP ZC sobe em 90d"
            color = "#DC3C3C"

        parities["ng_urea_lag"] = {
            "name": "NG \u2192 Custo Ureia (lag 90 dias)",
            "description": "NG caro \u2192 ureia sobe \u2192 CMP milho sobe em 90 dias",
            "value": round(urea_est, 0),
            "unit": "$/ton (estimado)",
            "ng_atual": round(ng, 3),
            "n_cost_per_bu_zc": round(n_cost_per_bu, 3),
            "impact_date": impact_date,
            "lag_days": 90,
            "signal": signal,
            "signal_color": color,
            "threshold_low": 350,
            "threshold_high": 500,
            "note": "Estimativa via f\u00f3rmula NG\u00d76.5+120. Hormuz = usar +$140 de pr\u00eamio geopol\u00edtico",
            "category": "insumos"
        }

    # ─────────────────────────────────────────────
    # 10. LE/ZC RATIO — Cattle/Corn Ratio (padr\u00e3o do setor)
    # LE (cents/lb \u2248 $/cwt) \u00f7 ZC ($/bu)
    # Breakeven hist\u00f3rico: 28-32 bu/cwt
    # ─────────────────────────────────────────────
    le = get_last(prices, "LE")   # cents/lb \u2248 $/cwt
    zc = get_last(prices, "ZC")   # cents/bu
    if le and zc:
        cattle_corn_ratio = le / (zc / 100)  # $/cwt \u00f7 $/bu = bu/cwt
        series_ratio, _ = calc_ratio_series(prices, "LE", "ZC", divisor_a=1, divisor_b=0.01)
        z = zscore(series_ratio)
        t7 = trend(series_ratio, 7)
        t30 = trend(series_ratio, 30)

        if cattle_corn_ratio > 38:
            signal = "EXCELENTE \u2014 Confinamento muito lucrativo"
            color = "#00C878"
        elif cattle_corn_ratio > 32:
            signal = "LUCRATIVO \u2014 Expans\u00e3o de rebanho esperada"
            color = "#00C878"
        elif cattle_corn_ratio > 28:
            signal = "BREAKEVEN \u2014 Margem apertada"
            color = "#DCB432"
        else:
            signal = "PREJU\u00cdZO \u2014 Abate precoce prov\u00e1vel"
            color = "#DC3C3C"

        parities["le_zc_ratio"] = {
            "name": "Cattle/Corn Ratio (Padr\u00e3o Setor)",
            "description": "Quantos bu de milho 1 cwt de boi compra. Breakeven hist\u00f3rico: 28-32 bu/cwt.",
            "value": round(cattle_corn_ratio, 2),
            "unit": "bu de milho por cwt de boi",
            "z_score": z,
            "trend_7d": t7,
            "trend_30d": t30,
            "signal": signal,
            "signal_color": color,
            "breakeven": 28.0,
            "threshold_low": 28.0,
            "threshold_high": 38.0,
            "category": "pecuaria",
            "industry_note": "Breakeven hist\u00f3rico: 28-32 bu/cwt",
            "lag_months": 12,
            "lag_note": (
                "Ratio baixo hoje \u2192 menos bezerros engordando "
                "\u2192 menor oferta LE em 12-18 meses."
            )
        }

    # ─────────────────────────────────────────────
    # 11. GF/LE SPREAD — Feeder vs Live Cattle
    # Diferença entre bezerro e boi gordo
    # Alto = recria cara (menos animais entrando)
    # ─────────────────────────────────────────────
    gf = get_last(prices, "GF")   # cents/lb
    if gf and le:
        spread = gf - le  # cents/lb
        bars_gf = get_bars(prices, "GF")
        bars_le2 = get_bars(prices, "LE")
        dates_gf = {b["date"]: b["close"] for b in bars_gf}
        dates_le2 = {b["date"]: b["close"] for b in bars_le2}
        common = sorted(set(dates_gf) & set(dates_le2))
        spread_series = [dates_gf[d] - dates_le2[d] for d in common]

        z = zscore(spread_series)
        t7 = trend(spread_series, 7)
        t30 = trend(spread_series, 30)

        if spread > 130:
            signal = "RECRIA MUITO CARA \u2014 Contra\u00e7\u00e3o de rebanho"
            color = "#DC3C3C"
        elif spread > 100:
            signal = "RECRIA CARA \u2014 Vigil\u00e2ncia"
            color = "#DCB432"
        elif spread > 60:
            signal = "SPREAD NORMAL"
            color = "#64748b"
        else:
            signal = "RECRIA BARATA \u2014 Expans\u00e3o poss\u00edvel"
            color = "#00C878"

        parities["gf_le_spread"] = {
            "name": "Feeder vs Live Cattle (Ciclo de Recria)",
            "description": "Diferen\u00e7a de pre\u00e7o entre bezerro (GF) e boi gordo (LE). Alto = recria cara = menos animais entrando no ciclo = alta futura de LE.",
            "value": round(spread, 2),
            "unit": "cents/lb",
            "z_score": z,
            "trend_7d": t7,
            "trend_30d": t30,
            "signal": signal,
            "signal_color": color,
            "threshold_low": 60.0,
            "threshold_high": 130.0,
            "category": "pecuaria"
        }

    # ─────────────────────────────────────────────
    # 12. HE/ZC RATIO — Hog/Corn Ratio (padr\u00e3o do setor)
    # FCR su\u00edno real: 3.5-4 kg ra\u00e7\u00e3o/kg ganho
    # Breakeven hist\u00f3rico: 12-14 bu/cwt
    # ─────────────────────────────────────────────
    he = get_last(prices, "HE")   # cents/lb \u2248 $/cwt
    if he and zc:
        hog_corn_ratio = he / (zc / 100)  # $/cwt \u00f7 $/bu = bu/cwt
        series_ratio_he, _ = calc_ratio_series(prices, "HE", "ZC", divisor_a=1, divisor_b=0.01)
        z = zscore(series_ratio_he)
        t7 = trend(series_ratio_he, 7)
        t30 = trend(series_ratio_he, 30)

        if hog_corn_ratio > 20:
            signal = "EXCELENTE \u2014 Produ\u00e7\u00e3o muito lucrativa"
            color = "#00C878"
        elif hog_corn_ratio > 16:
            signal = "LUCRATIVO"
            color = "#00C878"
        elif hog_corn_ratio > 12:
            signal = "BREAKEVEN \u2014 Margem apertada"
            color = "#DCB432"
        else:
            signal = "PREJU\u00cdZO \u2014 Redu\u00e7\u00e3o de produ\u00e7\u00e3o prov\u00e1vel"
            color = "#DC3C3C"

        parities["he_zc_ratio"] = {
            "name": "Hog/Corn Ratio (Padr\u00e3o Setor)",
            "description": "Quantos bu de milho 1 cwt de su\u00edno compra. FCR real: 3.5-4x. Breakeven: 12-14 bu/cwt.",
            "value": round(hog_corn_ratio, 2),
            "unit": "bu de milho por cwt de su\u00edno",
            "z_score": z,
            "trend_7d": t7,
            "trend_30d": t30,
            "signal": signal,
            "signal_color": color,
            "breakeven": 12.0,
            "threshold_low": 12.0,
            "threshold_high": 20.0,
            "category": "pecuaria",
            "industry_note": "FCR su\u00edno: ~3.5-4x. Breakeven: 12-14 bu/cwt",
            "lag_months": 9,
            "lag_note": (
                "Ratio baixo hoje \u2192 produtores reduzem plantel "
                "\u2192 menor oferta HE em 9-12 meses."
            )
        }

    # Salvar
    output = {
        "generated_at": generated_at,
        "parities": parities,
        "count": len(parities)
    }

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"[OK] {len(parities)} paridades calculadas \u2192 parities.json")
    for k, v in parities.items():
        sig = v.get("signal", "")
        val = v.get("value", "")
        unit = v.get("unit", "")
        z = v.get("z_score", "")
        print(f"  {v['name']:35s} | {str(val):8s} {unit:12s} | z={z} | {sig}")

if __name__ == "__main__":
    main()
