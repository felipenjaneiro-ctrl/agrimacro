"""
AgriMacro v3.0 - Stocks Watch Processor
Uses REAL USDA data where available, price proxy where not.
ZERO MOCK - every number has a real source.
"""
import json
import requests
from datetime import datetime
from pathlib import Path

USDA_KEY = "BA43C01C-A885-3616-9774-EFF03A68F06A"
USDA_BASE = "https://quickstats.nass.usda.gov/api/api_GET/"

USDA_STOCKS = {
    "ZC": {"commodity": "CORN", "stat": "STOCKS", "filter": "CORN, GRAIN - STOCKS, MEASURED IN BU", "unit": "billion bu", "divisor": 1e9},
    "ZS": {"commodity": "SOYBEANS", "stat": "STOCKS", "filter": "SOYBEANS - STOCKS, MEASURED IN BU", "unit": "billion bu", "divisor": 1e9},
    "ZW": {"commodity": "WHEAT", "stat": "STOCKS", "filter": "WHEAT - STOCKS, MEASURED IN BU", "unit": "billion bu", "divisor": 1e9},
    "KE": {"commodity": "WHEAT", "stat": "STOCKS", "filter": "WHEAT - STOCKS, MEASURED IN BU", "unit": "billion bu", "divisor": 1e9},
    "LE": {"commodity": "CATTLE", "stat": "INVENTORY", "filter": "CATTLE, INCL CALVES - INVENTORY", "unit": "million head", "divisor": 1e6},
    "GF": {"commodity": "CATTLE", "stat": "INVENTORY", "filter": "CATTLE, ON FEED - INVENTORY", "unit": "million head", "divisor": 1e6},
    "HE": {"commodity": "HOGS", "stat": "INVENTORY", "filter": "HOGS - INVENTORY", "unit": "million head", "divisor": 1e6},
}

PRICE_PROXY_ONLY = ["CT", "ZM", "ZL", "SB", "KC", "CC", "OJ", "CL", "NG", "GC", "SI", "DX"]


# --- PSD INTEGRATION v3.3 ---
PSD_STOCK_FILE = Path(__file__).parent / ".." / "agrimacro-dash" / "public" / "data" / "processed" / "psd_ending_stocks.json"

PSD_UNIT_MAP = {
    "CT": "mil fardos 480lb",
    "ZM": "mil ton metricas",
    "ZL": "mil ton metricas",
    "SB": "mil ton metricas",
    "KC": "mil sacas 60kg",
    "OJ": "ton metricas",
}


def load_psd_stocks():
    """Load PSD ending stocks data if available"""
    try:
        psd_path = PSD_STOCK_FILE.resolve()
        if psd_path.exists():
            with open(psd_path, encoding="utf-8") as f:
                data = json.load(f)
            comms = data.get("commodities", {})
            print(f"  [PSD] Loaded {len(comms)} commodities from psd_ending_stocks.json")
            return comms
        else:
            print(f"  [PSD] File not found: {psd_path}")
            return {}
    except Exception as e:
        print(f"  [PSD] Error loading: {e}")
        return {}


def _make_psd_entry(symbol, psd, price, seasonality):
    """Create stocks entry from PSD ending stocks data"""
    current = psd["current"]
    avg_5y = psd["avg_5y"]
    deviation = psd["deviation"]
    unit = PSD_UNIT_MAP.get(symbol, psd.get("unit", ""))
    if deviation < -15:
        state = "APERTO"
    elif deviation < -5:
        state = "NEUTRO_VIES_APERTO"
    elif deviation > 15:
        state = "EXCESSO"
    elif deviation > 5:
        state = "NEUTRO_VIES_EXCESSO"
    else:
        state = "NEUTRO"
    factors = []
    if state in ("APERTO", "NEUTRO_VIES_APERTO"):
        factors.append(f"Estoque {abs(deviation):.1f}% abaixo da media (USDA PSD)")
    elif state in ("EXCESSO", "NEUTRO_VIES_EXCESSO"):
        factors.append(f"Estoque {deviation:.1f}% acima da media (USDA PSD)")
    else:
        factors.append(f"Estoque {deviation:+.1f}% vs media (USDA PSD)")
    factors.append(f"Dado: ending stocks {psd.get('year', '?')} (USDA FAS)")
    history = [{"year": h["year"], "period": "ANNUAL", "value": h["value"]} for h in psd.get("history", [])]
    return {
        "symbol": symbol, "price": round(price, 2) if price else None,
        "stock_current": current, "stock_avg": avg_5y,
        "stock_unit": unit, "price_vs_avg": deviation,
        "state": state, "factors": factors,
        "data_available": {"stock_real": True, "stock_source": "USDA PSD Online", "stock_proxy": False, "curve": False, "cot": False},
        "stock_history": history
    }


def fetch_usda_stocks(commodity, stat, desc_filter=None, years_back=6):
    min_year = datetime.now().year - years_back
    params = {"key": USDA_KEY, "commodity_desc": commodity, "statisticcat_desc": stat,
              "year__GE": str(min_year), "agg_level_desc": "NATIONAL", "format": "JSON"}
    try:
        r = requests.get(USDA_BASE, params=params, timeout=30)
        if r.status_code != 200:
            return []
        rows = r.json().get("data", [])
        result = []
        for row in rows:
            sd = row.get("short_desc", "")
            if desc_filter and desc_filter not in sd:
                continue
            if "MEASURED IN $" in sd:
                continue
            val_str = row.get("Value", "").replace(",", "")
            try:
                val = float(val_str)
            except (ValueError, TypeError):
                continue
            result.append({"year": int(row.get("year", 0)), "period": row.get("reference_period_desc", ""),
                           "value": val, "desc": sd})
        po = {"FIRST OF JAN":1,"FIRST OF MAR":3,"FIRST OF JUN":6,"FIRST OF SEP":9,"FIRST OF DEC":12,
              "JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12,"YEAR":12}
        result.sort(key=lambda x: (x["year"], po.get(x["period"], 0)))
        return result
    except Exception as e:
        print(f"    USDA error for {commodity}/{stat}: {e}")
        return []


def analyze_usda_stocks(records, divisor):
    if not records:
        return None

    # --- DATA QUALITY LOG v3.3 ---
    # Log unique short_desc values to catch mixed series
    unique_descs = set(r.get("desc", "") for r in records)
    if len(unique_descs) > 1:
        print(f"    âš ï¸ MIXED SERIES ({len(unique_descs)} distinct):")
        for d in sorted(unique_descs):
            count = sum(1 for r in records if r.get("desc") == d)
            print(f"       [{count:3d}] {d}")

    # Deduplicate: keep only the largest value per year+period (total, not sub-categories)
    seen = {}
    for r in records:
        key = f"{r['year']}-{r['period']}"
        if key not in seen or r['value'] > seen[key]['value']:
            seen[key] = r
    records = sorted(seen.values(), key=lambda x: (x['year'], x.get('_sort', 0)))
    # Re-sort by period order
    po = {"FIRST OF JAN":1,"FIRST OF MAR":3,"FIRST OF JUN":6,"FIRST OF JUL":7,"FIRST OF SEP":9,"FIRST OF DEC":12,
          "JAN":1,"MAR":3,"JUN":6,"JUL":7,"SEP":9,"DEC":12,"END OF DEC":12,"YEAR":13}
    records.sort(key=lambda x: (x['year'], po.get(x['period'], 6)))
    latest = records[-1]
    latest_val = latest["value"]
    latest_period = latest["period"]
    latest_year = latest["year"]
    same_period = [r for r in records if r["period"] == latest_period and r["year"] != latest_year]
    if not same_period:
        same_period = [r for r in records if r["year"] != latest_year]
    if not same_period:
        return {"current": latest_val / divisor, "avg_5y": None, "deviation": None, "state": "SEM HISTORICO", "trend": None, "latest_period": f"{latest_year} {latest_period}"}
    avg_val = sum(r["value"] for r in same_period) / len(same_period)
    deviation = ((latest_val - avg_val) / avg_val) * 100 if avg_val > 0 else 0
    if deviation < -15:
        state = "APERTO"
    elif deviation < -5:
        state = "NEUTRO_VIES_APERTO"
    elif deviation > 15:
        state = "EXCESSO"
    elif deviation > 5:
        state = "NEUTRO_VIES_EXCESSO"
    else:
        state = "NEUTRO"
    # --- SANITY CHECK v3.3 ---
    # Flag extreme deviations (>80%) as suspicious data quality issues
    if abs(deviation) > 80:
        print(f"    âš ï¸ SANITY: desvio {deviation:+.1f}% extremo - possivel mistura de series")
        state = "VERIFICAR_UNIDADE"
    trend = None
    trend_pct = None
    if len(records) >= 2:
        prev = records[-2]["value"]
        if prev > 0:
            trend_pct = ((latest_val - prev) / prev) * 100
            trend = "SUBINDO" if trend_pct > 3 else ("CAINDO" if trend_pct < -3 else "ESTAVEL")
    return {"current": round(latest_val / divisor, 2), "avg_5y": round(avg_val / divisor, 2),
            "deviation": round(deviation, 1), "state": state, "trend": trend,
            "trend_pct": round(trend_pct, 1) if trend_pct else None,
            "latest_period": f"{latest_year} {latest_period}",
            "history": [{"year": r["year"], "period": r["period"], "value": round(r["value"] / divisor, 2)} for r in records[-12:]]}


def analyze_price_proxy(symbol, seasonality):
    if symbol not in seasonality:
        return None
    entry = seasonality[symbol]
    series = entry.get("series", {})
    current = series.get("current", [])
    average = series.get("average", [])
    if not current or not average:
        return None
    last_current = current[-1]
    last_price = last_current["close"]
    last_day = last_current["day"]
    avg_for_day = None
    for avg_point in average:
        if avg_point["day"] == last_day:
            avg_for_day = avg_point["close"]
            break
    if not avg_for_day:
        closest = min(average, key=lambda x: abs(x["day"] - last_day))
        avg_for_day = closest["close"]
    deviation = ((last_price - avg_for_day) / avg_for_day) * 100 if avg_for_day > 0 else 0
    if deviation > 20:
        state = "PRECO_ELEVADO"
    elif deviation > 10:
        state = "PRECO_ACIMA_MEDIA"
    elif deviation < -20:
        state = "PRECO_DEPRIMIDO"
    elif deviation < -10:
        state = "PRECO_ABAIXO_MEDIA"
    else:
        state = "PRECO_NEUTRO"
    trend = None
    trend_pct = None
    if len(current) >= 20:
        recent = [c["close"] for c in current[-5:]]
        older = [c["close"] for c in current[-20:-5]]
        recent_avg = sum(recent) / len(recent)
        older_avg = sum(older) / len(older)
        trend_pct = ((recent_avg - older_avg) / older_avg) * 100 if older_avg > 0 else 0
        trend = "SUBINDO" if trend_pct > 3 else ("CAINDO" if trend_pct < -3 else "ESTAVEL")
    return {"current": round(last_price, 2), "avg_5y": round(avg_for_day, 2), "deviation": round(deviation, 1),
            "state": state, "trend": trend, "trend_pct": round(trend_pct, 1) if trend_pct else None}


def _make_proxy_entry(symbol, proxy):
    factors = [f"Preco {proxy['deviation']:+.1f}% vs media 5Y"]
    if proxy["trend"] and proxy["trend_pct"]:
        factors.append(f"Tendencia: {proxy['trend']} ({proxy['trend_pct']:+.1f}%)")
    factors.append("Fonte: proxy de preco (estoque real indisponivel)")
    return {"symbol": symbol, "price": proxy["current"], "stock_current": None, "stock_avg": None,
            "stock_unit": None, "price_vs_avg": proxy["deviation"], "state": proxy["state"], "factors": factors,
            "data_available": {"stock_real": False, "stock_source": None, "stock_proxy": True, "curve": False, "cot": False}}


def process_stocks_watch(seasonality_file):
    with open(seasonality_file, encoding="utf-8") as f:
        seasonality = json.load(f)
    result = {"timestamp": datetime.now().isoformat(), "commodities": {}}
    psd_stocks = load_psd_stocks()
    all_symbols = list(USDA_STOCKS.keys()) + PRICE_PROXY_ONLY
    for symbol in all_symbols:
        if symbol in USDA_STOCKS:
            cfg = USDA_STOCKS[symbol]
            print(f"  {symbol}: Fetching USDA {cfg['commodity']}/{cfg['stat']}...")
            records = fetch_usda_stocks(cfg["commodity"], cfg["stat"], cfg.get("filter"))
            analysis = analyze_usda_stocks(records, cfg["divisor"])
            if analysis and analysis.get("deviation") is not None:
                factors = []
                if analysis["state"] in ("APERTO", "NEUTRO_VIES_APERTO"):
                    factors.append(f"Estoque {abs(analysis['deviation']):.1f}% abaixo da media (USDA)")
                elif analysis["state"] in ("EXCESSO", "NEUTRO_VIES_EXCESSO"):
                    factors.append(f"Estoque {analysis['deviation']:.1f}% acima da media (USDA)")
                else:
                    factors.append(f"Estoque {analysis['deviation']:+.1f}% vs media (USDA)")
                if analysis["trend"] and analysis.get("trend_pct"):
                    factors.append(f"Tendencia: {analysis['trend']} ({analysis['trend_pct']:+.1f}%)")
                if analysis.get("latest_period"):
                    factors.append(f"Dado mais recente: {analysis['latest_period']}")
                price = None
                if symbol in seasonality:
                    curr = seasonality[symbol].get("series", {}).get("current", [])
                    if curr:
                        price = curr[-1]["close"]
                result["commodities"][symbol] = {
                    "symbol": symbol, "price": round(price, 2) if price else None,
                    "stock_current": analysis["current"], "stock_avg": analysis["avg_5y"],
                    "stock_unit": cfg["unit"], "price_vs_avg": analysis["deviation"],
                    "state": analysis["state"], "factors": factors,
                    "data_available": {"stock_real": True, "stock_source": "USDA QuickStats", "stock_proxy": False, "curve": False, "cot": False},
                    "stock_history": analysis.get("history", [])}
                print(f"    OK: {analysis['current']} {cfg['unit']} | {analysis['state']} ({analysis['deviation']:+.1f}%)")
            else:
                proxy = analyze_price_proxy(symbol, seasonality)
                if proxy:
                    result["commodities"][symbol] = _make_proxy_entry(symbol, proxy)
                    print(f"    No USDA data, using price proxy | {proxy['state']}")
        elif symbol in PRICE_PROXY_ONLY:
            if symbol in psd_stocks:
                price = None
                if symbol in seasonality:
                    curr = seasonality[symbol].get("series", {}).get("current", [])
                    if curr:
                        price = curr[-1]["close"]
                result["commodities"][symbol] = _make_psd_entry(symbol, psd_stocks[symbol], price, seasonality)
                psd_d = psd_stocks[symbol]
                print(f"  {symbol}: PSD REAL | {result['commodities'][symbol]['state']} ({psd_d['deviation']:+.1f}%)")
            else:
                proxy = analyze_price_proxy(symbol, seasonality)
                if proxy:
                    result["commodities"][symbol] = _make_proxy_entry(symbol, proxy)
                    print(f"  {symbol}: Price proxy | {proxy['state']} ({proxy['deviation']:+.1f}%)")
    return result


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        result = process_stocks_watch(Path(sys.argv[1]))
    else:
        result = process_stocks_watch(Path("../agrimacro-dash/public/data/processed/seasonality.json"))
    print(f"\nProcessed {len(result['commodities'])} commodities:")
    real_count = sum(1 for d in result["commodities"].values() if d["data_available"].get("stock_real"))
    proxy_count = sum(1 for d in result["commodities"].values() if d["data_available"].get("stock_proxy"))
    for sym, data in result["commodities"].items():
        tag = "USDA REAL" if data["data_available"].get("stock_real") else "PRICE PROXY"
        print(f"  {sym:4s} | {tag:12s} | {data['state']:20s} | {data['price_vs_avg']:+6.1f}%")
    print(f"\nSummary: {real_count} with real USDA data, {proxy_count} with price proxy")
    
    # Save to JSON
    out_path = Path(__file__).parent / ".." / "agrimacro-dash" / "public" / "data" / "processed" / "stocks_watch.json"
    with open(out_path, "w", encoding="utf-8") as fout:
        json.dump(result, fout, indent=2, ensure_ascii=False, default=str)
    print(f"\n[SAVED] {out_path}")
