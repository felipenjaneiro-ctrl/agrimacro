import json, sys, os, time
from datetime import datetime
from pathlib import Path
try:
    import requests
except ImportError:
    os.system(f"{sys.executable} -m pip install requests --quiet")
    import requests

BASE = Path(r"C:\Users\felip\OneDrive") / "Área de Trabalho" / "agrimacro"
DATA_DIR = BASE / "agrimacro-dash" / "public" / "data" / "processed"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT = DATA_DIR / "futures_contracts.json"

MONTHS = {"F":"Jan","G":"Feb","H":"Mar","J":"Apr","K":"May","M":"Jun",
          "N":"Jul","Q":"Aug","U":"Sep","V":"Oct","X":"Nov","Z":"Dec"}
YR = datetime.now().year
YSUF = [str(y)[-2:] for y in range(YR, YR+3)]

COMMODITIES = {
    "ZC":{"name":"Milho (Corn)","exchange":"CBOT","months":["H","K","N","U","Z"],"unit":"cents/bu","yahoo":"ZC"},
    "ZS":{"name":"Soja (Soybeans)","exchange":"CBOT","months":["F","H","K","N","Q","U","X"],"unit":"cents/bu","yahoo":"ZS"},
    "ZW":{"name":"Trigo CBOT (Wheat SRW)","exchange":"CBOT","months":["H","K","N","U","Z"],"unit":"cents/bu","yahoo":"ZW"},
    "KE":{"name":"Trigo KC (Wheat HRW)","exchange":"KCBT","months":["H","K","N","U","Z"],"unit":"cents/bu","yahoo":"KE"},
    "ZM":{"name":"Farelo Soja (Soybean Meal)","exchange":"CBOT","months":["F","H","K","N","Q","U","V","Z"],"unit":"$/ton","yahoo":"ZM"},
    "ZL":{"name":"Oleo Soja (Soybean Oil)","exchange":"CBOT","months":["F","H","K","N","Q","U","V","Z"],"unit":"cents/lb","yahoo":"ZL"},
    "LE":{"name":"Boi Gordo (Live Cattle)","exchange":"CME","months":["G","J","M","Q","V","Z"],"unit":"cents/lb","yahoo":"LE"},
    "GF":{"name":"Feeder Cattle","exchange":"CME","months":["F","H","J","K","Q","U","V","X"],"unit":"cents/lb","yahoo":"GF"},
    "HE":{"name":"Suino Magro (Lean Hogs)","exchange":"CME","months":["G","J","K","M","N","Q","V","Z"],"unit":"cents/lb","yahoo":"HE"},
    "KC":{"name":"Cafe Arabica (Coffee C)","exchange":"ICE","months":["H","K","N","U","Z"],"unit":"cents/lb","yahoo":"KC"},
    "CC":{"name":"Cacau (Cocoa)","exchange":"ICE","months":["H","K","N","U","Z"],"unit":"$/ton","yahoo":"CC"},
    "SB":{"name":"Acucar #11 (Sugar)","exchange":"ICE","months":["H","K","N","V"],"unit":"cents/lb","yahoo":"SB"},
    "CT":{"name":"Algodao #2 (Cotton)","exchange":"ICE","months":["H","K","N","V","Z"],"unit":"cents/lb","yahoo":"CT"},
    "OJ":{"name":"Suco Laranja (Orange Juice)","exchange":"ICE","months":["F","H","K","N","U","X"],"unit":"cents/lb","yahoo":"OJ"},
    "CL":{"name":"Petroleo WTI (Crude Oil)","exchange":"NYMEX","months":["F","G","H","J","K","M","N","Q","U","V","X","Z"],"unit":"$/bbl","yahoo":"CL"},
    "NG":{"name":"Gas Natural (Natural Gas)","exchange":"NYMEX","months":["F","G","H","J","K","M","N","Q","U","V","X","Z"],"unit":"$/MMBtu","yahoo":"NG"},
    "GC":{"name":"Ouro (Gold)","exchange":"COMEX","months":["G","J","M","Q","Z"],"unit":"$/oz","yahoo":"GC"},
}

HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

def sf(v):
    try: x=float(v); return x if x>0 else None
    except: return None

def si(v):
    try: return int(float(v))
    except: return None

def fetch_yahoo(symbol):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
    try:
        r = requests.get(url, timeout=10, headers=HEADERS)
        j = r.json()
        result = j.get("chart",{}).get("result",[])
        if not result: return None
        meta = result[0].get("meta",{})
        price = meta.get("regularMarketPrice")
        if not price or price <= 0: return None
        quote = result[0].get("indicators",{}).get("quote",[{}])[0]
        opens = quote.get("open",[])
        highs = quote.get("high",[])
        lows = quote.get("low",[])
        vols = quote.get("volume",[])
        return {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "open": sf(opens[-1]) if opens else None,
            "high": sf(highs[-1]) if highs else None,
            "low": sf(lows[-1]) if lows else None,
            "close": float(price),
            "volume": si(vols[-1]) if vols else None,
            "source": "yahoo"
        }
    except: return None

def fetch_price(yahoo_sym):
    data = fetch_yahoo(yahoo_sym)
    if data and data.get("close"):
        return data
    return None

def main():
    print("="*60)
    print("  AgriMacro v3.1 - Futures Contracts Collector")
    print("  Fonte: Yahoo Finance")
    print("="*60)
    print()
    results = {"generated_at":datetime.now().isoformat(),"source":"Yahoo Finance","commodities":{}}
    total_try, total_ok = 0, 0

    for base, info in COMMODITIES.items():
        print(f"\n{base} - {info['name']}")
        contracts = []
        ybase = info["yahoo"]

        for ys in YSUF:
            for mc in info["months"]:
                yahoo_sym = f"{ybase}{mc}{ys}.CBT"

                if info["exchange"] == "CBOT":
                    yahoo_sym = f"{ybase}{mc}{ys}.CBT"
                elif info["exchange"] == "CME":
                    yahoo_sym = f"{ybase}{mc}{ys}.CME"
                elif info["exchange"] == "ICE":
                    yahoo_sym = f"{ybase}{mc}{ys}.NYB"
                elif info["exchange"] == "NYMEX":
                    yahoo_sym = f"{ybase}{mc}{ys}.NYM"
                elif info["exchange"] == "COMEX":
                    yahoo_sym = f"{ybase}{mc}{ys}.CMX"
                elif info["exchange"] == "KCBT":
                    yahoo_sym = f"{ybase}{mc}{ys}.CBT"

                yahoo_alt = f"{ybase}=F"

                total_try += 1
                data = fetch_price(yahoo_sym)

                if not data or not data.get("close"):
                    data = fetch_yahoo(yahoo_alt)

                if data and data.get("close"):
                    total_ok += 1
                    src = "yahoo"

                    contracts.append({
                        "contract":f"{base}{mc}{ys}","yahoo_symbol":yahoo_sym,
                        "month":MONTHS[mc],"month_code":mc,
                        "year":f"20{ys}","expiry_label":f"{MONTHS[mc]} 20{ys}",
                        "close":data["close"],"open":data.get("open"),
                        "high":data.get("high"),"low":data.get("low"),
                        "volume":data.get("volume"),"date":data.get("date",""),
                        "source":src})
                    vol_str = f"{data['volume']:,}" if data.get("volume") else "-"
                    print(f"  {base}{mc}{ys:2s} {MONTHS[mc]} 20{ys}: {data['close']:>10.2f} | Vol: {vol_str} [{src}]")

                time.sleep(0.35)

        mo = list(MONTHS.keys())
        contracts.sort(key=lambda c: (c["year"], mo.index(c["month_code"])))

        # Remove duplicatas (mesmo preco = provavelmente front month repetido)
        seen_prices = set()
        unique = []
        for c in contracts:
            key = (c["year"], c["month_code"])
            if key not in seen_prices:
                seen_prices.add(key)
                unique.append(c)
        contracts = unique

        results["commodities"][base] = {
            "ticker":base,"name":info["name"],"exchange":info["exchange"],
            "unit":info["unit"],"contracts":contracts,"contract_count":len(contracts)}

        if len(contracts) >= 2:
            spreads = []
            for i in range(len(contracts)-1):
                c1, c2 = contracts[i], contracts[i+1]
                if c1["close"] and c2["close"]:
                    sv = c2["close"] - c1["close"]
                    spreads.append({
                        "front":c1["contract"],"back":c2["contract"],
                        "front_label":c1["expiry_label"],"back_label":c2["expiry_label"],
                        "front_price":c1["close"],"back_price":c2["close"],
                        "spread":round(sv,4),
                        "spread_pct":round(sv/c1["close"]*100,2) if c1["close"] else None,
                        "structure":"contango" if sv>0 else "backwardation"})
            results["commodities"][base]["spreads"] = spreads
        print(f"  -> {len(contracts)} contratos")

    with open(OUTPUT,"w",encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n{'='*60}")
    print(f"[OK] {OUTPUT}")
    print(f"     {len(results['commodities'])} commodities")
    print(f"     Tentados: {total_try} | Com dados: {total_ok}")
    print(f"     Source: Yahoo Finance")

if __name__ == "__main__":
    main()
