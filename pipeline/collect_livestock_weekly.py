"""
collect_livestock_weekly.py — Indicadores semanais de proteína animal
Fontes: IBGE SIDRA (abate BR), price_history.json (packer proxy),
        livestock_psd.json (cold storage proxy)
"""
import json, math, requests
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).parent.parent
OUT = BASE / "agrimacro-dash" / "public" / "data" / "processed" / "livestock_weekly.json"
PSD_PATH = BASE / "agrimacro-dash" / "public" / "data" / "processed" / "livestock_psd.json"
PRICES_PATH = BASE / "agrimacro-dash" / "public" / "data" / "processed" / "price_history.json"


def fetch_cold_storage_proxy():
    """
    Cold storage proxy via USDA PSD ending stocks (USA).
    Dados anuais do livestock_psd.json.
    """
    try:
        if not PSD_PATH.exists():
            return None
        with open(PSD_PATH) as f:
            psd = json.load(f)

        result = {}
        for sym, psd_sym, label in [("LE", "LE", "Beef"), ("HE", "HE", "Pork")]:
            summ = psd.get("commodities", {}).get(psd_sym, {}).get("usa", {}).get("summaries", {})
            stocks = summ.get("ending_stocks")
            if not stocks:
                continue
            dev = stocks.get("deviation_pct", 0)
            if dev < -10:
                signal = "BAIXO"
                color = "#00C878"
                interp = f"Estoque {label} baixo nos EUA \u2014 demanda consumindo reservas, suporte altista"
            elif dev > 10:
                signal = "ALTO"
                color = "#DC3C3C"
                interp = f"Estoque {label} alto nos EUA \u2014 oferta acumulada, press\u00e3o baixista"
            else:
                signal = "NORMAL"
                color = "#DCB432"
                interp = f"Estoque {label} dentro do normal"

            result[f"cold_storage_{sym.lower()}"] = {
                "name": f"Cold Storage {label} (EUA)",
                "current": stocks.get("current"),
                "current_year": stocks.get("current_year"),
                "avg_5y": stocks.get("avg_5y"),
                "deviation_pct": dev,
                "unit": stocks.get("unit", "1000 MT CWE"),
                "history": stocks.get("history", []),
                "signal": signal,
                "signal_color": color,
                "interpretation": interp,
                "source": "USDA PSD Ending Stocks (proxy anual)",
            }
        return result if result else None
    except Exception as e:
        print(f"[WARN] Cold Storage proxy: {e}")
        return None


def fetch_conab_abate():
    """
    IBGE SIDRA tabela 1092: abate trimestral de bovinos no Brasil.
    Brasil = maior exportador mundial de beef.
    """
    try:
        url = (
            "https://apisidra.ibge.gov.br/values/t/1092/n1/all"
            "/v/284/p/last%206/c12716/115236/d/v284%200"
        )
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return None

        data = r.json()
        if len(data) <= 1:
            return None

        records = []
        for item in data[1:]:
            try:
                val = float(item.get("V", "0").replace(",", "."))
                if val > 0:
                    records.append({
                        "period": item.get("D3N", ""),
                        "value": round(val),
                        "unit": "cabe\u00e7as",
                    })
            except (ValueError, TypeError):
                continue

        if not records:
            return None

        values = [r["value"] for r in records]
        current = values[-1]
        avg = sum(values) / len(values)
        dev = (current - avg) / avg * 100 if avg > 0 else 0

        if dev > 5:
            signal = "ALTO"
            color = "#DC3C3C"
            interp = "Abate acima da m\u00e9dia \u2014 mais oferta global, press\u00e3o baixista LE"
        elif dev < -5:
            signal = "BAIXO"
            color = "#00C878"
            interp = "Abate abaixo da m\u00e9dia \u2014 menos oferta, suporte altista LE"
        else:
            signal = "NORMAL"
            color = "#DCB432"
            interp = "Abate dentro do normal"

        return {
            "abate_bovinos_br": {
                "name": "Abate Bovinos Brasil (IBGE)",
                "current": current,
                "period": records[-1]["period"],
                "avg_6q": round(avg),
                "deviation_pct": round(dev, 1),
                "unit": "cabe\u00e7as/trimestre",
                "history": records,
                "signal": signal,
                "signal_color": color,
                "interpretation": interp,
                "source": "IBGE SIDRA Tabela 1092",
            }
        }
    except Exception as e:
        print(f"[WARN] IBGE abate: {e}")
        return None


def fetch_packer_proxy():
    """
    Proxy de demanda dos packers via momentum de preço do LE.
    Quando spot sobe = packers pagando mais = demanda forte.
    """
    try:
        if not PRICES_PATH.exists():
            return None
        with open(PRICES_PATH) as f:
            prices = json.load(f)

        result = {}
        for sym, label in [("LE", "Live Cattle"), ("GF", "Feeder Cattle"), ("HE", "Lean Hogs")]:
            bars = prices.get(sym, {})
            if isinstance(bars, dict):
                bars = bars.get("bars", [])
            if len(bars) < 20:
                continue

            recent = [b["close"] for b in bars[-20:]]
            current = recent[-1]
            avg_20 = sum(recent) / len(recent)
            mom = (current - recent[0]) / recent[0] * 100

            if mom > 3:
                signal = "FORTE"
                color = "#00C878"
                interp = f"Demanda {label} forte \u2014 pre\u00e7o subindo, packers pagando pr\u00eamio"
            elif mom < -3:
                signal = "FRACO"
                color = "#DC3C3C"
                interp = f"Demanda {label} fraca \u2014 pre\u00e7o caindo, packers sem urg\u00eancia"
            else:
                signal = "NEUTRO"
                color = "#DCB432"
                interp = f"Demanda {label} neutra"

            result[f"packer_{sym.lower()}"] = {
                "name": f"Packer Activity {label}",
                "current_price": round(current, 2),
                "avg_20d": round(avg_20, 2),
                "momentum_20d": round(mom, 2),
                "signal": signal,
                "signal_color": color,
                "interpretation": interp,
                "source": "Price momentum 20d proxy",
            }
        return result if result else None
    except Exception as e:
        print(f"[WARN] Packer proxy: {e}")
        return None


def main():
    print("Coletando indicadores semanais de prote\u00edna animal...")

    output = {
        "generated_at": datetime.now().isoformat(),
        "data": {},
    }

    # Cold Storage proxy
    cs = fetch_cold_storage_proxy()
    if cs:
        output["data"].update(cs)
        for k, v in cs.items():
            print(f"[OK] {v['name']}: {v['signal']} ({v['deviation_pct']:+.1f}%)")
    else:
        print("[WARN] Cold Storage: sem dados")

    # IBGE Abate
    conab = fetch_conab_abate()
    if conab:
        output["data"].update(conab)
        ab = conab["abate_bovinos_br"]
        print(f"[OK] {ab['name']}: {ab['signal']} ({ab['deviation_pct']:+.1f}%)")
    else:
        print("[WARN] IBGE abate: sem dados")
        output["data"]["abate_bovinos_br"] = {
            "name": "Abate Bovinos Brasil",
            "current": None,
            "signal": "SEM DADOS",
            "signal_color": "#8899AA",
            "source": "IBGE SIDRA indispon\u00edvel",
            "is_fallback": True,
        }

    # Packer Proxy
    pp = fetch_packer_proxy()
    if pp:
        output["data"].update(pp)
        for k, v in pp.items():
            print(f"[OK] {v['name']}: {v['signal']} (mom {v['momentum_20d']:+.1f}%)")
    else:
        print("[WARN] Packer proxy: sem dados")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"[OK] livestock_weekly.json salvo com {len(output['data'])} indicadores")


if __name__ == "__main__":
    main()
