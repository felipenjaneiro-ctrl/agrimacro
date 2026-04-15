import requests
import json
import os
from datetime import datetime, timedelta

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "agrimacro-dash", "public", "data", "processed", "nasa_power.json")

REGIONS = {
    "corn_belt_iowa": {"lat": 42.0, "lon": -93.5, "label": "Corn Belt (Iowa, EUA)"},
    "cerrado_mato_grosso": {"lat": -13.0, "lon": -56.0, "label": "Cerrado (MT, Brasil)"},
    "parana_br": {"lat": -24.5, "lon": -51.5, "label": "Paraná (Brasil)"},
    "black_sea": {"lat": 47.5, "lon": 33.5, "label": "Black Sea (Ucrânia)"},
    "west_africa_ivory_coast": {"lat": 6.8, "lon": -5.3, "label": "Costa do Marfim (Cacau)"},
    "argentina_pampas": {"lat": -33.0, "lon": -63.0, "label": "Pampas (Argentina)"},
    "texas_feedlot": {"lat": 34.5, "lon": -102.0, "label": "Texas Feedlot (EUA)"},
    "minas_gerais_coffee": {"lat": -19.5, "lon": -44.5, "label": "Minas Gerais (Café, Brasil)"}
}

PARAMS = "PRECTOTCORR,T2M,T2M_MAX,T2M_MIN,RH2M,ALLSKY_SFC_SW_DWN"

def fetch_region(name, info):
    end = datetime.utcnow()
    start = end - timedelta(days=30)
    url = (
        f"https://power.larc.nasa.gov/api/temporal/daily/point"
        f"?parameters={PARAMS}"
        f"&community=AG"
        f"&longitude={info['lon']}"
        f"&latitude={info['lat']}"
        f"&start={start.strftime('%Y%m%d')}"
        f"&end={end.strftime('%Y%m%d')}"
        f"&format=JSON"
    )
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        props = data.get("properties", {}).get("parameter", {})
        precip = props.get("PRECTOTCORR", {})
        temp = props.get("T2M", {})
        dates = sorted(precip.keys())
        last_7 = dates[-7:] if len(dates) >= 7 else dates
        last_30 = dates
        precip_7d = round(sum(precip.get(d, 0) for d in last_7), 2)
        precip_30d = round(sum(precip.get(d, 0) for d in last_30), 2)
        temp_avg = round(sum(temp.get(d, 0) for d in last_7) / max(len(last_7), 1), 1)
        return {
            "label": info["label"],
            "lat": info["lat"],
            "lon": info["lon"],
            "precip_7d_mm": precip_7d,
            "precip_30d_mm": precip_30d,
            "temp_avg_7d_c": temp_avg,
            "last_date": dates[-1] if dates else "N/A",
            "status": "ok"
        }
    except Exception as e:
        return {"label": info["label"], "status": "error", "error": str(e)}

def main():
    print("NASA POWER — coletando dados climáticos por região...")
    results = {}
    for name, info in REGIONS.items():
        print(f"  {info['label']}...")
        results[name] = fetch_region(name, info)
    output = {
        "updated_at": datetime.utcnow().isoformat() + "Z",
        "source": "NASA POWER API (community=AG)",
        "regions": results
    }
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"Salvo em {OUTPUT_PATH}")
    ok = sum(1 for r in results.values() if r.get("status") == "ok")
    print(f"Resultado: {ok}/{len(REGIONS)} regiões OK")

if __name__ == "__main__":
    main()
