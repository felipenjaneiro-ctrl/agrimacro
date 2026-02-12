#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AgriMacro v3.2 - Agricultural Weather Collector
Coleta dados meteorologicos para regioes agricolas chave.
Fontes:
  - Tomorrow.io — previsao 15 dias (regioes principais)
  - Open-Meteo (fallback gratuito, sem key) — previsao 16 dias
  - NOAA CPC — status ENSO (El Nino / La Nina)
Output: weather_agro.json em processed/
"""
import json, os, sys
from datetime import datetime, timedelta

try:
    import requests
except ImportError:
    print("  [ERROR] requests not installed")
    raise

OUT = os.path.join(os.path.dirname(__file__), "..", "agrimacro-dash", "public", "data", "processed", "weather_agro.json")
TOMORROW_KEY_PATH = os.path.join(os.path.expanduser("~"), ".tomorrow_key")
NOAA_KEY_PATH = os.path.join(os.path.expanduser("~"), ".noaa_key")

# ── Regioes agricolas ─────────────────────────────────────────────
REGIONS = {
    "corn_belt": {
        "label": "Corn Belt (IA/IL)",
        "lat": 41.5, "lon": -93.5,
        "crops": ["milho", "soja"],
        "alerts_config": {"frost_below_c": -2, "drought_precip_mm_7d": 5, "flood_precip_mm_7d": 100}
    },
    "cerrado_mt": {
        "label": "Cerrado (MT/GO/MS)",
        "lat": -14.5, "lon": -53.0,
        "crops": ["soja", "milho safrinha", "algodao"],
        "alerts_config": {"drought_precip_mm_7d": 10, "flood_precip_mm_7d": 150}
    },
    "sul_pr_rs": {
        "label": "Sul do Brasil (PR/RS)",
        "lat": -25.5, "lon": -51.0,
        "crops": ["soja", "milho", "trigo"],
        "alerts_config": {"frost_below_c": 0, "drought_precip_mm_7d": 8, "flood_precip_mm_7d": 120}
    },
    "minas_cafe": {
        "label": "Minas Gerais (Cafe)",
        "lat": -21.5, "lon": -45.5,
        "crops": ["cafe arabica"],
        "alerts_config": {"frost_below_c": 2, "drought_precip_mm_7d": 5}
    },
    "pampas_arg": {
        "label": "Pampas (Argentina)",
        "lat": -34.5, "lon": -61.0,
        "crops": ["soja", "milho", "trigo"],
        "alerts_config": {"frost_below_c": -1, "drought_precip_mm_7d": 8, "flood_precip_mm_7d": 110}
    },
    "delta_ms": {
        "label": "Delta Mississippi (US)",
        "lat": 33.5, "lon": -90.5,
        "crops": ["algodao", "soja", "arroz"],
        "alerts_config": {"flood_precip_mm_7d": 100}
    },
}

def get_key(path, env_var=None):
    if os.path.exists(path):
        k = open(path).read().strip()
        if k:
            return k
    if env_var:
        return os.environ.get(env_var, "")
    return ""

def fetch_tomorrow_io(lat, lon, api_key):
    """Fetch 15-day forecast from Tomorrow.io"""
    try:
        url = "https://api.tomorrow.io/v4/weather/forecast"
        params = {
            "location": f"{lat},{lon}",
            "apikey": api_key,
            "timesteps": "1d",
            "units": "metric",
        }
        resp = requests.get(url, params=params, timeout=20)
        if resp.status_code != 200:
            return None
        data = resp.json()
        daily = data.get("timelines", {}).get("daily", [])
        forecast = []
        for day in daily[:15]:
            vals = day.get("values", {})
            forecast.append({
                "date": day.get("time", "")[:10],
                "temp_max": round(vals.get("temperatureMax", 0), 1),
                "temp_min": round(vals.get("temperatureMin", 0), 1),
                "temp_avg": round(vals.get("temperatureAvg", 0), 1),
                "precip_mm": round(vals.get("precipitationIntensityAvg", 0) * 24, 1),
                "precip_prob": round(vals.get("precipitationProbabilityMax", 0), 0),
                "humidity": round(vals.get("humidityAvg", 0), 0),
                "wind_kmh": round(vals.get("windSpeedAvg", 0) * 3.6, 1),
            })
        return forecast
    except Exception as e:
        print(f"    [WARN] Tomorrow.io: {e}")
        return None

def fetch_open_meteo(lat, lon):
    """Fallback: Open-Meteo 16-day forecast (free, no key)"""
    try:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_probability_max,relative_humidity_2m_mean,wind_speed_10m_max",
            "forecast_days": 16,
            "timezone": "auto",
        }
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        daily = data.get("daily", {})
        dates = daily.get("time", [])
        forecast = []
        for i, d in enumerate(dates[:15]):
            forecast.append({
                "date": d,
                "temp_max": daily.get("temperature_2m_max", [None])[i],
                "temp_min": daily.get("temperature_2m_min", [None])[i],
                "temp_avg": round(((daily.get("temperature_2m_max", [0])[i] or 0) + (daily.get("temperature_2m_min", [0])[i] or 0)) / 2, 1),
                "precip_mm": daily.get("precipitation_sum", [0])[i] or 0,
                "precip_prob": daily.get("precipitation_probability_max", [0])[i] or 0,
                "humidity": daily.get("relative_humidity_2m_mean", [0])[i] or 0,
                "wind_kmh": round((daily.get("wind_speed_10m_max", [0])[i] or 0), 1),
            })
        return forecast
    except Exception as e:
        print(f"    [WARN] Open-Meteo: {e}")
        return None

def detect_alerts(forecast, config):
    """Detect agricultural weather alerts from forecast data"""
    alerts = []
    if not forecast:
        return alerts

    # Check frost
    frost_thresh = config.get("frost_below_c")
    if frost_thresh is not None:
        frost_days = [f for f in forecast[:7] if f.get("temp_min") is not None and f["temp_min"] <= frost_thresh]
        if frost_days:
            min_t = min(f["temp_min"] for f in frost_days)
            alerts.append({
                "type": "GEADA",
                "severity": "ALTA" if min_t <= frost_thresh - 3 else "MEDIA",
                "message": f"Risco de geada nos proximos 7 dias (min {min_t:.1f}C em {frost_days[0]['date']})",
                "days_affected": len(frost_days)
            })

    # Check drought (low precip in 7 days)
    drought_thresh = config.get("drought_precip_mm_7d")
    if drought_thresh is not None:
        precip_7d = sum(f.get("precip_mm", 0) for f in forecast[:7])
        if precip_7d < drought_thresh:
            alerts.append({
                "type": "SECA",
                "severity": "ALTA" if precip_7d < drought_thresh * 0.3 else "MEDIA",
                "message": f"Precipitacao acumulada 7d: {precip_7d:.1f}mm (limiar: {drought_thresh}mm)",
                "precip_7d_mm": round(precip_7d, 1)
            })

    # Check flooding (excess precip in 7 days)
    flood_thresh = config.get("flood_precip_mm_7d")
    if flood_thresh is not None:
        precip_7d = sum(f.get("precip_mm", 0) for f in forecast[:7])
        if precip_7d > flood_thresh:
            alerts.append({
                "type": "EXCESSO_HIDRICO",
                "severity": "ALTA" if precip_7d > flood_thresh * 1.5 else "MEDIA",
                "message": f"Excesso hidrico previsto: {precip_7d:.1f}mm em 7 dias (limiar: {flood_thresh}mm)",
                "precip_7d_mm": round(precip_7d, 1)
            })

    return alerts

def fetch_enso_status():
    """Fetch ENSO status from NOAA CPC"""
    try:
        # NOAA CPC ENSO diagnostic
        url = "https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt"
        resp = requests.get(url, timeout=15)
        if resp.status_code != 200:
            return {"status": "N/A", "oni_value": None, "source": "NOAA CPC"}
        lines = resp.text.strip().split("\n")
        # Last valid line has latest ONI
        last_line = [l for l in lines if l.strip() and not l.startswith("SEAS")][-1]
        parts = last_line.split()
        if len(parts) >= 4:
            oni = float(parts[-1])
            if oni >= 0.5:
                status = "El Nino"
            elif oni <= -0.5:
                status = "La Nina"
            else:
                status = "Neutro"
            season = parts[0] if parts else ""
            return {"status": status, "oni_value": oni, "season": season, "source": "NOAA CPC ONI"}
        return {"status": "N/A", "oni_value": None, "source": "NOAA CPC"}
    except Exception as e:
        print(f"  [WARN] ENSO fetch: {e}")
        return {"status": "N/A", "oni_value": None, "source": "error"}

def build_summary(regions_data, enso):
    """Build a text summary for the report"""
    parts = []
    for key, reg in regions_data.items():
        alerts = reg.get("alerts", [])
        precip_7d = reg.get("precip_7d_mm", 0)
        label = reg.get("label", key)
        if alerts:
            for a in alerts:
                parts.append(f"{label}: {a['message']}")
        else:
            parts.append(f"{label}: sem alertas, precipitacao 7d = {precip_7d:.0f}mm")
    if enso.get("status") != "N/A":
        parts.append(f"ENSO: {enso['status']} (ONI={enso.get('oni_value','N/A')})")
    return ". ".join(parts[:6]) + "." if parts else "Dados climaticos coletados sem alertas significativos."

def main():
    print("Collecting agricultural weather data...")

    tomorrow_key = get_key(TOMORROW_KEY_PATH, "TOMORROW_IO_KEY")
    use_tomorrow = bool(tomorrow_key)
    if use_tomorrow:
        print(f"  [OK] Tomorrow.io key found")
    else:
        print(f"  [INFO] No Tomorrow.io key — using Open-Meteo (free)")

    regions_data = {}
    for key, reg in REGIONS.items():
        print(f"  [{reg['label']}] Fetching forecast...")
        lat, lon = reg["lat"], reg["lon"]

        # Try Tomorrow.io first, fallback to Open-Meteo
        forecast = None
        source = "open-meteo"
        if use_tomorrow:
            forecast = fetch_tomorrow_io(lat, lon, tomorrow_key)
            if forecast:
                source = "tomorrow.io"

        if not forecast:
            forecast = fetch_open_meteo(lat, lon)
            source = "open-meteo"

        if not forecast:
            print(f"    [WARN] No data for {key}")
            regions_data[key] = {"label": reg["label"], "lat": lat, "lon": lon, "error": "no data"}
            continue

        # Current (today's forecast)
        today_f = forecast[0] if forecast else {}

        # 7-day accumulated precip
        precip_7d = sum(f.get("precip_mm", 0) for f in forecast[:7])
        precip_15d = sum(f.get("precip_mm", 0) for f in forecast[:15])

        # Detect alerts
        alerts = detect_alerts(forecast, reg.get("alerts_config", {}))

        regions_data[key] = {
            "label": reg["label"],
            "lat": lat, "lon": lon,
            "crops": reg["crops"],
            "source": source,
            "current": {
                "temp_max": today_f.get("temp_max"),
                "temp_min": today_f.get("temp_min"),
                "precip_mm": today_f.get("precip_mm", 0),
                "humidity": today_f.get("humidity", 0),
            },
            "forecast_15d": forecast,
            "precip_7d_mm": round(precip_7d, 1),
            "precip_15d_mm": round(precip_15d, 1),
            "temp_min_7d": round(min((f.get("temp_min") or 99) for f in forecast[:7]), 1),
            "temp_max_7d": round(max((f.get("temp_max") or -99) for f in forecast[:7]), 1),
            "alerts": alerts,
        }

        n_alerts = len(alerts)
        alert_str = f" | {n_alerts} ALERTAS!" if n_alerts else ""
        print(f"    [OK] {source}: {len(forecast)} dias | Precip 7d: {precip_7d:.0f}mm{alert_str}")

    # ENSO status
    print("  [ENSO] Fetching NOAA CPC...")
    enso = fetch_enso_status()
    print(f"    [OK] ENSO: {enso['status']} (ONI={enso.get('oni_value','N/A')})")

    # Summary
    summary = build_summary(regions_data, enso)

    output = {
        "generated_at": datetime.now().isoformat(),
        "source_primary": "tomorrow.io" if use_tomorrow else "open-meteo",
        "regions": regions_data,
        "enso": enso,
        "summary": summary,
        "total_alerts": sum(len(r.get("alerts", [])) for r in regions_data.values()),
    }

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    total_alerts = output["total_alerts"]
    print(f"  [OK] Weather saved: {len(regions_data)} regions, {total_alerts} alerts, ENSO={enso['status']}")

if __name__ == "__main__":
    main()
