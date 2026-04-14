#!/usr/bin/env python3
"""
collect_drought_monitor.py - AgriMacro US Drought Monitor Collector
===================================================================
Collects drought severity data from the US Drought Monitor (UNL/USDA/NOAA).
Updated every Thursday. Data includes national and state-level drought categories.

Output: drought_monitor.json
"""

import json
import os
import re
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
ROOT_DIR = SCRIPT_DIR.parent

OUTPUT_CANDIDATES = [
    ROOT_DIR / "agrimacro-dash" / "public" / "data" / "processed",
    SCRIPT_DIR / "public" / "data" / "processed",
    SCRIPT_DIR,
]
OUTPUT_DIR = next((c for c in OUTPUT_CANDIDATES if c.exists()), SCRIPT_DIR)
OUTPUT_FILE = OUTPUT_DIR / "drought_monitor.json"

# ---------------------------------------------------------------------------
# Agricultural regions (state FIPS codes)
# ---------------------------------------------------------------------------
REGIONS = {
    "corn_belt": {
        "label": "Corn Belt",
        "states": {"Iowa": "19", "Illinois": "17", "Indiana": "18", "Ohio": "39",
                   "Minnesota": "27", "Nebraska": "31", "Wisconsin": "55", "Missouri": "29"},
    },
    "southern_plains": {
        "label": "Southern Plains",
        "states": {"Texas": "48", "Oklahoma": "40", "Kansas": "20"},
    },
    "delta": {
        "label": "Delta",
        "states": {"Mississippi": "28", "Arkansas": "05", "Louisiana": "22"},
    },
    "pacific_nw": {
        "label": "Pacific Northwest",
        "states": {"Washington": "53", "Oregon": "41", "Idaho": "16"},
    },
}

# Signal thresholds
SIGNAL_RULES = {
    "corn_belt":      {"d2_critical": 30, "d2_alert": 15},
    "southern_plains": {"d3_critical": 20, "d3_alert": 10},
    "delta":           {"d2_critical": 35, "d2_alert": 25},
    "pacific_nw":      {"d2_critical": 30, "d2_alert": 20},
}


def fetch_url(url, label=""):
    """Fetch URL. Returns bytes or None."""
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "AgriMacro/3.2",
            "Accept": "application/json, text/html, */*",
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read()
    except Exception as e:
        print(f"    {label} fetch failed: {e}")
        return None


def get_recent_tuesday():
    """Drought Monitor uses Tuesday as reference date."""
    today = datetime.now()
    # Find the most recent Tuesday
    days_since_tue = (today.weekday() - 1) % 7
    tue = today - timedelta(days=days_since_tue)
    return tue


def try_dm_json():
    """
    Try the Drought Monitor current JSON endpoint.
    Returns GeoJSON FeatureCollection with DM categories (0-4) and Shape_Area.
    We calculate percentages from the area proportions.
    """
    url = "https://droughtmonitor.unl.edu/data/json/usdm_current.json"
    raw = fetch_url(url, "DM JSON current")
    if not raw:
        return None
    try:
        data = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    # It's a GeoJSON FeatureCollection with features that have DM=0..4 and Shape_Area
    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        features = data.get("features", [])
        areas = {}
        for f in features:
            props = f.get("properties", {})
            dm = props.get("DM")
            area = props.get("Shape_Area", 0)
            if dm is not None:
                areas[dm] = areas.get(dm, 0) + area
        total_area = sum(areas.values())
        if total_area > 0:
            # Calculate US total area (including non-drought)
            # DM 0=D0, 1=D1, 2=D2, 3=D3, 4=D4
            # The GeoJSON only has drought areas, so we estimate none from total US
            # US CONUS area ~ 7663941.7 km2 (in same units)
            # But since we don't know the units, compute relative percentages
            d0_pct = areas.get(0, 0) / total_area * 100
            d1_pct = areas.get(1, 0) / total_area * 100
            d2_pct = areas.get(2, 0) / total_area * 100
            d3_pct = areas.get(3, 0) / total_area * 100
            d4_pct = areas.get(4, 0) / total_area * 100
            return {
                "d0_pct": round(d0_pct, 1),
                "d1_pct": round(d1_pct, 1),
                "d2_pct": round(d2_pct, 1),
                "d3_pct": round(d3_pct, 1),
                "d4_pct": round(d4_pct, 1),
            }
    # Fallback: try as regular JSON
    return data


def try_dm_state_api(fips, date_str):
    """
    Try Drought Monitor state data API.
    date_str format: YYYYMMDD
    """
    url = f"https://droughtmonitor.unl.edu/DmData/DataDownload.aspx/GetDMDataByState?fips={fips}&oression=1&type=percent&datestring={date_str}"
    raw = fetch_url(url, f"DM state {fips}")
    if raw:
        try:
            data = json.loads(raw.decode("utf-8"))
            # Response may be wrapped in {"d": "..."}
            if isinstance(data, dict) and "d" in data:
                inner = json.loads(data["d"])
                return inner
            return data
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
    return None


def try_dm_comprehensive_api(date_str):
    """Try comprehensive stats endpoint for national data."""
    urls = [
        f"https://droughtmonitor.unl.edu/DmData/DataDownload.aspx/GetDMDataByArea?type=national&datestring={date_str}",
        f"https://usdm.unl.edu/DmData/TimeSeries.aspx/GetDroughtSeverityStatisticsByAreaPercent?aession=1&startdate={date_str[:4]}-01-01&enddate={date_str[:4]}-12-31&statisticsType=1",
    ]
    for url in urls:
        raw = fetch_url(url, "DM comprehensive")
        if raw:
            try:
                data = json.loads(raw.decode("utf-8"))
                if isinstance(data, dict) and "d" in data:
                    return json.loads(data["d"])
                return data
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
    return None


def try_dm_csv_scrape():
    """
    Try to scrape national summary from the DM statistics page.
    Last resort: parse HTML for the national summary table.
    """
    url = "https://droughtmonitor.unl.edu/CurrentMap/StateDroughtMonitor.aspx?US"
    raw = fetch_url(url, "DM HTML scrape")
    if not raw:
        return None
    try:
        html = raw.decode("utf-8", errors="replace")
        # Look for percentage patterns in the HTML
        # Pattern: D0 XX.XX%, D1 XX.XX%, etc.
        national = {}
        for cat, pattern in [
            ("none_pct", r"None[:\s]+(\d+\.?\d*)%"),
            ("d0_pct", r"D0[:\s]+(\d+\.?\d*)%"),
            ("d1_pct", r"D1[:\s]+(\d+\.?\d*)%"),
            ("d2_pct", r"D2[:\s]+(\d+\.?\d*)%"),
            ("d3_pct", r"D3[:\s]+(\d+\.?\d*)%"),
            ("d4_pct", r"D4[:\s]+(\d+\.?\d*)%"),
        ]:
            m = re.search(pattern, html, re.IGNORECASE)
            if m:
                national[cat] = float(m.group(1))
        if national:
            return national
    except Exception:
        pass
    return None


def parse_dm_response(data, source="api"):
    """
    Parse drought monitor data into standardized national format.
    The API returns various formats depending on endpoint.
    """
    national = {
        "none_pct": 0, "d0_pct": 0, "d1_pct": 0,
        "d2_pct": 0, "d3_pct": 0, "d4_pct": 0,
    }

    if isinstance(data, dict):
        # Direct dict with percentages
        for key_map in [
            {"None": "none_pct", "D0": "d0_pct", "D1": "d1_pct", "D2": "d2_pct", "D3": "d3_pct", "D4": "d4_pct"},
            {"none_pct": "none_pct", "d0_pct": "d0_pct", "d1_pct": "d1_pct", "d2_pct": "d2_pct", "d3_pct": "d3_pct", "d4_pct": "d4_pct"},
        ]:
            found = False
            for src_key, dst_key in key_map.items():
                if src_key in data:
                    try:
                        national[dst_key] = float(data[src_key])
                        found = True
                    except (ValueError, TypeError):
                        pass
            if found:
                break

    elif isinstance(data, list):
        # List of records — take the latest
        if data:
            latest = data[-1] if isinstance(data[-1], dict) else data[0]
            for key in ["None", "D0", "D1", "D2", "D3", "D4",
                        "none", "d0", "d1", "d2", "d3", "d4"]:
                if key in latest:
                    cat = key.lower()
                    dst = f"{cat}_pct" if cat != "none" else "none_pct"
                    try:
                        national[dst] = float(latest[key])
                    except (ValueError, TypeError):
                        pass

    total = sum(national.values())
    if total > 0 and abs(total - 100) > 5:
        # Normalize to percentages if needed
        factor = 100 / total if total > 0 else 1
        for k in national:
            national[k] = round(national[k] * factor, 1)

    national["any_drought_pct"] = round(
        national["d0_pct"] + national["d1_pct"] + national["d2_pct"] +
        national["d3_pct"] + national["d4_pct"], 1
    )
    return national


def parse_state_data(data):
    """Parse state-level drought data into percentages."""
    result = {
        "none_pct": 0, "d0_pct": 0, "d1_pct": 0,
        "d2_pct": 0, "d3_pct": 0, "d4_pct": 0,
    }
    if not data:
        return result

    if isinstance(data, list) and data:
        row = data[-1] if isinstance(data[-1], dict) else data[0]
    elif isinstance(data, dict):
        row = data
    else:
        return result

    for src, dst in [("None", "none_pct"), ("D0", "d0_pct"), ("D1", "d1_pct"),
                     ("D2", "d2_pct"), ("D3", "d3_pct"), ("D4", "d4_pct"),
                     ("none", "none_pct"), ("d0", "d0_pct"), ("d1", "d1_pct"),
                     ("d2", "d2_pct"), ("d3", "d3_pct"), ("d4", "d4_pct")]:
        if src in row:
            try:
                result[dst] = float(row[src])
            except (ValueError, TypeError):
                pass
    return result


def compute_region_signal(region_key, d2_plus, d3_plus):
    """Determine signal for a region based on thresholds."""
    rules = SIGNAL_RULES.get(region_key, {})

    if region_key in ("southern_plains",):
        # Use D3+ thresholds
        if d3_plus >= rules.get("d3_critical", 20):
            return "CRITICO"
        if d3_plus >= rules.get("d3_alert", 10):
            return "ALERTA"
    else:
        # Use D2+ thresholds
        if d2_plus >= rules.get("d2_critical", 30):
            return "CRITICO"
        if d2_plus >= rules.get("d2_alert", 15):
            return "ALERTA"
    return "NORMAL"


def main():
    print("  collect_drought_monitor: starting...")

    tue = get_recent_tuesday()
    date_str = tue.strftime("%Y%m%d")
    date_iso = tue.strftime("%Y-%m-%d")
    prev_tue = tue - timedelta(days=7)
    prev_date_str = prev_tue.strftime("%Y%m%d")

    # --- National data ---
    national = None
    source_used = "none"

    # Strategy 1: Current JSON (GeoJSON FeatureCollection)
    print("    Trying DM current JSON...")
    dm_json = try_dm_json()
    if dm_json and isinstance(dm_json, dict) and "d0_pct" in dm_json:
        # Already parsed from GeoJSON
        national = dm_json
        national["none_pct"] = 0  # GeoJSON only has drought areas
        national["any_drought_pct"] = round(
            dm_json.get("d0_pct", 0) + dm_json.get("d1_pct", 0) +
            dm_json.get("d2_pct", 0) + dm_json.get("d3_pct", 0) +
            dm_json.get("d4_pct", 0), 1
        )
        source_used = "usdm_current.json (GeoJSON)"
        print(f"    OK via {source_used}")
    elif dm_json:
        national = parse_dm_response(dm_json, "json")
        source_used = "usdm_current.json"
        print(f"    OK via {source_used}")

    # Strategy 2: Comprehensive API
    if not national or national.get("any_drought_pct", 0) == 0:
        print("    Trying DM comprehensive API...")
        for ds in [date_str, prev_date_str]:
            comp = try_dm_comprehensive_api(ds)
            if comp:
                national = parse_dm_response(comp, "api")
                source_used = f"comprehensive API ({ds})"
                print(f"    OK via {source_used}")
                break

    # Strategy 3: HTML scrape
    if not national or national.get("any_drought_pct", 0) == 0:
        print("    Trying HTML scrape...")
        scraped = try_dm_csv_scrape()
        if scraped:
            national = scraped
            national["any_drought_pct"] = round(
                national.get("d0_pct", 0) + national.get("d1_pct", 0) +
                national.get("d2_pct", 0) + national.get("d3_pct", 0) +
                national.get("d4_pct", 0), 1
            )
            source_used = "HTML scrape"
            print(f"    OK via {source_used}")

    if not national:
        national = {
            "none_pct": 0, "d0_pct": 0, "d1_pct": 0,
            "d2_pct": 0, "d3_pct": 0, "d4_pct": 0, "any_drought_pct": 0,
        }

    # --- Regional data ---
    regions_out = {}
    crop_alerts = []

    for region_key, region_cfg in REGIONS.items():
        print(f"    Region: {region_cfg['label']}...")
        state_totals = {"d0": [], "d1": [], "d2": [], "d3": [], "d4": []}
        states_abbrev = list(region_cfg["states"].keys())

        for state_name, fips in region_cfg["states"].items():
            for ds in [date_str, prev_date_str]:
                st_data = try_dm_state_api(fips, ds)
                if st_data:
                    parsed = parse_state_data(st_data)
                    for cat in state_totals:
                        state_totals[cat].append(parsed.get(f"{cat}_pct", 0))
                    break

        # Average across states
        n = max(len(state_totals["d0"]), 1)
        d0_avg = sum(state_totals["d0"]) / n if state_totals["d0"] else 0
        d1_avg = sum(state_totals["d1"]) / n if state_totals["d1"] else 0
        d2_avg = sum(state_totals["d2"]) / n if state_totals["d2"] else 0
        d3_avg = sum(state_totals["d3"]) / n if state_totals["d3"] else 0
        d4_avg = sum(state_totals["d4"]) / n if state_totals["d4"] else 0

        d2_plus = round(d2_avg + d3_avg + d4_avg, 1)
        d3_plus = round(d3_avg + d4_avg, 1)
        any_drought = round(d0_avg + d1_avg + d2_avg + d3_avg + d4_avg, 1)

        signal = compute_region_signal(region_key, d2_plus, d3_plus)

        # Get state abbreviation list
        abbrevs = []
        for sn in region_cfg["states"]:
            # Simple state name to abbreviation
            st_abbr_map = {
                "Iowa": "IA", "Illinois": "IL", "Indiana": "IN", "Ohio": "OH",
                "Minnesota": "MN", "Nebraska": "NE", "Wisconsin": "WI", "Missouri": "MO",
                "Texas": "TX", "Oklahoma": "OK", "Kansas": "KS",
                "Mississippi": "MS", "Arkansas": "AR", "Louisiana": "LA",
                "Washington": "WA", "Oregon": "OR", "Idaho": "ID",
            }
            abbrevs.append(st_abbr_map.get(sn, sn[:2].upper()))

        regions_out[region_key] = {
            "label": region_cfg["label"],
            "states": abbrevs,
            "d2_plus_pct": d2_plus,
            "d3_plus_pct": d3_plus,
            "any_drought_pct": any_drought,
            "signal": signal,
            "n_states_reporting": len(state_totals["d0"]),
        }

        # Crop alerts
        if region_key == "corn_belt" and d2_plus > 30:
            crop_alerts.append({"crop": "MILHO", "region": "Corn Belt",
                                "message": f"{d2_plus}% em D2+ - CRITICO"})
        elif region_key == "corn_belt" and d2_plus > 15:
            crop_alerts.append({"crop": "MILHO", "region": "Corn Belt",
                                "message": f"{d2_plus}% em D2+ - ALERTA"})
        else:
            crop_alerts.append({"crop": "MILHO", "region": "Corn Belt",
                                "message": f"{d2_plus}% em D2+ - NORMAL"})

        if region_key == "southern_plains":
            if d3_plus > 20:
                crop_alerts.append({"crop": "TRIGO", "region": "Southern Plains",
                                    "message": f"{d3_plus}% em D3+ - CRITICO"})
            elif d3_plus > 10:
                crop_alerts.append({"crop": "TRIGO", "region": "Southern Plains",
                                    "message": f"{d3_plus}% em D3+ - ALERTA"})
            else:
                crop_alerts.append({"crop": "TRIGO", "region": "Southern Plains",
                                    "message": f"{d3_plus}% em D3+ - NORMAL"})

        if region_key == "delta" and d2_plus > 25:
            crop_alerts.append({"crop": "ALGODAO/ARROZ", "region": "Delta",
                                "message": f"{d2_plus}% em D2+ - ALERTA"})

    has_data = national.get("any_drought_pct", 0) > 0 or any(
        r.get("n_states_reporting", 0) > 0 for r in regions_out.values()
    )

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": f"US Drought Monitor (UNL/USDA/NOAA) via {source_used}",
        "is_fallback": not has_data,
        "report_date": date_iso,
        "national": national,
        "regions": regions_out,
        "crop_alerts": crop_alerts,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    n_regions = sum(1 for r in regions_out.values() if r.get("n_states_reporting", 0) > 0)
    print(f"  drought_monitor: national={source_used}, {n_regions} regions with data")
    print(f"  Saved to {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    main()
