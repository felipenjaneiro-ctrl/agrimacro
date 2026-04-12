#!/usr/bin/env python3
"""
collect_crop_progress.py - AgriMacro USDA NASS Crop Progress Collector
======================================================================
Collects weekly crop progress data from USDA NASS QuickStats API.
Published every Monday during the US growing season (April-November).

Crops: Corn, Soybeans, Winter Wheat, Cotton
Stages: planted, emerged, silking/blooming/heading, harvested, etc.
Scope: US national + priority states per crop

Requires USDA_NASS_KEY in .env (free at https://quickstats.nass.usda.gov/api)
Output: crop_progress.json
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
ROOT_DIR = SCRIPT_DIR.parent
ENV_FILE = ROOT_DIR / ".env" if (ROOT_DIR / ".env").exists() else SCRIPT_DIR / ".env"

OUTPUT_CANDIDATES = [
    ROOT_DIR / "agrimacro-dash" / "public" / "data" / "processed",
    SCRIPT_DIR / "public" / "data" / "processed",
    SCRIPT_DIR,
]
OUTPUT_DIR = next((c for c in OUTPUT_CANDIDATES if c.exists()), SCRIPT_DIR)
OUTPUT_FILE = OUTPUT_DIR / "crop_progress.json"

# ---------------------------------------------------------------------------
# Load .env
# ---------------------------------------------------------------------------
def load_env():
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

load_env()

API_KEY = os.environ.get("USDA_NASS_KEY", "")
BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
CURRENT_YEAR = datetime.now().year

# ---------------------------------------------------------------------------
# Crop definitions
# ---------------------------------------------------------------------------
CROPS = {
    "CORN": {
        "commodity": "CORN",
        "stages": [
            "PROGRESS, MEASURED IN PCT PLANTED",
            "PROGRESS, MEASURED IN PCT EMERGED",
            "PROGRESS, MEASURED IN PCT SILKING",
            "PROGRESS, MEASURED IN PCT DOUGH",
            "PROGRESS, MEASURED IN PCT DENTED",
            "PROGRESS, MEASURED IN PCT MATURE",
            "PROGRESS, MEASURED IN PCT HARVESTED",
        ],
        "stage_keys": ["planted", "emerged", "silking", "dough", "dented", "mature", "harvested"],
        "states": ["IOWA", "ILLINOIS", "INDIANA", "MINNESOTA", "NEBRASKA", "OHIO", "MISSOURI"],
    },
    "SOYBEANS": {
        "commodity": "SOYBEANS",
        "stages": [
            "PROGRESS, MEASURED IN PCT PLANTED",
            "PROGRESS, MEASURED IN PCT EMERGED",
            "PROGRESS, MEASURED IN PCT BLOOMING",
            "PROGRESS, MEASURED IN PCT SETTING PODS",
            "PROGRESS, MEASURED IN PCT DROPPING LEAVES",
            "PROGRESS, MEASURED IN PCT HARVESTED",
        ],
        "stage_keys": ["planted", "emerged", "blooming", "setting_pods", "dropping_leaves", "harvested"],
        "states": ["IOWA", "ILLINOIS", "INDIANA", "MINNESOTA", "NEBRASKA", "OHIO", "MISSOURI"],
    },
    "WHEAT_WINTER": {
        "commodity": "WHEAT",
        "stages": [
            "PROGRESS, MEASURED IN PCT PLANTED",
            "PROGRESS, MEASURED IN PCT EMERGED",
            "PROGRESS, MEASURED IN PCT HEADED",
            "PROGRESS, MEASURED IN PCT HARVESTED",
        ],
        "stage_keys": ["planted", "emerged", "headed", "harvested"],
        "states": ["KANSAS", "OKLAHOMA", "TEXAS"],
    },
    "COTTON": {
        "commodity": "COTTON",
        "stages": [
            "PROGRESS, MEASURED IN PCT PLANTED",
            "PROGRESS, MEASURED IN PCT SQUARING",
            "PROGRESS, MEASURED IN PCT SETTING BOLLS",
            "PROGRESS, MEASURED IN PCT HARVESTED",
        ],
        "stage_keys": ["planted", "squaring", "setting_bolls", "harvested"],
        "states": ["TEXAS", "GEORGIA", "MISSISSIPPI"],
    },
}


def nass_query(params):
    """Query NASS QuickStats API. Returns list of row dicts or empty list on error."""
    params["key"] = API_KEY
    params["format"] = "JSON"
    url = BASE_URL + "?" + urllib.parse.urlencode(params)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "AgriMacro/3.2"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body.get("data", [])
    except urllib.error.HTTPError as e:
        print(f"    NASS HTTP {e.code}: {e.reason} for {params.get('commodity_desc','?')}")
        return []
    except Exception as e:
        print(f"    NASS error: {e}")
        return []


def parse_stage(statisticcat_desc):
    """Extract short stage key from NASS statisticcat_desc string."""
    s = statisticcat_desc.upper()
    for keyword, key in [
        ("PLANTED", "planted"), ("EMERGED", "emerged"), ("SILKING", "silking"),
        ("DOUGH", "dough"), ("DENTED", "dented"), ("MATURE", "mature"),
        ("HARVESTED", "harvested"), ("BLOOMING", "blooming"),
        ("SETTING PODS", "setting_pods"), ("DROPPING LEAVES", "dropping_leaves"),
        ("HEADED", "headed"), ("SQUARING", "squaring"), ("SETTING BOLLS", "setting_bolls"),
    ]:
        if keyword in s:
            return key
    return None


def _base_params(commodity, agg_level, year, state=None):
    """Build minimal NASS query params that the API accepts."""
    p = {
        "source_desc": "SURVEY",
        "commodity_desc": commodity,
        "statisticcat_desc": "PROGRESS",
        "agg_level_desc": agg_level,
        "year": str(year),
    }
    if commodity == "WHEAT":
        p["class_desc"] = "WINTER"
    if state:
        p["state_name"] = state
    return p


def _extract_latest_week(rows):
    """From a list of NASS rows, extract data for the latest week only."""
    if not rows:
        return {}, None
    latest = max(rows, key=lambda r: int(r.get("end_code", "0") or "0"))
    week_ref = latest.get("reference_period_desc", latest.get("end_code", ""))
    data = {}
    for row in rows:
        if row.get("end_code") != latest.get("end_code"):
            continue
        stage = parse_stage(row.get("unit_desc", ""))
        if not stage:
            stage = parse_stage(row.get("short_desc", ""))
        if stage:
            val = row.get("Value", "").replace(",", "").strip()
            try:
                data[stage] = float(val)
            except (ValueError, TypeError):
                pass
    return data, week_ref


def collect_crop(crop_key, crop_cfg):
    """Collect progress for one crop. Returns dict with national + states."""
    commodity = crop_cfg["commodity"]
    result = {"national": {}, "states": {}, "week_ending": None, "year": CURRENT_YEAR}

    # Try current year first, fall back to previous year if empty
    for year in [CURRENT_YEAR, CURRENT_YEAR - 1]:
        params = _base_params(commodity, "NATIONAL", year)
        rows = nass_query(params)
        if rows:
            result["national"], result["week_ending"] = _extract_latest_week(rows)
            result["year"] = year
            break
        time.sleep(0.3)

    # State-level queries (use same year that had national data)
    year = result["year"]
    time.sleep(0.3)
    for state in crop_cfg["states"]:
        params_st = _base_params(commodity, "STATE", year, state=state)
        st_rows = nass_query(params_st)
        if st_rows:
            state_data, _ = _extract_latest_week(st_rows)
            if state_data:
                result["states"][state.title()] = state_data
        time.sleep(0.3)

    return result


def is_season_active():
    """Crop progress is published roughly April through November."""
    month = datetime.now().month
    return 4 <= month <= 11


def main():
    print("  collect_crop_progress: starting...")

    if not API_KEY:
        print("  ERROR: USDA_NASS_KEY not found in .env")
        print("  Get a free key at https://quickstats.nass.usda.gov/api")
        fallback = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "is_fallback": True,
            "error": "USDA_NASS_KEY not configured in .env",
            "season_active": is_season_active(),
            "crops": {},
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(fallback, f, indent=2, ensure_ascii=False)
        print(f"  Wrote fallback to {OUTPUT_FILE}")
        return fallback

    active = is_season_active()
    crops_out = {}
    total_national = 0
    total_states = 0

    for crop_key, crop_cfg in CROPS.items():
        print(f"    Collecting {crop_key}...")
        data = collect_crop(crop_key, crop_cfg)
        crops_out[crop_key] = data
        n_nat = len(data["national"])
        n_st = len(data["states"])
        total_national += n_nat
        total_states += n_st
        print(f"      national: {n_nat} stages, states: {n_st}")
        time.sleep(0.5)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "is_fallback": total_national == 0,
        "season_active": active,
        "year": CURRENT_YEAR,
        "source": "USDA NASS QuickStats",
        "crops": crops_out,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"  crop_progress: {total_national} national stages, {total_states} state entries")
    print(f"  Saved to {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    main()
