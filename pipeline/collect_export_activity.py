#!/usr/bin/env python3
"""
collect_export_activity.py - AgriMacro Export Activity Collector
================================================================
Source: UN Comtrade API (free, public, no auth required).
  - Annual 2024 vs 2023 for YoY comparison
  - Monthly 2025 data for current pace tracking
  - WASDE projections for target comparison

Output: export_activity.json
"""

import json
import time
import urllib.request
from datetime import datetime, timezone
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
OUTPUT_FILE = OUTPUT_DIR / "export_activity.json"

CURRENT_YEAR = datetime.now().year
CURRENT_MONTH = datetime.now().month
US_CODE = 842

# ---------------------------------------------------------------------------
# Commodities: sym -> HS code + WASDE targets (MT)
# ---------------------------------------------------------------------------
COMMODITIES = {
    "ZC": {"name": "Corn",         "hs": "1005", "target": 62230000, "prev_target": 57150000},
    "ZS": {"name": "Soybeans",     "hs": "1201", "target": 49670000, "prev_target": 46270000},
    "ZW": {"name": "Wheat",        "hs": "1001", "target": 22680000, "prev_target": 19770000},
    "CT": {"name": "Cotton",       "hs": "5201", "target": 2720000,  "prev_target": 2830000},
    "ZM": {"name": "Soybean Meal", "hs": "2304", "target": 14880000, "prev_target": 14240000},
    "SB": {"name": "Sugar",        "hs": "1701", "target": 330000,   "prev_target": 290000},
    "KC": {"name": "Coffee",       "hs": "0901", "target": 130000,   "prev_target": 125000},
}


def fetch_comtrade(freq, hs, period):
    """Fetch one Comtrade query. Returns {qty_kg, value_usd} or None."""
    url = (
        f"https://comtradeapi.un.org/public/v1/preview/C/{freq}/HS"
        f"?reporterCode={US_CODE}&partnerCode=0&period={period}"
        f"&cmdCode={hs}&flowCode=X"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "AgriMacro/3.2"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            recs = data.get("data", [])
            if recs:
                r = recs[0]
                return {
                    "qty_kg": r.get("qty") or r.get("netWgt") or 0,
                    "value_usd": r.get("primaryValue") or r.get("fobvalue") or 0,
                }
    except Exception as e:
        print(f"      Comtrade {freq} {hs} {period}: {e}")
    return None


def main():
    print("  collect_export_activity: starting (UN Comtrade)...")

    commodities_out = {}
    ok_count = 0

    for sym, cfg in COMMODITIES.items():
        print(f"    {sym} ({cfg['name']})...")
        entry = {
            "name": cfg["name"],
            "source": "UN Comtrade",
            # Annual full-year data
            "annual_2024_mt": None,
            "annual_2024_usd": None,
            "annual_2023_mt": None,
            "annual_2023_usd": None,
            "yoy_pct": None,
            # Current year monthly accumulation
            "ytd_2025_mt": None,
            "ytd_2025_months": 0,
            # WASDE
            "usda_target_mt": cfg["target"],
            "pct_of_target": None,
            "vs_last_year_pct": None,
            "signal": "NORMAL",
        }

        # --- Annual 2024 ---
        a24 = fetch_comtrade("A", cfg["hs"], "2024")
        time.sleep(3.5)  # Comtrade strict rate limit
        if a24 and a24["qty_kg"] > 0:
            entry["annual_2024_mt"] = round(a24["qty_kg"] / 1000, 0)
            entry["annual_2024_usd"] = round(a24["value_usd"], 0)
            ok_count += 1

        # --- Annual 2023 ---
        a23 = fetch_comtrade("A", cfg["hs"], "2023")
        time.sleep(3.5)
        if a23 and a23["qty_kg"] > 0:
            entry["annual_2023_mt"] = round(a23["qty_kg"] / 1000, 0)
            entry["annual_2023_usd"] = round(a23["value_usd"], 0)

        # YoY 2024 vs 2023
        if entry["annual_2024_mt"] and entry["annual_2023_mt"] and entry["annual_2023_mt"] > 0:
            entry["yoy_pct"] = round(
                (entry["annual_2024_mt"] / entry["annual_2023_mt"] - 1) * 100, 1
            )
            entry["vs_last_year_pct"] = entry["yoy_pct"]

        # --- Monthly 2025 Q1 (3 months only to avoid rate limit) ---
        ytd_kg = 0
        months_ok = 0
        for m in range(1, 4):  # Jan-Mar only (avoid 429)
            period = f"2025{m:02d}"
            md = fetch_comtrade("M", cfg["hs"], period)
            time.sleep(3.5)
            if md and md["qty_kg"] > 0:
                ytd_kg += md["qty_kg"]
                months_ok += 1

        if months_ok > 0:
            entry["ytd_2025_mt"] = round(ytd_kg / 1000, 0)
            entry["ytd_2025_months"] = months_ok
            entry["pct_of_target"] = round((ytd_kg / 1000) / cfg["target"] * 100, 1)
            print(f"      YTD Q1 ({months_ok}m): {entry['ytd_2025_mt']:,.0f} MT ({entry['pct_of_target']:.1f}% of target)")

        # Signal
        if entry["pct_of_target"] is not None:
            expected = (months_ok / 12) * 100
            ahead = entry["pct_of_target"] - expected
            if ahead > 5:
                entry["signal"] = "FORTE"
            elif ahead < -5:
                entry["signal"] = "FRACO"
        elif entry["yoy_pct"] is not None:
            if entry["yoy_pct"] > 5:
                entry["signal"] = "FORTE"
            elif entry["yoy_pct"] < -5:
                entry["signal"] = "FRACO"

        commodities_out[sym] = entry
        print(f"      Done: signal={entry['signal']}")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "UN Comtrade (comtradeapi.un.org)",
        "is_fallback": ok_count == 0,
        "marketing_year": f"2025/2026",
        "note": "Annual 2024/2023 from Comtrade. YTD 2025 from monthly data. WASDE targets hardcoded.",
        "commodities": commodities_out,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"  export_activity: {ok_count}/{len(COMMODITIES)} with annual data")
    print(f"  Saved to {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    main()
