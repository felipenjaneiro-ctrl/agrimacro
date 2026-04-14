#!/usr/bin/env python3
"""
collect_fertilizer.py - AgriMacro Fertilizer Price Collector
============================================================
Collects fertilizer prices from World Bank Commodity API and IndexMundi.
Tracks Urea, DAP, MOP/KCl, TSP — key inputs for agricultural cost analysis.

Lag note: Fertilizer prices impact agricultural costs with 6-12 month lag.
Output: fertilizer_prices.json
"""

import json
import os
import re
import urllib.request
import urllib.error
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
OUTPUT_FILE = OUTPUT_DIR / "fertilizer_prices.json"

CURRENT_YEAR = datetime.now().year
CURRENT_MONTH = datetime.now().month

# ---------------------------------------------------------------------------
# Fertilizer definitions
# ---------------------------------------------------------------------------
FERTILIZERS = {
    "urea": {
        "name": "Ureia",
        "wb_indicator": "COMMODITY_UREA_EE_EUR",
        "wb_alt_indicators": [
            "GEP.FERT.UREA",
        ],
        "indexmundi_slug": "urea",
        "unit": "USD/ton",
    },
    "dap": {
        "name": "DAP (Fosfato Diamonico)",
        "wb_indicator": "GEP.FERT.DAP",
        "wb_alt_indicators": [],
        "indexmundi_slug": "diammonium-phosphate",
        "unit": "USD/ton",
    },
    "mop": {
        "name": "MOP/KCl (Potassio)",
        "wb_indicator": "GEP.FERT.POTASSIUM",
        "wb_alt_indicators": [],
        "indexmundi_slug": "potassium-chloride",
        "unit": "USD/ton",
    },
    "tsp": {
        "name": "TSP (Superfosfato Triplo)",
        "wb_indicator": "GEP.FERT.TSP",
        "wb_alt_indicators": [],
        "indexmundi_slug": "triple-superphosphate",
        "unit": "USD/ton",
    },
}


def fetch_url(url, label=""):
    """Fetch URL. Returns bytes or None."""
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "AgriMacro/3.2 (commodity research)",
            "Accept": "application/json, text/html, */*",
        })
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read()
    except Exception as e:
        print(f"    {label} fetch failed: {e}")
        return None


def try_world_bank_api(indicator, start_year=None, end_year=None):
    """
    Fetch from World Bank API v2.
    Returns list of {date, value} or None.
    """
    sy = start_year or (CURRENT_YEAR - 2)
    ey = end_year or CURRENT_YEAR
    url = (
        f"https://api.worldbank.org/v2/country/WLD/indicator/{indicator}"
        f"?date={sy}:{ey}&format=json&per_page=100"
    )
    raw = fetch_url(url, f"WB {indicator}")
    if not raw:
        return None
    try:
        data = json.loads(raw.decode("utf-8"))
        # WB API returns [metadata, [records]]
        if isinstance(data, list) and len(data) >= 2 and isinstance(data[1], list):
            records = []
            for r in data[1]:
                val = r.get("value")
                date = r.get("date", "")
                if val is not None:
                    records.append({"date": date, "price": float(val)})
            # Sort chronologically
            records.sort(key=lambda x: x["date"])
            return records
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
    return None


def try_world_bank_commodity_prices():
    """
    Try the World Bank Commodity Markets data (Pink Sheet).
    Uses the commodity price API endpoint.
    """
    # Try the commodity prices endpoint
    url = (
        f"https://api.worldbank.org/v2/country/WLD/indicator/COMMODITY.FERT.UREA.NGAS.EUR"
        f"?date={CURRENT_YEAR - 2}:{CURRENT_YEAR}&format=json&per_page=50"
    )
    raw = fetch_url(url, "WB Commodity Prices")
    if raw:
        try:
            data = json.loads(raw.decode("utf-8"))
            if isinstance(data, list) and len(data) >= 2:
                return data[1]
        except (json.JSONDecodeError, ValueError):
            pass
    return None


def try_indexmundi(slug):
    """
    Scrape price data from IndexMundi commodity page.
    Returns list of {date, price} or None.
    """
    url = f"https://www.indexmundi.com/commodities/?commodity={slug}&months=24"
    raw = fetch_url(url, f"IndexMundi {slug}")
    if not raw:
        return None
    try:
        html = raw.decode("utf-8", errors="replace")
        records = []

        # Look for data in the HTML table
        # Pattern: Month Year followed by price value
        # IndexMundi tables have rows like: <td>Jan 2025</td><td>320.00</td>
        table_match = re.search(r'<table[^>]*class="[^"]*tblData[^"]*"[^>]*>(.*?)</table>',
                                html, re.DOTALL | re.IGNORECASE)
        if not table_match:
            # Try alternative table pattern
            table_match = re.search(r'<table[^>]*>(.*?Month.*?)</table>',
                                    html, re.DOTALL | re.IGNORECASE)

        if table_match:
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_match.group(1), re.DOTALL)
            for row in rows:
                cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
                if len(cells) >= 2:
                    date_text = re.sub(r'<[^>]+>', '', cells[0]).strip()
                    price_text = re.sub(r'<[^>]+>', '', cells[1]).strip().replace(",", "")
                    try:
                        price = float(price_text)
                        # Parse date like "Jan 2025"
                        if re.match(r'[A-Z][a-z]+ \d{4}', date_text):
                            records.append({"date": date_text, "price": price})
                    except ValueError:
                        continue

        # Also try to find current price from the page header
        if not records:
            price_match = re.search(
                r'(?:Current|Latest|Price)[:\s]*\$?([\d,]+\.?\d*)',
                html, re.IGNORECASE
            )
            if price_match:
                try:
                    price = float(price_match.group(1).replace(",", ""))
                    records.append({
                        "date": f"{datetime.now().strftime('%b')} {CURRENT_YEAR}",
                        "price": price,
                    })
                except ValueError:
                    pass

        return records if records else None
    except Exception:
        return None


def compute_signal(yoy_pct):
    """Determine price signal based on YoY change."""
    if yoy_pct is None:
        return "SEM DADOS"
    if yoy_pct > 15:
        return "ALTA FORTE"
    if yoy_pct > 5:
        return "ALTA"
    if yoy_pct < -15:
        return "QUEDA FORTE"
    if yoy_pct < -5:
        return "QUEDA"
    return "ESTAVEL"


def main():
    print("  collect_fertilizer: starting...")

    fertilizers_out = {}
    total_ok = 0

    for fert_key, fert_cfg in FERTILIZERS.items():
        print(f"    {fert_cfg['name']}...")
        history = None
        source = "none"

        # Strategy 1: World Bank API (primary indicator)
        indicators_to_try = [fert_cfg["wb_indicator"]] + fert_cfg.get("wb_alt_indicators", [])
        for indicator in indicators_to_try:
            wb_data = try_world_bank_api(indicator)
            if wb_data and len(wb_data) >= 2:
                history = wb_data
                source = f"World Bank ({indicator})"
                print(f"      OK via {source}: {len(history)} records")
                break

        # Strategy 2: IndexMundi scrape
        if not history:
            im_data = try_indexmundi(fert_cfg["indexmundi_slug"])
            if im_data and len(im_data) >= 2:
                history = im_data
                source = "IndexMundi"
                print(f"      OK via {source}: {len(history)} records")

        # Build output
        entry = {
            "name": fert_cfg["name"],
            "unit": fert_cfg["unit"],
            "source": source,
            "price_usd_ton": None,
            "previous_month": None,
            "change_mom_pct": None,
            "year_ago": None,
            "change_yoy_pct": None,
            "signal": "SEM DADOS",
            "history": [],
        }

        if history and len(history) >= 1:
            total_ok += 1
            latest = history[-1]
            entry["price_usd_ton"] = latest["price"]
            entry["history"] = history[-24:]  # Last 24 months

            if len(history) >= 2:
                entry["previous_month"] = history[-2]["price"]
                if history[-2]["price"] > 0:
                    entry["change_mom_pct"] = round(
                        (latest["price"] / history[-2]["price"] - 1) * 100, 1
                    )

            # Find year-ago value (approximately 12 months back)
            if len(history) >= 12:
                ya = history[-12]
                entry["year_ago"] = ya["price"]
                if ya["price"] > 0:
                    entry["change_yoy_pct"] = round(
                        (latest["price"] / ya["price"] - 1) * 100, 1
                    )
            elif len(history) >= 6:
                # Use oldest available as approximation
                ya = history[0]
                entry["year_ago"] = ya["price"]
                if ya["price"] > 0:
                    entry["change_yoy_pct"] = round(
                        (latest["price"] / ya["price"] - 1) * 100, 1
                    )

            entry["signal"] = compute_signal(entry["change_yoy_pct"])

        fertilizers_out[fert_key] = entry

    # Cost impact assessment
    yoy_changes = [
        f["change_yoy_pct"] for f in fertilizers_out.values()
        if f["change_yoy_pct"] is not None
    ]
    avg_yoy = sum(yoy_changes) / len(yoy_changes) if yoy_changes else 0

    if avg_yoy > 10:
        cost_signal = "PRESSAO CUSTO"
    elif avg_yoy < -10:
        cost_signal = "ALIVIO CUSTO"
    else:
        cost_signal = "ESTAVEL"

    details = []
    for fk, fv in fertilizers_out.items():
        if fv["change_yoy_pct"] is not None and abs(fv["change_yoy_pct"]) > 5:
            details.append(f"{fv['name']} {fv['change_yoy_pct']:+.1f}% YoY")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "World Bank / IndexMundi",
        "is_fallback": total_ok == 0,
        "lag_note": "Precos de fertilizantes impactam custo agricola com lag de 6-12 meses",
        "fertilizers": fertilizers_out,
        "cost_impact": {
            "signal": cost_signal,
            "avg_yoy_pct": round(avg_yoy, 1),
            "detail": ", ".join(details) if details else "Sem variacao significativa",
        },
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"  fertilizer: {total_ok}/{len(FERTILIZERS)} fertilizers with data")
    print(f"  Saved to {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    main()
