#!/usr/bin/env python3
"""
collect_comexstat.py — AgriMacro Intelligence
Coleta dados de exportação brasileira via API REST Comex Stat (MDIC)
Fonte: https://api-comexstat.mdic.gov.br

Commodities monitoradas (SH4 heading codes):
  1201 - Soja em grão
  1005 - Milho em grão
  2304 - Farelo de soja
  1507 - Óleo de soja
  0201 - Carne bovina fresca/refrigerada
  0202 - Carne bovina congelada

Output: JSON padronizado AgriMacro em data/comexstat/
Princípio: ZERO MOCK — apenas dados reais do MDIC/SISCOMEX
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

API_URL = "https://api-comexstat.mdic.gov.br/general"

# SH4 heading codes — AgriMacro commodities
COMMODITIES = {
    "soja_grao":    {"heading": "1201", "label": "Soybeans",           "label_pt": "Soja em Grão"},
    "milho_grao":   {"heading": "1005", "label": "Corn",              "label_pt": "Milho em Grão"},
    "farelo_soja":  {"heading": "2304", "label": "Soybean Meal",      "label_pt": "Farelo de Soja"},
    "oleo_soja":    {"heading": "1507", "label": "Soybean Oil",       "label_pt": "Óleo de Soja"},
    "carne_bov_fr": {"heading": "0201", "label": "Beef Fresh/Chilled","label_pt": "Carne Bovina Fresca"},
    "carne_bov_cg": {"heading": "0202", "label": "Beef Frozen",       "label_pt": "Carne Bovina Congelada"},
}

# Diretórios
BASE_DIR = Path(os.environ.get("AGRIMACRO_DATA_DIR", "data"))
OUTPUT_DIR = BASE_DIR / "comexstat"
CACHE_DIR = OUTPUT_DIR / "cache"

# Logging
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("collect_comexstat")


# ─────────────────────────────────────────────
# API CLIENT
# ─────────────────────────────────────────────

def _post(payload, timeout=60):
    """POST to Comex Stat API, returns list of records."""
    headers = {"Content-Type": "application/json"}
    resp = requests.post(API_URL, json=payload, headers=headers, timeout=timeout, verify=False)
    resp.raise_for_status()
    body = resp.json()
    return body.get("data", {}).get("list", [])


def fetch_monthly_totals(headings, period_from, period_to):
    """
    Busca totais mensais por heading (SH4).
    Returns: year, monthNumber, headingCode, heading, metricFOB, metricKG
    """
    payload = {
        "flow": "export",
        "monthDetail": True,
        "period": {"from": period_from, "to": period_to},
        "filters": [{"filter": "heading", "values": headings}],
        "details": ["heading"],
        "metrics": ["metricFOB", "metricKG"],
    }
    logger.info(f"Fetching monthly totals: {period_from} to {period_to}, headings={headings}")
    records = _post(payload)
    logger.info(f"  -> {len(records)} records")
    return records


def fetch_by_country(headings, period_from, period_to):
    """
    Busca totais anuais por heading + pais de destino.
    Nota: monthDetail=False obrigatorio (API nao combina heading+country+monthDetail).
    Returns: year, headingCode, heading, country, metricFOB, metricKG
    """
    payload = {
        "flow": "export",
        "monthDetail": False,
        "period": {"from": period_from, "to": period_to},
        "filters": [{"filter": "heading", "values": headings}],
        "details": ["heading", "country"],
        "metrics": ["metricFOB", "metricKG"],
    }
    logger.info(f"Fetching by country: {period_from} to {period_to}")
    records = _post(payload)
    logger.info(f"  -> {len(records)} records")
    return records


# ─────────────────────────────────────────────
# DATA PROCESSING
# ─────────────────────────────────────────────

def build_heading_map():
    """Maps heading code -> commodity key."""
    return {info["heading"]: key for key, info in COMMODITIES.items()}


def process_monthly(records):
    """
    Processa registros mensais em serie temporal por commodity.
    """
    heading_map = build_heading_map()
    result = {}

    for rec in records:
        heading_code = rec.get("headingCode", "")
        commodity_key = heading_map.get(heading_code)
        if not commodity_key:
            continue

        year = rec.get("year", "")
        month = rec.get("monthNumber", "")
        period = f"{year}-{str(month).zfill(2)}"
        fob = float(rec.get("metricFOB", 0))
        kg = float(rec.get("metricKG", 0))

        if commodity_key not in result:
            result[commodity_key] = {}

        if period in result[commodity_key]:
            result[commodity_key][period]["fob_usd"] += fob
            result[commodity_key][period]["weight_kg"] += kg
            result[commodity_key][period]["weight_mmt"] += kg / 1e9
        else:
            result[commodity_key][period] = {
                "fob_usd": fob,
                "weight_kg": kg,
                "weight_mmt": kg / 1e9,
            }

    # Round
    for commodity in result:
        for period in result[commodity]:
            result[commodity][period]["weight_mmt"] = round(result[commodity][period]["weight_mmt"], 4)
            result[commodity][period]["fob_usd"] = round(result[commodity][period]["fob_usd"], 2)

    return result


def process_by_country(records):
    """
    Processa registros por pais: {commodity: {country: {year: {fob, kg, mmt}}}}.
    """
    heading_map = build_heading_map()
    result = {}

    for rec in records:
        heading_code = rec.get("headingCode", "")
        commodity_key = heading_map.get(heading_code)
        if not commodity_key:
            continue

        country = rec.get("country", "Unknown")
        year = rec.get("year", "")
        fob = float(rec.get("metricFOB", 0))
        kg = float(rec.get("metricKG", 0))

        if commodity_key not in result:
            result[commodity_key] = {}
        if country not in result[commodity_key]:
            result[commodity_key][country] = {}

        result[commodity_key][country][year] = {
            "fob_usd": round(fob, 2),
            "weight_kg": kg,
            "weight_mmt": round(kg / 1e9, 4),
        }

    return result


def compute_ytd(monthly_data, year):
    """
    Calcula acumulado Year-to-Date por commodity.
    Alimenta o Export Race Tracker.
    """
    ytd = {}
    year_str = str(year)

    for commodity, months in monthly_data.items():
        cumulative = 0
        month_list = []

        for period in sorted(months.keys()):
            if period.startswith(year_str):
                monthly_mmt = months[period]["weight_mmt"]
                cumulative += monthly_mmt
                month_list.append({
                    "period": period,
                    "monthly_mmt": round(monthly_mmt, 4),
                    "cumulative_mmt": round(cumulative, 4),
                })

        ytd[commodity] = {
            "year": year,
            "months": month_list,
            "total_mmt": round(cumulative, 4),
        }

    return ytd


def compute_totals(monthly_data, country_data):
    """Resumo por commodity: total + top destinos."""
    totals = {}

    for key, info in COMMODITIES.items():
        months = monthly_data.get(key, {})
        total_kg = sum(m["weight_kg"] for m in months.values())
        total_fob = sum(m["fob_usd"] for m in months.values())

        # Top destinations
        top_dest = {}
        if key in country_data:
            for country, years in country_data[key].items():
                country_kg = sum(y["weight_kg"] for y in years.values())
                top_dest[country] = round(country_kg / 1e9, 4)
            top_dest = dict(sorted(top_dest.items(), key=lambda x: x[1], reverse=True)[:10])

        totals[key] = {
            "label": info["label"],
            "label_pt": info["label_pt"],
            "heading": info["heading"],
            "total_fob_usd": round(total_fob, 2),
            "total_weight_mmt": round(total_kg / 1e9, 4),
            "top_destinations_mmt": top_dest,
        }

    return totals


# ─────────────────────────────────────────────
# CACHE
# ─────────────────────────────────────────────

def load_cache():
    cache_file = CACHE_DIR / "comexstat_latest.json"
    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_cache(data):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_DIR / "comexstat_latest.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def save_output(data, filename):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    filepath = OUTPUT_DIR / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved: {filepath}")
    return filepath


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def collect(year=None, months_back=12):
    """
    Coleta completa: dados mensais + breakdown por pais.
    """
    now = datetime.now()
    ts = now.isoformat()

    # Periodo
    if year:
        period_from = f"{year}-01"
        period_to = f"{year}-12"
    else:
        start = now - timedelta(days=30 * months_back)
        period_from = start.strftime("%Y-%m")
        period_to = now.strftime("%Y-%m")

    all_headings = [info["heading"] for info in COMMODITIES.values()]

    try:
        # Call 1: Monthly totals (with month detail, no country breakdown)
        monthly_raw = fetch_monthly_totals(all_headings, period_from, period_to)

        # Call 2: Annual by country (no month detail, with country breakdown)
        country_raw = fetch_by_country(all_headings, period_from, period_to)

    except Exception as e:
        logger.error(f"API error: {e}")
        cached = load_cache()
        if cached:
            logger.warning("Using cached data")
            cached["status"] = "cached"
            cached["cache_note"] = f"API failed: {e}"
            return cached
        return {"source": "comexstat", "status": "error", "error": str(e), "data": {}}

    # Process
    monthly_data = process_monthly(monthly_raw)
    country_data = process_by_country(country_raw)
    current_year = year or now.year
    ytd = compute_ytd(monthly_data, current_year)
    totals = compute_totals(monthly_data, country_data)

    output = {
        "source": "comexstat",
        "source_url": "https://comexstat.mdic.gov.br",
        "collection_timestamp": ts,
        "status": "ok",
        "period": {"from": period_from, "to": period_to},
        "data": {
            "totals": totals,
            "monthly": monthly_data,
            "by_country": country_data,
            "ytd": ytd,
        },
    }

    # Save
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    save_output(output, f"comexstat_export_{timestamp}.json")
    save_output(output, "comexstat_export_latest.json")
    save_cache(output)

    # Log summary
    logger.info("=" * 60)
    logger.info("COMEX STAT - COLLECTION SUMMARY")
    logger.info(f"Period: {period_from} to {period_to}")
    logger.info("=" * 60)
    for key, t in totals.items():
        mmt = t["total_weight_mmt"]
        fob_b = round(t["total_fob_usd"] / 1e9, 2)
        top3 = list(t["top_destinations_mmt"].keys())[:3]
        logger.info(f"  {t['label']:.<25} {mmt:>8.2f} MMT | USD {fob_b:>6.2f}B | Top: {', '.join(top3)}")
    logger.info("=" * 60)

    # YTD summary
    if ytd:
        logger.info(f"YTD {current_year}:")
        for key, y in ytd.items():
            if y["total_mmt"] > 0:
                label = COMMODITIES[key]["label"]
                logger.info(f"  {label}: {y['total_mmt']:.2f} MMT accumulated")
    logger.info("=" * 60)

    return output


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="AgriMacro - Comex Stat Collector")
    parser.add_argument("--year", type=int, default=None, help="Specific year (default: last 12 months)")
    parser.add_argument("--months", type=int, default=12, help="Months back if no year specified")
    parser.add_argument("--output-dir", type=str, default=None, help="Override output directory")
    args = parser.parse_args()

    if args.output_dir:
        OUTPUT_DIR = Path(args.output_dir)
        CACHE_DIR = OUTPUT_DIR / "cache"

    result = collect(year=args.year, months_back=args.months)

    if result["status"] == "ok":
        sys.exit(0)
    elif result["status"] == "cached":
        sys.exit(0)
    else:
        sys.exit(1)
