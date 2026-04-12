#!/usr/bin/env python3
"""
collect_google_trends.py - AgriMacro Google Trends Collector
=============================================================
Collects search interest index from Google Trends for agricultural
and macro terms as a leading sentiment indicator.

Uses pytrends (unofficial Google Trends API).
Rate-limited: 60s delay between groups to avoid blocks.
Output: google_trends.json
"""

import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
ROOT_DIR = SCRIPT_DIR.parent

OUTPUT_CANDIDATES = [
    ROOT_DIR / "agrimacro-dash" / "public" / "data" / "processed",
    SCRIPT_DIR / "public" / "data" / "processed",
    SCRIPT_DIR,
]
OUTPUT_DIR = next((c for c in OUTPUT_CANDIDATES if c.exists()), SCRIPT_DIR)
OUTPUT_FILE = OUTPUT_DIR / "google_trends.json"

# ---------------------------------------------------------------------------
# Term groups — each queried separately to respect API limits (max 5 per call)
# ---------------------------------------------------------------------------
GROUPS = [
    {
        "name": "Graos BR",
        "geo": "BR",
        "terms": ["soja", "milho", "trigo", "preco soja", "preco milho"],
    },
    {
        "name": "Graos US",
        "geo": "US",
        "terms": ["soybean price", "corn price", "wheat price", "grain market"],
    },
    {
        "name": "Macro",
        "geo": "",  # worldwide
        "terms": ["dolar hoje", "inflacao", "juros", "commodities"],
    },
    {
        "name": "Clima",
        "geo": "",  # worldwide
        "terms": ["seca cerrado", "geada parana", "el nino", "la nina"],
    },
]

TIMEFRAME = "today 3-m"
SPIKE_SIGMA = 1.5
DELAY_BETWEEN_GROUPS = 60  # seconds


def _analyze_series(values):
    """Compute current, avg_30d, direction, spike from a list of weekly values."""
    if not values or len(values) < 4:
        return None

    current = values[-1]

    # Last ~30 days = last 4 weekly data points
    recent = values[-4:]
    avg_30d = round(sum(recent) / len(recent), 1)

    # Direction: compare last 2 weeks average vs prior 2 weeks
    if len(values) >= 4:
        last_half = sum(values[-2:]) / 2
        prev_half = sum(values[-4:-2]) / 2
        if last_half > prev_half * 1.08:
            direction = "subindo"
        elif last_half < prev_half * 0.92:
            direction = "caindo"
        else:
            direction = "estavel"
    else:
        direction = "estavel"

    # Spike detection: current > avg + 1.5 * stdev
    if len(values) >= 4:
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        stdev = math.sqrt(variance) if variance > 0 else 0
        spike = current > mean + SPIKE_SIGMA * stdev
    else:
        spike = False

    return {
        "current": int(current),
        "avg_30d": avg_30d,
        "direction": direction,
        "spike": spike,
    }


def collect_group(group, pytrends_obj):
    """Collect trends for one group of terms. Returns dict {term: analysis}."""
    terms = group["terms"]
    geo = group["geo"]
    results = {}

    try:
        pytrends_obj.build_payload(terms, cat=0, timeframe=TIMEFRAME, geo=geo)
        df = pytrends_obj.interest_over_time()

        if df is None or df.empty:
            print(f"      No data returned for group '{group['name']}'")
            return results

        for term in terms:
            if term not in df.columns:
                continue
            values = df[term].tolist()
            analysis = _analyze_series(values)
            if analysis:
                analysis["geo"] = geo or "worldwide"
                results[term] = analysis

    except Exception as e:
        print(f"      Error for group '{group['name']}': {e}")

    return results


def main():
    print("  collect_google_trends: starting...")

    try:
        from pytrends.request import TrendReq
    except ImportError:
        print("  ERROR: pytrends not installed. Run: pip install pytrends")
        fallback = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "is_fallback": True,
            "error": "pytrends not installed",
            "trends": {},
            "spikes": [],
            "summary": "",
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(fallback, f, indent=2, ensure_ascii=False)
        return fallback

    pytrends = TrendReq(hl="pt-BR", tz=180, timeout=(10, 30))
    all_trends = {}
    is_fallback = True

    for i, group in enumerate(GROUPS):
        print(f"    Group {i+1}/{len(GROUPS)}: {group['name']} ({len(group['terms'])} terms, geo={group['geo'] or 'worldwide'})...")
        results = collect_group(group, pytrends)
        if results:
            all_trends.update(results)
            is_fallback = False
        ok = len(results)
        total = len(group["terms"])
        print(f"      {ok}/{total} terms collected")

        # Rate limit: wait between groups (skip after last)
        if i < len(GROUPS) - 1:
            print(f"      Waiting {DELAY_BETWEEN_GROUPS}s (rate limit)...")
            time.sleep(DELAY_BETWEEN_GROUPS)

    # Spikes
    spikes = [t for t, d in all_trends.items() if d.get("spike")]
    summary = ""
    if spikes:
        summary = "Interesse elevado em: " + ", ".join(spikes)
    elif all_trends:
        # Top 3 by current value
        top = sorted(all_trends.items(), key=lambda x: x[1].get("current", 0), reverse=True)[:3]
        summary = "Termos mais buscados: " + ", ".join(f"{t} ({d['current']})" for t, d in top)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "is_fallback": is_fallback,
        "source": "Google Trends via pytrends",
        "timeframe": TIMEFRAME,
        "trends": all_trends,
        "spikes": spikes,
        "summary": summary,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"  google_trends: {len(all_trends)} terms, {len(spikes)} spikes")
    print(f"  Saved to {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    main()
