#!/usr/bin/env python3
"""
collect_fedwatch.py - AgriMacro FedWatch Collector
===================================================
Calculates Fed rate-decision probabilities from Fed Funds Futures (ZQ)
prices via yfinance, supplemented by FRED for the current effective rate.

Method:
  Implied rate = 100 - ZQ futures price
  If implied rate < current midpoint -> market expects cut
  If implied rate > current midpoint -> market expects hike
  Probability = distance from current rate / 0.25 (one step)

Fallback chain:
  1. yfinance ZQ futures + FRED effective rate (primary)
  2. FRED only (limited: current rate + target range, no probabilities)

Output: fedwatch.json
"""

import json
import os
import urllib.request
import urllib.error
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
OUTPUT_FILE = OUTPUT_DIR / "fedwatch.json"

FRED_KEY_PATH = os.path.join(os.path.expanduser("~"), ".fred_key")

# FOMC scheduled meeting dates for 2026 (announcement day)
# Source: federalreserve.gov — update annually
FOMC_DATES_2026 = [
    "2026-01-29",
    "2026-03-19",
    "2026-05-07",
    "2026-06-17",
    "2026-07-29",
    "2026-09-17",
    "2026-11-05",
    "2026-12-16",
]

# Fed Funds Futures contract months (ZQ) — ticker format for yfinance
# Month codes: F=Jan G=Feb H=Mar J=Apr K=May M=Jun N=Jul Q=Aug U=Sep V=Oct X=Nov Z=Dec
MONTH_CODES = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}


def _get_fred_key():
    if os.path.exists(FRED_KEY_PATH):
        return open(FRED_KEY_PATH).read().strip()
    return ""


def _fred_latest(series_id, key):
    """Fetch latest value from FRED series."""
    try:
        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&api_key={key}"
            f"&file_type=json&sort_order=desc&limit=5"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "AgriMacro/3.2"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            for obs in data.get("observations", []):
                val = obs.get("value", ".")
                if val != ".":
                    return float(val), obs["date"]
    except Exception as e:
        print(f"    FRED {series_id} error: {e}")
    return None, None


def fetch_current_rate():
    """Get current Fed Funds target range from FRED."""
    key = _get_fred_key()
    if not key:
        print("    No FRED key found, skipping FRED data")
        return None, None, None

    upper, upper_date = _fred_latest("DFEDTARU", key)
    lower, lower_date = _fred_latest("DFEDTARL", key)
    effective, eff_date = _fred_latest("DFF", key)

    if upper is not None and lower is not None:
        midpoint = (upper + lower) / 2
        return midpoint, {"upper": upper, "lower": lower, "effective": effective, "date": upper_date}, None
    elif effective is not None:
        return effective, {"effective": effective, "date": eff_date}, None

    return None, None, "FRED data unavailable"


def fetch_zq_futures():
    """Fetch Fed Funds Futures (ZQ) prices from yfinance for upcoming months."""
    try:
        import yfinance as yf
    except ImportError:
        print("    yfinance not available")
        return {}

    today = datetime.now()
    year = today.year
    results = {}

    # Fetch contracts for current month + next 8 months
    for offset in range(9):
        m = ((today.month - 1 + offset) % 12) + 1
        y = year + ((today.month - 1 + offset) // 12)
        code = MONTH_CODES[m]
        yy = str(y)[-2:]
        ticker = f"ZQ{code}{yy}.CBT"

        try:
            tk = yf.Ticker(ticker)
            h = tk.history(period="5d")
            if not h.empty:
                close = float(h.iloc[-1]["Close"])
                implied = round(100.0 - close, 4)
                date_str = f"{y}-{m:02d}"
                results[date_str] = {
                    "ticker": ticker,
                    "close": round(close, 4),
                    "implied_rate": implied,
                    "trade_date": h.index[-1].strftime("%Y-%m-%d"),
                }
        except Exception:
            pass

    return results


def _upcoming_meetings():
    """Return FOMC meetings that haven't happened yet."""
    today = datetime.now().strftime("%Y-%m-%d")
    return [d for d in FOMC_DATES_2026 if d >= today]


def _meeting_month(date_str):
    """Return YYYY-MM from a meeting date."""
    return date_str[:7]


def compute_probabilities(midpoint, zq_data, meetings):
    """
    Compute rate-decision probabilities for each meeting.

    Uses the standard CME FedWatch method:
    - Implied rate from ZQ for the meeting month
    - Compare to the current effective midpoint
    - Delta / 0.25 gives probability of a 25bp move
    """
    step = 0.25  # 25 basis points
    results = []

    for meeting_date in meetings:
        mm = _meeting_month(meeting_date)
        zq = zq_data.get(mm)
        if not zq:
            results.append({
                "date": meeting_date,
                "implied_rate": None,
                "expected": "sem dados",
                "cut_prob": None,
                "hold_prob": None,
                "hike_prob": None,
            })
            continue

        implied = zq["implied_rate"]
        delta = implied - midpoint

        if abs(delta) < 0.02:
            # Within noise — hold
            hold_prob = 100.0
            cut_prob = 0.0
            hike_prob = 0.0
            expected = "hold"
        elif delta < 0:
            # Implied rate below midpoint -> cut expected
            raw_cut = min(abs(delta) / step * 100, 100)
            cut_prob = round(raw_cut, 1)
            hold_prob = round(100 - cut_prob, 1)
            hike_prob = 0.0
            expected = "cut" if cut_prob > 50 else "hold"
        else:
            # Implied rate above midpoint -> hike expected
            raw_hike = min(delta / step * 100, 100)
            hike_prob = round(raw_hike, 1)
            hold_prob = round(100 - hike_prob, 1)
            cut_prob = 0.0
            expected = "hike" if hike_prob > 50 else "hold"

        results.append({
            "date": meeting_date,
            "implied_rate": round(implied, 4),
            "expected": expected,
            "cut_prob": cut_prob,
            "hold_prob": hold_prob,
            "hike_prob": hike_prob,
        })

    return results


def main():
    print("  collect_fedwatch: starting...")

    # Step 1: Get current rate from FRED
    midpoint, rate_info, rate_err = fetch_current_rate()
    if midpoint is not None:
        print(f"    Current rate midpoint: {midpoint}% (target: {rate_info.get('lower','?')}-{rate_info.get('upper','?')}%)")
    else:
        print(f"    Could not get current rate: {rate_err}")

    # Step 2: Get ZQ futures
    zq_data = fetch_zq_futures()
    print(f"    ZQ futures: {len(zq_data)} months")
    for mm, zq in sorted(zq_data.items()):
        print(f"      {mm}: implied={zq['implied_rate']:.3f}%")

    # Step 3: Compute probabilities
    meetings = _upcoming_meetings()
    is_fallback = False

    if midpoint is not None and zq_data:
        meetings_ahead = compute_probabilities(midpoint, zq_data, meetings)
        # Next meeting summary
        next_mtg = meetings_ahead[0] if meetings_ahead else None
        probabilities = None
        market_expectation = None
        if next_mtg and next_mtg["hold_prob"] is not None:
            probabilities = {
                "cut_25bps": next_mtg["cut_prob"],
                "hold": next_mtg["hold_prob"],
                "hike_25bps": next_mtg["hike_prob"],
            }
            market_expectation = next_mtg["expected"]
    elif midpoint is not None:
        # FRED only fallback — no probabilities
        meetings_ahead = [{"date": d, "implied_rate": None, "expected": "sem dados", "cut_prob": None, "hold_prob": None, "hike_prob": None} for d in meetings]
        probabilities = None
        market_expectation = None
        print("    No ZQ data — probabilities unavailable")
    else:
        is_fallback = True
        meetings_ahead = []
        probabilities = None
        market_expectation = None
        rate_info = None
        print("    All sources failed — fallback mode")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "is_fallback": is_fallback,
        "source": "Fed Funds Futures (ZQ) via yfinance + FRED/SGS",
        "current_rate": midpoint,
        "target_range": rate_info,
        "next_meeting": meetings[0] if meetings else None,
        "probabilities": probabilities,
        "market_expectation": market_expectation,
        "meetings_ahead": meetings_ahead,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"    Market expectation: {market_expectation or 'N/A'}")
    print(f"  fedwatch: saved to {OUTPUT_FILE}")
    return output


if __name__ == "__main__":
    main()
