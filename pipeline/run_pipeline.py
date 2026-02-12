"""
AgriMacro v3.2 - Pipeline Runner (18 Steps)
Orchestrates all data collection, processing, and content generation.

Steps:
  CORE (1-8): Prices, COT, Seasonality, Spreads, Stocks, Physical US, Physical Intl, Daily Reading
  OPTIONAL (9-13): BCB/IBGE, EIA, USDA FAS, News Agro, Weather/Clima
  GENERATION (14-18): Calendar, Daily Report, PDF Report, Video Script, Video MP4
"""
import os
import sys
import json
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    icons = {"INFO": "\u25c6", "OK": "\u2714", "WARN": "!", "ERR": "\u2718"}
    print(f"[{ts}] [{icons.get(level, '\u25c6')}] {msg}")

def main():
    log("AgriMacro Pipeline v3.2 starting...")
    start = time.time()

    base = Path(__file__).parent.parent / "agrimacro-dash" / "public" / "data"
    raw_path = base / "raw"
    proc_path = base / "processed"
    reports_path = Path(__file__).parent.parent / "reports"

    raw_path.mkdir(parents=True, exist_ok=True)
    proc_path.mkdir(parents=True, exist_ok=True)
    reports_path.mkdir(parents=True, exist_ok=True)

    results = {}
    total_steps = 18

    # =========================================================
    # CORE STEPS (1-8)
    # =========================================================

    log(f"Step 1/{total_steps}: Collecting prices from Yahoo Finance...")
    try:
        from collect_prices import collect_all_prices
        from collect_ibkr import collect_ibkr_data
        ibkr_ok = collect_ibkr_data()
        if not ibkr_ok:
            print("  IBKR unavailable, using Yahoo Finance...")
        prices = collect_all_prices()
        with open(raw_path / "price_history.json", "w") as f:
            json.dump(prices, f)
        results["prices"] = {"status": "OK", "count": len(prices)}
        log(f"Prices collected: {len(prices)} commodities", "OK")
    except Exception as e:
        results["prices"] = {"status": "ERROR", "error": str(e)}
        log(f"Prices failed: {e}", "ERR")

    log(f"Step 2/{total_steps}: Collecting COT from CFTC...")
    try:
        from collect_cot import collect_cot_data
        cot = collect_cot_data()
        with open(raw_path / "cot_data.json", "w") as f:
            json.dump(cot, f)
        results["cot"] = {"status": "OK", "count": len(cot)}
        log(f"COT collected: {len(cot)} commodities", "OK")
    except Exception as e:
        results["cot"] = {"status": "ERROR", "error": str(e)}
        log(f"COT failed: {e}", "ERR")

    log(f"Step 3/{total_steps}: Processing seasonality...")
    try:
        from process_seasonality import process_seasonality
        season = process_seasonality(raw_path / "price_history.json")
        with open(proc_path / "seasonality.json", "w") as f:
            json.dump(season, f)
        results["seasonality"] = {"status": "OK", "count": len(season)}
        log(f"Seasonality processed: {len(season)} commodities", "OK")
    except Exception as e:
        results["seasonality"] = {"status": "ERROR", "error": str(e)}
        log(f"Seasonality failed: {e}", "ERR")

    log(f"Step 4/{total_steps}: Processing spreads...")
    try:
        from process_spreads import process_spreads
        spreads = process_spreads(raw_path / "price_history.json")
        with open(proc_path / "spreads.json", "w") as f:
            json.dump(spreads, f)
        results["spreads"] = {"status": "OK", "count": len(spreads.get("spreads", {}))}
        log(f"Spreads processed: {len(spreads.get('spreads', {}))} spreads", "OK")
    except Exception as e:
        results["spreads"] = {"status": "ERROR", "error": str(e)}
        log(f"Spreads failed: {e}", "ERR")

    log(f"Step 5/{total_steps}: Processing stocks watch...")
    try:
        from process_stocks import process_stocks_watch
        stocks = process_stocks_watch(proc_path / "seasonality.json")
        with open(proc_path / "stocks_watch.json", "w") as f:
            json.dump(stocks, f)
        results["stocks"] = {"status": "OK", "count": len(stocks.get("commodities", {}))}
        log(f"Stocks watch processed: {len(stocks.get('commodities', {}))} commodities", "OK")
    except Exception as e:
        results["stocks"] = {"status": "ERROR", "error": str(e)}
        log(f"Stocks watch failed: {e}", "ERR")

    log(f"Step 6/{total_steps}: Collecting physical market prices...")
    try:
        from collect_physical import collect_physical
        physical = collect_physical(str(raw_path / "price_history.json"))
        with open(proc_path / "physical.json", "w") as f:
            json.dump(physical, f)
        results["physical"] = {"status": "OK", "count": len(physical.get("us_cash", {}))}
        log(f"Physical prices collected: {len(physical.get('us_cash', {}))} US markets", "OK")
    except Exception as e:
        results["physical"] = {"status": "ERROR", "error": str(e)}
        log(f"Physical prices failed: {e}", "ERR")

    log(f"Step 7/{total_steps}: Collecting international physical prices...")
    try:
        from collect_physical_intl import collect_physical_intl
        phys_intl = collect_physical_intl()
        with open(proc_path / "physical_intl.json", "w", encoding="utf-8") as f:
            json.dump(phys_intl, f, ensure_ascii=False, indent=2)
        results["physical_intl"] = {"status": "OK", "count": phys_intl.get("markets_with_data", 0)}
        log(f"Intl physical collected: {phys_intl.get('markets_with_data', 0)} markets with data", "OK")
    except Exception as e:
        results["physical_intl"] = {"status": "ERROR", "error": str(e)}
        log(f"Intl physical failed: {e}", "ERR")

    log(f"Step 8/{total_steps}: Generating daily reading...")
    try:
        from generate_reading import save_reading
        reading = save_reading(proc_path)
        results["reading"] = {"status": "OK"}
        log("Daily reading generated", "OK")
    except Exception as e:
        results["reading"] = {"status": "ERROR", "error": str(e)}
        log(f"Daily reading failed: {e}", "ERR")

    # =========================================================
    # OPTIONAL STEPS (9-13) - NEVER block the pipeline
    # =========================================================

    log(f"Step 9/{total_steps}: Collecting BCB, IBGE, CONAB data...")
    try:
        from collect_bcb_ibge import main as collect_bcb_ibge
        collect_bcb_ibge()
        results["bcb_ibge"] = {"status": "OK", "sources": "BCB, IBGE, CONAB"}
        log("BCB + IBGE + CONAB collected", "OK")
    except BaseException as e:
        results["bcb_ibge"] = {"status": "WARN", "error": str(e)}
        log(f"BCB/IBGE/CONAB failed (non-blocking): {e}", "WARN")

    log(f"Step 10/{total_steps}: Collecting EIA energy data...")
    try:
        from collect_eia import main as collect_eia
        collect_eia()
        results["eia"] = {"status": "OK"}
        log("EIA energy data collected", "OK")
    except BaseException as e:
        results["eia"] = {"status": "WARN", "error": str(e)}
        log(f"EIA failed (non-blocking): {e}", "WARN")

    log(f"Step 11/{total_steps}: Collecting USDA FAS data...")
    try:
        from collect_usda_fas import main as collect_fas
        collect_fas()
        results["usda_fas"] = {"status": "OK"}
        log("USDA FAS collected", "OK")
    except BaseException as e:
        results["usda_fas"] = {"status": "WARN", "error": str(e)}
        log(f"USDA FAS failed (non-blocking): {e}", "WARN")

    log(f"Step 12/{total_steps}: Collecting news & FRED macro...")
    try:
        from collect_news import main as collect_news
        collect_news()
        results["news"] = {"status": "OK"}
        log("News & FRED collected", "OK")
    except BaseException as e:
        results["news"] = {"status": "WARN", "error": str(e)}
        log(f"News failed (non-blocking): {e}", "WARN")

    log(f"Step 13/{total_steps}: Collecting agricultural weather...")
    try:
        from collect_weather import main as collect_weather
        collect_weather()
        results["weather"] = {"status": "OK"}
        log("Weather data collected", "OK")
    except BaseException as e:
        results["weather"] = {"status": "WARN", "error": str(e)}
        log(f"Weather failed (non-blocking): {e}", "WARN")

    # =========================================================
    # GENERATION STEPS (14-18)
    # =========================================================

    log(f"Step 14/{total_steps}: Generating calendar...")
    try:
        from collect_calendar import main as generate_calendar
        generate_calendar()
        results["calendar"] = {"status": "OK"}
        log("Calendar generated", "OK")
    except Exception as e:
        results["calendar"] = {"status": "WARN", "error": str(e)}
        log(f"Calendar failed (non-blocking): {e}", "WARN")

    log(f"Step 15/{total_steps}: Generating daily report...")
    try:
        from generate_report import main as generate_report
        generate_report()
        results["report"] = {"status": "OK"}
        log("Daily report generated", "OK")
    except Exception as e:
        results["report"] = {"status": "ERROR", "error": str(e)}
        log(f"Report generation failed: {e}", "ERR")

    log(f"Step 16/{total_steps}: Generating PDF report...")
    try:
        from generate_report_pdf import build_pdf
        build_pdf()
        results["pdf"] = {"status": "OK"}
        log("PDF report generated", "OK")
    except Exception as e:
        results["pdf"] = {"status": "ERROR", "error": str(e)}
        log(f"PDF generation failed: {e}", "ERR")

    log(f"Step 17/{total_steps}: Generating video script...")
    try:
        from generate_video_script import main as generate_video
        generate_video()
        results["video_script"] = {"status": "OK"}
        log("Video script generated", "OK")
    except Exception as e:
        results["video_script"] = {"status": "ERROR", "error": str(e)}
        log(f"Video script failed: {e}", "ERR")

    log(f"Step 18/{total_steps}: Generating video MP4...")
    try:
        from step18_video_generator import main as generate_video_mp4
        generate_video_mp4()
        results["video_mp4"] = {"status": "OK"}
        log("Video MP4 generated", "OK")
    except BaseException as e:
        results["video_mp4"] = {"status": "WARN", "error": str(e)}
        log(f"Video MP4 failed (non-blocking): {e}", "WARN")

    # =========================================================
    # SUMMARY
    # =========================================================
    elapsed = time.time() - start
    ok_count = sum(1 for r in results.values() if r.get("status") == "OK")
    warn_count = sum(1 for r in results.values() if r.get("status") == "WARN")
    err_count = sum(1 for r in results.values() if r.get("status") == "ERROR")
    total = len(results)

    log("=" * 50)
    log(f"Pipeline completed in {elapsed:.1f}s")
    log(f"Results: {ok_count} OK / {warn_count} WARN / {err_count} ERR (total {total})")

    if warn_count > 0:
        warns = [k for k, v in results.items() if v.get("status") == "WARN"]
        log(f"Warnings (non-blocking): {', '.join(warns)}", "WARN")
    if err_count > 0:
        errs = [k for k, v in results.items() if v.get("status") == "ERROR"]
        log(f"Errors: {', '.join(errs)}", "ERR")

    run_log = {
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": elapsed,
        "pipeline_version": "3.2",
        "total_steps": total_steps,
        "ok": ok_count,
        "warnings": warn_count,
        "errors": err_count,
        "results": results
    }
    with open(base / "last_run.json", "w") as f:
        json.dump(run_log, f, indent=2)

    return 0

if __name__ == "__main__":
    sys.exit(main())
