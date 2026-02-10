"""
AgriMacro v3.2 - Pipeline Runner
Orchestrates all data collection and processing
"""
import os
import sys
import json
import time
from datetime import datetime
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent))

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    icons = {"INFO": "◆", "OK": "✓", "WARN": "!", "ERR": "✗"}
    print(f"[{ts}] [{icons.get(level, '◆')}] {msg}")

def main():
    log("AgriMacro Pipeline v3.2 starting...")
    start = time.time()

    # Paths
    base = Path(__file__).parent.parent / "agrimacro-dash" / "public" / "data"
    raw_path = base / "raw"
    proc_path = base / "processed"
    reports_path = Path(__file__).parent.parent / "reports"

    raw_path.mkdir(parents=True, exist_ok=True)
    proc_path.mkdir(parents=True, exist_ok=True)
    reports_path.mkdir(parents=True, exist_ok=True)

    results = {}

    # =========================================================
    # CORE STEPS (1-8) — essential data collection
    # =========================================================

    # Step 1: Collect prices
    log("Step 1/14: Collecting prices from Yahoo Finance...")
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

    # Step 2: Collect COT (CFTC CSV)
    log("Step 2/14: Collecting COT from CFTC...")
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

    # Step 3: Process seasonality
    log("Step 3/14: Processing seasonality...")
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

    # Step 4: Process spreads
    log("Step 4/14: Processing spreads...")
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

    # Step 5: Process stocks watch
    log("Step 5/14: Processing stocks watch...")
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

    # Step 6: Collect physical market prices
    log("Step 6/14: Collecting physical market prices...")
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

    # Step 7: Collect international physical market prices
    log("Step 7/14: Collecting international physical prices...")
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

    # Step 8: Generate daily reading
    log("Step 8/14: Generating daily reading...")
    try:
        from generate_reading import save_reading
        reading = save_reading(proc_path)
        results["reading"] = {"status": "OK"}
        log("Daily reading generated", "OK")
    except Exception as e:
        results["reading"] = {"status": "ERROR", "error": str(e)}
        log(f"Daily reading failed: {e}", "ERR")

    # =========================================================
    # OPTIONAL STEPS (9-11) — external APIs, may be unavailable
    # These NEVER block the pipeline
    # =========================================================

    # Step 9: Collect BCB, IBGE, CONAB
    log("Step 9/14: Collecting BCB, IBGE, CONAB data...")
    try:
        from collect_bcb_ibge import main as collect_bcb_ibge
        collect_bcb_ibge()
        results["bcb_ibge"] = {"status": "OK", "sources": "BCB, IBGE, CONAB"}
        log("BCB + IBGE + CONAB collected", "OK")
    except Exception as e:
        results["bcb_ibge"] = {"status": "WARN", "error": str(e)}
        log(f"BCB/IBGE/CONAB failed (non-blocking): {e}", "WARN")

    # Step 10: Collect EIA energy data
    log("Step 10/14: Collecting EIA energy data...")
    try:
        from collect_eia import main as collect_eia
        collect_eia()
        results["eia"] = {"status": "OK"}
        log("EIA energy data collected", "OK")
    except Exception as e:
        results["eia"] = {"status": "WARN", "error": str(e)}
        log(f"EIA failed (non-blocking): {e}", "WARN")

    # Step 11: Collect USDA FAS (Export Sales + PSD)
    log("Step 11/14: Collecting USDA FAS data...")
    try:
        from collect_usda_fas import main as collect_fas
        collect_fas()
        results["usda_fas"] = {"status": "OK"}
        log("USDA FAS collected", "OK")
    except Exception as e:
        results["usda_fas"] = {"status": "WARN", "error": str(e)}
        log(f"USDA FAS failed (non-blocking): {e}", "WARN")

    # =========================================================
    # GENERATION STEPS (12-14) — reports and content
    # =========================================================

    # Step 12: Generate daily report (Claude API)
    log("Step 12/14: Generating daily report...")
    try:
        from generate_report import main as generate_report
        generate_report()
        results["report"] = {"status": "OK"}
        log("Daily report generated", "OK")
    except Exception as e:
        results["report"] = {"status": "ERROR", "error": str(e)}
        log(f"Report generation failed: {e}", "ERR")

    # Step 13: Generate daily PDF
    log("Step 13/14: Generating daily PDF...")
    try:
        from generate_daily_pdf import main as generate_pdf
        generate_pdf()
        results["pdf"] = {"status": "OK"}
        log("Daily PDF generated", "OK")
    except Exception as e:
        results["pdf"] = {"status": "ERROR", "error": str(e)}
        log(f"PDF generation failed: {e}", "ERR")

    # Step 14: Generate visual content (PDF visual + video script)
    log("Step 14/14: Generating visual content...")
    try:
        from generate_content import main as generate_content
        generate_content()
        results["content"] = {"status": "OK"}
        log("Visual PDF + video script generated", "OK")
    except Exception as e:
        results["content"] = {"status": "ERROR", "error": str(e)}
        log(f"Content generation failed: {e}", "ERR")

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

    # Save run log
    run_log = {
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": elapsed,
        "ok": ok_count,
        "warnings": warn_count,
        "errors": err_count,
        "results": results
    }
    with open(base / "last_run.json", "w") as f:
        json.dump(run_log, f, indent=2)

    # ALWAYS exit 0 — pipeline never blocks the .bat
    return 0

if __name__ == "__main__":
    sys.exit(main())
