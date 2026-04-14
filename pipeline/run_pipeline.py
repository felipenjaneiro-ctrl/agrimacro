"""
AgriMacro v3.3 - Pipeline Runner (31 Steps)
Orchestrates all data collection, processing, and content generation.

Steps:
  PRICES (1-3): IBKR (primary), Yahoo (fallback), Price Validation
  CORE (4-11): COT, Seasonality, Spreads, Parities, Stocks, Physical US, Physical Intl, Daily Reading
  OPTIONAL (12-25): BCB/IBGE, EIA, USDA FAS, Livestock PSD/Weekly, Bilateral, News, Weather, Crop Progress, Macro, Google Trends, FedWatch, Correlations, Grok
  GENERATION (26-31): Calendar, Daily Report, Grain Ratios, Intel Synthesis, PDF Report, Video Script, Video MP4
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
    total_steps = 34

    # =========================================================
    # CORE STEPS (1-10)
    # =========================================================

    # --- COLETA DE PRECOS -------------------------------------------
    ibkr_symbols = set()

    # Step 1: IBKR (FONTE PRIMARIA)
    log(f"Step 1/{total_steps}: Coletando precos via IBKR (fonte primaria)...")
    try:
        from collect_ibkr import collect_ibkr_data
        ibkr_result = collect_ibkr_data()
        if isinstance(ibkr_result, dict):
            ibkr_symbols = ibkr_result.get("symbols_collected", set())
            ibkr_ok = ibkr_result.get("status", False)
        else:
            ibkr_ok = bool(ibkr_result)
        if ibkr_ok:
            log(f"IBKR: {len(ibkr_symbols)} simbolos coletados", "OK")
            try:
                _gp = json.load(open(proc_path / "ibkr_greeks.json"))
                _pg = _gp.get("portfolio_greeks", {})
                log(f"Greeks: {_pg.get('positions_with_greeks',0)}/{_pg.get('positions_with_greeks',0)+_pg.get('positions_without_greeks',0)} posicoes | delta={_pg.get('total_delta',0)} theta={_pg.get('total_theta',0)} vega={_pg.get('total_vega',0)}", "OK")
            except Exception:
                pass
        else:
            log("IBKR offline -- continuando com Yahoo", "WARN")
        results["prices_ibkr"] = {"status": "OK" if ibkr_ok else "WARN", "symbols": len(ibkr_symbols)}
    except Exception as e:
        log(f"IBKR offline -- continuando com Yahoo: {e}", "WARN")
        results["prices_ibkr"] = {"status": "WARN", "error": str(e)}

    # Step 1b: Options Chain (OPTIONAL -- falha nao bloqueia)
    try:
        from ib_insync import util as _ib_util
        from collect_options_chain import main as collect_chain
        _ib_util.run(collect_chain())
        log("Options chain coletada", "OK")
        results["options_chain"] = {"status": "OK"}
    except Exception as e:
        log(f"Options chain falhou (nao critico): {e}", "WARN")
        results["options_chain"] = {"status": "WARN", "error": str(e)}

    # Step 2: Yahoo Finance (FALLBACK para gaps)
    gaps = 19 - len(ibkr_symbols)
    log(f"Step 2/{total_steps}: Yahoo Finance (fallback para {gaps} gaps)...")
    try:
        from collect_prices import main as collect_prices_main
        collect_prices_main(skip_symbols=ibkr_symbols)
        log("Yahoo: gaps preenchidos", "OK")
        results["prices_yahoo"] = {"status": "OK"}
    except Exception as e:
        log(f"Yahoo falhou: {e}", "WARN")
        results["prices_yahoo"] = {"status": "WARN", "error": str(e)}

    # Step 3: VALIDACAO OBRIGATORIA (sempre roda)
    log(f"Step 3/{total_steps}: Validando integridade dos precos...")
    try:
        from validate_prices import validate_and_fix
        val = validate_and_fix()
        blocked = [
            s for s, v in val.get("details", {}).items()
            if v.get("is_suspicious")
        ]
        if blocked:
            log(f"DADOS SUSPEITOS DETECTADOS E BLOQUEADOS: {blocked}", "WARN")
        else:
            log("Todos os precos validados -- zero suspeitos", "OK")
        results["price_validation"] = {
            "status": "WARN" if blocked else "OK",
            "blocked": blocked
        }
    except Exception as e:
        log(f"Validacao falhou (CRITICO): {e}", "ERR")
        results["price_validation"] = {"status": "ERR", "error": str(e)}

    log(f"Step 4/{total_steps}: Collecting COT from CFTC...")
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

    log(f"Step 5/{total_steps}: Processing seasonality...")
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

    log(f"Step 6/{total_steps}: Processing spreads...")
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

    # Step 5: Collect parities and correlations
    log(f"Step 7/{total_steps}: Calculating market parities...")
    try:
        from collect_parities import main as collect_parities
        collect_parities()
        results["parities"] = {"status": "OK"}
        log("Parities calculated", "OK")
    except Exception as e:
        results["parities"] = {"status": "WARN", "error": str(e)}
        log(f"Parities failed (non-blocking): {e}", "WARN")

    log(f"Step 8/{total_steps}: Processing stocks watch...")
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

    log(f"Step 9/{total_steps}: Collecting physical market prices...")
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

    log(f"Step 10/{total_steps}: Collecting international physical prices...")
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

    log(f"Step 11/{total_steps}: Generating daily reading...")
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

    log(f"Step 12/{total_steps}: Collecting BCB, IBGE, CONAB data...")
    try:
        from collect_new_sources import collect_bcb
        collect_bcb()
        log("BCB/SGS collected", "OK")
    except BaseException as e:
        log(f"BCB/SGS failed (non-blocking): {e}", "WARN")
    try:
        from collect_conab_ibge import main as collect_conab_auto
        collect_conab_auto()
        results["bcb_ibge"] = {"status": "OK", "sources": "BCB, IBGE, CONAB"}
        log("IBGE + CONAB collected", "OK")
    except BaseException as e:
        results["bcb_ibge"] = {"status": "WARN", "error": str(e)}
        log(f"IBGE/CONAB failed (non-blocking): {e}", "WARN")

    log(f"Step 13/{total_steps}: Collecting EIA energy data...")
    try:
        from collect_eia import main as collect_eia
        collect_eia()
        results["eia"] = {"status": "OK"}
        log("EIA energy data collected", "OK")
    except BaseException as e:
        results["eia"] = {"status": "WARN", "error": str(e)}
        log(f"EIA failed (non-blocking): {e}", "WARN")

    log(f"Step 14/{total_steps}: Collecting USDA FAS data...")
    try:
        from collect_usda_psd_csv import main as collect_fas
        collect_fas()
        results["usda_fas"] = {"status": "OK"}
        log("USDA FAS collected", "OK")
    except BaseException as e:
        results["usda_fas"] = {"status": "WARN", "error": str(e)}
        log(f"USDA FAS failed (non-blocking): {e}", "WARN")

    log(f"Step 15/{total_steps}: Collecting livestock PSD data...")
    try:
        from collect_livestock_psd import main as collect_livestock_psd
        collect_livestock_psd()
        results["livestock_psd"] = {"status": "OK"}
        log("Livestock PSD collected", "OK")
    except Exception as e:
        results["livestock_psd"] = {"status": "WARN", "error": str(e)}
        log(f"Livestock PSD failed (non-blocking): {e}", "WARN")

    log(f"Step 16/{total_steps}: Collecting livestock weekly indicators...")
    try:
        from collect_livestock_weekly import main as collect_livestock_weekly
        collect_livestock_weekly()
        results["livestock_weekly"] = {"status": "OK"}
        log("Livestock weekly collected", "OK")
    except Exception as e:
        results["livestock_weekly"] = {"status": "WARN", "error": str(e)}
        log(f"Livestock weekly failed (non-blocking): {e}", "WARN")

    log(f"Step 17/{total_steps}: Generating bilateral indicators...")
    try:
        from generate_bilateral import main as generate_bilateral
        generate_bilateral()
        results["bilateral"] = {"status": "OK"}
        log("Bilateral indicators generated", "OK")
    except Exception as e:
        results["bilateral"] = {"status": "WARN", "error": str(e)}
        log(f"Bilateral failed (non-blocking): {e}", "WARN")

    log(f"Step 18/{total_steps}: Collecting news & FRED macro...")
    try:
        from collect_news import main as collect_news
        collect_news()
        results["news"] = {"status": "OK"}
        log("News & FRED collected", "OK")
    except BaseException as e:
        results["news"] = {"status": "WARN", "error": str(e)}
        log(f"News failed (non-blocking): {e}", "WARN")

    log(f"Step 19/{total_steps}: Collecting agricultural weather...")
    try:
        from collect_weather import main as collect_weather
        collect_weather()
        results["weather"] = {"status": "OK"}
        log("Weather data collected", "OK")
    except BaseException as e:
        results["weather"] = {"status": "WARN", "error": str(e)}
        log(f"Weather failed (non-blocking): {e}", "WARN")

    log(f"Step 20/{total_steps}: Collecting USDA crop progress...")
    try:
        from collect_crop_progress import main as collect_crop_progress
        collect_crop_progress()
        results["crop_progress"] = {"status": "OK"}
        log("Crop progress collected", "OK")
    except BaseException as e:
        results["crop_progress"] = {"status": "WARN", "error": str(e)}
        log(f"Crop progress failed (non-blocking): {e}", "WARN")

    log(f"Step 20b/{total_steps}: Collecting export activity...")
    try:
        from collect_export_activity import main as collect_export_activity
        collect_export_activity()
        results["export_activity"] = {"status": "OK"}
        log("Export activity collected", "OK")
    except BaseException as e:
        results["export_activity"] = {"status": "WARN", "error": str(e)}
        log(f"Export activity failed (non-blocking): {e}", "WARN")

    log(f"Step 20c/{total_steps}: Collecting drought monitor...")
    try:
        from collect_drought_monitor import main as collect_drought_monitor
        collect_drought_monitor()
        results["drought_monitor"] = {"status": "OK"}
        log("Drought monitor collected", "OK")
    except BaseException as e:
        results["drought_monitor"] = {"status": "WARN", "error": str(e)}
        log(f"Drought monitor failed (non-blocking): {e}", "WARN")

    log(f"Step 20d/{total_steps}: Collecting fertilizer prices...")
    try:
        from collect_fertilizer import main as collect_fertilizer
        collect_fertilizer()
        results["fertilizer"] = {"status": "OK"}
        log("Fertilizer prices collected", "OK")
    except BaseException as e:
        results["fertilizer"] = {"status": "WARN", "error": str(e)}
        log(f"Fertilizer prices failed (non-blocking): {e}", "WARN")

    log(f"Step 21/{total_steps}: Collecting macro indicators (S&P500, VIX, 10Y)...")
    try:
        from collect_macro_indicators import main as collect_macro
        collect_macro()
        results["macro_indicators"] = {"status": "OK"}
        log("Macro indicators collected", "OK")
    except BaseException as e:
        results["macro_indicators"] = {"status": "WARN", "error": str(e)}
        log(f"Macro indicators failed (non-blocking): {e}", "WARN")

    log(f"Step 22/{total_steps}: Collecting Google Trends...")
    try:
        from collect_google_trends import main as collect_gtrends
        collect_gtrends()
        results["google_trends"] = {"status": "OK"}
        log("Google Trends collected", "OK")
    except BaseException as e:
        results["google_trends"] = {"status": "WARN", "error": str(e)}
        log(f"Google Trends failed (non-blocking): {e}", "WARN")

    log(f"Step 23/{total_steps}: Collecting FedWatch probabilities...")
    try:
        from collect_fedwatch import main as collect_fedwatch
        collect_fedwatch()
        results["fedwatch"] = {"status": "OK"}
        log("FedWatch collected", "OK")
    except BaseException as e:
        results["fedwatch"] = {"status": "WARN", "error": str(e)}
        log(f"FedWatch failed (non-blocking): {e}", "WARN")

    log(f"Step 24/{total_steps}: Computing correlation matrix & causal chains...")
    try:
        from collect_correlations import main as collect_correlations
        collect_correlations()
        results["correlations"] = {"status": "OK"}
        log("Correlations computed", "OK")
    except BaseException as e:
        results["correlations"] = {"status": "WARN", "error": str(e)}
        log(f"Correlations failed (non-blocking): {e}", "WARN")

    log(f"Step 25/{total_steps}: Collecting Grok emails...")
    try:
        from collect_grok_email import main as collect_grok
        collect_grok()
        results["grok_email"] = {"status": "OK"}
        log("Grok email collected", "OK")
    except BaseException as e:
        results["grok_email"] = {"status": "WARN", "error": str(e)}
        log(f"Grok email failed (non-blocking): {e}", "WARN")

    # =========================================================
    # GENERATION STEPS (20-25)
    # =========================================================

    log(f"Step 26/{total_steps}: Generating calendar...")
    try:
        from collect_calendar import main as generate_calendar
        generate_calendar()
        results["calendar"] = {"status": "OK"}
        log("Calendar generated", "OK")
    except Exception as e:
        results["calendar"] = {"status": "WARN", "error": str(e)}
        log(f"Calendar failed (non-blocking): {e}", "WARN")

    log(f"Step 27/{total_steps}: Generating daily report...")
    try:
        from generate_report import main as generate_report
        generate_report()
        results["report"] = {"status": "OK"}
        log("Daily report generated", "OK")
    except Exception as e:
        results["report"] = {"status": "ERROR", "error": str(e)}
        log(f"Report generation failed: {e}", "ERR")


    # -- Grain Ratios (automatico) ---
    try:
        import subprocess as _sp
        _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        _r1 = _sp.run([sys.executable, os.path.join(_root,"grain_ratio_engine.py")], cwd=_root)
        _r2 = _sp.run([sys.executable, os.path.join(_root,"grain_ratios_enrich.py")], cwd=_root)
        print("    grain_ratios OK" if _r1.returncode==0 and _r2.returncode==0 else "    grain_ratios WARN")
    except Exception as _e: print(f"    grain_ratios ERR: {_e}")
    # ------------------------------------

    log(f"Step 28/{total_steps}: Generating intel synthesis...")
    try:
        from generate_intel_synthesis import main as generate_synthesis
        generate_synthesis()
        results["intel_synthesis"] = {"status": "OK"}
        log("Intel synthesis generated", "OK")
    except BaseException as e:
        results["intel_synthesis"] = {"status": "WARN", "error": str(e)}
        log(f"Intel synthesis failed (non-blocking): {e}", "WARN")

    log(f"Step 28b/{total_steps}: Running intelligence engine (daily frame)...")
    try:
        from intelligence_engine import main as run_intelligence_engine
        run_intelligence_engine()
        results["intelligence_frame"] = {"status": "OK"}
        log("Intelligence frame generated", "OK")
    except BaseException as e:
        results["intelligence_frame"] = {"status": "WARN", "error": str(e)}
        log(f"Intelligence engine failed (non-blocking): {e}", "WARN")

    log(f"Step 28c/{total_steps}: Running entry timing scan...")
    try:
        from skill_entry_timing import main as run_entry_scan
        run_entry_scan()
        results["entry_timing"] = {"status": "OK"}
        log("Entry timing scan complete", "OK")
    except BaseException as e:
        results["entry_timing"] = {"status": "WARN", "error": str(e)}
        log(f"Entry timing scan failed (non-blocking): {e}", "WARN")

    log(f"Step 28d/{total_steps}: Running theta calendar...")
    try:
        from skill_theta_calendar import run_theta_calendar
        run_theta_calendar()
        results["theta_calendar"] = {"status": "OK"}
        log("Theta calendar generated", "OK")
    except BaseException as e:
        results["theta_calendar"] = {"status": "WARN", "error": str(e)}
        log(f"Theta calendar failed (non-blocking): {e}", "WARN")

    log(f"Step 29/{total_steps}: Generating PDF report (v4 with Options Intelligence)...")
    try:
        from patch_report_v4 import build_pdf_v4
        build_pdf_v4()
        results["pdf"] = {"status": "OK"}
        log("PDF v4 report generated (with Options Intelligence + Track Record)", "OK")
    except Exception as e:
        results["pdf"] = {"status": "WARN", "error": str(e)}
        log(f"PDF v4 generation failed (non-blocking): {e}", "WARN")

    log(f"Step 30/{total_steps}: Generating video script...")
    try:
        from generate_video_script import main as generate_video
        generate_video()
        results["video_script"] = {"status": "OK"}
        log("Video script generated", "OK")
    except Exception as e:
        results["video_script"] = {"status": "ERROR", "error": str(e)}
        log(f"Video script failed: {e}", "ERR")

    log(f"Step 31/{total_steps}: Generating video MP4...")
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
