#!/usr/bin/env python3
"""
sync_morning_intel.py - AgriMacro Morning Sync (Scheduled Task)
================================================================
Runs daily at 9:25 AM via Windows Task Scheduler.
Collects Grok emails + refreshes intel synthesis.

Steps:
  1. Collect Grok Tasks emails from Gmail (if OAuth configured)
  2. Collect macro indicators (S&P, VIX, 10Y)
  3. Collect FedWatch probabilities
  4. Recompute correlations
  5. Regenerate intel synthesis
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")

def run_step(name, func):
    try:
        log(f"  {name}...")
        func()
        log(f"  {name} OK")
        return True
    except Exception as e:
        log(f"  {name} WARN: {e}")
        return False

def main():
    log("AgriMacro Morning Sync starting...")
    start = time.time()
    results = {}

    # 1. Grok emails
    def step_grok():
        from collect_grok_email import main as collect_grok
        collect_grok()
    results["grok"] = run_step("Grok email", step_grok)

    # 2. Macro indicators
    def step_macro():
        from collect_macro_indicators import main as collect_macro
        collect_macro()
    results["macro"] = run_step("Macro indicators", step_macro)

    # 3. FedWatch
    def step_fedwatch():
        from collect_fedwatch import main as collect_fedwatch
        collect_fedwatch()
    results["fedwatch"] = run_step("FedWatch", step_fedwatch)

    # 4. Correlations
    def step_corr():
        from collect_correlations import main as collect_corr
        collect_corr()
    results["correlations"] = run_step("Correlations", step_corr)

    # 5. Intel synthesis
    def step_synth():
        from generate_intel_synthesis import main as gen_synth
        gen_synth()
    results["synthesis"] = run_step("Intel synthesis", step_synth)

    elapsed = time.time() - start
    ok = sum(1 for v in results.values() if v)
    log(f"Morning sync done: {ok}/{len(results)} OK in {elapsed:.1f}s")

if __name__ == "__main__":
    main()
