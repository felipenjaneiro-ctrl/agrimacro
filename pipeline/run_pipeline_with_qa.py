#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════
 run_pipeline_with_qa.py — Pipeline completo com AA+QA Gate
═══════════════════════════════════════════════════════════════

 Substitui a sequência manual de comandos.
 Executa todos os coletores + QA + PDF em ordem.

 Uso:
   python run_pipeline_with_qa.py           # Normal
   python run_pipeline_with_qa.py --force   # Ignora BLOCKs do QA
   python run_pipeline_with_qa.py --skip-collect  # Só QA + PDF
   python run_pipeline_with_qa.py --qa-only # Só roda QA, sem gerar PDF
═══════════════════════════════════════════════════════════════
"""

import subprocess, sys, time, os
from pathlib import Path
from datetime import datetime

PIPELINE_DIR = Path(__file__).parent

# Ordem dos coletores
COLLECTORS = [
    "collect_prices.py",
    "collect_physical_intl.py",
    "collect_eia.py",
    "collect_bcb.py",
    "collect_cot.py",
    "collect_stocks.py",
    "collect_weather.py",
    "collect_news.py",
    "collect_calendar.py",
    "calculate_spreads.py",
    "generate_daily_reading.py",
]

def run_step(script, label=None):
    """Executa um script Python e retorna sucesso/falha."""
    path = PIPELINE_DIR / script
    if not path.exists():
        print(f"  ⚠️  {script} não encontrado — pulando")
        return True  # Não bloqueia pipeline

    label = label or script
    print(f"\n  ▶ {label}...", end=" ", flush=True)
    start = time.time()

    result = subprocess.run(
        [sys.executable, str(path)],
        capture_output=True, text=True,
        cwd=str(PIPELINE_DIR),
        timeout=300  # 5 min max por coletor
    )

    elapsed = time.time() - start

    if result.returncode == 0:
        print(f"✅ ({elapsed:.1f}s)")
        return True
    else:
        print(f"❌ ({elapsed:.1f}s)")
        # Mostra últimas linhas do erro
        err = result.stderr.strip() or result.stdout.strip()
        if err:
            for line in err.split("\n")[-5:]:
                print(f"    {line}")
        return False


def run_qa(force=False):
    """Executa AA+QA Engine."""
    print("\n" + "=" * 60)
    print("  GATE 3.5 — AA+QA Engine")
    print("=" * 60)

    qa_path = PIPELINE_DIR / "aa_qa_engine.py"
    if not qa_path.exists():
        print("  ⚠️  aa_qa_engine.py não encontrado — QA desabilitado")
        return True

    args = [sys.executable, str(qa_path)]
    if force:
        args.append("--force")

    result = subprocess.run(
        args, capture_output=False, text=True,
        cwd=str(PIPELINE_DIR)
    )

    if result.returncode != 0 and not force:
        return False
    return True


def main():
    force = "--force" in sys.argv
    skip_collect = "--skip-collect" in sys.argv
    qa_only = "--qa-only" in sys.argv

    print("═" * 60)
    print(f"  AgriMacro Pipeline + QA — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("═" * 60)

    # ── GATE 2: Coleta ──
    if not skip_collect and not qa_only:
        print("\n  ── GATE 2: Coleta de Dados ──")
        failed = []
        for script in COLLECTORS:
            try:
                ok = run_step(script)
                if not ok:
                    failed.append(script)
            except subprocess.TimeoutExpired:
                print(f"  ⏰ TIMEOUT: {script}")
                failed.append(script)
            except Exception as e:
                print(f"  ❌ ERRO: {script} — {e}")
                failed.append(script)

        if failed:
            print(f"\n  ⚠️  {len(failed)} coletor(es) falharam: {', '.join(failed)}")
            print("  Continuando com dados disponíveis...")

    # ── GATE 3.5: AA+QA ──
    qa_ok = run_qa(force=force)

    if not qa_ok:
        print("\n  🛑 Pipeline interrompido pelo QA Engine")
        print("  Use --force para ignorar bloqueios")
        sys.exit(1)

    if qa_only:
        print("\n  ✅ Auditoria concluída (--qa-only)")
        sys.exit(0)

    # ── GATE 4: Geração do PDF ──
    print("\n  ── GATE 4: Geração do Relatório ──")
    pdf_args = [sys.executable, str(PIPELINE_DIR / "generate_report_pdf.py")]
    if force:
        pdf_args.append("--force")

    ok = run_step("generate_report_pdf.py", "Gerando PDF")

    if ok:
        print("\n" + "═" * 60)
        print("  ✅ Pipeline completo com sucesso!")
        print("═" * 60)
    else:
        print("\n  ❌ Falha na geração do PDF")
        sys.exit(1)


if __name__ == "__main__":
    main()
