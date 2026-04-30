import { NextResponse } from "next/server";
import { execSync } from "child_process";

export async function POST() {
  let gitOutput = "";
  let freshnessOutput = "";
  let freshnessError: string | null = null;

  // STEP 1: git pull (obrigatorio - falha aqui aborta endpoint com 500)
  try {
    gitOutput = execSync("cd /var/www/agrimacro && git pull origin main 2>&1", {
      encoding: "utf-8",
      timeout: 30_000,
    });
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : "unknown";
    return NextResponse.json(
      { status: "error", stage: "git_pull", error: msg.slice(0, 500) },
      { status: 500 }
    );
  }

  // STEP 2: regenerar data_freshness.json (best-effort - nao aborta sync)
  // SCP do PC atualiza mtime de ibkr_portfolio.json (sync marker), mas
  // data_freshness.json so e regenerado pelo cron VPS as 09:15 ET. Sem isso,
  // banner permanece STALE apos sync_portfolio.ps1 ate o proximo cron.
  try {
    freshnessOutput = execSync(
      "cd /var/www/agrimacro && python3 pipeline/check_data_freshness.py 2>&1",
      { encoding: "utf-8", timeout: 10_000 }
    ).trim();
  } catch (err: unknown) {
    freshnessError = (err instanceof Error ? err.message : "unknown").slice(0, 500);
  }

  return NextResponse.json({
    status: "ok",
    git_output: gitOutput.trim(),
    freshness_output: freshnessOutput,
    freshness_error: freshnessError,
    synced_at: new Date().toISOString(),
  });
}
