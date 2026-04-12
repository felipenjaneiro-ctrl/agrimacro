import { NextResponse } from "next/server";
import { execSync } from "child_process";
import { join } from "path";
import { existsSync } from "fs";

let lastRefresh: number = 0;
const MIN_INTERVAL = 120_000; // min 2 min between runs

const STEPS = [
  "collect_cepea.py",
  "collect_psd_stocks.py",
  "collect_eia.py",
  "process_stocks.py",
  "generate_reading.py",
  "generate_report.py",
  "aa_headline_parity.py",
  "confidence_score.py",
  "qa_daily_report.py",
  "generate_report_pdf.py",
];

export async function POST() {
  const now = Date.now();
  if (now - lastRefresh < MIN_INTERVAL) {
    return NextResponse.json({ status: "skipped", reason: "too_soon" });
  }
  const pipelinePath = join(process.cwd(), "..", "pipeline");
  const results: { step: string; ok: boolean; time: number; error?: string }[] = [];
  let failed = false;

  for (const step of STEPS) {
    const script = join(pipelinePath, step);
    if (!existsSync(script)) {
      results.push({ step, ok: false, time: 0, error: "not found" });
      continue;
    }
    const t0 = Date.now();
    try {
      execSync(`python "${script}"`, {
        cwd: pipelinePath,
        timeout: 300_000,
        encoding: "utf-8",
        stdio: ["pipe", "pipe", "pipe"],
      });
      results.push({ step, ok: true, time: Date.now() - t0 });
    } catch (err: any) {
      results.push({ step, ok: false, time: Date.now() - t0, error: err.message?.slice(0, 200) });
      if (step === "generate_report_pdf.py") failed = true;
    }
  }

  lastRefresh = now;
  const ok = results.filter(r => r.ok).length;
  const total = results.length;
  const totalTime = results.reduce((s, r) => s + r.time, 0);

  return NextResponse.json({
    status: failed ? "error" : "ok",
    timestamp: new Date().toISOString(),
    summary: `${ok}/${total} steps ok`,
    totalTime,
    results,
  });
}

export async function GET() {
  return NextResponse.json({ lastRefresh, uptime: Date.now() - lastRefresh });
}
