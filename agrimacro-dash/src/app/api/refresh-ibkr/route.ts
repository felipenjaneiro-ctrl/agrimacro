import { NextResponse } from "next/server";
import { execSync } from "child_process";
import { existsSync, readFileSync } from "fs";
import { join } from "path";

let lastRefresh: number = 0;
const MIN_INTERVAL = 60_000; // minimum 1 min between refreshes

export async function POST() {
  const now = Date.now();
  if (now - lastRefresh < MIN_INTERVAL) {
    return NextResponse.json({ status: "skipped", reason: "too_soon", lastRefresh });
  }
  try {
    const pipelinePath = join(process.cwd(), "..", "pipeline");
    const script = join(pipelinePath, "collect_ibkr.py");
    if (!existsSync(script)) {
      return NextResponse.json({ status: "error", error: "collect_ibkr.py not found" }, { status: 500 });
    }
    execSync(`python "${script}"`, {
      cwd: pipelinePath,
      timeout: 120_000,
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"]
    });
    lastRefresh = now;
    // Reload portfolio JSON to confirm
    const portfolioPath = join(process.cwd(), "public", "data", "processed", "ibkr_portfolio.json");
    let positions = 0;
    if (existsSync(portfolioPath)) {
      const data = JSON.parse(readFileSync(portfolioPath, "utf-8"));
      positions = data.positions?.length || 0;
    }
    return NextResponse.json({ status: "ok", timestamp: new Date().toISOString(), positions });
  } catch (err: any) {
    return NextResponse.json({ status: "error", error: err.message?.slice(0, 500) }, { status: 500 });
  }
}

export async function GET() {
  return NextResponse.json({ lastRefresh, uptime: Date.now() - lastRefresh });
}