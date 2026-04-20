import { NextResponse } from "next/server";
import { execSync } from "child_process";

export async function POST() {
  try {
    const output = execSync("cd /var/www/agrimacro && git pull origin main 2>&1", {
      encoding: "utf-8",
      timeout: 30_000,
    });
    return NextResponse.json({
      status: "ok",
      output: output.trim(),
      synced_at: new Date().toISOString(),
    });
  } catch (err: any) {
    return NextResponse.json(
      { status: "error", error: (err.message || "unknown").slice(0, 500) },
      { status: 500 }
    );
  }
}
