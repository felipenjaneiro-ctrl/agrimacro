import { NextRequest, NextResponse } from "next/server";
import { execSync } from "child_process";
import { existsSync } from "fs";
import { join } from "path";

const SCRIPT = join(process.cwd(), "..", "pipeline", "ibkr_orders.py");

export async function POST(req: NextRequest) {
  try {
    const payload = await req.json();
    const mode = payload.mode || "validate"; // "validate" | "execute"

    if (!["validate", "execute"].includes(mode)) {
      return NextResponse.json({ status: "error", error: "mode must be 'validate' or 'execute'" }, { status: 400 });
    }

    if (!existsSync(SCRIPT)) {
      return NextResponse.json({ status: "error", error: "ibkr_orders.py not found" }, { status: 500 });
    }

    const flag = mode === "execute" ? "--execute" : "--validate";
    const input = JSON.stringify(payload);

    const output = execSync(
      `echo '${input.replace(/'/g, "'\\''")}' | python "${SCRIPT}" ${flag}`,
      {
        cwd: join(process.cwd(), "..", "pipeline"),
        timeout: 30_000,
        encoding: "utf-8",
        shell: "bash",
        stdio: ["pipe", "pipe", "pipe"],
      }
    );

    const result = JSON.parse(output.trim());
    return NextResponse.json(result);
  } catch (err: any) {
    const stderr = err.stderr || "";
    const msg = err.message || "Unknown error";

    if (msg.includes("ECONNREFUSED") || msg.includes("timeout") || stderr.includes("not connected")) {
      return NextResponse.json({
        status: "error",
        error: "IBKR not connected. Open TWS or IB Gateway.",
      });
    }

    return NextResponse.json({ status: "error", error: msg.slice(0, 500) }, { status: 500 });
  }
}

export async function GET() {
  // Quick connection check
  try {
    if (!existsSync(SCRIPT)) {
      return NextResponse.json({ status: "error", error: "ibkr_orders.py not found" });
    }

    const output = execSync(`python "${SCRIPT}" --check`, {
      cwd: join(process.cwd(), "..", "pipeline"),
      timeout: 10_000,
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
    });

    return NextResponse.json(JSON.parse(output.trim()));
  } catch {
    return NextResponse.json({ status: "disconnected" });
  }
}
