import { NextRequest, NextResponse } from "next/server";
import { execSync } from "child_process";
import { existsSync, readFileSync } from "fs";
import { join } from "path";

const PIPELINE = join(process.cwd(), "..", "pipeline");

const SKILL_MAP: Record<string, { script: string; output?: string; args?: string }> = {
  entry_timing:     { script: "skill_entry_timing.py",        output: "entry_timing.json" },
  pretrade:         { script: "skill_pretrade_checklist.py",   output: null as any },
  position_sizing:  { script: "skill_position_sizing.py",     output: "position_sizing.json" },
  theta_calendar:   { script: "skill_theta_calendar.py",      output: "theta_calendar.json" },
  opportunity_scan: { script: "skill_opportunity_scanner.py",  output: "opportunity_scan.json" },
  stress_test:      { script: "skill_stress_test.py",         output: "stress_test.json" },
  vega_monitor:     { script: "skill_vega_monitor.py",        output: "vega_monitor.json" },
  roll_decision:    { script: "skill_roll_decision.py",       output: "roll_decisions.json", args: "portfolio" },
  commodity_dna:    { script: "commodity_dna.py",              output: "commodity_dna.json" },
  trade_journal:    { script: "skill_trade_journal.py",       output: "trade_journal.json", args: "show" },
};

export async function POST(req: NextRequest) {
  try {
    const { skill, args: extraArgs } = await req.json();

    if (!skill || !SKILL_MAP[skill]) {
      return NextResponse.json({
        error: `Unknown skill: ${skill}. Available: ${Object.keys(SKILL_MAP).join(", ")}`,
      }, { status: 400 });
    }

    const config = SKILL_MAP[skill];
    const scriptPath = join(PIPELINE, config.script);

    if (!existsSync(scriptPath)) {
      return NextResponse.json({ error: `Script not found: ${config.script}` }, { status: 500 });
    }

    const cmdArgs = extraArgs || config.args || "";
    const cmd = `python "${scriptPath}" ${cmdArgs}`;

    const stdout = execSync(cmd, {
      cwd: PIPELINE,
      timeout: 60_000,
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
    });

    // Try to load JSON output
    let data = null;
    if (config.output) {
      const outPath = join(PIPELINE, config.output);
      if (existsSync(outPath)) {
        try { data = JSON.parse(readFileSync(outPath, "utf-8")); } catch {}
      }
    }

    return NextResponse.json({
      skill,
      status: "ok",
      stdout: stdout.slice(0, 5000),
      data,
      timestamp: new Date().toISOString(),
    });
  } catch (error: any) {
    const stderr = error.stderr?.slice(0, 1000) || "";
    return NextResponse.json({
      error: error.message?.slice(0, 500) || "Unknown error",
      stderr,
    }, { status: 500 });
  }
}

export async function GET() {
  return NextResponse.json({
    available_skills: Object.keys(SKILL_MAP),
    descriptions: {
      entry_timing: "Scan all underlyings for entry opportunities",
      pretrade: "10-filter GO/NO-GO checklist (pass sym dir as args)",
      position_sizing: "Calculate position size (pass sym dir score as args)",
      theta_calendar: "Position timeline with DTE phases",
      opportunity_scan: "Consolidated daily ranking",
      stress_test: "Portfolio stress scenarios",
      vega_monitor: "Strategic reserve deploy scanner",
      roll_decision: "Portfolio roll/close decisions",
      commodity_dna: "Regenerate commodity DNA profiles",
      trade_journal: "Show trade journal entries",
    },
  });
}
