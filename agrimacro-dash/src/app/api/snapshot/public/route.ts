import { NextRequest, NextResponse } from "next/server";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

const ALLOWED_ORIGINS = [
  "http://localhost:3000",
  "http://127.0.0.1:3000",
];

// Files explicitly BLOCKED — never include portfolio/account data
const BLOCKED_FILES = new Set([
  "ibkr_portfolio.json",
  "ibkr_export.json",
]);

// Market data files to include
const MARKET_FILES: Record<string, string> = {
  prices: "price_history.json",
  futures_contracts: "futures_contracts.json",
  cot: "cot.json",
  spreads: "spreads.json",
  correlations: "correlations.json",
  intel_synthesis: "intel_synthesis.json",
  macro_indicators: "macro_indicators.json",
  fedwatch: "fedwatch.json",
  bcb: "bcb_data.json",
  psd_ending_stocks: "psd_ending_stocks.json",
  crop_progress: "crop_progress.json",
  grain_ratios: "grain_ratios.json",
  weather: "weather_agro.json",
  google_trends: "google_trends.json",
  grok_sentiment: "grok_sentiment.json",
  grok_news: "grok_news.json",
  grok_macro: "grok_macro.json",
  usda_gtr: "usda_gtr.json",
  physical_br: "physical_br.json",
  physical_intl: "physical_intl.json",
  eia_data: "eia_data.json",
  seasonality: "seasonality.json",
  conab: "conab_data.json",
  news: "news.json",
};

function loadJSON(name: string): any {
  // Double-check: never load blocked files regardless of how we got here
  if (BLOCKED_FILES.has(name)) return null;

  const processed = join(process.cwd(), "public", "data", "processed", name);
  if (existsSync(processed)) {
    try { return JSON.parse(readFileSync(processed, "utf-8")); } catch { /* skip */ }
  }
  // Fallback to raw dir for price_history
  const raw = join(process.cwd(), "public", "data", "raw", name);
  if (existsSync(raw)) {
    try { return JSON.parse(readFileSync(raw, "utf-8")); } catch { /* skip */ }
  }
  return null;
}

function corsHeaders(origin: string | null): Record<string, string> {
  const headers: Record<string, string> = {
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "X-Snapshot-Token, Content-Type",
    "Access-Control-Max-Age": "86400",
  };
  if (origin && ALLOWED_ORIGINS.includes(origin)) {
    headers["Access-Control-Allow-Origin"] = origin;
  }
  return headers;
}

export async function OPTIONS(req: NextRequest) {
  const origin = req.headers.get("origin");
  return new NextResponse(null, { status: 204, headers: corsHeaders(origin) });
}

export async function GET(req: NextRequest) {
  const origin = req.headers.get("origin");
  const cors = corsHeaders(origin);

  // Token validation
  const expectedToken = process.env.SNAPSHOT_TOKEN;
  if (!expectedToken) {
    return NextResponse.json(
      { error: "SNAPSHOT_TOKEN not configured on server" },
      { status: 500, headers: cors }
    );
  }

  const providedToken = req.headers.get("x-snapshot-token");
  if (!providedToken || providedToken !== expectedToken) {
    return NextResponse.json(
      { error: "Unauthorized — invalid or missing X-Snapshot-Token" },
      { status: 401, headers: cors }
    );
  }

  // Load all market data
  const data: Record<string, any> = {};
  const loaded: string[] = [];
  const missing: string[] = [];

  for (const [key, filename] of Object.entries(MARKET_FILES)) {
    const json = loadJSON(filename);
    if (json !== null) {
      data[key] = json;
      loaded.push(key);
    } else {
      missing.push(key);
    }
  }

  // Final safety check — strip any field that looks like portfolio data
  delete data["ibkr_portfolio"];
  delete data["ibkr_export"];

  const response = {
    generated_at: new Date().toISOString(),
    source: "AgriMacro Intelligence — Market Data Only",
    privacy_note: "Portfolio data excluded",
    stats: {
      loaded: loaded.length,
      missing: missing.length,
      missing_keys: missing,
    },
    data,
  };

  return NextResponse.json(response, { headers: cors });
}
