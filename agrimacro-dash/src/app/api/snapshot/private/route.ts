import { NextRequest, NextResponse } from "next/server";
import { readFileSync, existsSync, appendFileSync, mkdirSync } from "fs";
import { join } from "path";

// --------------- Rate limiter (in-memory, per-process) ---------------
const accessLog: number[] = [];
const RATE_LIMIT = 10;
const RATE_WINDOW_MS = 60 * 60 * 1000; // 1 hour

function checkRateLimit(): boolean {
  const now = Date.now();
  // Prune old entries
  while (accessLog.length > 0 && accessLog[0] < now - RATE_WINDOW_MS) {
    accessLog.shift();
  }
  if (accessLog.length >= RATE_LIMIT) return false;
  accessLog.push(now);
  return true;
}

// --------------- Access logging ---------------
function logAccess(ip: string, status: number) {
  const logsDir = join(process.cwd(), "logs");
  if (!existsSync(logsDir)) {
    try { mkdirSync(logsDir, { recursive: true }); } catch { /* ignore */ }
  }
  const logFile = join(logsDir, "snapshot_access.log");
  const entry = `${new Date().toISOString()} | endpoint=private | ip=${ip} | status=${status}\n`;
  try { appendFileSync(logFile, entry); } catch { /* ignore */ }
}

// --------------- CORS ---------------
const ALLOWED_ORIGINS = ["http://localhost:3000", "http://127.0.0.1:3000"];

function corsHeaders(origin: string | null): Record<string, string> {
  const headers: Record<string, string> = {
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "X-Snapshot-Token, Content-Type",
    "Access-Control-Max-Age": "86400",
    "X-Data-Classification": "confidential-masked",
  };
  if (origin && ALLOWED_ORIGINS.includes(origin)) {
    headers["Access-Control-Allow-Origin"] = origin;
  }
  return headers;
}

// --------------- JSON loaders ---------------
function loadJSON(name: string): any {
  const processed = join(process.cwd(), "public", "data", "processed", name);
  if (existsSync(processed)) {
    try { return JSON.parse(readFileSync(processed, "utf-8")); } catch { /* skip */ }
  }
  const raw = join(process.cwd(), "public", "data", "raw", name);
  if (existsSync(raw)) {
    try { return JSON.parse(readFileSync(raw, "utf-8")); } catch { /* skip */ }
  }
  return null;
}

// --------------- Portfolio masking ---------------
function toRange(val: number): string {
  const abs = Math.abs(val);
  if (abs < 1000) return val < 0 ? "-<$1K" : "<$1K";
  if (abs < 5000) return val < 0 ? "-$1K-$5K" : "$1K-$5K";
  if (abs < 10000) return val < 0 ? "-$5K-$10K" : "$5K-$10K";
  if (abs < 25000) return val < 0 ? "-$10K-$25K" : "$10K-$25K";
  if (abs < 50000) return val < 0 ? "-$25K-$50K" : "$25K-$50K";
  if (abs < 100000) return val < 0 ? "-$50K-$100K" : "$50K-$100K";
  if (abs < 250000) return val < 0 ? "-$100K-$250K" : "$100K-$250K";
  if (abs < 500000) return val < 0 ? "-$250K-$500K" : "$250K-$500K";
  if (abs < 1000000) return val < 0 ? "-$500K-$1M" : "$500K-$1M";
  if (abs < 5000000) return val < 0 ? "-$1M-$5M" : "$1M-$5M";
  return val < 0 ? "-$5M+" : "$5M+";
}

function maskPortfolio(raw: any): any {
  if (!raw) return null;

  const masked: any = {
    generated_at: raw.generated_at,
    privacy_mode: "portfolio_masked",
    position_count: raw.position_count || raw.positions?.length || 0,
  };

  // Summary: replace exact values with ranges
  if (raw.summary) {
    masked.summary_ranges = {};
    for (const [key, val] of Object.entries(raw.summary)) {
      const num = parseFloat(val as string);
      if (!isNaN(num)) {
        masked.summary_ranges[key] = toRange(num);
      }
    }
  }

  // Positions: keep symbol, direction, quantity, sec_type; mask USD values
  if (raw.positions && Array.isArray(raw.positions)) {
    masked.positions = raw.positions.map((p: any) => {
      const pos = p.position ?? p.quantity ?? 0;
      const direction = pos > 0 ? "LONG" : pos < 0 ? "SHORT" : "FLAT";
      const mv = p.market_value ?? p.marketValue ?? 0;
      const avg = p.avg_cost ?? p.avgCost ?? 0;
      const pnlPct = avg !== 0 ? ((mv / Math.abs(pos) - avg) / avg * 100) : null;

      return {
        symbol: p.symbol,
        local_symbol: p.local_symbol,
        sec_type: p.sec_type,
        direction,
        quantity: Math.abs(pos),
        market_value_range: toRange(mv),
        pnl_pct: pnlPct !== null ? Number(pnlPct.toFixed(2)) : null,
      };
    });
  }

  // Explicitly strip sensitive fields that might exist in enriched format
  delete masked.account;
  delete masked.account_id;
  delete masked.account_number;

  return masked;
}

// --------------- Market data files (same as public) ---------------
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

// --------------- Route handlers ---------------
export async function OPTIONS(req: NextRequest) {
  const origin = req.headers.get("origin");
  return new NextResponse(null, { status: 204, headers: corsHeaders(origin) });
}

export async function GET(req: NextRequest) {
  const origin = req.headers.get("origin");
  const cors = corsHeaders(origin);
  const ip = req.headers.get("x-forwarded-for") || req.headers.get("x-real-ip") || "unknown";

  // Token validation — must use PRIVATE token, public token must NOT work
  const expectedToken = process.env.SNAPSHOT_PRIVATE_TOKEN;
  if (!expectedToken) {
    logAccess(ip, 500);
    return NextResponse.json(
      { error: "SNAPSHOT_PRIVATE_TOKEN not configured on server" },
      { status: 500, headers: cors }
    );
  }

  const providedToken = req.headers.get("x-snapshot-token");
  if (!providedToken || providedToken !== expectedToken) {
    logAccess(ip, 401);
    return NextResponse.json(
      { error: "Unauthorized — invalid or missing X-Snapshot-Token" },
      { status: 401, headers: cors }
    );
  }

  // Rate limiting
  if (!checkRateLimit()) {
    logAccess(ip, 429);
    return NextResponse.json(
      { error: "Rate limit exceeded — max 10 requests per hour" },
      { status: 429, headers: { ...cors, "Retry-After": "3600" } }
    );
  }

  // Load market data
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

  // Load and mask portfolio
  const rawPortfolio = loadJSON("ibkr_portfolio.json");
  if (rawPortfolio && !rawPortfolio.is_fallback) {
    data.portfolio_masked = maskPortfolio(rawPortfolio);
    loaded.push("portfolio_masked");
  } else {
    missing.push("portfolio_masked");
  }

  logAccess(ip, 200);

  const response = {
    generated_at: new Date().toISOString(),
    source: "AgriMacro Intelligence — Market Data + Masked Portfolio",
    privacy_mode: "portfolio_masked",
    privacy_note: "Portfolio included with masked absolute values. No account IDs or exact USD amounts.",
    stats: {
      loaded: loaded.length,
      missing: missing.length,
      missing_keys: missing,
    },
    data,
  };

  return NextResponse.json(response, { headers: cors });
}
