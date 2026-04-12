import { NextRequest, NextResponse } from "next/server";
import Anthropic from "@anthropic-ai/sdk";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

const KEY_PATH = process.env.ANTHROPIC_API_KEY_FILE || join(process.env.HOME || process.env.USERPROFILE || "", ".anthropic_key");

function getKey(): string {
  if (process.env.ANTHROPIC_API_KEY) return process.env.ANTHROPIC_API_KEY;
  if (existsSync(KEY_PATH)) return readFileSync(KEY_PATH, "utf-8").trim();
  throw new Error("No API key found");
}

function loadJSON(name: string): any {
  const p = join(process.cwd(), "public", "data", "processed", name);
  if (!existsSync(p)) return null;
  try { return JSON.parse(readFileSync(p, "utf-8")); } catch { return null; }
}

function buildContext(): string {
  const portfolio = loadJSON("ibkr_portfolio.json");
  const stocks = loadJSON("stocks_watch.json");
  const reading = loadJSON("daily_reading.json");
  const spreads = loadJSON("spreads.json");

  let ctx = "You are an expert commodities trading analyst embedded in the AgriMacro dashboard. You have access to real market data.\n\n";

  if (portfolio) {
    ctx += "=== IBKR PORTFOLIO ===\n";
    ctx += "Account: " + (portfolio.account || "") + "\n";
    if (portfolio.summary) {
      ctx += "Net Liquidation: $" + (portfolio.summary.NetLiquidation || "?") + "\n";
      ctx += "Buying Power: $" + (portfolio.summary.BuyingPower || "?") + "\n";
      ctx += "Unrealized PnL: $" + (portfolio.summary.UnrealizedPnL || "?") + "\n";
      ctx += "Cash: $" + (portfolio.summary.TotalCashValue || "?") + "\n";
    }
    if (portfolio.positions) {
      ctx += "Positions (" + portfolio.positions.length + "):\n";
      portfolio.positions.forEach((p: any) => {
        ctx += "  " + p.symbol + " (" + p.local_symbol + "): " + p.position + " contracts @ " + p.avg_cost + "\n";
      });
    }
    ctx += "\n";
  }

  if (stocks?.commodities) {
    ctx += "=== STOCKS WATCH ===\n";
    Object.entries(stocks.commodities).forEach(([sym, data]: [string, any]) => {
      if (data.stock_current) {
        ctx += sym + ": " + data.stock_current + " " + (data.stock_unit||"") + " | State: " + (data.state||"?") + " | Deviation: " + ((data.stock_avg ? ((data.stock_current-data.stock_avg)/data.stock_avg*100).toFixed(1) : "?")) + "%\n";
      }
    });
    ctx += "\n";
  }

  if (reading) {
    ctx += "=== DAILY READING ===\n";
    ctx += JSON.stringify(reading).slice(0, 2000) + "\n\n";
  }

  if (spreads) {
    ctx += "=== SPREADS ===\n";
    ctx += JSON.stringify(spreads).slice(0, 1500) + "\n\n";
  }

  const cotData = loadJSON("cot.json");
  if (cotData?.commodities) {
    const extremes: string[] = [];
    for (const [sym, v] of Object.entries(cotData.commodities) as any[]) {
      const leg = v?.legacy;
      if (!leg?.latest || !leg?.history?.length) continue;
      const nets = leg.history.map((h: any) => h.noncomm_net).filter((n: any) => n != null);
      if (nets.length < 20) continue;
      const mn = Math.min(...nets), mx = Math.max(...nets);
      if (mx === mn) continue;
      const cotIndex = ((leg.latest.noncomm_net - mn) / (mx - mn)) * 100;
      if (cotIndex > 80 || cotIndex < 20) {
        extremes.push(`${sym}: net=${leg.latest.noncomm_net} | cot_index=${cotIndex.toFixed(0)}/100 | ${cotIndex > 80 ? "CROWDED LONG" : "CROWDED SHORT"}`);
      }
    }
    if (extremes.length) {
      ctx += "=== COT EXTREMOS ===\n";
      ctx += extremes.join("\n") + "\n\n";
    }
  }

  ctx += "Responda em Português Brasileiro. Seja direto e acionável. Foque em implicações práticas para o portfólio atual. Máximo 800 palavras. Use marcadores quando listar itens.\n";
  return ctx;
}

export async function POST(req: NextRequest) {
  try {
    const { messages } = await req.json();
    const client = new Anthropic({ apiKey: getKey() });
    const systemPrompt = buildContext();

    const response = await client.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 4096,
      system: systemPrompt,
      messages: messages,
    });

    const text = response.content
      .filter((c: any) => c.type === "text")
      .map((c: any) => c.text)
      .join("");

    return NextResponse.json({ response: text });
  } catch (error: any) {
    return NextResponse.json({ error: error.message || "Unknown error" }, { status: 500 });
  }
}