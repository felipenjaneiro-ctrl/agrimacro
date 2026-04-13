import { NextRequest, NextResponse } from "next/server";
import Anthropic from "@anthropic-ai/sdk";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

const KEY_PATH =
  process.env.ANTHROPIC_API_KEY_FILE ||
  join(process.env.HOME || process.env.USERPROFILE || "", ".anthropic_key");

function getKey(): string {
  if (process.env.ANTHROPIC_API_KEY) return process.env.ANTHROPIC_API_KEY;
  if (existsSync(KEY_PATH)) return readFileSync(KEY_PATH, "utf-8").trim();
  throw new Error("No API key found");
}

function loadJSON(name: string): any {
  const p = join(process.cwd(), "public", "data", "processed", name);
  if (!existsSync(p)) return null;
  try {
    return JSON.parse(readFileSync(p, "utf-8"));
  } catch {
    return null;
  }
}

// ─── Build dynamic context from pipeline JSONs ───

function buildContext(): string {
  let ctx = "";

  // 1. IBKR Portfolio
  const portfolio = loadJSON("ibkr_portfolio.json");
  if (portfolio) {
    ctx += "=== IBKR PORTFOLIO ===\n";
    ctx += "Account: " + (portfolio.account || "") + "\n";
    if (portfolio.summary) {
      ctx +=
        "Net Liquidation: $" +
        (portfolio.summary.NetLiquidation || "?") +
        "\n";
      ctx +=
        "Buying Power: $" + (portfolio.summary.BuyingPower || "?") + "\n";
      ctx +=
        "Unrealized PnL: $" +
        (portfolio.summary.UnrealizedPnL || "?") +
        "\n";
      ctx +=
        "Cash: $" + (portfolio.summary.TotalCashValue || "?") + "\n";
    }
    if (portfolio.positions) {
      ctx += "Positions (" + portfolio.positions.length + "):\n";
      portfolio.positions.forEach((p: any) => {
        ctx +=
          "  " +
          p.symbol +
          " (" +
          p.local_symbol +
          "): " +
          p.position +
          " contracts @ " +
          p.avg_cost +
          "\n";
      });
    }
    ctx += "\n";
  }

  // 2. Stocks Watch
  const stocks = loadJSON("stocks_watch.json");
  if (stocks?.commodities) {
    ctx += "=== STOCKS WATCH ===\n";
    Object.entries(stocks.commodities).forEach(
      ([sym, data]: [string, any]) => {
        if (data.stock_current) {
          ctx +=
            sym +
            ": " +
            data.stock_current +
            " " +
            (data.stock_unit || "") +
            " | State: " +
            (data.state || "?") +
            " | Deviation: " +
            (data.stock_avg
              ? (
                  ((data.stock_current - data.stock_avg) / data.stock_avg) *
                  100
                ).toFixed(1)
              : "?") +
            "%\n";
        }
      },
    );
    ctx += "\n";
  }

  // 3. Daily Reading
  const reading = loadJSON("daily_reading.json");
  if (reading) {
    ctx += "=== DAILY READING ===\n";
    ctx += JSON.stringify(reading).slice(0, 2000) + "\n\n";
  }

  // 4. Spreads (with forward-curve and lag metadata)
  const spreads = loadJSON("spreads.json");
  if (spreads?.spreads) {
    ctx += "=== SPREADS (COM CURVA FORWARD E LAGS) ===\n";
    Object.entries(spreads.spreads).forEach(([key, s]: [string, any]) => {
      ctx +=
        `${s.name}: ${s.current} ${s.unit} | Z=${s.zscore_1y} | ${s.regime} | ${s.trend}\n`;
      if (s.value_forward !== undefined)
        ctx += `  Forward: ${s.value_forward} (${s.method || ""})\n`;
      if (s.value_3m !== undefined)
        ctx += `  3m: ${s.value_3m} | 6m: ${s.value_6m || "N/A"}\n`;
      if (s.contracts)
        ctx += `  Contratos: ${JSON.stringify(s.contracts)}\n`;
      if (s.lag_note)
        ctx += `  Lag: ${s.lag_note}\n`;
    });
    ctx += "\n";
  }

  // 5. Paridades e Ratios (com new crop, basis real, lags)
  const parities = loadJSON("parities.json");
  if (parities?.parities) {
    ctx += "=== PARIDADES E RATIOS (METODOLOGIA CORRETA) ===\n";
    ctx += "NOTA: Corn/Soy usa new crop (ZCZ/ZSX), não front-month\n";
    ctx += "NOTA: Feedlot usa ciclo real GF+1m/ZC+4m/LE+6m\n";
    ctx += "NOTA: Basis BR = FOB Paranaguá vs Chicago\n\n";

    Object.entries(parities.parities).forEach(([key, val]: [string, any]) => {
      if (val?.value !== undefined) {
        ctx += `${val.name}: ${val.value} ${val.unit || ""} — ${val.signal || ""}\n`;
        if (val.lag_note) ctx += `  Lag: ${val.lag_note}\n`;
        if (val.contracts)
          ctx += `  Contratos: ${JSON.stringify(val.contracts)}\n`;
      }
    });
    ctx += "\n";
  }

  // 6. COT — 3 janelas (156w/52w/26w), apenas extremos
  const cotData = loadJSON("cot.json");
  if (cotData?.commodities) {
    const cotLines: string[] = [];

    for (const [sym, v] of Object.entries(cotData.commodities) as any[]) {
      // Disaggregated (managed money) — preferido
      const dis = v?.disaggregated;
      if (dis?.cot_index !== undefined) {
        const extreme =
          dis.cot_index >= 80 ||
          dis.cot_index <= 20 ||
          (dis.cot_index_52w !== undefined &&
            (dis.cot_index_52w >= 80 || dis.cot_index_52w <= 20));
        if (extreme) {
          cotLines.push(
            `${sym}: 156w=${dis.cot_index} | 52w=${dis.cot_index_52w ?? "N/A"} | 26w=${dis.cot_index_26w ?? "N/A"} | MM_net=${dis.latest?.managed_money_net ?? "?"}`,
          );
        }
        continue;
      }

      // Legacy fallback
      const leg = v?.legacy;
      if (!leg?.latest || !leg?.history?.length) continue;
      const nets = leg.history
        .map((h: any) => h.noncomm_net)
        .filter((n: any) => n != null);
      if (nets.length < 20) continue;
      const mn = Math.min(...nets),
        mx = Math.max(...nets);
      if (mx === mn) continue;
      const cotIndex = ((leg.latest.noncomm_net - mn) / (mx - mn)) * 100;
      if (cotIndex > 80 || cotIndex < 20) {
        cotLines.push(
          `${sym}: net=${leg.latest.noncomm_net} | cot_index=${cotIndex.toFixed(0)}/100 | ${cotIndex > 80 ? "CROWDED LONG" : "CROWDED SHORT"} (legacy)`,
        );
      }
    }

    if (cotLines.length) {
      ctx += "=== COT EXTREMOS — 3 JANELAS (156w/52w/26w) ===\n";
      ctx += cotLines.join("\n") + "\n\n";
    }
  }

  // 7. Futures curve (new crop contracts)
  const futures = loadJSON("futures_contracts.json");
  if (futures?.commodities) {
    const newCropCodes: Record<string, string[]> = {
      ZC: ["Z"],
      ZS: ["X"],
      ZW: ["Z"],
      LE: ["V", "Z"],
      GF: ["V", "X"],
      CL: ["N", "V"],
    };
    const fwdLines: string[] = [];

    for (const [sym, months] of Object.entries(newCropCodes)) {
      const contracts =
        futures.commodities[sym]?.contracts || [];
      for (const mc of months) {
        const c = contracts.find(
          (ct: any) => ct.month_code === mc && ct.close && ct.close > 0,
        );
        if (c)
          fwdLines.push(`${c.contract}: ${c.close} (${c.expiry_label})`);
      }
    }

    if (fwdLines.length) {
      ctx += "=== CURVA FORWARD (NEW CROP) ===\n";
      ctx += fwdLines.join("\n") + "\n\n";
    }
  }

  // 8. Intel Synthesis
  const intel = loadJSON("intel_synthesis.json");
  if (intel && !intel.is_fallback) {
    ctx += "=== SÍNTESE DE INTELIGÊNCIA (PIPELINE) ===\n";
    if (intel.summary) ctx += intel.summary.slice(0, 1000) + "\n";
    if (intel.priority_high?.length) {
      ctx += "Sinais de alta prioridade:\n";
      intel.priority_high.forEach((s: any) => {
        ctx += `  ${s.title || s.commodity || "?"}: ${s.detail || s.signal || s.reason || ""}\n`;
      });
    }
    ctx += "\n";
  }

  // 9. Price Validation — flag suspicious data
  const validation = loadJSON("price_validation.json");
  if (validation?.details) {
    const suspicious = Object.entries(validation.details)
      .filter(([, v]: any) => v.is_suspicious)
      .map(([k, v]: any) => `${k}: ${v.reason}`);
    if (suspicious.length > 0) {
      ctx += "=== DADOS SUSPEITOS — NÃO USAR PARA ANÁLISE ===\n";
      ctx += suspicious.join("\n") + "\n";
      ctx +=
        "Estes preços foram detectados como incorretos pelo validador.\n";
      ctx += "Use web_search para obter preços atuais destes símbolos.\n\n";
    }
  }

  // 10. PSD Ending Stocks fallbacks
  const psd = loadJSON("psd_ending_stocks.json");
  if (psd?.commodities) {
    const fallbacks = Object.entries(psd.commodities)
      .filter(([, v]: any) => v.is_fallback)
      .map(([k, v]: any) => `${k} (${v.source || "fallback"})`);
    if (fallbacks.length > 0) {
      ctx += "=== DADOS DE ESTOQUE COM FALLBACK ===\n";
      ctx +=
        `Commodities sem dados primários: ${fallbacks.join(", ")}\n`;
      ctx +=
        "Use web_search para verificar dados atuais destas commodities.\n\n";
    }
  }

  return ctx;
}

// ─── System prompt with verification protocol ───

const SYSTEM_PROMPT_HEADER = `Você é um analista sênior de commodities agrícolas com acesso a dados do AgriMacro Intelligence e capacidade de busca web.

PROTOCOLO OBRIGATÓRIO DE VERIFICAÇÃO:

1. DADOS DE FALLBACK: Qualquer dado marcado como "fallback" nos contextos abaixo DEVE ser verificado via web_search antes de usar na análise.
   Exemplo: se CC (Cacau) tem dados de fallback, busque "cocoa supply demand balance 2025/26 ICCO".

2. DADOS SUSPEITOS: Símbolos listados como suspeitos NÃO devem ser analisados com os dados do contexto. Use web_search para obter preços atuais.

3. VERIFICAÇÃO DE QUALIDADE: Para qualquer análise crítica (especialmente sobre posição de mercado, déficit/superávit, safra), faça uma busca web para confirmar que os dados do pipeline estão atualizados.

4. CONFLITO DE DADOS: Se web_search retornar informação que contradiz o contexto do pipeline, informe explicitamente:
   "CONFLITO: Pipeline diz X, mas fonte atual diz Y. Usando Y."

5. METODOLOGIA CORRETA:
   - Corn/Soy ratio: usar new crop (ZCZ/ZSX), não front-month
   - Feedlot margin: ciclo real GF+1m/ZC+4m/LE+6m
   - Basis BR: FOB Paranaguá vs Chicago (não BRL×ZS)
   - COT: verificar as 3 janelas (156w/52w/26w)
   - Lags: CL→fertilizantes 6-12m, Crush→oferta bovina 12-18m

6. FORMATO DA RESPOSTA:
   - Português brasileiro
   - Máximo 1000 palavras
   - Sempre indicar fonte (pipeline/web) para cada dado usado
   - Sempre indicar lag quando relevante
   - Se dado for fallback ou suspeito, dizer explicitamente

`;

// ─── API handler ───

export async function POST(req: NextRequest) {
  try {
    const { messages } = await req.json();
    const client = new Anthropic({ apiKey: getKey() });
    const context = buildContext();
    const systemPrompt = SYSTEM_PROMPT_HEADER + context;

    const response = await client.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 8096,
      system: systemPrompt,
      messages: messages,
      tools: [
        {
          type: "web_search_20250305",
          name: "web_search",
          max_uses: 5,
        },
      ],
    });

    // Extract text from response, handling web_search result blocks
    const text = response.content
      .filter((c: any) => c.type === "text")
      .map((c: any) => c.text)
      .join("");

    return NextResponse.json({ response: text });
  } catch (error: any) {
    console.error("[chat/route] Error:", error.message || error);
    return NextResponse.json(
      { error: error.message || "Unknown error" },
      { status: 500 },
    );
  }
}
