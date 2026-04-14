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

function loadRawJSON(name: string): any {
  const p = join(process.cwd(), "public", "data", "raw", name);
  if (!existsSync(p)) return null;
  try { return JSON.parse(readFileSync(p, "utf-8")); } catch { return null; }
}

function truncate(obj: any, maxLen: number): string {
  const s = JSON.stringify(obj);
  return s.length > maxLen ? s.slice(0, maxLen) + "..." : s;
}

function buildStrategyContext(userThesis: string): string {
  const sections: string[] = [];

  sections.push(`Você é um analista sênior de commodities com acesso completo aos dados do AgriMacro. O usuário vai descrever uma tese ou estratégia de mercado.

Sua resposta DEVE seguir EXATAMENTE este formato com estas seções (use ## para cada título):

## VALIDAÇÃO DA TESE
Avalie cada premissa do usuário contra os dados disponíveis.

## RISCOS IDENTIFICADOS
Liste riscos não mencionados pelo usuário.

## DADOS QUE SUPORTAM
Dados que confirmam a tese. Prefixe cada ponto com [SUPORTA].

## DADOS QUE CONTRADIZEM
Dados que contradizem a tese. Prefixe cada ponto com [CONTRADIZ].

## CORRELAÇÕES RELEVANTES
Cruzamentos entre ativos, spreads, COT e macro relevantes.

## SAZONALIDADE
Padrões sazonais que afetam a tese.

## SUGESTÕES DE AJUSTE
Ajustes ou estruturas complementares.

## MONITORAMENTO CRÍTICO
O que monitorar nas próximas 24-72 horas.

Regras:
- Seja direto, técnico e não hesite em discordar quando os dados contradizem a tese
- QUANTIFIQUE sempre que possível (z-scores, percentis, correlações)
- Use [SUPORTA], [CONTRADIZ] ou [NEUTRO] como prefixo em pontos-chave
- Responda em português brasileiro, tom institucional e analítico
- NÃO invente dados. Use APENAS o que está disponível abaixo.`);

  // === DADOS DE MERCADO ===
  const prices = loadRawJSON("price_history.json") || loadJSON("price_history.json");
  if (prices) {
    sections.push("\n=== DADOS DE MERCADO (preços recentes) ===");
    const summary: string[] = [];
    for (const [sym, data] of Object.entries(prices)) {
      const arr = data as any[];
      if (!Array.isArray(arr) || arr.length === 0) continue;
      const last = arr[arr.length - 1];
      const prev = arr.length > 1 ? arr[arr.length - 2] : null;
      const chg = prev ? ((last.close - prev.close) / prev.close * 100).toFixed(2) : "?";
      summary.push(`${sym}: ${last.close} (${Number(chg) >= 0 ? "+" : ""}${chg}%) [${last.date}]`);
    }
    sections.push(summary.join("\n"));
  }

  const futures = loadJSON("futures_contracts.json");
  if (futures) {
    sections.push("\n=== CURVA FORWARD (contango/backwardation) ===");
    sections.push(truncate(futures, 3000));
  }

  const cot = loadJSON("cot.json");
  if (cot) {
    sections.push("\n=== POSICIONAMENTO COT (CFTC) ===");
    sections.push(truncate(cot, 3000));
  }

  // === FUNDAMENTOS ===
  const psd = loadJSON("psd_ending_stocks.json");
  if (psd) {
    sections.push("\n=== ESTOQUES FINAIS (USDA PSD) ===");
    sections.push(truncate(psd, 2000));
  }

  const crop = loadJSON("crop_progress.json");
  if (crop) {
    sections.push("\n=== PROGRESSO DE SAFRA EUA ===");
    sections.push(truncate(crop, 1500));
  }

  const gtr = loadJSON("usda_gtr.json");
  if (gtr) {
    sections.push("\n=== USDA GTR (frete, BDI, competitividade) ===");
    sections.push(truncate(gtr, 1500));
  }

  const grainRatios = loadJSON("grain_ratios.json");
  if (grainRatios) {
    sections.push("\n=== GRAIN RATIOS (Lasso, COT Index, margem vs COP) ===");
    sections.push(truncate(grainRatios, 2000));
  }

  const conab = loadJSON("conab_data.json");
  if (conab) {
    sections.push("\n=== CONAB (produção brasileira) ===");
    sections.push(truncate(conab, 1500));
  }

  // === SPREADS E CORRELAÇÕES ===
  const spreads = loadJSON("spreads.json");
  if (spreads) {
    sections.push("\n=== SPREADS (z-score, regime) ===");
    sections.push(truncate(spreads, 2500));
  }

  const corr = loadJSON("correlations.json");
  if (corr) {
    sections.push("\n=== CORRELAÇÕES E COMPOSITE SIGNALS ===");
    sections.push(truncate(corr, 2500));
  }

  const synthesis = loadJSON("intel_synthesis.json");
  if (synthesis) {
    sections.push("\n=== SÍNTESE DO DIA (sinais HIGH/MEDIUM/LOW) ===");
    sections.push(truncate(synthesis, 2000));
  }

  // === MACRO ===
  const macro = loadJSON("macro_indicators.json");
  if (macro) {
    sections.push("\n=== MACRO (VIX, S&P, Juros 10Y) ===");
    sections.push(JSON.stringify(macro));
  }

  const fedwatch = loadJSON("fedwatch.json");
  if (fedwatch) {
    sections.push("\n=== FEDWATCH (probabilidades FOMC) ===");
    sections.push(truncate(fedwatch, 1000));
  }

  const bcb = loadJSON("bcb_data.json");
  if (bcb) {
    sections.push("\n=== BCB (BRL/USD, Selic) ===");
    const brlRecent = bcb.brl_usd?.slice(-5) || [];
    const selicRecent = bcb.selic_meta?.slice(-3) || [];
    sections.push(JSON.stringify({ brl_usd_recent: brlRecent, selic_recent: selicRecent }));
  }

  const trends = loadJSON("google_trends.json");
  if (trends) {
    sections.push("\n=== GOOGLE TRENDS (spikes de sentimento) ===");
    sections.push(truncate(trends, 1000));
  }

  // === CLIMA ===
  const weather = loadJSON("weather_agro.json");
  if (weather) {
    sections.push("\n=== CLIMA (alertas + ENSO) ===");
    sections.push(truncate(weather, 1500));
  }

  // (Portfolio IBKR moved to enriched context section below with structured labels)

  // === SENTIMENTO GROK ===
  for (const gf of ["grok_sentiment.json", "grok_news.json", "grok_macro.json"]) {
    const g = loadJSON(gf);
    if (g && !g.is_fallback && g.content) {
      const label = gf.replace("grok_", "").replace(".json", "").toUpperCase();
      sections.push(`\n=== GROK ${label} ===`);
      sections.push(g.content.slice(0, 1500));
    }
  }

  // === DADOS ADICIONAIS BRASIL ===
  const bilateral = loadJSON("bilateral_indicators.json");
  if (bilateral) {
    sections.push("\n=== INDICADORES BILATERAIS ===");
    sections.push(truncate(bilateral, 2000));
  }

  const physBR = loadJSON("physical_br.json");
  if (physBR) {
    sections.push("\n=== MERCADO FÍSICO BRASIL (CEPEA/Paranaguá) ===");
    sections.push(truncate(physBR, 2000));
  }

  const imea = loadJSON("imea_soja.json");
  if (imea) {
    sections.push("\n=== IMEA SOJA (COP Mato Grosso) ===");
    sections.push(truncate(imea, 2000));
  }

  const usdaTransport = loadJSON("usda_brazil_transport.json");
  if (usdaTransport) {
    sections.push("\n=== USDA BRAZIL TRANSPORT (frete/logística) ===");
    sections.push(truncate(usdaTransport, 2000));
  }

  const sugarAlcohol = loadJSON("sugar_alcohol_br.json");
  if (sugarAlcohol) {
    sections.push("\n=== AÇÚCAR E ÁLCOOL BRASIL ===");
    sections.push(truncate(sugarAlcohol, 2000));
  }

  // ═══════════════════════════════════════════════════════
  // ENRICHED CONTEXT — 5 seções com labels específicos
  // ═══════════════════════════════════════════════════════

  // 1. COMMODITY DNA — drivers rankeados por underlying
  try {
    const dnaStatic = loadJSON("commodity_dna_static.json");
    const dnaDynamic = loadJSON("commodity_dna.json");
    if (dnaStatic?.commodities || dnaDynamic?.commodities) {
      sections.push("\n=== COMMODITY DNA — DRIVERS RANKEADOS ===");
      const staticC = dnaStatic?.commodities || {};
      const dynC = dnaDynamic?.commodities || {};
      const allSyms = [...new Set([...Object.keys(staticC), ...Object.keys(dynC)])].sort();
      for (const sym of allSyms) {
        const st = staticC[sym];
        const dy = dynC[sym];
        if (st?.drivers_ranked) {
          const top3 = st.drivers_ranked.slice(0, 3);
          const driverStr = top3.map((d: any) => `${d.driver} (${d.weight})`).join(" | ");
          const signal = dy?.composite_signal || "";
          sections.push(`DNA ${sym} (${st.name || sym}): ${signal ? signal + " — " : ""}${driverStr}`);
        }
      }
    }
  } catch {}

  // 2. OPTIONS INTELLIGENCE — IV, IV Rank, Skew, Term Structure
  try {
    const chain = loadJSON("options_chain.json");
    if (chain?.underlyings) {
      sections.push("\n=== OPTIONS INTELLIGENCE — IV + SKEW + TERM STRUCTURE ===");
      for (const [sym, data] of Object.entries(chain.underlyings) as any[]) {
        const ivr = data.iv_rank || {};
        const sk = data.skew || {};
        const ts = data.term_structure || {};
        const iv = ivr.current_iv ? (ivr.current_iv * 100).toFixed(1) + "%" : null;
        const rank = ivr.rank_52w != null ? `Rank=${ivr.rank_52w.toFixed(0)}%` : (ivr.history_days ? `Rank=building(${ivr.history_days}d)` : null);
        const skew = sk.skew_pct != null ? `Skew=${sk.skew_pct > 0 ? "+" : ""}${sk.skew_pct}%` : null;
        const term = ts.structure || null;
        const parts = [`IV=${iv || "?"}`, rank, skew, term ? `Term=${term}` : null].filter(Boolean);
        sections.push(`IV ${sym}: ${parts.join(" | ")}`);
      }
    }
  } catch {}

  // 3. COT POSITIONING — índice + sinal por commodity
  try {
    const cotData = loadJSON("cot.json");
    if (cotData?.commodities) {
      sections.push("\n=== COT POSITIONING (CFTC) ===");
      for (const [sym, data] of Object.entries(cotData.commodities) as any[]) {
        const dis = data.disaggregated || {};
        const leg = data.legacy || {};
        if (dis.cot_index != null) {
          const label = dis.cot_index >= 80 ? "CROWDED LONG" : dis.cot_index <= 20 ? "CROWDED SHORT" : dis.cot_index >= 65 ? "longs acumulando" : dis.cot_index <= 35 ? "shorts acumulando" : "neutro";
          const w52 = dis.cot_index_52w != null ? ` | 52w=${dis.cot_index_52w.toFixed(0)}` : "";
          sections.push(`COT ${sym}: idx=${dis.cot_index.toFixed(0)} (${label})${w52}`);
        } else if (leg.latest?.noncomm_net != null) {
          sections.push(`COT ${sym}: net=${leg.latest.noncomm_net} (legacy)`);
        }
      }
    }
  } catch {}

  // 4. SAZONALIDADE — retorno médio do mês atual por commodity
  try {
    const seasonData = loadJSON("seasonality.json");
    if (seasonData) {
      const months = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"];
      const curMonth = new Date().getMonth();
      sections.push(`\n=== SAZONALIDADE — ${months[curMonth].toUpperCase()} ===`);
      for (const sym of Object.keys(seasonData).sort()) {
        const s = seasonData[sym];
        if (!s?.monthly_returns) continue;
        const v = s.monthly_returns[curMonth];
        const avg = typeof v === "number" ? v : v?.avg ?? null;
        if (avg == null) continue;
        const pct = typeof v === "object" ? v?.positive_pct : null;
        const deviation = Math.abs(avg) >= 2 ? " **FORTE**" : Math.abs(avg) >= 1 ? " *moderado*" : "";
        sections.push(`Sazonalidade ${sym} ${months[curMonth]}: desvio ${avg >= 0 ? "+" : ""}${avg.toFixed(2)}% vs historico${pct != null ? ` (${pct}% positivo)` : ""}${deviation}`);
      }
    }
  } catch {}

  // 5. POSIÇÕES IBKR — resumo estruturado
  try {
    const port = loadJSON("ibkr_portfolio.json");
    if (port?.positions && !port.is_fallback) {
      sections.push("\n=== PORTFOLIO IBKR — POSIÇÕES ABERTAS ===");
      const summ = port.summary || {};
      sections.push(`Net Liquidation: $${summ.NetLiquidation || "?"} | Cash: $${summ.TotalCashValue || "?"} | Unrealized P&L: $${summ.UnrealizedPnL || "?"}`);
      // Group by symbol
      const bySymbol: Record<string, any[]> = {};
      for (const p of port.positions) {
        if (p.sec_type !== "FOP" && p.sec_type !== "FUT") continue;
        if (!bySymbol[p.symbol]) bySymbol[p.symbol] = [];
        bySymbol[p.symbol].push(p);
      }
      const lines: string[] = [];
      for (const sym of Object.keys(bySymbol).sort()) {
        const legs = bySymbol[sym];
        const parts = legs.map((l: any) => {
          const ls = l.local_symbol || "";
          const pos = l.position > 0 ? `+${l.position}` : `${l.position}`;
          return `${pos}x ${ls}`;
        });
        lines.push(`Portfolio IBKR: ${sym} — ${parts.join(", ")}`);
      }
      sections.push(lines.join("\n"));
    }
  } catch {}

  // === TESE DO USUÁRIO ===
  sections.push("\n=== TESE DO USUÁRIO ===");
  sections.push(userThesis);

  return sections.join("\n");
}

export async function POST(req: NextRequest) {
  try {
    const { thesis, history } = await req.json();

    if (!thesis || typeof thesis !== "string") {
      return NextResponse.json({ error: "Campo 'thesis' é obrigatório" }, { status: 400 });
    }

    const client = new Anthropic({ apiKey: getKey() });
    const systemPrompt = buildStrategyContext(thesis);

    const messages: { role: "user" | "assistant"; content: string }[] = [];

    // Include conversation history for follow-ups
    if (Array.isArray(history) && history.length > 0) {
      for (const h of history.slice(-4)) {
        messages.push({ role: h.role, content: h.content });
      }
    }

    messages.push({ role: "user", content: thesis });

    const response = await client.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 4096,
      system: systemPrompt,
      messages,
    });

    const text = response.content
      .filter((c: any) => c.type === "text")
      .map((c: any) => c.text)
      .join("");

    return NextResponse.json({ response: text });
  } catch (error: any) {
    console.error("Strategy API error:", error);
    return NextResponse.json({ error: error.message || "Unknown error" }, { status: 500 });
  }
}
