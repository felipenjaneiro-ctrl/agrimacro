import { NextRequest, NextResponse } from "next/server";
import Anthropic from "@anthropic-ai/sdk";
import { readFileSync, existsSync } from "fs";
import { join } from "path";

const KEY_PATH = process.env.ANTHROPIC_API_KEY_FILE || join(process.env.HOME || process.env.USERPROFILE || "", ".anthropic_key");
const DATA_DIR = join(process.cwd(), "public", "data", "processed");
const PIPELINE_DIR = join(process.cwd(), "..", "pipeline");

function getKey(): string {
  if (process.env.ANTHROPIC_API_KEY) return process.env.ANTHROPIC_API_KEY;
  if (existsSync(KEY_PATH)) return readFileSync(KEY_PATH, "utf-8").trim();
  throw new Error("No API key found");
}

function loadData(filename: string) {
  try { return JSON.parse(readFileSync(join(DATA_DIR, filename), "utf8")); } catch { return null; }
}
function loadPipeline(filename: string) {
  try { return JSON.parse(readFileSync(join(PIPELINE_DIR, filename), "utf8")); } catch { return null; }
}
function trunc(obj: any, max: number): string {
  const s = JSON.stringify(obj);
  return s.length > max ? s.slice(0, max) + "..." : s;
}

// ═══════════════════════════════════════════════════════
// BUILD SNAPSHOT — All data for council context
// ═══════════════════════════════════════════════════════
function buildSnapshot(): string {
  let ctx = "";

  // Portfolio
  const port = loadData("ibkr_portfolio.json");
  if (port?.summary) {
    ctx += "=== PORTFOLIO ===\n";
    ctx += `Net Liq: $${port.summary.NetLiquidation} | Cash: $${port.summary.TotalCashValue} | UnrPnL: $${port.summary.UnrealizedPnL}\n`;
    if (port.positions) {
      ctx += `Positions (${port.positions.length}):\n`;
      port.positions.filter((p: any) => p.sec_type === "FOP" || p.sec_type === "FUT").forEach((p: any) => {
        ctx += `  ${p.symbol} ${p.local_symbol}: ${p.position > 0 ? "+" : ""}${p.position} delta=${p.delta ?? "?"} iv=${p.iv ? (p.iv * 100).toFixed(0) + "%" : "?"}\n`;
      });
    }
    ctx += "\n";
  }

  // Options Intelligence
  const oc = loadData("options_chain.json");
  if (oc?.underlyings) {
    ctx += "=== IV / SKEW / TERM STRUCTURE ===\n";
    Object.entries(oc.underlyings).forEach(([sym, data]: any) => {
      const ivr = data.iv_rank || {};
      const sk = data.skew || {};
      const ts = data.term_structure || {};
      const iv = ivr.current_iv ? (ivr.current_iv * 100).toFixed(0) + "%" : "?";
      const skew = sk.skew_pct != null ? `${sk.skew_pct > 0 ? "+" : ""}${sk.skew_pct}%` : "?";
      ctx += `${sym}: IV=${iv} Term=${ts.structure || "?"} Skew=${skew}\n`;
    });
    ctx += "\n";
  }

  // COT
  const cot = loadData("cot.json");
  if (cot?.commodities) {
    ctx += "=== COT POSITIONING ===\n";
    Object.entries(cot.commodities).forEach(([sym, data]: any) => {
      const dis = data.disaggregated || {};
      if (dis.cot_index != null) {
        const label = dis.cot_index >= 80 ? "CROWDED LONG" : dis.cot_index <= 20 ? "CROWDED SHORT" : "neutral";
        ctx += `${sym}: idx=${dis.cot_index.toFixed(0)} ${label}\n`;
      }
    });
    ctx += "\n";
  }

  // Macro
  const macro = loadData("macro_indicators.json");
  if (macro) {
    ctx += "=== MACRO ===\n";
    if (macro.vix) ctx += `VIX: ${macro.vix.value} (${macro.vix.level})\n`;
    if (macro.sp500) ctx += `S&P500: ${macro.sp500.value}\n`;
    if (macro.treasury_10y) ctx += `10Y: ${macro.treasury_10y.value}%\n`;
    ctx += "\n";
  }

  // Spreads
  const spreads = loadData("spreads.json");
  if (spreads?.spreads) {
    ctx += "=== SPREADS ===\n";
    Object.entries(spreads.spreads).forEach(([k, v]: any) => {
      ctx += `${k}: z=${v.zscore_1y} ${v.regime}\n`;
    });
    ctx += "\n";
  }

  // Commodity DNA dynamic
  const dna = loadData("commodity_dna.json");
  if (dna?.commodities) {
    ctx += "=== COMMODITY DNA (sinais atuais) ===\n";
    Object.entries(dna.commodities).forEach(([sym, data]: any) => {
      const top = data.drivers_ranked?.[0];
      ctx += `${sym}: ${data.composite_signal} | #1=${top?.driver || "?"}: ${top?.signal?.slice(0, 60) || "?"}\n`;
    });
    ctx += "\n";
  }

  // Skills output
  const timing = loadPipeline("entry_timing.json");
  if (timing) {
    ctx += "=== ENTRY TIMING (hoje) ===\n";
    if (timing.best_opportunity) {
      const b = timing.best_opportunity;
      ctx += `Best: ${b.sym} ${b.direction} Grade=${b.grade} (${b.score}/${b.max_score})\n`;
    }
    ctx += `Blocked: ${timing.blocked_count}\n\n`;
  }

  const theta = loadPipeline("theta_calendar.json");
  if (theta) {
    ctx += "=== THETA CALENDAR ===\n";
    ctx += `Posicoes: ${theta.positions} | Decisao: ${theta.in_decision_window} | Acao: ${theta.action_needed}\n`;
    (theta.timeline || []).slice(0, 5).forEach((t: any) => {
      ctx += `  ${t.sym} ${t.contract}: DTE=${t.dte || "?"} ${t.phase}\n`;
    });
    ctx += "\n";
  }

  const opp = loadPipeline("opportunity_scan.json");
  if (opp) {
    ctx += "=== OPPORTUNITY SCANNER ===\n";
    if (opp.best_opportunity) {
      ctx += `Best: ${opp.best_opportunity.sym} ${opp.best_opportunity.direction} Grade=${opp.best_opportunity.grade}\n`;
    }
    ctx += `Top PUTs: ${(opp.top_puts || []).slice(0, 3).map((p: any) => `${p.sym}(${p.grade})`).join(", ")}\n`;
    ctx += `Top CALLs: ${(opp.top_calls || []).slice(0, 3).map((c: any) => `${c.sym}(${c.grade})`).join(", ")}\n`;
    ctx += `Blocked: ${(opp.blocked || []).length}\n\n`;
  }

  const stress = loadPipeline("stress_test.json");
  if (stress) {
    ctx += "=== STRESS TEST ===\n";
    ctx += `Risk Level: ${stress.risk_level}\n`;
    if (stress.most_vulnerable) {
      ctx += `Most vulnerable: ${stress.most_vulnerable.sym} ${stress.most_vulnerable.contract} — worst $${stress.most_vulnerable.worst_loss?.toLocaleString()} (${stress.most_vulnerable.vuln_pct}%)\n`;
    }
    ctx += "\n";
  }

  const vega = loadPipeline("vega_monitor.json");
  if (vega) {
    ctx += "=== VEGA MONITOR ===\n";
    ctx += `Regime: ${vega.regime || "?"} | VIX: ${vega.vix?.value || "?"} (${vega.vix?.level || "?"})\n`;
    ctx += `Opportunities: ${vega.opportunities?.length || 0}\n`;
    ctx += `Reserve deployable: $${vega.reserve?.deployable?.toLocaleString() || "?"}\n\n`;
  }

  // Cross analysis key findings
  const cross = loadPipeline("cross_analysis.json");
  if (cross?.key_findings) {
    ctx += "=== CROSS ANALYSIS (regras comprovadas) ===\n";
    const kf = cross.key_findings;
    ctx += `Roll impact: ${kf.roll_impact}\n`;
    ctx += `Best combo: ${kf.best_combo} (WR=${kf.best_combo_wr}%)\n`;
    ctx += `Most predictable: ${kf.most_predictable}\n`;
    ctx += `Best month: ${kf.best_month} (clean WR=${kf.best_month_clean_wr}%)\n\n`;
  }

  return ctx;
}

// ═══════════════════════════════════════════════════════
// COUNCIL SYSTEM PROMPT
// ═══════════════════════════════════════════════════════
const COUNCIL_SYSTEM = `Voce e o Chairman do Council AgriMacro v2.2, um sistema de decisao multi-camadas com 5 conselheiros adversariais para trading de opcoes sobre futuros de commodities.

Voce tem acesso a dados REAIS (nunca fabricar):
- Portfolio real com posicoes ativas (IBKR) incluindo gregas sign-aware
- IV, Skew, Term Structure de 16 commodities (options_chain.json)
- COT positioning CFTC (disaggregated + legacy, 3 janelas: 156w/52w/26w)
- Macro: VIX, S&P500, Treasury 10Y, DXY, BRL/USD
- Spreads com z-scores e regimes (soy_crush, ke_zw, zl_cl, feedlot, etc.)
- Commodity DNA: drivers estruturais rankeados + sinais dinamicos
- Entry Timing Scanner (8 fatores, scores por underlying)
- Theta Calendar (fases: CRITICAL/CLOSE/DECISION/HARVEST/PRODUCTIVE/DISTANT)
- Opportunity Scanner (ranking consolidado PUT/CALL com fundamentals)
- Stress Test (price shock +/-5/10/15%, IV +/-30%, cascade -10%)
- Vega Monitor (regime NORMAL/VEGA, reserva estrategica, deploy conditions)
- Cross Analysis (5 regras comprovadas de 183 ciclos reais com $510K P&L)
- Capital Management: 60/25/15 split, regime VEGA expande para 65%

REGRAS ABSOLUTAS (HARD STOP — nunca violar):
R01. Curva forward desfavoravel = NAO OPERAR (backwardation forte bloqueia PUT selling)
R02. IV Rank < 25% = NAO vender premium
R03. WASDE day = fechar/reduzir graos 24h antes
R06. Max loss = 2x credito recebido (stop mecanico)
R07. NUNCA adicionar a posicao perdedora
R08. Fechar a 50% do max profit (nao esperar expiracao)
R11. Max 3 underlyings correlacionados por setor

REGRA #1 DO CROSS-ANALYSIS: NUNCA ROLAR.
Dados reais comprovam: 0 rolls = 67.6% WR (avg +$7K). 1 roll = 34.2% WR (avg -$12K).
Cada roll piora resultado medio em ~$12K. Se DTE < 14, FECHAR e abrir novo ciclo.

METODOLOGIA DO COUNCIL v2.2:
O relatorio deve refletir 5 perspectivas adversariais:
1. TECNICO (price action, momentum, suporte/resistencia, IV/skew)
2. FUNDAMENTAL (WASDE, estoques, safra BR, demanda China, COT)
3. RISCO (stress test, correlacao, capital usage, drawdown protocol)
4. SAZONALIDADE (padrao historico do mes, retorno medio, win rate)
5. CONTRARIAN (o que pode dar errado? qual cenario nao estamos vendo?)

Cada perspectiva deve CRUZAR dados antes do veredicto — nao analisar isoladamente.

FORMATO DO RELATORIO EXECUTIVO:

## STATUS DO PORTFOLIO
Capital, margem (% usado vs limite regime), posicoes ativas com DTE e fase theta.
Se margem > limite: quantificar excesso e qual posicao fechar para liberar.

## POSICOES — ANALISE INDIVIDUAL
Para cada posicao aberta: underlying, DTE, fase theta, delta, P&L estimado.
Veredicto: MANTER / FECHAR / MONITORAR (com threshold).
Se DTE < 21: urgencia e proximo passo.

## ACOES IMEDIATAS (HOJE)
Lista numerada de acoes concretas com prioridade.
Cada acao deve ter: o que fazer, por que, threshold numerico.

## OPORTUNIDADES DE ENTRADA
Top 3 oportunidades rankeadas do opportunity scanner.
Para cada: score, fundamentals, IV, COT, sazonalidade, bloqueadores.
Se capital indisponivel: quanto liberar e como.

## RISCOS E CENARIOS ADVERSOS
Posicao mais vulneravel (stress test). Correlacoes perigosas.
Cenario "cisne negro" mais provavel. Vega exposure.

## RECOMENDACAO DO CHAIRMAN
1-3 acoes priorizadas com thresholds numericos.
Se houver conflito entre perspectivas, explicitar e justificar.
Proximo checkpoint: quando reavaliar (data/hora ou trigger).

Responda em portugues brasileiro, tom institucional e analitico.
Maximo 1200 palavras. Direto, acionavel, com numeros concretos.
Se dado N/A: escrever explicitamente "N/A" — NUNCA fabricar.`;

const QUICK_SYSTEM = `Voce e o analista de risco do AgriMacro. Faca uma analise RAPIDA (max 300 palavras) do portfolio atual.
Foco em: 1) Posicoes que precisam de acao 2) Melhor oportunidade do dia 3) Risco principal.
Portugues brasileiro, direto e acionavel. Sem introducao.`;

export async function POST(req: NextRequest) {
  try {
    const { mode } = await req.json();
    const client = new Anthropic({ apiKey: getKey() });
    const snapshot = buildSnapshot();

    const system = mode === "quick" ? QUICK_SYSTEM : COUNCIL_SYSTEM;
    const maxTokens = mode === "quick" ? 2048 : 6000;

    const response = await client.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: maxTokens,
      system: system + "\n\n" + snapshot,
      messages: [{ role: "user", content: mode === "quick"
        ? "Analise rapida do portfolio. O que fazer agora?"
        : "Produza o relatorio executivo completo do Council AgriMacro v2.2 para hoje. Use as 5 perspectivas adversariais (Tecnico, Fundamental, Risco, Sazonalidade, Contrarian). Cruze os dados antes de cada veredicto. Inclua thresholds numericos em todas as recomendacoes."
      }],
    });

    const text = response.content
      .filter((c: any) => c.type === "text")
      .map((c: any) => c.text)
      .join("");

    return NextResponse.json({
      response: text,
      mode,
      timestamp: new Date().toISOString(),
      snapshot_size: snapshot.length,
    });
  } catch (error: any) {
    console.error("[council] Error:", error.message);
    return NextResponse.json({ error: error.message || "Unknown error" }, { status: 500 });
  }
}
