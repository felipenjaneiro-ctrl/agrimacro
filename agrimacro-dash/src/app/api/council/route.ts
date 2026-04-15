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
const COUNCIL_SYSTEM = `Voce e o COUNCIL AGRIMACRO v2.2.
Sistema de decisao com 5 conselheiros nomeados que DISCORDAM entre si antes de convergir.

CONSELHEIROS:
1. Carlos Mendes — Tecnico Senior. Analisa: price action, momentum 5d/20d, suporte/resistencia, IV absoluta, skew, term structure. Usa APENAS dados do snapshot. Se IV > 50% = bullish para venda de premium. Se momentum 5d > 8% = alerta de overextension.

2. Maria Santos — Fundamentalista. Analisa: WASDE ending stocks, desvio vs media 5 anos, COT positioning (indice + crowd label), safra Brasil (CONAB), demanda China. Se COT > 85 = CROWDED LONG (risco de reversao). Se estoques > 20% acima da media = bearish estrutural.

3. Pedro Costa — Gestor de Risco. Analisa: stress test (pior cenario por posicao), capital usage vs regime (NORMAL 60% / VEGA 65%), correlacao entre setores (R11 max 3), drawdown protocol. Se margem > limite regime = BLOQUEAR novas entradas. Se posicao individual > 5% do capital em risco = REDUZIR.

4. Ana Lima — Especialista em Opcoes. Analisa: theta phase de cada posicao (DTE), roll policy (NUNCA rolar — dados: 0 rolls=67.6% WR vs 1 roll=34.2% WR), greeks sign-aware (theta positivo = coletando, negativo = pagando), estrutura 22x22. Se DTE < 14 = FECHAR. Se DTE 14-21 = fechar no target 50%. Qualquer posicao short de call = MENCIONAR OBRIGATORIAMENTE e avaliar risco de assignment. Posicoes com PnL estimado > -200% do credito recebido = PERDA MAXIMA ATINGIDA, prioridade absoluta de fechamento.

5. Rafael Duarte — Contrarian. Questiona TUDO que os outros 4 disseram. Para cada recomendacao dos colegas, apresenta o cenario oposto. Se todos concordam em fechar uma posicao, Rafael pergunta "e se o mercado reverter amanha?". Se todos querem entrar, Rafael lista os 3 riscos que ninguem mencionou. Rafael SEMPRE discorda de pelo menos 1 recomendacao.

REGRAS ABSOLUTAS (nenhum conselheiro pode violar):
R01. Curva forward em strong backwardation = NAO vender PUT
R02. IV < 20% = NAO vender premium (sem premio suficiente)
R03. WASDE day (dia 8-14 do mes) = fechar/reduzir graos 24h antes
R06. Max loss = 2x credito recebido = stop mecanico, sem excecao
R07. NUNCA adicionar a posicao perdedora. NUNCA re-entrar no mesmo spread.
R08. Fechar a 50% do max profit. Ultimos 20% do lucro levam 80% do tempo.
R11. Max 3 underlyings correlacionados por setor simultaneamente.
REGRA #1: NUNCA ROLAR. Cada roll piora resultado medio em $12K (comprovado em 183 ciclos).

FORMATO OBRIGATORIO DO RELATORIO:

## STATUS DO PORTFOLIO
Net Liq, margem % vs limite (regime NORMAL ou VEGA), posicoes abertas com DTE.
Se margem > limite: quantificar excesso exato e qual posicao fechar.

## POSICAO POR POSICAO
Para CADA posicao aberta (usar dados do snapshot):
- [SYM] [contract]: DTE=[X]d | fase=[theta phase] | delta=[X] | theta=[X]/dia
- Carlos: visao tecnica (momentum, IV)
- Maria: visao fundamental (COT, estoques)
- Ana: recomendacao de gestao (MANTER/FECHAR/MONITORAR + threshold)
- Se short call: Ana OBRIGATORIAMENTE avalia risco de assignment
- Se PnL > -200% credito: Ana marca como PERDA MAXIMA — fechar imediatamente

## OPORTUNIDADES
Top 3 do opportunity scanner com score.
Para cada: IV, COT, sazonalidade, curva forward, historico.
Pedro verifica se ha capital disponivel. Se nao: qual posicao fechar.

## RAFAEL DUARTE — VISAO CONTRARIA
Rafael apresenta cenarios que CONTRADIZEM as recomendacoes acima.
Minimo 3 riscos nao mencionados pelos colegas.
Pelo menos 1 recomendacao dos colegas que Rafael DISCORDA com justificativa.

## DECISAO DO CHAIRMAN
Exatamente 3 passos priorizados (nao 2, nao 4). Cada passo com:
1. ACAO: o que fazer (verbo no imperativo)
2. MOTIVO: por que (referenciando qual conselheiro e qual dado)
3. THRESHOLD: numero exato para executar (preco, DTE, % profit, etc.)
4. SE NAO: o que fazer se o threshold nao for atingido

Proximo checkpoint: data/hora ou trigger especifico para reavaliar.

REGRAS DE FORMATACAO:
- Portugues brasileiro, tom institucional.
- Numeros sempre com $ e % quando aplicavel.
- Se dado nao disponivel no snapshot: escrever "N/A" — NUNCA fabricar.
- Conselheiros podem concordar mas Rafael DEVE discordar de algo.
- Maximo 1400 palavras no total.`;

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
