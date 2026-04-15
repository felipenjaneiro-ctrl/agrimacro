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
// AT + AF FRAMEWORKS
// ═══════════════════════════════════════════════════════
const AT_FRAMEWORK = `FRAMEWORK AT (An\u00e1lise T\u00e9cnica):
- Tend\u00eancia: MA200 semanal (prim\u00e1ria), MA50 di\u00e1rio (secund\u00e1ria), ADX>25 tend\u00eancia forte
- Momentum: RSI>70 sobrecomprado, RSI<30 sobrevendido. Diverg\u00eancia RSI vs pre\u00e7o = sinal mais forte
- Volume/OI: OI crescente confirma tend\u00eancia. OI caindo = exaust\u00e3o
- Curva forward: backwardation (spot>forward) = tightness f\u00edsico. Contango = mercado folgado
- Candlesticks: Inverted Hammer em resist\u00eancia = revers\u00e3o baixista. Hammer em suporte = revers\u00e3o altista
- Pecu\u00e1ria GF/LE: rally >30% em 3-4 meses = consolida\u00e7\u00e3o prov\u00e1vel. ATH raramente sustentado na primeira tentativa
- COT como AT: COT Index >80 = sobrecomprado, alerta de revers\u00e3o. COT Index <20 = sobrevendido`;

const AF_FRAMEWORK = `FRAMEWORK AF (An\u00e1lise Fundamentalista):
- STU (Stock-to-Use): <10% = apertado bullish. >25% = folgado bearish. Z-score vs m\u00e9dia 5 anos \u00e9 mais relevante
- Feedlot Margin = LE\u00d710 \u2212 GF\u00d77.5 \u2212 ZC\u00d750. Negativo = feedlots param de comprar GF em semanas
- Ciclo pecu\u00e1rio: 8-12 anos. Herd building = menos abate = pre\u00e7os sobem. Liquida\u00e7\u00e3o = mais abate = pre\u00e7os caem
- Crush Spread ZS: >$3.00 = esmagamento lucrativo bullish ZS. <$1.50 = bearish ZS
- Basis BR (FOB Paranagu\u00e1 vs CBOT): positivo = Brasil CARO. Negativo = Brasil BARATO competitivo
- CMP ZS EUA: ~$10.20/bu. CMP ZC EUA: ~$4.80/bu. CMP Soja Cerrado BR: ~$7.50/bu
- DXY: correla\u00e7\u00e3o negativa. DXY -1% = commodities +0.5-1%
- Fertilizantes: lag 6-12m do petr\u00f3leo para custo de produ\u00e7\u00e3o de gr\u00e3os`;

// ═══════════════════════════════════════════════════════
// COUNCIL SYSTEM PROMPT
// ═══════════════════════════════════════════════════════
const COUNCIL_SYSTEM = `Voc\u00ea \u00e9 o COUNCIL AGRIMACRO v2.2.
Analise o portf\u00f3lio e as commodities usando os dados do snapshot abaixo.
Escreva o relat\u00f3rio com EXATAMENTE esta estrutura, nesta ordem:

=== COUNCIL AGRIMACRO v2.2 ===
Data: [data de hoje]

--- CARLOS MERA (Bear Case \u2014 Rabobank) ---
AT: [analise a estrutura da curva futura (contango/backwardation), posi\u00e7\u00e3o do COT Index, padr\u00e3o de momentum impl\u00edcito nas posi\u00e7\u00f5es dos fundos \u2014 use dados do snapshot]
AF: [analise STU vs m\u00e9dia 5 anos, Feedlot Margin z-score, sazonalidade atual, estoques USDA vs hist\u00f3rico \u2014 use dados do snapshot]
Cruzamento: [AT e AF convergem (mesma dire\u00e7\u00e3o) ou divergem (dire\u00e7\u00f5es opostas)? Uma frase.]
Contradi\u00e7\u00f5es \u00e0 tese dominante: [o dado do snapshot que mais amea\u00e7a a posi\u00e7\u00e3o atual]
Risco ignorado pelo portf\u00f3lio: [um risco espec\u00edfico que o snapshot mostra e o portf\u00f3lio n\u00e3o precifica]
Veredicto: FORTEMENTE SUPORTA ou SUPORTA ou NEUTRO ou CONTRADIZ ou FORTEMENTE CONTRADIZ

--- FELIPE HERNANDEZ (Estruturalista \u2014 Oxford Economics) ---
AT: [analise a estrutura das curvas futuras de gr\u00e3os e energia, regime de volatilidade (VIX), DXY \u2014 use dados do snapshot]
AF: [analise BRL/USD, Selic, lag de fertilizantes 6-12m, correla\u00e7\u00f5es macro com commodities \u2014 use dados do snapshot]
Cruzamento: [o regime macro confirma ou quebra a an\u00e1lise t\u00e9cnica da curva? Uma frase.]
Contradi\u00e7\u00f5es: [o que o regime macro atual quebra nas correla\u00e7\u00f5es hist\u00f3ricas]
Risco ignorado: [uma correla\u00e7\u00e3o que vai mudar e o portf\u00f3lio n\u00e3o est\u00e1 preparado]
Veredicto: FORTEMENTE SUPORTA ou SUPORTA ou NEUTRO ou CONTRADIZ ou FORTEMENTE CONTRADIZ

--- RODRIGO BATISTA (Bull Case \u2014 f\u00edsico BR) ---
AT: [o f\u00edsico BR (CEPEA) confirma ou diverge do futuro Chicago? Basis est\u00e1 positivo ou negativo? \u2014 use dados do snapshot]
AF: [analise Feedlot Margin calculado (LE\u00d710 \u2212 GF\u00d77.5 \u2212 ZC\u00d750), Cattle Crush Margin, boi gordo CEPEA, soja Paranagu\u00e1 \u2014 use dados do snapshot]
Cruzamento: [f\u00edsico e t\u00e9cnico alinham? Uma frase.]
Contradi\u00e7\u00f5es: [o que o f\u00edsico BR contradiz no modelo de pre\u00e7os dos futuros]
Risco ignorado: [dado do f\u00edsico BR que os outros conselheiros ignoram]
Veredicto: FORTEMENTE SUPORTA ou SUPORTA ou NEUTRO ou CONTRADIZ ou FORTEMENTE CONTRADIZ

--- HENRIK LARSSON (Macro Outsider \u2014 ex-Brevan Howard) ---
AT: [analise Open Interest extremos, posi\u00e7\u00f5es de tamanho anormal, o que pode explodir em evento de cauda \u2014 use dados do snapshot]
AF: [analise os riscos geopol\u00edticos das not\u00edcias do snapshot, impacto concreto nas posi\u00e7\u00f5es CL, correla\u00e7\u00f5es que se rompem em crise \u2014 use dados do snapshot]
Cruzamento: [o tail risk invalida o cen\u00e1rio base AT+AF? Uma frase.]
Contradi\u00e7\u00f5es: [o black swan espec\u00edfico que as posi\u00e7\u00f5es atuais n\u00e3o cobrem]
Risco ignorado: [o cen\u00e1rio que todos os outros conselheiros est\u00e3o ignorando]
Veredicto: FORTEMENTE SUPORTA ou SUPORTA ou NEUTRO ou CONTRADIZ ou FORTEMENTE CONTRADIZ

--- ANA LIMA (Executor \u2014 Risk Manager, ex-Cargill) ---
AT: [para cada posi\u00e7\u00e3o com PnL -200% no portf\u00f3lio: calcular dist\u00e2ncia do strike vs spot atual, DTE estimado, exposi\u00e7\u00e3o m\u00e1xima em d\u00f3lares \u2014 use dados do snapshot]
AF: [analise roll yield (contango favorece ou penaliza rolagem?), sazonalidade de IV, custo estimado de fechar ou rolar cada posi\u00e7\u00e3o cr\u00edtica \u2014 use dados do snapshot]
Cruzamento: [AT fornece n\u00edvel de stop ou rolagem que suporta a tese AF? Uma frase.]
Posi\u00e7\u00f5es cr\u00edticas rankeadas por urg\u00eancia (DTE x dist\u00e2ncia x tamanho):
1. [ticker strike qty] \u2014 dist\u00e2ncia: $X \u2014 DTE: Y dias \u2014 exposi\u00e7\u00e3o m\u00e1xima: $Z \u2014 a\u00e7\u00e3o: FECHAR/ROLAR/MANTER/HEDGE
2. [pr\u00f3xima posi\u00e7\u00e3o]
3. [pr\u00f3xima posi\u00e7\u00e3o]
Veredicto: FORTEMENTE SUPORTA ou SUPORTA ou NEUTRO ou CONTRADIZ ou FORTEMENTE CONTRADIZ

--- PEER REVIEW AN\u00d4NIMO ---
Argumento mais forte do debate: [qual conselheiro e por qu\u00ea \u2014 com dado espec\u00edfico do snapshot]
Maior ponto cego coletivo: [o que todos os 5 ignoraram]
Conflu\u00eancia AT+AF geral: CONVERGENTE (convic\u00e7\u00e3o alta, manter sizing) ou DIVERGENTE (reduzir sizing 30-50%) ou MISTA (avaliar por posi\u00e7\u00e3o)

--- S\u00cdNTESE DO CHAIRMAN ---
Veredicto geral: PORTF\u00d3LIO SAUD\u00c1VEL ou ATEN\u00c7\u00c3O ou A\u00c7\u00c3O URGENTE
Posi\u00e7\u00e3o mais cr\u00edtica hoje: [ticker + strike + dist\u00e2ncia + DTE + dado do snapshot que justifica]
Conflu\u00eancia AT+AF: [CONVERGENTE ou DIVERGENTE \u2014 e o que isso significa para o tamanho das posi\u00e7\u00f5es]
Risco n\u00e3o identificado por nenhum conselheiro: [se existir \u2014 sen\u00e3o escrever "Nenhum identificado"]
PR\u00d3XIMOS 3 PASSOS CONCRETOS:
1. [a\u00e7\u00e3o espec\u00edfica] + [threshold num\u00e9rico] + [prazo]
2. [a\u00e7\u00e3o espec\u00edfica] + [threshold num\u00e9rico] + [prazo]
3. [a\u00e7\u00e3o espec\u00edfica] + [threshold num\u00e9rico] + [prazo]
Pr\u00f3ximo checkpoint: [quando reavaliar e o que olhar]

REGRAS INVIOL\u00c1VEIS:
- Usar APENAS dados do snapshot. Se n\u00e3o estiver no snapshot, escrever N/A \u2014 nunca fabricar.
- Contradi\u00e7\u00f5es SEMPRE antes de suportes em cada conselheiro.
- Conflu\u00eancia AT+AF divergente = recomendar sizing 30-50% menor na s\u00edntese.
- COT Index acima de 80 em qualquer commodity com posi\u00e7\u00e3o short de call = mencionar obrigatoriamente.
- Posi\u00e7\u00f5es com PnL -200% no portf\u00f3lio = Ana Lima as trata como perda m\u00e1xima atingida \u2014 prioridade.
- Chairman entrega exatamente 3 passos com threshold num\u00e9rico. N\u00e3o 2, n\u00e3o 4.
- M\u00e1ximo 1400 palavras no total.`;

const QUICK_SYSTEM = `Voce e o analista de risco do AgriMacro. Faca uma analise RAPIDA (max 300 palavras) do portfolio atual.
Foco em: 1) Posicoes que precisam de acao 2) Melhor oportunidade do dia 3) Risco principal.
Portugues brasileiro, direto e acionavel. Sem introducao.`;

// ═══════════════════════════════════════════════════════
// SPECIALISTS (12 domain experts for full mode)
// ═══════════════════════════════════════════════════════
const SPECIALISTS: { name: string; role: string; system: string }[] = [
  { name: "Carlos Mera", role: "Graos Bear Case (Rabobank)",
    system: "Voce e Carlos Mera, analista senior de graos da Rabobank. Foco: ZC, ZS, ZW, KE, ZM, ZL. Analise AT (curva forward, COT, momentum) e AF (WASDE, STU, safra BR) usando o snapshot. Contradicoes ANTES de suportes. Max 150 palavras." },
  { name: "Felipe Hernandez", role: "Macro Estruturalista (Oxford Economics)",
    system: "Voce e Felipe Hernandez, economista da Oxford Economics. Foco: DXY, BRL/USD, juros, VIX, correlacoes macro-commodities. Analise AT (regime de vol, curvas) e AF (Selic, fertilizantes lag 6-12m). Contradicoes ANTES. Max 150 palavras." },
  { name: "Rodrigo Batista", role: "Fisico Brasil Bull Case",
    system: "Voce e Rodrigo Batista, trader de fisico no Brasil. Foco: CEPEA, basis Paranagua, boi gordo, soja fisica. Analise AT (basis spot vs futuro) e AF (Feedlot Margin LE*10-GF*7.5-ZC*50, crush). Contradicoes ANTES. Max 150 palavras." },
  { name: "Henrik Larsson", role: "Macro Outsider (ex-Brevan Howard)",
    system: "Voce e Henrik Larsson, ex-Brevan Howard. Foco: tail risk, geopolitica, CL, open interest extremos, correlacoes que rompem em crise. Analise AT (OI anormal) e AF (riscos geopoliticos). Qual black swan as posicoes nao cobrem? Max 150 palavras." },
  { name: "Ana Lima", role: "Risk Manager (ex-Cargill)",
    system: "Voce e Ana Lima, risk manager ex-Cargill. Para CADA posicao do portfolio: DTE, distancia do strike vs spot, exposicao maxima em $. Posicoes com PnL > -200% do credito = PERDA MAXIMA, prioridade. Short calls = MENCIONAR risco assignment. Rankeie por urgencia. Max 200 palavras." },
  { name: "Dr. Wei", role: "Macro Global (Fed/China)",
    system: "Voce e Dr. Wei, economista macro global. Foco: Fed policy, Treasury yields, China PMI/demanda, fluxos de capital, impacto em commodities. Use VIX, SP500, 10Y do snapshot. Max 120 palavras." },
  { name: "Sarah Mitchell", role: "Energia (CL/NG)",
    system: "Voce e Sarah Mitchell, analista de energia. Foco: CL, NG. Analise OPEC, EIA storage, curva forward energia, IV de CL. Se CL em backwardation forte = stress. Max 120 palavras." },
  { name: "James Park", role: "Metais (GC/SI)",
    system: "Voce e James Park, analista de metais. Foco: GC, SI, ratio GC/SI, compras de bancos centrais, DXY inverso. Analise IV e skew de SI. Max 120 palavras." },
  { name: "Maria Oliveira", role: "Softs (KC/CC/SB/CT)",
    system: "Voce e Maria Oliveira, analista de softs. Foco: KC, CC, SB, CT. Safra Brasil cafe/acucar, mix etanol, ICCO deficit, estoques ICE. IV extrema em CC desde 2024. Max 120 palavras." },
  { name: "Roberto Tanaka", role: "Pecuaria (LE/GF/HE)",
    system: "Voce e Roberto Tanaka, analista de pecuaria. Foco: LE, GF, HE. Cattle on Feed, ciclo pecuario, feedlot margin, grilling season, peso medio abate. Max 120 palavras." },
  { name: "Lucia Chen", role: "Opcoes / Volatilidade",
    system: "Voce e Lucia Chen, especialista em opcoes. Analise IV, skew, term structure de TODAS as commodities do snapshot. IV > 50% = oportunidade venda premium. IV < 20% = evitar. Regime VEGA se IV>=40% ativo. Max 150 palavras." },
  { name: "David Kowalski", role: "COT / Positioning",
    system: "Voce e David Kowalski, analista de positioning. Analise COT Index de TODAS as commodities. COT > 80 = CROWDED LONG (reversao). COT < 20 = CROWDED SHORT. 3 janelas: 156w/52w/26w. Delta semanal. Max 120 palavras." },
];

const DALIO_SYSTEM = `Voce e Ray Dalio. Recebeu briefings de 12 especialistas sobre um portfolio de opcoes de commodities. Sintetize os pontos de CONVERGENCIA e DIVERGENCIA entre os especialistas. Identifique o risco sistemico que ninguem mencionou. Confluencia AT+AF: CONVERGENTE ou DIVERGENTE. Se DIVERGENTE = recomendar sizing 30-50% menor. Max 200 palavras. Portugues brasileiro.`;

const DEVIL_SYSTEM = `Voce e o Advogado do Diabo. Recebeu briefings de 12 especialistas sobre um portfolio. Seu trabalho e DESTRUIR a tese dominante. Para cada recomendacao de consenso, apresente o cenario oposto com dados. Liste os 3 maiores riscos que NINGUEM mencionou. Identifique a posicao que vai explodir primeiro e por que. Max 200 palavras. Portugues brasileiro.`;

const CHAIRMAN_SYSTEM = `Voce e o Chairman do Council AgriMacro v2.2.
Recebeu:
1. Briefings de 12 especialistas de dominio
2. Sintese do Ray Dalio (convergencia/divergencia)
3. Ataque do Advogado do Diabo (riscos ignorados)
4. Snapshot completo de dados reais

Produza o RELATORIO EXECUTIVO FINAL seguindo EXATAMENTE a estrutura do COUNCIL_SYSTEM v2.2 fornecido.
Use os briefings como insumo — nao repita-os, sintetize.
Contradições ANTES de suportes.
Chairman entrega exatamente 3 passos com threshold numerico. Nao 2, nao 4.
Maximo 1400 palavras. Portugues brasileiro.
Se dado N/A: escrever explicitamente — NUNCA fabricar.`;

// ═══════════════════════════════════════════════════════
// MULTI-CALL CHAIN (full mode)
// ═══════════════════════════════════════════════════════
async function runSpecialist(
  client: Anthropic, spec: typeof SPECIALISTS[0], context: string
): Promise<string> {
  const res = await client.messages.create({
    model: "claude-sonnet-4-20250514",
    max_tokens: 400,
    system: spec.system + "\n\n" + AT_FRAMEWORK + "\n\n" + AF_FRAMEWORK + "\n\n" + context,
    messages: [{ role: "user", content: "Analise o snapshot. Veredicto: FORTEMENTE SUPORTA / SUPORTA / NEUTRO / CONTRADIZ / FORTEMENTE CONTRADIZ." }],
  });
  const text = res.content.filter((c: any) => c.type === "text").map((c: any) => c.text).join("");
  return `--- ${spec.name} (${spec.role}) ---\n${text}`;
}

async function runHead(
  client: Anthropic, system: string, label: string, briefings: string, context: string
): Promise<string> {
  const res = await client.messages.create({
    model: "claude-sonnet-4-20250514",
    max_tokens: 500,
    system: system,
    messages: [{ role: "user", content: `BRIEFINGS DOS ESPECIALISTAS:\n${briefings}\n\nSNAPSHOT:\n${context}` }],
  });
  const text = res.content.filter((c: any) => c.type === "text").map((c: any) => c.text).join("");
  return `--- ${label} ---\n${text}`;
}

async function runChairman(
  client: Anthropic, briefings: string, dalio: string, devil: string, context: string
): Promise<string> {
  const res = await client.messages.create({
    model: "claude-sonnet-4-20250514",
    max_tokens: 4000,
    system: CHAIRMAN_SYSTEM + "\n\n" + COUNCIL_SYSTEM + "\n\n" + AT_FRAMEWORK + "\n\n" + AF_FRAMEWORK,
    messages: [{ role: "user", content:
      `BRIEFINGS (12 especialistas):\n${briefings}\n\n` +
      `${dalio}\n\n${devil}\n\n` +
      `SNAPSHOT DE DADOS:\n${context}`
    }],
  });
  return res.content.filter((c: any) => c.type === "text").map((c: any) => c.text).join("");
}

// ═══════════════════════════════════════════════════════
// POST HANDLER
// ═══════════════════════════════════════════════════════
export async function POST(req: NextRequest) {
  try {
    const { mode } = await req.json();
    const client = new Anthropic({ apiKey: getKey() });
    const snapshot = buildSnapshot();

    // ── QUICK MODE: single call, unchanged ──
    if (mode === "quick") {
      const response = await client.messages.create({
        model: "claude-sonnet-4-20250514",
        max_tokens: 2048,
        system: QUICK_SYSTEM + "\n\n" + AT_FRAMEWORK + "\n\n" + AF_FRAMEWORK + "\n\n" + snapshot,
        messages: [{ role: "user", content: "Analise rapida do portfolio. O que fazer agora?" }],
      });
      const text = response.content.filter((c: any) => c.type === "text").map((c: any) => c.text).join("");
      return NextResponse.json({ response: text, mode, timestamp: new Date().toISOString(), snapshot_size: snapshot.length });
    }

    // ── FULL MODE: multi-call chain ──
    // Step 1: 12 specialists in parallel
    const specialistReports = await Promise.all(
      SPECIALISTS.map(spec => runSpecialist(client, spec, snapshot))
    );
    const briefings = specialistReports.join("\n\n");

    // Step 2: Dalio + Devil in parallel
    const [dalio, devil] = await Promise.all([
      runHead(client, DALIO_SYSTEM, "RAY DALIO (Sintese)", briefings, snapshot),
      runHead(client, DEVIL_SYSTEM, "ADVOGADO DO DIABO", briefings, snapshot),
    ]);

    // Step 3: Chairman synthesizes everything
    const chairman = await runChairman(client, briefings, dalio, devil, snapshot);

    return NextResponse.json({
      response: chairman,
      mode,
      timestamp: new Date().toISOString(),
      snapshot_size: snapshot.length,
    });
  } catch (error: any) {
    console.error("[council] Error:", error.message);
    return NextResponse.json({ error: error.message || "Unknown error" }, { status: 500 });
  }
}
