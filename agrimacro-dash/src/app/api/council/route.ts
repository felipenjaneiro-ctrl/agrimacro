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
  const parts: string[] = [];

  // ── BLOCO 1: PORTFOLIO IBKR (Carlos Lima, Ana Lima) ──
  const port = loadData("ibkr_portfolio.json");
  if (port?.summary) {
    parts.push("== PORTFOLIO IBKR ==");
    parts.push(`Net Liq: $${port.summary.NetLiquidation} | Cash: $${port.summary.TotalCashValue} | BuyPow: $${port.summary.BuyingPower} | UnrPnL: $${port.summary.UnrealizedPnL}`);
    if (port.positions) {
      parts.push(`Posicoes (${port.positions.length}):`);
      port.positions.filter((p: any) => p.sec_type === "FOP" || p.sec_type === "FUT").forEach((p: any) => {
        parts.push(`  ${p.symbol} ${p.local_symbol}: ${p.position > 0 ? "+" : ""}${p.position} delta=${p.delta ?? "?"} theta=${p.theta ?? "?"} iv=${p.iv ? (p.iv * 100).toFixed(0) + "%" : "?"} avgCost=${p.avg_cost ?? "?"}`);
      });
    }
    // T-Bills / Reserve
    const tbills = (port.positions || []).filter((p: any) => p.sec_type === "BILL" || (p.symbol || "").includes("US-T") || (p.local_symbol || "").includes("IBCID"));
    if (tbills.length) {
      const tbVal = tbills.reduce((s: number, t: any) => s + Math.abs(t.market_value || 0), 0);
      parts.push(`T-Bills (reserva estrategica): $${tbVal.toFixed(0)}`);
    }
    // Portfolio greeks
    const pg = port.portfolio_greeks;
    if (pg) parts.push(`Portfolio Greeks: delta=${pg.total_delta} theta=$${pg.total_theta}/dia vega=${pg.total_vega}`);
  }

  // ── BLOCO 2: OPTIONS CHAIN (Lucia Chen, todos) ──
  const oc = loadData("options_chain.json");
  if (oc?.underlyings) {
    parts.push("== OPTIONS CHAIN: IV / SKEW / TERM STRUCTURE ==");
    Object.entries(oc.underlyings).forEach(([sym, data]: any) => {
      const ivr = data.iv_rank || {};
      const sk = data.skew || {};
      const ts = data.term_structure || {};
      const iv = ivr.current_iv ? (ivr.current_iv * 100).toFixed(1) + "%" : "?";
      const rank = ivr.rank_52w != null ? `Rank=${ivr.rank_52w.toFixed(0)}%` : `Rank=building(${ivr.history_days || 0}d)`;
      const skew = sk.skew_pct != null ? `Skew=${sk.skew_pct > 0 ? "+" : ""}${sk.skew_pct}%` : "Skew=?";
      parts.push(`${sym}: IV=${iv} ${rank} ${skew} Term=${ts.structure || "?"} und=$${data.und_price || "?"}`);
    });
  }

  // ── BLOCO 3: COT POSITIONING (David Kowalski, todos) ──
  const cot = loadData("cot.json");
  if (cot?.commodities) {
    parts.push("== COT POSITIONING (CFTC) ==");
    Object.entries(cot.commodities).forEach(([sym, data]: any) => {
      const dis = data.disaggregated || {};
      const leg = data.legacy || {};
      if (dis.cot_index != null) {
        const label = dis.cot_index >= 80 ? "CROWDED LONG" : dis.cot_index <= 20 ? "CROWDED SHORT" : dis.cot_index >= 65 ? "longs acumulando" : dis.cot_index <= 35 ? "shorts acumulando" : "neutro";
        const w52 = dis.cot_index_52w != null ? ` 52w=${dis.cot_index_52w.toFixed(0)}` : "";
        const w26 = dis.cot_index_26w != null ? ` 26w=${dis.cot_index_26w.toFixed(0)}` : "";
        const mmNet = dis.latest?.managed_money_net != null ? ` MM_net=${dis.latest.managed_money_net}` : "";
        parts.push(`${sym}: idx=${dis.cot_index.toFixed(0)} (${label})${w52}${w26}${mmNet}`);
      } else if (leg.latest?.noncomm_net != null) {
        parts.push(`${sym}: legacy net=${leg.latest.noncomm_net}`);
      }
    });
  }

  // ── BLOCO 4: EIA ENERGIA (Sarah Mitchell) ──
  const eia = loadData("eia_data.json");
  if (eia) {
    parts.push("== EIA ENERGIA ==");
    const fields = ["wti_spot", "crude_stocks", "ethanol_production", "diesel_price", "natural_gas_spot", "gasoline_stocks", "refinery_utilization"];
    for (const f of fields) {
      if (eia[f]) parts.push(`${f}: ${eia[f].value} ${eia[f].unit || ""} (${eia[f].date || "?"})`);
    }
  }

  // ── BLOCO 5: BCB / MACRO BR (Ana Rodrigues) ──
  const bcb = loadData("bcb_data.json");
  if (bcb) {
    parts.push("== BCB / MACRO BRASIL ==");
    const brl = bcb.brl_usd;
    if (Array.isArray(brl) && brl.length) {
      const last = brl[brl.length - 1];
      parts.push(`BRL/USD: ${last.value} (${last.date})`);
    }
    const selic = bcb.selic_meta;
    if (Array.isArray(selic) && selic.length) {
      const last = selic[selic.length - 1];
      parts.push(`Selic: ${last.value}% (${last.date})`);
    }
  }
  const ibge = loadData("ibge_data.json");
  if (ibge) {
    if (ibge.ipca) parts.push(`IPCA: ${ibge.ipca.value}% (${ibge.ipca.date})`);
    if (ibge.ipca_food) parts.push(`IPCA Alimentacao: ${ibge.ipca_food.value}% (${ibge.ipca_food.date})`);
  }

  // ── BLOCO 6: MACRO GLOBAL (Dr. Wei, Felipe Hernandez) ──
  const macro = loadData("macro_indicators.json");
  if (macro) {
    parts.push("== MACRO GLOBAL ==");
    if (macro.vix) parts.push(`VIX: ${macro.vix.value} (${macro.vix.level}, ${macro.vix.change_pct > 0 ? "+" : ""}${macro.vix.change_pct}%)`);
    if (macro.sp500) parts.push(`S&P500: ${macro.sp500.value}`);
    if (macro.treasury_10y) parts.push(`Treasury 10Y: ${macro.treasury_10y.value}%`);
  }
  const fw = loadData("fedwatch.json");
  if (fw?.probabilities) parts.push(`FedWatch: ${fw.market_expectation} | hold=${fw.probabilities.hold}% cut=${fw.probabilities.cut_25bps}% | FOMC=${fw.next_meeting}`);

  // ── BLOCO 7: SPREADS + PARIDADES (Carlos Mera, Rodrigo Batista) ──
  const spreads = loadData("spreads.json");
  if (spreads?.spreads) {
    parts.push("== SPREADS (z-scores) ==");
    Object.entries(spreads.spreads).forEach(([k, v]: any) => {
      parts.push(`${v.name || k}: ${v.current?.toFixed(4) || "?"} ${v.unit || ""} | z=${v.zscore_1y?.toFixed(2) || "?"} | ${v.regime} | ${v.trend || ""}`);
    });
  }
  const parities = loadData("parities.json");
  if (parities?.parities) {
    parts.push("== PARIDADES ==");
    Object.values(parities.parities).forEach((p: any) => {
      if (p?.value != null) parts.push(`${p.name}: ${p.value} ${p.unit || ""} | z=${p.z_score ?? "?"} | ${p.signal || ""}`);
    });
  }

  // ── BLOCO 8: CONAB + FISICO BR (Rodrigo Batista) ──
  const conab = loadData("conab_data.json");
  if (conab?.safras) {
    parts.push("== CONAB SAFRA ==");
    Object.entries(conab.safras).forEach(([crop, d]: any) => {
      if (d.area_ha || d.producao_ton) parts.push(`${crop}: area=${d.area_ha}ha | prod=${d.producao_ton}t | prod/ha=${d.produtividade || "?"}kg/ha`);
    });
  }
  const physBr = loadData("physical_br.json");
  if (physBr) {
    const products = physBr.products || physBr;
    if (typeof products === "object") {
      const lines: string[] = [];
      Object.entries(products).forEach(([k, v]: any) => {
        if (v?.price) lines.push(`${v.label || k}: R$${v.price} ${v.unit || ""}${v.change_pct != null ? ` (${v.change_pct >= 0 ? "+" : ""}${v.change_pct}%)` : ""}`);
      });
      if (lines.length) { parts.push("== FISICO BR (CEPEA) =="); lines.forEach(l => parts.push(l)); }
    }
  }

  // ── BLOCO 9: CLIMA / WEATHER (todos) ──
  const weather = loadData("weather_agro.json");
  if (weather) {
    if (weather.enso) parts.push(`== CLIMA == ENSO: ${weather.enso.status} (ONI=${weather.enso.oni_value})`);
    if (weather.regions) {
      Object.values(weather.regions).forEach((r: any) => {
        (r.alerts || []).forEach((a: any) => parts.push(`Alerta ${r.label}: ${a.type} ${a.severity}`));
      });
    }
  }

  // ── BLOCO 10: NOTICIAS + CALENDARIO (Henrik Larsson, todos) ──
  const news = loadData("news.json");
  if (news?.items?.length) {
    parts.push("== NOTICIAS RECENTES ==");
    news.items.slice(0, 8).forEach((n: any) => parts.push(`- ${n.title} [${n.source || "?"}]`));
  }
  const calendar = loadData("calendar.json");
  if (calendar?.events?.length) {
    const today = new Date().toISOString().slice(0, 10);
    const upcoming = calendar.events.filter((e: any) => e.date >= today).slice(0, 8);
    if (upcoming.length) {
      parts.push("== CALENDARIO ECONOMICO (proximos 7 dias) ==");
      upcoming.forEach((e: any) => parts.push(`${e.date}: ${e.name || e.event} — impacto: ${e.impact || "N/A"}`));
    }
  }

  // ── BLOCO 11: FISICO INTERNACIONAL + ARGENTINA (Raj, Maria) ──
  const physIntl = loadData("physical_intl.json");
  if (physIntl) {
    const keys = ["soy_fob_arg", "corn_fob_arg", "wheat_fob_arg", "soyoil_fob_arg", "soymeal_fob_arg", "soy_fob_gulf", "corn_fob_gulf"];
    const intlLines: string[] = [];
    for (const k of keys) {
      if (physIntl[k]) intlLines.push(`${k}: $${physIntl[k].price || physIntl[k].value || physIntl[k]} ${physIntl[k].unit || ""}`);
    }
    if (physIntl.international) {
      Object.entries(physIntl.international).forEach(([k, v]: any) => {
        if (v?.price) intlLines.push(`${v.label || k}: $${v.price} ${v.unit || ""}`);
      });
    }
    if (intlLines.length) { parts.push("== FISICO INTERNACIONAL =="); intlLines.forEach(l => parts.push(l)); }
  }
  const bilateral = loadData("bilateral_indicators.json");
  if (bilateral?.summary) {
    parts.push("== BILATERAL BR vs EUA ==");
    if (bilateral.lcs?.status === "OK") parts.push(`LCS: Spread $${bilateral.lcs.spread_usd_mt?.toFixed(2)}/MT | Competitivo: ${bilateral.lcs.competitive_origin} | FOB BR=$${bilateral.lcs.br_fob?.toFixed(0)} EUA=$${bilateral.lcs.us_fob?.toFixed(0)}`);
    if (bilateral.bci?.status === "OK") parts.push(`BCI: Score=${bilateral.bci.bci_score} (${bilateral.bci.bci_signal})`);
  }

  // ── BLOCO 12: COMMODITY DNA + DAILY READING (Chairman) ──
  const dna = loadData("commodity_dna.json");
  if (dna?.commodities) {
    parts.push("== COMMODITY DNA (sinais atuais) ==");
    Object.entries(dna.commodities).forEach(([sym, data]: any) => {
      const top = data.drivers_ranked?.[0];
      parts.push(`${sym}: ${data.composite_signal} | #1=${top?.driver || "?"}: ${(top?.signal || "?").slice(0, 60)}`);
    });
  }
  const daily = loadData("daily_reading.json");
  if (daily?.narrative) parts.push(`== LEITURA DO DIA ==\n${daily.narrative.substring(0, 600)}`);

  // ── BLOCO 13: SKILLS OUTPUT (Ana Lima, todos) ──
  const timing = loadPipeline("entry_timing.json");
  if (timing) {
    parts.push("== ENTRY TIMING ==");
    if (timing.best_opportunity) { const b = timing.best_opportunity; parts.push(`Best: ${b.sym} ${b.direction} Grade=${b.grade} (${b.score}/${b.max_score})`); }
    parts.push(`Blocked: ${timing.blocked_count}`);
  }
  const theta = loadPipeline("theta_calendar.json");
  if (theta) {
    parts.push("== THETA CALENDAR ==");
    parts.push(`Posicoes: ${theta.positions} | Decisao: ${theta.in_decision_window} | Acao: ${theta.action_needed}`);
    (theta.timeline || []).slice(0, 8).forEach((t: any) => parts.push(`  ${t.sym} ${t.contract}: DTE=${t.dte || "?"} ${t.phase} delta=${t.delta ?? "?"} theta=${t.theta ?? "?"}`));
  }
  const opp = loadPipeline("opportunity_scan.json");
  if (opp) {
    parts.push("== OPPORTUNITY SCANNER ==");
    if (opp.best_opportunity) parts.push(`Best: ${opp.best_opportunity.sym} ${opp.best_opportunity.direction} Grade=${opp.best_opportunity.grade}`);
    parts.push(`Top PUTs: ${(opp.top_puts || []).slice(0, 3).map((p: any) => `${p.sym}(${p.grade})`).join(", ")}`);
    parts.push(`Top CALLs: ${(opp.top_calls || []).slice(0, 3).map((c: any) => `${c.sym}(${c.grade})`).join(", ")}`);
    parts.push(`Blocked: ${(opp.blocked || []).length}`);
  }
  const stress = loadPipeline("stress_test.json");
  if (stress) {
    parts.push("== STRESS TEST ==");
    parts.push(`Risk Level: ${stress.risk_level}`);
    if (stress.most_vulnerable) parts.push(`Most vulnerable: ${stress.most_vulnerable.sym} ${stress.most_vulnerable.contract} — worst $${stress.most_vulnerable.worst_loss?.toLocaleString()} (${stress.most_vulnerable.vuln_pct}%)`);
    (stress.risk_notes || []).forEach((n: string) => parts.push(`  ! ${n}`));
  }
  const vega = loadPipeline("vega_monitor.json");
  if (vega) {
    parts.push("== VEGA MONITOR ==");
    parts.push(`Regime: ${vega.regime || "?"} | VIX: ${vega.vix?.value || "?"} (${vega.vix?.level || "?"})`);
    parts.push(`Opportunities: ${vega.opportunities?.length || 0} | Reserve: $${vega.reserve?.deployable?.toLocaleString() || "?"}`);
  }
  const cross = loadPipeline("cross_analysis.json");
  if (cross?.key_findings) {
    parts.push("== CROSS ANALYSIS ==");
    const kf = cross.key_findings;
    parts.push(`Roll: ${kf.roll_impact} | Best combo: ${kf.best_combo} (WR=${kf.best_combo_wr}%) | Most predictable: ${kf.most_predictable} | Best month: ${kf.best_month}`);
  }

  // ── BLOCO 14: STOCKS / PSD / SAZONALIDADE ──
  const sw = loadData("stocks_watch.json");
  if (sw?.commodities) {
    parts.push("== ESTOQUES (stocks_watch) ==");
    Object.entries(sw.commodities).forEach(([sym, d]: any) => {
      if (d.stock_current != null) {
        const dev = d.stock_avg ? ((d.stock_current - d.stock_avg) / d.stock_avg * 100).toFixed(1) : "?";
        parts.push(`${sym}: ${d.stock_current} ${d.stock_unit || ""} | avg5y=${d.stock_avg || "?"} | dev=${dev}% | ${d.state || "?"}`);
      }
    });
  }
  const psd = loadData("psd_ending_stocks.json");
  if (psd?.commodities) {
    parts.push("== PSD USDA ==");
    Object.entries(psd.commodities).forEach(([sym, d]: any) => {
      if (d.current != null) parts.push(`${sym}: ${d.current} ${d.unit || ""} | avg5y=${d.avg_5y || "?"} | dev=${d.deviation != null ? (d.deviation > 0 ? "+" : "") + d.deviation.toFixed(1) + "%" : "?"}`);
    });
  }
  const season = loadData("seasonality.json");
  if (season) {
    const months = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"];
    const cm = new Date().getMonth();
    parts.push(`== SAZONALIDADE ${months[cm]} ==`);
    Object.keys(season).sort().forEach(sym => {
      const s = season[sym]; if (!s?.monthly_returns) return;
      const v = s.monthly_returns[cm];
      const avg = typeof v === "number" ? v : v?.avg ?? null;
      if (avg != null) parts.push(`${sym}: ${avg >= 0 ? "+" : ""}${avg.toFixed(2)}%`);
    });
  }

  return parts.join("\n");
}

// ═══════════════════════════════════════════════════════
// COMPACT SNAPSHOT (for specialists — respects rate limits)
// ═══════════════════════════════════════════════════════
function buildCompactSnapshot(): string {
  const p: string[] = [];

  // Portfolio summary (not full positions)
  const port = loadData("ibkr_portfolio.json");
  if (port?.summary) {
    p.push(`PORTFOLIO: NetLiq=$${port.summary.NetLiquidation} Cash=$${port.summary.TotalCashValue} UnrPnL=$${port.summary.UnrealizedPnL}`);
    const fop = (port.positions || []).filter((x: any) => x.sec_type === "FOP" || x.sec_type === "FUT");
    const syms = [...new Set(fop.map((x: any) => x.symbol))].sort();
    p.push(`Positions: ${syms.join(", ")} (${fop.length} legs)`);
  }

  // IV one-liner per underlying
  const oc = loadData("options_chain.json");
  if (oc?.underlyings) {
    p.push("IV:");
    Object.entries(oc.underlyings).forEach(([sym, d]: any) => {
      const iv = d.iv_rank?.current_iv ? (d.iv_rank.current_iv * 100).toFixed(0) + "%" : "?";
      const term = d.term_structure?.structure || "?";
      p.push(`  ${sym}:IV=${iv} T=${term}`);
    });
  }

  // COT extremes only
  const cot = loadData("cot.json");
  if (cot?.commodities) {
    const extremes: string[] = [];
    Object.entries(cot.commodities).forEach(([sym, d]: any) => {
      const idx = d.disaggregated?.cot_index;
      if (idx != null) extremes.push(`${sym}=${idx.toFixed(0)}`);
    });
    if (extremes.length) p.push(`COT: ${extremes.join(" ")}`);
  }

  // Macro one-liner
  const macro = loadData("macro_indicators.json");
  if (macro) {
    const parts: string[] = [];
    if (macro.vix) parts.push(`VIX=${macro.vix.value}`);
    if (macro.sp500) parts.push(`SP500=${macro.sp500.value}`);
    if (macro.treasury_10y) parts.push(`10Y=${macro.treasury_10y.value}%`);
    if (parts.length) p.push(`MACRO: ${parts.join(" ")}`);
  }

  // Spreads one-liner
  const spreads = loadData("spreads.json");
  if (spreads?.spreads) {
    const sp: string[] = [];
    Object.entries(spreads.spreads).forEach(([k, v]: any) => sp.push(`${k}:z=${v.zscore_1y}`));
    p.push(`SPREADS: ${sp.join(" ")}`);
  }

  // Stocks deviations
  const sw = loadData("stocks_watch.json");
  if (sw?.commodities) {
    const stk: string[] = [];
    Object.entries(sw.commodities).forEach(([sym, d]: any) => {
      if (d.stock_current != null && d.stock_avg) {
        const dev = ((d.stock_current - d.stock_avg) / d.stock_avg * 100).toFixed(0);
        stk.push(`${sym}=${dev}%`);
      }
    });
    if (stk.length) p.push(`ESTOQUES_DEV: ${stk.join(" ")}`);
  }

  // DNA signals
  const dna = loadData("commodity_dna.json");
  if (dna?.commodities) {
    const sigs: string[] = [];
    Object.entries(dna.commodities).forEach(([sym, d]: any) => sigs.push(`${sym}:${d.composite_signal}`));
    p.push(`DNA: ${sigs.join(" ")}`);
  }

  return p.join("\n");
}

// ═══════════════════════════════════════════════════════
// WEIGHT ENGINE (pesos dinamicos por fator)
const WEIGHT_ENGINE = `WEIGHT ENGINE \u2014 PESOS DIN\u00c2MICOS AgriMacro. Cada fator recebe peso proporcional a INTENSIDADE (z-score), HORIZONTE (prazo), REGIME (tendencia vs lateral). COT: >85/<15=40%, 70-85/15-30=25%, 30-70=10%, dsem>20K=+10%. STU: z>2=35%, z1-2=20%, z<1=10%. CLIMA: seca ativa(<5mm)=35%, moderada=20%, normal=5%, ENSO ativo=+10%. SAZONALIDADE: desvio>15%=25%, 5-15%=15%, <5%=5%. GEOPOLITICA: evento ativo=40%, subsidio=25%, nada=5%. EXPORTACOES: pace<80%/>110%=25%, basis extremo=20%, normal=10%. MARGENS: z>2=30%, z1-2=20%, z<1=10%. DTE SHORT: <10d=50%(sobrepoe tudo), 10-20d=35%, 20-30d=20%, >30d=5%. HORIZONTE: 1-4sem=COT40+Clima30+Geo20+Saz10. 1-3m=STU35+Export25+COT20+Margens20. 6-12m=Estrutural40+COP30+Ciclo30. INSTRUCAO: liste PESOS [commodity] hoje: COT=X% | STU=X% | Clima=X% | Geo=X% antes de analisar. Ordem decrescente de peso.`;

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
// BRIEFINGS PROFISSIONAIS (background real dos conselheiros)
// ═══════════════════════════════════════════════════════
const BRIEFING_DALIO = `RAY DALIO (Head \u2014 Bridgewater): Fundou a Bridgewater em 1975. Framework All Weather + Risk Parity. Economia = maquina com ciclos de divida curtos (5-8 anos) e longos (75-100 anos). METODOLOGIA: 1) Identificar ciclo de divida. 2) Risk Parity: risco balanceado ou concentrado? 3) Stress test radical. 4) Beautiful vs Ugly Deleveraging? 5) Correlacoes quebradas, divida/GDP, bancos centrais.`;

const BRIEFING_CARLOS = `CARLOS MERA (Rabobank Head of ACMR): 7+ anos em trading fisico, +1.000 fazendas visitadas. Frase real (2026): "Agriculture is no longer playing by supply-and-demand rules \u2014 it's also playing by geopolitical ones." METODOLOGIA Rabobank: 1) Geopolitica primeiro: tarifas sobrepoe fundamentos desde 2025. 2) WASDE vs expectativa Bloomberg, nao vs relatorio anterior. 3) STU z-score, nao nivel absoluto. 4) Subsidios como distorcao. 5) Qual governo distorce este mercado?`;

const BRIEFING_HENRIK = `HENRIK LARSSON (ex-Brevan Howard): $35B AUM, retorno 99% em 2020. Filosofia: trades convexos (upside >> downside), hedge sempre, stress testing 150+ fatores. METODOLOGIA: 1) Convexidade: short call sem hedge = violacao. 2) Tail risk antes de direcao. 3) Correlacoes que rompem em crise. 4) Regime change: o que muda se Fed muda? Se China fecha? Se Hormuz fecha 30 dias?`;

const BRIEFING_DAVID = `DAVID KOWALSKI (COT, metodologia Larry Williams): Baseado em "Trade Stocks and Commodities with the Insiders" (Wiley). METODOLOGIA: 1) Commercials = Smart Money (produtores reais). 2) Non-Commercials = trend followers tardios. COT Index >80 = sobrecomprado. 3) Lag: graos 2-3 semanas, energias 3-4 semanas, metais 1-2 semanas. CHECKLIST: COT 3 janelas consenso/divergencia, variacao semanal, OI crescendo/caindo.`;

const BRIEFING_ANA = `ANA LIMA (ex-Cargill, maior trader fisico do mundo): METODOLOGIA: 1) DTE x distancia x quantidade = urgencia. 2) Exposicao maxima REAL: GF=500lbs, CL=1000bbl, ZL=600bu, ZC/ZS=5000bu, SI=5000oz, GC=100oz. 3) Roll yield: backwardation favorece short calls, contango penaliza. 4) IV Rank <20% = vender premium arriscado. >80% = favoravel. 5) Theta/exposicao <0.1% = submunera risco. RANKING: DTE<10+dist<$5=EMERGENCIA, DTE<20+dist<$10=CRITICO, DTE<30+dist<$20=ATENCAO.`;

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
- M\u00e1ximo 1400 palavras no total.

GUARDRAILS DE QUALIDADE:
- Nunca usar pre\u00e7os, datas ou n\u00fameros que n\u00e3o estejam no snapshot. Se o snapshot diz CL=$92.15, usar $92.15 \u2014 n\u00e3o inventar outro valor.
- Se um campo do snapshot est\u00e1 ausente ou "?", escrever "N/A" \u2014 n\u00e3o estimar.
- Dist\u00e2ncia strike vs spot: calcular a partir dos dados reais do snapshot (local_symbol cont\u00e9m o strike, und_price cont\u00e9m o spot). Se Ana Lima ou Carlos Lima reportarem dist\u00e2ncia > $50 em posi\u00e7\u00e3o com spot e strike vis\u00edveis no snapshot, o n\u00famero est\u00e1 errado \u2014 recalcular.
- Data do relat\u00f3rio = data do snapshot (generated_at ou \u00faltima data de pre\u00e7o), nunca data de treinamento do modelo.
- N\u00e3o repetir os briefings dos especialistas no relat\u00f3rio final \u2014 sintetizar.
- Se dois conselheiros contradizem entre si, o Chairman deve explicitar a contradi\u00e7\u00e3o e justificar qual lado prevalece.`;

const QUICK_SYSTEM = `Voce e o analista de risco do AgriMacro. Faca uma analise RAPIDA (max 300 palavras) do portfolio atual.
Foco em: 1) Posicoes que precisam de acao 2) Melhor oportunidade do dia 3) Risco principal.
Portugues brasileiro, direto e acionavel. Sem introducao.`;

// ═══════════════════════════════════════════════════════
// SPECIALISTS (12 domain experts for full mode)
// ═══════════════════════════════════════════════════════
const SPECIALISTS: { name: string; role: string; system: string }[] = [
  { name: "Carlos Mera", role: "Graos Bear Case (Rabobank)",
    system: BRIEFING_CARLOS + " Foco: ZC, ZS, ZW, KE, ZM, ZL. Analise AT (curva forward, COT, momentum) e AF (WASDE, STU, safra BR) usando o snapshot. Contradicoes ANTES de suportes. Max 150 palavras." },
  { name: "Felipe Hernandez", role: "Macro Estruturalista (Oxford Economics)",
    system: "Voce e Felipe Hernandez, economista da Oxford Economics. Foco: DXY, BRL/USD, juros, VIX, correlacoes macro-commodities. Analise AT (regime de vol, curvas) e AF (Selic, fertilizantes lag 6-12m). Contradicoes ANTES. Max 150 palavras." },
  { name: "Rodrigo Batista", role: "Fisico Brasil Bull Case",
    system: "Voce e Rodrigo Batista, trader de fisico no Brasil. Foco: CEPEA, basis Paranagua, boi gordo, soja fisica. Analise AT (basis spot vs futuro) e AF (Feedlot Margin LE*10-GF*7.5-ZC*50, crush). Contradicoes ANTES. Max 150 palavras." },
  { name: "Henrik Larsson", role: "Macro Outsider (ex-Brevan Howard)",
    system: BRIEFING_HENRIK + " Foco: tail risk, geopolitica, CL, open interest extremos, correlacoes que rompem em crise. Analise AT (OI anormal) e AF (riscos geopoliticos). Qual black swan as posicoes nao cobrem? Max 150 palavras." },
  { name: "Ana Lima", role: "Risk Manager (ex-Cargill)",
    system: BRIEFING_ANA + " Para CADA posicao do portfolio: DTE, distancia do strike vs spot, exposicao maxima em $. Posicoes com PnL > -200% do credito = PERDA MAXIMA, prioridade. Short calls = MENCIONAR risco assignment. Rankeie por urgencia. Max 200 palavras." },
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
    system: BRIEFING_DAVID + " Analise COT Index de TODAS as commodities. COT > 80 = CROWDED LONG (reversao). COT < 20 = CROWDED SHORT. 3 janelas: 156w/52w/26w. Delta semanal. Max 120 palavras." },
];

const DALIO_SYSTEM = BRIEFING_DALIO + ` Recebeu briefings de 12 especialistas sobre um portfolio de opcoes de commodities. Sintetize os pontos de CONVERGENCIA e DIVERGENCIA entre os especialistas. Identifique o risco sistemico que ninguem mencionou. Confluencia AT+AF: CONVERGENTE ou DIVERGENTE. Se DIVERGENTE = recomendar sizing 30-50% menor. Max 200 palavras. Portugues brasileiro.`;

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
    system: spec.system + "\n\n" + WEIGHT_ENGINE + "\n\n" + AT_FRAMEWORK + "\n\n" + AF_FRAMEWORK + "\n\n" + context,
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
    system: CHAIRMAN_SYSTEM + "\n\n" + COUNCIL_SYSTEM + "\n\n" + AT_FRAMEWORK + "\n\n" + AF_FRAMEWORK + "\n\n" + BRIEFING_DALIO + "\n" + BRIEFING_CARLOS + "\n" + BRIEFING_HENRIK + "\n" + BRIEFING_DAVID + "\n" + BRIEFING_ANA + "\n\n" + WEIGHT_ENGINE,
    messages: [{ role: "user", content:
      `BRIEFINGS (12 especialistas):\n${briefings}\n\n` +
      `${dalio}\n\n${devil}\n\n` +
      `SNAPSHOT DE DADOS:\n${context}`
    }],
  });
  return res.content.filter((c: any) => c.type === "text").map((c: any) => c.text).join("");
}

// ═══════════════════════════════════════════════════════
// JOB STORE (in-memory, per-process)
// ═══════════════════════════════════════════════════════
interface CouncilJob {
  status: "running" | "complete" | "error";
  stage: string;
  detail: string;
  response?: string;
  error?: string;
  snapshot_size?: number;
  started_at: string;
  completed_at?: string;
}
const jobs: Record<string, CouncilJob> = {};

async function runFullCouncil(jobId: string) {
  const job = jobs[jobId];
  try {
    const client = new Anthropic({ apiKey: getKey() });
    const snapshot = buildSnapshot();
    const compact = buildCompactSnapshot();

    const BATCH_SIZE = 4;
    const BATCH_DELAY_MS = 12000;
    const allReports: string[] = [];

    // Stage 1: 12 specialists in batches
    for (let i = 0; i < SPECIALISTS.length; i += BATCH_SIZE) {
      const batch = SPECIALISTS.slice(i, i + BATCH_SIZE);
      const batchNum = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(SPECIALISTS.length / BATCH_SIZE);
      job.stage = "specialists";
      job.detail = `Especialistas batch ${batchNum}/${totalBatches} (${batch.map(s => s.name.split(" ")[0]).join(", ")})...`;

      const batchResults = await Promise.all(
        batch.map(spec => runSpecialist(client, spec, compact))
      );
      allReports.push(...batchResults);

      if (i + BATCH_SIZE < SPECIALISTS.length) {
        await new Promise(resolve => setTimeout(resolve, BATCH_DELAY_MS));
      }
    }
    const briefings = allReports.join("\n\n");

    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY_MS));

    // Stage 2: Dalio
    job.stage = "heads";
    job.detail = "Ray Dalio sintetizando...";
    const dalio = await runHead(client, DALIO_SYSTEM, "RAY DALIO (Sintese)", briefings, compact);

    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY_MS));

    // Stage 2b: Devil
    job.stage = "heads";
    job.detail = "Advogado do Diabo atacando...";
    const devil = await runHead(client, DEVIL_SYSTEM, "ADVOGADO DO DIABO", briefings, compact);

    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY_MS));

    // Stage 3: Chairman
    job.stage = "chairman";
    job.detail = "Chairman produzindo relatorio final...";
    const chairman = await runChairman(client, briefings, dalio, devil, snapshot);

    // Done
    job.status = "complete";
    job.stage = "complete";
    job.detail = "";
    job.response = chairman;
    job.snapshot_size = snapshot.length;
    job.completed_at = new Date().toISOString();
  } catch (err: any) {
    job.status = "error";
    job.stage = "error";
    job.error = err.message?.slice(0, 500) || "Unknown error";
    job.completed_at = new Date().toISOString();
  }
}

// ═══════════════════════════════════════════════════════
// POST HANDLER (starts job or runs quick)
// ═══════════════════════════════════════════════════════
export async function POST(req: NextRequest) {
  try {
    const { mode } = await req.json();

    // ── QUICK MODE: single call, returns immediately ──
    if (mode === "quick") {
      const client = new Anthropic({ apiKey: getKey() });
      const snapshot = buildSnapshot();
      const response = await client.messages.create({
        model: "claude-sonnet-4-20250514",
        max_tokens: 2048,
        system: QUICK_SYSTEM + "\n\n" + AT_FRAMEWORK + "\n\n" + AF_FRAMEWORK + "\n\n" + snapshot,
        messages: [{ role: "user", content: "Analise rapida do portfolio. O que fazer agora?" }],
      });
      const text = response.content.filter((c: any) => c.type === "text").map((c: any) => c.text).join("");
      return NextResponse.json({ response: text, mode, timestamp: new Date().toISOString(), snapshot_size: snapshot.length });
    }

    // ── FULL MODE: create job, run in background ──
    const jobId = `council_${Date.now()}`;
    jobs[jobId] = {
      status: "running",
      stage: "starting",
      detail: "Iniciando Council v2.2...",
      started_at: new Date().toISOString(),
    };

    // Fire and forget — runs in background
    runFullCouncil(jobId).catch(() => {});

    // Clean old jobs (keep last 5)
    const allIds = Object.keys(jobs).sort();
    if (allIds.length > 5) {
      for (const old of allIds.slice(0, allIds.length - 5)) {
        delete jobs[old];
      }
    }

    return NextResponse.json({ jobId, status: "running" });
  } catch (error: any) {
    console.error("[council] Error:", error.message);
    return NextResponse.json({ error: error.message || "Unknown error" }, { status: 500 });
  }
}

// ═══════════════════════════════════════════════════════
// GET HANDLER (poll job status)
// ═══════════════════════════════════════════════════════
export async function GET(req: NextRequest) {
  const jobId = req.nextUrl.searchParams.get("jobId");
  if (!jobId || !jobs[jobId]) {
    return NextResponse.json({ error: "Job not found" }, { status: 404 });
  }
  const job = jobs[jobId];
  return NextResponse.json({
    jobId,
    status: job.status,
    stage: job.stage,
    detail: job.detail,
    response: job.status === "complete" ? job.response : undefined,
    error: job.status === "error" ? job.error : undefined,
    snapshot_size: job.snapshot_size,
    started_at: job.started_at,
    completed_at: job.completed_at,
  });
}
