import { NextRequest, NextResponse } from "next/server";
import Anthropic from "@anthropic-ai/sdk";
import { readFileSync, writeFileSync, existsSync } from "fs";
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

  // ── BLOCO 6: MACRO GLOBAL (Dr. Wei, Jennifer Bond) ──
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
// FOCUSED SNAPSHOTS (domain-specific for each specialist)
// ═══════════════════════════════════════════════════════
function filterSyms(obj: any, syms: string[]): any {
  if (!obj) return {};
  const out: any = {};
  for (const s of syms) { if (obj[s]) out[s] = obj[s]; }
  return out;
}
function filterPositions(port: any, syms?: string[]): string {
  if (!port?.positions) return "";
  const pos = port.positions.filter((p: any) => (p.sec_type === "FOP" || p.sec_type === "FUT") && (!syms || syms.includes(p.symbol)));
  return pos.map((p: any) => `${p.symbol} ${p.local_symbol}: ${p.position > 0 ? "+" : ""}${p.position} delta=${p.delta ?? "?"} theta=${p.theta ?? "?"} iv=${p.iv ? (p.iv * 100).toFixed(0) + "%" : "?"} avgCost=${p.avg_cost ?? "?"}`).join("\n");
}
function filterCOT(cot: any, syms?: string[]): string {
  if (!cot?.commodities) return "";
  const lines: string[] = [];
  Object.entries(cot.commodities).forEach(([sym, d]: any) => {
    if (syms && !syms.includes(sym)) return;
    const dis = d.disaggregated || {};
    if (dis.cot_index != null) {
      const label = dis.cot_index >= 80 ? "CROWDED_LONG" : dis.cot_index <= 20 ? "CROWDED_SHORT" : "neutral";
      const w52 = dis.cot_index_52w != null ? ` 52w=${dis.cot_index_52w.toFixed(0)}` : "";
      const mm = dis.latest?.managed_money_net != null ? ` MM=${dis.latest.managed_money_net}` : "";
      lines.push(`${sym}: idx=${dis.cot_index.toFixed(0)} ${label}${w52}${mm}`);
    }
  });
  return lines.join("\n");
}
function filterIV(oc: any, syms?: string[]): string {
  if (!oc?.underlyings) return "";
  const lines: string[] = [];
  Object.entries(oc.underlyings).forEach(([sym, d]: any) => {
    if (syms && !syms.includes(sym)) return;
    const ivr = d.iv_rank || {};
    const sk = d.skew || {};
    const ts = d.term_structure || {};
    lines.push(`${sym}: IV=${ivr.current_iv ? (ivr.current_iv * 100).toFixed(1) + "%" : "?"} Rank=${ivr.rank_52w != null ? ivr.rank_52w.toFixed(0) + "%" : "?"} Skew=${sk.skew_pct != null ? sk.skew_pct + "%" : "?"} Term=${ts.structure || "?"}`);
  });
  return lines.join("\n");
}
function filterNews(news: any, keywords: string[]): string {
  if (!news?.items?.length) return "";
  const kw = keywords.map(k => k.toLowerCase());
  const matched = news.items.filter((n: any) => kw.some(k => (n.title || "").toLowerCase().includes(k)));
  return matched.slice(0, 5).map((n: any) => `- ${n.title} [${n.source || "?"}]`).join("\n");
}
function macroBlock(): string {
  const p: string[] = [];
  const macro = loadData("macro_indicators.json");
  if (macro) {
    if (macro.vix) p.push(`VIX=${macro.vix.value}(${macro.vix.level})`);
    if (macro.sp500) p.push(`SP500=${macro.sp500.value}`);
    if (macro.treasury_10y) p.push(`10Y=${macro.treasury_10y.value}%`);
  }
  const bcb = loadData("bcb_data.json");
  if (bcb) {
    const brl = bcb.brl_usd; if (Array.isArray(brl) && brl.length) p.push(`BRL/USD=${brl[brl.length - 1].value}`);
    const selic = bcb.selic_meta; if (Array.isArray(selic) && selic.length) p.push(`Selic=${selic[selic.length - 1].value}%`);
  }
  const fw = loadData("fedwatch.json");
  if (fw?.probabilities) p.push(`Fed:${fw.market_expectation} hold=${fw.probabilities.hold}%`);
  return p.join(" | ");
}

function buildFocusedSnapshot(domain: string): string {
  const p: string[] = [];
  const port = loadData("ibkr_portfolio.json");
  const oc = loadData("options_chain.json");
  const cot = loadData("cot.json");
  const news = loadData("news.json");
  const spreads = loadData("spreads.json");
  const sw = loadData("stocks_watch.json");
  const season = loadData("seasonality.json");
  const physBr = loadData("physical_br.json");
  const conab = loadData("conab_data.json");
  const bilateral = loadData("bilateral_indicators.json");
  const eia = loadData("eia_data.json");
  const weather = loadData("weather_agro.json");
  const calendar = loadData("calendar.json");
  const theta = loadPipeline("theta_calendar.json");
  const stress = loadPipeline("stress_test.json");

  const GRAINS = ["ZC", "ZS", "ZW", "ZM", "ZL", "KE"];
  const ENERGY = ["CL", "NG"];
  const METALS = ["GC", "SI"];
  const SOFTS = ["KC", "CC", "SB", "CT"];
  const LIVESTOCK = ["LE", "GF", "HE"];
  const activeSym: string[] = Array.from(new Set((port?.positions || []).filter((x: any) => x.sec_type === "FOP" || x.sec_type === "FUT").map((x: any) => x.symbol))) as string[];

  switch (domain) {
    case "grains_bear": {
      p.push("== GRAINS DATA ==");
      p.push(filterCOT(cot, GRAINS));
      p.push(filterIV(oc, GRAINS));
      if (sw?.commodities) GRAINS.forEach(s => { const d: any = sw.commodities[s]; if (d?.stock_current) p.push(`STK ${s}: ${d.stock_current} ${d.stock_unit || ""} dev=${d.stock_avg ? ((d.stock_current - d.stock_avg) / d.stock_avg * 100).toFixed(0) + "%" : "?"}`); });
      if (conab?.safras) Object.entries(conab.safras).forEach(([c, d]: any) => { if (d.producao_ton) p.push(`CONAB ${c}: prod=${d.producao_ton}t`); });
      if (season) GRAINS.forEach(s => { const v = season[s]?.monthly_returns?.[new Date().getMonth()]; if (v != null) p.push(`Saz ${s}: ${typeof v === "number" ? v.toFixed(2) : v.avg?.toFixed(2) || "?"}%`); });
      if (bilateral?.summary) p.push(`Bilateral: ${JSON.stringify(bilateral.summary).slice(0, 200)}`);
      const gNews = filterNews(news, ["tariff", "grain", "Ukraine", "China", "soy", "corn", "wheat"]);
      if (gNews) p.push("NEWS:\n" + gNews);
      break;
    }
    case "macro_structural": {
      p.push("== MACRO ==");
      p.push(macroBlock());
      if (eia) { const fields = ["wti_spot", "natural_gas_spot", "diesel_price"]; fields.forEach(f => { if (eia[f]) p.push(`EIA ${f}: ${eia[f].value} ${eia[f].unit || ""}`); }); }
      if (spreads?.spreads) Object.entries(spreads.spreads).forEach(([k, v]: any) => p.push(`Spread ${k}: z=${v.zscore_1y} ${v.regime}`));
      const mNews = filterNews(news, ["Fed", "dollar", "inflation", "rate", "China", "GDP", "tariff"]);
      if (mNews) p.push("NEWS:\n" + mNews);
      break;
    }
    case "physical_brazil": {
      p.push("== FISICO BR ==");
      p.push(macroBlock());
      if (physBr) { const products = physBr.products || physBr; Object.entries(products).forEach(([k, v]: any) => { if (v?.price) p.push(`CEPEA ${v.label || k}: R$${v.price}`); }); }
      if (conab?.safras) Object.entries(conab.safras).forEach(([c, d]: any) => { if (d.producao_ton) p.push(`CONAB ${c}: ${d.producao_ton}t`); });
      if (bilateral?.lcs) p.push(`Bilateral LCS: spread=$${bilateral.lcs.spread_usd_mt?.toFixed(2)} comp=${bilateral.lcs.competitive_origin}`);
      if (season) ["ZS", "ZC"].forEach(s => { const v = season[s]?.monthly_returns?.[new Date().getMonth()]; if (v != null) p.push(`Saz ${s}: ${typeof v === "number" ? v.toFixed(2) : "?"}%`); });
      break;
    }
    case "tail_risk": {
      p.push("== TAIL RISK ==");
      p.push(macroBlock());
      if (news?.items) p.push("NEWS FULL:\n" + news.items.slice(0, 8).map((n: any) => `- ${n.title}`).join("\n"));
      p.push(filterCOT(cot));  // ALL commodities
      if (port) p.push("PORTFOLIO:\n" + filterPositions(port));
      if (calendar?.events) { const today = new Date().toISOString().slice(0, 10); const up = calendar.events.filter((e: any) => e.date >= today).slice(0, 6); if (up.length) p.push("CALENDAR:\n" + up.map((e: any) => `${e.date}: ${e.name || e.event}`).join("\n")); }
      break;
    }
    case "portfolio_risk":
    case "portfolio_full": {
      p.push("== PORTFOLIO FULL ==");
      if (port?.summary) p.push(`NetLiq=$${port.summary.NetLiquidation} Cash=$${port.summary.TotalCashValue} BuyPow=$${port.summary.BuyingPower}`);
      if (port) p.push(filterPositions(port));
      if (port?.portfolio_greeks) { const pg = port.portfolio_greeks; p.push(`Greeks: delta=${pg.total_delta} theta=$${pg.total_theta}/dia vega=${pg.total_vega}`); }
      const tbills = (port?.positions || []).filter((p: any) => (p.symbol || "").includes("US-T") || (p.local_symbol || "").includes("IBCID"));
      if (tbills.length) p.push(`T-Bills: $${tbills.reduce((s: number, t: any) => s + Math.abs(t.market_value || 0), 0).toFixed(0)}`);
      if (theta?.timeline) p.push("THETA:\n" + theta.timeline.map((t: any) => `${t.sym} ${t.contract}: DTE=${t.dte || "?"} ${t.phase}`).join("\n"));
      if (stress) { p.push(`Stress: ${stress.risk_level}`); if (stress.most_vulnerable) p.push(`Vulnerable: ${stress.most_vulnerable.sym} worst=$${stress.most_vulnerable.worst_loss}`); }
      p.push(filterCOT(cot, activeSym));
      break;
    }
    case "cot_specialist": {
      p.push("== COT FULL ==");
      p.push(filterCOT(cot));  // ALL commodities, full detail
      if (season) { p.push("SEASONALITY:"); Object.keys(season).sort().forEach(s => { const v = season[s]?.monthly_returns?.[new Date().getMonth()]; if (v != null) p.push(`${s}:${typeof v === "number" ? v.toFixed(1) : "?"}%`); }); }
      const cotExtreme = Object.entries(cot?.commodities || {}).filter(([, d]: any) => { const idx = d.disaggregated?.cot_index; return idx != null && (idx >= 80 || idx <= 20); }).map(([s]) => s);
      if (cotExtreme.length && port) p.push("POSITIONS w/ extreme COT:\n" + filterPositions(port, cotExtreme));
      break;
    }
    case "energy": {
      p.push("== ENERGY ==");
      p.push(filterIV(oc, ENERGY));
      p.push(filterCOT(cot, ENERGY));
      if (eia) Object.entries(eia).forEach(([k, v]: any) => { if (v?.value) p.push(`EIA ${k}: ${v.value} ${v.unit || ""}`); });
      if (port) p.push("CL/NG positions:\n" + filterPositions(port, ENERGY));
      const eNews = filterNews(news, ["oil", "gas", "OPEC", "Hormuz", "energy", "refinery", "barrel"]);
      if (eNews) p.push("NEWS:\n" + eNews);
      break;
    }
    case "metals": {
      p.push("== METALS ==");
      p.push(filterIV(oc, METALS));
      p.push(filterCOT(cot, METALS));
      p.push(macroBlock());
      if (port) p.push("GC/SI positions:\n" + filterPositions(port, METALS));
      const mNews = filterNews(news, ["gold", "silver", "Fed", "dollar", "inflation", "safe-haven"]);
      if (mNews) p.push("NEWS:\n" + mNews);
      break;
    }
    case "softs": {
      p.push("== SOFTS ==");
      p.push(filterIV(oc, SOFTS));
      p.push(filterCOT(cot, SOFTS));
      if (sw?.commodities) SOFTS.forEach(s => { const d: any = sw.commodities[s]; if (d?.stock_current) p.push(`STK ${s}: ${d.stock_current} dev=${d.stock_avg ? ((d.stock_current - d.stock_avg) / d.stock_avg * 100).toFixed(0) + "%" : "?"}`); });
      if (physBr) { const products = physBr.products || physBr; ["cafe", "acucar", "algodao", "etanol"].forEach(k => { Object.entries(products).forEach(([pk, v]: any) => { if (pk.includes(k) && v?.price) p.push(`CEPEA ${v.label || pk}: R$${v.price}`); }); }); }
      const sNews = filterNews(news, ["cocoa", "coffee", "sugar", "cotton", "West Africa", "Brazil", "drought"]);
      if (sNews) p.push("NEWS:\n" + sNews);
      break;
    }
    case "livestock": {
      p.push("== LIVESTOCK ==");
      p.push(filterIV(oc, LIVESTOCK));
      p.push(filterCOT(cot, LIVESTOCK));
      if (spreads?.spreads) { ["feedlot", "cattle_crush"].forEach(k => { const v: any = spreads.spreads[k]; if (v) p.push(`Spread ${k}: z=${v.zscore_1y} ${v.regime}`); }); }
      if (sw?.commodities) LIVESTOCK.forEach(s => { const d: any = sw.commodities[s]; if (d?.stock_current) p.push(`STK ${s}: ${d.stock_current}`); });
      if (physBr) { const products = physBr.products || physBr; Object.entries(products).forEach(([k, v]: any) => { if (k.includes("boi") && v?.price) p.push(`CEPEA ${v.label || k}: R$${v.price}`); }); }
      if (port) p.push("GF/LE/HE positions:\n" + filterPositions(port, LIVESTOCK));
      if (season) LIVESTOCK.forEach(s => { const v = season[s]?.monthly_returns?.[new Date().getMonth()]; if (v != null) p.push(`Saz ${s}: ${typeof v === "number" ? v.toFixed(2) : "?"}%`); });
      break;
    }
    case "options_vol": {
      p.push("== OPTIONS/VOL ==");
      p.push(filterIV(oc));  // ALL underlyings
      p.push(macroBlock());  // VIX
      if (port?.portfolio_greeks) { const pg = port.portfolio_greeks; p.push(`Portfolio Greeks: delta=${pg.total_delta} theta=$${pg.total_theta}/dia vega=${pg.total_vega}`); }
      break;
    }
    default:
      return buildCompactSnapshot();
  }

  return p.filter(Boolean).join("\n");
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

const BRIEFING_JENNIFER = `JENNIFER BOND (USDA ERS / Feed Grains Specialist): 30 anos USDA. Membro ICEC para milho no WASDE. Co-autora "Understanding USDA Crop Forecasts". METODOLOGIA: 1) Balanco PSD: area x yield vs usos domesticos (feed, food, ethanol) vs exportacoes vs estoques finais. 2) Crop Progress G/E <60% em julho = risco de revisao bearish de yield. 3) Hog/Corn e Steer/Corn Ratio: ratio alto = pecuaria lucrativa = mais demanda milho. 4) Corn/Soy Planting Ratio: <2.3=mais milho, >2.5=mais soja. 5) Season-Average Price como driver de decisao de plantio seguinte.`;

const BRIEFING_WEI = `DR. WEI CHEN (Peterson Institute, ex-PBOC): 32 anos. Modelou impacto importacoes chinesas de soja 2002-2004. METODOLOGIA: 1) Caixin vs NBS PMI: divergencia = stress no setor privado. 2) PBOC → RMB/USD → poder de compra importacoes → demanda soja/carne. 3) Herd rebuilding cycle suino chines: ciclos 3-4 anos, quando cresce demanda por racao explode. 4) Estoques estrategicos ocultos China: importacao acima do esperado = recomposicao. 5) Trade war: desvio de comercio para BR/ARG por tarifa EUA.`;

const BRIEFING_SARAH = `SARAH MITCHELL (ex-Citi/IEA, metodologia Ed Morse): 35 anos. IEA World Energy Outlook 10 anos. METODOLOGIA: 1) EIA Weekly: estoques vs media 5a, producao shale. Draw maior que esperado = spike. 2) OPEC+ compliance real vs declarado (satellite tanker tracking). 3) Shale response: WTI>$75-80 por 60d = +500k bbl/dia em 6-9m. 4) Fertilizante lag: +$10/bbl NG = +$50/ton ureia = +$0.30/bu CMP milho em 6-12m. 5) Crack spreads: >$20/bbl = refinarias full capacity. 6) Chokepoints: Hormuz(20% petroleo), Bosphorus, Malacca(30% comercio maritimo).`;

const BRIEFING_JAMES = `JAMES PARK (ex-Goldman Sachs/WGC, metais preciosos): 28 anos. Estruturou primeiros ETFs ouro (GLD) 2004. METODOLOGIA: 1) Real yields TIPS 10Y: negativo = custo oportunidade zero = bullish GC. Cada -0.5% real yield = +5% GC. 2) DXY inverso: -1% DXY = +0.7-1% GC. 3) Bancos centrais >1000t/ano desde 2022 (Turquia, China, India, Polonia). 4) GC/SI Ratio: >80:1 = prata barata, <50:1 = ouro barato. 5) Prata industrial: solar >100oz/MW, semicondutores, EVs. 6) COMEX vs LBMA spread alto = stress fisico.`;

const BRIEFING_MARIA = `MARIA OLIVEIRA (ex-Marex/ICCO, softs tropicais): 30 anos. Co-autora ICCO Quarterly Bulletin. METODOLOGIA: 1) Grinding ratios trimestrais: >100% YoY = demanda forte cacau = bullish. <95% = demanda destruida. 2) Crop cycle West Africa: precipitacao jan-mar → main crop out. Lag 9 meses. 3) Paridade etanol/acucar BR: hidratado >70% gasolina → usinas desviam cana → bullish SB. 4) Biennial bearing cafe Arabica BR: anos alternados forte/fraco. 5) Vietnam robusta: El Nino = seco = deficit = bullish KC. 6) India cotton MSP distorce mercado.`;

const BRIEFING_ROBERTO = `ROBERTO TANAKA (ex-LMIC/Texas A&M, pecuaria): 38 anos. 200+ analises mercado bovino. Ciclo pecuario 8-12 anos. METODOLOGIA: 1) Cattle on Feed: Placements >110% YoY = mais oferta 4-6m. Marketings <100% = atraso abate. 2) Herd building: Heifer retention >50% = building (bullish LP). <45% = liquidacao. 3) Feedlot Margin LMIC: LE*10-GF*7.5-ZC*50. Negativo 2+m = param de repor GF. 4) Packer margin: Boxed Beef Cutout vs LE alto = suporte. 5) Peso abate acima media = oversupply tecnico. 6) HE/ZC e LE/ZC breakevens.`;

const BRIEFING_LUCIA = `LUCIA CHEN (ex-Chicago Trading/CBOE, opcoes): 26 anos. Floor trader CBOT. Framework Natenberg. METODOLOGIA: 1) IVR: <20%=opcoes baratas nao vender, >80%=caras regime de venda. 2) Vol term structure: inversao (proximo>distante) = evento iminente. Inversao+skew alto = vol event. 3) Skew: put skew alto = medo de queda, vender puts coleta premio de medo. 4) Vol crush pre-WASDE: vender premium 48h antes, fechar antes do numero. 5) Gamma explode <10 DTE: nao carregar short ATM. 6) Delta hedging: net ±0.05 do target.`;

const BRIEFING_RODRIGO = `RODRIGO BATISTA (ex-Cargill Santos/Bunge Paranagua, fisico BR): 35 anos. Maior especialista em basis soja BR vs Chicago. METODOLOGIA: 1) Basis BR = FOB Paranagua - CBOT ZS1. Positivo = BR caro. Negativo = BR barato, exportacao agressiva. 2) Landed Cost Spread Shanghai: custo BR vs EUA para China. 3) Ritmo embarques ANEC: pace >110% = pressao baixista. 4) BRL/USD: -1% BRL = exportador vende mais = pressao CBOT. 5) Selic alta = produtor vende rapido = mais oferta CP. 6) Paridade etanol/acucar: desvio cana → bullish SB.`;

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

--- JENNIFER BOND (Gr\u00e3os/USDA \u2014 ERS Feed Grains) ---
AT: [analise a estrutura das curvas futuras de gr\u00e3os (ZC, ZS, ZW), Crop Progress, momentum de pre\u00e7os \u2014 use dados do snapshot]
AF: [analise balan\u00e7o PSD (produ\u00e7\u00e3o vs usos vs estoques finais), Hog/Corn e Steer/Corn ratio, Corn/Soy Planting Ratio \u2014 use dados do snapshot]
Cruzamento: [o balan\u00e7o USDA confirma ou contradiz a estrutura t\u00e9cnica da curva? Uma frase.]
Contradi\u00e7\u00f5es: [o que os dados USDA contradizem na tese atual de gr\u00e3os]
Risco ignorado: [um desequil\u00edbrio no balan\u00e7o PSD que o portf\u00f3lio n\u00e3o est\u00e1 precificando]
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
const SPECIALISTS: { name: string; role: string; domain: string; system: string }[] = [
  { name: "Carlos Mera", role: "Graos Bear Case (Rabobank)", domain: "grains_bear",
    system: BRIEFING_CARLOS + " Foco: ZC, ZS, ZW, KE, ZM, ZL. Analise AT (curva forward, COT, momentum) e AF (WASDE, STU, safra BR). Contradicoes ANTES. Max 150 palavras." },
  { name: "Jennifer Bond", role: "Graos/USDA (ERS Feed Grains)", domain: "grains_bear",
    system: BRIEFING_JENNIFER + " Foco: ZC, ZS, ZW. Analise AT (curva forward, Crop Progress, momentum) e AF (balanco PSD, Hog/Corn ratio, Corn/Soy Planting Ratio). Contradicoes ANTES. Max 150 palavras." },
  { name: "Rodrigo Batista", role: "Fisico Brasil Bull Case", domain: "physical_brazil",
    system: BRIEFING_RODRIGO + " Foco: CEPEA, basis Paranagua, boi gordo, soja fisica. Analise AT (basis spot vs futuro) e AF (Feedlot Margin LE*10-GF*7.5-ZC*50, crush, LCS Shanghai). Contradicoes ANTES. Max 150 palavras." },
  { name: "Henrik Larsson", role: "Macro Outsider (ex-Brevan Howard)", domain: "tail_risk",
    system: BRIEFING_HENRIK + " Foco: tail risk, geopolitica, CL, open interest extremos, correlacoes que rompem em crise. Analise AT (OI anormal) e AF (riscos geopoliticos). Qual black swan as posicoes nao cobrem? Max 150 palavras." },
  { name: "Ana Lima", role: "Risk Manager (ex-Cargill)", domain: "portfolio_risk",
    system: BRIEFING_ANA + " Para CADA posicao do portfolio: DTE, distancia do strike vs spot, exposicao maxima em $. PnL > -200% credito = PERDA MAXIMA. Short calls = risco assignment. Rankeie por urgencia. Max 200 palavras." },
  { name: "Dr. Wei", role: "Macro Global (Fed/China)", domain: "macro_structural",
    system: BRIEFING_WEI + " Foco: Fed policy, Treasury yields, China PMI/demanda, RMB/USD, fluxos de capital. Use VIX, SP500, 10Y do snapshot. Max 120 palavras." },
  { name: "Sarah Mitchell", role: "Energia (CL/NG)", domain: "energy",
    system: BRIEFING_SARAH + " Foco: CL, NG. OPEC, EIA storage, curva forward, IV de CL. Backwardation forte = stress. Max 120 palavras." },
  { name: "James Park", role: "Metais (GC/SI)", domain: "metals",
    system: BRIEFING_JAMES + " Foco: GC, SI, ratio GC/SI, bancos centrais, DXY inverso. IV e skew de SI. Real yields TIPS. Max 120 palavras." },
  { name: "Maria Oliveira", role: "Softs (KC/CC/SB/CT)", domain: "softs",
    system: BRIEFING_MARIA + " Foco: KC, CC, SB, CT. Safra Brasil cafe/acucar, mix etanol, ICCO deficit, estoques ICE. IV extrema CC. Max 120 palavras." },
  { name: "Roberto Tanaka", role: "Pecuaria (LE/GF/HE)", domain: "livestock",
    system: BRIEFING_ROBERTO + " Foco: LE, GF, HE. Cattle on Feed, ciclo pecuario, feedlot margin, packer margin, grilling season. Max 120 palavras." },
  { name: "Lucia Chen", role: "Opcoes / Volatilidade", domain: "options_vol",
    system: BRIEFING_LUCIA + " Analise IV, skew, term structure de TODAS commodities. IV>50%=venda premium. IV<20%=evitar. Regime VEGA se IV>=40%. Max 150 palavras." },
  { name: "David Kowalski", role: "COT / Positioning", domain: "cot_specialist",
    system: BRIEFING_DAVID + " Analise COT Index de TODAS commodities. COT>80=CROWDED LONG. COT<20=CROWDED SHORT. 3 janelas: 156w/52w/26w. Delta semanal. Max 120 palavras." },
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
  client: Anthropic, spec: typeof SPECIALISTS[0], _context: string
): Promise<string> {
  const focused = buildFocusedSnapshot(spec.domain);
  const res = await client.messages.create({
    model: "claude-sonnet-4-20250514",
    max_tokens: 400,
    system: spec.system + "\n\n" + WEIGHT_ENGINE + "\n\n" + AT_FRAMEWORK + "\n\n" + AF_FRAMEWORK + "\n\n" + focused,
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
// JOB STORE (file-based, survives restarts)
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

const JOBS_FILE = join(process.cwd(), "council_jobs.json");

function loadJobs(): Record<string, CouncilJob> {
  try {
    if (existsSync(JOBS_FILE)) return JSON.parse(readFileSync(JOBS_FILE, "utf-8"));
  } catch { }
  return {};
}

function saveJob(jobId: string, job: CouncilJob) {
  try {
    const all = loadJobs();
    all[jobId] = job;
    const keys = Object.keys(all).sort();
    const trimmed: Record<string, CouncilJob> = {};
    for (const k of keys.slice(-10)) trimmed[k] = all[k];
    writeFileSync(JOBS_FILE, JSON.stringify(trimmed, null, 2));
  } catch (e) {
    console.error("saveJob error:", e);
  }
}

function updateJob(jobId: string, updates: Partial<CouncilJob>) {
  const job = loadJobs()[jobId];
  if (!job) return;
  saveJob(jobId, { ...job, ...updates });
}

async function runFullCouncil(jobId: string) {
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
      updateJob(jobId, { stage: "specialists", detail: `Especialistas batch ${batchNum}/${totalBatches} (${batch.map(s => s.name.split(" ")[0]).join(", ")})...` });

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
    updateJob(jobId, { stage: "heads", detail: "Ray Dalio sintetizando..." });
    const dalio = await runHead(client, DALIO_SYSTEM, "RAY DALIO (Sintese)", briefings, compact);

    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY_MS));

    // Stage 2b: Devil
    updateJob(jobId, { stage: "heads", detail: "Advogado do Diabo atacando..." });
    const devil = await runHead(client, DEVIL_SYSTEM, "ADVOGADO DO DIABO", briefings, compact);

    await new Promise(resolve => setTimeout(resolve, BATCH_DELAY_MS));

    // Stage 3: Chairman
    updateJob(jobId, { stage: "chairman", detail: "Chairman produzindo relatorio final..." });
    const chairman = await runChairman(client, briefings, dalio, devil, snapshot);

    // Done
    updateJob(jobId, { status: "complete", stage: "complete", detail: "", response: chairman, snapshot_size: snapshot.length, completed_at: new Date().toISOString() });
  } catch (err: any) {
    updateJob(jobId, { status: "error", stage: "error", error: err.message?.slice(0, 500) || "Unknown error", completed_at: new Date().toISOString() });
  }
}

// ═══════════════════════════════════════════════════════
// COMMODITY DEEP DIVE (single commodity, single call)
// ═══════════════════════════════════════════════════════
const COMMODITY_DOMAINS: Record<string, string> = {
  ZC: "grains_bear", ZS: "grains_bear", ZW: "grains_bear", KE: "grains_bear", ZM: "grains_bear", ZL: "grains_bear",
  LE: "livestock", GF: "livestock", HE: "livestock",
  CL: "energy", NG: "energy",
  GC: "metals", SI: "metals",
  KC: "softs", CC: "softs", SB: "softs", CT: "softs",
  DX: "macro_structural",
};

async function runCommodityAnalysis(jobId: string, commodity: string) {
  try {
    const client = new Anthropic({ apiKey: getKey() });
    const domain = COMMODITY_DOMAINS[commodity] || "grains_bear";
    const focused = buildFocusedSnapshot(domain);
    const portfolioSnap = buildFocusedSnapshot("portfolio_risk");

    updateJob(jobId, { stage: "analyzing", detail: `Analisando ${commodity}...` });

    const system = `Voce e o Council AgriMacro analisando uma commodity especifica: ${commodity}.
Use os frameworks AT e AF abaixo e os dados do snapshot.

${AT_FRAMEWORK}

${AF_FRAMEWORK}

${WEIGHT_ENGINE}

FORMATO OBRIGATORIO:
=== ANALISE ${commodity} ===
Data: [data do snapshot]

--- AT (Analise Tecnica) ---
Preco atual: [do snapshot]
Momentum 5d/20d: [calcular do snapshot se disponivel]
IV: [do snapshot] | Skew: [do snapshot] | Term: [do snapshot]
Curva forward: [contango/backwardation do snapshot]
Suportes e resistencias: [inferir dos dados]

--- AF (Analise Fundamental) ---
COT Index: [do snapshot, 3 janelas] | Crowd: [label]
Estoques: [do snapshot, desvio vs 5y]
Sazonalidade ${new Date().toLocaleString("pt-BR", {month:"long"})}: [do snapshot]
Spreads relevantes: [do snapshot]

--- CRUZAMENTO AT+AF ---
Confluencia: CONVERGENTE ou DIVERGENTE
Direcao dominante: BULLISH / BEARISH / NEUTRO com threshold especifico
Risco critico: [1 risco que o portfolio nao esta precificando]

--- POSICOES ABERTAS ${commodity} ---
[filtrar do snapshot]

--- ACAO RECOMENDADA ---
[concreta com threshold e prazo]

Se dado N/A: escrever explicitamente. NUNCA fabricar.
Maximo 500 palavras. Portugues brasileiro.`;

    const res = await client.messages.create({
      model: "claude-sonnet-4-20250514",
      max_tokens: 1500,
      system: system,
      messages: [{ role: "user", content: `DADOS ${commodity}:\n${focused}\n\nPORTFOLIO:\n${portfolioSnap}` }],
    });

    const text = res.content.filter((c: any) => c.type === "text").map((c: any) => c.text).join("");
    updateJob(jobId, { status: "complete", stage: "complete", detail: "", response: text, snapshot_size: focused.length, completed_at: new Date().toISOString() });
  } catch (err: any) {
    updateJob(jobId, { status: "error", stage: "error", error: err.message?.slice(0, 500) || "Unknown error", completed_at: new Date().toISOString() });
  }
}

// ═══════════════════════════════════════════════════════
// POST HANDLER (starts job or runs quick/commodity)
// ═══════════════════════════════════════════════════════
export async function POST(req: NextRequest) {
  try {
    const { mode, commodity } = await req.json();

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

    // ── COMMODITY MODE: single-call deep dive ──
    if (mode === "commodity" && commodity) {
      const jobId = `commodity_${commodity}_${Date.now()}`;
      saveJob(jobId, { status: "running", stage: "starting", detail: `Analisando ${commodity}...`, started_at: new Date().toISOString() });
      runCommodityAnalysis(jobId, commodity).catch(() => {});
      return NextResponse.json({ jobId, status: "running" });
    }

    // ── FULL MODE: create job, run in background ──
    const jobId = `council_${Date.now()}`;
    saveJob(jobId, {
      status: "running",
      stage: "starting",
      detail: "Iniciando Council v2.2...",
      started_at: new Date().toISOString(),
    });

    // Fire and forget — runs in background
    runFullCouncil(jobId).catch(() => {});

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
  const job = jobId ? loadJobs()[jobId] : undefined;
  if (!jobId || !job) {
    return NextResponse.json({ error: "Job not found" }, { status: 404 });
  }
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
