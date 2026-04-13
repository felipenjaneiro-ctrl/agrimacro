"use client";
import { useState, useEffect } from "react";

const C = {
  bg: "#0E1A24", panel: "#142332", border: "#1E3044",
  text: "#E8ECF1", textMuted: "#8C96A5",
  green: "#00C878", red: "#DC3C3C", gold: "#DCB432",
  blue: "#468CDC", orange: "#E88C2A",
  panelDark: "#0A1520",
};

function regimeColor(r: string) {
  if (r === "Very Bullish") return C.green;
  if (r === "Bullish") return C.blue;
  if (r === "Neutral") return C.gold;
  if (r === "Bearish") return C.orange;
  if (r === "Very Bearish") return C.red;
  return C.textMuted;
}

function PanelCurrentState({ d }: { d: any }) {
  if (!d) return <div style={{ color: C.textMuted, padding: 20 }}>Carregando...</div>;
  const rc = regimeColor(d.regime || "");
  const isTrigger = d.trigger_active;
  return (
    <div style={{ padding: "14px 16px", boxSizing: "border-box", overflowY: "auto" }}>
      <div style={{ border: "2px solid " + rc, borderRadius: 8, padding: "10px 14px", marginBottom: 12, background: rc + "18" }}>
        <div style={{ fontSize: 30, fontWeight: 900, color: rc, letterSpacing: 1 }}>
          SCORE: {d.score != null ? (d.score >= 0 ? "+" : "") + d.score.toFixed(1) : "N/A"}
        </div>
        <div style={{ fontSize: 13, fontWeight: 700, color: rc, marginTop: 2 }}>{d.regime || "N/A"}</div>
        <div style={{ fontSize: 11, color: C.textMuted, marginTop: 4 }}>
          Price: <span style={{ color: C.text }}>{d.current_price != null ? "$" + d.current_price.toFixed(2) : "N/A"}</span>
          {d.expected_price_3m != null && (
            <span style={{ color: C.textMuted, marginLeft: 10 }}>
              Exp 3m: <span style={{ color: C.gold }}>${d.expected_price_3m.toFixed(2)}</span>
            </span>
          )}
        </div>
      </div>
      <div style={{ marginBottom: 10 }}>
        {(d.score_factors || []).map((f: any, i: number) => {
          const barW = Math.min(Math.abs(f.value) / 1.2 * 100, 100);
          const fc = f.direction === "positive" ? C.green : C.red;
          return (
            <div key={i} style={{ marginBottom: 6 }}>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: C.textMuted, marginBottom: 2 }}>
                <span>{f.name}</span>
                <span style={{ color: fc, fontWeight: 700 }}>{f.value >= 0 ? "+" : ""}{f.value.toFixed(1)}</span>
              </div>
              <div style={{ background: C.panelDark, height: 6, borderRadius: 3 }}>
                <div style={{ width: barW + "%", height: "100%", background: fc, borderRadius: 3 }} />
              </div>
            </div>
          );
        })}
      </div>
      <div style={{ display: "flex", gap: 6, marginBottom: 10 }}>
        {[
          { label: "Mom 3m", val: d.momentum_3m },
          { label: "Mom 6m", val: d.momentum_6m },
          { label: "Mom 12m", val: d.momentum_12m },
        ].map((item, i) => (
          <div key={i} style={{ flex: 1, background: C.panelDark, borderRadius: 6, padding: "6px 8px", textAlign: "center" }}>
            <div style={{ fontSize: 9, color: C.textMuted }}>{item.label}</div>
            <div style={{ fontSize: 12, fontWeight: 700, color: item.val == null ? C.textMuted : item.val >= 0 ? C.green : C.red }}>
              {item.val != null ? (item.val >= 0 ? "+" : "") + (item.val * 100).toFixed(1) + "%" : "N/A"}
            </div>
          </div>
        ))}
      </div>
      <div style={{ display: "flex", gap: 6, marginBottom: 10 }}>
        {[
          { label: "CFTC Net Long", val: d.cftc_net_long, sign: false },
          { label: "CFTC Delta", val: d.cftc_delta, sign: true },
        ].map((item, i) => (
          <div key={i} style={{ flex: 1, background: C.panelDark, borderRadius: 6, padding: "6px 8px", textAlign: "center" }}>
            <div style={{ fontSize: 9, color: C.textMuted }}>{item.label}</div>
            <div style={{ fontSize: 11, fontWeight: 700, color: item.val == null ? C.textMuted : (item.sign && item.val < 0) ? C.red : C.text }}>
              {item.val != null ? (item.sign && item.val > 0 ? "+" : "") + item.val.toLocaleString() : "N/A"}
            </div>
          </div>
        ))}
      </div>
      <div style={{ borderRadius: 6, padding: "7px 10px", fontSize: 10, fontWeight: 700,
        background: isTrigger ? C.red + "33" : C.gold + "22",
        border: "1px solid " + (isTrigger ? C.red : C.gold),
        color: isTrigger ? C.red : C.gold, display: "flex", alignItems: "center", gap: 6 }}>
        <span>{isTrigger ? "HEDGE IMEDIATO" : "Monitorar"}</span>
        <span style={{ fontWeight: 400 }}>{d.trigger_reason || "N/A"}</span>
      </div>
    </div>
  );
}

function PanelSeasonality({ d, symbol }: { d: any; symbol?: string }) {
  if (!d?.seasonality?.monthly) return <div style={{ color: C.textMuted, padding: 20 }}>N/A</div>;
  const monthly = d.seasonality.monthly;
  const quarterly = d.seasonality.quarterly || [];
  const curMonth = new Date().getMonth();
  const maxAbs = Math.max(...monthly.map((m: any) => Math.abs(m.avg_ret)), 0.025);
  return (
    <div style={{ padding: "14px 16px", height: "100%", boxSizing: "border-box" }}>
      <div style={{ display: "flex", marginBottom: 4 }}>
        {quarterly.map((q: any, qi: number) => (
          <div key={qi} style={{ flex: 3, textAlign: "center", fontSize: 9, fontWeight: 700, color: C.textMuted, borderLeft: qi > 0 ? "1px solid " + C.border : "none", paddingBottom: 2 }}>
            {q.quarter} {q.avg_ret_m != null ? (q.avg_ret_m >= 0 ? "+" : "") + (q.avg_ret_m * 100).toFixed(1) + "%" : ""}
          </div>
        ))}
      </div>
      <div style={{ display: "flex", alignItems: "flex-end", height: 130, gap: 2, marginBottom: 8, position: "relative" }}>
        <div style={{ position: "absolute", left: 0, right: 0, top: "50%", height: 1, background: C.border, zIndex: 0 }} />
        {monthly.map((m: any, i: number) => {
          const isCurrent = i === curMonth;
          const isPos = m.avg_ret >= 0;
          const barH = (Math.abs(m.avg_ret) / maxAbs) * 58;
          const fc = isPos ? C.green : C.red;
          return (
            <div key={i} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", height: "100%", position: "relative", zIndex: 1 }}>
              <div style={{ flex: 1, display: "flex", flexDirection: "column", justifyContent: "flex-end", width: "100%" }}>
                {isPos && <div style={{ height: barH, background: fc, borderRadius: "2px 2px 0 0", border: isCurrent ? "1px solid " + C.gold : "none" }} />}
              </div>
              <div style={{ flex: 1, display: "flex", flexDirection: "column", justifyContent: "flex-start", width: "100%" }}>
                {!isPos && <div style={{ height: barH, background: fc, borderRadius: "0 0 2px 2px", border: isCurrent ? "1px solid " + C.gold : "none" }} />}
              </div>
            </div>
          );
        })}
      </div>
      <div style={{ display: "flex", gap: 2 }}>
        {monthly.map((m: any, i: number) => {
          const isCurrent = i === curMonth;
          const isPos = m.avg_ret >= 0;
          return (
            <div key={i} style={{ flex: 1, textAlign: "center" }}>
              <div style={{ fontSize: 7, color: isCurrent ? C.gold : C.textMuted, fontWeight: isCurrent ? 700 : 400 }}>{m.month}</div>
              <div style={{ fontSize: 8, color: isPos ? C.green : C.red, fontWeight: 600 }}>{(m.avg_ret * 100).toFixed(1)}%</div>
              <div style={{ fontSize: 7, color: C.textMuted }}>{Math.round(m.pct_pos * 100)}%</div>
            </div>
          );
        })}
      </div>
      <div style={{ marginTop: 8, fontSize: 9, color: C.textMuted, textAlign: "center" }}>
        Barras = retorno medio | % abaixo = freq. positiva | <span style={{ color: C.gold }}>borda ouro</span> = mes atual
      </div>
    </div>
  );
}

function PanelCOT({ d }: { d: any }) {
  if (!d?.cot_signals) return <div style={{ color: C.textMuted, padding: 20 }}>N/A</div>;
  const signals = d.cot_signals;
  const maxAbs = Math.max(...signals.map((s: any) => Math.abs(s.avg_fwd3m)), 0.01);
  const m6 = d.momentum_6m || 0;
  const m3 = d.momentum_3m || 0;
  const activeName = m6 > 0.10 && m3 > 0.05 ? "Crowded Long"
    : m6 > 0.08 && m3 < m6 * 0.40 ? "Momentum Exhaustion" : "";
  return (
    <div style={{ padding: "14px 16px", height: "100%", boxSizing: "border-box" }}>
      {signals.map((s: any, i: number) => {
        const isActive = s.name === activeName;
        const barW = Math.max((Math.abs(s.avg_fwd3m) / maxAbs) * 100, 5);
        const isNeg = s.avg_fwd3m < 0;
        return (
          <div key={i} style={{ marginBottom: 12, padding: "10px 12px", borderRadius: 7,
            background: isActive ? C.red + "22" : C.panelDark,
            border: "1px solid " + (isActive ? C.red : C.border) }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
              <div style={{ fontSize: 11, fontWeight: 700, color: isActive ? C.red : C.text }}>
                {isActive ? "ATIVO -- " : ""}{s.name}
              </div>
              <div style={{ fontSize: 10, color: C.textMuted }}>n={s.n}</div>
            </div>
            <div style={{ background: C.bg, height: 22, borderRadius: 4, overflow: "hidden", marginBottom: 6 }}>
              <div style={{ width: barW + "%", height: "100%", background: isNeg ? C.red : C.green, borderRadius: 4,
                display: "flex", alignItems: "center", justifyContent: "flex-end", paddingRight: 6 }}>
                <span style={{ fontSize: 10, fontWeight: 700, color: "#fff" }}>{(s.avg_fwd3m * 100).toFixed(2)}%</span>
              </div>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: C.textMuted }}>
              <span>Avg Fwd 3m: <span style={{ color: isNeg ? C.red : C.green, fontWeight: 700 }}>{(s.avg_fwd3m * 100).toFixed(2)}%</span></span>
              <span>% Neg: <span style={{ color: C.red }}>{Math.round(s.pct_neg * 100)}%</span></span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

function PanelFramework({ d }: { d: any }) {
  if (!d?.framework) return <div style={{ color: C.textMuted, padding: 20 }}>N/A</div>;
  const fw = d.framework;
  const phases = [
    { key: "phase_now",        label: "AGORA",      color: C.blue,    icon: ">" },
    { key: "phase_protection", label: "PROTECAO",   color: C.gold,    icon: "!" },
    { key: "phase_trigger",    label: "TRIGGER",    color: C.red,     icon: "!!" },
    { key: "phase_max_risk",   label: "RISCO MAX",  color: "#8B1A1A", icon: "X" },
  ];
  return (
    <div style={{ padding: "14px 16px", height: "100%", boxSizing: "border-box", overflowY: "auto" }}>
      {phases.map((ph, i) => (
        <div key={i} style={{ marginBottom: 8, padding: "8px 10px", borderRadius: 6,
          background: ph.color + "22", borderLeft: "3px solid " + ph.color }}>
          <div style={{ fontSize: 9, fontWeight: 800, color: ph.color, marginBottom: 3, letterSpacing: 1 }}>
            [{ph.icon}] {ph.label}
          </div>
          <div style={{ fontSize: 10, color: C.text, lineHeight: 1.4 }}>{(fw as any)[ph.key] || "N/A"}</div>
        </div>
      ))}
      {(d.structural_alerts || []).length > 0 && (
        <div style={{ marginTop: 10, paddingTop: 8, borderTop: "1px solid " + C.border }}>
          <div style={{ fontSize: 9, fontWeight: 700, color: C.gold, marginBottom: 6, letterSpacing: 1 }}>ALERTAS ESTRUTURAIS</div>
          {(d.structural_alerts || []).map((a: string, i: number) => (
            <div key={i} style={{ fontSize: 10, color: C.textMuted, marginBottom: 3, paddingLeft: 8, borderLeft: "2px solid " + C.gold }}>{a}</div>
          ))}
        </div>
      )}
    </div>
  );
}

const EXPLANATIONS: Record<string, { title: string; text: string }[]> = {
  LE: [
    { title: "1. SAZONALIDADE", text: "Media historica do retorno mensal do Live Cattle de 1990-2024 (ISU AgDM B2-12, Iowa/Minn Choice Steers). Cada barra mostra o retorno medio; o percentual abaixo e a frequencia de meses positivos nos 35 anos. Q1/Q2 sao sazonalmente fracos (abril negativo 77% das vezes), Q3/Q4 sao os mais fortes. Fevereiro: -0.41%, 43% positivo. A borda dourada marca o mes atual." },
    { title: "2. CURRENT STATE", text: "Score de -3 a +3 combinando 4 fatores: (1) Sazonalidade do mes atual, (2) Momentum 6m -- rallies acima de 15% em 6 meses tendem a ser seguidos de fraqueza pois o mercado fica cheio de especuladores com lucro, (3) Distancia do high 12m -- preco proximo das maximas aumenta risco de realizacao, (4) Desaceleracao do momentum -- 3m desacelerando vs 6m sinaliza exaustao. Trigger ativa quando mom 6m >15% E 3m desacelera." },
    { title: "3. COT PROXY", text: "Proxy do posicionamento dos fundos (Managed Money, CFTC) usando momentum como indicador. Crowded Long: mom 6m>10% e 3m>5% -- fundos provavelmente com posicao maxima, -0.85% medio fwd 3m (56% neg, n=63). Momentum Exhaustion: rally 6m forte mas 3m desacelera abaixo de 40% do ritmo -- sinal mais forte, -2.24% medio fwd 3m (61% neg, n=54). Trend Break: queda >2% apos rally (71% neg, n=7)." },
    { title: "4. FRAMEWORK DE ACAO", text: "Estrategia em 4 fases: Azul=fase atual de monitoramento. Ambar=janela de compra de protecao (puts) antes do pior periodo sazonal. Vermelho=trigger de urgencia que acelera o hedge quando os dois criterios sao atendidos simultaneamente. Vermelho escuro=janela historica de maximo risco onde Q3 entregou -5.86% de retorno medio fwd 3m nos regimes Bearish/Very Bearish." },
  ],
  GF: [
    { title: "1. SAZONALIDADE", text: "Dados ISU AgDM Tabela 3 -- Iowa 500-600lb Steers $/cwt (2004-2024, 20 anos). GF tem sazonalidade similar ao LE com lag de 30-60 dias: os precos de reposicao respondem ao gado gordo com defasagem natural do ciclo de producao. O spread LE/GF e um termometro importante de tensao no mercado de recria." },
    { title: "2. CURRENT STATE", text: "Mesmo modelo de score 4 fatores do LE. GF historicamente tem volatilidade percentual maior que LE, especialmente em ciclos de liquidacao de rebanho. Drawdowns de 40-45% ocorreram em 2014-2016. O custo de recria (ratio milho/GF) e um fator estrutural monitorado nos alertas mas nao embutido no score matematico." },
    { title: "3. COT PROXY", text: "GF tem menor liquidez que LE, entao fundos especulativos representam proporcao maior do open interest e o posicionamento pode ser mais volatil. Os thresholds de momentum seguem o mesmo criterio do LE. O sinal de Trend Break After Rally e especialmente relevante em GF dado seus drawdowns historicos mais profundos." },
    { title: "4. FRAMEWORK DE ACAO", text: "GF oferece oportunidade de arbitragem de spread vs LE quando um mercado cai mais rapido que o outro. A janela de risco Jul-Set reflete a pressao sazonal de animais mais pesados chegando ao mercado de abate. Quando LE cai mais de 3% em 1 mes e GF ainda esta estavel, o spread e uma oportunidade de posicionamento." },
  ],
  HE: [
    { title: "1. SAZONALIDADE", text: "Lean Hogs tem sazonalidade INVERSA ao gado bovino -- um ponto critico para quem vem do mercado de cattle. Pico de preco: Mai-Jun quando producao ainda e baixa mas demanda de verao (BBQ season) sobe. Piso sazonal: Set-Out quando a oferta atinge o maximo anual do ciclo. Dados ISU AgDM Tabela 4, 2004-2024." },
    { title: "2. CURRENT STATE", text: "O modelo de score para HE usa mapa sazonal invertido: Q1/Q2 positivos, Q3/Q4 negativos. A sazonalidade de Fev-Abr e favoravel (score +0.5 a +1.0 so por sazonalidade). Momentum e COT seguem a mesma logica do LE/GF mas no contexto do ciclo inverso -- um rally forte em HE durante Q2 (pico) deve ser tratado como sinal de exaustao iminente." },
    { title: "3. COT PROXY", text: "Para HE, Crowded Long em termos de momentum frequentemente coincide com o pico sazonal de Mai-Jun, criando o cenario de maior risco historico. Fatores externos como demanda da China (maior importador) e doencas como PED/PRRS podem criar movimentos abruptos que superam completamente o sinal sazonal -- por isso os alertas estruturais sao criticos." },
    { title: "4. FRAMEWORK DE ACAO", text: "Estrategia oposta ao LE: o periodo favoravel para comprar protecao e ANTES do pico de preco de verao (Abr-Mai), quando as opcoes ainda estao baratas. O risco maximo de queda e Set-Out. O spread hog/corn (margem de producao) e o principal indicador fundamental: quando a margem esta negativa, produtores reducao oferta, suportando o preco. Quando positiva, incentiva expansao." },
  ],
};

function PanelOfertaGlobal({ psdData, symbol }: { psdData: any; symbol: string }) {
  const psdSym = symbol === "GF" ? "LE" : symbol;  // GF uses beef data
  const d = psdData?.commodities?.[psdSym];
  if (!d) return <div style={{ color: C.textMuted, padding: 20, fontSize: 10 }}>Dados PSD indispon\u00edveis. Rode: python pipeline/collect_livestock_psd.py</div>;

  const regions: {key: string; label: string; flag: string}[] = [
    { key: "usa", label: "EUA", flag: "\ud83c\uddfa\ud83c\uddf8" },
    { key: "brazil", label: "Brasil", flag: "\ud83c\udde7\ud83c\uddf7" },
    { key: "china", label: "China", flag: "\ud83c\udde8\ud83c\uddf3" },
  ];
  const attrs: {key: string; label: string}[] = [
    { key: "production", label: "Produ\u00e7\u00e3o" },
    { key: "exports", label: "Exporta\u00e7\u00f5es" },
    { key: "imports", label: "Importa\u00e7\u00f5es" },
    { key: "ending_stocks", label: "Estoque Final" },
    { key: "consumption", label: "Consumo" },
  ];

  // Competitiveness: Brazil exports vs US exports
  const brExp = d.brazil?.summaries?.exports?.current;
  const usExp = d.usa?.summaries?.exports?.current;

  return (
    <div style={{ padding: "14px 16px", boxSizing: "border-box" }}>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10, marginBottom: 12 }}>
        {regions.map(r => {
          const summ = d[r.key]?.summaries;
          if (!summ) return (
            <div key={r.key} style={{ background: C.panelDark, borderRadius: 6, padding: "8px 10px" }}>
              <div style={{ fontSize: 10, color: C.textMuted }}>{r.flag} {r.label}: sem dados</div>
            </div>
          );
          return (
            <div key={r.key} style={{ background: C.panelDark, borderRadius: 6, padding: "8px 10px" }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: C.text, marginBottom: 6 }}>{r.flag} {r.label}</div>
              {attrs.map(a => {
                const s = summ[a.key];
                if (!s) return null;
                const dev = s.deviation_pct || 0;
                const devColor = Math.abs(dev) < 3 ? C.textMuted : dev > 0 ? C.green : C.red;
                return (
                  <div key={a.key} style={{ display: "flex", justifyContent: "space-between", fontSize: 9, marginBottom: 3 }}>
                    <span style={{ color: C.textMuted }}>{a.label}</span>
                    <span>
                      <span style={{ color: C.text, fontWeight: 600 }}>{s.current?.toLocaleString()}</span>
                      <span style={{ color: devColor, marginLeft: 4, fontWeight: 600 }}>({dev >= 0 ? "+" : ""}{dev}%)</span>
                    </span>
                  </div>
                );
              })}
              {/* Mini sparkline for production history */}
              {summ.production?.history && (() => {
                const hist = summ.production.history;
                const vals = hist.map((h: any) => h.value);
                const mn = Math.min(...vals);
                const mx = Math.max(...vals);
                const rng = mx - mn || 1;
                return (
                  <div style={{ display: "flex", alignItems: "flex-end", gap: 1, height: 20, marginTop: 4 }}>
                    {vals.map((v: number, i: number) => (
                      <div key={i} style={{
                        flex: 1, background: i === vals.length - 1 ? C.gold : C.blue,
                        height: Math.max(((v - mn) / rng) * 18, 2), borderRadius: 1, opacity: i === vals.length - 1 ? 1 : 0.5,
                      }} />
                    ))}
                  </div>
                );
              })()}
            </div>
          );
        })}
      </div>
      {brExp != null && usExp != null && usExp > 0 && (
        <div style={{ fontSize: 10, color: C.textMuted, textAlign: "center", padding: "4px 8px", background: C.panelDark, borderRadius: 4 }}>
          Brasil exporta <span style={{ color: C.green, fontWeight: 700 }}>{(brExp / usExp * 100).toFixed(0)}%</span> do volume dos EUA em {d.name} | Unidade: 1000 MT CWE
        </div>
      )}
    </div>
  );
}

function PanelCOTDelta({ cotData, symbol }: { cotData: any; symbol: string }) {
  const da = cotData?.commodities?.[symbol]?.disaggregated?.delta_analysis;
  if (!da) return <div style={{ color: C.textMuted, padding: 20, fontSize: 10 }}>COT Delta indispon\u00edvel para {symbol}</div>;
  const sig = da.signals?.[0];
  const dirColor = da.dominant_direction === "BULLISH" ? C.green : da.dominant_direction === "BEARISH" ? C.red : C.textMuted;
  const deltas = da.deltas_8w || [];
  const maxD = Math.max(...deltas.map((d: number) => Math.abs(d)), 1);
  return (
    <div style={{ padding: "14px 16px", boxSizing: "border-box" }}>
      <div style={{ display: "flex", gap: 8, marginBottom: 10, flexWrap: "wrap" }}>
        <div style={{ background: C.panelDark, borderRadius: 6, padding: "6px 10px", textAlign: "center", minWidth: 80 }}>
          <div style={{ fontSize: 9, color: C.textMuted }}>COT Index</div>
          <div style={{ fontSize: 14, fontWeight: 700, color: da.cot_index >= 70 ? C.green : da.cot_index <= 30 ? C.red : C.gold }}>{da.cot_index?.toFixed(1) ?? "N/A"}</div>
        </div>
        <div style={{ background: C.panelDark, borderRadius: 6, padding: "6px 10px", textAlign: "center", minWidth: 80 }}>
          <div style={{ fontSize: 9, color: C.textMuted }}>Delta Semanal</div>
          <div style={{ fontSize: 14, fontWeight: 700, color: da.current_delta >= 0 ? C.green : C.red }}>{da.current_delta >= 0 ? "+" : ""}{da.current_delta?.toLocaleString() ?? "N/A"}</div>
        </div>
        <div style={{ background: C.panelDark, borderRadius: 6, padding: "6px 10px", textAlign: "center", minWidth: 80 }}>
          <div style={{ fontSize: 9, color: C.textMuted }}>OI Trend</div>
          <div style={{ fontSize: 14, fontWeight: 700, color: (da.oi_trend_pct || 0) >= 0 ? C.green : C.red }}>{da.oi_trend_pct != null ? (da.oi_trend_pct >= 0 ? "+" : "") + da.oi_trend_pct.toFixed(1) + "%" : "N/A"}</div>
        </div>
        <div style={{ background: C.panelDark, borderRadius: 6, padding: "6px 10px", textAlign: "center", minWidth: 80 }}>
          <div style={{ fontSize: 9, color: C.textMuted }}>Reversal Score</div>
          <div style={{ fontSize: 14, fontWeight: 700, color: C.text }}>{da.reversal_score ?? 0}</div>
        </div>
      </div>
      {sig && (
        <div style={{ padding: "8px 12px", borderRadius: 6, marginBottom: 10, background: (sig.color || dirColor) + "22", border: "1px solid " + (sig.color || dirColor) }}>
          <div style={{ fontSize: 11, fontWeight: 700, color: sig.color || dirColor }}>{sig.label || sig.type}</div>
          <div style={{ fontSize: 10, color: C.textMuted, marginTop: 2 }}>{sig.description}</div>
        </div>
      )}
      <div style={{ fontSize: 9, color: C.textMuted, marginBottom: 4 }}>Deltas \u00FAltimas {deltas.length} semanas (Managed Money Net Change)</div>
      <div style={{ display: "flex", alignItems: "center", gap: 2, height: 50 }}>
        {deltas.map((d: number, i: number) => {
          const h = Math.max(Math.abs(d) / maxD * 40, 2);
          const isLast = i === deltas.length - 1;
          return (
            <div key={i} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", height: "100%" }}>
              <div style={{ width: "100%", maxWidth: 20, height: h, background: d >= 0 ? C.green : C.red, borderRadius: 2, opacity: isLast ? 1 : 0.6, border: isLast ? "1px solid " + C.gold : "none" }} />
            </div>
          );
        })}
      </div>
    </div>
  );
}

export default function LivestockRiskTab() {
  const [sym, setSym] = useState<"LE" | "GF" | "HE">("LE");
  const [data, setData] = useState<any>(null);
  const [cotData, setCotData] = useState<any>(null);
  const [psdData, setPsdData] = useState<any>(null);
  const [weeklyData, setWeeklyData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [revalidating, setRevalidating] = useState(false);
  const [genAt, setGenAt] = useState("");

  const fetchData = (isRefresh = false) => {
    if (isRefresh) setRevalidating(true);
    Promise.all([
      fetch("/data/processed/bottleneck.json").then(r => r.json()),
      fetch("/data/processed/cot.json").then(r => r.json()).catch(() => null),
      fetch("/data/processed/livestock_psd.json").then(r => r.json()).catch(() => null),
      fetch("/data/processed/livestock_weekly.json").then(r => r.json()).catch(() => null),
    ]).then(([bj, cj, pj, wj]) => {
      const comms = bj.commodities || {};
      if (Object.keys(comms).length > 0) {
        setData(comms);
        setGenAt(bj.generated_at || "");
      }
      if (cj) setCotData(cj);
      if (pj) setPsdData(pj);
      if (wj) setWeeklyData(wj);
    }).catch(() => {})
      .finally(() => { setLoading(false); setRevalidating(false); });
  };

  useEffect(() => { fetchData(); }, []);

  const d = data?.[sym];
  const names: Record<string, string> = { LE: "Live Cattle", GF: "Feeder Cattle", HE: "Lean Hogs" };

  const panelStyle = {
    background: C.panel, border: "1px solid " + C.border, borderRadius: 8,
    overflow: "hidden", display: "flex", flexDirection: "column" as const,
  };
  const panelHdr = (title: string) => (
    <div style={{ background: C.panelDark, padding: "7px 14px", fontSize: 9, fontWeight: 800,
      letterSpacing: 1.2, color: C.textMuted, borderBottom: "1px solid " + C.border,
      flexShrink: 0, textTransform: "uppercase" as const }}>{title}</div>
  );

  return (
    <div style={{ padding: "16px 20px", color: C.text, fontFamily: "Segoe UI, Helvetica Neue, sans-serif", opacity: revalidating ? 0.85 : 1, transition: "opacity .3s" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16, flexWrap: "wrap", gap: 10 }}>
        <div>
          <div style={{ fontSize: 17, fontWeight: 800, letterSpacing: 1 }}>LIVESTOCK RISK -- BOTTLENECK THESIS</div>
          <div style={{ fontSize: 10, color: C.textMuted, marginTop: 2 }}>
            ISU AgDM B2-12 (1990-2024) | CFTC COT Proxy | Multi-Factor Model
            {genAt && <span style={{ marginLeft: 10, color: C.gold }}> Atualizado: {genAt.replace("T", " ")}</span>}
          </div>
        </div>
        <div style={{ display: "flex", gap: 6 }}>
          {(["LE", "GF", "HE"] as const).map(s => (
            <button key={s} onClick={() => setSym(s)} style={{
              padding: "8px 18px", borderRadius: 6, cursor: "pointer", fontWeight: 700, fontSize: 12,
              border: "none", background: sym === s ? C.gold : C.panel, color: sym === s ? C.bg : C.textMuted,
            }}>
              {s} <span style={{ fontSize: 10, fontWeight: 400 }}>-- {names[s]}</span>
            </button>
          ))}
        </div>
      </div>

      {revalidating && (
        <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8, padding: "4px 10px",
          background: "rgba(220,180,50,.08)", border: "1px solid rgba(220,180,50,.25)", borderRadius: 6, width: "fit-content" }}>
          <div style={{ width: 8, height: 8, borderRadius: "50%", border: "2px solid #DCB432", borderTopColor: "transparent",
            animation: "spin .8s linear infinite" }} />
          <span style={{ fontSize: 10, color: "#DCB432", fontWeight: 600 }}>Atualizando...</span>
        </div>
      )}
      {loading && !data && <div style={{ textAlign: "center", color: C.textMuted, padding: 40 }}>Carregando bottleneck.json...</div>}
      {!loading && !d && !data && (
        <div style={{ textAlign: "center", color: C.red, padding: 40 }}>
          Dado nao encontrado. Rode: python bottleneck_backtest.py
        </div>
      )}

      {d && (
        <>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gridTemplateRows: "340px 280px", gap: 12, marginBottom: 12 }}>
            <div style={panelStyle}>
              {panelHdr("CURRENT STATE -- " + names[sym] + " -- " + new Date().toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" }))}
              <div style={{ flex: 1, overflow: "auto" }}><PanelCurrentState d={d} /></div>
            </div>
            <div style={panelStyle}>
              {panelHdr("SAZONALIDADE -- Retorno Mensal Medio Historico (labels = % vezes positivo)")}
              <div style={{ flex: 1, overflow: "hidden" }}><PanelSeasonality d={d} symbol={sym} /></div>
            </div>
            <div style={panelStyle}>
              {panelHdr("COT PROXY -- Sinais de Short (Momentum como proxy de posicionamento)")}
              <div style={{ flex: 1, overflow: "auto" }}><PanelCOT d={d} /></div>
            </div>
            <div style={panelStyle}>
              {panelHdr("FRAMEWORK DE ACAO -- Fases e Triggers")}
              <div style={{ flex: 1, overflow: "auto" }}><PanelFramework d={d} /></div>
            </div>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 16 }}>
            <div style={panelStyle}>
              {panelHdr("OFERTA GLOBAL -- " + names[sym] + " (USDA PSD)")}
              <PanelOfertaGlobal psdData={psdData} symbol={sym} />
            </div>
            <div style={panelStyle}>
              {panelHdr("COT DELTA -- " + names[sym] + " (CFTC Disaggregated Managed Money)")}
              <PanelCOTDelta cotData={cotData} symbol={sym} />
            </div>
          </div>

          {/* Indicadores Semanais */}
          {weeklyData?.data && (() => {
            const wd = weeklyData.data;
            const symLower = sym.toLowerCase();
            const cards: {name:string; signal:string; color:string; value:string; detail:string; interp:string}[] = [];

            // Cold Storage
            const cs = wd[`cold_storage_${symLower}`];
            if (cs && !cs.is_fallback) {
              cards.push({
                name: cs.name || "Cold Storage",
                signal: cs.signal,
                color: cs.signal_color || C.textMuted,
                value: `${cs.current?.toLocaleString()} ${cs.unit || ""}`,
                detail: `vs 5A: ${cs.deviation_pct >= 0 ? "+" : ""}${cs.deviation_pct}%`,
                interp: cs.interpretation || "",
              });
            }

            // Abate BR (only for LE/GF)
            if ((sym === "LE" || sym === "GF") && wd.abate_bovinos_br && !wd.abate_bovinos_br.is_fallback) {
              const ab = wd.abate_bovinos_br;
              cards.push({
                name: ab.name || "Abate Bovinos BR",
                signal: ab.signal,
                color: ab.signal_color || C.textMuted,
                value: `${ab.current?.toLocaleString()} ${ab.unit || ""}`,
                detail: `vs m\u00e9dia: ${ab.deviation_pct >= 0 ? "+" : ""}${ab.deviation_pct}%`,
                interp: ab.interpretation || "",
              });
            }

            // Packer Proxy
            const pp = wd[`packer_${symLower}`];
            if (pp) {
              cards.push({
                name: pp.name || "Packer Activity",
                signal: pp.signal,
                color: pp.signal_color || C.textMuted,
                value: `$${pp.current_price} (avg20: $${pp.avg_20d})`,
                detail: `Mom 20d: ${pp.momentum_20d >= 0 ? "+" : ""}${pp.momentum_20d}%`,
                interp: pp.interpretation || "",
              });
            }

            if (cards.length === 0) return null;

            return (
              <div style={{ ...panelStyle, marginBottom: 16 }}>
                {panelHdr("INDICADORES SEMANAIS -- " + names[sym])}
                <div style={{ display: "grid", gridTemplateColumns: `repeat(${Math.min(cards.length, 3)}, 1fr)`, gap: 10, padding: "14px 16px" }}>
                  {cards.map((c, i) => (
                    <div key={i} style={{ background: C.panelDark, borderRadius: 6, padding: "10px 12px", borderLeft: "3px solid " + c.color }}>
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
                        <div style={{ fontSize: 10, fontWeight: 700, color: C.text }}>{c.name}</div>
                        <div style={{ fontSize: 9, fontWeight: 700, color: c.color, background: c.color + "22", padding: "2px 6px", borderRadius: 3 }}>{c.signal}</div>
                      </div>
                      <div style={{ fontSize: 12, fontWeight: 700, color: C.text, marginBottom: 2 }}>{c.value}</div>
                      <div style={{ fontSize: 10, color: c.color, fontWeight: 600, marginBottom: 4 }}>{c.detail}</div>
                      <div style={{ fontSize: 9, color: C.textMuted, lineHeight: 1.4 }}>{c.interp}</div>
                    </div>
                  ))}
                </div>
              </div>
            );
          })()}

          {d.strategy && (
            <div style={{ background: C.panel, border: "1px solid " + C.border, borderRadius: 8, padding: "10px 16px", marginBottom: 16, display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
              <div style={{ fontSize: 10, fontWeight: 700, color: C.textMuted, marginRight: 6 }}>BACKTEST STRATEGY:</div>
              {[
                { label: "CAGR", val: d.strategy.cagr, pct: true },
                { label: "Sharpe", val: d.strategy.sharpe, pct: false },
                { label: "Max DD", val: d.strategy.max_dd, pct: true },
              ].map((item, i) => (
                <div key={i} style={{ textAlign: "center", marginRight: 12 }}>
                  <div style={{ fontSize: 9, color: C.textMuted }}>{item.label}</div>
                  <div style={{ fontSize: 13, fontWeight: 700, color: item.val < 0 ? C.red : C.green }}>
                    {item.pct ? (item.val >= 0 ? "+" : "") + (item.val * 100).toFixed(1) + "%" : item.val.toFixed(2)}
                  </div>
                </div>
              ))}
              <div style={{ fontSize: 10, color: C.textMuted, margin: "0 4px" }}>vs B&H:</div>
              {[
                { label: "CAGR", val: d.strategy.bh_cagr, pct: true },
                { label: "Sharpe", val: d.strategy.bh_sharpe, pct: false },
                { label: "Max DD", val: d.strategy.bh_max_dd, pct: true },
              ].map((item, i) => (
                <div key={i} style={{ textAlign: "center", marginRight: 12 }}>
                  <div style={{ fontSize: 9, color: C.textMuted }}>{item.label}</div>
                  <div style={{ fontSize: 13, fontWeight: 700, color: C.textMuted }}>
                    {item.pct ? (item.val >= 0 ? "+" : "") + (item.val * 100).toFixed(1) + "%" : item.val.toFixed(2)}
                  </div>
                </div>
              ))}
              <div style={{ flex: 1 }} />
              <div style={{ display: "flex", gap: 5 }}>
                {(d.regimes || []).map((r: any, i: number) => (
                  <div key={i} style={{
                    textAlign: "center", padding: "4px 8px", borderRadius: 5, minWidth: 55,
                    background: r.name === d.regime ? regimeColor(r.name) + "33" : C.panelDark,
                    border: "1px solid " + (r.name === d.regime ? regimeColor(r.name) : C.border),
                  }}>
                    <div style={{ fontSize: 8, color: C.textMuted }}>{r.name.replace("Very ", "V.")}</div>
                    <div style={{ fontSize: 9, fontWeight: 700, color: r.avg_fwd3m < 0 ? C.red : C.green }}>
                      {(r.avg_fwd3m * 100).toFixed(2)}%
                    </div>
                    <div style={{ fontSize: 7, color: C.textMuted }}>n={r.n}</div>
                  </div>
                ))}
              </div>
            </div>
          )}

          <div style={{ background: C.panel, border: "1px solid " + C.border, borderRadius: 8, overflow: "hidden" }}>
            <div style={{ background: C.panelDark, padding: "10px 16px", fontSize: 10, fontWeight: 800,
              letterSpacing: 1.5, color: C.textMuted, borderBottom: "1px solid " + C.border, textTransform: "uppercase" }}>
              COMO LER ESTE DASHBOARD -- {names[sym].toUpperCase()}
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr" }}>
              {(EXPLANATIONS[sym] || []).map((ex, i) => (
                <div key={i} style={{
                  padding: "14px 18px",
                  borderRight: i % 2 === 0 ? "1px solid " + C.border : "none",
                  borderBottom: i < 2 ? "1px solid " + C.border : "none",
                }}>
                  <div style={{ fontSize: 11, fontWeight: 800, color: C.gold, marginBottom: 6 }}>{ex.title}</div>
                  <div style={{ fontSize: 11, color: C.textMuted, lineHeight: 1.65 }}>{ex.text}</div>
                </div>
              ))}
            </div>
            <div style={{ padding: "8px 18px", borderTop: "1px solid " + C.border, fontSize: 9, color: C.textMuted }}>
              Fonte: ISU AgDM B2-12 (2025) | CFTC COT Reports | AgriMacro Multi-Factor Model | Backtest 1990-2024 (LE), 2004-2024 (GF, HE) | AgriMacro v3.3
            </div>
          </div>
        </>
      )}
    </div>
  );
}
