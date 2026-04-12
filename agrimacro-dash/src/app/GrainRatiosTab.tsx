// GrainRatiosTab.tsx
// AgriMacro Intelligence -- Aba Grain Ratios & Arbitragem
// Lê: /data/processed/grain_ratios.json

import { useEffect, useState } from "react"

const C = {
  bg: "#0E1A24", panel: "#142332", border: "#1E3044",
  text: "#E8ECF1", textMuted: "#8C96A5",
  green: "#00C878", red: "#DC3C3C", gold: "#DCB432",
  blue: "#468CDC", cyan: "#00C8DC",
}

function Badge({ signal }: { signal: string }) {
  const color = signal === "BULL" ? C.green : signal === "BEAR" ? C.red : C.textMuted
  return (
    <span style={{
      background: color, color: "#fff", fontSize: 9, fontWeight: 700,
      padding: "2px 8px", borderRadius: 4, letterSpacing: 1,
    }}>{signal}</span>
  )
}

function SectionTitle({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      background: C.bg, color: C.gold, fontWeight: 700, fontSize: 11,
      padding: "6px 12px", borderRadius: 4, marginBottom: 8, letterSpacing: 0.5,
      borderLeft: `3px solid ${C.gold}`,
    }}>{children}</div>
  )
}

function Panel({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) {
  return (
    <div style={{
      background: C.panel, border: `1px solid ${C.border}`, borderRadius: 8,
      padding: 14, ...style,
    }}>{children}</div>
  )
}

function StatRow({ label, value, color, sub }: { label: string; value: string; color?: string; sub?: string }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center",
                  padding: "4px 0", borderBottom: `1px solid ${C.border}` }}>
      <span style={{ fontSize: 11, color: C.textMuted }}>{label}</span>
      <div style={{ textAlign: "right" }}>
        <span style={{ fontSize: 12, fontWeight: 600, color: color || C.text }}>{value}</span>
        {sub && <div style={{ fontSize: 9, color: C.textMuted }}>{sub}</div>}
      </div>
    </div>
  )
}

export default function GrainRatiosTab() {
  const [gr, setGr] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [revalidating, setRevalidating] = useState(false)
  const [error, setError] = useState("")

  const fetchData = (isRefresh = false) => {
    if (isRefresh) setRevalidating(true)
    fetch("/data/processed/grain_ratios.json")
      .then(r => r.ok ? r.json() : Promise.reject("grain_ratios.json nao encontrado"))
      .then(d => {
        if (d && typeof d === "object" && Object.keys(d).length > 0) {
          setGr(d)
          setError("")
        } else if (!gr) {
          setError("JSON vazio retornado")
        }
      })
      .catch(e => { if (!gr) setError(String(e)) })
      .finally(() => { setLoading(false); setRevalidating(false) })
  }

  useEffect(() => { fetchData() }, [])

  // Sem dados nenhum ainda (primeiro load)
  if (loading && !gr) return (
    <div style={{ padding: 40, textAlign: "center", color: C.textMuted }}>
      Carregando Grain Ratios...
    </div>
  )
  if (!gr) return (
    <div style={{ padding: 40, textAlign: "center", color: C.red }}>
      {error || "Dados nao disponíveis. Execute grain_ratio_engine.py primeiro."}
    </div>
  )

  const snap  = gr.current_snapshot || {}
  const scs   = gr.scorecards || {}
  const arb   = gr.arbitrage || {}
  const mdl   = gr.model_results || {}
  const stubs = gr.stu_backtest || {}
  const cops  = gr.cop_backtest || {}
  const rank  = gr.factor_ranking || []
  const seas  = gr.seasonality || {}
  const meta  = gr.meta || {}

  const grainPt: Record<string, string> = {
    corn: "Milho (ZC)", soy: "Soja (ZS)", wheat: "Trigo (ZW)"
  }
  const bucketPt: Record<string, string> = {
    critico_lt8: "Crítico <8%", apertado_8_12: "Apertado 8-12%",
    normal_12_18: "Normal 12-18%", folgado_gt18: "Folgado >18%"
  }
  const monthNames = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]

  function nv(v: any, d = "N/A") {
    if (v === null || v === undefined) return d
    const n = parseFloat(v)
    return isNaN(n) ? d : n.toFixed(2)
  }
  function pct(v: any) {
    const n = parseFloat(v)
    return isNaN(n) ? "N/A" : `${n > 0 ? "+" : ""}${n.toFixed(1)}%`
  }
  function scoreColor(v: number) { return v > 0 ? C.green : v < 0 ? C.red : C.textMuted }

  const cif   = arb.spread_delivered_china?.cif_qingdao || {}
  const sp    = arb.spread_delivered_china?.spreads || {}
  const adv   = arb.spread_delivered_china?.competitive_advantage || {}
  const bdi   = arb.bdi || {}
  const bGulf = arb.basis_gulf || {}
  const bBR   = arb.basis_br || {}
  const fobPr = arb.fob_paranagua || {}
  const fobRo = arb.fob_rosario || {}

  return (
    <div style={{ padding: "16px 20px", color: C.text, fontSize: 12, opacity: revalidating ? 0.85 : 1, transition: "opacity .3s" }}>

      {revalidating && (
        <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8, padding: "4px 10px",
          background: "rgba(220,180,50,.08)", border: "1px solid rgba(220,180,50,.25)", borderRadius: 6, width: "fit-content" }}>
          <div style={{ width: 8, height: 8, borderRadius: "50%", border: "2px solid #DCB432", borderTopColor: "transparent",
            animation: "spin .8s linear infinite" }} />
          <span style={{ fontSize: 10, color: "#DCB432", fontWeight: 600 }}>Atualizando...</span>
        </div>
      )}

      {/* META HEADER */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center",
                    marginBottom: 16, padding: "8px 14px",
                    background: C.panel, borderRadius: 8, border: `1px solid ${C.border}` }}>
        <div>
          <span style={{ fontWeight: 700, fontSize: 14, color: C.gold }}>
            Grain Ratios & Arbitragem de Origem
          </span>
          <span style={{ marginLeft: 12, fontSize: 10, color: C.textMuted }}>
            Engine v{meta.engine_version} | {meta.n_months} meses | Treino: {meta.train_period}
          </span>
        </div>
        <div style={{ fontSize: 10, color: C.textMuted }}>
          Atualizado: {meta.generated_at ? new Date(meta.generated_at).toLocaleString("pt-BR") : "N/A"}
        </div>
      </div>

      {/* ROW 1: SCORECARDS */}
      <SectionTitle> Scorecard Multi-Fator -- Sinal Atual</SectionTitle>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10, marginBottom: 16 }}>
        {["corn","soy","wheat"].map(grain => {
          const sc = scs[grain] || {}
          const signals = sc.signals || []
          return (
            <Panel key={grain}>
              <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
                <span style={{ fontWeight: 700, fontSize: 12 }}>{grainPt[grain]}</span>
                <Badge signal={sc.composite_signal || "N/A"} />
              </div>
              <div style={{ fontSize: 10, color: C.textMuted, marginBottom: 8 }}>
                Score: <span style={{ color: scoreColor(sc.composite_score || 0), fontWeight: 700 }}>
                  {sc.composite_score > 0 ? "+" : ""}{sc.composite_score}
                </span>
                &nbsp;|&nbsp; Bull: {sc.bull_weight} &nbsp;Bear: {sc.bear_weight}
              </div>
              {signals.map((s: any, i: number) => (
                <div key={i} style={{ display: "flex", gap: 6, alignItems: "flex-start",
                                      padding: "3px 0", borderBottom: `1px solid ${C.border}` }}>
                  <Badge signal={s.signal} />
                  <div>
                    <div style={{ fontSize: 10, fontWeight: 600 }}>{s.factor}</div>
                    <div style={{ fontSize: 9, color: C.textMuted }}>{s.detail}</div>
                  </div>
                </div>
              ))}
            </Panel>
          )
        })}
      </div>

      {/* ROW 2: MODELO + RATIOS */}
      <div style={{ display: "grid", gridTemplateColumns: "1.4fr 1fr", gap: 10, marginBottom: 16 }}>

        {/* Acurácia do Modelo */}
        <div>
          <SectionTitle> Acurácia do Modelo (Walk-Forward Lasso)</SectionTitle>
          <Panel>
            <div style={{ fontSize: 10, color: C.textMuted, marginBottom: 8, fontStyle: "italic" }}>
              R² out-of-sample = acurácia em dados nunca vistos pelo modelo (2020-2024)
            </div>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
              <thead>
                <tr style={{ background: C.bg }}>
                  {["Commodity","Horizonte","R² In-Sample","R² Out-Sample","Dir.Accuracy","N Teste"].map(h => (
                    <th key={h} style={{ padding: "4px 6px", textAlign: "center",
                                        color: C.textMuted, fontWeight: 600, fontSize: 9 }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {["corn","soy","wheat"].map(grain =>
                  ["3m","6m","12m"].map((hz, i) => {
                    const r = mdl[grain]?.[hz]
                    if (!r) return null
                    const r2out = parseFloat(r.r2_out_of_sample)
                    const r2color = r2out > 30 ? C.green : r2out > 15 ? C.gold : C.red
                    return (
                      <tr key={`${grain}-${hz}`} style={{ background: i % 2 === 0 ? "transparent" : C.bg }}>
                        <td style={{ padding: "4px 6px", color: C.gold, fontWeight: 600 }}>
                          {i === 0 ? grainPt[grain] : ""}
                        </td>
                        <td style={{ padding: "4px 6px", textAlign: "center" }}>Fwd {hz}</td>
                        <td style={{ padding: "4px 6px", textAlign: "center", color: C.textMuted }}>
                          {r.r2_in_sample}%
                        </td>
                        <td style={{ padding: "4px 6px", textAlign: "center",
                                     color: r2color, fontWeight: 700 }}>
                          {r.r2_out_of_sample}%
                        </td>
                        <td style={{ padding: "4px 6px", textAlign: "center",
                                     color: parseFloat(r.directional_accuracy) > 60 ? C.green : C.text }}>
                          {r.directional_accuracy}%
                        </td>
                        <td style={{ padding: "4px 6px", textAlign: "center", color: C.textMuted }}>
                          {r.n_test}
                        </td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </Panel>
        </div>

        {/* Ratios Atuais */}
        <div>
          <SectionTitle> Ratios Históricos -- Estado Atual</SectionTitle>
          <Panel>
            {Object.entries(snap.ratios || {}).map(([key, val]: [string, any]) => {
              if (!val) return null
              const pctVal = parseFloat(val.pct)
              const barColor = pctVal < 20 ? C.green : pctVal > 80 ? C.red : C.gold
              const labels: Record<string, string> = {
                corn_soy: "Corn/Soy", wheat_corn: "Wheat/Corn",
                corn_cattle: "Corn/Cattle", crush_spread: "Crush Spread", oil_crude: "SoyOil/Crude"
              }
              return (
                <div key={key} style={{ marginBottom: 10 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
                    <span style={{ fontSize: 10, color: C.textMuted }}>{labels[key] || key}</span>
                    <div style={{ textAlign: "right" }}>
                      <span style={{ fontSize: 11, fontWeight: 700 }}>{nv(val.current)}</span>
                      <span style={{ fontSize: 9, color: C.textMuted, marginLeft: 6 }}>
                        Z: {nv(val.z_score)} | P: {val.pct?.toFixed(0)}%
                      </span>
                    </div>
                  </div>
                  {/* Barra percentil */}
                  <div style={{ background: C.bg, borderRadius: 3, height: 5, position: "relative" }}>
                    <div style={{
                      width: `${Math.min(100, Math.max(0, pctVal))}%`,
                      background: barColor, height: 5, borderRadius: 3,
                    }} />
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between",
                                fontSize: 8, color: C.textMuted }}>
                    <span>Min {nv(val.min)}</span>
                    <span style={{ color: barColor, fontWeight: 700 }}>{val.status?.replace(/_/g," ")}</span>
                    <span>Max {nv(val.max)}</span>
                  </div>
                </div>
              )
            })}
          </Panel>
        </div>
      </div>

      {/* ROW 3: STU BACKTEST + COP BACKTEST + FATORES */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10, marginBottom: 16 }}>

        {/* STU Backtest */}
        <div>
          <SectionTitle> STU Backtest -- Retorno 12m por Bucket</SectionTitle>
          <Panel style={{ padding: 10 }}>
            {["corn","soy","wheat"].map(grain => (
              <div key={grain} style={{ marginBottom: 10 }}>
                <div style={{ color: C.gold, fontWeight: 700, fontSize: 10,
                              borderBottom: `1px solid ${C.border}`, marginBottom: 4, paddingBottom: 2 }}>
                  {grainPt[grain]}
                </div>
                {Object.entries(stubs[grain] || {}).map(([bk, bd]: [string, any]) => {
                  const avg = parseFloat(bd.avg_fwd12m)
                  return (
                    <div key={bk} style={{ display: "flex", justifyContent: "space-between",
                                           fontSize: 10, padding: "2px 0" }}>
                      <span style={{ color: C.textMuted }}>{bucketPt[bk]}</span>
                      <div style={{ textAlign: "right" }}>
                        <span style={{ fontWeight: 700, color: avg > 0 ? C.green : C.red }}>
                          {avg > 0 ? "+" : ""}{avg.toFixed(1)}%
                        </span>
                        <span style={{ fontSize: 9, color: C.textMuted, marginLeft: 6 }}>
                          ({bd.pct_positive}%^ n={bd.n})
                        </span>
                      </div>
                    </div>
                  )
                })}
              </div>
            ))}
          </Panel>
        </div>

        {/* COP Backtest */}
        <div>
          <SectionTitle> Margem vs COP -- Retorno Fwd 12m</SectionTitle>
          <Panel style={{ padding: 10 }}>
            {["corn","soy","wheat"].map(grain => {
              const c = cops[grain] || {}
              const mg = snap.margins?.[grain] || {}
              const below = c.below_cop || {}
              const above = c.above_cop || {}
              return (
                <div key={grain} style={{ marginBottom: 12 }}>
                  <div style={{ color: C.gold, fontWeight: 700, fontSize: 10,
                                borderBottom: `1px solid ${C.border}`, marginBottom: 4, paddingBottom: 2 }}>
                    {grainPt[grain]}
                  </div>
                  <StatRow
                    label={`Preço atual: $${nv(mg.price)}`}
                    value={`COP: $${nv(mg.cop)}`}
                    color={parseFloat(mg.margin) < 0 ? C.red : C.green}
                    sub={`Margem: ${parseFloat(mg.margin) > 0 ? "+" : ""}${nv(mg.margin)}`}
                  />
                  <div style={{ display: "flex", gap: 8, marginTop: 6 }}>
                    <div style={{ flex: 1, background: C.bg, borderRadius: 5, padding: 6 }}>
                      <div style={{ fontSize: 9, color: C.textMuted }}>Quando P &lt; COP</div>
                      <div style={{ fontSize: 11, fontWeight: 700, color: C.green }}>
                        {below.avg_fwd12m > 0 ? "+" : ""}{below.avg_fwd12m}%
                      </div>
                      <div style={{ fontSize: 9, color: C.textMuted }}>n={below.n} | {below.pct_positive}%^</div>
                    </div>
                    <div style={{ flex: 1, background: C.bg, borderRadius: 5, padding: 6 }}>
                      <div style={{ fontSize: 9, color: C.textMuted }}>{"Quando P >= COP"}</div>
                      <div style={{ fontSize: 11, fontWeight: 700, color: C.textMuted }}>
                        {above.avg_fwd12m > 0 ? "+" : ""}{above.avg_fwd12m}%
                      </div>
                      <div style={{ fontSize: 9, color: C.textMuted }}>n={above.n} | {above.pct_positive}%^</div>
                    </div>
                  </div>
                </div>
              )
            })}
          </Panel>
        </div>

        {/* Fatores ranking + Sazonalidade */}
        <div>
          <SectionTitle> Top Fatores (Lasso)</SectionTitle>
          <Panel style={{ marginBottom: 10, padding: 10 }}>
            {rank.slice(0, 8).map((r: any, i: number) => (
              <div key={r.factor} style={{ display: "flex", alignItems: "center",
                                          gap: 6, padding: "3px 0",
                                          borderBottom: `1px solid ${C.border}` }}>
                <span style={{ fontSize: 9, color: C.textMuted, width: 14 }}>{i+1}</span>
                <span style={{ fontSize: 10, flex: 1 }}>{r.factor.replace(/_/g," ")}</span>
                <div style={{ background: C.bg, borderRadius: 3, width: 60, height: 6 }}>
                  <div style={{ width: `${Math.min(100, r.total_importance)}%`,
                                background: C.blue, height: 6, borderRadius: 3 }} />
                </div>
                <span style={{ fontSize: 9, color: C.gold, width: 36, textAlign: "right" }}>
                  {r.total_importance.toFixed(0)}pt
                </span>
              </div>
            ))}
          </Panel>

          <SectionTitle> Sazonalidade Histórica Real (milho)</SectionTitle>
          <Panel style={{ padding: 10 }}>
            <div style={{ display: "flex", gap: 2, flexWrap: "wrap" }}>
              {monthNames.map((mn, i) => {
                const val = seas.corn?.[i+1] || 0
                const col = val > 0.5 ? C.green : val < -0.5 ? C.red : C.textMuted
                return (
                  <div key={mn} style={{ textAlign: "center", flex: 1, minWidth: 28 }}>
                    <div style={{ fontSize: 8, color: C.textMuted }}>{mn}</div>
                    <div style={{ fontSize: 10, fontWeight: 700, color: col }}>
                      {val > 0 ? "+" : ""}{val.toFixed(1)}%
                    </div>
                  </div>
                )
              })}
            </div>
            <div style={{ fontSize: 9, color: C.textMuted, marginTop: 6, fontStyle: "italic" }}>
              Desvio médio mensal real (yfinance 2000-2024)
            </div>
          </Panel>
        </div>
      </div>

      {/* ROW 4: ARBITRAGEM DE ORIGEM (destaque) */}
      <SectionTitle> Arbitragem de Origem -- Spread CIF Qingdao (USD/ton)</SectionTitle>
      <Panel>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center",
                      marginBottom: 10 }}>
          <div style={{ display: "flex", gap: 20 }}>
            <div>
              <span style={{ fontSize: 10, color: C.textMuted }}>Baltic Dry Index: </span>
              <span style={{ fontSize: 13, fontWeight: 700, color: C.gold }}>
                {bdi.value || "N/A"} pts
              </span>
              <span style={{ fontSize: 9, color: C.textMuted, marginLeft: 6 }}>({bdi.date || "N/A"})</span>
            </div>
            <div>
              <span style={{ fontSize: 10, color: C.textMuted }}>Frete Gulf {"->"} China: </span>
              <span style={{ fontSize: 12, fontWeight: 700 }}>
                ${arb.spread_delivered_china?.freight_gulf_china_per_ton || "?"}/ton
              </span>
            </div>
            <div>
              <span style={{ fontSize: 10, color: C.textMuted }}>Frete Santos {"->"} China: </span>
              <span style={{ fontSize: 12, fontWeight: 700 }}>
                ${arb.spread_delivered_china?.freight_santos_china_per_ton || "?"}/ton
              </span>
            </div>
          </div>
          <div style={{ fontSize: 9, color: C.textMuted, textAlign: "right" }}>
            Frete via regressão BDI histórica<br/>
            Precisão estimada: ±15%
          </div>
        </div>

        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
          <thead>
            <tr style={{ background: C.bg }}>
              {["Commodity","FOB Gulf (US)","FOB Paranaguá (BR)","FOB Rosário (ARG)",
                "CIF Qingdao US","CIF Qingdao BR","CIF Qingdao ARG","Spread US-BR","Vantagem"].map(h => (
                <th key={h} style={{ padding: "5px 8px", textAlign: "center",
                                     color: C.textMuted, fontWeight: 600, fontSize: 9 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {[
              {
                label: "Soja (ZS)",
                fob_us: arb.spread_delivered_china?.fob_gulf?.soy,
                fob_br: fobPr.soy, fob_arg: fobRo.soy,
                cif_us: cif.soy_us, cif_br: cif.soy_br, cif_arg: cif.soy_arg,
                spread: sp.soy_us_vs_br, adv: adv.soy_china,
              },
              {
                label: "Milho (ZC)",
                fob_us: arb.spread_delivered_china?.fob_gulf?.corn,
                fob_br: fobPr.corn, fob_arg: fobRo.corn,
                cif_us: cif.corn_us, cif_br: cif.corn_br, cif_arg: cif.corn_arg,
                spread: sp.corn_us_vs_br, adv: adv.corn_china,
              },
            ].map((row, i) => {
              const spreadVal = parseFloat(row.spread)
              const spreadColor = spreadVal > 8 ? C.red : spreadVal < -8 ? C.green : C.textMuted
              const advColor = row.adv?.includes("BR") ? C.red : row.adv?.includes("US") ? C.green : C.textMuted
              return (
                <tr key={i} style={{ background: i % 2 === 0 ? "transparent" : C.bg }}>
                  <td style={{ padding: "6px 8px", fontWeight: 700, color: C.gold }}>{row.label}</td>
                  <td style={{ padding: "6px 8px", textAlign: "center" }}>${nv(row.fob_us, "N/A")}</td>
                  <td style={{ padding: "6px 8px", textAlign: "center" }}>${nv(row.fob_br, "N/A")}</td>
                  <td style={{ padding: "6px 8px", textAlign: "center" }}>${nv(row.fob_arg, "N/A")}</td>
                  <td style={{ padding: "6px 8px", textAlign: "center" }}>${nv(row.cif_us, "N/A")}</td>
                  <td style={{ padding: "6px 8px", textAlign: "center" }}>${nv(row.cif_br, "N/A")}</td>
                  <td style={{ padding: "6px 8px", textAlign: "center" }}>${nv(row.cif_arg, "N/A")}</td>
                  <td style={{ padding: "6px 8px", textAlign: "center",
                               fontWeight: 700, color: spreadColor }}>
                    {isNaN(spreadVal) ? "N/A" : `${spreadVal > 0 ? "+" : ""}${spreadVal.toFixed(1)}`}
                  </td>
                  <td style={{ padding: "6px 8px", textAlign: "center",
                               fontWeight: 700, color: advColor }}>{row.adv || "N/A"}</td>
                </tr>
              )
            })}
          </tbody>
        </table>

        {/* Basis monitor */}
        <div style={{ display: "flex", gap: 12, marginTop: 12 }}>
          {[
            { label: "Basis Gulf Milho", value: bGulf.corn, unit: "cents/bu" },
            { label: "Basis Gulf Soja",  value: bGulf.soy,  unit: "cents/bu" },
            { label: "Basis BR Soja",    value: bBR.soy,    unit: "USD/ton" },
            { label: "Basis BR Milho",   value: bBR.corn,   unit: "USD/ton" },
          ].map(b => {
            const val = parseFloat(b.value)
            const col = val > 0 ? C.green : val < 0 ? C.red : C.textMuted
            return (
              <div key={b.label} style={{ flex: 1, background: C.bg, borderRadius: 6, padding: 8 }}>
                <div style={{ fontSize: 9, color: C.textMuted }}>{b.label}</div>
                <div style={{ fontSize: 14, fontWeight: 700, color: isNaN(val) ? C.textMuted : col }}>
                  {isNaN(val) ? "N/A" : `${val > 0 ? "+" : ""}${val.toFixed(1)}`}
                </div>
                <div style={{ fontSize: 9, color: C.textMuted }}>{b.unit}</div>
              </div>
            )
          })}
        </div>

        <div style={{ marginTop: 8, fontSize: 9, color: C.textMuted, fontStyle: "italic" }}>
          Fontes: yfinance CME | USDA AMS Basis | CEPEA Paranaguá | MAGyP Rosário |
          Baltic Dry Index via Macrotrends | USDA WASDE/PSD | CFTC COT
        </div>
      </Panel>
    </div>
  )
}
