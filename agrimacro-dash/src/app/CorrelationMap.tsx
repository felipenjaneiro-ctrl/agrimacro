"use client";
import { useState, useMemo } from "react";

/* ---------------------------------------------------------------------------
   AgriMacro Correlation Map — Interactive SVG visualization
   Reads from correlations.json (matrix, composite_signals, causal_chains, etc.)
   --------------------------------------------------------------------------- */

interface CorrelationMapProps {
  correlations: any;
  prices: any;  // price_history for latest prices
}

// -- Node positions (SVG coordinates in a 900x460 viewport) ----------------
const NODES: { sym: string; x: number; y: number; group: string }[] = [
  // Energia — center top (hub)
  { sym: "CL", x: 420, y: 60, group: "Energia" },
  { sym: "NG", x: 520, y: 55, group: "Energia" },
  // Grãos — left cluster
  { sym: "ZS", x: 120, y: 120, group: "Gr\u00e3os" },
  { sym: "ZC", x: 60, y: 210, group: "Gr\u00e3os" },
  { sym: "ZW", x: 140, y: 290, group: "Gr\u00e3os" },
  { sym: "KE", x: 60, y: 360, group: "Gr\u00e3os" },
  { sym: "ZM", x: 230, y: 170, group: "Gr\u00e3os" },
  { sym: "ZL", x: 280, y: 90, group: "Gr\u00e3os" },
  // Metais + Macro — right
  { sym: "GC", x: 760, y: 100, group: "Metais" },
  { sym: "SI", x: 840, y: 170, group: "Metais" },
  { sym: "DX", x: 700, y: 200, group: "Macro" },
  // Softs — center bottom
  { sym: "SB", x: 370, y: 360, group: "Softs" },
  { sym: "KC", x: 470, y: 400, group: "Softs" },
  { sym: "CT", x: 320, y: 430, group: "Softs" },
  { sym: "CC", x: 540, y: 370, group: "Softs" },
  // Pecuária — bottom right
  { sym: "LE", x: 700, y: 340, group: "Pecu\u00e1ria" },
  { sym: "GF", x: 790, y: 310, group: "Pecu\u00e1ria" },
  { sym: "HE", x: 750, y: 410, group: "Pecu\u00e1ria" },
];

const GROUP_COLORS: Record<string, string> = {
  "Energia": "#DC3C3C",
  "Gr\u00e3os": "#00C878",
  "Pecu\u00e1ria": "#DCB432",
  "Metais": "#A0A0FF",
  "Macro": "#FFFFFF",
  "Softs": "#FF9040",
};

const FRIENDLY: Record<string, string> = {
  ZC: "Corn", ZS: "Soybeans", ZW: "Wheat CBOT", KE: "Wheat KC",
  ZM: "Soybean Meal", ZL: "Soybean Oil", SB: "Sugar", KC: "Coffee",
  CT: "Cotton", CC: "Cocoa", LE: "Live Cattle", GF: "Feeder Cattle",
  HE: "Lean Hogs", CL: "Crude Oil", NG: "Natural Gas",
  GC: "Gold", SI: "Silver", DX: "Dollar Index",
};

const NODE_MAP = Object.fromEntries(NODES.map(n => [n.sym, n]));
const CONN_THRESHOLD = 0.35;

// Causal chain arrow definitions
const CHAIN_ARROWS: { id: string; path: [string, string][]; color: string }[] = [
  { id: "crush", path: [["ZS", "ZM"], ["ZS", "ZL"]], color: "#00C878" },
  { id: "energia_acucar", path: [["CL", "SB"], ["CL", "ZL"]], color: "#DC3C3C" },
  { id: "macro_risco", path: [["DX", "ZS"], ["DX", "ZC"], ["DX", "GC"], ["DX", "CL"]], color: "#FFFFFF" },
  { id: "energia_metais", path: [["CL", "DX"], ["DX", "GC"], ["GC", "SI"]], color: "#A0A0FF" },
];

export default function CorrelationMap({ correlations, prices }: CorrelationMapProps) {
  const [selected, setSelected] = useState<string | null>(null);
  const [showChains, setShowChains] = useState(false);

  const matrix = correlations?.matrix || {};
  const composites = correlations?.composite_signals || [];
  const fundamentals = correlations?.fundamental_signals || {};
  const lagged = correlations?.lagged || {};
  const chains = correlations?.causal_chains || [];

  // Build connections list
  const connections = useMemo(() => {
    const conns: { a: string; b: string; r: number }[] = [];
    const seen = new Set<string>();
    for (const a of Object.keys(matrix)) {
      for (const b of Object.keys(matrix[a] || {})) {
        const key = [a, b].sort().join("-");
        if (seen.has(key)) continue;
        seen.add(key);
        const r = matrix[a][b];
        if (Math.abs(r) >= CONN_THRESHOLD) {
          conns.push({ a, b, r });
        }
      }
    }
    return conns.sort((x, y) => Math.abs(y.r) - Math.abs(x.r));
  }, [matrix]);

  // Get latest price for a symbol
  const getPrice = (sym: string): number | null => {
    if (!prices) return null;
    const data = prices[sym];
    if (!data) return null;
    const records = Array.isArray(data) ? data : data.candles;
    if (!records || records.length === 0) return null;
    return records[records.length - 1]?.close || null;
  };

  // Get composite signal for a symbol
  const getComposite = (sym: string) =>
    composites.find((c: any) => c.asset === sym);

  // Get top correlations for selected symbol
  const getTopCorrs = (sym: string) => {
    const row = matrix[sym] || {};
    return Object.entries(row)
      .map(([k, v]) => ({ sym: k, r: v as number }))
      .sort((a, b) => Math.abs(b.r) - Math.abs(a.r))
      .slice(0, 8);
  };

  // Get lagged correlations involving symbol
  const getLagged = (sym: string) => {
    return Object.entries(lagged)
      .filter(([k]) => k.includes(sym))
      .map(([k, v]) => ({ key: k, r: v as number }))
      .sort((a, b) => Math.abs(b.r) - Math.abs(a.r))
      .slice(0, 6);
  };

  // Get chains involving symbol
  const getChains = (sym: string) =>
    chains.filter((c: any) => c.nodes?.includes(sym));

  const W = 900, H = 460;

  return (
    <div style={{ display: "flex", gap: 16, minHeight: 500 }}>
      {/* SVG Map */}
      <div style={{ flex: 1, position: "relative" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
          <div style={{ display: "flex", gap: 12 }}>
            {Object.entries(GROUP_COLORS).map(([g, c]) => (
              <div key={g} style={{ display: "flex", alignItems: "center", gap: 4 }}>
                <div style={{ width: 8, height: 8, borderRadius: "50%", background: c }} />
                <span style={{ fontSize: 9, color: "#8899AA" }}>{g}</span>
              </div>
            ))}
          </div>
          <button
            onClick={() => setShowChains(ch => !ch)}
            style={{
              fontSize: 9, fontWeight: 600, padding: "4px 12px", borderRadius: 4, cursor: "pointer",
              background: showChains ? "rgba(220,180,50,.15)" : "transparent",
              color: showChains ? "#DCB432" : "#8899AA",
              border: `1px solid ${showChains ? "#DCB43266" : "rgba(148,163,184,.15)"}`,
            }}
          >
            {showChains ? "\u2713 " : ""}Cadeias Causais
          </button>
        </div>

        <svg viewBox={`0 0 ${W} ${H}`} style={{ width: "100%", height: "auto", background: "#0E1A24", borderRadius: 8, border: "1px solid rgba(148,163,184,.08)" }}>
          <defs>
            <marker id="arrowG" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
              <path d="M0,0 L8,3 L0,6" fill="#DCB43288" />
            </marker>
          </defs>

          {/* Connection lines */}
          {connections.map(({ a, b, r }) => {
            const na = NODE_MAP[a], nb = NODE_MAP[b];
            if (!na || !nb) return null;
            const absR = Math.abs(r);
            const isPos = r > 0;
            const strong = absR >= 0.7;
            const dimmed = selected && selected !== a && selected !== b;
            const highlighted = selected && (selected === a || selected === b);
            return (
              <line
                key={`${a}-${b}`}
                x1={na.x} y1={na.y} x2={nb.x} y2={nb.y}
                stroke={isPos ? "#00C878" : "#DC3C3C"}
                strokeWidth={strong ? 2.5 : 1.2}
                strokeDasharray={strong ? "none" : "4,3"}
                opacity={dimmed ? 0.06 : highlighted ? 0.9 : 0.35}
                style={{ transition: "opacity .2s" }}
              />
            );
          })}

          {/* Causal chain arrows (overlay) */}
          {showChains && CHAIN_ARROWS.map(chain =>
            chain.path.map(([from, to]) => {
              const nf = NODE_MAP[from], nt = NODE_MAP[to];
              if (!nf || !nt) return null;
              const dx = nt.x - nf.x, dy = nt.y - nf.y;
              const len = Math.sqrt(dx * dx + dy * dy);
              const ux = dx / len, uy = dy / len;
              const x1 = nf.x + ux * 22, y1 = nf.y + uy * 22;
              const x2 = nt.x - ux * 22, y2 = nt.y - uy * 22;
              return (
                <line
                  key={`chain-${chain.id}-${from}-${to}`}
                  x1={x1} y1={y1} x2={x2} y2={y2}
                  stroke="#DCB43288" strokeWidth={2}
                  markerEnd="url(#arrowG)"
                  strokeDasharray="6,4"
                  opacity={0.7}
                >
                  <animate attributeName="stroke-dashoffset" from="20" to="0" dur="1.5s" repeatCount="indefinite" />
                </line>
              );
            })
          )}

          {/* Nodes */}
          {NODES.map(node => {
            const color = GROUP_COLORS[node.group] || "#888";
            const isSel = selected === node.sym;
            const dimmed = selected && !isSel && !connections.some(c =>
              (c.a === selected && c.b === node.sym) || (c.b === selected && c.a === node.sym)
            );
            const comp = getComposite(node.sym);
            const ringColor = comp?.signal === "BULLISH" ? "#00C878" : comp?.signal === "BEARISH" ? "#DC3C3C" : null;
            return (
              <g key={node.sym} onClick={() => setSelected(isSel ? null : node.sym)} style={{ cursor: "pointer" }}>
                {/* Outer ring for composite signal */}
                {ringColor && !dimmed && (
                  <circle cx={node.x} cy={node.y} r={21} fill="none" stroke={ringColor} strokeWidth={1.5} opacity={0.5} />
                )}
                {/* Selection pulse */}
                {isSel && (
                  <circle cx={node.x} cy={node.y} r={24} fill="none" stroke="#DCB432" strokeWidth={2} opacity={0.8}>
                    <animate attributeName="r" values="22;28;22" dur="1.5s" repeatCount="indefinite" />
                    <animate attributeName="opacity" values="0.8;0.2;0.8" dur="1.5s" repeatCount="indefinite" />
                  </circle>
                )}
                <circle
                  cx={node.x} cy={node.y} r={18}
                  fill={isSel ? color : `${color}22`}
                  stroke={color}
                  strokeWidth={isSel ? 2.5 : 1.5}
                  opacity={dimmed ? 0.15 : 1}
                  style={{ transition: "opacity .2s" }}
                />
                <text
                  x={node.x} y={node.y + 1}
                  textAnchor="middle" dominantBaseline="middle"
                  fill={isSel ? "#0E1A24" : dimmed ? "#444" : "#E8ECF1"}
                  fontSize={10} fontWeight={700} fontFamily="monospace"
                  style={{ transition: "fill .2s", pointerEvents: "none" }}
                >
                  {node.sym}
                </text>
              </g>
            );
          })}
        </svg>
      </div>

      {/* Detail Panel */}
      <div style={{ width: 260, minWidth: 260, background: "#142332", borderRadius: 8, padding: 14, border: "1px solid rgba(148,163,184,.08)", overflowY: "auto", maxHeight: 500 }}>
        {!selected ? (
          <div style={{ textAlign: "center", padding: "40px 10px" }}>
            <div style={{ fontSize: 11, color: "#64748b", lineHeight: 1.7 }}>
              Clique em um ativo no mapa para ver correla\u00e7\u00f5es, sinais e cadeias causais.
            </div>
            <div style={{ fontSize: 9, color: "#4a5568", marginTop: 12 }}>
              {connections.length} conex\u00f5es (|r| {">"} {CONN_THRESHOLD})
            </div>
          </div>
        ) : (() => {
          const price = getPrice(selected);
          const comp = getComposite(selected);
          const topCorrs = getTopCorrs(selected);
          const laggedCorrs = getLagged(selected);
          const relChains = getChains(selected);
          const fund = fundamentals[selected] || {};
          const compColor = comp?.signal === "BULLISH" ? "#00C878" : comp?.signal === "BEARISH" ? "#DC3C3C" : "#8899AA";
          return (
            <div>
              {/* Header */}
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
                <div>
                  <div style={{ fontSize: 16, fontWeight: 800, color: GROUP_COLORS[NODE_MAP[selected]?.group || ""] || "#FFF", fontFamily: "monospace" }}>{selected}</div>
                  <div style={{ fontSize: 9, color: "#8899AA" }}>{FRIENDLY[selected]}</div>
                </div>
                {price != null && (
                  <div style={{ fontSize: 14, fontWeight: 700, fontFamily: "monospace", color: "#FFFFFF" }}>{price.toFixed(2)}</div>
                )}
              </div>

              {/* Composite signal */}
              {comp && (
                <div style={{ background: `${compColor}12`, border: `1px solid ${compColor}33`, borderRadius: 6, padding: 8, marginBottom: 10 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <div style={{ fontSize: 11, fontWeight: 800, color: compColor }}>{comp.signal}</div>
                    <div style={{ fontSize: 9, color: "#8899AA" }}>conf. {(comp.confidence * 100).toFixed(0)}% ({comp.sources_count} fontes)</div>
                  </div>
                  {comp.factors_bull?.length > 0 && comp.factors_bull.slice(0, 2).map((f: string, i: number) => (
                    <div key={`b${i}`} style={{ fontSize: 8, color: "#00C878", marginTop: 3 }}>+ {f}</div>
                  ))}
                  {comp.factors_bear?.length > 0 && comp.factors_bear.slice(0, 2).map((f: string, i: number) => (
                    <div key={`r${i}`} style={{ fontSize: 8, color: "#DC3C3C", marginTop: 3 }}>{"\u2013"} {f}</div>
                  ))}
                </div>
              )}

              {/* Fundamentals */}
              {Object.keys(fund).length > 0 && (
                <div style={{ marginBottom: 10 }}>
                  <div style={{ fontSize: 9, fontWeight: 600, color: "#8899AA", letterSpacing: 0.5, marginBottom: 4 }}>FUNDAMENTOS</div>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 4, fontSize: 9 }}>
                    {fund.cot_mm_net != null && <><div style={{ color: "#8899AA" }}>COT MM Net</div><div style={{ color: "#E8ECF1", fontFamily: "monospace" }}>{(fund.cot_mm_net / 1000).toFixed(0)}k</div></>}
                    {fund.cot_index != null && <><div style={{ color: "#8899AA" }}>COT Index</div><div style={{ color: fund.cot_signal === "BEAR" ? "#DC3C3C" : fund.cot_signal === "BULL" ? "#00C878" : "#E8ECF1", fontFamily: "monospace" }}>{fund.cot_index.toFixed(0)}/100</div></>}
                    {fund.stu_z_score != null && <><div style={{ color: "#8899AA" }}>STU z-score</div><div style={{ color: fund.stu_z_score > 1 ? "#DC3C3C" : fund.stu_z_score < -1 ? "#00C878" : "#E8ECF1", fontFamily: "monospace" }}>{fund.stu_z_score.toFixed(1)}</div></>}
                    {fund.margin_vs_cop != null && <><div style={{ color: "#8899AA" }}>Margem COP</div><div style={{ color: fund.margin_vs_cop < 0 ? "#DC3C3C" : "#00C878", fontFamily: "monospace" }}>{fund.margin_vs_cop.toFixed(2)}</div></>}
                  </div>
                </div>
              )}

              {/* Top correlations */}
              <div style={{ marginBottom: 10 }}>
                <div style={{ fontSize: 9, fontWeight: 600, color: "#8899AA", letterSpacing: 0.5, marginBottom: 4 }}>CORRELA\u00c7\u00d5ES (252d)</div>
                {topCorrs.map(c => (
                  <div key={c.sym} style={{ display: "flex", justifyContent: "space-between", alignItems: "center", padding: "3px 0" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      <div style={{ width: 6, height: 6, borderRadius: "50%", background: GROUP_COLORS[NODE_MAP[c.sym]?.group || ""] || "#888" }} />
                      <span style={{ fontSize: 10, color: "#E8ECF1", fontFamily: "monospace" }}>{c.sym}</span>
                    </div>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      <div style={{ width: 40, height: 4, background: "rgba(148,163,184,.1)", borderRadius: 2, overflow: "hidden" }}>
                        <div style={{
                          height: "100%", borderRadius: 2,
                          width: `${Math.abs(c.r) * 100}%`,
                          background: c.r > 0 ? "#00C878" : "#DC3C3C",
                          marginLeft: c.r < 0 ? "auto" : 0,
                        }} />
                      </div>
                      <span style={{ fontSize: 9, fontFamily: "monospace", fontWeight: 600, color: c.r > 0 ? "#00C878" : "#DC3C3C", minWidth: 38, textAlign: "right" }}>{c.r > 0 ? "+" : ""}{c.r.toFixed(2)}</span>
                    </div>
                  </div>
                ))}
              </div>

              {/* Lagged correlations */}
              {laggedCorrs.length > 0 && (
                <div style={{ marginBottom: 10 }}>
                  <div style={{ fontSize: 9, fontWeight: 600, color: "#8899AA", letterSpacing: 0.5, marginBottom: 4 }}>DEFASAGEM (lidera/segue)</div>
                  {laggedCorrs.map(l => {
                    const parts = l.key.replace("COT_MM_", "COT ").replace("_leads_", " \u2192 ").replace(/_(\d+)d$/, " ($1d)");
                    return (
                      <div key={l.key} style={{ display: "flex", justifyContent: "space-between", padding: "2px 0", fontSize: 9 }}>
                        <span style={{ color: "#8899AA" }}>{parts}</span>
                        <span style={{ fontFamily: "monospace", fontWeight: 600, color: l.r > 0 ? "#00C878" : "#DC3C3C" }}>{l.r > 0 ? "+" : ""}{l.r.toFixed(2)}</span>
                      </div>
                    );
                  })}
                </div>
              )}

              {/* Causal chains */}
              {relChains.length > 0 && (
                <div>
                  <div style={{ fontSize: 9, fontWeight: 600, color: "#8899AA", letterSpacing: 0.5, marginBottom: 4 }}>CADEIAS CAUSAIS</div>
                  {relChains.map((ch: any) => (
                    <div key={ch.id} style={{ background: "#0E1A24", borderRadius: 4, padding: 6, marginBottom: 4, border: "1px solid rgba(220,180,50,.1)" }}>
                      <div style={{ fontSize: 9, fontWeight: 700, color: "#DCB432" }}>{ch.name}</div>
                      <div style={{ fontSize: 8, color: "#8899AA", marginTop: 2 }}>{ch.current_signal}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          );
        })()}
      </div>
    </div>
  );
}
