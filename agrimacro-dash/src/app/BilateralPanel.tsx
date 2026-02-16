"use client";
import { useState, useEffect } from "react";

// ============================================================
// AgriMacro Color Palette (matches dashboard.tsx C object)
// ============================================================
const C = {
  bg: "#0E1A24",
  panel: "#142332",
  border: "#1E3044",
  text: "#E8ECF1",
  textMuted: "#8C96A5",
  green: "#00C878",
  red: "#DC3C3C",
  gold: "#DCB432",
  blue: "#468CDC",
  cyan: "#00C8DC",
};

// ============================================================
// TYPES
// ============================================================
interface BilateralData {
  generated_at: string;
  date: string;
  summary: {
    lcs_spread: number | null;
    lcs_origin: string | null;
    ert_leader: string | null;
    ert_br_share: number | null;
    bci_score: number | null;
    bci_signal: string | null;
  };
  lcs: any;
  ert: any;
  bci: any;
}

// ============================================================
// HELPER COMPONENTS
// ============================================================

function Gauge({ value, max = 100, zones, size = 120 }: {
  value: number; max?: number; zones: { min: number; max: number; color: string; label: string }[]; size?: number;
}) {
  const pct = Math.min(Math.max(value / max, 0), 1);
  const angle = -135 + pct * 270; // -135 to +135 degrees
  const r = size / 2 - 10;
  const cx = size / 2;
  const cy = size / 2;

  return (
    <svg width={size} height={size * 0.7} viewBox={`0 0 ${size} ${size * 0.75}`}>
      {/* Background arc segments */}
      {zones.map((zone, i) => {
        const startPct = zone.min / max;
        const endPct = zone.max / max;
        const startAngle = (-135 + startPct * 270) * Math.PI / 180;
        const endAngle = (-135 + endPct * 270) * Math.PI / 180;
        const x1 = cx + r * Math.cos(startAngle);
        const y1 = cy + r * Math.sin(startAngle);
        const x2 = cx + r * Math.cos(endAngle);
        const y2 = cy + r * Math.sin(endAngle);
        const largeArc = (endPct - startPct) > 0.5 ? 1 : 0;
        return (
          <path
            key={i}
            d={`M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2}`}
            fill="none"
            stroke={zone.color}
            strokeWidth={8}
            opacity={0.3}
          />
        );
      })}
      {/* Active arc */}
      {(() => {
        const startAngle = -135 * Math.PI / 180;
        const endAngle = angle * Math.PI / 180;
        const x1 = cx + r * Math.cos(startAngle);
        const y1 = cy + r * Math.sin(startAngle);
        const x2 = cx + r * Math.cos(endAngle);
        const y2 = cy + r * Math.sin(endAngle);
        const largeArc = pct > 0.5 ? 1 : 0;
        const color = zones.find(z => value >= z.min && value < z.max)?.color || C.gold;
        return (
          <path
            d={`M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2}`}
            fill="none"
            stroke={color}
            strokeWidth={8}
            strokeLinecap="round"
          />
        );
      })()}
      {/* Needle */}
      {(() => {
        const needleAngle = angle * Math.PI / 180;
        const nx = cx + (r - 15) * Math.cos(needleAngle);
        const ny = cy + (r - 15) * Math.sin(needleAngle);
        return <line x1={cx} y1={cy} x2={nx} y2={ny} stroke={C.text} strokeWidth={2} />;
      })()}
      <circle cx={cx} cy={cy} r={3} fill={C.text} />
      <text x={cx} y={cy + 18} textAnchor="middle" fill={C.text} fontSize={20} fontWeight="bold">
        {value.toFixed(0)}
      </text>
    </svg>
  );
}

function HBar({ label, value, maxValue, color, showValue = true }: {
  label: string; value: number; maxValue: number; color: string; showValue?: boolean;
}) {
  const pct = maxValue > 0 ? Math.min(value / maxValue * 100, 100) : 0;
  return (
    <div style={{ marginBottom: 6 }}>
      <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, color: C.textMuted, marginBottom: 2 }}>
        <span>{label}</span>
        {showValue && <span style={{ color: C.text }}>${value.toFixed(0)}/mt</span>}
      </div>
      <div style={{ background: C.border, borderRadius: 3, height: 8, overflow: "hidden" }}>
        <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: 3, transition: "width 0.5s" }} />
      </div>
    </div>
  );
}

function ComponentBar({ name, score, weight, signal }: {
  name: string; score: number; weight: number; signal: string;
}) {
  const color = signal === "BULLISH" ? C.green : signal === "BEARISH" ? C.red : C.gold;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4, fontSize: 11 }}>
      <span style={{ color: C.textMuted, width: 130, flexShrink: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {name}
      </span>
      <div style={{ flex: 1, background: C.border, borderRadius: 2, height: 6, overflow: "hidden" }}>
        <div style={{ width: `${score}%`, height: "100%", background: color, borderRadius: 2 }} />
      </div>
      <span style={{ color, width: 32, textAlign: "right", fontWeight: 600 }}>{score.toFixed(0)}</span>
      <span style={{ color: C.textMuted, width: 24, textAlign: "right" }}>{weight}%</span>
    </div>
  );
}

function SignalBadge({ text, type }: { text: string; type: "positive" | "negative" | "neutral" }) {
  const colors = {
    positive: { bg: C.green + "22", text: C.green, border: C.green + "44" },
    negative: { bg: C.red + "22", text: C.red, border: C.red + "44" },
    neutral: { bg: C.gold + "22", text: C.gold, border: C.gold + "44" },
  };
  const c = colors[type];
  return (
    <span style={{
      display: "inline-block", padding: "2px 8px", borderRadius: 4,
      background: c.bg, color: c.text, border: `1px solid ${c.border}`,
      fontSize: 10, fontWeight: 700, letterSpacing: "0.5px",
    }}>
      {text}
    </span>
  );
}

// ============================================================
// CARD 1: LANDED COST SPREAD SHANGHAI
// ============================================================

function LCSCard({ data }: { data: any }) {
  if (!data || data.status !== "OK") return null;
  
  const isBR = data.competitive_origin === "BR";
  const spreadColor = isBR ? C.green : C.red;
  const maxBar = Math.max(data.us_landed, data.br_landed) * 1.05;

  return (
    <div style={{
      background: C.panel, border: `1px solid ${C.border}`, borderRadius: 8,
      padding: 16, flex: 1, minWidth: 280,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div>
          <div style={{ fontSize: 13, fontWeight: 700, color: C.text }}>Landed Cost Spread Shanghai</div>
          <div style={{ fontSize: 10, color: C.textMuted }}>US Gulf vs BR Santos → Shanghai</div>
        </div>
        <SignalBadge
          text={isBR ? "BR COMPETITIVE" : "US COMPETITIVE"}
          type={isBR ? "positive" : "negative"}
        />
      </div>

      {/* Spread headline */}
      <div style={{ textAlign: "center", margin: "8px 0 16px" }}>
        <span style={{ fontSize: 28, fontWeight: 700, color: spreadColor }}>
          ${data.spread_usd_mt > 0 ? "+" : ""}{data.spread_usd_mt.toFixed(2)}
        </span>
        <span style={{ fontSize: 13, color: C.textMuted, marginLeft: 4 }}>/mt</span>
        <div style={{ fontSize: 11, color: C.textMuted }}>
          {data.spread_pct > 0 ? "+" : ""}{data.spread_pct.toFixed(1)}%
        </div>
      </div>

      {/* Route bars */}
      <HBar label="🇺🇸 US Gulf → Shanghai" value={data.us_landed} maxValue={maxBar} color={C.gold} />
      <HBar label="🇧🇷 BR Santos → Shanghai" value={data.br_landed} maxValue={maxBar} color={C.green} />

      {/* Component breakdown */}
      <div style={{ display: "flex", gap: 16, marginTop: 12, fontSize: 10, color: C.textMuted }}>
        <div>
          <div>FOB Spread</div>
          <div style={{ color: data.fob_spread > 0 ? C.green : C.red, fontWeight: 600, fontSize: 12 }}>
            ${data.fob_spread > 0 ? "+" : ""}{data.fob_spread.toFixed(0)}
          </div>
        </div>
        <div>
          <div>Ocean Advantage</div>
          <div style={{ color: C.green, fontWeight: 600, fontSize: 12 }}>
            ${data.ocean_advantage > 0 ? "+" : ""}{data.ocean_advantage.toFixed(0)}
          </div>
        </div>
        <div>
          <div>CBOT</div>
          <div style={{ color: C.text, fontWeight: 600, fontSize: 12 }}>
            {data.cbot_cents_bu.toFixed(0)}¢/bu
          </div>
        </div>
        <div>
          <div>PTAX</div>
          <div style={{ color: C.text, fontWeight: 600, fontSize: 12 }}>
            R$ {data.ptax.toFixed(2)}
          </div>
        </div>
      </div>
    </div>
  );
}

// ============================================================
// CARD 2: EXPORT RACE TRACKER
// ============================================================

function ERTCard({ data }: { data: any }) {
  if (!data || data.status !== "OK") return null;

  const isBRLeader = data.leader === "BR";

  return (
    <div style={{
      background: C.panel, border: `1px solid ${C.border}`, borderRadius: 8,
      padding: 16, flex: 1, minWidth: 280,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div>
          <div style={{ fontSize: 13, fontWeight: 700, color: C.text }}>Export Race — Soybeans</div>
          <div style={{ fontSize: 10, color: C.textMuted }}>BR vs US | MY {data.marketing_year}</div>
        </div>
        <SignalBadge
          text={`${data.leader} LEADS +${data.lead_pace_pct.toFixed(1)}pp`}
          type={isBRLeader ? "positive" : "negative"}
        />
      </div>

      {/* Race bars */}
      <div style={{ margin: "12px 0" }}>
        {/* US bar */}
        <div style={{ marginBottom: 8 }}>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, marginBottom: 3 }}>
            <span style={{ color: C.textMuted }}>🇺🇸 US</span>
            <span style={{ color: C.text }}>{data.us_ytd_mmt.toFixed(1)} / {(data.us_pace_pct > 0 ? data.us_ytd_mmt / data.us_pace_pct * 100 : 50).toFixed(0)} MMT</span>
          </div>
          <div style={{ background: C.border, borderRadius: 4, height: 18, overflow: "hidden", position: "relative" }}>
            <div style={{ width: `${Math.min(data.us_pace_pct, 100)}%`, height: "100%", background: C.gold, borderRadius: 4 }} />
            <span style={{ position: "absolute", right: 6, top: 2, fontSize: 10, color: C.text, fontWeight: 600 }}>
              {data.us_pace_pct.toFixed(1)}%
            </span>
          </div>
          <div style={{ fontSize: 9, color: C.textMuted, marginTop: 1 }}>{data.us_pace_signal}</div>
        </div>
        
        {/* BR bar */}
        <div>
          <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, marginBottom: 3 }}>
            <span style={{ color: C.textMuted }}>🇧🇷 BR</span>
            <span style={{ color: C.text }}>{data.br_ytd_mmt.toFixed(1)} / {(data.br_pace_pct > 0 ? data.br_ytd_mmt / data.br_pace_pct * 100 : 105).toFixed(0)} MMT</span>
          </div>
          <div style={{ background: C.border, borderRadius: 4, height: 18, overflow: "hidden", position: "relative" }}>
            <div style={{ width: `${Math.min(data.br_pace_pct, 100)}%`, height: "100%", background: C.green, borderRadius: 4 }} />
            <span style={{ position: "absolute", right: 6, top: 2, fontSize: 10, color: C.text, fontWeight: 600 }}>
              {data.br_pace_pct.toFixed(1)}%
            </span>
          </div>
          <div style={{ fontSize: 9, color: C.textMuted, marginTop: 1 }}>{data.br_pace_signal}</div>
        </div>
      </div>

      {/* Market share */}
      <div style={{ marginTop: 12 }}>
        <div style={{ fontSize: 10, color: C.textMuted, marginBottom: 4 }}>Market Share (BR+US)</div>
        <div style={{ display: "flex", borderRadius: 4, overflow: "hidden", height: 20 }}>
          <div style={{
            width: `${data.us_market_share_pct}%`, background: C.gold, display: "flex",
            alignItems: "center", justifyContent: "center", fontSize: 10, color: "#000", fontWeight: 700,
          }}>
            {data.us_market_share_pct > 15 ? `US ${data.us_market_share_pct.toFixed(0)}%` : ""}
          </div>
          <div style={{
            width: `${data.br_market_share_pct}%`, background: C.green, display: "flex",
            alignItems: "center", justifyContent: "center", fontSize: 10, color: "#000", fontWeight: 700,
          }}>
            {data.br_market_share_pct > 15 ? `BR ${data.br_market_share_pct.toFixed(0)}%` : ""}
          </div>
        </div>
        <div style={{ fontSize: 10, color: C.textMuted, marginTop: 2 }}>
          vs 5yr avg: <span style={{ color: data.share_shift_pp > 0 ? C.green : C.red, fontWeight: 600 }}>
            {data.share_shift_pp > 0 ? "+" : ""}{data.share_shift_pp.toFixed(1)}pp
          </span>
        </div>
      </div>

      {/* China race */}
      {data.china_br_share_pct > 0 && (
        <div style={{ marginTop: 10, fontSize: 10, color: C.textMuted }}>
          China sources: 
          <span style={{ color: C.green, fontWeight: 600 }}> BR {data.china_br_share_pct.toFixed(0)}%</span> | 
          <span style={{ color: C.gold, fontWeight: 600 }}> US {data.china_us_share_pct.toFixed(0)}%</span>
        </div>
      )}
    </div>
  );
}

// ============================================================
// CARD 3: BRAZIL COMPETITIVENESS INDEX
// ============================================================

function BCICard({ data }: { data: any }) {
  if (!data || data.status !== "OK") return null;

  const signalType = data.bci_score >= 60 ? "positive" : data.bci_score >= 40 ? "neutral" : "negative";

  const gaugeZones = [
    { min: 0, max: 20, color: C.red, label: "Very Weak" },
    { min: 20, max: 40, color: "#E8734A", label: "Weak" },
    { min: 40, max: 60, color: C.gold, label: "Neutral" },
    { min: 60, max: 80, color: "#7DD87D", label: "Moderate" },
    { min: 80, max: 100, color: C.green, label: "Strong" },
  ];

  return (
    <div style={{
      background: C.panel, border: `1px solid ${C.border}`, borderRadius: 8,
      padding: 16, flex: 1, minWidth: 280,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
        <div>
          <div style={{ fontSize: 13, fontWeight: 700, color: C.text }}>Brazil Competitiveness Index</div>
          <div style={{ fontSize: 10, color: C.textMuted }}>Soybeans | Composite Score</div>
        </div>
        <SignalBadge text={data.bci_signal} type={signalType} />
      </div>

      {/* Gauge */}
      <div style={{ display: "flex", justifyContent: "center", margin: "4px 0" }}>
        <Gauge value={data.bci_score} zones={gaugeZones} size={140} />
      </div>

      {/* Trend */}
      {data.bci_trend && (
        <div style={{ textAlign: "center", fontSize: 10, color: C.textMuted, marginBottom: 8 }}>
          Trend: <span style={{ 
            color: data.bci_trend === "IMPROVING" ? C.green : data.bci_trend === "DETERIORATING" ? C.red : C.textMuted,
            fontWeight: 600 
          }}>
            {data.bci_trend === "IMPROVING" ? "▲" : data.bci_trend === "DETERIORATING" ? "▼" : "●"} {data.bci_trend}
          </span>
        </div>
      )}

      {/* Components */}
      <div style={{ marginTop: 8 }}>
        <div style={{ fontSize: 10, color: C.textMuted, marginBottom: 6, display: "flex", justifyContent: "space-between" }}>
          <span>Component</span>
          <span style={{ display: "flex", gap: 20 }}>
            <span style={{ width: 32, textAlign: "right" }}>Score</span>
            <span style={{ width: 24, textAlign: "right" }}>Wt</span>
          </span>
        </div>
        {(data.components || []).map((c: any, i: number) => (
          <ComponentBar key={i} name={c.name} score={c.score} weight={c.weight_pct} signal={c.signal} />
        ))}
      </div>

      {/* Strongest / Weakest */}
      <div style={{ display: "flex", gap: 16, marginTop: 10, fontSize: 10 }}>
        <div>
          <span style={{ color: C.textMuted }}>Strongest: </span>
          <span style={{ color: C.green, fontWeight: 600 }}>{data.strongest}</span>
        </div>
        <div>
          <span style={{ color: C.textMuted }}>Weakest: </span>
          <span style={{ color: C.red, fontWeight: 600 }}>{data.weakest}</span>
        </div>
      </div>
    </div>
  );
}

// ============================================================
// MAIN PANEL COMPONENT
// ============================================================

export default function BilateralPanel() {
  const [data, setData] = useState<BilateralData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    fetch("/data/processed/bilateral_indicators.json")
      .then((r) => {
        if (!r.ok) throw new Error("bilateral_indicators.json not found");
        return r.json();
      })
      .then((d) => { setData(d); setLoading(false); })
      .catch((e) => { setError(e.message); setLoading(false); });
  }, []);

  if (loading) {
    return (
      <div style={{ padding: 20, color: C.textMuted, fontSize: 12 }}>
        Loading bilateral indicators...
      </div>
    );
  }

  if (error || !data) {
    return (
      <div style={{ padding: 20, color: C.textMuted, fontSize: 12 }}>
        Bilateral indicators not available. Run <code>python generate_bilateral.py</code> first.
      </div>
    );
  }

  return (
    <div>
      {/* Header */}
      <div style={{
        display: "flex", justifyContent: "space-between", alignItems: "center",
        marginBottom: 16, paddingBottom: 8, borderBottom: `1px solid ${C.border}`,
      }}>
        <div>
          <h2 style={{ margin: 0, fontSize: 16, fontWeight: 700, color: C.text }}>
            Bilateral Intelligence
          </h2>
          <div style={{ fontSize: 10, color: C.textMuted }}>
            Proprietary indicators — US vs Brazil competitiveness
          </div>
        </div>
        <div style={{ fontSize: 10, color: C.textMuted }}>
          {data.date} • {new Date(data.generated_at).toLocaleTimeString()}
        </div>
      </div>

      {/* Summary strip */}
      <div style={{
        display: "flex", gap: 16, marginBottom: 16, padding: "10px 16px",
        background: C.bg, borderRadius: 6, border: `1px solid ${C.border}`,
        justifyContent: "space-around",
      }}>
        {data.summary.lcs_spread !== null && (
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: 9, color: C.textMuted, textTransform: "uppercase", letterSpacing: 1 }}>LCS Shanghai</div>
            <div style={{ fontSize: 18, fontWeight: 700, color: data.summary.lcs_spread > 0 ? C.green : C.red }}>
              ${data.summary.lcs_spread > 0 ? "+" : ""}{data.summary.lcs_spread.toFixed(0)}
            </div>
            <div style={{ fontSize: 9, color: C.textMuted }}>{data.summary.lcs_origin} competitive</div>
          </div>
        )}
        {data.summary.ert_leader !== null && (
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: 9, color: C.textMuted, textTransform: "uppercase", letterSpacing: 1 }}>Export Race</div>
            <div style={{ fontSize: 18, fontWeight: 700, color: data.summary.ert_leader === "BR" ? C.green : C.gold }}>
              {data.summary.ert_leader}
            </div>
            <div style={{ fontSize: 9, color: C.textMuted }}>BR {data.summary.ert_br_share?.toFixed(0)}% share</div>
          </div>
        )}
        {data.summary.bci_score !== null && (
          <div style={{ textAlign: "center" }}>
            <div style={{ fontSize: 9, color: C.textMuted, textTransform: "uppercase", letterSpacing: 1 }}>BCI Score</div>
            <div style={{ fontSize: 18, fontWeight: 700, color: 
              data.summary.bci_score >= 60 ? C.green : data.summary.bci_score >= 40 ? C.gold : C.red
            }}>
              {data.summary.bci_score.toFixed(0)}
            </div>
            <div style={{ fontSize: 9, color: C.textMuted }}>{data.summary.bci_signal}</div>
          </div>
        )}
      </div>

      {/* Cards grid */}
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
        <LCSCard data={data.lcs} />
        <ERTCard data={data.ert} />
        <BCICard data={data.bci} />
      </div>
    </div>
  );
}
