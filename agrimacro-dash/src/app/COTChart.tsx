"use client";
import { useEffect, useRef, useState } from "react";

interface LegacyEntry {
  date: string;
  noncomm_net: number;
  comm_net: number;
  open_interest?: number;
}

interface DisaggEntry {
  date: string;
  managed_money_net: number;
  producer_net: number;
  swap_net: number;
  open_interest?: number;
}

interface Props {
  symbol: string;
  legacyHistory: LegacyEntry[];
  disaggHistory: DisaggEntry[];
}

const C = {
  bg: "#0a1520",
  panel: "#142332",
  border: "#1e3a4a",
  zero: "#334155",
  text: "#94a3b8",
  textBright: "#e2e8f0",
  green: "#00C878",
  red: "#DC3C3C",
  purple: "#a855f7",
  blue: "#468CDC",
  gold: "#DCB432",
};

function Legend({
  series,
}: {
  series: { color: string; label: string }[];
}) {
  return (
    <div
      style={{
        display: "flex",
        gap: 16,
        padding: "6px 12px",
        background: C.panel,
        borderBottom: `1px solid ${C.border}`,
        flexWrap: "wrap",
      }}
    >
      {series.map((s) => (
        <div
          key={s.label}
          style={{ display: "flex", alignItems: "center", gap: 5 }}
        >
          <div
            style={{
              width: 24,
              height: 2,
              background: s.color,
              borderRadius: 1,
            }}
          />
          <span style={{ fontSize: 10, color: C.text }}>{s.label}</span>
        </div>
      ))}
    </div>
  );
}

function Panel({
  entries,
  series,
  panelHeight,
}: {
  entries: any[];
  series: { key: string; color: string; label: string }[];
  panelHeight: number;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(600);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const obs = new ResizeObserver((ents) => {
      const w = ents[0].contentRect.width;
      if (w > 0) setWidth(Math.floor(w));
    });
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  if (!entries?.length) return null;

  const padL = 8,
    padR = 80,
    padT = 12,
    padB = 24;
  const W = width - padL - padR;
  const H = panelHeight - padT - padB;
  const n = entries.length;

  let minVal = Infinity,
    maxVal = -Infinity;
  entries.forEach((e) => {
    series.forEach((s) => {
      const v = e[s.key];
      if (typeof v === "number") {
        if (v < minVal) minVal = v;
        if (v > maxVal) maxVal = v;
      }
    });
  });
  const range = maxVal - minVal || 1;
  minVal -= range * 0.05;
  maxVal += range * 0.05;

  const toX = (i: number) => padL + (i / (n - 1)) * W;
  const toY = (v: number) =>
    padT + H - ((v - minVal) / (maxVal - minVal)) * H;
  const zeroY = toY(0);

  const xLabels: { x: number; label: string }[] = [];
  entries.forEach((e, i) => {
    if (i === 0 || i === n - 1 || i % 26 === 0) {
      const d = new Date(e.date);
      const label = d.toLocaleDateString("en-US", {
        month: "short",
        year: "2-digit",
      });
      xLabels.push({ x: toX(i), label });
    }
  });

  return (
    <div ref={containerRef} style={{ width: "100%" }}>
      <svg
        width="100%"
        height={panelHeight}
        viewBox={`0 0 ${width} ${panelHeight}`}
        style={{ display: "block", background: C.bg }}
      >
        {/* Grid lines */}
        {[0.25, 0.5, 0.75].map((frac) => {
          const y = padT + H * frac;
          return (
            <line
              key={frac}
              x1={padL}
              y1={y}
              x2={padL + W}
              y2={y}
              stroke="#1e3a4a"
              strokeWidth={0.5}
            />
          );
        })}

        {/* Zero line */}
        {zeroY >= padT && zeroY <= padT + H && (
          <line
            x1={padL}
            y1={zeroY}
            x2={padL + W}
            y2={zeroY}
            stroke={C.zero}
            strokeWidth={1}
            strokeDasharray="4,4"
          />
        )}

        {/* Series */}
        {series.map((s) => {
          const pts = entries
            .map((e, i) => {
              const v = e[s.key];
              if (typeof v !== "number") return null;
              return `${toX(i).toFixed(1)},${toY(v).toFixed(1)}`;
            })
            .filter(Boolean) as string[];
          if (!pts.length) return null;

          const areaPath = `M${toX(0).toFixed(1)},${zeroY.toFixed(1)} L${pts.join(" L")} L${toX(n - 1).toFixed(1)},${zeroY.toFixed(1)} Z`;

          return (
            <g key={s.key}>
              <path d={areaPath} fill={s.color} fillOpacity={0.06} />
              <polyline
                points={pts.join(" ")}
                fill="none"
                stroke={s.color}
                strokeWidth={1.5}
                strokeLinejoin="round"
              />
            </g>
          );
        })}

        {/* Current values (right side) */}
        {series.map((s, si) => {
          const last = entries[entries.length - 1];
          const v = last?.[s.key];
          if (typeof v !== "number") return null;
          const formatted =
            v >= 0
              ? `+${(v / 1000).toFixed(0)}K`
              : `${(v / 1000).toFixed(0)}K`;
          return (
            <g key={s.key + "_label"}>
              <rect
                x={padL + W + 4}
                y={padT + si * 22}
                width={72}
                height={18}
                rx={3}
                fill={s.color + "22"}
              />
              <line
                x1={padL + W + 4}
                y1={padT + si * 22 + 9}
                x2={padL + W + 8}
                y2={padT + si * 22 + 9}
                stroke={s.color}
                strokeWidth={2}
              />
              <text
                x={padL + W + 12}
                y={padT + si * 22 + 13}
                fontSize={10}
                fill={s.color}
                fontFamily="monospace"
                fontWeight={700}
              >
                {formatted}
              </text>
              <text
                x={padL + W + 56}
                y={padT + si * 22 + 13}
                fontSize={7}
                fill={C.text}
              >
                {s.label.split(" ")[0]}
              </text>
            </g>
          );
        })}

        {/* Dots at last point */}
        {series.map((s) => {
          const last = entries[entries.length - 1];
          const v = last?.[s.key];
          if (typeof v !== "number") return null;
          return (
            <circle
              key={s.key + "_dot"}
              cx={toX(n - 1)}
              cy={toY(v)}
              r={3}
              fill={s.color}
            />
          );
        })}

        {/* X axis dates */}
        {xLabels.map(({ x, label }, i) => (
          <text
            key={i}
            x={x}
            y={padT + H + 16}
            fontSize={9}
            fill={C.text}
            textAnchor="middle"
            fontFamily="monospace"
          >
            {label}
          </text>
        ))}

        {/* Y axis label */}
        {zeroY >= padT && zeroY <= padT + H && (
          <text
            x={padL + 2}
            y={zeroY - 3}
            fontSize={7}
            fill={C.zero}
            fontFamily="monospace"
          >
            0
          </text>
        )}
      </svg>
    </div>
  );
}

const legacySeries = [
  {
    key: "noncomm_net",
    color: C.green,
    label: "Non-Commercial (Large Specs)",
  },
  { key: "comm_net", color: C.red, label: "Commercial (Hedgers)" },
];

const disaggSeries = [
  { key: "managed_money_net", color: C.purple, label: "Managed Money" },
  { key: "swap_net", color: C.blue, label: "Swap Dealers" },
  { key: "producer_net", color: C.red, label: "Producer/Merchant" },
];

export default function COTChart({
  symbol,
  legacyHistory,
  disaggHistory,
}: Props) {
  return (
    <div
      style={{
        border: `1px solid ${C.border}`,
        borderRadius: 8,
        overflow: "hidden",
        background: C.bg,
      }}
    >
      {/* Panel 1 — Legacy */}
      <div
        style={{
          padding: "6px 12px",
          background: C.panel,
          borderBottom: `1px solid ${C.border}`,
          fontSize: 10,
          color: C.text,
          fontWeight: 700,
          letterSpacing: "0.05em",
        }}
      >
        LEGACY COT — {symbol}
      </div>
      <Legend series={legacySeries} />
      <Panel
        entries={legacyHistory}
        series={legacySeries}
        panelHeight={180}
      />

      {/* Panel 2 — Disaggregated */}
      <div
        style={{
          padding: "6px 12px",
          background: C.panel,
          borderTop: `1px solid ${C.border}`,
          borderBottom: `1px solid ${C.border}`,
          fontSize: 10,
          color: C.text,
          fontWeight: 700,
          letterSpacing: "0.05em",
        }}
      >
        DISAGGREGATED COT — {symbol}
      </div>
      <Legend series={disaggSeries} />
      <Panel
        entries={disaggHistory}
        series={disaggSeries}
        panelHeight={180}
      />

      {/* Footer explanation */}
      <div
        style={{
          padding: "8px 12px",
          background: C.panel,
          borderTop: `1px solid ${C.border}`,
          fontSize: 9,
          color: "#475569",
          lineHeight: 1.6,
        }}
      >
        <strong style={{ color: C.textBright }}>Legacy:</strong> Commercials
        (hedgers) vs Non-Commercials (especuladores).{" "}
        <strong style={{ color: C.textBright }}>Disaggregated:</strong> Managed
        Money = fundos especulativos. Linha acima do zero = net long (comprado).
        Abaixo = net short (vendido).
      </div>
    </div>
  );
}
