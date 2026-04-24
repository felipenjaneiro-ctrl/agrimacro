"use client";
import { useEffect, useRef, useState } from "react";
import { createChart, LineSeries, ColorType, type IChartApi, type Time } from "lightweight-charts";

interface GreeksTabProps {
  sym: string;
  C: {
    bg: string;
    panel: string;
    border: string;
    text: string;
    textDim: string;
    textMuted: string;
    green: string;
    red: string;
    gold: string;
    [k: string]: string;
  };
}

interface IvAnalyticsEntry {
  name: string;
  atm_iv: number;
  iv_rank_252d: number | null;
  iv_rank_days_available: number;
  skew_pp: number | null;
  skew_direction: "balanced" | "put_skewed" | "call_skewed" | null;
  skew_extreme: boolean;
  skew_type: string;
  skew_source: string;
  front_expiry: string;
  front_dte: number;
  atm_strike: number;
  spot: number | null;
  call_atm_iv: number;
  put_atm_iv: number;
  put_25d_iv: number | null;
  call_25d_iv: number | null;
}

interface IvHistoryEntry {
  date: string;
  atm_iv: number;
  skew_pp: number | null;
  spot: number | null;
}

type RankDisplay = {
  label: string;
  color: string;
  value: string | null;
  pct: number;
};

function ivRankDisplay(rank: number | null, daysAvailable: number, muted: string): RankDisplay {
  if (rank === null) {
    return {
      label: `Histórico insuficiente (${daysAvailable}/252)`,
      color: muted,
      value: null,
      pct: 0,
    };
  }
  let label: string;
  let color: string;
  if (rank < 25) {
    label = "LOW";
    color = "#DC3C3C";
  } else if (rank < 50) {
    label = "MEDIUM";
    color = "#DCB432";
  } else if (rank < 75) {
    label = "HIGH";
    color = "#00C878";
  } else {
    label = "EXTREME";
    color = "#DCB432";
  }
  return { label, color, value: rank.toFixed(1), pct: rank };
}

type SkewDisplay = {
  text: string;
  label: string;
  color: string;
  badge: string | null;
};

function skewDisplay(
  skew_pp: number | null,
  direction: string | null,
  extreme: boolean,
  muted: string
): SkewDisplay {
  if (skew_pp === null) {
    return { text: "n/a", label: "indisponível", color: muted, badge: null };
  }
  const text = (skew_pp > 0 ? "+" : "") + skew_pp.toFixed(2) + " pp";
  const color = direction === "balanced" ? muted : "#DCB432";
  const label = direction ?? "—";
  return { text, label, color, badge: extreme ? "extreme" : null };
}

export default function GreeksTab({ sym, C }: GreeksTabProps) {
  const [data, setData] = useState<IvAnalyticsEntry | null>(null);
  const [history, setHistory] = useState<IvHistoryEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [reloadKey, setReloadKey] = useState(0);

  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartApiRef = useRef<IChartApi | null>(null);

  // Fetch
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetch(`/api/iv-analytics?commodity=${encodeURIComponent(sym)}&history=true`)
      .then((r) => r.json())
      .then((body) => {
        if (cancelled) return;
        if (body.error) {
          setError(`Sem dados de volatilidade para ${sym}`);
          setData(null);
          setHistory([]);
        } else {
          setData(body.data ?? null);
          setHistory(Array.isArray(body.history) ? body.history : []);
        }
      })
      .catch(() => {
        if (!cancelled) setError("Não foi possível carregar dados de volatilidade.");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [sym, reloadKey]);

  // Mini-chart (só renderiza quando há >=10 pontos)
  useEffect(() => {
    if (history.length < 10 || !chartContainerRef.current) return;

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: C.bg },
        textColor: C.textDim,
        fontFamily: "monospace",
      },
      grid: {
        vertLines: { visible: false },
        horzLines: { color: "#1a2832" },
      },
      width: chartContainerRef.current.clientWidth,
      height: 160,
      timeScale: { timeVisible: false, borderVisible: false },
      rightPriceScale: { borderVisible: false },
      crosshair: { mode: 1 },
    });

    const series = chart.addSeries(LineSeries, {
      color: "#00C878",
      lineWidth: 2,
    });

    const lastN = history.slice(-90);
    const chartData = lastN.map((h) => ({
      time: h.date as unknown as Time,
      value: h.atm_iv * 100,
    }));

    series.setData(chartData);
    chart.timeScale().fitContent();
    chartApiRef.current = chart;

    const handleResize = () => {
      if (chartContainerRef.current && chartApiRef.current) {
        chartApiRef.current.applyOptions({ width: chartContainerRef.current.clientWidth });
      }
    };
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
      chartApiRef.current = null;
    };
  }, [history, C.bg, C.textDim]);

  if (loading) {
    return <div style={{ padding: 12, color: C.textMuted }}>Carregando volatilidade...</div>;
  }

  if (error) {
    return (
      <div style={{ padding: 12, color: C.red }}>
        {error}
        <button
          onClick={() => setReloadKey((k) => k + 1)}
          style={{
            marginLeft: 12,
            padding: "4px 10px",
            background: C.panel,
            color: C.text,
            border: `1px solid ${C.border}`,
            borderRadius: 3,
            cursor: "pointer",
            fontSize: 11,
          }}
        >
          Tentar novamente
        </button>
      </div>
    );
  }

  if (!data) {
    return <div style={{ padding: 12, color: C.textMuted }}>Sem dados para {sym}</div>;
  }

  const ivRank = ivRankDisplay(data.iv_rank_252d, data.iv_rank_days_available, C.textMuted);
  const skewInfo = skewDisplay(data.skew_pp, data.skew_direction, data.skew_extreme, C.textMuted);
  const atmIvPct = (data.atm_iv * 100).toFixed(2);
  const spotStr = data.spot !== null ? data.spot.toFixed(2) : "—";

  const cardStyle: React.CSSProperties = {
    flex: "1 1 200px",
    background: C.panel,
    border: `1px solid ${C.border}`,
    borderRadius: 6,
    padding: 12,
    minWidth: 180,
  };

  return (
    <div style={{ padding: 12, fontFamily: "monospace" }}>
      <div style={{ fontSize: 13, color: C.textMuted, marginBottom: 12, letterSpacing: 0.5 }}>
        GREEKS &amp; VOLATILIDADE — {sym} {data.name ? <span style={{ color: C.textDim }}>({data.name})</span> : null}
      </div>

      {/* 3 cards */}
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 12 }}>
        {/* Card IV Rank */}
        <div style={cardStyle}>
          <div style={{ fontSize: 10, color: C.textMuted, marginBottom: 6, letterSpacing: 0.5 }}>
            IV RANK 252d
          </div>
          <div style={{ fontSize: 28, fontWeight: 600, color: ivRank.color, lineHeight: 1 }}>
            {ivRank.value ?? <span style={{ fontSize: 14 }}>—</span>}
          </div>
          {ivRank.value !== null && (
            <div
              style={{
                marginTop: 10,
                height: 6,
                background: C.bg,
                borderRadius: 3,
                overflow: "hidden",
              }}
            >
              <div
                style={{
                  width: `${ivRank.pct}%`,
                  height: "100%",
                  background: ivRank.color,
                  borderRadius: 3,
                }}
              />
            </div>
          )}
          <div style={{ fontSize: 10, color: ivRank.color, marginTop: 8, fontWeight: 500 }}>
            {ivRank.label}
          </div>
        </div>

        {/* Card IV ATM */}
        <div style={cardStyle}>
          <div style={{ fontSize: 10, color: C.textMuted, marginBottom: 6, letterSpacing: 0.5 }}>
            IV ATM (front)
          </div>
          <div style={{ fontSize: 28, fontWeight: 600, color: C.text, lineHeight: 1 }}>
            {atmIvPct}%
          </div>
          <div style={{ fontSize: 10, color: C.textMuted, marginTop: 10 }}>
            call: {(data.call_atm_iv * 100).toFixed(2)}% | put: {(data.put_atm_iv * 100).toFixed(2)}%
          </div>
        </div>

        {/* Card Skew */}
        <div style={cardStyle}>
          <div style={{ fontSize: 10, color: C.textMuted, marginBottom: 6, letterSpacing: 0.5 }}>
            SKEW 25-DELTA
            <span
              title="Skew 25-delta = IV(put 25d) - IV(call 25d). Positivo = mercado paga mais por proteção downside (típico em energia com geopolítica, grãos em weather premium). Negativo = mercado paga mais por upside (squeeze potencial)."
              style={{ marginLeft: 6, cursor: "help", color: C.textMuted }}
            >
              ⓘ
            </span>
          </div>
          <div style={{ fontSize: 28, fontWeight: 600, color: skewInfo.color, lineHeight: 1 }}>
            {skewInfo.text}
          </div>
          <div style={{ fontSize: 10, color: skewInfo.color, marginTop: 10, fontWeight: 500 }}>
            {skewInfo.label}
            {skewInfo.badge && (
              <span
                style={{
                  marginLeft: 8,
                  padding: "1px 6px",
                  background: C.red,
                  color: "#fff",
                  borderRadius: 3,
                  fontSize: 9,
                  fontWeight: 600,
                  letterSpacing: 0.5,
                }}
              >
                {skewInfo.badge.toUpperCase()}
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Metadata */}
      <div style={{ fontSize: 11, color: C.textMuted, marginBottom: 16 }}>
        Front expiry: {data.front_expiry} ({data.front_dte} DTE) | ATM strike: $
        {data.atm_strike} | Spot: ${spotStr}
      </div>

      {/* Mini-chart */}
      <div>
        <div style={{ fontSize: 11, color: C.textMuted, marginBottom: 6 }}>
          Histórico IV ATM — últimos 90 dias
        </div>
        {history.length < 10 ? (
          <div
            style={{
              height: 160,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              color: C.textMuted,
              fontSize: 12,
              background: C.panel,
              borderRadius: 6,
              border: `1px solid ${C.border}`,
            }}
          >
            Coletando histórico... ({history.length}/10 dias mínimos)
          </div>
        ) : (
          <div
            ref={chartContainerRef}
            style={{
              height: 160,
              background: C.bg,
              borderRadius: 6,
              border: `1px solid ${C.border}`,
            }}
          />
        )}
      </div>
    </div>
  );
}
