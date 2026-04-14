"use client";
import { useEffect, useRef } from "react";
import {
  createChart,
  CandlestickSeries,
  LineSeries,
  HistogramSeries,
  ColorType,
  CrosshairMode,
  LineStyle,
} from "lightweight-charts";
import type { IChartApi, ISeriesApi, SeriesType, Time } from "lightweight-charts";

interface Bar {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

interface Props {
  bars: Bar[];
  symbol: string;
  height?: number;
  showRSI?: boolean;
  cotIndex?: number;
  basis?: number;
}

// ── Moving Average ──
function calcMA(data: { time: Time; close: number }[], period: number) {
  if (data.length < period) return [];
  return data.slice(period - 1).map((_, i) => ({
    time: data[i + period - 1].time,
    value:
      data.slice(i, i + period).reduce((s, d) => s + d.close, 0) / period,
  }));
}

// ── RSI ──
function calcRSI(bars: Bar[], period = 14) {
  if (bars.length < period + 1) return [];
  const closes = bars.map((b) => b.close);
  const result: { time: Time; value: number }[] = [];
  let gains = 0,
    losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff >= 0) gains += diff;
    else losses -= diff;
  }
  let avgGain = gains / period;
  let avgLoss = losses / period;
  for (let i = period; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    avgGain = (avgGain * (period - 1) + Math.max(diff, 0)) / period;
    avgLoss = (avgLoss * (period - 1) + Math.max(-diff, 0)) / period;
    const rs = avgLoss === 0 ? 100 : avgGain / avgLoss;
    result.push({
      time: bars[i].date as unknown as Time,
      value: 100 - 100 / (1 + rs),
    });
  }
  return result;
}

export default function LightweightChart({
  bars,
  symbol,
  height = 400,
  showRSI = true,
  cotIndex,
  basis,
}: Props) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const rsiContainerRef = useRef<HTMLDivElement>(null);
  const chartApiRef = useRef<IChartApi | null>(null);
  const rsiApiRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!chartContainerRef.current || !bars?.length) return;

    // ── Main chart ──
    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height,
      layout: {
        background: { type: ColorType.Solid, color: "#0E1A24" },
        textColor: "#94a3b8",
        fontFamily: "monospace",
      },
      grid: {
        vertLines: { color: "#1e3a4a" },
        horzLines: { color: "#1e3a4a" },
      },
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderColor: "#1e3a4a" },
      timeScale: { borderColor: "#1e3a4a", timeVisible: false },
    });
    chartApiRef.current = chart;

    // Prepare data sorted by date
    const candleData = bars
      .map((b) => ({
        time: b.date as unknown as Time,
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
      }))
      .sort((a, b) => (a.time as string).localeCompare(b.time as string));

    // Candlestick
    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#00C878",
      downColor: "#DC3C3C",
      borderUpColor: "#00C878",
      borderDownColor: "#DC3C3C",
      wickUpColor: "#00C878",
      wickDownColor: "#DC3C3C",
    });
    candleSeries.setData(candleData);

    // Volume
    const volData = bars
      .filter((b) => b.volume && b.volume > 0)
      .map((b) => ({
        time: b.date as unknown as Time,
        value: b.volume!,
        color: b.close >= b.open ? "#00C87840" : "#DC3C3C40",
      }))
      .sort((a, b) => (a.time as string).localeCompare(b.time as string));

    if (volData.length > 0) {
      const volSeries = chart.addSeries(HistogramSeries, {
        color: "#00C87840",
        priceFormat: { type: "volume" },
        priceScaleId: "vol",
      });
      chart.priceScale("vol").applyOptions({
        scaleMargins: { top: 0.8, bottom: 0 },
      });
      volSeries.setData(volData);
    }

    // MA21
    const ma21 = calcMA(candleData, 21);
    if (ma21.length > 0) {
      const s = chart.addSeries(LineSeries, {
        color: "#00C878",
        lineWidth: 1,
        title: "MA21",
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceLineVisible: false,
      });
      s.setData(ma21);
    }

    // MA50
    const ma50 = calcMA(candleData, 50);
    if (ma50.length > 0) {
      const s = chart.addSeries(LineSeries, {
        color: "#468CDC",
        lineWidth: 1,
        title: "MA50",
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceLineVisible: false,
      });
      s.setData(ma50);
    }

    // MA200
    const ma200 = calcMA(candleData, 200);
    if (ma200.length > 0) {
      const s = chart.addSeries(LineSeries, {
        color: "#DCB432",
        lineWidth: 2,
        title: "MA200",
        crosshairMarkerVisible: false,
        lastValueVisible: false,
        priceLineVisible: false,
      });
      s.setData(ma200);
    }

    chart.timeScale().fitContent();

    // ── RSI chart ──
    let rsiChart: IChartApi | null = null;
    if (showRSI && rsiContainerRef.current) {
      rsiChart = createChart(rsiContainerRef.current, {
        width: rsiContainerRef.current.clientWidth,
        height: 100,
        layout: {
          background: { type: ColorType.Solid, color: "#0a1520" },
          textColor: "#94a3b8",
          fontFamily: "monospace",
        },
        grid: {
          vertLines: { color: "#1e3a4a" },
          horzLines: { color: "#1e3a4a" },
        },
        crosshair: { mode: CrosshairMode.Normal },
        rightPriceScale: { borderColor: "#1e3a4a" },
        timeScale: { borderColor: "#1e3a4a", visible: false },
      });
      rsiApiRef.current = rsiChart;

      const rsiData = calcRSI(bars);
      if (rsiData.length > 0) {
        const rsiSeries = rsiChart.addSeries(LineSeries, {
          color: "#a855f7",
          lineWidth: 1,
          title: "RSI(14)",
          priceLineVisible: false,
        });
        rsiSeries.setData(rsiData);

        rsiSeries.createPriceLine({
          price: 70,
          color: "#DC3C3C40",
          lineWidth: 1,
          lineStyle: LineStyle.Dashed,
          axisLabelVisible: true,
          title: "Overbought",
        });
        rsiSeries.createPriceLine({
          price: 30,
          color: "#00C87840",
          lineWidth: 1,
          lineStyle: LineStyle.Dashed,
          axisLabelVisible: true,
          title: "Oversold",
        });
        rsiSeries.createPriceLine({
          price: 50,
          color: "#94a3b822",
          lineWidth: 1,
          lineStyle: LineStyle.Dotted,
          axisLabelVisible: false,
          title: "",
        });
      }

      rsiChart.timeScale().fitContent();

      // Sync time scales
      chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
        if (range) rsiChart?.timeScale().setVisibleLogicalRange(range);
      });
      rsiChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
        if (range) chart.timeScale().setVisibleLogicalRange(range);
      });
    }

    // Resize observer
    const obs = new ResizeObserver(() => {
      if (chartContainerRef.current) {
        chart.applyOptions({ width: chartContainerRef.current.clientWidth });
      }
      if (rsiContainerRef.current && rsiChart) {
        rsiChart.applyOptions({ width: rsiContainerRef.current.clientWidth });
      }
    });
    obs.observe(chartContainerRef.current);

    return () => {
      obs.disconnect();
      chart.remove();
      rsiChart?.remove();
      chartApiRef.current = null;
      rsiApiRef.current = null;
    };
  }, [bars, symbol, height, showRSI]);

  return (
    <div
      style={{
        background: "#0E1A24",
        border: "1px solid #1e3a4a",
        borderRadius: 8,
        overflow: "hidden",
      }}
    >
      {/* Header */}
      <div
        style={{
          padding: "8px 12px",
          display: "flex",
          alignItems: "center",
          gap: 12,
          borderBottom: "1px solid #1e3a4a",
        }}
      >
        <span
          style={{
            fontSize: 13,
            fontWeight: 700,
            color: "#e2e8f0",
            fontFamily: "monospace",
          }}
        >
          {symbol}
        </span>
        {cotIndex !== undefined && cotIndex !== null && (
          <span
            style={{
              fontSize: 9,
              fontWeight: 700,
              color:
                cotIndex >= 80
                  ? "#DC3C3C"
                  : cotIndex <= 20
                    ? "#00C878"
                    : "#64748b",
              background:
                (cotIndex >= 80
                  ? "#DC3C3C"
                  : cotIndex <= 20
                    ? "#00C878"
                    : "#64748b") + "22",
              padding: "2px 6px",
              borderRadius: 4,
            }}
          >
            COT {cotIndex}/100
          </span>
        )}
        {basis !== undefined && basis !== null && (
          <span
            style={{
              fontSize: 9,
              color: basis > 0 ? "#DC3C3C" : "#00C878",
            }}
          >
            Basis BR: {basis > 0 ? "+" : ""}
            {basis.toFixed(2)} USD/bu
          </span>
        )}
        <span style={{ fontSize: 8, color: "#475569", marginLeft: "auto" }}>
          MA21{" "}
          <span style={{ color: "#00C878" }}>{"\u2500\u2500"}</span> MA50{" "}
          <span style={{ color: "#468CDC" }}>{"\u2500\u2500"}</span> MA200{" "}
          <span style={{ color: "#DCB432" }}>{"\u2500\u2500"}</span>
        </span>
      </div>
      {/* Main chart */}
      <div ref={chartContainerRef} style={{ width: "100%" }} />
      {/* RSI */}
      {showRSI && (
        <div
          ref={rsiContainerRef}
          style={{ width: "100%", borderTop: "1px solid #1e3a4a" }}
        />
      )}
    </div>
  );
}
