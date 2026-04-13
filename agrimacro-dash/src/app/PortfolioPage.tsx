"use client";
import { useState, useMemo } from "react";

/* ── palette ── */
const C = {
  bg: "#0E1A24",
  panel: "#142332",
  panel2: "#0a1520",
  border: "#1e3a4a",
  green: "#00C878",
  red: "#DC3C3C",
  gold: "#DCB432",
  blue: "#468CDC",
  text: "#e2e8f0",
  muted: "#94a3b8",
  dim: "#64748b",
};

/* ── helpers ── */
const STRIKE_DIVISORS: Record<string, number> = { CL: 100, SI: 100, GF: 10, ZL: 100, CC: 1, ZC: 100, ZS: 100, ZW: 100, ZM: 100, SB: 100, KC: 100, CT: 100, LE: 100, HE: 100, NG: 100, GC: 100 };
const OPT_MULTIPLIERS: Record<string, number> = { CL: 1000, SI: 5000, GF: 500, ZL: 600, CC: 10, ZC: 50, ZS: 50, ZW: 50, ZM: 100, SB: 1120, KC: 375, CT: 500, LE: 400, HE: 400, NG: 10000, GC: 100 };
const MONTH_MAP: Record<string, string> = { F: "Jan", G: "Feb", H: "Mar", J: "Apr", K: "May", M: "Jun", N: "Jul", Q: "Aug", U: "Sep", V: "Oct", X: "Nov", Z: "Dec" };

function fmtUsd(v: number, decimals = 0) {
  const s = Math.abs(v).toLocaleString("en-US", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
  return (v < 0 ? "-$" : "$") + s;
}
function fmtNum(v: number | null | undefined, d = 2) {
  if (v == null || isNaN(v as number)) return "-";
  return (v as number).toFixed(d);
}
function plColor(v: number) { return v >= 0 ? C.green : C.red; }

function parsePos(pos: any) {
  const ls = (pos.local_symbol || "").trim();
  const parts = ls.split(" ");
  const isOption = pos.sec_type === "FOP" || pos.sec_type === "OPT" || (parts.length >= 2 && /^[CP]\d+$/.test(parts[parts.length - 1]));
  let strike = 0, optType = "", expLabel = "";
  if (isOption && parts.length >= 2) {
    const rp = parts[parts.length - 1];
    optType = rp[0];
    strike = parseInt(rp.slice(1)) / (STRIKE_DIVISORS[pos.symbol] || 100);
    const expCode = parts[0].slice(-2);
    expLabel = (MONTH_MAP[expCode[0]] || expCode[0]) + "/2" + expCode[1];
  }
  const qty = pos.position || 0;
  return {
    symbol: pos.symbol, localSymbol: ls, secType: pos.sec_type || "",
    isOption, optType, strike, expLabel, qty,
    avgCost: pos.avg_cost || 0, mktValue: pos.market_value || 0,
    delta: pos.delta, gamma: pos.gamma, theta: pos.theta, vega: pos.vega,
    iv: pos.iv, undPrice: pos.und_price, optPrice: pos.opt_price,
  };
}

/* ── gaussian helper ── */
function gaussianY(x: number, mean: number, std: number) {
  return Math.exp(-0.5 * ((x - mean) / std) ** 2) / (std * Math.sqrt(2 * Math.PI));
}

/* ── types ── */
interface PortfolioPageProps {
  portfolio: any;
  greeks: any;
  optionsChain: any;
  prices?: any;
}

/* ══════════════════════════════════════════════ */
export default function PortfolioPage({ portfolio, greeks, optionsChain, prices }: PortfolioPageProps) {
  const [chainUnd, setChainUnd] = useState("CL");
  const [chainExp, setChainExp] = useState("");
  const [payoffUnd, setPayoffUnd] = useState("CL");
  const [payoffExpiry, setPayoffExpiry] = useState("all");
  const [builderLegs, setBuilderLegs] = useState<{ strike: number; right: string; qty: number }[]>([]);

  /* ── parse portfolio ── */
  const summ = portfolio?.summary || {};
  const netLiq = parseFloat(summ.NetLiquidation || "0");
  const buyPow = parseFloat(summ.BuyingPower || "0");
  const excessLiq = buyPow / 4; // excess = buyingPower/4 approx
  const pGreeks = portfolio?.portfolio_greeks || greeks?.portfolio_greeks || {};
  const totalDelta = pGreeks.total_delta ?? 0;
  const totalTheta = pGreeks.total_theta ?? 0;
  const totalVega = pGreeks.total_vega ?? 0;

  const rawPositions: any[] = portfolio?.positions || [];
  const parsed = useMemo(() => rawPositions.map(parsePos), [rawPositions]);

  const byUnd = useMemo(() => {
    const m: Record<string, ReturnType<typeof parsePos>[]> = {};
    parsed.forEach(p => { if (!m[p.symbol]) m[p.symbol] = []; m[p.symbol].push(p); });
    return m;
  }, [parsed]);
  const symbols = Object.keys(byUnd).sort();
  const optSymbols = symbols.filter(s => byUnd[s].some(p => p.isOption));

  /* ── options chain data ── */
  const chainUnderlyings = Object.keys(optionsChain?.underlyings || {});
  const activeChainUnd = chainUnderlyings.includes(chainUnd) ? chainUnd : chainUnderlyings[0] || "";
  const chainData = optionsChain?.underlyings?.[activeChainUnd] || {};
  const chainExps = Object.keys(chainData.expirations || {}).sort();
  const activeChainExp = chainExps.includes(chainExp) ? chainExp : chainExps[0] || "";
  const expData = chainData.expirations?.[activeChainExp] || { calls: [], puts: [] };

  /* ══════════════════════════════════════════════
     SECTION 1 — HEADER KPIs
     ══════════════════════════════════════════════ */
  const renderKPIs = () => (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 12, marginBottom: 24 }}>
      {[
        { label: "NET LIQUIDATION", value: fmtUsd(netLiq), color: C.text },
        { label: "BUYING POWER", value: fmtUsd(buyPow), color: C.blue },
        { label: "DELTA TOTAL", value: (totalDelta > 0 ? "+" : "") + fmtNum(totalDelta, 1), color: plColor(totalDelta) },
        { label: "THETA / DIA", value: (totalTheta > 0 ? "+" : "") + "$" + fmtNum(totalTheta, 0) + "/dia", color: plColor(totalTheta) },
      ].map((kpi, i) => (
        <div key={i} style={{ padding: 18, background: C.panel, borderRadius: 8, border: `1px solid ${C.border}` }}>
          <div style={{ fontSize: 9, color: C.dim, textTransform: "uppercase", letterSpacing: 1, marginBottom: 8, fontWeight: 600 }}>{kpi.label}</div>
          <div style={{ fontSize: 22, fontWeight: 700, color: kpi.color, fontFamily: "monospace" }}>{kpi.value}</div>
        </div>
      ))}
    </div>
  );

  /* ══════════════════════════════════════════════
     SECTION 2 — POSITION MONITOR
     ══════════════════════════════════════════════ */
  const renderPositions = () => {
    const thStyle = { padding: "10px 8px", textAlign: "left" as const, color: C.dim, fontWeight: 600, fontSize: 9, textTransform: "uppercase" as const, letterSpacing: 0.5 };
    const tdStyle = { padding: "7px 8px", fontFamily: "monospace", fontSize: 11 };

    return (
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 14, fontWeight: 700, color: C.text, marginBottom: 12 }}>Monitor de Posicoes</div>
        <div style={{ background: C.panel, borderRadius: 8, border: `1px solid ${C.border}`, overflow: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
            <thead>
              <tr style={{ borderBottom: `1px solid ${C.border}` }}>
                {["Instrumento", "Pos", "Avg Cost", "Mkt Value", "P&L", "Delta", "Theta", "Vega", "IV%"].map(h => (
                  <th key={h} style={thStyle}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {symbols.map(sym => {
                const legs = byUnd[sym];
                const grpDelta = legs.reduce((s, p) => s + (p.delta != null ? p.delta * Math.abs(p.qty) : 0), 0);
                const grpTheta = legs.reduce((s, p) => s + (p.theta != null ? p.theta * Math.abs(p.qty) : 0), 0);
                const grpVega = legs.reduce((s, p) => s + (p.vega != null ? p.vega * Math.abs(p.qty) : 0), 0);
                const grpMV = legs.reduce((s, p) => s + p.mktValue, 0);

                return [
                  // Group header
                  <tr key={sym + "-hdr"} style={{ background: "rgba(70,140,220,.06)", borderBottom: `1px solid ${C.border}33` }}>
                    <td colSpan={5} style={{ ...tdStyle, color: C.gold, fontWeight: 700, fontSize: 11 }}>
                      {sym} <span style={{ color: C.muted, fontWeight: 400, fontSize: 10 }}>({legs.length} legs)</span>
                    </td>
                    <td style={{ ...tdStyle, color: plColor(grpDelta), fontWeight: 600 }}>{fmtNum(grpDelta, 1)}</td>
                    <td style={{ ...tdStyle, color: plColor(grpTheta), fontWeight: 600 }}>{fmtNum(grpTheta, 2)}</td>
                    <td style={{ ...tdStyle, color: C.gold, fontWeight: 600 }}>{fmtNum(grpVega, 1)}</td>
                    <td style={tdStyle}></td>
                  </tr>,
                  // Individual positions
                  ...legs.map((p, i) => {
                    const label = p.isOption
                      ? `${p.localSymbol}`
                      : p.localSymbol || p.symbol;
                    const pl = p.mktValue - (p.avgCost * Math.abs(p.qty));
                    return (
                      <tr key={sym + "-" + i} style={{ borderBottom: `1px solid ${C.border}22` }}>
                        <td style={{ ...tdStyle, color: C.text, paddingLeft: 20 }}>
                          <span style={{ color: p.qty > 0 ? C.green : C.red, fontSize: 8, marginRight: 6 }}>{p.qty > 0 ? "LONG" : "SHORT"}</span>
                          {label}
                        </td>
                        <td style={{ ...tdStyle, color: p.qty > 0 ? C.green : C.red, fontWeight: 600 }}>{p.qty > 0 ? "+" : ""}{p.qty}</td>
                        <td style={{ ...tdStyle, color: C.muted }}>{fmtUsd(p.avgCost, 2)}</td>
                        <td style={{ ...tdStyle, color: C.text }}>{fmtUsd(p.mktValue, 0)}</td>
                        <td style={{ ...tdStyle, color: plColor(pl), fontWeight: 600 }}>{pl >= 0 ? "+" : ""}{fmtUsd(pl, 0)}</td>
                        <td style={{ ...tdStyle, color: p.delta != null ? plColor(p.delta) : C.dim }}>{p.delta != null ? fmtNum(p.delta, 4) : "-"}</td>
                        <td style={{ ...tdStyle, color: p.theta != null ? plColor(p.theta) : C.dim }}>{p.theta != null ? fmtNum(p.theta, 4) : "-"}</td>
                        <td style={{ ...tdStyle, color: C.gold }}>{p.vega != null ? fmtNum(p.vega, 4) : "-"}</td>
                        <td style={{ ...tdStyle, color: C.muted }}>{p.iv != null ? (p.iv * 100).toFixed(0) + "%" : "-"}</td>
                      </tr>
                    );
                  }),
                ];
              })}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  /* ── position map for cross-referencing chain with portfolio ── */
  const posMap = useMemo(() => {
    // Map: symbol -> strike -> optType -> total qty
    const m: Record<string, Record<number, Record<string, number>>> = {};
    parsed.filter(p => p.isOption).forEach(p => {
      if (!m[p.symbol]) m[p.symbol] = {};
      if (!m[p.symbol][p.strike]) m[p.symbol][p.strike] = {};
      m[p.symbol][p.strike][p.optType] = (m[p.symbol][p.strike][p.optType] || 0) + p.qty;
    });
    return m;
  }, [parsed]);

  // Net direction per underlying from portfolio
  const undDirection = useMemo(() => {
    const m: Record<string, number> = {};
    parsed.filter(p => p.isOption).forEach(p => {
      m[p.symbol] = (m[p.symbol] || 0) + p.qty;
    });
    return m;
  }, [parsed]);

  /* ══════════════════════════════════════════════
     SECTION 3 — STRATEGY BUILDER (Options Chain)
     ══════════════════════════════════════════════ */
  const renderStrategyBuilder = () => {
    if (!chainUnderlyings.length) {
      return (
        <div style={{ marginBottom: 32, padding: 24, background: C.panel, borderRadius: 8, border: `1px solid ${C.border}`, color: C.muted, fontSize: 12 }}>
          Options chain nao disponivel. Execute: <code style={{ color: C.gold }}>python pipeline/collect_options_chain.py</code>
        </div>
      );
    }

    const activeUndData = optionsChain?.underlyings?.[activeChainUnd] || {};
    const calls: any[] = expData.calls || [];
    const puts: any[] = expData.puts || [];
    const allStrikes = [...new Set([...calls.map((c: any) => c.strike), ...puts.map((p: any) => p.strike)])].sort((a, b) => a - b);
    const callMap = Object.fromEntries(calls.map((c: any) => [c.strike, c]));
    const putMap = Object.fromEntries(puts.map((p: any) => [p.strike, p]));
    const atmStrike = expData.atm_strike;
    const myStrikes = posMap[activeChainUnd] || {};

    const builderDelta = builderLegs.reduce((s, l) => {
      const row = l.right === "C" ? callMap[l.strike] : putMap[l.strike];
      return s + (row?.delta || 0) * l.qty;
    }, 0);
    const builderTheta = builderLegs.reduce((s, l) => {
      const row = l.right === "C" ? callMap[l.strike] : putMap[l.strike];
      return s + (row?.theta || 0) * l.qty;
    }, 0);

    const addLeg = (strike: number, right: string) => {
      const existing = builderLegs.find(l => l.strike === strike && l.right === right);
      if (existing) {
        setBuilderLegs(builderLegs.map(l => l === existing ? { ...l, qty: l.qty + 1 } : l));
      } else {
        setBuilderLegs([...builderLegs, { strike, right, qty: 1 }]);
      }
    };

    const removeLeg = (idx: number) => {
      setBuilderLegs(builderLegs.filter((_, i) => i !== idx));
    };

    const thSt = { padding: "6px 4px", textAlign: "right" as const, color: C.dim, fontSize: 8, fontWeight: 600, textTransform: "uppercase" as const };
    const tdSt = { padding: "5px 4px", textAlign: "right" as const, fontFamily: "monospace", fontSize: 10 };

    const renderVal = (v: any, color = C.muted) => {
      if (v == null || v === -100) return <span style={{ color: C.dim }}>-</span>;
      return <span style={{ color }}>{typeof v === "number" ? v.toFixed(2) : v}</span>;
    };

    return (
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 14, fontWeight: 700, color: C.text, marginBottom: 12 }}>Strategy Builder</div>

        {/* Underlying tabs — ALL from chain, with position badges */}
        <div style={{ display: "flex", gap: 5, marginBottom: 8, flexWrap: "wrap" }}>
          {chainUnderlyings.map(s => {
            const isActive = s === activeChainUnd;
            const hasPos = undDirection[s] != null;
            const dir = undDirection[s] || 0;
            const undData = optionsChain?.underlyings?.[s] || {};
            return (
              <button key={s} onClick={() => { setChainUnd(s); setChainExp(""); setBuilderLegs([]); }} style={{
                padding: "5px 12px", fontSize: 10, fontWeight: 600, borderRadius: 5, cursor: "pointer",
                background: isActive ? "rgba(70,140,220,.18)" : hasPos ? "rgba(220,180,50,.06)" : "transparent",
                color: isActive ? C.blue : hasPos ? C.text : C.muted,
                border: `1px solid ${isActive ? "rgba(70,140,220,.4)" : hasPos ? "rgba(220,180,50,.25)" : C.border}`,
                display: "flex", alignItems: "center", gap: 4,
              }}>
                {s}
                {hasPos && (
                  <span style={{
                    display: "inline-block", width: 6, height: 6, borderRadius: "50%",
                    background: dir > 0 ? C.green : dir < 0 ? C.red : C.gold,
                  }} />
                )}
                <span style={{ color: C.dim, fontSize: 8, marginLeft: 1 }}>{undData.und_price ? undData.und_price.toFixed(1) : ""}</span>
              </button>
            );
          })}
        </div>

        {/* Expiry tabs — contract code + DTE */}
        <div style={{ display: "flex", gap: 5, marginBottom: 12, flexWrap: "wrap" }}>
          {chainExps.map(exp => {
            const ed = chainData.expirations?.[exp] || {};
            const contract = ed.contract || "";
            const dte = ed.days_to_exp;
            const label = contract || exp;
            const dteStr = dte != null ? ` (${dte}d)` : "";
            return (
              <button key={exp} onClick={() => { setChainExp(exp); setBuilderLegs([]); }} style={{
                padding: "4px 12px", fontSize: 9, fontWeight: 600, borderRadius: 4, cursor: "pointer",
                background: exp === activeChainExp ? "rgba(220,180,50,.18)" : "transparent",
                color: exp === activeChainExp ? C.gold : C.muted,
                border: `1px solid ${exp === activeChainExp ? "rgba(220,180,50,.4)" : C.border}`,
              }}>
                {label}
                <span style={{ color: C.dim, fontSize: 8 }}>{dteStr}</span>
              </button>
            );
          })}
        </div>

        {/* Und price + name header */}
        {activeUndData.name && (
          <div style={{ fontSize: 11, color: C.muted, marginBottom: 8 }}>
            <span style={{ color: C.text, fontWeight: 600 }}>{activeUndData.name}</span>
            {activeUndData.und_price && <span style={{ marginLeft: 8, fontFamily: "monospace", color: C.gold }}>{activeUndData.und_price.toFixed(2)}</span>}
            <span style={{ marginLeft: 8, color: C.dim }}>{allStrikes.length} strikes</span>
          </div>
        )}

        {/* Chain table: CALLS | STRIKE | PUTS */}
        <div style={{ background: C.panel, borderRadius: 8, border: `1px solid ${C.border}`, overflow: "auto", maxHeight: 520 }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead style={{ position: "sticky", top: 0, zIndex: 2, background: C.panel }}>
              <tr style={{ borderBottom: `2px solid ${C.border}` }}>
                <th colSpan={8} style={{ padding: "8px", textAlign: "center", color: C.green, fontSize: 10, fontWeight: 700, background: "rgba(0,200,120,.04)" }}>CALLS</th>
                <th style={{ padding: "8px", textAlign: "center", color: C.gold, fontSize: 10, fontWeight: 700, background: "rgba(220,180,50,.04)" }}>STRIKE</th>
                <th colSpan={8} style={{ padding: "8px", textAlign: "center", color: C.red, fontSize: 10, fontWeight: 700, background: "rgba(220,60,60,.04)" }}>PUTS</th>
              </tr>
              <tr style={{ borderBottom: `1px solid ${C.border}`, background: C.panel }}>
                {["Pos", "Delta", "IV%", "Vol", "Last", "Bid", "Ask", "+"].map(h => <th key={"c" + h} style={thSt}>{h}</th>)}
                <th style={{ ...thSt, textAlign: "center" }}>STRIKE</th>
                {["+", "Bid", "Ask", "Last", "Vol", "IV%", "Delta", "Pos"].map(h => <th key={"p" + h} style={{ ...thSt, textAlign: h === "+" ? "center" : "right" }}>{h}</th>)}
              </tr>
            </thead>
            <tbody>
              {allStrikes.map(strike => {
                const c = callMap[strike] || {};
                const p = putMap[strike] || {};
                const isATM = strike === atmStrike;
                const callQty = myStrikes[strike]?.["C"];
                const putQty = myStrikes[strike]?.["P"];
                const hasPosition = callQty != null || putQty != null;
                const rowBg = hasPosition
                  ? "rgba(220,180,50,.10)"
                  : isATM ? "rgba(220,180,50,.05)" : "transparent";
                const leftBorder = hasPosition ? `3px solid ${C.gold}` : "3px solid transparent";

                return (
                  <tr key={strike} style={{ borderBottom: `1px solid ${C.border}22`, background: rowBg, borderLeft: leftBorder }}>
                    {/* CALL position */}
                    <td style={{ ...tdSt, textAlign: "center" }}>
                      {callQty != null ? (
                        <span style={{ fontSize: 9, fontWeight: 700, color: callQty > 0 ? C.green : C.red }}>
                          {callQty > 0 ? "+" : ""}{callQty}
                        </span>
                      ) : null}
                    </td>
                    {/* CALL data */}
                    <td style={tdSt}>{renderVal(c.delta, C.green)}</td>
                    <td style={tdSt}>{c.iv != null ? <span style={{ color: C.muted }}>{(c.iv * 100).toFixed(0)}</span> : <span style={{ color: C.dim }}>-</span>}</td>
                    <td style={tdSt}>{renderVal(c.volume, C.muted)}</td>
                    <td style={tdSt}>{renderVal(c.last, C.text)}</td>
                    <td style={tdSt}>{renderVal(c.bid, C.green)}</td>
                    <td style={tdSt}>{renderVal(c.ask, C.red)}</td>
                    <td style={{ ...tdSt, textAlign: "center", cursor: "pointer" }} onClick={() => addLeg(strike, "C")}>
                      <span style={{ color: C.blue, fontSize: 12, fontWeight: 700 }}>+</span>
                    </td>
                    {/* STRIKE */}
                    <td style={{
                      ...tdSt, textAlign: "center", fontSize: 11, fontWeight: hasPosition ? 700 : isATM ? 700 : 600,
                      color: hasPosition ? C.gold : isATM ? C.gold : C.text,
                    }}>
                      {strike.toFixed(1)}
                    </td>
                    {/* PUT side */}
                    <td style={{ ...tdSt, textAlign: "center", cursor: "pointer" }} onClick={() => addLeg(strike, "P")}>
                      <span style={{ color: C.blue, fontSize: 12, fontWeight: 700 }}>+</span>
                    </td>
                    <td style={tdSt}>{renderVal(p.bid, C.green)}</td>
                    <td style={tdSt}>{renderVal(p.ask, C.red)}</td>
                    <td style={tdSt}>{renderVal(p.last, C.text)}</td>
                    <td style={tdSt}>{renderVal(p.volume, C.muted)}</td>
                    <td style={tdSt}>{p.iv != null ? <span style={{ color: C.muted }}>{(p.iv * 100).toFixed(0)}</span> : <span style={{ color: C.dim }}>-</span>}</td>
                    <td style={tdSt}>{renderVal(p.delta, C.red)}</td>
                    {/* PUT position */}
                    <td style={{ ...tdSt, textAlign: "center" }}>
                      {putQty != null ? (
                        <span style={{ fontSize: 9, fontWeight: 700, color: putQty > 0 ? C.green : C.red }}>
                          {putQty > 0 ? "+" : ""}{putQty}
                        </span>
                      ) : null}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Builder legs panel */}
        {builderLegs.length > 0 && (
          <div style={{ marginTop: 12, padding: 14, background: C.panel2, borderRadius: 8, border: `1px solid ${C.border}` }}>
            <div style={{ fontSize: 10, color: C.gold, fontWeight: 700, marginBottom: 8, textTransform: "uppercase", letterSpacing: 1 }}>Estrutura Selecionada</div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 10 }}>
              {builderLegs.map((leg, i) => (
                <div key={i} style={{
                  padding: "5px 10px", fontSize: 10, fontFamily: "monospace", borderRadius: 4,
                  background: leg.right === "C" ? "rgba(0,200,120,.1)" : "rgba(220,60,60,.1)",
                  border: `1px solid ${leg.right === "C" ? "rgba(0,200,120,.3)" : "rgba(220,60,60,.3)"}`,
                  color: leg.right === "C" ? C.green : C.red, cursor: "pointer",
                }} onClick={() => removeLeg(i)}>
                  {leg.right === "C" ? "Call" : "Put"} {leg.strike} x{leg.qty} <span style={{ color: C.dim, marginLeft: 4 }}>x</span>
                </div>
              ))}
            </div>
            <div style={{ display: "flex", gap: 24, fontSize: 10 }}>
              <span style={{ color: C.muted }}>Delta: <span style={{ color: plColor(builderDelta), fontWeight: 700, fontFamily: "monospace" }}>{fmtNum(builderDelta, 3)}</span></span>
              <span style={{ color: C.muted }}>Theta: <span style={{ color: plColor(builderTheta), fontWeight: 700, fontFamily: "monospace" }}>{fmtNum(builderTheta, 4)}</span></span>
            </div>
          </div>
        )}
      </div>
    );
  };

  /* ══════════════════════════════════════════════
     SECTION 4 — PAYOFF DIAGRAM + GAUSSIAN
     ══════════════════════════════════════════════ */
  const renderPayoffAndGaussian = () => {
    const activeSym = optSymbols.includes(payoffUnd) ? payoffUnd : optSymbols[0];
    if (!activeSym) return null;

    const getExpiry = (localSymbol: string) => {
      const code = localSymbol.split(" ")[0];
      const mc = code.slice(-2, -1);
      const digit = code.slice(-1);
      const decade = Math.floor(new Date().getFullYear() / 10) * 10;
      const yr = decade + parseInt(digit);
      return (MONTH_MAP[mc] || mc) + "/" + yr;
    };

    const allOptLegs = byUnd[activeSym]?.filter(p => p.isOption) || [];
    const expiries = [...new Set(allOptLegs.map(p => getExpiry(p.localSymbol)))].sort();
    const selExp = payoffExpiry;
    const optLegs = allOptLegs.filter(p => selExp === "all" || getExpiry(p.localSymbol) === selExp);

    const spot = optLegs[0]?.undPrice || prices?.[activeSym]?.slice(-1)?.[0]?.close || 0;
    if (!spot || !optLegs.length) return null;

    const mult = OPT_MULTIPLIERS[activeSym] || 100;
    const lo = spot * 0.7, hi = spot * 1.3;
    const pts = 150;
    const priceRange = Array.from({ length: pts }, (_, i) => lo + (hi - lo) * i / (pts - 1));

    // Payoff at expiration
    const payoff = priceRange.map(S => optLegs.reduce((tot, p) => {
      if (!p.strike) return tot;
      const intr = p.optType === "C" ? Math.max(0, S - p.strike) : Math.max(0, p.strike - S);
      return tot + intr * Math.abs(p.qty) * mult * (p.qty > 0 ? 1 : -1);
    }, 0));

    // Current P&L estimate (using delta approximation)
    const payoffNow = priceRange.map(S => {
      const dS = S - spot;
      return optLegs.reduce((tot, p) => {
        if (p.delta == null) return tot;
        const posDelta = p.delta * p.qty * mult;
        const posGamma = (p.gamma || 0) * p.qty * mult;
        return tot + posDelta * dS + 0.5 * posGamma * dS * dS;
      }, 0);
    });

    // Breakeven points
    const breakevenPoints: number[] = [];
    for (let i = 1; i < payoff.length; i++) {
      if ((payoff[i - 1] < 0 && payoff[i] >= 0) || (payoff[i - 1] >= 0 && payoff[i] < 0)) {
        const price = priceRange[i - 1] + (priceRange[i] - priceRange[i - 1]) * Math.abs(payoff[i - 1]) / (Math.abs(payoff[i - 1]) + Math.abs(payoff[i]));
        breakevenPoints.push(Math.round(price * 100) / 100);
      }
    }

    const minPL = Math.min(...payoff, ...payoffNow);
    const maxPL = Math.max(...payoff, ...payoffNow);
    const plRange = Math.max(Math.abs(minPL), Math.abs(maxPL), 1);

    // SVG dimensions
    const svgW = 480, svgH = 260, padL = 65, padR = 15, padT = 20, padB = 35;
    const plotW = svgW - padL - padR, plotH = svgH - padT - padB;
    const toX = (i: number) => padL + (i / (pts - 1)) * plotW;
    const toY = (v: number) => padT + plotH / 2 - (v / plRange) * (plotH / 2);
    const zeroY = toY(0);
    const spotX = padL + ((spot - lo) / (hi - lo)) * plotW;

    const pathExpiry = "M" + payoff.map((v, i) => `${toX(i).toFixed(1)},${toY(v).toFixed(1)}`).join("L");
    const pathNow = "M" + payoffNow.map((v, i) => `${toX(i).toFixed(1)},${toY(v).toFixed(1)}`).join("L");

    // Area fills
    const areaGreen = "M" + payoff.map((v, i) => {
      const y = v >= 0 ? toY(v) : zeroY;
      return `${toX(i).toFixed(1)},${y.toFixed(1)}`;
    }).join("L") + `L${toX(pts - 1).toFixed(1)},${zeroY.toFixed(1)}L${toX(0).toFixed(1)},${zeroY.toFixed(1)}Z`;
    const areaRed = "M" + payoff.map((v, i) => {
      const y = v <= 0 ? toY(v) : zeroY;
      return `${toX(i).toFixed(1)},${y.toFixed(1)}`;
    }).join("L") + `L${toX(pts - 1).toFixed(1)},${zeroY.toFixed(1)}L${toX(0).toFixed(1)},${zeroY.toFixed(1)}Z`;

    const uniqueStrikes = [...new Set(optLegs.map(p => p.strike))].filter(k => k >= lo && k <= hi);

    // X axis labels
    const xLabels = [0, Math.floor(pts / 4), Math.floor(pts / 2), Math.floor(3 * pts / 4), pts - 1];

    // ── Gaussian ──
    const meanPL = payoff.reduce((a, b) => a + b, 0) / payoff.length;
    const stdPL = Math.sqrt(payoff.reduce((s, v) => s + (v - meanPL) ** 2, 0) / payoff.length) || 1;
    const gsvgW = 300, gsvgH = 260, gPadL = 10, gPadR = 10, gPadT = 20, gPadB = 35;
    const gPlotW = gsvgW - gPadL - gPadR, gPlotH = gsvgH - gPadT - gPadB;
    const gLo = meanPL - 3 * stdPL, gHi = meanPL + 3 * stdPL;
    const gPts = 100;
    const gRange = Array.from({ length: gPts }, (_, i) => gLo + (gHi - gLo) * i / (gPts - 1));
    const gValues = gRange.map(x => gaussianY(x, meanPL, stdPL));
    const gMax = Math.max(...gValues);
    const gToX = (i: number) => gPadL + (i / (gPts - 1)) * gPlotW;
    const gToY = (v: number) => gPadT + gPlotH - (v / gMax) * gPlotH;

    // VaR 95% and 99%
    const sortedPayoff = [...payoff].sort((a, b) => a - b);
    const var95 = sortedPayoff[Math.floor(sortedPayoff.length * 0.05)];
    const var99 = sortedPayoff[Math.floor(sortedPayoff.length * 0.01)];
    const probProfit = payoff.filter(v => v >= 0).length / payoff.length;

    // Gaussian area paths
    const gAreaGreen = gRange.map((x, i) => {
      if (x >= 0) return `${gToX(i).toFixed(1)},${gToY(gValues[i]).toFixed(1)}`;
      return `${gToX(i).toFixed(1)},${gToY(0).toFixed(1)}`;
    });
    const gAreaRed = gRange.map((x, i) => {
      if (x < 0) return `${gToX(i).toFixed(1)},${gToY(gValues[i]).toFixed(1)}`;
      return `${gToX(i).toFixed(1)},${gToY(0).toFixed(1)}`;
    });

    const zeroIdx = gRange.findIndex(x => x >= 0);
    const zeroGX = gPadL + (zeroIdx / (gPts - 1)) * gPlotW;

    return (
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontSize: 14, fontWeight: 700, color: C.text, marginBottom: 12 }}>Payoff & Distribuicao</div>

        {/* Underlying / Expiry selectors */}
        <div style={{ display: "flex", gap: 6, marginBottom: 8, flexWrap: "wrap" }}>
          {optSymbols.map(s => (
            <button key={s} onClick={() => { setPayoffUnd(s); setPayoffExpiry("all"); }} style={{
              padding: "5px 14px", fontSize: 10, fontWeight: 600, borderRadius: 5, cursor: "pointer",
              background: s === activeSym ? "rgba(124,58,237,.18)" : "transparent",
              color: s === activeSym ? "#a78bfa" : C.muted,
              border: `1px solid ${s === activeSym ? "rgba(124,58,237,.4)" : C.border}`,
            }}>{s} ({byUnd[s].filter(p => p.isOption).length} legs)</button>
          ))}
        </div>
        {expiries.length > 1 && (
          <div style={{ display: "flex", gap: 5, marginBottom: 12, flexWrap: "wrap" }}>
            <button onClick={() => setPayoffExpiry("all")} style={{
              padding: "4px 12px", fontSize: 9, fontWeight: 600, borderRadius: 4, cursor: "pointer",
              background: selExp === "all" ? "rgba(220,180,50,.18)" : "transparent",
              color: selExp === "all" ? C.gold : C.muted,
              border: `1px solid ${selExp === "all" ? "rgba(220,180,50,.4)" : C.border}`,
            }}>Todos</button>
            {expiries.map(exp => (
              <button key={exp} onClick={() => setPayoffExpiry(exp)} style={{
                padding: "4px 12px", fontSize: 9, fontWeight: 600, borderRadius: 4, cursor: "pointer",
                background: selExp === exp ? "rgba(220,180,50,.18)" : "transparent",
                color: selExp === exp ? C.gold : C.muted,
                border: `1px solid ${selExp === exp ? "rgba(220,180,50,.4)" : C.border}`,
              }}>{exp}</button>
            ))}
          </div>
        )}

        <div style={{ display: "grid", gridTemplateColumns: "3fr 2fr", gap: 16 }}>
          {/* LEFT: Payoff Diagram */}
          <div style={{ background: C.panel, borderRadius: 8, border: `1px solid ${C.border}`, padding: 12 }}>
            <div style={{ fontSize: 10, color: C.dim, marginBottom: 6, fontWeight: 600 }}>PAYOFF DIAGRAM</div>
            <svg width={svgW} height={svgH} style={{ display: "block" }}>
              {/* grid */}
              <line x1={padL} y1={zeroY} x2={svgW - padR} y2={zeroY} stroke={C.border} strokeWidth={1} />
              <line x1={spotX} y1={padT} x2={spotX} y2={svgH - padB} stroke={C.gold} strokeWidth={1} strokeDasharray="4,3" />
              <text x={spotX} y={padT - 4} fill={C.gold} fontSize={8} textAnchor="middle">Spot {spot.toFixed(1)}</text>

              {/* strike lines */}
              {uniqueStrikes.map(k => {
                const sx = padL + ((k - lo) / (hi - lo)) * plotW;
                return <line key={k} x1={sx} y1={padT} x2={sx} y2={svgH - padB} stroke="rgba(148,163,184,.12)" strokeWidth={0.5} strokeDasharray="2,4" />;
              })}

              {/* breakeven lines */}
              {breakevenPoints.map((bp, i) => {
                const bx = padL + ((bp - lo) / (hi - lo)) * plotW;
                return (
                  <g key={"be" + i}>
                    <line x1={bx} y1={padT} x2={bx} y2={svgH - padB} stroke="rgba(220,180,50,.5)" strokeWidth={1} strokeDasharray="3,3" />
                    <text x={bx} y={svgH - padB + 12} fill={C.gold} fontSize={7} textAnchor="middle">BE {bp.toFixed(1)}</text>
                  </g>
                );
              })}

              {/* area fills */}
              <path d={areaGreen} fill="rgba(0,200,120,.08)" />
              <path d={areaRed} fill="rgba(220,60,60,.08)" />

              {/* P&L now (dashed) */}
              <path d={pathNow} fill="none" stroke={C.blue} strokeWidth={1.5} strokeDasharray="4,3" opacity={0.7} />

              {/* P&L at expiry (solid) */}
              <path d={pathExpiry} fill="none" stroke={C.text} strokeWidth={2} />

              {/* Y axis labels */}
              {[-1, -0.5, 0, 0.5, 1].map(frac => {
                const val = frac * plRange;
                return (
                  <text key={frac} x={padL - 6} y={toY(val) + 3} fill={C.dim} fontSize={8} textAnchor="end" fontFamily="monospace">
                    {fmtUsd(val, 0)}
                  </text>
                );
              })}

              {/* X axis labels */}
              {xLabels.map(i => (
                <text key={i} x={toX(i)} y={svgH - padB + 14} fill={C.dim} fontSize={8} textAnchor="middle" fontFamily="monospace">
                  {priceRange[i].toFixed(1)}
                </text>
              ))}

              {/* legend */}
              <line x1={padL + 10} y1={padT + 6} x2={padL + 30} y2={padT + 6} stroke={C.text} strokeWidth={2} />
              <text x={padL + 34} y={padT + 9} fill={C.muted} fontSize={8}>Vencimento</text>
              <line x1={padL + 110} y1={padT + 6} x2={padL + 130} y2={padT + 6} stroke={C.blue} strokeWidth={1.5} strokeDasharray="4,3" />
              <text x={padL + 134} y={padT + 9} fill={C.muted} fontSize={8}>Hoje (delta)</text>
            </svg>
          </div>

          {/* RIGHT: Gaussian Distribution */}
          <div style={{ background: C.panel, borderRadius: 8, border: `1px solid ${C.border}`, padding: 12 }}>
            <div style={{ fontSize: 10, color: C.dim, marginBottom: 6, fontWeight: 600 }}>DISTRIBUICAO P&L</div>
            <svg width={gsvgW} height={gsvgH} style={{ display: "block" }}>
              {/* zero line */}
              <line x1={zeroGX} y1={gPadT} x2={zeroGX} y2={gsvgH - gPadB} stroke={C.border} strokeWidth={1} />

              {/* green area (profit) */}
              <path d={`M${gAreaGreen.join("L")}L${gToX(gPts - 1).toFixed(1)},${gToY(0).toFixed(1)}L${zeroGX.toFixed(1)},${gToY(0).toFixed(1)}Z`} fill="rgba(0,200,120,.12)" />

              {/* red area (loss) */}
              <path d={`M${gAreaRed.join("L")}L${zeroGX.toFixed(1)},${gToY(0).toFixed(1)}L${gToX(0).toFixed(1)},${gToY(0).toFixed(1)}Z`} fill="rgba(220,60,60,.12)" />

              {/* curve */}
              <path d={"M" + gValues.map((v, i) => `${gToX(i).toFixed(1)},${gToY(v).toFixed(1)}`).join("L")} fill="none" stroke={C.muted} strokeWidth={1.5} />

              {/* VaR markers */}
              {[
                { val: var95, label: "VaR 95%", color: C.gold },
                { val: var99, label: "VaR 99%", color: C.red },
              ].map(({ val, label, color }) => {
                const vx = gPadL + ((val - gLo) / (gHi - gLo)) * gPlotW;
                if (vx < gPadL || vx > gsvgW - gPadR) return null;
                return (
                  <g key={label}>
                    <line x1={vx} y1={gPadT} x2={vx} y2={gsvgH - gPadB} stroke={color} strokeWidth={1} strokeDasharray="3,3" />
                    <text x={vx} y={gPadT - 4} fill={color} fontSize={7} textAnchor="middle">{label}: {fmtUsd(val, 0)}</text>
                  </g>
                );
              })}

              {/* X axis */}
              {[0, Math.floor(gPts / 4), Math.floor(gPts / 2), Math.floor(3 * gPts / 4), gPts - 1].map(i => (
                <text key={i} x={gToX(i)} y={gsvgH - gPadB + 14} fill={C.dim} fontSize={7} textAnchor="middle" fontFamily="monospace">
                  {fmtUsd(gRange[i], 0)}
                </text>
              ))}

              {/* prob labels */}
              <text x={gsvgW / 2 + 30} y={gsvgH - 8} fill={C.green} fontSize={9} fontWeight={700}>Lucro: {(probProfit * 100).toFixed(0)}%</text>
              <text x={gsvgW / 2 - 70} y={gsvgH - 8} fill={C.red} fontSize={9} fontWeight={700}>Perda: {((1 - probProfit) * 100).toFixed(0)}%</text>
            </svg>
          </div>
        </div>
      </div>
    );
  };

  /* ══════════════════════════════════════════════
     RENDER
     ══════════════════════════════════════════════ */
  if (!portfolio) {
    return <div style={{ padding: 40, color: C.muted, fontSize: 14 }}>Carregando dados do portfolio...</div>;
  }

  return (
    <div style={{ overflow: "auto", padding: 24, minHeight: "100vh" }}>
      {renderKPIs()}
      {renderPositions()}
      {renderStrategyBuilder()}
      {renderPayoffAndGaussian()}
    </div>
  );
}
