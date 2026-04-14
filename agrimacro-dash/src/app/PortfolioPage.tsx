"use client";
import { useState, useMemo, useCallback } from "react";

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
function fmtUsdCompact(v: number) {
  const abs = Math.abs(v);
  if (abs >= 1e6) return (v < 0 ? "-$" : "+$") + (abs / 1e6).toFixed(1) + "M";
  if (abs >= 1e3) return (v < 0 ? "-$" : "+$") + (abs / 1e3).toFixed(0) + "K";
  return (v < 0 ? "-$" : "$") + abs.toFixed(0);
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

interface BuilderLeg {
  type: "call" | "put";
  strike: number;
  expiration: string;
  action: "buy" | "sell";
  quantity: number;
  bid: number;
  ask: number;
  delta: number;
  iv: number;
  theta: number;
}

/* ══════════════════════════════════════════════ */
export default function PortfolioPage({ portfolio, greeks, optionsChain, prices }: PortfolioPageProps) {
  const [chainUnd, setChainUnd] = useState("CL");
  const [chainExp, setChainExp] = useState("");
  const [payoffUnd, setPayoffUnd] = useState("CL");
  const [payoffExpiry, setPayoffExpiry] = useState("all");
  const [builderLegs, setBuilderLegs] = useState<BuilderLeg[]>([]);
  const [payoffTooltip, setPayoffTooltip] = useState<{
    x: number; y: number; price: number; plExp: number; plToday: number;
  } | null>(null);
  const [useBuilderPayoff, setUseBuilderPayoff] = useState(false);
  const [orderStatus, setOrderStatus] = useState<'idle' | 'confirming' | 'sending' | 'success' | 'error'>('idle');
  const [orderResult, setOrderResult] = useState<any>(null);
  const [orderType, setOrderType] = useState<'LMT' | 'MKT'>('LMT');
  const [orderNote, setOrderNote] = useState('');

  /* ── parse portfolio ── */
  const summ = portfolio?.summary || {};
  const netLiq = parseFloat(summ.NetLiquidation || "0");
  const buyPow = parseFloat(summ.BuyingPower || "0");
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

  /* ── position map for cross-referencing chain with portfolio ── */
  const posMap = useMemo(() => {
    const m: Record<string, Record<number, Record<string, number>>> = {};
    parsed.filter(p => p.isOption).forEach(p => {
      if (!m[p.symbol]) m[p.symbol] = {};
      if (!m[p.symbol][p.strike]) m[p.symbol][p.strike] = {};
      m[p.symbol][p.strike][p.optType] = (m[p.symbol][p.strike][p.optType] || 0) + p.qty;
    });
    return m;
  }, [parsed]);

  const undDirection = useMemo(() => {
    const m: Record<string, number> = {};
    parsed.filter(p => p.isOption).forEach(p => {
      m[p.symbol] = (m[p.symbol] || 0) + p.qty;
    });
    return m;
  }, [parsed]);

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

                return [
                  <tr key={sym + "-hdr"} style={{ background: "rgba(70,140,220,.06)", borderBottom: `1px solid ${C.border}33` }}>
                    <td colSpan={5} style={{ ...tdStyle, color: C.gold, fontWeight: 700, fontSize: 11 }}>
                      {sym} <span style={{ color: C.muted, fontWeight: 400, fontSize: 10 }}>({legs.length} legs)</span>
                    </td>
                    <td style={{ ...tdStyle, color: plColor(grpDelta), fontWeight: 600 }}>{fmtNum(grpDelta, 1)}</td>
                    <td style={{ ...tdStyle, color: plColor(grpTheta), fontWeight: 600 }}>{fmtNum(grpTheta, 2)}</td>
                    <td style={{ ...tdStyle, color: C.gold, fontWeight: 600 }}>{fmtNum(grpVega, 1)}</td>
                    <td style={tdStyle}></td>
                  </tr>,
                  ...legs.map((p, i) => {
                    const label = p.isOption ? `${p.localSymbol}` : p.localSymbol || p.symbol;
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

    const mult = OPT_MULTIPLIERS[activeChainUnd] || 100;

    const addLeg = (strike: number, right: string) => {
      const row = right === "C" ? callMap[strike] : putMap[strike];
      const existing = builderLegs.find(l => l.strike === strike && l.type === (right === "C" ? "call" : "put"));
      if (existing) {
        setBuilderLegs(builderLegs.map(l => l === existing ? { ...l, quantity: l.quantity + 1 } : l));
      } else {
        const ed = chainData.expirations?.[activeChainExp] || {};
        const contract = ed.contract || activeChainExp;
        setBuilderLegs([...builderLegs, {
          type: right === "C" ? "call" : "put",
          strike,
          expiration: contract,
          action: "sell",
          quantity: 1,
          bid: row?.bid ?? 0,
          ask: row?.ask ?? 0,
          delta: row?.delta ?? 0,
          iv: row?.iv ?? 0,
          theta: row?.theta ?? 0,
        }]);
      }
      setUseBuilderPayoff(false);
    };

    const removeLeg = (idx: number) => {
      setBuilderLegs(builderLegs.filter((_, i) => i !== idx));
      setUseBuilderPayoff(false);
    };

    const updateLeg = (idx: number, updates: Partial<BuilderLeg>) => {
      setBuilderLegs(builderLegs.map((l, i) => i === idx ? { ...l, ...updates } : l));
      setUseBuilderPayoff(false);
    };

    // Builder totals
    const bTotalDelta = builderLegs.reduce((s, l) => {
      const sign = l.action === "sell" ? -1 : 1;
      return s + l.delta * l.quantity * sign;
    }, 0);
    const bTotalTheta = builderLegs.reduce((s, l) => {
      const sign = l.action === "sell" ? -1 : 1;
      return s + l.theta * l.quantity * sign;
    }, 0);
    const bTotalCredit = builderLegs.reduce((s, l) => {
      const price = l.action === "sell" ? l.bid : -l.ask;
      return s + price * l.quantity * mult;
    }, 0);

    // Compute max gain / max loss / breakeven from builder legs
    const computeBuilderPayoff = useCallback(() => {
      if (!builderLegs.length) return { maxGain: 0, maxLoss: 0, breakeven: 0 };
      const spot = activeUndData.und_price || 100;
      const lo = spot * 0.5, hi = spot * 1.5;
      const testPrices = Array.from({ length: 200 }, (_, i) => lo + (hi - lo) * i / 199);
      const payoffs = testPrices.map(S => builderLegs.reduce((tot, l) => {
        const sign = l.action === "sell" ? -1 : 1;
        const intr = l.type === "call" ? Math.max(0, S - l.strike) : Math.max(0, l.strike - S);
        const premium = l.action === "sell" ? l.bid : -l.ask;
        return tot + (premium + sign * intr) * l.quantity * mult * (l.action === "sell" ? 1 : 1);
      }, 0));
      // Simplified: credit received + intrinsic at expiry
      const builderPayoffs = testPrices.map(S => {
        return builderLegs.reduce((tot, l) => {
          const sign = l.action === "sell" ? -1 : 1;
          const intr = l.type === "call" ? Math.max(0, S - l.strike) : Math.max(0, l.strike - S);
          const premium = l.action === "sell" ? l.bid : l.ask;
          return tot + (sign * intr * -1 + (l.action === "sell" ? premium : -premium)) * l.quantity * mult;
        }, 0);
      });
      const maxG = Math.max(...builderPayoffs);
      const maxL = Math.min(...builderPayoffs);
      let be = 0;
      for (let i = 1; i < builderPayoffs.length; i++) {
        if ((builderPayoffs[i - 1] < 0 && builderPayoffs[i] >= 0) || (builderPayoffs[i - 1] >= 0 && builderPayoffs[i] < 0)) {
          be = testPrices[i - 1] + (testPrices[i] - testPrices[i - 1]) * Math.abs(builderPayoffs[i - 1]) / (Math.abs(builderPayoffs[i - 1]) + Math.abs(builderPayoffs[i]));
          break;
        }
      }
      return { maxGain: maxG, maxLoss: maxL, breakeven: be };
    }, [builderLegs, activeUndData.und_price, mult]);

    const builderStats = builderLegs.length > 0 ? computeBuilderPayoff() : { maxGain: 0, maxLoss: 0, breakeven: 0 };

    /* ── MELHORIA 4: column widths for POS visibility ── */
    const posColW = "42px";
    const thSt = { padding: "6px 4px", textAlign: "right" as const, color: C.dim, fontSize: 8, fontWeight: 600, textTransform: "uppercase" as const };
    const tdSt = { padding: "5px 4px", textAlign: "right" as const, fontFamily: "monospace", fontSize: 10 };

    const renderVal = (v: any, color = C.muted) => {
      if (v == null || v === -100) return <span style={{ color: C.dim }}>-</span>;
      return <span style={{ color }}>{typeof v === "number" ? v.toFixed(2) : v}</span>;
    };

    return (
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontSize: 14, fontWeight: 700, color: C.text, marginBottom: 12 }}>Strategy Builder</div>

        {/* Underlying tabs */}
        <div style={{ display: "flex", gap: 5, marginBottom: 8, flexWrap: "wrap" }}>
          {chainUnderlyings.map(s => {
            const isActive = s === activeChainUnd;
            const hasPos = undDirection[s] != null;
            const dir = undDirection[s] || 0;
            const undData = optionsChain?.underlyings?.[s] || {};
            return (
              <button key={s} onClick={() => { setChainUnd(s); setChainExp(""); setBuilderLegs([]); setUseBuilderPayoff(false); }} style={{
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

        {/* Expiry tabs */}
        <div style={{ display: "flex", gap: 5, marginBottom: 12, flexWrap: "wrap" }}>
          {chainExps.map(exp => {
            const ed = chainData.expirations?.[exp] || {};
            const contract = ed.contract || "";
            const dte = ed.days_to_exp;
            const label = contract || exp;
            const dteStr = dte != null ? ` (${dte}d)` : "";
            return (
              <button key={exp} onClick={() => { setChainExp(exp); setBuilderLegs([]); setUseBuilderPayoff(false); }} style={{
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

        {/* Chain table: CALLS | STRIKE | PUTS — MELHORIA 4: POS column visible */}
        <div style={{ background: C.panel, borderRadius: 8, border: `1px solid ${C.border}`, overflow: "auto", maxHeight: 520 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", tableLayout: "fixed" }}>
            <colgroup>
              {/* CALLS: POS, DELTA, IV%, VOL, LAST, BID, ASK, + */}
              <col style={{ width: posColW }} />
              <col style={{ width: "52px" }} />
              <col style={{ width: "42px" }} />
              <col style={{ width: "42px" }} />
              <col style={{ width: "52px" }} />
              <col style={{ width: "52px" }} />
              <col style={{ width: "52px" }} />
              <col style={{ width: "30px" }} />
              {/* STRIKE */}
              <col style={{ width: "62px" }} />
              {/* PUTS: +, BID, ASK, LAST, VOL, IV%, DELTA, POS */}
              <col style={{ width: "30px" }} />
              <col style={{ width: "52px" }} />
              <col style={{ width: "52px" }} />
              <col style={{ width: "52px" }} />
              <col style={{ width: "42px" }} />
              <col style={{ width: "42px" }} />
              <col style={{ width: "52px" }} />
              <col style={{ width: posColW }} />
            </colgroup>
            <thead style={{ position: "sticky", top: 0, zIndex: 2, background: C.panel }}>
              <tr style={{ borderBottom: `2px solid ${C.border}` }}>
                <th colSpan={8} style={{ padding: "8px", textAlign: "center", color: C.green, fontSize: 10, fontWeight: 700, background: "rgba(0,200,120,.04)" }}>CALLS</th>
                <th style={{ padding: "8px", textAlign: "center", color: C.gold, fontSize: 10, fontWeight: 700, background: "rgba(220,180,50,.04)" }}>STRIKE</th>
                <th colSpan={8} style={{ padding: "8px", textAlign: "center", color: C.red, fontSize: 10, fontWeight: 700, background: "rgba(220,60,60,.04)" }}>PUTS</th>
              </tr>
              <tr style={{ borderBottom: `1px solid ${C.border}`, background: C.panel }}>
                {["Pos", "Delta", "IV%", "Vol", "Last", "Bid", "Ask", "+"].map(h => (
                  <th key={"c" + h} style={{ ...thSt, minWidth: h === "Pos" ? 40 : undefined }}>{h}</th>
                ))}
                <th style={{ ...thSt, textAlign: "center" }}>STRIKE</th>
                {["+", "Bid", "Ask", "Last", "Vol", "IV%", "Delta", "Pos"].map(h => (
                  <th key={"p" + h} style={{ ...thSt, textAlign: h === "+" ? "center" : "right", minWidth: h === "Pos" ? 40 : undefined }}>{h}</th>
                ))}
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
                    {/* CALL POS — first column, always visible */}
                    <td style={{ ...tdSt, textAlign: "center", minWidth: 40 }}>
                      {callQty != null ? (
                        <span style={{ fontSize: 9, fontWeight: 700, color: callQty > 0 ? C.green : C.red }}>
                          {callQty > 0 ? "+" : ""}{callQty}
                        </span>
                      ) : null}
                    </td>
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
                    {/* PUT POS — last column, always visible */}
                    <td style={{ ...tdSt, textAlign: "center", minWidth: 40 }}>
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

        {/* ══════ MELHORIA 3: Builder Legs Panel (carrinho) ══════ */}
        {builderLegs.length > 0 && (
          <div style={{ marginTop: 12, padding: 14, background: C.panel2, borderRadius: 8, border: `1px solid ${C.border}` }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
              <div style={{ fontSize: 10, color: C.gold, fontWeight: 700, textTransform: "uppercase", letterSpacing: 1 }}>ESTRUTURA MONTADA</div>
              <div style={{ display: "flex", gap: 6 }}>
                <button onClick={() => { setBuilderLegs([]); setUseBuilderPayoff(false); }} style={{
                  padding: "3px 10px", fontSize: 9, background: "rgba(220,60,60,.15)", color: C.red,
                  border: `1px solid rgba(220,60,60,.3)`, borderRadius: 4, cursor: "pointer", fontWeight: 600,
                }}>Limpar</button>
                <button onClick={() => setUseBuilderPayoff(true)} style={{
                  padding: "3px 10px", fontSize: 9, background: "rgba(70,140,220,.15)", color: C.blue,
                  border: `1px solid rgba(70,140,220,.3)`, borderRadius: 4, cursor: "pointer", fontWeight: 600,
                }}>Ver Payoff</button>
                <button onClick={() => { setOrderStatus('confirming'); setOrderResult(null); }} style={{
                  padding: "3px 12px", fontSize: 9, background: "rgba(220,180,50,.18)", color: C.gold,
                  border: `1px solid rgba(220,180,50,.5)`, borderRadius: 4, cursor: "pointer", fontWeight: 700,
                  letterSpacing: 0.5,
                }}>Executar Ordem no IBKR</button>
              </div>
            </div>

            {/* Legs table */}
            <div style={{ overflow: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 10 }}>
                <thead>
                  <tr style={{ borderBottom: `1px solid ${C.border}` }}>
                    {["Ac", "Tipo", "Strike", "Venc", "Bid", "Ask", "Qty", ""].map(h => (
                      <th key={h} style={{ padding: "5px 6px", textAlign: "left", color: C.dim, fontSize: 8, fontWeight: 600, textTransform: "uppercase" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {builderLegs.map((leg, i) => (
                    <tr key={i} style={{ borderBottom: `1px solid ${C.border}22` }}>
                      <td style={{ padding: "5px 6px" }}>
                        <button onClick={() => updateLeg(i, { action: leg.action === "sell" ? "buy" : "sell" })} style={{
                          padding: "2px 6px", fontSize: 9, fontWeight: 700, borderRadius: 3, cursor: "pointer",
                          background: leg.action === "sell" ? "rgba(220,60,60,.15)" : "rgba(0,200,120,.15)",
                          color: leg.action === "sell" ? C.red : C.green,
                          border: `1px solid ${leg.action === "sell" ? "rgba(220,60,60,.3)" : "rgba(0,200,120,.3)"}`,
                        }}>{leg.action === "sell" ? "S" : "B"}</button>
                      </td>
                      <td style={{ padding: "5px 6px", color: leg.type === "call" ? C.green : C.red, fontWeight: 600, fontFamily: "monospace" }}>
                        {leg.type.toUpperCase()}
                      </td>
                      <td style={{ padding: "5px 6px", color: C.text, fontFamily: "monospace" }}>{leg.strike.toFixed(1)}</td>
                      <td style={{ padding: "5px 6px", color: C.muted, fontFamily: "monospace" }}>{leg.expiration}</td>
                      <td style={{ padding: "5px 6px", color: C.green, fontFamily: "monospace" }}>{leg.bid.toFixed(2)}</td>
                      <td style={{ padding: "5px 6px", color: C.red, fontFamily: "monospace" }}>{leg.ask.toFixed(2)}</td>
                      <td style={{ padding: "5px 6px" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
                          <button onClick={() => updateLeg(i, { quantity: Math.max(1, leg.quantity - 1) })} style={{
                            width: 18, height: 18, fontSize: 10, background: "rgba(148,163,184,.1)",
                            color: C.muted, border: `1px solid ${C.border}`, borderRadius: 3, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center",
                          }}>-</button>
                          <span style={{ color: C.text, fontFamily: "monospace", fontWeight: 600, minWidth: 20, textAlign: "center" }}>{leg.quantity}</span>
                          <button onClick={() => updateLeg(i, { quantity: leg.quantity + 1 })} style={{
                            width: 18, height: 18, fontSize: 10, background: "rgba(148,163,184,.1)",
                            color: C.muted, border: `1px solid ${C.border}`, borderRadius: 3, cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center",
                          }}>+</button>
                        </div>
                      </td>
                      <td style={{ padding: "5px 6px" }}>
                        <button onClick={() => removeLeg(i)} style={{
                          fontSize: 10, color: C.dim, background: "none", border: "none", cursor: "pointer",
                        }}>x</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Dynamic totals */}
            <div style={{ marginTop: 10, paddingTop: 10, borderTop: `1px solid ${C.border}`, display: "flex", gap: 20, flexWrap: "wrap", fontSize: 10 }}>
              <span style={{ color: C.muted }}>Delta: <span style={{ color: plColor(bTotalDelta), fontWeight: 700, fontFamily: "monospace" }}>{fmtNum(bTotalDelta, 2)}</span></span>
              <span style={{ color: C.muted }}>Theta: <span style={{ color: plColor(bTotalTheta), fontWeight: 700, fontFamily: "monospace" }}>${fmtNum(bTotalTheta, 2)}/dia</span></span>
              <span style={{ color: C.muted }}>{bTotalCredit >= 0 ? "Credito" : "Debito"}: <span style={{ color: bTotalCredit >= 0 ? C.green : C.red, fontWeight: 700, fontFamily: "monospace" }}>{fmtUsd(Math.abs(bTotalCredit), 0)}</span></span>
              <span style={{ color: C.muted }}>Max Ganho: <span style={{ color: C.green, fontWeight: 700, fontFamily: "monospace" }}>{fmtUsd(builderStats.maxGain, 0)}</span></span>
              <span style={{ color: C.muted }}>Max Perda: <span style={{ color: C.red, fontWeight: 700, fontFamily: "monospace" }}>{fmtUsd(builderStats.maxLoss, 0)}</span></span>
              {builderStats.breakeven > 0 && (
                <span style={{ color: C.muted }}>Breakeven: <span style={{ color: C.gold, fontWeight: 700, fontFamily: "monospace" }}>${builderStats.breakeven.toFixed(2)}</span></span>
              )}
            </div>
          </div>
        )}

        {/* ══════ ORDER CONFIRMATION MODAL ══════ */}
        {orderStatus !== 'idle' && builderLegs.length > 0 && (
          <div style={{
            position: "fixed", top: 0, left: 0, right: 0, bottom: 0,
            background: "rgba(0,0,0,.7)", zIndex: 9999,
            display: "flex", alignItems: "center", justifyContent: "center",
          }} onClick={(e) => { if (e.target === e.currentTarget && orderStatus !== 'sending') setOrderStatus('idle'); }}>
            <div style={{
              background: C.panel, border: `1px solid ${C.gold}`, borderRadius: 12,
              padding: 28, minWidth: 440, maxWidth: 560, boxShadow: "0 20px 60px rgba(0,0,0,.6)",
            }}>
              {/* Header */}
              <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 20 }}>
                <span style={{ fontSize: 22 }}>{orderStatus === 'success' ? '\u2705' : orderStatus === 'error' ? '\u274C' : '\u26A0\uFE0F'}</span>
                <span style={{ fontSize: 16, fontWeight: 700, color: C.gold, letterSpacing: 0.5 }}>
                  {orderStatus === 'confirming' && 'CONFIRMAR EXECUCAO'}
                  {orderStatus === 'sending' && 'ENVIANDO ORDEM...'}
                  {orderStatus === 'success' && 'ORDEM ENVIADA'}
                  {orderStatus === 'error' && 'ERRO NA ORDEM'}
                </span>
              </div>

              {/* Legs summary */}
              {(orderStatus === 'confirming' || orderStatus === 'sending') && (
                <div style={{ marginBottom: 16 }}>
                  <div style={{ background: C.bg, borderRadius: 8, padding: 12, border: `1px solid ${C.border}` }}>
                    {builderLegs.map((leg, i) => (
                      <div key={i} style={{
                        display: "flex", justifyContent: "space-between", alignItems: "center",
                        padding: "6px 0", borderBottom: i < builderLegs.length - 1 ? `1px solid ${C.border}33` : "none",
                        fontSize: 11, fontFamily: "monospace",
                      }}>
                        <span style={{ color: C.text }}>
                          <span style={{ color: leg.action === "sell" ? C.red : C.green, fontWeight: 700 }}>
                            {leg.action === "sell" ? "SELL" : "BUY"}
                          </span>
                          {' '}{leg.quantity}x {leg.type.toUpperCase()} ${leg.strike.toFixed(1)}
                        </span>
                        <span style={{ color: C.muted, fontSize: 10 }}>{leg.expiration}</span>
                      </div>
                    ))}
                  </div>

                  {/* Order details */}
                  <div style={{ marginTop: 12, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, fontSize: 11 }}>
                    <div style={{ padding: "8px 10px", background: C.bg, borderRadius: 6, border: `1px solid ${C.border}` }}>
                      <div style={{ color: C.dim, fontSize: 8, textTransform: "uppercase", marginBottom: 4 }}>Tipo de Ordem</div>
                      <div style={{ display: "flex", gap: 6 }}>
                        {(["LMT", "MKT"] as const).map(t => (
                          <button key={t} onClick={() => setOrderType(t)} style={{
                            padding: "3px 10px", fontSize: 10, fontWeight: 600, borderRadius: 4, cursor: "pointer",
                            background: orderType === t ? "rgba(220,180,50,.2)" : "transparent",
                            color: orderType === t ? C.gold : C.dim,
                            border: `1px solid ${orderType === t ? "rgba(220,180,50,.4)" : C.border}`,
                          }}>{t === "LMT" ? "Limite" : "Mercado"}</button>
                        ))}
                      </div>
                    </div>
                    <div style={{ padding: "8px 10px", background: C.bg, borderRadius: 6, border: `1px solid ${C.border}` }}>
                      <div style={{ color: C.dim, fontSize: 8, textTransform: "uppercase", marginBottom: 4 }}>Valor Estimado</div>
                      <div style={{
                        fontFamily: "monospace", fontWeight: 700, fontSize: 14,
                        color: bTotalCredit >= 0 ? C.green : C.red,
                      }}>
                        {fmtUsd(Math.abs(bTotalCredit), 0)} {bTotalCredit >= 0 ? "credito" : "debito"}
                      </div>
                    </div>
                  </div>

                  {/* Margin estimate */}
                  <div style={{
                    marginTop: 8, padding: "8px 10px", background: C.bg, borderRadius: 6,
                    border: `1px solid ${C.border}`, fontSize: 11,
                  }}>
                    <div style={{ color: C.dim, fontSize: 8, textTransform: "uppercase", marginBottom: 4 }}>Margem Estimada</div>
                    <div style={{ fontFamily: "monospace", color: C.gold, fontWeight: 600 }}>
                      ~{fmtUsd(Math.abs(builderStats.maxLoss) * 0.8, 0)}
                      <span style={{ color: C.dim, fontWeight: 400, fontSize: 9, marginLeft: 6 }}>(estimativa ~80% da perda maxima)</span>
                    </div>
                  </div>

                  {/* Note field */}
                  <div style={{ marginTop: 8 }}>
                    <input
                      type="text"
                      placeholder="Nota (campo livre)..."
                      value={orderNote}
                      onChange={(e) => setOrderNote(e.target.value)}
                      style={{
                        width: "100%", padding: "8px 10px", fontSize: 11, fontFamily: "monospace",
                        background: C.bg, color: C.text, border: `1px solid ${C.border}`,
                        borderRadius: 6, outline: "none", boxSizing: "border-box",
                      }}
                    />
                  </div>
                </div>
              )}

              {/* Success result */}
              {orderStatus === 'success' && orderResult && (
                <div style={{ marginBottom: 16, background: C.bg, borderRadius: 8, padding: 12, border: `1px solid rgba(0,200,120,.3)` }}>
                  <div style={{ fontSize: 11, color: C.green, fontWeight: 600, marginBottom: 8 }}>
                    {orderResult.legs_submitted} leg(s) enviada(s) via {orderResult.port_name}
                  </div>
                  {orderResult.results?.map((r: any, i: number) => (
                    <div key={i} style={{ fontSize: 10, fontFamily: "monospace", color: r.status === "submitted" ? C.text : C.red, marginBottom: 4 }}>
                      Leg {r.leg}: {r.status === "submitted" ? `${r.action} ${r.quantity}x ${r.contract} @ ${r.limit_price ?? "MKT"} (ID: ${r.order_id})` : r.error}
                    </div>
                  ))}
                  <div style={{ fontSize: 9, color: C.dim, marginTop: 8 }}>
                    Conta: {orderResult.account} | {orderResult.timestamp}
                  </div>
                </div>
              )}

              {/* Error result */}
              {orderStatus === 'error' && orderResult && (
                <div style={{ marginBottom: 16, background: C.bg, borderRadius: 8, padding: 12, border: `1px solid rgba(220,60,60,.3)` }}>
                  <div style={{ fontSize: 11, color: C.red, fontFamily: "monospace" }}>
                    {orderResult.error || orderResult.errors?.join(', ') || 'Erro desconhecido'}
                  </div>
                </div>
              )}

              {/* Action buttons */}
              <div style={{ display: "flex", justifyContent: "flex-end", gap: 10, marginTop: 4 }}>
                {orderStatus !== 'sending' && (
                  <button onClick={() => { setOrderStatus('idle'); setOrderResult(null); }} style={{
                    padding: "8px 20px", fontSize: 11, fontWeight: 600, borderRadius: 6, cursor: "pointer",
                    background: "rgba(148,163,184,.1)", color: C.muted,
                    border: `1px solid ${C.border}`,
                  }}>{orderStatus === 'success' || orderStatus === 'error' ? 'Fechar' : 'CANCELAR'}</button>
                )}
                {orderStatus === 'confirming' && (
                  <button
                    onClick={async () => {
                      setOrderStatus('sending');
                      try {
                        // Build expiry from leg expiration field
                        const apiLegs = builderLegs.map(leg => ({
                          symbol: activeChainUnd,
                          type: leg.type,
                          strike: leg.strike,
                          action: leg.action,
                          quantity: leg.quantity,
                          expiry: activeChainExp.replace(/-/g, ''),
                          bid: leg.bid,
                          ask: leg.ask,
                          limit_price: leg.action === "sell" ? leg.bid : leg.ask,
                        }));
                        const resp = await fetch('/api/ibkr-order', {
                          method: 'POST',
                          headers: { 'Content-Type': 'application/json' },
                          body: JSON.stringify({
                            mode: 'execute',
                            legs: apiLegs,
                            order_type: orderType,
                            note: orderNote,
                          }),
                        });
                        const data = await resp.json();
                        if (data.status === 'ok') {
                          setOrderStatus('success');
                        } else {
                          setOrderStatus('error');
                        }
                        setOrderResult(data);
                      } catch (err: any) {
                        setOrderStatus('error');
                        setOrderResult({ error: err.message || 'Falha na comunicacao com o servidor' });
                      }
                    }}
                    style={{
                      padding: "8px 24px", fontSize: 11, fontWeight: 700, borderRadius: 6, cursor: "pointer",
                      background: "rgba(220,180,50,.2)", color: C.gold,
                      border: `1px solid ${C.gold}`, letterSpacing: 0.5,
                    }}
                  >
                    CONFIRMAR E ENVIAR
                  </button>
                )}
              </div>

              {/* Sending spinner */}
              {orderStatus === 'sending' && (
                <div style={{ textAlign: "center", padding: 16, color: C.gold, fontSize: 12 }}>
                  Conectando ao IBKR e enviando ordem...
                </div>
              )}
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
    const svgW = 480, svgH = 260, padL = 65, padR = 15, padT = 25, padB = 35;
    const plotW = svgW - padL - padR, plotH = svgH - padT - padB;
    const toX = (i: number) => padL + (i / (pts - 1)) * plotW;
    const toY = (v: number) => padT + plotH / 2 - (v / plRange) * (plotH / 2);
    const zeroY = toY(0);
    const spotX = padL + ((spot - lo) / (hi - lo)) * plotW;

    const pathExpiry = "M" + payoff.map((v, i) => `${toX(i).toFixed(1)},${toY(v).toFixed(1)}`).join("L");
    const pathNow = "M" + payoffNow.map((v, i) => `${toX(i).toFixed(1)},${toY(v).toFixed(1)}`).join("L");

    // MELHORIA 1: Area fills (green where P&L > 0, red where P&L < 0)
    const areaGreen = "M" + payoff.map((v, i) => {
      const y = v >= 0 ? toY(v) : zeroY;
      return `${toX(i).toFixed(1)},${y.toFixed(1)}`;
    }).join("L") + `L${toX(pts - 1).toFixed(1)},${zeroY.toFixed(1)}L${toX(0).toFixed(1)},${zeroY.toFixed(1)}Z`;
    const areaRed = "M" + payoff.map((v, i) => {
      const y = v <= 0 ? toY(v) : zeroY;
      return `${toX(i).toFixed(1)},${y.toFixed(1)}`;
    }).join("L") + `L${toX(pts - 1).toFixed(1)},${zeroY.toFixed(1)}L${toX(0).toFixed(1)},${zeroY.toFixed(1)}Z`;

    // MELHORIA 1: Strike lines with position info
    const positionStrikes = optLegs.map(p => ({
      strike: p.strike,
      optType: p.optType,
      qty: p.qty,
    })).filter(s => s.strike >= lo && s.strike <= hi);

    // Group by strike
    const strikeMap: Record<number, { optType: string; qty: number }[]> = {};
    positionStrikes.forEach(s => {
      if (!strikeMap[s.strike]) strikeMap[s.strike] = [];
      const existing = strikeMap[s.strike].find(e => e.optType === s.optType);
      if (existing) existing.qty += s.qty;
      else strikeMap[s.strike].push({ optType: s.optType, qty: s.qty });
    });

    // X axis labels
    const xLabels = [0, Math.floor(pts / 4), Math.floor(pts / 2), Math.floor(3 * pts / 4), pts - 1];

    // MELHORIA 1: Tooltip handler
    const handlePayoffMouseMove = (e: React.MouseEvent<SVGSVGElement>) => {
      const rect = e.currentTarget.getBoundingClientRect();
      const mouseX = e.clientX - rect.left;
      const mouseY = e.clientY - rect.top;
      // Convert mouseX to price index
      const plotX = mouseX - padL;
      if (plotX < 0 || plotX > plotW) { setPayoffTooltip(null); return; }
      const idx = Math.round((plotX / plotW) * (pts - 1));
      const clampedIdx = Math.max(0, Math.min(pts - 1, idx));
      setPayoffTooltip({
        x: mouseX,
        y: mouseY,
        price: priceRange[clampedIdx],
        plExp: payoff[clampedIdx],
        plToday: payoffNow[clampedIdx],
      });
    };

    // ── Gaussian ──  (MELHORIA 2: estilo ThinkorSwim)
    const meanPL = payoff.reduce((a, b) => a + b, 0) / payoff.length;
    const stdPL = Math.sqrt(payoff.reduce((s, v) => s + (v - meanPL) ** 2, 0) / payoff.length) || 1;
    const gsvgW = 300, gsvgH = 280, gPadL = 10, gPadR = 10, gPadT = 20, gPadB = 35;
    const gPlotW = gsvgW - gPadL - gPadR, gPlotH = gsvgH - gPadT - gPadB;
    const gLo = meanPL - 3.5 * stdPL, gHi = meanPL + 3.5 * stdPL;
    const gPts = 120;
    const gRange = Array.from({ length: gPts }, (_, i) => gLo + (gHi - gLo) * i / (gPts - 1));
    const gValues = gRange.map(x => gaussianY(x, meanPL, stdPL));
    const gMax = Math.max(...gValues);
    const gToX = (i: number) => gPadL + (i / (gPts - 1)) * gPlotW;
    const gToY = (v: number) => gPadT + gPlotH - (v / gMax) * gPlotH;
    const gValToIdx = (val: number) => Math.round(((val - gLo) / (gHi - gLo)) * (gPts - 1));

    // VaR 95% and 99%
    const sortedPayoff = [...payoff].sort((a, b) => a - b);
    const var95 = sortedPayoff[Math.floor(sortedPayoff.length * 0.05)];
    const var99 = sortedPayoff[Math.floor(sortedPayoff.length * 0.01)];
    const probProfit = payoff.filter(v => v >= 0).length / payoff.length;
    const expectedPL = meanPL;

    // Theta 30 days
    const thetaDay = optLegs.reduce((s, p) => s + ((p.theta || 0) * Math.abs(p.qty)), 0);
    const theta30 = thetaDay * 30;

    // Sigma band indices
    const sigma1Lo = gValToIdx(meanPL - stdPL);
    const sigma1Hi = gValToIdx(meanPL + stdPL);
    const sigma2Lo = gValToIdx(meanPL - 2 * stdPL);
    const sigma2Hi = gValToIdx(meanPL + 2 * stdPL);
    const zeroIdx = gValToIdx(0);
    const zeroGX = gToX(Math.max(0, Math.min(gPts - 1, zeroIdx)));

    // Build area path helper
    const buildGaussArea = (startIdx: number, endIdx: number, color: string) => {
      const s = Math.max(0, Math.min(gPts - 1, startIdx));
      const e = Math.max(0, Math.min(gPts - 1, endIdx));
      if (s >= e) return null;
      const pts2: string[] = [];
      for (let i = s; i <= e; i++) {
        pts2.push(`${gToX(i).toFixed(1)},${gToY(gValues[i]).toFixed(1)}`);
      }
      const path = `M${gToX(s).toFixed(1)},${gToY(0).toFixed(1)}L${pts2.join("L")}L${gToX(e).toFixed(1)},${gToY(0).toFixed(1)}Z`;
      return <path d={path} fill={color} />;
    };

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
          {/* LEFT: Payoff Diagram — MELHORIA 1 */}
          <div style={{ background: C.panel, borderRadius: 8, border: `1px solid ${C.border}`, padding: 12, position: "relative" }}>
            <div style={{ fontSize: 10, color: C.dim, marginBottom: 6, fontWeight: 600 }}>PAYOFF DIAGRAM</div>
            <svg
              width={svgW}
              height={svgH}
              style={{ display: "block", cursor: "crosshair" }}
              onMouseMove={handlePayoffMouseMove}
              onMouseLeave={() => setPayoffTooltip(null)}
            >
              {/* Area fills: green (profit) and red (loss) */}
              <path d={areaGreen} fill="#00C87820" />
              <path d={areaRed} fill="#DC3C3C20" />

              {/* Breakeven horizontal line (y=0) */}
              <line x1={padL} y1={zeroY} x2={svgW - padR} y2={zeroY} stroke="#475569" strokeWidth={1} />
              <text x={padL + 4} y={zeroY - 5} fill="#475569" fontSize={7} fontWeight={600}>BREAKEVEN</text>

              {/* LUCRO / PERDA labels */}
              <text x={svgW - padR - 4} y={padT + 14} fill={C.green} fontSize={8} textAnchor="end" opacity={0.6}>LUCRO &#9650;</text>
              <text x={svgW - padR - 4} y={svgH - padB - 6} fill={C.red} fontSize={8} textAnchor="end" opacity={0.6}>PERDA &#9660;</text>

              {/* Spot line */}
              <line x1={spotX} y1={padT} x2={spotX} y2={svgH - padB} stroke={C.gold} strokeWidth={1} strokeDasharray="4,3" />
              <text x={spotX} y={padT - 4} fill={C.gold} fontSize={8} textAnchor="middle">Spot {spot.toFixed(1)}</text>

              {/* MELHORIA 1: Strike vertical lines with position labels */}
              {Object.entries(strikeMap).map(([k, legs]) => {
                const strikeVal = parseFloat(k);
                const sx = padL + ((strikeVal - lo) / (hi - lo)) * plotW;
                return legs.map((leg, li) => {
                  const isShort = leg.qty < 0;
                  const color = isShort ? C.red : C.green;
                  const label = `${leg.optType === "C" ? "C" : "P"}$${strikeVal.toFixed(0)} ${leg.qty > 0 ? "+" : ""}${leg.qty}`;
                  return (
                    <g key={`strike-${k}-${li}`}>
                      <line x1={sx} y1={padT} x2={sx} y2={svgH - padB} stroke={color} strokeWidth={0.8} strokeDasharray="3,4" opacity={0.7} />
                      <text x={sx} y={padT + 12 + li * 10} fill={color} fontSize={7} textAnchor="middle" fontFamily="monospace" fontWeight={600}>{label}</text>
                    </g>
                  );
                });
              })}

              {/* Breakeven vertical lines */}
              {breakevenPoints.map((bp, i) => {
                const bx = padL + ((bp - lo) / (hi - lo)) * plotW;
                return (
                  <g key={"be" + i}>
                    <line x1={bx} y1={padT} x2={bx} y2={svgH - padB} stroke="rgba(220,180,50,.5)" strokeWidth={1} strokeDasharray="3,3" />
                    <text x={bx} y={svgH - padB + 12} fill={C.gold} fontSize={7} textAnchor="middle">BE {bp.toFixed(1)}</text>
                  </g>
                );
              })}

              {/* P&L now (dashed blue) */}
              <path d={pathNow} fill="none" stroke={C.blue} strokeWidth={1.5} strokeDasharray="4,3" opacity={0.7} />

              {/* P&L at expiry (solid white) */}
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

              {/* Tooltip crosshair */}
              {payoffTooltip && (
                <>
                  <line x1={payoffTooltip.x} y1={padT} x2={payoffTooltip.x} y2={svgH - padB} stroke="rgba(255,255,255,.2)" strokeWidth={0.5} />
                  <line x1={padL} y1={payoffTooltip.y} x2={svgW - padR} y2={payoffTooltip.y} stroke="rgba(255,255,255,.2)" strokeWidth={0.5} />
                </>
              )}

              {/* legend */}
              <line x1={padL + 10} y1={padT + 6} x2={padL + 30} y2={padT + 6} stroke={C.text} strokeWidth={2} />
              <text x={padL + 34} y={padT + 9} fill={C.muted} fontSize={8}>Vencimento</text>
              <line x1={padL + 110} y1={padT + 6} x2={padL + 130} y2={padT + 6} stroke={C.blue} strokeWidth={1.5} strokeDasharray="4,3" />
              <text x={padL + 134} y={padT + 9} fill={C.muted} fontSize={8}>Hoje (delta)</text>
            </svg>

            {/* MELHORIA 1: Tooltip overlay */}
            {payoffTooltip && (
              <div style={{
                position: "absolute",
                left: payoffTooltip.x + 20,
                top: payoffTooltip.y - 10,
                pointerEvents: "none",
                background: "#142332",
                border: "1px solid #1e3a4a",
                borderRadius: 6,
                padding: "6px 10px",
                fontSize: 11,
                color: "#e2e8f0",
                zIndex: 10,
                whiteSpace: "nowrap",
              }}>
                <div>Preco: <span style={{ fontFamily: "monospace", fontWeight: 600 }}>${payoffTooltip.price.toFixed(2)}</span></div>
                <div>Vencimento: <span style={{ fontFamily: "monospace", fontWeight: 600, color: plColor(payoffTooltip.plExp) }}>
                  {payoffTooltip.plExp > 0 ? "+" : ""}{fmtUsd(payoffTooltip.plExp, 0)}
                </span></div>
                <div>Hoje: <span style={{ fontFamily: "monospace", fontWeight: 600, color: plColor(payoffTooltip.plToday) }}>
                  {payoffTooltip.plToday > 0 ? "+" : ""}{fmtUsd(payoffTooltip.plToday, 0)}
                </span></div>
              </div>
            )}
          </div>

          {/* RIGHT: Gaussian Distribution — MELHORIA 2: estilo ThinkorSwim */}
          <div style={{ background: C.panel, borderRadius: 8, border: `1px solid ${C.border}`, padding: 12 }}>
            <div style={{ fontSize: 10, color: C.dim, marginBottom: 6, fontWeight: 600 }}>DISTRIBUICAO P&L</div>
            <svg width={gsvgW} height={gsvgH} style={{ display: "block" }}>
              {/* Background full area */}
              {buildGaussArea(0, gPts - 1, "#1e3a4a20")}

              {/* Sigma bands: +/- 2 sigma (95%) — orange */}
              {buildGaussArea(Math.max(0, sigma2Lo), Math.min(gPts - 1, sigma2Hi), "rgba(220,140,50,.08)")}

              {/* Sigma bands: +/- 1 sigma (68%) — yellow */}
              {buildGaussArea(Math.max(0, sigma1Lo), Math.min(gPts - 1, sigma1Hi), "#DCB43215")}

              {/* Red area (loss: < breakeven) */}
              {zeroIdx > 0 && buildGaussArea(0, Math.min(gPts - 1, zeroIdx), "#DC3C3C25")}

              {/* Green area (profit: > breakeven) */}
              {zeroIdx < gPts - 1 && buildGaussArea(Math.max(0, zeroIdx), gPts - 1, "#00C87825")}

              {/* Curve */}
              <path d={"M" + gValues.map((v, i) => `${gToX(i).toFixed(1)},${gToY(v).toFixed(1)}`).join("L")} fill="none" stroke={C.muted} strokeWidth={1.5} />

              {/* Break Even line */}
              {zeroIdx >= 0 && zeroIdx < gPts && (
                <g>
                  <line x1={zeroGX} y1={gPadT} x2={zeroGX} y2={gsvgH - gPadB} stroke="#ffffff" strokeWidth={1} opacity={0.6} />
                  <text x={zeroGX + 3} y={gPadT + 10} fill="#ffffff" fontSize={7} fontWeight={600}>Break Even $0</text>
                </g>
              )}

              {/* VaR 95% line */}
              {(() => {
                const vIdx = gValToIdx(var95);
                const vx = gToX(Math.max(0, Math.min(gPts - 1, vIdx)));
                if (vx >= gPadL && vx <= gsvgW - gPadR) {
                  return (
                    <g>
                      <line x1={vx} y1={gPadT} x2={vx} y2={gsvgH - gPadB} stroke={C.gold} strokeWidth={1} strokeDasharray="3,3" />
                      <text x={vx - 3} y={gPadT + 22} fill={C.gold} fontSize={7} textAnchor="end" fontWeight={600}>VaR 95% {fmtUsd(var95, 0)}</text>
                    </g>
                  );
                }
                return null;
              })()}

              {/* VaR 99% line */}
              {(() => {
                const vIdx = gValToIdx(var99);
                const vx = gToX(Math.max(0, Math.min(gPts - 1, vIdx)));
                if (vx >= gPadL && vx <= gsvgW - gPadR) {
                  return (
                    <g>
                      <line x1={vx} y1={gPadT} x2={vx} y2={gsvgH - gPadB} stroke={C.red} strokeWidth={1} strokeDasharray="3,3" />
                      <text x={vx - 3} y={gPadT + 34} fill={C.red} fontSize={7} textAnchor="end" fontWeight={600}>VaR 99% {fmtUsd(var99, 0)}</text>
                    </g>
                  );
                }
                return null;
              })()}

              {/* Theta 30d line */}
              {(() => {
                const tIdx = gValToIdx(theta30);
                const tx = gToX(Math.max(0, Math.min(gPts - 1, tIdx)));
                if (tx >= gPadL && tx <= gsvgW - gPadR && theta30 !== 0) {
                  return (
                    <g>
                      <line x1={tx} y1={gPadT} x2={tx} y2={gsvgH - gPadB} stroke={C.gold} strokeWidth={1} strokeDasharray="5,3" />
                      <text x={tx + 3} y={gPadT + 46} fill={C.gold} fontSize={7} fontWeight={600}>Theta 30d {fmtUsd(theta30, 0)}</text>
                    </g>
                  );
                }
                return null;
              })()}

              {/* X axis in USD */}
              {[0, Math.floor(gPts / 4), Math.floor(gPts / 2), Math.floor(3 * gPts / 4), gPts - 1].map(i => (
                <text key={i} x={gToX(i)} y={gsvgH - gPadB + 14} fill={C.dim} fontSize={7} textAnchor="middle" fontFamily="monospace">
                  {fmtUsdCompact(gRange[i])}
                </text>
              ))}
            </svg>

            {/* MELHORIA 2: Statistics grid 2x3 */}
            <div style={{
              display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 1,
              background: C.border, borderRadius: 6, overflow: "hidden", marginTop: 8,
            }}>
              {[
                { label: "Prob. Lucro", value: `${(probProfit * 100).toFixed(0)}%`, color: C.green },
                { label: "Prob. Perda", value: `${((1 - probProfit) * 100).toFixed(0)}%`, color: C.red },
                { label: "Theta 30 dias", value: `${theta30 >= 0 ? "+" : ""}${fmtUsd(theta30, 0)}`, color: theta30 >= 0 ? C.green : C.red },
                { label: "P&L Esperado", value: `${expectedPL >= 0 ? "+" : ""}${fmtUsd(expectedPL, 0)}`, color: expectedPL >= 0 ? C.green : C.red },
                { label: "VaR 95%", value: fmtUsd(var95, 0), color: C.red },
                { label: "VaR 99%", value: fmtUsd(var99, 0), color: C.red },
              ].map((stat, i) => (
                <div key={i} style={{ padding: "8px 10px", background: C.panel2, textAlign: "center" }}>
                  <div style={{ fontSize: 8, color: C.dim, textTransform: "uppercase", letterSpacing: 0.5, marginBottom: 4 }}>{stat.label}</div>
                  <div style={{ fontSize: 13, fontWeight: 700, color: stat.color, fontFamily: "monospace" }}>{stat.value}</div>
                </div>
              ))}
            </div>
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
