"use client";
import { useState } from "react";

const C = {
  bg:      '#0E1A24',
  panel:   '#142332',
  panel2:  '#0a1520',
  border:  '#1e3a4a',
  green:   '#00C878',
  red:     '#DC3C3C',
  gold:    '#DCB432',
  blue:    '#468CDC',
  text:    '#e2e8f0',
  muted:   '#94a3b8',
  dim:     '#64748b',
};

// Mapeamento de spreads relevantes por commodity
const SPREAD_RELEVANCE: Record<string, string[]> = {
  ZS: ['soy_crush', 'zc_zs'],
  ZC: ['soy_crush', 'zc_zs', 'zc_zm', 'new_crop_zc', 'feedlot'],
  ZM: ['soy_crush', 'zc_zm'],
  ZL: ['zl_cl'],
  ZW: ['ke_zw', 'feed_wheat'],
  KE: ['ke_zw', 'feed_wheat'],
  LE: ['feedlot', 'cattle_crush'],
  GF: ['feedlot', 'cattle_crush'],
  HE: ['feedlot'],
  CL: ['zl_cl'],
  SB: [],
  KC: [],
  CT: [],
  CC: [],
};

// Mapeamento de paridades relevantes por commodity
// Chaves reais do parities.json:
// zc_zs, zc_zw, cl_zl_biodiesel, dxy_grains, brl_competitiveness,
// zm_zs_ratio, gasoline_ethanol, ethanol_sugar_brazil, ng_urea_lag,
// le_zc_ratio, gf_le_spread, he_zc_ratio
const PARITY_RELEVANCE: Record<string, string[]> = {
  ZS: ['zc_zs', 'zm_zs_ratio', 'brl_competitiveness'],
  ZC: ['zc_zs', 'zc_zw', 'le_zc_ratio', 'he_zc_ratio', 'ng_urea_lag'],
  ZM: ['zm_zs_ratio'],
  ZL: ['cl_zl_biodiesel'],
  ZW: ['zc_zw', 'dxy_grains'],
  KE: ['zc_zw'],
  CL: ['cl_zl_biodiesel', 'gasoline_ethanol'],
  LE: ['le_zc_ratio', 'gf_le_spread'],
  GF: ['gf_le_spread', 'le_zc_ratio'],
  HE: ['he_zc_ratio'],
  SB: ['ethanol_sugar_brazil'],
  NG: ['ng_urea_lag', 'gasoline_ethanol'],
  DX: ['dxy_grains', 'brl_competitiveness'],
};

// Nomes das commodities
const COMM_NAMES: Record<string, string> = {
  ZC:'Corn', ZS:'Soybeans', ZW:'Wheat CBOT', KE:'Wheat KC',
  ZM:'Soybean Meal', ZL:'Soybean Oil', SB:'Sugar #11',
  KC:'Coffee C', CT:'Cotton #2', CC:'Cocoa', OJ:'Orange Juice',
  LE:'Live Cattle', GF:'Feeder Cattle', HE:'Lean Hogs',
  CL:'Crude Oil', NG:'Natural Gas', GC:'Gold', SI:'Silver', DX:'Dollar Index',
};

export default function CommodityTab({ selected, prices, psdData, cot, season, spreads, physicalBr, parities, stocks }: {
  selected: string;
  prices: any;
  psdData: any;
  cot: any;
  season: any;
  spreads: any;
  physicalBr: any;
  parities: any;
  stocks: any;
}) {
  const name = COMM_NAMES[selected] || selected;

  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({});
  const toggle = (key: string) => setCollapsed(p => ({ ...p, [key]: !p[key] }));

  // Header de seção colapsável
  const SectionHeader = ({ id, emoji, title, badge }: { id: string; emoji: string; title: string; badge?: string }) => (
    <div
      onClick={() => toggle(id)}
      style={{
        display: 'flex', alignItems: 'center', gap: 8,
        padding: '10px 16px',
        background: C.panel2,
        borderBottom: `1px solid ${C.border}`,
        cursor: 'pointer',
        userSelect: 'none',
      }}
    >
      <span style={{ fontSize: 14 }}>{emoji}</span>
      <span style={{
        fontSize: 11, fontWeight: 700, color: C.text,
        textTransform: 'uppercase', letterSpacing: '0.08em', flex: 1,
      }}>
        {title}
      </span>
      {badge && (
        <span style={{
          fontSize: 9, fontWeight: 700, color: C.gold,
          background: C.gold + '22', padding: '2px 6px',
          borderRadius: 4,
        }}>
          {badge}
        </span>
      )}
      <span style={{
        fontSize: 10, color: C.dim,
        transform: collapsed[id] ? 'rotate(-90deg)' : 'rotate(0deg)',
        transition: 'transform 0.2s',
      }}>{"▾"}</span>
    </div>
  );

  // Seção wrapper
  const Section = ({ id, emoji, title, badge, children }: { id: string; emoji: string; title: string; badge?: string; children: React.ReactNode }) => (
    <div style={{
      border: `1px solid ${C.border}`,
      borderRadius: 8, overflow: 'hidden',
      marginBottom: 12,
    }}>
      <SectionHeader id={id} emoji={emoji} title={title} badge={badge} />
      {!collapsed[id] && (
        <div style={{ padding: '14px 16px', background: C.panel }}>
          {children}
        </div>
      )}
    </div>
  );

  // SEÇÃO 1: Preço + mini gráfico 7d
  const PriceSection = () => {
    const barsRaw = prices?.[selected];
    const bars = Array.isArray(barsRaw) ? barsRaw : barsRaw?.bars || [];
    const last7 = bars.slice(-7);
    const current = bars[bars.length - 1]?.close;
    const prev = bars[bars.length - 2]?.close;
    const change = current && prev ? ((current - prev) / prev * 100) : 0;
    const change1M = bars.length >= 22 && bars[bars.length - 22]?.close
      ? ((current - bars[bars.length - 22].close) / bars[bars.length - 22].close * 100) : null;
    const recent260 = bars.slice(-260);
    const high52 = recent260.length ? Math.max(...recent260.map((b: any) => b.high || b.close)) : 0;
    const low52 = recent260.length ? Math.min(...recent260.map((b: any) => b.low || b.close)) : 0;

    const sparkW = 200, sparkH = 50;
    const vals = last7.map((b: any) => b.close).filter((v: number) => v != null);
    const minV = vals.length ? Math.min(...vals) : 0;
    const maxV = vals.length ? Math.max(...vals) : 1;
    const range = maxV - minV || 1;
    const pts = vals.map((v: number, i: number) => {
      const x = vals.length > 1 ? (i / (vals.length - 1)) * sparkW : sparkW / 2;
      const y = sparkH - ((v - minV) / range * sparkH);
      return `${x},${y}`;
    }).join(' ');
    const uptrend = vals.length >= 2 && vals[vals.length - 1] >= vals[0];

    return (
      <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: 16, alignItems: 'center' }}>
        <div>
          <div style={{
            fontSize: 32, fontWeight: 700, fontFamily: 'monospace',
            color: change >= 0 ? C.green : C.red,
          }}>
            {current?.toFixed(2) || '—'}
          </div>
          <div style={{ display: 'flex', gap: 12, marginTop: 4 }}>
            <span style={{
              fontSize: 12, fontFamily: 'monospace',
              color: change >= 0 ? C.green : C.red,
            }}>
              {change >= 0 ? '+' : ''}{change.toFixed(2)}% (1D)
            </span>
            {change1M !== null && (
              <span style={{
                fontSize: 12, fontFamily: 'monospace',
                color: change1M >= 0 ? C.green : C.red,
              }}>
                {change1M >= 0 ? '+' : ''}{change1M.toFixed(2)}% (1M)
              </span>
            )}
          </div>
          <div style={{
            display: 'flex', gap: 16, marginTop: 8,
            fontSize: 10, color: C.dim, fontFamily: 'monospace',
          }}>
            <span>52W H: <span style={{ color: C.text }}>{high52.toFixed(2)}</span></span>
            <span>52W L: <span style={{ color: C.text }}>{low52.toFixed(2)}</span></span>
          </div>
        </div>
        {vals.length >= 2 && (
          <svg width={sparkW} height={sparkH}>
            <polyline points={pts} fill="none" stroke={uptrend ? C.green : C.red} strokeWidth={2} />
          </svg>
        )}
      </div>
    );
  };

  // SEÇÃO 2: Estoques PSD
  const EstoquesSection = () => {
    const psd = psdData?.commodities?.[selected];
    const sw = stocks?.commodities?.[selected];
    if (!psd && !sw) return (
      <div style={{ color: C.dim, fontSize: 11 }}>
        Sem dados de estoque disponíveis para {selected}
      </div>
    );

    const current = psd?.current ?? sw?.stock_current ?? sw?.price;
    const avg = psd?.avg_5y ?? sw?.stock_avg ?? sw?.avg_5y;
    const dev = psd?.deviation ?? (sw?.price_vs_avg != null ? sw.price_vs_avg : undefined);
    const unit = psd?.unit ?? sw?.stock_unit ?? '';
    const devColor = dev == null ? C.dim : dev > 20 ? C.green : dev < -10 ? C.red : C.gold;

    return (
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 12 }}>
        {[
          { label: 'ATUAL', value: current != null ? Number(current).toLocaleString('en') : '—', unit },
          { label: 'MÉDIA 5A', value: avg != null ? Number(avg).toLocaleString('en') : '—', unit },
          { label: 'DESVIO', value: dev != null ? `${dev > 0 ? '+' : ''}${Number(dev).toFixed(1)}%` : '—', color: devColor },
        ].map((item, i) => (
          <div key={i} style={{
            padding: 12, background: C.panel2, borderRadius: 6,
            borderLeft: `3px solid ${item.color || C.border}`,
          }}>
            <div style={{ fontSize: 9, color: C.dim, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>
              {item.label}
            </div>
            <div style={{ fontSize: 18, fontWeight: 700, fontFamily: 'monospace', color: item.color || C.text }}>
              {item.value}
            </div>
            {item.unit && (
              <div style={{ fontSize: 9, color: C.dim, marginTop: 2 }}>{item.unit}</div>
            )}
          </div>
        ))}
      </div>
    );
  };

  // SEÇÃO 3: COT Index + Delta
  const COTSection = () => {
    const comm = cot?.commodities?.[selected];
    const dis = comm?.disaggregated;
    const leg = comm?.legacy;
    const da = dis?.delta_analysis;
    const cotIdx = da?.cot_index;
    const mmNet = dis?.latest?.managed_money_net ?? dis?.latest?.mm_net ?? leg?.latest?.noncomm_net;
    const cotColor = cotIdx == null ? C.dim : cotIdx >= 85 ? C.red : cotIdx <= 15 ? C.green :
                     cotIdx >= 70 ? C.gold : cotIdx <= 30 ? C.blue : C.dim;

    if (!comm) return <div style={{ color: C.dim, fontSize: 11 }}>Sem dados COT para {selected}</div>;

    return (
      <div>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 12, marginBottom: 12 }}>
          {[
            { label: 'COT INDEX', value: cotIdx != null ? `${cotIdx.toFixed(1)}` : '—',
              color: cotColor,
              sub: cotIdx == null ? '' : cotIdx >= 85 ? 'EXTREMO COMPRADO' : cotIdx <= 15 ? 'EXTREMO VENDIDO' :
                   cotIdx >= 70 ? 'ALTO' : cotIdx <= 30 ? 'BAIXO' : 'NEUTRO' },
            { label: 'MM NET', value: mmNet != null ?
              `${mmNet > 0 ? '+' : ''}${(mmNet / 1000).toFixed(0)}K` : '—',
              color: mmNet != null ? (mmNet > 0 ? C.green : C.red) : C.dim,
              sub: 'contratos' },
            { label: 'DELTA SEMANAL', value: da?.current_delta != null ?
              `${da.current_delta > 0 ? '+' : ''}${(da.current_delta / 1000).toFixed(0)}K` : '—',
              color: da?.current_delta != null ? (da.current_delta > 0 ? C.green : C.red) : C.dim,
              sub: da?.dominant_direction || '—' },
          ].map((item, i) => (
            <div key={i} style={{
              padding: 12, background: C.panel2, borderRadius: 6,
              borderLeft: `3px solid ${item.color}`,
            }}>
              <div style={{ fontSize: 9, color: C.dim, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>
                {item.label}
              </div>
              <div style={{ fontSize: 18, fontWeight: 700, fontFamily: 'monospace', color: item.color }}>
                {item.value}
              </div>
              <div style={{ fontSize: 9, color: item.color, marginTop: 2, fontWeight: 600 }}>
                {item.sub}
              </div>
            </div>
          ))}
        </div>
        {cotIdx != null && (
          <div>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 8, color: C.dim, marginBottom: 3 }}>
              <span>0 — Extremo Vendido</span>
              <span>100 — Extremo Comprado</span>
            </div>
            <div style={{ height: 6, background: '#1e3a4a', borderRadius: 3, position: 'relative' }}>
              <div style={{
                position: 'absolute', left: 0, top: 0, height: '100%',
                width: `${cotIdx}%`,
                background: cotColor, borderRadius: 3,
                transition: 'width 0.5s',
              }} />
            </div>
          </div>
        )}
      </div>
    );
  };

  // SEÇÃO 4: Sazonalidade do mês
  const SazonalidadeSection = () => {
    const s = season?.[selected];
    if (!s) return <div style={{ color: C.dim, fontSize: 11 }}>Sem dados de sazonalidade</div>;

    const monthLabels = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'];
    const curMonth = new Date().getMonth();
    const series = s.series || {};
    const currentSeries = series.current || [];
    const avgSeries = series.average || [];

    // Try to extract monthly returns from the season data
    const years = Object.keys(series).filter(y => y !== 'current' && y !== 'average');
    let monthlyAvgs: number[] = [];

    if (years.length >= 3) {
      for (let m = 0; m < 12; m++) {
        const rets: number[] = [];
        years.forEach(y => {
          const pts = series[y] || [];
          // Find points in this month
          const monthPts = pts.filter((p: any) => {
            if (!p.date) return false;
            const d = new Date(p.date);
            return d.getMonth() === m;
          });
          if (monthPts.length >= 2) {
            const first = monthPts[0].close;
            const last = monthPts[monthPts.length - 1].close;
            if (first > 0) rets.push((last - first) / first * 100);
          }
        });
        monthlyAvgs.push(rets.length ? rets.reduce((a, b) => a + b, 0) / rets.length : 0);
      }
    }

    if (monthlyAvgs.every(v => v === 0)) {
      return <div style={{ color: C.dim, fontSize: 11 }}>Dados sazonais disponíveis apenas na aba Sazonalidade</div>;
    }

    const maxAbs = Math.max(...monthlyAvgs.map(v => Math.abs(v)), 0.1);

    return (
      <div>
        <div style={{ display: 'flex', gap: 3, marginBottom: 12 }}>
          {monthLabels.map((m, i) => {
            const avg = monthlyAvgs[i] || 0;
            const h = Math.abs(avg) / maxAbs * 40;
            const isNow = i === curMonth;
            return (
              <div key={i} style={{ flex: 1, textAlign: 'center' }}>
                <div style={{ height: 50, display: 'flex', alignItems: 'flex-end', justifyContent: 'center' }}>
                  <div style={{
                    width: '100%', height: `${h}px`,
                    background: avg >= 0 ? C.green : C.red,
                    opacity: isNow ? 1 : 0.5,
                    border: isNow ? `1px solid ${C.gold}` : 'none',
                    borderRadius: 2,
                  }} />
                </div>
                <div style={{ fontSize: 8, color: isNow ? C.gold : C.dim, fontWeight: isNow ? 700 : 400, marginTop: 2 }}>
                  {m}
                </div>
                <div style={{ fontSize: 8, fontFamily: 'monospace', color: avg >= 0 ? C.green : C.red, fontWeight: isNow ? 700 : 400 }}>
                  {avg > 0 ? '+' : ''}{avg.toFixed(1)}%
                </div>
              </div>
            );
          })}
        </div>
        <div style={{
          padding: '8px 12px', background: C.panel2, borderRadius: 6,
          borderLeft: `3px solid ${C.gold}`, fontSize: 11, color: C.text,
        }}>
          <strong style={{ color: C.gold }}>{monthLabels[curMonth]}:</strong>{' '}
          Retorno médio histórico {monthlyAvgs[curMonth] > 0 ? '+' : ''}{monthlyAvgs[curMonth].toFixed(1)}%
        </div>
      </div>
    );
  };

  // SEÇÃO 5: Spreads relevantes
  const SpreadsSection = () => {
    const relevant = SPREAD_RELEVANCE[selected] || [];
    const allSpreads = spreads?.spreads || {};
    const items = relevant.map(k => allSpreads[k]).filter(Boolean);

    if (!items.length) return (
      <div style={{ color: C.dim, fontSize: 11 }}>
        Sem spreads diretamente relacionados a {selected}
      </div>
    );

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        {items.map((sp: any, i: number) => {
          const z = sp.zscore_1y ?? sp.zscore ?? 0;
          const zColor = Math.abs(z) >= 2 ? C.red : Math.abs(z) >= 1 ? C.gold : C.dim;
          return (
            <div key={i} style={{
              padding: '10px 14px', background: C.panel2,
              borderRadius: 6, borderLeft: `3px solid ${zColor}`,
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={{ flex: 1 }}>
                  <div style={{ fontSize: 11, fontWeight: 700, color: C.text }}>{sp.name}</div>
                  <div style={{ fontSize: 10, color: C.dim, marginTop: 2 }}>
                    {(sp.interpretation || sp.description || '').slice(0, 80)}{(sp.interpretation || sp.description || '').length > 80 ? '...' : ''}
                  </div>
                </div>
                <div style={{ textAlign: 'right', flexShrink: 0, marginLeft: 12 }}>
                  <div style={{ fontSize: 16, fontWeight: 700, fontFamily: 'monospace', color: zColor }}>
                    {typeof sp.current === 'number' ? sp.current.toFixed(2) : sp.current}
                  </div>
                  <div style={{ fontSize: 9, color: zColor, fontWeight: 700 }}>
                    z={z > 0 ? '+' : ''}{z.toFixed(2)}
                  </div>
                </div>
              </div>
              {sp.signal_now && (
                <div style={{
                  marginTop: 6, fontSize: 10, color: C.text,
                  padding: '6px 8px', background: zColor + '11',
                  borderRadius: 4,
                }}>
                  {sp.signal_now}
                </div>
              )}
            </div>
          );
        })}
      </div>
    );
  };

  // SEÇÃO 6: Físico BR (CEPEA)
  const FisicoBRSection = () => {
    // physical_br.json uses keys like ZS_BR, ZC_BR, LE_BR
    const key = `${selected === 'GF' ? 'LE' : selected}_BR`;
    const products = physicalBr?.products || physicalBr?.data || physicalBr || {};
    const data = products[key];

    if (!data) return (
      <div style={{ color: C.dim, fontSize: 11 }}>
        Sem dados físico BR disponíveis para {selected}
      </div>
    );

    const price = data.price ?? data.preco ?? data.valor;
    const unit = data.unit ?? data.unidade ?? '';
    const local = data.location ?? data.local ?? data.label ?? '';
    const date = data.date ?? data.data ?? '';
    const changePct = data.change_pct;

    return (
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2,1fr)', gap: 12 }}>
        <div style={{
          padding: 12, background: C.panel2, borderRadius: 6,
          borderLeft: `3px solid ${C.gold}`,
        }}>
          <div style={{ fontSize: 9, color: C.dim, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>
            PREÇO FÍSICO BR
          </div>
          <div style={{ fontSize: 22, fontWeight: 700, fontFamily: 'monospace', color: C.gold }}>
            {typeof price === 'number' ? `R$ ${price.toFixed(2)}` : price || '—'}
          </div>
          <div style={{ fontSize: 9, color: C.dim, marginTop: 2 }}>
            {unit}{local ? ` • ${local}` : ''}
          </div>
          {changePct != null && (
            <div style={{ fontSize: 10, fontFamily: 'monospace', color: changePct >= 0 ? C.green : C.red, marginTop: 4 }}>
              {changePct >= 0 ? '+' : ''}{changePct}% d/d
            </div>
          )}
        </div>
        <div style={{
          padding: 12, background: C.panel2, borderRadius: 6,
          borderLeft: `3px solid ${C.border}`,
        }}>
          <div style={{ fontSize: 9, color: C.dim, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: 4 }}>
            REFERÊNCIA
          </div>
          <div style={{ fontSize: 11, color: C.text, lineHeight: 1.5 }}>
            CEPEA/Esalq
          </div>
          <div style={{ fontSize: 9, color: C.dim, marginTop: 4 }}>
            Atualizado: {date || 'N/A'}
          </div>
        </div>
      </div>
    );
  };

  // SEÇÃO 7: Paridades relacionadas
  const ParidadesSection = () => {
    const relevant = PARITY_RELEVANCE[selected] || [];
    const allParities = parities?.parities || {};
    const items = relevant.map(k => ({ key: k, ...allParities[k] })).filter((p: any) => p.name);

    if (!items.length) return (
      <div style={{ color: C.dim, fontSize: 11 }}>
        Sem paridades diretamente relacionadas a {selected}
      </div>
    );

    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        {items.map((par: any, i: number) => {
          const signalColor = par.signal_color || C.dim;
          return (
            <div key={i} style={{
              padding: '10px 14px', background: C.panel2,
              borderRadius: 6, borderLeft: `3px solid ${signalColor}`,
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <div style={{ flex: 1 }}>
                  <div style={{ fontSize: 11, fontWeight: 700, color: C.text }}>{par.name}</div>
                  <div style={{ fontSize: 10, color: C.dim, marginTop: 2 }}>
                    {(par.description || '').slice(0, 70)}{(par.description || '').length > 70 ? '...' : ''}
                  </div>
                </div>
                <div style={{ textAlign: 'right', marginLeft: 12, flexShrink: 0 }}>
                  <div style={{ fontSize: 16, fontWeight: 700, fontFamily: 'monospace', color: signalColor }}>
                    {typeof par.value === 'number' ? par.value.toFixed(2) : par.value}
                  </div>
                  <div style={{ fontSize: 9, fontWeight: 700, color: signalColor, marginTop: 2 }}>
                    {(par.signal || '').slice(0, 25)}
                  </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    );
  };

  // RENDER PRINCIPAL
  const cotComm = cot?.commodities?.[selected];
  const cotIdx = cotComm?.disaggregated?.delta_analysis?.cot_index;
  const cotBadge = cotIdx == null ? undefined :
    cotIdx >= 85 ? 'EXTREMO ↑' : cotIdx <= 15 ? 'EXTREMO ↓' :
    cotIdx >= 70 ? `${cotIdx.toFixed(0)}/100` : cotIdx <= 30 ? `${cotIdx.toFixed(0)}/100` : undefined;

  const psd = psdData?.commodities?.[selected];
  const devBadge = psd?.deviation != null
    ? `${psd.deviation > 0 ? '+' : ''}${Number(psd.deviation).toFixed(1)}%`
    : undefined;

  const monthLabels = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'];
  const curMonthLabel = monthLabels[new Date().getMonth()];

  return (
    <div style={{ color: C.text, fontFamily: 'system-ui, sans-serif' }}>
      <div style={{
        marginBottom: 16, paddingBottom: 12,
        borderBottom: `1px solid ${C.border}`,
      }}>
        <div style={{ fontSize: 11, color: C.dim, textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 4 }}>
          VISÃO GERAL
        </div>
        <div style={{ fontSize: 20, fontWeight: 700, color: C.text }}>
          {selected} — {name}
        </div>
      </div>

      <Section id="price" emoji="📈" title="Preço & Performance">
        <PriceSection />
      </Section>

      <Section id="psd" emoji="📦" title="Estoques (USDA PSD)" badge={devBadge}>
        <EstoquesSection />
      </Section>

      <Section id="cot" emoji="📊" title="Posicionamento COT" badge={cotBadge}>
        <COTSection />
      </Section>

      <Section id="sazon" emoji="🗓" title="Sazonalidade" badge={curMonthLabel}>
        <SazonalidadeSection />
      </Section>

      <Section id="spreads" emoji="⚖️" title="Spreads Relevantes">
        <SpreadsSection />
      </Section>

      <Section id="fisico" emoji="🌍" title="Mercado Físico Brasil (CEPEA)">
        <FisicoBRSection />
      </Section>

      <Section id="paridades" emoji="🔗" title="Paridades Relacionadas">
        <ParidadesSection />
      </Section>
    </div>
  );
}
