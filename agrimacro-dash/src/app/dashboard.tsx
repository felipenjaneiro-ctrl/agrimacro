import BilateralPanel from "./BilateralPanel";
import LightweightChart from "./LightweightChart";
import GrainRatiosTab from "./GrainRatiosTab";
import LivestockRiskTab from "./LivestockRiskTab";
import CostOfProductionTab from "./CostOfProductionTab";
import SyncedChartPanel from "./SyncedChartPanel";
import CorrelationMap from "./CorrelationMap";
import CommodityTab from "./CommodityTab";
import PortfolioPage from "./PortfolioPage";
import { useState, useEffect, useRef } from "react";

/* ---------------------------------------------------------------------------
   AgriMacro v3.2 -- Dashboard Profissional de Commodities Agrícolas
   ZERO MOCK -- Somente dados reais via pipeline JSON
   --------------------------------------------------------------------------- */

function isStale(dateStr: string | undefined): boolean {
  if (!dateStr) return false;
  const today = new Date().toISOString().slice(0, 10);
  return dateStr < today;
}

function staleDays(dateStr: string | undefined): number {
  if (!dateStr) return 0;
  const today = new Date();
  const d = new Date(dateStr + "T12:00:00");
  return Math.round((today.getTime() - d.getTime()) / 86400000);
}

function gaussianY(x: number, mean: number, std: number): number {
  if (std === 0) return 0;
  return Math.exp(-0.5 * Math.pow((x - mean) / std, 2));
}

function cleanGrokContent(raw: string | undefined): string {
  if (!raw) return "";
  return raw
    .replace(/Continue reading[\s\S]*/i, "")
    .replace(/©\s*\d{4}\s*X\.?AI\s*LLC[\s\S]*/i, "")
    .replace(/Unsubscribe[\s\S]*/i, "")
    .replace(/^\s*\.{2,}\s*$/gm, "")
    .trimEnd();
}

function buildGaussianSVG(
  currentVal: number, mean: number, std: number,
  _label: string, unit: string, width = 320, height = 110
): string {
  if (std === 0) return "";
  const pad = { l: 40, r: 20, t: 20, b: 28 };
  const W = width - pad.l - pad.r;
  const H = height - pad.t - pad.b;
  const xMin = mean - 3.5 * std, xMax = mean + 3.5 * std;
  const steps = 120;
  const pts = Array.from({ length: steps + 1 }, (_, i) => {
    const x = xMin + (xMax - xMin) * i / steps;
    return { x, y: gaussianY(x, mean, std) };
  });
  const toSX = (x: number) => pad.l + ((x - xMin) / (xMax - xMin)) * W;
  const toSY = (y: number) => pad.t + H - y * H;
  const curvePath = pts.map((p, i) => `${i === 0 ? "M" : "L"}${toSX(p.x).toFixed(1)},${toSY(p.y).toFixed(1)}`).join(" ");
  const lo1 = mean - std, hi1 = mean + std;
  const fillPts = pts.filter(p => p.x >= lo1 && p.x <= hi1);
  const baseY = toSY(0);
  const fillPath = fillPts.length > 1
    ? `M${toSX(lo1).toFixed(1)},${baseY} ` + fillPts.map(p => `L${toSX(p.x).toFixed(1)},${toSY(p.y).toFixed(1)}`).join(" ") + ` L${toSX(hi1).toFixed(1)},${baseY} Z`
    : "";
  const cx = toSX(Math.max(xMin, Math.min(xMax, currentVal)));
  const cy = toSY(gaussianY(currentVal, mean, std));
  const zScore = (currentVal - mean) / std;
  const cc = Math.abs(zScore) > 2 ? "#DC3C3C" : Math.abs(zScore) > 1 ? "#DCB432" : "#00C878";
  const xLabels = [-2, -1, 0, 1, 2].map(n => ({ x: toSX(mean + n * std), label: n === 0 ? "μ" : `${n > 0 ? "+" : ""}${n}σ` }));
  const sigmaLines = [-2, -1, 1, 2].map(n => {
    const lx = toSX(mean + n * std);
    return `<line x1="${lx.toFixed(1)}" y1="${pad.t}" x2="${lx.toFixed(1)}" y2="${baseY}" stroke="#1e3a4a" stroke-width="1" stroke-dasharray="${Math.abs(n) === 1 ? "4,3" : "2,3"}"/>`;
  }).join("");
  const valTxt = unit.trim() ? currentVal.toFixed(2) + unit : currentVal.toFixed(1) + "%";
  const lblX = Math.min(Math.max(cx, pad.l + 30), pad.l + W - 30);
  return `<svg width="${width}" height="${height}" xmlns="http://www.w3.org/2000/svg">
<rect width="${width}" height="${height}" fill="#0E1A24" rx="6"/>
${fillPath ? `<path d="${fillPath}" fill="#00C878" opacity="0.12"/>` : ""}
<path d="${curvePath}" fill="none" stroke="#8899AA" stroke-width="1.5"/>
<line x1="${pad.l}" y1="${baseY}" x2="${pad.l + W}" y2="${baseY}" stroke="#1e3a4a" stroke-width="1"/>
${sigmaLines}
<line x1="${cx.toFixed(1)}" y1="${pad.t}" x2="${cx.toFixed(1)}" y2="${baseY}" stroke="${cc}" stroke-width="1.5" stroke-dasharray="3,2"/>
<circle cx="${cx.toFixed(1)}" cy="${cy.toFixed(1)}" r="4" fill="${cc}" stroke="#0E1A24" stroke-width="1.5"/>
${xLabels.map(l => `<text x="${l.x.toFixed(1)}" y="${(baseY + 16).toFixed(1)}" text-anchor="middle" font-size="8" fill="#64748b" font-family="monospace">${l.label}</text>`).join("")}
<text x="${pad.l + W}" y="${pad.t + 12}" text-anchor="end" font-size="9" fill="${cc}" font-family="monospace" font-weight="bold">z=${zScore.toFixed(2)}</text>
<text x="${lblX.toFixed(1)}" y="${(cy - 8).toFixed(1)}" text-anchor="middle" font-size="7.5" fill="${cc}" font-family="monospace">${valTxt}</text>
</svg>`;
}

// -- Types ------------------------------------------------------------------
interface OHLCV { date:string; open:number; high:number; low:number; close:number; volume:number; }
type PriceData = Record<string, OHLCV[]>;

interface SeasonPoint { day:number; close:number; date?:string; }
interface SeasonEntry { symbol:string; status:string; years:string[]; series:Record<string,SeasonPoint[]>; }
type SeasonData = Record<string, SeasonEntry>;

interface SpreadInfo {
  name:string; unit:string; current:number; zscore_1y:number; percentile:number;
  regime:string; points:number; description?:string; trend?:string; trend_pct?:number;
  mean_1y?:number; std_1y?:number;
  category?:string; interpretation?:string; signal_now?:string; watch_if?:string;
  history?:{date:string;value:number}[];
}
interface SpreadsData { timestamp:string; spreads:Record<string,SpreadInfo>; }

interface EIASeries {
  name:string; latest_period:string|null; latest_value:number|null;
  unit:string; wow_change_pct:number|null; mom_change_pct:number|null;
  yoy_change_pct:number|null; high_52w:number|null; low_52w:number|null;
  pct_range_52w:number|null;
  history:{period:string;value:number;unit:string}[];
}
interface EIAData {
  metadata:{source:string;collected_at:string;series_ok:number;series_total:number};
  series:Record<string,EIASeries>;
  analysis_summary:string[];
}

interface EIASeries {
  name:string; latest_period:string|null; latest_value:number|null;
  unit:string; wow_change_pct:number|null; mom_change_pct:number|null;
  yoy_change_pct:number|null; high_52w:number|null; low_52w:number|null;
  pct_range_52w:number|null;
  history:{period:string;value:number;unit:string}[];
}
interface EIAData {
  metadata:{source:string;collected_at:string;series_ok:number;series_total:number};
  series:Record<string,EIASeries>;
  analysis_summary:string[];
}

interface StockItem {
  symbol:string; price:number; avg_5y?:number; price_vs_avg:number; state:string;
  factors:string[]; data_available:{stock_real?:boolean;stock_source?:string;stock_proxy:boolean;curve:boolean;cot:boolean};
  stock_current?:number; stock_avg?:number; stock_unit?:string;
  stock_history?:{year:number;period:string;value:number}[];
}
interface StocksData { timestamp:string; commodities:Record<string,StockItem>; }

interface COTHistoryEntry {
  date:string; open_interest:number|null;
  comm_long?:number|null; comm_short?:number|null; comm_net?:number|null;
  noncomm_long?:number|null; noncomm_short?:number|null; noncomm_net?:number|null;
  producer_long?:number|null; producer_short?:number|null; producer_net?:number|null;
  swap_long?:number|null; swap_short?:number|null; swap_net?:number|null;
  managed_money_long?:number|null; managed_money_short?:number|null; managed_money_net?:number|null;
  mm_long_pct?:number|null; mm_short_pct?:number|null;
}
interface COTDeltaSignal {
  type:string; label:string; direction:string; probability:number;
  color:string; icon:string; description:string;
}
interface COTDeltaAnalysis {
  signals:COTDeltaSignal[]; dominant_direction:string; reversal_score:number;
  cot_index:number; cot_index_52w?:number; cot_index_26w?:number; neg_streak:number; pos_streak:number;
  current_delta:number; prev_delta:number; oi_trend_pct:number; deltas_8w:number[];
}
interface COTCommodity {
  ticker:string; name:string;
  legacy?:{ticker:string;name:string;report_type:string;latest:COTHistoryEntry;history:COTHistoryEntry[];weeks:number};
  disaggregated?:{ticker:string;name:string;report_type:string;latest:COTHistoryEntry;history:COTHistoryEntry[];weeks:number;cot_index?:number;cot_index_52w?:number;cot_index_26w?:number;delta_analysis?:COTDeltaAnalysis};
}
interface COTData {
  generated_at:string; source:string; year:number;
  commodities:Record<string,COTCommodity>;
}

interface DailyReading {
  timestamp:string; date:string;
  blocos:{title:string;body:string}[];
  perguntas:string[];
  resumo:Record<string,string>;
  sources:string[];
}

interface FuturesContract {
  contract:string; expiry_label:string; close:number; open?:number;
  high?:number; low?:number; volume?:number; date?:string; source?:string;
  month?:string; month_code?:string; year?:string;
}
interface FuturesCommodity {
  ticker:string; name:string; exchange?:string; unit?:string;
  contracts:FuturesContract[];
  spreads?:{front:string;back:string;spread:number;structure:string}[];
}
interface FuturesData {
  commodities:Record<string,FuturesCommodity>;
}

type Tab = "Visão Geral"|"Gráfico + COT"|"Comparativo"|"Spreads"|"Sazonalidade"|"Stocks Watch"|"Custo Produção"|"Físico Intl"|"Leitura do Dia"|"Energia"|"Portfolio"|"Bilateral"|"Grain Ratios"|"Livestock Risk"|"Paridades";

// -- Color Theme ------------------------------------------------------------
const C = {
  bg:"#0d1117", panel:"#161b22", panelAlt:"#1c2128", border:"rgba(148,163,184,.12)",
  text:"#e2e8f0", textDim:"#94a3b8", textMuted:"#64748b",
  green:"#22c55e", red:"#ef4444", amber:"#f59e0b", blue:"#3b82f6", cyan:"#06b6d4", purple:"#a78bfa", gold:"#DCB432",
  greenBg:"rgba(34,197,94,.12)", redBg:"rgba(239,68,68,.12)", amberBg:"rgba(245,158,11,.12)", blueBg:"rgba(59,130,246,.12)",
  greenBorder:"rgba(34,197,94,.3)", redBorder:"rgba(239,68,68,.3)", amberBorder:"rgba(245,158,11,.3)",
  candleUp:"#00C878", candleDn:"#DC3C3C", wick:"rgba(148,163,184,.75)",
  ma9:"#DCB432", ma21:"#4ade80", ma50:"#3b82f6", ma200:"#f97316", bb:"rgba(148,163,184,.18)",
  rsiLine:"#e2e8f0", rsiOverbought:"rgba(239,68,68,.18)", rsiOversold:"rgba(34,197,94,.18)",
};

// -- Commodities ------------------------------------------------------------
const COMMODITIES:{sym:string;name:string;group:string;unit:string}[] = [
  {sym:"ZC",name:"Corn",group:"Grãos",unit:"¢/bu"},
  {sym:"ZS",name:"Soybeans",group:"Grãos",unit:"¢/bu"},
  {sym:"ZW",name:"Wheat CBOT",group:"Grãos",unit:"¢/bu"},
  {sym:"KE",name:"Wheat KC",group:"Grãos",unit:"¢/bu"},
  {sym:"ZM",name:"Soybean Meal",group:"Grãos",unit:"$/st"},
  {sym:"ZL",name:"Soybean Oil",group:"Grãos",unit:"¢/lb"},
  {sym:"SB",name:"Sugar #11",group:"Softs",unit:"¢/lb"},
  {sym:"KC",name:"Coffee C",group:"Softs",unit:"¢/lb"},
  {sym:"CT",name:"Cotton #2",group:"Softs",unit:"¢/lb"},
  {sym:"CC",name:"Cocoa",group:"Softs",unit:"$/mt"},
  {sym:"OJ",name:"Orange Juice",group:"Softs",unit:"¢/lb"},
  {sym:"LE",name:"Live Cattle",group:"Pecuária",unit:"¢/lb"},
  {sym:"GF",name:"Feeder Cattle",group:"Pecuária",unit:"¢/lb"},
  {sym:"HE",name:"Lean Hogs",group:"Pecuária",unit:"¢/lb"},
  {sym:"CL",name:"Crude Oil",group:"Energia",unit:"$/bbl"},
  {sym:"NG",name:"Natural Gas",group:"Energia",unit:"$/MMBtu"},
  {sym:"GC",name:"Gold",group:"Metais",unit:"$/oz"},
  {sym:"SI",name:"Silver",group:"Metais",unit:"$/oz"},
  {sym:"DX",name:"Dollar Index",group:"Macro",unit:"index"},
];

const TABS:Tab[] = ["Visão Geral","Gráfico + COT","Comparativo","Spreads","Sazonalidade","Stocks Watch","Custo Produção","Físico Intl","Leitura do Dia","Energia","Portfolio","Bilateral","Paridades","Grain Ratios","Livestock Risk"];

const SEASON_COLORS:Record<string,string> = {
  "2021":"#6366f1","2022":"#3b82f6","2023":"#8b5cf6","2024":"#ec4899","2025":"#22c55e","2026":"#f59e0b",
  "current":"#f59e0b","average":"#e2e8f0",
};

const SPREAD_NAMES:Record<string,string> = {
  soy_crush:"Soy Crush Margin",ke_zw:"KC-CBOT Wheat",zl_cl:"Soy Oil / Crude",
  feedlot:"Feedlot Margin",zc_zm:"Corn / Meal",zc_zs:"Corn / Soy Ratio",
  cattle_crush:"Cattle Crush Margin",feed_wheat:"Feed Wheat Ratio",
};

const SPREAD_DETAILS:Record<string,{whatIsIt:string;whyMatters:string}> = {
  soy_crush:{
    whatIsIt:"Lucro das esmagadoras ao transformar soja em farelo + óleo. Quando a margem está alta, esmagadoras compram mais soja.",
    whyMatters:"Margem alta = mais demanda por soja = preço da soja tende a subir.",
  },
  ke_zw:{
    whatIsIt:"Diferença de preço entre trigo de alta proteína (KC) e trigo comum (CBOT). Normalmente o trigo duro vale mais.",
    whyMatters:"Prêmio sumindo = excesso de trigo duro ou falta de trigo mole. Sinal de mudança no mercado.",
  },
  zl_cl:{
    whatIsIt:"Compara o preço do óleo de soja com o petróleo. Quando o óleo está muito caro em relação ao petróleo, biodiesel fica menos competitivo.",
    whyMatters:"Ratio muito alto = óleo de soja caro demais ? pode perder demanda do biodiesel ? pressão de baixa no óleo.",
  },
  feedlot:{
    whatIsIt:"Lucro do confinador: preço de venda do boi gordo menos o custo do boi magro e da ração.",
    whyMatters:"Margem positiva = confinadores entram, mais boi no mercado no futuro. Margem negativa = confinadores saem, oferta cai.",
  },
  zc_zm:{
    whatIsIt:"Compara o custo do milho com o farelo de soja na formulação de ração. Quem está mais barato ganha espaço na mistura.",
    whyMatters:"Ratio baixo = milho mais competitivo na ração ? mais demanda por milho. Ratio alto = farelo ganha espaço.",
  },
  zc_zs:{
    whatIsIt:"Quantos bushels de milho uma de soja compra. Acima de 2.4 = mais vantajoso plantar soja. Abaixo de 2.2 = milho ganha.",
    whyMatters:"Esse número influencia o que o produtor americano vai plantar na próxima safra -- e isso afeta os preços futuros.",
  },
};

const SPREAD_FRIENDLY_NAMES:Record<string,string> = {
  soy_crush:"Margem de Esmagamento (Soja)",
  ke_zw:"Pr\u00eamio Trigo Duro vs Mole (KC-CBOT)",
  zl_cl:"\u00d3leo de Soja vs Petr\u00f3leo (Biodiesel)",
  feedlot:"Margem de Confinamento (Feedlot)",
  zc_zm:"Milho vs Farelo (Ra\u00e7\u00e3o Animal)",
  zc_zs:"Soja vs Milho (Decis\u00e3o de Plantio)",
  cattle_crush:"Margem Bruta Confinamento (Cattle Crush)",
  feed_wheat:"Trigo vs Milho na Ra\u00e7\u00e3o (Feed Wheat)",
};

const SPREAD_CATEGORY_LABELS:Record<string,string> = {
  graos:"\u{1F33E} Gr\u00e3os & Oleaginosas",
  pecuaria:"\u{1F404} Pecu\u00e1ria",
  energia:"\u26A1 Energia & Biodiesel",
  basis:"\u{1F30D} Basis Internacional",
  outros:"Outros",
};

function getAlertLevel(sp:{regime:string;zscore_1y:number;percentile:number;key?:string}):"ok"|"atencao"|"alerta" {
  if(sp.regime==="EXTREMO"||Math.abs(sp.zscore_1y)>=2||sp.percentile>=95||sp.percentile<=5) return "alerta";
  if(sp.regime!=="NORMAL"||Math.abs(sp.zscore_1y)>=1.3||sp.percentile>=90||sp.percentile<=10) return "atencao";
  return "ok";
}

function getVerdict(sp:{key?:string;regime:string;zscore_1y:number;percentile:number;current:number;trend?:string;trend_pct?:number;name:string}):string {
  const pctLabel = sp.percentile>=90?"no nível mais caro":sp.percentile>=75?"acima da média":sp.percentile<=10?"no nível mais barato":sp.percentile<=25?"abaixo da média":"dentro do normal";
  const trendLabel = sp.trend==="SUBINDO"?"e subindo":sp.trend==="CAINDO"?"e caindo":"e estável";
  const alert = getAlertLevel(sp);
  const prefix = alert==="alerta"?"":alert==="atencao"?"":"";
  return prefix + (SPREAD_FRIENDLY_NAMES[sp.key||""]||sp.name) + " está " + pctLabel + " no último ano " + trendLabel + (sp.trend_pct?` (${sp.trend_pct>0?"+":""}${sp.trend_pct?.toFixed(1)}%).`:".");
}

function getThermometerZone(pct:number):{text:string;color:string} {
  if(pct<=10) return {text:"Muito Barato",color:"#22c55e"};
  if(pct<=25) return {text:"Barato",color:"#4ade80"};
  if(pct<=40) return {text:"Abaixo da Média",color:"#60a5fa"};
  if(pct<=60) return {text:"Na Média",color:"#94a3b8"};
  if(pct<=75) return {text:"Acima da Média",color:"#fbbf24"};
  if(pct<=90) return {text:"Caro",color:"#f97316"};
  return {text:"Muito Caro",color:"#ef4444"};
}

// -- Physical Markets Data (International - placeholder) ---------------------
const PHYS_INTL:{cat:string;items:{origin:string;price:string;basis:string;trend:string;source:string}[]}[] = [
  {cat:"Coffee",items:[
    {origin:" Brasil (Santos) -- Arabica NY 2/3",price:"--",basis:"--",trend:"Colheita 25/26 iniciando",source:"Cecafé"},
    {origin:" Brasil (Cerrado) -- Fine Cup",price:"--",basis:"--",trend:"Prêmio qualidade",source:"Cecafé"},
    {origin:" Colômbia -- Excelso EP",price:"--",basis:"--",trend:"Prêmio qualidade alto",source:"FNC"},
    {origin:" Vietnã -- Robusta G2",price:"--",basis:"--",trend:"Safra acima esperado",source:"VICOFA"},
  ]},
  {cat:"Soybeans",items:[
    {origin:" Brasil (Paranaguá) -- GMO",price:"--",basis:"--",trend:"Colheita recorde",source:"CEPEA"},
    {origin:" Argentina -- GMO",price:"--",basis:"--",trend:"Safra recuperando",source:"B.Cereales"},
  ]},
  {cat:"Corn",items:[
    {origin:" Brasil (Paranaguá) -- GMO",price:"--",basis:"--",trend:"Safrinha nos portos",source:"CEPEA"},
    {origin:" Argentina -- GMO",price:"--",basis:"--",trend:"Safra volumosa",source:"B.Cereales"},
  ]},
  {cat:"Live Cattle",items:[
    {origin:" Brasil (SP) -- Boi Gordo @",price:"--",basis:"--",trend:"Alta sazonal",source:"CEPEA"},
    {origin:" Argentina (Liniers)",price:"--",basis:"--",trend:"Câmbio favorece export",source:"IPCVA"},
  ]},
  {cat:"Wheat",items:[
    {origin:" Rússia (FOB BS) -- 12.5%",price:"--",basis:"--",trend:"Safra grande",source:"IKAR"},
    {origin:" Argentina -- Trigo Pan",price:"--",basis:"--",trend:"Normalizado",source:"B.Cereales"},
  ]},
  {cat:"Cocoa",items:[
    {origin:" Costa do Marfim -- Grade I",price:"--",basis:"--",trend:"Menor safra 10 anos",source:"CCC"},
    {origin:" Gana -- Grade I",price:"--",basis:"--",trend:"Produção -50%",source:"COCOBOD"},
  ]},
  {cat:" China Demand",items:[
    {origin:" Soja Import (DCE)",price:"--",basis:"--",trend:"Importação desacelerando",source:"GACC"},
    {origin:" Milho Import (DCE)",price:"--",basis:"--",trend:"Estoques altos",source:"GACC"},
    {origin:" Suínos (Zhengzhou)",price:"--",basis:"--",trend:"Demanda estável",source:"MARA"},
  ]},
];
// -- Helper Components ------------------------------------------------------

function Badge({label,color}:{label:string;color:string}) {
  const bg = color===C.green?C.greenBg:color===C.red?C.redBg:color===C.amber?C.amberBg:C.blueBg;
  const border = color===C.green?C.greenBorder:color===C.red?C.redBorder:color===C.amber?C.amberBorder:"rgba(59,130,246,.3)";
  return <span style={{display:"inline-block",padding:"3px 10px",borderRadius:4,fontSize:10,fontWeight:700,
    color,background:bg,border:`1px solid ${border}`,letterSpacing:.3}}>{label}</span>;
}

function DevBar({val}:{val:number}) {
  const pct = Math.min(Math.abs(val),50)/50*100;
  const color = val<-15?C.red:val>15?C.green:C.amber;
  const left = val<0;
  return (
    <div style={{display:"flex",alignItems:"center",gap:8}}>
      <div style={{width:80,height:8,background:"rgba(148,163,184,.1)",borderRadius:4,position:"relative",overflow:"hidden"}}>
        <div style={{
          position:"absolute",height:"100%",borderRadius:4,background:color,
          width:`${pct/2}%`,
          left:left?"auto":"50%",right:left?"50%":"auto",
        }}/>
        <div style={{position:"absolute",left:"50%",top:0,bottom:0,width:1,background:"rgba(148,163,184,.3)"}}/>
      </div>
      <span style={{fontSize:11,fontFamily:"monospace",fontWeight:600,color,minWidth:50}}>
        {val>=0?"+":""}{val.toFixed(1)}%
      </span>
    </div>
  );
}

function PctBar({val}:{val:number}) {
  const color = val>70?C.green:val<30?C.red:C.amber;
  return (
    <div style={{display:"flex",alignItems:"center",gap:8}}>
      <div style={{width:60,height:6,background:"rgba(148,163,184,.1)",borderRadius:3,overflow:"hidden"}}>
        <div style={{height:"100%",width:`${val}%`,background:color,borderRadius:3}}/>
      </div>
      <span style={{fontSize:10,fontFamily:"monospace",color:C.textDim}}>{val.toFixed(0)}%</span>
    </div>
  );
}


function SectionTitle({children}:{children:React.ReactNode}) {
  return <div style={{fontSize:13,fontWeight:700,color:C.text,marginBottom:12,paddingBottom:8,
    borderBottom:`1px solid ${C.border}`,display:"flex",alignItems:"center",gap:8}}>{children}</div>;
}

function TableHeader({cols}:{cols:string[]}) {
  return (
    <tr style={{background:C.panelAlt}}>
      {cols.map((c,i)=>(
        <th key={i} style={{padding:"10px 12px",textAlign:i===0?"left":"right",fontSize:10,fontWeight:700,
          color:C.textMuted,textTransform:"uppercase",letterSpacing:.5,borderBottom:`1px solid ${C.border}`}}>{c}</th>
      ))}
    </tr>
  );
}

function DataPlaceholder({title,detail}:{title:string;detail:string}) {
  return (
    <div style={{padding:40,textAlign:"center",background:C.panelAlt,borderRadius:8,border:`1px dashed ${C.border}`}}>
      <div style={{fontSize:14,fontWeight:600,color:C.textDim,marginBottom:6}}>{title}</div>
      <div style={{fontSize:11,color:C.textMuted,maxWidth:400,margin:"0 auto"}}>{detail}</div>
    </div>
  );
}

function LoadingSpinner() {
  return (
    <div style={{display:"flex",alignItems:"center",justifyContent:"center",padding:60}}>
      <div style={{width:32,height:32,border:`3px solid ${C.border}`,borderTopColor:C.blue,
        borderRadius:"50%",animation:"spin 1s linear infinite"}}/>
      <style>{`@keyframes spin{to{transform:rotate(360deg)}}`}</style>
    </div>
  );
}
// -- Technical Indicators ---------------------------------------------------

function emaCalc(data:number[],period:number):number[] {
  const k=2/(period+1);
  const ema:number[]=[];
  data.forEach((v,i)=>{
    if(i===0)ema.push(v);
    else ema.push(v*k+ema[i-1]*(1-k));
  });
  return ema;
}

function smaCalc(data:number[],period:number):number[] {
  const sma:number[]=[];
  for(let i=0;i<data.length;i++){
    if(i<period-1){sma.push(NaN);continue;}
    const slice=data.slice(i-period+1,i+1);
    sma.push(slice.reduce((a,b)=>a+b,0)/period);
  }
  return sma;
}

function bbCalc(data:number[],period:number=20,mult:number=2):{upper:number[];middle:number[];lower:number[]} {
  const middle=smaCalc(data,period);
  const upper:number[]=[];
  const lower:number[]=[];
  for(let i=0;i<data.length;i++){
    if(i<period-1){upper.push(NaN);lower.push(NaN);continue;}
    const slice=data.slice(i-period+1,i+1);
    const avg=middle[i];
    const std=Math.sqrt(slice.reduce((sum,v)=>sum+(v-avg)**2,0)/period);
    upper.push(avg+mult*std);
    lower.push(avg-mult*std);
  }
  return {upper,middle,lower};
}

function rsiCalc(data:number[],period:number=14):number[] {
  const rsi:number[]=[];
  let gains=0,losses=0;
  
  for(let i=0;i<data.length;i++){
    if(i===0){rsi.push(50);continue;}
    const change=data[i]-data[i-1];
    const gain=change>0?change:0;
    const loss=change<0?-change:0;
    
    if(i<period){
      gains+=gain;
      losses+=loss;
      rsi.push(50);
    } else if(i===period){
      gains+=gain;
      losses+=loss;
      const avgGain=gains/period;
      const avgLoss=losses/period;
      const rs=avgLoss===0?100:avgGain/avgLoss;
      rsi.push(100-100/(1+rs));
    } else {
      gains=(gains*(period-1)+gain)/period;
      losses=(losses*(period-1)+loss)/period;
      const avgGain=gains;
      const avgLoss=losses;
      const rs=avgLoss===0?100:avgGain/avgLoss;
      rsi.push(100-100/(1+rs));
    }
  }
  return rsi;
}
// -- Price Chart with RSI ---------------------------------------------------

function PriceChart({candles,symbol,annotations}:{candles:OHLCV[];symbol:string;annotations?:{label:string;value:number;color:string}[]}) {
  const ref = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [dims,setDims] = useState({w:900,h:560});
  const [hover,setHover] = useState<{i:number;x:number;y:number}|null>(null);
  const VISIBLE = 90;
  const RSI_HEIGHT = 110;

  useEffect(()=>{
    const el=containerRef.current;if(!el)return;
    const obs=new ResizeObserver(entries=>{
      const{width}=entries[0].contentRect;
      if(width>0)setDims({w:Math.floor(width),h:560});
    });
    obs.observe(el);return()=>obs.disconnect();
  },[]);

  useEffect(()=>{
    const cvs=ref.current;if(!cvs||!candles.length)return;
    const ctx=cvs.getContext("2d");if(!ctx)return;
    
    const W=dims.w,H=dims.h;
    const mainH=H-RSI_HEIGHT-30;
    cvs.width=W*2;cvs.height=H*2;ctx.scale(2,2);
    cvs.style.width=W+"px";cvs.style.height=H+"px";

    const data=candles.slice(-VISIBLE);
    const closes=data.map(c=>c.close);
    const ema9=emaCalc(closes,9);
    const ma21=smaCalc(closes,21);
    const ma50=smaCalc(closes,50);
    const ma200=smaCalc(closes,200);
    const bb=bbCalc(closes,20,2);
    const rsi=rsiCalc(closes,14);

    const pad={t:24,b:30,l:55,r:16};
    const chartH=mainH-pad.t-pad.b;
    const allP=data.flatMap(c=>[c.high,c.low]);
    const mn=Math.min(...allP),mx=Math.max(...allP),range=mx-mn||1;
    const pMn=mn-range*.05,pMx=mx+range*.05;
    const bW=Math.max(2,(W-pad.l-pad.r)/data.length-1);

    const yP=(v:number)=>pad.t+(1-(v-pMn)/(pMx-pMn))*chartH;
    const xC=(i:number)=>pad.l+i*(W-pad.l-pad.r)/data.length+bW/2;

    // Background
    ctx.fillStyle=C.bg;ctx.fillRect(0,0,W,H);

    // Grid lines
    ctx.strokeStyle="rgba(148,163,184,.06)";ctx.lineWidth=1;
    for(let i=0;i<5;i++){
      const y=pad.t+i*(chartH/4);
      ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(W-pad.r,y);ctx.stroke();
    }

    // Y axis labels
    ctx.fillStyle=C.textMuted;ctx.font="10px monospace";ctx.textAlign="right";
    for(let i=0;i<5;i++){
      const v=pMx-(pMx-pMn)*i/4;
      ctx.fillText(v.toFixed(2),pad.l-6,pad.t+i*(chartH/4)+4);
    }

    // Bollinger Bands fill
    ctx.fillStyle=C.bb;ctx.beginPath();
    let started=false;
    for(let i=0;i<data.length;i++){
      if(isNaN(bb.upper[i]))continue;
      const x=xC(i);
      if(!started){ctx.moveTo(x,yP(bb.upper[i]));started=true;}
      else ctx.lineTo(x,yP(bb.upper[i]));
    }
    for(let i=data.length-1;i>=0;i--){
      if(isNaN(bb.lower[i]))continue;
      ctx.lineTo(xC(i),yP(bb.lower[i]));
    }
    ctx.closePath();ctx.fill();

    // Moving averages
    const drawLine=(arr:number[],color:string,width:number=1)=>{
      ctx.strokeStyle=color;ctx.lineWidth=width;ctx.beginPath();
      let s=false;
      arr.forEach((v,i)=>{
        if(isNaN(v))return;
        const x=xC(i),y=yP(v);
        if(!s){ctx.moveTo(x,y);s=true;}else ctx.lineTo(x,y);
      });
      ctx.stroke();
    };
    drawLine(ema9,C.ma9,1);
    drawLine(ma21,C.ma21,1.2);
    drawLine(ma50,C.ma50,1.2);
    if(ma200.some(v=>!isNaN(v)))drawLine(ma200,C.ma200,1.2);

    // Candles
    for(let i=0;i<data.length;i++){
      const c=data[i];const x=xC(i);const up=c.close>=c.open;
      ctx.strokeStyle=C.wick;ctx.lineWidth=1.5;
      ctx.beginPath();ctx.moveTo(x,yP(c.high));ctx.lineTo(x,yP(c.low));ctx.stroke();
      ctx.fillStyle=up?C.candleUp:C.candleDn;
      const top=yP(Math.max(c.open,c.close));
      const bot=yP(Math.min(c.open,c.close));
      const h=Math.max(1,bot-top);
      ctx.fillRect(x-bW/2,top,bW,h);
      ctx.strokeStyle=up?"rgba(0,200,120,0.6)":"rgba(220,60,60,0.6)";
      ctx.lineWidth=0.5;
      ctx.strokeRect(x-bW/2,top,bW,h);
    }

    // Annotation lines (COP, FOB, etc.)
    if(annotations){
      for(const ann of annotations){
        if(ann.value<pMn||ann.value>pMx) continue;
        const ay=yP(ann.value);
        ctx.strokeStyle=ann.color;ctx.lineWidth=1;ctx.setLineDash([6,4]);
        ctx.beginPath();ctx.moveTo(pad.l,ay);ctx.lineTo(W-pad.r,ay);ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle=ann.color;ctx.font="bold 9px monospace";ctx.textAlign="right";
        ctx.fillText(ann.label,W-pad.r-2,ay-4);
      }
    }

    // Volume bars (small)
    const volH=30;
    const maxVol=Math.max(...data.map(c=>c.volume||1));
    ctx.globalAlpha=0.3;
    for(let i=0;i<data.length;i++){
      const c=data[i];const vh=(c.volume/maxVol)*volH;const up=c.close>=c.open;
      ctx.fillStyle=up?C.candleUp:C.candleDn;
      ctx.fillRect(xC(i)-bW/2,mainH-pad.b-vh,bW,vh);
    }
    ctx.globalAlpha=1;

    // X axis dates
    ctx.fillStyle=C.textMuted;ctx.font="9px monospace";ctx.textAlign="center";
    const step=Math.ceil(data.length/8);
    for(let i=0;i<data.length;i+=step){
      const d=data[i].date;
      ctx.fillText(d.slice(5),xC(i),mainH-pad.b+14);
    }

    // RSI Panel
    const rsiTop=mainH+10;
    const rsiH=RSI_HEIGHT-20;

    // RSI label (before background so it's behind everything)
    ctx.fillStyle="rgba(148,163,184,.5)";
    ctx.font="bold 9px monospace";ctx.textAlign="left";
    ctx.fillText("RSI 14",pad.l+4,rsiTop+10);

    // RSI background
    ctx.fillStyle=C.panelAlt;
    ctx.fillRect(pad.l,rsiTop,W-pad.l-pad.r,rsiH);

    // RSI zones
    ctx.fillStyle=C.rsiOverbought;
    ctx.fillRect(pad.l,rsiTop,W-pad.l-pad.r,rsiH*0.3);
    ctx.fillStyle=C.rsiOversold;
    ctx.fillRect(pad.l,rsiTop+rsiH*0.7,W-pad.l-pad.r,rsiH*0.3);

    // Linha 70 (sobrecompra)
    const y70=rsiTop+rsiH*0.30;
    ctx.strokeStyle="rgba(239,68,68,.5)";ctx.lineWidth=0.75;
    ctx.setLineDash([3,3]);
    ctx.beginPath();ctx.moveTo(pad.l,y70);ctx.lineTo(W-pad.r,y70);ctx.stroke();

    // Linha 30 (sobrevenda)
    const y30=rsiTop+rsiH*0.70;
    ctx.strokeStyle="rgba(34,197,94,.5)";ctx.lineWidth=0.75;
    ctx.setLineDash([3,3]);
    ctx.beginPath();ctx.moveTo(pad.l,y30);ctx.lineTo(W-pad.r,y30);ctx.stroke();

    ctx.setLineDash([]);

    // RSI 50 line
    ctx.strokeStyle="rgba(148,163,184,.2)";ctx.lineWidth=1;
    ctx.setLineDash([4,4]);
    ctx.beginPath();ctx.moveTo(pad.l,rsiTop+rsiH*0.5);ctx.lineTo(W-pad.r,rsiTop+rsiH*0.5);ctx.stroke();
    ctx.setLineDash([]);

    // RSI line
    const yRsi=(v:number)=>rsiTop+rsiH*(1-v/100);
    ctx.strokeStyle=C.rsiLine;ctx.lineWidth=2.5;ctx.beginPath();
    let rsiStarted=false;
    for(let i=0;i<rsi.length;i++){
      const x=xC(i),y=yRsi(rsi[i]);
      if(!rsiStarted){ctx.moveTo(x,y);rsiStarted=true;}else ctx.lineTo(x,y);
    }
    ctx.stroke();

    // RSI labels
    ctx.fillStyle=C.textMuted;ctx.font="9px monospace";ctx.textAlign="right";
    ctx.fillText("70",pad.l-4,rsiTop+rsiH*0.3+3);
    ctx.fillText("50",pad.l-4,rsiTop+rsiH*0.5+3);
    ctx.fillText("30",pad.l-4,rsiTop+rsiH*0.7+3);

    // RSI value
    const lastRsi=rsi[rsi.length-1];
    ctx.fillStyle=lastRsi>70?C.red:lastRsi<30?C.green:C.amber;
    ctx.font="bold 11px monospace";ctx.textAlign="left";
    ctx.fillText(`RSI: ${lastRsi.toFixed(1)}`,pad.l+5,rsiTop+24);

    // Legend
    const legendY=8;
    ctx.font="9px monospace";ctx.textAlign="left";
    const legends=[
      {label:"EMA9",color:C.ma9},{label:"MA21",color:C.ma21},
      {label:"MA50",color:C.ma50},{label:"MA200",color:C.ma200},{label:"BB",color:"rgba(148,163,184,.5)"}
    ];
    let lx=pad.l;
    legends.forEach(({label,color})=>{
      ctx.fillStyle=color;ctx.fillRect(lx,legendY,16,2);
      ctx.fillStyle=C.textMuted;ctx.fillText(label,lx+20,legendY+4);
      lx+=60;
    });

    // Hover crosshair
    if(hover&&hover.i>=0&&hover.i<data.length){
      const hc=data[hover.i];
      const x=xC(hover.i);
      ctx.strokeStyle="rgba(148,163,184,.3)";ctx.lineWidth=1;
      ctx.setLineDash([4,4]);
      ctx.beginPath();ctx.moveTo(x,pad.t);ctx.lineTo(x,mainH-pad.b);ctx.stroke();
      ctx.setLineDash([]);
      
      // Tooltip
      ctx.fillStyle="rgba(22,27,34,.95)";
      const tx=Math.min(x+10,W-140);
      ctx.fillRect(tx,pad.t,130,85);
      ctx.strokeStyle=C.border;ctx.strokeRect(tx,pad.t,130,85);
      ctx.fillStyle=C.text;ctx.font="bold 10px monospace";
      ctx.fillText(hc.date,tx+8,pad.t+14);
      ctx.font="10px monospace";ctx.fillStyle=C.textDim;
      ctx.fillText(`O: ${hc.open.toFixed(2)}`,tx+8,pad.t+30);
      ctx.fillText(`H: ${hc.high.toFixed(2)}`,tx+8,pad.t+44);
      ctx.fillText(`L: ${hc.low.toFixed(2)}`,tx+8,pad.t+58);
      ctx.fillStyle=hc.close>=hc.open?C.green:C.red;
      ctx.fillText(`C: ${hc.close.toFixed(2)}`,tx+8,pad.t+72);
    }
  },[candles,dims,hover]);

  const handleMouse=(e:React.MouseEvent)=>{
    const rect=ref.current?.getBoundingClientRect();if(!rect)return;
    const x=e.clientX-rect.left;
    const data=candles.slice(-VISIBLE);
    const pad={l:55,r:16};
    const bW=(dims.w-pad.l-pad.r)/data.length;
    const i=Math.floor((x-pad.l)/bW);
    if(i>=0&&i<data.length)setHover({i,x:e.clientX,y:e.clientY});
    else setHover(null);
  };

  return (
    <div ref={containerRef} style={{width:"100%"}}>
      <canvas ref={ref} style={{borderRadius:6,display:"block",cursor:"crosshair"}}
        onMouseMove={handleMouse} onMouseLeave={()=>setHover(null)}/>
    </div>
  );
}
// -- Season Chart -----------------------------------------------------------

function SeasonChart({entry}:{entry:SeasonEntry}) {
  const ref=useRef<HTMLCanvasElement>(null);
  const containerRef=useRef<HTMLDivElement>(null);
  const [w,setW]=useState(900);

  useEffect(()=>{
    const el=containerRef.current;if(!el)return;
    const obs=new ResizeObserver(entries=>{const{width}=entries[0].contentRect;if(width>0)setW(Math.floor(width));});
    obs.observe(el);return()=>obs.disconnect();
  },[]);

  useEffect(()=>{
    const cvs=ref.current;if(!cvs)return;
    const ctx=cvs.getContext("2d");if(!ctx)return;
    const H=320;
    cvs.width=w*2;cvs.height=H*2;ctx.scale(2,2);
    cvs.style.width=w+"px";cvs.style.height=H+"px";

    const pad={t:30,b:30,l:60,r:16};
    const cH=H-pad.t-pad.b;

    // Normalize each year to % return from first data point (base=0%)
    const normalized: Record<string,{day:number;pct:number}[]> = {};
    let allPcts:number[] = [];
    for(const yr of entry.years){
      const s=entry.series[yr];if(!s||!s.length)continue;
      const base=s[0].close;
      if(!base||base<=0)continue;
      normalized[yr]=s.map(p=>({day:p.day,pct:(p.close/base-1)*100}));
      allPcts.push(...normalized[yr].map(p=>p.pct));
    }
    if(!allPcts.length)return;

    const mn=Math.min(...allPcts),mx=Math.max(...allPcts),range=mx-mn||1;
    const pMn=mn-range*.08,pMx=mx+range*.08;
    const yP=(v:number)=>pad.t+(1-(v-pMn)/(pMx-pMn))*cH;
    const xP=(day:number)=>pad.l+(day/365)*(w-pad.l-pad.r);

    // Background
    ctx.fillStyle=C.bg;ctx.fillRect(0,0,w,H);

    // Grid
    ctx.strokeStyle="rgba(148,163,184,.06)";ctx.lineWidth=1;
    for(let i=0;i<5;i++){const y=pad.t+i*(cH/4);ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();}

    // Zero line (0% return)
    if(pMn<0&&pMx>0){
      const zeroY=yP(0);
      ctx.strokeStyle="rgba(148,163,184,.25)";ctx.lineWidth=1;ctx.setLineDash([4,4]);
      ctx.beginPath();ctx.moveTo(pad.l,zeroY);ctx.lineTo(w-pad.r,zeroY);ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle=C.textMuted;ctx.font="9px monospace";ctx.textAlign="right";
      ctx.fillText("0%",pad.l-6,zeroY+3);
    }

    // Y axis labels (% return)
    ctx.fillStyle=C.textMuted;ctx.font="10px monospace";ctx.textAlign="right";
    for(let i=0;i<5;i++){
      const v=pMx-(pMx-pMn)*i/4;
      const label=`${v>0?"+":""}${v.toFixed(1)}%`;
      ctx.fillText(label,pad.l-6,pad.t+i*(cH/4)+4);
    }

    // X axis months
    ctx.textAlign="center";ctx.font="9px monospace";
    const months=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    for(let m=0;m<12;m++){const day=m*30.4+15;ctx.fillText(months[m],xP(day),H-pad.b+14);}

    // Draw lines (years first, then current, then average on top)
    const drawOrder = entry.years.filter(y=>y!=="current"&&y!=="average"&&normalized[y]);
    if(normalized["average"])drawOrder.push("average");
    if(normalized["current"])drawOrder.push("current");

    for(const yr of drawOrder){
      const pts=normalized[yr];if(!pts||!pts.length)continue;
      const col=SEASON_COLORS[yr]||C.textMuted;
      ctx.strokeStyle=col;
      ctx.lineWidth=yr==="current"?2.5:yr==="average"?2:1;
      ctx.setLineDash(yr==="average"?[6,4]:[]);
      ctx.globalAlpha=yr==="current"||yr==="average"?1:.35;
      ctx.beginPath();
      let started=false;
      for(const p of pts){
        const x=xP(p.day),y=yP(p.pct);
        if(!started){ctx.moveTo(x,y);started=true;}else ctx.lineTo(x,y);
      }
      ctx.stroke();
      ctx.globalAlpha=1;ctx.setLineDash([]);
      // End-of-line label with YTD return
      if(pts.length>0&&(yr==="current"||yr==="average")){
        const lastPt=pts[pts.length-1];
        ctx.fillStyle=col;ctx.font="bold 9px monospace";ctx.textAlign="left";
        ctx.fillText(`${lastPt.pct>0?"+":""}${lastPt.pct.toFixed(1)}%`,xP(lastPt.day)+4,yP(lastPt.pct)+3);
      }
    }

    // Legend
    let lx=pad.l;
    ctx.font="9px monospace";
    for(const yr of entry.years){
      if(!normalized[yr])continue;
      const col=SEASON_COLORS[yr]||C.textMuted;
      ctx.fillStyle=col;
      ctx.fillRect(lx,10,yr==="current"||yr==="average"?20:14,yr==="average"?2:3);
      ctx.fillStyle=C.textMuted;ctx.textAlign="left";
      ctx.fillText(yr,lx+(yr==="current"||yr==="average"?24:18),14);
      lx+=60;
    }
  },[entry,w]);

  return (
    <div ref={containerRef} style={{width:"100%"}}>
      <canvas ref={ref} style={{borderRadius:6,display:"block"}}/>
    </div>
  );
}

// -- Spread Chart (mini) ----------------------------------------------------

function SpreadChart({history,regime}:{history:{date:string;value:number}[];regime:string}) {
  const ref=useRef<HTMLCanvasElement>(null);
  useEffect(()=>{
    const cvs=ref.current;if(!cvs||!history.length)return;
    const ctx=cvs.getContext("2d");if(!ctx)return;
    const W=230,H=60;
    cvs.width=W*2;cvs.height=H*2;ctx.scale(2,2);
    cvs.style.width=W+"px";cvs.style.height=H+"px";
    const vals=history.map(h=>h.value);
    const mn=Math.min(...vals),mx=Math.max(...vals),range=mx-mn||1;
    const yP=(v:number)=>5+(1-(v-mn)/range)*(H-10);
    const xP=(i:number)=>5+i*(W-10)/(vals.length-1);
    ctx.clearRect(0,0,W,H);
    const color=regime==="EXTREMO"?"#ef4444":regime==="NORMAL"?"#3b82f6":"#f59e0b";
    // Area fill
    ctx.beginPath();
    vals.forEach((v,i)=>{const x=xP(i),y=yP(v);if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);});
    ctx.lineTo(xP(vals.length-1),H);ctx.lineTo(xP(0),H);ctx.closePath();
    const grad=ctx.createLinearGradient(0,0,0,H);
    grad.addColorStop(0,color+"30");grad.addColorStop(1,color+"00");
    ctx.fillStyle=grad;ctx.fill();
    // Line
    ctx.beginPath();
    vals.forEach((v,i)=>{const x=xP(i),y=yP(v);if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);});
    ctx.strokeStyle=color;ctx.lineWidth=2;ctx.lineCap="round";ctx.lineJoin="round";ctx.stroke();
    // Dot
    const lastY=yP(vals[vals.length-1]);
    ctx.fillStyle=color;ctx.beginPath();ctx.arc(xP(vals.length-1),lastY,4,0,Math.PI*2);ctx.fill();
    ctx.strokeStyle="rgba(0,0,0,0.4)";ctx.lineWidth=1;ctx.stroke();
    // Arrow
    const isUp=vals[vals.length-1]>vals[0];
    ctx.fillStyle=isUp?"#10b981":"#ef4444";ctx.font="bold 14px sans-serif";
    ctx.textAlign="right";ctx.fillText(isUp?"?":"?",W-2,14);
  },[history,regime]);
  return <canvas ref={ref} style={{borderRadius:6,display:"block"}}/>;
}

// -- Compare Chart ----------------------------------------------------------

function CompareChart({symbols,prices}:{symbols:string[];prices:PriceData}) {
  const ref=useRef<HTMLCanvasElement>(null);
  const containerRef=useRef<HTMLDivElement>(null);
  const [w,setW]=useState(900);
  const VISIBLE=120;
  const COLORS=["#22c55e","#3b82f6","#f59e0b","#ef4444","#a78bfa","#06b6d4"];

  useEffect(()=>{
    const el=containerRef.current;if(!el)return;
    const obs=new ResizeObserver(entries=>{const{width}=entries[0].contentRect;if(width>0)setW(Math.floor(width));});
    obs.observe(el);return()=>obs.disconnect();
  },[]);

  useEffect(()=>{
    const cvs=ref.current;if(!cvs)return;
    const ctx=cvs.getContext("2d");if(!ctx)return;
    const H=320;
    cvs.width=w*2;cvs.height=H*2;ctx.scale(2,2);
    cvs.style.width=w+"px";cvs.style.height=H+"px";
    
    const pad={t:24,b:30,l:50,r:16};
    const cH=H-pad.t-pad.b;
    
    // Normalize all series to base 100
    const series:Record<string,number[]>={};
    let maxLen=0;
    for(const sym of symbols){
      const data=prices[sym];if(!data)continue;
      const slice=data.slice(-VISIBLE);
      const base=slice[0]?.close||1;
      series[sym]=slice.map(c=>(c.close/base)*100);
      maxLen=Math.max(maxLen,series[sym].length);
    }
    
    let allVals=Object.values(series).flat();
    if(!allVals.length)return;
    const mn=Math.min(...allVals),mx=Math.max(...allVals),range=mx-mn||1;
    const pMn=mn-range*.05,pMx=mx+range*.05;
    const yP=(v:number)=>pad.t+(1-(v-pMn)/(pMx-pMn))*cH;
    const xP=(i:number)=>pad.l+i*(w-pad.l-pad.r)/(maxLen-1);

    ctx.fillStyle=C.bg;ctx.fillRect(0,0,w,H);
    
    // Grid
    ctx.strokeStyle="rgba(148,163,184,.06)";
    for(let i=0;i<5;i++){const y=pad.t+i*(cH/4);ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();}
    
    // 100 line
    ctx.strokeStyle="rgba(148,163,184,.2)";ctx.setLineDash([4,4]);
    ctx.beginPath();ctx.moveTo(pad.l,yP(100));ctx.lineTo(w-pad.r,yP(100));ctx.stroke();
    ctx.setLineDash([]);
    
    // Y axis
    ctx.fillStyle=C.textMuted;ctx.font="10px monospace";ctx.textAlign="right";
    for(let i=0;i<5;i++){const v=pMx-(pMx-pMn)*i/4;ctx.fillText(v.toFixed(0),pad.l-6,pad.t+i*(cH/4)+4);}

    // Draw lines
    symbols.forEach((sym,si)=>{
      const data=series[sym];if(!data)return;
      ctx.strokeStyle=COLORS[si%COLORS.length];ctx.lineWidth=2;ctx.beginPath();
      data.forEach((v,i)=>{
        const x=xP(i),y=yP(v);
        if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);
      });
      ctx.stroke();
    });

    // Legend
    ctx.font="10px monospace";ctx.textAlign="left";
    let lx=pad.l;
    symbols.forEach((sym,si)=>{
      ctx.fillStyle=COLORS[si%COLORS.length];
      ctx.fillRect(lx,8,14,3);
      ctx.fillText(sym,lx+18,12);
      lx+=55;
    });
  },[symbols,prices,w]);

  return (
    <div ref={containerRef} style={{width:"100%"}}>
      <canvas ref={ref} style={{borderRadius:6,display:"block"}}/>
    </div>
  );
}
// -- COT Chart (Barchart-style: multi-line like barchart.com) --------------

function COTChart({history,type,width}:{history:COTHistoryEntry[];type:"legacy"|"disaggregated";width?:number}) {
  const ref=useRef<HTMLCanvasElement>(null);
  const containerRef=useRef<HTMLDivElement>(null);
  const [w,setW]=useState(width||900);
  const [hoverIdx,setHoverIdx]=useState<number|null>(null);

  useEffect(()=>{
    const el=containerRef.current;if(!el)return;
    const obs=new ResizeObserver(entries=>{const{width:cw}=entries[0].contentRect;if(cw>0)setW(Math.floor(cw));});
    obs.observe(el);return()=>obs.disconnect();
  },[]);

  useEffect(()=>{
    const cvs=ref.current;if(!cvs||!history.length)return;
    const ctx=cvs.getContext("2d");if(!ctx)return;
    const data=history.slice(-109);
    if(!data.length)return;

    const colors={comm:"#ef4444",noncomm:"#3b82f6",mm:"#22c55e",prod:"#f59e0b",swap:"#a78bfa"};

    // Build all series in one panel
    type Line={label:string;vals:number[];color:string;lw:number};
    let seriesLines:Line[]=[];

    if(type==="legacy"){
      const commNet=data.map(r=>(r.comm_long||0)-(r.comm_short||0));
      const ncNet=data.map(r=>(r.noncomm_long||0)-(r.noncomm_short||0));
      seriesLines=[
        {label:"Commercial Net",vals:commNet,color:colors.comm,lw:2.0},
        {label:"Non-Comm Net",vals:ncNet,color:colors.noncomm,lw:2.5},
      ];
    } else {
      const mm=data.map(r=>(r.managed_money_long||0)-(r.managed_money_short||0));
      const pr=data.map(r=>(r.producer_long||0)-(r.producer_short||0));
      const sw=data.map(r=>(r.swap_long||0)-(r.swap_short||0));
      seriesLines=[
        {label:"Managed Money",vals:mm,color:colors.mm,lw:2.5},
        {label:"Producer",vals:pr,color:colors.prod,lw:1.5},
        {label:"Swap Dealers",vals:sw,color:colors.swap,lw:1.5},
      ];
    }

    const H=220;
    cvs.width=w*2;cvs.height=H*2;ctx.scale(2,2);
    cvs.style.width=w+"px";cvs.style.height=H+"px";
    ctx.fillStyle=C.bg;ctx.fillRect(0,0,w,H);

    const pad={l:10,r:75,t:14,b:24};
    const chartW=w-pad.l-pad.r;
    const chartH=H-pad.t-pad.b;
    const n=data.length;
    const xC=(i:number)=>pad.l+(i+0.5)*chartW/n;

    // Shared scale across all lines
    const allVals=seriesLines.flatMap(s=>s.vals);
    const minV=Math.min(...allVals);const maxV=Math.max(...allVals);
    const range=maxV-minV||1;
    const yP=(v:number)=>pad.t+chartH-(v-minV)/range*chartH;

    // Grid lines
    const gridN=4;
    for(let g=0;g<=gridN;g++){
      const val=minV+(maxV-minV)*g/gridN;
      const y=yP(val);
      ctx.strokeStyle="rgba(148,163,184,.08)";ctx.lineWidth=0.5;
      ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();
    }

    // Zero line if range crosses zero
    if(minV<0 && maxV>0){
      const zy=yP(0);
      ctx.strokeStyle="rgba(148,163,184,.45)";ctx.lineWidth=1.0;ctx.setLineDash([4,3]);
      ctx.beginPath();ctx.moveTo(pad.l,zy);ctx.lineTo(w-pad.r,zy);ctx.stroke();
      ctx.setLineDash([]);
    }

    // Open Interest (background area, secondary axis)
    const oiVals=data.map(d=>d.open_interest||0);
    const oiValid=oiVals.filter(v=>v>0);
    if(oiValid.length>1){
      const oiMin=Math.min(...oiValid)*0.95;const oiMax=Math.max(...oiValid)*1.05;
      const toOIY=(v:number)=>pad.t+chartH-((v-oiMin)/(oiMax-oiMin||1))*chartH;
      // Area fill
      ctx.beginPath();
      let oiStarted=false;
      data.forEach((d,i)=>{if(!d.open_interest)return;const x=xC(i),y=toOIY(d.open_interest);if(!oiStarted){ctx.moveTo(x,y);oiStarted=true;}else ctx.lineTo(x,y);});
      ctx.lineTo(xC(data.length-1),pad.t+chartH);ctx.lineTo(xC(0),pad.t+chartH);ctx.closePath();
      ctx.fillStyle="rgba(148,163,184,.06)";ctx.fill();
      // OI line
      ctx.beginPath();oiStarted=false;
      data.forEach((d,i)=>{if(!d.open_interest)return;const x=xC(i),y=toOIY(d.open_interest);if(!oiStarted){ctx.moveTo(x,y);oiStarted=true;}else ctx.lineTo(x,y);});
      ctx.strokeStyle="rgba(148,163,184,.35)";ctx.lineWidth=1;ctx.setLineDash([]);ctx.stroke();
      // OI label
      const lastOI=data[data.length-1]?.open_interest||0;
      ctx.fillStyle="rgba(148,163,184,.5)";ctx.font="8px monospace";ctx.textAlign="right";
      ctx.fillText("OI: "+(lastOI/1000).toFixed(0)+"K",w-pad.r+70,pad.t+10);
    }

    // Draw each line with filled area to zero
    const zeroY=minV<0&&maxV>0?yP(0):yP(Math.min(0,minV));
    for(const line of seriesLines){
      // Filled area
      ctx.beginPath();
      ctx.moveTo(xC(0),zeroY);
      for(let i=0;i<n;i++) ctx.lineTo(xC(i),yP(line.vals[i]));
      ctx.lineTo(xC(n-1),zeroY);
      ctx.closePath();
      ctx.fillStyle=line.color.replace(")",",0.08)").replace("rgb","rgba");
      if(line.color.startsWith("#")){
        const hex=line.color;
        const r=parseInt(hex.slice(1,3),16);const g=parseInt(hex.slice(3,5),16);const b=parseInt(hex.slice(5,7),16);
        ctx.fillStyle="rgba("+r+","+g+","+b+",0.08)";
      }
      ctx.fill();

      // Line
      ctx.strokeStyle=line.color;ctx.lineWidth=line.lw;ctx.beginPath();
      for(let i=0;i<n;i++){
        const x=xC(i),y=yP(line.vals[i]);
        if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);
      }
      ctx.stroke();

      // Value badge on right
      const lastVal=line.vals[n-1];
      const valStr=(lastVal>=0?"+":"")+(Math.abs(lastVal)>=1000?(lastVal/1000).toFixed(0)+"K":lastVal.toFixed(0));
      const badgeY=yP(lastVal);
      ctx.fillStyle=line.color;
      const tw=ctx.measureText(valStr).width+12;
      ctx.fillRect(w-pad.r+4,badgeY-8,Math.max(tw,48),16);
      ctx.beginPath();ctx.moveTo(w-pad.r+4,badgeY);ctx.lineTo(w-pad.r,badgeY-4);ctx.lineTo(w-pad.r,badgeY+4);ctx.closePath();ctx.fill();
      ctx.fillStyle="#fff";ctx.font="bold 9px monospace";ctx.textAlign="left";
      ctx.fillText(valStr,w-pad.r+8,badgeY+3);

      // COT Index badge for Managed Money (disaggregated)
      if(type==="disaggregated"&&line.label==="Managed Money"&&line.vals.length>=20){
        const mmVals=line.vals;
        const mmMin=Math.min(...mmVals),mmMax=Math.max(...mmVals);
        if(mmMax!==mmMin){
          const cotIdx=((mmVals[mmVals.length-1]-mmMin)/(mmMax-mmMin))*100;
          const idxStr="IDX:"+cotIdx.toFixed(0);
          ctx.fillStyle=cotIdx>80?"#DC3C3C":cotIdx<20?"#DC3C3C":line.color;
          ctx.fillRect(w-pad.r+4,badgeY+10,42,13);
          ctx.fillStyle="#fff";ctx.font="bold 8px monospace";ctx.textAlign="left";
          ctx.fillText(idxStr,w-pad.r+8,badgeY+20);
        }
      }
    }

    // Hover
    if(hoverIdx!==null&&hoverIdx>=0&&hoverIdx<n){
      const hx=xC(hoverIdx);
      ctx.strokeStyle="rgba(148,163,184,.3)";ctx.lineWidth=1;ctx.setLineDash([3,3]);
      ctx.beginPath();ctx.moveTo(hx,pad.t);ctx.lineTo(hx,H-pad.b);ctx.stroke();
      ctx.setLineDash([]);
      // Dots on each line
      for(const line of seriesLines){
        const hVal=line.vals[hoverIdx];
        ctx.fillStyle=line.color;
        ctx.beginPath();ctx.arc(hx,yP(hVal),3.5,0,Math.PI*2);ctx.fill();
        ctx.strokeStyle=C.bg;ctx.lineWidth=1;ctx.stroke();
      }
      // Tooltip box
      const hRec=data[hoverIdx];
      const hasMmPct=type==="disaggregated"&&hRec?.mm_long_pct!=null;
      const hasOI=!!hRec?.open_interest;
      const extraLines=(hasMmPct?1:0)+(hasOI?1:0);
      const tooltipW=170;const tooltipX=hoverIdx>n/2?hx-tooltipW-10:hx+10;
      const tooltipH=14+seriesLines.length*14+extraLines*12+6;
      ctx.fillStyle="rgba(22,27,34,.92)";
      ctx.beginPath();ctx.roundRect(tooltipX,pad.t+2,tooltipW,tooltipH,4);ctx.fill();
      ctx.strokeStyle="rgba(148,163,184,.2)";ctx.lineWidth=0.5;
      ctx.beginPath();ctx.roundRect(tooltipX,pad.t+2,tooltipW,tooltipH,4);ctx.stroke();
      ctx.fillStyle=C.textDim;ctx.font="bold 9px monospace";ctx.textAlign="left";
      ctx.fillText(hRec.date,tooltipX+6,pad.t+14);
      let tLineY=pad.t+14;
      seriesLines.forEach((line,li)=>{
        tLineY+=14;
        const hVal=line.vals[hoverIdx];
        const vs=(hVal>=0?"+":"")+(Math.abs(hVal)>=1000?(hVal/1000).toFixed(1)+"K":hVal.toFixed(0));
        ctx.fillStyle=line.color;ctx.font="8px monospace";
        ctx.fillText("● "+line.label+": "+vs,tooltipX+6,tLineY);
      });
      if(hasMmPct){
        tLineY+=12;
        ctx.fillStyle="rgba(148,163,184,.7)";ctx.font="7px monospace";
        ctx.fillText("  MM: L"+hRec.mm_long_pct.toFixed(1)+"% / S"+hRec.mm_short_pct.toFixed(1)+"% do OI",tooltipX+6,tLineY);
      }
      if(hasOI){
        tLineY+=12;
        ctx.fillStyle="rgba(148,163,184,.5)";ctx.font="7px monospace";
        ctx.fillText("  OI: "+(hRec.open_interest/1000).toFixed(0)+"K",tooltipX+6,tLineY);
      }
    }

    // X axis dates
    ctx.fillStyle=C.textMuted;ctx.font="8px monospace";ctx.textAlign="center";
    const dateStep=Math.max(1,Math.floor(n/8));
    data.forEach((d,i)=>{
      if(i%dateStep===0||i===n-1){
        ctx.fillText(d.date.slice(2,7),xC(i),H-pad.b+12);
      }
    });

    // Legend at top
    let lx=pad.l+4;
    for(const line of seriesLines){
      ctx.fillStyle=line.color;ctx.font="bold 8px monospace";
      ctx.fillRect(lx,2,8,8);
      ctx.fillStyle=C.textDim;ctx.font="8px sans-serif";ctx.textAlign="left";
      ctx.fillText(line.label,lx+11,10);
      lx+=ctx.measureText(line.label).width+22;
    }

  },[history,type,w,hoverIdx]);

  const handleMouse=(e:React.MouseEvent)=>{
    const rect=ref.current?.getBoundingClientRect();if(!rect)return;
    const x=e.clientX-rect.left;
    const data=history.slice(-109);
    const pad={l:10,r:75};
    const bW=(w-pad.l-pad.r)/data.length;
    const i=Math.floor((x-pad.l)/bW);
    if(i>=0&&i<data.length)setHoverIdx(i);else setHoverIdx(null);
  };

  return (
    <div ref={containerRef} style={{width:"100%"}}>
      <canvas ref={ref} style={{borderRadius:6,display:"block",cursor:"crosshair"}}
        onMouseMove={handleMouse} onMouseLeave={()=>setHoverIdx(null)}/>
    </div>
  );
}
// -- Main Dashboard ---------------------------------------------------------

export default function Dashboard() {
  // State
  const [prices,setPrices] = useState<PriceData|null>(null);
  const [season,setSeason] = useState<SeasonData|null>(null);
  const [spreads,setSpreads] = useState<SpreadsData|null>(null);
  const [eiaData,setEiaData] = useState<EIAData|null>(null);
  const [stocks,setStocks] = useState<StocksData|null>(null);
  const [cot,setCot] = useState<COTData|null>(null);
  const [reading,setReading] = useState<DailyReading|null>(null);
  const [futures,setFutures] = useState<FuturesData|null>(null);
  const [contractHist,setContractHist] = useState<any>(null);
  const [physical,setPhysical] = useState<any>(null);
  const [physIntl,setPhysIntl] = useState<any>(null);
  const [selected,setSelected] = useState("ZC");
  const [stockSelected,setStockSelected] = useState<string>("ZC");
  const [tab,setTab] = useState<Tab>("Visão Geral");
  const [viewMode,setViewMode] = useState<"commodity"|"global"|"intel">("commodity");
  const [loading,setLoading] = useState(true);
  const [errors,setErrors] = useState<string[]>([]);
  const [cmp1,setCmp1] = useState<string>("ZCH26");
  const [cmp2,setCmp2] = useState<string>("ZSH26");
  const [cmp3,setCmp3] = useState<string>("");
  const compareSyms = [cmp1,cmp2,cmp3].filter(s=>s!=="");
  const [fwdSyms,setFwdSyms] = useState<string[]>(["ZC"]);
  const [portfolio,setPortfolio] = useState<any>(null);
  const [greeksData,setGreeksData] = useState<any>(null);
  const [lastIbkrRefresh,setLastIbkrRefresh] = useState<string|null>(null);
  const [ibkrRefreshing,setIbkrRefreshing] = useState(false);
  const [pipeRefresh,setPipeRefresh] = useState(false);
  const [pipeMsg,setPipeMsg] = useState("");
  const [calendar,setCalendar] = useState<any>(null);
  const [news,setNews] = useState<any>(null);
  const [grData, setGrData] = useState<any>(null);
  const [payoffUnd, setPayoffUnd] = useState("CL");
  const [payoffExpiry, setPayoffExpiry] = useState("all");
  const [optionsChain, setOptionsChain] = useState<any>(null);
  const [psdData, setPsdData] = useState<any>(null);
  const [physicalBrData, setPhysicalBrData] = useState<any>(null);
  const [imeaData, setImeaData] = useState<any>(null);
  const [commodityDna, setCommodityDna] = useState<any>(null);
  const [showStrategyBuilder, setShowStrategyBuilder] = useState(false);
  const [sbUnderlying, setSbUnderlying] = useState("CL");
  const [sbDirection, setSbDirection] = useState<"PUT"|"CALL">("PUT");
  const [sbStrike, setSbStrike] = useState("");
  const [sbDte, setSbDte] = useState("45");
  const [sbQty, setSbQty] = useState("22");
  const [sbStructure, setSbStructure] = useState("butterfly");
  const [sbNote, setSbNote] = useState("");
  const [sbLoading, setSbLoading] = useState(false);
  const [sbResult, setSbResult] = useState<string|null>(null);
  // INTEL state
  const [weatherData, setWeatherData] = useState<any>(null);
  const [bcbData, setBcbData] = useState<any>(null);
  const [intelSentiment, setIntelSentiment] = useState<{text:string;time:string}>(()=>{
    if(typeof window!=="undefined"){try{const s=localStorage.getItem("agrimacro_intel_sentiment");if(s)return JSON.parse(s);}catch{}}
    return {text:"",time:""};
  });
  const [intelNews, setIntelNews] = useState<{text:string;time:string}>(()=>{
    if(typeof window!=="undefined"){try{const s=localStorage.getItem("agrimacro_intel_news");if(s)return JSON.parse(s);}catch{}}
    return {text:"",time:""};
  });
  const [macroIndicators, setMacroIndicators] = useState<any>(null);
  const [cropProgressData, setCropProgressData] = useState<any>(null);
  const [exportActivityData, setExportActivityData] = useState<any>(null);
  const [droughtData, setDroughtData] = useState<any>(null);
  const [fertilizerData, setFertilizerData] = useState<any>(null);
  const [intelFrame, setIntelFrame] = useState<any>(null);
  const [priceValidation, setPriceValidation] = useState<any>(null);
  const [bottleneckData, setBottleneckData] = useState<any>(null);
  const [googleTrends, setGoogleTrends] = useState<any>(null);
  const [fedwatch, setFedwatch] = useState<any>(null);
  const [intelSynthesis, setIntelSynthesis] = useState<any>(null);
  const [correlations, setCorrelations] = useState<any>(null);
  const [livestockPsdData, setLivestockPsdData] = useState<any>(null);
  const [livestockWeeklyData, setLivestockWeeklyData] = useState<any>(null);
  const [dxProcessed, setDxProcessed] = useState<any[]|null>(null);
  const [intelCouncil, setIntelCouncil] = useState<{text:string;time:string}|null>(null);
  const [intelCouncilLoading, setIntelCouncilLoading] = useState(false);
  const [councilStage, setCouncilStage] = useState("");
  const [councilHistory, setCouncilHistory] = useState<Array<{text:string;time:string}>>(()=>{
    if(typeof window!=="undefined"){try{const s=localStorage.getItem("agrimacro_council_history");if(s)return JSON.parse(s);}catch{}}
    return [];
  });
  const [sentimentDraft, setSentimentDraft] = useState("");
  const [newsDraft, setNewsDraft] = useState("");
  const [grokSentiment, setGrokSentiment] = useState<any>(null);
  const [grokNews, setGrokNews] = useState<any>(null);
  // Strategy state
  const [strategyInput, setStrategyInput] = useState("");
  const [strategyResult, setStrategyResult] = useState<{text:string;thesis:string;time:string}|null>(null);
  const [strategyLoading, setStrategyLoading] = useState(false);
  const [strategyHistory, setStrategyHistory] = useState<{text:string;thesis:string;time:string}[]>(()=>{
    if(typeof window!=="undefined"){try{const s=localStorage.getItem("agrimacro_strategy_history");if(s)return JSON.parse(s);}catch{}}
    return [];
  });
  const [strategyCollapsed, setStrategyCollapsed] = useState<Record<string,boolean>>({});
  const [strategyRefineMode, setStrategyRefineMode] = useState(false);
  const [strategyConvoHistory, setStrategyConvoHistory] = useState<{role:string;content:string}[]>([]);
  const [paritiesData, setParitiesData] = useState<any>(null);
  const [conabData, setConabData] = useState<any>(null);
  const [bilateralData, setBilateralData] = useState<any>(null);
  const [cotDeltaView, setCotDeltaView] = useState<'overview'|'detail'>('overview');
  const [selectedCotSym, setSelectedCotSym] = useState<string>('');

  // Load data
  useEffect(()=>{
    const errs:string[]=[];
    Promise.all([
      fetch("/data/raw/price_history.json").then(r=>{if(!r.ok)throw new Error("prices");return r.json();}).then(setPrices).catch(()=>errs.push("prices")),
      fetch("/data/processed/seasonality.json").then(r=>{if(!r.ok)throw new Error("season");return r.json();}).then(setSeason).catch(()=>errs.push("season")),
      fetch("/data/processed/spreads.json").then(r=>{if(!r.ok)throw new Error("spreads");return r.json();}).then(setSpreads).catch(()=>errs.push("spreads")),
      fetch("/data/processed/eia_data.json").then(r=>{if(!r.ok)throw new Error("eia");return r.json();}).then(setEiaData).catch(()=>errs.push("eia")),
      fetch("/data/processed/stocks_watch.json").then(r=>{if(!r.ok)throw new Error("stocks");return r.json();}).then(setStocks).catch(()=>errs.push("stocks")),
      fetch("/data/processed/cot.json").then(r=>{if(!r.ok)throw new Error("cot");return r.json();}).then(setCot).catch(()=>errs.push("cot")),
      fetch("/data/processed/daily_reading.json").then(r=>{if(!r.ok)throw new Error("reading");return r.json();}).then(setReading).catch(()=>errs.push("reading")),
      fetch("/data/processed/physical.json").then(r=>{if(!r.ok)throw new Error("physical");return r.json();}).then(setPhysical).catch(()=>errs.push("physical")),
      fetch("/data/processed/physical_intl.json").then(r=>{if(!r.ok)throw new Error("physIntl");return r.json();}).then(setPhysIntl).catch(()=>errs.push("physIntl")),
      fetch("/data/processed/futures_contracts.json").then(r=>{if(!r.ok)throw new Error("futures");return r.json();}).then(setFutures).catch(()=>errs.push("futures")),
      fetch("/data/processed/contract_history.json").then(r=>r.json()).then(d=>setContractHist(d)).catch(()=>console.warn("No contract history")),
          fetch("/data/processed/calendar.json").then(r=>r.json()).then(setCalendar).catch(()=>console.warn("No calendar")),
      fetch("/data/processed/news.json").then(r=>r.json()).then(setNews).catch(()=>console.warn("No news")),
      fetch("/data/processed/ibkr_portfolio.json").then(r=>r.json()).then(d=>{setPortfolio(d);setLastIbkrRefresh(d.generated_at||d.export_timestamp||new Date().toISOString());}).catch(()=>console.warn("No IBKR data")),
      fetch("/api/ibkr-greeks").then(r=>r.json()).then(d=>{if(d?.portfolio_greeks) setGreeksData(d);}).catch(()=>{}),
      fetch("/data/processed/options_chain.json").then(r=>r.json()).then(setOptionsChain).catch(()=>console.warn("No options chain")),
      fetch("/data/processed/weather_agro.json").then(r=>r.json()).then(setWeatherData).catch(()=>console.warn("No weather data")),
      fetch("/data/processed/bcb_data.json").then(r=>r.json()).then(setBcbData).catch(()=>console.warn("No BCB data")),
      fetch("/data/processed/macro_indicators.json").then(r=>r.json()).then(setMacroIndicators).catch(()=>console.warn("No macro indicators")),
      fetch("/data/processed/google_trends.json").then(r=>r.json()).then(setGoogleTrends).catch(()=>console.warn("No Google Trends")),
      fetch("/data/processed/fedwatch.json").then(r=>r.json()).then(setFedwatch).catch(()=>console.warn("No FedWatch")),
      fetch("/data/processed/intel_synthesis.json").then(r=>r.json()).then(setIntelSynthesis).catch(()=>console.warn("No Intel Synthesis")),
      fetch("/data/processed/correlations.json").then(r=>r.json()).then(setCorrelations).catch(()=>console.warn("No Correlations")),
      fetch("/data/processed/price_history.json").then(r=>r.json()).then(d=>{if(d?.DX) setDxProcessed(d.DX);}).catch(()=>{}),
      fetch("/data/processed/grok_sentiment.json").then(r=>r.json()).then(d=>{if(!d?.is_fallback) setGrokSentiment(d);}).catch(()=>{}),
      fetch("/data/processed/grok_news.json").then(r=>r.json()).then(d=>{if(!d?.is_fallback) setGrokNews(d);}).catch(()=>{}),
      fetch("/data/processed/crop_progress.json").then(r=>r.json()).then(d=>{if(!d?.is_fallback) setCropProgressData(d);}).catch(()=>console.warn("No crop progress")),
      fetch("/data/processed/export_activity.json").then(r=>r.json()).then(d=>{if(!d?.is_fallback) setExportActivityData(d);}).catch(()=>console.warn("No export activity")),
      fetch("/data/processed/drought_monitor.json").then(r=>r.json()).then(d=>{if(!d?.is_fallback) setDroughtData(d);}).catch(()=>console.warn("No drought data")),
      fetch("/data/processed/fertilizer_prices.json").then(r=>r.json()).then(d=>{if(!d?.is_fallback) setFertilizerData(d);}).catch(()=>console.warn("No fertilizer data")),
      fetch("/data/processed/intelligence_frame.json").then(r=>r.json()).then(setIntelFrame).catch(()=>console.warn("No intelligence frame")),
      fetch("/data/processed/commodity_dna.json").then(r=>r.json()).then(setCommodityDna).catch(()=>console.warn("No commodity DNA")),
      fetch("/data/processed/price_validation.json").then(r=>r.json()).then(setPriceValidation).catch(()=>console.warn("No price validation")),
      fetch("/data/processed/bottleneck.json").then(r=>r.json()).then(d=>{if(d?.commodities)setBottleneckData(d.commodities);}).catch(()=>console.warn("No bottleneck")),
    ]).finally(()=>{setErrors(errs);setLoading(false);});
  },[]);

  // Load grain_ratios for COP annotations
  useEffect(() => { fetch("/data/processed/grain_ratios.json").then(r=>r.json()).then(setGrData).catch(()=>{}) }, []);

  // Load additional data for Intel enrichment
  useEffect(() => {
    fetch("/data/processed/psd_ending_stocks.json").then(r=>r.json()).then(setPsdData).catch(()=>{});
    fetch("/data/processed/physical_br.json").then(r=>r.json()).then(setPhysicalBrData).catch(()=>{});
    fetch("/data/processed/imea_soja.json").then(r=>r.json()).then(setImeaData).catch(()=>{});
    fetch("/data/processed/parities.json").then(r=>r.json()).then(setParitiesData).catch(()=>{});
    fetch("/data/processed/conab_data.json").then(r=>r.json()).then(setConabData).catch(()=>{});
    fetch("/data/bilateral/basis_temporal.json").then(r=>r.json()).then(setBilateralData).catch(()=>{});
    fetch("/data/processed/livestock_psd.json").then(r=>r.json()).then(setLivestockPsdData).catch(()=>{});
    fetch("/data/processed/livestock_weekly.json").then(r=>r.json()).then(setLivestockWeeklyData).catch(()=>{});
  }, []);

  // IBKR Auto-Refresh
  const refreshIbkr = async () => {
    if(ibkrRefreshing) return;
    setIbkrRefreshing(true);
    try {
      const res = await fetch("/api/refresh-ibkr", {method:"POST"});
      const data = await res.json();
      if(data.status === "ok") {
        const p = await fetch("/data/processed/ibkr_portfolio.json?t="+Date.now());
        if(p.ok){ const d = await p.json(); setPortfolio(d); setLastIbkrRefresh(new Date().toISOString()); }
        const pr = await fetch("/data/raw/price_history.json?t="+Date.now());
        if(pr.ok){ const d = await pr.json(); setPrices(d); }
      }
    } catch(e){ console.warn("IBKR refresh failed",e); }
    setIbkrRefreshing(false);
  };
  const refreshPipeline = async () => {
    if(pipeRefresh) return;
    setPipeRefresh(true); setPipeMsg("Rodando...");
    try {
      const res = await fetch("/api/refresh-pipeline", {method:"POST"});
      const d = await res.json();
      if(d.status==="ok"){ setPipeMsg(d.summary); setTimeout(()=>window.location.reload(), 2000); }
      else if(d.status==="skipped"){ setPipeMsg("Aguarde 2min"); }
      else { setPipeMsg("Erro"); }
    } catch(e){ setPipeMsg("Falha"); }
    setPipeRefresh(false);
  };

  useEffect(()=>{
    const interval = setInterval(()=>{ refreshIbkr(); }, 5*60*1000);
    return ()=>clearInterval(interval);
  },[]);

  // Helpers
  const lastDate = prices&&prices[selected]?.length ? prices[selected][prices[selected].length-1].date : "--";
  const pipelineOk = !loading && errors.length === 0;

  const ibkrTime = lastIbkrRefresh ? new Date(lastIbkrRefresh).toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"}) : "--:--";

  const getPrice = (sym:string):number|null => {
    if(!prices||!prices[sym]||!prices[sym].length)return null;
    return prices[sym][prices[sym].length-1].close;
  };
  
  const getChange = (sym:string):{abs:number;pct:number}|null => {
    if(!prices||!prices[sym]||prices[sym].length<2)return null;
    const arr=prices[sym];const last=arr[arr.length-1];const prev=arr[arr.length-2];
    return {abs:last.close-prev.close,pct:((last.close-prev.close)/prev.close)*100};
  };
  
  const getSeasonDev = (sym:string):{current:number;avg:number;dev:number}|null => {
    if(!season||!season[sym])return null;
    const e=season[sym];
    const curr=e.series["current"];
    const avg=e.series["average"];
    if(!curr?.length||!avg?.length)return null;
    const lastCurr=curr[curr.length-1].close;
    const lastDay=curr[curr.length-1].day;
    let closest=avg[0];
    for(const aa of avg){if(Math.abs(aa.day-lastDay)<Math.abs(closest.day-lastDay))closest=aa;}
    const dev=((lastCurr-closest.close)/closest.close)*100;
    return {current:lastCurr,avg:closest.close,dev};
  };

  const stocksList = stocks ? Object.values(stocks.commodities) : [];
  const spreadList = spreads ? Object.entries(spreads.spreads).map(([k,v])=>({key:k,...v})) : [];
  const hasCOT = (sym:string) => cot?.commodities?.[sym]?.legacy?.history?.length || cot?.commodities?.[sym]?.disaggregated?.history?.length;
  // -- Tab: Gráfico + COT --------------------------------------------------
  const cotComm = cot?.commodities?.[selected];
  const cotLegacy = cotComm?.legacy;
  const cotDisagg = cotComm?.disaggregated;

  const renderGraficoCOT = () => (
    <div>
      {prices && prices[selected] ? (
        <LightweightChart
          bars={prices[selected]}
          symbol={selected}
          height={420}
          showRSI={true}
          cotIndex={cot?.commodities?.[selected]?.disaggregated?.cot_index ?? undefined}
          basis={paritiesData?.parities?.brl_competitiveness?.value ?? undefined}
        />
      ) : (
        <DataPlaceholder title="Sem dados de pre\u00e7o" detail={`${selected} n\u00e3o encontrado em price_history.json`} />
      )}
      <div style={{marginTop:12}}>
        {hasCOT(selected) ? (
          <>
            <div style={{fontSize:11,fontWeight:700,color:C.textDim,padding:"8px 0 4px",borderTop:`1px solid ${C.border}`}}>
              COT -- LEGACY (Commercial vs Non-Commercial)
            </div>
            {cotLegacy?.history?.length ? (
              <COTChart history={cotLegacy.history} type="legacy" />
            ) : null}
            <div style={{fontSize:11,fontWeight:700,color:C.textDim,padding:"8px 0 4px",borderTop:`1px solid ${C.border}`,marginTop:4}}>
              COT -- DISAGGREGATED (Managed Money / Producer / Swap)
            </div>
            {cotDisagg?.history?.length ? (
              <COTChart history={cotDisagg.history} type="disaggregated" />
            ) : null}
            {/* Summary cards */}
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(140px,1fr))",gap:10,marginTop:12}}>
              {cotLegacy?.latest && (()=>{
                const l=cotLegacy.latest;
                const commNet=l.comm_net||0;const ncNet=l.noncomm_net||0;
                return <>
                  <div style={{padding:10,background:C.panelAlt,borderRadius:6,borderLeft:`3px solid #ef4444`}}>
                    <div style={{fontSize:8,color:C.textMuted}}>COMMERCIAL NET</div>
                    <div style={{fontSize:16,fontWeight:800,fontFamily:"monospace",color:commNet>=0?C.green:C.red}}>{(commNet/1000).toFixed(1)}K</div>
                  </div>
                  <div style={{padding:10,background:C.panelAlt,borderRadius:6,borderLeft:`3px solid #3b82f6`}}>
                    <div style={{fontSize:8,color:C.textMuted}}>NON-COMM NET</div>
                    <div style={{fontSize:16,fontWeight:800,fontFamily:"monospace",color:ncNet>=0?C.green:C.red}}>{(ncNet/1000).toFixed(1)}K</div>
                  </div>
                  <div style={{padding:10,background:C.panelAlt,borderRadius:6,borderLeft:`3px solid ${C.border}`}}>
                    <div style={{fontSize:8,color:C.textMuted}}>OPEN INTEREST</div>
                    <div style={{fontSize:16,fontWeight:800,fontFamily:"monospace",color:C.text}}>{((l.open_interest||0)/1000).toFixed(0)}K</div>
                  </div>
                </>;
              })()}
              {cotDisagg?.latest && (()=>{
                const d=cotDisagg.latest;
                return <>
                  <div style={{padding:10,background:C.panelAlt,borderRadius:6,borderLeft:`3px solid #22c55e`}}>
                    <div style={{fontSize:8,color:C.textMuted}}>MANAGED MONEY</div>
                    <div style={{fontSize:16,fontWeight:800,fontFamily:"monospace",color:(d.managed_money_net||0)>=0?C.green:C.red}}>{((d.managed_money_net||0)/1000).toFixed(1)}K</div>
                  </div>
                  <div style={{padding:10,background:C.panelAlt,borderRadius:6,borderLeft:`3px solid #f59e0b`}}>
                    <div style={{fontSize:8,color:C.textMuted}}>PRODUCER</div>
                    <div style={{fontSize:16,fontWeight:800,fontFamily:"monospace",color:(d.producer_net||0)>=0?C.green:C.red}}>{((d.producer_net||0)/1000).toFixed(1)}K</div>
                  </div>
                </>;
              })()}
            </div>
            {/* COT DELTA ANALYSIS */}
            {(()=>{
              const da = cotDisagg?.delta_analysis;
              if (!da) return null;
              const allComms = cot?.commodities || {};

              const GaugeChart = ({ value, color }: { value: number; color: string }) => {
                const angle = (value / 100) * 180 - 90;
                const rad = (angle * Math.PI) / 180;
                const cx = 100, cy = 100, r = 70;
                const x = cx + r * Math.cos(rad);
                const y = cy + r * Math.sin(rad);
                return (
                  <svg viewBox="0 0 200 120" style={{width:'100%',maxWidth:200}}>
                    <path d="M 30 100 A 70 70 0 0 1 170 100"
                      fill="none" stroke="#1e3a4a" strokeWidth={14}
                      strokeLinecap="round"/>
                    <path d={`M 30 100 A 70 70 0 ${value > 50 ? 1 : 0} 1 ${x} ${y}`}
                      fill="none" stroke={color} strokeWidth={14}
                      strokeLinecap="round"/>
                    <text x="100" y="95" textAnchor="middle"
                      fontSize="22" fontWeight="bold" fill={color}
                      fontFamily="monospace">
                      {value}%
                    </text>
                    <text x="100" y="112" textAnchor="middle"
                      fontSize="9" fill="#64748b">
                      PROB. REVERS\u00c3O
                    </text>
                  </svg>
                );
              };

              const DeltaBars = ({ deltas }: { deltas: number[] }) => {
                const maxVal = Math.max(...deltas.map(Math.abs), 1000);
                const h = 80, w = 240, pad = 8;
                const barW = (w - pad*2) / deltas.length - 3;
                const midY = h / 2;
                return (
                  <svg viewBox={`0 0 ${w} ${h}`} style={{width:'100%',maxWidth:w}}>
                    <line x1={pad} y1={midY} x2={w-pad} y2={midY}
                      stroke="#1e3a4a" strokeWidth={1}
                      strokeDasharray="3,3"/>
                    {deltas.map((d, i) => {
                      const barH = Math.abs(d) / maxVal * (midY - 4);
                      const xPos = pad + i * (barW + 3);
                      const isPos = d >= 0;
                      const yPos = isPos ? midY - barH : midY;
                      const barColor = isPos ? '#00C878' : '#DC3C3C';
                      return (
                        <g key={i}>
                          <rect x={xPos} y={yPos} width={barW} height={Math.max(barH,1)}
                            fill={barColor} opacity={0.8} rx={1}/>
                          <text x={xPos + barW/2}
                                y={isPos ? yPos - 2 : yPos + barH + 8}
                                textAnchor="middle" fontSize={6}
                                fill={barColor} fontFamily="monospace">
                            {d > 0 ? '+' : ''}{(d/1000).toFixed(0)}k
                          </text>
                        </g>
                      );
                    })}
                  </svg>
                );
              };

              // Build overview grid data
              const overviewSyms = Object.entries(allComms)
                .filter(([,v]) => v.disaggregated?.delta_analysis)
                .map(([sym, v]) => ({sym, name: v.name, da: v.disaggregated!.delta_analysis!}));

              const detailSym = selectedCotSym || selected;
              const detailData = allComms[detailSym]?.disaggregated?.delta_analysis;

              return (
                <div style={{marginTop:16,borderTop:`1px solid ${C.border}`,paddingTop:12}}>
                  <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',marginBottom:10}}>
                    <div style={{fontSize:11,fontWeight:700,color:C.textDim}}>
                      COT DELTA ANALYSIS \u2014 SINAIS DE REVERS\u00c3O
                    </div>
                    <div style={{display:'flex',gap:4}}>
                      {(['overview','detail'] as const).map(v => (
                        <button key={v} onClick={()=>setCotDeltaView(v)} style={{
                          padding:'3px 10px',fontSize:9,fontWeight:cotDeltaView===v?700:500,
                          color:cotDeltaView===v?'#DCB432':'#64748b',
                          background:cotDeltaView===v?'rgba(220,180,50,.12)':'transparent',
                          border:`1px solid ${cotDeltaView===v?'#DCB432':'#1e3a4a'}`,
                          borderRadius:4,cursor:'pointer'
                        }}>{v === 'overview' ? 'Overview' : 'Detalhe'}</button>
                      ))}
                    </div>
                  </div>

                  {cotDeltaView === 'overview' ? (
                    <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fill,minmax(280px,1fr))',gap:10}}>
                      {overviewSyms.map(({sym,name,da:d}) => {
                        const sig = d.signals[0];
                        const dirColor = d.dominant_direction==='BEARISH'?'#DC3C3C':d.dominant_direction==='BULLISH'?'#00C878':'#64748b';
                        const deltaMax = 50000;
                        const deltaPct = Math.min(Math.abs(d.current_delta)/deltaMax*100, 100);
                        const streak = d.neg_streak || d.pos_streak;
                        return (
                          <div key={sym} onClick={()=>{setSelectedCotSym(sym);setCotDeltaView('detail');}}
                            style={{
                              background:C.panelAlt,border:`1px solid #1e3a4a`,
                              borderLeft:`3px solid ${dirColor}`,borderRadius:8,
                              padding:12,cursor:'pointer',transition:'all .15s',
                            }}>
                            <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:6}}>
                              <div>
                                <span style={{fontSize:12,fontWeight:700,color:C.text}}>{sym}</span>
                                <span style={{fontSize:9,color:C.textMuted,marginLeft:6}}>{name}</span>
                              </div>
                              <div style={{fontSize:9,fontWeight:700,color:dirColor}}>
                                {sig.icon} {d.dominant_direction}
                              </div>
                            </div>
                            <div style={{height:1,background:'#1e3a4a',marginBottom:8}}/>
                            <div style={{display:'flex',alignItems:'center',gap:8,marginBottom:6}}>
                              <span style={{fontSize:10,color:C.textMuted,width:50}}>Delta:</span>
                              <span style={{fontSize:12,fontWeight:700,fontFamily:'monospace',color:d.current_delta>=0?'#00C878':'#DC3C3C'}}>
                                {d.current_delta>=0?'+':''}{(d.current_delta/1000).toFixed(1)}K
                              </span>
                              <div style={{flex:1,height:6,background:'#1e3a4a',borderRadius:3,position:'relative',overflow:'hidden'}}>
                                <div style={{
                                  position:'absolute',
                                  [d.current_delta>=0?'left':'right']:'50%',
                                  top:0,height:'100%',borderRadius:3,
                                  width:`${deltaPct/2}%`,
                                  background:d.current_delta>=0?'#00C878':'#DC3C3C',opacity:0.8
                                }}/>
                              </div>
                              <span style={{fontSize:9,fontFamily:'monospace',color:dirColor}}>
                                {d.reversal_score}%
                              </span>
                            </div>
                            {streak > 0 && (
                              <div style={{fontSize:9,color:C.textMuted,marginBottom:4}}>
                                {d.neg_streak > 0 ? `Saindo h\u00e1 ${streak} semanas` : `Entrando h\u00e1 ${streak} semanas`}
                              </div>
                            )}
                            <div style={{fontSize:9,fontWeight:600,color:sig.color}}>
                              {sig.label}
                            </div>
                            <div style={{fontSize:8,color:'#64748b',marginTop:2}}>{sig.description}</div>
                          </div>
                        );
                      })}
                    </div>
                  ) : (
                    /* Detail view */
                    detailData ? (
                      <div style={{background:C.panelAlt,border:`1px solid #1e3a4a`,borderRadius:8,padding:16}}>
                        <div style={{display:'flex',alignItems:'center',gap:8,marginBottom:12}}>
                          <button onClick={()=>setCotDeltaView('overview')} style={{
                            background:'transparent',border:'1px solid #1e3a4a',borderRadius:4,
                            color:'#64748b',fontSize:9,padding:'2px 8px',cursor:'pointer'
                          }}>\u2190 Voltar</button>
                          <span style={{fontSize:14,fontWeight:700,color:C.text}}>{detailSym}</span>
                          <span style={{fontSize:10,color:C.textMuted}}>{allComms[detailSym]?.name}</span>
                          <span style={{fontSize:10,fontWeight:700,marginLeft:'auto',
                            color:detailData.dominant_direction==='BEARISH'?'#DC3C3C':detailData.dominant_direction==='BULLISH'?'#00C878':'#64748b'
                          }}>
                            {detailData.dominant_direction}
                          </span>
                        </div>

                        <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:16}}>
                          {/* Left: Delta bars + metrics */}
                          <div>
                            <div style={{fontSize:9,fontWeight:700,color:C.textMuted,marginBottom:6}}>
                              DELTA SEMANAL (\u00daltimas {detailData.deltas_8w.length + 1} semanas)
                            </div>
                            <DeltaBars deltas={detailData.deltas_8w} />
                            <div style={{display:'grid',gridTemplateColumns:'1fr 1fr 1fr',gap:8,marginTop:12}}>
                              <div style={{padding:8,background:'#0E1A24',borderRadius:4}}>
                                <div style={{fontSize:7,color:'#64748b',marginBottom:2}}>COT INDEX</div>
                                <div style={{display:'flex', gap:8, alignItems:'center'}}>
                                  {[
                                    {label:'156w', val: detailData.cot_index},
                                    {label:'52w',  val: detailData.cot_index_52w},
                                    {label:'26w',  val: detailData.cot_index_26w},
                                  ].map(({label, val}) => {
                                    if (val === undefined || val === null) return null;
                                    const color = val >= 80 ? '#DC3C3C' : val <= 20 ? '#00C878' :
                                                  val >= 70 ? '#DCB432' : val <= 30 ? '#3b82f6' : '#64748b';
                                    return (
                                      <div key={label} style={{
                                        textAlign:'center', padding:'4px 8px',
                                        background: color + '15',
                                        border: `1px solid ${color}33`,
                                        borderRadius:4
                                      }}>
                                        <div style={{fontSize:8, color:'#64748b', marginBottom:2}}>
                                          {label}
                                        </div>
                                        <div style={{fontSize:14, fontWeight:700,
                                                     fontFamily:'monospace', color}}>
                                          {typeof val === 'number' ? val.toFixed(0) : val}
                                        </div>
                                      </div>
                                    );
                                  })}
                                </div>
                              </div>
                              <div style={{padding:8,background:'#0E1A24',borderRadius:4}}>
                                <div style={{fontSize:7,color:'#64748b'}}>OI TREND</div>
                                <div style={{fontSize:14,fontWeight:700,fontFamily:'monospace',
                                  color:detailData.oi_trend_pct<0?'#DC3C3C':'#00C878'
                                }}>{detailData.oi_trend_pct>0?'+':''}{detailData.oi_trend_pct}%</div>
                              </div>
                              <div style={{padding:8,background:'#0E1A24',borderRadius:4}}>
                                <div style={{fontSize:7,color:'#64748b'}}>STREAK</div>
                                <div style={{fontSize:14,fontWeight:700,fontFamily:'monospace',
                                  color:detailData.neg_streak>0?'#DC3C3C':'#00C878'
                                }}>{detailData.neg_streak||detailData.pos_streak}w</div>
                              </div>
                            </div>
                          </div>

                          {/* Right: Gauge + signals */}
                          <div>
                            <div style={{display:'flex',justifyContent:'center',marginBottom:8}}>
                              <GaugeChart
                                value={detailData.reversal_score}
                                color={detailData.dominant_direction==='BEARISH'?'#DC3C3C':detailData.dominant_direction==='BULLISH'?'#00C878':'#64748b'}
                              />
                            </div>
                            <div style={{fontSize:9,fontWeight:700,color:C.textMuted,marginBottom:6}}>
                              SINAIS ATIVOS
                            </div>
                            {detailData.signals.map((sig,i) => (
                              <div key={i} style={{
                                padding:8,background:'#0E1A24',borderRadius:4,
                                borderLeft:`3px solid ${sig.color}`,marginBottom:6
                              }}>
                                <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                                  <span style={{fontSize:10,fontWeight:700,color:sig.color}}>
                                    {sig.icon} {sig.label}
                                  </span>
                                  <span style={{fontSize:10,fontFamily:'monospace',color:sig.color}}>
                                    {sig.probability}%
                                  </span>
                                </div>
                                <div style={{fontSize:8,color:'#64748b',marginTop:3}}>{sig.description}</div>
                              </div>
                            ))}
                          </div>
                        </div>
                      </div>
                    ) : (
                      <div style={{color:'#64748b',fontSize:11,fontStyle:'italic'}}>
                        Selecione uma commodity no overview para ver detalhes.
                      </div>
                    )
                  )}
                </div>
              );
            })()}

            <div style={{marginTop:6,fontSize:8,color:C.textMuted}}>
              CFTC Commitments of Traders | {cotLegacy?.weeks||0}w history | Last: {cotLegacy?.latest?.date||"--"}
            </div>
          </>
        ) : (
          <DataPlaceholder title="COT -- Dados Pendentes" detail="Execute collect_cot.py para coletar dados CFTC." />
        )}
      </div>
    </div>
  );

  // -- Tab: Comparativo ---------------------------------------------------
  const getSymFromContract = (contract: string) => { const m = contract.match(/^([A-Z]{2})/); return m ? m[1] : contract; };
  const renderComparativo = () => (
    <div>
      <SectionTitle>Comparativo Normalizado (Base 100)</SectionTitle>
      <div style={{marginBottom:16,display:"flex",gap:12,flexWrap:"wrap",alignItems:"center"}}>
        <div style={{display:"flex",flexDirection:"column",gap:4}}>
          <label style={{fontSize:9,color:C.textMuted,textTransform:"uppercase",letterSpacing:0.5}}>Commodity 1</label>
          <select value={cmp1} onChange={e=>setCmp1(e.target.value)} style={{padding:"8px 12px",fontSize:11,borderRadius:6,cursor:"pointer",background:C.panel,color:C.text,border:`1px solid ${C.border}`,minWidth:180,fontFamily:"monospace"}}>
            <option value="">--</option>
            {futures && Object.entries(futures.commodities||{}).flatMap(([sym,fc])=>(fc.contracts||[]).map(ct=><option key={ct.contract} value={ct.contract}>{ct.contract} - {ct.expiry_label}</option>))}
          </select>
        </div>
        <div style={{display:"flex",flexDirection:"column",gap:4}}>
          <label style={{fontSize:9,color:C.textMuted,textTransform:"uppercase",letterSpacing:0.5}}>Commodity 2</label>
          <select value={cmp2} onChange={e=>setCmp2(e.target.value)} style={{padding:"8px 12px",fontSize:11,borderRadius:6,cursor:"pointer",background:C.panel,color:C.text,border:`1px solid ${C.border}`,minWidth:180,fontFamily:"monospace"}}>
            <option value="">--</option>
            {futures && Object.entries(futures.commodities||{}).flatMap(([sym,fc])=>(fc.contracts||[]).map(ct=><option key={ct.contract} value={ct.contract}>{ct.contract} - {ct.expiry_label}</option>))}
          </select>
        </div>
        <div style={{display:"flex",flexDirection:"column",gap:4}}>
          <label style={{fontSize:9,color:C.textMuted,textTransform:"uppercase",letterSpacing:0.5}}>Commodity 3</label>
          <select value={cmp3} onChange={e=>setCmp3(e.target.value)} style={{padding:"8px 12px",fontSize:11,borderRadius:6,cursor:"pointer",background:C.panel,color:C.text,border:`1px solid ${C.border}`,minWidth:180,fontFamily:"monospace"}}>
            <option value="">--</option>
            {futures && Object.entries(futures.commodities||{}).flatMap(([sym,fc])=>(fc.contracts||[]).map(ct=><option key={ct.contract} value={ct.contract}>{ct.contract} - {ct.expiry_label}</option>))}
          </select>
        </div>
      </div>
      <div style={{fontSize:10,color:C.textMuted,marginBottom:16}}>Selecione contratos para comparar (Base 100, front month)</div>
      {(() => {
        const syms = compareSyms;
        const contractPrices: any = {};
        for (const c of syms) {
          if (contractHist?.contracts?.[c]?.bars?.length) {
            contractPrices[c] = contractHist.contracts[c].bars;
          } else if (prices) {
            const sym = getSymFromContract(c);
            if (prices[sym]) contractPrices[c] = prices[sym];
          }
        }
        const validSyms = syms.filter(s => contractPrices[s]?.length);
        return validSyms.length ? <CompareChart symbols={validSyms} prices={contractPrices} /> : <DataPlaceholder title="Carregando..." detail="" />;
      })()}

      {prices && (
        <div style={{marginTop:20,overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
            <thead><TableHeader cols={["Contrato","Último","1D","1M","3M","YTD","Min 52w","Max 52w"]} /></thead>
            <tbody>
              {compareSyms.map(contract=>{
                const sym=getSymFromContract(contract);const d=contractHist?.contracts?.[contract]?.bars?.length ? contractHist.contracts[contract].bars : prices?.[sym];if(!d?.length)return null;
                const last=d[d.length-1].close;
                const d1=d.length>1?((last-d[d.length-2].close)/d[d.length-2].close*100):0;
                const d21=d.length>21?((last-d[d.length-22].close)/d[d.length-22].close*100):0;
                const d63=d.length>63?((last-d[d.length-64].close)/d[d.length-64].close*100):0;
                const yr=d[d.length-1].date.slice(0,4);
                const ytdStart=d.find((c:any)=>c.date.startsWith(yr));
                const ytd=ytdStart?((last-ytdStart.close)/ytdStart.close*100):0;
                const last252=d.slice(-252);
                const min52=Math.min(...last252.map((c:any)=>c.low));
                const max52=Math.max(...last252.map((c:any)=>c.high));
                const nm=contract + " (" + (COMMODITIES.find(c=>c.sym===sym)?.name||sym) + ")";
                return (
                  <tr key={contract} style={{borderBottom:`1px solid ${C.border}`}}>
                    <td style={{padding:"8px 12px",fontWeight:600}}>{sym} <span style={{color:C.textMuted,fontWeight:400}}>({nm})</span></td>
                    <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",fontWeight:600}}>{last.toFixed(2)}</td>
                    <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",color:d1>=0?C.green:C.red}}>{d1>=0?"+":""}{d1.toFixed(2)}%</td>
                    <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",color:d21>=0?C.green:C.red}}>{d21>=0?"+":""}{d21.toFixed(2)}%</td>
                    <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",color:d63>=0?C.green:C.red}}>{d63>=0?"+":""}{d63.toFixed(2)}%</td>
                    <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",color:ytd>=0?C.green:C.red}}>{ytd>=0?"+":""}{ytd.toFixed(2)}%</td>
                    <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace"}}>{min52.toFixed(2)}</td>
                    <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace"}}>{max52.toFixed(2)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* -- Contratos Futuros -- */}
      {/* Curva Forward -- contract_history.json */}
      {(contractHist || futures) && (
        <div style={{marginTop:28}}>
          <div style={{fontSize:16,fontWeight:700,color:C.text,marginBottom:12,display:"flex",alignItems:"center",gap:8}}>
            Curva Forward -- Contango vs Backwardation
          </div>
          <div style={{marginBottom:12,display:"flex",gap:8,flexWrap:"wrap"}}>
            {(()=>{
              const syms = new Set<string>();
              if(contractHist?.contracts){
                Object.values(contractHist.contracts as Record<string,any>).forEach((c:any)=>{if(c.commodity)syms.add(c.commodity);});
              } else if(futures?.commodities){
                Object.keys(futures.commodities).forEach(s=>syms.add(s));
              }
              return Array.from(syms).sort().map(sym=>{
                const isSel=fwdSyms.includes(sym);
                return (
                  <button key={sym} onClick={()=>setFwdSyms([sym])} style={{
                    padding:"4px 10px",fontSize:10,fontWeight:isSel?700:500,borderRadius:4,cursor:"pointer",
                    background:isSel?"rgba(59,130,246,.15)":"transparent",
                    color:isSel?C.blue:C.textMuted,border:`1px solid ${C.border}`,transition:"all .15s"
                  }}>{sym}</button>
                );
              });
            })()}
          </div>
          <div style={{fontSize:10,color:C.textMuted,marginBottom:8}}>Eixo X = data de vencimento | Eixo Y = último preço | Fonte: contract_history.json</div>
          <svg viewBox="0 0 900 380" style={{width:"100%",background:C.panel,borderRadius:8,border:`1px solid ${C.border}`}}>
            {(()=>{
              const MO:Record<string,number>={F:1,G:2,H:3,J:4,K:5,M:6,N:7,Q:8,U:9,V:10,X:11,Z:12};
              const allCurves = fwdSyms.map((sym,si)=>{
                let pts:{label:string;val:number;idx:number}[]=[];
                if(contractHist?.contracts){
                  const entries = Object.values(contractHist.contracts as Record<string,any>)
                    .filter((c:any)=>c.commodity===sym && c.bars?.length>0);
                  const sorted = entries.map((c:any)=>{
                    const code=(c.symbol||"").replace(sym,"");
                    const mc=code.charAt(0);const ys=code.slice(1);
                    const yr=ys.length===2?2000+parseInt(ys):parseInt(ys);
                    const lastBar=c.bars[c.bars.length-1];
                    return {label:c.expiry_label||`${mc}${ys}`,val:lastBar.close as number,sortKey:yr*100+(MO[mc]||0)};
                  }).filter(p=>p.val>0).sort((a,b)=>a.sortKey-b.sortKey);
                  pts=sorted.map((p,i)=>({label:p.label,val:p.val,idx:i}));
                } else {
                  const fc=(futures?.commodities||{})[sym];if(!fc)return null;
                  const cts=(fc.contracts||[]).filter((c:any)=>c.close>0).sort((a:any,b:any)=>{
                    return (parseInt(a.year)*100+(MO[a.month_code]||0))-(parseInt(b.year)*100+(MO[b.month_code]||0));
                  });
                  pts=cts.map((c:any,i:number)=>({label:c.expiry_label,val:c.close,idx:i}));
                }
                if(pts.length<2) return null;
                return {sym,pts,color:["#3b82f6","#ef4444","#22c55e","#f59e0b","#a855f7"][si%5]};
              }).filter(Boolean) as {sym:string;pts:{label:string;val:number;idx:number}[];color:string}[];
              if(!allCurves.length) return <text x="450" y="190" textAnchor="middle" fill={C.textMuted} fontSize="12">Selecione uma commodity acima</text>;
              const allVals=allCurves.flatMap(c=>c.pts.map(p=>p.val));
              const minV=Math.min(...allVals);const maxV=Math.max(...allVals);
              const rng=maxV-minV||1;const pMin=minV-rng*.08;const pMax=maxV+rng*.08;
              const padL=65;const chartW=900-padL-20;const chartH=280;const topY=35;
              const maxLen=Math.max(...allCurves.map(c=>c.pts.length));
              const scX=(i:number)=>padL+(maxLen>1?i/(maxLen-1)*chartW:chartW/2);
              const scY=(v:number)=>topY+(1-(v-pMin)/(pMax-pMin))*chartH;
              const c0=allCurves[0];const f0=c0.pts[0].val;const b0=c0.pts[c0.pts.length-1].val;
              const struct=b0>f0?"CONTANGO":"BACKWARDATION";const sCol=struct==="CONTANGO"?C.amber:C.cyan;
              return (
                <g>
                  <text x={padL} y={18} fill={C.textDim} fontSize="10" fontWeight="600">{c0.sym} -- {COMMODITIES.find(cc=>cc.sym===c0.sym)?.unit||""} -- Estrutura: </text>
                  <text x={padL+210} y={18} fill={sCol} fontSize="10" fontWeight="700">{struct} ({((b0-f0)/f0*100).toFixed(2)}%)</text>
                  {Array.from({length:6}).map((_,gi)=>{
                    const val=pMin+(pMax-pMin)*gi/5;const y=scY(val);
                    return <g key={gi}><line x1={padL} x2={900-20} y1={y} y2={y} stroke={C.border} strokeWidth="0.5"/>
                      <text x={padL-8} y={y+3} textAnchor="end" fill={C.textMuted} fontSize="9" fontFamily="monospace">{val.toFixed(2)}</text></g>;
                  })}
                  <line x1={padL} x2={900-20} y1={scY(f0)} y2={scY(f0)} stroke={C.textMuted} strokeWidth="0.8" strokeDasharray="4,4"/>
                  <text x={900-18} y={scY(f0)+3} textAnchor="start" fill={C.textMuted} fontSize="8">front</text>
                  {allCurves.map(curve=>{
                    const ap=curve.pts.map(p=>`${scX(p.idx)},${scY(p.val)}`).join(" ");
                    const fY=scY(curve.pts[0].val);
                    return (<g key={curve.sym}>
                      <polygon fill={curve.pts[curve.pts.length-1].val>curve.pts[0].val?"rgba(245,158,11,.08)":"rgba(6,182,212,.08)"}
                        points={`${scX(0)},${fY} ${ap} ${scX(curve.pts.length-1)},${fY}`}/>
                      <polyline fill="none" stroke={curve.color} strokeWidth="2.5" points={ap}/>
                      {curve.pts.map((p,pi)=>(<g key={pi}>
                        <circle cx={scX(p.idx)} cy={scY(p.val)} r="4" fill={curve.color} stroke={C.panel} strokeWidth="1.5"/>
                        <text x={scX(p.idx)} y={scY(p.val)-8} textAnchor="middle" fill={curve.color} fontSize="8" fontWeight="600" fontFamily="monospace">{p.val.toFixed(1)}</text>
                      </g>))}
                      <text x={scX(curve.pts.length-1)+10} y={scY(curve.pts[curve.pts.length-1].val)+4} fill={curve.color} fontSize="10" fontWeight="700">{curve.sym}</text>
                    </g>);
                  })}
                  {c0.pts.map((p,pi)=>(
                    <text key={pi} x={scX(pi)} y={topY+chartH+20} textAnchor="middle" fill={C.textMuted} fontSize="8" fontWeight="500"
                      transform={`rotate(-35,${scX(pi)},${topY+chartH+20})`}>{p.label}</text>
                  ))}
                </g>
              );
            })()}
          </svg>
          <div style={{display:"flex",gap:20,justifyContent:"center",marginTop:8,fontSize:10}}>
            <div style={{display:"flex",alignItems:"center",gap:4}}>
              <div style={{width:12,height:3,borderRadius:2,background:C.amber}}/>
              <span style={{color:C.textDim}}>Contango (back &gt; front)</span>
            </div>
            <div style={{display:"flex",alignItems:"center",gap:4}}>
              <div style={{width:12,height:3,borderRadius:2,background:C.cyan}}/>
              <span style={{color:C.textDim}}>Backwardation (front &gt; back)</span>
            </div>
          </div>
        </div>
      )}

      {futures && (
        <div style={{marginTop:28}}>
          <SectionTitle>Contratos Futuros -- Pre\u00e7os por Vencimento</SectionTitle>
          <div style={{fontSize:10,color:C.textMuted,marginBottom:12}}>
            {fwdSyms.length>0?`Mostrando: ${fwdSyms.join(", ")}`:"Todos os vencimentos"} | Fonte: Yahoo Finance / Stooq
          </div>
          {(()=>{
            const activeFwd = fwdSyms.length > 0 ? fwdSyms : Object.keys(futures.commodities||{});
            const filtered = Object.entries(futures.commodities||{}).filter(([sym])=>activeFwd.includes(sym));
            return filtered;
          })().map(([sym,fc])=>{
            const nm=COMMODITIES.find(c=>c.sym===sym)?.name||sym;
            const contracts=fc.contracts||[];
            if(!contracts.length) return null;
            return (
              <div key={sym} style={{marginBottom:16}}>
                <div style={{fontSize:11,fontWeight:700,color:C.text,marginBottom:6,padding:"6px 0",borderBottom:`1px solid ${C.border}`}}>
                  {sym} -- {nm} <span style={{color:C.textMuted,fontWeight:400}}>({fc.unit||""} | {fc.exchange||""})</span>
                </div>
                <div style={{overflowX:"auto"}}>
                  <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
                    <thead>
                      <tr style={{borderBottom:`1px solid ${C.border}`}}>
                        {["Contrato","Vencimento","Último","Open","High","Low","Volume","Spread","Estrutura"].map(h=>(
                          <th key={h} style={{padding:"6px 8px",textAlign:h==="Contrato"||h==="Vencimento"?"left":"right",
                            fontSize:9,fontWeight:600,color:C.textMuted,textTransform:"uppercase",letterSpacing:0.5}}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {contracts.map((ct,ci)=>{
                        const spr=fc.spreads?.find(s=>s.front===ct.contract);
                        return (
                          <tr key={ct.contract} style={{borderBottom:`1px solid ${C.border}`,background:ci===0?`rgba(59,130,246,.06)`:"transparent"}}>
                            <td style={{padding:"6px 8px",fontWeight:600,fontFamily:"monospace"}}>{ct.contract}</td>
                            <td style={{padding:"6px 8px",color:C.textDim}}>{ct.expiry_label}</td>
                            <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"monospace",fontWeight:700}}>{ct.close?.toFixed(2)||"--"}</td>
                            <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"monospace",color:C.textDim}}>{ct.open?.toFixed(2)||"--"}</td>
                            <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"monospace",color:C.textDim}}>{ct.high?.toFixed(2)||"--"}</td>
                            <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"monospace",color:C.textDim}}>{ct.low?.toFixed(2)||"--"}</td>
                            <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"monospace",color:C.textMuted}}>{ct.volume?.toLocaleString()||"--"}</td>
                            <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"monospace",color:spr?(spr.spread>=0?C.amber:C.cyan):C.textMuted}}>
                              {spr?(spr.spread>=0?"+":"")+spr.spread.toFixed(2):"--"}
                            </td>
                            <td style={{padding:"6px 8px",textAlign:"right"}}>
                              {spr?<span style={{fontSize:9,padding:"2px 6px",borderRadius:3,
                                background:spr.structure==="contango"?C.amberBg:"rgba(6,182,212,.12)",
                                color:spr.structure==="contango"?C.amber:C.cyan}}>{spr.structure}</span>:null}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            );
          })}
        </div>
      )}

    </div>
  );

  // -- Tab: Spreads -------------------------------------------------------
      const renderSpreads = () => {
    const alertCounts = {alerta:0,atencao:0,ok:0};
    spreadList.forEach(sp=>{const al=getAlertLevel({...sp,key:sp.key});alertCounts[al]++;});

    // Group by category
    const categories: Record<string, any[]> = {};
    spreadList.forEach(sp => {
      const cat = sp.category || 'outros';
      if (!categories[cat]) categories[cat] = [];
      categories[cat].push(sp);
    });
    // Sort within each category: alerts first
    const catOrder = ['graos','pecuaria','energia','basis','outros'];
    Object.values(categories).forEach(arr => arr.sort((a:any,b:any) => {
      const ord:any = {alerta:0,atencao:1,ok:2};
      return (ord[getAlertLevel({...a,key:a.key})]??2)-(ord[getAlertLevel({...b,key:b.key})]??2);
    }));

    return (
    <div>
      <SectionTitle>Rela\u00e7\u00f5es de Pre\u00e7o -- Spreads & Margens</SectionTitle>
      <div style={{fontSize:12,color:C.textMuted,marginBottom:16,lineHeight:1.6,maxWidth:750,borderLeft:'3px solid #DCB432',paddingLeft:10}}>
        Spreads comparam pre\u00e7os entre commodities ligadas. Clique para expandir e ver o que cada um significa, o sinal atual e o que monitorar.
      </div>

      {/* Summary */}
      <div style={{display:"flex",gap:12,marginBottom:20}}>
        <div style={{background:"rgba(220,60,60,.08)",border:"1px solid rgba(220,60,60,.2)",borderRadius:8,padding:"10px 20px",textAlign:"center",flex:1}}>
          <div style={{fontSize:24,fontWeight:800,color:"#DC3C3C",fontFamily:"monospace"}}>{alertCounts.alerta}</div>
          <div style={{fontSize:10,fontWeight:600,color:"#DC3C3C"}}>Extremo</div>
        </div>
        <div style={{background:"rgba(220,180,50,.08)",border:"1px solid rgba(220,180,50,.2)",borderRadius:8,padding:"10px 20px",textAlign:"center",flex:1}}>
          <div style={{fontSize:24,fontWeight:800,color:"#DCB432",fontFamily:"monospace"}}>{alertCounts.atencao}</div>
          <div style={{fontSize:10,fontWeight:600,color:"#DCB432"}}>Aten\u00e7\u00e3o</div>
        </div>
        <div style={{background:"rgba(0,200,120,.06)",border:"1px solid rgba(0,200,120,.15)",borderRadius:8,padding:"10px 20px",textAlign:"center",flex:1}}>
          <div style={{fontSize:24,fontWeight:800,color:"#00C878",fontFamily:"monospace"}}>{alertCounts.ok}</div>
          <div style={{fontSize:10,fontWeight:600,color:"#00C878"}}>Normal</div>
        </div>
      </div>

      {spreadList.length > 0 ? (
        <div>
          {catOrder.filter(cat => categories[cat]?.length).map(cat => {
            const items = categories[cat];
            const catAlerts = items.filter((sp:any) => getAlertLevel({...sp,key:sp.key}) === 'alerta').length;
            return (
              <div key={cat} style={{marginBottom:24}}>
                <div style={{display:'flex',alignItems:'center',gap:8,marginBottom:12}}>
                  <span style={{fontSize:12,fontWeight:700,color:'#94a3b8',letterSpacing:'0.08em',textTransform:'uppercase'}}>
                    {SPREAD_CATEGORY_LABELS[cat] || cat}
                  </span>
                  {catAlerts > 0 && <span style={{background:'#DC3C3C',color:'#fff',fontSize:8,fontWeight:700,borderRadius:8,padding:'1px 6px'}}>{catAlerts}</span>}
                </div>

                {items.map((sp:any) => {
                  const zs = sp.zscore_1y || 0;
                  const zColor = Math.abs(zs) >= 2.5 ? '#DC3C3C' : Math.abs(zs) >= 1.5 ? '#DCB432' : '#00C878';
                  const regimeLabel = Math.abs(zs) >= 2.5 ? 'EXTREMO' : Math.abs(zs) >= 1.5 ? 'ATEN\u00c7\u00c3O' : Math.abs(zs) >= 0.5 ? 'NORMAL' : 'NEUTRO';
                  const tp = sp.trend_pct || 0;
                  const trendArrow = tp > 1 ? '\u2191' : tp < -1 ? '\u2193' : '\u2192';
                  const friendlyName = SPREAD_FRIENDLY_NAMES[sp.key] || SPREAD_NAMES[sp.key] || sp.name;
                  const interpretation = sp.interpretation || SPREAD_DETAILS[sp.key]?.whatIsIt || '';
                  const watchIf = sp.watch_if || '';
                  const signalNow = sp.signal_now || '';

                  return (
                    <details key={sp.key} style={{marginBottom:10}}>
                      <summary style={{listStyle:'none',cursor:'pointer'}}>
                        <div style={{
                          background:'#0E1A24',border:`1px solid ${zColor}33`,borderLeft:`3px solid ${zColor}`,
                          borderRadius:8,padding:'12px 16px',display:'flex',alignItems:'center',justifyContent:'space-between'
                        }}>
                          <div style={{flex:1}}>
                            <div style={{fontSize:12,fontWeight:700,color:'#e2e8f0',marginBottom:3}}>{friendlyName}</div>
                            <div style={{display:'flex',alignItems:'center',gap:12,flexWrap:'wrap'}}>
                              <span style={{fontSize:18,fontWeight:700,fontFamily:'monospace',color:zColor}}>
                                {typeof sp.current === 'number' ? sp.current.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:4}) : sp.current}
                              </span>
                              <span style={{fontSize:10,color:'#64748b'}}>{sp.unit}</span>
                              <span style={{fontSize:10,color:zColor,fontFamily:'monospace'}}>z={zs > 0 ? '+' : ''}{zs.toFixed(2)}</span>
                              <span style={{fontSize:9,fontWeight:700,background:zColor+'22',color:zColor,padding:'2px 6px',borderRadius:4}}>{regimeLabel}</span>
                              <span style={{fontSize:10,color:'#94a3b8'}}>{trendArrow} {Math.abs(tp).toFixed(1)}% (7d)</span>
                            </div>
                          </div>
                          <div style={{fontSize:10,color:'#475569',marginLeft:8}}>\u25BC</div>
                        </div>
                      </summary>

                      {/* Z-score bar */}
                      <div style={{height:3,background:'#1e3a4a',marginTop:-1}}>
                        <div style={{height:'100%',background:zColor,
                          width:`${Math.min(Math.abs(zs)/3*100,100)}%`,
                          marginLeft:zs < 0 ? `${Math.max(50 - Math.abs(zs)/3*50,0)}%` : '50%',
                          transition:'width 0.3s'}}/>
                      </div>

                      {/* Expanded content */}
                      <div style={{background:'#0E1A24',border:'1px solid #1e3a4a',borderTop:'none',borderRadius:'0 0 8px 8px',padding:'14px 16px'}}>
                        {/* Gaussiana + Sparkline */}
                        <div style={{display:'flex',gap:16,marginBottom:12,flexWrap:'wrap'}}>
                          {sp.mean_1y && sp.std_1y && sp.current != null && (()=>{
                            const svg = buildGaussianSVG(sp.current, sp.mean_1y, sp.std_1y, sp.name, " "+(sp.unit||""), 320, 100);
                            return svg ? <div dangerouslySetInnerHTML={{__html:svg}}/> : null;
                          })()}
                          {sp.history?.length > 0 && (
                            <div style={{flex:1,minWidth:200}}>
                              <div style={{fontSize:9,color:'#64748b',fontWeight:700,marginBottom:4}}>TEND\u00caNCIA (20 DIAS)</div>
                              <SpreadChart history={sp.history} regime={sp.regime} />
                            </div>
                          )}
                        </div>

                        {interpretation && (
                          <div style={{marginBottom:10}}>
                            <div style={{fontSize:9,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:'0.08em',marginBottom:4}}>
                              {"\u{1F4AC}"} O que significa
                            </div>
                            <div style={{fontSize:11,color:'#94a3b8',lineHeight:1.6}}>{interpretation}</div>
                          </div>
                        )}

                        {signalNow && (
                          <div style={{marginBottom:10,padding:'8px 10px',background:zColor+'11',borderRadius:6,borderLeft:`2px solid ${zColor}`}}>
                            <div style={{fontSize:9,fontWeight:700,color:zColor,textTransform:'uppercase',letterSpacing:'0.08em',marginBottom:3}}>
                              {"\u{1F4CD}"} Agora
                            </div>
                            <div style={{fontSize:11,color:'#e2e8f0',lineHeight:1.5}}>{signalNow}</div>
                          </div>
                        )}

                        {watchIf && (
                          <div>
                            <div style={{fontSize:9,fontWeight:700,color:'#64748b',textTransform:'uppercase',letterSpacing:'0.08em',marginBottom:4}}>
                              {"\u{1F441}"} Monitore se
                            </div>
                            <div style={{fontSize:11,color:'#64748b',lineHeight:1.5}}>{watchIf}</div>
                          </div>
                        )}

                        {/* ITEM 5: Feedlot cycle corrected */}
                        {sp.key === 'feedlot' && (sp as any).method === 'cycle_corrected' && (
                          <div style={{fontSize:9, color:'#64748b', marginTop:8, padding:'6px 8px', background:'#0E1A2480', borderRadius:4, borderLeft:'2px solid #DCB432'}}>
                            {"\u23F1"} Ciclo real: {(sp as any).contracts?.gf || '?'} {"\u2192"} {(sp as any).contracts?.zc || '?'} {"\u2192"} {(sp as any).contracts?.le || '?'}
                          </div>
                        )}

                        {/* ITEM 5: Soy Crush forward vs spot */}
                        {sp.key === 'soy_crush' && (sp as any).value_forward !== undefined && (
                          <div style={{display:'flex', gap:12, marginTop:8}}>
                            <div>
                              <div style={{fontSize:8, color:'#64748b'}}>SPOT</div>
                              <div style={{fontSize:13, fontFamily:'monospace', color:'#e2e8f0'}}>
                                {typeof sp.current === 'number' ? sp.current.toFixed(2) : sp.current}
                              </div>
                            </div>
                            <div>
                              <div style={{fontSize:8, color:'#64748b'}}>FORWARD +2m</div>
                              <div style={{fontSize:13, fontFamily:'monospace',
                                color: (sp as any).value_forward < sp.current ? '#DC3C3C' : '#00C878'}}>
                                {(sp as any).value_forward?.toFixed(2)}
                                <span style={{fontSize:8, marginLeft:3}}>
                                  ({(((sp as any).value_forward / sp.current - 1) * 100).toFixed(0)}%)
                                </span>
                              </div>
                            </div>
                          </div>
                        )}

                        {/* ITEM 6: ZL/CL term structure */}
                        {sp.key === 'zl_cl' && ((sp as any).value_3m || (sp as any).value_6m) && (
                          <div style={{marginTop:8}}>
                            <div style={{fontSize:8, color:'#64748b', marginBottom:4}}>
                              ESTRUTURA DE TERMO
                            </div>
                            <div style={{display:'flex', gap:8}}>
                              {[
                                {label:'Spot', val: sp.current},
                                {label:'+3m',  val: (sp as any).value_3m},
                                {label:'+6m',  val: (sp as any).value_6m},
                              ].filter(x => x.val != null).map(({label, val}) => (
                                <div key={label} style={{
                                  padding:'3px 6px',
                                  background:'#142332',
                                  borderRadius:4,
                                  fontSize:10,
                                  fontFamily:'monospace'
                                }}>
                                  <span style={{color:'#64748b', fontSize:8}}>{label} </span>
                                  <span style={{color:'#e2e8f0'}}>{val?.toFixed(4)}</span>
                                </div>
                              ))}
                            </div>
                            {(sp as any).term_note && (
                              <div style={{fontSize:9, color:'#DCB432', marginTop:4}}>
                                {(sp as any).term_note}
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    </details>
                  );
                })}
              </div>
            );
          })}
        </div>
      ) : (
        <DataPlaceholder title="Sem dados de spreads" detail="Execute o pipeline para calcular spreads" />
      )}
    </div>
    );
  };

  // -- Tab: Sazonalidade ---------------------------------------------------
  const renderSazonalidade = () => (
    <div>
      <SectionTitle>Sazonalidade -- {selected} ({COMMODITIES.find(c=>c.sym===selected)?.name})</SectionTitle>
      {season && season[selected] ? (
        <SeasonChart entry={season[selected]} />
      ) : (
        <DataPlaceholder title="Sem dados" detail={`${selected} não encontrado em seasonality.json`} />
      )}
      <div style={{marginTop:24}}>
        <SectionTitle>Preço Atual vs Média Histórica (5 Anos)</SectionTitle>
        <div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
            <thead><TableHeader cols={["Commodity","Atual","Média 5Y","Desvio %","Sinal"]} /></thead>
            <tbody>
              {COMMODITIES.map(c=>{
                const sd=getSeasonDev(c.sym);
                if(!sd) return (
                  <tr key={c.sym} style={{borderBottom:`1px solid ${C.border}`}}>
                    <td style={{padding:"8px 12px",fontWeight:600}}>{c.sym} <span style={{color:C.textMuted,fontWeight:400}}>({c.name})</span></td>
                    <td colSpan={4} style={{padding:"8px 12px",color:C.textMuted,textAlign:"center"}}>--</td>
                  </tr>
                );
                const sigCol = Math.abs(sd.dev)>15?C.red:Math.abs(sd.dev)>5?C.amber:C.green;
                const sigLabel = sd.dev<-15?"ABAIXO":sd.dev>15?"ACIMA":Math.abs(sd.dev)>5?"DESVIADO":"NORMAL";
                return (
                  <tr key={c.sym} onClick={()=>setSelected(c.sym)} style={{borderBottom:`1px solid ${C.border}`,cursor:"pointer",
                    background:c.sym===selected?"rgba(59,130,246,.08)":"transparent"}}>
                    <td style={{padding:"8px 12px",fontWeight:600}}>{c.sym} <span style={{color:C.textMuted,fontWeight:400}}>({c.name})</span></td>
                    <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",fontWeight:600}}>{sd.current.toFixed(2)}</td>
                    <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",color:C.textMuted}}>{sd.avg.toFixed(2)}</td>
                    <td style={{padding:"8px 12px",textAlign:"right"}}><DevBar val={sd.dev} /></td>
                    <td style={{padding:"8px 12px",textAlign:"right"}}><Badge label={sigLabel} color={sigCol} /></td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );

  // -- Tab: Stocks Watch --------------------------------------------------
  const renderStocksWatch = () => {
    const STOCKS_ORDER = ['ZC','ZS','ZW','KE','ZM','ZL','LE','GF','HE','SB','KC','CT','CC','OJ','CL','NG','GC','SI'];
    const allStockSymbols = Array.from(new Set([
      ...Object.keys(psdData?.commodities || {}),
      ...Object.keys(stocks?.commodities || {})
    ]));
    const orderedSymbols = [
      ...STOCKS_ORDER.filter(s => allStockSymbols.includes(s)),
      ...allStockSymbols.filter(s => !STOCKS_ORDER.includes(s))
    ];

    const getStockData = (sym: string) => {
      const psd = psdData?.commodities?.[sym];
      const sw = stocks?.commodities?.[sym];
      if (psd) {
        const dev = psd.avg_5y && psd.avg_5y > 0 ? ((psd.current - psd.avg_5y) / psd.avg_5y * 100) : 0;
        return {
          symbol: sym,
          stock_current: psd.current,
          stock_avg: psd.avg_5y,
          stock_unit: psd.unit || '(1000 MT)',
          deviation: psd.deviation ?? dev,
          year: psd.year,
          history: psd.history || [],
          source: 'PSD' as const,
          state: sw?.state || (dev > 30 ? 'ACIMA_MEDIA' : dev < -20 ? 'ABAIXO_MEDIA' : 'NEUTRO'),
          factors: sw?.factors || [],
          data_available: { stock_real: true },
          price: sw?.price,
          price_vs_avg: sw?.price_vs_avg
        };
      }
      if (sw) {
        return { ...sw, source: 'PROXY' as const, history: [] as any[], deviation: 0 };
      }
      return null;
    };

    const allStockData = orderedSymbols.map(s => getStockData(s)).filter(Boolean) as any[];
    const realStocks = allStockData.filter(s => s.data_available?.stock_real || s.source === 'PSD');
    const aperto = allStockData.filter(s=>s.state?.includes("APERTO")||s.state==="PRECO_ELEVADO"||s.state==="PRECO_DEPRIMIDO"||s.state==="ABAIXO_MEDIA");
    const excesso = allStockData.filter(s=>s.state?.includes("EXCESSO")||s.state==="PRECO_ACIMA_MEDIA"||s.state==="ACIMA_MEDIA");
    const neutro = allStockData.filter(s=>s.state==="NEUTRO"||s.state==="PRECO_NEUTRO");

    const selStockData = getStockData(stockSelected);
    const selNm = COMMODITIES.find(c=>c.sym===stockSelected)?.name||stockSelected;

    // Group separators for commodity selector
    const groupBoundaries = new Set(['ZL','HE','OJ','SI'].filter(s => orderedSymbols.includes(s)));

    return (
      <div>
        <SectionTitle>Estoques -- USDA PSD + QuickStats</SectionTitle>

        {/* -- GRAFICO PRINCIPAL - TOPO -- */}
        {realStocks.length > 0 && (
          <div style={{marginBottom:24}}>
            {/* Commodity selector with group separators */}
            <div style={{display:"flex",gap:4,flexWrap:"wrap",marginBottom:12,alignItems:"center"}}>
              {orderedSymbols.map((sym,i)=>{
                const d = getStockData(sym);
                if (!d) return null;
                const nm=COMMODITIES.find(c=>c.sym===sym)?.name||sym;
                const isSel=stockSelected===sym;
                const isPsd = d.source === 'PSD';
                const showSep = i > 0 && groupBoundaries.has(orderedSymbols[i-1]);
                return (
                  <span key={sym}>
                    {showSep && <div style={{width:1,height:20,background:'#1e3a4a',margin:'0 4px'}}/>}
                    <button onClick={()=>setStockSelected(sym)} style={{
                      padding:"5px 12px",fontSize:10,fontWeight:isSel?700:500,borderRadius:4,cursor:"pointer",
                      background:isSel?"rgba(59,130,246,.15)":"transparent",
                      color:isSel?C.blue:isPsd?C.textDim:C.textMuted,border:`1px solid ${isSel?C.blue:C.border}`,
                      transition:"all .15s",opacity:isPsd?1:0.7
                    }}>{sym} <span style={{fontSize:9,color:isSel?C.blue:C.textMuted}}>({nm})</span></button>
                  </span>
                );
              })}
            </div>

            {/* Stock chart — PSD bar chart by year */}
            {(()=>{
              if (!selStockData) return <div style={{padding:20,textAlign:"center",color:C.textMuted,fontSize:11}}>Sem dados para {stockSelected}</div>;

              // PSD history: [{year, value}]
              const psdHist = selStockData.history as {year:number;value:number}[];
              const hasPsdHist = psdHist && psdHist.length > 0;

              // Try stocks_watch stock_history as fallback
              const swStock = stocks?.commodities?.[stockSelected];
              const swHist = swStock?.stock_history;
              const hasSwHist = swHist?.length > 0;

              if (!hasPsdHist && !hasSwHist) {
                return <div style={{padding:20,textAlign:"center",color:C.textMuted,fontSize:11}}>Sem hist\u00f3rico de estoque para {stockSelected}</div>;
              }

              const avg = selStockData.stock_avg || 0;
              const current = selStockData.stock_current;
              const dev = avg > 0 && current ? ((current - avg) / avg * 100) : 0;
              const devColor = Math.abs(dev) > 15 ? C.red : Math.abs(dev) > 5 ? C.amber : C.green;
              const unit_ = selStockData.stock_unit || '';

              if (hasPsdHist) {
                // PSD annual bar chart
                const years = psdHist.map(h => h.year);
                const vals = psdHist.map(h => h.value);
                const maxVal = Math.max(...vals, avg) * 1.1;
                const minVal_ = Math.min(...vals) * 0.85;
                const range_ = maxVal - minVal_ || 1;

                const W=900, H=280;
                const pad_={l:70,r:30,t:25,b:35};
                const chartW_=W-pad_.l-pad_.r;
                const chartH_=H-pad_.t-pad_.b;
                const barW_=Math.min(60, (chartW_ / years.length) * 0.7);
                const gap_=(chartW_ - barW_ * years.length) / (years.length + 1);
                const yC_=(v:number) => pad_.t + chartH_ * (1 - (v - minVal_) / range_);

                const colorList=["#3b82f6","#8b5cf6","#f97316","#ec4899","#f59e0b","#22c55e","#06b6d4"];

                return (
                  <div style={{background:C.panelAlt,borderRadius:8,padding:20,border:`1px solid ${C.border}`}}>
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:16}}>
                      <div>
                        <div style={{fontSize:14,fontWeight:800,color:C.text}}>{stockSelected} -- {selNm}</div>
                        <div style={{fontSize:10,color:C.textMuted,marginTop:2}}>Ending Stocks {unit_} | Fonte: {selStockData.source === 'PSD' ? 'USDA PSD Online' : 'USDA QuickStats'}</div>
                      </div>
                      <div style={{display:"flex",gap:16}}>
                        <div style={{textAlign:"right"}}>
                          <div style={{fontSize:9,color:C.textMuted}}>ATUAL</div>
                          <div style={{fontSize:18,fontWeight:800,fontFamily:"monospace",color:C.text}}>{current != null ? (current >= 1000 ? (current/1000).toFixed(1)+'M' : current.toLocaleString()) : '--'}</div>
                        </div>
                        <div style={{textAlign:"right"}}>
                          <div style={{fontSize:9,color:C.textMuted}}>M\u00c9DIA 5Y</div>
                          <div style={{fontSize:18,fontWeight:800,fontFamily:"monospace",color:C.textDim}}>{avg >= 1000 ? (avg/1000).toFixed(1)+'M' : avg.toLocaleString()}</div>
                        </div>
                        <div style={{textAlign:"right"}}>
                          <div style={{fontSize:9,color:C.textMuted}}>DESVIO</div>
                          <div style={{fontSize:18,fontWeight:800,fontFamily:"monospace",color:devColor}}>{dev>=0?"+":""}{dev.toFixed(1)}%</div>
                        </div>
                      </div>
                    </div>
                    {/* Legend */}
                    <div style={{display:"flex",gap:14,marginBottom:10,flexWrap:"wrap"}}>
                      {years.map((y,i)=>(
                        <div key={y} style={{display:"flex",alignItems:"center",gap:4}}>
                          <div style={{width:14,height:10,borderRadius:2,background:colorList[i%colorList.length]}}/>
                          <span style={{fontSize:10,color:colorList[i%colorList.length],fontWeight:600}}>{y}</span>
                        </div>
                      ))}
                      {avg > 0 && <div style={{display:"flex",alignItems:"center",gap:4}}>
                        <div style={{width:20,height:0,borderTop:"2px dashed "+C.amber}}/>
                        <span style={{fontSize:10,color:C.amber,fontWeight:600}}>M\u00e9dia 5Y ({avg >= 1000 ? (avg/1000).toFixed(1)+'M' : avg.toLocaleString()})</span>
                      </div>}
                    </div>
                    {/* Bar chart */}
                    <svg width={W} height={H} style={{display:"block",width:"100%"}} viewBox={`0 0 ${W} ${H}`}>
                      {[0,0.25,0.5,0.75,1].map(pct=>{
                        const gy=pad_.t+chartH_*(1-pct);const val=minVal_+range_*pct;
                        return (<g key={"g"+pct}><line x1={pad_.l} y1={gy} x2={W-pad_.r} y2={gy} stroke="rgba(148,163,184,.08)" strokeWidth={1}/><text x={pad_.l-8} y={gy+3} fill={C.textMuted} fontSize={9} textAnchor="end" fontFamily="monospace">{val>=10000?(val/1000).toFixed(0)+'k':val.toFixed(0)}</text></g>);
                      })}
                      {avg > 0 && <line x1={pad_.l} y1={yC_(avg)} x2={W-pad_.r} y2={yC_(avg)} stroke={C.amber} strokeWidth={1.5} strokeDasharray="6,4"/>}
                      {psdHist.map((h,i)=>{
                        const bx=pad_.l+gap_*(i+1)+barW_*i;
                        const by=yC_(h.value);
                        const bh=yC_(minVal_)-by;
                        const col=colorList[i%colorList.length];
                        const isLast = i === psdHist.length - 1;
                        return (
                          <g key={h.year}>
                            <rect x={bx} y={by} width={barW_} height={Math.max(1,bh)} rx={3} fill={col} opacity={isLast?1:0.8}/>
                            <text x={bx+barW_/2} y={by-5} fill={col} fontSize={10} textAnchor="middle" fontFamily="monospace" fontWeight="bold">
                              {h.value>=10000?(h.value/1000).toFixed(1)+'k':h.value.toLocaleString()}
                            </text>
                            <text x={bx+barW_/2} y={H-pad_.b+16} fill={C.textMuted} fontSize={11} textAnchor="middle" fontFamily="monospace" fontWeight="700">
                              {h.year}
                            </text>
                          </g>
                        );
                      })}
                    </svg>

                    {/* Gaussiana */}
                    {(()=>{
                      if(!avg || !current) return null;
                      const gStd = avg * 0.20;
                      const svg = buildGaussianSVG(current, avg, gStd, stockSelected+" Estoque", " "+unit_, 420, 110);
                      if(!svg) return null;
                      return (
                        <div style={{marginTop:16,padding:12,background:"#0E1A24",borderRadius:8,border:"1px solid #1e3a4a"}}>
                          <div style={{fontSize:10,color:"#DCB432",fontWeight:700,marginBottom:8,letterSpacing:"0.5px"}}>DISTRIBUI\u00c7\u00c3O HIST\u00d3RICA \u2014 ESTOQUE {stockSelected}</div>
                          <div dangerouslySetInnerHTML={{__html:svg}}/>
                          <div style={{fontSize:9,color:"#64748b",marginTop:4}}>\u03bc = m\u00e9dia 5Y USDA PSD | \u03c3 = desvio padr\u00e3o estimado (20%) | zona verde = \u00b11\u03c3 (normal)</div>
                        </div>
                      );
                    })()}
                  </div>
                );
              }

              // Fallback: stocks_watch quarterly bar chart (original logic)
              if (hasSwHist) {
                const hist=swHist;
                const swAvg=swStock?.stock_avg||0;
                const normalize=(p:string)=>p.replace("FIRST OF ","").replace("END OF ","").slice(0,3);
                const deduped:Record<string,{year:number;period:string;value:number}>={};
                hist.forEach((h:any)=>{
                  if(h.period==="YEAR") return;
                  const key=h.year+"-"+normalize(h.period);
                  if(!deduped[key]||h.value>deduped[key].value) deduped[key]={year:h.year,period:normalize(h.period),value:h.value};
                });
                const clean=Object.values(deduped);
                if(!clean.length) return null;
                const periodOrder:Record<string,number>={"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12};
                const periods=[...new Set(clean.map(h=>h.period))].sort((a,b)=>(periodOrder[a]||0)-(periodOrder[b]||0));
                const years=[...new Set(clean.map(h=>h.year))].sort();
                const yearData:Record<number,{period:string;value:number}[]>={};
                years.forEach(y=>{yearData[y]=clean.filter(h=>h.year===y).sort((a,b)=>(periodOrder[a.period]||0)-(periodOrder[b.period]||0));});
                const allVals=clean.map(h=>h.value);
                const maxVal=Math.max(...allVals,swAvg)*1.1;
                const minVal_=Math.min(...allVals)*0.9;
                const range_=maxVal-minVal_||1;
                const W=900,H=300;
                const pad_={l:60,r:30,t:25,b:35};
                const chartW_=W-pad_.l-pad_.r;
                const chartH_=H-pad_.t-pad_.b;
                const yC_=(v:number)=>pad_.t+chartH_*(1-(v-minVal_)/range_);
                const colorList=["#3b82f6","#8b5cf6","#ec4899","#f59e0b","#22c55e","#06b6d4"];
                const yearColors:Record<number,string>={};
                years.forEach((y,i)=>{yearColors[y]=colorList[i%colorList.length];});
                const swDev=swStock?.stock_avg&&swStock.stock_avg>0?((swStock.stock_current-swStock.stock_avg)/swStock.stock_avg*100):0;
                const swDevColor=Math.abs(swDev)>15?C.red:Math.abs(swDev)>5?C.amber:C.green;
                return (
                  <div style={{background:C.panelAlt,borderRadius:8,padding:20,border:`1px solid ${C.border}`}}>
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:16}}>
                      <div>
                        <div style={{fontSize:14,fontWeight:800,color:C.text}}>{stockSelected} -- {selNm}</div>
                        <div style={{fontSize:10,color:C.textMuted,marginTop:2}}>Estoque Trimestral ({swStock?.stock_unit||""}) | Fonte: USDA QuickStats</div>
                      </div>
                      <div style={{display:"flex",gap:16}}>
                        <div style={{textAlign:"right"}}><div style={{fontSize:9,color:C.textMuted}}>ATUAL</div><div style={{fontSize:18,fontWeight:800,fontFamily:"monospace",color:C.text}}>{swStock?.stock_current?.toFixed(2)||"--"}</div></div>
                        <div style={{textAlign:"right"}}><div style={{fontSize:9,color:C.textMuted}}>M\u00c9DIA</div><div style={{fontSize:18,fontWeight:800,fontFamily:"monospace",color:C.textDim}}>{swStock?.stock_avg?.toFixed(2)||"--"}</div></div>
                        <div style={{textAlign:"right"}}><div style={{fontSize:9,color:C.textMuted}}>DESVIO</div><div style={{fontSize:18,fontWeight:800,fontFamily:"monospace",color:swDevColor}}>{swDev>=0?"+":""}{swDev.toFixed(1)}%</div></div>
                      </div>
                    </div>
                    <div style={{display:"flex",gap:14,marginBottom:10,flexWrap:"wrap"}}>
                      {years.map(y=>(<div key={y} style={{display:"flex",alignItems:"center",gap:4}}><div style={{width:20,height:3,borderRadius:2,background:yearColors[y]}}/><span style={{fontSize:10,color:yearColors[y],fontWeight:600}}>{y}</span></div>))}
                      {swAvg > 0 && <div style={{display:"flex",alignItems:"center",gap:4}}><div style={{width:20,height:0,borderTop:"2px dashed "+C.amber}}/><span style={{fontSize:10,color:C.amber,fontWeight:600}}>M\u00e9dia ({swAvg.toFixed(1)})</span></div>}
                    </div>
                    <svg width={W} height={H} style={{display:"block",width:"100%"}} viewBox={"0 0 "+W+" "+H}>
                      {[0,0.25,0.5,0.75,1].map(pct=>{const gy=pad_.t+chartH_*(1-pct);const val=minVal_+range_*pct;return (<g key={"g"+pct}><line x1={pad_.l} y1={gy} x2={W-pad_.r} y2={gy} stroke="rgba(148,163,184,.08)" strokeWidth={1}/><text x={pad_.l-8} y={gy+3} fill={C.textMuted} fontSize={9} textAnchor="end" fontFamily="monospace">{val>=100?val.toFixed(0):val.toFixed(1)}</text></g>);})}
                      {swAvg > 0 && <line x1={pad_.l} y1={yC_(swAvg)} x2={W-pad_.r} y2={yC_(swAvg)} stroke={C.amber} strokeWidth={1.5} strokeDasharray="6,4"/>}
                      {(()=>{const nYears=years.length;const nPeriods=periods.length;if(!nPeriods||!nYears)return null;const groupW=chartW_/nPeriods;const barGap=2;const bW=Math.max(4,Math.min(20,(groupW-barGap*(nYears+1))/nYears));const groupTotalW=nYears*bW+(nYears-1)*barGap;const groupOffset=(groupW-groupTotalW)/2;const zeroY=yC_(minVal_);return periods.map((per,pi)=>(<g key={per}>{years.map((yr,yi)=>{const entry=yearData[yr]?.find((d:any)=>d.period===per);if(!entry)return null;const bx=pad_.l+pi*groupW+groupOffset+yi*(bW+barGap);const by=yC_(entry.value);const bh=zeroY-by;return (<g key={yr}><rect x={bx} y={by} width={bW} height={Math.max(1,bh)} rx={2} fill={yearColors[yr]} opacity={0.85}/><text x={bx+bW/2} y={by-4} fill={yearColors[yr]} fontSize={8} textAnchor="middle" fontFamily="monospace" fontWeight="bold">{entry.value>=100?entry.value.toFixed(0):entry.value.toFixed(1)}</text></g>);})}</g>));})()}
                    </svg>
                  </div>
                );
              }

              return null;
            })()}
          </div>
        )}

        {/* Summary cards */}
        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:12,marginBottom:20}}>
          <div style={{background:C.redBg,borderRadius:8,padding:16,border:`1px solid ${C.redBorder}`,textAlign:"center"}}>
            <div style={{fontSize:28,fontWeight:800,color:C.red}}>{aperto.length}</div>
            <div style={{fontSize:11,color:C.red,fontWeight:600}}>ALERTA</div>
            <div style={{fontSize:9,color:C.textMuted,marginTop:4}}>Aperto ou extremo</div>
          </div>
          <div style={{background:C.amberBg,borderRadius:8,padding:16,border:`1px solid ${C.amberBorder}`,textAlign:"center"}}>
            <div style={{fontSize:28,fontWeight:800,color:C.amber}}>{neutro.length}</div>
            <div style={{fontSize:11,color:C.amber,fontWeight:600}}>NEUTRO</div>
            <div style={{fontSize:9,color:C.textMuted,marginTop:4}}>Equilibrado</div>
          </div>
          <div style={{background:C.greenBg,borderRadius:8,padding:16,border:`1px solid ${C.greenBorder}`,textAlign:"center"}}>
            <div style={{fontSize:28,fontWeight:800,color:C.green}}>{excesso.length}</div>
            <div style={{fontSize:11,color:C.green,fontWeight:600}}>ATENCAO</div>
            <div style={{fontSize:9,color:C.textMuted,marginTop:4}}>Acima media ou excesso</div>
          </div>
        </div>

        {/* -- Tabela de Estoque Real USDA -- */}
        {realStocks.length > 0 && (
          <div style={{marginBottom:20}}>
            <div style={{fontSize:12,fontWeight:700,color:C.textDim,marginBottom:8}}>Ending Stocks -- USDA PSD + QuickStats</div>
            <div style={{overflowX:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                <thead><TableHeader cols={["Commodity","Ending Stock","M\u00e9dia 5Y","Unidade","Desvio %","Estado","Fonte"]} /></thead>
                <tbody>
                  {realStocks.map(st=>{
                    const nm=COMMODITIES.find(c=>c.sym===st.symbol)?.name||st.symbol;
                    const dev=st.stock_avg&&st.stock_avg>0?((st.stock_current-st.stock_avg)/st.stock_avg*100):0;
                    const devCol=Math.abs(dev)>15?C.red:Math.abs(dev)>5?C.amber:C.green;
                    const isSel=stockSelected===st.symbol;
                    const fmtVal = (v:number|null) => v == null ? '--' : v >= 10000 ? (v/1000).toFixed(1)+'k' : v.toLocaleString();
                    return (
                      <tr key={st.symbol+"_real"} onClick={()=>setStockSelected(st.symbol)} style={{borderBottom:`1px solid ${C.border}`,cursor:"pointer",background:isSel?"rgba(59,130,246,.06)":"transparent"}}>
                        <td style={{padding:"8px 12px",fontWeight:700}}>{st.symbol} <span style={{color:C.textMuted,fontWeight:400}}>({nm})</span></td>
                        <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",fontWeight:700,fontSize:12}}>{fmtVal(st.stock_current)}</td>
                        <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",color:C.textDim}}>{fmtVal(st.stock_avg)}</td>
                        <td style={{padding:"8px 12px",textAlign:"center",fontSize:10,color:C.textMuted}}>{st.stock_unit||"--"}</td>
                        <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",fontWeight:700,color:devCol}}>{dev>=0?"+":""}{dev.toFixed(1)}%</td>
                        <td style={{padding:"8px 12px",textAlign:"center"}}><Badge label={(st.state||'').replace(/_/g," ")} color={(st.state||'').includes("APERTO")||(st.state||'').includes("ABAIXO")?C.red:(st.state||'').includes("EXCESSO")||(st.state||'').includes("ACIMA")?C.green:C.amber} /></td>
                        <td style={{padding:"8px 12px",textAlign:"center",fontSize:9,color:C.textMuted}}>{st.source||'--'}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* -- Proxy de Preço -- enriquecido com COT + sazonalidade -- */}
        <div>
          <div style={{fontSize:12,fontWeight:700,color:C.textDim,marginBottom:10}}>Proxy de Preço -- Commodities sem dados USDA</div>
          <div style={{display:"grid",gap:10}}>
            {stocksList.filter(s=>!s.data_available?.stock_real).map(st=>{
              const sym=st.symbol;
              const nm=COMMODITIES.find(c=>c.sym===sym)?.name||sym;
              const pva=st.price_vs_avg||0;
              const stateLabel=(st.state||"").replace(/_/g," ");
              const stateCol=st.state?.includes("ELEVADO")||st.state?.includes("DEPRIMIDO")?C.red:st.state?.includes("ACIMA")||st.state?.includes("ABAIXO")?C.amber:C.green;

              // COT enrichment
              const cotD=cot?.commodities?.[sym]?.disaggregated;
              const mmHist=cotD?.history?.map((h:any)=>h.managed_money_net||h.mm_net||0).filter((v:number)=>v!==0);
              let mmNet:number|null=null, mmPctile:number|null=null;
              if(mmHist&&mmHist.length>=10){
                mmNet=mmHist[mmHist.length-1];
                const sorted=[...mmHist].sort((a:number,b:number)=>a-b);
                mmPctile=Math.round(sorted.findIndex((v:number)=>v>=mmNet!)/sorted.length*100);
              }

              // Seasonality -- current month avg return
              const candles=prices?.[sym];
              let seasonAvg:number|null=null, seasonPctPos:number|null=null, monthLabel="";
              if(candles&&candles.length>=252){
                const cm=new Date().getMonth();
                monthLabel=["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"][cm];
                const rets:number[]=[];
                for(let i=22;i<candles.length;i++){
                  if(new Date(candles[i].date).getMonth()===cm){
                    const prev=candles[i-22]?.close;
                    if(prev&&prev>0)rets.push((candles[i].close-prev)/prev*100);
                  }
                }
                if(rets.length>=3){
                  seasonAvg=rets.reduce((s,v)=>s+v,0)/rets.length;
                  seasonPctPos=Math.round(rets.filter(r=>r>0).length/rets.length*100);
                }
              }

              // Weekly / monthly change
              let weekChg:number|null=null, monthChg:number|null=null;
              if(candles&&candles.length>=5){
                weekChg=(candles[candles.length-1].close-candles[candles.length-5].close)/candles[candles.length-5].close*100;
              }
              if(candles&&candles.length>=22){
                monthChg=(candles[candles.length-1].close-candles[candles.length-22].close)/candles[candles.length-22].close*100;
              }

              // Combined state badge: price vs avg + COT percentile
              let combLabel=stateLabel, combCol=stateCol;
              if(mmPctile!==null){
                if(mmPctile>75&&pva>10){combLabel="SOBRECOMPRADO";combCol=C.red;}
                else if(mmPctile<25&&pva<-10){combLabel="SOBREVENDIDO";combCol=C.green;}
              }

              return (
                <div key={sym+"_proxy"} style={{background:C.panelAlt,borderRadius:8,padding:"12px 16px",border:`1px solid ${C.border}`,borderLeft:`3px solid ${combCol}`}}>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:6}}>
                    <div style={{display:"flex",alignItems:"baseline",gap:8}}>
                      <span style={{fontSize:13,fontWeight:700,color:C.text}}>{sym}</span>
                      <span style={{fontSize:11,color:C.textMuted}}>-- {nm}</span>
                    </div>
                    <div style={{display:"flex",alignItems:"center",gap:10}}>
                      <span style={{fontSize:16,fontWeight:700,fontFamily:"monospace",color:C.text}}>{st.price?.toFixed(2)||"--"}</span>
                      <span style={{fontSize:11,fontWeight:600,fontFamily:"monospace",color:pva>=0?C.red:C.green}}>{pva>=0?"+":""}{pva.toFixed(1)}% vs média</span>
                    </div>
                  </div>
                  <div style={{display:"flex",gap:16,flexWrap:"wrap",fontSize:10,color:C.textDim}}>
                    {mmNet!==null&&mmPctile!==null&&(
                      <span>COT: MM {mmNet>=0?"+":""}{(mmNet/1000).toFixed(1)}K <span style={{color:mmPctile>60?C.green:mmPctile<40?C.red:C.textMuted,fontWeight:600}}>Pctl {mmPctile}%</span></span>
                    )}
                    {seasonAvg!==null&&seasonPctPos!==null&&(
                      <span>Sazonal {monthLabel}: <span style={{color:seasonAvg>=0?C.green:C.red,fontWeight:600}}>{seasonAvg>0?"+":""}{seasonAvg.toFixed(1)}%</span> {seasonPctPos}%+</span>
                    )}
                    {weekChg!==null&&(
                      <span>Sem: <span style={{color:weekChg>=0?C.green:C.red,fontFamily:"monospace"}}>{weekChg>=0?"+":""}{weekChg.toFixed(2)}%</span></span>
                    )}
                    {monthChg!==null&&(
                      <span>Mês: <span style={{color:monthChg>=0?C.green:C.red,fontFamily:"monospace"}}>{monthChg>=0?"+":""}{monthChg.toFixed(2)}%</span></span>
                    )}
                    <span style={{fontSize:9,fontWeight:700,padding:"1px 8px",borderRadius:3,background:`${combCol}22`,color:combCol}}>{combLabel}</span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    );
  };
  // -- Tab: Energia ---------------------------------------------------------
  const renderEnergia = () => {
    const s = eiaData?.series || {};
    const get = (id:string) => s[id] || null;

    // Helper: spark SVG
    const Spark = ({data,color,w=180,h=50}:{data:{period:string;value:number}[];color:string;w?:number;h?:number}) => {
      if(!data||data.length<2) return <span style={{color:C.textMuted,fontSize:11}}>--</span>;
      const vals = [...data].reverse().map(d=>d.value);
      const mn=Math.min(...vals),mx=Math.max(...vals),rng=mx-mn||1;
      const pts=vals.map((v,i)=>`${(i/(vals.length-1))*w},${h-4-((v-mn)/rng)*(h-8)}`).join(" ");
      return (
        <svg width={w} height={h} style={{display:"block"}}>
          <defs><linearGradient id={`sg-${color.replace('#','')}`} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.25}/><stop offset="100%" stopColor={color} stopOpacity={0}/>
          </linearGradient></defs>
          <polygon points={`0,${h} ${pts} ${w},${h}`} fill={`url(#sg-${color.replace('#','')})`}/>
          <polyline points={pts} fill="none" stroke={color} strokeWidth={2} strokeLinecap="round" strokeLinejoin="round"/>
          <circle cx={w} cy={h-4-((vals[vals.length-1]-mn)/rng)*(h-8)} r={3} fill={color} stroke="rgba(0,0,0,0.4)" strokeWidth={1}/>
        </svg>
      );
    };

    // Helper: range bar (52w)
    const RangeBar = ({series}:{series:EIASeries|null}) => {
      if(!series||series.pct_range_52w===null||series.high_52w===null||series.low_52w===null) return null;
      const pct = series.pct_range_52w;
      const color = pct>=80?"#ef4444":pct>=60?"#f59e0b":pct>=40?"#94a3b8":pct>=20?"#60a5fa":"#22c55e";
      const label = pct>=80?"Perto da máxima":pct>=60?"Acima da média":pct>=40?"Na média":pct>=20?"Abaixo da média":"Perto da mínima";
      return (
        <div>
          <div style={{display:"flex",justifyContent:"space-between",fontSize:10,color:C.textMuted,marginBottom:4}}>
            <span>Mín: {series.low_52w.toLocaleString("en-US",{maximumFractionDigits:1})}</span>
            <span style={{color,fontWeight:700}}>{label}</span>
            <span>Máx: {series.high_52w.toLocaleString("en-US",{maximumFractionDigits:1})}</span>
          </div>
          <div style={{position:"relative",height:10,borderRadius:5,overflow:"hidden",
            background:"linear-gradient(90deg, #22c55e20 0%, #60a5fa20 25%, #94a3b820 50%, #f59e0b20 75%, #ef444420 100%)"}}>
            <div style={{position:"absolute",top:-1,bottom:-1,left:`calc(${Math.min(98,Math.max(2,pct))}% - 5px)`,
              width:10,borderRadius:5,background:color,boxShadow:`0 0 8px ${color}80`,transition:"left 0.6s ease"}}/>
          </div>
        </div>
      );
    };

    // Helper: change badge
    const ChgBadge = ({val,suffix="WoW"}:{val:number|null|undefined;suffix?:string}) => {
      if(val===null||val===undefined) return <span style={{color:C.textMuted,fontSize:10}}>--</span>;
      const color = val>0?"#10b981":val<0?"#ef4444":"#94a3b8";
      const arrow = val>0?"?":val<0?"?":"?";
      return <span style={{fontSize:11,fontWeight:600,color}}>{arrow} {val>0?"+":""}{val.toFixed(1)}% {suffix}</span>;
    };

    // -- Data cards config --
    const priceCards = [
      {id:"wti_spot",label:"Petróleo WTI",icon:"",color:"#f59e0b",
        why:"Preço do petróleo afeta o custo do diesel, frete e insumos agrícolas. Petróleo em alta = custo do produtor sobe."},
      {id:"natural_gas_spot",label:"Gás Natural (Henry Hub)",icon:"",color:"#3b82f6",
        why:"Gás natural é matéria-prima de fertilizantes nitrogenados (ureia). Gás caro = adubo caro."},
      {id:"diesel_retail",label:"Diesel (Preço Bomba EUA)",icon:"",color:"#ef4444",
        why:"Diesel é o principal combustível do campo -- colheitadeiras, caminhões, irrigação. Cada centavo afeta a margem do produtor."},
      {id:"gasoline_retail",label:"Gasolina (Preço Bomba EUA)",icon:"",color:"#a855f7",
        why:"Gasolina alta incentiva etanol de milho, aumentando demanda por milho e puxando preços agrícolas."},
    ];

    const stockCards = [
      {id:"crude_stocks",label:"Estoques Petróleo Cru",icon:"",color:"#f59e0b",unit:"MBbl",
        why:"Estoques altos = petróleo tende a cair = diesel mais barato = custo agrícola menor. Estoques baixos = risco de alta."},
      {id:"gasoline_stocks",label:"Estoques Gasolina",icon:"",color:"#a855f7",unit:"MBbl",
        why:"Estoques baixos de gasolina = mais demanda por etanol = mais demanda por milho = milho sobe."},
      {id:"distillate_stocks",label:"Estoques Diesel/Destilados",icon:"",color:"#ef4444",unit:"MBbl",
        why:"Estoque de diesel apertado = risco de frete caro na colheita. Produtor deve monitorar antes de contratar transporte."},
      {id:"ethanol_stocks",label:"Estoques Etanol",icon:"",color:"#22c55e",unit:"MBbl",
        why:"Etanol consome ~40% do milho americano. Estoques baixos de etanol = usinas precisam comprar mais milho."},
    ];

    const prodCards = [
      {id:"ethanol_production",label:"Produção Etanol",icon:"",color:"#22c55e",unit:"MBbl/d",
        why:"Produção alta de etanol = forte demanda por milho. Queda na produção = demanda enfraquecendo."},
      {id:"refinery_utilization",label:"Utilização Refinarias",icon:"",color:"#f59e0b",unit:"%",
        why:"Refinarias a pleno = demanda forte por combustíveis. Se cair abaixo de 85%, sinal de desaceleração econômica."},
      {id:"crude_production",label:"Produção Petróleo EUA",icon:"",color:"#3b82f6",unit:"MBbl/d",
        why:"EUA é o maior produtor mundial. Produção recorde = petróleo tende a cair = diesel mais barato para o campo."},
    ];

    if(!eiaData) return (
      <div>
        <SectionTitle>Energia -- Petróleo, Gás e Combustíveis</SectionTitle>
        <DataPlaceholder title="Sem dados EIA" detail="Execute o pipeline para coletar dados da EIA (Energy Information Administration)" />
      </div>
    );

    // Count alerts
    const alertCount = Object.values(s).filter(v=>v.pct_range_52w!==null&&(v.pct_range_52w>=80||v.pct_range_52w<=20)).length;

    return (
    <div>
      <SectionTitle>Energia -- Petróleo, Gás e Combustíveis</SectionTitle>
      <div style={{fontSize:13,color:C.textMuted,marginBottom:20,lineHeight:1.6,maxWidth:750}}>
        Dados semanais da EIA (Energy Information Administration). Energia afeta diretamente o custo do produtor rural
        -- diesel, frete, fertilizantes e demanda por etanol.
        <strong style={{color:C.textDim}}> Atualizado: {eiaData.metadata.collected_at?.slice(0,10)||"--"}</strong>
      </div>

      {/* -- PREÇOS -- */}
      <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:12}}>Preços -- Quanto custa a energia?</div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(2,1fr)",gap:14,marginBottom:28}}>
        {priceCards.map(pc=>{
          const d = get(pc.id);
          if(!d||!d.latest_value) return null;
          const trendUp = (d.wow_change_pct||0)>0;
          return (
            <div key={pc.id} style={{background:C.panelAlt,borderRadius:12,padding:"18px 20px",
              border:`1px solid ${C.border}`,borderLeft:`4px solid ${pc.color}`}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:10}}>
                <div style={{display:"flex",alignItems:"center",gap:8}}>
                  <span style={{fontSize:20}}>{pc.icon}</span>
                  <span style={{fontSize:14,fontWeight:700,color:C.text}}>{pc.label}</span>
                </div>
                <ChgBadge val={d.wow_change_pct} />
              </div>
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16,alignItems:"center"}}>
                <div>
                  <div style={{fontSize:32,fontWeight:800,fontFamily:"monospace",color:C.text,letterSpacing:-1}}>
                    ${d.latest_value.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:3})}
                  </div>
                  <div style={{fontSize:11,color:C.textMuted}}>{d.unit} * {d.latest_period}</div>
                  <div style={{display:"flex",gap:12,marginTop:6}}>
                    <ChgBadge val={d.mom_change_pct} suffix="MoM"/>
                    {d.yoy_change_pct!==null && <ChgBadge val={d.yoy_change_pct} suffix="YoY"/>}
                  </div>
                </div>
                <Spark data={d.history.slice(0,26)} color={pc.color} />
              </div>
              <div style={{marginTop:12}}><RangeBar series={d}/></div>
              <details style={{marginTop:10,cursor:"pointer"}}>
                <summary style={{fontSize:11,color:C.textMuted,listStyle:"none",display:"flex",alignItems:"center",gap:4}}>
                  <span style={{fontSize:9}}>?</span> Por que me interessa?
                </summary>
                <div style={{marginTop:6,padding:"8px 12px",borderRadius:6,background:"rgba(59,130,246,0.05)",
                  border:"1px solid rgba(59,130,246,0.1)",fontSize:12,color:C.textDim,lineHeight:1.5}}>
                  ?? {pc.why}
                </div>
              </details>
            </div>
          );
        })}
      </div>

      {/* -- ESTOQUES -- */}
      <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:12}}>Estoques -- Quanto tem guardado?</div>
      <div style={{fontSize:12,color:C.textMuted,marginBottom:14,lineHeight:1.5}}>
        Estoques baixos = risco de preço subir. Estoques altos = pressão de baixa.
        A barra mostra onde o estoque está dentro do range do último ano.
      </div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(2,1fr)",gap:14,marginBottom:28}}>
        {stockCards.map(sc=>{
          const d = get(sc.id);
          if(!d||!d.latest_value) return null;
          const pct = d.pct_range_52w;
          const isLow = pct!==null && pct<=25;
          const isHigh = pct!==null && pct>=75;
          const alertColor = isLow?"#f59e0b":isHigh?"#3b82f6":"rgba(255,255,255,0.05)";
          const alertBg = isLow?"rgba(245,158,11,0.04)":isHigh?"rgba(59,130,246,0.04)":"transparent";
          return (
            <div key={sc.id} style={{background:alertBg,borderRadius:12,padding:"18px 20px",
              border:`1px solid ${C.border}`,borderLeft:`4px solid ${sc.color}`}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:10}}>
                <div style={{display:"flex",alignItems:"center",gap:8}}>
                  <span style={{fontSize:18}}>{sc.icon}</span>
                  <span style={{fontSize:14,fontWeight:700,color:C.text}}>{sc.label}</span>
                </div>
                {isLow && <span style={{fontSize:10,fontWeight:700,padding:"3px 10px",borderRadius:12,
                  background:"rgba(245,158,11,0.1)",border:"1px solid rgba(245,158,11,0.3)",color:"#fbbf24"}}>Baixo</span>}
                {isHigh && <span style={{fontSize:10,fontWeight:700,padding:"3px 10px",borderRadius:12,
                  background:"rgba(59,130,246,0.1)",border:"1px solid rgba(59,130,246,0.3)",color:"#60a5fa"}}>Alto</span>}
              </div>
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16,alignItems:"center"}}>
                <div>
                  <div style={{fontSize:26,fontWeight:800,fontFamily:"monospace",color:C.text}}>
                    {d.latest_value>=1000?`${(d.latest_value/1000).toFixed(1)}M`:d.latest_value.toLocaleString("en-US")}
                  </div>
                  <div style={{fontSize:11,color:C.textMuted}}>{sc.unit} * {d.latest_period}</div>
                  <div style={{marginTop:4}}><ChgBadge val={d.wow_change_pct} /></div>
                </div>
                <Spark data={d.history.slice(0,26)} color={sc.color} />
              </div>
              <div style={{marginTop:12}}><RangeBar series={d}/></div>
              <details style={{marginTop:10,cursor:"pointer"}}>
                <summary style={{fontSize:11,color:C.textMuted,listStyle:"none",display:"flex",alignItems:"center",gap:4}}>
                  <span style={{fontSize:9}}>?</span> Por que me interessa?
                </summary>
                <div style={{marginTop:6,padding:"8px 12px",borderRadius:6,background:"rgba(59,130,246,0.05)",
                  border:"1px solid rgba(59,130,246,0.1)",fontSize:12,color:C.textDim,lineHeight:1.5}}>
                  ?? {sc.why}
                </div>
              </details>
            </div>
          );
        })}
      </div>

      {/* -- PRODUÇÃO -- */}
      <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:12}}>Produção e Refino</div>
      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:14,marginBottom:28}}>
        {prodCards.map(pc=>{
          const d = get(pc.id);
          if(!d||!d.latest_value) return null;
          return (
            <div key={pc.id} style={{background:C.panelAlt,borderRadius:12,padding:"16px 18px",
              border:`1px solid ${C.border}`,borderLeft:`4px solid ${pc.color}`}}>
              <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:10}}>
                <span style={{fontSize:16}}>{pc.icon}</span>
                <span style={{fontSize:13,fontWeight:700,color:C.text}}>{pc.label}</span>
              </div>
              <div style={{fontSize:26,fontWeight:800,fontFamily:"monospace",color:C.text}}>
                {pc.id==="refinery_utilization"
                  ? `${d.latest_value.toFixed(1)}%`
                  : d.latest_value>=1000
                  ? `${(d.latest_value/1000).toFixed(1)}M`
                  : d.latest_value.toLocaleString("en-US")}
              </div>
              <div style={{fontSize:11,color:C.textMuted}}>{pc.unit} * {d.latest_period}</div>
              <div style={{marginTop:4}}><ChgBadge val={d.wow_change_pct}/></div>
              <div style={{marginTop:10}}>
                <Spark data={d.history.slice(0,26)} color={pc.color} w={160} h={40}/>
              </div>
              <details style={{marginTop:8,cursor:"pointer"}}>
                <summary style={{fontSize:11,color:C.textMuted,listStyle:"none",display:"flex",alignItems:"center",gap:4}}>
                  <span style={{fontSize:9}}>?</span> Por que me interessa?
                </summary>
                <div style={{marginTop:6,padding:"8px 10px",borderRadius:6,background:"rgba(59,130,246,0.05)",
                  border:"1px solid rgba(59,130,246,0.1)",fontSize:11,color:C.textDim,lineHeight:1.5}}>
                  ?? {pc.why}
                </div>
              </details>
            </div>
          );
        })}
      </div>

      {/* Help footer */}
      <div style={{padding:"16px 20px",borderRadius:12,
        background:"rgba(255,255,255,0.02)",border:`1px solid ${C.border}`}}>
        <div style={{fontSize:12,fontWeight:700,color:C.textDim,marginBottom:10}}>Como a energia afeta o agro</div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:20,fontSize:12,color:C.textMuted,lineHeight:1.6}}>
          <div><strong style={{color:C.textDim}}>Diesel e frete</strong><br/>Petróleo sobe ? diesel sobe ? custo de produção e transporte agrícola aumenta. Produtor perde margem.</div>
          <div><strong style={{color:C.textDim}}>Etanol e milho</strong><br/>~40% do milho dos EUA vira etanol. Mais demanda por etanol = mais demanda por milho = preço do milho sobe.</div>
          <div><strong style={{color:C.textDim}}>Gás e fertilizantes</strong><br/>Gás natural é matéria-prima da ureia. Gás caro ? adubo caro ? custo da lavoura sobe, especialmente milho e trigo.</div>
        </div>
      </div>
    </div>
    );
  };

      // -- Tab: Físico Intl ----------------------------------------------------
  const renderFisicoIntl = () => {
    const physData = physical;
    const usCash = physData?.us_cash || {};
    const usList = Object.entries(usCash) as [string,any][];
    const basisColor = (v:number) => v < -10 ? C.red : v < -5 ? C.amber : v < 0 ? C.textDim : C.green;
    return (
    <div>
      {/* -- US CASH MARKETS (dados reais USDA) -- */}
      <SectionTitle> Mercado Físico EUA -- Cash vs Futures</SectionTitle>
      <div style={{fontSize:11,color:C.textMuted,marginBottom:16}}>
        Preços cash USDA (Prices Received) vs front-month futures. Atualizado: {physData?.timestamp?.slice(0,10) || "--"}
      </div>
      {usList.length === 0 ? (
        <div style={{padding:20,textAlign:"center",color:C.textMuted,fontSize:12}}>
          Dados físicos não carregados. Execute o pipeline para gerar physical.json
        </div>
      ) : (
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:11,marginBottom:32}}>
          <thead><tr style={{borderBottom:`2px solid ${C.border}`}}>
            {["Commodity","Cash","Futures","Basis","Basis %","Trend","12m History","Fonte"].map(h=>(
              <th key={h} style={{padding:"8px 10px",textAlign:h==="Commodity"||h==="Fonte"||h==="Trend"||h==="12m History"?"left":"right",
                fontSize:10,fontWeight:700,color:C.textMuted,textTransform:"uppercase",letterSpacing:.5}}>{h}</th>
            ))}
          </tr></thead>
          <tbody>
            {usList.map(([sym,d])=>{
              const hist = d.history || [];
              const maxH = Math.max(...hist.map((h:any)=>h.value));
              const minH = Math.min(...hist.map((h:any)=>h.value));
              const rangeH = maxH - minH || 1;
              const sparkW = 120; const sparkH = 28;
              const points = hist.map((h:any,i:number)=>`${(i/(hist.length-1))*sparkW},${sparkH-((h.value-minH)/rangeH)*sparkH}`).join(" ");
              return (
              <tr key={sym} style={{borderBottom:`1px solid ${C.border}`}}>
                <td style={{padding:"8px 10px",fontWeight:600}}>
                  <span style={{color:C.text}}>{sym}</span>
                  <span style={{color:C.textMuted,marginLeft:6,fontSize:10}}>{d.label}</span>
                </td>
                <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"monospace",fontWeight:700,color:C.text,fontSize:13}}>
                  {d.cash_price?.toFixed(1)} <span style={{fontSize:9,color:C.textMuted}}>{d.cash_unit}</span>
                </td>
                <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"monospace",color:C.textDim}}>
                  {d.futures_price?.toFixed(1)}
                </td>
                <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"monospace",fontWeight:600,color:basisColor(d.basis_pct)}}>
                  {d.basis > 0 ? "+" : ""}{d.basis?.toFixed(1)}
                </td>
                <td style={{padding:"8px 10px",textAlign:"right"}}>
                  <span style={{display:"inline-block",padding:"2px 8px",borderRadius:4,fontSize:10,fontWeight:700,
                    color:basisColor(d.basis_pct),
                    background:d.basis_pct<-10?C.redBg:d.basis_pct<-5?C.amberBg:d.basis_pct<0?"rgba(148,163,184,.1)":C.greenBg}}>
                    {d.basis_pct > 0 ? "+" : ""}{d.basis_pct?.toFixed(1)}%
                  </span>
                </td>
                <td style={{padding:"8px 10px",color:C.textDim,fontSize:10}}>{d.trend}</td>
                <td style={{padding:"8px 10px"}}>
                  <svg width={sparkW} height={sparkH} style={{display:"block"}}>
                    <polyline points={points} fill="none" stroke={C.cyan} strokeWidth={1.5} />
                    <circle cx={(hist.length-1)/(hist.length-1)*sparkW} cy={sparkH-((hist[hist.length-1]?.value-minH)/rangeH)*sparkH} r={2.5} fill={C.cyan} />
                  </svg>
                  <div style={{fontSize:8,color:C.textMuted,display:"flex",justifyContent:"space-between",width:sparkW}}>
                    <span>{hist[0]?.period}</span><span>{hist[hist.length-1]?.period}</span>
                  </div>
                </td>
                <td style={{padding:"8px 10px"}}>
                  <span style={{display:"inline-block",padding:"2px 8px",borderRadius:4,fontSize:9,fontWeight:600,
                    color:C.green,background:C.greenBg,border:`1px solid ${C.greenBorder}`}}>USDA</span>
                </td>
              </tr>
            );})}
          </tbody>
        </table>
      )}

      {/* -- INTERNATIONAL (dados reais) -- */}
      <SectionTitle>Mercado Físico Internacional</SectionTitle>
      <div style={{fontSize:11,color:C.textMuted,marginBottom:16}}>
        {physIntl ? <>Fontes: CEPEA/ESALQ via Notícias Agrícolas + MAGyP FOB Argentina. Atualizado: {physIntl.timestamp?.slice(0,10) || "--"} | <span style={{color:C.green}}>{physIntl.markets_with_data} com dados</span> / {physIntl.total_markets} total</> : "Carregando dados internacionais..."}
      </div>
      {(()=>{
        const intlData = physIntl?.international || {};
        const entries = Object.entries(intlData) as [string,any][];
        if(entries.length===0) return <div style={{padding:20,textAlign:"center",color:C.textMuted,fontSize:12}}>Execute o pipeline para gerar physical_intl.json</div>;
        const withData = entries.filter(([,d])=>d.price!=null);
        const noData = entries.filter(([,d])=>d.price==null);
        const srcBadge = (s:string) => {
          const col = s.includes("CEPEA")?C.green:s.includes("MAGyP")?C.blue:C.textMuted;
          const bg = s.includes("CEPEA")?C.greenBg:s.includes("MAGyP")?C.blueBg:"rgba(148,163,184,.06)";
          const bdr = s.includes("CEPEA")?C.greenBorder:s.includes("MAGyP")?"rgba(59,130,246,.3)":"rgba(148,163,184,.15)";
          return <span style={{display:"inline-block",padding:"2px 8px",borderRadius:4,fontSize:9,fontWeight:600,color:col,background:bg,border:`1px solid ${bdr}`}}>{s}</span>;
        };
        return <>
          {/* With data */}
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11,marginBottom:24}}>
            <thead><tr style={{borderBottom:`2px solid ${C.border}`}}>
              {["Mercado","Preço","Unidade","Data","Variação","Sparkline","Fonte"].map(h=>(
                <th key={h} style={{padding:"8px 10px",textAlign:h==="Mercado"||h==="Fonte"||h==="Sparkline"?"left":"right",
                  fontSize:10,fontWeight:700,color:C.textMuted,textTransform:"uppercase",letterSpacing:.5}}>{h}</th>
              ))}
            </tr></thead>
            <tbody>
              {withData.map(([sym,d])=>{
                const hist = d.history||[];
                const sparkW=100; const sparkH=24;
                let points="";
                if(hist.length>=2){
                  const vals=hist.map((h:any)=>h.value);
                  const mx=Math.max(...vals);const mn=Math.min(...vals);const rng=mx-mn||1;
                  points=vals.map((v:number,i:number)=>`${(i/(vals.length-1))*sparkW},${sparkH-((v-mn)/rng)*sparkH}`).join(" ");
                }
                const trendColor = d.trend?.startsWith("+")?C.green:d.trend?.startsWith("-")?C.red:d.trend==="--"?C.textMuted:C.textDim;
                return (
                <tr key={sym} style={{borderBottom:`1px solid ${C.border}`}}>
                  <td style={{padding:"8px 10px",fontWeight:600}}>
                    <span style={{color:C.text}}>{d.label}</span>
                  </td>
                  <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"monospace",fontWeight:700,color:C.text,fontSize:13}}>
                    {typeof d.price==="number"?d.price.toLocaleString("pt-BR",{minimumFractionDigits:2,maximumFractionDigits:2}):"--"}
                  </td>
                  <td style={{padding:"8px 10px",textAlign:"right",fontSize:9,color:C.textMuted}}>{d.price_unit}</td>
                  <td style={{padding:"8px 10px",textAlign:"right",fontSize:10,color:C.textDim}}>{d.period}</td>
                  <td style={{padding:"8px 10px",textAlign:"right"}}>
                    <span style={{fontFamily:"monospace",fontWeight:600,fontSize:11,color:trendColor}}>{d.trend}</span>
                  </td>
                  <td style={{padding:"8px 10px"}}>
                    {points ? <svg width={sparkW} height={sparkH} style={{display:"block"}}>
                      <polyline points={points} fill="none" stroke={C.cyan} strokeWidth={1.5}/>
                    </svg> : <span style={{fontSize:9,color:C.textMuted}}>{"--"}</span>}
                  </td>
                  <td style={{padding:"8px 10px"}}>{srcBadge(d.source)}</td>
                </tr>);
              })}
            </tbody>
          </table>
          {/* Without data */}
          {noData.length>0 && <>
            <div style={{fontSize:11,fontWeight:600,color:C.textMuted,marginBottom:8,marginTop:16}}>{"⚠️"} Mercados sem API gratuita ({noData.length})</div>
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(280px,1fr))",gap:8}}>
              {noData.map(([sym,d])=>(
                <div key={sym} style={{padding:"8px 12px",background:"rgba(148,163,184,.04)",borderRadius:6,border:`1px solid ${C.border}`,fontSize:10}}>
                  <span style={{color:C.textDim}}>{d.label}</span>
                  <span style={{float:"right",color:C.textMuted,fontSize:9}}>{d.source}</span>
                </div>
              ))}
            </div>
          </>}
        </>;
      })()}
    </div>
    );
  };
  // Tab: Leitura do Dia
  const renderLeituraDoDia = () => {
    const catColors:Record<string,string> = {"usda":"#f59e0b","cftc":"#3b82f6","macro":"#a855f7","eia":"#22c55e","expiry":"#ef4444"};
    const catLabels:Record<string,string> = {"usda":"USDA","cftc":"CFTC","macro":"MACRO","eia":"EIA","expiry":"VENC"};
    const today = new Date().toLocaleDateString("sv-SE");
    const upcoming = calendar?.events?.filter((e:any)=>e.date>=today).slice(0,20) || [];
    const highImpact = upcoming.filter((e:any)=>e.impact==="high");
    const fredEntries = news?.fred ? Object.entries(news.fred) : [];
    const articles = news?.news || [];

    return (
    <div>
      <SectionTitle>Leitura do Dia</SectionTitle>

      {/* FILM ENTRY — Entrada do Dia (from intelligence_frame) */}
      {intelFrame?.film_entry && (
        <div style={{marginBottom:24, padding:16, background:'#0E1A24', borderRadius:8, border:'1px solid #DCB432', borderLeft:'4px solid #DCB432'}}>
          <div style={{display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:10}}>
            <div style={{fontSize:12, fontWeight:700, color:'#DCB432', textTransform:'uppercase', letterSpacing:'0.08em'}}>
              Entrada do Dia — {intelFrame.film_entry.date}
            </div>
            {intelFrame.by_commodity && (
              <div style={{display:'flex', gap:4}}>
                {Object.entries(intelFrame.by_commodity as Record<string,any>).filter(([,v]:any) => Math.abs(v.score) >= 2).slice(0,4).map(([sym, info]:any) => (
                  <span key={sym} style={{fontSize:9, padding:'2px 6px', borderRadius:4, fontWeight:700, fontFamily:'monospace',
                    background: info.score > 0 ? 'rgba(0,200,120,.12)' : 'rgba(220,60,60,.12)',
                    color: info.score > 0 ? '#00C878' : '#DC3C3C',
                  }}>{sym} {info.score > 0 ? '+' : ''}{info.score}</span>
                ))}
              </div>
            )}
          </div>
          <div style={{fontSize:13, color:'#e2e8f0', fontWeight:600, marginBottom:10, lineHeight:1.5}}>
            {intelFrame.film_entry.one_liner}
          </div>
          {intelFrame.film_entry.key_events?.length > 0 && (
            <div style={{display:'flex', flexDirection:'column', gap:4}}>
              {intelFrame.film_entry.key_events.map((ev:string, i:number) => (
                <div key={i} style={{fontSize:10, color:'#94a3b8', paddingLeft:8, borderLeft:'2px solid #1e3a4a'}}>
                  {ev}
                </div>
              ))}
            </div>
          )}
          {intelFrame.market_narrative && (
            <div style={{marginTop:10, fontSize:11, color:'#64748b', fontStyle:'italic', lineHeight:1.5}}>
              {intelFrame.market_narrative}
            </div>
          )}
        </div>
      )}

      {/* CALENDARIO */}
      <div style={{marginBottom:28}}>
        <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:14,display:"flex",alignItems:"center",gap:8}}>
          Calendario de Eventos
          {highImpact.length>0 && <span style={{fontSize:10,padding:"2px 8px",background:"rgba(239,68,68,.15)",color:C.red,borderRadius:10,fontWeight:600}}>{highImpact.length} HIGH IMPACT</span>}
        </div>
        {upcoming.length > 0 ? (
          <div style={{display:"grid",gap:6}}>
            {upcoming.map((ev:any,i:number)=>{
              const isToday = ev.date === today;
              const isHigh = ev.impact === "high";
              const catColor = catColors[ev.category] || C.textMuted;
              return (
                <div key={i} style={{display:"flex",alignItems:"center",gap:12,padding:"10px 14px",background:isToday?"rgba(245,158,11,.06)":C.panelAlt,borderRadius:6,border:`1px solid ${isToday?"rgba(245,158,11,.3)":C.border}`,borderLeft:`3px solid ${catColor}`}}>
                  <div style={{minWidth:70,fontSize:11,fontWeight:600,color:isToday?C.amber:C.textMuted,fontFamily:"monospace"}}>{ev.date.slice(5)}</div>
                  <span style={{fontSize:8,padding:"1px 6px",background:catColor+"22",color:catColor,borderRadius:4,fontWeight:700,textTransform:"uppercase",letterSpacing:.5}}>{catLabels[ev.category]||ev.category}</span>
                  <div style={{flex:1,fontSize:11,color:C.text,fontWeight:isHigh?700:400}}>{ev.name}</div>
                  {isHigh && <span style={{fontSize:8,padding:"1px 6px",background:"rgba(239,68,68,.15)",color:C.red,borderRadius:4,fontWeight:700}}>HIGH</span>}
                  {isToday && <span style={{fontSize:8,padding:"1px 6px",background:"rgba(245,158,11,.15)",color:C.amber,borderRadius:4,fontWeight:700}}>HOJE</span>}
                </div>
              );
            })}
          </div>
        ) : <div style={{fontSize:11,color:C.textMuted,padding:16}}>Sem eventos. Execute o pipeline.</div>}
      </div>

      {/* FRED MACRO */}
      {fredEntries.length > 0 && (
        <div style={{marginBottom:28}}>
          <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:14}}>Indicadores Macro (FRED)</div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(180px,1fr))",gap:10}}>
            {fredEntries.map(([key,ind]:any)=>{
              const isPos = ind.change > 0;
              const isNeg = ind.change < 0;
              return (
                <div key={key} style={{padding:14,background:C.panelAlt,borderRadius:8,border:`1px solid ${C.border}`}}>
                  <div style={{fontSize:9,color:C.textMuted,textTransform:"uppercase",marginBottom:6,letterSpacing:.5}}>{ind.name}</div>
                  <div style={{display:"flex",alignItems:"baseline",gap:8}}>
                    <span style={{fontSize:18,fontWeight:700,color:C.text}}>{typeof ind.value==="number"?ind.value.toFixed(2):ind.value}</span>
                    {ind.change!==null && <span style={{fontSize:10,fontWeight:600,color:isPos?C.green:isNeg?C.red:C.textMuted}}>{isPos?"+":""}{ind.change.toFixed(2)}</span>}
                  </div>
                  <div style={{fontSize:9,color:C.textMuted,marginTop:4}}>{ind.date}</div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* LEITURA ORIGINAL */}
      {reading ? (
        <>
          <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:14}}>Analise Integrada</div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(300px,1fr))",gap:16,marginBottom:24}}>
            {reading.blocos.map((bl:any,i:number)=>{
              const colors=[C.amber,C.red,C.purple,C.cyan,C.green,C.blue];
              const color=colors[i%colors.length];
              return (
                <div key={i} style={{padding:18,background:C.panelAlt,borderRadius:8,border:`1px solid ${C.border}`,borderLeft:`4px solid ${color}`}}>
                  <div style={{fontSize:13,fontWeight:700,color:color,marginBottom:8}}>{bl.title}</div>
                  <div style={{fontSize:11,color:C.textDim,lineHeight:1.7}}>{bl.body}</div>
                </div>
              );
            })}
          </div>
          <div style={{marginBottom:24}}>
            <div style={{fontSize:13,fontWeight:700,color:C.text,marginBottom:12}}>Perguntas do Dia</div>
            {reading.perguntas.map((q:any,i:number)=>(
              <div key={i} style={{display:"flex",gap:12,marginBottom:10,padding:"12px 16px",background:C.panelAlt,borderRadius:6,border:`1px solid ${C.border}`}}>
                <span style={{fontSize:16,fontWeight:800,color:C.blue,fontFamily:"monospace",minWidth:24}}>{i+1}.</span>
                <span style={{fontSize:11,color:C.textDim,lineHeight:1.6}}>{q}</span>
              </div>
            ))}
          </div>
          <div style={{padding:18,background:C.panelAlt,borderRadius:8,border:`1px solid ${C.border}`,marginBottom:20}}>
            <div style={{fontSize:13,fontWeight:700,color:C.text,marginBottom:12}}>Resumo Quantitativo</div>
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))",gap:16}}>
              {Object.entries(reading.resumo).map(([key,val]:any)=>(
                <div key={key}>
                  <div style={{fontSize:9,color:C.textMuted,textTransform:"uppercase",marginBottom:4}}>{key.replace(/_/g," ")}</div>
                  <div style={{fontSize:12,color:C.text,fontWeight:500}}>{val}</div>
                </div>
              ))}
            </div>
          </div>
        </>
      ) : (
        <DataPlaceholder title="Leitura indisponivel" detail="Execute o pipeline para gerar a leitura do dia" />
      )}

      {/* NOTICIAS */}
      {articles.length > 0 && (
        <div style={{marginBottom:28}}>
          <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:14}}>Noticias ({articles.length})</div>
          <div style={{display:"grid",gap:8}}>
            {articles.slice(0,15).map((a:any,i:number)=>(
              <div key={i} style={{padding:"12px 16px",background:C.panelAlt,borderRadius:6,border:`1px solid ${C.border}`,display:"flex",gap:12,alignItems:"flex-start"}}>
                <span style={{fontSize:8,padding:"2px 6px",background:a.category==="usda"?"rgba(245,158,11,.15)":"rgba(59,130,246,.15)",color:a.category==="usda"?C.amber:C.blue,borderRadius:4,fontWeight:700,whiteSpace:"nowrap",marginTop:2}}>{a.source}</span>
                <div style={{flex:1}}>
                  <div style={{fontSize:11,color:C.text,fontWeight:600,marginBottom:4}}>{a.title}</div>
                  {a.description && <div style={{fontSize:10,color:C.textDim,lineHeight:1.5}}>{a.description.slice(0,200)}</div>}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Disclaimer */}
      <div style={{padding:"12px 16px",borderRadius:6,background:"rgba(148,163,184,.03)",border:`1px solid rgba(148,163,184,.08)`,fontSize:9,color:C.textMuted}}>
        <strong>DISCLAIMER:</strong> Material informativo e educacional.
        {reading && <>{" "}Fontes: {reading.sources.join(", ")}. Gerado em {reading.date}.</>}
        {" "}Dados FRED via Federal Reserve. Nao constitui recomendacao de investimento.
      </div>
    </div>
    );
  };
  // -- Tab Switch ---------------------------------------------------------
  // Tab: Portfolio

  const STRIKE_DIVISORS: Record<string,number> = {CL:100,SI:100,GF:10,ZL:100,CC:1,ZC:100,ZS:100,ZW:100,ZM:100,SB:100,KC:100,CT:100,LE:100,HE:100,NG:100,GC:100};
  const OPT_MULTIPLIERS: Record<string,number> = {CL:1000,SI:5000,GF:500,ZL:600,CC:10,ZC:50,ZS:50,ZW:50,ZM:100,SB:1120,KC:375,CT:500,LE:400,HE:400,GF2:500,NG:10000,GC:100};

  function parsePosition(pos: any) {
    const ls = (pos.local_symbol || "").trim();
    const parts = ls.split(" ");
    const isOption = pos.sec_type === "FOP" || pos.sec_type === "OPT" || (parts.length >= 2 && /^[CP]\d+$/.test(parts[parts.length-1]));
    let strike = 0, optType = "", expLabel = "";
    if (isOption && parts.length >= 2) {
      const rightPart = parts[parts.length - 1];
      optType = rightPart[0];
      const rawStrike = parseInt(rightPart.slice(1));
      strike = rawStrike / (STRIKE_DIVISORS[pos.symbol] || 100);
      const expCode = parts[0].slice(-2);
      const monthMap: Record<string,string> = {F:"Jan",G:"Fev",H:"Mar",J:"Abr",K:"Mai",M:"Jun",N:"Jul",Q:"Ago",U:"Set",V:"Out",X:"Nov",Z:"Dez"};
      expLabel = (monthMap[expCode[0]] || expCode[0]) + "/2" + expCode[1];
    }
    const qty = pos.position || 0;
    const avgCost = pos.avg_cost || 0;
    const mktValue = pos.market_value || 0;
    return {
      symbol: pos.symbol, localSymbol: ls, secType: pos.sec_type || "",
      isOption, optType, strike, expLabel, qty, avgCost, mktValue,
      direction: qty > 0 ? "LONG" as const : "SHORT" as const,
    };
  }

  const renderPortfolio = () => {
    const summ = portfolio?.summary || {};
    const netLiq = parseFloat(summ.NetLiquidation || "0");
    const cash = parseFloat(summ.TotalCashValue || "0");
    const unrealPnl = parseFloat(summ.UnrealizedPnL || "0");
    const buyPow = parseFloat(summ.BuyingPower || "0");
    const grossPos = parseFloat(summ.GrossPositionValue || "0");
    const marginUtil = netLiq > 0 ? (grossPos / netLiq) * 100 : 0;

    const rawPositions: any[] = portfolio?.positions || [];
    const parsed = rawPositions.map(parsePosition);

    // Group by underlying
    const byUnd: Record<string, typeof parsed> = {};
    parsed.forEach(p => { if (!byUnd[p.symbol]) byUnd[p.symbol] = []; byUnd[p.symbol].push(p); });
    const symbols = Object.keys(byUnd).sort();

    // Underlying summary
    const undSummary = symbols.map(sym => {
      const legs = byUnd[sym];
      const netQty = legs.reduce((s, p) => s + p.qty, 0);
      const totalMV = legs.reduce((s, p) => s + p.mktValue, 0);
      const longMV = legs.filter(p => p.qty > 0).reduce((s, p) => s + p.mktValue, 0);
      const shortMV = legs.filter(p => p.qty < 0).reduce((s, p) => s + Math.abs(p.mktValue), 0);
      const dir = netQty > 0 ? "LONG" : netQty < 0 ? "SHORT" : "NEUTRO";
      const commName = COMMODITIES.find(c => c.sym === sym)?.name || sym;
      return { sym, commName, legs: legs.length, netQty, totalMV, longMV, shortMV, dir };
    });

    // SVG chart data — market value by underlying (exclude US-T for scale)
    const chartData = undSummary.filter(u => u.sym !== "US-T").sort((a, b) => b.totalMV - a.totalMV);
    const maxAbsMV = Math.max(...chartData.map(u => Math.abs(u.totalMV)), 1);
    const barH = 28;
    const chartH = Math.max(chartData.length * (barH + 8) + 40, 120);
    const chartW = 700;
    const labelW = 80;
    const barArea = chartW - labelW - 90;
    const centerX = labelW + barArea / 2;

    // Totals
    const totalLongMV = parsed.filter(p => p.qty > 0).reduce((s, p) => s + p.mktValue, 0);
    const totalShortMV = parsed.filter(p => p.qty < 0).reduce((s, p) => s + Math.abs(p.mktValue), 0);

    return (
    <div style={{overflow:"auto",padding:24}}>
      {/* SEÇÃO A: Summary Cards */}
      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(160px,1fr))",gap:12,marginBottom:24}}>
        {[
          {label:"Net Liquidation",value:netLiq,fmt:"$",color:C.text},
          {label:"Buying Power",value:buyPow,fmt:"$",color:C.blue},
          {label:"Cash",value:cash,fmt:"$",color:C.cyan},
          {label:"Gross Position",value:grossPos,fmt:"$",color:C.text},
          {label:"Unrealized P&L",value:unrealPnl,fmt:"$",color:unrealPnl>=0?C.green:C.red},
          {label:"Margin Util",value:marginUtil,fmt:"",color:marginUtil>50?C.red:C.green},
        ].map((card,i)=>(
          <div key={i} style={{padding:16,background:C.panelAlt,borderRadius:8,border:`1px solid ${C.border}`}}>
            <div style={{fontSize:9,color:C.textMuted,textTransform:"uppercase",letterSpacing:.5,marginBottom:8}}>{card.label}</div>
            <div style={{fontSize:20,fontWeight:700,color:card.color,fontFamily:"monospace"}}>{card.fmt}{typeof card.value==="number"?card.value.toLocaleString("en-US",{minimumFractionDigits:0,maximumFractionDigits:0}):card.value}{card.label==="Margin Util"?"%":""}</div>
          </div>
        ))}
      </div>

      <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:24}}>
        <button onClick={refreshIbkr} disabled={ibkrRefreshing} style={{padding:"6px 16px",fontSize:11,fontWeight:600,background:ibkrRefreshing?"#555":C.blue,color:"#fff",border:"none",borderRadius:6,cursor:ibkrRefreshing?"wait":"pointer"}}>
          {ibkrRefreshing?"Atualizando...":"Atualizar IBKR"}
        </button>
        <span style={{fontSize:10,color:C.textMuted}}>Ultima atualizacao: {ibkrTime}</span>
        <span style={{fontSize:10,color:C.textMuted}}>| {parsed.length} posicoes | {symbols.length} underlyings</span>
      </div>

      {/* SEÇÃO E: Resumo Direcional */}
      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:12,marginBottom:24}}>
        <div style={{padding:16,background:"rgba(0,200,120,.06)",borderRadius:8,border:"1px solid rgba(0,200,120,.2)",textAlign:"center"}}>
          <div style={{fontSize:9,color:"#00C878",fontWeight:600,textTransform:"uppercase",letterSpacing:.5,marginBottom:6}}>LONG Exposure</div>
          <div style={{fontSize:20,fontWeight:700,color:"#00C878",fontFamily:"monospace"}}>${totalLongMV.toLocaleString("en-US",{maximumFractionDigits:0})}</div>
        </div>
        <div style={{padding:16,background:"rgba(220,60,60,.06)",borderRadius:8,border:"1px solid rgba(220,60,60,.2)",textAlign:"center"}}>
          <div style={{fontSize:9,color:"#DC3C3C",fontWeight:600,textTransform:"uppercase",letterSpacing:.5,marginBottom:6}}>SHORT Exposure</div>
          <div style={{fontSize:20,fontWeight:700,color:"#DC3C3C",fontFamily:"monospace"}}>${totalShortMV.toLocaleString("en-US",{maximumFractionDigits:0})}</div>
        </div>
        <div style={{padding:16,background:unrealPnl>=0?"rgba(0,200,120,.06)":"rgba(220,60,60,.06)",borderRadius:8,border:`1px solid ${unrealPnl>=0?"rgba(0,200,120,.2)":"rgba(220,60,60,.2)"}`,textAlign:"center"}}>
          <div style={{fontSize:9,color:unrealPnl>=0?"#00C878":"#DC3C3C",fontWeight:600,textTransform:"uppercase",letterSpacing:.5,marginBottom:6}}>P&L TOTAL</div>
          <div style={{fontSize:20,fontWeight:700,color:unrealPnl>=0?"#00C878":"#DC3C3C",fontFamily:"monospace"}}>{unrealPnl>=0?"+":""}${unrealPnl.toLocaleString("en-US",{maximumFractionDigits:0})}</div>
        </div>
      </div>

      {/* GREEKS REAIS DO PORTFOLIO */}
      {greeksData?.portfolio_greeks && (
        <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:12,marginBottom:24,background:"#0E1A24",borderRadius:8,padding:16,border:"1px solid #1e3a4a"}}>
          <div>
            <div style={{fontSize:9,color:"#64748b",marginBottom:4}}>DELTA TOTAL</div>
            <div style={{fontSize:18,fontWeight:700,fontFamily:"monospace",color:greeksData.portfolio_greeks.total_delta>0?"#00C878":"#DC3C3C"}}>{greeksData.portfolio_greeks.total_delta>0?"+":""}{greeksData.portfolio_greeks.total_delta?.toFixed(1)}</div>
            <div style={{fontSize:9,color:"#64748b"}}>por $1 no underlying</div>
          </div>
          <div>
            <div style={{fontSize:9,color:"#64748b",marginBottom:4}}>THETA DIARIO</div>
            <div style={{fontSize:18,fontWeight:700,fontFamily:"monospace",color:greeksData.portfolio_greeks.total_theta>0?"#00C878":"#DC3C3C"}}>{greeksData.portfolio_greeks.total_theta>0?"+":""}${greeksData.portfolio_greeks.total_theta?.toFixed(0)}/dia</div>
            <div style={{fontSize:9,color:"#64748b"}}>decaimento de tempo</div>
          </div>
          <div>
            <div style={{fontSize:9,color:"#64748b",marginBottom:4}}>VEGA TOTAL</div>
            <div style={{fontSize:18,fontWeight:700,fontFamily:"monospace",color:"#DCB432"}}>{greeksData.portfolio_greeks.total_vega>0?"+":""}${greeksData.portfolio_greeks.total_vega?.toFixed(0)}</div>
            <div style={{fontSize:9,color:"#64748b"}}>por 1% de IV</div>
          </div>
          <div>
            <div style={{fontSize:9,color:"#64748b",marginBottom:4}}>COBERTURA</div>
            <div style={{fontSize:18,fontWeight:700,fontFamily:"monospace",color:"#00C878"}}>{greeksData.portfolio_greeks.positions_with_greeks}/{greeksData.portfolio_greeks.positions_with_greeks+greeksData.portfolio_greeks.positions_without_greeks}</div>
            <div style={{fontSize:9,color:"#64748b"}}>posicoes com Greeks</div>
          </div>
        </div>
      )}

      {/* SEÇÃO B: Exposição por Underlying */}
      <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:14}}>Exposicao por Underlying ({symbols.length})</div>
      <div style={{background:C.panelAlt,borderRadius:8,border:`1px solid ${C.border}`,overflow:"hidden",marginBottom:24}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
          <thead>
            <tr style={{borderBottom:`1px solid ${C.border}`}}>
              {["Underlying","Nome","Legs","Net Qty","Direcao","Long MV","Short MV","Net MV"].map(h=>(
                <th key={h} style={{padding:"10px 12px",textAlign:"left",color:C.textMuted,fontWeight:600,fontSize:9,textTransform:"uppercase"}}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {undSummary.map(u=>(
              <tr key={u.sym} style={{borderBottom:`1px solid ${C.border}22`}}>
                <td style={{padding:"8px 12px",color:C.amber,fontWeight:700,fontFamily:"monospace"}}>{u.sym}</td>
                <td style={{padding:"8px 12px",color:C.text,fontSize:10}}>{u.commName}</td>
                <td style={{padding:"8px 12px",color:C.textMuted,fontFamily:"monospace",textAlign:"center"}}>{u.legs}</td>
                <td style={{padding:"8px 12px",color:u.netQty>0?C.green:u.netQty<0?C.red:C.textMuted,fontWeight:600,fontFamily:"monospace"}}>{u.netQty>0?"+":""}{u.netQty}</td>
                <td style={{padding:"8px 12px"}}><span style={{fontSize:8,fontWeight:700,padding:"2px 6px",borderRadius:3,
                  background:u.dir==="LONG"?"rgba(0,200,120,.15)":u.dir==="SHORT"?"rgba(220,60,60,.15)":"rgba(148,163,184,.1)",
                  color:u.dir==="LONG"?"#00C878":u.dir==="SHORT"?"#DC3C3C":"#8899AA"}}>{u.dir}</span></td>
                <td style={{padding:"8px 12px",color:C.green,fontFamily:"monospace"}}>${u.longMV.toLocaleString("en-US",{maximumFractionDigits:0})}</td>
                <td style={{padding:"8px 12px",color:C.red,fontFamily:"monospace"}}>${u.shortMV.toLocaleString("en-US",{maximumFractionDigits:0})}</td>
                <td style={{padding:"8px 12px",color:u.totalMV>=0?C.green:C.red,fontWeight:600,fontFamily:"monospace"}}>{u.totalMV>=0?"+":""}${u.totalMV.toLocaleString("en-US",{maximumFractionDigits:0})}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* PAYOFF DIAGRAM — OPÇÕES */}
      {(()=>{
        const optUnderlyings = symbols.filter(s=>byUnd[s].some(p=>p.isOption));
        if(!optUnderlyings.length) return null;
        const activeSym = optUnderlyings.includes(payoffUnd)?payoffUnd:optUnderlyings[0];

        const getExpiry = (localSymbol: string) => {
          const code = localSymbol.split(' ')[0];
          const monthMap: Record<string,string> = {
            F:'Jan',G:'Fev',H:'Mar',J:'Abr',K:'Mai',M:'Jun',
            N:'Jul',Q:'Ago',U:'Set',V:'Out',X:'Nov',Z:'Dez'
          };
          const mc = code.slice(-2,-1);
          const digit = code.slice(-1);
          const decade = Math.floor(new Date().getFullYear() / 10) * 10;
          const yr = decade + parseInt(digit);
          return (monthMap[mc] || mc) + '/' + yr;
        };

        const allOptLegs = byUnd[activeSym]?.filter(p=>p.isOption)||[];
        const expiries = [...new Set(allOptLegs.map(p=>getExpiry(p.localSymbol)))].sort();
        const selExp = payoffExpiry;

        const optLegs = allOptLegs.filter(p=>
          selExp === 'all' || getExpiry(p.localSymbol) === selExp
        );

        const spot = prices?.[activeSym]?.slice(-1)[0]?.close || 0;
        if(!spot||!optLegs.length) return null;

        const mult = OPT_MULTIPLIERS[activeSym]||100;
        const lo = spot*0.6, hi = spot*1.4;
        const pts = 120;
        const priceRange = Array.from({length:pts},(_,i)=>lo+(hi-lo)*i/(pts-1));

        const payoff = priceRange.map(S=>optLegs.reduce((tot,p)=>{
          if(!p.strike) return tot;
          const intr = p.optType==="C"?Math.max(0,S-p.strike):Math.max(0,p.strike-S);
          return tot + intr*Math.abs(p.qty)*mult*(p.qty>0?1:-1);
        },0));

        const premiumCredit = optLegs.reduce((tot,p)=>tot+(p.mktValue||0),0);

        // Breakeven points
        const breakevenPoints: number[] = [];
        for (let i = 1; i < payoff.length; i++) {
          if ((payoff[i-1] < 0 && payoff[i] >= 0) || (payoff[i-1] >= 0 && payoff[i] < 0)) {
            const price = priceRange[i-1] + (priceRange[i] - priceRange[i-1]) * Math.abs(payoff[i-1]) / (Math.abs(payoff[i-1]) + Math.abs(payoff[i]));
            breakevenPoints.push(Math.round(price * 100) / 100);
          }
        }

        const minPL = Math.min(...payoff), maxPL = Math.max(...payoff);
        const plRange = Math.max(Math.abs(minPL),Math.abs(maxPL),1);
        const svgW=780, svgH=220, padL=70, padR=20, padT=20, padB=30;
        const plotW=svgW-padL-padR, plotH=svgH-padT-padB;
        const toX=(i:number)=>padL+(i/(pts-1))*plotW;
        const toY=(v:number)=>padT+plotH/2-(v/plRange)*(plotH/2);
        const spotX=padL+((spot-lo)/(hi-lo))*plotW;
        const zeroY=toY(0);
        const priceToSvgX=(price:number)=>padL+((price-lo)/(hi-lo))*plotW;

        // Build path
        const linePts = payoff.map((v,i)=>`${toX(i).toFixed(1)},${toY(v).toFixed(1)}`);
        const pathD = "M"+linePts.join("L");
        // Area paths (positive green, negative red)
        const areaGreen = "M"+linePts.map((pt,i)=>{const v=payoff[i];return v>=0?pt:`${toX(i).toFixed(1)},${zeroY.toFixed(1)}`;}).join("L")+`L${toX(pts-1).toFixed(1)},${zeroY.toFixed(1)}L${toX(0).toFixed(1)},${zeroY.toFixed(1)}Z`;
        const areaRed = "M"+linePts.map((pt,i)=>{const v=payoff[i];return v<=0?pt:`${toX(i).toFixed(1)},${zeroY.toFixed(1)}`;}).join("L")+`L${toX(pts-1).toFixed(1)},${zeroY.toFixed(1)}L${toX(0).toFixed(1)},${zeroY.toFixed(1)}Z`;

        // Unique strikes with direction for coloring
        const uniqueStrikes = [...new Set(optLegs.map(p=>p.strike))].filter(k=>k>=lo&&k<=hi).map(k=>{
          const leg = optLegs.find(p=>p.strike===k);
          return {strike:k, isShort:leg?leg.qty<0:false};
        });

        // Axis labels
        const xLabels = [0,Math.floor(pts/6),Math.floor(pts/3),Math.floor(pts/2),Math.floor(2*pts/3),Math.floor(5*pts/6),pts-1];

        return (
          <div style={{marginBottom:24}}>
            <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:10}}>Payoff Diagram</div>
            {/* Underlying selector */}
            <div style={{display:"flex",gap:6,marginBottom:8,flexWrap:"wrap"}}>
              {optUnderlyings.map(s=>(
                <button key={s} onClick={()=>{setPayoffUnd(s);setPayoffExpiry('all');}} style={{
                  padding:"5px 14px",fontSize:10,fontWeight:600,borderRadius:5,cursor:"pointer",
                  background:s===activeSym?"rgba(124,58,237,.18)":"transparent",
                  color:s===activeSym?"#a78bfa":"#8899AA",
                  border:`1px solid ${s===activeSym?"rgba(124,58,237,.4)":"rgba(148,163,184,.15)"}`,
                }}>{s} ({byUnd[s].filter(p=>p.isOption).length} legs)</button>
              ))}
            </div>
            {/* Expiry selector */}
            {expiries.length > 1 && (
              <div style={{display:"flex",gap:5,marginBottom:8,flexWrap:"wrap"}}>
                <button onClick={()=>setPayoffExpiry('all')} style={{
                  padding:"4px 12px",fontSize:9,fontWeight:600,borderRadius:4,cursor:"pointer",
                  background:selExp==='all'?"rgba(220,180,50,.18)":"transparent",
                  color:selExp==='all'?"#DCB432":"#8899AA",
                  border:`1px solid ${selExp==='all'?"rgba(220,180,50,.4)":"rgba(148,163,184,.15)"}`,
                }}>Todos</button>
                {expiries.map(exp=>(
                  <button key={exp} onClick={()=>setPayoffExpiry(exp)} style={{
                    padding:"4px 12px",fontSize:9,fontWeight:600,borderRadius:4,cursor:"pointer",
                    background:selExp===exp?"rgba(220,180,50,.18)":"transparent",
                    color:selExp===exp?"#DCB432":"#8899AA",
                    border:`1px solid ${selExp===exp?"rgba(220,180,50,.4)":"rgba(148,163,184,.15)"}`,
                  }}>{exp} ({allOptLegs.filter(p=>getExpiry(p.localSymbol)===exp).length} legs)</button>
                ))}
              </div>
            )}
            {/* Mixed expiry warning */}
            {selExp === 'all' && expiries.length > 1 && (
              <div style={{
                fontSize:10, color:'#DCB432', marginBottom:8,
                padding:'4px 8px', background:'rgba(220,180,50,0.1)',
                borderRadius:4, border:'1px solid rgba(220,180,50,0.3)'
              }}>
                {`Aten\u00E7\u00E3o: ${expiries.length} vencimentos combinados (${expiries.join(', ')}). Selecione um vencimento para an\u00E1lise precisa.`}
              </div>
            )}
            <div style={{background:"#142332",borderRadius:8,border:`1px solid ${C.border}`,padding:16}}>
              <svg width="100%" viewBox={`0 0 ${svgW} ${svgH}`} style={{overflow:"visible"}}>
                {/* Grid */}
                {[0.25,0.5,0.75].map(f=><line key={f} x1={padL} y1={padT+plotH*f} x2={padL+plotW} y2={padT+plotH*f} stroke="#1e3a4a" strokeWidth={0.5}/>)}
                {xLabels.map(i=><line key={i} x1={toX(i)} y1={padT} x2={toX(i)} y2={padT+plotH} stroke="#1e3a4a" strokeWidth={0.5}/>)}
                {/* Areas */}
                <path d={areaGreen} fill="#00C878" opacity={0.12}/>
                <path d={areaRed} fill="#DC3C3C" opacity={0.12}/>
                {/* Zero line */}
                <line x1={padL} y1={zeroY} x2={padL+plotW} y2={zeroY} stroke="#ffffff" strokeWidth={1} strokeDasharray="6,4" opacity={0.3}/>
                {/* Payoff line */}
                <path d={pathD} fill="none" stroke="#E8ECF1" strokeWidth={2}/>
                {/* Spot line */}
                <line x1={spotX} y1={padT} x2={spotX} y2={padT+plotH} stroke="#DCB432" strokeWidth={1.5} strokeDasharray="4,3"/>
                <text x={spotX} y={padT-4} textAnchor="middle" fill="#DCB432" fontSize={9} fontWeight={700}>ATUAL {spot.toFixed(1)}</text>
                {/* X axis labels */}
                {xLabels.map(i=><text key={i} x={toX(i)} y={svgH-4} textAnchor="middle" fill="#8899AA" fontSize={8}>{priceRange[i].toFixed(0)}</text>)}
                {/* Y axis labels */}
                {[maxPL,0,minPL].map(v=><text key={v} x={padL-6} y={toY(v)+3} textAnchor="end" fill="#8899AA" fontSize={8}>{v>=0?"+":""}{(v/1000).toFixed(0)}K</text>)}
                {/* Strike lines with labels */}
                {uniqueStrikes.map(({strike,isShort})=>{
                  const sx=priceToSvgX(strike);
                  const color=isShort?'#DC3C3C':'#00C878';
                  return <g key={strike}>
                    <line x1={sx} y1={padT} x2={sx} y2={padT+plotH} stroke={color} strokeWidth={0.75} strokeDasharray="3,3" opacity={0.5}/>
                    <text x={sx} y={padT-4} textAnchor="middle" fontSize={8} fill={color} fontFamily="monospace">{strike}</text>
                  </g>;
                })}
              </svg>
              <div style={{display:"flex",justifyContent:"space-between",marginTop:8,fontSize:9,color:"#8899AA"}}>
                <span>{optLegs.length} legs | mult={mult}{selExp!=='all'?` | ${selExp}`:''}</span>
                <span>Max P&L: <span style={{color:"#00C878"}}>+${(maxPL/1000).toFixed(0)}K</span> | Min: <span style={{color:"#DC3C3C"}}>${(minPL/1000).toFixed(0)}K</span></span>
              </div>
              {breakevenPoints.length > 0 && (
                <div style={{fontSize:10, color:'#DCB432', marginTop:4}}>
                  Breakeven: {breakevenPoints.map(p => '$' + p).join(' | ')}
                </div>
              )}
              <div style={{fontSize:10, color:'#64748b', marginTop:4}}>
                {`Valor de mercado atual das posi\u00E7\u00F5es: `}
                <span style={{
                  color: premiumCredit >= 0 ? '#00C878' : '#DC3C3C',
                  fontWeight:700
                }}>
                  {premiumCredit >= 0 ? '+' : ''}
                  ${premiumCredit.toLocaleString('en-US', {maximumFractionDigits:0})}
                </span>
                {` (n\u00E3o inclui pr\u00EAmio original de entrada)`}
              </div>
            </div>
          </div>
        );
      })()}

      {/* DISTRIBUIÇÃO DE P&L ESPERADO (Gaussiana) */}
      {(()=>{
        if(!grossPos) return null;
        const mean = unrealPnl;
        const pgk = greeksData?.portfolio_greeks;
        const vegaStd = pgk?.total_vega ? Math.abs(pgk.total_vega) * 5 : null;
        const deltaStd = pgk?.total_delta ? Math.abs(pgk.total_delta) * 0.02 * 97 : null;
        const std = vegaStd && deltaStd ? Math.max(vegaStd, deltaStd) : vegaStd || deltaStd || grossPos * 0.15;
        const var95 = mean - 1.645 * std;
        const var99 = mean - 2.33 * std;
        const lo = mean - 3.5 * std, hi = mean + 3.5 * std;
        const pts = 200;
        const gauss = (x:number) => Math.exp(-0.5*((x-mean)/std)**2) / (std*Math.sqrt(2*Math.PI));
        const data = Array.from({length:pts},(_,i)=>{const x=lo+(hi-lo)*i/(pts-1);return {x,y:gauss(x)};});
        const maxY = Math.max(...data.map(d=>d.y));

        const svgW=780, svgH=160, padL=60, padR=20, padT=14, padB=28;
        const plotW=svgW-padL-padR, plotH=svgH-padT-padB;
        const toX=(v:number)=>padL+((v-lo)/(hi-lo))*plotW;
        const toY=(v:number)=>padT+plotH-(v/maxY)*plotH;
        const zeroX=toX(0);
        const meanX=toX(mean);
        const var95X=toX(var95);
        const var99X=toX(var99);

        const curvePts = data.map(d=>`${toX(d.x).toFixed(1)},${toY(d.y).toFixed(1)}`);
        const curveD = "M"+curvePts.join("L");
        const baseY = padT+plotH;
        // Green area (right of zero)
        const greenPts = data.filter(d=>d.x>=0);
        const greenD = greenPts.length>1 ? `M${toX(0).toFixed(1)},${baseY} `+greenPts.map(d=>`L${toX(d.x).toFixed(1)},${toY(d.y).toFixed(1)}`).join(" ")+` L${toX(greenPts[greenPts.length-1].x).toFixed(1)},${baseY}Z` : "";
        // Red area (left of zero)
        const redPts = data.filter(d=>d.x<=0);
        const redD = redPts.length>1 ? `M${toX(redPts[0].x).toFixed(1)},${baseY} `+redPts.map(d=>`L${toX(d.x).toFixed(1)},${toY(d.y).toFixed(1)}`).join(" ")+` L${toX(0).toFixed(1)},${baseY}Z` : "";

        const fmtK = (v:number) => (v>=0?"+":"")+(v/1000).toFixed(0)+"K";

        return (
          <div style={{marginBottom:24}}>
            <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:10}}>Distribuicao de P&L Esperado</div>
            <div style={{background:"#142332",borderRadius:8,border:`1px solid ${C.border}`,padding:16}}>
              <svg width="100%" viewBox={`0 0 ${svgW} ${svgH}`} style={{overflow:"visible"}}>
                {greenD && <path d={greenD} fill="#00C878" opacity={0.15}/>}
                {redD && <path d={redD} fill="#DC3C3C" opacity={0.15}/>}
                <path d={curveD} fill="none" stroke="#E8ECF1" strokeWidth={1.5}/>
                {/* Zero line */}
                {zeroX>=padL&&zeroX<=padL+plotW && <><line x1={zeroX} y1={padT} x2={zeroX} y2={baseY} stroke="#ffffff" strokeWidth={0.5} opacity={0.3} strokeDasharray="4,3"/>
                <text x={zeroX} y={svgH-4} textAnchor="middle" fill="#8899AA" fontSize={8}>$0</text></>}
                {/* VaR lines */}
                <line x1={var95X} y1={padT} x2={var95X} y2={baseY} stroke="#DC3C3C" strokeWidth={1} strokeDasharray="3,3" opacity={0.7}/>
                <text x={var95X} y={padT-2} textAnchor="middle" fill="#DC3C3C" fontSize={8} fontWeight={600}>VaR 95% {fmtK(var95)}</text>
                <line x1={var99X} y1={padT} x2={var99X} y2={baseY} stroke="#DC3C3C" strokeWidth={1} strokeDasharray="2,2" opacity={0.5}/>
                <text x={var99X+30} y={padT+12} textAnchor="start" fill="#DC3C3C" fontSize={7}>VaR 99% {fmtK(var99)}</text>
                {/* Current position */}
                <line x1={meanX} y1={padT} x2={meanX} y2={baseY} stroke="#DCB432" strokeWidth={1.5} strokeDasharray="4,3"/>
                <text x={meanX} y={svgH-4} textAnchor="middle" fill="#DCB432" fontSize={9} fontWeight={700}>ATUAL {fmtK(mean)}</text>
              </svg>
              <div style={{display:"flex",justifyContent:"space-between",marginTop:8,fontSize:9,color:"#8899AA"}}>
                <span>Modelo: Normal(μ=${fmtK(mean)}, σ=${(std/1000).toFixed(0)}K) {pgk?"[Greeks reais]":"[estimado]"}</span>
                <span>VaR 95%: <span style={{color:"#DC3C3C"}}>${fmtK(var95)}</span> | VaR 99%: <span style={{color:"#DC3C3C"}}>${fmtK(var99)}</span></span>
              </div>
            </div>
          </div>
        );
      })()}

      {/* SEÇÃO D: Gráfico SVG de Market Value por Underlying */}
      {chartData.length > 0 && (
        <div style={{marginBottom:24}}>
          <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:14}}>Market Value por Underlying</div>
          <div style={{background:C.panelAlt,borderRadius:8,border:`1px solid ${C.border}`,padding:16}}>
            <svg width="100%" viewBox={`0 0 ${chartW} ${chartH}`} style={{overflow:"visible"}}>
              {/* Zero line */}
              <line x1={centerX} y1={10} x2={centerX} y2={chartH - 20} stroke="#8899AA" strokeWidth={1} strokeDasharray="4,4" opacity={0.4} />
              <text x={centerX} y={chartH - 6} textAnchor="middle" fill="#8899AA" fontSize={9}>$0</text>
              {chartData.map((u, i) => {
                const y = 16 + i * (barH + 8);
                const barW = (u.totalMV / maxAbsMV) * (barArea / 2);
                const x = u.totalMV >= 0 ? centerX : centerX + barW;
                const color = u.totalMV >= 0 ? "#00C878" : "#DC3C3C";
                const valLabel = (u.totalMV >= 0 ? "+" : "") + "$" + Math.round(u.totalMV).toLocaleString("en-US");
                return (
                  <g key={u.sym}>
                    <text x={labelW - 8} y={y + barH / 2 + 4} textAnchor="end" fill={C.amber} fontSize={11} fontWeight={700} fontFamily="monospace">{u.sym}</text>
                    <rect x={x} y={y} width={Math.abs(barW)} height={barH} rx={4} fill={color} opacity={0.8} />
                    <text x={u.totalMV >= 0 ? centerX + Math.abs(barW) + 6 : centerX - Math.abs(barW) - 6} y={y + barH / 2 + 4}
                      textAnchor={u.totalMV >= 0 ? "start" : "end"} fill={color} fontSize={9} fontFamily="monospace" fontWeight={600}>{valLabel}</text>
                  </g>
                );
              })}
            </svg>
          </div>
        </div>
      )}

      {/* SEÇÃO C: Tabela de posições detalhada */}
      <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:14}}>Posicoes Detalhadas ({parsed.length})</div>
      {symbols.map(sym => {
        const legs = byUnd[sym];
        const commName = COMMODITIES.find(c => c.sym === sym)?.name || sym;
        return (
          <div key={sym} style={{marginBottom:20}}>
            <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:8}}>
              <span style={{fontSize:13,fontWeight:700,color:C.amber}}>{commName}</span>
              <span style={{fontSize:10,color:C.textMuted,background:C.panel,padding:"2px 8px",borderRadius:4}}>{sym}</span>
              <span style={{fontSize:10,color:C.textMuted}}>{legs.length} legs</span>
            </div>
            <div style={{background:C.panelAlt,borderRadius:6,border:`1px solid ${C.border}`,overflow:"hidden"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
                <thead>
                  <tr style={{borderBottom:`1px solid ${C.border}`}}>
                    {["Contrato","Tipo","Dir","Qtd","Strike","Venc","Custo/Ctto","Mkt Value"].map(h=>(
                      <th key={h} style={{padding:"8px 10px",textAlign:"left",color:C.textMuted,fontWeight:600,fontSize:9,textTransform:"uppercase"}}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {legs.map((p,j) => (
                    <tr key={j} style={{borderBottom:`1px solid ${C.border}22`}}>
                      <td style={{padding:"6px 10px",color:C.text,fontFamily:"monospace",fontWeight:500}}>{p.localSymbol}</td>
                      <td style={{padding:"6px 10px"}}><span style={{fontSize:8,fontWeight:700,padding:"2px 5px",borderRadius:3,
                        background:p.isOption?(p.optType==="C"?"rgba(0,200,120,.15)":"rgba(220,60,60,.15)"):"rgba(148,163,184,.1)",
                        color:p.isOption?(p.optType==="C"?"#00C878":"#DC3C3C"):"#8899AA"
                      }}>{p.isOption?(p.optType==="C"?"CALL":"PUT"):p.secType}</span></td>
                      <td style={{padding:"6px 10px"}}><span style={{fontSize:8,fontWeight:700,color:p.direction==="LONG"?"#00C878":"#DC3C3C"}}>{p.direction==="LONG"?"L":"S"}</span></td>
                      <td style={{padding:"6px 10px",color:p.qty>0?C.green:C.red,fontWeight:600,fontFamily:"monospace"}}>{p.qty>0?"+":""}{p.qty}</td>
                      <td style={{padding:"6px 10px",color:C.text,fontFamily:"monospace"}}>{p.isOption?p.strike.toFixed(1):"—"}</td>
                      <td style={{padding:"6px 10px",color:C.textMuted}}>{p.expLabel||"—"}</td>
                      <td style={{padding:"6px 10px",color:C.textMuted,fontFamily:"monospace"}}>${p.avgCost.toLocaleString("en-US",{maximumFractionDigits:0})}</td>
                      <td style={{padding:"6px 10px",color:C.text,fontFamily:"monospace"}}>{p.mktValue>=0?"":"-"}${Math.abs(p.mktValue).toLocaleString("en-US",{maximumFractionDigits:0})}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        );
      })}

      {parsed.length === 0 && <DataPlaceholder title="Sem posicoes" detail="Faca export do IBKR via pipeline" />}
    </div>
    );
  };

  // == COMMODITY-FIRST VIEW ====================================================
  const renderCommodityView = () => {
    const sym = selected;
    const comm = COMMODITIES.find(c=>c.sym===sym);
    const name = comm?.name || sym;
    const group = comm?.group || "";
    const unit = comm?.unit || "";
    const p = getPrice(sym);
    const ch = getChange(sym);
    const candles = prices?.[sym];

    // Week change
    const weekChange = candles && candles.length >= 5 ? ((candles[candles.length-1].close - candles[candles.length-5].close) / candles[candles.length-5].close * 100) : null;
    // Month change
    const monthChange = candles && candles.length >= 22 ? ((candles[candles.length-1].close - candles[candles.length-22].close) / candles[candles.length-22].close * 100) : null;

    // COT data
    const cotComm_ = cot?.commodities?.[sym];
    const cotLeg_ = cotComm_?.legacy;
    const cotDis_ = cotComm_?.disaggregated;
    const hasCotData = cotLeg_?.history?.length || cotDis_?.history?.length;

    // Physical data
    const physIntlData = physIntl?.international || {};
    const physBR = physIntlData[`${sym}_BR`];
    const physAR = physIntlData[`${sym}_AR`];
    const physUS = physical?.us_cash;

    // Forward curve contracts
    const MO:Record<string,number> = {F:1,G:2,H:3,J:4,K:5,M:6,N:7,Q:8,U:9,V:10,X:11,Z:12};
    const fwdContracts = (() => {
      if (!contractHist?.contracts) return [];
      const _now = new Date();
      const currentYearMonth = _now.getFullYear() * 100 + (_now.getMonth() + 1);
      return Object.values(contractHist.contracts as Record<string,any>)
        .filter((c:any) => c.commodity === sym && c.bars?.length > 0)
        .map((c:any) => {
          const code = (c.symbol||"").replace(sym,"");
          const mc = code.charAt(0); const ys = code.slice(1);
          const yr = ys.length===2?2000+parseInt(ys):parseInt(ys);
          const lastBar = c.bars[c.bars.length-1];
          return { label: c.expiry_label||`${mc}${ys}`, val: lastBar.close as number, sortKey: yr*100+(MO[mc]||0) };
        })
        .filter(c => c.val > 0 && c.sortKey >= currentYearMonth)
        .sort((a,b) => a.sortKey - b.sortKey);
    })();

    // Stocks watch
    const stockEntry = stocks?.commodities?.[sym];

    // -- CAMADA 1: 5-FACTOR SCORECARD --------------------------
    // Factor 1: Technical -- price vs MA200, RSI zone
    const techFactor = (() => {
      if (!candles || candles.length < 200) return null;
      const closes = candles.map(c => c.close);
      const ma200 = closes.slice(-200).reduce((s,v)=>s+v,0)/200;
      const rsiArr = rsiCalc(closes, 14);
      const rsi = rsiArr[rsiArr.length-1];
      const aboveMa = p !== null && p > ma200;
      const rsiZone = rsi > 70 ? "SOBRECOMPRADO" : rsi < 30 ? "SOBREVENDIDO" : "NEUTRO";
      const signal = aboveMa && rsi < 70 ? "BULL" : !aboveMa && rsi > 30 ? "BEAR" : "NEUTRO";
      const score = signal === "BULL" ? 1 : signal === "BEAR" ? -1 : 0;
      return { label: "Técnico", detail: `${aboveMa?"Acima":"Abaixo"} MA200 | RSI ${rsi.toFixed(0)} (${rsiZone})`, signal, score, section: "tecnica" };
    })();

    // Factor 2: COT -- Managed Money percentile
    const cotFactor = (() => {
      if (!cotDis_?.history?.length) return null;
      const hist = cotDis_.history.map((h:any) => h.managed_money_net || h.mm_net || 0).filter((v:number)=>v!==0);
      if (hist.length < 10) return null;
      const current = hist[hist.length-1];
      const sorted = [...hist].sort((a:number,b:number) => a-b);
      const pctile = Math.round(sorted.findIndex((v:number) => v >= current) / sorted.length * 100);
      const signal = pctile > 60 ? "BULL" : pctile < 40 ? "BEAR" : "NEUTRO";
      return { label: "COT", detail: `MM Percentil ${pctile}% (${(current/1000).toFixed(0)}K)`, signal, score: signal==="BULL"?1:signal==="BEAR"?-1:0, section: "cot" };
    })();

    // Factor 3: Seasonal -- current month avg return
    const seasonFactor = (() => {
      if (!candles || candles.length < 252) return null;
      const currentMonth = new Date().getMonth();
      const monthNames = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"];
      // Calculate monthly returns
      const monthlyReturns: number[] = [];
      for (let i = 22; i < candles.length; i++) {
        const d = new Date(candles[i].date);
        if (d.getMonth() === currentMonth) {
          const prev = candles[i-22]?.close;
          if (prev && prev > 0) monthlyReturns.push((candles[i].close - prev) / prev * 100);
        }
      }
      if (monthlyReturns.length < 3) return null;
      const avg = monthlyReturns.reduce((s,v)=>s+v,0) / monthlyReturns.length;
      const pctPos = Math.round(monthlyReturns.filter(r=>r>0).length / monthlyReturns.length * 100);
      const signal = avg > 1.5 ? "BULL" : avg < -1.5 ? "BEAR" : "NEUTRO";
      return { label: "Sazonal", detail: `${monthNames[currentMonth]}: ${avg>0?"+":""}${avg.toFixed(1)}% médio (${pctPos}% positivo)`, signal, score: signal==="BULL"?1:signal==="BEAR"?-1:0, section: "sazonalidade" };
    })();

    // Factor 4: Fundamentals -- stocks state or margin
    const fundFactor = (() => {
      if (!stockEntry) return null;
      const state = stockEntry.state || "";
      const pva = typeof stockEntry.price_vs_avg === "number" ? stockEntry.price_vs_avg : 0;
      const signal = state.includes("APERTADO") || state.includes("DEPRIMIDO") ? "BULL" : state.includes("ELEVADO") || state.includes("EXCESSO") ? "BEAR" : "NEUTRO";
      return { label: "Fundamentos", detail: `${state.replace(/_/g," ")} (${pva>0?"+":""}${pva.toFixed(1)}% vs média)`, signal, score: signal==="BULL"?1:signal==="BEAR"?-1:0, section: "fundamentos" };
    })();

    // Factor 5: Physical -- basis (cash - futures)
    const physFactor = (() => {
      if (!physBR || p === null) return null;
      const physPrice = parseFloat(physBR.price);
      if (isNaN(physPrice) || physPrice <= 0) return null;
      // Can't directly compare BRL/saca vs cents/bu -- show basis direction only if same unit
      const basisLabel = physBR.price_unit || "";
      const signal = "NEUTRO" as const;
      return { label: "Físico", detail: `${physBR.price} ${basisLabel}`, signal, score: 0, section: "fisico" };
    })();

    const factors = [techFactor, cotFactor, seasonFactor, fundFactor, physFactor].filter(Boolean) as {label:string;detail:string;signal:string;score:number;section:string}[];
    const compositeScore = factors.length > 0 ? factors.reduce((s,f) => s + f.score, 0) : 0;
    const compositeLabel = compositeScore >= 2 ? "BULL" : compositeScore <= -2 ? "BEAR" : "NEUTRO";
    const compositeColor = compositeScore >= 2 ? "#00C878" : compositeScore <= -2 ? "#DC3C3C" : "#DCB432";

    // -- CAMADA 3: Annotations for PriceChart --------------------
    const chartAnnotations: {label:string;value:number;color:string}[] = [];
    // COP line (grain_ratios margins)
    // Attempt to read from grainRatios state if available
    const grKey = sym === "ZC" ? "corn" : sym === "ZS" ? "soy" : sym === "ZW" ? "wheat" : null;
    // We need grainRatios data -- fetch inline if needed
    // grData is hoisted to Dashboard component level
    if (grKey && grData?.current_snapshot?.margins?.[grKey]?.cop) {
      const cop = grData.current_snapshot.margins[grKey].cop;
      // Convert $/bu to cents/bu if the commodity is in cents
      const copVal = unit.includes("¢") ? cop * 100 : cop;
      chartAnnotations.push({ label: `COP $${cop.toFixed(2)}`, value: copVal, color: "#DCB432" });
    }
    // FOB line from physical_intl
    if (physBR && physBR.price) {
      const fobPrice = parseFloat(physBR.price);
      if (!isNaN(fobPrice) && fobPrice > 0) {
        chartAnnotations.push({ label: `FOB ${fobPrice.toFixed(1)} ${physBR.price_unit||""}`, value: fobPrice, color: "#DC3C3C" });
      }
    }

    // -- CAMADA 2: Monthly returns calculation -------------------
    const monthlyBars = (() => {
      if (!candles || candles.length < 252) return null;
      const months = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"];
      const result: {month:string;avgReturn:number;pctPositive:number;stdDev:number;isCurrent:boolean}[] = [];
      const currentMonth = new Date().getMonth();
      for (let m = 0; m < 12; m++) {
        const returns: number[] = [];
        for (let i = 22; i < candles.length; i++) {
          const d = new Date(candles[i].date);
          if (d.getMonth() === m) {
            const prev = candles[i-22]?.close;
            if (prev && prev > 0) returns.push((candles[i].close - prev) / prev * 100);
          }
        }
        if (returns.length < 3) { result.push({month:months[m],avgReturn:0,pctPositive:0,stdDev:0,isCurrent:m===currentMonth}); continue; }
        const avg = returns.reduce((s,v)=>s+v,0)/returns.length;
        const pctPos = Math.round(returns.filter(r=>r>0).length/returns.length*100);
        const std = Math.sqrt(returns.reduce((s,v)=>s+(v-avg)**2,0)/returns.length);
        result.push({month:months[m],avgReturn:avg,pctPositive:pctPos,stdDev:std,isCurrent:m===currentMonth});
      }
      return result;
    })();

    // -- CAMADA 4: AI Narrative state ----------------------------
    // Section header component
    const SH = ({title,id}:{title:string;id?:string}) => (
      <div id={id} style={{display:"flex",alignItems:"center",gap:8,padding:"14px 0 10px",borderBottom:`1px solid #1E3044`,marginBottom:14,scrollMarginTop:20}}>
        <div style={{width:3,height:18,borderRadius:2,background:"#DCB432"}} />
        <span style={{fontSize:14,fontWeight:700,color:"#DCB432",letterSpacing:0.3}}>{title}</span>
      </div>
    );

    return (
      <div style={{maxWidth:1100,margin:"0 auto"}}>

        {/* -- SEÇÃO 1: VISÃO RÁPIDA + SCORECARD ------------------- */}
        <div style={{background:"#142332",borderRadius:10,padding:"20px 24px",marginBottom:20,border:"1px solid #1E3044"}}>
          <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",flexWrap:"wrap",gap:16}}>
            <div>
              <div style={{fontSize:22,fontWeight:800,color:"#E8ECF1",letterSpacing:0.5}}>{sym} <span style={{fontWeight:400,fontSize:16,color:"#8C96A5"}}>-- {name}</span></div>
              <div style={{fontSize:10,color:"#8C96A5",marginTop:2}}>{group} | {unit} | {candles?.length||0} dias de histórico</div>
            </div>
            <div style={{display:"flex",gap:16,alignItems:"center"}}>
              {p !== null && (
                <div style={{fontSize:32,fontWeight:800,fontFamily:"monospace",color:"#E8ECF1"}}>{p.toFixed(2)}</div>
              )}
              <div style={{display:"flex",flexDirection:"column",gap:3}}>
                {ch && <div style={{fontSize:11,fontFamily:"monospace",fontWeight:600,color:ch.pct>=0?"#00C878":"#DC3C3C",background:ch.pct>=0?"rgba(0,200,120,.1)":"rgba(220,60,60,.1)",padding:"2px 8px",borderRadius:4}}>Dia {ch.pct>=0?"+":""}{ch.pct.toFixed(2)}%</div>}
                {weekChange!==null && <div style={{fontSize:10,fontFamily:"monospace",color:weekChange>=0?"#00C878":"#DC3C3C"}}>Sem {weekChange>=0?"+":""}{weekChange.toFixed(2)}%</div>}
                {monthChange!==null && <div style={{fontSize:10,fontFamily:"monospace",color:monthChange>=0?"#00C878":"#DC3C3C"}}>Mês {monthChange>=0?"+":""}{monthChange.toFixed(2)}%</div>}
              </div>
            </div>
          </div>

          {/* 5-Factor Scorecard */}
          <div style={{marginTop:16,borderTop:"1px solid #1E3044",paddingTop:14}}>
            <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:10}}>
              <span style={{fontSize:18,fontWeight:800,fontFamily:"monospace",color:compositeColor}}>{compositeLabel}</span>
              <span style={{fontSize:10,color:"#8C96A5"}}>Score composto: {compositeScore>0?"+":""}{compositeScore} ({factors.length} fatores)</span>
            </div>
            <div style={{display:"grid",gridTemplateColumns:`repeat(${Math.min(factors.length||1,5)},1fr)`,gap:8}}>
              {factors.map(f => {
                const col = f.signal==="BULL"?"#00C878":f.signal==="BEAR"?"#DC3C3C":"#8C96A5";
                return (
                  <div key={f.label} onClick={()=>{const el=document.getElementById(`section-${f.section}`);el?.scrollIntoView({behavior:"smooth"})}}
                    style={{padding:"8px 10px",background:"#0E1A24",borderRadius:6,border:`1px solid ${col}33`,cursor:"pointer",transition:"border-color .2s"}}>
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:3}}>
                      <span style={{fontSize:9,fontWeight:700,color:"#8C96A5",letterSpacing:0.5}}>{f.label.toUpperCase()}</span>
                      <span style={{fontSize:8,fontWeight:700,padding:"1px 6px",borderRadius:3,background:`${col}22`,color:col}}>{f.signal}</span>
                    </div>
                    <div style={{fontSize:9,color:"#8C96A5",lineHeight:1.3}}>{f.detail}</div>
                  </div>
                );
              })}
              {factors.length === 0 && <div style={{fontSize:10,color:"#8C96A5",padding:8}}>Dados insuficientes para scorecard</div>}
            </div>
          </div>
        </div>

        {/* -- SEÇÃO 2+5: ANÁLISE TÉCNICA + COT (zoom sincronizado) -- */}
        {candles && candles.length > 0 && (
          <div style={{marginBottom:28}}>
            <SH title="ANÁLISE TÉCNICA + COT" id="section-tecnica" />
            <SyncedChartPanel candles={candles} symbol={sym}
              annotations={chartAnnotations.length>0?chartAnnotations:undefined}
              cotLegacy={cotLeg_} cotDisagg={cotDis_} />
          </div>
        )}

        {/* -- SEÇÃO 3: CURVA FUTURA -------------------------------- */}
        {fwdContracts.length >= 2 && (
          <div style={{marginBottom:28}}>
            <SH title="CURVA FUTURA" id="section-curva" />
            <div style={{background:"#142332",borderRadius:8,padding:16,border:"1px solid #1E3044"}}>
              <svg viewBox="0 0 900 320" style={{width:"100%"}}>
                {(() => {
                  const vals = fwdContracts.map(c=>c.val);
                  const mn = Math.min(...vals), mx = Math.max(...vals), rng = mx-mn||1;
                  const pMin = mn-rng*.08, pMax = mx+rng*.08;
                  const padL = 60, chartW = 900-padL-20, chartH = 240, topY = 30;
                  const n = fwdContracts.length;
                  const scX = (i:number) => padL+(n>1?i/(n-1)*chartW:chartW/2);
                  const scY = (v:number) => topY+(1-(v-pMin)/(pMax-pMin))*chartH;
                  const f0 = vals[0], bk = vals[vals.length-1];
                  const struct = bk > f0 ? "CONTANGO" : "BACKWARDATION";
                  const sCol = struct==="CONTANGO" ? "#f59e0b" : "#06b6d4";
                  const pts = fwdContracts.map((c,i) => `${scX(i)},${scY(c.val)}`).join(" ");
                  return (
                    <g>
                      <text x={padL} y={16} fill="#8C96A5" fontSize="10" fontWeight="600">{struct} ({((bk-f0)/f0*100).toFixed(2)}%)</text>
                      <text x={900-20} y={16} fill={sCol} fontSize="10" fontWeight="700" textAnchor="end">{sym}</text>
                      {Array.from({length:5}).map((_,i) => {
                        const val = pMin+(pMax-pMin)*i/4; const y = scY(val);
                        return <g key={i}><line x1={padL} x2={900-20} y1={y} y2={y} stroke="#1E3044" strokeWidth="0.5"/>
                          <text x={padL-6} y={y+3} textAnchor="end" fill="#8C96A5" fontSize="9" fontFamily="monospace">{val.toFixed(1)}</text></g>;
                      })}
                      <line x1={padL} x2={900-20} y1={scY(f0)} y2={scY(f0)} stroke="#8C96A5" strokeWidth="0.6" strokeDasharray="4,4"/>
                      <polygon fill={bk>f0?"rgba(245,158,11,.06)":"rgba(6,182,212,.06)"} points={`${scX(0)},${scY(f0)} ${pts} ${scX(n-1)},${scY(f0)}`}/>
                      <polyline fill="none" stroke={sCol} strokeWidth="2.5" points={pts}/>
                      {fwdContracts.length < 3 && (
                        <text x={450} y={topY+chartH+35} textAnchor="middle" fill="#8C96A5" fontSize="9">Dados limitados — pipeline desatualizado</text>
                      )}
                      {fwdContracts.map((c,i) => (
                        <g key={i}>
                          <circle cx={scX(i)} cy={scY(c.val)} r={i===0?"5":"4"} fill={i===0?"#DCB432":sCol} stroke="#142332" strokeWidth="1.5"/>
                          {i===0 && <text x={scX(i)} y={scY(c.val)+14} textAnchor="middle" fill="#DCB432" fontSize="6.5" fontWeight="600">Próximo vencimento</text>}
                          <text x={scX(i)} y={scY(c.val)-7} textAnchor="middle" fill={i===0?"#DCB432":sCol} fontSize="7" fontWeight="600" fontFamily="monospace">{c.val.toFixed(1)}</text>
                        </g>
                      ))}
                      {fwdContracts.map((c,i) => (
                        <text key={`l${i}`} x={scX(i)} y={topY+chartH+16} textAnchor="middle" fill="#8C96A5" fontSize="7" transform={`rotate(-35,${scX(i)},${topY+chartH+16})`}>{c.label}</text>
                      ))}
                    </g>
                  );
                })()}
              </svg>
            </div>
          </div>
        )}

        {/* -- SEÇÃO 4: SAZONALIDADE (barras mensais apenas) -------- */}
        {monthlyBars && (
          <div style={{marginBottom:28}}>
            <SH title="SAZONALIDADE" id="section-sazonalidade" />
            {(()=>{
              const sd = getSeasonDev(sym);
              if (!sd) return null;
              return (
                <div style={{marginBottom:10,display:"flex",gap:16,fontSize:11,color:"#8C96A5"}}>
                  <span>Atual: <strong style={{color:"#E8ECF1",fontFamily:"monospace"}}>{sd.current.toFixed(2)}</strong></span>
                  <span>Média 5Y: <strong style={{fontFamily:"monospace"}}>{sd.avg.toFixed(2)}</strong></span>
                  <span>Desvio: <strong style={{color:Math.abs(sd.dev)>10?(sd.dev>0?"#DC3C3C":"#00C878"):"#8C96A5",fontFamily:"monospace"}}>{sd.dev>0?"+":""}{sd.dev.toFixed(1)}%</strong></span>
                </div>
              );
            })()}
            {monthlyBars && (
              <div style={{marginTop:16}}>
                <div style={{fontSize:10,fontWeight:600,color:"#8C96A5",marginBottom:8}}>RETORNO MÉDIO MENSAL HISTÓRICO</div>
                <svg viewBox="0 0 600 200" style={{width:"100%",background:"#0E1A24",borderRadius:6,display:"block"}}>
                  {(()=>{
                    const maxAbs = Math.max(...monthlyBars.map(b=>Math.abs(b.avgReturn)),1) || 1;
                    const maxStd = Math.max(...monthlyBars.map(b=>b.stdDev),0);
                    const topVal = Math.max(maxAbs, maxAbs+maxStd*0.5);
                    const padL=40,padR=10,padT=18,padB=22;
                    const chartW=600-padL-padR, chartH=200-padT-padB;
                    const zeroY=padT+chartH/2;
                    const barW=chartW/12*0.55;
                    const gap=chartW/12;
                    const scY=(v:number)=>zeroY-(v/topVal)*(chartH/2);
                    return (
                      <g>
                        {/* Zero line */}
                        <line x1={padL} x2={600-padR} y1={zeroY} y2={zeroY} stroke="#1E3044" strokeWidth="1"/>
                        {/* Grid lines */}
                        {[-topVal,-topVal/2,topVal/2,topVal].map((v,gi)=>{
                          const y=scY(v);
                          return <g key={gi}>
                            <line x1={padL} x2={600-padR} y1={y} y2={y} stroke="#1E3044" strokeWidth="0.5" strokeDasharray="2,3"/>
                            <text x={padL-4} y={y+3} textAnchor="end" fill="#64748b" fontSize="7" fontFamily="monospace">{v>0?"+":""}{v.toFixed(1)}%</text>
                          </g>;
                        })}
                        <text x={padL-4} y={zeroY+3} textAnchor="end" fill="#8C96A5" fontSize="7" fontFamily="monospace">0%</text>
                        {/* Bars */}
                        {monthlyBars.map((mb,i)=>{
                          const cx=padL+gap*i+gap/2;
                          const barTop=scY(mb.avgReturn);
                          const barBot=zeroY;
                          const y1=Math.min(barTop,barBot), h=Math.abs(barTop-barBot)||1;
                          const col=mb.avgReturn>=0?"#00C878":"#DC3C3C";
                          // Std dev zone
                          const sdTop=scY(mb.avgReturn+mb.stdDev);
                          const sdBot=scY(mb.avgReturn-mb.stdDev);
                          return (
                            <g key={i}>
                              {/* ±1sd zone */}
                              {mb.stdDev>0 && <rect x={cx-barW/2-2} y={Math.min(sdTop,sdBot)} width={barW+4} height={Math.abs(sdTop-sdBot)||1}
                                fill={col} opacity={0.06} rx={2}/>}
                              {/* Bar */}
                              <rect x={cx-barW/2} y={y1} width={barW} height={h} fill={col} opacity={0.8} rx={2}
                                stroke={mb.isCurrent?"#DCB432":"none"} strokeWidth={mb.isCurrent?1.5:0}/>
                              {/* Value label */}
                              <text x={cx} y={mb.avgReturn>=0?barTop-4:barTop+h+9} textAnchor="middle" fill={col} fontSize="7" fontWeight="600" fontFamily="monospace">
                                {mb.avgReturn>0?"+":""}{mb.avgReturn.toFixed(1)}%
                              </text>
                              {/* % positive label */}
                              <text x={cx} y={mb.avgReturn>=0?barTop-12:barTop+h+17} textAnchor="middle" fill="#64748b" fontSize="6">
                                {mb.pctPositive}%+
                              </text>
                              {/* Month label */}
                              <text x={cx} y={200-padB+12} textAnchor="middle" fill={mb.isCurrent?"#DCB432":"#64748b"} fontSize="8" fontWeight={mb.isCurrent?700:400}>
                                {mb.month}
                              </text>
                            </g>
                          );
                        })}
                      </g>
                    );
                  })()}
                </svg>
                <div style={{display:"flex",justifyContent:"space-between",fontSize:8,color:"#64748b",marginTop:4}}>
                  <span>Borda dourada = mês atual | Zona sombreada = ±1sd</span>
                  <span>Fonte: price_history.json ({candles?.length||0} dias)</span>
                </div>
              </div>
            )}
          </div>
        )}

        {/* SEÇÃO 5 (COT) integrada ao SyncedChartPanel acima -- scroll target mantido */}
        <div id="section-cot" style={{scrollMarginTop:20}} />

        {/* -- SEÇÃO 6: FUNDAMENTOS (condicional por grupo) --------- */}
        {stockEntry && (
          <div style={{marginBottom:28}}>
            <SH title="FUNDAMENTOS" id="section-fundamentos" />
            <div style={{background:"#142332",borderRadius:8,padding:16,border:"1px solid #1E3044"}}>
              <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(130px,1fr))",gap:12}}>
                {stockEntry.data_available?.stock_real && (
                  <>
                    <div style={{padding:10,background:"#0E1A24",borderRadius:6}}>
                      <div style={{fontSize:8,color:"#8C96A5",fontWeight:600}}>ESTOQUE ATUAL</div>
                      <div style={{fontSize:16,fontWeight:700,fontFamily:"monospace",color:"#E8ECF1"}}>{stockEntry.stock_current || "--"}</div>
                      <div style={{fontSize:9,color:"#8C96A5"}}>{stockEntry.stock_unit || ""}</div>
                    </div>
                    <div style={{padding:10,background:"#0E1A24",borderRadius:6}}>
                      <div style={{fontSize:8,color:"#8C96A5",fontWeight:600}}>MÉDIA 5Y</div>
                      <div style={{fontSize:16,fontWeight:700,fontFamily:"monospace",color:"#8C96A5"}}>{stockEntry.stock_avg || "--"}</div>
                    </div>
                  </>
                )}
                <div style={{padding:10,background:"#0E1A24",borderRadius:6}}>
                  <div style={{fontSize:8,color:"#8C96A5",fontWeight:600}}>PREÇO VS MÉDIA</div>
                  <div style={{fontSize:16,fontWeight:700,fontFamily:"monospace",color:(stockEntry.price_vs_avg||0)>0?"#DC3C3C":"#00C878"}}>{(stockEntry.price_vs_avg||0)>0?"+":""}{(stockEntry.price_vs_avg||0).toFixed(1)}%</div>
                </div>
                <div style={{padding:10,background:"#0E1A24",borderRadius:6}}>
                  <div style={{fontSize:8,color:"#8C96A5",fontWeight:600}}>ESTADO</div>
                  <div style={{fontSize:12,fontWeight:700,color:"#DCB432"}}>{(stockEntry.state||"").replace(/_/g," ")}</div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* -- SEÇÃO 7: MERCADO FÍSICO (condicional) ---------------- */}
        {(physBR || physAR || (physUS && physUS[name?.toLowerCase()])) && (
          <div style={{marginBottom:28}}>
            <SH title="MERCADO FÍSICO" id="section-fisico" />
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))",gap:12}}>
              {physBR && (
                <div style={{background:"#142332",borderRadius:8,padding:14,border:"1px solid #1E3044"}}>
                  <div style={{fontSize:10,fontWeight:600,color:"#DCB432",marginBottom:6}}>Brasil</div>
                  <div style={{fontSize:20,fontWeight:700,fontFamily:"monospace",color:"#E8ECF1"}}>{physBR.price} <span style={{fontSize:10,color:"#8C96A5"}}>{physBR.price_unit}</span></div>
                  {physBR.period && <div style={{fontSize:9,color:"#8C96A5",marginTop:4}}>{physBR.period}</div>}
                  {physBR.source && <div style={{fontSize:8,color:"#64748b"}}>{physBR.source}</div>}
                </div>
              )}
              {physAR && (
                <div style={{background:"#142332",borderRadius:8,padding:14,border:"1px solid #1E3044"}}>
                  <div style={{fontSize:10,fontWeight:600,color:"#DCB432",marginBottom:6}}>Argentina</div>
                  <div style={{fontSize:20,fontWeight:700,fontFamily:"monospace",color:"#E8ECF1"}}>{physAR.price} <span style={{fontSize:10,color:"#8C96A5"}}>{physAR.price_unit}</span></div>
                  {physAR.source && <div style={{fontSize:8,color:"#64748b"}}>{physAR.source}</div>}
                </div>
              )}
            </div>
          </div>
        )}

      </div>
    );
  };

  // -- INTEL: Central de Inteligência -----------------------------------------
  // ═══════════════════════════════════════════════════════
  // Per-commodity deep dive prompt (used by purple buttons)
  // ═══════════════════════════════════════════════════════
  const buildCommodityPromptTop = (sym: string) => {
    const sections: string[] = [];
    const name = COMMODITIES.find(c => c.sym === sym)?.name || sym;
    const monthNames2 = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"];
    const curMonth2 = new Date().getMonth();

    sections.push(`Analise completa da commodity ${sym} (${name}) usando o framework Council AgriMacro v2.2 com 5 conselheiros adversariais. Cada conselheiro DEVE cruzar Analise Tecnica + Analise Fundamental antes de emitir veredicto. Chairman entrega proximo passo concreto com threshold.`);

    // 1. DNA
    try {
      const dy = commodityDna?.commodities?.[sym];
      if (dy?.drivers_ranked) {
        sections.push(`\nCOMMODITY DNA ${sym}:`);
        if (dy.composite_signal) sections.push(`Sinal composto: ${dy.composite_signal} (${dy.bullish_drivers} bull / ${dy.bearish_drivers} bear)`);
        (dy.drivers_ranked as any[]).slice(0, 5).forEach((d: any) => sections.push(`  ${d.driver}: ${d.signal || d.value || ""}`));
      }
    } catch {}

    // 2. Options
    try {
      const und: any = optionsChain?.underlyings?.[sym];
      if (und) {
        sections.push(`\nOPTIONS ${sym}:`);
        const ivr = und.iv_rank || {};
        const sk = und.skew || {};
        const ts = und.term_structure || {};
        sections.push(`  IV: ${ivr.current_iv ? (ivr.current_iv * 100).toFixed(1) + "%" : "N/A"} | Rank: ${ivr.rank_52w != null ? ivr.rank_52w.toFixed(0) + "%" : "N/A"} | Skew: ${sk.skew_pct != null ? (sk.skew_pct > 0 ? "+" : "") + sk.skew_pct + "%" : "N/A"} | Term: ${ts.structure || "N/A"}`);
      } else { sections.push(`\nOPTIONS ${sym}: N/A`); }
    } catch {}

    // 3. COT
    try {
      const c: any = cot?.commodities?.[sym];
      const dis: any = c?.disaggregated || {};
      if (dis.cot_index != null) {
        const label = dis.cot_index >= 80 ? "CROWDED LONG" : dis.cot_index <= 20 ? "CROWDED SHORT" : "neutro";
        sections.push(`\nCOT ${sym}: idx=${dis.cot_index.toFixed(0)} (${label})${dis.cot_index_52w != null ? " | 52w=" + dis.cot_index_52w.toFixed(0) : ""}`);
      } else { sections.push(`\nCOT ${sym}: N/A`); }
    } catch {}

    // 4. Seasonality
    try {
      const s2: any = season?.[sym];
      if (s2?.monthly_returns) {
        const mr = s2.monthly_returns[curMonth2];
        const avg = typeof mr === "number" ? mr : mr?.avg ?? null;
        sections.push(`\nSAZONALIDADE ${sym} ${monthNames2[curMonth2]}: ${avg != null ? (avg >= 0 ? "+" : "") + avg.toFixed(2) + "%" : "N/A"}`);
      } else { sections.push(`\nSAZONALIDADE ${sym}: N/A`); }
    } catch {}

    // 5. Spreads
    try {
      if (spreads?.spreads) {
        const symTerms: Record<string, string[]> = {ZS:["soy","crush"],ZC:["corn","zc_"],ZW:["wheat","ke_zw"],ZL:["oil","zl_","crush"],ZM:["meal","crush"],KE:["ke_"],LE:["cattle","feedlot"],GF:["feeder","feedlot"],CL:["crude","cl","oil"],CC:["cocoa"],SB:["sugar"],KC:["coffee"]};
        const terms = symTerms[sym] || [sym.toLowerCase()];
        const related = Object.entries(spreads.spreads).filter(([k, v]: any) => terms.some(t => (v.name || k).toLowerCase().includes(t) || k.toLowerCase().includes(t)));
        if (related.length) {
          sections.push(`\nSPREADS ${sym}:`);
          related.forEach(([k, v]: any) => sections.push(`  ${v.name || k}: z=${v.zscore_1y?.toFixed(2) || "?"} | ${v.regime || "?"}`));
        }
      }
    } catch {}

    // 6. Portfolio
    try {
      const myPos = (portfolio?.positions || []).filter((p: any) => p.symbol === sym && (p.sec_type === "FOP" || p.sec_type === "FUT"));
      if (myPos.length) {
        sections.push(`\nPOSICAO ${sym}:`);
        myPos.forEach((p: any) => sections.push(`  ${p.position > 0 ? "+" : ""}${p.position}x ${p.local_symbol}${p.delta != null ? " d=" + p.delta.toFixed(4) : ""}`));
      } else { sections.push(`\nPOSICAO ${sym}: nenhuma`); }
    } catch {}

    // 7. Price
    try {
      const bars = prices?.[sym];
      if (Array.isArray(bars) && bars.length) {
        const last = bars[bars.length - 1];
        const p5 = bars.length >= 6 ? bars[bars.length - 6] : null;
        sections.push(`\nPRECO ${sym}: $${last.close} (${last.date})${p5 ? " | 5d=" + ((last.close - p5.close) / p5.close * 100).toFixed(1) + "%" : ""}`);
      }
    } catch {}

    sections.push(`\nSe algum dado "N/A", escreva explicitamente. Nunca fabricar. 5 conselheiros + Chairman com threshold.`);
    return sections.join("\n");
  };

  const renderIntelPage = () => {
    const panelStyle:any = {background:"#142332",borderRadius:10,padding:20,marginBottom:20,border:"1px solid rgba(148,163,184,.12)"};
    const sectionTitle = (t:string) => <div style={{fontSize:14,fontWeight:700,color:"#DCB432",marginBottom:14,letterSpacing:0.5}}>{t}</div>;
    const placeholder = (msg:string) => <div style={{color:"#64748b",fontSize:11,fontStyle:"italic"}}>{msg}</div>;

    // -- Shared AI context builder (all sources) --
    const buildIntelPrompt = () => {
      const sections:string[] = [];

      sections.push("Voc\u00ea \u00e9 um analista s\u00eanior de commodities com acesso completo ao contexto de mercado e portf\u00f3lio do usu\u00e1rio. Analise a situa\u00e7\u00e3o atual considerando:");

      // Intelligence Frame (replaces old synthesis when available)
      if (intelFrame?.market_narrative) {
        sections.push("\n=== INTELLIGENCE FRAME DO DIA ===");
        sections.push(intelFrame.market_narrative);

        // High priority alerts
        const highAlerts = (intelFrame.alerts || []).filter((a:any) => a.priority === "HIGH");
        if (highAlerts.length) {
          sections.push("\nALERTAS CRITICOS:");
          highAlerts.forEach((a:any) => sections.push(`- [${a.type}] ${a.commodity}: ${a.message}${a.lag ? ` (lag: ${a.lag})` : ""}`));
        }

        // Commodity scores
        const byCom = intelFrame.by_commodity || {};
        const scored = Object.entries(byCom)
          .filter(([,v]:any) => Math.abs(v.score) >= 2)
          .sort(([,a]:any, [,b]:any) => Math.abs(b.score) - Math.abs(a.score));
        if (scored.length) {
          sections.push("\nCOMMODITIES COM VIES FORTE:");
          scored.forEach(([sym, info]:any) => {
            const dir = info.score > 0 ? "ALTISTA" : "BAIXISTA";
            sections.push(`${sym}: ${dir} (${info.score > 0 ? "+" : ""}${info.score}) | ${info.signals?.slice(0,3).join(", ") || ""}`);
            if (info.key_number) sections.push(`  Numero-chave: ${info.key_number}`);
            if (info.lag_factor) sections.push(`  Lag: ${info.lag_factor}`);
          });
        }

        // Macro frame
        const mf = intelFrame.macro_frame || {};
        if (Object.keys(mf).length) {
          sections.push("\nMACRO FRAME:");
          if (mf.dxy) sections.push(`DXY: ${mf.dxy.value} (${mf.dxy.signal}) \u2014 ${mf.dxy.impact}`);
          if (mf.oil) sections.push(`CL: $${mf.oil.value} (${mf.oil.signal}) \u2014 ${mf.oil.impact}`);
          if (mf.fertilizer_index) sections.push(`Fertilizantes: ${mf.fertilizer_index.yoy_change > 0 ? "+" : ""}${mf.fertilizer_index.yoy_change}% YoY (${mf.fertilizer_index.signal})`);
        }
      }

      // Fallback: old synthesis if no intelligence frame
      if (!intelFrame?.market_narrative) {
        const syn = intelSynthesis;
        if(syn && !syn.is_fallback) {
          sections.push("\n=== S\u00cdNTESE DO DIA ===");
          if(syn.summary) sections.push(syn.summary);
          if(syn.priority_high?.length) sections.push("Sinais cr\u00edticos:\n" + syn.priority_high.map((s:any)=>"- "+s.title+(s.detail?" ("+s.detail+")":"")).join("\n"));
          if(syn.priority_medium?.length) sections.push("Sinais aten\u00e7\u00e3o:\n" + syn.priority_medium.map((s:any)=>"- "+s.title).join("\n"));
        }
      }

      // Macro
      const mi = macroIndicators;
      const fw = fedwatch;
      const macroLines:string[] = [];
      if(mi?.vix) macroLines.push(`VIX: ${mi.vix.value} (${mi.vix.level}, ${mi.vix.change_pct>0?"+":""}${mi.vix.change_pct}%)`);
      if(mi?.sp500) macroLines.push(`S&P 500: ${mi.sp500.price} (${mi.sp500.change_pct>0?"+":""}${mi.sp500.change_pct}% dia, ${mi.sp500.change_week_pct>0?"+":""}${mi.sp500.change_week_pct}% semana)`);
      if(mi?.treasury_10y) macroLines.push(`Juros 10Y EUA: ${mi.treasury_10y.yield_pct}% (${mi.treasury_10y.direction}, ${mi.treasury_10y.change_bps>0?"+":""}${mi.treasury_10y.change_bps} bps)`);
      if(dxSeries?.length) macroLines.push(`DXY: ${dxLast?.close?.toFixed(2)}`);
      if(bcbData?.brl_usd?.length) { const bl=bcbData.brl_usd.slice(-1)[0]; macroLines.push(`BRL/USD: ${bl.value} (${bl.date})`); }
      if(fw?.probabilities) macroLines.push(`Fed: ${fw.market_expectation} (hold ${fw.probabilities.hold}%, corte ${fw.probabilities.cut_25bps}%, alta ${fw.probabilities.hike_25bps}%) — próximo FOMC ${fw.next_meeting}`);
      if(macroLines.length) { sections.push("\n=== MACRO ==="); sections.push(macroLines.join("\n")); }

      // Composite signals
      if(correlations?.composite_signals?.length) {
        sections.push("\n=== COMPOSITE SIGNALS (multifatorial) ===");
        correlations.composite_signals.forEach((c:any)=>{
          const bulls = (c.factors_bull||[]).join("; ");
          const bears = (c.factors_bear||[]).join("; ");
          sections.push(`${c.asset}: ${c.signal} (confiança ${(c.confidence*100).toFixed(0)}%, ${c.sources_count} fontes)${bulls?" | +"+bulls:""}${bears?" | -"+bears:""}`);
        });
      }

      // COT extremes
      if(cot?.commodities) {
        const cotLines:string[] = [];
        Object.entries(cot.commodities).forEach(([sym,v]:any)=>{
          const leg = v?.legacy;
          if(!leg?.latest || !leg?.history?.length) return;
          const hist = leg.history;
          const nets = hist.map((h:any)=>h.noncomm_net).filter((n:any)=>n!=null);
          if(nets.length < 20) return;
          const mn = Math.min(...nets), mx = Math.max(...nets);
          if(mx === mn) return;
          const cotIndex = ((leg.latest.noncomm_net - mn) / (mx - mn)) * 100;
          if(cotIndex > 80 || cotIndex < 20) {
            const prev = hist.length >= 2 ? hist[hist.length-2] : null;
            const delta = prev?.noncomm_net != null ? leg.latest.noncomm_net - prev.noncomm_net : null;
            cotLines.push(`${sym}: net=${leg.latest.noncomm_net} | cot_index=${cotIndex.toFixed(0)}/100 | oi=${leg.latest.open_interest}${delta!=null?" | delta_semanal="+(delta>0?"+":"")+delta:""}`);
          }
        });
        if(cotLines.length) { sections.push("\n=== COT REPORT (POSICIONAMENTO EXTREMO) ==="); sections.push(cotLines.join("\n")); }
      }

      // Spreads
      if(spreads?.spreads) {
        const extremos = Object.entries(spreads.spreads).filter(([,v]:any)=>v.regime==="EXTREMO"||v.regime==="DISSONÂNCIA");
        if(extremos.length) {
          sections.push("\n=== SPREADS ===");
          extremos.forEach(([k,v]:any)=>sections.push(`${v.name}: ${v.current?.toFixed(4)} ${v.unit} | z=${v.zscore_1y?.toFixed(2)} | ${v.regime} | ${v.trend}`));
        }
      }

      // Climate + Sentiment
      const climaLines:string[] = [];
      if(weatherData?.enso) climaLines.push(`ENSO: ${weatherData.enso.status} (ONI=${weatherData.enso.oni_value})`);
      if(weatherData?.regions) {
        const alerts:string[] = [];
        Object.values(weatherData.regions).forEach((r:any)=>(r.alerts||[]).forEach((a:any)=>alerts.push(`${r.label}: ${a.type} ${a.severity}`)));
        if(alerts.length) climaLines.push(`Alertas: ${alerts.join("; ")}`);
      }
      if(googleTrends?.spikes?.length) climaLines.push(`Google Trends spikes: ${googleTrends.spikes.join(", ")}`);
      if(intelSentiment.text) climaLines.push(`Sentimento (Grok): ${intelSentiment.text.slice(0,500)}`);
      if(intelNews.text) climaLines.push(`Notícias: ${intelNews.text.slice(0,500)}`);
      if(climaLines.length) { sections.push("\n=== CLIMA E SENTIMENTO ==="); sections.push(climaLines.join("\n")); }

      // Grain Ratios scorecard
      if(grData?.scorecards) {
        const grLines:string[] = [];
        const snap = grData.current_snapshot || {};
        const grMap:{[k:string]:string} = {corn:"ZC",soy:"ZS",wheat:"ZW"};
        for(const [grain,sym] of Object.entries(grMap)) {
          const sc = grData.scorecards[grain];
          const cot = snap.cot?.[grain];
          const mg = snap.margins?.[grain];
          if(!sc) continue;
          const cotPct = cot?.cot_index != null ? ` | COT percentil=${cot.cot_index.toFixed(0)}` : "";
          const copLabel = mg?.margin != null ? (mg.margin < 0 ? " | Preço ABAIXO do custo de produção" : " | Preço ACIMA do custo de produção") : "";
          grLines.push(`${sym}: ${sc.composite_signal} (score=${sc.composite_score?.toFixed(0)})${cotPct}${copLabel}`);
        }
        if(grLines.length) { sections.push("\n=== GRAIN RATIOS (modelo multifatorial) ==="); sections.push(grLines.join("\n")); }
      }

      // Physical international / arbitrage
      if(grData?.arbitrage) {
        const arb = grData.arbitrage;
        const phLines:string[] = [];
        const cif = arb.spread_delivered_china || {};
        const fobP = cif.fob_paranagua || {};
        const fobG = cif.fob_gulf || {};
        const fobR = cif.fob_rosario || {};
        for(const crop of ["soy","corn"]) {
          const cropName = crop==="soy"?"Soja":"Milho";
          const parts:string[] = [];
          if(fobP[crop] && fobG[crop]) parts.push(`FOB: Paranaguá=$${fobP[crop]?.toFixed(0)} Gulf=$${fobG[crop]?.toFixed(0)}`);
          if(fobR[crop]) parts.push(`Rosario=$${fobR[crop]?.toFixed(0)}`);
          const cifQ = cif.cif_qingdao || {};
          const vals = [cifQ[crop+"_us"],cifQ[crop+"_br"],cifQ[crop+"_arg"]].filter((v:any)=>v!=null);
          if(vals.length>=2) parts.push(`CIF Qingdao: US=$${cifQ[crop+"_us"]?.toFixed(0)} BR=$${cifQ[crop+"_br"]?.toFixed(0)} ARG=$${cifQ[crop+"_arg"]?.toFixed(0)}`);
          if(parts.length) phLines.push(`${cropName}: ${parts.join(" | ")}`);
        }
        const bdiVal = typeof arb.bdi === "number" ? arb.bdi : arb.bdi?.value || cif.bdi_used;
        if(bdiVal != null && typeof bdiVal === "number") phLines.push(`BDI: ${bdiVal} pts`);
        if(phLines.length) { sections.push("\n=== MERCADO FÍSICO INTERNACIONAL ==="); sections.push(phLines.join("\n")); }
      }

      // Calendar — next 7 days
      if(calendar?.events) {
        const now = new Date();
        const in7 = new Date(now.getTime() + 7*24*60*60*1000);
        const todayStr = now.toISOString().slice(0,10);
        const in7Str = in7.toISOString().slice(0,10);
        const upcoming = calendar.events.filter((e:any)=>e.date>=todayStr && e.date<=in7Str).slice(0,6);
        if(upcoming.length) {
          sections.push("\n=== EVENTOS CRÍTICOS — PRÓXIMOS 7 DIAS ===");
          upcoming.forEach((e:any)=>sections.push(`${e.date} | ${e.name} (${e.category}) | impacto: ${e.impact}`));
        }
      }

      // Portfolio
      if(portfolio) {
        const pLines:string[] = [];
        const isEnriched = !!portfolio?.positions_by_underlying;
        const summ = portfolio?.summary || {};
        const acct = isEnriched ? (portfolio?.account || {}) : {};
        const netLiq = isEnriched ? acct.net_liquidation : parseFloat(summ.NetLiquidation || "0");
        const buyPow = isEnriched ? acct.buying_power : parseFloat(summ.BuyingPower || "0");
        const cash = isEnriched ? acct.total_cash : parseFloat(summ.TotalCashValue || "0");
        const unrealPnl = isEnriched ? acct.unrealized_pnl : parseFloat(summ.UnrealizedPnL || "0");

        if(netLiq) pLines.push(`Net Liquidation: $${Number(netLiq).toLocaleString("en-US",{maximumFractionDigits:0})}`);
        if(buyPow) pLines.push(`Buying Power: $${Number(buyPow).toLocaleString("en-US",{maximumFractionDigits:0})}`);
        if(cash) pLines.push(`Cash: $${Number(cash).toLocaleString("en-US",{maximumFractionDigits:0})}`);
        if(unrealPnl) pLines.push(`Unrealized PnL: $${Number(unrealPnl).toLocaleString("en-US",{maximumFractionDigits:0})}`);

        // Positions
        const positions = portfolio?.positions || [];
        if(positions.length) {
          pLines.push(`Posições abertas (${positions.length}):`);
          const agriSyms = new Set(["ZC","ZS","ZW","KE","ZM","ZL","SB","KC","CT","CC","LE","GF","HE","CL","NG","GC","SI","DX"]);
          const overlaps:string[] = [];
          positions.forEach((p:any)=>{
            const sym = p.symbol || p.local_symbol || "?";
            const qty = p.position || p.quantity || 0;
            const avg = p.avg_cost || p.avgCost || 0;
            const pnl = p.unrealizedPnL || p.unrealized_pnl || "";
            pLines.push(`  ${sym}: ${qty} @ ${avg}${pnl?" | PnL: $"+pnl:""}`);
            if(agriSyms.has(sym?.replace(/[A-Z]{2}[FGHJKMNQUVXZ]\d{2}/,"").replace(/\d+/,"").slice(0,2))) overlaps.push(sym);
          });
          if(overlaps.length) pLines.push(`Exposição AgriMacro: ${overlaps.join(", ")}`);
        }

        if(pLines.length) { sections.push("\n=== PORTFÓLIO PESSOAL (IBKR) ==="); sections.push(pLines.join("\n")); }
      }

      // Portfolio breakdown by underlying (parsed options)
      if(portfolio?.positions?.length) {
        const rawPos = portfolio.positions as any[];
        const optByUnd: Record<string, {strikes:number[];exps:Set<string>;netQty:number;legs:number}> = {};
        rawPos.forEach((pos:any) => {
          const ls = (pos.local_symbol||"").trim().split(" ");
          const isOpt = pos.sec_type==="FOP" && ls.length>=2 && /^[CP]\d+/.test(ls[ls.length-1]);
          if(!isOpt) return;
          const sym = pos.symbol;
          if(!optByUnd[sym]) optByUnd[sym] = {strikes:[],exps:new Set(),netQty:0,legs:0};
          const u = optByUnd[sym];
          u.legs++;
          u.netQty += pos.position;
          const right = ls[ls.length-1];
          const rawStrike = parseInt(right.slice(1));
          const div = ({CL:100,SI:100,GF:10,ZL:100,CC:1} as Record<string,number>)[sym]||100;
          u.strikes.push(rawStrike/div);
          const expCode = ls[0].slice(-2);
          const mm:Record<string,string> = {F:"Jan",G:"Fev",H:"Mar",J:"Abr",K:"Mai",M:"Jun",N:"Jul",Q:"Ago",U:"Set",V:"Out",X:"Nov",Z:"Dez"};
          u.exps.add((mm[expCode[0]]||expCode[0])+"/2"+expCode[1]);
        });
        const bLines:string[] = [];
        Object.entries(optByUnd).forEach(([sym,u])=>{
          const dir = u.netQty>0?"NET_LONG":u.netQty<0?"NET_SHORT":"NEUTRO";
          const strikes = [...new Set(u.strikes)].sort((a,b)=>a-b);
          bLines.push(`${sym}: ${u.legs} legs | ${dir} (net=${u.netQty}) | strikes=[${strikes.join(",")}] | venc=[${[...u.exps].join(",")}]`);
        });
        if(bLines.length) {
          sections.push("\n=== PORTFOLIO — BREAKDOWN POR UNDERLYING ===");
          sections.push(bLines.join("\n"));
          sections.push("Nota: gregas reais nao disponiveis no formato IBKR exportado");
        }
      }

      // Greeks e Options Intelligence
      if (greeksData && optionsChain) {
        sections.push(`\n=== OPTIONS INTELLIGENCE ===`);

        // IV rank por underlying (usando options_chain.json)
        const chain = optionsChain?.underlyings || {};

        Object.entries(chain).forEach(([sym, data]: any) => {
          const exps = Object.values(data.expirations || {});
          if (!exps.length) return;

          // ATM IV (primeira expiração disponível)
          const firstExp: any = exps[0];
          const atmCalls = firstExp?.calls || [];
          const atmCall = atmCalls.find((c: any) =>
            Math.abs((c.delta || 0) - 0.5) < 0.1
          );

          if (atmCall?.iv) {
            // Put/Call volume ratio
            const totalCallVol = atmCalls.reduce(
              (s: number, c: any) => s + (c.volume || 0), 0
            );
            const atmPuts = firstExp?.puts || [];
            const totalPutVol = atmPuts.reduce(
              (s: number, c: any) => s + (c.volume || 0), 0
            );
            const pcRatio = totalCallVol > 0
              ? (totalPutVol / totalCallVol).toFixed(2) : 'N/A';

            sections.push(
              `${sym}: IV=${(atmCall.iv * 100).toFixed(1)}% | ` +
              `P/C ratio=${pcRatio} | ` +
              `und_price=${data.und_price}`
            );
          }
        });

        // Greeks totais do portfólio
        const pgTotals = greeksData?.portfolio_greeks;
        if (pgTotals) {
          sections.push(
            `\nPortf\u00f3lio Greeks totais:` +
            ` Delta=${pgTotals.total_delta?.toFixed(2)}` +
            ` | Theta=$${pgTotals.total_theta?.toFixed(2)}/dia` +
            ` | Vega=${pgTotals.total_vega?.toFixed(2)}`
          );
        }
      }

      // Physical international data
      if(physIntl?.international) {
        const phIntlLines:string[] = [];
        Object.entries(physIntl.international).forEach(([k,v]:any)=>{
          if(v?.price) phIntlLines.push(`${v.label||k}: ${v.price_unit?.includes("R$")?"R$":"US$"}${v.price} ${v.price_unit||""} ${v.trend||""}`);
        });
        if(phIntlLines.length) { sections.push("\n=== MERCADO FÍSICO INTERNACIONAL (DETALHADO) ==="); sections.push(phIntlLines.join("\n")); }
      }

      // Stocks/ending stocks from stocks_watch
      if(stocks?.commodities) {
        const stkLines:string[] = [];
        Object.entries(stocks.commodities).forEach(([sym,v]:any)=>{
          if(v?.stock_current) {
            const dev = v.stock_avg ? ((v.stock_current - v.stock_avg) / v.stock_avg * 100).toFixed(1) : "?";
            stkLines.push(`${sym}: ${v.stock_current} ${v.stock_unit||""} | Média 5A: ${v.stock_avg||"?"} | Desvio: ${dev}% | ${v.state||"?"}`);
          }
        });
        if(stkLines.length) { sections.push("\n=== ESTOQUES / ENDING STOCKS ==="); sections.push(stkLines.join("\n")); }
      }

      // PSD USDA ending stocks detail
      if(psdData?.commodities) {
        const psdLines:string[] = [];
        const psdMap:{[k:string]:string} = {soybeans:"ZS",corn:"ZC",wheat:"ZW",soybean_oil:"ZL",soybean_meal:"ZM",cotton:"CT",sugar:"SB"};
        Object.entries(psdData.commodities).forEach(([k,v]:any)=>{
          const sym = psdMap[k] || k.toUpperCase();
          const parts:string[] = [`${sym}`];
          if(v.current != null) parts.push(`estoque=${v.current} ${v.unit||""}`);
          if(v.days_of_use != null) parts.push(`dias_consumo=${v.days_of_use}`);
          if(v.avg_5y != null) parts.push(`media_5a=${v.avg_5y}`);
          if(v.deviation != null) parts.push(`desvio=${v.deviation>0?"+":""}${v.deviation.toFixed(1)}%`);
          psdLines.push(parts.join(" | "));
        });
        if(psdLines.length) { sections.push("\n=== PSD USDA — ENDING STOCKS DETALHADO ==="); sections.push(psdLines.join("\n")); }
      }

      // Physical BR (CEPEA spot prices)
      if(physicalBrData) {
        const brLines:string[] = [];
        const products = physicalBrData.products || physicalBrData;
        if(typeof products === "object") {
          Object.entries(products).forEach(([k,v]:any)=>{
            if(v?.price) {
              const chg = v.change_pct != null ? ` (${v.change_pct>=0?"+":""}${v.change_pct}% d/d)` : (v.trend ? ` ${v.trend}` : "");
              brLines.push(`${v.label||k}: R$${v.price} ${v.unit||""}${chg}${v.source?" | "+v.source:""}`);
            }
          });
        }
        if(brLines.length) { sections.push("\n=== MERCADO FÍSICO BRASIL (CEPEA) ==="); sections.push(brLines.join("\n")); }
      }

      // IMEA Soja (COP Mato Grosso)
      if(imeaData && !imeaData.is_fallback) {
        const imLines:string[] = [];
        if(imeaData.cop_rs_saca != null) imLines.push(`COP MT: R$${imeaData.cop_rs_saca}/saca`);
        if(imeaData.preco_rs_saca != null) imLines.push(`Preço spot MT: R$${imeaData.preco_rs_saca}/saca`);
        if(imeaData.margem_pct != null) imLines.push(`Margem produtor: ${imeaData.margem_pct>0?"+":""}${imeaData.margem_pct.toFixed(1)}%`);
        if(imeaData.safra) imLines.push(`Safra: ${imeaData.safra}`);
        if(imeaData.summary) imLines.push(imeaData.summary);
        if(imLines.length) { sections.push("\n=== CUSTO DE PRODUÇÃO / MARGEM PRODUTOR (IMEA) ==="); sections.push(imLines.join("\n")); }
      }

      // COT Delta — Sinais de reversão
      if (cot?.commodities) {
        const cotSyms = ['ZC','ZS','ZW','ZL','ZM','CL','GF','LE','CC','SB','NG','KC','CT'];
        const deltaLines: string[] = [];
        cotSyms.forEach(sym => {
          const da = cot.commodities[sym]?.disaggregated?.delta_analysis;
          if (!da) return;
          const sig = da.signals?.[0];
          if (!sig || sig.type === 'NEUTRAL') return;
          deltaLines.push(`${sym}: ${sig.icon} ${sig.label} (${da.dominant_direction}, ${da.reversal_score}% prob) | Delta: ${da.current_delta > 0 ? '+' : ''}${da.current_delta?.toLocaleString()} | ${da.neg_streak || da.pos_streak}sem streak`);
        });
        if (deltaLines.length) {
          sections.push("\n=== COT DELTA \u2014 SINAIS DE REVERS\u00c3O ===");
          deltaLines.forEach(l => sections.push(l));
        }
      }

      // Paridades e correlações
      if (paritiesData?.parities) {
        const p = paritiesData.parities;
        sections.push("\n=== PARIDADES E CORRELAÇÕES ===");
        Object.values(p).forEach((par: any) => {
          let line = `${par.name}: ${par.value} ${par.unit}`;
          line += ` | z=${par.z_score ?? 'n/a'}`;
          line += ` | ${par.signal}`;
          sections.push(line);
        });
      }

      // LACUNA 1: Sazonalidade (pre-computed monthly_returns)
      if (season) {
        const months = ['Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'];
        const curMonth = new Date().getMonth();
        const symList = [selected, ...['ZC','ZS','ZW','LE','GF','CL','SB','KC','CC'].filter(s => s !== selected)].slice(0, 8);
        const seasonLines: string[] = [];
        symList.forEach(sym => {
          const s = (season as any)?.[sym];
          if (!s?.monthly_returns) return;
          const returns = s.monthly_returns;
          const v = returns[curMonth];
          const val = typeof v === 'number' ? v : v?.avg ?? 0;
          const pctPos = typeof v === 'object' ? v?.positive_pct : null;
          seasonLines.push(`${sym} em ${months[curMonth]}: ${val >= 0 ? '+' : ''}${val.toFixed(2)}%${pctPos != null ? ` (positivo ${pctPos}%)` : ''}`);
          // Add full year for selected
          if (sym === selected) {
            returns.forEach((rv: any, i: number) => {
              const rv2 = typeof rv === 'number' ? rv : rv?.avg ?? 0;
              const isCur = i === curMonth ? ' \u2190 MES ATUAL' : '';
              seasonLines.push(`  ${months[i]}: ${rv2 >= 0 ? '+' : ''}${rv2.toFixed(2)}%${isCur}`);
            });
          }
        });
        if (seasonLines.length) {
          sections.push(`\n=== SAZONALIDADE \u2014 ${months[curMonth].toUpperCase()} (${(season as any)?.[selected]?.window_full || '5A'}) ===`);
          seasonLines.forEach(l => sections.push(l));
        }
      }

      // LACUNA 2: Bilateral Indicators (detailed)
      if (bilateralData?.summary) {
        sections.push("\n=== BILATERAL BR vs EUA (DETALHADO) ===");
        if (bilateralData.summary) sections.push(`Resumo: ${JSON.stringify(bilateralData.summary).slice(0, 300)}`);
        if (bilateralData.lcs?.status === "OK") {
          const l = bilateralData.lcs;
          sections.push(`LCS: Spread $${l.spread_usd_mt?.toFixed(2)}/MT (${l.spread_pct?.toFixed(1)}%) | Mais competitivo: ${l.competitive_origin} | FOB BR=$${l.br_fob?.toFixed(0)} EUA=$${l.us_fob?.toFixed(0)}`);
        }
        if (bilateralData.bci?.status === "OK") {
          const b = bilateralData.bci;
          sections.push(`BCI: Score=${b.bci_score} (${b.bci_signal}) | Trend=${b.bci_trend} | Forte=${b.strongest} | Fraco=${b.weakest}`);
        }
      }

      // LACUNA 3: Livestock Bottleneck
      if (bottleneckData) {
        const bnLines: string[] = [];
        ['LE','GF','HE'].forEach(sym => {
          const d = (bottleneckData as any)[sym];
          if (!d) return;
          bnLines.push(`${sym}: Score=${d.score ?? 'N/A'} | Mom3m=${d.mom_3m ?? 'N/A'}% | Mom6m=${d.mom_6m ?? 'N/A'}% | ${d.recommendation ?? ''}`);
        });
        if (bnLines.length) {
          sections.push("\n=== LIVESTOCK BOTTLENECK THESIS ===");
          bnLines.forEach(l => sections.push(l));
        }
      }

      // EIA Energy
      if (eiaData?.series) {
        const s = eiaData.series as any;
        const eiaLines: string[] = [];
        const fields: [string, string][] = [
          ['wti_spot', 'WTI Spot'],
          ['natural_gas_spot', 'Henry Hub'],
          ['diesel_retail', 'Diesel Retail EUA'],
          ['gasoline_retail', 'Gasolina Retail EUA'],
          ['ethanol_production', 'Produ\u00e7\u00e3o Etanol'],
          ['crude_stocks', 'Estoques CL'],
          ['crude_production', 'Produ\u00e7\u00e3o CL EUA'],
          ['refinery_utilization', 'Utiliza\u00e7\u00e3o Refinarias'],
        ];
        fields.forEach(([key, label]) => {
          const item = s[key];
          if (!item?.latest_value) return;
          const chg = item.wow_change_pct != null
            ? ` (${item.wow_change_pct > 0 ? '+' : ''}${item.wow_change_pct.toFixed(1)}% sem)`
            : '';
          eiaLines.push(`${label}: ${item.latest_value} ${item.unit || ''}${chg}`);
        });
        if (eiaLines.length) {
          sections.push("\n=== ENERGIA (EIA) ===");
          eiaLines.forEach(l => sections.push(l));
        }
      }

      // CONAB Safra Brasil
      if (conabData?.boletim_info) {
        const bi = conabData.boletim_info;
        const cult = bi.principais_culturas || {};
        const conabLines: string[] = [];
        if (bi.producao_total_mt) conabLines.push(`Produ\u00e7\u00e3o total BR: ${bi.producao_total_mt} mi t (${bi.safra || ''}, ${bi.levantamento || ''}o lev.)`);
        for (const [k, v] of Object.entries(cult)) {
          const c = v as any;
          if (c?.producao_mt) {
            conabLines.push(`${k.toUpperCase()}: ${c.producao_mt} mi t | \u00e1rea ${c.area_mha || '?'} mi ha | prod. ${c.produtividade_kg_ha || '?'} kg/ha`);
          }
        }
        if (conabLines.length) {
          sections.push("\n=== CONAB \u2014 SAFRA BRASIL ===");
          conabLines.forEach(l => sections.push(l));
        }
      }

      // Bilateral (basis temporal)
      if (bilateralData && typeof bilateralData === 'object') {
        const biLines: string[] = [];
        const entries = bilateralData.commodities || bilateralData.data || bilateralData;
        if (typeof entries === 'object') {
          Object.entries(entries).forEach(([key, val]: any) => {
            if (!val || typeof val !== 'object') return;
            const name = val.name || val.label || key;
            const signal = val.signal || val.trend || '';
            const value = val.current_value ?? val.value ?? val.spread;
            const zscore = val.zscore ?? val.z_score;
            if (value !== undefined) {
              biLines.push(`${name}: ${value}${zscore != null ? ` (z=${zscore})` : ''}${signal ? ` \u2014 ${signal}` : ''}`);
            }
          });
        }
        if (biLines.length) {
          sections.push("\n=== BILATERAL BR vs EUA ===");
          biLines.forEach(l => sections.push(l));
        }
      }

      // PROTEÍNA ANIMAL — PSD Global
      if (livestockPsdData?.commodities) {
        const psdLines: string[] = [];
        const syms: Record<string,string> = { LE: "Beef", HE: "Pork", PO: "Poultry" };
        Object.entries(syms).forEach(([sym, name]) => {
          const d = livestockPsdData.commodities[sym];
          if (!d) return;
          const hasSummary = ['usa','brazil','china'].some(region => {
            const s = d[region]?.summaries;
            return s && Object.keys(s).length > 0;
          });
          if (!hasSummary) return;
          psdLines.push(`${name} (${sym}):`);
          const usaS = d.usa?.summaries || {};
          if (usaS.production) psdLines.push(`  EUA produção: ${usaS.production.current} 1000MT (${usaS.production.deviation_pct>0?"+":""}${usaS.production.deviation_pct}% vs 5A)`);
          if (usaS.imports) psdLines.push(`  EUA importações: ${usaS.imports.current} 1000MT (${usaS.imports.deviation_pct>0?"+":""}${usaS.imports.deviation_pct}% vs 5A)`);
          const brS = d.brazil?.summaries || {};
          if (brS.exports) psdLines.push(`  Brasil exportações: ${brS.exports.current} 1000MT (${brS.exports.deviation_pct>0?"+":""}${brS.exports.deviation_pct}% vs 5A)`);
          const cnS = d.china?.summaries || {};
          if (cnS.production) psdLines.push(`  China produção: ${cnS.production.current} 1000MT (${cnS.production.deviation_pct>0?"+":""}${cnS.production.deviation_pct}% vs 5A)`);
        });
        if (psdLines.length) {
          sections.push("\n=== PROTEÍNA ANIMAL — OFERTA GLOBAL (USDA PSD) ===");
          psdLines.forEach(l => sections.push(l));
        }
      }

      // PROTEÍNA ANIMAL — Indicadores Semanais
      if (livestockWeeklyData?.data) {
        const wLines: string[] = [];
        const wd = livestockWeeklyData.data;
        if (wd.cold_storage_le) {
          const cs = wd.cold_storage_le;
          wLines.push(`Cold Storage Beef: ${cs.signal}${cs.deviation_pct!=null?` (${cs.deviation_pct>0?"+":""}${cs.deviation_pct}% vs média)`:""}${cs.interpretation?` — ${cs.interpretation}`:""}`);
        }
        if (wd.cold_storage_he) {
          const cs = wd.cold_storage_he;
          wLines.push(`Cold Storage Pork: ${cs.signal}${cs.deviation_pct!=null?` (${cs.deviation_pct>0?"+":""}${cs.deviation_pct}% vs média)`:""}`);
        }
        if (wd.abate_bovinos_br && !wd.abate_bovinos_br.is_fallback) {
          const ab = wd.abate_bovinos_br;
          wLines.push(`Abate Bovinos Brasil: ${ab.signal}${ab.deviation_pct!=null?` (${ab.deviation_pct>0?"+":""}${ab.deviation_pct}% vs média)`:""}${ab.interpretation?` — ${ab.interpretation}`:""}`);
        }
        if (wd.packer_le) {
          const pp = wd.packer_le;
          wLines.push(`Packer Activity LE: ${pp.signal} (momentum 20d: ${pp.momentum_20d>0?"+":""}${pp.momentum_20d}%)${pp.interpretation?` — ${pp.interpretation}`:""}`);
        }
        if (wd.packer_he) {
          const pp = wd.packer_he;
          wLines.push(`Packer Activity HE: ${pp.signal} (momentum 20d: ${pp.momentum_20d>0?"+":""}${pp.momentum_20d}%)`);
        }
        if (wLines.length) {
          sections.push("\n=== PROTEÍNA ANIMAL — INDICADORES SEMANAIS ===");
          wLines.forEach(l => sections.push(l));
        }
      }

      // Crop Progress (USDA NASS)
      if (cropProgressData?.crops) {
        const cpLines: string[] = [];
        Object.entries(cropProgressData.crops).forEach(([key, data]: any) => {
          const nat = data.national || {};
          const parts: string[] = [`${key}`];
          if (nat.planted != null) {
            parts.push(`${nat.planted}% plantado`);
          }
          if (nat.emerged != null) parts.push(`${nat.emerged}% emergido`);
          if (nat.harvested != null) parts.push(`${nat.harvested}% colhido`);
          cpLines.push(parts.join(' | '));
        });
        if (cpLines.length) {
          sections.push("\n=== CROP PROGRESS (USDA) ===");
          cpLines.forEach(l => sections.push(l));
        }
      }

      // Export Activity
      if (exportActivityData?.commodities) {
        const exLines: string[] = [];
        Object.entries(exportActivityData.commodities).forEach(([sym, data]: any) => {
          const parts: string[] = [`${sym}`];
          if (data.pct_of_target != null) parts.push(`${data.pct_of_target.toFixed(1)}% do target USDA`);
          if (data.vs_last_year_pct != null) parts.push(`${data.vs_last_year_pct > 0 ? '+' : ''}${data.vs_last_year_pct.toFixed(1)}% vs ano ant.`);
          if (data.signal) parts.push(data.signal);
          exLines.push(parts.join(' | '));
        });
        if (exLines.length) {
          sections.push("\n=== EXPORT ACTIVITY ===");
          exLines.forEach(l => sections.push(l));
        }
      }

      // Drought Monitor
      if (droughtData?.national) {
        sections.push("\n=== DROUGHT MONITOR ===");
        const nat = droughtData.national;
        sections.push(`Nacional: ${nat.any_drought_pct?.toFixed(1)}% em drought (D2+: ${((nat.d2_pct||0)+(nat.d3_pct||0)+(nat.d4_pct||0)).toFixed(1)}%)`);
        if (droughtData.regions) {
          Object.entries(droughtData.regions).forEach(([key, reg]: any) => {
            sections.push(`${reg.label}: D2+=${reg.d2_plus_pct?.toFixed(1)}% | ${reg.signal}`);
          });
        }
        if (droughtData.crop_alerts?.length) {
          droughtData.crop_alerts.forEach((a: any) => sections.push(`ALERTA: ${a.message}`));
        }
      }

      // Fertilizer Prices
      if (fertilizerData?.fertilizers) {
        const fLines: string[] = [];
        Object.entries(fertilizerData.fertilizers).forEach(([key, data]: any) => {
          if (data.price_usd_ton != null) {
            fLines.push(`${data.name || key}: $${data.price_usd_ton}/ton${data.change_yoy_pct != null ? ` (${data.change_yoy_pct > 0 ? '+' : ''}${data.change_yoy_pct.toFixed(1)}% YoY)` : ''} ${data.signal || ''}`);
          }
        });
        if (fLines.length) {
          sections.push("\n=== FERTILIZANTES ===");
          sections.push(fertilizerData.lag_note || "Lag: impacto no custo de plantio 6-12 meses");
          fLines.forEach(l => sections.push(l));
          if (fertilizerData.cost_impact?.signal) {
            sections.push(`Impacto custo: ${fertilizerData.cost_impact.signal} ${fertilizerData.cost_impact.detail || ''}`);
          }
        }
      }

      // Analysis request
      sections.push("\n=== ANÁLISE SOLICITADA ===");
      sections.push("Com base em todos os dados acima:\n1. Quais são os maiores riscos para o portfólio atual dado o ambiente de mercado?\n2. Quais posições estão alinhadas com os sinais multifatoriais?\n3. Quais estão em conflito com os sinais (maior risco)?\n4. Recomendações de ajuste de exposição (apresentar como análise de risco, sem ser prescritivo)\n5. O que monitorar nas próximas 24-48 horas?\n\nResponda em português brasileiro, tom institucional e analítico.");

      return sections.join("\n");
    };

    // ═══════════════════════════════════════════════════════
    // Per-commodity deep dive prompt (purple buttons)
    // ═══════════════════════════════════════════════════════
    const buildCommodityPrompt = (sym: string) => {
      const sections: string[] = [];
      const name = COMMODITIES.find(c => c.sym === sym)?.name || sym;
      const monthNames = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"];
      const curMonth = new Date().getMonth();

      sections.push(`Analise completa da commodity ${sym} (${name}) usando o framework Council AgriMacro v2.2 com 5 conselheiros adversariais. Cada conselheiro DEVE cruzar Analise Tecnica + Analise Fundamental antes de emitir veredicto. Chairman entrega proximo passo concreto com threshold.`);

      // 1. COMMODITY DNA
      try {
        const dnaStatic = (window as any).__dnaStatic || null;
        const dnaDynamic = commodityDna;
        const st = dnaStatic?.commodities?.[sym];
        const dy = dnaDynamic?.commodities?.[sym];
        if (st?.drivers_ranked || dy?.drivers_ranked) {
          sections.push(`\nCOMMODITY DNA ${sym}:`);
          if (dy?.composite_signal) sections.push(`Sinal composto: ${dy.composite_signal} (${dy.bullish_drivers} bull / ${dy.bearish_drivers} bear)`);
          const drivers = st?.drivers_ranked || dy?.drivers_ranked || [];
          drivers.forEach((d: any) => sections.push(`  ${d.rank || "-"}. ${d.driver} (${d.weight || d.strength || "?"})${d.description ? " — " + d.description.slice(0, 100) : ""}`));
        }
      } catch {}

      // 2. OPTIONS (IV, Skew, Term Structure)
      try {
        const und = optionsChain?.underlyings?.[sym];
        if (und) {
          sections.push(`\nOPTIONS INTELLIGENCE ${sym}:`);
          const ivr = und.iv_rank || {};
          const sk = und.skew || {};
          const ts = und.term_structure || {};
          sections.push(`  IV ATM: ${ivr.current_iv ? (ivr.current_iv * 100).toFixed(1) + "%" : "N/A"}`);
          sections.push(`  IV Rank 52w: ${ivr.rank_52w != null ? ivr.rank_52w.toFixed(0) + "%" : "N/A (building, " + (ivr.history_days || 0) + " dias)"}`);
          sections.push(`  Skew (put25d vs call25d): ${sk.skew_pct != null ? (sk.skew_pct > 0 ? "+" : "") + sk.skew_pct + "%" : "N/A"}`);
          sections.push(`  Term Structure: ${ts.structure || "N/A"}`);
          if (ts.points?.length) {
            sections.push(`  Curva IV: ${ts.points.map((p: any) => p.dte + "d=" + (p.iv * 100).toFixed(0) + "%").join(" | ")}`);
          }
        } else {
          sections.push(`\nOPTIONS INTELLIGENCE ${sym}: N/A (sem dados de options chain)`);
        }
      } catch {}

      // 3. COT POSITIONING
      try {
        const c = cot?.commodities?.[sym];
        if (c) {
          sections.push(`\nCOT POSITIONING ${sym}:`);
          const dis: any = c.disaggregated || {};
          const leg: any = c.legacy || {};
          if (dis.cot_index != null) {
            const label = dis.cot_index >= 80 ? "CROWDED LONG" : dis.cot_index <= 20 ? "CROWDED SHORT" : dis.cot_index >= 65 ? "longs acumulando" : dis.cot_index <= 35 ? "shorts acumulando" : "neutro";
            sections.push(`  COT Index (156w): ${dis.cot_index.toFixed(0)} — ${label}`);
            if (dis.cot_index_52w != null) sections.push(`  COT Index (52w): ${dis.cot_index_52w.toFixed(0)}`);
            if (dis.cot_index_26w != null) sections.push(`  COT Index (26w): ${dis.cot_index_26w.toFixed(0)}`);
          } else if (leg.latest) {
            sections.push(`  Legacy net: ${leg.latest.noncomm_net} (sem disaggregated)`);
          } else {
            sections.push(`  N/A`);
          }
        } else {
          sections.push(`\nCOT POSITIONING ${sym}: N/A`);
        }
      } catch {}

      // 4. SAZONALIDADE
      try {
        const s: any = season?.[sym];
        if (s?.monthly_returns) {
          const mr = s.monthly_returns[curMonth];
          const avg = typeof mr === "number" ? mr : mr?.avg ?? null;
          sections.push(`\nSAZONALIDADE ${sym} ${monthNames[curMonth]}:`);
          if (avg != null) {
            const strength = Math.abs(avg) >= 2 ? "FORTE" : Math.abs(avg) >= 1 ? "moderado" : "fraco";
            sections.push(`  Desvio ${avg >= 0 ? "+" : ""}${avg.toFixed(2)}% vs historico (${strength})`);
          } else {
            sections.push(`  N/A`);
          }
        } else {
          sections.push(`\nSAZONALIDADE ${sym}: N/A`);
        }
      } catch {}

      // 5. SPREADS related
      try {
        if (spreads?.spreads) {
          const related: string[] = [];
          Object.entries(spreads.spreads).forEach(([k, v]: any) => {
            const nameStr = (v.name || k).toLowerCase();
            const symLower = sym.toLowerCase();
            const symNames: Record<string, string[]> = {
              ZS: ["soy", "soja", "crush"], ZC: ["corn", "milho", "zc_"], ZW: ["wheat", "trigo", "ke_zw", "feed_wheat"],
              ZL: ["oil", "oleo", "zl_", "crush"], ZM: ["meal", "farelo", "crush"], KE: ["ke_", "wheat"],
              LE: ["cattle", "feedlot", "crush"], GF: ["feeder", "feedlot"], CL: ["crude", "cl", "oil"],
              CC: ["cocoa", "cacau"], SB: ["sugar", "acucar"], KC: ["coffee", "cafe"],
            };
            const terms = symNames[sym] || [sym.toLowerCase()];
            if (terms.some(t => nameStr.includes(t) || k.toLowerCase().includes(t))) {
              related.push(`  ${v.name || k}: ${v.current?.toFixed(4) || "?"} ${v.unit || ""} | z=${v.zscore_1y?.toFixed(2) || "?"} | ${v.regime || "?"}`);
            }
          });
          if (related.length) {
            sections.push(`\nSPREADS relacionados a ${sym}:`);
            related.forEach(r => sections.push(r));
          }
        }
      } catch {}

      // 6. PORTFOLIO position for this commodity
      try {
        if (portfolio?.positions) {
          const myPos = portfolio.positions.filter((p: any) => p.symbol === sym && (p.sec_type === "FOP" || p.sec_type === "FUT"));
          if (myPos.length) {
            sections.push(`\nPOSICAO PORTFOLIO ${sym}:`);
            myPos.forEach((p: any) => {
              const pos = p.position > 0 ? `+${p.position}` : `${p.position}`;
              const d = p.delta != null ? ` delta=${p.delta.toFixed(4)}` : "";
              const iv = p.iv != null ? ` iv=${(p.iv * 100).toFixed(0)}%` : "";
              sections.push(`  ${pos}x ${p.local_symbol}${d}${iv}`);
            });
          } else {
            sections.push(`\nPOSICAO PORTFOLIO ${sym}: Nenhuma posicao aberta`);
          }
        }
      } catch {}

      // 7. Price context
      try {
        const bars = prices?.[sym];
        if (Array.isArray(bars) && bars.length) {
          const last = bars[bars.length - 1];
          const prev5 = bars.length >= 6 ? bars[bars.length - 6] : null;
          const prev20 = bars.length >= 21 ? bars[bars.length - 21] : null;
          sections.push(`\nPRECO ${sym}:`);
          sections.push(`  Atual: $${last.close} (${last.date})`);
          if (prev5) sections.push(`  Momentum 5d: ${((last.close - prev5.close) / prev5.close * 100).toFixed(1)}%`);
          if (prev20) sections.push(`  Momentum 20d: ${((last.close - prev20.close) / prev20.close * 100).toFixed(1)}%`);
        }
      } catch {}

      sections.push(`\nAnalise esta commodity usando os dados acima. Se algum dado nao estiver disponivel, escrever "N/A" — nunca fabricar. Formato: 5 conselheiros com veredicto, Chairman com proximo passo concreto e threshold.`);

      return sections.join("\n");
    };

    // -- MACRO cards --
    const dxSeries = prices?.["DX"] || dxProcessed;
    const dxLast = dxSeries?.slice(-1)[0];
    const dxPrev = dxSeries?.slice(-2,-1)[0];
    const dxChg = dxLast&&dxPrev ? ((dxLast.close-dxPrev.close)/dxPrev.close*100) : null;

    const brlSeries = bcbData?.brl_usd;
    const brlLast = brlSeries?.slice(-1)[0];
    const brlPrev = brlSeries?.slice(-2,-1)[0];
    const brlChg = brlLast&&brlPrev ? ((brlLast.value-brlPrev.value)/brlPrev.value*100) : null;

    const selicSeries = bcbData?.selic_meta;
    const selicLast = selicSeries?.slice(-1)[0];

    // macro_indicators.json data
    const mi = macroIndicators;
    const sp = mi?.sp500;
    const vix = mi?.vix;
    const ty = mi?.treasury_10y;

    const macroCards:{label:string;value:string;change:number|null;unit:string;date:string}[] = [
      {label:"DXY (Dollar Index)",value:dxLast?dxLast.close.toFixed(2):"--",change:dxChg,unit:"index",date:dxLast?.date||""},
      {label:"BRL/USD",value:brlLast?brlLast.value.toFixed(4):"--",change:brlChg,unit:"R$",date:brlLast?.date||""},
      {label:"Selic Meta",value:selicLast?selicLast.value.toFixed(2)+"%":"--",change:null,unit:"% a.a.",date:selicLast?.date||""},
      {label:"S&P 500",value:sp?sp.price.toLocaleString("en-US",{minimumFractionDigits:1}):"--",change:sp?.change_pct??null,unit:"index",date:sp?.date||""},
      {label:"VIX",value:vix?vix.value.toFixed(1):"--",change:vix?.change_pct??null,unit:"index",date:vix?.date||""},
      {label:"Juros 10Y EUA",value:ty?ty.yield_pct.toFixed(3)+"%":"--",change:null,unit:"%",date:ty?.date||""},
    ];

    // -- WEATHER --
    const weatherRegions = weatherData?.regions || {};
    const regionKeys = ["corn_belt","cerrado_mt","sul_pr_rs","pampas_arg"];
    const regionLabels:Record<string,string> = {corn_belt:"Corn Belt",cerrado_mt:"Cerrado MT",sul_pr_rs:"Sul BR (PR/RS)",pampas_arg:"Pampas Argentina"};

    function weatherStatus(r:any):{label:string;color:string} {
      if(!r) return {label:"Sem dados",color:"#64748b"};
      const alerts = r.alerts || [];
      if(alerts.some((a:any)=>a.severity==="ALTA"||a.severity==="CRITICA")) return {label:"Alerta",color:"#DC3C3C"};
      if(alerts.length>0) return {label:"Atenção",color:"#DCB432"};
      return {label:"Normal",color:"#00C878"};
    }

    return (
      <div style={{maxWidth:1000}}>
        <div style={{fontSize:20,fontWeight:800,color:"#DCB432",marginBottom:4,letterSpacing:1}}>CENTRAL DE INTELIGÊNCIA</div>
        <div style={{fontSize:11,color:"#64748b",marginBottom:24}}>Visão consolidada: macro, clima, sentimento e síntese IA</div>

        {/* SEÇÃO 0: SÍNTESE DO DIA */}
        {(()=>{
          const syn = intelSynthesis;
          if(!syn || syn.is_fallback) return null;
          const sc = syn.signal_count||{};
          const prioColors:Record<string,{bg:string;text:string;label:string}> = {
            high:{bg:"rgba(220,60,60,.12)",text:"#DC3C3C",label:"CRÍTICO"},
            medium:{bg:"rgba(220,180,50,.12)",text:"#DCB432",label:"ATENÇÃO"},
            low:{bg:"rgba(59,130,246,.10)",text:"#3b82f6",label:"INFO"},
          };
          const catIcons:Record<string,string> = {composite:"⚡",macro:"\ud83c\udfe6",spread:"\ud83d\udcca",causal:"\ud83d\udd17",clima:"☁️",sentimento:"\ud83d\udcf1",safra:"\ud83c\udf31"};
          const allSignals = [...(syn.priority_high||[]),...(syn.priority_medium||[]),...(syn.priority_low||[])];
          return (
            <div style={{...panelStyle,border:"1px solid rgba(220,180,50,.25)",marginBottom:24}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:14}}>
                {sectionTitle("SÍNTESE DO DIA")}
                <div style={{display:"flex",gap:8}}>
                  {sc.high>0 && <div style={{fontSize:10,fontWeight:700,color:"#DC3C3C",background:"rgba(220,60,60,.12)",padding:"3px 10px",borderRadius:12}}>{sc.high} crítico{sc.high>1?"s":""}</div>}
                  {sc.medium>0 && <div style={{fontSize:10,fontWeight:700,color:"#DCB432",background:"rgba(220,180,50,.12)",padding:"3px 10px",borderRadius:12}}>{sc.medium} atenção</div>}
                  {sc.low>0 && <div style={{fontSize:10,fontWeight:700,color:"#3b82f6",background:"rgba(59,130,246,.10)",padding:"3px 10px",borderRadius:12}}>{sc.low} info</div>}
                </div>
              </div>

              {/* Summary */}
              {syn.summary && (
                <div style={{background:"#0E1A24",borderRadius:8,padding:16,marginBottom:14,border:"1px solid rgba(220,180,50,.2)",borderLeft:"3px solid #DCB432"}}>
                  <div style={{fontSize:12,color:"#E8ECF1",lineHeight:1.8,whiteSpace:"pre-wrap"}}>{syn.summary}</div>
                  <div style={{fontSize:8,color:"#64748b",marginTop:8}}>{syn.generated_at?.slice(0,16).replace("T"," ")}</div>
                </div>
              )}

              {/* Signal list */}
              <div style={{display:"flex",flexDirection:"column",gap:6}}>
                {allSignals.slice(0,12).map((s:any,i:number)=>{
                  const pc = prioColors[s.priority]||prioColors.low;
                  return (
                    <div key={i} style={{display:"flex",alignItems:"flex-start",gap:8,padding:"8px 12px",background:"#0E1A24",borderRadius:6,borderLeft:`3px solid ${pc.text}`}}>
                      <div style={{fontSize:12,minWidth:18}}>{catIcons[s.category]||"◆"}</div>
                      <div style={{flex:1}}>
                        <div style={{fontSize:10,fontWeight:600,color:"#E8ECF1"}}>{s.title}</div>
                        {s.detail && <div style={{fontSize:9,color:"#8899AA",marginTop:2}}>{s.detail}</div>}
                      </div>
                      <div style={{fontSize:8,fontWeight:700,color:pc.text,background:pc.bg,padding:"2px 6px",borderRadius:4,whiteSpace:"nowrap"}}>{pc.label}</div>
                    </div>
                  );
                })}
              </div>

              {/* Causal chains */}
              {syn.causal_chains_active?.length>0 && (
                <div style={{marginTop:12}}>
                  <div style={{fontSize:9,fontWeight:600,color:"#8899AA",letterSpacing:0.5,marginBottom:6}}>CADEIAS CAUSAIS ATIVAS</div>
                  <div style={{display:"flex",gap:8,flexWrap:"wrap"}}>
                    {syn.causal_chains_active.map((c:any)=>(
                      <div key={c.id} style={{background:"#0E1A24",borderRadius:6,padding:"6px 10px",border:"1px solid rgba(220,180,50,.15)",fontSize:9}}>
                        <div style={{fontWeight:700,color:"#DCB432"}}>{c.name}</div>
                        <div style={{color:"#8899AA",marginTop:2}}>{c.signal}</div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Deepen with AI */}
              <div style={{marginTop:14,textAlign:"center"}}>
                <button onClick={async()=>{
                  if(intelCouncilLoading) return;
                  setIntelCouncilLoading(true);
                  try {
                    const prompt = buildIntelPrompt();
                    const res = await fetch("/api/chat",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({messages:[{role:"user",content:prompt}]})});
                    const d = await res.json();
                    if(res.ok && d.response){
                      const entry={text:d.response,time:new Date().toLocaleString("pt-BR")};
                      setIntelCouncil(entry);
                      setCouncilHistory(prev=>{const updated=[entry,...prev].slice(0,5);try{localStorage.setItem("agrimacro_council_history",JSON.stringify(updated));}catch{}return updated;});
                    } else setIntelCouncil({text:"ERRO: "+(d.error||`API ${res.status}`),time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})});
                  }catch(e:any){setIntelCouncil({text:"ERRO: "+e.message,time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})});}
                  setIntelCouncilLoading(false);
                }} disabled={intelCouncilLoading} style={{
                  padding:"8px 24px",fontSize:10,fontWeight:600,borderRadius:6,cursor:intelCouncilLoading?"wait":"pointer",
                  background:intelCouncilLoading?"#1E3044":"rgba(220,180,50,.10)",color:"#DCB432",
                  border:"1px solid #DCB43244",transition:"all .2s",
                }}>{intelCouncilLoading?"Analisando...":"Aprofundar com IA"}</button>

                {/* Council Quick Button */}
                <button onClick={async()=>{
                  if(intelCouncilLoading) return;
                  setIntelCouncilLoading(true);
                  try {
                    const res = await fetch("/api/council",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({mode:"quick"})});
                    const d = await res.json();
                    if(res.ok && d.response){
                      const entry={text:d.response,time:new Date().toLocaleString("pt-BR")};
                      setIntelCouncil(entry);
                      setCouncilHistory(prev=>{const updated=[entry,...prev].slice(0,5);try{localStorage.setItem("agrimacro_council_history",JSON.stringify(updated));}catch{}return updated;});
                    } else setIntelCouncil({text:"ERRO: "+(d.error||"API error"),time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})});
                  }catch(e:any){setIntelCouncil({text:"ERRO: "+e.message,time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})});}
                  setIntelCouncilLoading(false);
                }} disabled={intelCouncilLoading} style={{
                  padding:"8px 20px",fontSize:10,fontWeight:600,borderRadius:6,cursor:intelCouncilLoading?"wait":"pointer",
                  background:"rgba(124,58,237,.12)",color:"#a78bfa",marginLeft:8,
                  border:"1px solid rgba(124,58,237,.3)",transition:"all .2s",
                }}>{intelCouncilLoading?"Council...":"Council Quick"}</button>

                {/* Council Full Button — job polling */}
                <button onClick={async()=>{
                  if(intelCouncilLoading) return;
                  setIntelCouncilLoading(true);
                  setCouncilStage("Iniciando Council v2.2...");
                  try {
                    const res = await fetch("/api/council",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({mode:"full"})});
                    const d = await res.json();
                    if(!d.jobId) throw new Error(d.error || "No jobId returned");
                    const jobId = d.jobId;
                    // Poll every 5s
                    const poll = setInterval(async()=>{
                      try {
                        const sr = await fetch(`/api/council?jobId=${jobId}`);
                        const st = await sr.json();
                        if(st.status==="running"){
                          setCouncilStage(st.detail || st.stage || "Processando...");
                        } else if(st.status==="complete" && st.response){
                          clearInterval(poll);
                          const entry={text:st.response,time:new Date().toLocaleString("pt-BR")};
                          setIntelCouncil(entry);
                          setCouncilHistory(prev=>{const updated=[entry,...prev].slice(0,5);try{localStorage.setItem("agrimacro_council_history",JSON.stringify(updated));}catch{}return updated;});
                          setCouncilStage("");
                          setIntelCouncilLoading(false);
                        } else if(st.status==="error"){
                          clearInterval(poll);
                          setIntelCouncil({text:"ERRO: "+(st.error||"Unknown"),time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})});
                          setCouncilStage("");
                          setIntelCouncilLoading(false);
                        }
                      } catch(pe:any){
                        clearInterval(poll);
                        setIntelCouncil({text:"ERRO polling: "+pe.message,time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})});
                        setCouncilStage("");
                        setIntelCouncilLoading(false);
                      }
                    }, 5000);
                  }catch(e:any){
                    setIntelCouncil({text:"ERRO: "+e.message,time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})});
                    setCouncilStage("");
                    setIntelCouncilLoading(false);
                  }
                }} disabled={intelCouncilLoading} style={{
                  padding:"8px 20px",fontSize:10,fontWeight:600,borderRadius:6,cursor:intelCouncilLoading?"wait":"pointer",
                  background:intelCouncilLoading?"rgba(220,60,60,.20)":"rgba(220,60,60,.10)",color:"#ef4444",marginLeft:8,
                  border:"1px solid rgba(220,60,60,.25)",transition:"all .2s",
                }}>{intelCouncilLoading?(councilStage||"Council..."):"Council Full"}</button>

                <div style={{fontSize:9,color:"#64748b",marginTop:4}}>Aprofundar: chat | Council Quick: 300 palavras | Council Full: relatorio executivo completo</div>
              </div>
            </div>
          );
        })()}

        {/* MAPA DE CORRELAÇÕES */}
        <div style={panelStyle}>
          {sectionTitle("MAPA DE CORRELAÇÕES")}
          {correlations && !correlations.is_fallback ? (
            <CorrelationMap correlations={correlations} prices={prices} />
          ) : (
            placeholder("correlations.json não disponível. Execute o pipeline para gerar correlações.")
          )}
        </div>

        {/* SEÇÃO 1: MACRO DO DIA */}
        <div style={panelStyle}>
          {sectionTitle("MACRO DO DIA")}
          <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(150px,1fr))",gap:12}}>
            {macroCards.map(mc=>{
              const isVix = mc.label==="VIX";
              const isSP = mc.label==="S&P 500";
              const is10Y = mc.label==="Juros 10Y EUA";
              const vixColors:Record<string,{bg:string;text:string}> = {baixo:{bg:"rgba(0,200,120,.15)",text:"#00C878"},normal:{bg:"rgba(148,163,184,.12)",text:"#8899AA"},atencao:{bg:"rgba(245,158,11,.18)",text:"#f59e0b"},extremo:{bg:"rgba(220,60,60,.18)",text:"#DC3C3C"}};
              const vixLevel = isVix&&vix ? (vix.value>=30?"extremo":vix.value>=20?"atencao":vix.value>=15?"normal":"baixo") : "";
              const vc = isVix&&vix ? vixColors[vixLevel]||vixColors.normal : null;
              return (
                <div key={mc.label} style={{background:"#0E1A24",borderRadius:8,padding:14,border:isVix&&vc?`1px solid ${vc.text}33`:"1px solid rgba(148,163,184,.08)"}}>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:6}}>
                    <div style={{fontSize:9,color:"#8899AA",fontWeight:600,letterSpacing:0.5}}>{mc.label}</div>
                    {isVix&&vix&&vc && <div style={{fontSize:8,fontWeight:700,color:vc.text,background:vc.bg,padding:"2px 6px",borderRadius:4,letterSpacing:0.5,textTransform:"uppercase"}}>{vixLevel==="atencao"?"ATENÇÃO":vixLevel.toUpperCase()}</div>}
                  </div>
                  <div style={{fontSize:18,fontWeight:700,fontFamily:"monospace",color:mc.value==="--"?"#64748b":"#FFFFFF"}}>{mc.value}</div>
                  {mc.change!==null ? (
                    <div style={{fontSize:10,fontFamily:"monospace",color:isVix?(mc.change>0?"#DC3C3C":"#00C878"):(mc.change>=0?"#00C878":"#DC3C3C"),marginTop:4}}>
                      {mc.change>=0?"+":""}{mc.change.toFixed(2)}%
                    </div>
                  ) : is10Y&&ty ? (
                    <div style={{fontSize:10,fontFamily:"monospace",color:ty.direction==="subindo"?"#DC3C3C":ty.direction==="caindo"?"#00C878":"#8899AA",marginTop:4}}>
                      {ty.direction==="subindo"?"↑":ty.direction==="caindo"?"↓":"↔"} {ty.change_bps>=0?"+":""}{ty.change_bps} bps {ty.direction}
                    </div>
                  ) : mc.value==="--" ? (
                    <div style={{fontSize:9,color:"#64748b",marginTop:4,fontStyle:"italic"}}>Aguardando pipeline</div>
                  ) : null}
                  {isSP&&sp&&sp.change_week_pct!=null && (
                    <div style={{fontSize:9,fontFamily:"monospace",color:sp.change_week_pct>=0?"#00C878":"#DC3C3C",marginTop:2}}>
                      Semana: {sp.change_week_pct>=0?"+":""}{sp.change_week_pct.toFixed(2)}%
                    </div>
                  )}
                  {mc.date && (
                    <div style={{fontSize:8,marginTop:2,display:"flex",alignItems:"center",gap:4}}>
                      <span style={{color:"#64748b"}}>{mc.date}</span>
                      {isStale(mc.date) && (
                        <span style={{
                          background:"#92400e",color:"#fbbf24",fontSize:7,fontWeight:700,
                          padding:"1px 4px",borderRadius:3,animation:"stalePulse 2s infinite",letterSpacing:"0.3px"
                        }}>
                          {staleDays(mc.date)===1?"ONTEM":`${staleDays(mc.date)}d`}
                        </span>
                      )}
                    </div>
                  )}
                </div>
              );
            })}
          </div>

          {/* FedWatch sub-card */}
          {(()=>{
            const fw = fedwatch;
            if(!fw || fw.is_fallback) return (
              <div style={{background:"#0E1A24",borderRadius:8,padding:14,marginTop:12,border:"1px solid rgba(148,163,184,.08)"}}>
                <div style={{fontSize:9,fontWeight:600,color:"#8899AA",letterSpacing:0.5}}>FEDWATCH</div>
                <div style={{color:"#64748b",fontSize:10,fontStyle:"italic",marginTop:6}}>Aguardando pipeline (fedwatch.json)</div>
              </div>
            );
            const probs = fw.probabilities;
            const expMap:Record<string,{label:string;color:string}> = {hold:{label:"MANTER",color:"#DCB432"},cut:{label:"CORTE",color:"#00C878"},hike:{label:"ALTA",color:"#DC3C3C"}};
            const exp = expMap[fw.market_expectation]||expMap.hold;
            const bars:{label:string;value:number;color:string}[] = probs ? [
              {label:"Manter",value:probs.hold,color:"#DCB432"},
              {label:"Corte 25bp",value:probs.cut_25bps,color:"#00C878"},
              {label:"Alta 25bp",value:probs.hike_25bps,color:"#DC3C3C"},
            ] : [];
            const meetings = (fw.meetings_ahead||[]).slice(0,4);
            return (
              <div style={{background:"#0E1A24",borderRadius:8,padding:14,marginTop:12,border:"1px solid rgba(220,180,50,.12)"}}>
                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10}}>
                  <div style={{fontSize:11,fontWeight:700,color:"#DCB432"}}>FedWatch — Expectativa de Juros</div>
                  <div style={{fontSize:8,fontWeight:700,color:exp.color,background:`${exp.color}18`,padding:"2px 8px",borderRadius:4,letterSpacing:0.5}}>{exp.label}</div>
                </div>
                <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:10,marginBottom:12}}>
                  <div>
                    <div style={{fontSize:8,color:"#8899AA",marginBottom:2}}>Taxa Atual</div>
                    <div style={{fontSize:16,fontWeight:700,fontFamily:"monospace",color:"#FFFFFF"}}>{fw.current_rate?.toFixed(2)||"--"}%</div>
                    {fw.target_range && <div style={{fontSize:8,color:"#64748b"}}>{fw.target_range.lower}-{fw.target_range.upper}%</div>}
                  </div>
                  <div>
                    <div style={{fontSize:8,color:"#8899AA",marginBottom:2}}>Próximo FOMC</div>
                    <div style={{fontSize:14,fontWeight:700,color:"#FFFFFF"}}>{fw.next_meeting||"--"}</div>
                  </div>
                  <div>
                    <div style={{fontSize:8,color:"#8899AA",marginBottom:2}}>Expectativa</div>
                    <div style={{fontSize:14,fontWeight:800,color:exp.color}}>{exp.label}</div>
                    {probs && <div style={{fontSize:8,color:"#64748b"}}>{probs.hold?.toFixed(0)}% prob.</div>}
                  </div>
                </div>
                {bars.length>0 && (
                  <div style={{marginBottom:12}}>
                    <div style={{fontSize:9,fontWeight:600,color:"#8899AA",letterSpacing:0.5,marginBottom:6}}>PROBABILIDADES PRÓXIMO FOMC</div>
                    {bars.map(b=>(
                      <div key={b.label} style={{marginBottom:5}}>
                        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:2}}>
                          <span style={{fontSize:9,color:"#E8ECF1"}}>{b.label}</span>
                          <span style={{fontSize:10,fontFamily:"monospace",fontWeight:700,color:b.color}}>{b.value?.toFixed(1)}%</span>
                        </div>
                        <div style={{height:6,background:"rgba(148,163,184,.1)",borderRadius:3,overflow:"hidden"}}>
                          <div style={{height:"100%",width:`${Math.min(b.value||0,100)}%`,background:b.color,borderRadius:3,transition:"width .3s"}}/>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
                {meetings.length>1 && (
                  <div>
                    <div style={{fontSize:9,fontWeight:600,color:"#8899AA",letterSpacing:0.5,marginBottom:6}}>REUNIÕES SEGUINTES</div>
                    <div style={{display:"flex",gap:8,flexWrap:"wrap"}}>
                      {meetings.slice(1).map((m:any)=>{
                        const me = expMap[m.expected]||{label:m.expected,color:"#8899AA"};
                        return (
                          <div key={m.date} style={{background:"#142332",borderRadius:6,padding:"6px 10px",border:`1px solid ${me.color}22`,minWidth:90}}>
                            <div style={{fontSize:9,fontWeight:600,color:"#E8ECF1"}}>{m.date}</div>
                            <div style={{fontSize:8,color:me.color,fontWeight:700,marginTop:2}}>{me.label}{m.cut_prob!=null&&m.cut_prob>0?` (corte ${m.cut_prob.toFixed(0)}%)`:m.hike_prob!=null&&m.hike_prob>0?` (alta ${m.hike_prob.toFixed(0)}%)`:""}</div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
              </div>
            );
          })()}
        </div>

        {/* SEÇÃO 2: CLIMA */}
        <div style={panelStyle}>
          {sectionTitle("CLIMA — REGIÕES AGRÍCOLAS")}
          {!weatherData ? placeholder("weather_agro.json não disponível. Execute o pipeline para coletar dados climáticos.") : (
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(220px,1fr))",gap:12}}>
              {regionKeys.map(rk=>{
                const reg = weatherRegions[rk];
                const st = weatherStatus(reg);
                const cur = reg?.current;
                const alerts = reg?.alerts || [];
                return (
                  <div key={rk} style={{background:"#0E1A24",borderRadius:8,padding:14,border:`1px solid ${st.color}33`}}>
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:8}}>
                      <div style={{fontSize:11,fontWeight:700,color:"#FFFFFF"}}>{regionLabels[rk]||rk}</div>
                      <div style={{fontSize:9,fontWeight:700,color:st.color,background:`${st.color}18`,padding:"2px 8px",borderRadius:4,letterSpacing:0.5}}>{st.label.toUpperCase()}</div>
                    </div>
                    {cur ? (
                      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:6,fontSize:10,color:"#8899AA"}}>
                        <div>Temp: <span style={{color:"#FFFFFF"}}>{cur.temp_min?.toFixed(0)}° ~ {cur.temp_max?.toFixed(0)}°C</span></div>
                        <div>Chuva: <span style={{color:cur.precip_mm>20?"#3b82f6":"#FFFFFF"}}>{cur.precip_mm?.toFixed(0)}mm</span></div>
                        <div>Umidade: <span style={{color:"#FFFFFF"}}>{cur.humidity}%</span></div>
                        <div>Safras: <span style={{color:"#FFFFFF"}}>{reg?.crops?.join(", ")||"--"}</span></div>
                      </div>
                    ) : placeholder("Sem dados")}
                    {alerts.length>0 && (
                      <div style={{marginTop:8}}>
                        {alerts.map((a:any,i:number)=>(
                          <div key={i} style={{fontSize:9,color:a.severity==="ALTA"||a.severity==="CRITICA"?"#DC3C3C":"#DCB432",marginTop:4,padding:"4px 8px",background:"rgba(220,180,50,.08)",borderRadius:4}}>
                            {a.type}: {a.message}
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* SEÇÃO 3: SENTIMENTO DE MERCADO */}
        <div style={panelStyle}>
          {sectionTitle("SENTIMENTO DE MERCADO")}

          {/* Google Trends sub-card */}
          {(()=>{
            const gt = googleTrends;
            if(!gt || gt.is_fallback) return (
              <div style={{background:"#0E1A24",borderRadius:8,padding:14,marginBottom:14,border:"1px solid rgba(148,163,184,.08)"}}>
                <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:8}}>
                  <div style={{fontSize:11,fontWeight:700,color:"#DCB432"}}>Google Trends</div>
                </div>
                <div style={{color:"#64748b",fontSize:10,fontStyle:"italic"}}>Aguardando pipeline (google_trends.json)</div>
              </div>
            );
            const trends = gt.trends||{};
            const spikes = gt.spikes||[];
            const sorted = Object.entries(trends).sort((a:any,b:any)=>(b[1].current||0)-(a[1].current||0));
            const top5 = sorted.slice(0,5);
            return (
              <div style={{background:"#0E1A24",borderRadius:8,padding:14,marginBottom:14,border:"1px solid rgba(220,180,50,.12)"}}>
                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:10}}>
                  <div style={{fontSize:11,fontWeight:700,color:"#DCB432"}}>Google Trends (90 dias)</div>
                  {gt.generated_at && <div style={{fontSize:8,color:"#64748b"}}>{gt.generated_at.slice(0,10)}</div>}
                </div>
                {spikes.length>0 && (
                  <div style={{marginBottom:10,padding:"6px 10px",background:"rgba(220,60,60,.08)",borderRadius:6,border:"1px solid rgba(220,60,60,.15)"}}>
                    <div style={{fontSize:9,fontWeight:700,color:"#DC3C3C",letterSpacing:0.5,marginBottom:4}}>INTERESSE DETECTADO</div>
                    {spikes.map((term:string)=>{
                      const t=trends[term] as any;
                      const cur=t?.current||0;
                      const avg=t?.avg_30d||0;
                      const dir=cur>avg?"\u2191 ALTA":cur<avg?"\u2193 QUEDA":"\u2192 EST\u00C1VEL";
                      const col=cur>avg?"#DC3C3C":cur<avg?"#f97316":"#DCB432";
                      return <div key={term} style={{fontSize:11,color:col,marginTop:2,fontWeight:600}}>
                        {term} — {dir} ({cur} vs média {avg.toFixed(1)})
                      </div>;
                    })}
                  </div>
                )}
                <div style={{fontSize:9,fontWeight:600,color:"#8899AA",marginBottom:6,letterSpacing:0.5}}>TOP 5 POR INTERESSE</div>
                {top5.map(([term,d]:any)=>{
                  const isSpike = spikes.includes(term);
                  const barColor = isSpike?"#DC3C3C":d.direction==="subindo"?"#00C878":d.direction==="caindo"?"#DC3C3C":"#DCB432";
                  return (
                    <div key={term} style={{marginBottom:6}}>
                      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:2}}>
                        <div style={{fontSize:10,color:isSpike?"#DC3C3C":"#E8ECF1",fontWeight:isSpike?700:400}}>{term}</div>
                        <div style={{display:"flex",alignItems:"center",gap:6}}>
                          <span style={{fontSize:9,color:d.direction==="subindo"?"#00C878":d.direction==="caindo"?"#DC3C3C":"#8899AA"}}>{d.direction==="subindo"?"↑":d.direction==="caindo"?"↓":"↔"}</span>
                          <span style={{fontSize:10,fontFamily:"monospace",color:"#FFFFFF",fontWeight:600,minWidth:20,textAlign:"right"}}>{d.current}</span>
                        </div>
                      </div>
                      <div style={{height:4,background:"rgba(148,163,184,.1)",borderRadius:2,overflow:"hidden"}}>
                        <div style={{height:"100%",width:`${Math.min(d.current,100)}%`,background:barColor,borderRadius:2,transition:"width .3s"}}/>
                      </div>
                    </div>
                  );
                })}
                {gt.summary && <div style={{fontSize:9,color:"#8899AA",marginTop:8,fontStyle:"italic"}}>{gt.summary}</div>}
              </div>
            );
          })()}

          {/* Grok Tasks auto-feed */}
          {grokSentiment && (
            <div style={{background:"#0E1A24",borderRadius:8,padding:14,marginBottom:12,border:"1px solid rgba(0,200,120,.15)"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:6}}>
                <div style={{fontSize:10,fontWeight:700,color:"#00C878"}}>Grok Tasks (auto)</div>
                <div style={{fontSize:8,display:"flex",alignItems:"center",gap:4}}>
                  <span style={{color:"#64748b"}}>{grokSentiment.email_date||grokSentiment.generated_at?.slice(0,16)}</span>
                  {isStale(grokSentiment.generated_at?.slice(0,10)) && (
                    <span style={{background:"#92400e",color:"#fbbf24",fontSize:7,fontWeight:700,padding:"1px 4px",borderRadius:3,animation:"stalePulse 2s infinite"}}>
                      {staleDays(grokSentiment.generated_at?.slice(0,10))===1?"ONTEM":`${staleDays(grokSentiment.generated_at?.slice(0,10))}d`}
                    </span>
                  )}
                </div>
              </div>
              <div style={{fontSize:11,color:"#E8ECF1",lineHeight:1.7,whiteSpace:"pre-wrap"}}>{cleanGrokContent(grokSentiment.content)}</div>
            </div>
          )}

          {intelSentiment.text && (
            <div style={{background:"#0E1A24",borderRadius:8,padding:14,marginBottom:12,border:"1px solid rgba(220,180,50,.15)"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4}}>
                <div style={{fontSize:10,fontWeight:600,color:"#DCB432"}}>Colado manualmente</div>
                <div style={{fontSize:8,color:"#64748b"}}>{intelSentiment.time}</div>
              </div>
              <div style={{fontSize:11,color:"#E8ECF1",lineHeight:1.7,whiteSpace:"pre-wrap"}}>{intelSentiment.text}</div>
            </div>
          )}
          <textarea value={sentimentDraft} onChange={e=>setSentimentDraft(e.target.value)}
            placeholder="Cole aqui o resumo diário do Grok (fallback manual)..."
            style={{width:"100%",minHeight:80,background:"#0E1A24",color:"#E8ECF1",border:"1px solid rgba(148,163,184,.15)",borderRadius:6,padding:12,fontSize:11,lineHeight:1.6,resize:"vertical",fontFamily:"inherit"}} />
          <button onClick={()=>{
            if(!sentimentDraft.trim()) return;
            const entry = {text:sentimentDraft.trim(),time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})+" "+new Date().toLocaleDateString("pt-BR")};
            setIntelSentiment(entry); setSentimentDraft("");
            localStorage.setItem("agrimacro_intel_sentiment",JSON.stringify(entry));
          }} style={{marginTop:8,padding:"8px 20px",fontSize:10,fontWeight:600,background:"rgba(0,200,120,.12)",color:"#00C878",border:"1px solid rgba(0,200,120,.3)",borderRadius:6,cursor:"pointer"}}>
            Salvar Sentimento
          </button>
        </div>

        {/* SEÇÃO 4: NOTÍCIAS DO DIA */}
        <div style={panelStyle}>
          {sectionTitle("NOTÍCIAS DO DIA")}
          {/* Grok Tasks auto-feed */}
          {grokNews && (
            <div style={{background:"#0E1A24",borderRadius:8,padding:14,marginBottom:12,border:"1px solid rgba(0,200,120,.15)"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:6}}>
                <div style={{fontSize:10,fontWeight:700,color:"#00C878"}}>Grok Tasks (auto)</div>
                <div style={{fontSize:8,display:"flex",alignItems:"center",gap:4}}>
                  <span style={{color:"#64748b"}}>{grokNews.email_date||grokNews.generated_at?.slice(0,16)}</span>
                  {isStale(grokNews.generated_at?.slice(0,10)) && (
                    <span style={{background:"#92400e",color:"#fbbf24",fontSize:7,fontWeight:700,padding:"1px 4px",borderRadius:3,animation:"stalePulse 2s infinite"}}>
                      {staleDays(grokNews.generated_at?.slice(0,10))===1?"ONTEM":`${staleDays(grokNews.generated_at?.slice(0,10))}d`}
                    </span>
                  )}
                </div>
              </div>
              <div style={{fontSize:11,color:"#E8ECF1",lineHeight:1.7,whiteSpace:"pre-wrap"}}>{cleanGrokContent(grokNews.content)}</div>
            </div>
          )}
          {intelNews.text && (
            <div style={{background:"#0E1A24",borderRadius:8,padding:14,marginBottom:12,border:"1px solid rgba(220,180,50,.15)"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4}}>
                <div style={{fontSize:10,fontWeight:600,color:"#DCB432"}}>Colado manualmente</div>
                <div style={{fontSize:8,color:"#64748b"}}>{intelNews.time}</div>
              </div>
              <div style={{fontSize:11,color:"#E8ECF1",lineHeight:1.7,whiteSpace:"pre-wrap"}}>{intelNews.text}</div>
            </div>
          )}
          <textarea value={newsDraft} onChange={e=>setNewsDraft(e.target.value)}
            placeholder="Cole aqui notícias (fallback manual)..."
            style={{width:"100%",minHeight:80,background:"#0E1A24",color:"#E8ECF1",border:"1px solid rgba(148,163,184,.15)",borderRadius:6,padding:12,fontSize:11,lineHeight:1.6,resize:"vertical",fontFamily:"inherit"}} />
          <button onClick={()=>{
            if(!newsDraft.trim()) return;
            const entry = {text:newsDraft.trim(),time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})+" "+new Date().toLocaleDateString("pt-BR")};
            setIntelNews(entry); setNewsDraft("");
            localStorage.setItem("agrimacro_intel_news",JSON.stringify(entry));
          }} style={{marginTop:8,padding:"8px 20px",fontSize:10,fontWeight:600,background:"rgba(0,200,120,.12)",color:"#00C878",border:"1px solid rgba(0,200,120,.3)",borderRadius:6,cursor:"pointer"}}>
            Salvar Notícias
          </button>
        </div>

        {/* SEÇÃO 5: COUNCIL DIÁRIO — exibe resultado do "Aprofundar com IA" */}
        <div style={panelStyle}>
          {sectionTitle("COUNCIL DIÁRIO — SÍNTESE IA")}
          {intelCouncilLoading && (
            <div style={{textAlign:"center",padding:"16px 0"}}>
              <div style={{fontSize:11,color:"#DCB432",marginBottom:8}}>
                {councilStage || "Gerando sintese..."}
              </div>
              <div style={{width:280,height:4,background:"rgba(148,163,184,.1)",borderRadius:2,margin:"0 auto",overflow:"hidden"}}>
                <div style={{
                  height:"100%",background:"#DCB432",borderRadius:2,
                  transition:"width 1s ease",
                  width: councilStage.includes("Chairman") ? "85%"
                    : councilStage.includes("Diabo") ? "70%"
                    : councilStage.includes("Dalio") ? "55%"
                    : councilStage.includes("3/3") ? "45%"
                    : councilStage.includes("2/3") ? "30%"
                    : councilStage.includes("1/3") ? "20%"
                    : "5%",
                }}/>
              </div>
            </div>
          )}
          {intelCouncil && !intelCouncilLoading && (
            <div style={{background:"#0E1A24",borderRadius:8,padding:16,border:"1px solid rgba(220,180,50,.2)"}}>
              <div style={{fontSize:12,color:"#E8ECF1",lineHeight:1.8,whiteSpace:"pre-wrap"}}>{intelCouncil.text}</div>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginTop:10}}>
                <div style={{fontSize:9,color:"#64748b"}}>Gerado em {intelCouncil.time}</div>
                <button onClick={()=>setIntelCouncil(null)} style={{fontSize:9,color:"#DCB432",background:"none",border:"1px solid #DCB43244",borderRadius:4,padding:"2px 8px",cursor:"pointer"}}>Regenerar</button>
              </div>
            </div>
          )}
          {!intelCouncil && !intelCouncilLoading && (
            <div style={{textAlign:"center",padding:"16px 0",color:"#64748b",fontSize:11,fontStyle:"italic"}}>
              Use o botão "Aprofundar com IA" nas Cadeias Causais acima para gerar a síntese.
            </div>
          )}
          {councilHistory.length > 1 && (
            <div style={{marginTop:16}}>
              <div style={{fontSize:10,color:"#64748b",fontWeight:700,letterSpacing:"0.5px",marginBottom:8}}>
                ANÁLISES ANTERIORES ({councilHistory.length - 1})
              </div>
              {councilHistory.slice(1).map((item, i) => (
                <details key={i} style={{marginBottom:6,background:"#0E1A24",borderRadius:6,border:"1px solid #1e3a4a"}}>
                  <summary style={{padding:"8px 12px",cursor:"pointer",fontSize:10,color:"#64748b",listStyle:"none",display:"flex",justifyContent:"space-between"}}>
                    <span>Análise {councilHistory.length - 1 - i}</span>
                    <span>{item.time}</span>
                  </summary>
                  <div style={{padding:"8px 12px 12px",fontSize:11,color:"#94a3b8",lineHeight:1.6,borderTop:"1px solid #1e3a4a",whiteSpace:"pre-wrap",maxHeight:200,overflowY:"auto"}}>
                    {item.text}
                  </div>
                </details>
              ))}
            </div>
          )}
        </div>

        {/* SEÇÃO 6: ESTRATÉGIA ASSISTIDA */}
        {(()=>{
          const badgeColor = (tag:string):{bg:string;text:string;label:string} => {
            if(tag==="SUPORTA") return {bg:"rgba(0,200,120,.12)",text:"#00C878",label:"SUPORTA"};
            if(tag==="CONTRADIZ") return {bg:"rgba(220,60,60,.12)",text:"#DC3C3C",label:"CONTRADIZ"};
            return {bg:"rgba(220,180,50,.12)",text:"#DCB432",label:"NEUTRO"};
          };

          const renderStrategyResponse = (text:string) => {
            const sectionRegex = /^## (.+)$/gm;
            const parts:{title:string;content:string}[] = [];
            let match;
            const matches:{title:string;index:number}[] = [];
            while((match=sectionRegex.exec(text))!==null) {
              matches.push({title:match[1],index:match.index+match[0].length});
            }
            if(matches.length===0) {
              return <div style={{fontSize:12,color:"#E8ECF1",lineHeight:1.8,whiteSpace:"pre-wrap"}}>{text}</div>;
            }
            for(let i=0;i<matches.length;i++) {
              const end = i<matches.length-1 ? text.lastIndexOf("\n",matches[i+1].index-matches[i+1].title.length-4) : text.length;
              parts.push({title:matches[i].title,content:text.slice(matches[i].index,end).trim()});
            }
            // Preamble before first section
            const preamble = text.slice(0,matches[0].index-matches[0].title.length-4).trim();

            return (
              <div>
                {preamble && <div style={{fontSize:12,color:"#E8ECF1",lineHeight:1.8,whiteSpace:"pre-wrap",marginBottom:12}}>{preamble}</div>}
                {parts.map((sec,i)=>{
                  const key = `sec-${i}`;
                  const isCollapsed = strategyCollapsed[key];
                  const sectionIcon:Record<string,string> = {
                    "VALIDAÇÃO DA TESE":"✓","RISCOS IDENTIFICADOS":"⚠","DADOS QUE SUPORTAM":"▲",
                    "DADOS QUE CONTRADIZEM":"▼","CORRELAÇÕES RELEVANTES":"◈","SAZONALIDADE":"◐",
                    "SUGESTÕES DE AJUSTE":"◆","MONITORAMENTO CRÍTICO":"◉"
                  };
                  const icon = sectionIcon[sec.title]||"◆";
                  // Render content with inline badges
                  const renderContent = (raw:string) => {
                    const lines = raw.split("\n");
                    return lines.map((line,li)=>{
                      let badge = null;
                      let cleaned = line;
                      if(line.includes("[SUPORTA]")) { badge=badgeColor("SUPORTA"); cleaned=line.replace("[SUPORTA]","").trim(); }
                      else if(line.includes("[CONTRADIZ]")) { badge=badgeColor("CONTRADIZ"); cleaned=line.replace("[CONTRADIZ]","").trim(); }
                      else if(line.includes("[NEUTRO]")) { badge=badgeColor("NEUTRO"); cleaned=line.replace("[NEUTRO]","").trim(); }
                      // Highlight z-scores, percentis and numbers
                      const highlighted = cleaned.replace(/(z[=-]?\s*[+-]?\d+\.?\d*|percentil\s*[=:]?\s*\d+|[+-]?\d+\.?\d*%|\b\d+\.?\d*σ)/gi,(m)=>`⟨${m}⟩`);
                      const segments = highlighted.split(/⟨|⟩/);
                      return (
                        <div key={li} style={{display:"flex",alignItems:"flex-start",gap:6,marginBottom:3}}>
                          {badge && <span style={{fontSize:8,fontWeight:700,color:badge.text,background:badge.bg,padding:"1px 6px",borderRadius:3,whiteSpace:"nowrap",marginTop:2,flexShrink:0}}>{badge.label}</span>}
                          <span style={{fontSize:11,color:"#E8ECF1",lineHeight:1.7}}>
                            {segments.map((seg,si)=>{
                              if(si%2===1) return <strong key={si} style={{color:"#DCB432",fontFamily:"monospace"}}>{seg}</strong>;
                              return <span key={si}>{seg}</span>;
                            })}
                          </span>
                        </div>
                      );
                    });
                  };

                  return (
                    <div key={key} style={{marginBottom:8,background:"#0E1A24",borderRadius:8,border:"1px solid rgba(148,163,184,.08)",overflow:"hidden"}}>
                      <div onClick={()=>setStrategyCollapsed(prev=>({...prev,[key]:!prev[key]}))}
                        style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"10px 14px",cursor:"pointer",
                          background:isCollapsed?"transparent":"rgba(220,180,50,.04)"}}>
                        <div style={{display:"flex",alignItems:"center",gap:8}}>
                          <span style={{fontSize:12}}>{icon}</span>
                          <span style={{fontSize:11,fontWeight:700,color:"#DCB432",letterSpacing:0.3}}>{sec.title}</span>
                        </div>
                        <span style={{fontSize:10,color:"#64748b",transition:"transform .2s",transform:isCollapsed?"rotate(-90deg)":"rotate(0deg)"}}>▼</span>
                      </div>
                      {!isCollapsed && (
                        <div style={{padding:"8px 14px 14px"}}>{renderContent(sec.content)}</div>
                      )}
                    </div>
                  );
                })}
              </div>
            );
          };

          const handleAnalyze = async (input:string) => {
            if(!input.trim()||strategyLoading) return;
            setStrategyLoading(true);
            setStrategyCollapsed({});
            try {
              const res = await fetch("/api/strategy",{
                method:"POST",headers:{"Content-Type":"application/json"},
                body:JSON.stringify({thesis:input.trim(), history:strategyConvoHistory})
              });
              const d = await res.json();
              if(res.ok && d.response) {
                const entry = {text:d.response,thesis:input.trim(),time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})+" "+new Date().toLocaleDateString("pt-BR")};
                setStrategyResult(entry);
                // Update conversation history for follow-ups
                setStrategyConvoHistory(prev=>[...prev,{role:"user",content:input.trim()},{role:"assistant",content:d.response}]);
                // Save to history (max 3)
                const newHist = [entry,...strategyHistory].slice(0,3);
                setStrategyHistory(newHist);
                localStorage.setItem("agrimacro_strategy_history",JSON.stringify(newHist));
              } else {
                setStrategyResult({text:"ERRO: "+(d.error||`API ${res.status}`),thesis:input.trim(),time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})});
              }
            } catch(e:any) {
              setStrategyResult({text:"ERRO: "+(e.message||"Falha"),thesis:input.trim(),time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})});
            }
            setStrategyLoading(false);
            setStrategyRefineMode(false);
            setStrategyInput("");
          };

          return (
            <div style={{...panelStyle,border:"1px solid rgba(220,180,50,.2)",background:"linear-gradient(180deg,#142332 0%,#0E1A24 100%)"}}>
              {sectionTitle("ESTRATÉGIA ASSISTIDA")}
              <div style={{fontSize:10,color:"#8899AA",marginBottom:16,marginTop:-8}}>
                Descreva sua tese e receba análise estruturada com todos os dados do pipeline
              </div>

              {/* Snapshot buttons — always visible */}
              {(()=>{
                const formatMarketData = (d:any):string[] => {
                  const L:string[]=[];
                  // Prices
                  if(d.prices){L.push("\n== PRECOS ==");for(const [sym,arr] of Object.entries(d.prices)){const a=arr as any[];if(!Array.isArray(a)||!a.length)continue;const last=a[a.length-1];const prev=a.length>1?a[a.length-2]:null;const chg=prev?((last.close-prev.close)/prev.close*100).toFixed(2):"?";L.push(`${sym}: ${last.close} (${Number(chg)>=0?"+":""}${chg}%) [${last.date}]`);}}
                  // Macro
                  if(d.macro_indicators){const mi=d.macro_indicators;L.push("\n== MACRO ==");if(mi.vix)L.push(`VIX: ${mi.vix.value} (${mi.vix.level}, ${mi.vix.change_pct>0?"+":""}${mi.vix.change_pct}%)`);if(mi.sp500)L.push(`S&P500: ${mi.sp500.price} (${mi.sp500.change_pct>0?"+":""}${mi.sp500.change_pct}%)`);if(mi.treasury_10y)L.push(`10Y: ${mi.treasury_10y.yield_pct}% (${mi.treasury_10y.direction}, ${mi.treasury_10y.change_bps>0?"+":""}${mi.treasury_10y.change_bps}bps)`);}
                  if(d.bcb){const brl=d.bcb.brl_usd?.slice(-1)[0];if(brl)L.push(`BRL/USD: ${brl.value} [${brl.date}]`);const sel=d.bcb.selic_meta?.slice(-1)[0];if(sel)L.push(`Selic: ${sel.value}%`);}
                  if(d.fedwatch?.probabilities){const fw=d.fedwatch;L.push(`Fed: ${fw.market_expectation} (hold ${fw.probabilities.hold}%, corte ${fw.probabilities.cut_25bps}%) proximo ${fw.next_meeting}`);}
                  // COT enriched
                  if(d.cot?.commodities){L.push("\n== COT ==");for(const [sym,val] of Object.entries(d.cot.commodities)){const v=val as any;const leg=v?.legacy?.latest;if(!leg)continue;const hist=v?.legacy?.history||[];let cotIdx="?";if(hist.length>=20){const nets=hist.map((h:any)=>h.noncomm_net).filter((n:any)=>n!=null);if(nets.length>=20){const mn=Math.min(...nets),mx=Math.max(...nets);if(mx!==mn)cotIdx=((leg.noncomm_net-mn)/(mx-mn)*100).toFixed(0);}}const prev=hist.length>=2?hist[hist.length-2]:null;const delta=prev?leg.noncomm_net-prev.noncomm_net:null;L.push(`${sym}: Fundos ${leg.noncomm_net>0?"+":""}${leg.noncomm_net} | COT Index ${cotIdx}/100 | OI ${leg.open_interest}${delta!=null?" | dsem "+(delta>0?"+":"")+delta:""}`);}
                  } else if(d.cot){L.push("\n== COT ==");for(const [sym,val] of Object.entries(d.cot)){const v=val as any;if(!v?.managed_money&&!v?.legacy)continue;const mm=v.managed_money||v.legacy||{};L.push(`${sym}: net=${mm.net_position??mm.net??"?"}`);}}
                  // Spreads
                  if(d.spreads?.spreads){L.push("\n== SPREADS ==");for(const [k,v] of Object.entries(d.spreads.spreads)){const s=v as any;L.push(`${s.name||k}: ${s.current?.toFixed(4)} ${s.unit||""} z=${s.zscore_1y?.toFixed(2)} ${s.regime} ${s.trend||""}`);}}
                  // Composite signals
                  if(d.correlations?.composite_signals){L.push("\n== COMPOSITE SIGNALS ==");for(const c of d.correlations.composite_signals){L.push(`${(c as any).asset}: ${(c as any).signal} (conf=${((c as any).confidence*100).toFixed(0)}%, ${(c as any).sources_count} fontes)`);}}
                  // Synthesis
                  if(d.intel_synthesis&&!d.intel_synthesis.is_fallback){const syn=d.intel_synthesis;if(syn.summary){L.push("\n== SINTESE ==");L.push(syn.summary);}if(syn.priority_high?.length){L.push("Criticos:");syn.priority_high.forEach((s:any)=>L.push(`  - ${s.title}${s.detail?" ("+s.detail+")":""}`));}}
                  // Grain ratios
                  if(d.grain_ratios?.scorecards){L.push("\n== GRAIN RATIOS ==");const gr=d.grain_ratios;for(const [g,sym] of Object.entries({corn:"ZC",soy:"ZS",wheat:"ZW"} as Record<string,string>)){const sc=(gr.scorecards as any)[g];if(!sc)continue;L.push(`${sym}: ${sc.composite_signal} (score=${sc.composite_score?.toFixed(0)})`);}}
                  // PSD ending stocks enriched
                  if(d.psd_ending_stocks?.commodities){L.push("\n== ESTOQUES USDA ==");for(const [k,v] of Object.entries(d.psd_ending_stocks.commodities)){const s=v as any;if(!s?.current)continue;const dev=s.deviation!=null?s.deviation.toFixed(1)+"%":"?";const cls=s.deviation!=null?(s.deviation>15?"ACIMA avg":(s.deviation<-15?"ABAIXO avg":"NORMAL")):"";L.push(`${k}: ${s.current} ${s.unit||""} | Media 5A ${s.avg_5y||"?"} | ${dev} | ${cls}`);}}
                  // Physical BR
                  try{const pbr=d.physical_br?.products||d.physical_br;if(pbr&&typeof pbr==="object"){const entries=Object.entries(pbr).filter(([,v])=>(v as any)?.price);if(entries.length){L.push("\n== FISICO BR ==");for(const [k,v] of entries){const p=v as any;L.push(`${p.label||k}: R$${p.price} ${p.unit||""} ${p.change_pct!=null?((p.change_pct>=0?"+":"")+p.change_pct+"% d/d"):(p.trend||"")} | ${p.source||""}`);};}}}catch{}
                  // Physical intl
                  try{const pi=d.physical_intl?.international||d.physical_intl;if(pi&&typeof pi==="object"){const entries=Object.entries(pi).filter(([,v])=>(v as any)?.price);if(entries.length){L.push("\n== FISICO INTERNACIONAL ==");for(const [k,v] of entries){const p=v as any;L.push(`${p.label||k}: ${p.price_unit?.includes("R$")?"R$":"US$"}${p.price} ${p.price_unit||""} ${p.trend||""}`);};}}}catch{}
                  // EIA energy
                  try{const eia=d.eia?.series||d.eia_data?.series;if(eia){L.push("\n== ENERGIA (EIA) ==");for(const k of ["wti_spot","natural_gas_spot","crude_stocks","ethanol_production","diesel_retail"]){const s=eia[k];if(!s?.latest_value)continue;const wow=s.wow_change_pct!=null?` (${s.wow_change_pct>0?"+":""}${s.wow_change_pct.toFixed(1)}% sem)`:"";L.push(`${s.name||k}: ${s.latest_value} ${s.unit||""}${wow} [${s.latest_period||""}]`);}}}catch{}
                  // Seasonality
                  try{const sn=d.seasonality;if(sn){const now=new Date();const curMonth=now.getMonth();const monthNames=["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"];const items:string[]=[];for(const sym of ["ZS","ZC","ZL","GF"]){const s=sn[sym];if(!s?.series)continue;const years=Object.keys(s.series).filter((k:string)=>k.match(/^\d{4}$/));if(years.length<3)continue;let ups=0,total=0;for(const yr of years){const pts=s.series[yr]as any[];if(!pts||pts.length<2)continue;const mPts=pts.filter((p:any)=>{const d2=new Date(p.date);return d2.getMonth()===curMonth;});if(mPts.length>=2){total++;if(mPts[mPts.length-1].close>mPts[0].close)ups++;}}if(total>0)items.push(`${sym}: ${monthNames[curMonth]} positivo em ${((ups/total)*100).toFixed(0)}% dos ultimos ${total} anos`);}if(items.length){L.push("\n== SAZONALIDADE ==");items.forEach(i=>L.push(i));}}}catch{}
                  // CONAB
                  try{const cn=d.conab;if(cn?.boletim_info?.principais_culturas){const bi=cn.boletim_info;const cult=bi.principais_culturas;L.push(`\n== CONAB (${bi.safra||""} ${bi.levantamento||""}o lev.) ==`);if(bi.producao_total_mt)L.push(`Producao total BR: ${bi.producao_total_mt} mi t`);for(const [k,v] of Object.entries(cult)){const c=v as any;if(c?.producao_mt)L.push(`${k}: ${c.producao_mt} mi t | area ${c.area_mha||"?"} mi ha | prod. ${c.produtividade_kg_ha||"?"} kg/ha`);}}}catch{}
                  // Futures curve
                  try{const fc=d.futures_contracts?.commodities;if(fc){const items:string[]=[];for(const sym of ["ZS","ZC","ZL","ZW","CL","GF"]){const c=fc[sym];if(!c?.contracts?.length)continue;const cts=(c.contracts as any[]).slice(0,3);if(cts.length<2)continue;const f1=cts[0],f2=cts[1];const spread=((f2.close-f1.close)/f1.close*100).toFixed(2);const structure=f1.close>f2.close?"BACKWARDATION":"CONTANGO";const parts=cts.map((ct:any)=>`${ct.expiry_label||ct.contract}: ${ct.close}`).join(" | ");items.push(`${sym}: ${parts} [${structure} ${spread}%]`);}if(items.length){L.push("\n== CURVA FUTURA ==");items.forEach(i=>L.push(i));}}}catch{}
                  // News top 5
                  try{const nw=d.news?.news;if(nw?.length){L.push("\n== NOTICIAS ==");for(const n of nw.slice(0,5)){L.push(`- ${(n as any).title} [${(n as any).source}]`);}}}catch{}
                  // Weather
                  if(d.weather){L.push("\n== CLIMA ==");if(d.weather.enso)L.push(`ENSO: ${d.weather.enso.status} (ONI=${d.weather.enso.oni_value})`);if(d.weather.regions){for(const [rk,rv] of Object.entries(d.weather.regions)){const r=rv as any;const alerts=(r.alerts||[]).map((a:any)=>`${a.type} ${a.severity}`).join(", ");if(alerts)L.push(`${r.label||rk}: ${alerts}`);}}}
                  // Grok
                  for(const [key,label] of [["grok_sentiment","SENTIMENTO GROK"],["grok_news","NOTICIAS GROK"],["grok_macro","MACRO GROK"]]){const g=d[key];if(g&&!g.is_fallback&&g.content){L.push(`\n== ${label} ==`);L.push(g.content.slice(0,800));}}
                  return L;
                };
                return (
                  <div style={{display:"flex",gap:8,marginBottom:16}}>
                    <button id="snap-public-btn" onClick={async()=>{
                      const btn=document.getElementById("snap-public-btn");
                      if(btn) btn.textContent="Carregando...";
                      try{
                        const res=await fetch("/api/snapshot/public",{headers:{"X-Snapshot-Token":"agrimacro2026"}});
                        if(!res.ok) throw new Error(`HTTP ${res.status}`);
                        const snap=await res.json();const d=snap.data||{};
                        const lines:string[]=[];
                        const ts=new Date().toLocaleString("pt-BR",{day:"2-digit",month:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit"});
                        lines.push(`SNAPSHOT AGRIMACRO ${ts}`);lines.push("=".repeat(50));
                        lines.push(...formatMarketData(d));
                        await navigator.clipboard.writeText(lines.join("\n"));
                        if(btn){btn.innerHTML="\u{1F4CB} Copiado!";setTimeout(()=>{btn.innerHTML="\u{1F4CB} Copiar Snapshot";},2000);}
                      }catch(e:any){if(btn){btn.innerHTML="\u{1F4CB} Erro!";setTimeout(()=>{btn.innerHTML="\u{1F4CB} Copiar Snapshot";},2000);}}
                    }} style={{padding:"8px 16px",fontSize:10,fontWeight:600,borderRadius:6,cursor:"pointer",
                      background:"rgba(124,58,237,.10)",color:"#7C3AED",border:"1px solid rgba(124,58,237,.25)",transition:"all .2s"}}>
                      {"\u{1F4CB}"} Copiar Snapshot
                    </button>
                    <button id="snap-private-btn" onClick={async()=>{
                      const btn=document.getElementById("snap-private-btn");
                      if(btn) btn.textContent="Carregando...";
                      try{
                        const res=await fetch("/api/snapshot/private",{headers:{"X-Snapshot-Token":"agrimacro_private_2026_secure"}});
                        if(!res.ok) throw new Error(`HTTP ${res.status}`);
                        const snap=await res.json();const d=snap.data||{};
                        const lines:string[]=[];
                        const ts=new Date().toLocaleString("pt-BR",{day:"2-digit",month:"2-digit",year:"numeric",hour:"2-digit",minute:"2-digit"});
                        lines.push(`SNAPSHOT AGRIMACRO (COM PORTFOLIO) ${ts}`);lines.push("=".repeat(50));
                        const pm=d.portfolio_masked;
                        if(pm){
                          lines.push("\n== PORTFOLIO (mascarado) ==");
                          if(pm.summary_ranges){for(const [k,v] of Object.entries(pm.summary_ranges)){lines.push(`${k}: ${v}`);}}
                          if(pm.positions){lines.push(`Posicoes (${pm.positions.length}):`);for(const p of pm.positions as any[]){lines.push(`  ${p.symbol} ${p.local_symbol}: ${p.direction} ${p.quantity}x | MV: ${p.market_value_range}${p.pnl_pct!=null?" | PnL: "+p.pnl_pct+"%":""}`);}};
                        }
                        lines.push(...formatMarketData(d));
                        await navigator.clipboard.writeText(lines.join("\n"));
                        if(btn){btn.innerHTML="\u{1F512} Copiado!";setTimeout(()=>{btn.innerHTML="\u{1F512} Copiar com Portfolio";},2000);}
                      }catch(e:any){if(btn){btn.innerHTML="\u{1F512} Erro!";setTimeout(()=>{btn.innerHTML="\u{1F512} Copiar com Portfolio";},2000);}}
                    }} style={{padding:"8px 16px",fontSize:10,fontWeight:600,borderRadius:6,cursor:"pointer",
                      background:"rgba(91,33,182,.10)",color:"#5B21B6",border:"1px solid rgba(91,33,182,.25)",transition:"all .2s"}}>
                      {"\u{1F512}"} Copiar com Portfólio
                    </button>
                  </div>
                );
              })()}

              {/* Input area */}
              {(!strategyResult || strategyRefineMode) && (
                <div>
                  <textarea
                    value={strategyInput}
                    onChange={e=>setStrategyInput(e.target.value)}
                    placeholder={"Descreva sua tese, estratégia ou pergunta de mercado...\nEx: 'Estou short ZL via puts. ZS bearish com estoques altos. Como ficam minhas correlações com GF e CL dado cenário de estagflação e trégua Irã?'"}
                    onKeyDown={e=>{if(e.key==="Enter"&&e.ctrlKey){e.preventDefault();handleAnalyze(strategyInput);}}}
                    style={{
                      width:"100%",minHeight:strategyRefineMode?60:100,background:"#0E1A24",color:"#E8ECF1",
                      border:"1px solid rgba(220,180,50,.2)",borderRadius:8,padding:14,fontSize:12,
                      lineHeight:1.7,resize:"vertical",fontFamily:"inherit",
                      transition:"border-color .2s",
                    }}
                  />
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginTop:10}}>
                    <div style={{fontSize:9,color:"#64748b"}}>Ctrl+Enter para enviar • Acessa todos os JSONs do pipeline</div>
                    <div style={{display:"flex",gap:8}}>
                      {strategyRefineMode && (
                        <button onClick={()=>{setStrategyRefineMode(false);setStrategyInput("");}}
                          style={{padding:"8px 16px",fontSize:10,fontWeight:600,borderRadius:6,cursor:"pointer",
                            background:"transparent",color:"#8899AA",border:"1px solid rgba(148,163,184,.2)"}}>
                          Cancelar
                        </button>
                      )}
                      <button onClick={()=>handleAnalyze(strategyInput)} disabled={strategyLoading||!strategyInput.trim()}
                        style={{
                          padding:"10px 28px",fontSize:11,fontWeight:700,borderRadius:6,
                          cursor:strategyLoading||!strategyInput.trim()?"not-allowed":"pointer",
                          background:strategyLoading?"#1E3044":!strategyInput.trim()?"#1E3044":"rgba(220,180,50,.12)",
                          color:strategyLoading||!strategyInput.trim()?"#64748b":"#DCB432",
                          border:"1px solid "+(strategyLoading||!strategyInput.trim()?"rgba(148,163,184,.15)":"#DCB43266"),
                          transition:"all .2s",letterSpacing:0.5,
                        }}>
                        {strategyLoading?"Analisando...":strategyRefineMode?"Refinar Análise":"Analisar Estratégia"}
                      </button>
                    </div>
                  </div>
                </div>
              )}

              {/* Loading animation */}
              {strategyLoading && (
                <div style={{textAlign:"center",padding:"24px 0"}}>
                  <div style={{fontSize:11,color:"#DCB432",marginBottom:8}}>Cruzando dados de mercado, COT, spreads, clima e macro...</div>
                  <div style={{width:200,height:3,background:"rgba(148,163,184,.1)",borderRadius:2,margin:"0 auto",overflow:"hidden"}}>
                    <div style={{width:"40%",height:"100%",background:"#DCB432",borderRadius:2,animation:"pulse 1.5s ease-in-out infinite"}}/>
                  </div>
                  <style>{`@keyframes pulse{0%,100%{transform:translateX(-100%);opacity:.5}50%{transform:translateX(150%);opacity:1}}`}</style>
                </div>
              )}

              {/* Result */}
              {strategyResult && !strategyLoading && (
                <div style={{marginTop:strategyRefineMode?0:4}}>
                  {/* Thesis reminder */}
                  <div style={{background:"rgba(220,180,50,.06)",borderRadius:6,padding:"8px 12px",marginBottom:12,
                    borderLeft:"3px solid #DCB432"}}>
                    <div style={{fontSize:9,fontWeight:600,color:"#8899AA",letterSpacing:0.5,marginBottom:2}}>TESE ANALISADA</div>
                    <div style={{fontSize:11,color:"#E8ECF1",lineHeight:1.5}}>{strategyResult.thesis}</div>
                  </div>

                  {/* Parsed response with collapsible sections */}
                  {renderStrategyResponse(strategyResult.text)}

                  {/* Action bar */}
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginTop:14,paddingTop:12,
                    borderTop:"1px solid rgba(148,163,184,.08)"}}>
                    <div style={{fontSize:9,color:"#64748b"}}>{strategyResult.time}</div>
                    <div style={{display:"flex",gap:8}}>
                      <button onClick={()=>{setStrategyRefineMode(true);setStrategyInput("");}}
                        style={{padding:"6px 14px",fontSize:9,fontWeight:600,borderRadius:4,cursor:"pointer",
                          background:"rgba(220,180,50,.08)",color:"#DCB432",border:"1px solid #DCB43233"}}>
                        Refinar análise
                      </button>
                      <button onClick={()=>{
                        const blob = new Blob(["ESTRATÉGIA ASSISTIDA — AgriMacro\n"+strategyResult.time+"\n\nTESE: "+strategyResult.thesis+"\n\n"+strategyResult.text],{type:"text/plain"});
                        const url = URL.createObjectURL(blob);
                        const a = document.createElement("a");
                        a.href=url;a.download=`estrategia_${new Date().toISOString().slice(0,10)}.txt`;
                        a.click();URL.revokeObjectURL(url);
                      }} style={{padding:"6px 14px",fontSize:9,fontWeight:600,borderRadius:4,cursor:"pointer",
                        background:"rgba(59,130,246,.08)",color:"#3b82f6",border:"1px solid rgba(59,130,246,.2)"}}>
                        Exportar TXT
                      </button>
                      <button onClick={()=>{setStrategyResult(null);setStrategyRefineMode(false);setStrategyConvoHistory([]);setStrategyCollapsed({});}}
                        style={{padding:"6px 14px",fontSize:9,fontWeight:600,borderRadius:4,cursor:"pointer",
                          background:"rgba(220,60,60,.08)",color:"#DC3C3C",border:"1px solid rgba(220,60,60,.2)"}}>
                        Nova análise
                      </button>
                    </div>
                  </div>
                </div>
              )}

              {/* History */}
              {strategyHistory.length>0 && !strategyResult && (
                <div style={{marginTop:16}}>
                  <div style={{fontSize:9,fontWeight:600,color:"#8899AA",letterSpacing:0.5,marginBottom:8}}>ANÁLISES RECENTES</div>
                  {strategyHistory.map((h,i)=>(
                    <div key={i} onClick={()=>{setStrategyResult(h);setStrategyCollapsed({});}}
                      style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"8px 12px",
                        background:"#0E1A24",borderRadius:6,marginBottom:4,cursor:"pointer",
                        border:"1px solid rgba(148,163,184,.06)",transition:"border-color .15s"}}
                      onMouseEnter={e=>(e.currentTarget.style.borderColor="rgba(220,180,50,.2)")}
                      onMouseLeave={e=>(e.currentTarget.style.borderColor="rgba(148,163,184,.06)")}>
                      <div style={{fontSize:10,color:"#E8ECF1",flex:1,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",marginRight:12}}>
                        {h.thesis.slice(0,80)}{h.thesis.length>80?"...":""}
                      </div>
                      <div style={{fontSize:8,color:"#64748b",whiteSpace:"nowrap"}}>{h.time}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          );
        })()}
      </div>
    );
  };

  const renderParitiesTab = () => {
    const data = paritiesData;
    if (!data?.parities) return <div style={{color:"#64748b",fontSize:11,fontStyle:"italic",padding:16}}>Carregando paridades...</div>;

    const categories: Record<string, any[]> = {};
    Object.values(data.parities).forEach((p: any) => {
      const cat = p.category || 'outros';
      if (!categories[cat]) categories[cat] = [];
      categories[cat].push(p);
    });

    const catLabels: Record<string, string> = {
      acreagem: '\u{1F33E} Acreagem & Plantio',
      crush: '\u{1F527} Crush & Processamento',
      energia: '\u26A1 Energia & Biodiesel',
      macro: '\u{1F30D} Macro & C\u00e2mbio',
      pecuaria: '\u{1F404} Pecu\u00e1ria',
      softs: '\u{1F36C} Softs Brasileiros',
      insumos: '\u{1F9EA} Insumos & Fertilizantes',
    };
    const catOrder = ['acreagem','crush','energia','macro','pecuaria','softs','insumos'];

    return (
      <div style={{padding:16}}>
        <div style={{
          fontSize:11, color:'#64748b', marginBottom:16,
          borderLeft:'3px solid #DCB432', paddingLeft:8
        }}>
          Paridades e correla\u00e7\u00f5es que definem decis\u00f5es de
          plantio, produ\u00e7\u00e3o e exporta\u00e7\u00e3o. Atualizado diariamente.
        </div>

        {[...catOrder, ...Object.keys(categories).filter(c => !catOrder.includes(c))].filter(cat => categories[cat]?.length).map(cat => { const items = categories[cat]; return (
          <div key={cat} style={{marginBottom:24}}>
            <div style={{
              fontSize:11, fontWeight:700, color:'#94a3b8',
              letterSpacing:'0.1em', marginBottom:12,
              textTransform:'uppercase'
            }}>
              {catLabels[cat] || cat}
            </div>

            <div style={{
              display:'grid',
              gridTemplateColumns:'repeat(auto-fill, minmax(320px, 1fr))',
              gap:12
            }}>
              {items.map((par: any) => {
                const z = par.z_score;
                const zColor = z == null ? '#64748b'
                  : Math.abs(z) > 2 ? '#DC3C3C'
                  : Math.abs(z) > 1 ? '#DCB432'
                  : '#64748b';

                return (
                  <div key={par.name} style={{
                    background:'#0E1A24',
                    border:'1px solid #1e3a4a',
                    borderLeft:`3px solid ${par.signal_color || '#64748b'}`,
                    borderRadius:8, padding:14
                  }}>
                    <div style={{
                      fontSize:11, fontWeight:700,
                      color:'#e2e8f0', marginBottom:4
                    }}>
                      {par.name}
                    </div>
                    <div style={{
                      fontSize:9, color:'#64748b', marginBottom:10
                    }}>
                      {par.description}
                    </div>

                    <div style={{
                      display:'flex', alignItems:'baseline',
                      gap:8, marginBottom:8
                    }}>
                      <span style={{
                        fontSize:22, fontWeight:700,
                        fontFamily:'monospace',
                        color: par.signal_color || '#e2e8f0'
                      }}>
                        {typeof par.value === 'number'
                          ? par.value.toFixed(
                              par.unit === '%' ? 1
                              : par.unit === 'ratio' ? 4
                              : par.unit === 'correla\u00e7\u00e3o' ? 3
                              : 2
                            )
                          : par.value}
                      </span>
                      <span style={{fontSize:10, color:'#64748b'}}>
                        {par.unit}
                      </span>
                      {z != null && (
                        <span style={{
                          fontSize:10, color: zColor,
                          fontFamily:'monospace'
                        }}>
                          z={z > 0 ? '+' : ''}{z}
                        </span>
                      )}
                    </div>

                    {z != null && (
                      <div style={{
                        height:3, background:'#1e3a4a',
                        borderRadius:2, marginBottom:8
                      }}>
                        <div style={{
                          height:'100%', borderRadius:2,
                          background: zColor,
                          width: `${Math.min(Math.abs(z)/3*100, 100)}%`,
                          marginLeft: z < 0
                            ? `${Math.max(50 - Math.abs(z)/3*50, 0)}%`
                            : '50%'
                        }} />
                      </div>
                    )}

                    <div style={{
                      fontSize:10, fontWeight:600,
                      color: par.signal_color || '#64748b'
                    }}>
                      {par.signal}
                    </div>

                    {par.threshold_low != null && par.threshold_high != null && (() => {
                      const range = par.threshold_high - par.threshold_low;
                      const pos = range > 0 ? Math.min(Math.max((par.value - par.threshold_low) / range * 100, 0), 100) : 50;
                      return (
                        <div style={{marginTop:4}}>
                          <div style={{fontSize:8,color:'#475569',marginBottom:2,display:'flex',justifyContent:'space-between'}}>
                            <span>{par.threshold_low}</span>
                            <span style={{color:'#64748b'}}>range operacional</span>
                            <span>{par.threshold_high}</span>
                          </div>
                          <div style={{height:2,background:'#1e3a4a',borderRadius:1,position:'relative'}}>
                            <div style={{position:'absolute',left:`${pos}%`,top:-2,width:4,height:6,background:par.signal_color||'#DCB432',borderRadius:1,transform:'translateX(-50%)'}}/>
                          </div>
                        </div>
                      );
                    })()}

                    {(par.trend_7d !== undefined) && (
                      <div style={{
                        fontSize:9, color:'#64748b', marginTop:6,
                        display:'flex', gap:12
                      }}>
                        <span>7d: {par.trend_7d > 0 ? '\u2191' : '\u2193'}
                          {Math.abs(par.trend_7d).toFixed(1)}%
                        </span>
                        {par.trend_30d !== undefined && (
                          <span>30d: {par.trend_30d > 0 ? '\u2191' : '\u2193'}
                            {Math.abs(par.trend_30d).toFixed(1)}%
                          </span>
                        )}
                      </div>
                    )}

                    {par.impact_date && (
                      <div style={{
                        fontSize:9, color:'#DCB432', marginTop:6
                      }}>
                        {"\u23F1"} Impacto estimado em: {par.impact_date}
                      </div>
                    )}

                    {par.contracts && (
                      <div style={{
                        fontSize:8, color:'#64748b',
                        marginTop:4, fontFamily:'monospace'
                      }}>
                        Contratos: {
                          typeof par.contracts === 'string'
                            ? par.contracts
                            : Object.values(par.contracts).join(' / ')
                        }
                      </div>
                    )}

                    {par.lag_note && (
                      <div style={{
                        fontSize:9, color:'#475569',
                        marginTop:3, borderLeft:'2px solid #1e3a4a',
                        paddingLeft:6
                      }}>
                        {"\u23F1"} {par.lag_note}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        ); })}

        <div style={{fontSize:9, color:'#475569', marginTop:16}}>
          Atualizado: {data.generated_at?.slice(0,19).replace('T',' ')}
          {' | '}
          {data.count} paridades ativas
        </div>
      </div>
    );
  };

  const renderTab = () => {
    switch(tab) {
      case "Visão Geral": return <CommodityTab selected={selected} prices={prices} psdData={psdData} cot={cot} season={season} spreads={spreads} physicalBr={physicalBrData} parities={paritiesData} stocks={stocks} />;
      case "Gráfico + COT": return renderGraficoCOT();
      case "Comparativo": return renderComparativo();
      case "Spreads": return renderSpreads();
      case "Sazonalidade": return renderSazonalidade();
      case "Stocks Watch": return renderStocksWatch();
      case "Energia": return renderEnergia();
      case "Custo Produção": return <CostOfProductionTab />;
      case "Físico Intl": return renderFisicoIntl();
      case "Leitura do Dia": return renderLeituraDoDia();
      case "Portfolio": return <PortfolioPage portfolio={portfolio} greeks={greeksData} optionsChain={optionsChain} prices={prices} />;
      case "Bilateral": return <BilateralPanel />;
      case "Grain Ratios": return <GrainRatiosTab />;
      case "Livestock Risk": return <LivestockRiskTab />;
      case "Paridades": return renderParitiesTab();
      default: return null;
    }
  };
  // -- Main Return --------------------------------------------------------
  return (
    <div style={{display:"flex",minHeight:"100vh",background:C.bg,color:C.text,fontFamily:"'Segoe UI','Helvetica Neue',sans-serif"}}>
      <style>{`@keyframes cotPulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:0.6;transform:scale(1.3)} }`}</style>
      {/* Sidebar */}
      <div style={{width:220,minHeight:"100vh",background:C.panel,borderRight:`1px solid ${C.border}`,display:"flex",flexDirection:"column"}}>
        <div style={{padding:"18px 16px 10px",borderBottom:`1px solid ${C.border}`}}>
          <div style={{fontSize:16,fontWeight:800,letterSpacing:1.5,color:C.text}}>AGRIMACRO</div>
          <div style={{fontSize:9,color:C.textMuted,letterSpacing:1,marginTop:3}}>COMMODITIES DASHBOARD v3.2</div>
        </div>
        
        {/* Commodities list */}
        <div style={{flex:1,overflowY:"auto",padding:"10px 0"}}>
          {/* INTEL - Central de Inteligência */}
          <div onClick={()=>{setViewMode("intel");}} style={{
            display:"flex",alignItems:"center",gap:10,padding:"10px 16px",cursor:"pointer",
            background:viewMode==="intel"?"rgba(220,180,50,.12)":"transparent",
            borderLeft:viewMode==="intel"?"3px solid #DCB432":"3px solid transparent",
            transition:"all .15s",marginBottom:4,
          }}>
            <div style={{width:28,height:28,borderRadius:6,background:"rgba(220,180,50,.15)",display:"flex",alignItems:"center",justifyContent:"center",fontSize:14}}>{"\u{1F4CA}"}</div>
            <div>
              <div style={{fontSize:12,fontWeight:viewMode==="intel"?800:600,color:viewMode==="intel"?"#DCB432":C.textDim,letterSpacing:1,display:'flex',alignItems:'center',gap:6}}>
                INTEL
                {(()=>{
                  const cotAlerts = Object.values(cot?.commodities || {}).filter((c:any) =>
                    c.disaggregated?.delta_analysis?.dominant_direction !== 'NEUTRAL' &&
                    (c.disaggregated?.delta_analysis?.reversal_score || 0) > 60
                  ).length;
                  return cotAlerts > 0 ? (
                    <span style={{background:'#DC3C3C',color:'#fff',fontSize:8,fontWeight:700,borderRadius:8,padding:'1px 5px',lineHeight:'12px'}}>{cotAlerts}</span>
                  ) : null;
                })()}
              </div>
              <div style={{fontSize:8,color:C.textMuted}}>Central de Inteligência</div>
            </div>
          </div>
          <div style={{height:1,background:C.border,margin:"4px 16px 8px"}}/>
          {["Grãos","Softs","Pecuária","Energia","Metais","Macro"].map(grp=>(
            <div key={grp}>
              <div style={{padding:"10px 16px 4px",fontSize:9,fontWeight:700,color:C.textMuted,letterSpacing:1,textTransform:"uppercase"}}>{grp}</div>
              {COMMODITIES.filter(c=>c.group===grp).map(c=>{
                const p=getPrice(c.sym);const ch=getChange(c.sym);const sel=c.sym===selected;
                const isSusp = priceValidation?.details?.[c.sym]?.is_suspicious === true;
                const suspReason = priceValidation?.details?.[c.sym]?.reason;
                const cotComm = cot?.commodities?.[c.sym];
                const cotDis = cotComm?.disaggregated;
                const cotLeg = cotComm?.legacy;
                const da = cotDis?.delta_analysis;

                // COT Index (min-max over history)
                const mmHist = cotDis?.history?.map((h:any) => h.managed_money_net || 0).filter((v:number) => v !== 0);
                let cotIdx: number | null = null;
                if (mmHist && mmHist.length >= 10) {
                  const mmMin = Math.min(...mmHist);
                  const mmMax = Math.max(...mmHist);
                  const mmLast = mmHist[mmHist.length - 1];
                  cotIdx = mmMax > mmMin ? Math.round((mmLast - mmMin) / (mmMax - mmMin) * 100) : 50;
                }

                // Badge logic
                const hasSignal = da && da.dominant_direction !== 'NEUTRAL' && da.reversal_score >= 60;
                let badgeColor: string | null = null;
                let badgeLabel = '';
                if (cotIdx !== null) {
                  if (cotIdx >= 85) { badgeColor = '#DC3C3C'; badgeLabel = `COT ${cotIdx}/100 EXTREMO\u2191`; }
                  else if (cotIdx <= 15) { badgeColor = '#00C878'; badgeLabel = `COT ${cotIdx}/100 EXTREMO\u2193`; }
                  else if (cotIdx >= 70) { badgeColor = '#DCB432'; badgeLabel = `COT ${cotIdx}/100`; }
                  else if (cotIdx <= 30) { badgeColor = '#3b82f6'; badgeLabel = `COT ${cotIdx}/100`; }
                }
                if (hasSignal && !badgeColor) {
                  badgeColor = da!.dominant_direction === 'BEARISH' ? '#DC3C3C' : '#00C878';
                  badgeLabel = `${da!.dominant_direction} ${da!.reversal_score}%`;
                }
                const showBadge = badgeColor !== null;
                const isPulsing = hasSignal && cotIdx !== null && cotIdx >= 85;
                const deltaVal = da?.current_delta;
                const tooltipText = badgeLabel + (deltaVal ? ` | \u0394${deltaVal > 0 ? '+' : ''}${(deltaVal/1000).toFixed(0)}K` : '');

                return (
                  <div key={c.sym} onClick={()=>{setSelected(c.sym);setViewMode("commodity");setTab("Visão Geral");}} style={{
                    display:"flex",alignItems:"center",justifyContent:"space-between",padding:"7px 16px",cursor:"pointer",
                    background:sel&&viewMode==="commodity"?"rgba(0,200,120,.10)":sel?"rgba(59,130,246,.08)":"transparent",
                    borderLeft:sel&&viewMode==="commodity"?`3px solid #00C878`:sel?`3px solid ${C.blue}`:"3px solid transparent",
                    transition:"all .15s",
                  }}>
                    <div>
                      <div style={{fontSize:12,fontWeight:sel?700:500,color:sel?C.text:C.textDim}}>{c.sym}</div>
                      <div style={{fontSize:9,color:C.textMuted}}>{c.name}</div>
                    </div>
                    <div style={{textAlign:"right"}}>
                      <div style={{fontSize:11,fontWeight:600,fontFamily:"monospace",color:isSusp?"#64748b":p?C.text:C.textMuted,display:"flex",alignItems:"center",justifyContent:"flex-end",gap:2}}>
                        {p?p.toFixed(2):"--"}
                        {isSusp && <span style={{fontSize:8,color:"#DC3C3C",fontWeight:700}} title={suspReason||"Dado suspeito"}>{"\u26A0\uFE0F"}</span>}
                      </div>
                      {isSusp
                        ? <div style={{fontSize:9,color:"#64748b",fontFamily:"monospace"}}>?%</div>
                        : ch && <div style={{fontSize:9,fontFamily:"monospace",color:ch.pct>=0?C.green:C.red}}>{ch.pct>=0?"+":""}{ch.pct.toFixed(2)}%</div>
                      }
                      {showBadge && (
                        <div style={{display:'flex',alignItems:'center',gap:3,justifyContent:'flex-end',marginTop:1}} title={tooltipText}>
                          <div style={{
                            width: hasSignal ? 7 : 5, height: hasSignal ? 7 : 5,
                            borderRadius:'50%', background: badgeColor!,flexShrink:0,
                            boxShadow: hasSignal ? `0 0 4px ${badgeColor}` : 'none',
                            animation: isPulsing ? 'cotPulse 1.5s infinite' : 'none',
                          }}/>
                          <span style={{fontSize:8,fontFamily:'monospace',color:badgeColor!,opacity:0.9}}>
                            {cotIdx}{deltaVal != null && <span style={{opacity:0.7}}>{deltaVal > 0 ? '\u2191' : '\u2193'}</span>}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          ))}
        </div>
        
        {/* Pipeline status */}
        <div style={{padding:"12px 16px",borderTop:`1px solid ${C.border}`,fontSize:9}}>
          <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:4}}>
            <div style={{width:6,height:6,borderRadius:"50%",background:pipelineOk?C.green:C.red}}/>
            <span style={{color:C.textMuted}}>{pipelineOk?"Pipeline Online":"Dados Parciais"}</span>
          </div>
          <div style={{color:C.textMuted}}>Última atualização: {lastDate}</div>
        </div>
      </div>

      {/* Main content */}
      <div style={{flex:1,display:"flex",flexDirection:"column",overflow:"hidden"}}>
        {/* Header */}
        <div style={{padding:"14px 24px",borderBottom:`1px solid ${C.border}`,display:"flex",justifyContent:"space-between",alignItems:"center",background:C.panel}}>
          <div style={{display:"flex",alignItems:"center",gap:16}}>
            <div style={{fontSize:16,fontWeight:700}}>{viewMode==="intel"?"Central de Inteligência":COMMODITIES.find(c=>c.sym===selected)?.name||selected}</div>
            <Badge label="DADOS REAIS" color={C.green} />
            {pipelineOk && <Badge label="PIPELINE ONLINE" color={C.blue} />}
            {(priceValidation?.blocked ?? 0) > 0 && (
              <div style={{
                background:'#DC3C3C22',
                border:'1px solid #DC3C3C55',
                borderRadius:4, padding:'3px 10px',
                fontSize:9, color:'#DC3C3C',
                fontWeight:700, marginLeft:8,
                display:'flex', alignItems:'center', gap:4,
              }}>
                {"\u26A0\uFE0F"} {priceValidation.blocked} dado(s) suspeito(s)
              </div>
            )}
            <div style={{display:"flex",alignItems:"center",gap:8,marginLeft:12}}>
              <button onClick={refreshIbkr} disabled={ibkrRefreshing} style={{padding:"3px 10px",fontSize:10,fontWeight:600,background:ibkrRefreshing?"#555":C.blue,color:"#fff",border:"none",borderRadius:4,cursor:ibkrRefreshing?"wait":"pointer",letterSpacing:0.5}}>
                {ibkrRefreshing?"? Atualizando...":"IBKR Refresh"}
              </button>
              <span style={{fontSize:10,color:C.textMuted}}>IBKR: {ibkrTime}</span>
              <button onClick={refreshPipeline} disabled={pipeRefresh} style={{padding:"3px 10px",fontSize:10,fontWeight:600,background:pipeRefresh?"#555":"#e94560",color:"#fff",border:"none",borderRadius:4,cursor:pipeRefresh?"wait":"pointer",letterSpacing:0.5,marginLeft:8}}>
                {pipeRefresh?"? Rodando...":"Atualizar Pipeline"}
              </button>
              {pipeMsg && <span style={{fontSize:9,color:C.textMuted,marginLeft:4}}>{pipeMsg}</span>}
              <a href="/api/latest-pdf" target="_blank" rel="noopener noreferrer" style={{padding:"3px 10px",fontSize:10,fontWeight:600,background:"#a855f7",color:"#fff",border:"none",borderRadius:4,cursor:"pointer",letterSpacing:0.5,textDecoration:"none",marginLeft:8}}>PDF Report</a>
              <button onClick={()=>setShowStrategyBuilder(prev=>!prev)} style={{
                padding:"3px 12px",fontSize:10,fontWeight:700,borderRadius:4,cursor:"pointer",marginLeft:8,
                background:showStrategyBuilder?"rgba(124,58,237,.25)":"rgba(124,58,237,.10)",
                color:"#a78bfa",border:`1px solid ${showStrategyBuilder?"rgba(124,58,237,.5)":"rgba(124,58,237,.25)"}`,
                letterSpacing:0.5,transition:"all .2s",
              }}>{showStrategyBuilder?"\u{1F4D0} Fechar Builder":"\u{1F4D0} Strategy Builder"}</button>
            </div>
          </div>
          <div style={{fontSize:11,color:C.textMuted}}>{lastDate}</div>
        </div>

        {/* Strategy Builder Panel */}
        {showStrategyBuilder && (
          <div style={{padding:"14px 24px",borderBottom:`1px solid rgba(124,58,237,.3)`,background:"linear-gradient(180deg,#1a1030 0%,#0E1A24 100%)"}}>
            <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:10}}>
              <span style={{fontSize:13,fontWeight:700,color:"#a78bfa"}}>{"\u{1F4D0}"} Strategy Builder</span>
              <span style={{fontSize:9,color:"#64748b"}}>Monte estrutura e envie para analise com contexto completo</span>
            </div>
            <div style={{display:"flex",gap:12,flexWrap:"wrap",alignItems:"flex-end"}}>
              {/* Underlying */}
              <div>
                <div style={{fontSize:8,color:"#64748b",marginBottom:3,textTransform:"uppercase",letterSpacing:0.5}}>Underlying</div>
                <select value={sbUnderlying} onChange={e=>setSbUnderlying(e.target.value)} style={{
                  padding:"5px 8px",fontSize:11,background:"#142332",color:"#e2e8f0",border:"1px solid #1e3a4a",borderRadius:4,
                }}>
                  {COMMODITIES.filter(c=>c.group!=="Macro").map(c=>(
                    <option key={c.sym} value={c.sym}>{c.sym} — {c.name}</option>
                  ))}
                </select>
              </div>
              {/* Direction */}
              <div>
                <div style={{fontSize:8,color:"#64748b",marginBottom:3,textTransform:"uppercase",letterSpacing:0.5}}>Direcao</div>
                <div style={{display:"flex",gap:4}}>
                  {(["PUT","CALL"] as const).map(d=>(
                    <button key={d} onClick={()=>setSbDirection(d)} style={{
                      padding:"5px 12px",fontSize:10,fontWeight:600,borderRadius:4,cursor:"pointer",
                      background:sbDirection===d?(d==="PUT"?"rgba(220,60,60,.2)":"rgba(0,200,120,.2)"):"transparent",
                      color:sbDirection===d?(d==="PUT"?"#DC3C3C":"#00C878"):"#64748b",
                      border:`1px solid ${sbDirection===d?(d==="PUT"?"rgba(220,60,60,.4)":"rgba(0,200,120,.4)"):"#1e3a4a"}`,
                    }}>{d}</button>
                  ))}
                </div>
              </div>
              {/* Structure */}
              <div>
                <div style={{fontSize:8,color:"#64748b",marginBottom:3,textTransform:"uppercase",letterSpacing:0.5}}>Estrutura</div>
                <select value={sbStructure} onChange={e=>setSbStructure(e.target.value)} style={{
                  padding:"5px 8px",fontSize:11,background:"#142332",color:"#e2e8f0",border:"1px solid #1e3a4a",borderRadius:4,
                }}>
                  <option value="butterfly">Butterfly 22x22</option>
                  <option value="ratio">Ratio Spread</option>
                  <option value="vertical">Vertical Spread</option>
                  <option value="strangle">Strangle</option>
                  <option value="custom">Custom</option>
                </select>
              </div>
              {/* Strike */}
              <div>
                <div style={{fontSize:8,color:"#64748b",marginBottom:3,textTransform:"uppercase",letterSpacing:0.5}}>Strike (OTM)</div>
                <input type="text" value={sbStrike} onChange={e=>setSbStrike(e.target.value)}
                  placeholder={(() => { const u: any = optionsChain?.underlyings?.[sbUnderlying]; return u?.und_price ? `ATM ~${u.und_price.toFixed(0)}` : "ex: 65"; })()}
                  style={{width:90,padding:"5px 8px",fontSize:11,background:"#142332",color:"#e2e8f0",border:"1px solid #1e3a4a",borderRadius:4}} />
              </div>
              {/* DTE */}
              <div>
                <div style={{fontSize:8,color:"#64748b",marginBottom:3,textTransform:"uppercase",letterSpacing:0.5}}>DTE</div>
                <input type="text" value={sbDte} onChange={e=>setSbDte(e.target.value)} placeholder="45"
                  style={{width:50,padding:"5px 8px",fontSize:11,background:"#142332",color:"#e2e8f0",border:"1px solid #1e3a4a",borderRadius:4}} />
              </div>
              {/* Qty */}
              <div>
                <div style={{fontSize:8,color:"#64748b",marginBottom:3,textTransform:"uppercase",letterSpacing:0.5}}>Contratos</div>
                <input type="text" value={sbQty} onChange={e=>setSbQty(e.target.value)} placeholder="22"
                  style={{width:50,padding:"5px 8px",fontSize:11,background:"#142332",color:"#e2e8f0",border:"1px solid #1e3a4a",borderRadius:4}} />
              </div>
              {/* Note */}
              <div style={{flex:1,minWidth:150}}>
                <div style={{fontSize:8,color:"#64748b",marginBottom:3,textTransform:"uppercase",letterSpacing:0.5}}>Tese / Nota</div>
                <input type="text" value={sbNote} onChange={e=>setSbNote(e.target.value)} placeholder="Ex: IV alta, sazonalidade favoravel..."
                  style={{width:"100%",padding:"5px 8px",fontSize:11,background:"#142332",color:"#e2e8f0",border:"1px solid #1e3a4a",borderRadius:4,boxSizing:"border-box"}} />
              </div>
              {/* Analyze button */}
              <button onClick={async()=>{
                if(sbLoading) return;
                setSbLoading(true);
                setSbResult(null);
                try {
                  // Build enriched prompt
                  const ctx = buildCommodityPromptTop(sbUnderlying);
                  const structLabel = sbStructure==="butterfly"?"Butterfly 22x22 (BUY ATM + SELL OTM30 + BUY OTM15)"
                    :sbStructure==="ratio"?"Ratio Spread (BUY 1 ATM + SELL 4 OTM)"
                    :sbStructure==="vertical"?"Vertical Spread (BUY/SELL adjacentes)"
                    :sbStructure==="strangle"?"Strangle (SELL PUT OTM + SELL CALL OTM)"
                    :"Custom";
                  const prompt = `${ctx}\n\n=== ESTRUTURA PROPOSTA ===\nUnderlying: ${sbUnderlying}\nDirecao: SELL ${sbDirection}\nEstrutura: ${structLabel}\nStrike OTM: ${sbStrike || "a definir (recomendar)"}\nDTE: ${sbDte} dias\nContratos: ${sbQty}\nTese: ${sbNote || "N/A"}\n\nAvalie esta estrutura considerando:\n1. AT + AF para ${sbUnderlying} (dados acima)\n2. O strike proposto e adequado dado IV, delta, e risco?\n3. O DTE esta no sweet spot de theta?\n4. Alternativas se a estrutura nao for ideal\n5. Proximo passo concreto com threshold de entrada\n\nSe algum dado for N/A, dizer explicitamente. Nunca fabricar.`;

                  const res = await fetch("/api/chat",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({messages:[{role:"user",content:prompt}]})});
                  const d = await res.json();
                  if(res.ok && d.response) {
                    setSbResult(d.response);
                    // Also set in council panel for visibility
                    const entry = {text:`**[Builder ${sbUnderlying} ${sbDirection}]** ${d.response}`,time:new Date().toLocaleString("pt-BR")};
                    setIntelCouncil(entry);
                    setCouncilHistory(prev=>{const updated=[entry,...prev].slice(0,5);try{localStorage.setItem("agrimacro_council_history",JSON.stringify(updated));}catch{}return updated;});
                  } else setSbResult("ERRO: "+(d.error||"API error"));
                } catch(e:any) { setSbResult("ERRO: "+e.message); }
                setSbLoading(false);
              }} disabled={sbLoading} style={{
                padding:"5px 18px",fontSize:11,fontWeight:700,borderRadius:4,cursor:sbLoading?"wait":"pointer",
                background:sbLoading?"#1E3044":"rgba(124,58,237,.2)",color:"#a78bfa",
                border:"1px solid rgba(124,58,237,.4)",letterSpacing:0.5,transition:"all .2s",alignSelf:"flex-end",
              }}>{sbLoading?"Analisando...":"Analisar Estrutura"}</button>
            </div>

            {/* Quick context line */}
            {(()=>{
              const u: any = optionsChain?.underlyings?.[sbUnderlying];
              const iv = u?.iv_rank?.current_iv;
              const term = u?.term_structure?.structure;
              const c: any = cot?.commodities?.[sbUnderlying]?.disaggregated;
              const bars = prices?.[sbUnderlying];
              const price = Array.isArray(bars) && bars.length ? bars[bars.length-1].close : null;
              return (
                <div style={{marginTop:8,fontSize:9,color:"#64748b",fontFamily:"monospace",display:"flex",gap:12,flexWrap:"wrap"}}>
                  {price && <span>Preco: ${price.toFixed(2)}</span>}
                  {iv && <span>IV: {(iv*100).toFixed(0)}%</span>}
                  {term && <span>Term: {term}</span>}
                  {c?.cot_index != null && <span>COT: {c.cot_index.toFixed(0)}</span>}
                  {u?.skew?.skew_pct != null && <span>Skew: {u.skew.skew_pct > 0 ? "+" : ""}{u.skew.skew_pct}%</span>}
                </div>
              );
            })()}

            {/* Result inline */}
            {sbResult && (
              <div style={{marginTop:12,padding:12,background:"#0a1520",borderRadius:8,border:"1px solid rgba(124,58,237,.2)",maxHeight:300,overflowY:"auto"}}>
                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:8}}>
                  <span style={{fontSize:10,color:"#a78bfa",fontWeight:600}}>Analise da Estrutura</span>
                  <button onClick={()=>setSbResult(null)} style={{fontSize:9,color:"#64748b",background:"none",border:"none",cursor:"pointer"}}>fechar</button>
                </div>
                <div style={{fontSize:11,color:"#e2e8f0",lineHeight:1.6,whiteSpace:"pre-wrap"}}>{sbResult}</div>
              </div>
            )}
          </div>
        )}

        {/* Global tabs -- visões macro/cross-asset */}
        <div style={{display:"flex",gap:0,borderBottom:`1px solid #1E3044`,background:"#0E1A24",overflowX:"auto"}}>
          {([
            {label:"Visão Geral",tab:"Visão Geral"},
            {label:"Sazonalidade",tab:"Sazonalidade"},{label:"Comparativo",tab:"Comparativo"},
            {label:"Spreads",tab:"Spreads"},{label:"Físico Intl",tab:"Físico Intl"},
            {label:"Estoques",tab:"Stocks Watch"},{label:"Energia",tab:"Energia"},
            {label:"Custo Produção",tab:"Custo Produção"},
            {label:"Grain Ratios",tab:"Grain Ratios",only:["ZC","ZS","ZW","KE","ZM","ZL"]},
            {label:"Bilateral",tab:"Bilateral"},
            {label:"Paridades",tab:"Paridades"},
            {label:"Portfolio",tab:"Portfolio"},{label:"Livestock Risk",tab:"Livestock Risk"},{label:"Calendario",tab:"Leitura do Dia"},
          ] as {label:string;tab:Tab;only?:string[]}[]).filter(t=>!t.only||t.only.includes(selected)).map(t=>{
            const isActive = (t.tab==="Visão Geral" ? viewMode==="commodity" && tab==="Visão Geral" : viewMode==="global" && tab===t.tab);
            return (
              <button key={t.label} onClick={()=>{setViewMode(t.tab==="Visão Geral"?"commodity":"global");setTab(t.tab);}} style={{
                padding:"9px 16px",fontSize:10,fontWeight:isActive?700:500,
                color:isActive?"#DCB432":"#8C96A5",background:isActive?"#142332":"transparent",
                border:"none",cursor:"pointer",whiteSpace:"nowrap",transition:"all .15s",
                borderBottom:isActive?"2px solid #DCB432":"2px solid transparent",
              }}>{t.label}{t.tab==="Livestock Risk"&&(()=>{const ci=cot?.commodities?.LE?.disaggregated?.delta_analysis?.cot_index;return ci!=null&&(ci>=85||ci<=15)?<span style={{fontSize:8,marginLeft:3,background:"#DCB432",color:"#0E1A24",borderRadius:4,padding:"1px 4px",fontWeight:700}}>{"\u{1F404}"}</span>:null;})()}</button>
            );
          })}
        </div>

        {/* Content */}
        <div style={{flex:1,overflow:"auto",padding:24}}>
          {loading ? <LoadingSpinner /> : viewMode==="intel" ? renderIntelPage() : viewMode==="commodity" ? (tab==="Visão Geral" ? (<>
            {/* Deep Dive button for selected commodity */}
            <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:12}}>
              {/* Option 1: Send to Strategy chat (pre-fills textarea + auto-submits) */}
              <button onClick={()=>{
                const prompt = buildCommodityPromptTop(selected);
                setStrategyInput(prompt);
                setStrategyConvoHistory([]);
                setStrategyResult(null);
                setViewMode("intel");
                // Auto-submit after switching to intel tab
                setTimeout(()=>{
                  const textarea = document.querySelector('textarea[placeholder*="tese"]') as HTMLTextAreaElement;
                  if(textarea) textarea.scrollIntoView({behavior:"smooth",block:"center"});
                }, 200);
              }} style={{
                padding:"6px 16px",fontSize:10,fontWeight:600,borderRadius:6,cursor:"pointer",
                background:"rgba(124,58,237,.12)",color:"#a78bfa",
                border:"1px solid rgba(124,58,237,.3)",transition:"all .2s",
              }}>Deep Dive {selected} (Strategy)</button>

              {/* Option 2: Direct Council analysis */}
              <button onClick={async()=>{
                if(intelCouncilLoading) return;
                setIntelCouncilLoading(true);
                try {
                  const prompt = buildCommodityPromptTop(selected);
                  const res = await fetch("/api/chat",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({messages:[{role:"user",content:prompt}]})});
                  const d = await res.json();
                  if(res.ok && d.response){
                    const entry={text:`**[${selected}]** ${d.response}`,time:new Date().toLocaleString("pt-BR")};
                    setIntelCouncil(entry);
                    setCouncilHistory(prev=>{const updated=[entry,...prev].slice(0,5);try{localStorage.setItem("agrimacro_council_history",JSON.stringify(updated));}catch{}return updated;});
                  } else setIntelCouncil({text:"ERRO: "+(d.error||"API error"),time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})});
                }catch(e:any){setIntelCouncil({text:"ERRO: "+e.message,time:new Date().toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit"})});}
                setIntelCouncilLoading(false);
              }} disabled={intelCouncilLoading} style={{
                padding:"6px 16px",fontSize:10,fontWeight:600,borderRadius:6,cursor:intelCouncilLoading?"wait":"pointer",
                background:intelCouncilLoading?"#1E3044":"rgba(220,180,50,.10)",color:"#DCB432",
                border:"1px solid rgba(220,180,50,.3)",transition:"all .2s",
              }}>{intelCouncilLoading?`Analisando...`:`Council ${selected}`}</button>

              <span style={{fontSize:9,color:"#64748b"}}>Strategy: abre na aba Intel c/ prompt preenchido | Council: analise direta</span>
            </div>
            {renderTab()}
          </>) : renderCommodityView()) : renderTab()}
        </div>
      </div>
    </div>
  );
}






