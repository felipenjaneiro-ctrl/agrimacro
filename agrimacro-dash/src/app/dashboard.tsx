import BilateralPanel from "./BilateralPanel";
import { useState, useEffect, useRef } from "react";

/* ---------------------------------------------------------------------------
   AgriMacro v3.2 — Dashboard Profissional de Commodities Agrícolas
   ZERO MOCK — Somente dados reais via pipeline JSON
   --------------------------------------------------------------------------- */

// -- Types ------------------------------------------------------------------
interface OHLCV { date:string; open:number; high:number; low:number; close:number; volume:number; }
type PriceData = Record<string, OHLCV[]>;

interface SeasonPoint { day:number; close:number; date?:string; }
interface SeasonEntry { symbol:string; status:string; years:string[]; series:Record<string,SeasonPoint[]>; }
type SeasonData = Record<string, SeasonEntry>;

interface SpreadInfo { 
  name:string; unit:string; current:number; zscore_1y:number; percentile:number; 
  regime:string; points:number; description?:string; trend?:string; trend_pct?:number;
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
interface COTCommodity {
  ticker:string; name:string;
  legacy?:{ticker:string;name:string;report_type:string;latest:COTHistoryEntry;history:COTHistoryEntry[];weeks:number};
  disaggregated?:{ticker:string;name:string;report_type:string;latest:COTHistoryEntry;history:COTHistoryEntry[];weeks:number};
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

type Tab = "Gráfico + COT"|"Comparativo"|"Spreads"|"Sazonalidade"|"Stocks Watch"|"Custo Produção"|"Físico Intl"|"Leitura do Dia"|"Energia"|"Portfolio"|"Bilateral";

// -- Color Theme ------------------------------------------------------------
const C = {
  bg:"#0d1117", panel:"#161b22", panelAlt:"#1c2128", border:"rgba(148,163,184,.12)",
  text:"#e2e8f0", textDim:"#94a3b8", textMuted:"#64748b",
  green:"#22c55e", red:"#ef4444", amber:"#f59e0b", blue:"#3b82f6", cyan:"#06b6d4", purple:"#a78bfa",
  greenBg:"rgba(34,197,94,.12)", redBg:"rgba(239,68,68,.12)", amberBg:"rgba(245,158,11,.12)", blueBg:"rgba(59,130,246,.12)",
  greenBorder:"rgba(34,197,94,.3)", redBorder:"rgba(239,68,68,.3)", amberBorder:"rgba(245,158,11,.3)",
  candleUp:"#22c55e", candleDn:"#ef4444", wick:"rgba(148,163,184,.4)",
  ma9:"#06b6d4", ma21:"#fbbf24", ma50:"#a78bfa", ma200:"#ef4444", bb:"rgba(148,163,184,.18)",
  rsiLine:"#f59e0b", rsiOverbought:"rgba(239,68,68,.3)", rsiOversold:"rgba(34,197,94,.3)",
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

const TABS:Tab[] = ["Gráfico + COT","Comparativo","Spreads","Sazonalidade","Stocks Watch","Custo Produção","Físico Intl","Leitura do Dia","Energia","Portfolio","Bilateral"];

const SEASON_COLORS:Record<string,string> = {
  "2021":"#3b82f6","2022":"#8b5cf6","2023":"#ec4899","2024":"#f59e0b","2025":"#22c55e",
  "current":"#f59e0b","average":"#e2e8f0",
};

const SPREAD_NAMES:Record<string,string> = {
  soy_crush:"Soy Crush Margin",ke_zw:"KC-CBOT Wheat",zl_cl:"Soy Oil / Crude",
  feedlot:"Feedlot Margin",zc_zm:"Corn / Meal",zc_zs:"Corn / Soy Ratio",
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
    whyMatters:"Esse número influencia o que o produtor americano vai plantar na próxima safra — e isso afeta os preços futuros.",
  },
};

const SPREAD_FRIENDLY_NAMES:Record<string,string> = {
  soy_crush:"Margem de Esmagamento (Soja)",
  ke_zw:"Prêmio Trigo Duro vs Mole (KC–CBOT)",
  zl_cl:"Óleo de Soja vs Petróleo",
  feedlot:"Margem de Confinamento (Feedlot)",
  zc_zm:"Milho vs Farelo (Ração Animal)",
  zc_zs:"Soja vs Milho (Decisão de Plantio)",
};

function getAlertLevel(sp:{regime:string;zscore_1y:number;percentile:number}):"ok"|"atencao"|"alerta" {
  if(sp.regime==="EXTREMO"||Math.abs(sp.zscore_1y)>=2||sp.percentile>=95||sp.percentile<=5) return "alerta";
  if(sp.regime!=="NORMAL"||Math.abs(sp.zscore_1y)>=1.3||sp.percentile>=90||sp.percentile<=10) return "atencao";
  return "ok";
}

function getVerdict(sp:{key?:string;regime:string;zscore_1y:number;percentile:number;current:number;trend?:string;trend_pct?:number;name:string}):string {
  const pctLabel = sp.percentile>=90?"no nível mais caro":sp.percentile>=75?"acima da média":sp.percentile<=10?"no nível mais barato":sp.percentile<=25?"abaixo da média":"dentro do normal";
  const trendLabel = sp.trend==="SUBINDO"?"e subindo":sp.trend==="CAINDO"?"e caindo":"e estável";
  const alert = getAlertLevel(sp);
  const prefix = alert==="alerta"?"?? ":alert==="atencao"?"?? ":"";
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
// -- Cost of Production Data ------------------------------------------------
const COST_DATA:{sym:string;commodity:string;futuresUnit:string;regions:{region:string;cost:number;unit:string;source:string}[]}[] = [
  {sym:"ZC",commodity:"Corn",futuresUnit:"¢/bu",regions:[
    {region:"???? EUA (Heartland)",cost:475,unit:"¢/bu",source:"USDA ERS 2025f"},
    {region:"???? EUA (High Prod.)",cost:430,unit:"¢/bu",source:"Purdue 2025"},
    {region:"???? Brasil (MT)",cost:350,unit:"¢/bu",source:"CONAB 24/25"},
    {region:"???? Brasil (PR)",cost:380,unit:"¢/bu",source:"CONAB 24/25"},
    {region:"???? Argentina",cost:310,unit:"¢/bu",source:"Bolsa Cereales 24/25"},
  ]},
  {sym:"ZS",commodity:"Soybeans",futuresUnit:"¢/bu",regions:[
    {region:"???? EUA (Heartland)",cost:1103,unit:"¢/bu",source:"USDA ERS 2025f"},
    {region:"???? EUA (High Prod.)",cost:1050,unit:"¢/bu",source:"Purdue 2025"},
    {region:"???? Brasil (MT)",cost:870,unit:"¢/bu",source:"CONAB 24/25"},
    {region:"???? Brasil (MATOPIBA)",cost:830,unit:"¢/bu",source:"CONAB 24/25"},
    {region:"???? Argentina",cost:800,unit:"¢/bu",source:"Bolsa Cereales 24/25"},
  ]},
  {sym:"ZW",commodity:"Wheat",futuresUnit:"¢/bu",regions:[
    {region:"???? EUA (Kansas)",cost:560,unit:"¢/bu",source:"USDA ERS 2025f"},
    {region:"???? EUA (N.Dakota)",cost:530,unit:"¢/bu",source:"USDA ERS 2025f"},
    {region:"???? Rússia",cost:350,unit:"¢/bu",source:"IKAR est. 2025"},
    {region:"???? Argentina",cost:400,unit:"¢/bu",source:"Bolsa Cereales 24/25"},
    {region:"???? Austrália",cost:440,unit:"¢/bu",source:"ABARES est. 2025"},
  ]},
  {sym:"KC",commodity:"Coffee Arabica",futuresUnit:"¢/lb",regions:[
    {region:"???? Brasil (Cerrado)",cost:155,unit:"¢/lb",source:"CONAB 24/25"},
    {region:"???? Brasil (Sul MG)",cost:175,unit:"¢/lb",source:"CONAB 24/25"},
    {region:"???? Colômbia",cost:220,unit:"¢/lb",source:"FNC est. 2025"},
    {region:"???? Vietnã (Robusta eq.)",cost:105,unit:"¢/lb",source:"VICOFA est. 2025"},
    {region:"???? Etiópia",cost:130,unit:"¢/lb",source:"ECX est. 2025"},
  ]},
  {sym:"SB",commodity:"Sugar #11",futuresUnit:"¢/lb",regions:[
    {region:"???? Brasil (SP)",cost:13.5,unit:"¢/lb",source:"UNICA 24/25"},
    {region:"???? Índia",cost:17.5,unit:"¢/lb",source:"ISMA est. 2025"},
    {region:"???? Tailândia",cost:15.0,unit:"¢/lb",source:"OCSB est. 2025"},
  ]},
  {sym:"LE",commodity:"Live Cattle",futuresUnit:"¢/lb",regions:[
    {region:"???? EUA (Feedlot)",cost:195,unit:"¢/lb",source:"USDA ERS 2025f"},
    {region:"???? Brasil (Confin.)",cost:145,unit:"¢/lb",source:"CEPEA 2025"},
    {region:"???? Austrália",cost:165,unit:"¢/lb",source:"MLA est. 2025"},
    {region:"???? Argentina",cost:115,unit:"¢/lb",source:"IPCVA est. 2025"},
  ]},
  {sym:"CC",commodity:"Cocoa",futuresUnit:"$/mt",regions:[
    {region:"???? Costa do Marfim",cost:3200,unit:"$/mt",source:"CCC est. 2025"},
    {region:"???? Gana",cost:3600,unit:"$/mt",source:"COCOBOD est. 2025"},
    {region:"???? Indonésia",cost:2800,unit:"$/mt",source:"ASKINDO est. 2025"},
    {region:"???? Brasil (Bahia)",cost:3800,unit:"$/mt",source:"CEPLAC est. 2025"},
  ]},
  {sym:"CT",commodity:"Cotton #2",futuresUnit:"¢/lb",regions:[
    {region:"???? EUA (Texas)",cost:78,unit:"¢/lb",source:"USDA ERS 2025f"},
    {region:"???? Brasil (MT)",cost:58,unit:"¢/lb",source:"CONAB 24/25"},
    {region:"???? Índia",cost:62,unit:"¢/lb",source:"CAI est. 2025"},
    {region:"???? Austrália",cost:60,unit:"¢/lb",source:"Cotton AU est. 2025"},
  ]},
];

// -- Physical Markets Data (International - placeholder) ---------------------
const PHYS_INTL:{cat:string;items:{origin:string;price:string;basis:string;trend:string;source:string}[]}[] = [
  {cat:"? Coffee",items:[
    {origin:"???? Brasil (Santos) — Arabica NY 2/3",price:"—",basis:"—",trend:"Colheita 25/26 iniciando",source:"Cecafé"},
    {origin:"???? Brasil (Cerrado) — Fine Cup",price:"—",basis:"—",trend:"Prêmio qualidade",source:"Cecafé"},
    {origin:"???? Colômbia — Excelso EP",price:"—",basis:"—",trend:"Prêmio qualidade alto",source:"FNC"},
    {origin:"???? Vietnã — Robusta G2",price:"—",basis:"—",trend:"Safra acima esperado",source:"VICOFA"},
  ]},
  {cat:"?? Soybeans",items:[
    {origin:"???? Brasil (Paranaguá) — GMO",price:"—",basis:"—",trend:"Colheita recorde",source:"CEPEA"},
    {origin:"???? Argentina — GMO",price:"—",basis:"—",trend:"Safra recuperando",source:"B.Cereales"},
  ]},
  {cat:"?? Corn",items:[
    {origin:"???? Brasil (Paranaguá) — GMO",price:"—",basis:"—",trend:"Safrinha nos portos",source:"CEPEA"},
    {origin:"???? Argentina — GMO",price:"—",basis:"—",trend:"Safra volumosa",source:"B.Cereales"},
  ]},
  {cat:"?? Live Cattle",items:[
    {origin:"???? Brasil (SP) — Boi Gordo @",price:"—",basis:"—",trend:"Alta sazonal",source:"CEPEA"},
    {origin:"???? Argentina (Liniers)",price:"—",basis:"—",trend:"Câmbio favorece export",source:"IPCVA"},
  ]},
  {cat:"?? Wheat",items:[
    {origin:"???? Rússia (FOB BS) — 12.5%",price:"—",basis:"—",trend:"Safra grande",source:"IKAR"},
    {origin:"???? Argentina — Trigo Pan",price:"—",basis:"—",trend:"Normalizado",source:"B.Cereales"},
  ]},
  {cat:"?? Cocoa",items:[
    {origin:"???? Costa do Marfim — Grade I",price:"—",basis:"—",trend:"Menor safra 10 anos",source:"CCC"},
    {origin:"???? Gana — Grade I",price:"—",basis:"—",trend:"Produção -50%",source:"COCOBOD"},
  ]},
  {cat:"???? China Demand",items:[
    {origin:"???? Soja Import (DCE)",price:"—",basis:"—",trend:"Importação desacelerando",source:"GACC"},
    {origin:"???? Milho Import (DCE)",price:"—",basis:"—",trend:"Estoques altos",source:"GACC"},
    {origin:"???? Suínos (Zhengzhou)",price:"—",basis:"—",trend:"Demanda estável",source:"MARA"},
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

function MarginBar({price,cost}:{price:number;cost:number}) {
  const margin = ((price-cost)/cost)*100;
  const color = margin>20?"#10b981":margin>10?"#22c55e":margin>0?"#fbbf24":"#ef4444";
  const bgColor = margin>20?"rgba(16,185,129,0.08)":margin>10?"rgba(34,197,94,0.06)":margin>0?"rgba(251,191,36,0.06)":"rgba(239,68,68,0.08)";
  const label = margin>20?"Lucrando bem":margin>10?"Lucro moderado":margin>0?"Margem apertada":"No prejuízo";
  const icon = margin>20?"??":margin>10?"?":margin>0?"??":"??";
  const barWidth = Math.min(Math.abs(margin), 60);
  const barDirection = margin >= 0 ? "right" : "left";
  return (
    <div style={{display:"flex",alignItems:"center",gap:10,padding:"6px 10px",borderRadius:8,background:bgColor,minWidth:280}}>
      <span style={{fontSize:16}}>{icon}</span>
      <div style={{flex:1}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4}}>
          <span style={{fontSize:12,fontWeight:700,color}}>{label}</span>
          <span style={{fontSize:13,fontWeight:800,fontFamily:"monospace",color}}>
            {margin>0?"+":""}{margin.toFixed(0)}%
          </span>
        </div>
        <div style={{position:"relative",height:8,background:"rgba(148,163,184,0.1)",borderRadius:4,overflow:"hidden"}}>
          <div style={{position:"absolute",top:0,bottom:0,
            [barDirection]:barDirection==="right"?0:undefined,
            left:barDirection==="right"?`${50-barWidth/2}%`:undefined,
            width:`${barWidth}%`,background:color,borderRadius:4,
            boxShadow:`0 0 8px ${color}40`,transition:"width 0.5s ease"}}/>
          <div style={{position:"absolute",top:-2,bottom:-2,left:"50%",width:1,background:"rgba(255,255,255,0.15)"}}/>
        </div>
      </div>
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

function PriceChart({candles,symbol}:{candles:OHLCV[];symbol:string}) {
  const ref = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [dims,setDims] = useState({w:900,h:480});
  const [hover,setHover] = useState<{i:number;x:number;y:number}|null>(null);
  const VISIBLE = 120;
  const RSI_HEIGHT = 80;

  useEffect(()=>{
    const el=containerRef.current;if(!el)return;
    const obs=new ResizeObserver(entries=>{
      const{width}=entries[0].contentRect;
      if(width>0)setDims({w:Math.floor(width),h:480});
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
    drawLine(ema9,C.ma9,1.5);
    drawLine(ma21,C.ma21,1);
    drawLine(ma50,C.ma50,1);
    if(ma200.some(v=>!isNaN(v)))drawLine(ma200,C.ma200,1);

    // Candles
    for(let i=0;i<data.length;i++){
      const c=data[i];const x=xC(i);const up=c.close>=c.open;
      ctx.strokeStyle=C.wick;ctx.lineWidth=1;
      ctx.beginPath();ctx.moveTo(x,yP(c.high));ctx.lineTo(x,yP(c.low));ctx.stroke();
      ctx.fillStyle=up?C.candleUp:C.candleDn;
      const top=yP(Math.max(c.open,c.close));
      const bot=yP(Math.min(c.open,c.close));
      const h=Math.max(1,bot-top);
      ctx.fillRect(x-bW/2,top,bW,h);
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
    
    // RSI background
    ctx.fillStyle=C.panelAlt;
    ctx.fillRect(pad.l,rsiTop,W-pad.l-pad.r,rsiH);
    
    // RSI zones
    ctx.fillStyle=C.rsiOverbought;
    ctx.fillRect(pad.l,rsiTop,W-pad.l-pad.r,rsiH*0.3);
    ctx.fillStyle=C.rsiOversold;
    ctx.fillRect(pad.l,rsiTop+rsiH*0.7,W-pad.l-pad.r,rsiH*0.3);
    
    // RSI 50 line
    ctx.strokeStyle="rgba(148,163,184,.2)";ctx.lineWidth=1;
    ctx.setLineDash([4,4]);
    ctx.beginPath();ctx.moveTo(pad.l,rsiTop+rsiH*0.5);ctx.lineTo(W-pad.r,rsiTop+rsiH*0.5);ctx.stroke();
    ctx.setLineDash([]);
    
    // RSI line
    const yRsi=(v:number)=>rsiTop+rsiH*(1-v/100);
    ctx.strokeStyle=C.rsiLine;ctx.lineWidth=1.5;ctx.beginPath();
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
    ctx.fillText(`RSI: ${lastRsi.toFixed(1)}`,pad.l+5,rsiTop+12);

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
    
    let allVals:number[]=[];
    for(const yr of entry.years){const s=entry.series[yr];if(s)allVals.push(...s.map(p=>p.close));}
    if(!allVals.length)return;
    
    const mn=Math.min(...allVals),mx=Math.max(...allVals),range=mx-mn||1;
    const pMn=mn-range*.05,pMx=mx+range*.05;
    const yP=(v:number)=>pad.t+(1-(v-pMn)/(pMx-pMn))*cH;
    const xP=(day:number)=>pad.l+(day/365)*(w-pad.l-pad.r);

    // Background
    ctx.fillStyle=C.bg;ctx.fillRect(0,0,w,H);
    
    // Grid
    ctx.strokeStyle="rgba(148,163,184,.06)";ctx.lineWidth=1;
    for(let i=0;i<5;i++){const y=pad.t+i*(cH/4);ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();}
    
    // Y axis
    ctx.fillStyle=C.textMuted;ctx.font="10px monospace";ctx.textAlign="right";
    for(let i=0;i<5;i++){const v=pMx-(pMx-pMn)*i/4;ctx.fillText(v.toFixed(0),pad.l-6,pad.t+i*(cH/4)+4);}
    
    // X axis months
    ctx.textAlign="center";ctx.font="9px monospace";
    const months=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    for(let m=0;m<12;m++){const day=m*30.4+15;ctx.fillText(months[m],xP(day),H-pad.b+14);}

    // Draw lines (years first, then current, then average on top)
    const drawOrder = entry.years.filter(y=>y!=="current"&&y!=="average");
    drawOrder.push("average");
    if(entry.years.includes("current"))drawOrder.push("current");

    for(const yr of drawOrder){
      const s=entry.series[yr];if(!s||!s.length)continue;
      const col=SEASON_COLORS[yr]||C.textMuted;
      ctx.strokeStyle=col;
      ctx.lineWidth=yr==="current"?2.5:yr==="average"?2:1;
      ctx.setLineDash(yr==="average"?[6,4]:[]);
      ctx.globalAlpha=yr==="current"||yr==="average"?1:.35;
      ctx.beginPath();
      let started=false;
      for(const p of s){
        const x=xP(p.day),y=yP(p.close);
        if(!started){ctx.moveTo(x,y);started=true;}else ctx.lineTo(x,y);
      }
      ctx.stroke();
      ctx.globalAlpha=1;ctx.setLineDash([]);
    }

    // Legend
    let lx=pad.l;
    ctx.font="9px monospace";
    for(const yr of entry.years){
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
    type Line={label:string;vals:number[];color:string};
    let seriesLines:Line[]=[];

    if(type==="legacy"){
      const commNet=data.map(r=>(r.comm_long||0)-(r.comm_short||0));
      const ncNet=data.map(r=>(r.noncomm_long||0)-(r.noncomm_short||0));
      seriesLines=[
        {label:"Commercial Net",vals:commNet,color:colors.comm},
        {label:"Non-Comm Net",vals:ncNet,color:colors.noncomm},
      ];
    } else {
      const mm=data.map(r=>(r.managed_money_long||0)-(r.managed_money_short||0));
      const pr=data.map(r=>(r.producer_long||0)-(r.producer_short||0));
      const sw=data.map(r=>(r.swap_long||0)-(r.swap_short||0));
      seriesLines=[
        {label:"Managed Money",vals:mm,color:colors.mm},
        {label:"Producer",vals:pr,color:colors.prod},
        {label:"Swap Dealers",vals:sw,color:colors.swap},
      ];
    }

    const H=180;
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
      ctx.strokeStyle="rgba(148,163,184,.2)";ctx.lineWidth=0.8;ctx.setLineDash([4,3]);
      ctx.beginPath();ctx.moveTo(pad.l,zy);ctx.lineTo(w-pad.r,zy);ctx.stroke();
      ctx.setLineDash([]);
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
      ctx.strokeStyle=line.color;ctx.lineWidth=1.8;ctx.beginPath();
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
      const tooltipW=160;const tooltipX=hoverIdx>n/2?hx-tooltipW-10:hx+10;
      const tooltipH=14+seriesLines.length*14+4;
      ctx.fillStyle="rgba(22,27,34,.92)";
      ctx.beginPath();ctx.roundRect(tooltipX,pad.t+2,tooltipW,tooltipH,4);ctx.fill();
      ctx.strokeStyle="rgba(148,163,184,.2)";ctx.lineWidth=0.5;
      ctx.beginPath();ctx.roundRect(tooltipX,pad.t+2,tooltipW,tooltipH,4);ctx.stroke();
      ctx.fillStyle=C.textDim;ctx.font="bold 9px monospace";ctx.textAlign="left";
      ctx.fillText(data[hoverIdx].date,tooltipX+6,pad.t+14);
      seriesLines.forEach((line,li)=>{
        const hVal=line.vals[hoverIdx];
        const vs=(hVal>=0?"+":"")+(Math.abs(hVal)>=1000?(hVal/1000).toFixed(1)+"K":hVal.toFixed(0));
        ctx.fillStyle=line.color;ctx.font="8px monospace";
        ctx.fillText("\u25CF "+line.label+": "+vs,tooltipX+6,pad.t+14+14*(li+1));
      });
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
  const [tab,setTab] = useState<Tab>("Gráfico + COT");
  const [loading,setLoading] = useState(true);
  const [errors,setErrors] = useState<string[]>([]);
  const [cmp1,setCmp1] = useState<string>("ZCH26");
  const [cmp2,setCmp2] = useState<string>("ZSH26");
  const [cmp3,setCmp3] = useState<string>("");
  const compareSyms = [cmp1,cmp2,cmp3].filter(s=>s!=="");
  const [fwdSyms,setFwdSyms] = useState<string[]>(["ZC"]);
  const [portfolio,setPortfolio] = useState<any>(null);
  const [lastIbkrRefresh,setLastIbkrRefresh] = useState<string|null>(null);
  const [ibkrRefreshing,setIbkrRefreshing] = useState(false);
  const [pipeRefresh,setPipeRefresh] = useState(false);
  const [pipeMsg,setPipeMsg] = useState("");
  const [calendar,setCalendar] = useState<any>(null);
  const [news,setNews] = useState<any>(null);
  const [portfolioMsgs,setPortfolioMsgs] = useState<{role:string;content:string}[]>([]);
  const [portfolioInput,setPortfolioInput] = useState("");
  const [portfolioLoading,setPortfolioLoading] = useState(false);

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
      fetch("/data/processed/ibkr_export.json").then(r=>r.json()).then(d=>{setPortfolio(d);setLastIbkrRefresh(d.export_timestamp||new Date().toISOString());}).catch(()=>console.warn("No IBKR data")),
    ]).finally(()=>{setErrors(errs);setLoading(false);});
  },[]);

  // IBKR Auto-Refresh
  const refreshIbkr = async () => {
    if(ibkrRefreshing) return;
    setIbkrRefreshing(true);
    try {
      const res = await fetch("/api/refresh-ibkr", {method:"POST"});
      const data = await res.json();
      if(data.status === "ok") {
        const p = await fetch("/data/processed/ibkr_export.json?t="+Date.now());
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
  const lastDate = prices&&prices[selected]?.length ? prices[selected][prices[selected].length-1].date : "—";
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
        <PriceChart candles={prices[selected]} symbol={selected} />
      ) : (
        <DataPlaceholder title="Sem dados de preço" detail={`${selected} não encontrado em price_history.json`} />
      )}
      <div style={{marginTop:12}}>
        {hasCOT(selected) ? (
          <>
            <div style={{fontSize:11,fontWeight:700,color:C.textDim,padding:"8px 0 4px",borderTop:`1px solid ${C.border}`}}>
              COT — LEGACY (Commercial vs Non-Commercial)
            </div>
            {cotLegacy?.history?.length ? (
              <COTChart history={cotLegacy.history} type="legacy" />
            ) : null}
            <div style={{fontSize:11,fontWeight:700,color:C.textDim,padding:"8px 0 4px",borderTop:`1px solid ${C.border}`,marginTop:4}}>
              COT — DISAGGREGATED (Managed Money / Producer / Swap)
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
            <div style={{marginTop:6,fontSize:8,color:C.textMuted}}>
              CFTC Commitments of Traders | {cotLegacy?.weeks||0}w history | Last: {cotLegacy?.latest?.date||"—"}
            </div>
          </>
        ) : (
          <DataPlaceholder title="COT — Dados Pendentes" detail="Execute collect_cot.py para coletar dados CFTC." />
        )}
      </div>
    </div>
  );

  // -- Tab: Comparativo ---------------------------------------------------
  const getSymFromContract = (contract: string) => { const m = contract.match(/^([A-Z]{2})/); return m ? m[1] : contract; };
  const renderComparativo = () => (
    <div>
      <SectionTitle>?? Comparativo Normalizado (Base 100)</SectionTitle>
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
                const ytdStart=d.find(c=>c.date.startsWith(yr));
                const ytd=ytdStart?((last-ytdStart.close)/ytdStart.close*100):0;
                const last252=d.slice(-252);
                const min52=Math.min(...last252.map(c=>c.low));
                const max52=Math.max(...last252.map(c=>c.high));
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
      {/* Curva Forward */}
      {futures && (
        <div style={{marginTop:28}}>
          <div style={{fontSize:16,fontWeight:700,color:C.text,marginBottom:12,display:"flex",alignItems:"center",gap:8}}>
            <span style={{fontSize:18}}>??</span> Curva Forward — Contango vs Backwardation
          </div>
          <div style={{marginBottom:12,display:"flex",gap:8,flexWrap:"wrap"}}>
            {Object.keys(futures.commodities||{}).map(sym=>{
              const isSel=fwdSyms.includes(sym);
              return (
                <button key={sym} onClick={()=>{
                  setFwdSyms([sym]);
                }} style={{
                  padding:"4px 10px",fontSize:10,fontWeight:isSel?700:500,borderRadius:4,cursor:"pointer",
                  background:isSel?"rgba(59,130,246,.15)":"transparent",
                  color:isSel?C.blue:C.textMuted,border:`1px solid ${C.border}`,
                  transition:"all .15s"
                }}>{sym}</button>
              );
            })}
          </div>
          <div style={{fontSize:10,color:C.textMuted,marginBottom:8}}>Clique em uma commodity para ver sua curva forward</div>
          <svg viewBox="0 0 900 350" style={{width:"100%",background:C.panel,borderRadius:8,border:`1px solid ${C.border}`}}>
            {(()=>{
              const allCurves = fwdSyms.map((sym,si)=>{
                const fc = (futures.commodities||{})[sym];
                if(!fc) return null;
                const cts = (fc.contracts||[]).filter(c=>c.close>0).sort((a,b)=>{
                  const da=a.year+a.month_code; const db=b.year+b.month_code;
                  return da<db?-1:da>db?1:0;
                });
                if(cts.length<2) return null;
                const front=cts[0].close;
                const pts=cts.map((c,i)=>({label:c.expiry_label,val:c.close,pct:((c.close-front)/front*100),idx:i}));
                return {sym,pts,color:["#3b82f6","#ef4444","#22c55e","#f59e0b","#a855f7"][si%5]};
              }).filter(Boolean);
              if(!allCurves.length) return <text x="450" y="175" textAnchor="middle" fill={C.textMuted} fontSize="12">Selecione commodities acima</text>;
              const allPcts=allCurves.flatMap(c=>c.pts.map(p=>p.pct));
              const minP=Math.min(...allPcts,-2);const maxP=Math.max(...allPcts,2);
              const pad=60;const chartW=900-pad*2;const chartH=280;const top=30;
              const maxLen=Math.max(...allCurves.map(c=>c.pts.length));
              const scaleX=(i)=>pad+i/(maxLen-1)*chartW;
              const scaleY=(v)=>top+(1-(v-minP)/(maxP-minP))*chartH;
              const zeroY=scaleY(0);
              return (
                <g>
                  {Array.from({length:6}).map((_,gi)=>{
                    const val=minP+(maxP-minP)*gi/5;
                    const y=scaleY(val);
                    return <g key={gi}>
                      <line x1={pad} x2={900-pad} y1={y} y2={y} stroke={C.border} strokeWidth="0.5"/>
                      <text x={pad-8} y={y+3} textAnchor="end" fill={C.textMuted} fontSize="9">{`${val.toFixed(1)}%`}</text>
                    </g>;
                  })}
                  <line x1={pad} x2={900-pad} y1={zeroY} y2={zeroY} stroke={C.textMuted} strokeWidth="1" strokeDasharray="4,4"/>
                  {allCurves.map(curve=>(
                    <g key={curve.sym}>
                      <polyline fill="none" stroke={curve.color} strokeWidth="2.5" points={curve.pts.map(p=>`${scaleX(p.idx)},${scaleY(p.pct)}`).join(" ")}/>
                      {curve.pts.map((p,pi)=>(
                        <g key={pi}>
                          <circle cx={scaleX(p.idx)} cy={scaleY(p.pct)} r="3.5" fill={curve.color} stroke={C.panel} strokeWidth="1.5"/>
                          {pi===curve.pts.length-1 && <text x={scaleX(p.idx)+8} y={scaleY(p.pct)+4} fill={curve.color} fontSize="10" fontWeight="700">{curve.sym}</text>}
                        </g>
                      ))}
                    </g>
                  ))}
                  {allCurves[0]?.pts.map((p,pi)=>{
                    if(pi%Math.ceil(maxLen/10)!==0 && pi!==allCurves[0].pts.length-1) return null;
                    return <text key={pi} x={scaleX(pi)} y={top+chartH+18} textAnchor="middle" fill={C.textMuted} fontSize="8" transform={`rotate(-30,${scaleX(pi)},${top+chartH+18})`}>{p.label}</text>;
                  })}
                </g>
              );
            })()}
          </svg>
          <div style={{display:"flex",gap:16,justifyContent:"center",marginTop:8}}>
            {fwdSyms.map((sym,si)=>(
              <div key={sym} style={{display:"flex",alignItems:"center",gap:4,fontSize:10}}>
                <div style={{width:12,height:3,borderRadius:2,background:["#3b82f6","#ef4444","#22c55e","#f59e0b","#a855f7"][si%5]}}/>
                <span style={{color:C.textDim}}>{sym} (% vs front)</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {futures && (
        <div style={{marginTop:28}}>
          <SectionTitle>?? Contratos Futuros — Preços por Vencimento</SectionTitle>
          <div style={{fontSize:10,color:C.textMuted,marginBottom:12}}>
            Todos os vencimentos em negociação | Fonte: Yahoo Finance / Stooq | {Object.keys(futures.commodities||{}).length} commodities
          </div>
          {Object.entries(futures.commodities||{}).map(([sym,fc])=>{
            const nm=COMMODITIES.find(c=>c.sym===sym)?.name||sym;
            const contracts=fc.contracts||[];
            if(!contracts.length) return null;
            return (
              <div key={sym} style={{marginBottom:16}}>
                <div style={{fontSize:11,fontWeight:700,color:C.text,marginBottom:6,padding:"6px 0",borderBottom:`1px solid ${C.border}`}}>
                  {sym} — {nm} <span style={{color:C.textMuted,fontWeight:400}}>({fc.unit||""} | {fc.exchange||""})</span>
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
                            <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"monospace",fontWeight:700}}>{ct.close?.toFixed(2)||"—"}</td>
                            <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"monospace",color:C.textDim}}>{ct.open?.toFixed(2)||"—"}</td>
                            <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"monospace",color:C.textDim}}>{ct.high?.toFixed(2)||"—"}</td>
                            <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"monospace",color:C.textDim}}>{ct.low?.toFixed(2)||"—"}</td>
                            <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"monospace",color:C.textMuted}}>{ct.volume?.toLocaleString()||"—"}</td>
                            <td style={{padding:"6px 8px",textAlign:"right",fontFamily:"monospace",color:spr?(spr.spread>=0?C.amber:C.cyan):C.textMuted}}>
                              {spr?(spr.spread>=0?"+":"")+spr.spread.toFixed(2):"—"}
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
    const sorted=[...spreadList].sort((a,b)=>{
      const ord={alerta:0,atencao:1,ok:2};
      return (ord[getAlertLevel({...a,key:a.key})]??2)-(ord[getAlertLevel({...b,key:b.key})]??2);
    });

    return (
    <div>
      <SectionTitle>Relações de Preço — O que está caro ou barato?</SectionTitle>
      <div style={{fontSize:13,color:C.textMuted,marginBottom:20,lineHeight:1.6,maxWidth:750}}>
        Estas relações comparam preços entre commodities ligadas entre si.
        Quando uma relação sai do normal, pode indicar oportunidade ou risco.
        <strong style={{color:C.textDim}}> Vermelho = atenção. Verde = tranquilo.</strong>
      </div>

      {/* Summary cards */}
      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:12,marginBottom:24}}>
        <div style={{background:alertCounts.alerta>0?"rgba(239,68,68,0.06)":"rgba(34,197,94,0.04)",
          border:`1px solid ${alertCounts.alerta>0?"rgba(239,68,68,0.2)":"rgba(34,197,94,0.15)"}`,
          borderRadius:12,padding:"16px 20px",textAlign:"center"}}>
          <div style={{fontSize:32,fontWeight:800,color:alertCounts.alerta>0?"#f87171":"#4ade80",fontFamily:"monospace"}}>{alertCounts.alerta}</div>
          <div style={{fontSize:12,fontWeight:600,color:C.textDim}}>?? Pede atenção</div>
        </div>
        <div style={{background:"rgba(245,158,11,0.04)",border:"1px solid rgba(245,158,11,0.15)",
          borderRadius:12,padding:"16px 20px",textAlign:"center"}}>
          <div style={{fontSize:32,fontWeight:800,color:"#fbbf24",fontFamily:"monospace"}}>{alertCounts.atencao}</div>
          <div style={{fontSize:12,fontWeight:600,color:C.textDim}}>?? Fique atento</div>
        </div>
        <div style={{background:"rgba(34,197,94,0.04)",border:"1px solid rgba(34,197,94,0.12)",
          borderRadius:12,padding:"16px 20px",textAlign:"center"}}>
          <div style={{fontSize:32,fontWeight:800,color:"#4ade80",fontFamily:"monospace"}}>{alertCounts.ok}</div>
          <div style={{fontSize:12,fontWeight:600,color:C.textDim}}>? Sem preocupação</div>
        </div>
      </div>

      {spreadList.length>0 ? (
        <div style={{display:"grid",gap:14}}>
          {sorted.map(sp=>{
            const alert=getAlertLevel({...sp,key:sp.key});
            const borderColor=alert==="alerta"?"#ef4444":alert==="atencao"?"#f59e0b":"rgba(59,130,246,0.3)";
            const bgTint=alert==="alerta"?"rgba(239,68,68,0.02)":alert==="atencao"?"rgba(245,158,11,0.02)":"transparent";
            const details=SPREAD_DETAILS[sp.key]||{whatIsIt:"",whyMatters:""};
            const friendlyName=SPREAD_FRIENDLY_NAMES[sp.key]||SPREAD_NAMES[sp.key]||sp.name;
            const verdict=getVerdict({...sp,key:sp.key});
            const zone=getThermometerZone(sp.percentile);
            const trendPct=sp.trend_pct||0;
            const trendColor=Math.abs(trendPct)<3?"#94a3b8":trendPct>0?"#10b981":"#ef4444";
            const trendWord=Math.abs(trendPct)<3?"estável":trendPct>0?"subindo":"caindo";
            const trendArrow=Math.abs(trendPct)<3?"?":trendPct>0?"?":"?";
            const alertBadge=alert==="alerta"
              ?{icon:"??",label:"Atenção!",bg:"rgba(239,68,68,0.1)",border:"rgba(239,68,68,0.35)",color:"#f87171"}
              :alert==="atencao"
              ?{icon:"??",label:"Fique atento",bg:"rgba(245,158,11,0.08)",border:"rgba(245,158,11,0.3)",color:"#fbbf24"}
              :{icon:"?",label:"Sem preocupação",bg:"rgba(34,197,94,0.08)",border:"rgba(34,197,94,0.25)",color:"#4ade80"};

            const thermSegments=[
              {start:0,end:25,color:"#22c55e"},
              {start:25,end:40,color:"#4ade80"},
              {start:40,end:60,color:"#94a3b8"},
              {start:60,end:75,color:"#fbbf24"},
              {start:75,end:100,color:"#ef4444"},
            ];
            const markerPos=Math.min(98,Math.max(2,sp.percentile));

            return (
              <div key={sp.key} style={{background:bgTint,borderRadius:"0 12px 12px 0",padding:"20px 24px",
                border:`1px solid ${C.border}`,borderLeft:`4px solid ${borderColor}`}}>
                {/* Header */}
                <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:14}}>
                  <h3 style={{margin:0,fontSize:17,fontWeight:700,color:C.text,lineHeight:1.3}}>{friendlyName}</h3>
                  <span style={{display:"inline-flex",alignItems:"center",gap:6,padding:"5px 14px",borderRadius:20,
                    background:alertBadge.bg,border:`1px solid ${alertBadge.border}`}}>
                    <span style={{fontSize:13}}>{alertBadge.icon}</span>
                    <span style={{fontSize:11,fontWeight:700,color:alertBadge.color}}>{alertBadge.label}</span>
                  </span>
                </div>

                {/* 3-column layout */}
                <div style={{display:"grid",gridTemplateColumns:"180px 1fr 240px",gap:24,alignItems:"center",marginBottom:14}}>
                  {/* Value */}
                  <div>
                    <div style={{fontSize:9,color:C.textMuted,marginBottom:4,fontWeight:600,letterSpacing:1}}>VALOR ATUAL</div>
                    <div style={{display:"flex",alignItems:"baseline",gap:8}}>
                      <span style={{fontSize:28,fontWeight:800,fontFamily:"monospace",color:C.text,letterSpacing:-1}}>
                        {sp.current.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:4})}
                      </span>
                      <span style={{fontSize:11,color:C.textMuted}}>{sp.unit}</span>
                    </div>
                    <div style={{display:"flex",alignItems:"center",gap:6,marginTop:6}}>
                      <span style={{fontSize:14,color:trendColor}}>{trendArrow}</span>
                      <span style={{fontSize:12,color:trendColor,fontWeight:600}}>
                        {trendWord} ({trendPct>0?"+":""}{trendPct.toFixed(1)}%)
                      </span>
                    </div>
                  </div>

                  {/* Thermometer */}
                  <div>
                    <div style={{fontSize:9,color:C.textMuted,marginBottom:6,fontWeight:600,letterSpacing:1}}>COMPARADO AO ÚLTIMO ANO</div>
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:6}}>
                      <span style={{fontSize:13,fontWeight:700,color:zone.color}}>{zone.text}</span>
                      <span style={{fontSize:11,color:C.textMuted,fontFamily:"monospace"}}>posição: {sp.percentile}%</span>
                    </div>
                    <div style={{position:"relative",height:20,borderRadius:10,overflow:"hidden",display:"flex"}}>
                      {thermSegments.map((seg,i)=>(
                        <div key={i} style={{flex:seg.end-seg.start,background:seg.color+"20",
                          borderRight:i<thermSegments.length-1?"1px solid rgba(0,0,0,0.3)":"none"}} />
                      ))}
                      <div style={{position:"absolute",top:-3,bottom:-3,
                        left:`calc(${markerPos}% - 4px)`,width:8,borderRadius:4,
                        background:zone.color,boxShadow:`0 0 10px ${zone.color}90, 0 0 20px ${zone.color}40`,
                        transition:"left 0.6s ease"}} />
                    </div>
                    <div style={{display:"flex",justifyContent:"space-between",marginTop:4,fontSize:10,color:"rgba(255,255,255,0.25)"}}>
                      <span>Barato</span><span>Médio</span><span>Caro</span>
                    </div>
                  </div>

                  {/* Sparkline */}
                  <div>
                    <div style={{fontSize:9,color:C.textMuted,marginBottom:6,fontWeight:600,letterSpacing:1}}>TENDÊNCIA (20 DIAS)</div>
                    {sp.history && sp.history.length>0 ? (
                      <SpreadChart history={sp.history} regime={sp.regime} />
                    ) : <div style={{color:C.textMuted,fontSize:11}}>Sem histórico</div>}
                  </div>
                </div>

                {/* Verdict */}
                <div style={{padding:"10px 14px",borderRadius:8,marginBottom:10,
                  background:alert==="alerta"?"rgba(239,68,68,0.06)":alert==="atencao"?"rgba(245,158,11,0.06)":"rgba(255,255,255,0.02)",
                  border:`1px solid ${alert==="alerta"?"rgba(239,68,68,0.15)":alert==="atencao"?"rgba(245,158,11,0.15)":"rgba(255,255,255,0.05)"}`}}>
                  <div style={{fontSize:10,fontWeight:700,color:C.textDim,marginBottom:4,letterSpacing:0.5}}>?? RESUMO</div>
                  <div style={{fontSize:13,color:C.textDim,lineHeight:1.5}}>{verdict}</div>
                </div>

                {/* Expandable explanation */}
                {details.whatIsIt && (
                  <details style={{cursor:"pointer"}}>
                    <summary style={{fontSize:12,color:C.textMuted,padding:"4px 0",listStyle:"none",display:"flex",alignItems:"center",gap:4}}>
                      <span style={{fontSize:10}}>?</span> O que é isso? Como me afeta?
                    </summary>
                    <div style={{marginTop:8,padding:14,borderRadius:8,
                      background:"rgba(59,130,246,0.05)",border:"1px solid rgba(59,130,246,0.12)"}}>
                      <div style={{marginBottom:10}}>
                        <div style={{fontSize:10,fontWeight:700,color:"#60a5fa",marginBottom:4,letterSpacing:0.5}}>?? O QUE É</div>
                        <div style={{fontSize:12,color:C.textDim,lineHeight:1.6}}>{details.whatIsIt}</div>
                      </div>
                      <div>
                        <div style={{fontSize:10,fontWeight:700,color:"#fbbf24",marginBottom:4,letterSpacing:0.5}}>?? POR QUE ME INTERESSA</div>
                        <div style={{fontSize:12,color:C.textDim,lineHeight:1.6}}>{details.whyMatters}</div>
                      </div>
                    </div>
                  </details>
                )}
              </div>
            );
          })}
        </div>
      ) : (
        <DataPlaceholder title="Sem dados de spreads" detail="Execute o pipeline para calcular spreads" />
      )}

      {/* Help footer */}
      <div style={{marginTop:28,padding:"16px 20px",borderRadius:12,
        background:"rgba(255,255,255,0.02)",border:`1px solid ${C.border}`}}>
        <div style={{fontSize:12,fontWeight:700,color:C.textDim,marginBottom:10}}>?? Como ler esta página</div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:20,fontSize:12,color:C.textMuted,lineHeight:1.6}}>
          <div><strong style={{color:C.textDim}}>Barra "Caro/Barato"</strong><br/>Mostra onde o preço está comparado ao último ano. Bolinha na ponta vermelha = caro. Na verde = barato.</div>
          <div><strong style={{color:C.textDim}}>Gráfico de tendência</strong><br/>Mostra pra onde o preço está indo nos últimos 20 dias. Seta ? = subindo, ? = caindo.</div>
          <div><strong style={{color:C.textDim}}>Clique "O que é isso?"</strong><br/>Cada relação tem uma explicação simples do que significa e por que interessa ao produtor rural.</div>
        </div>
      </div>
    </div>
    );
  };

  // -- Tab: Sazonalidade ---------------------------------------------------
  const renderSazonalidade = () => (
    <div>
      <SectionTitle>?? Sazonalidade — {selected} ({COMMODITIES.find(c=>c.sym===selected)?.name})</SectionTitle>
      {season && season[selected] ? (
        <SeasonChart entry={season[selected]} />
      ) : (
        <DataPlaceholder title="Sem dados" detail={`${selected} não encontrado em seasonality.json`} />
      )}
      <div style={{marginTop:24}}>
        <SectionTitle>?? Preço Atual vs Média Histórica (5 Anos)</SectionTitle>
        <div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
            <thead><TableHeader cols={["Commodity","Atual","Média 5Y","Desvio %","Sinal"]} /></thead>
            <tbody>
              {COMMODITIES.map(c=>{
                const sd=getSeasonDev(c.sym);
                if(!sd) return (
                  <tr key={c.sym} style={{borderBottom:`1px solid ${C.border}`}}>
                    <td style={{padding:"8px 12px",fontWeight:600}}>{c.sym} <span style={{color:C.textMuted,fontWeight:400}}>({c.name})</span></td>
                    <td colSpan={4} style={{padding:"8px 12px",color:C.textMuted,textAlign:"center"}}>—</td>
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
    const realStocks = stocksList.filter(s=>s.data_available?.stock_real);
    const aperto = stocksList.filter(s=>s.state.includes("APERTO")||s.state==="PRECO_ELEVADO"||s.state==="PRECO_DEPRIMIDO");
    const excesso = stocksList.filter(s=>s.state.includes("EXCESSO")||s.state==="PRECO_ACIMA_MEDIA"||s.state==="PRECO_ABAIXO_MEDIA");
    const neutro = stocksList.filter(s=>s.state==="NEUTRO"||s.state==="PRECO_NEUTRO");
    const selStock = stocks?.commodities?.[stockSelected];
    const selNm = COMMODITIES.find(c=>c.sym===stockSelected)?.name||stockSelected;

    return (
      <div>
        <SectionTitle>?? Stocks Watch — Estoques Reais USDA + Análise de Preço</SectionTitle>

        {/* -- GRAFICO PRINCIPAL - TOPO -- */}
        {realStocks.length > 0 && (
          <div style={{marginBottom:24}}>
            {/* Commodity selector */}
            <div style={{display:"flex",gap:6,flexWrap:"wrap",marginBottom:12}}>
              {realStocks.map(st=>{
                const nm=COMMODITIES.find(c=>c.sym===st.symbol)?.name||st.symbol;
                const isSel=stockSelected===st.symbol;
                return (
                  <button key={st.symbol} onClick={()=>setStockSelected(st.symbol)} style={{
                    padding:"6px 14px",fontSize:10,fontWeight:isSel?700:500,borderRadius:4,cursor:"pointer",
                    background:isSel?"rgba(59,130,246,.15)":"transparent",
                    color:isSel?C.blue:C.textMuted,border:`1px solid ${isSel?C.blue:C.border}`,
                    transition:"all .15s"
                  }}>{st.symbol} <span style={{fontSize:9,color:isSel?C.blue:C.textMuted}}>({nm})</span></button>
                );
              })}
            </div>

            {/* Stock chart */}
            {(()=>{
              if(!selStock?.stock_history?.length) return <div style={{padding:20,textAlign:"center",color:C.textMuted,fontSize:11}}>Sem histórico de estoque para {stockSelected}</div>;
              const hist=selStock.stock_history;
              const avg=selStock.stock_avg||0;

              // Normalize periods and deduplicate
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
              const maxVal=Math.max(...allVals,avg)*1.1;
              const minVal=Math.min(...allVals)*0.9;
              const range=maxVal-minVal||1;

              const W=900,H=300;
              const pad={l:60,r:30,t:25,b:35};
              const chartW=W-pad.l-pad.r;
              const chartH=H-pad.t-pad.b;
              const xStep=periods.length>1?chartW/(periods.length-1):chartW;
              const yC=(v:number)=>pad.t+chartH*(1-(v-minVal)/range);

              const colorList=["#3b82f6","#8b5cf6","#ec4899","#f59e0b","#22c55e","#06b6d4"];
              const yearColors:Record<number,string>={};
              years.forEach((y,i)=>{yearColors[y]=colorList[i%colorList.length];});

              const dev=selStock.stock_avg&&selStock.stock_avg>0?((selStock.stock_current-selStock.stock_avg)/selStock.stock_avg*100):0;
              const devColor=Math.abs(dev)>15?C.red:Math.abs(dev)>5?C.amber:C.green;

              return (
                <div style={{background:C.panelAlt,borderRadius:8,padding:20,border:`1px solid ${C.border}`}}>
                  {/* Header with stats */}
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:16}}>
                    <div>
                      <div style={{fontSize:14,fontWeight:800,color:C.text}}>{stockSelected} — {selNm}</div>
                      <div style={{fontSize:10,color:C.textMuted,marginTop:2}}>Estoque Trimestral ({selStock.stock_unit||""}) | Fonte: USDA QuickStats</div>
                    </div>
                    <div style={{display:"flex",gap:16}}>
                      <div style={{textAlign:"right"}}>
                        <div style={{fontSize:9,color:C.textMuted}}>ATUAL</div>
                        <div style={{fontSize:18,fontWeight:800,fontFamily:"monospace",color:C.text}}>{selStock.stock_current?.toFixed(2)}</div>
                      </div>
                      <div style={{textAlign:"right"}}>
                        <div style={{fontSize:9,color:C.textMuted}}>MÉDIA</div>
                        <div style={{fontSize:18,fontWeight:800,fontFamily:"monospace",color:C.textDim}}>{selStock.stock_avg?.toFixed(2)}</div>
                      </div>
                      <div style={{textAlign:"right"}}>
                        <div style={{fontSize:9,color:C.textMuted}}>DESVIO</div>
                        <div style={{fontSize:18,fontWeight:800,fontFamily:"monospace",color:devColor}}>{dev>=0?"+":""}{dev.toFixed(1)}%</div>
                      </div>
                    </div>
                  </div>

                  {/* Legend */}
                  <div style={{display:"flex",gap:14,marginBottom:10,flexWrap:"wrap"}}>
                    {years.map(y=>(
                      <div key={y} style={{display:"flex",alignItems:"center",gap:4}}>
                        <div style={{width:20,height:3,borderRadius:2,background:yearColors[y]}}/>
                        <span style={{fontSize:10,color:yearColors[y],fontWeight:600}}>{y}</span>
                      </div>
                    ))}
                    <div style={{display:"flex",alignItems:"center",gap:4}}>
                      <div style={{width:20,height:0,borderTop:"2px dashed "+C.amber}}/>
                      <span style={{fontSize:10,color:C.amber,fontWeight:600}}>Média ({avg.toFixed(1)})</span>
                    </div>
                  </div>

                  {/* Bar Chart */}
                  {(()=>{
                    const nYears=years.length;
                    const nPeriods=periods.length;
                    if(!nPeriods||!nYears) return null;
                    const groupW=chartW/nPeriods;
                    const barGap=2;
                    const barW=Math.max(4,Math.min(20,(groupW-barGap*(nYears+1))/nYears));
                    const groupTotalW=nYears*barW+(nYears-1)*barGap;
                    const groupOffset=(groupW-groupTotalW)/2;
                    const zeroY=yC(minVal);
                    return (
                      <svg width={W} height={H} style={{display:"block",width:"100%"}} viewBox={"0 0 "+W+" "+H}>
                        {/* Grid */}
                        {[0,0.25,0.5,0.75,1].map(pct=>{
                          const gy=pad.t+chartH*(1-pct);const val=minVal+range*pct;
                          return (<g key={"g"+pct}><line x1={pad.l} y1={gy} x2={W-pad.r} y2={gy} stroke="rgba(148,163,184,.08)" strokeWidth={1}/><text x={pad.l-8} y={gy+3} fill={C.textMuted} fontSize={9} textAnchor="end" fontFamily="monospace">{val>=100?val.toFixed(0):val.toFixed(1)}</text></g>);
                        })}
                        {/* Average line */}
                        <line x1={pad.l} y1={yC(avg)} x2={W-pad.r} y2={yC(avg)} stroke={C.amber} strokeWidth={1.5} strokeDasharray="6,4"/>
                        {/* X axis labels */}
                        {periods.map((per,pi)=>(<text key={per} x={pad.l+pi*groupW+groupW/2} y={H-pad.b+16} fill={C.textMuted} fontSize={10} textAnchor="middle" fontFamily="monospace" fontWeight="600">{per}</text>))}
                        {/* Bars */}
                        {periods.map((per,pi)=>(
                          <g key={per}>
                            {years.map((yr,yi)=>{
                              const entry=yearData[yr]?.find(d=>d.period===per);
                              if(!entry) return null;
                              const bx=pad.l+pi*groupW+groupOffset+yi*(barW+barGap);
                              const by=yC(entry.value);
                              const bh=zeroY-by;
                              return (
                                <g key={yr}>
                                  <rect x={bx} y={by} width={barW} height={Math.max(1,bh)} rx={2} fill={yearColors[yr]} opacity={0.85}/>
                                  <text x={bx+barW/2} y={by-4} fill={yearColors[yr]} fontSize={8} textAnchor="middle" fontFamily="monospace" fontWeight="bold">{entry.value>=100?entry.value.toFixed(0):entry.value.toFixed(1)}</text>
                                </g>
                              );
                            })}
                          </g>
                        ))}
                      </svg>
                    );
                  })()}
                </div>
              );
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
            <div style={{fontSize:12,fontWeight:700,color:C.textDim,marginBottom:8}}>?? Estoque Real — USDA QuickStats</div>
            <div style={{overflowX:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                <thead><TableHeader cols={["Commodity","Estoque Atual","Média","Unidade","Desvio %","Estado","Tendência","Período"]} /></thead>
                <tbody>
                  {realStocks.map(st=>{
                    const nm=COMMODITIES.find(c=>c.sym===st.symbol)?.name||st.symbol;
                    const dev=st.stock_avg&&st.stock_avg>0?((st.stock_current-st.stock_avg)/st.stock_avg*100):0;
                    const devCol=Math.abs(dev)>15?C.red:Math.abs(dev)>5?C.amber:C.green;
                    const isSel=stockSelected===st.symbol;
                    return (
                      <tr key={st.symbol+"_real"} onClick={()=>setStockSelected(st.symbol)} style={{borderBottom:`1px solid ${C.border}`,cursor:"pointer",background:isSel?"rgba(59,130,246,.06)":"transparent"}}>
                        <td style={{padding:"8px 12px",fontWeight:700}}>{st.symbol} <span style={{color:C.textMuted,fontWeight:400}}>({nm})</span></td>
                        <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",fontWeight:700,fontSize:12}}>{st.stock_current?.toFixed(2)||"—"}</td>
                        <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",color:C.textDim}}>{st.stock_avg?.toFixed(2)||"—"}</td>
                        <td style={{padding:"8px 12px",textAlign:"center",fontSize:10,color:C.textMuted}}>{st.stock_unit||"—"}</td>
                        <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",fontWeight:700,color:devCol}}>{dev>=0?"+":""}{dev.toFixed(1)}%</td>
                        <td style={{padding:"8px 12px",textAlign:"center"}}><Badge label={st.state.replace(/_/g," ")} color={st.state.includes("APERTO")?C.red:st.state.includes("EXCESSO")?C.green:C.amber} /></td>
                        <td style={{padding:"8px 12px",textAlign:"center",fontSize:9,color:C.textMuted}}>{st.factors?.find((f:string)=>f.includes("Tendencia"))?.replace("Tendencia: ","")||"—"}</td>
                        <td style={{padding:"8px 12px",textAlign:"right",fontSize:9,color:C.textMuted}}>{st.factors?.find((f:string)=>f.includes("Dado mais recente"))?.replace("Dado mais recente: ","")||"—"}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* -- Tabela de Proxy (Preço vs Média) -- */}
        <div>
          <div style={{fontSize:12,fontWeight:700,color:C.textDim,marginBottom:8}}>?? Proxy de Preço — Sem Dados de Estoque Real</div>
          <div style={{overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead><TableHeader cols={["Commodity","Preço","Desvio vs Média 5Y","Fonte","Estado","Fatores"]} /></thead>
              <tbody>
                {stocksList.filter(s=>!s.data_available?.stock_real).map(st=>{
                  const nm=COMMODITIES.find(c=>c.sym===st.symbol)?.name||st.symbol;
                  return (
                    <tr key={st.symbol+"_proxy"} style={{borderBottom:`1px solid ${C.border}`}}>
                      <td style={{padding:"8px 12px",fontWeight:600}}>{st.symbol} <span style={{color:C.textMuted,fontWeight:400}}>({nm})</span></td>
                      <td style={{padding:"8px 12px",textAlign:"right",fontFamily:"monospace",fontWeight:700}}>{st.price?.toFixed(2)||"—"}</td>
                      <td style={{padding:"8px 12px",textAlign:"right"}}><DevBar val={st.price_vs_avg} /></td>
                      <td style={{padding:"8px 12px",textAlign:"center"}}><Badge label="PROXY" color={C.textMuted} /></td>
                      <td style={{padding:"8px 12px",textAlign:"center"}}><Badge label={st.state.replace(/_/g," ")} color={st.state.includes("ELEVADO")||st.state.includes("DEPRIMIDO")?C.red:st.state.includes("ACIMA")||st.state.includes("ABAIXO")?C.amber:C.green} /></td>
                      <td style={{padding:"8px 12px",fontSize:9,color:C.textMuted}}>{st.factors?.join("; ")||"—"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    );
  };
  // -- Tab: Custo Produção ------------------------------------------------
  const renderCustoProducao = () => {
    const allRegions:{sym:string;commodity:string;region:string;cost:number;unit:string;price:number|null;margin:number|null;source:string}[] = [];
    COST_DATA.forEach(cd=>{
      const p=getPrice(cd.sym);
      cd.regions.forEach(r=>{
        const margin = p ? ((p - r.cost) / r.cost) * 100 : null;
        allRegions.push({sym:cd.sym,commodity:cd.commodity,region:r.region,cost:r.cost,unit:r.unit,price:p,margin,source:r.source});
      });
    });
    const prejuizo = allRegions.filter(r=>r.margin!==null && r.margin<0).length;
    const apertado = allRegions.filter(r=>r.margin!==null && r.margin>=0 && r.margin<=10).length;
    const lucro = allRegions.filter(r=>r.margin!==null && r.margin>10).length;

    return (
    <div>
      <SectionTitle>Custo de Produção — Quem lucra e quem perde?</SectionTitle>
      <div style={{fontSize:13,color:C.textMuted,marginBottom:20,lineHeight:1.6,maxWidth:750}}>
        Compara o preço atual de mercado com o custo de produção em cada região.
        <strong style={{color:C.textDim}}> Barra verde = lucro. Amarela = apertado. Vermelha = prejuízo.</strong>
      </div>

      {/* Summary cards */}
      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:12,marginBottom:24}}>
        <div style={{background:prejuizo>0?"rgba(239,68,68,0.06)":"rgba(34,197,94,0.04)",
          border:`1px solid ${prejuizo>0?"rgba(239,68,68,0.2)":"rgba(34,197,94,0.15)"}`,
          borderRadius:12,padding:"16px 20px",textAlign:"center"}}>
          <div style={{fontSize:32,fontWeight:800,color:prejuizo>0?"#f87171":"#4ade80",fontFamily:"monospace"}}>{prejuizo}</div>
          <div style={{fontSize:12,fontWeight:600,color:C.textDim}}>?? No prejuízo</div>
        </div>
        <div style={{background:"rgba(245,158,11,0.04)",border:"1px solid rgba(245,158,11,0.15)",
          borderRadius:12,padding:"16px 20px",textAlign:"center"}}>
          <div style={{fontSize:32,fontWeight:800,color:"#fbbf24",fontFamily:"monospace"}}>{apertado}</div>
          <div style={{fontSize:12,fontWeight:600,color:C.textDim}}>?? Margem apertada</div>
        </div>
        <div style={{background:"rgba(34,197,94,0.04)",border:"1px solid rgba(34,197,94,0.12)",
          borderRadius:12,padding:"16px 20px",textAlign:"center"}}>
          <div style={{fontSize:32,fontWeight:800,color:"#4ade80",fontFamily:"monospace"}}>{lucro}</div>
          <div style={{fontSize:12,fontWeight:600,color:C.textDim}}>?? Lucrando</div>
        </div>
      </div>

      {COST_DATA.map(cd=>{
        const p=getPrice(cd.sym);
        const margins = cd.regions.map(r=>p ? ((p - r.cost) / r.cost) * 100 : null);
        const hasPrejuizo = margins.some(m=>m!==null && m<0);
        const allLucro = margins.every(m=>m!==null && m>0);
        const bestRegion = cd.regions.reduce((best,r,i)=>
          (margins[i]!==null && (best.margin===null || margins[i]!>best.margin)) ? {region:r.region,margin:margins[i]!} : best,
          {region:"",margin:null as number|null}
        );
        const worstRegion = cd.regions.reduce((worst,r,i)=>
          (margins[i]!==null && (worst.margin===null || margins[i]!<worst.margin)) ? {region:r.region,margin:margins[i]!} : worst,
          {region:"",margin:null as number|null}
        );

        const headerBorder = hasPrejuizo?"#ef4444":allLucro?"#10b981":"#f59e0b";
        const headerBg = hasPrejuizo?"rgba(239,68,68,0.03)":allLucro?"rgba(16,185,129,0.03)":"transparent";
        const alertBadge = hasPrejuizo
          ?{icon:"??",label:"Regiões no prejuízo",bg:"rgba(239,68,68,0.1)",border:"rgba(239,68,68,0.35)",color:"#f87171"}
          :allLucro
          ?{icon:"??",label:"Todas lucrando",bg:"rgba(16,185,129,0.1)",border:"rgba(16,185,129,0.3)",color:"#34d399"}
          :{icon:"??",label:"Margens apertadas",bg:"rgba(245,158,11,0.08)",border:"rgba(245,158,11,0.3)",color:"#fbbf24"};

        return (
          <div key={cd.sym} style={{marginBottom:16,background:headerBg,borderRadius:"0 12px 12px 0",
            padding:"20px 24px",border:`1px solid ${C.border}`,borderLeft:`4px solid ${headerBorder}`}}>

            {/* Header */}
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:16}}>
              <div>
                <h3 style={{margin:0,fontSize:17,fontWeight:700,color:C.text,lineHeight:1.3}}>{cd.commodity}</h3>
                {p && <div style={{fontSize:13,color:C.textMuted,marginTop:4}}>
                  Preço de mercado: <strong style={{color:C.text,fontFamily:"monospace",fontSize:15}}>
                    {p.toLocaleString("en-US",{minimumFractionDigits:2})}
                  </strong> <span style={{fontSize:11}}>{cd.futuresUnit}</span>
                </div>}
              </div>
              <span style={{display:"inline-flex",alignItems:"center",gap:6,padding:"5px 14px",borderRadius:20,
                background:alertBadge.bg,border:`1px solid ${alertBadge.border}`}}>
                <span style={{fontSize:13}}>{alertBadge.icon}</span>
                <span style={{fontSize:11,fontWeight:700,color:alertBadge.color}}>{alertBadge.label}</span>
              </span>
            </div>

            {/* Regions as visual cards */}
            <div style={{display:"grid",gap:10}}>
              {cd.regions.map((r,i)=>{
                const margin = p ? ((p - r.cost) / r.cost) * 100 : null;
                const isBest = bestRegion.region === r.region;
                const isWorst = worstRegion.region === r.region && hasPrejuizo;
                return (
                  <div key={i} style={{display:"grid",gridTemplateColumns:"220px 120px 1fr",gap:16,alignItems:"center",
                    padding:"12px 16px",borderRadius:10,
                    background:isWorst?"rgba(239,68,68,0.04)":isBest?"rgba(16,185,129,0.04)":"rgba(255,255,255,0.02)",
                    border:`1px solid ${isWorst?"rgba(239,68,68,0.15)":isBest?"rgba(16,185,129,0.12)":"rgba(255,255,255,0.04)"}`}}>

                    {/* Region name */}
                    <div>
                      <div style={{fontSize:14,fontWeight:600,color:C.text}}>{r.region}</div>
                      <div style={{fontSize:10,color:C.textMuted,marginTop:2}}>Fonte: {r.source}</div>
                    </div>

                    {/* Cost */}
                    <div style={{textAlign:"right"}}>
                      <div style={{fontSize:9,color:C.textMuted,fontWeight:600,letterSpacing:1,marginBottom:2}}>CUSTO</div>
                      <div style={{fontSize:16,fontWeight:700,fontFamily:"monospace",color:C.textDim}}>
                        {r.cost.toLocaleString("en-US",{minimumFractionDigits:1})}
                      </div>
                      <div style={{fontSize:10,color:C.textMuted}}>{r.unit}</div>
                    </div>

                    {/* Margin bar */}
                    {p ? <MarginBar price={p} cost={r.cost} /> : <span style={{color:C.textMuted,fontSize:12}}>Sem preço</span>}
                  </div>
                );
              })}
            </div>

            {/* Verdict */}
            <div style={{marginTop:14,padding:"10px 14px",borderRadius:8,
              background:hasPrejuizo?"rgba(239,68,68,0.06)":allLucro?"rgba(16,185,129,0.05)":"rgba(245,158,11,0.05)",
              border:`1px solid ${hasPrejuizo?"rgba(239,68,68,0.15)":allLucro?"rgba(16,185,129,0.12)":"rgba(245,158,11,0.12)"}`}}>
              <div style={{fontSize:10,fontWeight:700,color:C.textDim,marginBottom:4,letterSpacing:0.5}}>?? RESUMO</div>
              <div style={{fontSize:13,color:C.textDim,lineHeight:1.5}}>
                {hasPrejuizo && worstRegion.margin!==null && bestRegion.margin!==null
                  ? `?? ${worstRegion.region.replace(/^.{4}/,"")} está no prejuízo (${worstRegion.margin.toFixed(0)}%). O produtor mais competitivo é ${bestRegion.region.replace(/^.{4}/,"")} com margem de +${bestRegion.margin.toFixed(0)}%. Preço precisa subir para viabilizar todas as regiões.`
                  : allLucro && bestRegion.margin!==null && worstRegion.margin!==null
                  ? `?? Todas as regiões lucrando. Melhor margem: ${bestRegion.region.replace(/^.{4}/,"")} (+${bestRegion.margin.toFixed(0)}%). Margem mais apertada: ${worstRegion.region.replace(/^.{4}/,"")} (+${(worstRegion.margin).toFixed(0)}%).`
                  : bestRegion.margin!==null && worstRegion.margin!==null
                  ? `?? Margens apertadas em algumas regiões. ${bestRegion.region.replace(/^.{4}/,"")} lidera com +${bestRegion.margin.toFixed(0)}%. Atenção para regiões com margem abaixo de 10%.`
                  : "Dados de preço indisponíveis para cálculo."
                }
              </div>
            </div>

            {/* Sources */}
            <div style={{marginTop:8,fontSize:9,color:C.textMuted}}>
              Fontes: {cd.regions.map(r=>r.source).filter((v,i,a)=>a.indexOf(v)===i).join(", ")}
            </div>
          </div>
        );
      })}

      {/* Help footer */}
      <div style={{marginTop:28,padding:"16px 20px",borderRadius:12,
        background:"rgba(255,255,255,0.02)",border:`1px solid ${C.border}`}}>
        <div style={{fontSize:12,fontWeight:700,color:C.textDim,marginBottom:10}}>?? Como ler esta página</div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:20,fontSize:12,color:C.textMuted,lineHeight:1.6}}>
          <div><strong style={{color:C.textDim}}>Barra de margem</strong><br/>Verde = lucro bom (acima de 20%). Amarela = margem apertada. Vermelha = produtor está perdendo dinheiro.</div>
          <div><strong style={{color:C.textDim}}>Custo por região</strong><br/>Quanto custa produzir em cada local. Regiões com custo mais baixo conseguem lucrar mesmo com preços em queda.</div>
          <div><strong style={{color:C.textDim}}>Resumo</strong><br/>Mostra quem está na melhor e pior situação. Se muitas regiões estão no prejuízo, a oferta tende a cair — o que pode subir o preço.</div>
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
      if(!data||data.length<2) return <span style={{color:C.textMuted,fontSize:11}}>—</span>;
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
      if(val===null||val===undefined) return <span style={{color:C.textMuted,fontSize:10}}>—</span>;
      const color = val>0?"#10b981":val<0?"#ef4444":"#94a3b8";
      const arrow = val>0?"?":val<0?"?":"?";
      return <span style={{fontSize:11,fontWeight:600,color}}>{arrow} {val>0?"+":""}{val.toFixed(1)}% {suffix}</span>;
    };

    // -- Data cards config --
    const priceCards = [
      {id:"wti_spot",label:"Petróleo WTI",icon:"???",color:"#f59e0b",
        why:"Preço do petróleo afeta o custo do diesel, frete e insumos agrícolas. Petróleo em alta = custo do produtor sobe."},
      {id:"natural_gas_spot",label:"Gás Natural (Henry Hub)",icon:"??",color:"#3b82f6",
        why:"Gás natural é matéria-prima de fertilizantes nitrogenados (ureia). Gás caro = adubo caro."},
      {id:"diesel_retail",label:"Diesel (Preço Bomba EUA)",icon:"?",color:"#ef4444",
        why:"Diesel é o principal combustível do campo — colheitadeiras, caminhões, irrigação. Cada centavo afeta a margem do produtor."},
      {id:"gasoline_retail",label:"Gasolina (Preço Bomba EUA)",icon:"??",color:"#a855f7",
        why:"Gasolina alta incentiva etanol de milho, aumentando demanda por milho e puxando preços agrícolas."},
    ];

    const stockCards = [
      {id:"crude_stocks",label:"Estoques Petróleo Cru",icon:"???",color:"#f59e0b",unit:"MBbl",
        why:"Estoques altos = petróleo tende a cair = diesel mais barato = custo agrícola menor. Estoques baixos = risco de alta."},
      {id:"gasoline_stocks",label:"Estoques Gasolina",icon:"?",color:"#a855f7",unit:"MBbl",
        why:"Estoques baixos de gasolina = mais demanda por etanol = mais demanda por milho = milho sobe."},
      {id:"distillate_stocks",label:"Estoques Diesel/Destilados",icon:"??",color:"#ef4444",unit:"MBbl",
        why:"Estoque de diesel apertado = risco de frete caro na colheita. Produtor deve monitorar antes de contratar transporte."},
      {id:"ethanol_stocks",label:"Estoques Etanol",icon:"??",color:"#22c55e",unit:"MBbl",
        why:"Etanol consome ~40% do milho americano. Estoques baixos de etanol = usinas precisam comprar mais milho."},
    ];

    const prodCards = [
      {id:"ethanol_production",label:"Produção Etanol",icon:"??",color:"#22c55e",unit:"MBbl/d",
        why:"Produção alta de etanol = forte demanda por milho. Queda na produção = demanda enfraquecendo."},
      {id:"refinery_utilization",label:"Utilização Refinarias",icon:"??",color:"#f59e0b",unit:"%",
        why:"Refinarias a pleno = demanda forte por combustíveis. Se cair abaixo de 85%, sinal de desaceleração econômica."},
      {id:"crude_production",label:"Produção Petróleo EUA",icon:"????",color:"#3b82f6",unit:"MBbl/d",
        why:"EUA é o maior produtor mundial. Produção recorde = petróleo tende a cair = diesel mais barato para o campo."},
    ];

    if(!eiaData) return (
      <div>
        <SectionTitle>Energia — Petróleo, Gás e Combustíveis</SectionTitle>
        <DataPlaceholder title="Sem dados EIA" detail="Execute o pipeline para coletar dados da EIA (Energy Information Administration)" />
      </div>
    );

    // Count alerts
    const alertCount = Object.values(s).filter(v=>v.pct_range_52w!==null&&(v.pct_range_52w>=80||v.pct_range_52w<=20)).length;

    return (
    <div>
      <SectionTitle>Energia — Petróleo, Gás e Combustíveis</SectionTitle>
      <div style={{fontSize:13,color:C.textMuted,marginBottom:20,lineHeight:1.6,maxWidth:750}}>
        Dados semanais da EIA (Energy Information Administration). Energia afeta diretamente o custo do produtor rural
        — diesel, frete, fertilizantes e demanda por etanol.
        <strong style={{color:C.textDim}}> Atualizado: {eiaData.metadata.collected_at?.slice(0,10)||"—"}</strong>
      </div>

      {/* -- PREÇOS -- */}
      <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:12}}>?? Preços — Quanto custa a energia?</div>
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
                  <div style={{fontSize:11,color:C.textMuted}}>{d.unit} • {d.latest_period}</div>
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
      <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:12}}>?? Estoques — Quanto tem guardado?</div>
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
                  background:"rgba(245,158,11,0.1)",border:"1px solid rgba(245,158,11,0.3)",color:"#fbbf24"}}>?? Baixo</span>}
                {isHigh && <span style={{fontSize:10,fontWeight:700,padding:"3px 10px",borderRadius:12,
                  background:"rgba(59,130,246,0.1)",border:"1px solid rgba(59,130,246,0.3)",color:"#60a5fa"}}>?? Alto</span>}
              </div>
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16,alignItems:"center"}}>
                <div>
                  <div style={{fontSize:26,fontWeight:800,fontFamily:"monospace",color:C.text}}>
                    {d.latest_value>=1000?`${(d.latest_value/1000).toFixed(1)}M`:d.latest_value.toLocaleString("en-US")}
                  </div>
                  <div style={{fontSize:11,color:C.textMuted}}>{sc.unit} • {d.latest_period}</div>
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
      <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:12}}>?? Produção e Refino</div>
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
              <div style={{fontSize:11,color:C.textMuted}}>{pc.unit} • {d.latest_period}</div>
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
        <div style={{fontSize:12,fontWeight:700,color:C.textDim,marginBottom:10}}>?? Como a energia afeta o agro</div>
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
      <SectionTitle>???? Mercado Físico EUA — Cash vs Futures</SectionTitle>
      <div style={{fontSize:11,color:C.textMuted,marginBottom:16}}>
        Preços cash USDA (Prices Received) vs front-month futures. Atualizado: {physData?.timestamp?.slice(0,10) || "—"}
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
      <SectionTitle>?? Mercado Físico Internacional</SectionTitle>
      <div style={{fontSize:11,color:C.textMuted,marginBottom:16}}>
        {physIntl ? <>Fontes: CEPEA/ESALQ via Notícias Agrícolas + MAGyP FOB Argentina. Atualizado: {physIntl.timestamp?.slice(0,10) || "—"} | <span style={{color:C.green}}>{physIntl.markets_with_data} com dados</span> / {physIntl.total_markets} total</> : "Carregando dados internacionais..."}
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
                const trendColor = d.trend?.startsWith("+")?C.green:d.trend?.startsWith("-")?C.red:d.trend==="—"?C.textMuted:C.textDim;
                return (
                <tr key={sym} style={{borderBottom:`1px solid ${C.border}`}}>
                  <td style={{padding:"8px 10px",fontWeight:600}}>
                    <span style={{color:C.text}}>{d.label}</span>
                  </td>
                  <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"monospace",fontWeight:700,color:C.text,fontSize:13}}>
                    {typeof d.price==="number"?d.price.toLocaleString("pt-BR",{minimumFractionDigits:2,maximumFractionDigits:2}):"—"}
                  </td>
                  <td style={{padding:"8px 10px",textAlign:"right",fontSize:9,color:C.textMuted}}>{d.price_unit}</td>
                  <td style={{padding:"8px 10px",textAlign:"right",fontSize:10,color:C.textDim}}>{d.period}</td>
                  <td style={{padding:"8px 10px",textAlign:"right"}}>
                    <span style={{fontFamily:"monospace",fontWeight:600,fontSize:11,color:trendColor}}>{d.trend}</span>
                  </td>
                  <td style={{padding:"8px 10px"}}>
                    {points ? <svg width={sparkW} height={sparkH} style={{display:"block"}}>
                      <polyline points={points} fill="none" stroke={C.cyan} strokeWidth={1.5}/>
                    </svg> : <span style={{fontSize:9,color:C.textMuted}}>{"—"}</span>}
                  </td>
                  <td style={{padding:"8px 10px"}}>{srcBadge(d.source)}</td>
                </tr>);
              })}
            </tbody>
          </table>
          {/* Without data */}
          {noData.length>0 && <>
            <div style={{fontSize:11,fontWeight:600,color:C.textMuted,marginBottom:8,marginTop:16}}>{"\u26a0\ufe0f"} Mercados sem API gratuita ({noData.length})</div>
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
  // Tab: Portfolio + AI Trading Assistant
  const sendPortfolioChat = async () => {
    if(!portfolioInput.trim()||portfolioLoading) return;
    const userMsg = {role:"user",content:portfolioInput.trim()};
    const newMsgs = [...portfolioMsgs, userMsg];
    setPortfolioMsgs(newMsgs);
    setPortfolioInput("");
    setPortfolioLoading(true);
    try {
      const res = await fetch("/api/chat", {method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({messages:newMsgs,context:"portfolio"})});
      const data = await res.json();
      setPortfolioMsgs([...newMsgs, {role:"assistant",content:data.error?"Erro: "+data.error:data.response}]);
    } catch(e:any) {
      setPortfolioMsgs([...newMsgs, {role:"assistant",content:"Erro: "+e.message}]);
    }
    setPortfolioLoading(false);
  };

  const renderPortfolio = () => {
    const acct = portfolio?.account || {};
    const byUnderlying = portfolio?.positions_by_underlying || {};
    const equities = portfolio?.equity_positions || {};
    const fixedInc = portfolio?.fixed_income || {};
    const summary = portfolio?.portfolio_summary || {};
    const netLiq = acct.net_liquidation || 0;
    const cash = acct.total_cash || 0;
    const unrealPnl = acct.unrealized_pnl || 0;
    const buyPow = acct.buying_power || 0;
    const grossPos = acct.gross_position_value || 0;
    const marginUtil = acct.margin_utilization_pct || 0;
    const symbols = Object.keys(byUnderlying).sort();

    const suggestions = [
      "Analise meu portfolio e sugira hedges",
      "Quais posicoes tem mais risco agora?",
      "Sugira um trade em milho baseado nos spreads",
      "Como proteger minhas posicoes em wheat?",
      "Qual o cenario para soja nos proximos 30 dias?",
    ];

    return (
    <div style={{display:"flex",gap:0,height:"100%"}}>
      <div style={{flex:1,overflow:"auto",padding:24}}>
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

        {summary.dominant_strategy && (
          <div style={{padding:"12px 16px",background:C.panelAlt,borderRadius:8,border:`1px solid ${C.border}`,marginBottom:20,display:"flex",gap:24,alignItems:"center",flexWrap:"wrap"}}>
            <div><span style={{fontSize:9,color:C.textMuted,textTransform:"uppercase"}}>Estrategia</span><div style={{fontSize:12,fontWeight:600,color:C.amber,marginTop:2}}>{summary.dominant_strategy}</div></div>
            <div><span style={{fontSize:9,color:C.textMuted,textTransform:"uppercase"}}>Bias</span><div style={{fontSize:12,fontWeight:600,color:C.text,marginTop:2}}>{summary.directional_bias}</div></div>
            <div><span style={{fontSize:9,color:C.textMuted,textTransform:"uppercase"}}>Risco</span><div style={{fontSize:12,fontWeight:600,color:C.text,marginTop:2}}>{summary.risk_profile}</div></div>
            <div><span style={{fontSize:9,color:C.textMuted,textTransform:"uppercase"}}>Vencimentos</span><div style={{fontSize:12,fontWeight:600,color:C.red,marginTop:2}}>{summary.nearest_expirations}</div></div>
          </div>
        )}

        <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:20}}>
          <button onClick={refreshIbkr} disabled={ibkrRefreshing} style={{padding:"6px 16px",fontSize:11,fontWeight:600,background:ibkrRefreshing?"#555":C.blue,color:"#fff",border:"none",borderRadius:6,cursor:ibkrRefreshing?"wait":"pointer"}}>
            {ibkrRefreshing?"Atualizando...":"Atualizar IBKR"}
          </button>
          <span style={{fontSize:10,color:C.textMuted}}>Ultima atualizacao: {ibkrTime}</span>
          <span style={{fontSize:10,color:C.textMuted}}>| {summary.total_option_legs||0} legs, {summary.total_option_contracts_gross||0} contratos</span>
        </div>

        <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:14}}>Estrategias por Commodity ({symbols.length})</div>
        {symbols.map(sym=>{
          const data = byUnderlying[sym];
          const legs = data.legs || [];
          const structures = data.structures || [];
          return (
            <div key={sym} style={{marginBottom:20}}>
              <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:8}}>
                <span style={{fontSize:14,fontWeight:700,color:C.amber}}>{data.name || sym}</span>
                <span style={{fontSize:10,color:C.textMuted,background:C.panel,padding:"2px 8px",borderRadius:4}}>{sym}</span>
                <span style={{fontSize:10,color:C.cyan,fontWeight:600}}>{data.strategy_summary}</span>
              </div>
              {structures.length > 0 && (
                <div style={{display:"flex",gap:10,marginBottom:8,flexWrap:"wrap"}}>
                  {structures.map((st:any,si:number)=>(
                    <div key={si} style={{padding:"8px 14px",background:C.panelAlt,borderRadius:6,border:`1px solid ${C.border}`,fontSize:10}}>
                      <div style={{color:C.gold,fontWeight:700,textTransform:"uppercase",marginBottom:4}}>{(st.type||"").replace(/_/g," ")}</div>
                      <div style={{color:C.text}}>{st.long_strike && st.short_strike ? st.long_strike+"/"+st.short_strike : st.lower && st.upper ? st.lower+"/"+st.middle+"/"+st.upper : ""}{st.expiry ? " "+st.expiry : ""}{st.qty ? " x"+st.qty : ""}</div>
                      {st.max_risk_per_lot && <div style={{color:C.red,marginTop:2}}>{"Max risk/lot: $"+st.max_risk_per_lot}</div>}
                      {st.credit_received_per_lot && <div style={{color:C.green,marginTop:2}}>{"Credit/lot: $"+st.credit_received_per_lot.toFixed(2)}</div>}
                      {st.note && <div style={{color:C.textMuted,marginTop:2,fontStyle:"italic"}}>{st.note}</div>}
                    </div>
                  ))}
                </div>
              )}
              <div style={{background:C.panelAlt,borderRadius:6,border:`1px solid ${C.border}`,overflow:"hidden"}}>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
                  <thead>
                    <tr style={{borderBottom:`1px solid ${C.border}`}}>
                      {["Contrato","Tipo","Strike","Venc","Qtd","Side","Custo Med"].map(h=>(
                        <th key={h} style={{padding:"8px 10px",textAlign:"left",color:C.textMuted,fontWeight:600,fontSize:9,textTransform:"uppercase"}}>{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {legs.map((leg:any,j:number)=>(
                      <tr key={j} style={{borderBottom:`1px solid ${C.border}22`}}>
                        <td style={{padding:"6px 10px",color:C.text,fontFamily:"monospace",fontWeight:500}}>{leg.local_symbol}</td>
                        <td style={{padding:"6px 10px",color:leg.type==="PUT"?C.red:C.green,fontWeight:600}}>{leg.type}</td>
                        <td style={{padding:"6px 10px",color:C.text,fontFamily:"monospace"}}>{leg.strike}</td>
                        <td style={{padding:"6px 10px",color:C.textMuted}}>{leg.expiry}</td>
                        <td style={{padding:"6px 10px",color:leg.position>0?C.green:C.red,fontWeight:600,fontFamily:"monospace"}}>{leg.position>0?"+":""}{leg.position}</td>
                        <td style={{padding:"6px 10px",color:leg.side==="LONG"?C.green:C.red,fontWeight:600}}>{leg.side}</td>
                        <td style={{padding:"6px 10px",color:C.textMuted,fontFamily:"monospace"}}>{"$"+leg.avg_cost.toFixed(2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          );
        })}

        {Object.keys(equities).length > 0 && (
          <div style={{marginTop:20}}>
            <div style={{fontSize:14,fontWeight:700,color:C.text,marginBottom:14}}>Equity & Renda Fixa</div>
            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))",gap:12}}>
              {Object.entries({...equities,...fixedInc}).map(([k,v]:any)=>(
                <div key={k} style={{padding:14,background:C.panelAlt,borderRadius:8,border:`1px solid ${C.border}`}}>
                  <div style={{fontSize:12,fontWeight:700,color:C.amber}}>{v.name}</div>
                  <div style={{fontSize:10,color:C.textMuted,marginTop:2}}>{v.sec_type} | {v.position.toLocaleString()}{v.sec_type==="STK"?" shares":""}</div>
                  <div style={{fontSize:14,fontWeight:700,color:C.text,fontFamily:"monospace",marginTop:6}}>{"$"+v.notional_at_cost.toLocaleString("en-US",{minimumFractionDigits:0})}</div>
                </div>
              ))}
            </div>
          </div>
        )}

        {symbols.length===0 && Object.keys(equities).length===0 && <DataPlaceholder title="Sem posicoes" detail="Faca export do IBKR via Claude chat" />}
      </div>

            {/* RIGHT: AI Chat */}
      <div style={{width:400,borderLeft:`1px solid ${C.border}`,display:"flex",flexDirection:"column",background:C.panel}}>
        <div style={{padding:"14px 16px",borderBottom:`1px solid ${C.border}`,fontSize:13,fontWeight:700,color:C.amber}}>AI Trading Assistant</div>

        {/* Suggestions */}
        {portfolioMsgs.length===0 && (
          <div style={{padding:16,display:"flex",flexDirection:"column",gap:8}}>
            <div style={{fontSize:10,color:C.textMuted,marginBottom:4}}>Sugestoes:</div>
            {suggestions.map((s,i)=>(
              <button key={i} onClick={()=>{setPortfolioInput(s);}} style={{padding:"8px 12px",fontSize:10,color:C.text,background:C.panelAlt,border:`1px solid ${C.border}`,borderRadius:6,cursor:"pointer",textAlign:"left"}}>{s}</button>
            ))}
          </div>
        )}

        {/* Messages */}
        <div style={{flex:1,overflow:"auto",padding:12,display:"flex",flexDirection:"column",gap:10}}>
          {portfolioMsgs.map((m,i)=>(
            <div key={i} style={{padding:"10px 14px",borderRadius:8,fontSize:11,lineHeight:1.6,maxWidth:"90%",alignSelf:m.role==="user"?"flex-end":"flex-start",background:m.role==="user"?"rgba(59,130,246,.15)":C.panelAlt,color:C.text,border:`1px solid ${m.role==="user"?"rgba(59,130,246,.3)":C.border}`,whiteSpace:"pre-wrap"}}>
              {m.content}
            </div>
          ))}
          {portfolioLoading && <div style={{fontSize:10,color:C.textMuted,padding:8}}>Analisando...</div>}
        </div>

        {/* Input */}
        <div style={{padding:12,borderTop:`1px solid ${C.border}`,display:"flex",gap:8}}>
          <input value={portfolioInput} onChange={e=>setPortfolioInput(e.target.value)} onKeyDown={e=>e.key==="Enter"&&sendPortfolioChat()} placeholder="Pergunte sobre seu portfolio..." style={{flex:1,padding:"10px 14px",fontSize:11,background:C.bg,border:`1px solid ${C.border}`,borderRadius:6,color:C.text,outline:"none"}} />
          <button onClick={sendPortfolioChat} disabled={portfolioLoading} style={{padding:"10px 16px",fontSize:11,fontWeight:600,background:C.amber,color:"#000",border:"none",borderRadius:6,cursor:"pointer"}}>Enviar</button>
        </div>
      </div>
    </div>
    );
  };

  const renderTab = () => {
    switch(tab) {
      case "Gráfico + COT": return renderGraficoCOT();
      case "Comparativo": return renderComparativo();
      case "Spreads": return renderSpreads();
      case "Sazonalidade": return renderSazonalidade();
      case "Stocks Watch": return renderStocksWatch();
      case "Energia": return renderEnergia();
      case "Custo Produção": return renderCustoProducao();
      case "Físico Intl": return renderFisicoIntl();
      case "Leitura do Dia": return renderLeituraDoDia();
      case "Portfolio": return renderPortfolio();
      case "Bilateral": return <BilateralPanel />;
      default: return null;
    }
  };
  // -- Main Return --------------------------------------------------------
  return (
    <div style={{display:"flex",minHeight:"100vh",background:C.bg,color:C.text,fontFamily:"'Segoe UI','Helvetica Neue',sans-serif"}}>
      {/* Sidebar */}
      <div style={{width:220,minHeight:"100vh",background:C.panel,borderRight:`1px solid ${C.border}`,display:"flex",flexDirection:"column"}}>
        <div style={{padding:"18px 16px 10px",borderBottom:`1px solid ${C.border}`}}>
          <div style={{fontSize:16,fontWeight:800,letterSpacing:1.5,color:C.text}}>AGRIMACRO</div>
          <div style={{fontSize:9,color:C.textMuted,letterSpacing:1,marginTop:3}}>COMMODITIES DASHBOARD v3.2</div>
        </div>
        
        {/* Commodities list */}
        <div style={{flex:1,overflowY:"auto",padding:"10px 0"}}>
          {["Grãos","Softs","Pecuária","Energia","Metais","Macro"].map(grp=>(
            <div key={grp}>
              <div style={{padding:"10px 16px 4px",fontSize:9,fontWeight:700,color:C.textMuted,letterSpacing:1,textTransform:"uppercase"}}>{grp}</div>
              {COMMODITIES.filter(c=>c.group===grp).map(c=>{
                const p=getPrice(c.sym);const ch=getChange(c.sym);const sel=c.sym===selected;
                return (
                  <div key={c.sym} onClick={()=>setSelected(c.sym)} style={{
                    display:"flex",alignItems:"center",justifyContent:"space-between",padding:"7px 16px",cursor:"pointer",
                    background:sel?"rgba(59,130,246,.12)":"transparent",borderLeft:sel?`3px solid ${C.blue}`:"3px solid transparent",
                    transition:"all .15s",
                  }}>
                    <div>
                      <div style={{fontSize:12,fontWeight:sel?700:500,color:sel?C.text:C.textDim}}>{c.sym}</div>
                      <div style={{fontSize:9,color:C.textMuted}}>{c.name}</div>
                    </div>
                    <div style={{textAlign:"right"}}>
                      <div style={{fontSize:11,fontWeight:600,fontFamily:"monospace",color:p?C.text:C.textMuted}}>{p?p.toFixed(2):"—"}</div>
                      {ch && <div style={{fontSize:9,fontFamily:"monospace",color:ch.pct>=0?C.green:C.red}}>{ch.pct>=0?"+":""}{ch.pct.toFixed(2)}%</div>}
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
            <div style={{fontSize:16,fontWeight:700}}>{COMMODITIES.find(c=>c.sym===selected)?.name||selected}</div>
            <Badge label="DADOS REAIS" color={C.green} />
            {pipelineOk && <Badge label="PIPELINE ONLINE" color={C.blue} />}
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
            </div>
          </div>
          <div style={{fontSize:11,color:C.textMuted}}>{lastDate}</div>
        </div>

        {/* Tabs */}
        <div style={{display:"flex",gap:0,borderBottom:`1px solid ${C.border}`,background:C.panel,overflowX:"auto"}}>
          {TABS.map(t=>(
            <button key={t} onClick={()=>setTab(t)} style={{
              padding:"12px 18px",fontSize:11,fontWeight:tab===t?700:500,
              color:tab===t?C.blue:C.textDim,background:"transparent",border:"none",cursor:"pointer",
              borderBottom:tab===t?`2px solid ${C.blue}`:"2px solid transparent",whiteSpace:"nowrap",
              transition:"all .15s"
            }}>{t}</button>
          ))}
        </div>

        {/* Content */}
        <div style={{flex:1,overflow:"auto",padding:24}}>
          {loading ? <LoadingSpinner /> : renderTab()}
        </div>
      </div>
    </div>
  );
}





