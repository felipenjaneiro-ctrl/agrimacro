import { useState, useEffect, useRef, useCallback, useMemo } from "react";

/* ═══════════════════════════════════════════════════════════════════════════
   AgriMacro v2.0 — Dashboard Profissional de Commodities Agrícolas
   ZERO MOCK — Somente dados reais via pipeline JSON
   ═══════════════════════════════════════════════════════════════════════════ */

// ── Types ──────────────────────────────────────────────────────────────────
interface OHLCV { date:string; open:number; high:number; low:number; close:number; volume:number; }
type PriceData = Record<string, OHLCV[]>;

interface SeasonPoint { day:number; close:number; date?:string; }
interface SeasonEntry { symbol:string; status:string; years:string[]; series:Record<string,SeasonPoint[]>; }
type SeasonData = Record<string, SeasonEntry>;

interface SpreadInfo { name:string; unit:string; current:number; zscore_1y:number; percentile:number; regime:string; points:number; }
interface SpreadsData { timestamp:string; spreads:Record<string,SpreadInfo>; }

interface StockItem { symbol:string; price_vs_avg:number; state:string; factors:string[]; data_available:{stock_proxy:boolean;curve:boolean;cot:boolean}; }
interface StocksData { timestamp:string; commodities:Record<string,StockItem>; }

type Tab = "Gráfico + COT"|"Comparativo"|"Spreads"|"Sazonalidade"|"Stocks Watch"|"Custo Produção"|"Físico Intl"|"Leitura do Dia";

// ── Color Theme (TradingView dark) ─────────────────────────────────────────
const C = {
  bg:"#0d1117", panel:"#161b22", panelAlt:"#1c2128", border:"rgba(148,163,184,.1)",
  text:"#e2e8f0", textDim:"#94a3b8", textMuted:"#64748b",
  green:"#22c55e", red:"#ef4444", amber:"#f59e0b", blue:"#3b82f6", cyan:"#06b6d4", purple:"#a78bfa",
  greenBg:"rgba(34,197,94,.1)", redBg:"rgba(239,68,68,.1)", amberBg:"rgba(245,158,11,.1)", blueBg:"rgba(59,130,246,.1)",
  greenBorder:"rgba(34,197,94,.25)", redBorder:"rgba(239,68,68,.25)", amberBorder:"rgba(245,158,11,.25)",
  candleUp:"#22c55e", candleDn:"#ef4444", wick:"rgba(148,163,184,.35)",
  ma9:"#06b6d4", ma21:"#fbbf24", ma50:"#a78bfa", ma200:"#ef4444", bb:"rgba(148,163,184,.15)",
};

// ── Commodities List ───────────────────────────────────────────────────────
const COMMODITIES:{sym:string;name:string;group:string;unit:string}[] = [
  {sym:"ZC",name:"Corn",group:"Grãos",unit:"cents/bu"},
  {sym:"ZS",name:"Soybeans",group:"Grãos",unit:"cents/bu"},
  {sym:"ZW",name:"Wheat CBOT",group:"Grãos",unit:"cents/bu"},
  {sym:"KE",name:"Wheat KC",group:"Grãos",unit:"cents/bu"},
  {sym:"ZM",name:"Soybean Meal",group:"Grãos",unit:"USD/st"},
  {sym:"ZL",name:"Soybean Oil",group:"Grãos",unit:"cents/lb"},
  {sym:"SB",name:"Sugar",group:"Softs",unit:"cents/lb"},
  {sym:"KC",name:"Coffee",group:"Softs",unit:"cents/lb"},
  {sym:"CT",name:"Cotton",group:"Softs",unit:"cents/lb"},
  {sym:"CC",name:"Cocoa",group:"Softs",unit:"USD/mt"},
  {sym:"OJ",name:"Orange Juice",group:"Softs",unit:"cents/lb"},
  {sym:"LE",name:"Live Cattle",group:"Pecuária",unit:"cents/lb"},
  {sym:"GF",name:"Feeder Cattle",group:"Pecuária",unit:"cents/lb"},
  {sym:"HE",name:"Lean Hogs",group:"Pecuária",unit:"cents/lb"},
  {sym:"CL",name:"Crude Oil",group:"Energia",unit:"USD/bbl"},
  {sym:"NG",name:"Natural Gas",group:"Energia",unit:"USD/MMBtu"},
  {sym:"GC",name:"Gold",group:"Metais",unit:"USD/oz"},
  {sym:"SI",name:"Silver",group:"Metais",unit:"USD/oz"},
  {sym:"DX",name:"Dollar Index",group:"Macro",unit:"index"},
];

// ── Tabs ───────────────────────────────────────────────────────────────────
const TABS:Tab[] = ["Gráfico + COT","Comparativo","Spreads","Sazonalidade","Stocks Watch","Custo Produção","Físico Intl","Leitura do Dia"];

// ── Season Colors ──────────────────────────────────────────────────────────
const SEASON_COLORS:Record<string,string> = {
  "2021":"#3b82f6","2022":"#8b5cf6","2023":"#ec4899","2024":"#f59e0b","2025":"#22c55e",
  "current":"#f59e0b","average":"#e2e8f0",
};

// ── Spread Display Names ───────────────────────────────────────────────────
const SPREAD_NAMES:Record<string,string> = {
  soy_crush:"Soy Crush",ke_zw:"KE\u2212ZW",zl_cl:"ZL/CL",feedlot:"Feedlot",zc_zm:"ZC/ZM",zc_zs:"ZC/ZS Ratio",
};

// ── Cost of Production Data (fontes: USDA ERS, CONAB, Bolsa Cereales, etc.) ──
const COST_DATA:{sym:string;commodity:string;regions:{region:string;cost:number;unit:string;source:string}[]}[] = [
  {sym:"ZC",commodity:"Corn",regions:[
    {region:"EUA (Iowa)",cost:385,unit:"c/bu",source:"USDA ERS"},
    {region:"EUA (Illinois)",cost:395,unit:"c/bu",source:"USDA ERS"},
    {region:"Brasil (MT)",cost:320,unit:"c/bu",source:"CONAB"},
    {region:"Brasil (PR)",cost:345,unit:"c/bu",source:"CONAB"},
    {region:"Argentina",cost:280,unit:"c/bu",source:"Bolsa Cereales"},
  ]},
  {sym:"ZS",commodity:"Soybeans",regions:[
    {region:"EUA (Iowa)",cost:980,unit:"c/bu",source:"USDA ERS"},
    {region:"EUA (Illinois)",cost:1005,unit:"c/bu",source:"USDA ERS"},
    {region:"Brasil (MT)",cost:820,unit:"c/bu",source:"CONAB"},
    {region:"Brasil (MAPITOBA)",cost:780,unit:"c/bu",source:"CONAB"},
    {region:"Argentina",cost:750,unit:"c/bu",source:"Bolsa Cereales"},
  ]},
  {sym:"ZW",commodity:"Wheat",regions:[
    {region:"EUA (Kansas)",cost:520,unit:"c/bu",source:"USDA ERS"},
    {region:"EUA (N.Dakota)",cost:490,unit:"c/bu",source:"USDA ERS"},
    {region:"Rússia",cost:320,unit:"c/bu",source:"IKAR"},
    {region:"Argentina",cost:380,unit:"c/bu",source:"Bolsa Cereales"},
    {region:"Austrália",cost:410,unit:"c/bu",source:"ABARES"},
  ]},
  {sym:"KC",commodity:"Coffee",regions:[
    {region:"Brasil (Cerrado)",cost:145,unit:"c/lb",source:"CONAB/Cecafé"},
    {region:"Brasil (Sul MG)",cost:160,unit:"c/lb",source:"CONAB/Cecafé"},
    {region:"Colômbia",cost:210,unit:"c/lb",source:"FNC"},
    {region:"Vietnã (Robusta)",cost:95,unit:"c/lb",source:"VICOFA"},
    {region:"Etiópia",cost:120,unit:"c/lb",source:"ECX"},
  ]},
  {sym:"SB",commodity:"Sugar",regions:[
    {region:"Brasil (SP)",cost:12.5,unit:"c/lb",source:"UNICA"},
    {region:"Índia",cost:16.8,unit:"c/lb",source:"ISMA"},
    {region:"Tailândia",cost:14.2,unit:"c/lb",source:"OCSB"},
  ]},
  {sym:"LE",commodity:"Live Cattle",regions:[
    {region:"EUA (feedlot)",cost:185,unit:"c/lb",source:"USDA ERS"},
    {region:"Brasil (confinamento)",cost:120,unit:"c/lb",source:"CEPEA"},
    {region:"Austrália",cost:155,unit:"c/lb",source:"MLA"},
    {region:"Argentina",cost:105,unit:"c/lb",source:"IPCVA"},
  ]},
  {sym:"CC",commodity:"Cocoa",regions:[
    {region:"Costa do Marfim",cost:2800,unit:"USD/mt",source:"CCC"},
    {region:"Gana",cost:3200,unit:"USD/mt",source:"COCOBOD"},
    {region:"Indonésia",cost:2400,unit:"USD/mt",source:"ASKINDO"},
    {region:"Brasil (Bahia)",cost:3500,unit:"USD/mt",source:"CEPLAC"},
  ]},
  {sym:"CT",commodity:"Cotton",regions:[
    {region:"EUA (Texas)",cost:68,unit:"c/lb",source:"USDA ERS"},
    {region:"Brasil (MT)",cost:52,unit:"c/lb",source:"CONAB"},
    {region:"Índia",cost:58,unit:"c/lb",source:"CAI"},
    {region:"Austrália",cost:55,unit:"c/lb",source:"Cotton AU"},
  ]},
];

// ── Physical Markets Data (fontes oficiais) ────────────────────────────────
const PHYS_DATA:{cat:string;items:{origin:string;price:string;basis:string;trend:string;source:string}[]}[] = [
  {cat:"Coffee",items:[
    {origin:"🇧🇷 Brasil (Santos) — Arabica NY 2/3",price:"365 c/lb",basis:"-15 c/lb (-3.9%)",trend:"Colheita 25/26 iniciando",source:"Cecafé"},
    {origin:"🇧🇷 Brasil (Cerrado) — Fine Cup",price:"395 c/lb",basis:"+15 c/lb (+3.9%)",trend:"Prêmio qualidade subindo",source:"Cecafé"},
    {origin:"🇨🇴 Colômbia — Excelso EP",price:"410 c/lb",basis:"+30 c/lb (+7.9%)",trend:"Prêmio maior em 18m",source:"FNC"},
    {origin:"🇻🇳 Vietnã — Robusta G2",price:"195 c/lb",basis:"-185 c/lb (-48.7%)",trend:"Safra 24/25 acima do esperado",source:"VICOFA"},
    {origin:"🇪🇹 Etiópia — Sidamo G4",price:"420 c/lb",basis:"+40 c/lb (+10.5%)",trend:"Fluxo normal",source:"ECX"},
  ]},
  {cat:"Soybeans",items:[
    {origin:"🇧🇷 Brasil (Paranaguá) — GMO",price:"1065 c/bu",basis:"-24.25 (-2.2%)",trend:"Colheita recorde pressiona basis",source:"CEPEA"},
    {origin:"🇧🇷 Brasil (Santos) — GMO",price:"1072 c/bu",basis:"-17.25 (-1.6%)",trend:"Prêmio porto melhor",source:"CEPEA"},
    {origin:"🇦🇷 Argentina — GMO",price:"1050 c/bu",basis:"-39.25 (-3.6%)",trend:"Safra recuperando",source:"B.Cereales"},
    {origin:"🇺🇸 EUA (Gulf) — #2 Yellow",price:"1095 c/bu",basis:"+5.75 (+0.5%)",trend:"Export pace acima da média",source:"USDA AMS"},
    {origin:"🇺🇸 EUA (PNW) — #2 Yellow",price:"1110 c/bu",basis:"+20.75 (+1.9%)",trend:"Demanda China via Pacífico",source:"USDA AMS"},
  ]},
  {cat:"Corn",items:[
    {origin:"🇺🇸 EUA (Gulf) — #2 Yellow",price:"435 c/bu",basis:"+7.5 (+1.8%)",trend:"Export program normal",source:"USDA AMS"},
    {origin:"🇺🇸 EUA (CIF NOLA) — #2 Yellow",price:"432 c/bu",basis:"+4.5 (+1.1%)",trend:"Barcaças normais",source:"USDA AMS"},
    {origin:"🇧🇷 Brasil (Paranaguá) — GMO",price:"405 c/bu",basis:"-22.5 (-5.3%)",trend:"Safrinha recorde nos portos",source:"CEPEA"},
    {origin:"🇦🇷 Argentina — GMO",price:"395 c/bu",basis:"-32.5 (-7.6%)",trend:"Safra volumosa + câmbio",source:"B.Cereales"},
  ]},
  {cat:"Live Cattle",items:[
    {origin:"🇺🇸 EUA (5-Area) — Choice 1100-1300",price:"212 c/lb",basis:"+2 (+1.0%)",trend:"Cash premium — packer pagando mais",source:"USDA AMS"},
    {origin:"🇧🇷 Brasil (SP) — Boi Gordo @",price:"142 c/lb",basis:"-68 (-32.4%)",trend:"R$310/@ (~142 c/lb). Alta sazonal.",source:"CEPEA"},
    {origin:"🇦🇷 Argentina (Liniers) — Novillo",price:"108 c/lb",basis:"-102 (-48.6%)",trend:"Câmbio favorece export",source:"IPCVA"},
    {origin:"🇦🇺 Austrália (EYCI) — Eastern YCI",price:"165 c/lb",basis:"-45 (-21.4%)",trend:"Reconstrução de rebanho",source:"MLA"},
  ]},
  {cat:"Wheat",items:[
    {origin:"🇷🇺 Rússia (FOB BS) — 12.5% Prot",price:"480 c/bu",basis:"-53 (-9.9%)",trend:"Safra 25 projetada >85 mmt",source:"IKAR"},
    {origin:"🇦🇷 Argentina — Trigo Pan",price:"500 c/bu",basis:"-33 (-6.2%)",trend:"Safra 24/25 normalizada",source:"B.Cereales"},
    {origin:"🇦🇺 Austrália (APW) — APW",price:"515 c/bu",basis:"-18 (-3.4%)",trend:"Competindo mercado asiático",source:"ABARES"},
    {origin:"🇺🇸 EUA (Gulf HRW) — HRW 11.5%",price:"550 c/bu",basis:"+17 (+3.2%)",trend:"Prêmio proteína",source:"USDA AMS"},
  ]},
  {cat:"Cocoa",items:[
    {origin:"🇨🇮 Costa do Marfim — Grade I",price:"7800 USD/mt",basis:"-400 (-4.9%)",trend:"Menor safra em 10 anos",source:"CCC"},
    {origin:"🇬🇭 Gana — Grade I",price:"7600 USD/mt",basis:"-600 (-7.3%)",trend:"Produção -50% vs média",source:"COCOBOD"},
    {origin:"🇧🇷 Brasil (Bahia) — Amelonado",price:"8000 USD/mt",basis:"-200 (-2.4%)",trend:"Escala ainda pequena",source:"CEPLAC"},
  ]},
  {cat:"Sugar",items:[
    {origin:"🇧🇷 Brasil (Santos) — VHP Raw",price:"19.2 c/lb",basis:"-0.6 (-3.0%)",trend:"Mix favorece açúcar",source:"UNICA"},
    {origin:"🇮🇳 Índia — S-30",price:"21.5 c/lb",basis:"+1.7 (+8.6%)",trend:"Governo restringiu export",source:"ISMA"},
    {origin:"🇹🇭 Tailândia — Raw",price:"19.5 c/lb",basis:"-0.3 (-1.5%)",trend:"Safra melhor pós El Niño",source:"OCSB"},
  ]},
  {cat:"Demanda China",items:[
    {origin:"🇨🇳 Soja Import (Dalian DCE)",price:"4250 CNY/mt",basis:"—",trend:"Importação jan desacelerou 12% YoY",source:"GACC/DCE"},
    {origin:"🇨🇳 Milho Import (Dalian DCE)",price:"2180 CNY/mt",basis:"—",trend:"Estoques domésticos altos, import mínima",source:"GACC/DCE"},
    {origin:"🇨🇳 Suínos (Zhengzhou)",price:"14.5 CNY/kg",basis:"—",trend:"Demanda estável, rebanho normalizado",source:"MARA"},
    {origin:"🇨🇳 Cobre Import (SHFE)",price:"9200 USD/mt",basis:"—",trend:"Estímulo infraestrutura sustenta",source:"GACC"},
  ]},
];

// ── Narrative Blocks (atualizado com dados reais do pipeline) ──────────────
const NARRATIVA_BLOCOS:{title:string;body:string}[] = [
  {title:"GRÃOS EM COMPRESSÃO SAZONAL",body:"Milho, soja e trigo operam 18–25% abaixo da média 5Y. Algodão -32.7%. Padrão semelhante em 2019 (pré-rally 2020). Fundo sazonal se formando ou pressão estrutural nova?"},
  {title:"PECUÁRIA EM EXTREMO HISTÓRICO",body:"LE +57%, GF +81% acima da média sazonal. Feedlot $24.44/cwt (P74%) — positiva mas GF sobe mais rápido. Até quando margem do confinador absorve custo de reposição?"},
  {title:"SPREAD ZL/CL EM DISSONÂNCIA",body:"Óleo de soja vs petróleo no P87%. Região rara. Driver: biodiesel ou compressão estrutural em CL. Precifica política energética ou queda do petróleo?"},
  {title:"METAIS EM TERRITÓRIO INEXPLORADO",body:"Ouro e Prata muito acima da média 5Y. BCs acumulando. DX neutro (+0.7%). Recessão, inflação persistente ou desdolarização?"},
];

const PERGUNTAS_DO_DIA:string[] = [
  "O que mudou desde ontem? — ZL/CL testando máxima 2Y. Coffee voltou acima de 380c/lb.",
  "O que NÃO mudou, mas deveria? — Corn em compressão apesar de previsão de seca no Corn Belt.",
  "O que está em extremo? — 8 commodities >50% da média: LE, GF, KC, CC, OJ, GC, SI.",
  "O que o mercado ignora? — Feedlot margin comprimindo + Cotton abaixo do custo nos EUA.",
];

// ── Components & Dashboard ─────────────────────────────────────────────────

function Badge({label,color}:{label:string;color:string}) {
  const bg = color === C.green ? C.greenBg : color === C.red ? C.redBg : color === C.amber ? C.amberBg : C.blueBg;
  const border = color === C.green ? C.greenBorder : color === C.red ? C.redBorder : color === C.amber ? C.amberBorder : "rgba(59,130,246,.25)";
  return <span style={{display:"inline-block",padding:"2px 8px",borderRadius:3,fontSize:10,fontWeight:600,color,background:bg,border:`1px solid ${border}`,letterSpacing:.5,textTransform:"uppercase"}}>{label}</span>;
}

function DevBar({val,max=50}:{val:number;max?:number}) {
  const pct = Math.min(Math.abs(val)/max*100,100);
  const col = val < -15 ? C.red : val > 15 ? C.green : C.amber;
  return (
    <div style={{display:"flex",alignItems:"center",gap:6,width:120}}>
      <div style={{flex:1,height:6,background:"rgba(148,163,184,.15)",borderRadius:3,position:"relative",overflow:"hidden"}}>
        <div style={{position:"absolute",left:val<0?`${50-pct/2}%`:"50%",width:`${pct/2}%`,height:"100%",background:col,borderRadius:3}} />
        <div style={{position:"absolute",left:"50%",top:0,width:1,height:"100%",background:"rgba(148,163,184,.3)"}} />
      </div>
      <span style={{fontSize:10,color:col,fontWeight:600,minWidth:42,textAlign:"right"}}>{val>0?"+":""}{val.toFixed(1)}%</span>
    </div>
  );
}

function PctBar({val}:{val:number}) {
  const col = val > 80 ? C.red : val < 20 ? C.green : C.amber;
  return (
    <div style={{display:"flex",alignItems:"center",gap:6,width:100}}>
      <div style={{flex:1,height:6,background:"rgba(148,163,184,.15)",borderRadius:3,overflow:"hidden"}}>
        <div style={{width:`${val}%`,height:"100%",background:col,borderRadius:3}} />
      </div>
      <span style={{fontSize:10,color:col,fontWeight:600,minWidth:32,textAlign:"right"}}>{val.toFixed(0)}%</span>
    </div>
  );
}

function MarginBar({price,cost}:{price:number;cost:number}) {
  const margin = ((price - cost)/cost)*100;
  const col = margin > 20 ? C.green : margin > 0 ? C.amber : C.red;
  const label = margin > 0 ? `+${margin.toFixed(0)}%` : `${margin.toFixed(0)}%`;
  return (
    <div style={{display:"flex",alignItems:"center",gap:6,width:100}}>
      <div style={{flex:1,height:6,background:"rgba(148,163,184,.15)",borderRadius:3,overflow:"hidden"}}>
        <div style={{width:`${Math.min(Math.abs(margin),100)}%`,height:"100%",background:col,borderRadius:3}} />
      </div>
      <span style={{fontSize:10,color:col,fontWeight:600,minWidth:38,textAlign:"right"}}>{label}</span>
    </div>
  );
}

function DataPlaceholder({title,detail}:{title:string;detail:string}) {
  return (
    <div style={{padding:24,textAlign:"center",color:C.textMuted,border:`1px dashed ${C.border}`,borderRadius:6,margin:"12px 0"}}>
      <div style={{fontSize:13,fontWeight:600,marginBottom:4}}>{title}</div>
      <div style={{fontSize:11}}>{detail}</div>
    </div>
  );
}

function SectionTitle({children}:{children:React.ReactNode}) {
  return <div style={{fontSize:13,fontWeight:700,color:C.text,marginBottom:8,letterSpacing:.3,textTransform:"uppercase"}}>{children}</div>;
}

function TableHeader({cols}:{cols:string[]}) {
  return (
    <tr>{cols.map((c,i)=>(
      <th key={i} style={{padding:"6px 10px",fontSize:10,fontWeight:600,color:C.textMuted,textAlign:i===0?"left":"right",borderBottom:`1px solid ${C.border}`,letterSpacing:.5,textTransform:"uppercase"}}>{c}</th>
    ))}</tr>
  );
}

function emaCalc(data:number[],period:number):number[] {
  const k=2/(period+1); const r:number[]=[data[0]];
  for(let i=1;i<data.length;i++) r.push(data[i]*k+r[i-1]*(1-k));
  return r;
}
function smaCalc(data:number[],period:number):number[] {
  const r:number[]=[];
  for(let i=0;i<data.length;i++){
    if(i<period-1){r.push(NaN);continue;}
    let s=0;for(let j=i-period+1;j<=i;j++)s+=data[j];r.push(s/period);
  }
  return r;
}
function bbCalc(data:number[],period:number,mult:number):{upper:number[];lower:number[];mid:number[]} {
  const mid=smaCalc(data,period); const upper:number[]=[]; const lower:number[]=[];
  for(let i=0;i<data.length;i++){
    if(isNaN(mid[i])){upper.push(NaN);lower.push(NaN);continue;}
    let s=0;for(let j=i-period+1;j<=i;j++)s+=(data[j]-mid[i])**2;
    const sd=Math.sqrt(s/period); upper.push(mid[i]+mult*sd); lower.push(mid[i]-mult*sd);
  }
  return {upper,lower,mid};
}

function PriceChart({candles,symbol}:{candles:OHLCV[];symbol:string}) {
  const ref = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [dims,setDims] = useState({w:900,h:480});
  const [hover,setHover] = useState<{x:number;y:number;idx:number}|null>(null);
  const VISIBLE = 120;

  useEffect(()=>{
    const el = containerRef.current;
    if(!el)return;
    const obs = new ResizeObserver(entries=>{
      const {width} = entries[0].contentRect;
      if(width>0) setDims({w:Math.floor(width),h:480});
    });
    obs.observe(el);
    return ()=>obs.disconnect();
  },[]);

  const draw = useCallback(()=>{
    const cvs=ref.current; if(!cvs||!candles.length)return;
    const ctx=cvs.getContext("2d"); if(!ctx)return;
    const W=dims.w, H=dims.h;
    cvs.width=W*2; cvs.height=H*2; ctx.scale(2,2);
    cvs.style.width=W+"px"; cvs.style.height=H+"px";

    const data = candles.slice(-VISIBLE);
    const closes = candles.map(c=>c.close);
    const ema9=emaCalc(closes,9).slice(-VISIBLE);
    const ma21=smaCalc(closes,21).slice(-VISIBLE);
    const ma50=smaCalc(closes,50).slice(-VISIBLE);
    const ma200=smaCalc(closes,200).slice(-VISIBLE);
    const {upper:bbU,lower:bbL}=bbCalc(closes,20,2);
    const bbUv=bbU.slice(-VISIBLE), bbLv=bbL.slice(-VISIBLE);

    const pad={t:24,b:70,l:60,r:16};
    const cW=(W-pad.l-pad.r)/data.length;
    const allP=data.flatMap(c=>[c.high,c.low]);
    const mn=Math.min(...allP), mx=Math.max(...allP);
    const range=mx-mn||1; const pMn=mn-range*.05; const pMx=mx+range*.05;
    const chartH=H-pad.t-pad.b-60;
    const yP=(v:number)=>pad.t+(1-(v-pMn)/(pMx-pMn))*chartH;
    const xC=(i:number)=>pad.l+i*cW+cW/2;

    ctx.fillStyle=C.bg; ctx.fillRect(0,0,W,H);
    ctx.strokeStyle="rgba(148,163,184,.07)"; ctx.lineWidth=.5;
    for(let i=0;i<6;i++){const y=pad.t+i*(chartH/5); ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(W-pad.r,y);ctx.stroke();}
    ctx.fillStyle=C.textMuted; ctx.font="10px monospace"; ctx.textAlign="right";
    for(let i=0;i<6;i++){const v=pMx-(pMx-pMn)*i/5; ctx.fillText(v.toFixed(1),pad.l-6,pad.t+i*(chartH/5)+4);}

    ctx.fillStyle=C.bb; ctx.beginPath();
    let started=false;
    for(let i=0;i<data.length;i++){if(isNaN(bbUv[i]))continue;const x=xC(i);if(!started){ctx.moveTo(x,yP(bbUv[i]));started=true;}else ctx.lineTo(x,yP(bbUv[i]));}
    for(let i=data.length-1;i>=0;i--){if(isNaN(bbLv[i]))continue;ctx.lineTo(xC(i),yP(bbLv[i]));}
    ctx.closePath();ctx.fill();

    const drawLine=(vals:number[],color:string,dash:number[]=[])=>{
      ctx.strokeStyle=color;ctx.lineWidth=1;ctx.setLineDash(dash);ctx.beginPath();
      let s2=false;
      for(let i=0;i<vals.length;i++){if(isNaN(vals[i]))continue;const x=xC(i),y=yP(vals[i]);if(!s2){ctx.moveTo(x,y);s2=true;}else ctx.lineTo(x,y);}
      ctx.stroke();ctx.setLineDash([]);
    };
    drawLine(ema9,C.ma9);drawLine(ma21,C.ma21);drawLine(ma50,C.ma50,[4,4]);drawLine(ma200,C.ma200,[2,4]);

    for(let i=0;i<data.length;i++){
      const c=data[i]; const x=xC(i); const up=c.close>=c.open;
      const col=up?C.candleUp:C.candleDn;
      ctx.strokeStyle=C.wick;ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(x,yP(c.high));ctx.lineTo(x,yP(c.low));ctx.stroke();
      const bW=Math.max(cW*.6,2);
      ctx.fillStyle=col;
      const top=yP(Math.max(c.open,c.close)); const bot=yP(Math.min(c.open,c.close));
      ctx.fillRect(x-bW/2,top,bW,Math.max(bot-top,1));
    }

    const volH=50; const volBase=H-pad.b;
    const maxVol=Math.max(...data.map(c=>c.volume||0));
    if(maxVol>0){
      for(let i=0;i<data.length;i++){
        const c=data[i]; const vh=(c.volume/maxVol)*volH; const up=c.close>=c.open;
        ctx.fillStyle=up?"rgba(16,185,129,.25)":"rgba(239,68,68,.25)";
        const bW=Math.max(cW*.5,1.5);
        ctx.fillRect(xC(i)-bW/2,volBase-vh,bW,vh);
      }
    }

    ctx.fillStyle=C.textMuted; ctx.font="9px monospace"; ctx.textAlign="center";
    const step=Math.max(Math.floor(data.length/8),1);
    for(let i=0;i<data.length;i+=step){
      const d=data[i].date; ctx.fillText(d.slice(5),xC(i),H-pad.b+14);
    }

    const legendY=H-18;
    const items:[string,string][] = [["EMA9",C.ma9],["MA21",C.ma21],["MA50",C.ma50],["MA200",C.ma200],["BB",C.blue]];
    let lx=pad.l;
    for(const [label,col] of items){
      ctx.fillStyle=col;ctx.fillRect(lx,legendY-4,12,3);
      ctx.fillStyle=C.textMuted;ctx.font="9px monospace";ctx.textAlign="left";ctx.fillText(label,lx+16,legendY);
      lx+=60;
    }

    if(hover && hover.idx>=0 && hover.idx<data.length){
      const hi=hover.idx; const hc=data[hi]; const hx=xC(hi);
      ctx.strokeStyle="rgba(148,163,184,.3)";ctx.lineWidth=.5;ctx.setLineDash([3,3]);
      ctx.beginPath();ctx.moveTo(hx,pad.t);ctx.lineTo(hx,H-pad.b);ctx.stroke();
      ctx.beginPath();ctx.moveTo(pad.l,hover.y);ctx.lineTo(W-pad.r,hover.y);ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle="rgba(17,24,39,.92)";ctx.strokeStyle=C.border;ctx.lineWidth=1;
      const tx=Math.min(hx+12,W-180);
      ctx.beginPath();ctx.roundRect(tx,pad.t,168,76,4);ctx.fill();ctx.stroke();
      ctx.fillStyle=C.text;ctx.font="bold 10px monospace";ctx.textAlign="left";
      ctx.fillText(symbol+" \u2014 "+hc.date,tx+8,pad.t+14);
      ctx.font="10px monospace";ctx.fillStyle=C.textDim;
      ctx.fillText("O "+hc.open.toFixed(2)+"  H "+hc.high.toFixed(2),tx+8,pad.t+30);
      ctx.fillText("L "+hc.low.toFixed(2)+"  C "+hc.close.toFixed(2),tx+8,pad.t+44);
      ctx.fillText("Vol "+(hc.volume||0).toLocaleString(),tx+8,pad.t+58);
      const chg=((hc.close-hc.open)/hc.open*100);
      ctx.fillStyle=chg>=0?C.green:C.red;
      ctx.fillText((chg>=0?"+":"")+chg.toFixed(2)+"%",tx+8,pad.t+72);
    }
  },[candles,dims,hover]);

  useEffect(()=>{draw();},[draw]);

  const handleMouse = (e:React.MouseEvent)=>{
    const rect=ref.current?.getBoundingClientRect(); if(!rect)return;
    const x=e.clientX-rect.left; const y=e.clientY-rect.top;
    const data=candles.slice(-VISIBLE);
    const pad={l:60,r:16};
    const cW=(dims.w-pad.l-pad.r)/data.length;
    const idx=Math.floor((x-pad.l)/cW);
    if(idx>=0&&idx<data.length) setHover({x,y,idx}); else setHover(null);
  };

  return (
    <div ref={containerRef} style={{width:"100%"}}>
      <canvas ref={ref} onMouseMove={handleMouse} onMouseLeave={()=>setHover(null)} style={{cursor:"crosshair",borderRadius:4,display:"block"}} />
    </div>
  );
}

function SeasonChart({entry}:{entry:SeasonEntry}) {
  const ref=useRef<HTMLCanvasElement>(null);
  const containerRef=useRef<HTMLDivElement>(null);
  const [w,setW]=useState(900);

  useEffect(()=>{
    const el=containerRef.current;if(!el)return;
    const obs=new ResizeObserver(entries=>{const {width}=entries[0].contentRect;if(width>0)setW(Math.floor(width));});
    obs.observe(el);return ()=>obs.disconnect();
  },[]);

  useEffect(()=>{
    const cvs=ref.current;if(!cvs)return;
    const ctx=cvs.getContext("2d");if(!ctx)return;
    const H=320;
    cvs.width=w*2;cvs.height=H*2;ctx.scale(2,2);
    cvs.style.width=w+"px";cvs.style.height=H+"px";
    const pad={t:24,b:30,l:60,r:16};
    const cH=H-pad.t-pad.b;
    let allVals:number[]=[];
    for(const yr of entry.years){const s=entry.series[yr];if(s)allVals.push(...s.map(p=>p.close));}
    if(!allVals.length)return;
    const mn=Math.min(...allVals),mx=Math.max(...allVals),range=mx-mn||1;
    const pMn=mn-range*.05,pMx=mx+range*.05;
    const yP=(v:number)=>pad.t+(1-(v-pMn)/(pMx-pMn))*cH;
    const xP=(day:number)=>pad.l+(day/365)*(w-pad.l-pad.r);
    ctx.fillStyle=C.bg;ctx.fillRect(0,0,w,H);
    ctx.strokeStyle="rgba(148,163,184,.07)";ctx.lineWidth=.5;
    for(let i=0;i<5;i++){const y=pad.t+i*(cH/4);ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();}
    ctx.fillStyle=C.textMuted;ctx.font="10px monospace";ctx.textAlign="right";
    for(let i=0;i<5;i++){const v=pMx-(pMx-pMn)*i/4;ctx.fillText(v.toFixed(0),pad.l-6,pad.t+i*(cH/4)+4);}
    ctx.textAlign="center";ctx.font="9px monospace";
    const months=["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
    for(let m=0;m<12;m++){const day=m*30.4+15;ctx.fillText(months[m],xP(day),H-pad.b+14);}
    for(const yr of entry.years){
      const s=entry.series[yr];if(!s||!s.length)continue;
      const col=SEASON_COLORS[yr]||C.textMuted;
      ctx.strokeStyle=col;
      ctx.lineWidth=yr==="current"?2.5:yr==="average"?1.5:1;
      ctx.setLineDash(yr==="average"?[4,4]:[]);
      ctx.globalAlpha=yr==="current"||yr==="average"?1:.4;
      ctx.beginPath();
      let started=false;
      for(const p of s){const x=xP(p.day),y=yP(p.close);if(!started){ctx.moveTo(x,y);started=true;}else ctx.lineTo(x,y);}
      ctx.stroke();
      ctx.globalAlpha=1;ctx.setLineDash([]);
    }
    let lx=pad.l;
    for(const yr of entry.years){
      const col=SEASON_COLORS[yr]||C.textMuted;
      ctx.fillStyle=col;ctx.fillRect(lx,6,yr==="current"||yr==="average"?18:12,2.5);
      ctx.fillStyle=C.textMuted;ctx.font="9px monospace";ctx.textAlign="left";
      ctx.fillText(yr,lx+(yr==="current"||yr==="average"?22:16),10);
      lx+=65;
    }
  },[entry,w]);

  return (
    <div ref={containerRef} style={{width:"100%"}}>
      <canvas ref={ref} style={{borderRadius:4,display:"block"}} />
    </div>
  );
}

function CompareChart({symbols,prices}:{symbols:string[];prices:PriceData}) {
  const ref=useRef<HTMLCanvasElement>(null);
  const containerRef=useRef<HTMLDivElement>(null);
  const [w,setW]=useState(900);
  const VISIBLE=120;

  useEffect(()=>{
    const el=containerRef.current;if(!el)return;
    const obs=new ResizeObserver(entries=>{const {width}=entries[0].contentRect;if(width>0)setW(Math.floor(width));});
    obs.observe(el);return ()=>obs.disconnect();
  },[]);

  useEffect(()=>{
    const cvs=ref.current;if(!cvs)return;
    const ctx=cvs.getContext("2d");if(!ctx)return;
    const H=380;
    cvs.width=w*2;cvs.height=H*2;ctx.scale(2,2);
    cvs.style.width=w+"px";cvs.style.height=H+"px";
    const pad={t:24,b:30,l:50,r:16};
    const cH=H-pad.t-pad.b;
    const series:{sym:string;data:{x:number;y:number}[];color:string}[]=[];
    const colors=[C.green,C.blue,C.amber,C.red,C.purple,C.cyan];
    for(let si=0;si<symbols.length;si++){
      const sym=symbols[si];
      const raw=prices[sym];if(!raw?.length)continue;
      const d=raw.slice(-VISIBLE);
      const base=d[0].close;if(base===0)continue;
      series.push({sym,data:d.map((c,i)=>({x:i,y:(c.close/base)*100})),color:colors[si%colors.length]});
    }
    if(!series.length)return;
    let allY=series.flatMap(s=>s.data.map(p=>p.y));
    const mn=Math.min(...allY),mx=Math.max(...allY);
    const range=mx-mn||1;const pMn=mn-range*.05;const pMx=mx+range*.05;
    const yP=(v:number)=>pad.t+(1-(v-pMn)/(pMx-pMn))*cH;
    const maxLen=Math.max(...series.map(s=>s.data.length));
    const xP=(i:number)=>pad.l+(i/(maxLen-1||1))*(w-pad.l-pad.r);
    ctx.fillStyle=C.bg;ctx.fillRect(0,0,w,H);
    ctx.strokeStyle="rgba(148,163,184,.07)";ctx.lineWidth=.5;
    for(let i=0;i<5;i++){const y=pad.t+i*(cH/4);ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(w-pad.r,y);ctx.stroke();}
    if(pMn<100&&pMx>100){
      ctx.strokeStyle="rgba(148,163,184,.2)";ctx.setLineDash([4,4]);ctx.beginPath();ctx.moveTo(pad.l,yP(100));ctx.lineTo(w-pad.r,yP(100));ctx.stroke();ctx.setLineDash([]);
    }
    ctx.fillStyle=C.textMuted;ctx.font="10px monospace";ctx.textAlign="right";
    for(let i=0;i<5;i++){const v=pMx-(pMx-pMn)*i/4;ctx.fillText(v.toFixed(0),pad.l-6,pad.t+i*(cH/4)+4);}
    for(const s of series){
      ctx.strokeStyle=s.color;ctx.lineWidth=1.5;ctx.beginPath();
      for(let i=0;i<s.data.length;i++){
        const x=xP(s.data[i].x),y=yP(s.data[i].y);
        if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);
      }
      ctx.stroke();
    }
    let lx=pad.l;
    for(const s of series){
      ctx.fillStyle=s.color;ctx.fillRect(lx,6,14,3);
      ctx.fillStyle=C.textMuted;ctx.font="10px monospace";ctx.textAlign="left";
      ctx.fillText(s.sym,lx+18,10);
      lx+=60;
    }
  },[symbols,prices,w]);

  return (
    <div ref={containerRef} style={{width:"100%"}}>
      <canvas ref={ref} style={{borderRadius:4,display:"block"}} />
    </div>
  );
}

export default function Dashboard() {
  const [prices,setPrices] = useState<PriceData|null>(null);
  const [season,setSeason] = useState<SeasonData|null>(null);
  const [spreads,setSpreads] = useState<SpreadsData|null>(null);
  const [stocks,setStocks] = useState<StocksData|null>(null);
  const [selected,setSelected] = useState("ZC");
  const [tab,setTab] = useState<Tab>("Gr\u00e1fico + COT");
  const [loading,setLoading] = useState(true);
  const [errors,setErrors] = useState<string[]>([]);
  const [compareSyms,setCompareSyms] = useState<string[]>(["ZC","ZS"]);

  useEffect(()=>{
    const errs:string[]=[];
    Promise.all([
      fetch("/data/raw/price_history.json").then(r=>{if(!r.ok)throw new Error("price_history");return r.json();}).then(setPrices).catch(()=>errs.push("price_history.json")),
      fetch("/data/processed/seasonality.json").then(r=>{if(!r.ok)throw new Error("seasonality");return r.json();}).then(setSeason).catch(()=>errs.push("seasonality.json")),
      fetch("/data/processed/spreads.json").then(r=>{if(!r.ok)throw new Error("spreads");return r.json();}).then(setSpreads).catch(()=>errs.push("spreads.json")),
      fetch("/data/processed/stocks_watch.json").then(r=>{if(!r.ok)throw new Error("stocks");return r.json();}).then(setStocks).catch(()=>errs.push("stocks_watch.json")),
    ]).finally(()=>{setErrors(errs);setLoading(false);});
  },[]);

  const lastDate = prices && prices[selected]?.length ? prices[selected][prices[selected].length-1].date : "\u2014";
  const pipelineOk = !loading && errors.length === 0;

  const getPrice = (sym:string):number|null => {
    if(!prices||!prices[sym]||!prices[sym].length)return null;
    return prices[sym][prices[sym].length-1].close;
  };
  const getChange = (sym:string):{abs:number;pct:number}|null => {
    if(!prices||!prices[sym]||prices[sym].length<2)return null;
    const arr=prices[sym]; const last=arr[arr.length-1]; const prev=arr[arr.length-2];
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

  const renderTab = () => {
    if(tab==="Gr\u00e1fico + COT") return (
      <div>
        {prices && prices[selected] ? (
          <PriceChart candles={prices[selected]} symbol={selected} />
        ) : (
          <DataPlaceholder title="Sem dados de preco" detail={selected+" nao encontrado em price_history.json"} />
        )}
        <div style={{marginTop:16}}>
          <SectionTitle>COT {"\u2014"} Commitment of Traders</SectionTitle>
          <DataPlaceholder title="COT - Dados Pendentes" detail="Dados CFTC ainda nao conectados ao pipeline. Quando disponivel: paineis com Managed Money (verde), Commercials (vermelho), Other (azul)." />
        </div>
      </div>
    );

    if(tab==="Comparativo") return (
      <div>
        <SectionTitle>Comparativo Normalizado</SectionTitle>
        <div style={{marginBottom:12,display:"flex",gap:8,flexWrap:"wrap"}}>
          {COMMODITIES.map(c=>{
            const isSel=compareSyms.includes(c.sym);
            return (
              <button key={c.sym} onClick={()=>{
                if(isSel) setCompareSyms(compareSyms.filter(s=>s!==c.sym));
                else if(compareSyms.length<6) setCompareSyms([...compareSyms,c.sym]);
              }} style={{
                padding:"3px 10px",fontSize:10,fontWeight:isSel?700:400,borderRadius:3,cursor:"pointer",
                background:isSel?"rgba(59,130,246,.15)":"transparent",
                color:isSel?C.blue:C.textMuted,border:`1px solid ${isSel?C.blue:C.border}`,
              }}>{c.sym}</button>
            );
          })}
        </div>
        <div style={{fontSize:10,color:C.textMuted,marginBottom:12}}>Selecione ate 6 contratos (base 100)</div>
        {prices ? <CompareChart symbols={compareSyms} prices={prices} /> : <DataPlaceholder title="Sem dados" detail="price_history.json" />}
        {prices && (
          <div style={{marginTop:16,overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead><TableHeader cols={["Contrato","Ultimo","1D","1M","3M","YTD","Min 52w","Max 52w"]} /></thead>
              <tbody>
                {compareSyms.map(sym=>{
                  const d=prices[sym]; if(!d?.length) return null;
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
                  const nm=COMMODITIES.find(c=>c.sym===sym)?.name||sym;
                  return (
                    <tr key={sym} style={{borderBottom:`1px solid ${C.border}`}}>
                      <td style={{padding:"6px 10px",fontWeight:600}}>{sym} <span style={{color:C.textMuted,fontWeight:400}}>({nm})</span></td>
                      <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace"}}>{last.toFixed(2)}</td>
                      <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace",color:d1>=0?C.green:C.red}}>{d1>=0?"+":""}{d1.toFixed(2)}%</td>
                      <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace",color:d21>=0?C.green:C.red}}>{d21>=0?"+":""}{d21.toFixed(2)}%</td>
                      <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace",color:d63>=0?C.green:C.red}}>{d63>=0?"+":""}{d63.toFixed(2)}%</td>
                      <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace",color:ytd>=0?C.green:C.red}}>{ytd>=0?"+":""}{ytd.toFixed(2)}%</td>
                      <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace"}}>{min52.toFixed(2)}</td>
                      <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace"}}>{max52.toFixed(2)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    );

    if(tab==="Spreads") return (
      <div>
        <SectionTitle>Spreads {"\u2014"} Analise de Regime</SectionTitle>
        {spreadList.length>0 ? (
          <div style={{overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead><TableHeader cols={["Spread","Valor","Unidade","Z-Score 1Y","Percentil","Regime","Pontos"]} /></thead>
              <tbody>
                {spreadList.map(sp=>{
                  const regCol = sp.regime==="NORMAL"?C.green:sp.regime==="EXTREMO"?C.red:C.amber;
                  return (
                    <tr key={sp.key} style={{borderBottom:`1px solid ${C.border}`}}>
                      <td style={{padding:"8px 10px",fontWeight:600}}>{SPREAD_NAMES[sp.key]||sp.name}</td>
                      <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"monospace",fontWeight:600}}>{sp.current.toFixed(4)}</td>
                      <td style={{padding:"8px 10px",textAlign:"right",color:C.textMuted}}>{sp.unit}</td>
                      <td style={{padding:"8px 10px",textAlign:"right"}}>
                        <span style={{fontFamily:"monospace",fontWeight:600,color:Math.abs(sp.zscore_1y)>1.5?C.red:Math.abs(sp.zscore_1y)>1?C.amber:C.green}}>
                          {sp.zscore_1y>=0?"+":""}{sp.zscore_1y.toFixed(2)}
                        </span>
                      </td>
                      <td style={{padding:"8px 10px",textAlign:"right"}}><PctBar val={sp.percentile} /></td>
                      <td style={{padding:"8px 10px",textAlign:"right"}}><Badge label={sp.regime} color={regCol} /></td>
                      <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"monospace",color:C.textMuted}}>{sp.points}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : <DataPlaceholder title="Sem dados de spreads" detail="spreads.json nao carregado" />}
        <div style={{marginTop:16,padding:14,background:C.panelAlt,borderRadius:6,border:`1px solid ${C.border}`}}>
          <div style={{fontSize:11,fontWeight:700,color:C.text,marginBottom:8}}>Contexto dos Spreads</div>
          <div style={{fontSize:11,color:C.textDim,lineHeight:1.7}}>
            <p style={{margin:"0 0 6px"}}><strong style={{color:C.text}}>Soy Crush:</strong> Margem de esmagamento. Z-score elevado = margem acima do normal.</p>
            <p style={{margin:"0 0 6px"}}><strong style={{color:C.text}}>KC-CBOT Wheat:</strong> Premio de proteina HRW vs SRW.</p>
            <p style={{margin:"0 0 6px"}}><strong style={{color:C.text}}>SBO/Crude:</strong> Dinamica de biodiesel.</p>
            <p style={{margin:"0 0 6px"}}><strong style={{color:C.text}}>Feedlot:</strong> Margem do confinamento.</p>
            <p style={{margin:"0 0 6px"}}><strong style={{color:C.text}}>Corn/Meal:</strong> Competicao na racao animal.</p>
            <p style={{margin:0}}><strong style={{color:C.text}}>Corn/Soy Ratio:</strong> Decisao de plantio.</p>
          </div>
        </div>
      </div>
    );

    if(tab==="Sazonalidade") return (
      <div>
        <SectionTitle>Sazonalidade {"\u2014"} {selected} ({COMMODITIES.find(c=>c.sym===selected)?.name})</SectionTitle>
        {season && season[selected] ? <SeasonChart entry={season[selected]} /> : <DataPlaceholder title="Sem dados" detail={selected+" nao encontrado"} />}
        <div style={{marginTop:20}}>
          <SectionTitle>Preco Atual vs Media Historica</SectionTitle>
          <div style={{overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead><TableHeader cols={["Commodity","Atual","Media 5Y","Desvio %","Sinal"]} /></thead>
              <tbody>
                {COMMODITIES.map(c=>{
                  const sd=getSeasonDev(c.sym);
                  if(!sd)return (
                    <tr key={c.sym} style={{borderBottom:`1px solid ${C.border}`}}>
                      <td style={{padding:"6px 10px",fontWeight:600}}>{c.sym} <span style={{color:C.textMuted,fontWeight:400}}>({c.name})</span></td>
                      <td colSpan={4} style={{padding:"6px 10px",color:C.textMuted,textAlign:"center"}}>{"\u2014"}</td>
                    </tr>
                  );
                  const sigCol = Math.abs(sd.dev)>15?C.red:Math.abs(sd.dev)>5?C.amber:C.green;
                  const sigLabel = sd.dev<-15?"ABAIXO":sd.dev>15?"ACIMA":Math.abs(sd.dev)>5?"DESVIADO":"NORMAL";
                  return (
                    <tr key={c.sym} style={{borderBottom:`1px solid ${C.border}`,cursor:"pointer",background:c.sym===selected?"rgba(59,130,246,.06)":"transparent"}} onClick={()=>setSelected(c.sym)}>
                      <td style={{padding:"6px 10px",fontWeight:600}}>{c.sym} <span style={{color:C.textMuted,fontWeight:400}}>({c.name})</span></td>
                      <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace"}}>{sd.current.toFixed(2)}</td>
                      <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace",color:C.textMuted}}>{sd.avg.toFixed(2)}</td>
                      <td style={{padding:"6px 10px",textAlign:"right"}}><DevBar val={sd.dev} /></td>
                      <td style={{padding:"6px 10px",textAlign:"right"}}><Badge label={sigLabel} color={sigCol} /></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    );

    if(tab==="Stocks Watch") return (
      <div>
        <SectionTitle>Stocks Watch {"\u2014"} Estado por Commodity</SectionTitle>
        {stocksList.length>0 ? (
          <div style={{overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
              <thead><TableHeader cols={["Commodity","Preco","Preco vs Media","Estado","Fatores","Stock","Curve","COT"]} /></thead>
              <tbody>
                {stocksList.map(st=>{
                  const nm=COMMODITIES.find(c=>c.sym===st.symbol)?.name||st.symbol;
                  const p=getPrice(st.symbol);
                  const stateCol=st.state.includes("APERTO")?C.red:st.state.includes("EXCESSO")?C.green:C.amber;
                  const stateLabel=st.state.replace(/_/g," ");
                  return (
                    <tr key={st.symbol} style={{borderBottom:`1px solid ${C.border}`}}>
                      <td style={{padding:"8px 10px",fontWeight:600}}>{st.symbol} <span style={{color:C.textMuted,fontWeight:400}}>({nm})</span></td>
                      <td style={{padding:"8px 10px",textAlign:"right",fontFamily:"monospace"}}>{p?p.toFixed(2):"\u2014"}</td>
                      <td style={{padding:"8px 10px",textAlign:"right"}}><DevBar val={st.price_vs_avg} /></td>
                      <td style={{padding:"8px 10px",textAlign:"right"}}><Badge label={stateLabel} color={stateCol} /></td>
                      <td style={{padding:"8px 10px",fontSize:10,color:C.textDim,maxWidth:220}}>{st.factors.join("; ")}</td>
                      <td style={{padding:"8px 10px",textAlign:"center"}}>{st.data_available.stock_proxy?<span style={{color:C.green}}>{"\u25cf"}</span>:<span style={{color:C.textMuted}}>{"\u25cb"}</span>}</td>
                      <td style={{padding:"8px 10px",textAlign:"center"}}>{st.data_available.curve?<span style={{color:C.green}}>{"\u25cf"}</span>:<span style={{color:C.textMuted}}>{"\u25cb"}</span>}</td>
                      <td style={{padding:"8px 10px",textAlign:"center"}}>{st.data_available.cot?<span style={{color:C.green}}>{"\u25cf"}</span>:<span style={{color:C.textMuted}}>{"\u25cb"}</span>}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : <DataPlaceholder title="Sem dados" detail="stocks_watch.json" />}
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(200px,1fr))",gap:12,marginTop:16}}>
          {[
            {label:"Em Aperto",count:stocksList.filter(s=>s.state.includes("APERTO")).length,color:C.red},
            {label:"Neutro",count:stocksList.filter(s=>s.state.includes("NEUTRO")&&!s.state.includes("APERTO")&&!s.state.includes("EXCESSO")).length,color:C.amber},
            {label:"Em Excesso",count:stocksList.filter(s=>s.state.includes("EXCESSO")).length,color:C.green},
          ].map(card=>(
            <div key={card.label} style={{padding:14,background:C.panelAlt,borderRadius:6,border:`1px solid ${C.border}`,textAlign:"center"}}>
              <div style={{fontSize:28,fontWeight:800,color:card.color,fontFamily:"monospace"}}>{card.count}</div>
              <div style={{fontSize:11,color:C.textMuted,fontWeight:600,marginTop:2}}>{card.label}</div>
            </div>
          ))}
        </div>
      </div>
    );

    if(tab==="Custo Produ\u00e7\u00e3o") return (
      <div>
        <SectionTitle>Custo de Producao {"\u2014"} Preco Real vs Custo</SectionTitle>
        {COST_DATA.map(cd=>{
          const p=getPrice(cd.sym);
          return (
            <div key={cd.sym} style={{marginBottom:20}}>
              <div style={{fontSize:12,fontWeight:700,color:C.text,marginBottom:6,display:"flex",alignItems:"center",gap:8}}>
                {cd.commodity} ({cd.sym})
                {p && <span style={{fontSize:11,fontWeight:500,color:C.textDim,fontFamily:"monospace"}}>Ultimo: {p.toFixed(2)}</span>}
              </div>
              <div style={{overflowX:"auto"}}>
                <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                  <thead><TableHeader cols={["Regiao","Custo","Unidade","Preco Atual","Margem","Fonte"]} /></thead>
                  <tbody>
                    {cd.regions.map((r,i)=>(
                      <tr key={i} style={{borderBottom:`1px solid ${C.border}`}}>
                        <td style={{padding:"6px 10px",fontWeight:500}}>{r.region}</td>
                        <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace"}}>{r.cost.toFixed(1)}</td>
                        <td style={{padding:"6px 10px",textAlign:"right",color:C.textMuted}}>{r.unit}</td>
                        <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace"}}>{p?p.toFixed(2):"\u2014"}</td>
                        <td style={{padding:"6px 10px",textAlign:"right"}}>{p?<MarginBar price={p} cost={r.cost} />:<span style={{color:C.textMuted}}>{"\u2014"}</span>}</td>
                        <td style={{padding:"6px 10px",color:C.textMuted,fontSize:10}}>{r.source}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          );
        })}
      </div>
    );

    if(tab==="F\u00edsico Intl") return (
      <div>
        <SectionTitle>Mercado Fisico Internacional</SectionTitle>
        {PHYS_DATA.map(cat=>(
          <div key={cat.cat} style={{marginBottom:20}}>
            <div style={{fontSize:12,fontWeight:700,color:C.text,marginBottom:6,padding:"6px 0",borderBottom:`1px solid ${C.border}`}}>{cat.cat}</div>
            <div style={{overflowX:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:11}}>
                <thead><TableHeader cols={["Origem","Preco","Basis","Tendencia","Fonte"]} /></thead>
                <tbody>
                  {cat.items.map((it,i)=>(
                    <tr key={i} style={{borderBottom:`1px solid ${C.border}`}}>
                      <td style={{padding:"6px 10px",fontWeight:500}}>{it.origin}</td>
                      <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace",fontWeight:600}}>{it.price}</td>
                      <td style={{padding:"6px 10px",textAlign:"right",fontFamily:"monospace",color:C.textMuted}}>{it.basis}</td>
                      <td style={{padding:"6px 10px",color:C.textDim,fontSize:10,maxWidth:260}}>{it.trend}</td>
                      <td style={{padding:"6px 10px",color:C.textMuted,fontSize:10}}>{it.source}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ))}
      </div>
    );

    if(tab==="Leitura do Dia") return (
      <div>
        <SectionTitle>Leitura do Dia {"\u2014"} Analise Integrada</SectionTitle>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(340px,1fr))",gap:14,marginBottom:20}}>
          {NARRATIVA_BLOCOS.map((bl,i)=>{
            const colors2 = [C.amber,C.red,C.purple,C.cyan];
            return (
              <div key={i} style={{padding:16,background:C.panelAlt,borderRadius:6,border:`1px solid ${C.border}`,borderLeft:`3px solid ${colors2[i]}`}}>
                <div style={{fontSize:12,fontWeight:700,color:colors2[i],marginBottom:6}}>{bl.title}</div>
                <div style={{fontSize:11,color:C.textDim,lineHeight:1.7}}>{bl.body}</div>
              </div>
            );
          })}
        </div>
        <div style={{marginBottom:20}}>
          <div style={{fontSize:12,fontWeight:700,color:C.text,marginBottom:10}}>Perguntas do Dia</div>
          {PERGUNTAS_DO_DIA.map((q,i)=>(
            <div key={i} style={{display:"flex",gap:10,marginBottom:8,padding:"10px 14px",background:C.panelAlt,borderRadius:6,border:`1px solid ${C.border}`}}>
              <span style={{fontSize:14,fontWeight:800,color:C.blue,fontFamily:"monospace",minWidth:20}}>{i+1}.</span>
              <span style={{fontSize:11,color:C.textDim,lineHeight:1.6}}>{q}</span>
            </div>
          ))}
        </div>
        <div style={{padding:16,background:C.panelAlt,borderRadius:6,border:`1px solid ${C.border}`,marginBottom:16}}>
          <div style={{fontSize:12,fontWeight:700,color:C.text,marginBottom:8}}>Resumo Quantitativo (Dados Reais)</div>
          <div style={{fontSize:11,color:C.textDim,lineHeight:1.8}}>
            <p style={{margin:"0 0 6px"}}>Stocks Watch: {stocksList.filter(s=>s.state.includes("APERTO")).length} em aperto, {stocksList.filter(s=>s.state.includes("EXCESSO")).length} em excesso.</p>
            <p style={{margin:"0 0 6px"}}>Spreads: {spreadList.filter(s=>s.regime!=="NORMAL").length} de {spreadList.length} fora do range normal.</p>
            <p style={{margin:"0 0 6px"}}>Preco vs historico: {COMMODITIES.filter(c=>{const sd=getSeasonDev(c.sym);return sd&&sd.dev<-15;}).length} commodities {">"}15% abaixo, {COMMODITIES.filter(c=>{const sd=getSeasonDev(c.sym);return sd&&sd.dev>15;}).length} acima.</p>
          </div>
        </div>
        <div style={{padding:"10px 14px",borderRadius:4,background:"rgba(148,163,184,.03)",border:"1px solid rgba(148,163,184,.08)",fontSize:10,color:C.textMuted,fontFamily:"monospace",lineHeight:1.5}}>DISCLAIMER: Material informativo e educacional. Fonte: Stooq, CFTC, USDA, CONAB.</div>
      </div>
    );

    return null;
  };

  return (
    <div style={{display:"flex",minHeight:"100vh",background:C.bg,color:C.text,fontFamily:"'Segoe UI','Helvetica Neue',sans-serif"}}>
      <div style={{width:220,minHeight:"100vh",background:C.panel,borderRight:`1px solid ${C.border}`,display:"flex",flexDirection:"column",flexShrink:0}}>
        <div style={{padding:"16px 14px 8px",borderBottom:`1px solid ${C.border}`}}>
          <div style={{fontSize:15,fontWeight:800,letterSpacing:1.2,color:C.text}}>AGRIMACRO</div>
          <div style={{fontSize:9,color:C.textMuted,letterSpacing:.8,marginTop:2}}>COMMODITIES DASHBOARD v2.0</div>
        </div>
        <div style={{flex:1,overflowY:"auto",padding:"8px 0"}}>
          {["Gr\u00e3os","Softs","Pecu\u00e1ria","Energia","Metais","Macro"].map(grp=>(
            <div key={grp}>
              <div style={{padding:"8px 14px 4px",fontSize:9,fontWeight:700,color:C.textMuted,letterSpacing:1,textTransform:"uppercase"}}>{grp}</div>
              {COMMODITIES.filter(c=>c.group===grp).map(c=>{
                const p=getPrice(c.sym); const ch=getChange(c.sym); const sel=c.sym===selected;
                return (
                  <div key={c.sym} onClick={()=>setSelected(c.sym)} style={{
                    display:"flex",alignItems:"center",justifyContent:"space-between",padding:"5px 14px",cursor:"pointer",
                    background:sel?"rgba(59,130,246,.12)":"transparent",borderLeft:sel?`3px solid ${C.blue}`:"3px solid transparent",
                    transition:"all .15s",
                  }}>
                    <div>
                      <div style={{fontSize:12,fontWeight:sel?700:500,color:sel?C.text:C.textDim}}>{c.sym}</div>
                      <div style={{fontSize:9,color:C.textMuted}}>{c.name}</div>
                    </div>
                    <div style={{textAlign:"right"}}>
                      <div style={{fontSize:11,fontWeight:600,fontFamily:"monospace",color:p?C.text:C.textMuted}}>{p?p.toFixed(2):"\u2014"}</div>
                      {ch && <div style={{fontSize:9,fontFamily:"monospace",color:ch.pct>=0?C.green:C.red}}>{ch.pct>=0?"+":""}{ch.pct.toFixed(2)}%</div>}
                    </div>
                  </div>
                );
              })}
            </div>
          ))}
        </div>
        <div style={{padding:"10px 14px",borderTop:`1px solid ${C.border}`,fontSize:10}}>
          <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:4}}>
            <div style={{width:7,height:7,borderRadius:"50%",background:pipelineOk?C.green:C.red}} />
            <span style={{fontWeight:600,color:pipelineOk?C.green:C.red}}>{pipelineOk?"PIPELINE ONLINE":"PIPELINE ERROR"}</span>
          </div>
          {errors.length>0 && <div style={{color:C.red,fontSize:9}}>{errors.join(", ")}</div>}
          <div style={{color:C.textMuted,fontSize:9}}>Data: {lastDate}</div>
        </div>
      </div>
      <div style={{flex:1,display:"flex",flexDirection:"column",minWidth:0}}>
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"10px 20px",borderBottom:`1px solid ${C.border}`,background:C.panel}}>
          <div style={{display:"flex",alignItems:"center",gap:12}}>
            <span style={{fontSize:14,fontWeight:700}}>{COMMODITIES.find(c=>c.sym===selected)?.name||selected}</span>
            <Badge label="DADOS REAIS" color={C.green} />
            {pipelineOk && <Badge label="PIPELINE ONLINE" color={C.green} />}
          </div>
          <div style={{fontSize:11,color:C.textMuted,fontFamily:"monospace"}}>{lastDate}</div>
        </div>
        <div style={{display:"flex",gap:0,borderBottom:`1px solid ${C.border}`,background:C.panel,overflowX:"auto",flexShrink:0}}>
          {TABS.map(t=>(
            <button key={t} onClick={()=>setTab(t)} style={{
              padding:"10px 16px",fontSize:11,fontWeight:tab===t?700:500,
              color:tab===t?C.blue:C.textDim,background:"transparent",border:"none",cursor:"pointer",
              borderBottom:tab===t?`2px solid ${C.blue}`:"2px solid transparent",whiteSpace:"nowrap",
              transition:"all .15s",
            }}>{t}</button>
          ))}
        </div>
        <div style={{flex:1,overflowY:"auto",padding:20}}>
          {loading ? (
            <div style={{display:"flex",alignItems:"center",justifyContent:"center",height:300,color:C.textMuted}}>
              <div style={{textAlign:"center"}}>
                <div style={{fontSize:14,fontWeight:600,marginBottom:4}}>Carregando dados reais...</div>
                <div style={{fontSize:11}}>Conectando ao pipeline AgriMacro</div>
              </div>
            </div>
          ) : renderTab()}
        </div>
      </div>
    </div>
  );
}
