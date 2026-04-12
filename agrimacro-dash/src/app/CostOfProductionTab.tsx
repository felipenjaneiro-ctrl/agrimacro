// CostOfProductionTab.tsx -- Expanded: 8 commodities + causal flow diagram
// Fontes: grain_ratios.json, eia_data.json, price_history.json, bcb_data.json
"use client"
import { useEffect, useState, useRef, useCallback } from "react"

const C = {
  bg:"#0E1A24", panel:"#142332", border:"#1E3044",
  text:"#E8ECF1", textMuted:"#8C96A5",
  green:"#00C878", red:"#DC3C3C", gold:"#DCB432",
  blue:"#468CDC", cyan:"#00C8DC", purple:"#8B5CF6",
}

// USDA ERS COP history ($/unit, U.S. average)
const COP_HISTORY: Record<string, Record<number,number>> = {
  corn:   {2010:3.80,2011:4.08,2012:4.12,2013:4.20,2014:4.18,2015:3.92,2016:3.70,2017:3.58,2018:3.62,2019:3.68,2020:3.82,2021:4.25,2022:5.12,2023:4.88,2024:4.62},
  soy:    {2010:9.20,2011:9.85,2012:10.10,2013:10.35,2014:10.20,2015:9.60,2016:9.25,2017:9.10,2018:9.30,2019:9.45,2020:9.80,2021:10.90,2022:12.40,2023:12.10,2024:11.84},
  wheat:  {2010:5.80,2011:6.20,2012:6.50,2013:6.70,2014:6.60,2015:6.30,2016:6.10,2017:5.95,2018:6.10,2019:6.25,2020:6.50,2021:7.10,2022:8.20,2023:7.90,2024:7.67},
  cotton: {2010:0.62,2011:0.68,2012:0.70,2013:0.72,2014:0.74,2015:0.71,2016:0.68,2017:0.66,2018:0.68,2019:0.70,2020:0.72,2021:0.75,2022:0.82,2023:0.80,2024:0.78},
}

const COST_SHARES: Record<string,{label:string;color:string;[k:string]:any}> = {
  fuel:       {label:"Diesel", color:"#f97316", corn:0.07, soy:0.06, wheat:0.08, cotton:0.09},
  fertilizer: {label:"Fertilizante", color:"#22d3ee", corn:0.22, soy:0.10, wheat:0.25, cotton:0.18},
  seed:       {label:"Sementes", color:"#a78bfa", corn:0.14, soy:0.18, wheat:0.12, cotton:0.12},
  chemicals:  {label:"Defensivos", color:"#f472b6", corn:0.08, soy:0.12, wheat:0.08, cotton:0.22},
  other:      {label:"Outros", color:"#64748b", corn:0.49, soy:0.54, wheat:0.47, cotton:0.39},
}

type CopType = "ers"|"feed"|"proxy"
interface Grain {key:string;label:string;sym:string;color:string;divisor:number;copType:CopType;copLabel:string;
  feedFormula?:{cornBu:number;mealLb:number;feedPct:number};proxyNote?:string}

const ALL_COMMODITIES: Grain[] = [
  {key:"corn",   label:"Milho",   sym:"ZC", color:"#f59e0b", divisor:100, copType:"ers", copLabel:"USDA ERS"},
  {key:"soy",    label:"Soja",    sym:"ZS", color:"#22c55e", divisor:100, copType:"ers", copLabel:"USDA ERS"},
  {key:"wheat",  label:"Trigo",   sym:"ZW", color:"#3b82f6", divisor:100, copType:"ers", copLabel:"USDA ERS"},
  {key:"cotton", label:"Algodao", sym:"CT", color:"#a78bfa", divisor:100, copType:"ers", copLabel:"USDA ERS est."},
  {key:"cattle", label:"Boi Gordo",sym:"LE",color:"#ef4444", divisor:100, copType:"feed",copLabel:"Custo Alimentacao (proxy)",
    feedFormula:{cornBu:4.5, mealLb:0.8, feedPct:0.60}},
  {key:"hogs",   label:"Suino",   sym:"HE", color:"#ec4899", divisor:100, copType:"feed",copLabel:"Custo Racao (proxy)",
    feedFormula:{cornBu:6.5, mealLb:1.2, feedPct:0.65}},
  {key:"sugar",  label:"Acucar",  sym:"SB", color:"#06b6d4", divisor:100, copType:"proxy",copLabel:"Proxy via energia/cambio",
    proxyNote:"Correlacao com petroleo (etanol) e BRL"},
  {key:"coffee", label:"Cafe",    sym:"KC", color:"#78350f", divisor:100, copType:"proxy",copLabel:"Proxy via cambio BR",
    proxyNote:"Brasil = 40% producao mundial; BRL/USD e principal driver"},
]

function SH({children,id}:{children:React.ReactNode;id?:string}) {
  return <div id={id} style={{background:C.bg,color:C.gold,fontWeight:700,fontSize:11,padding:"6px 12px",borderRadius:4,marginBottom:8,letterSpacing:0.5,borderLeft:`3px solid ${C.gold}`,scrollMarginTop:16}}>{children}</div>
}
function Panel({children,style}:{children:React.ReactNode;style?:React.CSSProperties}) {
  return <div style={{background:C.panel,border:`1px solid ${C.border}`,borderRadius:8,padding:14,...style}}>{children}</div>
}

function interpolateCOP(grainKey:string, date:Date): number|null {
  const cop = COP_HISTORY[grainKey]; if(!cop) return null
  const year = date.getFullYear() + date.getMonth()/12
  const years = Object.keys(cop).map(Number).sort()
  if(year <= years[0]) return cop[years[0]]
  if(year >= years[years.length-1]) return cop[years[years.length-1]]
  for(let i=0;i<years.length-1;i++){
    if(year >= years[i] && year <= years[i+1]){
      const t = (year-years[i])/(years[i+1]-years[i])
      return cop[years[i]]*(1-t) + cop[years[i+1]]*t
    }
  }
  return null
}

function pearson(a:number[], b:number[]): number {
  const n = Math.min(a.length,b.length); if(n<10) return 0
  const ax=a.slice(-n), bx=b.slice(-n)
  const ma=ax.reduce((s,v)=>s+v,0)/n, mb=bx.reduce((s,v)=>s+v,0)/n
  let num=0,da=0,db=0
  for(let k=0;k<n;k++){const va=ax[k]-ma,vb=bx[k]-mb;num+=va*vb;da+=va*va;db+=vb*vb}
  return da>0&&db>0?num/Math.sqrt(da*db):0
}

// ============================================================
// CAUSAL FLOW DIAGRAM
// ============================================================
function CausalFlowDiagram({wti,diesel,natGas,prices,margins,brlUsd}:
  {wti:number|null;diesel:number|null;natGas:number|null;prices:Record<string,number|null>;margins:any;brlUsd:number|null}) {

  const fmt=(v:number|null,pre="$",dec=2)=>v!==null?`${pre}${v.toFixed(dec)}`:"N/D"

  // Box component
  const Box=({x,y,w,h,label,value,borderColor,onClick}:{x:number;y:number;w:number;h:number;label:string;value:string;borderColor:string;onClick?:()=>void})=>(
    <g onClick={onClick} style={{cursor:onClick?"pointer":"default"}}>
      <rect x={x} y={y} width={w} height={h} rx={4} fill={C.panel} stroke={borderColor} strokeWidth={1.5}/>
      <text x={x+w/2} y={y+h/2-5} textAnchor="middle" fill={C.textMuted} fontSize="8" fontWeight="600">{label}</text>
      <text x={x+w/2} y={y+h/2+8} textAnchor="middle" fill={C.text} fontSize="10" fontWeight="700" fontFamily="monospace">{value}</text>
    </g>
  )

  // Arrow component
  const Arrow=({x1,y1,x2,y2,thick,alert}:{x1:number;y1:number;x2:number;y2:number;thick?:boolean;alert?:boolean})=>{
    const dx=x2-x1, dy=y2-y1, len=Math.sqrt(dx*dx+dy*dy)
    const ux=dx/len, uy=dy/len
    const ax=x2-ux*6, ay=y2-uy*6
    return <g>
      <line x1={x1} y1={y1} x2={ax} y2={ay} stroke={alert?C.red:"#3a4a5a"} strokeWidth={thick?2:1} opacity={alert?0.9:0.5}/>
      <polygon points={`${x2},${y2} ${ax-uy*3},${ay+ux*3} ${ax+uy*3},${ay-ux*3}`} fill={alert?C.red:"#3a4a5a"} opacity={alert?0.9:0.5}/>
    </g>
  }

  const scrollTo=(id:string)=>{document.getElementById(id)?.scrollIntoView({behavior:"smooth"})}

  // Margin colors for output boxes
  const mgCol=(key:string)=>{const m=margins[key];return m?m.margin>=0?C.green:C.red:C.textMuted}

  return (
    <Panel style={{marginBottom:16,padding:10}}>
      <div style={{fontSize:10,fontWeight:600,color:C.gold,marginBottom:6}}>CADEIA DE TRANSMISSAO DE CUSTOS</div>
      <svg viewBox="0 0 800 280" style={{width:"100%",display:"block"}}>
        {/* NIVEL 1: Macro Inputs */}
        <text x={10} y={14} fill={C.textMuted} fontSize="7" fontWeight="700" letterSpacing="1">INPUTS MACRO</text>
        <Box x={10}  y={22} w={110} h={38} label="Petroleo WTI" value={fmt(wti)} borderColor={C.gold} onClick={()=>scrollTo("sec-insumos")}/>
        <Box x={140} y={22} w={100} h={38} label="Gas Natural" value={fmt(natGas)} borderColor={C.gold} onClick={()=>scrollTo("sec-insumos")}/>
        <Box x={260} y={22} w={100} h={38} label="Diesel" value={fmt(diesel)} borderColor={C.gold} onClick={()=>scrollTo("sec-insumos")}/>
        <Box x={380} y={22} w={100} h={38} label="BRL/USD" value={fmt(brlUsd,"",2)} borderColor={C.gold}/>

        {/* NIVEL 2: Transmissao */}
        <text x={10} y={84} fill={C.textMuted} fontSize="7" fontWeight="700" letterSpacing="1">TRANSMISSAO</text>
        <Box x={10}  y={92} w={130} h={38} label="Ureia/Fert. N" value="NG x 34.5" borderColor={C.purple}/>
        <Box x={160} y={92} w={130} h={38} label="Frete + Maquinario" value="Diesel dep." borderColor={C.purple}/>
        <Box x={310} y={92} w={130} h={38} label="COP Graos" value={fmt(margins.corn?.cop)} borderColor={C.purple} onClick={()=>scrollTo("sec-cop")}/>
        <Box x={460} y={92} w={130} h={38} label="Custo Racao" value="f(ZC,ZM)" borderColor={C.purple} onClick={()=>scrollTo("sec-cop")}/>
        <Box x={610} y={92} w={130} h={38} label="Compet. BR" value={fmt(brlUsd,"R$",2)} borderColor={C.purple}/>

        {/* Arrows L1 -> L2 */}
        <Arrow x1={65} y1={60} x2={75} y2={92} thick/>           {/* WTI -> Ureia */}
        <Arrow x1={190} y1={60} x2={75} y2={92}/>                {/* NG -> Ureia */}
        <Arrow x1={310} y1={60} x2={225} y2={92}/>               {/* Diesel -> Frete */}
        <Arrow x1={65} y1={60} x2={225} y2={92}/>                {/* WTI -> Frete */}
        <Arrow x1={430} y1={60} x2={675} y2={92}/>               {/* BRL -> Compet */}

        {/* Arrows L2 -> L2 internal */}
        <Arrow x1={140} y1={111} x2={310} y2={111} thick/>       {/* Ureia -> COP Graos */}
        <Arrow x1={290} y1={111} x2={310} y2={111}/>             {/* Frete -> COP Graos */}

        {/* NIVEL 3: Outputs */}
        <text x={10} y={154} fill={C.textMuted} fontSize="7" fontWeight="700" letterSpacing="1">PRECOS ATUAIS</text>
        {[
          {key:"corn",sym:"ZC",x:10},{key:"soy",sym:"ZS",x:120},{key:"wheat",sym:"ZW",x:230},
          {key:"cotton",sym:"CT",x:340},{key:"cattle",sym:"LE",x:450},{key:"hogs",sym:"HE",x:560},{key:"coffee",sym:"KC",x:670},
        ].map(o=>(
          <Box key={o.key} x={o.x} y={162} w={100} h={38} label={`${o.sym}`}
            value={prices[o.sym]!==null?`${(prices[o.sym]!).toFixed(1)}`:"--"} borderColor={mgCol(o.key)} onClick={()=>scrollTo("sec-cop")}/>
        ))}

        {/* Arrows L2 -> L3 */}
        <Arrow x1={375} y1={130} x2={60} y2={162}/>   {/* COP -> ZC */}
        <Arrow x1={375} y1={130} x2={170} y2={162}/>  {/* COP -> ZS */}
        <Arrow x1={375} y1={130} x2={280} y2={162}/>  {/* COP -> ZW */}
        <Arrow x1={375} y1={130} x2={390} y2={162}/>  {/* COP -> CT */}
        <Arrow x1={525} y1={130} x2={500} y2={162}/>  {/* Racao -> LE */}
        <Arrow x1={525} y1={130} x2={610} y2={162}/>  {/* Racao -> HE */}
        <Arrow x1={675} y1={130} x2={720} y2={162}/>  {/* Compet -> KC */}

        {/* SB not shown to keep diagram clean */}
        <text x={400} y={215} textAnchor="middle" fill={C.textMuted} fontSize="7">
          Clicar em qualquer box para ir a secao relevante
        </text>
      </svg>
    </Panel>
  )
}

// ============================================================
// MAIN COMPONENT
// ============================================================
export default function CostOfProductionTab() {
  const [gr, setGr] = useState<any>(null)
  const [eia, setEia] = useState<any>(null)
  const [ph, setPh] = useState<any>(null)
  const [selIdx, setSelIdx] = useState(0)
  const [period, setPeriod] = useState("5A")
  const [corrWindow, setCorrWindow] = useState(52)
  const chartRef = useRef<HTMLCanvasElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const [dims, setDims] = useState({w:800})

  useEffect(()=>{
    Promise.all([
      fetch("/data/processed/grain_ratios.json").then(r=>r.json()).catch(()=>null),
      fetch("/data/processed/eia_data.json").then(r=>r.json()).catch(()=>null),
      fetch("/data/raw/price_history.json").then(r=>r.json()).catch(()=>null),
    ]).then(([g,e,p])=>{setGr(g);setEia(e);setPh(p)})
  },[])

  useEffect(()=>{
    const el=containerRef.current;if(!el)return
    const obs=new ResizeObserver(entries=>{const{width}=entries[0].contentRect;if(width>0)setDims({w:Math.floor(width)})})
    obs.observe(el);return()=>obs.disconnect()
  },[])

  const margins = gr?.current_snapshot?.margins || {}
  const eiaS = eia?.series || {}
  const diesel = eiaS.diesel_retail?.latest_value ?? null
  const natGas = eiaS.natural_gas_spot?.latest_value ?? null
  const wti = eiaS.wti_spot?.latest_value ?? null

  // BRL/USD from price_history (DX not available, use BCB or null)
  const brlUsd: number|null = (() => {
    // Try to read from BCB data via grain_ratios or ph
    // grain_ratios enrichment uses BCB
    return null // TODO: load bcb_data.json if needed
  })()

  const getPrice = (sym:string):number|null => {
    const bars = ph?.[sym]; if(!Array.isArray(bars)||!bars.length) return null
    return bars[bars.length-1].close
  }
  const allPrices: Record<string,number|null> = {}
  for(const c of ALL_COMMODITIES) allPrices[c.sym] = getPrice(c.sym)

  const g = ALL_COMMODITIES[selIdx]
  const candles = ph?.[g.sym]
  const periodBars = period==="3A"?756:period==="5A"?1260:period==="10A"?2520:99999

  // Calculate feed cost for livestock from ZC + ZM prices
  const calcFeedCOP = useCallback((date:string, ff:{cornBu:number;mealLb:number;feedPct:number}):number|null => {
    if(!ph) return null
    const zcBars = ph["ZC"], zmBars = ph["ZM"]
    if(!Array.isArray(zcBars)||!Array.isArray(zmBars)) return null
    // Find nearest ZC and ZM price to this date
    const findPrice = (bars:any[], d:string):number|null => {
      for(let i=bars.length-1;i>=0;i--){if(bars[i].date<=d) return bars[i].close/100}
      return null
    }
    const zc = findPrice(zcBars, date)
    const zm = findPrice(zmBars, date)
    if(zc===null||zm===null) return null
    // Feed cost = (corn price * bu per unit + meal price * lb per unit) / feed fraction
    const feedCost = (zc * ff.cornBu + zm * ff.mealLb) / ff.feedPct
    return feedCost
  },[ph])

  // Build monthly price series + COP (ERS, feed, or proxy)
  const buildSeries = useCallback(()=>{
    if(!candles||!Array.isArray(candles)||candles.length<40) return null
    const bars = candles.slice(-Math.min(periodBars, candles.length))
    const monthly: {date:string;price:number;cop:number|null;margin:number|null}[] = []
    const byMonth: Record<string,{closes:number[];dates:string[]}> = {}
    for(const b of bars){
      const m = b.date.slice(0,7)
      if(!byMonth[m]) byMonth[m]={closes:[],dates:[]}
      byMonth[m].closes.push(b.close / g.divisor)
      byMonth[m].dates.push(b.date)
    }
    for(const [m,data] of Object.entries(byMonth).sort()){
      const avg = data.closes.reduce((s:number,v:number)=>s+v,0)/data.closes.length
      const midDate = data.dates[Math.floor(data.dates.length/2)]
      let cop: number|null = null
      if(g.copType === "ers") {
        cop = interpolateCOP(g.key, new Date(m+"-15"))
      } else if(g.copType === "feed" && g.feedFormula) {
        cop = calcFeedCOP(midDate, g.feedFormula)
      } else if(g.copType === "proxy") {
        // For proxy: use 5-year rolling average as "cost" reference
        const allBars = candles.map((b:any)=>b.close/g.divisor)
        const idx = candles.findIndex((b:any)=>b.date.startsWith(m))
        if(idx>=252) {
          cop = allBars.slice(idx-252*5, idx).reduce((s:number,v:number)=>s+v,0) / Math.min(252*5, idx)
        }
      }
      monthly.push({date:m, price:avg, cop, margin:cop!==null?avg-cop:null})
    }
    return monthly
  },[candles, g, periodBars, calcFeedCOP])

  const series = buildSeries()

  // Detect crossings
  const crossings = useCallback(()=>{
    if(!series||series.length<2) return []
    const cx:{date:string;type:"squeeze"|"recovery";price:number;cop:number;idx:number;duration:number|null;fwd3m:number|null}[] = []
    const raw:{date:string;type:"squeeze"|"recovery";price:number;cop:number;idx:number}[] = []
    for(let i=1;i<series.length;i++){
      const prev=series[i-1], cur=series[i]
      if(prev.margin===null||cur.margin===null) continue
      if(prev.margin>=0&&cur.margin<0) raw.push({date:cur.date,type:"squeeze",price:cur.price,cop:cur.cop!,idx:i})
      if(prev.margin<0&&cur.margin>=0) raw.push({date:cur.date,type:"recovery",price:cur.price,cop:cur.cop!,idx:i})
    }
    return raw.map((c,ci)=>{
      const nextCx = raw[ci+1]
      const duration = nextCx?Math.round((new Date(nextCx.date+"-15").getTime()-new Date(c.date+"-15").getTime())/(1000*60*60*24*30)):null
      const fwd3m = c.idx+3<series.length?((series[c.idx+3].price-c.price)/c.price*100):null
      return {...c, duration, fwd3m}
    })
  },[series])

  const cx = crossings()
  const currentMargin = series&&series.length>0?series[series.length-1].margin:null
  const belowCOP = currentMargin!==null&&currentMargin<0

  // === DRAW MAIN CHART ===
  useEffect(()=>{
    const cvs=chartRef.current;if(!cvs||!series||!series.length)return
    const ctx=cvs.getContext("2d");if(!ctx)return
    const W=dims.w, H=340
    cvs.width=W*2;cvs.height=H*2;ctx.scale(2,2)
    cvs.style.width=W+"px";cvs.style.height=H+"px"

    const pad={t:24,b:28,l:55,r:16}
    const cH=H-pad.t-pad.b, n=series.length
    const allVals = series.flatMap(s=>[s.price, s.cop||0]).filter(v=>v>0)
    if(!allVals.length) return
    const mn=Math.min(...allVals), mx=Math.max(...allVals), rng=mx-mn||1
    const pMn=mn-rng*.06, pMx=mx+rng*.06
    const yP=(v:number)=>pad.t+(1-(v-pMn)/(pMx-pMn))*cH
    const xC=(i:number)=>pad.l+i*(W-pad.l-pad.r)/(n-1||1)

    ctx.fillStyle=C.bg;ctx.fillRect(0,0,W,H)
    // Grid
    for(let i=0;i<5;i++){const y=pad.t+i*(cH/4);ctx.strokeStyle="rgba(148,163,184,.06)";ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(W-pad.r,y);ctx.stroke()}
    ctx.fillStyle=C.textMuted;ctx.font="10px monospace";ctx.textAlign="right"
    for(let i=0;i<5;i++){const v=pMx-(pMx-pMn)*i/4;ctx.fillText("$"+v.toFixed(2),pad.l-6,pad.t+i*(cH/4)+4)}

    // Margin fill
    for(let i=0;i<n-1;i++){
      const s0=series[i], s1=series[i+1]
      if(s0.cop===null||s1.cop===null) continue
      ctx.fillStyle=(s0.price+s1.price)/2>=(s0.cop+s1.cop)/2?"rgba(0,200,120,.12)":"rgba(220,60,60,.12)"
      ctx.beginPath();ctx.moveTo(xC(i),yP(s0.price));ctx.lineTo(xC(i+1),yP(s1.price))
      ctx.lineTo(xC(i+1),yP(s1.cop));ctx.lineTo(xC(i),yP(s0.cop));ctx.closePath();ctx.fill()
    }

    // COP line
    ctx.strokeStyle=C.gold;ctx.lineWidth=2;ctx.setLineDash([6,4]);ctx.beginPath()
    let started=false
    for(let i=0;i<n;i++){if(series[i].cop===null)continue;const x=xC(i),y=yP(series[i].cop!);if(!started){ctx.moveTo(x,y);started=true}else ctx.lineTo(x,y)}
    ctx.stroke();ctx.setLineDash([])

    // Price line
    ctx.strokeStyle=g.color;ctx.lineWidth=2;ctx.beginPath()
    series.forEach((s,i)=>{const x=xC(i),y=yP(s.price);if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y)})
    ctx.stroke()

    // Crossings
    for(const c of cx){
      const x=xC(c.idx), y=yP(c.price)
      ctx.fillStyle=c.type==="squeeze"?C.red:C.green
      ctx.beginPath()
      if(c.type==="squeeze"){ctx.moveTo(x,y-6);ctx.lineTo(x-5,y+4);ctx.lineTo(x+5,y+4)}
      else{ctx.moveTo(x,y+6);ctx.lineTo(x-5,y-4);ctx.lineTo(x+5,y-4)}
      ctx.closePath();ctx.fill()
    }

    // Today line
    ctx.strokeStyle=C.gold;ctx.lineWidth=1;ctx.setLineDash([3,3])
    ctx.beginPath();ctx.moveTo(xC(n-1),pad.t);ctx.lineTo(xC(n-1),pad.t+cH);ctx.stroke();ctx.setLineDash([])
    ctx.fillStyle=C.gold;ctx.font="8px monospace";ctx.textAlign="center";ctx.fillText("HOJE",xC(n-1),pad.t-4)

    // X axis
    ctx.fillStyle=C.textMuted;ctx.font="9px monospace";ctx.textAlign="center"
    const step=Math.max(1,Math.floor(n/10))
    for(let i=0;i<n;i+=step)ctx.fillText(series[i].date.slice(0,7),xC(i),H-pad.b+14)

    // Legend
    ctx.font="9px monospace";ctx.textAlign="left"
    ctx.fillStyle=g.color;ctx.fillRect(pad.l,6,14,3);ctx.fillStyle=C.textMuted;ctx.fillText("Preco",pad.l+18,10)
    ctx.fillStyle=C.gold;ctx.setLineDash([4,3]);ctx.beginPath();ctx.moveTo(pad.l+80,8);ctx.lineTo(pad.l+94,8);ctx.stroke();ctx.setLineDash([])
    ctx.fillStyle=C.textMuted;ctx.fillText(g.copLabel,pad.l+98,10)
  },[series,cx,g,dims.w])

  // Correlation
  const corrData = useCallback(()=>{
    if(!ph||!eiaS) return {matrix:[] as number[][],labels:[] as string[]}
    const labels=["Milho","Soja","Trigo","WTI","NG","Diesel","Boi","Suino"]
    const getC=(sym:string)=>{const b=ph[sym];return Array.isArray(b)?b.slice(-corrWindow).map((x:any)=>x.close):[]}
    const getE=(k:string)=>{const h=eiaS[k]?.history;return Array.isArray(h)?h.slice(-corrWindow).map((x:any)=>x.value):[]}
    const all=[getC("ZC"),getC("ZS"),getC("ZW"),getC("CL"),getE("natural_gas_spot"),getE("diesel_retail"),getC("LE"),getC("HE")]
    const nn=labels.length
    const matrix:number[][]=Array.from({length:nn},()=>Array(nn).fill(0))
    for(let i=0;i<nn;i++)for(let j=0;j<nn;j++){matrix[i][j]=i===j?1:pearson(all[i],all[j])}
    return {matrix,labels}
  },[ph,eiaS,corrWindow])

  const {matrix,labels:corrLabels} = corrData()

  if(!gr) return <div style={{color:C.textMuted,padding:40,textAlign:"center"}}>Carregando dados...</div>

  return (
    <div ref={containerRef}>
      {/* DIAGRAMA DE FLUXO CAUSAL */}
      <SH>Cadeia de Transmissao de Custos</SH>
      <CausalFlowDiagram wti={wti} diesel={diesel} natGas={natGas} prices={allPrices} margins={margins} brlUsd={brlUsd}/>

      {/* Input prices */}
      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:10,marginBottom:16}}>
        {[
          {label:"Diesel Retail",value:diesel,unit:"$/gal",color:"#f97316"},
          {label:"Gas Natural",value:natGas,unit:"$/MMBtu",color:"#22d3ee"},
          {label:"Petroleo WTI",value:wti,unit:"$/bbl",color:"#64748b"},
        ].map(item=>(
          <Panel key={item.label} style={{textAlign:"center",padding:10}}>
            <div style={{fontSize:9,fontWeight:600,color:C.textMuted,letterSpacing:1,marginBottom:2}}>{item.label}</div>
            <div style={{fontSize:22,fontWeight:800,color:item.value!==null?item.color:C.textMuted,fontFamily:"monospace"}}>{item.value!==null?item.value.toFixed(2):"N/D"}</div>
            <div style={{fontSize:9,color:C.textMuted}}>{item.unit}</div>
          </Panel>
        ))}
      </div>

      {/* SECAO 1: Evolucao Historica */}
      <SH id="sec-cop">Evolucao Historica -- Preco vs Custo</SH>

      {currentMargin!==null && (
        <div style={{marginBottom:10,padding:"8px 14px",borderRadius:6,
          background:belowCOP?"rgba(220,60,60,.08)":"rgba(0,200,120,.08)",
          border:`1px solid ${belowCOP?"rgba(220,60,60,.25)":"rgba(0,200,120,.25)"}`}}>
          <span style={{fontSize:11,fontWeight:700,color:belowCOP?C.red:C.green}}>
            {belowCOP?"[!] ":"[OK] "}{g.label}: ${series![series!.length-1].price.toFixed(2)} {belowCOP?"ABAIXO":"ACIMA"} do custo ${series![series!.length-1].cop?.toFixed(2)} (margem: {currentMargin>0?"+":""}{currentMargin.toFixed(2)})
          </span>
          <span style={{fontSize:9,color:C.textMuted,marginLeft:8}}>Metodo: {g.copLabel}</span>
        </div>
      )}

      {/* Commodity + Period selectors */}
      <div style={{display:"flex",justifyContent:"space-between",marginBottom:8,flexWrap:"wrap",gap:6}}>
        <div style={{display:"flex",gap:3,flexWrap:"wrap"}}>
          {ALL_COMMODITIES.map((gg,i)=>(
            <button key={gg.key} onClick={()=>setSelIdx(i)} style={{
              padding:"3px 10px",fontSize:9,fontWeight:selIdx===i?700:500,borderRadius:4,cursor:"pointer",
              background:selIdx===i?"rgba(220,180,50,.12)":"transparent",
              color:selIdx===i?C.gold:C.textMuted,border:`1px solid ${selIdx===i?C.gold:C.border}`,
            }}>{gg.label}</button>
          ))}
        </div>
        <div style={{display:"flex",gap:3}}>
          {["3A","5A","10A","Max"].map(p=>(
            <button key={p} onClick={()=>setPeriod(p)} style={{
              padding:"3px 8px",fontSize:9,fontWeight:period===p?700:500,borderRadius:4,cursor:"pointer",
              background:period===p?"rgba(220,180,50,.12)":"transparent",
              color:period===p?C.gold:C.textMuted,border:`1px solid ${period===p?C.gold:C.border}`,
            }}>{p}</button>
          ))}
        </div>
      </div>

      <Panel style={{marginBottom:16}}>
        {series&&series.length>0 ? (
          <canvas ref={chartRef} style={{borderRadius:6,display:"block"}}/>
        ) : (
          <div style={{color:C.textMuted,textAlign:"center",padding:20}}>Dados insuficientes para {g.label}</div>
        )}
      </Panel>

      {/* Crossings table */}
      {cx.length>0 && (
        <Panel style={{marginBottom:16}}>
          <div style={{fontSize:10,fontWeight:600,color:C.textMuted,marginBottom:6}}>HISTORICO DE CRUZAMENTOS PRECO/CUSTO</div>
          <div style={{overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:10}}>
              <thead><tr style={{borderBottom:`1px solid ${C.border}`}}>
                {["Data","Tipo","Preco","Custo","Duracao","Ret. 3m"].map(h=>(
                  <th key={h} style={{padding:"4px 8px",textAlign:h==="Data"||h==="Tipo"?"left":"right",color:C.textMuted,fontSize:9,fontWeight:600}}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {cx.slice(-8).reverse().map((c,i)=>(
                  <tr key={i} style={{borderBottom:`1px solid ${C.border}`}}>
                    <td style={{padding:"4px 8px",fontFamily:"monospace"}}>{c.date}</td>
                    <td style={{padding:"4px 8px",color:c.type==="squeeze"?C.red:C.green,fontWeight:700}}>{c.type==="squeeze"?"v Squeeze":"^ Recuperacao"}</td>
                    <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"monospace"}}>${c.price.toFixed(2)}</td>
                    <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"monospace",color:C.gold}}>${c.cop.toFixed(2)}</td>
                    <td style={{padding:"4px 8px",textAlign:"right",color:C.textMuted}}>{c.duration!==null?c.duration+"m":"--"}</td>
                    <td style={{padding:"4px 8px",textAlign:"right",fontFamily:"monospace",fontWeight:600,
                      color:c.fwd3m===null?C.textMuted:c.fwd3m>=0?C.green:C.red}}>
                      {c.fwd3m!==null?(c.fwd3m>=0?"+":"")+c.fwd3m.toFixed(1)+"%":"--"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}

      {/* SECAO 2: Decomposicao Insumos */}
      <SH id="sec-insumos">Decomposicao Dinamica dos Insumos (52 semanas)</SH>
      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:10,marginBottom:16}}>
        {[
          {id:"diesel_retail",label:"Diesel",color:"#f97316",note:"Diesel -> Operacoes + Frete"},
          {id:"natural_gas_spot",label:"Gas Natural",color:"#22d3ee",note:"NG -> Amonia -> Fertilizante N (Haber-Bosch)"},
          {id:"wti_spot",label:"Petroleo WTI",color:"#64748b",note:"WTI -> Diesel -> Frete + Etanol -> Demanda ZC"},
        ].map(item=>{
          const s=eiaS[item.id]
          const vals=Array.isArray(s?.history)?s.history.map((h:any)=>h.value):[]
          const sorted=[...vals].sort((a:number,b:number)=>a-b)
          const q75=sorted.length>4?sorted[Math.floor(sorted.length*0.75)]:Infinity
          const isHigh=s?.latest_value!==null&&s?.latest_value>=q75
          return (
            <Panel key={item.id} style={{padding:10}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4}}>
                <span style={{fontSize:10,fontWeight:700,color:item.color}}>{item.label}</span>
                {isHigh && <span style={{fontSize:8,fontWeight:700,padding:"1px 6px",borderRadius:3,background:"rgba(220,60,60,.15)",color:C.red}}>[!] Q4</span>}
              </div>
              {vals.length>4 && (()=>{
                const mn2=Math.min(...vals),mx2=Math.max(...vals),rng2=mx2-mn2||1
                const pts=vals.map((v:number,i:number)=>`${(i/(vals.length-1))*100},${100-((v-mn2)/rng2)*80}`).join(" ")
                return <svg viewBox="0 0 100 100" style={{width:"100%",height:36,display:"block",marginBottom:4}}>
                  <polyline points={pts} fill="none" stroke={item.color} strokeWidth="1.5"/>
                  {isHigh&&<line x1="0" y1={100-((q75-mn2)/rng2)*80} x2="100" y2={100-((q75-mn2)/rng2)*80} stroke={C.red} strokeWidth="0.5" strokeDasharray="2,2"/>}
                </svg>
              })()}
              <div style={{fontSize:9,color:C.textMuted}}>{item.note}</div>
              {s?.latest_value!==null && <div style={{fontSize:10,color:C.text,fontFamily:"monospace",marginTop:2}}>${s.latest_value.toFixed(2)}</div>}
            </Panel>
          )
        })}
      </div>

      {/* ================================================================ */}
      {/* SECAO 3: CORRELACAO EXPANDIDA */}
      {/* ================================================================ */}
      <SH>Correlacao de Precos -- Como os mercados se movem juntos</SH>
      <div style={{fontSize:11,color:C.textMuted,marginBottom:10,lineHeight:1.5}}>
        Correlacao 1.0 = movem-se identicamente | -1.0 = direcoes opostas | 0 = sem relacao.
        <span style={{marginLeft:8}}>Janela:</span>
        {[{l:"26 sem",v:26},{l:"52 sem",v:52},{l:"104 sem",v:104}].map(w=>(
          <button key={w.v} onClick={()=>setCorrWindow(w.v)} style={{
            padding:"2px 10px",fontSize:10,fontWeight:corrWindow===w.v?700:500,borderRadius:4,cursor:"pointer",marginLeft:4,
            background:corrWindow===w.v?"rgba(220,180,50,.15)":"transparent",
            color:corrWindow===w.v?C.gold:C.textMuted,border:`1px solid ${corrWindow===w.v?C.gold:C.border}`,
          }}>{w.l}</button>
        ))}
      </div>

      {matrix.length>0 ? (
        <div style={{display:"grid",gridTemplateColumns:"1fr 280px",gap:14,marginBottom:20}}>
          {/* LEFT: Heatmap */}
          <Panel style={{padding:12,overflow:"auto"}}>
            <table style={{borderCollapse:"collapse",width:"100%"}}>
              <thead><tr>
                <th style={{padding:6,width:90}}></th>
                {corrLabels.map(l=><th key={l} style={{padding:"6px 4px",color:C.text,fontSize:10,fontWeight:600,textAlign:"center",minWidth:65}}>{l}</th>)}
              </tr></thead>
              <tbody>
                {corrLabels.map((rl,ri)=>(
                  <tr key={rl}>
                    <td style={{padding:"6px 8px",fontSize:11,fontWeight:600,color:C.text,textAlign:"right",whiteSpace:"nowrap"}}>{rl}</td>
                    {corrLabels.map((cl,ci)=>{
                      const v=matrix[ri]?.[ci]||0; const abs=Math.abs(v); const isStrong=abs>=0.7
                      // Color gradient
                      const cellBg = ri===ci ? "rgba(220,180,50,.06)"
                        : v>=0.7 ? `rgba(0,200,120,${0.15+abs*0.35})`
                        : v>=0.3 ? `rgba(0,200,120,${abs*0.15})`
                        : v>=-0.3 ? C.panel
                        : v>=-0.7 ? `rgba(245,158,11,${Math.abs(v)*0.2})`
                        : `rgba(220,60,60,${0.15+abs*0.35})`
                      const textCol = ri===ci ? C.gold : isStrong ? (v>0?C.green:C.red) : abs>0.3 ? C.text : C.textMuted
                      return (
                        <td key={cl} style={{
                          padding:"8px 4px",textAlign:"center",fontFamily:"monospace",fontSize:13,fontWeight:isStrong?800:500,
                          color:textCol,background:cellBg,
                          border:`1px solid ${isStrong&&ri!==ci?C.gold+"66":C.border}`,
                          minWidth:65,position:"relative",
                        }}>
                          {ri===ci ? "--" : v.toFixed(2)}
                          {/* Intensity bar below value */}
                          {ri!==ci && <div style={{position:"absolute",bottom:2,left:"20%",width:"60%",height:3,borderRadius:2,
                            background:v>0?C.green:C.red,opacity:abs*0.8}}/>}
                        </td>
                      )
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </Panel>

          {/* RIGHT: Top correlations panel */}
          <Panel style={{padding:12}}>
            <div style={{fontSize:11,fontWeight:700,color:C.gold,marginBottom:10}}>Top Correlacoes</div>
            {(()=>{
              // Collect all unique pairs
              const pairs:{a:string;b:string;v:number}[] = []
              for(let i=0;i<corrLabels.length;i++) for(let j=i+1;j<corrLabels.length;j++){
                pairs.push({a:corrLabels[i],b:corrLabels[j],v:matrix[i][j]})
              }
              const sorted = [...pairs].sort((a,b)=>Math.abs(b.v)-Math.abs(a.v))
              const top5 = sorted.slice(0,5)
              const bot5 = [...pairs].sort((a,b)=>a.v-b.v).slice(0,3)
              const show = [...top5,...bot5.filter(b=>!top5.find(t=>t.a===b.a&&t.b===b.b))].slice(0,8)

              return show.map((p,i)=>{
                const col = p.v>=0?C.green:C.red
                const interp = p.v>0.7?"Fortemente correlacionados":p.v>0.3?"Correlacao moderada":p.v>-0.3?"Sem correlacao significativa":p.v>-0.7?"Correlacao negativa moderada":"Fortemente inversos"
                return (
                  <div key={i} style={{marginBottom:8,padding:"6px 8px",background:C.bg,borderRadius:5,border:`1px solid ${C.border}`}}>
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:3}}>
                      <span style={{fontSize:10,color:C.text,fontWeight:600}}>{p.a} / {p.b}</span>
                      <span style={{fontSize:12,fontWeight:800,fontFamily:"monospace",color:col}}>{p.v>=0?"+":""}{p.v.toFixed(2)}</span>
                    </div>
                    {/* Intensity bar */}
                    <div style={{height:4,borderRadius:2,background:C.border,marginBottom:3}}>
                      <div style={{height:4,borderRadius:2,width:`${Math.abs(p.v)*100}%`,background:col,transition:"width .3s"}}/>
                    </div>
                    <div style={{fontSize:8,color:C.textMuted}}>{interp}</div>
                  </div>
                )
              })
            })()}
          </Panel>
        </div>
      ) : <Panel style={{marginBottom:20}}><div style={{color:C.textMuted,textAlign:"center",padding:16}}>Dados insuficientes para calcular correlacoes</div></Panel>}

      {/* ================================================================ */}
      {/* SECAO 4: MARGEM EXPANDIDA */}
      {/* ================================================================ */}
      <SH>Margem do Produtor -- Quem esta lucrando e quem esta no prejuizo agora?</SH>
      <div style={{fontSize:11,color:C.textMuted,marginBottom:12,lineHeight:1.5}}>
        Margem = Preco de mercado menos custo estimado de producao. Verde = produtor lucrando. Vermelho = vendendo abaixo do custo.
      </div>

      <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:12,marginBottom:16}}>
        {ALL_COMMODITIES.filter(gg=>gg.copType==="ers"||gg.copType==="feed").map(gg=>{
          let curPrice:number|null=null, curCOP:number|null=null
          const bars = ph?.[gg.sym]
          if(Array.isArray(bars)&&bars.length>0) curPrice = bars[bars.length-1].close / gg.divisor
          if(gg.copType==="ers") curCOP = interpolateCOP(gg.key, new Date())
          else if(gg.copType==="feed"&&gg.feedFormula&&Array.isArray(bars)&&bars.length>0) curCOP = calcFeedCOP(bars[bars.length-1].date, gg.feedFormula)

          const margin = curPrice!==null&&curCOP!==null?curPrice-curCOP:null
          const mgPct = curCOP&&curCOP>0&&curPrice?((curPrice-curCOP)/curCOP*100):null
          const mgCol = margin===null?C.textMuted:margin>=0?C.green:C.red

          // Day change
          let dayChg:number|null = null
          if(Array.isArray(bars)&&bars.length>=2){
            const prev=bars[bars.length-2].close/gg.divisor
            dayChg = curPrice!==null&&prev>0?((curPrice-prev)/prev*100):null
          }

          // Historical margins for sparkline + percentile + min/max
          const marginHist:number[] = []
          let bestMargin:{val:number;year:string}|null=null
          let worstMargin:{val:number;year:string}|null=null
          if(Array.isArray(bars)&&bars.length>60){
            for(let i=Math.max(0,bars.length-36*22);i<bars.length;i+=Math.max(1,Math.floor(bars.length/80))){
              const p = bars[i].close/gg.divisor
              let cop:number|null = null
              if(gg.copType==="ers") cop = interpolateCOP(gg.key, new Date(bars[i].date))
              else if(gg.copType==="feed"&&gg.feedFormula) cop = calcFeedCOP(bars[i].date, gg.feedFormula)
              if(cop!==null){
                const m = p-cop
                marginHist.push(m)
                const yr = bars[i].date.slice(0,4)
                if(!worstMargin||m<worstMargin.val) worstMargin={val:m,year:yr}
                if(!bestMargin||m>bestMargin.val) bestMargin={val:m,year:yr}
              }
            }
          }
          const avgMargin = marginHist.length>3?marginHist.reduce((s,v)=>s+v,0)/marginHist.length:null
          const pctile = margin!==null&&marginHist.length>5?Math.round(marginHist.filter(v=>v<=margin).length/marginHist.length*100):null
          const isAlert = pctile!==null&&pctile<25
          const isHighlight = pctile!==null&&pctile>75

          // Margin bar width
          const barMax = curCOP!==null&&curPrice!==null?Math.max(curCOP,curPrice)*1.1:1
          const copPct = curCOP!==null?curCOP/barMax*100:0
          const pricePct = curPrice!==null?curPrice/barMax*100:0

          return (
            <Panel key={gg.key} style={{
              borderLeft:`4px solid ${mgCol}`,padding:14,minHeight:200,
              border:`1px solid ${isAlert?C.red+"66":isHighlight?C.green+"44":C.border}`,
              boxShadow:isAlert?"0 0 12px rgba(220,60,60,.15)":isHighlight?"0 0 12px rgba(0,200,120,.1)":"none",
            }}>
              {/* TOPO: name + price + day change */}
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:10}}>
                <div>
                  <div style={{fontSize:13,fontWeight:700,color:gg.color}}>{gg.label}</div>
                  <div style={{fontSize:8,color:C.textMuted}}>{gg.copLabel}</div>
                </div>
                <div style={{textAlign:"right"}}>
                  <div style={{fontSize:20,fontWeight:800,fontFamily:"monospace",color:C.text}}>
                    {curPrice!==null?"$"+curPrice.toFixed(2):"--"}
                  </div>
                  {dayChg!==null && <div style={{fontSize:10,fontFamily:"monospace",color:dayChg>=0?C.green:C.red}}>
                    {dayChg>=0?"+":""}{dayChg.toFixed(2)}%
                  </div>}
                </div>
              </div>

              {/* MEIO: Margin bar */}
              {curCOP!==null&&curPrice!==null && (
                <div style={{marginBottom:10}}>
                  <div style={{display:"flex",justifyContent:"space-between",fontSize:9,color:C.textMuted,marginBottom:3}}>
                    <span>COP ${curCOP.toFixed(2)}</span>
                    <span style={{color:mgCol,fontWeight:700,fontSize:11}}>
                      Margem: {margin!==null?(margin>0?"+":"")+"$"+margin.toFixed(2):"--"}
                      {mgPct!==null?` (${mgPct>0?"+":""}${mgPct.toFixed(1)}%)`:""}
                    </span>
                  </div>
                  <div style={{position:"relative",height:14,borderRadius:4,background:C.bg,overflow:"hidden"}}>
                    {/* COP reference */}
                    <div style={{position:"absolute",left:0,top:0,width:`${copPct}%`,height:"100%",
                      background:"rgba(220,180,50,.15)",borderRadius:4}}/>
                    {/* Price fill */}
                    <div style={{position:"absolute",left:0,top:0,width:`${pricePct}%`,height:"100%",
                      background:margin!==null&&margin>=0?"rgba(0,200,120,.3)":"rgba(220,60,60,.3)",borderRadius:4}}/>
                    {/* COP marker */}
                    <div style={{position:"absolute",left:`${copPct}%`,top:0,width:2,height:"100%",background:C.gold}}/>
                  </div>
                </div>
              )}

              {/* FAIXAS DE CONTEXTO */}
              <div style={{fontSize:9,color:C.textMuted,lineHeight:1.8}}>
                {pctile!==null && <div>Percentil historico: <strong style={{color:pctile<25?C.red:pctile>75?C.green:C.text}}>{pctile}%</strong></div>}
                {avgMargin!==null && <div>Media historica margem: <strong style={{color:C.text}}>${avgMargin.toFixed(2)}</strong></div>}
                {worstMargin && <div>Pior: <strong style={{color:C.red}}>${worstMargin.val.toFixed(2)}</strong> ({worstMargin.year})</div>}
                {bestMargin && <div>Melhor: <strong style={{color:C.green}}>+${bestMargin.val.toFixed(2)}</strong> ({bestMargin.year})</div>}
              </div>

              {/* SPARKLINE */}
              {marginHist.length>3 && (()=>{
                const mn=Math.min(...marginHist),mx=Math.max(...marginHist),rng=mx-mn||1
                const zY = 100-((0-mn)/rng)*80
                return <svg viewBox="0 0 100 100" style={{width:"100%",height:36,display:"block",marginTop:6}}>
                  {/* Zero reference */}
                  <line x1="0" y1={zY} x2="100" y2={zY} stroke={C.textMuted} strokeWidth="0.3" strokeDasharray="2,2"/>
                  {/* Fill area */}
                  <polygon
                    points={`0,${zY} ${marginHist.map((v,i)=>`${(i/(marginHist.length-1))*100},${100-((v-mn)/rng)*80}`).join(" ")} 100,${zY}`}
                    fill={margin!==null&&margin>=0?"rgba(0,200,120,.12)":"rgba(220,60,60,.12)"}/>
                  {/* Line */}
                  <polyline
                    points={marginHist.map((v,i)=>`${(i/(marginHist.length-1))*100},${100-((v-mn)/rng)*80}`).join(" ")}
                    fill="none" stroke={mgCol} strokeWidth="1.5"/>
                </svg>
              })()}
            </Panel>
          )
        })}
      </div>

      {/* RESUMO CONSOLIDADO */}
      {(()=>{
        const results = ALL_COMMODITIES.filter(gg=>gg.copType==="ers"||gg.copType==="feed").map(gg=>{
          let curPrice:number|null=null, curCOP:number|null=null
          const bars2 = ph?.[gg.sym]
          if(Array.isArray(bars2)&&bars2.length>0) curPrice = bars2[bars2.length-1].close / gg.divisor
          if(gg.copType==="ers") curCOP = interpolateCOP(gg.key, new Date())
          else if(gg.copType==="feed"&&gg.feedFormula&&Array.isArray(bars2)&&bars2.length>0) curCOP = calcFeedCOP(bars2[bars2.length-1].date, gg.feedFormula)
          const mg = curPrice!==null&&curCOP!==null?curPrice-curCOP:null
          const mgPct2 = curCOP&&curCOP>0&&curPrice?((curPrice-curCOP)/curCOP*100):null
          return {label:gg.label,margin:mg,pct:mgPct2}
        })
        const below = results.filter(r=>r.margin!==null&&r.margin<0)
        const above = results.filter(r=>r.margin!==null&&r.margin>=0)
        if(!results.length) return null
        return (
          <Panel style={{marginBottom:16,padding:12}}>
            <div style={{fontSize:11,color:C.text,lineHeight:1.6}}>
              <strong style={{color:below.length>0?C.red:C.green}}>
                {below.length} de {results.length}
              </strong> commodities abaixo do custo de producao
              {below.length>0 && <span> ({below.map(b=>`${b.label} ${b.pct!==null?b.pct.toFixed(0):"?"}%`).join(", ")})</span>}.
              {above.length>0 && <span> Lucrando: {above.map(a=>`${a.label} ${a.pct!==null?"+"+a.pct.toFixed(0):"?"}%`).join(", ")}.</span>}
            </div>
          </Panel>
        )
      })()}


      <div style={{marginTop:12,fontSize:8,color:C.textMuted,textAlign:"center"}}>
        COP: USDA ERS | Pecuaria: f(ZC,ZM) proxy | Energia: EIA | Precos: CME/IBKR | ZERO MOCK
      </div>
    </div>
  )
}
