"use client"
import { useEffect, useRef, useState, useCallback } from "react"

// Types needed
interface OHLCV { date:string;open:number;high:number;low:number;close:number;volume:number; }
interface COTHistoryEntry { date:string;[k:string]:any; }
interface Annotation { label:string;value:number;color:string; }

const C = {
  bg:"#0d1117", panel:"#161b22", panelAlt:"#1c2128", border:"rgba(148,163,184,.12)",
  text:"#e2e8f0", textDim:"#94a3b8", textMuted:"#64748b",
  green:"#00C878", red:"#DC3C3C", amber:"#f59e0b", blue:"#3b82f6",
  candleUp:"#00C878", candleDn:"#DC3C3C", wick:"rgba(148,163,184,.75)",
  ma9:"#DCB432", ma21:"#4ade80", ma50:"#3b82f6", ma200:"#f97316", bb:"rgba(148,163,184,.18)",
  rsiLine:"#e2e8f0", rsiOverbought:"rgba(239,68,68,.18)", rsiOversold:"rgba(34,197,94,.18)",
  gold:"#DCB432",
}

// --- Indicator calculations (duplicated to keep component standalone) ---
function emaCalc(data:number[],period:number):number[] {
  const k=2/(period+1); const ema:number[]=[];
  data.forEach((v,i)=>{ if(i===0)ema.push(v); else ema.push(v*k+ema[i-1]*(1-k)); });
  return ema;
}
function smaCalc(data:number[],period:number):number[] {
  const sma:number[]=[];
  for(let i=0;i<data.length;i++){ if(i<period-1){sma.push(NaN);continue;} sma.push(data.slice(i-period+1,i+1).reduce((a,b)=>a+b,0)/period); }
  return sma;
}
function bbCalc(data:number[],period=20,mult=2) {
  const middle=smaCalc(data,period); const upper:number[]=[]; const lower:number[]=[];
  for(let i=0;i<data.length;i++){
    if(i<period-1){upper.push(NaN);lower.push(NaN);continue;}
    const avg=middle[i]; const std=Math.sqrt(data.slice(i-period+1,i+1).reduce((s,v)=>s+(v-avg)**2,0)/period);
    upper.push(avg+mult*std); lower.push(avg-mult*std);
  }
  return {upper,middle,lower};
}
function rsiCalc(data:number[],period=14):number[] {
  const rsi:number[]=[]; let gains=0,losses=0;
  for(let i=0;i<data.length;i++){
    if(i===0){rsi.push(50);continue;} const change=data[i]-data[i-1];
    const gain=change>0?change:0; const loss=change<0?-change:0;
    if(i<period){gains+=gain;losses+=loss;rsi.push(50);}
    else if(i===period){gains+=gain;losses+=loss;const ag=gains/period;const al=losses/period;rsi.push(al===0?100:100-100/(1+ag/al));}
    else{gains=(gains*(period-1)+gain)/period;losses=(losses*(period-1)+loss)/period;rsi.push(losses===0?100:100-100/(1+gains/losses));}
  }
  return rsi;
}

// --- Period presets ---
const PERIODS:{label:string;bars:number}[] = [
  {label:"1M",bars:22},{label:"3M",bars:66},{label:"6M",bars:132},
  {label:"1A",bars:252},{label:"2A",bars:504},{label:"3A",bars:756},{label:"Max",bars:99999},
];

export default function SyncedChartPanel({
  candles, symbol, annotations, cotLegacy, cotDisagg
}:{
  candles:OHLCV[]; symbol:string; annotations?:Annotation[];
  cotLegacy?:{history:COTHistoryEntry[];latest?:any};
  cotDisagg?:{history:COTHistoryEntry[];latest?:any};
}) {
  const n = candles.length;
  const [range, setRange] = useState<[number,number]>([Math.max(0,n-252), n]); // default 1A
  const [activePeriod, setActivePeriod] = useState("1A");
  const [drag, setDrag] = useState<{startX:number;startIdx:number}|null>(null);
  const [sel, setSel] = useState<{startIdx:number;endIdx:number}|null>(null);
  const [isPanning, setIsPanning] = useState(false);
  const priceRef = useRef<HTMLCanvasElement>(null);
  const cotLRef = useRef<HTMLCanvasElement>(null);
  const cotDRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const [dims, setDims] = useState({w:900});

  useEffect(()=>{
    const el=containerRef.current;if(!el)return;
    const obs=new ResizeObserver(entries=>{const{width}=entries[0].contentRect;if(width>0)setDims({w:Math.floor(width)});});
    obs.observe(el);return()=>obs.disconnect();
  },[]);

  // Sync range when commodity changes
  useEffect(()=>{
    const bars = PERIODS.find(p=>p.label===activePeriod)?.bars||252;
    setRange([Math.max(0, n-Math.min(bars,n)), n]);
  },[n, symbol]);

  // Period button click
  const setPeriod = useCallback((label:string)=>{
    const bars = PERIODS.find(p=>p.label===label)?.bars||252;
    setActivePeriod(label);
    setRange([Math.max(0, n-Math.min(bars,n)), n]);
  },[n]);

  // Convert pixel X to data index
  const padL=55, padR=16;
  const xToIdx = useCallback((clientX:number, rect:DOMRect)=>{
    const x = clientX - rect.left;
    const [rs,re] = range;
    const visN = re-rs;
    const bw = (dims.w-padL-padR)/visN;
    return Math.round((x-padL)/bw) + rs;
  },[range,dims.w]);

  // Mouse handlers for drag-to-zoom + pan
  const onMouseDown = useCallback((e:React.MouseEvent)=>{
    const rect = priceRef.current?.getBoundingClientRect(); if(!rect) return;
    const idx = xToIdx(e.clientX, rect);
    if(e.shiftKey || isPanning) {
      // Pan mode
      setDrag({startX:e.clientX, startIdx:range[0]});
      setIsPanning(true);
    } else {
      // Selection mode
      setSel({startIdx:idx, endIdx:idx});
    }
  },[xToIdx, range, isPanning]);

  const onMouseMove = useCallback((e:React.MouseEvent)=>{
    if(sel) {
      const rect = priceRef.current?.getBoundingClientRect(); if(!rect) return;
      const idx = xToIdx(e.clientX, rect);
      setSel(prev => prev ? {...prev, endIdx: idx} : null);
    } else if(drag && isPanning) {
      const rect = priceRef.current?.getBoundingClientRect(); if(!rect) return;
      const dx = e.clientX - drag.startX;
      const visN = range[1]-range[0];
      const bw = (dims.w-padL-padR)/visN;
      const idxDelta = Math.round(-dx/bw);
      const newStart = Math.max(0, Math.min(n-visN, drag.startIdx+idxDelta));
      setRange([newStart, newStart+visN]);
    }
  },[sel,drag,isPanning,xToIdx,range,n,dims.w]);

  const onMouseUp = useCallback(()=>{
    if(sel) {
      const s = Math.min(sel.startIdx, sel.endIdx);
      const e = Math.max(sel.startIdx, sel.endIdx);
      if(e-s >= 5) {
        setRange([Math.max(0,s), Math.min(n,e)]);
        setActivePeriod("");
      }
      setSel(null);
    }
    setDrag(null);
    setIsPanning(false);
  },[sel,n]);

  const onDoubleClick = useCallback(()=>{
    setPeriod("1A");
  },[setPeriod]);

  const onWheel = useCallback((e:React.WheelEvent)=>{
    e.preventDefault();
    const rect = priceRef.current?.getBoundingClientRect(); if(!rect) return;
    const idx = xToIdx(e.clientX, rect);
    const [rs,re] = range;
    const visN = re-rs;
    const factor = e.deltaY > 0 ? 1.15 : 0.87; // zoom out / zoom in
    const newVis = Math.max(20, Math.min(n, Math.round(visN*factor)));
    // Keep cursor position stable
    const ratio = (idx-rs)/visN;
    const newStart = Math.max(0, Math.min(n-newVis, Math.round(idx-ratio*newVis)));
    setRange([newStart, newStart+newVis]);
    setActivePeriod("");
  },[range,n,xToIdx]);

  // === DRAW PRICE CHART ===
  const [hover, setHover] = useState<number|null>(null);
  const RSI_H = 110;

  useEffect(()=>{
    const cvs=priceRef.current;if(!cvs)return;
    const ctx=cvs.getContext("2d");if(!ctx)return;
    const W=dims.w, H=520;
    const mainH=H-RSI_H-20;
    cvs.width=W*2;cvs.height=H*2;ctx.scale(2,2);
    cvs.style.width=W+"px";cvs.style.height=H+"px";

    const [rs,re]=range;
    const data=candles.slice(rs,re);
    if(!data.length) return;
    const allCloses=candles.map(c=>c.close); // full series for indicators
    const ema9=emaCalc(allCloses,9).slice(rs,re);
    const ma21=smaCalc(allCloses,21).slice(rs,re);
    const ma50=smaCalc(allCloses,50).slice(rs,re);
    const ma200=smaCalc(allCloses,200).slice(rs,re);
    const bb=bbCalc(allCloses,20,2);
    const bbU=bb.upper.slice(rs,re), bbL=bb.lower.slice(rs,re);
    const rsiAll=rsiCalc(allCloses,14);
    const rsi=rsiAll.slice(rs,re);

    const pad={t:20,b:24,l:padL,r:padR};
    const chartH=mainH-pad.t-pad.b;
    const allP=data.flatMap(c=>[c.high,c.low]);
    const mn=Math.min(...allP),mx=Math.max(...allP),rng=mx-mn||1;
    const pMn=mn-rng*.04,pMx=mx+rng*.04;
    const bW=Math.max(1.5,(W-pad.l-pad.r)/data.length-1);
    const yP=(v:number)=>pad.t+(1-(v-pMn)/(pMx-pMn))*chartH;
    const xC=(i:number)=>pad.l+i*(W-pad.l-pad.r)/data.length+bW/2;

    ctx.fillStyle=C.bg;ctx.fillRect(0,0,W,H);

    // Grid
    ctx.strokeStyle="rgba(148,163,184,.06)";ctx.lineWidth=1;
    for(let i=0;i<5;i++){const y=pad.t+i*(chartH/4);ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(W-pad.r,y);ctx.stroke();}
    ctx.fillStyle=C.textMuted;ctx.font="10px monospace";ctx.textAlign="right";
    for(let i=0;i<5;i++){const v=pMx-(pMx-pMn)*i/4;ctx.fillText(v.toFixed(2),pad.l-6,pad.t+i*(chartH/4)+4);}

    // Bollinger fill
    ctx.fillStyle=C.bb;ctx.beginPath();let started=false;
    for(let i=0;i<data.length;i++){if(isNaN(bbU[i]))continue;const x=xC(i);if(!started){ctx.moveTo(x,yP(bbU[i]));started=true;}else ctx.lineTo(x,yP(bbU[i]));}
    for(let i=data.length-1;i>=0;i--){if(isNaN(bbL[i]))continue;ctx.lineTo(xC(i),yP(bbL[i]));}
    ctx.closePath();ctx.fill();

    // MAs
    const drawLine=(arr:number[],color:string,width:number)=>{ctx.strokeStyle=color;ctx.lineWidth=width;ctx.beginPath();let s2=false;arr.forEach((v,i)=>{if(isNaN(v))return;const x=xC(i),y=yP(v);if(!s2){ctx.moveTo(x,y);s2=true;}else ctx.lineTo(x,y);});ctx.stroke();};
    drawLine(ema9,C.ma9,1); drawLine(ma21,C.ma21,1.2); drawLine(ma50,C.ma50,1.2);
    if(ma200.some(v=>!isNaN(v))) drawLine(ma200,C.ma200,1.2);

    // Candles
    for(let i=0;i<data.length;i++){
      const c=data[i];const x=xC(i);const up=c.close>=c.open;
      ctx.strokeStyle=C.wick;ctx.lineWidth=1.5;ctx.beginPath();ctx.moveTo(x,yP(c.high));ctx.lineTo(x,yP(c.low));ctx.stroke();
      ctx.fillStyle=up?C.candleUp:C.candleDn;
      const top2=yP(Math.max(c.open,c.close)),bot2=yP(Math.min(c.open,c.close));
      const h2=Math.max(1,bot2-top2);
      ctx.fillRect(x-bW/2,top2,bW,h2);
      ctx.strokeStyle=up?"rgba(0,200,120,0.5)":"rgba(220,60,60,0.5)";
      ctx.lineWidth=0.5;ctx.strokeRect(x-bW/2,top2,bW,h2);
    }

    // Annotations
    if(annotations){for(const ann of annotations){if(ann.value<pMn||ann.value>pMx)continue;const ay=yP(ann.value);ctx.strokeStyle=ann.color;ctx.lineWidth=1;ctx.setLineDash([6,4]);ctx.beginPath();ctx.moveTo(pad.l,ay);ctx.lineTo(W-pad.r,ay);ctx.stroke();ctx.setLineDash([]);ctx.fillStyle=ann.color;ctx.font="bold 9px monospace";ctx.textAlign="right";ctx.fillText(ann.label,W-pad.r-2,ay-4);}}

    // Volume
    const volH=24;const maxVol=Math.max(...data.map(c=>c.volume||1));
    ctx.globalAlpha=0.25;
    for(let i=0;i<data.length;i++){const c=data[i];const vh=(c.volume/maxVol)*volH;ctx.fillStyle=c.close>=c.open?C.candleUp:C.candleDn;ctx.fillRect(xC(i)-bW/2,mainH-pad.b-vh,bW,vh);}
    ctx.globalAlpha=1;

    // X axis dates
    ctx.fillStyle=C.textMuted;ctx.font="9px monospace";ctx.textAlign="center";
    const step=Math.ceil(data.length/8);
    for(let i=0;i<data.length;i+=step){ctx.fillText(data[i].date.slice(5),xC(i),mainH-pad.b+14);}

    // Selection overlay
    if(sel){
      const s2=Math.max(0,Math.min(sel.startIdx,sel.endIdx)-rs);
      const e2=Math.min(data.length-1,Math.max(sel.startIdx,sel.endIdx)-rs);
      ctx.fillStyle="rgba(220,180,50,.12)";
      ctx.fillRect(xC(s2)-bW/2,pad.t,xC(e2)-xC(s2)+bW,chartH);
      ctx.strokeStyle="#DCB432";ctx.lineWidth=1;
      ctx.strokeRect(xC(s2)-bW/2,pad.t,xC(e2)-xC(s2)+bW,chartH);
    }

    // RSI Panel
    const rsiTop=mainH+8;const rsiH2=RSI_H-16;

    // RSI label
    ctx.fillStyle="rgba(148,163,184,.5)";ctx.font="bold 9px monospace";ctx.textAlign="left";
    ctx.fillText("RSI 14",pad.l+4,rsiTop+10);

    ctx.fillStyle=C.panelAlt;ctx.fillRect(pad.l,rsiTop,W-pad.l-pad.r,rsiH2);
    ctx.fillStyle=C.rsiOverbought;ctx.fillRect(pad.l,rsiTop,W-pad.l-pad.r,rsiH2*0.3);
    ctx.fillStyle=C.rsiOversold;ctx.fillRect(pad.l,rsiTop+rsiH2*0.7,W-pad.l-pad.r,rsiH2*0.3);

    // Linhas 70 e 30
    const y70=rsiTop+rsiH2*0.30;
    ctx.strokeStyle="rgba(239,68,68,.5)";ctx.lineWidth=0.75;ctx.setLineDash([3,3]);
    ctx.beginPath();ctx.moveTo(pad.l,y70);ctx.lineTo(W-pad.r,y70);ctx.stroke();
    const y30=rsiTop+rsiH2*0.70;
    ctx.strokeStyle="rgba(34,197,94,.5)";ctx.lineWidth=0.75;ctx.setLineDash([3,3]);
    ctx.beginPath();ctx.moveTo(pad.l,y30);ctx.lineTo(W-pad.r,y30);ctx.stroke();
    ctx.setLineDash([]);

    // Linha 50
    ctx.strokeStyle="rgba(148,163,184,.2)";ctx.lineWidth=1;ctx.setLineDash([4,4]);
    ctx.beginPath();ctx.moveTo(pad.l,rsiTop+rsiH2*0.5);ctx.lineTo(W-pad.r,rsiTop+rsiH2*0.5);ctx.stroke();ctx.setLineDash([]);

    // RSI line
    const yRsi=(v:number)=>rsiTop+rsiH2*(1-v/100);
    ctx.strokeStyle=C.rsiLine;ctx.lineWidth=2.5;ctx.beginPath();
    rsi.forEach((v,i)=>{const x=xC(i),y=yRsi(v);if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);});ctx.stroke();
    ctx.fillStyle=C.textMuted;ctx.font="9px monospace";ctx.textAlign="right";
    ctx.fillText("70",pad.l-4,rsiTop+rsiH2*0.3+3);ctx.fillText("50",pad.l-4,rsiTop+rsiH2*0.5+3);ctx.fillText("30",pad.l-4,rsiTop+rsiH2*0.7+3);
    const lastRsi=rsi[rsi.length-1];
    ctx.fillStyle=lastRsi>70?C.red:lastRsi<30?C.green:C.amber;ctx.font="bold 11px monospace";ctx.textAlign="left";
    ctx.fillText(`RSI: ${lastRsi.toFixed(1)}`,pad.l+4,rsiTop+24);

    // Legend
    ctx.font="8px monospace";ctx.textAlign="left";let lx=pad.l;
    [{l:"EMA9",c:C.ma9},{l:"MA21",c:C.ma21},{l:"MA50",c:C.ma50},{l:"MA200",c:C.ma200},{l:"BB",c:"rgba(148,163,184,.5)"}].forEach(({l,c})=>{
      ctx.fillStyle=c;ctx.fillRect(lx,6,14,2);ctx.fillStyle=C.textMuted;ctx.fillText(l,lx+17,9);lx+=55;
    });

    // Hover crosshair
    if(hover!==null&&hover>=0&&hover<data.length){
      const hc=data[hover];const x=xC(hover);
      ctx.strokeStyle="rgba(148,163,184,.3)";ctx.lineWidth=1;ctx.setLineDash([4,4]);
      ctx.beginPath();ctx.moveTo(x,pad.t);ctx.lineTo(x,mainH-pad.b);ctx.stroke();ctx.setLineDash([]);
      ctx.fillStyle="rgba(22,27,34,.95)";const tx=Math.min(x+10,W-140);
      ctx.fillRect(tx,pad.t,130,72);ctx.strokeStyle=C.border;ctx.strokeRect(tx,pad.t,130,72);
      ctx.fillStyle=C.text;ctx.font="bold 10px monospace";ctx.fillText(hc.date,tx+8,pad.t+14);
      ctx.font="10px monospace";ctx.fillStyle=C.textDim;
      ctx.fillText(`O: ${hc.open.toFixed(2)}`,tx+8,pad.t+28);ctx.fillText(`H: ${hc.high.toFixed(2)}`,tx+8,pad.t+40);ctx.fillText(`L: ${hc.low.toFixed(2)}`,tx+8,pad.t+52);
      ctx.fillStyle=hc.close>=hc.open?C.green:C.red;ctx.fillText(`C: ${hc.close.toFixed(2)}`,tx+8,pad.t+64);
    }
  },[candles,range,dims.w,hover,sel,annotations]);

  // === DRAW COT CHART (generic for both legacy and disaggregated) ===
  const drawCOT = useCallback((cvs:HTMLCanvasElement|null, history:COTHistoryEntry[], type:"legacy"|"disaggregated")=>{
    if(!cvs||!history.length)return;
    const ctx=cvs.getContext("2d");if(!ctx)return;

    // Align COT dates to price range
    const [rs,re]=range;
    const priceDates = candles.slice(rs,re).map(c=>c.date);
    if(!priceDates.length) return;
    const startDate=priceDates[0], endDate=priceDates[priceDates.length-1];

    // Filter COT to date range (COT is weekly, prices are daily)
    const filtered = history.filter(h => h.date >= startDate && h.date <= endDate);
    const data = filtered.length > 0 ? filtered : history.slice(-Math.min(52, history.length));
    if(!data.length) return;

    const colors={comm:"#ef4444",noncomm:"#3b82f6",mm:"#00C878",prod:"#f59e0b",swap:"#a78bfa"};
    type Line={label:string;vals:number[];color:string;lw:number};
    let lines:Line[]=[];
    if(type==="legacy"){
      lines=[
        {label:"Commercial Net",vals:data.map(r=>(r.comm_long||0)-(r.comm_short||0)),color:colors.comm,lw:2.0},
        {label:"Non-Comm Net",vals:data.map(r=>(r.noncomm_long||0)-(r.noncomm_short||0)),color:colors.noncomm,lw:2.5},
      ];
    } else {
      lines=[
        {label:"Managed Money",vals:data.map(r=>(r.managed_money_long||0)-(r.managed_money_short||0)),color:colors.mm,lw:2.5},
        {label:"Producer",vals:data.map(r=>(r.producer_long||0)-(r.producer_short||0)),color:colors.prod,lw:1.5},
        {label:"Swap Dealers",vals:data.map(r=>(r.swap_long||0)-(r.swap_short||0)),color:colors.swap,lw:1.5},
      ];
    }

    const W=dims.w, H=220;
    cvs.width=W*2;cvs.height=H*2;ctx.scale(2,2);
    cvs.style.width=W+"px";cvs.style.height=H+"px";
    ctx.fillStyle=C.bg;ctx.fillRect(0,0,W,H);

    const pad2={l:padL,r:70,t:14,b:20};
    const cW=W-pad2.l-pad2.r, cH=H-pad2.t-pad2.b;
    const cn=data.length;
    const xC2=(i:number)=>pad2.l+(i+0.5)*cW/cn;
    const allV=lines.flatMap(s=>s.vals);
    const minV=Math.min(...allV),maxV=Math.max(...allV),rngV=maxV-minV||1;
    const yP2=(v:number)=>pad2.t+cH-(v-minV)/rngV*cH;

    // Grid
    for(let g=0;g<=3;g++){const val=minV+(maxV-minV)*g/3;const y=yP2(val);ctx.strokeStyle="rgba(148,163,184,.08)";ctx.lineWidth=0.5;ctx.beginPath();ctx.moveTo(pad2.l,y);ctx.lineTo(W-pad2.r,y);ctx.stroke();}
    // Zero line
    if(minV<0&&maxV>0){const zy=yP2(0);ctx.strokeStyle="rgba(148,163,184,.45)";ctx.lineWidth=1.0;ctx.setLineDash([4,3]);ctx.beginPath();ctx.moveTo(pad2.l,zy);ctx.lineTo(W-pad2.r,zy);ctx.stroke();ctx.setLineDash([]);}

    // Open Interest (background area)
    const oiVals=data.map(d=>d.open_interest||0);const oiValid=oiVals.filter(v=>v>0);
    if(oiValid.length>1){
      const oiMin=Math.min(...oiValid)*0.95,oiMax=Math.max(...oiValid)*1.05;
      const toOIY=(v:number)=>pad2.t+cH-((v-oiMin)/(oiMax-oiMin||1))*cH;
      ctx.beginPath();let oiS=false;
      data.forEach((d,i)=>{if(!d.open_interest)return;const x=xC2(i),y=toOIY(d.open_interest);if(!oiS){ctx.moveTo(x,y);oiS=true;}else ctx.lineTo(x,y);});
      ctx.lineTo(xC2(cn-1),pad2.t+cH);ctx.lineTo(xC2(0),pad2.t+cH);ctx.closePath();
      ctx.fillStyle="rgba(148,163,184,.06)";ctx.fill();
      ctx.beginPath();oiS=false;
      data.forEach((d,i)=>{if(!d.open_interest)return;const x=xC2(i),y=toOIY(d.open_interest);if(!oiS){ctx.moveTo(x,y);oiS=true;}else ctx.lineTo(x,y);});
      ctx.strokeStyle="rgba(148,163,184,.35)";ctx.lineWidth=1;ctx.setLineDash([]);ctx.stroke();
      const lastOI=data[cn-1]?.open_interest||0;
      ctx.fillStyle="rgba(148,163,184,.5)";ctx.font="8px monospace";ctx.textAlign="right";
      ctx.fillText("OI: "+(lastOI/1000).toFixed(0)+"K",W-pad2.r+65,pad2.t+10);
    }

    const zeroY2=minV<0&&maxV>0?yP2(0):yP2(Math.min(0,minV));
    for(const line of lines){
      // Fill
      ctx.beginPath();ctx.moveTo(xC2(0),zeroY2);
      for(let i=0;i<cn;i++) ctx.lineTo(xC2(i),yP2(line.vals[i]));
      ctx.lineTo(xC2(cn-1),zeroY2);ctx.closePath();
      const hex=line.color;const r2=parseInt(hex.slice(1,3),16);const g2=parseInt(hex.slice(3,5),16);const b2=parseInt(hex.slice(5,7),16);
      ctx.fillStyle=`rgba(${r2},${g2},${b2},0.08)`;ctx.fill();
      // Line
      ctx.strokeStyle=line.color;ctx.lineWidth=line.lw;ctx.beginPath();
      for(let i=0;i<cn;i++){const x=xC2(i),y=yP2(line.vals[i]);if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y);}
      ctx.stroke();
      // Value badge
      const lv=line.vals[cn-1];const vs=(lv>=0?"+":"")+(Math.abs(lv)>=1000?(lv/1000).toFixed(0)+"K":lv.toFixed(0));
      const by=yP2(lv);ctx.fillStyle=line.color;const tw=ctx.measureText(vs).width+12;
      ctx.fillRect(W-pad2.r+4,by-8,Math.max(tw,48),16);
      ctx.beginPath();ctx.moveTo(W-pad2.r+4,by);ctx.lineTo(W-pad2.r,by-4);ctx.lineTo(W-pad2.r,by+4);ctx.closePath();ctx.fill();
      ctx.fillStyle="#fff";ctx.font="bold 9px monospace";ctx.textAlign="left";ctx.fillText(vs,W-pad2.r+8,by+3);
    }

    // X dates
    ctx.fillStyle=C.textMuted;ctx.font="8px monospace";ctx.textAlign="center";
    const dStep=Math.max(1,Math.floor(cn/8));
    data.forEach((d,i)=>{if(i%dStep===0||i===cn-1)ctx.fillText(d.date.slice(2,7),xC2(i),H-pad2.b+12);});

    // Legend
    let lx2=pad2.l+4;
    for(const line of lines){ctx.fillStyle=line.color;ctx.fillRect(lx2,2,8,8);ctx.fillStyle=C.textDim;ctx.font="8px sans-serif";ctx.textAlign="left";ctx.fillText(line.label,lx2+11,10);lx2+=ctx.measureText(line.label).width+22;}
  },[candles,range,dims.w]);

  // Draw COT panels
  useEffect(()=>{if(cotLegacy?.history?.length) drawCOT(cotLRef.current,cotLegacy.history,"legacy");},[cotLegacy,drawCOT]);
  useEffect(()=>{if(cotDisagg?.history?.length) drawCOT(cotDRef.current,cotDisagg.history,"disaggregated");},[cotDisagg,drawCOT]);

  // Price chart hover
  const handleHover = useCallback((e:React.MouseEvent)=>{
    const rect=priceRef.current?.getBoundingClientRect();if(!rect)return;
    const idx = xToIdx(e.clientX,rect) - range[0];
    const visN = range[1]-range[0];
    if(idx>=0&&idx<visN)setHover(idx);else setHover(null);
  },[xToIdx,range]);

  const hasCotL = !!cotLegacy?.history?.length;
  const hasCotD = !!cotDisagg?.history?.length;

  return (
    <div ref={containerRef} style={{width:"100%"}}>
      {/* Period selector */}
      <div style={{display:"flex",gap:4,marginBottom:8,alignItems:"center"}}>
        {PERIODS.map(p=>(
          <button key={p.label} onClick={()=>setPeriod(p.label)} style={{
            padding:"4px 10px",fontSize:9,fontWeight:activePeriod===p.label?700:500,
            borderRadius:4,cursor:"pointer",border:`1px solid ${activePeriod===p.label?"#DCB432":"#1E3044"}`,
            background:activePeriod===p.label?"rgba(220,180,50,.12)":"transparent",
            color:activePeriod===p.label?"#DCB432":"#8C96A5",transition:"all .15s",
          }}>{p.label}</button>
        ))}
        <span style={{fontSize:8,color:"#64748b",marginLeft:8}}>
          Arraste para zoom · Duplo-clique = reset · Scroll = zoom · Shift+arraste = pan
        </span>
      </div>

      {/* Price chart */}
      <canvas ref={priceRef} style={{borderRadius:6,display:"block",cursor:sel?"col-resize":isPanning?"grabbing":"crosshair"}}
        onMouseDown={onMouseDown} onMouseMove={(e)=>{onMouseMove(e);handleHover(e);}}
        onMouseUp={onMouseUp} onMouseLeave={()=>{onMouseUp();setHover(null);}}
        onDoubleClick={onDoubleClick} onWheel={onWheel} />

      {/* COT Legacy */}
      {hasCotL && (
        <div style={{marginTop:8}}>
          <div style={{fontSize:9,fontWeight:600,color:"#8C96A5",marginBottom:2}}>COT LEGACY -- Commercial vs Non-Commercial</div>
          <canvas ref={cotLRef} style={{borderRadius:6,display:"block"}} />
        </div>
      )}

      {/* COT Disaggregated */}
      {hasCotD && (
        <div style={{marginTop:8}}>
          <div style={{fontSize:9,fontWeight:600,color:"#8C96A5",marginBottom:2}}>COT DISAGGREGATED -- Managed Money / Producer / Swap</div>
          <canvas ref={cotDRef} style={{borderRadius:6,display:"block"}} />
        </div>
      )}

      {/* COT Summary cards */}
      {(hasCotL || hasCotD) && (
        <div style={{display:"flex",gap:8,marginTop:8,flexWrap:"wrap"}}>
          {cotLegacy?.latest && (()=>{
            const l=cotLegacy.latest;const cn=l.comm_net||0;const nn=l.noncomm_net||0;
            return <>
              <div style={{padding:"5px 10px",background:"#0E1A24",borderRadius:6,borderLeft:"3px solid #ef4444",fontSize:10}}>
                <div style={{color:"#8C96A5",fontSize:7}}>COMMERCIAL</div>
                <div style={{fontWeight:700,fontFamily:"monospace",color:cn>=0?"#00C878":"#DC3C3C"}}>{(cn/1000).toFixed(1)}K</div>
              </div>
              <div style={{padding:"5px 10px",background:"#0E1A24",borderRadius:6,borderLeft:"3px solid #3b82f6",fontSize:10}}>
                <div style={{color:"#8C96A5",fontSize:7}}>NON-COMM</div>
                <div style={{fontWeight:700,fontFamily:"monospace",color:nn>=0?"#00C878":"#DC3C3C"}}>{(nn/1000).toFixed(1)}K</div>
              </div>
            </>;
          })()}
          {cotDisagg?.latest && (()=>{
            const d=cotDisagg.latest;const mm=d.managed_money_net||0;const pr=d.producer_net||0;
            return <>
              <div style={{padding:"5px 10px",background:"#0E1A24",borderRadius:6,borderLeft:"3px solid #22c55e",fontSize:10}}>
                <div style={{color:"#8C96A5",fontSize:7}}>MANAGED MONEY</div>
                <div style={{fontWeight:700,fontFamily:"monospace",color:mm>=0?"#00C878":"#DC3C3C"}}>{(mm/1000).toFixed(1)}K</div>
              </div>
              <div style={{padding:"5px 10px",background:"#0E1A24",borderRadius:6,borderLeft:"3px solid #f59e0b",fontSize:10}}>
                <div style={{color:"#8C96A5",fontSize:7}}>PRODUCER</div>
                <div style={{fontWeight:700,fontFamily:"monospace",color:pr>=0?"#00C878":"#DC3C3C"}}>{(pr/1000).toFixed(1)}K</div>
              </div>
            </>;
          })()}
        </div>
      )}
    </div>
  );
}
