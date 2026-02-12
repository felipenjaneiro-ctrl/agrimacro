#!/usr/bin/env python3
"""
AgriMacro v3.2 - Relatorio PDF PROFISSIONAL ENHANCED (v4)
~15 paginas landscape A4, tema dark, TODOS os dados do pipeline.
NUNCA inclui ibkr_portfolio.json.
Data do relatorio = data de execucao (hoje), nao do report_daily.

Melhorias v4:
- 15 paginas (vs 11)
- Precos divididos em 2 paginas (grade 2x4): Graos/Oleaginosas + Carnes/Energia/Metais
- Tabela de Variacoes dedicada (pagina 5)
- Spreads em 2 paginas: termometro + detalhamento
- Graficos maiores com MA20, min/max 60d, preco atual em destaque
- Barra explicativa em cada pagina ("O QUE VOCE VE AQUI:")
- Fontes maiores
- Footer com numero de pagina
- Glossario para produtor (pagina 15)
"""
import json, os, sys, math
from datetime import datetime
from io import BytesIO
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.colors import HexColor
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader

# â”€â”€ CONFIG â”€â”€
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJ_DIR   = os.path.join(BASE_DIR, "..")
DATA_PROC  = os.path.join(PROJ_DIR, "agrimacro-dash", "public", "data", "processed")
DATA_RAW   = os.path.join(PROJ_DIR, "agrimacro-dash", "public", "data", "raw")
REPORT_DIR = os.path.join(PROJ_DIR, "agrimacro-dash", "public", "data", "reports")
TODAY      = datetime.now()
TODAY_STR  = TODAY.strftime("%Y-%m-%d")
TODAY_BR   = TODAY.strftime("%d/%m/%Y")
WDAY = ["segunda","terca","quarta","quinta","sexta","sabado","domingo"][TODAY.weekday()]
OUTPUT_PDF = os.path.join(REPORT_DIR, f"agrimacro_{TODAY_STR}.pdf")

BG="#0f1117"; PANEL="#1a1d27"; PANEL2="#1e2130"; BORDER="#2a2d3a"
TEXT="#e2e8f0"; TEXT2="#cbd5e1"; TEXT_MUT="#94a3b8"; TEXT_DIM="#64748b"
GREEN="#22c55e"; AMBER="#f59e0b"; RED="#ef4444"; BLUE="#3b82f6"
PURPLE="#a855f7"; CYAN="#06b6d4"; TEAL="#14b8a6"; PINK="#ec4899"
EXPLAIN_BG="#1a2e1a"  # fundo verde sutil para barra explicativa
PAGE_W, PAGE_H = landscape(A4)
M = 28  # margem

NM = {
    "ZC":"Milho","ZS":"Soja","ZW":"Trigo SRW","KE":"Trigo HRW","ZM":"Farelo Soja",
    "ZL":"Oleo Soja","KC":"Cafe Arabica","SB":"Acucar #11","CT":"Algodao #2",
    "CC":"Cacau","OJ":"Suco Laranja","LE":"Boi Gordo","GF":"Boi Engorda",
    "HE":"Porco Magro","CL":"Petroleo WTI","NG":"Gas Natural","GC":"Ouro",
    "SI":"Prata","RB":"Gasolina RBOB","DX":"Indice Dolar",
}
GRID_GRAOS = [("ZS","Soja"),("ZC","Milho"),("ZW","Trigo"),("ZM","Far.Soja"),
              ("ZL","Oleo Soja"),("KC","Cafe"),("SB","Acucar"),("CT","Algodao")]
GRID_CARNES = [("LE","Boi Vivo"),("HE","Porco"),("GF","Boi Eng."),("CC","Cacau"),
               ("CL","Petroleo"),("NG","Gas Nat."),("GC","Ouro"),("OJ","Suco Lar.")]
GRID_ALL = GRID_GRAOS + GRID_CARNES

SPR_NM = {"soy_crush":"Crush de Soja","ke_zw":"Trigo HRW/SRW","zl_cl":"Oleo Soja/Petroleo",
           "feedlot":"Margem Confinamento","zc_zm":"Milho/Farelo","zc_zs":"Ratio Milho/Soja"}
SPR_EXPLAIN = {
    "soy_crush":"Se sobe, industria paga mais pela soja em grao. Bom para quem vende soja.",
    "ke_zw":"Premium do trigo duro sobre o mole. Reflete qualidade e demanda de moagem.",
    "zl_cl":"Relacao oleo de soja vs petroleo. Indica competitividade do biodiesel.",
    "feedlot":"Margem do confinamento: boi gordo menos boi magro e milho. Positivo = lucro.",
    "zc_zm":"Milho vs farelo. Se milho sobe relativo, racao encarece.",
    "zc_zs":"Ratio classico. Abaixo de 2.3 = plante mais milho. Acima de 2.5 = plante mais soja.",
}
EIA_NM = {"wti_spot":"WTI Spot","brent_spot":"Brent Spot","henry_hub":"Gas Natural",
           "diesel_spot":"Diesel","crude_stocks":"Est. Petroleo","gasoline_stocks":"Est. Gasolina",
           "distillate_stocks":"Est. Destilados","crude_production":"Prod. Petroleo",
           "refinery_utilization":"Util. Refinarias","ethanol_production":"Prod. Etanol",
           "ethanol_stocks":"Est. Etanol"}
STATE_PT = {"APERTO":"APERTO","NEUTRO":"NEUTRO","NEUTRO_VIES_EXCESSO":"EXCESSO LEVE",
            "PRECO_DEPRIMIDO":"DEPRIMIDO","PRECO_NEUTRO":"NEUTRO","PRECO_ABAIXO_MEDIA":"ABAIXO MEDIA",
            "PRECO_ELEVADO":"ELEVADO","EXCESSO":"EXCESSO"}
STATE_CLR = {"APERTO":RED,"NEUTRO":TEXT_MUT,"NEUTRO_VIES_EXCESSO":AMBER,
             "PRECO_DEPRIMIDO":CYAN,"PRECO_NEUTRO":TEXT_MUT,"PRECO_ABAIXO_MEDIA":BLUE,
             "PRECO_ELEVADO":RED,"EXCESSO":AMBER}

# â”€â”€ DATA HELPERS â”€â”€
def load_json(path):
    for enc in ("utf-8-sig","utf-8","latin-1"):
        try:
            with open(path,"r",encoding=enc) as f: return json.load(f)
        except: continue
    return {}

def sload(folder, fn):
    p = os.path.join(folder, fn)
    return load_json(p) if os.path.exists(p) else {}

def price_list(sym, pr):
    d = pr.get(sym, [])
    return d.get("history",[]) if isinstance(d, dict) else d

def closes(sym, pr, n=60):
    h = price_list(sym, pr)
    if not h: return []
    return [float(p.get("close") or p.get("settlement") or 0) for p in h[-n:] if p.get("close") or p.get("settlement")]

def last_close(sym, pr):
    v = closes(sym, pr, 5)
    return v[-1] if v else None

def pchg(sym, pr, period=1):
    h = price_list(sym, pr)
    if len(h) < period+1: return None
    cur = None
    for p in reversed(h):
        c = p.get("close") or p.get("settlement")
        if c: cur=float(c); break
    if not cur: return None
    old = h[-(period+1)].get("close") or h[-(period+1)].get("settlement")
    if old and float(old)!=0: return ((cur-float(old))/float(old))*100
    return None

def hi52(sym, pr):
    v = closes(sym, pr, 252)
    return max(v) if v else None

def lo52(sym, pr):
    v = closes(sym, pr, 252)
    return min(v) if v else None

# â”€â”€ MPL SETUP â”€â”€
def setup_mpl():
    plt.rcParams.update({"figure.facecolor":BG,"axes.facecolor":PANEL,"axes.edgecolor":BORDER,
        "axes.labelcolor":TEXT,"text.color":TEXT,"xtick.color":TEXT_MUT,"ytick.color":TEXT_MUT,
        "grid.color":BORDER,"grid.alpha":0.2,"font.size":7.5,"font.family":"sans-serif"})

def fig2img(fig, dpi=150):
    buf = BytesIO()
    fig.savefig(buf,format="png",dpi=dpi,bbox_inches="tight",facecolor=fig.get_facecolor(),edgecolor="none",pad_inches=0.08)
    plt.close(fig); buf.seek(0)
    return ImageReader(buf)

# â”€â”€ REPORTLAB HELPERS â”€â”€
def dbg(c):
    c.setFillColor(HexColor(BG)); c.rect(0,0,PAGE_W,PAGE_H,fill=1,stroke=0)

def hdr(c, title, sub="", explain=""):
    """Header with title, subtitle, and optional explanation bar"""
    c.setFillColor(HexColor(PURPLE)); c.rect(0,PAGE_H-34,PAGE_W,34,fill=1,stroke=0)
    c.setFillColor(HexColor("#fff")); c.setFont("Helvetica-Bold",13); c.drawString(M,PAGE_H-24,title)
    c.setFillColor(HexColor(TEXT_MUT)); c.setFont("Helvetica",7.5)
    c.drawRightString(PAGE_W-M,PAGE_H-15,f"AgriMacro v3.2 | {TODAY_BR} ({WDAY})")
    if sub:
        c.setFillColor(HexColor(TEXT_MUT)); c.setFont("Helvetica",8)
        c.drawString(M,PAGE_H-47,sub)
    if explain:
        # Barra explicativa verde sutil
        ey = PAGE_H - 62 if sub else PAGE_H - 47
        c.setFillColor(HexColor(EXPLAIN_BG)); c.rect(M-5,ey-12,PAGE_W-2*M+10,16,fill=1,stroke=0)
        c.setFillColor(HexColor(GREEN)); c.setFont("Helvetica-Bold",7.5)
        c.drawString(M,ey-7,f"O QUE VOCE VE AQUI: ")
        tw = c.stringWidth("O QUE VOCE VE AQUI: ","Helvetica-Bold",7.5)
        c.setFillColor(HexColor(TEXT2)); c.setFont("Helvetica",7.5)
        c.drawString(M+tw,ey-7,explain[:120])

def ftr(c, pn, total):
    c.setFillColor(HexColor(BORDER)); c.rect(0,0,PAGE_W,16,fill=1,stroke=0)
    c.setFillColor(HexColor(TEXT_DIM)); c.setFont("Helvetica",6.5)
    c.drawString(M,4,"AgriMacro v3.2 | Dados reais de mercado | Distribuicao restrita")
    c.drawRightString(PAGE_W-M,4,f"Pagina {pn}/{total}")

def tblock(c, x, y, txt, font="Helvetica", sz=8.5, clr=TEXT, mw=None, ld=11):
    c.setFillColor(HexColor(clr)); c.setFont(font, sz)
    if not mw: c.drawString(x,y,txt); return y-ld
    words=txt.split(); lines=[]; cur=""
    for w in words:
        t = cur+" "+w if cur else w
        if c.stringWidth(t,font,sz)>mw:
            if cur: lines.append(cur)
            cur=w
        else: cur=t
    if cur: lines.append(cur)
    for ln in lines: c.drawString(x,y,ln); y-=ld
    return y

def panel(c, x, y, w, h, bc=None):
    c.setFillColor(HexColor(PANEL)); c.rect(x,y,w,h,fill=1,stroke=0)
    if bc: c.setStrokeColor(HexColor(bc)); c.setLineWidth(3); c.line(x,y,x,y+h)

def badge(c, x, y, text, color):
    tw = c.stringWidth(text,"Helvetica-Bold",7)+12
    c.setFillColor(HexColor(color)); c.roundRect(x,y-4,tw,15,3,fill=1,stroke=0)
    c.setFillColor(HexColor("#fff")); c.setFont("Helvetica-Bold",7); c.drawString(x+6,y,text)
    return tw

def num_fmt(v, dec=2):
    if v is None: return "-"
    if abs(v)>=1e6: return f"{v/1e6:,.1f}M"
    if abs(v)>=1e3 and dec==0: return f"{v:,.0f}"
    return f"{v:,.{dec}f}"

def chg_str(v):
    if v is None: return "-"
    s = "+" if v>=0 else ""
    return f"{s}{v:.1f}%"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  CHARTS â€” Enhanced with MA20, min/max 60d, larger
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def _draw_enhanced_chart(ax, sym, pr, nm, color=CYAN):
    """Draw an enhanced price chart on given axis: price line, MA20, min/max 60d, current price badge"""
    ax.set_facecolor(PANEL)
    v = closes(sym, pr, 60)
    if len(v) > 3:
        x = list(range(len(v)))
        # Main price line
        ax.plot(x, v, color=color, lw=1.5, zorder=3)
        ax.fill_between(x, min(v)*0.998, v, alpha=0.12, color=color)
        ax.scatter([x[-1]], [v[-1]], color=color, s=22, zorder=5, edgecolors="white", linewidths=0.5)
        # MA20
        if len(v) >= 20:
            ma20 = [np.mean(v[max(0,i-19):i+1]) for i in range(len(v))]
            ax.plot(x, ma20, color=AMBER, lw=0.8, ls="--", alpha=0.7, zorder=2)
        # Min/Max 60d lines
        vmin = min(v); vmax = max(v)
        ax.axhline(vmax, color=RED, lw=0.5, ls=":", alpha=0.5, zorder=1)
        ax.axhline(vmin, color=GREEN, lw=0.5, ls=":", alpha=0.5, zorder=1)
        # Current price (large)
        ax.text(0.03, 0.92, f"{v[-1]:,.1f}", transform=ax.transAxes, ha="left", va="top",
                fontsize=9, color=TEXT, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.2", facecolor=PANEL, edgecolor=BORDER, alpha=0.8))
        # Change badge
        chg = ((v[-1]-v[0])/v[0])*100 if v[0]!=0 else 0
        cc = GREEN if chg >= 0 else RED; ss = "+" if chg >= 0 else ""
        ax.text(0.97, 0.92, f"{ss}{chg:.1f}%", transform=ax.transAxes, ha="right", va="top",
                fontsize=8, color=cc, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.2", facecolor="#0f1117", edgecolor=cc, alpha=0.7, lw=0.5))
        # Min/Max labels
        ax.text(0.97, 0.03, f"Lo:{vmin:,.1f}", transform=ax.transAxes, ha="right", va="bottom",
                fontsize=5.5, color=GREEN, alpha=0.7)
        ax.text(0.03, 0.03, f"Hi:{vmax:,.1f}", transform=ax.transAxes, ha="left", va="bottom",
                fontsize=5.5, color=RED, alpha=0.7)
    else:
        ax.text(0.5, 0.5, "S/D", transform=ax.transAxes, ha="center", va="center", fontsize=11, color=TEXT_MUT)
    ax.set_title(f"{nm} ({sym})", fontsize=8, color=TEXT, pad=3, fontweight="bold")
    ax.tick_params(labelbottom=False, labelleft=False, length=0)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.4)


def chart_price_grid_2x4(grid_items, pr, title_extra=""):
    """2x4 grid of enhanced price charts â€” larger than old 4x4"""
    fig, axes = plt.subplots(2, 4, figsize=(11.5, 5.5))
    fig.patch.set_facecolor(BG)
    for idx, (sym, nm) in enumerate(grid_items[:8]):
        r, c2 = divmod(idx, 4)
        ax = axes[r][c2]
        _draw_enhanced_chart(ax, sym, pr, nm)
    fig.subplots_adjust(hspace=0.45, wspace=0.2, left=0.01, right=0.99, top=0.95, bottom=0.02)
    return fig2img(fig)


def chart_spreads(sd):
    spreads = sd.get("spreads",{})
    if not spreads: return None
    items=list(spreads.items()); n=len(items)
    fig,axes=plt.subplots(n,1,figsize=(11,max(n*1.2,4)))
    fig.patch.set_facecolor(BG)
    if n==1: axes=[axes]
    for i,(k,sp) in enumerate(items):
        ax=axes[i]; ax.set_facecolor(BG)
        pctl=sp.get("percentile",50); reg=sp.get("regime","NORMAL"); zs=sp.get("zscore_1y",0)
        nm=SPR_NM.get(k,sp.get("name",k))
        zc=["#22c55e","#86efac","#fde047","#fb923c","#ef4444"]
        for zi in range(5): ax.barh(0,20,left=zi*20,height=0.55,color=zc[zi],alpha=0.3)
        mc=GREEN if pctl<30 else (AMBER if pctl<70 else RED)
        ax.plot(pctl,0,"v",color=mc,ms=16,zorder=5)
        ax.plot(pctl,0,"v",color="white",ms=8,zorder=6)
        ax.set_xlim(-5,105); ax.set_ylim(-0.5,0.5)
        ax.text(-4,0,nm,va="center",ha="left",fontsize=9,color=TEXT,fontweight="bold")
        rc=GREEN if reg=="NORMAL" else (AMBER if reg in ("ATENCAO","NORMAL") else RED)
        if reg not in ("NORMAL",): rc=RED if "DISSON" in reg or "COMPRESS" in reg else AMBER
        ax.text(107,0,f"P{pctl:.0f} | Z:{zs:+.2f} | {reg}",va="center",ha="left",fontsize=7,color=rc)
        ax.axis("off")
    fig.subplots_adjust(hspace=0.6,left=0.2,right=0.78,top=0.96,bottom=0.04)
    return fig2img(fig)

def chart_eia(ed):
    series=ed.get("series",{})
    if not series: return None
    prio=["wti_spot","brent_spot","crude_stocks","gasoline_stocks","diesel_spot","ethanol_production",
          "henry_hub","crude_production","distillate_stocks","refinery_utilization"]
    sel=[k for k in prio if k in series][:6]
    if not sel: return None
    rows=(len(sel)+1)//2
    fig,axes=plt.subplots(rows,2,figsize=(11,rows*2.5))
    fig.patch.set_facecolor(BG)
    if rows==1: axes=[axes]
    for idx,key in enumerate(sel):
        r,c2=divmod(idx,2); ax=axes[r][c2]; ax.set_facecolor(PANEL)
        s=series[key]; hist=s.get("history",[])
        vals=[h["value"] for h in hist if h.get("value") is not None]
        if len(vals)>3:
            vp=vals[-52:]; x=list(range(len(vp)))
            ax.plot(x,vp,color=CYAN,lw=1.3,zorder=3)
            ax.fill_between(x,min(vp)*0.998,vp,alpha=0.1,color=CYAN)
            ax.scatter([x[-1]],[vp[-1]],color=CYAN,s=20,zorder=4,edgecolors="white",linewidths=0.4)
            # MA20
            if len(vp) >= 20:
                ma20 = [np.mean(vp[max(0,i-19):i+1]) for i in range(len(vp))]
                ax.plot(x, ma20, color=AMBER, lw=0.7, ls="--", alpha=0.6)
        nm=EIA_NM.get(key,key); lat=s.get("latest_value",""); un=hist[0].get("unit","") if hist else ""
        wow=s.get("wow_change_pct",0) or 0; ws="+" if wow>=0 else ""
        ax.set_title(f"{nm}: {lat} {un}  ({ws}{wow:.1f}% s/s)",fontsize=8,color=TEXT,pad=3,fontweight="bold")
        ax.tick_params(labelbottom=False,labelsize=6); ax.grid(True,alpha=0.12)
        for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.4)
    for idx in range(len(sel),rows*2):
        r,c2=divmod(idx,2); axes[r][c2].set_visible(False)
    fig.subplots_adjust(hspace=0.55,wspace=0.22,left=0.05,right=0.98,top=0.94,bottom=0.03)
    return fig2img(fig)

def chart_cot(cd):
    comms=cd.get("commodities",{})
    if not comms: return None
    items=[]
    for sym,data in comms.items():
        if not isinstance(data,dict): continue
        leg=data.get("legacy",{})
        lat=leg.get("latest",{})
        nc_net=lat.get("noncomm_net")
        cm_net=lat.get("comm_net")
        if nc_net is not None:
            nm=NM.get(sym,data.get("name",sym))
            items.append((nm,sym,float(nc_net),float(cm_net) if cm_net else 0))
    if not items: return None
    items.sort(key=lambda x:abs(x[2]),reverse=True); items=items[:14]
    fig,ax=plt.subplots(figsize=(11,max(len(items)*0.55,4)))
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)
    y=list(range(len(items)))
    nc=[it[2] for it in items]; cm=[it[3] for it in items]
    ax.barh(y,nc,color=[GREEN if v>=0 else RED for v in nc],alpha=0.7,height=0.35,zorder=3,label="Non-Commercial")
    ax.barh([yi+0.35 for yi in y],cm,color=[BLUE if v>=0 else AMBER for v in cm],alpha=0.5,height=0.35,zorder=3,label="Commercial")
    for i,(nm,sym,ncv,cmv) in enumerate(items):
        off=max(abs(ncv)*0.02,800); xt=ncv+off if ncv>=0 else ncv-off
        ax.text(xt,i,f"{ncv:,.0f}",va="center",ha="left" if ncv>=0 else "right",fontsize=7,color=TEXT,fontweight="bold")
    ax.set_yticks([yi+0.17 for yi in y]); ax.set_yticklabels([it[0] for it in items],fontsize=7.5)
    ax.axvline(0,color=BORDER,lw=0.8); ax.grid(True,axis="x",alpha=0.1); ax.invert_yaxis()
    ax.legend(fontsize=7,loc="lower right",framealpha=0.3)
    for sp in ax.spines.values(): sp.set_visible(False)
    fig.subplots_adjust(left=0.14,right=0.96,top=0.96,bottom=0.04)
    return fig2img(fig)

def chart_stocks(sw):
    comms=sw.get("commodities",{})
    if not comms: return None
    items=[]
    for sym,d in comms.items():
        if d.get("stock_current") is not None and d.get("stock_avg") is not None:
            items.append((NM.get(sym,sym),sym,d["stock_current"],d["stock_avg"],d.get("state",""),d.get("price_vs_avg",0)))
    if not items: return None
    items.sort(key=lambda x:abs(x[5]),reverse=True)
    fig,ax=plt.subplots(figsize=(11,max(len(items)*0.48,3.5)))
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)
    y=list(range(len(items)))
    devs=[it[5] for it in items]
    colors=[]
    for d in devs:
        if d>15: colors.append(RED)
        elif d>5: colors.append(AMBER)
        elif d<-15: colors.append(CYAN)
        elif d<-5: colors.append(BLUE)
        else: colors.append(TEXT_MUT)
    ax.barh(y,devs,color=colors,alpha=0.7,height=0.6,zorder=3)
    for i,(nm,sym,cur,avg,st,dev) in enumerate(items):
        off=max(abs(dev)*0.05,1)
        xt=dev+off if dev>=0 else dev-off
        ax.text(xt,i,f"{dev:+.1f}%",va="center",ha="left" if dev>=0 else "right",fontsize=7,color=TEXT,fontweight="bold")
    ax.set_yticks(y); ax.set_yticklabels([it[0] for it in items],fontsize=7.5)
    ax.axvline(0,color=BORDER,lw=0.8); ax.grid(True,axis="x",alpha=0.1); ax.invert_yaxis()
    ax.set_xlabel("Estoque vs Media (%)",fontsize=7.5,color=TEXT_MUT)
    for sp in ax.spines.values(): sp.set_visible(False)
    fig.subplots_adjust(left=0.14,right=0.96,top=0.96,bottom=0.08)
    return fig2img(fig)

def chart_macro_brl(bcb):
    brl=bcb.get("brl_usd",[])
    if not brl: return None
    vals=[float(r.get("valor",r.get("value",0))) for r in brl[-120:] if r.get("valor") or r.get("value")]
    if len(vals)<5: return None
    fig,ax=plt.subplots(figsize=(5.5,2.5))
    fig.patch.set_facecolor(BG); ax.set_facecolor(PANEL)
    x=list(range(len(vals)))
    ax.plot(x,vals,color=AMBER,lw=1.3,zorder=3)
    ax.fill_between(x,min(vals)*0.998,vals,alpha=0.1,color=AMBER)
    ax.scatter([x[-1]],[vals[-1]],color=AMBER,s=22,zorder=4,edgecolors="white",linewidths=0.5)
    if len(vals) >= 20:
        ma20 = [np.mean(vals[max(0,i-19):i+1]) for i in range(len(vals))]
        ax.plot(x, ma20, color=CYAN, lw=0.7, ls="--", alpha=0.6)
    ax.set_title(f"BRL/USD: {vals[-1]:.4f}",fontsize=9,color=TEXT,fontweight="bold",pad=3)
    ax.tick_params(labelbottom=False,labelsize=6); ax.grid(True,alpha=0.12)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.4)
    fig.subplots_adjust(left=0.12,right=0.97,top=0.88,bottom=0.05)
    return fig2img(fig)

def chart_physical_br(phys):
    intl=phys.get("international",{})
    br_keys=[k for k in intl if k.endswith("_BR")]
    if not br_keys: return None
    n=len(br_keys); cols=min(n,3); rows=math.ceil(n/cols)
    fig,axes=plt.subplots(rows,cols,figsize=(11,rows*2.2))
    fig.patch.set_facecolor(BG)
    if rows==1 and cols==1: axes=[[axes]]
    elif rows==1: axes=[axes]
    elif cols==1: axes=[[a] for a in axes]
    for idx,key in enumerate(br_keys):
        r,c2=divmod(idx,cols); ax=axes[r][c2]; ax.set_facecolor(PANEL)
        d=intl[key]; hist=d.get("history",[])
        vals=[float(h["value"]) for h in hist if h.get("value")]
        if vals:
            x=list(range(len(vals)))
            ax.plot(x,vals,color=TEAL,lw=1.3,zorder=3)
            ax.fill_between(x,min(vals)*0.998,vals,alpha=0.1,color=TEAL)
            ax.scatter([x[-1]],[vals[-1]],color=TEAL,s=18,zorder=4,edgecolors="white",linewidths=0.4)
        label=d.get("label","").replace("\U0001f1e7\U0001f1f7 ","").split(" \u2014 ")[-1]
        price=d.get("price",""); unit=d.get("price_unit",""); trend=d.get("trend","")
        ax.set_title(f"{key.replace('_BR','')}: {price} {unit} ({trend})",fontsize=7.5,color=TEXT,pad=2,fontweight="bold")
        ax.tick_params(labelbottom=False,labelsize=5.5); ax.grid(True,alpha=0.1)
        for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.4)
    for idx in range(len(br_keys),rows*cols):
        r,c2=divmod(idx,cols); axes[r][c2].set_visible(False)
    fig.subplots_adjust(hspace=0.55,wspace=0.22,left=0.05,right=0.98,top=0.92,bottom=0.03)
    return fig2img(fig)


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  PAGES (15 total)
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

# â”€â”€ PAGE 1: CAPA + RESUMO â”€â”€
def pg_cover(pdf, rd, dr, bcb):
    dbg(pdf)
    titulo = rd.get("titulo","Relatorio AgriMacro")
    subtitulo = rd.get("subtitulo","")
    pdf.setFillColor(HexColor(PURPLE)); pdf.rect(0,PAGE_H-130,PAGE_W,130,fill=1,stroke=0)
    pdf.setFillColor(HexColor("#fff")); pdf.setFont("Helvetica-Bold",9)
    pdf.drawString(M,PAGE_H-22,f"AGRIMACRO v3.2 | RELATORIO DIARIO | {TODAY_BR} ({WDAY.upper()})")
    pdf.setFont("Helvetica-Bold",24)
    pdf.drawString(M,PAGE_H-60,titulo[:65])
    if len(titulo)>65: pdf.setFont("Helvetica-Bold",16); pdf.drawString(M,PAGE_H-82,titulo[65:130])
    pdf.setFillColor(HexColor("#d1d5db")); pdf.setFont("Helvetica",11)
    pdf.drawString(M,PAGE_H-110,subtitulo[:95])
    rc=bcb.get("resumo_cambio",{}); rj=bcb.get("resumo_juros",{})
    brl=rc.get("brl_usd_atual"); selic=rj.get("selic_atual")
    rx = PAGE_W-M
    pdf.setFillColor(HexColor("#ffffff80")); pdf.setFont("Helvetica-Bold",10)
    if brl: pdf.drawRightString(rx,PAGE_H-40,f"USD/BRL {brl:.2f}")
    if selic: pdf.drawRightString(rx,PAGE_H-55,f"SELIC {selic:.1f}%")
    var30=rc.get("var_30d")
    if var30: pdf.setFont("Helvetica",8); pdf.drawRightString(rx,PAGE_H-68,f"BRL 30d: {var30:+.1f}%")
    y = PAGE_H-155
    resumo = rd.get("resumo_executivo","")
    if resumo:
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",11)
        pdf.drawString(M,y,"RESUMO EXECUTIVO"); y-=15
        y = tblock(pdf,M,y,resumo,sz=9,mw=PAGE_W-2*M,ld=13); y-=10
    perguntas = dr.get("perguntas",[])
    if perguntas:
        pdf.setFillColor(HexColor(CYAN)); pdf.setFont("Helvetica-Bold",10)
        pdf.drawString(M,y,"4 PERGUNTAS DO DIA"); y-=15
        for p in perguntas[:4]:
            panel(pdf,M,y-18,PAGE_W-2*M,20,bc=CYAN)
            pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",8)
            pdf.drawString(M+8,y-12,p[:130]); y-=24
    dests = rd.get("destaques",[])
    if dests and y>90:
        y-=8
        pdf.setFillColor(HexColor(AMBER)); pdf.setFont("Helvetica-Bold",10)
        pdf.drawString(M,y,"DESTAQUES"); y-=15
        for d in dests[:4]:
            if y<55: break
            t=d.get("titulo",""); cm=d.get("commodity",""); imp=d.get("impacto_produtor","")
            panel(pdf,M,y-48,PAGE_W-2*M,50,bc=AMBER)
            pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",8.5)
            pdf.drawString(M+8,y-14,f"[{cm}] {t[:85]}")
            if imp:
                pdf.setFont("Helvetica",7.5); pdf.setFillColor(HexColor(GREEN))
                tblock(pdf,M+8,y-28,f"Produtor: {imp[:160]}",sz=7.5,clr=GREEN,mw=PAGE_W-2*M-20,ld=10)
            y-=56


# â”€â”€ PAGE 2: MACRO BRASIL â”€â”€
def pg_macro(pdf, bcb, img_brl):
    dbg(pdf); hdr(pdf,"Cenario Macro Brasil","BCB: Cambio, Juros, Inflacao | Impacto direto no agro",
                  "Dolar forte = receita maior para exportador, mas insumos mais caros. Selic alta = custo de estocagem sobe.")
    y=PAGE_H-75
    rc=bcb.get("resumo_cambio",{}); rj=bcb.get("resumo_juros",{})
    cw=(PAGE_W-2*M-30)/4
    cards=[
        ("USD/BRL",f"{rc.get('brl_usd_atual',0):.4f}",f"5d: {rc.get('var_5d',0):+.2f}%  |  30d: {rc.get('var_30d',0):+.1f}%",AMBER),
        ("SELIC Meta",f"{rj.get('selic_atual',0):.1f}%","Taxa basica de juros",RED),
        ("Min 52s",f"{rc.get('min_52s',0):.4f}","Minima do dolar 52 semanas",GREEN),
        ("Max 52s",f"{rc.get('max_52s',0):.4f}","Maxima do dolar 52 semanas",RED),
    ]
    for i,(title,val,sub,clr) in enumerate(cards):
        x=M+i*(cw+10)
        panel(pdf,x,y-60,cw,62,bc=clr)
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7.5); pdf.drawString(x+10,y-12,title)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",18); pdf.drawString(x+10,y-36,val)
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",7); pdf.drawString(x+10,y-52,sub[:42])
    y-=82
    if img_brl:
        pdf.drawImage(img_brl,M,y-185,width=PAGE_W/2-M-10,height=180,preserveAspectRatio=True,mask="auto")
    rx=PAGE_W/2+10
    pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",10)
    pdf.drawString(rx,y-10,"POR QUE O MACRO IMPORTA PARA O AGRO?"); ry=y-30
    impacts=[
        ("Dolar forte","Receita em reais sobe para exportadores (soja, cafe, acucar). Mas insumos importados ficam mais caros.",AMBER),
        ("Selic alta","Custo de estocagem e capital de giro sobe. Produtor tende a vender mais rapido, pressionando precos.",RED),
        ("Inflacao (IPCA)","Se alimentos sobem, governo pode intervir (ex: zerar tarifa de importacao). Afeta margens.",PINK),
    ]
    for title,desc,clr in impacts:
        panel(pdf,rx,ry-40,PAGE_W/2-M-15,42,bc=clr)
        pdf.setFillColor(HexColor(clr)); pdf.setFont("Helvetica-Bold",8); pdf.drawString(rx+10,ry-12,title)
        tblock(pdf,rx+10,ry-26,desc,sz=7,clr=TEXT_MUT,mw=PAGE_W/2-M-40,ld=9)
        ry-=50
    ipca=bcb.get("ipca_mensal",[])
    if ipca and len(ipca)>3:
        ry-=10
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",8.5)
        pdf.drawString(rx,ry,"IPCA Mensal (ultimos 6 meses)"); ry-=13
        for rec in ipca[-6:]:
            dt=rec.get("data",rec.get("date",""))
            vl=rec.get("valor",rec.get("value",0))
            if vl:
                clr2=RED if float(vl)>0.5 else (AMBER if float(vl)>0.3 else GREEN)
                pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7.5)
                pdf.drawString(rx+10,ry,str(dt)[:10])
                pdf.setFillColor(HexColor(clr2)); pdf.setFont("Helvetica-Bold",7.5)
                pdf.drawString(rx+95,ry,f"{float(vl):.2f}%")
                ry-=11


# â”€â”€ PAGE 3: PRECOS GRAOS/OLEAGINOSAS (2x4) â”€â”€
def pg_prices_graos(pdf, pr, img):
    dbg(pdf); hdr(pdf,"Precos â€” Graos & Oleaginosas","60 dias | MA20 (tracejada amarela) | Min/Max 60d (pontilhada)",
                  "Graficos de precos futuros. Linha amarela = media 20 dias. Pontilhada = extremos dos ultimos 60 dias.")
    if img: pdf.drawImage(img,5,PAGE_H-430,width=PAGE_W-10,height=365,preserveAspectRatio=True,mask="auto")


# â”€â”€ PAGE 4: PRECOS CARNES/ENERGIA/METAIS (2x4) â”€â”€
def pg_prices_carnes(pdf, pr, img):
    dbg(pdf); hdr(pdf,"Precos â€” Carnes, Energia & Metais","60 dias | MA20 (tracejada amarela) | Min/Max 60d (pontilhada)",
                  "Mesma leitura: verde = subindo, vermelho = caindo. Preco atual no canto superior esquerdo.")
    if img: pdf.drawImage(img,5,PAGE_H-430,width=PAGE_W-10,height=365,preserveAspectRatio=True,mask="auto")


# â”€â”€ PAGE 5: TABELA DE VARIACOES â”€â”€
def pg_variations_table(pdf, pr):
    dbg(pdf); hdr(pdf,"Tabela de Variacoes","1 Dia, 1 Semana, 1 Mes, YTD | Maximo e Minimo 52 semanas",
                  "Numeros verdes = alta. Vermelhos = queda. Compare com a maxima/minima do ano para ver onde o preco esta.")
    y = PAGE_H - 78
    pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",9)
    pdf.drawString(M,y,"VARIACOES COMPLETAS â€” TODAS AS COMMODITIES"); y-=16
    cols=[M, M+120, M+195, M+265, M+335, M+420, M+510, M+610]
    hdrs2=["Commodity","Ultimo","1 Dia","1 Semana","1 Mes","52w Hi","52w Lo","Hi/Lo %"]
    pdf.setFont("Helvetica-Bold",7.5); pdf.setFillColor(HexColor(TEXT_MUT))
    for j,h in enumerate(hdrs2): pdf.drawString(cols[j],y,h)
    y-=4; pdf.setStrokeColor(HexColor(BORDER)); pdf.setLineWidth(0.4); pdf.line(M,y,PAGE_W-M,y); y-=12
    for sym,nm in GRID_ALL:
        if y < 35: break
        c1d=pchg(sym,pr,1); c1w=pchg(sym,pr,5); c1m=pchg(sym,pr,21)
        lc=last_close(sym,pr); h52=hi52(sym,pr); l52=lo52(sym,pr)
        # Stripe alternating
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",7.5)
        pdf.drawString(cols[0],y,f"{nm} ({sym})")
        pdf.setFont("Helvetica",7.5)
        if lc: pdf.setFillColor(HexColor(TEXT)); pdf.drawString(cols[1],y,num_fmt(lc))
        for j,val in enumerate([c1d,c1w,c1m]):
            if val is not None:
                cc=GREEN if val>=0 else RED; ss="+" if val>=0 else ""
                pdf.setFillColor(HexColor(cc)); pdf.drawString(cols[j+2],y,f"{ss}{val:.1f}%")
            else: pdf.setFillColor(HexColor(TEXT_DIM)); pdf.drawString(cols[j+2],y,"-")
        if h52: pdf.setFillColor(HexColor(TEXT_DIM)); pdf.drawString(cols[5],y,num_fmt(h52))
        if l52: pdf.drawString(cols[6],y,num_fmt(l52))
        # Hi/Lo position %
        if lc and h52 and l52 and h52!=l52:
            pct = (lc - l52) / (h52 - l52) * 100
            pc = GREEN if pct > 70 else (RED if pct < 30 else AMBER)
            pdf.setFillColor(HexColor(pc)); pdf.drawString(cols[7],y,f"{pct:.0f}%")
        y-=13


# â”€â”€ PAGE 6: SPREADS TERMOMETRO â”€â”€
def pg_spreads(pdf, sd, img_sp):
    dbg(pdf); hdr(pdf,"Relacoes de Preco â€” Spreads (Termometro)","Z-score e percentil 1 ano | Regime de mercado",
                  "Seta na esquerda (verde) = spread barato. Na direita (vermelho) = caro. Cinza = normal.")
    if img_sp: pdf.drawImage(img_sp,5,PAGE_H-320,width=PAGE_W-10,height=255,preserveAspectRatio=True,mask="auto")
    # Explicacoes por spread abaixo do grafico
    spreads=sd.get("spreads",{})
    y=PAGE_H-340
    pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",9)
    pdf.drawString(M,y,"O QUE CADA SPREAD SIGNIFICA PARA VOCE:"); y-=14
    for k,sp in spreads.items():
        if y<35: break
        nm=SPR_NM.get(k,sp.get("name",k))
        explain=SPR_EXPLAIN.get(k,"")
        if explain:
            panel(pdf,M,y-18,PAGE_W-2*M,20,bc=PURPLE)
            pdf.setFillColor(HexColor(CYAN)); pdf.setFont("Helvetica-Bold",7.5)
            pdf.drawString(M+8,y-12,nm+":")
            tw=pdf.stringWidth(nm+":","Helvetica-Bold",7.5)
            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7.5)
            pdf.drawString(M+12+tw,y-12,explain[:110])
            y-=24


# â”€â”€ PAGE 7: SPREADS DETALHAMENTO â”€â”€
def pg_spreads_detail(pdf, sd):
    dbg(pdf); hdr(pdf,"Spreads â€” Analise de Regime Detalhada","Valor atual, Z-score, percentil, tendencia | Classificacao de regime",
                  "Regime NORMAL = mercado equilibrado. COMPRESSAO ou DISSONANCIA = atencao, algo esta fora do padrao.")
    spreads=sd.get("spreads",{})
    y=PAGE_H-78
    for k,sp in spreads.items():
        if y<45: break
        nm=SPR_NM.get(k,sp.get("name",k)); reg=sp.get("regime","NORMAL")
        pctl=sp.get("percentile",50); zs=sp.get("zscore_1y",0)
        tr=sp.get("trend",""); tp=sp.get("trend_pct",0)
        desc=sp.get("description",""); cur=sp.get("current",""); un=sp.get("unit","")
        rc=GREEN if reg=="NORMAL" else RED
        if "DISSON" in reg or "COMPRESS" in reg: rc=RED
        elif reg!="NORMAL": rc=AMBER
        panel(pdf,M,y-52,PAGE_W-2*M,54,bc=rc)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",9)
        pdf.drawString(M+10,y-14,nm)
        pdf.setFillColor(HexColor(rc)); pdf.setFont("Helvetica-Bold",8)
        pdf.drawString(M+200,y-14,reg)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",7.5)
        pdf.drawString(M+310,y-14,f"Atual: {num_fmt(cur)} {un}")
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7)
        pdf.drawString(M+430,y-14,f"P{pctl:.0f} | Z:{zs:+.2f} | {tr} ({tp:+.1f}%)")
        if desc:
            pdf.setFont("Helvetica",7); pdf.setFillColor(HexColor(TEXT_DIM))
            pdf.drawString(M+10,y-30,desc[:140])
        explain=SPR_EXPLAIN.get(k,"")
        if explain:
            pdf.setFont("Helvetica",7); pdf.setFillColor(HexColor(GREEN))
            pdf.drawString(M+10,y-42,f"Produtor: {explain[:120]}")
        y-=60


# â”€â”€ PAGE 8: ESTOQUES â”€â”€
def pg_stocks(pdf, sw, img_stocks):
    dbg(pdf); hdr(pdf,"Estoques & Fundamentos","USDA stocks-to-use | Nivel atual vs media historica",
                  "Barras para a ESQUERDA (azul) = estoque apertado = preco tende a subir. Direita (vermelho) = excesso = preco pressionado.")
    comms=sw.get("commodities",{})
    if img_stocks:
        pdf.drawImage(img_stocks,5,PAGE_H-350,width=PAGE_W/2-10,height=295,preserveAspectRatio=True,mask="auto")
    # Legenda colorida
    rx=PAGE_W/2+10; y=PAGE_H-78
    pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",9)
    pdf.drawString(rx,y,"LEGENDA DE CORES"); y-=16
    legend_items = [
        (RED, "Muito acima da media (>15%)", "Excesso de oferta, preco tende a cair"),
        (AMBER, "Acima da media (5-15%)", "Oferta confortavel"),
        (TEXT_MUT, "Normal (-5% a +5%)", "Equilibrio oferta/demanda"),
        (BLUE, "Abaixo da media (-5% a -15%)", "Estoque apertando"),
        (CYAN, "Muito abaixo (<-15%)", "Escassez, preco tende a subir"),
    ]
    for clr, label, desc in legend_items:
        pdf.setFillColor(HexColor(clr)); pdf.rect(rx, y-2, 12, 10, fill=1, stroke=0)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",7.5)
        pdf.drawString(rx+18, y, label)
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6.5)
        pdf.drawString(rx+18, y-10, desc)
        y -= 24
    # Tabela detalhada
    y -= 8
    pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",9)
    pdf.drawString(rx,y,"DETALHAMENTO"); y-=14
    cols2=[rx,rx+75,rx+135,rx+195,rx+265]
    hdrs3=["Commodity","Estoque","Media","vs Media","Estado"]
    pdf.setFont("Helvetica-Bold",7); pdf.setFillColor(HexColor(TEXT_MUT))
    for j,h in enumerate(hdrs3): pdf.drawString(cols2[j],y,h)
    y-=3; pdf.setStrokeColor(HexColor(BORDER)); pdf.setLineWidth(0.3); pdf.line(rx,y,PAGE_W-M,y); y-=10
    for sym in list(comms.keys())[:14]:
        if y<50: break
        d=comms[sym]; nm=NM.get(sym,sym)
        cur=d.get("stock_current"); avg=d.get("stock_avg"); pva=d.get("price_vs_avg",0)
        st=d.get("state","")
        sc=STATE_CLR.get(st,TEXT_MUT); sn=STATE_PT.get(st,st)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",7)
        pdf.drawString(cols2[0],y,f"{nm}")
        if cur: pdf.drawString(cols2[1],y,f"{cur}")
        if avg: pdf.drawString(cols2[2],y,f"{avg}")
        if pva is not None:
            cc=RED if pva>15 else (AMBER if pva>5 else (CYAN if pva<-15 else TEXT_MUT))
            pdf.setFillColor(HexColor(cc)); pdf.drawString(cols2[3],y,f"{pva:+.1f}%")
        pdf.setFillColor(HexColor(sc)); pdf.setFont("Helvetica-Bold",6.5); pdf.drawString(cols2[4],y,sn)
        y-=10


# â”€â”€ PAGE 9: COT/CFTC â”€â”€
def pg_cot(pdf, cd, img_cot):
    dbg(pdf); hdr(pdf,"Posicionamento COT (CFTC)","Non-Commercial & Commercial net positions | Dados semanais",
                  "Barra verde = fundos comprados (apostando na alta). Vermelha = vendidos. Azul = hedgers (produtores/industria).")
    if img_cot: pdf.drawImage(img_cot,5,PAGE_H-390,width=PAGE_W-10,height=325,preserveAspectRatio=True,mask="auto")
    y=PAGE_H-408
    comms=cd.get("commodities",{})
    if comms:
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",8.5)
        pdf.drawString(M,y,"POSICOES DETALHADAS"); y-=12
        cols3=[M,M+70,M+155,M+240,M+330,M+430,M+530,M+640]
        hdrs4=["Comm.","NC Long","NC Short","NC Net","CM Long","CM Short","CM Net","OI"]
        pdf.setFont("Helvetica-Bold",6.5); pdf.setFillColor(HexColor(TEXT_MUT))
        for j,h in enumerate(hdrs4): pdf.drawString(cols3[j],y,h)
        y-=3; pdf.setStrokeColor(HexColor(BORDER)); pdf.setLineWidth(0.3); pdf.line(M,y,PAGE_W-M,y); y-=9
        sorted_c = sorted(comms.items(), key=lambda x: abs(x[1].get("legacy",{}).get("latest",{}).get("noncomm_net",0)), reverse=True)
        for sym,data in sorted_c[:10]:
            if y<35: break
            leg=data.get("legacy",{}).get("latest",{})
            nm=NM.get(sym,data.get("name",sym))[:10]
            ncl=leg.get("noncomm_long",0); ncs=leg.get("noncomm_short",0); ncn=leg.get("noncomm_net",0)
            cml=leg.get("comm_long",0); cms=leg.get("comm_short",0); cmn=leg.get("comm_net",0)
            oi=leg.get("open_interest",0)
            pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",6.5)
            pdf.drawString(cols3[0],y,nm)
            vals=[ncl,ncs,ncn,cml,cms,cmn,oi]
            for j,v in enumerate(vals):
                if v:
                    cc=TEXT
                    if j==2: cc=GREEN if v>0 else RED
                    if j==5: cc=BLUE if v>0 else AMBER
                    pdf.setFillColor(HexColor(cc))
                    pdf.drawString(cols3[j+1],y,f"{v:,.0f}")
            y-=9


# â”€â”€ PAGE 10: ENERGIA/EIA â”€â”€
def pg_energy(pdf, ed, img_eia):
    dbg(pdf); hdr(pdf,"Energia â€” Mercados EIA","Petroleo, gas, estoques, producao, refino | 52 semanas",
                  "Diesel = frete do grao. Gas natural = custo do fertilizante. Etanol = demanda de milho nos EUA.")
    if img_eia: pdf.drawImage(img_eia,5,PAGE_H-380,width=PAGE_W-10,height=315,preserveAspectRatio=True,mask="auto")
    y=PAGE_H-398
    impacts=[
        ("DIESEL e FRETE","Diesel e o principal custo de transporte de graos (20-30% do frete rodoviario). Alta no diesel = margem menor para quem vende FOB interior.",AMBER),
        ("ETANOL e MILHO","EUA usa ~35% do milho para etanol. Producao de etanol alta = demanda por milho firme.",GREEN),
        ("GAS e FERTILIZANTES","Gas natural e materia-prima para ureia e MAP. Gas caro = fertilizante caro = custo de producao maior na safrinha.",RED),
    ]
    cw=(PAGE_W-2*M-20)/3
    for i,(t,d,clr) in enumerate(impacts):
        x=M+i*(cw+10)
        panel(pdf,x,y-58,cw,60,bc=clr)
        pdf.setFillColor(HexColor(clr)); pdf.setFont("Helvetica-Bold",8); pdf.drawString(x+10,y-12,t)
        tblock(pdf,x+10,y-28,d,sz=6.5,clr=TEXT_MUT,mw=cw-22,ld=9)
    sums=ed.get("analysis_summary",[])
    if sums:
        y-=75
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",8.5)
        pdf.drawString(M,y,"RESUMO ANALITICO EIA"); y-=12
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7.5)
        for s in sums[:5]:
            if y<25: break
            pdf.drawString(M+5,y,f"  {s[:140]}"); y-=11


# â”€â”€ PAGE 11: MERCADO FISICO â”€â”€
def pg_physical(pdf, phys, img_phbr):
    dbg(pdf); hdr(pdf,"Mercado Fisico â€” Brasil & Internacional","CEPEA/ESALQ + origens globais | Precos em moeda local",
                  "Precos do mercado fisico (a vista). Compara com futuros para ver base e oportunidades de comercializacao.")
    intl=phys.get("international",{})
    if img_phbr: pdf.drawImage(img_phbr,5,PAGE_H-270,width=PAGE_W-10,height=205,preserveAspectRatio=True,mask="auto")
    y=PAGE_H-285
    pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",9)
    pdf.drawString(M,y,"TODOS OS MERCADOS FISICOS"); y-=14
    cols4=[M,M+120,M+210,M+300,M+400,M+520]
    hdrs5=["Origem","Preco","Unidade","Variacao","Fonte","Data"]
    pdf.setFont("Helvetica-Bold",7); pdf.setFillColor(HexColor(TEXT_MUT))
    for j,h in enumerate(hdrs5): pdf.drawString(cols4[j],y,h)
    y-=3; pdf.setStrokeColor(HexColor(BORDER)); pdf.setLineWidth(0.3); pdf.line(M,y,PAGE_W-M,y); y-=10
    for key,d in intl.items():
        if y<30: break
        label=d.get("label","").encode("ascii","ignore").decode()
        if not label: label=key
        price=d.get("price",""); unit=d.get("price_unit","")
        trend=d.get("trend",""); src=d.get("source",""); per=d.get("period","")
        is_br = "_BR" in key
        pdf.setFillColor(HexColor(TEAL if is_br else TEXT)); pdf.setFont("Helvetica",7)
        pdf.drawString(cols4[0],y,label[:20])
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",7)
        pdf.drawString(cols4[1],y,str(price))
        pdf.setFont("Helvetica",6.5); pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.drawString(cols4[2],y,str(unit)[:15])
        tc=GREEN if trend.startswith("+") else (RED if trend.startswith("-") else TEXT_MUT)
        pdf.setFillColor(HexColor(tc)); pdf.setFont("Helvetica-Bold",7)
        pdf.drawString(cols4[3],y,trend)
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6)
        pdf.drawString(cols4[4],y,str(src)[:25])
        pdf.drawString(cols4[5],y,str(per)[:12])
        y-=9


# â”€â”€ PAGE 12: CALENDARIO + AGENDA â”€â”€
def pg_calendar(pdf, cal, rd, dr):
    dbg(pdf); hdr(pdf,"Calendario de Eventos & Roteiro","Proximos releases | Blocos para video | Impacto produtor",
                  "Eventos HIGH impactam preco diretamente (ex: WASDE, USDA Stocks). Fique atento nas datas.")
    y=PAGE_H-78
    events=cal.get("events",[]) if isinstance(cal,dict) else (cal if isinstance(cal,list) else [])
    lw=PAGE_W/2-M-5
    pdf.setFillColor(HexColor(AMBER)); pdf.setFont("Helvetica-Bold",10)
    pdf.drawString(M,y,"PROXIMOS EVENTOS"); y2=y; y-=15
    if events:
        for ev in events[:12]:
            if y<50: break
            nm=ev.get("name",ev.get("event","")); dt=ev.get("date",ev.get("release_date",""))
            imp=ev.get("impact","")
            ic=RED if imp in ("HIGH","high") else (AMBER if imp in ("MEDIUM","medium") else TEXT_DIM)
            panel(pdf,M,y-15,lw,17,bc=ic)
            pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",7.5)
            pdf.drawString(M+8,y-10,f"{dt[:10]}  {nm[:45]}")
            pdf.setFillColor(HexColor(ic)); pdf.setFont("Helvetica-Bold",6.5)
            pdf.drawString(M+lw-45,y-10,imp.upper()[:10])
            y-=19
    rx=PAGE_W/2+10; ry=y2
    pdf.setFillColor(HexColor(PURPLE)); pdf.setFont("Helvetica-Bold",10)
    pdf.drawString(rx,ry,"BLOCOS PARA ROTEIRO DE VIDEO"); ry-=15
    blocos=dr.get("blocos",[])
    for bloco in blocos[:5]:
        if ry<120: break
        t=bloco.get("title",""); b=bloco.get("body","")
        panel(pdf,rx,ry-46,PAGE_W/2-M-15,48,bc=PURPLE)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",8); pdf.drawString(rx+10,ry-12,t[:60])
        tblock(pdf,rx+10,ry-26,b[:180],sz=7,clr=TEXT_MUT,mw=PAGE_W/2-M-40,ld=9)
        ry-=54
    dests=rd.get("destaques",[])
    fy=min(y,ry)-10
    if dests and fy>40:
        pdf.setFillColor(HexColor(GREEN)); pdf.setFont("Helvetica-Bold",9)
        pdf.drawString(M,fy,"IMPACTO PRODUTOR â€” MENSAGENS PARA O CAMPO"); fy-=14
        for d in dests[:4]:
            if fy<30: break
            imp=d.get("impacto_produtor",""); cm=d.get("commodity","")
            if imp:
                panel(pdf,M,fy-22,PAGE_W-2*M,24,bc=GREEN)
                pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",7.5)
                pdf.drawString(M+8,fy-14,f"[{cm}]")
                tblock(pdf,M+45,fy-14,imp[:170],sz=7,clr=TEXT2,mw=PAGE_W-2*M-60,ld=10)
                fy-=28


# â”€â”€ PAGE 13: CLIMA & SAFRA â”€â”€
def pg_weather(pdf, weather_data):
    dbg(pdf); hdr(pdf,"Clima & Safra","Previsao 15 dias para regioes agricolas chave",
                  "Chuva = bom para plantio/desenvolvimento. Seca prolongada = risco de quebra. Geada = perigo para cafe e trigo.")
    if not weather_data or not weather_data.get("regions"):
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",14)
        pdf.drawString(M,PAGE_H-120,"Dados climaticos nao disponiveis nesta execucao.")
        return
    regions = weather_data["regions"]
    enso = weather_data.get("enso", {})
    enso_status = enso.get("status", "N/A"); oni = enso.get("oni_value", "N/A")
    enso_color = RED if enso_status == "El Nino" else (BLUE if enso_status == "La Nina" else AMBER)
    pdf.setFillColor(HexColor(PANEL)); pdf.roundRect(M,PAGE_H-100,PAGE_W-2*M,22,4,fill=1)
    pdf.setFillColor(HexColor(enso_color)); pdf.setFont("Helvetica-Bold",11)
    pdf.drawString(M+8,PAGE_H-95,f"ENSO: {enso_status} (ONI = {oni})")
    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",9)
    pdf.drawString(M+250,PAGE_H-95,"Fonte: NOAA CPC")
    y_start=PAGE_H-125; card_w=(PAGE_W-2*M-20)/3; card_h=200; col=0; row_y=y_start
    for key, reg in regions.items():
        if reg.get("error"): continue
        x=M+col*(card_w+10); y=row_y
        pdf.setFillColor(HexColor(PANEL)); pdf.roundRect(x,y-card_h,card_w,card_h,6,fill=1)
        pdf.setFillColor(HexColor(CYAN)); pdf.setFont("Helvetica-Bold",11)
        pdf.drawString(x+8,y-18,reg.get("label",key))
        crops=", ".join(reg.get("crops",[])); pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",7)
        pdf.drawString(x+8,y-30,crops[:40])
        cur=reg.get("current",{}); pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",9)
        tmax=cur.get("temp_max","?"); tmin=cur.get("temp_min","?")
        pdf.drawString(x+8,y-48,f"Hoje: {tmin}C / {tmax}C")
        p7=reg.get("precip_7d_mm",0); p15=reg.get("precip_15d_mm",0)
        pdf.drawString(x+8,y-62,f"Chuva 7d: {p7:.0f}mm | 15d: {p15:.0f}mm")
        tmin7=reg.get("temp_min_7d","?"); tmax7=reg.get("temp_max_7d","?")
        pdf.drawString(x+8,y-76,f"Temp 7d: {tmin7}C a {tmax7}C")
        forecast=reg.get("forecast_15d",[])[:7]
        if forecast:
            bar_y=y-110; bar_w=(card_w-24)/7
            max_precip=max((f.get("precip_mm",0) for f in forecast),default=1) or 1
            pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6)
            pdf.drawString(x+8,bar_y+22,"Precip. 7 dias:")
            for i,f in enumerate(forecast):
                bx=x+8+i*bar_w; precip=f.get("precip_mm",0)
                bh=max(1,(precip/max_precip)*30)
                color=BLUE if precip<20 else (CYAN if precip<50 else AMBER)
                pdf.setFillColor(HexColor(color)); pdf.rect(bx+1,bar_y-30,bar_w-2,bh,fill=1)
                pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",5)
                day_label=f.get("date","")[-2:] if f.get("date") else ""
                pdf.drawCentredString(bx+bar_w/2,bar_y-38,day_label)
        alerts=reg.get("alerts",[])
        if alerts:
            alert_y=y-card_h+30
            for a in alerts[:2]:
                sev_color=RED if a.get("severity")=="ALTA" else AMBER
                pdf.setFillColor(HexColor(sev_color)); pdf.setFont("Helvetica-Bold",7)
                pdf.drawString(x+8,alert_y,f"! {a['type']}: {a.get('message','')[:50]}")
                alert_y-=12
        else:
            pdf.setFillColor(HexColor(GREEN)); pdf.setFont("Helvetica",7)
            pdf.drawString(x+8,y-card_h+18,"Sem alertas")
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6)
        pdf.drawString(x+8,y-card_h+6,f"Fonte: {reg.get('source','')}")
        col+=1
        if col>=3: col=0; row_y-=(card_h+10)
    summary=weather_data.get("summary","")
    if summary:
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",8)
        words=summary.split(); lines=[]; line=""
        for w in words:
            if len(line+" "+w)>120: lines.append(line); line=w
            else: line=(line+" "+w).strip()
        if line: lines.append(line)
        sy=45
        for l in lines[:3]: pdf.drawString(M,sy,l); sy-=12


# â”€â”€ PAGE 14: NOTICIAS â”€â”€
def pg_news(pdf, news_data):
    dbg(pdf); hdr(pdf,"Noticias & Contexto","Principais noticias do agronegocio",
                  "Acompanhe as noticias que movem o mercado. Brasil a esquerda, internacional a direita.")
    if not news_data or not news_data.get("news"):
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",14)
        pdf.drawString(M,PAGE_H-120,"Noticias nao disponiveis nesta execucao.")
        return
    articles=news_data["news"]
    br_news=[a for a in articles if a.get("category") in ("br_agro","br_gov")]
    us_news=[a for a in articles if a.get("category") in ("usda","market","us_agro")]
    CAT_COLORS={"br_agro":TEAL,"br_gov":GREEN,"usda":BLUE,"market":AMBER,"us_agro":CYAN}
    half_w=(PAGE_W-2*M-15)/2
    pdf.setFillColor(HexColor(TEAL)); pdf.setFont("Helvetica-Bold",13)
    pdf.drawString(M,PAGE_H-95,"BRASIL")
    y=PAGE_H-115
    for art in br_news[:8]:
        if y<60: break
        pdf.setFillColor(HexColor(PANEL)); pdf.roundRect(M,y-44,half_w,42,4,fill=1)
        cat_color=CAT_COLORS.get(art.get("category",""),TEXT_DIM)
        pdf.setFillColor(HexColor(cat_color)); pdf.setFont("Helvetica-Bold",7.5)
        pdf.drawString(M+6,y-12,art.get("source","")[:25])
        title=art.get("title","")
        if len(title)>75: title=title[:72]+"..."
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",9)
        pdf.drawString(M+6,y-26,title)
        desc=art.get("description","")
        if len(desc)>90: desc=desc[:87]+"..."
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",7)
        pdf.drawString(M+6,y-38,desc)
        y-=50
    rx=M+half_w+15
    pdf.setFillColor(HexColor(BLUE)); pdf.setFont("Helvetica-Bold",13)
    pdf.drawString(rx,PAGE_H-95,"INTERNACIONAL")
    y=PAGE_H-115
    for art in us_news[:8]:
        if y<60: break
        pdf.setFillColor(HexColor(PANEL)); pdf.roundRect(rx,y-44,half_w,42,4,fill=1)
        cat_color=CAT_COLORS.get(art.get("category",""),TEXT_DIM)
        pdf.setFillColor(HexColor(cat_color)); pdf.setFont("Helvetica-Bold",7.5)
        pdf.drawString(rx+6,y-12,art.get("source","")[:25])
        title=art.get("title","")
        if len(title)>75: title=title[:72]+"..."
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",9)
        pdf.drawString(rx+6,y-26,title)
        desc=art.get("description","")
        if len(desc)>90: desc=desc[:87]+"..."
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",7)
        pdf.drawString(rx+6,y-38,desc)
        y-=50
    total=news_data.get("total_news",0); cats=news_data.get("by_category",{})
    cat_str=" | ".join(f"{k}: {v}" for k,v in cats.items()) if cats else ""
    pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",8)
    pdf.drawString(M,40,f"Total: {total} artigos | {cat_str}")
    pdf.drawString(M,28,f"Fontes: Canal Rural, Google News, Yahoo Finance, USDA | Gerado: {TODAY_BR}")


# â”€â”€ PAGE 15: GLOSSARIO PARA PRODUTOR â”€â”€
def pg_glossary(pdf):
    dbg(pdf); hdr(pdf,"Glossario â€” Termos do Relatorio","Explicacao simples dos termos tecnicos usados neste relatorio",
                  "Se voce nao entendeu algum termo, esta pagina explica tudo em linguagem de produtor.")
    y = PAGE_H - 78
    terms = [
        ("COT (Commitment of Traders)","Relatorio semanal da CFTC que mostra quem esta comprado e vendido nos mercados futuros. Fundos (Non-Commercial) apostam em direcao. Hedgers (Commercial) protegem posicao."),
        ("Z-score","Mede quantos 'desvios' o valor atual esta da media. Z > +2 = muito acima do normal. Z < -2 = muito abaixo."),
        ("Percentil (P)","Posicao relativa em 1 ano. P10 = esta nos 10% mais baixos do ano. P90 = nos 10% mais altos."),
        ("Spread","Diferenca entre dois precos. Ex: Crush de Soja = preco do oleo + farelo menos preco da soja em grao."),
        ("Stocks-to-use","Estoque dividido pelo consumo. Quanto menor, mais apertado o mercado e maior a pressao de alta."),
        ("MA20 (Media Movel 20 dias)","Media dos precos dos ultimos 20 dias. Linha amarela tracejada nos graficos. Se preco esta acima da MA20, tendencia e de alta."),
        ("BRL/USD","Cotacao do dolar em reais. Dolar alto = receita maior em reais para exportador, mas insumos mais caros."),
        ("SELIC","Taxa basica de juros do Brasil. Selic alta = credito caro = custo de estocagem e financiamento sobe."),
        ("WASDE","Relatorio mensal do USDA com estimativas globais de oferta e demanda de graos, oleaginosas e carnes."),
        ("Mercado Fisico (Cash)","Preco do produto entregue fisicamente, ao contrario do futuro que e um contrato para entrega futura."),
        ("Regime de Mercado","Classificacao do spread: NORMAL, COMPRESSAO (spread diminuindo), DISSONANCIA (descolamento atipico)."),
        ("Non-Commercial (NC)","Fundos de investimento e especuladores nos mercados futuros. Quando compram muito, pode indicar alta."),
        ("Commercial (CM)","Produtores e industria que usam futuros para proteger (hedge). Vendem quando precos estao altos."),
        ("ENSO (El Nino / La Nina)","Fenomeno climatico que afeta chuvas no Brasil. El Nino = seca no Norte/Nordeste. La Nina = seca no Sul."),
        ("Basis (Base)","Diferenca entre preco fisico local e preco futuro. Base forte = mercado local demandando mais que Chicago."),
    ]
    for term, desc in terms:
        if y < 35: break
        h_needed = max(22, 12 + len(desc) // 100 * 10 + 10)
        panel(pdf, M, y - h_needed, PAGE_W - 2*M, h_needed, bc=PURPLE)
        pdf.setFillColor(HexColor(CYAN)); pdf.setFont("Helvetica-Bold", 8.5)
        pdf.drawString(M + 10, y - 12, term)
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica", 7.5)
        tblock(pdf, M + 10, y - 24, desc, sz=7.5, clr=TEXT_MUT, mw=PAGE_W - 2*M - 25, ld=10)
        y -= h_needed + 6


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
#  BUILD
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def build_pdf():
    print(f"  Data: {TODAY_BR} ({WDAY})")
    print("  Carregando dados...")
    pr   = sload(DATA_RAW, "price_history.json")
    sd   = sload(DATA_PROC, "spreads.json")
    ed   = sload(DATA_PROC, "eia_data.json")
    cd   = sload(DATA_PROC, "cot.json")
    sw   = sload(DATA_PROC, "stocks_watch.json")
    bcb  = sload(DATA_PROC, "bcb_data.json")
    phys = sload(DATA_PROC, "physical_intl.json")
    cal  = sload(DATA_PROC, "calendar.json")
    dr   = sload(DATA_PROC, "daily_reading.json")
    rd   = sload(DATA_PROC, "report_daily.json")
    wt   = sload(DATA_PROC, "weather_agro.json")
    nw   = sload(DATA_PROC, "news.json")
    if not pr: print("  [ERRO] price_history.json nao encontrado!"); sys.exit(1)
    sk=list(pr.keys())[0]; sv=pr[sk]
    print(f"  [OK] prices: {len(pr)} symbols, {len(sv)} records" if isinstance(sv,list) else f"  [WARN] tipo: {type(sv)}")
    setup_mpl()
    print("  Graficos: precos graos (2x4)..."); img_graos = chart_price_grid_2x4(GRID_GRAOS, pr)
    print("  Graficos: precos carnes (2x4)..."); img_carnes = chart_price_grid_2x4(GRID_CARNES, pr)
    print("  Graficos: spreads..."); img_sp = chart_spreads(sd)
    print("  Graficos: EIA..."); img_eia = chart_eia(ed)
    print("  Graficos: COT..."); img_cot = chart_cot(cd)
    print("  Graficos: estoques..."); img_stocks = chart_stocks(sw)
    print("  Graficos: BRL/USD..."); img_brl = chart_macro_brl(bcb)
    print("  Graficos: fisico BR..."); img_phbr = chart_physical_br(phys)
    os.makedirs(REPORT_DIR, exist_ok=True)
    T = 15
    print(f"  Montando PDF ({T} paginas)...")
    pdf = canvas.Canvas(OUTPUT_PDF, pagesize=landscape(A4))
    pdf.setTitle(f"AgriMacro Diario - {TODAY_STR}"); pdf.setAuthor("AgriMacro v3.2")

    pg_cover(pdf,rd,dr,bcb);                ftr(pdf,1,T);  pdf.showPage()  # 1. Capa
    pg_macro(pdf,bcb,img_brl);              ftr(pdf,2,T);  pdf.showPage()  # 2. Macro
    pg_prices_graos(pdf,pr,img_graos);      ftr(pdf,3,T);  pdf.showPage()  # 3. Precos Graos
    pg_prices_carnes(pdf,pr,img_carnes);    ftr(pdf,4,T);  pdf.showPage()  # 4. Precos Carnes
    pg_variations_table(pdf,pr);            ftr(pdf,5,T);  pdf.showPage()  # 5. Tabela Variacoes
    pg_spreads(pdf,sd,img_sp);              ftr(pdf,6,T);  pdf.showPage()  # 6. Spreads Termometro
    pg_spreads_detail(pdf,sd);              ftr(pdf,7,T);  pdf.showPage()  # 7. Spreads Detalhe
    pg_stocks(pdf,sw,img_stocks);           ftr(pdf,8,T);  pdf.showPage()  # 8. Estoques
    pg_cot(pdf,cd,img_cot);                 ftr(pdf,9,T);  pdf.showPage()  # 9. COT
    pg_energy(pdf,ed,img_eia);              ftr(pdf,10,T); pdf.showPage()  # 10. Energia
    pg_physical(pdf,phys,img_phbr);         ftr(pdf,11,T); pdf.showPage()  # 11. Fisico
    pg_calendar(pdf,cal,rd,dr);             ftr(pdf,12,T); pdf.showPage()  # 12. Calendario
    pg_weather(pdf,wt);                     ftr(pdf,13,T); pdf.showPage()  # 13. Clima
    pg_news(pdf,nw);                        ftr(pdf,14,T); pdf.showPage()  # 14. Noticias
    pg_glossary(pdf);                       ftr(pdf,15,T); pdf.showPage()  # 15. Glossario

    pdf.save()
    sz=os.path.getsize(OUTPUT_PDF)/1024
    print(f"\n  PDF: {OUTPUT_PDF}"); print(f"  Tamanho: {sz:.0f} KB | Paginas: {T}")

if __name__=="__main__":
    print("="*60); print("AgriMacro v3.2 - Relatorio PDF Profissional Enhanced"); print("="*60)
    build_pdf(); print("\nConcluido!")

