#!/usr/bin/env python3
"""
AgriMacro v3.2 - Relatorio PDF PROFISSIONAL v6
~18 paginas landscape A4, tema dark, TODOS os dados do pipeline.
NUNCA inclui ibkr_portfolio.json.

Melhorias v5:
- Paginas dedicadas por commodity (Soja, Milho, Boi, Cafe)
- Graficos maiores e mais legiveis
- Sazonalidade (media mensal historica vs ano atual)
- Linguagem de produtor rural (sem siglas tecnicas)
- Perguntas do dia COM explicacao de posicionamento
- Mercado fisico sem linhas vazias (None)
- Spreads com explicacao pratica integrada
- 18 paginas total
"""
import json, os, sys, math, re

# ── AA+QA Engine Gate ────────────────────────────────────────
try:
    from aa_qa_engine import run_audit as _run_qa_audit
    HAS_QA_ENGINE = True
except ImportError:
    HAS_QA_ENGINE = False
    print("⚠️  aa_qa_engine.py não encontrado — QA desabilitado")
from datetime import datetime
from io import BytesIO
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.colors import HexColor
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader

# ── Disclosure / Aviso Legal ──
try:
    from disclosure import get_cover_disclaimer, get_disclosure_page
    HAS_DISCLOSURE = True
except ImportError:
    HAS_DISCLOSURE = False
    print("⚠️  disclosure.py não encontrado — Disclosure desabilitado")


# ── CONFIG ──
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
EXPLAIN_BG="#1a2e1a"
# Badge colors for origin differentiation
CHI_BG="#1a2040"; CHI_CLR="#60a5fa"
BR_BG="#1a3020";  BR_CLR="#4ade80"
PAGE_W, PAGE_H = landscape(A4)
M = 28  # margem

NM = {
    "ZC":"Milho","ZS":"Soja","ZW":"Trigo SRW","KE":"Trigo HRW","ZM":"Farelo Soja",
    "ZL":"Oleo Soja","KC":"Cafe Arabica","SB":"Acucar #11","CT":"Algodao #2",
    "CC":"Cacau","OJ":"Suco Laranja","LE":"Boi Gordo","GF":"Boi Engorda",
    "HE":"Porco Magro","CL":"Petroleo WTI","NG":"Gas Natural","GC":"Ouro",
    "SI":"Prata","RB":"Gasolina RBOB","DX":"Indice Dolar",
}
# Nomes amigaveis para produtor (sem siglas)
NM_PROD = {
    "ZS":"Soja","ZC":"Milho","ZW":"Trigo","ZM":"Farelo de Soja",
    "ZL":"Oleo de Soja","KC":"Cafe","SB":"Acucar","CT":"Algodao",
    "CC":"Cacau","OJ":"Suco de Laranja","LE":"Boi Gordo","GF":"Boi de Engorda",
    "HE":"Porco","CL":"Petroleo","NG":"Gas Natural","GC":"Ouro","SI":"Prata",

    "ETH":"Etanol Hidratado","ETN":"Etanol Anidro",
    "RB":"Gasolina RBOB","HO":"Diesel/Heating Oil","DX":"Indice Dolar",}
# Mapeamento completo de tickers para nomes em portugues (para substituicao em texto)
TICKER_MAP = {
    "CT":"Algodao","SB":"Acucar","OJ":"Suco de Laranja","GC":"Ouro","SI":"Prata",
    "ZS":"Soja","ZC":"Milho","ZW":"Trigo","ZM":"Farelo de Soja","ZL":"Oleo de Soja",
    "KC":"Cafe","CC":"Cacau","LE":"Boi Gordo","GF":"Boi de Engorda","HE":"Porco",
    "CL":"Petroleo","NG":"Gas Natural","KE":"Trigo HRW","RB":"Gasolina","DX":"Dolar",
}
# Unidades por commodity
UNITS = {
    "ZS":"¢/bu","ZC":"¢/bu","ZW":"¢/bu","ZM":"$/ton","ZL":"¢/lb",
    "KC":"¢/lb","SB":"¢/lb","CT":"¢/lb","CC":"$/ton","OJ":"¢/lb",
    "LE":"¢/lb","GF":"¢/lb","HE":"¢/lb","CL":"$/bbl","NG":"$/MMBtu",
    "GC":"$/oz","SI":"$/oz","RB":"$/gal","HO":"$/gal","DX":"index",
}
# Mapeamento fisico BR
PHYS_MAP = {
    "ZS":"ZS_BR","ZC":"ZC_BR","KC":"KC_BR","LE":"LE_BR","CT":"CT_BR","ZW":"ZW_BR",
}
# Commodities para paginas dedicadas (OBRIGATORIOS: soja, milho, boi; + cafe)
DEDICATED = [
    ("ZS","Soja","soja","A rainha do agro brasileiro",CYAN),
    ("ZC","Milho","milho","Base da racao e do etanol",GREEN),
    ("LE","Boi Gordo","boi gordo","Pecuaria de corte",AMBER),
    ("KC","Cafe","cafe","O ouro verde do Brasil",TEAL),
]
# Demais commodities (pagina compacta)
OTHERS = [("ZW","Trigo"),("ZM","Far.Soja"),("ZL","Oleo Soja"),("SB","Acucar"),
           ("CT","Algodao"),("CC","Cacau"),("HE","Porco"),("GF","Boi Eng."),
           ("CL","Petroleo"),("NG","Gas Nat."),("GC","Ouro"),("OJ","Suco Lar.")]
GRID_ALL = [(s,n) for s,n,*_ in DEDICATED] + OTHERS

SPR_NM = {"soy_crush":"Esmagamento da Soja","ke_zw":"Trigo Duro vs Trigo Mole","zl_cl":"Oleo de Soja vs Petroleo",
           "feedlot":"Margem do Confinamento","zc_zm":"Milho vs Farelo de Soja","zc_zs":"Relacao Milho/Soja","le_gf":"Boi Gordo vs Engorda","crack":"Crack Spread (Refino)","gc_cl":"Ouro vs Petroleo"}
SPR_EXPLAIN = {
    "soy_crush":"Lucro da industria comprando soja e vendendo oleo + farelo. Sobe = industria paga mais pela soja. Bom pra quem vende soja.",
    "ke_zw":"Premio do trigo de qualidade sobre o trigo comum. Reflete demanda da industria de panificacao.",
    "zl_cl":"Competitividade do biodiesel de soja frente ao petroleo. Se sobe, biodiesel ganha espaco e puxa demanda por oleo de soja.",
    "feedlot":"Conta do confinador: boi gordo menos boi magro menos milho. Positivo = lucro. Negativo = prejuizo.",
    "zc_zm":"Milho contra farelo na racao. Se milho sobe relativo ao farelo, racao fica mais cara.",
    "zc_zs":"Ratio classico. Abaixo de 2.3 = plante mais milho. Acima de 2.5 = plante mais soja.",
    "le_gf":"Spread de reposicao. Gordo caro vs magro = lucro pro pecuarista. Magro subindo mais = aperto na oferta futura.",
    "crack":"Margem do refino: gasolina menos petroleo. Alto = refinarias lucrando. Baixo = demanda fraca por combustivel.",
    "gc_cl":"Ratio ouro/petroleo. Alto = medo e busca por seguranca. Baixo = economia aquecida.",
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

# ── DATA HELPERS ──
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

def closes_with_dates(sym, pr, n=252):
    """Return list of (date_str, close) tuples"""
    h = price_list(sym, pr)
    if not h: return []
    result = []
    for p in h[-n:]:
        c = p.get("close") or p.get("settlement")
        d = p.get("date","")
        if c and d:
            result.append((d, float(c)))
    return result

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

def hilo_pct(sym, pr):
    lc = last_close(sym, pr)
    h = hi52(sym, pr); l = lo52(sym, pr)
    if lc and h and l and h != l:
        return (lc - l) / (h - l) * 100
    return None


def replace_tickers(text):
    """Substitui tickers tecnicos por nomes em portugues no texto."""
    if not text: return text
    result = text
    eng_map = {
        "Soybean Oil / Crude Ratio": "Oleo de Soja vs Petroleo",
        "Soybean Oil/Crude": "Oleo de Soja/Petroleo",
        "Feedlot Margin": "Margem do Confinamento",
        "KC-CBOT Wheat Spread": "Trigo Duro vs Trigo Mole",
        "Corn/Soybean Ratio": "Relacao Milho/Soja",
        "Soy Crush": "Esmagamento da Soja",
        "Crush de Soja": "Esmagamento da Soja",
        "Crush Spread": "Margem de Esmagamento",
    }
    for eng, pt in eng_map.items():
        result = result.replace(eng, pt)
    for ticker, name in sorted(TICKER_MAP.items(), key=lambda x: len(x[0]), reverse=True):
        result = re.sub(r'\b' + ticker + r'\b', name, result)
    return result



# ── CONVERSAO & ARBITRAGEM ──
# Fatores de conversao para comparar Brasil vs Chicago
CONV = {
    "ZS": {"bu_kg": 27.216, "sc_kg": 60},   # Soja: 1 bu = 27.216kg, saca = 60kg
    "ZC": {"bu_kg": 25.4,   "sc_kg": 60},   # Milho: 1 bu = 25.4kg, saca = 60kg
    "LE": {"lb_kg": 0.4536, "ar_kg": 15},   # Boi: 1 lb = 0.4536kg, 1@ = 15kg
    "SB": {"lb_kg": 0.4536, "sc_kg": 50},   # Acucar: saca = 50kg
}


def sanitize_language(text, pr):
    """Substitui linguagem exagerada quando variacao PROXIMA nao justifica.
    Busca o % mencionado perto da palavra forte. Se nao achar, usa max global."""
    if not text or not pr: return text
    import re as _re
    result = text
    STRONG_WORDS = {
        "dispara": ("sobe", 5.0),
        "disparou": ("subiu", 5.0),
        "disparada": ("alta", 5.0),
        "despenca": ("recua", 5.0),
        "despencou": ("caiu", 5.0),
        "desaba": ("recua", 5.0),
        "desabou": ("caiu", 5.0),
        "derrete": ("cede", 5.0),
        "derreteu": ("cedeu", 5.0),
        "explode": ("avanca", 5.0),
        "explodiu": ("avancou", 5.0),
        "colapso": ("queda", 5.0),
        "colapsou": ("caiu", 5.0),
        "decolou": ("subiu", 5.0),
        "catapultou": ("subiu forte", 5.0),
        "afundou": ("caiu", 5.0),
        "foguete": ("alta", 5.0),
    }
    for strong, (moderate, min_pct) in STRONG_WORDS.items():
        if strong.lower() not in result.lower():
            continue
        # Busca % proximo da palavra (ate 30 chars depois)
        pattern = _re.escape(strong) + r'.{0,30}?(\d+[.,]\d+)\s*%'
        m = _re.search(pattern, result, _re.IGNORECASE)
        if m:
            nearby_pct = float(m.group(1).replace(",", "."))
        else:
            # Fallback: busca qualquer % na mesma frase
            sentence_pattern = r'[^.]*' + _re.escape(strong) + r'[^.]*?(\d+[.,]\d+)\s*%'
            m2 = _re.search(sentence_pattern, result, _re.IGNORECASE)
            if m2:
                nearby_pct = float(m2.group(1).replace(",", "."))
            else:
                nearby_pct = 0.0  # sem %, assume moderado
        # Substitui se a variacao proxima nao justifica a palavra forte
        if nearby_pct < min_pct:
            result = _re.sub(_re.escape(strong), moderate, result, flags=_re.IGNORECASE)
    return result


def chicago_to_brl(sym, chicago_price, brl_usd):
    """Converte preco de Chicago para unidade brasileira em R$"""
    if not chicago_price or not brl_usd or brl_usd == 0: return None
    c = CONV.get(sym)
    if not c: return None
    if sym in ("ZS", "ZC"):
        # c/bu -> R$/saca
        usd_per_bu = chicago_price / 100.0
        sc_per_bu = c["sc_kg"] / c["bu_kg"]  # sacas por bushel (invertido: bushels por saca)
        bu_per_sc = c["sc_kg"] / c["bu_kg"]  # quantos bushels cabem em 1 saca
        usd_per_sc = usd_per_bu * bu_per_sc
        return usd_per_sc * brl_usd
    elif sym == "LE":
        # c/lb -> R$/@
        usd_per_lb = chicago_price / 100.0
        lbs_per_ar = c["ar_kg"] / c["lb_kg"]  # lbs por arroba
        usd_per_ar = usd_per_lb * lbs_per_ar
        return usd_per_ar * brl_usd
    elif sym == "SB":
        # c/lb -> R$/saca 50kg
        usd_per_lb = chicago_price / 100.0
        lbs_per_sc = c["sc_kg"] / c["lb_kg"]  # lbs por saca
        usd_per_sc = usd_per_lb * lbs_per_sc
        return usd_per_sc * brl_usd
    return None

def get_brl_usd(bcb):
    """Extrai taxa BRL/USD mais recente do bcb_data"""
    rc = bcb.get("resumo_cambio", {})
    v = rc.get("brl_usd_atual")
    if v: return float(v)
    brl = bcb.get("brl_usd", [])
    if brl:
        for r in reversed(brl):
            val = r.get("valor") or r.get("value")
            if val: return float(val)
    return None

def calc_arbitrages(pr, phys, bcb):
    """Calcula arbitragens Brasil vs Chicago para soja, milho, boi, acucar"""
    fx = get_brl_usd(bcb)
    if not fx: return {}
    intl = phys.get("international", {})
    arbs = {}
    items = [
        ("ZS", "ZS_BR", "Soja", "R$/sc 60kg"),
        ("ZC", "ZC_BR", "Milho", "R$/sc 60kg"),
        ("LE", "LE_BR", "Boi Gordo", "R$/@"),
        ("SB", "SB_BR", "Acucar", "R$/sc 50kg"),
    ]
    for chi_sym, br_key, name, unit in items:
        chi_price = last_close(chi_sym, pr)
        br_data = intl.get(br_key, {})
        br_price = br_data.get("price")
        if chi_price and br_price:
            chi_brl = chicago_to_brl(chi_sym, chi_price, fx)
            if chi_brl and chi_brl > 0:
                spread = float(br_price) - chi_brl
                spread_pct = (spread / chi_brl) * 100
                arbs[chi_sym] = {
                    "name": name,
                    "br_price": float(br_price),
                    "chi_price_usd": chi_price,
                    "chi_price_brl": chi_brl,
                    "spread_brl": spread,
                    "spread_pct": spread_pct,
                    "unit": unit,
                    "fx": fx,
                    "br_source": br_data.get("source", "CEPEA"),
                }
    return arbs

def calc_extra_spreads(pr):
    """Calcula spreads extras que nao estao no spreads.json"""
    extras = {}
    # Boi Gordo vs Boi Engorda (spread de reposicao)
    le = last_close("LE", pr); gf = last_close("GF", pr)
    if le and gf and gf > 0:
        extras["le_gf"] = {"name": "Boi Gordo vs Boi Engorda", "current": le - gf,
            "unit": "c/lb", "ratio": le / gf,
            "explain": "Spread de reposicao. Gordo caro vs magro = lucro pro pecuarista. Magro subindo mais = aperto na oferta futura."}
    # Crack spread (precisa RB e CL)
    cl = last_close("CL", pr); rb = last_close("RB", pr)
    if cl and rb:
        # Simplificado: (RB * 42) - CL  (em $/bbl equivalente)
        crack = (rb * 42) - cl
        extras["crack"] = {"name": "Crack Spread (Gasolina)", "current": crack,
            "unit": "$/bbl", "ratio": rb * 42 / cl if cl > 0 else 0,
            "explain": "Margem do refino: gasolina menos petroleo. Alto = refinarias lucrando, estimula producao. Baixo = demanda fraca."}
    # Ouro/Petroleo
    gc = last_close("GC", pr)
    if gc and cl and cl > 0:
        extras["gc_cl"] = {"name": "Ouro / Petroleo", "current": gc / cl,
            "unit": "ratio", "ratio": gc / cl,
            "explain": "Poder de compra macro. Ratio alto = investidores buscando seguranca (ouro). Baixo = economia aquecida (petroleo)."}
    # Milho/Soja ratio (ja existe no spreads.json mas recalculamos pra ter certeza)
    zc = last_close("ZC", pr); zs = last_close("ZS", pr)
    if zc and zs and zs > 0:
        extras["corn_soy_ratio"] = {"name": "Relacao Milho/Soja", "current": zs / zc if zc > 0 else 0,
            "unit": "ratio",
            "explain": "Abaixo de 2.3 = plante mais milho. Acima de 2.5 = plante mais soja. Historico medio: 2.4."}
    return extras


# ── MPL SETUP ──
def setup_mpl():
    plt.rcParams.update({"figure.facecolor":BG,"axes.facecolor":PANEL,"axes.edgecolor":BORDER,
        "axes.labelcolor":TEXT,"text.color":TEXT,"xtick.color":TEXT_MUT,"ytick.color":TEXT_MUT,
        "grid.color":BORDER,"grid.alpha":0.2,"font.size":7.5,"font.family":"sans-serif"})

def fig2img(fig, dpi=150):
    buf = BytesIO()
    fig.savefig(buf,format="png",dpi=dpi,bbox_inches="tight",facecolor=fig.get_facecolor(),edgecolor="none",pad_inches=0.08)
    plt.close(fig); buf.seek(0)
    return ImageReader(buf)


# ── REPORTLAB HELPERS ──
def dbg(c):
    c.setFillColor(HexColor(BG)); c.rect(0,0,PAGE_W,PAGE_H,fill=1,stroke=0)

def hdr(c, title, sub="", explain=""):
    c.setFillColor(HexColor(PURPLE)); c.rect(0,PAGE_H-34,PAGE_W,34,fill=1,stroke=0)
    c.setFillColor(HexColor("#fff")); c.setFont("Helvetica-Bold",13); c.drawString(M,PAGE_H-24,title)
    c.setFillColor(HexColor(TEXT_MUT)); c.setFont("Helvetica",7.5)
    c.drawRightString(PAGE_W-M,PAGE_H-15,f"AgriMacro v3.2 | {TODAY_BR} ({WDAY})")
    if sub:
        c.setFillColor(HexColor(TEXT_MUT)); c.setFont("Helvetica",8)
        c.drawString(M,PAGE_H-47,sub)
    if explain:
        ey = PAGE_H - 62 if sub else PAGE_H - 47
        c.setFillColor(HexColor(EXPLAIN_BG)); c.rect(M-5,ey-12,PAGE_W-2*M+10,16,fill=1,stroke=0)
        c.setFillColor(HexColor(GREEN)); c.setFont("Helvetica-Bold",7.5)
        c.drawString(M,ey-7,"LEITURA RAPIDA: ")
        tw = c.stringWidth("LEITURA RAPIDA: ","Helvetica-Bold",7.5)
        c.setFillColor(HexColor(TEXT2)); c.setFont("Helvetica",7.5)
        c.drawString(M+tw,ey-7,explain[:130])

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


def origin_badge(c, x, y, is_brazil=True):
    """Desenha badge de origem: CHICAGO (azul) ou BRASIL (verde)"""
    if is_brazil:
        lbl = "BRASIL"; bg = BR_BG; clr = BR_CLR
    else:
        lbl = "CHICAGO"; bg = CHI_BG; clr = CHI_CLR
    tw = c.stringWidth(lbl, "Helvetica-Bold", 6.5) + 10
    c.setFillColor(HexColor(bg)); c.roundRect(x, y-3, tw, 12, 2, fill=1, stroke=0)
    c.setStrokeColor(HexColor(clr)); c.setLineWidth(0.5); c.roundRect(x, y-3, tw, 12, 2, fill=0, stroke=1)
    c.setFillColor(HexColor(clr)); c.setFont("Helvetica-Bold", 6.5); c.drawString(x+5, y, lbl)
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


# ═══════════════════════════════════════════
#  CHARTS
# ═══════════════════════════════════════════

def chart_commodity_main(sym, pr, color=CYAN):
    """Large 60d price chart for dedicated commodity page"""
    fig, ax = plt.subplots(figsize=(7.5, 3.8))
    fig.patch.set_facecolor(BG); ax.set_facecolor(PANEL)
    v = closes(sym, pr, 60)
    if len(v) > 3:
        x = list(range(len(v)))
        ax.plot(x, v, color=color, lw=2, zorder=3)
        ax.fill_between(x, min(v)*0.998, v, alpha=0.15, color=color)
        ax.scatter([x[-1]], [v[-1]], color=color, s=40, zorder=5, edgecolors="white", linewidths=0.8)
        if len(v) >= 20:
            ma20 = [np.mean(v[max(0,i-19):i+1]) for i in range(len(v))]
            ax.plot(x, ma20, color=AMBER, lw=1, ls="--", alpha=0.7, zorder=2, label="Media 20 dias")
        vmin = min(v); vmax = max(v)
        ax.axhline(vmax, color=RED, lw=0.6, ls=":", alpha=0.5, zorder=1)
        ax.axhline(vmin, color=GREEN, lw=0.6, ls=":", alpha=0.5, zorder=1)
        ax.text(0.02, 0.93, f"{v[-1]:,.1f}", transform=ax.transAxes, ha="left", va="top",
                fontsize=14, color=TEXT, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.3", facecolor=PANEL, edgecolor=BORDER, alpha=0.9))
        chg = ((v[-1]-v[0])/v[0])*100 if v[0]!=0 else 0
        cc = GREEN if chg >= 0 else RED; ss = "+" if chg >= 0 else ""
        ax.text(0.98, 0.93, f"{ss}{chg:.1f}% (60d)", transform=ax.transAxes, ha="right", va="top",
                fontsize=11, color=cc, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="#0f1117", edgecolor=cc, alpha=0.8, lw=0.7))
        ax.text(0.02, 0.04, f"Min 60d: {vmin:,.1f}", transform=ax.transAxes, ha="left", va="bottom",
                fontsize=7, color=GREEN, alpha=0.8)
        ax.text(0.98, 0.04, f"Max 60d: {vmax:,.1f}", transform=ax.transAxes, ha="right", va="bottom",
                fontsize=7, color=RED, alpha=0.8)
        ax.legend(fontsize=7, loc="upper center", framealpha=0.3)
    else:
        ax.text(0.5, 0.5, "Sem dados", transform=ax.transAxes, ha="center", va="center", fontsize=14, color=TEXT_MUT)
    origin = "CHICAGO"  # Todos os futuros sao CHICAGO (CME/CBOT/ICE)
    ax.set_title(f"Preco Futuro ({origin}) — Ultimos 60 dias", fontsize=10, color=TEXT, pad=5)
    ax.tick_params(labelbottom=False, labelsize=7); ax.grid(True, alpha=0.15)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.5)
    fig.subplots_adjust(left=0.08, right=0.97, top=0.90, bottom=0.05)
    return fig2img(fig)


def chart_seasonality(sym, pr, color=CYAN):
    """Seasonality chart: monthly average of historical data vs current year"""
    data = closes_with_dates(sym, pr, 500)  # ~2 years
    if len(data) < 60:
        return None
    # Group by month
    monthly_avg = defaultdict(list)
    current_year_data = {}
    current_year = TODAY.year
    for date_str, val in data:
        try:
            dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            month = dt.month
            if dt.year == current_year:
                current_year_data[month] = val
            else:
                monthly_avg[month].append(val)
        except:
            continue
    if not monthly_avg:
        # All data is current year — use it as both
        for date_str, val in data:
            try:
                dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
                monthly_avg[dt.month].append(val)
            except:
                continue
    months = sorted(monthly_avg.keys())
    if len(months) < 4:
        return None
    avg_vals = [np.mean(monthly_avg[m]) for m in months]
    fig, ax = plt.subplots(figsize=(4, 3))
    fig.patch.set_facecolor(BG); ax.set_facecolor(PANEL)
    month_labels = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
    ax.plot(months, avg_vals, color=TEXT_MUT, lw=1.5, ls="--", marker="o", ms=4, label="Media historica", zorder=2)
    ax.fill_between(months, min(avg_vals)*0.98, avg_vals, alpha=0.08, color=TEXT_MUT)
    # Current year overlay
    cur_months = sorted(current_year_data.keys())
    if cur_months:
        cur_vals = [current_year_data[m] for m in cur_months]
        ax.plot(cur_months, cur_vals, color=color, lw=2, marker="o", ms=5, label=f"{current_year}", zorder=3)
    # Mark current month
    cm = TODAY.month
    if cm in current_year_data:
        ax.scatter([cm], [current_year_data[cm]], color=color, s=80, zorder=5, edgecolors="white", linewidths=1)
        ax.annotate("HOJE", (cm, current_year_data[cm]), textcoords="offset points",
                    xytext=(8, 8), fontsize=7, color=color, fontweight="bold")
    ax.set_xticks(range(1,13))
    ax.set_xticklabels(month_labels, fontsize=6)
    ax.set_title("Sazonalidade (media mensal)", fontsize=9, color=TEXT, pad=4)
    ax.legend(fontsize=6.5, loc="best", framealpha=0.3)
    ax.grid(True, alpha=0.15); ax.tick_params(labelsize=6)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.4)
    fig.subplots_adjust(left=0.12, right=0.96, top=0.88, bottom=0.12)
    return fig2img(fig)


def chart_others_grid(items, pr):
    """Compact 3x4 grid for other commodities"""
    n = len(items)
    cols = 4; rows = math.ceil(n / cols)
    fig, axes = plt.subplots(rows, cols, figsize=(11.5, rows * 2.2))
    fig.patch.set_facecolor(BG)
    if rows == 1: axes = [axes]
    for idx, (sym, nm) in enumerate(items):
        r, c2 = divmod(idx, cols)
        ax = axes[r][c2] if rows > 1 else axes[c2]
        ax.set_facecolor(PANEL)
        v = closes(sym, pr, 60)
        if len(v) > 3:
            x = list(range(len(v)))
            ax.plot(x, v, color=CYAN, lw=1.3, zorder=3)
            ax.fill_between(x, min(v)*0.998, v, alpha=0.1, color=CYAN)
            ax.scatter([x[-1]], [v[-1]], color=CYAN, s=18, zorder=5, edgecolors="white", linewidths=0.4)
            if len(v) >= 20:
                ma20 = [np.mean(v[max(0,i-19):i+1]) for i in range(len(v))]
                ax.plot(x, ma20, color=AMBER, lw=0.7, ls="--", alpha=0.6)
            ax.text(0.03, 0.90, f"{v[-1]:,.1f}", transform=ax.transAxes, ha="left", va="top",
                    fontsize=8, color=TEXT, fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.15", facecolor=PANEL, edgecolor=BORDER, alpha=0.8))
            chg = ((v[-1]-v[0])/v[0])*100 if v[0]!=0 else 0
            cc = GREEN if chg >= 0 else RED; ss = "+" if chg >= 0 else ""
            ax.text(0.97, 0.90, f"{ss}{chg:.1f}%", transform=ax.transAxes, ha="right", va="top",
                    fontsize=7, color=cc, fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.15", facecolor="#0f1117", edgecolor=cc, alpha=0.7, lw=0.4))
        else:
            ax.text(0.5, 0.5, "S/D", transform=ax.transAxes, ha="center", va="center", fontsize=10, color=TEXT_MUT)
        ax.set_title(f"{NM_PROD.get(sym,nm)}", fontsize=7.5, color=TEXT, pad=2, fontweight="bold")
        ax.tick_params(labelbottom=False, labelleft=False, length=0)
        for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.3)
    # Hide unused
    for idx in range(n, rows * cols):
        r, c2 = divmod(idx, cols)
        (axes[r][c2] if rows > 1 else axes[c2]).set_visible(False)
    fig.subplots_adjust(hspace=0.5, wspace=0.18, left=0.01, right=0.99, top=0.95, bottom=0.02)
    return fig2img(fig)


def chart_spreads(sd):
    """Termometro de spreads REDESENHADO: fontes maiores, termos em portugues"""
    spreads = sd.get("spreads",{})
    if not spreads: return None
    items=list(spreads.items()); n=len(items)
    fig,axes=plt.subplots(n,1,figsize=(11,max(n*1.9,6)))
    fig.patch.set_facecolor(BG)
    if n==1: axes=[axes]
    for i,(k,sp) in enumerate(items):
        ax=axes[i]; ax.set_facecolor(BG)
        pctl=sp.get("percentile",50); reg=sp.get("regime","NORMAL")
        nm=SPR_NM.get(k,sp.get("name",k))
        cur=sp.get("current",""); un=sp.get("unit","")
        zc=["#22c55e","#86efac","#fde047","#fb923c","#ef4444"]
        seg_labels=["Barato","Abaixo","Normal","Acima","Caro"]
        for zi in range(5):
            ax.barh(0,20,left=zi*20,height=0.5,color=zc[zi],alpha=0.4)
        for zi in range(5):
            ax.text(zi*20+10, -0.65, seg_labels[zi], ha="center", va="top", fontsize=9, color=TEXT_DIM, fontweight="bold")
        mc=GREEN if pctl<30 else (AMBER if pctl<70 else RED)
        ax.plot(pctl, 0.48, "v", color=mc, ms=20, zorder=5)
        ax.text(pctl, 0.85, f"{pctl:.0f}", ha="center", va="bottom", fontsize=10, color=mc, fontweight="bold")
        ax.text(-6, 0, nm, va="center", ha="right", fontsize=12, color=TEXT, fontweight="bold")
        if cur:
            ax.text(-6, -0.4, f"Atual: {num_fmt(cur)} {un}", va="center", ha="right", fontsize=8, color=TEXT_DIM)
        rc=GREEN if reg=="NORMAL" else RED
        if "DISSON" in reg or "COMPRESS" in reg: rc=RED
        elif reg!="NORMAL": rc=AMBER
        regime_txt = "Equilibrado" if reg=="NORMAL" else ("Comprimido!" if "COMPRESS" in reg else ("Fora do padrao!" if "DISSON" in reg else reg))
        ax.text(107, 0, regime_txt, va="center", ha="left", fontsize=11, color=rc, fontweight="bold")
        ax.text(107, -0.4, f"Nivel {pctl:.0f} de 100", va="center", ha="left", fontsize=8.5, color=TEXT_MUT)
        ax.set_xlim(-5,105); ax.set_ylim(-1.0,1.1)
        ax.axis("off")
    fig.suptitle("TERMOMETRO DE RELACOES — Onde cada spread esta hoje", fontsize=13, color=TEXT, fontweight="bold", y=0.99)
    fig.subplots_adjust(hspace=1.0, left=0.25, right=0.76, top=0.93, bottom=0.03)
    return fig2img(fig)


def chart_eia(ed):
    series=ed.get("series",{})
    if not series: return None
    prio=["wti_spot","crude_stocks","gasoline_stocks","ethanol_production","crude_production","distillate_stocks"]
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
            if len(vp) >= 20:
                ma20 = [np.mean(vp[max(0,i-19):i+1]) for i in range(len(vp))]
                ax.plot(x, ma20, color=AMBER, lw=0.7, ls="--", alpha=0.6)
        nm=EIA_NM.get(key,key); lat=s.get("latest_value",""); un=hist[0].get("unit","") if hist else ""
        wow=s.get("wow_change_pct",0) or 0; ws="+" if wow>=0 else ""
        # Formata numeros EIA — v3.3: estoques ja vem em MBbl (mil barris)
        # Nao aplicar K/M adicional em estoques (evita "844K MBbl" que confunde)
        lat_fmt = lat
        is_stock = "stock" in key.lower()
        if lat and isinstance(lat, (int, float)):
            if is_stock:
                lat_fmt = f"{lat:,.0f}"  # Ex: 844,041 (sem K/M)
            elif abs(lat) >= 1e6: lat_fmt = f"{lat/1e6:,.1f}M"
            elif abs(lat) >= 1e3: lat_fmt = f"{lat/1e3:,.1f}K"
            else: lat_fmt = f"{lat:,.1f}"
        elif lat:
            try:
                lat_f = float(str(lat).replace(",",""))
                if is_stock:
                    lat_fmt = f"{lat_f:,.0f}"
                elif abs(lat_f) >= 1e6: lat_fmt = f"{lat_f/1e6:,.1f}M"
                elif abs(lat_f) >= 1e3: lat_fmt = f"{lat_f/1e3:,.1f}K"
                else: lat_fmt = f"{lat_f:,.1f}"
            except (ValueError, TypeError):
                lat_fmt = str(lat)
        # Unidade mais clara para estoques
        un_display = un
        if "stock" in key.lower() and un in ("","MBbl","thousand_bbl"):
            un_display = "mil bbl"
        ax.set_title(f"{nm}: {lat_fmt} {un_display}  ({ws}{wow:.1f}% s/s)",fontsize=8,color=TEXT,pad=3,fontweight="bold")
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
            nm=NM_PROD.get(sym,data.get("name",sym))
            items.append((nm,sym,float(nc_net),float(cm_net) if cm_net else 0))
    if not items: return None
    items.sort(key=lambda x:abs(x[2]),reverse=True); items=items[:14]
    fig,ax=plt.subplots(figsize=(11,max(len(items)*0.55,4)))
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)
    y=list(range(len(items)))
    nc=[it[2] for it in items]; cm=[it[3] for it in items]
    ax.barh(y,nc,color=[GREEN if v>=0 else RED for v in nc],alpha=0.7,height=0.35,zorder=3)
    ax.barh([yi+0.35 for yi in y],cm,color=[BLUE if v>=0 else AMBER for v in cm],alpha=0.5,height=0.35,zorder=3)
    for i,(nm,sym,ncv,cmv) in enumerate(items):
        off=max(abs(ncv)*0.02,800); xt=ncv+off if ncv>=0 else ncv-off
        ax.text(xt,i,f"{ncv:,.0f}",va="center",ha="left" if ncv>=0 else "right",fontsize=7,color=TEXT,fontweight="bold")
    ax.set_yticks([yi+0.17 for yi in y]); ax.set_yticklabels([it[0] for it in items],fontsize=7.5)
    ax.axvline(0,color=BORDER,lw=0.8); ax.grid(True,axis="x",alpha=0.1); ax.invert_yaxis()
    legend_patches = [
        mpatches.Patch(color=GREEN, alpha=0.7, label="Fundos COMPRADOS (apostam na alta)"),
        mpatches.Patch(color=RED, alpha=0.7, label="Fundos VENDIDOS (apostam na queda)"),
        mpatches.Patch(color=BLUE, alpha=0.5, label="Produtores/Industria COMPRADOS (protegendo compra)"),
        mpatches.Patch(color=AMBER, alpha=0.5, label="Produtores/Industria VENDIDOS (protegendo venda)"),
    ]
    ax.legend(handles=legend_patches, fontsize=7.5, loc="lower right", framealpha=0.5, fancybox=True, edgecolor=BORDER)
    for sp in ax.spines.values(): sp.set_visible(False)
    ax.set_title("Posicao dos Fundos e Produtores nos Futuros", fontsize=11, color=TEXT, fontweight="bold", pad=8)
    fig.subplots_adjust(left=0.14,right=0.96,top=0.93,bottom=0.04)
    return fig2img(fig)


def chart_stocks(sw):
    comms=sw.get("commodities",{})
    if not comms: return None
    items=[]
    for sym,d in comms.items():
        cur_raw = d.get("stock_current")
        avg_raw = d.get("stock_avg")
        if cur_raw is not None and avg_raw is not None:
            try:
                cur_f = float(cur_raw); avg_f = float(avg_raw)
            except (ValueError, TypeError):
                continue
            if avg_f == 0: continue
            # CALCULA desvio REAL de estoque (nao usa price_vs_avg!)
            stock_dev = ((cur_f - avg_f) / avg_f) * 100
            # Flag outliers extremos (>50% pode ser erro de unidade)
            is_suspect = abs(stock_dev) > 50
            items.append((NM_PROD.get(sym,sym), sym, cur_f, avg_f, d.get("state",""), stock_dev, is_suspect))
    if not items: return None
    items.sort(key=lambda x:abs(x[5]),reverse=True)
    fig,ax=plt.subplots(figsize=(11,max(len(items)*0.55,3.5)))
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)
    y_pos=list(range(len(items)))
    devs=[it[5] for it in items]
    colors=[]
    for it in items:
        dev = it[5]; suspect = it[6]
        if suspect:
            colors.append("#666666")  # Cinza = dado suspeito
        elif dev>15: colors.append(RED)
        elif dev>5: colors.append(AMBER)
        elif dev<-15: colors.append(CYAN)
        elif dev<-5: colors.append(BLUE)
        else: colors.append(TEXT_MUT)
    # Limita barras extremas para nao comprimir o grafico
    devs_capped = [max(min(d, 60), -60) for d in devs]
    ax.barh(y_pos, devs_capped, color=colors, alpha=0.7, height=0.6, zorder=3)
    for i, it in enumerate(items):
        nm, sym, cur, avg, st, dev, suspect = it
        off=max(abs(devs_capped[i])*0.05,1.5)
        xt=devs_capped[i]+off if devs_capped[i]>=0 else devs_capped[i]-off
        label = f"{dev:+.1f}%"
        if suspect:
            label += " VERIFICAR"
        ax.text(xt,i,label,va="center",ha="left" if devs_capped[i]>=0 else "right",
                fontsize=7,color="#ff6666" if suspect else TEXT,fontweight="bold")
    ax.set_yticks(y_pos); ax.set_yticklabels([it[0] for it in items],fontsize=7.5)
    ax.axvline(0,color=BORDER,lw=0.8); ax.grid(True,axis="x",alpha=0.1); ax.invert_yaxis()
    ax.set_xlabel("Estoque vs Media 5 Anos (%)",fontsize=7.5,color=TEXT_MUT)
    ax.set_xlim(-70, 70)
    # Nota de rodape sobre dados suspeitos
    suspects = [it[0] for it in items if it[6]]
    if suspects:
        ax.text(0.5, -0.06, f"ATENCAO: {', '.join(suspects)} com desvio >50% — possivel erro de unidade/serie. Verificar fonte.",
                transform=ax.transAxes, ha="center", fontsize=7, color="#ff6666", fontstyle="italic")
    for sp in ax.spines.values(): sp.set_visible(False)
    fig.subplots_adjust(left=0.18,right=0.96,top=0.96,bottom=0.10)
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
    ax.set_title(f"Dolar: R$ {vals[-1]:.4f}",fontsize=10,color=TEXT,fontweight="bold",pad=3)
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
        label=d.get("label","").replace("\U0001f1e7\U0001f1f7 ","").split(" — ")[-1]
        price=d.get("price",""); unit=d.get("price_unit",""); trend=d.get("trend","")
        sym_base = key.replace("_BR","")
        nm_pt = NM_PROD.get(sym_base, sym_base)
        ax.set_title(f"BRASIL: {nm_pt} - {price} {unit} ({trend})",fontsize=7.5,color=TEXT,pad=2,fontweight="bold")
        ax.tick_params(labelbottom=False,labelsize=5.5); ax.grid(True,alpha=0.1)
        for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.4)
    for idx in range(len(br_keys),rows*cols):
        r,c2=divmod(idx,cols); axes[r][c2].set_visible(False)
    fig.subplots_adjust(hspace=0.55,wspace=0.22,left=0.05,right=0.98,top=0.92,bottom=0.03)
    return fig2img(fig)



def chart_sugar_alcohol(pr, ed):
    """Grafico comparativo Acucar vs Etanol para pagina dedicada"""
    fig, axes = plt.subplots(1, 3, figsize=(11.5, 3.5))
    fig.patch.set_facecolor(BG)
    # 1. Acucar #11 (Chicago)
    ax = axes[0]; ax.set_facecolor(PANEL)
    v = closes("SB", pr, 60)
    if len(v) > 3:
        x = list(range(len(v)))
        ax.plot(x, v, color=TEAL, lw=1.5, zorder=3)
        ax.fill_between(x, min(v)*0.998, v, alpha=0.12, color=TEAL)
        ax.scatter([x[-1]], [v[-1]], color=TEAL, s=25, zorder=5, edgecolors="white", linewidths=0.5)
        chg = ((v[-1]-v[0])/v[0])*100 if v[0]!=0 else 0
        cc = GREEN if chg >= 0 else RED; ss = "+" if chg >= 0 else ""
        ax.text(0.03, 0.90, f"{v[-1]:,.2f}", transform=ax.transAxes, fontsize=10, color=TEXT, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.2", facecolor=PANEL, edgecolor=BORDER, alpha=0.8))
        ax.text(0.97, 0.90, f"{ss}{chg:.1f}%", transform=ax.transAxes, ha="right", fontsize=9, color=cc, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.2", facecolor="#0f1117", edgecolor=cc, alpha=0.7, lw=0.4))
    ax.set_title("Acucar (CHICAGO) - c/lb", fontsize=9, color=TEXT, fontweight="bold", pad=3)
    ax.tick_params(labelbottom=False, labelsize=6); ax.grid(True, alpha=0.12)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.4)
    # 2. Etanol producao EUA
    ax = axes[1]; ax.set_facecolor(PANEL)
    series = ed.get("series",{})
    eth = series.get("ethanol_production", {})
    eth_hist = eth.get("history", [])
    eth_vals = [h["value"] for h in eth_hist if h.get("value") is not None]
    if len(eth_vals) > 3:
        vp = eth_vals[-40:]; x = list(range(len(vp)))
        ax.plot(x, vp, color=GREEN, lw=1.5, zorder=3)
        ax.fill_between(x, min(vp)*0.998, vp, alpha=0.12, color=GREEN)
        ax.scatter([x[-1]], [vp[-1]], color=GREEN, s=25, zorder=5, edgecolors="white", linewidths=0.5)
        ax.text(0.03, 0.90, f"{vp[-1]:,.0f}", transform=ax.transAxes, fontsize=10, color=TEXT, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.2", facecolor=PANEL, edgecolor=BORDER, alpha=0.8))
    ax.set_title("Producao Etanol EUA (mil bbl/dia)", fontsize=9, color=TEXT, fontweight="bold", pad=3)
    ax.tick_params(labelbottom=False, labelsize=6); ax.grid(True, alpha=0.12)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.4)
    # 3. Estoques Etanol EUA
    ax = axes[2]; ax.set_facecolor(PANEL)
    eth_stk = series.get("ethanol_stocks", {})
    eth_stk_hist = eth_stk.get("history", [])
    eth_stk_vals = [h["value"] for h in eth_stk_hist if h.get("value") is not None]
    if len(eth_stk_vals) > 3:
        vp = eth_stk_vals[-40:]; x = list(range(len(vp)))
        ax.plot(x, vp, color=AMBER, lw=1.5, zorder=3)
        ax.fill_between(x, min(vp)*0.998, vp, alpha=0.12, color=AMBER)
        ax.scatter([x[-1]], [vp[-1]], color=AMBER, s=25, zorder=5, edgecolors="white", linewidths=0.5)
        ax.text(0.03, 0.90, f"{vp[-1]:,.0f}", transform=ax.transAxes, fontsize=10, color=TEXT, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.2", facecolor=PANEL, edgecolor=BORDER, alpha=0.8))
    ax.set_title("Estoques Etanol EUA (mil bbl)", fontsize=9, color=TEXT, fontweight="bold", pad=3)
    ax.tick_params(labelbottom=False, labelsize=6); ax.grid(True, alpha=0.12)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.4)
    fig.subplots_adjust(wspace=0.25, left=0.04, right=0.98, top=0.88, bottom=0.05)
    return fig2img(fig)



def chart_arbitrage_bars(arbs, fx):
    """Grafico de barras: BR vs Chicago convertido em R$"""
    if not arbs: return None
    items = list(arbs.items())
    n = len(items)
    fig, ax = plt.subplots(figsize=(11, max(n * 1.6, 4)))
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)
    y_pos = list(range(n))
    bar_h = 0.35
    br_vals = [arbs[k]["br_price"] for k, _ in items]
    chi_vals = [arbs[k]["chi_price_brl"] for k, _ in items]
    ax.barh([y - bar_h/2 for y in y_pos], br_vals, bar_h, color=GREEN, alpha=0.8, label="Brasil (fisico)", zorder=3)
    ax.barh([y + bar_h/2 for y in y_pos], chi_vals, bar_h, color=BLUE, alpha=0.8, label="Chicago (convertido R$)", zorder=3)
    for i, (sym, data) in enumerate(items):
        sp = data["spread_pct"]
        sc = GREEN if sp > 0 else RED
        txt = f"{sp:+.1f}%"
        max_val = max(data["br_price"], data["chi_price_brl"])
        ax.text(max_val * 1.02, i, txt, va="center", fontsize=11, color=sc, fontweight="bold")
    names = [arbs[k]["name"] for k, _ in items]
    ax.set_yticks(y_pos); ax.set_yticklabels(names, fontsize=11, fontweight="bold")
    ax.legend(fontsize=9, loc="lower right", framealpha=0.5, fancybox=True, edgecolor=BORDER)
    ax.set_xlabel(f"Preco em R$ (cambio: R$ {fx:.2f})", fontsize=9, color=TEXT_MUT)
    ax.grid(True, axis="x", alpha=0.1); ax.invert_yaxis()
    for sp in ax.spines.values(): sp.set_visible(False)
    fig.subplots_adjust(left=0.14, right=0.88, top=0.96, bottom=0.08)
    return fig2img(fig)


def chart_spreads_grid(sd, extra_spreads):
    """Grade de spreads mais legivel que o termometro"""
    all_sp = {}
    spreads = sd.get("spreads", {})
    for k, v in spreads.items():
        all_sp[k] = v
    for k, v in extra_spreads.items():
        if k not in all_sp:
            all_sp[k] = v
    if not all_sp: return None
    items = list(all_sp.items())
    n = len(items)
    cols = 3; rows = math.ceil(n / cols)
    fig, axes = plt.subplots(rows, cols, figsize=(11.5, rows * 2.0))
    fig.patch.set_facecolor(BG)
    if rows == 1: axes = [axes]
    for idx, (k, sp) in enumerate(items):
        r, c2 = divmod(idx, cols)
        ax = axes[r][c2] if rows > 1 else axes[c2]
        ax.set_facecolor(PANEL)
        pctl = sp.get("percentile", 50)
        nm = SPR_NM.get(k, sp.get("name", k))
        cur = sp.get("current", "")
        un = sp.get("unit", "")
        # Barra horizontal de percentil
        bar_colors = [GREEN, "#86efac", AMBER, "#fb923c", RED]
        for zi in range(5):
            ax.barh(0, 20, left=zi*20, height=0.6, color=bar_colors[zi], alpha=0.35)
        mc = GREEN if pctl < 30 else (AMBER if pctl < 70 else RED)
        ax.plot(pctl, 0.55, "v", color=mc, ms=14, zorder=5)
        ax.text(pctl, 0.95, f"{pctl:.0f}", ha="center", va="bottom", fontsize=9, color=mc, fontweight="bold")
        if cur:
            cur_txt = f"{num_fmt(cur)} {un}" if isinstance(cur, (int, float)) else f"{cur} {un}"
            ax.text(50, -0.55, cur_txt, ha="center", va="top", fontsize=8, color=TEXT_MUT)
        ax.set_xlim(-2, 102); ax.set_ylim(-0.8, 1.3)
        ax.set_title(nm, fontsize=9, color=TEXT, fontweight="bold", pad=2)
        ax.axis("off")
    # Hide unused
    for idx in range(n, rows * cols):
        r, c2 = divmod(idx, cols)
        (axes[r][c2] if rows > 1 else axes[c2]).set_visible(False)
    fig.suptitle("TERMOMETRO DE RELACOES — Nivel 0-100 (verde=barato, vermelho=caro)", fontsize=11, color=TEXT, fontweight="bold", y=0.99)
    fig.subplots_adjust(hspace=0.9, wspace=0.2, left=0.03, right=0.97, top=0.92, bottom=0.02)
    return fig2img(fig)


def chart_cattle_compare(pr):
    """Grafico comparativo Boi Gordo vs Boi Engorda"""
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.5))
    fig.patch.set_facecolor(BG)
    for idx, (sym, title, color) in enumerate([("LE", "Boi Gordo (Chicago) - c/lb", AMBER), ("GF", "Boi de Engorda (Chicago) - c/lb", CYAN)]):
        ax = axes[idx]; ax.set_facecolor(PANEL)
        v = closes(sym, pr, 60)
        if len(v) > 3:
            x = list(range(len(v)))
            ax.plot(x, v, color=color, lw=1.8, zorder=3)
            ax.fill_between(x, min(v)*0.998, v, alpha=0.12, color=color)
            ax.scatter([x[-1]], [v[-1]], color=color, s=30, zorder=5, edgecolors="white", linewidths=0.6)
            if len(v) >= 20:
                ma20 = [np.mean(v[max(0,i-19):i+1]) for i in range(len(v))]
                ax.plot(x, ma20, color=AMBER if sym == "GF" else CYAN, lw=0.8, ls="--", alpha=0.6)
            ax.text(0.03, 0.90, f"{v[-1]:,.2f}", transform=ax.transAxes, fontsize=12, color=TEXT, fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.2", facecolor=PANEL, edgecolor=BORDER, alpha=0.8))
            chg = ((v[-1]-v[0])/v[0])*100 if v[0]!=0 else 0
            cc = GREEN if chg >= 0 else RED; ss = "+" if chg >= 0 else ""
            ax.text(0.97, 0.90, f"{ss}{chg:.1f}%", transform=ax.transAxes, ha="right", fontsize=9, color=cc, fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.2", facecolor="#0f1117", edgecolor=cc, alpha=0.7, lw=0.4))
        ax.set_title(title, fontsize=10, color=TEXT, fontweight="bold", pad=3)
        ax.tick_params(labelbottom=False, labelsize=6); ax.grid(True, alpha=0.12)
        for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.4)
    fig.subplots_adjust(wspace=0.2, left=0.05, right=0.98, top=0.88, bottom=0.05)
    return fig2img(fig)



def chart_energy_sugar_cross(pr, ed):
    """Graficos cruzados: Petroleo vs Acucar, Etanol prod vs estoques, Diesel"""
    fig, axes = plt.subplots(2, 3, figsize=(11.5, 5.5))
    fig.patch.set_facecolor(BG)
    # 1. Petroleo WTI 60d
    ax = axes[0][0]; ax.set_facecolor(PANEL)
    v = closes("CL", pr, 60)
    if len(v) > 3:
        x = list(range(len(v)))
        ax.plot(x, v, color=AMBER, lw=1.5, zorder=3)
        ax.fill_between(x, min(v)*0.998, v, alpha=0.12, color=AMBER)
        ax.scatter([x[-1]], [v[-1]], color=AMBER, s=20, zorder=5, edgecolors="white", linewidths=0.4)
        ax.text(0.03, 0.88, f"${v[-1]:,.1f}", transform=ax.transAxes, fontsize=10, color=TEXT, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.15", facecolor=PANEL, edgecolor=BORDER, alpha=0.8))
    ax.set_title("Petroleo WTI ($/bbl)", fontsize=8.5, color=TEXT, fontweight="bold", pad=2)
    ax.tick_params(labelbottom=False, labelsize=5.5); ax.grid(True, alpha=0.12)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.3)
    # 2. Acucar #11 60d
    ax = axes[0][1]; ax.set_facecolor(PANEL)
    v = closes("SB", pr, 60)
    if len(v) > 3:
        x = list(range(len(v)))
        ax.plot(x, v, color=TEAL, lw=1.5, zorder=3)
        ax.fill_between(x, min(v)*0.998, v, alpha=0.12, color=TEAL)
        ax.scatter([x[-1]], [v[-1]], color=TEAL, s=20, zorder=5, edgecolors="white", linewidths=0.4)
        ax.text(0.03, 0.88, f"{v[-1]:,.2f}", transform=ax.transAxes, fontsize=10, color=TEXT, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.15", facecolor=PANEL, edgecolor=BORDER, alpha=0.8))
    ax.set_title("Acucar #11 (c/lb)", fontsize=8.5, color=TEXT, fontweight="bold", pad=2)
    ax.tick_params(labelbottom=False, labelsize=5.5); ax.grid(True, alpha=0.12)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.3)
    # 3. Milho 60d (insumo etanol)
    ax = axes[0][2]; ax.set_facecolor(PANEL)
    v = closes("ZC", pr, 60)
    if len(v) > 3:
        x = list(range(len(v)))
        ax.plot(x, v, color=GREEN, lw=1.5, zorder=3)
        ax.fill_between(x, min(v)*0.998, v, alpha=0.12, color=GREEN)
        ax.scatter([x[-1]], [v[-1]], color=GREEN, s=20, zorder=5, edgecolors="white", linewidths=0.4)
        ax.text(0.03, 0.88, f"{v[-1]:,.1f}", transform=ax.transAxes, fontsize=10, color=TEXT, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.15", facecolor=PANEL, edgecolor=BORDER, alpha=0.8))
    ax.set_title("Milho (c/bu) - insumo etanol", fontsize=8.5, color=TEXT, fontweight="bold", pad=2)
    ax.tick_params(labelbottom=False, labelsize=5.5); ax.grid(True, alpha=0.12)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.3)
    # 4. Gas Natural 60d (custo fertilizante)
    ax = axes[1][0]; ax.set_facecolor(PANEL)
    v = closes("NG", pr, 60)
    if len(v) > 3:
        x = list(range(len(v)))
        ax.plot(x, v, color=RED, lw=1.5, zorder=3)
        ax.fill_between(x, min(v)*0.998, v, alpha=0.12, color=RED)
        ax.scatter([x[-1]], [v[-1]], color=RED, s=20, zorder=5, edgecolors="white", linewidths=0.4)
        ax.text(0.03, 0.88, f"${v[-1]:,.2f}", transform=ax.transAxes, fontsize=10, color=TEXT, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.15", facecolor=PANEL, edgecolor=BORDER, alpha=0.8))
    ax.set_title("Gas Natural ($/MMBtu) - adubo", fontsize=8.5, color=TEXT, fontweight="bold", pad=2)
    ax.tick_params(labelbottom=False, labelsize=5.5); ax.grid(True, alpha=0.12)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.3)
    # 5. Etanol producao EUA (EIA)
    ax = axes[1][1]; ax.set_facecolor(PANEL)
    series = ed.get("series", {})
    eth = series.get("ethanol_production", {})
    eth_hist = eth.get("history", [])
    eth_vals = [h["value"] for h in eth_hist if h.get("value") is not None]
    if len(eth_vals) > 3:
        vp = eth_vals[-40:]; x = list(range(len(vp)))
        ax.plot(x, vp, color=CYAN, lw=1.5, zorder=3)
        ax.fill_between(x, min(vp)*0.998, vp, alpha=0.12, color=CYAN)
        ax.scatter([x[-1]], [vp[-1]], color=CYAN, s=20, zorder=5, edgecolors="white", linewidths=0.4)
        ax.text(0.03, 0.88, f"{vp[-1]:,.0f}", transform=ax.transAxes, fontsize=10, color=TEXT, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.15", facecolor=PANEL, edgecolor=BORDER, alpha=0.8))
    ax.set_title("Etanol EUA producao (MBbl/d)", fontsize=8.5, color=TEXT, fontweight="bold", pad=2)
    ax.tick_params(labelbottom=False, labelsize=5.5); ax.grid(True, alpha=0.12)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.3)
    # 6. Diesel retail EUA (EIA)
    ax = axes[1][2]; ax.set_facecolor(PANEL)
    diesel = series.get("diesel_retail", {})
    diesel_hist = diesel.get("history", [])
    diesel_vals = [h["value"] for h in diesel_hist if h.get("value") is not None]
    if len(diesel_vals) > 3:
        vp = diesel_vals[-40:]; x = list(range(len(vp)))
        ax.plot(x, vp, color=PURPLE, lw=1.5, zorder=3)
        ax.fill_between(x, min(vp)*0.998, vp, alpha=0.12, color=PURPLE)
        ax.scatter([x[-1]], [vp[-1]], color=PURPLE, s=20, zorder=5, edgecolors="white", linewidths=0.4)
        ax.text(0.03, 0.88, f"${vp[-1]:,.3f}", transform=ax.transAxes, fontsize=10, color=TEXT, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.15", facecolor=PANEL, edgecolor=BORDER, alpha=0.8))
    ax.set_title("Diesel EUA retail ($/gal)", fontsize=8.5, color=TEXT, fontweight="bold", pad=2)
    ax.tick_params(labelbottom=False, labelsize=5.5); ax.grid(True, alpha=0.12)
    for sp in ax.spines.values(): sp.set_color(BORDER); sp.set_linewidth(0.3)
    fig.subplots_adjust(hspace=0.45, wspace=0.22, left=0.04, right=0.98, top=0.94, bottom=0.03)
    return fig2img(fig)

# ═══════════════════════════════════════════
#  PAGE FUNCTIONS
# ═══════════════════════════════════════════

# ── PAGE 1: CAPA + RESUMO ──
def pg_cover(pdf, rd, dr, bcb, pr=None):
    dbg(pdf)
    titulo_raw = rd.get("titulo","Relatorio AgriMacro")
    titulo = sanitize_language(titulo_raw, pr) if pr else titulo_raw
    subtitulo = rd.get("subtitulo","")
    pdf.setFillColor(HexColor(PURPLE)); pdf.rect(0,PAGE_H-130,PAGE_W,130,fill=1,stroke=0)
    pdf.setFillColor(HexColor("#fff")); pdf.setFont("Helvetica-Bold",9)
    pdf.drawString(M,PAGE_H-22,f"AGRIMACRO v3.2 | RELATORIO DIARIO | {TODAY_BR} ({WDAY.upper()})")
    pdf.setFont("Helvetica-Bold",22)
    pdf.drawString(M,PAGE_H-58,titulo[:70])
    if len(titulo)>70: pdf.setFont("Helvetica-Bold",14); pdf.drawString(M,PAGE_H-78,titulo[70:140])
    pdf.setFillColor(HexColor("#d1d5db")); pdf.setFont("Helvetica",11)
    pdf.drawString(M,PAGE_H-108,subtitulo[:100])
    rc=bcb.get("resumo_cambio",{}); rj=bcb.get("resumo_juros",{})
    brl=rc.get("brl_usd_atual"); selic=rj.get("selic_atual")
    rx = PAGE_W-M
    pdf.setFillColor(HexColor("#ffffff80")); pdf.setFont("Helvetica-Bold",10)
    if brl: pdf.drawRightString(rx,PAGE_H-40,f"Dolar R$ {brl:.2f}")
    if selic: pdf.drawRightString(rx,PAGE_H-55,f"Selic {selic:.1f}%")
    var30=rc.get("var_30d")
    if var30: pdf.setFont("Helvetica",8); pdf.drawRightString(rx,PAGE_H-68,f"Dolar 30d: {var30:+.1f}%")
    y = PAGE_H-150
    resumo_raw = rd.get("resumo_executivo","")
    resumo = sanitize_language(resumo_raw, pr) if pr else resumo_raw
    if resumo:
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",11)
        pdf.drawString(M,y,"RESUMO DO DIA"); y-=14
        y = tblock(pdf,M,y,resumo,sz=9,mw=PAGE_W-2*M,ld=13); y-=8

    # 4 Perguntas com explicacao
    perguntas = dr.get("perguntas",[])
    explicacoes = dr.get("explicacoes_perguntas",[])  # novo campo esperado
    if perguntas:
        pdf.setFillColor(HexColor(CYAN)); pdf.setFont("Helvetica-Bold",10)
        pdf.drawString(M,y,"4 PERGUNTAS PARA PENSAR HOJE"); y-=5
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",7)
        pdf.drawString(M,y,"Cada pergunta tem um motivo. Entenda o raciocinio por tras:"); y-=12
        for i, p in enumerate(perguntas[:4]):
            p_clean = replace_tickers(p)
            exp = explicacoes[i] if i < len(explicacoes) else ""
            exp_clean = replace_tickers(exp) if exp else ""
            p_lines = max(1, len(p_clean) // 95 + 1)
            e_lines = max(1, len(exp_clean) // 95 + 1) if exp_clean else 0
            h_box = 14 + p_lines * 12 + e_lines * 10 + 4
            h_box = max(h_box, 28)
            panel(pdf,M,y-h_box,PAGE_W-2*M,h_box,bc=CYAN)
            ty = y - 12
            ty = tblock(pdf,M+8,ty,p_clean[:200],font="Helvetica-Bold",sz=8.5,clr=TEXT,mw=PAGE_W-2*M-20,ld=12)
            if exp_clean:
                ty -= 2
                tblock(pdf,M+8,ty,f"Por que importa: {exp_clean[:200]}",sz=7,clr=AMBER,mw=PAGE_W-2*M-20,ld=9)
            y -= h_box + 3

    # Destaques
    dests = rd.get("destaques",[])
    if dests and y>80:
        y-=5
        pdf.setFillColor(HexColor(GREEN)); pdf.setFont("Helvetica-Bold",10)
        pdf.drawString(M,y,"O QUE FAZER HOJE"); y-=14
        for d in dests[:3]:
            if y<45: break
            t=d.get("titulo",""); cm=d.get("commodity",""); imp=d.get("impacto_produtor","")
            t_clean = sanitize_language(replace_tickers(t), pr) if pr else replace_tickers(t)
            imp_clean = replace_tickers(imp) if imp else ""
            t_lines = max(1, len(t_clean) // 85 + 1)
            i_lines = max(1, len(imp_clean) // 90 + 1) if imp_clean else 0
            h_box = 10 + t_lines * 12 + i_lines * 10 + 6
            h_box = max(h_box, 32)
            panel(pdf,M,y-h_box,PAGE_W-2*M,h_box,bc=GREEN)
            ty = y - 12
            ty = tblock(pdf,M+8,ty,f"[{NM_PROD.get(cm,cm)}] {t_clean[:120]}",font="Helvetica-Bold",sz=8.5,clr=TEXT,mw=PAGE_W-2*M-20,ld=12)
            if imp_clean:
                ty -= 2
                tblock(pdf,M+8,ty,imp_clean[:250],sz=7.5,clr=GREEN,mw=PAGE_W-2*M-20,ld=10)
            y -= h_box + 3


# ── PAGE 2: MACRO BRASIL ──
def pg_macro(pdf, bcb, img_brl):
    dbg(pdf); hdr(pdf,"Cenario Macro Brasil","Dolar, Juros e Inflacao — o que mexe com o preco da sua safra",
                  "Dolar forte = mais reais por saca exportada, mas insumo importado fica mais caro. Selic alta = custo de estocagem sobe.")
    y=PAGE_H-75
    rc=bcb.get("resumo_cambio",{}); rj=bcb.get("resumo_juros",{})
    cw=(PAGE_W-2*M-30)/4
    cards=[
        ("Dolar Hoje",f"R$ {rc.get('brl_usd_atual',0):.4f}",f"5d: {rc.get('var_5d',0):+.2f}%  |  30d: {rc.get('var_30d',0):+.1f}%",AMBER),
        ("Selic",f"{rj.get('selic_atual',0):.1f}%","Taxa basica de juros",RED),
        ("Dolar Minima Ano",f"R$ {rc.get('min_52s',0):.4f}","Menor valor em 52 semanas",GREEN),
        ("Dolar Maxima Ano",f"R$ {rc.get('max_52s',0):.4f}","Maior valor em 52 semanas",RED),
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
    pdf.drawString(rx,y-10,"COMO ISSO AFETA SEU BOLSO?"); ry=y-30
    impacts=[
        ("Dolar forte","Receita em reais sobe para quem exporta soja, cafe, acucar. Mas adubo e defensivo importado ficam mais caros.",AMBER),
        ("Selic alta","Guardar grao no silo custa mais. Capital de giro fica caro. Produtor tende a vender mais rapido.",RED),
        ("Inflacao (IPCA)","Se alimentos sobem no mercado interno, governo pode intervir zerando tarifas de importacao. Afeta sua margem.",PINK),
    ]
    for title,desc,clr in impacts:
        panel(pdf,rx,ry-44,PAGE_W/2-M-15,46,bc=clr)
        pdf.setFillColor(HexColor(clr)); pdf.setFont("Helvetica-Bold",8.5); pdf.drawString(rx+10,ry-14,title)
        tblock(pdf,rx+10,ry-28,desc,sz=7.5,clr=TEXT_MUT,mw=PAGE_W/2-M-40,ld=10)
        ry-=54
    ipca=bcb.get("ipca_mensal",[])
    if ipca and len(ipca)>3:
        ry-=5
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",8.5)
        pdf.drawString(rx,ry,"Inflacao Mensal (ultimos 6 meses)"); ry-=13
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


# ── PAGES 3-6: DEDICATED COMMODITY PAGES ──
def pg_commodity(pdf, sym, title, subtitle, accent, pr, cd, sw, phys, rd, dr, img_main, img_season):
    """Dedicated page for a major commodity"""
    dbg(pdf)
    # Custom header with accent color
    pdf.setFillColor(HexColor(accent)); pdf.rect(0,PAGE_H-34,PAGE_W,34,fill=1,stroke=0)
    pdf.setFillColor(HexColor("#fff")); pdf.setFont("Helvetica-Bold",15)
    pdf.drawString(M,PAGE_H-24,f"{title.upper()}")
    pdf.setFillColor(HexColor("#ffffffcc")); pdf.setFont("Helvetica",9)
    pdf.drawString(M+pdf.stringWidth(f"{title.upper()} ","Helvetica-Bold",15)+10,PAGE_H-24,f"— {subtitle}")
    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7.5)
    pdf.drawRightString(PAGE_W-M,PAGE_H-15,f"AgriMacro v3.2 | {TODAY_BR} ({WDAY})")

    # ── LEFT SIDE: Main chart + Seasonality ──
    chart_w = PAGE_W * 0.58
    if img_main:
        pdf.drawImage(img_main, M, PAGE_H-275, width=chart_w-M-10, height=230, preserveAspectRatio=True, mask="auto")
    if img_season:
        pdf.drawImage(img_season, M, PAGE_H-480, width=chart_w*0.55, height=190, preserveAspectRatio=True, mask="auto")

    # ── RIGHT SIDE: Stats + COT + Physical + Interpretation ──
    rx = chart_w + 5
    rw = PAGE_W - rx - M
    y = PAGE_H - 50

    # --- Price Stats Panel ---
    lc = last_close(sym, pr)
    c1d = pchg(sym, pr, 1); c1w = pchg(sym, pr, 5); c1m = pchg(sym, pr, 21)
    h52 = hi52(sym, pr); l52 = lo52(sym, pr); hp = hilo_pct(sym, pr)

    panel(pdf, rx, y-80, rw, 82, bc=accent)
    is_br_commodity = False  # LE e GF sao contratos CME Chicago, NAO B3
    origin_badge(pdf, rx+10, y-10, is_brazil=is_br_commodity)
    origin_label = "PRECO FUTURO - B3 (Brasil)" if is_br_commodity else "PRECO FUTURO - CHICAGO (EUA)"
    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7); pdf.drawString(rx+65, y-10, origin_label)
    if lc:
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",22)
        pdf.drawString(rx+10, y-36, f"{lc:,.2f}")
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",7)
        pdf.drawString(rx+10, y-48, UNITS.get(sym,""))
    if c1d is not None:
        cc=GREEN if c1d>=0 else RED
        pdf.setFillColor(HexColor(cc)); pdf.setFont("Helvetica-Bold",9)
        pdf.drawString(rx+rw-90, y-14, f"Dia: {chg_str(c1d)}")
    if c1w is not None:
        cc=GREEN if c1w>=0 else RED
        pdf.setFillColor(HexColor(cc)); pdf.setFont("Helvetica-Bold",9)
        pdf.drawString(rx+rw-90, y-28, f"Sem: {chg_str(c1w)}")
    if c1m is not None:
        cc=GREEN if c1m>=0 else RED
        pdf.setFillColor(HexColor(cc)); pdf.setFont("Helvetica-Bold",9)
        pdf.drawString(rx+rw-90, y-42, f"Mes: {chg_str(c1m)}")
    # 52w range bar
    if hp is not None and h52 and l52:
        bar_y = y-65; bar_w = rw-20
        pdf.setFillColor(HexColor(BORDER)); pdf.rect(rx+10, bar_y, bar_w, 6, fill=1, stroke=0)
        # Fill bar to current position
        fill_w = bar_w * (hp/100)
        bar_clr = GREEN if hp > 70 else (RED if hp < 30 else AMBER)
        pdf.setFillColor(HexColor(bar_clr)); pdf.rect(rx+10, bar_y, fill_w, 6, fill=1, stroke=0)
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6)
        pdf.drawString(rx+10, bar_y-9, f"Min ano: {l52:,.1f}")
        pdf.drawRightString(rx+10+bar_w, bar_y-9, f"Max ano: {h52:,.1f}")
        pdf.setFillColor(HexColor(bar_clr)); pdf.setFont("Helvetica-Bold",6.5)
        pdf.drawCentredString(rx+10+fill_w, bar_y+8, f"{hp:.0f}%")
    y -= 90

    # --- Fund Position (COT) ---
    comms = cd.get("commodities",{})
    cot_data = comms.get(sym, {})
    if cot_data:
        leg = cot_data.get("legacy",{}).get("latest",{})
        nc_net = leg.get("noncomm_net")
        if nc_net is not None:
            cm_net = leg.get("comm_net")
            panel(pdf, rx, y-52, rw, 54, bc=PURPLE)
            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7)
            pdf.drawString(rx+10, y-10, "POSICAO DOS FUNDOS (grandes especuladores)")
            pdf.setFillColor(HexColor(GREEN if nc_net > 0 else RED)); pdf.setFont("Helvetica-Bold",14)
            direction = "COMPRADOS" if nc_net > 0 else "VENDIDOS"
            pdf.drawString(rx+10, y-28, f"{direction}: {abs(nc_net):,.0f} contratos")
            if cm_net is not None:
                cm_dir = "comprados" if cm_net > 0 else "vendidos"
                cm_clr = BLUE if cm_net > 0 else AMBER
                pdf.setFillColor(HexColor(cm_clr)); pdf.setFont("Helvetica",8)
                pdf.drawString(rx+10, y-42, f"Produtores/Industria: {cm_dir} {abs(cm_net):,.0f} contratos")
            y -= 58

    # --- Stocks/Fundamentals ---
    sw_comms = sw.get("commodities",{})
    sw_data = sw_comms.get(sym, {})
    if sw_data and sw_data.get("price_vs_avg") is not None:
        pva = sw_data["price_vs_avg"]
        state = sw_data.get("state","")
        state_nm = STATE_PT.get(state, state)
        sc = STATE_CLR.get(state, TEXT_MUT)
        panel(pdf, rx, y-34, rw, 36, bc=sc)
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7)
        pdf.drawString(rx+10, y-10, "ESTOQUES vs MEDIA 5 ANOS (USDA)")
        pdf.setFillColor(HexColor(sc)); pdf.setFont("Helvetica-Bold",11)
        pdf.drawString(rx+10, y-26, f"{pva:+.1f}% — {state_nm}")
        y -= 40

    # --- Physical Market BR ---
    phys_key = PHYS_MAP.get(sym)
    intl = phys.get("international",{})
    if phys_key and phys_key in intl:
        d = intl[phys_key]
        price = d.get("price",""); unit = d.get("price_unit",""); trend = d.get("trend","")
        if price and str(price) != "None":
            panel(pdf, rx, y-34, rw, 36, bc=TEAL)
            origin_badge(pdf, rx+10, y-10, is_brazil=True)
            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7)
            pdf.drawString(rx+55, y-10, "PRECO FISICO - mercado a vista no Brasil")
            pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",14)
            pdf.drawString(rx+10, y-28, f"{price} {unit}")
            tc = GREEN if str(trend).startswith("+") else (RED if str(trend).startswith("-") else TEXT_MUT)
            pdf.setFillColor(HexColor(tc)); pdf.setFont("Helvetica-Bold",9)
            pdf.drawString(rx+rw-50, y-28, str(trend))
            y -= 40

    # --- Producer Interpretation ---
    dests_list = []
    if dr and dr.get("destaques"): dests_list.extend(dr["destaques"])
    if rd and rd.get("destaques"): dests_list.extend(rd["destaques"])
    for d in dests_list:
        cm = d.get("commodity","")
        if cm == sym:
            imp = d.get("impacto_produtor","")
            if imp:
                imp = replace_tickers(imp)
                lines_est = max(3, len(imp) // 45 + 1)
                box_h = min(90, 18 + lines_est * 10)
                panel(pdf, rx, y-box_h, rw, box_h, bc=GREEN)
                pdf.setFillColor(HexColor(GREEN)); pdf.setFont("Helvetica-Bold",8)
                pdf.drawString(rx+10, y-12, "MENSAGEM PARA O PRODUTOR")
                tblock(pdf, rx+10, y-26, imp[:350], sz=7.5, clr=TEXT2, mw=rw-20, ld=10)
                y -= box_h + 6
                break

    # --- Seasonality legend (below seasonality chart on left) ---
    if img_season:
        sy = PAGE_H - 480
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6.5)
        pdf.drawString(M, sy-10, "Linha tracejada = media historica mensal | Linha solida = ano atual | Ponto = mes atual")


# ── PAGE 7: DEMAIS COMMODITIES ──
def pg_others(pdf, pr, img_grid):
    dbg(pdf); hdr(pdf,"Demais Commodities","Trigo, Acucar, Algodao, Cacau, Porco, Energia, Metais e mais",
                  "Graficos de 60 dias. Verde = subindo, vermelho = caindo. Preco atual no canto esquerdo.")
    if img_grid:
        pdf.drawImage(img_grid, 5, PAGE_H-460, width=PAGE_W-10, height=385, preserveAspectRatio=True, mask="auto")


# ── PAGE 8: TABELA DE VARIACOES ──
def pg_variations_table(pdf, pr):
    dbg(pdf); hdr(pdf,"Tabela de Variacoes","Todas as commodities — variacao diaria, semanal, mensal e posicao no ano",
                  "Verde = alta. Vermelho = queda. Barra mostra onde o preco esta entre a minima e maxima do ano.")
    y = PAGE_H - 78
    pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",9)
    pdf.drawString(M,y,"TODAS AS COMMODITIES"); y-=16
    cols=[M, M+110, M+175, M+250, M+320, M+395, M+470, M+550, M+640]
    hdrs2=["Commodity","Origem","Ultimo","Dia","Semana","Mes","Min Ano","Max Ano","Posicao"]
    pdf.setFont("Helvetica-Bold",7.5); pdf.setFillColor(HexColor(TEXT_MUT))
    for j,h in enumerate(hdrs2): pdf.drawString(cols[j],y,h)
    y-=4; pdf.setStrokeColor(HexColor(BORDER)); pdf.setLineWidth(0.4); pdf.line(M,y,PAGE_W-M,y); y-=13
    for sym,nm in GRID_ALL:
        if y < 35: break
        c1d=pchg(sym,pr,1); c1w=pchg(sym,pr,5); c1m=pchg(sym,pr,21)
        lc=last_close(sym,pr); h=hi52(sym,pr); l=lo52(sym,pr)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",7.5)
        pdf.drawString(cols[0],y,f"{NM_PROD.get(sym,nm)}")
        is_br = False  # LE e GF sao CME Chicago
        origin_badge(pdf, cols[1], y, is_brazil=is_br)
        pdf.setFont("Helvetica",7.5)
        if lc: pdf.setFillColor(HexColor(TEXT)); pdf.drawString(cols[2],y,num_fmt(lc))
        for j,val in enumerate([c1d,c1w,c1m]):
            if val is not None:
                cc=GREEN if val>=0 else RED
                pdf.setFillColor(HexColor(cc)); pdf.drawString(cols[j+3],y,chg_str(val))
            else: pdf.setFillColor(HexColor(TEXT_DIM)); pdf.drawString(cols[j+3],y,"-")
        if l: pdf.setFillColor(HexColor(TEXT_DIM)); pdf.drawString(cols[6],y,num_fmt(l))
        if h: pdf.drawString(cols[7],y,num_fmt(h))
        hp = hilo_pct(sym, pr)
        if hp is not None:
            # Mini bar
            bar_x = cols[8]; bar_w = 55
            pdf.setFillColor(HexColor(BORDER)); pdf.rect(bar_x, y-1, bar_w, 7, fill=1, stroke=0)
            fill_w = bar_w * (hp/100)
            pc = GREEN if hp > 70 else (RED if hp < 30 else AMBER)
            pdf.setFillColor(HexColor(pc)); pdf.rect(bar_x, y-1, fill_w, 7, fill=1, stroke=0)
            pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",6)
            pdf.drawString(bar_x+bar_w+4, y, f"{hp:.0f}%")
        y-=14


# ── PAGE 9: SPREADS ──
def pg_spreads(pdf, sd, img_sp):
    dbg(pdf)
    # Header especial - mais destaque
    pdf.setFillColor(HexColor("#2d1f6e")); pdf.rect(0,PAGE_H-38,PAGE_W,38,fill=1,stroke=0)
    pdf.setFillColor(HexColor("#fff")); pdf.setFont("Helvetica-Bold",15)
    pdf.drawString(M,PAGE_H-26,"RELACOES DE PRECO — O que esta barato e caro entre si")
    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7.5)
    pdf.drawRightString(PAGE_W-M,PAGE_H-15,f"AgriMacro v3.2 | {TODAY_BR} ({WDAY})")
    ey = PAGE_H - 54
    pdf.setFillColor(HexColor(EXPLAIN_BG)); pdf.rect(M-5,ey-14,PAGE_W-2*M+10,18,fill=1,stroke=0)
    pdf.setFillColor(HexColor(GREEN)); pdf.setFont("Helvetica-Bold",8)
    pdf.drawString(M,ey-8,"COMO LER: ")
    tw = pdf.stringWidth("COMO LER: ","Helvetica-Bold",8)
    pdf.setFillColor(HexColor(TEXT2)); pdf.setFont("Helvetica",8)
    pdf.drawString(M+tw,ey-8,"Seta na esquerda = barato (oportunidade?). Seta na direita = caro (cuidado). Meio = equilibrado.")
    if img_sp: pdf.drawImage(img_sp, 5, PAGE_H-370, width=PAGE_W-10, height=300, preserveAspectRatio=True, mask="auto")
    spreads=sd.get("spreads",{})
    y=PAGE_H-388
    pdf.setFillColor(HexColor(PURPLE)); pdf.setFont("Helvetica-Bold",12)
    pdf.drawString(M,y,"O QUE CADA RELACAO SIGNIFICA NA PRATICA"); y-=6
    col_w = (PAGE_W - 2*M - 15) / 2
    col = 0; cx = M; cy = y
    for k,sp in spreads.items():
        if cy < 30 and col == 0:
            col = 1; cx = M + col_w + 15; cy = y
        if cy < 30: break
        nm=SPR_NM.get(k,sp.get("name",k))
        explain=SPR_EXPLAIN.get(k,"")
        reg=sp.get("regime","NORMAL")
        rc=GREEN if reg=="NORMAL" else (RED if "DISSON" in reg or "COMPRESS" in reg else AMBER)
        regime_pt = "Equilibrado" if reg=="NORMAL" else ("Comprimido!" if "COMPRESS" in reg else ("Fora do padrao!" if "DISSON" in reg else reg))
        if explain:
            panel(pdf,cx,cy-44,col_w,46,bc=rc)
            pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",9.5)
            pdf.drawString(cx+8,cy-13,nm)
            pdf.setFillColor(HexColor(rc)); pdf.setFont("Helvetica-Bold",8)
            pdf.drawRightString(cx+col_w-8,cy-13,regime_pt)
            tblock(pdf,cx+8,cy-26,explain[:140],sz=7.5,clr=TEXT_MUT,mw=col_w-20,ld=9)
            cy-=50




# ── PAGE 9B: ARBITRAGEM BR vs CHICAGO ──
def pg_arbitrage(pdf, pr, phys, bcb, img_arb):
    """Pagina dedicada a arbitragem Brasil vs Chicago"""
    dbg(pdf)
    pdf.setFillColor(HexColor("#1a2040")); pdf.rect(0,PAGE_H-38,PAGE_W,38,fill=1,stroke=0)
    pdf.setFillColor(HexColor(CYAN)); pdf.setFont("Helvetica-Bold",15)
    pdf.drawString(M,PAGE_H-26,"ARBITRAGEM — Brasil vs Chicago em Reais")
    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7.5)
    pdf.drawRightString(PAGE_W-M,PAGE_H-15,f"AgriMacro v3.2 | {TODAY_BR} ({WDAY})")
    ey = PAGE_H - 54
    pdf.setFillColor(HexColor(EXPLAIN_BG)); pdf.rect(M-5,ey-14,PAGE_W-2*M+10,18,fill=1,stroke=0)
    pdf.setFillColor(HexColor(GREEN)); pdf.setFont("Helvetica-Bold",8)
    pdf.drawString(M,ey-8,"COMO LER: ")
    tw = pdf.stringWidth("COMO LER: ","Helvetica-Bold",8)
    pdf.setFillColor(HexColor(TEXT2)); pdf.setFont("Helvetica",8)
    pdf.drawString(M+tw,ey-8,"Positivo = Brasil mais caro que Chicago (premio). Negativo = Brasil mais barato (desconto). Inclui conversao cambial.")
    y = PAGE_H - 78
    fx = get_brl_usd(bcb)
    arbs = calc_arbitrages(pr, phys, bcb)
    # Cards de arbitragem
    if arbs:
        cw = (PAGE_W - 2*M - 30) / min(len(arbs), 4)
        for i, (sym, data) in enumerate(arbs.items()):
            if i >= 4: break
            x = M + i*(cw+10)
            sp = data["spread_pct"]; sp_brl = data["spread_brl"]
            bc = GREEN if sp > 2 else (RED if sp < -2 else AMBER)
            panel(pdf, x, y-90, cw, 92, bc=bc)
            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica-Bold",9)
            pdf.drawString(x+10, y-14, data["name"])
            # BR price
            origin_badge(pdf, x+10, y-28, is_brazil=True)
            pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",14)
            pdf.drawString(x+60, y-28, f"R$ {data['br_price']:,.2f}")
            # Chicago converted
            origin_badge(pdf, x+10, y-44, is_brazil=False)
            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",10)
            pdf.drawString(x+70, y-44, f"R$ {data['chi_price_brl']:,.2f}")
            # Spread
            pdf.setFillColor(HexColor(bc)); pdf.setFont("Helvetica-Bold",16)
            direction = "PREMIO" if sp > 0 else "DESCONTO"
            pdf.drawString(x+10, y-66, f"{sp:+.1f}%")
            pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",7)
            pdf.drawString(x+10, y-78, f"{direction} | R$ {sp_brl:+,.2f}/{data['unit'].split('/')[-1]}")
            pdf.drawString(x+10, y-88, data["br_source"])
        y -= 105
    # Grafico
    if img_arb:
        pdf.drawImage(img_arb, 5, y-220, width=PAGE_W-10, height=215, preserveAspectRatio=True, mask="auto")
        y -= 230
    # Explicacao
    if fx:
        pdf.setFillColor(HexColor(PURPLE)); pdf.setFont("Helvetica-Bold",11)
        pdf.drawString(M, y, "COMO FUNCIONA A ARBITRAGEM"); y -= 18
        comp_w = (PAGE_W - 2*M - 15) / 2
        explanations = [
            (GREEN, "PREMIO BRASIL", "Preco fisico no Brasil acima de Chicago convertido. Pode significar: demanda interna forte, logistica cara, ou safra apertada. Exportador pode preferir vender no mercado interno."),
            (RED, "DESCONTO BRASIL", "Preco fisico abaixo de Chicago convertido. Oportunidade de exportacao: produtor ganha mais vendendo para fora. Comum na colheita quando ha excesso de oferta local."),
            (AMBER, "CAMBIO E O FATOR CHAVE", f"Cambio atual: R$ {fx:.2f}. Dolar subindo = Chicago convertido sobe em R$ = desconto BR diminui ou vira premio. Cada R$ 0,10 no dolar muda ~R$ 2-3 na saca de soja."),
            (CYAN, "CUSTO DE INTERNACAO", "Lembre que entre Chicago e o interior do Brasil ha: frete maritimo, seguro, taxa portuaria, frete rodoviario e impostos. O spread 'real' precisa descontar esses custos (~US$ 30-50/ton)."),
        ]
        for idx, (clr, title, desc) in enumerate(explanations):
            col = idx % 2; row = idx // 2
            cx = M + col * (comp_w + 15)
            cy = y - row * 60
            panel(pdf, cx, cy-52, comp_w, 54, bc=clr)
            pdf.setFillColor(HexColor(clr)); pdf.setFont("Helvetica-Bold",8.5)
            pdf.drawString(cx+10, cy-13, title)
            tblock(pdf, cx+10, cy-26, desc[:200], sz=7, clr=TEXT_MUT, mw=comp_w-22, ld=9)
    pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6.5)
    pdf.drawString(M, 22, f"Fontes: CEPEA/ESALQ, CME/CBOT, BCB | Cambio: R$ {fx:.4f} | Conversao: soja 60kg/sc, milho 60kg/sc, boi 15kg/@, acucar 50kg/sc")


# ── PAGE 10: ESTOQUES ──
def pg_stocks(pdf, sw, img_stocks):
    dbg(pdf); hdr(pdf,"Estoques no Mundo vs Media 5 Anos — Tem produto ou ta faltando?",
                  "Dados do USDA: estoque atual vs media dos ultimos 5 anos",
                  "Barra azul para esquerda = estoque curto, preco tende a subir. Barra para direita = sobra, preco pressionado.")
    # Cross-validation warning
    warn_y = PAGE_H - 78
    pdf.setFillColor(HexColor("#2a1a1a")); pdf.rect(M-5, warn_y-14, PAGE_W-2*M+10, 18, fill=1, stroke=0)
    pdf.setFillColor(HexColor(AMBER)); pdf.setFont("Helvetica-Bold",7)
    pdf.drawString(M, warn_y-9, "ATENCAO:")
    tw = pdf.stringWidth("ATENCAO: ","Helvetica-Bold",7)
    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7)
    pdf.drawString(M+tw, warn_y-9, "Estoques USDA podem divergir do mercado fisico real. Use como referencia, nao como verdade absoluta. Sempre cruze com informacoes locais.")
    comms=sw.get("commodities",{})
    if img_stocks:
        pdf.drawImage(img_stocks,5,PAGE_H-385,width=PAGE_W/2-10,height=290,preserveAspectRatio=True,mask="auto")
    rx=PAGE_W/2+10; y=PAGE_H-95
    pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",9)
    pdf.drawString(rx,y,"COMO LER ESTE GRAFICO"); y-=16
    legend_items = [
        (RED, "Muito acima da media (>15%)", "Sobra de produto — preco tende a cair"),
        (AMBER, "Acima da media (5-15%)", "Oferta confortavel"),
        (TEXT_MUT, "Normal (-5% a +5%)", "Equilibrio entre oferta e demanda"),
        (BLUE, "Abaixo da media (-5% a -15%)", "Estoque apertando — fique atento"),
        (CYAN, "Muito abaixo (<-15%)", "Faltando produto — preco tende a subir"),
    ]
    for clr, label, desc in legend_items:
        pdf.setFillColor(HexColor(clr)); pdf.rect(rx, y-2, 12, 10, fill=1, stroke=0)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",7.5)
        pdf.drawString(rx+18, y, label)
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6.5)
        pdf.drawString(rx+18, y-10, desc)
        y -= 24
    y -= 8
    pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",9)
    pdf.drawString(rx,y,"DETALHAMENTO DE ESTOQUES"); y-=14
    cols2=[rx,rx+75,rx+135,rx+195,rx+270]
    hdrs3=["Produto","Estoque","Media 5A","Desvio","Nota"]
    pdf.setFont("Helvetica-Bold",7); pdf.setFillColor(HexColor(TEXT_MUT))
    for j,h in enumerate(hdrs3): pdf.drawString(cols2[j],y,h)
    y-=3; pdf.setStrokeColor(HexColor(BORDER)); pdf.setLineWidth(0.3); pdf.line(rx,y,PAGE_W-M,y); y-=10
    for sym in list(comms.keys())[:14]:
        if y<50: break
        d=comms[sym]; nm=NM_PROD.get(sym,sym)
        cur=d.get("stock_current"); avg=d.get("stock_avg")
        # Calcula desvio REAL de estoque
        stock_dev = None; is_suspect = False
        if cur is not None and avg is not None:
            try:
                cur_f = float(cur); avg_f = float(avg)
                if avg_f != 0:
                    stock_dev = ((cur_f - avg_f) / avg_f) * 100
                    is_suspect = abs(stock_dev) > 50
            except (ValueError, TypeError):
                pass
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",7)
        pdf.drawString(cols2[0],y,f"{nm}")
        if cur is not None:
            pdf.setFillColor(HexColor("#ff6666" if is_suspect else TEXT))
            pdf.drawString(cols2[1],y,f"{num_fmt(float(cur),0) if cur else '-'}")
        if avg is not None:
            pdf.setFillColor(HexColor(TEXT_DIM))
            pdf.drawString(cols2[2],y,f"{num_fmt(float(avg),0) if avg else '-'}")
        # --- FAILSAFE v3.3: dados ausentes ---
        if cur is None and avg is None and stock_dev is None:
            pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica-Oblique",6)
            pdf.drawString(cols2[1],y,"sem dado disponivel")
        if stock_dev is not None:
            if is_suspect:
                cc = "#ff6666"  # Vermelho claro = dado suspeito
            else:
                cc=RED if stock_dev>15 else (AMBER if stock_dev>5 else (CYAN if stock_dev<-15 else TEXT_MUT))
            pdf.setFillColor(HexColor(cc)); pdf.drawString(cols2[3],y,f"{stock_dev:+.1f}%")
        # Nota: se suspeito, mostra aviso; senao mostra classificacao USDA
        if is_suspect:
            pdf.setFillColor(HexColor("#ff6666")); pdf.setFont("Helvetica-Bold",6)
            pdf.drawString(cols2[4],y,"VERIFICAR UNIDADE")
        else:
            # Classificacao baseada em estoque (nao preco!)
            if stock_dev is not None:
                if stock_dev < -15: nota = "APERTO"
                elif stock_dev < -5: nota = "ABAIXO"
                elif stock_dev > 15: nota = "EXCESSO"
                elif stock_dev > 5: nota = "ACIMA"
                else: nota = "NEUTRO"
                nc = CYAN if "APERTO" in nota else (RED if "EXCESSO" in nota else TEXT_MUT)
                pdf.setFillColor(HexColor(nc)); pdf.setFont("Helvetica-Bold",6.5)
                pdf.drawString(cols2[4],y,nota)
        y-=10


# ── PAGE 11: COT ──
def pg_cot(pdf, cd, img_cot):
    dbg(pdf); hdr(pdf,"Posicao dos Fundos — Quem ta apostando em que?",
                  "Dados semanais dos grandes players nos mercados futuros",
                  "Veja a legenda no grafico: 4 cores, cada uma com significado diferente.")
    if img_cot: pdf.drawImage(img_cot,5,PAGE_H-400,width=PAGE_W-10,height=315,preserveAspectRatio=True,mask="auto")
    y=PAGE_H-418
    comms=cd.get("commodities",{})
    if comms:
        pdf.setFillColor(HexColor(PURPLE)); pdf.setFont("Helvetica-Bold",10)
        pdf.drawString(M,y,"ENTENDA AS CORES DO GRAFICO"); y-=5
        color_legend = [
            (GREEN, "VERDE", "Fundos COMPRADOS - Grandes especuladores apostando que o preco VAI SUBIR"),
            (RED, "VERMELHO", "Fundos VENDIDOS - Grandes especuladores apostando que o preco VAI CAIR"),
            (BLUE, "AZUL", "Produtores/Industria COMPRADOS - Travas de compra (hedge de quem precisa do produto)"),
            (AMBER, "LARANJA", "Produtores/Industria VENDIDOS - Travas de venda (hedge de quem produz)"),
        ]
        legend_w = (PAGE_W - 2*M - 10) / 2
        for idx, (clr, name, desc) in enumerate(color_legend):
            col = idx % 2; row = idx // 2
            lx = M + col * (legend_w + 10)
            ly = y - row * 28
            pdf.setFillColor(HexColor(clr)); pdf.rect(lx, ly-10, 14, 14, fill=1, stroke=0)
            pdf.setFillColor(HexColor(clr)); pdf.setFont("Helvetica-Bold",8)
            pdf.drawString(lx+20, ly-3, name)
            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7)
            pdf.drawString(lx+20, ly-14, desc[:80])
        y -= 60
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",8.5)
        pdf.drawString(M,y,"NUMEROS DETALHADOS"); y-=12
        cols3=[M,M+75,M+160,M+250,M+345,M+445,M+545,M+650]
        hdrs4=["Produto","Fundos Compra","Fundos Venda","Fundos Saldo","Hedge Compra","Hedge Venda","Hedge Saldo","Total Aberto"]
        pdf.setFont("Helvetica-Bold",6); pdf.setFillColor(HexColor(TEXT_MUT))
        for j,h in enumerate(hdrs4): pdf.drawString(cols3[j],y,h)
        y-=3; pdf.setStrokeColor(HexColor(BORDER)); pdf.setLineWidth(0.3); pdf.line(M,y,PAGE_W-M,y); y-=9
        sorted_c = sorted(comms.items(), key=lambda x: abs(x[1].get("legacy",{}).get("latest",{}).get("noncomm_net",0)), reverse=True)
        for sym,data in sorted_c[:10]:
            if y<35: break
            leg=data.get("legacy",{}).get("latest",{})
            nm=NM_PROD.get(sym,data.get("name",sym))[:12]
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


# ── PAGE 12: ENERGIA ──
def pg_energy(pdf, ed, img_eia):
    dbg(pdf); hdr(pdf,"Energia — Diesel, Gas e Etanol",
                  "O que mexe no frete, no fertilizante e na demanda de milho",
                  "Diesel = frete do grao. Gas natural = custo do fertilizante. Etanol = demanda de milho nos EUA.")
    if img_eia: pdf.drawImage(img_eia,5,PAGE_H-390,width=PAGE_W-10,height=305,preserveAspectRatio=True,mask="auto")
    y=PAGE_H-408
    impacts=[
        ("DIESEL E FRETE","Diesel e 20-30% do frete rodoviario. Diesel caro = margem menor pra quem vende no interior.",AMBER),
        ("ETANOL E MILHO","EUA usa ~35% do milho pra etanol. Producao de etanol alta = demanda por milho firme.",GREEN),
        ("GAS E ADUBO","Gas natural e materia-prima pra ureia e MAP. Gas caro = fertilizante caro = custo da safrinha sobe.",RED),
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
        pdf.drawString(M,y,"RESUMO ENERGIA"); y-=12
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7.5)
        for s in sums[:5]:
            if y<25: break
            pdf.drawString(M+5,y,f"  {s[:140]}"); y-=11



# ── PAGE 13: ACUCAR & ALCOOL (NOVA!) ──
def pg_sugar_alcohol(pdf, pr, ed, phys, bcb, img_sugar, sabr=None):
    """Pagina dedicada ao setor sucroalcooleiro"""
    dbg(pdf)
    pdf.setFillColor(HexColor("#1a3a1a")); pdf.rect(0,PAGE_H-38,PAGE_W,38,fill=1,stroke=0)
    pdf.setFillColor(HexColor(GREEN)); pdf.setFont("Helvetica-Bold",15)
    pdf.drawString(M,PAGE_H-26,"ACUCAR & ALCOOL — Setor Sucroalcooleiro")
    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",8)
    pdf.drawString(M,PAGE_H-12,"Cana-de-acucar, etanol de cana e comparativo com etanol de milho")
    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7.5)
    pdf.drawRightString(PAGE_W-M,PAGE_H-15,f"AgriMacro v3.2 | {TODAY_BR} ({WDAY})")
    y = PAGE_H - 55
    # Cards de precos
    sb_price = last_close("SB", pr); sb_chg_d = pchg("SB", pr, 1); sb_chg_m = pchg("SB", pr, 21)
    zc_price = last_close("ZC", pr)
    cw = (PAGE_W - 2*M - 30) / 4
    cards_sa = []
    intl = phys.get("international",{})
    if sb_price:
        cards_sa.append(("Acucar (Chicago)", f"{sb_price:,.2f} c/lb", f"Dia: {chg_str(sb_chg_d)} | Mes: {chg_str(sb_chg_m)}", TEAL, False))
    sb_br = intl.get("SB_BR",{})
    if sb_br and sb_br.get("price"):
        cards_sa.append(("Acucar (Brasil)", f"{sb_br['price']} {sb_br.get('price_unit','')}", f"Tendencia: {sb_br.get('trend','')}", GREEN, True))
    else:
        cards_sa.append(("Acucar (Brasil)", "Sem dado", "CEPEA/ESALQ", GREEN, True))
    # Etanol BR (hidratado e anidro)
    eth_br = intl.get("ETH_BR",{})
    if eth_br and eth_br.get("price"):
        cards_sa.append(("Etanol Hidratado BR", f"R$ {eth_br['price']:.4f}/L", f"{eth_br.get('trend','')}", CYAN, True))
    etn_br = intl.get("ETN_BR",{})
    if etn_br and etn_br.get("price"):
        cards_sa.append(("Etanol Anidro BR", f"R$ {etn_br['price']:.4f}/L", f"{etn_br.get('trend','')}", PURPLE, True))
    series = ed.get("series",{})
    eth_prod = series.get("ethanol_production",{}); eth_lat = eth_prod.get("latest_value",""); eth_wow = eth_prod.get("wow_change_pct",0) or 0
    if len(cards_sa) < 4:
        cards_sa.append(("Etanol EUA (producao)", f"{eth_lat} mil bbl/dia" if eth_lat else "S/D", f"Semana: {eth_wow:+.1f}%", AMBER, False))
    if len(cards_sa) < 4 and zc_price:
        zc_chg = pchg("ZC", pr, 1)
        cards_sa.append(("Milho (insumo etanol)", f"{zc_price:,.1f} c/bu", f"Dia: {chg_str(zc_chg)}", CYAN, False))
    for i, (title, val, sub, clr, is_br) in enumerate(cards_sa[:4]):
        x = M + i*(cw+10)
        panel(pdf, x, y-65, cw, 67, bc=clr)
        origin_badge(pdf, x+10, y-10, is_brazil=is_br)
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7); pdf.drawString(x+60, y-10, title)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",16); pdf.drawString(x+10, y-36, val[:20])
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",7); pdf.drawString(x+10, y-52, sub[:42])
    y -= 80
    # Graficos
    if img_sugar:
        pdf.drawImage(img_sugar, 5, y-200, width=PAGE_W-10, height=195, preserveAspectRatio=True, mask="auto")
        y -= 210
    # Comparativo Cana vs Milho
    pdf.setFillColor(HexColor(PURPLE)); pdf.setFont("Helvetica-Bold",11)
    pdf.drawString(M, y, "COMPARATIVO: ETANOL DE CANA vs ETANOL DE MILHO"); y -= 16
    comp_w = (PAGE_W - 2*M - 15) / 2
    # Painel Cana
    panel(pdf, M, y-105, comp_w, 107, bc=GREEN)
    pdf.setFillColor(HexColor(GREEN)); pdf.setFont("Helvetica-Bold",11)
    pdf.drawString(M+10, y-14, "ETANOL DE CANA (Brasil)")
    origin_badge(pdf, M+200, y-14, is_brazil=True)
    info_cana = [
        ("Materia-prima:", "Cana-de-acucar (safra abr-nov)"),
        ("Rendimento:", "~82 litros etanol / ton cana"),
        ("Vantagem:", "Menor custo, maior eficiencia energetica"),
        ("Mix usina:", "Decide: mais acucar ou mais etanol"),
        ("Fator decisivo:", "Acucar Chicago sobe = usina vira pra acucar"),
        ("Sazonalidade:", "Entressafra (dez-mar) = etanol mais caro"),
    ]
    iy = y - 28
    for label, val in info_cana:
        pdf.setFillColor(HexColor(CYAN)); pdf.setFont("Helvetica-Bold",7); pdf.drawString(M+10, iy, label)
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7); pdf.drawString(M+110, iy, val)
        iy -= 12
    # Painel Milho
    rx = M + comp_w + 15
    panel(pdf, rx, y-105, comp_w, 107, bc=AMBER)
    pdf.setFillColor(HexColor(AMBER)); pdf.setFont("Helvetica-Bold",11)
    pdf.drawString(rx+10, y-14, "ETANOL DE MILHO (EUA / BR emergente)")
    origin_badge(pdf, rx+290, y-14, is_brazil=False)
    info_milho = [
        ("Materia-prima:", "Milho (producao o ano todo)"),
        ("Rendimento:", "~400 litros etanol / ton milho"),
        ("Custo:", "Maior - milho e mais caro que cana"),
        ("Subproduto:", "DDG (racao animal) - receita adicional"),
        ("No Brasil:", "MT lidera (aproveita milho safrinha)"),
        ("Tendencia:", "Cresce no BR mas cana domina 85%"),
    ]
    iy = y - 28
    for label, val in info_milho:
        pdf.setFillColor(HexColor(CYAN)); pdf.setFont("Helvetica-Bold",7); pdf.drawString(rx+10, iy, label)
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7); pdf.drawString(rx+110, iy, val)
        iy -= 12
    y -= 115
    # Logica da Usina
    if y > 80:
        margin_items = [
            (TEAL, "RECEITA", "Usina vende acucar + etanol + energia eletrica. Mix varia conforme preco."),
            (RED, "CUSTO", "Cana = 70-80% do custo. ATR define pagamento ao fornecedor."),
            (GREEN, "DECISAO", "Acucar Chicago sobe = usina maximiza acucar. Etanol sobe no BR = mais etanol."),
            (AMBER, "SINAL", "Acucar alta + dolar forte = usina paga mais ATR. Entressafra = etanol caro."),
        ]
        card_w = (PAGE_W - 2*M - 30) / 4
        for idx, (clr, title, desc) in enumerate(margin_items):
            cx = M + idx * (card_w + 10)
            panel(pdf, cx, y-50, card_w, 52, bc=clr)
            pdf.setFillColor(HexColor(clr)); pdf.setFont("Helvetica-Bold",8); pdf.drawString(cx+8, y-12, f"USINA: {title}")
            tblock(pdf, cx+8, y-26, desc, sz=6.5, clr=TEXT_MUT, mw=card_w-18, ld=8)
    pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6.5)
    pdf.drawString(M, 22, "Fontes: UNICA, CEPEA/ESALQ, EIA, CONSECANA | Dados de custos sao estimativas baseadas em medias setoriais")




# ── PAGE: SINERGIA ENERGIA x SUCROALCOOLEIRO ──
def pg_energy_sugar_cross(pdf, pr, ed, phys, bcb, img_cross):
    """Pagina de dados cruzados entre energia e setor sucroalcooleiro"""
    dbg(pdf)
    pdf.setFillColor(HexColor("#1a1a3a")); pdf.rect(0,PAGE_H-38,PAGE_W,38,fill=1,stroke=0)
    pdf.setFillColor(HexColor(AMBER)); pdf.setFont("Helvetica-Bold",14)
    pdf.drawString(M,PAGE_H-26,"SINERGIA: ENERGIA x SUCROALCOOLEIRO")
    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",8)
    pdf.drawString(M,PAGE_H-12,"Dados cruzados: petroleo, acucar, etanol, diesel, gas — como se conectam")
    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7.5)
    pdf.drawRightString(PAGE_W-M,PAGE_H-15,f"AgriMacro v3.2 | {TODAY_BR} ({WDAY})")
    ey = PAGE_H - 54
    pdf.setFillColor(HexColor(EXPLAIN_BG)); pdf.rect(M-5,ey-14,PAGE_W-2*M+10,18,fill=1,stroke=0)
    pdf.setFillColor(HexColor(GREEN)); pdf.setFont("Helvetica-Bold",8)
    pdf.drawString(M,ey-8,"POR QUE IMPORTA: ")
    tw = pdf.stringWidth("POR QUE IMPORTA: ","Helvetica-Bold",8)
    pdf.setFillColor(HexColor(TEXT2)); pdf.setFont("Helvetica",8)
    pdf.drawString(M+tw,ey-8,"Petroleo alto = etanol competitivo = usina faz mais etanol = menos acucar = acucar sobe. Diesel = custo do frete. Gas = custo do adubo.")
    y = PAGE_H - 78
    # ── Cards de resumo cruzado ──
    cl_price = last_close("CL", pr)
    sb_price = last_close("SB", pr)
    ng_price = last_close("NG", pr)
    zc_price = last_close("ZC", pr)
    series = ed.get("series",{})
    intl = phys.get("international",{})
    cw = (PAGE_W - 2*M - 50) / 6
    mini_cards = []
    if cl_price: mini_cards.append(("Petroleo", f"${cl_price:,.1f}", "$/bbl", AMBER))
    if sb_price: mini_cards.append(("Acucar", f"{sb_price:,.2f}", "c/lb", TEAL))
    if ng_price: mini_cards.append(("Gas Nat.", f"${ng_price:,.2f}", "$/MMBtu", RED))
    if zc_price: mini_cards.append(("Milho", f"{zc_price:,.1f}", "c/bu", GREEN))
    eth_br = intl.get("ETH_BR",{})
    if eth_br and eth_br.get("price"):
        mini_cards.append(("Etanol BR", f"R${eth_br['price']:.2f}", "/litro", CYAN))
    diesel_s = series.get("diesel_retail",{})
    diesel_v = diesel_s.get("latest_value")
    if diesel_v:
        mini_cards.append(("Diesel EUA", f"${diesel_v:.3f}", "/gal", PURPLE))
    for i, (title, val, unit, clr) in enumerate(mini_cards[:6]):
        x = M + i*(cw+10)
        panel(pdf, x, y-48, cw, 50, bc=clr)
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",6.5); pdf.drawString(x+6, y-10, title)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",13); pdf.drawString(x+6, y-28, val[:12])
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6); pdf.drawString(x+6, y-40, unit)
    y -= 62
    # ── Graficos 2x3 ──
    if img_cross:
        pdf.drawImage(img_cross, 5, y-250, width=PAGE_W-10, height=245, preserveAspectRatio=True, mask="auto")
        y -= 260
    # ── Painel de correlacoes e impactos ──
    pdf.setFillColor(HexColor(PURPLE)); pdf.setFont("Helvetica-Bold",11)
    pdf.drawString(M, y, "COMO TUDO SE CONECTA — Cadeia de impactos"); y -= 14
    comp_w = (PAGE_W - 2*M - 20) / 3
    connections = [
        (AMBER, "PETROLEO SOBE",
         "1) Etanol fica competitivo na bomba\n"
         "2) Usina BR produz mais etanol, menos acucar\n"
         "3) Menos acucar no mercado = preco sobe\n"
         "4) Diesel sobe = frete do grao mais caro"),
        (TEAL, "ACUCAR CHICAGO SOBE",
         "1) Usina BR maximiza acucar (reduz etanol)\n"
         "2) Oferta de etanol cai = preco sobe na bomba\n"
         "3) Entressafra (dez-mar) amplifica o efeito\n"
         "4) Exportador ganha com dolar + acucar alto"),
        (GREEN, "MILHO SOBE",
         "1) Etanol de milho (EUA) fica mais caro\n"
         "2) Pode reduzir producao EUA de etanol\n"
         "3) Racao animal mais cara = pressao no boi\n"
         "4) Safrinha cara = margem do produtor aperta"),
    ]
    for i, (clr, title, desc) in enumerate(connections):
        cx = M + i * (comp_w + 10)
        # Calcula altura necessaria
        lines = desc.replace("\\n", "\n").split("\n")
        box_h = 16 + len(lines) * 10 + 4
        panel(pdf, cx, y-box_h, comp_w, box_h, bc=clr)
        pdf.setFillColor(HexColor(clr)); pdf.setFont("Helvetica-Bold",8.5)
        pdf.drawString(cx+10, y-13, title)
        dy = y - 27
        for line in lines:
            clean = line.strip()
            if clean:
                pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7)
                pdf.drawString(cx+10, dy, clean[:60])
                dy -= 10
    y -= 70
    # ── Paridade Etanol/Gasolina e Spread Acucar ──
    if y > 60:
        half_w = (PAGE_W - 2*M - 15) / 2
        # Paridade etanol/gasolina
        panel(pdf, M, y-52, half_w, 54, bc=CYAN)
        pdf.setFillColor(HexColor(CYAN)); pdf.setFont("Helvetica-Bold",9)
        pdf.drawString(M+10, y-13, "REGRA DA BOMBA: ETANOL vs GASOLINA")
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",8)
        pdf.drawString(M+10, y-27, "Se preco etanol < 70% da gasolina = vale abastecer etanol")
        eth_br_data = intl.get("ETH_BR",{})
        if eth_br_data and eth_br_data.get("price"):
            eth_p = float(eth_br_data["price"])
            # Estimativa gasolina BR: ~R$ 6.00/L (referencia ANP media nacional)
            gas_est = 6.00
            paridade = (eth_p / gas_est) * 100
            pc = GREEN if paridade < 70 else (AMBER if paridade < 75 else RED)
            veredito = "VALE ETANOL" if paridade < 70 else ("QUASE IGUAL" if paridade < 75 else "GASOLINA MELHOR")
            pdf.setFillColor(HexColor(pc)); pdf.setFont("Helvetica-Bold",12)
            pdf.drawString(M+10, y-42, f"Paridade: {paridade:.0f}% — {veredito}")
            pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6.5)
            pdf.drawString(M+10, y-52, f"Etanol: R$ {eth_p:.4f}/L | Gasolina ref: ~R$ {gas_est:.2f}/L (media nacional estimada)")
        else:
            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",8)
            pdf.drawString(M+10, y-42, "Dados de etanol BR indisponiveis — rode collect_physical_intl.py")
        # Spread acucar BR vs Chicago
        rx = M + half_w + 15
        panel(pdf, rx, y-52, half_w, 54, bc=TEAL)
        pdf.setFillColor(HexColor(TEAL)); pdf.setFont("Helvetica-Bold",9)
        pdf.drawString(rx+10, y-13, "SPREAD: ACUCAR BRASIL vs CHICAGO")
        sb_br_data = intl.get("SB_BR",{})
        sb_chi = last_close("SB", pr)
        fx = get_brl_usd(bcb) if "get_brl_usd" in dir() else None
        if not fx:
            rc = bcb.get("resumo_cambio",{}); fx = rc.get("brl_usd_atual")
        if sb_br_data and sb_br_data.get("price") and sb_chi and fx:
            br_p = float(sb_br_data["price"])
            # Converter Chicago c/lb para R$/sc 50kg
            chi_brl = (sb_chi / 100.0) * (50.0 / 0.4536) * float(fx)
            spread = br_p - chi_brl
            spread_pct = (spread / chi_brl) * 100 if chi_brl > 0 else 0
            sc = GREEN if spread_pct > 0 else RED
            direction = "PREMIO BR" if spread_pct > 0 else "DESCONTO BR"
            pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",8)
            pdf.drawString(rx+10, y-27, f"Brasil: R$ {br_p:.2f}/sc 50kg | Chicago conv: R$ {chi_brl:.2f}/sc")
            pdf.setFillColor(HexColor(sc)); pdf.setFont("Helvetica-Bold",11)
            pdf.drawString(rx+10, y-42, f"{direction}: {spread_pct:+.1f}% (R$ {spread:+.2f}/sc)")
        else:
            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",8)
            pdf.drawString(rx+10, y-27, "Dados insuficientes para calcular spread")
            pdf.drawString(rx+10, y-42, "Necessario: SB_BR + SB Chicago + cambio")
    pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6.5)
    pdf.drawString(M, 22, "Fontes: EIA, CEPEA/ESALQ, CME/CBOT, BCB, ANP (estimativa) | Paridade etanol/gasolina usa media nacional estimada")


# ── PAGE 14: PECUARIA COMPARATIVA ──
def pg_cattle_compare(pdf, pr, phys, bcb, img_cattle):
    """Pagina dedicada: Boi Gordo vs Engorda, Brasil vs Chicago"""
    dbg(pdf)
    pdf.setFillColor(HexColor(AMBER)); pdf.rect(0,PAGE_H-38,PAGE_W,38,fill=1,stroke=0)
    pdf.setFillColor(HexColor("#fff")); pdf.setFont("Helvetica-Bold",15)
    pdf.drawString(M,PAGE_H-26,"PECUARIA — Gordo, Engorda e Reposicao")
    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7.5)
    pdf.drawRightString(PAGE_W-M,PAGE_H-15,f"AgriMacro v3.2 | {TODAY_BR} ({WDAY})")
    pdf.setFillColor(HexColor("#ffffffcc")); pdf.setFont("Helvetica",8)
    pdf.drawString(M,PAGE_H-12,"Boi gordo, boi de engorda, spread de reposicao e arbitragem Brasil vs EUA")
    y = PAGE_H - 55
    # Cards
    le = last_close("LE", pr); gf = last_close("GF", pr)
    le_d = pchg("LE", pr, 1); gf_d = pchg("GF", pr, 1)
    le_m = pchg("LE", pr, 21); gf_m = pchg("GF", pr, 21)
    intl = phys.get("international", {})
    le_br = intl.get("LE_BR", {})
    fx = get_brl_usd(bcb)
    cw = (PAGE_W - 2*M - 30) / 4
    cards = []
    if le_br and le_br.get("price"):
        cards.append(("Boi Gordo BR", f"R$ {le_br['price']}", f"Tendencia: {le_br.get('trend','')}", GREEN, True))
    if le:
        cards.append(("Boi Gordo Chicago", f"{le:,.2f} c/lb", f"Dia: {chg_str(le_d)} | Mes: {chg_str(le_m)}", AMBER, False))
    if gf:
        cards.append(("Boi Engorda Chicago", f"{gf:,.2f} c/lb", f"Dia: {chg_str(gf_d)} | Mes: {chg_str(gf_m)}", CYAN, False))
    if le and gf and gf > 0:
        spread = le - gf; ratio = le / gf
        cards.append(("Spread Gordo-Magro", f"{spread:,.2f} c/lb", f"Ratio: {ratio:.2f}x", PURPLE, False))
    for i, (title, val, sub, clr, is_br) in enumerate(cards[:4]):
        x = M + i*(cw+10)
        panel(pdf, x, y-65, cw, 67, bc=clr)
        if is_br: origin_badge(pdf, x+10, y-10, is_brazil=True)
        else: origin_badge(pdf, x+10, y-10, is_brazil=False)
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7); pdf.drawString(x+60, y-10, title)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",16); pdf.drawString(x+10, y-36, val[:20])
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",7); pdf.drawString(x+10, y-52, sub[:42])
    y -= 80
    # Graficos
    if img_cattle:
        pdf.drawImage(img_cattle, 5, y-200, width=PAGE_W-10, height=195, preserveAspectRatio=True, mask="auto")
        y -= 210
    # Arbitragem Boi BR vs Chicago
    if le and le_br and le_br.get("price") and fx:
        chi_brl = chicago_to_brl("LE", le, fx)
        if chi_brl:
            br_val = float(le_br["price"])
            spread_brl = br_val - chi_brl
            spread_pct = (spread_brl / chi_brl) * 100
            comp_w = (PAGE_W - 2*M - 15) / 2
            panel(pdf, M, y-55, comp_w, 57, bc=GREEN)
            pdf.setFillColor(HexColor(GREEN)); pdf.setFont("Helvetica-Bold",10)
            pdf.drawString(M+10, y-14, "ARBITRAGEM: BOI GORDO BRASIL vs CHICAGO")
            pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",8.5)
            pdf.drawString(M+10, y-28, f"Brasil (CEPEA): R$ {br_val:.2f}/@ | Chicago convertido: R$ {chi_brl:.2f}/@")
            sc = GREEN if spread_pct > 0 else RED
            pdf.setFillColor(HexColor(sc)); pdf.setFont("Helvetica-Bold",12)
            direction = "PREMIO BR" if spread_pct > 0 else "DESCONTO BR"
            pdf.drawString(M+10, y-44, f"{direction}: {spread_pct:+.1f}% (R$ {spread_brl:+.2f}/@)")
            # Explicacao lado direito
            panel(pdf, M+comp_w+15, y-55, comp_w, 57, bc=AMBER)
            pdf.setFillColor(HexColor(AMBER)); pdf.setFont("Helvetica-Bold",9)
            pdf.drawString(M+comp_w+25, y-14, "SPREAD DE REPOSICAO (Gordo - Magro)")
            if le and gf:
                spread_rep = le - gf
                sc2 = GREEN if spread_rep > 0 else RED
                pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",8.5)
                pdf.drawString(M+comp_w+25, y-28, f"Gordo: {le:.2f} c/lb | Engorda: {gf:.2f} c/lb")
                pdf.setFillColor(HexColor(sc2)); pdf.setFont("Helvetica-Bold",12)
                pdf.drawString(M+comp_w+25, y-44, f"Spread: {spread_rep:+.2f} c/lb")
            y -= 65
    # Explicacoes praticas
    if y > 80:
        pdf.setFillColor(HexColor(PURPLE)); pdf.setFont("Helvetica-Bold",11)
        pdf.drawString(M, y, "O QUE ISSO SIGNIFICA PRA VOCE"); y -= 16
        exp_w = (PAGE_W - 2*M - 20) / 3
        exps = [
            (GREEN, "PECUARISTA DE CRIA", "Boi magro (engorda) caro = boa hora pra vender bezerro. Se o spread gordo-magro apertou, confinamento ta sofrendo."),
            (AMBER, "CONFINADOR", "Lucro = boi gordo - boi magro - milho - ração. Se gordo nao acompanha o magro, margem aperta. Fique de olho no milho tambem."),
            (CYAN, "EXPORTADOR", "Se boi BR tem premio sobre Chicago, mercado interno paga mais. Se tem desconto, exportacao fica atrativa (carne processada)."),
        ]
        for i, (clr, title, desc) in enumerate(exps):
            cx = M + i * (exp_w + 10)
            panel(pdf, cx, y-58, exp_w, 60, bc=clr)
            pdf.setFillColor(HexColor(clr)); pdf.setFont("Helvetica-Bold",8); pdf.drawString(cx+10, y-12, title)
            tblock(pdf, cx+10, y-26, desc, sz=7, clr=TEXT_MUT, mw=exp_w-22, ld=9)
    pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6.5)
    pdf.drawString(M, 22, "Fontes: CEPEA/ESALQ, CME Group, BCB | Boi Gordo BR = indicador B3/ESALQ, Chicago = Live Cattle CME")


# ── PAGE 14: MERCADO FISICO (sem dados vazios) ──
def pg_physical(pdf, phys, img_phbr):
    dbg(pdf); hdr(pdf,"Mercado Fisico — Preco a Vista no Brasil e no Mundo",
                  "Precos reais de venda (CEPEA/ESALQ + origens internacionais)",
                  "Compare o preco fisico com o futuro para achar oportunidades. Diferenca grande = base forte.")
    intl=phys.get("international",{})
    if img_phbr: pdf.drawImage(img_phbr,5,PAGE_H-260,width=PAGE_W-10,height=195,preserveAspectRatio=True,mask="auto")
    y=PAGE_H-275
    pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",9)
    pdf.drawString(M,y,"PRECOS DISPONIVEIS"); y-=14
    cols4=[M,M+140,M+230,M+310,M+410,M+530]
    hdrs5=["Origem","Preco","Unidade","Variacao","Fonte","Data"]
    pdf.setFont("Helvetica-Bold",7); pdf.setFillColor(HexColor(TEXT_MUT))
    for j,h in enumerate(hdrs5): pdf.drawString(cols4[j],y,h)
    y-=3; pdf.setStrokeColor(HexColor(BORDER)); pdf.setLineWidth(0.3); pdf.line(M,y,PAGE_W-M,y); y-=10
    for key,d in intl.items():
        if y<30: break
        price=d.get("price","")
        # SKIP entries without real data
        if not price or str(price) == "None" or str(price) == "":
            continue
        label=d.get("label","").encode("ascii","ignore").decode()
        if not label: label=key
        unit=d.get("price_unit","")
        trend=d.get("trend",""); src=d.get("source",""); per=d.get("period","")
        # Skip if source says "Sem API" or trend is empty dash
        if "Sem API" in str(trend) or "Sem API" in str(src):
            continue
        # Skip international entries without variation data (confuse more than inform)
        is_br = "_BR" in key
        if not is_br and (not trend or str(trend).strip() in ("","—","N/A")):
            continue
        is_br = "_BR" in key
        origin_badge(pdf, cols4[0], y, is_brazil=is_br)
        sym_base = key.replace("_BR","").replace("_US","").replace("_AR","")
        nm_pt = NM_PROD.get(sym_base, label[:20])
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",7.5)
        pdf.drawString(cols4[0]+50,y,nm_pt[:18])
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",7.5)
        pdf.drawString(cols4[1],y,str(price))
        pdf.setFont("Helvetica",6.5); pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.drawString(cols4[2],y,str(unit)[:15])
        tc=GREEN if str(trend).startswith("+") else (RED if str(trend).startswith("-") else TEXT_MUT)
        pdf.setFillColor(HexColor(tc)); pdf.setFont("Helvetica-Bold",7)
        pdf.drawString(cols4[3],y,str(trend))
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6.5)
        pdf.drawString(cols4[4],y,str(src)[:30])
        pdf.drawString(cols4[5],y,str(per)[:12])
        y-=10


# ── PAGE 15: CLIMA ──
def pg_weather(pdf, weather_data):
    dbg(pdf); hdr(pdf,"Clima & Safra — Previsao 15 dias",
                  "Regioes agricolas chave no Brasil e no mundo",
                  "Chuva = bom para a lavoura. Seca prolongada = risco de quebra. Geada = perigo para cafe e trigo.")
    if not weather_data or not weather_data.get("regions"):
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",14)
        pdf.drawString(M,PAGE_H-120,"Dados climaticos nao disponiveis nesta execucao.")
        return
    regions = weather_data["regions"]
    enso = weather_data.get("enso", {})
    enso_status = enso.get("status", "N/A"); oni = enso.get("oni_value", "N/A")
    enso_txt = {"El Nino":"El Nino — tende a secar o Norte/Nordeste","La Nina":"La Nina — tende a secar o Sul","Neutral":"Neutro — sem influencia forte"}.get(enso_status, enso_status)
    enso_color = RED if enso_status == "El Nino" else (BLUE if enso_status == "La Nina" else AMBER)
    pdf.setFillColor(HexColor(PANEL)); pdf.roundRect(M,PAGE_H-100,PAGE_W-2*M,22,4,fill=1)
    pdf.setFillColor(HexColor(enso_color)); pdf.setFont("Helvetica-Bold",11)
    pdf.drawString(M+8,PAGE_H-95,f"Fenomeno ENSO: {enso_txt}")
    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",8)
    pdf.drawString(PAGE_W-M-120,PAGE_H-95,"Fonte: NOAA")
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
            pdf.drawString(x+8,bar_y+22,"Chuva prevista 7 dias:")
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


# ── PAGE 16: CALENDARIO ──
def pg_calendar(pdf, cal, rd, dr):
    dbg(pdf); hdr(pdf,"Calendario de Eventos — O que vem pela frente",
                  "Proximos releases que podem mexer com preco",
                  "Eventos marcados como ALTO IMPACTO movem preco diretamente (ex: WASDE, relatorio de estoques).")
    y=PAGE_H-78
    events=cal.get("events",[]) if isinstance(cal,dict) else (cal if isinstance(cal,list) else [])
    # Filter: only show today and future events
    future_events = []
    for ev in events:
        dt_str = ev.get("date",ev.get("release_date",""))[:10]
        try:
            if dt_str >= TODAY_STR: future_events.append(ev)
        except: future_events.append(ev)
    lw=PAGE_W/2-M-5
    pdf.setFillColor(HexColor(AMBER)); pdf.setFont("Helvetica-Bold",10)
    pdf.drawString(M,y,"PROXIMOS EVENTOS"); y2=y; y-=15
    if future_events:
        for ev in future_events[:12]:
            if y<50: break
            nm=ev.get("name",ev.get("event","")); dt=ev.get("date",ev.get("release_date",""))
            imp=ev.get("impact","")
            ic=RED if imp in ("HIGH","high") else (AMBER if imp in ("MEDIUM","medium") else TEXT_DIM)
            imp_pt = "ALTO" if imp in ("HIGH","high") else ("MEDIO" if imp in ("MEDIUM","medium") else "BAIXO")
            panel(pdf,M,y-15,lw,17,bc=ic)
            pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica",7.5)
            pdf.drawString(M+8,y-10,f"{dt[:10]}  {nm[:45]}")
            pdf.setFillColor(HexColor(ic)); pdf.setFont("Helvetica-Bold",6.5)
            pdf.drawString(M+lw-45,y-10,imp_pt)
            y-=19
    rx=PAGE_W/2+10; ry=y2
    pdf.setFillColor(HexColor(PURPLE)); pdf.setFont("Helvetica-Bold",10)
    pdf.drawString(rx,ry,"TEMAS PARA O VIDEO"); ry-=15
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
        pdf.drawString(M,fy,"MENSAGENS PARA O CAMPO"); fy-=14
        for d in dests[:4]:
            if fy<30: break
            imp=d.get("impacto_produtor",""); cm=d.get("commodity","")
            if imp:
                panel(pdf,M,fy-22,PAGE_W-2*M,24,bc=GREEN)
                pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",7.5)
                pdf.drawString(M+8,fy-14,f"[{NM_PROD.get(cm,cm)}]")
                tblock(pdf,M+50,fy-14,replace_tickers(imp[:170]),sz=7,clr=TEXT2,mw=PAGE_W-2*M-65,ld=10)
                fy-=28


# ── PAGE 17: NOTICIAS ──
def pg_news(pdf, news_data):
    dbg(pdf); hdr(pdf,"Noticias do Dia","O que saiu na imprensa do agro",
                  "Acompanhe as noticias que movem o mercado. Brasil a esquerda, mundo a direita.")
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
    origin_badge(pdf, M+60, PAGE_H-95, is_brazil=True)
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
    origin_badge(pdf, rx+120, PAGE_H-95, is_brazil=False)
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


# ── PAGE 18: GLOSSARIO ──
def pg_glossary(pdf):
    dbg(pdf); hdr(pdf,"Glossario — Termos Explicados","Se algum termo nao ficou claro, consulte aqui",
                  "Tudo em linguagem simples. Sem complicacao.")
    y = PAGE_H - 78
    terms = [
        ("Posicao dos Fundos","Relatorio semanal que mostra quem ta comprado (apostando na alta) e quem ta vendido (apostando na queda) nos mercados futuros. Quando os fundos compram muito, preco tende a subir."),
        ("Z-score","Mede se o valor esta normal ou fora do padrao. Acima de +2 = muito acima do normal. Abaixo de -2 = muito abaixo."),
        ("Percentil","Posicao relativa no ano. Percentil 10 = nos 10% mais baixos. Percentil 90 = nos 10% mais altos."),
        ("Spread","Diferenca entre dois precos. Ex: margem de esmagamento = quanto a industria lucra comprando soja e vendendo oleo + farelo."),
        ("Estoques-sobre-uso","Estoque dividido pelo consumo. Quanto menor, mais apertado o mercado e maior a pressao de alta."),
        ("Media de 20 dias","Media dos precos dos ultimos 20 dias. Linha amarela tracejada nos graficos. Preco acima da media = tendencia de alta."),
        ("Dolar (BRL/USD)","Cotacao do dolar em reais. Dolar alto = mais reais por saca exportada, mas insumo importado fica mais caro."),
        ("Selic","Taxa basica de juros do Brasil. Selic alta = credito caro, estocagem mais cara, produtor tende a vender mais rapido."),
        ("WASDE","Relatorio mensal do USDA (Departamento de Agricultura dos EUA) com estimativas de oferta e demanda mundial."),
        ("Mercado Fisico","Preco do produto entregue na mao, hoje. Diferente do futuro, que e um contrato pra entrega la na frente."),
        ("El Nino / La Nina","Fenomeno climatico que muda o padrao de chuvas. El Nino = seca no Norte/Nordeste. La Nina = seca no Sul."),
        ("Base","Diferenca entre preco fisico local e preco futuro de Chicago. Base forte = mercado local pagando mais que Chicago."),
        ("Sazonalidade","Padrao de preco que se repete todo ano. Ex: soja tende a subir de janeiro a maio e cair na colheita americana."),
        ("Esmagamento da Soja","Margem da industria que compra soja e vende oleo + farelo. Quando sobe, a industria paga mais pela soja em grao."),
        ("Margem do Confinamento","Conta do confinador: vende boi gordo, paga boi magro e milho. Positivo = lucro. Negativo = prejuizo."),
        ("ATR","Acucares Totais Recuperaveis - indice que mede qualidade da cana. Mais ATR = cana vale mais. Define pagamento ao fornecedor."),
        ("Chicago vs Brasil","Chicago = preco futuro internacional (referencia). Brasil = preco fisico local. Podem divergir por logistica, cambio e oferta."),
    ]
    for term, desc in terms:
        if y < 35: break
        h_needed = max(24, 14 + len(desc) // 90 * 10 + 10)
        panel(pdf, M, y - h_needed, PAGE_W - 2*M, h_needed, bc=PURPLE)
        pdf.setFillColor(HexColor(CYAN)); pdf.setFont("Helvetica-Bold", 8.5)
        pdf.drawString(M + 10, y - 12, term)
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica", 7.5)
        tblock(pdf, M + 10, y - 24, desc, sz=7.5, clr=TEXT_MUT, mw=PAGE_W - 2*M - 25, ld=10)
        y -= h_needed + 6


# ═══════════════════════════════════════════
#  BUILD
# ═══════════════════════════════════════════



# ── PÁGINA: AVISO LEGAL (DISCLOSURE) ──────────────────────────────────
def pg_disclosure(pdf):
    """Renderiza página de Aviso Legal completa."""
    if not HAS_DISCLOSURE:
        return

    W, H = landscape(A4)
    disc = get_disclosure_page()

    # Background dark
    pdf.setFillColor(HexColor("#1a1a2e"))
    pdf.rect(0, 0, W, H, fill=1, stroke=0)

    # Título
    pdf.setFillColor(HexColor("#e94560"))
    pdf.setFont("Helvetica-Bold", 22)
    pdf.drawString(50, H - 55, disc["title"])

    # Subtítulo
    pdf.setFillColor(HexColor("#aaaaaa"))
    pdf.setFont("Helvetica-Oblique", 11)
    pdf.drawString(50, H - 75, disc["subtitle"])

    # Linha separadora
    pdf.setStrokeColor(HexColor("#e94560"))
    pdf.setLineWidth(1)
    pdf.line(50, H - 85, W - 50, H - 85)

    # Seções — 2 colunas
    sections = disc["sections"]
    mid = (len(sections) + 1) // 2  # Dividir em 2 colunas
    col_width = (W - 120) / 2

    for col_idx, col_sections in enumerate([sections[:mid], sections[mid:]]):
        x_start = 50 + col_idx * (col_width + 20)
        y = H - 110

        for section in col_sections:
            # Heading
            pdf.setFillColor(HexColor("#e94560"))
            pdf.setFont("Helvetica-Bold", 10)
            pdf.drawString(x_start, y, section["heading"])
            y -= 14

            # Content — wrap text
            pdf.setFillColor(HexColor("#cccccc"))
            pdf.setFont("Helvetica", 7.5)

            text = section["content"]
            max_chars = int(col_width / 3.8)  # Approx chars per line at 7.5pt
            words = text.split()
            line = ""
            for word in words:
                test = line + " " + word if line else word
                if len(test) > max_chars:
                    pdf.drawString(x_start, y, line)
                    y -= 10
                    line = word
                    if y < 50:
                        break
                else:
                    line = test
            if line and y >= 50:
                pdf.drawString(x_start, y, line)
                y -= 10

            y -= 8  # Espaço entre seções

    # Footer
    pdf.setFillColor(HexColor("#666666"))
    pdf.setFont("Helvetica-Oblique", 6.5)
    footer = disc.get("footer", "")
    max_footer = int((W - 100) / 3.2)
    if len(footer) > max_footer:
        # Quebrar em 2 linhas
        mid_pt = footer.rfind(" ", 0, max_footer)
        if mid_pt > 0:
            pdf.drawString(50, 30, footer[:mid_pt])
            pdf.drawString(50, 20, footer[mid_pt+1:])
        else:
            pdf.drawString(50, 25, footer[:max_footer])
    else:
        pdf.drawString(50, 25, footer)

    # Versão
    pdf.setFillColor(HexColor("#444444"))
    pdf.setFont("Helvetica", 6)
    pdf.drawRightString(W - 50, 15, f"Disclosure v{disc.get('version', '1.0')} | {disc.get('last_updated', '')}")



# == BILATERAL INTELLIGENCE PAGE ============================================
def pg_bilateral(pdf, bilateral):
    dbg(pdf)
    hdr(pdf, 'Inteligencia Bilateral', 'US vs Brasil: competitividade, exportacoes e custo desembarcado',
        'Indicadores proprietarios AgriMacro que nenhum terminal Bloomberg ou DTN oferece.')
    y = PAGE_H - 75

    summary = bilateral.get('summary', {})
    lcs = bilateral.get('lcs', {})
    ert = bilateral.get('ert', {})
    bci = bilateral.get('bci', {})

    # === TOP STRIP: 3 headline numbers ===
    cw = (PAGE_W - 2*M - 20) / 3
    cards_data = []

    if summary.get('lcs_spread') is not None:
        sp = summary['lcs_spread']
        origin = summary.get('lcs_origin', '?')
        clr = GREEN if origin == 'BR' else RED
        cards_data.append(('Landed Cost Shanghai', f'${sp:+.0f}/mt', f'{origin} mais competitivo', clr))

    if summary.get('ert_leader') is not None:
        leader = summary['ert_leader']
        share = summary.get('ert_br_share', 0)
        clr = GREEN if leader == 'BR' else AMBER
        cards_data.append(('Corrida de Exportacao', f'{leader} lidera', f'BR {share:.0f}% do total BR+US', clr))

    if summary.get('bci_score') is not None:
        score = summary['bci_score']
        signal = summary.get('bci_signal', '?')
        clr = GREEN if score >= 60 else (AMBER if score >= 40 else RED)
        cards_data.append(('Indice Competitividade BR', f'{score:.0f}/100', signal, clr))

    for i, (title, val, sub, clr) in enumerate(cards_data):
        x = M + i * (cw + 10)
        panel(pdf, x, y - 60, cw, 62, bc=clr)
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont('Helvetica', 7.5); pdf.drawString(x + 10, y - 12, title)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont('Helvetica-Bold', 18); pdf.drawString(x + 10, y - 36, val)
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont('Helvetica', 7); pdf.drawString(x + 10, y - 52, sub[:42])

    y -= 82

    # === LEFT COLUMN: LCS Detail ===
    lx = M
    col_w = (PAGE_W - 2*M - 20) / 2

    if lcs.get('status') == 'OK':
        panel(pdf, lx, y - 180, col_w, 178)
        py = y - 14
        pdf.setFillColor(HexColor(AMBER)); pdf.setFont('Helvetica-Bold', 10)
        pdf.drawString(lx + 10, py, 'CUSTO DESEMBARCADO SHANGHAI (LCS)')
        py -= 18

        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont('Helvetica', 8)
        pdf.drawString(lx + 10, py, 'Rota EUA (Gulf > Shanghai):')
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont('Helvetica-Bold', 9)
        pdf.drawString(lx + 180, py, f'${lcs.get("us_landed", 0):.0f}/mt')
        py -= 14

        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont('Helvetica', 8)
        pdf.drawString(lx + 10, py, 'Rota Brasil (Santos > Shanghai):')
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont('Helvetica-Bold', 9)
        pdf.drawString(lx + 180, py, f'${lcs.get("br_landed", 0):.0f}/mt')
        py -= 18

        spread = lcs.get('spread_usd_mt', 0)
        sp_clr = GREEN if spread > 0 else RED
        pdf.setFillColor(HexColor(sp_clr)); pdf.setFont('Helvetica-Bold', 14)
        pdf.drawString(lx + 10, py, f'Spread: ${spread:+.2f}/mt')
        py -= 14
        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont('Helvetica', 7)
        pdf.drawString(lx + 10, py, f'Origem mais competitiva: {lcs.get("competitive_origin", "?")}')
        py -= 18

        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont('Helvetica', 7.5)
        fob_sp = lcs.get('fob_spread', 0)
        ocean_adv = lcs.get('ocean_advantage', 0)
        pdf.drawString(lx + 10, py, f'FOB Spread: ${fob_sp:+.0f}/mt  |  Vantagem Frete Maritimo: ${ocean_adv:+.0f}/mt')
        py -= 12
        ptax_v = lcs.get('ptax', 0)
        cbot_v = lcs.get('cbot_cents_bu', 0)
        pdf.drawString(lx + 10, py, f'CBOT: {cbot_v:.0f} cents/bu  |  PTAX: R$ {ptax_v:.2f}')

    # === RIGHT COLUMN: BCI Components ===
    rx = M + col_w + 20

    if bci.get('status') == 'OK':
        panel(pdf, rx, y - 180, col_w, 178)
        py = y - 14
        pdf.setFillColor(HexColor(AMBER)); pdf.setFont('Helvetica-Bold', 10)
        pdf.drawString(rx + 10, py, 'COMPONENTES BCI (0-100)')
        py -= 20

        comps = bci.get('components', [])
        for comp in sorted(comps, key=lambda c: c.get('score', 0) * c.get('weight_pct', 0) / 100, reverse=True):
            name = comp.get('name', '?')
            score = comp.get('score', 0)
            weight = comp.get('weight_pct', 0)
            signal = comp.get('signal', 'NEUTRAL')

            s_clr = GREEN if signal == 'BULLISH' else (RED if signal == 'BEARISH' else AMBER)

            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont('Helvetica', 7.5)
            pdf.drawString(rx + 10, py, name[:22])

            bar_x = rx + 130
            bar_w = col_w - 200
            pdf.setFillColor(HexColor(BORDER)); pdf.rect(bar_x, py - 2, bar_w, 8, fill=1, stroke=0)
            fill_w = bar_w * score / 100
            pdf.setFillColor(HexColor(s_clr)); pdf.rect(bar_x, py - 2, fill_w, 8, fill=1, stroke=0)

            pdf.setFillColor(HexColor(s_clr)); pdf.setFont('Helvetica-Bold', 7.5)
            pdf.drawString(bar_x + bar_w + 5, py, f'{score:.0f}')

            pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont('Helvetica', 6.5)
            pdf.drawString(bar_x + bar_w + 25, py, f'{weight}%')

            py -= 16

        py -= 5
        pdf.setFillColor(HexColor(GREEN)); pdf.setFont('Helvetica-Bold', 7.5)
        pdf.drawString(rx + 10, py, f'Mais forte: {bci.get("strongest", "?")}')
        pdf.setFillColor(HexColor(RED))
        pdf.drawString(rx + col_w/2, py, f'Mais fraco: {bci.get("weakest", "?")}')

    # === BOTTOM: Export Race ===
    y -= 200

    if ert.get('status') == 'OK':
        panel(pdf, M, y - 120, PAGE_W - 2*M, 118)
        py = y - 14
        pdf.setFillColor(HexColor(AMBER)); pdf.setFont('Helvetica-Bold', 10)
        pdf.drawString(M + 10, py, 'CORRIDA DE EXPORTACAO SOJA (MARKETING YEAR)')
        py -= 22

        us_ytd = ert.get('us_ytd_mmt', 0)
        us_pace = ert.get('us_pace_pct', 0)
        br_ytd = ert.get('br_ytd_mmt', 0)
        br_pace = ert.get('br_pace_pct', 0)

        bar_full = PAGE_W - 2*M - 120

        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont('Helvetica', 8)
        pdf.drawString(M + 10, py, f'EUA:  {us_ytd:.1f} MMT ({us_pace:.1f}%)')
        us_w = bar_full * min(us_pace, 100) / 100
        pdf.setFillColor(HexColor(AMBER)); pdf.rect(M + 110, py - 3, us_w, 12, fill=1, stroke=0)
        pdf.setFillColor(HexColor(BORDER)); pdf.rect(M + 110 + us_w, py - 3, bar_full - us_w, 12, fill=1, stroke=0)
        py -= 22

        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont('Helvetica', 8)
        pdf.drawString(M + 10, py, f'Brasil: {br_ytd:.1f} MMT ({br_pace:.1f}%)')
        br_w = bar_full * min(br_pace, 100) / 100
        pdf.setFillColor(HexColor(GREEN)); pdf.rect(M + 110, py - 3, br_w, 12, fill=1, stroke=0)
        pdf.setFillColor(HexColor(BORDER)); pdf.rect(M + 110 + br_w, py - 3, bar_full - br_w, 12, fill=1, stroke=0)
        py -= 22

        br_share = ert.get('br_market_share_pct', 0)
        us_share = ert.get('us_market_share_pct', 0)
        shift = ert.get('share_shift_pp', 0)

        pdf.setFillColor(HexColor(TEXT)); pdf.setFont('Helvetica-Bold', 8.5)
        pdf.drawString(M + 10, py, 'Market Share:')

        ms_x = M + 110
        us_ms_w = bar_full * us_share / 100
        br_ms_w = bar_full * br_share / 100
        pdf.setFillColor(HexColor(AMBER)); pdf.rect(ms_x, py - 3, us_ms_w, 14, fill=1, stroke=0)
        pdf.setFillColor(HexColor(GREEN)); pdf.rect(ms_x + us_ms_w, py - 3, br_ms_w, 14, fill=1, stroke=0)

        pdf.setFillColor(HexColor('#000')); pdf.setFont('Helvetica-Bold', 7)
        if us_share > 15:
            pdf.drawString(ms_x + 5, py, f'US {us_share:.0f}%')
        if br_share > 15:
            pdf.drawString(ms_x + us_ms_w + 5, py, f'BR {br_share:.0f}%')

        shift_clr = GREEN if shift > 0 else RED
        pdf.setFillColor(HexColor(shift_clr)); pdf.setFont('Helvetica', 7)
        pdf.drawRightString(PAGE_W - M - 10, py, f'vs 5yr: {shift:+.1f}pp')

    # Source
    pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont('Helvetica', 5.5)
    pdf.drawString(M, 22, 'Fontes: CEPEA/ESALQ, CBOT/CME, BCB/PTAX, USDA AMS GTR, Comex Stat/MDIC, IMEA | AgriMacro Intelligence (c) 2026')


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
    sabr = sload(DATA_PROC, "sugar_alcohol_br.json")  # Acucar & Alcool BR
    bilateral = sload(DATA_PROC, "bilateral_indicators.json")  # Bilateral Intelligence
    if not pr: print("  [ERRO] price_history.json nao encontrado!"); sys.exit(1)
    sk=list(pr.keys())[0]; sv=pr[sk]
    print(f"  [OK] prices: {len(pr)} symbols, {len(sv)} records" if isinstance(sv,list) else f"  [WARN] tipo: {type(sv)}")
    setup_mpl()

    # Generate charts
    print("  Graficos: commodities dedicadas...")
    comm_charts = {}
    for sym, title, slug, sub, accent in DEDICATED:
        print(f"    - {title} (preco + sazonalidade)...")
        comm_charts[sym] = {
            "main": chart_commodity_main(sym, pr, accent),
            "season": chart_seasonality(sym, pr, accent),
        }

    print("  Graficos: demais commodities (grade)...")
    img_others = chart_others_grid(OTHERS, pr)

    print("  Graficos: spreads...")
    img_sp = chart_spreads(sd)
    print("  Graficos: EIA...")
    img_eia = chart_eia(ed)
    print("  Graficos: posicao dos fundos...")
    img_cot = chart_cot(cd)
    print("  Graficos: estoques...")
    img_stocks = chart_stocks(sw)
    print("  Graficos: dolar...")
    img_brl = chart_macro_brl(bcb)
    print("  Graficos: fisico BR...")
    img_phbr = chart_physical_br(phys)
    print("  Graficos: acucar & alcool...")
    img_sugar = chart_sugar_alcohol(pr, ed)
    print("  Graficos: energia x sucroalcooleiro cruzado...")
    img_cross = chart_energy_sugar_cross(pr, ed)
    print("  Graficos: arbitragem BR vs Chicago...")
    arbs = calc_arbitrages(pr, phys, bcb)
    fx = get_brl_usd(bcb)
    img_arb = chart_arbitrage_bars(arbs, fx) if arbs else None
    print("  Graficos: pecuaria comparativa...")
    img_cattle = chart_cattle_compare(pr)
    print("  Graficos: spreads extras...")
    extra_sp = calc_extra_spreads(pr)
    img_sp_grid = chart_spreads_grid(sd, extra_sp)

    os.makedirs(REPORT_DIR, exist_ok=True)
    T = 22  # 18 originais + bilateral + arbitragem + pecuaria
    print(f"  Montando PDF ({T} paginas)...")
    pdf = canvas.Canvas(OUTPUT_PDF, pagesize=landscape(A4))
    pdf.setTitle(f"AgriMacro Diario - {TODAY_STR}"); pdf.setAuthor("AgriMacro v3.2")

    pn = 1
    # 1. Capa
    pg_cover(pdf,rd,dr,bcb,pr)
    # Disclaimer compacto no rodape da capa
    if HAS_DISCLOSURE:
        _dw = landscape(A4)[0]
        _dw = landscape(A4)[0]
        pdf.setFillColor(HexColor('#666666'))
        pdf.setFont('Helvetica', 5.5)
        _dt = get_cover_disclaimer()
        _mc = int((_dw - 100) / 2.8)
        if len(_dt) > _mc:
            _sp = _dt.rfind(' ', 0, _mc)
            if _sp > 0:
                pdf.drawString(50, 18, _dt[:_sp])
                pdf.drawString(50, 10, _dt[_sp+1:])
            else:
                pdf.drawString(50, 14, _dt[:_mc])
        else:
            pdf.drawString(50, 14, _dt)
    ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 2. Macro
    pg_macro(pdf,bcb,img_brl);                  ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 3-6. Commodity pages
    for sym, title, slug, sub, accent in DEDICATED:
        ch = comm_charts.get(sym, {})
        pg_commodity(pdf, sym, title, sub, accent, pr, cd, sw, phys, rd, dr,
                     ch.get("main"), ch.get("season"))
        ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 7. Others
    pg_others(pdf, pr, img_others);             ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 8. Variations
    pg_variations_table(pdf, pr);               ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 9. Spreads (redesenhado)
    pg_spreads(pdf, sd, img_sp);                ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 9B. Arbitragem BR vs Chicago (NOVA)
    pg_arbitrage(pdf, pr, phys, bcb, img_arb);  ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 10. Stocks
    pg_stocks(pdf, sw, img_stocks);             ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 11. COT
    pg_cot(pdf, cd, img_cot);                   ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 12. Energy
    pg_energy(pdf, ed, img_eia);                ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 13. Acucar & Alcool
    pg_sugar_alcohol(pdf, pr, ed, phys, bcb, img_sugar, sabr); ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 14. Sinergia Energia x Sucroalcooleiro (NOVA)
    pg_energy_sugar_cross(pdf, pr, ed, phys, bcb, img_cross); ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 15. Pecuaria Comparativa
    pg_cattle_compare(pdf, pr, phys, bcb, img_cattle); ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 15B. Bilateral Intelligence
    if bilateral and bilateral.get("summary", {}).get("bci_score") is not None:
        pg_bilateral(pdf, bilateral);             ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 16. Physical
    pg_physical(pdf, phys, img_phbr);           ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 16. Weather
    pg_weather(pdf, wt);                        ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 17. Calendar
    pg_calendar(pdf, cal, rd, dr);              ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 18. News
    pg_news(pdf, nw);                           ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 19. Glossary
    pg_glossary(pdf);                           ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 20. Aviso Legal (Disclosure)
    if HAS_DISCLOSURE:
        pg_disclosure(pdf);                       ftr(pdf,pn,T); pdf.showPage()

    pdf.save()
    sz=os.path.getsize(OUTPUT_PDF)/1024
    print(f"\n  PDF: {OUTPUT_PDF}"); print(f"  Tamanho: {sz:.0f} KB | Paginas: {T}")

if __name__=="__main__":
    print("="*60); print("AgriMacro v3.2 - Relatorio PDF Profissional v6"); print("="*60)
    build_pdf(); print("\nConcluido!")
