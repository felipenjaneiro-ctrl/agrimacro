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



# a"a" AA+QA Engine Gate a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"

try:

    from aa_qa_engine import run_audit as _run_qa_audit

    HAS_QA_ENGINE = True

except ImportError:

    HAS_QA_ENGINE = False

    print("a   aa_qa_engine.py nAo encontrado -- QA desabilitado")

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

from pg_grain_ratios import pg_grain_ratios



# a"a" Disclosure / Aviso Legal a"a"

try:

    from disclosure import get_cover_disclaimer, get_disclosure_page

    HAS_DISCLOSURE = True

except ImportError:

    HAS_DISCLOSURE = False

    print("a   disclosure.py nAo encontrado -- Disclosure desabilitado")





# a"a" CONFIG a"a"

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

    "ZS":"Ac/bu","ZC":"Ac/bu","ZW":"Ac/bu","ZM":"$/ton","ZL":"Ac/lb",

    "KC":"Ac/lb","SB":"Ac/lb","CT":"Ac/lb","CC":"$/ton","OJ":"Ac/lb",

    "LE":"Ac/lb","GF":"Ac/lb","HE":"Ac/lb","CL":"$/bbl","NG":"$/MMBtu",

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



# a"a" DATA HELPERS a"a"

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







# a"a" CONVERSAO & ARBITRAGEM a"a"

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





# a"a" MPL SETUP a"a"

def setup_mpl():

    plt.rcParams.update({"figure.facecolor":BG,"axes.facecolor":PANEL,"axes.edgecolor":BORDER,

        "axes.labelcolor":TEXT,"text.color":TEXT,"xtick.color":TEXT_MUT,"ytick.color":TEXT_MUT,

        "grid.color":BORDER,"grid.alpha":0.2,"font.size":7.5,"font.family":"sans-serif"})



def fig2img(fig, dpi=150):

    buf = BytesIO()

    fig.savefig(buf,format="png",dpi=dpi,bbox_inches="tight",facecolor=fig.get_facecolor(),edgecolor="none",pad_inches=0.08)

    plt.close(fig); buf.seek(0)

    return ImageReader(buf)





# a"a" REPORTLAB HELPERS a"a"

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





# a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*

#  CHARTS

# a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*



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

    ax.set_title(f"Preco Futuro ({origin}) -- Ultimos 60 dias", fontsize=10, color=TEXT, pad=5)

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

        # All data is current year a" use it as both

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

    fig.suptitle("TERMOMETRO DE RELACOES -- Onde cada spread esta hoje", fontsize=13, color=TEXT, fontweight="bold", y=0.99)

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

        # Formata numeros EIA a" v3.3: estoques ja vem em MBbl (mil barris)

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

        ax.text(0.5, -0.06, f"ATENCAO: {', '.join(suspects)} com desvio >50% -- possivel erro de unidade/serie. Verificar fonte.",

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

        label=d.get("label","").replace("\U0001f1e7\U0001f1f7 ","").split(" -- ")[-1]

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

    fig.suptitle("TERMOMETRO DE RELACOES -- Nivel 0-100 (verde=barato, vermelho=caro)", fontsize=11, color=TEXT, fontweight="bold", y=0.99)

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



# a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*

#  PAGE FUNCTIONS

# a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*



# a"a" PAGE 1: CAPA + RESUMO a"a"

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

    # Data date indicator — show when pipeline data is from
    data_date = ""
    if pr:
        _meta = pr.get("_meta", {})
        if _meta and _meta.get("generated_at"):
            data_date = _meta["generated_at"][:10]
        else:
            _any_sym = next((s for s in pr if s != "_meta" and isinstance(pr[s], list) and pr[s]), None)
            if _any_sym:
                data_date = pr[_any_sym][-1].get("date", "")
    if data_date and data_date != TODAY_STR:
        pdf.setFillColor(HexColor("#ff9900")); pdf.setFont("Helvetica", 7)
        pdf.drawRightString(rx, PAGE_H-82, f"Dados de mercado: {data_date}")

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





# a"a" PAGE 2: MACRO BRASIL a"a"

def pg_macro(pdf, bcb, img_brl, macro_ind=None, fw_data=None):

    dbg(pdf); hdr(pdf,"Cenario Macro Global + Brasil","Dolar, Juros, VIX, S&P, Fed -- o que mexe com o preco da sua safra",
                  "Dolar forte = mais reais por saca exportada. VIX alto = mercado nervoso. Fed corta juros = commodities sobem.")

    y=PAGE_H-75

    rc=bcb.get("resumo_cambio",{}); rj=bcb.get("resumo_juros",{})
    mi = macro_ind or {}
    sp = mi.get("sp500",{}); vix = mi.get("vix",{}); ty = mi.get("treasury_10y",{})
    fw = fw_data or {}

    # VIX level badge text
    vix_val = vix.get("value",0) or 0
    vix_lvl = vix.get("level","") or ""
    vix_clr = GREEN if vix_lvl=="baixo" else AMBER if vix_lvl=="normal" else RED

    cw=(PAGE_W-2*M-60)/7

    cards=[
        ("Dolar Hoje",f"R$ {rc.get('brl_usd_atual',0):.4f}",f"5d: {rc.get('var_5d',0):+.2f}%",AMBER),
        ("Selic",f"{rj.get('selic_atual',0):.1f}%","Taxa basica de juros",RED),
        ("S&P 500",f"{sp.get('price',0):,.0f}" if sp.get("price") else "--",f"Dia: {sp.get('change_pct',0):+.1f}% | Sem: {sp.get('change_week_pct',0):+.1f}%" if sp.get("price") else "",BLUE),
        ("VIX",f"{vix_val:.1f}" if vix_val else "--",f"Nivel: {vix_lvl.upper()}" if vix_lvl else "",vix_clr),
        ("Juros 10Y EUA",f"{ty.get('yield_pct',0):.3f}%" if ty.get("yield_pct") else "--",f"{ty.get('direction','')}, {ty.get('change_bps',0):+.0f} bps" if ty.get("yield_pct") else "",CYAN),
        ("Dolar Min Ano",f"R$ {rc.get('min_52s',0):.4f}","52 semanas",GREEN),
        ("Dolar Max Ano",f"R$ {rc.get('max_52s',0):.4f}","52 semanas",RED),
    ]

    for i,(title,val,sub,clr) in enumerate(cards):

        x=M+i*(cw+10)

        panel(pdf,x,y-60,cw,62,bc=clr)

        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",6.5); pdf.drawString(x+6,y-12,title[:16])

        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",14); pdf.drawString(x+6,y-34,val[:14])

        pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6); pdf.drawString(x+6,y-50,sub[:22])

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

    # FedWatch section
    if fw.get("probabilities"):
        ry -= 10
        pdf.setFillColor(HexColor("#DCB432")); pdf.setFont("Helvetica-Bold", 9)
        pdf.drawString(rx, ry, "FEDWATCH -- Expectativa de Juros EUA"); ry -= 16
        fp = fw["probabilities"]
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica", 7.5)
        rate = fw.get("current_rate", 0)
        pdf.drawString(rx+10, ry, f"Taxa atual: {rate:.2f}%  |  Proximo FOMC: {fw.get('next_meeting','?')}"); ry -= 14
        exp = fw.get("market_expectation", "hold")
        exp_clr = "#DCB432" if exp == "hold" else GREEN if exp == "cut" else RED
        pdf.setFillColor(HexColor(exp_clr)); pdf.setFont("Helvetica-Bold", 8)
        pdf.drawString(rx+10, ry, f"Expectativa: {exp.upper()}"); ry -= 16
        # Probability bars
        for lbl, val, clr in [("Manter", fp.get("hold",0), "#DCB432"), ("Corte 25bp", fp.get("cut_25bps",0), GREEN), ("Alta 25bp", fp.get("hike_25bps",0), RED)]:
            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica", 7)
            pdf.drawString(rx+10, ry, lbl)
            bar_x = rx + 80; bar_w = 120; bar_h = 7
            pdf.setFillColor(HexColor(PANEL)); pdf.rect(bar_x, ry-1, bar_w, bar_h, fill=1, stroke=0)
            if val and val > 0:
                pdf.setFillColor(HexColor(clr)); pdf.rect(bar_x, ry-1, bar_w*val/100, bar_h, fill=1, stroke=0)
            pdf.setFillColor(HexColor(clr)); pdf.setFont("Helvetica-Bold", 7)
            pdf.drawString(bar_x+bar_w+5, ry, f"{val:.0f}%")
            ry -= 12




# a"a" PAGES 3-6: DEDICATED COMMODITY PAGES a"a"

def pg_commodity(pdf, sym, title, subtitle, accent, pr, cd, sw, phys, rd, dr, img_main, img_season):

    """Dedicated page for a major commodity"""

    dbg(pdf)

    # Custom header with accent color

    pdf.setFillColor(HexColor(accent)); pdf.rect(0,PAGE_H-34,PAGE_W,34,fill=1,stroke=0)

    pdf.setFillColor(HexColor("#fff")); pdf.setFont("Helvetica-Bold",15)

    pdf.drawString(M,PAGE_H-24,f"{title.upper()}")

    pdf.setFillColor(HexColor("#ffffffcc")); pdf.setFont("Helvetica",9)

    pdf.drawString(M+pdf.stringWidth(f"{title.upper()} ","Helvetica-Bold",15)+10,PAGE_H-24,f"--  {subtitle}")

    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7.5)

    pdf.drawRightString(PAGE_W-M,PAGE_H-15,f"AgriMacro v3.2 | {TODAY_BR} ({WDAY})")



    # a"a" LEFT SIDE: Main chart + Seasonality a"a"

    chart_w = PAGE_W * 0.58

    if img_main:

        pdf.drawImage(img_main, M, PAGE_H-275, width=chart_w-M-10, height=230, preserveAspectRatio=True, mask="auto")

    if img_season:

        pdf.drawImage(img_season, M, PAGE_H-480, width=chart_w*0.55, height=190, preserveAspectRatio=True, mask="auto")



    # a"a" RIGHT SIDE: Stats + COT + Physical + Interpretation a"a"

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

        pdf.drawString(rx+10, y-26, f"{pva:+.1f}% -- {state_nm}")

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





# a"a" PAGE 7: DEMAIS COMMODITIES a"a"

def pg_others(pdf, pr, img_grid):

    dbg(pdf); hdr(pdf,"Demais Commodities","Trigo, Acucar, Algodao, Cacau, Porco, Energia, Metais e mais",

                  "Graficos de 60 dias. Verde = subindo, vermelho = caindo. Preco atual no canto esquerdo.")

    if img_grid:

        pdf.drawImage(img_grid, 5, PAGE_H-460, width=PAGE_W-10, height=385, preserveAspectRatio=True, mask="auto")





# a"a" PAGE 8: TABELA DE VARIACOES a"a"

def pg_variations_table(pdf, pr):

    dbg(pdf); hdr(pdf,"Tabela de Variacoes","Todas as commodities -- variacao diaria, semanal, mensal e posicao no ano",

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





# a"a" PAGE 9: SPREADS a"a"

def pg_spreads(pdf, sd, img_sp):

    dbg(pdf)

    # Header especial - mais destaque

    pdf.setFillColor(HexColor("#2d1f6e")); pdf.rect(0,PAGE_H-38,PAGE_W,38,fill=1,stroke=0)

    pdf.setFillColor(HexColor("#fff")); pdf.setFont("Helvetica-Bold",15)

    pdf.drawString(M,PAGE_H-26,"RELACOES DE PRECO -- O que esta barato e caro entre si")

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









# a"a" PAGE 9B: ARBITRAGEM BR vs CHICAGO a"a"

def pg_arbitrage(pdf, pr, phys, bcb, img_arb):

    """Pagina dedicada a arbitragem Brasil vs Chicago"""

    dbg(pdf)

    pdf.setFillColor(HexColor("#1a2040")); pdf.rect(0,PAGE_H-38,PAGE_W,38,fill=1,stroke=0)

    pdf.setFillColor(HexColor(CYAN)); pdf.setFont("Helvetica-Bold",15)

    pdf.drawString(M,PAGE_H-26,"ARBITRAGEM -- Brasil vs Chicago em Reais")

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





# a"a" PAGE 10: ESTOQUES a"a"

def pg_stocks(pdf, sw, img_stocks, crop_prog=None):

    dbg(pdf); hdr(pdf,"Estoques + Progresso de Safra EUA",

                  "Estoques USDA vs media 5 anos + plantio/colheita semanal NASS",

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

        (RED, "Muito acima da media (>15%)", "Sobra de produto -- preco tende a cair"),

        (AMBER, "Acima da media (5-15%)", "Oferta confortavel"),

        (TEXT_MUT, "Normal (-5% a +5%)", "Equilibrio entre oferta e demanda"),

        (BLUE, "Abaixo da media (-5% a -15%)", "Estoque apertando -- fique atento"),

        (CYAN, "Muito abaixo (<-15%)", "Faltando produto -- preco tende a subir"),

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

    # Crop Progress section
    cp = crop_prog
    if cp and not cp.get("is_fallback") and cp.get("crops"):
        y -= 10
        pdf.setFillColor(HexColor("#DCB432")); pdf.setFont("Helvetica-Bold", 9)
        pdf.drawString(M, y, "PROGRESSO DE SAFRA EUA (USDA NASS)"); y -= 16
        crop_names = {"CORN":"Milho","SOYBEANS":"Soja","WHEAT_WINTER":"Trigo Inverno","COTTON":"Algodao"}
        cols_cp = [M+10, M+100, M+180, M+280]
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica-Bold", 7)
        pdf.drawString(cols_cp[0], y, "Cultura"); pdf.drawString(cols_cp[1], y, "Nacional")
        pdf.drawString(cols_cp[2], y, "Estagio"); pdf.drawString(cols_cp[3], y, "Semana"); y -= 12
        for ck, cd_cr in cp["crops"].items():
            nat = cd_cr.get("national", {})
            if not nat: continue
            name = crop_names.get(ck, ck)
            week = cd_cr.get("week_ending", "")
            stages_order = ["harvested","mature","dented","dough","silking","dropping_leaves","setting_pods","blooming","headed","squaring","setting_bolls","emerged","planted"]
            st_name = ""; st_val = 0
            for st in stages_order:
                if st in nat and nat[st] is not None:
                    st_name = st.replace("_"," ").title(); st_val = nat[st]; break
            pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica", 7.5)
            pdf.drawString(cols_cp[0], y, name)
            pdf.setFont("Helvetica-Bold", 7.5); pdf.drawString(cols_cp[1], y, f"{st_val:.0f}%")
            pdf.setFont("Helvetica", 7); pdf.drawString(cols_cp[2], y, st_name)
            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.drawString(cols_cp[3], y, week)
            y -= 11


# a"a" PAGE 11: COT a"a"

def pg_cot(pdf, cd, img_cot):

    dbg(pdf); hdr(pdf,"Posicao dos Fundos -- Quem ta apostando em que?",

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





# a"a" PAGE 12: ENERGIA a"a"

def pg_energy(pdf, ed, img_eia):

    dbg(pdf); hdr(pdf,"Energia -- Diesel, Gas e Etanol",

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







# a"a" PAGE 13: ACUCAR & ALCOOL (NOVA!) a"a"

def pg_sugar_alcohol(pdf, pr, ed, phys, bcb, img_sugar, sabr=None):

    """Pagina dedicada ao setor sucroalcooleiro"""

    dbg(pdf)

    pdf.setFillColor(HexColor("#1a3a1a")); pdf.rect(0,PAGE_H-38,PAGE_W,38,fill=1,stroke=0)

    pdf.setFillColor(HexColor(GREEN)); pdf.setFont("Helvetica-Bold",15)

    pdf.drawString(M,PAGE_H-26,"ACUCAR & ALCOOL -- Setor Sucroalcooleiro")

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









# a"a" PAGE: SINERGIA ENERGIA x SUCROALCOOLEIRO a"a"

def pg_energy_sugar_cross(pdf, pr, ed, phys, bcb, img_cross):

    """Pagina de dados cruzados entre energia e setor sucroalcooleiro"""

    dbg(pdf)

    pdf.setFillColor(HexColor("#1a1a3a")); pdf.rect(0,PAGE_H-38,PAGE_W,38,fill=1,stroke=0)

    pdf.setFillColor(HexColor(AMBER)); pdf.setFont("Helvetica-Bold",14)

    pdf.drawString(M,PAGE_H-26,"SINERGIA: ENERGIA x SUCROALCOOLEIRO")

    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",8)

    pdf.drawString(M,PAGE_H-12,"Dados cruzados: petroleo, acucar, etanol, diesel, gas -- como se conectam")

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

    # a"a" Cards de resumo cruzado --a"

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

    # a"a" Graficos 2x3 a"a"

    if img_cross:

        pdf.drawImage(img_cross, 5, y-250, width=PAGE_W-10, height=245, preserveAspectRatio=True, mask="auto")

        y -= 260

    # a"a" Painel de correlacoes e impactos a"a"

    pdf.setFillColor(HexColor(PURPLE)); pdf.setFont("Helvetica-Bold",11)

    pdf.drawString(M, y, "COMO TUDO SE CONECTA -- Cadeia de impactos"); y -= 14

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

    # a"a" Paridade Etanol/Gasolina e Spread Acucar a"a"

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

            pdf.drawString(M+10, y-42, f"Paridade: {paridade:.0f}% -- {veredito}")

            pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6.5)

            pdf.drawString(M+10, y-52, f"Etanol: R$ {eth_p:.4f}/L | Gasolina ref: ~R$ {gas_est:.2f}/L (media nacional estimada)")

        else:

            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",8)

            pdf.drawString(M+10, y-42, "Dados de etanol BR indisponiveis -- rode collect_physical_intl.py")

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





# a"a" PAGE 14: PECUARIA COMPARATIVA a"a"

def pg_cattle_compare(pdf, pr, phys, bcb, img_cattle):

    """Pagina dedicada: Boi Gordo vs Engorda, Brasil vs Chicago"""

    dbg(pdf)

    pdf.setFillColor(HexColor(AMBER)); pdf.rect(0,PAGE_H-38,PAGE_W,38,fill=1,stroke=0)

    pdf.setFillColor(HexColor("#fff")); pdf.setFont("Helvetica-Bold",15)

    pdf.drawString(M,PAGE_H-26,"PECUARIA -- Gordo, Engorda e Reposicao")

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

            (AMBER, "CONFINADOR", "Lucro = boi gordo - boi magro - milho - raAAo. Se gordo nao acompanha o magro, margem aperta. Fique de olho no milho tambem."),

            (CYAN, "EXPORTADOR", "Se boi BR tem premio sobre Chicago, mercado interno paga mais. Se tem desconto, exportacao fica atrativa (carne processada)."),

        ]

        for i, (clr, title, desc) in enumerate(exps):

            cx = M + i * (exp_w + 10)

            panel(pdf, cx, y-58, exp_w, 60, bc=clr)

            pdf.setFillColor(HexColor(clr)); pdf.setFont("Helvetica-Bold",8); pdf.drawString(cx+10, y-12, title)

            tblock(pdf, cx+10, y-26, desc, sz=7, clr=TEXT_MUT, mw=exp_w-22, ld=9)

    pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica",6.5)

    pdf.drawString(M, 22, "Fontes: CEPEA/ESALQ, CME Group, BCB | Boi Gordo BR = indicador B3/ESALQ, Chicago = Live Cattle CME")





# a"a" PAGE 14: MERCADO FISICO (sem dados vazios) a"a"

def pg_physical(pdf, phys, img_phbr):

    dbg(pdf); hdr(pdf,"Mercado Fisico -- Preco a Vista no Brasil e no Mundo",

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

        if not is_br and (not trend or str(trend).strip() in ("","--","N/A")):

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





# a"a" PAGE 15: CLIMA a"a"

def pg_weather(pdf, weather_data, gtrends=None):

    dbg(pdf); hdr(pdf,"Clima, ENSO & Sentimento de Busca",

                  "Regioes agricolas + fenomeno ENSO + Google Trends como indicador antecedente",

                  "Chuva = bom para lavoura. Seca = risco de quebra. Spike de busca = mercado atento.")

    if not weather_data or not weather_data.get("regions"):

        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",14)

        pdf.drawString(M,PAGE_H-120,"Dados climaticos nao disponiveis nesta execucao.")

        return

    regions = weather_data["regions"]

    enso = weather_data.get("enso", {})

    enso_status = enso.get("status", "N/A"); oni = enso.get("oni_value", "N/A")

    enso_txt = {"El Nino":"El Nino -- tende a secar o Norte/Nordeste","La Nina":"La Nina -- tende a secar o Sul","Neutral":"Neutro -- sem influencia forte"}.get(enso_status, enso_status)

    enso_color = RED if enso_status == "El Nino" else (BLUE if enso_status == "La Nina" else AMBER)

    pdf.setFillColor(HexColor(PANEL)); pdf.roundRect(M,PAGE_H-100,PAGE_W-2*M,22,4,fill=1)

    pdf.setFillColor(HexColor(enso_color)); pdf.setFont("Helvetica-Bold",11)

    oni_str = f" (ONI: {oni})" if oni and oni != "N/A" else ""
    pdf.drawString(M+8,PAGE_H-95,f"Fenomeno ENSO: {enso_txt}{oni_str}")

    pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",8)

    pdf.drawString(PAGE_W-M-120,PAGE_H-95,"Fonte: NOAA CPC")

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

    # Google Trends section
    gt = gtrends
    if gt and not gt.get("is_fallback") and gt.get("trends"):
        trends = gt["trends"]
        spikes = gt.get("spikes", [])
        # Position at bottom-right area
        gx = PAGE_W/2 + 20; gy = 120
        pdf.setFillColor(HexColor("#DCB432")); pdf.setFont("Helvetica-Bold", 9)
        pdf.drawString(gx, gy, "GOOGLE TRENDS -- Indicador de Sentimento"); gy -= 14
        if spikes:
            pdf.setFillColor(HexColor(RED)); pdf.setFont("Helvetica-Bold", 7)
            pdf.drawString(gx+10, gy, f"SPIKES: {', '.join(spikes[:3])}"); gy -= 12
        sorted_t = sorted(trends.items(), key=lambda x: x[1].get("current",0), reverse=True)
        for term, data in sorted_t[:5]:
            cur = data.get("current", 0)
            direction = data.get("direction", "")
            is_spike = term in spikes
            clr_t = RED if is_spike else (GREEN if direction == "subindo" else TEXT_MUT)
            pdf.setFillColor(HexColor(clr_t)); pdf.setFont("Helvetica" + ("-Bold" if is_spike else ""), 7)
            pdf.drawString(gx+10, gy, f"{term}: {cur}")
            # Mini bar
            bar_x = gx + 150; bar_w = 80
            pdf.setFillColor(HexColor(PANEL)); pdf.rect(bar_x, gy-1, bar_w, 6, fill=1, stroke=0)
            pdf.setFillColor(HexColor(clr_t)); pdf.rect(bar_x, gy-1, bar_w*min(cur,100)/100, 6, fill=1, stroke=0)
            gy -= 10



# a"a" PAGE 16: CALENDARIO a"a"

def pg_calendar(pdf, cal, rd, dr):

    dbg(pdf); hdr(pdf,"Calendario de Eventos -- O que vem pela frente",

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





# a"a" PAGE 17: NOTICIAS a"a"

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





# -- PAGE: COUNCIL AGRIMACRO --
def pg_council(pdf, synthesis, corr, spr, macro_i, fw):
    dbg(pdf); hdr(pdf, "COUNCIL AGRIMACRO", "5 perspectivas + sintese do Chairman",
                  "Gerado via Claude API com dados reais do pipeline.")
    y = PAGE_H - 78
    council = None
    try:
        import urllib.request as _ur
        api_key = ""
        _kp = os.path.join(os.path.expanduser("~"), ".anthropic_key")
        if os.path.exists(_kp):
            with open(_kp) as _f: api_key = _f.read().strip()
        if not api_key:
            _ep = os.path.join(PROJ_DIR, ".env")
            if os.path.exists(_ep):
                for _ln in open(_ep, encoding="utf-8"):
                    if _ln.strip().startswith("ANTHROPIC_API_KEY="):
                        api_key = _ln.strip().split("=",1)[1]
        if api_key:
            ctx = []
            if synthesis and not synthesis.get("is_fallback"):
                ctx.append("SINTESE: " + (synthesis.get("summary","") or "")[:500])
                for s in (synthesis.get("priority_high") or [])[:3]:
                    ctx.append("- " + s.get("title",""))
            if corr and not corr.get("is_fallback"):
                for cs in (corr.get("composite_signals") or [])[:4]:
                    ctx.append(f"{cs['asset']}: {cs['signal']} ({cs['confidence']*100:.0f}%)")
            if spr and spr.get("spreads"):
                for k,v in spr["spreads"].items():
                    if v.get("regime") == "EXTREMO":
                        ctx.append(f"Spread {v['name']}: z={v.get('zscore_1y',0):.1f}")
            if macro_i and macro_i.get("vix"):
                ctx.append(f"VIX: {macro_i['vix'].get('value',0)} ({macro_i['vix'].get('level','')})")
            if fw and fw.get("market_expectation"):
                ctx.append(f"Fed: {fw['market_expectation']}")
            prompt = ("Voce e o Council AgriMacro. Gere EXATAMENTE este JSON (sem markdown):\n"
                '{"contrarian":"(2 frases)","first_principles":"(2 frases)",'
                '"expansionist":"(2 frases)","outsider":"(2 frases)",'
                '"executor":"(2 frases)","chairman":"(3 frases sintese final)"}\n\nDADOS:\n'
                + "\n".join(ctx))
            pl = json.dumps({"model":"claude-sonnet-4-20250514","max_tokens":1200,
                "messages":[{"role":"user","content":prompt}]}).encode("utf-8")
            rq = _ur.Request("https://api.anthropic.com/v1/messages", data=pl, method="POST",
                headers={"Content-Type":"application/json","x-api-key":api_key,"anthropic-version":"2023-06-01"})
            with _ur.urlopen(rq, timeout=30) as rsp:
                res = json.loads(rsp.read().decode("utf-8"))
            raw = ""
            for blk in res.get("content",[]):
                if blk.get("type")=="text": raw += blk.get("text","")
            raw = raw.strip()
            if raw.startswith("```"): raw = "\n".join(raw.split("\n")[1:])
            if raw.endswith("```"): raw = "\n".join(raw.split("\n")[:-1])
            council = json.loads(raw.strip())
            print("    Council API: OK")
    except Exception as _e:
        print(f"    Council API: {_e}")
    if not council:
        council = {"contrarian":"Indisponivel","first_principles":"","expansionist":"",
            "outsider":"","executor":"","chairman":"Execute o pipeline completo."}
    PERSP = [("contrarian","CONTRARIAN","#DC3C3C"),("first_principles","FIRST PRINCIPLES","#3b82f6"),
        ("expansionist","EXPANSIONIST","#22c55e"),("outsider","OUTSIDER","#a855f7"),
        ("executor","EXECUTOR","#f59e0b")]
    cw = (PAGE_W - 2*M - 4*8) / 5
    for i,(key,lbl,clr) in enumerate(PERSP):
        cx = M + i*(cw+8); ch = 160
        panel(pdf, cx, y-ch, cw, ch)
        pdf.setFillColor(HexColor(clr)); pdf.rect(cx, y-ch, 4, ch, fill=1, stroke=0)
        pdf.setFillColor(HexColor(clr)); pdf.setFont("Helvetica-Bold",8); pdf.drawString(cx+10, y-14, lbl)
        txt = council.get(key,"")
        if txt: tblock(pdf, cx+10, y-36, txt, sz=7, clr=TEXT, mw=cw-20, ld=10)
    y -= 180
    ch_txt = council.get("chairman","")
    if ch_txt:
        bh = 70
        pdf.setFillColor(HexColor(PANEL)); pdf.rect(M, y-bh, PAGE_W-2*M, bh, fill=1, stroke=0)
        pdf.setStrokeColor(HexColor("#DCB432")); pdf.setLineWidth(2)
        pdf.rect(M, y-bh, PAGE_W-2*M, bh, fill=0, stroke=1)
        pdf.setFillColor(HexColor("#DCB432")); pdf.setFont("Helvetica-Bold",9)
        pdf.drawString(M+12, y-16, "SINTESE DO CHAIRMAN")
        tblock(pdf, M+12, y-32, ch_txt, sz=8, clr=TEXT, mw=PAGE_W-2*M-24, ld=12)


# -- PAGE: COMPOSITE SIGNALS --
def pg_composite_signals(pdf, corr, synthesis):
    dbg(pdf); hdr(pdf, "SINAIS COMPOSTOS + CADEIAS CAUSAIS",
        "Cruzamento multifatorial: precos, COT, estoques, macro, sentimento",
        "Verde=altista, Vermelho=baixista. Minimo 3 fontes concordantes.")
    y = PAGE_H - 78
    composites = (corr or {}).get("composite_signals", [])
    chains = (synthesis or {}).get("causal_chains_active", [])
    if not composites:
        tblock(pdf, M, y, "Dados insuficientes. Execute o pipeline completo.", sz=9, clr=TEXT_MUT)
        return
    cw = (PAGE_W - 2*M - 10) / 2
    for i, cs in enumerate(composites[:8]):
        col = i % 2; row = i // 2
        cx = M + col*(cw+10); cy = y - row*62
        sig = cs.get("signal","NEUTRO"); conf = cs.get("confidence",0)
        asset = cs.get("asset","?"); name = NM.get(asset, asset)
        sc = GREEN if sig=="BULLISH" else RED if sig=="BEARISH" else TEXT_MUT
        panel(pdf, cx, cy-55, cw, 55)
        pdf.setFillColor(HexColor(sc)); pdf.rect(cx, cy-55, 4, 55, fill=1, stroke=0)
        pdf.setFillColor(HexColor(TEXT)); pdf.setFont("Helvetica-Bold",10)
        pdf.drawString(cx+12, cy-14, f"{asset} - {name}")
        bx = cx+12+pdf.stringWidth(f"{asset} - {name}","Helvetica-Bold",10)+8
        badge(pdf, bx, cy-14, sig, sc)
        pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",7)
        pdf.drawString(cx+12, cy-28, f"Confianca: {conf*100:.0f}% | {cs.get('sources_count',0)} fontes")
        factors = (cs.get("factors_bull",[]) + cs.get("factors_bear",[]))[:3]
        fy = cy - 40
        for f in factors:
            pre = "+" if f in cs.get("factors_bull",[]) else "-"
            fc = GREEN if pre=="+" else RED
            pdf.setFillColor(HexColor(fc)); pdf.setFont("Helvetica",6.5)
            pdf.drawString(cx+12, fy, f"{pre} {f[:70]}"); fy -= 9
    if chains:
        cy = y - (((len(composites[:8])+1)//2)*62) - 20
        pdf.setFillColor(HexColor("#DCB432")); pdf.setFont("Helvetica-Bold",9)
        pdf.drawString(M, cy, "CADEIAS CAUSAIS ATIVAS"); cy -= 18
        chw = (PAGE_W-2*M-3*8)/min(len(chains),4)
        for idx,ch in enumerate(chains[:4]):
            cx = M+idx*(chw+8)
            panel(pdf, cx, cy-40, chw, 40)
            pdf.setFillColor(HexColor("#DCB432")); pdf.setFont("Helvetica-Bold",7)
            pdf.drawString(cx+8, cy-12, ch.get("name","")[:30])
            pdf.setFillColor(HexColor(TEXT_MUT)); pdf.setFont("Helvetica",6)
            pdf.drawString(cx+8, cy-24, ch.get("signal","")[:60])


# -- PAGE 18: GLOSSARIO --

def pg_glossary(pdf):

    dbg(pdf); hdr(pdf,"Glossario -- Termos Explicados","Se algum termo nao ficou claro, consulte aqui",

                  "Tudo em linguagem simples. Sem complicacao.")

    y = PAGE_H - 78

    terms = [
        ("Posicao dos Fundos (COT)","Relatorio semanal: quem ta comprado e vendido nos futuros. Fundos compram muito = preco tende a subir."),
        ("Z-score","Mede se o valor esta normal ou fora do padrao. Acima de +2 = muito acima. Abaixo de -2 = muito abaixo."),
        ("Spread","Diferenca entre dois precos. Ex: esmagamento = lucro da industria comprando soja e vendendo oleo + farelo."),
        ("Estoques/Uso (STU)","Estoque dividido pelo consumo. Quanto menor, mais apertado e maior a pressao de alta."),
        ("WASDE","Relatorio mensal do USDA com estimativas de oferta e demanda mundial de graos."),
        ("El Nino / La Nina","Fenomeno climatico. El Nino = seca Norte/NE. La Nina = seca Sul. Afeta safras e precos."),
        ("Base","Diferenca entre preco fisico local e futuro de Chicago. Base forte = mercado local paga mais."),
        ("Sazonalidade","Padrao de preco que se repete todo ano. Soja sobe jan-mai, cai na colheita americana."),
        ("VIX","Indice de volatilidade do S&P 500. Acima de 25 = mercado nervoso. Acima de 35 = panico."),
        ("FedWatch","Probabilidade de o Fed (banco central EUA) subir, manter ou cortar juros na proxima reuniao."),
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

    # URL for full glossary
    pdf.setFillColor(HexColor(TEXT_DIM)); pdf.setFont("Helvetica", 8)
    pdf.drawString(M, 50, "Glossario completo disponivel em agrimacro.com")



# a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*

#  BUILD

# a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*







# a"a" PAGINA: AVISO LEGAL (DISCLOSURE) a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"

def pg_disclosure(pdf):

    """Renderiza pAgina de Aviso Legal completa."""

    if not HAS_DISCLOSURE:

        return



    W, H = landscape(A4)

    disc = get_disclosure_page()



    # Background dark

    pdf.setFillColor(HexColor("#1a1a2e"))

    pdf.rect(0, 0, W, H, fill=1, stroke=0)



    # TAtulo

    pdf.setFillColor(HexColor("#e94560"))

    pdf.setFont("Helvetica-Bold", 22)

    pdf.drawString(50, H - 55, disc["title"])



    # SubtAtulo

    pdf.setFillColor(HexColor("#aaaaaa"))

    pdf.setFont("Helvetica-Oblique", 11)

    pdf.drawString(50, H - 75, disc["subtitle"])



    # Linha separadora

    pdf.setStrokeColor(HexColor("#e94560"))

    pdf.setLineWidth(1)

    pdf.line(50, H - 85, W - 50, H - 85)



    # SeAAes a" 2 colunas

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



            # Content a" wrap text

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



            y -= 8  # EspaAo entre seAAes



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



    # VersAo

    pdf.setFillColor(HexColor("#444444"))

    pdf.setFont("Helvetica", 6)

    pdf.drawRightString(W - 50, 15, f"Disclosure v{disc.get('version', '1.0')} | {disc.get('last_updated', '')}")







# == BILATERAL INTELLIGENCE PAGE ============================================



# ---- Cross-Analysis Dashboard (9 indicadores) ----



# a"a" PAGE: CROSS-ANALYSIS DASHBOARD a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"a"

def pg_cross_dashboard(pdf, cd):

    """Page 18 - Painel de Cruzamento (9 indicadores bilaterais)"""

    dbg(pdf)

    hdr(pdf,

        "PAINEL DE CRUZAMENTO \u2014 9 Indicadores Bilaterais",

        "Cruzamento de dados EUA x Brasil para decis\u00f5es de mercado",

        "Cada card mostra um indicador com explica\u00e7\u00e3o e sinal de mercado")



    cols, rows_n = 3, 3

    gap = 8

    card_w = (PAGE_W - 2 * M - (cols - 1) * gap) / cols

    card_h = (PAGE_H - 2 * M - 70 - (rows_n - 1) * gap) / rows_n

    top_y = PAGE_H - M - 60



    # -- Signal translations ----------------------------------------------

    SIG_PT = {

        "NEUTRAL": "Neutro", "PREMIUM": "Pr\u00eamio BR", "DISCOUNT": "Desconto BR",

        "STRONG_PREMIUM": "Pr\u00eamio forte", "STRONG_DISCOUNT": "Desconto forte",

        "US_STRONG": "EUA forte", "BR_STRONG": "BR forte", "BALANCED": "Equilibrado",

        "STRONG_CARRY": "Carry forte", "FLAT": "Neutro", "BACKWARDATION": "Invers\u00e3o",

        "NORMAL": "Normal", "ELEVATED": "Elevado", "SQUEEZE": "Compress\u00e3o",

        "BUMPER": "Safra cheia", "CROP_LOSS": "Quebra safra", "TREND": "Tend\u00eancia",

        "BRL BULLISH": "Real forte", "BRL BEARISH": "Real fraco",

        "N/A": "S/dados",

    }



    def sig_pt(s):

        if not s:

            return "S/dados"

        return SIG_PT.get(s, s.replace("_", " ").title())



    def draw_card(idx, title, metric, explain, what_is, signal_raw, color):

        col = idx % cols

        row = idx // cols

        cx = M + col * (card_w + gap)

        cy = top_y - row * (card_h + gap)



        # Card background

        panel(pdf, cx, cy - card_h, card_w, card_h)



        # Color bar left

        pdf.setFillColor(HexColor(color))

        pdf.rect(cx, cy - card_h, 4, card_h, fill=1, stroke=0)



        # Title

        pdf.setFont("Helvetica-Bold", 9)

        pdf.setFillColor(HexColor(AMBER))

        pdf.drawString(cx + 12, cy - 15, title)



        # Metric (big number)

        pdf.setFont("Helvetica-Bold", 11)

        pdf.setFillColor(HexColor(color))

        pdf.drawString(cx + 12, cy - 32, metric[:int(card_w / 5)])



        # Explanation text

        ey = cy - 48

        pdf.setFont("Helvetica", 6.5)

        pdf.setFillColor(HexColor(TEXT))

        for line in explain:

            if ey < cy - card_h + 30:

                break

            pdf.drawString(cx + 12, ey, line[:int(card_w / 3.8)])

            ey -= 9



        # "O que eh" box

        ey -= 4

        pdf.setStrokeColor(HexColor(BORDER))

        pdf.setLineWidth(0.3)

        pdf.line(cx + 12, ey + 2, cx + card_w - 12, ey + 2)

        ey -= 8

        pdf.setFont("Helvetica-Bold", 5.5)

        pdf.setFillColor(HexColor(CYAN))

        pdf.drawString(cx + 12, ey, "O QUE \u00c9:")

        pdf.setFont("Helvetica", 5.5)

        pdf.setFillColor(HexColor(TEXT_DIM))

        wi_text = what_is[:int(card_w / 3)]

        pdf.drawString(cx + 42, ey, wi_text)

        if len(what_is) > int(card_w / 3):

            ey -= 8

            pdf.drawString(cx + 12, ey, what_is[int(card_w / 3):int(card_w / 3) * 2])



        # Signal badge

        badge(pdf, cx + 12, cy - card_h + 8, sig_pt(signal_raw), color)



    # -- Card 1: Basis Temporal -------------------------------------------

    bt = cd.get("basis_temporal", {})

    bt_c = bt.get("commodities", {})

    soja_b = bt_c.get("soja", {})

    milho_b = bt_c.get("milho", {})

    basis_val = soja_b.get("basis_pct", 0)

    basis_sig = soja_b.get("signal", "N/A")

    milho_val = milho_b.get("basis_pct", 0)

    basis_color = GREEN if basis_val > 0 else RED if basis_val < -5 else AMBER

    basis_explain = []

    if basis_val < 0:

        basis_explain.append("Brasil abaixo de Chicago.")

        basis_explain.append("Press\u00e3o de safra local ou c\u00e2mbio.")

    elif basis_val > 0:

        basis_explain.append("Brasil acima de Chicago.")

        basis_explain.append("Demanda local forte ou oferta curta.")

    else:

        basis_explain.append("Pre\u00e7os alinhados BR vs Chicago.")

    if milho_b:

        basis_explain.append(f"Milho: {milho_val:+.1f}%")

    draw_card(0, "BASE TEMPORAL", f"Soja: {basis_val:+.1f}%",

              basis_explain,

              "Diferen\u00e7a entre pre\u00e7o f\u00edsico BR (CEPEA) e futuro Chicago em R$.",

              basis_sig, basis_color)



    # -- Card 2: COT Momentum --------------------------------------------

    cot = cd.get("cot_momentum", {})

    cot_prods = cot.get("products", cot.get("symbols", {}))

    n_extreme = sum(1 for v in cot_prods.values() if v.get("extreme"))

    total_sym = len(cot_prods)

    ext_names = [NM.get(k, k) for k, v in cot_prods.items() if v.get("extreme")]

    cot_color = RED if n_extreme >= 3 else AMBER if n_extreme >= 1 else GREEN

    cot_explain = []

    if n_extreme > 0:

        cot_explain.append(f"{n_extreme} mercado(s) em posi\u00e7\u00e3o extrema.")

        cot_explain.append(", ".join(ext_names[:3]))

        cot_explain.append("Aten\u00e7\u00e3o: revers\u00e3o poss\u00edvel.")

    else:

        cot_explain.append("Nenhum mercado em extremo.")

        cot_explain.append("Posi\u00e7\u00f5es dentro do normal.")

    draw_card(1, "FUNDOS (CFTC)", f"{n_extreme} extremos / {total_sym} mercados",

              cot_explain,

              "Posi\u00e7\u00f5es dos grandes especuladores. Extremo = risco de revers\u00e3o.",

              f"{n_extreme} EXTREME" if n_extreme else "NORMAL", cot_color)



    # -- Card 3: Crush Margin --------------------------------------------

    cr = cd.get("crush_bilateral", {})

    us_cr = cr.get("us_crush", {})

    crush_usd = us_cr.get("margin_usd_bu", 0)

    crush_sig = us_cr.get("signal", "N/A")

    oil_pct = us_cr.get("oil_share_pct", 0)

    crush_color = GREEN if crush_usd > 1.5 else AMBER if crush_usd > 0.8 else RED

    crush_explain = []

    if crush_usd > 1.5:

        crush_explain.append("Margem forte p/ esmagadores EUA.")

        crush_explain.append("Incentiva compra de soja.")

    elif crush_usd > 0.8:

        crush_explain.append("Margem moderada.")

    else:

        crush_explain.append("Margem apertada \u2014 esmagadores reduzem.")

    crush_explain.append(f"\u00d3leo de soja = {oil_pct:.0f}% da receita.")

    draw_card(2, "MARGEM DE ESMAGAMENTO", f"${crush_usd:.3f}/bu | \u00d3leo {oil_pct:.0f}%",

              crush_explain,

              "Lucro ao processar soja em farelo + \u00f3leo. Forte = mais demanda por soja.",

              crush_sig, crush_color)



    # -- Card 4: Margem Produtor ------------------------------------------

    pm = cd.get("producer_margin", {})

    pm_idx = pm.get("cost_index", 0)

    pm_lvl = pm.get("cost_level", "N/A")

    pm_squeeze = pm.get("squeeze", False)

    pm_color = GREEN if pm_idx < 40 else RED if pm_idx > 70 or pm_squeeze else AMBER

    pm_explain = []

    if pm_squeeze:

        pm_explain.append("ALERTA: Compress\u00e3o de margem!")

        pm_explain.append("Custos subindo mais que pre\u00e7os.")

    elif pm_idx > 70:

        pm_explain.append("Custos elevados vs pre\u00e7o de venda.")

        pm_explain.append("Margem do produtor apertada.")

    elif pm_idx < 40:

        pm_explain.append("Custos baixos vs pre\u00e7o de venda.")

        pm_explain.append("Margem saud\u00e1vel para o produtor.")

    else:

        pm_explain.append("Custos moderados.")

    draw_card(3, "MARGEM DO PRODUTOR", f"\u00cdndice: {pm_idx:.0f}/100",

              pm_explain,

              "Rela\u00e7\u00e3o custo/pre\u00e7o. Abaixo de 40 = bom. Acima de 70 = aperto.",

              pm_lvl, pm_color)



    # -- Card 5: Diferencial Juros ----------------------------------------

    ir = cd.get("interest_differential", {})

    diff_pp = ir.get("differential_pp", 0)

    ir_sig = ir.get("signal", "N/A")

    brl_imp = ir.get("brl_impact", "")

    ir_color = GREEN if diff_pp > 8 else AMBER if diff_pp > 4 else RED

    ir_explain = []

    if diff_pp > 8:

        ir_explain.append("Diferencial alto atrai capital p/ BR.")

        ir_explain.append("Tende a valorizar o Real.")

        ir_explain.append("Real forte = pre\u00e7o menor em R$.")

    elif diff_pp > 4:

        ir_explain.append("Diferencial moderado.")

    else:

        ir_explain.append("Diferencial baixo \u2014 menos atrativo.")

        ir_explain.append("Pode enfraquecer o Real.")

    draw_card(4, "DIFERENCIAL DE JUROS", f"{diff_pp:.1f}pp (Selic vs Fed)",

              ir_explain,

              "Diferen\u00e7a entre juros BR e EUA. Maior = Real mais forte = soja cai em R$.",

              ir_sig, ir_color)



    # -- Card 6: Ritmo Exportacao -----------------------------------------

    ep = cd.get("export_pace_weekly", {})

    ep_pct = ep.get("pct_through", 0)

    ep_qlabel = ep.get("quarter_label", "N/A")

    us_d = ep.get("us", {})

    br_d = ep.get("br", {})

    us_pace = us_d.get("us_pace_pct", us_d.get("pace_pct", 0))

    br_pace = br_d.get("br_pace_pct", br_d.get("pace_pct", 0))

    ep_color = GREEN if us_pace > 50 else AMBER if us_pace > 30 else RED

    ep_explain = []

    ep_explain.append(f"EUA: {us_pace:.0f}% do esperado exportado.")

    ep_explain.append(f"Brasil: {br_pace:.0f}% do esperado.")

    if us_pace > 50:

        ep_explain.append("Ritmo forte nos EUA \u2014 demanda ativa.")

    elif us_pace < 30:

        ep_explain.append("Ritmo lento \u2014 demanda fraca.")

    draw_card(5, "RITMO DE EXPORTA\u00c7\u00c3O", f"Ano: {ep_pct:.0f}% | EUA {us_pace:.0f}% BR {br_pace:.0f}%",

              ep_explain,

              "Exporta\u00e7\u00f5es acumuladas vs meta anual. Ritmo forte = pre\u00e7os firmes.",

              ep_qlabel, ep_color)



    # -- Card 7: Argentina ------------------------------------------------

    ar = cd.get("argentina_trilateral", {})

    ar_crops = ar.get("crops", {})

    ar_soja = ar_crops.get("soja", {})

    ar_mmt = ar_soja.get("production_mmt", 0)

    ar_sig = ar_soja.get("signal", "N/A")

    ar_vs = ar_soja.get("vs_avg_pct", 0)

    ar_color = RED if ar_sig == "BUMPER" else GREEN if ar_sig == "CROP_LOSS" else AMBER

    ar_explain = []

    if ar_sig == "BUMPER":

        ar_explain.append("Safra cheia na Argentina.")

        ar_explain.append("Mais oferta global \u2014 pressiona pre\u00e7os.")

    elif ar_sig == "CROP_LOSS":

        ar_explain.append("Quebra de safra na Argentina.")

        ar_explain.append("Menos oferta \u2014 suporta pre\u00e7os.")

    else:

        ar_explain.append("Safra argentina dentro da m\u00e9dia.")

    if ar_vs != 0:

        ar_explain.append(f"{ar_vs:+.1f}% vs m\u00e9dia de 5 anos.")

    draw_card(6, "ARGENTINA", f"Soja: {ar_mmt:.0f} MMT ({ar_vs:+.1f}%)",

              ar_explain,

              "Produ\u00e7\u00e3o argentina. Safra cheia = baixista. Quebra = altista.",

              ar_sig, ar_color)



    # -- Card 8: Indice Seca ----------------------------------------------

    dr = cd.get("drought_accumulator", {})

    dr_reg = dr.get("regions", {})

    cerr = dr_reg.get("cerrado_soja", {})

    dr_day = cerr.get("day", 0)

    dr_pct = cerr.get("pct", 0)

    dr_stage = cerr.get("stage", "N/A")

    dr_color = RED if dr_pct < 40 else GREEN if dr_pct > 80 else AMBER

    dr_explain = []

    if dr_pct < 40:

        dr_explain.append("Umidade cr\u00edtica no Cerrado!")

        dr_explain.append("Risco de perda de produtividade.")

    elif dr_pct > 80:

        dr_explain.append("Condi\u00e7\u00f5es h\u00eddricas boas.")

        dr_explain.append("Lavouras bem supridas.")

    else:

        dr_explain.append("Umidade moderada.")

        dr_explain.append("Monitorar pr\u00f3ximas semanas.")

    draw_card(7, "\u00cdNDICE DE SECA", f"Cerrado: dia {dr_day} ({dr_pct}%)",

              dr_explain,

              "Acumulado de chuvas no Cerrado. Abaixo de 40% = seca. Afeta soja e milho.",

              dr_stage, dr_color)



    # -- Card 9: Frete Spread ---------------------------------------------

    fr = cd.get("freight_spread", {})

    us_frt = fr.get("us_corridor", {}).get("total_usd_mt", 0)

    br_frt = fr.get("br_corridor", {}).get("total_usd_mt", 0)

    fr_spread = fr.get("spread_usd_mt", 0)

    fr_adv = fr.get("advantage", "N/A")

    fr_color = GREEN if fr_adv == "BR" else RED if fr_adv == "US" else AMBER

    fr_explain = []

    fr_explain.append(f"Frete EUA: ${us_frt:.0f}/ton at\u00e9 porto.")

    fr_explain.append(f"Frete BR: ${br_frt:.0f}/ton at\u00e9 porto.")

    if fr_adv == "US":

        fr_explain.append("EUA mais competitivo no frete.")

        fr_explain.append("Comprador int'l prefere EUA.")

    elif fr_adv == "BR":

        fr_explain.append("Brasil mais competitivo no frete.")

    else:

        fr_explain.append("Fretes equilibrados.")

    draw_card(8, "FRETE SPREAD", f"EUA ${us_frt:.0f} vs BR ${br_frt:.0f} (dif ${fr_spread:.0f}/t)",

              fr_explain,

              "Custo de frete fazenda-porto. Quem tem frete menor vende mais barato ao mundo.",

              fr_adv, fr_color)



def pg_cot_basis(pdf, cd, cross_data=None):

    """COT & Base - Fundos especuladores (CFTC) + Base Brasil vs Chicago."""

    dbg(pdf)

    hdr(pdf,

        "COT & BASE \u2014 Fundos, Especuladores e Pre\u00e7os no Brasil",

        "Quem t\u00e1 comprado, quem t\u00e1 vendido, e como o Brasil compara com Chicago",

        "Esquerda: CFTC semanal  |  Direita: f\u00edsico BR vs Chicago em R$")



    cross = cross_data if cross_data else {}

    cot_mom = cross.get("cot_momentum", {}).get("symbols", {})

    basis_data = cross.get("basis_temporal", {}).get("commodities", {})



    # Montar lista COT de cd (fonte principal, 16+ commodities)

    rows = []

    comms = cd.get("commodities", {}) if cd else {}

    for sym, info in comms.items():

        leg = info.get("legacy", {})

        hist = leg.get("history", [])

        if not hist:

            continue

        cur = hist[0]

        prev = hist[1] if len(hist) > 1 else {}

        net = cur.get("noncomm_net", 0)

        comm_net = cur.get("comm_net", 0)

        oi = cur.get("open_interest", 1)

        prev_net = prev.get("noncomm_net", net)

        delta = net - prev_net

        pct_oi = (abs(net) / oi * 100) if oi else 0



        mom_info = cot_mom.get(sym, {})

        extreme = mom_info.get("extreme", False)

        momentum_raw = mom_info.get("momentum", "")

        mom_map = {

            "ACCUMULATING": "Acumulando",

            "DISTRIBUTING": "Distribuindo",

            "BOTTOM_FISHING": "Comprando fundo",

            "NEUTRAL": "Neutro",

            "TOPPING": "Topo",

        }

        momentum_pt = mom_map.get(momentum_raw, "")



        name = NM.get(sym, sym)

        rows.append({

            "sym": sym, "name": name, "net": net, "comm": comm_net,

            "delta": delta, "pct_oi": pct_oi, "extreme": extreme,

            "momentum": momentum_pt,

        })



    rows.sort(key=lambda r: r["net"], reverse=True)



    # Contagens para analise

    n_extreme = sum(1 for r in rows if r["extreme"])

    extremes_names = [r["name"] for r in rows if r["extreme"]]

    top_bull = rows[0] if rows else None

    top_bear = rows[-1] if rows else None



    # Layout

    Y_TOP = PAGE_H - 115

    LEFT_W = (PAGE_W - 2 * M - 12) * 0.58

    RIGHT_W = (PAGE_W - 2 * M - 12) - LEFT_W

    LEFT_X = M

    RIGHT_X = M + LEFT_W + 12



    # === COLUNA ESQUERDA - TABELA COT ===

    panel(pdf, LEFT_X, 48, LEFT_W, Y_TOP - 48)



    pdf.setFont("Helvetica-Bold", 11)

    pdf.setFillColor(AMBER)

    pdf.drawString(LEFT_X + 10, Y_TOP - 18, "POSI\u00c7\u00c3O DOS FUNDOS (CFTC)")

    pdf.setFont("Helvetica", 7)

    pdf.setFillColor(TEXT_DIM)

    pdf.drawString(LEFT_X + 10, Y_TOP - 30, "Posi\u00e7\u00f5es l\u00edquidas dos especuladores \u2014 atualiza\u00e7\u00e3o semanal")



    tbl_x = LEFT_X + 8

    tbl_y = Y_TOP - 48

    col_w = [LEFT_W * 0.22, LEFT_W * 0.18, LEFT_W * 0.17, LEFT_W * 0.14, LEFT_W * 0.11, LEFT_W * 0.16]

    headers = ["Commodity", "Fundos (l\u00edq)", "Ind\u00fastria", "\u0394 Semana", "%OI", "Status"]



    pdf.setFont("Helvetica-Bold", 6.5)

    pdf.setFillColor(CYAN)

    cx = tbl_x

    for i, h in enumerate(headers):

        pdf.drawString(cx + 2, tbl_y, h)

        cx += col_w[i]



    tbl_y -= 4

    pdf.setStrokeColor(BORDER)

    pdf.setLineWidth(0.5)

    pdf.line(tbl_x, tbl_y, tbl_x + sum(col_w), tbl_y)

    tbl_y -= 10



    max_rows = min(len(rows), 18)

    for idx in range(max_rows):

        r = rows[idx]

        row_h = 13



        if r["extreme"]:

            pdf.setFillColor("#1A2A1A" if r["net"] > 0 else "#2A1A1A")

            pdf.rect(tbl_x - 2, tbl_y - 3, sum(col_w) + 4, row_h, fill=1, stroke=0)



        cx = tbl_x

        pdf.setFont("Helvetica-Bold" if r["extreme"] else "Helvetica", 6.5)

        pdf.setFillColor(AMBER if r["extreme"] else TEXT)

        pdf.drawString(cx + 2, tbl_y, r["name"])

        cx += col_w[0]



        net_str = f"{r['net']:+,.0f}".replace(",", ".")

        pdf.setFont("Helvetica-Bold", 6.5)

        pdf.setFillColor(GREEN if r["net"] > 0 else RED)

        pdf.drawString(cx + 2, tbl_y, net_str)

        cx += col_w[1]



        comm_str = f"{r['comm']:+,.0f}".replace(",", ".")

        pdf.setFont("Helvetica", 6.5)

        pdf.setFillColor(RED if r["comm"] < 0 else GREEN)

        pdf.drawString(cx + 2, tbl_y, comm_str)

        cx += col_w[2]



        delta_str = f"{r['delta']:+,.0f}".replace(",", ".")

        pdf.setFont("Helvetica", 6.5)

        pdf.setFillColor(GREEN if r["delta"] > 0 else RED if r["delta"] < 0 else TEXT_DIM)

        pdf.drawString(cx + 2, tbl_y, delta_str)

        cx += col_w[3]



        pdf.setFont("Helvetica", 6.5)

        pdf.setFillColor(TEXT_DIM)

        pdf.drawString(cx + 2, tbl_y, f"{r['pct_oi']:.0f}%")

        cx += col_w[4]



        if r["extreme"]:

            badge(pdf, cx + 2, tbl_y - 2, "!! EXTREMO", AMBER)

        elif r["momentum"]:

            pdf.setFont("Helvetica", 6)

            pdf.setFillColor(PURPLE)

            pdf.drawString(cx + 2, tbl_y, r["momentum"])



        tbl_y -= row_h



    # Caixa COMO LER ESTA TABELA

    box_y = tbl_y - 12

    box_h = 72

    if box_y - box_h < 52:

        box_h = max(box_y - 54, 50)

    pdf.setStrokeColor(BORDER)

    pdf.setLineWidth(0.5)

    pdf.setFillColor("#0D1820")

    pdf.roundRect(tbl_x, box_y - box_h, sum(col_w), box_h, 4, fill=1, stroke=1)



    pdf.setFont("Helvetica-Bold", 7)

    pdf.setFillColor(CYAN)

    ey = box_y - 12

    pdf.drawString(tbl_x + 8, ey, "COMO LER ESTA TABELA")

    ey -= 12



    explics = [

        ("Fundos (l\u00edq):", "Saldo dos especuladores. Positivo = apostam em alta, negativo = baixa."),

        ("Ind\u00fastria:", "Hedge dos produtores/processadores. Geralmente oposto aos fundos."),

        ("\u0394 Semana:", "Mudan\u00e7a na \u00faltima semana. Mostra se est\u00e3o comprando ou vendendo."),

        ("!! EXTREMO:", "Posi\u00e7\u00e3o fora do padr\u00e3o hist\u00f3rico \u2014 aten\u00e7\u00e3o para revers\u00e3o."),

    ]

    for title_e, desc in explics:

        if ey < box_y - box_h + 6:

            break

        pdf.setFont("Helvetica-Bold", 6)

        pdf.setFillColor(AMBER)

        pdf.drawString(tbl_x + 12, ey, title_e)

        pdf.setFont("Helvetica", 6)

        pdf.setFillColor(TEXT_DIM)

        pdf.drawString(tbl_x + 12 + pdf.stringWidth(title_e, "Helvetica-Bold", 6) + 4, ey, desc)

        ey -= 11



    # === COLUNA DIREITA - BASE BRASIL ===

    panel(pdf, RIGHT_X, 48, RIGHT_W, Y_TOP - 48)



    pdf.setFont("Helvetica-Bold", 11)

    pdf.setFillColor(AMBER)

    pdf.drawString(RIGHT_X + 10, Y_TOP - 18, "BASE BRASIL vs CHICAGO")

    pdf.setFont("Helvetica", 7)

    pdf.setFillColor(TEXT_DIM)

    pdf.drawString(RIGHT_X + 10, Y_TOP - 30, "Pre\u00e7o f\u00edsico BR vs futuro Chicago convertido em R$")



    basis_keys = ["soja", "milho", "boi_gordo", "cafe", "algodao", "acucar", "trigo"]

    basis_display = {"soja": "Soja", "milho": "Milho", "boi_gordo": "Boi Gordo", "cafe": "Caf\u00e9", "algodao": "Algod\u00e3o", "acucar": "A\u00e7\u00facar", "trigo": "Trigo"}

    signal_map = {"PREMIUM": ("Pr\u00eamio BR", GREEN), "DISCOUNT": ("Desconto BR", RED), "NEUTRAL": ("Neutro", TEXT_DIM), "STRONG_PREMIUM": ("Pr\u00eamio forte", GREEN), "STRONG_DISCOUNT": ("Desconto forte", RED)}

    basis_comment = {"PREMIUM": "Brasil acima de Chicago \u2014 demanda local forte.", "DISCOUNT": "Brasil abaixo de Chicago \u2014 press\u00e3o de safra local.", "NEUTRAL": "Pre\u00e7os alinhados entre BR e Chicago.", "STRONG_PREMIUM": "Pr\u00eamio elevado \u2014 oferta local apertada.", "STRONG_DISCOUNT": "Desconto acentuado \u2014 excesso de oferta no BR."}



    card_y = Y_TOP - 48

    card_h = 52

    card_margin = 6



    for bk in basis_keys:

        bd = basis_data.get(bk, {})

        if not bd:

            continue

        if card_y - card_h < 120:

            break



        br_p = bd.get("br_price", 0)

        chi_p = bd.get("chi_brl", 0)

        basis_pct = bd.get("basis_pct", 0)

        sig_raw = bd.get("signal", "NEUTRAL")

        sig_label, sig_color = signal_map.get(sig_raw, ("Neutro", TEXT_DIM))

        comment = basis_comment.get(sig_raw, "")

        dname = basis_display.get(bk, bk.title())



        pdf.setFillColor("#0D1820")

        pdf.setStrokeColor(BORDER)

        pdf.roundRect(RIGHT_X + 8, card_y - card_h, RIGHT_W - 16, card_h, 4, fill=1, stroke=1)



        pdf.setFont("Helvetica-Bold", 8)

        pdf.setFillColor(TEXT)

        pdf.drawString(RIGHT_X + 16, card_y - 14, dname)



        pdf.setFont("Helvetica", 7)

        pdf.setFillColor(TEXT_DIM)

        price_str = f"Brasil R${br_p:,.2f}  |  Chicago R${chi_p:,.2f}"

        pdf.drawString(RIGHT_X + 16, card_y - 26, price_str)



        basis_str = f"{basis_pct:+.1f}%"

        pdf.setFont("Helvetica-Bold", 8)

        pdf.setFillColor(GREEN if basis_pct > 0 else RED if basis_pct < 0 else TEXT_DIM)

        pdf.drawString(RIGHT_X + 16, card_y - 38, basis_str)

        badge(pdf, RIGHT_X + 60, card_y - 40, sig_label, sig_color)



        if comment:

            pdf.setFont("Helvetica", 5.5)

            pdf.setFillColor(TEXT_DIM)

            pdf.drawString(RIGHT_X + 16, card_y - 48, comment)



        card_y -= (card_h + card_margin)



    # Caixa O QUE E A BASE

    if card_y - 48 > 56:

        qbox_h = 44

        pdf.setFillColor("#0D1820")

        pdf.setStrokeColor(BORDER)

        pdf.roundRect(RIGHT_X + 8, card_y - qbox_h, RIGHT_W - 16, qbox_h, 4, fill=1, stroke=1)

        pdf.setFont("Helvetica-Bold", 7)

        pdf.setFillColor(CYAN)

        pdf.drawString(RIGHT_X + 16, card_y - 13, "O QUE \u00c9 A BASE?")

        _basis_txt = "A base \u00e9 a diferen\u00e7a entre o pre\u00e7o f\u00edsico no Brasil (CEPEA/ESALQ) e o futuro em Chicago convertido para reais. Base negativa = Brasil mais barato. Base positiva = demanda local forte."

        tblock(pdf, RIGHT_X + 16, card_y - 24, _basis_txt, "Helvetica", 6, TEXT_DIM, RIGHT_W - 40, 8)



    # === ANALISE DO DIA (rodape, largura total) ===

    ana_h = 42

    ana_y = 46

    ana_w = PAGE_W - 2 * M



    pdf.setFillColor(PANEL)

    pdf.rect(M, ana_y - ana_h + 4, ana_w, ana_h, fill=1, stroke=0)



    pdf.setStrokeColor(AMBER)

    pdf.setLineWidth(3)

    pdf.line(M + 2, ana_y + 4, M + 2, ana_y - ana_h + 4)

    pdf.setLineWidth(0.5)



    pdf.setFont("Helvetica-Bold", 8)

    pdf.setFillColor(AMBER)

    pdf.drawString(M + 12, ana_y - 2, "\u25cc AN\u00c1LISE DO DIA \u2014 COT & BASE")



    parts = []

    if n_extreme > 0:

        ext_list = ", ".join(extremes_names[:3])

        if len(extremes_names) > 3:

            ext_list += "..."

        parts.append(f"{n_extreme} mercado(s) em posi\u00e7\u00e3o extrema ({ext_list}).")

    if top_bull:

        net_fmt = f"{top_bull['net']:+,.0f}".replace(",", ".")

        parts.append(f"{top_bull['name']} lidera compras com {net_fmt} contratos.")

    if top_bear and top_bear["net"] < 0:

        net_fmt = f"{top_bear['net']:+,.0f}".replace(",", ".")

        parts.append(f"{top_bear['name']} mais vendido com {net_fmt} contratos.")

    soja_b = basis_data.get("soja", {})

    if soja_b:

        bp = soja_b.get("basis_pct", 0)

        if bp != 0:

            direcao = "desconto" if bp < 0 else "pr\u00eamio"

            parts.append(f"Soja BR com {direcao} de {abs(bp):.1f}% sobre Chicago.")



    ana_text = "  ".join(parts) if parts else "Dados insuficientes para an\u00e1lise cruzada."

    tblock(pdf, M + 12, ana_y - 14, ana_text, "Helvetica", 6.5, TEXT, ana_w - 24, 8)



    pdf.setFont("Helvetica", 5)

    pdf.setFillColor(TEXT_MUT)

    pdf.drawString(M + 8, ana_y - ana_h + 7, "Fonte: CFTC Commitments of Traders, CEPEA/ESALQ, CME/CBOT, BCB/SGS")



def pg_bilateral(pdf, bilateral):

    dbg(pdf)

    hdr(pdf, "Intelig\u00eancia Bilateral EUA vs Brasil",

        "Competitividade, exporta\u00e7\u00f5es e custo desembarcado na China",

        "Indicadores propriet\u00e1rios AgriMacro que nenhum terminal Bloomberg ou DTN oferece.")

    y = PAGE_H - 75



    summary = bilateral.get("summary", {})

    lcs = bilateral.get("lcs", {})

    ert = bilateral.get("ert", {})

    bci = bilateral.get("bci", {})



    # === TOP STRIP: 3 headline numbers ===

    cw = (PAGE_W - 2 * M - 20) / 3

    cards_data = []



    if summary.get("lcs_spread") is not None:

        sp = summary["lcs_spread"]

        origin = summary.get("lcs_origin", "?")

        clr = GREEN if origin == "BR" else RED

        label_origin = "Brasil" if origin == "BR" else "EUA"

        cards_data.append(("Custo Desembarcado Shanghai", f"${sp:+.0f}/mt", f"{label_origin} mais competitivo", clr))



    if summary.get("ert_leader") is not None:

        leader = summary["ert_leader"]

        leader_pt = "Brasil" if leader == "BR" else "EUA"

        us_sig = ert.get("us_pace_signal", "?")

        br_sig = ert.get("br_pace_signal", "?")

        sig_pt = {"AHEAD": "Adiantado", "ON_PACE": "No ritmo", "BEHIND": "Atrasado"}

        clr = GREEN if leader == "BR" else AMBER

        cards_data.append(("Corrida de Exporta\u00e7\u00e3o", f"{leader_pt} lidera", f"EUA {sig_pt.get(us_sig, us_sig)} | BR {sig_pt.get(br_sig, br_sig)}", clr))



    if summary.get("bci_score") is not None:

        score = summary["bci_score"]

        signal = summary.get("bci_signal", "?")

        sig_pt2 = {"NEUTRAL": "Neutro", "BR_ADVANTAGE": "Vantagem BR", "US_ADVANTAGE": "Vantagem EUA"}

        clr = GREEN if score >= 60 else (AMBER if score >= 40 else RED)

        cards_data.append(("\u00cdndice Competitividade BR", f"{score:.0f}/100", sig_pt2.get(signal, signal), clr))



    for i, (title, val, sub, clr) in enumerate(cards_data):

        x = M + i * (cw + 10)

        panel(pdf, x, y - 60, cw, 62, bc=clr)

        pdf.setFillColor(HexColor(TEXT_MUT))

        pdf.setFont("Helvetica", 7.5)

        pdf.drawString(x + 10, y - 12, title)

        pdf.setFillColor(HexColor(TEXT))

        pdf.setFont("Helvetica-Bold", 18)

        pdf.drawString(x + 10, y - 36, val)

        pdf.setFillColor(HexColor(TEXT_DIM))

        pdf.setFont("Helvetica", 7)

        pdf.drawString(x + 10, y - 52, sub[:50])



    y -= 82



    # === LEFT COLUMN: LCS Detail ===

    lx = M

    col_w = (PAGE_W - 2 * M - 20) / 2



    if lcs.get("status") == "OK":

        panel(pdf, lx, y - 210, col_w, 208)

        py = y - 14

        pdf.setFillColor(HexColor(AMBER))

        pdf.setFont("Helvetica-Bold", 10)

        pdf.drawString(lx + 10, py, "CUSTO DESEMBARCADO SHANGHAI")

        py -= 12

        pdf.setFillColor(HexColor(TEXT_DIM))

        pdf.setFont("Helvetica", 6)

        pdf.drawString(lx + 10, py, "Quanto custa entregar soja no porto de Shanghai saindo de cada pa\u00eds")

        py -= 16



        pdf.setFillColor(HexColor(TEXT_MUT))

        pdf.setFont("Helvetica", 8)

        pdf.drawString(lx + 10, py, "Rota EUA (Gulf > Shanghai):")

        pdf.setFillColor(HexColor(TEXT))

        pdf.setFont("Helvetica-Bold", 9)

        pdf.drawString(lx + 200, py, f"${lcs.get('us_landed', 0):.0f}/mt")

        py -= 14



        pdf.setFillColor(HexColor(TEXT_MUT))

        pdf.setFont("Helvetica", 8)

        pdf.drawString(lx + 10, py, "Rota Brasil (Santos > Shanghai):")

        pdf.setFillColor(HexColor(TEXT))

        pdf.setFont("Helvetica-Bold", 9)

        pdf.drawString(lx + 200, py, f"${lcs.get('br_landed', 0):.0f}/mt")

        py -= 18



        spread = lcs.get("spread_usd_mt", 0)

        sp_clr = GREEN if spread > 0 else RED

        pdf.setFillColor(HexColor(sp_clr))

        pdf.setFont("Helvetica-Bold", 14)

        pdf.drawString(lx + 10, py, f"Spread: ${spread:+.2f}/mt")

        py -= 14

        comp_origin = lcs.get("competitive_origin", "?")

        comp_pt = "Brasil" if comp_origin == "BR" else "EUA"

        pdf.setFillColor(HexColor(TEXT_DIM))

        pdf.setFont("Helvetica", 7)

        pdf.drawString(lx + 10, py, f"Origem mais competitiva: {comp_pt}")

        py -= 16



        pdf.setFillColor(HexColor(TEXT_MUT))

        pdf.setFont("Helvetica", 7.5)

        fob_sp = lcs.get("fob_spread", 0)

        ocean_adv = lcs.get("ocean_advantage", 0)

        pdf.drawString(lx + 10, py, f"FOB Spread: ${fob_sp:+.0f}/mt  |  Frete Mar\u00edtimo: ${ocean_adv:+.0f}/mt")

        py -= 12

        ptax_v = lcs.get("ptax", 0)

        cbot_v = lcs.get("cbot_cents_bu", 0)

        pdf.drawString(lx + 10, py, f"CBOT: {cbot_v:.0f} cents/bu  |  PTAX: R$ {ptax_v:.2f}")

        py -= 16



        # Explicacao

        pdf.setStrokeColor(HexColor(BORDER))

        pdf.setLineWidth(0.3)

        pdf.line(lx + 10, py + 4, lx + col_w - 10, py + 4)

        py -= 8

        pdf.setFont("Helvetica-Bold", 5.5)

        pdf.setFillColor(HexColor(CYAN))

        pdf.drawString(lx + 10, py, "COMO LER:")

        pdf.setFont("Helvetica", 5.5)

        pdf.setFillColor(HexColor(TEXT_DIM))

        pdf.drawString(lx + 55, py, "Spread positivo = Brasil entrega mais barato na China. Importador chin\u00eas prefere comprar do BR.")

        py -= 8

        pdf.drawString(lx + 10, py, "Depende do c\u00e2mbio (PTAX), frete mar\u00edtimo e pre\u00e7o FOB. Muda diariamente.")



    # === RIGHT COLUMN: BCI Components ===

    rx = M + col_w + 20



    BCI_PT = {

        "Basis Spread": "Base (Pr\u00eamio/Desconto)",

        "FX (BRL/USD)": "C\u00e2mbio (R$/US$)",

        "FOB Spread": "Spread FOB",

        "Freight Advantage": "Vantagem no Frete",

        "Crush Margin": "Margem de Esmagamento",

        "Farmer Selling Pace": "Ritmo de Venda Produtor",

    }



    if bci.get("status") == "OK":

        panel(pdf, rx, y - 210, col_w, 208)

        py = y - 14

        pdf.setFillColor(HexColor(AMBER))

        pdf.setFont("Helvetica-Bold", 10)

        pdf.drawString(rx + 10, py, "\u00cdNDICE DE COMPETITIVIDADE BR (0-100)")

        py -= 12

        pdf.setFillColor(HexColor(TEXT_DIM))

        pdf.setFont("Helvetica", 6)

        pdf.drawString(rx + 10, py, "Cada componente mede uma vantagem do Brasil vs EUA. Acima de 50 = BR melhor.")

        py -= 16



        comps = bci.get("components", [])

        for comp in sorted(comps, key=lambda c: c.get("score", 0) * c.get("weight_pct", 0) / 100, reverse=True):

            name = comp.get("name", "?")

            name_pt = BCI_PT.get(name, name)

            score = comp.get("score", 0)

            weight = comp.get("weight_pct", 0)

            signal = comp.get("signal", "NEUTRAL")



            s_clr = GREEN if signal == "BULLISH" else (RED if signal == "BEARISH" else AMBER)



            pdf.setFillColor(HexColor(TEXT_MUT))

            pdf.setFont("Helvetica", 7)

            pdf.drawString(rx + 10, py, name_pt[:26])



            bar_x = rx + 140

            bar_w = col_w - 210

            pdf.setFillColor(HexColor(BORDER))

            pdf.rect(bar_x, py - 2, bar_w, 8, fill=1, stroke=0)

            fill_w = bar_w * score / 100

            pdf.setFillColor(HexColor(s_clr))

            pdf.rect(bar_x, py - 2, fill_w, 8, fill=1, stroke=0)



            pdf.setFillColor(HexColor(s_clr))

            pdf.setFont("Helvetica-Bold", 7.5)

            pdf.drawString(bar_x + bar_w + 5, py, f"{score:.0f}")



            pdf.setFillColor(HexColor(TEXT_DIM))

            pdf.setFont("Helvetica", 6.5)

            pdf.drawString(bar_x + bar_w + 25, py, f"{weight}%")



            py -= 16



        py -= 5

        strongest = bci.get("strongest", "?")

        weakest = bci.get("weakest", "?")

        strongest_pt = BCI_PT.get(strongest, strongest)

        weakest_pt = BCI_PT.get(weakest, weakest)

        pdf.setFillColor(HexColor(GREEN))

        pdf.setFont("Helvetica-Bold", 7)

        pdf.drawString(rx + 10, py, f"Mais forte: {strongest_pt}")

        pdf.setFillColor(HexColor(RED))

        pdf.drawString(rx + col_w / 2, py, f"Mais fraco: {weakest_pt}")



        # Explicacao

        py -= 14

        pdf.setStrokeColor(HexColor(BORDER))

        pdf.setLineWidth(0.3)

        pdf.line(rx + 10, py + 4, rx + col_w - 10, py + 4)

        py -= 8

        pdf.setFont("Helvetica-Bold", 5.5)

        pdf.setFillColor(HexColor(CYAN))

        pdf.drawString(rx + 10, py, "COMO LER:")

        pdf.setFont("Helvetica", 5.5)

        pdf.setFillColor(HexColor(TEXT_DIM))

        pdf.drawString(rx + 55, py, "Score acima de 50 = Brasil mais competitivo naquele quesito. Peso = import\u00e2ncia no \u00edndice total.")



    # === BOTTOM: Ritmo de Exportacao (cada pais vs seu benchmark sazonal) ===

    y -= 228



    sig_pt_map = {"AHEAD": "Adiantado", "ON_PACE": "No ritmo", "BEHIND": "Atrasado"}



    if ert.get("status") == "OK":

        panel(pdf, M, y - 190, PAGE_W - 2 * M, 188)

        py = y - 12



        us_ytd = ert.get("us_ytd_mmt", 0)

        us_pace = ert.get("us_pace_pct", 0)

        br_ytd = ert.get("br_ytd_mmt", 0)

        br_pace = ert.get("br_pace_pct", 0)

        us_sig = ert.get("us_pace_signal", "N/A")

        br_sig = ert.get("br_pace_signal", "N/A")

        data_src = ert.get("data_source", "wasde_seasonal_estimate")

        my_label = ert.get("marketing_year", "")

        quarter_label = ert.get("quarter_label", "")

        pct_through = ert.get("pct_through", 0)



        import datetime as _dtq

        _mq = _dtq.date.today().month

        # Benchmarks sazonais fixos: (EUA%, BR%) tipico neste mes do marketing year

        _BENCH = {1:(45,3),2:(55,10),3:(60,25),4:(65,50),5:(70,65),6:(75,75),7:(80,80),8:(85,82),9:(30,85),10:(55,90),11:(70,92),12:(80,95)}

        us_bench, br_bench = _BENCH.get(_mq, (50, 50))

        us_diff = us_pace - us_bench

        br_diff = br_pace - br_bench



        pdf.setFillColor(HexColor(AMBER))

        pdf.setFont("Helvetica-Bold", 10)

        pdf.drawString(M + 10, py, f"RITMO DE EXPORTACAO SOJA ({my_label})")

        pdf.setFillColor(HexColor(TEXT_DIM))

        pdf.setFont("Helvetica", 6)

        pdf.drawRightString(PAGE_W - M - 10, py, f"Periodo: {quarter_label} | {pct_through:.0f}% do ano safra")

        py -= 12



        pdf.setFillColor(HexColor(TEXT_DIM))

        pdf.setFont("Helvetica-Oblique", 6.5)

        pdf.drawString(M + 10, py, "Cada pais comparado contra seu proprio ritmo tipico nesta epoca (safras em periodos distintos).")

        py -= 14



        import datetime as _dt

        _month = _dt.date.today().month

        _season_notes = {

            1: "Jan: Colheita BR iniciando em MT. EUA dominam embarques. Normal BR estar com volume baixo.",

            2: "Fev: Colheita BR em MT/GO. Exportaces BR ainda minimas -- normal para o periodo.",

            3: "Mar: BR comeca embarcar forte. Virada sazonal: BR ultrapassa EUA em volume mensal.",

            4: "Abr: BR assume lideranca. Pico de embarques Santos/Paranagua.",

            5: "Mai: BR domina embarques globais. EUA em plantio.",

            6: "Jun: BR lidera. Safrinha milho compete por logistica.",

            7: "Jul: BR lidera exportaces. EUA em desenvolvimento de safra.",

            8: "Ago: BR ainda lidera. EUA preparando colheita.",

            9: "Set: Transicao -- EUA comeca colheita, BR desacelera.",

            10: "Out: EUA assume embarques. Colheita americana em pico.",

            11: "Nov: EUA domina. BR em plantio da nova safra.",

            12: "Dez: Entressafra BR. EUA lidera embarques.",

        }

        _note = _season_notes.get(_month, "")

        if _note:

            pdf.setFillColor(HexColor(CYAN))

            pdf.setFont("Helvetica-Bold", 6)

            pdf.drawString(M + 10, py, "CONTEXTO:")

            pdf.setFillColor(HexColor(TEXT_DIM))

            pdf.setFont("Helvetica", 6.5)

            pdf.drawString(M + 60, py, _note)

            py -= 16



        bar_full = 280

        bar_x0 = M + 200



        pdf.setFillColor(HexColor(AMBER))

        pdf.setFont("Helvetica-Bold", 8)

        pdf.drawString(M + 10, py, "EUA")

        pdf.setFillColor(HexColor(TEXT_MUT))

        pdf.setFont("Helvetica", 7)

        pdf.drawString(M + 40, py, f"Exportou {us_ytd:.1f} MMT ({us_pace:.0f}% do WASDE)")



        bench_w = bar_full * min(us_bench, 100) / 100

        actual_w = bar_full * min(us_pace, 100) / 100

        pdf.setFillColor(HexColor(BORDER))

        pdf.rect(bar_x0, py - 3, bar_full, 11, fill=1, stroke=0)

        us_bar_clr = GREEN if us_diff >= 0 else RED

        pdf.setFillColor(HexColor(us_bar_clr))

        pdf.rect(bar_x0, py - 3, actual_w, 11, fill=1, stroke=0)

        pdf.setStrokeColor(HexColor(TEXT))

        pdf.setLineWidth(1.5)

        bm_x = bar_x0 + bench_w

        pdf.line(bm_x, py - 5, bm_x, py + 10)



        pdf.setFillColor(HexColor(TEXT))

        pdf.setFont("Helvetica", 6)

        pdf.drawString(bar_x0 + bar_full + 8, py + 2, f"Real: {us_pace:.0f}%")

        pdf.setFillColor(HexColor(TEXT_DIM))

        pdf.drawString(bar_x0 + bar_full + 8, py - 7, f"Tipico: {us_bench}%")

        diff_clr = GREEN if us_diff >= 0 else RED

        us_sig_pt = sig_pt_map.get(us_sig, us_sig)

        pdf.setFillColor(HexColor(diff_clr))

        pdf.setFont("Helvetica-Bold", 7)

        pdf.drawString(bar_x0 + bar_full + 75, py, f"{us_diff:+.0f}pp  {us_sig_pt}")

        py -= 22



        pdf.setFillColor(HexColor(GREEN))

        pdf.setFont("Helvetica-Bold", 8)

        pdf.drawString(M + 10, py, "BRASIL")

        pdf.setFillColor(HexColor(TEXT_MUT))

        pdf.setFont("Helvetica", 7)

        pdf.drawString(M + 55, py, f"Exportou {br_ytd:.1f} MMT ({br_pace:.0f}% do WASDE)")



        bench_w = bar_full * min(br_bench, 100) / 100

        actual_w = bar_full * min(br_pace, 100) / 100

        pdf.setFillColor(HexColor(BORDER))

        pdf.rect(bar_x0, py - 3, bar_full, 11, fill=1, stroke=0)

        br_bar_clr = GREEN if br_diff >= 0 else RED

        pdf.setFillColor(HexColor(br_bar_clr))

        pdf.rect(bar_x0, py - 3, max(actual_w, 2), 11, fill=1, stroke=0)

        pdf.setStrokeColor(HexColor(TEXT))

        pdf.setLineWidth(1.5)

        bm_x = bar_x0 + bench_w

        pdf.line(bm_x, py - 5, bm_x, py + 10)



        pdf.setFillColor(HexColor(TEXT))

        pdf.setFont("Helvetica", 6)

        pdf.drawString(bar_x0 + bar_full + 8, py + 2, f"Real: {br_pace:.0f}%")

        pdf.setFillColor(HexColor(TEXT_DIM))

        pdf.drawString(bar_x0 + bar_full + 8, py - 7, f"Tipico: {br_bench}%")

        diff_clr = GREEN if br_diff >= 0 else RED

        br_sig_pt = sig_pt_map.get(br_sig, br_sig)

        pdf.setFillColor(HexColor(diff_clr))

        pdf.setFont("Helvetica-Bold", 7)

        pdf.drawString(bar_x0 + bar_full + 75, py, f"{br_diff:+.0f}pp  {br_sig_pt}")

        py -= 24



        pdf.setStrokeColor(HexColor(BORDER))

        pdf.setLineWidth(0.3)

        pdf.line(M + 10, py + 4, PAGE_W - M - 10, py + 4)

        py -= 8

        pdf.setFont("Helvetica-Bold", 5.5)

        pdf.setFillColor(HexColor(CYAN))

        pdf.drawString(M + 10, py, "COMO LER:")

        pdf.setFont("Helvetica", 5.5)

        pdf.setFillColor(HexColor(TEXT_DIM))

        pdf.drawString(M + 55, py, "Barra = exportaces acumuladas. Linha vertical = ritmo tipico nesta epoca. A frente = adiantado.")

        py -= 8

        pdf.drawString(M + 10, py, "EUA e BR tem safras em epocas diferentes. Comparar volume absoluto entre eles e enganoso.")



    pdf.setFillColor(HexColor(TEXT_DIM))

    pdf.setFont("Helvetica", 5.5)

    pdf.drawString(M, 22, "Fontes: CEPEA/ESALQ, CBOT/CME, BCB/PTAX, USDA AMS GTR, Comex Stat/MDIC, IMEA | AgriMacro Intelligence 2026")



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
    # INTEL sources
    intel_syn  = sload(DATA_PROC, "intel_synthesis.json")
    corr_data  = sload(DATA_PROC, "correlations.json")
    macro_ind  = sload(DATA_PROC, "macro_indicators.json")
    fw_data    = sload(DATA_PROC, "fedwatch.json")
    crop_prog  = sload(DATA_PROC, "crop_progress.json")
    gtrends    = sload(DATA_PROC, "google_trends.json")

    if not pr: print("  [ERRO] price_history.json nao encontrado!"); sys.exit(1)

    sk=list(pr.keys())[0]; sv=pr[sk]

    print(f"  [OK] prices: {len(pr)} symbols, {len(sv)} records" if isinstance(sv,list) else f"  [WARN] tipo: {type(sv)}")

    setup_mpl()

    # Load cross-analysis indicators (9 JSONs from bilateral/)

    cross_data = {}

    cross_dir = os.path.join(os.path.dirname(DATA_PROC), "bilateral")

    cross_files = [

        "basis_temporal", "cot_momentum", "crush_bilateral", "producer_margin",

        "interest_differential", "export_pace_weekly", "argentina_trilateral",

        "drought_accumulator", "freight_spread",

    ]

    for cf in cross_files:

        cf_path = os.path.join(cross_dir, f"{cf}.json")

        if os.path.exists(cf_path):

            with open(cf_path, "r", encoding="utf-8") as f:

                cross_data[cf] = json.load(f)

    print(f"  [OK] cross-analysis: {len(cross_data)}/{len(cross_files)} indicators loaded")





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

    T = 26  # 23 + council + composite + physical reativado

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

    # 2. Council AgriMacro
    pg_council(pdf, intel_syn, corr_data, sd, macro_ind, fw_data); ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 3. Composite Signals
    pg_composite_signals(pdf, corr_data, intel_syn); ftr(pdf,pn,T); pdf.showPage(); pn+=1
    # 4. Macro
    pg_macro(pdf,bcb,img_brl,macro_ind,fw_data);  ftr(pdf,pn,T); pdf.showPage(); pn+=1

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

    pg_stocks(pdf, sw, img_stocks, crop_prog);   ftr(pdf,pn,T); pdf.showPage(); pn+=1

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

    # Cross-Analysis Dashboard

    if cross_data:

        pg_cross_dashboard(pdf, cross_data);     ftr(pdf,pn,T); pdf.showPage(); pn+=1

    # COT Momentum and Basis

    if cross_data:

        pg_cot_basis(pdf, cd, cross_data);           ftr(pdf,pn,T); pdf.showPage(); pn+=1

    # Grain Ratios (after COT, before Physical)
    import os as _os_gr
    _gr_path = _os_gr.path.join(DATA_PROC, "grain_ratios.json")
    if _os_gr.path.exists(_gr_path):
        try:
            pg_grain_ratios(pdf, DATA_PROC); ftr(pdf,pn,T); pdf.showPage(); pn+=1
        except Exception as _e:
            print(f"    pg_grain_ratios SKIP: {_e}")

    # Physical
    pg_physical(pdf, phys, img_phbr);             ftr(pdf,pn,T); pdf.showPage(); pn+=1

    # 16. Weather

    pg_weather(pdf, wt, gtrends);                ftr(pdf,pn,T); pdf.showPage(); pn+=1

    # 17. Calendar

    pg_calendar(pdf, cal, rd, dr);              ftr(pdf,pn,T); pdf.showPage(); pn+=1

    # [REMOVIDO v3.3] pg_news a" pagina de noticias removida

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







