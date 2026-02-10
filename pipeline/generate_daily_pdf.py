import json, sys, os
from datetime import datetime
from pathlib import Path
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.colors import HexColor, white
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                     TableStyle, PageBreak, HRFlowable)
except ImportError:
    os.system(f"{sys.executable} -m pip install reportlab --quiet")
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.colors import HexColor, white
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                     TableStyle, PageBreak, HRFlowable)

BASE = Path(r"C:\Users\felip\OneDrive") / "Área de Trabalho" / "agrimacro"
DATA = BASE / "agrimacro-dash" / "public" / "data"
RAW, PROC = DATA / "raw", DATA / "processed"
REPORTS = BASE / "reports"; REPORTS.mkdir(exist_ok=True)
TODAY = datetime.now().strftime("%Y-%m-%d")
OUT = REPORTS / f"AgriMacro_{TODAY}.pdf"

BG = HexColor("#1a2332"); GRN = HexColor("#4caf50"); RED = HexColor("#f44336")
BLU = HexColor("#1565c0"); TXT = HexColor("#1a1a2e"); MUT = HexColor("#6c757d")
BDR = HexColor("#dee2e6"); ALT = HexColor("#f5f7fa")

NAMES = {"ZC":"Milho","ZS":"Soja","ZW":"Trigo CBOT","KE":"Trigo KC","ZM":"Farelo Soja",
    "ZL":"Oleo Soja","ZR":"Arroz","ZO":"Aveia","LE":"Boi Gordo","HE":"Suino Magro",
    "GF":"Feeder Cattle","KC":"Cafe Arabica","CC":"Cacau","SB":"Acucar #11",
    "CT":"Algodao #2","OJ":"Suco Laranja","CL":"Petroleo WTI","NG":"Gas Natural",
    "RB":"Gasolina","HO":"Heating Oil","EH":"Etanol","GC":"Ouro","SI":"Prata","DX":"Dolar Index"}
GROUPS = {"GRAOS":["ZC","ZS","ZW","KE","ZM","ZL"],"SOFTS":["KC","CC","SB","CT","OJ"],
    "PECUARIA":["LE","GF","HE"],"ENERGIA":["CL","NG"],"METAIS/MACRO":["GC","SI","DX"]}

def lj(p):
    try:
        with open(p,"r",encoding="utf-8") as f: return json.load(f)
    except: return None

def fp(v):
    if v is None: return chr(8212)
    try: v=float(v); return f"{v:,.2f}" if v>=10 else f"{v:.4f}"
    except: return str(v)

def fpct(v):
    if v is None: return chr(8212)
    try: v=float(v); return f"{'+' if v>0 else ''}{v:.2f}%"
    except: return str(v)

class PDF:
    def __init__(self):
        self.d, self.story = {}, []
        self.W, self.H = A4
        b = getSampleStyleSheet()
        self.s = {
            "t": ParagraphStyle("T",parent=b["Title"],fontSize=24,textColor=TXT,fontName="Helvetica-Bold",alignment=TA_CENTER,spaceAfter=4),
            "sec": ParagraphStyle("S",parent=b["Heading1"],fontSize=13,textColor=white,backColor=BG,borderPadding=(5,8,5,8),fontName="Helvetica-Bold",spaceBefore=14,spaceAfter=6),
            "sub": ParagraphStyle("Sub",parent=b["Heading2"],fontSize=10,textColor=BLU,fontName="Helvetica-Bold",spaceBefore=8,spaceAfter=3),
            "b": ParagraphStyle("B",parent=b["Normal"],fontSize=7.5,textColor=TXT,leading=10),
            "sm": ParagraphStyle("Sm",parent=b["Normal"],fontSize=6,textColor=MUT,leading=8),
            "c": ParagraphStyle("C",parent=b["Normal"],fontSize=9,textColor=MUT,alignment=TA_CENTER,spaceAfter=10),
        }

    def hf(self,c,doc):
        c.saveState()
        c.setStrokeColor(BLU);c.setLineWidth(1.5)
        c.line(12*mm,self.H-12*mm,self.W-12*mm,self.H-12*mm)
        c.setFont("Helvetica-Bold",6.5);c.setFillColor(MUT)
        c.drawString(12*mm,self.H-10*mm,"AGRIMACRO v3.2 - Relatorio Diario")
        c.drawRightString(self.W-12*mm,self.H-10*mm,TODAY)
        c.setFont("Helvetica",5.5)
        c.drawString(12*mm,7*mm,"Yahoo | IBKR | USDA | CEPEA | MAGyP | CFTC | BCB | IBGE | EIA")
        c.drawRightString(self.W-12*mm,7*mm,f"Pag {doc.page}")
        c.restoreState()

    def tb(self,rows,cw,cc=None):
        t=Table(rows,colWidths=cw)
        cm=[("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),6.5),
            ("BACKGROUND",(0,0),(-1,0),BG),("TEXTCOLOR",(0,0),(-1,0),white),
            ("TEXTCOLOR",(0,1),(-1,-1),TXT),("ALIGN",(0,0),(-1,-1),"CENTER"),
            ("ALIGN",(1,0),(1,-1),"LEFT"),("VALIGN",(0,0),(-1,-1),"MIDDLE"),
            ("BOTTOMPADDING",(0,0),(-1,-1),3),("TOPPADDING",(0,0),(-1,-1),3),
            ("GRID",(0,0),(-1,-1),.4,BDR)]
        for i in range(1,len(rows)):
            if i%2==0: cm.append(("BACKGROUND",(0,i),(-1,i),ALT))
            if cc is not None:
                try:
                    vs=str(rows[i][cc])
                    if vs and vs!=chr(8212):
                        v=float(vs.replace("+","").replace("%","").replace(",",""))
                        cl=GRN if v>0 else RED if v<0 else TXT
                        cm.append(("TEXTCOLOR",(cc,i),(cc,i),cl))
                except: pass
        t.setStyle(TableStyle(cm)); return t

    def load(self):
        print("Carregando dados...")
        # Nomes REAIS dos JSONs do pipeline
        for k,p in [
            ("prices", RAW/"price_history.json"),
            ("physical", PROC/"physical.json"),
            ("physical_intl", PROC/"physical_intl.json"),
            ("stocks", PROC/"stocks_watch.json"),
            ("spreads", PROC/"spreads.json"),
            ("seasonality", PROC/"seasonality.json"),
            ("cot", PROC/"cot.json"),
            ("futures", PROC/"futures_contracts.json"),
            ("reading", PROC/"daily_reading.json"),
            ("report", PROC/"report_daily.json"),
            ("bcb", PROC/"bcb_data.json"),
            ("ibge", PROC/"ibge_data.json"),
            ("eia", PROC/"eia_data.json"),
            ("usda_fas", PROC/"usda_fas.json"),
            ("last_run", DATA/"last_run.json"),
        ]:
            self.d[k]=lj(p); print(f"  {k}: {'OK' if self.d[k] else 'nao encontrado'}")

    def build(self):
        doc=SimpleDocTemplate(str(OUT),pagesize=A4,topMargin=15*mm,bottomMargin=15*mm,leftMargin=12*mm,rightMargin=12*mm)
        self._cover(); self._overview(); self._destaques(); self._macro(); self._brasil(); self._energia(); self._stocks(); self._spreads()
        self._physical(); self._physical_intl(); self._cot(); self._fut()
        self._reading(); self._disc()
        print("\nGerando PDF..."); doc.build(self.story,onFirstPage=self.hf,onLaterPages=self.hf)
        print(f"[OK] {OUT} ({OUT.stat().st_size/1024:.0f} KB)")

    def _cover(self):
        lr = self.d.get("last_run",{})
        res = lr.get("results",{})
        ok = sum(1 for v in res.values() if v.get("status")=="OK")
        rpt = self.d.get("report",{})
        titulo = rpt.get("titulo","Relatorio Diario")
        subtitulo = rpt.get("subtitulo","")
        resumo = rpt.get("resumo_executivo","")
        score = rpt.get("score_volatilidade","")
        riscos = rpt.get("principais_riscos",[])

        self.story+=[Spacer(1,35*mm),
            Paragraph("AGRIMACRO",ParagraphStyle("CT",parent=self.s["t"],fontSize=38,textColor=BLU)),
            Spacer(1,2*mm),Paragraph("COMMODITIES DASHBOARD v3.2",self.s["c"]),
            Spacer(1,6*mm),HRFlowable(width="50%",thickness=2,color=BLU,spaceAfter=4*mm,hAlign="CENTER"),
            Paragraph(f"Relatorio Diario - {datetime.now().strftime('%d/%m/%Y')}",self.s["c"]),
            Spacer(1,4*mm)]

        # Title from Claude report
        if titulo:
            self.story.append(Paragraph(titulo,ParagraphStyle("TT",parent=self.s["t"],fontSize=16,textColor=TXT)))
            self.story.append(Spacer(1,2*mm))
        if subtitulo:
            self.story.append(Paragraph(subtitulo,ParagraphStyle("ST",parent=self.s["c"],fontSize=11,textColor=BLU)))
            self.story.append(Spacer(1,4*mm))

        # Resumo executivo
        if resumo:
            self.story.append(Paragraph(resumo,ParagraphStyle("RE",parent=self.s["b"],fontSize=8.5,leading=12,textColor=TXT)))
            self.story.append(Spacer(1,4*mm))

        # Score + Riscos
        if score:
            score_txt = f"Volatilidade: {score}/10"
            if riscos:
                score_txt += f"  |  Riscos: {'; '.join(riscos[:3])}"
            self.story.append(Paragraph(score_txt,ParagraphStyle("SC",parent=self.s["sm"],fontSize=7,textColor=MUT)))
            self.story.append(Spacer(1,4*mm))

        self.story.append(Paragraph(f"Pipeline: {ok}/{len(res)} steps OK | {lr.get('elapsed_seconds',0):.0f}s",self.s["c"]))
        self.story.append(Spacer(1,8*mm))
        self.story.append(PageBreak())

    def _overview(self):
        prices = self.d.get("prices") or {}
        self.story.append(Paragraph("VISAO GERAL - Precos e Variacoes",self.s["sec"]))
        self.story.append(Spacer(1,3*mm))
        for g,tks in GROUPS.items():
            self.story.append(Paragraph(g,self.s["sub"]))
            rows=[["Ticker","Commodity","Ultimo","Var 1D","Var 5D","Min 52s","Max 52s","Vol"]]
            for tk in tks:
                candles = prices.get(tk,[])
                if candles and len(candles)>=2:
                    last = candles[-1]
                    prev = candles[-2]
                    close = last.get("close",0)
                    chg1d = ((close/prev["close"])-1)*100 if prev.get("close") else None
                    chg5d = ((close/candles[-6]["close"])-1)*100 if len(candles)>=6 and candles[-6].get("close") else None
                    closes = [c["close"] for c in candles[-252:] if c.get("close")]
                    mn52 = min(closes) if closes else None
                    mx52 = max(closes) if closes else None
                    rows.append([tk, NAMES.get(tk,tk), fp(close), fpct(chg1d), fpct(chg5d),
                        fp(mn52), fp(mx52),
                        f"{int(last.get('volume',0)):,}" if last.get("volume") else chr(8212)])
                else:
                    rows.append([tk, NAMES.get(tk,tk)]+[chr(8212)]*6)
            self.story+=[self.tb(rows,[12*mm,24*mm,18*mm,16*mm,16*mm,18*mm,18*mm,18*mm],cc=3),Spacer(1,3*mm)]
        self.story.append(PageBreak())

    def _stocks(self):
        sw = self.d.get("stocks")
        self.story.append(Paragraph("STOCKS WATCH - Estoques e Sinais",self.s["sec"])); self.story.append(Spacer(1,3*mm))
        if not sw: self.story+=[Paragraph("Indisponivel.",self.s["b"]),PageBreak()]; return
        cms = sw.get("commodities",{})
        rows=[["Ticker","Commodity","Estoque","Unidade","vs Media","Sinal"]]
        for tk in sorted(cms.keys()):
            c = cms[tk]
            rows.append([tk, NAMES.get(tk,tk), fp(c.get("stock_current")),
                str(c.get("stock_unit",chr(8212))),
                fpct(c.get("price_vs_avg")),
                str(c.get("state",chr(8212))).replace("_"," ")])
        if len(rows)>1: self.story.append(self.tb(rows,[12*mm,22*mm,18*mm,20*mm,18*mm,40*mm],cc=4))
        self.story.append(PageBreak())

    def _spreads(self):
        sp = self.d.get("spreads")
        self.story.append(Paragraph("SPREADS / MARGENS INDUSTRIAIS",self.s["sec"])); self.story.append(Spacer(1,3*mm))
        if not sp: self.story+=[Paragraph("Indisponivel.",self.s["b"]),PageBreak()]; return
        sps = sp.get("spreads",{})
        rows=[["Spread","Valor","Media 1Y","Z-Score","Percentil","Regime","Tendencia"]]
        for name, s in sps.items():
            rows.append([
                s.get("name",name),
                fp(s.get("current")),
                fp(s.get("mean_1y")),
                f"{s.get('zscore_1y',0):.2f}" if s.get("zscore_1y") is not None else chr(8212),
                f"{s.get('percentile',0):.0f}%" if s.get("percentile") is not None else chr(8212),
                str(s.get("regime",chr(8212))),
                f"{s.get('trend',chr(8212))} ({fpct(s.get('trend_pct'))})"
            ])
        if len(rows)>1: self.story.append(self.tb(rows,[30*mm,16*mm,16*mm,16*mm,16*mm,18*mm,24*mm]))
        self.story.append(PageBreak())

    def _physical(self):
        ph = self.d.get("physical")
        self.story.append(Paragraph("FISICO EUA - USDA Cash Prices",self.s["sec"])); self.story.append(Spacer(1,3*mm))
        if not ph: self.story+=[Paragraph("Indisponivel.",self.s["b"]),PageBreak()]; return
        us = ph.get("us_cash",{})
        rows=[["Ticker","Commodity","Cash","Futures","Basis","Basis%","Periodo"]]
        for tk, c in sorted(us.items()):
            rows.append([tk, c.get("label",NAMES.get(tk,tk)),
                fp(c.get("cash_price")), fp(c.get("futures_price")),
                fp(c.get("basis")), fpct(c.get("basis_pct")),
                str(c.get("period",chr(8212)))])
        if len(rows)>1: self.story.append(self.tb(rows,[12*mm,28*mm,18*mm,18*mm,16*mm,16*mm,20*mm],cc=5))
        self.story.append(PageBreak())

    def _physical_intl(self):
        ph = self.d.get("physical_intl")
        self.story.append(Paragraph("FISICO INTL - CEPEA + MAGyP",self.s["sec"])); self.story.append(Spacer(1,3*mm))
        if not ph: self.story+=[Paragraph("Indisponivel.",self.s["b"]),PageBreak()]; return

        # Formato: ph["international"]["ZS_BR"] = {label, price, price_unit, period, trend, source}
        intl = ph.get("international",{})
        if not intl and isinstance(ph, dict):
            # Talvez os dados estejam direto no root
            intl = {k:v for k,v in ph.items() if isinstance(v, dict) and "price" in str(v)}

        rows = [["Mercado","Preco","Unidade","Tendencia","Data","Fonte"]]
        for key, item in sorted(intl.items()):
            if not isinstance(item, dict): continue
            label = item.get("label", key)
            # Limpar emojis e encoding
            label = label.encode("ascii","ignore").decode("ascii").strip()
            if not label: label = key
            price = item.get("price")
            unit = item.get("price_unit", item.get("unit",""))
            period = item.get("period","")
            trend = item.get("trend","")
            source = item.get("source","")
            rows.append([
                label[:30],
                fp(price),
                str(unit)[:15],
                str(trend),
                str(period)[:10],
                str(source)[:20],
            ])

        if len(rows) > 1:
            self.story.append(self.tb(rows,[32*mm,18*mm,18*mm,16*mm,18*mm,24*mm]))
        else:
            self.story.append(Paragraph("Sem dados disponiveis.",self.s["b"]))
        self.story.append(PageBreak())



    def _cot(self):
        ct = self.d.get("cot")
        self.story.append(Paragraph("COT - Commitment of Traders (CFTC)",self.s["sec"])); self.story.append(Spacer(1,3*mm))
        if not ct: self.story+=[Paragraph("Rode collect_cot.py primeiro.",self.s["b"]),PageBreak()]; return
        cms=ct.get("commodities",{})
        self.story.append(Paragraph("Legacy - Commercial vs Non-Commercial",self.s["sub"]))
        rows=[["Tk","Commodity","Comm Net","NonComm Net","OI","Data"]]
        for tk,d in sorted(cms.items()):
            l=d.get("legacy",{}).get("latest",{})
            if l: rows.append([tk,d.get("name",tk),f"{l.get('comm_net',0):,.0f}" if l.get("comm_net") is not None else chr(8212),
                f"{l.get('noncomm_net',0):,.0f}" if l.get("noncomm_net") is not None else chr(8212),
                f"{l.get('open_interest',0):,.0f}" if l.get("open_interest") is not None else chr(8212),str(l.get("date",chr(8212)))])
        if len(rows)>1: self.story.append(self.tb(rows,[12*mm,24*mm,24*mm,24*mm,24*mm,22*mm]))
        self.story.append(Spacer(1,4*mm))
        self.story.append(Paragraph("Disaggregated - Managed Money + Producer",self.s["sub"]))
        rows=[["Tk","Commodity","MM Net","Producer Net","Swap Net","OI"]]
        for tk,d in sorted(cms.items()):
            l=d.get("disaggregated",{}).get("latest",{})
            if l: rows.append([tk,d.get("name",tk),
                f"{l.get('managed_money_net',0):,.0f}" if l.get("managed_money_net") is not None else chr(8212),
                f"{l.get('producer_net',0):,.0f}" if l.get("producer_net") is not None else chr(8212),
                f"{l.get('swap_net',0):,.0f}" if l.get("swap_net") is not None else chr(8212),
                f"{l.get('open_interest',0):,.0f}" if l.get("open_interest") is not None else chr(8212)])
        if len(rows)>1: self.story.append(self.tb(rows,[12*mm,22*mm,26*mm,22*mm,22*mm,22*mm]))
        self.story.append(PageBreak())

    def _fut(self):
        """Futuros: versão compacta — front-month + resumo curva por commodity"""
        ft = self.d.get("futures")
        self.story.append(Paragraph("CONTRATOS FUTUROS - Resumo",self.s["sec"])); self.story.append(Spacer(1,3*mm))
        if not ft: self.story+=[Paragraph("Rode collect_futures_contracts.py.",self.s["b"]),PageBreak()]; return

        # Tabela compacta: 1 linha por commodity (front-month)
        rows=[["Tk","Commodity","Front","Ultimo","Var","Vol","Curva","Spread F/B"]]
        for tk,d in sorted(ft.get("commodities",{}).items()):
            cs=d.get("contracts",[])
            if not cs: continue
            # Front month = primeiro contrato com dados
            front = cs[0] if cs else {}
            # Estrutura da curva
            sps = d.get("spreads",[])
            structure = ""
            spread_val = ""
            if sps:
                structure = sps[0].get("structure","").upper() if sps[0].get("structure") else ""
                sv = sps[0].get("spread")
                spread_val = f"{sv:.2f}" if sv is not None else chr(8212)

            # Variação (se tiver mais de 1 contrato, comparar)
            close = front.get("close")
            var_str = chr(8212)

            rows.append([
                tk,
                d.get("name",tk)[:18],
                front.get("contract",chr(8212)),
                fp(close),
                var_str,
                f"{front['volume']:,}" if front.get("volume") else chr(8212),
                structure if structure else chr(8212),
                spread_val if spread_val else chr(8212),
            ])

        if len(rows) > 1:
            self.story.append(self.tb(rows,[11*mm,22*mm,14*mm,18*mm,14*mm,16*mm,16*mm,16*mm]))

        # Mini tabela de curva para commodities-chave
        key_tickers = ["ZC","ZS","ZW","KC","CL","LE"]
        self.story.append(Spacer(1,4*mm))
        self.story.append(Paragraph("Curva de Futuros - Commodities Chave",self.s["sub"]))

        for tk in key_tickers:
            d = ft.get("commodities",{}).get(tk)
            if not d: continue
            cs = d.get("contracts",[])
            if len(cs) < 2: continue

            # Mostrar max 4 contratos
            show = cs[:4]
            rows = [[c.get("contract",chr(8212)) for c in show]]
            rows.append([fp(c.get("close")) for c in show])
            rows.append([f"{c['volume']:,}" if c.get("volume") else chr(8212) for c in show])

            # Header com nome
            self.story.append(Paragraph(f"{tk} - {d.get('name',tk)}",
                ParagraphStyle("FK",parent=self.s["b"],fontSize=7,fontName="Helvetica-Bold",textColor=BLU)))

            cw = [30*mm] * min(4, len(show))
            t = Table([["Contrato"] + rows[0][1:]] if len(rows[0])>1 else rows,
                colWidths=cw[:len(show)])
            # Simplificado: só contratos + preços inline
            labels = ["Contrato","Preco","Volume"]
            mini_rows = []
            for i, label in enumerate(labels):
                if i < len(rows):
                    mini_rows.append([label] + rows[i])
            if mini_rows:
                cw2 = [16*mm] + [24*mm]*len(show)
                t2 = Table(mini_rows, colWidths=cw2[:len(show)+1])
                t2.setStyle(TableStyle([
                    ("FONTSIZE",(0,0),(-1,-1),6),("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),
                    ("ALIGN",(0,0),(-1,-1),"CENTER"),("GRID",(0,0),(-1,-1),.3,BDR),
                    ("BACKGROUND",(0,0),(0,-1),ALT),("BOTTOMPADDING",(0,0),(-1,-1),2),
                    ("TOPPADDING",(0,0),(-1,-1),2)]))
                self.story.append(t2)
            self.story.append(Spacer(1,2*mm))

        self.story.append(PageBreak())


    def _destaques(self):
        rpt = self.d.get("report",{})
        destaques = rpt.get("destaques",[])
        if not destaques: return
        self.story.append(Paragraph("DESTAQUES DO DIA",self.s["sec"]))
        self.story.append(Spacer(1,3*mm))
        for d in destaques:
            titulo = d.get("titulo","")
            corpo = d.get("corpo","").replace("\n","<br/>")
            impacto = d.get("impacto_produtor","")
            self.story.append(Paragraph(f'{titulo} [{d.get("commodity","")}]',self.s["sub"]))
            self.story.append(Paragraph(corpo,self.s["b"]))
            if impacto:
                self.story.append(Spacer(1,1*mm))
                self.story.append(Paragraph(f'<b>Impacto produtor:</b> {impacto}',
                    ParagraphStyle("IP",parent=self.s["b"],fontSize=7,textColor=BLU,leftIndent=4*mm)))
            self.story.append(Spacer(1,3*mm))
        self.story.append(PageBreak())

    def _macro(self):
        rpt = self.d.get("report",{})
        macro = rpt.get("macro_e_cambio",{})
        cal = rpt.get("calendario_semana",[])
        analise = rpt.get("analise_tecnica_resumo","")
        pergunta = rpt.get("pergunta_do_dia","")
        frase = rpt.get("frase_do_dia","")
        if not macro and not cal: return

        self.story.append(Paragraph("MACRO, CAMBIO E CALENDARIO",self.s["sec"]))
        self.story.append(Spacer(1,3*mm))

        # Macro items
        if macro:
            for key, txt in macro.items():
                label = key.replace("_"," ").title()
                self.story.append(Paragraph(f'<b>{label}:</b> {txt}',self.s["b"]))
                self.story.append(Spacer(1,2*mm))

        # Calendario
        if cal:
            self.story.append(Spacer(1,3*mm))
            self.story.append(Paragraph("Calendario da Semana",self.s["sub"]))
            rows = [["Data","Evento","Impacto","Relevancia"]]
            for ev in cal:
                rows.append([ev.get("data",""),ev.get("evento",""),
                    ev.get("impacto","").upper(),ev.get("relevancia","")])
            t = Table(rows, colWidths=[16*mm,40*mm,16*mm,68*mm])
            t.setStyle(TableStyle([
                ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),("FONTSIZE",(0,0),(-1,-1),6.5),
                ("BACKGROUND",(0,0),(-1,0),BG),("TEXTCOLOR",(0,0),(-1,0),white),
                ("TEXTCOLOR",(0,1),(-1,-1),TXT),("ALIGN",(0,0),(-1,-1),"LEFT"),
                ("VALIGN",(0,0),(-1,-1),"MIDDLE"),("GRID",(0,0),(-1,-1),.4,BDR),
                ("BOTTOMPADDING",(0,0),(-1,-1),3),("TOPPADDING",(0,0),(-1,-1),3)]))
            self.story.append(t)

        # Analise tecnica
        if analise:
            self.story.append(Spacer(1,4*mm))
            self.story.append(Paragraph("Analise Tecnica",self.s["sub"]))
            self.story.append(Paragraph(analise,self.s["b"]))

        # Pergunta + Frase
        if pergunta:
            self.story.append(Spacer(1,4*mm))
            self.story.append(Paragraph(f'<b>Pergunta do dia:</b> {pergunta}',
                ParagraphStyle("PD",parent=self.s["b"],fontSize=7.5,textColor=BLU)))
        if frase:
            self.story.append(Spacer(1,2*mm))
            self.story.append(Paragraph(f'<i>"{frase}"</i>',
                ParagraphStyle("FD",parent=self.s["b"],fontSize=7,textColor=MUT)))

        self.story.append(PageBreak())

    def _brasil(self):
        bcb = self.d.get("bcb")
        ibge = self.d.get("ibge")
        if not bcb and not ibge: return
        self.story.append(Paragraph("BRASIL: CAMBIO, JUROS E INFLACAO",self.s["sec"]))
        self.story.append(Spacer(1,3*mm))

        # --- BCB ---
        # Formato: bcb["brl_usd"] = lista de {"data":"DD/MM/YYYY","valor":5.23}
        # Meta em bcb["_meta"]["series"]["brl_usd"]["name"]
        if bcb and isinstance(bcb, dict):
            meta_series = bcb.get("_meta",{}).get("series",{})
            bcb_map = {
                "brl_usd": "BRL/USD (PTAX)",
                "selic_meta": "Selic Meta (% a.a.)",
                "selic_diaria": "Selic Diaria (% a.d.)",
                "cdi_diario": "CDI Diario (% a.d.)",
                "ipca_mensal": "IPCA Mensal (%)",
                "igpm_mensal": "IGP-M Mensal (%)",
                "credito_rural": "Credito Rural (R$ mi)",
                "reservas_intl": "Reservas Intl (US$ mi)",
            }
            rows = [["Indicador","Valor Atual","Data","Var vs Anterior"]]
            for key, label in bcb_map.items():
                series_data = bcb.get(key)
                if not series_data or not isinstance(series_data, list) or len(series_data) == 0:
                    continue
                last = series_data[-1]
                val = (last.get("value") or last.get("valor")) if isinstance(last, dict) else last
                date = (last.get("date") or last.get("data","")) if isinstance(last, dict) else ""
                # Calcular variação vs anterior
                var = None
                var_str = None
                if len(series_data) >= 2:
                    prev = series_data[-2]
                    pv = (prev.get("value") or prev.get("valor")) if isinstance(prev, dict) else prev
                    if val is not None and pv is not None and pv != 0:
                        try:
                            # Para câmbio e taxas: variação percentual
                            fv, fp2 = float(val), float(pv)
                            if key in ("ipca_mensal","igpm_mensal"):
                                diff = fv - fp2
                                var = None; var_str = f"{'+' if diff>0 else ''}{diff:.2f} pp"
                            elif fp2 != 0:
                                var = ((fv - fp2) / abs(fp2)) * 100; var_str = None
                            else:
                                var = None; var_str = None
                        except:
                            var = None
                # Formatar valor
                try:
                    fval = float(val)
                    if "reservas" in key:
                        val_str = f"{fval:,.0f}"
                    elif "credito" in key:
                        val_str = f"{fval:,.2f}"
                    elif "brl" in key or "eur" in key:
                        val_str = f"{fval:.4f}"
                    elif "selic_meta" in key:
                        val_str = f"{fval:.2f}"
                    elif fval < 1:
                        val_str = f"{fval:.4f}"
                    else:
                        val_str = f"{fval:.2f}"
                except:
                    val_str = str(val) if val is not None else chr(8212)
                rows.append([label, val_str, str(date)[:10], var_str if var_str else fpct(var)])
            if len(rows) > 1:
                self.story.append(Paragraph("Indicadores BCB/SGS",self.s["sub"]))
                self.story.append(self.tb(rows,[34*mm,24*mm,22*mm,22*mm],cc=3))
                self.story.append(Spacer(1,3*mm))

        # --- IBGE ---
        # Formato: ibge["ipca_alimentos"] = lista de {"periodo":"202412","mes":"dezembro 2024","valor":0.33}
        if ibge and isinstance(ibge, dict):
            ibge_map = {
                "ipca_alimentos": "IPCA Alimentos (%)",
                # ipca_geral removido: é índice acumulado, não variação
                "pib_agro": "PIB Agropecuaria (R$ mi)",
                "abate_bovinos": "Abate Bovinos (cabecas)",
            }
            rows = [["Indicador","Ultimo","Periodo","Acum 12m"]]
            for key, label in ibge_map.items():
                series_data = ibge.get(key)
                if not series_data or not isinstance(series_data, list) or len(series_data) == 0:
                    continue
                last = series_data[-1]
                val = (last.get("value") or last.get("valor")) if isinstance(last, dict) else last
                periodo = last.get("periodo", last.get("mes","")) if isinstance(last, dict) else ""
                # Acumulado 12 meses (somar últimos 12 se for mensal %)
                acum12 = None
                if "ipca" in key or "igp" in key:
                    ultimos12 = series_data[-12:] if len(series_data) >= 12 else series_data
                    vals12 = []
                    for item in ultimos12:
                        v = item.get("valor") if isinstance(item, dict) else item
                        if v is not None:
                            try: vals12.append(float(v))
                            except: pass
                    if vals12:
                        acum12 = sum(vals12)
                # Formatar
                try:
                    fval = float(val)
                    if fval > 10000:
                        val_str = f"{fval:,.0f}"
                    else:
                        val_str = f"{fval:.2f}"
                except:
                    val_str = str(val) if val is not None else chr(8212)

                acum_str = f"{acum12:.2f}%" if acum12 is not None else chr(8212)
                rows.append([label, val_str, str(periodo), acum_str])
            if len(rows) > 1:
                self.story.append(Paragraph("Indicadores IBGE/SIDRA",self.s["sub"]))
                self.story.append(self.tb(rows,[34*mm,22*mm,22*mm,26*mm]))

        self.story.append(PageBreak())



    def _energia(self):
        eia = self.d.get("eia")
        if not eia: return
        series = eia.get("series",{})
        if not series: return
        self.story.append(Paragraph("ENERGIA - EIA Weekly Data",self.s["sec"]))
        self.story.append(Spacer(1,3*mm))

        # Ordem e labels customizados
        eia_order = [
            ("wti_spot", "WTI Spot ($/bbl)"),
            ("diesel_retail", "Diesel Retail ($/gal)"),
            ("gasoline_retail", "Gasolina Retail ($/gal)"),
            ("ethanol_production", "Etanol Producao (MBbl/d)"),
            ("ethanol_stocks", "Etanol Estoques (MBbl)"),
            ("crude_stocks", "Crude Oil Estoques (MBbl)"),
            ("natural_gas_spot", "Gas Natural HH ($/MMBtu)"),
        ]

        rows = [["Indicador","Valor","Periodo","Var Sem","Var Mes"]]
        for sid, label in eia_order:
            s = series.get(sid)
            if not s or s.get("latest_value") is None: continue
            val = s["latest_value"]
            period = str(s.get("latest_period",""))[:10]

            # Recalcular WoW e MoM usando history corretamente
            history = s.get("history",[])
            wow = None
            mom = None
            if len(history) >= 2:
                # history[0] = mais recente, history[1] = semana anterior
                # Mas verificar se períodos são realmente diferentes
                vals_by_period = {}
                for h in history:
                    p = h.get("period","")
                    v = h.get("value")
                    if p and v is not None and p not in vals_by_period:
                        vals_by_period[p] = v
                sorted_periods = sorted(vals_by_period.keys(), reverse=True)
                if len(sorted_periods) >= 2:
                    curr = vals_by_period[sorted_periods[0]]
                    prev = vals_by_period[sorted_periods[1]]
                    if prev and prev != 0:
                        wow = ((curr - prev) / abs(prev)) * 100
                if len(sorted_periods) >= 5:
                    month_ago = vals_by_period[sorted_periods[4]]
                    curr = vals_by_period[sorted_periods[0]]
                    if month_ago and month_ago != 0:
                        mom = ((curr - month_ago) / abs(month_ago)) * 100

            # Formatar valor
            try:
                fval = float(val)
                if "price" in sid or "spot" in sid or "retail" in sid:
                    val_str = f"${fval:.2f}"
                elif fval > 10000:
                    val_str = f"{fval:,.0f}"
                elif fval > 100:
                    val_str = f"{fval:,.0f}"
                else:
                    val_str = f"{fval:.1f}"
            except:
                val_str = fp(val)

            rows.append([label, val_str, period, fpct(wow), fpct(mom)])

        if len(rows) > 1:
            self.story.append(self.tb(rows,[36*mm,20*mm,20*mm,18*mm,18*mm],cc=3))

        self.story.append(PageBreak())



    def _reading(self):
        rd = self.d.get("reading")
        self.story.append(Paragraph("LEITURA DO DIA",self.s["sec"])); self.story.append(Spacer(1,3*mm))
        if not rd: self.story+=[Paragraph("Indisponivel.",self.s["b"])]; return
        blocos = rd.get("blocos",[])
        for bl in blocos:
            title = bl.get("title","")
            body = bl.get("body","")
            self.story.append(Paragraph(title, self.s["sub"]))
            self.story.append(Paragraph(body, self.s["b"]))
            self.story.append(Spacer(1,2*mm))

    def _disc(self):
        self.story+=[Spacer(1,6*mm),HRFlowable(width="100%",thickness=1,color=BDR,spaceAfter=3*mm),
            Paragraph("AVISO: Relatorio automatico AgriMacro v3.2. Dados publicos. Nao constitui recomendacao.",self.s["sm"]),
            Paragraph(f"Gerado {datetime.now().strftime('%d/%m/%Y %H:%M')} | Analise, reports comerciais e video.",self.s["sm"])]

if __name__=="__main__":
    print("="*60); print("  AgriMacro v3.2 - Relatorio PDF"); print("="*60); print()
    p=PDF(); p.load(); p.build()
