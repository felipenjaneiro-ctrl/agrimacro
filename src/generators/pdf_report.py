import os, sys, json
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT

DARK_BG = HexColor("#1a1a2e")
ACCENT = HexColor("#16213e")
GREEN = HexColor("#26a69a")
RED = HexColor("#ef5350")
YELLOW = HexColor("#f2c94c")
WHITE = HexColor("#ffffff")
GRAY = HexColor("#9aa4b2")
LIGHT = HexColor("#d1d4dc")

def get_styles():
    s = getSampleStyleSheet()
    s.add(ParagraphStyle("Title2", parent=s["Title"], fontSize=22, textColor=WHITE, alignment=TA_CENTER, spaceAfter=20))
    s.add(ParagraphStyle("H2", parent=s["Heading2"], fontSize=14, textColor=YELLOW, spaceBefore=12, spaceAfter=6))
    s.add(ParagraphStyle("Body2", parent=s["Normal"], fontSize=9, textColor=LIGHT, leading=13))
    s.add(ParagraphStyle("Small", parent=s["Normal"], fontSize=8, textColor=GRAY, leading=10))
    s.add(ParagraphStyle("Center", parent=s["Normal"], fontSize=9, textColor=LIGHT, alignment=TA_CENTER))
    return s

def page_bg(canvas, doc):
    canvas.saveState()
    canvas.setFillColor(DARK_BG)
    canvas.rect(0, 0, letter[0], letter[1], fill=1)
    canvas.setFillColor(GRAY)
    canvas.setFont("Helvetica", 7)
    canvas.drawString(inch, 0.4 * inch, f"AgriMacro v2.0 | {datetime.now().strftime('%Y-%m-%d')} | Dados reais | Nao e recomendacao")
    canvas.drawRightString(letter[0] - inch, 0.4 * inch, f"Pagina {doc.page}")
    canvas.restoreState()

def build_page1(story, styles, watch_data, spreads_data):
    story.append(Paragraph("AGRIMACRO - Relatorio Diario", styles["Title2"]))
    story.append(Paragraph(datetime.now().strftime("%d/%m/%Y"), styles["Center"]))
    story.append(Spacer(1, 20))
    story.append(Paragraph("ESTADO DO MERCADO", styles["H2"]))
    comms = watch_data.get("commodities", {})
    below = [(s, d) for s, d in comms.items() if "APERTO" in d.get("state", "")]
    above = [(s, d) for s, d in comms.items() if "EXCESSO" in d.get("state", "")]
    neutral = [(s, d) for s, d in comms.items() if "NEUTRO" in d.get("state", "")]
    story.append(Paragraph(f"Abaixo da media (aperto): {len(below)} | Acima (excesso): {len(above)} | Neutro: {len(neutral)}", styles["Body2"]))
    story.append(Spacer(1, 10))
    if below:
        rows = [["Commodity", "vs Media", "Estado"]]
        for sym, d in below:
            dev = d.get("price_vs_avg", 0)
            rows.append([sym, f"{dev:+.1f}%", d.get("state", "")])
        t = Table(rows, colWidths=[1.5*inch, 1.5*inch, 2.5*inch])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), ACCENT),
            ("TEXTCOLOR", (0, 0), (-1, -1), LIGHT),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, GRAY),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(t)
    story.append(Spacer(1, 15))
    story.append(Paragraph("SPREADS", styles["H2"]))
    sp = spreads_data.get("spreads", {})
    if sp:
        rows = [["Spread", "Valor", "Z-score", "Percentil", "Regime"]]
        for sid, d in sp.items():
            if d.get("status") != "ok": continue
            rows.append([d["name"], f"{d['current']:.4f}", str(d.get("zscore_1y", "")), f"{d.get('percentile', '')}%", d.get("regime", "")])
        t = Table(rows, colWidths=[1.3*inch, 1.2*inch, 1*inch, 1*inch, 1.2*inch])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), ACCENT),
            ("TEXTCOLOR", (0, 0), (-1, -1), LIGHT),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, GRAY),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(t)

def build_page2(story, styles, seasonality_data):
    story.append(PageBreak())
    story.append(Paragraph("SAZONALIDADE - Preco vs Media 5 Anos", styles["H2"]))
    story.append(Spacer(1, 10))
    rows = [["Commodity", "Preco Atual", "Media 5Y", "Desvio", "Posicao"]]
    items = []
    for sym, d in seasonality_data.items():
        s = d.get("stats")
        if not s: continue
        dev = s.get("deviation_pct", 0)
        pos = "ACIMA" if dev > 0 else "ABAIXO"
        items.append((sym, s["current_price"], s["avg_price"], dev, pos))
    items.sort(key=lambda x: x[3])
    for sym, cur, avg, dev, pos in items:
        rows.append([sym, f"{cur:.2f}", f"{avg:.2f}", f"{dev:+.1f}%", pos])
    t = Table(rows, colWidths=[1*inch, 1.3*inch, 1.3*inch, 1.2*inch, 1.2*inch])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), ACCENT),
        ("TEXTCOLOR", (0, 0), (-1, -1), LIGHT),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.5, GRAY),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t)

def build_page3(story, styles, watch_data):
    story.append(PageBreak())
    story.append(Paragraph("PERGUNTAS-GUIA DO DIA", styles["H2"]))
    story.append(Spacer(1, 10))
    questions = [
        "1. O que mudou desde ontem?",
        "2. O que NAO mudou, mas deveria?",
        "3. O que esta em extremo historico?",
        "4. O que o mercado parece estar ignorando?",
    ]
    for q in questions:
        story.append(Paragraph(q, styles["Body2"]))
        story.append(Spacer(1, 6))
    story.append(Spacer(1, 15))
    story.append(Paragraph("DESTAQUES", styles["H2"]))
    comms = watch_data.get("commodities", {})
    extremes = [(s, d) for s, d in comms.items() if "FORTE" in d.get("state", "")]
    if extremes:
        for sym, d in extremes:
            story.append(Paragraph(f"  {sym}: {d['state']} | {', '.join(d.get('factors', []))}", styles["Body2"]))
    else:
        story.append(Paragraph("  Nenhum extremo detectado hoje.", styles["Body2"]))
    story.append(Spacer(1, 15))
    story.append(Paragraph("DISCLAIMER", styles["Small"]))
    story.append(Paragraph("Este relatorio e apenas diagnostico de mercado. Nao constitui recomendacao de compra ou venda. Dados de fontes publicas (Yahoo Finance, USDA, CFTC). Uso exclusivamente educacional.", styles["Small"]))

def generate_pdf(output_path="outputs/reports"):
    os.makedirs(output_path, exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    filename = os.path.join(output_path, f"agrimacro_{today}.pdf")
    with open("data/processed/stocks_watch.json") as f:
        watch = json.load(f)
    with open("data/processed/spreads.json") as f:
        spreads = json.load(f)
    with open("data/processed/seasonality.json") as f:
        seas = json.load(f)
    doc = SimpleDocTemplate(filename, pagesize=letter, topMargin=0.7*inch, bottomMargin=0.7*inch, leftMargin=0.8*inch, rightMargin=0.8*inch)
    styles = get_styles()
    story = []
    build_page1(story, styles, watch, spreads)
    build_page2(story, styles, seas)
    build_page3(story, styles, watch)
    doc.build(story, onFirstPage=page_bg, onLaterPages=page_bg)
    print(f"PDF gerado: {filename}")
    return filename

if __name__ == "__main__":
    print("=" * 50)
    print("AGRIMACRO - GATE 4: RELATORIO PDF")
    print("=" * 50)
    f = generate_pdf()
    print(f"Pronto: {f}")
