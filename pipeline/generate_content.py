"""
generate_content.py — AgriMacro v3.2 Content Generator
Produz dois outputs a partir dos dados do pipeline:

1. PDF VISUAL — Relatório com gráficos e análises editoriais (não tabelas)
2. SCRIPT DE VÍDEO — Para HeyGen / YouTube estilo telejornal agro

Público: produtores rurais, profissionais do agro, traders de commodities
Linguagem: PT-BR, direta, profissional mas acessível
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from io import BytesIO

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm as MM
from reportlab.lib.colors import HexColor, white, black
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                 TableStyle, PageBreak, HRFlowable, Image)

# =============================================================================
# CORES E ESTILOS
# =============================================================================
DARK = HexColor("#1a2332")
BLUE = HexColor("#1565c0")
GREEN = HexColor("#2e7d32")
RED = HexColor("#c62828")
ORANGE = HexColor("#e65100")
GRAY = HexColor("#757575")
LIGHT = HexColor("#f5f7fa")
TXT = HexColor("#1a1a2e")
WHITE = white

# Chart colors
C_GREEN = "#2e7d32"
C_RED = "#c62828"
C_BLUE = "#1565c0"
C_ORANGE = "#e65100"
C_GRAY = "#9e9e9e"
C_DARK = "#1a2332"
C_BG = "#f8f9fa"

TODAY = datetime.now().strftime("%Y-%m-%d")
TODAY_BR = datetime.now().strftime("%d/%m/%Y")

# =============================================================================
# DATA LOADING
# =============================================================================

def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return None

def load_all_data(base_path):
    raw = base_path / "raw"
    proc = base_path / "processed"
    data = {}
    files = {
        "prices": raw / "price_history.json",
        "physical": proc / "physical.json",
        "physical_intl": proc / "physical_intl.json",
        "stocks": proc / "stocks_watch.json",
        "spreads": proc / "spreads.json",
        "cot": proc / "cot.json",
        "futures": proc / "futures_contracts.json",
        "reading": proc / "daily_reading.json",
        "report": proc / "report_daily.json",
        "bcb": proc / "bcb_data.json",
        "ibge": proc / "ibge_data.json",
        "eia": proc / "eia_data.json",
    }
    for k, p in files.items():
        data[k] = load_json(p)
        print(f"  {k}: {'OK' if data[k] else 'N/A'}")
    return data


# =============================================================================
# CHART GENERATORS
# =============================================================================

NAMES = {
    "ZC":"Milho","ZS":"Soja","ZW":"Trigo","KE":"Trigo KC","ZM":"Farelo",
    "ZL":"Óleo Soja","KC":"Café","CC":"Cacau","SB":"Açúcar","CT":"Algodão",
    "OJ":"Suco Laranja","LE":"Boi Gordo","GF":"Feeder","HE":"Suíno",
    "CL":"Petróleo","NG":"Gás Nat.","GC":"Ouro","SI":"Prata","DX":"Dólar Idx"
}

def fig_to_image(fig, width_mm=170, height_mm=80):
    """Convert matplotlib figure to ReportLab Image."""
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=C_BG)
    plt.close(fig)
    buf.seek(0)
    return Image(buf, width=width_mm*MM, height=height_mm*MM)


def chart_daily_changes(prices):
    """Gráfico de barras horizontais: variação diária das principais commodities."""
    fig, ax = plt.subplots(figsize=(8, 4.5), facecolor=C_BG)
    ax.set_facecolor(C_BG)

    tickers = ["ZC","ZS","ZW","KC","SB","CT","LE","HE","GF","CL","GC"]
    labels = []
    values = []

    for tk in tickers:
        candles = prices.get(tk, [])
        if candles and len(candles) >= 2:
            close = candles[-1].get("close", 0)
            prev = candles[-2].get("close", 0)
            if prev:
                chg = ((close / prev) - 1) * 100
                labels.append(NAMES.get(tk, tk))
                values.append(chg)

    if not values:
        plt.close(fig)
        return None

    # Ordenar por variação
    pairs = sorted(zip(labels, values), key=lambda x: x[1])
    labels, values = zip(*pairs)

    colors = [C_GREEN if v > 0 else C_RED for v in values]
    y_pos = range(len(labels))

    ax.barh(y_pos, values, color=colors, height=0.6, edgecolor="none")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=9, fontweight="bold")
    ax.set_xlabel("Variação (%)", fontsize=9)
    ax.axvline(x=0, color=C_DARK, linewidth=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    # Valores nas barras
    for i, (v, c) in enumerate(zip(values, colors)):
        offset = 0.15 if v >= 0 else -0.15
        ha = "left" if v >= 0 else "right"
        ax.text(v + offset, i, f"{v:+.1f}%", va="center", ha=ha, fontsize=8, color=c, fontweight="bold")

    ax.set_title("Variação do Dia — Principais Commodities", fontsize=11, fontweight="bold", color=C_DARK, pad=10)
    fig.tight_layout()
    return fig_to_image(fig, 170, 90)


def chart_spreads_regime(spreads):
    """Gauge-style chart mostrando percentis dos spreads."""
    sp = spreads.get("spreads", {})
    if not sp:
        return None

    fig, axes = plt.subplots(2, 3, figsize=(9, 5), facecolor=C_BG)
    axes = axes.flatten()

    spread_items = list(sp.items())[:6]

    for idx, (name, s) in enumerate(spread_items):
        ax = axes[idx]
        ax.set_facecolor(C_BG)
        pct = s.get("percentile", 50)
        regime = s.get("regime", "NORMAL")

        # Barra de percentil
        bar_colors = [C_GREEN if pct < 30 else C_ORANGE if pct < 70 else C_RED]
        ax.barh(0, pct, height=0.4, color=bar_colors[0], alpha=0.8)
        ax.barh(0, 100, height=0.4, color=C_GRAY, alpha=0.15)
        ax.set_xlim(0, 100)
        ax.set_ylim(-0.5, 0.5)
        ax.set_yticks([])

        # Labels
        short_name = s.get("name", name)[:20]
        ax.set_title(short_name, fontsize=8, fontweight="bold", color=C_DARK, pad=3)
        ax.text(pct, 0, f" {pct:.0f}%", va="center", fontsize=9, fontweight="bold",
                color=bar_colors[0])

        # Regime badge
        regime_color = {"NORMAL": C_GRAY, "COMPRESSÃO": C_RED, "DISSONÂNCIA": C_ORANGE}.get(regime, C_GRAY)
        ax.text(50, -0.35, regime, ha="center", fontsize=7, color=regime_color, fontstyle="italic")

        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_visible(False)
        ax.tick_params(axis="x", labelsize=7)

    # Remover axes extras
    for idx in range(len(spread_items), 6):
        axes[idx].set_visible(False)

    fig.suptitle("Spreads — Percentil Histórico e Regime", fontsize=11, fontweight="bold", color=C_DARK)
    fig.tight_layout()
    return fig_to_image(fig, 170, 85)


def chart_cot_positioning(cot):
    """Gráfico de posicionamento COT: Managed Money net long/short."""
    cms = cot.get("commodities", {})
    if not cms:
        return None

    fig, ax = plt.subplots(figsize=(8, 4.5), facecolor=C_BG)
    ax.set_facecolor(C_BG)

    tickers = ["ZC","ZS","ZW","KC","SB","CT","LE","HE","CL","GC"]
    labels = []
    mm_nets = []

    for tk in tickers:
        d = cms.get(tk, {})
        dis = d.get("disaggregated", {}).get("latest", {})
        if dis:
            mm = dis.get("managed_money_net")
            if mm is not None:
                labels.append(NAMES.get(tk, tk))
                mm_nets.append(mm)

    if not labels:
        plt.close(fig)
        return None

    colors = [C_GREEN if v > 0 else C_RED for v in mm_nets]
    x_pos = range(len(labels))

    ax.bar(x_pos, [v/1000 for v in mm_nets], color=colors, width=0.6, edgecolor="none")
    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, fontsize=8, fontweight="bold", rotation=45, ha="right")
    ax.set_ylabel("Managed Money Net (mil contratos)", fontsize=8)
    ax.axhline(y=0, color=C_DARK, linewidth=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    ax.set_title("COT — Posição dos Fundos (Managed Money)", fontsize=11, fontweight="bold", color=C_DARK, pad=10)
    fig.tight_layout()
    return fig_to_image(fig, 170, 85)


def chart_brasil_macro(bcb):
    """Mini dashboard Brasil: BRL e Selic timeline."""
    if not bcb:
        return None

    brl_data = bcb.get("brl_usd", [])
    selic_data = bcb.get("selic_meta", [])

    if not brl_data or len(brl_data) < 20:
        return None

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 3.5), facecolor=C_BG)

    # BRL/USD últimos 60 dias
    recent = brl_data[-60:]
    dates = list(range(len(recent)))
    vals = [float(r.get("value", r.get("valor", 0))) for r in recent if isinstance(r, dict)]
    if vals:
        ax1.set_facecolor(C_BG)
        ax1.plot(range(len(vals)), vals, color=C_BLUE, linewidth=2)
        vmin, vmax = min(vals), max(vals)
        pad = max((vmax - vmin) * 0.3, 0.05)
        ax1.set_ylim(vmin - pad, vmax + pad)
        ax1.fill_between(range(len(vals)), vals, vmin - pad, alpha=0.1, color=C_BLUE)
        ax1.set_title("BRL/USD — 60 dias", fontsize=9, fontweight="bold", color=C_DARK)
        ax1.set_ylabel("R$/US$", fontsize=8)
        last_val = vals[-1]
        ax1.axhline(y=last_val, color=C_ORANGE, linewidth=0.8, linestyle="--", alpha=0.6)
        ax1.text(len(vals)-1, last_val, f" R$ {last_val:.2f}", fontsize=8, color=C_ORANGE, fontweight="bold")
        ax1.spines["top"].set_visible(False)
        ax1.spines["right"].set_visible(False)
        ax1.set_xticks([])

    # Selic Meta
    recent_selic = selic_data[-24:] if len(selic_data) > 24 else selic_data
    svals = [float(r.get("value", r.get("valor", 0))) for r in recent_selic if isinstance(r, dict)]
    if svals:
        ax2.set_facecolor(C_BG)
        ax2.step(range(len(svals)), svals, color=C_RED, linewidth=2, where="post")
        svmin, svmax = min(svals), max(svals)
        spad = max((svmax - svmin) * 0.3, 0.5)
        ax2.set_ylim(svmin - spad, svmax + spad)
        ax2.fill_between(range(len(svals)), svals, svmin - spad, alpha=0.1, color=C_RED, step="post")
        ax2.set_title("Selic Meta — Últimas reuniões", fontsize=9, fontweight="bold", color=C_DARK)
        ax2.set_ylabel("% a.a.", fontsize=8)
        last_selic = svals[-1]
        ax2.text(len(svals)-1, last_selic, f" {last_selic:.1f}%", fontsize=9, color=C_RED, fontweight="bold")
        ax2.spines["top"].set_visible(False)
        ax2.spines["right"].set_visible(False)
        ax2.set_xticks([])

    fig.suptitle("Brasil — Câmbio e Política Monetária", fontsize=11, fontweight="bold", color=C_DARK, y=1.02)
    fig.tight_layout()
    return fig_to_image(fig, 170, 70)


def chart_energy(eia):
    """Gráficos de energia: WTI + Diesel + Etanol."""
    series = eia.get("series", {})
    if not series:
        return None

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(9, 3.5), facecolor=C_BG)

    def plot_series(ax, sid, title, prefix="$", color=C_BLUE):
        s = series.get(sid, {})
        hist = s.get("history", [])
        if not hist:
            ax.set_visible(False)
            return
        # Deduplicate by period
        seen = {}
        for h in hist:
            p = h.get("period", "")
            v = h.get("value")
            if p and v is not None and p not in seen:
                seen[p] = float(v)
        sorted_data = sorted(seen.items())
        if not sorted_data:
            ax.set_visible(False)
            return
        vals = [v for _, v in sorted_data]
        ax.set_facecolor(C_BG)
        ax.plot(range(len(vals)), vals, color=color, linewidth=2, marker="o", markersize=3)
        vmin, vmax = min(vals), max(vals)
        vpad = max((vmax - vmin) * 0.3, vmax * 0.02)
        ax.set_ylim(vmin - vpad, vmax + vpad)
        ax.fill_between(range(len(vals)), vals, vmin - vpad, alpha=0.1, color=color)
        ax.set_title(title, fontsize=8, fontweight="bold", color=C_DARK)
        last = vals[-1]
        ax.text(len(vals)-1, last, f" {prefix}{last:.2f}", fontsize=8, color=color, fontweight="bold")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.set_xticks([])

    plot_series(ax1, "wti_spot", "WTI ($/bbl)", "$", C_DARK)
    plot_series(ax2, "diesel_retail", "Diesel ($/gal)", "$", C_RED)
    plot_series(ax3, "ethanol_production", "Etanol Prod (MBbl/d)", "", C_GREEN)

    fig.suptitle("Energia — Impacto nos Custos Agrícolas", fontsize=11, fontweight="bold", color=C_DARK, y=1.02)
    fig.tight_layout()
    return fig_to_image(fig, 170, 70)


def chart_futures_curve(futures):
    """Curva de futuros das principais commodities."""
    ft = futures.get("commodities", {})
    if not ft:
        return None

    key_tickers = ["ZC", "ZS", "KC", "LE"]
    fig, axes = plt.subplots(1, 4, figsize=(9, 3.5), facecolor=C_BG)

    colors_map = {"ZC": C_GREEN, "ZS": "#1b5e20", "KC": "#4e342e", "LE": C_RED}

    for idx, tk in enumerate(key_tickers):
        ax = axes[idx]
        ax.set_facecolor(C_BG)
        d = ft.get(tk, {})
        contracts = d.get("contracts", [])[:5]
        if not contracts:
            ax.set_visible(False)
            continue

        labels = [c.get("contract", "")[-3:] for c in contracts]
        prices = [c.get("close", 0) for c in contracts]
        color = colors_map.get(tk, C_BLUE)

        ax.plot(range(len(prices)), prices, color=color, linewidth=2, marker="o", markersize=5)
        ax.fill_between(range(len(prices)), prices, alpha=0.1, color=color)
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, fontsize=7)
        ax.set_title(f"{NAMES.get(tk, tk)}", fontsize=9, fontweight="bold", color=C_DARK)

        # Contango/Backwardation
        if len(prices) >= 2:
            structure = "CONTANGO ↗" if prices[-1] > prices[0] else "BACKW ↘"
            s_color = C_ORANGE if prices[-1] > prices[0] else C_GREEN
            ax.text(0.5, 0.02, structure, transform=ax.transAxes, ha="center",
                    fontsize=7, color=s_color, fontstyle="italic")

        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    fig.suptitle("Curva de Futuros — Estrutura do Mercado", fontsize=11, fontweight="bold", color=C_DARK, y=1.02)
    fig.tight_layout()
    return fig_to_image(fig, 170, 70)


# =============================================================================
# PDF VISUAL REPORT
# =============================================================================

def build_visual_pdf(data, output_path):
    """Gera PDF visual com gráficos e análises editoriais."""
    print("\n📄 Gerando PDF visual...")

    W, H = A4
    doc = SimpleDocTemplate(
        str(output_path), pagesize=A4,
        topMargin=18*MM, bottomMargin=15*MM, leftMargin=14*MM, rightMargin=14*MM
    )

    base = getSampleStyleSheet()
    s = {
        "title": ParagraphStyle("VT", parent=base["Title"], fontSize=28, textColor=BLUE,
                                fontName="Helvetica-Bold", alignment=TA_CENTER, spaceAfter=4),
        "subtitle": ParagraphStyle("VS", parent=base["Normal"], fontSize=14, textColor=GRAY,
                                   alignment=TA_CENTER, spaceAfter=8),
        "sec": ParagraphStyle("SEC", parent=base["Heading1"], fontSize=14, textColor=WHITE,
                              backColor=DARK, borderPadding=(6, 10, 6, 10), fontName="Helvetica-Bold",
                              spaceBefore=14, spaceAfter=8),
        "sub": ParagraphStyle("SUB", parent=base["Heading2"], fontSize=11, textColor=BLUE,
                              fontName="Helvetica-Bold", spaceBefore=10, spaceAfter=4),
        "body": ParagraphStyle("BD", parent=base["Normal"], fontSize=9.5, textColor=TXT,
                               leading=13, alignment=TA_JUSTIFY),
        "callout": ParagraphStyle("CO", parent=base["Normal"], fontSize=9, textColor=BLUE,
                                  leftIndent=8*MM, rightIndent=8*MM, leading=12,
                                  backColor=HexColor("#e3f2fd"), borderPadding=(6, 8, 6, 8)),
        "insight": ParagraphStyle("IN", parent=base["Normal"], fontSize=9, textColor=GREEN,
                                  fontName="Helvetica-BoldOblique", leftIndent=4*MM),
        "small": ParagraphStyle("SM", parent=base["Normal"], fontSize=7, textColor=GRAY, leading=9),
        "center": ParagraphStyle("CE", parent=base["Normal"], fontSize=10, textColor=GRAY,
                                 alignment=TA_CENTER, spaceAfter=8),
    }

    story = []
    rpt = data.get("report", {}) or {}

    # -------------------------------------------------------------------------
    # CAPA
    # -------------------------------------------------------------------------
    story.append(Spacer(1, 30*MM))
    story.append(Paragraph("AGRIMACRO", s["title"]))
    story.append(Paragraph(f"Análise Visual — {TODAY_BR}", s["center"]))
    story.append(Spacer(1, 4*MM))
    story.append(HRFlowable(width="40%", thickness=2, color=BLUE, spaceAfter=6*MM, hAlign="CENTER"))

    titulo = rpt.get("titulo", "Relatório do Dia")
    subtitulo = rpt.get("subtitulo", "")
    resumo = rpt.get("resumo_executivo", "")

    story.append(Paragraph(titulo, ParagraphStyle("TT", parent=s["title"], fontSize=18, textColor=TXT)))
    if subtitulo:
        story.append(Paragraph(subtitulo, ParagraphStyle("ST", parent=s["subtitle"], fontSize=12, textColor=BLUE)))
    story.append(Spacer(1, 6*MM))

    if resumo:
        story.append(Paragraph(resumo, s["body"]))
        story.append(Spacer(1, 4*MM))

    score = rpt.get("score_volatilidade", "")
    riscos = rpt.get("principais_riscos", [])
    if score:
        risk_txt = f"<b>Volatilidade: {score}/10</b>"
        if riscos:
            risk_txt += f"&nbsp;&nbsp;|&nbsp;&nbsp;Riscos: {'; '.join(riscos[:3])}"
        story.append(Paragraph(risk_txt, s["callout"]))

    story.append(Spacer(1, 8*MM))
    story.append(Paragraph("Público: produtores rurais, profissionais do agro e traders", s["small"]))
    story.append(Paragraph("Fontes: Yahoo Finance, IBKR, USDA, CEPEA, MAGyP, CFTC, BCB, IBGE, EIA", s["small"]))
    story.append(PageBreak())

    # -------------------------------------------------------------------------
    # VISÃO DO MERCADO — Gráfico de variações
    # -------------------------------------------------------------------------
    story.append(Paragraph("O QUE MEXEU HOJE", s["sec"]))
    story.append(Spacer(1, 2*MM))

    if data.get("prices"):
        chart = chart_daily_changes(data["prices"])
        if chart:
            story.append(chart)
            story.append(Spacer(1, 4*MM))

    # Texto editorial sobre movimentos
    destaques = rpt.get("destaques", [])
    if destaques:
        for d in destaques[:3]:
            titulo_d = d.get("titulo", "")
            corpo = d.get("corpo", "").replace("\n", "<br/>")
            impacto = d.get("impacto_produtor", "")
            story.append(Paragraph(f'<b>{titulo_d}</b>', s["sub"]))
            story.append(Paragraph(corpo, s["body"]))
            if impacto:
                story.append(Spacer(1, 2*MM))
                story.append(Paragraph(f'<b>→ Para o produtor:</b> {impacto}', s["insight"]))
            story.append(Spacer(1, 4*MM))

    story.append(PageBreak())

    # -------------------------------------------------------------------------
    # ESTRUTURA DO MERCADO — Spreads + Futuros
    # -------------------------------------------------------------------------
    story.append(Paragraph("ESTRUTURA DO MERCADO", s["sec"]))
    story.append(Spacer(1, 2*MM))
    story.append(Paragraph(
        "Os spreads entre mercados revelam as tensões estruturais que movem os preços. "
        "Percentis abaixo de 20% ou acima de 80% indicam oportunidades de atenção.",
        s["body"]))
    story.append(Spacer(1, 4*MM))

    if data.get("spreads"):
        chart = chart_spreads_regime(data["spreads"])
        if chart:
            story.append(chart)
            story.append(Spacer(1, 4*MM))

        # Insight editorial
        sp = data["spreads"].get("spreads", {})
        alerts = []
        for name, ss in sp.items():
            pct = ss.get("percentile", 50)
            regime = ss.get("regime", "")
            if pct and (pct < 20 or pct > 80):
                alerts.append(f'{ss.get("name", name)}: percentil {pct:.0f}% — {regime}')
        if alerts:
            story.append(Paragraph("<b>⚠ Alertas de spread:</b>", s["sub"]))
            for a in alerts:
                story.append(Paragraph(f"• {a}", s["body"]))
            story.append(Spacer(1, 3*MM))

    story.append(Spacer(1, 4*MM))
    story.append(Paragraph("CURVA DE FUTUROS", s["sub"]))
    story.append(Paragraph(
        "A inclinação da curva (contango vs backwardation) sinaliza expectativas de oferta e demanda futura. "
        "Backwardation sugere aperto de curto prazo; contango sugere conforto de estoques.",
        s["body"]))
    story.append(Spacer(1, 3*MM))

    if data.get("futures"):
        chart = chart_futures_curve(data["futures"])
        if chart:
            story.append(chart)

    story.append(PageBreak())

    # -------------------------------------------------------------------------
    # POSICIONAMENTO DOS FUNDOS — COT
    # -------------------------------------------------------------------------
    story.append(Paragraph("ONDE ESTÃO OS FUNDOS", s["sec"]))
    story.append(Spacer(1, 2*MM))
    story.append(Paragraph(
        "O Commitment of Traders (CFTC) revela o posicionamento dos grandes players. "
        "Quando Managed Money está muito comprado, há risco de liquidação. "
        "Quando muito vendido, a commodity pode estar perto de um fundo.",
        s["body"]))
    story.append(Spacer(1, 4*MM))

    if data.get("cot"):
        chart = chart_cot_positioning(data["cot"])
        if chart:
            story.append(chart)
            story.append(Spacer(1, 4*MM))

        # Insights COT
        cms = data["cot"].get("commodities", {})
        extremes = []
        for tk in ["ZC","ZS","ZW","KC","SB","LE","CL"]:
            d = cms.get(tk, {})
            dis = d.get("disaggregated", {}).get("latest", {})
            mm = dis.get("managed_money_net")
            if mm is not None:
                if mm > 80000:
                    extremes.append(f'{NAMES.get(tk,tk)}: fundos muito comprados ({mm:,.0f} contratos) — risco de correção')
                elif mm < -80000:
                    extremes.append(f'{NAMES.get(tk,tk)}: fundos muito vendidos ({mm:,.0f} contratos) — possível fundo de preço')

        if extremes:
            story.append(Paragraph("<b>Posições extremas:</b>", s["sub"]))
            for e in extremes:
                story.append(Paragraph(f"• {e}", s["body"]))

    story.append(PageBreak())

    # -------------------------------------------------------------------------
    # BRASIL & ENERGIA
    # -------------------------------------------------------------------------
    story.append(Paragraph("CENÁRIO BRASIL + ENERGIA", s["sec"]))
    story.append(Spacer(1, 2*MM))

    if data.get("bcb"):
        chart = chart_brasil_macro(data["bcb"])
        if chart:
            story.append(chart)
            story.append(Spacer(1, 3*MM))

        # Texto
        brl = data["bcb"].get("brl_usd", [])
        if brl:
            last_brl = brl[-1].get("value", 0) if isinstance(brl[-1], dict) else 0
            story.append(Paragraph(
                f'<b>Câmbio:</b> Real em R$ {float(last_brl):.2f}/USD. '
                'Dólar mais fraco favorece exportações agrícolas brasileiras, '
                'melhorando receita em reais para soja, café e carnes.',
                s["body"]))
            story.append(Spacer(1, 3*MM))

        selic = data["bcb"].get("selic_meta", [])
        if selic:
            last_selic = selic[-1].get("value", 0) if isinstance(selic[-1], dict) else 0
            story.append(Paragraph(
                f'<b>Juros:</b> Selic em {float(last_selic):.1f}% a.a. — custo de carregamento elevado para estoques. '
                'Crédito rural mais caro pressiona produtores a vender safra mais rápido.',
                s["body"]))
            story.append(Spacer(1, 4*MM))

    if data.get("eia"):
        chart = chart_energy(data["eia"])
        if chart:
            story.append(chart)
            story.append(Spacer(1, 3*MM))

        wti = data["eia"].get("series", {}).get("wti_spot", {})
        diesel = data["eia"].get("series", {}).get("diesel_retail", {})
        if wti.get("latest_value") and diesel.get("latest_value"):
            story.append(Paragraph(
                f'<b>Energia:</b> Petróleo WTI em ${wti["latest_value"]:.2f}/barril, '
                f'diesel a ${diesel["latest_value"]:.3f}/galão. '
                'Custos de produção agrícola (diesel, fertilizantes) permanecem controlados. '
                'Etanol forte sustenta demanda por milho nos EUA.',
                s["body"]))

    story.append(PageBreak())

    # -------------------------------------------------------------------------
    # LEITURA & CALENDÁRIO
    # -------------------------------------------------------------------------
    story.append(Paragraph("AGENDA E LEITURA DO DIA", s["sec"]))
    story.append(Spacer(1, 2*MM))

    # Calendário
    cal = rpt.get("calendario_semana", [])
    if cal:
        story.append(Paragraph("Eventos da Semana", s["sub"]))
        for ev in cal:
            imp = ev.get("impacto", "").upper()
            emoji = "🔴" if imp == "ALTO" else "🟡" if imp == "MEDIO" else "⚪"
            story.append(Paragraph(
                f'<b>{ev.get("data","")}</b> — {ev.get("evento","")} ({imp}) — {ev.get("relevancia","")}',
                s["body"]))
        story.append(Spacer(1, 4*MM))

    # Pergunta do dia
    pergunta = rpt.get("pergunta_do_dia", "")
    if pergunta:
        story.append(Paragraph(f'<b>Pergunta do dia:</b> {pergunta}', s["callout"]))
        story.append(Spacer(1, 3*MM))

    # Frase
    frase = rpt.get("frase_do_dia", "")
    if frase:
        story.append(Paragraph(f'<i>"{frase}"</i>', ParagraphStyle("FR", parent=s["body"],
                     fontSize=9, textColor=GRAY, alignment=TA_CENTER)))

    # -------------------------------------------------------------------------
    # DISCLAIMER
    # -------------------------------------------------------------------------
    story.append(Spacer(1, 10*MM))
    story.append(HRFlowable(width="100%", thickness=1, color=HexColor("#dee2e6"), spaceAfter=3*MM))
    story.append(Paragraph(
        f"AgriMacro v3.2 — Relatório visual gerado automaticamente em {TODAY_BR}. "
        "Dados públicos. Não constitui recomendação de investimento.",
        s["small"]))

    # Header/Footer
    def hf(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica-Bold", 6.5)
        canvas.setFillColor(GRAY)
        canvas.drawString(14*MM, H-10*MM, f"AGRIMACRO — Análise Visual {TODAY_BR}")
        canvas.drawRightString(W-14*MM, H-10*MM, f"Pág {doc.page}")
        canvas.setStrokeColor(BLUE)
        canvas.setLineWidth(1)
        canvas.line(14*MM, H-12*MM, W-14*MM, H-12*MM)
        canvas.restoreState()

    print("  Construindo PDF...")
    doc.build(story, onFirstPage=hf, onLaterPages=hf)
    size = output_path.stat().st_size / 1024
    print(f"  ✅ {output_path} ({size:.0f} KB)")


# =============================================================================
# VIDEO SCRIPT FOR HEYGEN
# =============================================================================

def build_video_script(data, output_path):
    """Gera script de vídeo estilo telejornal agro para HeyGen."""
    print("\n🎬 Gerando script de vídeo...")

    rpt = data.get("report", {}) or {}
    prices = data.get("prices", {}) or {}
    cot_data = data.get("cot", {}) or {}
    bcb = data.get("bcb", {}) or {}
    eia = data.get("eia", {}) or {}
    spreads = data.get("spreads", {}) or {}

    # Extrair dados chave
    titulo = rpt.get("titulo", "Relatório do Dia")
    subtitulo = rpt.get("subtitulo", "")
    resumo = rpt.get("resumo_executivo", "")
    destaques = rpt.get("destaques", [])
    score = rpt.get("score_volatilidade", "")
    riscos = rpt.get("principais_riscos", [])
    cal = rpt.get("calendario_semana", [])
    pergunta = rpt.get("pergunta_do_dia", "")

    # Preços chave
    def get_price_change(tk):
        candles = prices.get(tk, [])
        if candles and len(candles) >= 2:
            c = candles[-1].get("close", 0)
            p = candles[-2].get("close", 0)
            if p:
                return c, ((c/p)-1)*100
        return None, None

    soja_price, soja_chg = get_price_change("ZS")
    milho_price, milho_chg = get_price_change("ZC")
    cafe_price, cafe_chg = get_price_change("KC")
    boi_price, boi_chg = get_price_change("LE")
    wti_val = eia.get("series",{}).get("wti_spot",{}).get("latest_value")
    diesel_val = eia.get("series",{}).get("diesel_retail",{}).get("latest_value")

    brl_data = bcb.get("brl_usd", [])
    brl_val = float(brl_data[-1].get("value", 0)) if brl_data and isinstance(brl_data[-1], dict) else None

    # Construir script
    script = []
    dur_total = 0

    def add_scene(titulo_scene, texto, duracao, notas=""):
        nonlocal dur_total
        dur_total += duracao
        script.append({
            "cena": len(script) + 1,
            "titulo": titulo_scene,
            "duracao": duracao,
            "fala": texto,
            "notas_producao": notas,
        })

    # CENA 1: ABERTURA (15s)
    add_scene("ABERTURA", 
        f"Bom dia, produtor! Aqui é o AgriMacro com o resumo do mercado de commodities "
        f"desta {datetime.now().strftime('%A').replace('Monday','segunda').replace('Tuesday','terça').replace('Wednesday','quarta').replace('Thursday','quinta').replace('Friday','sexta').replace('Saturday','sábado').replace('Sunday','domingo')}, "
        f"{TODAY_BR}. {subtitulo if subtitulo else titulo}. Vamos aos destaques.",
        15,
        "Avatar apresentador profissional. Background: estúdio com telas de mercado. "
        "Lower third: 'AGRIMACRO — Análise Diária de Commodities'"
    )

    # CENA 2: MANCHETE PRINCIPAL (25s)
    if destaques:
        d1 = destaques[0]
        # Reescrever para linguagem oral fluida, sem truncar
        impacto = d1.get("impacto_produtor", "")
        titulo_d = d1.get("titulo", "")
        corpo = d1.get("corpo", "")
        # Extrair essência em 2-3 frases faladas
        # Pegar primeiro parágrafo completo
        primeiro_para = corpo.split("\n")[0] if "\n" in corpo else corpo
        # Limitar a ~200 chars mas sem cortar no meio da frase
        if len(primeiro_para) > 250:
            corte = primeiro_para[:250].rfind(". ")
            if corte > 100:
                primeiro_para = primeiro_para[:corte+1]
            else:
                primeiro_para = primeiro_para[:250].rsplit(" ", 1)[0] + "."
        fala_manchete = f"{titulo_d}. {primeiro_para}"
        if impacto:
            fala_manchete += f" E o que isso significa pra você, produtor? {impacto}"
        add_scene("MANCHETE DO DIA",
            fala_manchete,
            25,
            f"Gráfico fullscreen mostrando o movimento do {d1.get('commodity','')}. "
            "Seta indicando direção. Lower third com ticker e variação."
        )

    # CENA 3: QUADRO DE PREÇOS (20s)
    # Formatação oral: números falados naturalmente
    def oral_price(tk, price, chg):
        """Formata preço para fala natural."""
        name = NAMES.get(tk, tk)
        direcao = "subindo" if chg > 0 else "caindo"
        pct = f"{abs(chg):.1f} por cento"
        # Formatação oral por commodity
        if tk == "ZS":
            return f"Soja a {price:.0f} centavos o bushel, {direcao} {pct}"
        elif tk == "ZC":
            return f"Milho a {price:.0f} centavos, {direcao} {pct}"
        elif tk == "KC":
            return f"Café arábica a {price:.0f} centavos a libra, {direcao} {pct}"
        elif tk == "LE":
            return f"Boi gordo a {price:.0f} centavos a libra, {direcao} {pct}"
        return f"{name} {direcao} {pct}"

    precos_partes = []
    if soja_price and soja_chg is not None:
        precos_partes.append(oral_price("ZS", soja_price, soja_chg))
    if milho_price and milho_chg is not None:
        precos_partes.append(oral_price("ZC", milho_price, milho_chg))
    if cafe_price and cafe_chg is not None:
        precos_partes.append(oral_price("KC", cafe_price, cafe_chg))
    if boi_price and boi_chg is not None:
        precos_partes.append(oral_price("LE", boi_price, boi_chg))

    if precos_partes:
        precos_txt = ". ".join(precos_partes) + "."
        add_scene("QUADRO DE PREÇOS",
            f"Vamos ao painel de preços. {precos_txt}",
            20,
            "Tela dividida com barras de variação coloridas (verde/vermelho). "
            "Cada commodity aparece com seta e percentual."
        )

    # CENA 4: FUNDOS E POSICIONAMENTO (20s)
    cot_txt = ""
    cms = cot_data.get("commodities", {})
    for tk, name in [("ZS","soja"),("ZC","milho"),("KC","café")]:
        d = cms.get(tk, {})
        mm = d.get("disaggregated",{}).get("latest",{}).get("managed_money_net")
        if mm is not None:
            pos = "comprados" if mm > 0 else "vendidos"
            cot_txt += f"Em {name}, os fundos estão {pos} em {abs(mm):,.0f} contratos. "

    if cot_txt:
        add_scene("POSIÇÃO DOS FUNDOS",
            f"No Commitment of Traders desta semana: {cot_txt} "
            "Fique atento: posições extremas dos fundos costumam antecipar reversões.",
            20,
            "Gráfico de barras mostrando posição Managed Money. "
            "Destaque visual para posições extremas."
        )

    # CENA 5: DÓLAR E ENERGIA (15s)
    macro_partes = []
    if brl_val:
        macro_partes.append(f"O dólar comercial fecha a semana em {brl_val:.2f} reais")
    if wti_val:
        macro_partes.append(f"petróleo WTI cotado a {wti_val:.0f} dólares o barril")
    if diesel_val:
        macro_partes.append(f"diesel nos Estados Unidos a {diesel_val:.2f} dólares o galão, "
            "impactando diretamente no custo de frete e produção agrícola")

    if macro_partes:
        macro_txt = ". ".join(macro_partes) + "."
        macro_txt = macro_txt[0].upper() + macro_txt[1:]
        add_scene("DÓLAR E ENERGIA",
            macro_txt,
            15,
            "Split screen: lado esquerdo gráfico BRL/USD, lado direito gráfico WTI. "
            "Lower third: cotações em tempo real."
        )

    # CENA 6: DESTAQUE 2 (se houver) (20s)
    if len(destaques) > 1:
        d2 = destaques[1]
        titulo_d2 = d2.get("titulo", "")
        corpo_d2 = d2.get("corpo", "")
        impacto_d2 = d2.get("impacto_produtor", "")
        # Pegar primeiro parágrafo sem truncar
        primeiro_p2 = corpo_d2.split("\n")[0] if "\n" in corpo_d2 else corpo_d2
        if len(primeiro_p2) > 220:
            corte2 = primeiro_p2[:220].rfind(". ")
            if corte2 > 80:
                primeiro_p2 = primeiro_p2[:corte2+1]
            else:
                primeiro_p2 = primeiro_p2[:220].rsplit(" ", 1)[0] + "."
        fala_d2 = f"{titulo_d2}. {primeiro_p2}"
        if impacto_d2:
            fala_d2 += f" Impacto para você: {impacto_d2}"
        add_scene("SEGUNDO DESTAQUE",
            fala_d2,
            20,
            f"Visual: gráfico ou imagem relacionada a {d2.get('commodity','')}."
        )

    # CENA 7: AGENDA DA SEMANA (15s)
    if cal:
        cal_txt = "Fique de olho na agenda: "
        for ev in cal[:3]:
            cal_txt += f'{ev.get("data","")}, {ev.get("evento","")}. '
        add_scene("AGENDA DA SEMANA",
            cal_txt,
            15,
            "Calendário visual com ícones por evento. Destaques em vermelho para alto impacto."
        )

    # CENA 8: ENCERRAMENTO (15s)
    add_scene("ENCERRAMENTO",
        f"Esse foi o resumo AgriMacro de hoje. "
        f'{"A pergunta que fica: " + pergunta + " " if pergunta else ""}'
        "Se essa análise foi útil, deixe o like e se inscreva no canal. "
        "Amanhã tem mais. Bons negócios e até a próxima!",
        15,
        "Avatar despede-se. Logo AgriMacro. Call to action: inscrever-se. "
        "Tela final com QR code ou link."
    )

    # Escrever script
    lines = []
    lines.append(f"# AGRIMACRO — Script de Vídeo")
    lines.append(f"**Data:** {TODAY_BR}")
    lines.append(f"**Duração estimada:** {dur_total // 60}min {dur_total % 60}s")
    lines.append(f"**Formato:** Telejornal agro / YouTube")
    lines.append(f"**Plataforma:** HeyGen AI Video")
    lines.append(f"**Público:** Produtores rurais, profissionais do agro, traders")
    lines.append(f"")
    lines.append(f"---")
    lines.append(f"")

    for cena in script:
        lines.append(f"## Cena {cena['cena']}: {cena['titulo']} ({cena['duracao']}s)")
        lines.append(f"")
        lines.append(f"**FALA:**")
        lines.append(f"> {cena['fala']}")
        lines.append(f"")
        lines.append(f"**PRODUÇÃO:**")
        lines.append(f"_{cena['notas_producao']}_")
        lines.append(f"")
        lines.append(f"---")
        lines.append(f"")

    lines.append(f"## Notas de Produção")
    lines.append(f"")
    lines.append(f"- **Tom:** Profissional mas acessível. Não é Bloomberg, é o jornal do produtor.")
    lines.append(f"- **Ritmo:** Dinâmico, sem pausas longas. Cada cena tem corte visual.")
    lines.append(f"- **Gráficos:** Usar os mesmos do relatório visual AgriMacro.")
    lines.append(f"- **Lower thirds:** Sempre mostrar ticker + preço + variação %.")
    lines.append(f"- **Música:** Trilha suave de fundo, estilo noticiário. Volume baixo.")
    lines.append(f"- **Avatar HeyGen:** Apresentador masculino ou feminino, roupa formal casual.")
    lines.append(f"- **Thumbnail YouTube:** Título '{titulo}' + emoji de seta + commodity principal.")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"  ✅ {output_path}")
    print(f"  ⏱ Duração: {dur_total // 60}min {dur_total % 60}s ({len(script)} cenas)")


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("  AGRIMACRO v3.2 — Content Generator")
    print(f"  {TODAY_BR}")
    print("=" * 60)

    # Data path
    possible = [
        Path(os.path.expanduser("~")) / "OneDrive" / "READET~1" / "agrimacro" / "agrimacro-dash" / "public" / "data",
        Path(os.path.expanduser("~")) / "OneDrive" / "Área de Trabalho" / "agrimacro" / "agrimacro-dash" / "public" / "data",
        Path("/mnt/user-data/uploads"),  # Fallback for Claude environment
    ]
    data_path = None
    for p in possible:
        if (p / "processed").exists() or p.exists():
            data_path = p
            break
    if not data_path:
        print("❌ Data path não encontrado")
        return

    print(f"\n📁 Dados: {data_path}")
    data = load_all_data(data_path)

    # Output path
    reports_possible = [
        Path(os.path.expanduser("~")) / "OneDrive" / "READET~1" / "agrimacro" / "reports",
        Path(os.path.expanduser("~")) / "OneDrive" / "Área de Trabalho" / "agrimacro" / "reports",
        Path("/mnt/user-data/outputs"),
    ]
    reports_dir = Path(".")
    for p in reports_possible:
        if p.exists():
            reports_dir = p
            break
    reports_dir.mkdir(exist_ok=True)

    # 1. PDF Visual
    pdf_path = reports_dir / f"AgriMacro_Visual_{TODAY}.pdf"
    build_visual_pdf(data, pdf_path)

    # 2. Video Script
    script_path = reports_dir / f"AgriMacro_VideoScript_{TODAY}.md"
    build_video_script(data, script_path)

    print(f"\n{'='*60}")
    print(f"✅ Conteúdo gerado:")
    print(f"   📄 {pdf_path.name} — Relatório visual com gráficos")
    print(f"   🎬 {script_path.name} — Script HeyGen / YouTube")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
