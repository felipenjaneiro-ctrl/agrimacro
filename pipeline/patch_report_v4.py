#!/usr/bin/env python3
"""
AgriMacro v4 PDF Patch — adds pg_options_intelligence + pg_track_record
to the existing generate_report_pdf.py build pipeline.

Run standalone:  python pipeline/patch_report_v4.py
Or from pipeline: imported as step in run_pipeline.py
"""

import json
import os
import sys
import math
from datetime import datetime
from pathlib import Path

# Ensure pipeline dir is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import generate_report_pdf as base_pdf

# Re-use base constants
from generate_report_pdf import (
    BG, PANEL, PANEL2, BORDER, TEXT, TEXT2, TEXT_MUT, TEXT_DIM,
    GREEN, AMBER, RED, BLUE, PURPLE, CYAN, TEAL,
    PAGE_W, PAGE_H, M, DATA_PROC, REPORT_DIR, TODAY_STR,
    sload, ftr, tblock, load_json,
)
from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import A4, landscape


# ═══════════════════════════════════════════════════════════
#  pg_options_intelligence — IV Rank, Skew, Term Structure
# ═══════════════════════════════════════════════════════════

def pg_options_intelligence(pdf, chain_data):
    """Full-page options intelligence: IV Rank, Skew, Term Structure per underlying."""

    # ── Background ──
    pdf.setFillColor(HexColor(BG))
    pdf.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)

    # ── Title bar ──
    pdf.setFillColor(HexColor(PANEL))
    pdf.rect(0, PAGE_H - 52, PAGE_W, 52, fill=1, stroke=0)
    pdf.setFillColor(HexColor(TEXT))
    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(M, PAGE_H - 36, "Options Intelligence")
    pdf.setFillColor(HexColor(TEXT_MUT))
    pdf.setFont("Helvetica", 9)
    pdf.drawString(M + 250, PAGE_H - 36, f"IV Rank | Skew | Term Structure | {TODAY_STR}")

    if not chain_data or not chain_data.get("underlyings"):
        pdf.setFillColor(HexColor(TEXT_MUT))
        pdf.setFont("Helvetica", 11)
        pdf.drawString(M, PAGE_H - 90, "Dados de options chain nao disponiveis. Execute: python pipeline/collect_options_chain.py")
        return

    underlyings = chain_data["underlyings"]
    syms = sorted(underlyings.keys())

    # ── Table header ──
    y = PAGE_H - 72
    cols = [
        (M,       60,  "SYM"),
        (M+62,    60,  "UND PRICE"),
        (M+124,   55,  "IV ATM"),
        (M+181,   65,  "IV RANK"),
        (M+248,   40,  "HIGH"),
        (M+290,   40,  "LOW"),
        (M+332,   55,  "SKEW %"),
        (M+389,   55,  "PUT 25d"),
        (M+446,   55,  "CALL 25d"),
        (M+503,   80,  "TERM STRUCT"),
        (M+585,   50,  "HIST"),
    ]

    pdf.setFillColor(HexColor(PANEL2))
    pdf.rect(M - 4, y - 4, PAGE_W - 2 * M + 8, 16, fill=1, stroke=0)
    pdf.setFont("Helvetica-Bold", 7)
    for x, w, label in cols:
        pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.drawString(x, y, label)

    y -= 18
    row_h = 17

    for sym in syms:
        u = underlyings[sym]
        ivr = u.get("iv_rank", {})
        sk = u.get("skew", {})
        ts = u.get("term_structure", {})
        und_price = u.get("und_price")

        if y < 30:
            break

        # Alternating row bg
        idx = syms.index(sym)
        if idx % 2 == 0:
            pdf.setFillColor(HexColor(PANEL))
            pdf.rect(M - 4, y - 4, PAGE_W - 2 * M + 8, row_h, fill=1, stroke=0)

        pdf.setFont("Helvetica-Bold", 8.5)

        # Symbol
        pdf.setFillColor(HexColor(AMBER))
        pdf.drawString(cols[0][0], y, sym)

        pdf.setFont("Helvetica", 8)

        # Und price
        pdf.setFillColor(HexColor(TEXT))
        pdf.drawString(cols[1][0], y, f"${und_price:.2f}" if und_price else "-")

        # IV ATM
        cur_iv = ivr.get("current_iv")
        if cur_iv is not None:
            iv_pct = cur_iv * 100
            clr = RED if iv_pct > 40 else AMBER if iv_pct > 25 else GREEN
            pdf.setFillColor(HexColor(clr))
            pdf.drawString(cols[2][0], y, f"{iv_pct:.1f}%")
        else:
            pdf.setFillColor(HexColor(TEXT_DIM))
            pdf.drawString(cols[2][0], y, "-")

        # IV Rank
        rank = ivr.get("rank_52w")
        if rank is not None:
            clr = RED if rank > 75 else AMBER if rank > 50 else GREEN
            pdf.setFillColor(HexColor(clr))
            pdf.setFont("Helvetica-Bold", 8)
            pdf.drawString(cols[3][0], y, f"{rank:.0f}%")
            pdf.setFont("Helvetica", 8)
        else:
            days = ivr.get("history_days", 0)
            pdf.setFillColor(HexColor(TEXT_DIM))
            pdf.drawString(cols[3][0], y, f"building")

        # High / Low
        hi = ivr.get("iv_high_52w")
        lo = ivr.get("iv_low_52w")
        pdf.setFillColor(HexColor(TEXT_MUT))
        pdf.drawString(cols[4][0], y, f"{hi*100:.1f}%" if hi else "-")
        pdf.drawString(cols[5][0], y, f"{lo*100:.1f}%" if lo else "-")

        # Skew %
        skew_pct = sk.get("skew_pct")
        if skew_pct is not None:
            # Positive skew = puts more expensive (protective demand)
            clr = RED if skew_pct > 10 else AMBER if skew_pct > 0 else GREEN
            pdf.setFillColor(HexColor(clr))
            pdf.drawString(cols[6][0], y, f"{skew_pct:+.1f}%")
        else:
            pdf.setFillColor(HexColor(TEXT_DIM))
            pdf.drawString(cols[6][0], y, "-")

        # Put 25d / Call 25d
        put_iv = sk.get("put_25d_iv")
        call_iv = sk.get("call_25d_iv")
        pdf.setFillColor(HexColor(TEXT_MUT))
        pdf.drawString(cols[7][0], y, f"{put_iv*100:.1f}%" if put_iv else "-")
        pdf.drawString(cols[8][0], y, f"{call_iv*100:.1f}%" if call_iv else "-")

        # Term Structure
        structure = ts.get("structure", "N/A")
        clr_map = {"BACKWARDATION": RED, "CONTANGO": GREEN, "FLAT": TEXT_MUT}
        pdf.setFillColor(HexColor(clr_map.get(structure, TEXT_DIM)))
        pdf.setFont("Helvetica-Bold", 7.5)
        pdf.drawString(cols[9][0], y, structure)
        # Add IV points
        pts = ts.get("points", [])
        if pts:
            pts_str = " | ".join(f"{p['dte']}d={p['iv']*100:.0f}%" for p in pts[:4])
            pdf.setFont("Helvetica", 5.5)
            pdf.setFillColor(HexColor(TEXT_DIM))
            pdf.drawString(cols[9][0], y - 7, pts_str)
        pdf.setFont("Helvetica", 8)

        # History days
        days = ivr.get("history_days", 0)
        pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.drawString(cols[10][0], y, f"{days}d")

        y -= row_h + (5 if pts else 0)

    # ── Legend ──
    y -= 10
    pdf.setFont("Helvetica", 7)
    pdf.setFillColor(HexColor(TEXT_DIM))
    legends = [
        "IV RANK: percentil da IV atual na janela de 52 semanas (acumula diariamente). >75% = IV alta historicamente, <25% = IV baixa.",
        "SKEW: IV put 25d - IV call 25d. Positivo = puts mais caros (demanda por protecao). Negativo = calls mais caros.",
        "TERM: Contango = IV sobe com prazo (mercado calmo). Backwardation = IV cai com prazo (stress no curto prazo).",
    ]
    for leg in legends:
        if y < 25:
            break
        pdf.drawString(M, y, leg)
        y -= 10


# ═══════════════════════════════════════════════════════════
#  pg_track_record — Portfolio performance history
# ═══════════════════════════════════════════════════════════

def pg_track_record(pdf, portfolio):
    """Track record page — portfolio snapshot + position summary."""

    pdf.setFillColor(HexColor(BG))
    pdf.rect(0, 0, PAGE_W, PAGE_H, fill=1, stroke=0)

    # Title bar
    pdf.setFillColor(HexColor(PANEL))
    pdf.rect(0, PAGE_H - 52, PAGE_W, 52, fill=1, stroke=0)
    pdf.setFillColor(HexColor(TEXT))
    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(M, PAGE_H - 36, "Track Record")
    pdf.setFillColor(HexColor(TEXT_MUT))
    pdf.setFont("Helvetica", 9)
    pdf.drawString(M + 170, PAGE_H - 36, f"Portfolio Snapshot | {TODAY_STR}")

    if not portfolio or not portfolio.get("positions"):
        pdf.setFillColor(HexColor(TEXT_MUT))
        pdf.setFont("Helvetica", 11)
        pdf.drawString(M, PAGE_H - 90, "Dados do portfolio IBKR nao disponiveis.")
        return

    summ = portfolio.get("summary", {})
    positions = portfolio.get("positions", [])
    pgreeks = portfolio.get("portfolio_greeks", {})

    # ── KPI Cards ──
    y = PAGE_H - 75
    card_w = 150
    kpis = [
        ("NET LIQUIDATION", f"${float(summ.get('NetLiquidation', 0)):,.0f}", TEXT),
        ("BUYING POWER", f"${float(summ.get('BuyingPower', 0)):,.0f}", BLUE),
        ("UNREALIZED P&L", f"${float(summ.get('UnrealizedPnL', 0)):,.0f}",
         GREEN if float(summ.get('UnrealizedPnL', 0)) >= 0 else RED),
        ("DELTA TOTAL", f"{pgreeks.get('total_delta', 0):.1f}", AMBER),
        ("THETA/DIA", f"${pgreeks.get('total_theta', 0):.0f}", CYAN),
    ]

    for i, (label, value, clr) in enumerate(kpis):
        x = M + i * (card_w + 8)
        pdf.setFillColor(HexColor(PANEL2))
        pdf.roundRect(x, y - 6, card_w, 36, 4, fill=1, stroke=0)
        pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.setFont("Helvetica", 6)
        pdf.drawString(x + 8, y + 20, label)
        pdf.setFillColor(HexColor(clr))
        pdf.setFont("Helvetica-Bold", 13)
        pdf.drawString(x + 8, y + 2, value)

    # ── Positions table ──
    y -= 50
    pos_cols = [
        (M,        70, "INSTRUMENTO"),
        (M + 72,   40, "POS"),
        (M + 114,  60, "AVG COST"),
        (M + 176,  60, "MKT VALUE"),
        (M + 238,  50, "DELTA"),
        (M + 290,  50, "THETA"),
        (M + 342,  45, "IV%"),
    ]

    pdf.setFillColor(HexColor(PANEL2))
    pdf.rect(M - 4, y - 4, PAGE_W - 2 * M + 8, 14, fill=1, stroke=0)
    pdf.setFont("Helvetica-Bold", 6.5)
    for x, w, label in pos_cols:
        pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.drawString(x, y, label)

    y -= 16

    # Group by symbol
    by_sym = {}
    for p in positions:
        sym = p.get("symbol", "?")
        if sym not in by_sym:
            by_sym[sym] = []
        by_sym[sym].append(p)

    for sym in sorted(by_sym.keys()):
        legs = by_sym[sym]
        if y < 35:
            break

        # Group header
        pdf.setFillColor(HexColor("#1a2540"))
        pdf.rect(M - 4, y - 4, PAGE_W - 2 * M + 8, 14, fill=1, stroke=0)
        pdf.setFillColor(HexColor(AMBER))
        pdf.setFont("Helvetica-Bold", 8)
        pdf.drawString(M, y, f"{sym} ({len(legs)} legs)")
        y -= 15

        for p in legs:
            if y < 35:
                break
            ls = p.get("local_symbol", "")
            pos = p.get("position", 0)
            avg = p.get("avg_cost", 0)
            mkt = p.get("market_value", 0)
            delta = p.get("delta")
            theta = p.get("theta")
            iv = p.get("iv")

            pdf.setFont("Helvetica", 7.5)

            # Instrument
            pdf.setFillColor(HexColor(TEXT))
            lbl = ls if len(ls) < 20 else ls[:18] + ".."
            pdf.drawString(pos_cols[0][0], y, lbl)

            # Position
            clr = GREEN if pos > 0 else RED
            pdf.setFillColor(HexColor(clr))
            pdf.drawString(pos_cols[1][0], y, f"{pos:+.0f}")

            # Avg cost
            pdf.setFillColor(HexColor(TEXT_MUT))
            pdf.drawString(pos_cols[2][0], y, f"${avg:,.2f}")

            # Mkt value
            pdf.setFillColor(HexColor(TEXT))
            pdf.drawString(pos_cols[3][0], y, f"${mkt:,.0f}")

            # Delta
            if delta is not None:
                clr = GREEN if delta > 0 else RED
                pdf.setFillColor(HexColor(clr))
                pdf.drawString(pos_cols[4][0], y, f"{delta:.4f}")
            else:
                pdf.setFillColor(HexColor(TEXT_DIM))
                pdf.drawString(pos_cols[4][0], y, "-")

            # Theta
            if theta is not None:
                clr = GREEN if theta > 0 else RED
                pdf.setFillColor(HexColor(clr))
                pdf.drawString(pos_cols[5][0], y, f"{theta:.4f}")
            else:
                pdf.setFillColor(HexColor(TEXT_DIM))
                pdf.drawString(pos_cols[5][0], y, "-")

            # IV
            if iv is not None:
                pdf.setFillColor(HexColor(TEXT_MUT))
                pdf.drawString(pos_cols[6][0], y, f"{iv*100:.0f}%")
            else:
                pdf.setFillColor(HexColor(TEXT_DIM))
                pdf.drawString(pos_cols[6][0], y, "-")

            y -= 13

    # Footer info
    pdf.setFillColor(HexColor(TEXT_DIM))
    pdf.setFont("Helvetica", 6)
    gen_at = portfolio.get("generated_at", "?")
    acct = portfolio.get("account", "?")
    pdf.drawString(M, 22, f"Account: {acct} | Generated: {gen_at} | Posicoes: {len(positions)}")


# ═══════════════════════════════════════════════════════════
#  Patched build_pdf — wraps original + adds new pages
# ═══════════════════════════════════════════════════════════

def build_pdf_v4():
    """
    Run the original build_pdf, then append new pages.
    Strategy: re-build entirely with the extra pages injected.
    """
    # The original build_pdf saves to OUTPUT_PDF.
    # We call it, then re-open and append? No — ReportLab can't append.
    # Instead, we monkey-patch the original to add pages before pdf.save().

    print("=" * 60)
    print("AgriMacro v4 - PDF com Options Intelligence + Track Record")
    print("=" * 60)

    # Load extra data
    chain_data = sload(DATA_PROC, "options_chain.json")
    portfolio_data = sload(DATA_PROC, "ibkr_portfolio.json")

    has_chain = bool(chain_data and chain_data.get("underlyings"))
    has_portfolio = bool(portfolio_data and portfolio_data.get("positions"))

    print(f"  Options chain: {'OK (' + str(len(chain_data.get('underlyings', {}))) + ' underlyings)' if has_chain else 'SKIP'}")
    print(f"  Portfolio: {'OK (' + str(len(portfolio_data.get('positions', []))) + ' positions)' if has_portfolio else 'SKIP'}")

    # Patch: intercept pdf.save() to inject pages before saving
    original_save = None
    captured_pdf = [None]

    import reportlab.pdfgen.canvas as canvas_mod

    original_canvas_save = canvas_mod.Canvas.save

    def patched_save(self):
        """Intercept save to add extra pages."""
        extra_pages = 0

        if has_chain:
            print("  [+] pg_options_intelligence...")
            pg_options_intelligence(self, chain_data)
            # Update page count in footer
            total_est = 28  # approximate
            ftr(self, total_est - 1, total_est)
            self.showPage()
            extra_pages += 1

        if has_portfolio:
            print("  [+] pg_track_record...")
            pg_track_record(self, portfolio_data)
            total_est = 28
            ftr(self, total_est, total_est)
            self.showPage()
            extra_pages += 1

        print(f"  [+] {extra_pages} paginas extras adicionadas")
        original_canvas_save(self)

    # Apply monkey-patch
    canvas_mod.Canvas.save = patched_save

    try:
        base_pdf.build_pdf()
    finally:
        # Restore original save
        canvas_mod.Canvas.save = original_canvas_save

    # Report
    output_pdf = base_pdf.OUTPUT_PDF
    if os.path.exists(output_pdf):
        sz = os.path.getsize(output_pdf) / 1024
        print(f"\n  PDF v4: {output_pdf}")
        print(f"  Tamanho: {sz:.0f} KB")
    else:
        print(f"\n  [ERR] PDF nao gerado: {output_pdf}")


if __name__ == "__main__":
    build_pdf_v4()
