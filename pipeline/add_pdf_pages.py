#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AgriMacro - Inject Pages 10 (Weather) and 11 (News) into generate_report_pdf.py
Run ONCE to patch the PDF generator. Creates a backup first.
Usage: python add_pdf_pages.py
"""
import os, shutil

PDF_FILE = os.path.join(os.path.dirname(__file__), "generate_report_pdf.py")
BACKUP = PDF_FILE + ".bak"

# ── New page functions to inject ──────────────────────────────────
NEW_FUNCTIONS = '''

# ── PAGE 10: CLIMA & SAFRA ───────────────────────────────────────
def pg_weather(pdf, weather_data):
    """Page 10: Agricultural weather - forecasts and alerts"""
    pdf.setFillColor(HexColor(BG))
    pdf.rect(0, 0, PAGE_W, PAGE_H, fill=1)

    # Title
    pdf.setFillColor(HexColor(CYAN))
    pdf.setFont("Helvetica-Bold", 22)
    pdf.drawString(M, PAGE_H - 45, "CLIMA & SAFRA")
    pdf.setFillColor(HexColor(TEXT_MUT))
    pdf.setFont("Helvetica", 10)
    pdf.drawString(M, PAGE_H - 60, f"Previsao 15 dias para regioes agricolas chave | {TODAY_BR}")

    if not weather_data or not weather_data.get("regions"):
        pdf.setFillColor(HexColor(TEXT_MUT))
        pdf.setFont("Helvetica", 14)
        pdf.drawString(M, PAGE_H - 120, "Dados climaticos nao disponiveis nesta execucao.")
        return

    regions = weather_data["regions"]
    enso = weather_data.get("enso", {})

    # ENSO banner
    enso_status = enso.get("status", "N/A")
    oni = enso.get("oni_value", "N/A")
    enso_color = RED if enso_status == "El Nino" else (BLUE if enso_status == "La Nina" else AMBER)
    pdf.setFillColor(HexColor(PANEL))
    pdf.roundRect(M, PAGE_H - 90, PAGE_W - 2*M, 22, 4, fill=1)
    pdf.setFillColor(HexColor(enso_color))
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(M + 8, PAGE_H - 85, f"ENSO: {enso_status} (ONI = {oni})")
    pdf.setFillColor(HexColor(TEXT_MUT))
    pdf.setFont("Helvetica", 9)
    pdf.drawString(M + 250, PAGE_H - 85, "Fonte: NOAA CPC")

    # Region cards
    y_start = PAGE_H - 115
    card_w = (PAGE_W - 2*M - 20) / 3
    card_h = 200
    col = 0
    row_y = y_start

    for key, reg in regions.items():
        if reg.get("error"):
            continue
        x = M + col * (card_w + 10)
        y = row_y

        # Card background
        pdf.setFillColor(HexColor(PANEL))
        pdf.roundRect(x, y - card_h, card_w, card_h, 6, fill=1)

        # Region name
        pdf.setFillColor(HexColor(CYAN))
        pdf.setFont("Helvetica-Bold", 11)
        pdf.drawString(x + 8, y - 18, reg.get("label", key))

        # Crops
        crops = ", ".join(reg.get("crops", []))
        pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.setFont("Helvetica", 7)
        pdf.drawString(x + 8, y - 30, crops[:40])

        # Current conditions
        cur = reg.get("current", {})
        pdf.setFillColor(HexColor(TEXT))
        pdf.setFont("Helvetica", 9)
        tmax = cur.get("temp_max", "?")
        tmin = cur.get("temp_min", "?")
        pdf.drawString(x + 8, y - 48, f"Hoje: {tmin}C / {tmax}C")

        # Precip 7d / 15d
        p7 = reg.get("precip_7d_mm", 0)
        p15 = reg.get("precip_15d_mm", 0)
        pdf.drawString(x + 8, y - 62, f"Chuva 7d: {p7:.0f}mm | 15d: {p15:.0f}mm")

        # Temp range 7d
        tmin7 = reg.get("temp_min_7d", "?")
        tmax7 = reg.get("temp_max_7d", "?")
        pdf.drawString(x + 8, y - 76, f"Temp 7d: {tmin7}C a {tmax7}C")

        # Mini forecast bars (7 days precipitation)
        forecast = reg.get("forecast_15d", [])[:7]
        if forecast:
            bar_y = y - 110
            bar_w = (card_w - 24) / 7
            max_precip = max((f.get("precip_mm", 0) for f in forecast), default=1) or 1
            pdf.setFillColor(HexColor(TEXT_DIM))
            pdf.setFont("Helvetica", 6)
            pdf.drawString(x + 8, bar_y + 22, "Precip. 7 dias:")
            for i, f in enumerate(forecast):
                bx = x + 8 + i * bar_w
                precip = f.get("precip_mm", 0)
                bh = max(1, (precip / max_precip) * 30)
                color = BLUE if precip < 20 else (CYAN if precip < 50 else AMBER)
                pdf.setFillColor(HexColor(color))
                pdf.rect(bx + 1, bar_y - 30, bar_w - 2, bh, fill=1)
                pdf.setFillColor(HexColor(TEXT_DIM))
                pdf.setFont("Helvetica", 5)
                day_label = f.get("date", "")[-2:] if f.get("date") else ""
                pdf.drawCentredString(bx + bar_w/2, bar_y - 38, day_label)

        # Alerts
        alerts = reg.get("alerts", [])
        if alerts:
            alert_y = y - card_h + 30
            for a in alerts[:2]:
                sev_color = RED if a.get("severity") == "ALTA" else AMBER
                pdf.setFillColor(HexColor(sev_color))
                pdf.setFont("Helvetica-Bold", 7)
                pdf.drawString(x + 8, alert_y, f"⚠ {a['type']}: {a.get('message','')[:50]}")
                alert_y -= 12
        else:
            pdf.setFillColor(HexColor(GREEN))
            pdf.setFont("Helvetica", 7)
            pdf.drawString(x + 8, y - card_h + 18, "Sem alertas")

        # Source
        pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.setFont("Helvetica", 6)
        pdf.drawString(x + 8, y - card_h + 6, f"Fonte: {reg.get('source','')}")

        col += 1
        if col >= 3:
            col = 0
            row_y -= (card_h + 10)

    # Summary at bottom
    summary = weather_data.get("summary", "")
    if summary:
        pdf.setFillColor(HexColor(TEXT_MUT))
        pdf.setFont("Helvetica", 8)
        # Word wrap summary
        words = summary.split()
        lines = []
        line = ""
        for w in words:
            if len(line + " " + w) > 120:
                lines.append(line)
                line = w
            else:
                line = (line + " " + w).strip()
        if line:
            lines.append(line)
        sy = 45
        for l in lines[:3]:
            pdf.drawString(M, sy, l)
            sy -= 12


# ── PAGE 11: NOTICIAS & CONTEXTO ────────────────────────────────
def pg_news(pdf, news_data):
    """Page 11: Top news from Brazilian and international sources"""
    pdf.setFillColor(HexColor(BG))
    pdf.rect(0, 0, PAGE_W, PAGE_H, fill=1)

    # Title
    pdf.setFillColor(HexColor(PURPLE))
    pdf.setFont("Helvetica-Bold", 22)
    pdf.drawString(M, PAGE_H - 45, "NOTICIAS & CONTEXTO")
    pdf.setFillColor(HexColor(TEXT_MUT))
    pdf.setFont("Helvetica", 10)
    pdf.drawString(M, PAGE_H - 60, f"Principais noticias do agronegocio | {TODAY_BR}")

    if not news_data or not news_data.get("news"):
        pdf.setFillColor(HexColor(TEXT_MUT))
        pdf.setFont("Helvetica", 14)
        pdf.drawString(M, PAGE_H - 120, "Noticias nao disponiveis nesta execucao.")
        return

    articles = news_data["news"]

    # Split by category
    br_news = [a for a in articles if a.get("category") in ("br_agro", "br_gov")]
    us_news = [a for a in articles if a.get("category") in ("usda", "market", "us_agro")]

    # Category colors
    CAT_COLORS = {
        "br_agro": TEAL, "br_gov": GREEN, "usda": BLUE,
        "market": AMBER, "us_agro": CYAN,
    }

    half_w = (PAGE_W - 2*M - 15) / 2

    # Left column: Brazil
    pdf.setFillColor(HexColor(TEAL))
    pdf.setFont("Helvetica-Bold", 13)
    pdf.drawString(M, PAGE_H - 85, "BRASIL")
    y = PAGE_H - 105
    for art in br_news[:8]:
        if y < 60:
            break
        # Card
        pdf.setFillColor(HexColor(PANEL))
        pdf.roundRect(M, y - 42, half_w, 40, 4, fill=1)
        # Source tag
        cat_color = CAT_COLORS.get(art.get("category", ""), TEXT_DIM)
        pdf.setFillColor(HexColor(cat_color))
        pdf.setFont("Helvetica-Bold", 7)
        pdf.drawString(M + 6, y - 12, art.get("source", "")[:25])
        # Title (truncate)
        title = art.get("title", "")
        if len(title) > 75:
            title = title[:72] + "..."
        pdf.setFillColor(HexColor(TEXT))
        pdf.setFont("Helvetica", 9)
        pdf.drawString(M + 6, y - 25, title)
        # Description snippet
        desc = art.get("description", "")
        if len(desc) > 90:
            desc = desc[:87] + "..."
        pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.setFont("Helvetica", 7)
        pdf.drawString(M + 6, y - 36, desc)
        y -= 48

    # Right column: International
    rx = M + half_w + 15
    pdf.setFillColor(HexColor(BLUE))
    pdf.setFont("Helvetica-Bold", 13)
    pdf.drawString(rx, PAGE_H - 85, "INTERNACIONAL")
    y = PAGE_H - 105
    for art in us_news[:8]:
        if y < 60:
            break
        pdf.setFillColor(HexColor(PANEL))
        pdf.roundRect(rx, y - 42, half_w, 40, 4, fill=1)
        cat_color = CAT_COLORS.get(art.get("category", ""), TEXT_DIM)
        pdf.setFillColor(HexColor(cat_color))
        pdf.setFont("Helvetica-Bold", 7)
        pdf.drawString(rx + 6, y - 12, art.get("source", "")[:25])
        title = art.get("title", "")
        if len(title) > 75:
            title = title[:72] + "..."
        pdf.setFillColor(HexColor(TEXT))
        pdf.setFont("Helvetica", 9)
        pdf.drawString(rx + 6, y - 25, title)
        desc = art.get("description", "")
        if len(desc) > 90:
            desc = desc[:87] + "..."
        pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.setFont("Helvetica", 7)
        pdf.drawString(rx + 6, y - 36, desc)
        y -= 48

    # Footer stats
    total = news_data.get("total_news", 0)
    cats = news_data.get("by_category", {})
    cat_str = " | ".join(f"{k}: {v}" for k, v in cats.items()) if cats else ""
    pdf.setFillColor(HexColor(TEXT_DIM))
    pdf.setFont("Helvetica", 8)
    pdf.drawString(M, 40, f"Total: {total} artigos | {cat_str}")
    pdf.drawString(M, 28, f"Fontes: Canal Rural, Google News, Yahoo Finance, USDA | Gerado: {TODAY_BR}")

'''

def main():
    if not os.path.exists(PDF_FILE):
        print(f"[ERROR] File not found: {PDF_FILE}")
        return

    # Backup
    if not os.path.exists(BACKUP):
        shutil.copy2(PDF_FILE, BACKUP)
        print(f"[OK] Backup: {BACKUP}")
    else:
        print(f"[INFO] Backup already exists")

    content = open(PDF_FILE, "r", encoding="utf-8").read()

    # Check if already patched
    if "pg_weather" in content:
        print("[INFO] Pages already injected — skipping")
        return

    # 1. Insert new functions BEFORE build_pdf()
    marker = "def build_pdf():"
    if marker not in content:
        print(f"[ERROR] Cannot find '{marker}' in file")
        return

    content = content.replace(marker, NEW_FUNCTIONS + "\n" + marker)
    print("[OK] Injected pg_weather() and pg_news() functions")

    # 2. Add weather and news data loading in build_pdf()
    # After:  rd   = sload(DATA_PROC, "report_daily.json")
    # Insert: wt   = sload(DATA_PROC, "weather_agro.json")
    #         nw   = sload(DATA_PROC, "news.json")
    old_load = 'rd   = sload(DATA_PROC, "report_daily.json")'
    new_load = '''rd   = sload(DATA_PROC, "report_daily.json")
    wt   = sload(DATA_PROC, "weather_agro.json")
    nw   = sload(DATA_PROC, "news.json")'''
    if old_load in content:
        content = content.replace(old_load, new_load)
        print("[OK] Added weather + news data loading")
    else:
        print("[WARN] Could not find load marker — add manually")

    # 3. Update T=9 to T=11
    content = content.replace("T=9", "T=11")
    print("[OK] Updated page count T=9 -> T=11")

    # 4. Add new pages after pg_calendar line
    old_cal = "pg_calendar(pdf,cal,rd,dr);   ftr(pdf,9,T); pdf.showPage()"
    new_cal = """pg_calendar(pdf,cal,rd,dr);   ftr(pdf,9,T); pdf.showPage()
    pg_weather(pdf,wt);           ftr(pdf,10,T); pdf.showPage()
    pg_news(pdf,nw);              ftr(pdf,11,T); pdf.showPage()"""
    if old_cal in content:
        content = content.replace(old_cal, new_cal)
        print("[OK] Added pg_weather (page 10) and pg_news (page 11)")
    else:
        print("[WARN] Could not find pg_calendar line — add pages manually")

    # Save
    with open(PDF_FILE, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"\n[DONE] {PDF_FILE} updated to 11 pages!")

if __name__ == "__main__":
    main()
