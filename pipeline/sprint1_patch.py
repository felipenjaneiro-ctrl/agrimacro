#!/usr/bin/env python3
"""
AgriMacro v3.3 Sprint 1 Patch
==============================
3 changes:
  1. Remove pg_news page from PDF assembly (decrement T)
  2. Fix COT/Basis page — "BASIS TEMPORAL" text overlap + improve readability
  3. Add seasonal context note to bilateral ERT section

Run from: pipeline/ folder
  python sprint1_patch.py

Creates backup before modifying.
"""
import re, os, sys, shutil
from datetime import datetime

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "generate_report_pdf.py")
BACKUP = SCRIPT + f".bak_sprint1_{datetime.now().strftime('%H%M%S')}"

def main():
    if not os.path.exists(SCRIPT):
        print(f"[ERRO] Arquivo nao encontrado: {SCRIPT}")
        sys.exit(1)

    # Backup
    shutil.copy2(SCRIPT, BACKUP)
    print(f"[OK] Backup: {BACKUP}")

    with open(SCRIPT, "r", encoding="utf-8") as f:
        src = f.read()

    original_len = len(src)
    changes = 0

    # ══════════════════════════════════════════════════════════════
    # 1. REMOVE pg_news FROM PAGE ASSEMBLY
    # ══════════════════════════════════════════════════════════════
    # Pattern: any line calling pg_news(...) with ftr and showPage
    news_pattern = re.compile(
        r'\n\s*#?\s*\d*\.?\s*(?:News|Noticias).*\n'  # comment line
        r'\s*pg_news\(pdf,\s*nw\);\s*ftr\(pdf,pn,T\);\s*pdf\.showPage\(\);\s*pn\+=1',
        re.IGNORECASE
    )
    if news_pattern.search(src):
        src = news_pattern.sub('\n    # [REMOVIDO v3.3] pg_news — pagina de noticias removida', src)
        changes += 1
        print("[1/3] OK: pg_news removido do assembly")
    else:
        # Try simpler pattern without comment line
        simple_news = re.compile(
            r'\s*pg_news\(pdf,\s*nw\);\s*ftr\(pdf,pn,T\);\s*pdf\.showPage\(\);\s*pn\+=1'
        )
        if simple_news.search(src):
            src = simple_news.sub('\n    # [REMOVIDO v3.3] pg_news — pagina de noticias removida', src)
            changes += 1
            print("[1/3] OK: pg_news removido do assembly (pattern simples)")
        else:
            print("[1/3] AVISO: pg_news nao encontrado no assembly — ja removido?")

    # Decrement T
    t_match = re.search(r'T\s*=\s*(\d+)', src)
    if t_match:
        old_t = int(t_match.group(1))
        new_t = old_t - 1
        src = src.replace(t_match.group(0), f"T = {new_t}", 1)
        changes += 1
        print(f"[1/3] OK: T = {old_t} -> T = {new_t}")

    # Also remove nw loading if desired (keep it — other pages might use news data)
    # We keep the loading, just remove the page render

    # ══════════════════════════════════════════════════════════════
    # 2. FIX COT/BASIS PAGE — OVERLAP + READABILITY
    # ══════════════════════════════════════════════════════════════
    # The pg_cot_basis function has "BASIS TEMPORAL" title overlapping the COT table.
    # We need to find and fix the y-coordinate positioning.

    # Strategy: Find the pg_cot_basis function and replace it entirely with improved version
    cot_basis_start = re.search(r'^def pg_cot_basis\(', src, re.MULTILINE)
    if cot_basis_start:
        # Find the end of the function (next def or end of file)
        func_start = cot_basis_start.start()
        next_def = re.search(r'^def \w+\(', src[func_start + 10:], re.MULTILINE)
        if next_def:
            func_end = func_start + 10 + next_def.start()
        else:
            func_end = len(src)

        old_func = src[func_start:func_end]

        # Create improved replacement
        new_cot_basis = '''def pg_cot_basis(pdf, cd):
    """Page: COT Scorecard + Basis Temporal (v3.3 — fixed overlap, improved readability)"""
    dbg(pdf)

    # ── HEADER ──
    hdr(pdf, "POSICAO DOS FUNDOS — Quem ta apostando esta semana",
        "Dados semanais CFTC | Maiores posicoes especulativas",
        "Verde = fundos comprados (apostam na alta). Vermelho = vendidos (apostam na queda). !! = posicao extrema.")

    y = PAGE_H - M - 60

    # ── SECTION 1: COT SCORECARD ──
    # Read cross-analysis data
    cross_dir = os.path.join(DATA_DIR, "bilateral")
    cot_data = {}
    cot_file = os.path.join(cross_dir, "cot_momentum.json")
    if os.path.exists(cot_file):
        try:
            import json
            with open(cot_file, "r") as f:
                cot_data = json.load(f)
        except:
            pass

    symbols_data = cot_data.get("symbols", cot_data.get("commodities", {}))

    # Separate into LONG and SHORT
    longs = []
    shorts = []
    for sym, d in symbols_data.items():
        net = d.get("net_position", d.get("noncommercial_net", 0))
        if net is None:
            net = 0
        delta = d.get("delta_1w", d.get("weekly_change", 0))
        if delta is None:
            delta = 0
        extreme = d.get("extreme", d.get("is_extreme", False))
        pct_oi = d.get("pct_oi", d.get("percent_oi", 0))
        if pct_oi is None:
            pct_oi = 0
        nm = NM.get(sym, sym)
        entry = {"sym": sym, "name": nm, "net": net, "delta": delta, "extreme": extreme, "pct_oi": pct_oi}
        if net >= 0:
            longs.append(entry)
        else:
            shorts.append(entry)

    # Sort by absolute net position
    longs.sort(key=lambda x: abs(x["net"]), reverse=True)
    shorts.sort(key=lambda x: abs(x["net"]), reverse=True)

    # Layout: two columns
    col_w = (PAGE_W - 2 * M - 20) / 2

    # ── LEFT COLUMN: FUNDOS COMPRADOS ──
    pdf.setFillColor(HexColor(GREEN))
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(M, y, "FUNDOS COMPRADOS (apostam na alta)")
    y_left = y - 18

    for entry in longs[:8]:
        if y_left < 280:
            break
        # Background panel
        bc = AMBER if entry["extreme"] else PANEL
        panel(pdf, M, y_left - 16, col_w, 18, bc=bc)

        # Name
        pdf.setFillColor(HexColor(GREEN))
        pdf.setFont("Helvetica-Bold", 8)
        pdf.drawString(M + 6, y_left - 11, entry["name"][:20])

        # Net position
        pdf.setFillColor(HexColor(TEXT))
        pdf.setFont("Helvetica", 8)
        net_str = f"+{entry['net']:,.0f}" if entry["net"] > 0 else f"{entry['net']:,.0f}"
        pdf.drawString(M + 130, y_left - 11, net_str)

        # Delta
        if entry["delta"] != 0:
            dc = GREEN if entry["delta"] > 0 else RED
            pdf.setFillColor(HexColor(dc))
            pdf.setFont("Helvetica", 7)
            pdf.drawString(M + 220, y_left - 11, f"({entry['delta']:+,.0f} sem)")

        # Extreme badge
        if entry["extreme"]:
            pdf.setFillColor(HexColor(RED))
            pdf.setFont("Helvetica-Bold", 7)
            pdf.drawString(M + col_w - 55, y_left - 11, "!! EXTREMO")

        y_left -= 20

    # ── RIGHT COLUMN: FUNDOS VENDIDOS ──
    rx = M + col_w + 20
    pdf.setFillColor(HexColor(RED))
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(rx, y, "FUNDOS VENDIDOS (apostam na queda)")
    y_right = y - 18

    for entry in shorts[:8]:
        if y_right < 280:
            break
        bc = AMBER if entry["extreme"] else PANEL
        panel(pdf, rx, y_right - 16, col_w, 18, bc=bc)

        pdf.setFillColor(HexColor(RED))
        pdf.setFont("Helvetica-Bold", 8)
        pdf.drawString(rx + 6, y_right - 11, entry["name"][:20])

        pdf.setFillColor(HexColor(TEXT))
        pdf.setFont("Helvetica", 8)
        pdf.drawString(rx + 130, y_right - 11, f"{entry['net']:,.0f}")

        if entry["delta"] != 0:
            dc = GREEN if entry["delta"] > 0 else RED
            pdf.setFillColor(HexColor(dc))
            pdf.setFont("Helvetica", 7)
            pdf.drawString(rx + 220, y_right - 11, f"({entry['delta']:+,.0f} sem)")

        if entry["extreme"]:
            pdf.setFillColor(HexColor(RED))
            pdf.setFont("Helvetica-Bold", 7)
            pdf.drawString(rx + col_w - 55, y_right - 11, "!! EXTREMO")

        y_right -= 20

    # Count extremes for summary
    n_extreme = sum(1 for s in list(symbols_data.values()) if s.get("extreme", s.get("is_extreme", False)))

    # ── DIVIDER ──
    y_div = min(y_left, y_right) - 10
    pdf.setStrokeColor(HexColor(BORDER))
    pdf.setLineWidth(0.5)
    pdf.line(M, y_div, PAGE_W - M, y_div)

    # ── SECTION 2: BASIS TEMPORAL (below divider, no overlap) ──
    y_basis = y_div - 20

    pdf.setFillColor(HexColor(CYAN))
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(M, y_basis, "BASE BRASIL vs CHICAGO (em Reais)")
    pdf.setFillColor(HexColor(TEXT_DIM))
    pdf.setFont("Helvetica", 7)
    pdf.drawString(M + 290, y_basis + 1, "Fonte: CEPEA/ESALQ + CME/CBOT + BCB/SGS")
    y_basis -= 18

    # Load basis data
    basis_data = {}
    basis_file = os.path.join(cross_dir, "basis_temporal.json")
    if os.path.exists(basis_file):
        try:
            import json
            with open(basis_file, "r") as f:
                basis_data = json.load(f)
        except:
            pass

    comms = basis_data.get("commodities", {})

    # Table header
    cols_b = [M, M + 100, M + 220, M + 340, M + 440, M + 560]
    hdrs_b = ["Produto", "Fisico BR (R$)", "Chicago (R$)", "Spread", "Sinal", "Diagnostico"]
    pdf.setFillColor(HexColor(PANEL))
    pdf.roundRect(M, y_basis - 14, PAGE_W - 2 * M, 16, 2, fill=1)
    pdf.setFillColor(HexColor(TEXT))
    pdf.setFont("Helvetica-Bold", 7.5)
    for i, h in enumerate(hdrs_b):
        pdf.drawString(cols_b[i] + 4, y_basis - 10, h)
    y_basis -= 18

    # Basis diagnostics in Portuguese
    SIGNAL_PT = {
        "HIGH_PREMIUM": "Premio alto — demanda interna forte",
        "PREMIUM": "Premio — mercado local paga mais",
        "NEUTRAL": "Neutro — equilibrio",
        "DISCOUNT": "Desconto — oportunidade de exportacao",
        "DEEP_DISCOUNT": "Desconto forte — exportacao atrativa",
        "EXPORT_WINDOW": "Janela de exportacao aberta",
    }

    for key in ["soja", "milho", "boi_gordo", "boi gordo", "cafe"]:
        c = comms.get(key, {})
        if not c:
            # Try alternative keys
            for k, v in comms.items():
                if key.replace("_", " ") in k.lower() or key in k.lower():
                    c = v
                    break
        if not c:
            continue

        br_price = c.get("br_price", c.get("fisico_br", 0))
        chi_price = c.get("chi_brl", c.get("chicago_brl", 0))
        basis_pct = c.get("basis_pct", c.get("spread_pct", 0))
        signal = c.get("signal", "N/A")
        diag = SIGNAL_PT.get(signal, signal)
        nm = key.replace("_", " ").title()

        if y_basis < 55:
            break

        # Row background
        sc = GREEN if basis_pct > 5 else (RED if basis_pct < -10 else PANEL)
        panel(pdf, M, y_basis - 14, PAGE_W - 2 * M, 16, bc=sc if sc != PANEL else None)

        pdf.setFillColor(HexColor(TEXT))
        pdf.setFont("Helvetica-Bold", 7.5)
        pdf.drawString(cols_b[0] + 4, y_basis - 10, nm)

        pdf.setFont("Helvetica", 7.5)
        if br_price:
            pdf.drawString(cols_b[1] + 4, y_basis - 10, f"R$ {br_price:,.2f}")
        if chi_price:
            pdf.drawString(cols_b[2] + 4, y_basis - 10, f"R$ {chi_price:,.2f}")

        bc = GREEN if basis_pct > 0 else RED
        pdf.setFillColor(HexColor(bc))
        pdf.setFont("Helvetica-Bold", 7.5)
        pdf.drawString(cols_b[3] + 4, y_basis - 10, f"{basis_pct:+.1f}%")

        sc2 = AMBER if "EXTREME" in signal or "DEEP" in signal else (GREEN if "PREMIUM" in signal else CYAN)
        pdf.setFillColor(HexColor(sc2))
        pdf.setFont("Helvetica-Bold", 7)
        pdf.drawString(cols_b[4] + 4, y_basis - 10, signal)

        pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.setFont("Helvetica", 6.5)
        pdf.drawString(cols_b[5] + 4, y_basis - 10, diag[:35])

        y_basis -= 18

    # ── ANALYSIS BOX ──
    y_an = y_basis - 10
    if y_an > 45:
        # Gold left border analysis box
        pdf.setFillColor(HexColor(PANEL))
        pdf.roundRect(M, y_an - 40, PAGE_W - 2 * M, 42, 3, fill=1)
        pdf.setStrokeColor(HexColor(AMBER))
        pdf.setLineWidth(3)
        pdf.line(M, y_an - 40, M, y_an + 2)

        pdf.setFillColor(HexColor(AMBER))
        pdf.setFont("Helvetica-Bold", 8)
        pdf.drawString(M + 10, y_an - 8, "ANALISE DO DIA")

        # Build analysis text from data
        extreme_names = []
        for sym, d in symbols_data.items():
            if d.get("extreme", d.get("is_extreme", False)):
                extreme_names.append(NM.get(sym, sym))

        analysis = f"{n_extreme} mercados em posicao extrema"
        if extreme_names:
            analysis += f" ({', '.join(extreme_names[:4])})"
        analysis += ". "

        # Add basis insight
        soja_basis = comms.get("soja", {}).get("basis_pct", 0)
        if soja_basis:
            if soja_basis < -3:
                analysis += f"Soja BR com desconto de {abs(soja_basis):.1f}% sobre Chicago — tipico de colheita. "
            elif soja_basis > 3:
                analysis += f"Soja BR com premio de {soja_basis:.1f}% — demanda interna forte. "

        milho_basis = comms.get("milho", {}).get("basis_pct", 0)
        if milho_basis and milho_basis > 15:
            analysis += f"Milho com premio de {milho_basis:.1f}% reflete logistica cara e demanda interna."

        pdf.setFillColor(HexColor(TEXT))
        pdf.setFont("Helvetica", 7.5)
        # Word wrap
        words = analysis.split()
        line = ""
        ly = y_an - 22
        for w in words:
            test = line + " " + w if line else w
            if len(test) > 130:
                pdf.drawString(M + 10, ly, line)
                line = w
                ly -= 10
            else:
                line = test
        if line:
            pdf.drawString(M + 10, ly, line)


'''
        src = src[:func_start] + new_cot_basis + src[func_end:]
        changes += 1
        print("[2/3] OK: pg_cot_basis reescrito (scorecard + basis sem overlap + analise)")
    else:
        print("[2/3] AVISO: pg_cot_basis nao encontrado — verificar se existe")

    # ══════════════════════════════════════════════════════════════
    # 3. ADD SEASONAL CONTEXT TO BILATERAL ERT SECTION
    # ══════════════════════════════════════════════════════════════
    # Find the ERT section in pg_bilateral() and add seasonal context note
    # Look for the line that draws "CORRIDA DE EXPORTACAO" or export race data

    # Strategy: find where pace/export data is rendered and add context note before it
    # Look for pattern like "CORRIDA DE EXPORTACAO" or "Export Race" or pace rendering

    ert_pattern = re.compile(
        r'(pdf\.drawString\([^)]*["\']CORRIDA DE EXPORTACAO[^"\']*["\'][^)]*\))',
        re.IGNORECASE
    )
    ert_match = ert_pattern.search(src)

    if ert_match:
        # Insert seasonal context NOTE after the title line
        old_line = ert_match.group(0)
        seasonal_block = old_line + """

    # ── SEASONAL CONTEXT (v3.3) ──
    import calendar
    from datetime import datetime as _dt
    _month = _dt.now().month
    _season_notes = {
        1: "Jan: Colheita BR iniciando em MT. EUA dominam embarques.",
        2: "Fev: Colheita BR em MT/GO. Exportacoes BR ainda minimas — NORMAL.",
        3: "Mar: BR comeca embarcar forte. Virada sazonal iminente.",
        4: "Abr: BR assume lideranca. Pico de embarques Santos/Paranagua.",
        5: "Mai: BR domina embarques globais. EUA em plantio.",
        6: "Jun: BR lidera. Safrinha milho em colheita.",
        7: "Jul: BR lidera exportacoes. EUA em desenvolvimento de safra.",
        8: "Ago: BR ainda lidera. EUA preparando colheita.",
        9: "Set: Transicao — EUA comeca colheita, BR desacelera.",
        10: "Out: EUA assume embarques. Colheita americana em pico.",
        11: "Nov: EUA domina. BR em plantio da nova safra.",
        12: "Dez: Entressafra BR. EUA lidera embarques."
    }
    _note = _season_notes.get(_month, "")
    if _note:
        _sy = y - 14  # position below title
        pdf.setFillColor(HexColor(AMBER))
        pdf.setFont("Helvetica-Bold", 7)
        pdf.drawString(M + 10, _sy, f"CONTEXTO SAZONAL: {_note}")
        y = _sy - 6
"""
        src = src.replace(old_line, seasonal_block)
        changes += 1
        print("[3/3] OK: Contexto sazonal adicionado ao ERT bilateral")
    else:
        # Try alternative: look for "us_pace" or export pace rendering
        alt_pattern = re.compile(r'(#\s*(?:ERT|Export Race|Corrida).*?\n)', re.IGNORECASE)
        alt_match = alt_pattern.search(src)
        if alt_match:
            old_comment = alt_match.group(0)
            seasonal_insert = old_comment + """    # ── SEASONAL CONTEXT (v3.3) ──
    import calendar
    from datetime import datetime as _dt
    _month = _dt.now().month
    _season_map = {1:"Jan: Colheita BR iniciando. EUA dominam embarques.",
        2:"Fev: Colheita BR em MT/GO. Exportacoes BR minimas — NORMAL para o periodo.",
        3:"Mar: BR comeca embarcar forte. Virada sazonal.",
        4:"Abr-Mai: BR assume lideranca global.",
        5:"Abr-Mai: BR domina embarques.",
        6:"Jun-Jul: BR lidera. Safrinha em colheita.",
        7:"Jun-Jul: BR lidera exportacoes.",
        8:"Ago-Set: Transicao. BR desacelera, EUA prepara colheita.",
        9:"Ago-Set: EUA comeca colheita.",
        10:"Out-Nov: EUA domina embarques.",
        11:"Out-Nov: EUA lidera. BR plantando.",
        12:"Dez: Entressafra BR. EUA lidera."}
    _snote = _season_map.get(_dt.now().month, "")
    if _snote:
        pdf.setFillColor(HexColor(AMBER)); pdf.setFont("Helvetica-Bold",7)
        pdf.drawString(M+10, y-2, f"PERIODO: {_snote}")
        y -= 14
"""
            src = src.replace(old_comment, seasonal_insert)
            changes += 1
            print("[3/3] OK: Contexto sazonal adicionado (pattern alternativo)")
        else:
            # Last resort: add note in pg_bilateral near the top
            bilat_func = re.search(r'def pg_bilateral\(', src)
            if bilat_func:
                print("[3/3] AVISO: Nao encontrei padrao ERT especifico em pg_bilateral.")
                print("         Nota sazonal precisa ser adicionada manualmente.")
            else:
                print("[3/3] AVISO: pg_bilateral nao encontrado — bilateral ainda nao integrado?")

    # ══════════════════════════════════════════════════════════════
    # SAVE
    # ══════════════════════════════════════════════════════════════
    if changes > 0:
        with open(SCRIPT, "w", encoding="utf-8") as f:
            f.write(src)
        print(f"\n[DONE] {changes} mudancas aplicadas em {SCRIPT}")
        print(f"  Backup em: {BACKUP}")
        print(f"\nAgora rode: python generate_report_pdf.py")
    else:
        print("\n[INFO] Nenhuma mudanca aplicada.")
        # Restore backup
        shutil.copy2(BACKUP, SCRIPT)
        os.remove(BACKUP)


if __name__ == "__main__":
    main()
