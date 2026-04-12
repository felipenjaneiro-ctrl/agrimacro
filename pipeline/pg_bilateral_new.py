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

    # === BOTTOM: Export Race ===
    y -= 228

    sig_pt_map = {"AHEAD": "Adiantado", "ON_PACE": "No ritmo", "BEHIND": "Atrasado"}

    if ert.get("status") == "OK":
        panel(pdf, M, y - 148, PAGE_W - 2 * M, 146)
        py = y - 12

        us_ytd = ert.get("us_ytd_mmt", 0)
        us_pace = ert.get("us_pace_pct", 0)
        br_ytd = ert.get("br_ytd_mmt", 0)
        br_pace = ert.get("br_pace_pct", 0)
        us_sig = ert.get("us_pace_signal", "N/A")
        br_sig = ert.get("br_pace_signal", "N/A")
        china_br = ert.get("china_br_share_pct", 0)
        china_us = ert.get("china_us_share_pct", 0)
        br_share = ert.get("br_market_share_pct", 0)
        us_share = ert.get("us_market_share_pct", 0)
        shift = ert.get("share_shift_pp", 0)
        data_src = ert.get("data_source", "wasde_seasonal_estimate")
        my_label = ert.get("marketing_year", "")

        # Title
        pdf.setFillColor(HexColor(AMBER))
        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(M + 10, py, f"CORRIDA DE EXPORTA\u00c7\u00c3O SOJA ({my_label})")

        if data_src == "wasde_seasonal_estimate":
            pdf.setFillColor(HexColor(TEXT_DIM))
            pdf.setFont("Helvetica", 5.5)
            pdf.drawRightString(PAGE_W - M - 10, py, "Estimativa sazonal WASDE (dados reais indisponiveis)")
        py -= 14

        # Seasonal context
        import datetime as _dt
        _month = _dt.date.today().month
        _season_notes = {
            1: "Jan: Colheita BR iniciando em MT. EUA dominam embarques.",
            2: "Fev: Colheita BR em MT/GO. Exporta\u00e7\u00f5es BR ainda m\u00ednimas \u2014 normal para o per\u00edodo.",
            3: "Mar: BR come\u00e7a embarcar forte. Virada sazonal iminente.",
            4: "Abr: BR assume lideran\u00e7a. Pico de embarques Santos/Paranagu\u00e1.",
            5: "Mai: BR domina embarques globais. EUA em plantio.",
            6: "Jun: BR lidera. Safrinha milho em colheita.",
            7: "Jul: BR lidera exporta\u00e7\u00f5es. EUA em desenvolvimento de safra.",
            8: "Ago: BR ainda lidera. EUA preparando colheita.",
            9: "Set: Transi\u00e7\u00e3o \u2014 EUA come\u00e7a colheita, BR desacelera.",
            10: "Out: EUA assume embarques. Colheita americana em pico.",
            11: "Nov: EUA domina. BR em plantio da nova safra.",
            12: "Dez: Entressafra BR. EUA lidera embarques.",
        }
        _note = _season_notes.get(_month, "")
        if _note:
            pdf.setFillColor(HexColor(TEXT_DIM))
            pdf.setFont("Helvetica-Oblique", 6.5)
            pdf.drawString(M + 10, py, _note)
            py -= 14

        # Pace bars
        bar_full = 320
        bar_x0 = M + 130
        sig_colors = {"AHEAD": GREEN, "ON_PACE": AMBER, "BEHIND": RED}

        # US row
        pdf.setFillColor(HexColor(TEXT_MUT))
        pdf.setFont("Helvetica-Bold", 7.5)
        pdf.drawString(M + 10, py, f"EUA: {us_ytd:.1f} MMT")
        us_w = bar_full * min(us_pace, 100) / 100
        pdf.setFillColor(HexColor(AMBER))
        pdf.rect(bar_x0, py - 3, us_w, 11, fill=1, stroke=0)
        pdf.setFillColor(HexColor(BORDER))
        pdf.rect(bar_x0 + us_w, py - 3, bar_full - us_w, 11, fill=1, stroke=0)
        pdf.setFillColor(HexColor(TEXT))
        pdf.setFont("Helvetica", 6.5)
        pdf.drawString(bar_x0 + bar_full + 8, py, f"{us_pace:.0f}% do WASDE")
        us_sig_c = sig_colors.get(us_sig, AMBER)
        pdf.setFillColor(HexColor(us_sig_c))
        pdf.setFont("Helvetica-Bold", 6.5)
        pdf.drawString(bar_x0 + bar_full + 100, py, sig_pt_map.get(us_sig, us_sig))
        py -= 18

        # BR row
        pdf.setFillColor(HexColor(TEXT_MUT))
        pdf.setFont("Helvetica-Bold", 7.5)
        pdf.drawString(M + 10, py, f"BR: {br_ytd:.1f} MMT")
        br_w = bar_full * min(br_pace, 100) / 100
        pdf.setFillColor(HexColor(GREEN))
        pdf.rect(bar_x0, py - 3, br_w, 11, fill=1, stroke=0)
        pdf.setFillColor(HexColor(BORDER))
        pdf.rect(bar_x0 + br_w, py - 3, bar_full - br_w, 11, fill=1, stroke=0)
        pdf.setFillColor(HexColor(TEXT))
        pdf.setFont("Helvetica", 6.5)
        pdf.drawString(bar_x0 + bar_full + 8, py, f"{br_pace:.0f}% do WASDE")
        br_sig_c = sig_colors.get(br_sig, AMBER)
        pdf.setFillColor(HexColor(br_sig_c))
        pdf.setFont("Helvetica-Bold", 6.5)
        pdf.drawString(bar_x0 + bar_full + 100, py, sig_pt_map.get(br_sig, br_sig))
        py -= 20

        # Market Share + China
        pdf.setFillColor(HexColor(TEXT))
        pdf.setFont("Helvetica-Bold", 7.5)
        pdf.drawString(M + 10, py, "Fatia de Mercado:")
        ms_bar_w = 250
        ms_x = M + 130
        us_ms_w = ms_bar_w * us_share / 100
        br_ms_w = ms_bar_w * br_share / 100
        pdf.setFillColor(HexColor(AMBER))
        pdf.rect(ms_x, py - 3, us_ms_w, 13, fill=1, stroke=0)
        pdf.setFillColor(HexColor(GREEN))
        pdf.rect(ms_x + us_ms_w, py - 3, br_ms_w, 13, fill=1, stroke=0)
        pdf.setFillColor(HexColor("#000"))
        pdf.setFont("Helvetica-Bold", 6.5)
        if us_share > 12:
            pdf.drawString(ms_x + 4, py, f"EUA {us_share:.0f}%")
        if br_share > 12:
            pdf.drawString(ms_x + us_ms_w + 4, py, f"BR {br_share:.0f}%")
        shift_clr = GREEN if shift > 0 else RED
        pdf.setFillColor(HexColor(shift_clr))
        pdf.setFont("Helvetica", 6.5)
        pdf.drawString(ms_x + ms_bar_w + 8, py, f"vs 5 anos: {shift:+.1f}pp")

        # China
        if china_br > 0 or china_us > 0:
            cx = M + 520
            pdf.setFillColor(HexColor(TEXT))
            pdf.setFont("Helvetica-Bold", 7.5)
            pdf.drawString(cx, py, "China:")
            ch_bar_w = 140
            ch_x = cx + 50
            us_ch_w = ch_bar_w * china_us / 100
            br_ch_w = ch_bar_w * china_br / 100
            pdf.setFillColor(HexColor(AMBER))
            pdf.rect(ch_x, py - 3, us_ch_w, 13, fill=1, stroke=0)
            pdf.setFillColor(HexColor(GREEN))
            pdf.rect(ch_x + us_ch_w, py - 3, br_ch_w, 13, fill=1, stroke=0)
            pdf.setFillColor(HexColor("#000"))
            pdf.setFont("Helvetica-Bold", 6)
            if china_us > 10:
                pdf.drawString(ch_x + 3, py, f"EUA {china_us:.0f}%")
            if china_br > 10:
                pdf.drawString(ch_x + us_ch_w + 3, py, f"BR {china_br:.0f}%")

    # Source
    pdf.setFillColor(HexColor(TEXT_DIM))
    pdf.setFont("Helvetica", 5.5)
    pdf.drawString(M, 22, "Fontes: CEPEA/ESALQ, CBOT/CME, BCB/PTAX, USDA AMS GTR, Comex Stat/MDIC, IMEA | AgriMacro Intelligence 2026")
