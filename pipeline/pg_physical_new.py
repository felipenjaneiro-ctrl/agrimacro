def pg_physical(pdf, phys, img_phbr):
    dbg(pdf)
    hdr(pdf, "Mercado F\u00edsico \u2014 Pre\u00e7o a Vista no Brasil e no Mundo",
        "Pre\u00e7os reais de venda (CEPEA/ESALQ + origens internacionais)",
        "Compare o pre\u00e7o f\u00edsico com o futuro para achar oportunidades. Diferen\u00e7a grande = base forte.")

    intl = phys.get("international", {})

    # Separate BR and non-BR entries
    br_items = {}
    intl_items = {}
    for key, d in intl.items():
        price = d.get("price", "")
        if not price or str(price) in ("None", ""):
            continue
        if "_BR" in key:
            br_items[key] = d
        else:
            intl_items[key] = d

    y = PAGE_H - 75

    # === TOP: BR Physical Prices (main focus for producer) ===
    panel(pdf, M, y - 195, PAGE_W - 2 * M, 193)
    py = y - 14
    pdf.setFillColor(HexColor(AMBER))
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(M + 10, py, "PRE\u00c7OS F\u00cdSICOS BRASIL (CEPEA/ESALQ)")
    py -= 11
    pdf.setFillColor(HexColor(TEXT_DIM))
    pdf.setFont("Helvetica", 6)
    pdf.drawString(M + 10, py, "Pre\u00e7o que o produtor recebe hoje na venda f\u00edsica. Fonte: CEPEA/ESALQ, Noticias Agr\u00edcolas.")
    py -= 14

    # Table header
    cols = [M + 10, M + 120, M + 230, M + 320, M + 420, M + 540]
    hdrs = ["Produto", "Pre\u00e7o", "Unidade", "Varia\u00e7\u00e3o", "Fonte", "Data"]
    pdf.setFont("Helvetica-Bold", 7)
    pdf.setFillColor(HexColor(TEXT_MUT))
    for j, h in enumerate(hdrs):
        pdf.drawString(cols[j], py, h)
    py -= 3
    pdf.setStrokeColor(HexColor(BORDER))
    pdf.setLineWidth(0.3)
    pdf.line(M + 10, py, PAGE_W - M - 10, py)
    py -= 10

    NM_PROD_PT = {
        "soja": "Soja", "milho": "Milho", "cafe": "Caf\u00e9 Ar\u00e1bica",
        "boi": "Boi Gordo", "algodao": "Algod\u00e3o (c/pluma)",
        "trigo": "Trigo", "acucar": "A\u00e7\u00facar",
        "etanol_h": "Etanol Hidratado", "etanol_a": "Etanol Anidro",
    }

    for key, d in br_items.items():
        if py < (y - 185):
            break
        price = d.get("price", "")
        label = d.get("label", "")
        unit = d.get("price_unit", "")
        trend = d.get("trend", "")
        src = d.get("source", "")
        per = d.get("period", "")

        sym_base = key.replace("_BR", "").lower()
        nm_pt = NM_PROD_PT.get(sym_base, "")
        if not nm_pt:
            nm_pt = NM_PROD.get(sym_base.upper(), label[:20]) if "NM_PROD" in dir() else label[:20]

        # Badge
        origin_badge(pdf, cols[0], py, is_brazil=True)
        pdf.setFillColor(HexColor(TEXT))
        pdf.setFont("Helvetica", 7.5)
        pdf.drawString(cols[0] + 50, py, nm_pt[:22])

        pdf.setFillColor(HexColor(TEXT))
        pdf.setFont("Helvetica-Bold", 7.5)
        pdf.drawString(cols[1], py, str(price))

        pdf.setFont("Helvetica", 6.5)
        pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.drawString(cols[2], py, str(unit)[:18])

        trend_str = str(trend).strip()
        if trend_str and trend_str not in ("\u2014", "N/A", ""):
            tc = GREEN if trend_str.startswith("+") else (RED if trend_str.startswith("-") else TEXT_MUT)
            pdf.setFillColor(HexColor(tc))
            pdf.setFont("Helvetica-Bold", 7)
            pdf.drawString(cols[3], py, trend_str)
        else:
            pdf.setFillColor(HexColor(TEXT_DIM))
            pdf.setFont("Helvetica", 6.5)
            pdf.drawString(cols[3], py, "s/var")

        pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.setFont("Helvetica", 6)
        pdf.drawString(cols[4], py, str(src)[:28])
        pdf.drawString(cols[5], py, str(per)[:12])
        py -= 11

    y -= 210

    # === MIDDLE: International Prices (Chicago, Argentina) ===
    if intl_items:
        n_intl = min(len(intl_items), 8)
        box_h = 30 + n_intl * 11
        panel(pdf, M, y - box_h, PAGE_W - 2 * M, box_h)
        py = y - 14
        pdf.setFillColor(HexColor(CYAN))
        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(M + 10, py, "PRE\u00c7OS INTERNACIONAIS (Chicago / Argentina)")
        py -= 11
        pdf.setFillColor(HexColor(TEXT_DIM))
        pdf.setFont("Helvetica", 6)
        pdf.drawString(M + 10, py, "Pre\u00e7os FOB e futuros nas principais origens concorrentes do Brasil.")
        py -= 14

        pdf.setFont("Helvetica-Bold", 7)
        pdf.setFillColor(HexColor(TEXT_MUT))
        for j, h in enumerate(hdrs):
            pdf.drawString(cols[j], py, h)
        py -= 3
        pdf.setStrokeColor(HexColor(BORDER))
        pdf.setLineWidth(0.3)
        pdf.line(M + 10, py, PAGE_W - M - 10, py)
        py -= 10

        count = 0
        for key, d in intl_items.items():
            if count >= 8 or py < (y - box_h + 5):
                break
            price = d.get("price", "")
            label = d.get("label", "")
            unit = d.get("price_unit", "")
            trend = d.get("trend", "")
            src = d.get("source", "")
            per = d.get("period", "")

            is_ar = "_AR" in key
            origin_badge(pdf, cols[0], py, is_brazil=False)

            sym_base = key.replace("_US", "").replace("_AR", "").replace("_INTL", "").lower()
            nm_pt = NM_PROD_PT.get(sym_base, label[:20])

            pdf.setFillColor(HexColor(TEXT))
            pdf.setFont("Helvetica", 7.5)
            origin_tag = " (ARG)" if is_ar else ""
            pdf.drawString(cols[0] + 50, py, (nm_pt + origin_tag)[:22])

            pdf.setFillColor(HexColor(TEXT))
            pdf.setFont("Helvetica-Bold", 7.5)
            pdf.drawString(cols[1], py, str(price))

            pdf.setFont("Helvetica", 6.5)
            pdf.setFillColor(HexColor(TEXT_DIM))
            pdf.drawString(cols[2], py, str(unit)[:18])

            trend_str = str(trend).strip()
            if trend_str and trend_str not in ("\u2014", "N/A", ""):
                tc = GREEN if trend_str.startswith("+") else (RED if trend_str.startswith("-") else TEXT_MUT)
                pdf.setFillColor(HexColor(tc))
                pdf.setFont("Helvetica-Bold", 7)
                pdf.drawString(cols[3], py, trend_str)
            else:
                pdf.setFillColor(HexColor(TEXT_DIM))
                pdf.setFont("Helvetica", 6.5)
                pdf.drawString(cols[3], py, "\u2014")

            pdf.setFillColor(HexColor(TEXT_DIM))
            pdf.setFont("Helvetica", 6)
            pdf.drawString(cols[4], py, str(src)[:28])
            pdf.drawString(cols[5], py, str(per)[:12])
            py -= 11
            count += 1

        y -= (box_h + 15)

    # === BOTTOM: Explanation ===
    panel(pdf, M, y - 70, PAGE_W - 2 * M, 68)
    py = y - 14
    pdf.setFillColor(HexColor(CYAN))
    pdf.setFont("Helvetica-Bold", 8)
    pdf.drawString(M + 10, py, "COMO USAR ESTA P\u00c1GINA")
    py -= 12

    explanations = [
        "Pre\u00e7o f\u00edsico = o que o produtor recebe ao vender o produto hoje, na m\u00e3o. Diferente do futuro (Chicago).",
        "Se o f\u00edsico sobe mais que o futuro, a BASE est\u00e1 forte \u2014 bom momento para vender no f\u00edsico.",
        "Se o f\u00edsico cai enquanto Chicago sobe, a BASE est\u00e1 fraca \u2014 pode ser melhor travar no futuro.",
        "Compare BR vs Argentina: se a Argentina vende mais barato, o comprador chin\u00eas prefere o concorrente.",
    ]
    pdf.setFont("Helvetica", 6.5)
    pdf.setFillColor(HexColor(TEXT_DIM))
    for exp in explanations:
        pdf.drawString(M + 10, py, exp)
        py -= 9
