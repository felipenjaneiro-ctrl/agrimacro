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
        net = cur.get("noncommercial_net", 0)
        comm_net = cur.get("commercial_net", 0)
        oi = cur.get("open_interest", 1)
        prev_net = prev.get("noncommercial_net", net)
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
        tblock(pdf, RIGHT_X + 16, card_y - 24, _basis_txt, 6, TEXT_DIM, RIGHT_W - 40, 8)

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
    tblock(pdf, M + 12, ana_y - 14, ana_text, 6.5, TEXT, ana_w - 24, 8)

    pdf.setFont("Helvetica", 5)
    pdf.setFillColor(TEXT_MUT)
    pdf.drawString(M + 8, ana_y - ana_h + 7, "Fonte: CFTC Commitments of Traders, CEPEA/ESALQ, CME/CBOT, BCB/SGS")

