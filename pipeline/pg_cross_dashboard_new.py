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

    # ── Signal translations ──────────────────────────────────────────────
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

    # ── Card 1: Basis Temporal ───────────────────────────────────────────
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

    # ── Card 2: COT Momentum ────────────────────────────────────────────
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

    # ── Card 3: Crush Margin ────────────────────────────────────────────
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

    # ── Card 4: Margem Produtor ──────────────────────────────────────────
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

    # ── Card 5: Diferencial Juros ────────────────────────────────────────
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

    # ── Card 6: Ritmo Exportacao ─────────────────────────────────────────
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

    # ── Card 7: Argentina ────────────────────────────────────────────────
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

    # ── Card 8: Indice Seca ──────────────────────────────────────────────
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

    # ── Card 9: Frete Spread ─────────────────────────────────────────────
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
