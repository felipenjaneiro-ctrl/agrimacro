# pg_grain_ratios.py - modulo AgriMacro (coloque em pipeline/)
import json, os
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor

def pg_grain_ratios(pdf, data_dir):
    BG    = HexColor("#0E1A24")
    PANEL = HexColor("#142332")
    GREEN = HexColor("#00C878")
    RED   = HexColor("#DC3C3C")
    GOLD  = HexColor("#DCB432")
    WHITE = HexColor("#FFFFFF")
    GREY  = HexColor("#8899AA")
    BORD  = HexColor("#1E3248")
    json_path = os.path.join(data_dir, "grain_ratios.json")
    if not os.path.exists(json_path):
        print(f"    [pg_grain_ratios] JSON nao encontrado: {json_path}")
        return
    with open(json_path, "r", encoding="utf-8") as f:
        gr = json.load(f)
    scorecards = gr.get("scorecards",    {})
    model_res  = gr.get("model_results", {})
    stu_bt     = gr.get("stu_backtest",  {})
    arbitrage  = gr.get("arbitrage",     {})
    meta       = gr.get("meta",          {})
    updated_at = meta.get("generated_at", gr.get("updated_at", ""))
    c = pdf
    W, H = landscape(A4)
    M    = 14 * mm
    IW   = W - 2 * M
    IH   = H - 2 * M
    H_TOP = IH * 0.24
    H_MID = IH * 0.40
    H_BOT = IH * 0.30
    GAP   = 6
    c.setFillColor(BG)
    c.rect(0, 0, W, H, stroke=0, fill=1)
    def rect(x, y, w, h, fill=PANEL):
        c.setFillColor(fill)
        c.roundRect(x, y, w, h, 3, stroke=0, fill=1)
    def txt(x, y, s, sz=7, col=WHITE, bold=False):
        c.setFont("Helvetica-Bold" if bold else "Helvetica", sz)
        c.setFillColor(col)
        c.drawString(x, y, str(s))
    def txt_c(x, y, w, s, sz=8, col=WHITE, bold=False):
        c.setFont("Helvetica-Bold" if bold else "Helvetica", sz)
        c.setFillColor(col)
        c.drawCentredString(x + w / 2, y, str(s))
    def sig_col(sig):
        s = str(sig).upper()
        if any(k in s for k in ("BUY","ALTA","BULL","LONG")): return GREEN
        if any(k in s for k in ("SELL","BAIXA","BEAR","SHORT")): return RED
        return GOLD
    def fmt(v):
        try:    return f"{float(v):.1f}"
        except: return str(v) if v else "-"
    ty = H - M + 4
    txt(M, ty, "GRAIN RATIOS  -  ANALISE INTEGRADA", 9, GOLD, True)
    if updated_at:
        txt(W-M-120, ty, f"atualizado: {updated_at[:16].replace('T',' ')}", 7, GREY)
    top_y = H - M - 12 - H_TOP
    grains = ["corn","soy","wheat"]
    cgap = 8
    cw_ = (IW - cgap*(len(grains)-1)) / len(grains)
    for i, g in enumerate(grains):
        cx = M + i*(cw_+cgap); cy = top_y
        sc = scorecards.get(g, {})
        sig = sc.get("composite_signal", sc.get("signal","N/A"))
        score = sc.get("composite_score", sc.get("weighted_score", sc.get("score",0)))
        try:    sstr = f"{float(score):+.1f}"
        except: sstr = str(score)
        g_label = g.upper()
        rect(cx, cy, cw_, H_TOP)
        txt_c(cx, cy+H_TOP-18, cw_, g_label,          11, WHITE, True)
        txt_c(cx, cy+H_TOP*0.52, cw_, sig,            14, sig_col(sig), True)
        txt_c(cx, cy+H_TOP*0.28, cw_, f"Score: {sstr}", 9, GREY)
        ey = cy + 10
        for key in ("lasso_r2","lasso_dir_acc","stu_p_value","fob_spread_usd"):
            v = sc.get(key)
            if v is not None:
                txt(cx+6, ey, f"{key.replace('_',' ').title()}: {v}", 6, GREY); ey += 9
    mid_y = top_y - GAP - H_MID
    mc = IW/2 - 4
    col_lh = ["Grain","Horizon","R2","DirAcc"]
    col_lw = [mc*0.22, mc*0.28, mc*0.22, mc*0.28]
    lx = M
    rect(lx, mid_y, mc, H_MID)
    txt(lx+6, mid_y+H_MID-14, "LASSO - Acuracia do Modelo", 8, GOLD, True)
    def lrow(data, ry, hdr=False):
        x0 = lx+4
        for j,(cell,cw2) in enumerate(zip(data,col_lw)):
            cx0 = x0+sum(col_lw[:j])
            if hdr:
                c.setFillColor(BORD); c.rect(cx0-1,ry-3,cw2,11,stroke=0,fill=1)
            c.setFont("Helvetica-Bold" if hdr else "Helvetica",6.5)
            c.setFillColor(GOLD if hdr else WHITE)
            c.drawString(cx0+2, ry, str(cell)[:12])
    ry = mid_y+H_MID-28
    lrow(col_lh, ry, hdr=True); ry -= 13
    if isinstance(model_res, dict):
        for gk, horizons in model_res.items():
            if isinstance(horizons, dict):
                for hz, m in horizons.items():
                    if not isinstance(m, dict): continue
                    r2 = m.get("r2_out_of_sample", m.get("r2", ""))
                    da = m.get("directional_accuracy", m.get("dir_acc", ""))
                    lrow([gk[:6], str(hz),
                          f"{float(r2):.1f}%" if r2 != "" else "-",
                          f"{float(da):.1f}%" if da != "" else "-"], ry)
                    ry -= 11
                    if ry < mid_y+10: break
            if ry < mid_y+10: break
    sx = M+mc+8
    rect(sx, mid_y, mc, H_MID)
    txt(sx+6, mid_y+H_MID-14, "STU BACKTEST - Buckets", 8, GOLD, True)
    col_sh=["Bucket","N","Win%","AvgRet"]
    col_sw=[mc*0.30,mc*0.15,mc*0.25,mc*0.30]
    def srow(data,ry,hdr=False):
        x0=sx+4
        for j,(cell,cw2) in enumerate(zip(data,col_sw)):
            cx0=x0+sum(col_sw[:j])
            if hdr:
                c.setFillColor(BORD); c.rect(cx0-1,ry-3,cw2,11,stroke=0,fill=1)
            c.setFont("Helvetica-Bold" if hdr else "Helvetica",6.5)
            c.setFillColor(GOLD if hdr else WHITE)
            c.drawString(cx0+2,ry,str(cell)[:14])
    ry=mid_y+H_MID-28
    srow(col_sh,ry,hdr=True); ry-=13
    if isinstance(stu_bt,dict):
        for gk, buckets in stu_bt.items():
            if isinstance(buckets, dict):
                for bk, bd in buckets.items():
                    if not isinstance(bd, dict): continue
                    n = bd.get("n", bd.get("count", ""))
                    wp = bd.get("pct_positive", bd.get("win_pct", ""))
                    ar = bd.get("avg_fwd12m", bd.get("avg_ret", ""))
                    srow([f"{gk[:4]}:{bk[:8]}", str(n),
                          f"{float(wp):.0f}%" if wp != "" else "-",
                          f"{float(ar):+.1f}%" if ar != "" else "-"], ry)
                    ry -= 11
                    if ry < mid_y+10: break
            if ry < mid_y+10: break
    bot_y=mid_y-GAP-H_BOT
    rect(M,bot_y,IW,H_BOT)
    txt(M+6,bot_y+H_BOT-14,"ARBITRAGEM CIF QINGDAO - US / BR / ARG  (USD/ton)",8,GOLD,True)
    arb_h=["Commodity","US FOB","BR FOB","ARG FOB","US CIF","BR CIF","ARG CIF","Origin"]
    nc=len(arb_h)
    acw=IW/nc-2
    def arow(data,ry,hdr=False):
        x0=M+4
        for j,cell in enumerate(data):
            cx0=x0+j*(acw+2)
            if hdr:
                c.setFillColor(BORD); c.rect(cx0-1,ry-3,acw,11,stroke=0,fill=1)
            col=sig_col(str(cell)) if (not hdr and j==nc-1) else (GOLD if hdr else WHITE)
            c.setFont("Helvetica-Bold" if hdr else "Helvetica",6.5)
            c.setFillColor(col)
            c.drawString(cx0+2,ry,str(cell)[:14])
    ry=bot_y+H_BOT-28
    arow(arb_h,ry,hdr=True); ry-=13
    if isinstance(arbitrage,dict):
        cif = arbitrage.get("spread_delivered_china", {})
        fob_g = cif.get("fob_gulf", {})
        fob_p = cif.get("fob_paranagua", {})
        fob_r = cif.get("fob_rosario", {})
        cif_q = cif.get("cif_qingdao", {})
        for crop in ["soy","corn","wheat"]:
            us_fob = fob_g.get(crop); br_fob = fob_p.get(crop); arg_fob = fob_r.get(crop)
            us_cif = cif_q.get(f"{crop}_us"); br_cif = cif_q.get(f"{crop}_br"); arg_cif = cif_q.get(f"{crop}_arg")
            origin = "-"
            sp = cif.get("spreads", {})
            us_br = sp.get(f"{crop}_us_vs_br")
            if us_br is not None and us_br > 0: origin = "BR"
            elif us_br is not None and us_br < 0: origin = "US"
            arow([crop.upper()[:10],
                  fmt(us_fob), fmt(br_fob), fmt(arg_fob),
                  fmt(us_cif), fmt(br_cif), fmt(arg_cif), origin], ry)
            ry -= 11
            if ry < bot_y+8: break
    print("    pg_grain_ratios() OK")
