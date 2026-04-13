import json, sys, os, io, zipfile
from datetime import datetime
from pathlib import Path
try:
    import requests
except ImportError:
    os.system(f"{sys.executable} -m pip install requests --quiet")
    import requests
try:
    import pandas as pd
except ImportError:
    os.system(f"{sys.executable} -m pip install pandas --quiet")
    import pandas as pd

BASE = Path(r"C:\Users\felip\OneDrive") / "Área de Trabalho" / "agrimacro"
DATA_DIR = BASE / "agrimacro-dash" / "public" / "data" / "processed"
DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_COT = BASE / "agrimacro-dash" / "public" / "data" / "raw" / "cot"
RAW_COT.mkdir(parents=True, exist_ok=True)
OUTPUT = DATA_DIR / "cot.json"
YEAR = datetime.now().year
YEARS = list(range(YEAR-3, YEAR+1))  # 4 anos para garantir 156+ semanas

CFTC = {
    "ZC": {"name":"Milho","code":"002602","match":"CORN"},
    "ZS": {"name":"Soja","code":"005602","match":"SOYBEANS"},
    "ZW": {"name":"Trigo CBOT","code":"001602","match":"WHEAT-SRW"},
    "KE": {"name":"Trigo KC","code":"001612","match":"WHEAT-HRW"},
    "ZM": {"name":"Farelo Soja","code":"026603","match":"SOYBEAN MEAL"},
    "ZL": {"name":"Oleo Soja","code":"007601","match":"SOYBEAN OIL"},
    "LE": {"name":"Boi Gordo","code":"057642","match":"LIVE CATTLE"},
    "GF": {"name":"Feeder Cattle","code":"061641","match":"FEEDER CATTLE"},
    "HE": {"name":"Suino Magro","code":"054642","match":"LEAN HOGS"},
    "KC": {"name":"Cafe Arabica","code":"083731","match":"COFFEE"},
    "CC": {"name":"Cacau","code":"073732","match":"COCOA"},
    "SB": {"name":"Acucar #11","code":"080732","match":"SUGAR NO. 11"},
    "CT": {"name":"Algodao #2","code":"033661","match":"COTTON NO. 2"},
    "OJ": {"name":"Suco Laranja","code":"040701","match":"ORANGE JUICE"},
    "CL": {"name":"Petroleo WTI","code":"067651","match":"CRUDE OIL"},
    "NG": {"name":"Gas Natural","code":"023651","match":"NAT GAS"},
    "RB": {"name":"Gasolina","code":"111659","match":"RBOB"},
    "GC": {"name":"Ouro","code":"088691","match":"GOLD"},
}

def download_zip(url, label, year=None):
    print(f"  Baixando {label}...")
    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        print(f"  OK ({len(r.content)/1024/1024:.1f} MB)")
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        csv_name = zf.namelist()[0]
        raw_bytes = zf.read(csv_name)
        df = pd.read_csv(io.BytesIO(raw_bytes), low_memory=False)
        # Salva com sufixo do ano para nao sobrescrever
        base, ext = csv_name.rsplit(".", 1) if "." in csv_name else (csv_name, "txt")
        save_name = f"{base}_{year}.{ext}" if year else csv_name
        with open(RAW_COT / save_name, "wb") as f:
            f.write(raw_bytes)
        print(f"  {len(df)} linhas | Salvo: {save_name}")
        return df
    except Exception as e:
        print(f"  [ERRO] {e}")
        return None

def find_rows(df, info):
    code_cols = [c for c in df.columns if "cftc" in c.lower() and "code" in c.lower() and "market" in c.lower()]
    name_cols = [c for c in df.columns if "market" in c.lower() and "name" in c.lower()]
    rows = pd.DataFrame()
    for col in code_cols:
        df[col] = df[col].astype(str).str.strip()
        m = df[df[col] == info["code"]]
        if len(m) > 0: rows = m; break
    if len(rows) == 0:
        for col in name_cols:
            m = df[df[col].astype(str).str.contains(info["match"], case=False, na=False)]
            if len(m) > 0: rows = m; break
    return rows

def find_date_col(df):
    for name in ["As of Date in Form YYYY-MM-DD", "Report_Date_as_YYYY-MM-DD", "As_of_Date_In_Form_YYMMDD"]:
        if name in df.columns: return name
    for c in df.columns:
        if "date" in c.lower():
            try:
                pd.to_datetime(df[c].head(5)); return c
            except: pass
    return None

def find_cols(df, patterns):
    for p in patterns:
        cols = [c for c in df.columns if all(k in c.lower() for k in p["must"]) and not any(k in c.lower() for k in p.get("not",[]))]
        if cols: return cols[0]
    return None

def calc_cot_index(series, window):
    """COT Index = (atual - min(janela)) / (max(janela) - min(janela)) * 100"""
    if not series:
        return 50.0
    if len(series) < window:
        window = len(series)
    subset = series[-window:]
    mn, mx = min(subset), max(subset)
    if mx == mn:
        return 50.0
    return round((series[-1] - mn) / (mx - mn) * 100, 1)


def calc_delta_signals(history, window=156):
    """
    Calcula sinais de revers\u00e3o baseados em padr\u00f5es COT.
    history: lista de dicts com date, managed_money_net, open_interest
    Retorna dict com sinais e probabilidades.
    """
    if len(history) < 10:
        return {}

    recent = history[-8:]

    # Deltas das \u00faltimas 8 semanas
    deltas = []
    for i in range(1, len(recent)):
        prev_mm = recent[i-1].get('managed_money_net') or 0
        curr_mm = recent[i].get('managed_money_net') or 0
        deltas.append(curr_mm - prev_mm)

    if not deltas:
        return {}

    current_delta = deltas[-1]
    prev_delta = deltas[-2] if len(deltas) >= 2 else 0

    # COT Index (min-max normalization over full window)
    mm_series = [h.get('managed_money_net') or 0 for h in history]
    cot_idx = calc_cot_index(mm_series, window)

    mm_net = recent[-1].get('managed_money_net') or 0
    oi = recent[-1].get('open_interest') or 0

    # OI trend
    oi_series = [r.get('open_interest') or 0 for r in recent]
    oi_trend = (oi_series[-1] - oi_series[-4]) / oi_series[-4] * 100 \
               if len(oi_series) >= 4 and oi_series[-4] > 0 else 0

    # Contar semanas consecutivas com delta negativo/positivo
    neg_streak = 0
    pos_streak = 0
    for d in reversed(deltas):
        if d < 0:
            if pos_streak > 0:
                break
            neg_streak += 1
        elif d > 0:
            if neg_streak > 0:
                break
            pos_streak += 1
        else:
            break

    signals = []
    score_bear = 0
    score_bull = 0

    # PADR\u00c3O 1: Topo Especulativo
    if cot_idx >= 85 and current_delta < 0:
        prob = min(55 + int(cot_idx - 85) * 2 + neg_streak * 5, 82)
        signals.append({
            'type': 'TOP_SPECULATIVE',
            'label': 'Topo Especulativo',
            'direction': 'BEARISH',
            'probability': prob,
            'color': '#DC3C3C',
            'icon': '\U0001f534',
            'description': f'COT {cot_idx:.0f}/100 + fundos saindo. '
                          f'{neg_streak}sem negativas consecutivas.'
        })
        score_bear += prob

    # PADR\u00c3O 2: Acelera\u00e7\u00e3o de Sa\u00edda
    if current_delta < -20000 and prev_delta < -20000 and neg_streak >= 2:
        prob = min(60 + neg_streak * 5, 80)
        signals.append({
            'type': 'ACCELERATION_EXIT',
            'label': 'Acelera\u00e7\u00e3o de Sa\u00edda',
            'direction': 'BEARISH',
            'probability': prob,
            'color': '#DC3C3C',
            'icon': '\u26a0\ufe0f',
            'description': f'Delta {int(current_delta):+,} por '
                          f'{neg_streak} semanas. Liquida\u00e7\u00e3o acelerada.'
        })
        score_bear += prob * 0.8

    # PADR\u00c3O 3: Fundo Especulativo
    if cot_idx <= 15 and current_delta > 0:
        prob = min(55 + int(15 - cot_idx) * 2 + pos_streak * 5, 80)
        signals.append({
            'type': 'BOTTOM_SPECULATIVE',
            'label': 'Fundo Especulativo',
            'direction': 'BULLISH',
            'probability': prob,
            'color': '#00C878',
            'icon': '\U0001f7e2',
            'description': f'COT {cot_idx:.0f}/100 + fundos comprando. '
                          f'{pos_streak}sem positivas consecutivas.'
        })
        score_bull += prob

    # PADR\u00c3O 4: Short Covering Iminente
    if cot_idx <= 25 and oi_trend < -3 and current_delta > 0:
        prob = min(60 + pos_streak * 8, 78)
        signals.append({
            'type': 'SHORT_COVERING',
            'label': 'Short Covering Iminente',
            'direction': 'BULLISH',
            'probability': prob,
            'color': '#00C878',
            'icon': '\U0001f7e2',
            'description': f'OI caindo {oi_trend:.1f}% + fundos '
                          f'encerrando shorts. Spike poss\u00edvel.'
        })
        score_bull += prob * 0.9

    # PADR\u00c3O 5: Diverg\u00eancia COT-Pre\u00e7o (bearish)
    if cot_idx >= 70 and current_delta < -15000:
        signals.append({
            'type': 'DIVERGENCE_BEAR',
            'label': 'Diverg\u00eancia Bearish',
            'direction': 'BEARISH',
            'probability': 62,
            'color': '#DCB432',
            'icon': '\u26a0\ufe0f',
            'description': 'Fundos comprados mas saindo. '
                          'Diverg\u00eancia COT-pre\u00e7o ativa.'
        })
        score_bear += 50

    # PADR\u00c3O 6: Momentum Comprador
    if current_delta > 15000 and prev_delta > 10000 and cot_idx < 80:
        prob = min(55 + pos_streak * 7, 75)
        signals.append({
            'type': 'BULL_MOMENTUM',
            'label': 'Momentum Comprador',
            'direction': 'BULLISH',
            'probability': prob,
            'color': '#00C878',
            'icon': '\U0001f4c8',
            'description': f'Fundos comprando agressivamente. '
                          f'Delta {int(current_delta):+,}.'
        })
        score_bull += prob * 0.7

    # Sem sinal claro
    if not signals:
        signals.append({
            'type': 'NEUTRAL',
            'label': 'Sem Sinal',
            'direction': 'NEUTRAL',
            'probability': 50,
            'color': '#64748b',
            'icon': '\u2b1c',
            'description': 'Posicionamento sem padr\u00e3o de revers\u00e3o claro.'
        })

    # Score final de revers\u00e3o
    dominant = 'BEARISH' if score_bear > score_bull else \
               'BULLISH' if score_bull > score_bear else 'NEUTRAL'
    reversal_score = int(max(score_bear, score_bull) /
                        max(score_bear + score_bull, 1) * 100)

    return {
        'signals': signals,
        'dominant_direction': dominant,
        'reversal_score': reversal_score,
        'cot_index': cot_idx,
        'neg_streak': neg_streak,
        'pos_streak': pos_streak,
        'current_delta': int(current_delta),
        'prev_delta': int(prev_delta),
        'oi_trend_pct': round(oi_trend, 2),
        'deltas_8w': [int(d) for d in deltas]
    }


def process_legacy(df):
    if df is None: return {}
    print("\n  Processando Legacy...")
    dc = find_date_col(df)
    if not dc: print("  [ERRO] Sem coluna de data"); return {}
    df["_d"] = pd.to_datetime(df[dc], errors="coerce")
    df = df.dropna(subset=["_d"])
    results = {}
    for tk, info in CFTC.items():
        rows = find_rows(df, info)
        if len(rows) == 0: continue
        rows = rows.sort_values("_d")
        nc_l = find_cols(rows, [{"must":["noncomm","long","all"],"not":["spread","change","pct","old","other"]},
                                 {"must":["noncomm","long"],"not":["spread","change"]}])
        nc_s = find_cols(rows, [{"must":["noncomm","short","all"],"not":["spread","change","pct","old","other"]},
                                 {"must":["noncomm","short"],"not":["spread","change"]}])
        c_l = find_cols(rows, [{"must":["comm","long","all"],"not":["noncomm","non_comm","spread","change","pct","old","other"]},
                                {"must":["comm","long"],"not":["noncomm","non_comm","non-comm","spread","change"]}])
        c_s = find_cols(rows, [{"must":["comm","short","all"],"not":["noncomm","non_comm","spread","change","pct","old","other"]},
                                {"must":["comm","short"],"not":["noncomm","non_comm","non-comm","spread","change"]}])
        oi_c = find_cols(rows, [{"must":["open_interest","all"],"not":["change","pct","old"]},
                                 {"must":["open","interest"],"not":["change"]}])
        hist = []
        for _, row in rows.tail(156).iterrows():
            try:
                ncl = float(row[nc_l]) if nc_l else None
                ncs = float(row[nc_s]) if nc_s else None
                cl = float(row[c_l]) if c_l else None
                cs = float(row[c_s]) if c_s else None
                oi = float(row[oi_c]) if oi_c else None
                hist.append({"date": row["_d"].strftime("%Y-%m-%d"),
                    "noncomm_long":ncl,"noncomm_short":ncs,
                    "noncomm_net":(ncl-ncs) if ncl is not None and ncs is not None else None,
                    "comm_long":cl,"comm_short":cs,
                    "comm_net":(cl-cs) if cl is not None and cs is not None else None,
                    "open_interest":oi})
            except: pass
        if hist:
            results[tk] = {"ticker":tk,"name":info["name"],"report_type":"legacy",
                "latest":hist[-1],"history":hist,"weeks":len(hist)}
            cn = hist[-1].get("comm_net")
            nn = hist[-1].get("noncomm_net")
            if cn is not None:
                print(f"    {tk:4s} {info['name']:20s} {len(hist):3d}w | CommNet:{cn:>10,.0f} | NonCommNet:{nn:>10,.0f}")
            else:
                print(f"    {tk}: parcial")
    return results

def process_disagg(df):
    if df is None: return {}
    print("\n  Processando Disaggregated...")
    dc = find_date_col(df)
    if not dc: print("  [ERRO] Sem coluna de data"); return {}
    df["_d"] = pd.to_datetime(df[dc], errors="coerce")
    df = df.dropna(subset=["_d"])
    results = {}
    for tk, info in CFTC.items():
        rows = find_rows(df, info)
        if len(rows) == 0: continue
        rows = rows.sort_values("_d")
        pm_l = find_cols(rows, [{"must":["prod","long","all"],"not":["change","pct","old"]},{"must":["prod","long"],"not":["change","spread"]}])
        pm_s = find_cols(rows, [{"must":["prod","short","all"],"not":["change","pct","old"]},{"must":["prod","short"],"not":["change","spread"]}])
        sw_l = find_cols(rows, [{"must":["swap","long","all"],"not":["change","pct","spread","old"]},{"must":["swap","long"],"not":["change","spread"]}])
        sw_s = find_cols(rows, [{"must":["swap","short","all"],"not":["change","pct","spread","old"]},{"must":["swap","short"],"not":["change","spread"]}])
        mm_l = find_cols(rows, [{"must":["money","long","all"],"not":["change","pct","spread","old"]},{"must":["money","long"],"not":["change","spread"]}])
        mm_s = find_cols(rows, [{"must":["money","short","all"],"not":["change","pct","spread","old"]},{"must":["money","short"],"not":["change","spread"]}])
        oi_c = find_cols(rows, [{"must":["open_interest","all"],"not":["change","pct","old"]},{"must":["open","interest"],"not":["change"]}])
        hist = []
        for _, row in rows.tail(156).iterrows():
            try:
                pml=float(row[pm_l]) if pm_l else None; pms=float(row[pm_s]) if pm_s else None
                swl=float(row[sw_l]) if sw_l else None; sws=float(row[sw_s]) if sw_s else None
                mml=float(row[mm_l]) if mm_l else None; mms=float(row[mm_s]) if mm_s else None
                oi=float(row[oi_c]) if oi_c else None
                e = {"date":row["_d"].strftime("%Y-%m-%d"),
                    "producer_long":pml,"producer_short":pms,
                    "producer_net":(pml-pms) if pml is not None and pms is not None else None,
                    "swap_long":swl,"swap_short":sws,
                    "swap_net":(swl-sws) if swl is not None and sws is not None else None,
                    "managed_money_long":mml,"managed_money_short":mms,
                    "managed_money_net":(mml-mms) if mml is not None and mms is not None else None,
                    "open_interest":oi}
                if oi and oi > 0:
                    if mml is not None: e["mm_long_pct"] = round(mml/oi*100,1)
                    if mms is not None: e["mm_short_pct"] = round(mms/oi*100,1)
                hist.append(e)
            except: pass
        if hist:
            delta_analysis = calc_delta_signals(hist)
            # Multi-window COT Index (managed money net)
            mm_series = [h.get("managed_money_net") or 0 for h in hist]
            cot_156 = calc_cot_index(mm_series, 156)
            cot_52 = calc_cot_index(mm_series, 52)
            cot_26 = calc_cot_index(mm_series, 26)
            results[tk] = {"ticker":tk,"name":info["name"],"report_type":"disaggregated",
                "latest":hist[-1],"history":hist,"weeks":len(hist),
                "cot_index": cot_156, "cot_index_52w": cot_52, "cot_index_26w": cot_26,
                "cot_window": 156,
                "delta_analysis": delta_analysis}
            mn = hist[-1].get("managed_money_net")
            pn = hist[-1].get("producer_net")
            if mn is not None:
                da_dir = delta_analysis.get("dominant_direction", "?")
                da_score = delta_analysis.get("reversal_score", 0)
                da_sig = delta_analysis.get("signals", [{}])[0].get("label", "?")
                print(f"    {tk:4s} {info['name']:20s} {len(hist):3d}w | MM_Net:{mn:>10,.0f} | Prod_Net:{pn:>10,.0f} | {da_dir} {da_score}% {da_sig}")
            else:
                print(f"    {tk}: parcial")
    return results

def main():
    print("="*60)
    print("  AgriMacro v3.1 - COT Collector (CFTC CSV)")
    print("="*60)
    print()
    print("[1/4] Legacy...")
    leg_parts = []
    for y in YEARS:
        part = download_zip(f"https://www.cftc.gov/files/dea/history/deacot{y}.zip", f"Legacy {y}", year=y)
        if part is not None and len(part) > 0: leg_parts.append(part)
    leg_df = pd.concat(leg_parts, ignore_index=True) if leg_parts else None
    print()
    print("[2/4] Disaggregated...")
    dis_parts = []
    for y in YEARS:
        part = download_zip(f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{y}.zip", f"Disagg {y}", year=y)
        if part is not None and len(part) > 0: dis_parts.append(part)
    dis_df = pd.concat(dis_parts, ignore_index=True) if dis_parts else None
    print()
    print("[3/4] Processando...")
    leg = process_legacy(leg_df)
    dis = process_disagg(dis_df)
    print()
    print("[4/4] Salvando cot.json...")
    out = {"generated_at":datetime.now().isoformat(),"source":"CFTC CSV","year":YEAR,"commodities":{}}
    for tk in sorted(set(list(leg.keys())+list(dis.keys()))):
        nm = CFTC.get(tk,{}).get("name",tk)
        out["commodities"][tk] = {"ticker":tk,"name":nm}
        if tk in leg: out["commodities"][tk]["legacy"] = leg[tk]
        if tk in dis: out["commodities"][tk]["disaggregated"] = dis[tk]
    # Data quality metadata
    all_weeks = [v.get("disaggregated",{}).get("weeks",0) or v.get("legacy",{}).get("weeks",0) for v in out["commodities"].values()]
    max_weeks = max(all_weeks) if all_weeks else 0
    target_weeks = 156
    used_weeks = min(max_weeks, target_weeks)
    out["data_quality"] = {
        "weeks_available": max_weeks,
        "weeks_used_for_index": used_weeks,
        "coverage_pct": round(used_weeks / target_weeks * 100, 1) if target_weeks > 0 else 0,
        "warning": "COT Index pode divergir de Barchart se < 156 semanas" if max_weeks < target_weeks else None
    }
    if max_weeks < target_weeks:
        print(f"  [AVISO] Apenas {max_weeks} semanas disponiveis (ideal: {target_weeks}). COT Index usa janela reduzida.")
    with open(OUTPUT,"w",encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False, default=str)
    print(f"\n[OK] {OUTPUT}")
    print(f"     {len(out['commodities'])} commodities")
    print(f"     Legacy: {len(leg)} | Disagg: {len(dis)}")
    print(f"     Semanas: {max_weeks} | Cobertura: {out['data_quality']['coverage_pct']}%")

def collect_cot_data():
    print("="*60)
    print("  AgriMacro v3.1 - COT Collector (CFTC CSV)")
    print("="*60)
    print()
    print("[1/4] Legacy...")
    leg_parts = []
    for y in YEARS:
        part = download_zip(f"https://www.cftc.gov/files/dea/history/deacot{y}.zip", f"Legacy {y}", year=y)
        if part is not None and len(part) > 0: leg_parts.append(part)
    leg_df = pd.concat(leg_parts, ignore_index=True) if leg_parts else None
    print()
    print("[2/4] Disaggregated...")
    dis_parts = []
    for y in YEARS:
        part = download_zip(f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{y}.zip", f"Disagg {y}", year=y)
        if part is not None and len(part) > 0: dis_parts.append(part)
    dis_df = pd.concat(dis_parts, ignore_index=True) if dis_parts else None
    print()
    print("[3/4] Processando...")
    leg = process_legacy(leg_df)
    dis = process_disagg(dis_df)
    print()
    print("[4/4] Montando resultado...")
    out = {"generated_at":datetime.now().isoformat(),"source":"CFTC CSV","year":YEAR,"commodities":{}}
    for tk in sorted(set(list(leg.keys())+list(dis.keys()))):
        nm = CFTC.get(tk,{}).get("name",tk)
        out["commodities"][tk] = {"ticker":tk,"name":nm}
        if tk in leg: out["commodities"][tk]["legacy"] = leg[tk]
        if tk in dis: out["commodities"][tk]["disaggregated"] = dis[tk]
    # Data quality metadata
    all_weeks = [v.get("disaggregated",{}).get("weeks",0) or v.get("legacy",{}).get("weeks",0) for v in out["commodities"].values()]
    max_weeks = max(all_weeks) if all_weeks else 0
    target_weeks = 156
    used_weeks = min(max_weeks, target_weeks)
    out["data_quality"] = {
        "weeks_available": max_weeks,
        "weeks_used_for_index": used_weeks,
        "coverage_pct": round(used_weeks / target_weeks * 100, 1) if target_weeks > 0 else 0,
        "warning": "COT Index pode divergir de Barchart se < 156 semanas" if max_weeks < target_weeks else None
    }
    if max_weeks < target_weeks:
        print(f"  [AVISO] Apenas {max_weeks} semanas disponiveis (ideal: {target_weeks}). COT Index usa janela reduzida.")
    with open(OUTPUT,"w",encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False, default=str)
    print(f"[OK] {len(out['commodities'])} commodities | Semanas: {max_weeks} | Cobertura: {out['data_quality']['coverage_pct']}%")
    return out


if __name__ == "__main__":
    main()



