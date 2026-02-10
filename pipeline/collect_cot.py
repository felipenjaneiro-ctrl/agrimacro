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
YEARS = list(range(YEAR-2, YEAR+1))  # 3 anos de historico

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

def download_zip(url, label):
    print(f"  Baixando {label}...")
    try:
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        print(f"  OK ({len(r.content)/1024/1024:.1f} MB)")
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        csv_name = zf.namelist()[0]
        df = pd.read_csv(io.BytesIO(zf.read(csv_name)), low_memory=False)
        with open(RAW_COT / csv_name, "wb") as f:
            f.write(zf.read(csv_name))
        print(f"  {len(df)} linhas | Salvo: {csv_name}")
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
            results[tk] = {"ticker":tk,"name":info["name"],"report_type":"disaggregated",
                "latest":hist[-1],"history":hist,"weeks":len(hist)}
            mn = hist[-1].get("managed_money_net")
            pn = hist[-1].get("producer_net")
            if mn is not None:
                print(f"    {tk:4s} {info['name']:20s} {len(hist):3d}w | MM_Net:{mn:>10,.0f} | Prod_Net:{pn:>10,.0f}")
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
        part = download_zip(f"https://www.cftc.gov/files/dea/history/deacot{y}.zip", f"Legacy {y}")
        if part is not None and len(part) > 0: leg_parts.append(part)
    leg_df = pd.concat(leg_parts, ignore_index=True) if leg_parts else None
    print()
    print("[2/4] Disaggregated...")
    dis_parts = []
    for y in YEARS:
        part = download_zip(f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{y}.zip", f"Disagg {y}")
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
    with open(OUTPUT,"w",encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False, default=str)
    print(f"\n[OK] {OUTPUT}")
    print(f"     {len(out['commodities'])} commodities")
    print(f"     Legacy: {len(leg)} | Disagg: {len(dis)}")

def collect_cot_data():
    print("="*60)
    print("  AgriMacro v3.1 - COT Collector (CFTC CSV)")
    print("="*60)
    print()
    print("[1/4] Legacy...")
    leg_parts = []
    for y in YEARS:
        part = download_zip(f"https://www.cftc.gov/files/dea/history/deacot{y}.zip", f"Legacy {y}")
        if part is not None and len(part) > 0: leg_parts.append(part)
    leg_df = pd.concat(leg_parts, ignore_index=True) if leg_parts else None
    print()
    print("[2/4] Disaggregated...")
    dis_parts = []
    for y in YEARS:
        part = download_zip(f"https://www.cftc.gov/files/dea/history/fut_disagg_txt_{y}.zip", f"Disagg {y}")
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
    with open(OUTPUT,"w",encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False, default=str)
    print(f"[OK] {len(out['commodities'])} commodities")
    return out


if __name__ == "__main__":
    main()



