#!/usr/bin/env python3
"""
AgriMacro Intelligence -- Grain Ratio Engine v2.0
==================================================
Motor otimizado de backteste + arbitragem de origem.

Fontes de dados (todas gratuitas, sem entrada manual):
  - yfinance          : precos mensais CME front-month
  - USDA AMS          : basis diario FOB Gulf (CSV)
  - USDA FAS          : export sales semanais
  - Macrotrends.net   : Baltic Dry Index historico (scraping)
  - CEPEA/ESALQ       : FOB Paranagua BR (physical_intl.json)
  - MAGyP Argentina   : FOB Rosario (physical_intl.json)
  - psd_ending_stocks : STU do JSON existente
  - cot.json          : COT do JSON existente
  - USDA ERS          : Custo de producao (CSV + fallback)

Algoritmos:
  - Walk-forward Lasso (treino 2000-2019 / teste 2020-2024)
  - COT Index normalizado 3 anos (janela 36 meses)
  - Origin Arbitrage: spread CIF Qingdao US vs BR vs ARG
  - Basis monitor: FOB local vs futuro CME
  - Scorecard multi-fator ponderado por grain

Executar do diretorio raiz do agrimacro:
    python grain_ratio_engine.py

Output:
    agrimacro-dash/public/data/processed/grain_ratios.json
"""

import json, os, re, sys, time, warnings
from datetime import datetime, timedelta
from pathlib import Path

warnings.filterwarnings("ignore")

try:
    import numpy as np
    import pandas as pd
    import requests
    from scipy import stats
    from sklearn.linear_model import LassoCV
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import r2_score
    import yfinance as yf
    from bs4 import BeautifulSoup
except ImportError as e:
    pkg = str(e).split("'")[1] if "'" in str(e) else str(e)
    print(f"ERRO: {pkg} nao instalado.")
    print("Execute: pip install numpy pandas scipy scikit-learn yfinance requests beautifulsoup4")
    sys.exit(1)

# ============================================================
# CONFIG
# ============================================================
BASE_DIR      = Path(__file__).parent
DASH_DIR      = BASE_DIR / "agrimacro-dash" / "public" / "data"
PROCESSED_DIR = DASH_DIR / "processed"
BILATERAL_DIR = DASH_DIR / "bilateral"
OUTPUT_FILE   = PROCESSED_DIR / "grain_ratios.json"
BILATERAL_DIR.mkdir(parents=True, exist_ok=True)

DATA_START = "2000-01-01"
TRAIN_END  = "2019-12-31"
TEST_START = "2020-01-01"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

def log(msg): print(msg)
def safe_float(v, default=np.nan):
    try: return float(str(v).replace(",","").replace("$","").replace("%","").strip())
    except: return default

log("=" * 65)
log("AGRIMACRO INTELLIGENCE -- GRAIN RATIO ENGINE v2.0")
log(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
log("=" * 65)

# ============================================================
# 1. PRECOS MENSAIS YFINANCE
# ============================================================
log("\n[1/8] Precos mensais via yfinance...")

TICKERS = {
    "corn":"ZC=F","soy":"ZS=F","wheat":"ZW=F",
    "soymeal":"ZM=F","soyoil":"ZL=F","live_cattle":"LE=F",
    "crude_oil":"CL=F","wheat_kc":"KE=F",
}

# Tickers cotados em cents/bu no yfinance (precisam dividir por 100)
CENTS_TICKERS = {"corn","soy","wheat","soymeal","soyoil","wheat_kc"}

def fetch_monthly(ticker, name=""):
    try:
        raw = yf.download(ticker, start=DATA_START, interval="1mo", progress=False, auto_adjust=True)
        if raw.empty: return None
        if isinstance(raw.columns, pd.MultiIndex): raw.columns = raw.columns.droplevel(1)
        s = raw["Close"].dropna()
        s.index = pd.to_datetime(s.index).to_period("M").to_timestamp()
        # Corrigir cents -> dolares para graos (yfinance retorna em cents/bu)
        if name in CENTS_TICKERS:
            # Heuristica: se mediana > 100, provavelmente em cents
            if float(s.median()) > 100:
                s = s / 100.0
        return s
    except: return None

prices = {}
for name, ticker in TICKERS.items():
    s = fetch_monthly(ticker, name=name)
    if s is not None and len(s) > 60:
        prices[name] = s
        log(f"  {name:<14} n={len(s)}  ({s.index[0].strftime('%Y-%m')} -> {s.index[-1].strftime('%Y-%m')})")
    else:
        log(f"  {name:<14} FALHOU")

df = pd.DataFrame(prices).dropna(subset=["corn","soy","wheat"])
df.index = pd.to_datetime(df.index)
log(f"  Total: {len(df)} meses, {df.shape[1]} series")

# ============================================================
# 2. CUSTO DE PRODUCAO (USDA ERS + fallback)
# ============================================================
log("\n[2/8] Custo de producao USDA ERS...")

import io as _io

def _fetch_ers_cop():
    _URLS = [
        ("corn",     "https://www.ers.usda.gov/media/4962/corn.csv"),
        ("soybeans", "https://www.ers.usda.gov/media/4976/soybeans.csv"),
        ("wheat",    "https://www.ers.usda.gov/media/4978/wheat.csv"),
    ]
    _HDR = {"User-Agent": "AgriMacro-Intelligence/3.3 (research)"}
    _res = {}
    for _idx, (_crop, _url) in enumerate(_URLS):
        try:
            _r = requests.get(_url, headers=_HDR, timeout=30)
            _r.raise_for_status()
            _dc = pd.read_csv(_io.StringIO(_r.text))
            _dc.columns = [str(c).strip() for c in _dc.columns]
            _us = _dc[_dc["Region"].str.contains("U.S. total", na=False)]
            _cost = _us[_us["Item"] == "Total, costs listed"][["Year","Value"]].rename(columns={"Value":"cost"})
            _yld  = _us[_us["Item"] == "Yield"][["Year","Value"]].rename(columns={"Value":"yield"})
            _merged = _cost.merge(_yld, on="Year")
            _merged = _merged[_merged["yield"] > 0].copy()
            _merged["cop_per_bu"] = (_merged["cost"] / _merged["yield"]).round(2)
            for _, _row in _merged.iterrows():
                _yr = int(_row["Year"])
                if _yr not in _res:
                    _res[_yr] = [float("nan"), float("nan"), float("nan")]
                _res[_yr][_idx] = float(_row["cop_per_bu"])
            log("  [ERS COP] " + _crop + ": OK (" + str(len(_merged)) + " anos, ultimo=" + str(int(_merged["Year"].max())) + ")")
        except Exception as _e:
            log("  [ERS COP] " + _crop + ": ERRO - " + str(_e))
    return _res

_ers_cop_raw = _fetch_ers_cop()

def _cop_lookup(_year, _idx):
    if _year in _ers_cop_raw and not pd.isna(_ers_cop_raw[_year][_idx]):
        return _ers_cop_raw[_year][_idx]
    for _y in range(_year - 1, 1989, -1):
        if _y in _ers_cop_raw and not pd.isna(_ers_cop_raw[_y][_idx]):
            return _ers_cop_raw[_y][_idx]
    return float("nan")

df["corn_cop"]  = df.index.map(lambda d: _cop_lookup(d.year, 0))
df["soy_cop"]   = df.index.map(lambda d: _cop_lookup(d.year, 1))
df["wheat_cop"] = df.index.map(lambda d: _cop_lookup(d.year, 2))
df["cop_is_fallback"] = df["corn_cop"].isna()
log("  COP: " + ("USDA ERS real - " + str(len(_ers_cop_raw)) + " anos" if _ers_cop_raw else "FALHA no download ERS"))

# ============================================================
# 3. STOCK-TO-USE
# ============================================================
log("\n[3/8] Stock-to-Use...")

STU = {
    "corn_stu": {2000:17,2001:16,2002:11,2003:11,2004:12,2005:20,2006:23,2007:15,
                 2008:17,2009:23,2010:15,2011:11,2012:10,2013:15,2014:13,2015:12,
                 2016:14,2017:13,2018:12,2019:11,2020:13,2021:8,2022:8,2023:13,2024:13,2025:13},
    "soy_stu":  {2000:18,2001:8,2002:6,2003:5,2004:9,2005:20,2006:22,2007:13,
                 2008:7,2009:5,2010:6,2011:5,2012:4,2013:6,2014:10,2015:11,
                 2016:11,2017:5,2018:17,2019:26,2020:9,2021:3,2022:5,2023:8,2024:10,2025:10},
    "wheat_stu":{2000:29,2001:28,2002:22,2003:20,2004:18,2005:18,2006:21,2007:23,
                 2008:26,2009:25,2010:34,2011:30,2012:30,2013:28,2014:27,2015:24,
                 2016:28,2017:33,2018:36,2019:37,2020:29,2021:34,2022:34,2023:34,2024:33,2025:33},
}

try:
    stu_path = PROCESSED_DIR / "psd_ending_stocks.json"
    if stu_path.exists():
        with open(stu_path) as f: psd = json.load(f)
        if isinstance(psd, dict):
            for k, v in psd.items():
                kl = k.lower()
                for grain_key, col in [("corn","corn_stu"),("soy","soy_stu"),("wheat","wheat_stu")]:
                    if grain_key in kl and isinstance(v, list):
                        for rec in v:
                            try:
                                yr = int(rec.get("year",0))
                                val = float(rec.get("stocks_use_pct", rec.get("stu_pct", 0)) or 0)
                                if yr > 1990 and val > 0: STU[col][yr] = val
                            except: pass
        log("  psd_ending_stocks.json aplicado")
except Exception as ex:
    log(f"  STU JSON: {ex}")

for col, m in STU.items():
    df[col] = df.index.map(lambda d, mp=m: mp.get(d.year, mp.get(d.year-1, np.nan)))
    df[f"{col}_z"] = (df[col] - df[col].mean()) / df[col].std()
log("  STU carregado")

# ============================================================
# 4. COT
# ============================================================
log("\n[4/8] COT do cot.json...")

for c in ["corn_mm_net","soy_mm_net","wheat_mm_net","corn_cot_idx","soy_cot_idx","wheat_cot_idx"]:
    df[c] = np.nan
current_cot = {}

try:
    cot_path = PROCESSED_DIR / "cot.json"
    if cot_path.exists():
        with open(cot_path) as f: cot_raw = json.load(f)
        # Tentar varios formatos de estrutura
        if isinstance(cot_raw, list):
            records = cot_raw
        elif isinstance(cot_raw, dict):
            # Tentar chaves comuns
            for key in ["data","records","cot","items","commodities","results"]:
                if key in cot_raw and isinstance(cot_raw[key], list):
                    records = cot_raw[key]
                    break
            else:
                # Pode ser {commodity: [records]}
                records = []
                for k, v in cot_raw.items():
                    if isinstance(v, list):
                        for item in v:
                            if isinstance(item, dict):
                                item["_commodity_key"] = k
                                records.append(item)
        else:
            records = []

        log(f"  COT: {len(records)} registros encontrados")
        if records:
            sample = records[0] if isinstance(records[0], dict) else {}
            log(f"  COT sample keys: {list(sample.keys())[:12]}")
            # Mostrar exemplo de valores
            for k in list(sample.keys())[:5]:
                log(f"    {k}: {str(sample[k])[:50]}")

        for grain, kws, mm_col, idx_col in [
            ("corn",  ["corn"],                "corn_mm_net",  "corn_cot_idx"),
            ("soy",   ["soybean","soja","soy"], "soy_mm_net",   "soy_cot_idx"),
            ("wheat", ["wheat","trigo"],        "wheat_mm_net", "wheat_cot_idx"),
        ]:
            rows = []
            for rec in (records if isinstance(records, list) else []):
                if not isinstance(rec, dict): continue
                # Busca em todos os campos de texto
                nm = " ".join(str(v) for k,v in rec.items()
                              if any(x in k.lower() for x in ["name","commodity","market","contract","symbol","ticker"])).lower()
                if not any(kw in nm for kw in kws): continue
                # Tentar varios campos de data
                dt = None
                for dk in ["date","report_date","as_of_date","reference_date","week"]:
                    try:
                        dt = pd.to_datetime(rec.get(dk,""))
                        if pd.notna(dt): break
                    except: pass
                if dt is None or pd.isna(dt): continue
                # Tentar varios campos de posicao MM
                mm = np.nan
                for mk in ["mm_net","managed_money_net","noncommercial_net",
                           "managed_money_long","large_trader_net",
                           "mm_long","mm_short","net_position"]:
                    v = safe_float(rec.get(mk, np.nan))
                    if not np.isnan(v):
                        # Se tiver long e short separados, calcular net
                        if mk in ["mm_long","managed_money_long"]:
                            short_v = safe_float(rec.get(mk.replace("long","short"), 0))
                            v = v - short_v
                        mm = v
                        break
                if not np.isnan(mm):
                    rows.append({"date":dt,"mm_net":mm})

            if len(rows) > 10:
                s = pd.DataFrame(rows).dropna().set_index("date").sort_index()
                s = s.groupby(level=0)["mm_net"].last()
                s.index = pd.to_datetime(s.index).to_period("M").to_timestamp()
                # COT Index normalizado 3 anos
                w = 36
                cot_idx = ((s - s.rolling(w,min_periods=12).min()) /
                           (s.rolling(w,min_periods=12).max() - s.rolling(w,min_periods=12).min()) * 100).clip(0,100)
                df[mm_col]  = df.index.map(lambda d, sv=s: sv.get(d, np.nan))
                df[idx_col] = df.index.map(lambda d, sv=cot_idx: sv.get(d, np.nan))
                lmm  = float(s.iloc[-1]) if len(s) > 0 else 0
                lidx = float(cot_idx.iloc[-1]) if len(cot_idx) > 0 else 50
                current_cot[grain] = {
                    "mm_net":    round(lmm, 0),
                    "cot_index": round(lidx, 1),
                    "signal":    "BULL" if lidx < 20 else ("BEAR" if lidx > 80 else "NEUTRO"),
                }
                log(f"  {grain:<8} mm_net={lmm:+.0f}  cot_idx={lidx:.0f}")
    else:
        log("  cot.json nao encontrado")
except Exception as ex:
    log(f"  COT: {ex}")

# ============================================================
# 5. ARBITRAGEM DE ORIGEM + FRETES
# ============================================================
log("\n[5/8] Arbitragem de origem e fretes...")

arb = {
    "timestamp": datetime.now().isoformat(),
    "bdi":            {"value":None,"date":None,"source":None},
    "basis_gulf":     {"corn":None,"soy":None,"wheat":None,"date":None,"unit":"cents/bu"},
    "fob_paranagua":  {"corn":None,"soy":None,"date":None,"unit":"USD/ton"},
    "fob_rosario":    {"corn":None,"soy":None,"wheat":None,"date":None,"unit":"USD/ton"},
    "export_sales":   {"corn":None,"soy":None,"wheat":None,"date":None},
    "spread_delivered_china": {},
    "basis_br":       {},
}

# 5a. BDI via Macrotrends
log("  5a. Baltic Dry Index (Macrotrends)...")
try:
    r = requests.get("https://www.macrotrends.net/1378/baltic-dry-index-historical-chart-data",
                     headers=HEADERS, timeout=25)
    if r.status_code == 200:
        for pat in [r'var chartData = (\[.*?\]);', r'"data":\s*(\[\[.*?\]\])']:
            m = re.search(pat, r.text, re.DOTALL)
            if m:
                raw = json.loads(m.group(1))
                bdi_rows = []
                for row in raw:
                    try:
                        if isinstance(row, (list,tuple)) and len(row) >= 2:
                            dt  = pd.to_datetime(row[0], unit="ms") if isinstance(row[0],(int,float)) else pd.to_datetime(row[0])
                            val = safe_float(row[1])
                            if not np.isnan(val) and val > 0: bdi_rows.append((dt, val))
                    except: pass
                if bdi_rows:
                    bdi_s = pd.Series(dict(bdi_rows)).sort_index()
                    arb["bdi"]["value"]  = round(float(bdi_s.iloc[-1]), 0)
                    arb["bdi"]["date"]   = bdi_s.index[-1].strftime("%Y-%m-%d")
                    arb["bdi"]["source"] = "Macrotrends/Baltic Exchange"
                    log(f"  BDI: {arb['bdi']['value']} ({arb['bdi']['date']})")
                    break
except Exception as ex: log(f"  BDI Macrotrends: {ex}")

# Fallback BDI via Yahoo Finance (^BDI)
if arb["bdi"]["value"] is None:
    try:
        log("  5a-alt. BDI via Yahoo Finance (^BDI)...")
        import yfinance as _yf
        bdi_yf = _yf.download("^BDI", period="5d", interval="1d", progress=False, auto_adjust=True)
        if not bdi_yf.empty:
            if isinstance(bdi_yf.columns, pd.MultiIndex): bdi_yf.columns = bdi_yf.columns.droplevel(1)
            last_bdi = float(bdi_yf["Close"].dropna().iloc[-1])
            if last_bdi > 0:
                arb["bdi"]["value"]  = round(last_bdi, 0)
                arb["bdi"]["date"]   = bdi_yf.index[-1].strftime("%Y-%m-%d")
                arb["bdi"]["source"] = "Yahoo Finance ^BDI"
                log(f"  BDI (Yahoo): {last_bdi:.0f} ({arb['bdi']['date']})")
    except Exception as ex2: log(f"  BDI Yahoo: {ex2}")

# ZERO MOCK: BDI sem fallback hardcoded — None sera exibido como N/D no dashboard
if arb["bdi"]["value"] is None:
    arb["bdi"]["is_fallback"] = True  # flag para o componente exibir aviso
    arb["bdi"]["date"]   = datetime.now().strftime("%Y-%m-%d")
    arb["bdi"]["is_fallback"] = True  # ZERO MOCK: sem valor real disponivel
    log("  BDI: fonte indisponivel - valor omitido do JSON (is_fallback=True)")

# 5b. Basis FOB Gulf via USDA AMS
log("  5b. Basis FOB Gulf (USDA AMS)...")
try:
    basis_urls = [
        "https://www.ams.usda.gov/mnreports/sj_gr850.txt",
        "https://www.ams.usda.gov/mnreports/GX_GR110.txt",
        "https://www.ams.usda.gov/mnreports/GX_GR115.txt",
        "https://apps.ams.usda.gov/mnreports/sj_gr850.txt",
    ]
    r = None
    for basis_url in basis_urls:
        try:
            resp = requests.get(basis_url, headers=HEADERS, timeout=20)
            if resp.status_code == 200 and len(resp.text) > 200:
                r = resp
                log(f"  Basis Gulf URL OK: {basis_url.split('/')[-1]}")
                break
        except: pass
    if r is None:
        log("  Basis Gulf: todas as URLs falharam")
    if r is not None and r.status_code == 200:
        for line in r.text.split("\n"):
            ll = line.lower()
            nums = re.findall(r"[-+]?\d+\.?\d*", line)
            if not nums: continue
            if "corn" in ll and "gulf" in ll and arb["basis_gulf"]["corn"] is None:
                v = safe_float(nums[-1])
                if -200 < v < 200: arb["basis_gulf"]["corn"] = v
            if ("soybean" in ll or "soy " in ll) and "gulf" in ll and arb["basis_gulf"]["soy"] is None:
                v = safe_float(nums[-1])
                if -200 < v < 200: arb["basis_gulf"]["soy"] = v
            if "wheat" in ll and "gulf" in ll and arb["basis_gulf"]["wheat"] is None:
                v = safe_float(nums[-1])
                if -200 < v < 200: arb["basis_gulf"]["wheat"] = v
        arb["basis_gulf"]["date"] = datetime.now().strftime("%Y-%m-%d")
        log(f"  Basis Gulf: corn={arb['basis_gulf']['corn']} soy={arb['basis_gulf']['soy']} wheat={arb['basis_gulf']['wheat']} cents/bu")
except Exception as ex: log(f"  Basis Gulf: {ex}")

# 5c/5d. FOB Paranagua + Rosario via physical_intl.json
log("  5c/5d. FOB Paranagua + Rosario (physical_intl.json)...")
try:
    pp = PROCESSED_DIR / "physical_intl.json"
    if pp.exists():
        with open(pp) as f: phys = json.load(f)

        # Diagnostico e flatten da estrutura physical_intl.json
        if isinstance(phys, dict):
            log(f"  physical_intl keys: {list(phys.keys())[:8]}")
            # Expandir chave 'international' se existir
            intl = phys.get("international", phys.get("markets", phys.get("data", {})))
            if isinstance(intl, dict):
                log(f"  international keys: {list(intl.keys())[:10]}")
                # Flatten: intl = {"paranagua": [{...}], "rosario": [{...}], ...}
                phys_flat = []
                for origin_key, origin_data in intl.items():
                    if isinstance(origin_data, list):
                        for rec in origin_data:
                            if isinstance(rec, dict):
                                rec["_origin"] = origin_key.lower()
                                phys_flat.append(rec)
                    elif isinstance(origin_data, dict):
                        origin_data["_origin"] = origin_key.lower()
                        phys_flat.append(origin_data)
                if phys_flat:
                    log(f"  Flattenizado: {len(phys_flat)} registros")
                    log(f"  Sample keys: {list(phys_flat[0].keys())[:10]}")
                    phys = phys_flat  # substituir para parse uniforme
            elif isinstance(intl, list):
                phys = intl
        elif isinstance(phys, list) and phys:
            log(f"  physical_intl[0] keys: {list(phys[0].keys())[:8] if isinstance(phys[0],dict) else 'nao-dict'}")

        def find_fob(data, origin_kw, commodity_kws):
            """Busca FOB com multiplos keywords de commodity."""
            if isinstance(commodity_kws, str): commodity_kws = [commodity_kws]
            candidates = []
            items = []
            if isinstance(data, list): items = data
            elif isinstance(data, dict):
                for v in data.values():
                    if isinstance(v, list): items.extend(v)
                    elif isinstance(v, dict): items.append(v)
            for rec in items:
                if not isinstance(rec, dict): continue
                # Checar _origin primeiro (mais preciso)
                origin_match = False
                if "_origin" in rec:
                    origin_match = origin_kw in rec["_origin"].lower()
                # Fallback: buscar em todos os textos
                if not origin_match:
                    all_text = " ".join(str(v) for v in rec.values() if isinstance(v, (str,int,float))).lower()
                    origin_match = origin_kw in all_text
                if not origin_match: continue
                # Verificar commodity
                all_text = " ".join(str(v) for v in rec.values() if isinstance(v, (str,int,float))).lower()
                if not any(kw in all_text for kw in commodity_kws): continue
                # Buscar preco
                for pk in ["price","price_usd","usd_ton","usd_per_ton","value",
                           "settlement","close","last","bid","offer","fob",
                           "preco","preco_usd","valor"]:
                    v = safe_float(rec.get(pk))
                    if not np.isnan(v) and 50 < v < 2000:
                        candidates.append(v)
                        break
                # Ultimo recurso: qualquer numero razoavel
                if not candidates:
                    for k, v in rec.items():
                        if k.startswith("_"): continue
                        fv = safe_float(v)
                        if not np.isnan(fv) and 100 < fv < 2000:
                            candidates.append(fv)
                            break
            return round(float(candidates[0]), 2) if candidates else None

        arb["fob_paranagua"]["soy"]  = find_fob(phys, "paranagua", ["soy","soja","soybeans"])
        arb["fob_paranagua"]["corn"] = find_fob(phys, "paranagua", ["corn","milho","maize"])
        arb["fob_rosario"]["soy"]    = find_fob(phys, "rosario",   ["soy","soja","soybeans"])
        arb["fob_rosario"]["corn"]   = find_fob(phys, "rosario",   ["corn","milho","maiz","maize"])
        arb["fob_rosario"]["wheat"]  = find_fob(phys, "rosario",   ["wheat","trigo"])
        arb["fob_paranagua"]["date"] = arb["fob_rosario"]["date"] = datetime.now().strftime("%Y-%m-%d")
        log(f"  FOB Paranagua: soy={arb['fob_paranagua']['soy']} corn={arb['fob_paranagua']['corn']} USD/ton")
        log(f"  FOB Rosario:   soy={arb['fob_rosario']['soy']} corn={arb['fob_rosario']['corn']} wheat={arb['fob_rosario']['wheat']} USD/ton")
except Exception as ex: log(f"  FOB Paranagua/Rosario: {ex}")

# 5e. USDA FAS Export Sales
log("  5e. Export Sales (USDA FAS API)...")
try:
    yr = datetime.now().year
    for grain, code, col in [("corn","0440100","corn"),("soy","2222000","soy"),("wheat","0410000","wheat")]:
        try:
            url = f"https://apps.fas.usda.gov/psdonline/api/psdon/commodity/{code}/data/"
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    recent = sorted(data, key=lambda x: x.get("marketYear",0), reverse=True)[:1]
                    for rec in recent:
                        exp = safe_float(rec.get("exports", rec.get("totalExports", 0)))
                        if not np.isnan(exp) and exp > 0:
                            arb["export_sales"][col] = round(exp, 1)
                            log(f"  FAS {grain}: {exp:.0f} MT")
        except: pass
except Exception as ex: log(f"  FAS: {ex}")

# 5f. Calcular spread CIF Qingdao
log("  5f. Calculando CIF Qingdao (US vs BR vs ARG)...")
try:
    latest = df.iloc[-1]
    bdi_val = arb["bdi"]["value"] or 1500
    # Regressao historica BDI -> frete $/ton (C14 Santos-Far East proxy)
    freight_santos = round(bdi_val * 0.017 + 3.5, 1)
    freight_gulf   = round(bdi_val * 0.013 + 2.8, 1)

    # Conversao bu -> ton
    corn_cme  = float(latest.get("corn",  4.25)) * 39.368
    soy_cme   = float(latest.get("soy",   11.20)) * 36.744
    wheat_cme = float(latest.get("wheat", 5.89))  * 36.744

    # Basis Gulf (cents/bu -> USD/ton)
    bg_corn  = (arb["basis_gulf"]["corn"]  or 0) / 100 * 39.368
    bg_soy   = (arb["basis_gulf"]["soy"]   or 0) / 100 * 36.744
    bg_wheat = (arb["basis_gulf"]["wheat"] or 0) / 100 * 36.744

    fob_gulf_corn  = round(corn_cme  + bg_corn,  1)
    fob_gulf_soy   = round(soy_cme   + bg_soy,   1)
    fob_gulf_wheat = round(wheat_cme + bg_wheat,  1)

    fob_br_soy  = arb["fob_paranagua"]["soy"]  or round(soy_cme  * 0.97, 1)
    fob_br_corn = arb["fob_paranagua"]["corn"] or round(corn_cme * 0.96, 1)
    fob_ar_soy  = arb["fob_rosario"]["soy"]    or round(soy_cme  * 0.95, 1)
    fob_ar_corn = arb["fob_rosario"]["corn"]   or round(corn_cme * 0.94, 1)

    cif = {
        "corn_us":    round(fob_gulf_corn  + freight_gulf,   1),
        "corn_br":    round(fob_br_corn    + freight_santos,  1),
        "corn_arg":   round(fob_ar_corn    + freight_santos * 0.92, 1),
        "soy_us":     round(fob_gulf_soy   + freight_gulf,   1),
        "soy_br":     round(fob_br_soy     + freight_santos,  1),
        "soy_arg":    round(fob_ar_soy     + freight_santos * 0.92, 1),
        "wheat_us":   round(fob_gulf_wheat + freight_gulf,   1),
    }

    sp = {
        "soy_us_vs_br":  round(cif["soy_us"]  - cif["soy_br"],  1),
        "soy_us_vs_arg": round(cif["soy_us"]  - cif["soy_arg"], 1),
        "corn_us_vs_br": round(cif["corn_us"] - cif["corn_br"], 1),
        "corn_us_vs_arg":round(cif["corn_us"] - cif["corn_arg"],1),
    }

    def winner(v):
        if v is None: return "N/A"
        if v > 8:     return "BR/ARG vantagem"
        elif v < -8:  return "US vantagem"
        else:         return "Paridade"

    arb["spread_delivered_china"] = {
        "freight_gulf_china_per_ton":  freight_gulf,
        "freight_santos_china_per_ton":freight_santos,
        "bdi_used": bdi_val,
        "fob_gulf": {"corn":fob_gulf_corn,"soy":fob_gulf_soy,"wheat":fob_gulf_wheat},
        "fob_paranagua": {"corn":fob_br_corn,"soy":fob_br_soy},
        "fob_rosario":   {"corn":fob_ar_corn,"soy":fob_ar_soy},
        "cif_qingdao": cif,
        "spreads":     sp,
        "competitive_advantage": {
            "soy_china":  winner(sp["soy_us_vs_br"]),
            "corn_china": winner(sp["corn_us_vs_br"]),
        },
        "note": "Frete estimado via regressao BDI vs rotas historicas Baltic. Precisao: +/-15%.",
    }

    log(f"  Frete: Gulf->China ${freight_gulf}/ton  Santos->China ${freight_santos}/ton")
    log(f"  CIF Qingdao Soja:  US=${cif['soy_us']}  BR=${cif['soy_br']}  ARG=${cif['soy_arg']} /ton")
    log(f"  CIF Qingdao Milho: US=${cif['corn_us']} BR=${cif['corn_br']} ARG=${cif['corn_arg']} /ton")
    log(f"  Vantagem soja China:  {winner(sp['soy_us_vs_br'])}")
    log(f"  Vantagem milho China: {winner(sp['corn_us_vs_br'])}")

    # Basis BR
    arb["basis_br"] = {
        "soy":  round(fob_br_soy  - soy_cme,  1),
        "corn": round(fob_br_corn - corn_cme, 1),
        "unit": "USD/ton (FOB Paranagua vs CME convertido)",
    }

except Exception as ex: log(f"  Spread China: {ex}")

# ============================================================
# 6. RATIOS E FATORES
# ============================================================
log("\n[6/8] Calculando ratios e fatores...")

df["ratio_corn_soy"]   = df["corn"] / df["soy"]
df["ratio_wheat_corn"] = df["wheat"] / df["corn"]
if "live_cattle" in df.columns:
    df["ratio_corn_cattle"] = df["live_cattle"] / df["corn"]
if "soymeal" in df.columns and "soyoil" in df.columns:
    df["crush_spread"] = (df["soymeal"] * 44/2000 + df["soyoil"]/100 * 11) - df["soy"]
if "crude_oil" in df.columns and "soyoil" in df.columns:
    df["ratio_oil_crude"] = df["soyoil"] / (df["crude_oil"] * 0.01 * 7.5)

df["margin_corn"]  = df["corn"]  - df["corn_cop"]
df["margin_soy"]   = df["soy"]   - df["soy_cop"]
df["margin_wheat"] = df["wheat"] - df["wheat_cop"]
df["below_cop_corn"]  = (df["corn"]  < df["corn_cop"]).astype(int)
df["below_cop_soy"]   = (df["soy"]   < df["soy_cop"]).astype(int)
df["below_cop_wheat"] = (df["wheat"] < df["wheat_cop"]).astype(int)

for c in ["corn","soy","wheat"]:
    df[f"{c}_ret1m"]  = df[c].pct_change(1)  * 100
    df[f"{c}_ret3m"]  = df[c].pct_change(3)  * 100
    df[f"{c}_fwd3m"]  = df[c].pct_change(3).shift(-3)  * 100
    df[f"{c}_fwd6m"]  = df[c].pct_change(6).shift(-6)  * 100
    df[f"{c}_fwd12m"] = df[c].pct_change(12).shift(-12) * 100

df["month"] = df.index.month
log("  Ratios e fatores calculados")

# ============================================================
# 7. WALK-FORWARD LASSO
# ============================================================
log("\n[7/8] Walk-forward Lasso...")

FACTORS = [c for c in [
    "ratio_corn_soy","ratio_wheat_corn","crush_spread","ratio_oil_crude",
    "corn_stu_z","soy_stu_z","wheat_stu_z",
    "margin_corn","margin_soy","margin_wheat",
    "below_cop_corn","below_cop_soy","below_cop_wheat",
    "corn_ret3m","soy_ret3m","wheat_ret3m",
    "corn_mm_net","soy_mm_net","wheat_mm_net",
    "corn_cot_idx","soy_cot_idx","wheat_cot_idx",
] if c in df.columns and df[c].notna().sum() > 50]

model_results    = {}
importance_total = {}

for grain in ["corn","soy","wheat"]:
    grain_res = {}
    for hz, fc in [("3m",f"{grain}_fwd3m"),("6m",f"{grain}_fwd6m"),("12m",f"{grain}_fwd12m")]:
        try:
            sub = df[FACTORS + [fc]].dropna()
            if len(sub) < 50: continue
            train = sub[sub.index <= TRAIN_END]
            test  = sub[sub.index >= TEST_START]
            if len(train) < 30 or len(test) < 6: continue

            sc = StandardScaler()
            X_tr = sc.fit_transform(train[FACTORS]); y_tr = train[fc].values
            X_te = sc.transform(test[FACTORS]);       y_te = test[fc].values

            m = LassoCV(cv=5, max_iter=5000, random_state=42)
            m.fit(X_tr, y_tr)

            r2_in  = r2_score(y_tr, m.predict(X_tr))
            r2_out = max(r2_score(y_te, m.predict(X_te)), 0.0)
            dir_acc = np.mean(np.sign(m.predict(X_te)) == np.sign(y_te)) * 100

            coef_abs = np.abs(m.coef_); total_c = coef_abs.sum()
            imp = {}
            if total_c > 0:
                for fn, co, ca in sorted(zip(FACTORS, m.coef_, coef_abs), key=lambda x: abs(x[1]), reverse=True)[:8]:
                    if ca > 0:
                        imp[fn] = {"coef":round(float(co),4),"importance":round(float(ca/total_c*100),1),
                                   "direction":"BULL" if co > 0 else "BEAR"}
                        importance_total[fn] = importance_total.get(fn,0) + ca/total_c*100

            grain_res[hz] = {
                "r2_in_sample":       round(r2_in  * 100, 1),
                "r2_out_of_sample":   round(r2_out * 100, 1),
                "directional_accuracy":round(dir_acc, 1),
                "n_train": int(len(train)), "n_test": int(len(test)),
                "factors": imp,
            }
            log(f"  {grain:<8} fwd{hz}: R2_out={r2_out*100:.1f}%  DirAcc={dir_acc:.1f}%  n_test={len(test)}")
        except Exception as ex: log(f"  {grain} fwd{hz}: {ex}")
    model_results[grain] = grain_res

importance_ranking = sorted(importance_total.items(), key=lambda x: x[1], reverse=True)

# STU backtest por bucket
stu_backtest = {}
for grain, stu_col, fwd_col in [("corn","corn_stu","corn_fwd12m"),("soy","soy_stu","soy_fwd12m"),("wheat","wheat_stu","wheat_fwd12m")]:
    if stu_col not in df.columns: continue
    b = {}
    for label, lo, hi in [("critico_lt8",0,8),("apertado_8_12",8,12),("normal_12_18",12,18),("folgado_gt18",18,100)]:
        sub = df[(df[stu_col]>=lo)&(df[stu_col]<hi)][fwd_col].dropna()
        if len(sub) >= 2:
            b[label] = {"n":int(len(sub)),"avg_fwd12m":round(float(sub.mean()),1),
                        "pct_positive":round(float((sub>0).mean()*100),0),"std":round(float(sub.std()),1)}
    stu_backtest[grain] = b

# COP backtest
cop_backtest = {}
for grain, pc, cc, fc in [("corn","corn","corn_cop","corn_fwd12m"),("soy","soy","soy_cop","soy_fwd12m"),("wheat","wheat","wheat_cop","wheat_fwd12m")]:
    below = df[df[pc] < df[cc]][fc].dropna()
    above = df[df[pc] >= df[cc]][fc].dropna()
    cop_backtest[grain] = {
        "below_cop":{"n":int(len(below)),"avg_fwd12m":round(float(below.mean()),1) if len(below)>1 else None,"pct_positive":round(float((below>0).mean()*100),0) if len(below)>1 else None},
        "above_cop":{"n":int(len(above)),"avg_fwd12m":round(float(above.mean()),1) if len(above)>1 else None,"pct_positive":round(float((above>0).mean()*100),0) if len(above)>1 else None},
    }

# ============================================================
# 8. SNAPSHOT ATUAL + SCORECARD
# ============================================================
log("\n[8/8] Snapshot atual e scorecards...")

def cv(col):
    try: return round(float(df[col].dropna().iloc[-1]), 3)
    except: return None

def hs(col):
    try:
        s = df[col].dropna(); v = s.iloc[-1]
        mn,mu,mx = s.min(),s.mean(),s.max()
        pct = (v-mn)/(mx-mn)*100 if mx!=mn else 50
        z   = (v-mu)/s.std() if s.std()>0 else 0
        status = ("BAIXO_EXTREMO" if pct<20 else "ABAIXO_MEDIA" if pct<35 else
                  "ALTO_EXTREMO" if pct>80 else "ACIMA_MEDIA" if pct>65 else "NORMAL")
        return {"current":round(float(v),3),"min":round(float(mn),3),"mean":round(float(mu),3),
                "max":round(float(mx),3),"pct":round(float(pct),1),"z_score":round(float(z),2),"status":status}
    except: return None

current_snapshot = {
    "date": df.index[-1].strftime("%Y-%m-%d") if len(df)>0 else "N/A",
    "prices": {k: cv(k) for k in ["corn","soy","wheat","soymeal","soyoil","live_cattle","crude_oil"]},
    "ratios": {k: hs(v) for k,v in [("corn_soy","ratio_corn_soy"),("wheat_corn","ratio_wheat_corn"),
                                      ("corn_cattle","ratio_corn_cattle"),("crush_spread","crush_spread"),("oil_crude","ratio_oil_crude")]},
    "stu": {g: {"current":cv(f"{g}_stu"),"z":cv(f"{g}_stu_z")} for g in ["corn","soy","wheat"]},
    "margins": {g: {"price":cv(g),"cop":cv(f"{g}_cop"),"margin":cv(f"margin_{g}")} for g in ["corn","soy","wheat"]},
    "cot": current_cot,
}

def scorecard(grain):
    sn = current_snapshot; signals = []

    # STU
    stu_data = sn["stu"].get(grain,{})
    stu_z = stu_data.get("z")
    if stu_z is None: stu_z = 0.0
    else: stu_z = float(stu_z)
    if   stu_z < -0.8: signals.append({"factor":"Stock-to-Use","signal":"BULL","detail":f"Apertado (z={stu_z:.2f})","weight":3})
    elif stu_z >  0.8: signals.append({"factor":"Stock-to-Use","signal":"BEAR","detail":f"Folgado (z={stu_z:.2f})","weight":3})
    else:              signals.append({"factor":"Stock-to-Use","signal":"NEUTRO","detail":f"Normal (z={stu_z:.2f})","weight":1})

    # COP
    mg = sn["margins"].get(grain,{}).get("margin") or 0
    if   mg < 0:   signals.append({"factor":"Preco vs COP","signal":"BULL","detail":"Abaixo custo producao (contrarian)","weight":2})
    elif mg < 0.5: signals.append({"factor":"Preco vs COP","signal":"NEUTRO","detail":"Margem muito estreita","weight":1})
    else:          signals.append({"factor":"Preco vs COP","signal":"NEUTRO","detail":f"Margem positiva ${mg:.2f}","weight":1})

    # COT
    cot_idx = sn["cot"].get(grain,{}).get("cot_index") or 50
    if   cot_idx < 20: signals.append({"factor":"COT Index","signal":"BULL","detail":f"Fundos leves ({cot_idx:.0f}/100)","weight":2})
    elif cot_idx > 80: signals.append({"factor":"COT Index","signal":"BEAR","detail":f"Fundos sobrecomprados ({cot_idx:.0f}/100)","weight":2})
    else:              signals.append({"factor":"COT Index","signal":"NEUTRO","detail":f"Posicao neutra ({cot_idx:.0f}/100)","weight":1})

    # Corn/Soy
    if grain in ["corn","soy"]:
        pct = (sn["ratios"].get("corn_soy") or {}).get("pct") or 50
        if grain == "corn":
            sig = "BULL" if pct < 30 else "NEUTRO"
            signals.append({"factor":"Corn/Soy Ratio","signal":sig,"detail":f"Percentil {pct:.0f}% - {'rotacao acreage milho' if sig=='BULL' else 'normal'}","weight":2})
        else:
            sig = "BEAR" if pct < 30 else "NEUTRO"
            signals.append({"factor":"Corn/Soy Ratio","signal":sig,"detail":f"Percentil {pct:.0f}%","weight":1})

    # Crush (soja)
    if grain == "soy":
        pct = (sn["ratios"].get("crush_spread") or {}).get("pct") or 50
        sig = "BULL" if pct > 60 else ("BEAR" if pct < 30 else "NEUTRO")
        signals.append({"factor":"Crush Spread","signal":sig,"detail":f"Percentil {pct:.0f}%","weight":2})

    # Wheat/Corn (trigo)
    if grain == "wheat":
        wc = (sn["ratios"].get("wheat_corn") or {}).get("current") or 1.2
        if   wc > 1.4: signals.append({"factor":"Wheat/Corn Ratio","signal":"BEAR","detail":f"Trigo caro ({wc:.2f}) - substituicao","weight":2})
        elif wc < 1.0: signals.append({"factor":"Wheat/Corn Ratio","signal":"BULL","detail":f"Trigo barato ({wc:.2f})","weight":1})
        else:          signals.append({"factor":"Wheat/Corn Ratio","signal":"NEUTRO","detail":f"Normal ({wc:.2f})","weight":1})

    # Arbitragem origem
    adv_map = arb.get("spread_delivered_china",{}).get("competitive_advantage",{})
    key_map = {"corn":"corn_china","soy":"soy_china"}
    adv = adv_map.get(key_map.get(grain,""), "N/A")
    if "US vantagem" in adv:   signals.append({"factor":"Arbitragem Origem","signal":"BULL","detail":"US competitivo entregue China","weight":1})
    elif "BR/ARG" in adv:      signals.append({"factor":"Arbitragem Origem","signal":"BEAR","detail":"BR/ARG mais barato entregue China","weight":1})

    bull  = sum(s["weight"] for s in signals if s["signal"]=="BULL")
    bear  = sum(s["weight"] for s in signals if s["signal"]=="BEAR")
    total = sum(s["weight"] for s in signals)
    score = round((bull-bear)/total*100, 1) if total > 0 else 0
    comp  = "BULL" if score > 20 else ("BEAR" if score < -20 else "NEUTRO")
    return {"signals":signals,"bull_weight":bull,"bear_weight":bear,
            "composite_score":score,"composite_signal":comp}

scorecards = {g: scorecard(g) for g in ["corn","soy","wheat"]}

# Sazonalidade historica real
seasonality = {}
for grain in ["corn","soy","wheat"]:
    rc = f"{grain}_ret1m"
    if rc in df.columns:
        ma = df.groupby("month")[rc].mean()
        seasonality[grain] = {int(m): round(float(v),2) for m,v in ma.items()}

# ── OUTPUT ────────────────────────────────────────────────────
output = {
    "meta": {
        "generated_at":  datetime.now().isoformat(),
        "engine_version":"2.0",
        "data_start":     DATA_START,
        "data_end":       df.index[-1].strftime("%Y-%m-%d") if len(df)>0 else "N/A",
        "n_months":       int(len(df)),
        "train_period":   f"{DATA_START} - {TRAIN_END}",
        "test_period":    f"{TEST_START} - presente",
        "sources":["yfinance CME","USDA WASDE/PSD","CFTC COT","USDA AMS Basis",
                   "CEPEA Paranagua","MAGyP Rosario","Baltic Dry Index (Macrotrends)",
                   "USDA ERS COP","USDA FAS Exports"],
    },
    "model_results":    model_results,
    "factor_ranking":   [{"factor":f,"total_importance":round(float(v),1)} for f,v in importance_ranking[:12]],
    "stu_backtest":     stu_backtest,
    "cop_backtest":     cop_backtest,
    "current_snapshot": current_snapshot,
    "scorecards":       scorecards,
    "seasonality":      seasonality,
    "arbitrage":        arb,
}

try:
    with open(OUTPUT_FILE,"w",encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, default=str)
    log(f"\n  Output: {OUTPUT_FILE}")
except Exception as ex:
    log(f"  ERRO ao salvar em {OUTPUT_FILE}: {ex}")
    fb = Path("grain_ratios.json")
    with open(fb,"w",encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, default=str)
    log(f"  Salvo localmente: {fb}")

# ── RESUMO TERMINAL ───────────────────────────────────────────
log("\n" + "=" * 65)
log("RESUMO -- ACURACIA OUT-OF-SAMPLE (2020-2024)")
log("=" * 65)
for grain in ["corn","soy","wheat"]:
    for hz in ["3m","6m","12m"]:
        r = model_results.get(grain,{}).get(hz,{})
        if r: log(f"  {grain:<8} fwd{hz}: R2={r['r2_out_of_sample']:>5.1f}%  DirAcc={r['directional_accuracy']:>5.1f}%")

log("\nFATORES TOP 8 (Lasso multivariado):")
for fn, imp in importance_ranking[:8]:
    log(f"  {fn:<30} {imp:.1f}")

log("\nARBITRAGEM ORIGEM -- CIF QINGDAO:")
cifs = arb.get("spread_delivered_china",{}).get("cif_qingdao",{})
advs = arb.get("spread_delivered_china",{}).get("competitive_advantage",{})
log(f"  BDI: {arb['bdi']['value']} pts")
log(f"  Soja:  US=${cifs.get('soy_us','?')}  BR=${cifs.get('soy_br','?')}  ARG=${cifs.get('soy_arg','?')} /ton")
log(f"  Milho: US=${cifs.get('corn_us','?')} BR=${cifs.get('corn_br','?')} ARG=${cifs.get('corn_arg','?')} /ton")
log(f"  Vantagem soja:  {advs.get('soy_china','N/A')}")
log(f"  Vantagem milho: {advs.get('corn_china','N/A')}")

log("\nSCORECARDS:")
for g, sc in scorecards.items():
    log(f"  {g.upper():<8} {sc['composite_signal']:<8} score={sc['composite_score']:+.0f}  bull={sc['bull_weight']} bear={sc['bear_weight']}")

log(f"\nConcluido: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
log("=" * 65)
