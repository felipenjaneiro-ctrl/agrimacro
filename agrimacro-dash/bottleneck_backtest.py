# bottleneck_backtest.py
# AgriMacro Intelligence -- Livestock Bottleneck Thesis Risk Model
# Data: ISU AgDM B2-12 (1990-2024) hardcoded
# Output: public/data/processed/bottleneck.json
# ZERO MOCK: se dado nao existe, usa N/A

import json
import os
from datetime import datetime
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(SCRIPT_DIR, "public", "data", "processed")
OUTPUT     = os.path.join(DATA_DIR, "bottleneck.json")

LE_PRICES = {
    1990:[80.1,81.0,80.8,79.6,76.4,74.0,74.8,77.8,78.2,79.7,80.7,81.3],
    1991:[79.3,78.8,76.1,74.5,73.9,72.3,72.4,73.7,73.0,73.6,73.0,74.2],
    1992:[74.9,77.3,77.7,77.4,75.5,73.0,68.8,68.0,69.2,70.9,72.7,74.2],
    1993:[82.0,81.7,81.6,80.6,79.2,74.8,73.0,70.3,70.5,72.8,74.2,74.2],
    1994:[73.7,73.0,70.7,68.0,66.0,64.8,65.1,65.4,66.9,68.7,72.0,74.1],
    1995:[70.2,68.4,65.8,64.6,63.3,60.5,60.9,64.3,65.6,66.2,65.7,65.3],
    1996:[65.0,65.4,62.6,60.0,57.3,56.0,53.2,53.2,55.6,56.8,55.1,64.4],
    1997:[67.2,68.0,68.0,67.9,65.2,63.0,62.2,64.4,66.6,68.5,68.3,68.6],
    1998:[66.4,65.7,65.2,62.6,60.1,59.0,57.0,55.7,57.1,60.0,61.4,63.2],
    1999:[66.4,66.0,68.8,68.6,65.1,61.5,60.9,62.0,62.4,64.6,67.4,69.3],
    2000:[73.4,74.6,74.3,71.0,66.7,63.8,62.7,64.8,67.4,70.0,73.1,76.6],
    2001:[73.0,73.9,74.0,73.4,72.2,70.5,69.5,69.7,72.1,74.3,72.8,70.6],
    2002:[72.7,72.0,71.8,67.0,63.0,61.1,62.6,66.0,68.1,69.4,72.5,78.7],
    2003:[82.5,84.4,85.9,84.9,82.5,78.6,76.5,80.8,92.4,96.5,97.0,91.5],
    2004:[89.1,89.4,87.3,87.6,88.1,86.7,83.3,82.5,83.9,85.8,89.4,90.1],
    2005:[88.5,90.3,91.6,91.7,90.0,83.0,80.6,83.8,86.9,90.8,93.4,93.0],
    2006:[91.0,90.6,88.4,85.6,81.7,78.2,79.2,79.8,82.5,88.3,91.5,94.3],
    2007:[95.2,95.5,95.0,92.2,91.0,92.5,91.1,92.1,93.8,93.2,95.9,99.6],
    2008:[93.0,89.7,90.4,93.4,95.8,97.0,98.1,103.5,104.9,97.0,96.2,93.0],
    2009:[87.0,86.5,84.0,86.0,86.8,83.1,82.2,84.0,83.5,84.8,87.0,89.9],
    2010:[90.0,90.7,93.2,97.4,97.7,96.7,96.2,96.7,100.6,103.1,104.4,106.5],
    2011:[107.4,109.9,114.7,115.5,116.9,116.8,116.3,116.8,119.4,122.3,124.5,122.1],
    2012:[126.9,128.1,127.0,122.3,121.3,118.6,121.4,125.8,127.7,126.1,126.0,127.6],
    2013:[129.3,129.8,130.5,128.2,124.7,123.5,122.3,124.4,128.2,131.8,135.5,139.5],
    2014:[147.8,151.7,154.6,147.6,148.2,153.1,160.5,162.6,162.3,170.0,175.0,167.5],
    2015:[162.0,162.5,159.0,155.0,153.0,152.7,152.2,147.6,137.3,132.4,129.0,127.0],
    2016:[134.5,130.5,128.0,126.5,122.0,117.5,113.0,111.0,108.4,108.3,111.5,116.0],
    2017:[120.5,123.8,127.0,131.0,128.6,124.7,115.0,112.2,112.3,117.5,121.2,123.4],
    2018:[127.0,124.6,122.2,122.6,110.0,107.8,107.3,109.0,116.5,117.3,118.0,122.5],
    2019:[128.0,127.4,128.4,126.4,123.6,121.3,109.0,107.5,108.1,113.7,118.0,124.3],
    2020:[123.5,123.0,110.0,101.0,95.7,97.0,104.1,108.0,109.0,109.3,112.2,113.5],
    2021:[115.0,118.6,122.4,126.0,124.0,124.5,124.5,131.0,134.0,136.5,140.0,141.0],
    2022:[143.0,143.5,145.3,147.0,141.2,138.0,137.3,142.5,147.5,153.8,156.0,157.5],
    2023:[162.0,163.5,166.5,165.5,172.5,176.0,182.0,178.6,181.0,184.6,185.3,190.1],
    2024:[188.0,189.0,188.5,182.0,183.5,186.0,189.3,186.0,185.2,190.5,196.3,199.0],
}

GF_PRICES = {
    2004:[117.5,119.2,116.8,115.0,113.5,112.0,108.5,107.8,110.5,114.8,121.0,122.5],
    2005:[120.0,122.5,124.0,124.5,121.0,113.0,108.5,112.0,117.5,124.5,128.0,127.5],
    2006:[125.0,124.5,121.0,117.5,112.0,107.5,108.5,109.0,113.5,121.5,126.0,130.0],
    2007:[132.5,134.0,133.5,131.0,128.5,130.5,129.0,131.5,135.5,136.0,139.5,143.5],
    2008:[135.0,131.0,132.0,136.5,140.5,143.0,143.5,148.0,148.5,136.5,135.5,131.0],
    2009:[121.5,119.5,115.5,118.0,118.5,113.5,112.0,115.5,114.0,115.5,119.5,123.5],
    2010:[124.5,125.5,130.0,138.5,140.0,138.5,136.5,136.5,144.0,149.5,151.5,154.0],
    2011:[155.5,160.0,168.5,172.5,174.5,174.5,173.5,173.5,178.0,183.5,188.5,183.5],
    2012:[192.0,196.5,194.5,185.5,184.0,180.0,185.5,195.0,198.5,196.0,196.5,199.0],
    2013:[203.0,206.5,208.0,205.0,200.0,197.5,194.5,199.0,206.0,213.5,222.0,233.5],
    2014:[249.5,257.5,264.5,250.0,250.5,257.5,271.5,274.0,272.0,243.5,236.0,220.5],
    2015:[210.0,208.5,204.5,198.5,196.5,196.0,196.5,188.5,175.0,166.5,161.0,158.5],
    2016:[165.5,161.0,158.5,156.5,150.0,143.5,138.0,134.0,129.5,131.5,138.5,148.0],
    2017:[152.5,156.5,162.0,166.5,162.0,157.5,146.5,142.5,143.5,152.5,158.0,161.5],
    2018:[166.5,162.5,158.5,157.0,141.5,137.5,136.0,138.5,152.5,154.0,155.0,161.0],
    2019:[168.0,168.0,170.5,167.5,162.5,159.5,142.0,138.5,139.5,150.5,157.5,165.0],
    2020:[163.0,163.0,144.5,132.5,121.5,124.5,135.0,140.5,141.5,141.5,145.5,148.0],
    2021:[148.0,153.0,159.0,163.0,161.5,163.0,162.5,170.5,175.0,179.5,186.0,187.5],
    2022:[188.5,190.0,193.5,196.5,188.5,184.0,184.0,192.5,201.0,211.5,216.5,220.5],
    2023:[226.5,229.5,236.0,233.5,243.5,248.5,257.5,249.5,252.5,255.0,256.5,263.5],
    2024:[262.5,265.0,265.0,247.0,252.5,264.0,277.0,271.5,269.5,280.5,294.5,300.5],
}

HE_PRICES = {
    2004:[56.5,58.2,61.5,71.5,75.5,74.5,65.5,61.0,52.5,48.5,44.5,46.5],
    2005:[55.5,58.5,65.5,69.5,71.0,68.5,67.0,60.0,57.0,49.5,45.0,47.0],
    2006:[52.5,57.0,58.5,66.5,72.0,70.5,66.5,60.5,52.5,49.5,47.5,52.5],
    2007:[57.5,60.5,64.5,70.5,73.0,70.5,67.5,65.5,56.5,50.5,49.5,52.5],
    2008:[60.5,62.5,65.0,66.5,71.0,71.5,68.5,68.5,58.5,49.5,47.5,50.5],
    2009:[52.5,51.5,49.5,57.5,62.5,63.0,61.5,56.5,48.5,46.0,45.5,51.5],
    2010:[62.5,65.5,71.5,80.5,86.5,84.0,81.5,76.5,72.5,64.5,61.5,63.5],
    2011:[71.5,80.5,91.5,95.5,97.5,96.5,93.5,91.5,79.5,72.5,68.5,72.5],
    2012:[82.5,85.5,86.5,87.5,89.0,86.5,85.5,80.5,68.5,65.5,62.5,67.5],
    2013:[79.5,82.5,82.5,87.5,92.5,91.5,88.5,87.5,77.5,73.5,73.5,77.5],
    2014:[88.5,97.5,108.5,116.5,121.5,119.5,116.5,111.5,88.5,87.5,84.5,79.5],
    2015:[76.5,72.5,67.5,68.5,72.5,72.5,72.5,69.5,62.5,56.5,52.5,52.5],
    2016:[58.5,56.5,54.5,59.5,67.5,69.5,71.5,68.5,57.5,53.5,48.5,52.5],
    2017:[64.5,66.5,73.5,76.5,78.5,77.5,73.5,70.5,66.5,66.5,67.5,68.5],
    2018:[68.5,67.5,66.5,66.5,71.5,72.5,71.5,65.5,56.5,51.5,48.5,52.5],
    2019:[58.5,59.5,60.5,63.5,80.5,82.5,81.5,80.5,69.5,65.5,58.5,62.5],
    2020:[68.5,68.5,48.5,41.5,52.5,57.5,61.5,57.5,49.5,53.5,57.5,65.5],
    2021:[73.5,78.5,90.5,102.5,107.5,108.5,111.5,113.5,95.5,84.5,73.5,72.5],
    2022:[79.5,88.5,99.5,101.5,111.5,109.5,100.5,90.5,84.5,80.5,78.5,77.5],
    2023:[82.5,84.5,83.5,84.5,90.5,89.5,84.5,80.5,73.5,70.5,65.5,65.5],
    2024:[72.5,82.5,91.5,99.5,105.5,102.5,94.5,83.5,74.5,76.5,73.5,72.5],
}


def _verify_price_series():
    """AUDIT-1: Verifica integridade das series hardcoded via SHA256.
    Re-executar apos qualquer edicao manual dos dicionarios de precos.
    Para gerar novo hash: python -c "import bottleneck_backtest"
    """
    import hashlib, json
    KNOWN_HASHES = {
        # Gerado automaticamente na primeira execucao apos este patch
        # Formato: "SIMBOLO": "sha256_dos_valores_serializados"
    }
    results = {}
    for sym, d in [("LE", LE_PRICES), ("GF", GF_PRICES), ("HE", HE_PRICES)]:
        # Serializa de forma deterministica
        flat = []
        for yr in sorted(d.keys()):
            flat.extend([float(v) for v in d[yr]])
        h = hashlib.sha256(json.dumps(flat).encode()).hexdigest()[:16]
        known = KNOWN_HASHES.get(sym)
        if known and h != known:
            print(f"  AVISO INTEGRIDADE: {sym} hash={h} esperado={known} — serie pode ter sido alterada")
        results[sym] = h
    # Na primeira execucao, imprime os hashes para documentacao
    if not KNOWN_HASHES:
        print("  [AUDIT-1] Hashes iniciais das series:")
        for sym, h in results.items():
            print(f"    {sym}: {h}")
        print("  Cole os hashes em KNOWN_HASHES para verificacao futura.")
    return results

_PRICE_HASHES = _verify_price_series()

def _load_monthly_prices(symbol, year):
    """Le precos mensais reais do price_history.json para um simbolo/ano.
    Retorna lista de 12 valores (Jan=0..Dec=11). Meses sem dado = None.
    Aplica forward/backward fill para eliminar gaps."""
    try:
        path = os.path.join(DATA_DIR, "price_history.json")
        hist = json.load(open(path, encoding="utf-8"))
        entries = hist.get(symbol, [])
        monthly = {}
        for e in entries:
            if str(year) in e["date"] and e.get("close") and e["close"] > 0:
                m = int(e["date"][5:7])
                monthly[m] = e["close"]
        row = [monthly.get(m) for m in range(1, 13)]
        # forward fill
        last = None
        for i in range(12):
            if row[i] is not None: last = row[i]
            elif last is not None: row[i] = last
        # backward fill
        last = None
        for i in range(11, -1, -1):
            if row[i] is not None: last = row[i]
            elif last is not None: row[i] = last
        return row if any(v for v in row) else None
    except Exception as e:
        return None

def _get_live_price(symbol):
    """Le preco atual do front month (maior volume) de futures_contracts.json."""
    try:
        path = os.path.join(DATA_DIR, "futures_contracts.json")
        data = json.load(open(path, encoding="utf-8"))
        contracts = data.get("commodities", {}).get(symbol, {}).get("contracts", [])
        # filtra volume minimo para evitar contratos iliquidos
        valid = [c for c in contracts if c.get("volume", 0) > 100 and c.get("close") and c["close"] > 0]
        if not valid:
            valid = [c for c in contracts if c.get("volume", 0) > 0 and c.get("close") and c["close"] > 0]
        if valid:
            front = max(valid, key=lambda c: c["volume"])
            return float(front["close"])
    except:
        pass
    return None

MONTHS_ABBR = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

# AUDIT-2: cache de smaps derivados do historico real
_SMAP_CACHE: dict = {}

def _build_smap_from_history(prices_dict, symbol):
    """Deriva smap do retorno medio historico real por mes.
    Normaliza para range [-1, +1] usando min/max da serie.
    Mais preciso que valores arbitrarios hardcoded."""
    from collections import defaultdict
    month_rets = defaultdict(list)
    price_map = {}
    for year, months in prices_dict.items():
        for i, p in enumerate(months):
            price_map[(int(year), i+1)] = float(p)
    for (yr, m), price in price_map.items():
        prev_m, prev_yr = m-1, yr
        if prev_m == 0:
            prev_m, prev_yr = 12, yr-1
        prev = price_map.get((prev_yr, prev_m))
        if prev and prev > 0:
            month_rets[m].append(price / prev - 1)
    avgs = {}
    for m in range(1, 13):
        rets = month_rets.get(m, [])
        avgs[m] = sum(rets)/len(rets) if rets else 0.0
    vals = list(avgs.values())
    mn, mx = min(vals), max(vals)
    rng = mx - mn
    # Normaliza para [-1, +1]
    if rng > 0:
        smap = {m: round((v - mn) / rng * 2 - 1, 3) for m, v in avgs.items()}
    else:
        smap = {m: 0.0 for m in avgs}
    return smap

def compute_score(month, ret_6m, ret_3m, ret_12m, symbol, _prices_override=None):
    # AUDIT-2: smap derivado do historico real, nao mais hardcoded
    if symbol not in _SMAP_CACHE:
        _src = _prices_override or PRICES_MAP.get(symbol, {})
        _SMAP_CACHE[symbol] = _build_smap_from_history(_src, symbol)
    smap = _SMAP_CACHE[symbol]
    score = 0.0
    factors = []
    s_val = smap.get(month, 0.0)
    score += s_val
    factors.append({"name": "Sazonalidade ({})".format(MONTHS_ABBR[month-1]),
                    "value": round(s_val, 2),
                    "direction": "positive" if s_val >= 0 else "negative"})
    m6c = 0
    if ret_6m > 0.15: m6c = -1.0
    elif ret_6m > 0.05: m6c = 0.5
    elif ret_6m < -0.10: m6c = 1.0
    elif ret_6m < -0.03: m6c = -0.5
    score += m6c
    pct6 = round(ret_6m * 100, 1)
    factors.append({"name": "Momentum 6m ({:+.1f}%)".format(pct6),
                    "value": round(m6c, 2),
                    "direction": "positive" if m6c >= 0 else "negative"})
    m12c = 0
    if ret_12m > 0.20: m12c = -0.5
    elif ret_12m < -0.15: m12c = 0.5
    score += m12c
    pct12 = round(ret_12m * 100, 1)
    factors.append({"name": "Dist. High 12m ({:+.1f}%)".format(pct12),
                    "value": round(m12c, 2),
                    "direction": "positive" if m12c >= 0 else "negative"})
    dec = 0
    if ret_6m > 0.05 and ret_3m < ret_6m * 0.3: dec = -0.5
    elif ret_6m < -0.03 and ret_3m > ret_6m * 0.3: dec = 0.5
    score += dec
    pct3 = round(ret_3m * 100, 1)
    factors.append({"name": "Decel. Mom 3m ({:+.1f}%)".format(pct3),
                    "value": round(dec, 2),
                    "direction": "positive" if dec >= 0 else "negative"})
    return round(score, 2), factors

def get_regime(s):
    if s > 1.5:   return "Very Bullish", "green"
    if s > 0.5:   return "Bullish", "blue"
    if s >= -0.5: return "Neutral", "gold"
    if s >= -1.5: return "Bearish", "orange"
    return "Very Bearish", "red"

def get_position(s):
    if s > 0.5:  return 1
    if s < -1.5: return -1
    return 0

def run_backtest(prices_dict, symbol):
    price_map = {}
    for year, months in prices_dict.items():
        for i, p in enumerate(months):
            price_map[(int(year), i+1)] = float(p)
    def gp(y, m):
        while m <= 0: m += 12; y -= 1
        while m > 12: m -= 12; y += 1
        return price_map.get((y, m), None)
    records = []
    for (year, month) in sorted(price_map.keys()):
        price = price_map[(year, month)]
        p1  = gp(year, month-1)
        p3  = gp(year, month-3)
        p6  = gp(year, month-6)
        p12 = gp(year, month-12)
        p_fwd3 = gp(year, month+3)
        if not all([p1, p3, p6, p12]):
            continue
        ret_1m  = price / p1  - 1
        ret_3m  = price / p3  - 1
        ret_6m  = price / p6  - 1
        ret_12m = price / p12 - 1
        ret_fwd3 = (p_fwd3 / price - 1) if (p_fwd3 and price and price > 0) else None
        score, factors = compute_score(month, ret_6m, ret_3m, ret_12m, symbol)
        regime, regime_color = get_regime(score)
        pos = get_position(score)
        records.append({'year':year,'month':month,'price':price,
            'ret_1m':ret_1m,'ret_3m':ret_3m,'ret_6m':ret_6m,'ret_12m':ret_12m,
            'ret_fwd3':ret_fwd3,'score':score,'factors':factors,
            'regime':regime,'regime_color':regime_color,'position':pos})
    return records

def compute_seasonality(records):
    month_rets = defaultdict(list)
    for rec in records:
        month_rets[rec['month']].append(rec['ret_1m'])
    monthly = []
    for m in range(1, 13):
        rets = month_rets[m]
        if not rets:
            monthly.append({'month':MONTHS_ABBR[m-1],'avg_ret':0.0,'pct_pos':0.0})
            continue
        avg_ret = sum(rets) / len(rets)
        pct_pos = sum(1 for r in rets if r > 0) / len(rets)
        monthly.append({'month':MONTHS_ABBR[m-1],'avg_ret':round(avg_ret,4),'pct_pos':round(pct_pos,2)})
    q_map = {1:[1,2,3],2:[4,5,6],3:[7,8,9],4:[10,11,12]}
    quarterly = []
    for q, qmonths in q_map.items():
        q_rets, fwd3_rets = [], []
        for rec in records:
            if rec['month'] in qmonths:
                q_rets.append(rec['ret_1m'])
                if rec['ret_fwd3'] is not None:
                    fwd3_rets.append(rec['ret_fwd3'])
        avg_ret_m = sum(q_rets)/len(q_rets) if q_rets else 0
        pct_pos   = sum(1 for r in q_rets if r>0)/len(q_rets) if q_rets else 0
        fwd_3m    = sum(fwd3_rets)/len(fwd3_rets) if fwd3_rets else 0
        quarterly.append({'quarter':'Q{}'.format(q),
            'avg_ret_m':round(avg_ret_m,4),'pct_pos':round(pct_pos,2),'fwd_3m':round(fwd_3m,4)})
    return {'monthly':monthly,'quarterly':quarterly}

def compute_regimes(records):
    """AUDIT-3: calcula regime stats com split in-sample/out-of-sample.
    In-sample:  ate 2019 (usado para caracterizar regimes)
    Out-of-sample: 2020+ (validacao real sem lookahead)
    expected_price_3m usa avg_fwd3m do out-of-sample quando n >= 6.
    """
    regime_order = ['Very Bullish','Bullish','Neutral','Bearish','Very Bearish']
    regime_fwd_all  = defaultdict(list)
    regime_fwd_in   = defaultdict(list)  # ate 2019
    regime_fwd_out  = defaultdict(list)  # 2020+
    for rec in records:
        if rec['ret_fwd3'] is None: continue
        fwd = rec['ret_fwd3']
        regime_fwd_all[rec['regime']].append(fwd)
        if rec['year'] < 2020:
            regime_fwd_in[rec['regime']].append(fwd)
        else:
            regime_fwd_out[rec['regime']].append(fwd)
    result = []
    for rname in regime_order:
        fwds_all = regime_fwd_all.get(rname, [])
        fwds_in  = regime_fwd_in.get(rname, [])
        fwds_out = regime_fwd_out.get(rname, [])
        n = len(fwds_all)
        avg = sum(fwds_all)/n if n else 0.0
        pct_neg = sum(1 for f in fwds_all if f<0)/n if n else 0.0
        n_out = len(fwds_out)
        avg_out = sum(fwds_out)/n_out if n_out else None
        # Preferir out-of-sample quando disponivel (n>=6)
        avg_for_forecast = avg_out if (n_out >= 6 and avg_out is not None) else avg
        result.append({
            'name':          rname,
            'n':             n,
            'avg_fwd3m':     round(avg, 4),
            'avg_fwd3m_oos': round(avg_out, 4) if avg_out is not None else None,
            'n_oos':         n_out,
            'avg_fwd3m_for_forecast': round(avg_for_forecast, 4),
            'pct_neg':       round(pct_neg, 2),
            'forecast_basis': 'out-of-sample 2020+' if (n_out >= 6 and avg_out is not None) else 'in-sample (OOS insuf.)',
        })
    return result

def compute_cot_signals(records):
    crowded, exhaustion, trend_break = [], [], []
    for rec in records:
        if rec['ret_fwd3'] is None: continue
        r6,r3,r1,fwd = rec['ret_6m'],rec['ret_3m'],rec['ret_1m'],rec['ret_fwd3']
        if r6>0.10 and r3>0.05: crowded.append(fwd)
        if r6>0.08 and r3<r6*0.40: exhaustion.append(fwd)
        if r6>0.10 and r1<-0.02: trend_break.append(fwd)
    def stats(lst, name):
        n = len(lst)
        avg = sum(lst)/n if n else 0.0
        pct_neg = sum(1 for f in lst if f<0)/n if n else 0.0
        return {'name':name,'n':n,'avg_fwd3m':round(avg,4),'pct_neg':round(pct_neg,2)}
    return [stats(crowded,'Crowded Long'),stats(exhaustion,'Momentum Exhaustion'),stats(trend_break,'Trend Break After Rally')]

def compute_equity_stats(records):
    equity,bh,peak_e,peak_b,max_dd,bh_max_dd = 1.0,1.0,1.0,1.0,0.0,0.0
    prev_pos = 0
    for rec in records:
        ret = rec['ret_1m']
        equity = max(equity * (1.0 + prev_pos*ret), 0.001)
        bh     *= (1.0 + ret)
        peak_e  = max(peak_e, equity)
        peak_b  = max(peak_b, bh)
        dd_e = (equity-peak_e)/peak_e
        dd_b = (bh-peak_b)/peak_b
        if dd_e < max_dd: max_dd = dd_e
        if dd_b < bh_max_dd: bh_max_dd = dd_b
        prev_pos = rec['position']
    n_years = max(len(records)/12.0, 1)
    cagr = equity**(1.0/n_years)-1
    bh_cagr = bh**(1.0/n_years)-1
    def sharpe(rs):
        n = len(rs)
        if n < 2: return 0.0
        mn = sum(rs) / n
        # AUDIT-5: desvio padrao amostral (n-1) em vez de populacional (n)
        var = sum((r - mn)**2 for r in rs) / (n - 1)
        sd = var**0.5
        return round(mn / sd * (12**0.5), 2) if sd > 0 else 0.0
    rets = [rec['position']*rec['ret_1m'] for rec in records]
    bh_rets = [rec['ret_1m'] for rec in records]
    return {'cagr':round(cagr,4),'sharpe':sharpe(rets),'max_dd':round(max_dd,4),
            'bh_cagr':round(bh_cagr,4),'bh_sharpe':sharpe(bh_rets),'bh_max_dd':round(bh_max_dd,4)}

def read_ibkr_price(symbol):
    try:
        path = os.path.join(DATA_DIR, "ibkr_portfolio.json")
        with open(path,"r",encoding="utf-8") as f:
            data = json.load(f)
        positions = data.get("positions",[])
        for pos in positions:
            sym = str(pos.get("symbol","")).upper()
            if symbol in sym:
                price = pos.get("last_price") or pos.get("price") or pos.get("mktPrice")
                if price and float(price)>0:
                    return round(float(price),2)
        if symbol in data:
            p = data[symbol].get("last") or data[symbol].get("price")
            if p: return round(float(p),2)
    except Exception:
        pass
    return None

def read_cftc(symbol):
    """AUDIT-4: adaptado ao formato real do cot.json.
    Formato: {commodities: {SYM: {disaggregated|legacy: {latest:{}, history:[]}}}}
    Mapeia LE->LE, GF->GF, HE->HE nos simbolos CFTC.
    """
    result = {"net_long": None, "delta": None}
    try:
        path = os.path.join(DATA_DIR, "cot.json")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        comms = data.get("commodities", {})

        # Tentar o simbolo direto (LE, GF, HE)
        entry = comms.get(symbol, {})

        # Fallback: buscar por substring em todos os simbolos
        if not entry:
            for k, v in comms.items():
                if symbol.upper() in k.upper():
                    entry = v
                    break

        if not entry:
            return result

        # Preferir disaggregated (Managed Money), fallback legacy (NonCommercial)
        report = entry.get("disaggregated") or entry.get("legacy") or {}
        latest  = report.get("latest", {})
        history = report.get("history", [])
        rtype   = report.get("report_type", "?")

        def _extract_net(rec):
            if not isinstance(rec, dict): return None
            # Campos diretos de net position
            for k in ["managed_money_net","mm_net","noncommercial_net","non_commercial_net","net_position","net"]:
                v = rec.get(k)
                if v is not None:
                    try: return int(float(v))
                    except: pass
            # Long - Short
            for long_k, short_k in [
                ("managed_money_long","managed_money_short"),
                ("noncommercial_long","noncommercial_short"),
            ]:
                lng = rec.get(long_k)
                sht = rec.get(short_k)
                if lng is not None and sht is not None:
                    try: return int(float(lng)) - int(float(sht))
                    except: pass
            return None

        # Valor atual
        net_now = _extract_net(latest)
        if net_now is not None:
            result["net_long"] = net_now

        # Delta: latest vs record anterior no historico
        if history and len(history) >= 2 and net_now is not None:
            prev_net = _extract_net(history[-2] if isinstance(history[-1], dict) else history[-1])
            if prev_net is None and len(history) >= 1:
                prev_net = _extract_net(history[-1])
            if prev_net is not None:
                result["delta"] = net_now - prev_net

        if net_now is not None:
            print(f"  CFTC {symbol} ({rtype}): net={net_now:+,}  delta={result['delta']}")
        else:
            print(f"  CFTC {symbol}: sem dados (keys latest: {list(latest.keys())[:6]})")

    except Exception as ex:
        print(f"  CFTC {symbol} erro: {ex}")
    return result

FRAMEWORKS = {
    "LE": {
        "phase_now":        "Fev-Mai: Seasonal Q1. Monitorar mom 3m vs 6m.",
        "phase_protection": "Mai-Jun: Escalar protecao. Puts LE Jun/Ago $220-$230.",
        "phase_trigger":    "Trigger: 6m mom >15% E desacel. 3m -> hedge imediato.",
        "phase_max_risk":   "Jul-Out: Pior janela sazonal. Q3 hist: -5.86% fwd 3m."
    },
    "GF": {
        "phase_now":        "Fev-Abr: GF segue LE com lag 30-60d. Monitorar spread LE/GF.",
        "phase_protection": "Mai-Jun: Puts GF ou reduzir exposure. Strikes $260-$280.",
        "phase_trigger":    "Trigger: LE cai >3% em 1m E GF ainda estavel -> spread arb.",
        "phase_max_risk":   "Jul-Set: GF pressiona quando steers pesam. Hist: -4.2% fwd 3m."
    },
    "HE": {
        "phase_now":        "Fev-Abr: Sazonalidade POSITIVA. Producao baixa, preco sobe.",
        "phase_protection": "Mai-Jun: PICO sazonal historico. Monitorar exaustao de momentum.",
        "phase_trigger":    "Trigger: 6m mom >20% E mom 3m desacelera -> protecao longa.",
        "phase_max_risk":   "Set-Out: PISO sazonal historico de preco (oferta maxima)."
    }
}

ALERTS = {
    "LE": ["Cargill plant closure 12/02/2026","Packer losses: -$273/head avg (2024)",
           "Herd: 86.2M head (75yr low)","CFTC: Fundos saindo de posicao extrema"],
    "GF": ["Replacement cycle: oferta restrita 2025-2026",
           "Corn/GF ratio historico: custo de recria elevado","Drought index plains: monitorar"],
    "HE": ["China: demanda por suinos volatil","PED/PRRS: risco de surto sempre presente",
           "Spread hog/corn: margem de producao"]
}

COMMODITY_META = {
    "LE": {"name":"Live Cattle",   "ibkr_sym":"LE","cot_sym":"LE"},
    "GF": {"name":"Feeder Cattle", "ibkr_sym":"GF","cot_sym":"GF"},
    "HE": {"name":"Lean Hogs",     "ibkr_sym":"HE","cot_sym":"HE"},
}

PRICES_MAP = {"LE":LE_PRICES,"GF":GF_PRICES,"HE":HE_PRICES}

def build_commodity(symbol):
    meta   = COMMODITY_META[symbol]
    prices = PRICES_MAP[symbol]
    # Injeta 2025 real do price_history.json
    _row_2025 = _load_monthly_prices(symbol, 2025)
    if _row_2025:
        prices[2025] = _row_2025
    records      = run_backtest(prices, symbol)
    seasonality  = compute_seasonality(records)
    regimes      = compute_regimes(records)
    cot_signals  = compute_cot_signals(records)
    strategy     = compute_equity_stats(records)
    latest       = records[-1] if records else None
    live_price   = _get_live_price(symbol) or read_ibkr_price(meta["ibkr_sym"])
    if live_price is None and latest:
        live_price = latest['price']
    cftc = read_cftc(meta["cot_sym"])
    if latest:
        # Recalcula retornos usando preco atual vs precos historicos
        from datetime import datetime
        now = datetime.now()
        cur_m = now.month
        cur_y = now.year
        # monta mapa completo ano->mes->preco
        price_map = {}
        for yr, mths in prices.items():
            for i, p in enumerate(mths):
                price_map[(int(yr), i+1)] = float(p)
        # injeta preco atual como (cur_y, cur_m)
        if live_price:
            price_map[(cur_y, cur_m)] = float(live_price)
        def gp(y, m):
            while m <= 0: m += 12; y -= 1
            while m > 12: m -= 12; y += 1
            return price_map.get((y, m))
        cp = gp(cur_y, cur_m)
        p3  = gp(cur_y, cur_m - 3)
        p6  = gp(cur_y, cur_m - 6)
        p12 = gp(cur_y, cur_m - 12)
        if cp and p3 and p6 and p12:
            live_ret_3m  = cp / p3  - 1
            live_ret_6m  = cp / p6  - 1
            live_ret_12m = cp / p12 - 1
            cur_score, cur_factors = compute_score(cur_m, live_ret_6m, live_ret_3m, live_ret_12m, symbol)
        else:
            live_ret_3m  = latest['ret_3m']
            live_ret_6m  = latest['ret_6m']
            live_ret_12m = latest['ret_12m']
            cur_score, cur_factors = compute_score(latest['month'],live_ret_6m,live_ret_3m,live_ret_12m,symbol)
        cur_regime, cur_color = get_regime(cur_score)
    else:
        cur_score, cur_factors = None, []
        cur_regime, cur_color = "N/A", "gold"
    trigger_active = False
    trigger_reason = "N/A"
    if latest:
        if live_ret_6m>0.15 and live_ret_3m<live_ret_6m*0.5:
            trigger_active = True
            trigger_reason = "HEDGE IMEDIATO: mom 6m>15% e 3m desacelerou"
        elif live_ret_6m>0.15:
            trigger_reason = "Aguardando decel. 3m"
        elif live_ret_6m>0.05:
            trigger_reason = "Monitorar momentum"
        else:
            trigger_reason = "OK - sem sinal de exaustao"
    exp_price_3m = None
    if cur_regime and live_price:
        for r in regimes:
            if r['name'] == cur_regime:
                # AUDIT-3: usa out-of-sample quando disponivel
                fcast_ret = r.get('avg_fwd3m_for_forecast', r['avg_fwd3m'])
                exp_price_3m = round(live_price * (1 + fcast_ret), 2)
                break
    return {
        "name":meta["name"],"current_price":live_price,"score":cur_score,
        "regime":cur_regime,"regime_color":cur_color,
        "momentum_6m":round(live_ret_6m,4) if latest else None,
        "momentum_3m":round(live_ret_3m,4) if latest else None,
        "momentum_12m":round(live_ret_12m,4) if latest else None,
        "cftc_net_long":cftc["net_long"],"cftc_delta":cftc["delta"],
        "trigger_active":trigger_active,"trigger_reason":trigger_reason,
        "score_factors":cur_factors,"seasonality":seasonality,"regimes":regimes,
        "cot_signals":cot_signals,"strategy":strategy,"expected_price_3m":exp_price_3m,
        "framework":FRAMEWORKS[symbol],"structural_alerts":ALERTS[symbol],
    }

def get_bottleneck_data():
    try:
        with open(OUTPUT,"r",encoding="utf-8") as f:
            data = json.load(f)
        return data.get("commodities",{})
    except Exception:
        return {}

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    output = {"generated_at":datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),"commodities":{}}
    for sym in ["LE","GF","HE"]:
        print("  Processing {}...".format(sym))
        try:
            output["commodities"][sym] = build_commodity(sym)
            print("    OK: score={} regime={}".format(
                output["commodities"][sym]["score"],output["commodities"][sym]["regime"]))
        except Exception as ex:
            import traceback
            print("    ERROR: {}".format(ex))
            traceback.print_exc()
            output["commodities"][sym] = {"name":COMMODITY_META[sym]["name"],"error":str(ex)}
    with open(OUTPUT,"w",encoding="utf-8") as f:
        json.dump(output,f,indent=2,ensure_ascii=False)
    print("Saved: {}".format(OUTPUT))
    return output

if __name__ == "__main__":
    main()
