import os, sys, json
import numpy as np
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

def calc_soy_crush(zs, zm, zl):
    if not all([zs, zm, zl]): return None
    cost = zs / 100
    rev_meal = (zm / 2000) * 44
    rev_oil = (zl / 100) * 11
    return round((rev_meal + rev_oil) - cost, 4)

def calc_ke_zw(ke, zw):
    if not all([ke, zw]): return None
    return round(ke - zw, 2)

def calc_zl_cl(zl, cl):
    if not all([zl, cl]) or cl == 0: return None
    return round((zl / 100 * 100) / cl, 4)

def calc_feedlot(le, gf, zc):
    if not all([le, gf, zc]): return None
    receita = (le / 100) * 1300
    custo_bz = (gf / 100) * 750
    custo_racao = (zc / 100) * 50
    margem = receita - custo_bz - custo_racao
    return round(margem / 13, 2)

def calc_zc_zm(zc, zm):
    if not all([zc, zm]) or zm == 0: return None
    zc_ton = (zc / 100) * (2000 / 56)
    return round(zc_ton / zm, 4)

def calc_zc_zs(zc, zs):
    if not all([zc, zs]) or zs == 0: return None
    return round(zc / zs, 4)

SPREAD_FUNCS = {
    "soy_crush": {"fn": calc_soy_crush, "keys": ["ZS","ZM","ZL"], "name": "Soy Crush", "unit": "USD/bu"},
    "ke_zw": {"fn": calc_ke_zw, "keys": ["KE","ZW"], "name": "KE-ZW", "unit": "cents/bu"},
    "zl_cl": {"fn": calc_zl_cl, "keys": ["ZL","CL"], "name": "ZL/CL", "unit": "ratio"},
    "feedlot": {"fn": calc_feedlot, "keys": ["LE","GF","ZC"], "name": "Feedlot Margin", "unit": "USD/cwt"},
    "zc_zm": {"fn": calc_zc_zm, "keys": ["ZC","ZM"], "name": "ZC/ZM", "unit": "ratio"},
    "zc_zs": {"fn": calc_zc_zs, "keys": ["ZC","ZS"], "name": "ZC/ZS Ratio", "unit": "ratio"},
}

def build_series(spread_id, price_history):
    cfg = SPREAD_FUNCS[spread_id]
    keys = cfg["keys"]
    maps = {}
    for k in keys:
        if k not in price_history: return []
        maps[k] = {r["date"]: r["close"] for r in price_history[k] if r.get("close")}
    dates = sorted(set.intersection(*[set(m.keys()) for m in maps.values()]))
    series = []
    for d in dates:
        args = [maps[k].get(d) for k in keys]
        val = cfg["fn"](*args)
        if val is not None:
            series.append({"date": d, "value": val})
    return series

def zscore(values, current, period=252):
    if len(values) < period: return None
    recent = values[-period:]
    m, s = np.mean(recent), np.std(recent)
    if s == 0: return 0
    return round((current - m) / s, 2)

def percentile(values, current):
    if not values: return None
    below = sum(1 for v in values if v < current)
    return round((below / len(values)) * 100, 1)

def regime(z, p):
    if z is not None and abs(z) > 2: return "EXTREMO"
    if p is not None and (p < 10 or p > 90): return "EXTREMO"
    if z is not None and abs(z) > 1: return "DISSONANCIA"
    if p is not None and (p < 25 or p > 75): return "DISSONANCIA"
    return "NORMAL"

def analyze_all(price_history):
    results = {"timestamp": datetime.now().isoformat(), "spreads": {}}
    for sid, cfg in SPREAD_FUNCS.items():
        series = build_series(sid, price_history)
        if not series:
            results["spreads"][sid] = {"name": cfg["name"], "status": "missing"}
            continue
        vals = [s["value"] for s in series]
        cur = vals[-1]
        z1 = zscore(vals, cur, 252)
        z3 = zscore(vals, cur, 756)
        p = percentile(vals, cur)
        r = regime(z1, p)
        results["spreads"][sid] = {
            "name": cfg["name"], "unit": cfg["unit"], "status": "ok",
            "current": cur, "date": series[-1]["date"],
            "zscore_1y": z1, "zscore_3y": z3, "percentile": p, "regime": r,
            "points": len(series)
        }
    return results

if __name__ == "__main__":
    print("=" * 50)
    print("AGRIMACRO - GATE 3: SPREADS")
    print("=" * 50)
    with open("data/raw/price_history.json") as f:
        ph = json.load(f)
    results = analyze_all(ph)
    os.makedirs("data/processed", exist_ok=True)
    with open("data/processed/spreads.json", "w") as f:
        json.dump(results, f, indent=2)
    for sid, d in results["spreads"].items():
        if d["status"] == "ok":
            print(f"  {d['name']:20s} | {d['current']:10.4f} {d['unit']:10s} | Z={d['zscore_1y']} | P={d['percentile']}% | {d['regime']}")
        else:
            print(f"  {d['name']:20s} | MISSING")
