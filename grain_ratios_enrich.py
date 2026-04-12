#!/usr/bin/env python3
"""
grain_ratios_enrich.py
======================
Pos-processador: le grain_ratios.json e injeta COT + FOB corretos.
Nao modifica grain_ratio_engine.py.

Executar DEPOIS de grain_ratio_engine.py:
    python grain_ratio_engine.py
    python grain_ratios_enrich.py

Estrutura COT confirmada:
    cot.json -> {"commodities": {"ZC": {"disaggregated": {"latest":{}, "history":[]}}}}

Estrutura FOB confirmada:
    physical_intl.json -> {"international": {"ZS_BR": {"price":127.27, "price_unit":"BRL/saca"}}}
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

BASE  = Path("agrimacro-dash/public/data/processed")
OUT   = BASE / "grain_ratios.json"
COT   = BASE / "cot.json"
PHYS  = BASE / "physical_intl.json"
BCB   = BASE / "bcb_data.json"

def log(msg): print(msg)

log("=" * 55)
log("GRAIN RATIOS ENRICH -- COT + FOB injector")
log(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
log("=" * 55)

if not OUT.exists():
    log("ERRO: grain_ratios.json nao encontrado. Rode grain_ratio_engine.py primeiro.")
    raise SystemExit(1)

with open(OUT, encoding="utf-8") as f:
    gr = json.load(f)

# â”€â”€ BRL/USD â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
brl_usd = None
brl_usd_is_fallback = True
try:
    if BCB.exists():
        with open(BCB, encoding="utf-8") as f:
            bcb = json.load(f)
        if isinstance(bcb, dict):
            # Estrutura BCB/SGS: {brl_usd: [{date, value}, ...]}
            for k in ["brl_usd","usd_brl","ptax","cambio","exchange_rate"]:
                v = bcb.get(k)
                if isinstance(v, list) and v:
                    # Pegar ultimo valor da serie
                    last = v[-1] if isinstance(v[-1], dict) else {}
                    fv = float(last.get("value", 0))
                    if 3 < fv < 10:
                        brl_usd = fv
                        brl_usd_is_fallback = False
                        break
                elif v is not None:
                    try:
                        fv = float(str(v).replace(",",".").split()[0])
                        if 3 < fv < 10:
                            brl_usd = fv
                            brl_usd_is_fallback = False
                            break
                    except: pass
            # Tentar resumo_cambio se existir
            if brl_usd is None:
                resumo = bcb.get("resumo_cambio", {})
                if isinstance(resumo, dict):
                    for k in ["usd_brl_last","usd_brl","ultimo"]:
                        v = resumo.get(k)
                        if v:
                            try:
                                fv = float(v)
                                if 3 < fv < 10:
                                    brl_usd = fv
                                    brl_usd_is_fallback = False
                                    break
                            except: pass
    if brl_usd is None:
        brl_usd = 5.8  # fallback conservador
        log("BRL/USD: usando fallback 5.80 (BCB indisponivel)")
    else:
        log(f"BRL/USD: {brl_usd:.2f} (BCB/SGS real)")
except Exception as e:
    brl_usd = 5.8
    log(f"BRL/USD fallback 5.80 ({e})")

# ============================================================
# COT
# ============================================================
log("\n--- COT ---")

current_cot = {}

try:
    if not COT.exists():
        log("cot.json nao encontrado")
    else:
        with open(COT, encoding="utf-8") as f:
            cot_raw = json.load(f)

        comms = cot_raw.get("commodities", {})
        log(f"Simbolos: {list(comms.keys())}")

        for sym, grain in [("ZC","corn"), ("ZS","soy"), ("ZW","wheat")]:
            entry = comms.get(sym, {})

            # Preferir disaggregated (tem Managed Money)
            # Fallback para legacy (tem NonCommercial como proxy)
            report = entry.get("disaggregated") or entry.get("legacy") or {}
            rtype  = report.get("report_type", "?")
            latest = report.get("latest", {})
            history = report.get("history", [])

            # Campos net por tipo
            if rtype == "disaggregated":
                net_keys   = ["managed_money_net", "mm_net"]
                long_key   = "managed_money_long"
                short_key  = "managed_money_short"
            else:
                net_keys   = ["noncommercial_net", "non_commercial_net", "mm_net"]
                long_key   = "noncommercial_long"
                short_key  = "noncommercial_short"

            def extract_mm(rec):
                if not isinstance(rec, dict): return None
                for k in net_keys + ["net", "net_position"]:
                    v = rec.get(k)
                    if v is not None:
                        try: return float(v)
                        except: pass
                # long - short
                try:
                    lng = float(rec.get(long_key, 0) or 0)
                    sht = float(rec.get(short_key, 0) or 0)
                    if lng != 0 or sht != 0:
                        return lng - sht
                except: pass
                return None

            def extract_date(rec):
                if not isinstance(rec, dict): return None
                for k in ["date", "report_date", "as_of_date", "week_ending", "week"]:
                    v = rec.get(k)
                    if v:
                        try:
                            dt = pd.to_datetime(v)
                            if pd.notna(dt): return dt
                        except: pass
                return None

            # Construir serie historica
            rows = []
            for rec in (history or []):
                dt = extract_date(rec)
                mm = extract_mm(rec)
                if dt and mm is not None:
                    rows.append({"date": dt, "mm_net": mm})

            # Adicionar latest
            dt_l = extract_date(latest)
            mm_l = extract_mm(latest)
            if dt_l and mm_l is not None:
                rows.append({"date": dt_l, "mm_net": mm_l})

            if not rows:
                log(f"  {sym} ({rtype}): sem dados. Latest keys: {list(latest.keys())[:8]}")
                # Mostrar amostra para debug
                for k, v in list(latest.items())[:5]:
                    log(f"    {k}: {str(v)[:60]}")
                continue

            s = (pd.DataFrame(rows)
                   .assign(date=lambda x: pd.to_datetime(x["date"]))
                   .dropna()
                   .sort_values("date")
                   .drop_duplicates("date")
                   .set_index("date")["mm_net"])
            s.index = s.index.to_period("M").to_timestamp()
            s = s.groupby(level=0).last()

            # COT Index: percentil 3 anos
            w = min(36, max(4, len(s) // 3))
            mn = s.rolling(w, min_periods=4).min()
            mx = s.rolling(w, min_periods=4).max()
            rng = mx - mn
            idx = ((s - mn) / rng * 100).where(rng > 0, 50.0).clip(0, 100)

            lmm  = float(s.iloc[-1])
            lidx = float(idx.dropna().iloc[-1]) if idx.dropna().any() else 50.0

            _n   = len(s)
            _win = min(36, max(4, _n))
            # AUDIT-B: documentar janela real vs ideal
            _warn = (f"Janela comprimida: {_win}m de 36 ideais â€” ampliar historico cot.json"
                     if _n < 36 else None)
            if _warn:
                log(f"  AVISO {sym}: {_warn}")
            current_cot[grain] = {
                "mm_net":            round(lmm, 0),
                "cot_index":         round(lidx, 1),
                "signal":            "BULL" if lidx < 20 else ("BEAR" if lidx > 80 else "NEUTRO"),
                "report_type":       rtype,
                "cot_window_months": _win,
                "cot_n_history":     _n,
                "cot_warning":       _warn,
            }
            log(f"  {sym} ({rtype:<15}) mm={lmm:+.0f}  idx={lidx:.0f}  n={_n}  win={_win}m")

except Exception as e:
    import traceback
    log(f"COT ERRO: {e}")
    log(traceback.format_exc()[:400])

# ============================================================
# FOB
# ============================================================
log("\n--- FOB ---")

fob_data = {
    "paranagua": {"soy": None, "corn": None},
    "rosario":   {"soy": None, "corn": None, "wheat": None},
}

try:
    if not PHYS.exists():
        log("physical_intl.json nao encontrado")
    else:
        with open(PHYS, encoding="utf-8") as f:
            phys = json.load(f)

        intl = phys.get("international", {}) if isinstance(phys, dict) else {}
        log(f"Chaves international: {list(intl.keys())}")

        def convert_price(price, unit, commodity="soy"):
            if price is None: return None
            try: p = float(price)
            except: return None
            if p <= 0: return None
            u = str(unit).upper().strip()

            # BRL/saca (60kg por saca)
            if "BRL" in u or "R$" in u:
                if p < 3000:  # BRL/saca
                    result = round(p / brl_usd / 0.06, 2)
                    log(f"    Convertido: {p:.2f} BRL/saca -> {result:.2f} USD/ton (BRL={brl_usd:.2f})")
                    return result
                else:         # BRL/ton
                    return round(p / brl_usd, 2)

            # USD/bushel
            if "BU" in u or "BUSHEL" in u:
                factor = 36.744 if "soy" in commodity else 39.368
                return round(p * factor, 2)

            # USD/ton direto
            if 50 < p < 2000:
                return round(p, 2)

            # Heuristicas
            if p < 50:   # provavelmente USD/bu
                factor = 36.744 if commodity == "soy" else 39.368
                return round(p * factor, 2)
            if p > 2000: # provavelmente BRL/ton
                return round(p / brl_usd, 2)

            return round(p, 2)

        for sym_key, origin, commodity in [
            ("ZS_BR", "paranagua", "soy"),
            ("ZC_BR", "paranagua", "corn"),
            ("ZS_AR", "rosario",   "soy"),
            ("ZC_AR", "rosario",   "corn"),
            ("ZW_AR", "rosario",   "wheat"),
        ]:
            rec = intl.get(sym_key)
            if rec is None:
                log(f"  {sym_key}: chave nao encontrada")
                continue
            if isinstance(rec, list):
                rec = rec[0] if rec else {}
            if not isinstance(rec, dict):
                continue

            price = rec.get("price") or rec.get("value") or rec.get("last")
            unit  = rec.get("price_unit") or rec.get("unit") or ""
            log(f"  {sym_key}: price={price}  unit={unit}")

            converted = convert_price(price, unit, commodity)
            if converted:
                fob_data[origin][commodity] = converted
                log(f"    -> {converted:.2f} USD/ton")

except Exception as e:
    import traceback
    log(f"FOB ERRO: {e}")
    log(traceback.format_exc()[:300])

# ============================================================
# RECALCULAR CIF QINGDAO COM DADOS CORRETOS
# ============================================================
log("\n--- CIF Qingdao ---")

bdi_raw = gr.get("arbitrage", {}).get("bdi", {})
bdi_val = bdi_raw.get("value") if isinstance(bdi_raw, dict) else None
bdi_is_fallback = bdi_val is None
if bdi_is_fallback:
    log("BDI: fonte indisponivel — frete sera null (ZERO MOCK: sem valor fabricado)")
    freight_gulf   = None
    freight_santos = None
else:
    freight_gulf   = round(bdi_val * 0.013 + 2.8, 1)
    freight_santos = round(bdi_val * 0.017 + 3.5, 1)
log(f"BDI={bdi_val}  Frete Gulf={freight_gulf}  Santos={freight_santos}  is_fallback={bdi_is_fallback}")

# Precos futuros do snapshot
snap = gr.get("current_snapshot", {})
prices = snap.get("prices", {})

def fut_to_ton(grain, dollar_per_bu):
    if dollar_per_bu is None: return None
    factor = {"corn": 39.368, "soy": 36.744, "wheat": 36.744}.get(grain, 36.744)
    return round(float(dollar_per_bu) * factor, 1)

corn_cme  = fut_to_ton("corn",  prices.get("corn"))
soy_cme   = fut_to_ton("soy",   prices.get("soy"))
wheat_cme = fut_to_ton("wheat", prices.get("wheat"))
log(f"CME ($/ton): corn={corn_cme} soy={soy_cme} wheat={wheat_cme}")

# Basis Gulf (cents/bu -> USD/ton)
basis_gulf = gr.get("arbitrage", {}).get("basis_gulf", {})
bg_corn  = (basis_gulf.get("corn")  or 0) / 100 * 39.368
bg_soy   = (basis_gulf.get("soy")   or 0) / 100 * 36.744
bg_wheat = (basis_gulf.get("wheat") or 0) / 100 * 36.744

fob_gulf_corn  = round((corn_cme  or 0) + bg_corn,  1) if corn_cme  else None
fob_gulf_soy   = round((soy_cme   or 0) + bg_soy,   1) if soy_cme   else None
fob_gulf_wheat = round((wheat_cme or 0) + bg_wheat,  1) if wheat_cme else None

# FOBs
# AUDIT-4: FOB fallback rastreado com flags de estimativa
_fob_est = {}
fob_br_soy  = fob_data["paranagua"]["soy"]  or (_fob_est.update({"paranagua_soy":"CME*0.97"})  or (round(soy_cme  * 0.97, 1) if soy_cme  else None))
fob_br_corn = fob_data["paranagua"]["corn"] or (_fob_est.update({"paranagua_corn":"CME*0.96"}) or (round(corn_cme * 0.96, 1) if corn_cme else None))
fob_ar_soy  = fob_data["rosario"]["soy"]    or (_fob_est.update({"rosario_soy":"CME*0.95"})    or (round(soy_cme  * 0.95, 1) if soy_cme  else None))
fob_ar_corn = fob_data["rosario"]["corn"]   or (_fob_est.update({"rosario_corn":"CME*0.94"})   or (round(corn_cme * 0.94, 1) if corn_cme else None))
if _fob_est:
    log(f"AVISO FOB estimado: {_fob_est}")

log(f"FOB Gulf: soy={fob_gulf_soy} corn={fob_gulf_corn}")
log(f"FOB Paranagua: soy={fob_br_soy} corn={fob_br_corn}")
log(f"FOB Rosario: soy={fob_ar_soy} corn={fob_ar_corn}")

# CIF Qingdao
def safe_add(a, b):
    if a is None or b is None: return None
    return round(a + b, 1)

def safe_mul(a, b):
    if a is None or b is None: return None
    return round(a * b, 1)

cif = {
    "corn_us":  safe_add(fob_gulf_corn,  freight_gulf),
    "corn_br":  safe_add(fob_br_corn,    freight_santos),
    "corn_arg": safe_add(fob_ar_corn,    safe_mul(freight_santos, 0.92)),
    "soy_us":   safe_add(fob_gulf_soy,   freight_gulf),
    "soy_br":   safe_add(fob_br_soy,     freight_santos),
    "soy_arg":  safe_add(fob_ar_soy,     safe_mul(freight_santos, 0.92)),
    "wheat_us": safe_add(fob_gulf_wheat, freight_gulf),
}

def spread(a, b):
    return round(a - b, 1) if a and b else None

def winner(v):
    if v is None: return "N/A"
    if v > 8:     return "BR/ARG vantagem"
    elif v < -8:  return "US vantagem"
    else:         return "Paridade"

sp = {
    "soy_us_vs_br":   spread(cif["soy_us"],  cif["soy_br"]),
    "soy_us_vs_arg":  spread(cif["soy_us"],  cif["soy_arg"]),
    "corn_us_vs_br":  spread(cif["corn_us"], cif["corn_br"]),
    "corn_us_vs_arg": spread(cif["corn_us"], cif["corn_arg"]),
}

log(f"\nCIF Qingdao:")
log(f"  Soja:  US={cif['soy_us']}  BR={cif['soy_br']}  ARG={cif['soy_arg']} USD/ton")
log(f"  Milho: US={cif['corn_us']} BR={cif['corn_br']} ARG={cif['corn_arg']} USD/ton")
log(f"  Spread soja US-BR: {sp['soy_us_vs_br']} -> {winner(sp['soy_us_vs_br'])}")
log(f"  Spread milho US-BR: {sp['corn_us_vs_br']} -> {winner(sp['corn_us_vs_br'])}")

# ============================================================
# RECALCULAR SCORECARDS COM COT REAL
# ============================================================
log("\n--- Scorecards ---")

def build_scorecard(grain, snap, current_cot, cif, sp):
    signals = []
    stu_z = float(snap.get("stu", {}).get(grain, {}).get("z") or 0)
    mg    = float(snap.get("margins", {}).get(grain, {}).get("margin") or 0)
    cot_d = current_cot.get(grain, {})
    # AUDIT-1: None quando dado indisponivel â€” nao usar 50 como fallback (ZERO MOCK)
    _cot_raw = cot_d.get("cot_index") if cot_d else None
    cot_idx  = float(_cot_raw) if _cot_raw is not None else None

    # STU
    if   stu_z < -0.8: signals.append({"factor":"Stock-to-Use","signal":"BULL","detail":f"Apertado z={stu_z:.2f}","weight":3})
    elif stu_z >  0.8: signals.append({"factor":"Stock-to-Use","signal":"BEAR","detail":f"Folgado z={stu_z:.2f}","weight":3})
    else:              signals.append({"factor":"Stock-to-Use","signal":"NEUTRO","detail":f"Normal z={stu_z:.2f}","weight":1})

    # COP
    if   mg < 0:   signals.append({"factor":"Preco vs COP","signal":"BULL","detail":"Abaixo custo producao","weight":2})
    elif mg < 0.5: signals.append({"factor":"Preco vs COP","signal":"NEUTRO","detail":"Margem estreita","weight":1})
    else:          signals.append({"factor":"Preco vs COP","signal":"NEUTRO","detail":f"Margem ${mg:.2f}","weight":1})

    # COT â€” AUDIT-1: fator excluido (weight=0) quando dado nao disponivel
    if cot_idx is not None:
        if   cot_idx < 20: signals.append({"factor":"COT Index","signal":"BULL","detail":f"Fundos leves {cot_idx:.0f}/100","weight":2})
        elif cot_idx > 80: signals.append({"factor":"COT Index","signal":"BEAR","detail":f"Sobrecomprado {cot_idx:.0f}/100","weight":2})
        else:              signals.append({"factor":"COT Index","signal":"NEUTRO","detail":f"Neutro {cot_idx:.0f}/100","weight":1})
    else:
        signals.append({"factor":"COT Index","signal":"N/A","detail":"Dado CFTC indisponivel","weight":0})

    # Arbitragem origem
    if grain == "soy" and sp.get("soy_us_vs_br") is not None:
        v = sp["soy_us_vs_br"]
        if   v >  8: signals.append({"factor":"Arbitragem Origem","signal":"BEAR","detail":f"BR ${v:+.1f}/ton mais barato China","weight":1})
        elif v < -8: signals.append({"factor":"Arbitragem Origem","signal":"BULL","detail":f"US ${-v:.1f}/ton mais barato China","weight":1})
    if grain == "corn" and sp.get("corn_us_vs_br") is not None:
        v = sp["corn_us_vs_br"]
        if   v >  8: signals.append({"factor":"Arbitragem Origem","signal":"BEAR","detail":f"BR ${v:+.1f}/ton mais barato China","weight":1})
        elif v < -8: signals.append({"factor":"Arbitragem Origem","signal":"BULL","detail":f"US ${-v:.1f}/ton mais barato China","weight":1})

    # Corn/Soy ratio
    cs_pct = float(snap.get("ratios", {}).get("corn_soy", {}).get("pct") or 50)
    if grain == "corn":
        sig = "BULL" if cs_pct < 30 else "NEUTRO"
        signals.append({"factor":"Corn/Soy Ratio","signal":sig,"detail":f"P{cs_pct:.0f}%","weight":2})
    elif grain == "soy":
        sig = "BEAR" if cs_pct < 30 else "NEUTRO"
        signals.append({"factor":"Corn/Soy Ratio","signal":sig,"detail":f"P{cs_pct:.0f}%","weight":1})

    bull  = sum(s["weight"] for s in signals if s["signal"] == "BULL")
    bear  = sum(s["weight"] for s in signals if s["signal"] == "BEAR")
    total = sum(s["weight"] for s in signals)
    score = round((bull - bear) / total * 100, 1) if total else 0
    comp  = "BULL" if score > 20 else ("BEAR" if score < -20 else "NEUTRO")
    return {"signals": signals, "bull_weight": bull, "bear_weight": bear,
            "composite_score": score, "composite_signal": comp}

new_scorecards = {}
for grain in ["corn", "soy", "wheat"]:
    sc = build_scorecard(grain, snap, current_cot, cif, sp)
    new_scorecards[grain] = sc
    log(f"  {grain.upper():<8} {sc['composite_signal']:<8} score={sc['composite_score']:+.0f}")

# ============================================================
# INJETAR NO grain_ratios.json
# ============================================================
log("\n--- Injetando dados ---")

# COT no snapshot
if current_cot:
    gr["current_snapshot"]["cot"] = current_cot

# Arbitrage
gr["arbitrage"]["fob_paranagua"]["soy"]  = fob_data["paranagua"]["soy"]
gr["arbitrage"]["fob_paranagua"]["corn"] = fob_data["paranagua"]["corn"]
gr["arbitrage"]["fob_rosario"]["soy"]    = fob_data["rosario"]["soy"]
gr["arbitrage"]["fob_rosario"]["corn"]   = fob_data["rosario"]["corn"]
gr["arbitrage"]["fob_rosario"]["wheat"]  = fob_data["rosario"]["wheat"]

gr["arbitrage"]["spread_delivered_china"]["fob_gulf"] = {
    "corn": fob_gulf_corn, "soy": fob_gulf_soy, "wheat": fob_gulf_wheat
}
gr["arbitrage"]["spread_delivered_china"]["fob_paranagua"] = fob_data["paranagua"]
gr["arbitrage"]["spread_delivered_china"]["fob_rosario"]   = fob_data["rosario"]
gr["arbitrage"]["spread_delivered_china"]["cif_qingdao"]   = cif
gr["arbitrage"]["spread_delivered_china"]["spreads"]       = sp
gr["arbitrage"]["spread_delivered_china"]["competitive_advantage"] = {
    "soy_china":  winner(sp["soy_us_vs_br"]),
    "corn_china": winner(sp["corn_us_vs_br"]),
}
gr["arbitrage"]["spread_delivered_china"]["bdi_used"] = bdi_val
gr["arbitrage"]["spread_delivered_china"]["bdi_is_fallback"] = bdi_is_fallback
gr["arbitrage"]["spread_delivered_china"]["fob_source_type"] = {
    "paranagua_soy":  "CEPEA_real"  if fob_data["paranagua"]["soy"]  else "CME_estimado_0.97",
    "paranagua_corn": "CEPEA_real"  if fob_data["paranagua"]["corn"] else "CME_estimado_0.96",
    "rosario_soy":    "MAGyP_real"  if fob_data["rosario"]["soy"]    else "CME_estimado_0.95",
    "rosario_corn":   "MAGyP_real"  if fob_data["rosario"]["corn"]   else "CME_estimado_0.94",
}

# Scorecards
gr["scorecards"] = new_scorecards

# Metadata
gr["meta"]["enriched_at"] = datetime.now().isoformat()
gr["meta"]["enrichment_sources"] = ["cot.json (CFTC disaggregated)", "physical_intl.json (CEPEA/MAGyP)", "bcb_data.json"]

with open(OUT, "w", encoding="utf-8") as f:
    json.dump(gr, f, indent=2, ensure_ascii=False, default=str)

log(f"\nSalvo: {OUT}")
log("=" * 55)
log("RESUMO FINAL")
log("=" * 55)
if current_cot:
    for g, d in current_cot.items():
        log(f"  COT {g.upper():<8} mm={d['mm_net']:+.0f}  idx={d['cot_index']:.0f}  {d['signal']}")
else:
    log("  COT: sem dados (verificar estrutura cot.json)")
log(f"  FOB Paranagua: soy={fob_data['paranagua']['soy']}  corn={fob_data['paranagua']['corn']} USD/ton")
log(f"  FOB Rosario:   soy={fob_data['rosario']['soy']}    corn={fob_data['rosario']['corn']} USD/ton")
log(f"  CIF Soja:  US={cif['soy_us']}  BR={cif['soy_br']}  ARG={cif['soy_arg']}")
log(f"  CIF Milho: US={cif['corn_us']} BR={cif['corn_br']} ARG={cif['corn_arg']}")
log(f"  Soja vantagem: {winner(sp['soy_us_vs_br'])}")
log(f"  Milho vantagem: {winner(sp['corn_us_vs_br'])}")
log("=" * 55)

