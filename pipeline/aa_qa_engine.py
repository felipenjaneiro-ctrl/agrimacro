#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════
 AgriMacro AA+QA Engine v1.0
 Audit & Quality Assurance — Padrão Institucional
═══════════════════════════════════════════════════════════════════

 GATE 3.5 — Auditoria automática de todos os dados antes do PDF
 GATE 4B  — QA Final + Log diário

 Uso:
   python aa_qa_engine.py              # Roda auditoria completa
   python aa_qa_engine.py --force      # Gera relatório mesmo com WARNs
   python aa_qa_engine.py --dry-run    # Só mostra resultado, não bloqueia

 Severidade:
   INFO  — Apenas log
   WARN  — Publica com nota
   FLAG  — Marca "sob revisão"
   BLOCK — Impede publicação
═══════════════════════════════════════════════════════════════════
"""

import json, os, sys, re, math, traceback
from datetime import datetime, timezone
from pathlib import Path

# ── Tenta importar YAML; fallback p/ parser simples se não tiver ──
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False

# ══════════════════════════════════════════════════════════════
# CONFIGURAÇÃO DE PATHS
# ══════════════════════════════════════════════════════════════

PIPELINE_DIR = Path(__file__).parent
DATA_RAW     = PIPELINE_DIR.parent / "agrimacro-dash" / "public" / "data" / "raw"
DATA_PROC    = PIPELINE_DIR.parent / "agrimacro-dash" / "public" / "data" / "processed"
REPORTS_DIR  = PIPELINE_DIR.parent / "agrimacro-dash" / "public" / "data" / "reports"
LOGS_DIR     = PIPELINE_DIR / "logs"
SYMBOLS_FILE = PIPELINE_DIR / "symbols.yml"

LOGS_DIR.mkdir(exist_ok=True)

# ══════════════════════════════════════════════════════════════
# CARREGAMENTO DO SYMBOLS.YML
# ══════════════════════════════════════════════════════════════

def load_symbols():
    """Carrega o registro central de símbolos."""
    if not SYMBOLS_FILE.exists():
        return None
    with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
        if HAS_YAML:
            return yaml.safe_load(f)
        else:
            # Fallback: parse manual básico (ranges e display_names)
            return _parse_symbols_fallback(f.read())

def _parse_symbols_fallback(text):
    """Parser simplificado caso PyYAML não esteja instalado."""
    # Extrai ranges usando regex
    symbols = {"chicago": {}, "brasil": {}, "eia": {}, "spreads": {},
               "macro_br": {}, "language_audit": {"extreme_words": []}}
    current_section = None
    current_symbol = None

    for line in text.split("\n"):
        stripped = line.strip()
        # Seções principais
        if stripped in ("chicago:", "brasil:", "eia:", "spreads:", "macro_br:"):
            current_section = stripped.rstrip(":")
            continue
        if stripped == "language_audit:":
            current_section = "language_audit"
            continue

        # Símbolos dentro de seção
        if current_section and current_section != "language_audit":
            m = re.match(r'^  (\w+):$', line)
            if m:
                current_symbol = m.group(1)
                symbols[current_section][current_symbol] = {}
                continue

            if current_symbol and current_section in symbols:
                # Range
                rm = re.match(r'^\s+range:\s*\[(.+?),\s*(.+?)\]', line)
                if rm:
                    try:
                        symbols[current_section][current_symbol]["range"] = [
                            float(rm.group(1)), float(rm.group(2))
                        ]
                    except ValueError:
                        pass
                # Display name
                dm = re.match(r'^\s+display_name:\s*"(.+?)"', line)
                if dm:
                    symbols[current_section][current_symbol]["display_name"] = dm.group(1)
                # Unit
                um = re.match(r'^\s+unit:\s*"(.+?)"', line)
                if um:
                    symbols[current_section][current_symbol]["unit"] = um.group(1)

        # Language audit words
        if current_section == "language_audit":
            wm = re.match(r'\s+-\s*\{\s*word:\s*"(.+?)",\s*min_change_pct:\s*([\d.]+)', line)
            if wm:
                symbols["language_audit"]["extreme_words"].append({
                    "word": wm.group(1),
                    "min_change_pct": float(wm.group(2))
                })

    return symbols


# ══════════════════════════════════════════════════════════════
# CARREGAMENTO DE DADOS
# ══════════════════════════════════════════════════════════════

def load_json(filename):
    """Carrega JSON de data/raw ou data/processed."""
    for base in [DATA_RAW, DATA_PROC, PIPELINE_DIR]:
        fp = base / filename
        if fp.exists():
            with open(fp, "r", encoding="utf-8") as f:
                return json.load(f)
    return None

def load_all_data():
    """Carrega todos os JSONs do pipeline."""
    files = {
        "pr":   "price_history.json",
        "sd":   "spreads.json",
        "ed":   "eia_data.json",
        "cd":   "cot.json",
        "sw":   "stocks_watch.json",
        "bcb":  "bcb_data.json",
        "phys": "physical_intl.json",
        "cal":  "calendar.json",
        "dr":   "daily_reading.json",
        "rd":   "report_daily.json",
        "wt":   "weather_agro.json",
        "nw":   "news.json",
        "sabr": "sugar_alcohol_br.json",
    }
    data = {}
    missing = []
    for key, fname in files.items():
        d = load_json(fname)
        if d is not None:
            data[key] = d
        else:
            missing.append(fname)
    return data, missing


# ══════════════════════════════════════════════════════════════
# CLASSE PRINCIPAL — AA+QA ENGINE
# ══════════════════════════════════════════════════════════════

class AAQAEngine:
    """Motor de Auditoria e Qualidade do AgriMacro."""

    SEVERITIES = ["INFO", "WARN", "FLAG", "BLOCK"]

    def __init__(self, symbols, data, missing_files):
        self.symbols = symbols
        self.data = data
        self.missing_files = missing_files
        self.findings = []   # Lista de {severity, code, message, details}
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.date_str = datetime.now().strftime("%Y-%m-%d")

    def add(self, severity, code, message, details=None):
        """Registra um achado de auditoria."""
        self.findings.append({
            "severity": severity,
            "code": code,
            "message": message,
            "details": details or {},
            "timestamp": self.timestamp
        })

    # ──────────────────────────────────────────────────────────
    # GATE 1: Verificação de arquivos e fontes
    # ──────────────────────────────────────────────────────────

    def check_data_availability(self):
        """Verifica se todos os arquivos de dados existem."""
        critical = ["price_history.json", "bcb_data.json", "physical_intl.json"]
        important = ["spreads.json", "eia_data.json", "cot.json", "stocks_watch.json"]

        for f in self.missing_files:
            if f in critical:
                self.add("BLOCK", "MISSING_CRITICAL_DATA",
                         f"Arquivo crítico ausente: {f}",
                         {"file": f, "level": "critical"})
            elif f in important:
                self.add("FLAG", "MISSING_DATA",
                         f"Arquivo importante ausente: {f}",
                         {"file": f, "level": "important"})
            else:
                self.add("WARN", "MISSING_DATA",
                         f"Arquivo ausente: {f}",
                         {"file": f, "level": "optional"})

    # ──────────────────────────────────────────────────────────
    # GATE 2: Verificação de timestamp / freshness
    # ──────────────────────────────────────────────────────────

    def check_data_freshness(self):
        """Verifica se os dados são do dia."""
        pr = self.data.get("pr")
        if not pr:
            return

        # Checa timestamp do price_history
        meta = pr.get("_meta", {})
        collected = meta.get("collected_at", "")
        if collected:
            try:
                dt = datetime.fromisoformat(collected.replace("Z", "+00:00"))
                age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
                if age_hours > 24:
                    self.add("FLAG", "STALE_DATA",
                             f"Dados de preço com {age_hours:.0f}h de idade",
                             {"hours_old": round(age_hours, 1), "collected_at": collected})
                elif age_hours > 48:
                    self.add("BLOCK", "VERY_STALE_DATA",
                             f"Dados de preço com {age_hours:.0f}h — MUITO ANTIGO",
                             {"hours_old": round(age_hours, 1)})
            except (ValueError, TypeError):
                self.add("WARN", "NO_TIMESTAMP",
                         "Não foi possível verificar idade dos dados de preço")

    # ──────────────────────────────────────────────────────────
    # GATE 3.5a: RANGE VALIDATION (4.1)
    # ──────────────────────────────────────────────────────────

    def check_price_ranges(self):
        """Valida se preços estão dentro das faixas plausíveis."""
        pr = self.data.get("pr", {})
        if not pr or not self.symbols:
            return

        chicago_syms = self.symbols.get("chicago", {})
        for sym, cfg in chicago_syms.items():
            rng = cfg.get("range")
            if not rng:
                continue

            # Busca preço atual
            sym_data = pr.get(sym, {})
            if isinstance(sym_data, dict):
                last = sym_data.get("last") or sym_data.get("close")
            elif isinstance(sym_data, list) and sym_data:
                last = sym_data[-1].get("close") if isinstance(sym_data[-1], dict) else sym_data[-1]
            else:
                continue

            if last is None:
                continue

            try:
                last = float(last)
            except (ValueError, TypeError):
                self.add("FLAG", "INVALID_PRICE",
                         f"{cfg.get('display_name', sym)}: preço não numérico ({last})")
                continue

            lo, hi = rng
            if last < lo or last > hi:
                mid = (lo + hi) / 2
                deviation = abs(last - mid) / mid * 100
                if deviation > 50:
                    self.add("BLOCK", "RANGE_CRITICAL",
                             f"{cfg.get('display_name', sym)}: {last} FORA da faixa [{lo}-{hi}] — desvio {deviation:.0f}%",
                             {"symbol": sym, "price": last, "range": rng, "deviation_pct": round(deviation, 1)})
                else:
                    self.add("FLAG", "RANGE_ERROR",
                             f"{cfg.get('display_name', sym)}: {last} fora da faixa [{lo}-{hi}]",
                             {"symbol": sym, "price": last, "range": rng})

        # Brasil
        brasil_syms = self.symbols.get("brasil", {})
        phys = self.data.get("phys", {})
        for sym, cfg in brasil_syms.items():
            rng = cfg.get("range")
            if not rng:
                continue
            phys_data = phys.get(sym, {})
            if isinstance(phys_data, dict):
                last = phys_data.get("last") or phys_data.get("price") or phys_data.get("close")
            else:
                continue
            if last is None:
                continue
            try:
                last = float(last)
            except (ValueError, TypeError):
                continue
            lo, hi = rng
            if last < lo or last > hi:
                mid = (lo + hi) / 2
                deviation = abs(last - mid) / mid * 100
                sev = "BLOCK" if deviation > 50 else "FLAG"
                self.add(sev, "RANGE_ERROR_BR",
                         f"{cfg.get('display_name', sym)}: R${last:.2f} fora da faixa [R${lo}-R${hi}]",
                         {"symbol": sym, "price": last, "range": rng})

    # ──────────────────────────────────────────────────────────
    # GATE 3.5b: UNIT CHECK (4.2)
    # ──────────────────────────────────────────────────────────

    def check_unit_coherence(self):
        """Valida se unidades são coerentes com a categoria."""
        if not self.symbols:
            return

        unit_rules = {
            "grains":    {"chicago": ["c/bu", "USD/short_ton", "c/lb"]},
            "softs":     {"chicago": ["c/lb", "USD/mt"]},
            "livestock": {"chicago": ["c/lb"]},
            "energy":    {"chicago": ["USD/bbl", "USD/MMBtu", "USD/gal"]},
            "metals":    {"chicago": ["USD/troy_oz"]},
        }

        for section in ["chicago"]:
            for sym, cfg in self.symbols.get(section, {}).items():
                cat = cfg.get("category", "")
                unit = cfg.get("unit", "")
                rules = unit_rules.get(cat, {}).get(section, [])
                if rules and unit not in rules:
                    self.add("BLOCK", "UNIT_MISMATCH",
                             f"{sym}: unidade '{unit}' incompatível com categoria '{cat}' (esperado: {rules})",
                             {"symbol": sym, "unit": unit, "category": cat, "expected": rules})

    # ──────────────────────────────────────────────────────────
    # GATE 3.5c: SPREAD VALIDATION (4.3)
    # ──────────────────────────────────────────────────────────

    def check_spreads(self):
        """Recalcula spreads e compara com valores armazenados."""
        pr = self.data.get("pr", {})
        sd = self.data.get("sd", {})
        if not pr or not sd:
            return

        def get_last(sym):
            d = pr.get(sym, {})
            if isinstance(d, dict):
                v = d.get("last") or d.get("close")
            elif isinstance(d, list) and d:
                v = d[-1].get("close") if isinstance(d[-1], dict) else d[-1]
            else:
                return None
            try:
                return float(v)
            except (ValueError, TypeError):
                return None

        # Recalcular spreads
        calcs = {}

        zm, zl, zs = get_last("ZM"), get_last("ZL"), get_last("ZS")
        if all(v is not None for v in [zm, zl, zs]):
            calcs["soy_crush"] = (zm * 0.022) + (zl * 0.11) - zs

        ke, zw = get_last("KE"), get_last("ZW")
        if ke is not None and zw is not None:
            calcs["ke_zw"] = ke - zw

        zl_v, cl = get_last("ZL"), get_last("CL")
        if zl_v is not None and cl is not None and cl != 0:
            calcs["zl_cl"] = zl_v / cl

        le, gf, zc = get_last("LE"), get_last("GF"), get_last("ZC")
        if all(v is not None for v in [le, gf, zc]):
            calcs["feedlot"] = le - gf - zc

        if zc is not None and zm is not None and zm != 0:
            calcs["zc_zm"] = zc / zm

        if zc is not None and zs is not None and zs != 0:
            calcs["zc_zs"] = zc / zs

        if le is not None and gf is not None:
            calcs["le_gf"] = le - gf

        rb = get_last("RB")
        if rb is not None and cl is not None:
            calcs["crack"] = (rb * 42) - cl

        gc = get_last("GC")
        if gc is not None and cl is not None and cl != 0:
            calcs["gc_cl"] = gc / cl

        # Comparar com valores armazenados
        for key, calc_val in calcs.items():
            stored = sd.get(key, {})
            if isinstance(stored, dict):
                stored_val = stored.get("value") or stored.get("last")
            elif isinstance(stored, (int, float)):
                stored_val = stored
            else:
                continue

            if stored_val is None:
                continue

            try:
                stored_val = float(stored_val)
            except (ValueError, TypeError):
                continue

            if stored_val == 0 and calc_val == 0:
                continue

            denom = max(abs(stored_val), abs(calc_val), 0.001)
            diff_pct = abs(calc_val - stored_val) / denom * 100

            if diff_pct > 0.3:
                sev = "FLAG" if diff_pct < 5 else "BLOCK"
                spread_name = key
                if self.symbols:
                    sp_cfg = self.symbols.get("spreads", {}).get(key, {})
                    spread_name = sp_cfg.get("display_name", key)
                self.add(sev, "SPREAD_MISMATCH",
                         f"{spread_name}: calculado={calc_val:.4f} vs armazenado={stored_val:.4f} (diff {diff_pct:.2f}%)",
                         {"spread": key, "calculated": round(calc_val, 4),
                          "stored": round(stored_val, 4), "diff_pct": round(diff_pct, 2)})

        # Range check nos spreads calculados
        if self.symbols:
            for key, val in calcs.items():
                sp_cfg = self.symbols.get("spreads", {}).get(key, {})
                rng = sp_cfg.get("range")
                if rng:
                    lo, hi = rng
                    if val < lo or val > hi:
                        self.add("FLAG", "SPREAD_RANGE",
                                 f"{sp_cfg.get('display_name', key)}: {val:.4f} fora de [{lo}, {hi}]",
                                 {"spread": key, "value": round(val, 4), "range": rng})

    # ──────────────────────────────────────────────────────────
    # GATE 3.5d: ESTOQUES vs MÉDIA 5 ANOS (4.4)
    # ──────────────────────────────────────────────────────────

    def check_stocks(self):
        """Verifica estoques USDA vs média 5 anos."""
        sw = self.data.get("sw", {})
        if not sw:
            return

        items = sw if isinstance(sw, list) else sw.get("commodities", sw.get("data", []))
        if not isinstance(items, list):
            # Pode ser dict com commodities como chaves
            if isinstance(sw, dict):
                items = []
                for k, v in sw.items():
                    if isinstance(v, dict) and "current" in v:
                        items.append({"name": k, **v})

        for item in items:
            if not isinstance(item, dict):
                continue
            name = item.get("name", item.get("commodity", "?"))
            current = item.get("current", item.get("stocks", item.get("ending_stocks")))
            avg5 = item.get("avg_5y", item.get("average_5y", item.get("mean_5y")))

            if current is None or avg5 is None:
                continue

            try:
                current = float(current)
                avg5 = float(avg5)
            except (ValueError, TypeError):
                continue

            if avg5 == 0:
                continue

            delta = (current - avg5) / avg5 * 100

            if abs(delta) > 100:
                self.add("BLOCK", "STOCKS_EXTREME",
                         f"Estoques {name}: {current:,.0f} vs média 5a {avg5:,.0f} (delta {delta:+.0f}%)",
                         {"commodity": name, "current": current, "avg_5y": avg5, "delta_pct": round(delta, 1)})
            elif abs(delta) > 50:
                self.add("FLAG", "STOCKS_OUTLIER",
                         f"Estoques {name}: desvio de {delta:+.0f}% vs média 5 anos",
                         {"commodity": name, "current": current, "avg_5y": avg5, "delta_pct": round(delta, 1)})

    # ──────────────────────────────────────────────────────────
    # GATE 3.5e: CROSS-PAGE CONSISTENCY (4.5)
    # ──────────────────────────────────────────────────────────

    def check_cross_consistency(self):
        """Compara dados entre diferentes fontes para mesmo símbolo."""
        pr = self.data.get("pr", {})
        rd = self.data.get("rd", {})
        if not pr or not rd:
            return

        # Se report_daily tem preços, comparar com price_history
        rd_prices = rd.get("prices", rd.get("summary", {}))
        if isinstance(rd_prices, dict):
            for sym, rd_val in rd_prices.items():
                if isinstance(rd_val, dict):
                    rd_price = rd_val.get("last") or rd_val.get("close") or rd_val.get("price")
                elif isinstance(rd_val, (int, float)):
                    rd_price = rd_val
                else:
                    continue

                pr_data = pr.get(sym, {})
                if isinstance(pr_data, dict):
                    pr_price = pr_data.get("last") or pr_data.get("close")
                elif isinstance(pr_data, list) and pr_data:
                    pr_price = pr_data[-1].get("close") if isinstance(pr_data[-1], dict) else pr_data[-1]
                else:
                    continue

                if rd_price is None or pr_price is None:
                    continue

                try:
                    rd_price = float(rd_price)
                    pr_price = float(pr_price)
                except (ValueError, TypeError):
                    continue

                if pr_price == 0:
                    continue

                diff_pct = abs(rd_price - pr_price) / pr_price * 100
                if diff_pct > 0.1:
                    self.add("FLAG", "CROSS_PAGE_MISMATCH",
                             f"{sym}: report_daily={rd_price} vs price_history={pr_price} (diff {diff_pct:.2f}%)",
                             {"symbol": sym, "rd_price": rd_price, "pr_price": pr_price})

    # ──────────────────────────────────────────────────────────
    # GATE 3.5f: AUDITORIA SEMÂNTICA (4.6)
    # ──────────────────────────────────────────────────────────

    def check_language(self):
        """Verifica se linguagem é compatível com magnitude da variação."""
        dr = self.data.get("dr", {})
        pr = self.data.get("pr", {})
        if not dr or not pr or not self.symbols:
            return

        # Extrai textos do daily_reading
        texts = []
        if isinstance(dr, dict):
            for key in ["text", "summary", "analysis", "editorial", "content"]:
                val = dr.get(key)
                if isinstance(val, str):
                    texts.append(val)
                elif isinstance(val, list):
                    texts.extend([str(v) for v in val if v])
            # Se tem seções
            for key in ["sections", "commodities", "items"]:
                val = dr.get(key)
                if isinstance(val, list):
                    for item in val:
                        if isinstance(item, dict):
                            for k2 in ["text", "analysis", "comment", "content"]:
                                t = item.get(k2)
                                if isinstance(t, str):
                                    texts.append(t)
                elif isinstance(val, dict):
                    for k, v in val.items():
                        if isinstance(v, str):
                            texts.append(v)
                        elif isinstance(v, dict):
                            for k2 in ["text", "analysis", "comment"]:
                                t = v.get(k2)
                                if isinstance(t, str):
                                    texts.append(t)

        full_text = " ".join(texts).lower()
        if not full_text.strip():
            return

        # Busca variações diárias
        max_change = 0
        for sym in list(self.symbols.get("chicago", {}).keys()):
            sym_data = pr.get(sym, {})
            if isinstance(sym_data, dict):
                chg = sym_data.get("change_pct", sym_data.get("pct_change", sym_data.get("1d_pct")))
                if chg is not None:
                    try:
                        max_change = max(max_change, abs(float(chg)))
                    except (ValueError, TypeError):
                        pass

        extreme_words = self.symbols.get("language_audit", {}).get("extreme_words", [])
        for rule in extreme_words:
            word = rule.get("word", "")
            min_pct = rule.get("min_change_pct", 2.0)
            if word in full_text and max_change < min_pct:
                self.add("FLAG", "LANGUAGE_OVERSTATEMENT",
                         f"Texto usa '{word}' mas maior variação diária é {max_change:.2f}% (mínimo: {min_pct}%)",
                         {"word": word, "max_change_pct": round(max_change, 2),
                          "required_pct": min_pct})

    # ──────────────────────────────────────────────────────────
    # GATE 3.5g: FONTE / RASTREABILIDADE (4.7)
    # ──────────────────────────────────────────────────────────

    def check_traceability(self):
        """Verifica se dados têm metadata de fonte e timestamp."""
        pr = self.data.get("pr", {})
        if not pr:
            return

        meta = pr.get("_meta", {})
        if not meta:
            self.add("WARN", "NO_META",
                     "price_history.json não contém _meta (sem rastreabilidade)")
        else:
            if "collected_at" not in meta:
                self.add("WARN", "NO_TIMESTAMP_META",
                         "price_history.json _meta sem collected_at")
            if "source" not in meta:
                self.add("INFO", "NO_SOURCE_META",
                         "price_history.json _meta sem campo source")

        # Verifica BCB
        bcb = self.data.get("bcb", {})
        if bcb:
            bcb_meta = bcb.get("_meta", {})
            if not bcb_meta:
                self.add("WARN", "NO_META_BCB",
                         "bcb_data.json sem _meta")

    # ──────────────────────────────────────────────────────────
    # GATE 3.5h: EIA DATA VALIDATION
    # ──────────────────────────────────────────────────────────

    def check_eia(self):
        """Valida dados EIA contra faixas do symbols.yml."""
        ed = self.data.get("ed", {})
        if not ed or not self.symbols:
            return

        eia_syms = self.symbols.get("eia", {})
        for key, cfg in eia_syms.items():
            rng = cfg.get("range")
            if not rng:
                continue

            val_data = ed.get(key, {})
            if isinstance(val_data, dict):
                val = val_data.get("value") or val_data.get("last") or val_data.get("close")
            elif isinstance(val_data, (int, float)):
                val = val_data
            elif isinstance(val_data, list) and val_data:
                last_item = val_data[-1]
                val = last_item.get("value") if isinstance(last_item, dict) else last_item
            else:
                continue

            if val is None:
                continue
            try:
                val = float(val)
            except (ValueError, TypeError):
                continue

            lo, hi = rng
            if val < lo or val > hi:
                self.add("FLAG", "EIA_RANGE",
                         f"EIA {cfg.get('display_name', key)}: {val:,.1f} fora de [{lo:,}-{hi:,}]",
                         {"key": key, "value": val, "range": rng})

    # ──────────────────────────────────────────────────────────
    # GATE 3.5i: MACRO BRASIL VALIDATION
    # ──────────────────────────────────────────────────────────

    def check_macro_br(self):
        """Valida câmbio e indicadores macro Brasil."""
        bcb = self.data.get("bcb", {})
        if not bcb or not self.symbols:
            return

        macro_syms = self.symbols.get("macro_br", {})

        # Câmbio
        brl_cfg = macro_syms.get("BRL_USD", {})
        brl_range = brl_cfg.get("range")
        if brl_range:
            # Tenta extrair câmbio de vários formatos possíveis
            fx = None
            for key in ["BRL_USD", "brl_usd", "usd_brl", "cambio", "dolar"]:
                val = bcb.get(key)
                if isinstance(val, dict):
                    fx = val.get("value") or val.get("last") or val.get("close")
                elif isinstance(val, (int, float)):
                    fx = val
                if fx is not None:
                    break

            if fx is not None:
                try:
                    fx = float(fx)
                    lo, hi = brl_range
                    if fx < lo or fx > hi:
                        self.add("BLOCK", "FX_RANGE_ERROR",
                                 f"Câmbio BRL/USD: {fx:.2f} fora de [{lo}-{hi}]",
                                 {"value": fx, "range": brl_range})
                except (ValueError, TypeError):
                    pass

    # ──────────────────────────────────────────────────────────
    # CONSOLIDAÇÃO E OUTPUT
    # ──────────────────────────────────────────────────────────

    def run_all(self):
        """Executa todas as verificações."""
        print("=" * 60)
        print("  AgriMacro AA+QA Engine v1.0")
        print(f"  {self.date_str} — Auditoria iniciada")
        print("=" * 60)

        checks = [
            ("Disponibilidade de dados", self.check_data_availability),
            ("Freshness dos dados", self.check_data_freshness),
            ("Range de preços", self.check_price_ranges),
            ("Coerência de unidades", self.check_unit_coherence),
            ("Validação de spreads", self.check_spreads),
            ("Estoques vs média 5a", self.check_stocks),
            ("Consistência cross-page", self.check_cross_consistency),
            ("Auditoria semântica", self.check_language),
            ("Rastreabilidade", self.check_traceability),
            ("Dados EIA", self.check_eia),
            ("Macro Brasil", self.check_macro_br),
        ]

        for name, func in checks:
            try:
                func()
                print(f"  ✓ {name}")
            except Exception as e:
                self.add("WARN", "CHECK_ERROR",
                         f"Erro ao executar '{name}': {str(e)}",
                         {"traceback": traceback.format_exc()})
                print(f"  ✗ {name} — ERRO: {e}")

        return self.get_status()

    def get_status(self):
        """Retorna status geral: PASS, WARN, FLAG, BLOCK."""
        if any(f["severity"] == "BLOCK" for f in self.findings):
            return "BLOCK"
        if any(f["severity"] == "FLAG" for f in self.findings):
            return "FLAG"
        if any(f["severity"] == "WARN" for f in self.findings):
            return "WARN"
        return "PASS"

    def count_by_severity(self):
        """Conta achados por severidade."""
        counts = {s: 0 for s in self.SEVERITIES}
        for f in self.findings:
            counts[f["severity"]] = counts.get(f["severity"], 0) + 1
        return counts

    def generate_report(self):
        """Gera o QA report JSON."""
        status = self.get_status()
        counts = self.count_by_severity()

        report = {
            "engine": "AgriMacro AA+QA Engine v1.0",
            "date": self.date_str,
            "timestamp": self.timestamp,
            "status": status,
            "summary": {
                "total_checks": len(self.findings),
                "blocks": counts["BLOCK"],
                "flags": counts["FLAG"],
                "warnings": counts["WARN"],
                "info": counts["INFO"],
            },
            "can_publish": status not in ("BLOCK",),
            "findings": self.findings,
            "confidence": self._calc_confidence(),
        }
        return report

    def _calc_confidence(self):
        """Calcula índice de confiança 0-100."""
        if not self.findings:
            return 100
        score = 100
        for f in self.findings:
            if f["severity"] == "BLOCK":
                score -= 25
            elif f["severity"] == "FLAG":
                score -= 10
            elif f["severity"] == "WARN":
                score -= 3
        return max(0, score)

    def print_summary(self):
        """Imprime resumo no terminal."""
        status = self.get_status()
        counts = self.count_by_severity()
        confidence = self._calc_confidence()

        print()
        print("─" * 60)

        if status == "PASS":
            print("  ✅ STATUS: PASS — Relatório liberado")
        elif status == "WARN":
            print("  ⚠️  STATUS: WARN — Relatório liberado com notas")
        elif status == "FLAG":
            print("  🟡 STATUS: FLAG — Relatório sob revisão")
        else:
            print("  🛑 STATUS: BLOCK — RELATÓRIO BLOQUEADO")

        print(f"  Confiança: {confidence}%")
        print(f"  BLOCK: {counts['BLOCK']} | FLAG: {counts['FLAG']} | "
              f"WARN: {counts['WARN']} | INFO: {counts['INFO']}")
        print("─" * 60)

        # Mostra bloqueios e flags
        for f in self.findings:
            if f["severity"] in ("BLOCK", "FLAG"):
                icon = "🛑" if f["severity"] == "BLOCK" else "🟡"
                print(f"  {icon} [{f['code']}] {f['message']}")

        if counts["WARN"] > 0:
            print(f"\n  ⚠️  {counts['WARN']} avisos (use --verbose para ver)")

        print("─" * 60)

    def save_outputs(self):
        """Salva todos os outputs obrigatórios."""
        report = self.generate_report()

        # 1. qa_report.json
        qa_path = LOGS_DIR / f"{self.date_str}_qa_report.json"
        with open(qa_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"  📄 {qa_path}")

        # 2. validation_summary.txt
        summary_path = LOGS_DIR / f"{self.date_str}_validation_summary.txt"
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write(f"AgriMacro QA Summary — {self.date_str}\n")
            f.write(f"Status: {report['status']}\n")
            f.write(f"Confiança: {report['confidence']}%\n")
            f.write(f"Can Publish: {report['can_publish']}\n\n")
            for finding in self.findings:
                f.write(f"[{finding['severity']}] {finding['code']}: {finding['message']}\n")
        print(f"  📄 {summary_path}")

        # 3. error_log.txt (só se houver erros)
        errors = [f for f in self.findings if f["severity"] in ("BLOCK", "FLAG")]
        if errors:
            err_path = LOGS_DIR / f"{self.date_str}_error_log.txt"
            with open(err_path, "w", encoding="utf-8") as f:
                for e in errors:
                    f.write(f"[{e['severity']}] {e['code']}\n")
                    f.write(f"  {e['message']}\n")
                    if e.get("details"):
                        f.write(f"  Details: {json.dumps(e['details'], ensure_ascii=False)}\n")
                    f.write("\n")
            print(f"  📄 {err_path}")

        # 4. data_snapshot.json (snapshot resumido dos dados atuais)
        snapshot = {"date": self.date_str, "timestamp": self.timestamp}
        pr = self.data.get("pr", {})
        snap_prices = {}
        for sym in ["ZS", "ZC", "ZW", "KE", "ZM", "ZL", "KC", "SB", "CT",
                     "CC", "OJ", "LE", "GF", "HE", "CL", "NG", "GC", "RB", "HO"]:
            d = pr.get(sym, {})
            if isinstance(d, dict):
                snap_prices[sym] = d.get("last") or d.get("close")
        snapshot["prices"] = snap_prices
        snap_path = LOGS_DIR / f"{self.date_str}_data_snapshot.json"
        with open(snap_path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2, ensure_ascii=False)
        print(f"  📄 {snap_path}")

        # Copia qa_report.json também p/ pasta de dados (dashboard pode ler)
        qa_dash = DATA_PROC / "qa_report.json"
        try:
            with open(qa_dash, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
        except Exception:
            pass  # Se pasta não existir, ok

        return report


# ══════════════════════════════════════════════════════════════
# FUNÇÃO PRINCIPAL — Chamável pelo pipeline ou standalone
# ══════════════════════════════════════════════════════════════

def run_audit(force=False, dry_run=False):
    """
    Executa auditoria completa.
    Retorna: (status, report_dict)
    status: "PASS", "WARN", "FLAG", "BLOCK"
    """
    # Carrega symbols
    symbols = load_symbols()
    if symbols is None:
        print("⚠️  symbols.yml não encontrado — auditoria parcial (sem range/unit checks)")

    # Carrega dados
    data, missing = load_all_data()
    if not data:
        print("🛑 Nenhum dado encontrado! Verifique os caminhos.")
        return "BLOCK", {"status": "BLOCK", "error": "no_data"}

    # Cria engine e roda
    engine = AAQAEngine(symbols, data, missing)
    status = engine.run_all()
    engine.print_summary()

    if not dry_run:
        print("\n  Salvando outputs...")
        report = engine.save_outputs()
    else:
        report = engine.generate_report()
        print("\n  [DRY RUN — nada salvo]")

    return status, report


def main():
    """Entry point CLI."""
    force = "--force" in sys.argv
    dry_run = "--dry-run" in sys.argv
    verbose = "--verbose" in sys.argv

    status, report = run_audit(force=force, dry_run=dry_run)

    if verbose and report.get("findings"):
        print("\n  TODOS OS ACHADOS:")
        for f in report.get("findings", []):
            print(f"  [{f['severity']}] {f['code']}: {f['message']}")

    # Exit code
    if status == "BLOCK" and not force:
        print("\n  🛑 RELATÓRIO BLOQUEADO. Use --force para ignorar.")
        sys.exit(1)
    elif status == "BLOCK" and force:
        print("\n  ⚠️  BLOQUEIO IGNORADO com --force. Prosseguindo...")
        sys.exit(0)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
