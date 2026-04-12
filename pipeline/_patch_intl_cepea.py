"""
_patch_intl_cepea.py — Integra physical_br.json no collect_physical_intl.py
Logica: collect_brazil() checa se physical_br.json existe e <24h → usa ele
        senao faz scraping original (renomeado para _collect_brazil_scraping)

Uso: cd pipeline && python _patch_intl_cepea.py
"""
import shutil, datetime, sys, ast
from pathlib import Path

TARGET = Path("collect_physical_intl.py")
if not TARGET.exists():
    print(f"[ERRO] {TARGET} nao encontrado!")
    sys.exit(1)

# Backup
bak = f"collect_physical_intl.bak_{datetime.datetime.now():%Y%m%d_%H%M%S}.py"
shutil.copy2(TARGET, bak)
print(f"[BACKUP] {bak}")

src = TARGET.read_text(encoding="utf-8")

# Check if already patched
if "_collect_brazil_scraping" in src:
    print("[SKIP] Ja integrado (encontrou _collect_brazil_scraping). Nada a fazer.")
    sys.exit(0)

# ── PATCH: rename original collect_brazil → _collect_brazil_scraping
# Then add new collect_brazil that checks physical_br.json first

old = "def collect_brazil():"
new = """def _load_cepea_json():
    \"\"\"Tenta carregar physical_br.json do collect_cepea.py (se <24h).\"\"\"
    import json as _json
    from datetime import datetime as _dt, timedelta as _td
    _base = Path(__file__).resolve().parent.parent
    _f = _base / "agrimacro-dash" / "public" / "data" / "processed" / "physical_br.json"
    if not _f.exists():
        return None
    try:
        data = _json.loads(_f.read_text(encoding="utf-8"))
        ts = _dt.strptime(data["timestamp"], "%Y-%m-%d %H:%M:%S")
        if _dt.now() - ts > _td(hours=24):
            log("physical_br.json > 24h — scraping direto")
            return None
        products = data.get("products", {})
        if not products:
            return None
        # Convert from collect_cepea format to collect_physical_intl format
        results = {}
        for sym, p in products.items():
            # History: cepea uses [{date,value}] newest first
            # intl expects [{period,value}] oldest first (reversed)
            hist_raw = p.get("history", [])
            hist = [{"period": h.get("date", ""), "value": h["value"]} for h in hist_raw]
            hist.reverse()
            # Trend
            change = p.get("change_pct")
            if change is not None:
                trend = f"{change:+.1f}% d/d"
            else:
                trend = "\\u2014"
            results[sym] = {
                "label": p.get("label", sym),
                "price": p["price"],
                "price_unit": p.get("unit", ""),
                "period": p.get("period", ""),
                "trend": trend,
                "source": p.get("source", "CEPEA/ESALQ"),
                "history": hist,
            }
        log(f"physical_br.json: {len(results)} produtos carregados ({data['timestamp']})", ok=True)
        return results
    except Exception as e:
        log(f"physical_br.json: erro ao ler — {e}")
        return None


def collect_brazil():
    \"\"\"Coleta precos Brasil. Prioridade: physical_br.json (collect_cepea.py) → scraping direto.\"\"\"
    cached = _load_cepea_json()
    if cached:
        return cached
    log("Fallback: scraping direto Noticias Agricolas...")
    return _collect_brazil_scraping()


def _collect_brazil_scraping():"""

src = src.replace(old, new, 1)

# ── Save & validate ──
TARGET.write_text(src, encoding="utf-8")

try:
    ast.parse(src)
    print("[OK] Patch aplicado + syntax OK!")
    print("  - _load_cepea_json(): le physical_br.json se <24h")
    print("  - collect_brazil(): prioriza CEPEA json, fallback scraping")
    print("  - _collect_brazil_scraping(): scraping original preservado")
    print("\nTeste: python collect_physical_intl.py")
except SyntaxError as e:
    print(f"[ERRO SYNTAX] {e}")
    print("Restaurando backup...")
    shutil.copy2(bak, TARGET)
    print(f"Restaurado de {bak}")
