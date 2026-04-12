"""
_patch_cepea.py — Aplica 2 fixes no collect_cepea.py:
1. Parser especial para ZW_BR (trigo) — tabelas sem class="cot-fisicas"
2. Fix agrobr API: cepea.get() → cepea.ultimo()

Uso: cd pipeline && python _patch_cepea.py
"""
import shutil, datetime, sys, ast
from pathlib import Path

TARGET = Path("collect_cepea.py")
if not TARGET.exists():
    print(f"[ERRO] {TARGET} nao encontrado!")
    sys.exit(1)

# Backup
bak = f"collect_cepea.bak_{datetime.datetime.now():%Y%m%d_%H%M%S}.py"
shutil.copy2(TARGET, bak)
print(f"[BACKUP] {bak}")

src = TARGET.read_text(encoding="utf-8")
changes = 0

# ── FIX 1: Add _parse_na_trigo before _scrape_na ──────────────
TRIGO_FUNC = '''def _parse_na_trigo(html):
    """Parser especial para trigo: 1 tabela por dia, colunas Data/Regiao/R$/t/Var."""
    tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL)
    history = []
    for t in tables:
        if 'R$/t' not in t:
            continue
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', t, re.DOTALL)
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            if len(cells) >= 3:
                date_str = re.sub(r'<[^>]+>', '', cells[0]).strip()
                val_str  = re.sub(r'<[^>]+>', '', cells[2]).strip()
                val_str  = val_str.replace('.', '').replace(',', '.')
                try:
                    val = float(val_str)
                    history.append({"date": date_str, "value": val})
                except ValueError:
                    pass
    if not history:
        return None, None, []
    price = history[0]["value"]
    period = history[0]["date"]
    return price, period, history[:10]


'''

old1 = "def _scrape_na(missing_symbols):"
if old1 in src and "_parse_na_trigo" not in src:
    src = src.replace(old1, TRIGO_FUNC + old1, 1)
    changes += 1
    print("[FIX 1] _parse_na_trigo adicionado")
elif "_parse_na_trigo" in src:
    print("[FIX 1] _parse_na_trigo ja existe — skip")
else:
    print("[FIX 1] FALHOU — _scrape_na nao encontrado")

# ── FIX 2: Use trigo parser for ZW_BR ─────────────────────────
old2 = "        price, period, hist = _parse_na_table(html)"
new2 = "        if sym == 'ZW_BR':\n            price, period, hist = _parse_na_trigo(html)\n        else:\n            price, period, hist = _parse_na_table(html)"

if old2 in src and "sym == 'ZW_BR'" not in src:
    src = src.replace(old2, new2, 1)
    changes += 1
    print("[FIX 2] ZW_BR dispatch adicionado")
elif "sym == 'ZW_BR'" in src:
    print("[FIX 2] ZW_BR dispatch ja existe — skip")
else:
    print("[FIX 2] FALHOU — linha _parse_na_table nao encontrada")

# ── FIX 3: agrobr API — cepea.get() → cepea.ultimo() ──────────
old3 = "from agrobr import cepea"
new3 = "from agrobr.cepea import ultimo as cepea_ultimo"

old3b = "df = await cepea.get(key)"
new3b = "df = await cepea_ultimo(key)"

if old3 in src:
    src = src.replace(old3, new3, 1)
    changes += 1
    print("[FIX 3a] import agrobr.cepea.ultimo")

if old3b in src:
    src = src.replace(old3b, new3b, 1)
    changes += 1
    print("[FIX 3b] cepea.get → cepea_ultimo")

# ── Save & validate ───────────────────────────────────────────
TARGET.write_text(src, encoding="utf-8")

try:
    ast.parse(src)
    print(f"\n[OK] {changes} fixes aplicados. Syntax OK.")
    print(f"     Teste: python collect_cepea.py")
except SyntaxError as e:
    print(f"\n[ERRO SYNTAX] {e}")
    print(f"Restaurando backup...")
    shutil.copy2(bak, TARGET)
    print(f"Restaurado de {bak}")
