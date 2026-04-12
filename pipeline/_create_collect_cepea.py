"""
_create_collect_cepea.py — Bootstrapper
Roda este script NA PASTA pipeline/ para criar collect_cepea.py
Uso: cd pipeline && python _create_collect_cepea.py
"""
import sys
from pathlib import Path

TARGET = Path(__file__).resolve().parent / "collect_cepea.py"

if TARGET.exists():
    print(f"[AVISO] {TARGET.name} ja existe!")
    resp = input("Sobrescrever? (s/n): ").strip().lower()
    if resp != 's':
        print("Cancelado.")
        sys.exit(0)

CONTENT = r'''"""
collect_cepea.py — Coletor CEPEA (precos fisicos Brasil)
AgriMacro v3.3

FASE 1: agrobr (CEPEA oficial) — async
FASE 2: Fallback Noticias Agricolas (scraping)

Output: processed/physical_br.json
"""

import json, re, sys, time, asyncio
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError

# -- Paths --
BASE    = Path(__file__).resolve().parent.parent
PROC    = BASE / "agrimacro-dash" / "public" / "data" / "processed"
OUT     = PROC / "physical_br.json"
PROC.mkdir(parents=True, exist_ok=True)

# -- Product mapping --
PRODUCTS = {
    "ZS_BR":  {"agrobr": "soja",              "label": "Soja Paranagua",       "unit": "R$/saca 60kg"},
    "ZC_BR":  {"agrobr": "milho",             "label": "Milho B3/CEPEA",       "unit": "R$/saca 60kg"},
    "KC_BR":  {"agrobr": "cafe_arabica",      "label": "Cafe Arabica CEPEA",   "unit": "R$/saca 60kg"},
    "LE_BR":  {"agrobr": "boi_gordo",         "label": "Boi Gordo CEPEA",      "unit": "R$/@"},
    "CT_BR":  {"agrobr": "algodao",           "label": "Algodao CEPEA",        "unit": "R$/@ (c/pluma)"},
    "ZW_BR":  {"agrobr": "trigo",             "label": "Trigo CEPEA",          "unit": "R$/ton"},
    "SB_BR":  {"agrobr": "acucar",            "label": "Acucar Cristal CEPEA", "unit": "R$/saca 50kg"},
    "ETH_BR": {"agrobr": "etanol_hidratado",  "label": "Etanol Hidratado",     "unit": "R$/litro"},
    "ETN_BR": {"agrobr": "etanol_anidro",     "label": "Etanol Anidro",        "unit": "R$/litro"},
}

# -- Noticias Agricolas fallback URLs --
BASE_NA = "https://www.noticiasagricolas.com.br"
NA_PATHS = {
    "ZS_BR":  "/cotacoes/soja/soja-indicador-cepea-esalq-porto-paranagua",
    "ZC_BR":  "/cotacoes/milho/indicador-cepea-esalq-milho",
    "KC_BR":  "/cotacoes/cafe/indicador-cepea-esalq-cafe-arabica",
    "LE_BR":  "/cotacoes/boi-gordo/boi-gordo-indicador-esalq-bmf",
    "CT_BR":  "/cotacoes/algodao/algodao-indicador-cepea-esalq-a-prazo",
    "ZW_BR":  "/cotacoes/trigo/preco-medio-do-trigo-cepea-esalq",
    "SB_BR":  "/cotacoes/sucroenergetico/acucar-cristal-cepea",
    "ETH_BR": "/cotacoes/sucroenergetico/indicador-semanal-etanol-hidratado-cepea-esalq",
    "ETN_BR": "/cotacoes/sucroenergetico/indicador-semanal-etanol-anidro-cepea-esalq",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


# === FASE 1 — agrobr (CEPEA oficial) ===

def _try_agrobr():
    try:
        from agrobr import cepea
    except ImportError:
        print("[CEPEA] agrobr nao instalado — pulando fase 1")
        return {}

    results = {}

    async def _fetch_all():
        for sym, info in PRODUCTS.items():
            key = info["agrobr"]
            try:
                df = await cepea.get(key)
                if df is None or (hasattr(df, 'empty') and df.empty):
                    print(f"  [agrobr] {sym} ({key}): sem dados")
                    continue

                val_col = None
                for col_candidate in ['valor', 'value', 'price', 'Valor', 'Price']:
                    if col_candidate in df.columns:
                        val_col = col_candidate
                        break
                if val_col is None and len(df.columns) > 0:
                    for c in reversed(df.columns.tolist()):
                        if df[c].dtype in ('float64', 'float32', 'int64', 'int32'):
                            val_col = c
                            break
                if val_col is None:
                    print(f"  [agrobr] {sym}: colunas nao reconhecidas: {df.columns.tolist()}")
                    continue

                last_row = df.iloc[-1]
                price = float(last_row[val_col])
                last_date = str(df.index[-1])[:10] if hasattr(df.index[-1], 'strftime') else str(df.index[-1])

                change_pct = None
                if len(df) >= 2:
                    prev = float(df.iloc[-2][val_col])
                    if prev > 0:
                        change_pct = round((price - prev) / prev * 100, 2)

                hist = []
                for idx, row in df.tail(10).iterrows():
                    d = str(idx)[:10] if hasattr(idx, 'strftime') else str(idx)
                    hist.append({"date": d, "value": round(float(row[val_col]), 4)})

                results[sym] = {
                    "label":     info["label"],
                    "price":     round(price, 4),
                    "unit":      info["unit"],
                    "period":    last_date,
                    "change_pct": change_pct,
                    "trend":     "up" if change_pct and change_pct > 0 else ("down" if change_pct and change_pct < 0 else "flat"),
                    "source":    "CEPEA/ESALQ via agrobr",
                    "history":   hist,
                }
                print(f"  [agrobr] {sym}: R$ {price:.4f} ({last_date})")

            except Exception as e:
                print(f"  [agrobr] {sym} ({key}): ERRO — {e}")

    try:
        asyncio.run(_fetch_all())
    except Exception as e:
        print(f"[CEPEA] agrobr falhou globalmente: {e}")
        return {}

    return results


# === FASE 2 — Fallback Noticias Agricolas (scraping) ===

def _fetch_url(url, timeout=15):
    req = Request(url, headers=HEADERS)
    try:
        with urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")
    except (URLError, Exception) as e:
        print(f"  [NA] Erro fetch {url}: {e}")
        return None


def _parse_na_table(html):
    tables = re.findall(r'<table class="cot-fisicas">(.*?)</table>', html, re.DOTALL)
    if not tables:
        return None, None, []

    table = tables[0]
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table, re.DOTALL)

    history = []
    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(cells) >= 2:
            date_str = re.sub(r'<[^>]+>', '', cells[0]).strip()
            val_str  = re.sub(r'<[^>]+>', '', cells[1]).strip()
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


def _scrape_na(missing_symbols):
    results = {}
    for sym in missing_symbols:
        if sym not in NA_PATHS:
            continue

        url = BASE_NA + NA_PATHS[sym]
        info = PRODUCTS[sym]
        print(f"  [NA] {sym}: buscando {url}")

        html = _fetch_url(url)
        if not html:
            continue

        price, period, hist = _parse_na_table(html)
        if price is None:
            print(f"  [NA] {sym}: tabela nao encontrada")
            continue

        change_pct = None
        if len(hist) >= 2:
            prev = hist[1]["value"]
            if prev > 0:
                change_pct = round((price - prev) / prev * 100, 2)

        results[sym] = {
            "label":     info["label"],
            "price":     round(price, 4),
            "unit":      info["unit"],
            "period":    period,
            "change_pct": change_pct,
            "trend":     "up" if change_pct and change_pct > 0 else ("down" if change_pct and change_pct < 0 else "flat"),
            "source":    "CEPEA/ESALQ via Noticias Agricolas",
            "history":   hist,
        }
        print(f"  [NA] {sym}: R$ {price:.4f} ({period})")
        time.sleep(1.5)

    return results


# === MAIN ===

def collect():
    print("=" * 60)
    print("COLLECT_CEPEA — Precos fisicos Brasil (CEPEA/ESALQ)")
    print("=" * 60)

    all_symbols = list(PRODUCTS.keys())
    results = {}

    print("\n[FASE 1] agrobr (CEPEA oficial)...")
    agrobr_data = _try_agrobr()
    results.update(agrobr_data)
    got = set(results.keys())
    print(f"  agrobr: {len(got)}/{len(all_symbols)} coletados")

    missing = [s for s in all_symbols if s not in got]
    if missing:
        print(f"\n[FASE 2] Fallback Noticias Agricolas para {len(missing)} faltantes...")
        na_data = _scrape_na(missing)
        results.update(na_data)
        print(f"  NA: {len(na_data)}/{len(missing)} coletados")
    else:
        print("\n[FASE 2] Todos coletados via agrobr — skip fallback")

    final_got = set(results.keys())
    final_miss = [s for s in all_symbols if s not in final_got]
    print(f"\n[RESULTADO] {len(final_got)}/{len(all_symbols)} produtos coletados")
    if final_miss:
        print(f"  SEM DADOS: {', '.join(final_miss)}")

    output = {
        "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source":     "CEPEA/ESALQ",
        "method":     "agrobr + NA fallback",
        "count":      len(results),
        "products":   results,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n[SAVE] {OUT}")
    print(f"  Size: {OUT.stat().st_size:,} bytes")

    return output


if __name__ == "__main__":
    collect()
'''

with open(TARGET, 'w', encoding='utf-8') as f:
    f.write(CONTENT)

print(f"[OK] Criado: {TARGET}")
print(f"     Size: {TARGET.stat().st_size:,} bytes")
print(f"\nProximo passo: python collect_cepea.py")
