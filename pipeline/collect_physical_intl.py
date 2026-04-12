"""
AgriMacro v3.1 - International Physical Market Data Collector
Brasil: Noticias Agricolas (CEPEA/ESALQ) - GRATUITO
Argentina: MAGyP API FOB - GRATUITO
Demais: SEM API GRATUITA - marcado honestamente
ZERO MOCK
"""

import json, re, urllib.request, urllib.error
from datetime import datetime, timedelta
from pathlib import Path


def log(msg, ok=False):
    icon = chr(10003) if ok else chr(9679)
    print(f"  [{icon}] {msg}")


def fetch_url(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/json,*/*"
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        log(f"  HTTP error: {e}")
        return None


BASE_NA = "https://www.noticiasagricolas.com.br"
BRAZIL_SOURCES = {
    "ZS_BR": {"path": "/cotacoes/soja/soja-indicador-cepea-esalq-porto-paranagua",
              "label": "🇧🇷 Brasil (Paranaguá) — Soja CEPEA", "unit": "R$/sc 60kg"},
    "ZC_BR": {"path": "/cotacoes/milho/indicador-cepea-esalq-milho",
              "label": "🇧🇷 Brasil (Campinas) — Milho CEPEA", "unit": "R$/sc 60kg"},
    "KC_BR": {"path": "/cotacoes/cafe/indicador-cepea-esalq-cafe-arabica",
              "label": "🇧🇷 Brasil — Café Arábica CEPEA", "unit": "R$/sc 60kg"},
    "LE_BR": {"path": "/cotacoes/boi-gordo/boi-gordo-indicador-esalq-bmf",
              "label": "🇧🇷 Brasil (SP) — Boi Gordo CEPEA", "unit": "R$/@"},
    "CT_BR": {"path": "/cotacoes/algodao/algodao-indicador-cepea-esalq-a-prazo",
              "label": "🇧🇷 Brasil — Algodão CEPEA", "unit": "R$/@"},
    "ZW_BR": {"path": "/cotacoes/trigo/preco-medio-do-trigo-cepea-esalq",
              "label": "🇧🇷 Brasil (PR) — Trigo CEPEA", "unit": "R$/t"},
    "SB_BR": {"path": "/cotacoes/sucroenergetico/acucar-cristal-cepea",
              "label": "Brasil (SP) \u2014 Acucar Cristal CEPEA", "unit": "R$/sc 50kg"},
    "ETH_BR": {"path": "/cotacoes/sucroenergetico/indicador-semanal-etanol-hidratado-cepea-esalq",
              "label": "Brasil (SP) \u2014 Etanol Hidratado CEPEA", "unit": "R$/litro"},
    "ETN_BR": {"path": "/cotacoes/sucroenergetico/indicador-semanal-etanol-anidro-cepea-esalq",
              "label": "Brasil (SP) \u2014 Etanol Anidro CEPEA", "unit": "R$/litro"},
}


def _load_cepea_json():
    """Tenta carregar physical_br.json do collect_cepea.py (se <24h)."""
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
                trend = "\u2014"
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
    """Coleta precos Brasil. Prioridade: physical_br.json (collect_cepea.py) → scraping direto."""
    cached = _load_cepea_json()
    if cached:
        return cached
    log("Fallback: scraping direto Noticias Agricolas...")
    return _collect_brazil_scraping()


def _collect_brazil_scraping():
    results = {}
    for sym, info in BRAZIL_SOURCES.items():
        log(f"Brasil {sym}: Fetching...")
        html = fetch_url(BASE_NA + info["path"], timeout=12)
        if not html: continue
        tables = re.findall(r'<table class="cot-fisicas">(.*?)</table>', html, re.DOTALL)
        if not tables:
            log(f"  {sym}: sem tabela"); continue
        history = []
        for tbl in tables[:30]:
            rows = re.findall(r'<tr>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>', tbl, re.DOTALL)
            for row in rows:
                date_str = row[0].strip()
                price_str = row[1].strip().replace(".", "").replace(",", ".")
                try:
                    history.append({"period": date_str, "value": float(price_str)})
                except ValueError: pass
        if not history: continue
        latest = history[0]
        trend = "—"
        if len(history) >= 2 and history[1]["value"] > 0:
            pct = ((latest["value"] - history[1]["value"]) / history[1]["value"]) * 100
            trend = f"{pct:+.1f}% d/d"
        results[sym] = {
            "label": info["label"], "price": latest["value"],
            "price_unit": info["unit"], "period": latest["period"],
            "trend": trend, "source": "CEPEA/ESALQ via NA",
            "history": list(reversed(history[:12])),
        }
        log(f"  {sym}: R$ {latest['value']:.2f} ({latest['period']})", ok=True)
    # Trigo tem formato diferente: 4 colunas (Data, Regiao, R$/t, Variacao)
    log("Brasil ZW_BR: Fetching (formato especial)...")
    html = fetch_url(BASE_NA + "/cotacoes/trigo/preco-medio-do-trigo-cepea-esalq", timeout=12)
    if html:
        tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL | re.IGNORECASE)
        history = []
        for t in tables:
            rows = re.findall(r'<tr>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>', t, re.DOTALL)
            for row in rows:
                date_str = row[0].strip()
                region = row[1].strip()
                if "Paran" not in region:
                    continue
                price_str = row[2].strip().replace(".", "").replace(",", ".")
                try:
                    history.append({"period": date_str, "value": float(price_str)})
                except ValueError:
                    pass
        if history:
            latest = history[0]
            trend = "\u2014"
            if len(history) >= 2 and history[1]["value"] > 0:
                pct = ((latest["value"] - history[1]["value"]) / history[1]["value"]) * 100
                trend = f"{pct:+.1f}% d/d"
            results["ZW_BR"] = {
                "label": "\U0001f1e7\U0001f1f7 Brasil (PR) \u2014 Trigo CEPEA",
                "price": latest["value"],
                "price_unit": "R$/t",
                "period": latest["period"],
                "trend": trend,
                "source": "CEPEA/ESALQ via NA",
                "history": list(reversed(history[:12])),
            }
            log(f"  ZW_BR: R$ {latest['value']:.2f} ({latest['period']})", ok=True)
    return results


ARG_NCM = {
    "ZS_AR": {"ncm": "1201", "label": "🇦🇷 Argentina — Soja FOB"},
    "ZC_AR": {"ncm": "1005", "label": "🇦🇷 Argentina — Milho FOB"},
    "ZW_AR": {"ncm": "1001", "label": "🇦🇷 Argentina — Trigo FOB"},
    "ZL_AR": {"ncm": "1507", "label": "🇦🇷 Argentina — Óleo de Soja FOB"},
    "ZM_AR": {"ncm": "2304", "label": "🇦🇷 Argentina — Farelo de Soja FOB"},
}


def collect_argentina():
    results = {}
    log("Argentina: Fetching MAGyP FOB API...")
    raw_data = None; used_date = None
    for days_back in range(0, 10):
        d = datetime.now() - timedelta(days=days_back)
        if d.weekday() >= 5: continue
        date_str = d.strftime("%d/%m/%Y")
        url = "https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/ws/ssma/precios_fob.php?Fecha=" + date_str
        raw = fetch_url(url, timeout=20)
        if not raw or not raw.strip(): continue
        try:
            parsed = json.loads(raw)
            posts = parsed.get("posts", []) if isinstance(parsed, dict) else parsed if isinstance(parsed, list) else []
            if posts:
                raw_data = posts; used_date = date_str; break
        except json.JSONDecodeError: continue
    if not raw_data:
        log("  Argentina: sem dados FOB"); return results
    log(f"  Argentina: {len(raw_data)} registros de {used_date}")
    for sym, info in ARG_NCM.items():
        matching = [p for p in raw_data if isinstance(p, dict) and str(p.get("posicion", "")).startswith(info["ncm"])]
        if matching:
            matching.sort(key=lambda x: (x.get("mesDesde", 99)))
            price = matching[0].get("precio")
            if price is not None:
                results[sym] = {"label": info["label"], "price": float(price), "price_unit": "USD/t",
                    "period": used_date, "trend": "—", "source": "MAGyP/FOB Oficial", "history": []}
                log(f"  {info['label']}: USD {price:.0f}/t", ok=True)
    return results


def get_unavailable():
    m = {}
    items = [
        ("ZW_RU", "🇷🇺 Rússia (FOB BS) — Trigo 12.5%", "USD/t", "IKAR (requer assinatura)"),
        ("CC_CI", "🇨🇮 Costa do Marfim — Cocoa Grade I", "USD/t", "CCC (sem API)"),
        ("CC_GH", "🇬🇭 Gana — Cocoa Grade I", "USD/t", "COCOBOD (sem API)"),
        ("KC_CO", "🇨🇴 Colômbia — Excelso EP", "USD/lb", "FNC (sem API)"),
        ("KC_VN", "🇻🇳 Vietnã — Robusta G2", "USD/t", "MARD (sem API)"),
        ("LE_AR", "🇦🇷 Argentina (Liniers) — Novillo", "ARS/kg", "Liniers (sem API)"),
        ("ZS_CN", "🇨🇳 China — Soja Import (DCE)", "CNY/t", "DCE (requer terminal)"),
        ("ZC_CN", "🇨🇳 China — Milho Import (DCE)", "CNY/t", "DCE (requer terminal)"),
        ("HE_CN", "🇨🇳 China — Suínos (Zhengzhou)", "CNY/t", "ZCE (requer terminal)"),
    ]
    for sym, label, unit, source in items:
        m[sym] = {"label": label, "price": None, "price_unit": unit, "period": "—",
                  "trend": "Sem API gratuita", "source": source, "history": []}
    return m


def collect_physical_intl(price_history_path=None):
    print("\n  === Collecting International Physical Market Data ===\n")
    all_data = {}
    print("  -- BRASIL (CEPEA via Noticias Agricolas) --")
    all_data.update(collect_brazil())
    print("\n  -- ARGENTINA (MAGyP FOB) --")
    all_data.update(collect_argentina())
    print("\n  -- Mercados sem API gratuita --")
    for sym, info in get_unavailable().items():
        if sym not in all_data:
            all_data[sym] = info
            log(f"{info['label']}: {info['source']}")
    output = {"timestamp": datetime.now().isoformat(), "international": all_data,
        "total_markets": len(all_data),
        "markets_with_data": sum(1 for v in all_data.values() if v.get("price") is not None),
        "markets_unavailable": sum(1 for v in all_data.values() if v.get("price") is None)}
    print(f"\n  === Summary: {output['markets_with_data']} with data, {output['markets_unavailable']} unavailable, {output['total_markets']} total ===\n")
    return output


if __name__ == "__main__":
    script_dir = Path(__file__).parent
    base = script_dir.parent / "agrimacro-dash" / "public" / "data"
    proc_path = base / "processed"
    proc_path.mkdir(parents=True, exist_ok=True)
    result = collect_physical_intl()
    out = proc_path / "physical_intl.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"  Saved to: {out}")
    print(f"  Markets with data: {result['markets_with_data']}/{result['total_markets']}")