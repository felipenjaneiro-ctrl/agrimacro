"""
AgriMacro v3.0 - Daily Reading Generator
Generates dynamic narrative based on actual data
"""
import json
from datetime import datetime
from pathlib import Path

def generate_daily_reading(proc_path: Path) -> dict:
    try:
        with open(proc_path / "seasonality.json", encoding="utf-8") as f:
            seasonality = json.load(f)
    except:
        seasonality = {}
    try:
        with open(proc_path / "spreads.json", encoding="utf-8") as f:
            spreads_data = json.load(f)
            spreads = spreads_data.get("spreads", {})
    except:
        spreads = {}
    try:
        with open(proc_path / "stocks_watch.json", encoding="utf-8") as f:
            stocks_data = json.load(f)
            stocks = stocks_data.get("commodities", {})
    except:
        stocks = {}

    blocos = []
    perguntas = []

    grain_syms = ["ZC", "ZS", "ZW", "KE", "ZM", "ZL"]
    grain_below = []
    grain_above = []
    for sym in grain_syms:
        if sym in stocks:
            dev = stocks[sym].get("price_vs_avg", 0)
            if dev < -15:
                grain_below.append(f"{sym} ({dev:+.1f}%)")
            elif dev > 15:
                grain_above.append(f"{sym} ({dev:+.1f}%)")

    if grain_below:
        blocos.append({"title":"GRÃOS EM COMPRESSÃO","body":f"Commodities agrícolas abaixo da média 5Y: {', '.join(grain_below)}. Padrão histórico sugere formação de fundo sazonal ou pressão estrutural de oferta. Monitorar relatórios USDA e clima no Corn Belt."})
    elif grain_above:
        blocos.append({"title":"GRÃOS EM ALTA","body":f"Commodities agrícolas acima da média 5Y: {', '.join(grain_above)}. Verificar se há suporte fundamental (estoques baixos, demanda forte) ou especulação."})

    livestock_syms = ["LE", "GF", "HE"]
    livestock_status = []
    for sym in livestock_syms:
        if sym in stocks:
            dev = stocks[sym].get("price_vs_avg", 0)
            livestock_status.append((sym, dev))
    if livestock_status:
        extreme = [s for s in livestock_status if abs(s[1]) > 30]
        if extreme:
            names = ", ".join([f"{s[0]} ({s[1]:+.1f}%)" for s in extreme])
            blocos.append({"title":"PECUÁRIA EM EXTREMO","body":f"Proteína animal em níveis extremos: {names}. Verificar margem do confinador, custo de reposição e demanda exportação."})

    spread_alerts = []
    for key, sp in spreads.items():
        if sp.get("regime") in ["EXTREMO", "DISSONÂNCIA"]:
            spread_alerts.append({"name":sp.get("name"),"zscore":sp.get("zscore_1y"),"regime":sp.get("regime"),"description":sp.get("description")})
    if spread_alerts:
        for alert in spread_alerts[:2]:
            direction = "elevado" if alert["zscore"] > 0 else "comprimido"
            blocos.append({"title":f"SPREAD {alert['name'].upper()} EM {alert['regime']}","body":f"Z-score {alert['zscore']:+.2f} ({direction}). {alert['description']}"})

    metals_syms = ["GC", "SI", "DX"]
    metals_status = []
    for sym in metals_syms:
        if sym in stocks:
            dev = stocks[sym].get("price_vs_avg", 0)
            metals_status.append((sym, dev))
    if metals_status:
        extreme = [s for s in metals_status if abs(s[1]) > 50]
        if extreme:
            names = ", ".join([f"{s[0]} ({s[1]:+.1f}%)" for s in extreme])
            blocos.append({"title":"METAIS/MACRO EM DESTAQUE","body":f"Ativos macro em níveis atípicos: {names}. Contexto: política monetária, inflação, geopolítica."})

    recent_changes = []
    for key, sp in spreads.items():
        if sp.get("trend") == "SUBINDO" and sp.get("trend_pct", 0) > 5:
            recent_changes.append(f"{sp['name']} subiu {sp['trend_pct']:.1f}%")
        elif sp.get("trend") == "CAINDO" and sp.get("trend_pct", 0) < -5:
            recent_changes.append(f"{sp['name']} caiu {abs(sp['trend_pct']):.1f}%")
    if recent_changes:
        perguntas.append(f"O que mudou? - {'; '.join(recent_changes[:2])}")
    else:
        perguntas.append("O que mudou? - Spreads relativamente estáveis nas últimas sessões.")

    stale = []
    for sym, data in stocks.items():
        if abs(data.get("price_vs_avg", 0)) > 20:
            stale.append(f"{sym} permanece {data.get('price_vs_avg', 0):+.1f}% da média")
    if stale:
        perguntas.append(f"O que NÃO mudou, mas deveria? - {stale[0]}")

    extremes = [f"{sym}" for sym, data in stocks.items() if abs(data.get("price_vs_avg", 0)) > 30]
    if extremes:
        perguntas.append(f"O que está em extremo? - {', '.join(extremes)}")

    ignored = []
    for key, sp in spreads.items():
        if sp.get("regime") == "DISSONÂNCIA":
            ignored.append(sp.get("name"))
    if ignored:
        perguntas.append(f"O que o mercado ignora? - {', '.join(ignored)} em dissonância")

    aperto_count = sum(1 for s in stocks.values() if "APERTO" in s.get("state", ""))
    excesso_count = sum(1 for s in stocks.values() if "EXCESSO" in s.get("state", ""))
    neutro_count = len(stocks) - aperto_count - excesso_count
    spreads_fora = sum(1 for s in spreads.values() if s.get("regime") != "NORMAL")
    abaixo_15 = sum(1 for s in stocks.values() if s.get("price_vs_avg", 0) < -15)
    acima_15 = sum(1 for s in stocks.values() if s.get("price_vs_avg", 0) > 15)

    resumo = {"stocks_watch":f"{aperto_count} em aperto, {excesso_count} em excesso, {neutro_count} neutro","spreads":f"{spreads_fora} de {len(spreads)} fora do range normal","preco_vs_historico":f"{abaixo_15} commodities >15% abaixo, {acima_15} >15% acima"}

    return {"timestamp":datetime.now().isoformat(),"date":datetime.now().strftime("%Y-%m-%d"),"blocos":blocos,"perguntas":perguntas,"resumo":resumo,"sources":["Yahoo Finance","IBKR","CFTC","USDA","CONAB"]}

def save_reading(proc_path: Path):
    reading = generate_daily_reading(proc_path)
    output_file = proc_path / "daily_reading.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(reading, f, ensure_ascii=False, indent=2)
    return reading

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        proc = Path(sys.argv[1])
    else:
        proc = Path("../agrimacro-dash/public/data/processed")
    result = save_reading(proc)
    print(f"\n=== LEITURA DO DIA - {result['date']} ===\n")
    for bloco in result["blocos"]:
        print(f"  {bloco['title']}")
        print(f"  {bloco['body']}\n")
    print("PERGUNTAS:")
    for i, p in enumerate(result["perguntas"], 1):
        print(f"  {i}. {p}")
    print(f"\nRESUMO:")
    for key, val in result["resumo"].items():
        print(f"  {key}: {val}")
