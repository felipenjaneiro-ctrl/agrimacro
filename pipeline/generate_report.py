"""
Agri-Macro Report v1.0 - Relatorio Diario Preliminar
Gera relatorio estruturado em PT-BR para produtores rurais e profissionais de commodities.
Usa Claude API para analise + dados reais do dashboard.
Output: report_daily.json (alimenta PDF + video)
"""
import json, os, sys
from datetime import datetime

# Paths
PROC = os.path.join(os.path.dirname(__file__), "..", "agrimacro-dash", "public", "data", "processed")
OUT = os.path.join(PROC, "report_daily.json")
KEY_PATH = os.path.join(os.path.expanduser("~"), ".anthropic_key")

def load_json(name):
    p = os.path.join(PROC, name)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return None

def get_api_key():
    if os.path.exists(KEY_PATH):
        k = open(KEY_PATH).read().strip()
        if k:
            return k
    return os.environ.get("ANTHROPIC_API_KEY", "")

def build_market_context():
    """Build comprehensive market context from all dashboard data"""
    ctx = []

    # 1. Prices - latest close and changes
    prices = load_json("price_history.json")
    if prices:
        ctx.append("=== PRECOS ATUAIS (fechamento mais recente) ===")
        for sym, bars in prices.items():
            if not bars:
                continue
            last = bars[-1]
            prev = bars[-2] if len(bars) > 1 else None
            close = last.get("close", 0)
            chg = ""
            if prev:
                diff = close - prev.get("close", 0)
                pct = (diff / prev["close"] * 100) if prev["close"] else 0
                chg = f" | 1D: {diff:+.2f} ({pct:+.1f}%)"
            # 52-week high/low
            recent = bars[-252:] if len(bars) >= 252 else bars
            highs = [b["high"] for b in recent if b.get("high")]
            lows = [b["low"] for b in recent if b.get("low")]
            hi52 = max(highs) if highs else 0
            lo52 = min(lows) if lows else 0
            ctx.append(f"  {sym}: {close:.2f}{chg} | 52w: {lo52:.2f}-{hi52:.2f}")

    # 2. Spreads
    spreads = load_json("spreads.json")
    if spreads and spreads.get("spreads"):
        ctx.append("\n=== SPREADS E Z-SCORES ===")
        for name, sp in spreads["spreads"].items():
            ctx.append(f"  {sp.get('name','')}: {sp.get('current',0):.2f} | Z-score 1Y: {sp.get('zscore_1y',0):.2f} | Percentil: {sp.get('percentile',0):.0f}% | Regime: {sp.get('regime','')}")

    # 3. COT - net positions
    cot = load_json("cot.json")
    if cot and cot.get("commodities"):
        ctx.append("\n=== COT (Commitment of Traders) ===")
        for sym, data in cot["commodities"].items():
            leg = data.get("legacy", {})
            dis = data.get("disaggregated", {})
            latest_leg = leg.get("latest", {}) if leg else {}
            latest_dis = dis.get("latest", {}) if dis else {}
            comm_net = latest_leg.get("comm_net", "N/A")
            noncomm_net = latest_leg.get("noncomm_net", "N/A")
            mm_net = latest_dis.get("managed_money_net", "N/A")
            ctx.append(f"  {sym}: Commercials Net={comm_net} | NonComm Net={noncomm_net} | ManagedMoney Net={mm_net}")

    # 4. Stocks Watch
    stocks = load_json("stocks_watch.json")
    if stocks and stocks.get("commodities"):
        ctx.append("\n=== ESTOQUES (Stocks Watch) ===")
        for sym, st in stocks["commodities"].items():
            real = st.get("data_available", {}).get("stock_real", False)
            if real:
                ctx.append(f"  {sym}: Estoque={st.get('stock_current','N/A')} {st.get('stock_unit','')} | Media={st.get('stock_avg','N/A')} | Estado: {st.get('state','')}")
            else:
                ctx.append(f"  {sym}: Preco vs Media 5Y: {st.get('price_vs_avg',0):+.1f}% | Estado: {st.get('state','')}")

    # 5. Physical prices
    physical = load_json("physical.json")
    if physical and isinstance(physical, dict):
        ctx.append("\n=== PRECOS FISICOS US ===")
        markets = physical.get("markets", physical)
        if isinstance(markets, list):
            for m in markets[:10]:
                ctx.append(f"  {m.get('market','')}: {m.get('price','')} {m.get('unit','')}")
        elif isinstance(markets, dict):
            for name, m in list(markets.items())[:10]:
                ctx.append(f"  {name}: {m}")

    # 6. Physical international
    phys_intl = load_json("physical_intl.json")
    if phys_intl:
        ctx.append("\n=== FISICO INTERNACIONAL ===")
        if isinstance(phys_intl, dict):
            for region, data in phys_intl.items():
                if isinstance(data, list):
                    for item in data[:5]:
                        ctx.append(f"  {region}: {item.get('product','')} = {item.get('price','')} {item.get('unit','')}")
                elif isinstance(data, dict):
                    ctx.append(f"  {region}: {json.dumps(data, ensure_ascii=False)[:200]}")

    # 7. FRED macro
    news = load_json("news.json")
    if news and news.get("fred"):
        ctx.append("\n=== INDICADORES MACRO (FRED) ===")
        for key, ind in news["fred"].items():
            chg = f" ({ind['change']:+.2f})" if ind.get("change") is not None else ""
            ctx.append(f"  {ind['name']}: {ind['value']}{chg} [{ind['date']}]")

    # 8. Calendar - upcoming high impact
    cal = load_json("calendar.json")
    if cal and cal.get("events"):
        today = datetime.now().strftime("%Y-%m-%d")
        upcoming = [e for e in cal["events"] if e["date"] >= today][:15]
        if upcoming:
            ctx.append("\n=== CALENDARIO - PROXIMOS EVENTOS ===")
            for e in upcoming:
                imp = " ** HIGH IMPACT **" if e.get("impact") == "high" else ""
                ctx.append(f"  {e['date']}: {e['name']} [{e.get('category','')}]{imp}")

    # 9. Seasonality highlights
    season = load_json("seasonality.json")
    if season:
        ctx.append("\n=== SAZONALIDADE (destaques) ===")
        for sym, data in list(season.items())[:10]:
            if isinstance(data, dict) and data.get("series"):
                years = list(data["series"].keys())
                ctx.append(f"  {sym}: {len(years)} anos de dados disponiveis")

    # 10. Weather data (if available)
    weather = load_json("weather_agro.json")
    if weather and weather.get("regions"):
        ctx.append("\n=== CLIMA AGRICOLA ===")
        for region_key, region_data in weather["regions"].items():
            label = region_data.get("label", region_key)
            current = region_data.get("current", {})
            precip_vs = region_data.get("precip_vs_normal_pct", "N/A")
            alert = region_data.get("alert", "nenhum")
            ctx.append(f"  {label}: Temp={current.get('temp_c','N/A')}C | Precip vs Normal: {precip_vs}% | Alerta: {alert}")
        enso = weather.get("enso_status", "N/A")
        ctx.append(f"  ENSO: {enso}")

    # 11. News headlines (if available)
    if news and news.get("news"):
        ctx.append("\n=== NOTICIAS RECENTES (top 10) ===")
        for art in news["news"][:10]:
            ctx.append(f"  [{art.get('source','')}] {art.get('title','')}")

    return "\n".join(ctx)

def generate_report_with_claude(context):
    """Call Claude API to generate structured report"""
    import requests

    api_key = get_api_key()
    if not api_key:
        print("  [ERROR] No Anthropic API key found")
        return None

    today = datetime.now().strftime("%d/%m/%Y")
    weekday_names = ["segunda-feira","terca-feira","quarta-feira","quinta-feira","sexta-feira","sabado","domingo"]
    weekday = weekday_names[datetime.now().weekday()]

    system_prompt = f"""Voce e o analista-chefe da Agri-Macro, uma plataforma gratuita e educativa de inteligencia em commodities agricolas.
Seu publico-alvo: produtores rurais brasileiros, pecuaristas, traders de commodities, profissionais do agronegocio.

*** DATA DO RELATORIO: {weekday}, {today} ***
Use ESTA data em todos os campos do relatorio. NAO use a data dos dados de mercado.
Os dados de mercado podem ter sido coletados em dias anteriores (finais de semana, feriados), mas o relatorio e de HOJE.

REGRAS:
- Escreva em portugues brasileiro, tom profissional mas acessivel
- NAO use jargao excessivo - explique termos tecnicos quando necessario
- Foque nas commodities relevantes para o Brasil: soja, milho, trigo, cafe, acucar, algodao, boi gordo, suinos
- Mencione tambem energia (petroleo) e macro (dolar, juros) quando impactam o agro
- Seja objetivo e direto, com dados concretos
- NAO faca recomendacoes de investimento - apenas informe e eduque
- Inclua contexto de COMO os dados afetam o produtor rural na pratica
- Use analogias do campo quando possivel para explicar conceitos de mercado
- No resumo_executivo, COMECE com "{weekday.capitalize()}, {today}" para deixar claro a data

FORMATO DO RELATORIO (retorne JSON valido):
{{
  "titulo": "Titulo chamativo do dia (max 80 chars)",
  "subtitulo": "Frase de destaque do dia",
  "data": "{today}",
  "dia_semana": "{weekday}",
  "resumo_executivo": "Paragrafo de 3-4 linhas resumindo o dia no mercado — COMECE com a data de hoje",
  "destaques": [
    {{"titulo": "...", "corpo": "2-3 paragrafos", "impacto_produtor": "Como isso afeta o produtor", "commodity": "SYM"}}
  ],
  "tabela_precos": [
    {{"commodity": "Soja", "simbolo": "ZS", "preco_usd": 0.0, "variacao_1d": "+0.0%", "variacao_semanal": "+0.0%", "tendencia": "alta/baixa/lateral", "comentario": "breve"}}
  ],
  "macro_e_cambio": {{
    "dolar_brl": "comentario sobre cambio e impacto",
    "juros_eua": "comentario sobre Fed e impacto commodities",
    "petroleo": "comentario sobre energia"
  }},
  "calendario_semana": [
    {{"data": "DD/MM", "evento": "nome", "impacto": "alto/medio", "relevancia": "porque importa"}}
  ],
  "analise_tecnica_resumo": "Paragrafo sobre padroes tecnicos relevantes (spreads, sazonalidade, COT)",
  "pergunta_do_dia": "Uma pergunta provocativa para o produtor refletir",
  "frase_do_dia": "Uma frase motivacional ou educativa relacionada ao mercado",
  "hashtags": ["#agrimacro", "#commodities", "...mais 3-5 relevantes"],
  "score_volatilidade": 1-10,
  "principais_riscos": ["risco 1", "risco 2", "risco 3"]
}}

Retorne APENAS o JSON, sem markdown, sem comentarios, sem texto antes ou depois."""

    user_prompt = f"""DATA DO RELATORIO: {weekday}, {today}
(Os dados abaixo podem ter datas anteriores — isso e normal. O relatorio e de HOJE, {today}.)

Com base nos dados de mercado abaixo, gere o relatorio diario da Agri-Macro para HOJE ({weekday}, {today}).

{context}

Gere o JSON do relatorio seguindo exatamente o formato especificado. Lembre-se: data = "{today}", dia_semana = "{weekday}"."""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 4096,
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}]
            },
            timeout=120
        )

        if resp.status_code != 200:
            print(f"  [ERROR] Claude API: HTTP {resp.status_code}")
            print(f"  {resp.text[:500]}")
            return None

        data = resp.json()
        text = data["content"][0]["text"]

        # Clean potential markdown fences
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        report = json.loads(text)

        # Force correct date in output regardless of what Claude returned
        report["data"] = today
        report["dia_semana"] = weekday

        return report

    except json.JSONDecodeError as e:
        print(f"  [ERROR] JSON parse: {e}")
        print(f"  Response text: {text[:500]}")
        return None
    except Exception as e:
        print(f"  [ERROR] Claude API call: {e}")
        return None

def main():
    print("Generating Agri-Macro daily report...")

    # Build context from all data sources
    context = build_market_context()
    print(f"  [OK] Market context: {len(context)} chars, {context.count(chr(10))} lines")

    # Generate report via Claude
    report = generate_report_with_claude(context)
    if not report:
        print("  [ERROR] Failed to generate report")
        sys.exit(1)

    # Add metadata
    report["_meta"] = {
        "generated_at": datetime.now().isoformat(),
        "generator": "Agri-Macro Report v1.0",
        "model": "claude-sonnet-4-20250514",
        "context_chars": len(context),
        "data_sources": ["prices","cot","spreads","stocks","physical","physical_intl","fred","calendar","seasonality","weather","news"]
    }

    # Save
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"  [OK] Report saved: {OUT}")
    print(f"  Titulo: {report.get('titulo','')}")
    print(f"  Data: {report.get('data','')} ({report.get('dia_semana','')})")
    print(f"  Destaques: {len(report.get('destaques',[]))}")
    print(f"  Score volatilidade: {report.get('score_volatilidade','')}/10")

if __name__ == "__main__":
    main()
