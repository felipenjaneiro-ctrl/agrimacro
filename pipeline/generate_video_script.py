#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AgriMacro v3.2 - Video Script Generator (Step 17)
Le todos os JSONs do pipeline e gera video_script.json otimizado para Synthesia.
Usa Claude API para gerar narracao natural em PT-BR.
Output: video_script.json em processed/
"""
import json, os, sys
from datetime import datetime

PROC = os.path.join(os.path.dirname(__file__), "..", "agrimacro-dash", "public", "data", "processed")
OUT = os.path.join(PROC, "video_script.json")
KEY_PATH = os.path.join(os.path.expanduser("~"), ".anthropic_key")

def sload(name):
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

def build_video_context():
    """Build comprehensive context for video script from all JSONs"""
    ctx = []

    today = datetime.now()
    date_str = today.strftime("%d/%m/%Y")
    weekday_names = ["segunda-feira","terca-feira","quarta-feira","quinta-feira","sexta-feira","sabado","domingo"]
    weekday = weekday_names[today.weekday()]
    ctx.append(f"DATA: {weekday}, {date_str}")

    # Report daily (main narrative)
    rd = sload("report_daily.json")
    if rd:
        ctx.append(f"\n=== RELATORIO DO DIA ===")
        ctx.append(f"Titulo: {rd.get('titulo','')}")
        ctx.append(f"Subtitulo: {rd.get('subtitulo','')}")
        ctx.append(f"Resumo: {rd.get('resumo_executivo','')}")
        for d in rd.get("destaques", []):
            ctx.append(f"  Destaque [{d.get('commodity','')}]: {d.get('titulo','')} — {d.get('corpo','')[:200]}")
            ctx.append(f"    Impacto produtor: {d.get('impacto_produtor','')}")
        if rd.get("macro_e_cambio"):
            mc = rd["macro_e_cambio"]
            ctx.append(f"  Dolar/BRL: {mc.get('dolar_brl','')}")
            ctx.append(f"  Juros EUA: {mc.get('juros_eua','')}")
            ctx.append(f"  Petroleo: {mc.get('petroleo','')}")
        ctx.append(f"  Analise tecnica: {rd.get('analise_tecnica_resumo','')[:200]}")
        ctx.append(f"  Riscos: {', '.join(rd.get('principais_riscos',[]))}")

    # Prices snapshot
    pr = sload("price_history.json")
    if pr:
        ctx.append(f"\n=== PRECOS (top movers) ===")
        movers = []
        for sym, bars in pr.items():
            if not bars or len(bars) < 2:
                continue
            last = bars[-1]
            prev = bars[-2]
            close = last.get("close", 0)
            pchg = ((close - prev.get("close", 0)) / prev["close"] * 100) if prev.get("close") else 0
            movers.append((sym, close, pchg))
        movers.sort(key=lambda x: abs(x[2]), reverse=True)
        for sym, close, pchg in movers[:10]:
            ctx.append(f"  {sym}: {close:.2f} ({pchg:+.1f}%)")

    # Spreads
    sd = sload("spreads.json")
    if sd and sd.get("spreads"):
        ctx.append(f"\n=== SPREADS ===")
        for name, sp in sd["spreads"].items():
            ctx.append(f"  {sp.get('name','')}: {sp.get('current',0):.2f} | Z={sp.get('zscore_1y',0):.2f} | P={sp.get('percentile',0):.0f}%")

    # COT highlights
    cd = sload("cot.json")
    if cd and cd.get("commodities"):
        ctx.append(f"\n=== COT (destaques) ===")
        for sym in ["ZS","ZC","ZW","KC","LE","HE","CT"]:
            if sym in cd["commodities"]:
                d = cd["commodities"][sym]
                dis = d.get("disaggregated", {}).get("latest", {})
                mm = dis.get("managed_money_net", "N/A")
                ctx.append(f"  {sym}: Managed Money Net = {mm}")

    # Stocks watch
    sw = sload("stocks_watch.json")
    if sw and sw.get("commodities"):
        ctx.append(f"\n=== ESTOQUES ===")
        for sym in ["ZC","ZS","ZW","CT","LE","HE"]:
            if sym in sw["commodities"]:
                s = sw["commodities"][sym]
                ctx.append(f"  {sym}: {s.get('state','')} | Estoque: {s.get('stock_current','N/A')}")

    # Weather
    wt = sload("weather_agro.json")
    if wt and wt.get("regions"):
        ctx.append(f"\n=== CLIMA ===")
        for key, reg in wt["regions"].items():
            alerts = reg.get("alerts", [])
            p7 = reg.get("precip_7d_mm", 0)
            alert_str = f" | ALERTAS: {', '.join(a['type'] for a in alerts)}" if alerts else ""
            ctx.append(f"  {reg.get('label','')}: Precip 7d={p7:.0f}mm{alert_str}")
        enso = wt.get("enso", {})
        ctx.append(f"  ENSO: {enso.get('status','N/A')} (ONI={enso.get('oni_value','N/A')})")

    # Calendar
    cal = sload("calendar.json")
    if cal and cal.get("events"):
        today_str = datetime.now().strftime("%Y-%m-%d")
        upcoming = [e for e in cal["events"] if e["date"] >= today_str][:10]
        if upcoming:
            ctx.append(f"\n=== CALENDARIO ===")
            for e in upcoming[:5]:
                imp = " **" if e.get("impact") == "high" else ""
                ctx.append(f"  {e['date']}: {e['name']}{imp}")

    # News headlines
    nw = sload("news.json")
    if nw and nw.get("news"):
        ctx.append(f"\n=== NOTICIAS (top 5) ===")
        for art in nw["news"][:5]:
            ctx.append(f"  [{art.get('source','')}] {art.get('title','')}")

    # BCB macro
    bcb = sload("bcb_data.json")
    if bcb and bcb.get("resumo_cambio"):
        rc = bcb["resumo_cambio"]
        ctx.append(f"\n=== CAMBIO BRL/USD ===")
        ctx.append(f"  Atual: R$ {rc.get('brl_usd_atual','')} | 5d: {rc.get('var_5d','')}% | 30d: {rc.get('var_30d','')}%")

    return "\n".join(ctx)

def generate_script_with_claude(context):
    """Call Claude API to generate video script"""
    import requests

    api_key = get_api_key()
    if not api_key:
        print("  [ERROR] No Anthropic API key found")
        return None

    today = datetime.now()
    date_str = today.strftime("%d/%m/%Y")
    weekday_names = ["segunda-feira","terca-feira","quarta-feira","quinta-feira","sexta-feira","sabado","domingo"]
    weekday = weekday_names[today.weekday()]

    system_prompt = f"""Voce e o roteirista da Agri-Macro, canal de YouTube sobre commodities agricolas para produtores rurais brasileiros.
Hoje e {weekday}, {date_str}.

Crie um roteiro de video de ~7 minutos (maximo 1100 palavras de narracao, alvo 155 palavras/minuto).

REGRAS DO ROTEIRO:
- Tom: jornalistico mas acessivel, como um "Jornal Nacional do campo"
- Fale diretamente com o produtor: "Bom dia, produtor!", "Voce que planta soja..."
- Use dados REAIS dos JSONs — NUNCA invente numeros
- Cite fontes: "segundo o USDA", "dados da CFTC", "CEPEA informa"
- Cada bloco deve ter uma "mensagem para o produtor" pratica
- Termine com uma reflexao ou pergunta provocativa
- Linguagem clara, sem jargao excessivo

ESTRUTURA OBRIGATORIA (JSON):
{{
  "date": "{date_str}",
  "weekday": "{weekday}",
  "duration_target_min": 7,
  "blocks": [
    {{
      "id": "abertura",
      "title": "Abertura",
      "duration_sec": 30,
      "narration": "Texto da narracao...",
      "visual_notes": "Descricao do que aparece na tela",
      "data_refs": ["report_daily.titulo"]
    }},
    {{
      "id": "destaque_1",
      "title": "Nome do bloco",
      "duration_sec": 90,
      "narration": "Texto...",
      "visual_notes": "...",
      "data_refs": ["prices.ZS", "spreads.soy_crush"]
    }},
    ... mais 4-6 blocos ...
    {{
      "id": "encerramento",
      "title": "Encerramento",
      "duration_sec": 30,
      "narration": "Texto final...",
      "visual_notes": "Logo AgriMacro + redes sociais",
      "data_refs": []
    }}
  ],
  "total_words": 1050,
  "total_duration_sec": 420,
  "sources_cited": ["USDA WASDE", "CFTC COT", "..."]
}}

BLOCOS SUGERIDOS (adapte ao que for relevante hoje):
1. Abertura (~30s) - cumprimento + manchete do dia
2. Graos & Oleaginosas (~100s) - soja, milho, trigo
3. Carnes & Pecuaria (~80s) - boi, porco, confinamento
4. Cafe, Acucar & Softs (~70s) - se relevante
5. Macro & Cambio (~60s) - dolar, juros, petroleo
6. Clima & Safra (~50s) - previsao, alertas
7. Agenda da Semana (~30s) - WASDE, COT, eventos
8. Encerramento (~30s) - reflexao + CTA

Retorne APENAS o JSON, sem markdown, sem comentarios."""

    user_prompt = f"""Com base nos dados abaixo, gere o roteiro do video diario da AgriMacro para HOJE ({weekday}, {date_str}).
Lembre: maximo 1100 palavras de narracao, dados reais, tom acessivel.

{context}

Gere o JSON do roteiro seguindo a estrutura especificada."""

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

        # Clean markdown fences
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        script = json.loads(text)

        # Force correct date
        script["date"] = date_str
        script["weekday"] = weekday

        # Count words in narration
        total_words = 0
        total_duration = 0
        for block in script.get("blocks", []):
            narration = block.get("narration", "")
            total_words += len(narration.split())
            total_duration += block.get("duration_sec", 0)
        script["total_words"] = total_words
        script["total_duration_sec"] = total_duration

        return script

    except json.JSONDecodeError as e:
        print(f"  [ERROR] JSON parse: {e}")
        print(f"  Response text (first 500): {text[:500]}")
        return None
    except Exception as e:
        print(f"  [ERROR] Claude API call: {e}")
        return None

def main():
    print("Generating video script for Synthesia...")

    # Build context
    context = build_video_context()
    print(f"  [OK] Video context: {len(context)} chars")

    # Generate script
    script = generate_script_with_claude(context)
    if not script:
        print("  [ERROR] Failed to generate video script")
        sys.exit(1)

    # Add metadata
    script["_meta"] = {
        "generated_at": datetime.now().isoformat(),
        "generator": "AgriMacro Video Script v1.0",
        "model": "claude-sonnet-4-20250514",
        "context_chars": len(context),
    }

    # Save
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(script, f, indent=2, ensure_ascii=False)

    n_blocks = len(script.get("blocks", []))
    total_words = script.get("total_words", 0)
    total_dur = script.get("total_duration_sec", 0)
    print(f"  [OK] Script saved: {n_blocks} blocos, {total_words} palavras, {total_dur}s (~{total_dur/60:.1f}min)")
    for b in script.get("blocks", []):
        wc = len(b.get("narration","").split())
        print(f"    [{b.get('id','')}] {b.get('title','')} — {b.get('duration_sec',0)}s, {wc}w")

if __name__ == "__main__":
    main()
