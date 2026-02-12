# AgriMacro v3.2 — Documentação do Pipeline de Coleta de Dados
# ==============================================================
# ESTE ARQUIVO SERVE COMO MEMORIA ENTRE SESSOES DO CLAUDE.
# Cole o conteudo relevante no inicio de cada nova conversa.
# Ultima atualizacao: 2026-02-12
# ==============================================================

## VISAO GERAL DO PIPELINE

O AgriMacro coleta dados de 15+ fontes, processa em JSONs, e gera:
- PDF diário (18 páginas, landscape A4, tema dark)
- Vídeo narrado (MP4, 5-7 min, português)
- Dashboard Next.js

## ESTRUTURA DE PASTAS

```
C:\Users\felip\OneDrive\Área de Trabalho\agrimacro\
├── pipeline\                          ← Scripts Python de coleta
│   ├── generate_report_pdf.py         ← Gerador PDF v6 (1687 linhas)
│   ├── collect_sugar_alcohol_br.py    ← NOVO: Coletor açúcar/etanol BR
│   ├── collect_prices.py              ← Coletor de preços (Yahoo/IBKR)
│   ├── collect_cot.py                 ← Coletor COT (CFTC)
│   ├── collect_physical.py            ← Coletor mercado físico (CEPEA)
│   ├── collect_eia.py                 ← Coletor energia (EIA)
│   ├── collect_weather.py             ← Coletor clima (NOAA/Open-Meteo)
│   ├── collect_news.py                ← Coletor notícias
│   ├── collect_bcb.py                 ← Coletor BCB (dólar, Selic, IPCA)
│   └── process_editorial.py           ← Gera daily_reading + report_daily
│
├── agrimacro-dash\                    ← Dashboard Next.js
│   └── public\data\
│       ├── raw\                       ← Dados brutos
│       │   └── price_history.json     ← Histórico de preços (Yahoo/IBKR)
│       ├── processed\                 ← Dados processados
│       │   ├── physical_intl.json     ← Mercado físico (CEPEA + internac.)
│       │   ├── sugar_alcohol_br.json  ← NOVO: Dados açúcar/etanol completos
│       │   ├── eia_data.json          ← Energia (EIA)
│       │   ├── cot.json              ← Posição fundos (CFTC)
│       │   ├── spreads.json          ← Spreads calculados
│       │   ├── stocks_watch.json     ← Estoques USDA
│       │   ├── bcb_data.json         ← Macro Brasil (BCB)
│       │   ├── weather_agro.json     ← Clima
│       │   ├── calendar.json         ← Eventos
│       │   ├── news.json             ← Notícias
│       │   ├── daily_reading.json    ← Leitura diária editorial
│       │   └── report_daily.json     ← Resumo do dia
│       └── reports\
│           └── agrimacro_YYYY-MM-DD.pdf  ← PDF gerado
```

## FONTES DE DADOS — MAPA COMPLETO

### Fontes Ativas (funcionando)
| # | Fonte | Frequência | Script | JSON Output | Status |
|---|-------|-----------|--------|-------------|--------|
| 1 | Yahoo Finance | Diário | collect_prices.py | price_history.json | ✅ OK |
| 2 | IBKR (TWS) | Tempo real | collect_prices.py | price_history.json | ✅ OK |
| 3 | CFTC/COT | Semanal (sex) | collect_cot.py | cot.json | ✅ OK |
| 4 | CEPEA/ESALQ | Diário | collect_physical.py | physical_intl.json | ✅ OK |
| 5 | EIA | Semanal (qua) | collect_eia.py | eia_data.json | ✅ OK |
| 6 | BCB/IBGE | Diário | collect_bcb.py | bcb_data.json | ✅ OK |
| 7 | NOAA/Open-Meteo | Diário | collect_weather.py | weather_agro.json | ⚠️ Intermitente |
| 8 | USDA | Mensal | collect_stocks.py | stocks_watch.json | ✅ OK |
| 9 | Canal Rural/Yahoo | Diário | collect_news.py | news.json | ✅ OK |

### Fontes Novas (recém-adicionadas 2026-02-12)
| # | Fonte | Frequência | Script | JSON Output | Status |
|---|-------|-----------|--------|-------------|--------|
| 10 | CEPEA Açúcar | Diário | collect_sugar_alcohol_br.py | sugar_alcohol_br.json + physical_intl.json | 🆕 TESTAR |
| 11 | CEPEA Etanol | Diário | collect_sugar_alcohol_br.py | sugar_alcohol_br.json + physical_intl.json | 🆕 TESTAR |
| 12 | ANP Combustíveis | Semanal | collect_sugar_alcohol_br.py | sugar_alcohol_br.json | 🆕 TESTAR |
| 13 | UNICA Produção | Quinzenal/safra | collect_sugar_alcohol_br.py | sugar_alcohol_br.json | 🆕 TESTAR |
| 14 | CONSECANA ATR | Mensal | collect_sugar_alcohol_br.py | sugar_alcohol_br.json | 🆕 TESTAR |

### Fontes Desejadas (ainda não implementadas)
| # | Fonte | O que falta | Prioridade |
|---|-------|------------|-----------|
| 15 | B3 Etanol Futuro (ETH) | Verificar acesso via IBKR/TWS | MEDIA |
| 16 | Stooq | Integração instável, considerar remover | BAIXA |

## FORMATO DOS JSONs — REFERÊNCIA RÁPIDA

### physical_intl.json (formato que o PDF lê)
```json
{
  "international": {
    "ZS_BR": {
      "label": "Soja",
      "price": 126.95,
      "price_unit": "R$/sc 60kg",
      "trend": "+1.2% d/d",
      "source": "CEPEA/ESALQ via NA",
      "period": "11/02/2026",
      "history": [{"date": "2026-02-10", "value": 125.5}, ...]
    },
    "SB_BR": {
      "label": "Acucar Cristal",
      "price": 142.50,
      "price_unit": "R$/saca 50kg",
      "trend": "+0.8% d/d",
      "source": "CEPEA/ESALQ",
      "period": "2026-02-12",
      "history": [...]
    },
    "ETH_BR": {
      "label": "Etanol Hidratado",
      "price": 2.8500,
      "price_unit": "R$/litro",
      "trend": "-0.3% d/d",
      "source": "CEPEA/ESALQ",
      "period": "2026-02-12",
      "history": [...]
    }
  }
}
```

### sugar_alcohol_br.json (dados completos do setor)
```json
{
  "metadata": {"date": "2026-02-12", "version": "1.0"},
  "cepea": {
    "acucar_cristal": {"price": 142.50, "unit": "R$/saca 50kg", ...},
    "etanol_hidratado": {"price": 2.85, "unit": "R$/litro", ...},
    "etanol_anidro": {"price": 3.10, "unit": "R$/litro", ...}
  },
  "anp": {
    "etanol_bomba": {"preco_medio": 3.899, "estado": "SP", ...},
    "gasolina_bomba": {"preco_medio": 5.799, ...},
    "paridade_etanol_gasolina": 0.672
  },
  "unica": {
    "moagem_cana_mil_ton": 580000,
    "mix_acucar_pct": 46.5,
    "mix_etanol_pct": 53.5, ...
  },
  "consecana": {
    "preco_atr_rs_kg": 1.0842,
    "atr_medio_kg_ton": 142.5, ...
  },
  "spreads": {
    "paridade_bomba": {"valor": 0.672, "interpretacao": "ETANOL COMPENSA"},
    "paridade_exportacao": {"ny_rs_sc50": 135.20, "cepea_rs_sc50": 142.50, ...},
    "spread_anidro_hidratado": {"valor_rs": 0.25},
    "margem_usina_acucar": {"margem_rs_ton": 45.30, ...}
  }
}
```

## ORDEM DE EXECUÇÃO DO PIPELINE

```powershell
# 1. Coleta de dados (rodar na ordem)
cd C:\Users\felip\OneDrive\Área de Trabalho\agrimacro\pipeline

python collect_prices.py              # Yahoo + IBKR → price_history.json
python collect_bcb.py                 # BCB → bcb_data.json
python collect_physical.py            # CEPEA soja/milho/cafe/boi → physical_intl.json
python collect_sugar_alcohol_br.py    # CEPEA açúcar/etanol + ANP + UNICA → sugar_alcohol_br.json
python collect_eia.py                 # EIA → eia_data.json
python collect_cot.py                 # CFTC COT → cot.json (sexta-feira)
python collect_weather.py             # NOAA → weather_agro.json
python collect_news.py                # Notícias → news.json

# 2. Processamento editorial (usa dados acima para gerar leitura do dia)
python process_editorial.py           # → daily_reading.json + report_daily.json

# 3. Geração de outputs
python generate_report_pdf.py         # → PDF 18 páginas
# python generate_video.py            # → MP4 narrado (quando implementado)
```

## n8n — CONFIGURAÇÃO LOCAL

O n8n roda localmente no Windows (http://localhost:5678).

### Workflow Principal: "AgriMacro Daily Pipeline"
- **Trigger:** Cron → Segunda a Sexta, 18:00 BRT
- **Nó 1:** Execute Command → `cd C:\Users\felip\...\pipeline && python collect_prices.py`
- **Nó 2:** Execute Command → `python collect_bcb.py`
- **Nó 3:** Execute Command → `python collect_physical.py`
- **Nó 4:** Execute Command → `python collect_sugar_alcohol_br.py`  ← ADICIONAR
- **Nó 5:** Execute Command → `python collect_eia.py`
- **Nó 6:** Execute Command → `python collect_news.py`
- **Nó 7:** Execute Command → `python process_editorial.py`
- **Nó 8:** Execute Command → `python generate_report_pdf.py`
- **Nó 9:** Git commit + push (via PowerShell)

### Para adicionar o novo coletor no n8n:
1. Abra http://localhost:5678
2. Abra o workflow "AgriMacro Daily Pipeline"
3. Adicione um nó "Execute Command" após collect_physical.py
4. Comando: `cd C:\Users\felip\OneDrive\Área de Trabalho\agrimacro\pipeline && python collect_sugar_alcohol_br.py`
5. Conecte na sequência
6. Salve e ative

## PROBLEMAS CONHECIDOS

| Problema | Status | Solução |
|----------|--------|---------|
| CEPEA muda HTML periodicamente | Recorrente | Re-scraping necessário quando quebra |
| Dashboard não carrega PDF novo | Aberto | Bug no Next.js, investigar |
| Weather data vazio (NOAA) | Intermitente | Open-Meteo como fallback |
| COT formato mudou (CFTC) | Resolvido v6 | Parser atualizado |
| Stooq instável | Aberto | Considerar remover fonte |
| CONSECANA sem API | Permanente | Scraping frágil, entrada manual backup |
| UNICA exige JS/navegação | Permanente | CSV manual como fallback |

## REGRAS DO PROJETO (IMUTÁVEIS)

1. **ZERO MOCK** — só dados reais de fontes oficiais
2. **Soja + Milho + Boi são OBRIGATÓRIOS** em vídeos e relatórios
3. **Evitar siglas técnicas** (COT, CL, ZS) — linguagem acessível
4. **Português sempre** — audiência é produtor rural brasileiro
5. **Tema dark** — fundo escuro, texto claro
6. **Landscape A4** — formato horizontal
7. **reportlab + matplotlib** — bibliotecas de geração PDF

## COMO USAR ESTE DOCUMENTO COM O CLAUDE

Quando iniciar uma nova sessão do Claude para trabalhar no AgriMacro:

1. Cole este documento (ou a seção relevante) no início da conversa
2. Anexe o arquivo Python que quer modificar
3. Descreva o problema ou melhoria desejada
4. Peça comandos PowerShell prontos para copiar/colar

### Exemplo de abertura de sessão:
```
"Sou Felipe, trader de commodities. Tenho o AgriMacro v3.2.
[cola a seção relevante deste documento]
[anexa o .py que precisa mudar]
Problema: [descreve]
Preciso de comandos PowerShell prontos."
```

## CHANGELOG

### 2026-02-12 — Expansão Açúcar & Álcool
- Criado `collect_sugar_alcohol_br.py` (CEPEA açúcar/etanol + ANP + UNICA + CONSECANA)
- Output: `sugar_alcohol_br.json` (dados completos do setor)
- Atualiza `physical_intl.json` com SB_BR, ETH_BR, ETN_BR
- Spreads: paridade bomba, paridade exportação, spread anidro/hidratado, margem usina
- Documentação do pipeline criada

### 2026-02-12 — PDF v6
- 18 páginas (era 15 na v5)
- Página 13: Açúcar & Álcool (nova)
- Badges CHICAGO/BRASIL em todas as páginas
- COT com legenda 4 cores
- Spreads com explicações em português
- replace_tickers() para linguagem acessível
