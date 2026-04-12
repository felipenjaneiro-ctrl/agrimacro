# CLAUDE.md — AgriMacro Intelligence

Este arquivo define o contexto permanente do projeto AgriMacro para o Claude Code.
Leia este arquivo inteiro antes de qualquer acao no projeto.

---

## IDENTIDADE DO PROJETO

**AgriMacro Intelligence** e uma plataforma proprietaria de analise de commodities agricolas que gera:
- Relatorios PDF diarios em portugues para produtores rurais brasileiros
- Videos de analise de mercado em portugues e ingles
- Dashboard interativo com dados em tempo real

**Dono e operador**: Felipe (trader e analista de commodities, baseado em Orlando FL)
**Perfil tecnico**: nao-programador -- toda implementacao tecnica e feita com assistencia do Claude
**Ambiente**: Windows 11, PowerShell, OneDrive

---

## LOCALIZACAO DO PROJETO

```
C:\Users\felip\OneDrive\Area de Trabalho\agrimacro\
```

Estrutura principal:
```
agrimacro/
  pipeline/
    run_pipeline.py        <- pipeline principal (18 steps)
  agrimacro-dash/          <- dashboard Next.js (localhost:3000)
  data/
    bilateral/             <- 9 JSONs de analise bilateral
  generate_report_pdf.py   <- gerador de PDF ReportLab
  CLAUDE.md                <- este arquivo
```

---

## COMANDOS ESSENCIAIS (PowerShell)

Rodar pipeline:
```powershell
python pipeline\run_pipeline.py
```

Iniciar dashboard:
```powershell
cd agrimacro-dash
npm run dev
```

Se o Next.js travar:
```powershell
Remove-Item -Recurse -Force .next
```

Autenticar Claude Code:
```powershell
# OAuth via PowerShell -- ja configurado
```

---

## PRINCIPIO ZERO MOCK (NAO NEGOCIAVEL)

**Nunca** use dados hardcoded, fabricados ou mocados em nenhum script ou componente.

Regras obrigatorias:
- Se uma fonte de dados falhar, setar `is_fallback=True` e retornar `null` -- NUNCA um valor fake
- Nunca hardcodar preco, taxa de cambio, producao ou qualquer dado de mercado
- Nunca criar fallback silencioso que retorna dado inventado

Fontes de dados validas e aprovadas:
- **USDA**: WASDE, NASS, ERS, PSD (bulk CSV)
- **CONAB**: safras e estoques brasileiros
- **CFTC**: posicionamento COT (Legacy + Disaggregated)
- **CME / ICE**: dados de mercado futuros
- **BCB/SGS**: cambio BRL/USD e macroeconomia brasileira
- **IBGE/SIDRA**: dados socioeconomicos Brasil
- **EIA**: energia (petroleo e gas)
- **Noticias Agricolas** (noticiasagricolas.com.br): republica dados CEPEA confiavelmente
- **Stooq.com**: series historicas de precos
- **balanca.economia.gov.br**: balanca comercial

Fontes com problemas conhecidos:
- `api.comexstat.mdic.gov.br`: bloqueado na rede do Felipe -- usar alternativa
- CEPEA oficial: retorna 403 -- usar Noticias Agricolas como proxy

---

## PALETA VISUAL (obrigatoria em todo output visual)

```
Background:  #0E1A24
Painel:      #142332
Verde:       #00C878  (alta, positivo, OK)
Vermelho:    #DC3C3C  (baixa, negativo, erro)
Ouro:        #DCB432  (neutro, destaque, aviso)
Texto:       #FFFFFF
Texto muted: #8899AA
```

Fonte: **DejaVu Sans** (compativel com Windows, previne mojibake)
Unicode: sempre usar escape sequences -- NUNCA caracteres especiais diretos no codigo Python

---

## DASHBOARD (Next.js)

URL: `localhost:3000`
Arquivo principal: `agrimacro-dash/components/dashboard.tsx`

Regras de navegacao:
- Navegacao FLAT -- sem sub-paginas ou hierarquias aninhadas
- Felipe tentou sub-paginas uma vez e reverteu imediatamente -- nao sugerir

Estrutura de cada commodity (7 secoes verticais):
1. Composite Signal Scorecard (Technical, COT, Seasonal, Fundamentals, Physical)
2. Candlestick chart com indicadores completos
3. Forward curve (ate 362 contratos)
4. Sazonalidade com bandas +/-1 sigma (19 commodities)
5. COT panels (Legacy + Disaggregated com zoom sincronizado)
6. Fundamentals
7. Physical market

---

## 19 COMMODITIES COBERTAS

**Graos**: ZC (milho), ZS (soja), ZM (farelo), ZL (oleo), ZW (trigo CBOT), KE (trigo KC)
**Pecuaria**: LE (boi gordo), GF (feeder cattle), HE (suino)
**Softs**: CT (algodao), SB (acucar), KC (cafe), CC (cacau)
**Energia**: CL (petroleo), NG (gas natural)
**Metais**: GC (ouro), SI (prata)
**FX**: DX (dolar index), BRL (real brasileiro)

---

## PIPELINE PDF (ReportLab)

Arquivo: `generate_report_pdf.py`
- Paginas `pg_physical` e `pg_news`: DESABILITADAS (comentadas) -- nao reabilitar sem instrucao
- 9 JSONs bilaterais em `data/bilateral/`
- Patches sempre via arquivos `.py` standalone -- NUNCA edicao manual direta
- Backup obrigatorio antes de qualquer mudanca: nomear `.bak_[descricao]`
- Validacao AST obrigatoria antes de modificar `generate_report_pdf.py`

---

## VIDEOS AGRIMACRO

Stack: edge-tts + Pillow (slides) + ffmpeg (montagem)
Narradores:
- Portugues: `pt-BR-AntonioNeural`
- Ingles: `en-US-GuyNeural`

Estrutura: 9 cenas, 7-10 minutos total
Commodities obrigatorias em TODO video: **soja, milho e gado**
Linguagem: acessivel para produtor rural -- sem tickers tecnicos (ZS, COT, CL, etc.)

---

## REGRAS GERAIS PARA O CLAUDE CODE

1. **Sempre criar backup** antes de modificar arquivos existentes
2. **Sempre usar PowerShell** -- comandos bash nao funcionam no ambiente do Felipe
3. **Sempre validar** JSONs gerados antes de confirmar sucesso
4. **Nunca modificar** arquivos diretamente -- criar script de patch separado
5. **Confirmar** com Felipe antes de qualquer acao irreversivel
6. **Falhar ruidosamente** -- erros devem ser visiveis, nunca silenciosos
7. **Sem dependencias desnecessarias** -- preferir stdlib Python e ferramentas ja instaladas

---

## SKILLS DISPONIVEIS (instaladas no claude.ai)

Council Generic, Council AgriMacro, Deep Research Synthesizer, Competitive Intelligence,
Source Validation, Knowledge Structuring, Code Review (com Zero Mock Audit),
Workflow Automation, Excalidraw Diagram, Infographic Builder, Flowchart Builder,
SCQA Writing, Content Repurposing, Tone Enforcer, LongForm Compressor,
Structured Copywriting, Video Script Generator, Video Editing Planner,
Hook Generator, Caption Formatter

---

*Ultima atualizacao: Abril 2026*
