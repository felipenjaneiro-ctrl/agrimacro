# AgriMacro Intelligence — Visão Arquitetural Completa v2

**Documento criado:** 30 de abril de 2026
**Sessão:** Felipe + Claude (chat principal)
**Status:** Visão consolidada, sob acordo
**Uso pretendido:** 100% pessoal/privado, NÃO comercial
**Filosofia analítica:** Pragmática, escola Chicago, anti-utopia

---

## 1. Princípios fundamentais

### 1.1 Filosofia operacional
- **Trade é a única fonte de renda** — sistema precisa qualidade institucional MAS prática
- **"Construir pra mim primeiro"** — comercialização 100% fora do radar
- **Decisão clara, não dado bruto** — sistema entrega ação, não relatório
- **Não-fardo** — intervenção pontual estruturada, sem exigir presença diária
- **P&L mensurável** — cada decisão rastreada e validada empiricamente

### 1.2 Filosofia analítica
- **Escola Chicago, pragmático, capitalista** — Friedman, Hayek, Sowell, Schumpeter
- **Realismo sobre utopia** — Mearsheimer, Kissinger, Luttwak, Vaclav Smil
- **Empírico sobre teorético** — Reinhart-Rogoff, Mauboussin, Bulkowski
- **Operadores reais** — Soros, Druckenmiller, Livermore (com filtro), Tudor
- **Ciclos sobre tendências** — austríacos, Minsky, Kindleberger, Strauss-Howe
- **Fator humano sem moralismo** — Kahneman, Thaler, demografia (Goodhart-Pradhan)

### 1.3 Princípios técnicos
- **ZERO MOCK** — apenas dados reais (USDA, CONAB, CFTC, CEPEA, CME, ICE, EIA, BCB/SGS, IBGE)
- **Tagueamento de proveniência** — cada conclusão sabe sua origem
- **Coordenadores: Felipe + Claude** — auditável, sem black box
- **Multi-AI distribuído por agent** — diversidade cognitiva real, sem orquestrador único

---

## 2. Arquitetura em 5 camadas

### Camada 0 — Fontes de dados

**AgriMacro Dashboard** (existente):
- 19 commodities: ZC, ZS, ZM, ZL, ZW, KE, LE, GF, HE, CT, SB, KC, CC, CL, NG, GC, SI, DX, BRL
- JSONs em data/processed/ e pipeline/
- Cron VPS diário (09:15 ET)
- Sync portfolio IBKR via SCP

**Skill método Felipe** (Camada 2 base):
- 7 filtros: Seguradora, Martingale, Banca do bicho, Curva (filtro zero), Sazonalidade, Adversário-vira-upside, Liquidez
- 4 axiomas: tese-cêntrica, hedge cruzado é ilusão, fato-vs-evento, fundamental confirma + técnico aciona
- Método 5 etapas: Tese central → Sub-eixos com prob → 7 filtros → Sinais invalidação → Decision tree

**Super-páginas por commodity** (UX):
- 1 página única por commodity (não fragmentada em painéis múltiplos)
- Calendário e Portfólio MANTÉM separados (não fundem)
- Layout intuitivo, equilibrado, profissional, fácil compreensão

### Camada 0.5 — Quantitative Sensitivity (Sobol/Morris)

**Quantitative Sensitivity Agent (Claude):**
- Sobol Indices: 1st/Total/Higher-Order
- Morris Method: triagem rápida
- Backtesting Opportunity Ranker (walk-forward 2015-2026)
- Output: vetor de sensibilidade dos fatores
- **Frequência:** background diário (cron VPS)
- Alimenta: Devil's Advocate (Gemini) + Chairman do Council
- Pesos probabilísticos pra Council Chairman

### Camada Macro — 5 agents externos + 2 internos

**Externos:**
- **Grok (xAI):** posts X, sentimento institucional últimas 24-48h
- **Perplexity Pro:** pesquisa factual + artigos acadêmicos com citações
- **Gemini Advanced:** Devil's Advocate (validação cruzada anti-bias)

**Internos (Claude + RAG livros):**
- **Bonds & Treasuries Agent**
- **História Econômica Agent (especialista ciclos)**

**Síntese Macro:**
- Claude integra os 5 outputs
- Gera **Tese Macroeconômica Semanal**
- Frequência: semanal (domingo 18:00 ET via cron)
- **Calibração diária:** cron 09:00 ET checa pontos de confirmação/descontinuação
- Se ponto descontinua: alerta dashboard + reestudo agendado
- Tese Macro nortea Camada Micro

### Camada Micro — 12 super especialistas Claude

Cada especialista tem:
- **Skill institucional profunda** (livros + papers da área)
- **Checklist canônico** com pesos % por situação (regime de mercado, vol, sazonalidade)
- **Acesso a knowledge bases relevantes** (não a todas — especialização)
- **Acesso a Trade Journal Felipe** (aprendizado contínuo)
- **Output estruturado** (não análise solta)

Dos 12 especialistas, exemplos:
1. Análise Técnica Institucional (CMT-grade)
2. COT & Posicionamento
3. Fundamental Commodities
4. Opções (Premium Selling / Livermore aplicado)
5. Geopolítica & Macro Outsider
6. Risk Management / Position Sizing
7. Brasil & EM (perspectiva pragmática)
8. Análise Comportamental & Sentimento
9. Sazonalidade & Ciclos
10. Spreads & Bilateral
11. Climatologia agrícola (NASA POWER, CONAB, USDA)
12. Bonds-commodities cross-analysis

Recebem: Tese Macro + AgriMacro JSONs + Trade Journal + skills + checklists
Geram: tese pontual por commodity

### Council Final — 5 advisors + Chairman

**5 advisors padrão:**
1. **Bear Case** (Contrarian) — riscos não precificados
2. **Bull Case** (Expansionista) — upside subestimado
3. **First Principles** (Estruturalista) — drivers reais longo prazo
4. **Macro Outsider** — lentes financeiras puras
5. **Operacional/Executor** — timing, sizing, stops

**Peer review anônimo** entre advisors.

**Chairman:**
- Síntese final
- Usa pesos probabilísticos do Sobol/Morris
- Output: relatório acionável + ação concreta + próximo passo

**Modo conversacional pós-relatório:**
- Tu pode conversar com qualquer advisor individualmente
- Conversa iterativa fluida
- Histórico salvo em DB

**Frequência Relatório Final:** sob demanda (botão dashboard)
**Onde aparece:** HTML interativo principal + Export PDF sob demanda

### Camada 4 — Aprendizado & Memória

**Trade Journal estruturado:**
- Cada decisão de trade vira entry estruturada
- Inputs: tese, sinais que justificaram, ação tomada, resultado imediato
- Captura **desvios** com justificativa (ex: rolagem ZL aos 22d em vez de 14d)
- **Follow-up automático:** 2-4 semanas depois, sistema pergunta resultado
- **Namespace global:** todos agents leem (Council, 12 especialistas, Sobol)

**Decision Patterns Memory:**
- Padrões fora do livro (quando Felipe desvia da regra)
- Acumula ao longo de meses
- Vira fonte de aprendizado: "Felipe historicamente antecipa rolagens em ZL quando IV cai e técnico é favorável → 73% acerto"

**Skill Calibration Engine:**
- Track record de cada advisor do Council
- Track record de cada especialista
- Pesos ajustam empiricamente conforme acerto/erro

**Knowledge Distillation:**
- Trade Journal + Decision Patterns viram input de RAG
- Sistema fica mais inteligente conforme Felipe opera
- Loop de feedback contínuo

---

## 3. Conhecimento empírico (knowledge bases)

### Princípio de acesso
- Cada agent acessa knowledge bases **relevantes ao seu domínio**
- Trade Journal é **namespace global** (todos leem)
- Não-exposição total evita contaminação cruzada

### Mapa de acesso

| Knowledge base | Agents com acesso |
|---|---|
| Bonds & Treasuries | Bonds Agent, Macro Outsider, Síntese Macro |
| História Econômica | História Agent, Estruturalista, Síntese Macro |
| Livermore premium selling | 12 especialistas (opções), Executor (Council) |
| Multifatorial 15-20 anos | 12 especialistas (técnico, fundamental), todo Council |
| Backtesting Opportunity Ranker | Sobol/Morris, Devil's Advocate, Chairman |
| Trade Journal Felipe | TODOS (global) |
| Skill método Felipe | Council inteiro, 12 especialistas |
| Checklists canônicos por commodity | 12 especialistas (cada um seu) |
| Geopolítica realismo | Macro Outsider, Geopolítica especialista |
| Comportamental | Especialista comportamental, Devil's Advocate |
| Análise técnica institucional | Técnico especialista, Bull/Bear Council |

### Implementação técnica
- Vector DB local no VPS (Chroma é suficiente, $0 adicional)
- Namespaces ou collections separadas por knowledge base
- Cada agent tem lista de namespaces autorizados no system prompt
- Query retorna apenas chunks dos namespaces autorizados

### Estudos próprios (knowledge bases destiladas)

Esses não são livros — são produtos do AgriMacro pós-viagem:

1. **Trade Journal AgriMacro** — histórico operacional Felipe
2. **Skill método Felipe** — 7 filtros + 4 axiomas formalizados
3. **Backtesting Opportunity Ranker** — walk-forward 2015-2026 (validação)
4. **Estudo multifatorial 15-20 anos** — FRED, CFTC, NASA POWER, USDA, IBKR
5. **Estudo Livermore premium selling commodities** — timing/duração/risco, GF case
6. **Checklists canônicos por commodity** — pesos % por regime de mercado

---

## 4. Biblioteca de livros recomendada (~70-80 obras)

**Filosofia:** Chicago, pragmático, capitalista. Realismo sobre utopia. Empírico sobre teorético.

### Bonds & Treasuries
- Sidney Homer & Richard Sylla — *A History of Interest Rates* (4th ed.) — bíblia
- Frank Fabozzi — *The Handbook of Fixed Income Securities* (10th ed.)
- Marcia Stigum — *Stigum's Money Market* (4th ed.)
- Bruce Tuckman & Angel Serrat — *Fixed Income Securities* (3rd ed.)
- Milton Friedman & Anna Schwartz — *A Monetary History of the United States, 1867-1960*
- Friedrich Hayek — *The Constitution of Liberty*
- Murray Rothbard — *America''s Great Depression*
- Allan Meltzer — *A History of the Federal Reserve* (2 vols)
- Anthony Crescenzi — *The Strategic Bond Investor* (3rd ed.)
- Howard Marks — *The Most Important Thing* + *Mastering the Market Cycle*

### História Econômica & Ciclos
- Joseph Schumpeter — *Business Cycles* (1939)
- Ludwig von Mises — *Human Action*
- Friedrich Hayek — *Prices and Production*
- Milton Friedman — *Capitalism and Freedom* + *Free to Choose*
- Thomas Sowell — *Basic Economics* + *Economic Facts and Fallacies*
- Carmen Reinhart & Kenneth Rogoff — *This Time Is Different*
- Hyman Minsky — *Stabilizing an Unstable Economy*
- Charles Kindleberger — *Manias, Panics, and Crashes*
- Ray Dalio — *Principles for Dealing with the Changing World Order* (com filtro)
- Niall Ferguson — *The Ascent of Money* + *The Cash Nexus*
- Charles Goodhart & Manoj Pradhan — *The Great Demographic Reversal*
- Peter Turchin — *Secular Cycles*

### Premium Selling / Options (Livermore aplicado)
- Edwin Lefèvre — *Reminiscences of a Stock Operator*
- Jesse Livermore — *How to Trade in Stocks*
- Nicolas Darvas — *How I Made $2,000,000 in the Stock Market*
- Stan Weinstein — *Secrets for Profiting in Bull and Bear Markets*
- Sheldon Natenberg — *Option Volatility and Pricing* — bíblia
- Euan Sinclair — *Volatility Trading* (2nd ed.)
- Lawrence McMillan — *Options as a Strategic Investment* (5th ed.)
- Tony Saliba — *The Options Workbook*
- Don Fishback — *Odds: The Key to 90% Winners*
- James Cordier — *The Complete Guide to Option Selling* (estudar erros do blowup também)

### Geopolítica & Macro Realista
- Henry Kissinger — *World Order* + *Diplomacy*
- John Mearsheimer — *The Tragedy of Great Power Politics*
- George Friedman — *The Next 100 Years* + *The Storm Before the Calm*
- Edward Luttwak — *The Rise of China vs. the Logic of Strategy*
- Robert Kaplan — *The Revenge of Geography*
- Daniel Yergin — *The Prize* + *The Quest* + *The New Map*
- Javier Blas & Jack Farchy — *The World for Sale*
- Vaclav Smil — *How the World Really Works* + *Energy and Civilization*
- Peter Zeihan — *The End of the World Is Just the Beginning* (com filtro)

### Comportamental sem moralismo
- Daniel Kahneman — *Thinking, Fast and Slow*
- Richard Thaler — *Misbehaving*
- Michael Mauboussin — *More Than You Know* + *The Success Equation*
- Robert Cialdini — *Influence*
- Charles MacKay — *Extraordinary Popular Delusions and the Madness of Crowds*

### Análise Técnica Institucional
- Charles Kirkpatrick & Julie Dahlquist — *Technical Analysis* (CMT textbook)
- John Murphy — *Technical Analysis of the Financial Markets*
- Robert Edwards & John Magee — *Technical Analysis of Stock Trends* (10th ed.)
- Thomas Bulkowski — *Encyclopedia of Chart Patterns* (3rd ed.)
- Martin Pring — *Technical Analysis Explained*

### Análise Fundamentalista (commodities)
- Helyette Geman — *Commodities and Commodity Derivatives*
- Daniel Sumner et al. — *Agricultural Policy in the United States*
- John Baffes (World Bank) — papers supercycles
- Robert Pindyck — papers commodity pricing
- USDA WASDE methodology docs

### COT & Posicionamento
- Larry Williams — *Trade Stocks and Commodities with the Insiders*
- Stephen Briese — *The Commitments of Traders Bible*
- Floyd Upperman — *Commitments of Traders*
- CFTC technical docs

### Risk Management / Position Sizing
- Ralph Vince — *The Mathematics of Money Management*
- Edward Thorp — *A Man for All Markets* + Kelly Criterion papers
- Nassim Taleb — *Dynamic Hedging* + *Antifragile*
- Aaron Brown — *Red-Blooded Risk*

### Macro Outsider (DXY/EM/flows)
- George Soros — *The Alchemy of Finance*
- Stanley Druckenmiller — entrevistas + papers
- Felix Zulauf — *The Zulauf Letter* archives
- Russell Napier — *Anatomy of the Bear* + *The Solid Ground*
- Marc Faber — *Tomorrow''s Gold* (com filtro)

### Brasil & EM (pragmático)
- Carmen Reinhart — crises EM
- Eliana Cardoso — *A Brief History of Brazilian Economy*
- Affonso Celso Pastore — papers Brasil macro
- Carlos Langoni — historiografia BCB

### Total estimado
~70-80 livros principais + papers + KBs próprias
Custo aquisição: $2k-4k (Kindle/PDF)
Custo indexação one-time: $200-500
Custo queries: fração de centavo cada

---

## 5. Dinâmica de uso (Cenário Híbrido)

### Distribuição de uso

| Função | Onde |
|---|---|
| Análise rápida diária | Dashboard (botão "Analisar X") |
| Relatório Final estruturado | Dashboard (botão "Gerar Council") |
| Conversação com Council pós-relatório | Dashboard (chat embutido) ou Claude.ai |
| Discussão estratégica/arquitetura | Claude.ai chat |
| Code/manutenção | Claude Code (terminal) |
| Trade Journal entries | Dashboard (botão "Registrar Trade") |
| Coleta dados | Pipeline VPS automático |

### Distribuição percentual estimada
- 90% — Dashboard (operação diária)
- 10% — Claude.ai chat (estratégia, arquitetura, brainstorm)

### Limites técnicos reais

**Rate limits:**
- Anthropic Claude: 50 RPM, 40k tokens/min — Council completo OK
- Grok: ~100/dia premium — limitante mas suficiente pra 5 análises/dia
- Perplexity: 60 RPM — OK
- Gemini: 60 RPM — OK

**Custo por execução:**
- Council Final completo: $1-5 (25-40 calls API + 50k-150k tokens)
- Trade Journal entry: $0.10
- Conversa pós-relatório: $0.50-2
- Tese Macro semanal: $3-8

**Custo mensal estimado:**
- 5 análises Council/dia x 22 dias = 110 x $3 = ~$330
- Assinaturas (Grok+Perplexity+Gemini): $70
- Infraestrutura adicional: $0-50
- **Total: $400-500/mês operacional** (Felipe confirmou orçamento aberto)

**Latência:**
- Council Final: 2-4 min end-to-end
- Chat individual: 5-30s
- Trade Journal: instantâneo

---

## 6. Schedule de implementação

### Pré-viagem (até 18 de maio de 2026)

**Tier 1 — Substância analítica:**
- [ ] P0.6 — Bug Unicode escapes UI (30-60 min)
- [ ] Skill método Felipe formalizada (commodity-research-felipe-method.md)
- [ ] Trade Journal estruturado (template + primeira entry hoje: rolagem ZL)
- [ ] Super-páginas por commodity (1 página por commodity, layout intuitivo)
- [ ] Tese Macro mínima viável (sistema simples: tese semanal + checagem diária)
- [ ] P0.18 — Refresh button funcional dashboard

**Não entra pré-viagem:**
- Alertas Telegram/mobile (TradingView cobre)
- Auto-recovery cron VPS (sistema atual já robusto pós P0.11/P0.23/P0.25)
- Mobile-first review separado (TradingView resolve)

### Durante viagem (1 jun — 31 jul, 8 semanas)

- Trade Journal capturando aprendizado real (8 semanas de operação nômade)
- Adquirir livros conforme interesse (Kindle/PDF)
- Mente trabalhando na arquitetura (sem pressão de implementação)

### Pós-viagem (setembro+)

**Mês 1 (setembro):**
- Construção dashboard com Council UI
- Backend orchestrator (chamadas APIs paralelas)
- Indexação dos primeiros 20-30 livros core
- Estudo Livermore premium selling (GF case)

**Mês 2 (outubro):**
- Sobol/Morris agent implementação
- Backtesting Opportunity Ranker walk-forward
- Bonds Agent + RAG bonds books
- História Agent + RAG ciclos books

**Mês 3 (novembro):**
- Estudo multifatorial 15-20 anos
- 12 especialistas com checklists profundos
- Knowledge bases destiladas (Felipe método, etc.)

**Mês 4 (dezembro):**
- Council Final completo integrado
- Modo conversacional persistente
- Skill Calibration Engine
- Decision Patterns Memory

**Total estimado:** 12-16 semanas (3-4 meses) pós-viagem

---

## 7. Princípios de governança do projeto

1. **Coordenadores: Felipe + Claude** — sem orquestrador externo
2. **Auditabilidade total** — toda decisão rastreável até origem
3. **ZERO MOCK** — apenas dados reais, sempre
4. **Tagueamento de proveniência** — qualidade analítica via transparência
5. **Especialização por agent** — não-exposição total evita contaminação
6. **Trade Journal global** — único namespace acessível a todos
7. **Aprendizado contínuo** — sistema evolui conforme Felipe opera
8. **Custo controlado** — $400-500/mês operacional aceitável
9. **Latência aceitável** — 2-4 min para Council profundo (não HFT)
10. **Anti-utopia** — escola Chicago, realismo, pragmatismo, capitalismo

---

## 8. Histórico desta conversa

**Sessão consolidação visão (30/abr/2026):**

- Discussão estendida sobre arquitetura Multi-AI
- 4 distorções identificadas e corrigidas iterativamente:
  1. Papel do Gemini (Devil''s Advocate confirmado)
  2. Bonds + História mantidos como agents (não eliminados)
  3. "Administração" = referência a Perplexity Computer ($200/mês), descartada — Felipe + Claude coordenam
  4. Macro vs Micro alinhada (Claude integra; Sobol/Morris adicionado)

- 3 questões fundamentais respondidas:
  1. Frequência Relatório Final: sob demanda
  2. Onde aparece: HTML interativo + PDF sob demanda
  3. Council: 5 advisors padrão

- Camada 4 (Aprendizado & Memória) adicionada — gatilho: trade real Felipe rolagem ZL hoje

- Estudos profundos resgatados de sessões anteriores:
  1. Sobol/Morris Quantitative Sensitivity Agent (memória #17)
  2. Backtesting walk-forward Opportunity Ranker
  3. Estudo multifatorial 15-20 anos
  4. Estudo Livermore aplicado a premium selling
  5. Tese Macro com calibração nortear Micro

- 4 perguntas estruturais finalizadas:
  1. Knowledge bases por agent (acesso filtrado, não global)
  2. Sobol/Morris frequência: background diário
  3. Tese Macro frequência: semanal calibrada diária
  4. Schedule: Opção B (arquitetura completa pós-viagem)

- Filosofia analítica explicitada: Chicago boy, pragmático, capitalista, anti-utopia
- Uso 100% pessoal/privado confirmado
- Lista de ~70-80 livros curada e aprovada

---

**Fim do documento.**
**Versionado:** este arquivo será commitado em git como referência permanente.
**Localização:** docs/architecture/AGRIMACRO_VISION_v2.md
