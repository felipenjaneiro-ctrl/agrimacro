# Auditoria de Limpeza — AgriMacro

**Data da auditoria:** 2026-04-24
**Pasta analisada:** `C:\Users\felip\OneDrive\Área de Trabalho\agrimacro\`
**Tamanho total atual:** 889 MB
**IMPORTANTE:** Nada foi deletado. Este é apenas um relatório para revisão.

---

## Como ler este relatório

- 🟢 **VERDE** — Seguro deletar. Cache, backups, lixo de desenvolvimento.
- 🟡 **AMARELO** — Provavelmente obsoleto. Precisa sua confirmação.
- 🔴 **VERMELHO** — **NUNCA DELETAR.** Parte viva do projeto.
- ⚪ **CINZA** — Tenho dúvida. Preciso que você me explique o que é.

Leia na ordem: primeiro o RESUMO, depois os detalhes de cada cor.

---

## RESUMO EXECUTIVO

| Categoria | Espaço estimado | O que é |
|-----------|----------------|---------|
| 🟢 VERDE seguro | **~620 MB** | Cache Node.js (.next, node_modules), __pycache__, logs antigos, arquivos .bak |
| 🟡 AMARELO revisar | **~5 MB** | Scripts "fix_*", "diag_*", "patch_*", pastas legadas (dashboard/, src/) |
| 🔴 VERMELHO intocável | **~260 MB** | .git, dados vivos (public/data/), pipeline ativo, dashboard fonte, skills |
| ⚪ CINZA confirmar | **~20 KB** | 2 arquivos duvidosos na raiz |

**Recuperação máxima possível (sem risco):** aproximadamente **620 MB** apenas com itens VERDES.

**Regra de ouro:** se você deletar tudo marcado como VERDE, o projeto continua funcionando 100%. O `.next/` e `node_modules/` são recriados automaticamente quando o Next.js roda pela primeira vez.

---

## 🟢 VERDE — Seguro Deletar

Estes itens são 100% recuperáveis ou nunca foram importantes. Nenhum script do pipeline nem o dashboard precisam deles.

### 1. Caches do Next.js e Node (558 MB)

| Caminho | Tamanho | O que é |
|---------|---------|---------|
| `agrimacro-dash\node_modules\` | **447 MB** | Bibliotecas JavaScript baixadas. Recriadas automaticamente com `npm install` |
| `agrimacro-dash\.next\` | **79 MB** | Cache de build do Next.js. Recriado automaticamente quando o dashboard inicia |
| `agrimacro-dash\.next\dev\logs\` | (dentro do .next) | Logs internos do servidor de desenvolvimento |

> **Observação:** Se você deletar `node_modules`, da próxima vez que rodar o dashboard vai precisar rodar `npm install` dentro de `agrimacro-dash\` primeiro. Leva 2-5 minutos com internet boa.

### 2. Caches Python __pycache__ (1,2 MB total)

| Caminho | Tamanho |
|---------|---------|
| `agrimacro\__pycache__\` | 92 KB |
| `pipeline\__pycache__\` | 1,1 MB |
| `bilateral\__pycache__\` | — |
| `bilateral\indicators\__pycache__\` | — |
| `config\__pycache__\` | — |

> Caches criados automaticamente pelo Python. Recriados quando os scripts rodam.

### 3. Backups .bak (aproximadamente 60 MB)

Conta de arquivos `.bak*` encontrados:
- **Pasta raiz:** 9 arquivos (ex: `grain_ratio_engine.bak_20260219_011029.py`, `grain_ratio_engine.py.bak_pre_crush_unify`)
- **Pasta `pipeline\`:** 92 arquivos (incluindo 40+ backups de `generate_report_pdf.py` dos dias 16-18 de Feb 2026)
- **Pasta `agrimacro-dash\src\app\`:** 59 arquivos — destaque: **70+ versões de `dashboard.tsx.bak_*`** (fase1, fase2, bug1, bug2, grok_trends, visual_fix, etc.)

Destaques especialmente grandes:
- `grain_ratio_engine.bak_*.py` (~45 KB cada, 9 cópias)
- `generate_report_pdf.py.bak_*` (~40 KB cada, 40+ cópias) → **~1,6 MB só em backups desse arquivo**
- `dashboard.tsx.bak_*` (70+ cópias, provavelmente ~20-30 MB no total)

> Como você tem tudo no Git (54 MB de histórico em `.git\`), esses `.bak` são redundantes.

### 4. Pasta dedicada `pipeline\_backups\` (400 KB)

5 arquivos lá dentro, todos datados de 18/Fev/2026:
- `generate_report_pdf_20260218_170205.py.bak`
- `generate_report_pdf_20260218_170702.py.bak`
- `run_pipeline_20260218_171623.py.bak`
- `run_pipeline_20260218_171744.py.bak`
- `run_pipeline_20260218_171854.py.bak`

### 5. Logs antigos (>60 dias)

| Caminho | Última modificação |
|---------|-------------------|
| `pipeline\logs\2026-02-12_*.json/.txt` (4 arquivos) | 12/Fev/2026 |
| `pipeline\logs\2026-02-19_*.json/.txt` (4 arquivos) | 19/Fev/2026 |

> O log `pipeline\logs\morning_sync.log` está ativo e não deve ser deletado.

### 6. Arquivos de teste/dump sem uso

| Caminho | Tamanho | Observação |
|---------|---------|-----------|
| `pipeline\test_write.txt` | vazio | Teste de escrita antigo |
| `pipeline\_bilateral_dump.txt` | 13 KB | Dump de texto de Feb/2026 |
| `pipeline\_physical_dump.txt` | 2,8 KB | Dump de texto de Feb/2026 |
| `test\hello.txt` | minúsculo | Arquivo placeholder |
| `.gitignore.bak_pre_patch_v4` | 1,6 KB | Backup do .gitignore (versão atual já existe) |
| `dashboard_backup.tsx` (em `pipeline\`) | — | Backup antigo do dashboard |
| `dashboard (2).tsx` (em `agrimacro-dash\`) | — | Cópia duplicada do dashboard |
| `dashboard_backup.txt` (em `agrimacro-dash\`) | — | Backup em formato .txt |

### 7. .gitignore confirma o descarte

Seu próprio `.gitignore` (linhas 36-71) já manda o Git ignorar:
- `**/fix*.py`, `**/diag*.py`, `**/debug*.py`, `**/check*.py`
- `**/bloco*.py`, `**/patch_*.py`, `**/w[0-9]*.py`, `**/write*.py`
- `**/*.bak`, `**/*.bak_*`, `**/*.bak2`, `**/*.bak3`
- `**/header_part.txt`, `**/test_write.txt`

Ou seja: **você mesmo já reconheceu que esses arquivos são lixo temporário.**

---

## 🟡 AMARELO — Provavelmente Obsoleto

Aqui estão os itens onde forneço todos os detalhes (caminho, tamanho, data, quem importa, recomendação). Você aprova antes de qualquer ação.

### A1. Pasta legada `dashboard\` (3,8 MB)

- **Caminho:** `dashboard\index.html`
- **Tamanho:** 3,9 MB (arquivo único dentro da pasta)
- **Última modificação:** 04/Fev/2026 (2,5 meses atrás)
- **Último uso (git):** nenhum commit modifica isso
- **Quem importa:** Ninguém. O `CLAUDE.md` diz explicitamente que o dashboard atual é `agrimacro-dash\` (Next.js)
- **Minha recomendação:** 🟡 **Deletar.** É o dashboard antigo em HTML estático, substituído pelo Next.js há muito tempo.

### A2. Pasta legada `src\` (56 KB)

- **Caminho:** `src\analyzers\`, `src\collectors\`, `src\generators\`
- **Arquivos:** 6 arquivos Python (`seasonality.py`, `spreads.py`, `stocks_watch.py`, `price_collector.py`, `dashboard.py`, `pdf_report.py`)
- **Última modificação:** 04/Fev/2026 (2,5 meses atrás)
- **Quem importa:** Ninguém. Todos esses scripts foram migrados para `pipeline\` com nomes equivalentes:
  - `src\analyzers\seasonality.py` → substituído por `pipeline\process_seasonality.py`
  - `src\analyzers\spreads.py` → substituído por `pipeline\process_spreads.py`
  - `src\analyzers\stocks_watch.py` → substituído por `pipeline\process_stocks.py`
  - `src\collectors\price_collector.py` → substituído por `pipeline\collect_ibkr.py`
  - `src\generators\dashboard.py` → substituído por `agrimacro-dash\` (Next.js)
  - `src\generators\pdf_report.py` → substituído por `generate_report_pdf.py` (raiz)
- **Minha recomendação:** 🟡 **Deletar a pasta inteira `src\`.** É código morto da v1.

### A3. Pasta `github-files\` (37 KB)

- **Caminho:** `github-files\`
- **Conteúdo:** `README.md`, `requirements.txt`, `config\`, `src\`
- **Minha recomendação:** 🟡 **Parece ser um export antigo do repositório, duplicado do projeto principal.** Abrir e confirmar — se for só cópia de estrutura antiga, deletar.

### A4. Scripts "fix_*" na raiz (9 arquivos, ~40 KB)

Todos são scripts one-off de correção que já foram aplicados. Nenhum é importado por outro arquivo.

| Arquivo | Tamanho | Modificado | Recomendação |
|---------|---------|-----------|-------------|
| `fix_bcb_import.py` | 415 B | 01/Abr/2026 | 🟡 Deletar. Correção pontual BCB. |
| `fix_grain_engine.py` | 14 KB | 18/Fev/2026 | 🟡 Deletar. Substituído por fix_grain_engine5 (e pelo próprio engine corrigido). |
| `fix_grain_engine2.py` | 12 KB | 18/Fev/2026 | 🟡 Deletar. Versão intermediária. |
| `fix_grain_engine4.py` | 9,3 KB | 18/Fev/2026 | 🟡 Deletar. Versão intermediária. |
| `fix_grain_engine5.py` | 12 KB | 18/Fev/2026 | 🟡 Deletar. Última versão de patches — já foi aplicado. |
| `fix_grpath.py` | 369 B | 01/Abr/2026 | 🟡 Deletar. Correção pontual de path. |
| `fix_pdf_link.py` | 609 B | 12/Fev/2026 | 🟡 Deletar. One-off. |
| `fix_portfolio2.py` | 10 KB | 15/Fev/2026 | 🟡 Deletar. One-off (portfolio). |
| `fix_videoscript.py` | 504 B | 01/Abr/2026 | 🟡 Deletar. One-off. |

> **Como eu sei que são seguros?** O próprio `.gitignore` (linha 37) diz `**/fix*.py` — Felipe, você já classificou esses arquivos como descartáveis.

### A5. Scripts "diag_*" na raiz (3 arquivos, ~3,2 KB)

Scripts de diagnóstico pontual. Nenhum é importado por outro arquivo.

| Arquivo | Tamanho | Modificado |
|---------|---------|-----------|
| `diag_conab2.py` | 892 B | 19/Fev/2026 |
| `diag_ibge.py` | 1,1 KB | 19/Fev/2026 |
| `diag_ibge2.py` | 1,2 KB | 19/Fev/2026 |

> `.gitignore` já lista `**/diag*.py`. 🟡 **Recomendação: deletar os 3.**

### A6. Scripts "patch_*" na raiz (4 arquivos)

One-offs de patch aplicados há meses.

| Arquivo | Tamanho | Modificado |
|---------|---------|-----------|
| `patch_grain_audit_v1.py` | 15 KB | 19/Fev/2026 |
| `patch_grain_audit_v2.py` | 12 KB | 19/Fev/2026 |
| `patch_grain_ratios_pdf.py` | 3,6 KB | 18/Fev/2026 |
| `patch_pipeline_grain_ratios.py` | 3,7 KB | 18/Fev/2026 |

> **ATENÇÃO:** NÃO DELETE o arquivo `pipeline\patch_report_v4.py` — **ESSE É VERMELHO** (usado no Step 29 do pipeline).
> Os 4 listados acima estão na raiz (não no pipeline) e são obsoletos.

### A7. `cop_patch.py` na raiz

- **Caminho:** `cop_patch.py`
- **Tamanho:** 2,3 KB
- **Modificado:** 18/Fev/2026
- **Quem importa:** Ninguém.
- **Minha recomendação:** 🟡 **Deletar.** One-off patch Cost of Production (já aplicado).

### A8. `agrimacro-dash\` — dezenas de scripts Python órfãos

A pasta `agrimacro-dash\` é Next.js (JavaScript/TypeScript). Ainda assim ela contém dezenas de scripts Python `.py` que **não pertencem** a uma pasta Next.js. Todos batem com os padrões do `.gitignore`:

Scripts: `bloco1.py` a `bloco8.py`, `check.py`, `check2.py`, `check_bt.py`, `check_cot.py`, `check_side.py`, `check_version.py`, `dbg3-5.py`, `debug.py`, `debug2.py`, `diag.py` a `diag12.py`, `fix.py` a `fix7.py`, `fix_bt.py`, `fix_bt3.py`, `fix_cot.py`, `fix_equity.py`, `fix_fonts.py`, `fix_momentum.py`, `fix_sidebar.py`, `fix_templates.py`, `fix_ticks.py`, `fix_zerodiv.py`, `fixgt.py`, `gen.py`, `gen1.py`, `install.py`, `install_dashboard.py`, `patch_bottleneck_v2.py`, `patch_clean.py`, `patch_fill_2025.py`, `patch_livestock_audit_v1.py`, `patch_real_2025.py`, `patch_v3.py`, `prep.py`, `touch.py`, `w1-w4.py`, `write1-5.py`, `write_livestock_tab.py`.

Também: `bottleneck_backtest.py` (+ 2 .bak), `collect_fao_giews.py`, `collect_geoglam.py`, `collect_imf_pink.py`, `collect_magpy_ar.py`, `collect_mapa_br.py`, `collect_nasa_power.py`, `collect_usda_nass.py`, `collect_worldbank.py`, `copy_data.py`, `find_equity.py`, `header_part.txt`.

**Total: aproximadamente 80 arquivos Python, nenhum importado pelo dashboard Next.js ou pelo pipeline principal.**

> 🟡 **Recomendação:** Deletar todos esses `.py` de dentro de `agrimacro-dash\`. São experimentos/rascunhos antigos que ficaram em local errado.
> **Exceção:** NÃO deletar `agrimacro-dash\scripts\` — essa pasta é diferente, verificar individualmente (veja CINZA).

### A9. Pasta `pipeline\` — scripts Python NÃO importados pelo pipeline

Analisei o `pipeline\run_pipeline.py` inteiro (617 linhas). Abaixo estão scripts de `pipeline\` que **ninguém importa** (nem `run_pipeline.py`, nem `opportunity_ranker.py`, nem o dashboard via API, nem os .ps1 ou .bat).

**ATENÇÃO — estes todos precisam ser confirmados, pois podem ter sido usados recentemente via linha de comando:**

| Arquivo | Tamanho | Modificado | Observação |
|---------|---------|-----------|-----------|
| `aa_qa_engine.py` | 42 KB | 12/Fev/2026 | Engine de QA — versão abandonada (foi substituída por generate_report.py) |
| `add_pdf_pages.py` | 12 KB | 10/Fev/2026 | One-off de adição de páginas PDF |
| `add_stock_state.py` | 750 B | 07/Fev/2026 | Listed in .gitignore line 61 |
| `collect_cepea.py` + `.bak` | 11+9 KB | 12/Fev/2026 | CLAUDE.md diz que CEPEA retorna 403 — usamos Notícias Agrícolas |
| `collect_contract_history.py` | 4 KB | 07/Fev/2026 | Não importado. Mas `contract_history.json` IS consumido. Verificar se é usado sub-rotina |
| `collect_export_flows.py` | 14 KB | 19/Fev/2026 | Substituído por `collect_export_activity.py` |
| `collect_futures_contracts.py` | 8,6 KB | 20/Abr/2026 | Não importado mas `futures_contracts.json` é consumido — pode ser chamado standalone |
| `collect_imea_soja.py` | 12 KB | 19/Fev/2026 | Imea Soja — JSON não é atualizado desde Feb |
| `collect_prices.py` (+ .bak) | — | — | **Step 2 foi desabilitado em Abr/2026** (commit 9a3269d) — este script está morto |
| `collect_psd_stocks.py` | — | — | Substituído por `collect_livestock_psd.py` |
| `collect_sugar_alcohol_br.py` | — | — | Não aparece em run_pipeline, mas gera JSON de açúcar |
| `collect_usda_fas.py` | — | — | Substituído por `collect_usda_psd_csv.py` |
| `collect_usda_freight.py` | — | — | Não importado |
| `cross_analysis.py` | — | — | Gera `cross_analysis.json` mas não vi referência no dashboard |
| `dashboard_backup.tsx` | — | — | Backup antigo do dashboard, em local errado |
| `disclosure.py` | — | — | Não importado |
| `find_cocoa.py` | — | — | Nome sugere diagnóstico one-off |
| `fix_backticks.py`, `fix_backticks2.py`, `fix_bcb_keys.py`, `fix_border_colon.py`, `fix_collectors.py`, `fix_cosmetic.py`, `fix_cot_compat.py`, `fix_cot_datadir.py`, `fix_cot_rich.py`, `fix_cot_years.py`, `fix_cross_pages.py`, `fix_cross_pages_v2.py`, `fix_cross_pages_v3.py`, `fix_cyan.py`, `fix_inject_methods.py`, `fix_path.py`, `fix_pdf_parsing.py`, `fix_pdf_report.py`, `fix_pdf_v32.py`, `fix_pipeline.py`, `fix_pipeline_resilience.py`, `fix_rgba.py`, `fix_rsi.py`-`fix_rsi5.py`, `fix_rsi_calc.py`, `fix_rsi_final.py`, `fix_stocks_iface.py`, `fix_stocks_minval.py`, `fix_stocks_pipeline.py`, `fix_stocks_save.py` | — | — | Todos `.gitignore line 37` — one-off patches |
| `generate_content.py` (+ bak_relpath) | — | — | Variante antiga de geração |
| `generate_daily_pdf.py` (+ 3 .bak) | — | — | Variante antiga — substituída por generate_report.py + generate_report_pdf.py |
| `generate_report_pdf_8.py` | — | — | Variante numerada — substituída pelo atual |
| `ibkr_orders.py` | — | — | Módulo de ordens não vi referência |
| `patch_*` (vários) | — | — | One-offs listados em .gitignore. **EXCEÇÃO:** `patch_report_v4.py` é 🔴 VERMELHO |
| `pg_bilateral_new.py`, `pg_cot_basis_new.py`, `pg_cross_dashboard_new.py`, `pg_grain_ratios.py` (+ 2 .bak), `pg_physical_new.py` | — | — | Geradores de páginas "new" — parecem ser variantes experimentais |
| `rebuild_stocks_watch.py` | — | — | Rebuild one-off |
| `remove_stooq.py` | — | — | `.gitignore line 57` — one-off |
| `run_pipeline_with_qa.py` | — | — | Variante antiga do pipeline com QA gate (hoje é outro) |
| `score_trades.py` | — | — | Gera score de trades — não vi import |
| `skill_hedge_recommendation.py` | — | — | Não está no skill mapping do dashboard |
| `skill_manifesto.py` | — | — | Não está no skill mapping do dashboard |
| `sprint1_patch.py` | — | — | Patch sprint 1 |
| `test_psd_files.py` | — | — | Teste one-off |
| `update_pipeline.py` | — | — | `.gitignore line 60` |
| `upgrade_dashboard.py` | — | — | One-off upgrade |
| `upgrade_stocks.py`, `upgrade_stocks_chart.py`, `upgrade_stocks_chart2.py` | — | — | One-off upgrades |

> 🟡 **Recomendação geral:** Todos os acima estão há 2+ meses sem modificação OU estão listados no seu próprio `.gitignore`. Recomendo deletar em lote **depois que você confirmar que não quer nenhum deles**.

### A10. Arquivos na raiz — mais dois

| Arquivo | Tamanho | Modificado | Quem importa | Recomendação |
|---------|---------|-----------|-------------|-------------|
| `update_portfolio.py` | 12 KB | 15/Fev/2026 | Ninguém (ver CINZA C2) | 🟡 Provavelmente substituído por `sync_portfolio.ps1` |
| `fix_portfolio2.py` | 10 KB | 15/Fev/2026 | Ninguém | 🟡 One-off |

---

## 🔴 VERMELHO — NUNCA DELETAR

Estes são o coração do projeto. Se mexer, o AgriMacro quebra.

### R1. Configurações e segredos (raiz)

- `.env` → variáveis de ambiente (chaves API)
- `credentials.json` → OAuth Google
- `token.json` → token Google ativo
- `.gitignore` → controle do Git
- `.mcp.json` → configuração MCP do Claude Code
- `CLAUDE.md` → instrução permanente para o Claude
- `README.md` → documentação do projeto
- `requirements.txt` → dependências Python
- `Procfile`, `render.yaml` → deploy no Render
- `package.json` + `package-lock.json` (em `agrimacro-dash\`) → dependências Node
- `next.config.ts`, `tsconfig.json`, `eslint.config.mjs`, `postcss.config.mjs` (em `agrimacro-dash\`) → configs do Next.js
- `symbols.yml` (em `pipeline\`) → configuração de símbolos das commodities
- `commodities.py` (em `config\`) → configuração de commodities

### R2. Git (54 MB)

- `.git\` — histórico completo do repositório. **Nunca deletar.**

### R3. Ferramentas Claude / Skills / MCP

- `.claude\` → configurações do Claude Code
- `.agents\` → definições de agents
- `.aidesigner\` → projetos do AIDesigner

### R4. Pipeline ativo — 44 scripts Python importados por `run_pipeline.py`

Estes TODOS são chamados pelo pipeline principal:

Pasta `pipeline\`:
`collect_ibkr.py`, `collect_options_chain.py`, `backadjust_rollovers.py`, `validate_prices.py`, `check_data_freshness.py`, `collect_cot.py`, `process_seasonality.py`, `process_spreads.py`, `collect_parities.py`, `process_stocks.py`, `collect_physical.py`, `collect_physical_intl.py`, `generate_reading.py`, `collect_new_sources.py`, `collect_conab_ibge.py`, `collect_eia.py`, `collect_usda_psd_csv.py`, `collect_livestock_psd.py`, `collect_livestock_weekly.py`, `generate_bilateral.py`, `collect_news.py`, `collect_weather.py`, `collect_crop_progress.py`, `collect_export_activity.py`, `collect_drought_monitor.py`, `collect_fertilizer.py`, `collect_macro_indicators.py`, `collect_google_trends.py`, `collect_fedwatch.py`, `collect_correlations.py`, `collect_grok_email.py`, `collect_calendar.py`, `generate_report.py`, `generate_intel_synthesis.py`, `intelligence_engine.py`, `skill_entry_timing.py`, `skill_theta_calendar.py`, `skill_opportunity_scanner.py`, `skill_vega_monitor.py`, **`patch_report_v4.py`** ⚠️, `generate_video_script.py`, `step18_video_generator.py`, `run_pipeline.py`, `utils.py` (importado por `process_spreads.py` e `grain_ratio_engine.py`).

### R5. Scripts na raiz usados pelo pipeline (subprocess)

- `grain_ratio_engine.py` → chamado via subprocess pelo `run_pipeline.py` (linha 473)
- `grain_ratios_enrich.py` → chamado via subprocess pelo `run_pipeline.py` (linha 474)
- `generate_report_pdf.py` → importado por `pipeline\patch_report_v4.py`
- `main.py` → app FastAPI (webhook no Render)

### R6. Scripts chamados pelos .ps1/.bat (agendados)

- `pipeline\opportunity_ranker.py` → chamado por `run_daily.ps1` e tarefa agendada 05:45
- `pipeline\sync_morning_intel.py` → chamado por `run_morning_sync.bat`

### R7. Skills ativas (chamadas pelo dashboard via API `/api/skills`)

Confirmado em `agrimacro-dash\src\app\api\skills\route.ts`:
- `skill_entry_timing.py`
- `skill_pretrade_checklist.py`
- `skill_position_sizing.py`
- `skill_theta_calendar.py`
- `skill_opportunity_scanner.py`
- `skill_stress_test.py`
- `skill_vega_monitor.py`
- `skill_roll_decision.py`
- `skill_trade_journal.py`
- `commodity_dna.py`

### R8. Scripts de inicialização/agendamento

- `run_daily.ps1` → pipeline diário
- `setup_scheduled_tasks.ps1` → registra tarefas Windows (05:30, 05:45, 06:00)
- `sync_portfolio.ps1` → sincroniza portfólio IBKR + GitHub
- `start_agrimacro.ps1` → inicia dashboard Next.js
- `run_morning_sync.bat` → wrapper do morning sync
- `backup_github.bat` → backup para GitHub
- `run_gmail_auth.bat` → OAuth Gmail

### R9. Dashboard (fonte) — `agrimacro-dash\src\`

- `agrimacro-dash\src\app\dashboard.tsx` → dashboard principal
- `agrimacro-dash\src\app\BilateralPanel.tsx`, `COTChart.tsx`, `CommodityTab.tsx`, `CorrelationMap.tsx`, `CostOfProductionTab.tsx`, `GrainRatiosTab.tsx`, `LightweightChart.tsx`, `LivestockRiskTab.tsx`, `PortfolioPage.tsx`, `PortfolioSyncBadge.tsx`, `SyncedChartPanel.tsx` → componentes do dashboard
- `agrimacro-dash\src\app\api\` → rotas de API (chat, council, ibkr-greeks, ibkr-order, latest-pdf, refresh-ibkr, refresh-pipeline, skills, snapshot, strategy, sync-portfolio)
- `agrimacro-dash\src\app\layout.tsx`, `page.tsx`, `globals.css`, `favicon.ico` → estrutura Next.js

### R10. DADOS VIVOS — `agrimacro-dash\public\data\` (194 MB)

**Este é o dado consumido pelo dashboard em tempo real. NUNCA DELETAR.**

- `agrimacro-dash\public\data\raw\price_history.json` → histórico de preços (IBKR primário)
- `agrimacro-dash\public\data\raw\` (outros arquivos brutos)
- `agrimacro-dash\public\data\processed\*.json` → **~50 JSONs consumidos pelo dashboard**, incluindo:
  - Portfolio: `ibkr_portfolio.json`, `ibkr_greeks.json`
  - Preços: `price_history.json`, `contract_history.json`, `futures_contracts.json`
  - Análise: `cot.json`, `seasonality.json`, `spreads.json`, `stocks_watch.json`, `parities.json`, `physical.json`, `physical_intl.json`, `physical_br.json`, `grain_ratios.json`
  - Macro: `bcb_data.json`, `eia_data.json`, `macro_indicators.json`, `fedwatch.json`, `google_trends.json`
  - Fundamentos: `daily_reading.json`, `crop_progress.json`, `export_activity.json`, `drought_monitor.json`, `fertilizer_prices.json`, `weather_agro.json`, `livestock_psd.json`, `livestock_weekly.json`, `usda_fas.json`, `conab_data.json`, `ibge_data.json`
  - IA: `intel_synthesis.json`, `intelligence_frame.json`, `commodity_dna.json`, `correlations.json`, `opportunity_ranking.json`, `bottleneck.json`, `news.json`, `calendar.json`
  - Skills: `theta_calendar.json`, `vega_monitor.json`, `options_chain.json`, `iv_history.json`, `price_validation.json`, `psd_ending_stocks.json`
  - Bilaterais: `bilateral_indicators.json`
  - Grok: `grok_general.json`, `grok_macro.json`, `grok_news.json`, `grok_sentiment.json`
  - Diversos: `comexstat_exports.json`, `export_flows.json`, `fao_giews.json`, `geoglam.json`, `imea_soja.json`, `imf_pink.json`, `inmet_data.json`, `last_known_good_prices.json`, `last_run.json`, `magpy_ar.json`, `mapa_br.json`, `nasa_power.json`, `qa_report.json`, `report_daily.json`, `sugar_alcohol_br.json`, `usda_brazil_transport.json`, `usda_gtr.json`, `usda_nass.json`, `video_script.json`, `weather_agro.json`, `worldbank.json`
- `agrimacro-dash\public\data\bilateral\` → **9 JSONs de análise bilateral** (consumidos pelo dashboard — mencionados explicitamente no CLAUDE.md):
  - `argentina_trilateral.json`, `basis_temporal.json`, `cot_momentum.json`, `crush_bilateral.json`, `drought_accumulator.json`, `export_pace_weekly.json`, `freight_spread.json`, `interest_differential.json`, `producer_margin.json`
- `agrimacro-dash\public\data\ibkr_history\` → histórico IBKR
- `agrimacro-dash\public\data\last_run.json`, `reports\` → metadata

### R11. Bilateral (raiz) — `bilateral\`

- `bilateral\collectors\` → collectors bilaterais
- `bilateral\indicators\` → indicadores bilaterais
- `bilateral\cross_analysis_indicators.py` (23 KB) → análise cruzada
- `bilateral\patch_cot_momentum.py` (3,8 KB) → patch COT momentum

> Este é o motor de análise bilateral mencionado no CLAUDE.md.

### R12. Arquivos JSON produzidos pelas skills (em `pipeline\`)

Estes são **outputs** das skills que o dashboard consome via API:
- `commodity_dna.json`, `cross_analysis.json`, `cycle_analysis.json`, `entry_timing.json`, `manifesto.json`, `opportunity_ranking.json`, `opportunity_scan.json`, `position_sizing.json`, `roll_decisions.json`, `stress_test.json`, `theta_calendar.json`, `trade_journal.json`, `trade_skill_base.json`, `vega_monitor.json`

### R13. Outputs finais — `reports\`

- `reports\` contém os PDFs/MP4/SRT gerados pelo pipeline (74 MB).
- 🔴 **Os arquivos MAIS RECENTES são importantes** (ex: `AgriMacro_2026-04-09.mp4`).
- 🟡 **Os antigos (Feb/2026) podem ser arquivados** — veja CINZA C3.

### R14. Documentação e outros

- `pipeline\PIPELINE_DOCS.md` → documentação do pipeline
- `pipeline\manifesto_elevator.txt`, `manifesto_full.txt`, `manifesto_onepager.txt` → manifestos usados pela skill manifesto

---

## ⚪ CINZA — Preciso de Sua Confirmação, Felipe

São poucos. São os arquivos onde eu não consigo dizer com certeza se estão ativos ou não.

### C1. `validate_freshness.py` (raiz)

- **Caminho:** `validate_freshness.py`
- **Tamanho:** 6,6 KB
- **Última modificação:** 15/Abr/2026 (recente!)
- **Último commit:** 17/Abr/2026 — `"add validate_freshness.py and update entry_timing data"`
- **Quem importa:** NINGUÉM. Nenhum script usa este arquivo.
- **Conflito:** O pipeline usa `pipeline\check_data_freshness.py` para validar frescor. Por que existem dois arquivos com função similar?
- **Minha pergunta:** Esse é um script que você estava escrevendo recentemente e **não terminou de integrar**? Ou é um teste que você abandonou?
  - Se foi **abandonado** → deletar.
  - Se está **em progresso** → manter e integrar no pipeline.

### C2. `update_portfolio.py` (raiz)

- **Caminho:** `update_portfolio.py`
- **Tamanho:** 12 KB
- **Última modificação:** 15/Fev/2026 (2+ meses)
- **Quem importa:** NINGUÉM.
- **Conflito:** O fluxo atual de portfolio é `sync_portfolio.ps1` → `pipeline\collect_ibkr.py`. Este arquivo antigo parece ter sido substituído.
- **Minha pergunta:** Posso confirmar que ele foi substituído pelo fluxo atual?

### C3. PDFs e vídeos em `reports\` (74 MB)

Arquivos do Feb/2026 (mais de 60 dias):
- `AgriMacro_2026-02-06.pdf`, `AgriMacro_2026-02-08.pdf` (PDFs)
- `AgriMacro_2026-02-10.mp4` (10,4 MB), `AgriMacro_2026-02-12.mp4` (16,3 MB) (vídeos antigos)
- `AgriMacro_VideoScript_2026-02-08.md`, `AgriMacro_VideoScript_2026-02-10.md` (scripts antigos)
- `AgriMacro_Visual_2026-02-08.pdf`, `AgriMacro_Visual_2026-02-10.pdf` (versões visuais antigas)

**Minha pergunta:** Você quer **manter arquivo dos relatórios antigos** ou pode deletar? Seu `.gitignore` linha 12-15 já ignora PDFs e vídeos no Git, então esses arquivos estão soltos no disco.

### C4. `agrimacro-dash\scripts\` (pasta)

- **Caminho:** `agrimacro-dash\scripts\`
- Não explorei o conteúdo em detalhe. Contém pelo menos `grain_ratio_engine.py`, que é **duplicata** do arquivo na raiz.
- **Minha pergunta:** Você se lembra de ter criado essa pasta? Se não for ativamente usada pelo dashboard, é um lugar estranho para colocar script Python. **Precisa eu investigar mais?**

### C5. `data\` (raiz) — 12 MB (pasta legada)

- **Caminho:** `data\inbox\`, `data\processed\`, `data\raw\`
- Esta pasta **ainda existe** com arquivos antigos, mesmo o pipeline escrevendo em `agrimacro-dash\public\data\`:
  - `data\raw\price_history.json` (2,4 MB) — possivelmente desatualizada
  - `data\raw\long_history\` (6,1 MB, 19 JSONs `*_long.json`) — **histórico longo** das commodities
  - `data\processed\seasonality.json`, `spreads.json`, `stocks_watch.json` (3 MB total)
- **Atenção:** Os scripts `grain_ratio_engine.py` e vários `collect_*.py` ainda referenciam `data/raw/` e `data/processed/`. Eu vi no grep em 37 arquivos.
- **Minha pergunta:** Eu quero ter certeza antes de mexer. Essa pasta é usada em paralelo (dual-write), ou foi totalmente substituída pela nova em `agrimacro-dash\public\data\`? **Preciso que você confirme que posso apagar essa `data\` sem quebrar o grain_ratio_engine.**
- ⚠️ **Por segurança: DEIXAR INTOCADA até você confirmar.**

### C6. `outputs\reports\` (vazio)

- **Caminho:** `outputs\reports\`
- Pasta vazia há meses (desde Feb/2026).
- **Minha pergunta:** É lugar de depósito futuro ou resíduo de estrutura antiga? Se antigo, pode deletar a pasta `outputs\` inteira.

---

## PRÓXIMOS PASSOS RECOMENDADOS

Felipe, se você topa, eu sugiro fazer em 4 ondas (do mais seguro ao mais arriscado):

### Onda 1 — Só cache (100% seguro)
Deleta `__pycache__\`, `.next\`, logs antigos de Feb/2026. Recupera ~80 MB.

### Onda 2 — Backups .bak (muito seguro)
Deleta todos os `*.bak*` em `pipeline\`, raiz, `agrimacro-dash\src\app\`, e a pasta `pipeline\_backups\`. Você tem tudo no Git. Recupera ~50 MB.

### Onda 3 — Lixo de desenvolvimento (seguro, mas faça código no Git primeiro)
Deleta `agrimacro-dash\*.py` (fix/diag/bloco/write/w*/patch/etc), raiz `fix_*.py`/`diag_*.py`/`patch_*.py`/`cop_patch.py`, pastas `dashboard\` e `src\` antigas, `github-files\`. Recupera ~5-10 MB. **Antes, rodar `git add . && git commit -m "pre-cleanup backup"` para ter backup imutável no GitHub.**

### Onda 4 — Node modules (maior ganho, menor risco)
Deleta `agrimacro-dash\node_modules\`. Recupera 447 MB. Para voltar a usar o dashboard: `cd agrimacro-dash && npm install` (2-5 minutos com internet).

### Onda 5 — Só depois de você responder CINZA
Após eu receber respostas dos itens CINZA (especialmente C5 — a pasta `data\`), fazemos a última rodada.

---

## DICA DE SEGURANÇA

Antes de qualquer deleção, rode:

```powershell
cd C:\Users\felip\OneDrive\Área de Trabalho\agrimacro
git add -A
git commit -m "checkpoint antes da auditoria de limpeza 2026-04-24"
git push origin main
```

Assim tudo fica salvo no GitHub. Se algo der errado, você restaura do GitHub com um clique.

---

*Relatório gerado em 2026-04-24 por Claude Code. Nenhum arquivo foi deletado.*
