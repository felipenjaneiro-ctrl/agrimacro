# AgriMacro Intelligence — Backlog
**Atualizado:** 2026-04-30
**Refs:** P0.X numbering das sessões 28-29-30 abr

## Status Legenda
- 🔴 Bloqueador (afeta operação diária)
- 🟡 Importante (não bloqueia mas vale resolver)
- 🟢 Cosmético (qualidade de vida)
- 🔵 Estratégico (decisão de produto/arquitetura)
- ⚫ Investigação (precisa coleta de dados antes de decidir)

## Em execução / Recém-fechados
| ID | Status | Descrição | Sessão |
|---|---|---|---|
| P0.11 | ✅ FECHADO | Refactor scp/rsync data/processed/ JSONs | 30/abr |
| P0.13 | ✅ FECHADO | Greeks live IBKR 28/28 reais | 28-29/abr |
| P0.14 | ✅ FECHADO | Greeks agregados em dollar | 28-29/abr |
| P0.23 | ✅ FECHADO | Refactor pipeline/*.json untrack | 30/abr |

## Backlog ativo

### 🔴 Bloqueadores
(nenhum atualmente)

### 🟡 Importante
| ID | Descrição | Esforço |
|---|---|---|
| P0.6 | Bug escapes Unicode UI ("Relações"…) | 30-60min |
| P0.18 | Refresh button funcional no dashboard | 1-2 sessões |
| Pré-viagem | Hardening: auto-recovery, alertas Telegram, monitoring | 1-2 sessões |
| Pré-viagem | Mobile-first review dashboard (iPhone) | 1 sessão |
| Camada 2 base | Skill commodity-research-felipe-method.md | 1-2 sessões |

### 🟢 Cosmético
| ID | Descrição |
|---|---|
| P0.7 | Housekeeping repo (50+ arquivos _quarantine, .md auditoria) |
| P0.9 | Pasta lixo no VPS (untracked OneDrive Windows path) |
| P0.10 | Separador visual watch_if no Soy Crush |
| P0.21 | Diretório "C:\Users\felip\OneDrive/" no VPS |
| ~25 fix_*.py / patch_*.py | Mover pra pipeline/_archive/ |

### 🔵 Estratégico (decisão antes de execução)
| ID | Descrição | Bloqueador |
|---|---|---|
| P0.17 | DTN ProphetX vs IQFeed vs IBKR upgrade | Aguarda 4 meses uso real |
| P0.19 | IB Gateway no VPS vs setup atual | Pré-viagem |
| P0.20 | IBKR member pricing | Ligar memberpricing@interactivebrokers.com |
| Multi-AI | Camada Macro: Bonds + História + Grok + Perplexity + Gemini | Pós-viagem |
| Comercialização | Dashboard público vs pessoal | Quando "muito bom mesmo" |

### ⚫ Investigação
| ID | Descrição |
|---|---|
| P0.22 | council_jobs.json untracked — verificar se gitignore deveria pegar |
| P0.24 | manifesto.json órfão (gerador em _quarantine) |
| HMDS errors | LEZ7/GFK7 sem dados — collect_ibkr.py deveria skip silencioso |
| collect_bcb_ibge.py | Step 9 nunca foi criado, sempre WARN |
| collect_cepea.py | Pendente desde fev 2026 |
| disclaimer PDF capa | Pendente desde fev 2026 |
| IV CL ~88% | Validar se é realista (Hormuz pricing) vs TWS |

## Princípios para gestão deste arquivo
1. Cada item recém-descoberto vai PRA AQUI antes de virar P0.X formal
2. P0.X é numeração estável (uma vez P0.21, sempre P0.21)
3. Quando fechado: move pra "Recém-fechados" com data
4. Reavaliar prioridade no início de cada sessão
5. Se um item não tem ação clara em 60 dias, considerar deletar (era ruído)

## Histórico de fechamentos (cronológico)
- 2026-04-30: P0.11, P0.23 (refactors estruturais SCP/untrack)
- 2026-04-29: P0.13, P0.14 (Greeks live + agregação dollar)
- 2026-04-28: P0.3 (Crush spread texto invertido)
