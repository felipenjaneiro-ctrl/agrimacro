#!/usr/bin/env python3
"""
AgriMacro — Strategy Manifesto Generator

Generates the strategy philosophy document in 3 formats:
  1. Manifesto completo (para referencia interna)
  2. One-pager (para compartilhar)
  3. Elevator pitch (30 segundos)

Based on real data from trade_skill_base.json and cross_analysis.json.

Run:
  python pipeline/skill_manifesto.py
"""

import json
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent.parent
OUT_DIR = BASE / "pipeline"


def jload(path):
    for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            with open(path, encoding=enc) as f:
                return json.load(f)
        except Exception:
            continue
    return {}


def generate_manifesto():
    print("=" * 60)
    print("STRATEGY MANIFESTO GENERATOR")
    print("=" * 60)

    skill = jload(BASE / "pipeline" / "trade_skill_base.json")
    cross = jload(BASE / "pipeline" / "cross_analysis.json")

    profile = skill.get("trader_profile", {})
    perf = skill.get("historical_performance", {})
    cm = skill.get("capital_management", {})
    rules = skill.get("validated_rules", [])
    strategy = skill.get("strategy_complete", {})
    entry = skill.get("entry_scoring", {})
    seasonality = skill.get("seasonality_edge", {})

    kf = cross.get("key_findings", {})
    roll_data = cross.get("win_rate_by_rolls", {})
    grade_data = cross.get("win_rate_by_grade", {})

    # Stats
    total_trades = perf.get("total_trades", 0)
    total_pnl = perf.get("total_pnl_net", 0)
    win_rate = perf.get("win_rate", 0) * 100
    period = perf.get("period", "?")
    no_roll_wr = roll_data.get("0", {}).get("win_rate", 0)
    best_combo = kf.get("best_combo", "?")
    best_combo_wr = kf.get("best_combo_wr", 0)
    best_month = kf.get("best_month", "?")

    # Hard stops
    hard_stops = [r for r in rules if r.get("severity") == "HARD_STOP"]

    tagline = "Vender tempo, colher theta, nunca rolar."

    # ══════════════════════════════════════════════════
    # FORMAT 1: MANIFESTO COMPLETO
    # ══════════════════════════════════════════════════
    manifesto_full = f"""
{'='*65}
AGRIMACRO — MANIFESTO DA ESTRATEGIA
{'='*65}
"{tagline}"

Gerado em: {datetime.now().strftime('%Y-%m-%d')}
Baseado em: {total_trades} trades reais ({period})

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. QUEM SOMOS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Operador de opcoes sobre futuros de commodities agricolas.
Estilo: premium seller — vendemos volatilidade, coletamos theta.
Instrumento: {profile.get('instrument_preference', 'FOP')}.
Estrutura: butterfly + ratio balanceada (22x22).
Conta: IBKR TWS. Sessao: {profile.get('session', '?')}.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
2. FILOSOFIA CENTRAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

O mercado paga um premio por incerteza. Nos vendemos essa incerteza.
O tempo e nosso aliado — cada dia que passa, nosso lucro cresce.
Nao tentamos adivinhar direcao. Construimos posicoes que lucram
com a PASSAGEM DO TEMPO, nao com o MOVIMENTO DO PRECO.

Principios inegociaveis:
- NUNCA rolar posicao. Dados reais: 0 rolls = {no_roll_wr}% WR.
  Cada roll piora o resultado em ~$12K (comprovado em 183 ciclos).
- Fechar a 50% do max profit. Nao esperar expiracao.
- Max loss = 2x credito recebido. Stop mecanico, sem excecao.
- Credito liquido positivo na abertura. OBRIGATORIO.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
3. EDGE COMPROVADO
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{total_trades} trades | P&L net: ${total_pnl:,.0f} | Win rate: {win_rate:.1f}%

Melhor combinacao: {best_combo} = {best_combo_wr}% WR
Melhor mes de entrada: {best_month}
Underlying mais previsivel: {kf.get('most_predictable', '?')}

Sem rolls:   WR={no_roll_wr}%, net positivo
Com 1 roll:  WR={roll_data.get('1', {}).get('win_rate', 0)}%, net negativo
Conclusao:   ROLAR DESTROI EDGE. Comprovado estatisticamente.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
4. ESTRUTURA DE ENTRADA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{json.dumps(strategy.get('structure_dual_trade', {}).get('consolidated', {}), indent=2, ensure_ascii=False)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
5. REGRAS ABSOLUTAS (HARD STOP)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    for r in hard_stops:
        manifesto_full += f"\n{r['id']}: {r['rule']}\n    Razao: {r['reason']}\n"

    manifesto_full += f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
6. GESTAO DE CAPITAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{cm.get('rule', '60/25/15 split')}

- Trades ativos: max {cm.get('active_trades_max_pct', 60)}% do capital
- Reserva estrategica: {cm.get('strategic_reserve_pct', 25)}% em T-Bills
- Caixa operacional: {cm.get('operational_cash_pct', 15)}%
- Max por underlying: {cm.get('active_trades', {}).get('max_per_underlying_pct', 15)}%
- Max por setor: {cm.get('active_trades', {}).get('max_per_sector_pct', 30)}%
- Regime VEGA: limite expande para 65% quando IV >= 40%

Drawdown protocol: 5%=reduz, 10%=fecha 50%, 15%=cash total, 20%=review

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
7. CHECKLIST DE ENTRADA (10 filtros)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    rule5 = cross.get("rule_5_best_combination", {})
    for item in rule5.get("entry_checklist", []):
        manifesto_full += f"  [ ] {item}\n"

    manifesto_full += f"""
GO se >= 7/10. CONDITIONAL se 5-6. NO-GO se < 5 ou mandatory falha.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"{tagline}"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

    # ══════════════════════════════════════════════════
    # FORMAT 2: ONE-PAGER
    # ══════════════════════════════════════════════════
    one_pager = f"""AGRIMACRO — Strategy One-Pager
"{tagline}"

WHAT: Premium selling em opcoes de commodities agricolas.
HOW:  Butterfly + ratio balanceada (22x22). Credito na abertura.
WHY:  Tempo e aliado. {no_roll_wr}% win rate sem rolls ({total_trades} trades).

EDGE: {best_combo} = {best_combo_wr}% WR. Melhor mes: {best_month}.
RISK: 60/25/15 capital split. Max loss = 2x credito. Nunca rolar.

TOP UNDERLYINGS: SI (prata), KE (trigo KC), HE (suino), ZW (trigo).
AVOID: KC (cafe), NG (gas) sem confluencia forte.

HARD STOPS:
1. Curva forward desfavoravel = NAO OPERAR
2. IV < 20% = NAO VENDER premium
3. WASDE day = FECHAR graos 24h antes
4. Loss > 2x credito = STOP mecanico
5. NUNCA rolar. NUNCA adicionar a perdedor.

TRACK RECORD: {period} | {total_trades} trades | ${total_pnl:,.0f} net
"""

    # ══════════════════════════════════════════════════
    # FORMAT 3: ELEVATOR PITCH
    # ══════════════════════════════════════════════════
    elevator = f"""AGRIMACRO — Elevator Pitch (30 segundos)

Vendemos opcoes de commodities agricolas — soja, milho, boi, prata.
Coletamos premio de quem quer protecao, como uma seguradora.
O tempo trabalha a nosso favor: cada dia que passa, ganhamos.

{total_trades} trades em {period}. ${total_pnl:,.0f} de lucro liquido.
Win rate de {no_roll_wr}% quando seguimos a regra #1: nunca rolar.

"{tagline}"
"""

    # Save all 3
    paths = {}

    p1 = OUT_DIR / "manifesto_full.txt"
    p1.write_text(manifesto_full, encoding="utf-8")
    paths["full"] = str(p1)

    p2 = OUT_DIR / "manifesto_onepager.txt"
    p2.write_text(one_pager, encoding="utf-8")
    paths["onepager"] = str(p2)

    p3 = OUT_DIR / "manifesto_elevator.txt"
    p3.write_text(elevator, encoding="utf-8")
    paths["elevator"] = str(p3)

    # Also save as JSON
    p4 = OUT_DIR / "manifesto.json"
    p4.write_text(json.dumps({
        "generated_at": datetime.now().isoformat(),
        "tagline": tagline,
        "track_record": {
            "trades": total_trades, "pnl_net": total_pnl,
            "win_rate": win_rate, "period": period,
            "no_roll_wr": no_roll_wr,
        },
        "formats": paths,
    }, indent=2, default=str), encoding="utf-8")

    # Print
    print(manifesto_full)
    print(f"\n{'='*60}")
    print("ONE-PAGER:")
    print(one_pager)
    print(f"\n{'='*60}")
    print("ELEVATOR PITCH:")
    print(elevator)

    print(f"\n[SAVED] {p1.name}")
    print(f"[SAVED] {p2.name}")
    print(f"[SAVED] {p3.name}")
    print(f"[SAVED] {p4.name}")
    print(f"\nTagline: \"{tagline}\"")


if __name__ == "__main__":
    generate_manifesto()
