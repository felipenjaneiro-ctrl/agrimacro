#!/usr/bin/env python3
"""
AgriMacro — Hedge Recommendation for Rural Producers

For a producer with X volume to sell in Y months, recommends
options hedge structure in simple language (no tickers, no greeks).

Usage:
  python pipeline/skill_hedge_recommendation.py
"""

import json
import math
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).parent.parent
PROC = BASE / "agrimacro-dash" / "public" / "data" / "processed"

# Commodity specs for BR producers
COMMODITY_SPECS = {
    "soja": {
        "ticker": "ZS", "name": "Soja",
        "unit_br": "sacas (60kg)", "unit_cbot": "bushels",
        "multiplier": 50,  # CBOT contract = 5000 bu = ~50 sacas equivalent
        "sacas_per_contract": 907,  # 5000 bu * 27.22 kg/bu / 60 kg/saca ~ 907
        "bu_per_saca": 2.2046,
    },
    "milho": {
        "ticker": "ZC", "name": "Milho",
        "unit_br": "sacas (60kg)", "unit_cbot": "bushels",
        "multiplier": 50,
        "sacas_per_contract": 2117,  # 5000 bu * 25.4 kg/bu / 60 kg/saca ~ 2117
        "bu_per_saca": 2.362,
    },
    "cafe": {
        "ticker": "KC", "name": "Cafe Arabica",
        "unit_br": "sacas (60kg)", "unit_cbot": "lbs",
        "multiplier": 375,  # 37,500 lbs per contract
        "sacas_per_contract": 283,  # 37500 lbs / 132.276 lbs per saca ~ 283
        "lbs_per_saca": 132.276,
    },
    "boi": {
        "ticker": "LE", "name": "Boi Gordo",
        "unit_br": "arrobas", "unit_cbot": "lbs",
        "multiplier": 400,  # 40,000 lbs per contract
        "arrobas_per_contract": 2667,  # 40000 lbs / 15 kg * 1 arroba/15kg ~ 2667
        "lbs_per_arroba": 33.069,
    },
}


def jload(path):
    for enc in ('utf-8-sig', 'utf-8', 'latin-1'):
        try:
            with open(path, encoding=enc) as f:
                return json.load(f)
        except Exception:
            continue
    return {}


def find_best_put(puts, target_delta=-0.30):
    """Find put closest to target delta."""
    with_delta = [p for p in puts if p.get("delta") is not None and p.get("iv")]
    if not with_delta:
        return None
    return min(with_delta, key=lambda p: abs(p["delta"] - target_delta))


def recommend_hedge(commodity, volume_br, months_to_sale, cost_of_production,
                    usd_brl=5.02, hedge_pct=0.70):
    """
    Generate hedge recommendation for a rural producer.
    """
    spec = COMMODITY_SPECS.get(commodity.lower())
    if not spec:
        print(f"  [ERR] Commodity '{commodity}' nao suportada. Use: soja, milho, cafe, boi")
        return

    ticker = spec["ticker"]
    name = spec["name"]
    unit = spec["unit_br"]

    # Load market data
    prices = jload(PROC / "price_history.json")
    options = jload(PROC / "options_chain.json")
    bcb = jload(PROC / "bcb_data.json")

    bars = prices.get(ticker, [])
    if isinstance(bars, dict):
        bars = bars.get("history", [])
    current_price_usd = bars[-1]["close"] if bars else 0

    # USD/BRL
    brl_data = bcb.get("brl_usd", [])
    if brl_data and isinstance(brl_data[-1], dict):
        usd_brl = brl_data[-1].get("value", usd_brl)

    # Find appropriate expiration (closest to months_to_sale)
    und = options.get("underlyings", {}).get(ticker, {})
    target_dte = months_to_sale * 30
    best_exp = None
    best_exp_data = None
    for ek, ed in und.get("expirations", {}).items():
        dte = ed.get("days_to_exp", 0)
        if dte >= target_dte * 0.6:  # allow some flexibility
            if best_exp is None or abs(dte - target_dte) < abs(best_exp_data.get("days_to_exp", 0) - target_dte):
                best_exp = ek
                best_exp_data = ed

    # Convert prices to BRL
    if commodity == "boi":
        # LE is in cents/lb, convert to R$/arroba
        current_price_brl = (current_price_usd / 100) * spec["lbs_per_arroba"] * usd_brl
        volume_unit = "arrobas"
        contracts_needed = math.ceil(volume_br * hedge_pct / spec["arrobas_per_contract"])
    elif commodity == "cafe":
        # KC is in cents/lb, convert to R$/saca
        current_price_brl = (current_price_usd / 100) * spec["lbs_per_saca"] * usd_brl
        volume_unit = "sacas"
        contracts_needed = math.ceil(volume_br * hedge_pct / spec["sacas_per_contract"])
    else:
        # ZS/ZC in cents/bu, convert to R$/saca
        current_price_brl = (current_price_usd / 100) * spec["bu_per_saca"] * usd_brl
        volume_unit = "sacas"
        contracts_needed = math.ceil(volume_br * hedge_pct / spec["sacas_per_contract"])

    margin_pct = ((current_price_brl - cost_of_production) / cost_of_production) * 100

    # Find puts for hedge
    atm_put = None
    otm_put = None
    if best_exp_data:
        puts = best_exp_data.get("puts", [])
        atm_put = find_best_put(puts, target_delta=-0.45)
        otm_put = find_best_put(puts, target_delta=-0.25)

    # ══════════════════════════════════════════════
    # PRINT RECOMMENDATION
    # ══════════════════════════════════════════════
    print(f"\n  {'='*60}")
    print(f"  RECOMENDACAO DE HEDGE — {name.upper()}")
    print(f"  {'='*60}")

    print(f"\n  SITUACAO DO PRODUTOR:")
    print(f"    Volume:              {volume_br:,.0f} {volume_unit}")
    print(f"    Venda em:            {months_to_sale} meses")
    print(f"    Custo de producao:   R${cost_of_production:,.2f}/{volume_unit.rstrip('s')}")
    print(f"    Hedge desejado:      {hedge_pct*100:.0f}% da producao ({volume_br*hedge_pct:,.0f} {volume_unit})")

    print(f"\n  MERCADO ATUAL:")
    print(f"    Preco Chicago:       ${current_price_usd:.2f} ({bars[-1]['date'] if bars else '?'})")
    print(f"    Dolar:               R${usd_brl:.4f}")
    print(f"    Preco equivalente:   R${current_price_brl:,.2f}/{volume_unit.rstrip('s')}")
    print(f"    Margem sobre custo:  {margin_pct:+.1f}%")

    if margin_pct < 0:
        print(f"    !! ATENCAO: Preco atual ABAIXO do custo de producao!")
    elif margin_pct < 10:
        print(f"    !! Margem apertada — hedge recomendado com urgencia")

    if best_exp_data:
        dte = best_exp_data.get("days_to_exp", 0)
        print(f"\n  VENCIMENTO RECOMENDADO:")
        print(f"    Contrato:            {best_exp_data.get('contract', best_exp)}")
        print(f"    Dias ate vencimento: {dte} dias")
        print(f"    Contratos:           {contracts_needed} contratos = {hedge_pct*100:.0f}% do volume")
    else:
        print(f"\n  [WARN] Sem dados de opcoes disponiveis para {ticker}")
        print(f"  Contratos estimados:   {contracts_needed}")

    # Hedge options
    print(f"\n  {'='*60}")
    print(f"  OPCOES DE PROTECAO (PUT = seguro contra queda)")
    print(f"  {'='*60}")

    print(f"\n  O que e uma PUT?")
    print(f"  E como fazer um contrato de venda antecipada com preco minimo")
    print(f"  garantido — mas voce MANTEM a chance de vender mais caro se o")
    print(f"  mercado subir. O custo e o 'premio' (como o premio de um seguro).")

    if atm_put:
        strike = atm_put["strike"]
        ask = atm_put.get("ask", 0) or 0
        iv = atm_put.get("iv", 0)

        if commodity == "boi":
            strike_brl = (strike / 100) * spec["lbs_per_arroba"] * usd_brl
            premium_per_unit = (ask / 100) * spec["lbs_per_arroba"] * usd_brl
        elif commodity == "cafe":
            strike_brl = (strike / 100) * spec["lbs_per_saca"] * usd_brl
            premium_per_unit = (ask / 100) * spec["lbs_per_saca"] * usd_brl
        else:
            strike_brl = (strike / 100) * spec["bu_per_saca"] * usd_brl
            premium_per_unit = (ask / 100) * spec["bu_per_saca"] * usd_brl

        total_premium = premium_per_unit * volume_br * hedge_pct
        guaranteed_price = strike_brl - premium_per_unit
        guaranteed_margin = ((guaranteed_price - cost_of_production) / cost_of_production) * 100

        print(f"\n  OPCAO 1 — PROTECAO PROXIMA DO PRECO ATUAL")
        print(f"  (Seguro 'completo' — protege perto do preco de hoje)")
        print(f"    Strike:              ${strike:.2f} = ~R${strike_brl:,.2f}/{volume_unit.rstrip('s')}")
        print(f"    Premio (custo):      ~R${premium_per_unit:,.2f}/{volume_unit.rstrip('s')}")
        print(f"    Custo total:         ~R${total_premium:,.0f}")
        print(f"    Preco GARANTIDO:     R${guaranteed_price:,.2f}/{volume_unit.rstrip('s')}")
        print(f"    Margem garantida:    {guaranteed_margin:+.1f}% sobre custo")

        if guaranteed_margin > 0:
            print(f"    --> Lucro GARANTIDO mesmo se mercado desabar")
        else:
            print(f"    --> Margem negativa — considerar opcao mais barata")

    if otm_put:
        strike = otm_put["strike"]
        ask = otm_put.get("ask", 0) or 0

        if commodity == "boi":
            strike_brl = (strike / 100) * spec["lbs_per_arroba"] * usd_brl
            premium_per_unit = (ask / 100) * spec["lbs_per_arroba"] * usd_brl
        elif commodity == "cafe":
            strike_brl = (strike / 100) * spec["lbs_per_saca"] * usd_brl
            premium_per_unit = (ask / 100) * spec["lbs_per_saca"] * usd_brl
        else:
            strike_brl = (strike / 100) * spec["bu_per_saca"] * usd_brl
            premium_per_unit = (ask / 100) * spec["bu_per_saca"] * usd_brl

        total_premium = premium_per_unit * volume_br * hedge_pct
        guaranteed_price = strike_brl - premium_per_unit
        guaranteed_margin = ((guaranteed_price - cost_of_production) / cost_of_production) * 100

        print(f"\n  OPCAO 2 — PROTECAO MAIS BARATA (so contra queda forte)")
        print(f"  (Seguro 'basico' — protege contra queda de 10%+)")
        print(f"    Strike:              ${strike:.2f} = ~R${strike_brl:,.2f}/{volume_unit.rstrip('s')}")
        print(f"    Premio (custo):      ~R${premium_per_unit:,.2f}/{volume_unit.rstrip('s')}")
        print(f"    Custo total:         ~R${total_premium:,.0f}")
        print(f"    Preco GARANTIDO:     R${guaranteed_price:,.2f}/{volume_unit.rstrip('s')}")
        print(f"    Margem garantida:    {guaranteed_margin:+.1f}% sobre custo")

    # Summary
    print(f"\n  {'='*60}")
    print(f"  RESUMO")
    print(f"  {'='*60}")
    print(f"  Sem hedge:  Se {name.lower()} cair 15%, perda de R${current_price_brl*0.15*volume_br:,.0f}")
    print(f"  Com hedge:  Preco minimo garantido, custo eh o premio do seguro")
    print(f"  Risco de NAO fazer hedge eh MAIOR que o custo do seguro")
    print(f"\n  Proximo passo: falar com corretor para executar a operacao")

    return {
        "commodity": commodity,
        "volume": volume_br,
        "months": months_to_sale,
        "contracts": contracts_needed,
        "current_price_brl": round(current_price_brl, 2),
        "cost_of_production": cost_of_production,
        "margin_pct": round(margin_pct, 1),
        "usd_brl": usd_brl,
    }


def main():
    print("=" * 60)
    print("HEDGE RECOMMENDATION — Para Produtor Rural")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d')}")
    print("=" * 60)

    print("\n=== EXEMPLO 1: Sojicultor ===")
    recommend_hedge(
        commodity="soja",
        volume_br=50000,
        months_to_sale=4,
        cost_of_production=95,
        hedge_pct=0.70,
    )

    print("\n\n=== EXEMPLO 2: Pecuarista ===")
    recommend_hedge(
        commodity="boi",
        volume_br=1000,
        months_to_sale=6,
        cost_of_production=290,
        hedge_pct=0.80,
    )


if __name__ == "__main__":
    main()
