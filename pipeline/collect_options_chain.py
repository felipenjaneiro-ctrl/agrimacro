"""
Coleta options chain completa do IBKR para todas as commodities
tradeaeis. Salva em options_chain.json.

Para cada underlying:
1. reqContractDetails -> lista futuros disponiveis
2. Pega ate 4 vencimentos futuros
3. reqSecDefOptParams -> strikes/expirations de opcoes
4. reqMktData (streaming) -> bid/ask/greeks

Tolerante a falhas: se um underlying falhar, loga WARN e continua.
"""

from ib_insync import IB, Future, FuturesOption
import json
import asyncio
import math
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).parent.parent
OUT_PATH = BASE / "agrimacro-dash" / "public" / "data" / "processed" / "options_chain.json"

# Todas as commodities tradeaeis
TRADEABLE_UNDERLYINGS = {
    # Graos (CBOT)
    "ZC": {"exchange": "CBOT", "currency": "USD", "name": "Corn"},
    "ZS": {"exchange": "CBOT", "currency": "USD", "name": "Soybeans"},
    "ZW": {"exchange": "CBOT", "currency": "USD", "name": "Wheat"},
    "KE": {"exchange": "CME", "tradingClass": "KE", "currency": "USD", "name": "KC Wheat"},
    "ZM": {"exchange": "CBOT", "currency": "USD", "name": "Soy Meal"},
    "ZL": {"exchange": "CBOT", "currency": "USD", "name": "Soy Oil"},
    # Softs (ICE/NYBOT)
    "SB": {"exchange": "NYBOT", "currency": "USD", "name": "Sugar #11"},
    "KC": {"exchange": "NYBOT", "currency": "USD", "name": "Coffee C"},
    "CT": {"exchange": "NYBOT", "currency": "USD", "name": "Cotton"},
    "CC": {"exchange": "NYBOT", "currency": "USD", "name": "Cocoa"},
    # Pecuaria (CME)
    "LE": {"exchange": "CME", "currency": "USD", "name": "Live Cattle"},
    "GF": {"exchange": "CME", "currency": "USD", "name": "Feeder Cattle"},
    "HE": {"exchange": "CME", "currency": "USD", "name": "Lean Hogs"},
    # Energia (NYMEX)
    "CL": {"exchange": "NYMEX", "currency": "USD", "name": "Crude Oil"},
    "NG": {"exchange": "NYMEX", "currency": "USD", "name": "Nat Gas"},
    # Metais (COMEX)
    "GC": {"exchange": "COMEX", "currency": "USD", "name": "Gold"},
    "SI": {"exchange": "COMEX", "currency": "USD", "name": "Silver"},
}


async def connect_ibkr(client_id=12):
    """Conecta ao IBKR tentando portas conhecidas (async)."""
    ports = [
        (7412, "TWS (Live - Custom)"),
        (7496, "TWS (Live)"),
        (7497, "TWS (Paper)"),
        (4001, "IB Gateway (Live)"),
        (4002, "IB Gateway (Paper)"),
    ]
    ib = IB()
    for port, name in ports:
        try:
            await ib.connectAsync("127.0.0.1", port, clientId=client_id,
                                  timeout=5, readonly=True)
            if ib.isConnected():
                print(f"  [OK] Conectado via {name} (porta {port})")
                return ib
        except Exception as e:
            print(f"  [--] {name} (porta {port}): {e}")
            continue
    raise ConnectionError("Nenhuma conexao IBKR disponivel.")


def safe_float(val):
    """Retorna float ou None se invalido."""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or f == -1.0:
            return None
        return f
    except (TypeError, ValueError):
        return None


async def get_future_contracts(ib: IB, sym: str, exchange: str,
                               trading_class: str = "", n: int = 4):
    """
    Retorna ate N contratos futuros com vencimento futuro,
    ordenados por data de expiracao.
    """
    kwargs = {"symbol": sym, "exchange": exchange, "currency": "USD"}
    if trading_class:
        kwargs["tradingClass"] = trading_class
    fut = Future(**kwargs)
    try:
        details = await asyncio.wait_for(
            ib.reqContractDetailsAsync(fut), timeout=10
        )
    except asyncio.TimeoutError:
        print(f"  [WARN] {sym}: timeout buscando contratos futuros")
        return []
    except Exception as e:
        print(f"  [WARN] {sym}: erro buscando futuros: {e}")
        return []

    if not details:
        return []

    now = datetime.now()
    valid = []
    for d in details:
        exp_str = d.contract.lastTradeDateOrContractMonth
        try:
            if len(exp_str) == 6:
                exp = datetime.strptime(exp_str, "%Y%m")
            else:
                exp = datetime.strptime(exp_str[:8], "%Y%m%d")
            if exp > now:
                valid.append((exp, d.contract))
        except Exception:
            continue

    valid.sort(key=lambda x: x[0])
    return [c for _, c in valid[:n]]


async def fetch_chain_for_underlying(ib: IB, sym: str, spec: dict) -> dict:
    """
    Busca options chain completa para um underlying.
    Coleta ate 4 vencimentos, 30 strikes ATM por vencimento.
    """
    exchange = spec["exchange"]
    name = spec["name"]

    # 1. Buscar contratos futuros disponiveis
    tc = spec.get("tradingClass", "")
    futures = await get_future_contracts(ib, sym, exchange,
                                         trading_class=tc, n=4)
    if not futures:
        print(f"  [WARN] {sym} ({name}): nenhum contrato futuro encontrado")
        return {}

    print(f"  [INFO] {sym}: {len(futures)} contratos futuros: "
          + ", ".join(f.localSymbol for f in futures))

    result = {
        "name": name,
        "und_price": None,
        "expirations": {},
    }

    # 2. Buscar preco do underlying (front-month)
    ib.reqMarketDataType(4)  # delayed-frozen
    front = futures[0]
    ticker = ib.reqMktData(front, "", True, False)
    await asyncio.sleep(3)
    und_price = (safe_float(ticker.last)
                 or safe_float(ticker.close)
                 or safe_float(ticker.marketPrice()))
    ib.cancelMktData(front)
    result["und_price"] = und_price

    if not und_price or und_price <= 0:
        print(f"  [WARN] {sym}: preco do underlying nao disponivel")
        return result

    print(f"  [INFO] {sym}: und_price = {und_price}")

    # 3. Para cada contrato futuro, buscar options chain
    for fut_contract in futures:
        fut_exp = fut_contract.lastTradeDateOrContractMonth
        local_sym = fut_contract.localSymbol

        # Calcular dias ate vencimento
        try:
            if len(fut_exp) == 6:
                exp_dt = datetime.strptime(fut_exp, "%Y%m")
            else:
                exp_dt = datetime.strptime(fut_exp[:8], "%Y%m%d")
            days_to_exp = max(0, (exp_dt - datetime.now()).days)
        except Exception:
            days_to_exp = 0

        # reqSecDefOptParams para este contrato
        chains = []
        try:
            chains = await asyncio.wait_for(
                ib.reqSecDefOptParamsAsync(
                    underlyingSymbol=sym,
                    futFopExchange=exchange,
                    underlyingSecType="FUT",
                    underlyingConId=fut_contract.conId,
                ),
                timeout=10,
            )
        except asyncio.TimeoutError:
            print(f"  [WARN] {sym} {local_sym}: timeout reqSecDefOptParams")
            continue
        except Exception as e:
            print(f"  [WARN] {sym} {local_sym}: erro reqSecDefOptParams: {e}")
            continue

        if not chains:
            print(f"  [WARN] {sym} {local_sym}: sem options chain")
            continue

        # Escolher chain com mais strikes
        chain = max(chains, key=lambda c: len(c.strikes))
        fop_exchange = chain.exchange
        trading_class = chain.tradingClass
        all_strikes = sorted(chain.strikes)
        opt_expirations = sorted(chain.expirations)

        if not opt_expirations:
            print(f"  [WARN] {sym} {local_sym}: chain vazia")
            continue

        # Usar a primeira expiracao de opcoes para este futuro
        opt_exp = opt_expirations[0]

        # Filtrar strikes centrados no ATM
        in_range = [s for s in all_strikes
                    if 0.80 * und_price <= s <= 1.20 * und_price]
        if not in_range:
            print(f"  [WARN] {sym} {local_sym}: nenhum strike em +-20% "
                  f"do ATM ({und_price})")
            continue

        below = sorted([s for s in in_range if s <= und_price],
                       reverse=True)[:15]
        above = sorted([s for s in in_range if s > und_price])[:15]
        atm_strikes = sorted(below + above)

        print(f"  [INFO] {sym} {local_sym} (exp={opt_exp}, "
              f"dte={days_to_exp}): {len(atm_strikes)} strikes "
              f"({atm_strikes[0]:.2f}-{atm_strikes[-1]:.2f}), "
              f"class={trading_class}")

        # Qualificar opcoes em batch
        contracts_to_qualify = []
        for strike in atm_strikes:
            for right in ["C", "P"]:
                fop = FuturesOption(
                    symbol=sym,
                    lastTradeDateOrContractMonth=opt_exp,
                    strike=strike,
                    right=right,
                    exchange=fop_exchange,
                    currency="USD",
                    tradingClass=trading_class,
                )
                contracts_to_qualify.append((fop, right, strike))

        batch_size = 50
        for i in range(0, len(contracts_to_qualify), batch_size):
            batch = [c[0] for c in contracts_to_qualify[i:i + batch_size]]
            try:
                await ib.qualifyContractsAsync(*batch)
            except Exception as e:
                print(f"  [WARN] {sym} {local_sym} qualify batch {i}: {e}")

        valid = [(c, r, s) for c, r, s in contracts_to_qualify if c.conId]
        print(f"  [INFO] {sym} {local_sym}: {len(valid)}/{len(contracts_to_qualify)} qualificados")

        if not valid:
            continue

        # Requisitar market data em batch (streaming)
        tickers = {}
        for contract, right, strike in valid:
            tk = ib.reqMktData(contract, "", False, False)
            tickers[(right, strike)] = tk

        await asyncio.sleep(5)

        # Coletar resultados
        calls = []
        puts = []
        for (right, strike), tk in tickers.items():
            data = {
                "strike": strike,
                "bid": safe_float(tk.bid),
                "ask": safe_float(tk.ask),
                "last": safe_float(tk.last),
                "volume": safe_float(tk.volume),
                "open_interest": safe_float(
                    tk.callOpenInterest if right == "C"
                    else tk.putOpenInterest
                ),
                "iv": (safe_float(tk.modelGreeks.impliedVol)
                       if tk.modelGreeks else None),
                "delta": (safe_float(tk.modelGreeks.delta)
                          if tk.modelGreeks else None),
                "gamma": (safe_float(tk.modelGreeks.gamma)
                          if tk.modelGreeks else None),
                "theta": (safe_float(tk.modelGreeks.theta)
                          if tk.modelGreeks else None),
                "vega": (safe_float(tk.modelGreeks.vega)
                         if tk.modelGreeks else None),
            }
            ib.cancelMktData(tk.contract)
            if right == "C":
                calls.append(data)
            else:
                puts.append(data)

        atm = min(atm_strikes, key=lambda s: abs(s - und_price))
        result["expirations"][opt_exp] = {
            "contract": local_sym,
            "days_to_exp": days_to_exp,
            "atm_strike": atm,
            "calls": sorted(calls, key=lambda x: x["strike"]),
            "puts": sorted(puts, key=lambda x: x["strike"]),
        }

        n_c = len(calls)
        n_p = len(puts)
        print(f"  [OK] {sym} {local_sym} {opt_exp}: "
              f"{n_c} calls, {n_p} puts")

    return result


async def main():
    print("=" * 60)
    print("COLLECT OPTIONS CHAIN — ALL UNDERLYINGS")
    print("=" * 60)

    ib = await connect_ibkr(client_id=12)
    ib.reqMarketDataType(4)  # delayed-frozen para todo o session

    symbols = list(TRADEABLE_UNDERLYINGS.keys())
    print(f"Processando {len(symbols)} underlyings: {', '.join(symbols)}")

    output = {
        "generated_at": datetime.now().isoformat(),
        "source": "IBKR TWS reqSecDefOptParams + reqMktData (FOP)",
        "underlyings": {},
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    failures = 0

    for idx, sym in enumerate(symbols):
        spec = TRADEABLE_UNDERLYINGS[sym]
        print(f"\n[{idx + 1}/{len(symbols)}] Processando {sym} ({spec['name']})...")

        try:
            chain = await asyncio.wait_for(
                fetch_chain_for_underlying(ib, sym, spec),
                timeout=60,
            )
            if chain and chain.get("expirations"):
                output["underlyings"][sym] = chain
                n_exp = len(chain["expirations"])
                total_opts = sum(
                    len(e.get("calls", [])) + len(e.get("puts", []))
                    for e in chain["expirations"].values()
                )
                print(f"  [OK] {sym}: {n_exp} vencimentos, "
                      f"{total_opts} opcoes")
                # Salvar incrementalmente
                output["generated_at"] = datetime.now().isoformat()
                with open(OUT_PATH, "w") as f:
                    json.dump(output, f, indent=2, default=str)
                print(f"  [SAVED] {len(output['underlyings'])} underlyings")
            else:
                failures += 1
                print(f"  [WARN] {sym}: sem dados coletados")
        except asyncio.TimeoutError:
            failures += 1
            print(f"  [ERR] {sym}: TIMEOUT (60s)")
        except Exception as e:
            failures += 1
            print(f"  [ERR] {sym}: {e}")

        if failures > 5:
            print(f"\n[ERR] {failures} falhas — possivel problema de conexao")

        # Rate limiting entre underlyings
        if idx < len(symbols) - 1:
            await asyncio.sleep(2)

    ib.disconnect()

    # Salvar final
    output["generated_at"] = datetime.now().isoformat()
    with open(OUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)

    n = len(output["underlyings"])
    total = sum(
        sum(len(e.get("calls", [])) + len(e.get("puts", []))
            for e in u.get("expirations", {}).values())
        for u in output["underlyings"].values()
    )
    print(f"\n{'=' * 60}")
    print(f"[DONE] {n}/{len(symbols)} underlyings, {total} opcoes total")
    print(f"[DONE] Falhas: {failures}")
    print(f"[DONE] Salvo em: {OUT_PATH}")
    print(f"{'=' * 60}")
    return output


if __name__ == "__main__":
    from ib_insync import util
    util.run(main())
