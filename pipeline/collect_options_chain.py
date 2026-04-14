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
from datetime import datetime, timedelta

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


def clean_greek(v):
    """Limpa valor de greek: None/nan/inf/absurdo -> None."""
    if v is None:
        return None
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f) or abs(f) > 1e10:
            return None
        if f == -1:
            return None
        return round(f, 6)
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
        # genericTickList='106' = Option Model Greeks (critical for IV/delta)
        tickers = {}
        for contract, right, strike in valid:
            tk = ib.reqMktData(contract, "106", False, False)
            tickers[(right, strike)] = tk

        # Grains (CBOT) need more time to populate modelGreeks via streaming
        wait_secs = 8 if exchange == "CBOT" else 5
        await asyncio.sleep(wait_secs)

        # Coletar resultados
        calls = []
        puts = []
        for (right, strike), tk in tickers.items():
            mg = tk.modelGreeks
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
                "iv": clean_greek(mg.impliedVol) if mg else None,
                "delta": clean_greek(mg.delta) if mg else None,
                "gamma": clean_greek(mg.gamma) if mg else None,
                "theta": clean_greek(mg.theta) if mg else None,
                "vega": clean_greek(mg.vega) if mg else None,
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
        iv_count = sum(1 for c in calls if c["iv"] is not None)
        print(f"  [OK] {sym} {local_sym} {opt_exp}: "
              f"{n_c} calls ({iv_count} com IV), {n_p} puts")

    return result


IV_HISTORY_PATH = BASE / "agrimacro-dash" / "public" / "data" / "processed" / "iv_history.json"


def get_atm_iv(expiration_data, und_price):
    """Extract ATM IV from an expiration's calls (closest delta to 0.5)."""
    calls = expiration_data.get("calls", [])
    with_iv = [c for c in calls if c.get("iv") is not None and c["iv"] > 0]
    if not with_iv:
        return None
    # Prefer delta-based ATM, fallback to strike-based
    with_delta = [c for c in with_iv if c.get("delta") is not None]
    if with_delta:
        best = min(with_delta, key=lambda c: abs(c["delta"] - 0.5))
        if abs(best["delta"] - 0.5) < 0.25:
            return best["iv"]
    # Fallback: closest strike to und_price
    if und_price and und_price > 0:
        best = min(with_iv, key=lambda c: abs(c["strike"] - und_price))
        return best["iv"]
    return None


def compute_iv_analytics(output):
    """
    Compute per-underlying:
    - iv_rank: 52-week percentile of current ATM IV (from iv_history.json)
    - skew: OTM put IV vs OTM call IV (25-delta)
    - term_structure: IV across expirations (contango/backwardation)

    Mutates output["underlyings"][sym] in-place.
    """
    today = datetime.now().strftime("%Y-%m-%d")

    # Load IV history for rank calculation
    iv_history = {}
    if IV_HISTORY_PATH.exists():
        try:
            iv_history = json.load(open(IV_HISTORY_PATH))
        except Exception:
            iv_history = {}

    for sym, data in output.get("underlyings", {}).items():
        expirations = data.get("expirations", {})
        und_price = data.get("und_price", 0) or 0
        if not expirations:
            continue

        sorted_exps = sorted(expirations.keys())

        # ── ATM IV from front-month ──
        front_exp = sorted_exps[0]
        front_data = expirations[front_exp]
        current_iv = get_atm_iv(front_data, und_price)

        # ── Term Structure: ATM IV per expiration ──
        term_points = []
        for exp_key in sorted_exps:
            exp_d = expirations[exp_key]
            dte = exp_d.get("days_to_exp", 0)
            atm_iv = get_atm_iv(exp_d, und_price)
            if atm_iv is not None:
                term_points.append({
                    "expiry": exp_key,
                    "dte": dte,
                    "iv": round(atm_iv, 4),
                })

        # Determine term structure shape
        term_shape = "FLAT"
        if len(term_points) >= 2:
            front_iv = term_points[0]["iv"]
            back_iv = term_points[-1]["iv"]
            if front_iv > 0:
                diff_pct = ((back_iv - front_iv) / front_iv) * 100
                if diff_pct > 5:
                    term_shape = "CONTANGO"
                elif diff_pct < -5:
                    term_shape = "BACKWARDATION"

        data["term_structure"] = {
            "points": term_points,
            "structure": term_shape,
        }

        # ── Skew: 25-delta put IV vs 25-delta call IV ──
        calls = front_data.get("calls", [])
        puts = front_data.get("puts", [])

        # Find 25-delta options (OTM wings)
        otm_call_iv = None
        otm_put_iv = None

        calls_with_d = [c for c in calls
                        if c.get("delta") is not None and c.get("iv") is not None]
        puts_with_d = [p for p in puts
                       if p.get("delta") is not None and p.get("iv") is not None]

        if calls_with_d:
            # 25-delta call: delta closest to 0.25
            best = min(calls_with_d, key=lambda c: abs(c["delta"] - 0.25))
            if abs(best["delta"] - 0.25) < 0.15:
                otm_call_iv = best["iv"]

        if puts_with_d:
            # 25-delta put: delta closest to -0.25
            best = min(puts_with_d, key=lambda p: abs(p["delta"] + 0.25))
            if abs(best["delta"] + 0.25) < 0.15:
                otm_put_iv = best["iv"]

        skew_val = None
        skew_pct = None
        if otm_put_iv and otm_call_iv and otm_call_iv > 0:
            skew_val = round(otm_put_iv - otm_call_iv, 4)
            skew_pct = round(((otm_put_iv / otm_call_iv) - 1) * 100, 1)

        data["skew"] = {
            "put_25d_iv": round(otm_put_iv, 4) if otm_put_iv else None,
            "call_25d_iv": round(otm_call_iv, 4) if otm_call_iv else None,
            "skew_val": skew_val,
            "skew_pct": skew_pct,
        }

        # ── IV Rank: percentile of current IV over 52-week history ──
        if current_iv is not None:
            # Append to history
            if sym not in iv_history:
                iv_history[sym] = []
            iv_history[sym].append({
                "date": today,
                "iv": round(current_iv, 4),
            })
            # Dedupe by date (keep latest per day)
            seen = {}
            for entry in iv_history[sym]:
                seen[entry["date"]] = entry
            iv_history[sym] = sorted(seen.values(), key=lambda x: x["date"])
            # Keep only last 365 days
            cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            iv_history[sym] = [e for e in iv_history[sym] if e["date"] >= cutoff]

            # Compute rank
            hist_ivs = [e["iv"] for e in iv_history[sym]]
            if len(hist_ivs) >= 2:
                iv_min = min(hist_ivs)
                iv_max = max(hist_ivs)
                if iv_max > iv_min:
                    iv_rank = round(
                        ((current_iv - iv_min) / (iv_max - iv_min)) * 100, 1
                    )
                else:
                    iv_rank = 50.0
            else:
                iv_rank = None  # Not enough history yet

            data["iv_rank"] = {
                "current_iv": round(current_iv, 4),
                "rank_52w": iv_rank,
                "iv_high_52w": round(iv_max, 4) if len(hist_ivs) >= 2 else None,
                "iv_low_52w": round(iv_min, 4) if len(hist_ivs) >= 2 else None,
                "history_days": len(hist_ivs),
            }
        else:
            data["iv_rank"] = {
                "current_iv": None,
                "rank_52w": None,
                "history_days": 0,
            }

        # Log
        iv_str = f"{current_iv*100:.1f}%" if current_iv else "N/A"
        rank_str = (f"{data['iv_rank']['rank_52w']:.0f}%"
                    if data['iv_rank'].get('rank_52w') is not None else "building")
        skew_str = f"{skew_pct:+.1f}%" if skew_pct is not None else "N/A"
        print(f"  {sym}: IV={iv_str} Rank={rank_str} "
              f"Skew={skew_str} Term={term_shape} "
              f"({len(term_points)} pts)")

    # Save IV history
    try:
        with open(IV_HISTORY_PATH, "w") as f:
            json.dump(iv_history, f, indent=1)
        n_syms = len(iv_history)
        total_pts = sum(len(v) for v in iv_history.values())
        print(f"  [SAVED] iv_history.json: {n_syms} syms, {total_pts} data points")
    except Exception as e:
        print(f"  [WARN] Failed to save iv_history.json: {e}")


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

    # ── Post-processing: IV Rank, Skew, Term Structure ──
    print("\n[POST] Calculando IV Rank, Skew, Term Structure...")
    compute_iv_analytics(output)

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
