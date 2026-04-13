
"""
AgriMacro v3.2 - IBKR Data Collector
Collects real-time and historical data from Interactive Brokers
"""
from ib_insync import *
import json
import os
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta


def collect_greeks(ib, positions):
    """
    Para cada posicao de opcao, busca modelGreeks via reqMktData snapshot.
    Nao usa calculateImpliedVolatility (causa erro 10090 e derruba conexao).
    Black-76 cobre o fallback.
    """
    greeks_data = {}
    option_positions = [p for p in positions
                        if p.contract.secType in ('FOP', 'OPT')]

    for i, pos in enumerate(option_positions):
        contract = pos.contract
        local_sym = contract.localSymbol
        try:
            if not ib.isConnected():
                print(f'  [!] Conexao perdida em {local_sym}, abortando Greeks')
                break

            ticker = ib.reqMktData(contract,
                                   genericTickList='106',
                                   snapshot=True,
                                   regulatorySnapshot=False)
            ib.sleep(1.5)

            mg = ticker.modelGreeks
            if mg and mg.delta is not None:
                greeks_data[local_sym] = {
                    'delta': round(mg.delta, 4),
                    'gamma': round(mg.gamma, 6) if mg.gamma else None,
                    'theta': round(mg.theta, 4) if mg.theta else None,
                    'vega': round(mg.vega, 4) if mg.vega else None,
                    'iv': round(mg.impliedVol, 4) if mg.impliedVol else None,
                    'und_price': round(mg.undPrice, 4) if mg.undPrice else None,
                    'opt_price': round(mg.optPrice, 4) if mg.optPrice else None,
                    'source': 'ibkr_live'
                }
                print(f"      [{i+1}/{len(option_positions)}] {local_sym}: delta={greeks_data[local_sym]['delta']} iv={greeks_data[local_sym].get('iv')}")
            else:
                greeks_data[local_sym] = {
                    'error': 'no_model_greeks',
                    'source': 'failed'
                }
                print(f"      [{i+1}/{len(option_positions)}] {local_sym}: no modelGreeks")

            ib.cancelMktData(contract)

        except Exception as e:
            greeks_data[local_sym] = {
                'error': str(e)[:100],
                'source': 'failed'
            }
            if 'Not connected' in str(e):
                print(f'  [!] Conexao perdida, abortando Greeks restantes')
                break

    print(f"    Greeks done: {sum(1 for v in greeks_data.values() if v.get('source')=='ibkr_live')}/{len(option_positions)} with live data")
    return greeks_data


def black76_greeks(F, K, T, r, sigma, option_type='C'):
    """
    Black-76 model para opcoes de futuros.
    F = preco futuro atual, K = strike, T = tempo em anos,
    r = taxa livre de risco, sigma = IV, option_type = 'C' ou 'P'
    """
    import math
    from scipy.stats import norm

    if T <= 0 or sigma <= 0:
        return {'delta': 0, 'gamma': 0, 'theta': 0, 'vega': 0,
                'iv': sigma, 'opt_price': 0, 'source': 'black76_local'}

    d1 = (math.log(F / K) + 0.5 * sigma**2 * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)

    e_rT = math.exp(-r * T)

    if option_type == 'C':
        delta = e_rT * norm.cdf(d1)
        price = e_rT * (F * norm.cdf(d1) - K * norm.cdf(d2))
    else:
        delta = -e_rT * norm.cdf(-d1)
        price = e_rT * (K * norm.cdf(-d2) - F * norm.cdf(-d1))

    gamma = e_rT * norm.pdf(d1) / (F * sigma * math.sqrt(T))
    theta = (-(F * norm.pdf(d1) * sigma * e_rT) / (2 * math.sqrt(T))
             - r * price) / 365
    vega = F * e_rT * norm.pdf(d1) * math.sqrt(T) / 100

    return {
        'delta': round(delta, 4),
        'gamma': round(gamma, 6),
        'theta': round(theta, 4),
        'vega': round(vega, 4),
        'iv': round(sigma, 4),
        'opt_price': round(price, 4),
        'source': 'black76_local'
    }


def fill_missing_greeks(greeks_map, positions, prices_data):
    """
    Para posicoes sem Greeks do IBKR, calcula via Black-76.
    Usa IV defaults por commodity (sem calculateImpliedVolatility).
    prices_data = dict com preco atual por simbolo {sym: {'last_price': float}}
    """
    import datetime as dt

    # IV padrao por asset class
    iv_defaults = {
        'CC': 0.45, 'ZL': 0.38, 'GF': 0.22,
        'ZC': 0.25, 'ZS': 0.22, 'ZW': 0.28,
        'GC': 0.18, 'SI': 0.28, 'CL': 0.42
    }

    r = 0.043  # 10Y treasury
    today = dt.date.today()
    filled = 0

    strike_divisors = {
        'CL': 100, 'SI': 100, 'GF': 10,
        'ZL': 100, 'CC': 1, 'GC': 10,
        'ZC': 100, 'ZS': 100, 'ZW': 100
    }

    month_map = {
        'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
        'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
    }

    for pos in positions:
        local_sym = pos.contract.localSymbol
        if local_sym in greeks_map and not greeks_map[local_sym].get('error'):
            continue  # ja tem Greeks reais

        if pos.contract.secType not in ('FOP', 'OPT'):
            continue

        parts = local_sym.split()
        if len(parts) < 2:
            continue

        right_part = parts[-1]
        opt_type = right_part[0]
        if opt_type not in ('C', 'P'):
            continue

        sym = pos.contract.symbol
        divisor = strike_divisors.get(sym, 100)

        try:
            K = int(right_part[1:]) / divisor
        except (ValueError, IndexError):
            continue

        # Preco futuro atual do price_history
        F = prices_data.get(sym, {}).get('last_price')
        if not F:
            existing = greeks_map.get(local_sym, {})
            F = existing.get('und_price')
        if not F or F <= 0:
            continue

        # Tempo ate expiracao
        contract_code = parts[0]
        try:
            mc = contract_code[-2]
            yr = 2020 + int(contract_code[-1])
            if yr < today.year:
                yr += 10
            month_num = month_map.get(mc, 6)
            exp_date = dt.date(yr, month_num, 16)
            T = max((exp_date - today).days / 365, 0.001)
        except (ValueError, IndexError):
            T = 90 / 365

        sigma = iv_defaults.get(sym, 0.30)
        source = 'black76_estimated_iv'

        result = black76_greeks(F, K, T, r, sigma, opt_type)
        result['und_price'] = round(F, 4)
        result['source'] = source
        result['iv_used'] = round(sigma, 4)
        greeks_map[local_sym] = result
        filled += 1
        print(f"      [B76] {local_sym}: delta={result['delta']} iv={result['iv_used']} F={F} K={K} T={T:.3f} [{source}]")

    print(f"    Black-76 fallback: filled {filled}")
    return greeks_map


# Commodity contract specs for IBKR
COMMODITIES = {
    "ZC": {"symbol": "ZC", "exchange": "CBOT", "name": "Corn"},
    "ZS": {"symbol": "ZS", "exchange": "CBOT", "name": "Soybeans"},
    "ZW": {"symbol": "ZW", "exchange": "CBOT", "name": "Wheat CBOT"},
    "KE": {"symbol": "KE", "exchange": "CBOT", "name": "Wheat KC"},
    "ZM": {"symbol": "ZM", "exchange": "CBOT", "name": "Soybean Meal"},
    "ZL": {"symbol": "ZL", "exchange": "CBOT", "name": "Soybean Oil"},
    "LE": {"symbol": "LE", "exchange": "CME", "name": "Live Cattle"},
    "GF": {"symbol": "GF", "exchange": "CME", "name": "Feeder Cattle"},
    "HE": {"symbol": "HE", "exchange": "CME", "name": "Lean Hogs"},
    "SB": {"symbol": "SB", "exchange": "NYBOT", "name": "Sugar"},
    "KC": {"symbol": "KC", "exchange": "NYBOT", "name": "Coffee"},
    "CT": {"symbol": "CT", "exchange": "NYBOT", "name": "Cotton"},
    "CC": {"symbol": "CC", "exchange": "NYBOT", "name": "Cocoa"},
    "OJ": {"symbol": "OJ", "exchange": "NYBOT", "name": "Orange Juice"},
    "CL": {"symbol": "CL", "exchange": "NYMEX", "name": "Crude Oil"},
    "NG": {"symbol": "NG", "exchange": "NYMEX", "name": "Natural Gas"},
    "GC": {"symbol": "GC", "exchange": "COMEX", "name": "Gold"},
    "SI": {"symbol": "SI", "exchange": "COMEX", "name": "Silver"},
    "DX": {"symbol": "DX", "exchange": "NYBOT", "name": "Dollar Index"},
}

def connect_ibkr(client_id=1):
    """
    Tenta conectar ao TWS (7496) primeiro, depois IB Gateway (4001).
    Retorna objeto IB conectado ou levanta exceção.
    """
    ports = [
        (7412, 'TWS (Live - Custom)'),
        (7496, 'TWS (Live)'),
        (7497, 'TWS (Paper)'),
        (4001, 'IB Gateway (Live)'),
        (4002, 'IB Gateway (Paper)'),
    ]

    for port, name in ports:
        try:
            ib = IB()
            ib.connect('127.0.0.1', port, clientId=client_id,
                      timeout=5, readonly=False)
            if ib.isConnected():
                print(f'  [OK] Conectado via {name} (porta {port})')
                return ib
        except Exception as e:
            print(f'  [--] {name} (porta {port}): {e}')
            continue

    raise ConnectionError(
        'Nenhuma conexao IBKR disponivel. '
        'Abra o TWS ou IB Gateway e tente novamente.'
    )


def collect_ibkr_data(client_id=10):
    base = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(base, '..', 'agrimacro-dash', 'public', 'data', 'processed')

    try:
        ib = connect_ibkr(client_id=client_id)
        print(f"  Connected to IBKR: {ib.managedAccounts()}")
    except ConnectionError as e:
        print(f"  IBKR connection failed: {e}")
        print(f"  Falling back to Yahoo Finance data")
        return {"status": False, "symbols_collected": set(), "error": str(e)}

    # === 1. Get all available contracts for each commodity ===
    print("  [1/4] Discovering contracts...")
    all_contracts = {}
    for sym, spec in COMMODITIES.items():
        try:
            fut = Future(symbol=spec['symbol'], exchange=spec['exchange'])
            contracts = ib.reqContractDetails(fut)
            if contracts:
                all_contracts[sym] = contracts
                print(f"    {sym}: {len(contracts)} contracts found")
            else:
                print(f"    {sym}: no contracts")
            ib.sleep(0.2)
        except Exception as e:
            print(f"    {sym}: error - {e}")

    # === 2. Get market data for front months ===
    print("  [2/4] Fetching market data...")
    prices_result = {}
    for sym, details_list in all_contracts.items():
        if not details_list:
            continue
        # Sort by expiry, take front month
        sorted_details = sorted(details_list, key=lambda d: d.contract.lastTradeDateOrContractMonth)
        # Filter only contracts not yet expired
        now_str = datetime.now().strftime('%Y%m%d')
        active = [d for d in sorted_details if d.contract.lastTradeDateOrContractMonth >= now_str]
        if not active:
            active = sorted_details[-3:]

        front = active[0].contract
        try:
            bars = ib.reqHistoricalData(
                front, endDateTime='', durationStr='1 Y',
                barSizeSetting='1 day', whatToShow='TRADES',
                useRTH=True, formatDate=1
            )
            if bars:
                prices_result[sym] = [{
                    "date": str(b.date),
                    "open": b.open, "high": b.high,
                    "low": b.low, "close": b.close,
                    "volume": int(b.volume)
                } for b in bars]
                print(f"    {sym} front ({front.localSymbol}): {len(bars)} bars")
            else:
                print(f"    {sym}: no historical data")
            ib.sleep(0.5)
        except Exception as e:
            print(f"    {sym}: historical error - {e}")

    # Save prices (same format as collect_prices.py output)
    prices_path = os.path.join(out_dir, 'price_history.json')
    # Merge with existing data (keep Yahoo/Stooq for any missing)
    existing = {}
    if os.path.exists(prices_path):
        try:
            existing = json.load(open(prices_path, 'r'))
        except:
            pass
    for sym, data in prices_result.items():
        existing[sym] = data
    with open(prices_path, 'w') as f:
        json.dump(existing, f, indent=1)
    print(f"    Saved {len(prices_result)} commodities to price_history.json")

    # === 3. Get contract history for all active contracts (up to 48 months forward) ===
    print("  [3/4] Fetching contract history...")
    contract_hist = {}
    horizon_str = (datetime.now() + relativedelta(months=48)).strftime('%Y%m%d')
    for sym, details_list in all_contracts.items():
        sorted_details = sorted(details_list, key=lambda d: d.contract.lastTradeDateOrContractMonth)
        now_str = datetime.now().strftime('%Y%m%d')
        active = [d for d in sorted_details
                  if now_str <= d.contract.lastTradeDateOrContractMonth <= horizon_str]

        for det in active:
            ct = det.contract
            contract_name = ct.localSymbol.replace(' ', '')
            try:
                bars = ib.reqHistoricalData(
                    ct, endDateTime='', durationStr='1 Y',
                    barSizeSetting='1 day', whatToShow='TRADES',
                    useRTH=True, formatDate=1
                )
                if bars and len(bars) > 5:
                    # Map localSymbol to our format (ZCH6 -> ZCH26, ZCH9 -> ZCH29)
                    month_code = contract_name[len(sym)] if len(contract_name) > len(sym) else ''
                    year_short = contract_name[len(sym)+1:] if len(contract_name) > len(sym)+1 else ''
                    # Use IBKR lastTradeDateOrContractMonth for reliable year
                    ltdm = ct.lastTradeDateOrContractMonth or ''
                    if len(ltdm) >= 4:
                        year_full = ltdm[:4]
                        our_name = sym + month_code + year_full[2:]
                    elif len(year_short) == 1:
                        # Single digit: derive decade from current year
                        cur_decade = datetime.now().year // 10 * 10
                        y = cur_decade + int(year_short)
                        if y < datetime.now().year:
                            y += 10  # e.g. digit 2 in 2028 -> 2032? no, wrap to next decade
                        year_full = str(y)
                        our_name = sym + month_code + year_full[2:]
                    elif len(year_short) == 2:
                        year_full = '20' + year_short
                        our_name = sym + month_code + year_short
                    else:
                        year_full = year_short
                        our_name = contract_name

                    month_names = {'F':'Jan','G':'Feb','H':'Mar','J':'Apr','K':'May','M':'Jun',
                                   'N':'Jul','Q':'Aug','U':'Sep','V':'Oct','X':'Nov','Z':'Dec'}

                    contract_hist[our_name] = {
                        "symbol": our_name,
                        "commodity": sym,
                        "local_symbol": contract_name,
                        "expiry_label": month_names.get(month_code, '?') + ' ' + year_full,
                        "bars": [{
                            "date": str(b.date),
                            "open": b.open, "high": b.high,
                            "low": b.low, "close": b.close,
                            "volume": int(b.volume)
                        } for b in bars]
                    }
                    print(f"    {our_name}: {len(bars)} bars")
                ib.sleep(0.3)
            except Exception as e:
                print(f"    {contract_name}: error - {e}")
                ib.sleep(1)

    hist_path = os.path.join(out_dir, 'contract_history.json')
    hist_output = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "source": "IBKR",
        "contract_count": len(contract_hist),
        "contracts": contract_hist
    }
    with open(hist_path, 'w') as f:
        json.dump(hist_output, f, indent=1)
    print(f"    Saved {len(contract_hist)} contracts to contract_history.json")

    # === 4. Get positions and P&L ===
    print("  [4/4] Fetching positions & P&L...")
    raw_positions = ib.positions()
    account = ib.accountSummary()

    # === 4b. Collect Greeks for option positions ===
    print("  [4b] Collecting Greeks for options...")
    greeks_map = collect_greeks(ib, raw_positions)

    # === 4c. Black-76 fallback for positions without Greeks ===
    print("  [4c] Black-76 fallback for missing Greeks...")
    prices_path_for_b76 = os.path.join(out_dir, 'price_history.json')
    prices_data_b76 = {}
    if os.path.exists(prices_path_for_b76):
        try:
            with open(prices_path_for_b76, 'r') as pf:
                ph = json.load(pf)
            for sym, bars_list in ph.items():
                if isinstance(bars_list, list) and bars_list:
                    prices_data_b76[sym] = {'last_price': bars_list[-1]['close']}
        except Exception:
            pass
    greeks_map = fill_missing_greeks(greeks_map, raw_positions, prices_data_b76)

    option_positions = [p for p in raw_positions
                        if p.contract.secType in ('FOP', 'OPT')]

    pos_data = []
    for pos in raw_positions:
        c = pos.contract
        greek_info = greeks_map.get(c.localSymbol, {})
        pos_data.append({
            "symbol": c.symbol,
            "local_symbol": c.localSymbol,
            "sec_type": c.secType,
            "exchange": c.exchange,
            "position": float(pos.position),
            "avg_cost": float(pos.avgCost),
            "market_value": float(pos.position * pos.avgCost),
            "delta": greek_info.get('delta'),
            "gamma": greek_info.get('gamma'),
            "theta": greek_info.get('theta'),
            "vega": greek_info.get('vega'),
            "iv": greek_info.get('iv'),
            "und_price": greek_info.get('und_price'),
            "opt_price": greek_info.get('opt_price')
        })

    acct_data = {}
    for item in account:
        if item.tag in ['NetLiquidation', 'TotalCashValue', 'BuyingPower',
                        'RealizedPnL', 'UnrealizedPnL', 'GrossPositionValue']:
            acct_data[item.tag] = item.value

    # Calcula Greeks agregados do portfolio (sem multiplicador — delta ja e por contrato)
    portfolio_greeks = {'total_delta': 0, 'total_theta': 0, 'total_vega': 0,
                        'positions_with_greeks': 0, 'positions_without_greeks': 0}
    for pos in option_positions:
        g = greeks_map.get(pos.contract.localSymbol, {})
        if g.get('delta') is not None:
            portfolio_greeks['positions_with_greeks'] += 1
            portfolio_greeks['total_delta'] += g['delta'] * float(pos.position)
            if g.get('theta') is not None:
                portfolio_greeks['total_theta'] += g['theta'] * abs(float(pos.position))
            if g.get('vega') is not None:
                portfolio_greeks['total_vega'] += g['vega'] * abs(float(pos.position))
        else:
            portfolio_greeks['positions_without_greeks'] += 1
    portfolio_greeks['total_delta'] = round(portfolio_greeks['total_delta'], 2)
    portfolio_greeks['total_theta'] = round(portfolio_greeks['total_theta'], 2)
    portfolio_greeks['total_vega'] = round(portfolio_greeks['total_vega'], 2)

    portfolio = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "account": ib.managedAccounts()[0] if ib.managedAccounts() else "",
        "summary": acct_data,
        "portfolio_greeks": portfolio_greeks,
        "positions": pos_data,
        "position_count": len(pos_data)
    }

    port_path = os.path.join(out_dir, 'ibkr_portfolio.json')
    with open(port_path, 'w') as f:
        json.dump(portfolio, f, indent=2)
    print(f"    Saved {len(pos_data)} positions to ibkr_portfolio.json")

    # Save separate greeks file
    greeks_path = os.path.join(out_dir, 'ibkr_greeks.json')
    greeks_output = {
        'generated_at': datetime.now().isoformat(),
        'source': 'IBKR via ib_insync (auto-detect port)',
        'portfolio_greeks': portfolio_greeks,
        'positions': greeks_map
    }
    with open(greeks_path, 'w') as f:
        json.dump(greeks_output, f, indent=2)
    print(f"    Saved ibkr_greeks.json ({portfolio_greeks['positions_with_greeks']} with Greeks, {portfolio_greeks['positions_without_greeks']} without)")

    ib.disconnect()
    print("  IBKR collection complete!")
    return {
        "status": True,
        "symbols_collected": set(prices_result.keys()),
        "prices_count": len(prices_result),
        "contracts_count": len(contract_hist),
        "positions_count": len(pos_data),
    }

if __name__ == "__main__":
    collect_ibkr_data()
