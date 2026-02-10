
"""
AgriMacro v3.2 - IBKR Data Collector
Collects real-time and historical data from Interactive Brokers
"""
from ib_insync import *
import json
import os
import sys
from datetime import datetime

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

def collect_ibkr_data(host='127.0.0.1', port=4001, client_id=10):
    base = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(base, '..', 'agrimacro-dash', 'public', 'data', 'processed')

    ib = IB()
    try:
        ib.connect(host, port, clientId=client_id)
        print(f"  Connected to IBKR: {ib.managedAccounts()}")
    except Exception as e:
        print(f"  IBKR connection failed: {e}")
        print(f"  Falling back to Yahoo Finance data")
        return False

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

    # === 3. Get contract history for all active contracts ===
    print("  [3/4] Fetching contract history...")
    contract_hist = {}
    for sym, details_list in all_contracts.items():
        sorted_details = sorted(details_list, key=lambda d: d.contract.lastTradeDateOrContractMonth)
        now_str = datetime.now().strftime('%Y%m%d')
        active = [d for d in sorted_details if d.contract.lastTradeDateOrContractMonth >= now_str][:8]

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
                    # Map localSymbol to our format (ZCH6 -> ZCH26)
                    month_code = contract_name[2] if len(contract_name) > 2 else ''
                    year_short = contract_name[3:] if len(contract_name) > 3 else ''
                    if len(year_short) == 1:
                        year_full = '202' + year_short
                        our_name = sym + month_code + '2' + year_short
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
    positions = ib.positions()
    account = ib.accountSummary()

    pos_data = []
    for pos in positions:
        c = pos.contract
        pos_data.append({
            "symbol": c.symbol,
            "local_symbol": c.localSymbol,
            "sec_type": c.secType,
            "exchange": c.exchange,
            "position": float(pos.position),
            "avg_cost": float(pos.avgCost),
            "market_value": float(pos.position * pos.avgCost)
        })

    acct_data = {}
    for item in account:
        if item.tag in ['NetLiquidation', 'TotalCashValue', 'BuyingPower',
                        'RealizedPnL', 'UnrealizedPnL', 'GrossPositionValue']:
            acct_data[item.tag] = item.value

    portfolio = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "account": ib.managedAccounts()[0] if ib.managedAccounts() else "",
        "summary": acct_data,
        "positions": pos_data,
        "position_count": len(pos_data)
    }

    port_path = os.path.join(out_dir, 'ibkr_portfolio.json')
    with open(port_path, 'w') as f:
        json.dump(portfolio, f, indent=2)
    print(f"    Saved {len(pos_data)} positions to ibkr_portfolio.json")

    ib.disconnect()
    print("  IBKR collection complete!")
    return True

if __name__ == "__main__":
    collect_ibkr_data()
