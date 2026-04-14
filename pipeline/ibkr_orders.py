"""
AgriMacro - IBKR Order Execution
Places orders via Interactive Brokers TWS/Gateway.
Called from Next.js API route with JSON payload on stdin.

Usage:
  python pipeline/ibkr_orders.py --help
  echo '{"legs":[...]}' | python pipeline/ibkr_orders.py --execute
  echo '{"legs":[...]}' | python pipeline/ibkr_orders.py --validate
"""

import argparse
import json
import sys
import time
from datetime import datetime

try:
    from ib_insync import *
except ImportError:
    print(json.dumps({"status": "error", "error": "ib_insync not installed"}))
    sys.exit(1)


# Re-use connect logic from collect_ibkr.py
def connect_ibkr(client_id=20):
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
                return ib, port, name
        except Exception:
            continue
    return None, None, None


EXCHANGES = {
    "ZC": "CBOT", "ZS": "CBOT", "ZW": "CBOT", "KE": "CBOT",
    "ZM": "CBOT", "ZL": "CBOT",
    "LE": "CME", "GF": "CME", "HE": "CME",
    "SB": "NYBOT", "KC": "NYBOT", "CT": "NYBOT",
    "CC": "NYBOT", "OJ": "NYBOT",
    "CL": "NYMEX", "NG": "NYMEX",
    "GC": "COMEX", "SI": "COMEX",
    "DX": "NYBOT",
}

OPT_MULTIPLIERS = {
    "CL": 1000, "SI": 5000, "GF": 500, "ZL": 600,
    "CC": 10, "ZC": 50, "ZS": 50, "ZW": 50, "ZM": 100,
    "SB": 1120, "KC": 375, "CT": 500, "LE": 400, "HE": 400,
    "NG": 10000, "GC": 100,
}


def build_contract(leg):
    """Build an ib_insync FuturesOption contract from a leg dict."""
    symbol = leg["symbol"]
    exchange = EXCHANGES.get(symbol, "SMART")
    expiry = leg.get("expiry", "")       # YYYYMMDD format
    strike = float(leg["strike"])
    right = "C" if leg["type"].lower() == "call" else "P"

    contract = FuturesOption(
        symbol=symbol,
        lastTradeDateOrContractMonth=expiry,
        strike=strike,
        right=right,
        exchange=exchange,
    )
    return contract


def build_order(leg, order_type="LMT"):
    """Build an ib_insync Order from a leg dict."""
    action = "SELL" if leg["action"].lower() == "sell" else "BUY"
    qty = int(leg["quantity"])

    if order_type == "LMT":
        price = float(leg.get("limit_price", 0))
        if price <= 0:
            # Use mid of bid/ask as default limit
            bid = float(leg.get("bid", 0))
            ask = float(leg.get("ask", 0))
            price = round((bid + ask) / 2, 2) if bid > 0 and ask > 0 else 0

        order = LimitOrder(action, qty, price)
    else:
        order = MarketOrder(action, qty)

    order.tif = leg.get("tif", "DAY")
    return order


def validate_payload(payload):
    """Validate the order payload before attempting execution."""
    errors = []
    legs = payload.get("legs", [])

    if not legs:
        errors.append("No legs provided")
        return errors

    for i, leg in enumerate(legs):
        prefix = f"Leg {i+1}"
        if "symbol" not in leg:
            errors.append(f"{prefix}: missing 'symbol'")
        if "strike" not in leg:
            errors.append(f"{prefix}: missing 'strike'")
        if "type" not in leg or leg["type"].lower() not in ("call", "put"):
            errors.append(f"{prefix}: 'type' must be 'call' or 'put'")
        if "action" not in leg or leg["action"].lower() not in ("buy", "sell"):
            errors.append(f"{prefix}: 'action' must be 'buy' or 'sell'")
        if "quantity" not in leg or int(leg.get("quantity", 0)) <= 0:
            errors.append(f"{prefix}: 'quantity' must be > 0")

    return errors


def execute_orders(payload):
    """Connect to IBKR and place orders for each leg."""
    legs = payload.get("legs", [])
    order_type = payload.get("order_type", "LMT")
    note = payload.get("note", "")

    # Validate first
    errors = validate_payload(payload)
    if errors:
        return {"status": "error", "errors": errors}

    # Connect
    ib, port, port_name = connect_ibkr(client_id=20)
    if not ib:
        return {
            "status": "error",
            "error": "Cannot connect to IBKR. Open TWS or IB Gateway.",
        }

    results = []
    try:
        account = ib.managedAccounts()[0] if ib.managedAccounts() else "unknown"

        for i, leg in enumerate(legs):
            try:
                contract = build_contract(leg)

                # Qualify the contract to get conId
                qualified = ib.qualifyContracts(contract)
                if not qualified:
                    results.append({
                        "leg": i + 1,
                        "status": "error",
                        "error": f"Contract not found: {leg['symbol']} {leg['type']} {leg['strike']}",
                    })
                    continue

                contract = qualified[0]
                order = build_order(leg, order_type)

                # Place the order
                trade = ib.placeOrder(contract, order)
                ib.sleep(1)  # Wait for order acknowledgment

                results.append({
                    "leg": i + 1,
                    "status": "submitted",
                    "order_id": trade.order.orderId,
                    "contract": contract.localSymbol,
                    "action": order.action,
                    "quantity": int(order.totalQuantity),
                    "order_type": order_type,
                    "limit_price": float(order.lmtPrice) if hasattr(order, 'lmtPrice') else None,
                    "order_status": trade.orderStatus.status if trade.orderStatus else "unknown",
                })

            except Exception as e:
                results.append({
                    "leg": i + 1,
                    "status": "error",
                    "error": str(e)[:200],
                })

        # Record in trade journal if any legs submitted
        submitted = [r for r in results if r["status"] == "submitted"]
        if submitted:
            try:
                from skill_trade_journal import open_trade
                underlying = legs[0].get("symbol", "?")
                is_put = any(l.get("type", "").lower() == "put" for l in legs)
                direction = "PUT" if is_put else "CALL"
                open_trade(
                    underlying=underlying,
                    direction=direction,
                    legs=[{"action": l.get("action"), "type": l.get("type"),
                           "strike": l.get("strike"), "quantity": l.get("quantity")}
                          for l in legs],
                    tese=note,
                )
            except Exception as journal_err:
                print(f"  [WARN] Journal recording failed: {journal_err}")

        return {
            "status": "ok",
            "account": account,
            "port": port,
            "port_name": port_name,
            "timestamp": datetime.now().isoformat(),
            "order_type": order_type,
            "note": note,
            "legs_submitted": len(submitted),
            "legs_failed": len([r for r in results if r["status"] == "error"]),
            "results": results,
        }

    except Exception as e:
        return {"status": "error", "error": str(e)[:500]}

    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


def validate_only(payload):
    """Validate payload and optionally check IBKR connection without placing orders."""
    errors = validate_payload(payload)
    if errors:
        return {"status": "invalid", "errors": errors}

    # Try connecting to verify IBKR is available
    ib, port, port_name = connect_ibkr(client_id=21)
    if not ib:
        return {
            "status": "valid_but_disconnected",
            "message": "Payload is valid but IBKR is not connected.",
        }

    try:
        legs = payload.get("legs", [])
        contract_checks = []
        for i, leg in enumerate(legs):
            contract = build_contract(leg)
            qualified = ib.qualifyContracts(contract)
            contract_checks.append({
                "leg": i + 1,
                "found": bool(qualified),
                "local_symbol": qualified[0].localSymbol if qualified else None,
            })

        account = ib.managedAccounts()[0] if ib.managedAccounts() else "unknown"
        return {
            "status": "valid",
            "account": account,
            "port": port,
            "port_name": port_name,
            "contracts": contract_checks,
        }
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(
        description="AgriMacro IBKR Order Execution",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Validate order (no execution):
    echo '{"legs":[{"symbol":"CL","type":"call","strike":117,"action":"sell","quantity":30,"expiry":"20260720","bid":3.54,"ask":3.60}]}' | python ibkr_orders.py --validate

  Execute order:
    echo '{"legs":[...], "order_type":"LMT", "note":"short call spread"}' | python ibkr_orders.py --execute

Payload format:
  {
    "legs": [
      {
        "symbol": "CL",          // underlying symbol
        "type": "call",           // "call" or "put"
        "strike": 117,            // strike price
        "action": "sell",         // "buy" or "sell"
        "quantity": 30,           // number of contracts
        "expiry": "20260720",     // YYYYMMDD expiry
        "bid": 3.54,              // current bid (for limit price default)
        "ask": 3.60,              // current ask
        "limit_price": 3.54       // optional: explicit limit price
      }
    ],
    "order_type": "LMT",         // "LMT" or "MKT"
    "note": ""                    // free-form note (logged, not sent to IBKR)
  }
""",
    )
    parser.add_argument("--execute", action="store_true", help="Execute orders (reads JSON from stdin)")
    parser.add_argument("--validate", action="store_true", help="Validate orders without executing (reads JSON from stdin)")
    parser.add_argument("--check", action="store_true", help="Check IBKR connection status only")

    args = parser.parse_args()

    if args.check:
        ib, port, port_name = connect_ibkr(client_id=22)
        if ib:
            account = ib.managedAccounts()[0] if ib.managedAccounts() else "unknown"
            ib.disconnect()
            print(json.dumps({
                "status": "connected",
                "account": account,
                "port": port,
                "port_name": port_name,
            }))
        else:
            print(json.dumps({"status": "disconnected"}))
        return

    if args.validate or args.execute:
        raw = sys.stdin.read()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            print(json.dumps({"status": "error", "error": f"Invalid JSON: {e}"}))
            sys.exit(1)

        if args.validate:
            result = validate_only(payload)
        else:
            result = execute_orders(payload)

        print(json.dumps(result, indent=2))
        return

    # No flags: show help
    parser.print_help()


if __name__ == "__main__":
    main()
