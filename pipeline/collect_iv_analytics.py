"""
AgriMacro - IV Analytics (Sprint A)
Le options_chain.json (ja coletado por collect_options_chain.py) e computa:
  - ATM IV = (call_ATM_IV + put_ATM_IV) / 2, front expiry
  - Skew 25-delta (lido de options_chain.skew.skew_val, nao recalculado)
      Nota: skew ATM via IV eh sempre ~0 (put-call parity do IBKR/Black-Scholes).
      A metrica util eh o 25-delta risk reversal, ja calculado upstream.
  - IV Rank 252d: rank do ATM IV atual contra historico de 252 dias uteis

Entrada:  agrimacro-dash/public/data/processed/options_chain.json
Saidas:
  - pipeline/cache/iv_history/{SYM}.json   (append-only, 1 entrada/dia/commodity)
  - agrimacro-dash/public/data/processed/iv_analytics.json  (snapshot atual)

Comportamento idempotente: se ja existe entrada hoje em iv_history/{SYM}.json,
substitui (nao duplica).

Se alguma commodity tem ATM IV mas nao tem skew_val (put ou call 25d None),
skew fica null mas atm_iv + iv_rank continuam -- commodity nao eh bloqueada.

Uso: python pipeline/collect_iv_analytics.py
"""
import json
import sys
from datetime import datetime, date
from pathlib import Path

BASE = Path(__file__).parent.parent
CHAIN_PATH = BASE / "agrimacro-dash" / "public" / "data" / "processed" / "options_chain.json"
OUT_SNAPSHOT = BASE / "agrimacro-dash" / "public" / "data" / "processed" / "iv_analytics.json"
CACHE_DIR = Path(__file__).parent / "cache" / "iv_history"

MIN_DAYS_FOR_RANK = 30  # historico minimo pra calcular IV Rank
RANK_WINDOW = 252       # dias uteis em 1 ano


def find_atm_iv(options_list, atm_strike):
    """Encontra option com strike == atm_strike e retorna seu IV (ou None)."""
    if atm_strike is None:
        return None
    for opt in options_list:
        if opt.get("strike") == atm_strike:
            iv = opt.get("iv")
            return iv if (iv is not None and iv > 0) else None
    return None


def classify_skew(skew_pp):
    """
    Classifica skew em balanced/put_skewed/call_skewed + flag extreme.
    Input: skew em pontos percentuais (ex: 2.34 para 2.34 pp).
    Retorna dict {direction, extreme} ou {direction: None, extreme: False}.
    """
    if skew_pp is None:
        return {"direction": None, "extreme": False}
    abs_skew = abs(skew_pp)
    extreme = abs_skew > 5.0
    if abs_skew < 2.0:
        direction = "balanced"
    elif skew_pp > 0:
        direction = "put_skewed"
    else:
        direction = "call_skewed"
    return {"direction": direction, "extreme": extreme}


def compute_iv_rank(history, current_iv):
    """
    Calcula IV Rank 252d.
    history: lista de {date, atm_iv, skew, spot}
    current_iv: float
    Retorna (iv_rank, days_available) ou (None, days_available) se < MIN_DAYS.
    """
    valid_ivs = [e["atm_iv"] for e in history[-RANK_WINDOW:] if e.get("atm_iv") is not None]
    days_available = len(valid_ivs)
    if days_available < MIN_DAYS_FOR_RANK:
        return None, days_available
    iv_min = min(valid_ivs)
    iv_max = max(valid_ivs)
    if iv_max == iv_min:
        return 50.0, days_available
    rank = (current_iv - iv_min) / (iv_max - iv_min) * 100
    return round(max(0.0, min(100.0, rank)), 1), days_available


def load_history(sym):
    """Carrega historico da commodity (lista de entradas), cria vazio se inexistente."""
    path = CACHE_DIR / f"{sym}.json"
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"  [{sym}] WARN: historico corrompido ({e}); recomecando do zero")
        return []


def save_history(sym, history):
    """Persiste historico da commodity."""
    path = CACHE_DIR / f"{sym}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)


def append_today(history, today_entry):
    """
    Adiciona entrada de hoje ao historico, substituindo se ja existe.
    Idempotente: rodar 2x no mesmo dia nao duplica.
    """
    today_str = today_entry["date"]
    filtered = [e for e in history if e.get("date") != today_str]
    filtered.append(today_entry)
    filtered.sort(key=lambda e: e.get("date", ""))
    return filtered


def main():
    if not CHAIN_PATH.exists():
        print(f"[ERROR] options_chain.json nao encontrado em: {CHAIN_PATH}")
        print("        Rode collect_options_chain.py antes.")
        return 1

    with open(CHAIN_PATH, "r", encoding="utf-8") as f:
        chain = json.load(f)

    underlyings = chain.get("underlyings", {})
    if not underlyings:
        print("[ERROR] options_chain.json vazio (zero underlyings)")
        return 1

    today_iso = date.today().isoformat()
    analytics = {}
    total = 0
    with_atm = 0
    with_rank = 0
    with_skew = 0
    missing_skew = 0
    insufficient = 0

    print(f"[IV ANALYTICS] Processando {len(underlyings)} commodities...")

    for sym, cl in underlyings.items():
        total += 1
        name = cl.get("name", sym)
        spot = cl.get("und_price")
        expirations = cl.get("expirations", {})

        if not expirations:
            print(f"  [{sym}] SKIP: sem expirations")
            continue

        front_key = sorted(expirations.keys())[0]
        front = expirations[front_key]
        atm_strike = front.get("atm_strike")
        dte = front.get("days_to_exp")
        calls = front.get("calls", [])
        puts = front.get("puts", [])

        call_iv = find_atm_iv(calls, atm_strike)
        put_iv = find_atm_iv(puts, atm_strike)

        if call_iv is None or put_iv is None:
            print(f"  [{sym}] SKIP: ATM IV indisponivel (call={call_iv} put={put_iv}) em strike {atm_strike}")
            continue

        with_atm += 1
        atm_iv = (call_iv + put_iv) / 2.0

        # Skew 25-delta lido do options_chain (nao recalculado)
        chain_skew = cl.get("skew", {})
        skew_val_dec = chain_skew.get("skew_val")  # decimal (ex: 0.0234)
        if skew_val_dec is not None:
            skew_pp = round(skew_val_dec * 100, 3)  # converter para pontos percentuais
            with_skew += 1
        else:
            skew_pp = None
            missing_skew += 1
            print(f"  [{sym}] WARN: 25-delta skew indisponivel (put_25d ou call_25d None). atm_iv mantido.")

        skew_class = classify_skew(skew_pp)

        # Historico da commodity
        history = load_history(sym)
        today_entry = {
            "date": today_iso,
            "atm_iv": round(atm_iv, 6),
            "skew_pp": skew_pp,  # em pontos percentuais (ou None)
            "spot": round(spot, 4) if spot else None,
        }
        history = append_today(history, today_entry)
        save_history(sym, history)

        # IV Rank 252d baseado no historico persistido (inclui hoje)
        iv_rank, days_available = compute_iv_rank(history, atm_iv)
        if iv_rank is None:
            insufficient += 1
        else:
            with_rank += 1

        analytics[sym] = {
            "name": name,
            "atm_iv": round(atm_iv, 4),
            "iv_rank_252d": iv_rank,
            "iv_rank_days_available": days_available,
            "skew_pp": skew_pp,
            "skew_type": "25-delta",
            "skew_source": "options_chain",
            "skew_direction": skew_class["direction"],
            "skew_extreme": skew_class["extreme"],
            "front_expiry": front_key,
            "front_dte": dte,
            "atm_strike": atm_strike,
            "spot": round(spot, 4) if spot else None,
            "call_atm_iv": round(call_iv, 4),
            "put_atm_iv": round(put_iv, 4),
            "put_25d_iv": chain_skew.get("put_25d_iv"),
            "call_25d_iv": chain_skew.get("call_25d_iv"),
        }

    if not analytics:
        print("[ERROR] Zero commodities produziram IV analytics (ATM IV indisponivel em todas)")
        return 1

    snapshot = {
        "as_of": datetime.now().isoformat(timespec="seconds"),
        "source_chain_generated_at": chain.get("generated_at"),
        "commodities": analytics,
        "stats": {
            "total": total,
            "with_atm_iv": with_atm,
            "with_skew_25d": with_skew,
            "missing_skew_25d": missing_skew,
            "with_iv_rank": with_rank,
            "insufficient_history": insufficient,
        },
    }

    OUT_SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_SNAPSHOT, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    print(f"[IV ANALYTICS] OK: {with_atm}/{total} com ATM IV | {with_skew} com skew 25d | {with_rank} com Rank 252d | {insufficient} hist insuficiente (<{MIN_DAYS_FOR_RANK}d)")
    print(f"  Snapshot: {OUT_SNAPSHOT.relative_to(BASE)}")
    print(f"  Historico: {CACHE_DIR.relative_to(BASE)}/*.json ({len(analytics)} arquivos)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
