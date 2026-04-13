"""
Validacao de sanidade de precos.
Roda APOS coleta mas ANTES de qualquer uso dos dados.

Limites baseados nos circuit breakers reais da CME:
- Graos: +-7% (CME daily limit ~25-35 cents = ~5-7%)
- Softs: +-10% (CC, KC tem limites maiores)
- Pecuaria: +-5% (LE, GF, HE -- limite CME ~4.5 cents = ~5%)
- Energia: +-15% (CL sem limite fixo, mas +-15% e anormal)
- Metais: +-8%
- Macro: +-2%
"""

import json
import os
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).parent.parent
PH_PATH = BASE / "agrimacro-dash" / "public" / "data" / "processed" / "price_history.json"
CACHE_PATH = BASE / "agrimacro-dash" / "public" / "data" / "processed" / "last_known_good_prices.json"
VAL_PATH = BASE / "agrimacro-dash" / "public" / "data" / "processed" / "price_validation.json"

# Limites de variacao diaria por simbolo (baseados em CME circuit breakers)
DAILY_LIMITS = {
    "ZC": 7.0,  "ZS": 7.0,  "ZW": 7.0,  "KE": 7.0,
    "ZM": 7.0,  "ZL": 7.0,
    "SB": 10.0, "KC": 10.0, "CT": 7.0,
    "CC": 15.0, "OJ": 10.0,
    "LE": 5.0,  "GF": 5.0,  "HE": 5.0,
    "CL": 15.0, "NG": 15.0,
    "GC": 8.0,  "SI": 10.0,
    "DX": 2.0,
}

# Faixas de preco absolutas validas (protecao contra zeros e absurdos)
PRICE_BOUNDS = {
    "ZC": (200, 1200),   "ZS": (600, 3000),  "ZW": (300, 2000),
    "KE": (300, 2000),   "ZM": (200, 800),   "ZL": (20, 200),
    "SB": (5, 50),       "KC": (50, 500),     "CT": (40, 200),
    "CC": (1000, 15000), "OJ": (50, 600),
    "LE": (100, 400),    "GF": (150, 500),    "HE": (50, 200),
    "CL": (20, 300),     "NG": (1, 30),
    "GC": (1000, 10000), "SI": (15, 200),
    "DX": (70, 130),
}


def load_cache():
    """Carrega ultimo preco bom conhecido."""
    if CACHE_PATH.exists():
        with open(CACHE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(data):
    """Salva precos validados no cache."""
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def validate_and_fix():
    """
    Valida price_history.json e corrige dados suspeitos.
    REGRAS (em ordem de aplicacao):
    1. Preco fora dos bounds absolutos -> BLOQUEADO
    2. Variacao diaria > limite -> BLOQUEADO
    3. Variacao vs media 20d > 3x desvio padrao -> WARN
    Acao em dado bloqueado:
    - Nao apagar a barra -- marcar como is_suspicious
    - Substituir current_price e change_pct pelo cache
    - Avisar no log com detalhe do problema
    """
    if not PH_PATH.exists():
        print("[ERR] price_history.json nao encontrado")
        return {"total": 0, "passed": 0, "warned": 0, "blocked": 0, "details": {}}

    with open(PH_PATH, encoding="utf-8") as f:
        data = json.load(f)

    cache = load_cache()
    new_cache = dict(cache)

    validation = {
        "generated_at": datetime.now().isoformat(),
        "total": 0,
        "passed": 0,
        "warned": 0,
        "blocked": 0,
        "details": {}
    }

    for sym, d in data.items():
        # Skip metadata keys
        if sym.startswith("_"):
            continue

        # Handle both formats: list of bars or dict with bars key
        if isinstance(d, list):
            bars = d
        elif isinstance(d, dict):
            bars = d.get("bars", [])
        else:
            continue

        if len(bars) < 2:
            continue

        validation["total"] += 1
        limit = DAILY_LIMITS.get(sym, 10.0)
        bounds = PRICE_BOUNDS.get(sym, (0, 999999))

        current = bars[-1].get("close", 0)
        prev = bars[-2].get("close", 0)
        issues = []

        # REGRA 1: Bounds absolutos
        if not (bounds[0] <= current <= bounds[1]):
            issues.append(
                "Preco {} fora dos bounds validos [{}, {}]".format(
                    current, bounds[0], bounds[1])
            )

        # REGRA 2: Variacao diaria
        if prev > 0:
            daily_chg = abs((current - prev) / prev * 100)
            if daily_chg > limit:
                issues.append(
                    "Variacao diaria {:.1f}% excede limite de +-{}% para {}".format(
                        daily_chg, limit, sym)
                )

        # REGRA 3: Variacao vs media 20 dias
        if len(bars) >= 20:
            closes = [b.get("close", 0) for b in bars[-21:-1]]
            closes = [c for c in closes if c > 0]
            if closes:
                avg = sum(closes) / len(closes)
                std = (sum((c - avg) ** 2 for c in closes) / len(closes)) ** 0.5
                if std > 0 and abs(current - avg) > 3 * std:
                    issues.append(
                        "Preco {} e {:.1f}\u03c3 da media 20d ({:.2f})".format(
                            current, abs(current - avg) / std, avg)
                    )

        # Calcular change_pct real (sempre recalculado)
        real_chg = ((current - prev) / prev * 100) if prev > 0 else 0

        # Build result entry for this symbol
        detail = {
            "current_price": current,
            "change_pct": round(real_chg, 2),
            "is_suspicious": False,
            "reason": None
        }

        if issues:
            # DADO SUSPEITO -- usar cache se disponivel
            cached = cache.get(sym, {})
            fallback_price = cached.get("current_price", prev)
            fallback_chg = cached.get("change_pct", 0)

            detail["is_suspicious"] = True
            detail["reason"] = " | ".join(issues)
            detail["current_price"] = fallback_price
            detail["change_pct"] = fallback_chg
            detail["suspicious_raw_price"] = current
            detail["suspicious_raw_chg"] = round(real_chg, 2)

            # Inject flags into price_history data
            if isinstance(d, dict):
                d["is_suspicious"] = True
                d["suspicious_reason"] = " | ".join(issues)
                d["current_price"] = fallback_price
                d["change_pct"] = fallback_chg
                d["suspicious_raw_price"] = current
                d["suspicious_raw_chg"] = round(real_chg, 2)

            validation["blocked"] += 1
            print("[BLOQUEADO] {}: {} -> usando cache {}".format(
                sym, " | ".join(issues), fallback_price))
        else:
            # DADO OK -- atualizar cache com dado bom
            new_cache[sym] = {
                "current_price": current,
                "change_pct": round(real_chg, 2),
                "date": bars[-1].get("date", ""),
                "saved_at": datetime.now().isoformat()
            }
            validation["passed"] += 1

        validation["details"][sym] = detail

    # Salvar price_history.json corrigido (NAO remove barras)
    with open(PH_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1)

    # Salvar cache atualizado
    save_cache(new_cache)

    # Salvar relatorio de validacao
    with open(VAL_PATH, "w", encoding="utf-8") as f:
        json.dump(validation, f, indent=2)

    print("\n[VALIDACAO COMPLETA]")
    print("  Total: {}".format(validation["total"]))
    print("  Aprovados: {}".format(validation["passed"]))
    print("  Bloqueados: {}".format(validation["blocked"]))

    blocked_syms = [
        s for s, v in validation["details"].items()
        if v["is_suspicious"]
    ]
    if blocked_syms:
        print("  SUSPEITOS: {}".format(blocked_syms))
    else:
        print("  Zero dados suspeitos")

    return validation


if __name__ == "__main__":
    validate_and_fix()
