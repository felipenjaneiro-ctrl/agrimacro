"""
Validacao de sanidade de precos.
Roda APOS coleta mas ANTES de qualquer uso dos dados.

Limites por grupo de commodity:
- Graos: +-5%
- Softs: +-7%
- Pecuaria: +-8% (volatil apos WASDE/Cold Storage/H5N1 news)
- Energia: +-10%
- Metais: +-7%
- FX: +-3%
Override: volume do dia > 5x media 20d anula flags de variacao
(gap com fluxo e movimento real, nao dado corrompido).

Rollover: volume do dia > 3x media 20d AND media dos 3 dias anteriores
< 0.3x media 20d -> marca is_rollover_gap (nao suspeito). Fingerprint
de troca de contrato na serie ContFuture do IBKR (sem back-adjustment).
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
    # Graos: +-5%
    "ZC": 5.0,  "ZS": 5.0,  "ZW": 5.0,  "KE": 5.0,
    "ZM": 5.0,  "ZL": 5.0,
    # Softs: +-7%
    "CC": 7.0,  "KC": 7.0,  "SB": 7.0,  "CT": 7.0,
    "OJ": 10.0,  # nao incluido no rebalance -- mantido
    # Pecuaria: +-8% (volatil apos reports)
    "LE": 8.0,  "GF": 8.0,  "HE": 8.0,
    # Energia: +-10%
    "CL": 10.0, "NG": 10.0,
    # Metais: +-7%
    "GC": 7.0,  "SI": 7.0,
    # FX: +-3%
    "DX": 3.0,  "BRL": 3.0,
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
    "BRL": (3, 10),
}


def detect_rollover(bars, idx):
    """
    Detecta se bars[idx] e um dia de rollover de contrato continuous futures.

    Heuristica: volume do dia > 3x media 20d AND media dos 3 dias uteis
    anteriores < 0.3x media 20d. Fingerprint indica liquidez migrando do
    contrato expirando para o novo front -- gap de rollover na serie
    ContFuture do IBKR (nao back-adjusted).

    Retorna: (is_rollover, vol_ratio, prior_ratio)
    """
    if idx < 20:
        return False, 0.0, 0.0
    window = bars[idx - 20:idx]
    vols = [b.get("volume", 0) or 0 for b in window]
    vols = [v for v in vols if v > 0]
    if not vols:
        return False, 0.0, 0.0
    avg_vol = sum(vols) / len(vols)
    if avg_vol <= 0:
        return False, 0.0, 0.0

    today_vol = bars[idx].get("volume", 0) or 0
    prior_3 = [bars[idx - k].get("volume", 0) or 0 for k in (1, 2, 3)]
    prior_mean = sum(prior_3) / 3

    vol_ratio = today_vol / avg_vol
    prior_ratio = prior_mean / avg_vol

    is_rollover = (vol_ratio > 3.0) and (prior_ratio < 0.3)
    return is_rollover, vol_ratio, prior_ratio


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
        "rollovers": 0,
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

        # REGRA 1: Bounds absolutos
        bounds_issue = None
        if not (bounds[0] <= current <= bounds[1]):
            bounds_issue = "Preco {} fora dos bounds validos [{}, {}]".format(
                current, bounds[0], bounds[1])

        # REGRA 2: Variacao diaria
        variation_issues = []
        if prev > 0:
            daily_chg = abs((current - prev) / prev * 100)
            if daily_chg > limit:
                variation_issues.append(
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
                    variation_issues.append(
                        "Preco {} e {:.1f}\u03c3 da media 20d ({:.2f})".format(
                            current, abs(current - avg) / std, avg)
                    )

        # REGRA 4a (rollover): gap por troca de contrato NAO e suspeito.
        # Checa antes do override de volume porque e fingerprint mais especifico.
        is_rollover_gap = False
        rollover_info = None
        if variation_issues:
            is_roll, vr, pr = detect_rollover(bars, len(bars) - 1)
            if is_roll:
                is_rollover_gap = True
                rollover_info = {"vol_ratio": round(vr, 2), "prior_ratio": round(pr, 2)}
                variation_issues = []
                validation["rollovers"] += 1
                print("[ROLLOVER] {}: gap por troca de contrato (vol {:.1f}x, prior {:.2f}x)".format(
                    sym, vr, pr))

        # REGRA 4b (override de volume): so se NAO for rollover.
        # volume do dia > 5x media 20d anula flags de variacao (news flow).
        if variation_issues and not is_rollover_gap and len(bars) >= 20:
            current_vol = bars[-1].get("volume", 0) or 0
            vols = [b.get("volume", 0) or 0 for b in bars[-21:-1]]
            vols = [v for v in vols if v > 0]
            if vols and current_vol > 0:
                avg_vol = sum(vols) / len(vols)
                if avg_vol > 0 and current_vol > 5 * avg_vol:
                    print("[OVERRIDE] {}: variacao anulada por volume {:.1f}x media (movimento real)".format(
                        sym, current_vol / avg_vol))
                    variation_issues = []

        issues = ([bounds_issue] if bounds_issue else []) + variation_issues

        # Calcular change_pct real (sempre recalculado)
        real_chg = ((current - prev) / prev * 100) if prev > 0 else 0

        # Build result entry for this symbol
        detail = {
            "current_price": current,
            "change_pct": round(real_chg, 2),
            "is_suspicious": False,
            "reason": None
        }
        if is_rollover_gap:
            detail["is_rollover_gap"] = True
            detail["rollover_info"] = rollover_info

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
    print("  Rollovers: {}".format(validation["rollovers"]))

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
