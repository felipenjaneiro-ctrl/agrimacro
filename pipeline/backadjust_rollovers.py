"""
Back-adjustment Panama de rollovers em continuous futures.

IBKR ContFuture entrega uma serie "spliced" (emendada) SEM back-adjustment:
no dia do rollover, o preco salta pelo spread calendario entre o contrato
expirando e o novo front. Isso distorce medias moveis, RSI, vol realizada
e qualquer comparacao historica. Este script detecta esses gaps e aplica
ajuste aditivo Panama (forward-adjusted).

Convencao Panama (current-level preserved):
- spread = close[rollover_day] - close[rollover_day - 1]
- Todas as barras ANTERIORES ao rollover recebem: open/high/low/close += spread
  (NAO subtrair -- subtrair invertido abriria mais o gap)
- Volume e datas permanecem inalterados
- Efeito: gap fechado; preco atual preservado; historico alinhado ao front atual

Rodar APOS collect_ibkr.py e ANTES de validate_prices.py.

Uso standalone:
    python backadjust_rollovers.py          # processa tudo e sobrescreve
    python -c "from backadjust_rollovers import backadjust; \\
               print(backadjust(dry_run=True, symbol_filter={'KC'}))"
"""

import json
import os
import shutil
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from validate_prices import detect_rollover

BASE = Path(__file__).parent.parent
# Dual-write: processed/ e raw/. Dashboard le de raw/; validate_prices le de processed/.
# collect_prices.py (Yahoo fallback) ja escreve em ambos -- manter paridade aqui tambem.
PH_PATHS = [
    BASE / "agrimacro-dash" / "public" / "data" / "processed" / "price_history.json",
    BASE / "agrimacro-dash" / "public" / "data" / "raw" / "price_history.json",
]
LOG_DIR = BASE / "data" / "logs"
LOG_PATH = LOG_DIR / "rollover_adjustments.log"

SPREAD_PCT_THRESHOLD = 0.02  # |spread / prev_close| > 2% confirma rollover real


def scan_rollovers(bars):
    """
    Varre a serie inteira e retorna todos os rollovers encontrados.
    Exige (via detect_rollover) volume spike + 3d colapso anterior E
    |spread| > 2% do close anterior (guardia contra falsos positivos).

    Retorna: lista de dicts com idx, date, spread, vol_ratio, prior_ratio.
    """
    found = []
    for i in range(20, len(bars)):
        is_roll, vol_ratio, prior_ratio = detect_rollover(bars, i)
        if not is_roll:
            continue
        prev_close = bars[i - 1].get("close", 0) or 0
        today_close = bars[i].get("close", 0) or 0
        if prev_close <= 0:
            continue
        spread = today_close - prev_close
        if abs(spread) / prev_close < SPREAD_PCT_THRESHOLD:
            continue
        found.append({
            "idx": i,
            "date": bars[i].get("date", ""),
            "spread": spread,
            "vol_ratio": vol_ratio,
            "prior_ratio": prior_ratio,
        })
    return found


def apply_panama(bars, rollovers):
    """
    Panama forward-adjusted: para cada rollover, SOMA spread nas barras
    ANTERIORES (open/high/low/close). Retorna nova lista (nao mutativa).
    Operacao aditiva -- ordem de aplicacao nao importa.
    """
    adjusted = [dict(b) for b in bars]
    for r in rollovers:
        idx = r["idx"]
        spread = r["spread"]
        for j in range(idx):
            for k in ("open", "high", "low", "close"):
                if k in adjusted[j] and adjusted[j][k] is not None:
                    adjusted[j][k] = adjusted[j][k] + spread
    return adjusted


def backadjust(dry_run=False, symbol_filter=None, verbose=True):
    """
    dry_run: nao sobrescreve arquivo nem cria backup/log; so retorna o resultado.
    symbol_filter: set/list de simbolos a processar (None = todos).
    verbose: imprime resumo no final.

    Retorna: {sym: {"rollovers": [...], "raw_bars": [...], "adjusted_bars": [...]}}
    """
    existing_paths = [p for p in PH_PATHS if p.exists()]
    if not existing_paths:
        print(f"[ERR] nenhum price_history.json encontrado em {[str(p) for p in PH_PATHS]}")
        return {}

    # Le do primeiro existente (ordem de PH_PATHS: processed/ tem prioridade se presente)
    with open(existing_paths[0], encoding="utf-8") as f:
        data = json.load(f)

    result = {}
    log_lines = []
    ts_iso = datetime.now().isoformat()

    for sym, d in list(data.items()):
        if sym.startswith("_"):
            continue
        if symbol_filter and sym not in symbol_filter:
            continue
        if isinstance(d, list):
            bars = d
            is_list = True
        elif isinstance(d, dict):
            bars = d.get("bars", [])
            is_list = False
        else:
            continue
        if len(bars) < 21:
            continue

        rollovers = scan_rollovers(bars)
        if not rollovers:
            result[sym] = {"rollovers": [], "raw_bars": bars, "adjusted_bars": bars}
            continue

        adjusted = apply_panama(bars, rollovers)
        result[sym] = {"rollovers": rollovers, "raw_bars": bars, "adjusted_bars": adjusted}

        for r in rollovers:
            log_lines.append(
                f"{ts_iso}\t{sym}\t{r['date']}\tspread={r['spread']:+.4f}\t"
                f"vol_ratio={r['vol_ratio']:.1f}x\tprior_ratio={r['prior_ratio']:.2f}x\t"
                f"bars_affected={r['idx']}"
            )

        if not dry_run:
            if is_list:
                data[sym] = adjusted
            else:
                d["bars"] = adjusted

    if dry_run:
        if verbose:
            total = sum(len(r["rollovers"]) for r in result.values())
            print(f"[DRY RUN] {len(result)} simbolos, {total} rollovers detectados (nada salvo)")
        return result

    ts_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Dual-write: backup + escrever em cada path existente
    backup_paths = []
    for p in existing_paths:
        bp = p.parent / f"price_history.json.bak_raw_{ts_tag}"
        shutil.copy2(p, bp)
        backup_paths.append(bp)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=1)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        for line in log_lines:
            f.write(line + "\n")

    if verbose:
        total = sum(len(r["rollovers"]) for r in result.values())
        adjusted_syms = sorted(s for s, r in result.items() if r["rollovers"])
        print("[BACK-ADJUSTMENT PANAMA]")
        print(f"  Simbolos varridos: {len(result)}")
        print(f"  Simbolos ajustados: {len(adjusted_syms)} -> {adjusted_syms}")
        print(f"  Rollovers detectados: {total}")
        print(f"  Paths escritos: {len(existing_paths)} -> {[p.parent.name + '/' + p.name for p in existing_paths]}")
        print(f"  Backups: {[bp.name for bp in backup_paths]}")
        print(f"  Log: {LOG_PATH.relative_to(BASE)}")
    return result


if __name__ == "__main__":
    backadjust()
