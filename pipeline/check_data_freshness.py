"""
check_data_freshness.py - AgriMacro Data Freshness Guard

Le mtime de price_history.json e classifica:
  FRESH    < 8h    (sync recente do PC/MacBook via sync_portfolio.ps1)
  STALE    8h-24h  (banner amarelo no dashboard, recomenda sync)
  CRITICAL > 24h   (banner vermelho, pipeline pula geracao de PDF/video)

Escreve data_freshness.json em agrimacro-dash/public/data/processed/.
Roda como Step 3b do run_pipeline.py.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).parent.parent
PROCESSED = BASE / "agrimacro-dash" / "public" / "data" / "processed"
PRICE_PATH = PROCESSED / "price_history.json"
OUT_PATH = PROCESSED / "data_freshness.json"

FRESH_HOURS = 8
CRITICAL_HOURS = 24


def check_freshness():
    now = datetime.now(timezone.utc)
    if not PRICE_PATH.exists():
        out = {
            "status": "CRITICAL",
            "last_sync_utc": None,
            "hours_old": None,
            "source": "IBKR via PC sync",
            "generated_at": now.isoformat(),
            "error": "price_history.json missing",
        }
    else:
        mtime = datetime.fromtimestamp(PRICE_PATH.stat().st_mtime, tz=timezone.utc)
        hours_old = (now - mtime).total_seconds() / 3600.0
        if hours_old < FRESH_HOURS:
            status = "FRESH"
        elif hours_old < CRITICAL_HOURS:
            status = "STALE"
        else:
            status = "CRITICAL"
        out = {
            "status": status,
            "last_sync_utc": mtime.isoformat(),
            "hours_old": round(hours_old, 2),
            "source": "IBKR via PC sync",
            "generated_at": now.isoformat(),
        }

    PROCESSED.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    return out


if __name__ == "__main__":
    r = check_freshness()
    age = r.get("hours_old")
    age_str = f"{age}h" if age is not None else "n/a"
    print(f"Freshness: {r['status']} (age: {age_str})")
