import json
import os
from datetime import datetime, timezone

DATA_PATH = os.path.join(os.path.dirname(__file__), "agrimacro-dash", "public", "data", "processed")

# Limites de frescor por fonte (em horas)
FRESHNESS_LIMITS = {
    "futures_contracts.json":   {"max_hours": 24,   "fields": ["updated_at", "timestamp"]},
    "cot.json":                 {"max_hours": 120,  "fields": ["updated_at", "report_date"]},
    "psd_ending_stocks.json":   {"max_hours": 720,  "fields": ["updated_at"]},
    "spreads.json":             {"max_hours": 24,   "fields": ["updated_at", "timestamp"]},
    "seasonality.json":         {"max_hours": 168,  "fields": ["updated_at"]},
    "physical_br.json":         {"max_hours": 48,   "fields": ["updated_at", "date"]},
    "physical_intl.json":       {"max_hours": 48,   "fields": ["updated_at", "date"]},
    "bcb_data.json":            {"max_hours": 48,   "fields": ["updated_at"]},
    "ibge_data.json":           {"max_hours": 720,  "fields": ["updated_at"]},
    "eia_data.json":            {"max_hours": 96,   "fields": ["updated_at", "date"]},
    "conab_data.json":          {"max_hours": 720,  "fields": ["updated_at"]},
    "nasa_power.json":          {"max_hours": 48,   "fields": ["updated_at"]},
    "news.json":                {"max_hours": 24,   "fields": ["updated_at", "generated_at"]},
    "cross_signals.json":       {"max_hours": 24,   "fields": ["updated_at"]},
    "ibkr_portfolio.json":      {"max_hours": 4,    "fields": ["updated_at", "timestamp"]},
    "price_history.json":       {"max_hours": 24,   "fields": ["updated_at"]},
    "daily_reading.json":       {"max_hours": 24,   "fields": ["updated_at", "generated_at"]},
    "weather_agro.json":        {"max_hours": 48,   "fields": ["updated_at"]},
    "calendar.json":            {"max_hours": 168,  "fields": ["updated_at"]},
    "usda_fas.json":            {"max_hours": 168,  "fields": ["updated_at"]},
}

STATUS_OK    = "✅"
STATUS_WARN  = "⚠️ "
STATUS_ERROR = "🔴"
STATUS_INVAL = "🔴🔴"

def parse_date(value):
    if not value:
        return None
    for fmt in [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]:
        try:
            dt = datetime.strptime(str(value)[:26], fmt)
            return dt.replace(tzinfo=timezone.utc)
        except:
            continue
    return None

def check_file(filename, config):
    path = os.path.join(DATA_PATH, filename)
    if not os.path.exists(path):
        return {"file": filename, "status": STATUS_ERROR, "message": "ARQUIVO NAO ENCONTRADO", "hours_old": None}
    
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return {"file": filename, "status": STATUS_INVAL, "message": f"JSON INVALIDO: {e}", "hours_old": None}
    
    date_value = None
    for field in config["fields"]:
        val = data.get(field)
        if val:
            date_value = parse_date(val)
            if date_value:
                break
        # tenta dentro de _meta
        meta = data.get("_meta", {})
        if isinstance(meta, dict):
            val = meta.get(field)
            if val:
                date_value = parse_date(val)
                if date_value:
                    break

    if not date_value:
        return {"file": filename, "status": STATUS_WARN, "message": "DATA NAO ENCONTRADA NO JSON", "hours_old": None}
    
    now = datetime.now(timezone.utc)
    hours_old = (now - date_value).total_seconds() / 3600
    max_h = config["max_hours"]
    
    if hours_old > max_h * 3:
        status = STATUS_INVAL
        msg = f"DADO INVALIDO — {hours_old:.0f}h atraso (limite: {max_h}h)"
    elif hours_old > max_h:
        status = STATUS_ERROR
        msg = f"DESATUALIZADO — {hours_old:.0f}h atraso (limite: {max_h}h)"
    elif hours_old > max_h * 0.75:
        status = STATUS_WARN
        msg = f"ATENÇÃO — {hours_old:.0f}h (limite: {max_h}h)"
    else:
        status = STATUS_OK
        msg = f"{hours_old:.0f}h atrás"
    
    return {"file": filename, "status": status, "message": msg, "hours_old": hours_old}

def main():
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\n{'='*65}")
    print(f"  AGRIMACRO — VALIDAÇÃO DE FRESCOR DE DADOS")
    print(f"  {now_str}")
    print(f"{'='*65}\n")
    
    results = []
    for filename, config in FRESHNESS_LIMITS.items():
        result = check_file(filename, config)
        results.append(result)
    
    # Separar por status
    invalid = [r for r in results if r["status"] == STATUS_INVAL]
    errors  = [r for r in results if r["status"] == STATUS_ERROR]
    warns   = [r for r in results if r["status"] == STATUS_WARN]
    ok      = [r for r in results if r["status"] == STATUS_OK]
    
    if invalid:
        print("🔴🔴 DADOS INVÁLIDOS (NÃO USAR NO RELATÓRIO):")
        for r in invalid:
            print(f"   {r['status']} {r['file']:<35} {r['message']}")
        print()
    
    if errors:
        print("🔴 DESATUALIZADOS (verificar antes de usar):")
        for r in errors:
            print(f"   {r['status']} {r['file']:<35} {r['message']}")
        print()
    
    if warns:
        print("⚠️  ATENÇÃO (próximo do limite):")
        for r in warns:
            print(f"   {r['status']} {r['file']:<35} {r['message']}")
        print()
    
    print("✅ OK:")
    for r in ok:
        print(f"   {r['status']} {r['file']:<35} {r['message']}")
    
    print(f"\n{'='*65}")
    print(f"  RESUMO: {len(ok)} OK | {len(warns)} ATENÇÃO | {len(errors)} ERRO | {len(invalid)} INVÁLIDO")
    
    # Salva relatório JSON para uso pelo dashboard/pipeline
    report = {
        "generated_at": datetime.now().isoformat(),
        "summary": {
            "ok": len(ok),
            "warn": len(warns),
            "error": len(errors),
            "invalid": len(invalid),
            "total": len(results)
        },
        "files": {r["file"]: {
            "status": r["status"],
            "message": r["message"],
            "hours_old": r["hours_old"]
        } for r in results},
        "blocked": [r["file"] for r in invalid + errors]
    }
    
    out_path = os.path.join(DATA_PATH, "data_freshness.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"\n  Relatório salvo em: data_freshness.json")
    print(f"{'='*65}\n")
    
    return len(invalid) + len(errors)

if __name__ == "__main__":
    exit(main())
