#!/usr/bin/env python3
"""Collect MAGyP Argentina crop estimates."""
import json, requests
from datetime import datetime
from pathlib import Path

OUT = Path(__file__).parent / "public" / "data" / "processed" / "magpy_ar.json"

CROPS = ["Soja", "Maiz", "Trigo", "Girasol"]
BASE = "https://datosestimaciones.magyp.gob.ar/api.php"

def main():
    out = {"updated_at": datetime.now().isoformat(), "source": "MAGyP Argentina", "crops": {}}
    for crop in CROPS:
        try:
            params = {"reporte": "estimaciones", "cultivo": crop, "formato": "json"}
            r = requests.get(BASE, params=params, timeout=20)
            if r.ok:
                data = r.json()
                records_raw = data if isinstance(data, list) else data.get("datos", data.get("data", []))
                # Group by campaign, take last 3
                by_campaign = {}
                for rec in records_raw:
                    camp = rec.get("campana", rec.get("campaña", rec.get("campaign", "?")))
                    if camp == "?":
                        continue
                    if camp not in by_campaign:
                        by_campaign[camp] = {"campaign": camp, "area_ha": 0, "production_ton": 0, "yield_kgha": 0, "count": 0}
                    try:
                        area = float(str(rec.get("supSembrada", rec.get("area_sembrada", 0))).replace(",", ""))
                        prod = float(str(rec.get("produccion", 0)).replace(",", ""))
                        rend = float(str(rec.get("rendimiento", 0)).replace(",", ""))
                        by_campaign[camp]["area_ha"] += area
                        by_campaign[camp]["production_ton"] += prod
                        if rend > 0:
                            by_campaign[camp]["yield_kgha"] = rend
                        by_campaign[camp]["count"] += 1
                    except (ValueError, TypeError):
                        continue

                campaigns = sorted(by_campaign.values(), key=lambda x: x["campaign"], reverse=True)[:3]
                out["crops"][crop] = campaigns
                print(f"  {crop}: {len(campaigns)} campaigns ({', '.join(c['campaign'] for c in campaigns)})")
            else:
                out["crops"][crop] = []
                print(f"  {crop}: HTTP {r.status_code}")
        except Exception as e:
            out["crops"][crop] = []
            print(f"  {crop}: {e}")

    total = sum(len(v) for v in out["crops"].values())
    if total > 0:
        print(f"[OK] magpy_ar: {total} campaigns across {len(CROPS)} crops")
    else:
        out["status"] = "unavailable"
        print("[WARN] magpy_ar: no data collected")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2, default=str))

if __name__ == "__main__":
    main()
