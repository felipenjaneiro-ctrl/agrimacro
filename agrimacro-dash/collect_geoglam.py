#!/usr/bin/env python3
"""Collect GEOGLAM Crop Monitor reports."""
import json, requests
from datetime import datetime
from pathlib import Path

OUT = Path(__file__).parent / "public" / "data" / "processed" / "geoglam.json"

def main():
    out = {"updated_at": datetime.now().isoformat(), "source": "GEOGLAM Crop Monitor", "reports": []}
    try:
        r = requests.get("https://cropmonitor.org/tools/api/v1.0/cmreports/", timeout=15)
        if r.ok:
            data = r.json()
            reports = data if isinstance(data, list) else data.get("reports", data.get("results", []))
            for rep in reports[:50]:
                out["reports"].append({
                    "region": rep.get("region", rep.get("country", "?")),
                    "crop": rep.get("crop", rep.get("commodity", "?")),
                    "condition": rep.get("condition", rep.get("status", "?")),
                    "date": rep.get("date", rep.get("report_date", "?")),
                    "description": str(rep.get("description", rep.get("summary", "")))[:200],
                })
            print(f"[OK] geoglam: {len(out['reports'])} reports")
        else:
            print(f"[WARN] API {r.status_code}, trying RSS...")
            raise Exception("API failed")
    except Exception as e:
        try:
            import feedparser
            feed = feedparser.parse("https://cropmonitor.org/feed/")
            for entry in feed.entries[:20]:
                out["reports"].append({
                    "region": "Global", "crop": "Multi",
                    "condition": "See report", "date": entry.get("published", "?"),
                    "description": entry.get("title", "")[:200],
                })
            if out["reports"]:
                print(f"[OK] geoglam RSS fallback: {len(out['reports'])} entries")
            else:
                out["status"] = "unavailable"
                out["error"] = str(e)[:200]
                print(f"[WARN] geoglam: {e}")
        except ImportError:
            out["status"] = "unavailable"
            out["error"] = str(e)[:200]
            print(f"[WARN] geoglam: {e}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2, default=str))

if __name__ == "__main__":
    main()
