#!/usr/bin/env python3
"""Collect MAPA Brazil agriculture data and news."""
import json, requests, re
from datetime import datetime
from pathlib import Path

OUT = Path(__file__).parent / "public" / "data" / "processed" / "mapa_br.json"

def main():
    out = {"updated_at": datetime.now().isoformat(), "source": "MAPA Brasil", "news": [], "exports": {}, "status": "partial"}
    headers = {"User-Agent": "AgriMacro/1.0 (commodity research)"}

    # Try AGROSTAT indicators
    try:
        r = requests.get("https://indicadores.agricultura.gov.br/agrostat/", headers=headers, timeout=15)
        if r.ok and len(r.text) > 500:
            # Extract key numbers from page if available
            text = r.text
            # Look for export values
            nums = re.findall(r'US\$\s*([\d.,]+)\s*(milh|bilh)', text[:5000])
            if nums:
                out["exports"]["raw_matches"] = [{"value": n[0], "scale": n[1]} for n in nums[:5]]
                out["status"] = "ok"
                print(f"  AGROSTAT: found {len(nums)} export figures")
            else:
                print("  AGROSTAT: page loaded but no extractable data")
        else:
            print(f"  AGROSTAT: HTTP {r.status_code if hasattr(r,'status_code') else 'fail'}")
    except Exception as e:
        print(f"  AGROSTAT: {e}")

    # Try MAPA news
    try:
        news_url = "https://www.gov.br/agricultura/pt-br/assuntos/noticias"
        r2 = requests.get(news_url, headers=headers, timeout=15)
        if r2.ok:
            titles = re.findall(r'<h2[^>]*class="tileHeadline"[^>]*>\s*<a[^>]*href="([^"]*)"[^>]*>\s*<span>([^<]+)</span>', r2.text)
            if not titles:
                titles = re.findall(r'<a[^>]*href="(/agricultura/[^"]*)"[^>]*title="([^"]*)"', r2.text)
            for url, title in titles[:10]:
                if not url.startswith("http"):
                    url = "https://www.gov.br" + url
                out["news"].append({"title": title.strip(), "url": url, "date": datetime.now().strftime("%Y-%m-%d")})
            print(f"  MAPA news: {len(out['news'])} articles")
        else:
            print(f"  MAPA news: HTTP {r2.status_code}")
    except Exception as e:
        print(f"  MAPA news: {e}")

    if not out["news"] and not out["exports"]:
        out["status"] = "unavailable"
        print("[WARN] mapa_br: no data collected")
    else:
        print(f"[OK] mapa_br: {len(out['news'])} news, exports={'yes' if out['exports'] else 'no'}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2, default=str))

if __name__ == "__main__":
    main()
