"""
AgriMacro v3.2 - News & Macro Data Collector
- USDA RSS feeds (reports, announcements)
- Yahoo Finance commodity news RSS
- FRED macro indicators (via API)
"""
import json, os, re
from datetime import datetime
import xml.etree.ElementTree as ET

try:
    import requests
except ImportError:
    print("  [ERROR] requests not installed")
    raise

OUT = os.path.join(os.path.dirname(__file__), "..", "agrimacro-dash", "public", "data", "processed", "news.json")
FRED_KEY_PATH = os.path.join(os.path.expanduser("~"), ".fred_key")

# ── RSS Feeds ─────────────────────────────────────────────────────
RSS_FEEDS = [
    {"url": "https://www.usda.gov/rss/home.xml", "source": "USDA", "category": "usda"},
    {"url": "https://search.ams.usda.gov/mndms/RSS", "source": "USDA Market News", "category": "usda"},
    {"url": "https://finance.yahoo.com/rss/headline?s=ZC=F,ZS=F,ZW=F,KC=F,CT=F,SB=F,CL=F,GC=F,LE=F,HE=F", "source": "Yahoo Finance", "category": "market"},
    {"url": "https://finance.yahoo.com/rss/headline?s=DBA,CORN,SOYB,WEAT", "source": "Yahoo ETFs", "category": "market"},
]

# ── FRED Series (macro indicators relevant to commodities) ───────
FRED_SERIES = {
    "DFF": {"name": "Fed Funds Rate", "category": "macro"},
    "T10Y2Y": {"name": "10Y-2Y Spread", "category": "macro"},
    "DTWEXBGS": {"name": "US Dollar Index (Broad)", "category": "macro"},
    "DCOILWTICO": {"name": "WTI Crude Oil", "category": "energy"},
    "GASREGW": {"name": "US Regular Gas Price", "category": "energy"},
    "DEXBZUS": {"name": "BRL/USD Exchange Rate", "category": "fx"},
    "BAMLH0A0HYM2": {"name": "HY OAS Spread", "category": "credit"},
    "T10YIE": {"name": "10Y Breakeven Inflation", "category": "macro"},
    "UNRATE": {"name": "Unemployment Rate", "category": "macro"},
}

def fetch_rss(url: str, source: str, category: str, max_items: int = 10) -> list:
    articles = []
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "AgriMacro/3.2"})
        if resp.status_code != 200:
            print(f"  [WARN] RSS {source}: HTTP {resp.status_code}")
            return []
        root = ET.fromstring(resp.content)
        # Handle both RSS and Atom
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)
        for item in items[:max_items]:
            title = (item.findtext("title") or item.findtext("atom:title", namespaces=ns) or "").strip()
            link = (item.findtext("link") or "").strip()
            if not link:
                link_el = item.find("atom:link", ns)
                link = link_el.get("href","") if link_el is not None else ""
            pub = (item.findtext("pubDate") or item.findtext("atom:updated", namespaces=ns) or "").strip()
            desc = (item.findtext("description") or item.findtext("atom:summary", namespaces=ns) or "").strip()
            desc = re.sub(r'<[^>]+>', '', desc)[:300]
            if title:
                articles.append({
                    "title": title,
                    "link": link,
                    "date": pub,
                    "description": desc,
                    "source": source,
                    "category": category
                })
    except Exception as e:
        print(f"  [WARN] RSS {source}: {e}")
    return articles

def fetch_fred() -> dict:
    fred_key = ""
    if os.path.exists(FRED_KEY_PATH):
        fred_key = open(FRED_KEY_PATH).read().strip()
    if not fred_key:
        print("  [WARN] No FRED API key found, skipping FRED data")
        return {}

    indicators = {}
    for series_id, meta in FRED_SERIES.items():
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={fred_key}&file_type=json&sort_order=desc&limit=30"
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                print(f"  [WARN] FRED {series_id}: HTTP {resp.status_code}")
                continue
            data = resp.json()
            obs = data.get("observations", [])
            valid = [o for o in obs if o.get("value") and o["value"] != "."]
            if valid:
                latest = valid[0]
                prev = valid[1] if len(valid) > 1 else None
                val = float(latest["value"])
                change = round(val - float(prev["value"]), 4) if prev else None
                indicators[series_id] = {
                    "name": meta["name"],
                    "category": meta["category"],
                    "value": val,
                    "date": latest["date"],
                    "change": change,
                    "history": [{"date": o["date"], "value": float(o["value"])} for o in valid[:30] if o["value"] != "."]
                }
                print(f"  [OK] FRED {series_id}: {val} ({latest['date']})")
        except Exception as e:
            print(f"  [WARN] FRED {series_id}: {e}")
    return indicators

def main():
    print("Collecting news & macro data...")

    # RSS News
    all_news = []
    for feed in RSS_FEEDS:
        articles = fetch_rss(feed["url"], feed["source"], feed["category"])
        all_news.extend(articles)
        print(f"  [OK] {feed['source']}: {len(articles)} articles")

    # FRED Macro
    fred_data = fetch_fred()
    print(f"  [OK] FRED: {len(fred_data)} indicators")

    output = {
        "generated_at": datetime.now().isoformat(),
        "news": all_news,
        "fred": fred_data,
        "total_news": len(all_news),
        "total_fred": len(fred_data)
    }

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"  [OK] News: {len(all_news)} articles, {len(fred_data)} FRED indicators saved")

if __name__ == "__main__":
    main()