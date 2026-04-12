#!/usr/bin/env python3
"""
collect_grok_email.py - AgriMacro Grok Tasks Email Collector
=============================================================
Reads Grok Tasks emails from Gmail via Google OAuth and saves
classified content to JSONs for Dashboard auto-display.

Setup (one-time):
  1. Create OAuth credentials at console.cloud.google.com
  2. Save as credentials.json in project root
  3. Run: python pipeline/collect_grok_email.py --auth
  4. Authorize in browser -> token.json saved

After setup, runs fully automated in the pipeline.

Output:
  grok_sentiment.json  (Sentimento / X / Twitter)
  grok_news.json       (Noticias / Agricola)
  grok_macro.json      (Macro / Geopolitica)
"""

import json
import os
import sys
import base64
import re
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
ROOT_DIR = SCRIPT_DIR.parent
PROC = ROOT_DIR / "agrimacro-dash" / "public" / "data" / "processed"
OUTPUT_DIR = PROC if PROC.exists() else SCRIPT_DIR

CREDS_PATH = ROOT_DIR / "credentials.json"
TOKEN_PATH = ROOT_DIR / "token.json"
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

SEARCH_QUERY = "from:noreply@x.ai newer_than:1d"

CLASSIFIERS = [
    (["sentimento", "sentiment", "twitter", "x (twitter)", "mercado financeiro", "redes sociais"], "grok_sentiment.json"),
    (["noticia", "noticias", "agricola", "agro", "news", "commodity", "safra"], "grok_news.json"),
    (["macro", "geopolitica", "geopolitics", "economia", "economic", "juros", "dolar"], "grok_macro.json"),
]


def _classify(subject):
    subj_lower = (subject or "").lower()
    for patterns, filename in CLASSIFIERS:
        if any(p in subj_lower for p in patterns):
            return filename
    return "grok_general.json"


def _make_output(content, subject="", email_date="", category=""):
    lines = [l.strip() for l in content.strip().split("\n") if l.strip()]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "is_fallback": False,
        "source": "Grok Tasks via Gmail",
        "category": category,
        "email_subject": subject,
        "email_date": email_date,
        "content": content.strip(),
        "summary": " ".join(lines[:3])[:300],
    }


def _make_fallback(reason):
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "is_fallback": True,
        "error": reason,
        "source": "Grok Tasks via Gmail",
        "content": "",
        "summary": "",
    }


def _save(filename, data):
    with open(OUTPUT_DIR / filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _strip_html(html):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["style", "script", "head"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    lines = [line.strip() for line in text.splitlines()]
    return re.sub(r"\n{3,}", "\n\n", "\n".join(line for line in lines if line))


def _extract_text(parts):
    plain = ""
    html = ""
    for part in parts:
        mime = part.get("mimeType", "")
        data = part.get("body", {}).get("data", "")
        if mime == "text/plain" and data:
            plain += base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        elif mime == "text/html" and data:
            html += base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        if "parts" in part:
            child_plain, child_html = _extract_text_parts(part["parts"])
            plain += child_plain
            html += child_html
    if plain.strip():
        return plain
    if html.strip():
        return _strip_html(html)
    return ""


def _extract_text_parts(parts):
    plain = ""
    html = ""
    for part in parts:
        mime = part.get("mimeType", "")
        data = part.get("body", {}).get("data", "")
        if mime == "text/plain" and data:
            plain += base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        elif mime == "text/html" and data:
            html += base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        if "parts" in part:
            cp, ch = _extract_text_parts(part["parts"])
            plain += cp
            html += ch
    return plain, html


def authenticate():
    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
    except ImportError:
        print("  Gmail libs not installed. Run: pip install google-auth google-auth-oauthlib google-api-python-client")
        return None

    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"  Token refresh failed: {e}")
                creds = None

        if not creds:
            if not CREDS_PATH.exists():
                print(f"  credentials.json not found at {CREDS_PATH}")
                print("  Setup: https://console.cloud.google.com/apis/credentials")
                print("  Create OAuth 2.0 Client ID (Desktop app) -> Download JSON")
                print("  Save as credentials.json in project root, then run --auth")
                return None
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)

        TOKEN_PATH.write_text(creds.to_json())
        print(f"  Token saved to {TOKEN_PATH}")

    try:
        return build("gmail", "v1", credentials=creds)
    except Exception as e:
        print(f"  Gmail build failed: {e}")
        return None


def fetch_emails(service):
    try:
        results = service.users().messages().list(userId="me", q=SEARCH_QUERY, maxResults=10).execute()
        messages = results.get("messages", [])
        if not messages:
            return []

        emails = []
        for ref in messages:
            msg = service.users().messages().get(userId="me", id=ref["id"], format="full").execute()
            headers = {h["name"].lower(): h["value"] for h in msg.get("payload", {}).get("headers", [])}
            payload = msg.get("payload", {})
            body = ""
            if "parts" in payload:
                body = _extract_text(payload["parts"])
            elif payload.get("body", {}).get("data"):
                raw = base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")
                mime = payload.get("mimeType", "")
                body = _strip_html(raw) if "html" in mime else raw

            if body:
                emails.append({
                    "subject": headers.get("subject", ""),
                    "from": headers.get("from", ""),
                    "date": headers.get("date", ""),
                    "body": body,
                })
        return emails

    except Exception as e:
        print(f"  Gmail fetch error: {e}")
        return []


def main():
    print("  collect_grok_email: starting...")

    if "--auth" in sys.argv:
        print("  Running OAuth flow (one-time)...")
        service = authenticate()
        if service:
            r = service.users().messages().list(userId="me", maxResults=1).execute()
            print(f"  Connected! ~{r.get('resultSizeEstimate', 0)} messages in inbox")
        else:
            print("  Authentication failed")
        return

    service = authenticate()
    if not service:
        reason = "Gmail not configured. Run: python pipeline/collect_grok_email.py --auth"
        for f in ["grok_sentiment.json", "grok_news.json", "grok_macro.json"]:
            if not (OUTPUT_DIR / f).exists():
                _save(f, _make_fallback(reason))
        print(f"  {reason}")
        return

    emails = fetch_emails(service)
    print(f"  Found {len(emails)} email(s)")

    if not emails:
        for f in ["grok_sentiment.json", "grok_news.json", "grok_macro.json"]:
            _save(f, _make_fallback("Nenhum email do Grok encontrado hoje"))
        print("  No emails — fallback written")
        return

    saved = {}
    for email in emails:
        filename = _classify(email["subject"])
        category = filename.replace("grok_", "").replace(".json", "")
        if filename not in saved:
            _save(filename, _make_output(email["body"], email["subject"], email["date"], category))
            saved[filename] = email["subject"]
            print(f"    {filename}: {email['subject'][:60]}")

    print(f"  Saved {len(saved)} category(ies)")


if __name__ == "__main__":
    main()
