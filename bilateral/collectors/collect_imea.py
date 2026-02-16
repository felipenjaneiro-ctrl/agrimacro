#!/usr/bin/env python3
"""
collect_imea.py — AgriMacro Intelligence
Coleta dados do IMEA (Instituto Mato-grossense de Economia Agropecuária)
Fonte: https://publicacoes.imea.com.br

Dados extraídos dos Boletins Semanais (PDF):
  - Comercialização % (safra atual + futura) → Farmer Selling Pace
  - Preço MT (R$/sc)
  - B3 e CME-Group
  - Dólar PTAX
  - Estimativas de safra (área, produtividade, produção)

Estratégia: busca o boletim mais recente por número incremental,
baixa o PDF, extrai texto com pdfplumber, parseia com regex.

Principio: ZERO MOCK — apenas dados reais do IMEA
"""

import json
import logging
import os
import re
import sys
from datetime import datetime
from io import BytesIO
from pathlib import Path

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber required. Run: pip install pdfplumber")
    sys.exit(1)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

BASE_PUB_URL = "https://publicacoes.imea.com.br/relatorio-de-mercado"

# Report types and their URL slugs
REPORT_TYPES = {
    "bs_soja": {"slug": "bs-soja", "label": "Boletim Semanal Soja"},
    "bs_milho": {"slug": "bs-milho", "label": "Boletim Semanal Milho"},
}

# Known recent numbers (Oct 2025) as starting points for search
# Numbers increment roughly weekly, shared across report types
KNOWN_NUMBERS = {
    "bs_soja": 864,    # Sep 8, 2025
    "bs_milho": 869,   # Oct 13, 2025
}

BASE_DIR = Path(os.environ.get("AGRIMACRO_DATA_DIR", "data"))
OUTPUT_DIR = BASE_DIR / "imea"
CACHE_DIR = OUTPUT_DIR / "cache"
PDF_DIR = OUTPUT_DIR / "pdfs"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("collect_imea")


# ─────────────────────────────────────────────
# PDF DOWNLOAD & TEXT EXTRACTION
# ─────────────────────────────────────────────

def find_latest_number(slug, start_from, max_search=40):
    """
    Search for the latest boletim by trying numbers from start_from upward.
    Returns (number, pdf_url) or (None, None) if not found.
    """
    # First try going up from known number
    best_num = None
    best_url = None

    for n in range(start_from, start_from + max_search):
        url = f"{BASE_PUB_URL}/{slug}/{n}"
        try:
            resp = requests.head(url, timeout=10, allow_redirects=True, verify=False)
            if resp.status_code == 200:
                final_url = resp.url
                if ".pdf" in final_url or "s3" in final_url:
                    best_num = n
                    best_url = final_url
                else:
                    best_num = n
                    best_url = url
            elif resp.status_code == 404 and best_num is not None:
                # We've gone past the latest
                break
        except Exception:
            continue

    if best_num is None:
        # Try going down from known number
        for n in range(start_from - 1, start_from - 20, -1):
            url = f"{BASE_PUB_URL}/{slug}/{n}"
            try:
                resp = requests.head(url, timeout=10, allow_redirects=True, verify=False)
                if resp.status_code == 200:
                    best_num = n
                    best_url = resp.url if ".pdf" in resp.url or "s3" in resp.url else url
                    break
            except Exception:
                continue

    return best_num, best_url


def download_pdf(url, save_path=None, timeout=30):
    """Download PDF and return bytes."""
    logger.info(f"  Downloading PDF: {url[:80]}...")
    resp = requests.get(url, timeout=timeout, allow_redirects=True, verify=False)
    resp.raise_for_status()

    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(resp.content)

    logger.info(f"  {len(resp.content):,} bytes")
    return resp.content


def extract_text(pdf_bytes):
    """Extract all text from PDF using pdfplumber."""
    text = ""
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text


# ─────────────────────────────────────────────
# REGEX PARSERS — v2 (table-based, validated)
# ─────────────────────────────────────────────

def _extract_last_value(pattern, text, br_format=True):
    """Extract last numeric value from a table row pattern."""
    m = re.search(pattern, text, re.IGNORECASE)
    if m:
        vals = re.findall(r"([\d]+[,.][\d]+)", m.group(1))
        if vals:
            v = vals[-1]
            if br_format:
                return float(v.replace(".", "").replace(",", "."))
            return float(v.replace(",", "."))
    return None


def _extract_series(pattern, text, br_format=True):
    """Extract all numeric values from a table row pattern."""
    m = re.search(pattern, text, re.IGNORECASE)
    if m:
        vals = re.findall(r"([\d]+[,.][\d]+)", m.group(1))
        if vals:
            if br_format:
                return [float(v.replace(".", "").replace(",", ".")) for v in vals]
            return [float(v.replace(",", ".")) for v in vals]
    return None


def parse_boletim_soja(text):
    """Extract key metrics from soja boletim (table-based, high accuracy)."""
    data = {"commodity": "soja", "report_type": "boletim_semanal"}

    # ── HEADER ──
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\.?\s+(?:de\s+)?(\d{4})", text)
    if m:
        data["report_date"] = f"{m.group(1)} de {m.group(2)} {m.group(3)}"
    m = re.search(r"n[ºo°]\s*(\d+)", text)
    if m:
        data["boletim_number"] = int(m.group(1))

    # ── DAILY TABLE (weekly data, 5 values Mon-Fri) ──
    v = _extract_last_value(r"Soja Disponível\s+MT\s+R\$/sc\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text)
    if v: data["preco_mt_rs_sc"] = v
    s = _extract_series(r"Soja Disponível\s+MT\s+R\$/sc\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text)
    if s: data["preco_mt_serie"] = s

    v = _extract_last_value(r"contrato corrente\s+Chicago\s+US\$/bu\s+CME\s+([\d,.]+(?:\s+[\d,.]+)*)", text, br_format=False)
    if v: data["cme_corrente_usd_bu"] = v

    v = _extract_last_value(r"contrato\s+\w+/\d+\s+Chicago\s+US\$/bu\s+CME\s+([\d,.]+(?:\s+[\d,.]+)*)", text, br_format=False)
    if v: data["cme_futura_usd_bu"] = v

    v = _extract_last_value(r"Dólar Compra PTAX\s+Brasil\s+R\$/US\$\s+B3\s+([\d,.]+(?:\s+[\d,.]+)*)", text, br_format=False)
    if v: data["dolar_ptax"] = v

    v = _extract_last_value(r"Paridade Exportação.*?MT\s+R\$/sc\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text)
    if v: data["paridade_exp_rs_sc"] = v

    v = _extract_last_value(r"Prêmio portuário.*?Santos.*?Esalq\s+([\d,.]+(?:\s+[\d,.]+)*)", text, br_format=False)
    if v: data["premio_santos_cus_bu"] = v

    v = _extract_last_value(r"Cepea\s+Brasil\s+R\$/sc\s+Cepea\s+([\d,.]+(?:\s+[\d,.]+)*)", text)
    if v: data["cepea_rs_sc"] = v

    # Diferencial de base (may have negatives)
    m = re.search(r"Diferencial de base.*?MT\s+R\$/sc\s+Imea\s+([-\d,.]+(?:\s+[-\d,.]+)*)", text)
    if m:
        vals = re.findall(r"(-?[\d]+[,.][\d]+)", m.group(1))
        if vals:
            data["dif_base_mt_cme_rs_sc"] = float(vals[-1].replace(",", "."))

    # ── WEEKLY TABLE ──
    v = _extract_last_value(r"Frete Grãos Sorriso.*?R\$/t\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text)
    if v: data["frete_sorriso_miritituba_rs_t"] = v

    v = _extract_last_value(r"Margem Bruta.*?Esmagamento.*?R\$/t\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text)
    if v: data["margem_esmagamento_rs_t"] = v

    v = _extract_last_value(r"Farelo de Soja\s+MT\s+R\$/t\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text)
    if v: data["farelo_mt_rs_t"] = v

    v = _extract_last_value(r"Óleo de Soja\s+MT\s+R\$/t\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text)
    if v: data["oleo_mt_rs_t"] = v

    # ── MONTHLY TABLE ──

    # Comercialização safra atual + futura
    com_matches = list(re.finditer(
        r"Comercialização\s+(\d{2}/\d{2})\s+MT\s+%\s+Imea\s+([\d%,.]+(?:\s+[\d%,.]+)*)", text))
    if len(com_matches) >= 1:
        safra = com_matches[0].group(1)
        vals = re.findall(r"([\d]+[,.][\d]+)%", com_matches[0].group(2))
        if vals:
            data["comercializacao_safra_atual"] = {
                "safra": safra, "pct": float(vals[-1].replace(",", ".")),
                "serie": [float(v.replace(",", ".")) for v in vals]
            }
    if len(com_matches) >= 2:
        safra = com_matches[1].group(1)
        vals = re.findall(r"([\d]+[,.][\d]+)%", com_matches[1].group(2))
        if vals:
            data["comercializacao_safra_futura"] = {
                "safra": safra, "pct": float(vals[-1].replace(",", ".")),
                "serie": [float(v.replace(",", ".")) for v in vals]
            }

    # Exportações MT (monthly series)
    s = _extract_series(r"Exportações de Soja grão\s+MT\s+mi\s+t\s+Secex\s+([\d,.-]+(?:\s+[\d,.-]+)*)", text, br_format=False)
    if s: data["exportacoes_mt_mi_t_serie"] = s

    # Estimativas safra
    v = _extract_last_value(r"Estimativa\s+Produ[çc][ãa]o\s+\d{2}/\d{2}\s+MT\s+mi\s+t\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text, br_format=False)
    if v: data["estimativa_producao_mi_t"] = v

    v = _extract_last_value(r"Estimativa\s+[ÁA]rea\s+\d{2}/\d{2}\s+MT\s+mi\s+ha\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text, br_format=False)
    if v: data["estimativa_area_mi_ha"] = v

    v = _extract_last_value(r"Estimativa\s+Produtividade\s+\d{2}/\d{2}\s+MT\s+sc/ha\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text, br_format=False)
    if v: data["estimativa_produtividade_sc_ha"] = v

    v = _extract_last_value(r"Estimativa\s+Custo\s+Insumos\s+\d{2}/\d{2}\s+MT\s+R\$/ha\s+Imea\s+([\d,.]+(?:\s+[\d,.-]+)*)", text)
    if v: data["estimativa_custo_insumos_rs_ha"] = v

    v = _extract_last_value(r"Estimativa\s+Custeio\s+\d{2}/\d{2}\s+MT\s+R\$/ha\s+Imea\s+([\d,.]+(?:\s+[\d,.-]+)*)", text)
    if v: data["estimativa_custeio_rs_ha"] = v

    # ── FALLBACK: narrative extraction if tables not found ──
    if "preco_mt_rs_sc" not in data:
        m = re.search(r"R\$\s*([\d,.]+)/sc\s*\n?\s*(?:Indicador|INDICADOR)", text)
        if m:
            data["preco_mt_rs_sc"] = float(m.group(1).replace(".", "").replace(",", "."))
    if "dolar_ptax" not in data:
        m = re.search(r"R\$\s*([\d,.]+)/US\$", text)
        if m:
            data["dolar_ptax"] = float(m.group(1).replace(".", "").replace(",", "."))
    if "comercializacao_safra_atual" not in data:
        m = re.search(r"comercializa[çc][ãa]o\s+(?:da\s+)?soja\s+(?:da\s+)?safra\s*(\d{2}/\d{2}).*?(\d{1,3}[,.]\d+)%",
                       text, re.IGNORECASE | re.DOTALL)
        if m:
            data["comercializacao_safra_atual"] = {"safra": m.group(1), "pct": float(m.group(2).replace(",", "."))}

    return data


def parse_boletim_milho(text):
    """Extract key metrics from milho boletim (hybrid: highlight boxes + tables)."""
    data = {"commodity": "milho", "report_type": "boletim_semanal"}

    # ── HEADER ──
    m = re.search(r"(\d{1,2})\s+de\s+(\w+)\.?\s+(?:de\s+)?(\d{4})", text)
    if m:
        data["report_date"] = f"{m.group(1)} de {m.group(2)} {m.group(3)}"
    m = re.search(r"n[ºo°]\s*(\d+)", text)
    if m:
        data["boletim_number"] = int(m.group(1))

    # ── HIGHLIGHT BOXES ──
    m = re.search(r"R\$\s*([\d,.]+)/sc\s*\n\s*INDICADOR", text)
    if m:
        data["preco_mt_rs_sc"] = float(m.group(1).replace(".", "").replace(",", "."))
    m = re.search(r"R\$\s*([\d,.]+)/sc\s*\n\s*B3\s*CORRENTE", text)
    if m:
        data["b3_rs_sc"] = float(m.group(1).replace(".", "").replace(",", "."))
    m = re.search(r"US\$\s*([\d,.]+)/bu\s*\n\s*CME", text)
    if m:
        data["cme_usd_bu"] = float(m.group(1).replace(",", "."))
    m = re.search(r"R\$\s*([\d,.]+)/US\$", text)
    if m:
        data["dolar_ptax"] = float(m.group(1).replace(".", "").replace(",", "."))
    m = re.search(r"R\$\s*(-?[\d,.]+)/sc\s*\n\s*DIF", text)
    if m:
        data["dif_base_mt_cme_rs_sc"] = float(m.group(1).replace(",", "."))
    m = re.search(r"Paridade.*?\n.*?R\$\s*([\d,.]+)/sc", text)
    if m:
        data["paridade_exp_rs_sc"] = float(m.group(1).replace(".", "").replace(",", "."))

    # ── TABLE-BASED (same pattern as soja, if tables present) ──
    v = _extract_last_value(r"Milho Disponível\s+MT\s+R\$/sc\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text)
    if v: data["preco_mt_rs_sc"] = v  # override highlight with table

    com_matches = list(re.finditer(
        r"Comercialização\s+(\d{2}/\d{2})\s+MT\s+%\s+Imea\s+([\d%,.]+(?:\s+[\d%,.]+)*)", text))
    if len(com_matches) >= 1:
        safra = com_matches[0].group(1)
        vals = re.findall(r"([\d]+[,.][\d]+)%", com_matches[0].group(2))
        if vals:
            data["comercializacao_safra_atual"] = {
                "safra": safra, "pct": float(vals[-1].replace(",", ".")),
                "serie": [float(v.replace(",", ".")) for v in vals]
            }
    if len(com_matches) >= 2:
        safra = com_matches[1].group(1)
        vals = re.findall(r"([\d]+[,.][\d]+)%", com_matches[1].group(2))
        if vals:
            data["comercializacao_safra_futura"] = {
                "safra": safra, "pct": float(vals[-1].replace(",", ".")),
                "serie": [float(v.replace(",", ".")) for v in vals]
            }

    # Estimativas
    v = _extract_last_value(r"Estimativa\s+Produ[çc][ãa]o\s+\d{2}/\d{2}\s+MT\s+mi\s+t\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text, br_format=False)
    if v: data["estimativa_producao_mi_t"] = v

    v = _extract_last_value(r"Estimativa\s+[ÁA]rea\s+\d{2}/\d{2}\s+MT\s+mi\s+ha\s+Imea\s+([\d,.]+(?:\s+[\d,.]+)*)", text, br_format=False)
    if v: data["estimativa_area_mi_ha"] = v

    # ── NARRATIVE FALLBACK for comercialização ──
    if "comercializacao_safra_atual" not in data:
        m = re.search(r"comercializa[çc][ãa]o\s+(?:de\s+)?milho\s+(?:da\s+)?safra\s*\n?\s*(\d{2}/\d{2}).*?(\d{1,3}[,.]\d+)%",
                       text, re.IGNORECASE | re.DOTALL)
        if m:
            data["comercializacao_safra_atual"] = {"safra": m.group(1), "pct": float(m.group(2).replace(",", "."))}
    if "comercializacao_safra_futura" not in data:
        # Look for second safra reference with "alcançou"
        all_safras = re.findall(r"safra\s*\n?\s*(\d{2}/\d{2})", text, re.IGNORECASE)
        if len(all_safras) >= 2:
            future_safra = all_safras[1]
            m = re.search(rf"safra\s*\n?\s*{re.escape(future_safra)}.*?(\d{{1,3}}[,.]\d+)%",
                          text, re.IGNORECASE | re.DOTALL)
            if m:
                data["comercializacao_safra_futura"] = {"safra": future_safra, "pct": float(m.group(1).replace(",", "."))}

    return data


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def save_json(data, filepath):
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"Saved: {filepath}")


def load_cache():
    f = CACHE_DIR / "imea_latest.json"
    if f.exists():
        try:
            return json.load(open(f, encoding="utf-8"))
        except Exception:
            pass
    return {}


def collect(report_types=None, save_pdfs=True):
    now = datetime.now()
    ts = now.isoformat()

    if report_types is None:
        report_types = list(REPORT_TYPES.keys())

    parsers = {
        "bs_soja": parse_boletim_soja,
        "bs_milho": parse_boletim_milho,
    }

    parsed = {}
    errors = {}

    for key in report_types:
        if key not in REPORT_TYPES:
            continue
        info = REPORT_TYPES[key]
        slug = info["slug"]
        label = info["label"]
        start = KNOWN_NUMBERS.get(key, 860)

        logger.info(f"Processing {key}: {label}")
        logger.info(f"  Searching for latest report (starting from #{start})...")

        try:
            num, pdf_url = find_latest_number(slug, start)
            if num is None:
                logger.warning(f"  Could not find any report for {slug}")
                errors[key] = "No report found"
                continue

            logger.info(f"  Found: #{num}")

            # Download PDF
            page_url = f"{BASE_PUB_URL}/{slug}/{num}"
            resp = requests.get(page_url, timeout=30, allow_redirects=True, verify=False)
            resp.raise_for_status()

            pdf_bytes = resp.content
            pdf_path = PDF_DIR / f"{slug}_{num}.pdf" if save_pdfs else None
            if pdf_path:
                pdf_path.parent.mkdir(parents=True, exist_ok=True)
                with open(pdf_path, "wb") as f:
                    f.write(pdf_bytes)
                logger.info(f"  Saved PDF: {pdf_path}")

            # Extract text
            text = extract_text(pdf_bytes)
            if not text or len(text) < 100:
                logger.warning(f"  Could not extract text from PDF (got {len(text)} chars)")
                errors[key] = "PDF text extraction failed"
                continue

            logger.info(f"  Extracted {len(text)} chars of text")

            # Parse
            data = parsers[key](text)
            data["source_url"] = page_url
            data["report_number"] = num
            parsed[key] = data

        except Exception as e:
            logger.error(f"  Error on {key}: {e}")
            errors[key] = str(e)

    if not parsed:
        cached = load_cache()
        if cached:
            cached["status"] = "cached"
            return cached
        return {"source": "imea", "status": "error", "errors": errors}

    output = {
        "source": "imea",
        "source_url": "https://publicacoes.imea.com.br",
        "collection_timestamp": ts,
        "status": "ok",
        "errors": errors if errors else None,
        "data": parsed,
    }

    timestamp = now.strftime("%Y%m%d_%H%M%S")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    save_json(output, OUTPUT_DIR / f"imea_{timestamp}.json")
    save_json(output, OUTPUT_DIR / "imea_latest.json")
    save_json(output, CACHE_DIR / "imea_latest.json")

    # Log summary
    logger.info("=" * 60)
    logger.info("IMEA - COLLECTION SUMMARY")
    logger.info("=" * 60)
    for key, data in parsed.items():
        label = REPORT_TYPES[key]["label"]
        logger.info(f"  {label} (#{data.get('report_number', '?')}):")
        if data.get("report_date"):
            logger.info(f"    Date: {data['report_date']}")
        if data.get("preco_mt_rs_sc"):
            logger.info(f"    Preço MT: R${data['preco_mt_rs_sc']}/sc")
        if data.get("cme_usd_bu"):
            logger.info(f"    CME: US${data['cme_usd_bu']}/bu")
        if data.get("dolar_ptax"):
            logger.info(f"    Dólar: R${data['dolar_ptax']}")
        if data.get("comercializacao_safra_atual"):
            c = data["comercializacao_safra_atual"]
            logger.info(f"    Comercialização {c['safra']}: {c['pct']}%")
        if data.get("comercializacao_safra_futura"):
            c = data["comercializacao_safra_futura"]
            logger.info(f"    Comercialização {c['safra']} (futura): {c['pct']}%")
        if data.get("estimativa_producao_mi_t"):
            logger.info(f"    Produção estimada: {data['estimativa_producao_mi_t']} mi t")
    if errors:
        for key, err in errors.items():
            logger.warning(f"  ERROR {key}: {err}")
    logger.info("=" * 60)

    return output


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="AgriMacro - IMEA Collector")
    parser.add_argument("--reports", nargs="+", default=None,
                        choices=list(REPORT_TYPES.keys()))
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    if args.output_dir:
        OUTPUT_DIR = Path(args.output_dir)
        CACHE_DIR = OUTPUT_DIR / "cache"
        PDF_DIR = OUTPUT_DIR / "pdfs"

    result = collect(report_types=args.reports)
    sys.exit(0 if result["status"] in ("ok", "cached") else 1)
