"""
=============================================================================
AgriMacro v3.2 — Coletor: Acucar & Alcool (Brasil)
=============================================================================
Fontes:
  1. CEPEA/ESALQ  → Acucar Cristal, Etanol Hidratado, Etanol Anidro
  2. ANP           → Precos combustiveis na bomba (etanol, gasolina)
  3. UNICA         → Producao safra (moagem, mix acucar/etanol)
  4. CONSECANA     → ATR (preco da cana para o fornecedor)

Output: sugar_alcohol_br.json  (em DATA_PROC)
        Atualiza physical_intl.json com CEPEA acucar + etanol

Uso:
  python collect_sugar_alcohol_br.py

Agendamento (n8n ou Task Scheduler):
  Rodar 1x por dia util, apos 18h (CEPEA atualiza ~17h)
=============================================================================
"""

import json, os, sys, re, traceback
from datetime import datetime, timedelta
from pathlib import Path

# === CAMINHOS (mesma estrutura do pipeline AgriMacro) ===
PROJ_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PROC  = os.path.join(PROJ_DIR, "agrimacro-dash", "public", "data", "processed")
DATA_RAW   = os.path.join(PROJ_DIR, "agrimacro-dash", "public", "data", "raw")
TODAY      = datetime.now().strftime("%Y-%m-%d")

# Se rodar de dentro de pipeline/, ajusta o caminho
if not os.path.exists(DATA_PROC):
    # Tenta caminho relativo do pipeline
    alt = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                       "..", "agrimacro-dash", "public", "data", "processed")
    if os.path.exists(alt):
        DATA_PROC = os.path.abspath(alt)
        DATA_RAW = os.path.abspath(alt.replace("processed", "raw"))
    else:
        print(f"[AVISO] Pasta de dados nao encontrada: {DATA_PROC}")
        print("  Criando pasta...")
        os.makedirs(DATA_PROC, exist_ok=True)
        os.makedirs(DATA_RAW, exist_ok=True)

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("="*60)
    print("ERRO: Bibliotecas necessarias nao instaladas!")
    print("Rode no PowerShell:")
    print("  pip install requests beautifulsoup4 lxml")
    print("="*60)
    sys.exit(1)

# =========================================================================
# HELPERS
# =========================================================================
def load_json(path):
    """Carrega JSON com fallback de encoding"""
    if not os.path.exists(path):
        return {}
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            with open(path, "r", encoding=enc) as f:
                return json.load(f)
        except:
            continue
    return {}

def save_json(path, data):
    """Salva JSON com encoding utf-8"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    print(f"  [SALVO] {os.path.basename(path)}")

def safe_float(val, default=None):
    """Converte para float com seguranca"""
    if val is None:
        return default
    try:
        # Remove R$, espaços, troca vírgula por ponto
        cleaned = str(val).replace("R$", "").replace(" ", "").replace(",", ".")
        return float(cleaned)
    except:
        return default

def get_page(url, timeout=30):
    """Faz GET com headers de browser"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8",
    }
    resp = requests.get(url, headers=headers, timeout=timeout, verify=True)
    resp.raise_for_status()
    return resp

# =========================================================================
# 1. CEPEA/ESALQ — Acucar Cristal + Etanol Hidratado + Etanol Anidro
# =========================================================================
def collect_cepea_sugar_ethanol():
    """
    Coleta indicadores CEPEA/ESALQ:
    - Acucar Cristal: cepea.esalq.usp.br/br/indicador/acucar.aspx
    - Etanol Hidratado: cepea.esalq.usp.br/br/indicador/etanol.aspx
    - Etanol Anidro: mesma pagina do etanol
    
    Retorna dict com dados coletados.
    """
    print("\n[1/4] CEPEA/ESALQ — Acucar + Etanol...")
    result = {
        "acucar_cristal": {"price": None, "unit": "R$/saca 50kg", "change_pct": None, 
                           "date": None, "source": "CEPEA/ESALQ", "history": []},
        "etanol_hidratado": {"price": None, "unit": "R$/litro", "change_pct": None, 
                             "date": None, "source": "CEPEA/ESALQ", "history": []},
        "etanol_anidro": {"price": None, "unit": "R$/litro", "change_pct": None, 
                          "date": None, "source": "CEPEA/ESALQ", "history": []},
    }
    
    # --- Acucar Cristal ---
    try:
        print("  Coletando Acucar Cristal...")
        url = "https://cepea.esalq.usp.br/br/indicador/acucar.aspx"
        resp = get_page(url)
        soup = BeautifulSoup(resp.text, "lxml")
        
        # CEPEA usa tabela com classe "imagenet-table" ou "responsive"
        # Tenta encontrar a tabela de indicadores
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 4:
                    # Formato tipico: Data | Preco a vista R$ | Var dia | Var mes
                    date_text = cells[0].get_text(strip=True)
                    price_text = cells[1].get_text(strip=True)
                    var_text = cells[2].get_text(strip=True)
                    
                    price_val = safe_float(price_text)
                    if price_val and price_val > 10 and price_val < 500:  # Sanity check acucar
                        result["acucar_cristal"]["price"] = price_val
                        result["acucar_cristal"]["date"] = date_text
                        result["acucar_cristal"]["change_pct"] = var_text
                        result["acucar_cristal"]["history"].append({
                            "date": date_text, "value": price_val
                        })
                        print(f"    Acucar Cristal: R$ {price_val:.2f}/sc 50kg ({var_text})")
                        break
            if result["acucar_cristal"]["price"]:
                break
        
        if not result["acucar_cristal"]["price"]:
            # Fallback: tenta scraping alternativo via indicador-data
            alt_url = "https://cepea.esalq.usp.br/br/indicador/series/acucar.aspx?id=53"
            print(f"    Tentando URL alternativa: {alt_url}")
            try:
                resp2 = get_page(alt_url)
                soup2 = BeautifulSoup(resp2.text, "lxml")
                # Procura span ou div com classe de valor
                valor_divs = soup2.find_all(class_=re.compile(r"val|price|indicador", re.I))
                for vd in valor_divs:
                    txt = vd.get_text(strip=True)
                    val = safe_float(txt)
                    if val and 10 < val < 500:
                        result["acucar_cristal"]["price"] = val
                        result["acucar_cristal"]["date"] = TODAY
                        print(f"    Acucar Cristal (alt): R$ {val:.2f}/sc 50kg")
                        break
            except Exception as e:
                print(f"    [WARN] Fallback acucar falhou: {e}")
                
    except Exception as e:
        print(f"  [ERRO] Acucar Cristal: {e}")
    
    # --- Etanol Hidratado + Anidro ---
    try:
        print("  Coletando Etanol Hidratado + Anidro...")
        url = "https://cepea.esalq.usp.br/br/indicador/etanol.aspx"
        resp = get_page(url)
        soup = BeautifulSoup(resp.text, "lxml")
        
        tables = soup.find_all("table")
        found_hidratado = False
        found_anidro = False
        
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                text_full = row.get_text(strip=True).lower()
                
                if len(cells) >= 3:
                    # Procura "hidratado" ou "anidro" no contexto
                    for i, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True)
                        val = safe_float(cell_text)
                        
                        if val and 0.5 < val < 10:  # Range razoavel para etanol R$/litro
                            if "hidratado" in text_full and not found_hidratado:
                                result["etanol_hidratado"]["price"] = val
                                result["etanol_hidratado"]["date"] = TODAY
                                found_hidratado = True
                                print(f"    Etanol Hidratado: R$ {val:.4f}/litro")
                            elif "anidro" in text_full and not found_anidro:
                                result["etanol_anidro"]["price"] = val
                                result["etanol_anidro"]["date"] = TODAY
                                found_anidro = True
                                print(f"    Etanol Anidro: R$ {val:.4f}/litro")
                            elif not found_hidratado and not found_anidro:
                                # Primeiro valor encontrado = hidratado (mais comum)
                                result["etanol_hidratado"]["price"] = val
                                result["etanol_hidratado"]["date"] = TODAY
                                found_hidratado = True
                                print(f"    Etanol (primeiro valor): R$ {val:.4f}/litro")
                                
    except Exception as e:
        print(f"  [ERRO] Etanol CEPEA: {e}")
    
    return result


# =========================================================================
# 2. ANP — Precos de Combustiveis na Bomba
# =========================================================================
def collect_anp_fuel():
    """
    Coleta precos semanais de combustiveis da ANP via dados.gov.br
    Foco: etanol hidratado e gasolina C (para calcular paridade 70%)
    
    API: https://dados.gov.br/dados/conjuntos-dados/serie-historica-de-precos-de-combustiveis
    Alternativa: CSV direto do site ANP
    """
    print("\n[2/4] ANP — Precos Combustiveis na Bomba...")
    result = {
        "etanol_bomba": {"preco_medio": None, "preco_min": None, "preco_max": None,
                         "estado": "SP", "unit": "R$/litro", "date": None},
        "gasolina_bomba": {"preco_medio": None, "preco_min": None, "preco_max": None,
                           "estado": "SP", "unit": "R$/litro", "date": None},
        "paridade_etanol_gasolina": None,  # < 0.70 = etanol compensa
        "source": "ANP - Levantamento Semanal",
    }
    
    try:
        # Tenta API dados.gov.br (formato CKAN)
        # URL do dataset: serie-historica-de-precos-de-combustiveis-e-de-glp
        api_url = "https://dados.gov.br/dados/api/publico/conjuntos-dados/serie-historica-de-precos-de-combustiveis-e-de-glp"
        
        print("  Tentando API dados.gov.br...")
        try:
            resp = get_page(api_url)
            data = resp.json()
            # Procura o recurso mais recente (CSV)
            resources = data.get("resources", data.get("result", {}).get("resources", []))
            csv_url = None
            for r in resources:
                name = r.get("name", "").lower() + r.get("description", "").lower()
                url = r.get("url", "")
                if url.endswith(".csv") and ("2026" in name or "2025" in name or "semana" in name):
                    csv_url = url
                    break
            if not csv_url and resources:
                # Pega o mais recente
                csv_url = resources[-1].get("url", "")
                
            if csv_url:
                print(f"    Baixando CSV: {csv_url[:80]}...")
                csv_resp = get_page(csv_url, timeout=60)
                lines = csv_resp.text.split("\n")
                
                # Parse CSV ANP (formato: regiao;estado;municipio;revenda;cnpj;produto;...)
                header = lines[0].lower().split(";") if ";" in lines[0] else lines[0].lower().split(",")
                sep = ";" if ";" in lines[0] else ","
                
                # Procura colunas relevantes
                idx_produto = next((i for i,h in enumerate(header) if "produto" in h), None)
                idx_preco = next((i for i,h in enumerate(header) if "preco" in h and "venda" in h), None)
                idx_estado = next((i for i,h in enumerate(header) if "estado" in h), None)
                
                if idx_produto is not None and idx_preco is not None:
                    etanol_precos = []
                    gasolina_precos = []
                    
                    for line in lines[-5000:]:  # Ultimas 5000 linhas (dados recentes)
                        cols = line.split(sep)
                        if len(cols) <= max(idx_produto, idx_preco):
                            continue
                        produto = cols[idx_produto].lower().strip()
                        preco = safe_float(cols[idx_preco])
                        estado = cols[idx_estado].strip() if idx_estado else ""
                        
                        if preco and estado.upper() == "SP":
                            if "etanol" in produto or "alcool" in produto:
                                etanol_precos.append(preco)
                            elif "gasolina" in produto and "aviacao" not in produto:
                                gasolina_precos.append(preco)
                    
                    if etanol_precos:
                        result["etanol_bomba"]["preco_medio"] = round(sum(etanol_precos)/len(etanol_precos), 3)
                        result["etanol_bomba"]["preco_min"] = round(min(etanol_precos), 3)
                        result["etanol_bomba"]["preco_max"] = round(max(etanol_precos), 3)
                        result["etanol_bomba"]["date"] = TODAY
                        print(f"    Etanol Bomba SP: R$ {result['etanol_bomba']['preco_medio']:.3f}/L")
                    
                    if gasolina_precos:
                        result["gasolina_bomba"]["preco_medio"] = round(sum(gasolina_precos)/len(gasolina_precos), 3)
                        result["gasolina_bomba"]["preco_min"] = round(min(gasolina_precos), 3)
                        result["gasolina_bomba"]["preco_max"] = round(max(gasolina_precos), 3)
                        result["gasolina_bomba"]["date"] = TODAY
                        print(f"    Gasolina Bomba SP: R$ {result['gasolina_bomba']['preco_medio']:.3f}/L")
                    
                    # Calcula paridade
                    if result["etanol_bomba"]["preco_medio"] and result["gasolina_bomba"]["preco_medio"]:
                        par = result["etanol_bomba"]["preco_medio"] / result["gasolina_bomba"]["preco_medio"]
                        result["paridade_etanol_gasolina"] = round(par, 3)
                        compensa = "ETANOL COMPENSA" if par < 0.70 else "GASOLINA COMPENSA"
                        print(f"    Paridade: {par:.1%} ({compensa})")
                        
        except Exception as e:
            print(f"    [WARN] API dados.gov.br falhou: {e}")
            
    except Exception as e:
        print(f"  [ERRO] ANP: {e}")
    
    # Fallback: se nao conseguiu via API, tenta scraping do site ANP
    if not result["etanol_bomba"]["preco_medio"]:
        try:
            print("  Tentando scraping direto ANP...")
            url = "https://precos.anp.gov.br/include/Resumo_Semanal_Index.asp"
            resp = get_page(url)
            soup = BeautifulSoup(resp.text, "lxml")
            
            tables = soup.find_all("table")
            for table in tables:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    text = row.get_text().lower()
                    if "etanol" in text and len(cells) >= 3:
                        for cell in cells:
                            val = safe_float(cell.get_text())
                            if val and 2.0 < val < 8.0:
                                result["etanol_bomba"]["preco_medio"] = val
                                result["etanol_bomba"]["date"] = TODAY
                                print(f"    Etanol Bomba (scraping): R$ {val:.3f}/L")
                                break
                    if "gasolina" in text and "comum" in text and len(cells) >= 3:
                        for cell in cells:
                            val = safe_float(cell.get_text())
                            if val and 4.0 < val < 10.0:
                                result["gasolina_bomba"]["preco_medio"] = val
                                result["gasolina_bomba"]["date"] = TODAY
                                print(f"    Gasolina Bomba (scraping): R$ {val:.3f}/L")
                                break
        except Exception as e:
            print(f"    [WARN] Scraping ANP falhou: {e}")
    
    return result


# =========================================================================
# 3. UNICA — Producao Safra (Moagem, Mix Acucar/Etanol)
# =========================================================================
def collect_unica():
    """
    Coleta dados de producao da UNICA (Unicadata)
    - Moagem de cana
    - Producao de acucar
    - Producao de etanol (hidratado + anidro)
    - Mix acucar vs etanol
    
    Dados quinzenais durante safra (abr-nov), consolidados na entressafra.
    """
    print("\n[3/4] UNICA — Producao Safra...")
    result = {
        "moagem_cana_mil_ton": None,
        "producao_acucar_mil_ton": None,
        "producao_etanol_total_mil_m3": None,
        "producao_etanol_hidratado_mil_m3": None,
        "producao_etanol_anidro_mil_m3": None,
        "mix_acucar_pct": None,  # % da cana que vira acucar
        "mix_etanol_pct": None,  # % da cana que vira etanol
        "safra": "2025/2026",
        "periodo": None,
        "source": "UNICA/Unicadata",
        "date": TODAY,
        "nota": "Dados quinzenais durante safra (abr-nov). Fora da safra, ultimo consolidado."
    }
    
    try:
        # Tenta acessar Unicadata
        url = "https://unicadata.com.br/listagem.php?idMn=31"
        print(f"  Tentando Unicadata: {url}")
        
        resp = get_page(url)
        soup = BeautifulSoup(resp.text, "lxml")
        
        # Unicadata tem tabelas com dados de producao
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 2:
                    label = cells[0].get_text(strip=True).lower()
                    value_text = cells[-1].get_text(strip=True)
                    val = safe_float(value_text)
                    
                    if "moagem" in label and val:
                        result["moagem_cana_mil_ton"] = val
                        print(f"    Moagem: {val:,.0f} mil ton")
                    elif "acucar" in label and "produ" in label and val:
                        result["producao_acucar_mil_ton"] = val
                        print(f"    Producao Acucar: {val:,.0f} mil ton")
                    elif "etanol" in label and "total" in label and val:
                        result["producao_etanol_total_mil_m3"] = val
                        print(f"    Producao Etanol Total: {val:,.0f} mil m3")
                    elif "hidratado" in label and val:
                        result["producao_etanol_hidratado_mil_m3"] = val
                    elif "anidro" in label and val:
                        result["producao_etanol_anidro_mil_m3"] = val
        
        # Calcula mix se tiver dados
        if result["producao_acucar_mil_ton"] and result["moagem_cana_mil_ton"]:
            # Mix aproximado: acucar usa ~47% do ATR tipicamente
            total_prod = (result["producao_acucar_mil_ton"] or 0) + \
                        (result["producao_etanol_total_mil_m3"] or 0) * 0.85  # conversao aprox
            if total_prod > 0:
                result["mix_acucar_pct"] = round(
                    (result["producao_acucar_mil_ton"] / total_prod) * 100, 1
                )
                result["mix_etanol_pct"] = round(100 - result["mix_acucar_pct"], 1)
                print(f"    Mix: {result['mix_acucar_pct']}% acucar / {result['mix_etanol_pct']}% etanol")
                
    except Exception as e:
        print(f"  [WARN] UNICA: {e}")
        print("  NOTA: Unicadata pode exigir navegacao interativa.")
        print("  Alternativa: baixar CSV manualmente de unicadata.com.br")
    
    return result


# =========================================================================
# 4. CONSECANA — ATR (Preco da Cana)
# =========================================================================
def collect_consecana():
    """
    Coleta dados CONSECANA-SP:
    - Preco do kg de ATR
    - ATR medio da cana (kg ATR / ton cana)
    - Preco da tonelada de cana
    
    Dados mensais. CONSECANA nao tem API publica — 
    scraping do site ou entrada manual.
    """
    print("\n[4/4] CONSECANA — ATR e Preco da Cana...")
    result = {
        "preco_atr_rs_kg": None,    # R$/kg ATR
        "atr_medio_kg_ton": None,   # kg ATR por tonelada de cana
        "preco_ton_cana": None,     # R$/tonelada de cana
        "mes_referencia": None,
        "safra": "2025/2026",
        "source": "CONSECANA-SP",
        "date": TODAY,
        "nota": "Atualizado mensalmente. ATR define pagamento ao fornecedor de cana."
    }
    
    try:
        url = "https://www.consecana.com.br"
        print(f"  Tentando CONSECANA: {url}")
        
        resp = get_page(url)
        soup = BeautifulSoup(resp.text, "lxml")
        
        # Procura valores de ATR na pagina
        text = soup.get_text()
        
        # Procura padrao "R$ X,XXXX" ou "0,XXXX" proximo de "ATR"
        atr_matches = re.findall(
            r'ATR[^0-9]*?(\d+[,\.]\d{2,4})', 
            text, re.IGNORECASE
        )
        if atr_matches:
            val = safe_float(atr_matches[0])
            if val and val < 2:  # ATR eh ~R$ 0.90-1.50/kg
                result["preco_atr_rs_kg"] = val
                print(f"    ATR: R$ {val:.4f}/kg")
            elif val and 100 < val < 200:  # Pode ser ATR em kg/ton
                result["atr_medio_kg_ton"] = val
                print(f"    ATR medio: {val:.1f} kg/ton cana")
        
        # Procura preco da tonelada
        ton_matches = re.findall(
            r'(?:tonelada|ton)[^0-9]*?R?\$?\s*(\d{2,3}[,\.]\d{2})',
            text, re.IGNORECASE
        )
        if ton_matches:
            val = safe_float(ton_matches[0])
            if val and 50 < val < 300:
                result["preco_ton_cana"] = val
                print(f"    Tonelada cana: R$ {val:.2f}")
                
        # Se nao achou, tenta tabelas
        tables = soup.find_all("table")
        for table in tables:
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                text_row = row.get_text().lower()
                if "atr" in text_row and len(cells) >= 2:
                    for cell in cells:
                        val = safe_float(cell.get_text())
                        if val:
                            if val < 2:
                                result["preco_atr_rs_kg"] = val
                            elif 100 < val < 200:
                                result["atr_medio_kg_ton"] = val
                                
    except Exception as e:
        print(f"  [WARN] CONSECANA: {e}")
        print("  NOTA: Site CONSECANA pode estar indisponivel ou exigir JS.")
        print("  Alternativa: inserir dados manualmente no JSON.")
    
    # Valores tipicos de referencia (caso scraping falhe)
    if not result["preco_atr_rs_kg"] and not result["atr_medio_kg_ton"]:
        print("  [INFO] Usando valores tipicos de referencia (atualize manualmente):")
        result["preco_atr_rs_kg"] = None  # Nao inventamos dados (ZERO MOCK)
        result["atr_medio_kg_ton"] = None
        result["nota"] += " SCRAPING FALHOU — inserir manualmente."
        print("  Para inserir: edite sugar_alcohol_br.json > consecana > preco_atr_rs_kg")
    
    return result


# =========================================================================
# 5. CALCULOS E SPREADS
# =========================================================================
def calculate_spreads(cepea, anp, unica, consecana, pr_data):
    """
    Calcula spreads e margens do setor sucroalcooleiro:
    - Paridade etanol/gasolina na bomba
    - Paridade de exportacao acucar (NY vs interno)
    - Spread hidratado vs anidro
    - Margem simplificada da usina
    """
    print("\n[CALC] Calculando spreads e margens...")
    spreads = {}
    
    # --- 1. Paridade Etanol/Gasolina ---
    if anp["etanol_bomba"]["preco_medio"] and anp["gasolina_bomba"]["preco_medio"]:
        par = anp["paridade_etanol_gasolina"]
        spreads["paridade_bomba"] = {
            "valor": par,
            "interpretacao": "ETANOL COMPENSA" if par < 0.70 else "GASOLINA COMPENSA",
            "regra": "Se paridade < 70% = abastecer etanol. Se > 70% = gasolina.",
            "etanol_rs": anp["etanol_bomba"]["preco_medio"],
            "gasolina_rs": anp["gasolina_bomba"]["preco_medio"],
        }
        print(f"  Paridade bomba: {par:.1%} → {spreads['paridade_bomba']['interpretacao']}")
    
    # --- 2. Paridade Exportacao Acucar ---
    # Acucar NY (SB) em cents/lb → converte para R$/ton
    # 1 lb = 0.453592 kg → 1 ton = 2204.62 lbs
    # preco_NY_clb * 22.0462 = USD/ton → * dolar = R$/ton
    try:
        if pr_data:
            sb_hist = pr_data.get("SB", [])
            if sb_hist:
                sb_last = None
                for entry in reversed(sb_hist):
                    if entry.get("close"):
                        sb_last = float(entry["close"])
                        break
                
                # Carrega dolar do BCB data
                bcb_data = load_json(os.path.join(DATA_PROC, "bcb_data.json"))
                dolar = None
                if bcb_data:
                    dolar = bcb_data.get("dolar_hoje") or bcb_data.get("usd_brl")
                    if isinstance(dolar, dict):
                        dolar = dolar.get("value") or dolar.get("close")
                    dolar = safe_float(dolar)
                
                if sb_last and dolar:
                    # Converte: cents/lb → USD/ton → R$/ton
                    usd_per_ton = sb_last * 22.0462  # 22.0462 lbs por ton / 100 (cents→USD)
                    # Correcao: SB eh em cents/lb, entao sb_last/100 * 2204.62
                    usd_per_ton = (sb_last / 100) * 2204.62
                    rs_per_ton = usd_per_ton * dolar
                    
                    # Converte para R$/saca 50kg: 1 ton = 20 sacas de 50kg
                    rs_per_sc = rs_per_ton / 20
                    
                    spreads["paridade_exportacao"] = {
                        "ny_clb": sb_last,
                        "dolar": dolar,
                        "ny_rs_ton": round(rs_per_ton, 2),
                        "ny_rs_sc50": round(rs_per_sc, 2),
                        "cepea_rs_sc50": cepea["acucar_cristal"]["price"],
                        "interpretacao": None,
                    }
                    
                    if cepea["acucar_cristal"]["price"]:
                        diff = rs_per_sc - cepea["acucar_cristal"]["price"]
                        if diff > 0:
                            interp = f"Exportar paga R$ {diff:.2f}/sc a mais"
                        else:
                            interp = f"Mercado interno paga R$ {abs(diff):.2f}/sc a mais"
                        spreads["paridade_exportacao"]["interpretacao"] = interp
                        print(f"  Paridade export: NY={rs_per_sc:.2f} vs CEPEA={cepea['acucar_cristal']['price']:.2f} → {interp}")
                    
    except Exception as e:
        print(f"  [WARN] Calculo paridade export: {e}")
    
    # --- 3. Spread Hidratado vs Anidro ---
    if cepea["etanol_hidratado"]["price"] and cepea["etanol_anidro"]["price"]:
        diff = cepea["etanol_anidro"]["price"] - cepea["etanol_hidratado"]["price"]
        spreads["spread_anidro_hidratado"] = {
            "valor_rs": round(diff, 4),
            "anidro_rs": cepea["etanol_anidro"]["price"],
            "hidratado_rs": cepea["etanol_hidratado"]["price"],
            "interpretacao": "Anidro paga premio" if diff > 0 else "Hidratado paga mais",
        }
        print(f"  Spread anidro-hidratado: R$ {diff:+.4f}/L")
    
    # --- 4. Margem Usina (simplificada) ---
    if cepea["acucar_cristal"]["price"] and consecana.get("preco_ton_cana"):
        # Estimativa: 1 ton cana → ~120kg acucar → 2.4 sacas 50kg
        receita_acucar = cepea["acucar_cristal"]["price"] * 2.4
        custo_cana = consecana["preco_ton_cana"]
        margem = receita_acucar - custo_cana
        spreads["margem_usina_acucar"] = {
            "receita_rs_ton": round(receita_acucar, 2),
            "custo_cana_rs_ton": custo_cana,
            "margem_rs_ton": round(margem, 2),
            "interpretacao": "POSITIVA" if margem > 0 else "NEGATIVA",
            "nota": "Estimativa simplificada. Nao inclui custos operacionais."
        }
        print(f"  Margem usina (acucar): R$ {margem:+.2f}/ton cana")
    
    return spreads


# =========================================================================
# 6. INTEGRACAO — Atualiza physical_intl.json
# =========================================================================
def update_physical_intl(cepea):
    """
    Atualiza physical_intl.json com dados CEPEA de acucar e etanol
    para que pg_sugar_alcohol() e pg_physical() mostrem dados reais.
    """
    print("\n[INTEG] Atualizando physical_intl.json...")
    phys_path = os.path.join(DATA_PROC, "physical_intl.json")
    phys = load_json(phys_path)
    
    if "international" not in phys:
        phys["international"] = {}
    
    intl = phys["international"]
    
    # Acucar Cristal Brasil
    if cepea["acucar_cristal"]["price"]:
        key = "SB_BR"
        old_hist = intl.get(key, {}).get("history", [])
        
        # Adiciona ao historico (evita duplicata do mesmo dia)
        new_entry = {"date": cepea["acucar_cristal"]["date"] or TODAY, 
                     "value": cepea["acucar_cristal"]["price"]}
        if not old_hist or old_hist[-1].get("date") != new_entry["date"]:
            old_hist.append(new_entry)
        # Mantem ultimos 60 dias
        old_hist = old_hist[-60:]
        
        # Calcula variacao
        trend = ""
        if cepea["acucar_cristal"]["change_pct"]:
            trend = cepea["acucar_cristal"]["change_pct"]
        elif len(old_hist) >= 2:
            prev = old_hist[-2]["value"]
            curr = old_hist[-1]["value"]
            if prev and prev > 0:
                pct = ((curr - prev) / prev) * 100
                trend = f"{pct:+.1f}% d/d"
        
        intl[key] = {
            "label": "Acucar Cristal",
            "price": cepea["acucar_cristal"]["price"],
            "price_unit": "R$/saca 50kg",
            "trend": trend,
            "source": "CEPEA/ESALQ",
            "period": cepea["acucar_cristal"]["date"] or TODAY,
            "history": old_hist,
        }
        print(f"  SB_BR: R$ {cepea['acucar_cristal']['price']:.2f}/sc ({trend})")
    
    # Etanol Hidratado Brasil
    if cepea["etanol_hidratado"]["price"]:
        key = "ETH_BR"
        old_hist = intl.get(key, {}).get("history", [])
        new_entry = {"date": TODAY, "value": cepea["etanol_hidratado"]["price"]}
        if not old_hist or old_hist[-1].get("date") != new_entry["date"]:
            old_hist.append(new_entry)
        old_hist = old_hist[-60:]
        
        trend = ""
        if len(old_hist) >= 2:
            prev = old_hist[-2]["value"]
            curr = old_hist[-1]["value"]
            if prev and prev > 0:
                pct = ((curr - prev) / prev) * 100
                trend = f"{pct:+.1f}% d/d"
        
        intl[key] = {
            "label": "Etanol Hidratado",
            "price": cepea["etanol_hidratado"]["price"],
            "price_unit": "R$/litro",
            "trend": trend,
            "source": "CEPEA/ESALQ",
            "period": TODAY,
            "history": old_hist,
        }
        print(f"  ETH_BR: R$ {cepea['etanol_hidratado']['price']:.4f}/L ({trend})")
    
    # Etanol Anidro Brasil
    if cepea["etanol_anidro"]["price"]:
        key = "ETN_BR"
        old_hist = intl.get(key, {}).get("history", [])
        new_entry = {"date": TODAY, "value": cepea["etanol_anidro"]["price"]}
        if not old_hist or old_hist[-1].get("date") != new_entry["date"]:
            old_hist.append(new_entry)
        old_hist = old_hist[-60:]
        
        intl[key] = {
            "label": "Etanol Anidro",
            "price": cepea["etanol_anidro"]["price"],
            "price_unit": "R$/litro",
            "trend": "",
            "source": "CEPEA/ESALQ",
            "period": TODAY,
            "history": old_hist,
        }
        print(f"  ETN_BR: R$ {cepea['etanol_anidro']['price']:.4f}/L")
    
    phys["international"] = intl
    save_json(phys_path, phys)


# =========================================================================
# MAIN
# =========================================================================
def main():
    print("=" * 60)
    print(f"AgriMacro v3.2 — Coletor Acucar & Alcool Brasil")
    print(f"Data: {TODAY}")
    print(f"Output: {DATA_PROC}")
    print("=" * 60)
    
    # Carrega price_history para calculos
    pr_data = load_json(os.path.join(DATA_RAW, "price_history.json"))
    
    # 1. CEPEA
    cepea = collect_cepea_sugar_ethanol()
    
    # 2. ANP
    anp = collect_anp_fuel()
    
    # 3. UNICA
    unica = collect_unica()
    
    # 4. CONSECANA
    consecana = collect_consecana()
    
    # 5. Calculos
    spreads = calculate_spreads(cepea, anp, unica, consecana, pr_data)
    
    # 6. Monta JSON consolidado
    output = {
        "metadata": {
            "date": TODAY,
            "version": "1.0",
            "pipeline": "collect_sugar_alcohol_br.py",
            "sources": ["CEPEA/ESALQ", "ANP", "UNICA", "CONSECANA"],
        },
        "cepea": cepea,
        "anp": anp,
        "unica": unica,
        "consecana": consecana,
        "spreads": spreads,
    }
    
    # Salva JSON consolidado
    output_path = os.path.join(DATA_PROC, "sugar_alcohol_br.json")
    save_json(output_path, output)
    
    # 7. Atualiza physical_intl.json
    update_physical_intl(cepea)
    
    # Resumo
    print("\n" + "=" * 60)
    print("RESUMO DA COLETA:")
    print(f"  CEPEA Acucar:      {'OK' if cepea['acucar_cristal']['price'] else 'FALHOU'}")
    print(f"  CEPEA Etanol Hid:  {'OK' if cepea['etanol_hidratado']['price'] else 'FALHOU'}")
    print(f"  CEPEA Etanol Ani:  {'OK' if cepea['etanol_anidro']['price'] else 'FALHOU'}")
    print(f"  ANP Etanol Bomba:  {'OK' if anp['etanol_bomba']['preco_medio'] else 'FALHOU'}")
    print(f"  ANP Gasolina:      {'OK' if anp['gasolina_bomba']['preco_medio'] else 'FALHOU'}")
    print(f"  ANP Paridade:      {anp['paridade_etanol_gasolina'] or 'N/A'}")
    print(f"  UNICA Moagem:      {'OK' if unica['moagem_cana_mil_ton'] else 'FALHOU'}")
    print(f"  CONSECANA ATR:     {'OK' if consecana['preco_atr_rs_kg'] else 'FALHOU'}")
    print(f"  Spreads calculados:{len(spreads)}")
    print("=" * 60)
    
    # Contagem de sucessos
    success = sum([
        bool(cepea['acucar_cristal']['price']),
        bool(cepea['etanol_hidratado']['price']),
        bool(anp['etanol_bomba']['preco_medio']),
    ])
    if success == 0:
        print("\n[ATENCAO] Nenhuma fonte retornou dados!")
        print("Possiveis causas:")
        print("  - Sites CEPEA/ANP fora do ar")
        print("  - Estrutura HTML mudou (precisa atualizar scraping)")
        print("  - Sem conexao com internet")
        print("  - Firewall bloqueando")
    elif success < 3:
        print(f"\n[PARCIAL] {success}/3 fontes principais coletadas.")
        print("Verifique as fontes que falharam e tente novamente.")
    else:
        print(f"\n[SUCESSO] Coleta completa! {success}/3 fontes OK.")
    
    return output


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[ERRO FATAL] {e}")
        traceback.print_exc()
        sys.exit(1)
