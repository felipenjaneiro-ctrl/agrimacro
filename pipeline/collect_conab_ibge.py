"""
collect_conab_ibge.py - AgriMacro v3.3
Fontes: IBGE Tabela 5457 (PAM) + USDA FAS psd_countries (BR)
Sem autenticacao. Roda automaticamente no pipeline mensal.
"""
import requests, json
from datetime import datetime
from pathlib import Path

HEADERS = {"User-Agent": "AgriMacro-Intelligence/3.3 (research)"}
BASE = Path(__file__).parent.parent
OUTPUT = str(BASE / "agrimacro-dash" / "public" / "data" / "processed" / "conab_data.json")
FAS_PATH = str(BASE / "agrimacro-dash" / "public" / "data" / "processed" / "usda_fas.json")

# IBGE PAM Tabela 5457 - variavel 214=producao(ton), 216=rendimento(kg/ha), 8331=area(ha)
# Produtos (classificacao 782)
CULTURAS_IBGE = {
    "soja":    {"cod": "40444", "key": "soja"},
    "milho":   {"cod": "40457", "key": "milho_total"},
    "trigo":   {"cod": "40469", "key": "trigo"},
    "algodao": {"cod": "40408", "key": "algodao_pluma"},
    "arroz":   {"cod": "40445", "key": "arroz"},
    "feijao":  {"cod": "40446", "key": "feijao_total"},
}

def fetch_5457(produto_cod, variavel):
    url = (
        f"https://servicodados.ibge.gov.br/api/v3/agregados/5457"
        f"/periodos/2020|2021|2022|2023|2024"
        f"/variaveis/{variavel}"
        f"?localidades=N1[all]&classificacao=782[{produto_cod}]"
    )
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    serie = data[0]["resultados"][0]["series"][0]["serie"]
    return {ano: float(v) for ano, v in serie.items() if v not in ("...", "-", "", None)}

def coleta_ibge():
    print("  [IBGE PAM 5457] Coletando...")
    culturas_ok = {}
    ultimo_ano = None
    for nome, info in CULTURAS_IBGE.items():
        try:
            prod = fetch_5457(info["cod"], "214")    # toneladas
            rend = fetch_5457(info["cod"], "216")    # kg/ha
            anos = sorted(set(prod) & set(rend))
            if not anos:
                print(f"    [SKIP] {nome}: sem dados")
                continue
            ano = anos[-1]
            if ultimo_ano is None or int(ano) > int(ultimo_ano):
                ultimo_ano = ano
            culturas_ok[info["key"]] = {
                "producao_mt": round(prod[ano] / 1e6, 1),
                "produtividade_kg_ha": round(rend[ano], 0),
                "ano_ref": ano,
                "source": "IBGE PAM Tabela 5457"
            }
            print(f"    [OK] {nome}: {round(prod[ano]/1e6,1)} MT | {ano}")
        except Exception as e:
            print(f"    [ERRO] {nome}: {e}")
    return culturas_ok, ultimo_ano

def coleta_usda_fas():
    print("  [USDA FAS] Lendo psd_countries BR...")
    culturas_ok = {}
    try:
        d = json.load(open(FAS_PATH, encoding="utf-8"))
        psd = d.get("psd_countries", {})
        mapa = {"soybeans": "soja", "corn": "milho_total", "wheat": "trigo",
                "cotton": "algodao_pluma", "rice": "arroz"}
        for commodity, key in mapa.items():
            dados_br = psd.get(commodity, {}).get("BR", {})
            if not dados_br:
                continue
            latest = str(dados_br.get("latest_year", ""))
            data_yr = dados_br.get("data", {}).get(latest, {})
            prod_tmt = data_yr.get("Production")
            if prod_tmt:
                culturas_ok[key] = {
                    "producao_mt": round(prod_tmt / 1000, 1),
                    "ano_ref": latest,
                    "source": "USDA FAS PSD (psd_countries BR)"
                }
                print(f"    [OK] {commodity}: {round(prod_tmt/1000,1)} MT | {latest}")
    except Exception as e:
        print(f"    [ERRO] USDA FAS: {e}")
    return culturas_ok

def main():
    print("\n[CONAB AUTO] Coletando dados automaticos de producao BR...")

    ibge_culturas, ibge_ano = coleta_ibge()
    fas_culturas = coleta_usda_fas()

    # Mescla: IBGE como primario, USDA FAS como complemento
    culturas_merged = {}
    todas_keys = set(list(ibge_culturas.keys()) + list(fas_culturas.keys()))
    for key in todas_keys:
        if key in ibge_culturas:
            culturas_merged[key] = ibge_culturas[key]
            if key in fas_culturas:
                culturas_merged[key]["usda_fas_mt"] = fas_culturas[key]["producao_mt"]
        elif key in fas_culturas:
            culturas_merged[key] = fas_culturas[key]

    # Carrega e atualiza conab_data.json
    try:
        with open(OUTPUT, "r", encoding="utf-8") as f:
            conab = json.load(f)
    except Exception:
        conab = {}

    conab["_meta"] = {
        "source": "CONAB (manual) + IBGE PAM + USDA FAS (automaticos)",
        "collected_at": datetime.now().isoformat(),
        "note": "boletim_info: atualizar manualmente apos cada boletim CONAB. ibge_auto: automatico."
    }
    conab["ibge_auto"] = {
        "ano_referencia": ibge_ano,
        "collected_at": datetime.now().isoformat(),
        "sources": ["IBGE PAM Tabela 5457", "USDA FAS PSD Countries"],
        "culturas": culturas_merged
    }

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(conab, f, ensure_ascii=False, indent=2)
    print(f"\n  [OK] Salvo: {OUTPUT}")
    print(f"  Culturas automaticas: {len(culturas_merged)}")

if __name__ == "__main__":
    main()