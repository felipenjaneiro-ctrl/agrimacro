"""
AgriMacro - Webhook API v2.0
Script principal para integração com n8n via webhook
Inclui coleta de dados reais de 21 commodities via Stooq
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import requests
from datetime import datetime
import uvicorn
from io import StringIO
import csv

app = FastAPI(
    title="AgriMacro API",
    description="API webhook para integração com n8n - Dados de Commodities",
    version="2.0.0"
)

# ============================================================================
# CONFIGURAÇÃO DAS COMMODITIES
# ============================================================================

COMMODITIES = [
    # Grãos
    {"symbol": "ZC", "name": "Corn", "exchange": "CBOT", "unit": "¢/bu", "stooq": "ZC.F"},
    {"symbol": "ZS", "name": "Soybeans", "exchange": "CBOT", "unit": "¢/bu", "stooq": "ZS.F"},
    {"symbol": "ZW", "name": "Wheat", "exchange": "CBOT", "unit": "¢/bu", "stooq": "ZW.F"},
    {"symbol": "ZM", "name": "Soybean Meal", "exchange": "CBOT", "unit": "$/ton", "stooq": "ZM.F"},
    {"symbol": "ZL", "name": "Soybean Oil", "exchange": "CBOT", "unit": "¢/lb", "stooq": "ZL.F"},
    {"symbol": "KE", "name": "KC HRW Wheat", "exchange": "CBOT", "unit": "¢/bu", "stooq": "KE.F"},
    {"symbol": "ZO", "name": "Oats", "exchange": "CBOT", "unit": "¢/bu", "stooq": "ZO.F"},
    {"symbol": "ZR", "name": "Rough Rice", "exchange": "CBOT", "unit": "¢/cwt", "stooq": "ZR.F"},
    # Pecuária
    {"symbol": "LE", "name": "Live Cattle", "exchange": "CME", "unit": "¢/lb", "stooq": "LE.F"},
    {"symbol": "GF", "name": "Feeder Cattle", "exchange": "CME", "unit": "¢/lb", "stooq": "GF.F"},
    {"symbol": "HE", "name": "Lean Hogs", "exchange": "CME", "unit": "¢/lb", "stooq": "HE.F"},
    # Softs
    {"symbol": "CT", "name": "Cotton", "exchange": "ICE", "unit": "¢/lb", "stooq": "CT.F"},
    {"symbol": "KC", "name": "Coffee", "exchange": "ICE", "unit": "¢/lb", "stooq": "KC.F"},
    {"symbol": "SB", "name": "Sugar #11", "exchange": "ICE", "unit": "¢/lb", "stooq": "SB.F"},
    {"symbol": "CC", "name": "Cocoa", "exchange": "ICE", "unit": "$/ton", "stooq": "CC.F"},
    {"symbol": "OJ", "name": "Orange Juice", "exchange": "ICE", "unit": "¢/lb", "stooq": "OJ.F"},
    # Energia
    {"symbol": "CL", "name": "Crude Oil WTI", "exchange": "NYMEX", "unit": "$/bbl", "stooq": "CL.F"},
    {"symbol": "NG", "name": "Natural Gas", "exchange": "NYMEX", "unit": "$/MMBtu", "stooq": "NG.F"},
    # Metais
    {"symbol": "GC", "name": "Gold", "exchange": "COMEX", "unit": "$/oz", "stooq": "GC.F"},
    {"symbol": "SI", "name": "Silver", "exchange": "COMEX", "unit": "$/oz", "stooq": "SI.F"},
    # Moedas
    {"symbol": "DX", "name": "US Dollar Index", "exchange": "ICE", "unit": "index", "stooq": "DX.F"},
]


def fetch_stooq_data(stooq_symbol: str) -> dict | None:
    """Busca dados de uma commodity no Stooq"""
    url = f"https://stooq.com/q/l/?s={stooq_symbol}&f=sd2t2ohlcv&h&e=csv"
    
    try:
        response = requests.get(url, timeout=10, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        response.raise_for_status()
        
        # Parse CSV
        reader = csv.DictReader(StringIO(response.text))
        rows = list(reader)
        
        if not rows:
            return None
        
        row = rows[0]
        
        # Validar dados
        close = row.get('Close', '')
        date = row.get('Date', '')
        
        if not close or close == 'N/D' or not date or date == 'N/D':
            return None
        
        try:
            close_val = float(close)
            open_val = float(row.get('Open', 0)) or None
            high_val = float(row.get('High', 0)) or None
            low_val = float(row.get('Low', 0)) or None
            volume_val = int(row.get('Volume', 0)) if row.get('Volume') else None
        except (ValueError, TypeError):
            return None
        
        # Calcular variação
        change = None
        change_pct = None
        if open_val and close_val:
            change = round(close_val - open_val, 4)
            change_pct = round((change / open_val) * 100, 4)
        
        return {
            "open": open_val,
            "high": high_val,
            "low": low_val,
            "close": close_val,
            "volume": volume_val,
            "date": date,
            "change": change,
            "change_pct": change_pct
        }
        
    except Exception:
        return None


def collect_all_commodities() -> dict:
    """Coleta dados de todas as 21 commodities"""
    results = {}
    successful = 0
    failed = 0
    
    for commodity in COMMODITIES:
        data = fetch_stooq_data(commodity["stooq"])
        
        if data:
            results[commodity["symbol"]] = {
                "symbol": commodity["symbol"],
                "name": commodity["name"],
                "exchange": commodity["exchange"],
                "unit": commodity["unit"],
                "price": data,
                "price_date": data["date"],
                "source": "stooq",
                "timestamp": datetime.now().isoformat()
            }
            successful += 1
        else:
            results[commodity["symbol"]] = {
                "symbol": commodity["symbol"],
                "name": commodity["name"],
                "exchange": commodity["exchange"],
                "unit": commodity["unit"],
                "price": None,
                "price_date": None,
                "source": "failed",
                "error": "Falha ao coletar dados",
                "timestamp": datetime.now().isoformat()
            }
            failed += 1
    
    return {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "total_commodities": len(COMMODITIES),
            "successful": successful,
            "failed": failed,
            "success_rate": round((successful / len(COMMODITIES)) * 100, 2),
            "sources_used": {
                "stooq": successful,
                "failed": failed
            }
        },
        "commodities": results
    }


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/")
async def root():
    """Endpoint raiz - retorna informações básicas da API"""
    return {
        "projeto": "AgriMacro",
        "status": "ativo",
        "versao": "2.0.0",
        "endpoints": {
            "/webhook": "GET para dados completos das commodities, POST para receber dados",
            "/commodities": "Lista todas as 21 commodities com preços atuais",
            "/health": "Verificação de saúde da API"
        },
        "timestamp": datetime.now().isoformat()
    }


@app.get("/webhook")
async def webhook_get():
    """Endpoint webhook GET - retorna dados completos das 21 commodities"""
    data = collect_all_commodities()
    return JSONResponse(content={
        "projeto": "AgriMacro",
        "status": "sucesso",
        "mensagem": f"Dados de {data['metadata']['successful']}/{data['metadata']['total_commodities']} commodities coletados",
        "metodo": "GET",
        "data": data,
        "timestamp": datetime.now().isoformat()
    })


@app.post("/webhook")
async def webhook_post(request: Request):
    """Endpoint webhook POST - recebe dados e retorna JSON"""
    try:
        body = await request.json()
    except Exception:
        body = {}
    
    return JSONResponse(content={
        "projeto": "AgriMacro",
        "status": "sucesso",
        "mensagem": "Dados recebidos com sucesso",
        "metodo": "POST",
        "dados_recebidos": body,
        "timestamp": datetime.now().isoformat()
    })


@app.get("/commodities")
async def get_commodities():
    """Endpoint para obter dados de todas as commodities"""
    return JSONResponse(content=collect_all_commodities())


@app.get("/commodity/{symbol}")
async def get_commodity(symbol: str):
    """Endpoint para obter dados de uma commodity específica"""
    symbol = symbol.upper()
    
    # Encontrar commodity
    commodity = next((c for c in COMMODITIES if c["symbol"] == symbol), None)
    
    if not commodity:
        return JSONResponse(
            status_code=404,
            content={"error": f"Commodity '{symbol}' não encontrada"}
        )
    
    data = fetch_stooq_data(commodity["stooq"])
    
    if data:
        return JSONResponse(content={
            "symbol": commodity["symbol"],
            "name": commodity["name"],
            "exchange": commodity["exchange"],
            "unit": commodity["unit"],
            "price": data,
            "price_date": data["date"],
            "source": "stooq",
            "timestamp": datetime.now().isoformat()
        })
    else:
        return JSONResponse(
            status_code=503,
            content={"error": f"Falha ao coletar dados de '{symbol}'"}
        )


@app.get("/health")
async def health_check():
    """Endpoint de verificação de saúde da API"""
    return {
        "status": "healthy",
        "servico": "AgriMacro API",
        "versao": "2.0.0",
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
