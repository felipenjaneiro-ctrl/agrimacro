"""
AgriMacro - Webhook API
Script principal para integração com n8n via webhook
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import pandas as pd
import requests
from datetime import datetime
import uvicorn

app = FastAPI(
    title="AgriMacro API",
    description="API webhook para integração com n8n",
    version="1.0.0"
)


@app.get("/")
async def root():
    """Endpoint raiz - retorna informações básicas da API"""
    return {
        "projeto": "AgriMacro",
        "status": "ativo",
        "versao": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/webhook")
async def webhook_get():
    """Endpoint webhook GET - retorna JSON simples"""
    return JSONResponse(content={
        "projeto": "AgriMacro",
        "status": "sucesso",
        "mensagem": "Webhook ativo e funcionando",
        "metodo": "GET",
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


@app.get("/health")
async def health_check():
    """Endpoint de verificação de saúde da API"""
    return {
        "status": "healthy",
        "servico": "AgriMacro API",
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
