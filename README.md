# AgriMacro API

API webhook para integração com n8n e automações.

## Endpoints

| Endpoint | Método | Descrição |
|----------|--------|-----------|
| `/` | GET | Informações básicas da API |
| `/webhook` | GET | Retorna JSON de status |
| `/webhook` | POST | Recebe dados e retorna confirmação |
| `/health` | GET | Verificação de saúde da API |

## Instalação Local

```bash
pip install -r requirements.txt
uvicorn main:app --reload
```

## Deploy

Este projeto está configurado para deploy no Render.com.

## Tecnologias

- FastAPI
- Uvicorn
- Pandas
- Requests
- Anthropic
