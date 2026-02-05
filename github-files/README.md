# 🌾 AgriMacro v2.0

Dashboard analítico + relatório diário de commodities agrícolas.

## O que é

Sistema que responde diariamente:
> "O que está acontecendo no mercado agrícola hoje, onde estão os riscos, onde há distorções, e o que merece atenção."

## Princípios

- **ZERO MOCK** — somente dados reais
- **Estoque como eixo central** — preço, curva e COT orbitam o estoque
- **Diagnóstico, não recomendação** — sem buy/sell/calls

## Commodities (21)

| Categoria | Símbolos |
|-----------|----------|
| Grãos | ZC, ZS, ZW, KE, ZM, ZL |
| Softs | SB, KC, CT, CC, OJ |
| Pecuária | LE, GF, HE |
| Energia | CL, NG |
| Metais | GC, SI |
| Macro | DX |

## Fontes de Dados

- **Primária:** Stooq (CSV)
- **Fallback:** Yahoo Finance
- **Fundamentais:** USDA, CFTC, CONAB, FRED

## Estrutura

```
agrimacro/
├── src/
│   ├── collectors/     # Coleta de dados
│   ├── analyzers/      # Análises
│   └── generators/     # PDF e dashboard
├── data/
├── outputs/
├── config/
└── tests/
```

## Gates

- **Gate 1:** Estrutura ✅
- **Gate 2:** Coleta de preços
- **Gate 3:** Análises (spreads, sazonalidade, COT, estoques)
- **Gate 4:** Relatório PDF
- **Gate 5:** Dashboard
