"""
AgriMacro v2.0 - Configuração central de commodities
"""

COMMODITIES = {
    # === GRÃOS ===
    "ZC": {
        "name": "Corn",
        "stooq": "zc.f",
        "yahoo": "ZC=F",
        "category": "grains",
        "unit": "cents/bu",
        "exchange": "CBOT"
    },
    "ZS": {
        "name": "Soybeans",
        "stooq": "zs.f",
        "yahoo": "ZS=F",
        "category": "grains",
        "unit": "cents/bu",
        "exchange": "CBOT"
    },
    "ZW": {
        "name": "Wheat CBOT",
        "stooq": "zw.f",
        "yahoo": "ZW=F",
        "category": "grains",
        "unit": "cents/bu",
        "exchange": "CBOT"
    },
    "KE": {
        "name": "Wheat KC",
        "stooq": "ke.f",
        "yahoo": "KE=F",
        "category": "grains",
        "unit": "cents/bu",
        "exchange": "KCBT"
    },
    "ZM": {
        "name": "Soybean Meal",
        "stooq": "zm.f",
        "yahoo": "ZM=F",
        "category": "grains",
        "unit": "USD/short ton",
        "exchange": "CBOT"
    },
    "ZL": {
        "name": "Soybean Oil",
        "stooq": "zl.f",
        "yahoo": "ZL=F",
        "category": "grains",
        "unit": "cents/lb",
        "exchange": "CBOT"
    },

    # === SOFTS ===
    "SB": {
        "name": "Sugar",
        "stooq": "sb.f",
        "yahoo": "SB=F",
        "category": "softs",
        "unit": "cents/lb",
        "exchange": "ICE"
    },
    "KC": {
        "name": "Coffee",
        "stooq": "kc.f",
        "yahoo": "KC=F",
        "category": "softs",
        "unit": "cents/lb",
        "exchange": "ICE"
    },
    "CT": {
        "name": "Cotton",
        "stooq": "ct.f",
        "yahoo": "CT=F",
        "category": "softs",
        "unit": "cents/lb",
        "exchange": "ICE"
    },
    "CC": {
        "name": "Cocoa",
        "stooq": "cc.f",
        "yahoo": "CC=F",
        "category": "softs",
        "unit": "USD/mt",
        "exchange": "ICE"
    },
    "OJ": {
        "name": "Orange Juice",
        "stooq": "oj.f",
        "yahoo": "OJ=F",
        "category": "softs",
        "unit": "cents/lb",
        "exchange": "ICE"
    },

    # === PECUÁRIA ===
    "LE": {
        "name": "Live Cattle",
        "stooq": "le.f",
        "yahoo": "LE=F",
        "category": "livestock",
        "unit": "cents/lb",
        "exchange": "CME"
    },
    "GF": {
        "name": "Feeder Cattle",
        "stooq": "gf.f",
        "yahoo": "GF=F",
        "category": "livestock",
        "unit": "cents/lb",
        "exchange": "CME"
    },
    "HE": {
        "name": "Lean Hogs",
        "stooq": "he.f",
        "yahoo": "HE=F",
        "category": "livestock",
        "unit": "cents/lb",
        "exchange": "CME"
    },

    # === ENERGIA ===
    "CL": {
        "name": "Crude Oil WTI",
        "stooq": "cl.f",
        "yahoo": "CL=F",
        "category": "energy",
        "unit": "USD/bbl",
        "exchange": "NYMEX"
    },
    "NG": {
        "name": "Natural Gas",
        "stooq": "ng.f",
        "yahoo": "NG=F",
        "category": "energy",
        "unit": "USD/MMBtu",
        "exchange": "NYMEX"
    },

    # === METAIS ===
    "GC": {
        "name": "Gold",
        "stooq": "gc.f",
        "yahoo": "GC=F",
        "category": "metals",
        "unit": "USD/oz",
        "exchange": "COMEX"
    },
    "SI": {
        "name": "Silver",
        "stooq": "si.f",
        "yahoo": "SI=F",
        "category": "metals",
        "unit": "USD/oz",
        "exchange": "COMEX"
    },

    # === MACRO ===
    "DX": {
        "name": "Dollar Index",
        "stooq": "dx.f",
        "yahoo": "DX-Y.NYB",
        "category": "macro",
        "unit": "index",
        "exchange": "ICE"
    },
}

# Agrupamento por categoria
CATEGORIES = {
    "grains": ["ZC", "ZS", "ZW", "KE", "ZM", "ZL"],
    "softs": ["SB", "KC", "CT", "CC", "OJ"],
    "livestock": ["LE", "GF", "HE"],
    "energy": ["CL", "NG"],
    "metals": ["GC", "SI"],
    "macro": ["DX"],
}

# Spreads definidos
SPREADS = {
    "soy_crush": {
        "name": "Soy Crush",
        "components": ["ZS", "ZM", "ZL"],
        "unit": "USD/bu"
    },
    "ke_zw": {
        "name": "KE-ZW Spread",
        "components": ["KE", "ZW"],
        "unit": "cents/bu"
    },
    "zl_cl": {
        "name": "ZL/CL Ratio",
        "components": ["ZL", "CL"],
        "unit": "ratio"
    },
    "feedlot": {
        "name": "Feedlot Margin",
        "components": ["LE", "GF", "ZC"],
        "unit": "USD/cwt"
    },
    "zc_zm": {
        "name": "ZC/ZM Feed Value",
        "components": ["ZC", "ZM"],
        "unit": "ratio"
    },
    "zc_zs": {
        "name": "Corn/Soy Ratio",
        "components": ["ZC", "ZS"],
        "unit": "ratio"
    },
}
