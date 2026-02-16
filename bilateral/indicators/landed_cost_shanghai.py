#!/usr/bin/env python3
"""
AgriMacro Intelligence — Proprietary Indicator #1
LANDED COST SPREAD SHANGHAI (LCS)

Compares total delivered cost of soybeans from US Gulf vs Brazil Santos to Shanghai.
Positive spread = Brazil more competitive. Negative = US more competitive.

Components:
  US Route:  CBOT + Gulf Basis + Barge Freight + Ocean Freight (Gulf→Shanghai)
  BR Route:  CEPEA/IMEA Price + Interior Freight + Ocean Freight (Santos→Shanghai)
             (all converted to US$/mt via PTAX)

  LCS = US Landed Cost - BR Landed Cost

Data Sources:
  - CBOT soybean futures (IBKR/CME)
  - Gulf basis/premium (USDA AMS GTR)
  - US barge freight (USDA AMS GTR)
  - Ocean freight rates (Baltic Exchange / Platts proxies)
  - CEPEA soybean price Paranaguá (CEPEA/ESALQ)
  - IMEA MT price + interior freight + Santos premium
  - BRL/USD exchange rate (BCB/PTAX)

Conversion Constants:
  1 bushel soybean = 27.2155 kg
  1 metric ton = 36.7437 bushels
  1 saca (60kg) = 2.20462 bushels
  1 metric ton = 16.6667 sacas
"""

import json
import os
from datetime import datetime, date
from dataclasses import dataclass, field, asdict
from typing import Optional

# ============================================================
# CONVERSION CONSTANTS
# ============================================================
BU_PER_MT = 36.7437          # bushels per metric ton (soybeans)
SC_PER_MT = 16.6667          # sacas (60kg) per metric ton
KG_PER_BU = 27.2155          # kg per bushel
BU_PER_SC = 2.20462          # bushels per saca

# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class USRoute:
    """US Gulf → Shanghai cost components (all in US$/mt unless noted)"""
    date: str                          # reference date
    cbot_nearby: float                 # CBOT nearby futures (¢US$/bu)
    gulf_basis: float                  # Gulf CIF basis/premium (¢US$/bu)
    fob_gulf_bu: float = 0.0          # FOB Gulf (¢US$/bu) = CBOT + basis
    fob_gulf_mt: float = 0.0          # FOB Gulf (US$/mt)
    barge_freight: float = 0.0        # Barge freight index (US$/mt, IL River→Gulf)
    ocean_freight_gulf_shanghai: float = 0.0  # Panamax Gulf→Shanghai (US$/mt)
    landed_cost_shanghai: float = 0.0 # Total delivered Shanghai (US$/mt)
    
    # Optional detail
    cbot_source: str = "CME"
    basis_source: str = "USDA_GTR"
    ocean_source: str = "Baltic/Platts"
    
    def calculate(self):
        """Calculate all derived fields"""
        self.fob_gulf_bu = self.cbot_nearby + self.gulf_basis  # ¢/bu
        self.fob_gulf_mt = (self.fob_gulf_bu / 100) * BU_PER_MT  # US$/mt
        self.landed_cost_shanghai = (
            self.fob_gulf_mt 
            + self.barge_freight 
            + self.ocean_freight_gulf_shanghai
        )
        return self


@dataclass
class BRRoute:
    """Brazil Santos → Shanghai cost components"""
    date: str                          # reference date
    
    # Primary price (choose one source)
    cepea_paranagua_rs_sc: float = 0.0    # CEPEA Paranaguá (R$/sc)
    imea_mt_rs_sc: float = 0.0            # IMEA Mato Grosso (R$/sc)
    premio_santos_cents_bu: float = 0.0   # Santos premium (¢US$/bu)
    
    # Freight
    frete_interior_rs_mt: float = 0.0     # Interior freight Sorriso→Santos (R$/mt)
    ocean_freight_santos_shanghai: float = 0.0  # Panamax Santos→Shanghai (US$/mt)
    
    # FX
    ptax: float = 0.0                     # BRL/USD exchange rate
    
    # Calculated fields (US$/mt)
    fob_santos_mt: float = 0.0            # FOB Santos (US$/mt)
    frete_interior_usd_mt: float = 0.0    # Interior freight (US$/mt)
    landed_cost_shanghai: float = 0.0     # Total delivered Shanghai (US$/mt)
    
    # Method flag
    price_method: str = "paranagua"       # "paranagua" or "mt_interior"
    
    # Sources
    price_source: str = "CEPEA"
    ocean_source: str = "Baltic/Platts"
    
    def calculate(self):
        """Calculate all derived fields based on chosen method"""
        if self.ptax <= 0:
            raise ValueError("PTAX must be positive")
        
        if self.price_method == "paranagua":
            # Method 1: CEPEA Paranaguá price (already includes interior freight)
            # CEPEA Paranaguá ≈ FOB Santos equivalent
            price_usd_mt = (self.cepea_paranagua_rs_sc / self.ptax) * SC_PER_MT
            self.fob_santos_mt = price_usd_mt
            self.frete_interior_usd_mt = 0  # included in Paranaguá price
            self.price_source = "CEPEA_Paranaguá"
            
        elif self.price_method == "mt_interior":
            # Method 2: IMEA MT price + interior freight + Santos premium
            # More granular, shows inland basis
            mt_price_usd_mt = (self.imea_mt_rs_sc / self.ptax) * SC_PER_MT
            self.frete_interior_usd_mt = self.frete_interior_rs_mt / self.ptax
            premio_usd_mt = (self.premio_santos_cents_bu / 100) * BU_PER_MT
            self.fob_santos_mt = mt_price_usd_mt + self.frete_interior_usd_mt + premio_usd_mt
            self.price_source = "IMEA_MT+frete+premio"
            
        else:
            raise ValueError(f"Unknown price_method: {self.price_method}")
        
        self.landed_cost_shanghai = (
            self.fob_santos_mt 
            + self.ocean_freight_santos_shanghai
        )
        return self


@dataclass
class LandedCostSpread:
    """The proprietary indicator"""
    date: str
    us_landed_shanghai: float          # US$/mt
    br_landed_shanghai: float          # US$/mt
    spread: float = 0.0               # US - BR (positive = BR cheaper)
    spread_pct: float = 0.0           # spread as % of avg
    competitive_origin: str = ""      # "US" or "BR"
    
    # Component breakdown
    us_fob: float = 0.0
    br_fob: float = 0.0
    fob_spread: float = 0.0           # FOB spread (before ocean)
    us_ocean: float = 0.0
    br_ocean: float = 0.0
    ocean_advantage: float = 0.0      # BR ocean advantage (shorter route)
    
    # Context
    br_price_method: str = ""
    ptax: float = 0.0
    cbot_cents_bu: float = 0.0
    imea_comercializacao_pct: float = 0.0  # farmer selling pace
    
    def calculate(self):
        self.spread = self.us_landed_shanghai - self.br_landed_shanghai
        avg = (self.us_landed_shanghai + self.br_landed_shanghai) / 2
        self.spread_pct = (self.spread / avg) * 100 if avg > 0 else 0
        self.competitive_origin = "BR" if self.spread > 0 else "US"
        self.fob_spread = self.us_fob - self.br_fob
        self.ocean_advantage = self.us_ocean - self.br_ocean
        return self


# ============================================================
# INDICATOR CALCULATOR
# ============================================================

def calculate_landed_cost_spread(
    # US inputs
    cbot_cents_bu: float,           # CBOT nearby (¢US$/bu)
    gulf_basis_cents_bu: float,     # Gulf basis (¢US$/bu)
    barge_freight_usd_mt: float,    # Barge IL→Gulf (US$/mt)
    ocean_gulf_shanghai: float,     # Ocean Gulf→Shanghai (US$/mt)
    
    # Brazil inputs
    ptax: float,                    # BRL/USD
    ocean_santos_shanghai: float,   # Ocean Santos→Shanghai (US$/mt)
    
    # Brazil price (choose one method)
    cepea_paranagua_rs_sc: float = 0.0,    # Method 1
    imea_mt_rs_sc: float = 0.0,            # Method 2
    frete_interior_rs_mt: float = 0.0,     # Method 2
    premio_santos_cents_bu: float = 0.0,   # Method 2
    
    # Context (optional)
    imea_comercializacao_pct: float = 0.0,
    ref_date: str = "",
) -> LandedCostSpread:
    """
    Calculate the Landed Cost Spread Shanghai.
    
    Returns positive spread when Brazil is more competitive (cheaper).
    Returns negative spread when US is more competitive.
    """
    if not ref_date:
        ref_date = date.today().isoformat()
    
    # --- US Route ---
    us = USRoute(
        date=ref_date,
        cbot_nearby=cbot_cents_bu,
        gulf_basis=gulf_basis_cents_bu,
        barge_freight=barge_freight_usd_mt,
        ocean_freight_gulf_shanghai=ocean_gulf_shanghai,
    ).calculate()
    
    # --- BR Route ---
    if cepea_paranagua_rs_sc > 0:
        br = BRRoute(
            date=ref_date,
            cepea_paranagua_rs_sc=cepea_paranagua_rs_sc,
            ptax=ptax,
            ocean_freight_santos_shanghai=ocean_santos_shanghai,
            price_method="paranagua",
        ).calculate()
    elif imea_mt_rs_sc > 0:
        br = BRRoute(
            date=ref_date,
            imea_mt_rs_sc=imea_mt_rs_sc,
            frete_interior_rs_mt=frete_interior_rs_mt,
            premio_santos_cents_bu=premio_santos_cents_bu,
            ptax=ptax,
            ocean_freight_santos_shanghai=ocean_santos_shanghai,
            price_method="mt_interior",
        ).calculate()
    else:
        raise ValueError("Must provide either CEPEA Paranaguá or IMEA MT price")
    
    # --- Spread ---
    lcs = LandedCostSpread(
        date=ref_date,
        us_landed_shanghai=round(us.landed_cost_shanghai, 2),
        br_landed_shanghai=round(br.landed_cost_shanghai, 2),
        us_fob=round(us.fob_gulf_mt, 2),
        br_fob=round(br.fob_santos_mt, 2),
        us_ocean=round(us.ocean_freight_gulf_shanghai, 2),
        br_ocean=round(br.ocean_freight_santos_shanghai, 2),
        br_price_method=br.price_method,
        ptax=ptax,
        cbot_cents_bu=cbot_cents_bu,
        imea_comercializacao_pct=imea_comercializacao_pct,
    ).calculate()
    
    return lcs


# ============================================================
# OUTPUT FORMATTERS
# ============================================================

def to_json(lcs: LandedCostSpread) -> str:
    """Export indicator as JSON for dashboard/API"""
    output = {
        "indicator": "LCS_SHANGHAI",
        "version": "1.0",
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "data": {
            "date": lcs.date,
            "spread_usd_mt": round(lcs.spread, 2),
            "spread_pct": round(lcs.spread_pct, 2),
            "competitive_origin": lcs.competitive_origin,
            "us_landed_shanghai": lcs.us_landed_shanghai,
            "br_landed_shanghai": lcs.br_landed_shanghai,
            "components": {
                "us_fob_gulf": lcs.us_fob,
                "br_fob_santos": lcs.br_fob,
                "fob_spread": round(lcs.fob_spread, 2),
                "us_ocean_freight": lcs.us_ocean,
                "br_ocean_freight": lcs.br_ocean,
                "ocean_advantage_br": round(lcs.ocean_advantage, 2),
            },
            "context": {
                "cbot_cents_bu": lcs.cbot_cents_bu,
                "ptax": lcs.ptax,
                "br_price_method": lcs.br_price_method,
                "imea_comercializacao_pct": lcs.imea_comercializacao_pct,
            }
        },
        "interpretation": {
            "signal": "BR_COMPETITIVE" if lcs.spread > 0 else "US_COMPETITIVE",
            "magnitude": (
                "STRONG" if abs(lcs.spread_pct) > 5 
                else "MODERATE" if abs(lcs.spread_pct) > 2 
                else "NARROW"
            ),
            "description": (
                f"Brazil is ${abs(lcs.spread):.2f}/mt cheaper to Shanghai "
                f"({abs(lcs.spread_pct):.1f}% advantage)"
                if lcs.spread > 0
                else f"US is ${abs(lcs.spread):.2f}/mt cheaper to Shanghai "
                f"({abs(lcs.spread_pct):.1f}% advantage)"
            ),
        }
    }
    return json.dumps(output, indent=2)


def to_report_text(lcs: LandedCostSpread) -> str:
    """Format for PDF report or video narration"""
    arrow = "▼" if lcs.spread > 0 else "▲"
    winner = "BRAZIL" if lcs.spread > 0 else "US"
    
    lines = [
        "=" * 60,
        "  LANDED COST SPREAD SHANGHAI — AgriMacro Intelligence",
        "=" * 60,
        f"  Date: {lcs.date}",
        "",
        f"  US Gulf → Shanghai:     US$ {lcs.us_landed_shanghai:>8.2f} /mt",
        f"    FOB Gulf:             US$ {lcs.us_fob:>8.2f} /mt",
        f"    Ocean freight:        US$ {lcs.us_ocean:>8.2f} /mt",
        "",
        f"  BR Santos → Shanghai:   US$ {lcs.br_landed_shanghai:>8.2f} /mt",
        f"    FOB Santos:           US$ {lcs.br_fob:>8.2f} /mt",
        f"    Ocean freight:        US$ {lcs.br_ocean:>8.2f} /mt",
        f"    (method: {lcs.br_price_method})",
        "",
        "  " + "-" * 56,
        f"  SPREAD:  {arrow} US$ {lcs.spread:>+8.2f} /mt  ({lcs.spread_pct:>+.1f}%)",
        f"  SIGNAL:  {winner} MORE COMPETITIVE",
        "",
        f"  FOB spread:        US$ {lcs.fob_spread:>+8.2f} /mt",
        f"  Ocean advantage:   US$ {lcs.ocean_advantage:>+8.2f} /mt (BR shorter route)",
        "  " + "-" * 56,
    ]
    
    if lcs.imea_comercializacao_pct > 0:
        lines.append(f"  MT Farmer Selling Pace: {lcs.imea_comercializacao_pct:.1f}%")
    lines.append(f"  CBOT: {lcs.cbot_cents_bu:.1f}¢/bu  |  PTAX: R$ {lcs.ptax:.2f}")
    lines.append("=" * 60)
    
    return "\n".join(lines)


def to_dashboard_card(lcs: LandedCostSpread) -> dict:
    """Format for Next.js dashboard component"""
    return {
        "card_type": "landed_cost_spread",
        "title": "Landed Cost Spread Shanghai",
        "subtitle": "US Gulf vs BR Santos → Shanghai",
        "date": lcs.date,
        "primary_value": f"${lcs.spread:+.2f}/mt",
        "primary_color": "#00C878" if lcs.spread > 0 else "#DC3C3C",
        "signal": "BR COMPETITIVE" if lcs.spread > 0 else "US COMPETITIVE",
        "magnitude": (
            "STRONG" if abs(lcs.spread_pct) > 5 
            else "MODERATE" if abs(lcs.spread_pct) > 2 
            else "NARROW"
        ),
        "bars": [
            {"label": "US Gulf→Shanghai", "value": lcs.us_landed_shanghai, "color": "#DCB432"},
            {"label": "BR Santos→Shanghai", "value": lcs.br_landed_shanghai, "color": "#00C878"},
        ],
        "components": [
            {"label": "FOB Spread", "value": f"${lcs.fob_spread:+.2f}"},
            {"label": "Ocean Advantage BR", "value": f"${lcs.ocean_advantage:+.2f}"},
            {"label": "CBOT", "value": f"{lcs.cbot_cents_bu:.0f}¢/bu"},
            {"label": "PTAX", "value": f"R$ {lcs.ptax:.2f}"},
        ],
        "farmer_selling_pace": lcs.imea_comercializacao_pct if lcs.imea_comercializacao_pct > 0 else None,
    }


# ============================================================
# INTEGRATED PIPELINE — loads from collector outputs
# ============================================================

def load_from_collectors(data_dir: str = "./data") -> Optional[LandedCostSpread]:
    """
    Load data from all 4 collector outputs and calculate LCS.
    Expected files in data_dir:
      - ibkr_quotes.json (CBOT prices from IBKR)
      - usda_gtr.json (Gulf basis, barge freight)
      - imea_soja.json (MT price, freight, comercialização)
      - cepea_soja.json (Paranaguá price)
      - bcb_ptax.json (exchange rate)
      - ocean_freight.json (shipping rates)
    """
    try:
        # Load IBKR/CME data
        with open(os.path.join(data_dir, "ibkr_quotes.json")) as f:
            ibkr = json.load(f)
        cbot = ibkr.get("ZS", {}).get("last", 0) * 100  # convert $/bu to ¢/bu
        
        # Load USDA GTR data
        with open(os.path.join(data_dir, "usda_gtr.json")) as f:
            gtr = json.load(f)
        gulf_basis = gtr.get("gulf_basis_cents_bu", 50)  # default ~50¢
        barge_freight = gtr.get("barge_freight_usd_mt", 25)  # default ~$25
        
        # Load IMEA data
        with open(os.path.join(data_dir, "imea_soja.json")) as f:
            imea = json.load(f)
        imea_price = imea.get("preco_mt_rs_sc", 0)
        frete_interior = imea.get("frete_sorriso_miritituba_rs_t", 0)
        # Convert Sorriso→Miritituba to approximate Sorriso→Santos 
        # (Santos route is ~30% more expensive than northern corridor)
        frete_santos = frete_interior * 1.3 if frete_interior > 0 else 0
        premio = imea.get("premio_santos_cents_bu", 0)
        comercializacao = imea.get("comercializacao_safra_atual_pct", 0)
        
        # Load CEPEA data (alternative pricing)
        cepea_price = 0
        cepea_file = os.path.join(data_dir, "cepea_soja.json")
        if os.path.exists(cepea_file):
            with open(cepea_file) as f:
                cepea = json.load(f)
            cepea_price = cepea.get("paranagua_rs_sc", 0)
        
        # Load PTAX
        with open(os.path.join(data_dir, "bcb_ptax.json")) as f:
            ptax_data = json.load(f)
        ptax = ptax_data.get("ptax", 5.50)
        
        # Load ocean freight
        ocean_file = os.path.join(data_dir, "ocean_freight.json")
        if os.path.exists(ocean_file):
            with open(ocean_file) as f:
                ocean = json.load(f)
            ocean_gulf = ocean.get("gulf_shanghai_usd_mt", 45)
            ocean_santos = ocean.get("santos_shanghai_usd_mt", 32)
        else:
            # Typical estimates (Santos has ~3,000nm shorter route)
            ocean_gulf = 45.0
            ocean_santos = 32.0
        
        # Calculate
        lcs = calculate_landed_cost_spread(
            cbot_cents_bu=cbot,
            gulf_basis_cents_bu=gulf_basis,
            barge_freight_usd_mt=barge_freight,
            ocean_gulf_shanghai=ocean_gulf,
            ptax=ptax,
            ocean_santos_shanghai=ocean_santos,
            cepea_paranagua_rs_sc=cepea_price if cepea_price > 0 else 0,
            imea_mt_rs_sc=imea_price if cepea_price == 0 else 0,
            frete_interior_rs_mt=frete_santos if cepea_price == 0 else 0,
            premio_santos_cents_bu=premio if cepea_price == 0 else 0,
            imea_comercializacao_pct=comercializacao,
        )
        
        return lcs
        
    except FileNotFoundError as e:
        print(f"[LCS] Missing data file: {e}")
        return None
    except Exception as e:
        print(f"[LCS] Error: {e}")
        return None


# ============================================================
# DEMO / VALIDATION WITH REAL IMEA DATA
# ============================================================

def demo_with_real_data():
    """
    Demo using real data extracted from IMEA boletim #864 (Sep 8, 2025)
    + typical market estimates for other components.
    """
    print("\n" + "=" * 60)
    print("  AgriMacro LCS — Demo with Real IMEA Data (Sep 2025)")
    print("=" * 60)
    
    # Real IMEA data from boletim soja #864
    imea_mt_price = 119.60          # R$/sc (Mato Grosso)
    imea_frete_miritituba = 317.76  # R$/t (Sorriso→Miritituba)
    imea_frete_santos = 317.76 * 1.3  # ~R$ 413/t estimated Santos route
    imea_premio_santos = -40.0      # ¢US$/bu (typical discount in Sep)
    imea_cme_corrente = 1019.0      # ¢US$/bu (from boletim)
    imea_ptax = 5.45                # R$/US$ (from boletim)
    imea_comercializacao_2425 = 91.94  # % safra 24/25
    imea_comercializacao_2526 = 27.40  # % safra 25/26
    
    # Typical September market estimates
    gulf_basis = 55.0               # ¢US$/bu (harvest pressure)
    barge_freight = 28.0            # US$/mt (IL River→Gulf)
    ocean_gulf_shanghai = 48.0      # US$/mt (Panamax)
    ocean_santos_shanghai = 33.0    # US$/mt (shorter route)
    
    # --- Method 1: Using IMEA MT price (granular) ---
    print("\n--- Method 1: IMEA MT Interior Price ---")
    lcs1 = calculate_landed_cost_spread(
        cbot_cents_bu=imea_cme_corrente,
        gulf_basis_cents_bu=gulf_basis,
        barge_freight_usd_mt=barge_freight,
        ocean_gulf_shanghai=ocean_gulf_shanghai,
        ptax=imea_ptax,
        ocean_santos_shanghai=ocean_santos_shanghai,
        imea_mt_rs_sc=imea_mt_price,
        frete_interior_rs_mt=imea_frete_santos,
        premio_santos_cents_bu=imea_premio_santos,
        imea_comercializacao_pct=imea_comercializacao_2526,
        ref_date="2025-09-08",
    )
    print(to_report_text(lcs1))
    
    # --- Method 2: Using CEPEA Paranaguá (simpler) ---
    # Estimate CEPEA Paranaguá from IMEA data:
    # MT price + freight ≈ Paranaguá equivalent
    cepea_estimated = (imea_mt_price / imea_ptax + imea_frete_santos / imea_ptax / SC_PER_MT) * imea_ptax
    # Simpler: typical Paranaguá premium over MT is ~R$15-20/sc
    cepea_paranagua = 138.00  # R$/sc (typical Sep 2025)
    
    print("\n--- Method 2: CEPEA Paranaguá Price ---")
    lcs2 = calculate_landed_cost_spread(
        cbot_cents_bu=imea_cme_corrente,
        gulf_basis_cents_bu=gulf_basis,
        barge_freight_usd_mt=barge_freight,
        ocean_gulf_shanghai=ocean_gulf_shanghai,
        ptax=imea_ptax,
        ocean_santos_shanghai=ocean_santos_shanghai,
        cepea_paranagua_rs_sc=cepea_paranagua,
        imea_comercializacao_pct=imea_comercializacao_2526,
        ref_date="2025-09-08",
    )
    print(to_report_text(lcs2))
    
    # --- JSON output ---
    print("\n--- JSON Output (Method 1) ---")
    print(to_json(lcs1))
    
    # --- Dashboard card ---
    print("\n--- Dashboard Card ---")
    card = to_dashboard_card(lcs1)
    print(json.dumps(card, indent=2))
    
    # --- Sensitivity Analysis ---
    print("\n" + "=" * 60)
    print("  SENSITIVITY ANALYSIS — Key Drivers")
    print("=" * 60)
    
    scenarios = [
        ("Base Case",        imea_cme_corrente, imea_ptax, imea_mt_price),
        ("CBOT +50¢",        imea_cme_corrente + 50, imea_ptax, imea_mt_price),
        ("CBOT -50¢",        imea_cme_corrente - 50, imea_ptax, imea_mt_price),
        ("BRL Weaker (6.00)", imea_cme_corrente, 6.00, imea_mt_price),
        ("BRL Stronger (5.00)", imea_cme_corrente, 5.00, imea_mt_price),
        ("MT Price +R$10",   imea_cme_corrente, imea_ptax, imea_mt_price + 10),
        ("MT Price -R$10",   imea_cme_corrente, imea_ptax, imea_mt_price - 10),
    ]
    
    print(f"\n  {'Scenario':<25} {'US Landed':>10} {'BR Landed':>10} {'Spread':>10} {'Signal':>15}")
    print("  " + "-" * 72)
    
    for name, cbot, ptax, mt_price in scenarios:
        s = calculate_landed_cost_spread(
            cbot_cents_bu=cbot,
            gulf_basis_cents_bu=gulf_basis,
            barge_freight_usd_mt=barge_freight,
            ocean_gulf_shanghai=ocean_gulf_shanghai,
            ptax=ptax,
            ocean_santos_shanghai=ocean_santos_shanghai,
            imea_mt_rs_sc=mt_price,
            frete_interior_rs_mt=imea_frete_santos,
            premio_santos_cents_bu=imea_premio_santos,
            ref_date="2025-09-08",
        )
        signal = "BR ✓" if s.spread > 0 else "US ✓"
        print(f"  {name:<25} ${s.us_landed_shanghai:>8.2f}  ${s.br_landed_shanghai:>8.2f}  ${s.spread:>+8.2f}  {signal:>15}")
    
    print("\n  Key insight: BRL depreciation is the strongest driver of")
    print("  Brazilian competitiveness (wider positive spread).\n")


if __name__ == "__main__":
    demo_with_real_data()
