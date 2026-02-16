#!/usr/bin/env python3
"""
AgriMacro Intelligence — Proprietary Indicator #3
BRAZIL COMPETITIVENESS INDEX (BCI)

Single composite score (0-100) measuring Brazil's export competitiveness
for soybeans and corn. Higher = Brazil more competitive.

Components (weighted):
  1. FX Component (30%)     — BRL weakness vs USD (weaker BRL = more competitive)
  2. Basis Component (20%)  — Santos premium vs Gulf basis spread
  3. Freight Component (15%) — BR interior + ocean vs US barge + ocean
  4. Selling Pace (15%)     — IMEA comercialização vs seasonal norm
  5. FOB Premium (10%)      — Santos FOB vs Gulf FOB spread
  6. Crush Margin (10%)     — BR crush economics vs US

Each component is normalized to 0-100 using historical percentiles,
then weighted to produce the final BCI.

Signal Interpretation:
  80-100: STRONG — Brazil highly competitive, expect large exports
  60-79:  MODERATE — Brazil competitive, normal flow
  40-59:  NEUTRAL — Neither origin dominant
  20-39:  WEAK — US more competitive, BR exports slow
  0-19:   VERY WEAK — Brazil uncompetitive, expect demand shift to US

Data Sources:
  BCB/SGS (PTAX), CEPEA, IMEA, USDA GTR, Comex Stat, CME/CBOT
"""

import json
import os
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import Optional, Dict, List

# ============================================================
# HISTORICAL RANGES (for percentile normalization)
# Based on 2019-2025 weekly observations
# ============================================================

HISTORICAL_RANGES = {
    "ptax": {
        "min": 3.70,    # strongest BRL (2019)
        "max": 6.40,    # weakest BRL (2024)
        "mean": 5.15,
        "std": 0.55,
        "note": "Higher PTAX = weaker BRL = more competitive for BR exports"
    },
    "basis_spread_cents_bu": {
        # Santos premium minus Gulf basis (both in ¢/bu)
        # Negative = Santos cheaper = BR more competitive
        "min": -120,    # Santos deeply discounted
        "max": 60,      # Santos at premium (rare)
        "mean": -35,
        "std": 30,
        "note": "More negative = BR more competitive"
    },
    "freight_spread_usd_mt": {
        # US total freight minus BR total freight (to Shanghai)
        # Positive = US freight higher = BR advantage
        "min": -15,     # BR freight higher (rare)
        "max": 35,      # US freight much higher
        "mean": 12,
        "std": 8,
        "note": "Higher = BR freight advantage"
    },
    "selling_pace_deviation_pp": {
        # IMEA comercialização minus seasonal average
        # Negative = farmer holding = tighter supply = less competitive short-term
        # But also signals farmer bullishness
        "min": -25,     # far behind seasonal (holding)
        "max": 20,      # far ahead (selling fast)
        "mean": 0,
        "std": 8,
        "note": "More positive = more supply available = more competitive"
    },
    "fob_spread_usd_mt": {
        # Gulf FOB minus Santos FOB (both in USD/mt)
        # Positive = Santos cheaper = BR more competitive
        "min": -40,
        "max": 60,
        "mean": 10,
        "std": 18,
        "note": "Higher = BR FOB cheaper"
    },
    "crush_margin_spread_usd_mt": {
        # BR crush margin minus US crush margin
        # Positive = BR crush more profitable = more demand for BR beans
        "min": -25,
        "max": 30,
        "mean": 5,
        "std": 10,
        "note": "Higher = BR crush more attractive"
    },
}

# Component weights (must sum to 1.0)
WEIGHTS = {
    "fx": 0.30,
    "basis": 0.20,
    "freight": 0.15,
    "selling_pace": 0.15,
    "fob_premium": 0.10,
    "crush_margin": 0.10,
}


# ============================================================
# NORMALIZATION
# ============================================================

def normalize_to_score(value: float, hist_key: str, invert: bool = False) -> float:
    """
    Normalize a raw value to 0-100 score using historical distribution.
    Uses Z-score approach capped at 0-100.
    
    invert=True means lower raw value = higher score
    """
    hist = HISTORICAL_RANGES.get(hist_key)
    if not hist:
        return 50.0  # neutral if no history
    
    mean = hist["mean"]
    std = hist["std"]
    
    if std <= 0:
        return 50.0
    
    z = (value - mean) / std
    
    if invert:
        z = -z
    
    # Convert Z-score to 0-100 scale
    # Z of -2.5 → 0, Z of 0 → 50, Z of +2.5 → 100
    score = 50 + (z * 20)
    
    return max(0, min(100, round(score, 1)))


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class BCIComponent:
    """Single component of the BCI"""
    name: str
    raw_value: float
    score: float          # 0-100 normalized
    weight: float
    weighted_score: float  # score * weight
    direction: str        # what does higher raw value mean
    signal: str           # "BULLISH" / "NEUTRAL" / "BEARISH" for BR
    
    def __post_init__(self):
        self.weighted_score = round(self.score * self.weight, 2)
        if self.score >= 65:
            self.signal = "BULLISH"
        elif self.score <= 35:
            self.signal = "BEARISH"
        else:
            self.signal = "NEUTRAL"


@dataclass
class BrazilCompetitivenessIndex:
    """The composite indicator"""
    date: str
    commodity: str
    bci_score: float = 0.0
    bci_signal: str = ""
    bci_trend: str = ""           # vs previous week
    
    components: List[BCIComponent] = field(default_factory=list)
    
    # Top contributors
    strongest_component: str = ""
    weakest_component: str = ""
    
    # Context
    ptax: float = 0.0
    cbot_cents_bu: float = 0.0
    
    def calculate(self):
        if not self.components:
            return self
        
        self.bci_score = round(
            sum(c.weighted_score for c in self.components), 1
        )
        
        # Signal
        if self.bci_score >= 80:
            self.bci_signal = "STRONG"
        elif self.bci_score >= 60:
            self.bci_signal = "MODERATE"
        elif self.bci_score >= 40:
            self.bci_signal = "NEUTRAL"
        elif self.bci_score >= 20:
            self.bci_signal = "WEAK"
        else:
            self.bci_signal = "VERY_WEAK"
        
        # Top contributors
        sorted_comps = sorted(self.components, key=lambda c: c.score, reverse=True)
        self.strongest_component = sorted_comps[0].name
        self.weakest_component = sorted_comps[-1].name
        
        return self


# ============================================================
# CALCULATOR
# ============================================================

def calculate_bci(
    # FX
    ptax: float,
    
    # Basis
    santos_premium_cents_bu: float = 0.0,   # Santos premium (negative = discount)
    gulf_basis_cents_bu: float = 0.0,        # Gulf basis
    
    # Freight
    br_total_freight_usd_mt: float = 0.0,   # Sorriso→Santos→Shanghai
    us_total_freight_usd_mt: float = 0.0,    # IL→Gulf→Shanghai
    
    # Selling pace
    imea_comercializacao_pct: float = 0.0,   # current selling pace
    seasonal_avg_comercializacao: float = 0.0, # what's normal for this time
    
    # FOB
    santos_fob_usd_mt: float = 0.0,
    gulf_fob_usd_mt: float = 0.0,
    
    # Crush
    br_crush_margin_usd_mt: float = 0.0,
    us_crush_margin_usd_mt: float = 0.0,
    
    # Context
    cbot_cents_bu: float = 0.0,
    commodity: str = "soybeans",
    ref_date: str = "",
    previous_bci: float = 0.0,
) -> BrazilCompetitivenessIndex:
    """Calculate the Brazil Competitiveness Index."""
    
    if not ref_date:
        ref_date = date.today().isoformat()
    
    components = []
    
    # 1. FX Component (30%)
    # Higher PTAX = weaker BRL = more competitive
    fx_score = normalize_to_score(ptax, "ptax", invert=False)
    components.append(BCIComponent(
        name="FX (BRL/USD)",
        raw_value=ptax,
        score=fx_score,
        weight=WEIGHTS["fx"],
        weighted_score=0,
        direction="Higher PTAX → BR more competitive",
        signal="",
    ))
    
    # 2. Basis Component (20%)
    # Basis spread = Santos premium - Gulf basis
    # More negative = Santos cheaper = BR more competitive
    basis_spread = santos_premium_cents_bu - gulf_basis_cents_bu
    basis_score = normalize_to_score(basis_spread, "basis_spread_cents_bu", invert=True)
    components.append(BCIComponent(
        name="Basis Spread",
        raw_value=basis_spread,
        score=basis_score,
        weight=WEIGHTS["basis"],
        weighted_score=0,
        direction="More negative → BR more competitive",
        signal="",
    ))
    
    # 3. Freight Component (15%)
    # Freight spread = US total - BR total
    # Higher = BR freight advantage
    freight_spread = us_total_freight_usd_mt - br_total_freight_usd_mt
    freight_score = normalize_to_score(freight_spread, "freight_spread_usd_mt", invert=False)
    components.append(BCIComponent(
        name="Freight Advantage",
        raw_value=freight_spread,
        score=freight_score,
        weight=WEIGHTS["freight"],
        weighted_score=0,
        direction="Higher → BR freight cheaper",
        signal="",
    ))
    
    # 4. Selling Pace (15%)
    # Deviation from seasonal = current - average
    # Positive = farmer selling faster = more supply = competitive
    pace_deviation = imea_comercializacao_pct - seasonal_avg_comercializacao
    pace_score = normalize_to_score(pace_deviation, "selling_pace_deviation_pp", invert=False)
    components.append(BCIComponent(
        name="Farmer Selling Pace",
        raw_value=pace_deviation,
        score=pace_score,
        weight=WEIGHTS["selling_pace"],
        weighted_score=0,
        direction="Positive → more supply available",
        signal="",
    ))
    
    # 5. FOB Premium (10%)
    # FOB spread = Gulf FOB - Santos FOB
    # Positive = Santos cheaper = competitive
    fob_spread = gulf_fob_usd_mt - santos_fob_usd_mt
    fob_score = normalize_to_score(fob_spread, "fob_spread_usd_mt", invert=False)
    components.append(BCIComponent(
        name="FOB Spread",
        raw_value=fob_spread,
        score=fob_score,
        weight=WEIGHTS["fob_premium"],
        weighted_score=0,
        direction="Higher → Santos FOB cheaper",
        signal="",
    ))
    
    # 6. Crush Margin (10%)
    # Crush spread = BR margin - US margin
    # Positive = BR crush more profitable
    crush_spread = br_crush_margin_usd_mt - us_crush_margin_usd_mt
    crush_score = normalize_to_score(crush_spread, "crush_margin_spread_usd_mt", invert=False)
    components.append(BCIComponent(
        name="Crush Margin",
        raw_value=crush_spread,
        score=crush_score,
        weight=WEIGHTS["crush_margin"],
        weighted_score=0,
        direction="Higher → BR crush more attractive",
        signal="",
    ))
    
    # Build index
    bci = BrazilCompetitivenessIndex(
        date=ref_date,
        commodity=commodity,
        components=components,
        ptax=ptax,
        cbot_cents_bu=cbot_cents_bu,
    ).calculate()
    
    # Trend
    if previous_bci > 0:
        diff = bci.bci_score - previous_bci
        if diff > 2:
            bci.bci_trend = "IMPROVING"
        elif diff < -2:
            bci.bci_trend = "DETERIORATING"
        else:
            bci.bci_trend = "STABLE"
    
    return bci


# ============================================================
# OUTPUT FORMATTERS
# ============================================================

def to_json(bci: BrazilCompetitivenessIndex) -> str:
    output = {
        "indicator": "BCI",
        "version": "1.0",
        "generated_at": datetime.now().isoformat() + "Z",
        "data": {
            "date": bci.date,
            "commodity": bci.commodity,
            "bci_score": bci.bci_score,
            "bci_signal": bci.bci_signal,
            "bci_trend": bci.bci_trend,
            "components": [
                {
                    "name": c.name,
                    "raw_value": round(c.raw_value, 2),
                    "score": c.score,
                    "weight_pct": int(c.weight * 100),
                    "weighted_score": c.weighted_score,
                    "signal": c.signal,
                }
                for c in bci.components
            ],
            "strongest": bci.strongest_component,
            "weakest": bci.weakest_component,
            "context": {
                "ptax": bci.ptax,
                "cbot_cents_bu": bci.cbot_cents_bu,
            },
        },
        "interpretation": {
            "signal": bci.bci_signal,
            "description": _description(bci),
        }
    }
    return json.dumps(output, indent=2)


def _description(bci: BrazilCompetitivenessIndex) -> str:
    descs = {
        "STRONG": "Brazil highly competitive — expect strong export flows and Santos congestion",
        "MODERATE": "Brazil competitive — normal to above-average export pace expected",
        "NEUTRAL": "Neither origin dominant — watch for catalysts (FX, weather, policy)",
        "WEAK": "US more competitive — BR exports may slow, farmer likely holding",
        "VERY_WEAK": "Brazil uncompetitive — demand shifts to US origins, bearish BR premiums",
    }
    base = descs.get(bci.bci_signal, "")
    return f"{base}. Strongest factor: {bci.strongest_component}. Weakest: {bci.weakest_component}."


def to_report_text(bci: BrazilCompetitivenessIndex) -> str:
    lines = [
        "=" * 60,
        f"  BRAZIL COMPETITIVENESS INDEX — {bci.commodity.upper()}",
        "  AgriMacro Intelligence",
        "=" * 60,
        f"  Date: {bci.date}",
        "",
    ]
    
    # Score gauge
    gauge_pos = int(bci.bci_score / 2)  # 0-50 chars
    gauge = "·" * 50
    gauge = gauge[:gauge_pos] + "▼" + gauge[gauge_pos + 1:]
    lines.extend([
        f"  BCI SCORE:  {bci.bci_score:.1f} / 100  [{bci.bci_signal}]",
        f"  [{'·' * 10}|{'·' * 10}|{'·' * 10}|{'·' * 10}|{'·' * 10}]",
        f"  [{gauge}]",
        f"   0    20   40   60   80  100",
        f"   WEAK      NEUTRAL   STRONG",
        "",
    ])
    
    if bci.bci_trend:
        lines.append(f"  Trend: {bci.bci_trend}")
        lines.append("")
    
    # Components
    lines.append("  COMPONENTS:")
    lines.append("  " + "-" * 56)
    lines.append(f"  {'Component':<22} {'Raw':>8} {'Score':>6} {'Wt':>4} {'WtScore':>8} {'Signal':>8}")
    lines.append("  " + "-" * 56)
    
    for c in sorted(bci.components, key=lambda x: x.weighted_score, reverse=True):
        lines.append(
            f"  {c.name:<22} {c.raw_value:>8.2f} {c.score:>6.1f} {int(c.weight*100):>3}% {c.weighted_score:>8.2f} {c.signal:>8}"
        )
    
    lines.extend([
        "  " + "-" * 56,
        f"  {'TOTAL':<22} {'':>8} {'':>6} {'':>4} {bci.bci_score:>8.1f}",
        "",
        f"  Strongest: {bci.strongest_component}",
        f"  Weakest:   {bci.weakest_component}",
        "",
        f"  CBOT: {bci.cbot_cents_bu:.0f}¢/bu  |  PTAX: R$ {bci.ptax:.2f}",
        "=" * 60,
    ])
    
    return "\n".join(lines)


def to_dashboard_card(bci: BrazilCompetitivenessIndex) -> dict:
    # Color based on score
    if bci.bci_score >= 60:
        color = "#00C878"  # green
    elif bci.bci_score >= 40:
        color = "#DCB432"  # gold
    else:
        color = "#DC3C3C"  # red
    
    return {
        "card_type": "bci",
        "title": f"Brazil Competitiveness Index",
        "subtitle": f"{bci.commodity.title()} | {bci.bci_signal}",
        "date": bci.date,
        "primary_value": f"{bci.bci_score:.0f}",
        "primary_max": 100,
        "primary_color": color,
        "trend": bci.bci_trend,
        "gauge": {
            "value": bci.bci_score,
            "zones": [
                {"min": 0, "max": 20, "color": "#DC3C3C", "label": "Very Weak"},
                {"min": 20, "max": 40, "color": "#E8734A", "label": "Weak"},
                {"min": 40, "max": 60, "color": "#DCB432", "label": "Neutral"},
                {"min": 60, "max": 80, "color": "#7DD87D", "label": "Moderate"},
                {"min": 80, "max": 100, "color": "#00C878", "label": "Strong"},
            ],
        },
        "components": [
            {
                "name": c.name,
                "score": c.score,
                "weight_pct": int(c.weight * 100),
                "signal": c.signal,
                "bar_color": "#00C878" if c.signal == "BULLISH" else "#DC3C3C" if c.signal == "BEARISH" else "#DCB432",
            }
            for c in bci.components
        ],
        "strongest": bci.strongest_component,
        "weakest": bci.weakest_component,
    }


# ============================================================
# INTEGRATED PIPELINE
# ============================================================

def load_from_collectors(data_dir: str = "./data") -> Optional[BrazilCompetitivenessIndex]:
    """Load from all collector outputs and calculate BCI."""
    try:
        # PTAX
        with open(os.path.join(data_dir, "bcb_ptax.json")) as f:
            ptax = json.load(f).get("ptax", 5.50)
        
        # IMEA
        imea_file = os.path.join(data_dir, "imea_soja.json")
        imea_comercializacao = 0
        santos_premium = 0
        if os.path.exists(imea_file):
            with open(imea_file) as f:
                imea = json.load(f)
            imea_comercializacao = imea.get("comercializacao_safra_atual_pct", 0)
            santos_premium = imea.get("premio_santos_cents_bu", 0)
        
        # USDA GTR
        gtr_file = os.path.join(data_dir, "usda_gtr.json")
        gulf_basis = 50
        us_freight = 73  # barge + ocean
        if os.path.exists(gtr_file):
            with open(gtr_file) as f:
                gtr = json.load(f)
            gulf_basis = gtr.get("gulf_basis_cents_bu", 50)
            barge = gtr.get("barge_freight_usd_mt", 25)
            ocean_us = gtr.get("ocean_gulf_shanghai_usd_mt", 48)
            us_freight = barge + ocean_us
        
        # BR transport
        br_transport_file = os.path.join(data_dir, "usda_brazil_transport.json")
        br_freight = 65
        if os.path.exists(br_transport_file):
            with open(br_transport_file) as f:
                br_t = json.load(f)
            interior = br_t.get("sorriso_santos_usd_mt", 32)
            ocean_br = br_t.get("santos_shanghai_usd_mt", 33)
            br_freight = interior + ocean_br
        
        # CBOT
        cbot = 1000
        ibkr_file = os.path.join(data_dir, "ibkr_quotes.json")
        if os.path.exists(ibkr_file):
            with open(ibkr_file) as f:
                ibkr = json.load(f)
            cbot = ibkr.get("ZS", {}).get("last", 10.0) * 100
        
        # FOB prices (derived)
        BU_PER_MT = 36.7437
        gulf_fob = (cbot + gulf_basis) / 100 * BU_PER_MT
        santos_fob = gulf_fob - 10  # approximate from basis
        
        bci = calculate_bci(
            ptax=ptax,
            santos_premium_cents_bu=santos_premium,
            gulf_basis_cents_bu=gulf_basis,
            br_total_freight_usd_mt=br_freight,
            us_total_freight_usd_mt=us_freight,
            imea_comercializacao_pct=imea_comercializacao,
            seasonal_avg_comercializacao=0,  # would need historical
            santos_fob_usd_mt=santos_fob,
            gulf_fob_usd_mt=gulf_fob,
            br_crush_margin_usd_mt=45,  # default
            us_crush_margin_usd_mt=40,  # default
            cbot_cents_bu=cbot,
        )
        
        return bci
        
    except Exception as e:
        print(f"[BCI] Error: {e}")
        return None


# ============================================================
# DEMO
# ============================================================

def demo():
    """Demo with representative market scenarios."""
    
    scenarios = [
        {
            "name": "Harvest Peak (Mar 2026) — BRL Weak",
            "ptax": 6.10,
            "santos_premium": -55,
            "gulf_basis": 50,
            "br_freight": 62,
            "us_freight": 76,
            "comercializacao": 35,
            "seasonal_avg": 30,
            "santos_fob": 385,
            "gulf_fob": 420,
            "br_crush": 52,
            "us_crush": 38,
            "cbot": 1040,
        },
        {
            "name": "US Harvest (Oct 2025) — BRL Strong",
            "ptax": 5.10,
            "santos_premium": -20,
            "gulf_basis": 65,
            "br_freight": 65,
            "us_freight": 70,
            "comercializacao": 88,
            "seasonal_avg": 85,
            "santos_fob": 410,
            "gulf_fob": 415,
            "br_crush": 40,
            "us_crush": 48,
            "cbot": 985,
        },
        {
            "name": "Trade War Scenario — Tariffs Active",
            "ptax": 5.80,
            "santos_premium": -90,
            "gulf_basis": 25,
            "br_freight": 60,
            "us_freight": 78,
            "comercializacao": 25,
            "seasonal_avg": 30,
            "santos_fob": 360,
            "gulf_fob": 380,
            "br_crush": 58,
            "us_crush": 30,
            "cbot": 920,
        },
        {
            "name": "BR Drought — Supply Shock",
            "ptax": 5.50,
            "santos_premium": 15,
            "gulf_basis": 55,
            "br_freight": 70,
            "us_freight": 72,
            "comercializacao": 15,
            "seasonal_avg": 30,
            "santos_fob": 440,
            "gulf_fob": 430,
            "br_crush": 35,
            "us_crush": 50,
            "cbot": 1150,
        },
    ]
    
    print("\n" + "=" * 60)
    print("  AgriMacro BCI — Scenario Analysis")
    print("=" * 60)
    
    prev_bci = 0
    
    for s in scenarios:
        bci = calculate_bci(
            ptax=s["ptax"],
            santos_premium_cents_bu=s["santos_premium"],
            gulf_basis_cents_bu=s["gulf_basis"],
            br_total_freight_usd_mt=s["br_freight"],
            us_total_freight_usd_mt=s["us_freight"],
            imea_comercializacao_pct=s["comercializacao"],
            seasonal_avg_comercializacao=s["seasonal_avg"],
            santos_fob_usd_mt=s["santos_fob"],
            gulf_fob_usd_mt=s["gulf_fob"],
            br_crush_margin_usd_mt=s["br_crush"],
            us_crush_margin_usd_mt=s["us_crush"],
            cbot_cents_bu=s["cbot"],
            ref_date="2026-02-16",
            previous_bci=prev_bci,
        )
        
        print(f"\n  >>> SCENARIO: {s['name']}")
        print(to_report_text(bci))
        prev_bci = bci.bci_score
    
    # Summary comparison
    print("\n" + "=" * 60)
    print("  SCENARIO COMPARISON")
    print("=" * 60)
    print(f"  {'Scenario':<40} {'BCI':>5} {'Signal':>12}")
    print("  " + "-" * 58)
    
    for s in scenarios:
        bci = calculate_bci(
            ptax=s["ptax"],
            santos_premium_cents_bu=s["santos_premium"],
            gulf_basis_cents_bu=s["gulf_basis"],
            br_total_freight_usd_mt=s["br_freight"],
            us_total_freight_usd_mt=s["us_freight"],
            imea_comercializacao_pct=s["comercializacao"],
            seasonal_avg_comercializacao=s["seasonal_avg"],
            santos_fob_usd_mt=s["santos_fob"],
            gulf_fob_usd_mt=s["gulf_fob"],
            br_crush_margin_usd_mt=s["br_crush"],
            us_crush_margin_usd_mt=s["us_crush"],
            cbot_cents_bu=s["cbot"],
        )
        print(f"  {s['name']:<40} {bci.bci_score:>5.1f} {bci.bci_signal:>12}")
    
    # JSON sample
    print("\n\n--- Dashboard Card (Harvest Peak) ---")
    bci = calculate_bci(
        ptax=6.10, santos_premium_cents_bu=-55, gulf_basis_cents_bu=50,
        br_total_freight_usd_mt=62, us_total_freight_usd_mt=76,
        imea_comercializacao_pct=35, seasonal_avg_comercializacao=30,
        santos_fob_usd_mt=385, gulf_fob_usd_mt=420,
        br_crush_margin_usd_mt=52, us_crush_margin_usd_mt=38,
        cbot_cents_bu=1040, ref_date="2026-02-16",
    )
    print(json.dumps(to_dashboard_card(bci), indent=2))


if __name__ == "__main__":
    demo()
