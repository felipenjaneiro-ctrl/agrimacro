#!/usr/bin/env python3
"""
AgriMacro Intelligence — Proprietary Indicator #2
EXPORT RACE TRACKER (ERT)

Compares year-to-date export accumulation: Brazil vs USA
Shows who is winning the race for global (especially Chinese) market share.

Data Sources:
  BR side:  Comex Stat (MDIC) — monthly exports by NCM/destination/port
  US side:  USDA Export Inspections — weekly actual shipments
            USDA Export Sales — weekly commitments (outstanding + accumulated)
  Context:  WASDE annual projections as "finish line"

Key Metrics:
  - YTD accumulated volume (MT) — BR vs US
  - Pace vs WASDE projection (% of annual target)
  - Market share shift (BR share of BR+US total)
  - Seasonal pace comparison (current year vs 5-year avg)
  - China concentration (% of total going to China)

Marketing Year:
  Soybeans: Sep 1 → Aug 31 (US), Feb 1 → Jan 31 (BR)
  Corn:     Sep 1 → Aug 31 (US), Feb 1 → Jan 31 (BR)
  Note: Different marketing years mean "race" comparison uses
        calendar year or rolling 12-month windows.

Conversion:
  1 metric ton = 36.7437 bushels (soybeans)
  1 metric ton = 39.368 bushels (corn)
"""

import json
import os
from datetime import datetime, date
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict

# ============================================================
# CONSTANTS
# ============================================================

BU_PER_MT_SOYBEAN = 36.7437
BU_PER_MT_CORN = 39.368

# WASDE projections (updated monthly — these are defaults)
# Will be overridden by actual WASDE data when available
DEFAULT_WASDE = {
    "soybeans": {
        "us_exports_mmt": 49.67,       # 2025/26 WASDE Feb estimate
        "br_exports_mmt": 105.50,      # 2025/26 WASDE Feb estimate
        "world_trade_mmt": 181.77,
    },
    "corn": {
        "us_exports_mmt": 62.23,       # 2025/26
        "br_exports_mmt": 46.00,       # 2025/26
        "world_trade_mmt": 192.31,
    },
}

# Marketing year months (start month)
MARKETING_YEAR = {
    "soybeans": {"us_start": 9, "br_start": 2},  # Sep, Feb
    "corn": {"us_start": 9, "br_start": 2},       # Sep, Feb
}

# Typical seasonal export patterns (% of annual by month)
# Based on 5-year averages
SEASONAL_PATTERN = {
    "soybeans": {
        "us": {9: 8, 10: 15, 11: 14, 12: 10, 1: 8, 2: 6, 3: 5, 4: 5, 5: 6, 6: 8, 7: 8, 8: 7},
        "br": {2: 3, 3: 12, 4: 16, 5: 15, 6: 12, 7: 10, 8: 8, 9: 6, 10: 5, 11: 5, 12: 4, 1: 4},
    },
    "corn": {
        "us": {9: 6, 10: 12, 11: 12, 12: 10, 1: 8, 2: 7, 3: 7, 4: 6, 5: 5, 6: 5, 7: 8, 8: 14},
        "br": {2: 2, 3: 3, 4: 3, 5: 4, 6: 6, 7: 14, 8: 18, 9: 16, 10: 14, 11: 10, 12: 6, 1: 4},
    },
}


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class MonthlyExport:
    """Single month of export data"""
    year: int
    month: int
    volume_mt: float           # metric tons
    value_usd: float = 0.0    # FOB value
    china_volume_mt: float = 0.0   # volume to China specifically
    avg_price_usd_mt: float = 0.0  # implied FOB price
    
    def __post_init__(self):
        if self.volume_mt > 0 and self.value_usd > 0:
            self.avg_price_usd_mt = self.value_usd / self.volume_mt


@dataclass
class ExportAccumulation:
    """Year-to-date accumulation for one origin"""
    origin: str                     # "US" or "BR"
    commodity: str                  # "soybeans" or "corn"
    marketing_year: str             # "2025/26"
    months_data: List[MonthlyExport] = field(default_factory=list)
    
    # Calculated fields
    ytd_volume_mmt: float = 0.0    # accumulated volume (million MT)
    ytd_china_mmt: float = 0.0     # accumulated to China
    china_share_pct: float = 0.0   # % of exports going to China
    wasde_target_mmt: float = 0.0  # WASDE annual projection
    pace_pct: float = 0.0          # % of WASDE target achieved
    expected_pace_pct: float = 0.0 # seasonal expected pace at this point
    pace_vs_seasonal: float = 0.0  # ahead (+) or behind (-) seasonal
    months_elapsed: int = 0
    
    def calculate(self, wasde_target: float = 0.0, current_month: int = 0):
        """Calculate all derived fields"""
        self.wasde_target_mmt = wasde_target
        
        total_vol = sum(m.volume_mt for m in self.months_data)
        total_china = sum(m.china_volume_mt for m in self.months_data)
        
        self.ytd_volume_mmt = round(total_vol / 1_000_000, 2)
        self.ytd_china_mmt = round(total_china / 1_000_000, 2)
        self.china_share_pct = round(
            (total_china / total_vol * 100) if total_vol > 0 else 0, 1
        )
        
        self.months_elapsed = len(self.months_data)
        
        if wasde_target > 0:
            self.pace_pct = round(self.ytd_volume_mmt / wasde_target * 100, 1)
        
        # Calculate expected seasonal pace
        my = MARKETING_YEAR.get(self.commodity, {})
        start_month = my.get(f"{self.origin.lower()}_start", 1)
        pattern = SEASONAL_PATTERN.get(self.commodity, {}).get(
            self.origin.lower(), {}
        )
        
        if pattern and current_month > 0:
            # Sum seasonal percentages from start to current month
            expected = 0
            m = start_month
            while True:
                expected += pattern.get(m, 0)
                if m == current_month:
                    break
                m = (m % 12) + 1
                if m == start_month:  # safety: full year
                    break
            self.expected_pace_pct = expected
            self.pace_vs_seasonal = round(self.pace_pct - expected, 1)
        
        return self


@dataclass
class ExportRace:
    """The proprietary indicator — bilateral comparison"""
    date: str
    commodity: str                      # "soybeans" or "corn"
    marketing_year: str
    
    # Accumulations
    us: Optional[ExportAccumulation] = None
    br: Optional[ExportAccumulation] = None
    
    # Race metrics
    br_us_total_mmt: float = 0.0       # Combined BR+US exports
    br_market_share_pct: float = 0.0   # BR share of BR+US
    us_market_share_pct: float = 0.0   # US share of BR+US
    share_shift_pp: float = 0.0        # BR share change vs 5yr avg
    
    # Race leader
    leader: str = ""                    # "BR" or "US"
    lead_mmt: float = 0.0              # Leader's advantage in MMT
    lead_pace_pct: float = 0.0         # Leader's pace advantage
    
    # China race
    china_total_mmt: float = 0.0
    br_china_share_pct: float = 0.0
    us_china_share_pct: float = 0.0
    
    # Signals
    pace_signal_us: str = ""           # "AHEAD" / "ON_PACE" / "BEHIND"
    pace_signal_br: str = ""
    
    def calculate(self):
        if not self.us or not self.br:
            return self
        
        # Combined totals
        self.br_us_total_mmt = round(
            self.us.ytd_volume_mmt + self.br.ytd_volume_mmt, 2
        )
        
        if self.br_us_total_mmt > 0:
            self.br_market_share_pct = round(
                self.br.ytd_volume_mmt / self.br_us_total_mmt * 100, 1
            )
            self.us_market_share_pct = round(
                self.us.ytd_volume_mmt / self.br_us_total_mmt * 100, 1
            )
        
        # Historical avg market shares (5yr avg)
        historical_br_share = {
            "soybeans": 68.0,  # BR typically ~68% of BR+US soy exports
            "corn": 42.0,      # BR typically ~42% of BR+US corn exports
        }
        avg_share = historical_br_share.get(self.commodity, 50)
        self.share_shift_pp = round(self.br_market_share_pct - avg_share, 1)
        
        # Race leader (by pace vs target, not raw volume)
        if self.us.pace_pct > 0 and self.br.pace_pct > 0:
            if self.br.pace_pct > self.us.pace_pct:
                self.leader = "BR"
                self.lead_pace_pct = round(self.br.pace_pct - self.us.pace_pct, 1)
            else:
                self.leader = "US"
                self.lead_pace_pct = round(self.us.pace_pct - self.br.pace_pct, 1)
        
        self.lead_mmt = round(
            abs(self.br.ytd_volume_mmt - self.us.ytd_volume_mmt), 2
        )
        
        # China race
        self.china_total_mmt = round(
            self.us.ytd_china_mmt + self.br.ytd_china_mmt, 2
        )
        if self.china_total_mmt > 0:
            self.br_china_share_pct = round(
                self.br.ytd_china_mmt / self.china_total_mmt * 100, 1
            )
            self.us_china_share_pct = round(
                self.us.ytd_china_mmt / self.china_total_mmt * 100, 1
            )
        
        # Pace signals
        for origin, accum in [("us", self.us), ("br", self.br)]:
            if accum.pace_vs_seasonal > 3:
                signal = "AHEAD"
            elif accum.pace_vs_seasonal < -3:
                signal = "BEHIND"
            else:
                signal = "ON_PACE"
            setattr(self, f"pace_signal_{origin}", signal)
        
        return self


# ============================================================
# CALCULATOR
# ============================================================

def calculate_export_race(
    commodity: str,
    marketing_year: str,
    us_monthly: List[Dict],          # [{year, month, volume_mt, value_usd, china_volume_mt}]
    br_monthly: List[Dict],          # same format
    wasde_us_mmt: float = 0.0,
    wasde_br_mmt: float = 0.0,
    current_month: int = 0,
    ref_date: str = "",
) -> ExportRace:
    """
    Calculate the Export Race Tracker.
    
    us_monthly/br_monthly: list of dicts with keys:
        year, month, volume_mt, value_usd (optional), china_volume_mt (optional)
    """
    if not ref_date:
        ref_date = date.today().isoformat()
    if not current_month:
        current_month = date.today().month
    
    # Default WASDE if not provided
    wasde = DEFAULT_WASDE.get(commodity, {})
    if wasde_us_mmt <= 0:
        wasde_us_mmt = wasde.get("us_exports_mmt", 0)
    if wasde_br_mmt <= 0:
        wasde_br_mmt = wasde.get("br_exports_mmt", 0)
    
    # Build US accumulation
    us_months = [MonthlyExport(**m) for m in us_monthly]
    us_accum = ExportAccumulation(
        origin="US",
        commodity=commodity,
        marketing_year=marketing_year,
        months_data=us_months,
    ).calculate(wasde_target=wasde_us_mmt, current_month=current_month)
    
    # Build BR accumulation
    br_months = [MonthlyExport(**m) for m in br_monthly]
    br_accum = ExportAccumulation(
        origin="BR",
        commodity=commodity,
        marketing_year=marketing_year,
        months_data=br_months,
    ).calculate(wasde_target=wasde_br_mmt, current_month=current_month)
    
    # Build race
    race = ExportRace(
        date=ref_date,
        commodity=commodity,
        marketing_year=marketing_year,
        us=us_accum,
        br=br_accum,
    ).calculate()
    
    return race


# ============================================================
# OUTPUT FORMATTERS
# ============================================================

def to_json(race: ExportRace) -> str:
    """Export as JSON for dashboard/API"""
    output = {
        "indicator": "EXPORT_RACE_TRACKER",
        "version": "1.0",
        "generated_at": datetime.now().isoformat() + "Z",
        "data": {
            "date": race.date,
            "commodity": race.commodity,
            "marketing_year": race.marketing_year,
            "race_summary": {
                "leader": race.leader,
                "lead_pace_pct": race.lead_pace_pct,
                "lead_volume_mmt": race.lead_mmt,
                "br_market_share_pct": race.br_market_share_pct,
                "us_market_share_pct": race.us_market_share_pct,
                "share_shift_vs_5yr_pp": race.share_shift_pp,
            },
            "us": {
                "ytd_volume_mmt": race.us.ytd_volume_mmt,
                "wasde_target_mmt": race.us.wasde_target_mmt,
                "pace_pct": race.us.pace_pct,
                "expected_pace_pct": race.us.expected_pace_pct,
                "pace_vs_seasonal": race.us.pace_vs_seasonal,
                "pace_signal": race.pace_signal_us,
                "china_share_pct": race.us.china_share_pct,
                "months_reported": race.us.months_elapsed,
            },
            "br": {
                "ytd_volume_mmt": race.br.ytd_volume_mmt,
                "wasde_target_mmt": race.br.wasde_target_mmt,
                "pace_pct": race.br.pace_pct,
                "expected_pace_pct": race.br.expected_pace_pct,
                "pace_vs_seasonal": race.br.pace_vs_seasonal,
                "pace_signal": race.pace_signal_br,
                "china_share_pct": race.br.china_share_pct,
                "months_reported": race.br.months_elapsed,
            },
            "china_race": {
                "total_mmt": race.china_total_mmt,
                "br_share_pct": race.br_china_share_pct,
                "us_share_pct": race.us_china_share_pct,
            },
        },
        "interpretation": {
            "headline": _headline(race),
            "market_share_story": _share_story(race),
            "pace_story": _pace_story(race),
        }
    }
    return json.dumps(output, indent=2)


def _headline(race: ExportRace) -> str:
    comm = race.commodity.title()
    if race.leader == "BR":
        return (f"Brazil leads {comm} export race by "
                f"{race.lead_pace_pct:.1f}pp pace advantage "
                f"({race.br.ytd_volume_mmt:.1f} vs {race.us.ytd_volume_mmt:.1f} MMT YTD)")
    else:
        return (f"US leads {comm} export race by "
                f"{race.lead_pace_pct:.1f}pp pace advantage "
                f"({race.us.ytd_volume_mmt:.1f} vs {race.br.ytd_volume_mmt:.1f} MMT YTD)")


def _share_story(race: ExportRace) -> str:
    direction = "gaining" if race.share_shift_pp > 0 else "losing"
    return (f"Brazil holds {race.br_market_share_pct:.1f}% of combined BR+US "
            f"{race.commodity} exports, {direction} "
            f"{abs(race.share_shift_pp):.1f}pp vs 5-year average")


def _pace_story(race: ExportRace) -> str:
    parts = []
    for origin, accum, signal in [
        ("US", race.us, race.pace_signal_us),
        ("BR", race.br, race.pace_signal_br),
    ]:
        if signal == "AHEAD":
            parts.append(f"{origin} running {accum.pace_vs_seasonal:+.1f}pp ahead of seasonal")
        elif signal == "BEHIND":
            parts.append(f"{origin} lagging {abs(accum.pace_vs_seasonal):.1f}pp behind seasonal")
        else:
            parts.append(f"{origin} on pace with seasonal norms")
    return ". ".join(parts)


def to_report_text(race: ExportRace) -> str:
    """Format for PDF report or video narration"""
    comm = race.commodity.upper()
    flag_br = "🇧🇷"
    flag_us = "🇺🇸"
    
    lines = [
        "=" * 60,
        f"  EXPORT RACE TRACKER — {comm} {race.marketing_year}",
        "  AgriMacro Intelligence",
        "=" * 60,
        f"  Date: {race.date}",
        "",
        f"  {flag_us} US  YTD:  {race.us.ytd_volume_mmt:>8.2f} MMT"
        f"  ({race.us.pace_pct:>5.1f}% of WASDE {race.us.wasde_target_mmt:.1f})",
        f"  {flag_br} BR  YTD:  {race.br.ytd_volume_mmt:>8.2f} MMT"
        f"  ({race.br.pace_pct:>5.1f}% of WASDE {race.br.wasde_target_mmt:.1f})",
        "",
        "  " + "-" * 56,
    ]
    
    # Race bar visualization
    total = race.us.ytd_volume_mmt + race.br.ytd_volume_mmt
    if total > 0:
        us_bar = int(race.us_market_share_pct / 2)
        br_bar = int(race.br_market_share_pct / 2)
        lines.append(f"  US [{('█' * us_bar).ljust(50)}] {race.us_market_share_pct:.1f}%")
        lines.append(f"  BR [{('█' * br_bar).ljust(50)}] {race.br_market_share_pct:.1f}%")
    
    lines.extend([
        "  " + "-" * 56,
        "",
        f"  LEADER:  {race.leader}  (+{race.lead_pace_pct:.1f}pp pace advantage)",
        "",
        f"  Pace vs Seasonal:",
        f"    US: {race.us.pace_vs_seasonal:>+6.1f}pp  [{race.pace_signal_us}]",
        f"    BR: {race.br.pace_vs_seasonal:>+6.1f}pp  [{race.pace_signal_br}]",
        "",
        f"  China Destination:",
        f"    US → China: {race.us.ytd_china_mmt:.2f} MMT ({race.us.china_share_pct:.1f}% of US exports)",
        f"    BR → China: {race.br.ytd_china_mmt:.2f} MMT ({race.br.china_share_pct:.1f}% of BR exports)",
        f"    China sources: BR {race.br_china_share_pct:.1f}% | US {race.us_china_share_pct:.1f}%",
        "",
        f"  Market Share: BR {race.br_market_share_pct:.1f}% vs US {race.us_market_share_pct:.1f}%",
        f"  vs 5yr avg:   {race.share_shift_pp:>+.1f}pp",
        "=" * 60,
    ])
    
    return "\n".join(lines)


def to_dashboard_card(race: ExportRace) -> dict:
    """Format for Next.js dashboard component"""
    return {
        "card_type": "export_race",
        "title": f"Export Race — {race.commodity.title()}",
        "subtitle": f"BR vs US | MY {race.marketing_year}",
        "date": race.date,
        "leader": race.leader,
        "leader_color": "#00C878" if race.leader == "BR" else "#DCB432",
        "lead_text": f"+{race.lead_pace_pct:.1f}pp pace",
        "bars": [
            {
                "label": "US",
                "value": race.us.ytd_volume_mmt,
                "target": race.us.wasde_target_mmt,
                "pace_pct": race.us.pace_pct,
                "signal": race.pace_signal_us,
                "color": "#DCB432",
            },
            {
                "label": "BR",
                "value": race.br.ytd_volume_mmt,
                "target": race.br.wasde_target_mmt,
                "pace_pct": race.br.pace_pct,
                "signal": race.pace_signal_br,
                "color": "#00C878",
            },
        ],
        "market_share": {
            "br_pct": race.br_market_share_pct,
            "us_pct": race.us_market_share_pct,
            "shift_pp": race.share_shift_pp,
        },
        "china_race": {
            "br_share": race.br_china_share_pct,
            "us_share": race.us_china_share_pct,
        },
    }


# ============================================================
# INTEGRATED PIPELINE — loads from collector outputs
# ============================================================

def load_from_collectors(
    commodity: str = "soybeans",
    data_dir: str = "./data",
) -> Optional[ExportRace]:
    """
    Load from collect_comexstat.py and USDA export data outputs.
    
    Expected files:
      - comexstat_exports.json (from collect_comexstat.py)
      - usda_export_sales.json (from existing pipeline)
      - wasde_data.json (from existing pipeline)
    """
    try:
        # NCM mapping
        ncm_map = {
            "soybeans": "1201",
            "corn": "1005",
            "soybean_meal": "2304",
        }
        ncm = ncm_map.get(commodity, "1201")
        
        # Load Comex Stat (Brazil)
        comex_file = os.path.join(data_dir, "comexstat_exports.json")
        br_monthly = []
        if os.path.exists(comex_file):
            with open(comex_file) as f:
                comex = json.load(f)
            
            # Extract monthly data for this commodity
            ncm_data = comex.get("data", {}).get(ncm, {})
            monthly_raw = ncm_data.get("monthly", [])
            
            for m in monthly_raw:
                china_vol = 0
                by_dest = ncm_data.get("by_destination", {})
                # Sum China destinations (CN = China, HK = Hong Kong)
                for dest_code in ["CN", "HK"]:
                    dest_data = by_dest.get(dest_code, {}).get("monthly", [])
                    for dm in dest_data:
                        if dm["year"] == m["year"] and dm["month"] == m["month"]:
                            china_vol += dm.get("volume_kg", 0) / 1000  # kg → MT
                
                br_monthly.append({
                    "year": m["year"],
                    "month": m["month"],
                    "volume_mt": m.get("volume_kg", 0) / 1000,
                    "value_usd": m.get("value_usd", 0),
                    "china_volume_mt": china_vol,
                })
        
        # Load USDA Export Sales/Inspections (US)
        usda_file = os.path.join(data_dir, "usda_export_sales.json")
        us_monthly = []
        if os.path.exists(usda_file):
            with open(usda_file) as f:
                usda = json.load(f)
            
            comm_data = usda.get(commodity, {})
            for m in comm_data.get("monthly_inspections", []):
                us_monthly.append({
                    "year": m["year"],
                    "month": m["month"],
                    "volume_mt": m.get("volume_mt", 0),
                    "value_usd": 0,
                    "china_volume_mt": m.get("china_mt", 0),
                })
        
        # Load WASDE targets
        wasde_us = 0
        wasde_br = 0
        wasde_file = os.path.join(data_dir, "wasde_data.json")
        if os.path.exists(wasde_file):
            with open(wasde_file) as f:
                wasde = json.load(f)
            wasde_comm = wasde.get(commodity, {})
            wasde_us = wasde_comm.get("us_exports_mmt", 0)
            wasde_br = wasde_comm.get("br_exports_mmt", 0)
        
        # Determine marketing year
        today = date.today()
        my_start = MARKETING_YEAR[commodity]["us_start"]
        if today.month >= my_start:
            my = f"{today.year}/{str(today.year + 1)[2:]}"
        else:
            my = f"{today.year - 1}/{str(today.year)[2:]}"
        
        race = calculate_export_race(
            commodity=commodity,
            marketing_year=my,
            us_monthly=us_monthly,
            br_monthly=br_monthly,
            wasde_us_mmt=wasde_us,
            wasde_br_mmt=wasde_br,
            current_month=today.month,
        )
        
        return race
        
    except Exception as e:
        print(f"[ERT] Error: {e}")
        return None


# ============================================================
# DEMO WITH REPRESENTATIVE DATA
# ============================================================

def demo():
    """
    Demo using representative 2025/26 soybean export data.
    Based on real patterns from Comex Stat and USDA Export Inspections.
    """
    print("\n" + "=" * 60)
    print("  AgriMacro ERT — Demo Soybeans 2025/26")
    print("=" * 60)
    
    # US soybean exports Sep 2025 → Jan 2026 (representative MMT)
    # US front-loads exports Sep-Dec
    us_data = [
        {"year": 2025, "month": 9,  "volume_mt": 3_200_000, "value_usd": 1_440_000_000, "china_volume_mt": 1_920_000},
        {"year": 2025, "month": 10, "volume_mt": 7_800_000, "value_usd": 3_510_000_000, "china_volume_mt": 4_680_000},
        {"year": 2025, "month": 11, "volume_mt": 7_200_000, "value_usd": 3_240_000_000, "china_volume_mt": 4_320_000},
        {"year": 2025, "month": 12, "volume_mt": 5_100_000, "value_usd": 2_295_000_000, "china_volume_mt": 3_060_000},
        {"year": 2026, "month": 1,  "volume_mt": 4_000_000, "value_usd": 1_800_000_000, "china_volume_mt": 2_400_000},
    ]
    
    # BR soybean exports Feb 2025 → Jan 2026 (representative)
    # BR peaks Mar-Jun after harvest
    br_data = [
        {"year": 2025, "month": 2,  "volume_mt": 3_500_000,  "value_usd": 1_505_000_000, "china_volume_mt": 2_520_000},
        {"year": 2025, "month": 3,  "volume_mt": 13_000_000, "value_usd": 5_590_000_000, "china_volume_mt": 9_360_000},
        {"year": 2025, "month": 4,  "volume_mt": 16_500_000, "value_usd": 7_095_000_000, "china_volume_mt": 11_880_000},
        {"year": 2025, "month": 5,  "volume_mt": 15_800_000, "value_usd": 6_794_000_000, "china_volume_mt": 11_376_000},
        {"year": 2025, "month": 6,  "volume_mt": 12_500_000, "value_usd": 5_375_000_000, "china_volume_mt": 9_000_000},
        {"year": 2025, "month": 7,  "volume_mt": 10_200_000, "value_usd": 4_386_000_000, "china_volume_mt": 7_344_000},
        {"year": 2025, "month": 8,  "volume_mt": 8_500_000,  "value_usd": 3_655_000_000, "china_volume_mt": 6_120_000},
        {"year": 2025, "month": 9,  "volume_mt": 6_300_000,  "value_usd": 2_709_000_000, "china_volume_mt": 4_536_000},
        {"year": 2025, "month": 10, "volume_mt": 5_000_000,  "value_usd": 2_150_000_000, "china_volume_mt": 3_600_000},
        {"year": 2025, "month": 11, "volume_mt": 4_800_000,  "value_usd": 2_064_000_000, "china_volume_mt": 3_456_000},
        {"year": 2025, "month": 12, "volume_mt": 4_200_000,  "value_usd": 1_806_000_000, "china_volume_mt": 3_024_000},
        {"year": 2026, "month": 1,  "volume_mt": 3_800_000,  "value_usd": 1_634_000_000, "china_volume_mt": 2_736_000},
    ]
    
    # Calculate
    race = calculate_export_race(
        commodity="soybeans",
        marketing_year="2025/26",
        us_monthly=us_data,
        br_monthly=br_data,
        wasde_us_mmt=49.67,
        wasde_br_mmt=105.50,
        current_month=1,  # January
        ref_date="2026-01-31",
    )
    
    # Print report
    print(to_report_text(race))
    
    # Print JSON
    print("\n--- JSON Output ---")
    print(to_json(race))
    
    # Print dashboard card
    print("\n--- Dashboard Card ---")
    print(json.dumps(to_dashboard_card(race), indent=2))
    
    # === CORN ===
    print("\n\n")
    print("=" * 60)
    print("  AgriMacro ERT — Demo Corn 2025/26")
    print("=" * 60)
    
    # US corn exports (representative)
    us_corn = [
        {"year": 2025, "month": 9,  "volume_mt": 3_700_000, "value_usd": 888_000_000, "china_volume_mt": 370_000},
        {"year": 2025, "month": 10, "volume_mt": 7_500_000, "value_usd": 1_800_000_000, "china_volume_mt": 750_000},
        {"year": 2025, "month": 11, "volume_mt": 7_200_000, "value_usd": 1_728_000_000, "china_volume_mt": 720_000},
        {"year": 2025, "month": 12, "volume_mt": 6_200_000, "value_usd": 1_488_000_000, "china_volume_mt": 620_000},
        {"year": 2026, "month": 1,  "volume_mt": 5_000_000, "value_usd": 1_200_000_000, "china_volume_mt": 500_000},
    ]
    
    # BR corn exports (safrinha peaks Jul-Oct)
    br_corn = [
        {"year": 2025, "month": 2,  "volume_mt": 500_000,   "value_usd": 110_000_000,  "china_volume_mt": 25_000},
        {"year": 2025, "month": 3,  "volume_mt": 800_000,   "value_usd": 176_000_000,  "china_volume_mt": 40_000},
        {"year": 2025, "month": 4,  "volume_mt": 900_000,   "value_usd": 198_000_000,  "china_volume_mt": 45_000},
        {"year": 2025, "month": 5,  "volume_mt": 1_200_000, "value_usd": 264_000_000,  "china_volume_mt": 60_000},
        {"year": 2025, "month": 6,  "volume_mt": 2_500_000, "value_usd": 550_000_000,  "china_volume_mt": 125_000},
        {"year": 2025, "month": 7,  "volume_mt": 6_500_000, "value_usd": 1_430_000_000, "china_volume_mt": 325_000},
        {"year": 2025, "month": 8,  "volume_mt": 8_200_000, "value_usd": 1_804_000_000, "china_volume_mt": 410_000},
        {"year": 2025, "month": 9,  "volume_mt": 7_500_000, "value_usd": 1_650_000_000, "china_volume_mt": 375_000},
        {"year": 2025, "month": 10, "volume_mt": 6_200_000, "value_usd": 1_364_000_000, "china_volume_mt": 310_000},
        {"year": 2025, "month": 11, "volume_mt": 4_500_000, "value_usd": 990_000_000,  "china_volume_mt": 225_000},
        {"year": 2025, "month": 12, "volume_mt": 2_800_000, "value_usd": 616_000_000,  "china_volume_mt": 140_000},
        {"year": 2026, "month": 1,  "volume_mt": 1_800_000, "value_usd": 396_000_000,  "china_volume_mt": 90_000},
    ]
    
    race_corn = calculate_export_race(
        commodity="corn",
        marketing_year="2025/26",
        us_monthly=us_corn,
        br_monthly=br_corn,
        wasde_us_mmt=62.23,
        wasde_br_mmt=46.00,
        current_month=1,
        ref_date="2026-01-31",
    )
    
    print(to_report_text(race_corn))
    
    # === SENSITIVITY: What if tariffs hit? ===
    print("\n\n")
    print("=" * 60)
    print("  SCENARIO: China Tariff on US Soybeans")
    print("  (US→China drops 50%, BR→China gains equivalent)")
    print("=" * 60)
    
    # Modify US data: China volume drops 50%
    us_tariff = []
    for m in us_data:
        m2 = dict(m)
        m2["china_volume_mt"] = int(m["china_volume_mt"] * 0.5)
        us_tariff.append(m2)
    
    # Modify BR data: China volume increases proportionally
    br_tariff = []
    for i, m in enumerate(br_data):
        m2 = dict(m)
        # BR picks up ~70% of what US loses to China
        us_loss = us_data[min(i, len(us_data)-1)]["china_volume_mt"] * 0.5 * 0.7
        m2["china_volume_mt"] = int(m["china_volume_mt"] + us_loss / len(br_data) * len(us_data))
        us_tariff_data = m2
        br_tariff.append(m2)
    
    race_tariff = calculate_export_race(
        commodity="soybeans",
        marketing_year="2025/26",
        us_monthly=us_tariff,
        br_monthly=br_tariff,
        wasde_us_mmt=49.67,
        wasde_br_mmt=105.50,
        current_month=1,
        ref_date="2026-01-31",
    )
    
    print(to_report_text(race_tariff))
    
    print("\n  Tariff Impact:")
    print(f"  BR China share: {race.br_china_share_pct:.1f}% → {race_tariff.br_china_share_pct:.1f}%")
    print(f"  US China share: {race.us_china_share_pct:.1f}% → {race_tariff.us_china_share_pct:.1f}%")
    print(f"  Market share shift: {race_tariff.share_shift_pp - race.share_shift_pp:+.1f}pp additional to BR")


if __name__ == "__main__":
    demo()
