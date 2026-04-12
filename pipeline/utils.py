"""
AgriMacro Pipeline — Shared utility functions.
Canonical formulas for spreads and ratios used across multiple scripts.
"""


def calculate_crush_spread(zm, zl, zs):
    """
    CME Soybean Crush Spread (Board Crush).

    1 bushel of soybeans (60 lbs) yields approximately:
      - 44 lbs of soybean meal
      - 11 lbs of soybean oil

    Crush Margin = Meal Revenue + Oil Revenue - Soybean Cost

    Args:
        zm: Soybean Meal price in USD per short ton (2000 lbs)
        zl: Soybean Oil price in cents per pound
        zs: Soybeans price in cents per bushel

    Returns:
        Crush margin in USD per bushel

    Formula:
        (ZM * 44/2000)  — meal value: 44 lbs yield / 2000 lbs per ton * price per ton
      + (ZL * 11/100)   — oil value:  11 lbs yield * price in cents converted to dollars
      - (ZS / 100)      — soy cost:   cents per bushel converted to dollars
    """
    return (zm * 44 / 2000) + (zl * 11 / 100) - (zs / 100)
