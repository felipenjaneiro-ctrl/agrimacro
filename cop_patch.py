import io as _io

def _fetch_ers_cop():
    _URLS = [
        ("corn",     "https://www.ers.usda.gov/media/4962/corn.csv"),
        ("soybeans", "https://www.ers.usda.gov/media/4976/soybeans.csv"),
        ("wheat",    "https://www.ers.usda.gov/media/4978/wheat.csv"),
    ]
    _HDR = {"User-Agent": "AgriMacro-Intelligence/3.3 (research)"}
    _res = {}
    for _idx, (_crop, _url) in enumerate(_URLS):
        try:
            _r = requests.get(_url, headers=_HDR, timeout=30)
            _r.raise_for_status()
            _dc = pd.read_csv(_io.StringIO(_r.text))
            _dc.columns = [str(c).strip() for c in _dc.columns]
            _us = _dc[_dc["Region"].str.contains("U.S. total", na=False)]
            _cost = _us[_us["Item"] == "Total, costs listed"][["Year","Value"]].rename(columns={"Value":"cost"})
            _yld  = _us[_us["Item"] == "Yield"][["Year","Value"]].rename(columns={"Value":"yield"})
            _merged = _cost.merge(_yld, on="Year")
            _merged = _merged[_merged["yield"] > 0].copy()
            _merged["cop_per_bu"] = (_merged["cost"] / _merged["yield"]).round(2)
            for _, _row in _merged.iterrows():
                _yr = int(_row["Year"])
                if _yr not in _res:
                    _res[_yr] = [float("nan"), float("nan"), float("nan")]
                _res[_yr][_idx] = float(_row["cop_per_bu"])
            log("  [ERS COP] " + _crop + ": OK (" + str(len(_merged)) + " anos, ultimo=" + str(int(_merged["Year"].max())) + ")")
        except Exception as _e:
            log("  [ERS COP] " + _crop + ": ERRO - " + str(_e))
    return _res

_ers_cop_raw = _fetch_ers_cop()

def _cop_lookup(_year, _idx):
    if _year in _ers_cop_raw and not pd.isna(_ers_cop_raw[_year][_idx]):
        return _ers_cop_raw[_year][_idx]
    for _y in range(_year - 1, 1989, -1):
        if _y in _ers_cop_raw and not pd.isna(_ers_cop_raw[_y][_idx]):
            return _ers_cop_raw[_y][_idx]
    return float("nan")

df["corn_cop"]  = df.index.map(lambda d: _cop_lookup(d.year, 0))
df["soy_cop"]   = df.index.map(lambda d: _cop_lookup(d.year, 1))
df["wheat_cop"] = df.index.map(lambda d: _cop_lookup(d.year, 2))
df["cop_is_fallback"] = df["corn_cop"].isna()
log("  COP: " + ("USDA ERS real - " + str(len(_ers_cop_raw)) + " anos" if _ers_cop_raw else "FALHA no download ERS"))