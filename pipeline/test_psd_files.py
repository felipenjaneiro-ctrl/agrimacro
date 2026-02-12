import requests

BASE = "https://apps.fas.usda.gov/psdonline/downloads"
names = [
    "psd_cocoa_csv.zip",
    "psd_tree_crops_csv.zip",
    "psd_tropical_csv.zip",
    "psd_horticulture_csv.zip",
    "psd_fruit_csv.zip",
    "psd_citrus_csv.zip",
    "psd_juices_csv.zip",
    "psd_alldata_csv.zip",
    "psd_dairy_csv.zip",
    "psd_livestock_csv.zip",
    "psd_grains_pulses_csv.zip",
]
print("Testando nomes de arquivo PSD...")
for n in names:
    r = requests.head(f"{BASE}/{n}", timeout=10, allow_redirects=True)
    tag = "OK" if r.status_code == 200 else f"{r.status_code}"
    size = int(r.headers.get("Content-Length",0))/1024
    print(f"  [{tag:>3s}] {n:40s} {size:>8.0f} KB" if r.status_code==200 else f"  [{tag:>3s}] {n}")
