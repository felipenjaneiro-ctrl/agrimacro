import requests, zipfile, io, csv
BASE = "https://apps.fas.usda.gov/psdonline/downloads"
# Tenta arquivos especificos que podem ter cocoa
for f in ["psd_alldata_csv.zip"]:
    r = requests.get(f"{BASE}/{f}", timeout=120)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
    with z.open(csv_name) as fh:
        text = io.TextIOWrapper(fh, encoding="utf-8-sig")
        reader = csv.DictReader(text)
        comms = set()
        for row in reader:
            c = row.get("Commodity_Description", "")
            if "cocoa" in c.lower() or "cacao" in c.lower():
                country = row.get("Country_Name", "")
                comms.add(f"{c} | {country}")
    if comms:
        print(f"Encontrado em {f}:")
        for c in sorted(comms)[:20]:
            print(f"  {c}")
    else:
        print(f"{f}: nenhum cocoa/cacao")
        # Show all unique commodity names starting with C
        r2 = requests.get(f"{BASE}/{f}", timeout=120)
        z2 = zipfile.ZipFile(io.BytesIO(r2.content))
        with z2.open(csv_name) as fh2:
            text2 = io.TextIOWrapper(fh2, encoding="utf-8-sig")
            reader2 = csv.DictReader(text2)
            all_c = set(row.get("Commodity_Description","") for row in reader2)
            print("Todas commodities com C:")
            for c in sorted(c2 for c2 in all_c if c2.startswith("C")):
                print(f"  {c}")
