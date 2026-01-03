from pathlib import Path
import pandas as pd
import re

REF_DIR = Path("Data/reference")

def norm(s):
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())

# keywords we expect somewhere in the sheet (not necessarily as column headers)
KEYWORDS = [norm(x) for x in [
    "Table", "Table ID", "Main", "Main code", "Sub", "Sub code", "Row", "Row number", "Description"
]]

for file in sorted(REF_DIR.glob("*.xlsx")):
    print(f"\n=== {file.name} ===")
    xls = pd.ExcelFile(file, engine="openpyxl")

    for sheet in xls.sheet_names:
        # Read without headers so we can scan raw cells
        df = pd.read_excel(file, sheet_name=sheet, engine="openpyxl", header=None, nrows=30)
        found_rows = []

        for r in range(df.shape[0]):
            row_vals = " ".join([str(x) for x in df.iloc[r].tolist() if str(x) != "nan"])
            row_n = norm(row_vals)
            score = sum(1 for k in KEYWORDS if k in row_n)
            if score >= 3:  # threshold: looks like a header-ish row
                found_rows.append((r, score, row_vals[:160]))

        if found_rows:
            best = sorted(found_rows, key=lambda x: (-x[1], x[0]))[0]
            print(f"  Sheet '{sheet}': likely header row {best[0]} (score={best[1]}) | {best[2]}")
