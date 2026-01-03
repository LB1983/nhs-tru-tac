from pathlib import Path
import pandas as pd

RAW_DIR = Path("Data/raw")

for path in sorted(RAW_DIR.glob("TAC_*.xlsx")):
    xls = pd.ExcelFile(path, engine="openpyxl")
    print(f"\n{path.name}")
    for s in xls.sheet_names:
        print(f"  - {s}")
