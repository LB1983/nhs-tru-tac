import pandas as pd
from pathlib import Path

IN_FILE = Path("outputs/top_750_lines_2023-24.csv")
OUT_DIR = Path("mappings")
OUT_DIR.mkdir(exist_ok=True)
OUT_FILE = OUT_DIR / "dim_tac_lines_seed.csv"

df = pd.read_csv(IN_FILE)

# Keep only the line key
keys = df[["TableID", "MainCode", "SubCode", "RowNumber"]].drop_duplicates()

# Add classification columns (blank for now)
keys["line_label"] = ""
keys["category_1"] = ""
keys["category_2"] = ""
keys["is_digital_data_it"] = ""
keys["notes"] = ""

keys = keys.sort_values(["TableID", "MainCode", "SubCode", "RowNumber"])

keys.to_csv(OUT_FILE, index=False)
print(f"Wrote {OUT_FILE} ({len(keys)} unique lines)")
