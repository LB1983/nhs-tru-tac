import pandas as pd

path = r"Data/raw/TAC_FTs_2023-24.xlsx"
sheet = "Pivot - data for all providers"

df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
print("Columns:")
for c in df.columns:
    print(" -", c)
print("\nHead:")
print(df.head(5).to_string(index=False))
