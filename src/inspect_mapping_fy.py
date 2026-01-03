import pandas as pd

path = "mappings/dim_tac_subcodes_by_year.csv"
df = pd.read_csv(path)

print("Distinct fy values in mapping:")
print(df["fy"].value_counts(dropna=False))

print("\nSample rows:")
print(df.head(10).to_string(index=False))
