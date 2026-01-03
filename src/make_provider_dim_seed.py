import duckdb
from pathlib import Path

OUT_DIR = Path("mappings")
OUT_DIR.mkdir(exist_ok=True)

con = duckdb.connect("Data/canonical/tru_tac.duckdb")

df = con.execute("""
SELECT
  sector,
  org_name_raw,
  MIN(fy) AS first_fy_seen,
  MAX(fy) AS last_fy_seen,
  COUNT(*) AS rows
FROM fact_tru_tac
GROUP BY 1,2
ORDER BY sector, org_name_raw
""").fetchdf()

out = OUT_DIR / "dim_provider_seed.csv"
df.to_csv(out, index=False)
print(f"Wrote {out} ({len(df)} rows)")

con.close()
