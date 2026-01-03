import duckdb
from pathlib import Path

OUT_DIR = Path("outputs")
OUT_DIR.mkdir(exist_ok=True)

con = duckdb.connect("Data/canonical/tru_tac.duckdb")

# Ensure enriched table exists
con.execute("SELECT 1 FROM fact_tru_tac_enriched LIMIT 1;")

df = con.execute("""
SELECT
  fy,
  sector,
  WorkSheetName,
  TableID,
  SubCode,
  COUNT(*) AS rows,
  SUM(ABS(amount)) AS abs_amount
FROM fact_tru_tac_enriched
WHERE is_unmapped=1
  AND fy IN ('2019-20','2020-21','2021-22','2022-23','2023-24')
GROUP BY 1,2,3,4,5
ORDER BY abs_amount DESC
LIMIT 300;
""").fetchdf()

out = OUT_DIR / "top_unmapped_subcodes_by_abs_amount.csv"
df.to_csv(out, index=False)
print(f"Wrote {out} ({len(df)} rows)")
con.close()
