import duckdb
from pathlib import Path

OUT_DIR = Path("outputs")
OUT_DIR.mkdir(exist_ok=True)

con = duckdb.connect("Data/canonical/tru_tac.duckdb")

df = con.execute("""
WITH line_totals AS (
  SELECT
    fy,
    sector,
    TableID,
    MainCode,
    SubCode,
    RowNumber,
    SUM(ABS(amount)) AS abs_amount
  FROM fact_tru_tac
  WHERE fy = '2023-24'
  GROUP BY 1,2,3,4,5,6
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY sector ORDER BY abs_amount DESC) AS rn
  FROM line_totals
)
SELECT *
FROM ranked
WHERE rn <= 750
ORDER BY sector, rn;
""").fetchdf()

out_path = OUT_DIR / "top_750_lines_2023-24.csv"
df.to_csv(out_path, index=False)

print(f"Wrote {out_path} ({len(df)} rows)")

con.close()
