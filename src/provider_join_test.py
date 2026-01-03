import duckdb

con = duckdb.connect("Data/canonical/tru_tac.duckdb")

# Load the provider dimension CSV as a view
con.execute("""
CREATE OR REPLACE VIEW dim_provider AS
SELECT * FROM read_csv_auto('mappings/dim_provider.csv', header=True);
""")

# Check join coverage (should be 100% if you didn't change org_name_raw)
df = con.execute("""
SELECT
  f.fy,
  f.sector,
  COUNT(*) AS fact_rows,
  SUM(CASE WHEN p.provider_id IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
FROM fact_tru_tac f
LEFT JOIN dim_provider p
  ON f.sector = p.sector
 AND f.org_name_raw = p.org_name_raw
GROUP BY 1,2
ORDER BY 1,2;
""").fetchdf()

print(df)

con.close()
