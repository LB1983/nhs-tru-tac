import duckdb

con = duckdb.connect("Data/canonical/tru_tac.duckdb")

# mapping with normalised worksheet key
con.execute("""
CREATE OR REPLACE VIEW dim_tac_subcodes_ws AS
SELECT
  fy,
  WorkSheetName,
  regexp_replace(lower(WorkSheetName), '[^a-z0-9]+', '', 'g') AS ws_key,
  SubCode,
  subcode_label,
  source_file
FROM read_csv_auto('mappings/dim_tac_subcodes_by_year.csv', header=True);
""")

df = con.execute("""
SELECT
  f.fy,
  f.sector,
  COUNT(*) AS fact_rows,
  SUM(CASE WHEN d.SubCode IS NULL THEN 1 ELSE 0 END) AS unmatched_rows,
  SUM(ABS(f.amount)) AS abs_total_amount,
  SUM(CASE WHEN d.SubCode IS NULL THEN ABS(f.amount) ELSE 0 END) AS abs_unmatched_amount,
  SUM(CASE WHEN d.SubCode IS NULL THEN ABS(f.amount) ELSE 0 END) / NULLIF(SUM(ABS(f.amount)),0) AS share_unmatched_abs
FROM (
  SELECT
    fy,
    sector,
    regexp_replace(lower(WorkSheetName), '[^a-z0-9]+', '', 'g') AS ws_key,
    WorkSheetName,
    SubCode,
    amount
  FROM fact_tru_tac
) f
LEFT JOIN dim_tac_subcodes_ws d
  ON f.fy = d.fy
 AND f.ws_key = d.ws_key
 AND f.SubCode = d.SubCode
GROUP BY 1,2
ORDER BY 1,2;
""").fetchdf()

print(df.to_string(index=False))
con.close()
