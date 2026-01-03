import duckdb

con = duckdb.connect("Data/canonical/tru_tac.duckdb")

# Provider dim
con.execute("""
CREATE OR REPLACE VIEW dim_provider AS
SELECT * FROM read_csv_auto('mappings/dim_provider.csv', header=True);
""")

# Subcode mapping with normalised worksheet key
con.execute("""
CREATE OR REPLACE VIEW dim_tac_subcodes_ws AS
SELECT
  fy,
  regexp_replace(lower(WorkSheetName), '[^a-z0-9]+', '', 'g') AS ws_key,
  SubCode,
  subcode_label,
  source_file AS mapping_source_file
FROM read_csv_auto('mappings/dim_tac_subcodes_by_year.csv', header=True);
""")

# Materialise enriched table
con.execute("""
CREATE OR REPLACE TABLE fact_tru_tac_enriched AS
SELECT
  f.*,
  p.provider_id,
  p.org_name_canonical,
  m.subcode_label,
  m.mapping_source_file,
  CASE WHEN m.subcode_label IS NULL THEN 1 ELSE 0 END AS is_unmapped
FROM (
  SELECT
    *,
    regexp_replace(lower(WorkSheetName), '[^a-z0-9]+', '', 'g') AS ws_key
  FROM fact_tru_tac
) f
LEFT JOIN dim_provider p
  ON f.sector = p.sector
 AND f.org_name_raw = p.org_name_raw
LEFT JOIN dim_tac_subcodes_ws m
  ON f.fy = m.fy
 AND f.ws_key = m.ws_key
 AND f.SubCode = m.SubCode;
""")

# Quick QC summary
df = con.execute("""
SELECT
  fy,
  sector,
  SUM(is_unmapped) AS unmapped_rows,
  COUNT(*) AS total_rows,
  SUM(ABS(amount)) AS abs_total,
  SUM(CASE WHEN is_unmapped=1 THEN ABS(amount) ELSE 0 END) AS abs_unmapped,
  SUM(CASE WHEN is_unmapped=1 THEN ABS(amount) ELSE 0 END)/NULLIF(SUM(ABS(amount)),0) AS share_abs_unmapped
FROM fact_tru_tac_enriched
GROUP BY 1,2
ORDER BY 1,2;
""").fetchdf()

print(df.to_string(index=False))
con.close()
