import duckdb

con = duckdb.connect("Data/canonical/tru_tac.duckdb")

con.execute("""
CREATE OR REPLACE VIEW dim_tac_subcodes AS
SELECT * FROM read_csv_auto('mappings/dim_tac_subcodes_by_year.csv', header=True);
""")

tests = {
    "fy+SubCode": """
        SELECT f.fy, f.sector,
               COUNT(*) AS fact_rows,
               SUM(CASE WHEN d.SubCode IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
        FROM fact_tru_tac f
        LEFT JOIN dim_tac_subcodes d
          ON f.fy=d.fy AND f.SubCode=d.SubCode
        GROUP BY 1,2 ORDER BY 1,2;
    """,
    "fy+TableID+SubCode": """
        SELECT f.fy, f.sector,
               COUNT(*) AS fact_rows,
               SUM(CASE WHEN d.SubCode IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
        FROM fact_tru_tac f
        LEFT JOIN dim_tac_subcodes d
          ON f.fy=d.fy AND CAST(f.TableID AS VARCHAR)=CAST(d.TableID AS VARCHAR) AND f.SubCode=d.SubCode
        GROUP BY 1,2 ORDER BY 1,2;
    """,
    "fy+WorkSheetName+TableID+SubCode": """
        SELECT f.fy, f.sector,
               COUNT(*) AS fact_rows,
               SUM(CASE WHEN d.SubCode IS NULL THEN 1 ELSE 0 END) AS unmatched_rows
        FROM fact_tru_tac f
        LEFT JOIN dim_tac_subcodes d
          ON f.fy=d.fy
         AND f.WorkSheetName=d.WorkSheetName
         AND CAST(f.TableID AS VARCHAR)=CAST(d.TableID AS VARCHAR)
         AND f.SubCode=d.SubCode
        GROUP BY 1,2 ORDER BY 1,2;
    """,
}

for name, sql in tests.items():
    print("\n====", name, "====")
    print(con.execute(sql).fetchdf().to_string(index=False))

con.close()
