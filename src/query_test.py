import duckdb

con = duckdb.connect("Data/canonical/tru_tac.duckdb")

# How many rows total?
print(con.execute("SELECT COUNT(*) AS rows FROM fact_tru_tac").fetchdf())

# How many organisations per year?
print(con.execute("""
  SELECT fy, sector, COUNT(DISTINCT org_name_raw) AS orgs
  FROM fact_tru_tac
  GROUP BY 1,2
  ORDER BY 1,2
""").fetchdf())

con.close()
