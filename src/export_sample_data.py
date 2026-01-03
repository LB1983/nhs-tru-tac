#!/usr/bin/env python3
"""
Script to export a sample of the DuckDB database for analysis.
Run this on your Windows machine to create a smaller dataset.
"""

import duckdb
import pandas as pd
from pathlib import Path

# Update this path to your database location
DB_PATH = r"C:\Users\laure\OneDrive\Documents\BevanBriefing\nhs-tru-tac\tru_tac.duckdb"
OUTPUT_DIR = Path("Data/samples")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("Connecting to database...")
con = duckdb.connect(str(DB_PATH), read_only=True)

# Export table schemas and statistics
print("\n1. Exporting table information...")
tables = con.execute("SHOW TABLES").fetchall()
print(f"Found {len(tables)} tables: {[t[0] for t in tables]}")

# Export metadata
metadata = {
    'tables': [],
    'row_counts': {},
    'columns': {}
}

for table_name, in tables:
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    metadata['tables'].append(table_name)
    metadata['row_counts'][table_name] = row_count

    schema = con.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
    metadata['columns'][table_name] = schema.to_dict('records')

    print(f"  {table_name}: {row_count:,} rows")

# Export sample data from main fact table (if it exists)
fact_tables = [t for t, in tables if 'fact' in t.lower() or 'tru_tac' in t.lower()]

if fact_tables:
    fact_table = fact_tables[0]
    print(f"\n2. Exporting sample from {fact_table}...")

    # Export a stratified sample (1000 rows per FY/sector combination)
    sample_query = f"""
        WITH sampled AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY fy, sector ORDER BY RANDOM()) as rn
            FROM {fact_table}
        )
        SELECT * EXCEPT(rn)
        FROM sampled
        WHERE rn <= 1000
    """

    sample_df = con.execute(sample_query).fetchdf()
    sample_df.to_parquet(OUTPUT_DIR / f"{fact_table}_sample.parquet", index=False)
    sample_df.to_csv(OUTPUT_DIR / f"{fact_table}_sample.csv", index=False)

    print(f"  Exported {len(sample_df):,} sample rows to:")
    print(f"    - {OUTPUT_DIR / f'{fact_table}_sample.parquet'}")
    print(f"    - {OUTPUT_DIR / f'{fact_table}_sample.csv'}")

    # Export aggregated summary statistics
    print(f"\n3. Exporting summary statistics...")

    summary_query = f"""
        SELECT
            fy,
            sector,
            WorkSheetName,
            COUNT(*) as record_count,
            COUNT(DISTINCT org_name_raw) as unique_orgs,
            COUNT(DISTINCT SubCode) as unique_subcodes,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            MIN(amount) as min_amount,
            MAX(amount) as max_amount
        FROM {fact_table}
        GROUP BY fy, sector, WorkSheetName
        ORDER BY fy, sector, WorkSheetName
    """

    summary_df = con.execute(summary_query).fetchdf()
    summary_df.to_csv(OUTPUT_DIR / "summary_statistics.csv", index=False)
    print(f"  Exported summary to: {OUTPUT_DIR / 'summary_statistics.csv'}")

con.close()
print("\n✓ Export complete!")
print(f"\nYou can now copy the {OUTPUT_DIR} folder to the repository for analysis.")
