#!/usr/bin/env python3
"""
Explore and analyze NHS TAC DuckDB database.
This script can work with existing databases or help locate them.
"""

import duckdb
import pandas as pd
from pathlib import Path
import sys

print("=" * 80)
print("NHS TAC DuckDB Database Explorer")
print("=" * 80)

# Possible database locations
db_paths = [
    Path("Data/canonical/tru_tac.duckdb"),
    Path("tru_tac.duckdb"),
    Path("../tru_tac.duckdb"),
]

# Try to find the database
db_path = None
for path in db_paths:
    if path.exists():
        db_path = path
        print(f"\n✓ Found database at: {path.absolute()}")
        break

if db_path is None:
    print("\n⚠ No DuckDB database found in standard locations.")
    print("\nSearching for .duckdb files...")

    # Search more broadly
    for p in Path(".").rglob("*.duckdb"):
        print(f"  Found: {p.absolute()}")
        if db_path is None:
            db_path = p

    if db_path is None:
        print("\n❌ No DuckDB database found.")
        print("\nTo create a database, you need:")
        print("  1. Raw TAC data files in Data/raw/")
        print("     Format: TAC_Trusts_YYYY-YY.xlsx or TAC_FTs_YYYY-YY.xlsx")
        print("  2. Run: python src/build_canonical.py")
        sys.exit(1)

# Connect to the database
print(f"\n{'=' * 80}")
print("Connecting to database...")
print('=' * 80)

try:
    con = duckdb.connect(str(db_path), read_only=True)

    # List all tables
    print("\n[1/5] Database Tables:")
    tables = con.execute("SHOW TABLES").fetchall()
    if tables:
        for table in tables:
            print(f"  ✓ {table[0]}")
    else:
        print("  ⚠ No tables found in database")
        sys.exit(1)

    # For each table, show structure and sample data
    for table_name, in tables:
        print(f"\n{'=' * 80}")
        print(f"[2/5] Table: {table_name}")
        print('=' * 80)

        # Get row count
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"\nTotal rows: {row_count:,}")

        # Get schema
        print("\nSchema:")
        schema = con.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
        for _, row in schema.iterrows():
            print(f"  {row['name']:25} {row['type']:15} {'NULL' if row['notnull'] == 0 else 'NOT NULL'}")

        # Get sample data
        print("\nSample data (first 5 rows):")
        sample = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
        print(sample.to_string(index=False))

        # Basic statistics
        print(f"\n{'=' * 80}")
        print(f"[3/5] Data Summary for {table_name}")
        print('=' * 80)

        # Check for common columns and provide stats
        cols = [c.lower() for c in sample.columns]

        if 'fy' in cols:
            print("\nRecords by Financial Year:")
            fy_counts = con.execute(f"SELECT fy, COUNT(*) as count FROM {table_name} GROUP BY fy ORDER BY fy").fetchdf()
            print(fy_counts.to_string(index=False))

        if 'sector' in cols:
            print("\nRecords by Sector:")
            sector_counts = con.execute(f"SELECT sector, COUNT(*) as count FROM {table_name} GROUP BY sector ORDER BY sector").fetchdf()
            print(sector_counts.to_string(index=False))

        if 'amount' in cols:
            print("\nAmount Statistics:")
            amount_stats = con.execute(f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(amount) as non_null_amounts,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount,
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount
                FROM {table_name}
            """).fetchdf()
            print(amount_stats.to_string(index=False))

        if 'worksheetname' in cols:
            print("\nTop 10 Worksheets by Record Count:")
            ws_counts = con.execute(f"""
                SELECT WorkSheetName, COUNT(*) as count
                FROM {table_name}
                GROUP BY WorkSheetName
                ORDER BY count DESC
                LIMIT 10
            """).fetchdf()
            print(ws_counts.to_string(index=False))

        if 'org_name_raw' in cols:
            print("\nTop 10 Organizations by Record Count:")
            org_counts = con.execute(f"""
                SELECT org_name_raw, COUNT(*) as count
                FROM {table_name}
                GROUP BY org_name_raw
                ORDER BY count DESC
                LIMIT 10
            """).fetchdf()
            print(org_counts.to_string(index=False))

    # Data quality checks
    print(f"\n{'=' * 80}")
    print("[4/5] Data Quality Checks")
    print('=' * 80)

    for table_name, in tables:
        print(f"\nTable: {table_name}")

        # Check for nulls in key columns
        schema = con.execute(f"PRAGMA table_info('{table_name}')").fetchdf()
        for _, col_info in schema.iterrows():
            col_name = col_info['name']
            null_count = con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL").fetchone()[0]
            if null_count > 0:
                null_pct = (null_count / row_count) * 100
                print(f"  ⚠ {col_name}: {null_count:,} nulls ({null_pct:.1f}%)")

    # Summary statistics
    print(f"\n{'=' * 80}")
    print("[5/5] Summary Statistics")
    print('=' * 80)

    for table_name, in tables:
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        col_count = len(con.execute(f"PRAGMA table_info('{table_name}')").fetchall())

        print(f"\n{table_name}:")
        print(f"  Rows: {row_count:,}")
        print(f"  Columns: {col_count}")

        # If it's the fact table, show more details
        if 'fact' in table_name.lower() or table_name.lower() == 'tru_tac':
            distinct_fy = con.execute(f"SELECT COUNT(DISTINCT fy) FROM {table_name}").fetchone()[0]
            distinct_orgs = con.execute(f"SELECT COUNT(DISTINCT org_name_raw) FROM {table_name}").fetchone()[0]
            distinct_subcodes = con.execute(f"SELECT COUNT(DISTINCT SubCode) FROM {table_name}").fetchone()[0]

            print(f"  Unique Financial Years: {distinct_fy}")
            print(f"  Unique Organizations: {distinct_orgs}")
            print(f"  Unique SubCodes: {distinct_subcodes}")

    con.close()

    print("\n" + "=" * 80)
    print("Database exploration complete!")
    print("=" * 80)

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
