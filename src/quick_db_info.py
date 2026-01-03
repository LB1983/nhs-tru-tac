#!/usr/bin/env python3
"""
Quick script to get database information.
Run this on Windows to tell Claude about your database.
"""

import duckdb
from pathlib import Path

DB_PATH = r"C:\Users\laure\OneDrive\Documents\BevanBriefing\nhs-tru-tac\tru_tac.duckdb"

db_file = Path(DB_PATH)
if db_file.exists():
    print(f"Database file size: {db_file.stat().st_size / (1024**3):.2f} GB")
else:
    print(f"Database not found at: {DB_PATH}")
    exit(1)

con = duckdb.connect(str(DB_PATH), read_only=True)

print("\nTables:")
tables = con.execute("SHOW TABLES").fetchall()
for table_name, in tables:
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"  {table_name}: {row_count:,} rows")

    # Show first few column names
    cols = con.execute(f"SELECT * FROM {table_name} LIMIT 0").description
    col_names = [c[0] for c in cols]
    print(f"    Columns ({len(col_names)}): {', '.join(col_names[:10])}{'...' if len(col_names) > 10 else ''}")

con.close()
