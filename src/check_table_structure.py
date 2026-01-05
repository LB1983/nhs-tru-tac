#!/usr/bin/env python3
"""Quick check of dim_tac_subcodes table structure"""

import duckdb
from pathlib import Path

DB_PATH = Path("Data/canonical/tru_tac.duckdb")
con = duckdb.connect(str(DB_PATH), read_only=True)

print("=" * 80)
print("CHECKING TABLE STRUCTURE")
print("=" * 80)

# List all tables
print("\nTables in database:")
tables = con.execute("SHOW TABLES").fetchall()
for t in tables:
    print(f"  - {t[0]}")

# Check dim_tac_subcodes structure
print("\n" + "=" * 80)
print("dim_tac_subcodes table structure:")
print("=" * 80)

schema = con.execute("PRAGMA table_info('dim_tac_subcodes')").fetchdf()
print(schema.to_string(index=False))

# Get sample data
print("\n" + "=" * 80)
print("Sample data (first 5 rows):")
print("=" * 80)

sample = con.execute("SELECT * FROM dim_tac_subcodes LIMIT 5").fetchdf()
print(sample.to_string(index=False))

# Count rows
row_count = con.execute("SELECT COUNT(*) FROM dim_tac_subcodes").fetchone()[0]
print(f"\nTotal rows: {row_count:,}")

# Try a simple query
print("\n" + "=" * 80)
print("Testing simple GROUP BY query:")
print("=" * 80)

try:
    test = con.execute("""
        SELECT SubCode, COUNT(*) as cnt
        FROM dim_tac_subcodes
        GROUP BY SubCode
        LIMIT 5
    """).fetchdf()
    print("✓ Simple query works")
    print(test.to_string(index=False))
except Exception as e:
    print(f"✗ Error: {e}")

con.close()
