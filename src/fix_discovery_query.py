#!/usr/bin/env python3
"""
Fixed version - explicitly names columns to avoid DuckDB binding issue
"""

import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path("Data/canonical/tru_tac.duckdb")
OUTPUT_DIR = Path("Data/analysis/code_discovery")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 80)
print("NHS TAC CODE DISCOVERY - IT & CONSULTANCY")
print("=" * 80)
print(f"\nConnecting to: {DB_PATH}\n")

con = duckdb.connect(str(DB_PATH), read_only=True)

# First, let's try selecting with explicit columns
print("Testing basic query...")
try:
    test = con.execute("""
        SELECT fy, WorkSheetName, TableID, SubCode, subcode_label
        FROM dim_tac_subcodes
        LIMIT 3
    """).fetchdf()
    print("✓ Basic query works!")
    print(test)
except Exception as e:
    print(f"✗ Error: {e}")
    print("\nTrying alternative table...")
    # Maybe we should use dim_tac_subcodes_ws instead?
    test = con.execute("SELECT * FROM dim_tac_subcodes_ws LIMIT 3").fetchdf()
    print(test)

# ============================================================================
# SEARCH FOR IT-RELATED CODES
# ============================================================================
print("\n" + "=" * 80)
print("1. IT-RELATED SUBCODES")
print("=" * 80)

# Use explicit column selection and avoid subqueries
it_search_query = """
WITH filtered_codes AS (
    SELECT fy, WorkSheetName, SubCode, subcode_label
    FROM dim_tac_subcodes
    WHERE LOWER(subcode_label) LIKE '%it %'
       OR LOWER(subcode_label) LIKE '% it%'
       OR LOWER(subcode_label) LIKE '%digital%'
       OR LOWER(subcode_label) LIKE '%technology%'
       OR LOWER(subcode_label) LIKE '%information%'
       OR LOWER(subcode_label) LIKE '%computer%'
       OR LOWER(subcode_label) LIKE '%software%'
       OR LOWER(subcode_label) LIKE '%hardware%'
       OR LOWER(subcode_label) LIKE '%system%'
)
SELECT
    SubCode,
    MAX(subcode_label) as subcode_label,
    MAX(WorkSheetName) as WorkSheetName,
    COUNT(DISTINCT fy) as years_present
FROM filtered_codes
GROUP BY SubCode
ORDER BY WorkSheetName, SubCode
"""

it_codes = con.execute(it_search_query).fetchdf()
print(f"\nFound {len(it_codes)} IT-related subcodes:\n")
print(it_codes.to_string(index=False))
it_codes.to_csv(OUTPUT_DIR / "it_related_codes.csv", index=False)
print(f"\n✓ Saved to: {OUTPUT_DIR / 'it_related_codes.csv'}")

# Get sample spending
if len(it_codes) > 0:
    print("\n" + "-" * 80)
    print("Sample spending data for IT codes (2023-24):")
    print("-" * 80)

    it_codes_list = "', '".join(it_codes['SubCode'].tolist())
    it_sample_query = f"""
    SELECT
        f.SubCode,
        MAX(d.subcode_label) as subcode_label,
        f.WorkSheetName,
        COUNT(*) as num_records,
        COUNT(DISTINCT f.org_name_raw) as num_orgs,
        SUM(f.amount) as total_amount,
        AVG(f.amount) as avg_amount
    FROM fact_tru_tac f
    LEFT JOIN dim_tac_subcodes d ON f.SubCode = d.SubCode AND f.fy = d.fy
    WHERE f.SubCode IN ('{it_codes_list}')
      AND f.fy = '2023-24'
    GROUP BY f.SubCode, f.WorkSheetName
    ORDER BY total_amount DESC
    """

    it_sample = con.execute(it_sample_query).fetchdf()
    print(it_sample.to_string(index=False))
    it_sample.to_csv(OUTPUT_DIR / "it_codes_sample_2023-24.csv", index=False)

# ============================================================================
# CONSULTANCY
# ============================================================================
print("\n" + "=" * 80)
print("2. CONSULTANCY-RELATED SUBCODES")
print("=" * 80)

consultancy_search_query = """
WITH filtered_codes AS (
    SELECT fy, WorkSheetName, SubCode, subcode_label
    FROM dim_tac_subcodes
    WHERE LOWER(subcode_label) LIKE '%consult%'
       OR LOWER(subcode_label) LIKE '%advisory%'
       OR LOWER(subcode_label) LIKE '%professional%'
       OR LOWER(subcode_label) LIKE '%contractor%'
       OR LOWER(subcode_label) LIKE '%outsourc%'
)
SELECT
    SubCode,
    MAX(subcode_label) as subcode_label,
    MAX(WorkSheetName) as WorkSheetName,
    COUNT(DISTINCT fy) as years_present
FROM filtered_codes
GROUP BY SubCode
ORDER BY WorkSheetName, SubCode
"""

consultancy_codes = con.execute(consultancy_search_query).fetchdf()
print(f"\nFound {len(consultancy_codes)} consultancy-related subcodes:\n")
print(consultancy_codes.to_string(index=False))
consultancy_codes.to_csv(OUTPUT_DIR / "consultancy_related_codes.csv", index=False)
print(f"\n✓ Saved to: {OUTPUT_DIR / 'consultancy_related_codes.csv'}")

# Get sample spending
if len(consultancy_codes) > 0:
    print("\n" + "-" * 80)
    print("Sample spending data for consultancy codes (2023-24):")
    print("-" * 80)

    consultancy_codes_list = "', '".join(consultancy_codes['SubCode'].tolist())
    consultancy_sample_query = f"""
    SELECT
        f.SubCode,
        MAX(d.subcode_label) as subcode_label,
        f.WorkSheetName,
        COUNT(*) as num_records,
        COUNT(DISTINCT f.org_name_raw) as num_orgs,
        SUM(f.amount) as total_amount,
        AVG(f.amount) as avg_amount
    FROM fact_tru_tac f
    LEFT JOIN dim_tac_subcodes d ON f.SubCode = d.SubCode AND f.fy = d.fy
    WHERE f.SubCode IN ('{consultancy_codes_list}')
      AND f.fy = '2023-24'
    GROUP BY f.SubCode, f.WorkSheetName
    ORDER BY total_amount DESC
    """

    consultancy_sample = con.execute(consultancy_sample_query).fetchdf()
    print(consultancy_sample.to_string(index=False))
    consultancy_sample.to_csv(OUTPUT_DIR / "consultancy_codes_sample_2023-24.csv", index=False)

# ============================================================================
# INTANGIBLES
# ============================================================================
print("\n" + "=" * 80)
print("3. INTANGIBLE ASSETS (SOFTWARE/IT ON BALANCE SHEET)")
print("=" * 80)

intangibles_query = """
WITH filtered_codes AS (
    SELECT fy, WorkSheetName, SubCode, subcode_label
    FROM dim_tac_subcodes
    WHERE WorkSheetName = 'TAC13 Intangibles'
)
SELECT
    SubCode,
    MAX(subcode_label) as subcode_label,
    MAX(WorkSheetName) as WorkSheetName,
    COUNT(DISTINCT fy) as years_present
FROM filtered_codes
GROUP BY SubCode
ORDER BY SubCode
"""

intangibles_codes = con.execute(intangibles_query).fetchdf()
print(f"\nFound {len(intangibles_codes)} intangible asset subcodes:\n")
print(intangibles_codes.to_string(index=False))
intangibles_codes.to_csv(OUTPUT_DIR / "intangibles_codes.csv", index=False)
print(f"\n✓ Saved to: {OUTPUT_DIR / 'intangibles_codes.csv'}")

# Get sample data
print("\n" + "-" * 80)
print("Sample intangible asset values (2023-24) - Top 20:")
print("-" * 80)

intangibles_sample_query = """
SELECT
    f.SubCode,
    MAX(d.subcode_label) as subcode_label,
    COUNT(*) as num_records,
    COUNT(DISTINCT f.org_name_raw) as num_orgs,
    SUM(f.amount) as total_amount,
    AVG(f.amount) as avg_amount
FROM fact_tru_tac f
LEFT JOIN dim_tac_subcodes d ON f.SubCode = d.SubCode AND f.fy = d.fy
WHERE f.WorkSheetName = 'TAC13 Intangibles'
  AND f.fy = '2023-24'
GROUP BY f.SubCode
ORDER BY total_amount DESC
LIMIT 20
"""

intangibles_sample = con.execute(intangibles_sample_query).fetchdf()
print(intangibles_sample.to_string(index=False))
intangibles_sample.to_csv(OUTPUT_DIR / "intangibles_sample_2023-24.csv", index=False)

# ============================================================================
# OPERATING EXPENSES
# ============================================================================
print("\n" + "=" * 80)
print("4. IT/CONSULTANCY OPERATING EXPENSES")
print("=" * 80)

opex_query = """
WITH filtered_codes AS (
    SELECT fy, WorkSheetName, SubCode, subcode_label
    FROM dim_tac_subcodes
    WHERE WorkSheetName = 'TAC08 Op Exp'
      AND (LOWER(subcode_label) LIKE '%it%'
       OR LOWER(subcode_label) LIKE '%consult%'
       OR LOWER(subcode_label) LIKE '%professional%')
)
SELECT
    SubCode,
    MAX(subcode_label) as subcode_label,
    MAX(WorkSheetName) as WorkSheetName,
    COUNT(DISTINCT fy) as years_present
FROM filtered_codes
GROUP BY SubCode
ORDER BY SubCode
"""

opex_codes = con.execute(opex_query).fetchdf()
print(f"\nFound {len(opex_codes)} IT/consultancy operating expense codes:\n")
print(opex_codes.to_string(index=False))
opex_codes.to_csv(OUTPUT_DIR / "opex_it_consultancy_codes.csv", index=False)

con.close()

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"\nTotal codes identified:")
print(f"  IT-related codes: {len(it_codes)}")
print(f"  Consultancy codes: {len(consultancy_codes)}")
print(f"  Intangible asset codes: {len(intangibles_codes)}")
print(f"  Operating expense codes: {len(opex_codes)}")

print(f"\n✓ All results saved to: {OUTPUT_DIR.absolute()}")
print("\nReview the CSV files to verify which codes should be included.")
print("Once verified, run: python src\\analyze_it_consultancy.py")

print("\n" + "=" * 80)
