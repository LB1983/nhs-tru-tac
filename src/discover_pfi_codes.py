#!/usr/bin/env python3
"""
Discover PFI (Private Finance Initiative) related codes in TAC database
"""

import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path("Data/canonical/tru_tac.duckdb")

print("=" * 80)
print("PFI CODE DISCOVERY")
print("=" * 80)

con = duckdb.connect(str(DB_PATH), read_only=True)

# Search for PFI-related codes
print("\n[1/3] Searching for PFI-related codes...")

pfi_codes = con.execute("""
    SELECT DISTINCT
        SubCode,
        subcode_label,
        ws_key,
        COUNT(DISTINCT fy) as years_present
    FROM dim_tac_subcodes_ws
    WHERE LOWER(subcode_label) LIKE '%pfi%'
       OR LOWER(subcode_label) LIKE '%private finance%'
       OR LOWER(subcode_label) LIKE '%finance lease%'
       OR LOWER(subcode_label) LIKE '%service concession%'
    GROUP BY SubCode, subcode_label, ws_key
    ORDER BY SubCode
""").fetchdf()

print(f"\n✓ Found {len(pfi_codes)} PFI-related codes:")
print(pfi_codes.to_string(index=False))

if len(pfi_codes) == 0:
    print("\n⚠️  No direct PFI codes found. Let me search for related terms...")

    # Try broader search
    related_codes = con.execute("""
        SELECT DISTINCT
            SubCode,
            subcode_label,
            ws_key
        FROM dim_tac_subcodes_ws
        WHERE LOWER(subcode_label) LIKE '%lease%'
           OR LOWER(subcode_label) LIKE '%finance cost%'
           OR LOWER(subcode_label) LIKE '%capital charge%'
           OR LOWER(subcode_label) LIKE '%depreciation%'
        ORDER BY SubCode
        LIMIT 20
    """).fetchdf()

    print("\nRelated codes (leases, finance costs, capital charges):")
    print(related_codes.to_string(index=False))

# Check specific worksheets that might contain PFI data
print("\n[2/3] Checking relevant worksheets...")

worksheets = con.execute("""
    SELECT DISTINCT ws_key
    FROM dim_tac_subcodes_ws
    WHERE LOWER(ws_key) LIKE '%pfi%'
       OR LOWER(ws_key) LIKE '%capital%'
       OR LOWER(ws_key) LIKE '%lease%'
       OR LOWER(ws_key) LIKE '%balance%'
    ORDER BY ws_key
""").fetchdf()

print(f"\n✓ Relevant worksheets ({len(worksheets)}):")
for ws in worksheets['ws_key']:
    print(f"  - {ws}")

# Get sample data if PFI codes exist
if len(pfi_codes) > 0:
    print("\n[3/3] Sample PFI spending data...")

    codes_list = "', '".join(pfi_codes['SubCode'].tolist())

    sample_data = con.execute(f"""
        SELECT
            f.org_name_raw,
            f.fy,
            f.SubCode,
            d.ws_key,
            SUM(f.amount) as total_amount
        FROM fact_tru_tac f
        JOIN dim_tac_subcodes_ws d ON f.SubCode = d.SubCode
        WHERE f.SubCode IN ('{codes_list}')
        GROUP BY f.org_name_raw, f.fy, f.SubCode, d.ws_key
        ORDER BY total_amount DESC
        LIMIT 10
    """).fetchdf()

    print("\nTop 10 PFI spending records:")
    print(sample_data.to_string(index=False))

    # Summary stats
    total_pfi = con.execute(f"""
        SELECT
            COUNT(DISTINCT org_name_raw) as num_orgs,
            COUNT(DISTINCT fy) as num_years,
            SUM(amount) as total_pfi
        FROM fact_tru_tac
        WHERE SubCode IN ('{codes_list}')
    """).fetchdf()

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"PFI codes identified: {len(pfi_codes)}")
    print(f"Organizations with PFI data: {total_pfi['num_orgs'].iloc[0]}")
    print(f"Financial years covered: {total_pfi['num_years'].iloc[0]}")
    print(f"Total PFI value in database: £{total_pfi['total_pfi'].iloc[0]:,.0f}")

con.close()

print("\n✓ PFI code discovery complete!")
