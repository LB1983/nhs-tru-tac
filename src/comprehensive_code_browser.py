#!/usr/bin/env python3
"""
Comprehensive subcode browser - shows all codes for manual review
"""

import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path("Data/canonical/tru_tac.duckdb")
OUTPUT_DIR = Path("Data/analysis/code_discovery")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 80)
print("NHS TAC COMPREHENSIVE CODE BROWSER")
print("=" * 80)

con = duckdb.connect(str(DB_PATH), read_only=True)

# ============================================================================
# 1. ALL OPERATING EXPENSE CODES
# ============================================================================
print("\n" + "=" * 80)
print("1. ALL OPERATING EXPENSE CODES (TAC08)")
print("=" * 80)

opex_all = con.execute("""
    SELECT
        SubCode,
        MAX(subcode_label) as subcode_label,
        COUNT(DISTINCT fy) as years_present
    FROM dim_tac_subcodes_ws
    WHERE LOWER(ws_key) LIKE '%opexp%'
    GROUP BY SubCode
    ORDER BY SubCode
""").fetchdf()

print(f"\nTotal operating expense codes: {len(opex_all)}")
print("\nShowing all codes - review for IT and consultancy:")
print(opex_all.to_string(index=False))
opex_all.to_csv(OUTPUT_DIR / "all_operating_expense_codes.csv", index=False)
print(f"\n✓ Saved to: all_operating_expense_codes.csv")

# Get spending for top codes
print("\n" + "-" * 80)
print("Top 30 operating expense codes by 2023-24 spending:")
print("-" * 80)

opex_spending = con.execute("""
    SELECT
        f.SubCode,
        MAX(d.subcode_label) as subcode_label,
        COUNT(DISTINCT f.org_name_raw) as num_orgs,
        SUM(f.amount) as total_amount
    FROM fact_tru_tac f
    LEFT JOIN dim_tac_subcodes_ws d ON f.SubCode = d.SubCode AND f.fy = d.fy
    WHERE f.WorkSheetName = 'TAC08 Op Exp'
      AND f.fy = '2023-24'
    GROUP BY f.SubCode
    ORDER BY total_amount DESC
    LIMIT 30
""").fetchdf()

print(opex_spending.to_string(index=False))
opex_spending.to_csv(OUTPUT_DIR / "top_opex_by_spending_2023-24.csv", index=False)

# ============================================================================
# 2. ALL INTANGIBLE CODES WITH LABELS
# ============================================================================
print("\n" + "=" * 80)
print("2. ALL INTANGIBLE ASSET CODES")
print("=" * 80)

intangibles_all = con.execute("""
    SELECT
        SubCode,
        MAX(subcode_label) as subcode_label,
        COUNT(DISTINCT fy) as years_present
    FROM dim_tac_subcodes_ws
    WHERE LOWER(ws_key) LIKE '%intang%'
    GROUP BY SubCode
    ORDER BY SubCode
""").fetchdf()

print(f"\nTotal intangible codes: {len(intangibles_all)}")
print("\nShowing first 50 - review for IT/software related:")
print(intangibles_all.head(50).to_string(index=False))
if len(intangibles_all) > 50:
    print(f"\n... and {len(intangibles_all) - 50} more in CSV file")
intangibles_all.to_csv(OUTPUT_DIR / "all_intangible_codes.csv", index=False)
print(f"\n✓ Saved to: all_intangible_codes.csv")

# Get values for top codes
print("\n" + "-" * 80)
print("Top 30 intangible codes by 2023-24 values:")
print("-" * 80)

intangibles_values = con.execute("""
    SELECT
        f.SubCode,
        MAX(d.subcode_label) as subcode_label,
        COUNT(DISTINCT f.org_name_raw) as num_orgs,
        SUM(f.amount) as total_amount
    FROM fact_tru_tac f
    LEFT JOIN dim_tac_subcodes_ws d ON f.SubCode = d.SubCode AND f.fy = d.fy
    WHERE f.WorkSheetName = 'TAC13 Intangibles'
      AND f.fy = '2023-24'
    GROUP BY f.SubCode
    ORDER BY total_amount DESC
    LIMIT 30
""").fetchdf()

print(intangibles_values.to_string(index=False))
intangibles_values.to_csv(OUTPUT_DIR / "top_intangibles_by_value_2023-24.csv", index=False)

# ============================================================================
# 3. SEARCH SPECIFIC TERMS
# ============================================================================
print("\n" + "=" * 80)
print("3. TARGETED SEARCHES")
print("=" * 80)

# Search for specific IT-related terms more carefully
print("\nSearching for 'computer' or 'digital' or 'information technology':")
it_specific = con.execute("""
    SELECT
        SubCode,
        MAX(subcode_label) as subcode_label,
        MAX(ws_key) as worksheet,
        COUNT(DISTINCT fy) as years_present
    FROM dim_tac_subcodes_ws
    WHERE LOWER(subcode_label) LIKE '%computer%'
       OR LOWER(subcode_label) LIKE '%digital%'
       OR LOWER(subcode_label) LIKE '%information technology%'
       OR LOWER(subcode_label) LIKE '%software license%'
       OR LOWER(subcode_label) LIKE '%software licence%'
       OR LOWER(subcode_label) LIKE '%it service%'
       OR LOWER(subcode_label) LIKE '%it cost%'
    GROUP BY SubCode
    ORDER BY worksheet, SubCode
""").fetchdf()
print(f"Found {len(it_specific)} codes")
print(it_specific.to_string(index=False))
it_specific.to_csv(OUTPUT_DIR / "specific_it_search.csv", index=False)

# Search for consultancy/advisory
print("\n\nSearching for 'consultancy' or 'advisory':")
consultancy_specific = con.execute("""
    SELECT
        SubCode,
        MAX(subcode_label) as subcode_label,
        MAX(ws_key) as worksheet,
        COUNT(DISTINCT fy) as years_present
    FROM dim_tac_subcodes_ws
    WHERE LOWER(subcode_label) LIKE '%consultanc%'
       OR LOWER(subcode_label) LIKE '%advisory%'
       OR LOWER(subcode_label) LIKE '%management consultanc%'
       OR LOWER(subcode_label) LIKE '%professional fee%'
       OR LOWER(subcode_label) LIKE '%professional service%'
    GROUP BY SubCode
    ORDER BY worksheet, SubCode
""").fetchdf()
print(f"Found {len(consultancy_specific)} codes")
print(consultancy_specific.to_string(index=False))
consultancy_specific.to_csv(OUTPUT_DIR / "specific_consultancy_search.csv", index=False)

# ============================================================================
# 4. STAFF COSTS - CHECK IF IT STAFF IS THERE
# ============================================================================
print("\n" + "=" * 80)
print("4. STAFF COSTS (TAC09) - Checking for IT staff")
print("=" * 80)

staff_all = con.execute("""
    SELECT
        SubCode,
        MAX(subcode_label) as subcode_label,
        COUNT(DISTINCT fy) as years_present
    FROM dim_tac_subcodes_ws
    WHERE LOWER(ws_key) LIKE '%staff%'
    GROUP BY SubCode
    ORDER BY SubCode
    LIMIT 50
""").fetchdf()

print(f"\nShowing first 50 staff cost codes:")
print(staff_all.to_string(index=False))
staff_all_full = con.execute("""
    SELECT
        SubCode,
        MAX(subcode_label) as subcode_label,
        COUNT(DISTINCT fy) as years_present
    FROM dim_tac_subcodes_ws
    WHERE LOWER(ws_key) LIKE '%staff%'
    GROUP BY SubCode
    ORDER BY SubCode
""").fetchdf()
staff_all_full.to_csv(OUTPUT_DIR / "all_staff_codes.csv", index=False)

# ============================================================================
# 5. CAPITAL EXPENDITURE - PPE ADDITIONS
# ============================================================================
print("\n" + "=" * 80)
print("5. CAPITAL EXPENDITURE - PPE Additions")
print("=" * 80)

ppe_additions = con.execute("""
    SELECT
        SubCode,
        MAX(subcode_label) as subcode_label,
        MAX(ws_key) as worksheet,
        COUNT(DISTINCT fy) as years_present
    FROM dim_tac_subcodes_ws
    WHERE LOWER(ws_key) LIKE '%ppe%'
      AND (LOWER(subcode_label) LIKE '%addition%'
       OR LOWER(subcode_label) LIKE '%purchase%'
       OR LOWER(subcode_label) LIKE '%acquisition%')
    GROUP BY SubCode
    ORDER BY SubCode
""").fetchdf()

print(f"\nFound {len(ppe_additions)} PPE addition codes:")
print(ppe_additions.to_string(index=False))
ppe_additions.to_csv(OUTPUT_DIR / "ppe_additions.csv", index=False)

con.close()

# ============================================================================
# SUMMARY & NEXT STEPS
# ============================================================================
print("\n" + "=" * 80)
print("SUMMARY & NEXT STEPS")
print("=" * 80)
print(f"\nGenerated comprehensive code lists:")
print(f"  1. all_operating_expense_codes.csv ({len(opex_all)} codes)")
print(f"  2. all_intangible_codes.csv ({len(intangibles_all)} codes)")
print(f"  3. top_opex_by_spending_2023-24.csv (top 30)")
print(f"  4. top_intangibles_by_value_2023-24.csv (top 30)")
print(f"  5. specific_it_search.csv ({len(it_specific)} codes)")
print(f"  6. specific_consultancy_search.csv ({len(consultancy_specific)} codes)")
print(f"  7. all_staff_codes.csv")
print(f"  8. ppe_additions.csv ({len(ppe_additions)} codes)")

print(f"\n✓ All files saved to: {OUTPUT_DIR.absolute()}")

print("\n" + "=" * 80)
print("NEXT STEPS:")
print("=" * 80)
print("\n1. Review the CSV files, especially:")
print("   - all_operating_expense_codes.csv")
print("   - all_intangible_codes.csv")
print("   - top_opex_by_spending_2023-24.csv")
print("\n2. Identify which SubCodes are actually IT or consultancy related")
print("\n3. Create a list of SubCodes to analyze")
print("\n4. We can then run targeted analysis on those specific codes")
print("\n" + "=" * 80)
