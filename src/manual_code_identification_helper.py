#!/usr/bin/env python3
"""
Analyze user-identified codes with spending data to help identify IT
"""

import duckdb
import pandas as pd
from pathlib import Path

DB_PATH = Path("Data/canonical/tru_tac.duckdb")
OUTPUT_DIR = Path("Data/analysis/code_discovery")

print("=" * 80)
print("MANUAL CODE IDENTIFICATION HELPER")
print("=" * 80)

con = duckdb.connect(str(DB_PATH), read_only=True)

# ============================================================================
# Let's look at the TOP spending categories to identify IT manually
# ============================================================================
print("\n1. TOP 50 OPERATING EXPENSE ITEMS BY TOTAL SPENDING (ALL YEARS)")
print("=" * 80)
print("Review these for IT-related items...")
print()

top_opex = con.execute("""
    SELECT
        f.SubCode,
        MAX(d.subcode_label) as subcode_label,
        COUNT(DISTINCT f.org_name_raw) as num_orgs,
        COUNT(DISTINCT f.fy) as num_years,
        SUM(f.amount) as total_amount_all_years,
        SUM(CASE WHEN f.fy = '2023-24' THEN f.amount ELSE 0 END) as amount_2023_24
    FROM fact_tru_tac f
    LEFT JOIN dim_tac_subcodes_ws d ON f.SubCode = d.SubCode AND f.fy = d.fy
    WHERE f.WorkSheetName = 'TAC08 Op Exp'
    GROUP BY f.SubCode
    ORDER BY total_amount_all_years DESC
    LIMIT 50
""").fetchdf()

# Format for readability
top_opex['total_amount_millions'] = top_opex['total_amount_all_years'] / 1_000_000
top_opex['amount_2023_24_millions'] = top_opex['amount_2023_24'] / 1_000_000

display_cols = ['SubCode', 'subcode_label', 'num_orgs', 'total_amount_millions', 'amount_2023_24_millions']
print(top_opex[display_cols].to_string(index=False))
top_opex.to_csv(OUTPUT_DIR / "top_50_opex_for_manual_review.csv", index=False)
print(f"\n✓ Saved to: top_50_opex_for_manual_review.csv")

# ============================================================================
# Show intangibles with context
# ============================================================================
print("\n" + "=" * 80)
print("2. TOP 30 INTANGIBLE ASSETS BY VALUE (ALL YEARS)")
print("=" * 80)
print("Software, licenses, and development costs would be IT-related...")
print()

top_intangibles = con.execute("""
    SELECT
        f.SubCode,
        MAX(d.subcode_label) as subcode_label,
        COUNT(DISTINCT f.org_name_raw) as num_orgs,
        COUNT(DISTINCT f.fy) as num_years,
        SUM(f.amount) as total_value_all_years,
        SUM(CASE WHEN f.fy = '2023-24' THEN f.amount ELSE 0 END) as value_2023_24
    FROM fact_tru_tac f
    LEFT JOIN dim_tac_subcodes_ws d ON f.SubCode = d.SubCode AND f.fy = d.fy
    WHERE f.WorkSheetName = 'TAC13 Intangibles'
    GROUP BY f.SubCode
    ORDER BY total_value_all_years DESC
    LIMIT 30
""").fetchdf()

top_intangibles['total_value_millions'] = top_intangibles['total_value_all_years'] / 1_000_000
top_intangibles['value_2023_24_millions'] = top_intangibles['value_2023_24'] / 1_000_000

display_cols = ['SubCode', 'subcode_label', 'num_orgs', 'total_value_millions', 'value_2023_24_millions']
print(top_intangibles[display_cols].to_string(index=False))
top_intangibles.to_csv(OUTPUT_DIR / "top_30_intangibles_for_manual_review.csv", index=False)
print(f"\n✓ Saved to: top_30_intangibles_for_manual_review.csv")

# ============================================================================
# Consultancy codes from previous search
# ============================================================================
print("\n" + "=" * 80)
print("3. CONSULTANCY CODES IDENTIFIED")
print("=" * 80)

consultancy_codes = con.execute("""
    SELECT
        SubCode,
        MAX(subcode_label) as subcode_label,
        MAX(ws_key) as worksheet,
        COUNT(DISTINCT fy) as years_present
    FROM dim_tac_subcodes_ws
    WHERE LOWER(subcode_label) LIKE '%consult%'
       OR LOWER(subcode_label) LIKE '%advisory%'
       OR LOWER(subcode_label) LIKE '%professional%'
    GROUP BY SubCode
    ORDER BY worksheet, SubCode
""").fetchdf()

print(f"\nFound {len(consultancy_codes)} consultancy codes:")
print(consultancy_codes.to_string(index=False))

# Get their spending
if len(consultancy_codes) > 0:
    codes_list = "', '".join(consultancy_codes['SubCode'].tolist())

    consultancy_spending = con.execute(f"""
        SELECT
            f.fy,
            SUM(f.amount) as total_consultancy_spend
        FROM fact_tru_tac f
        WHERE f.SubCode IN ('{codes_list}')
        GROUP BY f.fy
        ORDER BY f.fy
    """).fetchdf()

    print("\nConsultancy spending by year:")
    consultancy_spending['spend_millions'] = consultancy_spending['total_consultancy_spend'] / 1_000_000
    print(consultancy_spending[['fy', 'spend_millions']].to_string(index=False))
    consultancy_spending.to_csv(OUTPUT_DIR / "consultancy_spending_by_year.csv", index=False)

con.close()

# ============================================================================
# Instructions for user
# ============================================================================
print("\n" + "=" * 80)
print("NEXT STEPS - MANUAL CODE IDENTIFICATION")
print("=" * 80)

print("""
TO IDENTIFY IT CODES:

1. Open: top_50_opex_for_manual_review.csv
   - Look through the 'subcode_label' column
   - Identify which ones are IT-related (could be things like:
     • Premises costs (might include data centers)
     • Establishment costs
     • Clinical support services
     • Specific IT contracts
     • Hosting/telecommunications
   - Note down the SubCodes

2. Open: top_30_intangibles_for_manual_review.csv
   - Look for software, licenses, development costs
   - These are IT assets on the balance sheet
   - Note down the SubCodes

3. Create a text file called 'it_codes_identified.txt' with the SubCodes:

   Example content:
   EXP0123
   EXP0456
   INT0789

   (one SubCode per line)

4. Once you've identified the codes, let me know and I'll create
   a targeted analysis script using exactly those codes.

CONSULTANCY CODES:
Already identified {len(consultancy_codes)} consultancy codes.
Total 2023-24 spend: £{consultancy_spending[consultancy_spending['fy']=='2023-24']['spend_millions'].values[0] if len(consultancy_spending[consultancy_spending['fy']=='2023-24']) > 0 else 0:.1f}M

FILES GENERATED:
  - top_50_opex_for_manual_review.csv
  - top_30_intangibles_for_manual_review.csv
  - consultancy_spending_by_year.csv
""")

print("=" * 80)
