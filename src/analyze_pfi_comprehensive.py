#!/usr/bin/env python3
"""
Comprehensive PFI (Private Finance Initiative) Analysis
- Discovers PFI codes in TAC database
- Separates capital and revenue PFI spending
- Calculates PFI per bed
- Identifies organizations with highest PFI commitments
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

DB_PATH = Path("Data/canonical/tru_tac.duckdb")
BEDS_FILE = Path("Data/analysis/activity_integrated/beds_historical_matched.csv")
OUTPUT_DIR = Path("Data/analysis/pfi_analysis")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 10)

print("=" * 80)
print("PFI ANALYSIS - CAPITAL & REVENUE COSTS PER BED")
print("=" * 80)

con = duckdb.connect(str(DB_PATH), read_only=True)

# ============================================================================
# DISCOVER PFI CODES
# ============================================================================
print("\n[1/5] Discovering PFI-related codes...")

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
for _, row in pfi_codes.iterrows():
    print(f"  {row['SubCode']}: {row['subcode_label']} ({row['ws_key']})")

if len(pfi_codes) == 0:
    print("\n⚠️  No direct PFI codes found.")
    print("Searching for related terms (lease, finance costs, capital charges)...")

    pfi_codes = con.execute("""
        SELECT DISTINCT
            SubCode,
            subcode_label,
            ws_key
        FROM dim_tac_subcodes_ws
        WHERE LOWER(subcode_label) LIKE '%lease%'
           OR LOWER(subcode_label) LIKE '%finance cost%'
           OR LOWER(subcode_label) LIKE '%capital charge%'
        ORDER BY SubCode
        LIMIT 30
    """).fetchdf()

    print(f"\n✓ Found {len(pfi_codes)} related codes")

# ============================================================================
# CATEGORIZE AS CAPITAL OR REVENUE
# ============================================================================
print("\n[2/5] Categorizing PFI codes as Capital or Revenue...")

# Determine if code is capital or revenue based on worksheet and label
def categorize_pfi(row):
    label = row['subcode_label'].lower()
    worksheet = row['ws_key'].lower() if pd.notna(row['ws_key']) else ''

    # Capital indicators
    if 'capital' in label or 'capital' in worksheet:
        return 'Capital'
    elif 'depreciation' in label or 'impairment' in label:
        return 'Capital'
    elif 'balance sheet' in worksheet or 'sofp' in worksheet:
        return 'Capital'

    # Revenue indicators
    elif 'revenue' in label or 'operating' in label:
        return 'Revenue'
    elif 'expense' in label or 'cost' in label:
        return 'Revenue'
    elif 'soci' in worksheet or 'income' in worksheet:
        return 'Revenue'

    # Default to Revenue for PFI operating costs
    return 'Revenue'

pfi_codes['pfi_type'] = pfi_codes.apply(categorize_pfi, axis=1)

print("\nCapital PFI codes:")
capital_codes = pfi_codes[pfi_codes['pfi_type'] == 'Capital']
for _, row in capital_codes.iterrows():
    print(f"  {row['SubCode']}: {row['subcode_label']}")

print("\nRevenue PFI codes:")
revenue_codes = pfi_codes[pfi_codes['pfi_type'] == 'Revenue']
for _, row in revenue_codes.iterrows():
    print(f"  {row['SubCode']}: {row['subcode_label']}")

# ============================================================================
# EXTRACT PFI SPENDING DATA
# ============================================================================
print("\n[3/5] Extracting PFI spending data...")

codes_list = "', '".join(pfi_codes['SubCode'].tolist())

pfi_data = con.execute(f"""
    SELECT
        f.org_name_raw,
        f.sector,
        f.fy,
        f.SubCode,
        d.subcode_label,
        d.ws_key,
        SUM(f.amount) as amount
    FROM fact_tru_tac f
    JOIN dim_tac_subcodes_ws d ON f.SubCode = d.SubCode
    WHERE f.SubCode IN ('{codes_list}')
    GROUP BY f.org_name_raw, f.sector, f.fy, f.SubCode, d.subcode_label, d.ws_key
""").fetchdf()

# Add PFI type
pfi_data = pfi_data.merge(
    pfi_codes[['SubCode', 'pfi_type']],
    on='SubCode',
    how='left'
)

print(f"✓ Loaded {len(pfi_data)} PFI records")
print(f"  Organizations: {pfi_data['org_name_raw'].nunique()}")
print(f"  Financial years: {sorted(pfi_data['fy'].unique())}")

# Aggregate by org, year, and type
pfi_summary = pfi_data.groupby(['org_name_raw', 'sector', 'fy', 'pfi_type'])['amount'].sum().reset_index()
pfi_summary.columns = ['org_name_raw', 'sector', 'fy', 'pfi_type', 'pfi_spend']

# Pivot to get capital and revenue as separate columns
pfi_wide = pfi_summary.pivot_table(
    index=['org_name_raw', 'sector', 'fy'],
    columns='pfi_type',
    values='pfi_spend',
    fill_value=0
).reset_index()

pfi_wide['total_pfi'] = pfi_wide.get('Capital', 0) + pfi_wide.get('Revenue', 0)

print(f"\n✓ Aggregated to {len(pfi_wide)} org-year records")

# ============================================================================
# MERGE WITH BED DATA
# ============================================================================
print("\n[4/5] Merging with bed data for per-bed metrics...")

if BEDS_FILE.exists():
    beds = pd.read_csv(BEDS_FILE)
    print(f"✓ Loaded bed data: {len(beds)} records")

    # Merge PFI with beds
    pfi_with_beds = pfi_wide.merge(
        beds[['org_name_raw', 'fy', 'beds']],
        on=['org_name_raw', 'fy'],
        how='inner'
    )

    # Calculate per-bed metrics
    if 'Capital' in pfi_with_beds.columns:
        pfi_with_beds['capital_pfi_per_bed'] = pfi_with_beds['Capital'] / pfi_with_beds['beds']
    else:
        pfi_with_beds['capital_pfi_per_bed'] = 0

    if 'Revenue' in pfi_with_beds.columns:
        pfi_with_beds['revenue_pfi_per_bed'] = pfi_with_beds['Revenue'] / pfi_with_beds['beds']
    else:
        pfi_with_beds['revenue_pfi_per_bed'] = 0

    pfi_with_beds['total_pfi_per_bed'] = pfi_with_beds['total_pfi'] / pfi_with_beds['beds']

    # Filter out invalid values
    import numpy as np
    pfi_with_beds = pfi_with_beds.replace([np.inf, -np.inf], np.nan)
    pfi_with_beds = pfi_with_beds[pfi_with_beds['total_pfi_per_bed'].notna()]
    pfi_with_beds = pfi_with_beds[pfi_with_beds['beds'] > 0]

    print(f"✓ Merged data: {len(pfi_with_beds)} org-year records")
    print(f"  Organizations with both PFI and bed data: {pfi_with_beds['org_name_raw'].nunique()}")

    # Save detailed data
    pfi_with_beds.to_csv(OUTPUT_DIR / "pfi_with_beds.csv", index=False)
    print(f"✓ Saved: {OUTPUT_DIR / 'pfi_with_beds.csv'}")

else:
    print("⚠️  Bed data not found - analysis will be without per-bed metrics")
    pfi_with_beds = pfi_wide.copy()

# ============================================================================
# ANALYSIS
# ============================================================================
print("\n[5/5] Analyzing PFI spending...")

latest_year = pfi_with_beds['fy'].max()
latest = pfi_with_beds[pfi_with_beds['fy'] == latest_year].copy()

print(f"\nPFI Summary ({latest_year}):")
print(f"  Organizations with PFI: {len(latest)}")

if 'capital_pfi_per_bed' in latest.columns and 'revenue_pfi_per_bed' in latest.columns:
    print(f"\n  Capital PFI per bed:")
    print(f"    Mean: £{latest['capital_pfi_per_bed'].mean():,.0f}")
    print(f"    Median: £{latest['capital_pfi_per_bed'].median():,.0f}")
    print(f"    Max: £{latest['capital_pfi_per_bed'].max():,.0f}")

    print(f"\n  Revenue PFI per bed:")
    print(f"    Mean: £{latest['revenue_pfi_per_bed'].mean():,.0f}")
    print(f"    Median: £{latest['revenue_pfi_per_bed'].median():,.0f}")
    print(f"    Max: £{latest['revenue_pfi_per_bed'].max():,.0f}")

    print(f"\n  Total PFI per bed:")
    print(f"    Mean: £{latest['total_pfi_per_bed'].mean():,.0f}")
    print(f"    Median: £{latest['total_pfi_per_bed'].median():,.0f}")
    print(f"    Max: £{latest['total_pfi_per_bed'].max():,.0f}")

# Top PFI spenders
print(f"\nTop 10 organizations by total PFI per bed ({latest_year}):")
if 'total_pfi_per_bed' in latest.columns:
    top_pfi = latest.nlargest(10, 'total_pfi_per_bed')[
        ['org_name_raw', 'beds', 'total_pfi', 'total_pfi_per_bed', 'capital_pfi_per_bed', 'revenue_pfi_per_bed']
    ]
    for _, row in top_pfi.iterrows():
        print(f"  {row['org_name_raw'][:50]}")
        print(f"    Beds: {row['beds']:.0f}")
        print(f"    Total PFI: £{row['total_pfi']:,.0f} (£{row['total_pfi_per_bed']:,.0f}/bed)")
        print(f"    Capital: £{row['capital_pfi_per_bed']:,.0f}/bed | Revenue: £{row['revenue_pfi_per_bed']:,.0f}/bed")

con.close()

print("\n" + "=" * 80)
print("PFI ANALYSIS COMPLETE")
print("=" * 80)
print(f"\nOutput saved to: {OUTPUT_DIR}")
print("\nNext step: Review pfi_with_beds.csv for detailed organization-level data")
