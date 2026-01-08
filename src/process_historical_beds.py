#!/usr/bin/env python3
"""
Process historical NHS bed data from multiple quarterly files (Q4 2017-18 to Q4 2023-24)
"""

import pandas as pd
import duckdb
from pathlib import Path
import re

print("=" * 80)
print("NHS HISTORICAL BED DATA PROCESSOR")
print("=" * 80)

# ============================================================================
# CONFIGURATION
# ============================================================================
activity_dir = Path('Data/activity')
output_dir = Path('Data/analysis/activity_integrated')
output_dir.mkdir(parents=True, exist_ok=True)

db_path = Path('Data/canonical/tru_tac.duckdb')

# Map file names to financial years
bed_files = {
    'beds_Q4_201718.xlsx': '2017-18',
    'beds_Q4_201819.xlsx': '2018-19',
    'beds_Q4_201920.xlsx': '2019-20',
    'beds_Q4_202021.xlsx': '2020-21',
    'beds_Q4_2021-22.xlsx': '2021-22',
    'beds_Q4_202223.xlsx': '2022-23',
    'bed_Q4_202324.xlsx': '2023-24',
}

# ============================================================================
# LOAD AND COMBINE BED DATA
# ============================================================================
print("\n[1/4] Loading historical bed data...")

all_beds = []

for filename, fy in bed_files.items():
    filepath = activity_dir / filename

    if not filepath.exists():
        print(f"⚠ Skipping {filename} - file not found")
        continue

    try:
        # Read the specific sheet - headers on row 15 (index 14)
        beds_raw = pd.read_excel(filepath, sheet_name='NHS Trust by Sector', header=14)

        # Extract relevant columns (note: 'Total ' has trailing space)
        beds = beds_raw[['Org Name', 'Total ']].copy()
        beds.columns = ['org_name_activity', 'beds']

        # Clean data
        beds = beds[beds['org_name_activity'].notna()]
        beds = beds[beds['org_name_activity'] != 'Org Name']  # Remove repeated headers
        beds = beds[beds['beds'].notna()]

        # Convert beds to numeric
        beds['beds'] = pd.to_numeric(beds['beds'], errors='coerce')
        beds = beds[beds['beds'].notna()]

        # Add financial year
        beds['fy'] = fy

        all_beds.append(beds)
        print(f"  ✓ {filename}: {len(beds)} organizations")

    except Exception as e:
        print(f"  ✗ Error processing {filename}: {e}")

if not all_beds:
    print("\n✗ No bed data files found!")
    exit(1)

# Combine all years
beds_combined = pd.concat(all_beds, ignore_index=True)

print(f"\n✓ Combined data:")
print(f"  Total records: {len(beds_combined)}")
print(f"  Unique organizations: {beds_combined['org_name_activity'].nunique()}")
print(f"  Financial years: {sorted(beds_combined['fy'].unique())}")

# ============================================================================
# CLEAN ORGANIZATION NAMES
# ============================================================================
print("\n[2/4] Cleaning organization names...")

# Standardize names for matching (keep original capitalization to match TAC database)
beds_combined['org_name_raw'] = beds_combined['org_name_activity'].str.strip()

# Aggregate by org and year (in case of duplicates)
beds_agg = beds_combined.groupby(['org_name_raw', 'fy'])['beds'].mean().reset_index()

print(f"✓ Aggregated to {len(beds_agg)} org-year records")

# ============================================================================
# MATCH TO TAC ORGANIZATIONS
# ============================================================================
print("\n[3/4] Matching to TAC organizations...")

con = duckdb.connect(str(db_path), read_only=True)

# Get distinct organizations from TAC
tac_orgs = con.execute("""
    SELECT DISTINCT org_name_raw
    FROM fact_tru_tac
""").fetchdf()

print(f"  TAC organizations: {len(tac_orgs)}")

# Create uppercase version for case-insensitive matching
tac_orgs['org_name_upper'] = tac_orgs['org_name_raw'].str.upper()
beds_agg['org_name_upper'] = beds_agg['org_name_raw'].str.upper()

# Merge on uppercase names, but keep proper TAC name
matched = beds_agg.merge(
    tac_orgs[['org_name_raw', 'org_name_upper']],
    on='org_name_upper',
    how='inner',
    suffixes=('_bed', '_tac')
)

# Use the TAC org_name_raw (proper case) and drop the bed uppercase name
matched = matched[['org_name_raw_tac', 'fy', 'beds']].copy()
matched.columns = ['org_name_raw', 'fy', 'beds']

print(f"  Matched organizations: {matched['org_name_raw'].nunique()}")
print(f"  Match rate: {matched['org_name_raw'].nunique() / beds_agg['org_name_raw'].nunique() * 100:.1f}%")

# Show unmatched for reference
unmatched = beds_agg[~beds_agg['org_name_upper'].isin(tac_orgs['org_name_upper'])]
if len(unmatched) > 0:
    unmatched_orgs = unmatched['org_name_raw'].unique()
    print(f"\n  Some organizations didn't match. Sample unmatched:")
    print(f"  {list(unmatched_orgs[:10])}")

con.close()

# ============================================================================
# SAVE OUTPUT
# ============================================================================
print("\n[4/4] Saving matched data...")

output_file = output_dir / 'beds_historical_matched.csv'
matched.to_csv(output_file, index=False)
print(f"✓ Saved: {output_file}")

# Summary statistics
print("\nBed data summary by sector (using TAC organization names):")
# We'll add sector info when merging with consultancy data

print("\nBed data by financial year:")
summary = matched.groupby('fy').agg({
    'org_name_raw': 'nunique',
    'beds': ['mean', 'median', 'sum']
})
print(summary)

print("\n" + "=" * 80)
print("HISTORICAL BED DATA PROCESSING COMPLETE")
print("=" * 80)
print(f"\n✓ Processed bed data for {matched['org_name_raw'].nunique()} organizations")
print(f"✓ Financial years: {sorted(matched['fy'].unique())}")
print(f"✓ Average beds per org-year: {matched['beds'].mean():.0f}")
print(f"\nNext step: Run consultancy_with_beds.py to analyze consultancy per bed")
