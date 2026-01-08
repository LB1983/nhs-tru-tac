#!/usr/bin/env python3
"""
Process NHS bed data - customized for the actual file format
"""

import pandas as pd
import duckdb
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

ACTIVITY_DIR = Path("Data/activity")
DB_PATH = Path("Data/canonical/tru_tac.duckdb")
OUTPUT_DIR = Path("Data/analysis/activity_integrated")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 80)
print("NHS BED DATA PROCESSOR")
print("=" * 80)

# ============================================================================
# 1. LOAD BED DATA
# ============================================================================
print("\n[1/4] Loading bed data...")

bed_file = ACTIVITY_DIR / "beds.xlsx"
if not bed_file.exists():
    # Try other common names
    alternatives = list(ACTIVITY_DIR.glob("*bed*.xlsx"))
    if alternatives:
        bed_file = alternatives[0]
    else:
        print(f"⚠️  No bed file found in {ACTIVITY_DIR}")
        exit(1)

print(f"Loading: {bed_file.name}")

# Read the specific sheet - headers are on row 15 (index 14)
beds_raw = pd.read_excel(bed_file, sheet_name='NHS Trust by Sector', header=14)
print(f"✓ Loaded {len(beds_raw):,} rows")

# ============================================================================
# 2. EXTRACT RELEVANT COLUMNS
# ============================================================================
print("\n[2/4] Extracting data...")

# Select columns we need (note: 'Total ' has a trailing space in the Excel file)
beds = beds_raw[['Year', 'Period End', 'Org Name', 'Total ']].copy()
beds.columns = ['year', 'period', 'org_name_activity', 'beds']

# Remove header rows and invalid data
beds = beds[beds['org_name_activity'].notna()]
beds = beds[beds['org_name_activity'] != 'Org Name']  # Remove repeated headers

# Convert beds to numeric
beds['beds'] = pd.to_numeric(beds['beds'], errors='coerce')
beds = beds.dropna(subset=['beds'])

# Create FY column from Year (e.g., "2023-24")
beds['fy'] = beds['year'].astype(str)

print(f"✓ Extracted {len(beds):,} valid bed records")
print(f"  Unique organizations: {beds['org_name_activity'].nunique()}")
print(f"  Years covered: {sorted(beds['year'].unique())}")
print(f"  Periods: {sorted(beds['period'].unique())}")

# Aggregate by org and FY (sum across all periods in the year)
beds_agg = beds.groupby(['org_name_activity', 'fy']).agg({
    'beds': 'mean'  # Average beds across periods in the year
}).reset_index()

print(f"\nAggregated to {len(beds_agg):,} org-year records")

# ============================================================================
# 3. MATCH TO TAC ORGANIZATIONS
# ============================================================================
print("\n[3/4] Matching to TAC organizations...")

con = duckdb.connect(str(DB_PATH), read_only=True)

# Get TAC organizations
tac_orgs = con.execute("""
    SELECT DISTINCT org_name_raw, sector
    FROM fact_tru_tac
    ORDER BY org_name_raw
""").fetchdf()

print(f"  TAC organizations: {len(tac_orgs)}")

# Try exact match on organization name
beds_agg['org_name_clean'] = beds_agg['org_name_activity'].str.upper().str.strip()
tac_orgs['org_name_clean'] = tac_orgs['org_name_raw'].str.upper().str.strip()

beds_matched = beds_agg.merge(
    tac_orgs[['org_name_raw', 'org_name_clean', 'sector']],
    on='org_name_clean',
    how='inner'
)

print(f"  Matched organizations: {beds_matched['org_name_raw'].nunique()}")
print(f"  Match rate: {beds_matched['org_name_raw'].nunique() / beds_agg['org_name_activity'].nunique() * 100:.1f}%")

if beds_matched['org_name_raw'].nunique() < beds_agg['org_name_activity'].nunique():
    print(f"\n  Some organizations didn't match. Sample unmatched:")
    unmatched = beds_agg[~beds_agg['org_name_clean'].isin(beds_matched['org_name_clean'])]
    print(unmatched['org_name_activity'].head(10).to_list())

# Keep only the columns we need
beds_final = beds_matched[['org_name_raw', 'sector', 'fy', 'beds']].copy()

# ============================================================================
# 4. SAVE AND SUMMARIZE
# ============================================================================
print("\n[4/4] Saving matched data...")

beds_final.to_csv(OUTPUT_DIR / "beds_matched.csv", index=False)
print(f"✓ Saved: {OUTPUT_DIR / 'beds_matched.csv'}")

# Show summary
print("\nBed data summary by sector:")
summary = beds_final.groupby('sector')['beds'].describe()
print(summary)

print("\nBed data by financial year:")
by_year = beds_final.groupby('fy').agg({
    'org_name_raw': 'nunique',
    'beds': ['mean', 'median', 'sum']
})
print(by_year)

con.close()

print("\n" + "=" * 80)
print("BED DATA PROCESSING COMPLETE")
print("=" * 80)
print(f"""
✓ Processed bed data for {beds_final['org_name_raw'].nunique()} organizations
✓ Financial years: {sorted(beds_final['fy'].unique())}
✓ Average beds per organization: {beds_final['beds'].mean():.0f}

Next step:
Run: python src/consultancy_with_beds.py
""")
