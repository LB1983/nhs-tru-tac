#!/usr/bin/env python3
"""
Smart processor for NHS bed data - works with various file formats

USAGE:
1. Download bed data to Data/activity/ folder
2. Run this script
3. It will auto-detect the file structure and process it
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
# 1. FIND BED DATA FILE
# ============================================================================
print("\n[1/5] Looking for bed data files...")

if not ACTIVITY_DIR.exists():
    ACTIVITY_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Created directory: {ACTIVITY_DIR}")

# Look for bed-related files
bed_files = []
for pattern in ['*bed*.xlsx', '*bed*.csv', '*bed*.xls', 'beds.*']:
    bed_files.extend(ACTIVITY_DIR.glob(pattern))

if len(bed_files) == 0:
    print(f"\n⚠️  No bed data files found in {ACTIVITY_DIR}")
    print("\nPlease download bed data and save to Data/activity/")
    print("See DOWNLOAD_GUIDE.md for instructions")
    print("\nLooking for files matching: beds.xlsx, beds.csv, *bed*.xlsx, etc.")
    exit(1)

bed_file = bed_files[0]
print(f"✓ Found bed data file: {bed_file.name}")

# ============================================================================
# 2. LOAD AND INSPECT FILE
# ============================================================================
print(f"\n[2/5] Loading file: {bed_file.name}")

# Try to load - handle both CSV and Excel
if bed_file.suffix.lower() == '.csv':
    beds_raw = pd.read_csv(bed_file)
else:
    # For Excel, try to find the right sheet
    xl = pd.ExcelFile(bed_file)
    print(f"   Sheets available: {xl.sheet_names}")

    # Use first sheet or one with 'bed' or 'data' in name
    sheet_name = xl.sheet_names[0]
    for s in xl.sheet_names:
        if 'bed' in s.lower() or 'data' in s.lower():
            sheet_name = s
            break

    print(f"   Using sheet: {sheet_name}")
    beds_raw = pd.read_excel(bed_file, sheet_name=sheet_name)

print(f"\n✓ Loaded {len(beds_raw):,} rows, {len(beds_raw.columns)} columns")
print(f"\nColumns in file:")
for i, col in enumerate(beds_raw.columns, 1):
    print(f"   {i}. {col}")

print(f"\nFirst few rows:")
print(beds_raw.head(3))

# ============================================================================
# 3. AUTO-DETECT COLUMNS
# ============================================================================
print(f"\n[3/5] Auto-detecting column structure...")

# Try to find key columns
org_col = None
bed_col = None
date_col = None

# Look for organization name/code
for col in beds_raw.columns:
    col_lower = str(col).lower()
    if any(x in col_lower for x in ['org', 'trust', 'provider', 'name', 'code']):
        org_col = col
        print(f"   Organization column: {col}")
        break

# Look for bed count
for col in beds_raw.columns:
    col_lower = str(col).lower()
    if any(x in col_lower for x in ['bed', 'total', 'available', 'overnight']):
        if beds_raw[col].dtype in ['int64', 'float64']:
            bed_col = col
            print(f"   Bed count column: {col}")
            break

# Look for date/period
for col in beds_raw.columns:
    col_lower = str(col).lower()
    if any(x in col_lower for x in ['date', 'period', 'quarter', 'year', 'fy']):
        date_col = col
        print(f"   Date column: {col}")
        break

if not org_col or not bed_col:
    print("\n⚠️  Could not auto-detect columns")
    print("\nPlease tell me:")
    print("   1. Which column has organization names?")
    print("   2. Which column has bed counts?")
    print("   3. Which column has the date/period? (if any)")
    print("\nI'll update the script to use those columns.")
    exit(1)

# ============================================================================
# 4. CLEAN AND STANDARDIZE
# ============================================================================
print(f"\n[4/5] Cleaning and standardizing data...")

beds = beds_raw[[org_col, bed_col]].copy()
if date_col:
    beds = beds_raw[[org_col, bed_col, date_col]].copy()

beds.columns = ['org_name_activity', 'beds'] + (['period'] if date_col else [])

# Remove rows with missing data
beds = beds.dropna(subset=['org_name_activity', 'beds'])

# Convert beds to numeric
beds['beds'] = pd.to_numeric(beds['beds'], errors='coerce')
beds = beds.dropna(subset=['beds'])

print(f"✓ Cleaned to {len(beds):,} valid records")

# If we have period data, try to extract year
if 'period' in beds.columns:
    # Try to extract FY format (YYYY-YY)
    beds['fy'] = beds['period'].astype(str)
    print(f"   Unique periods: {beds['fy'].nunique()}")
    print(f"   Sample periods: {beds['fy'].unique()[:5]}")

# ============================================================================
# 5. MATCH TO TAC ORGANIZATIONS
# ============================================================================
print(f"\n[5/5] Matching to TAC organizations...")

con = duckdb.connect(str(DB_PATH), read_only=True)

# Get TAC organizations
tac_orgs = con.execute("""
    SELECT DISTINCT org_name_raw, sector
    FROM fact_tru_tac
    ORDER BY org_name_raw
""").fetchdf()

print(f"   TAC organizations: {len(tac_orgs)}")
print(f"   Activity file organizations: {beds['org_name_activity'].nunique()}")

# Try exact match first
beds_matched = beds.merge(
    tac_orgs,
    left_on='org_name_activity',
    right_on='org_name_raw',
    how='inner'
)

print(f"   Exact matches: {beds_matched['org_name_raw'].nunique()}")

if len(beds_matched) < len(beds) * 0.5:
    print(f"\n⚠️  Only {len(beds_matched)/len(beds)*100:.0f}% of records matched")
    print(f"\nSample unmatched organizations:")
    unmatched = beds[~beds['org_name_activity'].isin(beds_matched['org_name_activity'])]
    print(unmatched['org_name_activity'].head(10).to_list())
    print(f"\nThis might need fuzzy matching or manual mapping.")
    print(f"Creating mapping template...")

    # Create mapping template
    mapping_template = pd.DataFrame({
        'activity_name': beds['org_name_activity'].unique()
    })
    mapping_template['tac_name'] = ''
    mapping_template.to_csv(ACTIVITY_DIR / "org_mapping_template.csv", index=False)
    print(f"✓ Saved: {ACTIVITY_DIR / 'org_mapping_template.csv'}")
    print(f"\nYou can manually map organization names in this file,")
    print(f"then run the integration script again.")

# Save matched data
beds_matched.to_csv(OUTPUT_DIR / "beds_matched.csv", index=False)
print(f"\n✓ Saved matched bed data: {OUTPUT_DIR / 'beds_matched.csv'}")

# Show summary
if len(beds_matched) > 0:
    print(f"\nBed data summary:")
    summary = beds_matched.groupby('sector')['beds'].describe()
    print(summary)

con.close()

print(f"\n" + "=" * 80)
print(f"BED DATA PROCESSING COMPLETE")
print(f"=" * 80)
print(f"""
✓ Processed {len(beds):,} bed records
✓ Matched {beds_matched['org_name_raw'].nunique()} organizations to TAC data

Next steps:
1. Review: {OUTPUT_DIR / 'beds_matched.csv'}
2. Run: python src/consultancy_with_beds.py
3. Get consultancy per bed analysis!
""")
