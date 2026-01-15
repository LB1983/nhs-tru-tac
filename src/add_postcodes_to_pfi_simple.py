#!/usr/bin/env python3
"""
Add postcodes to PFI data using organization name matching
"""

import pandas as pd
from pathlib import Path

# Input files
PFI_DATA = Path("Data/analysis/pfi_analysis/pfi_with_beds.csv")
POSTCODE_FILE = Path("Data/reference/trust_postcodes.csv")
OUTPUT_DIR = Path("Data/analysis/pfi_analysis/flourish_exports")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 80)
print("PFI MAP DATA EXPORT FOR FLOURISH")
print("=" * 80)

# ============================================================================
# LOAD POSTCODE DATA
# ============================================================================
print("\n[1/3] Loading postcode data...")

# Read CSV - column 0 = ODS code, column 1 = org name, column 9 = postcode
postcodes = pd.read_csv(POSTCODE_FILE, header=None)

print(f"✓ Loaded {len(postcodes)} records")

# Extract relevant columns
postcode_lookup = postcodes[[0, 1, 9]].copy()
postcode_lookup.columns = ['ods_code', 'org_name', 'postcode']

# Clean organization names for matching
postcode_lookup['org_name_clean'] = postcode_lookup['org_name'].str.strip().str.upper()

# Remove duplicates, keep first occurrence
postcode_lookup = postcode_lookup.drop_duplicates(subset=['org_name_clean'], keep='first')

print(f"✓ Created lookup with {len(postcode_lookup)} organizations")
print(f"\nSample entries:")
for _, row in postcode_lookup.head(3).iterrows():
    print(f"  {row['ods_code']}: {row['org_name']} → {row['postcode']}")

# ============================================================================
# LOAD PFI DATA
# ============================================================================
print("\n[2/3] Loading PFI data...")

pfi = pd.read_csv(PFI_DATA)
latest_year = pfi['fy'].max()
latest = pfi[pfi['fy'] == latest_year].copy()

print(f"✓ Loaded {len(latest)} organizations from {latest_year}")

# Clean PFI org names for matching
latest['org_name_clean'] = latest['org_name_raw'].str.upper()

# ============================================================================
# MATCH POSTCODES TO PFI DATA
# ============================================================================
print("\n[3/3] Matching postcodes to PFI organizations...")

# Merge on cleaned organization names
map_data = latest.merge(
    postcode_lookup[['org_name_clean', 'ods_code', 'postcode']],
    on='org_name_clean',
    how='left'
)

matched = map_data['postcode'].notna().sum()
print(f"✓ Matched {matched}/{len(map_data)} organizations")

# Show some unmatched organizations
if matched < len(map_data):
    unmatched = map_data[map_data['postcode'].isna()]['org_name_raw'].head(5)
    print(f"\nSample unmatched organizations:")
    for org in unmatched:
        print(f"  - {org}")

# ============================================================================
# EXPORT MAP DATA
# ============================================================================

# Create comprehensive map export
if 'capital_pfi_per_bed' in map_data.columns and 'revenue_pfi_per_bed' in map_data.columns:
    map_export = map_data[[
        'org_name_raw', 'ods_code', 'postcode', 'sector', 'beds',
        'capital_pfi_per_bed', 'revenue_pfi_per_bed', 'total_pfi_per_bed',
        'Capital', 'Revenue', 'total_pfi'
    ]].copy()

    map_export.columns = [
        'Organization', 'ODS Code', 'Postcode', 'Sector', 'Beds',
        'Capital PFI per Bed (£)', 'Revenue PFI per Bed (£)', 'Total PFI per Bed (£)',
        'Total Capital PFI (£)', 'Total Revenue PFI (£)', 'Total PFI (£)'
    ]
else:
    map_export = map_data[[
        'org_name_raw', 'ods_code', 'postcode', 'sector', 'beds',
        'total_pfi_per_bed', 'total_pfi'
    ]].copy()

    map_export.columns = [
        'Organization', 'ODS Code', 'Postcode', 'Sector', 'Beds',
        'Total PFI per Bed (£)', 'Total PFI (£)'
    ]

# Sort by PFI per bed
map_export = map_export.sort_values('Total PFI per Bed (£)', ascending=False)

# Save
output_file = OUTPUT_DIR / "7_map_pfi_by_location.csv"
map_export.to_csv(output_file, index=False)

print("\n" + "=" * 80)
print("MAP EXPORT COMPLETE")
print("=" * 80)
print(f"\n✓ Saved: {output_file}")
print(f"\nOrganizations with postcodes: {map_export['Postcode'].notna().sum()}/{len(map_export)}")

if map_export['Postcode'].notna().sum() > 0:
    print("\nTop 5 PFI spenders (with postcodes):")
    top_5 = map_export[map_export['Postcode'].notna()].head(5)
    for _, row in top_5.iterrows():
        print(f"  {row['Organization'][:45]:45} £{row['Total PFI per Bed (£)']:,.0f}/bed")

print("\nFLOURISH MAP INSTRUCTIONS:")
print("1. Go to https://flourish.studio")
print("2. Create new project → Choose 'Projection map' or 'Point map'")
print("3. Upload: 7_map_pfi_by_location.csv")
print("4. Set 'Postcode' column as location")
print("5. Size bubbles by: 'Total PFI per Bed (£)'")
print("6. Color by: 'Sector' or create gradient by PFI amount")
print("7. Add popup showing: Organization, PFI breakdown, Beds")
