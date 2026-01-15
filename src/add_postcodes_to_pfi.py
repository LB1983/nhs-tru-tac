#!/usr/bin/env python3
"""
Add postcodes to PFI data for Flourish map visualization
Uses ODS codes for accurate matching
"""

import pandas as pd
from pathlib import Path
import duckdb

# Input files
PFI_DATA = Path("Data/analysis/pfi_analysis/pfi_with_beds.csv")
POSTCODE_FILE = Path("Data/reference/trust_postcodes.csv")
DB_PATH = Path("Data/canonical/tru_tac.duckdb")
OUTPUT_DIR = Path("Data/analysis/pfi_analysis/flourish_exports")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 80)
print("PFI MAP DATA EXPORT FOR FLOURISH")
print("=" * 80)

# ============================================================================
# LOAD POSTCODE DATA (ODS codes + postcodes)
# ============================================================================
print("\n[1/4] Loading postcode data...")

# Read CSV - expecting ODS code in first column, postcode in one of the columns
postcodes = pd.read_csv(POSTCODE_FILE, header=None)

print(f"✓ Loaded {len(postcodes)} records")
print(f"  Columns: {len(postcodes.columns)}")
print(f"\nFirst few rows:")
print(postcodes.head(3))

# Identify ODS code and postcode columns
# ODS codes are typically 3-5 character alphanumeric codes
# Postcodes match UK format
def looks_like_ods_code(series):
    """Check if series contains ODS-like codes"""
    sample = series.dropna().astype(str).head(100)
    # ODS codes are typically 3-5 chars, alphanumeric
    ods_like = sample.str.match(r'^[A-Z0-9]{3,5}$').sum()
    return ods_like / len(sample) > 0.8 if len(sample) > 0 else False

def looks_like_postcode(series):
    """Check if series contains UK postcodes"""
    sample = series.dropna().astype(str).head(100)
    # UK postcode pattern
    postcode_like = sample.str.match(r'^[A-Z]{1,2}\d{1,2}\s?\d[A-Z]{2}$', case=False).sum()
    return postcode_like / len(sample) > 0.5 if len(sample) > 0 else False

# Find ODS and postcode columns
ods_col = None
postcode_col = None

for col in postcodes.columns:
    if looks_like_ods_code(postcodes[col]):
        ods_col = col
        print(f"\n✓ Identified ODS code column: {col}")
        print(f"  Sample: {postcodes[col].head(3).tolist()}")
    if looks_like_postcode(postcodes[col]):
        postcode_col = col
        print(f"\n✓ Identified postcode column: {col}")
        print(f"  Sample: {postcodes[col].head(3).tolist()}")

if ods_col is None or postcode_col is None:
    print("\n⚠️  Could not auto-detect columns. Using first column as ODS and guessing postcode...")
    ods_col = 0
    # Look for postcode-like column
    for col in postcodes.columns:
        if postcodes[col].astype(str).str.contains(r'\d[A-Z]{2}$', case=False, na=False).any():
            postcode_col = col
            break

# Create clean lookup
postcode_lookup = postcodes[[ods_col, postcode_col]].copy()
postcode_lookup.columns = ['ods_code', 'postcode']
postcode_lookup['ods_code'] = postcode_lookup['ods_code'].str.strip().str.upper()
postcode_lookup = postcode_lookup.drop_duplicates(subset=['ods_code'])

print(f"\n✓ Created lookup with {len(postcode_lookup)} unique ODS codes")

# ============================================================================
# GET ODS CODES FROM TAC DATABASE
# ============================================================================
print("\n[2/4] Checking for ODS codes in TAC database...")

con = duckdb.connect(str(DB_PATH), read_only=True)

# Check if ODS code exists in database
try:
    ods_in_db = con.execute("""
        SELECT DISTINCT org_code, org_name_raw
        FROM fact_tru_tac
        WHERE org_code IS NOT NULL
        LIMIT 5
    """).fetchdf()

    if len(ods_in_db) > 0:
        print("✓ ODS codes found in database")
        has_ods = True
    else:
        print("⚠️  No ODS codes in database")
        has_ods = False
except:
    print("⚠️  org_code column not found in database")
    has_ods = False

# If no ODS codes, create name-based mapping
if not has_ods:
    print("\nCreating ODS lookup from organization names...")

    # Get all org names from TAC
    tac_orgs = con.execute("""
        SELECT DISTINCT org_name_raw
        FROM fact_tru_tac
    """).fetchdf()

    # Simple name-based matching to ODS codes
    # This is a fallback - ideally we'd have ODS codes in the TAC database
    tac_orgs['org_name_upper'] = tac_orgs['org_name_raw'].str.upper()

    print(f"  TAC organizations: {len(tac_orgs)}")
    ods_lookup = tac_orgs.copy()
    ods_lookup['ods_code'] = None  # Will be matched below
else:
    ods_lookup = con.execute("""
        SELECT DISTINCT org_code as ods_code, org_name_raw
        FROM fact_tru_tac
        WHERE org_code IS NOT NULL
    """).fetchdf()
    ods_lookup['ods_code'] = ods_lookup['ods_code'].str.strip().str.upper()

con.close()

# ============================================================================
# LOAD PFI DATA
# ============================================================================
print("\n[3/4] Loading PFI data...")

pfi = pd.read_csv(PFI_DATA)
latest_year = pfi['fy'].max()
latest = pfi[pfi['fy'] == latest_year].copy()

print(f"✓ Loaded {len(latest)} organizations from {latest_year}")

# ============================================================================
# MATCH POSTCODES TO PFI DATA
# ============================================================================
print("\n[4/4] Matching postcodes to PFI organizations...")

if has_ods:
    # Direct ODS code matching
    latest_with_ods = latest.merge(ods_lookup, on='org_name_raw', how='left')
    map_data = latest_with_ods.merge(postcode_lookup, on='ods_code', how='left')

    matched = map_data['postcode'].notna().sum()
    print(f"✓ Matched {matched}/{len(map_data)} organizations via ODS codes")
else:
    # Name-based fuzzy matching
    map_data = latest.copy()
    map_data['postcode'] = None

    matched_count = 0
    for idx, row in map_data.iterrows():
        org_name = row['org_name_raw'].upper()

        # Try exact match first
        exact = postcode_lookup[postcode_lookup['ods_code'] == org_name]
        if len(exact) > 0:
            map_data.at[idx, 'postcode'] = exact.iloc[0]['postcode']
            matched_count += 1

    print(f"✓ Matched {matched_count}/{len(map_data)} organizations via name matching")
    print(f"⚠️  For better matching, add ODS org_code column to TAC database")

# ============================================================================
# EXPORT MAP DATA
# ============================================================================

# Create comprehensive map export
if 'capital_pfi_per_bed' in map_data.columns and 'revenue_pfi_per_bed' in map_data.columns:
    map_export = map_data[[
        'org_name_raw', 'postcode', 'sector', 'beds',
        'capital_pfi_per_bed', 'revenue_pfi_per_bed', 'total_pfi_per_bed',
        'Capital', 'Revenue', 'total_pfi'
    ]].copy()

    map_export.columns = [
        'Organization', 'Postcode', 'Sector', 'Beds',
        'Capital PFI per Bed (£)', 'Revenue PFI per Bed (£)', 'Total PFI per Bed (£)',
        'Total Capital PFI (£)', 'Total Revenue PFI (£)', 'Total PFI (£)'
    ]
else:
    map_export = map_data[[
        'org_name_raw', 'postcode', 'sector', 'beds',
        'total_pfi_per_bed', 'total_pfi'
    ]].copy()

    map_export.columns = [
        'Organization', 'Postcode', 'Sector', 'Beds',
        'Total PFI per Bed (£)', 'Total PFI (£)'
    ]

# Sort by PFI per bed
map_export = map_export.sort_values('Total PFI per Bed (£)', ascending=False)

# Save
map_export.to_csv(OUTPUT_DIR / "7_map_pfi_by_location.csv", index=False)

print("\n" + "=" * 80)
print("MAP EXPORT COMPLETE")
print("=" * 80)
print(f"\n✓ Saved: {OUTPUT_DIR / '7_map_pfi_by_location.csv'}")
print(f"\nOrganizations with postcodes: {map_export['Postcode'].notna().sum()}/{len(map_export)}")

print("\nFLOURISH MAP INSTRUCTIONS:")
print("1. Go to https://flourish.studio")
print("2. Create new project → Choose 'Projection map' or 'Point map'")
print("3. Upload: 7_map_pfi_by_location.csv")
print("4. Set 'Postcode' column as location")
print("5. Size bubbles by: 'Total PFI per Bed (£)'")
print("6. Color by: 'Sector' or create gradient by PFI amount")
print("7. Add popup showing: Organization, PFI breakdown, Beds")
print("\nFlouris will automatically geocode UK postcodes!")
