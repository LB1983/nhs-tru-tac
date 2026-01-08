#!/usr/bin/env python3
"""
Integrate NHS activity data (beds, admissions, etc.) with TAC financial data

USAGE:
1. Download activity data files and place in Data/activity/
2. Update the file paths below
3. Run this script to create enriched analysis

EXPECTED FILE FORMAT (example for beds):
org_code, org_name, fy, beds, occupancy_pct, ...
"""

import duckdb
import pandas as pd
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION - UPDATE THESE PATHS
# ============================================================================

# Path to your downloaded activity data files
BED_DATA_FILE = Path("Data/activity/beds.csv")  # Update this
ADMISSIONS_DATA_FILE = Path("Data/activity/admissions.csv")  # Update this
AE_DATA_FILE = Path("Data/activity/ae_attendances.csv")  # Update this

DB_PATH = Path("Data/canonical/tru_tac.duckdb")
OUTPUT_DIR = Path("Data/analysis/activity_enriched")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 80)
print("NHS ACTIVITY DATA INTEGRATION")
print("=" * 80)

con = duckdb.connect(str(DB_PATH), read_only=True)

# ============================================================================
# 1. LOAD EXISTING ORGANIZATION LIST
# ============================================================================
print("\n[1/5] Loading organization list from TAC database...")

orgs = con.execute("""
    SELECT DISTINCT
        org_name_raw,
        sector,
        COUNT(DISTINCT fy) as years_in_tac
    FROM fact_tru_tac
    GROUP BY org_name_raw, sector
    ORDER BY org_name_raw
""").fetchdf()

print(f"Organizations in TAC database: {len(orgs)}")
orgs.to_csv(OUTPUT_DIR / "tac_organizations.csv", index=False)
print("✓ Saved organization list")

# ============================================================================
# 2. LOAD ACTIVITY DATA (BED EXAMPLE)
# ============================================================================
print("\n[2/5] Loading activity data...")

if BED_DATA_FILE.exists():
    print(f"Loading beds data from: {BED_DATA_FILE}")

    # Read the file - adjust based on actual format
    beds = pd.read_csv(BED_DATA_FILE)

    print(f"Bed data shape: {beds.shape}")
    print(f"Columns: {list(beds.columns)}")
    print("\nFirst few rows:")
    print(beds.head())

    # Expected columns (adjust based on actual file):
    # - Organization name/code
    # - Financial year or date
    # - Number of beds
    # - Occupancy rate, etc.

    beds.to_csv(OUTPUT_DIR / "beds_loaded.csv", index=False)
    print("✓ Beds data loaded")

else:
    print(f"⚠ Beds file not found: {BED_DATA_FILE}")
    print("Please download bed data and update the path")
    beds = None

# ============================================================================
# 3. ORGANIZATION NAME MATCHING
# ============================================================================
print("\n[3/5] Matching organization names...")

if beds is not None:
    # This section will need customization based on actual column names
    # Example matching logic:

    print("""
    IMPORTANT: Organization name matching

    The bed data and TAC data may use different organization names.

    Options:
    1. Match by ODS code (most reliable if available)
    2. Match by name (fuzzy matching may be needed)
    3. Manual mapping file

    Please review the organization names in both datasets:
    - TAC orgs: Data/analysis/activity_enriched/tac_organizations.csv
    - Activity orgs: Data/analysis/activity_enriched/beds_loaded.csv

    Create a mapping file if needed: Data/activity/org_name_mapping.csv
    Format: tac_name, activity_name, ods_code
    """)

# ============================================================================
# 4. CALCULATE CONSULTANCY WITH ACTIVITY METRICS
# ============================================================================
print("\n[4/5] Enriching consultancy analysis with activity data...")

# Get consultancy spending
consultancy_codes_query = """
    SELECT DISTINCT SubCode
    FROM dim_tac_subcodes_ws
    WHERE LOWER(subcode_label) LIKE '%consult%'
       OR LOWER(subcode_label) LIKE '%advisory%'
"""
consultancy_codes = con.execute(consultancy_codes_query).fetchdf()
codes_list = "', '".join(consultancy_codes['SubCode'].tolist())

consultancy_spend = con.execute(f"""
    SELECT
        org_name_raw,
        sector,
        fy,
        SUM(amount) as consultancy_spend
    FROM fact_tru_tac
    WHERE SubCode IN ('{codes_list}')
    GROUP BY org_name_raw, sector, fy
    ORDER BY org_name_raw, fy
""").fetchdf()

print(f"Consultancy records: {len(consultancy_spend)}")

# Get operating income (turnover)
operating_income = con.execute("""
    SELECT
        org_name_raw,
        fy,
        SUM(amount) as operating_income
    FROM fact_tru_tac
    WHERE WorkSheetName = 'TAC02 SoCI'
      AND SubCode IN (
          SELECT DISTINCT SubCode
          FROM dim_tac_subcodes_ws
          WHERE LOWER(subcode_label) LIKE '%operating income%'
      )
    GROUP BY org_name_raw, fy
""").fetchdf()

# Merge consultancy with turnover
enriched = consultancy_spend.merge(
    operating_income,
    on=['org_name_raw', 'fy'],
    how='left'
)

enriched['consultancy_millions'] = enriched['consultancy_spend'] / 1_000_000
enriched['turnover_millions'] = enriched['operating_income'] / 1_000_000
enriched['consultancy_pct_turnover'] = (
    enriched['consultancy_spend'] / enriched['operating_income']
) * 100

# If bed data is available and matched, add it here
# enriched = enriched.merge(beds, left_on=['org_name_raw', 'fy'], right_on=[...])
# enriched['consultancy_per_bed'] = enriched['consultancy_spend'] / enriched['beds']

enriched.to_csv(OUTPUT_DIR / "consultancy_enriched_base.csv", index=False)
print("✓ Created enriched consultancy dataset")

# ============================================================================
# 5. TEMPLATE FOR FULL INTEGRATION
# ============================================================================
print("\n[5/5] Integration template created")

print("""
=" * 80
NEXT STEPS FOR FULL INTEGRATION
=" * 80

1. DOWNLOAD ACTIVITY DATA:
   Visit the URLs in ACTIVITY_DATA_PLAN.md
   Download files for:
   - Beds (quarterly or annual)
   - Admitted patient care
   - A&E attendances
   - Outpatient attendances

2. PLACE FILES IN: Data/activity/

3. UPDATE THIS SCRIPT:
   - Set correct file paths
   - Adjust column names to match downloaded files
   - Add organization name matching logic

4. EXPECTED OUTPUT METRICS:
   - Consultancy spend per bed
   - Consultancy spend per admission
   - IT spend per bed (once IT codes identified)
   - Benchmarking: compare similar sized trusts
   - Efficiency rankings

5. RUN AGAIN: python src/integrate_activity_data.py

HELPFUL TIPS:
- Start with just beds data to test the process
- Use ODS codes for matching if available
- Check for organization name variations
- Validate data quality before analysis

Files created in: {OUTPUT_DIR.absolute()}
""")

con.close()

print("=" * 80)
