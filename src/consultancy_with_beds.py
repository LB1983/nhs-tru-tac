#!/usr/bin/env python3
"""
Consultancy analysis with bed-normalized metrics

Requires: beds_matched.csv from process_bed_data.py
"""

import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

DB_PATH = Path("Data/canonical/tru_tac.duckdb")
BEDS_FILE_HISTORICAL = Path("Data/analysis/activity_integrated/beds_historical_matched.csv")
BEDS_FILE_SINGLE = Path("Data/analysis/activity_integrated/beds_matched.csv")
OUTPUT_DIR = Path("Data/analysis/consultancy_per_bed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 10)

print("=" * 80)
print("CONSULTANCY ANALYSIS - NORMALIZED BY BEDS")
print("=" * 80)

# ============================================================================
# LOAD DATA
# ============================================================================
print("\n[1/4] Loading data...")

# Try to load historical bed data first, fall back to single-year data
if BEDS_FILE_HISTORICAL.exists():
    beds = pd.read_csv(BEDS_FILE_HISTORICAL)
    print(f"✓ Loaded historical bed data: {len(beds)} records, {beds['org_name_raw'].nunique()} organizations")
    if 'fy' in beds.columns:
        print(f"  Years covered: {sorted(beds['fy'].unique())}")
elif BEDS_FILE_SINGLE.exists():
    beds = pd.read_csv(BEDS_FILE_SINGLE)
    print(f"✓ Loaded single-year bed data: {len(beds)} records, {beds['org_name_raw'].nunique()} organizations")
    print(f"  (Will be applied to all consultancy years)")
else:
    print(f"\n⚠️  No bed data found!")
    print(f"\nPlease run either:")
    print(f"  - python src/process_historical_beds.py  (for year-matched data)")
    print(f"  - python src/process_bed_data_fixed.py   (for single-year data)")
    exit(1)

# Get consultancy data
con = duckdb.connect(str(DB_PATH), read_only=True)

consultancy_codes = con.execute("""
    SELECT DISTINCT SubCode
    FROM dim_tac_subcodes_ws
    WHERE LOWER(subcode_label) LIKE '%consult%'
       OR LOWER(subcode_label) LIKE '%advisory%'
""").fetchdf()

codes_list = "', '".join(consultancy_codes['SubCode'].tolist())

consultancy = con.execute(f"""
    SELECT
        org_name_raw,
        sector,
        fy,
        SUM(amount) as consultancy_spend
    FROM fact_tru_tac
    WHERE SubCode IN ('{codes_list}')
    GROUP BY org_name_raw, sector, fy
""").fetchdf()

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

consultancy = consultancy.merge(operating_income, on=['org_name_raw', 'fy'], how='left')
consultancy['consultancy_pct_turnover'] = (consultancy['consultancy_spend'] / consultancy['operating_income']) * 100

print(f"✓ Loaded consultancy data")

# ============================================================================
# MERGE WITH BEDS
# ============================================================================
print("\n[2/4] Merging consultancy with bed data...")

# Check if bed data years overlap with consultancy data years
if 'fy' in beds.columns:
    bed_years = set(beds['fy'].unique())
    consultancy_years = set(consultancy['fy'].unique())
    overlap = bed_years & consultancy_years

    if overlap:
        print(f"  Using year-matched bed data (overlapping years: {sorted(overlap)})")
        merged = consultancy.merge(beds[['org_name_raw', 'fy', 'beds']], on=['org_name_raw', 'fy'], how='inner')
    else:
        print(f"  Bed data years ({sorted(bed_years)}) don't overlap with consultancy years ({sorted(consultancy_years)[:3]}...)")
        print(f"  Using current bed counts as proxy for all historical years")
        # Use average beds (or just the available data) for all years
        beds_avg = beds.groupby('org_name_raw')['beds'].mean().reset_index()
        merged = consultancy.merge(beds_avg, on='org_name_raw', how='inner')
else:
    # Use average beds for all years
    beds_avg = beds.groupby('org_name_raw')['beds'].mean().reset_index()
    merged = consultancy.merge(beds_avg, on='org_name_raw', how='inner')

merged['consultancy_per_bed'] = merged['consultancy_spend'] / merged['beds']
merged['consultancy_millions'] = merged['consultancy_spend'] / 1_000_000
merged['turnover_millions'] = merged['operating_income'] / 1_000_000

print(f"✓ Merged data: {len(merged)} records")
print(f"✓ Organizations with bed data: {merged['org_name_raw'].nunique()}")

# Filter out invalid values (inf, -inf, NaN)
import numpy as np
merged = merged.replace([np.inf, -np.inf], np.nan)
merged = merged[merged['consultancy_per_bed'].notna()]
merged = merged[merged['beds'] > 0]  # Ensure positive bed counts

print(f"✓ After filtering invalid values: {len(merged)} records")

merged.to_csv(OUTPUT_DIR / "consultancy_with_beds.csv", index=False)

# ============================================================================
# ANALYSIS
# ============================================================================
print("\n[3/4] Analyzing consultancy per bed...")

latest_year = merged['fy'].max()
latest = merged[merged['fy'] == latest_year].copy()

# Calculate z-scores for per-bed spend
latest['z_score_per_bed'] = stats.zscore(latest['consultancy_per_bed'])

# Identify outliers
high_outliers = latest[latest['z_score_per_bed'] > 2].sort_values('consultancy_per_bed', ascending=False)
low_outliers = latest[latest['z_score_per_bed'] < -2].sort_values('consultancy_per_bed')

print(f"\nConsultancy per bed ({latest_year}):")
print(f"  Mean: £{latest['consultancy_per_bed'].mean():,.0f}")
print(f"  Median: £{latest['consultancy_per_bed'].median():,.0f}")
print(f"  Min: £{latest['consultancy_per_bed'].min():,.0f}")
print(f"  Max: £{latest['consultancy_per_bed'].max():,.0f}")

print(f"\nHIGH outliers (consultancy per bed >2 std devs):")
print(f"Found {len(high_outliers)} organizations")
display_cols = ['org_name_raw', 'sector', 'beds', 'consultancy_millions',
                'consultancy_per_bed', 'consultancy_pct_turnover', 'z_score_per_bed']
if len(high_outliers) > 0:
    print(high_outliers[display_cols].to_string(index=False))
    high_outliers.to_csv(OUTPUT_DIR / "high_outliers_per_bed.csv", index=False)

print(f"\nLOW outliers (efficient - low consultancy per bed):")
print(f"Found {len(low_outliers)} organizations")
if len(low_outliers) > 0:
    print(low_outliers[display_cols].to_string(index=False))
    low_outliers.to_csv(OUTPUT_DIR / "low_outliers_per_bed.csv", index=False)

# Sector comparison
sector_comp = latest.groupby('sector').agg({
    'beds': 'mean',
    'consultancy_per_bed': ['mean', 'median'],
    'consultancy_pct_turnover': ['mean', 'median'],
    'org_name_raw': 'count'
})
print(f"\nSector comparison ({latest_year}):")
print(sector_comp)

# ============================================================================
# VISUALIZATIONS
# ============================================================================
print("\n[4/4] Creating visualizations...")

# Filter extreme outliers for better visualization (but keep in CSV)
percentile_99 = latest['consultancy_per_bed'].quantile(0.99)
extreme_outliers = latest[latest['consultancy_per_bed'] > percentile_99]

if len(extreme_outliers) > 0:
    print(f"\nExtreme outliers (>99th percentile, £{percentile_99:,.0f}/bed):")
    for _, row in extreme_outliers.nlargest(5, 'consultancy_per_bed').iterrows():
        print(f"  {row['org_name_raw']}: £{row['consultancy_per_bed']:,.0f}/bed ({row['beds']:.0f} beds)")
    print(f"  (Filtering {len(extreme_outliers)} extreme outliers from charts for better visualization)")

# Use filtered data for visualization
latest_viz = latest[latest['consultancy_per_bed'] <= percentile_99].copy()

fig, axes = plt.subplots(2, 2, figsize=(18, 12))

# 1. Scatter: Beds vs Consultancy per bed
ax1 = axes[0, 0]
colors = {'FT': '#1f77b4', 'Trust': '#ff7f0e'}
for sector in latest_viz['sector'].unique():
    sector_data = latest_viz[latest_viz['sector'] == sector]
    ax1.scatter(sector_data['beds'], sector_data['consultancy_per_bed'],
               c=colors[sector], label=sector, alpha=0.6, s=80)

# Highlight outliers (within the filtered data)
all_outliers = latest_viz[abs(latest_viz['z_score_per_bed']) > 2]
for _, row in all_outliers.iterrows():
    ax1.scatter(row['beds'], row['consultancy_per_bed'],
               c='red', s=250, marker='*', edgecolors='black', linewidths=2)
    ax1.text(row['beds'], row['consultancy_per_bed'],
            '  ' + row['org_name_raw'][:20], fontsize=8)

ax1.set_xlabel('Number of Beds', fontweight='bold', fontsize=12)
ax1.set_ylabel('Consultancy Spend per Bed (£)', fontweight='bold', fontsize=12)
ax1.set_title('Consultancy per Bed vs Organization Size', fontweight='bold', fontsize=14)
ax1.legend()
ax1.grid(alpha=0.3)

# 2. Distribution histogram
ax2 = axes[0, 1]
ax2.hist(latest_viz['consultancy_per_bed'], bins=30, edgecolor='black', alpha=0.7, color='steelblue')
ax2.axvline(latest_viz['consultancy_per_bed'].mean(), color='red', linestyle='--',
           linewidth=2, label=f'Mean: £{latest_viz["consultancy_per_bed"].mean():,.0f}')
ax2.axvline(latest_viz['consultancy_per_bed'].median(), color='green', linestyle='--',
           linewidth=2, label=f'Median: £{latest_viz["consultancy_per_bed"].median():,.0f}')
ax2.set_xlabel('Consultancy per Bed (£)', fontweight='bold', fontsize=12)
ax2.set_ylabel('Number of Organizations', fontweight='bold', fontsize=12)
ax2.set_title('Distribution of Consultancy per Bed', fontweight='bold', fontsize=14)
ax2.legend()
ax2.grid(alpha=0.3, axis='y')

# 3. Comparison: % of turnover vs per bed
ax3 = axes[1, 0]
ax3.scatter(latest_viz['consultancy_pct_turnover'], latest_viz['consultancy_per_bed'],
           c=latest_viz['beds'], cmap='viridis', s=80, alpha=0.7, edgecolors='black', linewidths=0.5)
cbar = plt.colorbar(ax3.collections[0], ax=ax3)
cbar.set_label('Number of Beds', fontweight='bold')
ax3.set_xlabel('Consultancy as % of Turnover', fontweight='bold', fontsize=12)
ax3.set_ylabel('Consultancy per Bed (£)', fontweight='bold', fontsize=12)
ax3.set_title('Consultancy % vs Per-Bed Spend (colored by size)', fontweight='bold', fontsize=14)
ax3.grid(alpha=0.3)

# 4. Sector box plots
ax4 = axes[1, 1]
latest_viz.boxplot(column='consultancy_per_bed', by='sector', ax=ax4)
ax4.set_xlabel('Sector', fontweight='bold', fontsize=12)
ax4.set_ylabel('Consultancy per Bed (£)', fontweight='bold', fontsize=12)
ax4.set_title('Consultancy per Bed - Distribution by Sector', fontweight='bold', fontsize=14)
plt.sca(ax4)
plt.xticks(rotation=0)
ax4.get_figure().suptitle('')  # Remove auto-title

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "consultancy_per_bed_analysis.png", dpi=300, bbox_inches='tight')
plt.close()
print(f"✓ Saved: consultancy_per_bed_analysis.png")

con.close()

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

print(f"""
CONSULTANCY PER BED ANALYSIS ({latest_year}):

Organizations analyzed: {len(latest)}
Mean consultancy per bed: £{latest['consultancy_per_bed'].mean():,.0f}
Median consultancy per bed: £{latest['consultancy_per_bed'].median():,.0f}

HIGH OUTLIERS (>2 std devs): {len(high_outliers)}
  - These organizations spend significantly MORE per bed
  - Investigate for potential overspending

LOW OUTLIERS (<-2 std devs): {len(low_outliers)}
  - These organizations spend significantly LESS per bed
  - Potential best practice candidates

FILES CREATED:
✓ consultancy_with_beds.csv - Full dataset
✓ high_outliers_per_bed.csv - High spenders per bed
✓ low_outliers_per_bed.csv - Efficient spenders
✓ consultancy_per_bed_analysis.png - Visual dashboard

This analysis controls for organization SIZE (beds),
making comparisons more meaningful than absolute spending!

Output directory: {OUTPUT_DIR.absolute()}
""")

print("=" * 80)
