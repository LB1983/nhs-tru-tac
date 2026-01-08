#!/usr/bin/env python3
"""
Outlier Detection Analysis for NHS Consultancy Spending

Identifies organizations that are statistical outliers in:
- Consultancy spend (absolute)
- Consultancy as % of turnover
- Consultancy growth rates
- Sector-specific comparisons
"""

import duckdb
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

DB_PATH = Path("Data/canonical/tru_tac.duckdb")
OUTPUT_DIR = Path("Data/analysis/outlier_analysis")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 10)

print("=" * 80)
print("NHS CONSULTANCY OUTLIER ANALYSIS")
print("=" * 80)

con = duckdb.connect(str(DB_PATH), read_only=True)

# ============================================================================
# GET DATA
# ============================================================================
print("\nLoading consultancy and turnover data...")

# Get consultancy codes
consultancy_codes = con.execute("""
    SELECT DISTINCT SubCode
    FROM dim_tac_subcodes_ws
    WHERE LOWER(subcode_label) LIKE '%consult%'
       OR LOWER(subcode_label) LIKE '%advisory%'
""").fetchdf()

codes_list = "', '".join(consultancy_codes['SubCode'].tolist())

# Get consultancy spend
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

# Get operating income
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

# Merge
df = consultancy.merge(operating_income, on=['org_name_raw', 'fy'], how='inner')
df['consultancy_pct_turnover'] = (df['consultancy_spend'] / df['operating_income']) * 100
df['consultancy_millions'] = df['consultancy_spend'] / 1_000_000
df['turnover_millions'] = df['operating_income'] / 1_000_000

print(f"Organizations analyzed: {df['org_name_raw'].nunique()}")
print(f"Years covered: {sorted(df['fy'].unique())}")

# ============================================================================
# 1. OUTLIER DETECTION - CONSULTANCY AS % OF TURNOVER
# ============================================================================
print("\n" + "=" * 80)
print("1. OUTLIERS: CONSULTANCY AS % OF TURNOVER (2023-24)")
print("=" * 80)

latest_year = df['fy'].max()
latest = df[df['fy'] == latest_year].copy()

# Calculate Z-scores
latest['z_score_pct'] = stats.zscore(latest['consultancy_pct_turnover'])

# Identify outliers (|z| > 2 = beyond 95% of data)
high_outliers_pct = latest[latest['z_score_pct'] > 2].sort_values('consultancy_pct_turnover', ascending=False)
low_outliers_pct = latest[latest['z_score_pct'] < -2].sort_values('consultancy_pct_turnover')

print(f"\nHIGH OUTLIERS - Consultancy >2 std devs above mean:")
print(f"Found {len(high_outliers_pct)} organizations\n")
display_cols = ['org_name_raw', 'sector', 'consultancy_millions', 'turnover_millions',
                'consultancy_pct_turnover', 'z_score_pct']
print(high_outliers_pct[display_cols].to_string(index=False))
high_outliers_pct.to_csv(OUTPUT_DIR / "high_outliers_consultancy_pct.csv", index=False)

print(f"\nLOW OUTLIERS - Consultancy >2 std devs below mean:")
print(f"Found {len(low_outliers_pct)} organizations\n")
print(low_outliers_pct[display_cols].to_string(index=False))
low_outliers_pct.to_csv(OUTPUT_DIR / "low_outliers_consultancy_pct.csv", index=False)

# Summary stats
print(f"\nSummary statistics ({latest_year}):")
print(f"  Mean: {latest['consultancy_pct_turnover'].mean():.3f}%")
print(f"  Median: {latest['consultancy_pct_turnover'].median():.3f}%")
print(f"  Std Dev: {latest['consultancy_pct_turnover'].std():.3f}%")
print(f"  Min: {latest['consultancy_pct_turnover'].min():.3f}%")
print(f"  Max: {latest['consultancy_pct_turnover'].max():.3f}%")

# ============================================================================
# 2. OUTLIER DETECTION - ABSOLUTE SPEND
# ============================================================================
print("\n" + "=" * 80)
print("2. OUTLIERS: ABSOLUTE CONSULTANCY SPEND (2023-24)")
print("=" * 80)

latest['z_score_spend'] = stats.zscore(latest['consultancy_millions'])

high_outliers_spend = latest[latest['z_score_spend'] > 2].sort_values('consultancy_millions', ascending=False)

print(f"\nHIGH SPENDERS - Absolute spend >2 std devs above mean:")
print(f"Found {len(high_outliers_spend)} organizations\n")
print(high_outliers_spend[display_cols].to_string(index=False))
high_outliers_spend.to_csv(OUTPUT_DIR / "high_outliers_absolute_spend.csv", index=False)

# ============================================================================
# 3. GROWTH RATE OUTLIERS
# ============================================================================
print("\n" + "=" * 80)
print("3. OUTLIERS: CONSULTANCY GROWTH RATE")
print("=" * 80)

# Calculate growth from first to last year
first_year = df['fy'].min()
growth_df = df[df['fy'].isin([first_year, latest_year])].copy()

growth_pivot = growth_df.pivot_table(
    index=['org_name_raw', 'sector'],
    columns='fy',
    values='consultancy_spend'
).reset_index()

if first_year in growth_pivot.columns and latest_year in growth_pivot.columns:
    growth_pivot['growth_pct'] = (
        (growth_pivot[latest_year] - growth_pivot[first_year]) / growth_pivot[first_year]
    ) * 100

    # Remove infinite values
    growth_pivot = growth_pivot[~growth_pivot['growth_pct'].isin([np.inf, -np.inf])]
    growth_pivot = growth_pivot.dropna(subset=['growth_pct'])

    growth_pivot['z_score_growth'] = stats.zscore(growth_pivot['growth_pct'])

    high_growth = growth_pivot[growth_pivot['z_score_growth'] > 2].sort_values('growth_pct', ascending=False)
    low_growth = growth_pivot[growth_pivot['z_score_growth'] < -2].sort_values('growth_pct')

    print(f"\nHIGH GROWTH OUTLIERS ({first_year} to {latest_year}):")
    print(f"Found {len(high_growth)} organizations\n")
    growth_cols = ['org_name_raw', 'sector', first_year, latest_year, 'growth_pct', 'z_score_growth']
    if len(high_growth) > 0:
        print(high_growth[growth_cols].to_string(index=False))
        high_growth.to_csv(OUTPUT_DIR / "high_growth_outliers.csv", index=False)

    print(f"\nNEGATIVE GROWTH OUTLIERS ({first_year} to {latest_year}):")
    print(f"Found {len(low_growth)} organizations\n")
    if len(low_growth) > 0:
        print(low_growth[growth_cols].to_string(index=False))
        low_growth.to_csv(OUTPUT_DIR / "negative_growth_outliers.csv", index=False)

# ============================================================================
# 4. SECTOR-SPECIFIC OUTLIERS
# ============================================================================
print("\n" + "=" * 80)
print("4. SECTOR-SPECIFIC OUTLIERS")
print("=" * 80)

for sector in latest['sector'].unique():
    sector_data = latest[latest['sector'] == sector].copy()
    sector_data['z_score_sector'] = stats.zscore(sector_data['consultancy_pct_turnover'])

    outliers = sector_data[abs(sector_data['z_score_sector']) > 2]

    print(f"\n{sector} Outliers ({len(outliers)}):")
    if len(outliers) > 0:
        print(outliers[['org_name_raw', 'consultancy_millions', 'consultancy_pct_turnover',
                        'z_score_sector']].to_string(index=False))
        outliers.to_csv(OUTPUT_DIR / f"outliers_{sector.lower()}.csv", index=False)

# ============================================================================
# 5. VISUALIZATIONS
# ============================================================================
print("\n" + "=" * 80)
print("5. CREATING OUTLIER VISUALIZATIONS")
print("=" * 80)

fig = plt.figure(figsize=(20, 12))
gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)

# 1. Box plot by sector
ax1 = fig.add_subplot(gs[0, :])
latest.boxplot(column='consultancy_pct_turnover', by='sector', ax=ax1)
ax1.set_title('Consultancy as % of Turnover - Distribution by Sector', fontweight='bold', fontsize=14)
ax1.set_xlabel('Sector', fontweight='bold')
ax1.set_ylabel('Consultancy % of Turnover', fontweight='bold')
plt.sca(ax1)
plt.xticks(rotation=0)

# Mark outliers
for sector in latest['sector'].unique():
    sector_data = latest[latest['sector'] == sector]
    sector_outliers = sector_data[abs(stats.zscore(sector_data['consultancy_pct_turnover'])) > 2]
    for _, row in sector_outliers.iterrows():
        ax1.text(list(latest['sector'].unique()).index(sector) + 1,
                row['consultancy_pct_turnover'],
                '  ' + row['org_name_raw'][:20],
                fontsize=7, rotation=0)

# 2. Scatter: Turnover vs Consultancy %
ax2 = fig.add_subplot(gs[1, 0])
colors = {'FT': '#1f77b4', 'Trust': '#ff7f0e'}
for sector in latest['sector'].unique():
    sector_data = latest[latest['sector'] == sector]
    ax2.scatter(sector_data['turnover_millions'],
               sector_data['consultancy_pct_turnover'],
               c=colors[sector], label=sector, alpha=0.6, s=50)

# Highlight outliers
all_outliers = latest[abs(latest['z_score_pct']) > 2]
for _, row in all_outliers.iterrows():
    ax2.scatter(row['turnover_millions'], row['consultancy_pct_turnover'],
               c='red', s=200, marker='*', edgecolors='black', linewidths=2)
    ax2.text(row['turnover_millions'], row['consultancy_pct_turnover'],
            '  ' + row['org_name_raw'][:15], fontsize=7)

ax2.set_xlabel('Operating Income (£M)', fontweight='bold')
ax2.set_ylabel('Consultancy % of Turnover', fontweight='bold')
ax2.set_title('Consultancy % vs Organization Size', fontweight='bold', fontsize=12)
ax2.legend()
ax2.grid(alpha=0.3)

# 3. Distribution histogram
ax3 = fig.add_subplot(gs[1, 1])
ax3.hist(latest['consultancy_pct_turnover'], bins=30, edgecolor='black', alpha=0.7)
ax3.axvline(latest['consultancy_pct_turnover'].mean(), color='red', linestyle='--',
           linewidth=2, label=f'Mean: {latest["consultancy_pct_turnover"].mean():.2f}%')
ax3.axvline(latest['consultancy_pct_turnover'].median(), color='green', linestyle='--',
           linewidth=2, label=f'Median: {latest["consultancy_pct_turnover"].median():.2f}%')
ax3.set_xlabel('Consultancy % of Turnover', fontweight='bold')
ax3.set_ylabel('Number of Organizations', fontweight='bold')
ax3.set_title('Distribution of Consultancy %', fontweight='bold', fontsize=12)
ax3.legend()
ax3.grid(alpha=0.3, axis='y')

# 4. Time series of outliers
ax4 = fig.add_subplot(gs[1, 2])
if len(high_outliers_pct) > 0:
    for org in high_outliers_pct.head(5)['org_name_raw']:
        org_data = df[df['org_name_raw'] == org].sort_values('fy')
        ax4.plot(org_data['fy'], org_data['consultancy_pct_turnover'],
                marker='o', label=org[:20])
ax4.set_xlabel('Financial Year', fontweight='bold')
ax4.set_ylabel('Consultancy % of Turnover', fontweight='bold')
ax4.set_title('Top 5 Outliers - Trend Over Time', fontweight='bold', fontsize=12)
ax4.legend(fontsize=8)
ax4.grid(alpha=0.3)
ax4.tick_params(axis='x', rotation=45)

# 5. Growth rate scatter
ax5 = fig.add_subplot(gs[2, :])
if 'growth_pivot' in locals():
    for sector in growth_pivot['sector'].unique():
        sector_data = growth_pivot[growth_pivot['sector'] == sector]
        ax5.scatter(sector_data[first_year] / 1_000_000,
                   sector_data['growth_pct'],
                   c=colors.get(sector, 'gray'), label=sector, alpha=0.6, s=50)

    # Highlight growth outliers
    if len(high_growth) > 0:
        for _, row in high_growth.head(10).iterrows():
            ax5.scatter(row[first_year] / 1_000_000, row['growth_pct'],
                       c='red', s=200, marker='^', edgecolors='black', linewidths=2)
            ax5.text(row[first_year] / 1_000_000, row['growth_pct'],
                    '  ' + row['org_name_raw'][:15], fontsize=7)

    ax5.set_xlabel(f'Consultancy Spend {first_year} (£M)', fontweight='bold')
    ax5.set_ylabel('Growth Rate (%)', fontweight='bold')
    ax5.set_title(f'Consultancy Spend Growth ({first_year} to {latest_year})', fontweight='bold', fontsize=12)
    ax5.legend()
    ax5.grid(alpha=0.3)
    ax5.axhline(y=0, color='black', linestyle='-', linewidth=0.5)

plt.suptitle(f'NHS Consultancy Spending Outlier Analysis ({latest_year})',
            fontsize=16, fontweight='bold', y=0.995)

plt.savefig(OUTPUT_DIR / "outlier_dashboard.png", dpi=300, bbox_inches='tight')
plt.close()
print("✓ Saved: outlier_dashboard.png")

con.close()

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("OUTLIER ANALYSIS SUMMARY")
print("=" * 80)

print(f"""
OUTLIERS IDENTIFIED:

High Consultancy % of Turnover: {len(high_outliers_pct)} organizations
Low Consultancy % of Turnover: {len(low_outliers_pct)} organizations
High Absolute Spend: {len(high_outliers_spend)} organizations
High Growth Rate: {len(high_growth) if 'high_growth' in locals() else 0} organizations
Negative Growth: {len(low_growth) if 'low_growth' in locals() else 0} organizations

FILES CREATED:
✓ high_outliers_consultancy_pct.csv
✓ low_outliers_consultancy_pct.csv
✓ high_outliers_absolute_spend.csv
✓ high_growth_outliers.csv
✓ negative_growth_outliers.csv
✓ outliers_ft.csv
✓ outliers_trust.csv
✓ outlier_dashboard.png

INTERPRETATION:
- Z-score > 2 means organization is in top 2.5% (95th percentile+)
- Z-score < -2 means organization is in bottom 2.5%
- These outliers deserve further investigation:
  • Why is consultancy spend so high/low?
  • Are there special circumstances?
  • Can best practices be shared from low spenders?
  • Should high spenders be reviewed?

NEXT STEPS:
1. Review high outliers for investigation
2. Interview low outliers for best practices
3. Add activity data (beds) to normalize by size
4. Analyze what factors predict consultancy spend

Output directory: {OUTPUT_DIR.absolute()}
""")

print("=" * 80)
