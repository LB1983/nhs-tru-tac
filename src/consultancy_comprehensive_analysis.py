#!/usr/bin/env python3
"""
Detailed consultancy analysis:
- All organizations' consultancy spend year-on-year
- Consultancy spend as % of turnover
- Additional insights
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

DB_PATH = Path("Data/canonical/tru_tac.duckdb")
OUTPUT_DIR = Path("Data/analysis/consultancy_detailed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 10)

print("=" * 80)
print("COMPREHENSIVE CONSULTANCY ANALYSIS")
print("=" * 80)

con = duckdb.connect(str(DB_PATH), read_only=True)

# Get all consultancy-related codes
consultancy_codes = con.execute("""
    SELECT DISTINCT SubCode
    FROM dim_tac_subcodes_ws
    WHERE LOWER(subcode_label) LIKE '%consult%'
       OR LOWER(subcode_label) LIKE '%advisory%'
""").fetchdf()

codes_list = "', '".join(consultancy_codes['SubCode'].tolist())

print(f"\nAnalyzing {len(consultancy_codes)} consultancy-related SubCodes")

# ============================================================================
# 1. CONSULTANCY SPEND BY ORGANIZATION & YEAR
# ============================================================================
print("\n" + "=" * 80)
print("1. CONSULTANCY SPEND BY ORGANIZATION (ALL YEARS)")
print("=" * 80)

org_year_spend = con.execute(f"""
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

print(f"\nTotal records: {len(org_year_spend):,}")
print(f"Unique organizations: {org_year_spend['org_name_raw'].nunique()}")

# Save detailed data
org_year_spend['spend_thousands'] = org_year_spend['consultancy_spend'] / 1000
org_year_spend.to_csv(OUTPUT_DIR / "consultancy_by_org_year.csv", index=False)
print(f"✓ Saved: consultancy_by_org_year.csv")

# Show summary stats
print("\nSummary by year:")
year_summary = org_year_spend.groupby('fy').agg({
    'org_name_raw': 'nunique',
    'consultancy_spend': ['sum', 'mean', 'median', 'max']
}).round(0)
year_summary.columns = ['Num_Orgs', 'Total_Spend', 'Mean_Spend', 'Median_Spend', 'Max_Spend']
year_summary = year_summary / 1000  # Convert to thousands
print(year_summary.to_string())

# ============================================================================
# 2. TURNOVER (OPERATING INCOME) BY ORGANIZATION & YEAR
# ============================================================================
print("\n" + "=" * 80)
print("2. CALCULATING TURNOVER (OPERATING INCOME)")
print("=" * 80)

# Operating income SubCodes (from Statement of Comprehensive Income)
turnover = con.execute("""
    SELECT
        org_name_raw,
        sector,
        fy,
        SUM(amount) as operating_income
    FROM fact_tru_tac
    WHERE WorkSheetName = 'TAC02 SoCI'
      AND SubCode IN (
          SELECT DISTINCT SubCode
          FROM dim_tac_subcodes_ws
          WHERE LOWER(subcode_label) LIKE '%operating income%'
             OR LOWER(subcode_label) LIKE '%patient care%income%'
             OR LOWER(subcode_label) LIKE '%other%income%'
      )
    GROUP BY org_name_raw, sector, fy
    ORDER BY org_name_raw, fy
""").fetchdf()

print(f"\nTurnover data points: {len(turnover):,}")
turnover.to_csv(OUTPUT_DIR / "operating_income_by_org_year.csv", index=False)
print(f"✓ Saved: operating_income_by_org_year.csv")

# ============================================================================
# 3. CONSULTANCY AS % OF TURNOVER
# ============================================================================
print("\n" + "=" * 80)
print("3. CONSULTANCY AS % OF TURNOVER")
print("=" * 80)

# Merge consultancy spend with turnover
merged = org_year_spend.merge(
    turnover,
    on=['org_name_raw', 'sector', 'fy'],
    how='inner'
)

# Calculate percentage
merged['consultancy_pct_turnover'] = (merged['consultancy_spend'] / merged['operating_income']) * 100
merged['consultancy_millions'] = merged['consultancy_spend'] / 1_000_000
merged['turnover_millions'] = merged['operating_income'] / 1_000_000

print(f"\nOrganizations with both consultancy and turnover data: {merged['org_name_raw'].nunique()}")

# Save complete dataset
merged_save = merged[['org_name_raw', 'sector', 'fy', 'consultancy_millions',
                      'turnover_millions', 'consultancy_pct_turnover']].copy()
merged_save.to_csv(OUTPUT_DIR / "consultancy_with_turnover_pct.csv", index=False)
print(f"✓ Saved: consultancy_with_turnover_pct.csv")

# Summary statistics by year
print("\nConsultancy as % of turnover - by year:")
pct_summary = merged.groupby('fy').agg({
    'consultancy_pct_turnover': ['mean', 'median', 'min', 'max'],
    'org_name_raw': 'count'
})
pct_summary.columns = ['Mean_%', 'Median_%', 'Min_%', 'Max_%', 'Num_Orgs']
print(pct_summary.to_string())

# ============================================================================
# 4. TOP CONSULTANCY SPENDERS
# ============================================================================
print("\n" + "=" * 80)
print("4. TOP CONSULTANCY SPENDERS")
print("=" * 80)

# By total spend (all years)
top_total = merged.groupby(['org_name_raw', 'sector']).agg({
    'consultancy_spend': 'sum',
    'operating_income': 'sum',
    'fy': 'count'
}).reset_index()
top_total.columns = ['org_name_raw', 'sector', 'total_consultancy', 'total_turnover', 'num_years']
top_total['avg_pct_turnover'] = (top_total['total_consultancy'] / top_total['total_turnover']) * 100
top_total['total_consultancy_millions'] = top_total['total_consultancy'] / 1_000_000
top_total = top_total.sort_values('total_consultancy', ascending=False)

print("\nTop 20 organizations by total consultancy spend (all years):")
print(top_total.head(20)[['org_name_raw', 'sector', 'num_years',
                          'total_consultancy_millions', 'avg_pct_turnover']].to_string(index=False))
top_total.to_csv(OUTPUT_DIR / "top_consultancy_spenders_total.csv", index=False)

# By percentage of turnover (2023-24 only)
latest_year = merged['fy'].max()
top_pct = merged[merged['fy'] == latest_year].copy()
top_pct = top_pct.sort_values('consultancy_pct_turnover', ascending=False)

print(f"\nTop 20 organizations by consultancy % of turnover ({latest_year}):")
print(top_pct.head(20)[['org_name_raw', 'sector', 'consultancy_millions',
                        'turnover_millions', 'consultancy_pct_turnover']].to_string(index=False))
top_pct.to_csv(OUTPUT_DIR / f"top_consultancy_pct_{latest_year}.csv", index=False)

# ============================================================================
# 5. SECTOR COMPARISON
# ============================================================================
print("\n" + "=" * 80)
print("5. SECTOR COMPARISON (FT vs TRUST)")
print("=" * 80)

sector_comparison = merged.groupby(['sector', 'fy']).agg({
    'consultancy_spend': 'sum',
    'operating_income': 'sum',
    'org_name_raw': 'nunique'
}).reset_index()
sector_comparison['consultancy_pct_turnover'] = (
    sector_comparison['consultancy_spend'] / sector_comparison['operating_income']
) * 100

print("\nSector comparison by year:")
for fy in sorted(sector_comparison['fy'].unique()):
    print(f"\n{fy}:")
    fy_data = sector_comparison[sector_comparison['fy'] == fy]
    print(fy_data[['sector', 'org_name_raw', 'consultancy_pct_turnover']].to_string(index=False))

sector_comparison.to_csv(OUTPUT_DIR / "sector_comparison.csv", index=False)

# ============================================================================
# 6. VISUALIZATIONS
# ============================================================================
print("\n" + "=" * 80)
print("6. CREATING VISUALIZATIONS")
print("=" * 80)

# 6.1 Consultancy spend trend by sector
fig, axes = plt.subplots(2, 2, figsize=(18, 12))

# Top left: Total consultancy spend over time
ax1 = axes[0, 0]
for sector in sector_comparison['sector'].unique():
    data = sector_comparison[sector_comparison['sector'] == sector].sort_values('fy')
    ax1.plot(data['fy'], data['consultancy_spend'] / 1_000_000,
            marker='o', linewidth=2.5, markersize=8, label=sector)
ax1.set_xlabel('Financial Year', fontweight='bold')
ax1.set_ylabel('Consultancy Spend (£M)', fontweight='bold')
ax1.set_title('Total Consultancy Spending by Sector', fontweight='bold', fontsize=14)
ax1.legend()
ax1.grid(alpha=0.3)
ax1.tick_params(axis='x', rotation=45)

# Top right: Consultancy as % of turnover over time
ax2 = axes[0, 1]
for sector in sector_comparison['sector'].unique():
    data = sector_comparison[sector_comparison['sector'] == sector].sort_values('fy')
    ax2.plot(data['fy'], data['consultancy_pct_turnover'],
            marker='o', linewidth=2.5, markersize=8, label=sector)
ax2.set_xlabel('Financial Year', fontweight='bold')
ax2.set_ylabel('Consultancy as % of Turnover', fontweight='bold')
ax2.set_title('Consultancy Spending as % of Operating Income', fontweight='bold', fontsize=14)
ax2.legend()
ax2.grid(alpha=0.3)
ax2.tick_params(axis='x', rotation=45)

# Bottom left: Distribution of consultancy % (latest year)
ax3 = axes[1, 0]
latest_data = merged[merged['fy'] == latest_year]
for sector in latest_data['sector'].unique():
    sector_data = latest_data[latest_data['sector'] == sector]['consultancy_pct_turnover']
    ax3.hist(sector_data, bins=30, alpha=0.6, label=sector, edgecolor='black')
ax3.set_xlabel('Consultancy as % of Turnover', fontweight='bold')
ax3.set_ylabel('Number of Organizations', fontweight='bold')
ax3.set_title(f'Distribution of Consultancy % ({latest_year})', fontweight='bold', fontsize=14)
ax3.legend()
ax3.grid(alpha=0.3, axis='y')

# Bottom right: Top 15 spenders (latest year)
ax4 = axes[1, 1]
top_15 = latest_data.nlargest(15, 'consultancy_spend')
colors = ['#1f77b4' if s == 'FT' else '#ff7f0e' for s in top_15['sector']]
y_pos = range(len(top_15))
ax4.barh(y_pos, top_15['consultancy_millions'], color=colors)
ax4.set_yticks(y_pos)
ax4.set_yticklabels([name[:30] + '...' if len(name) > 30 else name
                     for name in top_15['org_name_raw']], fontsize=8)
ax4.set_xlabel('Consultancy Spend (£M)', fontweight='bold')
ax4.set_title(f'Top 15 Consultancy Spenders ({latest_year})', fontweight='bold', fontsize=14)
ax4.grid(alpha=0.3, axis='x')

# Create legend for sector colors
from matplotlib.patches import Patch
legend_elements = [Patch(facecolor='#1f77b4', label='FT'),
                   Patch(facecolor='#ff7f0e', label='Trust')]
ax4.legend(handles=legend_elements, loc='lower right')

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "consultancy_comprehensive_analysis.png", dpi=300, bbox_inches='tight')
plt.close()
print("✓ Saved: consultancy_comprehensive_analysis.png")

# ============================================================================
# 7. ADDITIONAL INSIGHTS
# ============================================================================
print("\n" + "=" * 80)
print("7. ADDITIONAL INSIGHTS")
print("=" * 80)

# Growth analysis
growth = merged.pivot_table(
    index='org_name_raw',
    columns='fy',
    values='consultancy_spend',
    aggfunc='sum'
)

if '2017-18' in growth.columns and latest_year in growth.columns:
    growth['total_growth'] = ((growth[latest_year] - growth['2017-18']) / growth['2017-18']) * 100
    growth = growth[growth['2017-18'] > 0]  # Only orgs with data in both years

    print(f"\nOrganizations with biggest consultancy spend growth (2017-18 to {latest_year}):")
    growth_sorted = growth.nlargest(10, 'total_growth')[[latest_year, '2017-18', 'total_growth']]
    growth_sorted.columns = [f'{latest_year}_spend', '2017-18_spend', 'Growth_%']
    print(growth_sorted.to_string())

con.close()

# ============================================================================
# SUMMARY & RECOMMENDATIONS
# ============================================================================
print("\n" + "=" * 80)
print("SUMMARY & WHAT TO LOOK INTO NEXT")
print("=" * 80)

total_consultancy_latest = sector_comparison[sector_comparison['fy'] == latest_year]['consultancy_spend'].sum()
avg_pct_latest = sector_comparison[sector_comparison['fy'] == latest_year]['consultancy_pct_turnover'].mean()

print(f"""
CONSULTANCY FINDINGS ({latest_year}):
  • Total consultancy spend: £{total_consultancy_latest/1_000_000:.1f}M
  • Average as % of turnover: {avg_pct_latest:.2f}%
  • Organizations analyzed: {merged[merged['fy']==latest_year]['org_name_raw'].nunique()}

FILES CREATED:
  ✓ consultancy_by_org_year.csv - Every org's spend by year
  ✓ consultancy_with_turnover_pct.csv - Spend as % of turnover
  ✓ top_consultancy_spenders_total.csv - Biggest spenders
  ✓ top_consultancy_pct_{latest_year}.csv - Highest % of turnover
  ✓ sector_comparison.csv - FT vs Trust comparison
  ✓ consultancy_comprehensive_analysis.png - Visual dashboard

SUGGESTED NEXT ANALYSES:

  1. IT SPENDING ANALYSIS
     - Capital IT spend (intangibles, software licenses)
     - Revenue IT spend (IT services, maintenance)
     - IT as % of turnover
     - Comparison with consultancy spend

  2. OUTSOURCING PATTERNS
     - Facilities management
     - Clinical services outsourced
     - Back-office functions

  3. EFFICIENCY METRICS
     - Admin costs as % of turnover
     - Non-clinical spend trends
     - Benchmarking across similar organizations

  4. REGIONAL ANALYSIS
     - Consultancy spend by region/ICS
     - Identify geographic patterns

  5. TIME SERIES ANALYSIS
     - Seasonal patterns in consultancy spend
     - Impact of major NHS initiatives
     - Correlation with financial performance

  6. CORRELATION ANALYSIS
     - Consultancy spend vs deficit/surplus
     - Consultancy vs quality metrics (if available)
     - Consultancy vs trust size/complexity
""")

print("=" * 80)
