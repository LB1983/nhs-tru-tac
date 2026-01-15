#!/usr/bin/env python3
"""
Export PFI analysis data in Flourish-ready formats
Creates multiple CSV files optimized for different Flourish chart types
"""

import pandas as pd
from pathlib import Path

# Input
PFI_DATA = Path("Data/analysis/pfi_analysis/pfi_with_beds.csv")
OUTPUT_DIR = Path("Data/analysis/pfi_analysis/flourish_exports")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("=" * 80)
print("PFI DATA EXPORT FOR FLOURISH")
print("=" * 80)

# Load PFI data
pfi = pd.read_csv(PFI_DATA)
print(f"\n✓ Loaded {len(pfi)} records")
print(f"  Organizations: {pfi['org_name_raw'].nunique()}")
print(f"  Years: {sorted(pfi['fy'].unique())}")

# ============================================================================
# 1. BAR CHART RACE - Top PFI spenders over time
# ============================================================================
print("\n[1/5] Creating bar chart race data...")

# Get top 20 organizations by maximum PFI per bed across all years
top_orgs = pfi.groupby('org_name_raw')['total_pfi_per_bed'].max().nlargest(20).index

race_data = pfi[pfi['org_name_raw'].isin(top_orgs)][['fy', 'org_name_raw', 'total_pfi_per_bed']].copy()
race_data = race_data.pivot(index='org_name_raw', columns='fy', values='total_pfi_per_bed').fillna(0)

race_data.to_csv(OUTPUT_DIR / "1_bar_chart_race.csv")
print(f"  ✓ Saved: 1_bar_chart_race.csv")
print(f"    → Flourish template: Bar Chart Race")
print(f"    → Shows: Top 20 orgs by PFI per bed over time")

# ============================================================================
# 2. SCATTER PLOT - Capital vs Revenue PFI per bed
# ============================================================================
print("\n[2/5] Creating scatter plot data...")

# Use latest year for scatter
latest_year = pfi['fy'].max()
scatter_data = pfi[pfi['fy'] == latest_year].copy()

# Ensure we have both capital and revenue columns
if 'capital_pfi_per_bed' in scatter_data.columns and 'revenue_pfi_per_bed' in scatter_data.columns:
    scatter_export = scatter_data[[
        'org_name_raw', 'sector', 'beds',
        'capital_pfi_per_bed', 'revenue_pfi_per_bed', 'total_pfi_per_bed'
    ]].copy()

    scatter_export.columns = ['Organization', 'Sector', 'Beds',
                              'Capital PFI per Bed', 'Revenue PFI per Bed', 'Total PFI per Bed']

    scatter_export.to_csv(OUTPUT_DIR / "2_scatter_capital_vs_revenue.csv", index=False)
    print(f"  ✓ Saved: 2_scatter_capital_vs_revenue.csv")
    print(f"    → Flourish template: Scatter")
    print(f"    → X-axis: Capital PFI per Bed, Y-axis: Revenue PFI per Bed")
    print(f"    → Size: Beds, Color: Sector")

# ============================================================================
# 3. STACKED BAR CHART - Capital vs Revenue breakdown by organization
# ============================================================================
print("\n[3/5] Creating stacked bar chart data...")

if 'capital_pfi_per_bed' in pfi.columns and 'revenue_pfi_per_bed' in pfi.columns:
    # Top 30 organizations by total PFI per bed in latest year
    latest = pfi[pfi['fy'] == latest_year].copy()
    top_30 = latest.nlargest(30, 'total_pfi_per_bed')

    stacked_data = top_30[['org_name_raw', 'capital_pfi_per_bed', 'revenue_pfi_per_bed']].copy()
    stacked_data.columns = ['Organization', 'Capital', 'Revenue']
    stacked_data = stacked_data.sort_values('Capital', ascending=False)

    stacked_data.to_csv(OUTPUT_DIR / "3_stacked_bar_capital_revenue.csv", index=False)
    print(f"  ✓ Saved: 3_stacked_bar_capital_revenue.csv")
    print(f"    → Flourish template: Stacked Bar Chart")
    print(f"    → Shows: Top 30 orgs, Capital vs Revenue per bed")

# ============================================================================
# 4. LINE CHART - PFI trends over time for top organizations
# ============================================================================
print("\n[4/5] Creating line chart data...")

# Top 10 organizations by average PFI per bed
top_10_avg = pfi.groupby('org_name_raw')['total_pfi_per_bed'].mean().nlargest(10).index

line_data = pfi[pfi['org_name_raw'].isin(top_10_avg)][['fy', 'org_name_raw', 'total_pfi_per_bed']].copy()
line_data = line_data.pivot(index='fy', columns='org_name_raw', values='total_pfi_per_bed')

line_data.to_csv(OUTPUT_DIR / "4_line_chart_trends.csv")
print(f"  ✓ Saved: 4_line_chart_trends.csv")
print(f"    → Flourish template: Line Chart")
print(f"    → Shows: Top 10 orgs PFI per bed trends")

# ============================================================================
# 5. TABLE - Detailed rankings with all metrics
# ============================================================================
print("\n[5/5] Creating data table...")

latest = pfi[pfi['fy'] == latest_year].copy()
table_data = latest.sort_values('total_pfi_per_bed', ascending=False).copy()

# Select and rename columns for clarity
if 'capital_pfi_per_bed' in table_data.columns:
    table_export = table_data[[
        'org_name_raw', 'sector', 'beds',
        'capital_pfi_per_bed', 'revenue_pfi_per_bed', 'total_pfi_per_bed',
        'Capital', 'Revenue', 'total_pfi'
    ]].copy()

    table_export.columns = [
        'Organization', 'Sector', 'Beds',
        'Capital PFI per Bed (£)', 'Revenue PFI per Bed (£)', 'Total PFI per Bed (£)',
        'Total Capital PFI (£)', 'Total Revenue PFI (£)', 'Total PFI (£)'
    ]
else:
    table_export = table_data[[
        'org_name_raw', 'sector', 'beds',
        'total_pfi_per_bed', 'total_pfi'
    ]].copy()

    table_export.columns = [
        'Organization', 'Sector', 'Beds',
        'Total PFI per Bed (£)', 'Total PFI (£)'
    ]

# Add ranking
table_export.insert(0, 'Rank', range(1, len(table_export) + 1))

table_export.to_csv(OUTPUT_DIR / "5_table_detailed_rankings.csv", index=False)
print(f"  ✓ Saved: 5_table_detailed_rankings.csv")
print(f"    → Flourish template: Table")
print(f"    → Shows: All organizations ranked by PFI per bed")

# ============================================================================
# 6. BONUS: Sector comparison for bar chart
# ============================================================================
print("\n[6/6] Creating sector comparison data...")

sector_comp = pfi[pfi['fy'] == latest_year].groupby('sector').agg({
    'capital_pfi_per_bed': 'mean',
    'revenue_pfi_per_bed': 'mean',
    'total_pfi_per_bed': 'mean',
    'org_name_raw': 'count'
}).reset_index()

sector_comp.columns = ['Sector', 'Avg Capital per Bed', 'Avg Revenue per Bed',
                       'Avg Total per Bed', 'Number of Organizations']

sector_comp.to_csv(OUTPUT_DIR / "6_sector_comparison.csv", index=False)
print(f"  ✓ Saved: 6_sector_comparison.csv")
print(f"    → Flourish template: Bar Chart or Column Chart")
print(f"    → Shows: Average PFI per bed by sector")

print("\n" + "=" * 80)
print("EXPORT COMPLETE")
print("=" * 80)
print(f"\n✓ All files saved to: {OUTPUT_DIR}")
print("\nFLOURISH IMPORT INSTRUCTIONS:")
print("1. Go to https://flourish.studio")
print("2. Create a new project")
print("3. Choose template based on file number (see above)")
print("4. Upload the corresponding CSV file")
print("5. Customize colors, labels, and formatting")
print("\nRECOMMENDED VISUALIZATIONS:")
print("  • Bar Chart Race (#1) - Great for showing changes over time")
print("  • Scatter Plot (#2) - Shows relationship between capital and revenue")
print("  • Stacked Bar (#3) - Compares capital vs revenue breakdown")
print("  • Line Chart (#4) - Tracks trends for top organizations")
print("  • Table (#5) - Detailed data for exploration")
print("  • Bar Chart (#6) - Sector comparison")
