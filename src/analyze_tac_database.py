#!/usr/bin/env python3
"""
Comprehensive analysis of NHS TAC database.
Run this script on your local machine where the database is located.
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Configuration
DB_PATH = Path("Data/canonical/tru_tac.duckdb")
OUTPUT_DIR = Path("Data/analysis/database_analysis")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Set style
sns.set_style("whitegrid")
sns.set_palette("husl")

print("=" * 80)
print("NHS TAC DATABASE ANALYSIS")
print("=" * 80)

# Connect to database
print(f"\nConnecting to: {DB_PATH}")
con = duckdb.connect(str(DB_PATH), read_only=True)

# Get table names
tables = con.execute("SHOW TABLES").fetchall()
print(f"Found {len(tables)} table(s): {[t[0] for t in tables]}")

# Assume main fact table is the first one or contains 'fact' in name
fact_table = None
for table_name, in tables:
    if 'fact' in table_name.lower() or table_name.lower() == 'tru_tac':
        fact_table = table_name
        break

if fact_table is None:
    fact_table = tables[0][0]

print(f"\nAnalyzing table: {fact_table}")

# ============================================================================
# 1. BASIC STATISTICS
# ============================================================================
print("\n" + "=" * 80)
print("1. DATABASE OVERVIEW")
print("=" * 80)

row_count = con.execute(f"SELECT COUNT(*) FROM {fact_table}").fetchone()[0]
print(f"\nTotal records: {row_count:,}")

# Get column info
schema = con.execute(f"PRAGMA table_info('{fact_table}')").fetchdf()
print(f"Total columns: {len(schema)}")
print("\nColumns:")
for _, row in schema.iterrows():
    print(f"  {row['name']:25} {row['type']}")

# Basic stats
stats_query = f"""
SELECT
    COUNT(DISTINCT fy) as unique_years,
    COUNT(DISTINCT sector) as unique_sectors,
    COUNT(DISTINCT org_name_raw) as unique_orgs,
    COUNT(DISTINCT WorkSheetName) as unique_worksheets,
    COUNT(DISTINCT MainCode) as unique_maincodes,
    COUNT(DISTINCT SubCode) as unique_subcodes,
    COUNT(*) as total_records,
    COUNT(amount) as non_null_amounts,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM {fact_table}
"""

stats = con.execute(stats_query).fetchdf()
print("\nDataset Statistics:")
print(stats.T.to_string())

# Save to CSV
stats.T.to_csv(OUTPUT_DIR / "01_database_statistics.csv")

# ============================================================================
# 2. TEMPORAL ANALYSIS
# ============================================================================
print("\n" + "=" * 80)
print("2. TEMPORAL ANALYSIS")
print("=" * 80)

by_year = con.execute(f"""
    SELECT
        fy,
        COUNT(*) as records,
        COUNT(DISTINCT org_name_raw) as orgs,
        COUNT(DISTINCT SubCode) as subcodes,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM {fact_table}
    GROUP BY fy
    ORDER BY fy
""").fetchdf()

print("\nRecords by Financial Year:")
print(by_year.to_string(index=False))
by_year.to_csv(OUTPUT_DIR / "02_by_year.csv", index=False)

# Visualization: Records by year
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))

ax1.bar(range(len(by_year)), by_year['records'], color='steelblue')
ax1.set_xlabel('Financial Year', fontweight='bold')
ax1.set_ylabel('Number of Records', fontweight='bold')
ax1.set_title('Records by Financial Year', fontweight='bold', fontsize=14)
ax1.set_xticks(range(len(by_year)))
ax1.set_xticklabels(by_year['fy'], rotation=45, ha='right')
ax1.grid(axis='y', alpha=0.3)

for i, v in enumerate(by_year['records']):
    ax1.text(i, v, f'{v:,.0f}', ha='center', va='bottom', fontweight='bold')

ax2.plot(range(len(by_year)), by_year['total_amount'] / 1e9,
         marker='o', linewidth=2.5, markersize=10, color='darkgreen')
ax2.set_xlabel('Financial Year', fontweight='bold')
ax2.set_ylabel('Total Amount (£ Billions)', fontweight='bold')
ax2.set_title('Total Financial Amount by Year', fontweight='bold', fontsize=14)
ax2.set_xticks(range(len(by_year)))
ax2.set_xticklabels(by_year['fy'], rotation=45, ha='right')
ax2.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "02_temporal_analysis.png", dpi=300, bbox_inches='tight')
plt.close()

# ============================================================================
# 3. SECTOR ANALYSIS
# ============================================================================
print("\n" + "=" * 80)
print("3. SECTOR ANALYSIS")
print("=" * 80)

by_sector = con.execute(f"""
    SELECT
        sector,
        COUNT(*) as records,
        COUNT(DISTINCT org_name_raw) as orgs,
        COUNT(DISTINCT fy) as years,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM {fact_table}
    GROUP BY sector
    ORDER BY sector
""").fetchdf()

print("\nRecords by Sector:")
print(by_sector.to_string(index=False))
by_sector.to_csv(OUTPUT_DIR / "03_by_sector.csv", index=False)

# Visualization
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))

colors = sns.color_palette("husl", len(by_sector))
ax1.bar(by_sector['sector'], by_sector['orgs'], color=colors)
ax1.set_xlabel('Sector', fontweight='bold')
ax1.set_ylabel('Number of Organizations', fontweight='bold')
ax1.set_title('Organizations by Sector', fontweight='bold', fontsize=14)
ax1.grid(axis='y', alpha=0.3)

for i, v in enumerate(by_sector['orgs']):
    ax1.text(i, v, str(v), ha='center', va='bottom', fontweight='bold')

wedges, texts, autotexts = ax2.pie(by_sector['total_amount'], labels=by_sector['sector'],
                                     autopct='%1.1f%%', startangle=90, colors=colors)
for autotext in autotexts:
    autotext.set_color('white')
    autotext.set_fontweight('bold')
    autotext.set_fontsize(12)
ax2.set_title('Total Amount by Sector', fontweight='bold', fontsize=14)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "03_sector_analysis.png", dpi=300, bbox_inches='tight')
plt.close()

# ============================================================================
# 4. WORKSHEET ANALYSIS
# ============================================================================
print("\n" + "=" * 80)
print("4. WORKSHEET ANALYSIS")
print("=" * 80)

by_worksheet = con.execute(f"""
    SELECT
        WorkSheetName,
        COUNT(*) as records,
        COUNT(DISTINCT org_name_raw) as orgs,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM {fact_table}
    GROUP BY WorkSheetName
    ORDER BY records DESC
    LIMIT 20
""").fetchdf()

print("\nTop 20 Worksheets by Record Count:")
print(by_worksheet.to_string(index=False))
by_worksheet.to_csv(OUTPUT_DIR / "04_by_worksheet.csv", index=False)

# Visualization
fig, ax = plt.subplots(figsize=(12, 10))
colors = sns.color_palette("viridis", len(by_worksheet))
y_pos = range(len(by_worksheet))

ax.barh(y_pos, by_worksheet['records'], color=colors)
ax.set_yticks(y_pos)
ax.set_yticklabels(by_worksheet['WorkSheetName'])
ax.set_xlabel('Number of Records', fontweight='bold')
ax.set_ylabel('Worksheet', fontweight='bold')
ax.set_title('Top 20 Worksheets by Record Count', fontweight='bold', fontsize=14, pad=20)
ax.grid(axis='x', alpha=0.3)

for i, v in enumerate(by_worksheet['records']):
    ax.text(v, i, f' {v:,.0f}', va='center', fontweight='bold')

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "04_worksheet_analysis.png", dpi=300, bbox_inches='tight')
plt.close()

# ============================================================================
# 5. TOP ORGANIZATIONS
# ============================================================================
print("\n" + "=" * 80)
print("5. TOP ORGANIZATIONS")
print("=" * 80)

top_orgs = con.execute(f"""
    SELECT
        org_name_raw,
        sector,
        COUNT(*) as records,
        COUNT(DISTINCT fy) as years,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM {fact_table}
    GROUP BY org_name_raw, sector
    ORDER BY records DESC
    LIMIT 20
""").fetchdf()

print("\nTop 20 Organizations by Record Count:")
print(top_orgs.to_string(index=False))
top_orgs.to_csv(OUTPUT_DIR / "05_top_organizations.csv", index=False)

# ============================================================================
# 6. YEAR-OVER-YEAR TRENDS BY SECTOR
# ============================================================================
print("\n" + "=" * 80)
print("6. YEAR-OVER-YEAR TRENDS")
print("=" * 80)

trends = con.execute(f"""
    SELECT
        fy,
        sector,
        COUNT(*) as records,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM {fact_table}
    GROUP BY fy, sector
    ORDER BY fy, sector
""").fetchdf()

print("\nTrends by Year and Sector:")
print(trends.to_string(index=False))
trends.to_csv(OUTPUT_DIR / "06_trends.csv", index=False)

# Visualization
fig, ax = plt.subplots(figsize=(14, 6))

for sector in trends['sector'].unique():
    sector_data = trends[trends['sector'] == sector]
    ax.plot(range(len(sector_data)), sector_data['total_amount'] / 1e9,
            marker='o', linewidth=2.5, markersize=10, label=sector)

ax.set_xlabel('Financial Year', fontweight='bold', fontsize=12)
ax.set_ylabel('Total Amount (£ Billions)', fontweight='bold', fontsize=12)
ax.set_title('Financial Trends by Sector Over Time', fontweight='bold', fontsize=14, pad=20)
ax.set_xticks(range(len(trends['fy'].unique())))
ax.set_xticklabels(sorted(trends['fy'].unique()), rotation=45, ha='right')
ax.legend(title='Sector', fontsize=10)
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "06_trends_by_sector.png", dpi=300, bbox_inches='tight')
plt.close()

# ============================================================================
# 7. DATA QUALITY REPORT
# ============================================================================
print("\n" + "=" * 80)
print("7. DATA QUALITY")
print("=" * 80)

quality_checks = []

for col in ['org_name_raw', 'fy', 'sector', 'WorkSheetName', 'SubCode', 'amount']:
    null_count = con.execute(f"SELECT COUNT(*) FROM {fact_table} WHERE {col} IS NULL").fetchone()[0]
    null_pct = (null_count / row_count) * 100
    quality_checks.append({
        'column': col,
        'null_count': null_count,
        'null_percentage': null_pct,
        'status': '✓' if null_pct < 1 else '⚠'
    })

quality_df = pd.DataFrame(quality_checks)
print("\nData Quality Checks:")
print(quality_df.to_string(index=False))
quality_df.to_csv(OUTPUT_DIR / "07_data_quality.csv", index=False)

# ============================================================================
# 8. EXECUTIVE SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("8. GENERATING EXECUTIVE SUMMARY")
print("=" * 80)

summary_md = f"""# NHS TAC Database Analysis Report

**Generated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}
**Database:** {DB_PATH}

## Executive Summary

### Dataset Overview
- **Total Records:** {row_count:,}
- **Financial Years:** {stats['unique_years'].values[0]}
- **Organizations:** {stats['unique_orgs'].values[0]:,}
- **Worksheets:** {stats['unique_worksheets'].values[0]}
- **SubCodes:** {stats['unique_subcodes'].values[0]:,}

### Financial Totals
- **Total Amount:** £{stats['total_amount'].values[0] / 1e9:.2f} billion
- **Average Amount:** £{stats['avg_amount'].values[0]:,.2f}

## By Financial Year

{by_year.to_markdown(index=False)}

## By Sector

{by_sector.to_markdown(index=False)}

## Top 20 Worksheets

{by_worksheet.to_markdown(index=False)}

## Top 20 Organizations

{top_orgs.to_markdown(index=False)}

## Data Quality

{quality_df.to_markdown(index=False)}

## Visualizations

All visualizations have been saved to: `{OUTPUT_DIR}`

- `02_temporal_analysis.png` - Records and amounts by year
- `03_sector_analysis.png` - Organizations and amounts by sector
- `04_worksheet_analysis.png` - Top worksheets by volume
- `06_trends_by_sector.png` - Financial trends over time

## Files Generated

All analysis outputs are in: `{OUTPUT_DIR}/`

CSV files:
- `01_database_statistics.csv` - Overall statistics
- `02_by_year.csv` - Breakdown by financial year
- `03_by_sector.csv` - Breakdown by sector
- `04_by_worksheet.csv` - Top worksheets
- `05_top_organizations.csv` - Top organizations
- `06_trends.csv` - Year-over-year trends
- `07_data_quality.csv` - Data quality checks
"""

with open(OUTPUT_DIR / "EXECUTIVE_SUMMARY.md", 'w') as f:
    f.write(summary_md)

print(f"\nExecutive summary saved to: {OUTPUT_DIR / 'EXECUTIVE_SUMMARY.md'}")

con.close()

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE!")
print("=" * 80)
print(f"\nAll outputs saved to: {OUTPUT_DIR.absolute()}")
print("\nGenerated files:")
print("  - EXECUTIVE_SUMMARY.md (comprehensive report)")
print("  - 7 CSV files with detailed breakdowns")
print("  - 4 PNG visualizations")
print("\nYou can now review these files or share them for further analysis.")
