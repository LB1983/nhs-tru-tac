#!/usr/bin/env python3
"""
Targeted analysis of IT and Consultancy spending in NHS TAC data.
Analyzes:
1. IT Capital Expenditure
2. IT Revenue Expenditure
3. IT Balance Sheet Valuations
4. Consultancy Services Spending
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
OUTPUT_DIR = Path("Data/analysis/it_consultancy_analysis")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Set style
sns.set_style("whitegrid")
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (14, 8)

print("=" * 80)
print("NHS TAC IT & CONSULTANCY ANALYSIS")
print("=" * 80)

# Connect to database
print(f"\nConnecting to: {DB_PATH}")
con = duckdb.connect(str(DB_PATH), read_only=True)

# ============================================================================
# STEP 1: DISCOVER RELEVANT SUBCODES
# ============================================================================
print("\n" + "=" * 80)
print("STEP 1: IDENTIFYING RELEVANT SUBCODES")
print("=" * 80)

print("\n[1/3] Searching for IT-related subcodes...")

# Search for IT-related subcodes in the dimension table
it_subcodes_query = """
SELECT DISTINCT SubCode, subcode_label, WorkSheetName
FROM dim_tac_subcodes
WHERE LOWER(subcode_label) LIKE '%it %'
   OR LOWER(subcode_label) LIKE '% it%'
   OR LOWER(subcode_label) LIKE '%digital%'
   OR LOWER(subcode_label) LIKE '%technology%'
   OR LOWER(subcode_label) LIKE '%information%'
   OR LOWER(subcode_label) LIKE '%computer%'
   OR LOWER(subcode_label) LIKE '%software%'
   OR LOWER(subcode_label) LIKE '%hardware%'
   OR LOWER(subcode_label) LIKE '%system%'
ORDER BY WorkSheetName, SubCode
"""

it_subcodes = con.execute(it_subcodes_query).fetchdf()
print(f"\nFound {len(it_subcodes)} IT-related subcodes:")
print(it_subcodes.to_string(index=False))
it_subcodes.to_csv(OUTPUT_DIR / "01_it_subcodes_identified.csv", index=False)

print("\n[2/3] Searching for consultancy-related subcodes...")

# Search for consultancy subcodes
consultancy_subcodes_query = """
SELECT DISTINCT SubCode, subcode_label, WorkSheetName
FROM dim_tac_subcodes
WHERE LOWER(subcode_label) LIKE '%consult%'
   OR LOWER(subcode_label) LIKE '%advisory%'
   OR LOWER(subcode_label) LIKE '%professional%'
ORDER BY WorkSheetName, SubCode
"""

consultancy_subcodes = con.execute(consultancy_subcodes_query).fetchdf()
print(f"\nFound {len(consultancy_subcodes)} consultancy-related subcodes:")
print(consultancy_subcodes.to_string(index=False))
consultancy_subcodes.to_csv(OUTPUT_DIR / "02_consultancy_subcodes_identified.csv", index=False)

print("\n[3/3] Searching for intangible assets (software) subcodes...")

# Software is typically in intangibles
intangible_subcodes_query = """
SELECT DISTINCT SubCode, subcode_label, WorkSheetName
FROM dim_tac_subcodes
WHERE WorkSheetName = 'TAC13 Intangibles'
   OR LOWER(subcode_label) LIKE '%software%'
   OR LOWER(subcode_label) LIKE '%licence%'
   OR LOWER(subcode_label) LIKE '%license%'
ORDER BY WorkSheetName, SubCode
"""

intangible_subcodes = con.execute(intangible_subcodes_query).fetchdf()
print(f"\nFound {len(intangible_subcodes)} intangible asset subcodes:")
print(intangible_subcodes.head(20).to_string(index=False))
if len(intangible_subcodes) > 20:
    print(f"... and {len(intangible_subcodes) - 20} more")
intangible_subcodes.to_csv(OUTPUT_DIR / "03_intangible_subcodes_identified.csv", index=False)

# ============================================================================
# STEP 2: ANALYZE IT SPENDING
# ============================================================================
print("\n" + "=" * 80)
print("STEP 2: IT SPENDING ANALYSIS")
print("=" * 80)

# Get all IT-related spending from fact table
if len(it_subcodes) > 0:
    it_codes_list = "', '".join(it_subcodes['SubCode'].tolist())

    it_spending_query = f"""
    SELECT
        f.fy,
        f.sector,
        d.subcode_label,
        d.WorkSheetName,
        COUNT(*) as record_count,
        SUM(f.amount) as total_amount,
        AVG(f.amount) as avg_amount,
        COUNT(DISTINCT f.org_name_raw) as num_orgs
    FROM fact_tru_tac f
    JOIN dim_tac_subcodes d ON f.SubCode = d.SubCode AND f.fy = d.fy
    WHERE f.SubCode IN ('{it_codes_list}')
    GROUP BY f.fy, f.sector, d.subcode_label, d.WorkSheetName
    ORDER BY f.fy, total_amount DESC
    """

    it_spending = con.execute(it_spending_query).fetchdf()
    print(f"\nIT Spending Data Points: {len(it_spending)}")
    print("\nTop IT spending categories:")
    print(it_spending.head(20).to_string(index=False))
    it_spending.to_csv(OUTPUT_DIR / "04_it_spending_detail.csv", index=False)

    # Aggregate by year
    it_by_year = it_spending.groupby('fy').agg({
        'total_amount': 'sum',
        'record_count': 'sum'
    }).reset_index()
    it_by_year.columns = ['fy', 'total_it_spend', 'record_count']
    print("\nIT Spending by Year:")
    print(it_by_year.to_string(index=False))
    it_by_year.to_csv(OUTPUT_DIR / "05_it_spending_by_year.csv", index=False)

    # Visualize IT spending over time
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Total IT spend by year
    ax1.plot(range(len(it_by_year)), it_by_year['total_it_spend'] / 1e6,
             marker='o', linewidth=3, markersize=12, color='#2E86AB')
    ax1.fill_between(range(len(it_by_year)), it_by_year['total_it_spend'] / 1e6,
                      alpha=0.3, color='#2E86AB')
    ax1.set_xlabel('Financial Year', fontweight='bold', fontsize=12)
    ax1.set_ylabel('IT Spend (£ Millions)', fontweight='bold', fontsize=12)
    ax1.set_title('Total IT Spending Over Time', fontweight='bold', fontsize=14, pad=15)
    ax1.set_xticks(range(len(it_by_year)))
    ax1.set_xticklabels(it_by_year['fy'], rotation=45, ha='right')
    ax1.grid(axis='y', alpha=0.3)

    for i, v in enumerate(it_by_year['total_it_spend']):
        ax1.text(i, v / 1e6 + (it_by_year['total_it_spend'].max() / 1e6 * 0.02),
                f'£{v/1e6:.1f}M', ha='center', va='bottom', fontweight='bold')

    # IT spend by sector
    it_by_sector_year = it_spending.groupby(['fy', 'sector'])['total_amount'].sum().reset_index()

    for sector in it_by_sector_year['sector'].unique():
        sector_data = it_by_sector_year[it_by_sector_year['sector'] == sector]
        ax2.plot(range(len(sector_data)), sector_data['total_amount'] / 1e6,
                marker='o', linewidth=2.5, markersize=10, label=sector)

    ax2.set_xlabel('Financial Year', fontweight='bold', fontsize=12)
    ax2.set_ylabel('IT Spend (£ Millions)', fontweight='bold', fontsize=12)
    ax2.set_title('IT Spending by Sector', fontweight='bold', fontsize=14, pad=15)
    ax2.set_xticks(range(len(it_by_year)))
    ax2.set_xticklabels(it_by_year['fy'], rotation=45, ha='right')
    ax2.legend(title='Sector')
    ax2.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "05_it_spending_trends.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"\n✓ Saved visualization: 05_it_spending_trends.png")

# ============================================================================
# STEP 3: ANALYZE INTANGIBLE ASSETS (SOFTWARE)
# ============================================================================
print("\n" + "=" * 80)
print("STEP 3: IT BALANCE SHEET ANALYSIS (Intangible Assets)")
print("=" * 80)

# Focus on TAC13 Intangibles worksheet
intangibles_query = """
SELECT
    f.fy,
    f.sector,
    d.subcode_label,
    f.SubCode,
    COUNT(*) as record_count,
    SUM(f.amount) as total_amount,
    AVG(f.amount) as avg_amount,
    COUNT(DISTINCT f.org_name_raw) as num_orgs
FROM fact_tru_tac f
JOIN dim_tac_subcodes d ON f.SubCode = d.SubCode AND f.fy = d.fy
WHERE f.WorkSheetName = 'TAC13 Intangibles'
GROUP BY f.fy, f.sector, d.subcode_label, f.SubCode
ORDER BY f.fy, total_amount DESC
"""

intangibles = con.execute(intangibles_query).fetchdf()
print(f"\nIntangible Assets Data Points: {len(intangibles)}")
print("\nTop intangible asset categories:")
print(intangibles.head(20).to_string(index=False))
intangibles.to_csv(OUTPUT_DIR / "06_intangibles_detail.csv", index=False)

# Aggregate by year
intangibles_by_year = intangibles.groupby('fy').agg({
    'total_amount': 'sum',
    'record_count': 'sum'
}).reset_index()
intangibles_by_year.columns = ['fy', 'total_intangibles_value', 'record_count']
print("\nIntangible Assets by Year:")
print(intangibles_by_year.to_string(index=False))
intangibles_by_year.to_csv(OUTPUT_DIR / "07_intangibles_by_year.csv", index=False)

# Visualize
fig, ax = plt.subplots(figsize=(14, 6))
ax.bar(range(len(intangibles_by_year)), intangibles_by_year['total_intangibles_value'] / 1e6,
       color='#A23B72', edgecolor='black', linewidth=1.5)
ax.set_xlabel('Financial Year', fontweight='bold', fontsize=12)
ax.set_ylabel('Total Value (£ Millions)', fontweight='bold', fontsize=12)
ax.set_title('Intangible Assets (Software/IT) Balance Sheet Values', fontweight='bold', fontsize=14, pad=15)
ax.set_xticks(range(len(intangibles_by_year)))
ax.set_xticklabels(intangibles_by_year['fy'], rotation=45, ha='right')
ax.grid(axis='y', alpha=0.3)

for i, v in enumerate(intangibles_by_year['total_intangibles_value']):
    ax.text(i, v / 1e6 + (intangibles_by_year['total_intangibles_value'].max() / 1e6 * 0.02),
            f'£{v/1e6:.1f}M', ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "07_intangibles_values.png", dpi=300, bbox_inches='tight')
plt.close()
print(f"\n✓ Saved visualization: 07_intangibles_values.png")

# ============================================================================
# STEP 4: ANALYZE CONSULTANCY SPENDING
# ============================================================================
print("\n" + "=" * 80)
print("STEP 4: CONSULTANCY SPENDING ANALYSIS")
print("=" * 80)

if len(consultancy_subcodes) > 0:
    consultancy_codes_list = "', '".join(consultancy_subcodes['SubCode'].tolist())

    consultancy_query = f"""
    SELECT
        f.fy,
        f.sector,
        d.subcode_label,
        d.WorkSheetName,
        COUNT(*) as record_count,
        SUM(f.amount) as total_amount,
        AVG(f.amount) as avg_amount,
        COUNT(DISTINCT f.org_name_raw) as num_orgs
    FROM fact_tru_tac f
    JOIN dim_tac_subcodes d ON f.SubCode = d.SubCode AND f.fy = d.fy
    WHERE f.SubCode IN ('{consultancy_codes_list}')
    GROUP BY f.fy, f.sector, d.subcode_label, d.WorkSheetName
    ORDER BY f.fy, total_amount DESC
    """

    consultancy_spending = con.execute(consultancy_query).fetchdf()
    print(f"\nConsultancy Spending Data Points: {len(consultancy_spending)}")
    print("\nConsultancy spending categories:")
    print(consultancy_spending.to_string(index=False))
    consultancy_spending.to_csv(OUTPUT_DIR / "08_consultancy_detail.csv", index=False)

    # Aggregate by year
    consultancy_by_year = consultancy_spending.groupby('fy').agg({
        'total_amount': 'sum',
        'record_count': 'sum'
    }).reset_index()
    consultancy_by_year.columns = ['fy', 'total_consultancy_spend', 'record_count']
    print("\nConsultancy Spending by Year:")
    print(consultancy_by_year.to_string(index=False))
    consultancy_by_year.to_csv(OUTPUT_DIR / "09_consultancy_by_year.csv", index=False)

    # Visualize
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(range(len(consultancy_by_year)), consultancy_by_year['total_consultancy_spend'] / 1e6,
            marker='o', linewidth=3, markersize=12, color='#F18F01')
    ax.fill_between(range(len(consultancy_by_year)), consultancy_by_year['total_consultancy_spend'] / 1e6,
                     alpha=0.3, color='#F18F01')
    ax.set_xlabel('Financial Year', fontweight='bold', fontsize=12)
    ax.set_ylabel('Consultancy Spend (£ Millions)', fontweight='bold', fontsize=12)
    ax.set_title('Consultancy Services Spending Over Time', fontweight='bold', fontsize=14, pad=15)
    ax.set_xticks(range(len(consultancy_by_year)))
    ax.set_xticklabels(consultancy_by_year['fy'], rotation=45, ha='right')
    ax.grid(axis='y', alpha=0.3)

    for i, v in enumerate(consultancy_by_year['total_consultancy_spend']):
        ax.text(i, v / 1e6 + (consultancy_by_year['total_consultancy_spend'].max() / 1e6 * 0.02),
                f'£{v/1e6:.1f}M', ha='center', va='bottom', fontweight='bold')

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "09_consultancy_trends.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"\n✓ Saved visualization: 09_consultancy_trends.png")

# ============================================================================
# STEP 5: COMBINED SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("STEP 5: COMBINED SUMMARY")
print("=" * 80)

# Create combined summary
summary_data = []
for fy in sorted(intangibles_by_year['fy'].unique()):
    row = {'fy': fy}

    if len(it_subcodes) > 0:
        it_val = it_by_year[it_by_year['fy'] == fy]['total_it_spend'].values
        row['it_spend'] = it_val[0] if len(it_val) > 0 else 0
    else:
        row['it_spend'] = 0

    int_val = intangibles_by_year[intangibles_by_year['fy'] == fy]['total_intangibles_value'].values
    row['intangibles_value'] = int_val[0] if len(int_val) > 0 else 0

    if len(consultancy_subcodes) > 0:
        cons_val = consultancy_by_year[consultancy_by_year['fy'] == fy]['total_consultancy_spend'].values
        row['consultancy_spend'] = cons_val[0] if len(cons_val) > 0 else 0
    else:
        row['consultancy_spend'] = 0

    summary_data.append(row)

summary_df = pd.DataFrame(summary_data)
print("\nCombined Summary (£ millions):")
summary_display = summary_df.copy()
summary_display['it_spend'] = summary_display['it_spend'] / 1e6
summary_display['intangibles_value'] = summary_display['intangibles_value'] / 1e6
summary_display['consultancy_spend'] = summary_display['consultancy_spend'] / 1e6
print(summary_display.to_string(index=False, float_format='%.2f'))
summary_df.to_csv(OUTPUT_DIR / "10_combined_summary.csv", index=False)

# Combined visualization
fig, ax = plt.subplots(figsize=(16, 8))

x = range(len(summary_df))
width = 0.25

if len(it_subcodes) > 0:
    ax.bar([i - width for i in x], summary_df['it_spend'] / 1e6,
           width, label='IT Spend', color='#2E86AB')

ax.bar(x, summary_df['intangibles_value'] / 1e6,
       width, label='Intangibles Value', color='#A23B72')

if len(consultancy_subcodes) > 0:
    ax.bar([i + width for i in x], summary_df['consultancy_spend'] / 1e6,
           width, label='Consultancy Spend', color='#F18F01')

ax.set_xlabel('Financial Year', fontweight='bold', fontsize=12)
ax.set_ylabel('Amount (£ Millions)', fontweight='bold', fontsize=12)
ax.set_title('IT & Consultancy Spending Summary', fontweight='bold', fontsize=14, pad=15)
ax.set_xticks(x)
ax.set_xticklabels(summary_df['fy'], rotation=45, ha='right')
ax.legend(fontsize=11)
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "10_combined_summary.png", dpi=300, bbox_inches='tight')
plt.close()
print(f"\n✓ Saved visualization: 10_combined_summary.png")

con.close()

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE!")
print("=" * 80)
print(f"\nAll outputs saved to: {OUTPUT_DIR.absolute()}")
print("\nGenerated files:")
print("  - SubCode identification CSVs (IT, consultancy, intangibles)")
print("  - Detailed spending breakdowns by year and sector")
print("  - Trend visualizations")
print("  - Combined summary report")
