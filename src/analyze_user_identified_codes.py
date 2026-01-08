#!/usr/bin/env python3
"""
Analyze specific SubCodes identified by the user
Edit the CONSULTANCY_CODES and IT_CODES lists below
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURE YOUR CODES HERE
# ============================================================================

# Consultancy codes you've identified
CONSULTANCY_CODES = [
    'EXP0190',  # Add more as you identify them
    # 'EXP0XXX',
    # 'EXP0YYY',
]

# IT codes you've identified (capital and revenue)
IT_OPEX_CODES = [
    # Add operating expense IT codes here
    # 'EXP0XXX',
]

IT_CAPEX_CODES = [
    # Add capital expenditure IT codes here (from intangibles or PPE)
    # 'INT0XXX',
    # 'PPE0XXX',
]

# ============================================================================
# ANALYSIS
# ============================================================================

DB_PATH = Path("Data/canonical/tru_tac.duckdb")
OUTPUT_DIR = Path("Data/analysis/user_identified_analysis")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

print("=" * 80)
print("ANALYSIS OF USER-IDENTIFIED CODES")
print("=" * 80)

print(f"\nConsultancy codes: {len(CONSULTANCY_CODES)}")
print(f"IT OpEx codes: {len(IT_OPEX_CODES)}")
print(f"IT CapEx codes: {len(IT_CAPEX_CODES)}")

con = duckdb.connect(str(DB_PATH), read_only=True)

# ============================================================================
# 1. CONSULTANCY ANALYSIS
# ============================================================================
if len(CONSULTANCY_CODES) > 0:
    print("\n" + "=" * 80)
    print("CONSULTANCY ANALYSIS")
    print("=" * 80)

    codes_list = "', '".join(CONSULTANCY_CODES)

    # Verify codes exist and get labels
    code_details = con.execute(f"""
        SELECT DISTINCT
            SubCode,
            MAX(subcode_label) as subcode_label
        FROM dim_tac_subcodes_ws
        WHERE SubCode IN ('{codes_list}')
        GROUP BY SubCode
    """).fetchdf()

    print("\nCodes being analyzed:")
    print(code_details.to_string(index=False))

    # By year
    by_year = con.execute(f"""
        SELECT
            fy,
            COUNT(*) as records,
            COUNT(DISTINCT org_name_raw) as orgs,
            SUM(amount) as total_spend
        FROM fact_tru_tac
        WHERE SubCode IN ('{codes_list}')
        GROUP BY fy
        ORDER BY fy
    """).fetchdf()

    by_year['spend_millions'] = by_year['total_spend'] / 1_000_000

    print("\nConsultancy spending by year:")
    print(by_year[['fy', 'orgs', 'spend_millions']].to_string(index=False))
    by_year.to_csv(OUTPUT_DIR / "consultancy_by_year.csv", index=False)

    # By sector
    by_sector = con.execute(f"""
        SELECT
            sector,
            fy,
            SUM(amount) as total_spend
        FROM fact_tru_tac
        WHERE SubCode IN ('{codes_list}')
        GROUP BY sector, fy
        ORDER BY fy, sector
    """).fetchdf()

    print("\nConsultancy by sector and year:")
    by_sector_pivot = by_sector.pivot(index='fy', columns='sector', values='total_spend') / 1_000_000
    print(by_sector_pivot.to_string())
    by_sector.to_csv(OUTPUT_DIR / "consultancy_by_sector_year.csv", index=False)

    # Top organizations
    top_orgs = con.execute(f"""
        SELECT
            org_name_raw,
            sector,
            COUNT(DISTINCT fy) as years,
            SUM(amount) as total_spend
        FROM fact_tru_tac
        WHERE SubCode IN ('{codes_list}')
        GROUP BY org_name_raw, sector
        ORDER BY total_spend DESC
        LIMIT 20
    """).fetchdf()

    top_orgs['spend_millions'] = top_orgs['total_spend'] / 1_000_000
    print("\nTop 20 organizations by consultancy spending:")
    print(top_orgs[['org_name_raw', 'sector', 'spend_millions']].to_string(index=False))
    top_orgs.to_csv(OUTPUT_DIR / "top_consultancy_orgs.csv", index=False)

    # Visualization
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Trend over time
    ax1.plot(range(len(by_year)), by_year['spend_millions'],
             marker='o', linewidth=3, markersize=12, color='#F18F01')
    ax1.fill_between(range(len(by_year)), by_year['spend_millions'],
                     alpha=0.3, color='#F18F01')
    ax1.set_xlabel('Financial Year', fontweight='bold', fontsize=12)
    ax1.set_ylabel('Consultancy Spend (£ Millions)', fontweight='bold', fontsize=12)
    ax1.set_title('NHS Consultancy Spending Over Time', fontweight='bold', fontsize=14, pad=15)
    ax1.set_xticks(range(len(by_year)))
    ax1.set_xticklabels(by_year['fy'], rotation=45, ha='right')
    ax1.grid(axis='y', alpha=0.3)

    for i, v in enumerate(by_year['spend_millions']):
        ax1.text(i, v + (by_year['spend_millions'].max() * 0.02),
                f'£{v:.1f}M', ha='center', va='bottom', fontweight='bold')

    # By sector
    for sector in by_sector['sector'].unique():
        sector_data = by_sector[by_sector['sector'] == sector].sort_values('fy')
        ax2.plot(range(len(sector_data)), sector_data['total_spend'] / 1_000_000,
                marker='o', linewidth=2.5, markersize=10, label=sector)

    ax2.set_xlabel('Financial Year', fontweight='bold', fontsize=12)
    ax2.set_ylabel('Consultancy Spend (£ Millions)', fontweight='bold', fontsize=12)
    ax2.set_title('Consultancy Spending by Sector', fontweight='bold', fontsize=14, pad=15)
    ax2.set_xticks(range(len(by_year)))
    ax2.set_xticklabels(by_year['fy'], rotation=45, ha='right')
    ax2.legend(title='Sector')
    ax2.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "consultancy_trends.png", dpi=300, bbox_inches='tight')
    plt.close()
    print(f"\n✓ Saved visualization: consultancy_trends.png")

# ============================================================================
# 2. IT ANALYSIS (if codes provided)
# ============================================================================
all_it_codes = IT_OPEX_CODES + IT_CAPEX_CODES

if len(all_it_codes) > 0:
    print("\n" + "=" * 80)
    print("IT ANALYSIS")
    print("=" * 80)

    codes_list = "', '".join(all_it_codes)

    # Verify codes
    code_details = con.execute(f"""
        SELECT DISTINCT
            SubCode,
            MAX(subcode_label) as subcode_label
        FROM dim_tac_subcodes_ws
        WHERE SubCode IN ('{codes_list}')
        GROUP BY SubCode
    """).fetchdf()

    print("\nIT codes being analyzed:")
    print(code_details.to_string(index=False))

    # By year
    by_year_it = con.execute(f"""
        SELECT
            fy,
            SUM(amount) as total_spend
        FROM fact_tru_tac
        WHERE SubCode IN ('{codes_list}')
        GROUP BY fy
        ORDER BY fy
    """).fetchdf()

    by_year_it['spend_millions'] = by_year_it['total_spend'] / 1_000_000

    print("\nIT spending by year:")
    print(by_year_it[['fy', 'spend_millions']].to_string(index=False))
    by_year_it.to_csv(OUTPUT_DIR / "it_by_year.csv", index=False)

con.close()

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

if len(CONSULTANCY_CODES) > 0:
    total_consultancy = by_year['total_spend'].sum() / 1_000_000
    latest_year_consultancy = by_year[by_year['fy'] == by_year['fy'].max()]['spend_millions'].values[0]
    print(f"\nConsultancy:")
    print(f"  Total spend (all years): £{total_consultancy:.1f}M")
    print(f"  Latest year spend ({by_year['fy'].max()}): £{latest_year_consultancy:.1f}M")
    print(f"  Number of codes analyzed: {len(CONSULTANCY_CODES)}")

if len(all_it_codes) > 0:
    total_it = by_year_it['total_spend'].sum() / 1_000_000
    latest_year_it = by_year_it[by_year_it['fy'] == by_year_it['fy'].max()]['spend_millions'].values[0]
    print(f"\nIT:")
    print(f"  Total spend (all years): £{total_it:.1f}M")
    print(f"  Latest year spend ({by_year_it['fy'].max()}): £{latest_year_it:.1f}M")
    print(f"  Number of codes analyzed: {len(all_it_codes)}")

print(f"\n✓ All outputs saved to: {OUTPUT_DIR.absolute()}")

print("\n" + "=" * 80)
print("TO ADD MORE CODES:")
print("=" * 80)
print("Edit this script (src/analyze_user_identified_codes.py)")
print("Add SubCodes to the lists at the top:")
print("  - CONSULTANCY_CODES")
print("  - IT_OPEX_CODES")
print("  - IT_CAPEX_CODES")
print("Then run again: python src\\analyze_user_identified_codes.py")
print("=" * 80)
