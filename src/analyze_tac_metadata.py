#!/usr/bin/env python3
"""
Comprehensive analysis of NHS TAC metadata and structure.
Analyzes the TAC schema, providers, and evolution over time.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from collections import Counter

# Set style for better visualizations
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

print("=" * 80)
print("NHS TAC METADATA ANALYSIS")
print("=" * 80)

# Load mapping files
print("\n[1/5] Loading mapping files...")
providers = pd.read_csv("mappings/dim_provider.csv")
tac_subcodes = pd.read_csv("mappings/dim_tac_subcodes_by_year.csv")
tac_lines = pd.read_csv("mappings/dim_tac_lines_seed.csv")

print(f"  ✓ Loaded {len(providers)} providers")
print(f"  ✓ Loaded {len(tac_subcodes)} TAC subcodes across all years")
print(f"  ✓ Loaded {len(tac_lines)} TAC lines")

# ============================================================================
# PROVIDER ANALYSIS
# ============================================================================
print("\n" + "=" * 80)
print("[2/5] PROVIDER ANALYSIS")
print("=" * 80)

print(f"\nTotal unique providers: {len(providers)}")
print(f"\nProviders by sector:")
sector_counts = providers['sector'].value_counts()
for sector, count in sector_counts.items():
    print(f"  {sector}: {count} ({count/len(providers)*100:.1f}%)")

print(f"\nProvider activity timespan:")
print(f"  Earliest first seen: {providers['first_fy_seen'].min()}")
print(f"  Latest last seen: {providers['last_fy_seen'].max()}")

# Providers active across all years
all_years = providers[providers['first_fy_seen'] == '2017-18']
all_years = all_years[all_years['last_fy_seen'] == '2023-24']
print(f"\nProviders active across all years (2017-18 to 2023-24): {len(all_years)}")

# Top providers by data volume
print(f"\nTop 10 providers by total data rows:")
top_providers = providers.nlargest(10, 'rows')[['org_name_canonical', 'sector', 'rows', 'first_fy_seen', 'last_fy_seen']]
print(top_providers.to_string(index=False))

# ============================================================================
# TAC STRUCTURE ANALYSIS
# ============================================================================
print("\n" + "=" * 80)
print("[3/5] TAC STRUCTURE ANALYSIS")
print("=" * 80)

print(f"\nFinancial years covered: {sorted(tac_subcodes['fy'].unique())}")

# Worksheet analysis
print(f"\nUnique worksheets across all years:")
worksheets = tac_subcodes['WorkSheetName'].unique()
print(f"  Total: {len(worksheets)}")

ws_counts = tac_subcodes.groupby('WorkSheetName').size().sort_values(ascending=False)
print(f"\nTop 10 worksheets by number of subcodes:")
for ws, count in ws_counts.head(10).items():
    print(f"  {ws}: {count} subcodes")

# SubCode analysis
print(f"\nSubCode statistics:")
print(f"  Total unique subcodes: {tac_subcodes['SubCode'].nunique()}")

subcode_by_fy = tac_subcodes.groupby('fy')['SubCode'].nunique()
print(f"\nSubcodes per financial year:")
for fy, count in subcode_by_fy.items():
    print(f"  {fy}: {count} unique subcodes")

# ============================================================================
# TAC LINES ANALYSIS
# ============================================================================
print("\n" + "=" * 80)
print("[4/5] TAC LINES ANALYSIS")
print("=" * 80)

print(f"\nTAC Lines structure:")
print(f"  Total lines: {len(tac_lines)}")
print(f"  Unique TableIDs: {tac_lines['TableID'].nunique()}")
print(f"  Unique MainCodes: {tac_lines['MainCode'].nunique()}")
print(f"  Unique SubCodes: {tac_lines['SubCode'].nunique()}")

# Lines by TableID
print(f"\nLines distribution by TableID:")
table_counts = tac_lines['TableID'].value_counts().sort_index()
for table_id, count in table_counts.items():
    print(f"  Table {table_id}: {count} lines ({count/len(tac_lines)*100:.1f}%)")

# Categorization analysis
if 'category_1' in tac_lines.columns:
    non_null_cat1 = tac_lines['category_1'].notna().sum()
    print(f"\nCategorization coverage:")
    print(f"  Lines with category_1: {non_null_cat1} ({non_null_cat1/len(tac_lines)*100:.1f}%)")

# ============================================================================
# SCHEMA EVOLUTION ANALYSIS
# ============================================================================
print("\n" + "=" * 80)
print("[5/5] SCHEMA EVOLUTION ANALYSIS")
print("=" * 80)

# Find subcodes that changed over time
subcode_years = tac_subcodes.groupby('SubCode')['fy'].apply(list).to_dict()
all_fy = sorted(tac_subcodes['fy'].unique())

# New subcodes per year
print(f"\nSchema evolution:")
prev_codes = set()
for fy in all_fy:
    current_codes = set(tac_subcodes[tac_subcodes['fy'] == fy]['SubCode'])
    new_codes = current_codes - prev_codes
    removed_codes = prev_codes - current_codes if prev_codes else set()

    print(f"\n  {fy}:")
    print(f"    Total subcodes: {len(current_codes)}")
    if new_codes:
        print(f"    New subcodes: {len(new_codes)}")
    if removed_codes:
        print(f"    Removed subcodes: {len(removed_codes)}")

    prev_codes = current_codes

# Find most common subcodes (appear in all years)
subcode_freq = tac_subcodes.groupby('SubCode')['fy'].nunique()
stable_subcodes = subcode_freq[subcode_freq == len(all_fy)]
print(f"\nStable subcodes (present in all {len(all_fy)} years): {len(stable_subcodes)}")

# Find volatile subcodes (appear in only one year)
volatile_subcodes = subcode_freq[subcode_freq == 1]
print(f"Volatile subcodes (present in only 1 year): {len(volatile_subcodes)}")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)

# ============================================================================
# SAVE SUMMARY STATISTICS
# ============================================================================
output_dir = Path("Data/analysis")
output_dir.mkdir(parents=True, exist_ok=True)

summary_stats = {
    'total_providers': len(providers),
    'total_subcodes': tac_subcodes['SubCode'].nunique(),
    'total_tac_lines': len(tac_lines),
    'financial_years': len(all_fy),
    'worksheets': len(worksheets),
    'stable_subcodes': len(stable_subcodes),
    'volatile_subcodes': len(volatile_subcodes),
}

summary_df = pd.DataFrame([summary_stats])
summary_df.to_csv(output_dir / "summary_statistics.csv", index=False)
print(f"\nSummary statistics saved to: {output_dir / 'summary_statistics.csv'}")

# Save detailed breakdowns
providers.to_csv(output_dir / "provider_analysis.csv", index=False)
print(f"Provider analysis saved to: {output_dir / 'provider_analysis.csv'}")

subcode_summary = tac_subcodes.groupby('SubCode').agg({
    'fy': lambda x: ', '.join(sorted(x.unique())),
    'subcode_label': 'first',
    'WorkSheetName': 'first'
}).reset_index()
subcode_summary.columns = ['SubCode', 'financial_years', 'label', 'primary_worksheet']
subcode_summary['num_years'] = subcode_summary['financial_years'].apply(lambda x: len(x.split(', ')))
subcode_summary = subcode_summary.sort_values('num_years', ascending=False)
subcode_summary.to_csv(output_dir / "subcode_analysis.csv", index=False)
print(f"SubCode analysis saved to: {output_dir / 'subcode_analysis.csv'}")
