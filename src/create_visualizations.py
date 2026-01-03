#!/usr/bin/env python3
"""
Create visualizations for NHS TAC data analysis.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Set style
sns.set_style("whitegrid")
sns.set_palette("husl")

# Create output directory
output_dir = Path("Data/analysis/visualizations")
output_dir.mkdir(parents=True, exist_ok=True)

print("=" * 80)
print("CREATING NHS TAC VISUALIZATIONS")
print("=" * 80)

# Load data
print("\nLoading data...")
providers = pd.read_csv("mappings/dim_provider.csv")
tac_subcodes = pd.read_csv("mappings/dim_tac_subcodes_by_year.csv")
tac_lines = pd.read_csv("mappings/dim_tac_lines_seed.csv")

# ============================================================================
# Figure 1: Provider Distribution by Sector
# ============================================================================
print("  [1/6] Creating provider sector distribution...")
fig, ax = plt.subplots(figsize=(10, 6))
sector_counts = providers['sector'].value_counts()
colors = sns.color_palette("husl", len(sector_counts))
wedges, texts, autotexts = ax.pie(sector_counts.values, labels=sector_counts.index,
                                    autopct='%1.1f%%', startangle=90, colors=colors)
for autotext in autotexts:
    autotext.set_color('white')
    autotext.set_fontweight('bold')
    autotext.set_fontsize(12)
ax.set_title('NHS Provider Distribution by Sector\n(Total: {} providers)'.format(len(providers)),
             fontsize=14, fontweight='bold', pad=20)
plt.tight_layout()
plt.savefig(output_dir / "01_provider_sector_distribution.png", dpi=300, bbox_inches='tight')
plt.close()
print(f"    ✓ Saved: {output_dir / '01_provider_sector_distribution.png'}")

# ============================================================================
# Figure 2: Provider Activity Timeline
# ============================================================================
print("  [2/6] Creating provider activity timeline...")
fig, ax = plt.subplots(figsize=(12, 6))

# Count providers by first and last year
first_year_counts = providers.groupby('first_fy_seen').size()
last_year_counts = providers.groupby('last_fy_seen').size()

x = range(len(first_year_counts))
width = 0.35

ax.bar([i - width/2 for i in x], first_year_counts.values, width,
       label='First Seen', color='#2ecc71')
ax.bar([i + width/2 for i in x], last_year_counts.values, width,
       label='Last Seen', color='#e74c3c')

ax.set_xlabel('Financial Year', fontsize=12, fontweight='bold')
ax.set_ylabel('Number of Providers', fontsize=12, fontweight='bold')
ax.set_title('Provider Activity Timeline\n(New vs Departing Providers by Year)',
             fontsize=14, fontweight='bold', pad=20)
ax.set_xticks(x)
ax.set_xticklabels(first_year_counts.index, rotation=45, ha='right')
ax.legend()
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig(output_dir / "02_provider_activity_timeline.png", dpi=300, bbox_inches='tight')
plt.close()
print(f"    ✓ Saved: {output_dir / '02_provider_activity_timeline.png'}")

# ============================================================================
# Figure 3: SubCode Evolution Over Time
# ============================================================================
print("  [3/6] Creating subcode evolution chart...")
fig, ax = plt.subplots(figsize=(12, 6))

subcode_by_fy = tac_subcodes.groupby('fy')['SubCode'].nunique().sort_index()

ax.plot(range(len(subcode_by_fy)), subcode_by_fy.values,
        marker='o', linewidth=2.5, markersize=10, color='#3498db')
ax.fill_between(range(len(subcode_by_fy)), subcode_by_fy.values, alpha=0.3, color='#3498db')

# Add value labels
for i, (fy, val) in enumerate(subcode_by_fy.items()):
    ax.text(i, val + 10, str(val), ha='center', va='bottom',
            fontweight='bold', fontsize=10)

ax.set_xlabel('Financial Year', fontsize=12, fontweight='bold')
ax.set_ylabel('Number of Unique SubCodes', fontsize=12, fontweight='bold')
ax.set_title('TAC Schema Evolution\n(Unique SubCodes per Financial Year)',
             fontsize=14, fontweight='bold', pad=20)
ax.set_xticks(range(len(subcode_by_fy)))
ax.set_xticklabels(subcode_by_fy.index, rotation=45, ha='right')
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig(output_dir / "03_subcode_evolution.png", dpi=300, bbox_inches='tight')
plt.close()
print(f"    ✓ Saved: {output_dir / '03_subcode_evolution.png'}")

# ============================================================================
# Figure 4: Top Worksheets by SubCode Count
# ============================================================================
print("  [4/6] Creating worksheet distribution chart...")
fig, ax = plt.subplots(figsize=(12, 8))

ws_counts = tac_subcodes.groupby('WorkSheetName').size().sort_values(ascending=True).tail(15)

colors = sns.color_palette("viridis", len(ws_counts))
ws_counts.plot(kind='barh', ax=ax, color=colors)

ax.set_xlabel('Number of SubCodes', fontsize=12, fontweight='bold')
ax.set_ylabel('Worksheet', fontsize=12, fontweight='bold')
ax.set_title('Top 15 TAC Worksheets by SubCode Count\n(Across All Years)',
             fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='x', alpha=0.3)

# Add value labels
for i, v in enumerate(ws_counts.values):
    ax.text(v + 5, i, str(v), va='center', fontweight='bold', fontsize=9)

plt.tight_layout()
plt.savefig(output_dir / "04_top_worksheets.png", dpi=300, bbox_inches='tight')
plt.close()
print(f"    ✓ Saved: {output_dir / '04_top_worksheets.png'}")

# ============================================================================
# Figure 5: TAC Lines by TableID
# ============================================================================
print("  [5/6] Creating TAC lines distribution...")
fig, ax = plt.subplots(figsize=(10, 6))

table_counts = tac_lines['TableID'].value_counts().sort_index()

colors = sns.color_palette("rocket", len(table_counts))
ax.bar(table_counts.index.astype(str), table_counts.values, color=colors, edgecolor='black', linewidth=1.5)

ax.set_xlabel('Table ID', fontsize=12, fontweight='bold')
ax.set_ylabel('Number of Lines', fontsize=12, fontweight='bold')
ax.set_title('TAC Lines Distribution by Table ID',
             fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='y', alpha=0.3)

# Add value labels and percentages
total = len(tac_lines)
for i, (table_id, count) in enumerate(table_counts.items()):
    percentage = count / total * 100
    ax.text(i, count + 5, f'{count}\n({percentage:.1f}%)',
            ha='center', va='bottom', fontweight='bold', fontsize=10)

plt.tight_layout()
plt.savefig(output_dir / "05_tac_lines_by_table.png", dpi=300, bbox_inches='tight')
plt.close()
print(f"    ✓ Saved: {output_dir / '05_tac_lines_by_table.png'}")

# ============================================================================
# Figure 6: SubCode Stability Analysis
# ============================================================================
print("  [6/6] Creating subcode stability analysis...")
fig, ax = plt.subplots(figsize=(12, 6))

# Calculate how many years each subcode appears
all_fy = sorted(tac_subcodes['fy'].unique())
subcode_freq = tac_subcodes.groupby('SubCode')['fy'].nunique()
freq_distribution = subcode_freq.value_counts().sort_index()

colors = sns.color_palette("mako", len(freq_distribution))
ax.bar(freq_distribution.index.astype(str), freq_distribution.values,
       color=colors, edgecolor='black', linewidth=1.5)

ax.set_xlabel('Number of Years Present', fontsize=12, fontweight='bold')
ax.set_ylabel('Number of SubCodes', fontsize=12, fontweight='bold')
ax.set_title('SubCode Stability Analysis\n(How many years each SubCode appears)',
             fontsize=14, fontweight='bold', pad=20)
ax.grid(axis='y', alpha=0.3)

# Add value labels
for i, (years, count) in enumerate(freq_distribution.items()):
    ax.text(i, count + 5, str(count), ha='center', va='bottom',
            fontweight='bold', fontsize=10)

# Add annotation for stable vs volatile
stable_count = freq_distribution.get(5, 0)
volatile_count = freq_distribution.get(1, 0)
ax.text(0.98, 0.95, f'Stable (5 years): {stable_count}\nVolatile (1 year): {volatile_count}',
        transform=ax.transAxes, ha='right', va='top',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
        fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig(output_dir / "06_subcode_stability.png", dpi=300, bbox_inches='tight')
plt.close()
print(f"    ✓ Saved: {output_dir / '06_subcode_stability.png'}")

print("\n" + "=" * 80)
print(f"ALL VISUALIZATIONS CREATED SUCCESSFULLY")
print(f"Output directory: {output_dir.absolute()}")
print("=" * 80)
