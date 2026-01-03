#!/usr/bin/env python3
"""Explore the structure of illustrative TAC files."""

from pathlib import Path
import pandas as pd

REF_DIR = Path("Data/reference")

print("=" * 80)
print("NHS TAC Illustrative Files - Structure Exploration")
print("=" * 80)

for path in sorted(REF_DIR.glob("*.xlsx")):
    print(f"\n{'=' * 80}")
    print(f"File: {path.name}")
    print('=' * 80)

    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
        print(f"\nSheets ({len(xls.sheet_names)}):")
        for s in xls.sheet_names:
            print(f"  - {s}")

        # Try to find and preview the "All data" sheet
        all_data_sheets = [s for s in xls.sheet_names
                          if 'all' in s.lower() and 'data' in s.lower()]

        if all_data_sheets:
            sheet_name = all_data_sheets[0]
            print(f"\nPreviewing '{sheet_name}' sheet:")
            df = pd.read_excel(path, sheet_name=sheet_name, nrows=10, engine="openpyxl")
            print(f"  Shape: {df.shape}")
            print(f"  Columns ({len(df.columns)}):")
            for col in df.columns:
                print(f"    - {col}")
            print(f"\n  First few rows:")
            print(df.head(3).to_string())

    except Exception as e:
        print(f"  Error reading file: {e}")

print(f"\n{'=' * 80}")
print("Exploration complete")
print('=' * 80)
