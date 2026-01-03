# NHS Provider TAC (TRU) Open Dataset — 2017–18 to 2023–24

This repo turns the NHS provider TRU “TAC” Excel workbooks into a single, queryable dataset (DuckDB + Parquet) suitable for reproducible analysis and publication.

## What’s in here

**Source files:** TRU TAC workbooks for:
- NHS Trusts (`TAC_Trusts_YYYY-YY.xlsx`)
- NHS Foundation Trusts (`TAC_FTs_YYYY-YY.xlsx`)
- Years: 2017–18 to 2023–24 (7 years)

**Canonical output:**
- `Data/canonical/fact_tru_tac.parquet`
- `Data/canonical/tru_tac.duckdb` (contains table `fact_tru_tac`)

The dataset is long-form at the grain:
**(financial year, sector, organisation name, TableID, MainCode, SubCode, RowNumber)** → amount

## What transformations are applied

Only:
- Extract the “All data” tab (or its equivalent; the script matches this robustly).
- Standardise the organisation and value columns to:
  - `org_name_raw` (organisation name as reported)
  - `amount` (numeric)
- Add metadata fields:
  - `fy`, `sector`, `source_file`, `schema_version`

No:
- Reclassification of lines (e.g. “digital”, “agency”, “estates”)
- Sign convention changes (income/expenditure are left as-is)
- Adjustments, imputations, or “cleaning” beyond numeric coercion on `amount`

## Folder structure

- `Data/raw/` — original Excel files (not modified)
- `Data/canonical/` — generated outputs (Parquet + DuckDB)
- `src/` — build scripts and example queries
- `outputs/` — generated extracts for analysis (CSV)
- `mappings/` — optional mapping tables (line labels, categories, etc.)

## Reproduce the dataset

### Requirements
Python 3.10+ recommended.

Install dependencies:
```bash
pip install pandas openpyxl duckdb pyarrow
