import re
from pathlib import Path

import duckdb
import pandas as pd

RAW_DIR = Path("Data/raw")
OUT_PARQUET = Path("Data/canonical/fact_tru_tac.parquet")
OUT_DUCKDB = Path("Data/canonical/tru_tac.duckdb")


def parse_metadata(filename: str):
    m = re.match(r"TAC_(Trusts|FTs)_(\d{4}-\d{2})\.xlsx$", filename)
    if not m:
        raise ValueError(f"Unexpected filename format: {filename}")
    sector = "Trust" if m.group(1) == "Trusts" else "FT"
    fy = m.group(2)
    return sector, fy


def load_all_data(xlsx_path: Path) -> pd.DataFrame:
    # Find the "All data" sheet robustly (case/spacing differences across years)
    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    sheets = xls.sheet_names

    def norm_sheet(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())

    target = norm_sheet("All data")
    all_data_sheet = next((s for s in sheets if norm_sheet(s) == target), None)

    if all_data_sheet is None:
        raise ValueError(
            f"{xlsx_path.name}: could not find an 'All data' sheet. "
            f"Available sheets: {sheets}"
        )

    df = pd.read_excel(xlsx_path, sheet_name=all_data_sheet, engine="openpyxl")

    # Normalise column names for matching
    def norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())

    col_lookup = {norm(c): c for c in df.columns}

    # Stable keys (normalised)
    stable_required_norm = ["worksheetname", "tableid", "maincode", "rownumber", "subcode"]
    stable_cols = {}
    missing_stable = []

    for k in stable_required_norm:
        if k in col_lookup:
            stable_cols[k] = col_lookup[k]
        else:
            missing_stable.append(k)

    if missing_stable:
        raise ValueError(
            f"{xlsx_path.name}: missing expected stable columns (normalised): {missing_stable}\n"
            f"Detected columns: {list(df.columns)}"
        )

    # Detect organisation column (prefer specific names)
    org_candidates = ["organisationname", "orgname", "providername", "organisation"]
    org_col = next((col_lookup[norm(c)] for c in org_candidates if norm(c) in col_lookup), None)

    # Detect amount column (prefer numeric-specific names)
    amount_candidates = ["valuenumber", "total", "amount", "valuenumeric", "value"]
    amount_col = next((col_lookup[norm(c)] for c in amount_candidates if norm(c) in col_lookup), None)

    if org_col is None or amount_col is None:
        raise ValueError(
            f"{xlsx_path.name}: could not detect org/value columns.\n"
            f"Detected columns: {list(df.columns)}"
        )

    # Standardise to canonical names
    df = df.rename(columns={
        org_col: "org_name_raw",
        amount_col: "amount",
        stable_cols["worksheetname"]: "WorkSheetName",
        stable_cols["tableid"]: "TableID",
        stable_cols["maincode"]: "MainCode",
        stable_cols["rownumber"]: "RowNumber",
        stable_cols["subcode"]: "SubCode",
    })

    required = ["org_name_raw", "WorkSheetName", "TableID", "MainCode", "RowNumber", "SubCode", "amount"]
    df = df[required].copy()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    return df


def main():
    files = sorted(p for p in RAW_DIR.glob("TAC_*.xlsx") if p.is_file())
    if not files:
        raise ValueError(f"No TAC_*.xlsx files found in {RAW_DIR.resolve()}")

    all_frames = []

    for xlsx_path in files:
        sector, fy = parse_metadata(xlsx_path.name)
        df = load_all_data(xlsx_path)

        df["fy"] = fy
        df["sector"] = sector
        df["source_file"] = xlsx_path.name
        df["schema_version"] = fy

        all_frames.append(df)
        print(f"Loaded {xlsx_path.name}: {len(df):,} rows")

    fact = pd.concat(all_frames, ignore_index=True)

    qc = fact.groupby(["fy", "sector"])["amount"].agg(["count", "sum"]).reset_index()
    print("\nQC summary (rows + total amount by FY/sector):")
    print(qc.to_string(index=False))

    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    fact.to_parquet(OUT_PARQUET, index=False)

    con = duckdb.connect(str(OUT_DUCKDB))
    con.execute("CREATE OR REPLACE TABLE fact_tru_tac AS SELECT * FROM read_parquet(?)", [str(OUT_PARQUET)])
    con.close()

    print(f"\nWrote: {OUT_PARQUET}")
    print(f"Wrote: {OUT_DUCKDB}")


if __name__ == "__main__":
    main()
