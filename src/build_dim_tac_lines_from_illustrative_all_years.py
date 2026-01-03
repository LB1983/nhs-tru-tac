import re
from pathlib import Path
import pandas as pd

REF_DIR = Path("Data/reference")
OUT_FILE = Path("mappings/dim_tac_lines_by_year.csv")
OUT_FILE.parent.mkdir(exist_ok=True)

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())

def pick_col(df, candidates):
    cols = {norm(c): c for c in df.columns}
    for c in candidates:
        k = norm(c)
        if k in cols:
            return cols[k]
    return None

def extract_from_sheet(df: pd.DataFrame, sheet: str, source_file: str, fy: str):
    table_col = pick_col(df, ["tableid", "table", "table_id"])
    main_col  = pick_col(df, ["maincode", "main code", "main_code"])
    sub_col   = pick_col(df, ["subcode", "sub code", "sub_code"])
    row_col   = pick_col(df, ["rownumber", "row number", "row", "rowno", "row_no"])

    if not all([table_col, main_col, sub_col, row_col]):
        return None

    label_col = pick_col(df, [
        "description",
        "line description",
        "row description",
        "narrative",
        "label",
        "name"
    ])

    out = df[[table_col, main_col, sub_col, row_col]].rename(columns={
        table_col: "TableID",
        main_col: "MainCode",
        sub_col: "SubCode",
        row_col: "RowNumber",
    }).copy()

    out["RowNumber"] = pd.to_numeric(out["RowNumber"], errors="coerce")
    out = out.dropna(subset=["RowNumber"])
    out["RowNumber"] = out["RowNumber"].astype(int)

    out["line_label"] = df[label_col].astype(str) if label_col else ""
    out["fy"] = fy
    out["source_file"] = source_file
    out["source_sheet"] = sheet

    return out

def infer_fy_from_filename(name: str) -> str:
    # examples:
    # 2020-21-NHS-Provider-TAC-Illustrative-file.xlsx
    # 202223-NHS-Provider-TAC-Illustrative-File.xlsx
    m = re.search(r"(20\d{2})[- ]?(?:to)?[- ]?(20\d{2})", name)
    if m:
        return f"{m.group(1)}-{m.group(2)[-2:]}"
    m = re.search(r"(20\d{2})(\d{2})", name)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return "unknown"

def main():
    frames = []

    for path in sorted(REF_DIR.glob("*.xlsx")):
        fy = infer_fy_from_filename(path.name)
        print(f"\nProcessing {path.name} (fy={fy})")

        xls = pd.ExcelFile(path, engine="openpyxl")
        for sheet in xls.sheet_names:
            df = pd.read_excel(path, sheet_name=sheet, engine="openpyxl")
            extracted = extract_from_sheet(df, sheet, path.name, fy)
            if extracted is not None and len(extracted) > 0:
                frames.append(extracted)
                print(f"  ✓ {sheet}: {len(extracted):,} rows")

    if not frames:
        raise ValueError("No mapping sheets found in illustrative files")

    all_lines = pd.concat(frames, ignore_index=True)

    dim = (
        all_lines
        .sort_values(["fy", "TableID", "MainCode", "SubCode", "RowNumber"])
        .drop_duplicates(["fy", "TableID", "MainCode", "SubCode", "RowNumber"])
    )

    # enrichment placeholders
    dim["category_1"] = ""
    dim["category_2"] = ""
    dim["is_digital_data_it"] = ""
    dim["notes"] = ""

    dim.to_csv(OUT_FILE, index=False)
    print(f"\nWrote {OUT_FILE} ({len(dim):,} rows)")

if __name__ == "__main__":
    main()
