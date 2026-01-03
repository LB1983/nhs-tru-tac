import re
from pathlib import Path
import pandas as pd

RAW_DIR = Path("Data/raw")
OUT_DIR = Path("mappings")
OUT_DIR.mkdir(exist_ok=True)

def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).strip().lower())

def find_mapping_sheet(xlsx_path: Path) -> str:
    xls = pd.ExcelFile(xlsx_path, engine="openpyxl")
    sheets = xls.sheet_names

    keywords = [
        "mapping",
        "schedule",
        "lookup",
        "reference",
        "code",
    ]
    kw = [norm(k) for k in keywords]

    scored = []
    for s in sheets:
        ns = norm(s)
        score = sum(1 for k in kw if k in ns)
        if score > 0:
            scored.append((score, s))

    if not scored:
        raise ValueError(
            f"{xlsx_path.name}: could not find mapping schedule sheet. "
            f"Available sheets: {sheets}"
        )

    scored.sort(reverse=True)
    return scored[0][1]

def standardise_mapping_columns(df: pd.DataFrame, source: str) -> pd.DataFrame:
    cols = {norm(c): c for c in df.columns}

    def pick(candidates):
        for c in candidates:
            k = norm(c)
            if k in cols:
                return cols[k]
        return None

    table_col = pick(["tableid", "table", "table_id"])
    main_col  = pick(["maincode", "main code", "main_code"])
    sub_col   = pick(["subcode", "sub code", "sub_code"])
    row_col   = pick(["rownumber", "row number", "row", "rowno", "row_no"])
    label_col = pick([
        "description",
        "line description",
        "row description",
        "narrative",
        "label",
        "name"
    ])

    missing = [
        name for name, col in {
            "TableID": table_col,
            "MainCode": main_col,
            "SubCode": sub_col,
            "RowNumber": row_col,
        }.items() if col is None
    ]

    if missing:
        raise ValueError(
            f"{source}: mapping schedule missing required columns {missing}. "
            f"Detected columns: {list(df.columns)}"
        )

    out = df[[table_col, main_col, sub_col, row_col]].rename(columns={
        table_col: "TableID",
        main_col: "MainCode",
        sub_col: "SubCode",
        row_col: "RowNumber",
    }).copy()

    if label_col:
        out["line_label"] = df[label_col].astype(str)
    else:
        out["line_label"] = ""

    out["RowNumber"] = pd.to_numeric(out["RowNumber"], errors="coerce")
    out = out.dropna(subset=["RowNumber"])
    out["RowNumber"] = out["RowNumber"].astype(int)

    return out

def parse_fy(filename: str) -> str:
    m = re.match(r"TAC_FTs_(\d{4}-\d{2})\.xlsx$", filename)
    return m.group(1) if m else "unknown"

def main():
    frames = []

    for xlsx_path in sorted(RAW_DIR.glob("TAC_FTs_*.xlsx")):
        fy = parse_fy(xlsx_path.name)
        sheet = find_mapping_sheet(xlsx_path)
        df = pd.read_excel(xlsx_path, sheet_name=sheet, engine="openpyxl")

        df_std = standardise_mapping_columns(df, xlsx_path.name)
        df_std["fy_source"] = fy
        df_std["source_file"] = xlsx_path.name
        df_std["source_sheet"] = sheet

        frames.append(df_std)
        print(f"Extracted mapping: {xlsx_path.name} | sheet='{sheet}' | rows={len(df_std):,}")

    all_map = pd.concat(frames, ignore_index=True)

    def fy_sort_key(fy):
        m = re.match(r"(\d{4})-\d{2}", str(fy))
        return int(m.group(1)) if m else 0

    all_map["fy_sort"] = all_map["fy_source"].apply(fy_sort_key)
    all_map = all_map.sort_values(["TableID", "MainCode", "SubCode", "RowNumber", "fy_sort"])

    dim = all_map.groupby(
        ["TableID", "MainCode", "SubCode", "RowNumber"],
        as_index=False
    ).tail(1)

    dim["category_1"] = ""
    dim["category_2"] = ""
    dim["is_digital_data_it"] = ""
    dim["notes"] = ""

    out_file = OUT_DIR / "dim_tac_lines.csv"
    dim = dim.drop(columns=["fy_sort"])
    dim.to_csv(out_file, index=False)

    print(f"\nWrote {out_file} ({len(dim):,} unique lines)")

if __name__ == "__main__":
    main()
