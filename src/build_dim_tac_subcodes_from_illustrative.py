import re
from pathlib import Path
import pandas as pd

REF_DIR = Path("Data/reference")
OUT_FILE = Path("mappings/dim_tac_subcodes_by_year.csv")
OUT_FILE.parent.mkdir(exist_ok=True)

MAINCODE_COL_RE = re.compile(r"^A\d{2}[A-Z]{2}\d{2}[A-Z]?$")   # e.g. A21CY01, A17PY03A

def infer_fy(filename: str) -> str:
    name = filename

    # 2023-24 style
    m = re.search(r"(20\d{2})-(\d{2})", name)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    # 202223 style
    m = re.search(r"(20\d{2})(\d{2})", name)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    # 1920 style (your 2019-20 file)
    if "1920" in name:
        return "2019-20"

    return "unknown"

def find_table_id(df_raw: pd.DataFrame):
    # Look for a cell containing "Table ID" in first ~60 rows
    for r in range(min(60, len(df_raw))):
        row = df_raw.iloc[r].astype(str).tolist()
        for c, v in enumerate(row):
            if isinstance(v, str) and "table id" in v.lower():
                if c + 1 < df_raw.shape[1]:
                    val = df_raw.iat[r, c+1]
                    try:
                        return int(float(val))
                    except:
                        return val
    return None


def find_header_row_and_subcode_col(df_raw: pd.DataFrame):
    """
    Find the row that looks like the start of a table by detecting
    a concentration of SubCode-like values in rows below.
    """
    for r in range(min(40, len(df_raw))):
        # Look ahead 10 rows to see if a column contains many subcodes
        for c in range(df_raw.shape[1]):
            values = []
            for rr in range(r+1, min(r+15, len(df_raw))):
                v = df_raw.iat[rr, c]
                if isinstance(v, str):
                    values.append(v.strip())

            # If we see several SubCode-looking values, assume this is the SubCode column
            subcode_hits = sum(
                1 for v in values
                if re.match(r"^[A-Z]{2,5}\d{3,4}[A-Z]?$", v)
            )

            if subcode_hits >= 3:
                return r, c

    return None, None


def extract_sheet_subcodes(xlsx_path: Path, sheet: str, fy: str):
    df_raw = pd.read_excel(xlsx_path, sheet_name=sheet, engine="openpyxl", header=None)

    hdr_row, subcode_col = find_header_row_and_subcode_col(df_raw)
    if hdr_row is None:
        return None

    records = []

    for r in range(hdr_row + 1, len(df_raw)):
        sub = df_raw.iat[r, subcode_col]

        if not isinstance(sub, str):
            continue

        sub = sub.strip()
        if not re.match(r"^[A-Z]{2,5}\d{3,4}[A-Z]?$", sub):
            continue

        # Label: first non-empty text cell to the left
        label = ""
        for c in range(subcode_col):
            v = df_raw.iat[r, c]
            if isinstance(v, str) and v.strip():
                label = v.strip()
                break

        records.append({
            "fy": fy,
            "WorkSheetName": sheet,
            "SubCode": sub,
            "subcode_label": label,
            "source_file": xlsx_path.name,
        })

    if not records:
        return None

    return pd.DataFrame(records)

def main():
    frames = []

    for xlsx_path in sorted(REF_DIR.glob("*.xlsx")):
        fy = infer_fy(xlsx_path.name)
        xls = pd.ExcelFile(xlsx_path, engine="openpyxl")

        print(f"\nProcessing {xlsx_path.name} (fy={fy})")

        for sheet in xls.sheet_names:
            if not str(sheet).strip().lower().startswith("tac"):
                continue

            df = extract_sheet_subcodes(xlsx_path, sheet, fy)
            if df is not None and len(df) > 0:
                frames.append(df)
                print(f"  ✓ {sheet}: {len(df):,} subcodes")

    if not frames:
        raise ValueError("No subcode mappings extracted. (Header detection likely needs tweaking.)")

    dim = pd.concat(frames, ignore_index=True)

    # Deduplicate on the real key for mapping
    dim = (dim
       .sort_values(["fy", "WorkSheetName", "SubCode"])
       .drop_duplicates(["fy", "WorkSheetName", "SubCode"], keep="last"))

    dim.to_csv(OUT_FILE, index=False)
    print(f"\nWrote {OUT_FILE} ({len(dim):,} rows)")

if __name__ == "__main__":
    main()
