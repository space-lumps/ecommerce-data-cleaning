"""
Audit all columns in clean Olist tables.

Output:
- reports/clean_dtypes_full.csv (one row per file+column)
- reports/clean_dtypes_flags.csv (only suspicious columns)

Heuristics flag columns that *look* mis-typed:
- *_id or *zip* should be str (not numeric)
- *_date / *timestamp* should be datetime (not object/str)
- numeric-like columns stored as object/str
"""

from __future__ import annotations

from pathlib import Path
import re
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
CLEAN = REPO_ROOT / "data" / "clean"
OUT = REPO_ROOT / "reports"
OUT.mkdir(parents=True, exist_ok=True)


ID_RE = re.compile(r".*_id$|^order_item_id$|.*zip.*|.*postal.*", re.IGNORECASE)
DT_RE = re.compile(r".*timestamp.*|.*_date$|.*approved_at$|.*delivered_.*|.*estimated_.*", re.IGNORECASE)

# Common numeric patterns in Olist; expand if needed
NUM_RE = re.compile(
    r"(^price$|^freight_value$|^payment_value$|^payment_installments$|^review_score$|"
    r".*weight.*|.*length.*|.*height.*|.*width.*|.*photos.*|.*lat.*|.*lng.*|.*longitude.*|.*latitude.*)",
    re.IGNORECASE,
)


def dtype_family(dtype) -> str:
    s = str(dtype)
    if s == "str":
        return "str"
    if s.startswith("datetime64"):
        return "datetime"
    if s in ("int64", "float64", "Int64", "Float64", "int32", "float32"):
        return "numeric"
    if s == "object":
        return "object"
    return s


def sample_non_null(df: pd.DataFrame, col: str, n: int = 3) -> str:
    ser = df[col].dropna()
    if ser.empty:
        return ""
    vals = ser.head(n).astype(str).tolist()
    return "|".join(vals)


def main() -> None:
    files = sorted(CLEAN.glob("*.parquet"))
    if not files:
        raise SystemExit(f"No clean parquet files found in {CLEAN}")

    full_rows = []
    flag_rows = []

    for p in files:
        df = pd.read_parquet(p)
        for col in df.columns:
            fam = dtype_family(df[col].dtype)
            sample = sample_non_null(df, col)

            # Heuristic expectations
            exp = None
            if ID_RE.match(col):
                exp = "str"
            elif DT_RE.match(col):
                exp = "datetime"
            elif NUM_RE.match(col):
                exp = "numeric"

            flag = False
            reason = ""

            if exp == "str" and fam != "str":
                flag = True
                reason = "expected str (id/zip-like)"
            elif exp == "datetime" and fam != "datetime":
                flag = True
                reason = "expected datetime (date-like)"
            elif exp == "numeric" and fam not in ("numeric",):
                # Allow numeric only; object/str is suspicious
                if fam in ("object", "str"):
                    flag = True
                    reason = "expected numeric (measure-like)"

            row = {
                "file": p.name,
                "column": col,
                "dtype": str(df[col].dtype),
                "dtype_family": fam,
                "expected_family_heuristic": exp or "",
                "sample_values": sample,
                "flagged": flag,
                "flag_reason": reason,
            }
            full_rows.append(row)
            if flag:
                flag_rows.append(row)

    full_path = OUT / "clean_dtypes_full.csv"
    flags_path = OUT / "clean_dtypes_flags.csv"

    pd.DataFrame(full_rows).to_csv(full_path, index=False)
    pd.DataFrame(flag_rows).to_csv(flags_path, index=False)

    print(f"Wrote {full_path} (rows={len(full_rows)})")
    print(f"Wrote {flags_path} (flags={len(flag_rows)})")


if __name__ == "__main__":
    main()