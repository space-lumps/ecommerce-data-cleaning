"""
Audit all columns in the cleaned Olist tables.

Output:
- reports/clean_dtypes_full.csv (one row per file+column)
- reports/clean_dtypes_flags.csv (only suspicious columns)

Notes:
- Uses the final cleaned dtypes from enforce_schema.py (nullable Int64/Float64/string)
- dtype column shows the cleaned display version (always "string" for string columns)
- dtype_family groups types into simple families for validation and reporting
"""

from __future__ import annotations

import re

import pandas as pd

from ecom_pipeline.utils.io import clean_dir, repo_root, reports_dir, write_csv
from ecom_pipeline.utils.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

REPO_ROOT = repo_root()
CLEAN = clean_dir()
OUT = reports_dir()

ID_RE = re.compile(r".*_id$|.*zip.*|.*postal.*", re.IGNORECASE)
DT_RE = re.compile(
    r".*timestamp.*|.*_date$|.*approved_at$|.*delivered_.*|.*estimated_.*",
    re.IGNORECASE,
)

NUM_RE = re.compile(
    r"(^price$|^freight_value$|^payment_value$|^payment_installments$|^review_score$|"
    r".*weight.*|.*length.*|.*height.*|.*width.*|.*photos.*|.*lat.*|.*lng.*|.*longitude.*|.*latitude.*)",
    re.IGNORECASE,
)


def dtype_family(dtype) -> str:
    """Return simple family name consistently."""
    s = str(dtype).lower()

    if s in {"string", "str"} or "string" in s or s == "object":
        return "string"
    if s.startswith("datetime64"):
        return "datetime"
    if "int" in s:
        return "int"
    if "float" in s:
        return "float"

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
        logger.info("Auditing %s", p.name)

        df = pd.read_parquet(p)

        for col in df.columns:
            total_rows = len(df)
            null_count = int(df[col].isna().sum())
            null_pct = round((null_count / total_rows) * 100, 4) if total_rows else 0.0

            fam = dtype_family(df[col].dtype)
            sample = sample_non_null(df, col)

            # Clean dtype for consistent display in reports
            cleaned_dtype = str(df[col].dtype)
            if cleaned_dtype.startswith("string") or cleaned_dtype == "str":
                cleaned_dtype = "string"

            # Heuristic expectations
            exp = None
            if col == "order_item_id":
                exp = "numeric"
            elif ID_RE.match(col):
                exp = "string"
            elif DT_RE.match(col):
                exp = "datetime"
            elif NUM_RE.match(col):
                exp = "numeric"

            flag = False
            reason = ""

            if exp == "string" and fam != "string":
                flag = True
                reason = "expected string (id/zip-like)"
            elif exp == "datetime" and fam != "datetime":
                flag = True
                reason = "expected datetime (date-like)"
            elif exp == "numeric" and fam not in ("int", "float"):
                if fam in ("object", "string"):
                    flag = True
                    reason = "expected numeric (measure-like)"

            row = {
                "file": p.name,
                "column": col,
                "dtype": cleaned_dtype,
                "dtype_family": fam,
                "expected_family_heuristic": exp or "",
                "sample_values": sample,
                "null_count": null_count,
                "null_pct": null_pct,
                "flagged": flag,
                "flag_reason": reason,
            }
            full_rows.append(row)
            if flag:
                flag_rows.append(row)

    full_path = OUT / "clean_dtypes_full.csv"
    flags_path = OUT / "clean_dtypes_flags.csv"

    columns = [
        "file",
        "column",
        "dtype",
        "dtype_family",
        "expected_family_heuristic",
        "sample_values",
        "null_count",
        "null_pct",
        "flagged",
        "flag_reason",
    ]

    write_csv(pd.DataFrame(full_rows, columns=columns), full_path)
    write_csv(pd.DataFrame(flag_rows, columns=columns), flags_path)

    logger.info("Wrote %s (rows=%s)", full_path, len(full_rows))
    logger.info("Wrote %s (flags=%s)", flags_path, len(flag_rows))


if __name__ == "__main__":
    main()
