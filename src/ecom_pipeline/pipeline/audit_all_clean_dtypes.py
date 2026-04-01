"""
Audit all columns in the cleaned Olist tables.

Production step that provides detailed per-column statistics and flags potential issues.
Uses SCHEMA_CONTRACT as primary source for expected type when available.
"""

from __future__ import annotations

import re

import pandas as pd

from ecom_pipeline.config.schema_contract import SCHEMA_CONTRACT
from ecom_pipeline.utils.io import clean_dir, reports_dir, write_csv
from ecom_pipeline.utils.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

_CLEAN = clean_dir()
_OUT = reports_dir()

# Heuristic patterns (used only as fallback)
ID_RE = re.compile(r".*_id$", re.IGNORECASE)
ZIP_RE = re.compile(r".*zip.*|.*postal.*", re.IGNORECASE)
DT_RE = re.compile(
    r".*timestamp.*|.*_date$|.*approved_at$|.*delivered_.*|.*estimated_.*",
    re.IGNORECASE,
)
NUM_RE = re.compile(
    r"(^price$|^freight_value$|^payment_value$|^payment_installments$|^review_score$|"
    r".*weight.*|.*length.*|.*height.*|.*width.*|.*photos.*|.*qty$|.*count$)",
    re.IGNORECASE,
)


def get_dtype_family(dtype) -> str:
    """Return logical family name for reporting."""
    s = str(dtype).lower()
    if "string" in s or s == "object":
        return "string"
    if s.startswith("datetime64"):
        return "datetime"
    if "int" in s:
        return "int"
    if "float" in s:
        return "float"
    return s


def main() -> None:
    files = sorted(_CLEAN.glob("*.parquet"))
    if not files:
        raise SystemExit(f"No clean parquet files found in {_CLEAN}")

    full_rows = []
    flag_rows = []

    for p in files:
        logger.info("Auditing dtypes for %s", p.name)

        df = pd.read_parquet(p)
        contract = SCHEMA_CONTRACT.get(p.name, {})

        for col in df.columns:
            total_rows = len(df)
            null_count = int(df[col].isna().sum())
            null_pct = round((null_count / total_rows) * 100, 4) if total_rows else 0.0

            fam = get_dtype_family(df[col].dtype)
            sample = sample_non_null(df, col)

            # Primary: Use SCHEMA_CONTRACT if column is defined there
            exp = None
            if col in contract.get("columns", {}):
                exp = contract["columns"][col].get("dtype_family")
            # Fallback heuristics for derived columns
            elif col == "order_item_id":
                exp = "numeric"
            elif ID_RE.match(col):
                exp = "string"
            elif ZIP_RE.match(col):
                exp = "string"
            elif DT_RE.match(col):
                exp = "datetime"
            elif NUM_RE.match(col):
                exp = "numeric"
            elif col in (
                "customer_state_name",
                "customer_city",
                "seller_city",
                "geolocation_city",
            ):
                exp = "string"
            elif any(
                term in col.lower() for term in ["lat", "lng", "latitude", "longitude"]
            ):
                exp = "numeric"

            flag = False
            reason = ""

            if exp and fam != exp:
                # Numeric columns can be either "int" or "float" in practice
                if exp == "numeric" and fam in ("int", "float"):
                    pass  # This is acceptable
                else:
                    flag = True
                    reason = f"expected {exp}, got {fam}"

            row = {
                "file": p.name,
                "column": col,
                "dtype": str(df[col].dtype),
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

    # Write reports
    full_path = _OUT / "clean_dtypes_full.csv"
    flags_path = _OUT / "clean_dtypes_flags.csv"

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


def sample_non_null(df: pd.DataFrame, col: str, n: int = 3) -> str:
    ser = df[col].dropna()
    if ser.empty:
        return ""
    vals = ser.head(n).astype(str).tolist()
    return "|".join(vals)


if __name__ == "__main__":
    main()
