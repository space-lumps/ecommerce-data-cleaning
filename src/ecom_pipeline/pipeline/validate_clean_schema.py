"""
Validate clean Olist tables against schema expectations.

Input:
- data/clean/*.parquet

Output:
- reports/clean_schema_audit.csv

Checks:
- string_cols are pandas nullable string dtype
- datetime_cols are datetime64 dtype
- integer_cols are in the integer family
- float_cols are in the float family

Purpose: Confirm enforce_schema.py applied the correct nullable types.
"""

from typing import Any

import pandas as pd

from ecom_pipeline.pipeline.enforce_schema import CAST_RULES
from ecom_pipeline.utils.io import (
    clean_dir,
    read_parquet,
    repo_root,
    reports_dir,
    write_csv,
)
from ecom_pipeline.utils.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

REPO_ROOT = repo_root()
CLEAN = clean_dir()
OUT = reports_dir()


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


def main() -> None:
    rows: list[dict[str, Any]] = []

    for filename, rules in CAST_RULES.items():
        path = CLEAN / filename

        if not path.exists():
            logger.error("Missing clean file: %s", path)
            rows.append({"file": filename, "status": "missing_clean_file"})
            continue

        logger.info("Validating %s", filename)
        df = read_parquet(path)

        # string cols
        for col in rules.get("string_cols", []):
            if col not in df.columns:
                rows.append(
                    {
                        "file": filename,
                        "column": col,
                        "expected": "string",
                        "actual": "missing",
                        "pass": False,
                    }
                )
                continue
            actual = dtype_family(df[col].dtype)
            rows.append(
                {
                    "file": filename,
                    "column": col,
                    "expected": "string",
                    "actual": actual,
                    "pass": actual == "string",
                }
            )

        # datetime cols
        for col in rules.get("datetime_cols", []):
            if col not in df.columns:
                rows.append(
                    {
                        "file": filename,
                        "column": col,
                        "expected": "datetime",
                        "actual": "missing",
                        "pass": False,
                    }
                )
                continue
            actual = dtype_family(df[col].dtype)
            rows.append(
                {
                    "file": filename,
                    "column": col,
                    "expected": "datetime",
                    "actual": actual,
                    "pass": actual == "datetime",
                }
            )

        # integer cols
        for col in rules.get("integer_cols", []):
            if col not in df.columns:
                rows.append(
                    {
                        "file": filename,
                        "column": col,
                        "expected": "int",
                        "actual": "missing",
                        "pass": False,
                    }
                )
                continue
            actual = dtype_family(df[col].dtype)
            rows.append(
                {
                    "file": filename,
                    "column": col,
                    "expected": "int",
                    "actual": actual,
                    "pass": actual == "int",
                }
            )

        # float cols
        for col in rules.get("float_cols", []):
            if col not in df.columns:
                rows.append(
                    {
                        "file": filename,
                        "column": col,
                        "expected": "float",
                        "actual": "missing",
                        "pass": False,
                    }
                )
                continue
            actual = dtype_family(df[col].dtype)
            rows.append(
                {
                    "file": filename,
                    "column": col,
                    "expected": "float",
                    "actual": actual,
                    "pass": actual == "float",
                }
            )

    out_path = OUT / "clean_schema_audit.csv"
    write_csv(pd.DataFrame(rows), out_path)

    fails = sum(1 for r in rows if r.get("pass") is False)
    logger.info("Wrote %s (fails=%s)", out_path, fails)


if __name__ == "__main__":
    main()
