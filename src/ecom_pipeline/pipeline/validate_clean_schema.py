"""
Validate clean Olist tables against the declarative schema contract.

Input:
- data/clean/*.parquet

Output:
- reports/clean_schema_audit.csv

Purpose:
- Confirm that enforce_schema.py produced the exact structure defined in SCHEMA_CONTRACT
- Check logical dtype families (string / numeric / datetime)
- Verify required columns are present
- Support future extension to primary key and foreign key checks
"""

from typing import Any

import pandas as pd

from ecom_pipeline.config.schema_contract import SCHEMA_CONTRACT
from ecom_pipeline.utils.io import (
    clean_dir,
    read_parquet,
    reports_dir,
    write_csv,
)
from ecom_pipeline.utils.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

_CLEAN = clean_dir()
_OUT = reports_dir()


def get_dtype_family(dtype) -> str:
    """Map pandas dtype to logical dtype_family as defined in SCHEMA_CONTRACT.

    Returns exactly 'string', 'numeric', or 'datetime'.
    This is intentionally strict for validation purposes.
    """
    dtype_str = str(dtype).lower()

    # Prioritize exact nullable string dtype
    if dtype_str == "string":
        return "string"

    # Legacy object is NOT accepted for string columns in final clean data
    if dtype_str == "object":
        return "object"  # <-- this will cause failure if expected is "string"

    if dtype_str.startswith("datetime64"):
        return "datetime"

    if "int" in dtype_str or "float" in dtype_str:
        return "numeric"

    return dtype_str


def main() -> None:
    rows: list[dict[str, Any]] = []

    for filename, contract in SCHEMA_CONTRACT.items():
        path = _CLEAN / filename

        if not path.exists():
            logger.error("Missing clean file: %s", path)
            rows.append({"file": filename, "status": "missing_clean_file"})
            continue

        logger.info("Validating schema for %s", filename)
        df = read_parquet(path)

        # Check every column defined in the contract
        for col, rules in contract.get("columns", {}).items():
            expected_family = rules["dtype_family"]
            nullable = rules.get("nullable", True)

            if col not in df.columns:
                rows.append(
                    {
                        "file": filename,
                        "column": col,
                        "expected": expected_family,
                        "actual": "missing",
                        "pass": False,
                    }
                )
                continue

            actual_family = get_dtype_family(df[col].dtype)

            # For string columns we also enforce nullable 'string' dtype (not object)
            if expected_family == "string":
                is_correct = str(df[col].dtype) == "string"
            else:
                is_correct = actual_family == expected_family

            rows.append(
                {
                    "file": filename,
                    "column": col,
                    "expected": expected_family,
                    "actual": actual_family,
                    "nullable_expected": nullable,
                    "actual_has_nulls": df[col].isna().any(),
                    "pass": is_correct,
                }
            )

    # Write audit report
    audit_df = pd.DataFrame(rows)
    out_path = _OUT / "clean_schema_audit.csv"
    write_csv(audit_df, out_path)

    fails = sum(1 for r in rows if r.get("pass") is False)
    logger.info("Schema validation complete → %s (fails=%s)", out_path, fails)


if __name__ == "__main__":
    main()
