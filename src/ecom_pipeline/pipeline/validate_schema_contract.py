"""
Validate clean parquet tables against SCHEMA_CONTRACT.

Checks (fail-fast via nonzero fail count):
- required columns exist
- dtype_family matches (str/numeric/datetime)
- non-null constraints
- primary key uniqueness (if defined)
- foreign key integrity (if defined)

Output:
- reports/clean_contract_audit.csv
"""

from __future__ import annotations

import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
    is_string_dtype,
)

from ecom_pipeline.utils.io import repo_root, read_parquet, write_csv
from ecom_pipeline.utils.logging import configure_logging, get_logger
from ecom_pipeline.config.schema_contract import SCHEMA_CONTRACT


configure_logging()
logger = get_logger(__name__)

REPO_ROOT = repo_root()
CLEAN = REPO_ROOT / "data" / "clean"
OUT = REPO_ROOT / "reports"


def dtype_family(series: pd.Series) -> str:
    """
    Map pandas dtype to a logical family.

    Notes:
    - 'str' accepts pandas string dtype and object dtype (object is common when reading parquet/csv).
      If you want to enforce strictly pandas string dtype later, add a stricter rule.
    """
    if is_datetime64_any_dtype(series.dtype):
        return "datetime"
    if is_numeric_dtype(series.dtype):
        return "numeric"
    if is_string_dtype(series.dtype) or is_object_dtype(series.dtype):
        return "str"
    return str(series.dtype)


def load_clean_table(filename: str) -> pd.DataFrame:
    path = CLEAN / filename
    if not path.exists():
        raise FileNotFoundError(f"Missing clean file: {path}")
    return read_parquet(path)


def main() -> None:
    rows: list[dict] = []
    fails = 0

    # Cache referenced tables for FK checks
    table_cache: dict[str, pd.DataFrame] = {}

    def get_table(table_name: str) -> pd.DataFrame:
        if table_name not in table_cache:
            table_cache[table_name] = load_clean_table(table_name)
        return table_cache[table_name]

    for table_name, spec in SCHEMA_CONTRACT.items():
        logger.info("Contract validating %s", table_name)

        # ---- load table ----
        try:
            df = get_table(table_name)
        except Exception as exc:
            fails += 1
            rows.append(
                {
                    "table": table_name,
                    "check": "table_exists_and_readable",
                    "status": "fail",
                    "details": str(exc),
                }
            )
            continue

        # ---- required columns ----
        required_cols = spec.get("required_columns", [])
        missing_required = [c for c in required_cols if c not in df.columns]
        if missing_required:
            fails += 1
            rows.append(
                {
                    "table": table_name,
                    "check": "required_columns",
                    "status": "fail",
                    "details": f"missing={missing_required}",
                }
            )
            # still continue checks for existing columns
        else:
            rows.append(
                {
                    "table": table_name,
                    "check": "required_columns",
                    "status": "pass",
                    "details": "",
                }
            )

        # ---- per-column checks ----
        columns_spec = spec.get("columns", {})

        for col, col_spec in columns_spec.items():
            if col not in df.columns:
                # already captured by required_columns if required
                continue

            ser = df[col]
            expected_family = col_spec.get("dtype_family")
            actual_family = dtype_family(ser)

            if expected_family and actual_family != expected_family:
                fails += 1
                rows.append(
                    {
                        "table": table_name,
                        "check": "dtype_family",
                        "column": col,
                        "status": "fail",
                        "details": f"expected={expected_family} actual={actual_family} pandas_dtype={ser.dtype}",
                    }
                )
            else:
                rows.append(
                    {
                        "table": table_name,
                        "check": "dtype_family",
                        "column": col,
                        "status": "pass",
                        "details": f"expected={expected_family} actual={actual_family}",
                    }
                )

            # nullability
            nullable = col_spec.get("nullable", True)
            if nullable is False:
                null_count = int(ser.isna().sum())
                if null_count > 0:
                    fails += 1
                    rows.append(
                        {
                            "table": table_name,
                            "check": "non_null",
                            "column": col,
                            "status": "fail",
                            "details": f"null_count={null_count}",
                        }
                    )
                else:
                    rows.append(
                        {
                            "table": table_name,
                            "check": "non_null",
                            "column": col,
                            "status": "pass",
                            "details": "",
                        }
                    )

            # allowed values (for low-cardinality categorical columns)
            allowed_values = col_spec.get("allowed_values")
            if allowed_values is not None:
                bad = ser.dropna()
                bad = bad[~bad.isin(allowed_values)]
                bad_count = int(len(bad))
                if bad_count > 0:
                    sample = "|".join(bad.astype(str).head(5).tolist())
                    fails += 1
                    rows.append(
                        {
                            "table": table_name,
                            "check": "allowed_values",
                            "column": col,
                            "status": "fail",
                            "details": f"bad_count={bad_count} sample={sample}",
                        }
                    )
                else:
                    rows.append(
                        {
                            "table": table_name,
                            "check": "allowed_values",
                            "column": col,
                            "status": "pass",
                            "details": "",
                        }
                    )

            # min/max (numeric)
            if expected_family == "numeric":
                if "min" in col_spec:
                    min_val = col_spec["min"]
                    bad = ser.dropna() < min_val
                    bad_count = int(bad.sum())
                    if bad_count > 0:
                        fails += 1
                        rows.append(
                            {
                                "table": table_name,
                                "check": "min_value",
                                "column": col,
                                "status": "fail",
                                "details": f"min={min_val} bad_count={bad_count}",
                            }
                        )
                if "max" in col_spec:
                    max_val = col_spec["max"]
                    bad = ser.dropna() > max_val
                    bad_count = int(bad.sum())
                    if bad_count > 0:
                        fails += 1
                        rows.append(
                            {
                                "table": table_name,
                                "check": "max_value",
                                "column": col,
                                "status": "fail",
                                "details": f"max={max_val} bad_count={bad_count}",
                            }
                        )

        # ---- primary key uniqueness ----
        pk = spec.get("primary_key")
        if pk:
            if any(c not in df.columns for c in pk):
                fails += 1
                rows.append(
                    {
                        "table": table_name,
                        "check": "primary_key_columns_exist",
                        "status": "fail",
                        "details": f"pk={pk}",
                    }
                )
            else:
                dup_count = int(df.duplicated(subset=pk).sum())
                if dup_count > 0:
                    fails += 1
                    rows.append(
                        {
                            "table": table_name,
                            "check": "primary_key_unique",
                            "status": "fail",
                            "details": f"pk={pk} duplicate_rows={dup_count}",
                        }
                    )
                else:
                    rows.append(
                        {
                            "table": table_name,
                            "check": "primary_key_unique",
                            "status": "pass",
                            "details": f"pk={pk}",
                        }
                    )

        # ---- foreign key checks ----
        fks = spec.get("foreign_keys", [])
        for fk in fks:
            from_cols = fk["from_columns"]
            to_table = fk["to_table"]
            to_cols = fk["to_columns"]

            if any(c not in df.columns for c in from_cols):
                fails += 1
                rows.append(
                    {
                        "table": table_name,
                        "check": "foreign_key_columns_exist",
                        "status": "fail",
                        "details": f"from_cols={from_cols} to_table={to_table}",
                    }
                )
                continue

            try:
                df_to = get_table(to_table)
            except Exception as exc:
                fails += 1
                rows.append(
                    {
                        "table": table_name,
                        "check": "foreign_key_target_load",
                        "status": "fail",
                        "details": f"to_table={to_table} error={exc}",
                    }
                )
                continue

            if any(c not in df_to.columns for c in to_cols):
                fails += 1
                rows.append(
                    {
                        "table": table_name,
                        "check": "foreign_key_target_columns_exist",
                        "status": "fail",
                        "details": f"to_table={to_table} to_cols={to_cols}",
                    }
                )
                continue

            # Check orphans: non-null FK values not present in target PK columns
            left = df[from_cols].dropna().drop_duplicates()
            right = df_to[to_cols].dropna().drop_duplicates()

            merged = left.merge(right, how="left", left_on=from_cols, right_on=to_cols, indicator=True)
            orphan_count = int((merged["_merge"] == "left_only").sum())

            if orphan_count > 0:
                sample = merged.loc[merged["_merge"] == "left_only", from_cols].head(5).astype(str).to_dict("records")
                fails += 1
                rows.append(
                    {
                        "table": table_name,
                        "check": "foreign_key_integrity",
                        "status": "fail",
                        "details": f"from_cols={from_cols} to_table={to_table} orphan_count={orphan_count} sample={sample}",
                    }
                )
            else:
                rows.append(
                    {
                        "table": table_name,
                        "check": "foreign_key_integrity",
                        "status": "pass",
                        "details": f"from_cols={from_cols} to_table={to_table}",
                    }
                )

    out_path = OUT / "clean_contract_audit.csv"
    write_csv(pd.DataFrame(rows), out_path)
    logger.info("Wrote %s (fails=%s)", out_path, fails)

    if fails > 0:
        raise SystemExit(f"Schema contract validation failed (fails={fails}). See {out_path}")


if __name__ == "__main__":
    main()