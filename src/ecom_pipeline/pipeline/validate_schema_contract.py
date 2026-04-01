"""
Full schema contract validator for cleaned Olist tables.

It performs the following checks (fails fast on any failure):
- Required columns are present
- dtype_family matches the contract (string / numeric / datetime)
- Non-null constraints for columns marked nullable=False
- Primary key uniqueness (if defined)
- Cross-table foreign key integrity (orphan detection)

Output:
- reports/clean_contract_audit.csv (detailed per-check results)
"""

from __future__ import annotations

import pandas as pd
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
    is_string_dtype,
)

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

# Runtime-only helpers (prefixed with _ so they are hidden from pdoc)
_CLEAN = clean_dir()
_OUT = reports_dir()


def get_dtype_family(series: pd.Series) -> str:
    """Map pandas dtype to logical family defined in SCHEMA_CONTRACT.

    Returns exactly 'string', 'numeric', or 'datetime'.
    """
    if is_datetime64_any_dtype(series.dtype):
        return "datetime"
    if is_numeric_dtype(series.dtype):
        return "numeric"
    if is_string_dtype(series.dtype) or is_object_dtype(series.dtype):
        return "string"
    return str(series.dtype).lower()


def load_clean_table(filename: str) -> pd.DataFrame:
    """Load a clean parquet file. Raises FileNotFoundError if missing."""
    path = _CLEAN / filename
    if not path.exists():
        raise FileNotFoundError(f"Missing clean file: {path}")
    return read_parquet(path)


def main() -> None:
    rows: list[dict] = []
    fails = 0

    # Cache tables for foreign key checks to avoid repeated reads
    table_cache: dict[str, pd.DataFrame] = {}

    def get_table(table_name: str) -> pd.DataFrame:
        if table_name not in table_cache:
            table_cache[table_name] = load_clean_table(table_name)
        return table_cache[table_name]

    for table_name, spec in SCHEMA_CONTRACT.items():
        logger.info("Running full contract validation for %s", table_name)

        # ---- Load table ----
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

        # ---- Required columns ----
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
        else:
            rows.append(
                {
                    "table": table_name,
                    "check": "required_columns",
                    "status": "pass",
                    "details": "",
                }
            )

        # ---- Per-column checks ----
        columns_spec = spec.get("columns", {})

        for col, col_spec in columns_spec.items():
            if col not in df.columns:
                continue

            ser = df[col]
            expected_family = col_spec.get("dtype_family")
            actual_family = get_dtype_family(ser)

            # dtype check
            if expected_family and actual_family != expected_family:
                fails += 1
                rows.append(
                    {
                        "table": table_name,
                        "check": "dtype_family",
                        "column": col,
                        "status": "fail",
                        "details": f"expected={expected_family} "
                        "actual={actual_family} "
                        "pandas_dtype={ser.dtype}",
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

            # nullability check
            nullable = col_spec.get("nullable", True)
            if not nullable:
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

            # allowed values check
            allowed_values = col_spec.get("allowed_values")
            if allowed_values is not None:
                bad = ser.dropna()[~ser.dropna().isin(allowed_values)]
                bad_count = len(bad)
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

            # min/max checks for numeric columns
            if expected_family == "numeric":
                for bound in ["min", "max"]:
                    if bound in col_spec:
                        bound_val = col_spec[bound]
                        ser_non_null = ser.dropna()
                        if bound == "min":
                            bad = ser_non_null < bound_val
                        else:
                            bad = ser_non_null > bound_val
                        bad_count = int(bad.sum())
                        if bad_count > 0:
                            fails += 1
                            rows.append(
                                {
                                    "table": table_name,
                                    "check": f"{bound}_value",
                                    "column": col,
                                    "status": "fail",
                                    "details": (
                                        f"{bound}={bound_val} bad_count={bad_count}"
                                    ),
                                }
                            )

        # ---- Primary key uniqueness ----
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

        # ---- Foreign key integrity ----
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

            # Orphan check
            left = df[from_cols].dropna().drop_duplicates()
            right = df_to[to_cols].dropna().drop_duplicates()

            merged = left.merge(
                right, how="left", left_on=from_cols, right_on=to_cols, indicator=True
            )
            orphan_count = int((merged["_merge"] == "left_only").sum())

            if orphan_count > 0:
                sample = (
                    merged.loc[merged["_merge"] == "left_only", from_cols]
                    .head(5)
                    .astype(str)
                    .to_dict("records")
                )
                fails += 1
                rows.append(
                    {
                        "table": table_name,
                        "check": "foreign_key_integrity",
                        "status": "fail",
                        "details": f"from_cols={from_cols} to_table={to_table} "
                        f"orphan_count={orphan_count} sample={sample}",
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

    # Write final audit report
    out_path = _OUT / "clean_contract_audit.csv"
    write_csv(pd.DataFrame(rows), out_path)

    logger.info("Full contract validation complete → %s (fails=%s)", out_path, fails)

    if fails > 0:
        raise SystemExit(
            f"Schema contract validation failed (fails={fails}). See {out_path}"
        )


if __name__ == "__main__":
    main()
