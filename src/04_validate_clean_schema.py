"""
Validate clean Olist tables against schema expectations.

Input:
- data/clean/*.parquet

Output:
- reports/clean_schema_audit.csv

Checks:
- string_cols are pandas str dtype
- datetime_cols are datetime64 dtype
- numeric_cols are not object/str (float/int ok)
"""

from pathlib import Path
import importlib.util
import pandas as pd

from utils.io import repo_root, read_parquet, write_csv
from utils.logging import configure_logging, get_logger


configure_logging()
logger = get_logger(__name__)


# workaround for module import:
# 03_enforce_schema cannot be imported directly since python does not allow importing modules beginning with a number
_spec = importlib.util.spec_from_file_location(
    "enforce_schema",
    Path(__file__).resolve().parent / "03_enforce_schema.py",
)
if _spec is None or _spec.loader is None:
    raise SystemExit("Failed to load 03_enforce_schema.py via importlib")

_enforce = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_enforce)
CAST_RULES = _enforce.CAST_RULES


REPO_ROOT = repo_root()
CLEAN = REPO_ROOT / "data" / "clean"
OUT = REPO_ROOT / "reports"


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


def main() -> None:
    rows = []

    for filename, rules in CAST_RULES.items():
        path = CLEAN / filename

        if not path.exists():
            logger.error("Missing clean file: %s", path)
            rows.append({"file": filename, "status": "missing_clean_file"})
            continue

        logger.info("Validating %s", filename)

        try:
            df = read_parquet(path)
        except Exception:
            logger.exception("Read failed: %s", path)
            raise

        # ---- string cols ----
        for col in rules.get("string_cols", []):
            if col not in df.columns:
                rows.append({"file": filename, "column": col, "expected": "str", "actual": "missing", "pass": False})
                continue
            actual = dtype_family(df[col].dtype)
            rows.append({"file": filename, "column": col, "expected": "str", "actual": actual, "pass": actual == "str"})

        # ---- datetime cols ----
        for col in rules.get("datetime_cols", []):
            if col not in df.columns:
                rows.append({"file": filename, "column": col, "expected": "datetime", "actual": "missing", "pass": False})
                continue
            actual = dtype_family(df[col].dtype)
            rows.append({"file": filename, "column": col, "expected": "datetime", "actual": actual, "pass": actual == "datetime"})

        # ---- numeric cols ----
        for col in rules.get("numeric_cols", []):
            if col not in df.columns:
                rows.append({"file": filename, "column": col, "expected": "numeric", "actual": "missing", "pass": False})
                continue
            actual = dtype_family(df[col].dtype)
            rows.append({"file": filename, "column": col, "expected": "numeric", "actual": actual, "pass": actual == "numeric"})

    out_path = OUT / "clean_schema_audit.csv"

    try:
        write_csv(pd.DataFrame(rows), out_path)
    except Exception:
        logger.exception("Write failed: %s", out_path)
        raise


    fails = sum(1 for r in rows if r.get("pass") is False)
    logger.info("Wrote %s (fails=%s)", out_path, fails)


if __name__ == "__main__":
    main()