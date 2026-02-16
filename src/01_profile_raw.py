"""
Profile raw Olist CSV files and record structural metadata.

This script does NOT transform data.

It documents:
- row counts
- column counts
- column names
- inferred dtypes

Purpose:
- understand dataset shape before cleaning
- detect schema inconsistencies early
- generate an auditable artifact for review

Output:
- reports/raw_profile.csv
"""

from pathlib import Path
import pandas as pd


# -------------------------
# Directory configuration
# -------------------------

# Anchor execution to repo root (independent of working directory)
REPO_ROOT = Path(__file__).resolve().parents[1]

# Immutable input data
RAW = REPO_ROOT / "data" / "raw"

# Profiling output location
OUT = REPO_ROOT / "reports"
OUT.mkdir(parents=True, exist_ok=True)


# -------------------------
# Deterministic source list
# -------------------------

FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]


# -------------------------
# Profiling logic
# -------------------------

def profile_csv(filename: str) -> dict:
    """
    Read a CSV file and return structural metadata.

    Returns:
        dict with file name, row count, column count,
        column list, and inferred dtypes.
    """
    path = RAW / filename

    try:
        df = pd.read_csv(path)
    except Exception as exc:
        raise SystemExit(f"Failed to read {path}:\n{exc}") from exc

    # Serialize dtypes as: "col:type|col:type|..."
    dtypes_str = "|".join(
        f"{col}:{dtype}" for col, dtype in df.dtypes.items()
    )

    return {
        "file": filename,
        "rows": int(len(df)),
        "cols": int(df.shape[1]),
        "columns": ",".join(df.columns),
        "dtypes": dtypes_str,
    }


# -------------------------
# Pipeline entry point
# -------------------------

def main() -> None:
    """Profile all raw CSVs and write consolidated report."""
    rows = []
    out_path = OUT / "raw_profile.csv"
    
    for f in FILES:
        try:
            rows.append(profile_csv(f))
        except Exception as exc:
            rows.append({
                "file": f,
                "rows": None,
                "cols": None,
                "columns": None,
                "dtypes": None,
                "error": str(exc),
            })

    pd.DataFrame(rows).to_csv(out_path, index=False)

if __name__ == "__main__":
    main()