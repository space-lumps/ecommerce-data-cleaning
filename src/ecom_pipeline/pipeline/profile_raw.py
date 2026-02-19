"""
Profile raw Olist CSV files and record structural metadata.

This script does NOT transform data.

It documents:
- row counts
- column counts
- column names
- inferred dtypes
- null counts
- null percentages

Purpose:
- understand dataset shape before cleaning
- detect schema inconsistencies early
- generate an auditable artifact for review

Output:
- reports/raw_profile.csv
"""

import pandas as pd

from ecom_pipeline.utils.io import repo_root, ensure_dir, raw_dir, reports_dir

# -------------------------
# Directory configuration
# -------------------------

REPO_ROOT = repo_root()
RAW = raw_dir()
OUT = reports_dir()


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


def profile_csv(filename: str) -> list[dict]:
    """
    Profile a CSV at the column level.

    Returns:
        list of dict rows (one per column)
    """
    path = RAW / filename

    try:
        df = pd.read_csv(path)
    except Exception as exc:
        raise SystemExit(f"Failed to read {path}:\n{exc}") from exc

    rows = []

    total_rows = len(df)

    for col in df.columns:
        null_count = int(df[col].isna().sum())
        null_pct = round((null_count / total_rows) * 100, 4) if total_rows else 0.0

        rows.append(
            {
                "file": filename,
                "column": col,
                "rows": total_rows,
                "dtype": str(df[col].dtype),
                "null_count": null_count,
                "null_pct": null_pct,
            }
        )

    return rows


# -------------------------
# Pipeline entry point
# -------------------------


def main() -> None:
    """Profile all raw CSVs and write consolidated report."""
    all_rows = []

    out_path = OUT / "raw_profile.csv"
    ensure_dir(out_path.parent)

    for f in FILES:
        try:
            all_rows.extend(profile_csv(f))
        except Exception as exc:
            all_rows.append(
                {
                    "file": f,
                    "column": None,
                    "rows": None,
                    "dtype": None,
                    "null_count": None,
                    "null_pct": None,
                    "error": str(exc),
                }
            )

    pd.DataFrame(all_rows).to_csv(out_path, index=False)


if __name__ == "__main__":
    main()
