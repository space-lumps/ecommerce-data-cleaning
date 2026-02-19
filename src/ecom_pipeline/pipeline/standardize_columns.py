"""
Standardize raw Olist CSV files into a clean, machine-friendly format.

This script performs a *light* transformation only:
- normalizes column names
- preserves all rows and values
- writes results to data/interim as Parquet

Why this step exists:
- downstream joins depend on consistent column naming
- Parquet is faster and safer than CSV for analytics
- separating 'raw' from 'interim' enforces pipeline discipline
"""

import pandas as pd

from ecom_pipeline.utils.io import interim_dir, raw_dir, repo_root, write_parquet

# -------------------------
# Directory configuration
# -------------------------

# Anchor execution to repo root (independent of working directory)
REPO_ROOT = repo_root()

# Immutable input data
RAW = raw_dir()

# Lightly cleaned outputs (still one file per source table)

INTERIM = interim_dir()

# Ensure the interim directory exists so the script is re-runnable
# INTERIM.mkdir(parents=True, exist_ok=True)


# -------------------------
# Source file list
# -------------------------

# Explicit file list keeps the pipeline deterministic and auditable
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
# Transformation logic
# -------------------------


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of the DataFrame with standardized column names.

    Operations performed:
    - strip leading/trailing whitespace
    - convert to lowercase
    - replace spaces with underscores

    No data values are modified.
    """
    df = df.copy()

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    return df


# -------------------------
# Pipeline entry point
# -------------------------


def main() -> None:
    """
    Read each raw CSV, standardize column names, and write to Parquet.

    One output file is written per input file.
    """
    # Failure policy:
    # - Strict in this stage because it produces pipeline inputs for downstream steps.
    # - If any table fails to read/write, we stop to avoid partial/invalid interim state.

    for filename in FILES:
        in_path = RAW / filename
        out_path = INTERIM / filename.replace(".csv", ".parquet")

        try:
            df = pd.read_csv(in_path)
        except Exception as exc:
            raise SystemExit(f"Failed to read {in_path}:\n{exc}") from exc

        df = standardize_columns(df)

        try:
            write_parquet(df, out_path)
        except Exception as exc:
            raise SystemExit(f"Failed to write {out_path}:\n{exc}") from exc

        print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
