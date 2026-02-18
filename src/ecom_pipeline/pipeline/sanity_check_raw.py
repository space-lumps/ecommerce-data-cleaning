"""
Sanity check raw Olist CSV files.

This script does NOT transform data.
It verifies:
- expected raw source files exist
- files can be read by pandas
- prints row/col counts and inferred dtypes

Run from repo root:
    python src/00_sanity_check_raw.py
"""

from pathlib import Path
import sys
import pandas as pd

from ecom_pipeline.utils.io import repo_root

# Anchor execution to repo root (independent of working directory)
REPO_ROOT = repo_root()

# Immutable input data
RAW = REPO_ROOT / "data" / "raw"

# Pipeline file validation
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

def main() -> None:
    # build a list of missing files in the repo
    missing = [f for f in FILES if not (RAW / f).exists()]
    if missing:
        msg = "Missing files in data/raw:\n- " + "\n- ".join(missing)
        raise SystemExit(msg)

    for f in FILES:
        # load file into memory
        path = RAW / f
        try:
            df = pd.read_csv(path)
        except Exception as exc:
            raise SystemExit(f"Failed to read {path}:\n{exc}") from exc

        # print output shape
        print(f"\n{f}")
        print(f"rows={len(df):,} cols={df.shape[1]:,}")

        #print inferred data types
        print(df.dtypes)

if __name__ == "__main__":
    main()
