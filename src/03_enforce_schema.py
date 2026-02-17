"""
Enforce a minimal, analytics-friendly schema on interim Olist tables.

Input:
- data/interim/*.parquet (column names already standardized)

Output:
- data/clean/*.parquet (types normalized for joins/analytics)

Policy:
- Strict: stop on any read/cast/write failure to avoid partial clean outputs.
"""

import pandas as pd
from utils.io import repo_root, read_parquet, write_parquet
from utils.logging import configure_logging, get_logger


configure_logging()
logger = get_logger(__name__)

REPO_ROOT = repo_root()
INTERIM = REPO_ROOT / "data" / "interim"
CLEAN = REPO_ROOT / "data" / "clean"

FILES = [
    "olist_orders_dataset.parquet",
    "olist_order_items_dataset.parquet",
    "olist_order_payments_dataset.parquet",
    "olist_order_reviews_dataset.parquet",
    "olist_products_dataset.parquet",
    "olist_customers_dataset.parquet",
    "olist_sellers_dataset.parquet",
    "olist_geolocation_dataset.parquet",
    "product_category_name_translation.parquet",
]

RENAME_MAP = {
    "product_name_lenght": "product_name_length",
    "product_description_lenght": "product_description_length",
}

CAST_RULES = {
    'olist_customers_dataset.parquet': {
        'datetime_cols': [],
        'string_cols': ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix'],
        'numeric_cols': []
        },
    'olist_geolocation_dataset.parquet': {
        'datetime_cols': [],
        'string_cols': ['geolocation_zip_code_prefix'],
        'numeric_cols': ['geolocation_lat', 'geolocation_lng']
        },
    'olist_order_items_dataset.parquet': {
        'datetime_cols': ['shipping_limit_date'],
        'string_cols': ['order_id', 'order_item_id', 'product_id', 'seller_id'],
        'numeric_cols': ['price', 'freight_value']
        },
    'olist_order_payments_dataset.parquet': {
        'datetime_cols': [],
        'string_cols': ['order_id'],
        'numeric_cols': ['payment_installments', 'payment_value', 'payment_sequential']
        },
    'olist_order_reviews_dataset.parquet': {
        'datetime_cols': ['review_creation_date', 'review_answer_timestamp'],
        'string_cols': ['review_id', 'order_id'],
        'numeric_cols': ['review_score']
        },
    'olist_orders_dataset.parquet': {
        'datetime_cols': [
            'order_purchase_timestamp',
            'order_approved_at',
            'order_delivered_carrier_date',
            'order_delivered_customer_date',
            'order_estimated_delivery_date'
            ],
        'string_cols': ['order_id', 'customer_id', 'order_status'],
        'numeric_cols': []
        },
    'olist_products_dataset.parquet': {
        'datetime_cols': [],
        'string_cols': ['product_id', 'product_category_name'],
        'numeric_cols': [
            'product_name_length',
            'product_description_length',
            'product_photos_qty',
            'product_weight_g',
            'product_length_cm',
            'product_height_cm',
            'product_width_cm'
            ]
        },
    'olist_sellers_dataset.parquet': {
        'datetime_cols': [],
        'string_cols': ['seller_id', 'seller_zip_code_prefix'],
        'numeric_cols': []
        },
    'product_category_name_translation.parquet': {
        'datetime_cols': [],
        'string_cols': ['product_category_name', 'product_category_name_english'],
        'numeric_cols': []
    },
}

def enforce_schema(filename: str, df: pd.DataFrame) -> pd.DataFrame:
    rules = CAST_RULES.get(filename, {})
    df = df.copy()

    for col in rules.get("string_cols", []):
        if col in df.columns:
            df[col] = df[col].astype("str")

    for col in rules.get("datetime_cols", []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in rules.get("numeric_cols", []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def main() -> None:
    for filename in FILES:
        in_path = INTERIM / filename
        out_path = CLEAN / filename

        logger.info("Processing %s", filename)

        # -----------------
        # Read
        # -----------------
        try:
            df = read_parquet(in_path)
        except Exception:
            logger.exception("Read failed: %s", in_path)
            raise

        # -----------------
        # Rename (if needed)
        # -----------------
        if filename == "olist_products_dataset.parquet":
            before = set(df.columns)
            df = df.rename(columns=RENAME_MAP)
            after = set(df.columns)
            if before != after:
                logger.info("Applied column rename map for %s", filename)

        # -----------------
        # Enforce schema
        # -----------------
        try:
            df = enforce_schema(filename, df)
        except Exception:
            logger.exception("Schema enforcement failed: %s", filename)
            raise

        # -----------------
        # Write
        # -----------------
        try:
            write_parquet(df, out_path)
        except Exception:
            logger.exception("Write failed: %s", out_path)
            raise


        logger.info("Wrote %s", out_path)


if __name__ == "__main__":
    main()