"""
Enforce a minimal, analytics-friendly schema on interim Olist tables.

Input:
- data/interim/*.parquet (column names already standardized)

Output:
- data/clean/*.parquet (types normalized for joins/analytics)

Changes applied:
- integer_cols → Int64 (nullable integer)
- float_cols   → Float64 (nullable float)
- string_cols  → string (nullable string)
- datetime_cols → datetime64[ns] (nullable datetime)
- Prevents pandas from silently converting integer columns to float
  when NaNs are present
- Ensures correct INT64 types when loaded into BigQuery and Looker Studio
"""

import time

import pandas as pd

from ecom_pipeline.utils.io import (
    clean_dir,
    interim_dir,
    read_parquet,
    repo_root,
    write_parquet,
)
from ecom_pipeline.utils.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

REPO_ROOT = repo_root()
INTERIM = interim_dir()
CLEAN = clean_dir()

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
    "olist_customers_dataset.parquet": {
        "datetime_cols": [],
        "string_cols": [
            "customer_id",
            "customer_unique_id",
        ],
        "integer_cols": [],
        "float_cols": [],
        "zip_cols": ["customer_zip_code_prefix"],
    },
    "olist_geolocation_dataset.parquet": {
        "datetime_cols": [],
        "string_cols": [],
        "integer_cols": [],
        "float_cols": ["geolocation_lat", "geolocation_lng"],
        "zip_cols": ["geolocation_zip_code_prefix"],
    },
    "olist_order_items_dataset.parquet": {
        "datetime_cols": ["shipping_limit_date"],
        "string_cols": ["order_id", "product_id", "seller_id"],
        "integer_cols": ["order_item_id"],
        "float_cols": ["price", "freight_value"],
    },
    "olist_order_payments_dataset.parquet": {
        "datetime_cols": [],
        "string_cols": ["order_id"],
        "integer_cols": ["payment_installments", "payment_sequential"],
        "float_cols": ["payment_value"],
    },
    "olist_order_reviews_dataset.parquet": {
        "datetime_cols": ["review_creation_date", "review_answer_timestamp"],
        "string_cols": ["review_id", "order_id"],
        "integer_cols": ["review_score"],
        "float_cols": [],
    },
    "olist_orders_dataset.parquet": {
        "datetime_cols": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "string_cols": ["order_id", "customer_id", "order_status"],
        "integer_cols": [],
        "float_cols": [],
    },
    "olist_products_dataset.parquet": {
        "datetime_cols": [],
        "string_cols": ["product_id", "product_category_name"],
        "integer_cols": [
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        "float_cols": [],
    },
    "olist_sellers_dataset.parquet": {
        "datetime_cols": [],
        "string_cols": ["seller_id"],
        "integer_cols": [],
        "float_cols": [],
        "zip_cols": ["seller_zip_code_prefix"],
    },
    "product_category_name_translation.parquet": {
        "datetime_cols": [],
        "string_cols": ["product_category_name", "product_category_name_english"],
        "integer_cols": [],
        "float_cols": [],
    },
}


def enforce_schema(filename: str, df: pd.DataFrame) -> pd.DataFrame:
    rules = CAST_RULES.get(filename, {})
    df = df.copy()

    start_time = time.time()
    initial_rows = len(df)
    initial_nulls = df.isna().sum().sum()

    logger.info(
        "Starting schema enforcement for %s | rows=%s | nulls=%s",
        filename,
        f"{initial_rows:,}",
        f"{initial_nulls:,}",
    )

    # String columns - nullable string
    for col in rules.get("string_cols", []):
        if col in df.columns:
            df[col] = df[col].astype("string")

    # Datetime columns - nullable datetime
    for col in rules.get("datetime_cols", []):
        if col in df.columns:
            # Force proper timestamp handling for Parquet + BigQuery
            df[col] = pd.to_datetime(df[col], errors="coerce")
            # This line helps Arrow write the correct logical type
            df[col] = df[col].dt.tz_localize(None).astype("datetime64[ns]")

    # Integer columns - nullable Int64
    for col in rules.get("integer_cols", []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Float columns - nullable Float64
    for col in rules.get("float_cols", []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")

    # Force zip code prefixes to string and ensure leading zeros are preserved
    # Brazilian CEP prefixes are 5 digits, many start with 0
    # Use zfill to pad with leading zeros to 5 digits
    for col in rules.get("zip_cols", []):
        if col in df.columns:
            df[col] = df[col].astype("string").str.zfill(5)
            logger.info(f"Fixed leading zeros for {col} (zfill to 5 digits)")

    final_rows = len(df)
    final_nulls = df.isna().sum().sum()
    duration = time.time() - start_time

    logger.info(
        "Finished schema enforcement for %s | duration=%.2fs | rows=%s | nulls=%s → %s",
        filename,
        duration,
        f"{final_rows:,}",
        f"{initial_nulls:,}",
        f"{final_nulls:,}",
    )

    # === ADD FULL BRAZILIAN STATE NAMES FOR BETTER BI VISUALIZATION ===
    # This creates customer_state_name with full English names (e.g. "São Paulo")
    # Looker Studio Filled maps recognize full state names much more reliably than "SP"
    # We keep the original two-letter customer_state for any other analysis
    if filename == "olist_customers_dataset.parquet":
        state_name_map = {
            "SP": "São Paulo",
            "RJ": "Rio de Janeiro",
            "MG": "Minas Gerais",
            "RS": "Rio Grande do Sul",
            "BA": "Bahia",
            "PR": "Paraná",
            "PE": "Pernambuco",
            "CE": "Ceará",
            "PA": "Pará",
            "MA": "Maranhão",
            "SC": "Santa Catarina",
            "GO": "Goiás",
            "DF": "Distrito Federal",
            "ES": "Espírito Santo",
            "PB": "Paraíba",
            "RN": "Rio Grande do Norte",
            "MT": "Mato Grosso",
            "MS": "Mato Grosso do Sul",
            "AL": "Alagoas",
            "PI": "Piauí",
            "SE": "Sergipe",
            "TO": "Tocantins",
            "RO": "Rondônia",
            "AM": "Amazonas",
            "AC": "Acre",
            "AP": "Amapá",
            "RR": "Roraima",
        }

        df["customer_state_name"] = (
            df["customer_state"]
            .map(state_name_map)
            .fillna(df["customer_state"])  # fallback for any unexpected codes
        )

        logger.info("Added customer_state_name column with full Brazilian state names")

    return df


def main() -> None:
    for filename in FILES:
        in_path = INTERIM / filename
        out_path = CLEAN / filename

        logger.info("Processing %s", filename)

        df = read_parquet(in_path)

        if filename == "olist_products_dataset.parquet":
            df = df.rename(columns=RENAME_MAP)

        df = enforce_schema(filename, df)

        write_parquet(df, out_path)
        logger.info("Wrote %s", out_path)


if __name__ == "__main__":
    main()
