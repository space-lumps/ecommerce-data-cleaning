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

from ecom_pipeline.config.schema_contract import SCHEMA_CONTRACT
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


def enforce_schema(filename: str, df: pd.DataFrame) -> pd.DataFrame:
    """Apply schema enforcement using SCHEMA_CONTRACT as the single source of truth.

    - string   → pandas nullable 'string'
    - datetime → datetime64[ns]
    - numeric  → Int64 or Float64 based on 'numeric_type' in contract
    """
    contract = SCHEMA_CONTRACT.get(filename, {})
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

    # Apply legacy rename (needed for products table column typos)
    if filename == "olist_products_dataset.parquet":
        df = df.rename(columns=RENAME_MAP)

    # ------------------------------------------------------------------
    # Type casting driven by SCHEMA_CONTRACT
    # ------------------------------------------------------------------
    for col, spec in contract.get("columns", {}).items():
        if col not in df.columns:
            continue

        dtype_family = spec.get("dtype_family")

        if dtype_family == "string":
            df[col] = df[col].astype("string")

        elif dtype_family == "datetime":
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].dt.tz_localize(None).astype("datetime64[ns]")

        elif dtype_family == "numeric":
            numeric_type = spec.get("numeric_type")
            if numeric_type is None:
                raise ValueError(
                    f"Column '{col}' in {filename} is numeric "
                    "but missing 'numeric_type' in SCHEMA_CONTRACT. "
                    "Must be 'Int64' or 'Float64'."
                )

            if numeric_type == "Float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
            else:
                # For Int64: first convert to float (safe), then to Int64
                # This avoids the "cannot safely cast float64 to int64" error
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
                df[col] = df[col].astype("Int64")

    # ------------------------------------------------------------------
    # Special business rules (not pure type casting)
    # ------------------------------------------------------------------
    # Brazilian zip codes: preserve leading zeros
    zip_cols = [col for col in df.columns if "zip_code_prefix" in col.lower()]
    for col in zip_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.zfill(5)
            logger.info("Applied zfill(5) to zip column: %s", col)

    # Derived column: full Brazilian state names for better BI visualizations
    if filename == "olist_customers_dataset.parquet" and "customer_state" in df.columns:
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
            df["customer_state"].map(state_name_map).fillna(df["customer_state"])
        )
        logger.info("Added derived column: customer_state_name")

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

    return df


def main() -> None:
    for filename in FILES:
        in_path = INTERIM / filename
        out_path = CLEAN / filename

        logger.info("Processing %s", filename)

        df = read_parquet(in_path)

        df = enforce_schema(filename, df)

        write_parquet(df, out_path)
        logger.info("Wrote %s", out_path)


if __name__ == "__main__":
    main()
