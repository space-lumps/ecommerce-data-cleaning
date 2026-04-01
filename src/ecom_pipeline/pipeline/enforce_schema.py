"""
Apply schema enforcement and data quality fixes to produce clean Olist tables.

Input:
- data/interim/*.parquet (standardized column names)

Output:
- data/clean/*.parquet (strongly typed and analytics-ready)

Responsibilities:
- Enforces all types, nullability, and constraints from SCHEMA_CONTRACT
- Prevents pandas from silently converting integers to float when NaNs exist
- Applies business rules (zip code padding, full state names)
- Enriches the category translation table to fix foreign key integrity
- Ensures consistent, BigQuery-friendly Parquet output
"""

import time

import pandas as pd

from ecom_pipeline.config.schema_contract import SCHEMA_CONTRACT
from ecom_pipeline.utils.io import (
    clean_dir,
    interim_dir,
    read_parquet,
    write_parquet,
)
from ecom_pipeline.utils.logging import configure_logging, get_logger

configure_logging()
logger = get_logger(__name__)

_INTERIM = interim_dir()
_CLEAN = clean_dir()

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


def enrich_category_translation(
    products_df: pd.DataFrame, translation_df: pd.DataFrame
) -> pd.DataFrame:
    """Add missing Portuguese categories from products into the translation table."""
    prod_cats = products_df["product_category_name"].dropna().unique()
    trans_cats = translation_df["product_category_name"].unique()

    missing_cats = [cat for cat in prod_cats if cat not in trans_cats]

    if not missing_cats:
        logger.info("✅ No missing product categories found.")
        return translation_df

    logger.info(
        f"Found {len(missing_cats)} missing categories. Enriching translation table..."
    )

    manual_mappings = {
        "pc_gamer": "pc_gamer",
        "portateis_cozinha_e_preparadores_de_alimentos": (
            "kitchen_portables_and_food_preparators"
        ),
    }

    new_rows = []
    for cat in missing_cats:
        english = manual_mappings.get(cat, cat.replace("_", " ").title())
        new_rows.append(
            {
                "product_category_name": cat,
                "product_category_name_english": english,
            }
        )

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        translation_df = pd.concat([translation_df, new_df], ignore_index=True)
        logger.info(f"Added {len(new_rows)} new category translations.")

    return translation_df


def enforce_schema(filename: str, df: pd.DataFrame) -> pd.DataFrame:
    """Apply schema enforcement using SCHEMA_CONTRACT as the single source of truth."""
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

    # Apply legacy rename (needed for products table)
    if filename == "olist_products_dataset.parquet":
        df = df.rename(columns=RENAME_MAP)

    # ------------------------------------------------------------------
    # Main type casting from SCHEMA_CONTRACT
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
            if numeric_type == "Float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
                df[col] = df[col].astype("Int64")

    # ------------------------------------------------------------------
    # Special business rules (derived columns)
    # ------------------------------------------------------------------
    # Brazilian zip codes: preserve leading zeros
    zip_cols = [col for col in df.columns if "zip_code_prefix" in col.lower()]
    for col in zip_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").str.zfill(5)
            logger.info("Applied zfill(5) to zip column: %s", col)

    # Derived column: full Brazilian state names (created as string)
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
            df["customer_state"]
            .map(state_name_map)
            .fillna(df["customer_state"])
            .astype("string")  # Ensure consistent nullable string dtype
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
    processed_dfs = {}  # Cache cleaned DataFrames in memory

    for filename in FILES:
        in_path = _INTERIM / filename
        df = read_parquet(in_path)

        df = enforce_schema(filename, df)

        # Special handling: enrich translation using the *cleaned* products table
        if filename == "product_category_name_translation.parquet":
            if "olist_products_dataset.parquet" in processed_dfs:
                products_clean = processed_dfs["olist_products_dataset.parquet"]
                df = enrich_category_translation(products_clean, df)
            else:
                logger.warning(
                    "Cleaned products table not found in cache. Skipping enrichment."
                )

        # Save to clean/
        out_path = _CLEAN / filename
        write_parquet(df, out_path)
        logger.info("Wrote %s", out_path)

        # Cache the cleaned df for later use (especially for translation)
        processed_dfs[filename] = df

    logger.info("Schema enforcement completed for all tables.")


if __name__ == "__main__":
    main()
