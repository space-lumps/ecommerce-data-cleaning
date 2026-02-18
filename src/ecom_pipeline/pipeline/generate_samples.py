"""
Generate a small, referentially-consistent sample of the Olist raw dataset.

Input:
- data/raw/*.csv

Output:
- data/samples/*.csv

Sampling approach:
- Sample N orders (deterministic seed)
- Keep only related rows in customers, items, payments, reviews, products, sellers
- For geolocation, keep rows matching sampled customer/seller ZIP prefixes (capped per ZIP)
- Keep full category translation table (small)
"""

from __future__ import annotations

import os
import pandas as pd

from ecom_pipeline.utils.io import repo_root, ensure_dir
from ecom_pipeline.utils.logging import configure_logging, get_logger


configure_logging()
logger = get_logger(__name__)

REPO_ROOT = repo_root()
RAW_DIR = REPO_ROOT / "data" / "raw"
SAMPLES_DIR = REPO_ROOT / "data" / "samples"

FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def read_csv(name: str) -> pd.DataFrame:
    path = RAW_DIR / name
    return pd.read_csv(path)


def write_csv(df: pd.DataFrame, name: str) -> None:
    ensure_dir(SAMPLES_DIR)
    out_path = SAMPLES_DIR / name
    df.to_csv(out_path, index=False)


def main() -> None:
    # Tunables via env vars
    n_orders = int(os.getenv("ECOM_SAMPLE_ORDERS", "1000"))
    seed = int(os.getenv("ECOM_SAMPLE_SEED", "42"))
    geo_max_per_zip = int(os.getenv("ECOM_SAMPLE_GEO_MAX_PER_ZIP", "25"))

    # ---- orders (anchor table) ----
    logger.info("Reading orders")
    orders = read_csv(FILES["orders"])

    if n_orders >= len(orders):
        logger.info("Requested sample size >= total orders; using full orders table")
        orders_s = orders.copy()
    else:
        orders_s = orders.sample(n=n_orders, random_state=seed)

    order_ids = set(orders_s["order_id"].astype(str))
    customer_ids = set(orders_s["customer_id"].astype(str))

    logger.info("Sampled orders: %s", len(orders_s))

    # ---- customers ----
    logger.info("Reading customers")
    customers = read_csv(FILES["customers"])
    customers_s = customers[customers["customer_id"].astype(str).isin(customer_ids)].copy()

    # ---- order_items (and derived product_ids, seller_ids) ----
    logger.info("Reading order_items")
    order_items = read_csv(FILES["order_items"])
    order_items_s = order_items[order_items["order_id"].astype(str).isin(order_ids)].copy()

    product_ids = set(order_items_s["product_id"].astype(str))
    seller_ids = set(order_items_s["seller_id"].astype(str))

    # ---- payments ----
    logger.info("Reading order_payments")
    payments = read_csv(FILES["order_payments"])
    payments_s = payments[payments["order_id"].astype(str).isin(order_ids)].copy()

    # ---- reviews ----
    logger.info("Reading order_reviews")
    reviews = read_csv(FILES["order_reviews"])
    reviews_s = reviews[reviews["order_id"].astype(str).isin(order_ids)].copy()

    # ---- products ----
    logger.info("Reading products")
    products = read_csv(FILES["products"])
    products_s = products[products["product_id"].astype(str).isin(product_ids)].copy()

    # ---- sellers ----
    logger.info("Reading sellers")
    sellers = read_csv(FILES["sellers"])
    sellers_s = sellers[sellers["seller_id"].astype(str).isin(seller_ids)].copy()

    # ---- category translation (small; keep all) ----
    logger.info("Reading category translation")
    cat = read_csv(FILES["category_translation"])
    cat_s = cat.copy()

    # ---- geolocation (large; filter by sampled ZIP prefixes, cap per ZIP) ----
    logger.info("Reading geolocation")
    geo = read_csv(FILES["geolocation"])

    zip_prefixes = set()
    if "customer_zip_code_prefix" in customers_s.columns:
        zip_prefixes |= set(customers_s["customer_zip_code_prefix"].dropna().astype(str))
    if "seller_zip_code_prefix" in sellers_s.columns:
        zip_prefixes |= set(sellers_s["seller_zip_code_prefix"].dropna().astype(str))

    geo_match = geo[geo["geolocation_zip_code_prefix"].astype(str).isin(zip_prefixes)].copy()

    if geo_max_per_zip > 0 and not geo_match.empty:
        geo_s = (
            geo_match.groupby("geolocation_zip_code_prefix", group_keys=False)
            .apply(lambda g: g.sample(n=min(len(g), geo_max_per_zip), random_state=seed))
        )
    else:
        geo_s = geo_match


    # ---- write outputs ----
    logger.info("Writing samples to %s", SAMPLES_DIR)
    write_csv(orders_s, FILES["orders"])
    write_csv(customers_s, FILES["customers"])
    write_csv(order_items_s, FILES["order_items"])
    write_csv(payments_s, FILES["order_payments"])
    write_csv(reviews_s, FILES["order_reviews"])
    write_csv(products_s, FILES["products"])
    write_csv(sellers_s, FILES["sellers"])
    write_csv(geo_s, FILES["geolocation"])
    write_csv(cat_s, FILES["category_translation"])

    logger.info(
        "Done. Sample sizes: orders=%s customers=%s items=%s payments=%s reviews=%s products=%s sellers=%s geo=%s cat=%s",
        len(orders_s), len(customers_s), len(order_items_s), len(payments_s), len(reviews_s),
        len(products_s), len(sellers_s), len(geo_s), len(cat_s),
    )


if __name__ == "__main__":
    main()
