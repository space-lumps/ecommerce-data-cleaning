"""
Declarative schema contract for the Olist pipeline.

This is intentionally framework-agnostic:
- can be used by pandas-based validators today
- can be reused later for dlt / DB loading

Notes:
- "dtype_family" is logical (str / datetime / numeric), not pandas dtype strings.
- Some nulls are meaningful (e.g., delivery timestamps on canceled orders).
"""

from __future__ import annotations


SCHEMA_CONTRACT: dict[str, dict] = {
    # -----------------------------
    # Core dimension tables
    # -----------------------------
    "olist_customers_dataset.parquet": {
        "primary_key": ["customer_id"],
        "required_columns": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
        "columns": {
            "customer_id": {"dtype_family": "str", "nullable": False},
            "customer_unique_id": {"dtype_family": "str", "nullable": False},
            # keep as str to preserve leading zeros and avoid accidental numeric coercion
            "customer_zip_code_prefix": {"dtype_family": "str", "nullable": False},
            "customer_city": {"dtype_family": "str", "nullable": False},
            "customer_state": {"dtype_family": "str", "nullable": False},
        },
    },
    "olist_sellers_dataset.parquet": {
        "primary_key": ["seller_id"],
        "required_columns": [
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        ],
        "columns": {
            "seller_id": {"dtype_family": "str", "nullable": False},
            "seller_zip_code_prefix": {"dtype_family": "str", "nullable": False},
            "seller_city": {"dtype_family": "str", "nullable": False},
            "seller_state": {"dtype_family": "str", "nullable": False},
        },
    },
    "olist_products_dataset.parquet": {
        "primary_key": ["product_id"],
        "required_columns": [
            "product_id",
            "product_category_name",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        "columns": {
            "product_id": {"dtype_family": "str", "nullable": False},
            # category is nullable in raw; do not force imputation unless you explicitly choose to
            "product_category_name": {"dtype_family": "str", "nullable": True},
            "product_name_length": {"dtype_family": "numeric", "nullable": True},
            "product_description_length": {"dtype_family": "numeric", "nullable": True},
            "product_photos_qty": {"dtype_family": "numeric", "nullable": True},
            "product_weight_g": {"dtype_family": "numeric", "nullable": True},
            "product_length_cm": {"dtype_family": "numeric", "nullable": True},
            "product_height_cm": {"dtype_family": "numeric", "nullable": True},
            "product_width_cm": {"dtype_family": "numeric", "nullable": True},
        },
    },
    "product_category_name_translation.parquet": {
        "primary_key": ["product_category_name"],
        "required_columns": ["product_category_name", "product_category_name_english"],
        "columns": {
            "product_category_name": {"dtype_family": "str", "nullable": False},
            "product_category_name_english": {"dtype_family": "str", "nullable": False},
        },
    },
    "olist_geolocation_dataset.parquet": {
        # geolocation is not strictly keyed; many rows per zip
        "primary_key": None,
        "required_columns": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        ],
        "columns": {
            "geolocation_zip_code_prefix": {"dtype_family": "str", "nullable": False},
            "geolocation_lat": {"dtype_family": "numeric", "nullable": False},
            "geolocation_lng": {"dtype_family": "numeric", "nullable": False},
            "geolocation_city": {"dtype_family": "str", "nullable": False},
            "geolocation_state": {"dtype_family": "str", "nullable": False},
        },
    },
    # -----------------------------
    # Fact tables
    # -----------------------------
    "olist_orders_dataset.parquet": {
        "primary_key": ["order_id"],
        "required_columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        "columns": {
            "order_id": {"dtype_family": "str", "nullable": False},
            "customer_id": {"dtype_family": "str", "nullable": False},
            "order_status": {
                "dtype_family": "str",
                "nullable": False,
                "allowed_values": [
                    "created",
                    "approved",
                    "invoiced",
                    "processing",
                    "shipped",
                    "delivered",
                    "canceled",
                    "unavailable",
                ],
            },
            "order_purchase_timestamp": {"dtype_family": "datetime", "nullable": False},
            # meaningful nulls: some orders may not have approval or delivery timestamps
            "order_approved_at": {"dtype_family": "datetime", "nullable": True},
            "order_delivered_carrier_date": {"dtype_family": "datetime", "nullable": True},
            "order_delivered_customer_date": {"dtype_family": "datetime", "nullable": True},
            "order_estimated_delivery_date": {"dtype_family": "datetime", "nullable": False},
        },
        "foreign_keys": [
            {
                "from_columns": ["customer_id"],
                "to_table": "olist_customers_dataset.parquet",
                "to_columns": ["customer_id"],
            }
        ],
    },
    "olist_order_items_dataset.parquet": {
        "primary_key": ["order_id", "order_item_id"],
        "required_columns": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
        "columns": {
            "order_id": {"dtype_family": "str", "nullable": False},
            "order_item_id": {"dtype_family": "numeric", "nullable": False},
            "product_id": {"dtype_family": "str", "nullable": False},
            "seller_id": {"dtype_family": "str", "nullable": False},
            "shipping_limit_date": {"dtype_family": "datetime", "nullable": False},
            "price": {"dtype_family": "numeric", "nullable": False, "min": 0},
            "freight_value": {"dtype_family": "numeric", "nullable": False, "min": 0},
        },
        "foreign_keys": [
            {
                "from_columns": ["order_id"],
                "to_table": "olist_orders_dataset.parquet",
                "to_columns": ["order_id"],
            },
            {
                "from_columns": ["product_id"],
                "to_table": "olist_products_dataset.parquet",
                "to_columns": ["product_id"],
            },
            {
                "from_columns": ["seller_id"],
                "to_table": "olist_sellers_dataset.parquet",
                "to_columns": ["seller_id"],
            },
        ],
    },
    "olist_order_payments_dataset.parquet": {
        "primary_key": ["order_id", "payment_sequential"],
        "required_columns": [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ],
        "columns": {
            "order_id": {"dtype_family": "str", "nullable": False},
            "payment_sequential": {"dtype_family": "numeric", "nullable": False},
            "payment_type": {"dtype_family": "str", "nullable": False},
            "payment_installments": {"dtype_family": "numeric", "nullable": False, "min": 0},
            "payment_value": {"dtype_family": "numeric", "nullable": False, "min": 0},
        },
        "foreign_keys": [
            {
                "from_columns": ["order_id"],
                "to_table": "olist_orders_dataset.parquet",
                "to_columns": ["order_id"],
            }
        ],
    },
    "olist_order_reviews_dataset.parquet": {
        "primary_key": ["order_id", "review_id"],
        "required_columns": [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ],
        "columns": {
            "review_id": {"dtype_family": "str", "nullable": False},
            "order_id": {"dtype_family": "str", "nullable": False},
            "review_score": {"dtype_family": "numeric", "nullable": False, "min": 1, "max": 5},
            # nullable is expected in the source dataset
            "review_comment_title": {"dtype_family": "str", "nullable": True},
            "review_comment_message": {"dtype_family": "str", "nullable": True},
            "review_creation_date": {"dtype_family": "datetime", "nullable": False},
            "review_answer_timestamp": {"dtype_family": "datetime", "nullable": False},
        },
        "foreign_keys": [
            {
                "from_columns": ["order_id"],
                "to_table": "olist_orders_dataset.parquet",
                "to_columns": ["order_id"],
            }
        ],
    },
}
