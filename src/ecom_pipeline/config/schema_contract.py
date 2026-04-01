"""
Declarative schema contract for the Olist pipeline.

This is intentionally framework-agnostic:
- can be used by pandas-based validators today
- can be reused later for dlt / DB loading

Notes:
- "dtype_family" defines logical types only: 'string', 'numeric', or 'datetime'.
- It is NOT a direct pandas dtype string (we do not store 'StringDtype'
  or 'Int64' here).
- In practice we map:
    - 'string'   → pandas nullable 'string' dtype (pd.StringDtype)
    - 'numeric'  → pandas nullable numeric dtypes (Int64, Float64, etc.)
    - 'datetime' → pandas 'datetime64[ns]'
- We chose the pandas nullable 'string' dtype for all text columns because it
  properly supports pd.NA (pandas' native missing value) instead of Python None
  or numpy NaN, leading to more consistent behavior in cleaning and downstream use.
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
            "customer_id": {"dtype_family": "string", "nullable": False},
            "customer_unique_id": {"dtype_family": "string", "nullable": False},
            "customer_zip_code_prefix": {"dtype_family": "string", "nullable": False},
            "customer_city": {"dtype_family": "string", "nullable": False},
            "customer_state": {"dtype_family": "string", "nullable": False},
        },
        # No outgoing FKs (root table)
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
            "seller_id": {"dtype_family": "string", "nullable": False},
            "seller_zip_code_prefix": {"dtype_family": "string", "nullable": False},
            "seller_city": {"dtype_family": "string", "nullable": False},
            "seller_state": {"dtype_family": "string", "nullable": False},
        },
        # No outgoing FKs
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
            "product_id": {"dtype_family": "string", "nullable": False},
            "product_category_name": {"dtype_family": "string", "nullable": True},
            "product_name_length": {
                "dtype_family": "numeric",
                "numeric_type": "Int64",
                "nullable": True,
            },
            "product_description_length": {
                "dtype_family": "numeric",
                "numeric_type": "Int64",
                "nullable": True,
            },
            "product_photos_qty": {
                "dtype_family": "numeric",
                "numeric_type": "Int64",
                "nullable": True,
            },
            "product_weight_g": {
                "dtype_family": "numeric",
                "numeric_type": "Int64",
                "nullable": True,
            },
            "product_length_cm": {
                "dtype_family": "numeric",
                "numeric_type": "Int64",
                "nullable": True,
            },
            "product_height_cm": {
                "dtype_family": "numeric",
                "numeric_type": "Int64",
                "nullable": True,
            },
            "product_width_cm": {
                "dtype_family": "numeric",
                "numeric_type": "Int64",
                "nullable": True,
            },
        },
        "foreign_keys": [
            {
                "from_columns": ["product_category_name"],
                "to_table": "product_category_name_translation.parquet",
                "to_columns": ["product_category_name"],
            }
        ],
    },
    "product_category_name_translation.parquet": {
        "primary_key": ["product_category_name"],
        "required_columns": ["product_category_name", "product_category_name_english"],
        "columns": {
            "product_category_name": {"dtype_family": "string", "nullable": False},
            "product_category_name_english": {
                "dtype_family": "string",
                "nullable": False,
            },
        },
        # No outgoing FKs
        "foreign_keys": [
            {
                "from_columns": ["product_category_name"],
                "to_table": "olist_products_dataset.parquet",
                "to_columns": ["product_category_name"],
            }
        ],
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
            "geolocation_zip_code_prefix": {
                "dtype_family": "string",
                "nullable": False,
            },
            "geolocation_lat": {
                "dtype_family": "numeric",
                "numeric_type": "Float64",
                "nullable": False,
            },
            "geolocation_lng": {
                "dtype_family": "numeric",
                "numeric_type": "Float64",
                "nullable": False,
            },
            "geolocation_city": {"dtype_family": "string", "nullable": False},
            "geolocation_state": {"dtype_family": "string", "nullable": False},
        },
        # No strict FKs (it's a many-to-one reference table)
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
            "order_id": {"dtype_family": "string", "nullable": False},
            "customer_id": {"dtype_family": "string", "nullable": False},
            "order_status": {
                "dtype_family": "string",
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
            "order_approved_at": {"dtype_family": "datetime", "nullable": True},
            "order_delivered_carrier_date": {
                "dtype_family": "datetime",
                "nullable": True,
            },
            "order_delivered_customer_date": {
                "dtype_family": "datetime",
                "nullable": True,
            },
            "order_estimated_delivery_date": {
                "dtype_family": "datetime",
                "nullable": False,
            },
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
            "order_id": {"dtype_family": "string", "nullable": False},
            "order_item_id": {
                "dtype_family": "numeric",
                "numeric_type": "Int64",
                "nullable": False,
            },
            "product_id": {"dtype_family": "string", "nullable": False},
            "seller_id": {"dtype_family": "string", "nullable": False},
            "shipping_limit_date": {"dtype_family": "datetime", "nullable": False},
            "price": {
                "dtype_family": "numeric",
                "numeric_type": "Float64",
                "nullable": False,
                "min": 0,
            },
            "freight_value": {
                "dtype_family": "numeric",
                "numeric_type": "Float64",
                "nullable": False,
                "min": 0,
            },
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
            "order_id": {"dtype_family": "string", "nullable": False},
            "payment_sequential": {
                "dtype_family": "numeric",
                "numeric_type": "Int64",
                "nullable": False,
            },
            "payment_type": {"dtype_family": "string", "nullable": False},
            "payment_installments": {
                "dtype_family": "numeric",
                "numeric_type": "Int64",
                "nullable": False,
                "min": 0,
            },
            "payment_value": {
                "dtype_family": "numeric",
                "numeric_type": "Float64",
                "nullable": False,
                "min": 0,
            },
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
            "review_id": {"dtype_family": "string", "nullable": False},
            "order_id": {"dtype_family": "string", "nullable": False},
            "review_score": {
                "dtype_family": "numeric",
                "numeric_type": "Int64",
                "nullable": False,
                "min": 1,
                "max": 5,
            },
            "review_comment_title": {"dtype_family": "string", "nullable": True},
            "review_comment_message": {"dtype_family": "string", "nullable": True},
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
