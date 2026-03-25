# Olist Ecommerce Clean Data Dictionary

**Source of truth**: Final cleaned schema after `enforce_schema.py`
(nullable Int64, Float64, string, and datetime64[ns] types)

Generated from `reports/clean_dtypes_full.csv`

## Dataset: olist_customers_dataset

| Column | Clean Dtype | Null % | Null Count |
|--------|--------|--------|--------|
| `customer_city` | `string` | 0.0000 | 0 |
| `customer_id` | `string` | 0.0000 | 0 |
| `customer_state` | `string` | 0.0000 | 0 |
| `customer_unique_id` | `string` | 0.0000 | 0 |
| `customer_zip_code_prefix` | `string` | 0.0000 | 0 |

## Dataset: olist_geolocation_dataset

| Column | Clean Dtype | Null % | Null Count |
|--------|--------|--------|--------|
| `geolocation_city` | `string` | 0.0000 | 0 |
| `geolocation_lat` | `Float64` | 0.0000 | 0 |
| `geolocation_lng` | `Float64` | 0.0000 | 0 |
| `geolocation_state` | `string` | 0.0000 | 0 |
| `geolocation_zip_code_prefix` | `string` | 0.0000 | 0 |

## Dataset: olist_order_items_dataset

| Column | Clean Dtype | Null % | Null Count |
|--------|--------|--------|--------|
| `freight_value` | `Float64` | 0.0000 | 0 |
| `order_id` | `string` | 0.0000 | 0 |
| `order_item_id` | `Int64` | 0.0000 | 0 |
| `price` | `Float64` | 0.0000 | 0 |
| `product_id` | `string` | 0.0000 | 0 |
| `seller_id` | `string` | 0.0000 | 0 |
| `shipping_limit_date` | `datetime64[ns]` | 0.0000 | 0 |

## Dataset: olist_order_payments_dataset

| Column | Clean Dtype | Null % | Null Count |
|--------|--------|--------|--------|
| `order_id` | `string` | 0.0000 | 0 |
| `payment_installments` | `Int64` | 0.0000 | 0 |
| `payment_sequential` | `Int64` | 0.0000 | 0 |
| `payment_type` | `string` | 0.0000 | 0 |
| `payment_value` | `Float64` | 0.0000 | 0 |

## Dataset: olist_order_reviews_dataset

| Column | Clean Dtype | Null % | Null Count |
|--------|--------|--------|--------|
| `order_id` | `string` | 0.0000 | 0 |
| `review_answer_timestamp` | `datetime64[ns]` | 0.0000 | 0 |
| `review_comment_message` | `string` | 58.7025 | 58247 |
| `review_comment_title` | `string` | 88.3415 | 87656 |
| `review_creation_date` | `datetime64[ns]` | 0.0000 | 0 |
| `review_id` | `string` | 0.0000 | 0 |
| `review_score` | `Int64` | 0.0000 | 0 |

## Dataset: olist_orders_dataset

| Column | Clean Dtype | Null % | Null Count |
|--------|--------|--------|--------|
| `customer_id` | `string` | 0.0000 | 0 |
| `order_approved_at` | `datetime64[ns]` | 0.1609 | 160 |
| `order_delivered_carrier_date` | `datetime64[ns]` | 1.7930 | 1783 |
| `order_delivered_customer_date` | `datetime64[ns]` | 2.9817 | 2965 |
| `order_estimated_delivery_date` | `datetime64[ns]` | 0.0000 | 0 |
| `order_id` | `string` | 0.0000 | 0 |
| `order_purchase_timestamp` | `datetime64[ns]` | 0.0000 | 0 |
| `order_status` | `string` | 0.0000 | 0 |

## Dataset: olist_products_dataset

| Column | Clean Dtype | Null % | Null Count |
|--------|--------|--------|--------|
| `product_category_name` | `string` | 1.8512 | 610 |
| `product_description_length` | `Int64` | 1.8512 | 610 |
| `product_height_cm` | `Int64` | 0.0061 | 2 |
| `product_id` | `string` | 0.0000 | 0 |
| `product_length_cm` | `Int64` | 0.0061 | 2 |
| `product_name_length` | `Int64` | 1.8512 | 610 |
| `product_photos_qty` | `Int64` | 1.8512 | 610 |
| `product_weight_g` | `Int64` | 0.0061 | 2 |
| `product_width_cm` | `Int64` | 0.0061 | 2 |

## Dataset: olist_sellers_dataset

| Column | Clean Dtype | Null % | Null Count |
|--------|--------|--------|--------|
| `seller_city` | `string` | 0.0000 | 0 |
| `seller_id` | `string` | 0.0000 | 0 |
| `seller_state` | `string` | 0.0000 | 0 |
| `seller_zip_code_prefix` | `string` | 0.0000 | 0 |

## Dataset: product_category_name_translation

| Column | Clean Dtype | Null % | Null Count |
|--------|--------|--------|--------|
| `product_category_name` | `string` | 0.0000 | 0 |
| `product_category_name_english` | `string` | 0.0000 | 0 |

