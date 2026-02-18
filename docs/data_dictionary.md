# Data Dictionary

Derived from `reports/raw_profile.csv`.

## Dataset: olist_customers_dataset


| Column                     | Raw Dtype | Null % | Null Count |
| -------------------------- | --------- | ------ | ---------- |
| `customer_city`            | `str`     | 0.0000 | 0          |
| `customer_id`              | `str`     | 0.0000 | 0          |
| `customer_state`           | `str`     | 0.0000 | 0          |
| `customer_unique_id`       | `str`     | 0.0000 | 0          |
| `customer_zip_code_prefix` | `int64`   | 0.0000 | 0          |


## Dataset: olist_geolocation_dataset


| Column                        | Raw Dtype | Null % | Null Count |
| ----------------------------- | --------- | ------ | ---------- |
| `geolocation_city`            | `str`     | 0.0000 | 0          |
| `geolocation_lat`             | `float64` | 0.0000 | 0          |
| `geolocation_lng`             | `float64` | 0.0000 | 0          |
| `geolocation_state`           | `str`     | 0.0000 | 0          |
| `geolocation_zip_code_prefix` | `int64`   | 0.0000 | 0          |


## Dataset: olist_order_items_dataset


| Column                | Raw Dtype | Null % | Null Count |
| --------------------- | --------- | ------ | ---------- |
| `freight_value`       | `float64` | 0.0000 | 0          |
| `order_id`            | `str`     | 0.0000 | 0          |
| `order_item_id`       | `int64`   | 0.0000 | 0          |
| `price`               | `float64` | 0.0000 | 0          |
| `product_id`          | `str`     | 0.0000 | 0          |
| `seller_id`           | `str`     | 0.0000 | 0          |
| `shipping_limit_date` | `str`     | 0.0000 | 0          |


## Dataset: olist_order_payments_dataset


| Column                 | Raw Dtype | Null % | Null Count |
| ---------------------- | --------- | ------ | ---------- |
| `order_id`             | `str`     | 0.0000 | 0          |
| `payment_installments` | `int64`   | 0.0000 | 0          |
| `payment_sequential`   | `int64`   | 0.0000 | 0          |
| `payment_type`         | `str`     | 0.0000 | 0          |
| `payment_value`        | `float64` | 0.0000 | 0          |


## Dataset: olist_order_reviews_dataset


| Column                    | Raw Dtype | Null %  | Null Count |
| ------------------------- | --------- | ------- | ---------- |
| `order_id`                | `str`     | 0.0000  | 0          |
| `review_answer_timestamp` | `str`     | 0.0000  | 0          |
| `review_comment_message`  | `str`     | 58.7025 | 58247      |
| `review_comment_title`    | `str`     | 88.3415 | 87656      |
| `review_creation_date`    | `str`     | 0.0000  | 0          |
| `review_id`               | `str`     | 0.0000  | 0          |
| `review_score`            | `int64`   | 0.0000  | 0          |


## Dataset: olist_orders_dataset


| Column                          | Raw Dtype | Null % | Null Count |
| ------------------------------- | --------- | ------ | ---------- |
| `customer_id`                   | `str`     | 0.0000 | 0          |
| `order_approved_at`             | `str`     | 0.1609 | 160        |
| `order_delivered_carrier_date`  | `str`     | 1.7930 | 1783       |
| `order_delivered_customer_date` | `str`     | 2.9817 | 2965       |
| `order_estimated_delivery_date` | `str`     | 0.0000 | 0          |
| `order_id`                      | `str`     | 0.0000 | 0          |
| `order_purchase_timestamp`      | `str`     | 0.0000 | 0          |
| `order_status`                  | `str`     | 0.0000 | 0          |


## Dataset: olist_products_dataset


| Column                       | Raw Dtype | Null % | Null Count |
| ---------------------------- | --------- | ------ | ---------- |
| `product_category_name`      | `str`     | 1.8512 | 610        |
| `product_description_lenght` | `float64` | 1.8512 | 610        |
| `product_height_cm`          | `float64` | 0.0061 | 2          |
| `product_id`                 | `str`     | 0.0000 | 0          |
| `product_length_cm`          | `float64` | 0.0061 | 2          |
| `product_name_lenght`        | `float64` | 1.8512 | 610        |
| `product_photos_qty`         | `float64` | 1.8512 | 610        |
| `product_weight_g`           | `float64` | 0.0061 | 2          |
| `product_width_cm`           | `float64` | 0.0061 | 2          |


## Dataset: olist_sellers_dataset


| Column                   | Raw Dtype | Null % | Null Count |
| ------------------------ | --------- | ------ | ---------- |
| `seller_city`            | `str`     | 0.0000 | 0          |
| `seller_id`              | `str`     | 0.0000 | 0          |
| `seller_state`           | `str`     | 0.0000 | 0          |
| `seller_zip_code_prefix` | `int64`   | 0.0000 | 0          |


## Dataset: product_category_name_translation


| Column                          | Raw Dtype | Null % | Null Count |
| ------------------------------- | --------- | ------ | ---------- |
| `product_category_name`         | `str`     | 0.0000 | 0          |
| `product_category_name_english` | `str`     | 0.0000 | 0          |


