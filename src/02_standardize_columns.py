"""
Standardize raw Olist CSV files into a clean, machine-friendly format.

This script performs a *light* transformation only:
- normalizes column names
- preserves all rows and values
- writes results to data/interim as Parquet

Why this step exists:
- downstream joins depend on consistent column naming
- Parquet is faster and safer than CSV for analytics
- separating 'raw' from 'interim' enforces pipeline discipline
"""

from pathlib import Path
import pandas as pd


# -------------------------
# Directory configuration
# -------------------------

# Raw, immutable input data (from Kaggle)
RAW = Path("data/raw")

# Lightly cleaned outputs (still one file per source table)
INTERIM = Path("data/interim")

# Ensure the interim directory exists so the script is re-runnable
INTERIM.mkdir(parents=True, exist_ok=True)


# -------------------------
# Source file list
# -------------------------

# Explicit file list keeps the pipeline deterministic and auditable
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


# -------------------------
# Transformation logic
# -------------------------

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
	"""
	Return a copy of the DataFrame with standardized column names.

	Operations performed:
	- strip leading/trailing whitespace
	- convert to lowercase
	- replace spaces with underscores

	No data values are modified.
	"""
	df = df.copy()

	df.columns = (
		df.columns
			.str.strip()
			.str.lower()
			.str.replace(" ", "_")
	)

	return df


# -------------------------
# Pipeline entry point
# -------------------------

def main():
	"""
	Read each raw CSV, standardize column names, and write to Parquet.

	One output file is written per input file.
	This preserves table boundaries for downstream modeling.
	"""
	for filename in FILES:
		in_path = RAW / filename
		out_path = INTERIM / filename.replace(".csv", ".parquet")

		# Load raw CSV
		df = pd.read_csv(in_path)

		# Normalize column names only
		df = standardize_columns(df)

		# Write optimized, typed columnar output
		df.to_parquet(out_path, index=False)

		print(f"Wrote {out_path}")


if __name__ == "__main__":
	main()