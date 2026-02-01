"""
Profile raw Olist CSV files and record basic structural metadata.

This script does NOT transform data.
Its purpose is to document:
- row counts
- column counts
- column names

Why this step exists:
- to understand dataset shape before cleaning
- to catch schema inconsistencies early
- to produce an auditable artifact reviewers can inspect
"""

from pathlib import Path
import pandas as pd


# -------------------------
# Directory configuration
# -------------------------

# Raw, immutable input data
RAW = Path("data/raw")

# Output location for profiling artifacts (reports, not datasets)
OUT = Path("reports")

# Ensure reports directory exists so script is re-runnable
OUT.mkdir(parents=True, exist_ok=True)


# -------------------------
# Source file list
# -------------------------

# Explicit list keeps profiling deterministic and auditable
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
# Profiling logic
# -------------------------

def profile_csv(filename: str) -> dict:
	"""
	Read a CSV file and return basic structural metadata.

	Parameters:
	- filename: name of CSV file in data/raw

	Returns:
	- dictionary with row count, column count, and column names
	"""
	path = RAW / filename
	df = pd.read_csv(path)

	return {
		"file": filename,
		"rows": int(len(df)),
		"cols": int(df.shape[1]),
		"columns": ",".join(df.columns),
	}


# -------------------------
# Pipeline entry point
# -------------------------

def main():
	"""
	Profile all raw CSVs and write a consolidated report to disk.
	"""
	rows = [profile_csv(f) for f in FILES]

	out_path = OUT / "raw_profile.csv"

	# Convert list of dictionaries into a tabular report
	pd.DataFrame(rows).to_csv(out_path, index=False)

	print(f"Wrote {out_path}")


if __name__ == "__main__":
	main()
