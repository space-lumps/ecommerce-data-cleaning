from pathlib import Path
import pandas as pd

RAW = Path("data/raw")

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

def main():
	missing = [f for f in FILES if not (RAW / f).exists()]
	if missing:
		raise SystemExit(
			"Missing files in data/raw:\n- " + "\n- ".join(missing)
		)

	for f in FILES:
		path = RAW / f
		df = pd.read_csv(path)
		print(f"\n{f}")
		print(f"rows={len(df):,} cols={df.shape[1]:,}")
		print(df.dtypes)

if __name__ == "__main__":
	main()
