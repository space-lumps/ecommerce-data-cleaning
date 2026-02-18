"""
Generate docs/data_dictionary.md from reports/raw_profile.csv.
"""

import pandas as pd

from ecom_pipeline.utils.io import repo_root, ensure_dir


def main() -> None:
	repo = repo_root()
	profile_path = repo / "reports" / "raw_profile.csv"
	out_path = repo / "docs" / "data_dictionary.md"

	df = pd.read_csv(profile_path)

	# Keep only rows with actual columns
	df = df[df["column"].notna()].copy()

	# Order consistently
	df = df.sort_values(["file", "column"])

	lines: list[str] = []
	lines.append("# Data Dictionary")
	lines.append("")
	lines.append("Derived from `reports/raw_profile.csv`.")
	lines.append("")

	for file_name, g in df.groupby("file", sort=True):
		dataset = file_name.replace(".csv", "")
		lines.append(f"## Dataset: {dataset}")
		lines.append("")
		lines.append("| Column | Raw Dtype | Null % | Null Count |")
		lines.append("|--------|-----------|--------|------------|")

		for _, r in g.iterrows():
			col = str(r["column"])
			dtype = str(r["dtype"])
			null_pct = f'{float(r["null_pct"]):.4f}'
			null_count = int(r["null_count"])
			lines.append(f"| `{col}` | `{dtype}` | {null_pct} | {null_count} |")

		lines.append("")

	ensure_dir(out_path.parent)
	out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
	main()
