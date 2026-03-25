"""
Generate docs/data_dictionary.md from the clean audit.

This script creates the final data dictionary based on the cleaned tables
(after enforce_schema.py has applied nullable types).

Input:
- reports/clean_dtypes_full.csv

Output:
- docs/data_dictionary.md

Notes:
- Uses the cleaned dtype display from audit_all_clean_dtypes.py
- Shows final schema (nullable Int64, Float64, string, etc.)
- This is the source of truth for the cleaned data used in BigQuery and Looker Studio
"""

import pandas as pd

from ecom_pipeline.utils.io import docs_dir, ensure_dir, reports_dir


def main() -> None:
    # Use the clean audit instead of raw profile
    profile_path = reports_dir() / "clean_dtypes_full.csv"
    out_path = docs_dir() / "data_dictionary.md"

    if not profile_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {profile_path}\n"
            "Run audit_all_clean_dtypes.py first to generate "
            "reports/clean_dtypes_full.csv"
        )

    try:
        df = pd.read_csv(profile_path)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read {profile_path}: {exc}\n"
            "Check that the file exists and is a valid CSV "
            "from audit_all_clean_dtypes.py"
        ) from exc

    # Keep only rows with actual columns
    df = df[df["column"].notna()].copy()
    lines: list[str] = []

    if df.empty:
        lines.append("# Data Dictionary")
        lines.append("")
        lines.append("No data available.")
        out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    # Order consistently
    df = df.sort_values(["file", "column"])

    lines.append("# Olist Ecommerce Clean Data Dictionary")
    lines.append("")
    lines.append("**Source of truth**: Final cleaned schema after `enforce_schema.py`")
    lines.append("(nullable Int64, Float64, string, and datetime64[ns] types)")
    lines.append("")
    lines.append("Generated from `reports/clean_dtypes_full.csv`")
    lines.append("")

    for file_name, g in df.groupby("file", sort=True):
        dataset = file_name.replace(".parquet", "")
        lines.append(f"## Dataset: {dataset}")
        lines.append("")
        lines.append("| Column | Clean Dtype | Null % | Null Count |")
        lines.append("|--------|--------|--------|--------|")

        for _, r in g.iterrows():
            col = str(r["column"])
            dtype = str(r["dtype"])
            null_pct = (
                f"{float(r['null_pct']):.4f}"
                if not pd.isna(r["null_pct"])
                else "0.0000"
            )
            null_count = int(r["null_count"])

            lines.append(f"| `{col}` | `{dtype}` | {null_pct} | {null_count} |")

        lines.append("")

    ensure_dir(out_path.parent)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"✅ Data dictionary generated: {out_path}")


if __name__ == "__main__":
    main()
