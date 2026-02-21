"""
Generate docs/data_dictionary.md from reports/raw_profile.csv.
"""

import pandas as pd

from ecom_pipeline.utils.io import docs_dir, ensure_dir, reports_dir


def main() -> None:
    profile_path = reports_dir() / "raw_profile.csv"
    out_path = docs_dir() / "data_dictionary.md"

    if not profile_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {profile_path}\n"
            "Run profile_raw.py first to generate reports/raw_profile.csv"
        )

    try:
        df = pd.read_csv(profile_path)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read {profile_path}: {exc}\n"
            "Check that the file exists and is a valid CSV from profile_raw.py"
        ) from exc

    # Keep only rows with actual columns
    df = df[df["column"].notna()].copy()
    lines: list[str] = []

    if df.empty:
        print("Warning: No valid columns found in raw_profile.csv")
        # Still write empty dictionary or exit gracefully
        lines.append("# Data Dictionary")
        lines.append("")
        lines.append("No data available.")
        out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    # Order consistently
    df = df.sort_values(["file", "column"])

    lines.append("# Data Dictionary")
    lines.append("")
    lines.append("Derived from `reports/raw_profile.csv`.")
    lines.append("")

    for file_name, g in df.groupby("file", sort=True):
        dataset = file_name.replace(".csv", "")  # type: ignore[union-attr,arg-type]
        lines.append(f"## Dataset: {dataset}")  # type: ignore[str-bytes-safe]
        lines.append("")
        lines.append("| Column | Raw Dtype | Null % | Null Count |")
        lines.append("|--------|-----------|--------|------------|")

        for _, r in g.iterrows():
            col = str(r["column"])
            dtype = str(r["dtype"])
            null_pct_raw = float(r["null_pct"])
            null_pct = f"{null_pct_raw:.4f}" if not pd.isna(null_pct_raw) else "0.0000"
            null_count = int(r["null_count"])
            lines.append(f"| `{col}` | `{dtype}` | {null_pct} | {null_count} |")

        lines.append("")

    ensure_dir(out_path.parent)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
