"""
Shared file I/O utilities for the data pipeline.
"""

import os
from pathlib import Path

import pandas as pd


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def raw_dir() -> Path:
    override = os.environ.get("ECOM_RAW_DIR")
    if override:
        return Path(override)
    return repo_root() / "data" / "raw"


def interim_dir() -> Path:
    override = os.environ.get("ECOM_INTERIM_DIR")
    if override:
        return Path(override)
    return repo_root() / "data" / "interim"


def clean_dir() -> Path:
    override = os.environ.get("ECOM_CLEAN_DIR")
    if override:
        return Path(override)
    return repo_root() / "data" / "clean"


# samples directory is used for development purposes only
def samples_dir() -> Path:
    override = os.environ.get("ECOM_SAMPLES_DIR")
    if override:
        return Path(override)
    return repo_root() / "data" / "samples"


def reports_dir() -> Path:
    override = os.environ.get("ECOM_REPORTS_DIR")
    if override:
        return Path(override)
    return repo_root() / "reports"


def docs_dir() -> Path:
    override = os.environ.get("ECOM_DOCS_DIR")
    if override:
        return Path(override)
    return repo_root() / "docs"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """
    Write DataFrame to Parquet file with settings optimized for BigQuery compatibility.

    Key parameters:
    - engine='pyarrow': Most reliable for timestamp handling
    - compression='snappy': Good balance of speed and size
    - use_deprecated_int96_timestamps=True: Critical fix for BigQuery to correctly
      recognize datetime64[ns] columns as TIMESTAMP instead of INT64.

    This addresses a common pandas/pyarrow → BigQuery interoperability issue
    where timestamps lose their logical type metadata.
    """
    ensure_dir(path.parent)
    df.to_parquet(
        path,
        engine="pyarrow",
        compression="snappy",
        use_deprecated_int96_timestamps=True,  # fix BigQuery timestamp parsing issue
    )


def write_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    df.to_csv(path, index=False)
