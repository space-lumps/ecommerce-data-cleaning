"""
Shared file I/O utilities for the data pipeline.
"""

from pathlib import Path
import pandas as pd
import os


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
    ensure_dir(path.parent)
    df.to_parquet(path, index=False)


def write_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    df.to_csv(path, index=False)
