from pathlib import Path

import pandas as pd
import pytest

# We import the real functions — no mocking needed for smoke tests
from ecom_pipeline.utils.io import (
    ensure_dir,
    read_parquet,
    repo_root,
    write_csv,
    write_parquet,
)


# test for basic accuracy and achoring to real repo
def test_repo_root_is_path():
    root = repo_root()
    assert isinstance(root, Path)
    assert root.exists()  # should point to real repo root
    assert (root / "README.md").exists()  # quick reality check


@pytest.fixture
def temp_df():
    """Small DataFrame fixture used by multiple tests."""
    return pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})


@pytest.fixture
def temp_path(tmp_path):  # tmp_path is built-in pytest fixture (temporary directory)
    return tmp_path / "test_output"


# test write and read preserves data
def test_write_and_read_parquet_roundtrip(temp_df, temp_path):
    path = temp_path.with_suffix(".parquet")

    write_parquet(temp_df, path)  # use the real function
    assert path.exists()

    read_back = read_parquet(path)
    pd.testing.assert_frame_equal(read_back, temp_df)  # checks content + dtypes


# test that CSV path works and directory is ensured
def test_write_csv_creates_file(temp_df, temp_path):
    path = temp_path.with_suffix(".csv")

    write_csv(temp_df, path)
    assert path.exists()
    assert path.stat().st_size > 0  # file is not empty

    # Quick content check
    read_back = pd.read_csv(path)
    assert len(read_back) == 2  # noqa: PLR2004 # expected row count from temp_df fixture


# test mkdir -p behavior
def test_ensure_dir_creates_parents(tmp_path):
    nested = tmp_path / "a" / "b" / "c"
    assert not nested.exists()

    ensure_dir(nested)
    assert nested.exists()
    assert nested.is_dir()
