"""
End-to-end (E2E) test for the full data cleaning pipeline.

This test:
- Creates isolated temporary directories to avoid modifying real data
- Copies sample CSVs from data/samples into a temp raw/ directory
- Sets environment variables to point the pipeline at the temp directories
- Executes the entire pipeline via subprocess
- Verifies successful completion (no exceptions) and presence of expected Parquet outputs

Ensures the pipeline runs end-to-end on sample data without errors.
"""

import shutil
import tempfile
import os
from pathlib import Path
import subprocess
import sys


def test_pipeline_runs_end_to_end() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)

        raw_dir = tmp / "raw"
        clean_dir = tmp / "clean"

        raw_dir.mkdir()
        clean_dir.mkdir()

        # Copy samples into temp raw dir
        samples_dir = repo_root / "data" / "samples"
        for f in samples_dir.glob("*.csv"):
            shutil.copy(f, raw_dir / f.name)

        env = os.environ.copy()
        env["ECOM_RAW_DIR"] = str(raw_dir)
        env["ECOM_CLEAN_DIR"] = str(clean_dir)

        subprocess.run(
            [sys.executable, "run_pipeline.py"],
            cwd=repo_root,
            check=True,
            env=env,
        )

        assert any(clean_dir.glob("*.parquet"))
