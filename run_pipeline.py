"""
Run full ecom pipeline in order.
"""

import time
from pathlib import Path

import pandas as pd

from ecom_pipeline.pipeline import (
    audit_all_clean_dtypes,
    enforce_schema,
    generate_data_dictionary,
    profile_raw,
    sanity_check_raw,
    standardize_columns,
    validate_clean_schema,
    validate_schema_contract,
)
from ecom_pipeline.pipeline.standardize_columns import FILES as PROCESSED_FILES
from ecom_pipeline.utils.logging import configure_logging, get_logger

configure_logging(level="info")
logger = get_logger(__name__)

STEPS = [
    ("sanity_check_raw", sanity_check_raw),
    ("profile_raw", profile_raw),
    ("generate_data_dictionary", generate_data_dictionary),
    ("standardize_columns", standardize_columns),
    ("enforce_schema", enforce_schema),
    ("validate_clean_schema", validate_clean_schema),
    ("audit_all_clean_dtypes", audit_all_clean_dtypes),
    ("validate_schema_contract", validate_schema_contract),
]


def main() -> None:
    sanity_check_raw.main()
    profile_raw.main()
    generate_data_dictionary.main()
    standardize_columns.main()
    enforce_schema.main()
    validate_clean_schema.main()
    audit_all_clean_dtypes.main()
    validate_schema_contract.main()

    # ── Dynamic summary after all steps ──
    reports_path = Path("reports")

    successes = 0
    for name, module in STEPS:
        t_start = time.time()
        logger.info("Starting %s", name)
        try:
            module.main()
            successes += 1
            logger.info("Completed %s in %.2fs", name, time.time() - t_start)
        except Exception as e:
            logger.error("Step %s failed: %s", name, e)
            raise

    try:
        contract_audit = pd.read_csv(reports_path / "clean_contract_audit.csv")
        fails = contract_audit["status"].eq("fail").sum()
        contract_status = "all passed" if fails == 0 else f"{fails} failures"
    except Exception:
        contract_status = "audit file missing"

    try:
        dtypes_flags = pd.read_csv(reports_path / "clean_dtypes_flags.csv")
        flag_count = len(dtypes_flags)
        dtype_status = (
            f"{flag_count} suspicious dtypes flagged"
            if flag_count > 0
            else "0 dtype flags"
        )
    except Exception:
        dtype_status = "audit file missing"

    # quick null summary from enforce_schema logs or from clean files

    logger.info("═" * 70)
    logger.info("Pipeline completed successfully")
    logger.info("Summary:")
    logger.info(f"- Steps completed successfully: {successes}/{len(STEPS)}")
    logger.info(f"- Files processed: {len(PROCESSED_FILES)}")
    logger.info(f"- Schema contract validation: {contract_status}")
    logger.info(f"- Dtype audit: {dtype_status}")
    logger.info("- See enforce_schema logs above for per-table null reduction stats")
    logger.info("- Full details: reports/clean_*.csv and docs/data_dictionary.md")
    logger.info("═" * 70)


if __name__ == "__main__":
    main()
