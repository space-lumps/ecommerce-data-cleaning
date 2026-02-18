"""
Run full ecom pipeline in order.
"""

from ecom_pipeline.pipeline import (
    sanity_check_raw,
    profile_raw,
    standardize_columns,
    enforce_schema,
    validate_clean_schema,
    audit_all_clean_dtypes,
    validate_schema_contract,
)


def main() -> None:
    sanity_check_raw.main()
    profile_raw.main()
    standardize_columns.main()
    enforce_schema.main()
    validate_clean_schema.main()
    audit_all_clean_dtypes.main()
    validate_schema_contract.main()

if __name__ == "__main__":
    main()
