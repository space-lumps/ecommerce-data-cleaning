"""
Run full ecom pipeline in order.
"""

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


def main() -> None:
    sanity_check_raw.main()
    profile_raw.main()
    generate_data_dictionary.main()
    standardize_columns.main()
    enforce_schema.main()
    validate_clean_schema.main()
    audit_all_clean_dtypes.main()
    validate_schema_contract.main()


if __name__ == "__main__":
    main()
