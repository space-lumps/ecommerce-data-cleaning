# Schema Contract

## Purpose

The schema contract defines the structural guarantees for all cleaned Olist datasets produced by the pipeline.

It acts as a single source of truth for:
- Required columns
- Primary key constraints
- Logical data types (`string`, `numeric`, `datetime`)
- Nullable rules and domain constraints (e.g. allowed values, min/max)

## Why This Matters

- Prevents silent schema drift between pipeline runs
- Makes downstream analytics and loading steps safer and more predictable
- Serves as living documentation for anyone consuming the cleaned `.parquet` files

## Design Decisions

- **dtype_family** uses logical types only (`string` / `numeric` / `datetime`)
- Text columns use pandas nullable `string` dtype (supports `pd.NA`) instead of legacy `object`
- Some nulls are intentionally allowed (e.g. delivery timestamps on canceled orders)
- Foreign key relationships are declared for future validation and lineage tracking

## Relationship to Other Components

- **CAST_RULES**: Controls type casting and cleaning logic during transformation
- **Schema Contract**: Validates the final output structure after cleaning
- **Data Validator**: Enforces this contract on the produced Parquet files

## Usage

The contract is defined in `src/ecom_pipeline/config/schema_contract.py` and consumed by the validation module.