# Schema Contract

## Purpose

The schema contract defines the structural guarantees for all cleaned Olist datasets produced by the pipeline.

It serves as the **single source of truth** for:
- Required columns
- Primary key constraints
- Logical data types (`string`, `numeric`, `datetime`)
- Nullable rules and domain constraints (e.g. allowed values, min/max)
- Numeric type precision (`Int64` vs `Float64`)

## Why This Matters

- Prevents silent schema drift between pipeline runs
- Makes downstream analytics, validation, and loading steps safer and more predictable
- Serves as living documentation for anyone consuming the cleaned `.parquet` files

## Design Decisions

- **dtype_family** uses logical types only (`string` / `numeric` / `datetime`)
- Text columns use pandas nullable `string` dtype (supports `pd.NA`) instead of legacy `object`
- Numeric columns explicitly declare `numeric_type` (`Int64` or `Float64`)
- Some nulls are intentionally allowed (e.g. delivery timestamps on canceled orders)
- Foreign key relationships are declared for future validation and lineage tracking

## Relationship to Other Components

- **Schema Contract**: Defines the expected structure
- **enforce_schema.py**: Enforces the contract during cleaning (single source of truth)
- **Validation modules**: Check that the output matches the contract

## Usage

The contract is defined in `src/ecom_pipeline/config/schema_contract.py` and is used by:
- `enforce_schema.py` for type casting
- Validation scripts for post-cleaning checks