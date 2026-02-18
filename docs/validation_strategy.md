# Validation Strategy

This project enforces a clean, reproducible dataset by combining schema casting, validation checks, and audits.

---

## Goals

- Produce `data/clean/*.parquet` with stable, expected dtypes.
- Detect schema drift early (missing columns, unexpected dtypes).
- Generate auditable artifacts under `reports/` to support review and debugging.

---

## Validation Layers

### 1) Raw profiling (observability)

**Script:** `ecom_pipeline.pipeline.profile_raw`  
**Output:** `reports/raw_profile.csv`

Profiles each raw CSV at the column level:

- `dtype` (pandas inferred dtype)
- `null_count`
- `null_pct`

Purpose:
- establish baseline expectations
- identify sparse fields and null-heavy columns
- support downstream documentation generation

---

### 2) Schema enforcement (cleaning + typing)

**Script:** `ecom_pipeline.pipeline.enforce_schema`  
**Inputs:** `data/raw/*.csv`  
**Outputs:** `data/clean/*.parquet`

Applies deterministic casting rules (`CAST_RULES`) to convert raw CSVs into clean parquet tables with consistent dtypes.

Core guarantees:
- ID-like and categorical fields cast to string family
- datetime fields cast to datetime
- measure fields cast to numeric

---

### 3) Schema validation (contract verification)

**Script:** `ecom_pipeline.pipeline.validate_clean_schema`  
**Input:** `data/clean/*.parquet`  
**Output:** `reports/clean_schema_audit.csv`

Verifies that the clean parquet outputs match the expected dtype families defined in `CAST_RULES`:

- string columns must be in the string family
- datetime columns must be datetime
- numeric columns must be numeric

A non-zero fail count indicates schema mismatch and should be treated as a pipeline failure.

---

### 4) Heuristic dtype audit (anomaly detection)

**Script:** `ecom_pipeline.pipeline.audit_all_clean_dtypes`  
**Input:** `data/clean/*.parquet`  
**Outputs:**
- `reports/clean_dtypes_full.csv`
- `reports/clean_dtypes_flags.csv`

Scans all columns and flags suspicious patterns such as:

- ID/ZIP-like fields not in string family
- date-like fields not in datetime family
- measure-like fields stored as string/object

This is designed to catch unexpected issues not explicitly covered by the schema rules.

---

## Audit Artifacts

- `reports/raw_profile.csv`: raw baseline profiling
- `reports/clean_schema_audit.csv`: schema validation results
- `reports/clean_dtypes_full.csv`: full dtype inventory of clean outputs
- `reports/clean_dtypes_flags.csv`: filtered suspicious columns only

These files are intended to be small, reviewable, and commit-safe.

---

## Future Improvements

- Add primary key and uniqueness checks per table.
- Add row-count reconciliation from raw → clean.
- Replace orchestration with `dlt` while retaining the same validation layers.
