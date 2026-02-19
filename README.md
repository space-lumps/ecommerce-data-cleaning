# E-commerce Data Cleaning (Olist)

## Objective

Build a reproducible, production-style data cleaning and validation pipeline for the Olist e-commerce dataset.

This project demonstrates:

- Structured modular pipeline design
- Explicit schema enforcement
- Data type auditing and validation
- Reproducible execution using `uv`
- Clean `src/` package architecture
- Module-based execution (`python -m`)

---

## Setup

### Clone the repository

```bash
git clone https://github.com/space-lumps/ecommerce-data-cleaning.git
cd ecommerce-data-cleaning
```

---

## Environment Setup (uv)

```bash
uv venv
uv pip install -e .
```
This installs the ecom_pipeline package locally so modules can be executed without modifying `PYTHONPATH`.

---

## Dataset Setup

The pipeline can run using either the included sample dataset or the full Kaggle dataset.

### Option A — Use included sample dataset (no Kaggle required)

1. Copy sample CSVs into `data/raw/`:

```bash
cp data/samples/*.csv data/raw/
```
Ensure `data/raw/` contains only one dataset version (either samples or full Kaggle data).

Run the pipeline:

```bash
uv run python run_pipeline.py
```

### Option B - Download full dataset from Kaggle

Expected location:

```text
data/raw/
```

### Manual Download

1. Download the dataset from Kaggle: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
2. Extract the CSV files.
3. Move all `.csv` files into: `data/raw/`

### Kaggle CLI (recommended)

Install Kaggle CLI:

```bash
pip install kaggle
```

Place `kaggle.json` in `~/.kaggle/`, then run:

```bash
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw --unzip
```

---

### Data Directories

```
data/raw/       # Original source files
data/interim/   # Optional intermediate artifacts
data/clean/     # Cleaned parquet outputs
data/samples/   # Static lightweight dataset (no Kaggle required)
```

---

## Project Structure

```
ecommerce-data-cleaning/
│
├── data/
│   ├── raw/
│   ├── interim/
│   ├── clean/
│   └── samples/
│
├── reports/
├── tools/
├── docs/
│   ├── data_dictionary.md
│   ├── validation_strategy.md
│   └── schema_contract.md
│
├── src/
│   └── ecom_pipeline/
│       ├── config/
│       ├── validation/
│       ├── utils/
│       └── pipeline/
│
├── run_pipeline.py
├── pyproject.toml
├── requirements.txt
├── requirements-lock.txt
├── uv.lock
└── README.md
```
The project follows a proper `src/` layout.  
All reusable code lives inside the `ecom_pipeline` package.

---


---

## Dependency Files

- `pyproject.toml` — Defines the installable package and dependencies.
- `uv.lock` — Locked dependency graph for reproducible environments (used by `uv`).
- `requirements.txt` — Traditional dependency list (optional compatibility).
- `requirements-lock.txt` — Pinned dependency versions (optional compatibility).

For this project, `uv` + `pyproject.toml` + `uv.lock` are the authoritative installation method.

---

## Execution

Run individual modules:

```bash
uv run python -m ecom_pipeline.pipeline.sanity_check_raw
uv run python -m ecom_pipeline.pipeline.profile_raw
uv run python -m ecom_pipeline.pipeline.generate_data_dictionary
uv run python -m ecom_pipeline.pipeline.standardize_columns
uv run python -m ecom_pipeline.pipeline.enforce_schema
uv run python -m ecom_pipeline.pipeline.validate_clean_schema
uv run python -m ecom_pipeline.pipeline.audit_all_clean_dtypes
uv run python -m ecom_pipeline.pipeline.validate_schema_contract
```

Run the full pipeline:

```bash
uv run python run_pipeline.py
```

---

## Pipeline Stages

1. **Sanity Check Raw**
  Confirms raw files exist and are readable.
2. **Profile Raw**
  Profiles source datasets before transformation.
3. **Generate Data Dictionary**  
  Generates `docs/data_dictionary.md` from `reports/raw_profile.csv`.
4. **Standardize Columns**
  Applies consistent column naming.
5. **Enforce Schema**
  Applies explicit casting rules to produce clean parquet outputs.
6. **Validate Clean Schema**
  Verifies data types match expectations.
7. **Audit Dtypes**
  Flags suspicious type patterns using heuristics.
8. **Validate Schema Contract**
  Enforces required columns, primary key uniqueness, and logical dtype guarantees.

---

## Outputs

- `data/clean/*.parquet`
- `docs/data_dictionary.md`
- `reports/raw_profile.csv`
- `reports/clean_schema_audit.csv`
- `reports/clean_dtypes_full.csv`
- `reports/clean_dtypes_flags.csv`
- `reports/clean_contract_audit.csv`

---

## Validation & Schema Enforcement

The pipeline enforces structural guarantees after cleaning.

### 1. Deterministic Type Casting

- Implemented in `enforce_schema.py`
- Ensures consistent logical dtypes (str, datetime, numeric)

### 2. Clean Schema Validation

- Implemented in `validate_clean_schema.py`
- Verifies expected dtype families after casting

### 3. Schema Contract Validation

- Implemented in `validate_schema_contract.py`
- Enforces:
  - Required columns
  - Primary key uniqueness
  - Logical dtype expectations
  - Structural dataset integrity

If any contract rule fails, the dataset is considered invalid.

---

## Skills Demonstrated

- Python package architecture (`src/` layout)
- Schema-driven data cleaning
- Defensive data validation
- Reproducible execution environments (`uv`)
- Logging and structured reporting
- Implemented E2E testing with pytest and CI via GitHub Actions for automated validation on samples
- Clean project organization for portfolio use

---

## Challenges and Learnings

This project involved iterating on a real-world data cleaning pipeline, revealing several practical lessons in data engineering:

- **Handling Inconsistent Data Types in Schema Validation**: Encountered nuances with Python/Pandas dtypes like "str" vs. "string" vs. "object"—e.g., CSV imports defaulting to "object" required explicit coercion in `enforce_schema.py` to match expected schemas, preventing downstream errors in analysis or ML workflows. This highlighted the importance of strict type enforcement early in pipelines.
- **Balancing Reproducibility and Simplicity**: Setting up a virtual environment with `uv` for fast, locked dependencies was straightforward, but integrating environment variables (e.g., for data dirs) taught me about flexible config without hardcoding paths.
- **Modular Design for Maintainability**: Structuring as a package with separate scripts for extraction, validation, and auditing improved testability, but required careful import management to avoid circular dependencies.
- **Testing Real-World Data Quirks**: Sample CSVs had edge cases like missing values or inconsistent formats, reinforcing the need for audits and E2E tests to catch issues that unit tests might miss.
- **Automation Trade-offs**: Implementing CI with GitHub Actions automated checks, but debugging workflow failures (e.g., env var mismatches) emphasized clear logging and isolation in tests.

These experiences strengthened my approach to building robust, scalable data pipelines.

---

## Future Improvements

- Add foreign key integrity validation (cross-table checks)
- Add domain/value constraints (e.g., non-negative price, valid order_status domain)
- Add CI pipeline (GitHub Actions) to run validation automatically
- Optional: Integrate `dlt` for declarative pipeline orchestration