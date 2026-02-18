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

## Dataset Setup

This project expects the Olist CSV files to be placed in:

    data/raw/

You can download the dataset from Kaggle:

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

### Option 1 — Manual Download

1. Download the dataset from Kaggle.
2. Extract the CSV files.
3. Move all `.csv` files into:

    data/raw/

### Option 2 — Kaggle CLI (recommended)

Install Kaggle CLI:

```bash
pip install kaggle
```

Authenticate (place `kaggle.json` in `~/.kaggle/`).

Then run from project root:

```bash
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw --unzip
```

This will download and extract the required CSV files directly into `data/raw/`.


### Data Directories

```
data/raw/       # Original source files
data/interim/   # Optional intermediate artifacts
data/clean/     # Cleaned parquet outputs
data/samples/   # Smaller representative dataset for lightweight testing
```

The `data/samples/` directory contains a reduced dataset that mirrors the structure of the full dataset and can be used for faster development or demonstration.

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
│
├── docs/           # Project notes and documentation (currently empty)
│   ├── data_dictionary.md
│   └── vaidation_strategy.md
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

## Environment Setup

This project uses `uv` for environment and dependency management.

### Create virtual environment

```bash
uv venv
```

### Install project in editable mode

```bash
uv pip install -e .
```

This installs the `ecom_pipeline` package locally so modules can be executed without modifying `PYTHONPATH`.

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

3. **Standardize Columns**  
   Applies consistent column naming.

4. **Enforce Schema**  
   Applies explicit casting rules to produce clean parquet outputs.

5. **Validate Clean Schema**  
   Verifies data types match expectations.

6. **Audit Dtypes**  
   Flags suspicious type patterns using heuristics.

---

## Outputs

- `data/clean/*.parquet`
- `reports/clean_schema_audit.csv`
- `reports/clean_dtypes_full.csv`
- `reports/clean_dtypes_flags.csv`
- `reports/clean_contract_audit.csv`

---

## Skills Demonstrated

- Python package architecture (`src/` layout)
- Schema-driven data cleaning
- Defensive data validation
- Reproducible execution environments (`uv`)
- Logging and structured reporting
- Clean project organization for portfolio use

---

## Future Improvements

- Replace orchestration with `dlt`
- Add primary key validation checks
- Introduce CI for automated validation
- Expand schema contract enforcement
