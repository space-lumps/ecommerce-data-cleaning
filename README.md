# E-commerce Data Cleaning (Olist)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/space-lumps/ecommerce-data-cleaning/actions/workflows/ci.yml/badge.svg)](https://github.com/space-lumps/ecommerce-data-cleaning/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![Release](https://img.shields.io/github/v/release/space-lumps/ecommerce-data-cleaning?include_prereleases)](https://github.com/space-lumps/ecommerce-data-cleaning/releases/latest)


## Table of Contents

- [Objective](#objective)
- [Setup](#setup)
- [Project Structure](#project-structure)
- [Pipeline](#pipeline)
- [Skills Demonstrated](#skills-demonstrated)
- [Challenges and Insights](#challenges-and-learnings)
- [API Documentation](#api-documentation)
- [License](#license)

## Quick Start

```bash
git clone https://github.com/space-lumps/ecommerce-data-cleaning.git
cd ecommerce-data-cleaning

# Create & activate virtual environment (recommended: uv)
uv venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows

# Install dependencies + editable package
uv pip install -e .

# Copy sample data (no Kaggle needed)
cp data/samples/*.csv data/raw/

# Run the full pipeline
uv run python run_pipeline.py
```

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

## Visualizations

### Exploratory Analysis: Revenue by Brazilian State

Early Tableau choropleth map created to validate the cleaned dataset:

![Revenue by Brazilian State](assets/images/sales-revenue-by-brazilian-state.png)

- Shows total revenue concentration across Brazilian states (darker shades = higher revenue).
- Highlights strong market dominance by São Paulo.
- Built using aggregated `price` and `customer_state` from the cleaned data.

### 2017 Revenue & Sales Dashboard

Polished interactive dashboard summarizing key findings from the 2017 Olist dataset:

![2017 Brazilian E-commerce Revenue & Sales Dashboard](assets/images/2017-brazilian-ecom-revenue-dash.png)

- KPIs: Total Revenue, Unique Customers, Total Orders
- Monthly order trend with seasonality insights
- State-level revenue distribution and top performers
- Key insights and actionable business recommendations

---

## Setup

### Clone the repository

```bash
git clone https://github.com/space-lumps/ecommerce-data-cleaning.git
cd ecommerce-data-cleaning
```

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
├── .github/
│   └── workflows/
│       └── ci.yml
├── data/
│   ├── clean/
│   ├── interim/
│   ├── raw/
│   └── samples/
│       └── [sample CSV files]     # e.g., olist_orders_dataset.csv (truncated for brevity)
├── docs/
│   ├── api/
│   │   └── [generated API docs]   # e.g., index.html (via pdoc)
│   ├── data_dictionary.md
│   ├── schema_contract.md
│   └── validation_strategy.md
├── reports/
│   └── [generated reports]        # e.g., raw_profile.csv (generated at runtime)
├── src/
│   └── ecom_pipeline/
│       ├── __init__.py
│       ├── config/
│       │   ├── __init__.py
│       │   └── schema_contract.py
│       ├── pipeline/
│       │   ├── __init__.py
│       │   ├── audit_all_clean_dtypes.py
│       │   ├── enforce_schema.py
│       │   ├── generate_data_dictionary.py
│       │   ├── profile_raw.py
│       │   ├── sanity_check_raw.py
│       │   ├── standardize_columns.py
│       │   ├── validate_clean_schema.py
│       │   └── validate_schema_contract.py
│       └── utils/
│           ├── __init__.py
│           ├── io.py
│           └── logging.py
├── tests/
│   ├── test_io.py                 # smoke tests for io utils
│   └── test_pipeline_e2e.py       # end-to-end pipeline smoke test
├── .pre-commit-config.yaml
├── .gitignore
├── .ruff.toml
├── LICENSE
├── README.md
├── pyproject.toml
├── requirements-lock.txt
├── requirements.txt
├── run_pipeline.py
└── uv.lock
```
The project follows a proper `src/` layout.  
All reusable code lives inside the `ecom_pipeline` package.
Tests live in a top-level `tests/` directory.

---

## Dependency Files

- `pyproject.toml` — Defines the installable package and dependencies.
- `uv.lock` — Locked dependency graph for reproducible environments (used by `uv`).
- `requirements.txt` — Traditional dependency list (optional compatibility).
- `requirements-lock.txt` — Pinned dependency versions (optional compatibility).

For this project, `uv` + `pyproject.toml` + `uv.lock` are the authoritative installation method.

---

## Pipeline

### Execution

Run the full pipeline:

```bash
uv run python run_pipeline.py
```

Run individual modules:

```bash
uv run python -m ecom_pipeline.pipeline.sanity_check_raw
uv run python -m ecom_pipeline.pipeline.profile_raw
uv run python -m ecom_pipeline.pipeline.standardize_columns
uv run python -m ecom_pipeline.pipeline.enforce_schema
uv run python -m ecom_pipeline.pipeline.validate_clean_schema
uv run python -m ecom_pipeline.pipeline.audit_all_clean_dtypes
uv run python -m ecom_pipeline.pipeline.validate_schema_contract
uv run python -m ecom_pipeline.pipeline.generate_data_dictionary
```

---

### Pipeline Stages

1. **Sanity Check Raw**
  Confirms raw files exist and are readable.
2. **Profile Raw**
  Profiles source datasets before transformation.
3. **Standardize Columns**
  Applies consistent column naming.
4. **Enforce Schema**
Applies explicit casting rules (including strict `datetime64[ns]` handling) to produce clean parquet outputs.
5. **Validate Clean Schema**
  Verifies data types match expectations.
6. **Audit Dtypes**
  Flags suspicious type patterns using heuristics.
7. **Validate Schema Contract**
  Enforces required columns, primary key uniqueness, and logical dtype guarantees.
8. **Generate Data Dictionary**  
  Generates `docs/data_dictionary.md` from `reports/clean_dtypes_full.csv`.

---

### Outputs

- `data/clean/*.parquet`
- `reports/raw_profile.csv`
- `reports/clean_schema_audit.csv`
- `reports/clean_dtypes_full.csv`
- `reports/clean_dtypes_flags.csv`
- `reports/clean_contract_audit.csv`
- `docs/data_dictionary.md`

---

## Validation & Schema Enforcement

The pipeline now features significantly improved schema enforcement and type handling:

### Key Improvements in This Update

- **Brazilian CEP (zip code) prefixes**: All `*_zip_code_prefix` columns are now preserved as 5-digit strings with leading zeros (e.g. `01001` instead of `1001`). This ensures accurate joins with the geolocation table and correct location-based analysis.
- **State names**: Added `customer_state_name` with full English names (e.g. "São Paulo", "Rio de Janeiro") to improve compatibility with Looker Studio Filled maps and make visualizations more readable.
- **Explicit nullable types**: Integer columns are now consistently cast to `Int64`, floats to `Float64`, and strings to the modern nullable `string` dtype.
- Prevents pandas from silently converting integer columns to `float64` when missing values (NaNs) are present.
- **Strict `datetime64[ns]` handling** for all timestamp columns to ensure correct `TIMESTAMP` / `DATETIME` types in BigQuery (fixed Parquet logical type metadata issue).
- Much cleaner and more reliable types when loading into BigQuery and Looker Studio (especially `INT64` instead of `FLOAT64`, `TIMESTAMP` instead of `INTEGER`).
- Improved `dtype_family` logic and display cleaning so reports and the data dictionary are consistent.
- Updated `generate_data_dictionary.py` to use the clean audit instead of raw profile.

### Pipeline Stages (Updated)
1. Sanity Check Raw  
2. Profile Raw  
3. **Enforce Schema** (`enforce_schema.py`) – applies nullable dtypes  
4. **Validate Clean Schema** (`validate_clean_schema.py`)  
5. **Audit Dtypes** (`audit_all_clean_dtypes.py`) – now includes null statistics  
6. Generate Data Dictionary (now based on cleaned data)  
7. Schema Contract Validation

### Outputs
- `data/clean/*.parquet` – production-ready files with correct nullable types
- `docs/data_dictionary.md` – living, accurate documentation of the final schema
- `reports/clean_dtypes_full.csv` – detailed audit with null counts/percentages
- `reports/clean_schema_audit.csv` – pass/fail validation of expected types

This refactor makes the cleaning pipeline more robust, reproducible, and visualization-ready.

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

## Challenges and Insights

This project involved iterating on a real-world data cleaning pipeline, revealing several practical lessons in data engineering:

- **Handling Inconsistent Data Types in Schema Validation**: Encountered nuances with Python/Pandas dtypes like "str" vs. "string" vs. "object"—e.g., CSV imports defaulting to "object" required explicit coercion in `enforce_schema.py` to match expected schemas, preventing downstream errors in analysis or ML workflows. This highlighted the importance of strict type enforcement early in pipelines.
- **Balancing Reproducibility and Simplicity**: Setting up a virtual environment with `uv` for fast, locked dependencies was straightforward, but integrating environment variables (e.g., for data dirs) taught me about flexible config without hardcoding paths.
- **Modular Design for Maintainability**: Structuring as a package with separate scripts for extraction, validation, and auditing improved testability, but required careful import management to avoid circular dependencies.
- **Testing Real-World Data Quirks**: Sample CSVs had edge cases like missing values or inconsistent formats, reinforcing the need for audits and E2E tests to catch issues that unit tests might miss.
- **Automation Trade-offs**: Implementing CI with GitHub Actions automated checks, but debugging workflow failures (e.g., env var mismatches) emphasized clear logging and isolation in tests.

These experiences strengthened my approach to building robust, scalable data pipelines.

---

## Future Improvements

- Add foreign key integrity validation (cross-table checks – partially implemented)
- Add domain/value constraints (e.g., non-negative price, valid order_status domain)
- Optional: Add test coverage reporting to CI (pytest-cov)
- Optional: Containerize with Dockerfile
- Explore dlt for declarative orchestration (in a separate branch)

---

## API Documentation

The project is structured as an installable Python package (`ecom_pipeline`).

Full API reference (auto-generated from docstrings):  
→ [View API Documentation](docs/api/index.html)

---

## License

MIT License

Copyright (c) 2026 Corin Stedman

See the [LICENSE](LICENSE) file for details.