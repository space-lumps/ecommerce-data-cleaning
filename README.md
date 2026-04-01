# E-commerce Data Cleaning (Olist)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/space-lumps/ecommerce-data-cleaning/actions/workflows/ci.yml/badge.svg)](https://github.com/space-lumps/ecommerce-data-cleaning/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![Release](https://img.shields.io/github/v/release/space-lumps/ecommerce-data-cleaning?include_prereleases)](https://github.com/space-lumps/ecommerce-data-cleaning/releases/latest)


## Table of Contents

- [Objective](#objective)
- [Visualizations](#visualizations)
- [Setup](#setup)
- [Dataset Setup](#dataset-setup)
- [Project Structure](#project-structure)
- [Pipeline](#pipeline)
- [Validation & Schema Enforcement](#validation--schema-enforcement)
- [Skills & Key Learnings](#skills--key-learnings)
- [Future Improvements](#future-improvements)
- [API Documentation](#api-documentation)
- [License](#license)

## Objective

Build a reproducible, production-style data cleaning, validation, and ingestion pipeline for the Olist Brazilian e-commerce dataset. Raw CSVs are transformed into clean, schema-enforced Parquet files optimized for BigQuery and downstream analytics.

**Key enhancement (merged from `feature/improve-schema-enforcement` branch):**  
Strengthened deterministic type casting and explicit schema enforcement during Parquet serialization. This gives full control over column types and eliminates common BigQuery import errors caused by weak/ambiguous typing (e.g., `object` → `STRING` coercion failures).

The resulting Parquet files now power reliable analysis and an interactive **Looker Studio dashboard** showing key revenue and order metrics across Brazilian states and product categories.

This project demonstrates modern data engineering and analytics practices:
- Modular `src/` package layout
- Explicit schema contracts and validation
- Automated CI testing + post-ingestion verification
- Reproducible environments via `uv`

---

## Visualizations

### Exploratory Analysis: Revenue by Brazilian State

Early Tableau choropleth map created to validate the cleaned dataset:

<p align="left">
  <img src="assets/images/sales-revenue-by-brazilian-state.png" alt="Tableau Map - Revenue by Brazilian State" width="55%">
</p>

- Shows revenue concentration across states (darker = higher revenue)
- Highlights strong market dominance by São Paulo.
- Built from aggregated `price` and `customer_state`

### 2017 Revenue & Sales Dashboard

Polished interactive dashboard summarizing key findings from the 2017 Olist dataset:

![2017 Brazilian E-commerce Revenue & Sales Dashboard](assets/images/2017-brazilian-ecom-revenue-dash.png)

- KPIs: Total Revenue, Unique Customers, Total Orders
- Monthly order trend with seasonality insights
- State-level revenue distribution and top performers
- Key insights and actionable business recommendations

---

## Setup

```bash
git clone https://github.com/space-lumps/ecommerce-data-cleaning.git
cd ecommerce-data-cleaning

uv venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate

uv pip install -e .

# Use sample data (no Kaggle needed)
cp data/samples/*.csv data/raw/
uv run python run_pipeline.py
```

## Dataset Setup

The pipeline can run using either the included sample dataset or the full Kaggle dataset.

### Option A — Sample Data (recommended for quick start, no Kaggle needed)

Copy the included samples and run the pipeline:

```bash
cp data/samples/*.csv data/raw/
uv run python run_pipeline.py
```

### Option B - Download full dataset from Kaggle

Download from [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place all `.csv` files in `data/raw/`.

Using Kaggle CLI (fastest):

```bash
pip install kaggle
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw --unzip
```

Ensure `data/raw/` contains only one version of the data at a time.

---

## Project Structure

```text
ecommerce-data-cleaning/
├── src/ecom_pipeline/          # All reusable code (installable package)
├── data/samples/               # Lightweight test data (included)
├── docs/                       # schema_contract.md, data_dictionary.md, etc.
├── reports/                    # Generated audits and profiles
├── tests/                      # E2E and IO smoke tests
├── .github/workflows/ci.yml
├── pyproject.toml + uv.lock    # Reproducible environment
└── run_pipeline.py
```

The project follows a proper `src/` layout.  
All reusable code lives inside the `ecom_pipeline` package.
Tests live in a top-level `tests/` directory.

---

## Pipeline

### Execution

```bash
uv run python run_pipeline.py
```

### Pipeline Stages

1. **Sanity Check Raw**
  Confirms raw files exist and are readable.
2. **Profile Raw**
  Profiles source datasets before transformation.
3. **Standardize Columns**
  Applies consistent column naming.
4. **Enforce Schema**
  Applies explicit type casting using SCHEMA_CONTRACT as the single source of truth.
6. **Audit Dtypes**
  Flags suspicious type patterns and generates detailed reports.
7. **Validate Schema Contract**
  Comprehensive validation (required columns, dtypes, nullability, PK uniqueness, FK integrity, domain constraints)
8. **Generate Data Dictionary**
  Generates `docs/data_dictionary.md` from `reports/clean_dtypes_full.csv`.

---

## Validation & Schema Enforcement

This pipeline features strong type safety and relational integrity for BigQuery compatibility:

Key Improvements

- All type casting is driven by `SCHEMA_CONTRACT` as the single source of truth
- Strict nullable dtypes (`string`, `Int64`, `Float64`, `datetime64[ns]`)
- Brazilian CEP zip codes preserved with leading zeros
- Full English state names added for better visualization support
- Cross-table foreign key integrity checks with orphan detection
- Automatic enrichment of product_category_name_translation table with missing categories from products (e.g. `pc_gamer`, `portateis_cozinha_e_preparadores_de_alimentos`)

These changes eliminate common import failures and produce cleaner, more reliable outputs for analysis and visualization.

### Outputs
- `data/clean/*.parquet` – production-ready files with correct nullable types
- `docs/data_dictionary.md` – living, accurate documentation of the final schema
- `reports/clean_contract_audit.csv` – comprehensive contract validation (required columns, dtypes, constraints, FK integrity)
- `reports/clean_dtypes_full.csv` – detailed audit with null counts/percentages
- `reports/clean_dtypes_flags.csv` – flagged suspicious columns for manual review

### Schema Contract

A declarative schema contract (`src/ecom_pipeline/config/schema_contract.py`) defines the expected structure of every cleaned dataset and serves as the **single source of truth** for type enforcement.

It specifies:
- Required columns and primary keys
- Logical data types (`string`, `numeric`, `datetime`)
- Nullable rules and domain constraints (including `numeric_type`: `Int64` or `Float64`)
- Foreign key relationships (fully enforced with cross-table referential integrity checks)

This contract is enforced by `enforce_schema.py` and validated by `validate_schema_contract.py`, ensuring consistency and preventing schema drift.

---

## Skills & Key Learnings

- Modular Python package with proper `src/` layout for maintainability
- Strict schema enforcement + deterministic type casting
- Cross-table foreign key validation with automatic data enrichment
- Defensive validation + CI testing
- Reproducible environments with `uv`
- Production-ready data pipeline powering a Looker Studio dashboard

**Key Learnings**
- Explicit type casting early prevents BigQuery import failures and silent data issues
- Handling real-world data quirks (incomplete lookup tables) is critical for clean relational pipelines
- Clear schema contracts + validation save significant debugging time

---

## Future Improvements

- Expand domain-specific validation rules (e.g. price ≥ 0, valid order_status values)
- Increase test coverage for edge cases

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