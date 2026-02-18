# Schema Contract

## Purpose

Define structural guarantees for cleaned datasets.

## Validations Enforced

- Required columns
- Primary key uniqueness
- Expected dtype family (logical types)
- Dataset-level structural integrity

## Why This Matters

Prevents silent schema drift and ensures downstream analytics safety.

## Relationship to CAST_RULES

- CAST_RULES: enforces type casting during cleaning
- Schema Contract: validates final structural correctness
