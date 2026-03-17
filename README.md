# FIPE pricing pipeline (WIP)

This repo contains a prototype pipeline to support FIPE pricing decisions for used phones.

## What it does (high level)
- Pulls recent offers from a Google Sheet (Intercept pricing sheet)
- Builds a list of product + condition keys
- Queries a BigQuery view with engagement + conversion metrics
- Produces a pricing output (price calculation still WIP)
- Appends results to an output sheet

## Core files
- `Context.md`: business context and definitions
- `definitions.py`: main functions (Sheets, BigQuery, helpers)
- `combined_offer_metrics.sql`: view query used as the base dataset
- `config.yaml`: runtime configuration
- `test.py`: local test runner

## Current status
- Sheets input + key extraction is working
- BigQuery query + filtering by product/deflators is working
- Output append to a separate sheet is working
- Price calculation logic is not implemented yet

## Quick start (local)
1) Create/activate a Python venv
2) Install deps:
   - `pip install pandas pyyaml google-api-python-client google-auth google-cloud-bigquery db-dtypes`
3) Configure `config.yaml`
4) Run:
   - `python test.py`
