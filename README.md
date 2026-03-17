# Olist E-Commerce — End-to-End Data Engineering Pipeline

Module 2 Data Engineering Project · Singapore LTMP 2040

## Architecture

![Architecture Diagram](docs/architecture_diagram.png)

## Pipeline Overview

Raw CSV data from Kaggle → Python ingest → BigQuery → dbt star schema → Great Expectations quality checks → Jupyter EDA → Streamlit dashboard.

## Stack

| Layer | Tool |
|---|---|
| Storage | BigQuery (GCP Free Tier) |
| Transformation | dbt Core 1.11 |
| Data Quality | Great Expectations 1.15 |
| Analysis | Jupyter + SQLAlchemy + Plotly |
| Orchestration | Dagster OSS (optional+) |
| Dashboard | Streamlit Community Cloud |
| Language | Python 3.11 |

## Project Structure
