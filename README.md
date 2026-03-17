# Olist E-Commerce — End-to-End Data Engineering Pipeline

A portfolio data engineering project demonstrating a production-grade ELT pipeline
built on the Brazilian Olist e-commerce dataset (100K+ orders, 9 source tables).

## Business Context

Olist is a Brazilian marketplace connecting SME sellers to consumers. The raw data
arrives as 9 fragmented CSVs with no quality controls or analytical structure.
This pipeline transforms it into a queryable star schema with RFM customer
segmentation, revenue trend analysis, and automated data quality gates.

**Key finding:** Top 5% of customers (VIP + Big Spender segments) generate ~24%
of total GMV. November 2017 Black Friday peak: BRL 1.15M GMV in a single month.

## Target Audience

Data engineers and analysts who need a reference implementation of:
- ELT pipeline (extract → load raw → transform with dbt)
- Star schema design with derived business metrics
- Automated data quality validation (Great Expectations)
- Exploratory analysis with SQLAlchemy + Jupyter

## Architecture

![Architecture Diagram](docs/architecture_diagram.png)

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

    olist-pipeline/
    ├── ingest/load_olist.py               # Loads 9 CSVs → BigQuery olist_raw
    ├── dbt/olist_pipeline/models/
    │   ├── staging/                       # 7 stg_* views · type casting
    │   └── marts/                         # fact_orders · 4 dims · mart_customer_rfm
    ├── tests/ge_validation.py             # Great Expectations · 8/8 pass
    ├── notebooks/
    │   ├── 01_monthly_sales_trends.ipynb
    │   ├── 02_product_category_analysis.ipynb
    │   └── 03_customer_segmentation.ipynb
    └── docs/                              # Architecture diagram · charts · DAG

## Setup
```bash
git clone https://github.com/YOUR_USERNAME/olist-pipeline.git
cd olist-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
gcloud auth application-default login
```

## Running the Pipeline
```bash
# 1. Ingest raw data
python ingest/load_olist.py

# 2. Transform with dbt
cd dbt/olist_pipeline
dbt run --select staging.* marts.*
dbt test --select marts.*

# 3. Data quality validation
python tests/ge_validation.py

# 4. Exploratory analysis
jupyter notebook notebooks/
```

## Key Results

| Metric | Value |
|---|---|
| fact_orders rows | 112,650 |
| Unique customers (RFM) | 93,400 |
| Peak GMV month | Nov 2017 — BRL 1.15M |
| Top revenue category | health_beauty — BRL 1.41M |
| dbt tests | 28/29 PASS |
| GE expectations | 8/8 PASS |

## dbt Lineage

![dbt Lineage DAG](docs/dbt_lineage_dag.png)

## Data Source

[Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
· Kaggle · CC BY-NC-SA 4.0
