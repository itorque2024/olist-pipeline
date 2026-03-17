import pandas as pd
from google.cloud import bigquery
import os, time

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET    = "olist_raw"
DATA_DIR   = "data/raw"

# CSV filename → BigQuery table name
FILES = {
    "olist_orders_dataset.csv":              "orders",
    "olist_order_items_dataset.csv":         "order_items",
    "olist_order_payments_dataset.csv":      "order_payments",
    "olist_order_reviews_dataset.csv":       "order_reviews",
    "olist_customers_dataset.csv":           "customers",
    "olist_products_dataset.csv":            "products",
    "olist_sellers_dataset.csv":             "sellers",
    "olist_geolocation_dataset.csv":         "geolocation",
    "product_category_name_translation.csv": "category_translation",
}

client = bigquery.Client(project=PROJECT_ID)

def load_csv(filename, table_name):
    filepath = os.path.join(DATA_DIR, filename)
    print(f"Loading {filename} → {DATASET}.{table_name}...")

    # Geolocation has 1M rows — deduplicate on zip prefix first
    if table_name == "geolocation":
        df = pd.read_csv(filepath).drop_duplicates(
            subset=["geolocation_zip_code_prefix"]
        )
        print(f"  Deduped geolocation: {len(df):,} unique zip prefixes")
    else:
        df = pd.read_csv(filepath)

    table_id  = f"{PROJECT_ID}.{DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    print(f"  ✅ {table.num_rows:,} rows loaded")
    return table.num_rows

if __name__ == "__main__":
    print(f"Ingesting Olist → BigQuery project: {PROJECT_ID}")
    print("=" * 60)
    total = 0
    for filename, table_name in FILES.items():
        total += load_csv(filename, table_name)
        time.sleep(1)
    print("=" * 60)
    print(f"Done! Total rows: {total:,}")
    print("Expected: ~627,000 rows across 9 tables")