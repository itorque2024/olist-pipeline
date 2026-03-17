from dagster import asset, AssetExecutionContext, Output
import subprocess, os

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "sg-20min-town-mod2proj")

@asset(group_name="ingestion",
       description="Load all 9 Olist CSVs into BigQuery olist_raw")
def raw_olist_tables(context: AssetExecutionContext):
    result = subprocess.run(
        ["python", "ingest/load_olist.py"],
        capture_output=True, text=True, cwd=os.getcwd()
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Ingestion failed: {result.stderr}")
    return Output(value=True, metadata={"status": "9 tables loaded"})


@asset(group_name="transformation", deps=[raw_olist_tables],
       description="Run dbt staging + mart models to build star schema")
def dbt_star_schema(context: AssetExecutionContext):
    dbt_dir = os.path.join(os.getcwd(), "dbt", "olist_pipeline")
    result  = subprocess.run(
        ["dbt", "run", "--select", "staging.* marts.*"],
        capture_output=True, text=True, cwd=dbt_dir
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")
    return Output(value=True, metadata={"models": "staging + marts"})


@asset(group_name="quality", deps=[dbt_star_schema],
       description="Run dbt schema tests against all mart models")
def dbt_tests(context: AssetExecutionContext):
    dbt_dir = os.path.join(os.getcwd(), "dbt", "olist_pipeline")
    result  = subprocess.run(
        ["dbt", "test", "--select", "marts.*"],
        capture_output=True, text=True, cwd=dbt_dir
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt tests failed: {result.stderr}")
    return Output(value=True, metadata={"status": "all tests passed"})


@asset(group_name="quality", deps=[dbt_star_schema],
       description="Run Great Expectations business logic validation suite")
def great_expectations_validation(context: AssetExecutionContext):
    result = subprocess.run(
        ["python", "tests/ge_validation.py"],
        capture_output=True, text=True, cwd=os.getcwd()
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"GE validation failed: {result.stderr}")
    return Output(value=True, metadata={"status": "8/8 GE expectations passed"})
