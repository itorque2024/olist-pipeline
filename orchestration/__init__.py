from dagster import Definitions
from orchestration.assets import (
    raw_olist_tables,
    dbt_star_schema,
    dbt_tests,
    great_expectations_validation,
)

defs = Definitions(assets=[
    raw_olist_tables,
    dbt_star_schema,
    dbt_tests,
    great_expectations_validation,
])
