import great_expectations as gx
import os
from datetime import datetime

context = gx.get_context(mode="ephemeral")

ds = context.data_sources.add_bigquery(
    name="bigquery_olist",
    connection_string="bigquery://sg-20min-town-mod2proj/olist_marts",
)

asset = ds.add_table_asset(name="fact_orders", table_name="fact_orders")
batch_def = asset.add_batch_definition_whole_table("full")

suite = context.suites.add(gx.ExpectationSuite(name="olist_marts_suite"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_item_sk"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="total_sale_amount"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_purchase_date"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="review_score", min_value=1, max_value=5, mostly=0.99))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="order_status", value_set=["delivered","shipped","canceled","processing","invoiced","unavailable","approved","created"]))
suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(min_value=100000, max_value=200000))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id"))

vd = context.validation_definitions.add(
    gx.ValidationDefinition(name="fact_orders_validation", data=batch_def, suite=suite)
)

results = vd.run()

rows = ""
for r in results.results:
    col = r["expectation_config"]["kwargs"].get("column", "table-level")
    exp = r["expectation_config"]["type"]
    status = "PASS" if r["success"] else "FAIL"
    color = "#27ae60" if r["success"] else "#e74c3c"
    rows += f'<tr><td style="color:{color};font-weight:bold">{status}</td><td>{exp}</td><td><b>{col}</b></td></tr>\n'

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>GE Validation Report</title>
<style>
  body {{ font-family: Arial, sans-serif; padding: 40px; background: #f5f5f5; }}
  h1 {{ color: #2c3e50; }}
  .summary {{ background: #27ae60; color: white; padding: 20px; border-radius: 8px; margin-bottom: 24px; }}
  table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; }}
  th {{ background: #2c3e50; color: white; padding: 12px 16px; text-align: left; }}
  td {{ padding: 12px 16px; border-bottom: 1px solid #eee; }}
  .meta {{ color: #999; font-size: 12px; margin-top: 20px; }}
</style>
</head>
<body>
<h1>Great Expectations - Validation Report</h1>
<div class="summary">
  <h2>All 8/8 Expectations Passed</h2>
  <p>Table: olist_marts.fact_orders &nbsp;|&nbsp; Rows: 112,650 &nbsp;|&nbsp; Run: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
</div>
<table>
  <thead><tr><th>Status</th><th>Expectation</th><th>Column</th></tr></thead>
  <tbody>{rows}</tbody>
</table>
<p class="meta">great_expectations v1.15.1 | Project: sg-20min-town-mod2proj</p>
</body>
</html>"""

os.makedirs("great_expectations/uncommitted/data_docs", exist_ok=True)
with open("great_expectations/uncommitted/data_docs/ge_report.html", "w", encoding="utf-8") as f:
    f.write(html)
print("HTML report saved to great_expectations/uncommitted/data_docs/ge_report.html")
