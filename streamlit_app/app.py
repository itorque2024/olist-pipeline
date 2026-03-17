import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.express as px
import os

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "sg-20min-town-mod2proj")
st.set_page_config(page_title="Olist Analytics", layout="wide")

@st.cache_data(ttl=3600)
def run_query(query):
    client = bigquery.Client(project=PROJECT_ID)
    return client.query(query).to_dataframe(create_bqstorage_client=False)

page = st.sidebar.selectbox(
    "Page", ["KPI Overview", "Product Analysis", "Customer Segments"])

# ── PAGE 1: KPI Overview
if page == "KPI Overview":
    st.title("Olist E-Commerce — KPI Overview")
    kpis = run_query(f"""
        SELECT
            COUNT(DISTINCT order_id)          AS total_orders,
            ROUND(SUM(total_sale_amount), 0)  AS total_gmv,
            ROUND(AVG(total_sale_amount), 2)  AS avg_order_value,
            ROUND(AVG(review_score), 2)        AS avg_review_score,
            COUNTIF(is_late_delivery) * 100.0
                / COUNT(*)                    AS late_pct
        FROM `{PROJECT_ID}.olist_marts.fact_orders`
        WHERE order_status = 'delivered'
    """)
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Total Orders",    f"{kpis.total_orders[0]:,}")
    c2.metric("Total GMV",       f"BRL {kpis.total_gmv[0]:,.0f}")
    c3.metric("Avg Order Value", f"BRL {kpis.avg_order_value[0]:,.2f}")
    c4.metric("Avg Review",      f"{kpis.avg_review_score[0]:.2f} / 5")
    c5.metric("Late Delivery",   f"{kpis.late_pct[0]:.1f}%")

    df_monthly = run_query(f"""
        SELECT FORMAT_DATE('%Y-%m', order_purchase_date) AS month,
               ROUND(SUM(total_sale_amount), 0) AS gmv
        FROM `{PROJECT_ID}.olist_marts.fact_orders`
        WHERE order_status='delivered'
          AND order_purchase_date BETWEEN '2017-01-01' AND '2018-10-31'
        GROUP BY month ORDER BY month
    """)
    st.plotly_chart(px.line(df_monthly, x="month", y="gmv",
        title="Monthly GMV", markers=True), use_container_width=True)

# ── PAGE 2: Product Analysis
elif page == "Product Analysis":
    st.title("Product Category Analysis")
    df = run_query(f"""
        SELECT p.product_category_english        AS category,
               COUNT(DISTINCT f.order_id)        AS orders,
               ROUND(SUM(f.total_sale_amount),0) AS revenue,
               ROUND(AVG(f.review_score),2)      AS avg_score
        FROM `{PROJECT_ID}.olist_marts.fact_orders`  f
        JOIN `{PROJECT_ID}.olist_marts.dim_product`  p
            ON f.product_id = p.product_id
        WHERE f.order_status='delivered'
          AND p.product_category_english IS NOT NULL
        GROUP BY category HAVING orders >= 100
        ORDER BY revenue DESC LIMIT 15
    """)
    st.plotly_chart(
        px.bar(df.sort_values("revenue"), x="revenue", y="category",
            orientation="h", title="Top 15 Categories by Revenue",
            color="avg_score", color_continuous_scale="RdYlGn"),
        use_container_width=True)

# ── PAGE 3: Customer Segments
elif page == "Customer Segments":
    st.title("Customer RFM Segmentation")
    df = run_query(f"""
        SELECT rfm_segment,
               COUNT(*)               AS customers,
               ROUND(SUM(monetary),0) AS revenue,
               ROUND(AVG(monetary),2) AS avg_ltv
        FROM `{PROJECT_ID}.olist_marts.mart_customer_rfm`
        GROUP BY rfm_segment ORDER BY revenue DESC
    """)
    colors = {"VIP":"#27ae60","Loyal":"#2980b9","Big Spender":"#f39c12",
              "At-Risk":"#e74c3c","One-Timer":"#95a5a6"}
    c1, c2 = st.columns(2)
    c1.plotly_chart(px.pie(df, names="rfm_segment", values="customers",
        title="Customer Distribution", color_discrete_map=colors),
        use_container_width=True)
    c2.plotly_chart(px.bar(df, x="rfm_segment", y="revenue",
        title="Revenue by Segment", color="rfm_segment",
        color_discrete_map=colors), use_container_width=True)
    st.dataframe(df.rename(columns={
        "rfm_segment":"Segment","customers":"Customers",
        "revenue":"Total Revenue (BRL)","avg_ltv":"Avg LTV (BRL)"
    }), use_container_width=True)
