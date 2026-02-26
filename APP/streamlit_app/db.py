"""
Snowflake connector module for the Streamlit dashboard.
Falls back to mock data when USE_MOCK_DATA=true or connection fails.
"""
import os
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

USE_MOCK = os.getenv('USE_MOCK_DATA', 'true').lower() in ('true', '1', 'yes')


def _get_conn():
    try:
        import snowflake.connector
        return snowflake.connector.connect(
            account   = os.getenv('SNOWFLAKE_ACCOUNT', ''),
            user      = os.getenv('SNOWFLAKE_USER', ''),
            password  = os.getenv('SNOWFLAKE_PASSWORD', ''),
            database  = os.getenv('SNOWFLAKE_DATABASE', 'RETAIL_DW'),
            warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'RETAIL_WH'),
            role      = os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
        )
    except Exception:
        return None


def run_query(sql: str) -> Optional[pd.DataFrame]:
    if USE_MOCK:
        return None
    conn = _get_conn()
    if conn is None:
        return None
    try:
        cs = conn.cursor()
        cs.execute(sql)
        cols = [desc[0].lower() for desc in cs.description]
        rows = cs.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ── Pre-built query helpers ──────────────────────────────────

KPI_SUMMARY_SQL = """
SELECT
    SUM(net_sales_amount)           AS gross_revenue,
    SUM(discount_amount)            AS total_discounts,
    SUM(net_sales_amount)           AS net_revenue,
    SUM(cogs_amount)                AS total_cogs,
    SUM(gross_profit_amount)        AS gross_profit,
    ROUND(SUM(gross_profit_amount)/NULLIF(SUM(net_sales_amount),0)*100,2) AS gross_margin_pct,
    COUNT(DISTINCT transaction_id)  AS total_transactions,
    COUNT(DISTINCT customer_sk)     AS unique_customers,
    SUM(quantity_sold)              AS units_sold,
    ROUND(SUM(net_sales_amount)/NULLIF(COUNT(DISTINCT transaction_id),0),2) AS avg_transaction_value
FROM RETAIL_DW.CONSUMPTION_LAYER.FACT_SALES
WHERE transaction_type='SALE'
"""

MONTHLY_TREND_SQL = """
SELECT
    d.year_number, d.month_number, d.month_name,
    d.year_number||'-'||LPAD(d.month_number::VARCHAR,2,'0') AS year_month,
    SUM(fs.net_sales_amount)   AS net_revenue,
    SUM(fs.gross_profit_amount) AS gross_profit,
    COUNT(DISTINCT fs.transaction_id) AS transactions,
    COUNT(DISTINCT fs.customer_sk) AS unique_customers,
    SUM(fs.quantity_sold) AS units_sold,
    ROUND(SUM(fs.net_sales_amount)/NULLIF(COUNT(DISTINCT fs.transaction_id),0),2) AS avg_basket_size
FROM RETAIL_DW.CONSUMPTION_LAYER.FACT_SALES fs
JOIN RETAIL_DW.CONSUMPTION_LAYER.DIM_DATE d ON fs.date_key=d.date_key
WHERE fs.transaction_type='SALE'
GROUP BY 1,2,3,4 ORDER BY 1,2
"""

TOP_PRODUCTS_SQL = """
SELECT dp.product_name, dp.category_name, dp.brand,
    SUM(fs.quantity_sold) AS units_sold, SUM(fs.net_sales_amount) AS net_revenue,
    SUM(fs.gross_profit_amount) AS gross_profit,
    ROUND(SUM(fs.gross_profit_amount)/NULLIF(SUM(fs.net_sales_amount),0)*100,2) AS margin_pct,
    COUNT(DISTINCT fs.transaction_id) AS transactions
FROM RETAIL_DW.CONSUMPTION_LAYER.FACT_SALES fs
JOIN RETAIL_DW.CONSUMPTION_LAYER.DIM_PRODUCT dp ON fs.product_sk=dp.product_sk
WHERE fs.transaction_type='SALE'
GROUP BY 1,2,3 ORDER BY net_revenue DESC LIMIT 20
"""

STORE_PERF_SQL = """
SELECT ds.store_id, ds.store_name, ds.store_type, ds.region, ds.city, ds.state,
    SUM(fs.net_sales_amount) AS net_revenue, SUM(fs.gross_profit_amount) AS gross_profit,
    ROUND(SUM(fs.gross_profit_amount)/NULLIF(SUM(fs.net_sales_amount),0)*100,2) AS margin_pct,
    COUNT(DISTINCT fs.transaction_id) AS transactions,
    COUNT(DISTINCT fs.customer_sk) AS unique_customers, SUM(fs.quantity_sold) AS units_sold,
    ROUND(SUM(fs.net_sales_amount)/NULLIF(COUNT(DISTINCT fs.transaction_id),0),2) AS avg_basket_value
FROM RETAIL_DW.CONSUMPTION_LAYER.FACT_SALES fs
JOIN RETAIL_DW.CONSUMPTION_LAYER.DIM_STORE ds ON fs.store_sk=ds.store_sk
WHERE fs.transaction_type='SALE'
GROUP BY 1,2,3,4,5,6 ORDER BY net_revenue DESC
"""
