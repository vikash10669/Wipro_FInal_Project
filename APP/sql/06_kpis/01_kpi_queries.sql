-- ============================================================
-- KPI ANALYTICAL QUERIES
-- Retail Chain Data Warehouse – Business Intelligence Layer
-- ============================================================

USE DATABASE RETAIL_DW;
USE SCHEMA CONSUMPTION_LAYER;
USE WAREHOUSE RETAIL_WH;

-- ============================================================
-- KPI 1: Total Revenue (Overall)
-- ============================================================
SELECT
    SUM(net_sales_amount)                               AS gross_revenue,
    SUM(discount_amount)                                AS total_discounts,
    SUM(net_sales_amount) - SUM(discount_amount)        AS net_revenue,
    SUM(cogs_amount)                                    AS total_cogs,
    SUM(gross_profit_amount)                            AS gross_profit,
    ROUND(SUM(gross_profit_amount) /
          NULLIF(SUM(net_sales_amount), 0) * 100, 2)    AS gross_margin_pct,
    COUNT(DISTINCT transaction_id)                      AS total_transactions,
    COUNT(DISTINCT customer_sk)                         AS unique_customers,
    SUM(quantity_sold)                                  AS units_sold,
    ROUND(SUM(net_sales_amount) /
          NULLIF(COUNT(DISTINCT transaction_id), 0), 2) AS avg_transaction_value
FROM FACT_SALES
WHERE transaction_type = 'SALE';

-- ============================================================
-- KPI 2: Monthly Revenue Trend
-- ============================================================
SELECT
    d.year_number,
    d.month_number,
    d.month_name,
    d.year_number || '-' || LPAD(d.month_number::VARCHAR, 2, '0') AS year_month,
    SUM(fs.net_sales_amount)                            AS net_revenue,
    SUM(fs.gross_profit_amount)                         AS gross_profit,
    COUNT(DISTINCT fs.transaction_id)                   AS transactions,
    COUNT(DISTINCT fs.customer_sk)                      AS unique_customers,
    SUM(fs.quantity_sold)                               AS units_sold,
    ROUND(SUM(fs.net_sales_amount) /
          NULLIF(COUNT(DISTINCT fs.transaction_id),0), 2) AS avg_basket_size,
    -- MoM growth
    LAG(SUM(fs.net_sales_amount)) OVER (ORDER BY d.year_number, d.month_number) AS prev_month_revenue,
    ROUND((SUM(fs.net_sales_amount) - LAG(SUM(fs.net_sales_amount))
           OVER (ORDER BY d.year_number, d.month_number)) /
          NULLIF(LAG(SUM(fs.net_sales_amount))
                 OVER (ORDER BY d.year_number, d.month_number), 0) * 100, 2)    AS mom_growth_pct
FROM FACT_SALES fs
JOIN DIM_DATE   d ON fs.date_key = d.date_key
WHERE fs.transaction_type = 'SALE'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2;

-- ============================================================
-- KPI 3: Top 20 Products by Revenue
-- ============================================================
SELECT
    ROW_NUMBER() OVER (ORDER BY SUM(fs.net_sales_amount) DESC) AS rank,
    dp.product_id,
    dp.product_name,
    dp.category_name,
    dp.brand,
    SUM(fs.quantity_sold)                               AS units_sold,
    SUM(fs.net_sales_amount)                            AS net_revenue,
    SUM(fs.gross_profit_amount)                         AS gross_profit,
    ROUND(SUM(fs.gross_profit_amount) /
          NULLIF(SUM(fs.net_sales_amount),0) * 100, 2)  AS margin_pct,
    COUNT(DISTINCT fs.transaction_id)                   AS transactions,
    ROUND(SUM(fs.net_sales_amount) /
          NULLIF(SUM(fs.quantity_sold),0), 2)            AS avg_selling_price
FROM FACT_SALES fs
JOIN DIM_PRODUCT dp ON fs.product_sk = dp.product_sk
WHERE fs.transaction_type = 'SALE'
GROUP BY dp.product_id, dp.product_name, dp.category_name, dp.brand
ORDER BY net_revenue DESC
LIMIT 20;

-- ============================================================
-- KPI 4: Store-Wise Performance
-- ============================================================
SELECT
    ds.store_id,
    ds.store_name,
    ds.store_type,
    ds.region,
    ds.city,
    ds.state,
    SUM(fs.net_sales_amount)                            AS net_revenue,
    SUM(fs.gross_profit_amount)                         AS gross_profit,
    ROUND(SUM(fs.gross_profit_amount) /
          NULLIF(SUM(fs.net_sales_amount),0) * 100, 2)  AS margin_pct,
    COUNT(DISTINCT fs.transaction_id)                   AS transactions,
    COUNT(DISTINCT fs.customer_sk)                      AS unique_customers,
    SUM(fs.quantity_sold)                               AS units_sold,
    ROUND(SUM(fs.net_sales_amount) /
          NULLIF(COUNT(DISTINCT fs.transaction_id),0), 2) AS avg_basket_value,
    -- Rank within region
    RANK() OVER (PARTITION BY ds.region ORDER BY SUM(fs.net_sales_amount) DESC) AS rank_in_region,
    -- % of total revenue
    ROUND(SUM(fs.net_sales_amount) /
          SUM(SUM(fs.net_sales_amount)) OVER () * 100, 2) AS pct_of_total_revenue
FROM FACT_SALES  fs
JOIN DIM_STORE   ds ON fs.store_sk = ds.store_sk
WHERE fs.transaction_type = 'SALE'
GROUP BY ds.store_id, ds.store_name, ds.store_type, ds.region, ds.city, ds.state
ORDER BY net_revenue DESC;

-- ============================================================
-- KPI 5: Category Performance
-- ============================================================
SELECT
    dp.category_name,
    dp.parent_category_name,
    COUNT(DISTINCT dp.product_id)                       AS product_count,
    SUM(fs.quantity_sold)                               AS units_sold,
    SUM(fs.net_sales_amount)                            AS net_revenue,
    SUM(fs.gross_profit_amount)                         AS gross_profit,
    ROUND(SUM(fs.gross_profit_amount) /
          NULLIF(SUM(fs.net_sales_amount),0) * 100, 2)  AS margin_pct,
    ROUND(SUM(fs.net_sales_amount) /
          NULLIF(SUM(SUM(fs.net_sales_amount)) OVER (), 0) * 100, 2) AS pct_of_revenue
FROM FACT_SALES  fs
JOIN DIM_PRODUCT dp ON fs.product_sk = dp.product_sk
WHERE fs.transaction_type = 'SALE'
GROUP BY dp.category_name, dp.parent_category_name
ORDER BY net_revenue DESC;

-- ============================================================
-- KPI 6: Customer Purchase Trends & Segmentation
-- ============================================================
SELECT
    dc.loyalty_tier,
    dc.age_group,
    dc.gender,
    dc.region,
    COUNT(DISTINCT dc.customer_id)                      AS customer_count,
    COUNT(DISTINCT fs.transaction_id)                   AS total_purchases,
    SUM(fs.net_sales_amount)                            AS total_revenue,
    ROUND(SUM(fs.net_sales_amount) /
          NULLIF(COUNT(DISTINCT dc.customer_id),0), 2)  AS avg_revenue_per_customer,
    ROUND(COUNT(DISTINCT fs.transaction_id) /
          NULLIF(COUNT(DISTINCT dc.customer_id),0), 2)  AS avg_purchases_per_customer,
    ROUND(SUM(fs.net_sales_amount) /
          NULLIF(COUNT(DISTINCT fs.transaction_id),0), 2) AS avg_order_value
FROM FACT_SALES    fs
JOIN DIM_CUSTOMER  dc ON fs.customer_sk = dc.customer_sk
WHERE fs.transaction_type = 'SALE'
  AND dc.scd_is_current = TRUE
GROUP BY dc.loyalty_tier, dc.age_group, dc.gender, dc.region
ORDER BY total_revenue DESC;

-- ============================================================
-- KPI 7: Top 10 Customers by Lifetime Value
-- ============================================================
SELECT
    ROW_NUMBER() OVER (ORDER BY SUM(fs.net_sales_amount) DESC) AS rank,
    dc.customer_id,
    dc.full_name,
    dc.email,
    dc.loyalty_tier,
    dc.region,
    COUNT(DISTINCT fs.transaction_id)                   AS total_orders,
    SUM(fs.quantity_sold)                               AS total_items,
    SUM(fs.net_sales_amount)                            AS lifetime_value,
    MIN(d.full_date)                                    AS first_purchase_date,
    MAX(d.full_date)                                    AS last_purchase_date,
    DATEDIFF('day', MIN(d.full_date), MAX(d.full_date)) AS customer_lifespan_days,
    ROUND(SUM(fs.net_sales_amount) /
          NULLIF(COUNT(DISTINCT fs.transaction_id),0), 2) AS avg_order_value
FROM FACT_SALES    fs
JOIN DIM_CUSTOMER  dc ON fs.customer_sk = dc.customer_sk
JOIN DIM_DATE       d ON fs.date_key    = d.date_key
WHERE fs.transaction_type = 'SALE'
  AND dc.scd_is_current = TRUE
GROUP BY dc.customer_id, dc.full_name, dc.email, dc.loyalty_tier, dc.region
ORDER BY lifetime_value DESC
LIMIT 10;

-- ============================================================
-- KPI 8: Sales by Channel & Payment Method
-- ============================================================
SELECT
    ch.channel_name,
    pm.payment_method_name,
    COUNT(DISTINCT fs.transaction_id)                   AS transactions,
    SUM(fs.net_sales_amount)                            AS net_revenue,
    ROUND(SUM(fs.net_sales_amount) /
          NULLIF(COUNT(DISTINCT fs.transaction_id),0), 2) AS avg_order_value,
    ROUND(SUM(fs.net_sales_amount) /
          NULLIF(SUM(SUM(fs.net_sales_amount)) OVER (), 0) * 100, 2) AS revenue_share_pct
FROM FACT_SALES           fs
JOIN DIM_CHANNEL          ch ON fs.channel_sk        = ch.channel_sk
JOIN DIM_PAYMENT_METHOD   pm ON fs.payment_method_sk = pm.payment_method_sk
WHERE fs.transaction_type = 'SALE'
GROUP BY ch.channel_name, pm.payment_method_name
ORDER BY net_revenue DESC;

-- ============================================================
-- KPI 9: Regional Performance Heatmap
-- ============================================================
SELECT
    ds.region,
    d.year_number,
    d.quarter_name,
    SUM(fs.net_sales_amount)                            AS net_revenue,
    SUM(fs.gross_profit_amount)                         AS gross_profit,
    COUNT(DISTINCT fs.transaction_id)                   AS transactions,
    COUNT(DISTINCT ds.store_id)                         AS active_stores,
    ROUND(SUM(fs.net_sales_amount) /
          NULLIF(COUNT(DISTINCT ds.store_id),0), 2)     AS revenue_per_store
FROM FACT_SALES  fs
JOIN DIM_STORE   ds ON fs.store_sk = ds.store_sk
JOIN DIM_DATE    d  ON fs.date_key = d.date_key
WHERE fs.transaction_type = 'SALE'
GROUP BY ds.region, d.year_number, d.quarter_name
ORDER BY ds.region, d.year_number, d.quarter_name;

-- ============================================================
-- KPI 10: Inventory Health Dashboard
-- ============================================================
SELECT
    ds.store_name,
    ds.region,
    dp.product_name,
    dp.category_name,
    fi.quantity_on_hand,
    fi.quantity_available,
    fi.reorder_point,
    fi.inventory_value_cost,
    fi.inventory_value_retail,
    fi.below_reorder_flag,
    fi.days_since_last_sale,
    fi.days_since_restock,
    CASE
        WHEN fi.quantity_available = 0           THEN 'OUT_OF_STOCK'
        WHEN fi.below_reorder_flag               THEN 'REORDER_NEEDED'
        WHEN fi.days_since_last_sale > 90        THEN 'SLOW_MOVING'
        ELSE 'HEALTHY'
    END AS inventory_status
FROM FACT_INVENTORY   fi
JOIN DIM_STORE        ds ON fi.store_sk   = ds.store_sk
JOIN DIM_PRODUCT      dp ON fi.product_sk = dp.product_sk
JOIN DIM_DATE         d  ON fi.date_key   = d.date_key
WHERE d.full_date = (SELECT MAX(d2.full_date) FROM FACT_INVENTORY fi2 JOIN DIM_DATE d2 ON fi2.date_key = d2.date_key)
ORDER BY fi.below_reorder_flag DESC, fi.quantity_available ASC
LIMIT 100;

-- ============================================================
-- KPI 11: Return Rate Analysis
-- ============================================================
SELECT
    d_ret.year_number,
    d_ret.month_name,
    ds.store_name,
    ds.region,
    fr.return_reason,
    COUNT(fr.return_id)                                 AS return_count,
    SUM(fr.refund_amount)                               AS total_refunds,
    ROUND(COUNT(fr.return_id) /
          NULLIF(txn_counts.monthly_transactions,0) * 100, 2) AS return_rate_pct
FROM FACT_RETURNS   fr
JOIN DIM_DATE       d_ret ON fr.return_date_key = d_ret.date_key
JOIN DIM_STORE      ds    ON fr.store_sk        = ds.store_sk
JOIN (
    SELECT
        d.year_number,
        d.month_number,
        fs.store_sk,
        COUNT(DISTINCT fs.transaction_id) AS monthly_transactions
    FROM FACT_SALES fs
    JOIN DIM_DATE d ON fs.date_key = d.date_key
    WHERE fs.transaction_type = 'SALE'
    GROUP BY 1, 2, 3
) txn_counts ON txn_counts.store_sk    = fr.store_sk
            AND txn_counts.year_number  = d_ret.year_number
            AND txn_counts.month_number = d_ret.month_number
GROUP BY 1, 2, 3, 4, 5, txn_counts.monthly_transactions
ORDER BY total_refunds DESC;

-- ============================================================
-- KPI 12: Year-over-Year Comparison
-- ============================================================
WITH yearly AS (
    SELECT
        d.year_number,
        SUM(fs.net_sales_amount)                        AS net_revenue,
        SUM(fs.gross_profit_amount)                     AS gross_profit,
        COUNT(DISTINCT fs.transaction_id)               AS transactions,
        COUNT(DISTINCT fs.customer_sk)                  AS customers
    FROM FACT_SALES fs
    JOIN DIM_DATE   d ON fs.date_key = d.date_key
    WHERE fs.transaction_type = 'SALE'
    GROUP BY d.year_number
)
SELECT
    y.year_number,
    y.net_revenue,
    y.gross_profit,
    y.transactions,
    y.customers,
    ROUND(y.gross_profit / NULLIF(y.net_revenue,0) * 100, 2)        AS margin_pct,
    LAG(y.net_revenue)   OVER (ORDER BY y.year_number)              AS prev_year_revenue,
    ROUND((y.net_revenue - LAG(y.net_revenue) OVER (ORDER BY y.year_number)) /
          NULLIF(LAG(y.net_revenue) OVER (ORDER BY y.year_number),0) * 100, 2) AS yoy_growth_pct
FROM yearly y
ORDER BY y.year_number;
