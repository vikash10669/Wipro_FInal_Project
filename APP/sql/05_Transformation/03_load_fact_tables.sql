-- ============================================================
-- FACT TABLE POPULATION
-- Clean Layer → Consumption Layer (Fact tables)
-- ============================================================

USE DATABASE RETAIL_DW;
USE SCHEMA CONSUMPTION_LAYER;
USE WAREHOUSE RETAIL_WH;

-- ============================================================
-- LOAD FACT_SALES
-- Joins clean sales lines with dimension surrogate keys
-- ============================================================
INSERT INTO FACT_SALES (
    date_key, store_sk, customer_sk, product_sk, payment_method_sk, channel_sk,
    transaction_id, transaction_code, line_id, line_number, transaction_type,
    quantity_sold, unit_price, unit_cost, gross_sales_amount, discount_amount,
    net_sales_amount, tax_amount, total_sales_amount, cogs_amount, gross_profit_amount,
    gross_margin_pct, loyalty_points_earned
)
SELECT
    -- Date key from transaction date
    TO_NUMBER(TO_CHAR(t.transaction_date::DATE, 'YYYYMMDD'))    AS date_key,

    -- Store SCD lookup (current version at transaction time)
    ds.store_sk,

    -- Customer SCD lookup
    dc.customer_sk,

    -- Product SCD lookup (version active at transaction time)
    dp.product_sk,

    -- Payment method lookup
    pm.payment_method_sk,

    -- Channel lookup
    ch.channel_sk,

    -- Degenerate dimensions
    t.transaction_id,
    t.transaction_code,
    l.line_id,
    l.line_number,
    t.transaction_type,

    -- Measures
    l.quantity                                                  AS quantity_sold,
    l.unit_price,
    l.unit_cost,
    l.quantity * l.unit_price                                   AS gross_sales_amount,
    l.discount_amount,
    l.line_total_amount                                         AS net_sales_amount,
    l.tax_amount,
    l.line_total_amount + l.tax_amount                          AS total_sales_amount,
    l.line_cost_amount                                          AS cogs_amount,
    l.line_total_amount - l.line_cost_amount                    AS gross_profit_amount,
    ROUND((l.line_total_amount - l.line_cost_amount) /
          NULLIF(l.line_total_amount, 0), 4)                    AS gross_margin_pct,
    ROUND(l.line_total_amount * 0.01)                           AS loyalty_points_earned

FROM CLEAN_LAYER.CLN_SALES_LINE      l
JOIN CLEAN_LAYER.CLN_SALES_TRANSACTION t ON l.transaction_id = t.transaction_id

-- Store SCD Type 2 resolution
LEFT JOIN DIM_STORE ds ON
    ds.store_id = t.store_id AND
    t.transaction_date::DATE BETWEEN ds.scd_effective_date AND ds.scd_expiry_date

-- Customer SCD Type 2 resolution
LEFT JOIN DIM_CUSTOMER dc ON
    dc.customer_id = t.customer_id AND
    t.transaction_date::DATE BETWEEN dc.scd_effective_date AND dc.scd_expiry_date

-- Product SCD Type 2 resolution (price at time of sale)
LEFT JOIN DIM_PRODUCT dp ON
    dp.product_id = l.product_id AND
    t.transaction_date::DATE BETWEEN dp.scd_effective_date AND dp.scd_expiry_date

-- Payment method lookup
LEFT JOIN (
    SELECT DISTINCT
        p.transaction_id,
        pm.payment_method_sk,
        ROW_NUMBER() OVER (PARTITION BY p.transaction_id ORDER BY p.payment_date) AS rn
    FROM CLEAN_LAYER.CLN_PAYMENT p
    JOIN DIM_PAYMENT_METHOD pm ON pm.payment_method_code = p.payment_method
) pm ON pm.transaction_id = t.transaction_id AND pm.rn = 1

-- Channel lookup
LEFT JOIN DIM_CHANNEL ch ON ch.channel_code = t.channel

-- Exclude already loaded lines
WHERE NOT EXISTS (
    SELECT 1 FROM FACT_SALES fs
    WHERE fs.line_id = l.line_id
);

-- ============================================================
-- LOAD FACT_INVENTORY
-- ============================================================
INSERT INTO FACT_INVENTORY (
    date_key, store_sk, product_sk, inventory_id,
    quantity_on_hand, quantity_reserved, quantity_available,
    reorder_point, reorder_quantity,
    inventory_value_cost, inventory_value_retail,
    days_since_last_sale, days_since_restock, below_reorder_flag
)
SELECT
    TO_NUMBER(TO_CHAR(i.snapshot_date, 'YYYYMMDD'))             AS date_key,
    ds.store_sk,
    dp.product_sk,
    i.inventory_id,
    i.quantity_on_hand,
    i.quantity_reserved,
    i.quantity_available,
    i.reorder_point,
    i.reorder_quantity,
    i.quantity_on_hand * COALESCE(dp.unit_cost, 0)              AS inventory_value_cost,
    i.quantity_on_hand * COALESCE(dp.unit_price, 0)             AS inventory_value_retail,
    DATEDIFF('day', i.last_sold_date, i.snapshot_date)          AS days_since_last_sale,
    DATEDIFF('day', i.last_restock_date, i.snapshot_date)       AS days_since_restock,
    i.quantity_available <= i.reorder_point                     AS below_reorder_flag

FROM CLEAN_LAYER.CLN_INVENTORY i

LEFT JOIN DIM_STORE ds ON
    ds.store_id = i.store_id AND ds.scd_is_current = TRUE

LEFT JOIN DIM_PRODUCT dp ON
    dp.product_id = i.product_id AND dp.scd_is_current = TRUE

WHERE NOT EXISTS (
    SELECT 1 FROM FACT_INVENTORY fi
    WHERE fi.inventory_id = i.inventory_id
      AND fi.date_key = TO_NUMBER(TO_CHAR(i.snapshot_date, 'YYYYMMDD'))
);

-- ============================================================
-- LOAD FACT_RETURNS
-- ============================================================
INSERT INTO FACT_RETURNS (
    return_date_key, store_sk, customer_sk, original_date_key,
    return_id, return_code, original_transaction_id, return_reason,
    refund_method, refund_amount, is_restocked
)
SELECT
    TO_NUMBER(TO_CHAR(r.return_date::DATE, 'YYYYMMDD'))         AS return_date_key,
    ds.store_sk,
    dc.customer_sk,
    TO_NUMBER(TO_CHAR(t.transaction_date::DATE, 'YYYYMMDD'))    AS original_date_key,
    r.return_id,
    r.return_code,
    r.original_transaction_id,
    r.return_reason,
    r.refund_method,
    r.refund_amount,
    r.is_restocked

FROM CLEAN_LAYER.CLN_RETURN r
LEFT JOIN CLEAN_LAYER.CLN_SALES_TRANSACTION t ON r.original_transaction_id = t.transaction_id

LEFT JOIN DIM_STORE ds ON
    ds.store_id = r.store_id AND ds.scd_is_current = TRUE

LEFT JOIN DIM_CUSTOMER dc ON
    dc.customer_id = r.customer_id AND dc.scd_is_current = TRUE

WHERE NOT EXISTS (
    SELECT 1 FROM FACT_RETURNS fr WHERE fr.return_id = r.return_id
);

-- ============================================================
-- REFRESH AGGREGATE TABLES
-- ============================================================

-- Monthly Store Sales
TRUNCATE TABLE AGG_MONTHLY_STORE_SALES;
INSERT INTO AGG_MONTHLY_STORE_SALES (
    year_number, month_number, year_month, store_sk, store_id, store_name,
    store_type, region, transaction_count, customer_count, total_quantity,
    gross_sales_amount, discount_amount, net_sales_amount, tax_amount,
    total_sales_amount, cogs_amount, gross_profit_amount, gross_margin_pct,
    return_amount, net_revenue
)
SELECT
    d.year_number,
    d.month_number,
    d.year_number || '-' || LPAD(d.month_number::VARCHAR, 2, '0') AS year_month,
    fs.store_sk,
    ds.store_id,
    ds.store_name,
    ds.store_type,
    ds.region,
    COUNT(DISTINCT fs.transaction_id)                   AS transaction_count,
    COUNT(DISTINCT fs.customer_sk)                      AS customer_count,
    SUM(fs.quantity_sold)                               AS total_quantity,
    SUM(fs.gross_sales_amount)                          AS gross_sales_amount,
    SUM(fs.discount_amount)                             AS discount_amount,
    SUM(fs.net_sales_amount)                            AS net_sales_amount,
    SUM(fs.tax_amount)                                  AS tax_amount,
    SUM(fs.total_sales_amount)                          AS total_sales_amount,
    SUM(fs.cogs_amount)                                 AS cogs_amount,
    SUM(fs.gross_profit_amount)                         AS gross_profit_amount,
    ROUND(SUM(fs.gross_profit_amount) /
          NULLIF(SUM(fs.net_sales_amount), 0), 4)       AS gross_margin_pct,
    COALESCE(r.return_amount, 0)                        AS return_amount,
    SUM(fs.net_sales_amount) - COALESCE(r.return_amount, 0) AS net_revenue
FROM FACT_SALES fs
JOIN DIM_DATE  d  ON fs.date_key = d.date_key
JOIN DIM_STORE ds ON fs.store_sk = ds.store_sk
LEFT JOIN (
    SELECT
        fr.store_sk,
        d2.year_number,
        d2.month_number,
        SUM(fr.refund_amount) AS return_amount
    FROM FACT_RETURNS fr
    JOIN DIM_DATE d2 ON fr.return_date_key = d2.date_key
    GROUP BY 1, 2, 3
) r ON r.store_sk = fs.store_sk
    AND r.year_number  = d.year_number
    AND r.month_number = d.month_number
WHERE fs.transaction_type = 'SALE'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, r.return_amount;

-- Monthly Product Sales
TRUNCATE TABLE AGG_MONTHLY_PRODUCT_SALES;
INSERT INTO AGG_MONTHLY_PRODUCT_SALES (
    year_number, month_number, year_month, product_sk, product_id, product_name,
    category_name, brand, total_quantity, gross_sales_amount, net_sales_amount,
    cogs_amount, gross_profit_amount, gross_margin_pct, transaction_count
)
SELECT
    d.year_number,
    d.month_number,
    d.year_number || '-' || LPAD(d.month_number::VARCHAR, 2, '0') AS year_month,
    fs.product_sk,
    dp.product_id,
    dp.product_name,
    dp.category_name,
    dp.brand,
    SUM(fs.quantity_sold)                               AS total_quantity,
    SUM(fs.gross_sales_amount)                          AS gross_sales_amount,
    SUM(fs.net_sales_amount)                            AS net_sales_amount,
    SUM(fs.cogs_amount)                                 AS cogs_amount,
    SUM(fs.gross_profit_amount)                         AS gross_profit_amount,
    ROUND(SUM(fs.gross_profit_amount) /
          NULLIF(SUM(fs.net_sales_amount), 0), 4)       AS gross_margin_pct,
    COUNT(DISTINCT fs.transaction_id)                   AS transaction_count
FROM FACT_SALES fs
JOIN DIM_DATE    d  ON fs.date_key = d.date_key
JOIN DIM_PRODUCT dp ON fs.product_sk = dp.product_sk
WHERE fs.transaction_type = 'SALE'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;
