-- ============================================================
-- SNOWFLAKE TASKS
-- Orchestrates the full ETL pipeline using Snowflake Tasks
-- ============================================================

USE DATABASE RETAIL_DW;
USE WAREHOUSE RETAIL_WH;

-- ============================================================
-- TASK 1 (Root): Stage → Clean Layer (runs every hour)
-- ============================================================
CREATE OR REPLACE TASK TASK_STAGE_TO_CLEAN
    WAREHOUSE   = RETAIL_WH
    SCHEDULE    = 'USING CRON 0 * * * * UTC'
    COMMENT     = 'Root task: merge raw stage data into clean layer'
AS
CALL SYSTEM$EXECUTE_IMMEDIATE($$
    -- Location
    MERGE INTO RETAIL_DW.CLEAN_LAYER.CLN_LOCATION tgt
    USING (
        SELECT DISTINCT
            TRY_TO_NUMBER(location_id)  AS location_id,
            TRIM(street_address)        AS street_address,
            INITCAP(TRIM(city))         AS city,
            UPPER(TRIM(state))          AS state,
            TRIM(zip_code)              AS zip_code,
            INITCAP(TRIM(COALESCE(country,'USA'))) AS country,
            UPPER(TRIM(region))         AS region,
            TRY_TO_TIMESTAMP(created_at) AS created_at,
            TRY_TO_TIMESTAMP(updated_at) AS updated_at
        FROM RETAIL_DW.STAGE_LAYER.STG_LOCATION_RAW
        WHERE TRY_TO_NUMBER(location_id) IS NOT NULL
    ) src
    ON tgt.location_id = src.location_id
    WHEN NOT MATCHED THEN INSERT (location_id, street_address, city, state, zip_code, country, region, created_at, updated_at)
    VALUES (src.location_id, src.street_address, src.city, src.state, src.zip_code, src.country, src.region, src.created_at, src.updated_at);
$$);

-- ============================================================
-- TASK 2: Clean → SCD Dimensions (depends on Task 1)
-- ============================================================
CREATE OR REPLACE TASK TASK_LOAD_DIMENSIONS
    WAREHOUSE   = RETAIL_WH
    AFTER       TASK_STAGE_TO_CLEAN
    COMMENT     = 'Load/update SCD Type 2 dimension tables'
AS
CALL SYSTEM$EXECUTE_IMMEDIATE($$
    -- Refresh DIM_LOCATION
    MERGE INTO RETAIL_DW.CONSUMPTION_LAYER.DIM_LOCATION tgt
    USING (
        SELECT location_id, street_address, city, state, zip_code, country, region
        FROM RETAIL_DW.CLEAN_LAYER.CLN_LOCATION
    ) src
    ON tgt.location_id = src.location_id
    WHEN NOT MATCHED THEN INSERT (location_id, street_address, city, state, zip_code, country, region)
    VALUES (src.location_id, src.street_address, src.city, src.state, src.zip_code, src.country, src.region);
$$);

-- ============================================================
-- TASK 3: Load Facts (depends on Task 2)
-- ============================================================
CREATE OR REPLACE TASK TASK_LOAD_FACTS
    WAREHOUSE   = RETAIL_WH
    AFTER       TASK_LOAD_DIMENSIONS
    COMMENT     = 'Load new records into fact tables'
AS
CALL SYSTEM$EXECUTE_IMMEDIATE($$
    -- Insert new sales facts not yet in FACT_SALES
    INSERT INTO RETAIL_DW.CONSUMPTION_LAYER.FACT_SALES (
        date_key, store_sk, customer_sk, product_sk,
        transaction_id, transaction_code, line_id, line_number, transaction_type,
        quantity_sold, unit_price, unit_cost, gross_sales_amount, discount_amount,
        net_sales_amount, tax_amount, total_sales_amount, cogs_amount, gross_profit_amount,
        gross_margin_pct, loyalty_points_earned
    )
    SELECT
        TO_NUMBER(TO_CHAR(t.transaction_date::DATE,'YYYYMMDD')),
        ds.store_sk, dc.customer_sk, dp.product_sk,
        t.transaction_id, t.transaction_code, l.line_id, l.line_number, t.transaction_type,
        l.quantity, l.unit_price, l.unit_cost,
        l.quantity * l.unit_price, l.discount_amount, l.line_total_amount,
        l.tax_amount, l.line_total_amount + l.tax_amount, l.line_cost_amount,
        l.line_total_amount - l.line_cost_amount,
        ROUND((l.line_total_amount - l.line_cost_amount)/NULLIF(l.line_total_amount,0),4),
        ROUND(l.line_total_amount * 0.01)
    FROM RETAIL_DW.CLEAN_LAYER.CLN_SALES_LINE l
    JOIN RETAIL_DW.CLEAN_LAYER.CLN_SALES_TRANSACTION t ON l.transaction_id = t.transaction_id
    LEFT JOIN RETAIL_DW.CONSUMPTION_LAYER.DIM_STORE ds
        ON ds.store_id = t.store_id AND t.transaction_date::DATE BETWEEN ds.scd_effective_date AND ds.scd_expiry_date
    LEFT JOIN RETAIL_DW.CONSUMPTION_LAYER.DIM_CUSTOMER dc
        ON dc.customer_id = t.customer_id AND t.transaction_date::DATE BETWEEN dc.scd_effective_date AND dc.scd_expiry_date
    LEFT JOIN RETAIL_DW.CONSUMPTION_LAYER.DIM_PRODUCT dp
        ON dp.product_id = l.product_id AND t.transaction_date::DATE BETWEEN dp.scd_effective_date AND dp.scd_expiry_date
    WHERE NOT EXISTS (
        SELECT 1 FROM RETAIL_DW.CONSUMPTION_LAYER.FACT_SALES fs WHERE fs.line_id = l.line_id
    );
$$);

-- ============================================================
-- TASK 4: Refresh Aggregates (depends on Task 3)
-- ============================================================
CREATE OR REPLACE TASK TASK_REFRESH_AGGREGATES
    WAREHOUSE   = RETAIL_WH
    AFTER       TASK_LOAD_FACTS
    COMMENT     = 'Refresh monthly aggregate tables for BI layer'
AS
CALL SYSTEM$EXECUTE_IMMEDIATE($$
    TRUNCATE TABLE RETAIL_DW.CONSUMPTION_LAYER.AGG_MONTHLY_STORE_SALES;
    INSERT INTO RETAIL_DW.CONSUMPTION_LAYER.AGG_MONTHLY_STORE_SALES (
        year_number, month_number, year_month, store_sk, store_id, store_name,
        store_type, region, transaction_count, customer_count, total_quantity,
        gross_sales_amount, discount_amount, net_sales_amount, tax_amount,
        total_sales_amount, cogs_amount, gross_profit_amount, gross_margin_pct,
        return_amount, net_revenue
    )
    SELECT
        d.year_number, d.month_number,
        d.year_number || '-' || LPAD(d.month_number::VARCHAR,2,'0'),
        fs.store_sk, ds.store_id, ds.store_name, ds.store_type, ds.region,
        COUNT(DISTINCT fs.transaction_id), COUNT(DISTINCT fs.customer_sk),
        SUM(fs.quantity_sold), SUM(fs.gross_sales_amount), SUM(fs.discount_amount),
        SUM(fs.net_sales_amount), SUM(fs.tax_amount), SUM(fs.total_sales_amount),
        SUM(fs.cogs_amount), SUM(fs.gross_profit_amount),
        ROUND(SUM(fs.gross_profit_amount)/NULLIF(SUM(fs.net_sales_amount),0),4),
        0, SUM(fs.net_sales_amount)
    FROM RETAIL_DW.CONSUMPTION_LAYER.FACT_SALES fs
    JOIN RETAIL_DW.CONSUMPTION_LAYER.DIM_DATE d ON fs.date_key = d.date_key
    JOIN RETAIL_DW.CONSUMPTION_LAYER.DIM_STORE ds ON fs.store_sk = ds.store_sk
    WHERE fs.transaction_type = 'SALE'
    GROUP BY 1,2,3,4,5,6,7,8;
$$);

-- ============================================================
-- Resume all tasks (they start suspended by default)
-- ============================================================
ALTER TASK TASK_REFRESH_AGGREGATES RESUME;
ALTER TASK TASK_LOAD_FACTS         RESUME;
ALTER TASK TASK_LOAD_DIMENSIONS    RESUME;
ALTER TASK TASK_STAGE_TO_CLEAN     RESUME;

-- To pause:
-- ALTER TASK TASK_STAGE_TO_CLEAN SUSPEND;

-- Check task run history:
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY SCHEDULED_TIME DESC LIMIT 20;
