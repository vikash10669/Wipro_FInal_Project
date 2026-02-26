-- ============================================================
-- CONSUMPTION LAYER - FACT TABLES
-- ============================================================

USE DATABASE RETAIL_DW;
USE SCHEMA CONSUMPTION_LAYER;
USE WAREHOUSE RETAIL_WH;

-- ============================================================
-- FACT SALES
-- Grain: One row per sales transaction line item
-- ============================================================
CREATE OR REPLACE TABLE FACT_SALES (
    sales_fact_sk           NUMBER AUTOINCREMENT PRIMARY KEY,

    -- Dimension Foreign Keys
    date_key                NUMBER          NOT NULL REFERENCES DIM_DATE(date_key),
    store_sk                NUMBER          NOT NULL REFERENCES DIM_STORE(store_sk),
    customer_sk             NUMBER,         -- nullable for anonymous transactions
    product_sk              NUMBER          NOT NULL REFERENCES DIM_PRODUCT(product_sk),
    payment_method_sk       NUMBER          REFERENCES DIM_PAYMENT_METHOD(payment_method_sk),
    channel_sk              NUMBER          REFERENCES DIM_CHANNEL(channel_sk),

    -- Degenerate Dimensions (no dimension table needed)
    transaction_id          NUMBER          NOT NULL,
    transaction_code        VARCHAR(50),
    line_id                 NUMBER          NOT NULL,
    line_number             NUMBER,
    transaction_type        VARCHAR(20),    -- SALE, RETURN, EXCHANGE

    -- Measures
    quantity_sold           NUMBER          NOT NULL DEFAULT 0,
    unit_price              NUMBER(10,2)    NOT NULL DEFAULT 0,
    unit_cost               NUMBER(10,2)    NOT NULL DEFAULT 0,
    gross_sales_amount      NUMBER(12,2)    NOT NULL DEFAULT 0,   -- qty * unit_price
    discount_amount         NUMBER(12,2)    NOT NULL DEFAULT 0,
    net_sales_amount        NUMBER(12,2)    NOT NULL DEFAULT 0,   -- gross - discount
    tax_amount              NUMBER(12,2)    NOT NULL DEFAULT 0,
    total_sales_amount      NUMBER(12,2)    NOT NULL DEFAULT 0,   -- net + tax
    cogs_amount             NUMBER(12,2)    NOT NULL DEFAULT 0,   -- qty * unit_cost
    gross_profit_amount     NUMBER(12,2)    NOT NULL DEFAULT 0,   -- net - cogs
    gross_margin_pct        NUMBER(6,4),                          -- gross_profit/net_sales
    loyalty_points_earned   NUMBER          NOT NULL DEFAULT 0,

    -- Audit
    _dw_inserted_ts         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- FACT INVENTORY (Periodic Snapshot)
-- Grain: One row per store per product per snapshot_date
-- ============================================================
CREATE OR REPLACE TABLE FACT_INVENTORY (
    inventory_fact_sk       NUMBER AUTOINCREMENT PRIMARY KEY,

    -- Dimension Foreign Keys
    date_key                NUMBER          NOT NULL REFERENCES DIM_DATE(date_key),
    store_sk                NUMBER          NOT NULL REFERENCES DIM_STORE(store_sk),
    product_sk              NUMBER          NOT NULL REFERENCES DIM_PRODUCT(product_sk),

    -- Degenerate Dimension
    inventory_id            NUMBER,

    -- Measures
    quantity_on_hand        NUMBER          NOT NULL DEFAULT 0,
    quantity_reserved       NUMBER          NOT NULL DEFAULT 0,
    quantity_available      NUMBER          NOT NULL DEFAULT 0,
    reorder_point           NUMBER          NOT NULL DEFAULT 0,
    reorder_quantity        NUMBER          NOT NULL DEFAULT 0,
    inventory_value_cost    NUMBER(14,2)    NOT NULL DEFAULT 0,   -- qty_on_hand * unit_cost
    inventory_value_retail  NUMBER(14,2)    NOT NULL DEFAULT 0,   -- qty_on_hand * unit_price
    days_since_last_sale    NUMBER,
    days_since_restock      NUMBER,
    below_reorder_flag      BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Audit
    _dw_inserted_ts         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- FACT RETURNS
-- Grain: One row per return transaction
-- ============================================================
CREATE OR REPLACE TABLE FACT_RETURNS (
    return_fact_sk          NUMBER AUTOINCREMENT PRIMARY KEY,

    -- Dimension Foreign Keys
    return_date_key         NUMBER          NOT NULL REFERENCES DIM_DATE(date_key),
    store_sk                NUMBER          NOT NULL REFERENCES DIM_STORE(store_sk),
    customer_sk             NUMBER,
    original_date_key       NUMBER,

    -- Degenerate Dimensions
    return_id               NUMBER          NOT NULL,
    return_code             VARCHAR(50),
    original_transaction_id NUMBER,
    return_reason           VARCHAR(100),
    refund_method           VARCHAR(50),

    -- Measures
    refund_amount           NUMBER(12,2)    NOT NULL DEFAULT 0,
    is_restocked            BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Audit
    _dw_inserted_ts         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- AGGREGATE TABLE: Monthly Store Sales Summary
-- Pre-aggregated for dashboard performance
-- ============================================================
CREATE OR REPLACE TABLE AGG_MONTHLY_STORE_SALES (
    agg_sk                  NUMBER AUTOINCREMENT PRIMARY KEY,
    year_number             NUMBER(4)       NOT NULL,
    month_number            NUMBER(2)       NOT NULL,
    year_month              VARCHAR(7)      NOT NULL,   -- YYYY-MM
    store_sk                NUMBER          NOT NULL REFERENCES DIM_STORE(store_sk),
    store_id                NUMBER,
    store_name              VARCHAR(200),
    store_type              VARCHAR(50),
    region                  VARCHAR(50),
    transaction_count       NUMBER          NOT NULL DEFAULT 0,
    customer_count          NUMBER          NOT NULL DEFAULT 0,
    total_quantity          NUMBER          NOT NULL DEFAULT 0,
    gross_sales_amount      NUMBER(14,2)    NOT NULL DEFAULT 0,
    discount_amount         NUMBER(14,2)    NOT NULL DEFAULT 0,
    net_sales_amount        NUMBER(14,2)    NOT NULL DEFAULT 0,
    tax_amount              NUMBER(14,2)    NOT NULL DEFAULT 0,
    total_sales_amount      NUMBER(14,2)    NOT NULL DEFAULT 0,
    cogs_amount             NUMBER(14,2)    NOT NULL DEFAULT 0,
    gross_profit_amount     NUMBER(14,2)    NOT NULL DEFAULT 0,
    gross_margin_pct        NUMBER(6,4),
    return_amount           NUMBER(14,2)    NOT NULL DEFAULT 0,
    net_revenue             NUMBER(14,2)    NOT NULL DEFAULT 0,
    _dw_refreshed_ts        TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- AGGREGATE TABLE: Monthly Product Sales Summary
-- ============================================================
CREATE OR REPLACE TABLE AGG_MONTHLY_PRODUCT_SALES (
    agg_sk                  NUMBER AUTOINCREMENT PRIMARY KEY,
    year_number             NUMBER(4)       NOT NULL,
    month_number            NUMBER(2)       NOT NULL,
    year_month              VARCHAR(7)      NOT NULL,
    product_sk              NUMBER          NOT NULL REFERENCES DIM_PRODUCT(product_sk),
    product_id              NUMBER,
    product_name            VARCHAR(300),
    category_name           VARCHAR(200),
    brand                   VARCHAR(100),
    total_quantity          NUMBER          NOT NULL DEFAULT 0,
    gross_sales_amount      NUMBER(14,2)    NOT NULL DEFAULT 0,
    net_sales_amount        NUMBER(14,2)    NOT NULL DEFAULT 0,
    cogs_amount             NUMBER(14,2)    NOT NULL DEFAULT 0,
    gross_profit_amount     NUMBER(14,2)    NOT NULL DEFAULT 0,
    gross_margin_pct        NUMBER(6,4),
    transaction_count       NUMBER          NOT NULL DEFAULT 0,
    _dw_refreshed_ts        TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);
