-- ============================================================
-- CLEAN / CURATED LAYER (Layer 2)
-- Typed, validated, deduplicated tables
-- Supports Streams for CDC-based MERGE into Consumption layer
-- ============================================================

USE DATABASE RETAIL_DW;
USE SCHEMA CLEAN_LAYER;
USE WAREHOUSE RETAIL_WH;

-- ============================================================
-- CLEAN LOCATION
-- ============================================================
CREATE OR REPLACE TABLE CLN_LOCATION (
    location_sk         NUMBER AUTOINCREMENT PRIMARY KEY,
    location_id         NUMBER          NOT NULL UNIQUE,
    street_address      VARCHAR(200),
    city                VARCHAR(100),
    state               VARCHAR(100),
    zip_code            VARCHAR(20),
    country             VARCHAR(100),
    region              VARCHAR(50),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    _dw_inserted_ts     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _is_deleted         BOOLEAN         DEFAULT FALSE
);

-- ============================================================
-- CLEAN STORE
-- ============================================================
CREATE OR REPLACE TABLE CLN_STORE (
    store_sk            NUMBER AUTOINCREMENT PRIMARY KEY,
    store_id            NUMBER          NOT NULL UNIQUE,
    store_code          VARCHAR(20),
    store_name          VARCHAR(200),
    store_type          VARCHAR(50),
    location_id         NUMBER,
    manager_name        VARCHAR(200),
    phone_number        VARCHAR(20),
    email               VARCHAR(200),
    open_date           DATE,
    close_date          DATE,
    is_active           BOOLEAN,
    square_footage      NUMBER,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    _dw_inserted_ts     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _is_deleted         BOOLEAN         DEFAULT FALSE
);

-- ============================================================
-- CLEAN CUSTOMER
-- ============================================================
CREATE OR REPLACE TABLE CLN_CUSTOMER (
    customer_sk         NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_id         NUMBER          NOT NULL UNIQUE,
    customer_code       VARCHAR(20),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    full_name           VARCHAR(200)    AS (first_name || ' ' || last_name),
    email               VARCHAR(200),
    phone_number        VARCHAR(20),
    date_of_birth       DATE,
    age                 NUMBER          AS (DATEDIFF('year', date_of_birth, CURRENT_DATE())),
    gender              VARCHAR(10),
    loyalty_tier        VARCHAR(20),
    loyalty_points      NUMBER,
    registration_date   DATE,
    location_id         NUMBER,
    is_active           BOOLEAN,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    _dw_inserted_ts     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _is_deleted         BOOLEAN         DEFAULT FALSE
);

-- ============================================================
-- CLEAN PRODUCT CATEGORY
-- ============================================================
CREATE OR REPLACE TABLE CLN_PRODUCT_CATEGORY (
    category_sk         NUMBER AUTOINCREMENT PRIMARY KEY,
    category_id         NUMBER          NOT NULL UNIQUE,
    category_code       VARCHAR(20),
    category_name       VARCHAR(200),
    parent_category_id  NUMBER,
    description         VARCHAR(1000),
    is_active           BOOLEAN,
    created_at          TIMESTAMP,
    _dw_inserted_ts     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- CLEAN PRODUCT
-- ============================================================
CREATE OR REPLACE TABLE CLN_PRODUCT (
    product_sk          NUMBER AUTOINCREMENT PRIMARY KEY,
    product_id          NUMBER          NOT NULL UNIQUE,
    product_code        VARCHAR(50),
    sku                 VARCHAR(100),
    product_name        VARCHAR(300),
    category_id         NUMBER,
    supplier_id         NUMBER,
    unit_cost           NUMBER(10,2),
    unit_price          NUMBER(10,2),
    gross_margin_pct    NUMBER(6,4)     AS (ROUND((unit_price - unit_cost) / NULLIF(unit_price,0), 4)),
    discount_pct        NUMBER(5,2),
    weight_kg           NUMBER(8,3),
    brand               VARCHAR(100),
    size                VARCHAR(50),
    color               VARCHAR(50),
    is_perishable       BOOLEAN,
    is_active           BOOLEAN,
    launch_date         DATE,
    discontinue_date    DATE,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    _dw_inserted_ts     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _is_deleted         BOOLEAN         DEFAULT FALSE
);

-- ============================================================
-- CLEAN SALES TRANSACTION
-- ============================================================
CREATE OR REPLACE TABLE CLN_SALES_TRANSACTION (
    transaction_sk              NUMBER AUTOINCREMENT PRIMARY KEY,
    transaction_id              NUMBER          NOT NULL UNIQUE,
    transaction_code            VARCHAR(50),
    transaction_date            TIMESTAMP,
    transaction_date_key        NUMBER          AS (TO_NUMBER(TO_CHAR(transaction_date::DATE, 'YYYYMMDD'))),
    store_id                    NUMBER,
    customer_id                 NUMBER,
    cashier_id                  NUMBER,
    transaction_type            VARCHAR(20),
    channel                     VARCHAR(30),
    subtotal_amount             NUMBER(12,2),
    discount_amount             NUMBER(12,2),
    tax_amount                  NUMBER(12,2),
    total_amount                NUMBER(12,2),
    loyalty_points_earned       NUMBER,
    loyalty_points_redeemed     NUMBER,
    notes                       VARCHAR(1000),
    created_at                  TIMESTAMP,
    _dw_inserted_ts             TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _is_deleted                 BOOLEAN         DEFAULT FALSE
);

-- ============================================================
-- CLEAN SALES LINE
-- ============================================================
CREATE OR REPLACE TABLE CLN_SALES_LINE (
    line_sk             NUMBER AUTOINCREMENT PRIMARY KEY,
    line_id             NUMBER          NOT NULL UNIQUE,
    transaction_id      NUMBER,
    line_number         NUMBER,
    product_id          NUMBER,
    quantity            NUMBER,
    unit_price          NUMBER(10,2),
    unit_cost           NUMBER(10,2),
    discount_pct        NUMBER(5,2),
    discount_amount     NUMBER(10,2),
    line_total_amount   NUMBER(12,2),
    line_cost_amount    NUMBER(12,2),
    gross_profit        NUMBER(12,2)    AS (line_total_amount - line_cost_amount),
    tax_rate            NUMBER(5,2),
    tax_amount          NUMBER(10,2),
    created_at          TIMESTAMP,
    _dw_inserted_ts     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- CLEAN PAYMENT
-- ============================================================
CREATE OR REPLACE TABLE CLN_PAYMENT (
    payment_sk          NUMBER AUTOINCREMENT PRIMARY KEY,
    payment_id          NUMBER          NOT NULL UNIQUE,
    transaction_id      NUMBER,
    payment_method      VARCHAR(50),
    payment_amount      NUMBER(12,2),
    payment_status      VARCHAR(20),
    payment_reference   VARCHAR(100),
    payment_date        TIMESTAMP,
    card_last_four      VARCHAR(4),
    created_at          TIMESTAMP,
    _dw_inserted_ts     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- CLEAN RETURN TRANSACTION
-- ============================================================
CREATE OR REPLACE TABLE CLN_RETURN (
    return_sk               NUMBER AUTOINCREMENT PRIMARY KEY,
    return_id               NUMBER          NOT NULL UNIQUE,
    return_code             VARCHAR(50),
    original_transaction_id NUMBER,
    return_date             TIMESTAMP,
    store_id                NUMBER,
    customer_id             NUMBER,
    return_reason           VARCHAR(100),
    refund_method           VARCHAR(50),
    refund_amount           NUMBER(12,2),
    is_restocked            BOOLEAN,
    created_at              TIMESTAMP,
    _dw_inserted_ts         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- CLEAN INVENTORY
-- ============================================================
CREATE OR REPLACE TABLE CLN_INVENTORY (
    inventory_sk            NUMBER AUTOINCREMENT PRIMARY KEY,
    inventory_id            NUMBER          NOT NULL,
    store_id                NUMBER,
    product_id              NUMBER,
    quantity_on_hand        NUMBER,
    quantity_reserved       NUMBER,
    quantity_available      NUMBER,
    reorder_point           NUMBER,
    reorder_quantity        NUMBER,
    below_reorder_flag      BOOLEAN         AS (quantity_available <= reorder_point),
    last_restock_date       DATE,
    last_sold_date          DATE,
    snapshot_date           DATE,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP,
    _dw_inserted_ts         TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- CREATE STREAMS for CDC (Change Data Capture)
-- Used by MERGE jobs to propagate changes to Consumption layer
-- ============================================================
CREATE OR REPLACE STREAM STM_CLN_CUSTOMER
    ON TABLE CLN_CUSTOMER
    COMMENT = 'CDC stream on clean customer table';

CREATE OR REPLACE STREAM STM_CLN_STORE
    ON TABLE CLN_STORE
    COMMENT = 'CDC stream on clean store table';

CREATE OR REPLACE STREAM STM_CLN_PRODUCT
    ON TABLE CLN_PRODUCT
    COMMENT = 'CDC stream on clean product table';

CREATE OR REPLACE STREAM STM_CLN_SALES_TRANSACTION
    ON TABLE CLN_SALES_TRANSACTION
    COMMENT = 'CDC stream on clean sales transaction table';

CREATE OR REPLACE STREAM STM_CLN_SALES_LINE
    ON TABLE CLN_SALES_LINE
    COMMENT = 'CDC stream on clean sales line table';

CREATE OR REPLACE STREAM STM_CLN_INVENTORY
    ON TABLE CLN_INVENTORY
    COMMENT = 'CDC stream on clean inventory table';
