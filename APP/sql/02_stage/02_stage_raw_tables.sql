-- ============================================================
-- STAGE LAYER (Layer 1) - RAW TABLES
-- Exact copies of source CSV columns, all VARCHAR
-- Loaded via COPY INTO from internal stages
-- ============================================================

USE DATABASE RETAIL_DW;
USE SCHEMA STAGE_LAYER;
USE WAREHOUSE RETAIL_WH;

-- ============================================================
-- RAW LOCATION
-- ============================================================
CREATE OR REPLACE TABLE STG_LOCATION_RAW (
    location_id     VARCHAR,
    street_address  VARCHAR,
    city            VARCHAR,
    state           VARCHAR,
    zip_code        VARCHAR,
    country         VARCHAR,
    region          VARCHAR,
    created_at      VARCHAR,
    updated_at      VARCHAR,
    _stg_file_name  VARCHAR,
    _stg_load_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- RAW STORE
-- ============================================================
CREATE OR REPLACE TABLE STG_STORE_RAW (
    store_id        VARCHAR,
    store_code      VARCHAR,
    store_name      VARCHAR,
    store_type      VARCHAR,
    location_id     VARCHAR,
    manager_name    VARCHAR,
    phone_number    VARCHAR,
    email           VARCHAR,
    open_date       VARCHAR,
    close_date      VARCHAR,
    is_active       VARCHAR,
    square_footage  VARCHAR,
    created_at      VARCHAR,
    updated_at      VARCHAR,
    _stg_file_name  VARCHAR,
    _stg_load_ts    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- RAW CUSTOMER
-- ============================================================
CREATE OR REPLACE TABLE STG_CUSTOMER_RAW (
    customer_id         VARCHAR,
    customer_code       VARCHAR,
    first_name          VARCHAR,
    last_name           VARCHAR,
    email               VARCHAR,
    phone_number        VARCHAR,
    date_of_birth       VARCHAR,
    gender              VARCHAR,
    loyalty_tier        VARCHAR,
    loyalty_points      VARCHAR,
    registration_date   VARCHAR,
    location_id         VARCHAR,
    is_active           VARCHAR,
    created_at          VARCHAR,
    updated_at          VARCHAR,
    _stg_file_name      VARCHAR,
    _stg_load_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- RAW PRODUCT CATEGORY
-- ============================================================
CREATE OR REPLACE TABLE STG_PRODUCT_CATEGORY_RAW (
    category_id         VARCHAR,
    category_code       VARCHAR,
    category_name       VARCHAR,
    parent_category_id  VARCHAR,
    description         VARCHAR,
    is_active           VARCHAR,
    created_at          VARCHAR,
    _stg_file_name      VARCHAR,
    _stg_load_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- RAW PRODUCT
-- ============================================================
CREATE OR REPLACE TABLE STG_PRODUCT_RAW (
    product_id          VARCHAR,
    product_code        VARCHAR,
    sku                 VARCHAR,
    product_name        VARCHAR,
    category_id         VARCHAR,
    supplier_id         VARCHAR,
    unit_cost           VARCHAR,
    unit_price          VARCHAR,
    discount_pct        VARCHAR,
    weight_kg           VARCHAR,
    brand               VARCHAR,
    size                VARCHAR,
    color               VARCHAR,
    is_perishable       VARCHAR,
    is_active           VARCHAR,
    launch_date         VARCHAR,
    discontinue_date    VARCHAR,
    created_at          VARCHAR,
    updated_at          VARCHAR,
    _stg_file_name      VARCHAR,
    _stg_load_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- RAW SALES TRANSACTION (Header)
-- ============================================================
CREATE OR REPLACE TABLE STG_SALES_TRANSACTION_RAW (
    transaction_id          VARCHAR,
    transaction_code        VARCHAR,
    transaction_date        VARCHAR,
    store_id                VARCHAR,
    customer_id             VARCHAR,
    cashier_id              VARCHAR,
    transaction_type        VARCHAR,
    channel                 VARCHAR,
    subtotal_amount         VARCHAR,
    discount_amount         VARCHAR,
    tax_amount              VARCHAR,
    total_amount            VARCHAR,
    loyalty_points_earned   VARCHAR,
    loyalty_points_redeemed VARCHAR,
    notes                   VARCHAR,
    created_at              VARCHAR,
    _stg_file_name          VARCHAR,
    _stg_load_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- RAW SALES TRANSACTION LINE (Detail)
-- ============================================================
CREATE OR REPLACE TABLE STG_SALES_LINE_RAW (
    line_id             VARCHAR,
    transaction_id      VARCHAR,
    line_number         VARCHAR,
    product_id          VARCHAR,
    quantity            VARCHAR,
    unit_price          VARCHAR,
    unit_cost           VARCHAR,
    discount_pct        VARCHAR,
    discount_amount     VARCHAR,
    line_total_amount   VARCHAR,
    line_cost_amount    VARCHAR,
    tax_rate            VARCHAR,
    tax_amount          VARCHAR,
    created_at          VARCHAR,
    _stg_file_name      VARCHAR,
    _stg_load_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- RAW PAYMENT
-- ============================================================
CREATE OR REPLACE TABLE STG_PAYMENT_RAW (
    payment_id          VARCHAR,
    transaction_id      VARCHAR,
    payment_method      VARCHAR,
    payment_amount      VARCHAR,
    payment_status      VARCHAR,
    payment_reference   VARCHAR,
    payment_date        VARCHAR,
    card_last_four      VARCHAR,
    created_at          VARCHAR,
    _stg_file_name      VARCHAR,
    _stg_load_ts        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- RAW RETURN TRANSACTION
-- ============================================================
CREATE OR REPLACE TABLE STG_RETURN_RAW (
    return_id               VARCHAR,
    return_code             VARCHAR,
    original_transaction_id VARCHAR,
    return_date             VARCHAR,
    store_id                VARCHAR,
    customer_id             VARCHAR,
    return_reason           VARCHAR,
    refund_method           VARCHAR,
    refund_amount           VARCHAR,
    is_restocked            VARCHAR,
    created_at              VARCHAR,
    _stg_file_name          VARCHAR,
    _stg_load_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- RAW INVENTORY
-- ============================================================
CREATE OR REPLACE TABLE STG_INVENTORY_RAW (
    inventory_id            VARCHAR,
    store_id                VARCHAR,
    product_id              VARCHAR,
    quantity_on_hand        VARCHAR,
    quantity_reserved       VARCHAR,
    quantity_available      VARCHAR,
    reorder_point           VARCHAR,
    reorder_quantity        VARCHAR,
    last_restock_date       VARCHAR,
    last_sold_date          VARCHAR,
    snapshot_date           VARCHAR,
    created_at              VARCHAR,
    updated_at              VARCHAR,
    _stg_file_name          VARCHAR,
    _stg_load_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
