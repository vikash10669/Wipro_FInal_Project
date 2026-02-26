-- ============================================================
-- CONSUMPTION LAYER (Layer 3) - STAR SCHEMA
-- Dimension Tables + Fact Tables
-- Implements SCD Type 2 for Store, Product, Customer
-- ============================================================

USE DATABASE RETAIL_DW;
USE SCHEMA CONSUMPTION_LAYER;
USE WAREHOUSE RETAIL_WH;

-- ============================================================
-- DIM DATE
-- Pre-populated calendar dimension
-- ============================================================
CREATE OR REPLACE TABLE DIM_DATE (
    date_key        NUMBER          PRIMARY KEY,   -- YYYYMMDD
    full_date       DATE            NOT NULL UNIQUE,
    day_of_week     NUMBER(1)       NOT NULL,      -- 1=Sunday
    day_name        VARCHAR(10)     NOT NULL,
    day_of_month    NUMBER(2)       NOT NULL,
    day_of_year     NUMBER(3)       NOT NULL,
    week_of_year    NUMBER(2)       NOT NULL,
    month_number    NUMBER(2)       NOT NULL,
    month_name      VARCHAR(10)     NOT NULL,
    month_short     VARCHAR(3)      NOT NULL,
    quarter_number  NUMBER(1)       NOT NULL,
    quarter_name    VARCHAR(6)      NOT NULL,
    year_number     NUMBER(4)       NOT NULL,
    is_weekend      BOOLEAN         NOT NULL,
    is_holiday      BOOLEAN         NOT NULL DEFAULT FALSE,
    fiscal_year     NUMBER(4),
    fiscal_quarter  NUMBER(1),
    fiscal_month    NUMBER(2)
);

-- Populate DIM_DATE for 2020-2030
INSERT INTO DIM_DATE
WITH date_series AS (
    SELECT DATEADD(day, SEQ4(), '2020-01-01'::DATE) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 3653))
)
SELECT
    TO_NUMBER(TO_CHAR(full_date, 'YYYYMMDD'))           AS date_key,
    full_date,
    DAYOFWEEK(full_date)                                 AS day_of_week,
    DAYNAME(full_date)                                   AS day_name,
    DAY(full_date)                                       AS day_of_month,
    DAYOFYEAR(full_date)                                 AS day_of_year,
    WEEKOFYEAR(full_date)                                AS week_of_year,
    MONTH(full_date)                                     AS month_number,
    MONTHNAME(full_date)                                 AS month_name,
    LEFT(MONTHNAME(full_date), 3)                        AS month_short,
    QUARTER(full_date)                                   AS quarter_number,
    'Q' || QUARTER(full_date)                            AS quarter_name,
    YEAR(full_date)                                      AS year_number,
    CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE                                                AS is_holiday,
    YEAR(full_date)                                      AS fiscal_year,
    QUARTER(full_date)                                   AS fiscal_quarter,
    MONTH(full_date)                                     AS fiscal_month
FROM date_series;

-- ============================================================
-- DIM LOCATION
-- ============================================================
CREATE OR REPLACE TABLE DIM_LOCATION (
    location_sk         NUMBER AUTOINCREMENT PRIMARY KEY,
    location_id         NUMBER          NOT NULL,
    street_address      VARCHAR(200),
    city                VARCHAR(100),
    state               VARCHAR(100),
    zip_code            VARCHAR(20),
    country             VARCHAR(100),
    region              VARCHAR(50),
    _dw_inserted_ts     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- DIM STORE (SCD Type 2)
-- Tracks historical changes: location move, manager change, store type change
-- ============================================================
CREATE OR REPLACE TABLE DIM_STORE (
    store_sk            NUMBER AUTOINCREMENT PRIMARY KEY,
    store_id            NUMBER          NOT NULL,   -- natural key
    store_code          VARCHAR(20),
    store_name          VARCHAR(200),
    store_type          VARCHAR(50),
    location_id         NUMBER,
    city                VARCHAR(100),
    state               VARCHAR(100),
    region              VARCHAR(50),
    manager_name        VARCHAR(200),
    phone_number        VARCHAR(20),
    email               VARCHAR(200),
    open_date           DATE,
    close_date          DATE,
    is_active           BOOLEAN,
    square_footage      NUMBER,
    -- SCD Type 2 metadata
    scd_effective_date  DATE            NOT NULL,
    scd_expiry_date     DATE            NOT NULL DEFAULT '9999-12-31',
    scd_is_current      BOOLEAN         NOT NULL DEFAULT TRUE,
    scd_action          VARCHAR(10),    -- INSERT, UPDATE
    _dw_inserted_ts     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- DIM CUSTOMER (SCD Type 2)
-- Tracks loyalty tier changes, address changes
-- ============================================================
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_sk         NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_id         NUMBER          NOT NULL,   -- natural key
    customer_code       VARCHAR(20),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    full_name           VARCHAR(200),
    email               VARCHAR(200),
    phone_number        VARCHAR(20),
    date_of_birth       DATE,
    age_group           VARCHAR(20),    -- YOUTH, ADULT, SENIOR
    gender              VARCHAR(10),
    loyalty_tier        VARCHAR(20),
    loyalty_points      NUMBER,
    registration_date   DATE,
    city                VARCHAR(100),
    state               VARCHAR(100),
    region              VARCHAR(50),
    is_active           BOOLEAN,
    -- SCD Type 2 metadata
    scd_effective_date  DATE            NOT NULL,
    scd_expiry_date     DATE            NOT NULL DEFAULT '9999-12-31',
    scd_is_current      BOOLEAN         NOT NULL DEFAULT TRUE,
    scd_action          VARCHAR(10),
    _dw_inserted_ts     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- DIM PRODUCT (SCD Type 2)
-- Tracks price changes, category reassignment
-- ============================================================
CREATE OR REPLACE TABLE DIM_PRODUCT (
    product_sk          NUMBER AUTOINCREMENT PRIMARY KEY,
    product_id          NUMBER          NOT NULL,   -- natural key
    product_code        VARCHAR(50),
    sku                 VARCHAR(100),
    product_name        VARCHAR(300),
    category_id         NUMBER,
    category_name       VARCHAR(200),
    parent_category_name VARCHAR(200),
    brand               VARCHAR(100),
    size                VARCHAR(50),
    color               VARCHAR(50),
    unit_cost           NUMBER(10,2),
    unit_price          NUMBER(10,2),
    gross_margin_pct    NUMBER(6,4),
    discount_pct        NUMBER(5,2),
    weight_kg           NUMBER(8,3),
    is_perishable       BOOLEAN,
    is_active           BOOLEAN,
    launch_date         DATE,
    -- SCD Type 2 metadata
    scd_effective_date  DATE            NOT NULL,
    scd_expiry_date     DATE            NOT NULL DEFAULT '9999-12-31',
    scd_is_current      BOOLEAN         NOT NULL DEFAULT TRUE,
    scd_action          VARCHAR(10),
    _dw_inserted_ts     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP(),
    _dw_updated_ts      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- DIM PAYMENT METHOD
-- ============================================================
CREATE OR REPLACE TABLE DIM_PAYMENT_METHOD (
    payment_method_sk   NUMBER AUTOINCREMENT PRIMARY KEY,
    payment_method_code VARCHAR(50)     NOT NULL UNIQUE,
    payment_method_name VARCHAR(100),
    payment_category    VARCHAR(50),   -- CASH, CARD, DIGITAL, LOYALTY
    is_digital          BOOLEAN,
    _dw_inserted_ts     TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO DIM_PAYMENT_METHOD (payment_method_code, payment_method_name, payment_category, is_digital)
VALUES
    ('CASH',           'Cash',               'CASH',    FALSE),
    ('CREDIT_CARD',    'Credit Card',         'CARD',    FALSE),
    ('DEBIT_CARD',     'Debit Card',          'CARD',    FALSE),
    ('DIGITAL_WALLET', 'Digital Wallet',      'DIGITAL', TRUE),
    ('GIFT_CARD',      'Gift Card',           'DIGITAL', TRUE),
    ('LOYALTY_POINTS', 'Loyalty Points',      'LOYALTY', TRUE),
    ('BANK_TRANSFER',  'Bank Transfer',       'DIGITAL', TRUE);

-- ============================================================
-- DIM CHANNEL
-- ============================================================
CREATE OR REPLACE TABLE DIM_CHANNEL (
    channel_sk      NUMBER AUTOINCREMENT PRIMARY KEY,
    channel_code    VARCHAR(30)     NOT NULL UNIQUE,
    channel_name    VARCHAR(100),
    channel_type    VARCHAR(50),
    is_digital      BOOLEAN,
    _dw_inserted_ts TIMESTAMP       DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO DIM_CHANNEL (channel_code, channel_name, channel_type, is_digital)
VALUES
    ('IN_STORE', 'In-Store',     'PHYSICAL', FALSE),
    ('ONLINE',   'Online',       'DIGITAL',  TRUE),
    ('MOBILE',   'Mobile App',   'DIGITAL',  TRUE),
    ('PHONE',    'Phone Order',  'MIXED',    FALSE);
