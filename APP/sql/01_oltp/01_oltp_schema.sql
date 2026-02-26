-- ============================================================
-- RETAIL CHAIN - OLTP DATABASE SCHEMA
-- Source Transactional System (OLTP)
-- ============================================================
-- ER Diagram Description:
--
--  LOCATION (1) ----< STORE (1) ----< SALES_TRANSACTION (M)
--  CUSTOMER (1) ----< SALES_TRANSACTION (M)
--  SALES_TRANSACTION (1) ----< SALES_TRANSACTION_LINE (M) >---- PRODUCT
--  SALES_TRANSACTION (1) ----< PAYMENT (M)
--  SALES_TRANSACTION (1) ----< RETURN_TRANSACTION (M)
--  PRODUCT (M) >---- PRODUCT_CATEGORY
--  PRODUCT (1) ----< INVENTORY (M) >---- STORE
--  SUPPLIER (1) ----< PRODUCT (M)
-- ============================================================

-- ============================================================
-- LOCATION TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS location (
    location_id     SERIAL          PRIMARY KEY,
    street_address  VARCHAR(200)    NOT NULL,
    city            VARCHAR(100)    NOT NULL,
    state           VARCHAR(100)    NOT NULL,
    zip_code        VARCHAR(20)     NOT NULL,
    country         VARCHAR(100)    NOT NULL DEFAULT 'USA',
    region          VARCHAR(50)     NOT NULL,   -- NORTH, SOUTH, EAST, WEST
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- STORE TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS store (
    store_id        SERIAL          PRIMARY KEY,
    store_code      VARCHAR(20)     NOT NULL UNIQUE,
    store_name      VARCHAR(200)    NOT NULL,
    store_type      VARCHAR(50)     NOT NULL,   -- FLAGSHIP, STANDARD, OUTLET, KIOSK
    location_id     INT             NOT NULL REFERENCES location(location_id),
    manager_name    VARCHAR(200),
    phone_number    VARCHAR(20),
    email           VARCHAR(200),
    open_date       DATE            NOT NULL,
    close_date      DATE,
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    square_footage  INT,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- CUSTOMER TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS customer (
    customer_id         SERIAL          PRIMARY KEY,
    customer_code       VARCHAR(20)     NOT NULL UNIQUE,
    first_name          VARCHAR(100)    NOT NULL,
    last_name           VARCHAR(100)    NOT NULL,
    email               VARCHAR(200)    NOT NULL UNIQUE,
    phone_number        VARCHAR(20),
    date_of_birth       DATE,
    gender              VARCHAR(10),
    loyalty_tier        VARCHAR(20)     NOT NULL DEFAULT 'BRONZE',  -- BRONZE, SILVER, GOLD, PLATINUM
    loyalty_points      INT             NOT NULL DEFAULT 0,
    registration_date   DATE            NOT NULL,
    location_id         INT             REFERENCES location(location_id),
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- PRODUCT CATEGORY TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS product_category (
    category_id         SERIAL          PRIMARY KEY,
    category_code       VARCHAR(20)     NOT NULL UNIQUE,
    category_name       VARCHAR(200)    NOT NULL,
    parent_category_id  INT             REFERENCES product_category(category_id),
    description         TEXT,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- SUPPLIER TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS supplier (
    supplier_id     SERIAL          PRIMARY KEY,
    supplier_code   VARCHAR(20)     NOT NULL UNIQUE,
    supplier_name   VARCHAR(200)    NOT NULL,
    contact_name    VARCHAR(200),
    email           VARCHAR(200),
    phone_number    VARCHAR(20),
    address         VARCHAR(300),
    country         VARCHAR(100),
    payment_terms   VARCHAR(100),
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- PRODUCT TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS product (
    product_id          SERIAL          PRIMARY KEY,
    product_code        VARCHAR(50)     NOT NULL UNIQUE,
    sku                 VARCHAR(100)    NOT NULL UNIQUE,
    product_name        VARCHAR(300)    NOT NULL,
    category_id         INT             NOT NULL REFERENCES product_category(category_id),
    supplier_id         INT             REFERENCES supplier(supplier_id),
    unit_cost           DECIMAL(10,2)   NOT NULL,
    unit_price          DECIMAL(10,2)   NOT NULL,
    discount_pct        DECIMAL(5,2)    NOT NULL DEFAULT 0.00,
    weight_kg           DECIMAL(8,3),
    brand               VARCHAR(100),
    size                VARCHAR(50),
    color               VARCHAR(50),
    is_perishable       BOOLEAN         NOT NULL DEFAULT FALSE,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    launch_date         DATE,
    discontinue_date    DATE,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- SALES TRANSACTION TABLE (Header)
-- ============================================================
CREATE TABLE IF NOT EXISTS sales_transaction (
    transaction_id          SERIAL          PRIMARY KEY,
    transaction_code        VARCHAR(50)     NOT NULL UNIQUE,
    transaction_date        TIMESTAMP       NOT NULL,
    store_id                INT             NOT NULL REFERENCES store(store_id),
    customer_id             INT             REFERENCES customer(customer_id),
    cashier_id              INT,
    transaction_type        VARCHAR(20)     NOT NULL DEFAULT 'SALE',  -- SALE, RETURN, EXCHANGE
    channel                 VARCHAR(30)     NOT NULL DEFAULT 'IN_STORE',  -- IN_STORE, ONLINE, MOBILE
    subtotal_amount         DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    discount_amount         DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    tax_amount              DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    total_amount            DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    loyalty_points_earned   INT             NOT NULL DEFAULT 0,
    loyalty_points_redeemed INT             NOT NULL DEFAULT 0,
    notes                   TEXT,
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- SALES TRANSACTION LINE TABLE (Detail)
-- ============================================================
CREATE TABLE IF NOT EXISTS sales_transaction_line (
    line_id             SERIAL          PRIMARY KEY,
    transaction_id      INT             NOT NULL REFERENCES sales_transaction(transaction_id),
    line_number         INT             NOT NULL,
    product_id          INT             NOT NULL REFERENCES product(product_id),
    quantity            INT             NOT NULL,
    unit_price          DECIMAL(10,2)   NOT NULL,
    unit_cost           DECIMAL(10,2)   NOT NULL,
    discount_pct        DECIMAL(5,2)    NOT NULL DEFAULT 0.00,
    discount_amount     DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    line_total_amount   DECIMAL(12,2)   NOT NULL,
    line_cost_amount    DECIMAL(12,2)   NOT NULL,
    tax_rate            DECIMAL(5,2)    NOT NULL DEFAULT 0.00,
    tax_amount          DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- PAYMENT TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS payment (
    payment_id          SERIAL          PRIMARY KEY,
    transaction_id      INT             NOT NULL REFERENCES sales_transaction(transaction_id),
    payment_method      VARCHAR(50)     NOT NULL,   -- CASH, CREDIT_CARD, DEBIT_CARD, DIGITAL_WALLET, GIFT_CARD
    payment_amount      DECIMAL(12,2)   NOT NULL,
    payment_status      VARCHAR(20)     NOT NULL DEFAULT 'COMPLETED',  -- PENDING, COMPLETED, FAILED, REFUNDED
    payment_reference   VARCHAR(100),
    payment_date        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    card_last_four      VARCHAR(4),
    created_at          TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- RETURN TRANSACTION TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS return_transaction (
    return_id               SERIAL          PRIMARY KEY,
    return_code             VARCHAR(50)     NOT NULL UNIQUE,
    original_transaction_id INT             NOT NULL REFERENCES sales_transaction(transaction_id),
    return_date             TIMESTAMP       NOT NULL,
    store_id                INT             NOT NULL REFERENCES store(store_id),
    customer_id             INT             REFERENCES customer(customer_id),
    return_reason           VARCHAR(100),
    refund_method           VARCHAR(50),
    refund_amount           DECIMAL(12,2)   NOT NULL,
    is_restocked            BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- INVENTORY TABLE
-- ============================================================
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id            SERIAL          PRIMARY KEY,
    store_id                INT             NOT NULL REFERENCES store(store_id),
    product_id              INT             NOT NULL REFERENCES product(product_id),
    quantity_on_hand        INT             NOT NULL DEFAULT 0,
    quantity_reserved       INT             NOT NULL DEFAULT 0,
    quantity_available      INT             NOT NULL DEFAULT 0,
    reorder_point           INT             NOT NULL DEFAULT 10,
    reorder_quantity        INT             NOT NULL DEFAULT 50,
    last_restock_date       DATE,
    last_sold_date          DATE,
    snapshot_date           DATE            NOT NULL DEFAULT CURRENT_DATE,
    created_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (store_id, product_id, snapshot_date)
);
