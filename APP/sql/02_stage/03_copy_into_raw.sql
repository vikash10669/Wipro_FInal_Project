-- ============================================================
-- STAGE LAYER - COPY INTO COMMANDS
-- Load CSV files from internal stages into raw tables
-- ============================================================

USE DATABASE RETAIL_DW;
USE SCHEMA STAGE_LAYER;
USE WAREHOUSE RETAIL_WH;

-- ============================================================
-- Step 1: PUT local CSV files to Snowflake stages
-- (Run from SnowSQL CLI or Python connector)
-- ============================================================
-- PUT file:///path/to/data/location.csv           @STG_LOCATION_STAGE;
-- PUT file:///path/to/data/store.csv              @STG_STORE_STAGE;
-- PUT file:///path/to/data/customer.csv           @STG_CUSTOMER_STAGE;
-- PUT file:///path/to/data/product_category.csv   @STG_PRODUCT_STAGE;
-- PUT file:///path/to/data/product.csv            @STG_PRODUCT_STAGE;
-- PUT file:///path/to/data/sales_transaction.csv  @STG_SALES_STAGE;
-- PUT file:///path/to/data/sales_line.csv         @STG_SALES_STAGE;
-- PUT file:///path/to/data/payment.csv            @STG_PAYMENT_STAGE;
-- PUT file:///path/to/data/return_transaction.csv @STG_RETURN_STAGE;
-- PUT file:///path/to/data/inventory.csv          @STG_INVENTORY_STAGE;

-- ============================================================
-- Step 2: COPY INTO raw tables
-- ============================================================

COPY INTO STG_LOCATION_RAW (
    location_id, street_address, city, state, zip_code, country, region,
    created_at, updated_at, _stg_file_name
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        METADATA$FILENAME
    FROM @STG_LOCATION_STAGE
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL'))
ON_ERROR = 'CONTINUE';

COPY INTO STG_STORE_RAW (
    store_id, store_code, store_name, store_type, location_id, manager_name,
    phone_number, email, open_date, close_date, is_active, square_footage,
    created_at, updated_at, _stg_file_name
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
        METADATA$FILENAME
    FROM @STG_STORE_STAGE
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL'))
ON_ERROR = 'CONTINUE';

COPY INTO STG_CUSTOMER_RAW (
    customer_id, customer_code, first_name, last_name, email, phone_number,
    date_of_birth, gender, loyalty_tier, loyalty_points, registration_date,
    location_id, is_active, created_at, updated_at, _stg_file_name
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
        METADATA$FILENAME
    FROM @STG_CUSTOMER_STAGE
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL'))
ON_ERROR = 'CONTINUE';

COPY INTO STG_PRODUCT_RAW (
    product_id, product_code, sku, product_name, category_id, supplier_id,
    unit_cost, unit_price, discount_pct, weight_kg, brand, size, color,
    is_perishable, is_active, launch_date, discontinue_date, created_at, updated_at,
    _stg_file_name
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
        METADATA$FILENAME
    FROM @STG_PRODUCT_STAGE
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL'))
ON_ERROR = 'CONTINUE';

COPY INTO STG_SALES_TRANSACTION_RAW (
    transaction_id, transaction_code, transaction_date, store_id, customer_id,
    cashier_id, transaction_type, channel, subtotal_amount, discount_amount,
    tax_amount, total_amount, loyalty_points_earned, loyalty_points_redeemed,
    notes, created_at, _stg_file_name
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
        METADATA$FILENAME
    FROM @STG_SALES_STAGE
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL'))
ON_ERROR = 'CONTINUE';

COPY INTO STG_SALES_LINE_RAW (
    line_id, transaction_id, line_number, product_id, quantity,
    unit_price, unit_cost, discount_pct, discount_amount, line_total_amount,
    line_cost_amount, tax_rate, tax_amount, created_at, _stg_file_name
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
        METADATA$FILENAME
    FROM @STG_SALES_STAGE
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL'))
ON_ERROR = 'CONTINUE';

COPY INTO STG_PAYMENT_RAW (
    payment_id, transaction_id, payment_method, payment_amount, payment_status,
    payment_reference, payment_date, card_last_four, created_at, _stg_file_name
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        METADATA$FILENAME
    FROM @STG_PAYMENT_STAGE
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL'))
ON_ERROR = 'CONTINUE';

COPY INTO STG_RETURN_RAW (
    return_id, return_code, original_transaction_id, return_date, store_id,
    customer_id, return_reason, refund_method, refund_amount, is_restocked,
    created_at, _stg_file_name
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
        METADATA$FILENAME
    FROM @STG_RETURN_STAGE
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL'))
ON_ERROR = 'CONTINUE';

COPY INTO STG_INVENTORY_RAW (
    inventory_id, store_id, product_id, quantity_on_hand, quantity_reserved,
    quantity_available, reorder_point, reorder_quantity, last_restock_date,
    last_sold_date, snapshot_date, created_at, updated_at, _stg_file_name
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
        METADATA$FILENAME
    FROM @STG_INVENTORY_STAGE
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL'))
ON_ERROR = 'CONTINUE';

-- ============================================================
-- Verify row counts after loading
-- ============================================================
SELECT 'STG_LOCATION_RAW'       AS table_name, COUNT(*) AS row_count FROM STG_LOCATION_RAW
UNION ALL
SELECT 'STG_STORE_RAW',         COUNT(*) FROM STG_STORE_RAW
UNION ALL
SELECT 'STG_CUSTOMER_RAW',      COUNT(*) FROM STG_CUSTOMER_RAW
UNION ALL
SELECT 'STG_PRODUCT_RAW',       COUNT(*) FROM STG_PRODUCT_RAW
UNION ALL
SELECT 'STG_SALES_TRANSACTION_RAW', COUNT(*) FROM STG_SALES_TRANSACTION_RAW
UNION ALL
SELECT 'STG_SALES_LINE_RAW',    COUNT(*) FROM STG_SALES_LINE_RAW
UNION ALL
SELECT 'STG_PAYMENT_RAW',       COUNT(*) FROM STG_PAYMENT_RAW
UNION ALL
SELECT 'STG_RETURN_RAW',        COUNT(*) FROM STG_RETURN_RAW
UNION ALL
SELECT 'STG_INVENTORY_RAW',     COUNT(*) FROM STG_INVENTORY_RAW
ORDER BY 1;
