-- ============================================================
-- STAGE → CLEAN LAYER MERGE TRANSFORMATIONS
-- Cleans, validates, and casts data from Stage raw tables
-- ============================================================

USE DATABASE RETAIL_DW;
USE WAREHOUSE RETAIL_WH;

-- ============================================================
-- MERGE: Location
-- ============================================================
MERGE INTO CLEAN_LAYER.CLN_LOCATION tgt
USING (
    SELECT DISTINCT
        TRY_TO_NUMBER(location_id)                              AS location_id,
        TRIM(street_address)                                    AS street_address,
        INITCAP(TRIM(city))                                     AS city,
        UPPER(TRIM(state))                                      AS state,
        TRIM(zip_code)                                          AS zip_code,
        INITCAP(TRIM(COALESCE(country, 'USA')))                 AS country,
        UPPER(TRIM(region))                                     AS region,
        TRY_TO_TIMESTAMP(created_at)                            AS created_at,
        TRY_TO_TIMESTAMP(updated_at)                            AS updated_at
    FROM STAGE_LAYER.STG_LOCATION_RAW
    WHERE location_id IS NOT NULL
      AND TRY_TO_NUMBER(location_id) IS NOT NULL
) src
ON tgt.location_id = src.location_id
WHEN MATCHED AND (
    tgt.street_address <> src.street_address OR
    tgt.city           <> src.city           OR
    tgt.state          <> src.state          OR
    tgt.region         <> src.region
) THEN UPDATE SET
    tgt.street_address  = src.street_address,
    tgt.city            = src.city,
    tgt.state           = src.state,
    tgt.zip_code        = src.zip_code,
    tgt.country         = src.country,
    tgt.region          = src.region,
    tgt.updated_at      = src.updated_at,
    tgt._dw_updated_ts  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    location_id, street_address, city, state, zip_code, country, region,
    created_at, updated_at
) VALUES (
    src.location_id, src.street_address, src.city, src.state, src.zip_code,
    src.country, src.region, src.created_at, src.updated_at
);

-- ============================================================
-- MERGE: Store
-- ============================================================
MERGE INTO CLEAN_LAYER.CLN_STORE tgt
USING (
    SELECT DISTINCT
        TRY_TO_NUMBER(store_id)                                 AS store_id,
        UPPER(TRIM(store_code))                                 AS store_code,
        TRIM(store_name)                                        AS store_name,
        UPPER(TRIM(store_type))                                 AS store_type,
        TRY_TO_NUMBER(location_id)                             AS location_id,
        TRIM(manager_name)                                      AS manager_name,
        TRIM(phone_number)                                      AS phone_number,
        LOWER(TRIM(email))                                      AS email,
        TRY_TO_DATE(open_date)                                  AS open_date,
        TRY_TO_DATE(close_date)                                 AS close_date,
        CASE WHEN UPPER(is_active) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_active,
        TRY_TO_NUMBER(square_footage)                          AS square_footage,
        TRY_TO_TIMESTAMP(created_at)                           AS created_at,
        TRY_TO_TIMESTAMP(updated_at)                           AS updated_at
    FROM STAGE_LAYER.STG_STORE_RAW
    WHERE store_id IS NOT NULL
      AND TRY_TO_NUMBER(store_id) IS NOT NULL
) src
ON tgt.store_id = src.store_id
WHEN MATCHED AND (
    tgt.store_name   <> src.store_name  OR
    tgt.location_id  <> src.location_id OR
    tgt.manager_name <> src.manager_name OR
    tgt.is_active    <> src.is_active
) THEN UPDATE SET
    tgt.store_name      = src.store_name,
    tgt.store_type      = src.store_type,
    tgt.location_id     = src.location_id,
    tgt.manager_name    = src.manager_name,
    tgt.phone_number    = src.phone_number,
    tgt.email           = src.email,
    tgt.close_date      = src.close_date,
    tgt.is_active       = src.is_active,
    tgt.square_footage  = src.square_footage,
    tgt.updated_at      = src.updated_at,
    tgt._dw_updated_ts  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    store_id, store_code, store_name, store_type, location_id, manager_name,
    phone_number, email, open_date, close_date, is_active, square_footage,
    created_at, updated_at
) VALUES (
    src.store_id, src.store_code, src.store_name, src.store_type, src.location_id,
    src.manager_name, src.phone_number, src.email, src.open_date, src.close_date,
    src.is_active, src.square_footage, src.created_at, src.updated_at
);

-- ============================================================
-- MERGE: Customer
-- ============================================================
MERGE INTO CLEAN_LAYER.CLN_CUSTOMER tgt
USING (
    SELECT DISTINCT
        TRY_TO_NUMBER(customer_id)                             AS customer_id,
        UPPER(TRIM(customer_code))                             AS customer_code,
        INITCAP(TRIM(first_name))                              AS first_name,
        INITCAP(TRIM(last_name))                               AS last_name,
        LOWER(TRIM(email))                                     AS email,
        TRIM(phone_number)                                     AS phone_number,
        TRY_TO_DATE(date_of_birth)                             AS date_of_birth,
        UPPER(TRIM(gender))                                    AS gender,
        UPPER(TRIM(COALESCE(loyalty_tier, 'BRONZE')))          AS loyalty_tier,
        COALESCE(TRY_TO_NUMBER(loyalty_points), 0)             AS loyalty_points,
        TRY_TO_DATE(registration_date)                         AS registration_date,
        TRY_TO_NUMBER(location_id)                             AS location_id,
        CASE WHEN UPPER(is_active) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_active,
        TRY_TO_TIMESTAMP(created_at)                           AS created_at,
        TRY_TO_TIMESTAMP(updated_at)                           AS updated_at
    FROM STAGE_LAYER.STG_CUSTOMER_RAW
    WHERE customer_id IS NOT NULL
      AND TRY_TO_NUMBER(customer_id) IS NOT NULL
      AND email IS NOT NULL AND TRIM(email) != ''
) src
ON tgt.customer_id = src.customer_id
WHEN MATCHED AND (
    tgt.email          <> src.email          OR
    tgt.loyalty_tier   <> src.loyalty_tier   OR
    tgt.loyalty_points <> src.loyalty_points OR
    tgt.is_active      <> src.is_active
) THEN UPDATE SET
    tgt.first_name      = src.first_name,
    tgt.last_name       = src.last_name,
    tgt.email           = src.email,
    tgt.phone_number    = src.phone_number,
    tgt.gender          = src.gender,
    tgt.loyalty_tier    = src.loyalty_tier,
    tgt.loyalty_points  = src.loyalty_points,
    tgt.location_id     = src.location_id,
    tgt.is_active       = src.is_active,
    tgt.updated_at      = src.updated_at,
    tgt._dw_updated_ts  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    customer_id, customer_code, first_name, last_name, email, phone_number,
    date_of_birth, gender, loyalty_tier, loyalty_points, registration_date,
    location_id, is_active, created_at, updated_at
) VALUES (
    src.customer_id, src.customer_code, src.first_name, src.last_name, src.email,
    src.phone_number, src.date_of_birth, src.gender, src.loyalty_tier,
    src.loyalty_points, src.registration_date, src.location_id, src.is_active,
    src.created_at, src.updated_at
);

-- ============================================================
-- MERGE: Product
-- ============================================================
MERGE INTO CLEAN_LAYER.CLN_PRODUCT tgt
USING (
    SELECT DISTINCT
        TRY_TO_NUMBER(product_id)                              AS product_id,
        UPPER(TRIM(product_code))                              AS product_code,
        UPPER(TRIM(sku))                                       AS sku,
        TRIM(product_name)                                     AS product_name,
        TRY_TO_NUMBER(category_id)                             AS category_id,
        TRY_TO_NUMBER(supplier_id)                             AS supplier_id,
        COALESCE(TRY_TO_DECIMAL(unit_cost, 10, 2), 0)          AS unit_cost,
        COALESCE(TRY_TO_DECIMAL(unit_price, 10, 2), 0)         AS unit_price,
        COALESCE(TRY_TO_DECIMAL(discount_pct, 5, 2), 0)        AS discount_pct,
        TRY_TO_DECIMAL(weight_kg, 8, 3)                        AS weight_kg,
        INITCAP(TRIM(brand))                                   AS brand,
        TRIM(size)                                             AS size,
        INITCAP(TRIM(color))                                   AS color,
        CASE WHEN UPPER(is_perishable) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_perishable,
        CASE WHEN UPPER(is_active) IN ('TRUE','1','YES') THEN TRUE ELSE FALSE END AS is_active,
        TRY_TO_DATE(launch_date)                               AS launch_date,
        TRY_TO_DATE(discontinue_date)                          AS discontinue_date,
        TRY_TO_TIMESTAMP(created_at)                           AS created_at,
        TRY_TO_TIMESTAMP(updated_at)                           AS updated_at
    FROM STAGE_LAYER.STG_PRODUCT_RAW
    WHERE product_id IS NOT NULL
      AND TRY_TO_NUMBER(product_id) IS NOT NULL
) src
ON tgt.product_id = src.product_id
WHEN MATCHED AND (
    tgt.unit_price   <> src.unit_price   OR
    tgt.unit_cost    <> src.unit_cost    OR
    tgt.is_active    <> src.is_active    OR
    tgt.product_name <> src.product_name
) THEN UPDATE SET
    tgt.product_name    = src.product_name,
    tgt.unit_cost       = src.unit_cost,
    tgt.unit_price      = src.unit_price,
    tgt.discount_pct    = src.discount_pct,
    tgt.is_active       = src.is_active,
    tgt.discontinue_date = src.discontinue_date,
    tgt.updated_at      = src.updated_at,
    tgt._dw_updated_ts  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    product_id, product_code, sku, product_name, category_id, supplier_id,
    unit_cost, unit_price, discount_pct, weight_kg, brand, size, color,
    is_perishable, is_active, launch_date, discontinue_date, created_at, updated_at
) VALUES (
    src.product_id, src.product_code, src.sku, src.product_name, src.category_id,
    src.supplier_id, src.unit_cost, src.unit_price, src.discount_pct, src.weight_kg,
    src.brand, src.size, src.color, src.is_perishable, src.is_active,
    src.launch_date, src.discontinue_date, src.created_at, src.updated_at
);

-- ============================================================
-- MERGE: Sales Transaction
-- ============================================================
MERGE INTO CLEAN_LAYER.CLN_SALES_TRANSACTION tgt
USING (
    SELECT DISTINCT
        TRY_TO_NUMBER(transaction_id)                          AS transaction_id,
        UPPER(TRIM(transaction_code))                          AS transaction_code,
        TRY_TO_TIMESTAMP(transaction_date)                     AS transaction_date,
        TRY_TO_NUMBER(store_id)                                AS store_id,
        TRY_TO_NUMBER(customer_id)                             AS customer_id,
        TRY_TO_NUMBER(cashier_id)                              AS cashier_id,
        UPPER(COALESCE(TRIM(transaction_type), 'SALE'))        AS transaction_type,
        UPPER(COALESCE(TRIM(channel), 'IN_STORE'))             AS channel,
        COALESCE(TRY_TO_DECIMAL(subtotal_amount, 12, 2), 0)    AS subtotal_amount,
        COALESCE(TRY_TO_DECIMAL(discount_amount, 12, 2), 0)    AS discount_amount,
        COALESCE(TRY_TO_DECIMAL(tax_amount, 12, 2), 0)         AS tax_amount,
        COALESCE(TRY_TO_DECIMAL(total_amount, 12, 2), 0)       AS total_amount,
        COALESCE(TRY_TO_NUMBER(loyalty_points_earned), 0)      AS loyalty_points_earned,
        COALESCE(TRY_TO_NUMBER(loyalty_points_redeemed), 0)    AS loyalty_points_redeemed,
        TRIM(notes)                                            AS notes,
        TRY_TO_TIMESTAMP(created_at)                           AS created_at
    FROM STAGE_LAYER.STG_SALES_TRANSACTION_RAW
    WHERE transaction_id IS NOT NULL
      AND TRY_TO_NUMBER(transaction_id) IS NOT NULL
      AND TRY_TO_TIMESTAMP(transaction_date) IS NOT NULL
      AND TRY_TO_DECIMAL(total_amount, 12, 2) >= 0
) src
ON tgt.transaction_id = src.transaction_id
WHEN NOT MATCHED THEN INSERT (
    transaction_id, transaction_code, transaction_date, store_id, customer_id,
    cashier_id, transaction_type, channel, subtotal_amount, discount_amount,
    tax_amount, total_amount, loyalty_points_earned, loyalty_points_redeemed,
    notes, created_at
) VALUES (
    src.transaction_id, src.transaction_code, src.transaction_date, src.store_id,
    src.customer_id, src.cashier_id, src.transaction_type, src.channel,
    src.subtotal_amount, src.discount_amount, src.tax_amount, src.total_amount,
    src.loyalty_points_earned, src.loyalty_points_redeemed, src.notes, src.created_at
);

-- ============================================================
-- MERGE: Sales Line
-- ============================================================
MERGE INTO CLEAN_LAYER.CLN_SALES_LINE tgt
USING (
    SELECT DISTINCT
        TRY_TO_NUMBER(line_id)                                 AS line_id,
        TRY_TO_NUMBER(transaction_id)                          AS transaction_id,
        TRY_TO_NUMBER(line_number)                             AS line_number,
        TRY_TO_NUMBER(product_id)                              AS product_id,
        COALESCE(TRY_TO_NUMBER(quantity), 0)                   AS quantity,
        COALESCE(TRY_TO_DECIMAL(unit_price, 10, 2), 0)         AS unit_price,
        COALESCE(TRY_TO_DECIMAL(unit_cost, 10, 2), 0)          AS unit_cost,
        COALESCE(TRY_TO_DECIMAL(discount_pct, 5, 2), 0)        AS discount_pct,
        COALESCE(TRY_TO_DECIMAL(discount_amount, 10, 2), 0)    AS discount_amount,
        COALESCE(TRY_TO_DECIMAL(line_total_amount, 12, 2), 0)  AS line_total_amount,
        COALESCE(TRY_TO_DECIMAL(line_cost_amount, 12, 2), 0)   AS line_cost_amount,
        COALESCE(TRY_TO_DECIMAL(tax_rate, 5, 2), 0)            AS tax_rate,
        COALESCE(TRY_TO_DECIMAL(tax_amount, 10, 2), 0)         AS tax_amount,
        TRY_TO_TIMESTAMP(created_at)                           AS created_at
    FROM STAGE_LAYER.STG_SALES_LINE_RAW
    WHERE line_id IS NOT NULL
      AND TRY_TO_NUMBER(line_id) IS NOT NULL
      AND TRY_TO_NUMBER(quantity) > 0
) src
ON tgt.line_id = src.line_id
WHEN NOT MATCHED THEN INSERT (
    line_id, transaction_id, line_number, product_id, quantity,
    unit_price, unit_cost, discount_pct, discount_amount,
    line_total_amount, line_cost_amount, tax_rate, tax_amount, created_at
) VALUES (
    src.line_id, src.transaction_id, src.line_number, src.product_id, src.quantity,
    src.unit_price, src.unit_cost, src.discount_pct, src.discount_amount,
    src.line_total_amount, src.line_cost_amount, src.tax_rate, src.tax_amount, src.created_at
);

-- ============================================================
-- MERGE: Payment
-- ============================================================
MERGE INTO CLEAN_LAYER.CLN_PAYMENT tgt
USING (
    SELECT DISTINCT
        TRY_TO_NUMBER(payment_id)                              AS payment_id,
        TRY_TO_NUMBER(transaction_id)                          AS transaction_id,
        UPPER(TRIM(payment_method))                            AS payment_method,
        COALESCE(TRY_TO_DECIMAL(payment_amount, 12, 2), 0)     AS payment_amount,
        UPPER(COALESCE(TRIM(payment_status), 'COMPLETED'))     AS payment_status,
        TRIM(payment_reference)                                AS payment_reference,
        TRY_TO_TIMESTAMP(payment_date)                         AS payment_date,
        RIGHT(TRIM(card_last_four), 4)                         AS card_last_four,
        TRY_TO_TIMESTAMP(created_at)                           AS created_at
    FROM STAGE_LAYER.STG_PAYMENT_RAW
    WHERE payment_id IS NOT NULL
      AND TRY_TO_NUMBER(payment_id) IS NOT NULL
) src
ON tgt.payment_id = src.payment_id
WHEN NOT MATCHED THEN INSERT (
    payment_id, transaction_id, payment_method, payment_amount, payment_status,
    payment_reference, payment_date, card_last_four, created_at
) VALUES (
    src.payment_id, src.transaction_id, src.payment_method, src.payment_amount,
    src.payment_status, src.payment_reference, src.payment_date,
    src.card_last_four, src.created_at
);

-- ============================================================
-- MERGE: Inventory
-- ============================================================
MERGE INTO CLEAN_LAYER.CLN_INVENTORY tgt
USING (
    SELECT
        TRY_TO_NUMBER(inventory_id)                            AS inventory_id,
        TRY_TO_NUMBER(store_id)                                AS store_id,
        TRY_TO_NUMBER(product_id)                              AS product_id,
        COALESCE(TRY_TO_NUMBER(quantity_on_hand), 0)           AS quantity_on_hand,
        COALESCE(TRY_TO_NUMBER(quantity_reserved), 0)          AS quantity_reserved,
        COALESCE(TRY_TO_NUMBER(quantity_available), 0)         AS quantity_available,
        COALESCE(TRY_TO_NUMBER(reorder_point), 10)             AS reorder_point,
        COALESCE(TRY_TO_NUMBER(reorder_quantity), 50)          AS reorder_quantity,
        TRY_TO_DATE(last_restock_date)                         AS last_restock_date,
        TRY_TO_DATE(last_sold_date)                            AS last_sold_date,
        COALESCE(TRY_TO_DATE(snapshot_date), CURRENT_DATE())   AS snapshot_date,
        TRY_TO_TIMESTAMP(created_at)                           AS created_at,
        TRY_TO_TIMESTAMP(updated_at)                           AS updated_at
    FROM STAGE_LAYER.STG_INVENTORY_RAW
    WHERE inventory_id IS NOT NULL
      AND TRY_TO_NUMBER(inventory_id) IS NOT NULL
) src
ON tgt.inventory_id = src.inventory_id AND tgt.snapshot_date = src.snapshot_date
WHEN MATCHED AND tgt.quantity_on_hand <> src.quantity_on_hand THEN UPDATE SET
    tgt.quantity_on_hand    = src.quantity_on_hand,
    tgt.quantity_reserved   = src.quantity_reserved,
    tgt.quantity_available  = src.quantity_available,
    tgt.last_restock_date   = src.last_restock_date,
    tgt.last_sold_date      = src.last_sold_date,
    tgt.updated_at          = src.updated_at,
    tgt._dw_updated_ts      = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    inventory_id, store_id, product_id, quantity_on_hand, quantity_reserved,
    quantity_available, reorder_point, reorder_quantity, last_restock_date,
    last_sold_date, snapshot_date, created_at, updated_at
) VALUES (
    src.inventory_id, src.store_id, src.product_id, src.quantity_on_hand,
    src.quantity_reserved, src.quantity_available, src.reorder_point,
    src.reorder_quantity, src.last_restock_date, src.last_sold_date,
    src.snapshot_date, src.created_at, src.updated_at
);
