-- ============================================================
-- SCD TYPE 2 MERGE SCRIPTS
-- Clean Layer → Consumption Layer (Dimension population)
-- ============================================================

USE DATABASE RETAIL_DW;
USE SCHEMA CONSUMPTION_LAYER;
USE WAREHOUSE RETAIL_WH;

-- ============================================================
-- SCD Type 2: DIM_STORE
-- Tracks: location_id (store moves), manager_name, store_type
-- ============================================================
MERGE INTO DIM_STORE tgt
USING (
    SELECT
        s.store_id,
        s.store_code,
        s.store_name,
        s.store_type,
        s.location_id,
        l.city,
        l.state,
        l.region,
        s.manager_name,
        s.phone_number,
        s.email,
        s.open_date,
        s.close_date,
        s.is_active,
        s.square_footage,
        CURRENT_DATE() AS effective_date
    FROM CLEAN_LAYER.CLN_STORE s
    LEFT JOIN CLEAN_LAYER.CLN_LOCATION l ON s.location_id = l.location_id
    WHERE s._is_deleted = FALSE
) src
ON tgt.store_id = src.store_id AND tgt.scd_is_current = TRUE
-- Expire the current row when tracked attributes change
WHEN MATCHED AND (
    tgt.location_id  <> src.location_id  OR
    tgt.manager_name <> src.manager_name OR
    tgt.store_type   <> src.store_type   OR
    tgt.city         <> src.city         OR
    tgt.state        <> src.state
) THEN UPDATE SET
    tgt.scd_expiry_date = DATEADD(day, -1, src.effective_date),
    tgt.scd_is_current  = FALSE,
    tgt.scd_action      = 'EXPIRE',
    tgt._dw_updated_ts  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    store_id, store_code, store_name, store_type, location_id, city, state, region,
    manager_name, phone_number, email, open_date, close_date, is_active, square_footage,
    scd_effective_date, scd_expiry_date, scd_is_current, scd_action
) VALUES (
    src.store_id, src.store_code, src.store_name, src.store_type, src.location_id,
    src.city, src.state, src.region, src.manager_name, src.phone_number,
    src.email, src.open_date, src.close_date, src.is_active, src.square_footage,
    src.effective_date, '9999-12-31', TRUE, 'INSERT'
);

-- Insert new current version after expiring old (for changed records)
INSERT INTO DIM_STORE (
    store_id, store_code, store_name, store_type, location_id, city, state, region,
    manager_name, phone_number, email, open_date, close_date, is_active, square_footage,
    scd_effective_date, scd_expiry_date, scd_is_current, scd_action
)
SELECT
    s.store_id, s.store_code, s.store_name, s.store_type, s.location_id,
    l.city, l.state, l.region, s.manager_name, s.phone_number,
    s.email, s.open_date, s.close_date, s.is_active, s.square_footage,
    CURRENT_DATE(), '9999-12-31', TRUE, 'UPDATE'
FROM CLEAN_LAYER.CLN_STORE s
LEFT JOIN CLEAN_LAYER.CLN_LOCATION l ON s.location_id = l.location_id
WHERE s._is_deleted = FALSE
  AND EXISTS (
    SELECT 1 FROM DIM_STORE d
    WHERE d.store_id = s.store_id
      AND d.scd_action = 'EXPIRE'
      AND d.scd_expiry_date = DATEADD(day, -1, CURRENT_DATE())
  )
  AND NOT EXISTS (
    SELECT 1 FROM DIM_STORE d
    WHERE d.store_id = s.store_id
      AND d.scd_is_current = TRUE
      AND d.scd_action = 'UPDATE'
      AND d.scd_effective_date = CURRENT_DATE()
  );

-- ============================================================
-- SCD Type 2: DIM_CUSTOMER
-- Tracks: loyalty_tier, location (region), is_active
-- ============================================================
MERGE INTO DIM_CUSTOMER tgt
USING (
    SELECT
        c.customer_id,
        c.customer_code,
        c.first_name,
        c.last_name,
        c.first_name || ' ' || c.last_name AS full_name,
        c.email,
        c.phone_number,
        c.date_of_birth,
        CASE
            WHEN DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) < 25 THEN 'YOUTH'
            WHEN DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) < 60 THEN 'ADULT'
            ELSE 'SENIOR'
        END AS age_group,
        c.gender,
        c.loyalty_tier,
        c.loyalty_points,
        c.registration_date,
        l.city,
        l.state,
        l.region,
        c.is_active,
        CURRENT_DATE() AS effective_date
    FROM CLEAN_LAYER.CLN_CUSTOMER c
    LEFT JOIN CLEAN_LAYER.CLN_LOCATION l ON c.location_id = l.location_id
    WHERE c._is_deleted = FALSE
) src
ON tgt.customer_id = src.customer_id AND tgt.scd_is_current = TRUE
WHEN MATCHED AND (
    tgt.loyalty_tier <> src.loyalty_tier OR
    tgt.region       <> src.region       OR
    tgt.city         <> src.city         OR
    tgt.is_active    <> src.is_active
) THEN UPDATE SET
    tgt.scd_expiry_date = DATEADD(day, -1, src.effective_date),
    tgt.scd_is_current  = FALSE,
    tgt.scd_action      = 'EXPIRE',
    tgt._dw_updated_ts  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    customer_id, customer_code, first_name, last_name, full_name, email, phone_number,
    date_of_birth, age_group, gender, loyalty_tier, loyalty_points, registration_date,
    city, state, region, is_active, scd_effective_date, scd_expiry_date, scd_is_current, scd_action
) VALUES (
    src.customer_id, src.customer_code, src.first_name, src.last_name, src.full_name,
    src.email, src.phone_number, src.date_of_birth, src.age_group, src.gender,
    src.loyalty_tier, src.loyalty_points, src.registration_date,
    src.city, src.state, src.region, src.is_active,
    src.effective_date, '9999-12-31', TRUE, 'INSERT'
);

-- Insert new version after expiry
INSERT INTO DIM_CUSTOMER (
    customer_id, customer_code, first_name, last_name, full_name, email, phone_number,
    date_of_birth, age_group, gender, loyalty_tier, loyalty_points, registration_date,
    city, state, region, is_active, scd_effective_date, scd_expiry_date, scd_is_current, scd_action
)
SELECT
    c.customer_id, c.customer_code, c.first_name, c.last_name,
    c.first_name || ' ' || c.last_name,
    c.email, c.phone_number, c.date_of_birth,
    CASE
        WHEN DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) < 25 THEN 'YOUTH'
        WHEN DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) < 60 THEN 'ADULT'
        ELSE 'SENIOR'
    END,
    c.gender, c.loyalty_tier, c.loyalty_points, c.registration_date,
    l.city, l.state, l.region, c.is_active,
    CURRENT_DATE(), '9999-12-31', TRUE, 'UPDATE'
FROM CLEAN_LAYER.CLN_CUSTOMER c
LEFT JOIN CLEAN_LAYER.CLN_LOCATION l ON c.location_id = l.location_id
WHERE c._is_deleted = FALSE
  AND EXISTS (
    SELECT 1 FROM DIM_CUSTOMER d
    WHERE d.customer_id = c.customer_id
      AND d.scd_action = 'EXPIRE'
      AND d.scd_expiry_date = DATEADD(day, -1, CURRENT_DATE())
  )
  AND NOT EXISTS (
    SELECT 1 FROM DIM_CUSTOMER d
    WHERE d.customer_id = c.customer_id
      AND d.scd_is_current = TRUE
      AND d.scd_effective_date = CURRENT_DATE()
  );

-- ============================================================
-- SCD Type 2: DIM_PRODUCT
-- Tracks: unit_price (price changes), category, is_active
-- ============================================================
MERGE INTO DIM_PRODUCT tgt
USING (
    SELECT
        p.product_id,
        p.product_code,
        p.sku,
        p.product_name,
        p.category_id,
        pc.category_name,
        pcp.category_name   AS parent_category_name,
        p.brand,
        p.size,
        p.color,
        p.unit_cost,
        p.unit_price,
        ROUND((p.unit_price - p.unit_cost) / NULLIF(p.unit_price, 0), 4) AS gross_margin_pct,
        p.discount_pct,
        p.weight_kg,
        p.is_perishable,
        p.is_active,
        p.launch_date,
        CURRENT_DATE() AS effective_date
    FROM CLEAN_LAYER.CLN_PRODUCT p
    LEFT JOIN CLEAN_LAYER.CLN_PRODUCT_CATEGORY pc  ON p.category_id = pc.category_id
    LEFT JOIN CLEAN_LAYER.CLN_PRODUCT_CATEGORY pcp ON pc.parent_category_id = pcp.category_id
    WHERE p._is_deleted = FALSE
) src
ON tgt.product_id = src.product_id AND tgt.scd_is_current = TRUE
WHEN MATCHED AND (
    tgt.unit_price    <> src.unit_price    OR
    tgt.unit_cost     <> src.unit_cost     OR
    tgt.category_name <> src.category_name OR
    tgt.is_active     <> src.is_active
) THEN UPDATE SET
    tgt.scd_expiry_date = DATEADD(day, -1, src.effective_date),
    tgt.scd_is_current  = FALSE,
    tgt.scd_action      = 'EXPIRE',
    tgt._dw_updated_ts  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    product_id, product_code, sku, product_name, category_id, category_name,
    parent_category_name, brand, size, color, unit_cost, unit_price,
    gross_margin_pct, discount_pct, weight_kg, is_perishable, is_active, launch_date,
    scd_effective_date, scd_expiry_date, scd_is_current, scd_action
) VALUES (
    src.product_id, src.product_code, src.sku, src.product_name, src.category_id,
    src.category_name, src.parent_category_name, src.brand, src.size, src.color,
    src.unit_cost, src.unit_price, src.gross_margin_pct, src.discount_pct,
    src.weight_kg, src.is_perishable, src.is_active, src.launch_date,
    src.effective_date, '9999-12-31', TRUE, 'INSERT'
);

-- Insert new version for changed product records
INSERT INTO DIM_PRODUCT (
    product_id, product_code, sku, product_name, category_id, category_name,
    parent_category_name, brand, size, color, unit_cost, unit_price,
    gross_margin_pct, discount_pct, weight_kg, is_perishable, is_active, launch_date,
    scd_effective_date, scd_expiry_date, scd_is_current, scd_action
)
SELECT
    p.product_id, p.product_code, p.sku, p.product_name, p.category_id,
    pc.category_name, pcp.category_name,
    p.brand, p.size, p.color, p.unit_cost, p.unit_price,
    ROUND((p.unit_price - p.unit_cost) / NULLIF(p.unit_price, 0), 4),
    p.discount_pct, p.weight_kg, p.is_perishable, p.is_active, p.launch_date,
    CURRENT_DATE(), '9999-12-31', TRUE, 'UPDATE'
FROM CLEAN_LAYER.CLN_PRODUCT p
LEFT JOIN CLEAN_LAYER.CLN_PRODUCT_CATEGORY pc  ON p.category_id = pc.category_id
LEFT JOIN CLEAN_LAYER.CLN_PRODUCT_CATEGORY pcp ON pc.parent_category_id = pcp.category_id
WHERE p._is_deleted = FALSE
  AND EXISTS (
    SELECT 1 FROM DIM_PRODUCT d
    WHERE d.product_id = p.product_id
      AND d.scd_action = 'EXPIRE'
      AND d.scd_expiry_date = DATEADD(day, -1, CURRENT_DATE())
  )
  AND NOT EXISTS (
    SELECT 1 FROM DIM_PRODUCT d
    WHERE d.product_id = p.product_id
      AND d.scd_is_current = TRUE
      AND d.scd_effective_date = CURRENT_DATE()
  );
