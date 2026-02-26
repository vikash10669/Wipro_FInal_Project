-- ============================================================
-- RETAIL CHAIN DATA WAREHOUSE - SETUP
-- Creates Database, Warehouse, Roles, and Schemas
-- ============================================================

-- Step 1: Use SYSADMIN to create the warehouse
USE ROLE SYSADMIN;

-- Create dedicated virtual warehouse
CREATE WAREHOUSE IF NOT EXISTS RETAIL_WH
    WITH WAREHOUSE_SIZE    = 'X-SMALL'
         AUTO_SUSPEND      = 60
         AUTO_RESUME       = TRUE
         INITIALLY_SUSPENDED = TRUE
         COMMENT           = 'Warehouse for Retail Chain DW project';

-- Create the main database
CREATE DATABASE IF NOT EXISTS RETAIL_DW
    COMMENT = 'Retail Chain Data Warehouse - 3-Layer Architecture';

-- ============================================================
-- 3-Layer Architecture Schemas:
--   STAGE_LAYER     -> Raw ingestion (as-is from source)
--   CLEAN_LAYER     -> Curated / cleansed data
--   CONSUMPTION_LAYER -> Star schema for analytics
-- ============================================================

CREATE SCHEMA IF NOT EXISTS RETAIL_DW.STAGE_LAYER
    COMMENT = 'Layer 1: Raw data as landed from source systems';

CREATE SCHEMA IF NOT EXISTS RETAIL_DW.CLEAN_LAYER
    COMMENT = 'Layer 2: Cleansed, conformed, deduplicated data';

CREATE SCHEMA IF NOT EXISTS RETAIL_DW.CONSUMPTION_LAYER
    COMMENT = 'Layer 3: Dimensional model (Star Schema) for BI/analytics';

-- ============================================================
-- Internal Stages for CSV ingestion
-- ============================================================
USE SCHEMA RETAIL_DW.STAGE_LAYER;

CREATE STAGE IF NOT EXISTS RETAIL_DW.STAGE_LAYER.STG_LOCATION_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL', 'null'))
    COMMENT = 'Stage for location CSV files';

CREATE STAGE IF NOT EXISTS RETAIL_DW.STAGE_LAYER.STG_STORE_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL', 'null'))
    COMMENT = 'Stage for store CSV files';

CREATE STAGE IF NOT EXISTS RETAIL_DW.STAGE_LAYER.STG_CUSTOMER_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL', 'null'))
    COMMENT = 'Stage for customer CSV files';

CREATE STAGE IF NOT EXISTS RETAIL_DW.STAGE_LAYER.STG_PRODUCT_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL', 'null'))
    COMMENT = 'Stage for product CSV files';

CREATE STAGE IF NOT EXISTS RETAIL_DW.STAGE_LAYER.STG_SALES_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL', 'null'))
    COMMENT = 'Stage for sales transaction CSV files';

CREATE STAGE IF NOT EXISTS RETAIL_DW.STAGE_LAYER.STG_INVENTORY_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL', 'null'))
    COMMENT = 'Stage for inventory CSV files';

CREATE STAGE IF NOT EXISTS RETAIL_DW.STAGE_LAYER.STG_PAYMENT_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL', 'null'))
    COMMENT = 'Stage for payment CSV files';

CREATE STAGE IF NOT EXISTS RETAIL_DW.STAGE_LAYER.STG_RETURN_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('', 'NULL', 'null'))
    COMMENT = 'Stage for return transaction CSV files';
