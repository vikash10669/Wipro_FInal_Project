"""
Snowflake Loader
Uploads generated CSVs to Snowflake internal stages and loads into raw tables.
Requires: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD env vars
"""
import os
import glob
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

STAGE_MAP = {
    'location.csv':           ('STG_LOCATION_STAGE',  'STG_LOCATION_RAW'),
    'store.csv':              ('STG_STORE_STAGE',      'STG_STORE_RAW'),
    'customer.csv':           ('STG_CUSTOMER_STAGE',   'STG_CUSTOMER_RAW'),
    'product_category.csv':   ('STG_PRODUCT_STAGE',    'STG_PRODUCT_CATEGORY_RAW'),
    'product.csv':            ('STG_PRODUCT_STAGE',    'STG_PRODUCT_RAW'),
    'sales_transaction.csv':  ('STG_SALES_STAGE',      'STG_SALES_TRANSACTION_RAW'),
    'sales_line.csv':         ('STG_SALES_STAGE',      'STG_SALES_LINE_RAW'),
    'payment.csv':            ('STG_PAYMENT_STAGE',    'STG_PAYMENT_RAW'),
    'return_transaction.csv': ('STG_RETURN_STAGE',     'STG_RETURN_RAW'),
    'inventory.csv':          ('STG_INVENTORY_STAGE',  'STG_INVENTORY_RAW'),
}

COPY_SQLS = {
    'STG_LOCATION_RAW': """
        COPY INTO STAGE_LAYER.STG_LOCATION_RAW
            (location_id,street_address,city,state,zip_code,country,region,created_at,updated_at,_stg_file_name)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,METADATA$FILENAME FROM @STAGE_LAYER.STG_LOCATION_STAGE)
        FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1 NULL_IF=('','NULL'))
        PURGE=FALSE ON_ERROR='CONTINUE'
    """,
    'STG_STORE_RAW': """
        COPY INTO STAGE_LAYER.STG_STORE_RAW
            (store_id,store_code,store_name,store_type,location_id,manager_name,phone_number,email,
             open_date,close_date,is_active,square_footage,created_at,updated_at,_stg_file_name)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,METADATA$FILENAME FROM @STAGE_LAYER.STG_STORE_STAGE)
        FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1 NULL_IF=('','NULL'))
        PURGE=FALSE ON_ERROR='CONTINUE'
    """,
    'STG_CUSTOMER_RAW': """
        COPY INTO STAGE_LAYER.STG_CUSTOMER_RAW
            (customer_id,customer_code,first_name,last_name,email,phone_number,date_of_birth,
             gender,loyalty_tier,loyalty_points,registration_date,location_id,is_active,created_at,updated_at,_stg_file_name)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,METADATA$FILENAME FROM @STAGE_LAYER.STG_CUSTOMER_STAGE)
        FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1 NULL_IF=('','NULL'))
        PURGE=FALSE ON_ERROR='CONTINUE'
    """,
    'STG_PRODUCT_CATEGORY_RAW': """
        COPY INTO STAGE_LAYER.STG_PRODUCT_CATEGORY_RAW
            (category_id,category_code,category_name,parent_category_id,description,is_active,created_at,_stg_file_name)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,METADATA$FILENAME FROM @STAGE_LAYER.STG_PRODUCT_STAGE)
        FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1 NULL_IF=('','NULL'))
        PURGE=FALSE ON_ERROR='CONTINUE'
    """,
    'STG_PRODUCT_RAW': """
        COPY INTO STAGE_LAYER.STG_PRODUCT_RAW
            (product_id,product_code,sku,product_name,category_id,supplier_id,unit_cost,unit_price,
             discount_pct,weight_kg,brand,size,color,is_perishable,is_active,launch_date,discontinue_date,
             created_at,updated_at,_stg_file_name)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,METADATA$FILENAME FROM @STAGE_LAYER.STG_PRODUCT_STAGE)
        FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1 NULL_IF=('','NULL'))
        PURGE=FALSE ON_ERROR='CONTINUE'
    """,
    'STG_SALES_TRANSACTION_RAW': """
        COPY INTO STAGE_LAYER.STG_SALES_TRANSACTION_RAW
            (transaction_id,transaction_code,transaction_date,store_id,customer_id,cashier_id,
             transaction_type,channel,subtotal_amount,discount_amount,tax_amount,total_amount,
             loyalty_points_earned,loyalty_points_redeemed,notes,created_at,_stg_file_name)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,METADATA$FILENAME FROM @STAGE_LAYER.STG_SALES_STAGE)
        FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1 NULL_IF=('','NULL'))
        PURGE=FALSE ON_ERROR='CONTINUE'
    """,
    'STG_SALES_LINE_RAW': """
        COPY INTO STAGE_LAYER.STG_SALES_LINE_RAW
            (line_id,transaction_id,line_number,product_id,quantity,unit_price,unit_cost,
             discount_pct,discount_amount,line_total_amount,line_cost_amount,tax_rate,tax_amount,created_at,_stg_file_name)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,METADATA$FILENAME FROM @STAGE_LAYER.STG_SALES_STAGE)
        FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1 NULL_IF=('','NULL'))
        PURGE=FALSE ON_ERROR='CONTINUE'
    """,
    'STG_PAYMENT_RAW': """
        COPY INTO STAGE_LAYER.STG_PAYMENT_RAW
            (payment_id,transaction_id,payment_method,payment_amount,payment_status,
             payment_reference,payment_date,card_last_four,created_at,_stg_file_name)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,METADATA$FILENAME FROM @STAGE_LAYER.STG_PAYMENT_STAGE)
        FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1 NULL_IF=('','NULL'))
        PURGE=FALSE ON_ERROR='CONTINUE'
    """,
    'STG_RETURN_RAW': """
        COPY INTO STAGE_LAYER.STG_RETURN_RAW
            (return_id,return_code,original_transaction_id,return_date,store_id,customer_id,
             return_reason,refund_method,refund_amount,is_restocked,created_at,_stg_file_name)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,METADATA$FILENAME FROM @STAGE_LAYER.STG_RETURN_STAGE)
        FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1 NULL_IF=('','NULL'))
        PURGE=FALSE ON_ERROR='CONTINUE'
    """,
    'STG_INVENTORY_RAW': """
        COPY INTO STAGE_LAYER.STG_INVENTORY_RAW
            (inventory_id,store_id,product_id,quantity_on_hand,quantity_reserved,quantity_available,
             reorder_point,reorder_quantity,last_restock_date,last_sold_date,snapshot_date,
             created_at,updated_at,_stg_file_name)
        FROM (SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,METADATA$FILENAME FROM @STAGE_LAYER.STG_INVENTORY_STAGE)
        FILE_FORMAT=(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1 NULL_IF=('','NULL'))
        PURGE=FALSE ON_ERROR='CONTINUE'
    """,
}

def get_connection():
    return snowflake.connector.connect(
        account   = os.getenv('SNOWFLAKE_ACCOUNT'),
        user      = os.getenv('SNOWFLAKE_USER'),
        password  = os.getenv('SNOWFLAKE_PASSWORD'),
        database  = os.getenv('SNOWFLAKE_DATABASE', 'RETAIL_DW'),
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'RETAIL_WH'),
        role      = os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
    )

def upload_and_load(csv_dir: str):
    conn = get_connection()
    cs   = conn.cursor()
    try:
        cs.execute('USE DATABASE RETAIL_DW')
        cs.execute('USE WAREHOUSE RETAIL_WH')

        csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))
        for csv_path in csv_files:
            fname = os.path.basename(csv_path)
            if fname not in STAGE_MAP:
                continue
            stage_name, table_name = STAGE_MAP[fname]
            print(f"\nUploading {fname} → @{stage_name}")
            cs.execute(f"PUT file://{csv_path} @STAGE_LAYER.{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
            print(f"  PUT complete. Running COPY INTO {table_name}...")
            cs.execute(COPY_SQLS[table_name])
            for row in cs.fetchall():
                print(f"    {row}")
    finally:
        cs.close()
        conn.close()

if __name__ == '__main__':
    csv_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'csv')
    upload_and_load(csv_dir)
