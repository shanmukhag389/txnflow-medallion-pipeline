import argparse
import os
import snowflake.connector

def run_snowflake_load(execution_date):
    # Fetch credentials directly from the secure environment layer
    conn = snowflake.connector.connect(
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        warehouse='TXNFLOW_WH',
        database='TXNFLOW',
        schema='GOLD'
    )
    
    cursor = conn.cursor()
    try:
        print(f"🚀 Ingesting gold data to Snowflake for partition date: {execution_date}")
        
        # Incremental idempotent load for Customer tables
        cursor.execute(f"""
            COPY INTO TXNFLOW.GOLD.CUSTOMER_DAILY
            FROM @TXNFLOW.GOLD.S3_GOLD_STAGE/customer_daily/dt={execution_date}/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """)
        
        # Incremental idempotent load for Merchant tables
        cursor.execute(f"""
            COPY INTO TXNFLOW.GOLD.MERCHANT_DAILY
            FROM @TXNFLOW.GOLD.S3_GOLD_STAGE/merchant_daily/dt={execution_date}/
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """)
        
        print("✅ Snowflake copy execution finished successfully.")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Partition Date (YYYY-MM-DD)")
    args = parser.parse_args()
    run_snowflake_load(args.date)

