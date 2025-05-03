#!/usr/bin/env python3
"""
NYC Taxi Data Ingestion Script
This script downloads NYC Yellow Taxi trip data for 2024 in Parquet format
and loads it into a Snowflake database.
"""

import os
import sys
import logging
import tempfile
from datetime import datetime
import requests
import pyarrow.parquet as pq
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nyc_taxi_ingest.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("nyc_taxi_ingest")

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT = "DXGDUZO-BK44299"
SNOWFLAKE_USER = "DBT_EXECUTOR"
SNOWFLAKE_PASSWORD = "STyven@1996"  # Set this via environment variable or enter manually
SNOWFLAKE_ROLE = "DBT_EXECUTOR_ROLE"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "RAW_DB"
SNOWFLAKE_SCHEMA = "PUBLIC"

# Data source parameters
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
TABLE_NAME = "YELLOW_TAXI_TRIPS"


def get_snowflake_connection():
    """Establish connection to Snowflake"""
    try:
        conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        logger.info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


def create_table_if_not_exists(conn):
    """Drop the existing table to ensure a clean slate"""
    try:
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(f"""
        SHOW TABLES LIKE '{TABLE_NAME}' IN SCHEMA {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}
        """)
        
        if cursor.fetchone():
            logger.info(f"Table {TABLE_NAME} already exists, dropping it for clean import")
            cursor.execute(f"DROP TABLE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{TABLE_NAME}")
            logger.info(f"Table {TABLE_NAME} dropped successfully")
        else:
            logger.info(f"Table {TABLE_NAME} does not exist and will be created during data loading")
    
    except Exception as e:
        logger.error(f"Failed to check/drop table: {e}")
        raise
    finally:
        cursor.close()


def download_parquet_file(year, month):
    """Download a single parquet file for a specific month"""
    month_str = str(month).zfill(2)
    file_name = f"yellow_tripdata_{year}-{month_str}.parquet"
    url = f"{BASE_URL}{file_name}"
    
    logger.info(f"Downloading {url}")
    
    # Create a temporary file to store the download
    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            # Write the file to disk
            for chunk in response.iter_content(chunk_size=8192):
                temp_file.write(chunk)
                
            temp_file_path = temp_file.name
            logger.info(f"Downloaded {file_name} to {temp_file_path}")
            return temp_file_path
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {file_name}: {e}")
            os.unlink(temp_file.name)
            return None


def load_parquet_to_snowflake(conn, file_path, year, month):
    """Load a Parquet file into Snowflake"""
    try:
        month_str = str(month).zfill(2)
        logger.info(f"Loading data for {year}-{month_str}")
        
        # Read the Parquet file into a pandas DataFrame
        df = pq.read_table(file_path).to_pandas()
        
        # Convert timestamp columns to strings in ISO format for Snowflake compatibility
        # First, examine a sample value
        sample_val = None
        if 'tpep_pickup_datetime' in df.columns and len(df) > 0:
            sample_val = df['tpep_pickup_datetime'].iloc[0]
            logger.info(f"Sample timestamp value before conversion: {sample_val} (type: {type(sample_val)})")
        
        # Handle specific datetime columns as strings
        for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
            if col in df.columns:
                try:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        # Convert numeric timestamps to datetime then to string
                        if df[col].max() > 1e18:  # Likely nanoseconds
                            df[col] = pd.to_datetime(df[col], unit='ns').dt.strftime('%Y-%m-%d %H:%M:%S')
                        elif df[col].max() > 1e15:  # Likely microseconds
                            df[col] = pd.to_datetime(df[col], unit='us').dt.strftime('%Y-%m-%d %H:%M:%S')
                        elif df[col].max() > 1e12:  # Likely milliseconds
                            df[col] = pd.to_datetime(df[col], unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')
                        else:  # Likely seconds
                            df[col] = pd.to_datetime(df[col], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # If it's a string or already datetime, convert to formatted string
                        df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    logger.info(f"Converted {col} to string format: {df[col].iloc[0]} (type: {type(df[col].iloc[0])})")
                except Exception as e:
                    logger.warning(f"Could not convert column {col} to string datetime: {e}")
        
        # Print column types for debugging after conversion
        logger.info(f"DataFrame datatypes after datetime conversion:\n{df.dtypes}")
        
        # Convert all column names to uppercase to match Snowflake's case-insensitive behavior
        df.columns = [col.upper() for col in df.columns]
        
        # Add a month column to track the data source - convert to string date format
        data_month = pd.to_datetime(f"{year}-{month_str}-01")
        df['DATA_MONTH'] = data_month.strftime('%Y-%m-%d')
        
        # Let's create the table first with specific types
        if month == 1:  # Only for the first month
            try:
                create_sql = f"""
                CREATE OR REPLACE TABLE {TABLE_NAME} (
                    VENDORID INTEGER,
                    TPEP_PICKUP_DATETIME VARCHAR(50),
                    TPEP_DROPOFF_DATETIME VARCHAR(50),
                    PASSENGER_COUNT FLOAT,
                    TRIP_DISTANCE FLOAT,
                    RATECODEID FLOAT,
                    STORE_AND_FWD_FLAG VARCHAR,
                    PULOCATIONID INTEGER,
                    DOLOCATIONID INTEGER,
                    PAYMENT_TYPE INTEGER,
                    FARE_AMOUNT FLOAT,
                    EXTRA FLOAT,
                    MTA_TAX FLOAT,
                    TIP_AMOUNT FLOAT,
                    TOLLS_AMOUNT FLOAT,
                    IMPROVEMENT_SURCHARGE FLOAT,
                    TOTAL_AMOUNT FLOAT,
                    CONGESTION_SURCHARGE FLOAT,
                    AIRPORT_FEE FLOAT,
                    DATA_MONTH VARCHAR(10)
                )
                """
                cursor = conn.cursor()
                cursor.execute(create_sql)
                cursor.close()
                logger.info(f"Created table {TABLE_NAME} with specific column types")
            except Exception as e:
                logger.warning(f"Could not create table with specific types: {e}")
        
        # Write the DataFrame to Snowflake (removed invalid date_format parameter)
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=TABLE_NAME,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            auto_create_table=False,  # We'll create the table manually
            quote_identifiers=False,  # Don't quote column names
            chunk_size=10000          # Smaller chunks to avoid memory issues
        )
        
        logger.info(f"Loaded {nrows} rows for {year}-{month_str} in {nchunks} chunks")
        return nrows
    
    except Exception as e:
        logger.error(f"Failed to load data for {year}-{month_str}: {e}")
        return 0
    
    finally:
        # Clean up the temporary file
        if os.path.exists(file_path):
            os.unlink(file_path)
            logger.info(f"Deleted temporary file {file_path}")


def process_month(conn, year, month):
    """Process a single month: download and load to Snowflake"""
    file_path = download_parquet_file(year, month)
    if file_path:
        rows_loaded = load_parquet_to_snowflake(conn, file_path, year, month)
        return rows_loaded
    return 0


def main():
    """Main execution function"""
    # Check for password
    global SNOWFLAKE_PASSWORD
    SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD", SNOWFLAKE_PASSWORD)
    
    if not SNOWFLAKE_PASSWORD:
        SNOWFLAKE_PASSWORD = input("Enter your Snowflake password: ")
    
    try:
        # Connect to Snowflake
        conn = get_snowflake_connection()
        
        # Drop existing table for clean import
        create_table_if_not_exists(conn)
        
        # Process all months for 2024
        year = 2024
        # Current year has data only up to the current month
        current_month = datetime.now().month if year == datetime.now().year else 12
        
        # Start with sequential processing to debug any issues
        logger.info("Starting sequential processing of months")
        total_rows = 0
        
        # Process January first to create the table with correct schema
        month = 1
        logger.info(f"Processing first month {year}-{month:02d} to create table schema")
        rows_loaded = process_month(conn, year, month)
        total_rows += rows_loaded
        logger.info(f"Processed first month with {rows_loaded} rows")
        
        # Then process the rest in parallel
        if current_month > 1:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(process_month, conn, year, month) for month in range(2, current_month + 1)]
                
                for future in futures:
                    total_rows += future.result()
        
        logger.info(f"Completed ingestion of {total_rows} total rows for {year}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
    
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("Snowflake connection closed")


if __name__ == "__main__":
    main()