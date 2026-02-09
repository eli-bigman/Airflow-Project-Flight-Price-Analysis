import os
import shutil
import logging
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Fallback logger setup
try:
    from utils.logger import FlightLogger
    logger = FlightLogger.get_logger("flight_pipeline.ingestion")
except ImportError:
    logger = logging.getLogger("flight_pipeline.ingestion")
    logger.setLevel(logging.INFO)

INPUT_DIR = '/opt/airflow/data/input'
ARCHIVE_DIR = '/opt/airflow/data/archive'
ERROR_DIR = '/opt/airflow/data/error'


def extract_and_load_staging(**kwargs):
    """
    Scans data/input for CSVs, loads them to MySQL Staging, and archives them.
    No offset logic. No complex hashing. Just pure file processing.
    """
    logger.info("Starting Batch Ingestion (Process & Archive)...")
    
    # 1. Scan for files
    if not os.path.exists(INPUT_DIR):
        os.makedirs(INPUT_DIR, exist_ok=True)
        
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]
    
    if not files:
        logger.info(f"No CSV files found in {INPUT_DIR}. Skipping.")
        return

    # Connect to MySQL
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    engine = mysql_hook.get_sqlalchemy_engine()
    
    # Process each file
    for filename in files:
        file_path = os.path.join(INPUT_DIR, filename)
        logger.info(f"Processing file: {filename}")
        
        try:
            # 2. Read CSV (Load entire file)
            # Since files are "new batches", we load them fully.
            df = pd.read_csv(file_path)
            
            if df.empty:
                logger.warning(f"File {filename} is empty. Moving to archive without loading.")
            else:
                # 3. Rename columns to snake_case (Standardization)
                df.rename(columns={
                    'Airline': 'airline',
                    'Source': 'source_code',
                    'Source Name': 'source_name',
                    'Destination': 'destination_code',
                    'Destination Name': 'destination_name',
                    'Departure Date & Time': 'departure_datetime',
                    'Arrival Date & Time': 'arrival_datetime',
                    'Duration (hrs)': 'duration_hours',
                    'Stopovers': 'stopovers',
                    'Aircraft Type': 'aircraft_type',
                    'Class': 'class',
                    'Booking Source': 'booking_source',
                    'Base Fare (BDT)': 'base_fare',
                    'Tax & Surcharge (BDT)': 'tax_surcharge',
                    'Total Fare (BDT)': 'total_fare',
                    'Seasonality': 'seasonality',
                    'Days Before Departure': 'days_before_departure'
                }, inplace=True)
                
                # 4. Load to MySQL
                # 'append' mode because we are adding this batch to the historical table
                df.to_sql('raw_flight_data', con=engine, schema='staging_flight_data', if_exists='append', index=False)
                logger.info(f"Successfully loaded {len(df)} rows from {filename} to MySQL.")

            # 5. Archive the File
            if not os.path.exists(ARCHIVE_DIR):
                os.makedirs(ARCHIVE_DIR, exist_ok=True)
                
            shutil.move(file_path, os.path.join(ARCHIVE_DIR, filename))
            logger.info(f"Archived file to: {os.path.join(ARCHIVE_DIR, filename)}")
            
        except Exception as e:
            logger.error(f"Failed to process {filename}: {e}")
            if not os.path.exists(ERROR_DIR):
                os.makedirs(ERROR_DIR, exist_ok=True)
                
            shutil.move(file_path, os.path.join(ERROR_DIR, filename))
            logger.info(f"Error {filename} moved to: {os.path.join(ERROR_DIR, filename)}")

    logger.info("Batch Ingestion Complete.")
