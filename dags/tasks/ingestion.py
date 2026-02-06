import os
import hashlib
import logging
import pandas as pd
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Fallback logger setup
try:
    from utils.logger import FlightLogger
    logger = FlightLogger.get_logger("flight_pipeline.ingestion")
except ImportError:
    logger = logging.getLogger("flight_pipeline.ingestion")
    logger.setLevel(logging.INFO)

DATA_PATH = '/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv'

def extract_and_load_staging(**kwargs):
    """
    Reads CSV in chunks and loads into MySQL Staging with Global Hash Deduplication & Incremental Loading.
    """
    logger.info("Starting extraction from CSV with Scalable Chunking & Incremental Loading...")
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"File not found: {DATA_PATH}")
    
    # 1. Get current file size (lines)
    with open(DATA_PATH, 'rb') as f:
        file_line_count = sum(1 for _ in f)
    
    # 2. Get stored offset
    offset = int(Variable.get("flight_csv_offset", default_var=0))
    logger.info(f"Current File Lines: {file_line_count}. Stored Offset: {offset}")
    
    # 3. Handle File Reset (New file is smaller than offset)
    if file_line_count < offset:
        logger.warning(f"File size ({file_line_count}) < Offset ({offset}). Detecting Reset/New File. Resetting offset to 0.")
        offset = 0
        
    # 4. Check if new data exists
    rows_to_process = file_line_count - 1 - offset # -1 for header
    if rows_to_process <= 0:
        logger.info("No new rows to process. Skipping extraction.")
        return

    # Connect to MySQL using Airflow Hook
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    engine = mysql_hook.get_sqlalchemy_engine()
    
    # Ensure hash table exists
    mysql_hook.run("CREATE TABLE IF NOT EXISTS processed_hashes (row_hash CHAR(32) PRIMARY KEY, load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    
    # Initialize State
    CHUNK_SIZE = 10000
    rows_processed_in_this_run = 0
    
    # Generator for chunks
    skip_range = range(1, offset + 1) if offset > 0 else None
    
    csv_reader = pd.read_csv(DATA_PATH, chunksize=CHUNK_SIZE, skiprows=skip_range)
    
    # Determine Write Mode
    write_mode = 'replace' if offset == 0 else 'append'

    for i, chunk in enumerate(csv_reader):
        chunk_start_len = len(chunk)
        rows_processed_in_this_run += chunk_start_len
        
        # 1. Calculate Hashes
        chunk['row_hash'] = chunk.apply(lambda x: hashlib.md5(str(tuple(x)).encode('utf-8')).hexdigest(), axis=1)
        
        # 2. Check against DB (Persistent Deduplication)
        chunk_hashes = tuple(chunk['row_hash'].tolist())
        if not chunk_hashes:
             continue

        # Safer: chunk formatted str for IN clause
        ids_str = "'" + "','".join(chunk_hashes) + "'"
        existing_df = mysql_hook.get_pandas_df(f"SELECT row_hash FROM processed_hashes WHERE row_hash IN ({ids_str})")
        existing_hashes = set(existing_df['row_hash'].tolist())
        
        # 3. Filter Duplicates
        is_new = ~chunk['row_hash'].isin(existing_hashes)
        clean_chunk = chunk[is_new].copy()
        
        chunk_dropped = chunk_start_len - len(clean_chunk)
        
        # 4. Prepare Hash DF for Insert
        if not clean_chunk.empty:
            new_hashes_df = clean_chunk[['row_hash']].copy()
            new_hashes_df['load_timestamp'] = pd.Timestamp.now()
            
            # Drop hash col from data DF before loading to raw_flight_data
            clean_chunk_data = clean_chunk.drop(columns=['row_hash'])
            
            # 5. Rename columns to snake_case
            clean_chunk_data.rename(columns={
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
            
            # 6. Load Data & Hashes to MySQL
            current_chunk_mode = write_mode if i == 0 else 'append'
            clean_chunk_data.to_sql('raw_flight_data', con=engine, schema='staging_flight_data', if_exists=current_chunk_mode, index=False)
            new_hashes_df.to_sql('processed_hashes', con=engine, schema='staging_flight_data', if_exists='append', index=False)
            
            logger.info(f"Chunk {i} Loaded: {len(clean_chunk)} rows. (Dropped {chunk_dropped} dupes). Mode: {current_chunk_mode}")
        else:
             logger.info(f"Chunk {i}: All {chunk_start_len} rows were already in DB. Skipping.")

    # Update Offset Variable
    new_offset = offset + rows_processed_in_this_run
    Variable.set("flight_csv_offset", new_offset)
    
    logger.info(f"Incremental Extraction Complete. Processed {rows_processed_in_this_run} new rows. New Offset: {new_offset}.")
