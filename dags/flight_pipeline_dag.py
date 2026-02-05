from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.sensors.sql import SqlSensor
from datetime import timedelta
import pandas as pd
import os
import hashlib
from sqlalchemy import create_engine
import logging

try:
    from utils.logger import FlightLogger
    logger = FlightLogger.get_logger("flight_pipeline")
except ImportError:
    # Fallback if utils not found (e.g. during simple testing)
    logger = logging.getLogger("flight_pipeline")
    logger.setLevel(logging.INFO)


# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'flight_price_pipeline',
    default_args=default_args,
    description='ETL pipeline for Flight Price Analysis',
    schedule_interval='@daily',
    catchup=False,
)

DATA_PATH = '/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv'

def extract_and_load_staging(**kwargs):
    """
    Reads CSV in chunks and loads into MySQL Staging with Global Hash Deduplication & Incremental Loading.
    """
    logger.info("Starting extraction from CSV with Scalable Chunking & Incremental Loading...")
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"File not found: {DATA_PATH}")
    
    # --- Incremental Logic ---
    # 1. Get current file size (lines) to detect resets or growth
    # Note: Counting lines in python can be slow for massive files, but for 57k it's instant.
    # For huge files, os.path.getsize (bytes) is better, but CSV parsing is line-based.
    # Let's use a robust line counter.
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
    
    # Ensure hash table exists (in case init script wasn't run)
    mysql_hook.run("CREATE TABLE IF NOT EXISTS processed_hashes (row_hash CHAR(32) PRIMARY KEY, load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    
    # Initialize State
    CHUNK_SIZE = 10000
    total_loaded = 0
    dropped_rows = 0
    
    # Generator for chunks
    # header=0 (keep header from first line), skiprows=range(1, offset+1) (skip processed rows)
    skip_range = range(1, offset + 1) if offset > 0 else None
    
    csv_reader = pd.read_csv(DATA_PATH, chunksize=CHUNK_SIZE, skiprows=skip_range)
    
    # Determine Write Mode
    # If offset=0, it's a full reload (or first load), so REPLACE.
    # If offset>0, we are appending new data, so APPEND.
    # Note: With persistent hashes, we can always use 'append' for data, 
    # but for safety/cleanup on full resets, 'replace' might be desired.
    # However, 'replace' on data table doesn't clear hash table. 
    # Decision: If offset=0, we should probably clear the staging table?
    # Yes, let's keep the logic consistent.
    write_mode = 'replace' if offset == 0 else 'append'
    
    rows_processed_in_this_run = 0

    for i, chunk in enumerate(csv_reader):
        chunk_start_len = len(chunk)
        rows_processed_in_this_run += chunk_start_len
        
        # 1. Calculate Hashes
        # Create a hash for each row (using all columns)
        chunk['row_hash'] = chunk.apply(lambda x: hashlib.md5(str(tuple(x)).encode('utf-8')).hexdigest(), axis=1)
        
        # 2. Check against DB (Persistent Deduplication)
        # Verify which hashes already exist
        chunk_hashes = tuple(chunk['row_hash'].tolist())
        if not chunk_hashes:
             continue

        # SQL IN clause needs formatted list
        format_strings = ','.join(['%s'] * len(chunk_hashes))
        placeholders = str(chunk_hashes)
        # Note: mysql_hook.get_pandas_df or get_records with proper parameter binding is safer
        # But constructing large IN clauses can be tricky.
        # Efficient approach: Load hashes to temp table? Or simple batch query.
        # Let's use simple batch query with parameter binding if possible, or straight execution if list is reasonable (10k is fine for IN)
        
        # Safer: chunk formatted str
        ids_str = "'" + "','".join(chunk_hashes) + "'"
        existing_df = mysql_hook.get_pandas_df(f"SELECT row_hash FROM processed_hashes WHERE row_hash IN ({ids_str})")
        existing_hashes = set(existing_df['row_hash'].tolist())
        
        # 3. Filter Duplicates
        is_new = ~chunk['row_hash'].isin(existing_hashes)
        clean_chunk = chunk[is_new].copy()
        
        # 4. Filter Stats
        chunk_dropped = chunk_start_len - len(clean_chunk)
        dropped_rows += chunk_dropped
        total_loaded += len(clean_chunk)
        
        # 5. Prepare Hash DF for Insert
        if not clean_chunk.empty:
            new_hashes_df = clean_chunk[['row_hash']].copy()
            new_hashes_df['load_timestamp'] = pd.Timestamp.now()
            
            # Drop hash col from data DF before loading to raw_flight_data
            clean_chunk_data = clean_chunk.drop(columns=['row_hash'])
            
            # 6. Rename columns
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
            
            # 7. Load Data to MySQL
            current_chunk_mode = write_mode if i == 0 else 'append'
            clean_chunk_data.to_sql('raw_flight_data', con=engine, schema='staging_flight_data', if_exists=current_chunk_mode, index=False)
            
            # 8. Load Hashes to MySQL
            # We use 'append' always for hashes
            new_hashes_df.to_sql('processed_hashes', con=engine, schema='staging_flight_data', if_exists='append', index=False)
            
            logger.info(f"Chunk {i} Loaded: {len(clean_chunk)} rows. (Dropped {chunk_dropped} dupes). Mode: {current_chunk_mode}")
        else:
             logger.info(f"Chunk {i}: All {chunk_start_len} rows were already in DB. Skipping.")

    # Update Offset Variable
    new_offset = offset + rows_processed_in_this_run
    Variable.set("flight_csv_offset", new_offset)
    
    logger.info(f"Incremental Extraction Complete. Processed {rows_processed_in_this_run} new rows. New Offset: {new_offset}.")

def transform_and_load_analytics(**kwargs):
    """
    Extracts from MySQL, transforms to Star Schema, and loads to Postgres.
    """
    # 1. Extract from MySQL
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    df_raw = mysql_hook.get_pandas_df("SELECT * FROM staging_flight_data.raw_flight_data")
    logger.info(f"Extracted {len(df_raw)} rows from MySQL.")
    
    # 2. Transform Step
    
    # --- Data Cleaning ---
    # A. String Standardization
    string_cols = ['airline', 'source_name', 'destination_name', 'aircraft_type', 'class', 'booking_source', 'seasonality']
    for col in string_cols:
        if col in df_raw.columns:
            df_raw[col] = df_raw[col].astype(str).str.strip().str.title()
            
    # B. Stopover Parsing ("Direct" -> 0, "1 Stop" -> 1)
    def parse_stopovers(val):
        s = str(val).lower().strip()
        if 'direct' in s or 'non-stop' in s:
            return 0
        if 'stop' in s:
            try:
                # Extract first number found
                return int(''.join(filter(str.isdigit, s)))
            except:
                return 0 # Fallback
        return 0
        
    df_raw['stopovers'] = df_raw['stopovers'].apply(parse_stopovers)
    
    # C. Numeric Rounding & Validation
    numeric_cols = ['duration_hours', 'base_fare', 'tax_surcharge', 'total_fare']
    for col in numeric_cols:
        df_raw[col] = pd.to_numeric(df_raw[col], errors='coerce').fillna(0).round(2)
        
    # Drop invalid negatives
    initial_len = len(df_raw)
    df_raw = df_raw[(df_raw['total_fare'] > 0) & (df_raw['duration_hours'] > 0)]
    dropped_invalid = initial_len - len(df_raw)
    if dropped_invalid > 0:
        logger.warning(f"Dropped {dropped_invalid} rows with invalid (<=0) Fare or Duration.")

    # --- Dimension: Airlines ---
    dim_airlines = df_raw[['airline']].drop_duplicates().reset_index(drop=True)
    dim_airlines.rename(columns={'airline': 'airline_name'}, inplace=True)
    
    # --- Dimension: Airports ---
    # Combine Source and Destination to get unique airports
    source_airports = df_raw[['source_code', 'source_name']].rename(columns={'source_code': 'airport_code', 'source_name': 'airport_name'})
    dest_airports = df_raw[['destination_code', 'destination_name']].rename(columns={'destination_code': 'airport_code', 'destination_name': 'airport_name'})
    dim_airports = pd.concat([source_airports, dest_airports]).drop_duplicates().reset_index(drop=True)
    
    # --- Dimension: Date ---
    # Parse dates
    df_raw['departure_dt'] = pd.to_datetime(df_raw['departure_datetime'], errors='coerce')
    dim_date = pd.DataFrame({'date_id': df_raw['departure_dt'].dt.date.unique()})
    dim_date['year'] = pd.to_datetime(dim_date['date_id']).dt.year
    dim_date['month'] = pd.to_datetime(dim_date['date_id']).dt.month
    dim_date['day'] = pd.to_datetime(dim_date['date_id']).dt.day
    dim_date['quarter'] = pd.to_datetime(dim_date['date_id']).dt.quarter
    dim_date['day_of_week'] = pd.to_datetime(dim_date['date_id']).dt.dayofweek
    dim_date['is_weekend'] = dim_date['day_of_week'].apply(lambda x: x >= 5)
    # Mapping seasonality (simplification: taking first occurrence if conflicting, or merge from raw)
    # Ideally date dimension is pre-populated. Here we derive from data.
    # Joining back with raw to get seasonality for that date might cause dupes if multiple seasons?
    # Let's assume seasonality is consistent per date or take the mode.
    # For now, we will leave seasonality NULL in dim_date derived solely from date, 
    # OR we can try to merge it. Simpler approach: Just load date parts.
    
    # 3. Load Dimensions to Postgres & Get IDs
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    pg_engine = pg_hook.get_sqlalchemy_engine()
    
    # Helper to load and fetch IDs
    def load_dim_and_get_map(dim_df, table_name, unique_col, id_col, schema='analytics'):
        # Check existing
        try:
            existing = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", pg_engine)
        except:
            existing = pd.DataFrame()
        
        if not existing.empty:
            # Filter out already existing
            new_records = dim_df[~dim_df[unique_col].isin(existing[unique_col])]
        else:
            new_records = dim_df
            
        if not new_records.empty:
            new_records.to_sql(table_name, pg_engine, schema=schema, if_exists='append', index=False)
            logger.info(f"Inserted {len(new_records)} new rows into {table_name}")
        
        # Reload to get all IDs
        final_dim = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", pg_engine)
        return dict(zip(final_dim[unique_col], final_dim[id_col]))

    # Load Dimensions
    airline_map = load_dim_and_get_map(dim_airlines, 'dim_airlines', 'airline_name', 'airline_id')
    airport_map = load_dim_and_get_map(dim_airports, 'dim_airports', 'airport_code', 'airport_id')
    
    # Load Date Dim (manual handling for date type)
    # Ensure date_id is unique
    dim_date.dropna(subset=['date_id'], inplace=True)
    try:
        existing_dates = pd.read_sql("SELECT date_id FROM analytics.dim_date", pg_engine)
        new_dates = dim_date[~dim_date['date_id'].isin(existing_dates['date_id'])]
        if not new_dates.empty:
            new_dates.to_sql('dim_date', pg_engine, schema='analytics', if_exists='append', index=False)
    except Exception as e:
        logger.warning(f"Date load issue (might be empty table): {e}")
        dim_date.to_sql('dim_date', pg_engine, schema='analytics', if_exists='append', index=False)

    # 4. Prepare Fact Table
    df_fact = df_raw.copy()
    df_fact['airline_id'] = df_fact['airline'].map(airline_map)
    df_fact['source_airport_id'] = df_fact['source_code'].map(airport_map)
    df_fact['destination_airport_id'] = df_fact['destination_code'].map(airport_map)
    df_fact['departure_date_id'] = df_fact['departure_dt'].dt.date
    
    # Select Fact Columns
    fact_columns = [
        'airline_id', 'source_airport_id', 'destination_airport_id', 'departure_date_id',
        'aircraft_type', 'class', 'stopovers', 'booking_source',
        'duration_hours', 'days_before_departure', 'base_fare', 'tax_surcharge', 'total_fare'
    ]
    
    # Handle missing FKs if any
    df_fact = df_fact.dropna(subset=['airline_id', 'source_airport_id', 'destination_airport_id', 'departure_date_id'])
    
    # Load Facts
    logger.info("Loading facts...")
    df_fact[fact_columns].to_sql('fact_flights', pg_engine, schema='analytics', if_exists='append', index=False)
    logger.info("Fact table loaded successfully.")

def validate_row_counts(**kwargs):
    """
    Validates data integrity by comparing row counts across Source, Staging, and Analytics.
    """
    logger.info("Starting Data Validation...")
    
    # 1. Source Count
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"File not found: {DATA_PATH}")
    source_count = len(pd.read_csv(DATA_PATH))
    logger.info(f"Source CSV Count: {source_count}")
    
    # 2. Staging Count
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    staging_count = mysql_hook.get_first("SELECT COUNT(*) FROM staging_flight_data.raw_flight_data")[0]
    logger.info(f"MySQL Staging Count: {staging_count}")
    
    # 3. Analytics Count
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    analytics_count = pg_hook.get_first("SELECT COUNT(*) FROM analytics.fact_flights")[0]
    logger.info(f"Postgres Analytics Count: {analytics_count}")
    
    # Validation Logic
    # Staging should match Source exactly
    if source_count != staging_count:
        raise ValueError(f"Data Loss detected! Source: {source_count}, Staging: {staging_count}")
        
    # Analytics might be slightly less due to drops (bad data), but let's warn if difference is > 1%
    diff = source_count - analytics_count
    if diff > (source_count * 0.01):
        raise ValueError(f"High Data Loss in Analytics! Source: {source_count}, Analytics: {analytics_count}, Dropped: {diff}")
        
    logger.info("Data Validation Passed successfully.")


# Define Tasks
# Sensors
wait_for_mysql = SqlSensor(
    task_id='wait_for_mysql',
    conn_id='mysql_default',
    sql="SELECT 1",
    poke_interval=10,
    timeout=600,
    dag=dag,
)

wait_for_postgres = SqlSensor(
    task_id='wait_for_postgres',
    conn_id='postgres_default',
    sql="SELECT 1",
    poke_interval=10,
    timeout=600,
    dag=dag,
)

t1 = PythonOperator(
    task_id='load_csv_to_mysql_staging',
    python_callable=extract_and_load_staging,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_and_load_star_schema',
    python_callable=transform_and_load_analytics,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_row_counts',
    python_callable=validate_row_counts,
    dag=dag,
)

# Dependencies
[wait_for_mysql, wait_for_postgres] >> t1 >> t2 >> validate_task
