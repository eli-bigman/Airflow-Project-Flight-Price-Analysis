from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import os
from sqlalchemy import create_engine
import logging

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
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
    Reads CSV and loads into MySQL Staging.
    """
    logging.info("Starting extraction from CSV...")
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"File not found: {DATA_PATH}")
    
    df = pd.read_csv(DATA_PATH)
    
    # Rename columns to match MySQL schema
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
    
    # Simple transformation: Clean logic if needed
    # For now, just load raw data
    
    logging.info(f"Loaded DataFrame with {len(df)} rows. Columns: {df.columns.tolist()}")

    # Connect to MySQL using Airflow Hook
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    engine = mysql_hook.get_sqlalchemy_engine()
    
    logging.info("Loading data into MySQL staging_flight_data.raw_flight_data...")
    # Use replace for idempotency on full reload scenarios, or append for incremental
    # Here we use replace to ensure freshness for this demo
    df.to_sql('raw_flight_data', con=engine, schema='staging_flight_data', if_exists='replace', index=False)
    logging.info("Data loaded successfully to MySQL.")

def transform_and_load_analytics(**kwargs):
    """
    Extracts from MySQL, transforms to Star Schema, and loads to Postgres.
    """
    # 1. Extract from MySQL
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    df_raw = mysql_hook.get_pandas_df("SELECT * FROM staging_flight_data.raw_flight_data")
    logging.info(f"Extracted {len(df_raw)} rows from MySQL.")
    
    # 2. Transform Step
    
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
            logging.info(f"Inserted {len(new_records)} new rows into {table_name}")
        
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
        logging.warning(f"Date load issue (might be empty table): {e}")
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
    logging.info("Loading facts...")
    df_fact[fact_columns].to_sql('fact_flights', pg_engine, schema='analytics', if_exists='append', index=False)
    logging.info("Fact table loaded successfully.")


# Define Tasks
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

t1 >> t2
