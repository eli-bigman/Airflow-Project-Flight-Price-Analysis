import logging
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Fallback logger setup
try:
    from utils.logger import FlightLogger
    logger = FlightLogger.get_logger("flight_pipeline.transformation")
except ImportError:
    logger = logging.getLogger("flight_pipeline.transformation")
    logger.setLevel(logging.INFO)

def _parse_stopovers(val):
    """Helper to parse stopover strings into integers."""
    s = str(val).lower().strip()
    if 'direct' in s or 'non-stop' in s:
        return 0
    if 'stop' in s:
        try:
            return int(''.join(filter(str.isdigit, s)))
        except ValueError:
            return 0
    return 0

def _load_dim_and_get_map(pg_engine, dim_df, table_name, unique_col, id_col, schema='analytics'):
    """Helper to load dimension tables and return a mapping dict."""
    try:
        existing = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", pg_engine)
    except Exception:
        existing = pd.DataFrame()
    
    if not existing.empty:
        new_records = dim_df[~dim_df[unique_col].isin(existing[unique_col])]
    else:
        new_records = dim_df
        
    if not new_records.empty:
        new_records.to_sql(table_name, pg_engine, schema=schema, if_exists='append', index=False)
        logger.info(f"Inserted {len(new_records)} new rows into {table_name}")
    
    final_dim = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", pg_engine)
    return dict(zip(final_dim[unique_col], final_dim[id_col]))

def transform_and_load_analytics(**kwargs):
    """
    Extracts from MySQL, transforms to Star Schema, and loads to Postgres.
    """
    # 1. Extract from MySQL
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    df_raw = mysql_hook.get_pandas_df("SELECT * FROM staging_flight_data.raw_flight_data")
    logger.info(f"Extracted {len(df_raw)} rows from MySQL.")
    
    # 2. Transform Step
    # A. String Standardization
    string_cols = ['airline', 'source_name', 'destination_name', 'aircraft_type', 'class', 'booking_source', 'seasonality']
    for col in string_cols:
        if col in df_raw.columns:
            df_raw[col] = df_raw[col].astype(str).str.strip().str.title()
            
    # B. Stopover Parsing
    df_raw['stopovers'] = df_raw['stopovers'].apply(_parse_stopovers)
    
    # C. Numeric Rounding & Validation
    numeric_cols = ['duration_hours', 'base_fare', 'tax_surcharge', 'total_fare']
    for col in numeric_cols:
        # Improved: Check if col exists to prevent KeyErrors
        if col in df_raw.columns:
            df_raw[col] = pd.to_numeric(df_raw[col], errors='coerce').fillna(0).round(2)
        else:
             logger.warning(f"Column {col} missing from source data. Filling with 0.")
             df_raw[col] = 0.0
        
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
    source_airports = df_raw[['source_code', 'source_name']].rename(columns={'source_code': 'airport_code', 'source_name': 'airport_name'})
    dest_airports = df_raw[['destination_code', 'destination_name']].rename(columns={'destination_code': 'airport_code', 'destination_name': 'airport_name'})
    dim_airports = pd.concat([source_airports, dest_airports]).drop_duplicates().reset_index(drop=True)
    
    # --- Dimension: Date ---
    df_raw['departure_dt'] = pd.to_datetime(df_raw['departure_datetime'], errors='coerce')
    dim_date = pd.DataFrame({'date_id': df_raw['departure_dt'].dt.date.unique()})
    dim_date['year'] = pd.to_datetime(dim_date['date_id']).dt.year
    dim_date['month'] = pd.to_datetime(dim_date['date_id']).dt.month
    dim_date['day'] = pd.to_datetime(dim_date['date_id']).dt.day
    dim_date['quarter'] = pd.to_datetime(dim_date['date_id']).dt.quarter
    dim_date['day_of_week'] = pd.to_datetime(dim_date['date_id']).dt.dayofweek
    dim_date['is_weekend'] = dim_date['day_of_week'].apply(lambda x: x >= 5)
    
    seasonality_map = df_raw.set_index(df_raw['departure_dt'].dt.date)['seasonality'].to_dict()
    dim_date['seasonality'] = dim_date['date_id'].map(seasonality_map).fillna('Normal')
    
    # 3. Load Dimensions to Postgres & Get IDs
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    pg_engine = pg_hook.get_sqlalchemy_engine()
    
    airline_map = _load_dim_and_get_map(pg_engine, dim_airlines, 'dim_airlines', 'airline_name', 'airline_id')
    airport_map = _load_dim_and_get_map(pg_engine, dim_airports, 'dim_airports', 'airport_code', 'airport_id')
    
    # Load Date Dim
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
    
    fact_columns = [
        'airline_id', 'source_airport_id', 'destination_airport_id', 'departure_date_id',
        'aircraft_type', 'class', 'stopovers', 'booking_source',
        'duration_hours', 'days_before_departure', 'base_fare', 'tax_surcharge', 'total_fare'
    ]
    
    df_fact = df_fact.dropna(subset=['airline_id', 'source_airport_id', 'destination_airport_id', 'departure_date_id'])
    
    logger.info("Loading facts...")
    df_fact[fact_columns].to_sql('fact_flights', pg_engine, schema='analytics', if_exists='append', index=False)
    logger.info("Fact table loaded successfully.")
