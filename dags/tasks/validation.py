import os
import logging
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Fallback logger setup
try:
    from utils.logger import FlightLogger
    logger = FlightLogger.get_logger("flight_pipeline.validation")
except ImportError:
    logger = logging.getLogger("flight_pipeline.validation")
    logger.setLevel(logging.INFO)

DATA_PATH = '/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv'

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
