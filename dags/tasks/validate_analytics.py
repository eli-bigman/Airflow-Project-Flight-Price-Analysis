import logging
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from decimal import Decimal

# Fallback logger setup
try:
    from utils.logger import FlightLogger
    logger = FlightLogger.get_logger("flight_pipeline.validate_analytics")
except ImportError:
    logger = logging.getLogger("flight_pipeline.validate_analytics")
    logger.setLevel(logging.INFO)

def validate_analytics_data(**kwargs):
    """
    Validates data integrity in Analytics layer after transformation.
    """
    logger.info("Starting Post-Transformation Validation...")
    
    # Hooks
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # 1. Row Counts
    staging_sql = "SELECT COUNT(*) FROM staging_flight_data.raw_flight_data"
    analytics_sql = "SELECT COUNT(*) FROM analytics.fact_flights"
    
    staging_count = mysql_hook.get_first(staging_sql)[0]
    analytics_count = pg_hook.get_first(analytics_sql)[0]
    
    logger.info(f"Row Counts -> Staging: {staging_count}, Analytics: {analytics_count}")
    
    
        
    # 2. Quality Checks (Negative Values)
    neg_check_sql = "SELECT total_fare, duration_hours FROM analytics.fact_flights WHERE total_fare < 0 OR duration_hours < 0 LIMIT 10"
    neg_rows = pg_hook.get_pandas_df(neg_check_sql)
    
    if not neg_rows.empty:
        msg = f"Data Quality Failure! Found {len(neg_rows)} rows with negative Fare or Duration in Analytics."
        logger.error(msg)
        raise ValueError(msg)

    #  3. SUM Check (Total Fare)
    staging_sum_sql = "SELECT SUM(total_fare) FROM staging_flight_data.raw_flight_data"
    analytics_sum_sql = "SELECT SUM(total_fare) FROM analytics.fact_flights"
    
    staging_sum = mysql_hook.get_first(staging_sum_sql)[0] or 0
    analytics_sum = pg_hook.get_first(analytics_sum_sql)[0] or 0
    
    logger.info(f"Total Fare Sum -> Staging: {staging_sum:,.2f}, Analytics: {analytics_sum:,.2f}")
    
    # Allow 5% deviation for dropped rows
    # Allow 5% deviation for dropped rows
    if abs(staging_sum - analytics_sum) > (staging_sum * Decimal('0.05')):
        logger.warning(f"Significant Fare Sum difference! Staging: {staging_sum}, Analytics: {analytics_sum}")

    # 4. Integrity Check (Orphaned Facts)
    # Check if any fact row has an invalid airline_id
    orphan_sql = """
        SELECT count(*) 
        FROM analytics.fact_flights f 
        LEFT JOIN analytics.dim_airlines d ON f.airline_id = d.airline_id 
        WHERE d.airline_id IS NULL
    """
    orphans = pg_hook.get_first(orphan_sql)[0]
    if orphans > 0:
        msg = f"Data Integrity Failure! Found {orphans} orphaned rows in Fact Table (Invalid Airline ID)."
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Post-Transformation Validation Passed.")
