import os
import shutil
import logging
import pandas as pd

# Fallback logger setup
try:
    from utils.logger import FlightLogger
    logger = FlightLogger.get_logger("flight_pipeline.validate_input")
except ImportError:
    logger = logging.getLogger("flight_pipeline.validate_input")
    logger.setLevel(logging.INFO)

INPUT_DIR = '/opt/airflow/data/input'
ERROR_DIR = '/opt/airflow/data/error'
MANDATORY_COLUMNS = ['Total Fare (BDT)', 'Departure Date & Time', 'Arrival Date & Time']

def validate_input_files(**kwargs):
    """
    Scans data/input for CSVs and validates their schema.
    If invalid, moves to error directory and raises exception.
    """
    logger.info("Starting Pre-Ingestion Validation...")
    
    if not os.path.exists(INPUT_DIR):
        logger.info(f"Input directory {INPUT_DIR} does not exist. creating it.")
        os.makedirs(INPUT_DIR, exist_ok=True)
        return # Nothing to validate yet

    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]
    
    if not files:
        logger.info(f"No CSV files found in {INPUT_DIR}. Skipping validation.")
        return

    for filename in files:
        file_path = os.path.join(INPUT_DIR, filename)
        logger.info(f"Validating file: {filename}")
        
        try:
            # Read CSV (Header only for efficiency?)
            # Reading full file to check for empty as well
            df = pd.read_csv(file_path)
            
            if df.empty:
                error_msg = f"File {filename} is empty."
                logger.error(error_msg)
                # Move to error
                _move_to_error(file_path, filename)
                raise ValueError(error_msg)
            
            # Check Mandatory Columns
            missing_cols = [col for col in MANDATORY_COLUMNS if col not in df.columns]
            if missing_cols:
                error_msg = f"Schema Validation Failed! Missing columns: {missing_cols}"
                logger.error(error_msg)
                _move_to_error(file_path, filename)
                raise ValueError(error_msg)
                
            logger.info(f"File {filename} passed validation.")
            
        except Exception as e:
            logger.error(f"Validation failed for {filename}: {e}")
            _move_to_error(file_path, filename)
            raise e

def _move_to_error(file_path, filename):
    if not os.path.exists(ERROR_DIR):
        os.makedirs(ERROR_DIR, exist_ok=True)
    shutil.move(file_path, os.path.join(ERROR_DIR, filename))
    logger.info(f"Moved invalid file to: {os.path.join(ERROR_DIR, filename)}")
