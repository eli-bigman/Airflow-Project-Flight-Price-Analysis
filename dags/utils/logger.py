import logging
import sys
import os

class FlightLogger:
    """
    Custom Logger for Flight Price Analysis Pipeline.
    Ensures consistent formatting and output handling (File + Console).
    """
    
    @staticmethod
    def get_logger(name: str):
        """
        Returns a configured logger instance.
        Logs to:
        1. Console (Standard Output) -> captured by Airflow UI
        2. File -> /opt/airflow/logs/flight_pipeline_debug.log (for quick local debugging)
        """
        logger = logging.getLogger(name)
        
        # Prevent adding handlers multiple times (Airflow quirk)
        if logger.hasHandlers():
            return logger
            
        logger.setLevel(logging.INFO)
        
        # Formatter: [TIMESTAMP] [LEVEL] [LOGGER_NAME] Message
        formatter = logging.Formatter(
            fmt='[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 1. Console Handler (Stream)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 2. File Handler (Simplified for Debugging)
        # We target the standard airflow logs folder so it persists (if mounted) or is easy to find
        log_dir = os.environ.get('AIRFLOW_HOME', '/opt/airflow') + '/logs'
        log_file = f"{log_dir}/flight_pipeline_debug.log"
        
        try:
            os.makedirs(log_dir, exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            # Fallback if permission denied (common in Docker if volume issues)
            print(f"Warning: Could not setup file logging at {log_file}: {e}")

        logger.propagate = False # Prevent double logging (Airflow root logger)
        return logger
