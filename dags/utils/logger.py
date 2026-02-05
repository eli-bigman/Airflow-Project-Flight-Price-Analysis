import logging
import sys
import os

class FlightLogger:
    """
    Custom Logger for Flight Price Analysis Pipeline.
    Ensures consistent formatting and output handling (File + Console).
    """
    
    _initialized = False

    @staticmethod
    def get_logger(name: str):
        """
        Returns a configured logger instance.
        """
        logger = logging.getLogger(name)
        
        # Prevent adding handlers multiple times
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
        
        # 2. File Handler (Optional - ensures logs persist if needed outside Docker stdout)
        # Check if logs directory exists and is writable
        log_dir = os.environ.get('AIRFLOW_HOME', '/opt/airflow') + '/logs/custom_pipeline_logs'
        try:
            os.makedirs(log_dir, exist_ok=True)
            file_handler = logging.FileHandler(f"{log_dir}/pipeline.log")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            # Fallback if permission denied or path issue (common in Docker)
            print(f"Warning: Could not setup file logging: {e}")

        logger.propagate = False # Prevent double logging (Airflow root logger)
        return logger
