# utils/logger.py

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

def setup_logger(name, log_folder="logs", level=logging.INFO):
    """
    Set up a logger with both file and console handlers.
    
    Args:
        name (str): Logger name (usually __name__ or script name)
        log_folder (str): Directory to store log files
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    
    # Create logs directory
    log_path = Path(log_folder)
    log_path.mkdir(exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid duplicate handlers if logger already exists
    if logger.handlers:
        return logger
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    simple_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # File handler - detailed logs
    today = datetime.now().strftime('%Y%m%d')
    log_file = log_path / f"{name}_{today}.log"
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)  # Log everything to file
    file_handler.setFormatter(detailed_formatter)
    
    # Console handler - less verbose
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(simple_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def log_api_request(logger, method, url, params=None, headers=None, payload=None):
    """Log API request details"""
    logger.info(f"API Request: {method} {url}")
    if params:
        logger.debug(f"Parameters: {params}")
    if headers:
        # Don't log sensitive auth headers
        safe_headers = {k: v for k, v in headers.items() if 'authorization' not in k.lower()}
        logger.debug(f"Headers: {safe_headers}")
    if payload:
        logger.debug(f"Payload: {payload}")

def log_api_response(logger, response):
    """Log API response details"""
    logger.info(f"API Response: {response.status_code}")
    logger.debug(f"Response Headers: {dict(response.headers)}")
    
    if response.status_code >= 400:
        logger.error(f"Error Response: {response.text}")
    else:
        # Log first 500 chars of response for debugging
        content_preview = response.text[:500] + "..." if len(response.text) > 500 else response.text
        logger.debug(f"Response Content: {content_preview}")

def log_file_operation(logger, operation, file_path, size=None, records=None):
    """Log file operations"""
    abs_path = os.path.abspath(file_path)
    logger.info(f"File {operation}: {abs_path}")
    if size:
        logger.info(f"File size: {size:,} bytes")
    if records is not None:
        logger.info(f"Records: {records:,}")

class LoggerContext:
    """Context manager for logging function execution"""
    
    def __init__(self, logger, function_name, **kwargs):
        self.logger = logger
        self.function_name = function_name
        self.start_time = None
        self.kwargs = kwargs
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f"Starting {self.function_name}")
        if self.kwargs:
            self.logger.debug(f"Parameters: {self.kwargs}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.now() - self.start_time).total_seconds()
        
        if exc_type is None:
            self.logger.info(f"Completed {self.function_name} in {duration:.2f}s")
        else:
            self.logger.error(f"Failed {self.function_name} after {duration:.2f}s: {exc_val}")
            
        return False  # Don't suppress exceptions