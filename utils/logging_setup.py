import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from typing import Dict, List, Optional, Any, Tuple, Union
from pathlib import Path

# Use Path objects for directory handling
LOG_DIR_DEFAULT = Path('logs')

def setup_logging(level=logging.INFO, log_directory: Union[str, Path] = LOG_DIR_DEFAULT):
    """
    Set up logging configuration for the application.

    Configures a root logger and handlers for console output,
    a rotating main log file, a rotating error log file, and
    a time-rotating daily log file.

    Args:
        level: The logging level (e.g., logging.INFO, logging.DEBUG).
        log_directory: The directory where log files will be stored.
                       Defaults to 'logs' in the current working directory.
    """
    log_dir_path = Path(log_directory)

    try:
        # Create logs directory if it doesn't exist
        log_dir_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        # Use basic print since logging might not be configured yet if dir creation fails
        print(f"Error: Could not create log directory {log_dir_path}: {e}", file=sys.stderr)
        # Optionally raise the error or exit
        # raise e
        return # Continue without file logging if directory fails

    # Create timestamp for log file names (optional, TimedRotating handles dates)
    # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # --- Configure Root Logger ---
    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(level) # Set the minimum level for the root logger

    # --- Clear Existing Handlers (Important for reconfiguration) ---
    # Prevents adding duplicate handlers if setup_logging is called multiple times
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # --- Create Formatters ---
    # More detailed format for files
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    # Simpler format for console
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S' # Show only time on console for brevity
    )

    # --- Create Handlers ---

    # 1. Console Handler (StreamHandler)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level) # Respect the overall level for console
    console_handler.setFormatter(console_formatter)

    # 2. Main Rotating File Handler (RotatingFileHandler - by size)
    # Rotates when file reaches maxBytes, keeps backupCount files.
    main_log_file = log_dir_path / 'trading_system.log'
    file_handler = RotatingFileHandler(
        main_log_file,
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,             # Keep 5 backup files (e.g., .log.1, .log.2)
        encoding='utf-8'           # Specify encoding
    )
    file_handler.setLevel(level) # Log everything at the specified level or higher
    file_handler.setFormatter(file_formatter)

    # 3. Error File Handler (RotatingFileHandler - only errors)
    # Logs only messages with level ERROR or higher.
    error_log_file = log_dir_path / 'errors.log'
    error_handler = RotatingFileHandler(
        error_log_file,
        maxBytes=5 * 1024 * 1024,   # 5 MB for errors
        backupCount=3,              # Keep 3 error log backups
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR) # Only log ERROR and CRITICAL
    error_handler.setFormatter(file_formatter) # Use detailed format for errors

    # 4. Daily Rotating File Handler (TimedRotatingFileHandler - by time)
    # Rotates at midnight, keeps logs for a specified number of days.
    daily_log_file = log_dir_path / 'daily_activity.log'
    daily_handler = TimedRotatingFileHandler(
        daily_log_file,
        when='midnight',            # Rotate daily at midnight
        interval=1,                 # Check every 1 day
        backupCount=30,             # Keep logs for 30 days
        encoding='utf-8',
        utc=True                    # Use UTC for rotation time
    )
    daily_handler.setLevel(level) # Log everything at the specified level or higher
    daily_handler.setFormatter(file_formatter)

    # --- Add Handlers to the Root Logger ---
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    logger.addHandler(daily_handler)

    if level == logging.DEBUG:
        logging.getLogger('tradelocker.api').setLevel(logging.DEBUG)

    # --- Optional: Silence overly verbose libraries ---
    # logging.getLogger('websockets').setLevel(logging.WARNING)
    # logging.getLogger('aiohttp').setLevel(logging.WARNING)

    # Log setup completion using the newly configured logger
    logging.info(f"Logging setup complete. Level: {logging.getLevelName(level)}. Log directory: {log_dir_path}")