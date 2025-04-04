#!/usr/bin/env python
"""
Convenience script for running the Tradovate Trading System.
Compatible with Python 3.11+
"""
import os
import argparse
import subprocess
import sys
import logging
from pathlib import Path

# Assuming config.py is in the same directory or Python path
from config import load_config, get_config_value

# Basic logging setup for the runner script itself
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define the root directory of the project
PROJECT_ROOT = Path(__file__).parent.resolve()
DEFAULT_CONFIG_FILE = PROJECT_ROOT / 'config.ini'
MAIN_SCRIPT_PATH = PROJECT_ROOT / 'main.py'

def check_python_version():
    """Check if the Python version meets the minimum requirement."""
    if sys.version_info < (3, 11):
        logger.warning(
            f"TradeLocker system is recommended for Python 3.11+. "
            f"You are using {sys.version_info.major}.{sys.version_info.minor}."
        )
        proceed = input("Proceed anyway? (y/n): ").lower()
        if proceed != 'y':
            logger.info("Exiting due to Python version.")
            sys.exit(1)

def check_requirements(requirements_file: Path = PROJECT_ROOT / 'requirements.txt'):
    """Check if required packages listed in requirements.txt are installed."""
    if not requirements_file.exists():
        logger.warning(f"Requirements file not found at {requirements_file}. Skipping check.")
        return

    logger.info(f"Checking requirements from {requirements_file}...")
    try:
        # Use pip check to verify installed packages against requirements
        # This is more robust than importing each one individually
        subprocess.check_call([sys.executable, "-m", "pip", "check"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
        logger.info("All required packages seem to be installed correctly.")
    except FileNotFoundError:
         logger.error("Could not find 'pip'. Make sure pip is installed and in your PATH.")
         sys.exit(1)
    except subprocess.CalledProcessError:
        logger.warning("Some package dependencies might be missing or incompatible.")
        install = input("Attempt to install/update requirements from file? (y/n): ").lower()
        if install == 'y':
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", str(requirements_file)])
                logger.info("Requirements installation/update attempted successfully.")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to install requirements: {e}")
                logger.error("Please install the required packages manually and try again.")
                sys.exit(1)
            except FileNotFoundError:
                 logger.error("Could not find 'pip'. Make sure pip is installed and in your PATH.")
                 sys.exit(1)
        else:
            logger.warning("Proceeding without installing requirements. The system might fail.")

# Corrected function signature
def parse_arguments(config: dict) -> argparse.Namespace:
    """Parse command line arguments, using config for defaults."""
    parser = argparse.ArgumentParser(
        description='TradeLocker Trading System Runner (Tradovate Mode)',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # --- Get defaults from config ---
    # Use the get_config_value helper for safe access
    default_config_file = str(DEFAULT_CONFIG_FILE)
    default_ml_enabled = get_config_value(config, 'MACHINE_LEARNING.enabled', False)
    # Assuming Tradovate section will be added to config.ini
    default_tradovate_env = get_config_value(config, 'TRADOVATE.environment', 'demo')
    # Keep default instrument type for potential use in strategy loading
    default_instrument_type = get_config_value(config, 'TRADING.default_instrument_type', 'forex')


    # --- Define Arguments ---
    parser.add_argument(
        '--config',
        type=str,
        default=default_config_file,
        help='Path to the configuration file (config.ini)'
    )

    # Tradovate Options
    tradovate_group = parser.add_argument_group('Tradovate Options')
    tradovate_group.add_argument(
        '--tradovate-env',
        type=str,
        default=default_tradovate_env,
        choices=['demo', 'live'],
        help='Tradovate API environment to connect to.'
    )
    # Add arguments for credentials if desired, but reading from config is safer
    # tradovate_group.add_argument('--tradovate-user', type=str, help='Tradovate Username (override config)')
    # tradovate_group.add_argument('--tradovate-pass', type=str, help='Tradovate Password (override config)')
    # tradovate_group.add_argument('--tradovate-api-key', type=str, help='Tradovate API Key (override config)')
    # tradovate_group.add_argument('--tradovate-api-secret', type=str, help='Tradovate API Secret (override config)')
    # tradovate_group.add_argument('--tradovate-device-id', type=str, help='Tradovate Device ID (override config)')


    # Trading options
    trading_group = parser.add_argument_group('Trading Options')
    # Retain instrument type for potential default strategy behaviour
    trading_group.add_argument(
        '--instrument-type', '-i',
        type=str,
        default=default_instrument_type,
        choices=['futures', 'forex', 'commodities', 'indices', 'stocks', 'crypto'], # Adjust choices if needed
        help='Primary instrument type for default settings (e.g., loading strategies)'
    )
    trading_group.add_argument('--duration', type=int, help='Run duration in seconds (runs indefinitely if not set)')
    trading_group.add_argument('--enable-ml', action='store_true', default=default_ml_enabled, help='Enable machine learning features')
    trading_group.add_argument('--disable-ml', action='store_false', dest='enable_ml', help='Disable machine learning features') # Allows overriding default
    trading_group.add_argument('--debug', action='store_true', help='Enable debug logging level')
    # Consider if '--live' is still needed or if '--tradovate-env live' is sufficient.
    # Keeping it for now, main.py logic will determine precedence.
    trading_group.add_argument('--live', action='store_true', help='Use live trading mode (may interact with --tradovate-env)')

    # Instrument selection
    instruments_group = parser.add_argument_group('Instrument Selection')
    instruments_group.add_argument(
        '--instruments',
        type=str,
        nargs='+',
        help='Specific list of instruments to trade (overrides config for the run)'
    )

    return parser.parse_args()

def main():
    """Main execution function for the runner script."""
    check_python_version()
    check_requirements()

    # Load initial config to get defaults for arg parsing
    # Suppress logging during this initial load if config file might be missing
    initial_config = load_config(str(DEFAULT_CONFIG_FILE))

    # Parse arguments, using loaded config for defaults
    args = parse_arguments(initial_config)

    # Reload config using the potentially specified file path from args
    config = load_config(args.config)
    if not config:
        logger.error("Failed to load configuration. Exiting.")
        sys.exit(1)

    # --- Prepare command for main.py ---
    cmd = [sys.executable, str(MAIN_SCRIPT_PATH)]

    # Pass essential arguments
    cmd.extend(['--config', args.config]) # Pass the config file path
    cmd.extend(['--tradovate-env', args.tradovate_env])
    # Pass instrument type if still relevant for main.py logic
    cmd.extend(['--instrument-type', args.instrument_type])


    # Pass other options
    if args.duration:
        cmd.extend(['--duration', str(args.duration)])
    if args.enable_ml:
        cmd.append('--enable-ml') # Only add if true
    if args.debug:
        cmd.append('--debug')
    if args.live:
        cmd.append('--live')

    # Pass instruments if specified via command line
    if args.instruments:
        cmd.append('--instruments')
        cmd.extend(args.instruments)
    # Note: main.py will load instruments from the config file if not provided via CLI

    # --- Execute main.py ---
    logger.info(f"Executing command: {' '.join(cmd)}")
    try:
        # Use subprocess.run which waits for completion
        process = subprocess.run(cmd, check=False) # check=False allows us to see the return code
        logger.info(f"main.py finished with exit code: {process.returncode}")
    except FileNotFoundError:
         logger.error(f"Error: Could not find main script at '{MAIN_SCRIPT_PATH}' or python executable '{sys.executable}'")
         sys.exit(1)
    except Exception as e:
        logger.exception(f"An error occurred while running main.py: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()