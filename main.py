#!/usr/bin/env python
"""
Main entry point for the TradeLocker Futures Trading System.
Initializes and runs the core trading components.
"""
import argparse
import asyncio
import logging
import os
import signal  # For graceful shutdown
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any, Union

# --- Setup Root Logger Early ---
# Basic config initially, will be refined by setup_logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
# Get module-specific logger
logger = logging.getLogger(__name__)

# --- Project Imports ---
# Use absolute imports assuming standard project structure
try:
    from config import (
        load_config,
        get_config_value,
    )
    from core.trading_system import TradingSystem
    from ml_models.model_manager import ModelManager  # Keep import even if ML disabled
    from utils.logging_setup import setup_logging  # Use the dedicated setup function
except ImportError as e:
    logger.critical(f"Failed to import necessary modules: {e}. Check project structure and PYTHONPATH.")
    sys.exit(1)


# Define project root relative to this file
PROJECT_ROOT = Path(__file__).parent.resolve()


def create_directories(config: Dict[str, Any]):
    """Create necessary directories defined in config if they don't exist."""
    # Get directories from config, provide defaults
    # Using .get() on potentially missing keys for robustness
    log_dir = get_config_value(config, "REPORTING.log_directory", "logs")
    model_dir = get_config_value(config, "MACHINE_LEARNING.model_directory", "models")
    data_dir = get_config_value(config, "DATA.directory", "data")
    report_dir = get_config_value(config, "REPORTING.report_directory", "reports")
    chart_dir = get_config_value(config, "REPORTING.charts_directory", "charts")

    directories = [log_dir, model_dir, data_dir, report_dir, chart_dir]
    base_path = PROJECT_ROOT  # Assume directories are relative to project root

    for directory in directories:
        # Ensure directory is treated as a string path before creating Path object
        if not isinstance(directory, (str, Path)):
            logger.warning(f"Invalid directory path type in config: {directory}. Skipping.")
            continue
        dir_path = base_path / directory
        try:
            if not dir_path.exists():
                dir_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory: {dir_path}")
        except OSError as e:
            logger.error(f"Failed to create directory {dir_path}: {e}")
            # Decide if this is critical enough to exit
            # sys.exit(1)
        except Exception as e:
             logger.error(f"Unexpected error creating directory {dir_path}: {e}")


async def main_async(args: argparse.Namespace):
    """Asynchronous main function to run the trading system."""
    # Load configuration using the path from arguments
    config = load_config(args.config)
    if not config:
        # Use basic print as logging might not be fully set up if config fails
        print(f"CRITICAL: Failed to load configuration from '{args.config}'. Exiting.", file=sys.stderr)
        return  # Exit if config fails

    # Setup logging using the dedicated function and config/args
    log_level_str = "DEBUG" if args.debug else get_config_value(config, "LOGGING.level", "INFO")
    log_level = getattr(logging, log_level_str.upper(), logging.INFO)
    log_directory = get_config_value(config, "REPORTING.log_directory", "logs")
    # Ensure log_directory is a valid path before passing
    log_dir_path = str(PROJECT_ROOT / (log_directory if isinstance(log_directory, (str, Path)) else 'logs'))
    setup_logging(level=log_level, log_directory=log_dir_path)

    # Now that logging is fully configured, log the start
    logger.info("--- TradeLocker Trading System Starting ---")
    logger.info(f"Configuration loaded from: {args.config}")
    logger.debug(f"Runtime Arguments: {args}")

    # Create necessary directories based on config
    create_directories(config)

    # Determine trading mode
    # Use tradovate_env as the primary determinant for API connection
    # The old 'live' flag might be used internally by TradingSystem if needed, but env dictates connection
    trading_mode = args.tradovate_env # Should be 'demo' or 'live'
    logger.info(f"Tradovate Environment: {trading_mode.upper()}")
    # Determine if --live flag was passed, for potential internal logic (if needed)
    # is_live_trading = args.live
    # logger.info(f"Legacy --live flag set: {is_live_trading}")

    # --- Prepare System Configuration ---
    # system_config holds the loaded config, potentially modified by args
    system_config = config.copy()

    # --- Remove Challenge/Leverage Specific Logic ---
    # try:
    #     challenge_params = get_challenge_config(system_config)  # Uses defaults from config
    #     leverage = get_leverage(system_config)
    # except Exception as e:
    #     logger.error(f"Failed to calculate challenge/leverage parameters: {e}", exc_info=True)
    #     return # Cannot proceed without these

    # --- Remove CLI Overrides for Challenge Params ---
    # if args.profit_target_percent is not None:
    #     ...
    # if args.max_daily_loss_percent is not None:
    #     ...
    # if args.max_total_loss_percent is not None:
    #     ...

    # --- No longer merging challenge params or leverage ---
    # system_config.update(challenge_params)
    # system_config["leverage"] = leverage

    # Override instruments if provided via CLI
    instruments_key = args.instrument_type.lower()
    if args.instruments:
        system_config.setdefault("INSTRUMENTS", {})[instruments_key] = args.instruments
        logger.info(f"Overriding instruments for {args.instrument_type}: {args.instruments}")
    else:
        # Ensure the instruments for the selected type are loaded correctly from config
        loaded_instruments = get_config_value(config, f"INSTRUMENTS.{instruments_key}", [])
        if isinstance(loaded_instruments, str): # Handle comma-separated string
            loaded_instruments = [i.strip() for i in loaded_instruments.split(",") if i.strip()]
        system_config.setdefault("INSTRUMENTS", {})[instruments_key] = loaded_instruments
        logger.info(f"Using instruments for {args.instrument_type} from config: {loaded_instruments}")

    # Determine if ML is enabled based on args and config
    enable_ml = args.enable_ml  # Arg takes precedence
    system_config.setdefault("MACHINE_LEARNING", {})["enabled"] = enable_ml
    logger.info(f"Machine Learning Enabled: {enable_ml}")

    # --- Initialize Components ---
    model_manager = None
    if enable_ml:
        try:
            # Pass relevant ML config to ModelManager
            ml_config = system_config.get("MACHINE_LEARNING", {})
            model_save_path = ml_config.get("model_directory", "models/")
            model_features = ml_config.get("features", []) # Ensure features are defined in config if needed
            model_manager = ModelManager(
                model_save_path=str(PROJECT_ROOT / model_save_path),
                model_features=model_features,
                enable_machine_learning=True,
                config=system_config # Pass full config if needed by sub-components
            )
            logger.info("ModelManager instance created.")
        except Exception as e:
            logger.error(f"Failed to create ModelManager instance: {e}", exc_info=True)
            enable_ml = False # Disable ML if creation fails
            model_manager = None
            system_config["MACHINE_LEARNING"]["enabled"] = False # Update config state
            logger.warning("Proceeding with Machine Learning disabled.")

    # Initialize Trading System
    trading_system: Optional[TradingSystem] = None
    run_task: Optional[asyncio.Task] = None
    stop_task: Optional[asyncio.Task] = None
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        """Sets the stop event when a shutdown signal is received."""
        if not stop_event.is_set():
            logger.warning("Shutdown signal received!")
            stop_event.set()
        else:
             logger.warning("Multiple shutdown signals received, already stopping.")

    # Add signal handlers for SIGINT (Ctrl+C) and SIGTERM
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Fallback for Windows
            signal.signal(sig, lambda s, f: signal_handler())

    try:
        logger.info("Initializing TradingSystem...")
        trading_system = TradingSystem(
            config=system_config,  # Pass the fully prepared config
            # Pass Tradovate environment, remove old challenge args
            tradovate_env=args.tradovate_env,
            instrument_type=args.instrument_type,
            # trading_mode is now derived from tradovate_env, decide if TradingSystem needs both
            # trading_mode=trading_mode, # Or let TradingSystem use tradovate_env
            enable_ml=enable_ml,
            model_manager=model_manager,  # Pass the instance
        )
        logger.info("TradingSystem initialized.")

        # --- Run the System ---
        logger.info("Starting trading system run...")
        # Start the system's main execution logic
        run_task = asyncio.create_task(trading_system.run_async(duration=args.duration))
        # Create task for waiting on the stop event
        stop_task = asyncio.create_task(stop_event.wait())

        # Wait for either the run task or the stop task to complete
        done, pending = await asyncio.wait(
            {run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
        )

        # --- Handle Completion ---
        if stop_task in done:
            logger.info("Stop event was set. Cancelling main run task...")
            if not run_task.done():
                run_task.cancel()
        elif run_task in done:
            logger.info("Main run task completed.")
            # Cancel the stop_task as it's no longer needed
            if not stop_task.done():
                stop_task.cancel()

        # Wait for any pending cancellations to finish
        if pending:
            await asyncio.wait(pending, return_when=asyncio.ALL_COMPLETED)

        # Check for exceptions in the run_task
        if run_task: # Check if task was created
            if run_task.cancelled():
                logger.info("Main run task was cancelled successfully.")
            elif run_task.exception():
                logger.error("Trading system run task failed with an exception:", exc_info=run_task.exception())

        logger.info("Trading system run finished or was interrupted.")

    except Exception as e:
        logger.critical(f"Critical error during trading system setup or execution: {e}", exc_info=True)
        # Attempt to cancel tasks if they were started
        if run_task and not run_task.done(): run_task.cancel()
        if stop_task and not stop_task.done(): stop_task.cancel()
        # Wait briefly for cancellations
        await asyncio.sleep(0.1)

    finally:
        logger.info("Initiating final shutdown process...")
        # Remove signal handlers before shutdown to prevent interference
        for sig in (signal.SIGINT, signal.SIGTERM):
            try: loop.remove_signal_handler(sig)
            except (NotImplementedError, ValueError): pass # Ignore errors if handler wasn't added or already removed

        if trading_system:
            await trading_system.shutdown_async()  # Ensure async shutdown is called
        else:
             logger.warning("TradingSystem instance was not created, skipping shutdown call.")
        logger.info("--- TradeLocker Trading System Stopped ---")


def main():
    """Synchronous entry point: Parses arguments and runs the async main function."""
    parser = argparse.ArgumentParser(
        description="TradeLocker Trading System Runner (Tradovate Mode)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # Show defaults in help
    )

    # Define arguments (required arguments first)
    parser.add_argument('--config', type=str, required=True, help='Path to configuration file (config.ini)')
    # Tradovate specific required arg
    parser.add_argument('--tradovate-env', type=str, required=True, choices=['demo', 'live'], help='Tradovate API environment')

    # Optional arguments
    parser.add_argument('--instrument-type', type=str, default='futures', choices=['futures', 'forex', 'commodities', 'indices', 'stocks', 'crypto'], help='Primary instrument type for default settings')
    parser.add_argument('--duration', type=int, default=None, help='Run duration in seconds (runs indefinitely if not set)')
    parser.add_argument('--enable-ml', action='store_true', default=False, help='Enable machine learning features (overrides config)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging level')
    # Keep --live for now, logic in main_async or TradingSystem will decide precedence with --tradovate-env
    parser.add_argument('--live', action='store_true', help='Use live trading mode (may interact with --tradovate-env)')
    parser.add_argument('--instruments', type=str, nargs='+', help='Specific list of instruments to trade (overrides config for the run)')

    args = parser.parse_args()

    try:
        # Run the asynchronous main function
        asyncio.run(main_async(args))
        logger.info("main.py finished execution.")
        sys.exit(0) # Explicitly exit with success code
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected in main. Exiting.")
        sys.exit(130) # Standard exit code for Ctrl+C
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {e}", exc_info=True)
        sys.exit(1) # Exit with error code

if __name__ == "__main__":
    main()