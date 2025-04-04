# config.py
"""
Configuration loader for the TradeLocker Trading System.
Loads settings primarily from config.ini, providing minimal code defaults.
Handles type conversions and structure mapping.
"""
import os
import logging
import configparser
from typing import Dict, Any, Optional, List, Union

# Use standard logger
logger = logging.getLogger(__name__)

# --- Helper Functions ---

def _parse_value(value: str) -> Union[str, bool, int, float, List[str]]:
    """Attempt to parse string value into a more specific type."""
    val_lower = value.lower()
    if val_lower in ['true', 'yes', 'on']:
        return True
    if val_lower in ['false', 'no', 'off']:
        return False
    if value.isdigit():
        return int(value)
    try:
        # Check if it's a float
        float_val = float(value)
        # Avoid converting integers to floats unnecessarily
        if '.' in value or 'e' in value.lower():
             return float_val
        else:
             # If it looks like an int but was parsed as float (e.g., "1.0"), return int
             if float_val == int(float_val):
                  return int(float_val)
             else:
                  return float_val # It's genuinely a float
    except ValueError:
        # Not a standard number, check for comma-separated list
        if ',' in value:
            return [item.strip() for item in value.split(',') if item.strip()]
        # Otherwise, return as string
        return value

def _parse_section(parser: configparser.ConfigParser, section: str) -> Dict[str, Any]:
    """Parse a specific section from ConfigParser into a dictionary."""
    data = {}
    if parser.has_section(section):
        for key, value in parser.items(section):
            data[key] = _parse_value(value)
    return data

def load_config(config_file: str = 'config.ini') -> Dict[str, Any]:
    """
    Loads configuration from the specified INI file.

    Args:
        config_file: Path to the configuration INI file.

    Returns:
        A dictionary containing the loaded configuration.
    """
    config = {}
    parser = configparser.ConfigParser(inline_comment_prefixes=('#', ';'))
    # Preserve key case (e.g., account_id vs account_ID)
    parser.optionxform = str

    if not os.path.exists(config_file):
        logger.error(f"Configuration file '{config_file}' not found. Cannot load settings.")
        # Return an empty config or raise an error, depending on desired behavior
        return config # Returning empty dict

    try:
        files_read = parser.read(config_file)
        if not files_read:
            logger.error(f"ConfigParser failed to read file (it might be empty or inaccessible): {config_file}")
            return config

        logger.info(f"Loading configuration from: {config_file}")

        # --- Load Core Sections ---
        config['API'] = _parse_section(parser, 'API')
        config['CHALLENGE'] = _parse_section(parser, 'CHALLENGE')
        config['INSTRUMENTS'] = _parse_section(parser, 'INSTRUMENTS')
        config['RISK_MANAGEMENT'] = _parse_section(parser, 'RISK_MANAGEMENT')
        config['STRATEGIES_ENABLED'] = _parse_section(parser, 'STRATEGIES') # Strategy toggles
        config['ADVANCED_FEATURES'] = _parse_section(parser, 'ADVANCED_FEATURES')
        config['PROFIT_TARGETS'] = _parse_section(parser, 'PROFIT_TARGETS')
        config['TRAILING_STOP'] = _parse_section(parser, 'TRAILING_STOP')
        config['TIME_FILTERS'] = _parse_section(parser, 'TIME_FILTERS')
        config['MACHINE_LEARNING'] = _parse_section(parser, 'MACHINE_LEARNING')
        config['REPORTING'] = _parse_section(parser, 'REPORTING')
        # --- Load Tradovate Settings ---
        config['TRADOVATE'] = _parse_section(parser, 'TRADOVATE')

        # --- Load Strategy-Specific Parameters ---
        config['STRATEGY_PARAMS'] = {}
        strategy_sections = [s for s in parser.sections() if s.endswith('_STRATEGY')]
        for section_name in strategy_sections:
            # Use a cleaner key name (remove _STRATEGY suffix)
            strategy_key = section_name.replace('_STRATEGY', '')
            config['STRATEGY_PARAMS'][strategy_key] = _parse_section(parser, section_name)
            logger.debug(f"Loaded parameters for strategy: {strategy_key}")

        # --- Add Environment Variables as Fallback/Override for API ---
        # Allow environment variables to override INI settings for sensitive data
        api_conf = config.get('API', {})
        api_conf['email'] = os.environ.get('TRADELOCKER_EMAIL', api_conf.get('email'))
        api_conf['password'] = os.environ.get('TRADELOCKER_PASSWORD', api_conf.get('password'))
        api_conf['server'] = os.environ.get('TRADELOCKER_SERVER', api_conf.get('server'))
        api_conf['account_id'] = os.environ.get('TRADELOCKER_ACCOUNT_ID', api_conf.get('account_id'))
        api_conf['environment'] = os.environ.get('TRADELOCKER_ENVIRONMENT', api_conf.get('environment', 'demo'))
        config['API'] = api_conf # Update the config dict

        # --- Add Environment Variables as Fallback/Override for Tradovate ---
        tv_conf = config.get('TRADOVATE', {})
        tv_conf['environment'] = os.environ.get('TRADOVATE_ENVIRONMENT', tv_conf.get('environment', 'demo'))
        tv_conf['username'] = os.environ.get('TRADOVATE_USERNAME', tv_conf.get('username'))
        tv_conf['password'] = os.environ.get('TRADOVATE_PASSWORD', tv_conf.get('password'))
        tv_conf['api_key'] = os.environ.get('TRADOVATE_API_KEY', tv_conf.get('api_key'))
        tv_conf['api_secret'] = os.environ.get('TRADOVATE_API_SECRET', tv_conf.get('api_secret'))
        tv_conf['device_id'] = os.environ.get('TRADOVATE_DEVICE_ID', tv_conf.get('device_id'))
        # Example: Override websocket URLs via env vars if needed
        # tv_conf['websocket_url_demo'] = os.environ.get('TRADOVATE_WS_DEMO', tv_conf.get('websocket_url_demo'))
        # tv_conf['websocket_url_live'] = os.environ.get('TRADOVATE_WS_LIVE', tv_conf.get('websocket_url_live'))
        config['TRADOVATE'] = tv_conf # Update the config dict

        # --- Basic Validation ---
        if not config.get('API', {}).get('email') or not config.get('API', {}).get('password'):
            logger.warning("API email or password not found in config file or environment variables. API connection will likely fail.")

        logger.info("Configuration loaded successfully.")

    except configparser.Error as e:
        logger.error(f"Error parsing configuration file '{config_file}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error loading configuration from {config_file}: {e}", exc_info=True)

    return config

# --- Convenience Accessor Functions ---

def get_config_value(config: Dict[str, Any], key_path: str, default: Any = None) -> Any:
    """
    Get a value from the loaded configuration using dot notation for nested access.

    Example: get_config_value(config, 'RISK_MANAGEMENT.max_risk_per_trade_percent', 1.0)
             get_config_value(config, 'STRATEGY_PARAMS.MACD_VOLUME.stop_loss_pips', 20)

    Args:
        config: The loaded configuration dictionary.
        key_path: The dot-separated path to the desired value.
        default: The default value to return if the key is not found.

    Returns:
        The configuration value or the default.
    """
    keys = key_path.split('.')
    value = config
    try:
        for key in keys:
            if isinstance(value, dict):
                value = value[key]
            else:
                # Tried to access a key on a non-dictionary item
                logger.debug(f"Config path '{key_path}' not found at key '{key}'. Returning default.")
                return default
        return value
    except KeyError:
        logger.debug(f"Config key '{key}' not found in path '{key_path}'. Returning default.")
        return default
    except Exception as e:
        logger.warning(f"Error accessing config path '{key_path}': {e}. Returning default.")
        return default

def is_strategy_enabled(config: Dict[str, Any], strategy_name: str) -> bool:
    """Check if a specific strategy is enabled in the configuration."""
    # Strategy names in config might be like 'momentum_enabled'
    config_key = f"{strategy_name.lower()}_enabled"
    return get_config_value(config, f'STRATEGIES_ENABLED.{config_key}', False)

def get_strategy_params(config: Dict[str, Any], strategy_name: str) -> Dict[str, Any]:
    """Get parameters for a specific strategy."""
    # Strategy param keys might be like 'MACD_VOLUME'
    config_key = strategy_name.upper()
    return get_config_value(config, f'STRATEGY_PARAMS.{config_key}', {})

# --- Challenge/Leverage Helpers (Moved from old config.py, adapted) ---

# Removed get_challenge_config and get_leverage functions as they are no longer relevant

# (Function definitions for get_challenge_config and get_leverage were previously here)