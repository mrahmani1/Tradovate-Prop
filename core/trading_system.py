# core/trading_system.py
"""
Core trading system that integrates all components. Handles asynchronous operations.
"""
import asyncio
import datetime
import logging
import time
import os
import signal
from typing import Dict, List, Optional, Any, Tuple

# --- Tradovate Import ---
# Replace TradeLocker specific imports
# from tradelocker.api import TradeLockerAPI, TradeLockerError, TradeLockerAuthError
try:
    # Assuming the library provides an async client
    # from tradovate import TradovateClient
    # from tradovate.errors import TradovateError, TradovateAuthError # Adapt based on actual library exceptions
    from .tradovate_adapter import TradovateAdapter # <-- ADD NEW IMPORT
    # Define generic error types or handle specific errors from the adapter if needed
    TradovateError = Exception # Placeholder
    TradovateAuthError = Exception # Placeholder
except ImportError:
    # logger.critical("Tradovate library not found. Please install it: pip install tradovate-api") # Original message
    logger.critical("TradovateAdapter not found. Ensure core/tradovate_adapter.py exists.")
    # TradovateClient = None # Placeholder to avoid further NameErrors # <-- REMOVE OLD
    TradovateAdapter = None # <-- ADD NEW PLACEHOLDER
    TradovateError = Exception # Placeholder
    TradovateAuthError = Exception # Placeholder

from core.rules_engine import RulesEngine
from core.position_manager import PositionManager
from core.strategy_engine import StrategyEngine
from ml_models.model_manager import ModelManager
from analytics.performance import PerformanceAnalytics
# --- Update Data Class Imports (Assume they might need changes later) ---
from utils.data_classes import Account, PriceBar, PriceSnapshot, Position, Trade, TradingSignal # TODO: Review/Update these data classes for Tradovate
from config import get_config_value # Use the helper

# Use a logger for this module
logger = logging.getLogger(__name__)

class TradingSystem:
    """Main trading system class, handling async operations."""

    def __init__(
        self,
        config: Dict[str, Any],
        # --- Updated Arguments ---
        tradovate_env: str, # Passed from main/run (e.g., 'demo', 'live')
        instrument_type: str, # Passed from main/run (e.g., 'futures', 'forex') - may influence strategy loading etc.
        # --- Removed Arguments ---
        # challenge_type: str,
        # account_size: str,
        # trading_mode: str = 'paper', # Replaced by tradovate_env
        enable_ml: bool = False, # Passed from main/run
        model_manager: Optional[ModelManager] = None # Passed from main
    ):
        """Initialize trading system."""
        self.config = config
        self.tradovate_env = tradovate_env
        # self.account_size_str = account_size # Removed
        self.instrument_type = instrument_type
        # self.trading_mode = trading_mode # Determined by tradovate_env
        self.enable_ml = enable_ml
        logger.info(f"Initializing TradingSystem: Env={tradovate_env}, ML={enable_ml}")
        logger.debug(f"Full config received: {config}") # Log full config only in debug

        # --- Initialize Core Components ---
        logger.info("Initializing API, RulesEngine, PositionManager, StrategyEngine...")

        # --- Tradovate API Client Initialization ---
        if TradovateAdapter is None:
            raise ImportError("TradovateAdapter is not available. Cannot initialize TradingSystem.")

        tv_config = self.config.get('TRADOVATE', {})
        api_key = tv_config.get('api_key')
        api_secret = tv_config.get('api_secret')
        username = tv_config.get('username')
        password = tv_config.get('password')
        device_id = tv_config.get('device_id') # Optional

        if not username or not password or not api_key or not api_secret:
             logger.critical("Tradovate credentials (username, password, api_key, api_secret) not found in config or environment variables!")
             raise ValueError("Missing Tradovate credentials")

        # TODO: Verify API call/field: TradovateClient __init__ parameters (environment, username, password, api_key, api_secret, device_id)
        self.api = TradovateAdapter(
            environment=self.tradovate_env,
            username=username,
            password=password,
            api_key=api_key,
            api_secret=api_secret,
            device_id=device_id # Pass device_id if available/needed
            # Add other relevant args based on the library's __init__
        )
        logger.info(f"TradovateAdapter instantiated for env: {self.tradovate_env}")

        # TODO: Review/Refactor these components for Tradovate API and data structures
        self.rules_engine = RulesEngine(config) # Rules engine uses the config - Needs review for Tradovate rules
        # TODO: Verify API calls/fields within PositionManager initialization and methods
        self.position_manager = PositionManager(self.api, self.rules_engine, config) # Needs heavy refactoring for Tradovate API
        self.model_manager = model_manager
        # TODO: Verify API calls/fields within StrategyEngine initialization and methods
        self.strategy_engine = StrategyEngine(self.api, self.position_manager, config, self.model_manager) # Needs heavy refactoring for Tradovate API

        # Initialize analytics
        logger.info("Initializing PerformanceAnalytics...")
        self.analytics = PerformanceAnalytics()

        # --- State Variables ---
        self.is_running = False
        self._stop_requested = asyncio.Event() # Internal signal for graceful stop
        self.account: Optional[Account] = None # Current account state
        self.initial_balance = 0.0 # Set during initialization
        self.start_of_day_balance = 0.0 # Balance at the start of the current trading day
        self.daily_pl = 0.0
        self.best_day_pl = 0.0 # Tracked for challenge rules
        self.current_day: Optional[datetime.date] = None

        # Instruments to trade (derived from config)
        self.instruments_to_trade: List[str] = self._get_instruments_from_config()
        self.instrument_data: Dict[str, Dict] = {} # Cache for bars, indicators, etc.

        # Task management
        self._tasks: Set[asyncio.Task] = set() # Use a set to avoid duplicates

        # Register trade completion callback
        self.position_manager.register_trade_callback(self._on_trade_completed)

        logger.info("TradingSystem __init__ complete.")

    def _get_instruments_from_config(self) -> List[str]:
        """Extracts the list of instruments for the current type from config."""
        instruments_config = self.config.get('INSTRUMENTS', {})
        key = self.instrument_type.lower()
        instruments = instruments_config.get(key, [])
        if isinstance(instruments, str): # Handle comma-separated string
            return [i.strip() for i in instruments.split(',') if i.strip()]
        elif isinstance(instruments, list):
            return instruments
        else:
            logger.warning(f"Instruments for type '{key}' not found or invalid format in config. Using empty list.")
            return []

    async def initialize(self) -> bool:
        """Initialize the trading system asynchronously."""
        logger.info("Initializing Trading System components...")
        try:
            # 1. Initialize API
            # logger.info("Initializing TradeLocker API connection...") # Remove old log
            # if not await self.api.initialize(): # Remove old initialize call if adapter doesn't need it
            #     logger.critical("Failed to initialize TradeLocker API. Cannot proceed.")
            #     return False
            # logger.info("TradeLocker API initialized.")

            # 1. Initialize Tradovate API Connection & Auth (via Adapter)
            logger.info("Initializing Tradovate connection via Adapter...")
            try:
                # Assuming async connect and authorize methods exist in the Adapter
                # TODO: Verify API call/field: self.api.connect()
                if not await self.api.connect(): # Use adapter's connect
                    logger.critical("Adapter failed to connect (likely token issue).")
                    return False
                # TODO: Verify API call/field: self.api.authorize()
                if not await self.api.authorize(): # Use adapter's authorize
                     logger.critical("Adapter failed to authorize (token issue).")
                     return False
                logger.info(f"Tradovate Adapter connected and authorized for env: {self.tradovate_env}")
            except TradovateAuthError as auth_err:
                 logger.critical(f"Tradovate authentication failed via Adapter: {auth_err}", exc_info=True)

            # 2. Load Account Information
            logger.info("Fetching initial account information...")
            # TODO: Verify API call/field: self.api.get_account() and returned fields (id, balance)
            self.account = await self.api.get_account()
            if not self.account:
                logger.critical("Failed to get account information. Cannot proceed.")
                # TODO: Verify API call/field: self.api.shutdown_async() or equivalent disconnect method
                await self.api.shutdown_async() # Clean up API session
                return False
            logger.info(f"Account info loaded: ID={self.account.id}, Balance={self.account.balance:.2f}")

            # Store initial state
            self.initial_balance = self.account.balance
            self.start_of_day_balance = self.account.balance # Initial value
            self.best_day_pl = 0.0
            self.daily_pl = 0.0
            self.current_day = datetime.datetime.now(datetime.timezone.utc).date()
            self.rules_engine.update_start_of_day_balance(self.start_of_day_balance) # Inform rules engine

            # 3. Initialize Instruments & Subscriptions
            logger.info(f"Initializing instruments: {self.instruments_to_trade}")
            if not self.instruments_to_trade:
                 logger.warning("No instruments specified to trade.")
                 # Decide if this is critical - maybe allow running without instruments?

            successful_subscriptions = 0
            # Ensure instrument cache in API is populated (Tradovate might require specific calls like `contract/list` or `product/list` first)
            # if not self.api.contracts_cache: # Example cache name
            #      logger.warning("Tradovate contracts cache is empty. Fetching...")
            #      # TODO: Verify API call/field: self.api.list_contracts() or equivalent to populate cache if needed
            #      await self.api.list_contracts() # Example call

            for symbol in self.instruments_to_trade:
                # TODO: Find Tradovate contract details (contractId, tickSize, etc.)
                # TODO: Verify API call/field: self.api.get_contract_details(symbol) or equivalent method
                # contract_info = self.api.get_contract_details(symbol) # Example method
                # if not contract_info:
                #     logger.error(f"Details for instrument '{symbol}' not found. Skipping subscription.")
                #     continue

                logger.info(f"Subscribing to market data for {symbol}...")
                try:
                    # Subscribe to quotes (bids/asks)
                    # TODO: Verify API call/field: self.api.subscribe_quote(symbol, callback) parameters and mechanism
                    await self.api.subscribe_quote(symbol, self._on_market_data_update)
                    # Optionally subscribe to DOM if needed by strategies
                    # TODO: Verify API call/field: self.api.subscribe_dom(symbol, callback) if used
                    # await self.api.subscribe_dom(symbol, self._on_dom_update)
                    # Optionally subscribe to charts if needed for bar generation
                    # TODO: Verify API call/field: self.api.subscribe_chart(symbol, timeframe, callback) if used
                    # timeframe = get_config_value(self.config, 'DATA.timeframe', '1m')
                    # await self.api.subscribe_chart(symbol, timeframe, self._on_chart_update)

                    successful_subscriptions += 1
                    # Initialize local data cache for this instrument (Adapt structure as needed)
                    self.instrument_data[symbol] = {
                        'bars': [], 'current_price': None, 'indicators': {}, 'dom': None, # Example fields
                        # 'contract_id': contract_info.get('contractId'), # Store relevant details
                        # 'tick_size': contract_info.get('tickSize')
                    }
                    logger.debug(f"Initialized local cache for: {symbol}")
                except TradovateError as sub_err:
                    logger.error(f"Error subscribing to market data for {symbol}: {sub_err}", exc_info=True)
                except Exception as e:
                    logger.error(f"Unexpected error subscribing for {symbol}: {e}", exc_info=True)


            if successful_subscriptions == 0 and self.instruments_to_trade:
                 logger.error("Failed to subscribe to market data for any specified instruments.")
                 # return False # Optional: Treat as critical failure

            # TODO: Subscribe to account, order, and position updates using user/syncRequest and WebSocket events
            # This is often handled automatically by Tradovate libraries after auth,
            # but might require specific event listeners.
            logger.info("Setting up listeners for account, order, and position updates...")
            try:
                # Example: Register listeners if the library uses an event emitter pattern
                # TODO: Verify API call/field: Event listener mechanism (e.g., add_event_listener) or callback registration
                # self.api.add_event_listener('account_update', self._on_account_update)
                # self.api.add_event_listener('order_update', self._on_order_update)
                # self.api.add_event_listener('position_update', self._on_position_update)
                # OR the library might push these directly to callbacks provided during init/connect.

                # Perform initial sync request to get current state
                # TODO: Verify API call/field: self.api.sync_request() or equivalent user sync mechanism
                await self.api.sync_request() # Example sync call
                logger.info("Initial user data sync requested.")

            except TradovateError as sync_err:
                 logger.error(f"Error setting up user data listeners or sync: {sync_err}", exc_info=True)
            except Exception as e:
                 logger.error(f"Unexpected error during user data setup: {e}", exc_info=True)

            # 4. Initialize Position Manager
            logger.info("Initializing Position Manager...")
            if not await self.position_manager.initialize():
                 logger.error("Failed to initialize Position Manager.")
                 # return False # Optional: Treat as critical

            # 5. Initialize Strategy Engine
            logger.info("Initializing Strategy Engine...")
            if not await self.strategy_engine.initialize(list(self.instrument_data.keys())):
                 logger.error("Failed to initialize Strategy Engine.")
                 # return False # Optional: Treat as critical

            # 6. Initialize ML Model Manager (if enabled)
            if self.enable_ml and self.model_manager:
                logger.info("Initializing Model Manager...")
                if not await self.model_manager.initialize():
                     logger.warning("Failed to initialize Model Manager. Continuing without ML.")
                     self.enable_ml = False # Disable ML if init fails
                     self.model_manager = None
                     self.strategy_engine.model_manager = None # Update strategy engine too
                else:
                     logger.info("Model Manager initialized.")

            logger.info(f"Trading system initialization complete.")
            logger.info(f"Challenge: {self.challenge_type}, Size: {self.account_size_str}, Type: {self.instrument_type}")
            # Log key risk parameters
            logger.info(f"Max Daily Loss: {self.rules_engine.challenge.max_daily_loss_percent}% (${self.rules_engine.challenge.max_daily_loss_abs:.2f})")
            logger.info(f"Max Total Loss: {self.rules_engine.challenge.max_total_loss_percent}% (${self.rules_engine.challenge.max_total_loss_abs:.2f})")
            logger.info(f"Profit Target: {self.rules_engine.challenge.profit_target_percent}% (${self.rules_engine.challenge.profit_target_abs:.2f})")

            return True

        except Exception as e:
            logger.critical(f"Critical error during trading system initialization: {e}", exc_info=True)
            # Ensure partial cleanup if init fails mid-way
            await self.shutdown_async()
            return False

    # --- WebSocket Callback Handlers ---
    # These are called SYNCHRONOUSLY by the API client's listener loop.
    # They should do minimal work and schedule async tasks for heavy processing.

    def _on_market_data_update(self, data: Dict[str, Any]):
        """Sync callback for market data. Parses and schedules async processing."""
        # TODO: Verify exact field names from the chosen tradovate-api library's WS feed.
        # This implementation assumes a common structure for quote updates.
        try:
            # Tradovate WS messages often come in frames; actual data might be nested.
            # Check if data is a list (heartbeat/event frame) or dict (actual data)
            # TODO: Verify API call/field: Structure of WebSocket quote messages (e.g., `d`, `e`, `d.symbol`, `d.bid`, `d.ask`, `d.lastTradePrice`, `d.volume`, `d.ts`)
            if not isinstance(data, dict) or 'd' not in data:
                # logger.debug(f"Skipping non-data WS message: {data}")
                return # Not a standard data payload we expect here

            quote_data = data.get('d')
            event_type = data.get('e') # 'quote', 'chart', etc.

            # Only process quote events in this callback
            if event_type != 'quote':
                 # logger.debug(f"Ignoring non-quote event type: {event_type}")
                 return

            symbol = quote_data.get('symbol')
            if not symbol or symbol not in self.instrument_data:
                logger.warning(f"Received quote for unknown/untracked symbol: {symbol}")
                return

            # Extract price information (adapt field names as needed)
            bid = quote_data.get('bid')
            ask = quote_data.get('ask')
            last = quote_data.get('lastTradePrice', quote_data.get('last')) # Prefer last trade price
            volume = quote_data.get('volume', quote_data.get('lastTradeVolume', 0.0)) # Volume associated with last trade or update
            # Timestamp - Tradovate often uses 'ts' or similar for a precise timestamp
            timestamp_ms = quote_data.get('ts', quote_data.get('timestamp', time.time() * 1000))

            # Basic validation
            if bid is None or ask is None or last is None:
                logger.debug(f"Incomplete quote data for {symbol}: {quote_data}")
                return

            # Convert to float, handle potential errors
            try:
                bid_f = float(bid)
                ask_f = float(ask)
                last_f = float(last)
                vol_f = float(volume)
                timestamp = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=datetime.timezone.utc)
            except (ValueError, TypeError) as conv_err:
                logger.error(f"Error converting quote data types for {symbol}: {conv_err} - Data: {quote_data}")
                return

            # Create the internal PriceSnapshot object
            snapshot = PriceSnapshot(
                instrument=symbol,
                timestamp=timestamp,
                bid=bid_f,
                ask=ask_f,
                last_price=last_f,
                volume=vol_f
            )

            # Schedule the async processing
            task = asyncio.create_task(self._process_market_data_async(symbol, snapshot))
            self._add_task(task) # Track the task

        except Exception as e:
            logger.exception(f"Error in sync _on_market_data_update: {e} - Data: {data}")

    def _on_account_update(self, data: Dict[str, Any]):
        """Sync callback for account updates. Schedules async processing."""
        # TODO: Verify exact field names and structure from Tradovate user sync/update events
        # Expected data often comes from the 'users' or 'accounts' part of a sync event.
        try:
            account_data = None
            # Check if it's part of a larger sync frame (common in Tradovate)
            # TODO: Verify API call/field: Structure of WebSocket account update messages (e.g., `d.accounts`, `entity`, `entityType`, account fields like `id`, `name`, `accountType`, `currency.name`, `balance`, `totalCashValue`, `realizedPL`, `unrealizedPL`, `totalInitialMargin`, `buyingPower`)
            if 'd' in data and isinstance(data['d'], dict):
                sync_data = data['d']
                # Look for account updates within the sync data
                # The exact structure depends heavily on the library/raw feed
                accounts_data = sync_data.get('accounts', []) # Assuming a list of accounts
                if not accounts_data and 'account' in sync_data: # Handle single account object
                    accounts_data = [sync_data['account']]

                if accounts_data:
                    # Assuming we only care about the primary account used by the system
                    # We might need to find the correct account by ID if multiple are present
                    # For now, assume the first one is relevant or the API client filters
                    account_data = accounts_data[0]
            elif 'entity' in data and data.get('entityType') == 'account': # Handle single entity update
                account_data = data['entity']

            if account_data:
                # --- Data Parsed - Schedule Async Processing ---
                logger.debug(f"Received account data for processing: {account_data.get('id')}")
                task = asyncio.create_task(self._process_account_update_async(account_data))
                self._add_task(task)
            # else: # Optional: Log if format not recognized
                # logger.debug(f"Unrecognized account update format: {data}")

        except Exception as e:
            logger.exception(f"Error in sync _on_account_update: {e} - Data: {data}")

    def _on_order_update(self, data: Dict[str, Any]):
        """Sync callback for order updates. Delegates to PositionManager async."""
        # TODO: Verify exact field names and structure for Tradovate order updates.
        # These might come within user sync frames or as individual events.
        try:
            order_data = None
            # TODO: Verify API call/field: Structure of WebSocket order update messages (e.g., `d.orders`, `entity`, `entityType`, order fields like `id`, `accountId`, `contractId`, `ordStatus`, `action`, `orderQty`, `cumQty`, `avgPx`, `timestamp`, `parentId`, `orderType`)
            if 'd' in data and isinstance(data['d'], dict):
                # Check within a sync frame
                orders_data = data['d'].get('orders', [])
                if orders_data: # Process all orders in the frame
                    logger.debug(f"Processing {len(orders_data)} order(s) from sync frame...")
                    for order in orders_data:
                        task = asyncio.create_task(self.position_manager.process_order_update(order))
                        self._add_task(task)
                    return # Handled all orders in this frame
            elif 'entity' in data and data.get('entityType') == 'order': # Handle single entity update
                order_data = data['entity']
                logger.debug(f"Processing single order entity update: {order_data.get('id')}")
                task = asyncio.create_task(self.position_manager.process_order_update(order_data))
                self._add_task(task)
                return # Handled single order update

            # If neither format matched or no order data found
            # logger.debug(f"No order data found or recognized format: {data}")

        except Exception as e:
            logger.exception(f"Error in sync _on_order_update: {e} - Data: {data}")


    def _on_position_update(self, data: Dict[str, Any]):
        """Sync callback for position updates. Delegates to PositionManager async."""
        # TODO: Verify exact field names and structure for Tradovate position updates.
        try:
            # TODO: Verify API call/field: Structure of WebSocket position update messages (e.g., `d.positions`, `entity`, `entityType`, position fields like `id`, `accountId`, `contractId`, `netPos`, `avgEntryPrice`, `unrealizedPL`, `realizedPL`)
            if 'd' in data and isinstance(data['d'], dict):
                # Check within a sync frame
                positions_data = data['d'].get('positions', [])
                if positions_data:
                    logger.debug(f"Processing {len(positions_data)} position(s) from sync frame...")
                    for position in positions_data:
                        task = asyncio.create_task(self.position_manager.process_position_update(position))
                        self._add_task(task)
                    return # Handled all positions in this frame
            elif 'entity' in data and data.get('entityType') == 'position':
                position_data = data['entity']
                logger.debug(f"Processing single position entity update: {position_data.get('id')}")
                task = asyncio.create_task(self.position_manager.process_position_update(position_data))
                self._add_task(task)
                return # Handled single position update

            # If neither format matched or no position data found
            # logger.debug(f"No position data found or recognized format: {data}")

        except Exception as e:
            logger.exception(f"Error in sync _on_position_update: {e} - Data: {data}")

    # --- Asynchronous Processing Methods ---

    async def _process_market_data_async(self, instrument: str, snapshot: PriceSnapshot):
         """Async processing of a market data snapshot."""
         # TODO: Review downstream methods called from here for Tradovate compatibility.
         # Methods like _update_bars_if_needed, _update_position_pl need complete rewrites.
         # Method _update_indicators needs review.
         # Method strategy_engine.process_price_update needs review.
         try:
            if instrument not in self.instrument_data:
                 logger.warning(f"Attempted to process market data for untracked instrument: {instrument}")
                 return

            inst_data = self.instrument_data[instrument]
            inst_data['current_price'] = snapshot

            # Update bars, indicators, position P/L
            await self._update_bars_if_needed(instrument, snapshot)
            self._update_indicators(instrument) # Keep sync if fast
            self._update_position_pl(instrument, snapshot.last_price) # Keep sync if fast

            # Process through strategy engine
            await self.strategy_engine.process_price_update(instrument, snapshot)

            # Process through ML if enabled
            if self.enable_ml and self.model_manager:
                await self._process_ml_prediction(instrument)

         except Exception as e:
            logger.exception(f"Error processing market data async for {instrument}: {e}")

    async def _process_account_update_async(self, account_data: Dict[str, Any]):
        """Async processing of an account update message."""
        # TODO: Verify exact field names from Tradovate Account object structure.
        try:
            # Check for critical ID field
            # TODO: Verify API call/field: Account fields from WS update: `id`, `name`, `accountType`, `currency.name`, `balance`, `totalCashValue`, `realizedPL`, `unrealizedPL`, `totalInitialMargin`, `buyingPower`
            acc_id = account_data.get('id')
            if acc_id is None:
                logger.error(f"Received account data without ID: {account_data}")
                return
            acc_id = int(acc_id) # Ensure integer ID

            if not self.account:
                 # If account object doesn't exist yet, create it from the first update
                 logger.info("Initializing Account object from first update.")
                 # Extract necessary fields to create the initial Account object
                 acc_name = account_data.get('name')
                 acc_type = account_data.get('accountType') # e.g., 'Demo'
                 currency = account_data.get('currency', {}).get('name', 'USD') # Currency might be nested
                 balance = account_data.get('balance')
                 equity = account_data.get('totalCashValue') # Example field for equity

                 if balance is None or equity is None:
                     logger.error(f"Cannot initialize account ID {acc_id} - missing balance or equity fields in: {account_data}")
                     return

                 self.account = Account(
                     id=acc_id,
                     name=acc_name,
                     account_type=acc_type,
                     base_currency=currency,
                     balance=float(balance),
                     equity=float(equity),
                     initial_balance=float(balance), # Set initial balance on first creation
                     # Initialize other fields from the update if available
                     realized_pl = float(account_data.get('realizedPL', 0.0)),
                     unrealized_pl = float(account_data.get('unrealizedPL', 0.0)),
                     margin_used = float(account_data.get('totalInitialMargin', 0.0)),
                     margin_available = float(account_data.get('buyingPower', 0.0))
                 )
                 self.start_of_day_balance = self.account.balance # Initialize start of day balance
                 # Inform rules engine of the new initial balance (if rules engine is adapted)
                 # TODO: Review RulesEngine adaptation
                 # self.rules_engine.update_start_of_day_balance(self.start_of_day_balance)
                 logger.info(f"Account object created: ID={self.account.id}, Name='{self.account.name}', Bal={self.account.balance:.2f}")
            else:
                 # Account object exists, check if this update is for the correct account
                 if self.account.id != acc_id:
                     logger.warning(f"Received update for different account ID ({acc_id}) than expected ({self.account.id}). Ignoring.")
                     return

                 # Update existing account fields
                 updated = False
                 # --- Map Tradovate fields to Account dataclass --- Example mapping:
                 new_balance = account_data.get('balance')
                 new_equity = account_data.get('totalCashValue') # Example equity field
                 new_realized_pl = account_data.get('realizedPL')
                 new_unrealized_pl = account_data.get('unrealizedPL')
                 new_margin_used = account_data.get('totalInitialMargin') # Example margin field
                 new_margin_avail = account_data.get('buyingPower') # Example margin field

                 # Check and update, converting to float
                 if new_balance is not None and self.account.balance != float(new_balance): self.account.balance = float(new_balance); updated = True
                 if new_equity is not None and self.account.equity != float(new_equity): self.account.equity = float(new_equity); updated = True
                 if new_realized_pl is not None and self.account.realized_pl != float(new_realized_pl): self.account.realized_pl = float(new_realized_pl); updated = True
                 if new_unrealized_pl is not None and self.account.unrealized_pl != float(new_unrealized_pl): self.account.unrealized_pl = float(new_unrealized_pl); updated = True
                 if new_margin_used is not None and self.account.margin_used != float(new_margin_used): self.account.margin_used = float(new_margin_used); updated = True
                 if new_margin_avail is not None and self.account.margin_available != float(new_margin_avail): self.account.margin_available = float(new_margin_avail); updated = True
                 # --- End Mapping ---

                 if updated:
                      logger.info(f"Account {self.account.id} updated: Bal={self.account.balance:.2f}, Eq={self.account.equity:.2f}, RlzPL={self.account.realized_pl:.2f}, UnrlzPL={self.account.unrealized_pl:.2f}, MarginAvail={self.account.margin_available:.2f}")
                      self.analytics.add_account_snapshot(self.account)
                      # Re-check rules immediately after significant account update
                      # TODO: Review RulesEngine adaptation
                      # self._check_rules_and_act() # Run synchronous check
                 # else: # Optional: Log if no changes detected
                     # logger.debug(f"Account {self.account.id} update received, but no fields changed.")

        except ValueError as verr:
            logger.error(f"Error converting account data types: {verr} - Data: {account_data}")
        except TypeError as terr:
             logger.error(f"Type error processing account data: {terr} - Data: {account_data}")
        except Exception as e:
            logger.exception(f"Error processing account update async: {e} - Data: {account_data}")

    # --- Bar, Indicator, P/L Updates ---

    async def _update_bars_if_needed(self, instrument: str, snapshot: PriceSnapshot):
        """Update price bars (async if history fetch needed)."""
        # TODO: REWRITE FOR TRADOVATE
        # - Needs Tradovate historical data fetching (e.g., chart subscription or REST call)
        # TODO: Verify API call/field: Historical data fetching method (e.g., self.api.get_historical_data or similar REST endpoint /chart/get) and its parameters/response structure
        # - Needs Tradovate timeframe handling
        # - PriceBar data class might need changes
        raise NotImplementedError("_update_bars_if_needed needs rewrite for Tradovate")
        inst_data = self.instrument_data.get(instrument)
        if not inst_data: return

        bars = inst_data.get('bars', [])
        instrument_id = inst_data.get('instrument_id')
        route_id = inst_data.get('route_id')
        timeframe_str = get_config_value(self.config, 'DATA.timeframe', '1m') # Example config path
        min_bars = get_config_value(self.config, 'DATA.min_bars_for_indicators', 50)
        max_bars = get_config_value(self.config, 'DATA.max_bars_to_keep', 500)

        # Fetch history if needed
        if (not bars or len(bars) < min_bars) and instrument_id and route_id:
            logger.info(f"Fetching historical {timeframe_str} bars for {instrument}...")
            try:
                hist_bars = await self.api.get_historical_data(
                    instrument_id, route_id, timeframe=timeframe_str, count=max_bars
                )
                if hist_bars:
                    inst_data['bars'] = hist_bars
                    bars = hist_bars
                    logger.info(f"Fetched {len(bars)} bars for {instrument}.")
                else: logger.warning(f"Failed to fetch historical bars for {instrument}.")
            except Exception as hist_err: logger.exception(f"Error fetching history for {instrument}: {hist_err}"); return

        # Update/add bars based on snapshot time
        if not bars: return # Still no bars after fetch attempt
        last_bar = bars[-1]
        current_time = snapshot.timestamp.replace(tzinfo=datetime.timezone.utc)
        last_bar_time = last_bar.timestamp.replace(tzinfo=datetime.timezone.utc)
        timeframe_seconds = self.api._timeframe_to_seconds(timeframe_str) or 60

        if (current_time - last_bar_time).total_seconds() >= timeframe_seconds:
            interval_start_ts = int(current_time.timestamp() // timeframe_seconds) * timeframe_seconds
            bar_start_time = datetime.datetime.fromtimestamp(interval_start_ts, tz=datetime.timezone.utc)
            if bar_start_time > last_bar_time: # Avoid duplicates
                new_bar = PriceBar(instrument, bar_start_time, snapshot.last_price, snapshot.last_price, snapshot.last_price, snapshot.last_price, snapshot.volume)
                bars.append(new_bar)
                if len(bars) > max_bars: inst_data['bars'] = bars[-max_bars:]
                # logger.debug(f"Added new {timeframe_str} bar for {instrument} at {bar_start_time}")
        else: # Update last bar
            last_bar.high = max(last_bar.high, snapshot.last_price)
            last_bar.low = min(last_bar.low, snapshot.last_price)
            last_bar.close = snapshot.last_price
            last_bar.volume += snapshot.volume

    def _update_indicators(self, instrument: str):
        """Update technical indicators (sync)."""
        # TODO: REVIEW INDICATOR LOGIC - Ensure it works with potentially adapted PriceBar structure
        # This might be okay if PriceBar structure doesn't change drastically, but review needed.
        # raise NotImplementedError("_update_indicators needs review for Tradovate PriceBar structure") # Optional: Force review
        inst_data = self.instrument_data.get(instrument)
        if not inst_data: return
        bars = inst_data.get('bars', [])
        min_bars = get_config_value(self.config, 'DATA.min_bars_for_indicators', 50)
        if len(bars) < min_bars: return

        # TODO: Replace with calls to a dedicated indicator calculation module/library
        # Example using helpers (assuming they exist and are robust)
        # from utils.helpers import calculate_sma, calculate_ema, calculate_rsi, ...
        indicators = {}
        try:
             closes = [b.close for b in bars]
             # highs = [b.high for b in bars]
             # lows = [b.low for b in bars]
             # volumes = [b.volume for b in bars]
             # indicators['sma_20'] = calculate_sma(closes, 20)
             # indicators['ema_10'] = calculate_ema(closes, 10)
             # indicators['rsi_14'] = calculate_rsi(closes, 14)
             # ... calculate other needed indicators ...
             inst_data['indicators'] = indicators # Store calculated indicators
             # logger.debug(f"Updated indicators for {instrument}: {list(indicators.keys())}")
        except Exception as e:
             logger.error(f"Error calculating indicators for {instrument}: {e}", exc_info=True)
             inst_data['indicators'] = {} # Clear on error

    def _update_position_pl(self, instrument: str, current_price: float):
        """Update current_price attribute on the Position object (sync)."""
        # P/L calculation is now handled primarily by process_position_update from WS feed.
        # This method ensures the Position object has the latest price for TP/Trailing checks.
        position = self.position_manager.get_position(instrument) # Get from PositionManager
        if position:
            # Directly update the current price on the Position object
            position.current_price = current_price
            logger.debug(f"Updated current_price for {instrument} position to: {current_price}")
        # Removed old P/L calculation logic based on pips

    # --- ML Processing ---
    async def _process_ml_prediction(self, instrument: str):
        """Prepare data and get ML prediction (async)."""
        if not self.enable_ml or not self.model_manager: return
        try:
            inst_data = self.instrument_data.get(instrument, {})
            current_price = inst_data.get('current_price')
            indicators = inst_data.get('indicators', {})
            if not current_price or not indicators: return

            # Prepare input data matching model features
            input_data = {'instrument': instrument, 'timestamp': current_price.timestamp, **vars(current_price), **indicators}
            # Add more features as needed (e.g., spread, time features)
            input_data['spread'] = current_price.ask - current_price.bid
            # ...

            signal: Optional[TradingSignal] = await self.model_manager.predict(input_data)
            if signal:
                logger.info(f"ML Signal for {instrument}: {signal.signal_type} ({signal.confidence:.2f})")
                await self.strategy_engine.process_signal(instrument, signal) # Let strategy engine handle execution
        except Exception as e:
            logger.exception(f"Error processing ML prediction for {instrument}: {e}")

    # --- Trade Completion Callback ---
    def _on_trade_completed(self, trade: Trade):
        """Sync callback for completed trades (called by PositionManager)."""
        if not isinstance(trade, Trade): logger.error(f"Invalid type in _on_trade_completed: {type(trade)}"); return
        try:
            logger.info(f"Trade Completed: {trade.instrument} {trade.direction} Qty:{trade.quantity} P/L:${trade.profit_loss:.2f} Pips:{trade.profit_loss_pips:.1f} Reason:'{trade.exit_reason}'")
            self.analytics.add_trade(trade)

            # Update daily P/L (reset daily)
            self.daily_pl += trade.profit_loss
            # Update best day P/L for consistency rule
            self.best_day_pl = max(self.best_day_pl, self.daily_pl)
            # Update challenge state in rules engine
            self.rules_engine.update_challenge_metrics(self.best_day_pl)

            logger.info(f"Daily P/L updated: ${self.daily_pl:.2f}")

            # Schedule ML training update
            if self.enable_ml and self.model_manager:
                task = asyncio.create_task(self.model_manager.train_with_trade(trade))
                self._add_task(task)
        except Exception as e:
            logger.exception(f"Error processing completed trade: {e}")

    # --- Daily Rollover ---
    def _check_day_change(self):
        """Check for day change (UTC) and reset daily metrics."""
        current_date_utc = datetime.datetime.now(datetime.timezone.utc).date()
        if self.current_day is None: # First run
             self.current_day = current_date_utc
             logger.info(f"Initialized trading day (UTC): {self.current_day}")
             self.start_of_day_balance = self.account.balance if self.account else 0.0
             self.rules_engine.update_start_of_day_balance(self.start_of_day_balance)
             return

        if current_date_utc != self.current_day:
            logger.info(f"Trading day {self.current_day} completed. Final Daily P/L: ${self.daily_pl:.2f}")
            # --- Persist daily stats ---
            self.analytics.record_daily_summary(self.current_day, self.daily_pl, self.best_day_pl) # Add method to analytics

            # --- Reset daily metrics ---
            self.daily_pl = 0.0
            self.best_day_pl = 0.0 # Reset best day for consistency rule tracking
            self.current_day = current_date_utc
            self.start_of_day_balance = self.account.balance if self.account else 0.0 # Use current balance as start for new day
            self.rules_engine.reset_daily_limits(self.start_of_day_balance) # Pass new start balance
            logger.info(f"New trading day started: {self.current_day}. Start Balance: ${self.start_of_day_balance:.2f}")

            # --- Perform other end-of-day tasks ---
            # Example: Save models, generate reports
            # if self.enable_ml and self.model_manager:
            #     task = asyncio.create_task(self.model_manager.save_models())
            #     self._add_task(task)
            # task = asyncio.create_task(self.analytics.save_report_async()) # Example
            # self._add_task(task)

    # --- Rule Checking ---
    def _check_rules_and_act(self):
        """Synchronously check rules and schedule actions if needed."""
        if not self.account: return # Cannot check rules without account state

        violations = self.rules_engine.check_rules(
            self.account,
            self.position_manager.get_all_positions(), # Assumes sync getter
            self.daily_pl
        )
        if violations:
            for violation in violations:
                logger.warning(f"Rule Violation: {violation.get('message', 'Unknown')}")
                action = violation.get('action')
                if action == 'liquidate':
                    logger.critical(f"LIQUIDATION TRIGGERED! Reason: {violation.get('reason', 'Rule violation')}")
                    # Schedule the async liquidation task
                    task = asyncio.create_task(
                        self.position_manager.liquidate_all_positions(violation.get('reason', 'Rule violation'))
                    )
                    self._add_task(task)
                    # Optionally stop trading after liquidation
                    # self._stop_requested.set()
                elif action == 'stop_trading':
                     logger.warning(f"STOP TRADING TRIGGERED! Reason: {violation.get('reason', 'Rule violation')}")
                     self._stop_requested.set() # Signal graceful stop

    # --- Task Management ---
    def _add_task(self, task: asyncio.Task):
        """Add a task to the tracking set and add a done callback to remove it."""
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard) # Automatically remove when done

    async def _cancel_all_tasks(self):
        """Cancel all tracked background tasks."""
        if not self._tasks: return
        logger.info(f"Cancelling {len(self._tasks)} background tasks...")
        cancelled_tasks = []
        for task in list(self._tasks): # Iterate over a copy
            if not task.done():
                task.cancel()
                cancelled_tasks.append(task)
        if cancelled_tasks:
            await asyncio.gather(*cancelled_tasks, return_exceptions=True)
            logger.info("Finished cancelling tasks.")
        self._tasks.clear()

    # --- Main Execution Loop ---
    async def _main_loop(self):
        """Main asynchronous update loop."""
        logger.info("Starting main update loop...")
        loop_interval = 1.0 # Seconds
        last_account_update_time = 0
        account_update_interval = 60 # Seconds, update account state less frequently via REST

        while not self._stop_requested.is_set():
            try:
                loop_start_time = time.monotonic()

                # 1. Check for day change
                self._check_day_change()

                # 2. Update account state periodically via REST API
                # TODO: Verify API call/field: self.api.get_account() call used for periodic REST updates (if different from initial WS stream)
                now = time.monotonic()
                if now - last_account_update_time > account_update_interval:
                    try:
                        account_info = await self.api.get_account()
                        if account_info:
                             self.account = account_info
                             self.analytics.add_account_snapshot(self.account) # Add snapshot
                             self._check_rules_and_act() # Check rules after update
                        else: logger.warning("Failed to update account info via REST.")
                        last_account_update_time = now
                    except Exception as acc_err: logger.error(f"Error updating account via REST: {acc_err}", exc_info=True)

                # 3. Run strategy updates (e.g., for time-based logic)
                await self.strategy_engine.update()

                # 4. Run position manager updates (e.g., trailing stops)
                await self.position_manager.update()

                # 5. Check rules (already checked after account update, maybe check again?)
                # self._check_rules_and_act()

                # --- Loop Sleep ---
                loop_end_time = time.monotonic()
                loop_duration = loop_end_time - loop_start_time
                sleep_time = max(0, loop_interval - loop_duration)
                await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                logger.info("Main loop cancelled.")
                break
            except Exception as e:
                logger.exception(f"Critical error in main loop: {e}. Stopping.")
                self._stop_requested.set() # Stop loop on critical error
                break
        logger.info("Main update loop finished.")

    # --- Public Run/Stop Methods ---
    async def run_async(self, duration: Optional[int] = None):
        """Run the trading system asynchronously."""
        if self.is_running: logger.warning("System is already running."); return
        self._stop_requested.clear() # Ensure stop flag is clear

        try:
            if not await self.initialize():
                logger.error("Trading system initialization failed. Exiting.")
                return

            self.is_running = True
            logger.info("Trading system started.")

            # Start the main loop task
            loop_task = asyncio.create_task(self._main_loop())
            self._add_task(loop_task) # Track the main loop task

            # Wait for duration or stop signal
            if duration:
                logger.info(f"Running for specified duration: {duration} seconds")
                try:
                    await asyncio.wait_for(self._stop_requested.wait(), timeout=duration)
                    logger.info("Stop requested during duration.")
                except asyncio.TimeoutError:
                    logger.info("Specified duration ended.")
            else:
                logger.info("Running indefinitely. Waiting for stop signal...")
                await self._stop_requested.wait() # Wait until stop is called

            logger.info("Stopping main loop...")
            if not loop_task.done():
                 loop_task.cancel()
                 await asyncio.wait([loop_task], return_when=asyncio.ALL_COMPLETED)

        except asyncio.CancelledError:
             logger.info("run_async task cancelled.")
        except Exception as e:
            logger.exception(f"Unhandled error during trading system run: {e}")
        finally:
            self.is_running = False
            logger.info("Initiating system shutdown...")
            await self.shutdown_async()
            logger.info("System run finished.")

    async def stop_async(self):
        """Request a graceful stop of the trading system."""
        if not self.is_running: logger.info("System is not running."); return
        logger.warning("Stop requested for trading system.")
        self._stop_requested.set()

    async def shutdown_async(self):
        """Shut down the trading system asynchronously."""
        if getattr(self, '_shutting_down', False): return
        self._shutting_down = True
        logger.info("Initiating asynchronous shutdown...")

        # 1. Signal stop if not already done
        self._stop_requested.set()
        self.is_running = False # Explicitly set state

        # 2. Cancel all background tasks (including main loop if still running)
        await self._cancel_all_tasks()

        # 3. Liquidate positions (optional, based on config?)
        liquidate_on_shutdown = get_config_value(self.config, 'SYSTEM.liquidate_on_shutdown', True)
        if liquidate_on_shutdown and self.position_manager:
            logger.info("Liquidating all open positions on shutdown...")
            try: await self.position_manager.liquidate_all_positions("System shutdown")
            except Exception as liq_err: logger.exception(f"Error during shutdown liquidation: {liq_err}")

        # 4. Generate final performance report
        if self.analytics:
             logger.info("Generating final performance summary...")
             try:
                  self.analytics.print_performance_summary()
                  report_dir = get_config_value(self.config, 'REPORTING.report_directory', 'reports')
                  report_file = f"performance_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                  report_path = os.path.join(report_dir, report_file)
                  self.analytics.save_performance_report(report_path) # Assumes sync save
             except Exception as report_err: logger.error(f"Error generating/saving report: {report_err}", exc_info=True)

        # 5. Shut down Tradovate API connection
        if self.api:
            logger.info("Shutting down Tradovate API connection...")
            try:
                # Assuming a disconnect method exists
                # TODO: Verify API call/field: self.api.disconnect() or equivalent method
                await self.api.disconnect()
            except TradovateError as api_shut_err:
                logger.exception(f"Error shutting down Tradovate API: {api_shut_err}")
            except Exception as e:
                 logger.exception(f"Unexpected error during Tradovate disconnect: {e}")

        logger.info("Trading system asynchronous shutdown finished.")
        self._shutting_down = False

    # --- Sync Wrappers (for convenience if needed) ---
    def run(self, duration: Optional[int] = None):
        """Synchronous entry point to run the system."""
        try: asyncio.run(self.run_async(duration))
        except KeyboardInterrupt: logger.info("KeyboardInterrupt caught in sync run(). System should be stopping.")
        except Exception as e: logger.exception(f"Error in sync run wrapper: {e}")

    def stop(self):
        """Synchronous request to stop the system."""
        if not self.is_running: logger.info("System not running."); return
        logger.info("Requesting synchronous stop...")
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running(): asyncio.run_coroutine_threadsafe(self.stop_async(), loop)
            else: loop.run_until_complete(self.stop_async())
        except Exception as e: logger.exception(f"Error requesting sync stop: {e}")

    def shutdown(self):
        """Synchronous wrapper for shutdown."""
        logger.info("Requesting synchronous shutdown...")
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                 future = asyncio.run_coroutine_threadsafe(self.shutdown_async(), loop)
                 future.result(timeout=20) # Wait with timeout
            else: loop.run_until_complete(self.shutdown_async())
        except Exception as e: logger.exception(f"Error during sync shutdown: {e}")