# core/tradovate_adapter.py
"""
Adapter class to bridge the gap between TradingSystem's expectations
and the functions provided in the Tradovate_Python_Client directory.
"""

import asyncio
import logging
import sys
import os
import json # <-- Add JSON for WebSocket messages
import websockets # <-- Add websockets library
from asyncio import Queue # <-- Add Queue for potential message buffering/handling
from typing import Dict, Any, Optional, List, Callable, Set # <-- Add Callable, Set
from pathlib import Path
import datetime # Added for timestamp handling if needed later

# Add the client directory to the Python path to allow imports
# This assumes the script using this adapter is run from the project root
CLIENT_DIR = Path(__file__).parent.parent / 'Tradovate_Python_Client'
sys.path.insert(0, str(CLIENT_DIR))

# Attempt to import necessary functions from the client directory
# We might need to adjust paths or how these are imported based on their structure
try:
    from GetAccessToken import getAccessToken # Assuming function name is getAccessToken
    from GetAccountList import getAccountList # Assuming function name
    from PositionList import getPositions # <-- ADD CORRECT IMPORT
    from GetFillList import getFillList # Assuming function name
    from PlaceMarketOrder import placeMarketOrder # Assuming function name
    from PlaceLimitOrder import placeLimitOrder # Assuming function name
    # from PlaceTrailStop import placeTrailStop # Assuming function name - handle if needed
    from CancelOrder import cancelOrder # Assuming function name
    from ModifyOrder import modifyOrder # TODO: Need ModifyOrder.py or equivalent function
    from ContractFind import contractFind # TODO: Need ContractFind.py or equivalent function
    from ContractItem import contractItem # TODO: Need ContractItem.py or equivalent function
    # Import other necessary functions as we implement methods
except ImportError as e:
    logging.critical(f"Failed to import functions from {CLIENT_DIR}: {e}. Ensure the client files exist and are structured correctly.")
    # Optionally re-raise or exit depending on desired behavior
    raise

# Remove the path addition after imports (optional, good practice)
sys.path.pop(0)

# Import data classes needed for return types (adjust path if necessary)
from utils.data_classes import Account, Position, Trade, Order # Add others as needed
# TODO: Define or import a suitable Quote data class if needed for callbacks
# from utils.data_classes import Quote

logger = logging.getLogger(__name__)

# Tradovate WebSocket Endpoints (adjust if URLs change)
TRADOVATE_WS_DEMO = "wss://demo.tradovateapi.com/v1/websocket"
TRADOVATE_WS_LIVE = "wss://live.tradovateapi.com/v1/websocket"

class TradovateAdapter:
    """
    Adapts the Tradovate_Python_Client functions to the interface
    expected by TradingSystem (methods like connect, authorize, get_account, etc.).
    Includes WebSocket handling for real-time data.
    """
    def __init__(
        self,
        environment: str,
        username: str,
        password: str,
        api_key: str, # Corresponds to 'sec' in client's config?
        api_secret: str, # Corresponds to 'cid' in client's config? - VERIFY THIS MAPPING
        device_id: str, # Corresponds to 'deviceId'
        # Add other config params if needed (e.g., app_version from client config)
    ):
        """
        Initialize the adapter with connection details.

        Note: The mapping of api_key/api_secret to client's cid/sec needs verification.
        The client scripts likely read these from their own config.py.
        This adapter might need to override or directly use those functions' config handling.
        For now, we store them.
        """
        logger.info(f"Initializing TradovateAdapter for env: {environment}")
        self.environment = environment.upper() # Ensure consistent casing ('DEMO' or 'LIVE')
        self.username = username
        self.password = password
        self.api_key = api_key
        self.api_secret = api_secret
        self.device_id = device_id
        # Client might need appId and appVersion, often hardcoded or in its config
        self.app_id = "TradeLockerApp" # Or read from config if needed
        self.app_version = "1.0" # Or read from config

        self.access_token: Optional[str] = None
        self.token_expiry: Optional[datetime.datetime] = None

        # Determine if we are in 'live' mode based on environment
        self.is_live = self.environment == 'LIVE'
        self.ws_url = TRADOVATE_WS_LIVE if self.is_live else TRADOVATE_WS_DEMO

        # --- WebSocket related variables ---
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.ws_handler_task: Optional[asyncio.Task] = None
        self.subscribed_symbols: Set[str] = set() # Keep track of subscribed symbols
        self.symbol_callbacks: Dict[str, Callable] = {} # Map symbol to callback function
        self.ws_connected = asyncio.Event() # Event to signal successful WS connection/auth
        self.ws_request_id = 1 # Counter for WebSocket request IDs
        # ------------------------------------

        # Store credentials in a way the client functions might expect
        # This assumes the imported functions might read from a shared config object or need these passed directly
        # TODO: Revisit how credentials are passed to the imported functions.
        # They might implicitly use Tradovate_Python_Client/config.py.
        self.credentials = {
            'name': self.username,
            'password': self.password,
            'appId': self.app_id,
            'appVersion': self.app_version,
            'deviceId': self.device_id,
            'cid': self.api_secret, # Mapping based on MLAlgoTrader wrapper example - VERIFY
            'sec': self.api_key     # Mapping based on MLAlgoTrader wrapper example - VERIFY
        }
        logger.debug("TradovateAdapter initialized with credentials.")

    async def _get_valid_token(self) -> Optional[str]:
        """
        Gets a valid access token, requesting a new one if expired or missing.
        Wraps the synchronous getAccessToken call.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        # Add a small buffer (e.g., 5 minutes) to expiry time check
        buffer = datetime.timedelta(minutes=5)

        if self.access_token and self.token_expiry and (self.token_expiry - buffer) > now:
            # logger.debug("Using existing access token.")
            return self.access_token

        logger.info("Requesting new Tradovate access token...")
        try:
            # The client's getAccessToken likely takes credentials directly or reads its config
            # Assuming it needs credentials passed. Adjust if it reads config.py implicitly.
            # The 'live' parameter selects the environment.
            # TODO: Verify parameters for the actual getAccessToken function.
            loop = asyncio.get_running_loop()
            # Use to_thread for the potentially blocking network call
            token_data = await loop.run_in_executor(
                None, # Use default executor
                getAccessToken,
                self.is_live # Pass boolean for live/demo only
            )

            # Assuming getAccessToken returns a tuple: (token, expiry_timestamp_str_or_dt)
            if token_data and isinstance(token_data, (list, tuple)) and len(token_data) >= 2:
                self.access_token = token_data[0]
                expiry_info = token_data[1]

                # Parse expiry_info (it could be a string or datetime object)
                if isinstance(expiry_info, datetime.datetime):
                    self.token_expiry = expiry_info
                elif isinstance(expiry_info, str):
                    # Attempt to parse common ISO formats
                    try:
                        # Format like '2023-04-01T10:00:00.000Z'
                        self.token_expiry = datetime.datetime.fromisoformat(expiry_info.replace('Z', '+00:00'))
                    except ValueError:
                        logger.error(f"Could not parse token expiry timestamp: {expiry_info}")
                        self.token_expiry = None # Indicate failure
                else:
                     logger.error(f"Unexpected token expiry format: {type(expiry_info)}")
                     self.token_expiry = None

                if self.access_token and self.token_expiry:
                    logger.info(f"Successfully obtained new access token expiring at {self.token_expiry}")
                    return self.access_token
                else:
                    logger.error("Failed to obtain or parse access token/expiry.")
                    self.access_token = None
                    self.token_expiry = None
                    return None
            else:
                logger.error(f"Unexpected return format from getAccessToken: {token_data}")
                self.access_token = None
                self.token_expiry = None
                return None

        except Exception as e:
            logger.exception(f"Error getting access token: {e}")
            self.access_token = None
            self.token_expiry = None
            return None

    async def connect(self) -> bool:
        """
        Establishes the initial connection:
        1. Gets a REST API access token.
        2. Connects to the WebSocket endpoint and starts the handler.
        """
        logger.info("Connecting... (Getting initial access token)")
        token = await self._get_valid_token()
        if not token:
            logger.error("Connection failed (could not obtain access token).")
            return False
        logger.info("Access token obtained.")

        # --- Initiate WebSocket Connection ---
        logger.info(f"Connecting to WebSocket: {self.ws_url}")
        try:
            # Start the WebSocket handler task
            self.ws_handler_task = asyncio.create_task(self._websocket_handler())
            # Wait for the WebSocket to connect and authorize successfully
            await asyncio.wait_for(self.ws_connected.wait(), timeout=15) # Adjust timeout if needed
            logger.info("WebSocket connection and authorization successful.")
            return True
        except websockets.exceptions.InvalidURI:
            logger.exception(f"Invalid WebSocket URI: {self.ws_url}")
            return False
        except websockets.exceptions.WebSocketException as e:
            logger.exception(f"WebSocket connection failed: {e}")
            return False
        except asyncio.TimeoutError:
            logger.error("WebSocket connection/authorization timed out.")
            # Ensure task is cancelled if connection failed partway
            if self.ws_handler_task and not self.ws_handler_task.done():
                self.ws_handler_task.cancel()
            return False
        except Exception as e: # Catch other potential errors during connect
             logger.exception(f"An unexpected error occurred during WebSocket connect: {e}")
             if self.ws_handler_task and not self.ws_handler_task.done():
                self.ws_handler_task.cancel()
             return False
        # ------------------------------------

    async def authorize(self) -> bool:
        """
        Authorization is typically handled by obtaining the access token.
        This method ensures a valid token exists.
        """
        logger.info("Authorizing... (Checking/Refreshing access token)")
        token = await self._get_valid_token()
        return token is not None

    async def shutdown_async(self):
        """
        Clean up resources: close WebSocket connection and cancel handler task.
        """
        logger.info("Shutting down TradovateAdapter...")
        # --- Close WebSocket ---
        if self.ws_handler_task and not self.ws_handler_task.done():
            logger.info("Cancelling WebSocket handler task...")
            self.ws_handler_task.cancel()
            try:
                await self.ws_handler_task # Wait for task cancellation
            except asyncio.CancelledError:
                logger.info("WebSocket handler task cancelled.")
            except Exception as e:
                 logger.exception(f"Error during WebSocket handler task cancellation: {e}")

        if self.ws and self.ws.open:
            logger.info("Closing WebSocket connection...")
            try:
                await self.ws.close(code=1000, reason='Client shutdown')
                logger.info("WebSocket connection closed.")
            except websockets.exceptions.WebSocketException as e:
                logger.error(f"Error closing WebSocket: {e}")
        self.ws = None
        self.ws_connected.clear()
        # ---------------------

        self.access_token = None
        self.token_expiry = None
        logger.info("TradovateAdapter shut down.")

    async def _websocket_handler(self):
        """
        Handles the persistent WebSocket connection, authentication, message receiving,
        heartbeats, and routing data to callbacks.
        Runs as a background task.
        """
        logger.info("WebSocket handler started.")
        try:
            async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws: # Added keepalive pings
                self.ws = ws
                logger.info("WebSocket connected. Authorizing...")

                # --- Authorize WebSocket Connection ---
                auth_request = f"authorize\\n{self.ws_request_id}\\n\\n{self.access_token}"
                self.ws_request_id += 1
                await ws.send(auth_request)

                # --- Main Message Loop ---
                async for message in ws:
                    # Tradovate WS sends frames: type (char) + body (string)
                    frame_type = message[0]
                    body = message[1:]

                    if frame_type == 'o': # Open frame (occurs on successful connect/auth)
                        logger.info("WebSocket OPEN frame received. Connection authorized.")
                        self.ws_connected.set() # Signal successful connection
                        # Resubscribe to symbols if reconnecting? TODO: Add logic if needed

                    elif frame_type == 'h': # Heartbeat frame
                        # logger.debug("WebSocket HEARTBEAT received.")
                        # Respond to server heartbeat with a heartbeat '[]'
                        await ws.send("[]")

                    elif frame_type == 'a': # Data frame (JSON array)
                        try:
                            data_list = json.loads(body)
                            for item in data_list:
                                # Process each item in the data array
                                # logger.debug(f"WebSocket data item: {item}") # Very verbose
                                await self._process_websocket_data(item)
                        except json.JSONDecodeError:
                            logger.error(f"Failed to decode JSON data: {body}")
                        except Exception as e:
                            logger.exception(f"Error processing WebSocket data item: {e}")

                    elif frame_type == 'c': # Close frame
                        logger.warning(f"WebSocket CLOSE frame received: {body}")
                        self.ws_connected.clear()
                        break # Exit loop on close frame

                    else:
                        logger.warning(f"Received unknown WebSocket frame type '{frame_type}': {body}")

        except websockets.exceptions.ConnectionClosedOK:
            logger.info("WebSocket connection closed normally.")
        except websockets.exceptions.ConnectionClosedError as e:
            logger.error(f"WebSocket connection closed with error: {e}")
        except asyncio.CancelledError:
            logger.info("WebSocket handler task cancelled.")
            # Connection closing is handled in shutdown_async
        except Exception as e:
            logger.exception(f"WebSocket handler error: {e}")
        finally:
            logger.warning("WebSocket handler task finished.")
            self.ws = None
            self.ws_connected.clear()
            # TODO: Implement reconnection logic here if desired

    async def _process_websocket_data(self, data_item: Dict[str, Any]):
        """ Process individual data items received from WebSocket 'a' frames. """
        event_type = data_item.get('e')
        payload = data_item.get('d')

        if not event_type or not payload:
            logger.warning(f"Received data item with missing 'e' or 'd': {data_item}")
            return

        # logger.debug(f"Processing event type: {event_type}") # Verbose

        if event_type == 'md': # Market Data Events
            await self._handle_market_data(payload)
        elif event_type == 'props': # User Property updates (account changes?)
            logger.info(f"User Props Update: {payload}") # Log for now
            # TODO: Parse and route to _on_account_update if structure matches
        elif event_type == 'fill': # Order Fill event
             logger.info(f"Fill Event: {payload}")
             # TODO: Parse fill data and route to _on_fill callback
        elif event_type == 'order': # Order Status event
             logger.info(f"Order Event: {payload}")
             # TODO: Parse order status and route to _on_order_update callback
        # Add other event types ('chart', etc.) if needed
        else:
            # logger.debug(f"Ignoring unhandled event type: {event_type}")
            pass

    async def _handle_market_data(self, payload: Dict[str, Any]):
        """ Handles market data payloads ('md') from the WebSocket. """
        md_event_type = payload.get('md') # Tradovate nests event type here

        if md_event_type == 'quote':
            # --- Log the raw payload for inspection --- #
            logger.info(f"Raw Quote Payload Received: {json.dumps(payload)}") 
            # -------------------------------------------- #
            symbol = payload.get('contractSymbol')
            if symbol and symbol in self.symbol_callbacks:
                # logger.debug(f"Quote received for {symbol}: {payload}")
                # TODO: Map Tradovate quote fields ('bid', 'ask', 'bidSize', 'askSize', 'timestamp', etc.)
                #       to the expected format for the callback (e.g., a Quote dataclass).
                # Example mapping (VERIFY FIELD NAMES and structure):
                quote_data = {
                    'symbol': symbol,
                    'timestamp': payload.get('timestamp'), # Verify format
                    'bid': payload.get('bidPrice'),
                    'ask': payload.get('askPrice'),
                    'bid_size': payload.get('bidSize'),
                    'ask_size': payload.get('askSize'),
                    'last': payload.get('lastPrice'), # Or 'tradePrice'?
                    'last_size': payload.get('lastSize'), # Or 'tradeSize'?
                    # Add other relevant fields
                }
                # Create Quote object if using dataclasses
                # quote_obj = Quote(**quote_data)
                # await self.symbol_callbacks[symbol](quote_obj)
                await self.symbol_callbacks[symbol](quote_data) # Pass dict for now
            # else:
                # logger.warning(f"Received quote for unsubscribed/unknown symbol: {symbol}")
        # Add handling for other md types like 'dom', 'chart' if needed
        else:
            # logger.debug(f"Ignoring unhandled market data type: {md_event_type}")
            pass

    # --- Placeholder Methods (to be implemented) ---

    async def get_account(self) -> Optional[Account]:
        """
        Fetches account details using the getAccountList function.
        Assumes getAccountList returns a list of account dicts and we use the first one.
        Maps the Tradovate fields to the internal Account dataclass.
        """
        logger.info("Fetching account details via Adapter...")
        token = await self._get_valid_token()
        if not token:
            logger.error("get_account: Failed to get valid token.")
            return None

        try:
            loop = asyncio.get_running_loop()
            # TODO: Verify the exact parameters required by the imported getAccountList function.
            # Assuming it needs token and live status.
            accounts_data = await loop.run_in_executor(
                None, # Use default executor
                getAccountList,
                token,
                self.is_live
            )

            # TODO: Verify the structure returned by getAccountList.
            # Assuming it returns a list of account dictionaries.
            if not accounts_data or not isinstance(accounts_data, list) or not accounts_data[0]:
                logger.error(f"get_account: No account data returned or unexpected format: {accounts_data}")
                return None

            # Assume we use the first account returned
            # TODO: Add logic to select specific account if needed (e.g., by name/ID from config)
            primary_account_data = accounts_data[0]
            logger.debug(f"Received account data: {primary_account_data}")

            # --- Map Tradovate fields to internal Account dataclass ---
            # TODO: Verify these field names from the actual Tradovate response.
            acc_id = primary_account_data.get('id')
            acc_name = primary_account_data.get('name')
            acc_type = primary_account_data.get('accountType') # e.g., 'Demo', 'Live'
            currency = primary_account_data.get('currency', {}).get('name', 'USD') # Nested?
            balance = primary_account_data.get('balance') # Current cash balance?
            equity = primary_account_data.get('totalCashValue') # Total value including positions?
            realized_pl = primary_account_data.get('realizedPL', 0.0)
            unrealized_pl = primary_account_data.get('unrealizedPL', 0.0)
            margin_used = primary_account_data.get('totalInitialMargin', 0.0)
            margin_available = primary_account_data.get('buyingPower', 0.0)
            # ----------------------------------------------------------

            # Basic validation
            if acc_id is None or balance is None or equity is None:
                logger.error(f"get_account: Missing critical fields (id, balance, equity) in response: {primary_account_data}")
                return None

            # Create and return the Account dataclass instance
            account = Account(
                id=int(acc_id), # Ensure ID is integer
                name=str(acc_name) if acc_name is not None else f"Account {acc_id}",
                account_type=str(acc_type) if acc_type is not None else 'Unknown',
                base_currency=str(currency),
                balance=float(balance),
                equity=float(equity),
                initial_balance=float(balance), # Set initial balance from first fetch
                realized_pl=float(realized_pl),
                unrealized_pl=float(unrealized_pl),
                margin_used=float(margin_used),
                margin_available=float(margin_available)
            )
            logger.info(f"Successfully mapped account data: ID={account.id}, Balance={account.balance:.2f}")
            return account

        except Exception as e:
            logger.exception(f"get_account: Error during account fetch or processing: {e}")
            return None

    async def subscribe_quote(self, symbol: str, callback: Callable):
        """
        Subscribes to real-time quotes for a given symbol via WebSocket.
        Stores the callback to be invoked when new data arrives.
        """
        if not self.ws or not self.ws.open:
            logger.error(f"Cannot subscribe to {symbol}: WebSocket not connected.")
            return # Or raise an error?

        if symbol in self.subscribed_symbols:
            logger.warning(f"Already subscribed to {symbol}. Updating callback.")
            self.symbol_callbacks[symbol] = callback
            return

        logger.info(f"Subscribing to quotes for {symbol}...")
        self.subscribed_symbols.add(symbol)
        self.symbol_callbacks[symbol] = callback

        # --- Send subscribe message via WebSocket ---
        # Format: endpoint\nreqId\n\nbody
        endpoint = "md/subscribeQuote"
        req_id = self.ws_request_id
        self.ws_request_id += 1
        body = json.dumps({"symbol": symbol}) # Body is JSON string
        message = f"{endpoint}\\n{req_id}\\n\\n{body}"

        try:
            await self.ws.send(message)
            logger.info(f"Subscription request sent for {symbol} (Req ID: {req_id})")
            # TODO: Optionally wait for confirmation/handle errors if API provides them
        except websockets.exceptions.ConnectionClosed:
             logger.error(f"Failed to send subscription for {symbol}: WebSocket closed.")
             # Remove from subscribed list if send failed
             self.subscribed_symbols.discard(symbol)
             if symbol in self.symbol_callbacks: del self.symbol_callbacks[symbol]
        except Exception as e:
             logger.exception(f"Error sending subscription request for {symbol}: {e}")
             # Consider removing from lists here too

    async def sync_request(self):
        """
        Performs an initial synchronization by fetching current account state,
        positions, and orders.
        """
        logger.info("Performing initial sync request via Adapter...")

        # Fetching account details might also be part of a sync, but
        # TradingSystem.initialize already calls get_account separately.
        # We could call it here too if needed, but might be redundant.

        # Fetch positions
        try:
            positions = await self.position_list()
            logger.info(f"Sync request: Fetched {len(positions)} positions.")
            # TODO: Potentially update internal state or notify other components
            #       if the adapter needs to manage state directly.
            #       Currently, PositionManager likely calls position_list itself.
        except Exception as e:
            logger.error(f"Sync request: Failed to fetch positions: {e}", exc_info=True)
            # Decide if this is critical enough to raise an error or just log

        # Fetch orders
        try:
            orders = await self.order_list() # Call the order_list method
            logger.info(f"Sync request: Fetched {len(orders)} orders.")
            # TODO: Similar to positions, decide if adapter needs to store/process these.
        except Exception as e:
            logger.error(f"Sync request: Failed to fetch orders: {e}", exc_info=True)
            # Decide if this is critical

        logger.info("Sync request completed.")
        # This method might not need to return anything, just ensures data is fetched.

    async def account_list(self) -> List[Dict[str, Any]]:
        """ Fetches list of accounts (raw data). """
        logger.warning("account_list method not fully implemented yet.")
        # TODO: Implement using GetAccountList.py
        return []

    async def position_list(self) -> List[Dict[str, Any]]:
        """
        Fetches the list of current positions using the getPositionList function.
        Returns the raw list of position dictionaries.
        """
        logger.debug("Fetching position list via Adapter...") # Debug level might be more appropriate
        token = await self._get_valid_token()
        if not token:
            logger.error("position_list: Failed to get valid token.")
            return []

        try:
            loop = asyncio.get_running_loop()
            # TODO: Verify the exact parameters required by the imported getPositionList function.
            # Assuming it needs token and live status.
            # --- UPDATE: getPositions reads config.py for token, only needs 'live' param --- #
            positions_data = await loop.run_in_executor(
                None, # Use default executor
                # getPositionList, # <-- USE CORRECT FUNCTION NAME
                getPositions,
                # token, # <-- REMOVE TOKEN ARGUMENT
                self.is_live
            )

            # TODO: Verify the structure returned by getPositionList.
            # Assuming it returns a list of position dictionaries.
            if positions_data is None: # It might return None on error or empty list
                 logger.info("position_list: No positions returned or error occurred.")
                 return []
            elif not isinstance(positions_data, list):
                 logger.error(f"position_list: Unexpected return type: {type(positions_data)}")
                 return []
            else:
                logger.debug(f"position_list: Received {len(positions_data)} positions.")
                # We return the raw list as expected by some parts of TradingSystem/PositionManager
                return positions_data

        except Exception as e:
            logger.exception(f"position_list: Error during position fetch: {e}")
            return []

    async def order_list(self) -> List[Dict[str, Any]]:
        """
        Fetches list of orders (raw data).
        Currently returns an empty list as no specific function exists in the client scripts.
        """
        logger.warning(
            "order_list: Fetching the initial list of working orders is not currently supported "
            "by the available Tradovate_Python_Client scripts. Returning empty list."
        )
        # TODO: Implement this if a suitable function (e.g., 'getOrderList') is added
        #       to the client scripts or found within existing ones.
        return []

    async def order_placeOrder(self, order_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Places an order (Market or Limit) using the appropriate client function.

        Args:
            order_data: A dictionary representing the order, expected to contain
                        keys like 'accountId', 'symbol', 'orderQty', 'action', 'orderType', and 'price' (for limit orders).

        Returns:
            A dictionary containing the 'orderId' if successful, None otherwise.
            This matches the expected return format for PositionManager.
        """
        logger.info(f"Placing order via Adapter: {order_data}")
        token = await self._get_valid_token()
        if not token:
            logger.error("order_placeOrder: Failed to get valid token.")
            return None

        # Extract common parameters
        try:
            account_id = int(order_data['accountId'])
            symbol = str(order_data['symbol'])
            qty = int(order_data['orderQty'])
            action = str(order_data['action']) # 'Buy' or 'Sell'
            order_type = str(order_data['orderType']) # 'Market' or 'Limit'
        except KeyError as e:
            logger.error(f"order_placeOrder: Missing required key in order_data: {e}")
            return None
        except (ValueError, TypeError) as e:
            logger.error(f"order_placeOrder: Invalid data type in order_data: {e}")
            return None

        order_id: Optional[int] = None
        try:
            loop = asyncio.get_running_loop()

            if order_type == "Market":
                order_id = await loop.run_in_executor(
                    None,
                    placeMarketOrder,
                    self.is_live,
                    account_id,
                    symbol,
                    qty,
                    action,
                    token
                )
            elif order_type == "Limit":
                try:
                    price = float(order_data['price'])
                except KeyError:
                    logger.error("order_placeOrder: Missing 'price' key for Limit order.")
                    return None
                except (ValueError, TypeError):
                    logger.error("order_placeOrder: Invalid 'price' data type for Limit order.")
                    return None

                order_id = await loop.run_in_executor(
                    None,
                    placeLimitOrder,
                    self.is_live,
                    account_id,
                    symbol,
                    qty,
                    action,
                    price,
                    token
                )
            else:
                logger.error(f"order_placeOrder: Unsupported order type: {order_type}")
                return None

            # Return the result in the format expected by PositionManager (dict with orderId)
            if order_id is not None:
                logger.info(f"Order placement successful (ID: {order_id}). Returning dict.")
                return {"orderId": order_id}
            else:
                logger.error("Order placement failed (client function returned None). Returning None.")
                return None

        except Exception as e:
            logger.exception(f"order_placeOrder: Unexpected error during order placement call: {e}")
            return None

    async def order_modify_order(self, modification_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """ Modifies an existing order. Calls the client function. """
        logger.debug(f"Modifying order via Adapter: {modification_data}") # Debug level
        token = await self._get_valid_token()
        if not token:
            logger.error("order_modify_order: Failed to get valid token.")
            return None

        try:
            order_id = int(modification_data.pop('orderId')) # Pop ID, rest are mods
        except KeyError:
            logger.error("order_modify_order: Missing 'orderId' in modification_data.")
            return None
        except (ValueError, TypeError):
            logger.error("order_modify_order: Invalid 'orderId' data type.")
            return None

        # modification_data now contains qty, price, etc.
        if not modification_data:
             logger.warning("order_modify_order: No modification parameters provided.")
             # Return None or potentially fetch and return current order state?

        try:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                modifyOrder,
                self.is_live,
                order_id,
                token,
                **modification_data # Pass remaining args as keyword args
            )
            # TODO: Process result from modifyOrder script if it returns confirmation/order state
            if result is not None:
                 logger.info(f"Order modification request successful for ID: {order_id}")
            else:
                 # modifyOrder script currently logs a warning and returns None
                 logger.warning(f"Order modification request for ID {order_id} sent, but script not implemented.")

            return result # Return whatever modifyOrder returns (currently None)

        except ImportError:
             logger.error("order_modify_order: modifyOrder function not found. Ensure ModifyOrder.py exists and is importable.")
             return None
        except Exception as e:
            logger.exception(f"order_modify_order: Error calling modifyOrder: {e}")
            return None

    async def order_cancel_order(self, cancellation_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Cancels an existing order using the cancelOrder client function.

        Args:
            cancellation_data: Dictionary expected to contain 'orderId'.

        Returns:
            A dictionary containing the 'orderId' if successful, None otherwise.
        """
        logger.info(f"Cancelling order via Adapter: {cancellation_data}")
        token = await self._get_valid_token()
        if not token:
            logger.error("order_cancel_order: Failed to get valid token.")
            return None

        try:
            order_id = int(cancellation_data['orderId'])
        except KeyError:
            logger.error("order_cancel_order: Missing 'orderId' key in cancellation_data.")
            return None
        except (ValueError, TypeError):
            logger.error("order_cancel_order: Invalid 'orderId' data type.")
            return None

        try:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                cancelOrder,
                self.is_live,
                order_id,
                token
            )

            if result is not None:
                logger.info(f"Order cancellation request successful (ID: {order_id}).")
            else:
                 logger.error(f"Order cancellation request failed (ID: {order_id}). Client function returned None.")

            # Return the result (which is None or {"orderId": order_id})
            return result

        except Exception as e:
            logger.exception(f"order_cancel_order: Unexpected error during cancellation call: {e}")
            return None

    async def contract_find(self, query_data: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """ Finds contract details by name/pattern. Calls the client function. """
        logger.debug(f"Finding contract via Adapter: {query_data}")
        token = await self._get_valid_token()
        if not token:
            logger.error("contract_find: Failed to get valid token.")
            return None

        try:
            search_term = str(query_data['name']) # Assume search is by 'name'
        except KeyError:
            logger.error("contract_find: Missing 'name' key in query_data.")
            return None
        except (ValueError, TypeError):
            logger.error("contract_find: Invalid 'name' data type.")
            return None

        try:
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(
                None,
                contractFind,
                self.is_live,
                token,
                search_term
            )
            # TODO: Process results if contractFind returns data
            if results is not None:
                 logger.info(f"Contract find returned results for '{search_term}'.")
            else:
                 # contractFind script currently logs warning and returns None
                 logger.warning(f"Contract find request for '{search_term}' sent, but script not implemented.")

            return results # Return whatever contractFind returns (currently None, should be List[Dict] when implemented)

        except ImportError:
             logger.error("contract_find: contractFind function not found. Ensure ContractFind.py exists and is importable.")
             return None
        except Exception as e:
            logger.exception(f"contract_find: Error calling contractFind: {e}")
            return None

    async def contract_item(self, query_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """ Gets contract details by ID. Calls the client function. """
        logger.debug(f"Getting contract item via Adapter: {query_data}")
        token = await self._get_valid_token()
        if not token:
            logger.error("contract_item: Failed to get valid token.")
            return None

        try:
            contract_id = int(query_data['id']) # Assume query by 'id'
        except KeyError:
            logger.error("contract_item: Missing 'id' key in query_data.")
            return None
        except (ValueError, TypeError):
            logger.error("contract_item: Invalid 'id' data type.")
            return None

        try:
            loop = asyncio.get_running_loop()
            details = await loop.run_in_executor(
                None,
                contractItem,
                self.is_live,
                token,
                contract_id
            )
            # TODO: Process details if contractItem returns data
            if details is not None:
                 logger.info(f"Contract item returned details for ID: {contract_id}.")
            else:
                 # contractItem script currently logs warning and returns None
                 logger.warning(f"Contract item request for ID {contract_id} sent, but script not implemented.")

            return details # Return whatever contractItem returns (currently None)

        except ImportError:
            logger.error("contract_item: contractItem function not found. Ensure ContractItem.py exists and is importable.")
            return None
        except Exception as e:
            logger.exception(f"contract_item: Error calling contractItem: {e}")
            return None

# TODO: Add implementations for all the placeholder methods above,
#       mapping them to the appropriate functions from Tradovate_Python_Client,
#       handling async calls, parameter mapping, and return value parsing/conversion.
# TODO: Verify the exact function names and parameters required by the imported client scripts.
# TODO: Address the real-time data subscription issue.
# TODO: Verify credential mapping (api_key/secret vs cid/sec).
# TODO: Check how the client scripts handle configuration (do they read config.py? Need override?)
# TODO: Implement WebSocket reconnection logic in _websocket_handler.
# TODO: Implement parsing for fill/order/props events in _process_websocket_data and route to callbacks (_on_fill, etc.).
# TODO: Define and use appropriate data classes (Quote, Fill, etc.) for callbacks.
# TODO: Add unsubscribe logic if needed.
# TODO: Verify Tradovate WebSocket message formats and field names.
# </rewritten_file> 