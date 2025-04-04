# tradelocker/api.py
"""
Asynchronous TradeLocker and Tradovate API client for market data and order execution.
Compatible with Tradovate REST/TradeLocker REST API v2 (adjust endpoints if needed).

Requires: aiohttp, websockets
"""
import asyncio
import json
import logging
import time
import datetime
import os
from typing import Dict, List, Optional, Any, Callable, Tuple, Union, Mapping, Set

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException, ConnectionClosedOK, ConnectionClosedError


from utils.data_classes import PriceBar, PriceSnapshot, Position, Account
from utils.helpers import get_instrument_details # For formatting price in error messages

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.0.0 Safari/537.36"

# Use standard logger
logger = logging.getLogger(__name__)

# --- Constants ---
# Base URLs (Consider making these configurable via config.ini)
DEFAULT_DEMO_API_URL = "https://demo.tradelocker.com/backend-api"
DEFAULT_LIVE_API_URL = "https://live.tradelocker.com/backend-api"
DEFAULT_DEMO_WS_URL = "wss://demo.tradelocker.com/ws"
DEFAULT_LIVE_WS_URL = "wss://live.tradelocker.com/ws"

USER_AGENT = "TradeLocker-Python-Client/2.1"
TOKEN_REFRESH_BUFFER = 300  # Refresh token 5 minutes before expiry (seconds)
WEBSOCKET_PING_INTERVAL = 25 # Send pings slightly more often
WEBSOCKET_PING_TIMEOUT = 10
WEBSOCKET_CLOSE_TIMEOUT = 5
WEBSOCKET_RECONNECT_DELAY = 5  # Seconds
HEARTBEAT_INTERVAL = 20 # Seconds (Client-side heartbeat/ping)

# API Paths (relative to base_url - Verify these against current TradeLocker docs)
PATH_AUTH_TOKEN = "/auth/jwt/token"
PATH_AUTH_REFRESH = "/auth/jwt/refresh"
PATH_AUTH_ACCOUNTS = "/auth/jwt/all-accounts" # Used for initial account ID/Num lookup
PATH_TRADE_ACCOUNTS = "/trade/accounts" # Used for basic account details (name, currency)
PATH_TRADE_CONFIG = "/trade/config" # Crucial for parsing state/positions/orders
PATH_TRADE_ACCOUNT_STATE = "/trade/accounts/{account_id}/state" # Detailed balance/margin/PL
PATH_TRADE_INSTRUMENTS = "/trade/accounts/{account_id}/instruments" # List of tradable instruments
PATH_TRADE_POSITIONS = "/trade/accounts/{account_id}/positions" # Current open positions
PATH_TRADE_ORDERS = "/trade/accounts/{account_id}/orders" # Pending/working orders
PATH_TRADE_HISTORY = "/trade/history" # OHLCV data
PATH_TRADE_QUOTES = "/trade/quotes" # Real-time bid/ask
PATH_TRADE_ORDER_BY_ID = "/trade/orders/{order_id}" # Modify/cancel specific order
PATH_TRADE_POSITION_BY_ID = "/trade/positions/{position_id}" # Modify/close specific position
PATH_TRADE_CLOSE_ALL_POSITIONS = "/trade/accounts/{account_id}/positions" # Bulk close

# --- Custom Exceptions ---
class TradeLockerError(Exception):
    """Base exception for TradeLocker API errors."""
    pass

class TradeLockerAuthError(TradeLockerError):
    """Exception for authentication errors."""
    pass

class TradeLockerAPIError(TradeLockerError):
    """Exception for general API request errors."""
    def __init__(self, message, status_code=None, response_body=None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body

class TradeLockerWebSocketError(TradeLockerError):
    """Exception for WebSocket related errors."""
    pass


# --- Main API Class ---
class TradeLockerAPI:
    """Asynchronous client for TradeLocker REST API and WebSocket."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize TradeLocker API client.

        Args:
            config: Main configuration dictionary (loaded from config.py).
        """
        self.config = config
        self._log = logger # Use the module logger

        # --- Configuration Extraction ---
        api_config = config.get('API', {})
        self._email = api_config.get('email')
        self._password = api_config.get('password')
        self._server = api_config.get('server', 'TradingView') # Default, verify broker name
        self._target_account_id = str(api_config.get('account_id')) if api_config.get('account_id') else None
        self._environment = api_config.get('environment', 'demo').lower()

        # Determine URLs based on environment
        if self._environment == 'live':
            self.base_url = api_config.get('base_url', DEFAULT_LIVE_API_URL)
            self.ws_url = api_config.get('ws_url', DEFAULT_LIVE_WS_URL)
        else: # Default to demo
            self.base_url = api_config.get('base_url', DEFAULT_DEMO_API_URL)
            self.ws_url = api_config.get('ws_url', DEFAULT_DEMO_WS_URL)

        self._log.info(f"API Environment: {self._environment.upper()}")
        self._log.debug(f"API Base URL: {self.base_url}")
        self._log.debug(f"WebSocket URL: {self.ws_url} (Type: {type(self.ws_url)})")

        # --- State Variables ---
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expires_at: float = 0
        self.account_id: Optional[str] = None # The selected account ID (string)
        self.account_num: Optional[str] = None # The selected account number (string, often needed in headers)

        self.session: Optional[aiohttp.ClientSession] = None
        self._session_owner: bool = False # Flag to track if we created the session

        self.ws_connection: Optional[websockets.WebSocketClientProtocol] = None
        self._ws_listener_task: Optional[asyncio.Task] = None
        self._ws_heartbeat_task: Optional[asyncio.Task] = None
        self._ws_reconnect_lock = asyncio.Lock()
        self._is_shutting_down = False
        self._ws_connected = asyncio.Event() # Event to signal WS connection status

        # --- WebSocket Subscription Management ---
        self._ws_subscriptions: Dict[str, Dict[str, Any]] = {}
        self._ws_callback_map: Dict[str, List[Callable]] = {} # Maps channel type ('quotes', 'account') to callbacks

        # --- Caches ---
        self.instruments_cache: Dict[str, Dict] = {} # Cache by instrument ID (string)
        self.market_data_cache: Dict[str, PriceSnapshot] = {} # Cache by instrument symbol (string)
        self._trade_config_cache: Optional[Dict] = None # Cache for /trade/config response

        # Rate limiting info
        self.rate_limit_remaining: int = 300
        self.rate_limit_reset: int = 0

    # --- Async Context Manager ---
    async def __aenter__(self):
        await self._create_session()
        # Initialization now happens explicitly via await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown_async()

    # --- Session Management ---
    async def _create_session(self):
        """Create or reuse an aiohttp ClientSession."""
        if self.session is None or self.session.closed:
            # Consider adding timeout config here
            # timeout = aiohttp.ClientTimeout(total=60) # Example: 60s total timeout
            self.session = aiohttp.ClientSession(
                headers={"User-Agent": USER_AGENT}
                # timeout=timeout
            )
            self._session_owner = True
            self._log.debug("Created new aiohttp ClientSession.")

    async def _close_session(self):
        """Close the aiohttp ClientSession if we own it."""
        if self.session and not self.session.closed and self._session_owner:
            await self.session.close()
            self.session = None
            self._session_owner = False
            self._log.debug("Closed owned aiohttp ClientSession.")
        elif self.session and not self.session.closed and not self._session_owner:
            self._log.debug("Skipping closure of externally provided ClientSession.")

    # --- Initialization ---
    async def initialize(self) -> bool:
        """
        Initialize API connection: create session, authenticate, get account info, start WebSocket.

        Returns:
            bool: True if initialization was successful, False otherwise.
        """
        self._is_shutting_down = False
        self._log.info("Initializing TradeLocker API...")
        try:
            await self._create_session()

            if not await self._authenticate():
                self._log.error("Initialization failed: Authentication error.")
                return False

            if not await self._get_account_info():
                self._log.error("Initialization failed: Could not retrieve account info.")
                # Account ID/Num are crucial for most /trade/ endpoints
                return False

            # Fetch initial trade config needed for parsing
            if not await self._get_trade_config():
                 self._log.error("Initialization failed: Could not retrieve trade configuration.")
                 return False

            # Fetch instruments list
            await self.get_instruments() # Populates self.instruments_cache

            # Start WebSocket connection management in the background
            # This task will run continuously, attempting to connect/reconnect
            asyncio.create_task(self._manage_websocket_connection())

            # Wait briefly for the initial WS connection attempt
            try:
                await asyncio.wait_for(self._ws_connected.wait(), timeout=15.0)
                self._log.info("WebSocket connected successfully during initialization.")
            except asyncio.TimeoutError:
                self._log.warning("WebSocket did not connect within the initial timeout period. Will keep trying in the background.")
                # Decide if initial WS connection is critical to proceed
                # return False

            self._log.info("TradeLocker API initialized successfully.")
            return True

        except Exception as e:
            self._log.exception(f"Failed to initialize TradeLocker API: {e}")
            await self.shutdown_async() # Clean up on failure
            return False

    # --- Authentication ---
    def _is_token_valid(self) -> bool:
        """Check if the current access token is likely valid."""
        return (
            self.access_token is not None and
            time.time() < (self.token_expires_at - TOKEN_REFRESH_BUFFER)
        )

    async def _authenticate(self) -> bool:
        """Authenticate or refresh token."""
        if self._is_token_valid():
            self._log.debug("Access token is still valid.")
            return True

        # Try refreshing first if possible
        if self.refresh_token and time.time() < self.token_expires_at + 3600*24*5: # Allow refresh for a few days
             self._log.info("Access token expired or nearing expiry, attempting refresh.")
             if await self._refresh_auth_token():
                 return True
             else:
                 self._log.warning("Token refresh failed, attempting full re-authentication.")
                 # Clear tokens after failed refresh to force full auth
                 self._clear_tokens()

        # Perform full authentication
        self._log.info("Performing full authentication.")
        url = f"{self.base_url}{PATH_AUTH_TOKEN}"
        payload = {
            "email": self._email,
            "password": self._password,
            "server": self._server
        }
        if not self._email or not self._password or not self._server:
             self._log.error("Missing email, password, or server for authentication.")
             return False

        try:
            status, response_data = await self._request("POST", url, json_data=payload, add_auth=False)

            if status in [200, 201] and isinstance(response_data, dict):
                access_token = response_data.get('accessToken')
                refresh_token = response_data.get('refreshToken')

                if access_token and refresh_token:
                    self.access_token = access_token
                    self.refresh_token = refresh_token
                    # TODO: Decode JWT to get precise expiry. Assume 1 hour for now.
                    self.token_expires_at = time.time() + 3600
                    self._log.info(f"Successfully authenticated (Status: {status}). Token expires around {datetime.datetime.fromtimestamp(self.token_expires_at)}")
                    return True
                else:
                    self._log.error(f"Authentication response OK (Status: {status}) but missing tokens. Body: {response_data}")
                    self._clear_tokens()
                    return False
            else:
                error_msg = response_data.get('message') if isinstance(response_data, dict) else str(response_data)
                self._log.error(f"Authentication request failed. Status: {status}, Response: {error_msg}")
                self._clear_tokens()
                return False

        except TradeLockerAPIError as e:
            self._log.error(f"Authentication API error: {e}")
            self._clear_tokens()
            return False
        except Exception as e:
            self._log.exception(f"Unexpected error during authentication: {e}")
            self._clear_tokens()
            return False

    def _clear_tokens(self):
        """Helper to clear authentication tokens and expiry."""
        self.access_token = None
        self.refresh_token = None
        self.token_expires_at = 0
        self._log.debug("Cleared authentication tokens.")

    async def _refresh_auth_token(self) -> bool:
        """Refresh authentication token using the refresh token."""
        if not self.refresh_token:
            self._log.error("Cannot refresh token: No refresh token available.")
            return False

        self._log.info("Attempting to refresh authentication token.")
        url = f"{self.base_url}{PATH_AUTH_REFRESH}"
        payload = {"refreshToken": self.refresh_token}

        try:
            status, response_data = await self._request("POST", url, json_data=payload, add_auth=False)

            if status == 200 and isinstance(response_data, dict):
                access_token = response_data.get('accessToken')
                new_refresh_token = response_data.get('refreshToken') # Check if it rotates

                if not access_token:
                    self._log.error("Token refresh failed: Missing access token in response.")
                    return False # Keep existing (expired) tokens for now

                self.access_token = access_token
                if new_refresh_token:
                    self.refresh_token = new_refresh_token
                    self._log.debug("Refresh token was also updated.")

                # TODO: Decode JWT for actual expiry. Assume 1 hour for now.
                self.token_expires_at = time.time() + 3600
                self._log.info(f"Successfully refreshed authentication token. New expiry around {datetime.datetime.fromtimestamp(self.token_expires_at)}")
                return True
            else:
                error_msg = response_data.get('message') if isinstance(response_data, dict) else str(response_data)
                self._log.error(f"Token refresh request failed. Status: {status}, Response: {error_msg}")
                # Don't clear tokens here, let the main auth logic handle retrying full auth
                return False

        except TradeLockerAPIError as e:
            self._log.error(f"Token refresh API error: {e}")
            return False
        except Exception as e:
            self._log.exception(f"Unexpected token refresh error: {e}")
            return False

    async def _ensure_authenticated(self):
        """Ensure the access token is valid, refreshing or re-authenticating if necessary."""
        if not self._is_token_valid():
            self._log.info("Authentication required or token expired.")
            if not await self._authenticate():
                raise TradeLockerAuthError("Failed to authenticate or refresh token.")

    # --- Account Info ---
    async def _get_account_info(self) -> bool:
        """
        Get account details using /auth/jwt/all-accounts and select the target account.
        Sets self.account_id and self.account_num.
        """
        await self._ensure_authenticated()
        url = f"{self.base_url}{PATH_AUTH_ACCOUNTS}"
        self._log.info(f"Fetching account list from: {url}")

        try:
            # --- CORRECTED CALL: Explicitly set include_acc_num=False ---
            status, response_data = await self._request(
                "GET",
                url,
                include_acc_num=False
            )
            
            if 200 <= status < 300 and isinstance(response_data, dict) and 'accounts' in response_data:
                accounts_list = response_data.get('accounts')
                if not isinstance(accounts_list, list):
                    self._log.error(f"API response for accounts is not a list. Body: {response_data}")
                    return False
                if not accounts_list:
                    self._log.error("No accounts found for this user.")
                    return False

                self._log.debug(f"Received accounts list: {accounts_list}")
                selected_account = None

                if self._target_account_id:
                    self._log.debug(f"Looking for specific account ID: {self._target_account_id}")
                    for account in accounts_list:
                        api_account_id = str(account.get('id', ''))
                        if api_account_id == self._target_account_id:
                            selected_account = account
                            break
                    if not selected_account:
                        self._log.warning(f"Specified account ID '{self._target_account_id}' not found. Falling back to the first account.")
                        selected_account = accounts_list[0]
                else:
                    self._log.info("No specific account ID configured, using the first available account.")
                    selected_account = accounts_list[0]

                if not selected_account: # Should not happen if accounts_list is not empty
                    self._log.error("Could not select an account.")
                    return False

                # Store as strings
                self.account_id = str(selected_account.get('id', ''))
                self.account_num = str(selected_account.get('accNum', ''))

                if not self.account_id or not self.account_num:
                    self._log.error(f"Selected account is missing required 'id' or 'accNum'. Account data: {selected_account}")
                    self.account_id = self.account_num = None
                    return False

                self._log.info(f"Successfully selected account: Name='{selected_account.get('name', 'N/A')}', ID='{self.account_id}', accNum='{self.account_num}'")
                return True
            else:
                self._log.error(f"Failed to get valid account information structure. Status: {status}, Response Body: {response_data}")
                return False

        except (TradeLockerAuthError, TradeLockerAPIError) as e:
            self._log.error(f"API error getting account information: {e}")
            return False
        except Exception as e:
            self._log.exception(f"Unexpected error getting account information: {e}")
            return False

# --- WebSocket Handling ---
    async def _manage_websocket_connection(self):
        """Manages the WebSocket connection lifecycle, including reconnections."""
        while not self._is_shutting_down:
            async with self._ws_reconnect_lock:
                if self.ws_connection and not self.ws_connection.closed:
                    await asyncio.sleep(WEBSOCKET_RECONNECT_DELAY * 2) # Check less frequently if connected
                    continue

                self._log.info("Attempting to establish WebSocket connection...")
                self._ws_connected.clear() # Signal disconnected state
                try: # START OF THE MAIN TRY BLOCK FOR CONNECTION
                    await self._ensure_authenticated()
                    if not self.access_token:
                         raise TradeLockerAuthError("Cannot connect WebSocket without access token.")

                    # --- DEBUGGING LINES PLACED CORRECTLY INSIDE TRY ---
                    self._log.debug(f"WebSocket URL BEFORE connect attempt: {self.ws_url} (Type: {type(self.ws_url)})")
                    if not isinstance(self.ws_url, str) or not (self.ws_url.startswith('ws://') or self.ws_url.startswith('wss://')):
                         self._log.critical(f"INVALID WebSocket URL detected before connect: {self.ws_url}. Aborting connection attempt.")
                         # Raise an error that will be caught by the outer except blocks
                         raise websockets.InvalidURI(f"Invalid scheme for WebSocket URL: {self.ws_url}")
                    # --- END DEBUGGING LINES ---

                    connect_url = f"{self.ws_url}?token={self.access_token}"

                    # Use connect() within a context manager for cleaner handling
                    async with websockets.connect(
                        connect_url,
                        ping_interval=WEBSOCKET_PING_INTERVAL,
                        ping_timeout=WEBSOCKET_PING_TIMEOUT,
                        close_timeout=WEBSOCKET_CLOSE_TIMEOUT,
                        user_agent_header=USER_AGENT
                    ) as ws:
                        self.ws_connection = ws
                        self._log.info(f"WebSocket connection established to {self.ws_url}")
                        self._ws_connected.set() # Signal connected state

                        # Cancel previous tasks if they exist (e.g., after reconnect)
                        if self._ws_listener_task and not self._ws_listener_task.done(): self._ws_listener_task.cancel()
                        if self._ws_heartbeat_task and not self._ws_heartbeat_task.done(): self._ws_heartbeat_task.cancel()

                        # Start listener and heartbeat tasks for the new connection
                        self._ws_listener_task = asyncio.create_task(self._websocket_listener())
                        self._ws_heartbeat_task = asyncio.create_task(self._websocket_heartbeat())

                        # Resubscribe to necessary channels
                        await self._resubscribe_ws_channels()

                        # Keep the connection alive by waiting on the listener task
                        await self._ws_listener_task

                # --- EXCEPTION HANDLING FOR THE CONNECTION ATTEMPT ---
                except (TradeLockerAuthError, websockets.InvalidURI, websockets.InvalidHandshake, websockets.SecurityError) as e:
                    self._log.error(f"WebSocket connection failed (unrecoverable auth/config error): {e}. Stopping connection attempts.")
                    self._is_shutting_down = True # Stop trying if auth/config is bad
                    break # Exit the while loop
                except (ConnectionClosedError, ConnectionClosedOK) as e:
                     self._log.warning(f"WebSocket connection closed ({type(e).__name__}). Code: {e.code}, Reason: {e.reason}. Attempting reconnect...")
                except WebSocketException as e:
                    self._log.warning(f"WebSocket exception occurred: {e}. Attempting reconnect...")
                except OSError as e:
                     self._log.warning(f"OS error during WebSocket connection (e.g., network issue): {e}. Attempting reconnect...")
                except asyncio.TimeoutError:
                     self._log.warning("WebSocket connection attempt timed out. Attempting reconnect...")
                except Exception as e:
                    self._log.exception(f"Unexpected WebSocket error in management loop: {e}. Attempting reconnect...")

                # --- FINALLY BLOCK FOR CLEANUP AFTER EACH ATTEMPT ---
                finally:
                    self._log.debug("WebSocket connection attempt finished or failed.")
                    self._ws_connected.clear() # Ensure disconnected state
                    self.ws_connection = None # Clear connection object
                    # Cancel tasks again in finally block to be safe
                    if self._ws_listener_task and not self._ws_listener_task.done(): self._ws_listener_task.cancel()
                    if self._ws_heartbeat_task and not self._ws_heartbeat_task.done(): self._ws_heartbeat_task.cancel()
                    # Wait before retrying connection
                    if not self._is_shutting_down:
                         await asyncio.sleep(WEBSOCKET_RECONNECT_DELAY)

        self._log.info("WebSocket management loop stopped.")
        self._ws_connected.clear() # Final signal disconnected

    async def _websocket_heartbeat(self):
        """Send periodic heartbeats (pings) to keep WebSocket connection alive."""
        try:
            while self.ws_connection and not self.ws_connection.closed:
                try:
                    await self.ws_connection.ping()
                    self._log.debug("Sent WebSocket PING frame")
                except WebSocketException as e:
                     self._log.warning(f"Failed to send WebSocket ping: {e}. Connection might be closing.")
                     break # Exit heartbeat loop if ping fails
                await asyncio.sleep(HEARTBEAT_INTERVAL)
        except asyncio.CancelledError:
            self._log.info("WebSocket heartbeat task cancelled.")
        except Exception as e:
            self._log.exception(f"Unexpected error in WebSocket heartbeat task: {e}")
        finally:
             self._log.debug("WebSocket heartbeat task finished.")

    async def _websocket_listener(self):
        """Listen for WebSocket messages and dispatch them to callbacks."""
        if not self.ws_connection or self.ws_connection.closed:
            self._log.error("WebSocket listener started without an active connection.")
            return

        self._log.info("WebSocket listener started.")
        try:
            async for message in self.ws_connection:
                try:
                    if isinstance(message, bytes): message = message.decode('utf-8')
                    if not isinstance(message, str):
                         self._log.warning(f"Received unexpected message type: {type(message)}")
                         continue

                    data = json.loads(message)
                    # self._log.debug(f"WS RCV: {data}") # Very verbose

                    msg_type = data.get('type')
                    channel = data.get('channel')

                    if msg_type == 'pong': continue # Ignore our own pongs if server echoes them
                    if msg_type == 'ping': # Respond to server ping
                        await self._ws_send_json({'type': 'pong'})
                        self._log.debug("Responded to server WebSocket PING message")
                        continue
                    if msg_type == 'error':
                        error_code = data.get('code', 'unknown')
                        error_message = data.get('message', 'Unknown WebSocket error')
                        self._log.error(f"WebSocket error received - Code: {error_code}, Message: {error_message}, Data: {data}")
                        if error_code in ['token_expired', 'invalid_token', 'unauthorized']:
                            self._log.warning("WebSocket reported auth error, triggering reconnect.")
                            if self.ws_connection and not self.ws_connection.closed:
                                await self.ws_connection.close(code=1008, reason="Auth Error")
                        continue

                    if channel and channel in self._ws_callback_map:
                         callbacks = self._ws_callback_map[channel]
                         # self._log.debug(f"Dispatching message for channel '{channel}' to {len(callbacks)} callbacks.")
                         for callback in callbacks:
                              try:
                                   callback(data) # Callbacks should be sync or schedule tasks
                              except Exception as cb_err:
                                   self._log.exception(f"Error in WebSocket callback for channel '{channel}': {cb_err}")
                    # else: # Log unhandled channels only if needed
                    #      if channel: self._log.debug(f"Received message for unhandled channel: {channel}")
                    #      elif msg_type: self._log.debug(f"Received message with type '{msg_type}' but no channel: {data}")
                    #      else: self._log.warning(f"Received WebSocket message with no identifiable type or channel: {data}")

                except json.JSONDecodeError:
                    self._log.error(f"Failed to decode WebSocket JSON message: {message}")
                except Exception as e:
                    self._log.exception(f"Error processing WebSocket message: {e}")

        except asyncio.CancelledError:
            self._log.info("WebSocket listener task cancelled.")
        except (ConnectionClosedError, ConnectionClosedOK) as e:
            self._log.info(f"WebSocket connection closed normally. Code: {e.code}, Reason: {e.reason}")
        except WebSocketException as e:
            self._log.warning(f"WebSocket listener error: {e}")
        except Exception as e:
            self._log.exception(f"Unexpected error in WebSocket listener: {e}")
        finally:
            self._log.info("WebSocket listener stopped.")
            self._ws_connected.clear() # Ensure disconnected state is signaled

    async def _ws_send_json(self, message: Dict):
        """Send a JSON message over the WebSocket connection."""
        if self.ws_connection and not self.ws_connection.closed:
            try:
                await self.ws_connection.send(json.dumps(message))
                # self._log.debug(f"WS SENT: {message}") # Verbose
            except WebSocketException as e:
                self._log.warning(f"Failed to send WebSocket message: {e}. Connection may be closing.")
                raise TradeLockerWebSocketError(f"Failed to send WS message: {e}") from e
            except Exception as e:
                 self._log.exception(f"Unexpected error sending WebSocket message: {e}")
                 raise TradeLockerWebSocketError(f"Unexpected error sending WS message: {e}") from e
        else:
            # self._log.warning(f"Cannot send WebSocket message, connection not available: {message}")
            raise TradeLockerWebSocketError("WebSocket connection not available.")

    # --- WebSocket Subscription Management (Improved) ---
    def subscribe(self, channel: str, callback: Callable[[Dict], Any], **params):
        """
        Subscribe to a WebSocket channel. Stores parameters for resubscription.

        Args:
            channel: The channel name (e.g., 'quotes', 'account', 'orders').
            callback: The function to call when a message for this channel is received.
                      This callback should be synchronous or schedule its own async tasks.
            **params: Additional parameters for the subscription message (e.g., tradableInstrumentId, routeId).
        """
        param_key = "_".join(map(str, params.values())) if params else "global"
        subscription_key = f"{channel}_{param_key}"

        if subscription_key in self._ws_subscriptions:
            self._log.warning(f"Already subscribed with key: {subscription_key}. Adding callback anyway.")
            if channel not in self._ws_callback_map: self._ws_callback_map[channel] = []
            if callback not in self._ws_callback_map[channel]: self._ws_callback_map[channel].append(callback)
            return

        self._ws_subscriptions[subscription_key] = {'channel': channel, 'callback': callback, 'params': params}
        if channel not in self._ws_callback_map: self._ws_callback_map[channel] = []
        if callback not in self._ws_callback_map[channel]: self._ws_callback_map[channel].append(callback)
        self._log.info(f"Registered subscription: Key='{subscription_key}', Channel='{channel}', Params={params}")

        subscription_message = {'type': 'subscribe', 'channel': channel, **params}
        async def send_sub():
             try:
                  # Wait up to 5s for connection before sending initial sub
                  await asyncio.wait_for(self._ws_connected.wait(), timeout=5.0)
                  await self._ws_send_json(subscription_message)
                  self._log.info(f"Sent subscribe message for: {subscription_key}")
             except asyncio.TimeoutError:
                  self._log.warning(f"WS not connected, subscribe for {subscription_key} will be sent on reconnect.")
             except TradeLockerWebSocketError as e:
                  self._log.error(f"Error sending subscribe message for {subscription_key}: {e}")
             except Exception as e:
                  self._log.exception(f"Unexpected error sending subscribe for {subscription_key}: {e}")
        asyncio.create_task(send_sub())


    def unsubscribe(self, channel: str, callback: Callable[[Dict], Any], **params):
        """Unsubscribe a specific callback and potentially the channel subscription."""
        param_key = "_".join(map(str, params.values())) if params else "global"
        subscription_key = f"{channel}_{param_key}"

        removed_from_map = False
        if channel in self._ws_callback_map and callback in self._ws_callback_map[channel]:
            self._ws_callback_map[channel].remove(callback)
            self._log.info(f"Removed callback from channel map: {channel}")
            removed_from_map = True
            if not self._ws_callback_map[channel]:
                del self._ws_callback_map[channel]
                self._log.debug(f"Removed channel '{channel}' from callback map as it's empty.")

        if subscription_key in self._ws_subscriptions:
            del self._ws_subscriptions[subscription_key]
            self._log.info(f"Removed subscription instance: {subscription_key}")

            # Simple: Send unsubscribe if we removed the key
            unsubscription_message = {'type': 'unsubscribe', 'channel': channel, **params}
            async def send_unsub():
                 try:
                      if await self._ws_connected.wait(): # Check if connected
                           await self._ws_send_json(unsubscription_message)
                           self._log.info(f"Sent unsubscribe message for: {subscription_key}")
                      # else: # Don't worry about sending unsub if not connected
                 except TradeLockerWebSocketError as e:
                      self._log.error(f"Error sending unsubscribe message for {subscription_key}: {e}")
                 except Exception as e:
                      self._log.exception(f"Unexpected error sending unsubscribe for {subscription_key}: {e}")
            asyncio.create_task(send_unsub())
        elif removed_from_map:
             self._log.warning(f"Removed callback for {channel}, but subscription key {subscription_key} not found.")
        else:
             self._log.warning(f"Callback or subscription key {subscription_key} not found during unsubscribe.")


    async def _resubscribe_ws_channels(self):
        """Resend subscription messages for all registered subscriptions after a reconnect."""
        self._log.info(f"Resubscribing to {len(self._ws_subscriptions)} WebSocket channels...")
        if not self.ws_connection or self.ws_connection.closed:
             self._log.error("Cannot resubscribe, WebSocket connection is not active.")
             return

        success_count = 0
        for key, sub_info in self._ws_subscriptions.items():
            message = {'type': 'subscribe', 'channel': sub_info['channel'], **sub_info['params']}
            try:
                await self._ws_send_json(message)
                self._log.debug(f"Resent subscription for: {key}")
                success_count += 1
                await asyncio.sleep(0.05) # Small delay between subscriptions
            except TradeLockerWebSocketError as e:
                 self._log.error(f"Failed to resend subscription for {key}: {e}")
            except Exception as e:
                 self._log.exception(f"Unexpected error resending subscription for {key}: {e}")

        self._log.info(f"Finished resubscribing. Sent {success_count}/{len(self._ws_subscriptions)} messages.")


    # --- REST API Request Helper ---
    def _get_auth_headers(self, include_acc_num: bool = True) -> Dict[str, str]:
        """Construct standard authenticated request headers."""
        if not self.access_token: raise TradeLockerAuthError("Access token is missing.")
        headers = {"Authorization": f"Bearer {self.access_token}", "Accept": "application/json", "User-Agent": USER_AGENT}
        if include_acc_num:
             if not self.account_num: raise TradeLockerAuthError("Account Number (accNum) is missing.")
             headers["accNum"] = self.account_num
        return headers

    def _update_rate_limits(self, headers: Mapping[str, str]):
        """Update rate limit info from response headers."""
        try:
            if 'X-RateLimit-Remaining' in headers: self.rate_limit_remaining = int(headers['X-RateLimit-Remaining'])
            if 'X-RateLimit-Reset' in headers: self.rate_limit_reset = int(headers['X-RateLimit-Reset'])
            if self.rate_limit_remaining < 50:
                reset_dt = datetime.datetime.fromtimestamp(self.rate_limit_reset, tz=datetime.timezone.utc) if self.rate_limit_reset else "N/A"
                self._log.warning(f"API rate limit low: {self.rate_limit_remaining} remaining. Reset at {reset_dt}")
        except (ValueError, TypeError) as e:
            self._log.warning(f"Could not parse rate limit headers: {e}. Headers: {headers}")

    async def _request(
        self, method: str, url: str, params: Optional[Dict] = None, json_data: Optional[Dict] = None,
        data: Optional[Any] = None, add_auth: bool = True, include_acc_num: bool = True
    ) -> Tuple[int, Optional[Union[Dict, List, str]]]:
        """Makes an asynchronous HTTP request. Returns status code and parsed body."""
        await self._create_session()
        headers = {"User-Agent": USER_AGENT}
        if add_auth:
            try:
                await self._ensure_authenticated()
                auth_headers = self._get_auth_headers(include_acc_num=include_acc_num)
                headers.update(auth_headers)
            except TradeLockerAuthError as e:
                 self._log.error(f"Authentication failed for request {method} {url}: {e}")
                 raise

        if json_data is not None and method in ["POST", "PUT", "PATCH"]:
             headers["Content-Type"] = "application/json"
             headers["Accept"] = "application/json"

        self._log.debug(f"Request: {method} {url} Params: {params} JSONData: {json_data is not None} Auth: {add_auth}")
        response_body: Optional[Union[Dict, List, str]] = None
        status_code: int = 0

        try:
            async with self.session.request(method, url, params=params, json=json_data, data=data, headers=headers) as response:
                self._update_rate_limits(response.headers)
                status_code = response.status
                try:
                    if response.content_type == 'application/json': response_body = await response.json()
                    else: response_body = await response.text()
                except (json.JSONDecodeError, aiohttp.ContentTypeError) as json_err:
                    response_text = await response.text()
                    self._log.error(f"Failed to decode JSON response. Status: {status_code}, Error: {json_err}, Body: {response_text[:500]}")
                    response_body = response_text
                except Exception as read_err:
                     self._log.error(f"Error reading response body. Status: {status_code}, Error: {read_err}")
                     response_body = f"Error reading response body: {read_err}"

                # self._log.debug(f"Response: Status={status_code}, Type={type(response_body).__name__}, Body Snippet: {str(response_body)[:200]}")

                if 200 <= status_code < 300: return status_code, response_body
                elif status_code in [401, 403]:
                     self._log.error(f"Authorization error ({status_code}) for {method} {url}. Response: {response_body}")
                     if status_code == 401: self._clear_tokens()
                     raise TradeLockerAuthError(f"Authorization failed ({status_code})", status_code, response_body)
                elif status_code == 429:
                     self._log.error(f"Rate limit exceeded ({status_code}) for {method} {url}. Response: {response_body}")
                     raise TradeLockerAPIError("Rate limit exceeded", status_code, response_body)
                else:
                     self._log.error(f"API request failed ({status_code}) for {method} {url}. Response: {response_body}")
                     raise TradeLockerAPIError(f"API request failed with status {status_code}", status_code, response_body)

        except aiohttp.ClientConnectionError as e: raise TradeLockerAPIError(f"Connection error: {e}") from e
        except asyncio.TimeoutError as e: raise TradeLockerAPIError(f"Request timeout: {e}") from e
        # Auth errors are re-raised above
        except Exception as e: raise TradeLockerAPIError(f"Unexpected API request error: {e}") from e


    # --- Data Fetching Methods ---

    async def get_instruments(self) -> List[Dict]:
        """Get available instruments for the account."""
        if not self.account_id: raise TradeLockerError("Account ID not set.")
        url = f"{self.base_url}{PATH_TRADE_INSTRUMENTS.format(account_id=self.account_id)}"
        try:
            status, data = await self._request("GET", url) # accNum header included by default
            if status == 200 and isinstance(data, dict):
                 instruments = data.get('instruments', [])
                 self.instruments_cache.clear()
                 for instrument in instruments:
                     tradable_id = instrument.get('tradableInstrumentId')
                     if tradable_id:
                         self.instruments_cache[str(tradable_id)] = instrument
                 self._log.info(f"Fetched and cached {len(instruments)} instruments.")
                 return instruments
            else:
                 self._log.error(f"Failed to get instruments. Status: {status}, Response: {data}")
                 return []
        except (TradeLockerAuthError, TradeLockerAPIError) as e:
            self._log.error(f"Failed to get instruments: {e}")
            return []
        except Exception as e:
            self._log.exception(f"Unexpected error getting instruments: {e}")
            return []

    async def _get_trade_config(self) -> Optional[Dict]:
        """Get and cache the TradeLocker /trade/config response."""
        if self._trade_config_cache: return self._trade_config_cache
        url = f"{self.base_url}{PATH_TRADE_CONFIG}"
        self._log.info("Fetching trade configuration...")
        try:
            # Assume /trade/config might also need accNum
            status, data = await self._request("GET", url, include_acc_num=True)
            if 200 <= status < 300 and isinstance(data, dict):
                 # Check structure - might be direct dict or {'s':'ok', 'd':{...}}
                 if 'accountDetailsConfig' in data and 'positionsConfig' in data: # Check for key fields
                      self._trade_config_cache = data
                 elif data.get('s') == 'ok' and isinstance(data.get('d'), dict):
                      self._trade_config_cache = data['d']
                 else:
                      self._log.error(f"Trade configuration response has unexpected structure. Status: {status}, Body: {data}")
                      return None

                 self._log.info("Fetched and cached trade configuration successfully.")
                 self._log.debug(f"Trade config cache keys: {list(self._trade_config_cache.keys())}")
                 return self._trade_config_cache
            else:
                self._log.error(f"Failed to get valid trade configuration. Status: {status}, Response: {data}")
                return None
        except (TradeLockerAuthError, TradeLockerAPIError) as e:
            self._log.error(f"API error getting trade configuration: {e}")
            return None
        except Exception as e:
            self._log.exception(f"Unexpected error getting trade configuration: {e}")
            return None

    async def get_account(self) -> Optional[Account]:
        """Get detailed account information including state (balance, P/L, margin)."""
        if not self.account_id:
            self._log.error("Cannot get account details: Account ID not set.")
            return None
        self._log.debug(f"Fetching detailed account state for Account ID: {self.account_id}")

        # --- 1. Get Base Account Info (Name, Currency) ---
        base_account_data = {}
        url_accounts = f"{self.base_url}{PATH_TRADE_ACCOUNTS}"
        try:
            status_base, accounts_list_response = await self._request("GET", url_accounts, include_acc_num=True)
            accounts_list = None
            if 200 <= status_base < 300:
                 if isinstance(accounts_list_response, list): accounts_list = accounts_list_response
                 elif isinstance(accounts_list_response, dict) and accounts_list_response.get('s') == 'ok' and isinstance(accounts_list_response.get('d'), list):
                      accounts_list = accounts_list_response.get('d')

            if accounts_list:
                for acc in accounts_list:
                    if str(acc.get('id')) == self.account_id:
                        base_account_data = acc
                        break
                if not base_account_data: self._log.warning(f"Account ID {self.account_id} not found in {url_accounts} response.")
            else: self._log.warning(f"Failed to get valid account list from {url_accounts}. Status: {status_base}.")
        except Exception as e: self._log.error(f"Error getting base account info from {url_accounts}: {e}")

        # --- 2. Get Account State (Balance, Margin, P/L) ---
        url_state = f"{self.base_url}{PATH_TRADE_ACCOUNT_STATE.format(account_id=self.account_id)}"
        try:
            status_state, state_data = await self._request("GET", url_state, include_acc_num=True)
            actual_state_payload = None
            if 200 <= status_state < 300:
                 if isinstance(state_data, dict) and state_data.get('s') == 'ok' and 'd' in state_data: actual_state_payload = state_data.get('d')
                 elif isinstance(state_data, dict): actual_state_payload = state_data # Direct response?
                 else: self._log.error(f"Account state response format invalid. Status: {status_state}, Body: {state_data}"); return None
            else: self._log.error(f"Failed to get account state from {url_state}. Status: {status_state}, Response: {state_data}"); return None

            # --- 3. Get Config for Parsing State ---
            config_trade = await self._get_trade_config()
            if not config_trade: self._log.error("Cannot parse account state: Failed to get trade configuration."); return None
            account_details_config = config_trade.get('accountDetailsConfig', {})
            columns = account_details_config.get('columns', [])
            if not columns: self._log.error("Cannot parse account state: 'columns' missing in trade config."); return None
            field_indices = {col.get('id'): i for i, col in enumerate(columns) if col.get('id')}

            # --- Extract State Values ---
            account_details_values = actual_state_payload.get('accountDetailsData', [])
            if not isinstance(account_details_values, list): account_details_values = []

            def get_value(field_id, default=0.0):
                idx = field_indices.get(field_id)
                if idx is not None and idx < len(account_details_values):
                    raw_value = account_details_values[idx]
                    try: return float(raw_value) if raw_value is not None else default
                    except (ValueError, TypeError): return default
                return default

            # --- 4. Create Account object ---
            balance = get_value('balance')
            equity = get_value('equity', balance + get_value('unrealizedPnl')) # Calculate if not present

            account = Account(
                id=self.account_id,
                name=base_account_data.get('name', f"Account_{self.account_id}"),
                account_type=base_account_data.get('type', 'Unknown'),
                base_currency=base_account_data.get('currency', 'USD'),
                balance=balance,
                equity=equity,
                initial_balance=get_value('initialBalance', balance), # Approx if not available
                day_pl=get_value('dayPnl'),
                overall_pl=get_value('overallPnl'),
                margin_used=get_value('marginUsed'),
                margin_available=get_value('marginAvailable'),
                margin_level_percent=get_value('marginLevel') # Check field name
            )
            self._log.info(f"Account details updated for {account.id}. Balance: {account.balance:.2f}, Equity: {account.equity:.2f}")
            return account

        except (TradeLockerAuthError, TradeLockerAPIError) as e: self._log.error(f"API error getting account state: {e}"); return None
        except Exception as e: self._log.exception(f"Unexpected error getting account details: {e}"); return None


    async def get_positions(self) -> List[Position]:
        """Get current open positions."""
        if not self.account_id: raise TradeLockerError("Account ID not set.")
        url = f"{self.base_url}{PATH_TRADE_POSITIONS.format(account_id=self.account_id)}"
        positions = []
        try:
            status, data = await self._request("GET", url) # accNum header included by default
            if status != 200 or not isinstance(data, dict):
                self._log.error(f"Failed to get positions. Status: {status}, Response: {data}")
                return []

            config_trade = await self._get_trade_config()
            if not config_trade: self._log.error("Cannot parse positions: Failed to get trade config."); return []
            positions_config = config_trade.get('positionsConfig', {})
            columns = positions_config.get('columns', [])
            field_indices = {col.get('id'): i for i, col in enumerate(columns) if col.get('id')}

            positions_data = data.get('positions', [])
            if not isinstance(positions_data, list): positions_data = []

            for pos_values in positions_data:
                 if not isinstance(pos_values, list): continue

                 def get_value(field_id, default=None):
                    idx = field_indices.get(field_id)
                    return pos_values[idx] if idx is not None and idx < len(pos_values) else default

                 qty = get_value('qty', 0.0)
                 if not qty or float(qty) == 0: continue # Skip zero quantity

                 instrument_symbol = get_value('symbol', "Unknown")
                 instrument_id = get_value('tradableInstrumentId', "Unknown")
                 side = str(get_value('side', 'buy')).lower()
                 entry_price = get_value('entryPrice', 0.0)
                 unrealized_pl = get_value('unrealizedPnl', 0.0)
                 position_id = get_value('id') # Crucial ID
                 stop_loss = get_value('stopLoss')
                 take_profit = get_value('takeProfit')
                 # Timestamps might be 'created' or 'updated' in ms
                 entry_time_ms = get_value('created', time.time() * 1000)
                 entry_time = datetime.datetime.fromtimestamp(entry_time_ms / 1000.0, tz=datetime.timezone.utc)

                 positions.append(Position(
                     id=str(position_id) if position_id else None,
                     instrument=instrument_symbol,
                     direction="long" if side == "buy" else "short",
                     quantity=abs(float(qty)),
                     entry_price=float(entry_price),
                     entry_time=entry_time,
                     unrealized_pl=float(unrealized_pl),
                     stop_loss=float(stop_loss) if stop_loss is not None else None,
                     take_profit=float(take_profit) if take_profit is not None else None,
                     strategy_type=get_value('strategyId') # Example field
                 ))
            self._log.info(f"Fetched {len(positions)} open positions.")
            return positions

        except (TradeLockerAuthError, TradeLockerAPIError) as e: self._log.error(f"Failed to get positions: {e}"); return []
        except Exception as e: self._log.exception(f"Unexpected error getting positions: {e}"); return []


    async def get_historical_data(
        self, instrument_id: Union[int, str], route_id: Union[int, str],
        timeframe: str = "1m", count: Optional[int] = None,
        from_time_ms: Optional[int] = None, to_time_ms: Optional[int] = None
    ) -> List[PriceBar]:
        """Get historical OHLCV data."""
        resolution = self._normalize_timeframe_for_api(timeframe)
        now_ms = int(time.time() * 1000)
        to_time_ms = to_time_ms or now_ms

        if count is not None and count > 0:
             seconds_per_bar = self._timeframe_to_seconds(timeframe)
             if seconds_per_bar > 0: from_time_ms = to_time_ms - (count * seconds_per_bar * 1000)
             else: from_time_ms = to_time_ms - (100 * 60 * 1000) # Default 100 mins
        elif from_time_ms is None:
             from_time_ms = to_time_ms - (100 * 60 * 1000) # Default 100 mins

        url = f"{self.base_url}{PATH_TRADE_HISTORY}"
        params = {
            "tradableInstrumentId": str(instrument_id), "routeId": str(route_id),
            "from": from_time_ms, "to": to_time_ms, "resolution": resolution
        }
        bars = []
        try:
            status, data = await self._request("GET", url)
            if status == 200 and isinstance(data, dict):
                bar_details = data.get('barDetails', [])
                if not isinstance(bar_details, list): bar_details = []

                instrument_str = self.get_symbol_from_id(str(instrument_id)) or str(instrument_id)
                for bar_data in bar_details:
                     ts_ms = bar_data.get('t')
                     if ts_ms is None: continue
                     timestamp = datetime.datetime.fromtimestamp(ts_ms / 1000.0, tz=datetime.timezone.utc)
                     bars.append(PriceBar(
                         instrument=instrument_str, timestamp=timestamp,
                         open=float(bar_data.get('o', 0.0)), high=float(bar_data.get('h', 0.0)),
                         low=float(bar_data.get('l', 0.0)), close=float(bar_data.get('c', 0.0)),
                         volume=float(bar_data.get('v', 0.0))
                     ))
                self._log.info(f"Fetched {len(bars)} historical bars for {instrument_id} ({timeframe}).")
                if count is not None and count > 0 and len(bars) > count: bars = bars[-count:]
                return bars
            else: self._log.error(f"Failed to get historical data. Status: {status}, Response: {data}"); return []
        except (TradeLockerAuthError, TradeLockerAPIError) as e: self._log.error(f"Failed to get historical data: {e}"); return []
        except Exception as e: self._log.exception(f"Unexpected error getting historical data: {e}"); return []

    def _timeframe_to_seconds(self, timeframe: str) -> int:
        """Convert common timeframe string to seconds (approximate)."""
        tf = str(timeframe).lower(); num = 1
        num_part = ''.join(filter(str.isdigit, tf)); unit_part = ''.join(filter(str.isalpha, tf))
        if num_part: num = int(num_part)
        if not unit_part and num_part: unit_part = 'm'
        if 'm' in unit_part: return num * 60
        if 'h' in unit_part: return num * 3600
        if 'd' in unit_part: return num * 86400
        if 'w' in unit_part: return num * 604800
        return 0

    def _normalize_timeframe_for_api(self, timeframe: str) -> str:
        """Normalize timeframe to common TradeLocker API formats (e.g., '1', '60', 'D')."""
        tf = str(timeframe).lower(); num = 1
        num_part = ''.join(filter(str.isdigit, tf)); unit_part = ''.join(filter(str.isalpha, tf))
        if num_part: num = int(num_part)
        if not unit_part and num_part: return str(num) # Assume minutes
        if 'm' in unit_part: return str(num)
        if 'h' in unit_part: return str(num * 60)
        if 'd' in unit_part: return "D"
        if 'w' in unit_part: return "W"
        self._log.warning(f"Could not normalize timeframe '{timeframe}', using it directly.")
        return timeframe

    def get_symbol_from_id(self, instrument_id: str) -> Optional[str]:
         """Lookup instrument symbol from cached instrument ID."""
         instrument_info = self.instruments_cache.get(str(instrument_id))
         return instrument_info.get('symbol') if instrument_info else None

    async def get_current_price(self, instrument_id: Union[int, str], route_id: Union[int, str]) -> Optional[PriceSnapshot]:
        """Get current quote (bid/ask) for an instrument."""
        url = f"{self.base_url}{PATH_TRADE_QUOTES}"
        params = {"tradableInstrumentId": str(instrument_id), "routeId": str(route_id)}
        instrument_symbol = self.get_symbol_from_id(str(instrument_id)) or str(instrument_id)
        try:
            status, data = await self._request("GET", url)
            if status == 200 and isinstance(data, dict):
                 bid = data.get('bp', data.get('bid', 0.0))
                 ask = data.get('ap', data.get('ask', 0.0))
                 last = data.get('lp', data.get('last', (bid + ask) / 2.0 if bid and ask else 0.0))
                 timestamp_ms = data.get('t', data.get('timestamp', time.time() * 1000))
                 timestamp = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=datetime.timezone.utc)
                 snapshot = PriceSnapshot(
                     instrument=instrument_symbol, timestamp=timestamp, bid=float(bid), ask=float(ask),
                     last_price=float(last), volume=float(data.get('v', 0.0))
                 )
                 self.market_data_cache[instrument_symbol] = snapshot # Cache by symbol
                 # self._log.debug(f"Fetched current price for {instrument_symbol}: Bid={bid}, Ask={ask}")
                 return snapshot
            else:
                self._log.error(f"Failed to get current price for {instrument_symbol}. Status: {status}, Response: {data}")
                return self.market_data_cache.get(instrument_symbol) # Return cached on failure
        except (TradeLockerAuthError, TradeLockerAPIError) as e: self._log.error(f"Failed to get current price for {instrument_symbol}: {e}"); return self.market_data_cache.get(instrument_symbol)
        except Exception as e: self._log.exception(f"Unexpected error getting current price for {instrument_symbol}: {e}"); return self.market_data_cache.get(instrument_symbol)


    # --- Order Execution Methods ---

    async def _place_order(self, order_details: Dict) -> Optional[Dict]:
        """Helper function to place any type of order."""
        if not self.account_id: raise TradeLockerError("Account ID not set.")
        url = f"{self.base_url}{PATH_TRADE_ORDERS.format(account_id=self.account_id)}"

        required = ['tradableInstrumentId', 'routeId', 'side', 'qty', 'type']
        if not all(k in order_details for k in required):
             missing = [k for k in required if k not in order_details]
             self._log.error(f"Cannot place order: Missing required fields: {missing}. Details: {order_details}")
             return None
        order_details.setdefault('accountId', self.account_id) # Ensure accountId is present

        try:
            status, data = await self._request("POST", url, json_data=order_details) # accNum header included by default

            if status in [200, 201] and isinstance(data, dict):
                 # Response structure might be {'s':'ok', 'd': {'orderId': ...}} or direct
                 order_data = data.get('d', data) # Get 'd' part or use whole dict
                 order_id = order_data.get('orderId') if isinstance(order_data, dict) else None

                 if order_id:
                     symbol = self.get_symbol_from_id(order_details['tradableInstrumentId']) or order_details['tradableInstrumentId']
                     self._log.info(f"Order placed successfully. Type: {order_details['type']}, Side: {order_details['side']}, Qty: {order_details['qty']}, Symbol: {symbol}. OrderID: {order_id}")
                     return {"orderId": order_id, "status_code": status, "response_data": data}
                 else:
                     self._log.error(f"Order placement request accepted (Status: {status}) but no orderId found. Response: {data}")
                     return None # Treat as failure if no ID
            else:
                 # Error handled by _request, just return None
                 return None
        except (TradeLockerAuthError, TradeLockerAPIError) as e: self._log.error(f"Failed to place order: {e}"); return None
        except Exception as e: self._log.exception(f"Unexpected error placing order: {e}"); return None

    async def place_market_order(
        self, instrument_id: Union[int, str], route_id: Union[int, str], direction: str, quantity: float,
        stop_loss: Optional[float] = None, take_profit: Optional[float] = None,
        tag: Optional[str] = None, position_id: Optional[str] = None
    ) -> Optional[Dict]:
        """Place a market order."""
        order = {
            "tradableInstrumentId": str(instrument_id), "routeId": str(route_id),
            "side": direction.lower(), "qty": float(quantity), "type": "market",
            "validity": "IOC", # Or FOK depending on broker/preference
        }
        if stop_loss is not None: order["stopLoss"] = float(stop_loss); order["stopLossType"] = "absolute"
        if take_profit is not None: order["takeProfit"] = float(take_profit); order["takeProfitType"] = "absolute"
        if tag: order["tag"] = str(tag) # Optional tag/comment
        if position_id: order["positionId"] = str(position_id) # Link to existing position (e.g., for closing)
        return await self._place_order(order)

    async def place_limit_order(
        self, instrument_id: Union[int, str], route_id: Union[int, str], direction: str, quantity: float, price: float,
        stop_loss: Optional[float] = None, take_profit: Optional[float] = None, tag: Optional[str] = None
    ) -> Optional[Dict]:
        """Place a limit order."""
        order = {
            "tradableInstrumentId": str(instrument_id), "routeId": str(route_id),
            "side": direction.lower(), "qty": float(quantity), "type": "limit",
            "validity": "GTC", "price": float(price)
        }
        if stop_loss is not None: order["stopLoss"] = float(stop_loss); order["stopLossType"] = "absolute"
        if take_profit is not None: order["takeProfit"] = float(take_profit); order["takeProfitType"] = "absolute"
        if tag: order["tag"] = str(tag)
        return await self._place_order(order)

    async def place_stop_order(
        self, instrument_id: Union[int, str], route_id: Union[int, str], direction: str, quantity: float, stop_price: float,
        stop_loss: Optional[float] = None, take_profit: Optional[float] = None, tag: Optional[str] = None
    ) -> Optional[Dict]:
        """Place a stop market order."""
        order = {
            "tradableInstrumentId": str(instrument_id), "routeId": str(route_id),
            "side": direction.lower(), "qty": float(quantity), "type": "stop",
            "validity": "GTC", "stopPrice": float(stop_price)
        }
        if stop_loss is not None: order["stopLoss"] = float(stop_loss); order["stopLossType"] = "absolute"
        if take_profit is not None: order["takeProfit"] = float(take_profit); order["takeProfitType"] = "absolute"
        if tag: order["tag"] = str(tag)
        return await self._place_order(order)

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order."""
        if not order_id: self._log.error("Cannot cancel order: Order ID is required."); return False
        url = f"{self.base_url}{PATH_TRADE_ORDER_BY_ID.format(order_id=order_id)}"
        try:
            status, data = await self._request("DELETE", url)
            # Success can be 200 with {'s':'ok'} or 204 No Content
            if status in [200, 204]:
                 self._log.info(f"Order {order_id} cancelled successfully (Status {status}).")
                 return True
            else:
                 # Error handled by _request
                 return False
        except (TradeLockerAuthError, TradeLockerAPIError) as e: self._log.error(f"Failed to cancel order {order_id}: {e}"); return False
        except Exception as e: self._log.exception(f"Unexpected error cancelling order {order_id}: {e}"); return False

    async def modify_order(self, order_id: str, modifications: Dict[str, Any]) -> bool:
        """Modify a pending order (e.g., price, SL, TP)."""
        if not order_id: self._log.error("Cannot modify order: Order ID required."); return False
        if not modifications: self._log.warning("No modifications provided for order {order_id}."); return False
        url = f"{self.base_url}{PATH_TRADE_ORDER_BY_ID.format(order_id=order_id)}"
        try:
            # Use PATCH or PUT depending on API design (PATCH is common for partial updates)
            status, data = await self._request("PATCH", url, json_data=modifications)
            if 200 <= status < 300:
                 self._log.info(f"Order {order_id} modified successfully. Modifications: {modifications}")
                 return True
            else: return False # Error handled by _request
        except (TradeLockerAuthError, TradeLockerAPIError) as e: self._log.error(f"Failed to modify order {order_id}: {e}"); return False
        except Exception as e: self._log.exception(f"Unexpected error modifying order {order_id}: {e}"); return False

    async def close_position(self, position_id: str, quantity: Optional[float] = None) -> bool:
        """Close or partially close a position using its ID."""
        if not position_id: self._log.error("Cannot close position: Position ID required."); return False
        url = f"{self.base_url}{PATH_TRADE_POSITION_BY_ID.format(position_id=position_id)}"
        payload = {}
        if quantity is not None: payload["qty"] = float(quantity) # API expects quantity for partial close

        try:
            status, data = await self._request("DELETE", url, json_data=payload if payload else None)
            if status in [200, 204]:
                 close_type = f"partially ({quantity} lots)" if quantity else "fully"
                 self._log.info(f"Position {position_id} {close_type} closed successfully (Status {status}).")
                 return True
            else: return False # Error handled by _request
        except (TradeLockerAuthError, TradeLockerAPIError) as e: self._log.error(f"Failed to close position {position_id}: {e}"); return False
        except Exception as e: self._log.exception(f"Unexpected error closing position {position_id}: {e}"); return False

    async def modify_position(self, position_id: str, stop_loss: Optional[float] = None, take_profit: Optional[float] = None) -> bool:
        """Modify SL/TP for an open position using its ID."""
        if not position_id: self._log.error("Cannot modify position: Position ID required."); return False
        modifications = {}
        if stop_loss is not None: modifications["stopLoss"] = float(stop_loss)
        if take_profit is not None: modifications["takeProfit"] = float(take_profit)
        if not modifications: self._log.warning("No modifications provided for position {position_id}."); return False

        url = f"{self.base_url}{PATH_TRADE_POSITION_BY_ID.format(position_id=position_id)}"
        try:
            status, data = await self._request("PATCH", url, json_data=modifications)
            if 200 <= status < 300:
                 self._log.info(f"Position {position_id} modified successfully. New SL={stop_loss}, TP={take_profit}")
                 return True
            else: return False # Error handled by _request
        except (TradeLockerAuthError, TradeLockerAPIError) as e: self._log.error(f"Failed to modify position {position_id}: {e}"); return False
        except Exception as e: self._log.exception(f"Unexpected error modifying position {position_id}: {e}"); return False

    async def close_all_positions(self, instrument_id: Optional[Union[int, str]] = None) -> bool:
        """Close all open positions, optionally filtering by instrument ID."""
        if not self.account_id: raise TradeLockerError("Account ID not set.")
        url = f"{self.base_url}{PATH_TRADE_CLOSE_ALL_POSITIONS.format(account_id=self.account_id)}"
        params = {}
        log_msg = "Closing all positions..."
        if instrument_id is not None:
            params["tradableInstrumentId"] = str(instrument_id)
            symbol = self.get_symbol_from_id(str(instrument_id)) or str(instrument_id)
            log_msg = f"Closing all positions for instrument {symbol}..."
        self._log.info(log_msg)

        try:
            status, data = await self._request("DELETE", url, params=params)
            if status in [200, 204]:
                 self._log.info(f"Close all positions request successful (Status {status}).")
                 return True
            else: return False # Error handled by _request
        except (TradeLockerAuthError, TradeLockerAPIError) as e: self._log.error(f"Failed to close all positions: {e}"); return False
        except Exception as e: self._log.exception(f"Unexpected error closing all positions: {e}"); return False

    # --- Shutdown ---
    async def shutdown_async(self):
        """Clean up resources asynchronously: close WebSocket and HTTP session."""
        if getattr(self, '_shutting_down_active', False): return # Prevent concurrent shutdowns
        self._shutting_down_active = True
        self._log.info("Shutting down TradeLocker API client...")
        self._is_shutting_down = True # Signal background tasks to stop

        # Stop WebSocket tasks first
        if self._ws_heartbeat_task and not self._ws_heartbeat_task.done(): self._ws_heartbeat_task.cancel()
        if self._ws_listener_task and not self._ws_listener_task.done(): self._ws_listener_task.cancel()

        # Close WebSocket connection gracefully
        if self.ws_connection and not self.ws_connection.closed:
            self._log.debug("Closing WebSocket connection...")
            try:
                await self.ws_connection.close(code=1000, reason="Client shutting down")
            except WebSocketException as e: self._log.warning(f"Error closing WebSocket connection: {e}")
            except Exception as e: self._log.exception(f"Unexpected error closing WebSocket: {e}")
            self.ws_connection = None
            self._ws_connected.clear()

        # Wait briefly for tasks to finish cancellation
        tasks_to_wait = [t for t in [self._ws_listener_task, self._ws_heartbeat_task] if t]
        if tasks_to_wait:
            try: await asyncio.gather(*tasks_to_wait, return_exceptions=True)
            except asyncio.CancelledError: pass # Expected

        # Close HTTP session
        await self._close_session()

        self._log.info("TradeLocker API client shut down complete.")
        self._shutting_down_active = False

    def shutdown(self):
        """Synchronous wrapper for shutdown (use async version if possible)."""
        self._log.warning("Using synchronous shutdown() is discouraged. Use 'await shutdown_async()' or 'async with' instead.")
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Schedule coroutine on running loop and wait (can block)
                future = asyncio.run_coroutine_threadsafe(self.shutdown_async(), loop)
                future.result(timeout=15) # Wait with timeout
            else:
                loop.run_until_complete(self.shutdown_async())
        except Exception as e:
            self._log.exception(f"Error during synchronous shutdown: {e}")

# --- Example Usage (Keep for testing) ---
async def example_main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # Load config using the refactored config loader
    from config import load_config
    main_config = load_config('config.ini') # Load from default file

    if not main_config.get('API', {}).get('email') or main_config.get('API', {}).get('email') == 'YOUR_TRADELOCKER_EMAIL':
         print("Please update config.ini with your TradeLocker credentials.")
         return

    api = TradeLockerAPI(main_config)

    async with api: # Uses __aenter__ and __aexit__ for setup/teardown
        initialized = await api.initialize()
        print("\nAPI Initialized:", initialized)

        if initialized and api.account_id:
            account_info = await api.get_account()
            if account_info: print(f"\nAccount Info: ID={account_info.id}, Balance={account_info.balance}, Currency={account_info.base_currency}")

            instruments = await api.get_instruments() # Already fetched in init, but can call again
            if instruments:
                print(f"\nFound {len(instruments)} instruments. First few:")
                for inst in instruments[:3]: print(f" - Symbol: {inst.get('symbol')}, ID: {inst.get('tradableInstrumentId')}, RouteID: {inst.get('routeId')}")

                first_instrument = instruments[0]
                instrument_id = first_instrument.get('tradableInstrumentId')
                route_id = first_instrument.get('routeId')
                symbol = first_instrument.get('symbol')

                if instrument_id and route_id and symbol:
                    price = await api.get_current_price(instrument_id, route_id)
                    if price: print(f"\nCurrent Price for {symbol}: Bid={price.bid}, Ask={price.ask}")

                    hist_data = await api.get_historical_data(instrument_id, route_id, timeframe="1m", count=5)
                    if hist_data:
                        print(f"\nLast {len(hist_data)} 1m bars for {symbol}:")
                        for bar in hist_data: print(f"  - Time: {bar.timestamp}, Close: {bar.close}")

            positions = await api.get_positions()
            print(f"\nCurrent Open Positions: {len(positions)}")
            for pos in positions: print(f" - ID: {pos.id}, Symbol: {pos.instrument}, Dir: {pos.direction}, Qty: {pos.quantity}, Entry: {pos.entry_price}, SL: {pos.stop_loss}, TP: {pos.take_profit}")

            # --- Example WebSocket Subscription ---
            def handle_quote_update(data):
                 quote = data.get('d', {})
                 symbol = quote.get('s')
                 bid = quote.get('bp')
                 ask = quote.get('ap')
                 # Use logger instead of print for consistency
                 logger.info(f"WS Quote: {symbol} Bid={bid} Ask={ask}")

            if instruments and instrument_id and route_id:
                 print("\nSubscribing to quote updates via WebSocket...")
                 api.subscribe('quotes', handle_quote_update, tradableInstrumentId=instrument_id, routeId=route_id)
                 await asyncio.sleep(15) # Keep running to receive updates
                 # api.unsubscribe('quotes', handle_quote_update, tradableInstrumentId=instrument_id, routeId=route_id) # Example unsubscribe

            # --- Example Order (Use with extreme caution!) ---
            # print("\nAttempting to place small market order...")
            # result = await api.place_market_order(instrument_id, route_id, 'buy', 0.01, tag="TestOrder")
            # if result: print(f"Order Result: {result}")

        else: print("Could not initialize API or find account ID.")
    print("\nExiting async with block (shutdown called)...")

if __name__ == "__main__":
    try:
        asyncio.run(example_main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")