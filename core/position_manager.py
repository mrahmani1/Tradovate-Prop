# core/position_manager.py
"""
Manages open positions, handles entries/exits, SL/TP modifications,
and interacts with the Tradovate API asynchronously.
"""
import asyncio
import datetime
import logging
import time # Add time import for TTL
import math # Add math for rounding ticks
from typing import Dict, List, Optional, Any, Tuple, Union, Callable

# Use absolute imports assuming standard project structure
try:
    from tradovate import TradovateClient
    from tradovate.errors import TradovateError
except ImportError:
    logging.getLogger(__name__).critical("Tradovate library not found.")
    TradovateClient = None
    TradovateError = Exception

from core.rules_engine import RulesEngine
from utils.data_classes import Position, Trade, Account
from config import get_config_value # Use config
# from utils.helpers import get_instrument_details, calculate_pip_distance # Use utils helpers - REMOVING THESE HELPERS FOR NOW

logger = logging.getLogger(__name__)

CACHE_TTL = 3600 # Seconds (e.g., 1 hour)

class PositionManagerError(Exception):
    """Custom exception for PositionManager critical errors."""
    pass

class PositionManager:
    """Manages trading positions with enhanced features."""

    def __init__(
        self,
        api: TradovateClient,
        rules_engine: RulesEngine,
        config: Dict[str, Any]
    ):
        """Initialize position manager."""
        if TradovateClient is None:
            raise ImportError("TradovateClient is not available. Cannot initialize PositionManager.")

        self.api = api
        self.rules_engine = rules_engine
        self.config = config

        # State: Use instrument symbol as key for positions
        self._positions: Dict[str, Position] = {} # Symbol -> Position object
        # Meta stores additional state not on the Position object itself
        self._position_meta: Dict[str, Dict] = {} # Symbol -> Meta data (targets, trailing, etc.)
        # Track active Tradovate orders (important for SL/TP management)
        self._active_orders: Dict[int, Dict] = {} # order_id (int) -> Tradovate order object or relevant details
        self.primary_account_id: Optional[int] = None # Determined during initialize

        self.trade_history: List[Trade] = []
        self._trade_callbacks: List[Callable[[Trade], Any]] = [] # Store callbacks

        # Cache for contract details to avoid repeated API calls
        self._contract_details_cache: Dict[str, Dict] = {}

        # Config shortcuts (consider caching more if needed)
        self._instrument_specs = config.get('instrument_specs', {})

    async def initialize(self) -> bool:
        """Initialize by fetching current state (accounts, positions, orders) from the Tradovate API.
        
        Raises:
            PositionManagerError: If critical initialization steps fail (e.g., fetching accounts, finding primary account).
        Returns:
            bool: True if initialization steps involving positions/orders succeeded (or if none existed). 
                  False indicates non-critical issues during position/order loading.
        """
        logger.info("Initializing Position Manager...")
        try:
            # 1. Get relevant account IDs
            # TODO: Verify API call/field: self.api.account_list() and returned account fields (e.g., `id`)
            accounts = await self.api.account_list() 
            if not accounts:
                msg = "Failed to fetch Tradovate accounts during initialization."
                logger.critical(msg)
                raise PositionManagerError(msg)

            # TODO: Select the correct account ID based on config or logic more robustly
            # Find demo or live account based on config if possible
            # For now, assume the first account is the primary one.
            primary_account = accounts[0]
            primary_account_id = primary_account.get('id') 
            if not primary_account_id:
                msg = "Could not determine primary account ID from fetched accounts."
                logger.critical(msg)
                raise PositionManagerError(msg)

            self.primary_account_id = int(primary_account_id)
            logger.info(f"Position Manager operating on Account ID: {self.primary_account_id} (Name: {primary_account.get('name', 'N/A')})")

            # Steps 2 (Positions) and 4 (Orders) can proceed but log errors if fetches fail
            # A failure here is less critical than not knowing the account ID.
            initialization_successful = True

            # 2. Fetch current positions for the account
            try:
                # TODO: Verify API call/field: self.api.position_list() and returned position fields (e.g., `accountId`, `id`, `contractId`, `netPos`, `avgEntryPrice`, `unrealizedPL`, `realizedPL`)
                current_api_positions = await self.api.position_list() 
                account_positions = [p for p in current_api_positions if p.get('accountId') == self.primary_account_id]
                self._positions.clear()
                self._position_meta.clear()
                for pos_data in account_positions:
                    # 3. Parse Tradovate position data into internal Position object
                    # TODO: Implement robust parsing based on actual Tradovate position object fields
                    # TODO: Need a reliable way to get the instrument *symbol* from the position data (might require contract lookup)
                    pos_id = pos_data.get('id')
                    contract_id = pos_data.get('contractId')
                    # Get symbol using cached helper
                    contract_details = await self._get_cached_contract_details(contract_id=contract_id)
                    symbol = contract_details.get('name') if contract_details else None
                    net_pos = float(pos_data.get('netPos', 0.0))
                    avg_entry = float(pos_data.get('avgEntryPrice', 0.0))

                    if not pos_id or not contract_id or net_pos == 0:
                        logger.warning(f"Skipping incomplete or flat position data: ID={pos_id}, Contract={contract_id}, NetPos={net_pos}")
                        continue
                    if not symbol: # If symbol lookup failed or wasn't present
                        logger.warning(f"Skipping position ID {pos_id} because symbol could not be determined.")
                        continue

                    direction = 'long' if net_pos > 0 else 'short'
                    quantity = abs(net_pos)

                    # Create Position object (ensure fields match data_classes.py)
                    pos = Position(
                        id=int(pos_id),
                        account_id=self.primary_account_id,
                        contract_id=int(contract_id),
                        instrument=symbol,
                        direction=direction,
                        quantity=quantity,
                        entry_price=avg_entry,
                        unrealized_pl=float(pos_data.get('unrealizedPL', 0.0)),
                        realized_pl=float(pos_data.get('realizedPL', 0.0))
                        # entry_time needs to be tracked separately, maybe from first fill event
                        # stop_loss_order_id / take_profit_order_id need linking based on active orders
                    )

                    self._positions[symbol] = pos
                    # Initialize basic meta data
                    self._position_meta[symbol] = self._create_default_meta(pos)
                    logger.info(f"Loaded existing Tradovate position: {symbol} {pos.direction} Qty:{pos.quantity:.2f} @ {pos.entry_price:.5f} (ID: {pos.id})")

            except TradovateError as e:
                logger.error(f"Tradovate API error fetching positions during initialization: {e}", exc_info=True)
                initialization_successful = False # Mark init as partially failed
            except Exception as e:
                logger.exception(f"Unexpected error fetching/processing positions during initialization: {e}")
                initialization_successful = False # Mark init as partially failed

            # 4. Fetch active orders for the account
            try:
                # TODO: Verify API call/field: self.api.order_list() and returned order fields (e.g., `accountId`, `id`, `ordStatus`, `contractId`, `orderType`, `action`, `parentId`?)
                active_api_orders = await self.api.order_list() 
                account_orders = [o for o in active_api_orders if o.get('accountId') == self.primary_account_id and
                                    o.get('ordStatus') in ['Working', 'Pending', 'Accepted', 'Received']] # Include relevant active statuses
                self._active_orders.clear()
                for order_data in account_orders:
                    order_id = order_data.get('id')
                    if order_id:
                        order_id_int = int(order_id)
                        self._active_orders[order_id_int] = order_data
                        # --- Attempt to Link SL/TP Orders to Initial Positions ---
                        # TODO: Refine linking logic based on actual Tradovate bracket/linking fields (e.g., parentId, linkedOrderId, OCO flags)
                        contract_id = order_data.get('contractId')
                        order_type = order_data.get('orderType')
                        action = order_data.get('action') # 'Buy' or 'Sell'
                        symbol = await self._get_symbol_for_contract(contract_id) # Requires symbol lookup
                        if symbol and symbol in self._positions:
                            position = self._positions[symbol]
                            # Identify potential SL order (Stop order, opposite action to position)
                            if order_type == 'Stop' and action != position.direction: # Action is 'Sell' for long SL, 'Buy' for short SL
                                if position.stop_loss_order_id is None: # Link if not already linked
                                    position.stop_loss_order_id = order_id_int
                                    logger.info(f"Linked existing SL Order {order_id_int} to position {symbol}")
                                else:
                                    logger.warning(f"Position {symbol} already has SL {position.stop_loss_order_id}, found another candidate {order_id_int}")
                            # Identify potential TP order (Limit order, opposite action to position)
                            elif order_type == 'Limit' and action != position.direction: # Action is 'Sell' for long TP, 'Buy' for short TP
                                if position.take_profit_order_id is None:
                                    position.take_profit_order_id = order_id_int
                                    logger.info(f"Linked existing TP Order {order_id_int} to position {symbol}")
                                else:
                                    logger.warning(f"Position {symbol} already has TP {position.take_profit_order_id}, found another candidate {order_id_int}")
                        logger.debug(f"Loaded active order: ID={order_id_int}, Status={order_data.get('ordStatus')}, Type={order_type}")

            except TradovateError as e:
                logger.error(f"Tradovate API error fetching orders during initialization: {e}", exc_info=True)
                initialization_successful = False # Mark init as partially failed
            except Exception as e:
                logger.exception(f"Unexpected error fetching/processing orders during initialization: {e}")
                initialization_successful = False # Mark init as partially failed

            logger.info(f"Position Manager initialization state: Account ID set. {len(self._positions)} positions loaded, {len(self._active_orders)} active orders loaded. Success status: {initialization_successful}")
            return initialization_successful # Return whether position/order loading had issues

        except TradovateError as e:
            # Catch API errors during critical account fetch
            msg = f"Tradovate API error during critical initialization (account fetch): {e}"
            logger.critical(msg, exc_info=True)
            raise PositionManagerError(msg) from e
        except Exception as e:
            # Catch unexpected errors during critical account fetch
            msg = f"Unexpected critical error during Position Manager initialization (account fetch): {e}"
            logger.critical(msg, exc_info=True)
            raise PositionManagerError(msg) from e

    def _create_default_meta(self, position: Position) -> Dict:
        """Creates the default metadata dictionary for a new or loaded position."""
        # TODO: Review/Update this based on how SL/TP are managed (separate orders vs attached)
        # Tradovate often uses separate SL/TP orders linked by IDs (e.g., bracket orders)
        return {
            'entry_price': position.entry_price,
            'initial_stop_loss_price': None, # Price level of initial SL order
            'initial_take_profit_price': None, # Price level of initial TP order
            'profit_targets': [], # List of {'price': float, 'percent_to_close': float, 'hit': bool} - TODO: Adapt logic
            'trailing_stop_active': False,
            'trailing_stop_config': {}, # {'activation_pips': float, 'trail_pips': float} - TODO: Adapt logic
            'trailing_stop_level': None, # Current trailing SL price (needs to be derived from active SL order)
            'highest_price_seen': position.entry_price, # For trailing stop logic (long)
            'lowest_price_seen': position.entry_price, # For trailing stop logic (short)
            'exit_strategy': 'default', # Can be overridden by strategy
            'partial_exits': [], # List of {'time': dt, 'price': float, 'quantity': float, 'reason': str}
            'initial_quantity': position.quantity # Store initial quantity for partial exit calcs
        }

    async def update(self):
        """Periodic update task for managing positions (e.g., trailing stops)."""
        # TODO: Review trailing stop and profit target logic for Tradovate
        # This might involve checking market price against activation levels and modifying linked SL orders
        active_positions = list(self._positions.values()) # Get a copy
        tasks = []
        for position in active_positions:
            # Schedule async checks for each position
            tasks.append(asyncio.create_task(self._check_position_management(position)))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True) # Run checks concurrently

    async def _check_position_management(self, position: Position):
        """Run management checks (trailing stop, profit targets) for a single position."""
        try:
            # Ensure position still exists (might have been closed by another task/update)
            if position.instrument not in self._positions: return

            await self._update_trailing_stop(position)
            await self._check_profit_targets(position)
            # Add other periodic checks if needed (e.g., time-based exits)
        except Exception as e:
            logger.exception(f"Error during position management check for {position.id} ({position.instrument}): {e}")

    async def _update_trailing_stop(self, position: Position):
        """Update trailing stop loss if active and conditions met."""
        # TODO: Review config keys and overall logic for Tradovate context
        # Assumes position.current_price is updated reliably by TradingSystem
        # Relies on modify_position working correctly to modify the actual SL order
        symbol = position.instrument
        meta = self._position_meta.get(symbol)
        current_price = position.current_price

        # --- Basic Checks ---
        if not meta or current_price is None:
            return

        ts_config = meta.get('trailing_stop_config', {})
        activation_pips = ts_config.get('activation_pips') # Pips/Ticks in profit to activate
        trail_pips = ts_config.get('trail_pips') # Pips/Ticks to trail behind high/low

        # Check if trailing is configured
        if trail_pips is None or trail_pips <= 0:
            return

        # --- Get Contract Details (for tick size) ---
        contract_details = await self._get_cached_contract_details(symbol=symbol)
        if not contract_details:
            logger.warning(f"Cannot update trailing stop for {symbol}: Missing contract details.")
            return
        tick_size = contract_details.get('tickSize')
        if tick_size is None or tick_size <= 0:
            logger.warning(f"Cannot update trailing stop for {symbol}: Invalid tickSize {tick_size}.")
            return

        # Convert config pips/ticks to price distance
        trail_distance = trail_pips * tick_size
        activation_distance = (activation_pips * tick_size) if activation_pips is not None else None

        # --- Activation Check ---
        is_active = meta.get('trailing_stop_active', False)
        if not is_active:
            entry_price = meta.get('entry_price')
            if entry_price is None:
                return # Cannot activate without entry price
            price_diff = abs(current_price - entry_price)
            # Activate if profit distance meets activation threshold OR if no threshold defined
            if activation_distance is None or price_diff >= activation_distance:
                # Check direction - only activate if in profit
                in_profit = (position.direction == 'long' and current_price > entry_price) or \
                            (position.direction == 'short' and current_price < entry_price)

                if activation_distance is None or in_profit: # Activate if no threshold OR if threshold met AND in profit
                    logger.info(f"Activating trailing stop for {symbol} (ID: {position.id}). Activation dist: {activation_distance}, Current diff: {price_diff}")
                    meta['trailing_stop_active'] = True
                    is_active = True
                    # Set initial high/low water marks upon activation
                    meta['lowest_price_seen'] = current_price
                    # Optional: Immediately move SL to B/E or B/E + buffer on activation? Add logic here if needed.
                    # initial_sl_price = meta.get('initial_stop_loss_price')
                    # meta['trailing_stop_level'] = max(initial_sl_price, entry_price) if position.direction == 'long' else min(initial_sl_price, entry_price)
                    # await self.modify_position(symbol, stop_loss=meta['trailing_stop_level'])

        # --- Trailing Logic (if active) ---
        if not is_active:
            return

        # Update high/low water marks
        meta['highest_price_seen'] = max(meta.get('highest_price_seen', current_price), current_price)
        meta['lowest_price_seen'] = min(meta.get('lowest_price_seen', current_price), current_price)

        # Calculate potential new stop level
        potential_new_sl = None
        if position.direction == 'long':
            potential_new_sl = meta['highest_price_seen'] - trail_distance
        else: # Short
            potential_new_sl = meta['lowest_price_seen'] + trail_distance

        # Round to nearest tick
        potential_new_sl = round(potential_new_sl / tick_size) * tick_size

        # Get current SL order price (requires order linking to be working)
        current_sl_order_id = getattr(position, 'stop_loss_order_id', None)
        current_sl_order = self._active_orders.get(current_sl_order_id) if current_sl_order_id else None
        current_sl_price = current_sl_order.get('stopPrice') if current_sl_order else None

        # Check if we need to update the SL order
        needs_update = False
        if current_sl_price is None: # No active SL order linked? Maybe place one?
             logger.warning(f"Trailing stop active for {symbol}, but no current SL order price found. Cannot trail.")
             # Or potentially place a new SL order here at potential_new_sl?
        elif position.direction == 'long' and potential_new_sl > current_sl_price:
            needs_update = True
        elif position.direction == 'short' and potential_new_sl < current_sl_price:
            needs_update = True

        # Avoid modifying if the change is less than a tick (or negligible)
        if needs_update and abs(potential_new_sl - current_sl_price) < (tick_size / 2):
            needs_update = False

        if needs_update:
            logger.info(f"Trailing Stop Update for {symbol}: Moving SL from {current_sl_price:.5f} to {potential_new_sl:.5f}")
            # Use modify_position which handles modifying the SL order
            success = await self.modify_position(instrument_symbol=symbol, stop_loss=potential_new_sl)
            if success:
                meta['trailing_stop_level'] = potential_new_sl # Update meta with the new target level
                logger.info(f"Trailing Stop modify request submitted for {symbol}.")
            else:
                logger.error(f"Failed to submit trailing stop modify request for {symbol}.")

    async def _check_profit_targets(self, position: Position):
        """Check if any configured profit targets have been hit for a position."""
        symbol = position.instrument
        meta = self._position_meta.get(symbol)
        current_price = position.current_price

        # Basic checks
        if not meta or not meta.get('profit_targets') or current_price is None:
            return

        targets = meta['profit_targets']
        initial_quantity = meta.get('initial_quantity')
        if initial_quantity is None or initial_quantity <= 0:
            logger.warning(f"Cannot check profit targets for {symbol}: initial_quantity not found or invalid in metadata.")
            return

        # Fetch contract details once for min order quantity check
        contract_details = await self._get_cached_contract_details(symbol=symbol)
        min_order_qty = contract_details.get('minOrderQty', 0.000001) if contract_details else 0.000001

        # Iterate through targets that haven't been hit yet
        for i, target_info in enumerate(targets):
            if target_info.get('hit'):
                continue

            target_price = target_info.get('price')
            percent_to_close = target_info.get('percent_to_close')
            if target_price is None or percent_to_close is None or not (0 < percent_to_close <= 100):
                logger.warning(f"Invalid profit target config for {symbol} at index {i}: {target_info}")
                continue

            # Check if target price is reached
            target_hit = False
            if position.direction == 'long' and current_price >= target_price:
                target_hit = True
            elif position.direction == 'short' and current_price <= target_price:
                target_hit = True

            if target_hit:
                logger.info(f"Profit Target {i+1} hit for {symbol} (ID: {position.id}) at Price={current_price:.5f} (Target={target_price:.5f})")

                # --- Simplified Quantity Calculation ---
                # Calculate the cumulative quantity that *should* be closed by this target level
                cumulative_target_qty_at_this_level = round(initial_quantity * (percent_to_close / 100.0), 8)

                # Calculate the total quantity already closed from previous partial exits (including other TPs)
                total_qty_already_closed = sum(p['quantity'] for p in meta.get('partial_exits', []))

                # Calculate how much more needs to be closed to reach this target's cumulative level
                qty_needed_for_target = max(0.0, cumulative_target_qty_at_this_level - total_qty_already_closed)

                # Determine the actual quantity to close now, capped by remaining position size
                remaining_on_broker = position.quantity # Current tracked quantity
                qty_to_close_now = round(min(qty_needed_for_target, remaining_on_broker), 8)

                # ----------------------------------------

                if qty_to_close_now < min_order_qty:
                    logger.warning(f"Calculated quantity {qty_to_close_now:.8f} for TP{i+1} ({symbol}) is below minimum order size ({min_order_qty}). Marking target as hit without execution.")
                    target_info['hit'] = True
                    continue # Skip execution

                # Execute the partial exit
                success = await self._execute_partial_exit(position, qty_to_close_now, f"Profit target {i+1} hit")

                if success:
                    # Mark target as hit and record the partial exit
                    target_info['hit'] = True
                    meta.setdefault('partial_exits', []).append({
                        'time': datetime.datetime.now(datetime.timezone.utc),
                        'price': current_price, # Record price at time of check
                        'quantity': qty_to_close_now,
                        'reason': f"Profit target {i+1}"
                    })
                    logger.info(f"Successfully submitted partial exit order for TP{i+1} ({symbol}), Qty: {qty_to_close_now}")

                    # Optional: Move SL to Breakeven after first TP hit
                    # TODO: Review this logic - depends on SL being a separate order
                    move_to_be = get_config_value(self.config, 'PROFIT_TARGETS.move_to_breakeven_on_first_target', True)
                    if i == 0 and move_to_be:
                        be_price = meta['entry_price']
                        logger.info(f"Attempting to move SL to Breakeven ({be_price:.5f}) for {symbol} after TP1 hit.")
                        # Requires modify_position to work with SL orders
                        # await self.modify_position(symbol, stop_loss=be_price)

                    # Optional: Close entire remaining position on final target hit
                    # TODO: Review this logic
                    # close_on_final = get_config_value(self.config, 'PROFIT_TARGETS.close_on_final_target', True)
                    # is_last_target = all(t.get('hit') for t in targets)
                    # if is_last_target and close_on_final and position.quantity > 0: # Check if any quantity *should* remain
                    #    logger.info(f"Closing remaining position for {symbol} after final TP hit.")
                    #    await self.close_position(symbol, reason="Final profit target hit")
                else:
                    logger.error(f"Failed to submit partial exit order for TP{i+1} ({symbol})")
                    # Decide whether to retry or mark as failed

    async def _modify_order_quantity(self, order_id: int, new_quantity: float) -> bool:
        """Helper to modify the quantity of an existing active order."""
        if order_id not in self._active_orders:
            logger.warning(f"Cannot modify quantity for order {order_id}: Order not found in active cache.")
            return False
        
        # Ensure new quantity is positive
        if new_quantity <= 0:
            logger.warning(f"Cannot modify order {order_id} quantity to {new_quantity}. Attempting to cancel instead.")
            # If the new quantity is zero or less, we should cancel the order
            try:
                # TODO: Verify API call/field: self.api.order_cancel_order(orderId=order_id)
                await self.api.order_cancel_order(orderId=order_id)
                logger.info(f"Cancelled order {order_id} instead of modifying quantity to zero/negative.")
                self._active_orders.pop(order_id, None) # Remove from active cache
                return True # Indicate success (cancellation is the correct action)
            except Exception as e:
                logger.error(f"Failed to cancel order {order_id} when trying to modify quantity to zero: {e}", exc_info=True)
                return False

        # Get existing order details for the payload
        current_order_details = self._active_orders.get(order_id)
        if not current_order_details:
             logger.warning(f"Cannot modify quantity for order {order_id}: Order details missing after initial check.")
             return False
        
        logger.info(f"Attempting to modify order {order_id} quantity to {new_quantity:.8f}")

        # Construct the modification payload
        # Requires orderId and the fields to modify (orderQty)
        # May require other fields depending on API strictness (e.g., orderType, price)
        modify_payload = {
            "orderId": order_id,
            "orderQty": round(new_quantity, 8) # Ensure proper precision
            # TODO: Verify if other fields like price/stopPrice are needed for modification
            # Add them from current_order_details if required by the API
            # "price": current_order_details.get('price'),
            # "stopPrice": current_order_details.get('stopPrice'),
        }

        try:
            # TODO: Verify API call/field: self.api.order_modify_order(**modify_payload) and response structure (confirmation fields)
            result = await self.api.order_modify_order(**modify_payload)
            # Check result status - modify often returns the modified order details or a confirmation
            if result and result.get('orderId') == order_id: # Simple check, adjust based on actual response
                logger.info(f"Order {order_id} quantity modification request submitted successfully.")
                # Update cache optimistically (WS update will provide definitive state)
                if order_id in self._active_orders:
                    self._active_orders[order_id]['orderQty'] = new_quantity
                return True
            else:
                logger.error(f"Order {order_id} quantity modification request failed or confirmation unclear. Response: {result}")
                return False
        except TradovateError as e:
            logger.error(f"API error modifying order {order_id} quantity: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.exception(f"Unexpected error modifying order {order_id} quantity: {e}")
            return False

    async def _execute_partial_exit(self, position: Position, quantity_to_close: float, reason: str) -> bool:
        """Execute a partial exit by closing a portion of the position and adjusting SL/TP quantities."""
        # TODO: REWRITE FOR TRADOVATE
        # - Place an opposing MARKET or LIMIT order for quantity_to_close
        # - Needs accountId, contractId, action (Buy/Sell opposite to position), orderQty
        symbol = position.instrument
        contract_id = position.contract_id
        account_id = self.primary_account_id

        if not contract_id or not account_id:
            logger.error(f"Cannot execute partial exit for {symbol}: Missing contractId or accountId.")
            return False

        if quantity_to_close <= 0:
            logger.warning(f"Attempted partial exit for {symbol} with invalid quantity: {quantity_to_close}")
            return False

        logger.info(f"Executing partial exit for {symbol} (PosID: {position.id}): Close {quantity_to_close:.8f} lots. Reason: {reason}")

        partial_exit_action = 'Sell' if position.direction == 'long' else 'Buy'

        payload = {
            "accountId": account_id,
            "contractId": contract_id,
            "action": partial_exit_action,
            "orderQty": round(quantity_to_close, 8), # Ensure precision
            "orderType": "Market", # Use Market for immediate partial exit
            # "text": f"Partial Exit - {reason}"[:50]
        }
        try:
            # TODO: Verify API call/field: self.api.order_placeOrder(**payload) for partial exit and response structure (`orderId`)
            result = await self.api.order_placeOrder(**payload)
            order_id = result.get('orderId')
            if order_id:
                logger.info(f"Partial exit order {order_id} submitted for {symbol}.")
                self._active_orders[int(order_id)] = {"id": int(order_id), "ordStatus": "Pending", **payload}
                # Actual position quantity update will happen via fill in process_order_update

                # --- Modify associated SL/TP orders --- 
                # Calculate the expected remaining quantity AFTER this exit order fills
                remaining_quantity = position.quantity - quantity_to_close
                logger.info(f"Attempting to modify associated SL/TP orders for {symbol} to remaining quantity: {remaining_quantity:.8f}")

                sl_order_id = getattr(position, 'stop_loss_order_id', None)
                tp_order_id = getattr(position, 'take_profit_order_id', None)

                # Modify SL quantity if exists and active
                if sl_order_id and sl_order_id in self._active_orders:
                    logger.info(f"Modifying SL order {sl_order_id} quantity to {remaining_quantity:.8f}")
                    sl_modify_success = await self._modify_order_quantity(sl_order_id, remaining_quantity)
                    if not sl_modify_success:
                         logger.error(f"Failed to submit modification request for SL order {sl_order_id} quantity after partial exit for {symbol}. Check WS updates for final status.")
                         # Continue anyway, main exit order submitted

                # Modify TP quantity if exists and active
                if tp_order_id and tp_order_id in self._active_orders:
                    logger.info(f"Modifying TP order {tp_order_id} quantity to {remaining_quantity:.8f}")
                    tp_modify_success = await self._modify_order_quantity(tp_order_id, remaining_quantity)
                    if not tp_modify_success:
                         logger.error(f"Failed to submit modification request for TP order {tp_order_id} quantity after partial exit for {symbol}. Check WS updates for final status.")
                         # Continue anyway, main exit order submitted

                return True # Return True as the primary partial exit order was submitted
            else:
                logger.error(f"Partial exit order submission failed for {symbol}. Response: {result}")
                return False
        except TradovateError as e:
            logger.error(f"API error during partial exit for {symbol}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.exception(f"Unexpected error during partial exit for {symbol}: {e}")
            return False

    async def _calculate_ticks(self, entry_price: float, exit_price: float, direction: str, symbol: str) -> Optional[float]:
        """Calculate profit/loss in ticks."""
        if entry_price is None or exit_price is None:
            return 0.0

        contract_details = await self._get_cached_contract_details(symbol=symbol)
        if not contract_details:
            logger.warning(f"Cannot calculate ticks for {symbol}: Missing contract details.")
            return None # Indicate failure to calculate due to missing info
        
        tick_size = contract_details.get('tickSize')
        if tick_size is None or tick_size == 0:
            logger.warning(f"Cannot calculate ticks for {symbol}: Invalid tickSize {tick_size} in contract details.")
            return None # Indicate failure

        price_diff = exit_price - entry_price if direction == 'long' else entry_price - exit_price
        
        try:
            ticks = price_diff / tick_size
            return ticks
        except ZeroDivisionError:
             logger.warning(f"Cannot calculate ticks for {symbol}: tickSize is zero.")
             return None

    # --- Public Methods ---

    def get_position(self, instrument_symbol: str) -> Optional[Position]:
        """Get position by instrument symbol."""
        return self._positions.get(instrument_symbol)

    def get_all_positions(self) -> List[Position]:
        """Get all currently managed open positions."""
        return list(self._positions.values())

    def register_trade_callback(self, callback: Callable[[Trade], Any]):
        """Register a callback function to be called when a trade is completed."""
        if callback not in self._trade_callbacks:
            self._trade_callbacks.append(callback)
            logger.info(f"Registered trade callback: {callback.__name__}")

    async def open_position(
        self, instrument_symbol: str, direction: str, quantity: float,
        order_type: str = 'market', price: Optional[float] = None,
        stop_loss: Optional[float] = None, # Price level for SL (e.g., from OSO)
        take_profit: Optional[float] = None, # Price level for TP (e.g., from OSO)
        sl_ticks: Optional[Union[int, float]] = None, # SL distance in ticks (used if stop_loss price is None)
        tp_ticks: Optional[Union[int, float]] = None, # TP distance in ticks (used if take_profit price is None)
        strategy_type: Optional[str] = None,
        # ... (other args remain)
    ) -> bool:
        """Request to open a new position using Tradovate API."""
        logger.info(f"Request received to open {direction} {quantity:.2f} lots of {instrument_symbol} ({order_type})")
        # --- 1. Validate Input & Fetch Contract Details ---
        if quantity <= 0:
            logger.error(f"Cannot open position for {instrument_symbol}: Invalid quantity ({quantity})")
            return False
        if direction.lower() not in ['long', 'short']:
            logger.error(f"Cannot open position for {instrument_symbol}: Invalid direction ({direction})")
            return False
        action = 'Buy' if direction.lower() == 'long' else 'Sell'

        # Use cached helper
        contract_details = await self._get_cached_contract_details(symbol=instrument_symbol)
        if not contract_details:
            logger.error(f"Cannot open position: Failed to get contract details for {instrument_symbol}.")
            return False
        contract_id = contract_details.get('id')
        # tick_size = contract_details.get('tickSize') # Might need tick size later
        if not contract_id:
            logger.error(f"Cannot open position: Missing contract ID for {instrument_symbol}.")
            return False

        # --- 1.5 Fetch Account Info (Needed for Rules & Margin) ---
        # TODO: Consider caching account info or getting it from TradingSystem if updated frequently via WS
        account = None
        if self.primary_account_id:
            logger.debug(f"Fetching latest account info for {self.primary_account_id} (for rules/margin)")
            try:
                # Reuse logic similar to get_account_and_contract_info, but simplified
                # TODO: Verify API call/field: self.api.account_item(id=self.primary_account_id) and returned fields (`id`, `name`, `buyingPower`, `balance`, `totalCashValue`)
                acc_data = await self.api.account_item(id=self.primary_account_id)
                if acc_data:
                    # Create simple Account object for checks
                    account = Account(
                        id=int(acc_data.get('id')),
                        name=acc_data.get('name'),
                        margin_available=float(acc_data.get('buyingPower', 0.0)),
                        balance=float(acc_data.get('balance', 0.0)), # Needed for RulesEngine
                        equity=float(acc_data.get('totalCashValue', 0.0)), # Needed for RulesEngine
                        # Add other fields if RulesEngine needs them
                    )
            except Exception as e:
                logger.error(f"Failed to fetch account info for entry checks: {e}", exc_info=True)
                # Decide whether to proceed without checks or fail
                return False # Fail if account info is crucial and missing
        else:
             logger.error("Cannot perform entry checks: Primary account ID not set.")
             return False
        
        if not account:
            logger.error("Cannot perform entry checks: Failed to create Account object from API data.")
            return False

        # --- 2. Check Rules Engine ---
        can_enter_rules, reason_rules = self.rules_engine.can_enter_position(
            instrument=instrument_symbol, direction=direction, quantity=quantity,
            account=account, # Pass fetched account object
            current_positions=self.get_all_positions(),
            contract_details=contract_details # Pass fetched contract details
        )
        if not can_enter_rules:
            logger.warning(f"Rules Engine Check Failed: Cannot open position for {instrument_symbol}: {reason_rules}")
            return False

        # --- 2.5 Check Margin Requirements ---
        initial_margin_per_contract = contract_details.get('initialMargin') # Verify exact field name from API
        if initial_margin_per_contract is None:
            logger.warning(f"Cannot perform margin check for {instrument_symbol}: Initial margin not found in contract details. Skipping check.")
            # Decide if this is acceptable or should block the trade
            # return False # Uncomment to block trade if margin info is missing
        else:
            required_margin = float(initial_margin_per_contract) * quantity
            available_margin = account.margin_available # From fetched account data
            logger.debug(f"Margin Check for {quantity} {instrument_symbol}: Required=${required_margin:.2f}, Available=${available_margin:.2f}")
            if required_margin > available_margin:
                 logger.error(f"Entry blocked for {instrument_symbol}: Insufficient margin. Required: ${required_margin:.2f}, Available: ${available_margin:.2f}")
                 return False

        # --- 3. Construct Tradovate Order Payload ---
        # Ref: OpenAPI spec for /order/placeOrder and potentially /order/placeOSO
        order_payload: Dict[str, Any] = {
            "accountId": self.primary_account_id,
            "contractId": contract_id,
            "action": action,
            "orderQty": quantity,
        }
        # Add order type specific parameters
        if order_type.lower() == 'market':
            order_payload['orderType'] = 'Market'
        elif order_type.lower() == 'limit':
            if price is None:
                logger.error(f"Limit order for {instrument_symbol} requires a price.")
                return False
            order_payload['orderType'] = 'Limit'
            order_payload['price'] = price
        elif order_type.lower() == 'stop':
            if price is None:
                logger.error(f"Stop (entry) order for {instrument_symbol} requires a stop price.")
                return False
            order_payload['orderType'] = 'Stop'
            order_payload['stopPrice'] = price
        else:
            logger.error(f"Unsupported order type for entry: {order_type}")
            return False

        # Add optional text/tag
        if strategy_type:
            order_payload['text'] = strategy_type[:50] # Max length might apply

        # --- Store SL/TP Tick info for processing AFTER fill ---
        # This data will be stored with the order in _active_orders
        tick_based_sl_tp_intent = None
        if stop_loss is None and take_profit is None and (sl_ticks is not None or tp_ticks is not None):
            tick_based_sl_tp_intent = {'sl_ticks': sl_ticks, 'tp_ticks': tp_ticks}
            logger.info(f"SL/TP placement intent by ticks recorded for {instrument_symbol} (SL: {sl_ticks}, TP: {tp_ticks}). Will be placed after entry fill.")

        # --- 4. Handle SL/TP (Using Bracket Orders - placeOSO) ---
        if stop_loss is not None or take_profit is not None:
            logger.info(f"Attempting to place OSO (Bracket) Order for {instrument_symbol} with SL={stop_loss}, TP={take_profit}")
            bracket_orders = []
            # Define the SL order (opposite action, Stop type)
            if stop_loss is not None:
                sl_action = 'Sell' if action == 'Buy' else 'Buy'
                bracket_orders.append({
                    "action": sl_action,
                    "orderType": 'Stop',
                    "stopPrice": stop_loss,
                    "orderQty": quantity # SL qty usually matches entry
                })
            # Define the TP order (opposite action, Limit type)
            if take_profit is not None:
                tp_action = 'Sell' if action == 'Buy' else 'Buy'
                bracket_orders.append({
                    "action": tp_action,
                    "orderType": 'Limit',
                    "price": take_profit,
                    "orderQty": quantity # TP qty usually matches entry
                })

            # Construct the placeOSO payload
            oso_payload = {
                "accountId": self.primary_account_id,
                "contractId": contract_id,
                "action": action,
                "orderQty": quantity,
                "orderType": order_payload['orderType'], # Use entry order type
                # Add price/stopPrice for Limit/Stop entries
                **({'price': price} if order_type.lower() == 'limit' else {}),
                **({'stopPrice': price} if order_type.lower() == 'stop' else {}),
                "brackets": bracket_orders,
                **({'text': strategy_type[:50]} if strategy_type else {})
            }

            try:
                # Use placeOSO endpoint
                # TODO: Verify API call/field: self.api.order_placeOSO(**oso_payload) parameters and response structure (`orderId`, potentially linked order IDs like `stopLossOrderId`, `takeProfitOrderId`)
                result = await self.api.order_placeOSO(**oso_payload) # Assumes placeOSO method exists
                order_id = result.get('orderId') # OSO call might return the primary order ID
                if order_id:
                    logger.info(f"Bracket Order (OSO) submitted successfully for {instrument_symbol}. Entry Order ID: {order_id}")
                    # Add placeholder to active orders - details will come via WS update
                    self._active_orders[int(order_id)] = {"id": int(order_id), "ordStatus": "Pending", "is_oso": True, **oso_payload} # Store basic info
                    # --- SL/TP Linking for OSO ---
                    # OSO response might contain linked IDs, or we wait for WS updates
                    # If response has linked IDs, store them temporarily or directly on position if known
                    # Example placeholder: linked_sl_id = result.get('stopLossOrderId')
                    # linked_tp_id = result.get('takeProfitOrderId')
                    # If we know the position ID immediately (unlikely), we could link here.
                    # Otherwise, process_order_update needs to handle linking when SL/TP orders arrive.
                    logger.info("Waiting for WebSocket updates to link SL/TP orders from OSO submission.")
                    return True
                else:
                    logger.error(f"Bracket Order submission failed for {instrument_symbol}. API Response: {result}")
                    return False
            except TradovateError as e:
                logger.error(f"Tradovate API error placing Bracket Order for {instrument_symbol}: {e}", exc_info=True)
                return False
            except Exception as e:
                logger.exception(f"Unexpected error placing Bracket Order for {instrument_symbol}: {e}")
                return False

        # --- 5. Place Simple Entry Order (No SL/TP price provided or using ticks) ---
        else:
            logger.info(f"Placing simple entry order for {instrument_symbol} (SL/TP prices not provided)")
            try:
                # TODO: Verify API call/field: self.api.order_placeOrder(**order_payload) for simple entry and response structure (`orderId`)
                result = await self.api.order_placeOrder(**order_payload)
                order_id = result.get('orderId')
                if order_id:
                    order_id_int = int(order_id)
                    logger.info(f"Simple Entry Order submitted successfully for {instrument_symbol}. Order ID: {order_id_int}")
                    order_cache_entry = {"id": order_id_int, "ordStatus": "Pending", **order_payload} 

                    # Store the tick-based SL/TP intent WITH the order data if present
                    if tick_based_sl_tp_intent:
                        order_cache_entry['tick_based_sl_tp'] = tick_based_sl_tp_intent
                        # Log confirmation that intent is stored
                        logger.debug(f"Stored tick-based SL/TP intent with order {order_id_int}")

                    self._active_orders[order_id_int] = order_cache_entry
                    return True
                else:
                    logger.error(f"Simple Entry Order submission failed for {instrument_symbol}. API Response: {result}")
                    return False
            except TradovateError as e:
                 logger.error(f"Tradovate API error placing Simple Entry Order for {instrument_symbol}: {e}", exc_info=True)
                 return False
            except Exception as e:
                 logger.exception(f"Unexpected error placing Simple Entry Order for {instrument_symbol}: {e}")
                 return False

    async def close_position(self, instrument_symbol: str, reason: Optional[str] = None) -> bool:
        """Request to close an entire position using Tradovate API."""
        position = self.get_position(instrument_symbol)
        if not position:
            logger.warning(f"Request to close {instrument_symbol}, but no active position found.")
            return False
        if not self.primary_account_id or not position.contract_id:
            logger.error(f"Cannot close {instrument_symbol}: Missing account ID or contract ID.")
            return False

        logger.info(f"Requesting to close position for {instrument_symbol} (ID: {position.id}, Contract: {position.contract_id}). Reason: {reason or 'Manual'}")

        # --- 1. Cancel Associated Working SL/TP Orders --- 
        # We must cancel protective orders before placing the closing order
        sl_order_id = getattr(position, 'stop_loss_order_id', None) # Use getattr for safety
        tp_order_id = getattr(position, 'take_profit_order_id', None)

        orders_to_cancel = []
        if sl_order_id and sl_order_id in self._active_orders:
            orders_to_cancel.append(sl_order_id)
        if tp_order_id and tp_order_id in self._active_orders:
            orders_to_cancel.append(tp_order_id)

        if orders_to_cancel:
            logger.info(f"Cancelling associated SL/TP orders for {instrument_symbol}: {orders_to_cancel}")
            for order_id in orders_to_cancel:
                try:
                    # Assumes an order_cancel_order method exists
                    # TODO: Verify API call/field: self.api.order_cancel_order(orderId=order_id)
                    await self.api.order_cancel_order(orderId=order_id)
                    # Remove from local cache optimistically (WS update will confirm)
                    self._active_orders.pop(order_id, None)
                except TradovateError as e:
                    # Log error but continue to attempt closing position
                    logger.error(f"API error cancelling order {order_id} for {instrument_symbol}: {e}", exc_info=True)
                except Exception as e:
                    logger.exception(f"Unexpected error cancelling order {order_id} for {instrument_symbol}: {e}")

        # --- 2. Place Opposing Order --- 
        close_action = 'Sell' if position.direction == 'long' else 'Buy'
        close_qty = position.quantity

        close_payload = {
            "accountId": self.primary_account_id,
            "contractId": position.contract_id,
            "action": close_action,
            "orderQty": close_qty,
            "orderType": "Market", # Use Market order for quick closure
            # Add text/tag if desired
            # "text": f"Close {instrument_symbol} - {reason or 'Manual'}"[:50]
        }

        try:
            logger.info(f"Placing closing {close_action} order for {close_qty} {instrument_symbol}...")
            # TODO: Verify API call/field: self.api.order_placeOrder(**close_payload) for closing order and response structure (`orderId`)
            result = await self.api.order_placeOrder(**close_payload) # Use placeOrder
            order_id = result.get('orderId')
            if order_id:
                logger.info(f"Closing order submitted successfully for {instrument_symbol}. Order ID: {order_id}")
                self._active_orders[int(order_id)] = {"id": int(order_id), "ordStatus": "Pending", **close_payload}
                # Position state will be updated by the fill event via process_order_update
                return True
            else:
                logger.error(f"Closing order submission failed for {instrument_symbol}. API Response: {result}")
                return False
        except TradovateError as e:
            logger.error(f"Tradovate API error placing closing order for {instrument_symbol}: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.exception(f"Unexpected error placing closing order for {instrument_symbol}: {e}")
            return False

    async def liquidate_all_positions(self, reason: Optional[str] = None) -> bool:
        """Request to close all open positions using Tradovate API."""
        positions_to_close = self.get_all_positions()
        if not positions_to_close:
            logger.info("No open positions to liquidate.")
            return True
        if not self.primary_account_id:
            logger.error("Cannot liquidate positions: Missing primary account ID.")
            return False

        logger.warning(f"Requesting liquidation of {len(positions_to_close)} positions. Reason: {reason or 'System Request'}")

        # Option 1: Use dedicated liquidate endpoint if available (Check OpenAPI spec for /position/liquidate)
        try:
            logger.info(f"Attempting bulk liquidation via position/liquidate for account {self.primary_account_id}...")
            # Assumes a position_liquidate method exists based on OpenAPI spec possibility
            # TODO: Verify API call/field: self.api.position_liquidate(accountId=self.primary_account_id) existence and parameters/response
            result = await self.api.position_liquidate(accountId=self.primary_account_id)
            # Check result structure - success might be indicated by status or specific fields
            logger.info(f"Bulk liquidation API call result: {result}") # Log result for debugging
            # Assuming success if no exception is raised and result looks okay (adapt based on actual response)
            # Note: Individual position closure and order cancellation might still happen via WS updates
            return True # Indicate the command was sent
        except AttributeError: # If self.api.position_liquidate doesn't exist
            logger.warning("position/liquidate endpoint not found or not implemented in library. Closing positions individually.")
        except TradovateError as e:
            logger.error(f"API error during bulk liquidation: {e}", exc_info=True)
            # Fall through to individual closure
        except Exception as e:
            logger.exception(f"Unexpected error during bulk liquidation: {e}")
            # Fall through to individual closure

        # Option 2: Fallback - Iterate and close individually
        logger.info("Closing positions individually...")
        success_flags = []
        for position in positions_to_close:
            success = await self.close_position(position.instrument, reason=f"Liquidation - {reason or 'System Request'}")
            success_flags.append(success)

        return all(success_flags) # Return True only if all individual closures were submitted successfully

    async def modify_position(
        self, instrument_symbol: str,
        stop_loss: Optional[float] = None, take_profit: Optional[float] = None
    ) -> bool:
        """Request to modify existing SL/TP ORDERS associated with a position."""
        position = self.get_position(instrument_symbol)
        if not position:
            logger.warning(f"Request to modify SL/TP for {instrument_symbol}, but no active position found.")
            return False

        logger.info(f"Request received to modify SL/TP for {instrument_symbol}: SL={stop_loss}, TP={take_profit}")

        success_sl = True # Assume success if not modified
        success_tp = True # Assume success if not modified

        # --- Modify Stop Loss Order ---
        if stop_loss is not None:
            sl_order_id = getattr(position, 'stop_loss_order_id', None)
            if sl_order_id and sl_order_id in self._active_orders:
                logger.info(f"Attempting to modify SL order {sl_order_id} for {instrument_symbol} to price {stop_loss:.5f}")
                try:
                    # Assumes order_modify_order exists
                    # Key parameters: orderId, stopPrice (for Stop orders)
                    modify_payload = {
                        "orderId": sl_order_id,
                        "stopPrice": stop_loss
                        # Potentially need orderQty, action, etc., if required by API - check spec
                    }
                    # TODO: Verify API call/field: self.api.order_modify_order(**modify_payload) for SL and response structure
                    result = await self.api.order_modify_order(**modify_payload)
                    # TODO: Check result structure for confirmation
                    logger.info(f"SL Order {sl_order_id} modify request result: {result}")
                    # Optimistically update local cache if modify seems successful (WS update confirms)
                    if result and result.get('orderId') == sl_order_id: # Basic check
                        self._active_orders[sl_order_id]['stopPrice'] = stop_loss
                        success_sl = True
                    else:
                        logger.error(f"Failed to modify SL order {sl_order_id} for {instrument_symbol}. API Response: {result}")
                        success_sl = False
                except TradovateError as e:
                    logger.error(f"API error modifying SL order {sl_order_id} for {instrument_symbol}: {e}", exc_info=True)
                    success_sl = False
                except Exception as e:
                    logger.exception(f"Unexpected error modifying SL order {sl_order_id} for {instrument_symbol}: {e}")
                    success_sl = False
            else:
                logger.warning(f"Cannot modify SL for {instrument_symbol}: No active SL order ID found/linked for this position.")
                # Optionally place a *new* SL order here if desired? Requires careful state management.
                success_sl = False # Indicate modification failed

        # --- Modify Take Profit Order ---
        if take_profit is not None:
            tp_order_id = getattr(position, 'take_profit_order_id', None)
            if tp_order_id and tp_order_id in self._active_orders:
                logger.info(f"Attempting to modify TP order {tp_order_id} for {instrument_symbol} to price {take_profit:.5f}")
                try:
                    # Assumes order_modify_order exists
                    # Key parameters: orderId, price (for Limit orders)
                    modify_payload = {
                        "orderId": tp_order_id,
                        "price": take_profit
                        # Potentially need orderQty, action, etc. - check spec
                    }
                    # TODO: Verify API call/field: self.api.order_modify_order(**modify_payload) for TP and response structure
                    result = await self.api.order_modify_order(**modify_payload)
                    # TODO: Check result structure for confirmation
                    logger.info(f"TP Order {tp_order_id} modify request result: {result}")
                    if result and result.get('orderId') == tp_order_id: # Basic check
                        self._active_orders[tp_order_id]['price'] = take_profit
                        success_tp = True
                    else:
                        logger.error(f"Failed to modify TP order {tp_order_id} for {instrument_symbol}. API Response: {result}")
                        success_tp = False
                except TradovateError as e:
                    logger.error(f"API error modifying TP order {tp_order_id} for {instrument_symbol}: {e}", exc_info=True)
                    success_tp = False
                except Exception as e:
                    logger.exception(f"Unexpected error modifying TP order {tp_order_id} for {instrument_symbol}: {e}")
                    success_tp = False
            else:
                logger.warning(f"Cannot modify TP for {instrument_symbol}: No active TP order ID found/linked for this position.")
                # Optionally place a *new* TP order here if desired?
                success_tp = False # Indicate modification failed

        return success_sl and success_tp # Return True only if all requested modifications succeeded

    # --- Update Handlers (Called by TradingSystem from WS messages) ---

    async def process_order_update(self, order_data: Dict[str, Any]):
        """Handle order updates received via WebSocket."""
        # Parses Tradovate order updates and handles fills to update position state.
        try:
            # TODO: Verify API call/field: WS Order update fields (`id`, `accountId`, `contractId`, `ordStatus`, `action`, `orderQty`, `cumQty`, `avgPx`, `timestamp`, `parentId`, `orderType`)
            order_id = order_data.get('id')
            if order_id is None:
                logger.warning(f"Received order update without ID: {order_data}")
                return
            order_id = int(order_id)
            account_id = order_data.get('accountId')
            contract_id = order_data.get('contractId')
            status = order_data.get('ordStatus')
            action = order_data.get('action') # 'Buy' or 'Sell'
            order_qty = float(order_data.get('orderQty', 0.0))
            cum_qty = float(order_data.get('cumQty', 0.0)) # Cumulative filled quantity
            avg_px = float(order_data.get('avgPx', 0.0)) # Average fill price
            timestamp_str = order_data.get('timestamp') # ISO 8601 format e.g., "2023-10-27T10:30:00.123Z"
            parent_id = order_data.get('parentId') # Example linking field
            order_type = order_data.get('orderType')

            if not all([account_id, contract_id, status, action, timestamp_str]):
                 logger.warning(f"Incomplete order update received for ID {order_id}: Missing key fields.")
                 return

            # Ensure this update is for the primary account we are managing
            if account_id != self.primary_account_id:
                 logger.debug(f"Ignoring order update for account {account_id} (managing {self.primary_account_id})")
                 return

            logger.debug(f"Processing order update: ID={order_id}, Status={status}, Action={action}, Qty={order_qty}, Filled={cum_qty}@{avg_px}")

            # --- Get previous state if known, preserving tick intent ---
            prev_order_state = self._active_orders.get(order_id, {})
            prev_cum_qty = float(prev_order_state.get('cumQty', 0.0))
            # Preserve tick intent from previous state if not present in new data
            tick_based_sl_tp_intent = prev_order_state.get('tick_based_sl_tp')
            if tick_based_sl_tp_intent and 'tick_based_sl_tp' not in order_data:
                 order_data['tick_based_sl_tp'] = tick_based_sl_tp_intent

            # --- Update active order cache ---
            self._active_orders[order_id] = order_data

            # --- Link new SL/TP orders from brackets or separate placement ---
            # If this order has a parent ID, try to link it to the position associated with the parent
            if parent_id and contract_id:
                symbol = await self._get_symbol_for_contract(contract_id)
                position = self._positions.get(symbol)
                if position:
                    if order_type == 'Stop' and action != position.direction: # Potential SL
                        if position.stop_loss_order_id is None or position.stop_loss_order_id != order_id:
                            position.stop_loss_order_id = order_id
                            logger.info(f"Linked new SL Order {order_id} (Parent: {parent_id}) to position {symbol}")
                    elif order_type == 'Limit' and action != position.direction: # Potential TP
                        if position.take_profit_order_id is None or position.take_profit_order_id != order_id:
                            position.take_profit_order_id = order_id
                            logger.info(f"Linked new TP Order {order_id} (Parent: {parent_id}) to position {symbol}")

            # --- Detect Fills --- 
            fill_qty = cum_qty - prev_cum_qty
            fill_price = avg_px # Use average price as fill price for now

            if fill_qty > 0.0000001: # Use small tolerance for float comparison
                logger.info(f"FILL DETECTED for Order {order_id}: Qty={fill_qty:.2f} @ AvgPx={fill_price:.5f} (CumQty: {cum_qty:.2f})")

                # Get instrument symbol (needs helper or cached contract details)
                symbol = await self._get_symbol_for_contract(contract_id)
                if not symbol:
                    logger.error(f"Cannot process fill for order {order_id}: Failed to get symbol for contract {contract_id}")
                    return # Cannot proceed without symbol

                fill_time = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))

                # --- Update Position State --- 
                existing_pos = self._positions.get(symbol)
                is_entry_fill = not existing_pos # True if this fill opens a position

                # --- Call helper to update internal position based on fill ---
                await self._update_position_from_fill(
                    symbol=symbol,
                    contract_id=contract_id,
                    fill_qty=fill_qty,
                    fill_price=fill_price,
                    fill_time=fill_time,
                    fill_action=action, # Action of the order that filled
                    order_id=order_id # Pass order ID for context
                )

                # --- Place separate SL/TP if this was an entry fill with tick intent --- 
                # Re-check intent from the potentially updated order_data in cache
                current_tick_intent = self._active_orders.get(order_id, {}).get('tick_based_sl_tp')
                if is_entry_fill and current_tick_intent:
                    logger.info(f"Entry order {order_id} filled, attempting to place separate SL/TP based on ticks for {symbol}")
                    await self._place_tick_based_sl_tp(
                        symbol=symbol,
                        contract_id=contract_id,
                        entry_fill_price=fill_price, # Use the fill price of the entry
                        tick_intent=current_tick_intent
                    )
                    # Remove the intent after attempting placement
                    self._active_orders.get(order_id, {}).pop('tick_based_sl_tp', None)
                elif is_entry_fill:
                     logger.debug(f"New position {symbol} opened by fill from order {order_id}. Waiting for potential linked SL/TP order updates if OSO was used.")


            # --- Check for Terminal Order Status --- 
            # Based on Tradovate API spec for ordStatus (e.g., Filled, Canceled, Rejected, Expired)
            terminal_statuses = ['Filled', 'Canceled', 'Rejected', 'Expired'] # Adjust based on exact statuses
            if status in terminal_statuses:
                logger.info(f"Order {order_id} reached terminal state: {status}. Removing from active orders.")
                # --- Clear links if SL/TP order is terminal --- 
                await self._clear_sl_tp_link(order_id)
                # Remove from cache AFTER potentially using it to trigger SL/TP
                self._active_orders.pop(order_id, None) 
                # TODO: Clean up any associated pending metadata if applicable
        except ValueError as verr:
            logger.error(f"Error converting order data types for ID {order_data.get('id')}: {verr} - Data: {order_data}", exc_info=True)
        except TypeError as terr:
            logger.error(f"Type error processing order data for ID {order_data.get('id')}: {terr} - Data: {order_data}", exc_info=True)
        except Exception as e:
            logger.exception(f"Error processing order update for ID {order_data.get('id')}: {e} - Data: {order_data}")

    async def _update_position_from_fill(self, symbol: str, contract_id: int, fill_qty: float, fill_price: float, fill_time: datetime.datetime, fill_action: str, order_id: int):
        """Updates the internal position state based on a detected fill."""
        existing_pos = self._positions.get(symbol)
        current_pos_qty = existing_pos.quantity if existing_pos else 0.0
        current_direction = existing_pos.direction if existing_pos else None

        # Calculate position change based on fill action and quantity
        pos_change = fill_qty if fill_action == 'Buy' else -fill_qty
        # Use math.fsum for potentially better float precision with small numbers
        new_pos_qty_signed = math.fsum([(current_pos_qty * (1 if current_direction == 'long' else -1 if current_direction == 'short' else 0)), pos_change])
        
        new_abs_qty = abs(new_pos_qty_signed)
        new_direction = 'long' if new_pos_qty_signed > 0.000001 else 'short' if new_pos_qty_signed < -0.000001 else None # Use tolerance

        if existing_pos:
            # --- Modify Existing Position --- 
            if new_direction is None: # Position closed
                logger.info(f"Fill from order {order_id} results in closure of position for {symbol}")
                closed_position = self._positions.pop(symbol, None)
                if closed_position:
                    fill_info = {
                        'order_id': order_id, 'fill_price': fill_price, 'fill_qty': fill_qty,
                        'fill_time': fill_time, 'action': fill_action 
                    }
                    await self._handle_position_closure(closed_position, fill_info) # Pass position object
                else:
                    logger.warning(f"Tried to close position {symbol} based on fill, but it was already removed.")
            elif new_direction == existing_pos.direction: # Adding to position
                logger.info(f"Fill from order {order_id} adds to existing {existing_pos.direction} position for {symbol}")
                # Calculate new average entry price
                old_total_value = existing_pos.entry_price * existing_pos.quantity
                fill_total_value = fill_price * fill_qty
                new_avg_entry = (old_total_value + fill_total_value) / new_abs_qty
                existing_pos.entry_price = new_avg_entry
                existing_pos.quantity = new_abs_qty
                # Optionally update entry_time if relevant (e.g., time of last addition)
                # existing_pos.entry_time = fill_time
            else: # Reducing position or reversing
                logger.info(f"Fill from order {order_id} reduces/reverses position for {symbol} to {new_direction} Qty:{new_abs_qty:.2f}")
                # Calculate realized P/L for the reduced portion (optional, requires cost basis logic)
                reduction_qty = existing_pos.quantity - new_abs_qty # How much was reduced/closed
                if reduction_qty > 0:
                     # TODO: Implement logic to calculate and record realized P/L for partial closes if needed.
                     pass
                     
                existing_pos.quantity = new_abs_qty
                existing_pos.direction = new_direction
                # If reversed, the entry price becomes the price of the reversing fill
                if current_direction is not None and new_direction != current_direction:
                     logger.info(f"Position {symbol} reversed. New entry price: {fill_price:.5f}")
                     existing_pos.entry_price = fill_price
                     existing_pos.entry_time = fill_time
                     # Reset meta? Or adjust? Depends on strategy. Resetting for now.
                     self._position_meta[symbol] = self._create_default_meta(existing_pos)
                     # TODO: Cancel old SL/TP orders associated with the previous direction? 
                     # await self._cancel_position_sl_tp(existing_pos) # Needs implementation


        elif new_direction is not None: # --- Open New Position --- 
            logger.info(f"Fill from order {order_id} opens new {new_direction} position for {symbol}")
            new_pos = Position(
                id=None, # Tradovate Position ID comes from position updates, link later if needed
                account_id=self.primary_account_id,
                contract_id=contract_id,
                instrument=symbol,
                direction=new_direction,
                quantity=new_abs_qty,
                entry_price=fill_price,
                entry_time=fill_time
                # Other fields (PL, SL/TP IDs) updated by position updates or linked orders
            )
            self._positions[symbol] = new_pos
            self._position_meta[symbol] = self._create_default_meta(new_pos)
            logger.info(f"Position {symbol} opened. Qty: {new_pos.quantity:.2f} @ {new_pos.entry_price:.5f}")

        else: # Fill resulted in zero quantity from zero start - should not happen but log if it does
             logger.warning(f"Fill processing resulted in zero quantity for {symbol} from zero start. Fill: Qty={fill_qty}, Px={fill_price}, Action={fill_action}")

    async def _place_tick_based_sl_tp(self, symbol: str, contract_id: int, entry_fill_price: float, tick_intent: Dict):
        """Calculates and places separate SL/TP orders based on tick offsets after an entry fill."""
        position = self._positions.get(symbol) 
        if not position:
            logger.error(f"Cannot place tick-based SL/TP for {symbol}: Position not found after fill.")
            return

        contract_details = await self._get_cached_contract_details(symbol=symbol)
        if not contract_details or contract_details.get('tickSize') is None:
            logger.error(f"Cannot place tick-based SL/TP for {symbol}: Failed to get tickSize from contract details.")
            return

        tick_size = contract_details['tickSize']
        if tick_size <= 0: # Prevent ZeroDivisionError
             logger.error(f"Cannot place tick-based SL/TP for {symbol}: Invalid tickSize ({tick_size}).")
             return
             
        sl_ticks = tick_intent.get('sl_ticks')
        tp_ticks = tick_intent.get('tp_ticks')
        calculated_sl_price = None
        calculated_tp_price = None

        # Calculate SL price
        if sl_ticks is not None:
            sl_delta = sl_ticks * tick_size
            raw_sl_price = entry_fill_price - sl_delta if position.direction == 'long' else entry_fill_price + sl_delta
            # Round to nearest tick using integer division and multiplication
            calculated_sl_price = round(raw_sl_price / tick_size) * tick_size
            logger.info(f"Calculated SL price for {symbol}: {calculated_sl_price:.5f} ({sl_ticks} ticks from {entry_fill_price:.5f}) - Raw: {raw_sl_price:.7f}")

        # Calculate TP price
        if tp_ticks is not None:
            tp_delta = tp_ticks * tick_size
            raw_tp_price = entry_fill_price + tp_delta if position.direction == 'long' else entry_fill_price - tp_delta
             # Round to nearest tick
            calculated_tp_price = round(raw_tp_price / tick_size) * tick_size
            logger.info(f"Calculated TP price for {symbol}: {calculated_tp_price:.5f} ({tp_ticks} ticks from {entry_fill_price:.5f}) - Raw: {raw_tp_price:.7f}")

        await self._place_separate_sl_tp(
            symbol=symbol,
            contract_id=contract_id,
            position_qty=position.quantity, # Use current position quantity
            position_direction=position.direction,
            stop_loss_price=calculated_sl_price,
            take_profit_price=calculated_tp_price
        )

    async def _place_separate_sl_tp(self, symbol: str, contract_id: int, position_qty: float, position_direction: str, stop_loss_price: Optional[float], take_profit_price: Optional[float]):
        """Places separate SL and/or TP orders using order_placeOrder."""
        # (This helper remains largely the same, just called from _place_tick_based_sl_tp or potentially elsewhere)
        if not stop_loss_price and not take_profit_price:
             return

        logger.info(f"Placing separate SL/TP for {symbol}: SL={stop_loss_price}, TP={take_profit_price}")
        position = self._positions.get(symbol) # Re-fetch position in case state changed slightly
        if not position: 
             logger.error(f"Cannot place separate SL/TP: Position {symbol} disappeared unexpectedly.")
             return
        # Use the LATEST position quantity when placing orders
        current_position_qty = position.quantity

        if current_position_qty <= 0:
             logger.warning(f"Attempted to place SL/TP for {symbol} but position quantity is zero or negative.")
             return

        sl_order_id = None
        tp_order_id = None

        # Place Stop Loss
        if stop_loss_price is not None:
            # Check if SL already exists for this position (race condition mitigation)
            if position.stop_loss_order_id and position.stop_loss_order_id in self._active_orders:
                 logger.warning(f"Attempted to place separate SL for {symbol}, but an active SL order ({position.stop_loss_order_id}) already exists.")
            else:
                sl_action = 'Sell' if position_direction == 'long' else 'Buy'
                sl_payload = {
                    "accountId": self.primary_account_id,
                    "contractId": contract_id,
                    "action": sl_action,
                    "orderQty": current_position_qty, # Use current quantity
                    "orderType": "Stop",
                    "stopPrice": stop_loss_price,
                    # "text": f"SL {symbol}"[:50]
                }
                try:
                    # TODO: Verify API call/field: self.api.order_placeOrder for separate SL
                    result = await self.api.order_placeOrder(**sl_payload)
                    sl_order_id = result.get('orderId')
                    if sl_order_id:
                        sl_order_id_int = int(sl_order_id)
                        logger.info(f"Separate SL Order {sl_order_id_int} placed for {symbol} at {stop_loss_price}")
                        self._active_orders[sl_order_id_int] = {"id": sl_order_id_int, "ordStatus": "Pending", **sl_payload}
                        # Link immediately
                        if symbol in self._positions: # Check position still exists
                             self._positions[symbol].stop_loss_order_id = sl_order_id_int 
                    else:
                        logger.error(f"Failed to place separate SL order for {symbol}. Response: {result}")
                except Exception as e:
                    logger.exception(f"Error placing separate SL order for {symbol}: {e}")

        # Place Take Profit
        if take_profit_price is not None:
            # Check if TP already exists for this position
            if position.take_profit_order_id and position.take_profit_order_id in self._active_orders:
                 logger.warning(f"Attempted to place separate TP for {symbol}, but an active TP order ({position.take_profit_order_id}) already exists.")
            else:
                tp_action = 'Sell' if position_direction == 'long' else 'Buy'
                tp_payload = {
                    "accountId": self.primary_account_id,
                    "contractId": contract_id,
                    "action": tp_action,
                    "orderQty": current_position_qty, # Use current quantity
                    "orderType": "Limit",
                    "price": take_profit_price,
                    # "text": f"TP {symbol}"[:50]
                }
                try:
                    # TODO: Verify API call/field: self.api.order_placeOrder for separate TP
                    result = await self.api.order_placeOrder(**tp_payload)
                    tp_order_id = result.get('orderId')
                    if tp_order_id:
                        tp_order_id_int = int(tp_order_id)
                        logger.info(f"Separate TP Order {tp_order_id_int} placed for {symbol} at {take_profit_price}")
                        self._active_orders[tp_order_id_int] = {"id": tp_order_id_int, "ordStatus": "Pending", **tp_payload}
                        # Link immediately
                        if symbol in self._positions: # Check position still exists
                            self._positions[symbol].take_profit_order_id = tp_order_id_int 
                    else:
                        logger.error(f"Failed to place separate TP order for {symbol}. Response: {result}")
                except Exception as e:
                    logger.exception(f"Error placing separate TP order for {symbol}: {e}")

    async def _clear_sl_tp_link(self, order_id_to_clear: int):
        """Finds any position linked to the given SL/TP order ID and clears the link."""
        for symbol, position in self._positions.items():
            if getattr(position, 'stop_loss_order_id', None) == order_id_to_clear:
                logger.debug(f"Clearing SL link for order {order_id_to_clear} from position {symbol}")
                position.stop_loss_order_id = None
                return # Assume only one position links to an order
            if getattr(position, 'take_profit_order_id', None) == order_id_to_clear:
                logger.debug(f"Clearing TP link for order {order_id_to_clear} from position {symbol}")
                position.take_profit_order_id = None
                return

    # Placeholder for symbol lookup - Implement using API or cache
    async def _get_symbol_for_contract(self, contract_id: int) -> Optional[str]:
        # TODO: Implement reliable symbol lookup
        # Option 1: Check a pre-populated contract details cache
        # Option 2: Call self.api.contract_item(id=contract_id) - potential performance impact
        logger.warning(f"Symbol lookup not implemented. Using placeholder lookup for contract {contract_id}")
        try:
            # Assumes self.api has a method like contract_item based on OpenAPI spec
            contract_details = await self.api.contract_item(id=contract_id)
            return contract_details.get('name') if contract_details else None
        except TradovateError as e:
            logger.error(f"API error fetching contract details for {contract_id}: {e}")
            return None
        except Exception as e:
            logger.exception(f"Unexpected error fetching contract details for {contract_id}: {e}")
            return None

    async def process_position_update(self, position_data: Dict[str, Any]):
        """Handle position updates received via WebSocket."""
        # Parses Tradovate position updates to reconcile internal state (primarily P/L).
        # Fill updates from process_order_update are usually the primary driver for quantity/entry changes.
        try:
            pos_id = position_data.get('id')
            account_id = position_data.get('accountId')
            contract_id = position_data.get('contractId')
            net_pos = float(position_data.get('netPos', 0.0))
            avg_entry = float(position_data.get('avgEntryPrice', 0.0))
            unrealized_pl = float(position_data.get('unrealizedPL', 0.0))
            realized_pl = float(position_data.get('realizedPL', 0.0))
            # timestamp = position_data.get('timestamp') # Optional: Use timestamp if needed

            if pos_id is None or account_id is None or contract_id is None:
                logger.warning(f"Incomplete position update received: {position_data}")
                return

            # Ignore if not for the primary account
            if account_id != self.primary_account_id:
                return

            # Get symbol
            symbol = await self._get_symbol_for_contract(contract_id)
            if not symbol:
                logger.error(f"Cannot process position update for ID {pos_id}: Failed to get symbol for contract {contract_id}")
                return

            logger.debug(f"Processing position update: Symbol={symbol}, ID={pos_id}, NetPos={net_pos}, AvgEntry={avg_entry}, UnrPL={unrealized_pl}")

            existing_pos = self._positions.get(symbol)

            # --- Update existing position --- 
            if existing_pos:
                updated = False
                new_direction = 'long' if net_pos > 0 else 'short' if net_pos < 0 else None
                new_quantity = abs(net_pos)

                # Reconcile core position state (Order fills should be primary source, but update if different)
                if abs(existing_pos.quantity - new_quantity) > 0.0001: # Allow for float tolerance
                    logger.warning(f"Position update quantity ({new_quantity}) differs from tracked quantity ({existing_pos.quantity}) for {symbol}. Reconciling.")
                    existing_pos.quantity = new_quantity
                    updated = True
                if existing_pos.direction != new_direction:
                    logger.warning(f"Position update direction ({new_direction}) differs from tracked direction ({existing_pos.direction}) for {symbol}. Reconciling.")
                    existing_pos.direction = new_direction
                    updated = True
                if abs(existing_pos.entry_price - avg_entry) > 0.00001: # Allow for float tolerance
                    logger.warning(f"Position update entry price ({avg_entry}) differs from tracked price ({existing_pos.entry_price}) for {symbol}. Reconciling.")
                    existing_pos.entry_price = avg_entry
                    updated = True

                # Update P/L fields
                if existing_pos.unrealized_pl != unrealized_pl:
                    existing_pos.unrealized_pl = unrealized_pl
                    updated = True
                if existing_pos.realized_pl != realized_pl:
                    existing_pos.realized_pl = realized_pl
                    updated = True

                if updated:
                    logger.info(f"Position reconciled/updated via WS: {symbol} Qty={existing_pos.quantity} Dir={existing_pos.direction} UnrPL={existing_pos.unrealized_pl:.2f}")

                # --- Handle potential missed closure --- 
                if new_quantity < 0.0001 and existing_pos.quantity >= 0.0001: # If update shows flat but we thought it was open
                    logger.warning(f"Position update indicates {symbol} is flat, but it was tracked as open. Handling potential missed closure.")
                    # Need details about the closing fill if possible, otherwise log with limited info
                    await self._handle_position_closure(symbol, {'reason': 'Position update reconciliation'}) # Pass minimal details

            # --- Handle update for untracked position --- 
            elif net_pos != 0: # Position exists according to update, but not tracked locally
                logger.warning(f"Received position update for untracked position: {symbol} (ID: {pos_id}). Creating local representation.")
                # This might happen if fills were missed or system started mid-trade
                new_pos = Position(
                    id=int(pos_id),
                    account_id=self.primary_account_id,
                    contract_id=int(contract_id),
                    instrument=symbol,
                    direction='long' if net_pos > 0 else 'short',
                    quantity=abs(net_pos),
                    entry_price=avg_entry,
                    unrealized_pl=unrealized_pl,
                    realized_pl=realized_pl
                )
                self._positions[symbol] = new_pos
                self._position_meta[symbol] = self._create_default_meta(new_pos)
                # TODO: Attempt to find associated active SL/TP orders for this new position
        except ValueError as verr:
            logger.error(f"Error converting position data types for ID {position_data.get('id')}: {verr} - Data: {position_data}", exc_info=True)
        except TypeError as terr:
            logger.error(f"Type error processing position data for ID {position_data.get('id')}: {terr} - Data: {position_data}", exc_info=True)
        except Exception as e:
            logger.exception(f"Error processing position update for ID {position_data.get('id')}: {e} - Data: {position_data}")

    async def _handle_position_closure(self, closed_position: Position, fill_info: Optional[Dict] = None):
        """Internal handler when a position is confirmed closed by a fill or reconciliation."""
        # This is triggered by process_order_update (on closing fill) or process_position_update (on reconciliation)
        # Assumes the position object passed in is the one to be processed (already removed from self._positions elsewhere)
        symbol = closed_position.instrument
        meta = self._position_meta.pop(symbol, {}) # Remove associated meta

        if not closed_position:
            logger.warning(f"_handle_position_closure called for {symbol}, but position object was None.")
            return # Should not happen if called correctly

        logger.info(f"Handling closure of position for {symbol} (Contract: {closed_position.contract_id})...")

        # --- Determine Exit Details --- 
        exit_price = None
        exit_time = datetime.datetime.now(datetime.timezone.utc) # Fallback time
        exit_reason = "Unknown"
        final_pl = closed_position.unrealized_pl # Use last known unrealized P/L as a fallback if fill info missing
        fill_based_exit = False

        if fill_info:
            exit_price = fill_info.get('fill_price')
            exit_time = fill_info.get('fill_time', exit_time)
            exit_reason = fill_info.get('reason', f"Fill Order {fill_info.get('order_id')}") # Use fill order ID if available
            fill_based_exit = True
            # TODO: Calculate realized P/L more accurately based on fills if possible
            # Tradovate position object might have final realizedPL, but it might lag the fill.
            # For now, recalculate based on fill price.
            if exit_price is not None:
                # Calculate P/L using tick value from cached contract details
                contract_details = await self._get_cached_contract_details(symbol=symbol)
                tick_value = contract_details.get('tickValue', 0.0) if contract_details else 0.0
                tick_size = contract_details.get('tickSize', 0.0) if contract_details else 0.0
                if tick_size > 0 and tick_value > 0:
                    price_diff = exit_price - closed_position.entry_price if closed_position.direction == 'long' else closed_position.entry_price - exit_price
                    num_ticks = round(price_diff / tick_size) # Calculate number of ticks gained/lost
                    final_pl = num_ticks * tick_value * closed_position.quantity # Total P/L
                else:
                    logger.warning(f"Cannot calculate P/L accurately for {symbol}: Missing tickValue/tickSize in contract details. Using last unrealized P/L: {final_pl}")
            else:
                logger.warning(f"Closing fill info for {symbol} provided, but missing fill_price. Using last unrealized P/L: {final_pl}")

        else:
            # Closure triggered by reconciliation (e.g., position update showed netPos=0)
            logger.warning(f"Position closure for {symbol} handled via reconciliation. Exit details may be approximate.")
            exit_reason = "Position Update Reconciliation"
            # Attempt to use the position's average exit price if available from reconciliation? (Difficult)
            # For now, stick with last known P/L as best estimate in this case
            final_pl = closed_position.unrealized_pl
            exit_price = closed_position.entry_price # Cannot determine exit price from reconciliation alone
            logger.warning(f"Using entry price as placeholder exit price for reconciled closure of {symbol}")


        # Ensure essential exit details are present
        if exit_price is None:
            logger.error(f"Cannot log trade for {symbol}: Missing exit price.")
            # Should we still try to log with estimated P/L?
            return

        # Calculate final pips based on selected exit price - REMOVED Pip Calculation
        # final_pips = self._calculate_pips(position.entry_price, exit_price, position.direction, symbol)
        final_pips = 0.0 # Placeholder, as pips are less relevant than PL in ticks/currency

        # Create Trade object
        trade = Trade(
            id=str(closed_position.id) if closed_position.id else None, # Use position ID if available for linking
            instrument=symbol,
            direction=closed_position.direction,
            quantity=meta.get('initial_quantity', closed_position.quantity), # Log initial quantity from meta
            entry_price=closed_position.entry_price,
            entry_time=closed_position.entry_time or exit_time, # Use best available entry time
            exit_price=exit_price,
            exit_time=exit_time,
            profit_loss=final_pl or 0.0,
            profit_loss_pips=final_pips,
            strategy_type=getattr(closed_position, 'strategy_type', None), # Get strategy from Position object if set
            exit_reason=exit_reason,
            features=meta # Store final position metadata
        )

        # Add to history and notify callbacks
        self.trade_history.append(trade)
        logger.info(f"TRADE LOGGED: {trade.instrument} {trade.direction} Qty:{trade.quantity:.2f} Entry:{trade.entry_price:.5f} Exit:{trade.exit_price:.5f} P/L:${trade.profit_loss:.2f} Pips:{trade.profit_loss_pips:.1f} Reason:'{trade.exit_reason}'.")
        for callback in self._trade_callbacks:
            try:
                 # Allow callback to be sync or async
                 result = callback(trade)
                 if asyncio.iscoroutine(result):
                      # Schedule async callback without awaiting it here
                      asyncio.create_task(result)
            except Exception as cb_err:
                 logger.exception(f"Error in trade callback {callback.__name__}: {cb_err}")

        # Cancel any orphaned associated SL/TP orders if they weren't cancelled automatically
        # Check _active_orders directly, as position object links might be cleared already
        sl_order_id_to_cancel = getattr(closed_position, 'stop_loss_order_id', None)
        tp_order_id_to_cancel = getattr(closed_position, 'take_profit_order_id', None)

        if sl_order_id_to_cancel and sl_order_id_to_cancel in self._active_orders:
            logger.info(f"Cancelling potentially orphaned SL order {sl_order_id_to_cancel} for closed position {symbol}")
            try:
                await self.api.order_cancel_order(orderId=sl_order_id_to_cancel)
                self._active_orders.pop(sl_order_id_to_cancel, None) # Clean cache
            except Exception as cancel_err:
                 logger.error(f"Failed to cancel orphaned SL order {sl_order_id_to_cancel}: {cancel_err}")

        if tp_order_id_to_cancel and tp_order_id_to_cancel in self._active_orders:
            logger.info(f"Cancelling potentially orphaned TP order {tp_order_id_to_cancel} for closed position {symbol}")
            try:
                await self.api.order_cancel_order(orderId=tp_order_id_to_cancel)
                self._active_orders.pop(tp_order_id_to_cancel, None) # Clean cache
            except Exception as cancel_err:
                 logger.error(f"Failed to cancel orphaned TP order {tp_order_id_to_cancel}: {cancel_err}")

    # Placeholder for fetching contract details - Implement using API/cache
    async def _get_cached_contract_details(self, symbol: Optional[str] = None, contract_id: Optional[int] = None) -> Optional[Dict]:
        """Gets contract details from cache or fetches via API. Requires symbol OR contract_id. Cache has a TTL."""
        cache_key = symbol if symbol else str(contract_id)
        current_time = time.monotonic() # Use monotonic clock for intervals

        if not cache_key:
            logger.error("Cannot get contract details: Must provide symbol or contract_id")
            return None

        # Check cache and TTL
        cached_entry = self._contract_details_cache.get(cache_key)
        if cached_entry:
            details, timestamp = cached_entry
            if current_time - timestamp < CACHE_TTL:
                 logger.debug(f"Returning cached contract details for {cache_key}")
                 return details
            else:
                 logger.info(f"Cache expired for {cache_key}. Fetching fresh details.")
                 # Remove expired entry
                 del self._contract_details_cache[cache_key]

        logger.info(f"Contract details for {cache_key} not in cache or expired. Fetching from API...")
        contract = None
        try:
            if symbol:
                # TODO: Verify API call: self.api.contract_find(name=symbol)
                contract = await self.api.contract_find(name=symbol)
                if not contract:
                    # Consider if suggest is reliable enough - might return wrong contract
                    logger.warning(f"contract_find failed for {symbol}, trying contract_suggest as fallback.")
                    # TODO: Verify API call: self.api.contract_suggest(t=symbol, l=1)
                    suggestions = await self.api.contract_suggest(t=symbol, l=1)
                    if suggestions: contract = suggestions[0]
            elif contract_id:
                # TODO: Verify API call: self.api.contract_item(id=contract_id)
                contract = await self.api.contract_item(id=contract_id)

            if contract and contract.get('id'):
                contract_id_str = str(contract['id'])
                # Use symbol (name) as primary cache key if available
                primary_key = contract.get('name', contract_id_str)
                cache_value = (contract, current_time) # Store tuple (details, timestamp)

                self._contract_details_cache[primary_key] = cache_value
                # Also cache by ID if symbol was used for lookup and key is different
                if primary_key != contract_id_str:
                     self._contract_details_cache[contract_id_str] = cache_value
                logger.info(f"Cached contract details for {primary_key} (ID: {contract_id_str}) with TTL.")
                return contract
            else:
                 logger.error(f"Failed to fetch valid contract details for {cache_key}. API returned: {contract}")
                 return None

        except TradovateError as e:
            logger.error(f"API error fetching contract details for {cache_key}: {e}")
            return None
        except Exception as e:
            logger.exception(f"Unexpected error fetching contract details for {cache_key}: {e}")
            return None

    # --- Data Accessors for Strategies ---
    async def get_account_and_contract_info(self, instrument_symbol: str) -> Tuple[Optional[Account], Optional[Dict]]:
        """Provides current account state and contract details for sizing/rules."""
        # TODO: Fetch account info more reliably - potentially cache in PM or get from TradingSystem
        account = None
        if self.primary_account_id:
            logger.debug(f"Fetching latest account info for {self.primary_account_id} (for sizing/rules)")
            try:
                # Assumes an account_item method or similar exists
                # TODO: Verify API call: self.api.account_item(id=self.primary_account_id)
                acc_data = await self.api.account_item(id=self.primary_account_id)
                if acc_data:
                    # TODO: Reuse the parsing logic from TradingSystem._process_account_update_async if possible
                    # Or create an Account object directly
                    account = Account(
                        id=int(acc_data.get('id')),\
                        name=acc_data.get('name'),\
                        account_type=acc_data.get('accountType'),\
                        base_currency=acc_data.get('currency', {}).get('name', 'USD'),\
                        balance=float(acc_data.get('balance', 0.0)),\
                        equity=float(acc_data.get('totalCashValue', 0.0)),\
                        realized_pl=float(acc_data.get('realizedPL', 0.0)),\
                        unrealized_pl=float(acc_data.get('unrealizedPL', 0.0)),\
                        margin_used=float(acc_data.get('totalInitialMargin', 0.0)),\
                        margin_available=float(acc_data.get('buyingPower', 0.0))\
                    )
            except Exception as e:
                logger.error(f"Failed to fetch account info for sizing: {e}", exc_info=True)

        contract_details = await self._get_cached_contract_details(symbol=instrument_symbol)
        return account, contract_details