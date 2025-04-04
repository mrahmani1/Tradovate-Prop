# core/strategy_engine.py
"""
Manages and executes trading strategies based on market data and signals.
Integrates with PositionManager and optionally ModelManager.
"""
import asyncio
import datetime
import logging
import math
from typing import Dict, List, Optional, Any, Tuple, Callable, Union, Type

# Use absolute imports
from tradelocker.api import TradeLockerAPI
from core.position_manager import PositionManager
from utils.data_classes import PriceSnapshot, TradingSignal, Position
from utils.helpers import calculate_pip_distance, calculate_position_size, get_instrument_details # Use helpers
from ml_models.model_manager import ModelManager # Keep import
from config import get_config_value, is_strategy_enabled, get_strategy_params # Use config helpers

logger = logging.getLogger(__name__)

# --- Base Strategy Class ---
class Strategy:
    """Abstract base class for trading strategies."""
    def __init__(self, name: str, api: TradeLockerAPI, position_manager: PositionManager, config: Dict[str, Any]):
        self.name = name
        self.api = api
        self.position_manager = position_manager
        self.config = config
        # Get strategy-specific parameters from the main config
        self.params = get_strategy_params(config, name)
        self.enabled = is_strategy_enabled(config, name)
        self._instruments: List[str] = [] # Instruments this strategy instance trades
        logger.info(f"Strategy '{name}' initialized. Enabled: {self.enabled}. Params: {self.params}")

    async def initialize(self, instruments: List[str]):
        """Initialize strategy with relevant instruments."""
        self._instruments = [inst for inst in instruments if self._is_relevant_instrument(inst)]
        logger.info(f"Strategy '{self.name}' will monitor instruments: {self._instruments}")
        # Subclass specific initialization can go here
        await self._initialize()

    async def _initialize(self):
        """Placeholder for subclass specific async initialization."""
        pass

    def _is_relevant_instrument(self, instrument: str) -> bool:
        """Check if the strategy should trade this instrument (based on config or type)."""
        # Default: trade all instruments passed during initialization
        # Subclasses can override this (e.g., only trade forex)
        return True

    async def on_price_update(self, instrument: str, price_snapshot: PriceSnapshot, market_data: Dict):
        """Process a new price snapshot for a relevant instrument."""
        # To be implemented by subclasses
        pass

    async def on_signal(self, instrument: str, signal: TradingSignal):
        """Process an external signal (e.g., from ML model)."""
        # Default behavior: ignore external signals
        pass

    async def update(self):
        """Periodic update call for time-based logic or checks."""
        # To be implemented by subclasses if needed
        pass

    async def generate_signal(
        self, instrument: str, signal_type: str, direction: str, confidence: float,
        stop_loss_pips: Optional[float] = None, # TODO: Rename to stop_loss_ticks if using ticks
        take_profit_pips: Optional[float] = None, # TODO: Rename to take_profit_ticks if using ticks
        features: Optional[Dict] = None
    ) -> Optional[TradingSignal]:
        """Helper to create and potentially execute a trading signal."""
        if not self.enabled: return None

        logger.debug(f"Strategy '{self.name}' evaluating signal: {instrument} {signal_type} {direction} (Conf: {confidence:.2f})")

        # --- Store Ticks, Don't Calculate Prices Here ---
        # Let PositionManager calculate prices based on entry fill if needed
        stop_loss_ticks = stop_loss_pips # Rename variable for clarity if input is ticks
        take_profit_ticks = take_profit_pips # Rename variable for clarity if input is ticks
        sl_price, tp_price = None, None # Initialize prices as None

        # --- Create Signal Object (without calculated prices) ---
        signal = TradingSignal(
            instrument=instrument,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            signal_type=signal_type,
            direction=direction,
            confidence=confidence,
            strategy_name=self.name,
            suggested_stop_loss=sl_price, # Pass None
            suggested_take_profit=tp_price, # Pass None
            features=features or {}
        )
        # Store ticks instead of pips
        signal.features['sl_ticks'] = stop_loss_ticks
        signal.features['tp_ticks'] = take_profit_ticks

        # --- Execute Signal (Delegate to Position Manager) ---
        # Position manager handles rule checks and position sizing
        min_confidence = self.params.get('min_confidence_to_trade', 0.6)
        if confidence >= min_confidence:
            logger.info(f"Strategy '{self.name}' generating signal for {instrument}: {signal_type} {direction} (Conf: {confidence:.2f}). Requesting position open.")

            # --- Position Sizing ---
            # TODO: Implement robust position sizing based on ticks and tick value
            # This requires reliable Account data and contract details (tickValue)
            pos_size = 0.01 # Default minimum size
            try:
                account, contract_details = await self.position_manager.get_account_and_contract_info(instrument)
                if account and contract_details and stop_loss_ticks is not None and stop_loss_ticks > 0:
                    risk_percent = get_config_value(self.config, 'RISK_MANAGEMENT.max_risk_per_trade_percent', 1.0)
                    account_balance = account.balance # Or use equity?
                    risk_amount = account_balance * (risk_percent / 100.0)
                    
                    sl_distance_ticks = stop_loss_ticks
                    tick_value = contract_details.get('tickValue')

                    if tick_value is None or tick_value <= 0:
                         logger.warning(f"Cannot calculate position size for {instrument}: Missing or invalid tickValue in contract details.")
                    else:
                        risk_per_contract = sl_distance_ticks * tick_value
                        if risk_per_contract > 0:
                            calculated_size = risk_amount / risk_per_contract
                            # Adjust for instrument properties (e.g., lot size, rounding)
                            # For now, just floor it to avoid exceeding risk
                            # TODO: Implement proper rounding/step based on contract rules if necessary
                            calculated_size = max(0, math.floor(calculated_size)) # Use math.floor
                            pos_size = float(calculated_size)
                            logger.info(f"Calculated position size for {instrument}: {pos_size} (RiskAmt: {risk_amount:.2f}, RiskPerCont: {risk_per_contract:.2f}, SL Ticks: {sl_distance_ticks})")
                        else:
                            logger.warning(f"Cannot calculate position size for {instrument}: Risk per contract is zero or negative (SL Ticks: {sl_distance_ticks}, TickValue: {tick_value}). Using default size.")
                    
                    # --- Adjust for Min/Max Order Size ---
                    min_order_qty = float(contract_details.get('minOrderQty', 0.01))
                    pos_size = max(min_order_qty, pos_size)
                    
                elif not account:
                    logger.warning(f"Cannot calculate position size for {instrument}: Account info not available. Using default size.")
                elif not contract_details:
                     logger.warning(f"Cannot calculate position size for {instrument}: Contract details not available. Using default size.")
                elif stop_loss_ticks is None or stop_loss_ticks <= 0:
                     logger.warning(f"Cannot calculate position size for {instrument}: Invalid stop loss distance ({stop_loss_ticks} ticks). Using default size.")

            except Exception as sizing_err:
                logger.exception(f"Error during position sizing calculation for {instrument}. Using default size. Error: {sizing_err}")
                pos_size = 0.01 # Fallback on any calculation error

            if pos_size < 0.01: # Final safety check 
                 logger.error(f"Calculated position size {pos_size} is too small for {instrument}. Aborting open request.")
                 return signal # Return signal without execution

            # --- Pass None for SL/TP Prices ---
            # PositionManager needs modification to handle ticks from signal.features or separate args
            await self.position_manager.open_position(
                instrument_symbol=instrument,
                direction=direction,
                quantity=pos_size,
                order_type='market', # Default to market, strategy can specify others
                stop_loss=None, # Pass None
                take_profit=None, # Pass None
                strategy_type=self.name,
                # TODO: Pass SL/TP ticks via a different mechanism if open_position signature changes
                # Example: open_position(..., sl_ticks=stop_loss_ticks, tp_ticks=take_profit_ticks)
                # For now, assume PM might look at signal features or needs modification
            )
            # Return the signal even if execution fails (for logging/analysis)
            return signal
        else:
            logger.debug(f"Signal confidence {confidence:.2f} below threshold {min_confidence} for {instrument}. Signal not executed.")
            return None # Confidence too low


# --- Example Concrete Strategy ---
# Create separate files for each strategy (e.g., strategies/macd_volume.py)
# For now, including one example here based on original config

class MacdVolumeStrategy(Strategy):
    """Example Strategy: MACD Crossover with Volume Confirmation."""

    def __init__(self, api: TradeLockerAPI, position_manager: PositionManager, config: Dict[str, Any]):
        super().__init__("MACD_VOLUME", api, position_manager, config)
        # Parameters loaded in base class __init__ using get_strategy_params
        self.fast_period = self.params.get('fast_period', 12)
        self.slow_period = self.params.get('slow_period', 26)
        self.signal_period = self.params.get('signal_period', 9)
        self.volume_threshold = self.params.get('volume_threshold', 0.1) # e.g., 10% increase
        self.stop_loss_pips = self.params.get('stop_loss_pips', 25)
        self.take_profit_pips = self.params.get('take_profit_pips', 50)
        # Add other params from config as needed

        self._last_macd: Dict[str, Tuple[Optional[float], Optional[float]]] = {} # Store previous MACD/Signal

    async def on_price_update(self, instrument: str, price_snapshot: PriceSnapshot, market_data: Dict):
        """Process price update for MACD crossover."""
        if instrument not in self._instruments: return # Only process relevant instruments

        indicators = market_data.get('indicators', {})
        bars = market_data.get('bars', [])
        if len(bars) < self.slow_period + self.signal_period: return # Need enough data

        # --- Calculate MACD ---
        # Use a robust library or helper function
        # from utils.helpers import calculate_macd
        # macd_result = calculate_macd(closes, self.fast_period, self.slow_period, self.signal_period)
        # For now, assume indicators dict contains 'macd', 'macd_signal', 'macd_hist'
        macd_line = indicators.get('macd')
        signal_line = indicators.get('macd_signal')
        # volume = indicators.get('volume') # Need volume indicator
        # avg_volume = indicators.get('volume_sma_20') # Example

        if macd_line is None or signal_line is None: return # Cannot proceed

        # --- Crossover Logic ---
        prev_macd, prev_signal = self._last_macd.get(instrument, (None, None))
        self._last_macd[instrument] = (macd_line, signal_line) # Update history

        if prev_macd is None or prev_signal is None: return # Need previous values for crossover

        signal = None
        confidence = 0.7 # Base confidence for crossover

        # Bullish Crossover (MACD crosses above Signal)
        if prev_macd <= prev_signal and macd_line > signal_line:
            logger.debug(f"MACD Bullish Crossover detected for {instrument}")
            # Optional: Volume confirmation
            # if volume and avg_volume and volume > avg_volume * (1 + self.volume_threshold):
            #     confidence += 0.1 # Increase confidence with volume
            # else:
            #     logger.debug(f"MACD Bullish Crossover - Volume confirmation failed for {instrument}")
            #     return # Skip signal if volume confirmation required

            signal = await self.generate_signal(
                instrument=instrument,
                signal_type='buy',
                direction='long',
                confidence=confidence,
                stop_loss_pips=self.stop_loss_pips,
                take_profit_pips=self.take_profit_pips,
                features={'macd': macd_line, 'signal': signal_line}
            )

        # Bearish Crossover (MACD crosses below Signal)
        elif prev_macd >= prev_signal and macd_line < signal_line:
            logger.debug(f"MACD Bearish Crossover detected for {instrument}")
            # Optional: Volume confirmation
            # if volume and avg_volume and volume > avg_volume * (1 + self.volume_threshold):
            #     confidence += 0.1
            # else:
            #     logger.debug(f"MACD Bearish Crossover - Volume confirmation failed for {instrument}")
            #     return

            signal = await self.generate_signal(
                instrument=instrument,
                signal_type='sell',
                direction='short',
                confidence=confidence,
                stop_loss_pips=self.stop_loss_pips,
                take_profit_pips=self.take_profit_pips,
                features={'macd': macd_line, 'signal': signal_line}
            )

        # Note: generate_signal handles execution via PositionManager


# --- Strategy Engine ---
class StrategyEngine:
    """Manages and executes trading strategies."""

    def __init__(
        self,
        api: TradeLockerAPI,
        position_manager: PositionManager,
        config: Dict[str, Any],
        model_manager: Optional[ModelManager] = None
    ):
        """Initialize strategy engine."""
        self.api = api
        self.position_manager = position_manager
        self.config = config
        self.model_manager = model_manager # Can be None
        self.strategies: Dict[str, Strategy] = {} # name -> Strategy instance
        self._market_data: Dict[str, Dict] = {} # Shared market data cache (symbol -> {'bars':[], 'indicators':{}})

        self._register_strategies()

    def _register_strategies(self):
        """Register strategies based on configuration."""
        logger.info("Registering strategies...")
        # Example: Dynamically load strategies based on config or predefined list
        # Strategy classes should ideally be imported or discovered dynamically
        available_strategies: Dict[str, Type[Strategy]] = {
             "MACD_VOLUME": MacdVolumeStrategy,
             # "ADAPTIVE": AdaptiveStrategy, # Add other strategy classes here
             # "MEAN_REVERSION": MeanReversionStrategy,
        }

        for name, strategy_cls in available_strategies.items():
             # Check if strategy is enabled in the main config
             if is_strategy_enabled(self.config, name):
                  try:
                       # Instantiate strategy, passing dependencies
                       strategy_instance = strategy_cls(self.api, self.position_manager, self.config)
                       self.strategies[strategy_instance.name] = strategy_instance
                  except Exception as e:
                       logger.error(f"Failed to instantiate strategy '{name}': {e}", exc_info=True)
             else:
                  logger.info(f"Strategy '{name}' is disabled in configuration.")

        if not self.strategies:
             logger.warning("No strategies were enabled or registered.")

    async def initialize(self, instruments: List[str]):
        """Initialize all registered strategies."""
        logger.info("Initializing Strategy Engine...")
        init_tasks = [
            strategy.initialize(instruments) for strategy in self.strategies.values()
        ]
        results = await asyncio.gather(*init_tasks, return_exceptions=True)
        success = True
        for i, result in enumerate(results):
             name = list(self.strategies.keys())[i]
             if isinstance(result, Exception):
                  logger.error(f"Error initializing strategy '{name}': {result}", exc_info=result)
                  success = False
             elif result is False: # Strategy explicitly returned False
                  logger.warning(f"Strategy '{name}' failed to initialize.")
                  success = False
        logger.info(f"Strategy Engine initialization {'successful' if success else 'partially failed'}.")
        return success

    async def process_price_update(self, instrument: str, price_snapshot: PriceSnapshot):
        """Process a price update, update shared market data, and notify strategies."""
        # Update shared market data cache (used by strategies)
        if instrument not in self._market_data:
            self._market_data[instrument] = {'bars': [], 'indicators': {}, 'current_price': None}
        self._market_data[instrument]['current_price'] = price_snapshot
        # TODO: Add bar aggregation and indicator calculation logic here or in TradingSystem
        # For now, assume TradingSystem updates self._market_data[instrument]['indicators'] and ['bars']

        # Notify relevant strategies
        tasks = []
        for strategy in self.strategies.values():
            if instrument in strategy._instruments: # Check if strategy trades this instrument
                tasks.append(asyncio.create_task(
                    strategy.on_price_update(instrument, price_snapshot, self._market_data[instrument])
                ))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True) # Allow strategies to process concurrently


    async def process_signal(self, instrument: str, signal: TradingSignal):
        """Process an external signal (e.g., from ML) by notifying strategies."""
        logger.debug(f"Processing external signal for {instrument}: {signal.signal_type}")
        tasks = []
        for strategy in self.strategies.values():
            if instrument in strategy._instruments:
                tasks.append(asyncio.create_task(
                    strategy.on_signal(instrument, signal)
                ))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def update(self):
        """Periodic update call for all strategies."""
        # logger.debug("Running periodic strategy engine update...")
        tasks = [
            asyncio.create_task(strategy.update()) for strategy in self.strategies.values()
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)