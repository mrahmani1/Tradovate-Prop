# core/rules_engine.py
"""
Enforces trading rules, risk limits, and challenge constraints.
"""
import datetime
import logging
from typing import Dict, List, Optional, Any, Union, Tuple

from utils.data_classes import Account, Position, Challenge
from config import get_config_value # Use helper
from utils.helpers import get_instrument_category # Use helper

logger = logging.getLogger(__name__)

class RulesEngine:
    """Enforces trading rules and constraints."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize rules engine."""
        self.config = config
        self.daily_loss_limit_triggered = False
        self.daily_loss_limit_reset_time: Optional[datetime.datetime] = None
        self.start_of_day_balance: float = 0.0 # Track start-of-day balance for daily loss calc

        # Initialize challenge object based on config
        self.challenge = self._init_challenge(config)
        logger.info(f"RulesEngine initialized for Challenge: {self.challenge.challenge_type}, Size: {self.challenge.account_size}")

    def _init_challenge(self, config: Dict[str, Any]) -> Challenge:
        """Initialize challenge object from configuration."""
        # Use get_config_value for safe access with defaults
        challenge_type = get_config_value(config, 'CHALLENGE.default_challenge_type', '1phase')
        account_size_str = get_config_value(config, 'CHALLENGE.default_account_size', '50K')
        instrument_type = get_config_value(config, 'CHALLENGE.default_instrument_type', 'forex')
        leverage = get_config_value(config, 'API.leverage', 30) # Leverage might be under API or specific section

        # Risk percentages from RISK_MANAGEMENT section
        profit_target_percent = get_config_value(config, 'RISK_MANAGEMENT.profit_target_percent', 10.0)
        max_daily_loss_percent = get_config_value(config, 'RISK_MANAGEMENT.max_daily_loss_percent', 4.0)
        max_total_loss_percent = get_config_value(config, 'RISK_MANAGEMENT.max_total_loss_percent', 8.0)

        # Challenge specific rules from CHALLENGE section
        min_trading_days = get_config_value(config, 'CHALLENGE.min_trading_days', 5)
        consistency_target_percent = get_config_value(config, 'CHALLENGE.consistency_target_percent', 50.0)

        try:
            account_size = int(account_size_str.replace('K', '000').replace('M', '000000'))
        except ValueError:
            logger.error(f"Invalid account size format: {account_size_str}. Using 0.")
            account_size = 0

        # Phase-specific adjustments (Example)
        if challenge_type == '2phase':
            profit_target_percent = get_config_value(config, 'CHALLENGE.phase1_profit_target_percent', 8.0)
            # Phase 2 target might be different, handle phase logic elsewhere

        return Challenge(
            challenge_type=challenge_type,
            account_size=account_size,
            instrument_type=instrument_type,
            leverage=leverage,
            profit_target_percent=profit_target_percent,
            max_daily_loss_percent=max_daily_loss_percent,
            max_total_loss_percent=max_total_loss_percent,
            min_trading_days=min_trading_days,
            consistency_target_percent=consistency_target_percent,
            # start_date is handled by default factory
        )

    def update_start_of_day_balance(self, balance: float):
        """Update the balance used for daily loss calculation."""
        self.start_of_day_balance = balance
        logger.info(f"Start of day balance updated to: ${balance:.2f}")

    def reset_daily_limits(self, current_balance: float):
        """Reset daily limits and update start-of-day balance."""
        self.daily_loss_limit_triggered = False
        self.daily_loss_limit_reset_time = None
        self.update_start_of_day_balance(current_balance)
        # Reset daily challenge metrics if needed (best day profit is overall, not daily)
        logger.info("Daily loss limit state reset.")

    def update_challenge_metrics(self, best_day_profit: float):
        """Update challenge metrics like best day profit."""
        # Only update if the new best day is better
        self.challenge.best_day_profit = max(self.challenge.best_day_profit, best_day_profit)

    def check_rules(
        self,
        account: Account,
        positions: List[Position],
        daily_pl: float
    ) -> List[Dict]:
        """
        Check trading rules based on current state.

        Args:
            account: Current Account object.
            positions: List of current Position objects.
            daily_pl: Current calculated daily P/L.

        Returns:
            List of rule violation dictionaries, each containing:
            {'rule': str, 'message': str, 'action': 'warn'|'liquidate'|'stop_trading', 'reason': str}
        """
        violations = []
        now = datetime.datetime.now(datetime.timezone.utc)

        # --- Max Loss Rules ---
        # Total Loss Limit (based on initial challenge size vs current equity)
        max_total_loss_abs = self.challenge.max_total_loss_abs
        if account.equity <= (self.challenge.account_size - max_total_loss_abs):
            violations.append({
                'rule': 'max_total_loss',
                'message': f"Account equity (${account.equity:.2f}) breached max total loss limit (${max_total_loss_abs:.2f}) relative to starting size ${self.challenge.account_size}.",
                'action': 'liquidate', # Usually requires immediate stop
                'reason': 'Maximum total loss limit breached'
            })

        # Daily Loss Limit (based on start-of-day balance vs current equity)
        max_daily_loss_abs = self.challenge.max_daily_loss_abs
        # Calculate drawdown from start of day equity/balance
        current_daily_drawdown = self.start_of_day_balance - account.equity
        if current_daily_drawdown >= max_daily_loss_abs:
            # Check if already triggered today
            if not self.daily_loss_limit_triggered:
                self.daily_loss_limit_triggered = True
                self.daily_loss_limit_reset_time = self._get_reset_time()
                violations.append({
                    'rule': 'daily_loss_limit',
                    'message': f"Equity drawdown (${current_daily_drawdown:.2f}) breached daily loss limit (${max_daily_loss_abs:.2f}) relative to start-of-day balance ${self.start_of_day_balance:.2f}.",
                    'action': 'liquidate', # Usually requires immediate stop for the day
                    'reason': 'Daily loss limit breached'
                })
            # else: # Already triggered, no new violation, but trading should be halted by can_enter_position

        # Reset daily loss trigger at reset time
        if self.daily_loss_limit_triggered and self.daily_loss_limit_reset_time and now >= self.daily_loss_limit_reset_time:
             logger.info("Daily loss limit reset time reached.")
             # Resetting happens in TradingSystem._check_day_change now
             # self.reset_daily_limits(account.balance) # Pass current balance

        # --- Position & Risk Rules ---
        # Max total lots across all positions
        max_position_size_lots = get_config_value(self.config, 'RISK_MANAGEMENT.max_position_size_lots', 5.0)
        total_position_size = sum(p.quantity for p in positions)
        if total_position_size > max_position_size_lots:
            violations.append({
                'rule': 'max_position_size_lots',
                'message': f"Total position size ({total_position_size:.2f} lots) exceeds maximum ({max_position_size_lots:.2f} lots).",
                'action': 'warn', # Usually prevent new entries, not liquidate existing
                'reason': 'Maximum total lots exceeded'
            })

        # Max concurrent instruments
        max_instruments = get_config_value(self.config, 'RISK_MANAGEMENT.max_instruments_traded', 5)
        current_instruments_count = len(set(p.instrument for p in positions))
        if current_instruments_count > max_instruments:
             violations.append({
                'rule': 'max_instruments_traded',
                'message': f"Number of instruments with open positions ({current_instruments_count}) exceeds maximum ({max_instruments}).",
                'action': 'warn',
                'reason': 'Maximum instruments traded exceeded'
            })

        # Max exposure per category (more complex, requires calculating risk per position)
        # Simplified check based on lots per category:
        max_category_exposure_percent = get_config_value(self.config, 'RISK_MANAGEMENT.max_category_exposure_percent', 60.0)
        category_exposure: Dict[str, float] = {}
        for pos in positions:
            category = get_instrument_category(pos.instrument)
            category_exposure[category] = category_exposure.get(category, 0.0) + pos.quantity
        for category, exposure_lots in category_exposure.items():
             # Compare category lots to overall max lots allowed
             if exposure_lots > (max_position_size_lots * (max_category_exposure_percent / 100.0)):
                  violations.append({
                     'rule': 'category_exposure',
                     'message': f"Exposure to {category} ({exposure_lots:.2f} lots) exceeds limit based on max total lots.",
                     'action': 'warn',
                     'reason': f"Excessive {category} exposure"
                 })

        # Correlation Risk (Simplified Check)
        correlated_pairs = get_config_value(self.config, 'RISK_MANAGEMENT.correlated_instruments', []) # Expect list of lists [['EURUSD', 'GBPUSD'], ...]
        position_map = {p.instrument: p for p in positions}
        checked_pairs = set()
        for pair in correlated_pairs:
            pair_tuple = tuple(sorted(pair))
            if len(pair) == 2 and pair_tuple not in checked_pairs:
                inst1, inst2 = pair
                if inst1 in position_map and inst2 in position_map:
                    pos1 = position_map[inst1]
                    pos2 = position_map[inst2]
                    # Check if both positions are in the same direction
                    if pos1.direction == pos2.direction:
                        violations.append({
                            'rule': 'correlation_risk',
                            'message': f"Correlated instruments {inst1} and {inst2} both have {pos1.direction} positions.",
                            'action': 'warn',
                            'reason': 'Increased correlation risk'
                        })
                        checked_pairs.add(pair_tuple) # Avoid duplicate warnings for the same pair

        # --- Challenge Specific Rules (Informational/Warning) ---
        # These don't typically trigger liquidation unless combined with loss limits
        challenge_progress = self.challenge.get_progress(account.balance, daily_pl)

        # Consistency Target Check (Warning if potentially failing)
        if challenge_progress['profit'] > 0 and not challenge_progress['consistency_met']:
             violations.append({
                'rule': 'consistency_target',
                'message': f"Best day profit (${self.challenge.best_day_profit:.2f}) exceeds {self.challenge.consistency_target_percent}% of total profit (${challenge_progress['profit']:.2f}). May fail consistency rule.",
                'action': 'warn',
                'reason': 'Potential consistency rule failure'
            })

        # Min Trading Days Check (Informational)
        if not challenge_progress['min_days_met']:
             days_needed = self.challenge.min_trading_days - challenge_progress['days_traded']
             violations.append({
                'rule': 'min_trading_days',
                'message': f"Minimum trading days not yet met ({challenge_progress['days_traded']}/{self.challenge.min_trading_days}). Need {days_needed} more.",
                'action': 'info', # Not usually a violation, just status
                'reason': 'Minimum trading days pending'
            })

        return violations

    def can_enter_position(
        self,
        instrument: str,
        direction: str,
        quantity: float,
        account: Account,
        current_positions: List[Position],
        contract_details: Optional[Dict] = None
    ) -> Tuple[bool, Optional[str]]:
        """Check if a new position entry is allowed based on rules."""

        # 1. Check if trading is halted due to daily loss limit
        if self.daily_loss_limit_triggered:
            reset_time_str = self.daily_loss_limit_reset_time.strftime('%Y-%m-%d %H:%M:%S UTC') if self.daily_loss_limit_reset_time else "next reset"
            return False, f"Trading halted: Daily loss limit reached. Resets at {reset_time_str}."

        # 2. Check Max Position Size (Contracts/Lots)
        # TODO: Rename 'lots' to 'contracts' in config/logs for futures clarity
        max_position_size_contracts = get_config_value(self.config, 'RISK_MANAGEMENT.max_position_size_lots', 5.0)
        total_position_size = sum(p.quantity for p in current_positions)
        if total_position_size + quantity > max_position_size_contracts:
            return False, f"Entry blocked: Would exceed maximum total contracts ({max_position_size_contracts:.2f}). Current: {total_position_size:.2f}, Adding: {quantity:.2f}"

        # 3. Check Max Instruments Traded
        max_instruments = get_config_value(self.config, 'RISK_MANAGEMENT.max_instruments_traded', 5)
        current_instrument_symbols = {p.instrument for p in current_positions}
        if instrument not in current_instrument_symbols and len(current_instrument_symbols) >= max_instruments:
            return False, f"Entry blocked: Would exceed maximum concurrent instruments ({max_instruments})."

        # 4. Check Max Allocation per Instrument (if defined)
        # Example: Define max contracts per instrument in config: RISK_MANAGEMENT.max_contracts_per_instrument = {'ES': 2.0, 'default': 1.0}
        # TODO: Update config key name potentially
        max_contracts_per_instrument_config = get_config_value(self.config, 'RISK_MANAGEMENT.max_lots_per_instrument', {})
        instrument_max_contracts = max_contracts_per_instrument_config.get(instrument, max_contracts_per_instrument_config.get('default'))
        if instrument_max_contracts is not None:
             instrument_current_contracts = sum(p.quantity for p in current_positions if p.instrument == instrument)
             if instrument_current_contracts + quantity > instrument_max_contracts:
                  return False, f"Entry blocked: Would exceed max contracts ({instrument_max_contracts:.2f}) for {instrument}. Current: {instrument_current_contracts:.2f}"

        # 5. Check if Market is Open (using contract_details if available)
        if not self._is_market_open(instrument, contract_details=contract_details):
            return False, f"Entry blocked: Market for {instrument} is likely closed or not open according to API."

        # Add other checks as needed (e.g., margin availability - handled in PositionManager now)

        return True, None # Allowed

    def can_exit_position(self, position: Position, account: Account) -> Tuple[bool, Optional[str]]:
        """Check if a position exit is allowed (usually always true)."""
        # Typically, closing positions is always allowed, even if limits were breached.
        # Add specific rules here if needed (e.g., minimum holding time - not typical for risk rules).
        return True, None

    def _get_reset_time(self) -> datetime.datetime:
        """Calculate the next reset time (e.g., midnight UTC)."""
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        # Reset at next midnight UTC
        tomorrow_utc = now_utc.date() + datetime.timedelta(days=1)
        reset_time = datetime.datetime.combine(tomorrow_utc, datetime.time(0, 0, 0), tzinfo=datetime.timezone.utc)
        return reset_time

    def _is_market_open(self, instrument: str, contract_details: Optional[Dict] = None) -> bool:
        """Check if market is likely open (prefers contract_details status)."""
        # TODO: Implement proper session time parsing from contract_details['tradingHours'] if needed

        # Prefer API status if available
        if contract_details:
            is_open_api = contract_details.get('isOpen') # Verify exact field name
            if is_open_api is False: # Explicitly False from API
                logger.debug(f"Market check for {instrument}: API reports closed (isOpen=False).")
                return False
            elif is_open_api is True: # Explicitly True from API
                 logger.debug(f"Market check for {instrument}: API reports open (isOpen=True).")
                 return True
            # If isOpen is None or missing, fall through to basic time check

        # Fallback: Basic check (e.g., avoid weekends) - VERY ROUGH
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        weekday = now_utc.weekday()

        # General Futures Market (Globex approximation) - Closed Sat, part of Sun
        if weekday == 5: # Saturday
            return False
        if weekday == 6 and now_utc.hour < 22: # Sunday before ~6 PM ET / 10 PM UTC
            return False
        # Add specific instrument/exchange logic here if needed

        logger.debug(f"Market check for {instrument}: Using basic time check (isOpen flag unavailable/null).")
        return True # Assume open if basic checks pass