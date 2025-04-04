# analytics/performance.py
"""
Calculates and reports trading performance metrics.
"""
import datetime
import logging
import os
from typing import Dict, List, Optional, Any, Tuple, Union
from pathlib import Path

import pandas as pd
import numpy as np

# Handle optional matplotlib import
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    plt = None # Assign None if not available

# Use absolute imports
from utils.data_classes import Trade, Account

logger = logging.getLogger(__name__)

class PerformanceAnalytics:
    """Analyzes trading performance."""

    def __init__(self, initial_balance: float = 0.0): # Allow setting initial balance
        """Initialize performance analytics."""
        self.trades: List[Trade] = []
        self.daily_stats: Dict[str, Dict] = {} # date_str -> stats_dict
        self.account_history: List[Dict] = [] # {'timestamp': dt, 'balance': float, ...}
        self.initial_balance = initial_balance # Store initial balance for equity curve
        logger.info(f"PerformanceAnalytics initialized. Initial Balance set to: ${initial_balance:.2f}")

    def set_initial_balance(self, balance: float):
         """Set or update the initial balance."""
         self.initial_balance = balance
         logger.info(f"Initial balance updated to: ${balance:.2f}")

    def add_trade(self, trade: Trade):
        """Add a completed trade for analysis."""
        if not isinstance(trade, Trade):
             logger.error(f"Invalid object passed to add_trade: {type(trade)}")
             return
        self.trades.append(trade)
        self._update_daily_stats(trade)

    def add_account_snapshot(self, account: Account):
        """Add account snapshot to history."""
        if not isinstance(account, Account):
             logger.error(f"Invalid object passed to add_account_snapshot: {type(account)}")
             return
        self.account_history.append({
            'timestamp': datetime.datetime.now(datetime.timezone.utc), # Use UTC
            'balance': account.balance,
            'equity': account.equity,
            'day_pl': account.day_pl,
            'overall_pl': account.overall_pl,
            'margin_used': account.margin_used,
            'margin_available': account.margin_available,
            'margin_level_percent': account.margin_level_percent
        })

    def record_daily_summary(self, date: datetime.date, daily_pl: float, best_day_pl: float):
         """Explicitly record end-of-day summary stats."""
         # This can be used by TradingSystem._check_day_change
         date_str = date.isoformat()
         if date_str in self.daily_stats:
              self.daily_stats[date_str]['final_daily_pl'] = daily_pl
              self.daily_stats[date_str]['final_best_day_pl_recorded'] = best_day_pl
              logger.debug(f"Recorded final daily summary for {date_str}: P/L={daily_pl:.2f}")
         else:
              logger.warning(f"Attempted to record daily summary for {date_str}, but no trades occurred on that day.")


    def _update_daily_stats(self, trade: Trade):
        """Update daily statistics with a trade."""
        date = trade.exit_time.date()
        date_str = date.isoformat()

        if date_str not in self.daily_stats:
            self.daily_stats[date_str] = {
                'date': date, # Store date object
                'profit_loss': 0.0, 'trades': 0, 'winners': 0, 'losers': 0,
                'total_pips': 0.0, 'winning_pips': 0.0, 'losing_pips': 0.0,
                'instruments': set(), 'strategies': set(),
                'best_trade_pl': -float('inf'), 'worst_trade_pl': float('inf'),
                'total_volume': 0.0 # Track lots traded
            }

        stats = self.daily_stats[date_str]
        stats['profit_loss'] += trade.profit_loss
        stats['trades'] += 1
        stats['total_pips'] += trade.profit_loss_pips
        stats['instruments'].add(trade.instrument)
        stats['strategies'].add(trade.strategy_type or 'Unknown')
        stats['total_volume'] += trade.quantity

        if trade.profit_loss > 0:
            stats['winners'] += 1
            stats['winning_pips'] += trade.profit_loss_pips
        else:
            stats['losers'] += 1
            stats['losing_pips'] += trade.profit_loss_pips # Keep negative

        stats['best_trade_pl'] = max(stats['best_trade_pl'], trade.profit_loss)
        stats['worst_trade_pl'] = min(stats['worst_trade_pl'], trade.profit_loss)

    def get_overall_stats(self) -> Dict[str, Any]:
        """Calculate overall trading statistics."""
        if not self.trades: return self._get_empty_stats()

        df = self.get_trades_dataframe()
        total_trades = len(df)
        total_pl = df['profit_loss'].sum()

        winners = df[df['profit_loss'] > 0]
        losers = df[df['profit_loss'] <= 0]
        win_count = len(winners)
        loss_count = len(losers)

        win_rate = win_count / total_trades if total_trades > 0 else 0.0
        gross_profit = winners['profit_loss'].sum()
        gross_loss = abs(losers['profit_loss'].sum()) # Use absolute value for loss

        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        average_trade = total_pl / total_trades if total_trades > 0 else 0.0
        average_winner = gross_profit / win_count if win_count > 0 else 0.0
        average_loser = abs(losers['profit_loss'].sum() / loss_count) if loss_count > 0 else 0.0 # Avg loss amount

        best_trade = df['profit_loss'].max()
        worst_trade = df['profit_loss'].min()

        # Calculate equity curve and drawdown
        equity_curve, timestamps = self._calculate_equity_curve()
        max_drawdown_percent, max_drawdown_abs = self._calculate_max_drawdown(equity_curve)

        # Sharpe Ratio (requires risk-free rate and trade frequency analysis - simplified)
        # Simplified: Assume risk-free rate = 0, use daily returns if possible
        # For now, calculate based on trade returns
        if total_trades > 1:
             returns_std = df['profit_loss'].std()
             sharpe_ratio = (average_trade / returns_std) * np.sqrt(total_trades) if returns_std > 0 else 0 # Simplified annualization factor
        else:
             sharpe_ratio = 0.0


        return {
            'total_trades': total_trades,
            'total_profit_loss': total_pl,
            'win_rate': win_rate,
            'profit_factor': profit_factor,
            'average_trade': average_trade,
            'average_winner': average_winner,
            'average_loser': average_loser,
            'max_drawdown_percent': max_drawdown_percent,
            'max_drawdown_abs': max_drawdown_abs,
            'best_trade': best_trade,
            'worst_trade': worst_trade,
            'sharpe_ratio_simplified': sharpe_ratio,
            'gross_profit': gross_profit,
            'gross_loss': gross_loss,
            'start_date': df['exit_time'].min().date() if not df.empty else None,
            'end_date': df['exit_time'].max().date() if not df.empty else None,
        }

    def _get_empty_stats(self) -> Dict[str, Any]:
         """Return a dictionary with zeroed/default stats."""
         return {
            'total_trades': 0, 'total_profit_loss': 0.0, 'win_rate': 0.0, 'profit_factor': 0.0,
            'average_trade': 0.0, 'average_winner': 0.0, 'average_loser': 0.0,
            'max_drawdown_percent': 0.0, 'max_drawdown_abs': 0.0, 'best_trade': 0.0, 'worst_trade': 0.0,
            'sharpe_ratio_simplified': 0.0, 'gross_profit': 0.0, 'gross_loss': 0.0,
            'start_date': None, 'end_date': None,
        }

    def get_trades_dataframe(self) -> pd.DataFrame:
        """Convert trade list to a Pandas DataFrame."""
        if not self.trades:
            return pd.DataFrame(columns=[f.name for f in Trade.__dataclass_fields__.values()]) # Empty DF with columns
        # Convert list of dataclasses to list of dicts, then to DataFrame
        trade_dicts = [vars(trade) for trade in self.trades]
        df = pd.DataFrame(trade_dicts)
        # Convert timestamp columns to datetime objects if they aren't already
        df['entry_time'] = pd.to_datetime(df['entry_time'], utc=True)
        df['exit_time'] = pd.to_datetime(df['exit_time'], utc=True)
        df = df.sort_values(by='exit_time').reset_index(drop=True)
        return df

    def _calculate_equity_curve(self) -> Tuple[List[float], List[datetime.datetime]]:
        """Calculate equity curve based on initial balance and trades."""
        if not self.trades:
            now = datetime.datetime.now(datetime.timezone.utc)
            return [self.initial_balance], [now] # Return initial balance at current time

        df = self.get_trades_dataframe()
        # Calculate cumulative profit
        df['cumulative_pl'] = df['profit_loss'].cumsum()
        # Calculate equity curve starting from initial balance
        equity = [self.initial_balance] + (self.initial_balance + df['cumulative_pl']).tolist()
        # Get timestamps (use exit time for each trade point)
        timestamps = [df['exit_time'].iloc[0] - datetime.timedelta(seconds=1)] + df['exit_time'].tolist() # Add pseudo start time

        return equity, timestamps

    def _calculate_max_drawdown(self, equity_curve: List[float]) -> Tuple[float, float]:
        """Calculate maximum drawdown (percentage and absolute) from equity curve."""
        if len(equity_curve) <= 1: return 0.0, 0.0

        max_drawdown_pct = 0.0
        max_drawdown_abs = 0.0
        peak_equity = equity_curve[0]

        for equity in equity_curve[1:]:
            if equity > peak_equity:
                peak_equity = equity
            else:
                drawdown_abs = peak_equity - equity
                drawdown_pct = (drawdown_abs / peak_equity) * 100.0 if peak_equity > 0 else 0.0
                max_drawdown_abs = max(max_drawdown_abs, drawdown_abs)
                max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)

        return max_drawdown_pct / 100.0, max_drawdown_abs # Return percentage as 0.xx

    def get_stats_by_group(self, group_by_key: str) -> Dict[str, Dict]:
        """Calculate statistics grouped by a specific key (e.g., 'instrument', 'strategy_type')."""
        if not self.trades: return {}
        df = self.get_trades_dataframe()
        grouped_stats = {}

        # Fill NaN strategy types for grouping
        df[group_by_key] = df[group_by_key].fillna('Unknown')

        grouped = df.groupby(group_by_key)

        for name, group in grouped:
            stats = {}
            stats['total_trades'] = len(group)
            stats['profit_loss'] = group['profit_loss'].sum()
            winners = group[group['profit_loss'] > 0]
            losers = group[group['profit_loss'] <= 0]
            stats['winners'] = len(winners)
            stats['losers'] = len(losers)
            stats['win_rate'] = stats['winners'] / stats['total_trades'] if stats['total_trades'] > 0 else 0.0
            stats['average_trade'] = stats['profit_loss'] / stats['total_trades'] if stats['total_trades'] > 0 else 0.0
            stats['best_trade'] = group['profit_loss'].max()
            stats['worst_trade'] = group['profit_loss'].min()
            grouped_stats[name] = stats

        return grouped_stats

    def generate_charts(self, output_dir: Union[str, Path] = 'charts'):
        """Generate performance charts using Matplotlib."""
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("Matplotlib not installed. Skipping chart generation.")
            return
        if not self.trades:
            logger.warning("No trades available to generate charts.")
            return

        output_path = Path(output_dir)
        try:
            output_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Generating performance charts in {output_path}...")

            # --- Equity Curve ---
            equity_curve, timestamps = self._calculate_equity_curve()
            if len(equity_curve) > 1:
                fig, ax = plt.subplots(figsize=(12, 6))
                ax.plot(timestamps, equity_curve, label='Equity Curve')
                ax.set_title('Equity Curve Over Time')
                ax.set_xlabel('Time')
                ax.set_ylabel(f'Equity (${self.initial_balance:.0f} Start)')
                ax.grid(True, linestyle='--', alpha=0.6)
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M')) # Format x-axis dates
                fig.autofmt_xdate() # Auto-rotate date labels
                plt.legend()
                plt.tight_layout()
                plt.savefig(output_path / 'equity_curve.png')
                plt.close(fig)

            # --- Profit Distribution ---
            df = self.get_trades_dataframe()
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.hist(df['profit_loss'], bins=30, edgecolor='black')
            ax.set_title('Trade Profit/Loss Distribution')
            ax.set_xlabel('Profit/Loss ($)')
            ax.set_ylabel('Number of Trades')
            ax.grid(True, axis='y', linestyle='--', alpha=0.6)
            plt.tight_layout()
            plt.savefig(output_path / 'profit_distribution.png')
            plt.close(fig)

            # --- Win Rate by Instrument ---
            instrument_stats = self.get_stats_by_group('instrument')
            if instrument_stats:
                df_inst = pd.DataFrame.from_dict(instrument_stats, orient='index').sort_values('profit_loss', ascending=False)
                fig, ax = plt.subplots(figsize=(12, 6))
                ax.bar(df_inst.index, df_inst['win_rate'] * 100, color='skyblue')
                ax.set_title('Win Rate by Instrument')
                ax.set_xlabel('Instrument')
                ax.set_ylabel('Win Rate (%)')
                ax.tick_params(axis='x', rotation=45)
                ax.grid(True, axis='y', linestyle='--', alpha=0.6)
                plt.tight_layout()
                plt.savefig(output_path / 'win_rate_by_instrument.png')
                plt.close(fig)

            # --- Profit by Strategy ---
            strategy_stats = self.get_stats_by_group('strategy_type')
            if strategy_stats:
                df_strat = pd.DataFrame.from_dict(strategy_stats, orient='index').sort_values('profit_loss', ascending=False)
                fig, ax = plt.subplots(figsize=(12, 6))
                colors = ['green' if x > 0 else 'red' for x in df_strat['profit_loss']]
                ax.bar(df_strat.index, df_strat['profit_loss'], color=colors)
                ax.set_title('Total Profit/Loss by Strategy')
                ax.set_xlabel('Strategy')
                ax.set_ylabel('Profit/Loss ($)')
                ax.tick_params(axis='x', rotation=45)
                ax.grid(True, axis='y', linestyle='--', alpha=0.6)
                plt.tight_layout()
                plt.savefig(output_path / 'profit_by_strategy.png')
                plt.close(fig)

            logger.info(f"Performance charts generated successfully.")

        except Exception as e:
            logger.exception(f"Error generating charts: {e}")

    def print_performance_summary(self):
        """Print performance summary to console."""
        stats = self.get_overall_stats()
        if stats['total_trades'] == 0:
            print("\n===== PERFORMANCE SUMMARY (No Trades) =====")
            return

        print("\n===== PERFORMANCE SUMMARY =====")
        print(f" Period: {stats.get('start_date', 'N/A')} to {stats.get('end_date', 'N/A')}")
        print(f" Total P&L: ${stats['total_profit_loss']:,.2f}")
        print(f" Total Trades: {stats['total_trades']}")
        print(f" Win Rate: {stats['win_rate']:.2%}")
        print(f" Profit Factor: {stats['profit_factor']:.2f}")
        print("-" * 30)
        print(f" Avg Trade: ${stats['average_trade']:,.2f}")
        print(f" Avg Winner: ${stats['average_winner']:,.2f}")
        print(f" Avg Loser: ${stats['average_loser']:,.2f}")
        print("-" * 30)
        print(f" Max Drawdown: {stats['max_drawdown_percent']:.2%} (${stats['max_drawdown_abs']:,.2f})")
        print(f" Sharpe Ratio (simplified): {stats['sharpe_ratio_simplified']:.2f}")
        print(f" Best Trade: ${stats['best_trade']:,.2f}")
        print(f" Worst Trade: ${stats['worst_trade']:,.2f}")

        # Print daily stats (optional, can be long)
        # print("\n----- Daily Performance -----")
        # daily_df = pd.DataFrame.from_dict(self.daily_stats, orient='index').sort_index()
        # print(daily_df[['profit_loss', 'trades', 'winners', 'losers']].head()) # Print head

        # Print instrument stats
        print("\n----- Instrument Performance -----")
        instrument_stats = self.get_stats_by_group('instrument')
        inst_df = pd.DataFrame.from_dict(instrument_stats, orient='index').sort_values('profit_loss', ascending=False)
        print(inst_df[['profit_loss', 'trades', 'win_rate']].to_string(formatters={'profit_loss': '${:,.2f}'.format, 'win_rate': '{:.2%}'.format}))

        # Print strategy stats
        print("\n----- Strategy Performance -----")
        strategy_stats = self.get_stats_by_group('strategy_type')
        strat_df = pd.DataFrame.from_dict(strategy_stats, orient='index').sort_values('profit_loss', ascending=False)
        print(strat_df[['profit_loss', 'trades', 'win_rate', 'average_trade']].to_string(formatters={'profit_loss': '${:,.2f}'.format, 'win_rate': '{:.2%}'.format, 'average_trade': '${:,.2f}'.format}))

        print("================================")

    def save_performance_report(self, filepath: Union[str, Path] = 'performance_report.html'):
        """Generate and save performance report as HTML."""
        filepath = Path(filepath)
        try:
            filepath.parent.mkdir(parents=True, exist_ok=True) # Ensure directory exists
            stats = self.get_overall_stats()
            if stats['total_trades'] == 0:
                 logger.warning("No trades to generate report.")
                 # Optionally create a simple "No Trades" report
                 with open(filepath, 'w') as f:
                      f.write("<html><body><h1>Performance Report</h1><p>No trades executed.</p></body></html>")
                 return

            # Generate charts first (if enabled)
            chart_dir = filepath.parent / 'charts' # Save charts in subdir relative to report
            chart_files = {}
            if MATPLOTLIB_AVAILABLE:
                 self.generate_charts(chart_dir)
                 # Get relative paths for embedding
                 chart_files = {
                      'equity': 'charts/equity_curve.png',
                      'dist': 'charts/profit_distribution.png',
                      'win_inst': 'charts/win_rate_by_instrument.png',
                      'profit_strat': 'charts/profit_by_strategy.png'
                 }
                 # Check if files actually exist
                 for key, file in list(chart_files.items()):
                      if not (filepath.parent / file).exists():
                           logger.warning(f"Chart file not found: {file}. Removing from report.")
                           del chart_files[key]


            # --- HTML Generation (Simplified Example) ---
            # Using f-strings for basic templating
            html = f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>Performance Report</title>
                <style>
                    body {{ font-family: sans-serif; margin: 15px; }}
                    table {{ border-collapse: collapse; width: 100%; margin-bottom: 1em; }}
                    th, td {{ border: 1px solid #ccc; padding: 6px; text-align: left; }}
                    th {{ background-color: #eee; }}
                    .positive {{ color: green; }} .negative {{ color: red; }}
                    .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-bottom: 1em; }}
                    .metric {{ background-color: #f9f9f9; border: 1px solid #ddd; padding: 10px; text-align: center; }}
                    .metric-label {{ font-size: 0.9em; color: #555; }}
                    .metric-value {{ font-size: 1.4em; font-weight: bold; margin-top: 5px; }}
                    img.chart {{ max-width: 100%; height: auto; margin-top: 1em; border: 1px solid #ddd; }}
                </style>
            </head>
            <body>
                <h1>Performance Report</h1>
                <p>Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Period: {stats.get('start_date', 'N/A')} to {stats.get('end_date', 'N/A')}</p>

                <h2>Overall Summary</h2>
                <div class="summary-grid">
                    <div class="metric"><div class="metric-label">Total P/L</div><div class="metric-value {'positive' if stats['total_profit_loss'] > 0 else 'negative'}">${stats['total_profit_loss']:,.2f}</div></div>
                    <div class="metric"><div class="metric-label">Total Trades</div><div class="metric-value">{stats['total_trades']}</div></div>
                    <div class="metric"><div class="metric-label">Win Rate</div><div class="metric-value">{stats['win_rate']:.2%}</div></div>
                    <div class="metric"><div class="metric-label">Profit Factor</div><div class="metric-value">{stats['profit_factor']:.2f}</div></div>
                    <div class="metric"><div class="metric-label">Avg Trade P/L</div><div class="metric-value {'positive' if stats['average_trade'] > 0 else 'negative'}">${stats['average_trade']:,.2f}</div></div>
                    <div class="metric"><div class="metric-label">Max Drawdown</div><div class="metric-value negative">{stats['max_drawdown_percent']:.2%}</div></div>
                </div>

                <h2>Charts</h2>
                {f'<img src="{chart_files["equity"]}" alt="Equity Curve" class="chart">' if "equity" in chart_files else ""}
                {f'<img src="{chart_files["dist"]}" alt="Profit Distribution" class="chart">' if "dist" in chart_files else ""}
                {f'<img src="{chart_files["win_inst"]}" alt="Win Rate by Instrument" class="chart">' if "win_inst" in chart_files else ""}
                {f'<img src="{chart_files["profit_strat"]}" alt="Profit by Strategy" class="chart">' if "profit_strat" in chart_files else ""}

                <h2>Detailed Statistics</h2>
                {self._dict_to_html_table(self._get_detailed_stats_dict(stats))}

                <h2>Performance by Instrument</h2>
                {self._dataframe_to_html_table(pd.DataFrame.from_dict(self.get_stats_by_group('instrument'), orient='index').sort_values('profit_loss', ascending=False))}

                <h2>Performance by Strategy</h2>
                {self._dataframe_to_html_table(pd.DataFrame.from_dict(self.get_stats_by_group('strategy_type'), orient='index').sort_values('profit_loss', ascending=False))}

                <h2>Recent Trades</h2>
                {self._dataframe_to_html_table(self.get_trades_dataframe().tail(20))}

            </body></html>
            """

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html)
            logger.info(f"Performance report saved to {filepath}")

        except Exception as e:
            logger.exception(f"Error saving performance report: {e}")

    def _get_detailed_stats_dict(self, overall_stats: Dict) -> Dict:
         """Prepare overall stats for table display."""
         return {
             "Total Trades": overall_stats['total_trades'],
             "Total P/L ($)": f"{overall_stats['total_profit_loss']:,.2f}",
             "Gross Profit ($)": f"{overall_stats['gross_profit']:,.2f}",
             "Gross Loss ($)": f"{overall_stats['gross_loss']:,.2f}",
             "Profit Factor": f"{overall_stats['profit_factor']:.2f}",
             "Win Rate (%)": f"{overall_stats['win_rate']:.2%}",
             "Avg Trade ($)": f"{overall_stats['average_trade']:,.2f}",
             "Avg Winning Trade ($)": f"{overall_stats['average_winner']:,.2f}",
             "Avg Losing Trade ($)": f"{overall_stats['average_loser']:,.2f}",
             "Max Drawdown (%)": f"{overall_stats['max_drawdown_percent']:.2%}",
             "Max Drawdown ($)": f"{overall_stats['max_drawdown_abs']:,.2f}",
             "Sharpe Ratio (simplified)": f"{overall_stats['sharpe_ratio_simplified']:.2f}",
             "Best Trade ($)": f"{overall_stats['best_trade']:,.2f}",
             "Worst Trade ($)": f"{overall_stats['worst_trade']:,.2f}",
         }

    def _dict_to_html_table(self, data: Dict) -> str:
         """Convert a simple dictionary to an HTML table."""
         rows = "".join([f"<tr><th>{k}</th><td>{v}</td></tr>" for k, v in data.items()])
         return f"<table>{rows}</table>"

    def _dataframe_to_html_table(self, df: pd.DataFrame) -> str:
         """Convert a Pandas DataFrame to an HTML table with basic formatting."""
         if df.empty: return "<p>No data available.</p>"
         # Basic formatting for numbers
         float_format = lambda x: f"{x:,.2f}" if isinstance(x, (float, np.number)) else x
         percent_format = lambda x: f"{x:.2%}" if isinstance(x, (float, np.number)) else x
         formatters = {}
         for col in df.columns:
             if 'rate' in col.lower(): formatters[col] = percent_format
             elif df[col].dtype in [np.float64, np.int64] and ('p/l' in col.lower() or '$' in col.lower() or 'trade' in col.lower() or 'profit' in col.lower() or 'loss' in col.lower()):
                  formatters[col] = float_format

         return df.to_html(escape=False, formatters=formatters, border=1, classes='data-table')