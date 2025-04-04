# ml_models/model_manager.py
"""
Manages machine learning models for prediction and training.
Includes market regime and sentiment analysis components.
"""
import asyncio
import os
import datetime
import json
import logging
import pickle
from typing import Dict, List, Optional, Any, Tuple, Union
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.metrics import accuracy_score, mean_squared_error, f1_score, classification_report
import xgboost as xgb

# Deep learning imports (handle optional import)
try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential, load_model, Model
    from tensorflow.keras.layers import LSTM, GRU, Dense, Dropout, Input, Concatenate, BatchNormalization
    from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau
    from tensorflow.keras.optimizers import Adam
    TENSORFLOW_AVAILABLE = True
except ImportError:
    TENSORFLOW_AVAILABLE = False
    # Define dummy classes if TF not installed, so the rest of the code doesn't break
    Model = object
    EarlyStopping = object
    ReduceLROnPlateau = object
    Adam = object
    LSTM = GRU = Dense = Dropout = Input = Concatenate = BatchNormalization = object
    load_model = lambda x: None # Dummy load_model

# Market Regime / Sentiment Analysis (handle optional import)
try:
    from statsmodels.tsa.stattools import adfuller # Example for trend stationarity
    from arch import arch_model # For GARCH volatility modeling
    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False
    adfuller = lambda x: (0, 0.5) # Dummy function
    arch_model = None # Dummy class

# Use absolute imports
from utils.data_classes import TradingSignal, Trade, Position, PriceSnapshot
from config import get_config_value # Use helper

logger = logging.getLogger(__name__)

# --- Market Regime Classifier ---
class MarketRegimeClassifier:
    """Classifies market regimes based on price action and volatility."""
    def __init__(self, config: Dict[str, Any]):
        self.config = config # Pass main config
        # Example params from config
        self.lookback_period = get_config_value(config, 'MARKET_REGIME.lookback_period', 100)
        self.trend_strength_threshold = get_config_value(config, 'MARKET_REGIME.trend_strength_threshold', 0.5)
        self.volatility_expansion_threshold = get_config_value(config, 'MARKET_REGIME.volatility_expansion_threshold', 1.5) # e.g., std dev > 1.5 * avg
        self.volatility_contraction_threshold = get_config_value(config, 'MARKET_REGIME.volatility_contraction_threshold', 0.7) # e.g., std dev < 0.7 * avg

    def classify(self, prices: List[float], volumes: Optional[List[float]] = None) -> Dict[str, Any]:
        """Classify market regime."""
        if len(prices) < self.lookback_period:
            return {'regime': 'unknown', 'confidence': 0.0, 'details': {}}

        recent_prices = np.array(prices[-self.lookback_period:])
        returns = np.diff(recent_prices) / recent_prices[:-1]

        # 1. Volatility Analysis (e.g., comparing recent vs longer term)
        volatility_short = np.std(returns[-20:]) if len(returns) >= 20 else np.std(returns)
        volatility_long = np.std(returns)
        vol_ratio = volatility_short / volatility_long if volatility_long > 1e-9 else 1.0

        volatility_regime = 'normal'
        if vol_ratio > self.volatility_expansion_threshold: volatility_regime = 'expansion'
        elif vol_ratio < self.volatility_contraction_threshold: volatility_regime = 'contraction'

        # 2. Trend Analysis (e.g., using slope, R-squared, or ADF test)
        x = np.arange(len(recent_prices))
        slope, intercept = np.polyfit(x, recent_prices, 1)
        r_value = np.corrcoef(x, recent_prices)[0, 1]
        r_squared = r_value**2

        trend_regime = 'ranging'
        trend_confidence = 1.0 - r_squared
        if r_squared >= self.trend_strength_threshold:
            trend_regime = 'trending_up' if slope > 0 else 'trending_down'
            trend_confidence = r_squared

        # Optional: Stationarity test (ADF)
        adf_p_value = 0.5
        if STATSMODELS_AVAILABLE:
             try:
                  adf_result = adfuller(recent_prices)
                  adf_p_value = adf_result[1]
                  if adf_p_value < 0.05: # Stationary -> likely ranging
                       if trend_regime.startswith('trending'): trend_regime = 'ranging' # Override if stationary
                  else: # Non-stationary -> likely trending
                       if trend_regime == 'ranging': trend_regime = 'trending_up' if slope > 0 else 'trending_down' # Override if non-stationary
             except Exception as adf_err:
                  logger.warning(f"ADF test failed: {adf_err}")

        # 3. Combine Trend and Volatility
        final_regime = 'unknown'
        if trend_regime.startswith('trending'):
            final_regime = f"{trend_regime}_{volatility_regime}" # e.g., trending_up_expansion
        else: # Ranging
            final_regime = f"{trend_regime}_{volatility_regime}" # e.g., ranging_contraction

        details = {
            'volatility_short': volatility_short, 'volatility_long': volatility_long, 'volatility_ratio': vol_ratio,
            'volatility_regime': volatility_regime, 'trend_regime': trend_regime,
            'slope': slope, 'r_squared': r_squared, 'adf_p_value': adf_p_value
        }
        # Confidence could be based on R-squared for trend or vol_ratio deviation for vol
        confidence = trend_confidence if trend_regime != 'ranging' else (1.0 - abs(1.0 - vol_ratio))

        return {'regime': final_regime, 'confidence': confidence, 'details': details}


# --- Sentiment Analyzer ---
class SentimentAnalyzer:
    """Analyzes market sentiment from technical indicators."""
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        # Add weights or config if analyzing multiple sources later

    def analyze(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze sentiment from technical indicators."""
        scores = []
        # RSI
        rsi = indicators.get('rsi_14') # Example key
        if rsi is not None:
            if rsi < 30: scores.append(1.0) # Bullish
            elif rsi > 70: scores.append(-1.0) # Bearish
            else: scores.append((50.0 - rsi) / 40.0) # Scale between -1 and 1

        # MACD
        macd_hist = indicators.get('macd_hist') # Example key
        if macd_hist is not None:
             # Simple: positive hist = bullish, negative = bearish
             # Scale based on magnitude relative to recent range? Needs more context.
             scores.append(np.sign(macd_hist) * min(abs(macd_hist) / 0.001, 1.0)) # Example scaling

        # Moving Average Cross
        sma_fast = indicators.get('sma_10') # Example key
        sma_slow = indicators.get('sma_20') # Example key
        if sma_fast is not None and sma_slow is not None:
             if sma_fast > sma_slow: scores.append(0.8) # Bullish cross
             elif sma_fast < sma_slow: scores.append(-0.8) # Bearish cross

        # Combine scores (simple average)
        overall_sentiment = np.mean(scores) if scores else 0.0

        # Categorize
        category = 'neutral'
        if overall_sentiment > 0.6: category = 'very_bullish'
        elif overall_sentiment > 0.2: category = 'bullish'
        elif overall_sentiment < -0.6: category = 'very_bearish'
        elif overall_sentiment < -0.2: category = 'bearish'

        return {'overall_sentiment': overall_sentiment, 'category': category}


# --- Deep Learning Model Handling ---
class TimeSeriesModels:
    """Handles building, training, loading, and predicting with TF/Keras models."""
    def __init__(self, model_save_path: Union[str, Path]):
        self.model_save_path = Path(model_save_path)
        self.models: Dict[str, Model] = {} # model_name -> Keras Model
        self.scalers: Dict[str, Any] = {} # scaler_name -> Scaler object
        self.model_save_path.mkdir(parents=True, exist_ok=True)
        if not TENSORFLOW_AVAILABLE:
             logger.warning("TensorFlow not available. Deep learning features disabled.")

    def build_lstm_model(self, model_name: str, sequence_length: int, n_features: int, n_outputs: int = 1) -> Optional[Model]:
        """Builds a standard LSTM model."""
        if not TENSORFLOW_AVAILABLE: return None
        try:
            model = Sequential([
                Input(shape=(sequence_length, n_features)),
                LSTM(64, return_sequences=True),
                BatchNormalization(),
                Dropout(0.2),
                LSTM(32),
                BatchNormalization(),
                Dropout(0.2),
                Dense(16, activation='relu'),
                Dense(n_outputs, activation='linear') # Linear for regression, change for classification
            ])
            model.compile(optimizer=Adam(learning_rate=0.001), loss='mse', metrics=['mae']) # Default for regression
            self.models[model_name] = model
            logger.info(f"Built LSTM model: {model_name}")
            model.summary(print_fn=logger.debug)
            return model
        except Exception as e:
            logger.exception(f"Error building LSTM model {model_name}: {e}")
            return None

    # Add build_gru_model similarly if needed

    def train_model(self, model_name: str, X_train: np.ndarray, y_train: np.ndarray,
                    X_val: np.ndarray, y_val: np.ndarray, epochs: int = 50, batch_size: int = 32) -> Optional[Dict]:
        """Trains a compiled Keras model."""
        if not TENSORFLOW_AVAILABLE: return None
        if model_name not in self.models:
            logger.error(f"Model {model_name} not found for training.")
            return None
        model = self.models[model_name]

        early_stopping = EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True, verbose=1)
        reduce_lr = ReduceLROnPlateau(monitor='val_loss', factor=0.2, patience=5, min_lr=1e-6, verbose=1)

        try:
            history = model.fit(
                X_train, y_train,
                validation_data=(X_val, y_val),
                epochs=epochs,
                batch_size=batch_size,
                callbacks=[early_stopping, reduce_lr],
                verbose=2 # Less verbose output
            )
            self.save_model(model_name) # Save best model found by early stopping
            logger.info(f"Finished training model {model_name}.")
            return history.history
        except Exception as e:
            logger.exception(f"Error training model {model_name}: {e}")
            return None

    def predict(self, model_name: str, X: np.ndarray) -> Optional[np.ndarray]:
        """Make predictions with a loaded model."""
        if not TENSORFLOW_AVAILABLE: return None
        if model_name not in self.models:
            logger.error(f"Model {model_name} not found for prediction.")
            return None
        model = self.models[model_name]
        try:
            return model.predict(X)
        except Exception as e:
            logger.exception(f"Error predicting with model {model_name}: {e}")
            return None

    def save_model(self, model_name: str):
        """Save Keras model."""
        if not TENSORFLOW_AVAILABLE or model_name not in self.models: return
        model_path = self.model_save_path / f"{model_name}.h5"
        try:
            self.models[model_name].save(model_path)
            logger.info(f"Saved Keras model to {model_path}")
        except Exception as e:
            logger.exception(f"Failed to save Keras model {model_name}: {e}")

    def load_model(self, model_name: str) -> bool:
        """Load Keras model."""
        if not TENSORFLOW_AVAILABLE: return False
        model_path = self.model_save_path / f"{model_name}.h5"
        if model_path.exists():
            try:
                self.models[model_name] = load_model(model_path)
                logger.info(f"Loaded Keras model from {model_path}")
                return True
            except Exception as e:
                logger.exception(f"Failed to load Keras model {model_name}: {e}")
        return False

    def save_scaler(self, scaler_name: str, scaler: Any):
        """Save a scaler object."""
        scaler_path = self.model_save_path / f"{scaler_name}_scaler.joblib"
        try:
            joblib.dump(scaler, scaler_path)
            logger.info(f"Saved scaler to {scaler_path}")
        except Exception as e:
            logger.exception(f"Failed to save scaler {scaler_name}: {e}")

    def load_scaler(self, scaler_name: str) -> bool:
        """Load a scaler object."""
        scaler_path = self.model_save_path / f"{scaler_name}_scaler.joblib"
        if scaler_path.exists():
            try:
                self.scalers[scaler_name] = joblib.load(scaler_path)
                logger.info(f"Loaded scaler from {scaler_path}")
                return True
            except Exception as e:
                logger.exception(f"Failed to load scaler {scaler_name}: {e}")
        return False


# --- Main Model Manager ---
class ModelManager:
    """Manages machine learning models, including training and prediction."""

    def __init__(
        self,
        model_save_path: Union[str, Path],
        model_features: List[str], # Explicitly require features list
        enable_machine_learning: bool = True,
        config: Optional[Dict[str, Any]] = None # Pass main config if needed by sub-components
    ):
        """Initialize model manager."""
        self.model_save_path = Path(model_save_path)
        self.enable_machine_learning = enable_machine_learning
        self.model_features = model_features
        self.config = config or {} # Store config if passed

        if not self.enable_machine_learning:
            logger.info("Machine Learning is disabled. ModelManager initialized in inactive state.")
            return

        self.model_save_path.mkdir(parents=True, exist_ok=True)

        # --- Traditional ML Models ---
        self.ml_models: Dict[str, Any] = {} # model_name -> model object
        self.feature_scalers: Dict[str, StandardScaler] = {} # model_name -> scaler
        self._init_ml_models() # Initialize model structures

        # --- Deep Learning Models ---
        self.dl_enabled = TENSORFLOW_AVAILABLE and get_config_value(self.config, 'MACHINE_LEARNING.deep_learning_enabled', False)
        self.time_series_models = TimeSeriesModels(self.model_save_path / 'deep_learning') if self.dl_enabled else None

        # --- Market Analysis Components ---
        self.regime_classifier = MarketRegimeClassifier(self.config)
        self.sentiment_analyzer = SentimentAnalyzer(self.config)

        # --- Training Data ---
        self.training_data: List[Dict] = [] # Stores processed trade data for training
        self.training_data_path = self.model_save_path / "training_data.pkl" # Use pickle for datetimes

        # --- State ---
        self.is_initialized = False
        self.walk_forward_results: Dict = {} # Store WFO results

    def _init_ml_models(self):
        """Define the structure of ML models to be used."""
        # Define models and their corresponding scalers
        self.ml_models = {
            'entry_signal': RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, class_weight='balanced'),
            'exit_signal': RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42, class_weight='balanced'),
            # Add more models as needed (e.g., for predicting price movement, volatility)
            # 'price_move_regressor': GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42),
        }
        self.feature_scalers = {name: StandardScaler() for name in self.ml_models}
        logger.debug(f"Initialized ML model structures: {list(self.ml_models.keys())}")

    async def initialize(self) -> bool:
        """Load models, scalers, and training data."""
        if not self.enable_machine_learning: return False
        if self.is_initialized: return True
        logger.info("Initializing Model Manager...")

        # Load ML models and scalers
        for name in self.ml_models.keys():
            self._load_ml_model(name)
            self._load_scaler(name)

        # Load DL models and scalers if enabled
        if self.dl_enabled and self.time_series_models:
            # Example: Load specific DL models defined in config
            dl_models_to_load = get_config_value(self.config, 'MACHINE_LEARNING.deep_learning_models', ['lstm_eurusd'])
            for model_name in dl_models_to_load:
                 self.time_series_models.load_model(model_name)
                 self.time_series_models.load_scaler(f"{model_name}_scaler") # Assuming scaler naming convention

        # Load training data
        self._load_training_data()

        self.is_initialized = True
        logger.info("Model Manager initialized successfully.")
        return True

    # --- Model/Scaler Loading & Saving (Internal Helpers) ---
    def _load_ml_model(self, model_name: str):
        model_path = self.model_save_path / f"{model_name}.joblib"
        if model_path.exists():
            try:
                self.ml_models[model_name] = joblib.load(model_path)
                logger.info(f"Loaded ML model '{model_name}' from {model_path}")
            except Exception as e: logger.warning(f"Failed to load ML model '{model_name}': {e}", exc_info=True)

    def _save_ml_model(self, model_name: str):
        if model_name in self.ml_models:
            model_path = self.model_save_path / f"{model_name}.joblib"
            try: joblib.dump(self.ml_models[model_name], model_path); logger.info(f"Saved ML model '{model_name}' to {model_path}")
            except Exception as e: logger.exception(f"Failed to save ML model '{model_name}': {e}")

    def _load_scaler(self, scaler_name: str):
        scaler_path = self.model_save_path / f"{scaler_name}_scaler.joblib"
        if scaler_path.exists():
            try: self.feature_scalers[scaler_name] = joblib.load(scaler_path); logger.info(f"Loaded scaler '{scaler_name}' from {scaler_path}")
            except Exception as e: logger.warning(f"Failed to load scaler '{scaler_name}': {e}", exc_info=True)

    def _save_scaler(self, scaler_name: str):
        if scaler_name in self.feature_scalers:
            scaler_path = self.model_save_path / f"{scaler_name}_scaler.joblib"
            try: joblib.dump(self.feature_scalers[scaler_name], scaler_path); logger.info(f"Saved scaler '{scaler_name}' to {scaler_path}")
            except Exception as e: logger.exception(f"Failed to save scaler '{scaler_name}': {e}")

    # --- Training Data Handling ---
    def _load_training_data(self):
        if self.training_data_path.exists():
            try:
                with open(self.training_data_path, 'rb') as f:
                    self.training_data = pickle.load(f)
                logger.info(f"Loaded {len(self.training_data)} training data points from {self.training_data_path}")
            except Exception as e:
                logger.warning(f"Failed to load training data: {e}", exc_info=True)
                self.training_data = [] # Start fresh if loading fails

    def _save_training_data(self):
        try:
            with open(self.training_data_path, 'wb') as f:
                pickle.dump(self.training_data, f)
            logger.debug(f"Saved {len(self.training_data)} training data points to {self.training_data_path}")
        except Exception as e:
            logger.error(f"Error saving training data: {e}", exc_info=True)

    async def train_with_trade(self, trade: Trade):
        """Add trade data for future training and trigger retraining if needed."""
        if not self.enable_machine_learning: return
        try:
            # Extract features and target from the trade
            features = trade.features or {}
            if not features: logger.warning(f"Trade {trade.id} has no features, cannot use for training."); return

            # Ensure all expected features are present, fill missing with 0 or NaN
            feature_values = {f: features.get(f) for f in self.model_features}

            # Define targets based on the trade outcome
            targets = {
                'entry_profitable': 1 if trade.profit_loss > 0 else 0,
                'exit_quality': 1 if trade.exit_reason == 'TP' else (0 if trade.exit_reason == 'SL' else 0.5), # Example target
                'price_move_pips': trade.profit_loss_pips,
                'duration_minutes': (trade.exit_time - trade.entry_time).total_seconds() / 60.0
            }

            training_point = {
                'timestamp': trade.exit_time, # Use exit time for sorting
                'instrument': trade.instrument,
                'features': feature_values,
                'targets': targets
            }
            self.training_data.append(training_point)
            self._save_training_data() # Save after adding

            # --- Retraining Logic ---
            min_samples = get_config_value(self.config, 'MACHINE_LEARNING.min_training_samples', 100)
            update_freq = get_config_value(self.config, 'MACHINE_LEARNING.model_update_frequency', 'daily') # daily, weekly, trade_count

            trigger_retrain = False
            if update_freq == 'trade_count':
                 retrain_interval = get_config_value(self.config, 'MACHINE_LEARNING.retrain_trade_interval', 50)
                 if len(self.training_data) >= min_samples and len(self.training_data) % retrain_interval == 0:
                      trigger_retrain = True
            # Add logic for 'daily', 'weekly' based on checking last retrain time

            if trigger_retrain:
                 logger.info(f"Triggering model retraining ({len(self.training_data)} samples)...")
                 # Run retraining in the background
                 asyncio.create_task(self.update_models())

        except Exception as e:
            logger.exception(f"Error adding trade to training data: {e}")

    async def update_models(self):
        """Retrain ML models using the collected training data."""
        if not self.enable_machine_learning or len(self.training_data) < get_config_value(self.config, 'MACHINE_LEARNING.min_training_samples', 100):
            logger.info("Skipping model update: Not enough training data.")
            return

        logger.info(f"Starting model update with {len(self.training_data)} data points...")
        try:
            # 1. Prepare DataFrames
            df = pd.DataFrame(self.training_data)
            feature_df = pd.json_normalize(df['features']).fillna(0) # Expand features, fill NaNs
            target_df = pd.json_normalize(df['targets'])
            # Ensure feature columns match self.model_features
            for col in self.model_features:
                 if col not in feature_df.columns: feature_df[col] = 0 # Add missing feature columns with 0
            feature_df = feature_df[self.model_features] # Select and order features

            X = feature_df.values
            # Example targets (adjust based on models defined in _init_ml_models)
            y_entry = target_df['entry_profitable'].values
            y_exit = target_df['exit_quality'].values
            # y_price = target_df['price_move_pips'].values

            # 2. Perform Walk-Forward Optimization or Simple Train/Test Split
            # Using TimeSeriesSplit for walk-forward validation
            n_splits = get_config_value(self.config, 'MACHINE_LEARNING.wfo_n_splits', 5)
            tscv = TimeSeriesSplit(n_splits=n_splits)
            wfo_results = {model_name: {'accuracy': [], 'f1': [], 'report': []} for model_name in ['entry_signal', 'exit_signal']}

            for train_index, test_index in tscv.split(X):
                X_train, X_test = X[train_index], X[test_index]
                y_entry_train, y_entry_test = y_entry[train_index], y_entry[test_index]
                y_exit_train, y_exit_test = y_exit[train_index], y_exit[test_index]

                # --- Train and Evaluate Entry Signal Model ---
                scaler_entry = self.feature_scalers['entry_signal']
                X_train_scaled = scaler_entry.fit_transform(X_train) # Fit on train
                X_test_scaled = scaler_entry.transform(X_test) # Transform test
                model_entry = self.ml_models['entry_signal']
                model_entry.fit(X_train_scaled, y_entry_train)
                preds = model_entry.predict(X_test_scaled)
                wfo_results['entry_signal']['accuracy'].append(accuracy_score(y_entry_test, preds))
                wfo_results['entry_signal']['f1'].append(f1_score(y_entry_test, preds, average='weighted'))
                # wfo_results['entry_signal']['report'].append(classification_report(y_entry_test, preds, output_dict=True))

                # --- Train and Evaluate Exit Signal Model ---
                scaler_exit = self.feature_scalers['exit_signal']
                X_train_scaled = scaler_exit.fit_transform(X_train)
                X_test_scaled = scaler_exit.transform(X_test)
                model_exit = self.ml_models['exit_signal']
                model_exit.fit(X_train_scaled, y_exit_train)
                preds = model_exit.predict(X_test_scaled)
                wfo_results['exit_signal']['accuracy'].append(accuracy_score(y_exit_test, preds))
                wfo_results['exit_signal']['f1'].append(f1_score(y_exit_test, preds, average='weighted'))
                # wfo_results['exit_signal']['report'].append(classification_report(y_exit_test, preds, output_dict=True))

            # Log average WFO results
            for model_name, results in wfo_results.items():
                 avg_acc = np.mean(results['accuracy'])
                 avg_f1 = np.mean(results['f1'])
                 logger.info(f"WFO Results for {model_name}: Avg Accuracy={avg_acc:.3f}, Avg F1={avg_f1:.3f}")
            self.walk_forward_results = wfo_results # Store latest results

            # 3. Final Training on All Data
            logger.info("Training final models on all available data...")
            for model_name in self.ml_models.keys():
                 scaler = self.feature_scalers[model_name]
                 X_scaled = scaler.fit_transform(X) # Fit scaler on all data
                 model = self.ml_models[model_name]
                 target_map = {'entry_signal': y_entry, 'exit_signal': y_exit} # Map model name to target
                 if model_name in target_map:
                      model.fit(X_scaled, target_map[model_name])
                      self._save_ml_model(model_name) # Save final model
                      self._save_scaler(model_name) # Save final scaler
                 else:
                      logger.warning(f"No target data mapping found for model '{model_name}' during final training.")

            logger.info("Model update process completed.")

        except Exception as e:
            logger.exception(f"Error updating models: {e}")

    # --- Prediction ---
    async def predict(self, input_features: Dict[str, Any]) -> Optional[TradingSignal]:
        """Generate trading signal based on input features using the 'entry_signal' model."""
        if not self.enable_machine_learning or not self.is_initialized: return None
        model_name = 'entry_signal'
        if model_name not in self.ml_models or model_name not in self.feature_scalers:
            logger.warning(f"Model or scaler '{model_name}' not available for prediction.")
            return None

        try:
            # 1. Prepare Feature Vector
            feature_vector_dict = {f: input_features.get(f, 0.0) for f in self.model_features} # Ensure all features exist
            feature_vector = np.array([list(feature_vector_dict.values())]) # Create 2D array

            # 2. Scale Features
            scaler = self.feature_scalers[model_name]
            X_scaled = scaler.transform(feature_vector)

            # 3. Predict Probability
            model = self.ml_models[model_name]
            probabilities = model.predict_proba(X_scaled)[0] # Get probabilities for class 0 and 1
            prob_enter = probabilities[1] # Probability of class 1 (profitable entry)

            # 4. Determine Signal based on Threshold
            threshold = get_config_value(self.config, 'MACHINE_LEARNING.prediction_confidence_threshold', 0.7)
            instrument = input_features.get('instrument', 'UNKNOWN') # Get instrument from input

            if prob_enter >= threshold:
                # Determine direction (requires another model or logic)
                # Placeholder: Assume long if prob > threshold, needs refinement
                direction = 'long' # TODO: Predict direction separately or use combined model
                signal_type = 'buy'

                # TODO: Predict SL/TP pips using regression models if available
                sl_pips = get_config_value(self.config, 'MACHINE_LEARNING.default_sl_pips', 30)
                tp_pips = get_config_value(self.config, 'MACHINE_LEARNING.default_tp_pips', 60)

                logger.info(f"ML Prediction for {instrument}: ENTER {direction} (Prob: {prob_enter:.3f} >= {threshold})")
                # Use the generate_signal helper from a base Strategy class or similar utility
                # This requires access to API/PositionManager or passing signal back to StrategyEngine
                # For now, just create the signal object
                return TradingSignal(
                    instrument=instrument,
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    signal_type=signal_type,
                    direction=direction,
                    confidence=prob_enter,
                    strategy_name="ML_Entry",
                    # SL/TP prices need calculation based on current price
                    suggested_stop_loss=None, # Calculate based on sl_pips and current price
                    suggested_take_profit=None, # Calculate based on tp_pips and current price
                    features={'ml_confidence': prob_enter, 'sl_pips': sl_pips, 'tp_pips': tp_pips, **feature_vector_dict}
                )
            else:
                # logger.debug(f"ML Prediction for {instrument}: HOLD (Prob: {prob_enter:.3f} < {threshold})")
                return None

        except Exception as e:
            logger.exception(f"Error during ML prediction for {input_features.get('instrument', 'UNKNOWN')}: {e}")
            return None

    async def predict_exit(self, position: Position, current_features: Dict[str, Any]) -> Tuple[bool, float, Optional[str]]:
        """Predict whether a position should be exited using the 'exit_signal' model."""
        if not self.enable_machine_learning or not self.is_initialized: return False, 0.0, None
        model_name = 'exit_signal'
        if model_name not in self.ml_models or model_name not in self.feature_scalers:
            logger.warning(f"Model or scaler '{model_name}' not available for exit prediction.")
            return False, 0.0, None

        try:
            # 1. Prepare Feature Vector (Combine position entry features with current features)
            features = position.features or {} # Features at time of entry
            features.update(current_features) # Add current market features
            # Add position-specific runtime features
            features['hours_in_trade'] = (datetime.datetime.now(datetime.timezone.utc) - position.entry_time).total_seconds() / 3600.0
            features['unrealized_pl_percent'] = (position.unrealized_pl / (position.entry_price * position.quantity * 1000)) * 100 if position.entry_price else 0 # Rough % P/L

            feature_vector_dict = {f: features.get(f, 0.0) for f in self.model_features}
            feature_vector = np.array([list(feature_vector_dict.values())])

            # 2. Scale Features
            scaler = self.feature_scalers[model_name]
            X_scaled = scaler.transform(feature_vector)

            # 3. Predict Probability
            model = self.ml_models[model_name]
            probabilities = model.predict_proba(X_scaled)[0]
            prob_exit = probabilities[1] # Probability of class 1 (good exit)

            # 4. Determine Action
            # Exit if probability of *good* exit is low? Or probability of *bad* state is high?
            # Let's assume model predicts probability of "exit now" signal (class 1)
            exit_threshold = get_config_value(self.config, 'MACHINE_LEARNING.exit_confidence_threshold', 0.6)
            should_exit = prob_exit >= exit_threshold
            reason = f"ML Exit Signal (Prob: {prob_exit:.3f})" if should_exit else None

            if should_exit:
                 logger.info(f"ML Exit Prediction for {position.instrument} (ID: {position.id}): EXIT (Prob: {prob_exit:.3f} >= {exit_threshold})")

            return should_exit, prob_exit, reason

        except Exception as e:
            logger.exception(f"Error during ML exit prediction for position {position.id}: {e}")
            return False, 0.0, None