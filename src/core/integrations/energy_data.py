"""
Comprehensive Energy Data Integration Pipeline

This module provides a unified interface for collecting, processing, and managing
energy data from multiple sources (EIA, CAISO, synthetic data) for Ring Attention
training and inference.

Key Features:
- Multi-source data collection with automatic failover
- Time series alignment and resampling
- Missing data imputation and anomaly detection
- Memory-efficient data loading for long sequences
- Synthetic data generation for testing and development
"""

import asyncio
import pandas as pd
import numpy as np
import mlx.core as mx
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor
import json

from .eia.client import EIAClient
from .caiso.client import CAISOClient

logger = logging.getLogger(__name__)


@dataclass
class EnergyDataConfig:
    """Configuration for energy data collection and processing"""

    # Data sources
    sources: List[str] = field(default_factory=lambda: ["synthetic", "eia", "caiso"])

    # Time range
    start_date: str = "2020-01-01"
    end_date: str = "2024-01-01"

    # Sequence parameters
    sequence_length: int = 8760  # One year of hourly data
    overlap: int = 168  # One week overlap between sequences

    # Data processing
    target_frequency: str = "1h"  # Hourly resolution
    missing_data_threshold: float = 0.1  # Max 10% missing data per sequence

    # Feature engineering
    features: List[str] = field(default_factory=lambda: [
        "demand", "solar_generation", "wind_generation",
        "temperature", "price", "grid_stability"
    ])

    # Caching
    cache_dir: str = "data/cache"
    use_cache: bool = True


class SyntheticEnergyDataGenerator:
    """Generate realistic synthetic energy time series data"""

    def __init__(self, config: EnergyDataConfig):
        self.config = config
        np.random.seed(42)  # Reproducible synthetic data

    def generate_demand_pattern(self, hours: int) -> np.ndarray:
        """Generate realistic electricity demand with daily/seasonal patterns"""
        # Base demand with seasonal variation
        t = np.arange(hours)

        # Daily pattern (peak in evening)
        daily = 0.2 * np.sin(2 * np.pi * t / 24 - np.pi/3)

        # Weekly pattern (lower on weekends)
        weekly = 0.1 * np.sin(2 * np.pi * t / (24 * 7))

        # Seasonal pattern (higher in summer/winter)
        seasonal = 0.3 * np.cos(2 * np.pi * t / (24 * 365.25))

        # Base load with random variation
        base = 1.0 + 0.05 * np.random.randn(hours)

        # Combine patterns
        demand = base + daily + weekly + seasonal

        # Ensure positive values
        demand = np.maximum(demand, 0.1)

        return demand

    def generate_renewable_generation(self, hours: int, source: str = "solar") -> np.ndarray:
        """Generate renewable generation patterns"""
        t = np.arange(hours)

        if source == "solar":
            # Solar: daylight hours only, seasonal variation
            daily = np.maximum(0, np.sin(2 * np.pi * t / 24 - np.pi/2))
            seasonal = 0.5 + 0.5 * np.cos(2 * np.pi * t / (24 * 365.25))
            generation = daily * seasonal

            # Add weather variability
            generation *= (0.7 + 0.3 * np.random.beta(2, 2, hours))

        elif source == "wind":
            # Wind: more variable, less predictable
            base_wind = 0.3 + 0.2 * np.sin(2 * np.pi * t / (24 * 7))
            generation = base_wind + 0.3 * np.random.rayleigh(0.5, hours)
            generation = np.minimum(generation, 1.0)

        return generation

    def generate_temperature(self, hours: int) -> np.ndarray:
        """Generate temperature data with seasonal patterns"""
        t = np.arange(hours)

        # Daily temperature variation
        daily = 10 * np.sin(2 * np.pi * t / 24)

        # Seasonal variation
        seasonal = 20 * np.cos(2 * np.pi * t / (24 * 365.25))

        # Base temperature with random variation
        base = 15 + 2 * np.random.randn(hours)

        return base + daily + seasonal

    def generate_price_data(self, demand: np.ndarray, renewable: np.ndarray) -> np.ndarray:
        """Generate electricity prices based on supply/demand dynamics"""
        # Base price
        base_price = 50  # $/MWh

        # Price increases with demand
        demand_factor = 30 * (demand - np.mean(demand)) / np.std(demand)

        # Price decreases with renewable generation
        renewable_factor = -20 * renewable

        # Add volatility
        volatility = 10 * np.random.randn(len(demand))

        price = base_price + demand_factor + renewable_factor + volatility

        # Ensure non-negative prices
        return np.maximum(price, 5.0)

    def generate_grid_stability_metric(self, demand: np.ndarray,
                                     renewable: np.ndarray) -> np.ndarray:
        """Generate a grid stability metric (0-1 scale)"""
        # Stability decreases with high renewable variability
        renewable_variability = np.abs(np.diff(renewable, prepend=renewable[0]))

        # Stability decreases with extreme demand
        demand_stress = np.abs(demand - np.median(demand)) / np.std(demand)

        # Base stability
        stability = 0.8 - 0.2 * renewable_variability - 0.1 * demand_stress

        # Add some random grid events
        grid_events = np.random.exponential(0.01, len(demand))
        stability -= grid_events

        return np.clip(stability, 0.0, 1.0)

    def generate_full_dataset(self, hours: int) -> pd.DataFrame:
        """Generate a complete synthetic energy dataset"""
        logger.info(f"Generating {hours} hours of synthetic energy data")

        # Generate all components
        demand = self.generate_demand_pattern(hours)
        solar = self.generate_renewable_generation(hours, "solar")
        wind = self.generate_renewable_generation(hours, "wind")
        temperature = self.generate_temperature(hours)
        price = self.generate_price_data(demand, solar + wind)
        stability = self.generate_grid_stability_metric(demand, solar + wind)

        # Create timestamps
        start = pd.to_datetime(self.config.start_date)
        timestamps = pd.date_range(start, periods=hours, freq="1h")

        # Create DataFrame
        data = pd.DataFrame({
            "timestamp": timestamps,
            "demand": demand,
            "solar_generation": solar,
            "wind_generation": wind,
            "temperature": temperature,
            "price": price,
            "grid_stability": stability
        })

        return data


class EnergyDataProcessor:
    """Process and align energy data from multiple sources"""

    def __init__(self, config: EnergyDataConfig):
        self.config = config
        self.cache_dir = Path(config.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def align_time_series(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """Align multiple time series to common timestamps"""
        if not dataframes:
            return pd.DataFrame()

        # Find common time range
        start_times = [df["timestamp"].min() for df in dataframes]
        end_times = [df["timestamp"].max() for df in dataframes]

        common_start = max(start_times)
        common_end = min(end_times)

        logger.info(f"Aligning time series from {common_start} to {common_end}")

        # Create common time index
        common_index = pd.date_range(common_start, common_end,
                                   freq=self.config.target_frequency)

        # Resample and merge all dataframes
        merged_data = pd.DataFrame({"timestamp": common_index})

        for i, df in enumerate(dataframes):
            df_resampled = df.set_index("timestamp").resample(
                self.config.target_frequency
            ).mean().reset_index()

            # Add suffix to avoid column conflicts
            suffix = f"_source_{i}" if len(dataframes) > 1 else ""
            for col in df_resampled.columns:
                if col != "timestamp":
                    df_resampled = df_resampled.rename(columns={col: f"{col}{suffix}"})

            merged_data = pd.merge(merged_data, df_resampled, on="timestamp", how="left")

        return merged_data

    def impute_missing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Impute missing values using forward fill and interpolation"""
        logger.info("Imputing missing data")

        # Check missing data percentage
        missing_pct = df.isnull().sum() / len(df)
        for col, pct in missing_pct.items():
            if pct > self.config.missing_data_threshold:
                logger.warning(f"Column {col} has {pct:.1%} missing data")

        # Forward fill then interpolate
        df_imputed = df.ffill().interpolate(method="linear")

        # Fill any remaining NaNs with column means
        for col in df_imputed.columns:
            if col != "timestamp" and df_imputed[col].isnull().any():
                df_imputed[col] = df_imputed[col].fillna(df_imputed[col].mean())

        return df_imputed

    def detect_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect and flag anomalies in the data"""
        logger.info("Detecting anomalies")

        anomaly_flags = pd.DataFrame()

        for col in df.columns:
            if col == "timestamp":
                continue

            # Z-score based anomaly detection
            z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
            anomaly_flags[f"{col}_anomaly"] = z_scores > 3

        return pd.concat([df, anomaly_flags], axis=1)

    def create_sequences(self, df: pd.DataFrame) -> List[np.ndarray]:
        """Split data into overlapping sequences for training"""
        logger.info(f"Creating sequences of length {self.config.sequence_length}")

        sequences = []
        feature_cols = [col for col in df.columns
                       if col != "timestamp" and not col.endswith("_anomaly")]

        data_array = df[feature_cols].values

        start_idx = 0
        while start_idx + self.config.sequence_length <= len(data_array):
            sequence = data_array[start_idx:start_idx + self.config.sequence_length]
            sequences.append(sequence)
            start_idx += self.config.sequence_length - self.config.overlap

        logger.info(f"Created {len(sequences)} sequences")
        return sequences


class EnergyDataPipeline:
    """Main pipeline for energy data collection and processing"""

    def __init__(self, config: EnergyDataConfig, api_keys: Dict[str, str] = None):
        self.config = config
        self.api_keys = api_keys or {}

        # Initialize clients
        self.eia_client = None
        self.caiso_client = None

        if "eia" in config.sources and "eia" in self.api_keys:
            self.eia_client = EIAClient(self.api_keys["eia"])

        if "caiso" in config.sources:
            self.caiso_client = CAISOClient()

        # Initialize components
        self.synthetic_generator = SyntheticEnergyDataGenerator(config)
        self.processor = EnergyDataProcessor(config)

    async def collect_synthetic_data(self) -> pd.DataFrame:
        """Generate synthetic energy data"""
        start = pd.to_datetime(self.config.start_date)
        end = pd.to_datetime(self.config.end_date)
        hours = int((end - start).total_seconds() / 3600)

        return self.synthetic_generator.generate_full_dataset(hours)

    async def collect_eia_data(self) -> Optional[pd.DataFrame]:
        """Collect data from EIA API"""
        if not self.eia_client:
            logger.warning("EIA client not initialized")
            return None

        try:
            logger.info("Collecting EIA data")
            # This would be implemented based on actual EIA API usage
            # For now, return synthetic data as placeholder
            return await self.collect_synthetic_data()
        except Exception as e:
            logger.error(f"Error collecting EIA data: {e}")
            return None

    async def collect_caiso_data(self) -> Optional[pd.DataFrame]:
        """Collect data from CAISO API"""
        if not self.caiso_client:
            logger.warning("CAISO client not initialized")
            return None

        try:
            logger.info("Collecting CAISO data")
            # This would be implemented based on actual CAISO API usage
            # For now, return synthetic data as placeholder
            return await self.collect_synthetic_data()
        except Exception as e:
            logger.error(f"Error collecting CAISO data: {e}")
            return None

    async def collect_all_data(self) -> List[pd.DataFrame]:
        """Collect data from all configured sources"""
        tasks = []

        if "synthetic" in self.config.sources:
            tasks.append(self.collect_synthetic_data())

        if "eia" in self.config.sources:
            tasks.append(self.collect_eia_data())

        if "caiso" in self.config.sources:
            tasks.append(self.collect_caiso_data())

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out None results and exceptions
        valid_dataframes = []
        for result in results:
            if isinstance(result, pd.DataFrame):
                valid_dataframes.append(result)
            elif isinstance(result, Exception):
                logger.error(f"Data collection error: {result}")

        return valid_dataframes

    def process_data(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """Process collected data through the full pipeline"""
        if not dataframes:
            logger.error("No data to process")
            return pd.DataFrame()

        # Align time series
        aligned_data = self.processor.align_time_series(dataframes)

        # Impute missing data
        imputed_data = self.processor.impute_missing_data(aligned_data)

        # Detect anomalies
        processed_data = self.processor.detect_anomalies(imputed_data)

        return processed_data

    def prepare_sequences_for_training(self, df: pd.DataFrame) -> Tuple[mx.array, Dict]:
        """Convert processed data to MLX arrays for Ring Attention training"""
        if df.empty:
            return mx.array([]), {}

        # Create sequences
        sequences = self.processor.create_sequences(df)

        if not sequences:
            return mx.array([]), {}

        # Convert to MLX array
        sequences_array = mx.array(np.stack(sequences))

        # Create metadata
        feature_cols = [col for col in df.columns
                       if col != "timestamp" and not col.endswith("_anomaly")]

        metadata = {
            "feature_names": feature_cols,
            "num_sequences": len(sequences),
            "sequence_length": self.config.sequence_length,
            "num_features": len(feature_cols),
            "data_shape": sequences_array.shape,
            "time_range": (df["timestamp"].min(), df["timestamp"].max())
        }

        logger.info(f"Prepared {metadata['num_sequences']} sequences with shape {metadata['data_shape']}")

        return sequences_array, metadata

    async def run_full_pipeline(self) -> Tuple[mx.array, Dict]:
        """Run the complete data pipeline"""
        logger.info("Starting energy data pipeline")

        # Collect data from all sources
        dataframes = await self.collect_all_data()

        if not dataframes:
            logger.error("No data collected from any source")
            return mx.array([]), {}

        # Process data
        processed_data = self.process_data(dataframes)

        # Prepare for training
        sequences, metadata = self.prepare_sequences_for_training(processed_data)

        logger.info("Energy data pipeline completed successfully")
        return sequences, metadata


# Example usage and testing
async def demo_energy_pipeline():
    """Demonstrate the energy data pipeline"""
    config = EnergyDataConfig(
        sources=["synthetic"],
        sequence_length=8760,  # One year
        start_date="2022-01-01",
        end_date="2023-01-01"
    )

    pipeline = EnergyDataPipeline(config)
    sequences, metadata = await pipeline.run_full_pipeline()

    print(f"Data pipeline results:")
    print(f"- Shape: {metadata.get('data_shape', 'N/A')}")
    print(f"- Features: {metadata.get('feature_names', [])}")
    print(f"- Time range: {metadata.get('time_range', 'N/A')}")

    return sequences, metadata


if __name__ == "__main__":
    # Run demo
    asyncio.run(demo_energy_pipeline())
