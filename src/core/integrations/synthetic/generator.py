"""Synthetic energy data generator with realistic patterns."""

import numpy as np
import pandas as pd
from typing import Optional
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class SyntheticDataConfig:
    """Configuration for synthetic energy data generation."""
    
    # Time parameters
    start_date: str = "2023-01-01"
    end_date: str = "2023-12-31"
    frequency: str = "1h"
    
    # Randomization
    random_seed: int = 42
    
    # Pattern parameters
    base_demand_mw: float = 1000.0
    daily_variation: float = 0.2
    weekly_variation: float = 0.1
    seasonal_variation: float = 0.3
    noise_level: float = 0.05
    
    # Temperature parameters
    base_temperature_c: float = 15.0
    daily_temp_variation: float = 10.0
    seasonal_temp_variation: float = 20.0
    
    # Renewable parameters
    solar_capacity_factor: float = 0.25
    wind_capacity_factor: float = 0.35
    weather_variability: float = 0.3


class SyntheticEnergyDataGenerator:
    """Generate realistic synthetic energy time series data."""

    def __init__(self, config: SyntheticDataConfig):
        """Initialize generator with configuration.
        
        Args:
            config: Configuration for data generation
        """
        self.config = config
        self.rng = np.random.RandomState(config.random_seed)

    def generate_demand_pattern(self, hours: int) -> np.ndarray:
        """Generate realistic electricity demand with daily/seasonal patterns.
        
        Args:
            hours: Number of hours to generate
            
        Returns:
            Array of demand values in MW
        """
        t = np.arange(hours)

        # Daily pattern (peak in evening around 18:00)
        daily = self.config.daily_variation * np.sin(2 * np.pi * t / 24 - np.pi/3)

        # Weekly pattern (lower demand on weekends)
        weekly = self.config.weekly_variation * np.sin(2 * np.pi * t / (24 * 7))

        # Seasonal pattern (higher in summer/winter for cooling/heating)
        seasonal = self.config.seasonal_variation * np.cos(2 * np.pi * t / (24 * 365.25))

        # Base load with random variation
        base = self.config.base_demand_mw * (1.0 + self.config.noise_level * self.rng.randn(hours))

        # Combine all patterns
        demand = base * (1 + daily + weekly + seasonal)

        # Ensure positive values
        demand = np.maximum(demand, self.config.base_demand_mw * 0.1)

        return demand

    def generate_renewable_generation(self, hours: int, source: str = "solar") -> np.ndarray:
        """Generate renewable generation patterns.
        
        Args:
            hours: Number of hours to generate
            source: Type of renewable source ('solar' or 'wind')
            
        Returns:
            Array of generation values in MW
        """
        t = np.arange(hours)

        if source == "solar":
            # Solar: daylight hours only, seasonal variation
            daily = np.maximum(0, np.sin(2 * np.pi * t / 24 - np.pi/2))
            # Fix seasonal pattern: higher in summer (middle of year)
            seasonal = 0.5 + 0.5 * np.sin(2 * np.pi * t / (24 * 365.25) - np.pi/2)
            capacity_factor = self.config.solar_capacity_factor
            
            # Base generation pattern
            generation = capacity_factor * daily * seasonal
            
            # Add weather variability (cloud cover effects)
            weather_factor = 0.7 + 0.3 * self.rng.beta(2, 2, hours)
            generation *= weather_factor

        elif source == "wind":
            # Wind: more variable, less predictable patterns
            # Weekly pattern (weather systems)
            weekly_wind = 0.3 + 0.2 * np.sin(2 * np.pi * t / (24 * 7))
            
            # Random wind variations (Rayleigh distribution is common for wind)
            wind_variations = self.rng.rayleigh(0.5, hours)
            
            generation = self.config.wind_capacity_factor * (weekly_wind + 0.3 * wind_variations)
            generation = np.minimum(generation, self.config.wind_capacity_factor * 2)

        else:
            raise ValueError(f"Unknown renewable source: {source}")

        return np.maximum(generation, 0.0)

    def generate_temperature(self, hours: int) -> np.ndarray:
        """Generate temperature data with seasonal patterns.
        
        Args:
            hours: Number of hours to generate
            
        Returns:
            Array of temperature values in Celsius
        """
        t = np.arange(hours)

        # Daily temperature variation (warmer in afternoon)
        daily = self.config.daily_temp_variation * np.sin(2 * np.pi * t / 24)

        # Seasonal variation (summer/winter cycle)
        seasonal = self.config.seasonal_temp_variation * np.cos(2 * np.pi * t / (24 * 365.25))

        # Base temperature with random variation
        base = self.config.base_temperature_c + 2 * self.rng.randn(hours)

        return base + daily + seasonal

    def generate_price_data(self, demand: np.ndarray, renewable: np.ndarray) -> np.ndarray:
        """Generate electricity prices based on supply/demand dynamics.
        
        Args:
            demand: Demand values in MW
            renewable: Total renewable generation in MW
            
        Returns:
            Array of price values in $/MWh
        """
        # Base price
        base_price = 50.0  # $/MWh

        # Price increases with demand (demand stress)
        demand_normalized = (demand - np.mean(demand)) / np.std(demand)
        demand_factor = 30 * demand_normalized

        # Price decreases with renewable generation (merit order effect)
        renewable_normalized = renewable / np.mean(renewable) if np.mean(renewable) > 0 else renewable
        renewable_factor = -20 * renewable_normalized

        # Add price volatility
        volatility = 10 * self.rng.randn(len(demand))

        price = base_price + demand_factor + renewable_factor + volatility

        # Ensure non-negative prices (though negative prices can occur in real markets)
        return np.maximum(price, 5.0)

    def generate_grid_stability_metric(self, demand: np.ndarray, renewable: np.ndarray) -> np.ndarray:
        """Generate a grid stability metric (0-1 scale).
        
        Args:
            demand: Demand values in MW
            renewable: Total renewable generation in MW
            
        Returns:
            Array of stability values (0-1, higher is more stable)
        """
        # Stability decreases with high renewable variability
        renewable_variability = np.abs(np.diff(renewable, prepend=renewable[0]))
        renewable_variability = renewable_variability / np.max(renewable_variability)

        # Stability decreases with extreme demand
        demand_stress = np.abs(demand - np.median(demand)) / np.std(demand)
        demand_stress = np.minimum(demand_stress, 3.0) / 3.0  # Cap at 3 standard deviations

        # Base stability
        base_stability = 0.8
        stability = base_stability - 0.2 * renewable_variability - 0.1 * demand_stress

        # Add random grid events (outages, equipment failures)
        grid_events = self.rng.exponential(0.01, len(demand))
        stability -= grid_events

        return np.clip(stability, 0.0, 1.0)

    def generate_full_dataset(self, hours: Optional[int] = None) -> pd.DataFrame:
        """Generate a complete synthetic energy dataset.
        
        Args:
            hours: Number of hours to generate (if None, calculated from config dates)
            
        Returns:
            DataFrame with all synthetic energy data
        """
        if hours is None:
            start = pd.to_datetime(self.config.start_date)
            end = pd.to_datetime(self.config.end_date)
            hours = int((end - start).total_seconds() / 3600)

        logger.info(f"Generating {hours} hours of synthetic energy data")

        # Generate all components
        demand = self.generate_demand_pattern(hours)
        solar = self.generate_renewable_generation(hours, "solar")
        wind = self.generate_renewable_generation(hours, "wind")
        temperature = self.generate_temperature(hours)
        
        # Calculate derived metrics
        total_renewable = solar + wind
        price = self.generate_price_data(demand, total_renewable)
        stability = self.generate_grid_stability_metric(demand, total_renewable)

        # Create timestamps
        start = pd.to_datetime(self.config.start_date)
        timestamps = pd.date_range(start, periods=hours, freq=self.config.frequency)

        # Create DataFrame
        data = pd.DataFrame({
            "datetime": timestamps,
            "demand_mw": demand,
            "solar_generation_mw": solar,
            "wind_generation_mw": wind,
            "temperature_c": temperature,
            "price_usd_per_mwh": price,
            "grid_stability": stability
        })

        logger.info(f"Generated synthetic dataset with {len(data)} records")
        return data

    def generate_dataset_with_region(self, region: str = "SYNTHETIC", hours: Optional[int] = None) -> pd.DataFrame:
        """Generate synthetic dataset with region identifier.
        
        Args:
            region: Region identifier for the synthetic data
            hours: Number of hours to generate
            
        Returns:
            DataFrame with region column added
        """
        df = self.generate_full_dataset(hours)
        df["region"] = region
        return df


# Convenience functions for common use cases
def generate_synthetic_week(region: str = "SYNTHETIC") -> pd.DataFrame:
    """Generate one week of synthetic data."""
    config = SyntheticDataConfig(
        start_date="2023-01-01",
        end_date="2023-01-08"
    )
    generator = SyntheticEnergyDataGenerator(config)
    return generator.generate_dataset_with_region(region)


def generate_synthetic_month(region: str = "SYNTHETIC") -> pd.DataFrame:
    """Generate one month of synthetic data."""
    config = SyntheticDataConfig(
        start_date="2023-01-01",
        end_date="2023-02-01"
    )
    generator = SyntheticEnergyDataGenerator(config)
    return generator.generate_dataset_with_region(region)


def generate_synthetic_year(region: str = "SYNTHETIC") -> pd.DataFrame:
    """Generate one year of synthetic data."""
    config = SyntheticDataConfig(
        start_date="2023-01-01",
        end_date="2024-01-01"
    )
    generator = SyntheticEnergyDataGenerator(config)
    return generator.generate_dataset_with_region(region)
