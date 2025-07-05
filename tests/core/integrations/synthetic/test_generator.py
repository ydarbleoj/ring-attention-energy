"""Tests for synthetic energy data generator."""

import pytest
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from src.core.integrations.synthetic.generator import (
    SyntheticEnergyDataGenerator,
    SyntheticDataConfig,
    generate_synthetic_week,
    generate_synthetic_month,
    generate_synthetic_year
)


class TestSyntheticDataConfig:
    """Test synthetic data configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        config = SyntheticDataConfig()
        
        assert config.start_date == "2023-01-01"
        assert config.end_date == "2023-12-31"
        assert config.frequency == "1h"
        assert config.random_seed == 42
        assert config.base_demand_mw == 1000.0

    def test_custom_config(self):
        """Test custom configuration values."""
        config = SyntheticDataConfig(
            start_date="2022-01-01",
            base_demand_mw=500.0,
            random_seed=123
        )
        
        assert config.start_date == "2022-01-01"
        assert config.base_demand_mw == 500.0
        assert config.random_seed == 123


class TestSyntheticEnergyDataGenerator:
    """Test synthetic energy data generation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = SyntheticDataConfig(
            start_date="2023-01-01",
            end_date="2023-01-08",  # One week
            random_seed=42
        )
        self.generator = SyntheticEnergyDataGenerator(self.config)

    def test_generator_initialization(self):
        """Test generator initialization."""
        assert self.generator.config == self.config
        
        # Test reproducibility
        gen1 = SyntheticEnergyDataGenerator(self.config)
        gen2 = SyntheticEnergyDataGenerator(self.config)
        
        demand1 = gen1.generate_demand_pattern(24)
        demand2 = gen2.generate_demand_pattern(24)
        
        np.testing.assert_array_equal(demand1, demand2)

    def test_demand_pattern_generation(self):
        """Test electricity demand pattern generation."""
        hours = 168  # One week
        demand = self.generator.generate_demand_pattern(hours)

        # Basic checks
        assert len(demand) == hours
        assert np.all(demand > 0), "Demand should always be positive"
        assert np.all(demand < 5000), "Demand should be reasonable scale"

        # Check for patterns
        assert np.std(demand) > 0, "Demand should have variation"
        
        # Check daily patterns (should have peaks roughly every 24 hours)
        daily_maxima = []
        for day in range(7):
            day_start = day * 24
            day_end = (day + 1) * 24
            day_demand = demand[day_start:day_end]
            daily_maxima.append(np.max(day_demand))

        assert len(daily_maxima) == 7
        assert np.std(daily_maxima) > 0, "Daily maxima should vary"

    def test_solar_generation_patterns(self):
        """Test solar generation patterns."""
        hours = 48  # Two days
        solar = self.generator.generate_renewable_generation(hours, "solar")

        # Basic checks
        assert len(solar) == hours
        assert np.all(solar >= 0), "Solar generation should be non-negative"
        assert np.all(solar <= 1.0), "Solar generation should be reasonable"

        # Solar should be zero at night (hours 0-6 and 18-24 roughly)
        night_hours = list(range(0, 6)) + list(range(18, 24)) + list(range(24, 30)) + list(range(42, 48))
        day_hours = list(range(8, 16)) + list(range(32, 40))
        
        night_solar = solar[night_hours]
        day_solar = solar[day_hours]
        
        # Night should be generally lower than day
        assert np.mean(night_solar) < np.mean(day_solar)

    def test_wind_generation_patterns(self):
        """Test wind generation patterns."""
        hours = 48  # Two days
        wind = self.generator.generate_renewable_generation(hours, "wind")

        # Basic checks
        assert len(wind) == hours
        assert np.all(wind >= 0), "Wind generation should be non-negative"
        assert np.all(wind <= 1.0), "Wind generation should be reasonable"

        # Wind should have variation
        assert np.std(wind) > 0, "Wind should have variation"

    def test_invalid_renewable_source(self):
        """Test invalid renewable source raises error."""
        with pytest.raises(ValueError, match="Unknown renewable source"):
            self.generator.generate_renewable_generation(24, "invalid")

    def test_temperature_patterns(self):
        """Test temperature data generation."""
        hours = 72  # Three days
        temperature = self.generator.generate_temperature(hours)

        # Basic checks
        assert len(temperature) == hours
        assert -30 < np.min(temperature) < 60, "Temperature should be in reasonable range"
        assert -30 < np.max(temperature) < 60, "Temperature should be in reasonable range"

        # Should have daily variation
        temp_std = np.std(temperature)
        assert temp_std > 5, "Temperature should have reasonable daily variation"

    def test_price_data_generation(self):
        """Test electricity price generation."""
        hours = 48
        demand = self.generator.generate_demand_pattern(hours)
        renewable = self.generator.generate_renewable_generation(hours, "solar")

        price = self.generator.generate_price_data(demand, renewable)

        # Basic checks
        assert len(price) == hours
        assert np.all(price > 0), "Prices should be positive"
        assert np.all(price < 500), "Prices should be reasonable (< $500/MWh)"

        # Price should correlate with demand (generally)
        demand_high_idx = demand > np.percentile(demand, 75)
        demand_low_idx = demand < np.percentile(demand, 25)

        high_demand_prices = price[demand_high_idx]
        low_demand_prices = price[demand_low_idx]

        # This should generally be true due to our price model
        assert np.mean(high_demand_prices) > np.mean(low_demand_prices)

    def test_grid_stability_metric(self):
        """Test grid stability metric generation."""
        hours = 48
        demand = self.generator.generate_demand_pattern(hours)
        renewable = self.generator.generate_renewable_generation(hours, "wind")

        stability = self.generator.generate_grid_stability_metric(demand, renewable)

        # Basic checks
        assert len(stability) == hours
        assert np.all(stability >= 0), "Stability should be non-negative"
        assert np.all(stability <= 1.0), "Stability should not exceed 1.0"

        # Should have reasonable distribution
        assert 0.3 < np.mean(stability) < 0.9, "Average stability should be reasonable"

    def test_full_dataset_generation(self):
        """Test full synthetic dataset generation."""
        hours = 168  # One week
        dataset = self.generator.generate_full_dataset(hours)

        # Basic checks
        assert isinstance(dataset, pd.DataFrame)
        assert len(dataset) == hours

        # Check all required columns
        expected_columns = [
            "datetime", "demand_mw", "solar_generation_mw", "wind_generation_mw",
            "temperature_c", "price_usd_per_mwh", "grid_stability"
        ]
        for col in expected_columns:
            assert col in dataset.columns, f"Missing column: {col}"

        # Check data types and ranges
        assert pd.api.types.is_datetime64_any_dtype(dataset["datetime"])
        assert np.all(dataset["demand_mw"] > 0)
        assert np.all(dataset["solar_generation_mw"] >= 0)
        assert np.all(dataset["wind_generation_mw"] >= 0)
        assert np.all(dataset["price_usd_per_mwh"] > 0)
        assert np.all(dataset["grid_stability"] >= 0)
        assert np.all(dataset["grid_stability"] <= 1.0)

        # Check timestamps are consecutive
        time_diffs = dataset["datetime"].diff().dropna()
        assert all(time_diffs == pd.Timedelta(hours=1))

    def test_dataset_with_region(self):
        """Test dataset generation with region identifier."""
        region = "TEST_REGION"
        dataset = self.generator.generate_dataset_with_region(region, hours=24)

        assert "region" in dataset.columns
        assert all(dataset["region"] == region)

    def test_automatic_hours_calculation(self):
        """Test automatic hours calculation from config dates."""
        config = SyntheticDataConfig(
            start_date="2023-01-01",
            end_date="2023-01-02"  # 24 hours
        )
        generator = SyntheticEnergyDataGenerator(config)
        dataset = generator.generate_full_dataset()

        assert len(dataset) == 24


class TestConvenienceFunctions:
    """Test convenience functions for common use cases."""

    def test_generate_synthetic_week(self):
        """Test synthetic week generation."""
        df = generate_synthetic_week("TEST_REGION")
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 168  # 7 days * 24 hours
        assert "region" in df.columns
        assert all(df["region"] == "TEST_REGION")

    def test_generate_synthetic_month(self):
        """Test synthetic month generation."""
        df = generate_synthetic_month("TEST_REGION")
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 744  # 31 days * 24 hours (January)
        assert "region" in df.columns
        assert all(df["region"] == "TEST_REGION")

    def test_generate_synthetic_year(self):
        """Test synthetic year generation."""
        df = generate_synthetic_year("TEST_REGION")
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 8760  # 365 days * 24 hours
        assert "region" in df.columns
        assert all(df["region"] == "TEST_REGION")


class TestDataQuality:
    """Test data quality and realism."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = SyntheticDataConfig(random_seed=42)
        self.generator = SyntheticEnergyDataGenerator(self.config)

    def test_data_correlations(self):
        """Test that data shows expected correlations."""
        df = self.generator.generate_full_dataset(hours=8760)  # One year

        # Temperature and demand should have some correlation (cooling/heating)
        temp_demand_corr = df["temperature_c"].corr(df["demand_mw"])
        assert abs(temp_demand_corr) > 0.1, "Temperature and demand should be correlated"

        # Solar and price should have negative correlation (merit order effect)
        solar_price_corr = df["solar_generation_mw"].corr(df["price_usd_per_mwh"])
        assert solar_price_corr < 0, "Solar and price should be negatively correlated"

    def test_seasonal_patterns(self):
        """Test seasonal patterns in the data."""
        df = self.generator.generate_full_dataset(hours=8760)  # One year

        # Add month column for analysis
        df["month"] = df["datetime"].dt.month

        # Solar should be higher in summer months
        summer_solar = df[df["month"].isin([6, 7, 8])]["solar_generation_mw"].mean()
        winter_solar = df[df["month"].isin([12, 1, 2])]["solar_generation_mw"].mean()
        
        assert summer_solar > winter_solar, "Solar should be higher in summer"

    def test_no_missing_values(self):
        """Test that generated data has no missing values."""
        df = self.generator.generate_full_dataset(hours=1000)
        
        assert not df.isnull().any().any(), "Generated data should have no missing values"

    def test_realistic_ranges(self):
        """Test that all values are in realistic ranges."""
        df = self.generator.generate_full_dataset(hours=1000)
        
        # Demand should be reasonable (in MW)
        assert df["demand_mw"].min() > 0
        assert df["demand_mw"].max() < 10000  # Reasonable for a region
        
        # Generation should be reasonable
        assert df["solar_generation_mw"].min() >= 0
        assert df["wind_generation_mw"].min() >= 0
        
        # Prices should be reasonable
        assert df["price_usd_per_mwh"].min() > 0
        assert df["price_usd_per_mwh"].max() < 1000  # Extreme but possible
        
        # Grid stability should be 0-1
        assert df["grid_stability"].min() >= 0
        assert df["grid_stability"].max() <= 1.0


if __name__ == "__main__":
    # Run a quick test
    print("Running synthetic data generator test...")
    df = generate_synthetic_week("TEST")
    print(f"Generated {len(df)} records")
    print(f"Columns: {list(df.columns)}")
    print(f"Date range: {df['datetime'].min()} to {df['datetime'].max()}")
    print("✅ Synthetic data generator working correctly!")
