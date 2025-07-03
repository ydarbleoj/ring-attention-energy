"""
Simple tests for energy data integration pipeline

This test suite focuses on the core functionality without complex async operations
that might cause hanging.
"""

import pytest
import pandas as pd
import numpy as np
import mlx.core as mx
import asyncio
from datetime import datetime, timedelta

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from core.integrations.energy_data import (
    EnergyDataConfig,
    SyntheticEnergyDataGenerator,
    EnergyDataProcessor,
    EnergyDataPipeline
)


class TestSyntheticDataGeneration:
    """Test synthetic energy data generation"""

    def setup_method(self):
        self.config = EnergyDataConfig(
            sequence_length=168,  # One week
            start_date="2023-01-01",
            end_date="2023-01-08"
        )
        self.generator = SyntheticEnergyDataGenerator(self.config)

    def test_demand_pattern_basic(self):
        """Test basic demand pattern generation"""
        hours = 24  # One day
        demand = self.generator.generate_demand_pattern(hours)

        assert len(demand) == hours
        assert np.all(demand > 0), "Demand should always be positive"
        assert np.all(demand < 5.0), "Demand should be reasonable scale"

    def test_solar_generation(self):
        """Test solar generation patterns"""
        hours = 48  # Two days
        solar = self.generator.generate_renewable_generation(hours, "solar")

        assert len(solar) == hours
        assert np.all(solar >= 0), "Solar generation should be non-negative"
        assert np.all(solar <= 1.0), "Solar generation should not exceed capacity"

    def test_wind_generation(self):
        """Test wind generation patterns"""
        hours = 48  # Two days
        wind = self.generator.generate_renewable_generation(hours, "wind")

        assert len(wind) == hours
        assert np.all(wind >= 0), "Wind generation should be non-negative"
        assert np.all(wind <= 1.0), "Wind generation should not exceed capacity"

    def test_temperature_generation(self):
        """Test temperature data generation"""
        hours = 72  # Three days
        temperature = self.generator.generate_temperature(hours)

        assert len(temperature) == hours
        assert -20 < np.min(temperature) < 50, "Temperature should be in reasonable range"
        assert -20 < np.max(temperature) < 50, "Temperature should be in reasonable range"

    def test_price_generation(self):
        """Test electricity price generation"""
        hours = 48
        demand = self.generator.generate_demand_pattern(hours)
        renewable = self.generator.generate_renewable_generation(hours, "solar")

        price = self.generator.generate_price_data(demand, renewable)

        assert len(price) == hours
        assert np.all(price > 0), "Prices should be positive"
        assert np.all(price < 200), "Prices should be reasonable (< $200/MWh)"

    def test_grid_stability_metric(self):
        """Test grid stability metric generation"""
        hours = 48
        demand = self.generator.generate_demand_pattern(hours)
        renewable = self.generator.generate_renewable_generation(hours, "wind")

        stability = self.generator.generate_grid_stability_metric(demand, renewable)

        assert len(stability) == hours
        assert np.all(stability >= 0), "Stability should be non-negative"
        assert np.all(stability <= 1.0), "Stability should not exceed 1.0"

    def test_full_dataset_generation(self):
        """Test full synthetic dataset generation"""
        hours = 168  # One week
        dataset = self.generator.generate_full_dataset(hours)

        assert isinstance(dataset, pd.DataFrame)
        assert len(dataset) == hours

        # Check all required columns
        expected_columns = [
            "timestamp", "demand", "solar_generation", "wind_generation",
            "temperature", "price", "grid_stability"
        ]
        for col in expected_columns:
            assert col in dataset.columns, f"Missing column: {col}"

        # Check data types and ranges
        assert pd.api.types.is_datetime64_any_dtype(dataset["timestamp"])
        assert np.all(dataset["demand"] > 0)
        assert np.all(dataset["solar_generation"] >= 0)
        assert np.all(dataset["wind_generation"] >= 0)
        assert np.all(dataset["price"] > 0)
        assert np.all(dataset["grid_stability"] >= 0)
        assert np.all(dataset["grid_stability"] <= 1.0)


class TestEnergyDataProcessor:
    """Test energy data processing functionality"""

    def setup_method(self):
        self.config = EnergyDataConfig(
            sequence_length=168,  # One week
            overlap=24,  # One day overlap
            missing_data_threshold=0.1
        )
        self.processor = EnergyDataProcessor(self.config)

    def test_time_series_alignment(self):
        """Test alignment of multiple time series"""
        # Create two time series with different ranges
        base_time = pd.to_datetime("2023-01-01")

        df1 = pd.DataFrame({
            "timestamp": pd.date_range(base_time, periods=100, freq="1h"),
            "demand": np.random.randn(100)
        })

        df2 = pd.DataFrame({
            "timestamp": pd.date_range(base_time + timedelta(hours=10), periods=80, freq="1h"),
            "renewable": np.random.randn(80)
        })

        aligned = self.processor.align_time_series([df1, df2])

        assert isinstance(aligned, pd.DataFrame)
        assert "timestamp" in aligned.columns
        assert len(aligned) <= 80  # Limited by the shorter series overlap

    def test_missing_data_imputation(self):
        """Test missing data imputation"""
        # Create data with missing values
        timestamps = pd.date_range("2023-01-01", periods=100, freq="1h")
        data = np.random.randn(100)

        # Introduce missing values
        missing_indices = np.random.choice(100, 10, replace=False)
        data[missing_indices] = np.nan

        df = pd.DataFrame({
            "timestamp": timestamps,
            "demand": data,
            "temperature": np.random.randn(100)
        })

        # Introduce more missing values in temperature
        temp_missing = np.random.choice(100, 5, replace=False)
        df.loc[temp_missing, "temperature"] = np.nan

        imputed = self.processor.impute_missing_data(df)

        assert imputed.isnull().sum().sum() == 0, "All missing values should be imputed"
        assert len(imputed) == len(df), "Length should be preserved"

    def test_anomaly_detection(self):
        """Test anomaly detection functionality"""
        # Create data with obvious anomalies
        normal_data = np.random.normal(10, 2, 100)
        normal_data[50] = 100  # Clear anomaly
        normal_data[75] = -50  # Another anomaly

        df = pd.DataFrame({
            "timestamp": pd.date_range("2023-01-01", periods=100, freq="1h"),
            "demand": normal_data,
            "temperature": np.random.normal(20, 5, 100)
        })

        result = self.processor.detect_anomalies(df)

        # Should have anomaly flags
        assert "demand_anomaly" in result.columns
        assert "temperature_anomaly" in result.columns

        # The obvious anomalies should be detected
        assert result.loc[50, "demand_anomaly"] == True
        assert result.loc[75, "demand_anomaly"] == True

    def test_sequence_creation(self):
        """Test creation of overlapping sequences"""
        # Create data longer than sequence length
        hours = 400
        timestamps = pd.date_range("2023-01-01", periods=hours, freq="1h")

        df = pd.DataFrame({
            "timestamp": timestamps,
            "demand": np.random.randn(hours),
            "temperature": np.random.randn(hours),
            "price": np.random.randn(hours)
        })

        sequences = self.processor.create_sequences(df)

        assert isinstance(sequences, list)
        assert len(sequences) > 0, "Should create at least one sequence"

        # Check sequence properties
        seq_length = self.config.sequence_length
        num_features = 3  # demand, temperature, price

        for seq in sequences:
            assert seq.shape == (seq_length, num_features)


class TestEnergyDataPipeline:
    """Test the complete energy data pipeline"""

    def setup_method(self):
        self.config = EnergyDataConfig(
            sources=["synthetic"],
            sequence_length=168,
            start_date="2023-01-01",
            end_date="2023-01-08"
        )
        self.pipeline = EnergyDataPipeline(self.config)

    def test_synthetic_data_collection(self):
        """Test synthetic data collection (sync version)"""
        # Run async function in sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            data = loop.run_until_complete(self.pipeline.collect_synthetic_data())

            assert isinstance(data, pd.DataFrame)
            assert not data.empty
            assert "timestamp" in data.columns
            assert "demand" in data.columns
        finally:
            loop.close()

    def test_data_processing(self):
        """Test data processing with multiple dataframes"""
        # Create multiple synthetic dataframes
        generator = SyntheticEnergyDataGenerator(self.config)

        df1 = generator.generate_full_dataset(100)
        df2 = generator.generate_full_dataset(100)

        # Slightly offset the second dataframe
        df2["timestamp"] = df2["timestamp"] + timedelta(hours=50)

        processed = self.pipeline.process_data([df1, df2])

        assert isinstance(processed, pd.DataFrame)
        assert not processed.empty
        assert "timestamp" in processed.columns

    def test_prepare_sequences_for_training(self):
        """Test sequence preparation for MLX training"""
        # Generate sample data
        generator = SyntheticEnergyDataGenerator(self.config)
        df = generator.generate_full_dataset(200)  # Longer than sequence length

        sequences, metadata = self.pipeline.prepare_sequences_for_training(df)

        assert isinstance(sequences, mx.array)
        assert isinstance(metadata, dict)

        # Check metadata
        assert "num_sequences" in metadata
        assert "sequence_length" in metadata
        assert "num_features" in metadata
        assert "feature_names" in metadata

        # Check sequences
        assert sequences.ndim == 3  # (num_sequences, seq_length, num_features)
        assert sequences.shape[1] == self.config.sequence_length
        assert sequences.shape[2] > 0  # Should have features


class TestMLXIntegration:
    """Test integration with MLX arrays"""

    def test_mlx_array_conversion(self):
        """Test conversion to MLX arrays"""
        config = EnergyDataConfig(
            sources=["synthetic"],
            sequence_length=100,
            start_date="2023-01-01",
            end_date="2023-01-05"
        )

        # Create synthetic data
        generator = SyntheticEnergyDataGenerator(config)
        df = generator.generate_full_dataset(150)

        pipeline = EnergyDataPipeline(config)
        sequences, metadata = pipeline.prepare_sequences_for_training(df)

        assert isinstance(sequences, mx.array)
        assert sequences.dtype == mx.float32 or sequences.dtype == mx.float64

        # Test basic operations
        mean_vals = mx.mean(sequences, axis=(0, 1))
        assert mean_vals.shape == (sequences.shape[2],)


def test_simple_integration():
    """Simple integration test without complex async operations"""
    config = EnergyDataConfig(
        sources=["synthetic"],
        sequence_length=100,
        start_date="2023-01-01",
        end_date="2023-01-02"
    )

    generator = SyntheticEnergyDataGenerator(config)
    df = generator.generate_full_dataset(50)

    pipeline = EnergyDataPipeline(config)
    sequences, metadata = pipeline.prepare_sequences_for_training(df)

    assert isinstance(sequences, mx.array)
    assert len(metadata['feature_names']) >= 6

    print(f"✅ Simple integration test passed: {sequences.shape}")


if __name__ == "__main__":
    # Run the simple test directly
    test_simple_integration()
    print("✅ All simple tests completed!")
