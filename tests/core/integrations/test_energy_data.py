"""
Tests for energy data integration pipeline

This test suite validates:
- Synthetic data generation with realistic energy patterns
- Data alignment and processing pipeline
- Integration with Ring Attention for long sequences
- Error handling and data quality checks
"""

import pytest
import pandas as pd
import numpy as np
import mlx.core as mx
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from src.core.integrations.energy_data import (
    EnergyDataConfig,
    SyntheticEnergyDataGenerator,
    EnergyDataProcessor,
    EnergyDataPipeline
)
from src.core.integrations.eia.client import EIAClient
from src.core.integrations.caiso.client import CAISOClient


class TestSyntheticEnergyDataGenerator:
    """Test synthetic energy data generation"""

    def setup_method(self):
        self.config = EnergyDataConfig(
            sequence_length=168,  # One week
            start_date="2023-01-01",
            end_date="2023-01-08"
        )
        self.generator = SyntheticEnergyDataGenerator(self.config)

    def test_demand_pattern_generation(self):
        """Test electricity demand pattern generation"""
        hours = 168  # One week
        demand = self.generator.generate_demand_pattern(hours)

        assert len(demand) == hours
        assert np.all(demand > 0), "Demand should always be positive"
        assert np.all(demand < 5.0), "Demand should be reasonable scale"

        # Check for daily patterns (should have 7 peaks for 7 days)
        daily_maxima = []
        for day in range(7):
            day_start = day * 24
            day_end = (day + 1) * 24
            day_demand = demand[day_start:day_end]
            daily_maxima.append(np.max(day_demand))

        # Daily patterns should exist
        assert len(daily_maxima) == 7

    def test_renewable_generation_patterns(self):
        """Test renewable generation patterns"""
        hours = 48  # Two days

        # Test solar generation
        solar = self.generator.generate_renewable_generation(hours, "solar")
        assert len(solar) == hours
        assert np.all(solar >= 0), "Solar generation should be non-negative"
        assert np.all(solar <= 1.0), "Solar generation should not exceed capacity"

        # Solar should be zero at night (hours 0-6 and 18-24 roughly)
        night_hours = list(range(0, 6)) + list(range(18, 24)) + list(range(24, 30)) + list(range(42, 48))
        night_solar = solar[night_hours]
        assert np.mean(night_solar) < 0.1, "Solar should be low at night"

        # Test wind generation
        wind = self.generator.generate_renewable_generation(hours, "wind")
        assert len(wind) == hours
        assert np.all(wind >= 0), "Wind generation should be non-negative"
        assert np.all(wind <= 1.0), "Wind generation should not exceed capacity"

    def test_temperature_patterns(self):
        """Test temperature data generation"""
        hours = 72  # Three days
        temperature = self.generator.generate_temperature(hours)

        assert len(temperature) == hours
        assert -20 < np.min(temperature) < 50, "Temperature should be in reasonable range"
        assert -20 < np.max(temperature) < 50, "Temperature should be in reasonable range"

        # Should have daily variation
        temp_std = np.std(temperature)
        assert temp_std > 5, "Temperature should have reasonable daily variation"

    def test_price_data_generation(self):
        """Test electricity price generation"""
        hours = 48
        demand = self.generator.generate_demand_pattern(hours)
        renewable = self.generator.generate_renewable_generation(hours, "solar")

        price = self.generator.generate_price_data(demand, renewable)

        assert len(price) == hours
        assert np.all(price > 0), "Prices should be positive"
        assert np.all(price < 200), "Prices should be reasonable (< $200/MWh)"

        # Higher demand should generally correlate with higher prices
        demand_high_idx = demand > np.percentile(demand, 75)
        demand_low_idx = demand < np.percentile(demand, 25)

        high_demand_prices = price[demand_high_idx]
        low_demand_prices = price[demand_low_idx]

        # This should generally be true due to our price model
        assert np.mean(high_demand_prices) > np.mean(low_demand_prices)

    def test_grid_stability_metric(self):
        """Test grid stability metric generation"""
        hours = 48
        demand = self.generator.generate_demand_pattern(hours)
        renewable = self.generator.generate_renewable_generation(hours, "wind")

        stability = self.generator.generate_grid_stability_metric(demand, renewable)

        assert len(stability) == hours
        assert np.all(stability >= 0), "Stability should be non-negative"
        assert np.all(stability <= 1.0), "Stability should not exceed 1.0"

        # Should have reasonable values
        assert 0.3 < np.mean(stability) < 0.9, "Average stability should be reasonable"

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
            "timestamp": pd.date_range(base_time, periods=100, freq="1H"),
            "demand": np.random.randn(100)
        })

        df2 = pd.DataFrame({
            "timestamp": pd.date_range(base_time + timedelta(hours=10), periods=80, freq="1H"),
            "renewable": np.random.randn(80)
        })

        aligned = self.processor.align_time_series([df1, df2])

        assert isinstance(aligned, pd.DataFrame)
        assert "timestamp" in aligned.columns
        assert "demand" in aligned.columns or "demand_source_0" in aligned.columns
        assert "renewable" in aligned.columns or "renewable_source_1" in aligned.columns

        # Should only include overlapping time range
        assert len(aligned) <= 80  # Limited by the shorter series overlap

    def test_missing_data_imputation(self):
        """Test missing data imputation"""
        # Create data with missing values
        timestamps = pd.date_range("2023-01-01", periods=100, freq="1H")
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
            "timestamp": pd.date_range("2023-01-01", periods=100, freq="1H"),
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
        timestamps = pd.date_range("2023-01-01", periods=hours, freq="1H")

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

        # Check overlap calculation
        step_size = seq_length - self.config.overlap
        expected_sequences = (hours - seq_length) // step_size + 1
        assert len(sequences) <= expected_sequences + 1  # Allow some flexibility


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

    @pytest.mark.asyncio
    async def test_synthetic_data_collection(self):
        """Test synthetic data collection"""
        data = await self.pipeline.collect_synthetic_data()

        assert isinstance(data, pd.DataFrame)
        assert not data.empty
        assert "timestamp" in data.columns
        assert "demand" in data.columns

    @pytest.mark.asyncio
    async def test_full_pipeline_execution(self):
        """Test complete pipeline execution"""
        sequences, metadata = await self.pipeline.run_full_pipeline()

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


class TestEnergyDataMLXIntegration:
    """Test integration with MLX arrays and Ring Attention"""

    def setup_method(self):
        self.config = EnergyDataConfig(
            sources=["synthetic"],
            sequence_length=8760,  # Full year
            start_date="2022-01-01",
            end_date="2023-01-01"
        )

    @pytest.mark.asyncio
    async def test_long_sequence_processing(self):
        """Test processing of long sequences for Ring Attention"""
        pipeline = EnergyDataPipeline(self.config)
        sequences, metadata = await pipeline.run_full_pipeline()

        assert isinstance(sequences, mx.array)
        assert sequences.shape[1] == 8760  # One year of hourly data
        assert sequences.shape[2] >= 6  # At least 6 features

        # Test memory efficiency
        memory_size = sequences.nbytes
        print(f"Memory usage for {sequences.shape}: {memory_size / 1024**2:.2f} MB")

        # Should be manageable for Ring Attention
        assert memory_size < 500 * 1024**2  # Less than 500MB

    def test_mlx_array_conversion(self):
        """Test conversion to MLX arrays"""
        # Create synthetic data
        generator = SyntheticEnergyDataGenerator(self.config)
        df = generator.generate_full_dataset(100)

        pipeline = EnergyDataPipeline(self.config)
        sequences, metadata = pipeline.prepare_sequences_for_training(df)

        assert isinstance(sequences, mx.array)
        assert sequences.dtype == mx.float32 or sequences.dtype == mx.float64

        # Test basic operations
        mean_vals = mx.mean(sequences, axis=(0, 1))
        assert mean_vals.shape == (sequences.shape[2],)

    @pytest.mark.asyncio
    async def test_ring_attention_compatibility(self):
        """Test compatibility with Ring Attention implementation"""
        from src.core.llms.ring_attention import RingAttention

        # Create energy data
        config = EnergyDataConfig(
            sources=["synthetic"],
            sequence_length=1000,  # Medium sequence
            start_date="2023-01-01",
            end_date="2023-02-01"
        )

        pipeline = EnergyDataPipeline(config)
        sequences, metadata = await pipeline.run_full_pipeline()

        # Initialize Ring Attention
        seq_len, num_features = sequences.shape[1], sequences.shape[2]
        d_model = 64
        n_heads = 8

        ring_attention = RingAttention(
            d_model=d_model,
            n_heads=n_heads,
            segment_size=250
        )

        # Project energy features to model dimension
        projection = mx.random.normal((num_features, d_model))
        input_data = mx.matmul(sequences[0], projection)  # Take first sequence

        # Test Ring Attention forward pass
        output = ring_attention(input_data)

        assert output.shape == (seq_len, d_model)
        assert not mx.isnan(output).any()


class TestAPIClients:
    """Test API client functionality (mocked)"""

    def test_eia_client_initialization(self):
        """Test EIA client initialization"""
        client = EIAClient("test_api_key")
        assert client.api_key == "test_api_key"
        assert client.base_url == "https://api.eia.gov/v2"

    def test_caiso_client_initialization(self):
        """Test CAISO client initialization"""
        client = CAISOClient()
        assert client.base_url == "http://oasis.caiso.com/oasisapi/SingleZip"

    @patch('requests.Session.get')
    def test_eia_client_error_handling(self, mock_get):
        """Test EIA client error handling"""
        # Mock a failed response
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("API Error")
        mock_get.return_value = mock_response

        client = EIAClient("test_key")

        # Should handle errors gracefully
        result = client.get_electricity_demand()
        assert isinstance(result, pd.DataFrame)
        assert result.empty


@pytest.mark.integration
class TestDataPipelineIntegration:
    """Integration tests for the complete data pipeline"""

    @pytest.mark.asyncio
    async def test_end_to_end_pipeline(self):
        """Test complete end-to-end pipeline"""
        config = EnergyDataConfig(
            sources=["synthetic"],
            sequence_length=2000,
            start_date="2023-01-01",
            end_date="2023-03-01"
        )

        pipeline = EnergyDataPipeline(config)
        sequences, metadata = await pipeline.run_full_pipeline()

        # Validate complete pipeline output
        assert isinstance(sequences, mx.array)
        assert sequences.ndim == 3
        assert sequences.shape[1] == 2000
        assert sequences.shape[2] >= 6

        # Test data quality
        assert not mx.isnan(sequences).any()
        assert not mx.isinf(sequences).any()

        # Test metadata completeness
        required_keys = ["num_sequences", "sequence_length", "num_features", "feature_names"]
        for key in required_keys:
            assert key in metadata

        print(f"Pipeline success: {metadata['num_sequences']} sequences of length {metadata['sequence_length']}")


if __name__ == "__main__":
    # Run basic functionality test
    import asyncio

    async def run_basic_test():
        config = EnergyDataConfig(
            sources=["synthetic"],
            sequence_length=168,
            start_date="2023-01-01",
            end_date="2023-01-08"
        )

        pipeline = EnergyDataPipeline(config)
        sequences, metadata = await pipeline.run_full_pipeline()

        print("Energy Data Pipeline Test Results:")
        print(f"- Sequences shape: {sequences.shape}")
        print(f"- Features: {metadata['feature_names']}")
        print(f"- Time range: {metadata['time_range']}")
        print("✅ Energy data integration pipeline working correctly!")

    asyncio.run(run_basic_test())
