"""Tests for MLX ML modules."""

import pytest
import mlx.core as mx
import polars as pl
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import shutil

from src.core.ml.features import EnergyFeatureEngineer, create_energy_sequence_dataset
from src.core.ml.sequence_generator import SequenceGenerator, RingAttentionSequenceGenerator
from src.core.ml.data_loaders import EnergyDataLoader, MLXDataLoader


class TestEnergyFeatureEngineer:
    """Test feature engineering for energy data."""

    @pytest.fixture
    def sample_energy_df(self):
        """Create sample energy DataFrame for testing."""
        # Create 7 days of hourly data
        start_date = datetime(2024, 1, 1)
        dates = [start_date + timedelta(hours=i) for i in range(168)]

        # Generate realistic energy patterns
        hours = np.arange(168)
        daily_pattern = np.sin(2 * np.pi * hours / 24)
        weekly_pattern = np.sin(2 * np.pi * hours / 168)

        # Base demand with patterns
        base_demand = 1000 + 200 * daily_pattern + 100 * weekly_pattern
        demand = base_demand + np.random.normal(0, 50, 168)

        # Solar generation (daytime only)
        solar = np.maximum(0, 200 * np.sin(np.pi * (hours % 24) / 24))

        # Wind generation (more random)
        wind = np.maximum(0, 150 + 100 * np.random.normal(0, 1, 168))

        # Hydro (more stable)
        hydro = np.full(168, 300) + np.random.normal(0, 20, 168)

        # Natural gas (dispatchable)
        natural_gas = np.maximum(0, demand - solar - wind - hydro)

        df = pl.DataFrame({
            "datetime": dates,
            "demand_mwh": demand,
            "solar_mwh": solar,
            "wind_mwh": wind,
            "hydro_mwh": hydro,
            "natural_gas_mwh": natural_gas
        })

        return df

    @pytest.fixture
    def feature_engineer(self):
        """Create feature engineer instance."""
        return EnergyFeatureEngineer()

    def test_temporal_features(self, feature_engineer, sample_energy_df):
        """Test temporal feature creation."""
        df_with_features = feature_engineer.create_temporal_features(sample_energy_df)

        # Check that temporal features are added
        expected_features = [
            "year", "month", "day", "hour", "weekday", "day_of_year",
            "hour_sin", "hour_cos", "weekday_sin", "weekday_cos",
            "month_sin", "month_cos", "year_sin", "year_cos",
            "is_weekend", "is_business_hours", "is_peak_hours", "is_summer"
        ]

        for feature in expected_features:
            assert feature in df_with_features.columns

        # Check cyclical features are bounded
        assert df_with_features["hour_sin"].min() >= -1.0
        assert df_with_features["hour_sin"].max() <= 1.0
        assert df_with_features["hour_cos"].min() >= -1.0
        assert df_with_features["hour_cos"].max() <= 1.0

        # Check boolean features
        assert df_with_features["is_weekend"].dtype == pl.Boolean
        assert df_with_features["is_business_hours"].dtype == pl.Boolean

    def test_lagged_features(self, feature_engineer, sample_energy_df):
        """Test lagged feature creation."""
        columns = ["demand_mwh", "solar_mwh"]
        lags = [1, 2, 24]

        df_with_lags = feature_engineer.create_lagged_features(
            sample_energy_df, columns, lags
        )

        # Check that lagged features are added
        for col in columns:
            for lag in lags:
                lag_col = f"{col}_lag_{lag}h"
                assert lag_col in df_with_lags.columns

        # Check that lagged values are correct
        demand_lag_1 = df_with_lags["demand_mwh_lag_1h"]
        original_demand = df_with_lags["demand_mwh"]

        # First value should be null (no previous value)
        assert demand_lag_1[0] is None
        # Second value should equal first original value
        assert abs(demand_lag_1[1] - original_demand[0]) < 1e-6

    def test_renewable_features(self, feature_engineer, sample_energy_df):
        """Test renewable energy feature creation."""
        df_with_renewable = feature_engineer.create_renewable_features(sample_energy_df)

        # Check that renewable features are added
        assert "total_renewable_mwh" in df_with_renewable.columns

        # Check that total renewable is sum of individual renewables
        total_renewable = df_with_renewable["total_renewable_mwh"]
        expected_total = (
            df_with_renewable["solar_mwh"] +
            df_with_renewable["wind_mwh"] +
            df_with_renewable["hydro_mwh"]
        )

        # All values should be close
        assert (total_renewable - expected_total).abs().max() < 1e-6

    def test_to_mlx_features(self, feature_engineer, sample_energy_df):
        """Test conversion to MLX format."""
        # Add some features first
        df_with_features = feature_engineer.create_temporal_features(sample_energy_df)

        # Convert to MLX
        mlx_array, feature_names = feature_engineer.to_mlx_features(
            df_with_features, normalize=True
        )

        # Check MLX array properties
        assert isinstance(mlx_array, mx.array)
        assert mlx_array.shape[0] == len(df_with_features)
        assert mlx_array.shape[1] == len(feature_names)

        # Check feature names are reasonable
        assert len(feature_names) > 0
        assert all(isinstance(name, str) for name in feature_names)


class TestSequenceGenerator:
    """Test sequence generation for ring attention."""

    @pytest.fixture
    def sample_polars_df(self):
        """Create sample Polars DataFrame."""
        dates = [datetime(2024, 1, 1) + timedelta(hours=i) for i in range(500)]

        df = pl.DataFrame({
            "datetime": dates,
            "demand_mwh": 1000 + 200 * np.sin(2 * np.pi * np.arange(500) / 24),
            "solar_mwh": np.maximum(0, 200 * np.sin(2 * np.pi * np.arange(500) / 24)),
            "wind_mwh": 150 + 100 * np.random.normal(0, 1, 500)
        })

        return df

    @pytest.fixture
    def sequence_generator(self):
        """Create sequence generator."""
        return SequenceGenerator(
            sequence_length=168,  # 1 week
            stride=24,           # 1 day
            features=["demand_mwh", "solar_mwh", "wind_mwh"]
        )

    def test_sequence_generator_init(self, sequence_generator):
        """Test sequence generator initialization."""
        assert sequence_generator.sequence_length == 168
        assert sequence_generator.stride == 24
        assert sequence_generator.features == ["demand_mwh", "solar_mwh", "wind_mwh"]
        assert sequence_generator.normalize is True

    def test_fit_normalizer(self, sequence_generator, sample_polars_df):
        """Test normalizer fitting."""
        sequence_generator.fit_normalizer(sample_polars_df)

        # Check that normalization stats are computed
        assert len(sequence_generator.feature_stats) > 0

        for feature in sequence_generator.features:
            assert feature in sequence_generator.feature_stats
            assert "mean" in sequence_generator.feature_stats[feature]
            assert "std" in sequence_generator.feature_stats[feature]

    def test_create_sequences(self, sequence_generator, sample_polars_df):
        """Test sequence generation."""
        sequences = sequence_generator.create_sequences(sample_polars_df)

        # Should generate some sequences
        assert isinstance(sequences, mx.array)
        assert sequences.ndim == 3  # (batch, sequence, features)
        assert sequences.shape[1] == 168  # sequence_length
        assert sequences.shape[2] == 3    # number of features

    def test_create_training_dataset(self, sequence_generator, sample_polars_df):
        """Test training dataset creation."""
        # For now, just test sequence creation since that's what's available
        sequences = sequence_generator.create_sequences(sample_polars_df)

        # Check output shapes
        assert isinstance(sequences, mx.array)
        assert sequences.ndim == 3  # (batch, sequence, features)
        assert sequences.shape[1] == 168  # sequence_length
        assert sequences.shape[2] == 3    # number of features


class TestRingAttentionSequenceGenerator:
    """Test ring attention specific sequence generation."""

    @pytest.fixture
    def ring_sequence_generator(self):
        """Create ring attention sequence generator."""
        return RingAttentionSequenceGenerator(
            sequence_length=8760,  # 1 year
            ring_size=4,
            stride=168            # 1 week
        )

    def test_ring_attention_init(self, ring_sequence_generator):
        """Test ring attention generator initialization."""
        assert ring_sequence_generator.sequence_length == 8760
        assert ring_sequence_generator.ring_size == 4
        assert ring_sequence_generator.stride == 168

    def test_create_ring_sequences(self, ring_sequence_generator):
        """Test ring attention sequence creation."""
        # Create a sample dataset
        dates = [datetime(2024, 1, 1) + timedelta(hours=i) for i in range(8760)]
        df = pl.DataFrame({
            "datetime": dates,
            "demand_mwh": 1000 + 200 * np.sin(2 * np.pi * np.arange(8760) / 24),
            "solar_mwh": np.maximum(0, 200 * np.sin(2 * np.pi * np.arange(8760) / 24))
        })

        # Create sequences
        sequences = ring_sequence_generator.create_ring_sequences(df)

        # Should have ring_size sequences
        assert isinstance(sequences, list)
        assert len(sequences) == 4  # ring_size

        # Each sequence should have the right partition length
        partition_length = 8760 // 4
        for seq in sequences:
            assert isinstance(seq, mx.array)
            assert seq.shape[0] == partition_length


class TestCreateEnergySequenceDataset:
    """Test the high-level sequence dataset creation function."""

    @pytest.fixture
    def sample_df(self):
        """Create sample energy DataFrame."""
        dates = [datetime(2024, 1, 1) + timedelta(hours=i) for i in range(500)]

        df = pl.DataFrame({
            "datetime": dates,
            "demand_mwh": 1000 + 200 * np.sin(2 * np.pi * np.arange(500) / 24),
            "solar_mwh": np.maximum(0, 200 * np.sin(2 * np.pi * np.arange(500) / 24)),
            "wind_mwh": 150 + 100 * np.random.normal(0, 1, 500)
        })

        return df

    def test_create_energy_sequence_dataset(self, sample_df):
        """Test the complete sequence dataset creation."""
        feature_engineer = EnergyFeatureEngineer()

        input_seqs, target_seqs, feature_names = create_energy_sequence_dataset(
            sample_df,
            sequence_length=168,
            stride=24,
            feature_engineer=feature_engineer
        )

        # Check output types and shapes
        assert isinstance(input_seqs, mx.array)
        assert isinstance(target_seqs, mx.array)
        assert isinstance(feature_names, list)

        # Check shapes are consistent
        assert input_seqs.ndim == 3  # (batch, sequence, features)
        assert target_seqs.ndim == 2  # (batch, features)
        assert input_seqs.shape[0] == target_seqs.shape[0]  # Same batch size
        assert input_seqs.shape[2] == target_seqs.shape[1]  # Same feature count

        # Should have generated multiple sequences
        assert input_seqs.shape[0] > 1

        # Should have many features due to feature engineering
        assert len(feature_names) > 10

    def test_create_sequence_dataset_without_feature_engineer(self, sample_df):
        """Test sequence creation without explicit feature engineer."""
        input_seqs, target_seqs, feature_names = create_energy_sequence_dataset(
            sample_df,
            sequence_length=100,
            stride=50
        )

        # Should still work with default feature engineer
        assert isinstance(input_seqs, mx.array)
        assert isinstance(target_seqs, mx.array)
        assert len(feature_names) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
