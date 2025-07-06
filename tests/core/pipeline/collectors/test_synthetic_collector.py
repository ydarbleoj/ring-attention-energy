"""Tests for synthetic data collector."""

import pytest
import polars as pl
import asyncio
from unittest.mock import Mock, patch
from datetime import datetime

from src.core.pipeline.collectors.synthetic_collector import SyntheticCollector
from src.core.pipeline.collectors.base import DataCollectionResult
from src.core.integrations.synthetic.generator import SyntheticEnergyDataGenerator


class TestSyntheticCollector:
    """Test synthetic collector functionality."""

    @pytest.fixture
    def collector_config(self):
        """Create collector configuration."""
        return {
            "generator": {
                "region": "TEST",
                "seed": 42,
                "seasonal_strength": 0.3,
                "noise_level": 0.1,
                "trend_strength": 0.05
            },
            "retry": {
                "max_retries": 3,
                "initial_delay": 1.0,
                "max_delay": 60.0,
                "exponential_base": 2.0,
                "jitter": True
            }
        }

    @pytest.fixture
    def synthetic_collector(self, collector_config):
        """Create synthetic collector instance."""
        return SyntheticCollector(collector_config)

    @pytest.fixture
    def sample_demand_data(self):
        """Create sample demand data."""
        return pl.DataFrame({
            "datetime": [
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 1, 1, 1, 0),
                datetime(2024, 1, 1, 2, 0)
            ],
            "region": ["TEST", "TEST", "TEST"],
            "demand_mwh": [1000.0, 1100.0, 1200.0]
        })

    @pytest.fixture
    def sample_generation_data(self):
        """Create sample generation data."""
        return pl.DataFrame({
            "datetime": [
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 1, 1, 1, 0),
                datetime(2024, 1, 1, 2, 0)
            ],
            "region": ["TEST", "TEST", "TEST"],
            "fuel_type": ["coal", "natural_gas", "solar"],
            "generation_mwh": [800.0, 900.0, 1000.0]
        })

    def test_collector_initialization(self, synthetic_collector):
        """Test collector initialization."""
        assert synthetic_collector.name == "Synthetic"
        assert synthetic_collector.generator is not None
        assert synthetic_collector.region == "TEST"
        assert synthetic_collector.generator.config.random_seed == 42
        assert synthetic_collector.retry_config is not None

    def test_collector_info(self, synthetic_collector):
        """Test collector info method."""
        info = synthetic_collector.get_collector_info()

        assert info["name"] == "Synthetic"
        assert info["generator_configured"] is True
        assert info["generator_region"] == "TEST"
        assert info["generator_seed"] == 42
        assert "retry_config" in info
        assert info["retry_config"]["max_retries"] == 3

    def test_collector_initialization_without_config(self):
        """Test collector initialization without configuration."""
        collector = SyntheticCollector()

        assert collector.name == "Synthetic"
        assert collector.generator is not None
        assert collector.region == "TEST"  # Default
        assert collector.generator.config.random_seed == 42  # Default

    @pytest.mark.asyncio
    async def test_collect_demand_data_success(self, synthetic_collector, sample_demand_data):
        """Test successful demand data collection."""
        # Mock the generator method to return a pandas DataFrame that will be converted
        mock_df = sample_demand_data.to_pandas()
        with patch.object(synthetic_collector.generator, 'generate_dataset_with_region', return_value=mock_df):
            result = await synthetic_collector.collect_demand_data(
                start_date="2024-01-01",
                end_date="2024-01-02",
                region="TEST"
            )

            assert isinstance(result, DataCollectionResult)
            assert result.success
            assert len(result.data) == 3
            assert result.metadata["source"] == "Synthetic"
            assert result.metadata["data_type"] == "demand"
            assert result.metadata["region"] == "TEST"
            assert result.metadata["records_collected"] == 3
            assert result.metadata["generator_seed"] == 42

    @pytest.mark.asyncio
    async def test_collect_demand_data_with_kwargs(self, synthetic_collector, sample_demand_data):
        """Test demand data collection with additional parameters."""
        # Mock the generator method to return a pandas DataFrame
        mock_df = sample_demand_data.to_pandas()
        with patch.object(synthetic_collector.generator, 'generate_dataset_with_region', return_value=mock_df) as mock_gen:
            result = await synthetic_collector.collect_demand_data(
                start_date="2024-01-01",
                end_date="2024-01-02",
                region="TEST",
                freq="30T"  # 30 minute frequency
            )

            # Verify the generator was called with correct parameters
            mock_gen.assert_called_once_with(
                region="TEST",
                hours=24  # 24 hours from 2024-01-01 to 2024-01-02
            )
            assert result.success
            assert result.metadata["frequency"] == "30T"

    @pytest.mark.asyncio
    async def test_collect_generation_data_success(self, synthetic_collector, sample_generation_data):
        """Test successful generation data collection."""
        # Mock the generator method
        mock_df = sample_generation_data.to_pandas()
        with patch.object(synthetic_collector.generator, 'generate_dataset_with_region', return_value=mock_df):
            result = await synthetic_collector.collect_generation_data(
                start_date="2024-01-01",
                end_date="2024-01-02",
                region="TEST"
            )

            assert isinstance(result, DataCollectionResult)
            assert result.success
            assert len(result.data) == 3
            assert result.metadata["source"] == "Synthetic"
            assert result.metadata["data_type"] == "generation"
            assert result.metadata["region"] == "TEST"

    @pytest.mark.asyncio
    async def test_collect_generation_data_with_fuel_types(self, synthetic_collector, sample_generation_data):
        """Test generation data collection with fuel types."""
        fuel_types = ["coal", "natural_gas", "solar"]

        # Mock the generator method
        mock_df = sample_generation_data.to_pandas()
        with patch.object(synthetic_collector.generator, 'generate_dataset_with_region', return_value=mock_df) as mock_gen:
            result = await synthetic_collector.collect_generation_data(
                start_date="2024-01-01",
                end_date="2024-01-02",
                region="TEST",
                fuel_types=fuel_types
            )

            # Verify the generator was called with correct parameters
            mock_gen.assert_called_once_with(
                region="TEST",
                hours=24  # 24 hours from 2024-01-01 to 2024-01-02
            )
            assert result.success

    @pytest.mark.asyncio
    async def test_collect_comprehensive_data_success(self, synthetic_collector, sample_demand_data, sample_generation_data):
        """Test comprehensive data collection."""
        # Create mock DataFrame with both demand and generation data
        mock_df = sample_demand_data.to_pandas()

        # Mock the generator method
        with patch.object(synthetic_collector.generator, 'generate_dataset_with_region', return_value=mock_df):
            results = await synthetic_collector.collect_comprehensive_data(
                start_date="2024-01-01",
                end_date="2024-01-02",
                region="TEST"
            )

            assert isinstance(results, dict)
            assert "demand" in results
            assert "generation" in results

            # Check demand result
            demand_result = results["demand"]
            assert isinstance(demand_result, DataCollectionResult)
            assert demand_result.success
            assert len(demand_result.data) == 3
            assert demand_result.metadata["data_type"] == "demand"

            # Check generation result
            generation_result = results["generation"]
            assert isinstance(generation_result, DataCollectionResult)
            assert generation_result.success
            assert len(generation_result.data) == 3
            assert generation_result.metadata["data_type"] == "generation"

    @pytest.mark.asyncio
    async def test_collect_demand_data_failure(self, synthetic_collector):
        """Test demand data collection failure."""
        # Mock the generator to raise an exception
        with patch.object(synthetic_collector.generator, 'generate_dataset_with_region', side_effect=Exception("Generator Error")):
            result = await synthetic_collector.collect_demand_data(
                start_date="2024-01-01",
                end_date="2024-01-02",
                region="TEST"
            )

            assert isinstance(result, DataCollectionResult)
            assert not result.success
            assert result.data.is_empty()
            assert len(result.errors) > 0
            assert "Generator Error" in result.errors[0]

    @pytest.mark.asyncio
    async def test_collect_generation_data_failure(self, synthetic_collector):
        """Test generation data collection failure."""
        # Mock the generator to raise an exception
        with patch.object(synthetic_collector.generator, 'generate_dataset_with_region', side_effect=Exception("Generation Error")):
            result = await synthetic_collector.collect_generation_data(
                start_date="2024-01-01",
                end_date="2024-01-02",
                region="TEST"
            )

            assert isinstance(result, DataCollectionResult)
            assert not result.success
            assert result.data.is_empty()
            assert len(result.errors) > 0
            assert "Generation Error" in result.errors[0]

    @pytest.mark.asyncio
    async def test_collect_comprehensive_data_failure(self, synthetic_collector):
        """Test comprehensive data collection failure."""
        # Mock the generator to raise an exception
        with patch.object(synthetic_collector.generator, 'generate_dataset_with_region', side_effect=Exception("Comprehensive Error")):
            results = await synthetic_collector.collect_comprehensive_data(
                start_date="2024-01-01",
                end_date="2024-01-02",
                region="TEST"
            )

            assert isinstance(results, dict)
            assert "demand" in results
            assert "generation" in results

            # Both should be error results
            demand_result = results["demand"]
            generation_result = results["generation"]

            assert not demand_result.success
            assert not generation_result.success
            assert len(demand_result.errors) > 0
            assert len(generation_result.errors) > 0

    @pytest.mark.asyncio
    async def test_date_validation(self, synthetic_collector):
        """Test date validation in collectors."""
        result = await synthetic_collector.collect_demand_data(
            start_date="2024-01-02",  # end before start
            end_date="2024-01-01",
            region="TEST"
        )

        assert not result.success
        assert len(result.errors) > 0

    @pytest.mark.asyncio
    async def test_region_override(self, synthetic_collector, sample_demand_data):
        """Test that region can be overridden."""
        # Mock the generator method
        mock_df = sample_demand_data.to_pandas()
        with patch.object(synthetic_collector.generator, 'generate_dataset_with_region', return_value=mock_df):
            result = await synthetic_collector.collect_demand_data(
                start_date="2024-01-01",
                end_date="2024-01-02",
                region="CUSTOM_REGION"
            )

            assert result.success
            assert result.metadata["region"] == "CUSTOM_REGION"

    @pytest.mark.asyncio
    async def test_sync_methods_direct_call(self, synthetic_collector, sample_demand_data):
        """Test direct calls to sync methods."""
        # Mock the generator method
        mock_df = sample_demand_data.to_pandas()
        with patch.object(synthetic_collector.generator, 'generate_dataset_with_region', return_value=mock_df):
            result = synthetic_collector._collect_demand_sync(
                start_date="2024-01-01",
                end_date="2024-01-02",
                region="TEST",
                freq="H"
            )

            assert isinstance(result, pl.DataFrame)
            assert len(result) == 3
