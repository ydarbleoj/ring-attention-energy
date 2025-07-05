"""Tests for pipeline configuration."""

import pytest
from datetime import datetime
from src.core.pipeline.config import (
    EnergyPipelineConfig,
    MLDataSplitConfig,
    DataQualityConfig,
    RetryConfig,
    StorageConfig,
    ProcessingConfig,
    get_development_config,
    get_production_config,
    get_research_config
)


class TestMLDataSplitConfig:
    """Test ML data split configuration."""

    def test_default_config(self):
        """Test default split configuration."""
        config = MLDataSplitConfig()

        assert config.train_start == "2000-01-01"
        assert config.train_end == "2019-12-31"
        assert config.val_start == "2020-01-01"
        assert config.val_end == "2021-12-31"
        assert config.test_start == "2022-01-01"
        assert config.test_end == "2024-12-31"
        assert config.sequence_length == 8760
        assert config.sequence_overlap == 168

    def test_validation_success(self):
        """Test successful validation."""
        config = MLDataSplitConfig()
        # Should not raise any exception
        config.validate()

    def test_validation_small_split(self):
        """Test validation with too small split."""
        config = MLDataSplitConfig(
            train_start="2020-01-01",
            train_end="2020-06-01",  # Only 6 months
            min_split_size=17520     # 2 years
        )

        with pytest.raises(ValueError, match="Training split too small"):
            config.validate()

    def test_validation_overlap(self):
        """Test validation with excessive overlap."""
        config = MLDataSplitConfig(
            train_start="2020-01-01",
            train_end="2021-12-31",
            val_start="2021-01-01",  # 1 year overlap
            val_end="2023-12-31",    # Make val split large enough
            min_split_size=8760      # Reduce minimum size for test
        )

        with pytest.raises(ValueError, match="Training and validation splits overlap"):
            config.validate()

    def test_custom_config(self):
        """Test custom configuration."""
        config = MLDataSplitConfig(
            train_start="2010-01-01",
            sequence_length=4380,  # 6 months
            sequence_overlap=84    # 3.5 days
        )

        assert config.train_start == "2010-01-01"
        assert config.sequence_length == 4380
        assert config.sequence_overlap == 84


class TestDataQualityConfig:
    """Test data quality configuration."""

    def test_default_config(self):
        """Test default quality configuration."""
        config = DataQualityConfig()

        assert config.max_missing_pct == 0.1
        assert config.max_consecutive_missing == 24
        assert config.anomaly_std_threshold == 3.0
        assert config.min_demand_mw == 0.0
        assert config.max_demand_mw == 100000.0

    def test_validate_data_success(self):
        """Test successful data validation."""
        import pandas as pd
        import numpy as np

        config = DataQualityConfig()

        # Create valid data
        df = pd.DataFrame({
            'demand_mw': np.random.uniform(1000, 5000, 100),
            'generation_mw': np.random.uniform(500, 3000, 100),
            'price': np.random.uniform(20, 200, 100)
        })

        issues = config.validate_data(df)
        assert len(issues) == 0

    def test_validate_data_missing(self):
        """Test data validation with missing values."""
        import pandas as pd
        import numpy as np

        config = DataQualityConfig(max_missing_pct=0.05)  # 5% threshold

        # Create data with 40% missing values (2 out of 5 in pattern)
        df = pd.DataFrame({
            'demand_mw': [1000, 2000, np.nan, 3000, np.nan] * 20,
            'generation_mw': np.random.uniform(500, 3000, 100)
        })

        issues = config.validate_data(df)
        assert 'missing_demand_mw' in issues
        assert '40.0%' in issues['missing_demand_mw']

    def test_validate_data_out_of_range(self):
        """Test data validation with out-of-range values."""
        import pandas as pd
        import numpy as np

        config = DataQualityConfig()

        # Create data with out-of-range values
        df = pd.DataFrame({
            'demand_mw': [1000, 2000, 200000, 3000, -1000],  # Some out of range
            'generation_mw': np.random.uniform(500, 3000, 5)
        })

        issues = config.validate_data(df)
        assert 'range_demand_mw' in issues
        assert '2 values out of range' in issues['range_demand_mw']


class TestRetryConfig:
    """Test retry configuration."""

    def test_default_config(self):
        """Test default retry configuration."""
        config = RetryConfig()

        assert config.max_retries == 3
        assert config.initial_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter is True

    def test_custom_config(self):
        """Test custom retry configuration."""
        config = RetryConfig(
            max_retries=5,
            initial_delay=0.5,
            jitter=False
        )

        assert config.max_retries == 5
        assert config.initial_delay == 0.5
        assert config.jitter is False


class TestStorageConfig:
    """Test storage configuration."""

    def test_default_config(self):
        """Test default storage configuration."""
        config = StorageConfig()

        assert config.storage_root == "data/processed"
        assert config.cache_root == "data/cache"
        assert config.interim_root == "data/interim"
        assert config.final_format == "parquet"
        assert config.compression == "zstd"

    def test_get_file_path(self):
        """Test file path generation."""
        config = StorageConfig()

        # Test final stage
        path = config.get_file_path("demand", "PACW", 2023)
        assert str(path) == "data/processed/demand_PACW_2023.parquet"

        # Test raw stage
        path = config.get_file_path("demand", "PACW", 2023, "raw")
        assert str(path) == "data/cache/demand_PACW_2023.json"

        # Test interim stage
        path = config.get_file_path("demand", "PACW", 2023, "interim")
        assert str(path) == "data/interim/demand_PACW_2023.parquet"

    def test_custom_naming(self):
        """Test custom naming convention."""
        config = StorageConfig(
            naming_convention="{region}_{dataset}_{year}.{format}"
        )

        path = config.get_file_path("demand", "PACW", 2023)
        assert str(path) == "data/processed/PACW_demand_2023.parquet"


class TestProcessingConfig:
    """Test processing configuration."""

    def test_default_config(self):
        """Test default processing configuration."""
        config = ProcessingConfig()

        assert config.target_frequency == "1h"
        assert config.resampling_method == "mean"
        assert config.max_workers == 4
        assert config.batch_size_hours == 8760
        assert config.create_features is True
        assert config.normalize_features is True

    def test_custom_config(self):
        """Test custom processing configuration."""
        config = ProcessingConfig(
            target_frequency="30min",
            max_workers=8,
            create_features=False
        )

        assert config.target_frequency == "30min"
        assert config.max_workers == 8
        assert config.create_features is False


class TestEnergyPipelineConfig:
    """Test main pipeline configuration."""

    def test_default_config(self):
        """Test default pipeline configuration."""
        config = EnergyPipelineConfig()

        assert "eia" in config.sources
        assert "caiso" in config.sources
        assert "synthetic" in config.sources
        assert "PACW" in config.regions
        assert isinstance(config.splits, MLDataSplitConfig)
        assert isinstance(config.quality, DataQualityConfig)
        assert isinstance(config.retry, RetryConfig)
        assert isinstance(config.storage, StorageConfig)
        assert isinstance(config.processing, ProcessingConfig)

    def test_validation_success(self):
        """Test successful validation."""
        config = EnergyPipelineConfig()
        config.sources = ["synthetic"]  # Don't need API keys for synthetic
        config.validate()

    def test_validation_invalid_source(self):
        """Test validation with invalid source."""
        config = EnergyPipelineConfig()
        config.sources = ["invalid_source"]

        with pytest.raises(ValueError, match="Invalid source"):
            config.validate()

    def test_validation_missing_api_key(self):
        """Test validation with missing API key."""
        config = EnergyPipelineConfig()
        config.sources = ["eia"]
        config.api_keys = {}  # No EIA key

        with pytest.raises(ValueError, match="EIA API key required"):
            config.validate()

    def test_validation_no_regions(self):
        """Test validation with no regions."""
        config = EnergyPipelineConfig()
        config.sources = ["synthetic"]  # Use synthetic to avoid API key requirement
        config.regions = []

        with pytest.raises(ValueError, match="At least one region must be specified"):
            config.validate()

    def test_to_dict(self):
        """Test configuration serialization."""
        config = EnergyPipelineConfig()
        config.sources = ["synthetic"]
        config.regions = ["TEST"]
        config.api_keys = {"test": "key"}

        config_dict = config.to_dict()

        assert config_dict["sources"] == ["synthetic"]
        assert config_dict["regions"] == ["TEST"]
        assert config_dict["api_keys"] == {"test": "key"}
        assert "splits" in config_dict
        assert "storage" in config_dict


class TestConvenienceConfigs:
    """Test convenience configuration functions."""

    def test_development_config(self):
        """Test development configuration."""
        config = get_development_config()

        assert config.sources == ["synthetic"]
        assert config.regions == ["SYNTHETIC"]
        assert config.splits.train_start == "2020-01-01"
        assert config.splits.train_end == "2020-12-31"

    def test_production_config(self):
        """Test production configuration."""
        config = get_production_config()

        assert "eia" in config.sources
        assert "caiso" in config.sources
        assert "synthetic" not in config.sources
        assert "PACW" in config.regions
        assert config.splits.train_start == "2000-01-01"
        assert config.splits.train_end == "2019-12-31"

    def test_research_config(self):
        """Test research configuration."""
        config = get_research_config()

        assert "eia" in config.sources
        assert "synthetic" in config.sources
        assert "PACW" in config.regions
        assert "SYNTHETIC" in config.regions
        assert config.splits.train_start == "2010-01-01"
        assert config.splits.train_end == "2019-12-31"


class TestConfigIntegration:
    """Test configuration integration."""

    def test_full_pipeline_config(self):
        """Test full pipeline configuration setup."""
        config = EnergyPipelineConfig()
        config.sources = ["synthetic"]
        config.regions = ["TEST"]
        config.splits.sequence_length = 4380  # 6 months
        config.quality.max_missing_pct = 0.05
        config.retry.max_retries = 5
        config.storage.compression = "gzip"
        config.processing.max_workers = 8

        # Should validate successfully
        config.validate()

        # Test serialization
        config_dict = config.to_dict()
        assert config_dict["sources"] == ["synthetic"]
        assert config_dict["splits"]["sequence_length"] == 4380
        assert config_dict["storage"]["compression"] == "gzip"

    def test_realistic_production_setup(self):
        """Test realistic production setup."""
        config = EnergyPipelineConfig()
        config.sources = ["eia"]
        config.regions = ["PACW", "ERCO"]
        config.api_keys = {"eia": "test_key"}

        # Adjust for realistic timeline - ensure all splits are at least 2 years
        config.splits.train_start = "2010-01-01"
        config.splits.train_end = "2019-12-31"   # 10 years
        config.splits.val_start = "2020-01-01"
        config.splits.val_end = "2021-12-31"     # 2 years
        config.splits.test_start = "2022-01-01"
        config.splits.test_end = "2023-12-31"    # 2 years

        # Reduce minimum split size for this test
        config.splits.min_split_size = 8760      # 1 year minimum

        # Should validate successfully
        config.validate()

        # Check computed values
        assert len(config.sources) == 1
        assert len(config.regions) == 2
        assert config.splits.sequence_length == 8760


if __name__ == "__main__":
    # Run basic functionality test
    print("Testing pipeline configuration...")

    # Test development config
    dev_config = get_development_config()
    dev_config.validate()
    print("✅ Development config OK")

    # Test production config
    prod_config = get_production_config()
    prod_config.sources = ["synthetic"]  # Skip API key validation
    prod_config.validate()
    print("✅ Production config OK")

    # Test research config
    research_config = get_research_config()
    research_config.sources = ["synthetic"]  # Skip API key validation
    research_config.validate()
    print("✅ Research config OK")

    print("✅ All pipeline configurations working correctly!")
