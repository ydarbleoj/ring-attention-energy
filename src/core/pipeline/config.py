"""Configuration for the energy data pipeline."""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime
from pathlib import Path


@dataclass
class MLDataSplitConfig:
    """Configuration for ML train/validation/test splits."""

    # Temporal splits (no random shuffling for time series!)
    train_start: str = "2000-01-01"
    train_end: str = "2019-12-31"    # 20 years
    val_start: str = "2020-01-01"
    val_end: str = "2021-12-31"      # 2 years
    test_start: str = "2022-01-01"
    test_end: str = "2024-12-31"     # 3 years

    # Ring Attention specific
    sequence_length: int = 8760      # 1 year of hourly data
    sequence_overlap: int = 168      # 1 week overlap
    min_split_size: int = 17520      # 2 years minimum per split

    def validate(self) -> None:
        """Validate the split configuration."""
        train_start = datetime.fromisoformat(self.train_start)
        train_end = datetime.fromisoformat(self.train_end)
        val_start = datetime.fromisoformat(self.val_start)
        val_end = datetime.fromisoformat(self.val_end)
        test_start = datetime.fromisoformat(self.test_start)
        test_end = datetime.fromisoformat(self.test_end)

        # Check split sizes
        train_hours = int((train_end - train_start).total_seconds() / 3600)
        val_hours = int((val_end - val_start).total_seconds() / 3600)
        test_hours = int((test_end - test_start).total_seconds() / 3600)

        if train_hours < self.min_split_size:
            raise ValueError(f"Training split too small: {train_hours} < {self.min_split_size}")
        if val_hours < self.min_split_size:
            raise ValueError(f"Validation split too small: {val_hours} < {self.min_split_size}")
        if test_hours < self.min_split_size:
            raise ValueError(f"Test split too small: {test_hours} < {self.min_split_size}")

        # Check for overlaps (allowing small overlap for sequence continuity)
        if val_start < train_end and (train_end - val_start).days > 30:
            raise ValueError("Training and validation splits overlap too much")
        if test_start < val_end and (val_end - test_start).days > 30:
            raise ValueError("Validation and test splits overlap too much")


@dataclass
class DataQualityConfig:
    """Configuration for data quality checks."""

    # Missing data thresholds
    max_missing_pct: float = 0.1           # 10% missing data max
    max_consecutive_missing: int = 24      # 24 hours max consecutive missing

    # Anomaly detection
    anomaly_std_threshold: float = 3.0     # Z-score threshold
    anomaly_window: int = 168              # 1 week window for anomaly detection

    # Data validation
    min_demand_mw: float = 0.0
    max_demand_mw: float = 100000.0        # 100 GW max
    min_generation_mw: float = 0.0
    max_generation_mw: float = 50000.0     # 50 GW max
    min_price: float = -1000.0             # Allow negative prices
    max_price: float = 10000.0             # $10,000/MWh max

    def validate_data(self, df) -> Dict[str, Any]:
        """Validate a dataframe against quality thresholds."""
        issues = {}

        # Check missing data
        missing_pct = df.isnull().sum() / len(df)
        for col, pct in missing_pct.items():
            if pct > self.max_missing_pct:
                issues[f"missing_{col}"] = f"{pct:.1%} missing data"

        # Check value ranges
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if 'demand' in col.lower():
                out_of_range = ((df[col] < self.min_demand_mw) | (df[col] > self.max_demand_mw)).sum()
                if out_of_range > 0:
                    issues[f"range_{col}"] = f"{out_of_range} values out of range"

        return issues


@dataclass
class RetryConfig:
    """Configuration for retry logic."""

    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True

    # Specific retry settings for different operations
    api_retries: int = 3
    storage_retries: int = 2
    processing_retries: int = 1


@dataclass
class StorageConfig:
    """Configuration for data storage."""

    # Base paths
    storage_root: str = "data/processed"
    cache_root: str = "data/cache"
    interim_root: str = "data/interim"

    # Storage formats
    raw_format: str = "json"          # For API responses
    interim_format: str = "parquet"   # For intermediate processing
    final_format: str = "parquet"     # For ML-ready data

    # Compression
    compression: str = "zstd"         # Good balance of speed/compression

    # Partitioning
    partition_by: str = "year"        # Partition by year for 24 years of data

    # File naming
    naming_convention: str = "{dataset}_{region}_{year}.{format}"

    def get_file_path(self, dataset: str, region: str, year: int, stage: str = "final") -> Path:
        """Get file path for a dataset."""
        if stage == "raw":
            root = self.cache_root
            format_ext = self.raw_format
        elif stage == "interim":
            root = self.interim_root
            format_ext = self.interim_format
        else:
            root = self.storage_root
            format_ext = self.final_format

        filename = self.naming_convention.format(
            dataset=dataset,
            region=region,
            year=year,
            format=format_ext
        )

        return Path(root) / filename


@dataclass
class ProcessingConfig:
    """Configuration for data processing."""

    # Time series processing
    target_frequency: str = "1h"       # Hourly resolution
    resampling_method: str = "mean"    # How to resample

    # Parallel processing
    max_workers: int = 4               # For parallel processing
    batch_size_hours: int = 8760       # Process 1 year at a time

    # Feature engineering
    create_features: bool = True       # Whether to create engineered features
    normalize_features: bool = True    # Whether to normalize features

    # Memory management
    chunk_size: int = 100000          # Rows per chunk for large datasets
    lazy_loading: bool = True         # Use lazy loading for large datasets


@dataclass
class EnergyPipelineConfig:
    """Main configuration for the energy data pipeline."""

    # Data sources
    sources: List[str] = field(default_factory=lambda: ["eia", "caiso", "synthetic"])
    regions: List[str] = field(default_factory=lambda: ["PACW", "ERCO", "COAS"])

    # Sub-configurations
    splits: MLDataSplitConfig = field(default_factory=MLDataSplitConfig)
    quality: DataQualityConfig = field(default_factory=DataQualityConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)

    # API configuration
    api_keys: Dict[str, str] = field(default_factory=dict)
    api_timeout: int = 30

    # Logging
    log_level: str = "INFO"
    log_to_file: bool = True
    log_file: str = "pipeline.log"

    def validate(self) -> None:
        """Validate the entire pipeline configuration."""
        self.splits.validate()

        # Validate sources
        valid_sources = {"eia", "caiso", "synthetic"}
        for source in self.sources:
            if source not in valid_sources:
                raise ValueError(f"Invalid source: {source}")

        # Check API keys for non-synthetic sources
        for source in self.sources:
            if source == "eia" and "eia" not in self.api_keys:
                raise ValueError("EIA API key required for EIA data source")

        # Validate regions
        if not self.regions:
            raise ValueError("At least one region must be specified")

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'EnergyPipelineConfig':
        """Create configuration from dictionary."""
        # This would handle loading from YAML/JSON config files
        # For now, just return default
        return cls()

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        # This would handle saving to YAML/JSON config files
        return {
            "sources": self.sources,
            "regions": self.regions,
            "splits": {
                "train_start": self.splits.train_start,
                "train_end": self.splits.train_end,
                "val_start": self.splits.val_start,
                "val_end": self.splits.val_end,
                "test_start": self.splits.test_start,
                "test_end": self.splits.test_end,
                "sequence_length": self.splits.sequence_length,
                "sequence_overlap": self.splits.sequence_overlap,
            },
            "storage": {
                "storage_root": self.storage.storage_root,
                "cache_root": self.storage.cache_root,
                "compression": self.storage.compression,
            },
            "api_keys": self.api_keys  # Be careful with this in production
        }


# Convenience functions for common configurations
def get_development_config() -> EnergyPipelineConfig:
    """Get configuration for development (small dataset)."""
    config = EnergyPipelineConfig()
    config.sources = ["synthetic"]
    config.regions = ["SYNTHETIC"]
    config.splits.train_start = "2020-01-01"
    config.splits.train_end = "2020-12-31"
    config.splits.val_start = "2021-01-01"
    config.splits.val_end = "2021-12-31"
    config.splits.test_start = "2022-01-01"
    config.splits.test_end = "2022-12-31"
    return config


def get_production_config() -> EnergyPipelineConfig:
    """Get configuration for production (full dataset)."""
    config = EnergyPipelineConfig()
    config.sources = ["eia", "caiso"]
    config.regions = ["PACW", "ERCO", "COAS"]
    # Uses default 2000-2024 date range
    return config


def get_research_config() -> EnergyPipelineConfig:
    """Get configuration for research (mixed synthetic and real data)."""
    config = EnergyPipelineConfig()
    config.sources = ["eia", "synthetic"]
    config.regions = ["PACW", "SYNTHETIC"]
    config.splits.train_start = "2010-01-01"
    config.splits.train_end = "2019-12-31"
    return config
