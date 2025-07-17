"""MLX-compatible data loaders for energy time series."""

import mlx.core as mx
import polars as pl
import numpy as np
from typing import Iterator, List, Optional, Tuple, Dict, Any, Union
from pathlib import Path
import logging

from ..integrations.eia.services import DataLoader, StorageManager
from .sequence_generator import SequenceGenerator, RingAttentionSequenceGenerator

logger = logging.getLogger(__name__)


class EnergyDataLoader:
    """High-level data loader that bridges EIA service layer with MLX."""

    def __init__(
        self,
        eia_data_loader: DataLoader,
        sequence_generator: Optional[SequenceGenerator] = None
    ):
        """Initialize energy data loader.

        Args:
            eia_data_loader: EIA service layer data loader
            sequence_generator: Sequence generator for ML training
        """
        self.eia_loader = eia_data_loader
        self.seq_gen = sequence_generator or SequenceGenerator()

    @classmethod
    def create_from_storage(
        cls,
        storage_path: Union[str, Path],
        api_key: Optional[str] = None,
        sequence_config: Optional[Dict[str, Any]] = None
    ) -> "EnergyDataLoader":
        """Create data loader from existing storage.

        Args:
            storage_path: Path to Parquet storage
            api_key: EIA API key (optional if only loading from storage)
            sequence_config: Configuration for sequence generator

        Returns:
            Configured EnergyDataLoader
        """
        # Create EIA data loader
        if api_key:
            eia_loader = DataLoader.create_with_storage(api_key, storage_path)
        else:
            # Storage-only mode
            storage_manager = StorageManager(storage_path)
            eia_loader = DataLoader(client=None, storage_manager=storage_manager)

        # Create sequence generator
        seq_config = sequence_config or {}
        seq_gen = SequenceGenerator(**seq_config)

        return cls(eia_loader, seq_gen)

    def load_training_data(
        self,
        start_date: str,
        end_date: str,
        region: str = "PACW",
        use_cache: bool = True,
        normalize: bool = True
    ) -> pl.DataFrame:
        """Load and prepare training data.

        Args:
            start_date: Start date for data
            end_date: End date for data
            region: Energy region code
            use_cache: Whether to use cached data
            normalize: Whether to fit normalizer

        Returns:
            Prepared Polars DataFrame
        """
        logger.info(f"Loading training data for {region} from {start_date} to {end_date}")

        # Check if data exists in storage
        filename = f"comprehensive_{region}_{start_date}_to_{end_date}"

        if use_cache and self.eia_loader.storage.file_exists(filename):
            logger.info("Loading from storage cache...")
            df = self.eia_loader.load_from_storage(filename)
        else:
            logger.info("Fetching fresh data from API...")
            # Load comprehensive data (demand + generation)
            data = self.eia_loader.load_comprehensive_data(
                start_date=start_date,
                end_date=end_date,
                region=region,
                save_to_storage=True,
                storage_filename=f"comprehensive_{region}_{start_date}_to_{end_date}"
            )

            # Join demand and generation data
            df = self.eia_loader.join_demand_generation(
                data["demand"],
                data["generation"],
                save_to_storage=True,
                storage_filename=filename
            )

        # Fit normalizer if requested
        if normalize:
            self.seq_gen.fit_normalizer(df)
            logger.info("Fitted normalization statistics")

        return df

    def create_training_sequences(
        self,
        df: pl.DataFrame,
        target_column: str = "demand_demand_mwh",
        batch_size: int = 32
    ) -> Iterator[Tuple[mx.array, mx.array]]:
        """Create training sequences.

        Args:
            df: Input DataFrame
            target_column: Target column for prediction
            batch_size: Batch size for training

        Yields:
            Batched training sequences
        """
        sequences = self.seq_gen.create_sequences(df, target_column)
        yield from self.seq_gen.batch_sequences(sequences, batch_size)

    def create_prediction_sequences(
        self,
        df: pl.DataFrame,
        context_length: Optional[int] = None
    ) -> Iterator[mx.array]:
        """Create sequences for prediction.

        Args:
            df: Input DataFrame
            context_length: Context length for prediction

        Yields:
            Prediction sequences
        """
        yield from self.seq_gen.create_prediction_sequences(df, context_length)

    def get_data_info(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Get comprehensive data information.

        Args:
            df: Input DataFrame

        Returns:
            Data information dictionary
        """
        info = self.seq_gen.get_sequence_info(df)

        # Add data quality metrics
        info.update({
            "data_quality": {
                "total_records": len(df),
                "missing_values": {col: df[col].null_count() for col in df.columns},
                "date_gaps": self._check_date_gaps(df),
                "value_ranges": {
                    col: {"min": df[col].min(), "max": df[col].max()}
                    for col in df.columns if df[col].dtype in [pl.Float64, pl.Int64]
                }
            }
        })

        return info

    def _check_date_gaps(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Check for gaps in time series data."""
        if "datetime" not in df.columns:
            return {"error": "No datetime column found"}

        sorted_df = df.sort("datetime")
        datetime_series = sorted_df["datetime"]

        # Calculate time differences
        time_diffs = datetime_series.diff().dt.total_hours()

        # Expected hourly data (1 hour difference)
        expected_diff = 1.0
        gaps = time_diffs.filter(time_diffs > expected_diff * 1.5)  # Allow some tolerance

        return {
            "total_gaps": len(gaps),
            "max_gap_hours": float(gaps.max()) if len(gaps) > 0 else 0.0,
            "expected_interval_hours": expected_diff
        }


class MLXDataLoader:
    """Direct MLX data loader for pre-processed energy data."""

    def __init__(
        self,
        storage_manager: StorageManager,
        batch_size: int = 32,
        sequence_length: int = 8760,
        device: str = "gpu"
    ):
        """Initialize MLX data loader.

        Args:
            storage_manager: Storage manager for Parquet files
            batch_size: Batch size for training
            sequence_length: Sequence length for training
            device: MLX device
        """
        self.storage = storage_manager
        self.batch_size = batch_size
        self.sequence_length = sequence_length
        self.device = device

    def load_training_dataset(
        self,
        filenames: List[str],
        target_column: str = "demand_demand_mwh",
        features: Optional[List[str]] = None,
        shuffle: bool = True
    ) -> Iterator[Tuple[mx.array, mx.array]]:
        """Load training dataset from multiple files.

        Args:
            filenames: List of Parquet files to load
            target_column: Target column for prediction
            features: Feature columns to use
            shuffle: Whether to shuffle data

        Yields:
            Training batches as MLX arrays
        """
        # Load all data
        dfs = []
        for filename in filenames:
            try:
                df = self.storage.load_dataframe(filename)
                dfs.append(df)
                logger.info(f"Loaded {len(df)} records from {filename}")
            except FileNotFoundError:
                logger.warning(f"File not found: {filename}")

        if not dfs:
            raise ValueError("No data files could be loaded")

        # Concatenate all data
        combined_df = pl.concat(dfs)
        logger.info(f"Combined dataset: {len(combined_df)} total records")

        # Shuffle if requested
        if shuffle:
            combined_df = combined_df.sample(fraction=1.0, shuffle=True)

        # Create sequence generator
        seq_gen = SequenceGenerator(
            sequence_length=self.sequence_length,
            features=features,
            device=self.device
        )

        # Fit normalizer
        seq_gen.fit_normalizer(combined_df)

        # Generate sequences
        sequences = seq_gen.create_sequences(combined_df, target_column)
        yield from seq_gen.batch_sequences(sequences, self.batch_size)

    def load_validation_dataset(
        self,
        filename: str,
        target_column: str = "demand_demand_mwh",
        features: Optional[List[str]] = None
    ) -> Iterator[Tuple[mx.array, mx.array]]:
        """Load validation dataset.

        Args:
            filename: Validation data file
            target_column: Target column
            features: Feature columns

        Yields:
            Validation batches
        """
        df = self.storage.load_dataframe(filename)

        seq_gen = SequenceGenerator(
            sequence_length=self.sequence_length,
            features=features,
            device=self.device
        )

        # Note: Don't fit normalizer on validation data
        sequences = seq_gen.create_sequences(df, target_column)
        yield from seq_gen.batch_sequences(sequences, self.batch_size)


class RingAttentionDataLoader(EnergyDataLoader):
    """Specialized data loader for ring attention training."""

    def __init__(
        self,
        eia_data_loader: DataLoader,
        ring_config: Optional[Dict[str, Any]] = None
    ):
        """Initialize ring attention data loader.

        Args:
            eia_data_loader: EIA service layer data loader
            ring_config: Ring attention configuration
        """
        ring_config = ring_config or {}
        ring_seq_gen = RingAttentionSequenceGenerator(**ring_config)
        super().__init__(eia_data_loader, ring_seq_gen)

    def create_ring_training_sequences(
        self,
        df: pl.DataFrame,
        target_column: str = "demand_demand_mwh"
    ) -> Iterator[List[Tuple[mx.array, mx.array]]]:
        """Create ring attention training sequences.

        Args:
            df: Input DataFrame
            target_column: Target column

        Yields:
            Ring attention sequence segments
        """
        yield from self.seq_gen.create_ring_sequences(df, target_column)

    def get_ring_info(self) -> Dict[str, Any]:
        """Get ring attention configuration."""
        return self.seq_gen.get_ring_info()


# Utility functions for common operations
def create_energy_ml_pipeline(
    api_key: str,
    storage_path: Union[str, Path],
    sequence_config: Optional[Dict[str, Any]] = None,
    ring_attention: bool = False
) -> Union[EnergyDataLoader, RingAttentionDataLoader]:
    """Create complete ML pipeline for energy data.

    Args:
        api_key: EIA API key
        storage_path: Storage path for Parquet files
        sequence_config: Sequence generator configuration
        ring_attention: Whether to use ring attention

    Returns:
        Configured data loader
    """
    # Create EIA data loader
    eia_loader = DataLoader.create_with_storage(api_key, storage_path)

    if ring_attention:
        return RingAttentionDataLoader(eia_loader, sequence_config)
    else:
        seq_gen = SequenceGenerator(**(sequence_config or {}))
        return EnergyDataLoader(eia_loader, seq_gen)


def load_pretrained_sequences(
    storage_path: Union[str, Path],
    sequence_files: List[str],
    batch_size: int = 32
) -> MLXDataLoader:
    """Load pre-processed sequences for training.

    Args:
        storage_path: Storage path
        sequence_files: List of sequence files
        batch_size: Batch size

    Returns:
        Configured MLX data loader
    """
    storage = StorageManager(storage_path)
    return MLXDataLoader(storage, batch_size=batch_size)
