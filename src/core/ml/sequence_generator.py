"""Sequence generator for ring attention training on energy data."""

import mlx.core as mx
import polars as pl
import numpy as np
from typing import Iterator, List, Optional, Tuple, Dict, Any
from datetime import datetime, timedelta
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class SequenceGenerator:
    """Generates sequences for ring attention training from energy time series data."""

    def __init__(
        self,
        sequence_length: int = 8760,  # 1 year of hourly data
        stride: int = 24,             # Daily stride
        features: Optional[List[str]] = None,
        normalize: bool = True,
        device: str = "gpu"
    ):
        """Initialize sequence generator.

        Args:
            sequence_length: Length of each training sequence (hours)
            stride: Step size between sequences (hours)
            features: List of feature columns to include
            normalize: Whether to normalize features
            device: MLX device to use
        """
        self.sequence_length = sequence_length
        self.stride = stride
        self.features = features or self._default_features()
        self.normalize = normalize
        self.device = device

        # Normalization statistics
        self.feature_stats: Dict[str, Dict[str, float]] = {}

    def _default_features(self) -> List[str]:
        """Default feature set for energy modeling."""
        return [
            "demand_mwh",
            "solar_generation",
            "wind_generation",
            "hydro_generation",
            "natural_gas_generation",
            "hour_of_day",
            "day_of_week",
            "month_of_year"
        ]

    def fit_normalizer(self, df: pl.DataFrame) -> None:
        """Fit normalization statistics on training data.

        Args:
            df: Polars DataFrame with energy data
        """
        logger.info("Fitting normalization statistics...")

        for feature in self.features:
            if feature in df.columns:
                values = df[feature].to_numpy()
                # Handle missing values
                values = values[~np.isnan(values)]

                if len(values) > 0:
                    self.feature_stats[feature] = {
                        "mean": float(np.mean(values)),
                        "std": float(np.std(values)),
                        "min": float(np.min(values)),
                        "max": float(np.max(values))
                    }
                    logger.debug(f"Feature {feature}: mean={self.feature_stats[feature]['mean']:.2f}, "
                               f"std={self.feature_stats[feature]['std']:.2f}")
                else:
                    logger.warning(f"No valid data for feature: {feature}")

    def add_temporal_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add temporal features to the DataFrame.

        Args:
            df: Input DataFrame with datetime column

        Returns:
            DataFrame with added temporal features
        """
        return df.with_columns([
            pl.col("datetime").dt.hour().alias("hour_of_day"),
            pl.col("datetime").dt.weekday().alias("day_of_week"),
            pl.col("datetime").dt.month().alias("month_of_year"),
            pl.col("datetime").dt.ordinal_day().alias("day_of_year")
        ])

    def normalize_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize features using fitted statistics.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with normalized features
        """
        if not self.normalize or not self.feature_stats:
            return df

        expressions = []
        for col in df.columns:
            if col in self.feature_stats:
                stats = self.feature_stats[col]
                # Z-score normalization
                normalized = (pl.col(col) - stats["mean"]) / stats["std"]
                expressions.append(normalized.alias(f"{col}_norm"))
            else:
                expressions.append(pl.col(col))

        return df.with_columns(expressions)

    def create_sequences(
        self,
        df: pl.DataFrame,
        target_column: str = "demand_mwh"
    ) -> Iterator[Tuple[mx.array, mx.array]]:
        """Generate training sequences from DataFrame.

        Args:
            df: Input DataFrame with energy data
            target_column: Column to use as prediction target

        Yields:
            Tuple of (input_sequence, target_sequence) as MLX arrays
        """
        # Add temporal features
        df = self.add_temporal_features(df)

        # Normalize if requested
        if self.normalize:
            df = self.normalize_features(df)

        # Sort by datetime
        df = df.sort("datetime")

        # Select features for modeling
        available_features = [f for f in self.features if f in df.columns]
        if not available_features:
            raise ValueError(f"No features found in DataFrame. Available columns: {df.columns}")

        logger.info(f"Using features: {available_features}")

        # Convert to numpy for sequence generation
        feature_data = df.select(available_features).to_numpy()
        target_data = df[target_column].to_numpy()

        # Generate sequences
        num_sequences = (len(df) - self.sequence_length) // self.stride + 1

        for i in range(0, len(df) - self.sequence_length + 1, self.stride):
            # Input sequence (features)
            input_seq = feature_data[i:i + self.sequence_length]

            # Target sequence (shifted by 1 for prediction)
            target_seq = target_data[i + 1:i + self.sequence_length + 1]

            # Convert to MLX arrays
            input_mx = mx.array(input_seq, dtype=mx.float32)
            target_mx = mx.array(target_seq, dtype=mx.float32)

            yield input_mx, target_mx

    def create_prediction_sequences(
        self,
        df: pl.DataFrame,
        context_length: Optional[int] = None
    ) -> Iterator[mx.array]:
        """Generate sequences for prediction (inference).

        Args:
            df: Input DataFrame
            context_length: Length of context to use (defaults to sequence_length)

        Yields:
            Input sequences as MLX arrays
        """
        context_len = context_length or self.sequence_length

        # Add temporal features and normalize
        df = self.add_temporal_features(df)
        if self.normalize:
            df = self.normalize_features(df)

        # Sort by datetime
        df = df.sort("datetime")

        # Select features
        available_features = [f for f in self.features if f in df.columns]
        feature_data = df.select(available_features).to_numpy()

        # Generate non-overlapping sequences for prediction
        for i in range(0, len(df) - context_len + 1, context_len):
            input_seq = feature_data[i:i + context_len]
            yield mx.array(input_seq, dtype=mx.float32)

    def batch_sequences(
        self,
        sequences: Iterator[Tuple[mx.array, mx.array]],
        batch_size: int = 32
    ) -> Iterator[Tuple[mx.array, mx.array]]:
        """Batch sequences for training.

        Args:
            sequences: Iterator of (input, target) sequences
            batch_size: Number of sequences per batch

        Yields:
            Batched sequences as MLX arrays
        """
        batch_inputs = []
        batch_targets = []

        for input_seq, target_seq in sequences:
            batch_inputs.append(input_seq)
            batch_targets.append(target_seq)

            if len(batch_inputs) == batch_size:
                # Stack into batch dimension
                batch_input = mx.stack(batch_inputs, axis=0)
                batch_target = mx.stack(batch_targets, axis=0)

                yield batch_input, batch_target

                # Reset batch
                batch_inputs = []
                batch_targets = []

        # Yield remaining incomplete batch
        if batch_inputs:
            batch_input = mx.stack(batch_inputs, axis=0)
            batch_target = mx.stack(batch_targets, axis=0)
            yield batch_input, batch_target

    def get_sequence_info(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Get information about sequences that would be generated.

        Args:
            df: Input DataFrame

        Returns:
            Dictionary with sequence information
        """
        total_hours = len(df)
        num_sequences = (total_hours - self.sequence_length) // self.stride + 1

        available_features = [f for f in self.features if f in df.columns]

        return {
            "total_hours": total_hours,
            "sequence_length": self.sequence_length,
            "stride": self.stride,
            "num_sequences": num_sequences,
            "num_features": len(available_features),
            "available_features": available_features,
            "missing_features": [f for f in self.features if f not in df.columns],
            "date_range": {
                "start": str(df["datetime"].min()),
                "end": str(df["datetime"].max())
            }
        }


class RingAttentionSequenceGenerator(SequenceGenerator):
    """Specialized sequence generator optimized for ring attention training."""

    def __init__(
        self,
        sequence_length: int = 8760,  # 1 year
        ring_size: int = 8,           # Number of devices for ring attention
        overlap: int = 168,           # 1 week overlap between rings
        **kwargs
    ):
        """Initialize ring attention sequence generator.

        Args:
            sequence_length: Total sequence length
            ring_size: Number of devices in ring attention setup
            overlap: Overlap between ring segments
            **kwargs: Additional arguments for base class
        """
        super().__init__(sequence_length=sequence_length, **kwargs)
        self.ring_size = ring_size
        self.overlap = overlap
        self.segment_length = (sequence_length + overlap * (ring_size - 1)) // ring_size

    def create_ring_sequences(
        self,
        df: pl.DataFrame,
        target_column: str = "demand_mwh"
    ) -> Iterator[List[Tuple[mx.array, mx.array]]]:
        """Generate sequences optimized for ring attention.

        Args:
            df: Input DataFrame
            target_column: Target column for prediction

        Yields:
            List of sequence segments for ring attention devices
        """
        for input_seq, target_seq in self.create_sequences(df, target_column):
            # Split sequence into overlapping segments for ring attention
            segments = []

            for i in range(self.ring_size):
                start_idx = i * (self.segment_length - self.overlap)
                end_idx = start_idx + self.segment_length

                # Handle edge cases
                if end_idx > input_seq.shape[0]:
                    end_idx = input_seq.shape[0]
                    start_idx = max(0, end_idx - self.segment_length)

                segment_input = input_seq[start_idx:end_idx]
                segment_target = target_seq[start_idx:end_idx]

                segments.append((segment_input, segment_target))

            yield segments

    def get_ring_info(self) -> Dict[str, Any]:
        """Get ring attention configuration info."""
        return {
            "sequence_length": self.sequence_length,
            "ring_size": self.ring_size,
            "segment_length": self.segment_length,
            "overlap": self.overlap,
            "total_length_with_overlap": self.segment_length * self.ring_size - self.overlap * (self.ring_size - 1)
        }
