import mlx.core as mx
import polars as pl
import numpy as np
from typing import Dict, List, Optional, Tuple, Union, Any
from datetime import datetime, timedelta
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class EnergyFeatureEngineer:
    """Feature engineering utilities for energy time series data."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize feature engineer.

        Args:
            config: Configuration dictionary with feature engineering parameters
        """
        self.config = config or {}
        self.feature_cache = {}

    def create_temporal_features(self, df: pl.DataFrame, datetime_col: str = "datetime") -> pl.DataFrame:
        """Create temporal features from datetime column.

        Args:
            df: Input Polars DataFrame
            datetime_col: Name of datetime column

        Returns:
            DataFrame with added temporal features
        """
        logger.info("Creating temporal features...")

        # Ensure datetime column is properly formatted
        if df[datetime_col].dtype != pl.Datetime:
            df = df.with_columns(pl.col(datetime_col).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))

        # Extract temporal components
        df = df.with_columns([
            pl.col(datetime_col).dt.year().alias("year"),
            pl.col(datetime_col).dt.month().alias("month"),
            pl.col(datetime_col).dt.day().alias("day"),
            pl.col(datetime_col).dt.hour().alias("hour"),
            pl.col(datetime_col).dt.weekday().alias("weekday"),
            pl.col(datetime_col).dt.ordinal_day().alias("day_of_year")
        ])

        # Create cyclical features for periodicity
        df = df.with_columns([
            # Hourly cycle (0-23)
            (2 * np.pi * pl.col("hour") / 24).sin().alias("hour_sin"),
            (2 * np.pi * pl.col("hour") / 24).cos().alias("hour_cos"),

            # Daily cycle (0-6, Monday=0)
            (2 * np.pi * pl.col("weekday") / 7).sin().alias("weekday_sin"),
            (2 * np.pi * pl.col("weekday") / 7).cos().alias("weekday_cos"),

            # Monthly cycle (1-12)
            (2 * np.pi * pl.col("month") / 12).sin().alias("month_sin"),
            (2 * np.pi * pl.col("month") / 12).cos().alias("month_cos"),

            # Yearly cycle (1-365/366)
            (2 * np.pi * pl.col("day_of_year") / 365).sin().alias("year_sin"),
            (2 * np.pi * pl.col("day_of_year") / 365).cos().alias("year_cos")
        ])

        # Binary features for special periods
        df = df.with_columns([
            # Weekend flag
            (pl.col("weekday") >= 5).alias("is_weekend"),

            # Business hours (9-17)
            ((pl.col("hour") >= 9) & (pl.col("hour") <= 17)).alias("is_business_hours"),

            # Peak hours (typically 6-9 AM and 5-8 PM)
            (((pl.col("hour") >= 6) & (pl.col("hour") <= 9)) |
             ((pl.col("hour") >= 17) & (pl.col("hour") <= 20))).alias("is_peak_hours"),

            # Summer months (June-August)
            ((pl.col("month") >= 6) & (pl.col("month") <= 8)).alias("is_summer")
        ])

        logger.info(f"Added temporal features, DataFrame shape: {df.shape}")
        return df

    def create_lagged_features(
        self,
        df: pl.DataFrame,
        columns: List[str],
        lags: List[int] = [1, 2, 3, 6, 12, 24, 48, 168]  # 1h, 2h, 3h, 6h, 12h, 1d, 2d, 1w
    ) -> pl.DataFrame:
        """Create lagged features for time series modeling.

        Args:
            df: Input DataFrame
            columns: Columns to create lags for
            lags: List of lag periods (in hours)

        Returns:
            DataFrame with lagged features
        """
        logger.info(f"Creating lagged features for {len(columns)} columns with {len(lags)} lags...")

        # Create lagged features
        for col in columns:
            if col in df.columns:
                for lag in lags:
                    df = df.with_columns(
                        pl.col(col).shift(lag).alias(f"{col}_lag_{lag}h")
                    )

        logger.info(f"Added lagged features, DataFrame shape: {df.shape}")
        return df

    def create_rolling_features(
        self,
        df: pl.DataFrame,
        columns: List[str],
        windows: List[int] = [3, 6, 12, 24, 48, 168]  # 3h, 6h, 12h, 1d, 2d, 1w
    ) -> pl.DataFrame:
        """Create rolling window features (mean, std, min, max).

        Args:
            df: Input DataFrame
            columns: Columns to create rolling features for
            windows: List of window sizes (in hours)

        Returns:
            DataFrame with rolling features
        """
        logger.info(f"Creating rolling features for {len(columns)} columns with {len(windows)} windows...")

        for col in columns:
            if col in df.columns:
                for window in windows:
                    df = df.with_columns([
                        pl.col(col).rolling_mean(window).alias(f"{col}_rolling_mean_{window}h"),
                        pl.col(col).rolling_std(window).alias(f"{col}_rolling_std_{window}h"),
                        pl.col(col).rolling_min(window).alias(f"{col}_rolling_min_{window}h"),
                        pl.col(col).rolling_max(window).alias(f"{col}_rolling_max_{window}h")
                    ])

        logger.info(f"Added rolling features, DataFrame shape: {df.shape}")
        return df

    def create_renewable_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create renewable energy specific features.

        Args:
            df: Input DataFrame with generation data

        Returns:
            DataFrame with renewable energy features
        """
        logger.info("Creating renewable energy features...")

        # Identify renewable columns
        renewable_cols = [col for col in df.columns if any(
            renewable in col.lower() for renewable in ["solar", "wind", "hydro"]
        )]

        if not renewable_cols:
            logger.warning("No renewable energy columns found")
            return df

        # Calculate total renewable generation
        if len(renewable_cols) > 1:
            df = df.with_columns(
                pl.sum_horizontal(renewable_cols).alias("total_renewable_mwh")
            )
        else:
            df = df.with_columns(
                pl.col(renewable_cols[0]).alias("total_renewable_mwh")
            )

        # Calculate renewable penetration (if total generation available)
        total_gen_cols = [col for col in df.columns if "total_generation" in col.lower()]
        if total_gen_cols:
            df = df.with_columns(
                (pl.col("total_renewable_mwh") / pl.col(total_gen_cols[0]) * 100).alias("renewable_penetration_pct")
            )

        # Renewable variability features
        if "solar_mwh" in df.columns:
            df = df.with_columns([
                # Solar capacity factor (assuming max capacity, normalized)
                (pl.col("solar_mwh") / pl.col("solar_mwh").max()).alias("solar_capacity_factor"),
                # Solar ramp rate (change per hour)
                (pl.col("solar_mwh") - pl.col("solar_mwh").shift(1)).alias("solar_ramp_rate")
            ])

        if "wind_mwh" in df.columns:
            df = df.with_columns([
                # Wind capacity factor
                (pl.col("wind_mwh") / pl.col("wind_mwh").max()).alias("wind_capacity_factor"),
                # Wind ramp rate
                (pl.col("wind_mwh") - pl.col("wind_mwh").shift(1)).alias("wind_ramp_rate")
            ])

        # Renewable intermittency index (coefficient of variation)
        if len(renewable_cols) > 0:
            df = df.with_columns([
                # Rolling coefficient of variation for renewable generation
                (pl.col("total_renewable_mwh").rolling_std(24) /
                 pl.col("total_renewable_mwh").rolling_mean(24)).alias("renewable_intermittency_24h")
            ])

        logger.info(f"Added renewable features, DataFrame shape: {df.shape}")
        return df

    def create_demand_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create demand-specific features.

        Args:
            df: Input DataFrame with demand data

        Returns:
            DataFrame with demand features
        """
        logger.info("Creating demand features...")

        # Find demand columns
        demand_cols = [col for col in df.columns if "demand" in col.lower()]

        if not demand_cols:
            logger.warning("No demand columns found")
            return df

        primary_demand_col = demand_cols[0]

        # Demand growth rate
        df = df.with_columns([
            # Hourly growth rate
            ((pl.col(primary_demand_col) - pl.col(primary_demand_col).shift(1)) /
             pl.col(primary_demand_col).shift(1) * 100).alias("demand_growth_rate_1h"),

            # Daily growth rate
            ((pl.col(primary_demand_col) - pl.col(primary_demand_col).shift(24)) /
             pl.col(primary_demand_col).shift(24) * 100).alias("demand_growth_rate_24h")
        ])

        # Demand anomaly detection (z-score based)
        df = df.with_columns([
            # Z-score for demand (rolling 7-day window)
            ((pl.col(primary_demand_col) - pl.col(primary_demand_col).rolling_mean(168)) /
             pl.col(primary_demand_col).rolling_std(168)).alias("demand_z_score_7d")
        ])

        # Peak demand indicators
        df = df.with_columns([
            # Daily peak demand flag
            (pl.col(primary_demand_col) == pl.col(primary_demand_col).rolling_max(24)).alias("is_daily_peak"),

            # Weekly peak demand flag
            (pl.col(primary_demand_col) == pl.col(primary_demand_col).rolling_max(168)).alias("is_weekly_peak")
        ])

        logger.info(f"Added demand features, DataFrame shape: {df.shape}")
        return df

    def create_supply_demand_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """Create supply-demand balance features.

        Args:
            df: Input DataFrame with both supply and demand data

        Returns:
            DataFrame with supply-demand features
        """
        logger.info("Creating supply-demand features...")

        # Find relevant columns
        demand_cols = [col for col in df.columns if "demand" in col.lower()]
        generation_cols = [col for col in df.columns if
                          any(fuel in col.lower() for fuel in ["solar", "wind", "hydro", "gas", "coal", "nuclear"])]

        if not demand_cols or not generation_cols:
            logger.warning("Insufficient supply/demand columns for balance features")
            return df

        primary_demand_col = demand_cols[0]

        # Calculate total generation if not already present
        if "total_generation_mwh" not in df.columns:
            df = df.with_columns(
                pl.sum_horizontal(generation_cols).alias("total_generation_mwh")
            )

        # Supply-demand balance
        df = df.with_columns([
            # Net supply (generation - demand)
            (pl.col("total_generation_mwh") - pl.col(primary_demand_col)).alias("net_supply_mwh"),

            # Supply adequacy ratio
            (pl.col("total_generation_mwh") / pl.col(primary_demand_col)).alias("supply_adequacy_ratio"),

            # Supply shortage indicator
            (pl.col("total_generation_mwh") < pl.col(primary_demand_col)).alias("supply_shortage")
        ])

        # Grid stress indicators
        df = df.with_columns([
            # Reserve margin (assuming 15% reserve requirement)
            ((pl.col("total_generation_mwh") - pl.col(primary_demand_col)) /
             pl.col(primary_demand_col) * 100).alias("reserve_margin_pct")
        ])

        # Create grid stress flag separately to avoid reference issues
        df = df.with_columns([
            (pl.col("reserve_margin_pct") < 5.0).alias("grid_stress")
        ])

        logger.info(f"Added supply-demand features, DataFrame shape: {df.shape}")
        return df

    def create_all_features(
        self,
        df: pl.DataFrame,
        datetime_col: str = "datetime",
        include_lags: bool = True,
        include_rolling: bool = True
    ) -> pl.DataFrame:
        """Create all feature types in one operation.

        Args:
            df: Input DataFrame
            datetime_col: Name of datetime column
            include_lags: Whether to include lagged features
            include_rolling: Whether to include rolling features

        Returns:
            DataFrame with all engineered features
        """
        logger.info("Creating all features...")

        # 1. Temporal features
        df = self.create_temporal_features(df, datetime_col)

        # 2. Energy-specific features
        df = self.create_renewable_features(df)
        df = self.create_demand_features(df)
        df = self.create_supply_demand_features(df)

        # 3. Time series features (can be expensive, so optional)
        if include_lags:
            # Only lag the core energy columns
            core_cols = [col for col in df.columns if any(
                keyword in col.lower() for keyword in ["demand", "generation", "renewable", "solar", "wind"]
            )][:5]  # Limit to first 5 to avoid explosion
            df = self.create_lagged_features(df, core_cols)

        if include_rolling:
            # Only rolling features for core columns
            core_cols = [col for col in df.columns if any(
                keyword in col.lower() for keyword in ["demand", "generation", "renewable", "solar", "wind"]
            )][:3]  # Limit to first 3 to avoid explosion
            df = self.create_rolling_features(df, core_cols)

        logger.info(f"Feature engineering complete. Final DataFrame shape: {df.shape}")
        return df

    def to_mlx_features(
        self,
        df: pl.DataFrame,
        feature_columns: Optional[List[str]] = None,
        normalize: bool = True
    ) -> Tuple[mx.array, List[str]]:
        """Convert Polars DataFrame to MLX array for training.

        Args:
            df: Input DataFrame with features
            feature_columns: Specific columns to include (if None, auto-select)
            normalize: Whether to normalize features

        Returns:
            Tuple of (MLX array, feature names)
        """
        logger.info("Converting to MLX format...")

        # Auto-select feature columns if not provided
        if feature_columns is None:
            # Exclude datetime and string columns
            feature_columns = []
            for col in df.columns:
                if df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64, pl.Boolean]:
                    feature_columns.append(col)

        # Select features and handle missing values
        feature_df = df.select(feature_columns).fill_null(0.0)

        # Convert to numpy then MLX
        feature_array = feature_df.to_numpy().astype(np.float32)

        # Normalize if requested
        if normalize:
            # Store normalization stats for later use
            self.feature_stats = {
                'mean': np.mean(feature_array, axis=0),
                'std': np.std(feature_array, axis=0) + 1e-8  # Add small epsilon
            }

            # Apply normalization
            feature_array = (feature_array - self.feature_stats['mean']) / self.feature_stats['std']

        # Convert to MLX array
        mlx_array = mx.array(feature_array)

        logger.info(f"Created MLX array with shape: {mlx_array.shape}")
        return mlx_array, feature_columns

    def get_feature_importance_names(self) -> List[str]:
        """Get interpretable names for features for analysis.

        Returns:
            List of feature categories for interpretation
        """
        return [
            "Temporal Cyclical",
            "Temporal Linear",
            "Renewable Generation",
            "Demand Patterns",
            "Supply-Demand Balance",
            "Lagged Values",
            "Rolling Statistics",
            "Anomaly Indicators"
        ]


def create_energy_sequence_dataset(
    df: pl.DataFrame,
    sequence_length: int = 168,  # 1 week
    stride: int = 24,           # 1 day
    feature_engineer: Optional[EnergyFeatureEngineer] = None
) -> Tuple[mx.array, mx.array, List[str]]:
    """Create sequences for ring attention training.

    Args:
        df: Input DataFrame
        sequence_length: Length of each sequence
        stride: Step size between sequences
        feature_engineer: Feature engineer instance

    Returns:
        Tuple of (input sequences, target sequences, feature names)
    """
    logger.info(f"Creating sequence dataset with length={sequence_length}, stride={stride}")

    # Apply feature engineering if provided
    if feature_engineer:
        df = feature_engineer.create_all_features(df)

    # Convert to MLX format
    if not feature_engineer:
        feature_engineer = EnergyFeatureEngineer()

    features, feature_names = feature_engineer.to_mlx_features(df)

    # Create sequences
    sequences = []
    targets = []

    for i in range(0, len(features) - sequence_length, stride):
        seq = features[i:i + sequence_length]
        # Target is the next time step
        target = features[i + sequence_length] if i + sequence_length < len(features) else features[-1]

        sequences.append(seq)
        targets.append(target)

    # Convert to MLX arrays
    input_sequences = mx.stack(sequences)
    target_sequences = mx.stack(targets)

    logger.info(f"Created {len(sequences)} sequences with shape: {input_sequences.shape}")
    return input_sequences, target_sequences, feature_names
