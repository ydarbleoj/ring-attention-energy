"""
Multi-source storage manager for energy data integration.
Handles data from EIA, CAISO, weather, and other sources.
"""
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class DataSource(Enum):
    """Supported data sources"""
    EIA = "eia"
    CAISO = "caiso"
    WEATHER = "weather"
    ERCOT = "ercot"
    NYISO = "nyiso"
    PJM = "pjm"


class DataLayer(Enum):
    """Data storage layers"""
    RAW = "raw"
    HARMONIZED = "harmonized"
    FEATURES = "features"


@dataclass
class DatasetMetadata:
    """Metadata for a dataset"""
    source: DataSource
    layer: DataLayer
    region: str
    data_type: str
    start_date: datetime
    end_date: datetime
    record_count: int
    file_size_mb: float
    created_at: datetime
    updated_at: datetime
    schema_version: str = "1.0"


class MultiSourceStorageManager:
    """
    Multi-source storage manager with data harmonization capabilities
    """

    def __init__(self, base_path: Union[str, Path]):
        self.base_path = Path(base_path)
        self.ensure_directory_structure()

    def ensure_directory_structure(self):
        """Create directory structure for multi-source data"""
        layers = [layer.value for layer in DataLayer]
        sources = [source.value for source in DataSource]

        for layer in layers:
            layer_path = self.base_path / layer
            layer_path.mkdir(parents=True, exist_ok=True)

            for source in sources:
                source_path = layer_path / source
                source_path.mkdir(parents=True, exist_ok=True)

    def save_raw_data(
        self,
        data: pl.DataFrame,
        source: DataSource,
        region: str,
        data_type: str,
        timestamp: Optional[datetime] = None
    ) -> Path:
        """
        Save raw data from a specific source

        Args:
            data: DataFrame to save
            source: Data source (EIA, CAISO, etc.)
            region: Geographic region
            data_type: Type of data (demand, generation, etc.)
            timestamp: Optional timestamp for the data

        Returns:
            Path to saved file
        """
        if timestamp is None:
            timestamp = datetime.now()

        # Create filename
        date_str = timestamp.strftime("%Y%m%d")
        filename = f"{region}_{data_type}_{date_str}.parquet"

        # Create path
        file_path = self.base_path / DataLayer.RAW.value / source.value / filename

        # Save data
        data.write_parquet(file_path)

        # Save metadata
        metadata = DatasetMetadata(
            source=source,
            layer=DataLayer.RAW,
            region=region,
            data_type=data_type,
            start_date=timestamp,
            end_date=timestamp,
            record_count=len(data),
            file_size_mb=file_path.stat().st_size / (1024 * 1024),
            created_at=datetime.now(),
            updated_at=datetime.now()
        )

        self._save_metadata(metadata, file_path)

        logger.info(f"Saved {len(data)} {data_type} records for {region} from {source.value}")
        return file_path

    def load_raw_data(
        self,
        source: DataSource,
        region: str,
        data_type: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> pl.DataFrame:
        """
        Load raw data from a specific source

        Args:
            source: Data source
            region: Geographic region
            data_type: Type of data
            start_date: Optional start date filter
            end_date: Optional end date filter

        Returns:
            DataFrame with loaded data
        """
        source_path = self.base_path / DataLayer.RAW.value / source.value

        # Find matching files
        pattern = f"{region}_{data_type}_*.parquet"
        files = list(source_path.glob(pattern))

        if not files:
            logger.warning(f"No {data_type} files found for {region} from {source.value}")
            return pl.DataFrame()

        # Load and combine files
        dataframes = []
        for file_path in files:
            try:
                df = pl.read_parquet(file_path)
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Error loading {file_path}: {e}")
                continue

        if not dataframes:
            return pl.DataFrame()

        # Combine all dataframes
        combined_df = pl.concat(dataframes)

        # Apply date filters if provided
        if start_date or end_date:
            if 'timestamp' in combined_df.columns:
                if start_date:
                    combined_df = combined_df.filter(pl.col('timestamp') >= start_date)
                if end_date:
                    combined_df = combined_df.filter(pl.col('timestamp') <= end_date)

        logger.info(f"Loaded {len(combined_df)} {data_type} records for {region} from {source.value}")
        return combined_df

    def harmonize_data(
        self,
        source_data: Dict[DataSource, pl.DataFrame],
        region: str,
        data_type: str
    ) -> pl.DataFrame:
        """
        Harmonize data from multiple sources into a common schema

        Args:
            source_data: Dictionary mapping sources to their data
            region: Geographic region
            data_type: Type of data

        Returns:
            Harmonized DataFrame
        """
        harmonized_dfs = []

        for source, df in source_data.items():
            if df.is_empty():
                continue

            # Apply source-specific harmonization
            harmonized_df = self._harmonize_source_data(df, source, data_type)
            harmonized_dfs.append(harmonized_df)

        if not harmonized_dfs:
            return pl.DataFrame()

        # Combine harmonized data
        combined_df = pl.concat(harmonized_dfs)

        # Sort by timestamp
        if 'timestamp' in combined_df.columns:
            combined_df = combined_df.sort('timestamp')

        # Save harmonized data
        harmonized_path = self.save_harmonized_data(combined_df, region, data_type)

        logger.info(f"Harmonized {len(combined_df)} {data_type} records for {region}")
        return combined_df

    def save_harmonized_data(
        self,
        data: pl.DataFrame,
        region: str,
        data_type: str,
        timestamp: Optional[datetime] = None
    ) -> Path:
        """Save harmonized data"""
        if timestamp is None:
            timestamp = datetime.now()

        date_str = timestamp.strftime("%Y%m%d")
        filename = f"{region}_{data_type}_harmonized_{date_str}.parquet"

        file_path = self.base_path / DataLayer.HARMONIZED.value / filename
        data.write_parquet(file_path)

        logger.info(f"Saved harmonized {data_type} data for {region}")
        return file_path

    def _harmonize_source_data(
        self,
        df: pl.DataFrame,
        source: DataSource,
        data_type: str
    ) -> pl.DataFrame:
        """
        Apply source-specific harmonization

        Args:
            df: Source DataFrame
            source: Data source
            data_type: Type of data

        Returns:
            Harmonized DataFrame
        """
        # Common harmonization schema
        harmonized_schema = {
            'timestamp': pl.Datetime,
            'region': pl.Utf8,
            'data_source': pl.Utf8,
            'value': pl.Float64,
            'unit': pl.Utf8,
            'data_type': pl.Utf8
        }

        # Source-specific column mapping
        if source == DataSource.EIA:
            # EIA harmonization
            if data_type == 'demand' and 'value' in df.columns:
                harmonized_df = df.select([
                    pl.col('period').alias('timestamp'),
                    pl.lit('US').alias('region'),  # Default, should be passed in
                    pl.lit('EIA').alias('data_source'),
                    pl.col('value').alias('value'),
                    pl.lit('MWh').alias('unit'),
                    pl.lit(data_type).alias('data_type')
                ])
            else:
                # Generic EIA mapping
                harmonized_df = df.with_columns([
                    pl.lit('EIA').alias('data_source'),
                    pl.lit(data_type).alias('data_type')
                ])

        elif source == DataSource.CAISO:
            # CAISO harmonization
            if data_type == 'demand' and 'demand_mw' in df.columns:
                harmonized_df = df.select([
                    pl.col('timestamp').alias('timestamp'),
                    pl.lit('California').alias('region'),
                    pl.lit('CAISO').alias('data_source'),
                    pl.col('demand_mw').cast(pl.Float64).alias('value'),
                    pl.lit('MW').alias('unit'),
                    pl.lit(data_type).alias('data_type')
                ])
            elif data_type == 'generation' and 'generation_mw' in df.columns:
                harmonized_df = df.select([
                    pl.col('timestamp').alias('timestamp'),
                    pl.lit('California').alias('region'),
                    pl.lit('CAISO').alias('data_source'),
                    pl.col('generation_mw').cast(pl.Float64).alias('value'),
                    pl.lit('MW').alias('unit'),
                    pl.lit(data_type).alias('data_type')
                ])
            elif data_type == 'demand' and 'MW' in df.columns:
                # Handle case where raw CAISO data has 'MW' column instead of 'demand_mw'
                harmonized_df = df.select([
                    pl.col('timestamp') if 'timestamp' in df.columns else pl.lit(None).alias('timestamp'),
                    pl.lit('California').alias('region'),
                    pl.lit('CAISO').alias('data_source'),
                    pl.col('MW').cast(pl.Float64).alias('value'),
                    pl.lit('MW').alias('unit'),
                    pl.lit(data_type).alias('data_type')
                ])
            else:
                # Generic CAISO mapping
                harmonized_df = df.with_columns([
                    pl.lit('CAISO').alias('data_source'),
                    pl.lit(data_type).alias('data_type')
                ])

        else:
            # Generic harmonization
            harmonized_df = df.with_columns([
                pl.lit(source.value).alias('data_source'),
                pl.lit(data_type).alias('data_type')
            ])

        return harmonized_df

    def create_migration_features(
        self,
        regions: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> pl.DataFrame:
        """
        Create migration prediction features from multi-source data

        Args:
            regions: List of regions to analyze
            start_date: Start date for analysis
            end_date: End date for analysis

        Returns:
            DataFrame with migration features
        """
        features = []

        for region in regions:
            # Load harmonized data for each region
            region_data = self.load_harmonized_data(
                region=region,
                start_date=start_date,
                end_date=end_date
            )

            if region_data.is_empty():
                continue

            # Calculate regional features
            region_features = self._calculate_regional_features(region_data, region)
            features.append(region_features)

        if not features:
            return pl.DataFrame()

        # Combine features
        combined_features = pl.concat(features)

        # Calculate cross-regional features
        cross_regional_features = self._calculate_cross_regional_features(
            combined_features, regions
        )

        # Save feature data
        feature_path = self.save_feature_data(
            cross_regional_features,
            f"migration_features_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        )

        return cross_regional_features

    def load_harmonized_data(
        self,
        region: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> pl.DataFrame:
        """Load harmonized data for a region"""
        harmonized_path = self.base_path / DataLayer.HARMONIZED.value

        # Find matching files
        pattern = f"{region}_*_harmonized_*.parquet"
        files = list(harmonized_path.glob(pattern))

        if not files:
            logger.warning(f"No harmonized files found for {region}")
            return pl.DataFrame()

        # Load and combine files
        dataframes = []
        for file_path in files:
            try:
                df = pl.read_parquet(file_path)
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Error loading {file_path}: {e}")
                continue

        if not dataframes:
            return pl.DataFrame()

        combined_df = pl.concat(dataframes)

        # Apply date filters
        if start_date or end_date:
            if 'timestamp' in combined_df.columns:
                if start_date:
                    combined_df = combined_df.filter(pl.col('timestamp') >= start_date)
                if end_date:
                    combined_df = combined_df.filter(pl.col('timestamp') <= end_date)

        return combined_df

    def save_feature_data(
        self,
        data: pl.DataFrame,
        filename: str
    ) -> Path:
        """Save feature data"""
        file_path = self.base_path / DataLayer.FEATURES.value / f"{filename}.parquet"
        data.write_parquet(file_path)

        logger.info(f"Saved feature data to {file_path}")
        return file_path

    def _calculate_regional_features(
        self,
        data: pl.DataFrame,
        region: str
    ) -> pl.DataFrame:
        """Calculate features for a specific region"""
        # Group by time periods (daily, weekly, monthly)
        daily_features = data.group_by_dynamic(
            'timestamp',
            every='1d'
        ).agg([
            pl.col('value').mean().alias('daily_avg'),
            pl.col('value').max().alias('daily_max'),
            pl.col('value').min().alias('daily_min'),
            pl.col('value').std().alias('daily_std')
        ])

        # Add region identifier
        daily_features = daily_features.with_columns([
            pl.lit(region).alias('region')
        ])

        return daily_features

    def _calculate_cross_regional_features(
        self,
        regional_features: pl.DataFrame,
        regions: List[str]
    ) -> pl.DataFrame:
        """Calculate features across regions"""
        # Create region pairs for migration analysis
        cross_features = []

        for i, region_from in enumerate(regions):
            for j, region_to in enumerate(regions):
                if i != j:  # Don't compare region to itself
                    # Calculate differential features
                    from_data = regional_features.filter(
                        pl.col('region') == region_from
                    )
                    to_data = regional_features.filter(
                        pl.col('region') == region_to
                    )

                    if not from_data.is_empty() and not to_data.is_empty():
                        # Join on timestamp and calculate differentials
                        diff_features = from_data.join(
                            to_data,
                            on='timestamp',
                            suffix='_to'
                        ).with_columns([
                            pl.lit(region_from).alias('region_from'),
                            pl.lit(region_to).alias('region_to'),
                            (pl.col('daily_avg_to') - pl.col('daily_avg')).alias('avg_differential'),
                            (pl.col('daily_max_to') - pl.col('daily_max')).alias('max_differential'),
                            (pl.col('daily_avg_to') / pl.col('daily_avg')).alias('avg_ratio')
                        ])

                        cross_features.append(diff_features)

        if cross_features:
            return pl.concat(cross_features)
        else:
            return pl.DataFrame()

    def _save_metadata(self, metadata: DatasetMetadata, file_path: Path):
        """Save dataset metadata"""
        metadata_path = file_path.with_suffix('.json')

        metadata_dict = {
            'source': metadata.source.value,
            'layer': metadata.layer.value,
            'region': metadata.region,
            'data_type': metadata.data_type,
            'start_date': metadata.start_date.isoformat(),
            'end_date': metadata.end_date.isoformat(),
            'record_count': metadata.record_count,
            'file_size_mb': metadata.file_size_mb,
            'created_at': metadata.created_at.isoformat(),
            'updated_at': metadata.updated_at.isoformat(),
            'schema_version': metadata.schema_version
        }

        import json
        with open(metadata_path, 'w') as f:
            json.dump(metadata_dict, f, indent=2)

    def get_available_datasets(self) -> List[Dict[str, Any]]:
        """Get list of available datasets"""
        datasets = []

        for layer in DataLayer:
            layer_path = self.base_path / layer.value
            if layer_path.exists():
                for source in DataSource:
                    source_path = layer_path / source.value
                    if source_path.exists():
                        for file_path in source_path.glob('*.parquet'):
                            metadata_path = file_path.with_suffix('.json')
                            if metadata_path.exists():
                                try:
                                    import json
                                    with open(metadata_path, 'r') as f:
                                        metadata = json.load(f)
                                    datasets.append(metadata)
                                except Exception as e:
                                    logger.error(f"Error loading metadata from {metadata_path}: {e}")

        return datasets
