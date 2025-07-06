import asyncio
import logging
from typing import Dict, Any, Optional

import polars as pl

from .base import BaseEnergyDataCollector, DataCollectionResult
from ..config import RetryConfig
from src.core.integrations.synthetic.generator import SyntheticEnergyDataGenerator, SyntheticDataConfig

logger = logging.getLogger(__name__)


class SyntheticCollector(BaseEnergyDataCollector):
    """Collector for synthetic energy data for testing and development."""

    def __init__(self, config: Dict[str, Any] = None):
        """Initialize synthetic collector.

        Args:
            config: Configuration dictionary
        """
        super().__init__("Synthetic", config)
        self.retry_config = RetryConfig(**(config.get("retry", {}) if config else {}))

        # Setup synthetic data generator
        self.generator_config = config.get("generator", {}) if config else {}

        # Create SyntheticDataConfig with appropriate parameters
        data_config = SyntheticDataConfig(
            random_seed=self.generator_config.get("seed", 42),
            base_demand_mw=self.generator_config.get("base_demand_mw", 1000.0),
            seasonal_variation=self.generator_config.get("seasonal_strength", 0.3),
            noise_level=self.generator_config.get("noise_level", 0.1),
            daily_variation=self.generator_config.get("daily_variation", 0.2),
            weekly_variation=self.generator_config.get("weekly_variation", 0.1)
        )

        self.generator = SyntheticEnergyDataGenerator(data_config)
        self.region = self.generator_config.get("region", "TEST")

    def get_collector_info(self) -> Dict[str, Any]:
        """Get collector information and status.

        Returns:
            Dictionary with collector information
        """
        base_info = super().get_collector_info()
        base_info.update({
            "generator_configured": self.generator is not None,
            "generator_region": self.region,
            "generator_seed": self.generator.config.random_seed,
            "retry_config": {
                "max_retries": self.retry_config.max_retries,
                "initial_delay": self.retry_config.initial_delay,
                "max_delay": self.retry_config.max_delay
            }
        })
        return base_info

    def _collect_demand_sync(
        self,
        start_date: str,
        end_date: str,
        region: str = "TEST",
        freq: str = "H",
        **kwargs
    ) -> pl.DataFrame:
        """Synchronous demand data generation.

        Args:
            start_date: Start date for data generation
            end_date: End date for data generation
            region: Region code (used for metadata)
            freq: Frequency of data points
            **kwargs: Additional parameters

        Returns:
            Polars DataFrame with synthetic demand data
        """
        # Calculate number of hours from date range
        from datetime import datetime
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        hours = int((end_dt - start_dt).total_seconds() / 3600)

        # Generate full dataset and extract demand data
        df = self.generator.generate_dataset_with_region(region=region, hours=hours)

        # Convert to Polars and ensure expected columns
        if 'timestamp' in df.columns:
            df = df.rename(columns={'timestamp': 'datetime'})
        if 'demand' in df.columns:
            df = df.rename(columns={'demand': 'demand_mwh'})

        # Filter to demand data only
        demand_cols = ['datetime', 'region']
        if 'demand_mwh' in df.columns:
            demand_cols.append('demand_mwh')

        return pl.from_pandas(df[demand_cols])

    def _collect_generation_sync(
        self,
        start_date: str,
        end_date: str,
        region: str = "TEST",
        freq: str = "H",
        fuel_types: Optional[list] = None,
        **kwargs
    ) -> pl.DataFrame:
        """Synchronous generation data generation.

        Args:
            start_date: Start date for data generation
            end_date: End date for data generation
            region: Region code (used for metadata)
            freq: Frequency of data points
            fuel_types: List of fuel types to generate (not used with current generator)
            **kwargs: Additional parameters

        Returns:
            Polars DataFrame with synthetic generation data
        """
        # Calculate number of hours from date range
        from datetime import datetime
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        hours = int((end_dt - start_dt).total_seconds() / 3600)

        # Generate full dataset and extract generation data
        df = self.generator.generate_dataset_with_region(region=region, hours=hours)

        # Convert to Polars and ensure expected columns
        if 'timestamp' in df.columns:
            df = df.rename(columns={'timestamp': 'datetime'})

        # Filter to generation-related columns
        generation_cols = ['datetime', 'region']

        # Add any renewable generation columns
        for col in df.columns:
            if 'solar' in col.lower() or 'wind' in col.lower() or 'generation' in col.lower():
                generation_cols.append(col)

        # If no generation columns found, create synthetic fuel type data
        if len(generation_cols) == 2:  # Only datetime and region
            # Create simple synthetic generation data
            df['generation_mwh'] = df.get('demand', df.get('demand_mwh', 0)) * 0.8  # 80% of demand
            df['fuel_type'] = 'synthetic'
            generation_cols.extend(['generation_mwh', 'fuel_type'])

        return pl.from_pandas(df[generation_cols])

    def _collect_comprehensive_sync(
        self,
        start_date: str,
        end_date: str,
        region: str = "TEST",
        freq: str = "H",
        fuel_types: Optional[list] = None,
        **kwargs
    ) -> Dict[str, pl.DataFrame]:
        """Synchronous comprehensive data generation.

        Args:
            start_date: Start date for data generation
            end_date: End date for data generation
            region: Region code (used for metadata)
            freq: Frequency of data points
            fuel_types: List of fuel types to generate (not used with current generator)
            **kwargs: Additional parameters

        Returns:
            Dictionary with demand and generation DataFrames
        """
        # Generate both demand and generation data
        demand_df = self._collect_demand_sync(start_date, end_date, region, freq, **kwargs)
        generation_df = self._collect_generation_sync(start_date, end_date, region, freq, fuel_types, **kwargs)

        return {
            "demand": demand_df,
            "generation": generation_df
        }

    async def collect_demand_data(
        self,
        start_date: str,
        end_date: str,
        region: str = "TEST",
        **kwargs
    ) -> DataCollectionResult:
        """Collect synthetic demand data.

        Args:
            start_date: Start date for data generation (YYYY-MM-DD format)
            end_date: End date for data generation (YYYY-MM-DD format)
            region: Region code (defaults to TEST)
            **kwargs: Additional arguments for generator

        Returns:
            DataCollectionResult with synthetic demand data
        """
        # Validate date range
        try:
            self.validate_date_range(start_date, end_date)
        except ValueError as e:
            return self.create_error_result(str(e), "Synthetic")

        try:
            # Extract kwargs for sync method
            freq = kwargs.get("freq", "H")

            # Execute in thread pool
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(
                None,
                self._collect_demand_sync,
                start_date,
                end_date,
                region,
                freq
            )

            # Create successful result
            return DataCollectionResult(
                data=data,
                metadata={
                    "source": "Synthetic",
                    "data_type": "demand",
                    "region": region,
                    "start_date": start_date,
                    "end_date": end_date,
                    "records_collected": len(data),
                    "frequency": freq,
                    "generator_seed": self.generator.config.random_seed
                },
                source="Synthetic"
            )

        except Exception as e:
            logger.error(f"Error generating synthetic demand data: {e}")
            return self.create_error_result(f"Error generating synthetic demand data: {str(e)}", "Synthetic")

    async def collect_generation_data(
        self,
        start_date: str,
        end_date: str,
        region: str = "TEST",
        **kwargs
    ) -> DataCollectionResult:
        """Collect synthetic generation data.

        Args:
            start_date: Start date for data generation (YYYY-MM-DD format)
            end_date: End date for data generation (YYYY-MM-DD format)
            region: Region code (defaults to TEST)
            **kwargs: Additional arguments for generator

        Returns:
            DataCollectionResult with synthetic generation data
        """
        # Validate date range
        try:
            self.validate_date_range(start_date, end_date)
        except ValueError as e:
            return self.create_error_result(str(e), "Synthetic")

        try:
            # Extract kwargs for sync method
            freq = kwargs.get("freq", "H")
            fuel_types = kwargs.get("fuel_types")

            # Execute in thread pool
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(
                None,
                self._collect_generation_sync,
                start_date,
                end_date,
                region,
                freq,
                fuel_types
            )

            # Create successful result
            return DataCollectionResult(
                data=data,
                metadata={
                    "source": "Synthetic",
                    "data_type": "generation",
                    "region": region,
                    "start_date": start_date,
                    "end_date": end_date,
                    "records_collected": len(data),
                    "frequency": freq,
                    "fuel_types": fuel_types,
                    "generator_seed": self.generator.config.random_seed
                },
                source="Synthetic"
            )

        except Exception as e:
            logger.error(f"Error generating synthetic generation data: {e}")
            return self.create_error_result(f"Error generating synthetic generation data: {str(e)}", "Synthetic")

    async def collect_comprehensive_data(
        self,
        start_date: str,
        end_date: str,
        region: str = "TEST",
        **kwargs
    ) -> Dict[str, DataCollectionResult]:
        """Collect comprehensive synthetic data (demand and generation).

        Args:
            start_date: Start date for data generation (YYYY-MM-DD format)
            end_date: End date for data generation (YYYY-MM-DD format)
            region: Region code (defaults to TEST)
            **kwargs: Additional arguments for generator

        Returns:
            Dictionary with demand and generation DataCollectionResults
        """
        # Validate date range
        try:
            self.validate_date_range(start_date, end_date)
        except ValueError as e:
            error_result = self.create_error_result(str(e), "Synthetic")
            return {
                "demand": error_result,
                "generation": error_result
            }

        try:
            # Extract kwargs for sync method
            freq = kwargs.get("freq", "H")
            fuel_types = kwargs.get("fuel_types")

            # Execute in thread pool
            loop = asyncio.get_event_loop()
            comprehensive_data = await loop.run_in_executor(
                None,
                self._collect_comprehensive_sync,
                start_date,
                end_date,
                region,
                freq,
                fuel_types
            )

            # Create results for each data type
            results = {}

            for data_type in ["demand", "generation"]:
                if data_type in comprehensive_data:
                    data = comprehensive_data[data_type]
                    results[data_type] = DataCollectionResult(
                        data=data,
                        metadata={
                            "source": "Synthetic",
                            "data_type": data_type,
                            "region": region,
                            "start_date": start_date,
                            "end_date": end_date,
                            "records_collected": len(data),
                            "frequency": freq,
                            "fuel_types": fuel_types if data_type == "generation" else None,
                            "generator_seed": self.generator.config.random_seed
                        },
                        source="Synthetic"
                    )
                else:
                    results[data_type] = self.create_error_result(
                        f"No synthetic {data_type} data generated",
                        "Synthetic"
                    )

            return results

        except Exception as e:
            logger.error(f"Error generating comprehensive synthetic data: {e}")
            error_result = self.create_error_result(
                f"Error generating comprehensive synthetic data: {str(e)}",
                "Synthetic"
            )
            return {
                "demand": error_result,
                "generation": error_result
            }
