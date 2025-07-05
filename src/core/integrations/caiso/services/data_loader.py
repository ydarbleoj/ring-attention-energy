"""Data loader for CAISO data with Polars integration."""

import polars as pl
import pandas as pd
from typing import List, Optional, Union, Dict, Any
from datetime import datetime, date
import logging
from pathlib import Path

from .storage import StorageManager
from ..client import CAISOClient

logger = logging.getLogger(__name__)


class DataLoader:
    """Loads CAISO data and converts to Polars DataFrames with storage capabilities."""

    def __init__(self, storage_manager: StorageManager, client: Optional[CAISOClient] = None):
        """Initialize DataLoader.

        Args:
            storage_manager: Storage manager for Parquet operations
            client: CAISO API client (optional, will create one if not provided)
        """
        self.storage = storage_manager
        self.client = client or CAISOClient()

    @classmethod
    def create_with_storage(
        cls,
        storage_path: Union[str, Path],
        client: Optional[CAISOClient] = None
    ) -> "DataLoader":
        """Create DataLoader with storage manager.

        Args:
            storage_path: Base path for data storage
            client: CAISO API client (optional, will create one if not provided)

        Returns:
            Configured DataLoader instance
        """
        storage = StorageManager(base_path=storage_path)
        return cls(storage, client)

    def load_demand_data(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
        region: str = "CAISO",
        save_to_storage: bool = True,
        storage_filename: Optional[str] = None,
        storage_subfolder: Optional[str] = None
    ) -> pl.DataFrame:
        """Load demand data and convert to Polars DataFrame.

        Args:
            start_date: Start date for data
            end_date: End date for data
            region: Region code (e.g., CAISO)
            save_to_storage: Whether to save to storage
            storage_filename: Custom filename for storage
            storage_subfolder: Subfolder for storage

        Returns:
            Polars DataFrame with demand data
        """
        # Convert dates to strings if needed
        if isinstance(start_date, date):
            start_date = start_date.strftime("%Y%m%d")
        if isinstance(end_date, date):
            end_date = end_date.strftime("%Y%m%d")

        # Get data from CAISO API
        try:
            # Use validate=False to get raw DataFrame
            caiso_data = self.client.get_real_time_demand(
                start_date=start_date,
                end_date=end_date,
                validate=False
            )

            # Convert to Polars DataFrame with expected schema
            if not caiso_data.empty:
                # Ensure expected columns exist
                if 'timestamp' in caiso_data.columns:
                    caiso_data = caiso_data.rename(columns={'timestamp': 'datetime'})
                if 'demand' in caiso_data.columns:
                    caiso_data = caiso_data.rename(columns={'demand': 'demand_mwh'})

                # Add region column if not present
                if 'region' not in caiso_data.columns:
                    caiso_data['region'] = region

                # Convert to Polars
                df = pl.from_pandas(caiso_data[['datetime', 'region', 'demand_mwh']])
            else:
                # Create empty DataFrame with proper schema
                df = pl.DataFrame(schema={
                    "datetime": pl.Datetime,
                    "region": pl.Utf8,
                    "demand_mwh": pl.Float64
                })
        except Exception as e:
            logger.error(f"Error fetching CAISO demand data: {e}")
            # Create empty DataFrame with proper schema
            df = pl.DataFrame(schema={
                "datetime": pl.Datetime,
                "region": pl.Utf8,
                "demand_mwh": pl.Float64
            })

        # Save to storage if requested
        if save_to_storage:
            if not storage_filename:
                storage_filename = f"demand_{region}_{start_date}_to_{end_date}"

            # Only save if DataFrame has actual data rows
            if not df.is_empty():
                self.storage.save_dataframe(
                    df=df,
                    filename=storage_filename,
                    subfolder=storage_subfolder,
                    overwrite=True
                )
            else:
                logger.warning(f"Skipping save of empty DataFrame for {storage_filename}")

        logger.info(f"Loaded {len(df)} CAISO demand records for {region} from {start_date} to {end_date}")
        return df

    def load_generation_data(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
        region: str = "CAISO",
        save_to_storage: bool = True,
        storage_filename: Optional[str] = None,
        storage_subfolder: Optional[str] = None
    ) -> pl.DataFrame:
        """Load generation data and convert to Polars DataFrame.

        Args:
            start_date: Start date for data
            end_date: End date for data
            region: Region code (e.g., CAISO)
            save_to_storage: Whether to save to storage
            storage_filename: Custom filename for storage
            storage_subfolder: Subfolder for storage

        Returns:
            Polars DataFrame with generation data
        """
        # Convert dates to strings if needed
        if isinstance(start_date, date):
            start_date = start_date.strftime("%Y%m%d")
        if isinstance(end_date, date):
            end_date = end_date.strftime("%Y%m%d")

        # Get data from CAISO API
        try:
            # Use validate=False to get raw DataFrame
            caiso_data = self.client.get_generation_mix(
                start_date=start_date,
                end_date=end_date,
                validate=False
            )

            # Convert to Polars DataFrame with expected schema
            if not caiso_data.empty:
                # Ensure expected columns exist
                if 'timestamp' in caiso_data.columns:
                    caiso_data = caiso_data.rename(columns={'timestamp': 'datetime'})

                # Add region column if not present
                if 'region' not in caiso_data.columns:
                    caiso_data['region'] = region

                # Convert to Polars - keep all generation columns
                df = pl.from_pandas(caiso_data)
            else:
                # Create empty DataFrame with proper schema
                df = pl.DataFrame(schema={
                    "datetime": pl.Datetime,
                    "region": pl.Utf8
                })
        except Exception as e:
            logger.error(f"Error fetching CAISO generation data: {e}")
            # Create empty DataFrame with proper schema
            df = pl.DataFrame(schema={
                "datetime": pl.Datetime,
                "region": pl.Utf8
            })

        # Save to storage if requested
        if save_to_storage:
            if not storage_filename:
                storage_filename = f"generation_{region}_{start_date}_to_{end_date}"

            # Only save if DataFrame has actual data rows
            if not df.is_empty():
                self.storage.save_dataframe(
                    df=df,
                    filename=storage_filename,
                    subfolder=storage_subfolder,
                    overwrite=True
                )
            else:
                logger.warning(f"Skipping save of empty DataFrame for {storage_filename}")

        logger.info(f"Loaded {len(df)} CAISO generation records for {region} from {start_date} to {end_date}")
        return df

    def load_comprehensive_data(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
        region: str = "CAISO",
        save_to_storage: bool = True,
        storage_filename: Optional[str] = None,
        storage_subfolder: Optional[str] = None
    ) -> Dict[str, pl.DataFrame]:
        """Load both demand and generation data.

        Args:
            start_date: Start date for data
            end_date: End date for data
            region: Region code (e.g., CAISO)
            save_to_storage: Whether to save to storage
            storage_filename: Base filename for storage (will be suffixed)
            storage_subfolder: Subfolder for storage

        Returns:
            Dictionary with 'demand' and 'generation' DataFrames
        """
        # Load demand data
        demand_df = self.load_demand_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            save_to_storage=save_to_storage,
            storage_filename=f"{storage_filename}_demand" if storage_filename else None,
            storage_subfolder=storage_subfolder
        )

        # Load generation data
        generation_df = self.load_generation_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            save_to_storage=save_to_storage,
            storage_filename=f"{storage_filename}_generation" if storage_filename else None,
            storage_subfolder=storage_subfolder
        )

        return {
            "demand": demand_df,
            "generation": generation_df
        }

    def load_from_storage(
        self,
        filename: str,
        subfolder: Optional[str] = None
    ) -> pl.DataFrame:
        """Load DataFrame from storage.

        Args:
            filename: Name of file to load
            subfolder: Optional subfolder

        Returns:
            Loaded Polars DataFrame
        """
        return self.storage.load_dataframe(filename, subfolder)

    def join_demand_generation(
        self,
        demand_df: pl.DataFrame,
        generation_df: pl.DataFrame,
        join_on: str = "datetime",
        save_to_storage: bool = True,
        storage_filename: Optional[str] = None,
        storage_subfolder: Optional[str] = None
    ) -> pl.DataFrame:
        """Join demand and generation DataFrames.

        Args:
            demand_df: Demand DataFrame
            generation_df: Generation DataFrame
            join_on: Column to join on
            save_to_storage: Whether to save to storage
            storage_filename: Custom filename for storage
            storage_subfolder: Subfolder for storage

        Returns:
            Joined Polars DataFrame
        """
        # Rename columns to avoid conflicts
        demand_df = demand_df.with_columns([
            pl.col(col).alias(f"demand_{col}" if col != join_on else col)
            for col in demand_df.columns
        ])

        generation_df = generation_df.with_columns([
            pl.col(col).alias(f"generation_{col}" if col != join_on else col)
            for col in generation_df.columns
        ])

        # Join DataFrames
        joined_df = demand_df.join(generation_df, on=join_on, how="full")

        # Save to storage if requested
        if save_to_storage:
            if not storage_filename:
                storage_filename = f"joined_demand_generation_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

            self.storage.save_dataframe(
                df=joined_df,
                filename=storage_filename,
                subfolder=storage_subfolder,
                overwrite=True
            )

        logger.info(f"Joined CAISO DataFrames resulting in {len(joined_df)} records")
        return joined_df

    def get_storage_info(self) -> Dict[str, Any]:
        """Get information about stored files.

        Returns:
            Dictionary with storage information
        """
        files = self.storage.list_files()
        info = {
            "total_files": len(files),
            "files": []
        }

        for filename in files:
            try:
                file_info = self.storage.get_file_info(filename)
                info["files"].append(file_info)
            except FileNotFoundError:
                logger.warning(f"CAISO file {filename} not found during info gathering")

        return info