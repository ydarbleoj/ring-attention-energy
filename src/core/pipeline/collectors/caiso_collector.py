import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional, Union
from pathlib import Path

import polars as pl

from .base import BaseEnergyDataCollector, DataCollectionResult
from ..config import RetryConfig
from src.core.integrations.caiso.services.data_loader import DataLoader
from src.core.integrations.caiso.client import CAISOClient

logger = logging.getLogger(__name__)


class CAISOCollector(BaseEnergyDataCollector):
    """Collector for CAISO energy data with retry logic and comprehensive data collection."""

    def __init__(
        self,
        config: Dict[str, Any],
        client: Optional[CAISOClient] = None
    ):
        """Initialize CAISO collector.

        Args:
            config: Configuration dictionary including storage_path and retry settings
            client: Optional CAISO client instance
        """
        super().__init__("CAISO", config)
        self.client = client or CAISOClient()
        self.retry_config = RetryConfig(**(config.get("retry", {}) if config else {}))

        # Setup data loader
        try:
            self.data_loader = DataLoader.create_with_storage(
                storage_path=config["storage_path"],
                client=self.client
            )
        except Exception as e:
            logger.error(f"Failed to setup CAISO data loader: {e}")
            raise

    def get_collector_info(self) -> Dict[str, Any]:
        """Get collector information and status.

        Returns:
            Dictionary with collector information
        """
        base_info = super().get_collector_info()
        base_info.update({
            "client_configured": self.client is not None,
            "data_loader_ready": self.data_loader is not None,
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
        region: str = "CAISO",
        save_to_storage: bool = True,
        storage_filename: Optional[str] = None,
        storage_subfolder: Optional[str] = None
    ) -> pl.DataFrame:
        """Synchronous demand data collection (wrapped by async method).

        Args:
            start_date: Start date for data collection
            end_date: End date for data collection
            region: Region code (defaults to CAISO)
            save_to_storage: Whether to save to storage
            storage_filename: Custom filename for storage
            storage_subfolder: Subfolder for storage

        Returns:
            Polars DataFrame with demand data
        """
        return self.data_loader.load_demand_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            save_to_storage=save_to_storage,
            storage_filename=storage_filename,
            storage_subfolder=storage_subfolder
        )

    def _collect_generation_sync(
        self,
        start_date: str,
        end_date: str,
        region: str = "CAISO",
        save_to_storage: bool = True,
        storage_filename: Optional[str] = None,
        storage_subfolder: Optional[str] = None
    ) -> pl.DataFrame:
        """Synchronous generation data collection (wrapped by async method).

        Args:
            start_date: Start date for data collection
            end_date: End date for data collection
            region: Region code (defaults to CAISO)
            save_to_storage: Whether to save to storage
            storage_filename: Custom filename for storage
            storage_subfolder: Subfolder for storage

        Returns:
            Polars DataFrame with generation data
        """
        return self.data_loader.load_generation_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            save_to_storage=save_to_storage,
            storage_filename=storage_filename,
            storage_subfolder=storage_subfolder
        )

    def _collect_comprehensive_sync(
        self,
        start_date: str,
        end_date: str,
        region: str = "CAISO",
        save_to_storage: bool = True,
        storage_filename: Optional[str] = None,
        storage_subfolder: Optional[str] = None
    ) -> Dict[str, pl.DataFrame]:
        """Synchronous comprehensive data collection (wrapped by async method).

        Args:
            start_date: Start date for data collection
            end_date: End date for data collection
            region: Region code (defaults to CAISO)
            save_to_storage: Whether to save to storage
            storage_filename: Custom filename for storage
            storage_subfolder: Subfolder for storage

        Returns:
            Dictionary with demand and generation DataFrames
        """
        return self.data_loader.load_comprehensive_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            save_to_storage=save_to_storage,
            storage_filename=storage_filename,
            storage_subfolder=storage_subfolder
        )

    def _validate_date_range(self, start_date: str, end_date: str) -> bool:
        """Validate date range for data collection.

        Args:
            start_date: Start date string
            end_date: End date string

        Returns:
            True if valid, False otherwise
        """
        try:
            self.validate_date_range(start_date, end_date)
            return True
        except ValueError:
            return False

    async def collect_demand_data(
        self,
        start_date: str,
        end_date: str,
        region: str = "CAISO",
        **kwargs
    ) -> DataCollectionResult:
        """Collect demand data with retry logic.

        Args:
            start_date: Start date for data collection (YYYY-MM-DD format)
            end_date: End date for data collection (YYYY-MM-DD format)
            region: Region code (defaults to CAISO)
            **kwargs: Additional arguments for data loader

        Returns:
            DataCollectionResult with demand data
        """
        # Validate date range
        if not self._validate_date_range(start_date, end_date):
            return self.create_error_result(
                "Invalid date range: start_date must be <= end_date",
                "CAISO"
            )

        try:
            # Extract kwargs for sync method
            save_to_storage = kwargs.get("save_to_storage", True)
            storage_filename = kwargs.get("storage_filename")
            storage_subfolder = kwargs.get("storage_subfolder")

            # Execute in thread pool with retry logic
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(
                None,
                self._collect_demand_sync,
                start_date,
                end_date,
                region,
                save_to_storage,
                storage_filename,
                storage_subfolder
            )

            # Create successful result
            return DataCollectionResult(
                data=data,
                metadata={
                    "source": "CAISO",
                    "data_type": "demand",
                    "region": region,
                    "start_date": start_date,
                    "end_date": end_date,
                    "records_collected": len(data)
                },
                source="CAISO"
            )

        except Exception as e:
            logger.error(f"Error collecting demand data: {e}")
            return self.create_error_result(f"Error collecting demand data: {str(e)}", "CAISO")

    async def collect_generation_data(
        self,
        start_date: str,
        end_date: str,
        region: str = "CAISO",
        **kwargs
    ) -> DataCollectionResult:
        """Collect generation data with retry logic.

        Args:
            start_date: Start date for data collection (YYYY-MM-DD format)
            end_date: End date for data collection (YYYY-MM-DD format)
            region: Region code (defaults to CAISO)
            **kwargs: Additional arguments for data loader

        Returns:
            DataCollectionResult with generation data
        """
        # Validate date range
        if not self._validate_date_range(start_date, end_date):
            return self.create_error_result(
                "Invalid date range: start_date must be <= end_date",
                "CAISO"
            )

        try:
            # Extract kwargs for sync method
            save_to_storage = kwargs.get("save_to_storage", True)
            storage_filename = kwargs.get("storage_filename")
            storage_subfolder = kwargs.get("storage_subfolder")

            # Execute in thread pool with retry logic
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(
                None,
                self._collect_generation_sync,
                start_date,
                end_date,
                region,
                save_to_storage,
                storage_filename,
                storage_subfolder
            )

            # Create successful result
            return DataCollectionResult(
                data=data,
                metadata={
                    "source": "CAISO",
                    "data_type": "generation",
                    "region": region,
                    "start_date": start_date,
                    "end_date": end_date,
                    "records_collected": len(data)
                },
                source="CAISO"
            )

        except Exception as e:
            logger.error(f"Error collecting generation data: {e}")
            return self.create_error_result(f"Error collecting generation data: {str(e)}", "CAISO")

    async def collect_comprehensive_data(
        self,
        start_date: str,
        end_date: str,
        region: str = "CAISO",
        **kwargs
    ) -> Dict[str, DataCollectionResult]:
        """Collect comprehensive data (demand and generation) with retry logic.

        Args:
            start_date: Start date for data collection (YYYY-MM-DD format)
            end_date: End date for data collection (YYYY-MM-DD format)
            region: Region code (defaults to CAISO)
            **kwargs: Additional arguments for data loader

        Returns:
            Dictionary with demand and generation DataCollectionResults
        """
        # Validate date range
        if not self._validate_date_range(start_date, end_date):
            error_result = self.create_error_result(
                "Invalid date range: start_date must be <= end_date",
                "CAISO"
            )
            return {
                "demand": error_result,
                "generation": error_result
            }

        try:
            # Extract kwargs for sync method
            save_to_storage = kwargs.get("save_to_storage", True)
            storage_filename = kwargs.get("storage_filename")
            storage_subfolder = kwargs.get("storage_subfolder")

            # Execute in thread pool with retry logic
            loop = asyncio.get_event_loop()
            comprehensive_data = await loop.run_in_executor(
                None,
                self._collect_comprehensive_sync,
                start_date,
                end_date,
                region,
                save_to_storage,
                storage_filename,
                storage_subfolder
            )

            # Create results for each data type
            results = {}

            for data_type in ["demand", "generation"]:
                if data_type in comprehensive_data:
                    data = comprehensive_data[data_type]
                    results[data_type] = DataCollectionResult(
                        data=data,
                        metadata={
                            "source": "CAISO",
                            "data_type": data_type,
                            "region": region,
                            "start_date": start_date,
                            "end_date": end_date,
                            "records_collected": len(data)
                        },
                        source="CAISO"
                    )
                else:
                    results[data_type] = self.create_error_result(
                        f"No {data_type} data returned",
                        "CAISO"
                    )

            return results

        except Exception as e:
            logger.error(f"Error collecting comprehensive data: {e}")
            error_result = self.create_error_result(
                f"Error collecting comprehensive data: {str(e)}",
                "CAISO"
            )
            return {
                "demand": error_result,
                "generation": error_result
            }
