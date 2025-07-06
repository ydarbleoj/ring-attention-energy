import asyncio
import logging
from typing import Dict, Any, Optional
import polars as pl
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

from .base import BaseEnergyDataCollector, DataCollectionResult
from ...integrations.eia.service.data_loader import DataLoader
from ...integrations.eia.client import EIAClient
from ..config import RetryConfig

logger = logging.getLogger(__name__)


class EIACollector(BaseEnergyDataCollector):
    """Collector for EIA energy data with retry logic."""

    def __init__(self, api_key: str, config: Dict[str, Any] = None):
        """Initialize EIA collector.

        Args:
            api_key: EIA API key
            config: Configuration dictionary
        """
        super().__init__("EIA", config)
        self.api_key = api_key
        self.retry_config = RetryConfig(**(config.get("retry", {}) if config else {}))

        # Initialize the data loader using your existing service
        self.data_loader = None
        self._setup_data_loader()

    def _setup_data_loader(self) -> None:
        """Set up the EIA data loader."""
        try:
            # Use your existing storage path structure
            storage_path = self.config.get("storage_path", "data/cache")
            timeout = self.config.get("timeout", 30)

            self.data_loader = DataLoader.create_with_storage(
                api_key=self.api_key,
                storage_path=storage_path,
                timeout=timeout
            )
            self.logger.info("EIA data loader initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize EIA data loader: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=60),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        reraise=True
    )
    async def collect_demand_data(
        self,
        start_date: str,
        end_date: str,
        region: str,
        **kwargs
    ) -> DataCollectionResult:
        """Collect electricity demand data from EIA.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            region: Region identifier (e.g., PACW, ERCO)
            **kwargs: Additional parameters

        Returns:
            DataCollectionResult with demand data
        """
        try:
            # Validate inputs
            self.validate_date_range(start_date, end_date)

            self.logger.info(f"Collecting EIA demand data for {region} from {start_date} to {end_date}")

            # Use your existing data loader
            save_to_storage = kwargs.get("save_to_storage", True)
            storage_filename = kwargs.get("storage_filename")
            storage_subfolder = kwargs.get("storage_subfolder", "demand")

            # Call the existing method (this is sync, so we'll wrap it)
            demand_df = await asyncio.get_event_loop().run_in_executor(
                None,
                self._collect_demand_sync,
                start_date,
                end_date,
                region,
                save_to_storage,
                storage_filename,
                storage_subfolder
            )

            # Create metadata
            metadata = {
                "source": "EIA",
                "data_type": "demand",
                "region": region,
                "start_date": start_date,
                "end_date": end_date,
                "api_key_used": self.api_key[:8] + "..." if self.api_key else None,
                "records_collected": len(demand_df),
                "columns": list(demand_df.columns)
            }

            self.logger.info(f"Successfully collected {len(demand_df)} demand records for {region}")

            return DataCollectionResult(
                data=demand_df,
                metadata=metadata,
                source="EIA"
            )

        except Exception as e:
            error_msg = f"Failed to collect EIA demand data for {region}: {str(e)}"
            self.logger.error(error_msg)
            return self.create_error_result(error_msg, "EIA")

    def _collect_demand_sync(
        self,
        start_date: str,
        end_date: str,
        region: str,
        save_to_storage: bool,
        storage_filename: Optional[str],
        storage_subfolder: Optional[str]
    ) -> pl.DataFrame:
        """Synchronous wrapper for demand data collection."""
        return self.data_loader.load_demand_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            save_to_storage=save_to_storage,
            storage_filename=storage_filename,
            storage_subfolder=storage_subfolder
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=60),
        retry=retry_if_exception_type((ConnectionError, TimeoutError, OSError)),
        reraise=True
    )
    async def collect_generation_data(
        self,
        start_date: str,
        end_date: str,
        region: str,
        **kwargs
    ) -> DataCollectionResult:
        """Collect electricity generation data from EIA.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            region: Region identifier (e.g., PACW, ERCO)
            **kwargs: Additional parameters

        Returns:
            DataCollectionResult with generation data
        """
        try:
            # Validate inputs
            self.validate_date_range(start_date, end_date)

            self.logger.info(f"Collecting EIA generation data for {region} from {start_date} to {end_date}")

            # Use your existing data loader
            save_to_storage = kwargs.get("save_to_storage", True)
            storage_filename = kwargs.get("storage_filename")
            storage_subfolder = kwargs.get("storage_subfolder", "generation")

            # Call the existing method (this is sync, so we'll wrap it)
            generation_df = await asyncio.get_event_loop().run_in_executor(
                None,
                self._collect_generation_sync,
                start_date,
                end_date,
                region,
                save_to_storage,
                storage_filename,
                storage_subfolder
            )

            # Create metadata
            metadata = {
                "source": "EIA",
                "data_type": "generation",
                "region": region,
                "start_date": start_date,
                "end_date": end_date,
                "api_key_used": self.api_key[:8] + "..." if self.api_key else None,
                "records_collected": len(generation_df),
                "columns": list(generation_df.columns)
            }

            self.logger.info(f"Successfully collected {len(generation_df)} generation records for {region}")

            return DataCollectionResult(
                data=generation_df,
                metadata=metadata,
                source="EIA"
            )

        except Exception as e:
            error_msg = f"Failed to collect EIA generation data for {region}: {str(e)}"
            self.logger.error(error_msg)
            return self.create_error_result(error_msg, "EIA")

    def _collect_generation_sync(
        self,
        start_date: str,
        end_date: str,
        region: str,
        save_to_storage: bool,
        storage_filename: Optional[str],
        storage_subfolder: Optional[str]
    ) -> pl.DataFrame:
        """Synchronous wrapper for generation data collection."""
        return self.data_loader.load_generation_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            save_to_storage=save_to_storage,
            storage_filename=storage_filename,
            storage_subfolder=storage_subfolder
        )

    async def collect_comprehensive_data(
        self,
        start_date: str,
        end_date: str,
        region: str,
        **kwargs
    ) -> Dict[str, DataCollectionResult]:
        """Collect both demand and generation data efficiently.

        This override uses your existing comprehensive data method for efficiency.
        """
        try:
            self.validate_date_range(start_date, end_date)

            self.logger.info(f"Collecting comprehensive EIA data for {region} from {start_date} to {end_date}")

            # Use your existing comprehensive method
            save_to_storage = kwargs.get("save_to_storage", True)
            storage_filename = kwargs.get("storage_filename")
            storage_subfolder = kwargs.get("storage_subfolder")

            # Call the existing comprehensive method
            data_dict = await asyncio.get_event_loop().run_in_executor(
                None,
                self._collect_comprehensive_sync,
                start_date,
                end_date,
                region,
                save_to_storage,
                storage_filename,
                storage_subfolder
            )

            # Convert to DataCollectionResult objects
            results = {}
            for data_type, df in data_dict.items():
                metadata = {
                    "source": "EIA",
                    "data_type": data_type,
                    "region": region,
                    "start_date": start_date,
                    "end_date": end_date,
                    "records_collected": len(df),
                    "columns": list(df.columns)
                }

                results[data_type] = DataCollectionResult(
                    data=df,
                    metadata=metadata,
                    source="EIA"
                )

            total_records = sum(len(result.data) for result in results.values())
            self.logger.info(f"Successfully collected {total_records} total records for {region}")

            return results

        except Exception as e:
            error_msg = f"Failed to collect comprehensive EIA data for {region}: {str(e)}"
            self.logger.error(error_msg)
            return {
                "demand": self.create_error_result(error_msg, "EIA"),
                "generation": self.create_error_result(error_msg, "EIA")
            }

    def _collect_comprehensive_sync(
        self,
        start_date: str,
        end_date: str,
        region: str,
        save_to_storage: bool,
        storage_filename: Optional[str],
        storage_subfolder: Optional[str]
    ) -> Dict[str, pl.DataFrame]:
        """Synchronous wrapper for comprehensive data collection."""
        return self.data_loader.load_comprehensive_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            save_to_storage=save_to_storage,
            storage_filename=storage_filename,
            storage_subfolder=storage_subfolder
        )

    def get_collector_info(self) -> Dict[str, Any]:
        """Get information about this EIA collector."""
        base_info = super().get_collector_info()
        base_info.update({
            "api_key_configured": bool(self.api_key),
            "data_loader_ready": self.data_loader is not None,
            "retry_config": {
                "max_retries": self.retry_config.max_retries,
                "initial_delay": self.retry_config.initial_delay,
                "max_delay": self.retry_config.max_delay
            }
        })
        return base_info
