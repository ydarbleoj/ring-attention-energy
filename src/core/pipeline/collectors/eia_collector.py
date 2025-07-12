import asyncio
import logging
import time
import concurrent.futures
from typing import Dict, Any, Optional, List
from datetime import date
from pathlib import Path
import polars as pl
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

from .base import BaseEnergyDataCollector, DataCollectionResult
from ...integrations.eia.service.data_loader import DataLoader
from ...integrations.eia.services import EIADataService
from ...integrations.eia.service.raw_data_loader import RawDataLoader, RawDataMetadata
from ..config import RetryConfig

logger = logging.getLogger(__name__)


class EIACollector(BaseEnergyDataCollector):
    """Collector for EIA energy data with retry logic and batch processing."""

    def __init__(self, api_key: str, config: Dict[str, Any] = None, raw_data_path: str = "data/raw"):
        """Initialize EIA collector.

        Args:
            api_key: EIA API key
            config: Configuration dictionary
            raw_data_path: Path for storing raw JSON files
        """
        super().__init__("EIA", config)
        self.api_key = api_key
        self.retry_config = RetryConfig(**(config.get("retry", {}) if config else {}))

        # Initialize both the processed data loader and raw data loader
        self.data_loader = None
        self._setup_data_loader()

        # Initialize raw data loader for batch operations
        self.raw_data_path = Path(raw_data_path)
        self.eia_service = EIADataService(api_key=api_key, config=config)
        self.raw_loader = RawDataLoader(self.raw_data_path)

        self.logger.info(f"EIACollector initialized with raw path: {self.raw_data_path}")

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

    async def collect_demand_data_batch(
        self,
        start_date: str,
        end_date: str,
        regions: List[str],
        **kwargs
    ) -> Dict[str, DataCollectionResult]:
        """Collect demand data for multiple regions in parallel.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            regions: List of region identifiers (e.g., PACW, ERCO)
            **kwargs: Additional parameters

        Returns:
            Dictionary mapping region to DataCollectionResult with demand data
        """
        results = {}

        def collect_for_region(region):
            """Collect demand data for a single region."""
            return asyncio.run(
                self.collect_demand_data(
                    start_date=start_date,
                    end_date=end_date,
                    region=region,
                    **kwargs
                )
            )

        # Use ThreadPoolExecutor to collect data in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_region = {executor.submit(collect_for_region, region): region for region in regions}
            for future in concurrent.futures.as_completed(future_to_region):
                region = future_to_region[future]
                try:
                    result = future.result()
                    results[region] = result
                    self.logger.info(f"Successfully collected demand data for {region}")
                except Exception as e:
                    error_msg = f"Failed to collect demand data for {region}: {str(e)}"
                    self.logger.error(error_msg)
                    results[region] = self.create_error_result(error_msg, "EIA")

        return results

    async def collect_generation_data_batch(
        self,
        start_date: str,
        end_date: str,
        regions: List[str],
        **kwargs
    ) -> Dict[str, DataCollectionResult]:
        """Collect generation data for multiple regions in parallel.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            regions: List of region identifiers (e.g., PACW, ERCO)
            **kwargs: Additional parameters

        Returns:
            Dictionary mapping region to DataCollectionResult with generation data
        """
        results = {}

        def collect_for_region(region):
            """Collect generation data for a single region."""
            return asyncio.run(
                self.collect_generation_data(
                    start_date=start_date,
                    end_date=end_date,
                    region=region,
                    **kwargs
                )
            )

        # Use ThreadPoolExecutor to collect data in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_region = {executor.submit(collect_for_region, region): region for region in regions}
            for future in concurrent.futures.as_completed(future_to_region):
                region = future_to_region[future]
                try:
                    result = future.result()
                    results[region] = result
                    self.logger.info(f"Successfully collected generation data for {region}")
                except Exception as e:
                    error_msg = f"Failed to collect generation data for {region}: {str(e)}"
                    self.logger.error(error_msg)
                    results[region] = self.create_error_result(error_msg, "EIA")

        return results

    async def collect_comprehensive_data_batch(
        self,
        start_date: str,
        end_date: str,
        regions: List[str],
        **kwargs
    ) -> Dict[str, Dict[str, DataCollectionResult]]:
        """Collect comprehensive data (demand and generation) for multiple regions in parallel.

        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            regions: List of region identifiers (e.g., PACW, ERCO)
            **kwargs: Additional parameters

        Returns:
            Dictionary mapping region to another dictionary with DataCollectionResult for demand and generation data
        """
        results = {}

        def collect_for_region(region):
            """Collect comprehensive data for a single region."""
            return asyncio.run(
                self.collect_comprehensive_data(
                    start_date=start_date,
                    end_date=end_date,
                    region=region,
                    **kwargs
                )
            )

        # Use ThreadPoolExecutor to collect data in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_region = {executor.submit(collect_for_region, region): region for region in regions}
            for future in concurrent.futures.as_completed(future_to_region):
                region = future_to_region[future]
                try:
                    result = future.result()
                    results[region] = result
                    self.logger.info(f"Successfully collected comprehensive data for {region}")
                except Exception as e:
                    error_msg = f"Failed to collect comprehensive data for {region}: {str(e)}"
                    self.logger.error(error_msg)
                    results[region] = {
                        "demand": self.create_error_result(error_msg, "EIA"),
                        "generation": self.create_error_result(error_msg, "EIA")
                    }

        return results

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

    def collect_batch_data_sync(
        self,
        data_type: str,
        start_date: date,
        end_date: date,
        regions: List[str],
        max_workers: int = 5,
        delay_between_operations: float = 0.2
    ) -> Dict[str, Any]:
        """
        Collect raw data for all regions in a SINGLE API call for a specific data type and date range.

        This method now implements TRUE multi-region collection - one API call gets data for ALL regions,
        instead of separate API calls per region.

        Args:
            data_type: Either "demand" or "generation"
            start_date: Start date for the batch
            end_date: End date for the batch
            regions: List of regions to collect data for
            max_workers: Not used anymore (kept for compatibility)
            delay_between_operations: Rate limiting delay

        Returns:
            Dict with results including file_path (single file with all regions), success, etc.
        """
        self.logger.info(f"🚀 Starting TRUE multi-region batch collection for {data_type} data")
        self.logger.info(f"   Date range: {start_date} to {end_date}")
        self.logger.info(f"   Regions: {regions} (ALL in single API call)")

        batch_start_time = time.time()

        try:
            # Rate limiting
            time.sleep(delay_between_operations)

            # Convert dates to strings
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")

            # Make SINGLE API call for ALL regions using the new service
            api_response = self.eia_service.get_raw_data(
                data_type=data_type,
                regions=regions,
                start_date=start_str,
                end_date=end_str
            )

            # Create metadata for the consolidated file
            metadata = RawDataMetadata(
                timestamp=time.time(),
                region="_".join(regions),  # Multi-region identifier
                data_type=data_type,
                start_date=start_str,
                end_date=end_str,
                api_endpoint=f"/electricity/rto/{'fuel-type' if data_type == 'generation' else 'region'}-data",
                success=True
            )

            # Save raw data as SINGLE file with ALL regions
            file_path = self.raw_loader.save_raw_data(api_response, metadata)

            # Get metadata from saved file
            raw_package = self.raw_loader.load_raw_file(file_path)
            saved_metadata = raw_package["metadata"]

            records = saved_metadata.get("record_count", 0)
            bytes_size = saved_metadata.get("response_size_bytes", 0)

            batch_duration = time.time() - batch_start_time

            self.logger.info(f"✅ Multi-region batch: {records:,} records, {bytes_size:,} bytes in {batch_duration:.1f}s")
            self.logger.info(f"   📁 Single file: {file_path.name}")

            # Prepare results
            results = {
                'success': True,
                'data_type': data_type,
                'start_date': start_date,
                'end_date': end_date,
                'regions_processed': regions,
                'file_paths': [file_path],  # Single file with all regions
                'files_created': 1,
                'errors': [],
                'error_count': 0,
                'batch_duration_seconds': batch_duration,
                'total_records': records,
                'total_bytes': bytes_size,
                'api_calls': 1,  # Only one API call for all regions!
                'collection_strategy': 'true_multi_region'
            }

            self.logger.info(f"✅ TRUE multi-region collection completed: 1 file, 1 API call, {records:,} records")
            return results

        except Exception as e:
            batch_duration = time.time() - batch_start_time
            error_msg = f"Failed to collect {data_type} data for regions {regions}: {str(e)}"
            self.logger.error(f"❌ {error_msg}")

            return {
                'success': False,
                'data_type': data_type,
                'start_date': start_date,
                'end_date': end_date,
                'regions_processed': regions,
                'file_paths': [],
                'files_created': 0,
                'errors': [error_msg],
                'error_count': 1,
                'batch_duration_seconds': batch_duration,
                'collection_strategy': 'true_multi_region'
            }
