"""Extract stage orchestrator for the ETL pipeline with performance monitoring.

This orchestrator focuses on Phase 1: Extract stage
- Uses EIAClient for API calls and RawDataLoader to save raw JSON responses to data/raw/
- Uses ThreadPoolExecutor for region-based parallelism
- Measures RPS (requests per second) and latency
- Provides performance metrics and batch optimization
- Handles rate limiting and error recovery
"""

import time
import concurrent.futures
from datetime import date, datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from pathlib import Path

from .base import BaseOrchestrator, BatchConfig, BatchResult, PerformanceMetrics
from ...integrations.eia.client import EIAClient
from ...integrations.eia.service.raw_data_loader import RawDataLoader, RawDataMetadata
from ...integrations.config import get_config


@dataclass
class ExtractBatchConfig(BatchConfig):
    """Configuration for extract batch operations."""

    # Override defaults for extract operations
    days_per_batch: int = 7  # Start with 1 week batches
    max_concurrent_batches: int = 3  # Conservative concurrency

    # Rate limiting specific to EIA API
    delay_between_operations: float = 0.2  # 200ms between requests
    max_operations_per_second: float = 5.0  # EIA API limit

    # Extract-specific settings
    adaptive_batch_sizing: bool = True  # Adjust batch size based on performance


@dataclass
class ExtractBatchResult(BatchResult):
    """Result of a single batch extract operation."""

    data_type: str = "unknown"  # "demand" or "generation"
    file_path: Optional[Path] = None


class ExtractOrchestrator(BaseOrchestrator):
    """Orchestrator for the Extract stage of the ETL pipeline."""

    def __init__(self, raw_data_path: str = "data/raw", config_file: Optional[str] = None):
        """Initialize the extract orchestrator.

        Args:
            raw_data_path: Path to store raw data files
            config_file: Optional config file path (unused for now)
        """
        config = get_config()
        super().__init__(config)

        self.raw_data_path = Path(raw_data_path)

        # Initialize EIA components
        self.eia_client = EIAClient(config=self.config)
        self.raw_loader = RawDataLoader(self.raw_data_path)

        # Use extract-specific batch config
        self.batch_config = ExtractBatchConfig()

        self.logger.info(f"ExtractOrchestrator initialized with raw path: {self.raw_data_path}")

    def extract_historical_data(
        self,
        start_date: date,
        end_date: date,
        regions: List[str] = None,
        data_types: List[str] = None,
        max_workers: int = 5,
        batch_days: int = 45
    ) -> Dict[str, Any]:
        """
        Extract historical energy data using ThreadPoolExecutor for optimal performance.

        This is the main method to use for batch historical data extraction.
        Uses region-based parallelism with ThreadPoolExecutor based on performance testing.

        Args:
            start_date: Start date for extraction
            end_date: End date for extraction
            regions: List of regions (default: ['PACW', 'ERCO', 'CAL', 'TEX', 'MISO'])
            data_types: List of data types (default: ['demand', 'generation'])
            max_workers: Number of concurrent workers (default: 5)
            batch_days: Days per batch (default: 45 for optimal performance)

        Returns:
            Dict with extraction results and performance metrics
        """
        if regions is None:
            regions = ['PACW', 'ERCO', 'CAL', 'TEX', 'MISO']

        if data_types is None:
            data_types = ['demand', 'generation']

        self.logger.info(f"🚀 Starting historical data extraction")
        self.logger.info(f"   Date range: {start_date} to {end_date}")
        self.logger.info(f"   Regions: {regions}")
        self.logger.info(f"   Data types: {data_types}")
        self.logger.info(f"   Batch size: {batch_days} days")
        self.logger.info(f"   Max workers: {max_workers}")

        # Start performance tracking
        self._start_performance_tracking()
        extraction_start_time = time.time()

        try:
            # Generate all extraction tasks
            extraction_tasks = []
            for region in regions:
                for data_type in data_types:
                    batches = self._generate_date_batches(start_date, end_date, batch_days)
                    for batch_start, batch_end in batches:
                        extraction_tasks.append((region, data_type, batch_start, batch_end))

            self.logger.info(f"Generated {len(extraction_tasks)} extraction tasks")

            # Execute all tasks with ThreadPoolExecutor
            file_paths = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks
                future_to_task = {
                    executor.submit(self._extract_single_task, task): task
                    for task in extraction_tasks
                }

                # Collect results as they complete
                completed_tasks = 0
                for future in concurrent.futures.as_completed(future_to_task):
                    task = future_to_task[future]
                    completed_tasks += 1

                    try:
                        result = future.result()
                        if result.success and result.file_path:
                            file_paths.append(result.file_path)

                        # Log progress every 10% completion
                        if completed_tasks % max(1, len(extraction_tasks) // 10) == 0:
                            progress = (completed_tasks / len(extraction_tasks)) * 100
                            self.logger.info(f"Progress: {progress:.1f}% ({completed_tasks}/{len(extraction_tasks)} tasks)")

                    except Exception as e:
                        region, data_type, batch_start, batch_end = task
                        self.logger.error(f"Task failed: {region} {data_type} {batch_start}-{batch_end}: {e}")

            extraction_end_time = time.time()
            total_duration = extraction_end_time - extraction_start_time

            # Calculate metrics
            total_files = len(file_paths)
            estimated_records = total_files * 2300  # Average records per API call
            actual_rps = estimated_records / total_duration if total_duration > 0 else 0

            # Stop performance tracking
            self._end_performance_tracking()

            # Prepare results summary
            results = {
                'success': True,
                'total_files_created': total_files,
                'file_paths': file_paths,
                'estimated_total_records': estimated_records,
                'extraction_duration_seconds': total_duration,
                'extraction_duration_minutes': total_duration / 60,
                'estimated_rps': actual_rps,
                'regions_processed': regions,
                'data_types_processed': data_types
            }

            # Log completion summary
            self.logger.info(f"✅ Historical extraction completed successfully!")
            self.logger.info(f"   📁 Files created: {total_files}")
            self.logger.info(f"   📊 Estimated records: {estimated_records:,}")
            self.logger.info(f"   ⏱️  Duration: {total_duration:.1f} seconds ({total_duration/60:.1f} minutes)")
            self.logger.info(f"   🚀 Estimated RPS: {actual_rps:.1f}")

            return results

        except Exception as e:
            self._end_performance_tracking()
            self.logger.error(f"❌ Historical extraction failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'extraction_duration_seconds': time.time() - extraction_start_time
            }

    def _extract_single_batch(
        self,
        start_date: date,
        end_date: date,
        region: str,
        data_type: str
    ) -> ExtractBatchResult:
        """Extract a single batch with performance monitoring."""

        batch_start_time = time.time()

        # Rate limiting
        self._apply_rate_limiting_sync()

        try:
            # Track operation
            self.metrics.total_operations += 1
            operation_start_time = time.time()

            # Convert dates to strings
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")

            # Get raw API response
            if data_type == "demand":
                api_response = self.eia_client.get_demand_data_raw(region, start_str, end_str)
                api_endpoint = "/electricity/rto/region-data"
            elif data_type == "generation":
                api_response = self.eia_client.get_generation_data_raw(region, start_str, end_str)
                api_endpoint = "/electricity/rto/fuel-type-data"
            else:
                raise ValueError(f"Unknown data type: {data_type}")

            operation_end_time = time.time()
            operation_latency = operation_end_time - operation_start_time

            # Create metadata
            metadata = RawDataMetadata(
                timestamp=datetime.now().isoformat(),
                region=region,
                data_type=data_type,
                start_date=start_str,
                end_date=end_str,
                api_endpoint=api_endpoint,
                success=True
            )

            # Save raw data using RawDataLoader
            file_path = self.raw_loader.save_raw_data(api_response, metadata)

            # Update metrics
            self.metrics.successful_operations += 1
            self.metrics.operation_latencies.append(operation_latency)

            # Get records and size from metadata (auto-calculated by save_raw_data)
            raw_package = self.raw_loader.load_raw_file(file_path)
            saved_metadata = raw_package["metadata"]

            records = saved_metadata.get("record_count", 0)
            bytes_size = saved_metadata.get("response_size_bytes", 0)

            self.metrics.total_records_processed += records
            self.metrics.total_bytes_processed += bytes_size

            batch_duration = time.time() - batch_start_time

            if self.batch_config.log_individual_operations:
                self.logger.info(f"✅ {data_type} batch {start_date} to {end_date}: "
                               f"{records:,} records, {bytes_size:,} bytes, "
                               f"{operation_latency:.2f}s latency")

            return ExtractBatchResult(
                start_date=start_date,
                end_date=end_date,
                region=region,
                operation_type="extract",
                data_type=data_type,
                success=True,
                file_path=file_path,
                records_processed=records,
                bytes_processed=bytes_size,
                duration_seconds=batch_duration,
                operation_latency_seconds=operation_latency,
                output_path=file_path
            )

        except Exception as e:
            # Track failure
            self.metrics.failed_operations += 1
            batch_duration = time.time() - batch_start_time

            self.logger.error(f"❌ {data_type} batch {start_date} to {end_date} failed: {e}")

            return ExtractBatchResult(
                start_date=start_date,
                end_date=end_date,
                region=region,
                operation_type="extract",
                data_type=data_type,
                success=False,
                duration_seconds=batch_duration,
                error_message=str(e)
            )

    def _apply_rate_limiting_sync(self):
        """Apply rate limiting (synchronous version)."""
        if hasattr(self, '_last_request_time'):
            elapsed = time.time() - self._last_request_time
            if elapsed < self.batch_config.delay_between_operations:
                sleep_time = self.batch_config.delay_between_operations - elapsed
                time.sleep(sleep_time)

        self._last_request_time = time.time()

    def _generate_date_batches(self, start_date: date, end_date: date, batch_days: int = None) -> List[Tuple[date, date]]:
        """Generate date batches for processing.

        Args:
            start_date: Start date
            end_date: End date
            batch_days: Days per batch (default: use config)

        Returns:
            List of (start_date, end_date) tuples
        """
        if batch_days is None:
            batch_days = self.batch_config.days_per_batch

        batches = []
        current_date = start_date

        while current_date <= end_date:
            batch_end = date.fromordinal(min(
                current_date.toordinal() + batch_days - 1,
                end_date.toordinal()
            ))
            batches.append((current_date, batch_end))
            current_date = date.fromordinal(batch_end.toordinal() + 1)

        return batches

    def extract_latest_data(
        self,
        regions: List[str] = None,
        data_types: List[str] = None,
        days_back: int = 2
    ) -> Dict[str, Any]:
        """
        Extract the most recent data for specified regions.

        This is the main method to use for getting up-to-date data.

        Args:
            regions: List of regions (default: ['PACW', 'ERCO', 'CAL', 'TEX', 'MISO'])
            data_types: List of data types (default: ['demand', 'generation'])
            days_back: How many days back to fetch (default: 2)

        Returns:
            Dict with extraction results
        """
        if regions is None:
            regions = ['PACW', 'ERCO', 'CAL', 'TEX', 'MISO']

        if data_types is None:
            data_types = ['demand', 'generation']

        # Calculate date range (last few days)
        today = date.today()
        start_date = date.fromordinal(today.toordinal() - days_back)

        self.logger.info(f"🔄 Extracting latest data for {len(regions)} regions")
        self.logger.info(f"   Date range: {start_date} to {today}")

        # Use the main historical extraction method with fewer workers for latest data
        return self.extract_historical_data(
            start_date=start_date,
            end_date=today,
            regions=regions,
            data_types=data_types,
            max_workers=len(regions),  # One worker per region
            batch_days=1  # Daily batches for latest data
        )

    # Required abstract method from base class
    async def process_data(self, start_date: date, end_date: date, **kwargs) -> Dict[str, Any]:
        """
        Legacy method for compatibility with base class.

        Use extract_historical_data() or extract_latest_data() instead.
        """
        region = kwargs.get('region', 'PACW')
        data_types = kwargs.get('data_types', ['demand', 'generation'])

        # Convert to non-async call
        return self.extract_historical_data(
            start_date=start_date,
            end_date=end_date,
            regions=[region],
            data_types=data_types
        )

    # Main public methods - use these!

    # Helper methods

    def get_extraction_summary(self) -> Dict[str, any]:
        """Get summary of extracted data."""
        return self.raw_loader.get_extraction_summary()

    def list_extracted_files(self) -> List[Path]:
        """List all extracted raw files."""
        return self.raw_loader.list_raw_files()

    def _extract_single_task(self, task_data) -> ExtractBatchResult:
        """Extract a single task (region, data_type, batch_start, batch_end)."""
        region, data_type, batch_start, batch_end = task_data
        return self._extract_single_batch(batch_start, batch_end, region, data_type)
