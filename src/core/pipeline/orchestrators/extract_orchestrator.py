"""Extract stage orchestrator for the ETL pipeline with performance monitoring.

This orchestrator focuses on Phase 1: Extract stage
- Uses RawDataLoader to save raw JSON responses to data/raw/
- Measures RPS (requests per second) and latency
- Provides performance metrics and batch optimization
- Handles rate limiting and error recovery
"""

import asyncio
import time
from datetime import date
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from pathlib import Path

from .base import BaseOrchestrator, BatchConfig, BatchResult, PerformanceMetrics
from ...integrations.eia.client import EIAClient
from ...integrations.eia.service.raw_data_loader import RawDataLoader
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
        self.raw_loader = RawDataLoader(self.eia_client, self.raw_data_path)

        # Use extract-specific batch config
        self.batch_config = ExtractBatchConfig()

        self.logger.info(f"ExtractOrchestrator initialized with raw path: {self.raw_data_path}")

    async def process_data(
        self,
        start_date: date,
        end_date: date,
        region: str = "PACW",
        data_types: Optional[List[str]] = None
    ) -> Dict[str, List[ExtractBatchResult]]:
        """Extract Oregon energy data for the specified date range.

        Args:
            start_date: Start date for extraction
            end_date: End date for extraction
            region: Region code (default: PACW for Oregon)
            data_types: List of data types to extract (default: ["demand", "generation"])

        Returns:
            Dictionary mapping data types to their batch results
        """
        if data_types is None:
            data_types = ["demand", "generation"]

        self.logger.info(f"Starting Oregon data extraction: {start_date} to {end_date}")
        self.logger.info(f"Region: {region}, Data types: {data_types}")

        # Initialize performance tracking
        self._start_performance_tracking()

        try:
            # Generate date batches
            batches = self._generate_date_batches(start_date, end_date)
            self.logger.info(f"Generated {len(batches)} date batches ({self.batch_config.days_per_batch} days each)")

            # Extract data for each type
            results = {}
            for data_type in data_types:
                self.logger.info(f"Extracting {data_type} data...")
                results[data_type] = await self._extract_data_type_batches(
                    batches, region, data_type
                )

            self._end_performance_tracking()

            # Log performance summary
            additional_info = {
                "Region": region,
                "Data Types": ", ".join(data_types),
                "Date Range": f"{start_date} to {end_date}",
                "Total Batches": sum(len(batch_results) for batch_results in results.values()),
                "Raw Files Created": len(self.raw_loader.list_raw_files())
            }
            self._log_performance_summary("Extract", additional_info)

            return results

        except Exception as e:
            self._end_performance_tracking()
            self.logger.error(f"Extract orchestration failed: {e}")
            raise

    async def _extract_data_type_batches(
        self,
        batches: List[Tuple[date, date]],
        region: str,
        data_type: str
    ) -> List[ExtractBatchResult]:
        """Extract all batches for a specific data type."""

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.batch_config.max_concurrent_batches)

        async def extract_single_batch(start_date: date, end_date: date) -> ExtractBatchResult:
            async with semaphore:
                return await self._extract_single_batch(start_date, end_date, region, data_type)

        # Process all batches concurrently
        self.logger.info(f"Processing {len(batches)} {data_type} batches "
                        f"(max concurrent: {self.batch_config.max_concurrent_batches})")

        tasks = [extract_single_batch(start_date, end_date) for start_date, end_date in batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle exceptions and collect results
        batch_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                start_date, end_date = batches[i]
                self.logger.error(f"Batch {start_date} to {end_date} failed: {result}")
                self.metrics.error_messages.append(str(result))
                batch_results.append(ExtractBatchResult(
                    start_date=start_date,
                    end_date=end_date,
                    region=region,
                    operation_type="extract",
                    data_type=data_type,
                    success=False,
                    error_message=str(result)
                ))
            else:
                batch_results.append(result)

        return batch_results

    async def _extract_single_batch(
        self,
        start_date: date,
        end_date: date,
        region: str,
        data_type: str
    ) -> ExtractBatchResult:
        """Extract a single batch with performance monitoring."""

        batch_start_time = time.time()

        # Rate limiting
        await self._apply_rate_limiting()

        try:
            # Track operation
            self.metrics.total_operations += 1
            operation_start_time = time.time()

            # Perform extraction based on data type
            if data_type == "demand":
                file_path = self.raw_loader.extract_demand_data(region, start_date, end_date)
            elif data_type == "generation":
                file_path = self.raw_loader.extract_generation_data(region, start_date, end_date)
            else:
                raise ValueError(f"Unknown data type: {data_type}")

            operation_end_time = time.time()
            operation_latency = operation_end_time - operation_start_time

            # Update metrics
            self.metrics.successful_operations += 1
            self.metrics.operation_latencies.append(operation_latency)

            # Load file to get metadata
            raw_package = self.raw_loader.load_raw_file(file_path)
            metadata = raw_package["metadata"]

            records = metadata.get("record_count", 0)
            bytes_size = metadata.get("response_size_bytes", 0)

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

    def get_extraction_summary(self) -> Dict[str, any]:
        """Get summary of extracted data."""
        return self.raw_loader.get_extraction_summary()

    def list_extracted_files(self) -> List[Path]:
        """List all extracted raw files."""
        return self.raw_loader.list_raw_files()
