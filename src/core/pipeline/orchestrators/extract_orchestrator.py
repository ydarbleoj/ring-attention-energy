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
from ..collectors.eia_collector import EIACollector


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

        # Initialize EIA components - now using EIACollector
        self.eia_collector = EIACollector(
            api_key=config.api.eia_api_key or "",
            config={
                "eia_api_key": config.api.eia_api_key,
                "eia_base_url": config.api.eia_base_url,
                "eia_rate_limit_delay": config.api.eia_rate_limit_delay
            },
            raw_data_path=str(self.raw_data_path)
        )

        # Use extract-specific batch config
        self.batch_config = ExtractBatchConfig()

        self.logger.info(f"ExtractOrchestrator initialized with raw path: {self.raw_data_path}")
        self.logger.info("Using EIACollector for parallel batch processing")

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
        Extract historical energy data using time-based batching with EIACollector.

        This is the main method to use for batch historical data extraction.
        Now uses time-based batching instead of region-based batching - each batch
        collects data for ALL regions for a specific data_type and date range.

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

        print(f"🚀 Starting historical data extraction with time-based batching")
        print(f"   Date range: {start_date} to {end_date}")
        print(f"   Regions: {regions}")
        print(f"   Data types: {data_types}")
        print(f"   Batch size: {batch_days} days")
        print(f"   Region workers per batch: {max_workers}")

        # Start performance tracking
        self._start_performance_tracking()
        extraction_start_time = time.time()

        try:
            # Generate time-based extraction tasks: (data_type, batch_start, batch_end)
            extraction_tasks = []
            for data_type in data_types:
                batches = self._generate_date_batches(start_date, end_date, batch_days)
                for batch_start, batch_end in batches:
                    extraction_tasks.append((data_type, batch_start, batch_end))

            self.logger.info(f"Generated {len(extraction_tasks)} time-based extraction tasks")
            self.logger.info(f"Tasks structure: data_type + date_range (all regions processed in parallel per task)")

            # Execute all tasks sequentially (each task processes all regions in parallel internally)
            all_file_paths = []
            total_errors = []

            for task_idx, (data_type, batch_start, batch_end) in enumerate(extraction_tasks, 1):
                self.logger.info(f"Processing task {task_idx}/{len(extraction_tasks)}: {data_type} {batch_start} to {batch_end}")

                # Use EIACollector to process all regions for this data_type and date range
                batch_result = self.eia_collector.collect_batch_data_sync(
                    data_type=data_type,
                    start_date=batch_start,
                    end_date=batch_end,
                    regions=regions,
                    max_workers=max_workers,
                    delay_between_operations=self.batch_config.delay_between_operations
                )

                # Collect results
                if batch_result['success']:
                    all_file_paths.extend(batch_result['file_paths'])
                else:
                    total_errors.extend(batch_result['errors'])

                # Log progress
                progress = (task_idx / len(extraction_tasks)) * 100
                self.logger.info(f"Progress: {progress:.1f}% ({task_idx}/{len(extraction_tasks)} tasks completed)")

            extraction_end_time = time.time()
            total_duration = extraction_end_time - extraction_start_time

            # Calculate metrics
            total_files = len(all_file_paths)
            estimated_records = total_files * 2300  # Average records per API call
            actual_rps = estimated_records / total_duration if total_duration > 0 else 0

            # Stop performance tracking
            self._end_performance_tracking()

            # Prepare results summary
            results = {
                'success': len(total_errors) == 0,
                'total_files_created': total_files,
                'file_paths': all_file_paths,
                'estimated_total_records': estimated_records,
                'extraction_duration_seconds': total_duration,
                'extraction_duration_minutes': total_duration / 60,
                'estimated_rps': actual_rps,
                'regions_processed': regions,
                'data_types_processed': data_types,
                'batching_strategy': 'time-based',
                'total_tasks_processed': len(extraction_tasks),
                'errors': total_errors,
                'error_count': len(total_errors)
            }

            # Log completion summary
            if len(total_errors) == 0:
                self.logger.info(f"✅ Historical extraction completed successfully!")
            else:
                self.logger.warning(f"⚠️ Historical extraction completed with {len(total_errors)} errors")

            self.logger.info(f"   📁 Files created: {total_files}")
            self.logger.info(f"   📊 Estimated records: {estimated_records:,}")
            self.logger.info(f"   ⏱️  Duration: {total_duration:.1f} seconds ({total_duration/60:.1f} minutes)")
            self.logger.info(f"   🚀 Estimated RPS: {actual_rps:.1f}")
            self.logger.info(f"   📋 Batching: {len(extraction_tasks)} time-based tasks")

            return results

        except Exception as e:
            self._end_performance_tracking()
            self.logger.error(f"❌ Historical extraction failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'extraction_duration_seconds': time.time() - extraction_start_time,
                'batching_strategy': 'time-based'
            }

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
        return self.eia_collector.raw_loader.get_extraction_summary()

    def list_extracted_files(self) -> List[Path]:
        """List all extracted raw files."""
        return self.eia_collector.raw_loader.list_raw_files()
