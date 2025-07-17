import logging
import time
import concurrent.futures
from typing import Dict, Any, List
from datetime import date, timedelta
from pathlib import Path

from ...integrations.eia.services import EIADataService
from ...integrations.eia.services.raw_data_loader import RawDataLoader, RawDataMetadata

logger = logging.getLogger(__name__)


class EIACollector:
    def __init__(self, api_key: str, config: Dict[str, Any] = None, raw_data_path: str = "data/raw"):
        """Initialize EIA collector.

        Args:
            api_key: EIA API key
            config: Configuration dictionary
            raw_data_path: Path for storing raw JSON files
        """
        self.api_key = api_key
        self.logger = logger

        # Initialize raw data components only
        self.raw_data_path = Path(raw_data_path)
        self.eia_service = EIADataService(api_key=api_key, config=config)
        self.raw_loader = RawDataLoader(self.raw_data_path)

        self.logger.info(f"EIACollector initialized with raw path: {self.raw_data_path}")

    def collect_batch_data_sync(
        self,
        data_type: str,
        start_date: date,
        end_date: date,
        regions: List[str] = None,
        delay_between_operations: float = 0.2
    ) -> Dict[str, Any]:
        """
        Collect raw data for all regions in a SINGLE API call for a specific data type and date range.

        Args:
            data_type: Either "demand" or "generation"
            start_date: Start date for the batch
            end_date: End date for the batch
            regions: List of regions (defaults to all regions)
            delay_between_operations: Rate limiting delay

        Returns:
            Dict with results including file_path, success, records, etc.
        """
        if regions is None:
            regions = self.DEFAULT_REGIONS

        self.logger.info(f"🚀 Collecting {data_type} data for all regions")
        self.logger.info(f"   Date range: {start_date} to {end_date}")
        self.logger.info(f"   Regions: {regions}")

        batch_start_time = time.time()

        try:
            # Rate limiting
            time.sleep(delay_between_operations)

            # Convert dates to strings
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")

            # Make SINGLE API call for ALL regions
            api_response = self.eia_service.get_raw_data(
                data_type=data_type,
                regions=regions,
                start_date=start_str,
                end_date=end_str
            )

            # Create metadata for the file
            metadata = RawDataMetadata(
                timestamp=time.time(),
                region="_".join(regions),
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

            self.logger.info(f"✅ Batch complete: {records:,} records, {bytes_size:,} bytes in {batch_duration:.1f}s")

            return {
                'success': True,
                'data_type': data_type,
                'start_date': start_date,
                'end_date': end_date,
                'regions_processed': regions,
                'file_paths': [file_path],
                'files_created': 1,
                'errors': [],
                'error_count': 0,
                'batch_duration_seconds': batch_duration,
                'total_records': records,
                'total_bytes': bytes_size,
                'api_calls': 1
            }

        except Exception as e:
            batch_duration = time.time() - batch_start_time
            error_msg = f"Failed to collect {data_type} data: {str(e)}"
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
                'batch_duration_seconds': batch_duration
            }

    def collect_date_range_batch(
        self,
        start_date: date,
        end_date: date,
        data_types: List[str] = None,
        batch_days: int = 45,
        max_workers: int = 6
    ) -> Dict[str, Any]:
        """
        Collect data for a date range using date-based batching with parallel execution.

        Args:
            start_date: Start date for collection
            end_date: End date for collection
            data_types: List of data types (defaults to ['demand', 'generation'])
            batch_days: Days per batch (default 45)
            max_workers: Max parallel workers (default 6: 3 demand + 3 generation)

        Returns:
            Dict with collection results and performance metrics
        """
        if data_types is None:
            data_types = ['demand', 'generation']

        self.logger.info(f"🚀 Starting date range collection with {max_workers} workers")
        self.logger.info(f"   Date range: {start_date} to {end_date}")
        self.logger.info(f"   Data types: {data_types}")
        self.logger.info(f"   Batch size: {batch_days} days")

        # Generate date batches
        date_batches = self._generate_date_batches(start_date, end_date, batch_days)

        # Create tasks: (data_type, batch_start, batch_end)
        tasks = []
        for data_type in data_types:
            for batch_start, batch_end in date_batches:
                tasks.append((data_type, batch_start, batch_end))

        self.logger.info(f"   Generated {len(tasks)} total tasks")

        collection_start_time = time.time()
        all_results = []
        total_errors = []

        # Execute tasks in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(self.collect_batch_data_sync, data_type, batch_start, batch_end):
                (data_type, batch_start, batch_end)
                for data_type, batch_start, batch_end in tasks
            }

            for future in concurrent.futures.as_completed(future_to_task):
                data_type, batch_start, batch_end = future_to_task[future]
                try:
                    result = future.result()
                    all_results.append(result)

                    if result['success']:
                        self.logger.info(f"✅ {data_type} {batch_start}-{batch_end}: {result['total_records']:,} records")
                    else:
                        total_errors.extend(result['errors'])
                        self.logger.error(f"❌ {data_type} {batch_start}-{batch_end}: {result['errors']}")

                except Exception as e:
                    error_msg = f"Task failed {data_type} {batch_start}-{batch_end}: {str(e)}"
                    total_errors.append(error_msg)
                    self.logger.error(f"❌ {error_msg}")

        # Calculate summary
        collection_duration = time.time() - collection_start_time
        successful_results = [r for r in all_results if r['success']]

        total_files = sum(r['files_created'] for r in successful_results)
        total_records = sum(r['total_records'] for r in successful_results)
        total_bytes = sum(r['total_bytes'] for r in successful_results)
        total_api_calls = sum(r['api_calls'] for r in successful_results)

        # Calculate RPS
        rps = total_records / collection_duration if collection_duration > 0 else 0

        self.logger.info(f"🏆 Collection complete: {total_files} files, {total_records:,} records in {collection_duration:.1f}s")
        self.logger.info(f"   RPS: {rps:,.1f}")

        return {
            'success': len(total_errors) == 0,
            'total_duration_seconds': collection_duration,
            'total_files_created': total_files,
            'total_records': total_records,
            'total_bytes': total_bytes,
            'total_api_calls': total_api_calls,
            'rps': rps,
            'successful_batches': len(successful_results),
            'failed_batches': len(all_results) - len(successful_results),
            'errors': total_errors,
            'batch_results': all_results
        }

    def _generate_date_batches(self, start_date: date, end_date: date, batch_days: int) -> List[tuple]:
        """Generate date batches for processing."""
        batches = []
        current_date = start_date

        while current_date <= end_date:
            batch_end = min(current_date + timedelta(days=batch_days - 1), end_date)
            batches.append((current_date, batch_end))
            current_date = batch_end + timedelta(days=1)

        return batches

    def get_extraction_summary(self) -> Dict[str, Any]:
        """Get summary of extracted data."""
        return self.raw_loader.get_extraction_summary()

    def list_extracted_files(self) -> List[Path]:
        """List all extracted raw files."""
        return self.raw_loader.list_raw_files()
