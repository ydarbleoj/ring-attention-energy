"""
Generic API Extract Step

Source-agnostic API data extraction with source-specific configurations:
- Dynamic loading of source-specific services and configs
- High-performance async extraction with configurable concurrency
- Comprehensive performance metrics and error handling
- Worker-native pattern for easy integration scaling

Supports: EIA, CAISO, and other API-based data sources
"""

import asyncio
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Type, TYPE_CHECKING
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from pydantic import Field, field_validator

from ..base import BaseStep, ExtractStepConfig, StepOutput, StepMetrics

# Dynamic imports based on source
if TYPE_CHECKING:
    from ....integrations.eia.services import EIADataService
    from ....integrations.eia.services.raw_data_loader import RawDataLoader, RawDataMetadata


@dataclass
class BatchRequest:
    """Represents a single batch API request."""
    data_type: str
    regions: List[str]
    start_date: str
    end_date: str
    request_id: str


class ApiExtractStepConfig(ExtractStepConfig):
    """Configuration for generic API extraction with source-specific settings."""

    source: str = Field(..., description="Data source (eia, caiso, synthetic, etc.)")

    # Source-specific configuration will be loaded dynamically
    # This base config provides common fields
    api_key: Optional[str] = Field(None, description="API key for authentication")

    # Default performance settings (can be overridden by source-specific configs)
    rate_limit_delay: float = Field(default=0.0, description="Rate limiting delay")
    max_concurrent_batches: int = Field(default=10, description="Max concurrent requests")
    batch_size_days: int = Field(default=30, description="Date batch size in days")
    max_regions_per_request: int = Field(default=5, description="Max regions per request")

    raw_data_path: Optional[str] = Field(None, description="Path for raw data storage")

    @field_validator("source")
    @classmethod
    def validate_source(cls, v):
        supported_sources = {"eia", "caiso", "synthetic"}
        if v not in supported_sources:
            raise ValueError(f"Unsupported source: {v}. Supported: {supported_sources}")
        return v


class ApiExtractStep(BaseStep):
    """Generic API data extraction step with source-specific configurations."""

    def __init__(self, config: ApiExtractStepConfig):
        super().__init__(config)
        self.config: ApiExtractStepConfig = config

        # Load source-specific configuration and services
        self.source_config = self._load_source_config()
        self.data_service = self._get_data_service()
        self.raw_loader = self._get_raw_loader()

        # Performance tracking
        self.total_requests = 0
        self.total_records = 0
        self.total_bytes = 0
        self.failed_requests = 0
        self.request_latencies = []

        # self.logger.info(f"Initialized ApiExtractStep for source: {config.source}")

    def _load_source_config(self):
        """Load source-specific configuration."""
        if self.config.source == "eia":
            from ....integrations.eia.schema import EIAExtractConfig

            # Create source-specific config with merged settings
            source_config = EIAExtractConfig(
                step_name=self.config.step_name,
                step_id=self.config.step_id,
                start_date=self.config.start_date,
                end_date=self.config.end_date,
                regions=self.config.regions,
                data_types=self.config.data_types,
                api_key=self.config.api_key or "",
                raw_data_path=self.config.raw_data_path or "data/raw/eia",
                # Use configuration from parent, not hardcoded values
                rate_limit_delay=getattr(self.config, 'rate_limit_delay', 0.0),
                max_concurrent_batches=getattr(self.config, 'max_concurrent_batches', 50),
                batch_size_days=getattr(self.config, 'batch_size_days', 7),
                max_regions_per_request=getattr(self.config, 'max_regions_per_request', 8)
            )
            return source_config

        elif self.config.source == "caiso":
            # TODO: Implement CAISO config loading
            raise NotImplementedError("CAISO extract config not yet implemented")

        elif self.config.source == "synthetic":
            # TODO: Implement synthetic config loading
            raise NotImplementedError("Synthetic extract config not yet implemented")

        else:
            raise ValueError(f"Unknown source: {self.config.source}")

    def _get_data_service(self):
        """Get source-specific data service."""
        if self.config.source == "eia":
            # Import from the actual services.py file by being more specific
            from ....integrations.eia.service import EIADataService
            return EIADataService(api_key=self.source_config.api_key)

        elif self.config.source == "caiso":
            # TODO: Implement CAISO service loading
            raise NotImplementedError("CAISO data service not yet implemented")

        elif self.config.source == "synthetic":
            # TODO: Implement synthetic service loading
            raise NotImplementedError("Synthetic data service not yet implemented")

        else:
            raise ValueError(f"Unknown source: {self.config.source}")

    def _get_raw_loader(self):
        """Get source-specific raw data loader."""
        if self.config.source == "eia":
            from ....integrations.eia.services.raw_data_loader import RawDataLoader
            return RawDataLoader(self.source_config.raw_data_path)

        elif self.config.source == "caiso":
            # TODO: Implement CAISO raw loader
            raise NotImplementedError("CAISO raw loader not yet implemented")

        elif self.config.source == "synthetic":
            # TODO: Implement synthetic raw loader
            raise NotImplementedError("Synthetic raw loader not yet implemented")

        else:
            raise ValueError(f"Unknown source: {self.config.source}")

    def validate_input(self, config: ApiExtractStepConfig) -> None:
        """Validate generic API extract step configuration."""
        if not config.source:
            raise ValueError("source is required")

        # Source-specific validation will be handled by source-specific configs
        try:
            # Test that we can load source-specific components
            self._load_source_config()
        except Exception as e:
            raise ValueError(f"Failed to load source '{config.source}': {e}")

    def _generate_date_batches(self) -> List[tuple[str, str]]:
        """Generate date batches using source-specific configuration."""
        start_date = datetime.strptime(self.config.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(self.config.end_date, "%Y-%m-%d").date()

        batch_size_days = self.source_config.batch_size_days
        batches = []
        current_date = start_date

        while current_date <= end_date:
            batch_end = min(
                current_date + timedelta(days=batch_size_days - 1),
                end_date
            )
            batches.append((current_date.isoformat(), batch_end.isoformat()))
            current_date = batch_end + timedelta(days=1)

        # self.logger.info(f"Generated {len(batches)} date batches of {batch_size_days} days each")
        return batches

    def _create_batch_requests(self) -> List[BatchRequest]:
        """Create batch requests using source-specific configuration."""
        requests = []
        request_id = 0

        # Generate date batches
        date_batches = self._generate_date_batches()

        # Split regions into chunks based on source config
        max_regions = self.source_config.max_regions_per_request
        region_chunks = [
            self.config.regions[i:i + max_regions]
            for i in range(0, len(self.config.regions), max_regions)
        ]

        # Create batch requests for each combination
        for data_type in self.config.data_types:
            for start_date, end_date in date_batches:
                for region_chunk in region_chunks:
                    requests.append(BatchRequest(
                        data_type=data_type,
                        regions=region_chunk,
                        start_date=start_date,
                        end_date=end_date,
                        request_id=f"batch_{request_id:04d}_{data_type}_{len(region_chunk)}regions_{start_date}"
                    ))
                    request_id += 1

        # self.logger.info(f"Created {len(requests)} batch requests")
        return requests

    async def _execute_api_request(self, batch: BatchRequest) -> Dict[str, Any]:
        """Execute API request using source-specific data service."""
        request_start = time.time()

        try:
            # Run API call in thread pool using source-specific service
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                api_response = await loop.run_in_executor(
                    pool,
                    lambda: self.data_service.get_raw_data(
                        data_type=batch.data_type,
                        regions=batch.regions,
                        start_date=batch.start_date,
                        end_date=batch.end_date
                    )
                )

            # Extract data from response
            records = api_response.get('response', {}).get('data', [])
            record_count = len(records)

            # Calculate latency in milliseconds
            latency_ms = (time.time() - request_start) * 1000
            self.request_latencies.append(latency_ms)

            return {
                'success': True,
                'batch_id': batch.request_id,
                'data_type': batch.data_type,
                'regions': batch.regions,
                'start_date': batch.start_date,
                'end_date': batch.end_date,
                'response_data': api_response,
                'record_count': record_count,
                'response_size': len(str(api_response)),
                'latency_ms': latency_ms
            }

        except Exception as e:
            self.logger.error(f"API request failed for {batch.request_id}: {e}")
            return {
                'success': False,
                'batch_id': batch.request_id,
                'data_type': batch.data_type,
                'regions': batch.regions,
                'start_date': batch.start_date,
                'end_date': batch.end_date,
                'error': str(e)
            }

    def _process_response_in_thread(self, response_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process API response using source-specific raw loader."""
        try:
            if not response_data['success']:
                return response_data

            # Create metadata using source-specific format
            if self.config.source == "eia":
                from ....integrations.eia.services.raw_data_loader import RawDataMetadata

                metadata = RawDataMetadata(
                    timestamp=datetime.now().isoformat(),
                    region="_".join(response_data['regions']),
                    data_type=response_data['data_type'],
                    start_date=response_data['start_date'],
                    end_date=response_data['end_date'],
                    api_endpoint=f"/electricity/rto/{'fuel-type' if response_data['data_type'] == 'generation' else 'region'}-data",
                    success=True
                )
            else:
                # TODO: Add metadata creation for other sources
                raise NotImplementedError(f"Metadata creation not implemented for source: {self.config.source}")

            # Save raw data using source-specific loader
            file_path = self.raw_loader.save_raw_data(response_data['response_data'], metadata)

            return {
                'success': True,
                'batch_id': response_data['batch_id'],
                'file_path': file_path,
                'record_count': response_data['record_count'],
                'response_size': response_data['response_size'],
                'data_type': response_data['data_type'],
                'regions': response_data['regions'],
                'start_date': response_data['start_date'],
                'end_date': response_data['end_date']
            }

        except Exception as e:
            return {
                'success': False,
                'batch_id': response_data.get('batch_id', 'unknown'),
                'error': f"Processing error: {str(e)}"
            }

    async def _execute_async_extraction(self) -> Dict[str, Any]:
        """Execute async extraction with maximum concurrency."""

        # Create batch requests
        batch_requests = self._create_batch_requests()
        start_time = time.time()

        async def process_batch(batch: BatchRequest):
            # Execute API request (no rate limiting)
            response = await self._execute_api_request(batch)

            # Process response in thread pool if successful
            if response['success']:
                loop = asyncio.get_event_loop()
                with ThreadPoolExecutor() as pool:
                    result = await loop.run_in_executor(
                        pool,
                        self._process_response_in_thread,
                        response
                    )
                return result
            else:
                return response

        # Execute requests with source-specific concurrency
        max_concurrent = self.source_config.max_concurrent_batches
        # self.logger.info(f"Starting {len(batch_requests)} batch requests with max concurrency: {max_concurrent}")

        results = await asyncio.gather(*[
            process_batch(batch) for batch in batch_requests
        ], return_exceptions=True)

        # Process results
        successful_results = []
        failed_results = []

        for result in results:
            if isinstance(result, Exception):
                failed_results.append({'error': str(result)})
            elif result.get('success'):
                successful_results.append(result)
                self.total_records += result.get('record_count', 0)
                self.total_bytes += result.get('response_size', 0)
            else:
                failed_results.append(result)

        self.total_requests = len(batch_requests)
        self.failed_requests = len(failed_results)

        duration = time.time() - start_time

        # Collect output paths
        output_paths = [Path(r['file_path']) for r in successful_results if 'file_path' in r]

        # Calculate performance metrics
        avg_latency_ms = sum(self.request_latencies) / len(self.request_latencies) if self.request_latencies else 0
        min_latency_ms = min(self.request_latencies) if self.request_latencies else 0
        max_latency_ms = max(self.request_latencies) if self.request_latencies else 0
        actual_rps = self.total_requests / duration if duration > 0 else 0
        records_per_second = self.total_records / duration if duration > 0 else 0

        return {
            'success': len(failed_results) == 0,
            'source': self.config.source,
            'total_requests': self.total_requests,
            'successful_requests': len(successful_results),
            'failed_requests': self.failed_requests,
            'records_processed': self.total_records,
            'bytes_processed': self.total_bytes,
            'api_calls_made': self.total_requests,
            'files_created': len(output_paths),
            'output_paths': output_paths,
            'duration_seconds': duration,
            'records_per_second': records_per_second,
            'requests_per_second': actual_rps,
            'rps': actual_rps,
            'latency_ms_avg': avg_latency_ms,
            'latency_ms_min': min_latency_ms,
            'latency_ms_max': max_latency_ms,
            'throughput_records_sec': records_per_second,
            'data_types_processed': self.config.data_types,
            'regions_processed': self.config.regions,
            # Source-specific performance settings used
            'source_config': {
                'rate_limit_delay': self.source_config.rate_limit_delay,
                'batch_size_days': self.source_config.batch_size_days,
                'max_concurrent_batches': self.source_config.max_concurrent_batches,
                'max_regions_per_request': self.source_config.max_regions_per_request
            },
            'successful_results': successful_results,
            'failed_results': failed_results
        }

    def _execute(self) -> Dict[str, Any]:
        """Execute the async extraction (bridge to sync interface)."""
        try:
            # Check if there's already a running event loop
            loop = asyncio.get_running_loop()
            # If we're in an async context, we need to handle this differently
            if loop.is_running():
                # Create a new thread to run the async code
                import concurrent.futures
                import threading

                def run_async():
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(self._execute_async_extraction())
                    finally:
                        new_loop.close()

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_async)
                    return future.result()
            else:
                # No running loop, safe to create one
                return asyncio.run(self._execute_async_extraction())

        except RuntimeError:
            # No event loop running, create one
            return asyncio.run(self._execute_async_extraction())

# Example usage and testing
if __name__ == "__main__":
    print("🔬 Generic API Extract Step")
    print("   Source-agnostic extraction with dynamic config loading")
    print("   Supports: EIA (optimized), CAISO (TODO), Synthetic (TODO)")
    print("   Usage: ApiExtractStep(ApiExtractStepConfig(source='eia', ...))")
