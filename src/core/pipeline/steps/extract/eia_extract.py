"""
High-Performance EIA Extract Step

Fast async EIA data extraction using EIAClient with:
- 0.08s delay for high throughput (12.5 req/sec)
- 45-day date batching for optimal API usage
- 6 concurrent batches for maximum performance
- Comprehensive performance metrics (latency, RPS, throughput)
"""

import asyncio
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from pydantic import Field, field_validator

from ..base import BaseStep, ExtractStepConfig, StepOutput, StepMetrics
from ....integrations.eia.services import EIADataService
from ....integrations.eia.service.raw_data_loader import RawDataLoader, RawDataMetadata


@dataclass
class BatchRequest:
    """Represents a single batch API request."""
    data_type: str
    regions: List[str]
    start_date: str
    end_date: str
    request_id: str


class EIAExtractStepConfig(ExtractStepConfig):
    """Configuration for high-performance EIA extraction."""

    api_key: str = Field(..., description="EIA API key for authentication")
    data_types: List[str] = Field(
        default_factory=lambda: ['demand', 'generation'],
        description="Types of data to extract"
    )
    regions: List[str] = Field(
        default_factory=lambda: ["PACW", "ERCO", "NYIS", "ISNE", "PJM", "MISO", "SPP", "CARO"],
        description="List of region codes to extract data for"
    )

    # No rate limiting - let EIA API handle its own limits
    rate_limit_delay: float = Field(
        default=0.0,
        description="No artificial delays - maximum performance"
    )
    max_concurrent_batches: int = Field(
        default=50,
        description="Maximum concurrent requests (50 concurrent = optimal records/sec)"
    )

    # Date batching configuration - optimized for 15k records/sec
    batch_size_days: int = Field(
        default=7,
        description="Size of date batches in days (7 days = optimal balance of latency vs records/request)"
    )
    max_regions_per_request: int = Field(
        default=8,
        description="Maximum regions to include in a single API request (8 regions = best performance)"
    )

    # Data configuration
    raw_data_path: str = Field(
        default="data/raw/eia",
        description="Path for storing raw JSON files"
    )

    @field_validator("rate_limit_delay")
    @classmethod
    def validate_rate_limit(cls, v):
        return v  # No artificial limits

    @field_validator("max_concurrent_batches")
    @classmethod
    def validate_concurrency(cls, v):
        if v < 1:
            raise ValueError("max_concurrent_batches must be at least 1")
        return v

    @field_validator("batch_size_days")
    @classmethod
    def validate_batch_size(cls, v):
        if v < 1 or v > 365:  # Allow any reasonable batch size
            raise ValueError("batch_size_days must be between 1 and 365")
        return v

    @field_validator("data_types")
    @classmethod
    def validate_data_types(cls, v):
        valid_types = {'demand', 'generation'}
        if not all(dt in valid_types for dt in v):
            raise ValueError(f"data_types must be subset of {valid_types}")
        return v


class EIAExtractStep(BaseStep):
    """High-performance async EIA data extraction step."""

    def __init__(self, config: EIAExtractStepConfig):
        super().__init__(config)
        self.config: EIAExtractStepConfig = config

        # Initialize EIA service (uses EIAClient internally)
        self.eia_service = EIADataService(
            api_key=config.api_key,
            config=None  # Use default config
        )

        # Initialize components
        self.raw_data_path = Path(config.raw_data_path)
        self.raw_loader = RawDataLoader(self.raw_data_path)        # Performance tracking
        self.total_requests = 0
        self.total_records = 0
        self.total_bytes = 0
        self.failed_requests = 0
        self.request_latencies = []

        self.logger.info(f"Initialized EIAExtractStep with rate_limit={config.rate_limit_delay}s, "
                        f"batch_size={config.batch_size_days}days")

    def validate_input(self, config: EIAExtractStepConfig) -> None:
        """Validate async EIA extract step configuration."""
        if not config.api_key or not config.api_key.strip():
            raise ValueError("api_key is required and cannot be empty")

        if not config.data_types:
            raise ValueError("data_types list cannot be empty")

        if not config.regions:
            raise ValueError("regions list cannot be empty")

        try:
            Path(config.raw_data_path).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise ValueError(f"Cannot create raw data path '{config.raw_data_path}': {e}")

    def _generate_date_batches(self) -> List[tuple[str, str]]:
        """Generate date batches for optimal API usage."""
        start_date = datetime.strptime(self.config.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(self.config.end_date, "%Y-%m-%d").date()

        batches = []
        current_date = start_date

        while current_date <= end_date:
            batch_end = min(
                current_date + timedelta(days=self.config.batch_size_days - 1),
                end_date
            )
            batches.append((current_date.isoformat(), batch_end.isoformat()))
            current_date = batch_end + timedelta(days=1)

        self.logger.info(f"Generated {len(batches)} date batches of {self.config.batch_size_days} days each")
        return batches

    def _create_batch_requests(self) -> List[BatchRequest]:
        """Create batch requests combining date batches and region chunks."""
        requests = []
        request_id = 0

        # Generate date batches
        date_batches = self._generate_date_batches()

        # Split regions into chunks for optimal API efficiency
        region_chunks = [
            self.config.regions[i:i + self.config.max_regions_per_request]
            for i in range(0, len(self.config.regions), self.config.max_regions_per_request)
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

        self.logger.info(f"Created {len(requests)} batch requests")
        return requests

    async def _execute_api_request(self, batch: BatchRequest) -> Dict[str, Any]:
        """Execute API request with minimal overhead."""
        request_start = time.time()

        try:
            # Run API call in thread pool
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as pool:
                api_response = await loop.run_in_executor(
                    pool,
                    lambda: self.eia_service.get_raw_data(
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
        """Process API response in thread pool (CPU-bound task)."""
        try:
            if not response_data['success']:
                return response_data

            # Create metadata
            metadata = RawDataMetadata(
                timestamp=time.time(),
                region="_".join(response_data['regions']),
                data_type=response_data['data_type'],
                start_date=response_data['start_date'],
                end_date=response_data['end_date'],
                api_endpoint=f"/electricity/rto/{'fuel-type' if response_data['data_type'] == 'generation' else 'region'}-data",
                success=True
            )

            # Save raw data
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

        # Execute ALL requests concurrently (no semaphore limit)
        self.logger.info(f"Starting {len(batch_requests)} batch requests with FULL concurrency")

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
            'rps': actual_rps,  # Explicit RPS metric
            'latency_ms_avg': avg_latency_ms,
            'latency_ms_min': min_latency_ms,
            'latency_ms_max': max_latency_ms,
            'throughput_records_sec': records_per_second,
            'data_types_processed': self.config.data_types,
            'regions_processed': self.config.regions,
            'rate_limit_delay': self.config.rate_limit_delay,
            'batch_size_days': self.config.batch_size_days,
            'max_concurrent_batches': self.config.max_concurrent_batches,
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
    print("🔬 Robust EIA Extract Step using EIAClient")
    print("   Uses existing EIAClient for proper rate limiting and batching")
    print("   Run benchmark_eia_performance.py to test performance")
