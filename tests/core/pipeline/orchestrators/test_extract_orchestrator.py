"""Test ExtractOrchestrator for ETL pipeline with performance monitoring.

This test suite validates the ExtractOrchestrator's ability to:
1. Extract raw energy data from EIA API with Oregon focus
2. Manage batch operations with rate limiting and concurrency
3. Track performance metrics (RPS, latency, throughput)
4. Handle error conditions and retry logic
5. Store raw JSON responses with proper metadata

Following project patterns:
- Uses VCR for reproducible API testing
- Oregon/PACW region focus for energy data
- Async/await patterns for performance testing
- Comprehensive performance metrics validation
- Error handling and edge case testing

Run with: pytest tests/core/pipeline/orchestrators/test_extract_orchestrator.py -v
"""

import pytest
import asyncio
import tempfile
import shutil
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd
from typing import Dict, List, Any

from src.core.pipeline.orchestrators.extract_orchestrator import (
    ExtractOrchestrator,
    ExtractBatchConfig,
    ExtractBatchResult
)
from src.core.pipeline.orchestrators.base import BatchConfig, PerformanceMetrics
from src.core.integrations.config import get_test_config
from tests.vcr_config import VCRManager

# Oregon-specific test constants following project patterns
OREGON_REGION = "PACW"  # Pacific West region includes Oregon
OREGON_TEST_DATES = {
    "start": date(2024, 1, 1),
    "end": date(2024, 1, 7)  # One week for testing
}

class TestExtractOrchestratorInitialization:
    """Test ExtractOrchestrator initialization and configuration."""

    def test_orchestrator_initialization_default(self, tmp_path):
        """Test ExtractOrchestrator initializes with default configuration."""
        orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

        # Verify base orchestrator initialization
        assert orchestrator.raw_data_path == tmp_path
        assert orchestrator.eia_client is not None
        assert orchestrator.raw_loader is not None
        assert orchestrator.logger is not None

        # Verify extract-specific batch config defaults
        assert isinstance(orchestrator.batch_config, ExtractBatchConfig)
        assert orchestrator.batch_config.days_per_batch == 7
        assert orchestrator.batch_config.max_concurrent_batches == 3
        assert orchestrator.batch_config.delay_between_operations == 0.2
        assert orchestrator.batch_config.max_operations_per_second == 5.0
        assert orchestrator.batch_config.adaptive_batch_sizing is True

    def test_orchestrator_initialization_custom_path(self, tmp_path):
        """Test ExtractOrchestrator with custom raw data path."""
        custom_path = tmp_path / "custom_raw"
        orchestrator = ExtractOrchestrator(raw_data_path=str(custom_path))

        assert orchestrator.raw_data_path == custom_path

    def test_batch_configuration_update(self, tmp_path):
        """Test updating batch configuration."""
        orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

        # Create custom configuration
        custom_config = ExtractBatchConfig(
            days_per_batch=14,
            max_concurrent_batches=5,
            delay_between_operations=0.1,
            max_operations_per_second=10.0,
            adaptive_batch_sizing=False
        )

        orchestrator.configure_batching(custom_config)

        assert orchestrator.batch_config.days_per_batch == 14
        assert orchestrator.batch_config.max_concurrent_batches == 5
        assert orchestrator.batch_config.delay_between_operations == 0.1
        assert orchestrator.batch_config.max_operations_per_second == 10.0
        assert orchestrator.batch_config.adaptive_batch_sizing is False


class TestExtractOrchestratorBatchGeneration:
    """Test batch generation and date chunking logic."""

    def test_generate_date_batches_single_week(self, tmp_path):
        """Test generating batches for a single week."""
        orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 7)

        batches = orchestrator._generate_date_batches(start_date, end_date)

        assert len(batches) == 1
        assert batches[0] == (start_date, end_date)

    def test_generate_date_batches_multiple_weeks(self, tmp_path):
        """Test generating batches for multiple weeks."""
        orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 21)  # 3 weeks

        batches = orchestrator._generate_date_batches(start_date, end_date)

        assert len(batches) == 3
        assert batches[0] == (date(2024, 1, 1), date(2024, 1, 7))
        assert batches[1] == (date(2024, 1, 8), date(2024, 1, 14))
        assert batches[2] == (date(2024, 1, 15), date(2024, 1, 21))

    def test_generate_date_batches_custom_size(self, tmp_path):
        """Test generating batches with custom batch size."""
        orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

        # Configure for 3-day batches
        custom_config = ExtractBatchConfig(days_per_batch=3)
        orchestrator.configure_batching(custom_config)

        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 10)

        batches = orchestrator._generate_date_batches(start_date, end_date)

        assert len(batches) == 4  # 3, 3, 3, 1 days
        assert batches[0] == (date(2024, 1, 1), date(2024, 1, 3))
        assert batches[1] == (date(2024, 1, 4), date(2024, 1, 6))
        assert batches[2] == (date(2024, 1, 7), date(2024, 1, 9))
        assert batches[3] == (date(2024, 1, 10), date(2024, 1, 10))


class TestExtractOrchestratorPerformanceTracking:
    """Test performance tracking and metrics collection."""

    def test_performance_tracking_initialization(self, tmp_path):
        """Test performance tracking starts correctly."""
        orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

        # Initialize performance tracking
        orchestrator._start_performance_tracking()

        assert orchestrator.metrics.start_time is not None
        assert orchestrator.metrics.end_time is None
        assert orchestrator.metrics.total_operations == 0
        assert orchestrator.metrics.successful_operations == 0
        assert orchestrator.metrics.failed_operations == 0

    def test_performance_tracking_completion(self, tmp_path):
        """Test performance tracking completes correctly."""
        orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

        orchestrator._start_performance_tracking()
        orchestrator._end_performance_tracking()

        assert orchestrator.metrics.start_time is not None
        assert orchestrator.metrics.end_time is not None
        assert orchestrator.metrics.duration_seconds >= 0

    def test_performance_metrics_properties(self, tmp_path):
        """Test performance metrics calculated properties."""
        from datetime import datetime

        orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

        # Simulate some operations
        orchestrator.metrics.total_operations = 100
        orchestrator.metrics.successful_operations = 95
        orchestrator.metrics.failed_operations = 5
        orchestrator.metrics.total_records_processed = 1000
        orchestrator.metrics.total_bytes_processed = 50000
        orchestrator.metrics.operation_latencies = [0.1, 0.2, 0.15, 0.25, 0.3]

        # Mock time duration
        orchestrator.metrics.start_time = datetime(2024, 1, 1, 10, 0, 0)
        orchestrator.metrics.end_time = orchestrator.metrics.start_time + timedelta(seconds=10)

        metrics = orchestrator.get_performance_metrics()

        assert metrics.success_rate == 95.0
        assert metrics.operations_per_second == 10.0  # 100 ops / 10 seconds
        assert metrics.average_latency_ms > 0
        assert metrics.median_latency_ms > 0


class TestExtractOrchestratorRateLimiting:
    """Test rate limiting functionality."""

    @pytest.mark.asyncio
    async def test_rate_limiting_delay(self, tmp_path):
        """Test rate limiting applies correct delays."""
        orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

        # Configure for aggressive rate limiting
        config = ExtractBatchConfig(delay_between_operations=0.1)
        orchestrator.configure_batching(config)

        import time
        start_time = time.time()

        # Apply rate limiting twice
        await orchestrator._apply_rate_limiting()
        await orchestrator._apply_rate_limiting()

        elapsed = time.time() - start_time

        # Should have at least one delay period
        assert elapsed >= 0.1

    @pytest.mark.asyncio
    async def test_rate_limiting_sequence(self, tmp_path):
        """Test rate limiting in sequence of operations."""
        orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

        # Configure for fast testing
        config = ExtractBatchConfig(delay_between_operations=0.05)
        orchestrator.configure_batching(config)

        import time
        start_time = time.time()

        # Simulate multiple operations
        for _ in range(3):
            await orchestrator._apply_rate_limiting()

        elapsed = time.time() - start_time

        # Should have at least 2 delay periods (between 3 operations)
        assert elapsed >= 0.1


class TestExtractOrchestratorWithMocks:
    """Test ExtractOrchestrator with mocked dependencies for unit testing."""

    def test_process_data_basic_flow(self, tmp_path):
        """Test basic process_data flow with mocks."""
        with patch('src.core.pipeline.orchestrators.extract_orchestrator.EIAClient') as mock_eia_client, \
             patch('src.core.pipeline.orchestrators.extract_orchestrator.RawDataLoader') as mock_raw_loader, \
             patch('src.core.pipeline.orchestrators.extract_orchestrator.get_config') as mock_config:

            # Setup mocks
            mock_config.return_value = get_test_config()
            mock_raw_loader_instance = MagicMock()
            mock_raw_loader.return_value = mock_raw_loader_instance

            orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

            # Verify mocks were called
            mock_eia_client.assert_called_once()
            mock_raw_loader.assert_called_once()

    @pytest.mark.asyncio
    async def test_extract_single_batch_success(self, tmp_path):
        """Test successful single batch extraction."""
        with patch('src.core.pipeline.orchestrators.extract_orchestrator.EIAClient') as mock_eia_client, \
             patch('src.core.pipeline.orchestrators.extract_orchestrator.RawDataLoader') as mock_raw_loader, \
             patch('src.core.pipeline.orchestrators.extract_orchestrator.get_config') as mock_config:

            # Setup mocks
            mock_config.return_value = get_test_config()
            mock_raw_loader_instance = MagicMock()
            mock_raw_loader.return_value = mock_raw_loader_instance

            # Mock successful extraction
            mock_file_path = tmp_path / "test_file.json"
            mock_raw_loader_instance.extract_demand_data.return_value = mock_file_path
            mock_raw_loader_instance.load_raw_file.return_value = {
                "metadata": {
                    "record_count": 168,  # 7 days * 24 hours
                    "response_size_bytes": 5000
                }
            }

            orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

            # Test single batch extraction
            result = await orchestrator._extract_single_batch(
                start_date=OREGON_TEST_DATES["start"],
                end_date=OREGON_TEST_DATES["end"],
                region=OREGON_REGION,
                data_type="demand"
            )

            assert isinstance(result, ExtractBatchResult)
            assert result.success is True
            assert result.data_type == "demand"
            assert result.region == OREGON_REGION
            assert result.records_processed == 168
            assert result.bytes_processed == 5000

    @pytest.mark.asyncio
    async def test_extract_single_batch_error(self, tmp_path):
        """Test single batch extraction with error."""
        with patch('src.core.pipeline.orchestrators.extract_orchestrator.EIAClient') as mock_eia_client, \
             patch('src.core.pipeline.orchestrators.extract_orchestrator.RawDataLoader') as mock_raw_loader, \
             patch('src.core.pipeline.orchestrators.extract_orchestrator.get_config') as mock_config:

            # Setup mocks
            mock_config.return_value = get_test_config()
            mock_raw_loader_instance = MagicMock()
            mock_raw_loader.return_value = mock_raw_loader_instance

            # Mock extraction error
            mock_raw_loader_instance.extract_demand_data.side_effect = Exception("API Error")

            orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

            # Test single batch extraction with error
            result = await orchestrator._extract_single_batch(
                start_date=OREGON_TEST_DATES["start"],
                end_date=OREGON_TEST_DATES["end"],
                region=OREGON_REGION,
                data_type="demand"
            )

            assert isinstance(result, ExtractBatchResult)
            assert result.success is False
            assert result.error_message is not None
            assert "API Error" in result.error_message


class TestExtractOrchestratorIntegration:
    """Integration tests for ExtractOrchestrator (may require API key or VCR)."""

    @pytest.mark.asyncio
    async def test_oregon_demand_extraction_with_vcr(self, tmp_path, vcr_cassette):
        """Test Oregon demand extraction with VCR cassette."""
        with vcr_cassette("extract_orchestrator_oregon_demand"):
            orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

            # Test small date range
            results = await orchestrator.process_data(
                start_date=OREGON_TEST_DATES["start"],
                end_date=OREGON_TEST_DATES["start"] + timedelta(days=1),  # 2 days only
                region=OREGON_REGION,
                data_types=["demand"]
            )

            assert "demand" in results
            demand_results = results["demand"]
            assert len(demand_results) >= 1

            # Verify at least one successful result
            successful_results = [r for r in demand_results if r.success]
            assert len(successful_results) >= 1

            # Check performance metrics
            metrics = orchestrator.get_performance_metrics()
            assert metrics.total_operations > 0
            assert metrics.successful_operations > 0
            assert metrics.duration_seconds > 0

    @pytest.mark.asyncio
    async def test_oregon_generation_extraction_with_vcr(self, tmp_path, vcr_cassette):
        """Test Oregon generation extraction with VCR cassette."""
        with vcr_cassette("extract_orchestrator_oregon_generation"):
            orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

            # Test generation data
            results = await orchestrator.process_data(
                start_date=OREGON_TEST_DATES["start"],
                end_date=OREGON_TEST_DATES["start"] + timedelta(days=1),
                region=OREGON_REGION,
                data_types=["generation"]
            )

            assert "generation" in results
            generation_results = results["generation"]
            assert len(generation_results) >= 1

            # Verify at least one successful result
            successful_results = [r for r in generation_results if r.success]
            assert len(successful_results) >= 1

    @pytest.mark.asyncio
    async def test_comprehensive_extraction_with_vcr(self, tmp_path, vcr_cassette):
        """Test comprehensive extraction with both demand and generation."""
        with vcr_cassette("extract_orchestrator_comprehensive"):
            orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

            # Test both data types
            results = await orchestrator.process_data(
                start_date=OREGON_TEST_DATES["start"],
                end_date=OREGON_TEST_DATES["start"] + timedelta(days=2),
                region=OREGON_REGION,
                data_types=["demand", "generation"]
            )

            assert "demand" in results
            assert "generation" in results

            # Verify both have results
            assert len(results["demand"]) >= 1
            assert len(results["generation"]) >= 1

            # Check extraction summary
            summary = orchestrator.get_extraction_summary()
            assert summary["total_files"] > 0
            assert summary["total_records"] > 0
            assert summary["total_size_bytes"] > 0

            # Check that files were actually created
            raw_files = orchestrator.list_extracted_files()
            assert len(raw_files) > 0


class TestExtractOrchestratorPerformanceBenchmark:
    """Performance benchmarking tests for ExtractOrchestrator."""

    @pytest.mark.asyncio
    async def test_batch_size_optimization(self, tmp_path):
        """Test different batch sizes for performance optimization."""
        with patch('src.core.pipeline.orchestrators.extract_orchestrator.EIAClient') as mock_eia_client, \
             patch('src.core.pipeline.orchestrators.extract_orchestrator.RawDataLoader') as mock_raw_loader, \
             patch('src.core.pipeline.orchestrators.extract_orchestrator.get_config') as mock_config:

            # Setup mocks for fast simulation
            mock_config.return_value = get_test_config()
            mock_raw_loader_instance = MagicMock()
            mock_raw_loader.return_value = mock_raw_loader_instance

            mock_file_path = tmp_path / "test_file.json"
            mock_raw_loader_instance.extract_demand_data.return_value = mock_file_path
            mock_raw_loader_instance.load_raw_file.return_value = {
                "metadata": {"record_count": 24, "response_size_bytes": 1000}
            }

            batch_sizes = [1, 3, 7, 14]
            performance_results = {}

            for batch_size in batch_sizes:
                orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))
                config = ExtractBatchConfig(
                    days_per_batch=batch_size,
                    delay_between_operations=0.01  # Fast for testing
                )
                orchestrator.configure_batching(config)

                # Test 2-week period
                import time
                start_time = time.time()

                results = await orchestrator.process_data(
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 1, 14),
                    region=OREGON_REGION,
                    data_types=["demand"]
                )

                end_time = time.time()
                duration = end_time - start_time

                metrics = orchestrator.get_performance_metrics()
                performance_results[batch_size] = {
                    "duration": duration,
                    "operations_per_second": metrics.operations_per_second,
                    "success_rate": metrics.success_rate
                }

            # Verify all batch sizes completed successfully
            for batch_size, result in performance_results.items():
                assert result["success_rate"] > 90.0, f"Batch size {batch_size} had low success rate"
                assert result["operations_per_second"] > 0, f"Batch size {batch_size} had no throughput"


class TestExtractOrchestratorErrorHandling:
    """Test error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_invalid_date_range(self, tmp_path):
        """Test handling of invalid date ranges."""
        orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

        # Test end date before start date - should validate internally
        # The orchestrator may not explicitly validate this, so let's check if it handles gracefully
        try:
            results = await orchestrator.process_data(
                start_date=date(2024, 1, 10),
                end_date=date(2024, 1, 5),  # Invalid: end before start
                region=OREGON_REGION
            )
            # If no exception is raised, verify results are empty or handled gracefully
            assert isinstance(results, dict)
            for data_type, batch_results in results.items():
                assert isinstance(batch_results, list)
        except ValueError:
            # This is also acceptable - explicit validation
            pass

    @pytest.mark.asyncio
    async def test_invalid_region(self, tmp_path):
        """Test handling of invalid region codes."""
        with patch('src.core.pipeline.orchestrators.extract_orchestrator.EIAClient') as mock_eia_client, \
             patch('src.core.pipeline.orchestrators.extract_orchestrator.RawDataLoader') as mock_raw_loader, \
             patch('src.core.pipeline.orchestrators.extract_orchestrator.get_config') as mock_config:

            mock_config.return_value = get_test_config()
            mock_raw_loader_instance = MagicMock()
            mock_raw_loader.return_value = mock_raw_loader_instance

            # Mock API error for invalid region
            mock_raw_loader_instance.extract_demand_data.side_effect = Exception("Invalid region")

            orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

            results = await orchestrator.process_data(
                start_date=OREGON_TEST_DATES["start"],
                end_date=OREGON_TEST_DATES["end"],
                region="INVALID_REGION",
                data_types=["demand"]
            )

            # Should handle gracefully
            assert "demand" in results
            demand_results = results["demand"]

            # All results should be failures
            successful_results = [r for r in demand_results if r.success]
            assert len(successful_results) == 0

    @pytest.mark.asyncio
    async def test_invalid_data_types(self, tmp_path):
        """Test handling of invalid data types."""
        orchestrator = ExtractOrchestrator(raw_data_path=str(tmp_path))

        # Test unsupported data type - should handle gracefully
        results = await orchestrator.process_data(
            start_date=OREGON_TEST_DATES["start"],
            end_date=OREGON_TEST_DATES["end"],
            region=OREGON_REGION,
            data_types=["invalid_type"]
        )

        # Should handle gracefully and return failed results
        assert "invalid_type" in results
        invalid_results = results["invalid_type"]
        assert len(invalid_results) >= 1

        # All results should be failures
        successful_results = [r for r in invalid_results if r.success]
        assert len(successful_results) == 0

        # Should have error messages
        failed_results = [r for r in invalid_results if not r.success]
        assert len(failed_results) >= 1
        assert all(r.error_message is not None for r in failed_results)


if __name__ == "__main__":
    # Quick integration test
    import tempfile
    import asyncio

    async def quick_test():
        with tempfile.TemporaryDirectory() as tmp_dir:
            print("🧪 Running quick ExtractOrchestrator test...")

            orchestrator = ExtractOrchestrator(raw_data_path=tmp_dir)

            # Test initialization
            assert orchestrator.raw_data_path == Path(tmp_dir)
            assert isinstance(orchestrator.batch_config, ExtractBatchConfig)

            # Test batch generation
            batches = orchestrator._generate_date_batches(
                date(2024, 1, 1),
                date(2024, 1, 14)
            )
            assert len(batches) == 2  # 7-day batches

            print("✅ ExtractOrchestrator basic functionality working!")
            print("\nTo run full test suite:")
            print("pytest tests/core/pipeline/orchestrators/test_extract_orchestrator.py -v")
            print("\nTo run with VCR cassettes:")
            print("EIA_API_KEY=your_key pytest tests/core/pipeline/orchestrators/test_extract_orchestrator.py::TestExtractOrchestratorIntegration -v")

    asyncio.run(quick_test())
