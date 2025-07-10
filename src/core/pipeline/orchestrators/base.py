"""Base orchestrator functionality for ETL pipeline stages.

This module provides shared performance metrics, batch configuration,
and base classes for all pipeline orchestrators.
"""

import asyncio
import logging
import time
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean, median
from abc import ABC, abstractmethod


@dataclass
class PerformanceMetrics:
    """Base performance metrics for orchestrator operations."""

    # Request/operation metrics
    total_operations: int = 0
    successful_operations: int = 0
    failed_operations: int = 0

    # Timing metrics
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    operation_latencies: List[float] = field(default_factory=list)

    # Data metrics
    total_records_processed: int = 0
    total_bytes_processed: int = 0

    # Error tracking
    error_messages: List[str] = field(default_factory=list)
    retry_attempts: int = 0

    @property
    def duration_seconds(self) -> float:
        """Total duration in seconds."""
        if not self.start_time or not self.end_time:
            return 0.0
        return (self.end_time - self.start_time).total_seconds()

    @property
    def operations_per_second(self) -> float:
        """Average operations per second."""
        if self.duration_seconds == 0:
            return 0.0
        return self.total_operations / self.duration_seconds

    @property
    def success_rate(self) -> float:
        """Success rate as percentage."""
        if self.total_operations == 0:
            return 0.0
        return (self.successful_operations / self.total_operations) * 100

    @property
    def average_latency_ms(self) -> float:
        """Average operation latency in milliseconds."""
        if not self.operation_latencies:
            return 0.0
        return mean(self.operation_latencies) * 1000

    @property
    def median_latency_ms(self) -> float:
        """Median operation latency in milliseconds."""
        if not self.operation_latencies:
            return 0.0
        return median(self.operation_latencies) * 1000

    @property
    def records_per_second(self) -> float:
        """Records processed per second."""
        if self.duration_seconds == 0:
            return 0.0
        return self.total_records_processed / self.duration_seconds

    @property
    def throughput_mb_per_second(self) -> float:
        """Throughput in MB/second."""
        if self.duration_seconds == 0:
            return 0.0
        return (self.total_bytes_processed / (1024 * 1024)) / self.duration_seconds


@dataclass
class BatchConfig:
    """Base configuration for batch operations."""

    # Batch sizing
    days_per_batch: int = 7
    max_concurrent_batches: int = 3

    # Rate limiting
    delay_between_operations: float = 0.2
    max_operations_per_second: float = 5.0

    # Retry configuration
    max_retries: int = 3
    retry_backoff_factor: float = 2.0
    retry_max_delay: float = 60.0

    # Performance monitoring
    enable_performance_monitoring: bool = True
    log_individual_operations: bool = True


@dataclass
class BatchResult:
    """Base result for batch operations."""

    start_date: date
    end_date: date
    region: str
    operation_type: str
    success: bool

    records_processed: int = 0
    bytes_processed: int = 0

    # Performance metrics
    duration_seconds: float = 0.0
    operation_latency_seconds: float = 0.0
    retry_attempts: int = 0

    error_message: Optional[str] = None
    output_path: Optional[Path] = None


class BaseOrchestrator(ABC):
    """Base class for all ETL orchestrators."""

    def __init__(self, config=None):
        """Initialize base orchestrator.

        Args:
            config: Configuration object or dict
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

        # Performance tracking
        self.metrics = PerformanceMetrics()
        self.batch_config = BatchConfig()

        # Rate limiting
        self._last_operation_time = 0.0

        self.logger.info(f"{self.__class__.__name__} initialized")

    def configure_batching(self, config: BatchConfig) -> None:
        """Update batch configuration."""
        self.batch_config = config
        self.logger.info(f"Updated batch config: {config.days_per_batch} days/batch, "
                        f"{config.max_concurrent_batches} concurrent")

    def _generate_date_batches(self, start_date: date, end_date: date) -> List[Tuple[date, date]]:
        """Generate date batches based on configuration."""
        batches = []
        current_date = start_date

        while current_date <= end_date:
            batch_end = min(
                current_date + timedelta(days=self.batch_config.days_per_batch - 1),
                end_date
            )
            batches.append((current_date, batch_end))
            current_date = batch_end + timedelta(days=1)

        return batches

    async def _apply_rate_limiting(self) -> None:
        """Apply rate limiting between operations."""
        current_time = time.time()
        time_since_last_operation = current_time - self._last_operation_time

        if time_since_last_operation < self.batch_config.delay_between_operations:
            delay = self.batch_config.delay_between_operations - time_since_last_operation
            await asyncio.sleep(delay)

        self._last_operation_time = time.time()

    def _start_performance_tracking(self) -> None:
        """Initialize performance tracking."""
        self.metrics = PerformanceMetrics()
        self.metrics.start_time = datetime.now()

    def _end_performance_tracking(self) -> None:
        """Finalize performance tracking."""
        self.metrics.end_time = datetime.now()

    def _log_performance_summary(self, operation_name: str, additional_info: Dict[str, Any] = None) -> None:
        """Log a comprehensive performance summary."""

        self.logger.info("\n" + "="*80)
        self.logger.info(f"📊 {operation_name.upper()} PERFORMANCE SUMMARY")
        self.logger.info("="*80)

        # Overall metrics
        self.logger.info(f"⏱️  Duration: {self.metrics.duration_seconds:.2f} seconds")
        self.logger.info(f"📈 Operations: {self.metrics.total_operations} total, "
                        f"{self.metrics.successful_operations} successful, "
                        f"{self.metrics.failed_operations} failed")
        self.logger.info(f"⚡ Throughput: {self.metrics.operations_per_second:.2f} ops/second")
        self.logger.info(f"✅ Success Rate: {self.metrics.success_rate:.1f}%")
        self.logger.info(f"🕐 Latency: {self.metrics.average_latency_ms:.0f}ms avg, "
                        f"{self.metrics.median_latency_ms:.0f}ms median")

        # Data metrics
        self.logger.info(f"📊 Data: {self.metrics.total_records_processed:,} records, "
                        f"{self.metrics.total_bytes_processed:,} bytes")
        self.logger.info(f"🚀 Data Throughput: {self.metrics.records_per_second:.0f} records/second, "
                        f"{self.metrics.throughput_mb_per_second:.2f} MB/second")

        # Additional information
        if additional_info:
            self.logger.info(f"\n📋 Additional Information:")
            for key, value in additional_info.items():
                self.logger.info(f"   • {key}: {value}")

        # Error summary
        if self.metrics.error_messages:
            self.logger.info(f"\n❌ Errors ({len(self.metrics.error_messages)}):")
            for error in self.metrics.error_messages[:5]:  # Show first 5 errors
                self.logger.info(f"   • {error}")
            if len(self.metrics.error_messages) > 5:
                self.logger.info(f"   • ... and {len(self.metrics.error_messages) - 5} more")

        self.logger.info("="*80)

    def get_performance_metrics(self) -> PerformanceMetrics:
        """Get current performance metrics."""
        return self.metrics

    @abstractmethod
    async def process_data(self, start_date: date, end_date: date, **kwargs) -> Dict[str, Any]:
        """Process data for the given date range.

        This method must be implemented by subclasses.
        """
        pass
