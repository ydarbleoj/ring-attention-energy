"""
Pipeline benchmarking and performance monitoring.

Extracted from BaseOrchestrator to provide dedicated performance tracking
for pipeline steps and workflows. Follows single responsibility principle
by focusing solely on metrics collection and reporting.
"""

import logging
import time
import psutil
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from statistics import mean, median
from pathlib import Path


@dataclass
class BenchmarkMetrics:
    """Comprehensive performance metrics for pipeline operations."""

    # Operation metrics
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
    files_created: int = 0
    api_calls_made: int = 0

    # System resource metrics
    peak_memory_mb: Optional[float] = None
    avg_cpu_percent: Optional[float] = None
    disk_io_read_mb: Optional[float] = None
    disk_io_write_mb: Optional[float] = None

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


class PipelineBenchmark:
    """
    Dedicated performance monitoring for pipeline steps and workflows.

    Provides comprehensive metrics collection, system resource monitoring,
    and performance reporting capabilities separated from business logic.
    """

    def __init__(self, operation_name: str, enable_system_monitoring: bool = True):
        """
        Initialize benchmark tracking.

        Args:
            operation_name: Name of the operation being benchmarked
            enable_system_monitoring: Whether to track system resources
        """
        self.operation_name = operation_name
        self.enable_system_monitoring = enable_system_monitoring
        self.logger = logging.getLogger(f"PipelineBenchmark.{operation_name}")

        # Initialize metrics
        self.metrics = BenchmarkMetrics()

        # System monitoring
        self._process = psutil.Process() if enable_system_monitoring else None
        self._cpu_samples: List[float] = []
        self._memory_samples: List[float] = []
        self._initial_disk_io = None

        self.logger.info(f"Initialized benchmark for: {operation_name}")

    def start(self) -> None:
        """Start benchmark tracking."""
        self.metrics.start_time = datetime.now()

        if self.enable_system_monitoring and self._process:
            try:
                # Capture initial system state
                self._initial_disk_io = self._process.io_counters()
                self._cpu_samples.clear()
                self._memory_samples.clear()
            except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
                # AttributeError can occur on some systems where io_counters is not available
                self.logger.warning("Could not access system metrics (io_counters may not be available)")
                self.enable_system_monitoring = False

        self.logger.info(f"Started benchmarking: {self.operation_name}")

    def end(self) -> None:
        """End benchmark tracking and finalize metrics."""
        self.metrics.end_time = datetime.now()

        if self.enable_system_monitoring and self._process:
            try:
                # Capture final system state
                final_disk_io = self._process.io_counters()

                if self._initial_disk_io and final_disk_io:
                    self.metrics.disk_io_read_mb = (
                        final_disk_io.read_bytes - self._initial_disk_io.read_bytes
                    ) / (1024 * 1024)
                    self.metrics.disk_io_write_mb = (
                        final_disk_io.write_bytes - self._initial_disk_io.write_bytes
                    ) / (1024 * 1024)

                # Calculate averages
                if self._cpu_samples:
                    self.metrics.avg_cpu_percent = mean(self._cpu_samples)
                if self._memory_samples:
                    self.metrics.peak_memory_mb = max(self._memory_samples)

            except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
                self.logger.warning("Could not capture final system metrics")

        self.logger.info(f"Completed benchmarking: {self.operation_name} ({self.metrics.duration_seconds:.2f}s)")

    def record_operation(self, success: bool, latency_seconds: float, records: int = 0,
                        bytes_processed: int = 0, error_msg: Optional[str] = None) -> None:
        """
        Record a single operation.

        Args:
            success: Whether the operation succeeded
            latency_seconds: Operation latency in seconds
            records: Number of records processed
            bytes_processed: Number of bytes processed
            error_msg: Error message if operation failed
        """
        self.metrics.total_operations += 1
        self.metrics.operation_latencies.append(latency_seconds)
        self.metrics.total_records_processed += records
        self.metrics.total_bytes_processed += bytes_processed

        if success:
            self.metrics.successful_operations += 1
        else:
            self.metrics.failed_operations += 1
            if error_msg:
                self.metrics.error_messages.append(error_msg)

        # Sample system resources periodically
        if self.enable_system_monitoring and self._process and self.metrics.total_operations % 10 == 0:
            try:
                cpu_percent = self._process.cpu_percent()
                memory_mb = self._process.memory_info().rss / (1024 * 1024)

                self._cpu_samples.append(cpu_percent)
                self._memory_samples.append(memory_mb)

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

    def record_api_call(self) -> None:
        """Record an API call."""
        self.metrics.api_calls_made += 1

    def record_file_created(self, file_path: Path = None) -> None:
        """Record a file creation."""
        self.metrics.files_created += 1

    def record_retry(self) -> None:
        """Record a retry attempt."""
        self.metrics.retry_attempts += 1

    def log_progress(self, message: str, records: int = None, percentage: float = None) -> None:
        """
        Log progress with optional metrics.

        Args:
            message: Progress message
            records: Current record count
            percentage: Completion percentage
        """
        progress_parts = [message]

        if records is not None:
            progress_parts.append(f"{records:,} records")

        if percentage is not None:
            progress_parts.append(f"{percentage:.1f}% complete")

        if self.metrics.duration_seconds > 0:
            progress_parts.append(f"{self.metrics.duration_seconds:.1f}s elapsed")

        self.logger.info(" | ".join(progress_parts))

    def get_metrics(self) -> BenchmarkMetrics:
        """Get current benchmark metrics."""
        return self.metrics

    def log_summary(self, additional_info: Dict[str, Any] = None) -> None:
        """
        Log comprehensive performance summary.

        Args:
            additional_info: Additional information to include in summary
        """
        self.logger.info("\n" + "="*80)
        self.logger.info(f"📊 {self.operation_name.upper()} PERFORMANCE SUMMARY")
        self.logger.info("="*80)

        # Overall metrics
        self.logger.info(f"⏱️  Duration: {self.metrics.duration_seconds:.2f} seconds")
        self.logger.info(f"📈 Operations: {self.metrics.total_operations} total, "
                        f"{self.metrics.successful_operations} successful, "
                        f"{self.metrics.failed_operations} failed")

        if self.metrics.total_operations > 0:
            self.logger.info(f"⚡ Throughput: {self.metrics.operations_per_second:.2f} ops/second")
            self.logger.info(f"✅ Success Rate: {self.metrics.success_rate:.1f}%")
            self.logger.info(f"🕐 Latency: {self.metrics.average_latency_ms:.0f}ms avg, "
                            f"{self.metrics.median_latency_ms:.0f}ms median")

        # Data metrics
        if self.metrics.total_records_processed > 0:
            self.logger.info(f"📊 Data: {self.metrics.total_records_processed:,} records, "
                            f"{self.metrics.total_bytes_processed:,} bytes")
            self.logger.info(f"🚀 Data Throughput: {self.metrics.records_per_second:.0f} records/second, "
                            f"{self.metrics.throughput_mb_per_second:.2f} MB/second")

        # File and API metrics
        if self.metrics.files_created > 0 or self.metrics.api_calls_made > 0:
            self.logger.info(f"📁 Files Created: {self.metrics.files_created}")
            self.logger.info(f"🌐 API Calls: {self.metrics.api_calls_made}")

        # System resource metrics
        if self.enable_system_monitoring:
            if self.metrics.peak_memory_mb:
                self.logger.info(f"💾 Peak Memory: {self.metrics.peak_memory_mb:.1f} MB")
            if self.metrics.avg_cpu_percent:
                self.logger.info(f"⚙️  Avg CPU: {self.metrics.avg_cpu_percent:.1f}%")
            if self.metrics.disk_io_read_mb or self.metrics.disk_io_write_mb:
                self.logger.info(f"💿 Disk I/O: {self.metrics.disk_io_read_mb:.1f} MB read, "
                                f"{self.metrics.disk_io_write_mb:.1f} MB write")

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

    def save_metrics(self, output_path: Path) -> None:
        """
        Save metrics to JSON file.

        Args:
            output_path: Path to save metrics JSON
        """
        import json
        from dataclasses import asdict

        metrics_dict = asdict(self.metrics)

        # Convert datetime objects to strings
        if metrics_dict['start_time']:
            metrics_dict['start_time'] = metrics_dict['start_time'].isoformat()
        if metrics_dict['end_time']:
            metrics_dict['end_time'] = metrics_dict['end_time'].isoformat()

        # Add operation metadata
        metrics_dict['operation_name'] = self.operation_name
        metrics_dict['benchmark_version'] = "1.0"

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump(metrics_dict, f, indent=2, default=str)

        self.logger.info(f"Saved benchmark metrics to: {output_path}")


def benchmark_step(step_name: str, enable_system_monitoring: bool = True):
    """
    Decorator for benchmarking step execution.

    Args:
        step_name: Name of the step being benchmarked
        enable_system_monitoring: Whether to track system resources

    Returns:
        Decorated function with automatic benchmarking
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            benchmark = PipelineBenchmark(step_name, enable_system_monitoring)
            benchmark.start()

            try:
                result = func(*args, **kwargs)
                benchmark.record_operation(
                    success=True,
                    latency_seconds=benchmark.metrics.duration_seconds
                )
                return result
            except Exception as e:
                benchmark.record_operation(
                    success=False,
                    latency_seconds=benchmark.metrics.duration_seconds,
                    error_msg=str(e)
                )
                raise
            finally:
                benchmark.end()
                benchmark.log_summary()

        return wrapper
    return decorator
