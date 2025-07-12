"""Batch transformation orchestrator for processing all JSON files to Parquet.

High-performance orchestrator that processes the complete dataset of 548 JSON files
using parallel processing, progress monitoring, and comprehensive error handling.
Follows the proven patterns from the Extract orchestrator.
"""

import asyncio
import time
import json
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from ..services.transform_service import EIATransformService
from .base import BaseOrchestrator, BatchConfig, BatchResult, PerformanceMetrics


@dataclass
class BatchTransformConfig(BatchConfig):
    """Configuration for batch transform operations."""

    # Transform-specific settings
    files_per_batch: int = 10  # Process files in batches
    max_concurrent_batches: int = 8  # High concurrency for file I/O
    validate_data: bool = True  # Enable data quality validation
    skip_empty_files: bool = True  # Skip files with 0 records
    overwrite_existing: bool = False  # Skip existing output files

    # Performance settings
    progress_update_interval: int = 25  # Update progress every N files
    enable_performance_tracking: bool = True


@dataclass
class BatchTransformResult:
    """Result of a batch transform operation."""

    # Core result info
    success: bool
    error_message: Optional[str] = None

    # File processing results
    input_file: Optional[Path] = None
    output_file: Optional[Path] = None
    input_records: int = 0
    output_records: int = 0
    processing_time_seconds: float = 0.0
    file_size_bytes: int = 0

    # Data quality metrics
    data_quality_score: float = 1.0
    records_dropped: int = 0
    quality_issues: Optional[List[str]] = None

    # File categorization
    was_empty: bool = False
    was_skipped: bool = False


class BatchTransformOrchestrator(BaseOrchestrator):
    """Orchestrator for batch transformation of JSON files to Parquet."""

    def __init__(
        self,
        raw_data_path: str = "data/raw/eia",
        interim_data_path: str = "data/interim",
        config: Optional[BatchTransformConfig] = None
    ):
        """Initialize the batch transform orchestrator.

        Args:
            raw_data_path: Path to raw JSON files
            interim_data_path: Path to store interim Parquet files
            config: Optional batch configuration
        """
        from ...integrations.config import get_config
        base_config = get_config()
        super().__init__(base_config)

        self.raw_data_path = Path(raw_data_path)
        self.interim_data_path = Path(interim_data_path)

        # Create output directory
        self.interim_data_path.mkdir(parents=True, exist_ok=True)

        # Use custom or default batch config
        self.batch_config = config or BatchTransformConfig()

        # Initialize transform service
        self.transform_service = EIATransformService()

        # Tracking variables
        self.total_files_found = 0
        self.files_processed = 0
        self.files_skipped = 0
        self.files_failed = 0
        self.total_records_processed = 0
        self.empty_files_found = 0

        self.logger.info(f"BatchTransformOrchestrator initialized")
        self.logger.info(f"  Raw data path: {self.raw_data_path}")
        self.logger.info(f"  Interim path: {self.interim_data_path}")
        self.logger.info(f"  Batch config: {self.batch_config.files_per_batch} files/batch, {self.batch_config.max_concurrent_batches} concurrent")

    def transform_all_files(
        self,
        file_pattern: str = "**/*.json",
        regions: Optional[List[str]] = None,
        data_types: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Dict[str, Any]:
        """Transform all JSON files to Parquet format.

        Args:
            file_pattern: Glob pattern for finding JSON files
            regions: Filter by specific regions (e.g., ['PACW', 'ERCO'])
            data_types: Filter by data types (e.g., ['demand', 'generation'])
            years: Filter by specific years (e.g., [2024, 2025])

        Returns:
            Dictionary with comprehensive results and performance metrics
        """
        print(f"🚀 Starting batch transformation of EIA data")
        print(f"   Raw data path: {self.raw_data_path}")
        print(f"   Output path: {self.interim_data_path}")
        print(f"   Configuration: {self.batch_config.files_per_batch} files/batch, {self.batch_config.max_concurrent_batches} workers")

        # Start performance tracking
        if self.batch_config.enable_performance_tracking:
            self._start_performance_tracking()

        overall_start_time = time.time()

        try:
            # Discover all files to process
            files_to_process = self._discover_files(file_pattern, regions, data_types, years)
            self.total_files_found = len(files_to_process)

            if self.total_files_found == 0:
                print("❌ No files found matching criteria")
                return self._create_summary_result(overall_start_time, success=False, error="No files found")

            print(f"📁 Found {self.total_files_found:,} files to process")

            # Process files in batches using ThreadPoolExecutor
            all_results = self._process_files_parallel(files_to_process)

            # Create comprehensive summary
            summary = self._create_summary_result(overall_start_time, success=True, results=all_results)

            # Print final results
            self._print_final_summary(summary)

            return summary

        except Exception as e:
            self.logger.error(f"Batch transformation failed: {str(e)}")
            return self._create_summary_result(overall_start_time, success=False, error=str(e))

    def _discover_files(
        self,
        file_pattern: str,
        regions: Optional[List[str]],
        data_types: Optional[List[str]],
        years: Optional[List[int]]
    ) -> List[Path]:
        """Discover all JSON files matching the criteria."""

        print(f"🔍 Discovering files with pattern: {file_pattern}")

        # Find all JSON files
        all_files = list(self.raw_data_path.glob(file_pattern))

        # Filter files based on criteria
        filtered_files = []

        for file_path in all_files:
            # Parse filename for metadata
            filename = file_path.name

            # Skip if not EIA file format
            if not filename.startswith("eia_"):
                continue

            # Extract components from filename
            # Format: eia_{data_type}_{region}_{start_date}_to_{end_date}_{timestamp}.json
            try:
                parts = filename.replace(".json", "").split("_")
                if len(parts) >= 4:
                    file_data_type = parts[1]  # demand or generation
                    file_region = parts[2]     # PACW, ERCO, etc.

                    # Extract year from date part
                    date_part = parts[3]  # Should be start date like "2024-01-20"
                    file_year = int(date_part.split("-")[0])

                    # Apply filters
                    if regions and file_region not in regions:
                        continue
                    if data_types and file_data_type not in data_types:
                        continue
                    if years and file_year not in years:
                        continue

                    filtered_files.append(file_path)

            except (IndexError, ValueError) as e:
                self.logger.warning(f"Could not parse filename: {filename} - {e}")
                continue

        # Sort for consistent processing order
        filtered_files.sort()

        if regions:
            print(f"   Filtered by regions: {regions}")
        if data_types:
            print(f"   Filtered by data types: {data_types}")
        if years:
            print(f"   Filtered by years: {years}")

        print(f"   Found {len(filtered_files)} matching files")

        return filtered_files

    def _process_files_parallel(self, files_to_process: List[Path]) -> List[BatchTransformResult]:
        """Process files in parallel using ThreadPoolExecutor."""

        all_results = []

        # Create batches of files
        file_batches = self._create_file_batches(files_to_process)

        print(f"📦 Processing {len(file_batches)} batches with {self.batch_config.max_concurrent_batches} workers")

        with ThreadPoolExecutor(max_workers=self.batch_config.max_concurrent_batches) as executor:
            # Submit all batches
            future_to_batch = {
                executor.submit(self._process_file_batch, batch_files): batch_idx
                for batch_idx, batch_files in enumerate(file_batches)
            }

            # Process completed batches
            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]

                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)

                    # Update progress
                    self.files_processed += len(batch_results)

                    if self.files_processed % self.batch_config.progress_update_interval == 0:
                        progress_pct = (self.files_processed / self.total_files_found) * 100
                        print(f"📊 Progress: {self.files_processed}/{self.total_files_found} files ({progress_pct:.1f}%)")

                except Exception as e:
                    self.logger.error(f"Batch {batch_idx} failed: {str(e)}")
                    self.files_failed += len(file_batches[batch_idx])

        return all_results

    def _create_file_batches(self, files_to_process: List[Path]) -> List[List[Path]]:
        """Group files into processing batches."""

        batches = []
        batch_size = self.batch_config.files_per_batch

        for i in range(0, len(files_to_process), batch_size):
            batch = files_to_process[i:i + batch_size]
            batches.append(batch)

        return batches

    def _process_file_batch(self, batch_files: List[Path]) -> List[BatchTransformResult]:
        """Process a single batch of files."""

        batch_results = []

        for file_path in batch_files:
            result = self._process_single_file(file_path)
            batch_results.append(result)

        return batch_results

    def _process_single_file(self, input_file: Path) -> BatchTransformResult:
        """Process a single JSON file to Parquet."""

        start_time = time.time()

        try:
            # Generate output filename
            output_file = self._generate_output_filename(input_file)

            # Check if output already exists and skip if configured
            if output_file.exists() and not self.batch_config.overwrite_existing:
                return BatchTransformResult(
                    success=True,
                    input_file=input_file,
                    output_file=output_file,
                    was_skipped=True,
                    processing_time_seconds=time.time() - start_time
                )

            # Quick check for empty files
            if self.batch_config.skip_empty_files:
                if self._is_empty_file(input_file):
                    self.empty_files_found += 1
                    return BatchTransformResult(
                        success=True,
                        input_file=input_file,
                        was_empty=True,
                        was_skipped=True,
                        processing_time_seconds=time.time() - start_time
                    )

            # Transform the file
            transform_result = self.transform_service.transform_json_to_parquet(
                json_file_path=input_file,
                output_path=output_file,
                validate_data=self.batch_config.validate_data
            )

            # Convert to BatchTransformResult
            if transform_result["success"]:
                # Update counters
                self.total_records_processed += transform_result["output_records"]

                quality_data = transform_result.get("data_quality", {})

                return BatchTransformResult(
                    success=True,
                    input_file=input_file,
                    output_file=output_file,
                    input_records=transform_result["input_records"],
                    output_records=transform_result["output_records"],
                    processing_time_seconds=transform_result["processing_time_seconds"],
                    file_size_bytes=transform_result["file_size_bytes"],
                    data_quality_score=quality_data.get("data_quality_score", 1.0),
                    records_dropped=quality_data.get("records_dropped", 0),
                    quality_issues=quality_data.get("quality_issues", [])
                )
            else:
                return BatchTransformResult(
                    success=False,
                    input_file=input_file,
                    error_message=transform_result["error"],
                    processing_time_seconds=transform_result["processing_time_seconds"]
                )

        except Exception as e:
            return BatchTransformResult(
                success=False,
                input_file=input_file,
                error_message=str(e),
                processing_time_seconds=time.time() - start_time
            )

    def _generate_output_filename(self, input_file: Path) -> Path:
        """Generate output Parquet filename from input JSON filename.

        For year-based structure, we'll collect files by year and combine them later.
        For now, still create individual files but organize by year.
        """

        # Extract year from input filename
        # Format: eia_demand_PACW_2024-01-20_to_2024-03-04_20250711_160836.json
        input_name = input_file.stem  # Remove .json extension

        try:
            # Parse year from filename
            parts = input_name.split("_")
            if len(parts) >= 4:
                date_part = parts[3]  # Should be start date like "2024-01-20"
                year = date_part.split("-")[0]
            else:
                year = "unknown"
        except (IndexError, ValueError):
            year = "unknown"

        # Remove timestamp suffix (pattern: _YYYYMMDD_HHMMSS)
        if len(parts) >= 7:
            # Keep everything except the last part (timestamp)
            clean_name = "_".join(parts[:-1])
        else:
            clean_name = input_name

        # Organize by year subdirectory for now (we'll combine later)
        year_dir = self.interim_data_path / year
        year_dir.mkdir(exist_ok=True)

        output_filename = f"{clean_name}.parquet"
        return year_dir / output_filename

    def _is_empty_file(self, file_path: Path) -> bool:
        """Quick check if a JSON file has no data records."""

        try:
            with open(file_path, 'r') as f:
                data = json.load(f)

            metadata = data.get('metadata', {})
            record_count = metadata.get('record_count', -1)

            # Use metadata if available
            if record_count is not None:
                return record_count == 0

            # Fallback: check actual data
            api_response = data.get('api_response', {}).get('response', {})
            records = api_response.get('data', [])
            return len(records) == 0

        except Exception:
            # If we can't parse, assume it's not empty and let the transform handle it
            return False

    def _create_summary_result(
        self,
        overall_start_time: float,
        success: bool = True,
        results: Optional[List[BatchTransformResult]] = None,
        error: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create comprehensive summary of the batch transformation."""

        total_time = time.time() - overall_start_time

        if not success:
            return {
                "success": False,
                "error": error,
                "total_time_seconds": total_time,
                "files_found": self.total_files_found
            }

        results = results or []

        # Categorize results
        successful_results = [r for r in results if r.success and not r.was_skipped]
        failed_results = [r for r in results if not r.success]
        skipped_results = [r for r in results if r.was_skipped]
        empty_files = [r for r in results if r.was_empty]

        # Calculate totals
        total_input_records = sum(r.input_records for r in successful_results)
        total_output_records = sum(r.output_records for r in successful_results)
        total_file_size = sum(r.file_size_bytes for r in successful_results)
        total_processing_time = sum(r.processing_time_seconds for r in results)

        # Calculate rates
        files_per_second = len(successful_results) / total_time if total_time > 0 else 0
        records_per_second = total_output_records / total_time if total_time > 0 else 0

        # Performance metrics
        if self.batch_config.enable_performance_tracking:
            self._end_performance_tracking()
            performance_metrics = {
                "files_per_second": files_per_second,
                "records_per_second": records_per_second,
                "average_processing_time_per_file": total_processing_time / len(results) if results else 0,
                "total_file_size_mb": total_file_size / (1024 * 1024),
                "compression_ratio": total_input_records / total_output_records if total_output_records > 0 else 1.0
            }
        else:
            performance_metrics = {}

        return {
            "success": True,
            "total_time_seconds": total_time,
            "files_found": self.total_files_found,
            "files_processed": len(successful_results),
            "files_failed": len(failed_results),
            "files_skipped": len(skipped_results),
            "empty_files_found": len(empty_files),
            "total_input_records": total_input_records,
            "total_output_records": total_output_records,
            "total_file_size_bytes": total_file_size,
            "performance_metrics": performance_metrics,
            "successful_files": [str(r.output_file) for r in successful_results],
            "failed_files": [(str(r.input_file), r.error_message) for r in failed_results],
            "data_quality": {
                "average_quality_score": sum(r.data_quality_score for r in successful_results) / len(successful_results) if successful_results else 1.0,
                "total_records_dropped": sum(r.records_dropped for r in successful_results),
                "files_with_quality_issues": len([r for r in successful_results if r.quality_issues])
            }
        }

    def _print_final_summary(self, summary: Dict[str, Any]):
        """Print comprehensive final summary."""

        print(f"\n🎉 BATCH TRANSFORMATION COMPLETE")
        print(f"=" * 50)
        print(f"⏱️  Total time: {summary['total_time_seconds']:.1f} seconds ({summary['total_time_seconds']/60:.1f} minutes)")
        print(f"📁 Files found: {summary['files_found']:,}")
        print(f"✅ Files processed: {summary['files_processed']:,}")
        print(f"❌ Files failed: {summary['files_failed']:,}")
        print(f"⏭️  Files skipped: {summary['files_skipped']:,}")
        print(f"📭 Empty files: {summary['empty_files_found']:,}")
        print()

        print(f"📊 DATA METRICS:")
        print(f"  • Input records: {summary['total_input_records']:,}")
        print(f"  • Output records: {summary['total_output_records']:,}")
        print(f"  • File size: {summary['total_file_size_bytes'] / (1024*1024):.1f} MB")
        print()

        if summary.get('performance_metrics'):
            perf = summary['performance_metrics']
            print(f"⚡ PERFORMANCE:")
            print(f"  • Files/second: {perf['files_per_second']:.1f}")
            print(f"  • Records/second: {perf['records_per_second']:,.0f}")
            print(f"  • Avg time/file: {perf['average_processing_time_per_file']:.3f}s")

        quality = summary.get('data_quality', {})
        print(f"\n🔍 DATA QUALITY:")
        print(f"  • Average quality score: {quality.get('average_quality_score', 0):.1%}")
        print(f"  • Records dropped: {quality.get('total_records_dropped', 0):,}")
        print(f"  • Files with issues: {quality.get('files_with_quality_issues', 0)}")

        if summary['files_failed'] > 0:
            print(f"\n❌ FAILED FILES:")
            for file_path, error in summary['failed_files'][:5]:  # Show first 5
                print(f"  • {Path(file_path).name}: {error}")
            if len(summary['failed_files']) > 5:
                print(f"  • ... and {len(summary['failed_files']) - 5} more")

    # Required method from base class
    async def process_data(self, start_date: date, end_date: date, **kwargs) -> Dict[str, Any]:
        """Legacy method for compatibility."""
        # Convert to sync call
        return self.transform_all_files(**kwargs)
