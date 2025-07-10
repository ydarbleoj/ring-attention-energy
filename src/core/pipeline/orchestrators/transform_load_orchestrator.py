"""Transform and Load stage orchestrator for the ETL pipeline.

This orchestrator handles Phase 2 & 3: Transform and Load stages
- Transforms raw JSON files from data/raw/ to cleaned Parquet in data/interim/
- Loads interim data and combines multiple sources into data/processed/
- Uses Polars for high-performance data processing
- Provides validation and quality metrics
"""

import asyncio
import time
import json
from datetime import date, datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from .base import BaseOrchestrator, BatchConfig, BatchResult
from ...integrations.config import get_config


@dataclass
class TransformLoadBatchConfig(BatchConfig):
    """Configuration for transform and load operations."""

    # Override defaults for transform/load operations
    days_per_batch: int = 30  # Larger batches for file processing
    max_concurrent_batches: int = 4  # More concurrency for file I/O

    # Transform/Load specific settings
    validate_schema: bool = True
    create_data_quality_report: bool = True
    compression_level: str = "snappy"  # Parquet compression
    write_statistics: bool = True


@dataclass
class TransformLoadBatchResult(BatchResult):
    """Result of transform and load batch operation."""

    transform_stage: str = "unknown"  # "raw_to_interim" or "interim_to_processed"

    # File paths
    input_files: List[Path] = None
    output_file: Optional[Path] = None

    # Data quality metrics
    validation_errors: List[str] = None
    data_quality_score: float = 0.0


class TransformLoadOrchestrator(BaseOrchestrator):
    """Orchestrator for Transform and Load stages of the ETL pipeline."""

    def __init__(
        self,
        raw_data_path: str = "data/raw",
        interim_data_path: str = "data/interim",
        processed_data_path: str = "data/processed",
        config_file: Optional[str] = None
    ):
        """Initialize the transform/load orchestrator.

        Args:
            raw_data_path: Path to raw data files
            interim_data_path: Path to store interim processed files
            processed_data_path: Path to store final processed files
            config_file: Optional config file path (unused for now)
        """
        config = get_config()
        super().__init__(config)

        self.raw_data_path = Path(raw_data_path)
        self.interim_data_path = Path(interim_data_path)
        self.processed_data_path = Path(processed_data_path)

        # Create directories
        self.interim_data_path.mkdir(parents=True, exist_ok=True)
        self.processed_data_path.mkdir(parents=True, exist_ok=True)

        # Use transform/load specific batch config
        self.batch_config = TransformLoadBatchConfig()

        self.logger.info(f"TransformLoadOrchestrator initialized")
        self.logger.info(f"  Raw: {self.raw_data_path}")
        self.logger.info(f"  Interim: {self.interim_data_path}")
        self.logger.info(f"  Processed: {self.processed_data_path}")

    async def process_data(
        self,
        start_date: date,
        end_date: date,
        region: str = "PACW",
        data_types: Optional[List[str]] = None,
        stage: str = "raw_to_interim"
    ) -> Dict[str, List[TransformLoadBatchResult]]:
        """Process data through transform and load stages.

        Args:
            start_date: Start date for processing
            end_date: End date for processing
            region: Region code
            data_types: List of data types to process
            stage: Processing stage ("raw_to_interim" or "interim_to_processed")

        Returns:
            Dictionary mapping data types to their processing results
        """
        if data_types is None:
            data_types = ["demand", "generation"]

        self.logger.info(f"Starting {stage} processing: {start_date} to {end_date}")
        self.logger.info(f"Region: {region}, Data types: {data_types}")

        # Initialize performance tracking
        self._start_performance_tracking()

        try:
            if stage == "raw_to_interim":
                results = await self._transform_raw_to_interim(start_date, end_date, region, data_types)
            elif stage == "interim_to_processed":
                results = await self._load_interim_to_processed(start_date, end_date, region, data_types)
            else:
                raise ValueError(f"Unknown stage: {stage}")

            self._end_performance_tracking()

            # Log performance summary
            additional_info = {
                "Stage": stage,
                "Region": region,
                "Data Types": ", ".join(data_types),
                "Date Range": f"{start_date} to {end_date}",
                "Total Batches": sum(len(batch_results) for batch_results in results.values())
            }
            self._log_performance_summary(f"Transform/Load ({stage})", additional_info)

            return results

        except Exception as e:
            self._end_performance_tracking()
            self.logger.error(f"Transform/Load orchestration failed: {e}")
            raise

    async def _transform_raw_to_interim(
        self,
        start_date: date,
        end_date: date,
        region: str,
        data_types: List[str]
    ) -> Dict[str, List[TransformLoadBatchResult]]:
        """Transform raw JSON files to interim Parquet files."""

        results = {}

        for data_type in data_types:
            self.logger.info(f"Transforming {data_type} data from raw to interim...")

            # Find raw files for this data type and date range
            raw_files = self._find_raw_files(data_type, region, start_date, end_date)

            if not raw_files:
                self.logger.warning(f"No raw files found for {data_type} in date range")
                results[data_type] = []
                continue

            # Process files in batches
            file_batches = self._batch_files(raw_files)
            batch_results = []

            for file_batch in file_batches:
                result = await self._transform_file_batch(file_batch, data_type, region)
                batch_results.append(result)

            results[data_type] = batch_results

        return results

    async def _load_interim_to_processed(
        self,
        start_date: date,
        end_date: date,
        region: str,
        data_types: List[str]
    ) -> Dict[str, List[TransformLoadBatchResult]]:
        """Load interim files and combine into processed files."""

        results = {}

        # For now, implement a simple aggregation approach
        # In future, this could combine multiple sources (EIA + CAISO)

        for data_type in data_types:
            self.logger.info(f"Loading {data_type} data from interim to processed...")

            # Find interim files
            interim_files = self._find_interim_files(data_type, region, start_date, end_date)

            if not interim_files:
                self.logger.warning(f"No interim files found for {data_type}")
                results[data_type] = []
                continue

            # Combine interim files into processed format
            result = await self._combine_interim_files(interim_files, data_type, region, start_date, end_date)
            results[data_type] = [result] if result else []

        return results

    def _find_raw_files(
        self,
        data_type: str,
        region: str,
        start_date: date,
        end_date: date
    ) -> List[Path]:
        """Find raw JSON files matching criteria."""

        files = []

        # Search in the EIA raw data directory structure
        eia_raw_path = self.raw_data_path / "eia"

        if not eia_raw_path.exists():
            return files

        # Search through year directories
        for year_dir in eia_raw_path.iterdir():
            if not year_dir.is_dir():
                continue

            # Look for files matching pattern: eia_{data_type}_{region}_*.json
            pattern = f"eia_{data_type}_{region}_*.json"
            matching_files = list(year_dir.glob(pattern))

            # Filter by date range (extract from filename or file metadata)
            for file_path in matching_files:
                if self._file_in_date_range(file_path, start_date, end_date):
                    files.append(file_path)

        return sorted(files)

    def _find_interim_files(
        self,
        data_type: str,
        region: str,
        start_date: date,
        end_date: date
    ) -> List[Path]:
        """Find interim Parquet files matching criteria."""

        pattern = f"eia_{data_type}_{region}_*.parquet"
        files = list(self.interim_data_path.glob(f"**/{pattern}"))

        # Filter by date range
        filtered_files = []
        for file_path in files:
            if self._file_in_date_range(file_path, start_date, end_date):
                filtered_files.append(file_path)

        return sorted(filtered_files)

    def _file_in_date_range(self, file_path: Path, start_date: date, end_date: date) -> bool:
        """Check if file contains data in the specified date range."""
        # For now, use a simple heuristic based on filename
        # In production, this could check file metadata or contents

        filename = file_path.stem

        # Extract date components from filename (assuming format includes dates)
        try:
            # Look for date patterns in filename
            parts = filename.split('_')
            for part in parts:
                if len(part) == 10 and part.count('-') == 2:  # YYYY-MM-DD format
                    file_date = datetime.strptime(part, '%Y-%m-%d').date()
                    if start_date <= file_date <= end_date:
                        return True
        except:
            pass

        # Fallback: include all files (let the processing stage filter)
        return True

    def _batch_files(self, files: List[Path]) -> List[List[Path]]:
        """Group files into processing batches."""

        # Simple batching: group by similar date ranges or file size
        # For now, process individual files or small groups

        batch_size = 5  # Process up to 5 files per batch
        batches = []

        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            batches.append(batch)

        return batches

    async def _transform_file_batch(
        self,
        raw_files: List[Path],
        data_type: str,
        region: str
    ) -> TransformLoadBatchResult:
        """Transform a batch of raw files to interim format."""

        batch_start_time = time.time()

        try:
            self.metrics.total_operations += 1
            operation_start_time = time.time()

            # Load and combine raw JSON files
            all_records = []
            total_bytes = 0

            for raw_file in raw_files:
                try:
                    with open(raw_file, 'r') as f:
                        raw_package = json.load(f)

                    total_bytes += raw_file.stat().st_size

                    # Extract data from API response
                    api_response = raw_package.get("api_response", {})
                    response_data = api_response.get("response", {})
                    data = response_data.get("data", [])

                    all_records.extend(data)

                except Exception as e:
                    self.logger.error(f"Failed to load raw file {raw_file}: {e}")
                    continue

            if not all_records:
                raise ValueError("No valid records found in raw files")

            # Convert to Polars DataFrame
            df = pl.DataFrame(all_records)

            # Apply transformations based on data type
            df_transformed = self._apply_transformations(df, data_type)

            # Generate output filename
            start_date = datetime.now().date()  # Extract from actual data
            output_file = self._generate_interim_filename(data_type, region, start_date)
            output_path = self.interim_data_path / output_file

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save as Parquet
            df_transformed.write_parquet(
                output_path,
                compression=self.batch_config.compression_level,
                statistics=self.batch_config.write_statistics
            )

            operation_end_time = time.time()
            operation_latency = operation_end_time - operation_start_time

            # Update metrics
            self.metrics.successful_operations += 1
            self.metrics.operation_latencies.append(operation_latency)
            self.metrics.total_records_processed += len(df_transformed)
            self.metrics.total_bytes_processed += total_bytes

            batch_duration = time.time() - batch_start_time

            self.logger.info(f"✅ Transformed {len(raw_files)} {data_type} files: "
                           f"{len(df_transformed):,} records → {output_file}")

            return TransformLoadBatchResult(
                start_date=start_date,
                end_date=start_date,  # Will be updated with actual range
                region=region,
                operation_type="transform",
                transform_stage="raw_to_interim",
                success=True,
                input_files=raw_files,
                output_file=output_path,
                records_processed=len(df_transformed),
                bytes_processed=total_bytes,
                duration_seconds=batch_duration,
                operation_latency_seconds=operation_latency,
                output_path=output_path
            )

        except Exception as e:
            self.metrics.failed_operations += 1
            batch_duration = time.time() - batch_start_time
            error_msg = str(e)
            self.metrics.error_messages.append(error_msg)

            self.logger.error(f"❌ Transform batch failed: {e}")

            return TransformLoadBatchResult(
                start_date=date.today(),
                end_date=date.today(),
                region=region,
                operation_type="transform",
                transform_stage="raw_to_interim",
                success=False,
                input_files=raw_files,
                duration_seconds=batch_duration,
                error_message=error_msg
            )

    def _apply_transformations(self, df: pl.DataFrame, data_type: str) -> pl.DataFrame:
        """Apply data transformations specific to data type."""

        try:
            # Common transformations
            df_clean = df.clone()

            # Parse timestamp
            if "period" in df_clean.columns:
                df_clean = df_clean.with_columns([
                    pl.col("period").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%SZ").alias("timestamp")
                ])

            # Add metadata columns
            df_clean = df_clean.with_columns([
                pl.lit(data_type).alias("data_type"),
                pl.lit(datetime.now()).alias("processed_at")
            ])

            # Data type specific transformations
            if data_type == "demand":
                df_clean = self._transform_demand_data(df_clean)
            elif data_type == "generation":
                df_clean = self._transform_generation_data(df_clean)

            # Validation
            if self.batch_config.validate_schema:
                self._validate_schema(df_clean, data_type)

            return df_clean

        except Exception as e:
            self.logger.error(f"Transformation failed: {e}")
            raise

    def _transform_demand_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply demand-specific transformations."""

        # Rename value column to demand
        if "value" in df.columns:
            df = df.rename({"value": "demand"})

        # Ensure demand values are positive
        if "demand" in df.columns:
            df = df.filter(pl.col("demand") > 0)

        return df

    def _transform_generation_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply generation-specific transformations."""

        # Rename value column to generation
        if "value" in df.columns:
            df = df.rename({"value": "generation"})

        # Ensure generation values are non-negative
        if "generation" in df.columns:
            df = df.filter(pl.col("generation") >= 0)

        return df

    def _validate_schema(self, df: pl.DataFrame, data_type: str) -> None:
        """Validate DataFrame schema."""

        required_columns = ["timestamp", data_type, "data_type", "processed_at"]

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Check for empty DataFrame
        if len(df) == 0:
            raise ValueError("DataFrame is empty after transformations")

    def _generate_interim_filename(self, data_type: str, region: str, date_ref: date) -> str:
        """Generate interim filename."""

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"eia_{data_type}_{region}_{date_ref.year}_{timestamp}.parquet"

    async def _combine_interim_files(
        self,
        interim_files: List[Path],
        data_type: str,
        region: str,
        start_date: date,
        end_date: date
    ) -> Optional[TransformLoadBatchResult]:
        """Combine interim files into processed format."""

        if not interim_files:
            return None

        batch_start_time = time.time()

        try:
            self.metrics.total_operations += 1
            operation_start_time = time.time()

            # Load and combine all interim files
            dfs = []
            total_bytes = 0

            for interim_file in interim_files:
                df = pl.read_parquet(interim_file)
                dfs.append(df)
                total_bytes += interim_file.stat().st_size

            # Combine all DataFrames
            combined_df = pl.concat(dfs)

            # Sort by timestamp
            if "timestamp" in combined_df.columns:
                combined_df = combined_df.sort("timestamp")

            # Generate output filename
            output_file = f"eia_{data_type}_{region}_{start_date.year}_processed.parquet"
            output_path = self.processed_data_path / region / str(start_date.year) / output_file

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save combined file
            combined_df.write_parquet(
                output_path,
                compression=self.batch_config.compression_level,
                statistics=self.batch_config.write_statistics
            )

            operation_end_time = time.time()
            operation_latency = operation_end_time - operation_start_time

            # Update metrics
            self.metrics.successful_operations += 1
            self.metrics.operation_latencies.append(operation_latency)
            self.metrics.total_records_processed += len(combined_df)
            self.metrics.total_bytes_processed += total_bytes

            batch_duration = time.time() - batch_start_time

            self.logger.info(f"✅ Combined {len(interim_files)} {data_type} interim files: "
                           f"{len(combined_df):,} records → {output_file}")

            return TransformLoadBatchResult(
                start_date=start_date,
                end_date=end_date,
                region=region,
                operation_type="load",
                transform_stage="interim_to_processed",
                success=True,
                input_files=interim_files,
                output_file=output_path,
                records_processed=len(combined_df),
                bytes_processed=total_bytes,
                duration_seconds=batch_duration,
                operation_latency_seconds=operation_latency,
                output_path=output_path
            )

        except Exception as e:
            self.metrics.failed_operations += 1
            batch_duration = time.time() - batch_start_time
            error_msg = str(e)
            self.metrics.error_messages.append(error_msg)

            self.logger.error(f"❌ Load batch failed: {e}")

            return TransformLoadBatchResult(
                start_date=start_date,
                end_date=end_date,
                region=region,
                operation_type="load",
                transform_stage="interim_to_processed",
                success=False,
                input_files=interim_files,
                duration_seconds=batch_duration,
                error_message=error_msg
            )
