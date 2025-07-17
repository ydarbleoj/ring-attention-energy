"""
Data Cleaner Step for JSON to Parquet conversion.

Transforms all raw JSON files from a data source into a single consolidated
Parquet file for downstream ML processing.

Follows the contract:
- Input: data/raw/{source}/**/*.json files
- Output: data/interim/{source}_{signature}.parquet (single consolidated file)
"""

import hashlib
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import polars as pl

from pydantic import Field, field_validator

from ..base import BaseStep, StepConfig, StepOutput, StepMetrics


class DataCleanerStepConfig(StepConfig):
    """Configuration for data cleaning step."""

    source: str = Field(..., description="Data source name (e.g., 'eia', 'caiso')")
    raw_data_dir: Path = Field(
        default=Path("data/raw"),
        description="Directory containing raw JSON files"
    )
    interim_data_dir: Path = Field(
        default=Path("data/interim"),
        description="Directory for output Parquet files"
    )
    validate_data: bool = Field(
        default=True,
        description="Whether to perform data quality validation"
    )

    @field_validator("source")
    @classmethod
    def validate_source(cls, v):
        """Validate source is supported."""
        supported_sources = {"eia", "caiso", "synthetic"}
        if v not in supported_sources:
            raise ValueError(f"Source must be one of {supported_sources}, got {v}")
        return v


class DataCleanerStep(BaseStep):
    """
    Generic cleaning step that consolidates all raw JSON files into single Parquet.

    Uses source-specific transform services to handle different JSON schemas
    and data structures while maintaining consistent output format.
    """

    def __init__(self, config: DataCleanerStepConfig):
        super().__init__(config)
        self.config: DataCleanerStepConfig = config

        # Initialize source-specific transform service
        self.transform_service = self._get_transform_service()

        # Track processing stats
        self.files_processed = 0
        self.total_records_input = 0
        self.total_records_output = 0
        self.processing_errors = []

    def validate_input(self, config: DataCleanerStepConfig) -> None:
        """Validate transform step configuration."""
        if not config.raw_data_dir.exists():
            raise ValueError(f"Raw data directory does not exist: {config.raw_data_dir}")

        # Check if there are any JSON files to process
        source_dir = config.raw_data_dir / config.source
        if not source_dir.exists():
            raise ValueError(f"Source directory does not exist: {source_dir}")

        json_files = list(source_dir.rglob("*.json"))
        if not json_files:
            raise ValueError(f"No JSON files found in {source_dir}")

        self.logger.info(f"Found {len(json_files)} JSON files to process")

    def _get_transform_service(self):
        """Get source-specific transform service."""
        if self.config.source == "eia":
            try:
                # Try multiple import paths
                try:
                    from src.core.integrations.eia.services.transform import EIATransformService
                except ImportError:
                    from core.integrations.eia.services.transform import EIATransformService
                return EIATransformService()
            except ImportError as e:
                raise ImportError(f"Could not import EIATransformService: {e}")
        elif self.config.source == "caiso":
            # TODO: Implement CAISO transform service
            raise NotImplementedError("CAISO transform service not yet implemented")
        elif self.config.source == "synthetic":
            # TODO: Implement synthetic transform service
            raise NotImplementedError("Synthetic transform service not yet implemented")
        else:
            raise ValueError(f"Unsupported source: {self.config.source}")

    def _find_json_files(self) -> List[Path]:
        """Find all JSON files for the specified source."""
        source_dir = self.config.raw_data_dir / self.config.source
        json_files = list(source_dir.rglob("*.json"))

        # Sort by filename for consistent processing order
        json_files.sort(key=lambda p: p.name)

        self.logger.info(f"Found {len(json_files)} JSON files in {source_dir}")
        return json_files

    def _generate_output_filename(self, json_files: List[Path]) -> str:
        """Generate output filename with signature hash."""
        # Create signature from all input files
        file_signatures = []
        for json_file in json_files:
            # Use filename and file size for signature
            stat = json_file.stat()
            signature = f"{json_file.name}:{stat.st_size}:{stat.st_mtime}"
            file_signatures.append(signature)

        # Create hash of all signatures
        combined_signature = "|".join(sorted(file_signatures))
        signature_hash = hashlib.md5(combined_signature.encode()).hexdigest()[:8]

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.config.source}_{timestamp}_{signature_hash}.parquet"

        return filename

    def _transform_single_file(self, json_file: Path) -> Optional[pl.DataFrame]:
        """Transform a single JSON file to DataFrame."""
        try:
            self.logger.debug(f"Processing {json_file.name}")

            # Use source-specific transform service
            # Create temporary output path (we'll consolidate later)
            temp_output = json_file.with_suffix('.temp.parquet')

            result = self.transform_service.transform_json_to_parquet(
                json_file_path=json_file,
                output_path=temp_output,
                validate_data=self.config.validate_data
            )

            if result["success"]:
                # Read the temporary Parquet file
                df = pl.read_parquet(temp_output)

                # Clean up temporary file
                temp_output.unlink()

                # Update stats
                self.total_records_input += result.get("input_records", 0)
                self.total_records_output += len(df)
                self.files_processed += 1

                return df
            else:
                error_msg = f"Failed to transform {json_file.name}: {result.get('error', 'Unknown error')}"
                self.logger.error(error_msg)
                self.processing_errors.append(error_msg)
                return None

        except Exception as e:
            error_msg = f"Exception processing {json_file.name}: {str(e)}"
            self.logger.error(error_msg)
            self.processing_errors.append(error_msg)
            return None

    def _execute(self) -> Dict[str, Any]:
        """Execute the transform step."""
        start_time = time.time()

        # Find all JSON files
        json_files = self._find_json_files()

        # Generate output filename
        output_filename = self._generate_output_filename(json_files)
        output_path = self.config.interim_data_dir / output_filename

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Transforming {len(json_files)} files to {output_path.name}")

        # Process all files and collect DataFrames
        dataframes = []

        for json_file in json_files:
            df = self._transform_single_file(json_file)
            if df is not None and len(df) > 0:
                dataframes.append(df)

        if not dataframes:
            raise ValueError("No data was successfully transformed from JSON files")

        # Concatenate all DataFrames
        self.logger.info(f"Consolidating {len(dataframes)} DataFrames")
        consolidated_df = pl.concat(dataframes, how="vertical")

        # Sort by timestamp for better compression and query performance
        if "timestamp" in consolidated_df.columns:
            consolidated_df = consolidated_df.sort("timestamp")

        # Write consolidated Parquet file
        consolidated_df.write_parquet(
            output_path,
            compression="snappy",
            statistics=True,
            use_pyarrow=False
        )

        # Calculate final stats
        processing_time = time.time() - start_time
        output_size = output_path.stat().st_size
        final_record_count = len(consolidated_df)

        self.logger.info(
            f"Transform completed: {self.files_processed}/{len(json_files)} files processed, "
            f"{final_record_count:,} records, {output_size:,} bytes, {processing_time:.1f}s"
        )

        return {
            "records_processed": final_record_count,
            "bytes_processed": output_size,
            "files_created": 1,
            "output_paths": [output_path],

            # Transform-specific metadata
            "source": self.config.source,
            "input_files_found": len(json_files),
            "input_files_processed": self.files_processed,
            "input_files_failed": len(json_files) - self.files_processed,
            "total_input_records": self.total_records_input,
            "total_output_records": self.total_records_output,
            "processing_errors": self.processing_errors,
            "output_file": str(output_path),
            "compression_ratio": self.total_records_input / final_record_count if final_record_count > 0 else 1.0,
            "data_quality_score": final_record_count / self.total_records_input if self.total_records_input > 0 else 1.0
        }


# Example usage and testing
if __name__ == "__main__":
    print("🧹 Data Cleaner Step")
    print("   Consolidates raw JSON files into single Parquet for ML processing")
    print("   Usage: Create DataCleanerStepConfig with source='eia' and run step")
