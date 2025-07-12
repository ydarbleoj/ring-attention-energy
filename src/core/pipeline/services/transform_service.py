"""Lightweight transform service for JSON → Parquet conversion.

Handles flattening of EIA JSON responses with metadata preservation and data quality checks.
Uses Polars for high-performance data processing.
"""

import json
import polars as pl
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)


class EIATransformService:
    """Service for transforming EIA JSON files to clean Parquet format."""

    def __init__(self):
        """Initialize the transform service."""
        self.logger = logging.getLogger(__name__)

    def transform_json_to_parquet(
        self,
        json_file_path: Path,
        output_path: Path,
        validate_data: bool = True
    ) -> Dict[str, Any]:
        """Transform a single JSON file to Parquet format.

        Args:
            json_file_path: Path to input JSON file
            output_path: Path for output Parquet file
            validate_data: Whether to perform data quality validation

        Returns:
            Dictionary with transformation results and metrics
        """
        start_time = datetime.now()

        try:
            # Load and parse JSON
            with open(json_file_path, 'r') as f:
                data = json.load(f)

            # Extract metadata and records
            metadata = data.get('metadata', {})
            api_response = data.get('api_response', {}).get('response', {})
            records = api_response.get('data', [])

            self.logger.info(f"Processing {len(records)} records from {json_file_path.name}")

            # Transform to Polars DataFrame
            df = self._create_dataframe(records, metadata, json_file_path)

            # Data quality checks and cleaning
            if validate_data:
                df, quality_report = self._validate_and_clean_data(df)
            else:
                quality_report = {"validation_skipped": True}

            # Save to Parquet
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(output_path, compression="snappy")

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            # Return transformation results
            return {
                "success": True,
                "input_file": str(json_file_path),
                "output_file": str(output_path),
                "input_records": len(records),
                "output_records": len(df),
                "processing_time_seconds": processing_time,
                "data_quality": quality_report,
                "file_size_bytes": output_path.stat().st_size,
                "metadata": metadata
            }

        except Exception as e:
            self.logger.error(f"Error transforming {json_file_path}: {str(e)}")
            return {
                "success": False,
                "input_file": str(json_file_path),
                "error": str(e),
                "processing_time_seconds": (datetime.now() - start_time).total_seconds()
            }

    def _create_dataframe(
        self,
        records: List[Dict],
        metadata: Dict,
        source_file: Path
    ) -> pl.DataFrame:
        """Create Polars DataFrame from JSON records with metadata."""

        if not records:
            # Return empty DataFrame with expected schema
            return pl.DataFrame(schema=self._get_schema())

        # Flatten records and add metadata
        flattened_records = []

        for record in records:
            # Skip malformed records (like those with only "value-units")
            if not self._is_valid_record(record):
                continue

            flattened = {
                # Core energy data
                "datetime": self._parse_datetime(record.get("period")),
                "region": record.get("respondent"),
                "region_name": record.get("respondent-name"),
                "data_type": record.get("type"),
                "data_type_name": record.get("type-name"),
                "value": self._parse_numeric_value(record.get("value")),
                "value_units": record.get("value-units"),

                # Metadata columns (added to each record for easy filtering/analysis)
                "source_file": source_file.name,
                "extraction_timestamp": metadata.get("timestamp"),
                "api_endpoint": metadata.get("api_endpoint"),
                "extraction_date_range_start": metadata.get("start_date"),
                "extraction_date_range_end": metadata.get("end_date"),
                "record_count_in_file": metadata.get("record_count"),
                "extraction_success": metadata.get("success", True)
            }
            flattened_records.append(flattened)

        # Create DataFrame
        if flattened_records:
            df = pl.DataFrame(flattened_records)
        else:
            df = pl.DataFrame(schema=self._get_schema())

        return df

    def _is_valid_record(self, record: Dict) -> bool:
        """Check if a record has minimum required fields."""
        required_fields = ["period", "respondent", "type", "value"]
        return all(field in record and record[field] is not None for field in required_fields)

    def _parse_datetime(self, period_str: Optional[str]) -> Optional[datetime]:
        """Parse period string to datetime."""
        if not period_str:
            return None

        try:
            # Handle format like "2024-01-20T00"
            if period_str.endswith('T00'):
                # Add minutes and seconds for full parsing
                period_str = period_str.replace('T00', 'T00:00:00')
            elif 'T' in period_str and len(period_str.split('T')[1]) <= 2:
                # Handle formats like "2024-01-20T01"
                period_str = period_str + ':00:00'

            return datetime.fromisoformat(period_str)
        except ValueError:
            logger.warning(f"Could not parse datetime: {period_str}")
            return None

    def _parse_numeric_value(self, value_str: Optional[str]) -> Optional[float]:
        """Parse value string to numeric."""
        if not value_str:
            return None

        try:
            return float(value_str)
        except (ValueError, TypeError):
            logger.warning(f"Could not parse numeric value: {value_str}")
            return None

    def _get_schema(self) -> Dict[str, pl.DataType]:
        """Get expected DataFrame schema."""
        return {
            "datetime": pl.Datetime,
            "region": pl.Utf8,
            "region_name": pl.Utf8,
            "data_type": pl.Utf8,
            "data_type_name": pl.Utf8,
            "value": pl.Float64,
            "value_units": pl.Utf8,
            "source_file": pl.Utf8,
            "extraction_timestamp": pl.Utf8,
            "api_endpoint": pl.Utf8,
            "extraction_date_range_start": pl.Utf8,
            "extraction_date_range_end": pl.Utf8,
            "record_count_in_file": pl.Int64,
            "extraction_success": pl.Boolean
        }

    def _validate_and_clean_data(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, Dict[str, Any]]:
        """Validate and clean the DataFrame."""

        initial_count = len(df)
        quality_issues = []

        # Check for missing critical values
        null_datetime_count = df.filter(pl.col("datetime").is_null()).height
        null_value_count = df.filter(pl.col("value").is_null()).height

        if null_datetime_count > 0:
            quality_issues.append(f"{null_datetime_count} records with null datetime")

        if null_value_count > 0:
            quality_issues.append(f"{null_value_count} records with null value")

        # Remove records with critical missing data
        df_cleaned = df.filter(
            pl.col("datetime").is_not_null() &
            pl.col("value").is_not_null()
        )

        # Check for duplicate timestamps
        duplicate_count = len(df_cleaned) - df_cleaned.unique(subset=["datetime", "region"]).height
        if duplicate_count > 0:
            quality_issues.append(f"{duplicate_count} duplicate datetime/region combinations")

        # Remove duplicates (keep first occurrence)
        df_cleaned = df_cleaned.unique(subset=["datetime", "region"], keep="first")

        final_count = len(df_cleaned)
        records_dropped = initial_count - final_count

        quality_report = {
            "initial_records": initial_count,
            "final_records": final_count,
            "records_dropped": records_dropped,
            "drop_percentage": (records_dropped / initial_count * 100) if initial_count > 0 else 0,
            "quality_issues": quality_issues,
            "data_quality_score": (final_count / initial_count) if initial_count > 0 else 1.0
        }

        return df_cleaned, quality_report

    def get_file_info(self, parquet_file: Path) -> Dict[str, Any]:
        """Get information about a transformed Parquet file."""
        if not parquet_file.exists():
            return {"error": "File not found"}

        try:
            df = pl.read_parquet(parquet_file)

            return {
                "file_path": str(parquet_file),
                "file_size_bytes": parquet_file.stat().st_size,
                "record_count": len(df),
                "columns": df.columns,
                "schema": dict(zip(df.columns, [str(dtype) for dtype in df.dtypes])),
                "date_range": {
                    "start": df.select(pl.col("datetime").min()).item(),
                    "end": df.select(pl.col("datetime").max()).item()
                } if "datetime" in df.columns and len(df) > 0 else None,
                "regions": df.select(pl.col("region").unique()).to_series().to_list() if "region" in df.columns else [],
                "data_types": df.select(pl.col("data_type").unique()).to_series().to_list() if "data_type" in df.columns else []
            }
        except Exception as e:
            return {"error": str(e)}
