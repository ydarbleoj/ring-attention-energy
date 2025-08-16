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

            # Handle empty data files (normal for historical data gaps)
            if not records:
                self.logger.debug(f"Empty data file (normal for historical gaps): {json_file_path.name}")
                # Create empty DataFrame with proper schema
                df = self._create_dataframe(records, metadata, json_file_path)

                # Create empty output file with proper schema
                output_path.parent.mkdir(parents=True, exist_ok=True)
                df.write_parquet(output_path, compression="snappy")

                end_time = datetime.now()
                processing_time = (end_time - start_time).total_seconds()

                return {
                    "success": True,
                    "input_file": str(json_file_path),
                    "output_file": str(output_path),
                    "input_records": 0,
                    "output_records": 0,
                    "processing_time_seconds": processing_time,
                    "data_quality": {"empty_file": True},
                    "file_size_bytes": output_path.stat().st_size,
                    "metadata": metadata,
                    "empty_data": True
                }

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

        # Determine data type from metadata
        data_type_from_metadata = metadata.get("data_type", "unknown")

        if not records:
            # Return empty DataFrame with expected schema
            return pl.DataFrame(schema=self._get_schema(data_type_from_metadata))

        # Flatten records and add metadata - include ALL records for validation
        flattened_records = []

        for record in records:
            # Create a unified record structure with all possible columns
            # Use proper null values for type consistency
            flattened = {
                # Common fields
                "timestamp": self._parse_datetime(record.get("period")),
                "data_type": data_type_from_metadata,
                "source_file": source_file.name,

                # Energy data fields (demand/generation) - use empty strings for consistency
                "region": "",
                "fuel_type": "",
                "type_name": "",
                "value": None,  # Keep as None for numeric
                "value_units": "",

                # Price data fields - use empty strings for consistency
                "stateid": "",
                "state_description": "",
                "sectorid": "",
                "sector_name": "",
                "price": None,  # Keep as None for numeric
                "price_units": ""
            }

            # Populate fields based on data type
            if data_type_from_metadata == "price":
                # Price data specific fields
                flattened.update({
                    "stateid": record.get("stateid", ""),
                    "state_description": record.get("stateDescription", ""),
                    "sectorid": record.get("sectorid", ""),
                    "sector_name": record.get("sectorName", ""),
                    "price": self._parse_numeric_value(record.get("price")),
                    "price_units": record.get("price-units", "")
                })
            else:
                # Demand & generation data specific fields
                flattened.update({
                    "region": record.get("respondent", ""),
                    "fuel_type": record.get("fueltype", ""),
                    "type_name": record.get("type-name", ""),
                    "value": self._parse_numeric_value(record.get("value")),
                    "value_units": record.get("value-units", "")
                })

            flattened_records.append(flattened)

        # Create DataFrame with unified schema
        if flattened_records:
            df = pl.DataFrame(flattened_records)
            # Ensure consistent schema by casting to expected types
            expected_schema = self._get_schema(data_type_from_metadata)
            # Cast each column to its expected type
            cast_exprs = []
            for col_name, expected_type in expected_schema.items():
                if col_name in df.columns:
                    cast_exprs.append(pl.col(col_name).cast(expected_type))

            if cast_exprs:
                df = df.with_columns(cast_exprs)
        else:
            df = pl.DataFrame(schema=self._get_schema(data_type_from_metadata))

        return df

    def _is_valid_record(self, record: Dict, data_type: str = "energy") -> bool:
        """Check if a record has minimum required fields."""
        if data_type == "price":
            # Price data requires: period, stateid, price
            required_fields = ["period", "stateid", "price"]
        else:
            # Demand/generation data requires: period, respondent, value
            required_fields = ["period", "respondent", "value"]

        return all(field in record and record[field] is not None for field in required_fields)

    def _parse_datetime(self, period_str: Optional[str]) -> Optional[datetime]:
        """Parse period string to datetime."""
        if not period_str:
            return None

        try:
            # Handle monthly format like "2024-01" (price data)
            if len(period_str) == 7 and period_str.count('-') == 1:
                # Convert monthly to first day of month: "2024-01" -> "2024-01-01T00:00:00"
                period_str = period_str + "-01T00:00:00"
            # Handle format like "2024-01-20T00"
            elif period_str.endswith('T00'):
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

    def _get_schema(self, data_type: str = "energy") -> Dict[str, pl.DataType]:
        """Get unified DataFrame schema for all data types."""
        return {
            # Common fields
            "timestamp": pl.Datetime,
            "data_type": pl.Utf8,
            "source_file": pl.Utf8,

            # Energy data fields (demand/generation) - nullable for price data
            "region": pl.Utf8,
            "fuel_type": pl.Utf8,
            "type_name": pl.Utf8,
            "value": pl.Float64,
            "value_units": pl.Utf8,

            # Price data fields - nullable for energy data
            "stateid": pl.Utf8,
            "state_description": pl.Utf8,
            "sectorid": pl.Utf8,
            "sector_name": pl.Utf8,
            "price": pl.Float64,
            "price_units": pl.Utf8
        }

    def _validate_and_clean_data(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, Dict[str, Any]]:
        """Validate and clean the DataFrame."""

        initial_count = len(df)
        quality_issues = []

        # Check for missing critical values - handle unified schema
        null_timestamp_count = df.filter(pl.col("timestamp").is_null()).height

        # Check data type specific required fields
        data_type_values = df.select("data_type").unique().to_series().to_list()
        has_price_data = "price" in data_type_values
        has_energy_data = any(dt in ["demand", "generation"] for dt in data_type_values)

        if has_price_data:
            # Price data validation - check price and stateid
            null_price_count = df.filter(
                (pl.col("data_type") == "price") &
                pl.col("price").is_null()
            ).height
            null_stateid_count = df.filter(
                (pl.col("data_type") == "price") &
                (pl.col("stateid").is_null() | (pl.col("stateid") == ""))
            ).height

            if null_price_count > 0:
                quality_issues.append(f"{null_price_count} price records with null price")
            if null_stateid_count > 0:
                quality_issues.append(f"{null_stateid_count} price records with null/empty stateid")

        if has_energy_data:
            # Energy data validation - check value and region
            null_value_count = df.filter(
                (pl.col("data_type").is_in(["demand", "generation"])) &
                pl.col("value").is_null()
            ).height
            null_region_count = df.filter(
                (pl.col("data_type").is_in(["demand", "generation"])) &
                (pl.col("region").is_null() | (pl.col("region") == ""))
            ).height

            if null_value_count > 0:
                quality_issues.append(f"{null_value_count} energy records with null value")
            if null_region_count > 0:
                quality_issues.append(f"{null_region_count} energy records with null/empty region")

        if null_timestamp_count > 0:
            quality_issues.append(f"{null_timestamp_count} records with null timestamp")

        # Remove records with critical missing data - unified approach
        # For price data: timestamp, price, stateid must not be null/empty
        # For energy data: timestamp, value, region must not be null/empty
        price_filter = (
            (pl.col("data_type") == "price") &
            pl.col("timestamp").is_not_null() &
            pl.col("price").is_not_null() &
            pl.col("stateid").is_not_null() &
            (pl.col("stateid") != "")
        )

        energy_filter = (
            (pl.col("data_type").is_in(["demand", "generation"])) &
            pl.col("timestamp").is_not_null() &
            pl.col("value").is_not_null() &
            pl.col("region").is_not_null() &
            (pl.col("region") != "")
        )

        df_cleaned = df.filter(price_filter | energy_filter)

        # Check for duplicate timestamps - handle unified schema
        price_data = df_cleaned.filter(pl.col("data_type") == "price")
        energy_data = df_cleaned.filter(pl.col("data_type").is_in(["demand", "generation"]))

        cleaned_dfs = []

        if len(price_data) > 0:
            # Price data - check for duplicates by timestamp+stateid+sectorid
            price_duplicate_count = len(price_data) - price_data.unique(subset=["timestamp", "stateid", "sectorid"], maintain_order=True).height
            if price_duplicate_count > 0:
                quality_issues.append(f"{price_duplicate_count} duplicate price timestamp/stateid/sectorid combinations")
            price_cleaned = price_data.unique(subset=["timestamp", "stateid", "sectorid"], keep="first")
            cleaned_dfs.append(price_cleaned)

        if len(energy_data) > 0:
            # Energy data - check for duplicates accounting for fuel_type
            has_fuel_types = energy_data.filter(pl.col("fuel_type").is_not_null()).height > 0

            if has_fuel_types:
                # Generation data - check for duplicates including fuel_type
                energy_duplicate_count = len(energy_data) - energy_data.unique(subset=["timestamp", "region", "fuel_type"], maintain_order=True).height
                if energy_duplicate_count > 0:
                    quality_issues.append(f"{energy_duplicate_count} duplicate energy timestamp/region/fuel_type combinations")
                energy_cleaned = energy_data.unique(subset=["timestamp", "region", "fuel_type"], keep="first")
            else:
                # Demand data - check for duplicates without fuel_type
                energy_duplicate_count = len(energy_data) - energy_data.unique(subset=["timestamp", "region"], maintain_order=True).height
                if energy_duplicate_count > 0:
                    quality_issues.append(f"{energy_duplicate_count} duplicate energy timestamp/region combinations")
                energy_cleaned = energy_data.unique(subset=["timestamp", "region"], keep="first")

            cleaned_dfs.append(energy_cleaned)

        # Recombine cleaned data
        if cleaned_dfs:
            df_cleaned = pl.concat(cleaned_dfs, how="vertical")
        else:
            df_cleaned = df_cleaned.clear()  # Empty DataFrame with same schema

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
                    "start": df.select(pl.col("timestamp").min()).item(),
                    "end": df.select(pl.col("timestamp").max()).item()
                } if "timestamp" in df.columns and len(df) > 0 else None,
                "regions": df.select(pl.col("region").unique()).to_series().to_list() if "region" in df.columns else [],
                "data_types": df.select(pl.col("data_type").unique()).to_series().to_list() if "data_type" in df.columns else []
            }
        except Exception as e:
            return {"error": str(e)}
