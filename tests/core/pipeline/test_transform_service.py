"""Tests for EIA Transform Service.

Tests the JSON → Parquet transformation with unified schema for both demand and generation data.
"""

import pytest
import json
import polars as pl
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from src.core.pipeline.services.transform_service import EIATransformService


class TestEIATransformService:
    """Test suite for EIA Transform Service."""

    @pytest.fixture
    def transform_service(self):
        """Create transform service instance."""
        return EIATransformService()

    @pytest.fixture
    def sample_generation_data(self):
        """Sample EIA generation data structure."""
        return {
            "metadata": {
                "timestamp": "2025-07-11T16:12:27.702691",
                "source": "eia",
                "api_endpoint": "/electricity/rto/fuel-type-data",
                "region": "TEX",
                "data_type": "generation",
                "start_date": "2025-01-14",
                "end_date": "2025-02-27",
                "request_params": None,
                "response_size_bytes": 1626542,
                "record_count": 3,
                "success": True,
                "error_message": None
            },
            "api_response": {
                "response": {
                    "data": [
                        {
                            "period": "2025-01-14T00",
                            "respondent": "TEX",
                            "respondent-name": "Texas",
                            "fueltype": "BAT",
                            "type-name": "Battery storage",
                            "value": "2174",
                            "value-units": "megawatthours"
                        },
                        {
                            "period": "2025-01-14T01",
                            "respondent": "TEX",
                            "respondent-name": "Texas",
                            "fueltype": "COL",
                            "type-name": "Coal",
                            "value": "15420",
                            "value-units": "megawatthours"
                        },
                        {
                            "period": "2025-01-14T02",
                            "respondent": "TEX",
                            "respondent-name": "Texas",
                            "fueltype": "NG",
                            "type-name": "Natural gas",
                            "value": "28560",
                            "value-units": "megawatthours"
                        }
                    ]
                }
            }
        }

    @pytest.fixture
    def sample_demand_data(self):
        """Sample EIA demand data structure."""
        return {
            "metadata": {
                "timestamp": "2025-07-11T16:12:27.702691",
                "source": "eia",
                "api_endpoint": "/electricity/rto/region-data",
                "region": "PACW",
                "data_type": "demand",
                "start_date": "2025-01-14",
                "end_date": "2025-02-27",
                "request_params": None,
                "response_size_bytes": 845621,
                "record_count": 2,
                "success": True,
                "error_message": None
            },
            "api_response": {
                "response": {
                    "data": [
                        {
                            "period": "2025-01-14T00",
                            "respondent": "PACW",
                            "respondent-name": "Pacific Northwest",
                            "type-name": "Total electricity demand",
                            "value": "18420",
                            "value-units": "megawatthours"
                        },
                        {
                            "period": "2025-01-14T01",
                            "respondent": "PACW",
                            "respondent-name": "Pacific Northwest",
                            "type-name": "Total electricity demand",
                            "value": "17850",
                            "value-units": "megawatthours"
                        }
                    ]
                }
            }
        }

    def test_transform_generation_data(self, transform_service, sample_generation_data):
        """Test transformation of generation data with fuel types."""
        with TemporaryDirectory() as temp_dir:
            # Create input JSON file
            input_file = Path(temp_dir) / "test_generation.json"
            with open(input_file, 'w') as f:
                json.dump(sample_generation_data, f)

            # Transform to Parquet
            output_file = Path(temp_dir) / "test_generation.parquet"
            result = transform_service.transform_json_to_parquet(input_file, output_file)

            # Verify transformation success
            assert result["success"] is True
            assert result["input_records"] == 3
            assert result["output_records"] == 3
            assert output_file.exists()

            # Load and verify schema
            df = pl.read_parquet(output_file)
            expected_columns = {
                "timestamp", "region", "data_type", "fuel_type",
                "type_name", "value", "value_units", "source_file"
            }
            assert set(df.columns) == expected_columns

            # Verify data content
            assert df["data_type"].unique().to_list() == ["generation"]
            assert df["region"].unique().to_list() == ["TEX"]
            assert set(df["fuel_type"].unique().to_list()) == {"BAT", "COL", "NG"}
            assert df["value_units"].unique().to_list() == ["megawatthours"]

            # Verify data types
            assert df["timestamp"].dtype == pl.Datetime
            assert df["value"].dtype == pl.Float64

    def test_transform_demand_data(self, transform_service, sample_demand_data):
        """Test transformation of demand data (no fuel types)."""
        with TemporaryDirectory() as temp_dir:
            # Create input JSON file
            input_file = Path(temp_dir) / "test_demand.json"
            with open(input_file, 'w') as f:
                json.dump(sample_demand_data, f)

            # Transform to Parquet
            output_file = Path(temp_dir) / "test_demand.parquet"
            result = transform_service.transform_json_to_parquet(input_file, output_file)

            # Verify transformation success
            assert result["success"] is True
            assert result["input_records"] == 2
            assert result["output_records"] == 2
            assert output_file.exists()

            # Load and verify schema
            df = pl.read_parquet(output_file)
            expected_columns = {
                "timestamp", "region", "data_type", "fuel_type",
                "type_name", "value", "value_units", "source_file"
            }
            assert set(df.columns) == expected_columns

            # Verify data content
            assert df["data_type"].unique().to_list() == ["demand"]
            assert df["region"].unique().to_list() == ["PACW"]
            assert df["fuel_type"].null_count() == 2  # Should be null for demand data
            assert df["type_name"].unique().to_list() == ["Total electricity demand"]

    def test_unified_schema_consistency(self, transform_service, sample_generation_data, sample_demand_data):
        """Test that both generation and demand data produce the same schema."""
        with TemporaryDirectory() as temp_dir:
            # Transform both files
            gen_input = Path(temp_dir) / "generation.json"
            gen_output = Path(temp_dir) / "generation.parquet"
            with open(gen_input, 'w') as f:
                json.dump(sample_generation_data, f)

            demand_input = Path(temp_dir) / "demand.json"
            demand_output = Path(temp_dir) / "demand.parquet"
            with open(demand_input, 'w') as f:
                json.dump(sample_demand_data, f)

            transform_service.transform_json_to_parquet(gen_input, gen_output)
            transform_service.transform_json_to_parquet(demand_input, demand_output)

            # Load both files
            gen_df = pl.read_parquet(gen_output)
            demand_df = pl.read_parquet(demand_output)

            # Verify same schema
            assert gen_df.columns == demand_df.columns
            assert gen_df.dtypes == demand_df.dtypes

            # Verify they can be concatenated (unified schema)
            combined_df = pl.concat([gen_df, demand_df])
            assert len(combined_df) == 5  # 3 generation + 2 demand records

    def test_data_quality_validation(self, transform_service):
        """Test data quality validation and cleaning."""
        bad_data = {
            "metadata": {
                "data_type": "generation",
                "region": "TEST"
            },
            "api_response": {
                "response": {
                    "data": [
                        {
                            "period": "2025-01-14T00",
                            "respondent": "TEST",
                            "fueltype": "NG",
                            "type-name": "Natural gas",
                            "value": "1000",
                            "value-units": "megawatthours"
                        },
                        {
                            "period": None,  # Bad timestamp
                            "respondent": "TEST",
                            "fueltype": "COL",
                            "type-name": "Coal",
                            "value": "2000",
                            "value-units": "megawatthours"
                        },
                        {
                            "period": "2025-01-14T02",
                            "respondent": "TEST",
                            "fueltype": "NG",
                            "type-name": "Natural gas",
                            "value": None,  # Bad value
                            "value-units": "megawatthours"
                        }
                    ]
                }
            }
        }

        with TemporaryDirectory() as temp_dir:
            input_file = Path(temp_dir) / "bad_data.json"
            output_file = Path(temp_dir) / "bad_data.parquet"

            with open(input_file, 'w') as f:
                json.dump(bad_data, f)

            result = transform_service.transform_json_to_parquet(input_file, output_file)

            # Should succeed but with data quality issues
            assert result["success"] is True
            assert result["input_records"] == 3
            assert result["output_records"] == 1  # Only 1 good record
            assert result["data_quality"]["records_dropped"] == 2
            assert len(result["data_quality"]["quality_issues"]) > 0

    def test_datetime_parsing(self, transform_service):
        """Test various datetime format parsing."""
        test_cases = [
            "2025-01-14T00",
            "2025-01-14T23",
            "2025-12-31T12"
        ]

        for period_str in test_cases:
            result = transform_service._parse_datetime(period_str)
            assert result is not None
            assert isinstance(result, datetime)

    def test_get_schema(self, transform_service):
        """Test schema definition."""
        schema = transform_service._get_schema()

        expected_fields = {
            "timestamp", "region", "data_type", "fuel_type",
            "type_name", "value", "value_units", "source_file"
        }
        assert set(schema.keys()) == expected_fields
        assert schema["timestamp"] == pl.Datetime
        assert schema["value"] == pl.Float64

if __name__ == "__main__":
    pytest.main([__file__])
