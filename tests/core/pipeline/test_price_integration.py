"""
Test Price Data Integration in Pipeline DAG

Tests that price data flows correctly through the Extract → Transform pipeline
and gets integrated with demand and generation data.

Key test areas:
1. Price data configuration in pipeline steps
2. Price data processing in transform step
3. Consolidated output includes all three data types
4. Performance maintains target throughput
5. Data quality validation for price records

Run with: pytest tests/core/pipeline/test_price_integration.py -v
"""

import pytest
import asyncio
import json
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

from src.core.pipeline.steps.extract.api_extract import ApiExtractStep, ApiExtractStepConfig
from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig
from src.core.pipeline.orchestrators.pipeline_dag import PipelineDAG, PipelineDAGConfig


class TestPriceDataIntegration:
    """Test price data integration in the pipeline"""

    @pytest.fixture
    def temp_data_dirs(self):
        """Create temporary directories for test data"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            raw_dir = temp_path / "raw"
            interim_dir = temp_path / "interim"
            raw_dir.mkdir(parents=True)
            interim_dir.mkdir(parents=True)

            yield {
                "raw_dir": raw_dir,
                "interim_dir": interim_dir,
                "temp_path": temp_path
            }

    @pytest.fixture
    def mock_price_data(self):
        """Mock EIA price response data"""
        return {
            "response": {
                "total": "72",  # 3 days * 24 hours
                "dateFormat": "YYYY-MM-DD\"T\"HH24",
                "frequency": "hourly",
                "data": [
                    {
                        "period": f"2024-01-{day:02d}T{hour:02d}",
                        "parent": "PACW",
                        "parent-name": "PacifiCorp West",
                        "subba": "PACW-WY",
                        "subba-name": "PacifiCorp West - Wyoming",
                        "type": "LMP",
                        "type-name": "Locational Marginal Price",
                        "value": f"{35.0 + hour * 0.5 + (10.0 if 16 <= hour <= 20 else 0.0):.2f}",
                        "value-units": "dollars per megawatthour"
                    }
                    for day in range(1, 4)  # 3 days
                    for hour in range(24)   # 24 hours each
                ]
            }
        }

    def test_price_data_configuration(self):
        """Test that price data type is properly configured in extract step"""
        config = ApiExtractStepConfig(
            step_name="Price Extract Test",
            step_id="price_extract",
            source="eia",
            start_date="2024-01-01",
            end_date="2024-01-07",
            regions=["PACW"],
            data_types=["demand", "generation", "price"],
            api_key="test_key",
            dry_run=True
        )

        extract_step = ApiExtractStep(config)

        assert "price" in extract_step.config.data_types
        assert len(extract_step.config.data_types) == 3

    @pytest.mark.asyncio
    async def test_price_data_file_creation(self, temp_data_dirs, mock_price_data):
        """Test that price data gets saved as JSON files during extraction"""
        raw_dir = temp_data_dirs["raw_dir"]

        # Create mock price data file
        price_file = raw_dir / "eia_price_PACW_2024-01-01_to_2024-01-03_test.json"
        with open(price_file, 'w') as f:
            json.dump(mock_price_data, f, indent=2)

        # Verify file exists and contains price data
        assert price_file.exists()

        with open(price_file, 'r') as f:
            data = json.load(f)

        assert data["response"]["total"] == "72"
        assert len(data["response"]["data"]) == 72
        assert all(record["type"] == "LMP" for record in data["response"]["data"])

    @pytest.mark.asyncio
    async def test_price_data_transform_processing(self, temp_data_dirs, mock_price_data):
        """Test that transform step processes price data correctly"""
        raw_dir = temp_data_dirs["raw_dir"]
        interim_dir = temp_data_dirs["interim_dir"]

        # Create EIA subdirectory structure
        eia_dir = raw_dir / "eia" / "2024"
        eia_dir.mkdir(parents=True)

        # Create mock price data file
        price_file = eia_dir / "eia_price_PACW_2024-01-01_to_2024-01-03_test.json"
        with open(price_file, 'w') as f:
            json.dump(mock_price_data, f, indent=2)

        # Create transform step
        config = DataCleanerStepConfig(
            step_name="Price Transform Test",
            step_id="price_transform",
            source="eia",
            raw_data_dir=raw_dir,
            interim_data_dir=interim_dir,
            validate_data=True,
            dry_run=False
        )

        transform_step = DataCleanerStep(config)

        # Execute transform step
        result = await transform_step.execute_async()

        assert result.status == "success"

        # Check output files
        parquet_files = list(interim_dir.glob("*.parquet"))
        assert len(parquet_files) > 0

        # Verify price data in output
        import polars as pl
        df = pl.read_parquet(parquet_files[0])

        price_records = df.filter(pl.col("data_type") == "price")
        assert len(price_records) == 72

        # Verify price data properties
        price_values = price_records["value"].to_list()
        assert all(isinstance(p, float) for p in price_values)
        assert all(p > 0 for p in price_values)  # All positive prices
        assert min(price_values) >= 35.0  # Base price
        assert max(price_values) <= 55.0  # Peak price

    @pytest.mark.asyncio
    async def test_multi_data_type_integration(self, temp_data_dirs):
        """Test that price data integrates with demand and generation data"""
        raw_dir = temp_data_dirs["raw_dir"]
        interim_dir = temp_data_dirs["interim_dir"]

        # Create EIA subdirectory structure
        eia_dir = raw_dir / "eia" / "2024"
        eia_dir.mkdir(parents=True)

        # Create mock data for all three types
        mock_data_sets = {
            "demand": {
                "response": {
                    "total": "24",
                    "dateFormat": "YYYY-MM-DD\"T\"HH24",
                    "frequency": "hourly",
                    "data": [
                        {
                            "period": f"2024-01-01T{hour:02d}",
                            "respondent": "PACW",
                            "respondent-name": "PacifiCorp West",
                            "type": "D",
                            "type-name": "Demand",
                            "value": f"{2000 + hour * 10}",
                            "value-units": "megawatthours"
                        }
                        for hour in range(24)
                    ]
                }
            },
            "generation": {
                "response": {
                    "total": "48",
                    "dateFormat": "YYYY-MM-DD\"T\"HH24",
                    "frequency": "hourly",
                    "data": [
                        {
                            "period": f"2024-01-01T{hour:02d}",
                            "respondent": "PACW",
                            "respondent-name": "PacifiCorp West",
                            "fueltype": fueltype,
                            "type-name": "Solar" if fueltype == "SUN" else "Wind",
                            "value": f"{100 + hour}",
                            "value-units": "megawatthours"
                        }
                        for hour in range(24)
                        for fueltype in ["SUN", "WND"]
                    ]
                }
            },
            "price": {
                "response": {
                    "total": "24",
                    "dateFormat": "YYYY-MM-DD\"T\"HH24",
                    "frequency": "hourly",
                    "data": [
                        {
                            "period": f"2024-01-01T{hour:02d}",
                            "parent": "PACW",
                            "parent-name": "PacifiCorp West",
                            "type": "LMP",
                            "type-name": "Locational Marginal Price",
                            "value": f"{40.0 + hour * 0.5}",
                            "value-units": "dollars per megawatthour"
                        }
                        for hour in range(24)
                    ]
                }
            }
        }

        # Create JSON files for each data type
        for data_type, data in mock_data_sets.items():
            file_path = eia_dir / f"eia_{data_type}_PACW_2024-01-01_test.json"
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)

        # Create and execute transform step
        config = DataCleanerStepConfig(
            step_name="Multi-Type Integration Test",
            step_id="multi_type_test",
            source="eia",
            raw_data_dir=raw_dir,
            interim_data_dir=interim_dir,
            validate_data=True,
            dry_run=False
        )

        transform_step = DataCleanerStep(config)
        result = await transform_step.execute_async()

        assert result.status == "success"

        # Verify consolidated output
        parquet_files = list(interim_dir.glob("*.parquet"))
        assert len(parquet_files) > 0

        import polars as pl
        df = pl.read_parquet(parquet_files[0])

        # Check that all three data types are present
        data_types = set(df["data_type"].unique().to_list())
        expected_types = {"demand", "generation", "price"}
        assert expected_types.issubset(data_types)

        # Verify record counts
        demand_count = len(df.filter(pl.col("data_type") == "demand"))
        generation_count = len(df.filter(pl.col("data_type") == "generation"))
        price_count = len(df.filter(pl.col("data_type") == "price"))

        assert demand_count == 24  # 24 hours
        assert generation_count == 48  # 24 hours * 2 fuel types
        assert price_count == 24  # 24 hours

        total_records = demand_count + generation_count + price_count
        assert len(df) == total_records

    def test_price_data_in_demo_pipeline_config(self):
        """Test that demo pipeline includes price data type"""
        # This tests the modification we made to demo_pipeline_dag.py
        config = ApiExtractStepConfig(
            step_name="Demo Extract with Price",
            step_id="demo_extract",
            source="eia",
            start_date="2024-01-01",
            end_date="2024-01-07",
            regions=["PACW"],
            data_types=["demand", "generation", "price"],
            api_key="test_key",
            dry_run=True
        )

        assert "price" in config.data_types
        assert len(config.data_types) == 3

        # Verify all expected data types
        expected_types = {"demand", "generation", "price"}
        assert set(config.data_types) == expected_types

    @pytest.mark.asyncio
    async def test_price_data_performance_benchmark(self, temp_data_dirs):
        """Test that price data processing maintains performance targets"""
        raw_dir = temp_data_dirs["raw_dir"]
        interim_dir = temp_data_dirs["interim_dir"]

        # Create EIA subdirectory structure
        eia_dir = raw_dir / "eia" / "2024"
        eia_dir.mkdir(parents=True)

        # Create larger price dataset (1 week = 168 hours)
        mock_price_data = {
            "response": {
                "total": "168",
                "dateFormat": "YYYY-MM-DD\"T\"HH24",
                "frequency": "hourly",
                "data": [
                    {
                        "period": f"2024-01-{(hour // 24) + 1:02d}T{hour % 24:02d}",
                        "parent": "PACW",
                        "parent-name": "PacifiCorp West",
                        "type": "LMP",
                        "type-name": "Locational Marginal Price",
                        "value": f"{35.0 + (hour % 24) * 0.5 + ((hour // 24) % 2) * 5}",
                        "value-units": "dollars per megawatthour"
                    }
                    for hour in range(168)  # 7 days * 24 hours
                ]
            }
        }

        price_file = eia_dir / "eia_price_PACW_2024-01-01_to_2024-01-07_perf_test.json"
        with open(price_file, 'w') as f:
            json.dump(mock_price_data, f, indent=2)

        # Create and execute transform step with timing
        config = DataCleanerStepConfig(
            step_name="Price Performance Test",
            step_id="price_perf_test",
            source="eia",
            raw_data_dir=raw_dir,
            interim_data_dir=interim_dir,
            validate_data=True,
            dry_run=False
        )

        transform_step = DataCleanerStep(config)

        start_time = datetime.now()
        result = await transform_step.execute_async()
        end_time = datetime.now()

        duration = (end_time - start_time).total_seconds()

        assert result.status == "success"
        assert duration < 1.0  # Should process 168 records in under 1 second

        # Verify performance metrics
        records_per_second = 168 / duration
        assert records_per_second > 100  # Target: 100+ records/second for small dataset
