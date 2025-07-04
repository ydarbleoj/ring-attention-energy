"""
Test EIA Schema Models

Comprehensive test suite for EIA API response validation using real VCR cassette data.
Tests the Pydantic models that form Layer 1 of our data architecture.

Key test areas:
1. Schema validation against real API responses
2. Data type conversion and validation
3. DataFrame generation and transformation
4. Error handling and edge cases
5. Oregon-specific energy data patterns

Run with: pytest tests/core/integrations/eia/test_schema.py -v
"""

import pytest
import pandas as pd
import json
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from src.core.integrations.eia.schema import (
    EIAResponse,
    EIAResponseData,
    DemandRecord,
    GenerationRecord,
    EIAErrorResponse,
    validate_eia_response,
    extract_oregon_renewable_summary,
    FUEL_TYPE_CODES,
    RENEWABLE_FUEL_TYPES
)


class TestDemandRecord:
    """Test DemandRecord validation and properties"""

    def test_valid_demand_record(self):
        """Test parsing a valid demand record from VCR data"""
        record_data = {
            "period": "2023-01-01T00",
            "respondent": "PACW",
            "respondent-name": "PacifiCorp West",
            "type": "D",
            "type-name": "Demand",
            "value": "2455",
            "value-units": "megawatthours"
        }

        record = DemandRecord.model_validate(record_data)

        assert record.period == "2023-01-01T00"
        assert record.respondent == "PACW"
        assert record.respondent_name == "PacifiCorp West"
        assert record.type == "D"
        assert record.value == "2455"
        assert record.demand_mwh == 2455.0
        assert isinstance(record.timestamp, pd.Timestamp)
        assert record.timestamp == pd.Timestamp("2023-01-01 00:00:00")

    def test_invalid_period_format(self):
        """Test validation fails for invalid timestamp"""
        record_data = {
            "period": "invalid-date",
            "respondent": "PACW",
            "respondent-name": "PacifiCorp West",
            "type": "D",
            "type-name": "Demand",
            "value": "2455",
            "value-units": "megawatthours"
        }

        with pytest.raises(ValueError, match="Invalid EIA timestamp format"):
            DemandRecord.model_validate(record_data)

    def test_invalid_value_format(self):
        """Test validation fails for non-numeric value"""
        record_data = {
            "period": "2023-01-01T00",
            "respondent": "PACW",
            "respondent-name": "PacifiCorp West",
            "type": "D",
            "type-name": "Demand",
            "value": "not-a-number",
            "value-units": "megawatthours"
        }

        with pytest.raises(ValueError, match="Value cannot be converted to float"):
            DemandRecord.model_validate(record_data)

    def test_alias_handling(self):
        """Test that field aliases work correctly"""
        record_data = {
            "period": "2023-01-01T00",
            "respondent": "PACW",
            "respondent-name": "PacifiCorp West",  # Using alias
            "type": "D",
            "type-name": "Demand",  # Using alias
            "value": "2455",
            "value-units": "megawatthours"  # Using alias
        }

        record = DemandRecord.model_validate(record_data)
        assert record.respondent_name == "PacifiCorp West"
        assert record.type_name == "Demand"
        assert record.value_units == "megawatthours"


class TestGenerationRecord:
    """Test GenerationRecord validation and properties"""

    def test_valid_generation_record(self):
        """Test parsing a valid generation record from VCR data"""
        record_data = {
            "period": "2023-01-01T00",
            "respondent": "PACW",
            "respondent-name": "PacifiCorp West",
            "fueltype": "SUN",
            "type-name": "Solar",
            "value": "44",
            "value-units": "megawatthours"
        }

        record = GenerationRecord.model_validate(record_data)

        assert record.fueltype == "SUN"
        assert record.type_name == "Solar"
        assert record.generation_mwh == 44.0
        assert isinstance(record.timestamp, pd.Timestamp)

    def test_all_fuel_types_from_vcr(self):
        """Test parsing all fuel types found in VCR cassettes"""
        fuel_types = ["NG", "SUN", "WAT", "WND", "OTH"]

        for fueltype in fuel_types:
            record_data = {
                "period": "2023-01-01T00",
                "respondent": "PACW",
                "respondent-name": "PacifiCorp West",
                "fueltype": fueltype,
                "type-name": FUEL_TYPE_CODES.get(fueltype, "Unknown"),
                "value": "100",
                "value-units": "megawatthours"
            }

            record = GenerationRecord.model_validate(record_data)
            assert record.fueltype == fueltype
            assert record.generation_mwh == 100.0

    def test_zero_generation(self):
        """Test handling of zero generation values (common in VCR data)"""
        record_data = {
            "period": "2023-01-01T00",
            "respondent": "PACW",
            "respondent-name": "PacifiCorp West",
            "fueltype": "NG",
            "type-name": "Natural Gas",
            "value": "0",
            "value-units": "megawatthours"
        }

        record = GenerationRecord.model_validate(record_data)
        assert record.generation_mwh == 0.0


class TestEIAResponseData:
    """Test EIAResponseData parsing and record extraction"""

    @pytest.fixture
    def sample_demand_response_data(self) -> Dict[str, Any]:
        """Sample response data structure for demand endpoint"""
        return {
            "total": "169",
            "dateFormat": "YYYY-MM-DD\"T\"HH24",
            "frequency": "hourly",
            "data": [
                {
                    "period": "2023-01-01T00",
                    "respondent": "PACW",
                    "respondent-name": "PacifiCorp West",
                    "type": "D",
                    "type-name": "Demand",
                    "value": "2455",
                    "value-units": "megawatthours"
                },
                {
                    "period": "2023-01-01T01",
                    "respondent": "PACW",
                    "respondent-name": "PacifiCorp West",
                    "type": "D",
                    "type-name": "Demand",
                    "value": "2579",
                    "value-units": "megawatthours"
                }
            ]
        }

    @pytest.fixture
    def sample_generation_response_data(self) -> Dict[str, Any]:
        """Sample response data structure for generation endpoint"""
        return {
            "total": "845",
            "dateFormat": "YYYY-MM-DD\"T\"HH24",
            "frequency": "hourly",
            "data": [
                {
                    "period": "2023-01-01T00",
                    "respondent": "PACW",
                    "respondent-name": "PacifiCorp West",
                    "fueltype": "SUN",
                    "type-name": "Solar",
                    "value": "44",
                    "value-units": "megawatthours"
                },
                {
                    "period": "2023-01-01T00",
                    "respondent": "PACW",
                    "respondent-name": "PacifiCorp West",
                    "fueltype": "WND",
                    "type-name": "Wind",
                    "value": "282",
                    "value-units": "megawatthours"
                }
            ]
        }

    def test_demand_response_parsing(self, sample_demand_response_data):
        """Test parsing demand response data"""
        response_data = EIAResponseData.model_validate(sample_demand_response_data)

        assert response_data.total_records == 169
        assert response_data.frequency == "hourly"

        demand_records = response_data.parse_demand_records()
        assert len(demand_records) == 2
        assert all(isinstance(r, DemandRecord) for r in demand_records)
        assert demand_records[0].demand_mwh == 2455.0
        assert demand_records[1].demand_mwh == 2579.0

    def test_generation_response_parsing(self, sample_generation_response_data):
        """Test parsing generation response data"""
        response_data = EIAResponseData.model_validate(sample_generation_response_data)

        generation_records = response_data.parse_generation_records()
        assert len(generation_records) == 2
        assert all(isinstance(r, GenerationRecord) for r in generation_records)
        assert generation_records[0].fueltype == "SUN"
        assert generation_records[1].fueltype == "WND"


class TestEIAResponse:
    """Test full EIA response processing and DataFrame generation"""

    @pytest.fixture
    def sample_demand_response(self) -> Dict[str, Any]:
        """Complete EIA demand response structure"""
        return {
            "response": {
                "total": "3",
                "dateFormat": "YYYY-MM-DD\"T\"HH24",
                "frequency": "hourly",
                "data": [
                    {
                        "period": "2023-01-01T00",
                        "respondent": "PACW",
                        "respondent-name": "PacifiCorp West",
                        "type": "D",
                        "type-name": "Demand",
                        "value": "2455",
                        "value-units": "megawatthours"
                    },
                    {
                        "period": "2023-01-01T01",
                        "respondent": "PACW",
                        "respondent-name": "PacifiCorp West",
                        "type": "D",
                        "type-name": "Demand",
                        "value": "2579",
                        "value-units": "megawatthours"
                    }
                ]
            }
        }

    @pytest.fixture
    def sample_generation_response(self) -> Dict[str, Any]:
        """Complete EIA generation response structure"""
        return {
            "response": {
                "total": "4",
                "dateFormat": "YYYY-MM-DD\"T\"HH24",
                "frequency": "hourly",
                "data": [
                    {
                        "period": "2023-01-01T00",
                        "respondent": "PACW",
                        "respondent-name": "PacifiCorp West",
                        "fueltype": "SUN",
                        "type-name": "Solar",
                        "value": "44",
                        "value-units": "megawatthours"
                    },
                    {
                        "period": "2023-01-01T00",
                        "respondent": "PACW",
                        "respondent-name": "PacifiCorp West",
                        "fueltype": "WND",
                        "type-name": "Wind",
                        "value": "282",
                        "value-units": "megawatthours"
                    },
                    {
                        "period": "2023-01-01T01",
                        "respondent": "PACW",
                        "respondent-name": "PacifiCorp West",
                        "fueltype": "SUN",
                        "type-name": "Solar",
                        "value": "8",
                        "value-units": "megawatthours"
                    },
                    {
                        "period": "2023-01-01T01",
                        "respondent": "PACW",
                        "respondent-name": "PacifiCorp West",
                        "fueltype": "WND",
                        "type-name": "Wind",
                        "value": "289",
                        "value-units": "megawatthours"
                    }
                ]
            }
        }

    def test_demand_dataframe_generation(self, sample_demand_response):
        """Test converting demand response to DataFrame"""
        response = EIAResponse.model_validate(sample_demand_response)
        df = response.to_demand_dataframe()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["timestamp", "region", "demand_mwh"]
        assert df["region"].iloc[0] == "PACW"
        assert df["demand_mwh"].iloc[0] == 2455.0
        assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])

        # Check time ordering
        assert df["timestamp"].is_monotonic_increasing

    def test_generation_dataframe_generation(self, sample_generation_response):
        """Test converting generation response to wide DataFrame"""
        response = EIAResponse.model_validate(sample_generation_response)
        df = response.to_generation_dataframe()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2  # 2 unique timestamps
        assert "timestamp" in df.columns
        assert "region" in df.columns
        assert "solar_mwh" in df.columns
        assert "wind_mwh" in df.columns

        # Check pivoting worked correctly
        first_hour = df[df["timestamp"] == "2023-01-01 00:00:00"].iloc[0]
        assert first_hour["solar_mwh"] == 44.0
        assert first_hour["wind_mwh"] == 282.0

        second_hour = df[df["timestamp"] == "2023-01-01 01:00:00"].iloc[0]
        assert second_hour["solar_mwh"] == 8.0
        assert second_hour["wind_mwh"] == 289.0

    def test_empty_response_handling(self):
        """Test handling of empty responses"""
        empty_response = {
            "response": {
                "total": "0",
                "dateFormat": "YYYY-MM-DD\"T\"HH24",
                "frequency": "hourly",
                "data": []
            }
        }

        response = EIAResponse.model_validate(empty_response)

        demand_df = response.to_demand_dataframe()
        assert isinstance(demand_df, pd.DataFrame)
        assert len(demand_df) == 0
        assert list(demand_df.columns) == ["timestamp", "region", "demand_mwh"]

        generation_df = response.to_generation_dataframe()
        assert isinstance(generation_df, pd.DataFrame)
        assert len(generation_df) == 0


class TestVCRCassetteIntegration:
    """Test schema against real VCR cassette data"""

    @pytest.fixture
    def cassette_dir(self) -> Path:
        """Get path to VCR cassettes directory"""
        return Path(__file__).parent / "cassettes"

    def load_cassette_response(self, cassette_path: Path) -> Dict[str, Any]:
        """Load and extract API response from VCR cassette"""
        with open(cassette_path, 'r') as f:
            cassette_data = yaml.safe_load(f)

        # Get first interaction's response
        response_string = cassette_data['interactions'][0]['response']['body']['string']
        return json.loads(response_string)

    def test_oregon_demand_cassette(self, cassette_dir):
        """Test schema against Oregon demand VCR cassette"""
        cassette_path = cassette_dir / "eia_oregon_demand.yaml"
        if not cassette_path.exists():
            pytest.skip("VCR cassette not found")

        response_json = self.load_cassette_response(cassette_path)

        # Validate response
        response = EIAResponse.model_validate(response_json)
        assert response.response.total_records == 169

        # Test DataFrame generation
        df = response.to_demand_dataframe()
        assert len(df) == 169
        assert df["demand_mwh"].min() > 0
        assert df["demand_mwh"].max() < 10000  # Reasonable for Oregon

        print(f"✅ Oregon demand VCR test: {len(df)} records validated")
        print(f"📊 Demand range: {df['demand_mwh'].min():.1f} - {df['demand_mwh'].max():.1f} MWh")

    def test_oregon_renewable_cassette(self, cassette_dir):
        """Test schema against Oregon renewable VCR cassette"""
        cassette_path = cassette_dir / "eia_oregon_renewable.yaml"
        if not cassette_path.exists():
            pytest.skip("VCR cassette not found")

        response_json = self.load_cassette_response(cassette_path)

        # Validate response
        response = EIAResponse.model_validate(response_json)
        assert response.response.total_records == 845

        # Test DataFrame generation
        df = response.to_generation_dataframe()
        assert len(df) > 0

        # Check for expected fuel types from Oregon data
        expected_cols = ["solar_mwh", "wind_mwh", "hydro_mwh", "natural_gas_mwh", "other_mwh"]
        present_cols = [col for col in expected_cols if col in df.columns]
        assert len(present_cols) > 0

        print(f"✅ Oregon renewable VCR test: {len(df)} records, {len(present_cols)} fuel types")
        print(f"🌱 Fuel types: {present_cols}")

    def test_oregon_comprehensive_cassette(self, cassette_dir):
        """Test schema against Oregon comprehensive VCR cassette"""
        cassette_path = cassette_dir / "eia_oregon_comprehensive.yaml"
        if not cassette_path.exists():
            pytest.skip("VCR cassette not found")

        # Load cassette - comprehensive has multiple interactions
        with open(cassette_path, 'r') as f:
            cassette_data = yaml.safe_load(f)

        # Test each interaction
        interactions = cassette_data['interactions']
        successful_responses = 0

        for interaction in interactions:
            response_string = interaction['response']['body']['string']

            # Skip error responses (like pricing API 404s)
            if "error" in response_string:
                error_response = EIAErrorResponse.model_validate(json.loads(response_string))
                assert error_response.code in [404, 500]
                continue

            # Validate successful responses
            response_json = json.loads(response_string)
            response = EIAResponse.model_validate(response_json)
            successful_responses += 1

        assert successful_responses >= 2  # Should have demand + generation
        print(f"✅ Oregon comprehensive VCR test: {successful_responses} successful responses")


class TestErrorHandling:
    """Test error handling and edge cases"""

    def test_error_response_parsing(self):
        """Test parsing EIA error responses"""
        error_response = {
            "error": "Not found.",
            "code": 404
        }

        error = EIAErrorResponse.model_validate(error_response)
        assert error.error == "Not found."
        assert error.code == 404

    def test_validate_eia_response_success(self):
        """Test validate_eia_response with successful response"""
        success_response = {
            "response": {
                "total": "1",
                "dateFormat": "YYYY-MM-DD\"T\"HH24",
                "frequency": "hourly",
                "data": []
            }
        }

        result = validate_eia_response(success_response)
        assert isinstance(result, EIAResponse)

    def test_validate_eia_response_error(self):
        """Test validate_eia_response with error response"""
        error_response = {
            "error": "Not found.",
            "code": 404
        }

        result = validate_eia_response(error_response)
        assert isinstance(result, EIAErrorResponse)

    def test_validate_eia_response_invalid(self):
        """Test validate_eia_response with invalid response"""
        invalid_response = {
            "unexpected": "format"
        }

        with pytest.raises(ValueError, match="Invalid EIA response format"):
            validate_eia_response(invalid_response)


class TestOregonEnergyAnalysis:
    """Test Oregon-specific energy data analysis utilities"""

    def test_renewable_summary_extraction(self):
        """Test renewable energy summary calculation"""
        # Create sample generation DataFrame
        data = {
            "timestamp": pd.date_range("2023-01-01", periods=3, freq="h"),
            "region": ["PACW"] * 3,
            "solar_mwh": [0, 10, 20],
            "wind_mwh": [100, 150, 200],
            "hydro_mwh": [500, 500, 500],
            "natural_gas_mwh": [0, 0, 0]
        }
        df = pd.DataFrame(data)

        # Extract renewable summary
        summary_df = extract_oregon_renewable_summary(df)

        # Check calculations
        assert "total_renewable_mwh" in summary_df.columns
        assert summary_df["total_renewable_mwh"].iloc[0] == 600  # 0+100+500
        assert summary_df["total_renewable_mwh"].iloc[1] == 660  # 10+150+500

        assert "total_generation_mwh" in summary_df.columns
        assert "renewable_percentage" in summary_df.columns
        assert summary_df["renewable_percentage"].iloc[0] == 100.0  # All renewable

    def test_fuel_type_constants(self):
        """Test fuel type constants are properly defined"""
        assert "SUN" in FUEL_TYPE_CODES
        assert FUEL_TYPE_CODES["SUN"] == "Solar"

        assert "WND" in RENEWABLE_FUEL_TYPES
        assert "SUN" in RENEWABLE_FUEL_TYPES
        assert "NG" not in RENEWABLE_FUEL_TYPES


if __name__ == "__main__":
    # Run specific test for development
    pytest.main([__file__ + "::TestVCRCassetteIntegration::test_oregon_demand_cassette", "-v"])
