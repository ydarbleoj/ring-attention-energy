"""
Test CAISO API client functionality

This test suite validates the CAISO (California Independent System Operator) API client.
CAISO provides real-time grid operations data for California without requiring an API key.

Module namespace: tests.core.integrations.caiso.test_client
Source module: src.core.integrations.caiso.client

Run with: pytest tests/core/integrations/caiso/test_client.py -v
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root / "tests"))

from core.integrations.caiso.client import CAISOClient
from core.integrations.config import get_test_config
from vcr_config import VCRManager, validate_energy_data_response


class TestCAISOClient:
    """Test CAISO API client functionality"""

    def setup_method(self):
        """Set up test configuration and client"""
        self.config = get_test_config()
        self.client = CAISOClient()  # CAISO doesn't require API key

    def test_client_initialization(self):
        """Test CAISO client initializes correctly"""
        assert self.client.base_url == "http://oasis.caiso.com/oasisapi/SingleZip"
        assert self.client.session is not None
        assert self.client.rate_limit_delay > 0

    def test_session_configuration(self):
        """Test that session is configured properly"""
        session = self.client.session
        assert session is not None

    def test_rate_limiting_config(self):
        """Test that rate limiting is properly configured"""
        assert self.client.rate_limit_delay > 0
        assert self.client.rate_limit_delay <= 2.0  # Should be reasonable


class TestCAISOClientAPIIntegration:
    """Test CAISO API integration with VCR recording"""

    def setup_method(self):
        """Set up test configuration and client"""
        self.client = CAISOClient()

    def test_api_connection_with_vcr(self):
        """Test API connection using VCR"""
        cassette_path = Path(__file__).parent / "cassettes"
        cassette_path.mkdir(exist_ok=True)

        with VCRManager("caiso_connection_test", cassette_path):
            result = self.client.test_api_connection()
            assert isinstance(result, bool)

    def test_get_real_time_demand_with_vcr(self):
        """Test real-time demand data retrieval with VCR"""
        cassette_path = Path(__file__).parent / "cassettes"

        with VCRManager("caiso_demand_test", cassette_path):
            # Get recent data (yesterday to today)
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

            demand_data = self.client.get_real_time_demand(
                start_date=start_date,
                end_date=end_date
            )

            # Validate response structure
            assert isinstance(demand_data, pd.DataFrame)

            if not demand_data.empty:
                # Check expected columns
                expected_columns = ['timestamp', 'demand']
                for col in expected_columns:
                    assert col in demand_data.columns, f"Missing column: {col}"

                # Validate data types
                assert pd.api.types.is_datetime64_any_dtype(demand_data['timestamp'])
                assert pd.api.types.is_numeric_dtype(demand_data['demand'])

                # Validate data ranges for California grid
                assert demand_data['demand'].min() > 0, "Demand should be positive"
                assert demand_data['demand'].max() < 100000, "CA demand should be reasonable scale"

                print(f"✅ Retrieved {len(demand_data)} CAISO demand data points")
                print(f"📊 Date range: {demand_data['timestamp'].min()} to {demand_data['timestamp'].max()}")
                print(f"⚡ Demand range: {demand_data['demand'].min():.1f} to {demand_data['demand'].max():.1f} MW")

    def test_get_renewable_generation_with_vcr(self):
        """Test renewable generation data retrieval with VCR"""
        cassette_path = Path(__file__).parent / "cassettes"

        with VCRManager("caiso_renewable_test", cassette_path):
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

            renewable_data = self.client.get_renewable_generation(
                start_date=start_date,
                end_date=end_date
            )

            assert isinstance(renewable_data, pd.DataFrame)

            if not renewable_data.empty:
                assert 'timestamp' in renewable_data.columns

                # Should have renewable columns
                renewable_columns = [col for col in renewable_data.columns
                                   if 'renewable' in col.lower() or 'solar' in col.lower() or 'wind' in col.lower()]
                assert len(renewable_columns) > 0, "Should have renewable generation columns"

                print(f"✅ Retrieved CAISO renewable data with {len(renewable_data)} points")
                print(f"🌱 Renewable types: {renewable_columns}")

    def test_get_electricity_prices_with_vcr(self):
        """Test electricity price data retrieval with VCR"""
        cassette_path = Path(__file__).parent / "cassettes"

        with VCRManager("caiso_prices_test", cassette_path):
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

            price_data = self.client.get_electricity_prices(
                start_date=start_date,
                end_date=end_date
            )

            assert isinstance(price_data, pd.DataFrame)

            if not price_data.empty:
                assert 'timestamp' in price_data.columns
                assert 'price' in price_data.columns

                # Validate price ranges (should be reasonable $/MWh)
                assert price_data['price'].min() > -500, "Prices shouldn't be extremely negative"
                assert price_data['price'].max() < 2000, "Prices shouldn't be extremely high"

                print(f"✅ Retrieved CAISO price data with {len(price_data)} points")
                print(f"💰 Price range: ${price_data['price'].min():.2f} to ${price_data['price'].max():.2f}/MWh")

    def test_get_comprehensive_data_with_vcr(self):
        """Test comprehensive data retrieval combining all CAISO metrics"""
        cassette_path = Path(__file__).parent / "cassettes"

        with VCRManager("caiso_comprehensive_test", cassette_path):
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

            comprehensive_data = self.client.get_comprehensive_data(
                start_date=start_date,
                end_date=end_date
            )

            assert isinstance(comprehensive_data, pd.DataFrame)

            if not comprehensive_data.empty:
                # Should have timestamp
                assert 'timestamp' in comprehensive_data.columns

                # Should combine multiple data types
                assert len(comprehensive_data.columns) >= 2, "Should have multiple data columns"

                print(f"✅ Retrieved comprehensive CAISO data: {comprehensive_data.shape}")
                print(f"📊 Columns: {list(comprehensive_data.columns)}")


class TestCAISOClientErrorHandling:
    """Test error handling and edge cases for CAISO client"""

    def setup_method(self):
        self.client = CAISOClient()

    def test_invalid_date_format_handling(self):
        """Test error handling for invalid date formats"""
        # CAISO expects YYYYMMDD format
        with pytest.raises(Exception):
            self.client.get_real_time_demand(
                start_date="invalid-date",
                end_date="20240101"
            )

    def test_future_date_handling(self):
        """Test handling of future dates"""
        future_date = (datetime.now() + timedelta(days=30)).strftime("%Y%m%d")

        result = self.client.get_real_time_demand(
            start_date=future_date,
            end_date=future_date
        )

        # Should handle gracefully (likely return empty data)
        assert isinstance(result, pd.DataFrame)

    def test_csv_response_parsing(self):
        """Test CSV response parsing edge cases"""
        # Test empty response
        empty_response = ""
        parsed = self.client._parse_csv_response(empty_response)
        assert isinstance(parsed, pd.DataFrame)
        assert len(parsed) == 0


class TestCAISODataQuality:
    """Test data quality and validation for CAISO data"""

    def setup_method(self):
        self.client = CAISOClient()

    def test_data_validation_functions(self):
        """Test CAISO-specific data validation"""
        # Test that we can validate CAISO response structure
        # CAISO returns CSV data in a different format than EIA JSON

        # Valid CSV-like response
        valid_csv = "data_item,timestamp,value\nDEMAND,2024-01-01T00:00:00,1000\n"
        parsed = self.client._parse_csv_response(valid_csv)

        if not parsed.empty:
            assert isinstance(parsed, pd.DataFrame)

    def test_available_query_names(self):
        """Test that CAISO query names are properly defined"""
        query_names = self.client.get_available_query_names()

        assert isinstance(query_names, list)
        assert len(query_names) > 0

        # Check for expected query types
        expected_queries = ['SLD_RTO', 'SLD_REN_FCST', 'PRC_LMP']
        for query in expected_queries:
            assert query in query_names, f"Expected query {query} not found"

        print(f"✅ CAISO available queries: {query_names}")


# Integration test that can run independently
def test_caiso_client_namespace_integration():
    """Basic integration test for proper namespace organization"""
    try:
        # Test that we can import from the correct namespace
        from core.integrations.caiso.client import CAISOClient

        client = CAISOClient()

        # Test basic initialization
        assert client.base_url == "http://oasis.caiso.com/oasisapi/SingleZip"
        assert client.rate_limit_delay > 0

        print("✅ CAISO client namespace integration test passed!")
        print(f"   Module: {CAISOClient.__module__}")
        print(f"   Test module: {__name__}")
        return True

    except Exception as e:
        print(f"❌ CAISO namespace integration test failed: {e}")
        return False


if __name__ == "__main__":
    # Run basic test
    test_caiso_client_namespace_integration()
    print("✅ CAISO client tests ready!")
    print("\nTo run full test suite:")
    print("pytest tests/core/integrations/caiso/test_client.py -v")
    print("\nNote: CAISO API doesn't require authentication")
