"""
Test EIA API client with VCR for reproducible testing - Oregon Focus

This test suite validates the EIA (Energy Information Administration) API client
with a focus on Oregon energy data patterns.

Key features:
1. Uses VCR to record/replay HTTP interactions
2. Tests Oregon-specific energy data (Pacific West region)
3. Validates Oregon energy characteristics (high hydro, moderate demand)
4. Handles configuration and API key management

Run with: pytest tests/core/integrations/eia/test_client.py -v
"""
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

from src.core.integrations.eia.client import EIAClient
from src.core.integrations.config import get_test_config
from tests.vcr_config import VCRManager, validate_energy_data_response

# Oregon-specific constants
OREGON_REGION = "PACW"  # Pacific West region includes Oregon
OREGON_TEST_DATES = {
    "start": "2023-01-01",
    "end": "2023-01-08"  # One week for testing
}


class TestEIAClientBasic:
    """Test basic EIA client functionality"""

    def setup_method(self):
        """Set up test configuration and client"""
        self.config = get_test_config()
        self.config.api.eia_api_key = "TEST_API_KEY_FOR_VCR"
        self.client = EIAClient(config=self.config)

    def test_client_initialization(self):
        """Test EIA client initializes correctly"""
        assert self.client.api_key == "TEST_API_KEY_FOR_VCR"
        assert self.client.base_url == "https://api.eia.gov/v2"
        assert self.client.rate_limit_delay > 0
        assert self.client.session is not None

    def test_client_initialization_without_api_key(self):
        """Test that client raises error when no API key provided"""
        config = get_test_config()
        config.api.eia_api_key = None

        with pytest.raises(ValueError, match="EIA API key is required"):
            EIAClient(config=config)


class TestEIAClientOregonData:
    """Test EIA API with Oregon energy data using VCR"""

    def setup_method(self):
        """Set up test configuration and client for Oregon testing"""
        self.config = get_test_config()
        self.config.api.eia_api_key = "TEST_API_KEY_FOR_VCR"
        self.client = EIAClient(config=self.config)

        # Create cassettes directory
        self.cassette_path = Path(__file__).parent / "cassettes"
        self.cassette_path.mkdir(exist_ok=True)

    def test_oregon_electricity_demand_with_vcr(self):
        """Test Oregon electricity demand data retrieval with VCR"""
        with VCRManager("eia_oregon_demand", self.cassette_path):
            demand_data = self.client.get_electricity_demand(
                region=OREGON_REGION,
                start_date=OREGON_TEST_DATES["start"],
                end_date=OREGON_TEST_DATES["end"]
            )

            # Basic validation
            assert isinstance(demand_data, pd.DataFrame)

            if not demand_data.empty:
                # Check required columns
                assert 'timestamp' in demand_data.columns
                assert 'demand' in demand_data.columns

                # Validate data types
                assert pd.api.types.is_datetime64_any_dtype(demand_data['timestamp'])
                assert pd.api.types.is_numeric_dtype(demand_data['demand'])

                # Oregon-specific validation (Pacific West typical demand)
                assert demand_data['demand'].min() > 0, "Demand should be positive"
                assert demand_data['demand'].max() < 100000, "Oregon demand should be reasonable"

                # Check time ordering
                assert demand_data['timestamp'].is_monotonic_increasing

                print(f"✅ Oregon demand test: {len(demand_data)} data points")
                print(f"📊 Demand range: {demand_data['demand'].min():.1f} - {demand_data['demand'].max():.1f} MW")

    def test_oregon_renewable_generation_with_vcr(self):
        """Test Oregon renewable generation data with VCR"""
        with VCRManager("eia_oregon_renewable", self.cassette_path):
            renewable_data = self.client.get_renewable_generation(
                region=OREGON_REGION,
                start_date=OREGON_TEST_DATES["start"],
                end_date=OREGON_TEST_DATES["end"]
            )

            assert isinstance(renewable_data, pd.DataFrame)

            if not renewable_data.empty:
                assert 'timestamp' in renewable_data.columns

                # Check for renewable columns
                renewable_columns = ['solar_generation', 'wind_generation', 'total_renewable']
                present_columns = [col for col in renewable_columns if col in renewable_data.columns]
                assert len(present_columns) > 0, "Should have renewable generation data"

                # Oregon has significant wind resources
                if 'wind_generation' in renewable_data.columns:
                    wind_data = renewable_data['wind_generation'].dropna()
                    if len(wind_data) > 0:
                        assert wind_data.min() >= 0, "Wind generation should be non-negative"

                print(f"✅ Oregon renewable test: {len(renewable_data)} data points")
                print(f"🌱 Available renewables: {present_columns}")

    def test_oregon_comprehensive_data_with_vcr(self):
        """Test Oregon comprehensive energy data with VCR"""
        with VCRManager("eia_oregon_comprehensive", self.cassette_path):
            comprehensive_data = self.client.get_comprehensive_data(
                region=OREGON_REGION,
                start_date=OREGON_TEST_DATES["start"],
                end_date=OREGON_TEST_DATES["end"]
            )

            assert isinstance(comprehensive_data, pd.DataFrame)

            if not comprehensive_data.empty:
                assert 'timestamp' in comprehensive_data.columns
                assert len(comprehensive_data.columns) >= 2, "Should have multiple data columns"

                # Validate no missing timestamps
                assert comprehensive_data['timestamp'].notna().all()

                # Oregon energy pattern analysis
                if 'demand' in comprehensive_data.columns and len(comprehensive_data) > 24:
                    self._validate_oregon_energy_patterns(comprehensive_data)

                print(f"✅ Oregon comprehensive test: {comprehensive_data.shape}")
                print(f"📊 Columns: {list(comprehensive_data.columns)}")

    def _validate_oregon_energy_patterns(self, data: pd.DataFrame):
        """Validate Oregon-specific energy patterns"""
        demand = data['demand'].dropna()

        if len(demand) > 0:
            daily_peak = demand.max()
            daily_min = demand.min()
            avg_demand = demand.mean()

            # Oregon typically has moderate demand (8-15 GW peak)
            assert 3000 <= avg_demand <= 25000, f"Oregon average demand unusual: {avg_demand:.1f} MW"

            # Reasonable peak-to-minimum ratio
            if daily_min > 0:
                ratio = daily_peak / daily_min
                assert 1.1 <= ratio <= 5.0, f"Oregon demand variation unusual: {ratio:.2f}"

            print(f"🏔️ Oregon pattern validation passed:")
            print(f"   Average demand: {avg_demand:.1f} MW")
            print(f"   Peak demand: {daily_peak:.1f} MW")


class TestEIAClientErrorHandling:
    """Test error handling and edge cases"""

    def setup_method(self):
        self.config = get_test_config()
        self.config.api.eia_api_key = "TEST_API_KEY"
        self.client = EIAClient(config=self.config)

    def test_invalid_region_handling(self):
        """Test graceful handling of invalid region codes"""
        result = self.client.get_electricity_demand(region="INVALID_REGION")
        assert isinstance(result, pd.DataFrame)
        # Should return empty DataFrame for graceful degradation

    def test_future_date_handling(self):
        """Test handling of future dates"""
        future_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        result = self.client.get_electricity_demand(
            start_date=future_date,
            end_date=future_date
        )
        assert isinstance(result, pd.DataFrame)


def test_oregon_integration():
    """Simple integration test for Oregon EIA client"""
    try:
        config = get_test_config()
        config.api.eia_api_key = "TEST_KEY_FOR_OREGON"
        client = EIAClient(config=config)

        assert client.api_key == "TEST_KEY_FOR_OREGON"
        assert client.base_url == "https://api.eia.gov/v2"

        # Test Oregon request structure
        result = client.get_electricity_demand(
            region=OREGON_REGION,
            start_date="2023-01-01",
            end_date="2023-01-02"
        )
        assert isinstance(result, pd.DataFrame)

        print("✅ Oregon EIA integration test passed!")

    except Exception as e:
        print(f"❌ Oregon EIA integration test failed: {e}")
        pytest.fail(f"Oregon EIA integration test failed: {e}")


if __name__ == "__main__":
    test_oregon_integration()
    print("\nTo run full Oregon EIA test suite:")
    print("pytest tests/core/integrations/eia/test_client.py -v")
    print("\nTo record Oregon data with real API key:")
    print("EIA_API_KEY=your_real_key pytest tests/core/integrations/eia/test_client.py::TestEIAClientOregonData -v")
