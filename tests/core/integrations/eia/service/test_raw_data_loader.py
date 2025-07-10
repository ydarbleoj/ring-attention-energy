"""
Test RawDataLoader service for EIA ETL Extract stage with VCR for reproducible testing

This test suite validates the RawDataLoader service which handles the Extract
stage of the EIA ETL pipeline by saving raw API responses to data/raw/.

Key features:
1. Uses VCR to record/replay HTTP interactions
2. Tests Oregon-specific energy data (Pacific West region)
3. Validates proper file structure and metadata creation
4. Handles directory structure and error cases

Run with: pytest tests/core/integrations/eia/service/test_raw_data_loader.py -v
"""
import pytest
import json
from pathlib import Path
from datetime import date
import tempfile
import shutil
import os

from src.core.integrations.eia.service.raw_data_loader import RawDataLoader, RawDataMetadata
from src.core.integrations.eia.client import EIAClient
from src.core.integrations.config import get_test_config
from tests.vcr_config import VCRManager

# Oregon-specific constants for consistent testing
OREGON_REGION = "PACW"  # Pacific West region includes Oregon
OREGON_TEST_DATES = {
    "start": "2024-01-01",
    "end": "2024-01-07"  # One week for testing
}


class TestRawDataLoaderBasic:
    """Test basic RawDataLoader functionality"""

    def setup_method(self):
        """Set up test configuration and loader"""
        self.config = get_test_config()
        self.config.api.eia_api_key = os.getenv('EIA_API_KEY', 'TEST_KEY_FOR_VCR')
        self.client = EIAClient(config=self.config)

        # Create temporary directory for testing
        self.temp_raw_dir = tempfile.mkdtemp()
        self.raw_loader = RawDataLoader(self.client, self.temp_raw_dir)

    def teardown_method(self):
        """Clean up temporary directory"""
        shutil.rmtree(self.temp_raw_dir, ignore_errors=True)

    def test_loader_initialization(self):
        """Test RawDataLoader initializes correctly"""
        assert self.raw_loader.client == self.client
        assert str(self.raw_loader.raw_data_path) == self.temp_raw_dir
        assert self.raw_loader.raw_data_path.exists()

    def test_generate_filename(self):
        """Test filename generation follows expected pattern"""
        filename = self.raw_loader._generate_filename(
            data_type="demand",
            region=OREGON_REGION,
            start_date=OREGON_TEST_DATES["start"],
            end_date=OREGON_TEST_DATES["end"]
        )

        # Should match pattern: eia_demand_PACW_2024-01-01_to_2024-01-07_{timestamp}.json
        assert filename.startswith(f"eia_demand_{OREGON_REGION}_{OREGON_TEST_DATES['start']}_to_{OREGON_TEST_DATES['end']}_")
        assert filename.endswith(".json")

    def test_metadata_creation(self):
        """Test RawDataMetadata creation and serialization"""
        metadata = RawDataMetadata(
            timestamp="2024-01-01T00:00:00",
            region=OREGON_REGION,
            data_type="demand",
            start_date=OREGON_TEST_DATES["start"],
            end_date=OREGON_TEST_DATES["end"],
            record_count=168,
            success=True
        )

        # Should serialize to dict properly
        metadata_dict = metadata.__dict__
        assert metadata_dict["region"] == OREGON_REGION
        assert metadata_dict["record_count"] == 168
        assert metadata_dict["success"] is True


class TestRawDataLoaderOregonExtracts:
    """Test RawDataLoader with Oregon energy data using VCR"""

    def setup_method(self):
        """Set up test configuration and loader for Oregon testing"""
        self.config = get_test_config()
        self.config.api.eia_api_key = os.getenv('EIA_API_KEY', 'TEST_KEY_FOR_VCR')
        self.client = EIAClient(config=self.config)

        # Create temporary directory for testing
        self.temp_raw_dir = tempfile.mkdtemp()
        self.raw_loader = RawDataLoader(self.client, self.temp_raw_dir)

        # Create cassettes directory
        self.cassette_path = Path(__file__).parent.parent / "cassettes"
        self.cassette_path.mkdir(exist_ok=True)

    def teardown_method(self):
        """Clean up temporary directory"""
        shutil.rmtree(self.temp_raw_dir, ignore_errors=True)

    def test_extract_oregon_demand_data_with_vcr(self):
        """Test Oregon demand data extraction using VCR cassette"""
        with VCRManager("raw_loader_demand_test", self.cassette_path):
            file_path = self.raw_loader.extract_demand_data(
                region=OREGON_REGION,
                start_date=OREGON_TEST_DATES["start"],
                end_date=OREGON_TEST_DATES["end"]
            )

            # Verify file was created
            assert file_path.exists()
            assert file_path.name.startswith(f"eia_demand_{OREGON_REGION}")
            assert file_path.suffix == ".json"

            # Load and verify content
            raw_package = self.raw_loader.load_raw_file(file_path)

            assert "metadata" in raw_package
            assert "api_response" in raw_package

            # Verify metadata
            metadata = raw_package["metadata"]
            assert metadata["source"] == "eia"
            assert metadata["data_type"] == "demand"
            assert metadata["region"] == OREGON_REGION
            assert metadata["success"] is True
            assert metadata["record_count"] > 0

            # Verify API response structure
            api_response = raw_package["api_response"]
            assert "response" in api_response
            assert "data" in api_response["response"]
            assert len(api_response["response"]["data"]) > 0

            print(f"✅ Oregon demand extract: {len(api_response['response']['data'])} records")
            print(f"📁 File saved: {file_path.name}")

    def test_extract_oregon_generation_data_with_vcr(self):
        """Test Oregon generation data extraction using VCR cassette"""
        with VCRManager("raw_loader_generation_test", self.cassette_path):
            file_path = self.raw_loader.extract_generation_data(
                region=OREGON_REGION,
                start_date=OREGON_TEST_DATES["start"],
                end_date=OREGON_TEST_DATES["end"]
            )

            # Verify file was created
            assert file_path.exists()
            assert file_path.name.startswith(f"eia_generation_{OREGON_REGION}")

            # Load and verify content
            raw_package = self.raw_loader.load_raw_file(file_path)

            metadata = raw_package["metadata"]
            assert metadata["data_type"] == "generation"
            assert metadata["success"] is True

            # Verify API response has generation data
            api_response = raw_package["api_response"]
            data = api_response["response"]["data"]
            assert len(data) > 0

            # Should have fuel type information for generation data
            if data:
                first_record = data[0]
                assert "fueltype" in first_record

            print(f"✅ Oregon generation extract: {len(data)} records")


class TestRawDataLoaderUtilities:
    """Test RawDataLoader utility functions and error handling"""

    def setup_method(self):
        """Set up test configuration and loader"""
        self.config = get_test_config()
        self.config.api.eia_api_key = os.getenv('EIA_API_KEY', 'TEST_KEY_FOR_VCR')
        self.client = EIAClient(config=self.config)

        # Create temporary directory for testing
        self.temp_raw_dir = tempfile.mkdtemp()
        self.raw_loader = RawDataLoader(self.client, self.temp_raw_dir)

    def teardown_method(self):
        """Clean up temporary directory"""
        shutil.rmtree(self.temp_raw_dir, ignore_errors=True)

    def test_extract_with_date_objects(self):
        """Test extraction with date objects instead of strings"""
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 7)

        # Mock the client call for this test
        def mock_request(url, params):
            return {"response": {"data": [{"period": "2024-01-01T00:00:00Z", "value": 100}]}}

        self.raw_loader.client._make_request = mock_request

        file_path = self.raw_loader.extract_demand_data(
            region="TEST",
            start_date=start_date,
            end_date=end_date
        )

        # Verify dates were converted to strings in metadata
        raw_package = self.raw_loader.load_raw_file(file_path)
        metadata = raw_package["metadata"]
        assert metadata["start_date"] == "2024-01-01"
        assert metadata["end_date"] == "2024-01-07"

    def test_list_raw_files(self):
        """Test listing raw data files with filtering"""
        # Create some mock raw files
        (self.raw_loader.raw_data_path / "eia" / "2024").mkdir(parents=True, exist_ok=True)

        demand_file = self.raw_loader.raw_data_path / "eia" / "2024" / f"eia_demand_{OREGON_REGION}_2024-01-01_to_2024-01-07_123456.json"
        generation_file = self.raw_loader.raw_data_path / "eia" / "2024" / f"eia_generation_{OREGON_REGION}_2024-01-01_to_2024-01-07_123456.json"

        demand_file.write_text('{"metadata": {}, "api_response": {}}')
        generation_file.write_text('{"metadata": {}, "api_response": {}}')

        # Test listing all files
        all_files = self.raw_loader.list_raw_files()
        assert len(all_files) == 2

        # Test filtering by data type
        demand_files = self.raw_loader.list_raw_files(data_type="demand")
        assert len(demand_files) == 1
        assert "demand" in demand_files[0].name

        generation_files = self.raw_loader.list_raw_files(data_type="generation")
        assert len(generation_files) == 1
        assert "generation" in generation_files[0].name

    def test_extraction_summary(self):
        """Test extraction summary statistics"""
        # Create mock raw files with metadata
        (self.raw_loader.raw_data_path / "eia" / "2024").mkdir(parents=True, exist_ok=True)

        # Create demand file
        demand_data = {
            "metadata": {
                "data_type": "demand",
                "region": OREGON_REGION,
                "start_date": "2024-01-01",
                "success": True,
                "record_count": 168,
                "response_size_bytes": 15000
            },
            "api_response": {"response": {"data": []}}
        }

        demand_file = self.raw_loader.raw_data_path / "eia" / "2024" / f"eia_demand_{OREGON_REGION}_2024-01-01_to_2024-01-07_123456.json"
        demand_file.write_text(json.dumps(demand_data))

        # Create generation file
        generation_data = {
            "metadata": {
                "data_type": "generation",
                "region": "ERCO",
                "start_date": "2024-01-01",
                "success": True,
                "record_count": 200,
                "response_size_bytes": 18000
            },
            "api_response": {"response": {"data": []}}
        }

        generation_file = self.raw_loader.raw_data_path / "eia" / "2024" / "eia_generation_ERCO_2024-01-01_to_2024-01-07_123456.json"
        generation_file.write_text(json.dumps(generation_data))

        # Get summary
        summary = self.raw_loader.get_extraction_summary()

        assert summary["total_files"] == 2
        assert summary["by_data_type"]["demand"] == 1
        assert summary["by_data_type"]["generation"] == 1
        assert summary["by_region"][OREGON_REGION] == 1
        assert summary["by_region"]["ERCO"] == 1
        assert summary["by_date"]["2024"] == 2
        assert summary["failed_extractions"] == 0
        assert summary["total_records"] == 368  # 168 + 200
        assert summary["total_size_bytes"] == 33000  # 15000 + 18000

    def test_failed_extraction_handling(self):
        """Test handling of failed API extractions"""
        # Mock a failed API call
        def mock_failed_request(url, params):
            raise Exception("API rate limit exceeded")

        self.raw_loader.client._make_request = mock_failed_request

        file_path = self.raw_loader.extract_demand_data(
            region="TEST",
            start_date="2024-01-01",
            end_date="2024-01-07"
        )

        # Should still create a file (marked as failed)
        assert file_path.exists()
        assert "_FAILED.json" in file_path.name

        # Load and verify failure metadata
        raw_package = self.raw_loader.load_raw_file(file_path)
        metadata = raw_package["metadata"]

        assert metadata["success"] is False
        assert "API rate limit exceeded" in metadata["error_message"]
        assert metadata["record_count"] == 0

    def test_directory_structure_creation(self):
        """Test that proper directory structure is created"""
        # Mock successful API call
        def mock_request(url, params):
            return {"response": {"data": [{"period": "2024-01-01T00:00:00Z", "value": 100}]}}

        self.raw_loader.client._make_request = mock_request

        file_path = self.raw_loader.extract_demand_data(
            region=OREGON_REGION,
            start_date="2024-01-01",
            end_date="2024-01-07"
        )

        # Should create structure: raw_data_path/eia/2024/filename.json
        expected_dir = self.raw_loader.raw_data_path / "eia" / "2024"
        assert expected_dir.exists()
        assert file_path.parent == expected_dir
