"""
Test simplified RawDataLoader service for EIA ETL Extract stage

This test suite validates the RawDataLoader service which handles saving
raw API responses to data/raw/ with proper metadata.

The simplified RawDataLoader only handles:
1. Saving JSON responses to disk with metadata
2. Loading saved raw data files
3. Listing and summarizing saved files
4. Proper directory structure creation

Run with: pytest tests/core/integrations/eia/service/test_raw_data_loader.py -v
"""
import pytest
import json
from pathlib import Path
from datetime import datetime
import tempfile
import shutil

from src.core.integrations.eia.services.raw_data_loader import RawDataLoader, RawDataMetadata

# Test constants for consistent testing
TEST_REGION = "PACW"  # Pacific West region
TEST_DATES = {
    "start": "2024-01-01",
    "end": "2024-01-07"
}


class TestRawDataLoaderBasic:
    """Test basic RawDataLoader functionality"""

    def setup_method(self):
        """Set up test configuration and loader"""
        # Create temporary directory for testing
        self.temp_raw_dir = tempfile.mkdtemp()
        self.raw_loader = RawDataLoader(self.temp_raw_dir)

    def teardown_method(self):
        """Clean up temporary directory"""
        shutil.rmtree(self.temp_raw_dir, ignore_errors=True)

    def test_loader_initialization(self):
        """Test RawDataLoader initializes correctly"""
        assert str(self.raw_loader.raw_data_path) == self.temp_raw_dir
        assert self.raw_loader.raw_data_path.exists()

    def test_generate_filename(self):
        """Test filename generation follows expected pattern"""
        filename = self.raw_loader._generate_filename(
            data_type="demand",
            region=TEST_REGION,
            start_date=TEST_DATES["start"],
            end_date=TEST_DATES["end"]
        )

        # Should match pattern: eia_demand_PACW_2024-01-01_to_2024-01-07_{timestamp}.json
        assert filename.startswith(f"eia_demand_{TEST_REGION}_{TEST_DATES['start']}_to_{TEST_DATES['end']}_")
        assert filename.endswith(".json")

    def test_metadata_creation(self):
        """Test RawDataMetadata creation and serialization"""
        metadata = RawDataMetadata(
            timestamp="2024-01-01T00:00:00",
            region=TEST_REGION,
            data_type="demand",
            start_date=TEST_DATES["start"],
            end_date=TEST_DATES["end"],
            record_count=168,
            success=True
        )

        # Should serialize to dict properly
        metadata_dict = metadata.__dict__
        assert metadata_dict["region"] == TEST_REGION
        assert metadata_dict["record_count"] == 168
        assert metadata_dict["success"] is True


class TestRawDataLoaderSaveLoad:
    """Test RawDataLoader save and load functionality"""

    def setup_method(self):
        """Set up test configuration and loader"""
        # Create temporary directory for testing
        self.temp_raw_dir = tempfile.mkdtemp()
        self.raw_loader = RawDataLoader(self.temp_raw_dir)

    def teardown_method(self):
        """Clean up temporary directory"""
        shutil.rmtree(self.temp_raw_dir, ignore_errors=True)

    def test_save_and_load_raw_data(self):
        """Test saving and loading raw data with metadata"""
        # Create mock API response
        api_response = {
            "response": {
                "data": [
                    {"period": "2024-01-01T00:00:00Z", "value": 100, "subregion": "PACW"},
                    {"period": "2024-01-01T01:00:00Z", "value": 105, "subregion": "PACW"},
                ]
            }
        }

        # Create metadata
        metadata = RawDataMetadata(
            timestamp=datetime.now().isoformat(),
            region=TEST_REGION,
            data_type="demand",
            start_date=TEST_DATES["start"],
            end_date=TEST_DATES["end"],
            api_endpoint="/electricity/rto/region-data",
            success=True
        )

        # Save raw data
        file_path = self.raw_loader.save_raw_data(api_response, metadata)

        # Verify file was created
        assert file_path.exists()
        assert file_path.name.startswith(f"eia_demand_{TEST_REGION}")
        assert file_path.suffix == ".json"

        # Load and verify content
        raw_package = self.raw_loader.load_raw_file(file_path)

        assert "metadata" in raw_package
        assert "api_response" in raw_package

        # Verify metadata
        loaded_metadata = raw_package["metadata"]
        assert loaded_metadata["source"] == "eia"
        assert loaded_metadata["data_type"] == "demand"
        assert loaded_metadata["region"] == TEST_REGION
        assert loaded_metadata["success"] is True
        assert loaded_metadata["record_count"] == 2  # Should auto-calculate

        # Verify API response
        loaded_response = raw_package["api_response"]
        assert loaded_response == api_response

    def test_save_with_custom_filename(self):
        """Test saving with custom filename"""
        api_response = {"response": {"data": []}}
        metadata = RawDataMetadata(
            timestamp=datetime.now().isoformat(),
            region="TEST",
            data_type="custom",
            start_date="2024-01-01",
            end_date="2024-01-01"
        )

        custom_filename = "custom_test_file.json"
        file_path = self.raw_loader.save_raw_data(api_response, metadata, filename=custom_filename)

        assert file_path.name == custom_filename

    def test_directory_structure_creation(self):
        """Test that proper directory structure is created"""
        api_response = {"response": {"data": []}}
        metadata = RawDataMetadata(
            timestamp=datetime.now().isoformat(),
            region=TEST_REGION,
            data_type="demand",
            start_date="2024-01-01",
            end_date="2024-01-01"
        )

        file_path = self.raw_loader.save_raw_data(api_response, metadata)

        # Should create structure: raw_data_path/eia/2024/filename.json
        expected_dir = self.raw_loader.raw_data_path / "eia" / "2024"
        assert expected_dir.exists()
        assert file_path.parent == expected_dir

    def test_load_nonexistent_file(self):
        """Test loading a file that doesn't exist"""
        with pytest.raises(FileNotFoundError):
            self.raw_loader.load_raw_file("nonexistent_file.json")


class TestRawDataLoaderUtilities:
    """Test RawDataLoader utility functions"""

    def setup_method(self):
        """Set up test configuration and loader"""
        # Create temporary directory for testing
        self.temp_raw_dir = tempfile.mkdtemp()
        self.raw_loader = RawDataLoader(self.temp_raw_dir)

    def teardown_method(self):
        """Clean up temporary directory"""
        shutil.rmtree(self.temp_raw_dir, ignore_errors=True)

    def test_list_raw_files(self):
        """Test listing raw data files with filtering"""
        # Create directory structure
        (self.raw_loader.raw_data_path / "eia" / "2024").mkdir(parents=True, exist_ok=True)

        # Create mock raw files
        demand_file = self.raw_loader.raw_data_path / "eia" / "2024" / f"eia_demand_{TEST_REGION}_2024-01-01_to_2024-01-07_123456.json"
        generation_file = self.raw_loader.raw_data_path / "eia" / "2024" / f"eia_generation_{TEST_REGION}_2024-01-01_to_2024-01-07_123456.json"
        other_file = self.raw_loader.raw_data_path / "eia" / "2024" / "other_file.json"  # Should be ignored

        demand_file.write_text('{"metadata": {}, "api_response": {}}')
        generation_file.write_text('{"metadata": {}, "api_response": {}}')
        other_file.write_text('{"metadata": {}, "api_response": {}}')

        # Test listing all files (should only include eia_* files)
        all_files = self.raw_loader.list_raw_files()
        assert len(all_files) == 2
        assert all(f.name.startswith("eia_") for f in all_files)

        # Test filtering by data type
        demand_files = self.raw_loader.list_raw_files(data_type="demand")
        assert len(demand_files) == 1
        assert "demand" in demand_files[0].name

        generation_files = self.raw_loader.list_raw_files(data_type="generation")
        assert len(generation_files) == 1
        assert "generation" in generation_files[0].name

    def test_extraction_summary(self):
        """Test extraction summary statistics"""
        # Create directory structure
        (self.raw_loader.raw_data_path / "eia" / "2024").mkdir(parents=True, exist_ok=True)

        # Create demand file
        demand_data = {
            "metadata": {
                "data_type": "demand",
                "region": TEST_REGION,
                "start_date": "2024-01-01",
                "success": True,
                "record_count": 168,
                "response_size_bytes": 15000
            },
            "api_response": {"response": {"data": []}}
        }

        demand_file = self.raw_loader.raw_data_path / "eia" / "2024" / f"eia_demand_{TEST_REGION}_2024-01-01_to_2024-01-07_123456.json"
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

        # Create failed extraction file
        failed_data = {
            "metadata": {
                "data_type": "demand",
                "region": "TEST",
                "start_date": "2024-01-01",
                "success": False,
                "record_count": 0,
                "response_size_bytes": 0,
                "error_message": "API timeout"
            },
            "api_response": {}
        }

        failed_file = self.raw_loader.raw_data_path / "eia" / "2024" / "eia_demand_TEST_2024-01-01_to_2024-01-01_123456_FAILED.json"
        failed_file.write_text(json.dumps(failed_data))

        # Get summary
        summary = self.raw_loader.get_extraction_summary()

        assert summary["total_files"] == 3
        assert summary["by_data_type"]["demand"] == 2  # 1 success + 1 failed
        assert summary["by_data_type"]["generation"] == 1
        assert summary["by_region"][TEST_REGION] == 1
        assert summary["by_region"]["ERCO"] == 1
        assert summary["by_region"]["TEST"] == 1
        assert summary["by_date"]["2024"] == 3
        assert summary["failed_extractions"] == 1
        assert summary["total_records"] == 368  # 168 + 200 + 0
        assert summary["total_size_bytes"] == 33000  # 15000 + 18000 + 0

    def test_summary_with_corrupted_file(self):
        """Test summary handles corrupted files gracefully"""
        # Create directory structure
        (self.raw_loader.raw_data_path / "eia" / "2024").mkdir(parents=True, exist_ok=True)

        # Create corrupted file
        corrupted_file = self.raw_loader.raw_data_path / "eia" / "2024" / "eia_demand_TEST_corrupted.json"
        corrupted_file.write_text("invalid json content")

        # Should handle gracefully
        summary = self.raw_loader.get_extraction_summary()
        assert summary["total_files"] == 1
        assert summary["failed_extractions"] == 1
