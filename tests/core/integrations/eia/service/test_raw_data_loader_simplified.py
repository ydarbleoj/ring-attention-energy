"""
Test RawDataLoader service - simplified for JSON saving only

This test suite validates the simplified RawDataLoader service which handles
saving raw API responses to data/raw/ with proper metadata.
"""
import pytest
import json
from pathlib import Path
from datetime import date, datetime
import tempfile
import shutil
import os

from src.core.integrations.eia.services.raw_data_loader import RawDataLoader, RawDataMetadata

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
            timestamp=datetime.now().isoformat(),
            source="eia",
            region=OREGON_REGION,
            data_type="demand",
            start_date=OREGON_TEST_DATES["start"],
            end_date=OREGON_TEST_DATES["end"],
            record_count=100,
            success=True
        )

        # Test metadata can be converted to dict (required for JSON saving)
        metadata_dict = metadata.__dict__
        assert metadata_dict["region"] == OREGON_REGION
        assert metadata_dict["data_type"] == "demand"
        assert metadata_dict["success"] is True


class TestRawDataLoaderSaveData:
    """Test RawDataLoader save functionality"""

    def setup_method(self):
        """Set up test configuration and loader"""
        # Create temporary directory for testing
        self.temp_raw_dir = tempfile.mkdtemp()
        self.raw_loader = RawDataLoader(self.temp_raw_dir)

    def teardown_method(self):
        """Clean up temporary directory"""
        shutil.rmtree(self.temp_raw_dir, ignore_errors=True)

    def test_save_oregon_demand_data(self):
        """Test saving Oregon demand data with proper metadata"""
        # Sample Oregon demand data response
        sample_response = {
            'response': {
                'data': [
                    {
                        'period': '2024-01-01T00:00:00Z',
                        'respondent': OREGON_REGION,
                        'type': 'D',
                        'value': 5234.5
                    },
                    {
                        'period': '2024-01-01T01:00:00Z',
                        'respondent': OREGON_REGION,
                        'type': 'D',
                        'value': 5100.2
                    }
                ]
            }
        }

        # Create metadata
        metadata = RawDataMetadata(
            timestamp=datetime.now().isoformat(),
            source="eia",
            region=OREGON_REGION,
            data_type="demand",
            start_date=OREGON_TEST_DATES["start"],
            end_date=OREGON_TEST_DATES["end"],
            request_params={'frequency': 'hourly', 'region': OREGON_REGION}
        )

        # Save the data
        file_path = self.raw_loader.save_raw_data(sample_response, metadata)

        # Verify file structure and content
        assert file_path.exists()
        assert "eia_demand_PACW" in file_path.name
        assert "2024-01-01_to_2024-01-07" in file_path.name

        # Load and verify content
        loaded_data = self.raw_loader.load_raw_file(file_path)
        assert loaded_data['metadata']['region'] == OREGON_REGION
        assert loaded_data['metadata']['data_type'] == "demand"
        assert loaded_data['metadata']['record_count'] == 2
        assert len(loaded_data['api_response']['response']['data']) == 2

    def test_save_oregon_generation_data(self):
        """Test saving Oregon generation data with proper metadata"""
        # Sample Oregon generation data response
        sample_response = {
            'response': {
                'data': [
                    {
                        'period': '2024-01-01T00:00:00Z',
                        'respondent': OREGON_REGION,
                        'fueltype': 'WAT',  # Hydro - important for Oregon
                        'value': 3200.1
                    },
                    {
                        'period': '2024-01-01T01:00:00Z',
                        'respondent': OREGON_REGION,
                        'fueltype': 'WND',  # Wind
                        'value': 1500.8
                    }
                ]
            }
        }

        # Create metadata
        metadata = RawDataMetadata(
            timestamp=datetime.now().isoformat(),
            source="eia",
            region=OREGON_REGION,
            data_type="generation",
            start_date=OREGON_TEST_DATES["start"],
            end_date=OREGON_TEST_DATES["end"]
        )

        # Save the data
        file_path = self.raw_loader.save_raw_data(sample_response, metadata)

        # Verify file structure and content
        assert file_path.exists()
        assert "eia_generation_PACW" in file_path.name

        # Load and verify content
        loaded_data = self.raw_loader.load_raw_file(file_path)
        assert loaded_data['metadata']['region'] == OREGON_REGION
        assert loaded_data['metadata']['data_type'] == "generation"
        assert loaded_data['metadata']['record_count'] == 2


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
        """Test listing raw files functionality"""
        # Create sample data files
        sample_response = {'response': {'data': [{'period': '2024-01-01T00:00:00Z', 'value': 100}]}}

        # Save demand data
        demand_metadata = RawDataMetadata(
            timestamp=datetime.now().isoformat(),
            source="eia",
            region="PACW",
            data_type="demand",
            start_date="2024-01-01",
            end_date="2024-01-02"
        )
        demand_path = self.raw_loader.save_raw_data(sample_response, demand_metadata)

        # Save generation data
        gen_metadata = RawDataMetadata(
            timestamp=datetime.now().isoformat(),
            source="eia",
            region="CAL",
            data_type="generation",
            start_date="2024-01-01",
            end_date="2024-01-02"
        )
        gen_path = self.raw_loader.save_raw_data(sample_response, gen_metadata)

        # Test listing all files
        all_files = self.raw_loader.list_raw_files()
        assert len(all_files) == 2
        assert demand_path in all_files
        assert gen_path in all_files

        # Test filtering by data type
        demand_files = self.raw_loader.list_raw_files(data_type="demand")
        assert len(demand_files) == 1
        assert demand_path in demand_files

    def test_extraction_summary(self):
        """Test extraction summary functionality"""
        # Create multiple sample files
        sample_response = {'response': {'data': [{'period': '2024-01-01T00:00:00Z', 'value': 100}]}}

        # Save multiple files with different metadata
        for region in ["PACW", "CAL"]:
            for data_type in ["demand", "generation"]:
                metadata = RawDataMetadata(
                    timestamp=datetime.now().isoformat(),
                    source="eia",
                    region=region,
                    data_type=data_type,
                    start_date="2024-01-01",
                    end_date="2024-01-02",
                    record_count=1,
                    response_size_bytes=100
                )
                self.raw_loader.save_raw_data(sample_response, metadata)

        # Get summary
        summary = self.raw_loader.get_extraction_summary()

        # Verify summary structure
        assert summary["total_files"] == 4
        assert summary["by_data_type"]["demand"] == 2
        assert summary["by_data_type"]["generation"] == 2
        assert summary["by_region"]["PACW"] == 2
        assert summary["by_region"]["CAL"] == 2
        assert summary["total_records"] == 4
        assert summary["total_size_bytes"] == 400

    def test_directory_structure_creation(self):
        """Test that proper directory structure is created"""
        sample_response = {'response': {'data': [{'value': 100}]}}
        metadata = RawDataMetadata(
            timestamp=datetime.now().isoformat(),
            source="eia",
            region="PACW",
            data_type="demand",
            start_date="2024-01-01",
            end_date="2024-01-02"
        )

        file_path = self.raw_loader.save_raw_data(sample_response, metadata)

        # Verify directory structure: raw_data_path/eia/2024/filename.json
        assert file_path.parent.name == "2024"
        assert file_path.parent.parent.name == "eia"
        assert file_path.parent.parent.parent == self.raw_loader.raw_data_path

    def test_failed_extraction_handling(self):
        """Test handling of failed extraction metadata"""
        # Create failed extraction metadata
        failed_metadata = RawDataMetadata(
            timestamp=datetime.now().isoformat(),
            source="eia",
            region="PACW",
            data_type="demand",
            start_date="2024-01-01",
            end_date="2024-01-02",
            success=False,
            error_message="API request failed"
        )

        # Save failed extraction (empty response)
        file_path = self.raw_loader.save_raw_data({}, failed_metadata)

        # Verify file was still created
        assert file_path.exists()

        # Load and verify failure was recorded
        loaded_data = self.raw_loader.load_raw_file(file_path)
        assert loaded_data['metadata']['success'] is False
        assert loaded_data['metadata']['error_message'] == "API request failed"

        # Verify summary counts failures
        summary = self.raw_loader.get_extraction_summary()
        assert summary["failed_extractions"] == 1
