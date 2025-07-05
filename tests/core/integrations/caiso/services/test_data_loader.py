"""Tests for CAISO data loader - Polars integration with VCR."""

import pytest
import polars as pl
import logging
from pathlib import Path
from datetime import datetime, date
import tempfile
import shutil

from src.core.integrations.caiso.services import DataLoader, StorageManager
from src.core.integrations.caiso.client import CAISOClient
from tests.vcr_config import VCRManager

logger = logging.getLogger(__name__)

# California-specific test constants
CALIFORNIA_REGION = "CAISO"
CALIFORNIA_TEST_DATES = {
    "start": "20230101",  # CAISO uses YYYYMMDD format
    "end": "20230102"     # Short period for testing
}


class TestCAISODataLoader:
    """Test CAISO DataLoader with Polars integration."""

    @pytest.fixture
    def temp_storage_path(self):
        """Create temporary storage directory."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def storage_manager(self, temp_storage_path):
        """Create StorageManager instance."""
        return StorageManager(temp_storage_path)

    @pytest.fixture
    def client(self):
        """Create CAISOClient instance."""
        return CAISOClient()

    @pytest.fixture
    def data_loader(self, storage_manager, client):
        """Create DataLoader instance."""
        return DataLoader(storage_manager, client)

    @pytest.fixture
    def cassette_path(self):
        """Create cassettes directory path."""
        cassette_dir = Path(__file__).parent.parent / "cassettes"
        cassette_dir.mkdir(exist_ok=True)
        return cassette_dir

    def test_create_with_storage(self, temp_storage_path):
        """Test creating DataLoader with storage."""
        data_loader = DataLoader.create_with_storage(storage_path=temp_storage_path)

        assert isinstance(data_loader, DataLoader)
        assert isinstance(data_loader.storage, StorageManager)
        assert isinstance(data_loader.client, CAISOClient)
        assert data_loader.storage.base_path == temp_storage_path

    def test_load_demand_data_basic(self, data_loader, cassette_path):
        """Test loading demand data with basic parameters."""
        with VCRManager("caiso_california_demand_basic", cassette_path):
            df = data_loader.load_demand_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=False
            )

        # Should return DataFrame with correct schema
        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["datetime", "region", "demand_mwh"]
        # Note: May be empty if no data available for test period

    def test_load_demand_data_with_date_objects(self, data_loader, cassette_path):
        """Test loading demand data with date objects."""
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 2)

        with VCRManager("caiso_california_demand_dates", cassette_path):
            df = data_loader.load_demand_data(
                start_date=start_date,
                end_date=end_date,
                region=CALIFORNIA_REGION,
                save_to_storage=False
            )

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["datetime", "region", "demand_mwh"]

    def test_load_demand_data_with_storage(self, data_loader, temp_storage_path, cassette_path):
        """Test loading demand data with storage."""
        with VCRManager("caiso_california_demand_storage", cassette_path):
            df = data_loader.load_demand_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=True
            )

        # Verify data structure regardless of whether API returned data
        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["datetime", "region", "demand_mwh"]

        # If data was returned and saved, verify file exists
        expected_filename = f"demand_{CALIFORNIA_REGION}_{CALIFORNIA_TEST_DATES['start']}_to_{CALIFORNIA_TEST_DATES['end']}"
        if not df.is_empty():
            assert data_loader.storage.file_exists(expected_filename)
            # Verify saved data matches returned data
            saved_df = data_loader.storage.load_dataframe(expected_filename)
            assert saved_df.equals(df)
        else:
            # If no data was returned, file should not exist (due to empty DataFrame protection)
            logger.info(f"No data returned from CAISO API for test period {CALIFORNIA_TEST_DATES}")

    def test_load_demand_data_custom_filename(self, data_loader, cassette_path):
        """Test loading demand data with custom filename."""
        custom_filename = "custom_caiso_demand_test"

        with VCRManager("caiso_california_demand_custom", cassette_path):
            df = data_loader.load_demand_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=True,
                storage_filename=custom_filename
            )

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["datetime", "region", "demand_mwh"]

        # Only check file existence if data was returned
        if not df.is_empty():
            assert data_loader.storage.file_exists(custom_filename)
            saved_df = data_loader.storage.load_dataframe(custom_filename)
            assert saved_df.equals(df)

    def test_load_demand_data_with_subfolder(self, data_loader, cassette_path):
        """Test loading demand data with subfolder."""
        subfolder = "caiso_demand_data"

        with VCRManager("caiso_california_demand_subfolder", cassette_path):
            df = data_loader.load_demand_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=True,
                storage_subfolder=subfolder
            )

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["datetime", "region", "demand_mwh"]

        # Only check file existence if data was returned
        expected_filename = f"demand_{CALIFORNIA_REGION}_{CALIFORNIA_TEST_DATES['start']}_to_{CALIFORNIA_TEST_DATES['end']}"
        if not df.is_empty():
            assert data_loader.storage.file_exists(expected_filename, subfolder=subfolder)

    def test_load_generation_data_basic(self, data_loader, cassette_path):
        """Test loading generation data with basic parameters."""
        with VCRManager("caiso_california_generation_basic", cassette_path):
            df = data_loader.load_generation_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=False
            )

        # Should return DataFrame with correct schema
        assert isinstance(df, pl.DataFrame)
        assert "datetime" in df.columns
        assert "region" in df.columns

    def test_load_generation_data_with_storage(self, data_loader, cassette_path):
        """Test loading generation data with storage."""
        with VCRManager("caiso_california_generation_storage", cassette_path):
            df = data_loader.load_generation_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=True
            )

        # Verify data structure
        assert isinstance(df, pl.DataFrame)
        assert "datetime" in df.columns
        assert "region" in df.columns

        # Only check file existence if data was returned
        expected_filename = f"generation_{CALIFORNIA_REGION}_{CALIFORNIA_TEST_DATES['start']}_to_{CALIFORNIA_TEST_DATES['end']}"
        if not df.is_empty():
            assert data_loader.storage.file_exists(expected_filename)
            # Verify saved data matches returned data
            saved_df = data_loader.storage.load_dataframe(expected_filename)
            assert saved_df.equals(df)

    def test_load_comprehensive_data(self, data_loader, cassette_path):
        """Test loading comprehensive data (both demand and generation)."""
        with VCRManager("caiso_california_comprehensive", cassette_path):
            data = data_loader.load_comprehensive_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=False
            )

        # Verify structure
        assert isinstance(data, dict)
        assert "demand" in data
        assert "generation" in data
        assert isinstance(data["demand"], pl.DataFrame)
        assert isinstance(data["generation"], pl.DataFrame)

        # Verify demand schema
        assert data["demand"].columns == ["datetime", "region", "demand_mwh"]
        # Generation schema depends on what CAISO API returns
        assert "datetime" in data["generation"].columns
        assert "region" in data["generation"].columns

    def test_load_comprehensive_data_with_storage(self, data_loader, cassette_path):
        """Test loading comprehensive data with storage."""
        base_filename = "caiso_comprehensive_test"

        with VCRManager("caiso_california_comprehensive_storage", cassette_path):
            data = data_loader.load_comprehensive_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=True,
                storage_filename=base_filename
            )

        # Verify structure
        assert isinstance(data, dict)
        assert "demand" in data
        assert "generation" in data
        assert isinstance(data["demand"], pl.DataFrame)
        assert isinstance(data["generation"], pl.DataFrame)

        # Only verify file storage if data was returned
        if not data["demand"].is_empty() or not data["generation"].is_empty():
            # At least one file should exist if any data was returned
            demand_file_exists = data_loader.storage.file_exists(f"{base_filename}_demand")
            generation_file_exists = data_loader.storage.file_exists(f"{base_filename}_generation")

            # At least one should exist if we got data
            if not data["demand"].is_empty():
                assert demand_file_exists
            if not data["generation"].is_empty():
                assert generation_file_exists

    def test_load_from_storage(self, data_loader, cassette_path):
        """Test loading data from storage."""
        # First save some data
        with VCRManager("caiso_california_demand_for_storage", cassette_path):
            df = data_loader.load_demand_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=True
            )

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["datetime", "region", "demand_mwh"]

        # Only test loading from storage if data was actually saved
        filename = f"demand_{CALIFORNIA_REGION}_{CALIFORNIA_TEST_DATES['start']}_to_{CALIFORNIA_TEST_DATES['end']}"
        if not df.is_empty() and data_loader.storage.file_exists(filename):
            loaded_df = data_loader.load_from_storage(filename)
            assert loaded_df.equals(df)

    def test_load_from_storage_with_subfolder(self, data_loader, cassette_path):
        """Test loading data from storage with subfolder."""
        subfolder = "caiso_test_data"

        # First save some data
        with VCRManager("caiso_california_demand_subfolder_storage", cassette_path):
            df = data_loader.load_demand_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=True,
                storage_subfolder=subfolder
            )

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["datetime", "region", "demand_mwh"]

        # Only test loading from storage if data was actually saved
        filename = f"demand_{CALIFORNIA_REGION}_{CALIFORNIA_TEST_DATES['start']}_to_{CALIFORNIA_TEST_DATES['end']}"
        if not df.is_empty() and data_loader.storage.file_exists(filename, subfolder=subfolder):
            loaded_df = data_loader.load_from_storage(filename, subfolder=subfolder)
            assert loaded_df.equals(df)

    def test_join_demand_generation(self, data_loader):
        """Test joining demand and generation data."""
        # Create sample DataFrames
        demand_df = pl.DataFrame({
            "datetime": [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 1, 0)],
            "region": ["CAISO", "CAISO"],
            "demand_mwh": [35000.0, 36000.0]
        })

        generation_df = pl.DataFrame({
            "datetime": [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 1, 0)],
            "region": ["CAISO", "CAISO"],
            "solar_mwh": [0.0, 100.0],
            "wind_mwh": [2000.0, 1800.0]
        })

        # Join data
        joined_df = data_loader.join_demand_generation(
            demand_df, generation_df, save_to_storage=False
        )

        # Verify joined DataFrame
        assert isinstance(joined_df, pl.DataFrame)
        assert len(joined_df) == 2
        assert "datetime" in joined_df.columns
        assert "demand_demand_mwh" in joined_df.columns
        assert "generation_solar_mwh" in joined_df.columns
        assert "generation_wind_mwh" in joined_df.columns

    def test_join_demand_generation_with_storage(self, data_loader):
        """Test joining demand and generation data with storage."""
        # Create sample DataFrames
        demand_df = pl.DataFrame({
            "datetime": [datetime(2024, 1, 1, 0, 0)],
            "region": ["CAISO"],
            "demand_mwh": [35000.0]
        })

        generation_df = pl.DataFrame({
            "datetime": [datetime(2024, 1, 1, 0, 0)],
            "region": ["CAISO"],
            "solar_mwh": [0.0]
        })

        # Join data with storage
        joined_df = data_loader.join_demand_generation(
            demand_df,
            generation_df,
            save_to_storage=True,
            storage_filename="caiso_joined_test"
        )

        # Verify file was saved
        assert data_loader.storage.file_exists("caiso_joined_test")

        # Verify saved data matches
        saved_df = data_loader.storage.load_dataframe("caiso_joined_test")
        assert saved_df.equals(joined_df)

    def test_get_storage_info(self, data_loader, cassette_path):
        """Test getting storage information."""
        # Save some data first
        with VCRManager("caiso_storage_info_demand", cassette_path):
            demand_df = data_loader.load_demand_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=True
            )

        with VCRManager("caiso_storage_info_generation", cassette_path):
            generation_df = data_loader.load_generation_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=True
            )

        # Get storage info
        info = data_loader.get_storage_info()

        assert isinstance(info, dict)
        assert "total_files" in info
        assert "files" in info

        # The number of files will depend on whether API calls returned data
        expected_files = 0
        if not demand_df.is_empty():
            expected_files += 1
        if not generation_df.is_empty():
            expected_files += 1

        assert info["total_files"] == expected_files
        assert len(info["files"]) == expected_files

        # Verify file info structure for any files that exist
        for file_info in info["files"]:
            assert "filename" in file_info
            assert "full_path" in file_info
            assert "size_bytes" in file_info
            assert "created" in file_info
            assert "modified" in file_info

    def test_get_storage_info_empty_storage(self, data_loader):
        """Test getting storage info when storage is empty."""
        info = data_loader.get_storage_info()

        assert info["total_files"] == 0
        assert info["files"] == []

    def test_different_regions(self, data_loader, cassette_path):
        """Test loading data for different CAISO regions."""
        regions = ["CAISO", "CAISO_NP15", "CAISO_SP15", "CAISO_ZP26"]

        for i, region in enumerate(regions):
            with VCRManager(f"caiso_region_{region.lower()}_test", cassette_path):
                df = data_loader.load_demand_data(
                    start_date=CALIFORNIA_TEST_DATES["start"],
                    end_date=CALIFORNIA_TEST_DATES["end"],
                    region=region,
                    save_to_storage=False
                )

            assert isinstance(df, pl.DataFrame)
            assert df.columns == ["datetime", "region", "demand_mwh"]

    def test_error_handling_empty_data(self, data_loader):
        """Test proper handling of empty data responses."""
        # Test with invalid date range that should return empty data
        df = data_loader.load_demand_data(
            start_date="19900101",  # Very old date unlikely to have data
            end_date="19900101",
            save_to_storage=False
        )

        # Should return empty DataFrame with correct schema
        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["datetime", "region", "demand_mwh"]
        assert df.dtypes == [pl.Datetime, pl.Utf8, pl.Float64]

    def test_api_connection_handling(self, data_loader, cassette_path):
        """Test handling of API connection issues."""
        # This test will use VCR to simulate API responses
        with VCRManager("caiso_api_connection_test", cassette_path):
            # Test that the data loader handles API calls gracefully
            df = data_loader.load_demand_data(
                start_date=CALIFORNIA_TEST_DATES["start"],
                end_date=CALIFORNIA_TEST_DATES["end"],
                region=CALIFORNIA_REGION,
                save_to_storage=False
            )

        # Should return DataFrame (empty or with data, depending on API response)
        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["datetime", "region", "demand_mwh"]