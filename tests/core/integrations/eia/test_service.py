"""Tests for EIA service layer - storage and data loading with Polars."""

import pytest
import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime, date
from unittest.mock import Mock, patch
import tempfile
import shutil

from src.core.integrations.eia.services import StorageManager, DataLoader
from src.core.integrations.eia.client import EIAClient
from src.core.integrations.eia.schema import EIADemandResponse, EIAGenerationResponse


class TestStorageManager:
    """Test Polars/Parquet storage operations."""

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
    def sample_dataframe(self):
        """Create sample Polars DataFrame."""
        return pl.DataFrame({
            "datetime": [
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 1, 1, 1, 0),
                datetime(2024, 1, 1, 2, 0)
            ],
            "region": ["ERCO", "ERCO", "ERCO"],
            "demand_mwh": [1000.0, 1100.0, 1200.0]
        })

    def test_init_creates_base_directory(self, temp_storage_path):
        """Test that StorageManager creates base directory."""
        storage_path = temp_storage_path / "new_storage"
        storage_manager = StorageManager(storage_path)

        assert storage_path.exists()
        assert storage_path.is_dir()
        assert storage_manager.base_path == storage_path

    def test_save_dataframe_basic(self, storage_manager, sample_dataframe, temp_storage_path):
        """Test basic DataFrame saving."""
        filename = "test_data"

        saved_path = storage_manager.save_dataframe(sample_dataframe, filename)

        expected_path = temp_storage_path / "test_data.parquet"
        assert saved_path == expected_path
        assert saved_path.exists()

        # Verify file can be loaded
        loaded_df = pl.read_parquet(saved_path)
        assert loaded_df.equals(sample_dataframe)

    def test_save_dataframe_with_subfolder(self, storage_manager, sample_dataframe, temp_storage_path):
        """Test saving DataFrame with subfolder."""
        filename = "test_data"
        subfolder = "demand_data"

        saved_path = storage_manager.save_dataframe(sample_dataframe, filename, subfolder=subfolder)

        expected_path = temp_storage_path / subfolder / "test_data.parquet"
        assert saved_path == expected_path
        assert saved_path.exists()

        # Verify subfolder was created
        assert (temp_storage_path / subfolder).exists()

    def test_save_dataframe_overwrite_protection(self, storage_manager, sample_dataframe):
        """Test that overwrite protection works."""
        filename = "test_data"

        # Save first time
        storage_manager.save_dataframe(sample_dataframe, filename)

        # Try to save again without overwrite
        with pytest.raises(FileExistsError):
            storage_manager.save_dataframe(sample_dataframe, filename, overwrite=False)

    def test_save_dataframe_with_overwrite(self, storage_manager, sample_dataframe):
        """Test saving with overwrite enabled."""
        filename = "test_data"

        # Save first time
        storage_manager.save_dataframe(sample_dataframe, filename)

        # Modify DataFrame
        modified_df = sample_dataframe.with_columns(pl.col("demand_mwh") * 2)

        # Save again with overwrite
        storage_manager.save_dataframe(modified_df, filename, overwrite=True)

        # Verify the file was overwritten
        loaded_df = storage_manager.load_dataframe(filename)
        assert loaded_df.equals(modified_df)

    def test_save_empty_dataframe_raises_error(self, storage_manager):
        """Test that saving empty DataFrame raises error."""
        empty_df = pl.DataFrame()

        with pytest.raises(ValueError, match="Cannot save empty DataFrame"):
            storage_manager.save_dataframe(empty_df, "test")

    def test_load_dataframe_basic(self, storage_manager, sample_dataframe):
        """Test basic DataFrame loading."""
        filename = "test_data"

        # Save first
        storage_manager.save_dataframe(sample_dataframe, filename)

        # Load and verify
        loaded_df = storage_manager.load_dataframe(filename)
        assert loaded_df.equals(sample_dataframe)

    def test_load_dataframe_with_subfolder(self, storage_manager, sample_dataframe):
        """Test loading DataFrame from subfolder."""
        filename = "test_data"
        subfolder = "demand_data"

        # Save first
        storage_manager.save_dataframe(sample_dataframe, filename, subfolder=subfolder)

        # Load and verify
        loaded_df = storage_manager.load_dataframe(filename, subfolder=subfolder)
        assert loaded_df.equals(sample_dataframe)

    def test_load_dataframe_with_columns(self, storage_manager, sample_dataframe):
        """Test loading DataFrame with specific columns."""
        filename = "test_data"

        # Save first
        storage_manager.save_dataframe(sample_dataframe, filename)

        # Load specific columns
        loaded_df = storage_manager.load_dataframe(filename, columns=["datetime", "demand_mwh"])

        assert loaded_df.columns == ["datetime", "demand_mwh"]
        assert len(loaded_df) == len(sample_dataframe)

    def test_load_nonexistent_file_raises_error(self, storage_manager):
        """Test that loading nonexistent file raises error."""
        with pytest.raises(FileNotFoundError):
            storage_manager.load_dataframe("nonexistent")

    def test_file_exists(self, storage_manager, sample_dataframe):
        """Test file existence checking."""
        filename = "test_data"

        # Should not exist initially
        assert not storage_manager.file_exists(filename)

        # Save file
        storage_manager.save_dataframe(sample_dataframe, filename)

        # Should exist now
        assert storage_manager.file_exists(filename)

    def test_list_files(self, storage_manager, sample_dataframe):
        """Test listing files in storage."""
        # Should be empty initially
        assert storage_manager.list_files() == []

        # Save some files
        storage_manager.save_dataframe(sample_dataframe, "file1")
        storage_manager.save_dataframe(sample_dataframe, "file2")

        files = storage_manager.list_files()
        assert set(files) == {"file1", "file2"}

    def test_list_files_with_subfolder(self, storage_manager, sample_dataframe):
        """Test listing files in subfolder."""
        subfolder = "test_subfolder"

        # Save files in subfolder
        storage_manager.save_dataframe(sample_dataframe, "file1", subfolder=subfolder)
        storage_manager.save_dataframe(sample_dataframe, "file2", subfolder=subfolder)

        # List files in subfolder
        files = storage_manager.list_files(subfolder=subfolder)
        assert set(files) == {"file1", "file2"}

        # Root should be empty
        assert storage_manager.list_files() == []

    def test_get_file_info(self, storage_manager, sample_dataframe):
        """Test getting file information."""
        filename = "test_data"

        # Save file
        storage_manager.save_dataframe(sample_dataframe, filename)

        # Get file info
        info = storage_manager.get_file_info(filename)

        assert info["filename"] == filename
        assert "full_path" in info
        assert "size_bytes" in info
        assert "created" in info
        assert "modified" in info
        assert isinstance(info["size_bytes"], int)
        assert isinstance(info["created"], datetime)
        assert isinstance(info["modified"], datetime)

    def test_get_file_info_nonexistent_raises_error(self, storage_manager):
        """Test that getting info for nonexistent file raises error."""
        with pytest.raises(FileNotFoundError):
            storage_manager.get_file_info("nonexistent")

    def test_delete_file(self, storage_manager, sample_dataframe):
        """Test file deletion."""
        filename = "test_data"

        # Save file
        storage_manager.save_dataframe(sample_dataframe, filename)
        assert storage_manager.file_exists(filename)

        # Delete file
        result = storage_manager.delete_file(filename)
        assert result is True
        assert not storage_manager.file_exists(filename)

    def test_delete_nonexistent_file(self, storage_manager):
        """Test deleting nonexistent file."""
        result = storage_manager.delete_file("nonexistent")
        assert result is False


class TestDataLoader:
    """Test DataLoader with Polars integration."""

    @pytest.fixture
    def temp_storage_path(self):
        """Create temporary storage directory."""
        temp_dir = Path(tempfile.mkdtemp())
        yield temp_dir
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def mock_client(self):
        """Create mock EIA client."""
        return Mock(spec=EIAClient)

    @pytest.fixture
    def storage_manager(self, temp_storage_path):
        """Create StorageManager instance."""
        return StorageManager(temp_storage_path)

    @pytest.fixture
    def data_loader(self, mock_client, storage_manager):
        """Create DataLoader instance."""
        return DataLoader(mock_client, storage_manager)

    @pytest.fixture
    def sample_demand_response_data(self):
        """Sample demand response data."""
        return {
            "response": {
                "data": [
                    {
                        "period": "2024-01-01T00",
                        "respondent": "ERCO",
                        "respondent-name": "ERCOT",
                        "type": "D",
                        "type-name": "Demand",
                        "value": "1000",
                        "value-units": "megawatthours"
                    },
                    {
                        "period": "2024-01-01T01",
                        "respondent": "ERCO",
                        "respondent-name": "ERCOT",
                        "type": "D",
                        "type-name": "Demand",
                        "value": "1100",
                        "value-units": "megawatthours"
                    }
                ]
            }
        }

    @pytest.fixture
    def sample_generation_response_data(self):
        """Sample generation response data."""
        return {
            "response": {
                "data": [
                    {
                        "period": "2024-01-01T00",
                        "respondent": "ERCO",
                        "respondent-name": "ERCOT",
                        "fueltype": "NG",
                        "type": "NG",
                        "type-name": "Natural Gas",
                        "value": "500",
                        "value-units": "megawatthours"
                    },
                    {
                        "period": "2024-01-01T00",
                        "respondent": "ERCO",
                        "respondent-name": "ERCOT",
                        "fueltype": "SUN",
                        "type": "SUN",
                        "type-name": "Solar",
                        "value": "200",
                        "value-units": "megawatthours"
                    }
                ]
            }
        }

    def test_create_with_storage(self, temp_storage_path):
        """Test creating DataLoader with storage."""
        api_key = "test_key"

        with patch('src.core.integrations.eia.service.data_loader.EIAClient') as mock_client_class:
            mock_client_class.return_value = Mock(spec=EIAClient)

            data_loader = DataLoader.create_with_storage(
                api_key=api_key,
                storage_path=temp_storage_path
            )

            assert isinstance(data_loader, DataLoader)
            assert isinstance(data_loader.storage, StorageManager)
            mock_client_class.assert_called_once()
            # Check that it was called with the correct api_key
            call_args = mock_client_class.call_args
            assert call_args[1]['api_key'] == api_key

    def test_load_demand_data(self, data_loader, sample_demand_response_data):
        """Test loading demand data."""
        # Create a mock pandas DataFrame that the client would return
        mock_pandas_df = pd.DataFrame({
            'timestamp': pd.to_datetime(['2024-01-01T00:00:00', '2024-01-01T01:00:00']),
            'demand': [1000.0, 1100.0]
        })

        # Configure mock client
        data_loader.client.get_electricity_demand.return_value = mock_pandas_df

        # Load data
        df = data_loader.load_demand_data(
            start_date="2024-01-01",
            end_date="2024-01-01",
            save_to_storage=False
        )

        # Verify DataFrame
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2
        assert "datetime" in df.columns
        assert "region" in df.columns
        assert "demand_mwh" in df.columns

        # Verify client was called correctly
        data_loader.client.get_electricity_demand.assert_called_once_with(
            region="ERCO",
            start_date="2024-01-01",
            end_date="2024-01-01"
        )

    def test_load_demand_data_with_storage(self, data_loader, sample_demand_response_data):
        """Test loading demand data with storage."""
        # Create a mock pandas DataFrame that the client would return
        mock_pandas_df = pd.DataFrame({
            'timestamp': pd.to_datetime(['2024-01-01T00:00:00', '2024-01-01T01:00:00']),
            'demand': [1000.0, 1100.0]
        })

        # Configure mock client
        data_loader.client.get_electricity_demand.return_value = mock_pandas_df

        # Load data with storage
        df = data_loader.load_demand_data(
            start_date="2024-01-01",
            end_date="2024-01-01",
            save_to_storage=True
        )

        # Verify data was saved
        expected_filename = "demand_ERCO_2024-01-01_to_2024-01-01"
        assert data_loader.storage.file_exists(expected_filename)

        # Verify loaded data matches saved data
        saved_df = data_loader.storage.load_dataframe(expected_filename)
        assert saved_df.equals(df)

    def test_load_generation_data(self, data_loader, sample_generation_response_data):
        """Test loading generation data."""
        # Create a mock pandas DataFrame that the client would return
        mock_pandas_df = pd.DataFrame({
            'timestamp': pd.to_datetime(['2024-01-01T00:00:00', '2024-01-01T00:00:00']),
            'natural_gas': [500.0, 0.0],
            'solar': [0.0, 200.0]
        })

        # Configure mock client
        data_loader.client.get_generation_mix.return_value = mock_pandas_df

        # Load data
        df = data_loader.load_generation_data(
            start_date="2024-01-01",
            end_date="2024-01-01",
            save_to_storage=False
        )

        # Verify DataFrame
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 2
        assert "datetime" in df.columns
        assert "region" in df.columns

    def test_load_comprehensive_data(self, data_loader, sample_demand_response_data, sample_generation_response_data):
        """Test loading comprehensive data."""
        # Create mock pandas DataFrames
        mock_demand_df = pd.DataFrame({
            'timestamp': pd.to_datetime(['2024-01-01T00:00:00', '2024-01-01T01:00:00']),
            'demand': [1000.0, 1100.0]
        })

        mock_generation_df = pd.DataFrame({
            'timestamp': pd.to_datetime(['2024-01-01T00:00:00', '2024-01-01T00:00:00']),
            'natural_gas': [500.0, 0.0],
            'solar': [0.0, 200.0]
        })

        # Configure mock client
        data_loader.client.get_electricity_demand.return_value = mock_demand_df
        data_loader.client.get_generation_mix.return_value = mock_generation_df

        # Load comprehensive data
        data = data_loader.load_comprehensive_data(
            start_date="2024-01-01",
            end_date="2024-01-01",
            save_to_storage=False
        )

        # Verify structure
        assert isinstance(data, dict)
        assert "demand" in data
        assert "generation" in data
        assert isinstance(data["demand"], pl.DataFrame)
        assert isinstance(data["generation"], pl.DataFrame)

    def test_join_demand_generation(self, data_loader):
        """Test joining demand and generation data."""
        # Create sample DataFrames
        demand_df = pl.DataFrame({
            "datetime": [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 1, 0)],
            "region": ["ERCO", "ERCO"],
            "demand_mwh": [1000.0, 1100.0]
        })

        generation_df = pl.DataFrame({
            "datetime": [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 1, 0)],
            "region": ["ERCO", "ERCO"],
            "natural_gas_mwh": [500.0, 600.0]
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
        assert "generation_natural_gas_mwh" in joined_df.columns

    def test_get_storage_info(self, data_loader, sample_demand_response_data):
        """Test getting storage information."""
        # Create a mock pandas DataFrame
        mock_pandas_df = pd.DataFrame({
            'timestamp': pd.to_datetime(['2024-01-01T00:00:00', '2024-01-01T01:00:00']),
            'demand': [1000.0, 1100.0]
        })

        # Configure mock client and save some data
        data_loader.client.get_electricity_demand.return_value = mock_pandas_df
        data_loader.load_demand_data(
            start_date="2024-01-01",
            end_date="2024-01-01",
            save_to_storage=True
        )

        # Get storage info
        info = data_loader.get_storage_info()

        assert isinstance(info, dict)
        assert "total_files" in info
        assert "files" in info
        assert info["total_files"] == 1
        assert len(info["files"]) == 1
        assert info["files"][0]["filename"] == "demand_ERCO_2024-01-01_to_2024-01-01"
