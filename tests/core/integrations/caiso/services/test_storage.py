import pytest
import polars as pl
from pathlib import Path
from datetime import datetime
import tempfile
import shutil

from src.core.integrations.caiso.services.storage import StorageManager


class TestCAISOStorageManager:
    """Test CAISO Polars/Parquet storage operations."""

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
    def sample_caiso_demand_dataframe(self):
        """Create sample CAISO demand DataFrame."""
        return pl.DataFrame({
            "datetime": [
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 1, 1, 1, 0),
                datetime(2024, 1, 1, 2, 0)
            ],
            "region": ["CAISO", "CAISO", "CAISO"],
            "demand_mwh": [35000.0, 36000.0, 37000.0]
        })

    @pytest.fixture
    def sample_caiso_generation_dataframe(self):
        """Create sample CAISO generation DataFrame."""
        return pl.DataFrame({
            "datetime": [
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 1, 1, 1, 0),
                datetime(2024, 1, 1, 2, 0)
            ],
            "region": ["CAISO", "CAISO", "CAISO"],
            "solar_mwh": [0.0, 100.0, 500.0],
            "wind_mwh": [2000.0, 1800.0, 1900.0],
            "natural_gas_mwh": [15000.0, 16000.0, 17000.0]
        })

    def test_init_creates_base_directory(self, temp_storage_path):
        """Test that StorageManager creates base directory."""
        storage_path = temp_storage_path / "new_caiso_storage"
        storage_manager = StorageManager(storage_path)

        assert storage_path.exists()
        assert storage_path.is_dir()
        assert storage_manager.base_path == storage_path

    def test_save_dataframe_basic(self, storage_manager, sample_caiso_demand_dataframe, temp_storage_path):
        """Test basic DataFrame saving."""
        filename = "caiso_demand_test"

        saved_path = storage_manager.save_dataframe(sample_caiso_demand_dataframe, filename)

        expected_path = temp_storage_path / "caiso_demand_test.parquet"
        assert saved_path == expected_path
        assert saved_path.exists()

        # Verify file can be loaded
        loaded_df = pl.read_parquet(saved_path)
        assert loaded_df.equals(sample_caiso_demand_dataframe)

    def test_save_dataframe_with_subfolder(self, storage_manager, sample_caiso_demand_dataframe, temp_storage_path):
        """Test saving DataFrame with subfolder."""
        filename = "caiso_demand_test"
        subfolder = "caiso_demand_data"

        saved_path = storage_manager.save_dataframe(
            sample_caiso_demand_dataframe,
            filename,
            subfolder=subfolder
        )

        expected_path = temp_storage_path / subfolder / "caiso_demand_test.parquet"
        assert saved_path == expected_path
        assert saved_path.exists()

        # Verify subfolder was created
        assert (temp_storage_path / subfolder).exists()

    def test_save_dataframe_overwrite_protection(self, storage_manager, sample_caiso_demand_dataframe):
        """Test that overwrite protection works."""
        filename = "caiso_demand_test"

        # Save first time
        storage_manager.save_dataframe(sample_caiso_demand_dataframe, filename)

        # Try to save again without overwrite
        with pytest.raises(FileExistsError):
            storage_manager.save_dataframe(sample_caiso_demand_dataframe, filename, overwrite=False)

    def test_save_dataframe_with_overwrite(self, storage_manager, sample_caiso_demand_dataframe):
        """Test saving with overwrite enabled."""
        filename = "caiso_demand_test"

        # Save first time
        storage_manager.save_dataframe(sample_caiso_demand_dataframe, filename)

        # Modify DataFrame
        modified_df = sample_caiso_demand_dataframe.with_columns(pl.col("demand_mwh") * 1.1)

        # Save again with overwrite
        storage_manager.save_dataframe(modified_df, filename, overwrite=True)

        # Verify the file was overwritten
        loaded_df = storage_manager.load_dataframe(filename)
        assert loaded_df.equals(modified_df)

    def test_save_empty_dataframe_raises_error(self, storage_manager):
        """Test that saving empty DataFrame with no columns raises error."""
        empty_df = pl.DataFrame()

        with pytest.raises(ValueError, match="Cannot save empty DataFrame with no columns"):
            storage_manager.save_dataframe(empty_df, "test")

    def test_save_empty_dataframe_with_schema_succeeds(self, storage_manager):
        """Test that saving empty DataFrame with proper schema succeeds."""
        empty_df_with_schema = pl.DataFrame(schema={
            "datetime": pl.Datetime,
            "region": pl.Utf8,
            "demand_mwh": pl.Float64
        })

        # This should succeed now
        saved_path = storage_manager.save_dataframe(empty_df_with_schema, "empty_test")
        assert saved_path.exists()

        # Verify we can load it back
        loaded_df = storage_manager.load_dataframe("empty_test")
        assert loaded_df.equals(empty_df_with_schema)

    def test_load_dataframe_basic(self, storage_manager, sample_caiso_demand_dataframe):
        """Test basic DataFrame loading."""
        filename = "caiso_demand_test"

        # Save first
        storage_manager.save_dataframe(sample_caiso_demand_dataframe, filename)

        # Load and verify
        loaded_df = storage_manager.load_dataframe(filename)
        assert loaded_df.equals(sample_caiso_demand_dataframe)

    def test_load_dataframe_with_subfolder(self, storage_manager, sample_caiso_demand_dataframe):
        """Test loading DataFrame from subfolder."""
        filename = "caiso_demand_test"
        subfolder = "caiso_demand_data"

        # Save first
        storage_manager.save_dataframe(sample_caiso_demand_dataframe, filename, subfolder=subfolder)

        # Load and verify
        loaded_df = storage_manager.load_dataframe(filename, subfolder=subfolder)
        assert loaded_df.equals(sample_caiso_demand_dataframe)

    def test_load_dataframe_with_columns(self, storage_manager, sample_caiso_generation_dataframe):
        """Test loading DataFrame with specific columns."""
        filename = "caiso_generation_test"

        # Save first
        storage_manager.save_dataframe(sample_caiso_generation_dataframe, filename)

        # Load specific columns
        loaded_df = storage_manager.load_dataframe(filename, columns=["datetime", "solar_mwh"])

        assert loaded_df.columns == ["datetime", "solar_mwh"]
        assert len(loaded_df) == len(sample_caiso_generation_dataframe)

    def test_load_nonexistent_file_raises_error(self, storage_manager):
        """Test that loading nonexistent file raises error."""
        with pytest.raises(FileNotFoundError):
            storage_manager.load_dataframe("nonexistent_caiso_file")

    def test_file_exists(self, storage_manager, sample_caiso_demand_dataframe):
        """Test file existence checking."""
        filename = "caiso_demand_test"

        # Should not exist initially
        assert not storage_manager.file_exists(filename)

        # Save file
        storage_manager.save_dataframe(sample_caiso_demand_dataframe, filename)

        # Should exist now
        assert storage_manager.file_exists(filename)

    def test_list_files(self, storage_manager, sample_caiso_demand_dataframe, sample_caiso_generation_dataframe):
        """Test listing files in storage."""
        # Should be empty initially
        assert storage_manager.list_files() == []

        # Save some files
        storage_manager.save_dataframe(sample_caiso_demand_dataframe, "caiso_demand_1")
        storage_manager.save_dataframe(sample_caiso_generation_dataframe, "caiso_generation_1")

        files = storage_manager.list_files()
        assert set(files) == {"caiso_demand_1", "caiso_generation_1"}

    def test_list_files_with_subfolder(self, storage_manager, sample_caiso_demand_dataframe):
        """Test listing files in subfolder."""
        subfolder = "caiso_test_subfolder"

        # Save files in subfolder
        storage_manager.save_dataframe(sample_caiso_demand_dataframe, "caiso_file1", subfolder=subfolder)
        storage_manager.save_dataframe(sample_caiso_demand_dataframe, "caiso_file2", subfolder=subfolder)

        # List files in subfolder
        files = storage_manager.list_files(subfolder=subfolder)
        assert set(files) == {"caiso_file1", "caiso_file2"}

        # Root should be empty
        assert storage_manager.list_files() == []

    def test_get_file_info(self, storage_manager, sample_caiso_demand_dataframe):
        """Test getting file information."""
        filename = "caiso_demand_test"

        # Save file
        storage_manager.save_dataframe(sample_caiso_demand_dataframe, filename)

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
            storage_manager.get_file_info("nonexistent_caiso_file")

    def test_delete_file(self, storage_manager, sample_caiso_demand_dataframe):
        """Test file deletion."""
        filename = "caiso_demand_test"

        # Save file
        storage_manager.save_dataframe(sample_caiso_demand_dataframe, filename)
        assert storage_manager.file_exists(filename)

        # Delete file
        result = storage_manager.delete_file(filename)
        assert result is True
        assert not storage_manager.file_exists(filename)

    def test_delete_nonexistent_file(self, storage_manager):
        """Test deleting nonexistent file."""
        result = storage_manager.delete_file("nonexistent_caiso_file")
        assert result is False

    def test_compression_options(self, storage_manager, sample_caiso_demand_dataframe):
        """Test different compression options."""
        filename = "caiso_demand_test"

        # Test with different compression methods
        for compression in ["snappy", "gzip", "lz4"]:
            storage_manager.save_dataframe(
                sample_caiso_demand_dataframe,
                f"{filename}_{compression}",
                compression=compression
            )

            # Verify file exists and can be loaded
            assert storage_manager.file_exists(f"{filename}_{compression}")
            loaded_df = storage_manager.load_dataframe(f"{filename}_{compression}")
            assert loaded_df.equals(sample_caiso_demand_dataframe)

    def test_large_dataframe_handling(self, storage_manager):
        """Test handling of larger CAISO-style DataFrames."""
        # Create a larger DataFrame similar to what CAISO might provide
        large_df = pl.DataFrame({
            "datetime": [datetime(2024, 1, 1, hour, 0) for hour in range(24)] * 30,  # 30 days
            "region": ["CAISO"] * (24 * 30),
            "demand_mwh": [30000.0 + (hour * 1000) for hour in range(24)] * 30,
            "solar_mwh": [0.0 if hour < 6 or hour > 18 else hour * 100 for hour in range(24)] * 30,
            "wind_mwh": [2000.0 + (hour * 50) for hour in range(24)] * 30
        })

        filename = "caiso_large_test"

        # Save and load
        storage_manager.save_dataframe(large_df, filename)
        loaded_df = storage_manager.load_dataframe(filename)

        assert loaded_df.equals(large_df)
        assert len(loaded_df) == 24 * 30  # 30 days of hourly data