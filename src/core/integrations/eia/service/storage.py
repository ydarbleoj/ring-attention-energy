"""Storage manager for Polars/Parquet operations."""

import polars as pl
from pathlib import Path
from typing import Optional, Union, List
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class StorageManager:
    """Manages Polars DataFrame storage operations with Parquet format."""

    def __init__(self, base_path: Union[str, Path]):
        """Initialize storage manager with base path.

        Args:
            base_path: Base path for data storage
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save_dataframe(
        self,
        df: pl.DataFrame,
        filename: str,
        subfolder: Optional[str] = None,
        compression: str = "snappy",
        overwrite: bool = False
    ) -> Path:
        """Save a Polars DataFrame to Parquet format.

        Args:
            df: Polars DataFrame to save
            filename: Name of the file (without extension)
            subfolder: Optional subfolder within base_path
            compression: Compression method for Parquet
            overwrite: Whether to overwrite existing file

        Returns:
            Path to saved file

        Raises:
            FileExistsError: If file exists and overwrite=False
            ValueError: If DataFrame is empty
        """
        if df.is_empty():
            raise ValueError("Cannot save empty DataFrame")

        # Construct file path
        if subfolder:
            file_path = self.base_path / subfolder / f"{filename}.parquet"
        else:
            file_path = self.base_path / f"{filename}.parquet"

        # Create directory if it doesn't exist
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Check if file exists
        if file_path.exists() and not overwrite:
            raise FileExistsError(f"File {file_path} already exists. Use overwrite=True to replace.")

        # Save DataFrame
        df.write_parquet(file_path, compression=compression)

        logger.info(f"Saved DataFrame with {len(df)} rows to {file_path}")
        return file_path

    def load_dataframe(
        self,
        filename: str,
        subfolder: Optional[str] = None,
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """Load a Polars DataFrame from Parquet format.

        Args:
            filename: Name of the file (without extension)
            subfolder: Optional subfolder within base_path
            columns: Optional list of columns to load

        Returns:
            Loaded Polars DataFrame

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        # Construct file path
        if subfolder:
            file_path = self.base_path / subfolder / f"{filename}.parquet"
        else:
            file_path = self.base_path / f"{filename}.parquet"

        if not file_path.exists():
            raise FileNotFoundError(f"File {file_path} not found")

        # Load DataFrame
        if columns:
            df = pl.read_parquet(file_path, columns=columns)
        else:
            df = pl.read_parquet(file_path)

        logger.info(f"Loaded DataFrame with {len(df)} rows from {file_path}")
        return df

    def file_exists(self, filename: str, subfolder: Optional[str] = None) -> bool:
        """Check if a file exists in storage.

        Args:
            filename: Name of the file (without extension)
            subfolder: Optional subfolder within base_path

        Returns:
            True if file exists, False otherwise
        """
        if subfolder:
            file_path = self.base_path / subfolder / f"{filename}.parquet"
        else:
            file_path = self.base_path / f"{filename}.parquet"

        return file_path.exists()

    def list_files(self, subfolder: Optional[str] = None) -> List[str]:
        """List all Parquet files in storage.

        Args:
            subfolder: Optional subfolder within base_path

        Returns:
            List of filenames (without extensions)
        """
        if subfolder:
            search_path = self.base_path / subfolder
        else:
            search_path = self.base_path

        if not search_path.exists():
            return []

        parquet_files = list(search_path.glob("*.parquet"))
        return [f.stem for f in parquet_files]

    def get_file_info(self, filename: str, subfolder: Optional[str] = None) -> dict:
        """Get information about a stored file.

        Args:
            filename: Name of the file (without extension)
            subfolder: Optional subfolder within base_path

        Returns:
            Dictionary with file information

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        if subfolder:
            file_path = self.base_path / subfolder / f"{filename}.parquet"
        else:
            file_path = self.base_path / f"{filename}.parquet"

        if not file_path.exists():
            raise FileNotFoundError(f"File {file_path} not found")

        stat = file_path.stat()

        return {
            "filename": filename,
            "full_path": str(file_path),
            "size_bytes": stat.st_size,
            "created": datetime.fromtimestamp(stat.st_ctime),
            "modified": datetime.fromtimestamp(stat.st_mtime),
        }

    def delete_file(self, filename: str, subfolder: Optional[str] = None) -> bool:
        """Delete a file from storage.

        Args:
            filename: Name of the file (without extension)
            subfolder: Optional subfolder within base_path

        Returns:
            True if file was deleted, False if it didn't exist
        """
        if subfolder:
            file_path = self.base_path / subfolder / f"{filename}.parquet"
        else:
            file_path = self.base_path / f"{filename}.parquet"

        if file_path.exists():
            file_path.unlink()
            logger.info(f"Deleted file {file_path}")
            return True
        else:
            logger.warning(f"File {file_path} not found for deletion")
            return False
