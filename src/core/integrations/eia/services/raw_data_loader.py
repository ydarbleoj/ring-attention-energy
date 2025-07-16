import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class RawDataMetadata:
    """Metadata for raw data extraction."""

    timestamp: str
    source: str = "eia"
    api_endpoint: str = ""
    region: str = ""
    data_type: str = ""  # demand, generation, prices
    start_date: str = ""
    end_date: str = ""
    request_params: Dict[str, Any] = None
    response_size_bytes: int = 0
    record_count: int = 0
    success: bool = True
    error_message: Optional[str] = None


class RawDataLoader:
    """
    Saves raw JSON data to data/raw/ directory.

    This handles the storage part of the Extract stage:
    JSON Response → data/raw/*.json (with metadata)

    Simple responsibility: Take JSON and save it with proper metadata.
    """

    def __init__(self, raw_data_path: Union[str, Path] = "data/raw"):
        self.raw_data_path = Path(raw_data_path)
        self.raw_data_path.mkdir(parents=True, exist_ok=True)

    def save_raw_data(
        self,
        api_response: Dict[str, Any],
        metadata: RawDataMetadata,
        filename: Optional[str] = None
    ) -> Path:
        """
        Save raw API response to file with metadata.

        Args:
            api_response: Raw JSON response from API
            metadata: Metadata about the request/response
            filename: Optional custom filename (auto-generated if not provided)

        Returns:
            Path to saved raw data file
        """
        if filename is None:
            filename = self._generate_filename(
                metadata.data_type,
                metadata.region,
                metadata.start_date,
                metadata.end_date
            )

        # Create subdirectory structure: data/raw/eia/2024/
        year = metadata.start_date.split('-')[0] if metadata.start_date else datetime.now().strftime("%Y")
        file_dir = self.raw_data_path / "eia" / year
        file_dir.mkdir(parents=True, exist_ok=True)

        file_path = file_dir / filename

        # Calculate metadata if not provided
        if metadata.response_size_bytes == 0 and api_response:
            response_json = json.dumps(api_response)
            metadata.response_size_bytes = len(response_json.encode('utf-8'))

        if metadata.record_count == 0 and api_response:
            metadata.record_count = len(api_response.get('response', {}).get('data', []))

        # Prepare complete raw data package
        raw_package = {
            "metadata": asdict(metadata),
            "api_response": api_response
        }

        # Save to JSON file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(raw_package, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"Saved raw data to {file_path} ({metadata.response_size_bytes} bytes, {metadata.record_count} records)")
        return file_path

    def _generate_filename(
        self,
        data_type: str,
        region: str,
        start_date: str,
        end_date: str
    ) -> str:
        """Generate filename for raw data file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"eia_{data_type}_{region}_{start_date}_to_{end_date}_{timestamp}.json"

    def list_raw_files(self, data_type: Optional[str] = None) -> List[Path]:
        """List all raw data files."""
        pattern = "*.json"
        if data_type:
            pattern = f"*{data_type}*.json"

        raw_files = []
        for file_path in self.raw_data_path.rglob(pattern):
            if file_path.name.startswith("eia_"):
                raw_files.append(file_path)

        return sorted(raw_files)

    def load_raw_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """Load raw data file."""
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"Raw data file not found: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            raw_package = json.load(f)

        return raw_package

    def get_extraction_summary(self) -> Dict[str, Any]:
        """Get summary of all saved raw data."""
        raw_files = self.list_raw_files()

        summary = {
            "total_files": len(raw_files),
            "by_data_type": {},
            "by_region": {},
            "by_date": {},
            "failed_extractions": 0,
            "total_records": 0,
            "total_size_bytes": 0
        }

        for file_path in raw_files:
            try:
                raw_package = self.load_raw_file(file_path)
                metadata = raw_package.get("metadata", {})

                # Count by data type
                data_type = metadata.get("data_type", "unknown")
                summary["by_data_type"][data_type] = summary["by_data_type"].get(data_type, 0) + 1

                # Count by region
                region = metadata.get("region", "unknown")
                summary["by_region"][region] = summary["by_region"].get(region, 0) + 1

                # Count by date
                start_date = metadata.get("start_date", "unknown")
                year = start_date.split('-')[0] if '-' in start_date else "unknown"
                summary["by_date"][year] = summary["by_date"].get(year, 0) + 1

                # Check for failures
                if not metadata.get("success", True):
                    summary["failed_extractions"] += 1

                # Accumulate totals
                summary["total_records"] += metadata.get("record_count", 0)
                summary["total_size_bytes"] += metadata.get("response_size_bytes", 0)

            except Exception as e:
                logger.warning(f"Could not read raw file {file_path}: {e}")
                summary["failed_extractions"] += 1

        return summary
