"""Raw data loader for EIA API responses - Extract stage of ETL pipeline."""

import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass, asdict

from ..client import EIAClient

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
    Extracts raw JSON data from EIA API and saves to data/raw/ directory.

    This is the Extract stage of our ETL pipeline:
    EIA API → data/raw/*.json (with metadata)
    """

    def __init__(self, client: EIAClient, raw_data_path: Union[str, Path] = "data/raw"):
        """Initialize RawDataLoader.

        Args:
            client: EIA API client
            raw_data_path: Base path for raw data storage
        """
        self.client = client
        self.raw_data_path = Path(raw_data_path)
        self.raw_data_path.mkdir(parents=True, exist_ok=True)

    def _generate_filename(
        self,
        data_type: str,
        region: str,
        start_date: str,
        end_date: str
    ) -> str:
        """Generate filename for raw data file.

        Pattern: eia_{data_type}_{region}_{start_date}_to_{end_date}_{timestamp}.json
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"eia_{data_type}_{region}_{start_date}_to_{end_date}_{timestamp}.json"

    def _save_raw_response(
        self,
        data: Dict[str, Any],
        metadata: RawDataMetadata,
        filename: str
    ) -> Path:
        """Save raw API response with metadata to JSON file."""

        # Create subdirectory structure: data/raw/eia/2024/
        year = metadata.start_date.split('-')[0] if metadata.start_date else datetime.now().strftime("%Y")
        file_dir = self.raw_data_path / "eia" / year
        file_dir.mkdir(parents=True, exist_ok=True)

        file_path = file_dir / filename

        # Prepare complete raw data package
        raw_package = {
            "metadata": asdict(metadata),
            "api_response": data
        }

        # Save to JSON file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(raw_package, f, indent=2, ensure_ascii=False, default=str)

        logger.info(f"Saved raw data to {file_path} ({metadata.response_size_bytes} bytes, {metadata.record_count} records)")
        return file_path

    def extract_demand_data(
        self,
        region: str,
        start_date: Union[str, date],
        end_date: Union[str, date]
    ) -> Path:
        """Extract raw demand data from EIA API.

        Args:
            region: Region code (e.g., PACW, ERCO)
            start_date: Start date
            end_date: End date

        Returns:
            Path to saved raw data file
        """
        # Convert dates to strings if needed
        if isinstance(start_date, date):
            start_date = start_date.strftime("%Y-%m-%d")
        if isinstance(end_date, date):
            end_date = end_date.strftime("%Y-%m-%d")

        # Prepare metadata
        metadata = RawDataMetadata(
            timestamp=datetime.now().isoformat(),
            source="eia",
            api_endpoint="electricity/rto/region-data/data",
            region=region,
            data_type="demand",
            start_date=start_date,
            end_date=end_date
        )

        try:
            # Make direct API request to get raw JSON
            url = f"{self.client.base_url}/electricity/rto/region-data/data/"
            params = {
                'frequency': 'hourly',
                'data[0]': 'value',
                'facets[respondent][]': region,
                'facets[type][]': 'D',  # Demand
                'start': start_date,
                'end': end_date,
                'sort[0][column]': 'period',
                'sort[0][direction]': 'asc',
                'length': 5000
            }

            # Store request parameters in metadata
            metadata.request_params = params.copy()

            # Get raw response
            raw_data = self.client._make_request(url, params)

            # Update metadata with response info
            response_json = json.dumps(raw_data)
            metadata.response_size_bytes = len(response_json.encode('utf-8'))
            metadata.record_count = len(raw_data.get('response', {}).get('data', []))
            metadata.success = True

            # Generate filename and save
            filename = self._generate_filename("demand", region, start_date, end_date)
            file_path = self._save_raw_response(raw_data, metadata, filename)

            logger.info(f"Successfully extracted {metadata.record_count} demand records for {region}")
            return file_path

        except Exception as e:
            metadata.success = False
            metadata.error_message = str(e)
            logger.error(f"Failed to extract demand data for {region}: {e}")

            # Still save metadata for failed requests (useful for debugging)
            filename = self._generate_filename("demand", region, start_date, end_date)
            filename = filename.replace('.json', '_FAILED.json')
            return self._save_raw_response({}, metadata, filename)

    def extract_generation_data(
        self,
        region: str,
        start_date: Union[str, date],
        end_date: Union[str, date]
    ) -> Path:
        """Extract raw generation data from EIA API.

        Args:
            region: Region code (e.g., PACW, ERCO)
            start_date: Start date
            end_date: End date

        Returns:
            Path to saved raw data file
        """
        # Convert dates to strings if needed
        if isinstance(start_date, date):
            start_date = start_date.strftime("%Y-%m-%d")
        if isinstance(end_date, date):
            end_date = end_date.strftime("%Y-%m-%d")

        # Prepare metadata
        metadata = RawDataMetadata(
            timestamp=datetime.now().isoformat(),
            source="eia",
            api_endpoint="electricity/rto/fuel-type-data/data",
            region=region,
            data_type="generation",
            start_date=start_date,
            end_date=end_date
        )

        try:
            # Make direct API request to get raw JSON
            url = f"{self.client.base_url}/electricity/rto/fuel-type-data/data/"
            params = {
                'frequency': 'hourly',
                'data[0]': 'value',
                'facets[respondent][]': region,
                'start': start_date,
                'end': end_date,
                'sort[0][column]': 'period',
                'sort[0][direction]': 'asc',
                'length': 5000
            }

            # Store request parameters in metadata
            metadata.request_params = params.copy()

            # Get raw response
            raw_data = self.client._make_request(url, params)

            # Update metadata with response info
            response_json = json.dumps(raw_data)
            metadata.response_size_bytes = len(response_json.encode('utf-8'))
            metadata.record_count = len(raw_data.get('response', {}).get('data', []))
            metadata.success = True

            # Generate filename and save
            filename = self._generate_filename("generation", region, start_date, end_date)
            file_path = self._save_raw_response(raw_data, metadata, filename)

            logger.info(f"Successfully extracted {metadata.record_count} generation records for {region}")
            return file_path

        except Exception as e:
            metadata.success = False
            metadata.error_message = str(e)
            logger.error(f"Failed to extract generation data for {region}: {e}")

            # Still save metadata for failed requests (useful for debugging)
            filename = self._generate_filename("generation", region, start_date, end_date)
            filename = filename.replace('.json', '_FAILED.json')
            return self._save_raw_response({}, metadata, filename)

    def list_raw_files(self, data_type: Optional[str] = None) -> List[Path]:
        """List all raw data files.

        Args:
            data_type: Optional filter by data type (demand, generation)

        Returns:
            List of raw data file paths
        """
        pattern = "*.json"
        if data_type:
            pattern = f"*{data_type}*.json"

        raw_files = []
        for file_path in self.raw_data_path.rglob(pattern):
            if file_path.name.startswith("eia_"):
                raw_files.append(file_path)

        return sorted(raw_files)

    def load_raw_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """Load a raw data file and return the complete package.

        Args:
            file_path: Path to raw data file

        Returns:
            Dictionary with 'metadata' and 'api_response' keys
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"Raw data file not found: {file_path}")

        with open(file_path, 'r', encoding='utf-8') as f:
            raw_package = json.load(f)

        return raw_package

    def get_extraction_summary(self) -> Dict[str, Any]:
        """Get summary of all extracted raw data.

        Returns:
            Summary statistics of raw data extractions
        """
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
