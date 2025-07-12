"""Region-based consolidation service for EIA data.

Consolidates individual JSON files into optimized region-based Parquet files
for better performance and analytics efficiency.
"""

import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
from collections import defaultdict

from .transform_service import EIATransformService

logger = logging.getLogger(__name__)


class RegionConsolidationService:
    """Service for consolidating EIA data by region into optimal file sizes."""

    def __init__(self):
        """Initialize the consolidation service."""
        self.logger = logging.getLogger(__name__)
        self.transform_service = EIATransformService()

    def consolidate_by_region(
        self,
        raw_data_path: Path,
        interim_data_path: Path,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None,
        single_file: bool = True
    ) -> Dict[str, Any]:
        """Consolidate all EIA data into optimized Parquet files.

        Args:
            raw_data_path: Path to raw JSON files
            interim_data_path: Path for consolidated Parquet files
            regions: Optional list of regions to process
            years: Optional list of years to process
            single_file: If True, consolidate all regions into one file

        Returns:
            Dictionary with consolidation results and metrics
        """
        start_time = datetime.now()

        if single_file:
            print("🏗️  EIA SINGLE-FILE CONSOLIDATION")
        else:
            print("🏗️  EIA REGION-BASED CONSOLIDATION")
        print("=" * 50)

        # Discover all files
        all_files = self._discover_files(raw_data_path, regions, years)

        if not all_files:
            return {"success": False, "error": "No files found to consolidate"}

        print(f"📁 Found {len(all_files)} files to consolidate")

        if single_file:
            # Consolidate all files into a single file
            result = self._consolidate_all_files(all_files, interim_data_path)

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            if result["success"]:
                size_mb = result["file_size_bytes"] / (1024 * 1024)
                print(f"\n🎉 SINGLE-FILE CONSOLIDATION COMPLETE")
                print(f"  ⏱️  Total time: {processing_time:.1f} seconds")
                print(f"  📁 Input files: {len(all_files)}")
                print(f"  📦 Output file: 1")
                print(f"  📊 Total records: {result['total_records']:,}")
                print(f"  📄 File size: {size_mb:.1f} MB")
                print(f"  📈 Compression ratio: {len(all_files)}:1 → 1:1")

                # Regions included
                regions_found = set()
                for file_path in all_files:
                    region = self._extract_region_from_filename(file_path)
                    if region:
                        regions_found.add(region)
                print(f"  🌍 Regions included: {', '.join(sorted(regions_found))}")

            return {
                "success": result["success"],
                "total_time_seconds": processing_time,
                "input_files": len(all_files),
                "output_files": 1 if result["success"] else 0,
                "total_records": result.get("total_records", 0),
                "single_file_result": result
            }
        else:
            # Original region-based logic
            # Group files by region
            files_by_region = defaultdict(list)
            for file_path in all_files:
                region = self._extract_region_from_filename(file_path)
                if region:
                    files_by_region[region].append(file_path)

            print(f"🌍 Regions found: {list(files_by_region.keys())}")

            # Process each region
            consolidation_results = {}
            total_input_files = 0
            total_output_files = 0
            total_records = 0

            for region, region_files in files_by_region.items():
                print(f"\n🔄 Processing region: {region} ({len(region_files)} files)")

                result = self._consolidate_region(
                    region=region,
                    files=region_files,
                    interim_data_path=interim_data_path
                )

                consolidation_results[region] = result
                total_input_files += len(region_files)

                if result["success"]:
                    total_output_files += 1
                    total_records += result["total_records"]

                    # Print region summary
                    size_mb = result["file_size_bytes"] / (1024 * 1024)
                    print(f"  ✅ {region}: {result['total_records']:,} records → {size_mb:.1f} MB")
                else:
                    print(f"  ❌ {region}: Failed - {result.get('error', 'Unknown error')}")

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            # Final summary
            print(f"\n🎉 CONSOLIDATION COMPLETE")
            print(f"  ⏱️  Total time: {processing_time:.1f} seconds")
            print(f"  📁 Input files: {total_input_files}")
            print(f"  📦 Output files: {total_output_files}")
            print(f"  📊 Total records: {total_records:,}")
            print(f"  📈 Compression ratio: {total_input_files}:1 → {total_output_files}:1")

            return {
                "success": True,
                "total_time_seconds": processing_time,
                "input_files": total_input_files,
                "output_files": total_output_files,
                "total_records": total_records,
                "regions_processed": list(files_by_region.keys()),
                "results_by_region": consolidation_results
            }

    def _discover_files(
        self,
        raw_data_path: Path,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> List[Path]:
        """Discover all EIA JSON files to process."""

        files = list(raw_data_path.glob("**/*.json"))

        # Filter by regions and years if specified
        filtered_files = []

        for file_path in files:
            filename = file_path.name

            # Skip non-EIA files
            if not filename.startswith("eia_"):
                continue

            # Extract region and year
            file_region = self._extract_region_from_filename(file_path)
            file_year = self._extract_year_from_filename(file_path)

            # Apply filters
            if regions and file_region not in regions:
                continue
            if years and file_year not in years:
                continue

            filtered_files.append(file_path)

        return sorted(filtered_files)

    def _extract_region_from_filename(self, file_path: Path) -> Optional[str]:
        """Extract region code from filename."""
        try:
            # Format: eia_{data_type}_{region}_{date}_to_{date}_{timestamp}.json
            parts = file_path.name.split("_")
            if len(parts) >= 3:
                return parts[2]  # Region is the 3rd part
        except:
            pass
        return None

    def _extract_year_from_filename(self, file_path: Path) -> Optional[int]:
        """Extract year from filename."""
        try:
            # Look for date pattern
            parts = file_path.name.split("_")
            for part in parts:
                if "-" in part and len(part) >= 4:
                    year_str = part.split("-")[0]
                    if year_str.isdigit() and len(year_str) == 4:
                        return int(year_str)
        except:
            pass
        return None

    def _consolidate_region(
        self,
        region: str,
        files: List[Path],
        interim_data_path: Path
    ) -> Dict[str, Any]:
        """Consolidate all files for a single region."""

        try:
            # Process all files and collect DataFrames
            all_dataframes = []
            total_input_records = 0

            for file_path in files:
                # Transform individual file to DataFrame
                result = self.transform_service.transform_json_to_parquet(
                    json_file_path=file_path,
                    output_path=Path("/tmp/temp_transform.parquet"),  # Temporary
                    validate_data=True
                )

                if not result["success"]:
                    self.logger.warning(f"Failed to transform {file_path}: {result.get('error')}")
                    continue

                # Load the transformed data
                df = pl.read_parquet("/tmp/temp_transform.parquet")
                all_dataframes.append(df)
                total_input_records += result["input_records"]

                # Clean up temp file
                Path("/tmp/temp_transform.parquet").unlink(missing_ok=True)

            if not all_dataframes:
                return {"success": False, "error": "No valid data found"}

            # Combine all DataFrames
            consolidated_df = pl.concat(all_dataframes)

            # Sort by timestamp for optimal compression and query performance
            if "timestamp" in consolidated_df.columns:
                consolidated_df = consolidated_df.sort("timestamp")

            # Generate output filename
            output_filename = f"energy_{region.lower()}_consolidated.parquet"
            output_path = interim_data_path / output_filename

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save consolidated file
            consolidated_df.write_parquet(
                output_path,
                compression="snappy",
                statistics=True
            )

            # Get file size
            file_size = output_path.stat().st_size

            return {
                "success": True,
                "region": region,
                "input_files": len(files),
                "total_records": len(consolidated_df),
                "input_records": total_input_records,
                "output_file": output_path,
                "file_size_bytes": file_size,
                "data_types": consolidated_df["data_type"].unique().to_list(),
                "date_range": {
                    "start": consolidated_df["timestamp"].min(),
                    "end": consolidated_df["timestamp"].max()
                } if "timestamp" in consolidated_df.columns else None
            }

        except Exception as e:
            self.logger.error(f"Failed to consolidate region {region}: {str(e)}")
            return {
                "success": False,
                "region": region,
                "error": str(e)
            }

    def _consolidate_all_files(
        self,
        files: List[Path],
        interim_data_path: Path
    ) -> Dict[str, Any]:
        """Consolidate all files into a single optimized Parquet file."""

        try:
            print(f"🔄 Processing all {len(files)} files into single consolidated file...")

            # Process all files and collect DataFrames
            all_dataframes = []
            total_input_records = 0
            regions_found = set()
            data_types_found = set()

            for i, file_path in enumerate(files):
                if i % 50 == 0:  # Progress update every 50 files
                    print(f"  📊 Progress: {i}/{len(files)} files processed...")

                # Transform individual file to DataFrame
                result = self.transform_service.transform_json_to_parquet(
                    json_file_path=file_path,
                    output_path=Path("/tmp/temp_transform.parquet"),  # Temporary
                    validate_data=True
                )

                if not result["success"]:
                    self.logger.warning(f"Failed to transform {file_path}: {result.get('error')}")
                    continue

                # Load the transformed data
                df = pl.read_parquet("/tmp/temp_transform.parquet")
                all_dataframes.append(df)
                total_input_records += result["input_records"]

                # Track metadata
                if "region" in df.columns:
                    regions_found.update(df["region"].unique().to_list())
                if "data_type" in df.columns:
                    data_types_found.update(df["data_type"].unique().to_list())

                # Clean up temp file
                Path("/tmp/temp_transform.parquet").unlink(missing_ok=True)

            if not all_dataframes:
                return {"success": False, "error": "No valid data found"}

            print(f"  ✅ Successfully processed {len(all_dataframes)} files")
            print(f"  🌍 Regions: {', '.join(sorted(regions_found))}")
            print(f"  📊 Data types: {', '.join(sorted(data_types_found))}")

            # Combine all DataFrames
            print(f"  🔗 Combining {len(all_dataframes)} DataFrames...")
            consolidated_df = pl.concat(all_dataframes)

            # Sort by timestamp, then region for optimal compression and query performance
            if "timestamp" in consolidated_df.columns and "region" in consolidated_df.columns:
                print(f"  📈 Sorting by timestamp and region...")
                consolidated_df = consolidated_df.sort(["timestamp", "region"])
            elif "timestamp" in consolidated_df.columns:
                print(f"  📈 Sorting by timestamp...")
                consolidated_df = consolidated_df.sort("timestamp")

            # Generate output filename
            output_filename = "eia_all_regions_consolidated.parquet"
            output_path = interim_data_path / output_filename

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save consolidated file
            print(f"  💾 Writing consolidated file...")
            consolidated_df.write_parquet(
                output_path,
                compression="snappy",
                statistics=True
            )

            # Get file size
            file_size = output_path.stat().st_size

            return {
                "success": True,
                "input_files": len(files),
                "total_records": len(consolidated_df),
                "input_records": total_input_records,
                "output_file": output_path,
                "file_size_bytes": file_size,
                "regions": sorted(regions_found),
                "data_types": sorted(data_types_found),
                "date_range": {
                    "start": consolidated_df["timestamp"].min(),
                    "end": consolidated_df["timestamp"].max()
                } if "timestamp" in consolidated_df.columns else None
            }

        except Exception as e:
            self.logger.error(f"Failed to consolidate all files: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
