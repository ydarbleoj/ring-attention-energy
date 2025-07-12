#!/usr/bin/env python3
"""Run region-based consolidation of EIA data.

This script consolidates all EIA JSON files into optimized region-based Parquet files
for better performance and analytics efficiency.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.pipeline.services.region_consolidation_service import RegionConsolidationService


def run_region_consolidation():
    """Run the region-based consolidation."""

    print("🚀 EIA SINGLE-FILE CONSOLIDATION")
    print("=" * 60)
    print(f"🕐 Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Initialize the consolidation service
    consolidation_service = RegionConsolidationService()

    # Define paths
    raw_data_path = Path("data/raw/eia")
    interim_data_path = Path("data/interim")

    print(f"📁 Raw data path: {raw_data_path}")
    print(f"📦 Output path: {interim_data_path}")
    print()

    # Run consolidation for all regions into a single file
    results = consolidation_service.consolidate_by_region(
        raw_data_path=raw_data_path,
        interim_data_path=interim_data_path,
        single_file=True  # This is the key change!
    )

    if results["success"]:
        print(f"\n✅ CONSOLIDATION SUCCESSFUL!")

        # Show detailed results
        print(f"\n📈 FINAL RESULTS:")
        print(f"  🎯 Files consolidated: {results['input_files']} → {results['output_files']}")
        print(f"  📊 Total records: {results['total_records']:,}")
        print(f"  ⏱️  Processing time: {results['total_time_seconds']:.1f} seconds")

        # Show single file details
        if "single_file_result" in results:
            single_result = results["single_file_result"]
            size_mb = single_result["file_size_bytes"] / (1024 * 1024)
            print(f"\n📦 OUTPUT FILE:")
            print(f"  📄 eia_all_regions_consolidated.parquet: {size_mb:.1f} MB ({single_result['total_records']:,} records)")
            print(f"  🌍 Regions: {', '.join(single_result['regions'])}")
            print(f"  📊 Data types: {', '.join(single_result['data_types'])}")

            if single_result.get("date_range"):
                date_range = single_result["date_range"]
                print(f"  📅 Date range: {date_range['start']} to {date_range['end']}")

            # Check if files meet size targets
            print(f"\n🎯 SIZE ANALYSIS:")
            print(f"  📊 File size: {size_mb:.1f} MB")
            if size_mb > 128:
                print(f"  ✅ Exceeds 128MB HDFS block size target")
            else:
                print(f"  ⚠️  Still below 128MB HDFS block size ({128 - size_mb:.1f} MB to go)")

            if size_mb > 1024:
                print(f"  🏆 Exceeds 1GB optimal Parquet size!")
            else:
                print(f"  📈 {1024 - size_mb:.1f} MB to reach 1GB optimal size")

            # Performance metrics
            records_per_mb = single_result['total_records'] / size_mb
            print(f"  📊 Compression efficiency: {records_per_mb:,.0f} records/MB")

    else:
        print(f"\n❌ CONSOLIDATION FAILED: {results.get('error', 'Unknown error')}")

    return results


if __name__ == "__main__":
    success = run_region_consolidation()
    sys.exit(0 if success["success"] else 1)
