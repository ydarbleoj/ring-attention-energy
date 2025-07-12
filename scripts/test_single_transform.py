#!/usr/bin/env python3
"""Test script for single-file JSON → Parquet transformation.

This script demonstrates the Transform stage of the pipeline by processing
a single JSON file and showing the resulting data structure.
"""

import sys
from pathlib import Path
import polars as pl
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.pipeline.services.transform_service import EIATransformService


def test_single_file_transform():
    """Test transformation of a single JSON file."""

    print("🧪 EIA Transform Service - Single File Test")
    print("=" * 50)

    # Initialize transform service
    transform_service = EIATransformService()

    # Use a clean file for testing (PACW 2024 data)
    test_file = Path("data/raw/eia/2024/eia_demand_PACW_2024-01-20_to_2024-03-04_20250711_160836.json")
    output_file = Path("data/interim/test_transform_pacw_demand_2024_q1.parquet")

    if not test_file.exists():
        print(f"❌ Test file not found: {test_file}")
        return

    print(f"📄 Input file: {test_file.name}")
    print(f"📄 Output file: {output_file}")
    print()

    # Transform the file
    print("🔄 Transforming JSON to Parquet...")
    start_time = datetime.now()

    result = transform_service.transform_json_to_parquet(
        json_file_path=test_file,
        output_path=output_file,
        validate_data=True
    )

    end_time = datetime.now()
    total_time = (end_time - start_time).total_seconds()

    # Display results
    if result["success"]:
        print("✅ Transformation successful!")
        print()
        print("📊 Results:")
        print(f"  • Input records: {result['input_records']:,}")
        print(f"  • Output records: {result['output_records']:,}")
        print(f"  • Processing time: {result['processing_time_seconds']:.3f}s")
        print(f"  • Output file size: {result['file_size_bytes']:,} bytes")

        # Data quality report
        quality = result['data_quality']
        print(f"  • Data quality score: {quality['data_quality_score']:.2%}")
        print(f"  • Records dropped: {quality['records_dropped']}")

        if quality['quality_issues']:
            print("  • Quality issues:")
            for issue in quality['quality_issues']:
                print(f"    - {issue}")

        print()

        # Show the transformed data structure
        print("🔍 Examining transformed data...")
        df = pl.read_parquet(output_file)

        print(f"📋 Schema ({len(df.columns)} columns):")
        for col, dtype in zip(df.columns, df.dtypes):
            print(f"  • {col}: {dtype}")

        print()
        print(f"📅 Data sample (first 5 rows):")
        print(df.head().to_pandas().to_string(index=False))

        print()
        print(f"📈 Data summary:")
        print(f"  • Date range: {df.select(pl.col('datetime').min()).item()} to {df.select(pl.col('datetime').max()).item()}")
        print(f"  • Regions: {df.select(pl.col('region').unique()).to_series().to_list()}")
        print(f"  • Data types: {df.select(pl.col('data_type').unique()).to_series().to_list()}")

        # Show value statistics
        print(f"  • Value statistics:")
        stats = df.select([
            pl.col("value").min().alias("min_value"),
            pl.col("value").max().alias("max_value"),
            pl.col("value").mean().alias("avg_value"),
            pl.col("value").median().alias("median_value")
        ]).to_dicts()[0]

        for stat_name, stat_value in stats.items():
            print(f"    - {stat_name}: {stat_value:.2f} MWh")

    else:
        print("❌ Transformation failed!")
        print(f"Error: {result['error']}")

    print()
    print(f"⏱️  Total test time: {total_time:.3f}s")


def test_problematic_file():
    """Test transformation of the problematic ERCO file with data quality issues."""

    print("\n" + "=" * 50)
    print("🧪 Testing Problematic File (ERCO with missing data)")
    print("=" * 50)

    transform_service = EIATransformService()

    # Test the problematic ERCO file
    test_file = Path("data/raw/eia/2025/eia_demand_ERCO_2025-01-14_to_2025-02-27_20250711_160924.json")
    output_file = Path("data/interim/test_transform_erco_demand_2025_problematic.parquet")

    if not test_file.exists():
        print(f"❌ Test file not found: {test_file}")
        return

    print(f"📄 Input file: {test_file.name}")

    result = transform_service.transform_json_to_parquet(
        json_file_path=test_file,
        output_path=output_file,
        validate_data=True
    )

    if result["success"]:
        print("✅ Problematic file handled successfully!")
        print(f"  • Input records: {result['input_records']:,}")
        print(f"  • Output records: {result['output_records']:,}")

        quality = result['data_quality']
        print(f"  • Data quality score: {quality['data_quality_score']:.2%}")
        print(f"  • Records dropped: {quality['records_dropped']}")

        if quality['quality_issues']:
            print("  • Quality issues found:")
            for issue in quality['quality_issues']:
                print(f"    - {issue}")
    else:
        print(f"❌ Failed to process problematic file: {result['error']}")


if __name__ == "__main__":
    test_single_file_transform()
    test_problematic_file()

    print("\n" + "🎯 Next Steps:")
    print("1. Review the interim Parquet format above")
    print("2. Decide on processed stage aggregation strategy")
    print("3. Run batch transformation on all 548 files")
    print("4. Design multi-region/multi-type combination logic")
