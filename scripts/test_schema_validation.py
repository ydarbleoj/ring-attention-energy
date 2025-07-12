#!/usr/bin/env python3
"""Quick test to validate the transform service schema based on user requirements.

Verifies that the transform service correctly handles the unified schema:
- period -> timestamp
- respondent -> region
- data_type from metadata
- fuel_type (nullable)
- type_name
- value
- value_units
"""

import sys
from pathlib import Path
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.pipeline.services.transform_service import EIATransformService


def test_schema_with_real_file():
    """Test the transform service with a real EIA file."""

    print("🧪 SCHEMA VALIDATION TEST")
    print("=" * 40)

    # Find a sample file to test with
    raw_path = Path("data/raw/eia")
    sample_files = list(raw_path.glob("**/*.json"))

    if not sample_files:
        print("❌ No JSON files found in data/raw/eia")
        return

    # Pick the first file we find
    test_file = sample_files[0]
    print(f"📁 Testing with: {test_file.name}")

    # Load and examine raw data
    with open(test_file, 'r') as f:
        raw_data = json.load(f)

    metadata = raw_data.get('metadata', {})
    api_response = raw_data.get('api_response', {}).get('response', {})
    records = api_response.get('data', [])

    print(f"\n📊 Raw Data Analysis:")
    print(f"  Records: {len(records)}")
    print(f"  Data type: {metadata.get('data_type')}")
    print(f"  Region: {metadata.get('region')}")

    if records:
        sample_record = records[0]
        print(f"\n🔍 Sample Record Fields:")
        for key, value in sample_record.items():
            print(f"    {key}: {value}")

    # Test transformation
    transform_service = EIATransformService()

    # Create temporary output file
    output_file = Path("temp_test_output.parquet")

    try:
        print(f"\n🔄 Running transformation...")
        result = transform_service.transform_json_to_parquet(
            json_file_path=test_file,
            output_path=output_file,
            validate_data=True
        )

        if result["success"]:
            print("✅ Transformation successful!")
            print(f"  Input records: {result['input_records']}")
            print(f"  Output records: {result['output_records']}")
            print(f"  Processing time: {result['processing_time_seconds']:.3f}s")

            # Check schema
            import polars as pl
            df = pl.read_parquet(output_file)

            print(f"\n📋 Output Schema:")
            expected_columns = [
                "timestamp", "region", "data_type", "fuel_type",
                "type_name", "value", "value_units", "source_file"
            ]

            for col in expected_columns:
                if col in df.columns:
                    dtype = str(df.schema[col])
                    print(f"  ✅ {col}: {dtype}")
                else:
                    print(f"  ❌ MISSING: {col}")

            # Show sample data
            print(f"\n📊 Sample Output (first 3 rows):")
            print(df.head(3))

        else:
            print(f"❌ Transformation failed: {result.get('error')}")

    finally:
        # Clean up
        if output_file.exists():
            output_file.unlink()


if __name__ == "__main__":
    test_schema_with_real_file()
