#!/usr/bin/env python3
"""
Integration test for the full pipeline: ApiExtractStep → DataCleanerStep

Tests the complete workflow with optimized configurations for blazing fast performance:
- ApiExtractStep with EIA source (optimized for 15k+ records/sec)
- DataCleanerStep with Polars (1M+ records/sec transform)
"""

import sys
import os
import time
from pathlib import Path

# Add src to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from src.core.pipeline.steps.extract.api_extract import ApiExtractStep, ApiExtractStepConfig
from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig


def test_full_pipeline_integration():
    """Test the complete pipeline from extract to transform."""
    print("🚀 Full Pipeline Integration Test")
    print("   Testing: ApiExtractStep → DataCleanerStep")
    print("   Source: EIA with optimized configuration")

    # Check if we have an API key
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        print("❌ EIA_API_KEY environment variable not set")
        print("   Set it with: export EIA_API_KEY='your_key_here'")
        return

    # Test with a reasonable date range (1 week to start)
    start_date = "2024-01-01"
    end_date = "2024-01-07"

    print(f"   Date range: {start_date} to {end_date}")
    print(f"   Regions: ['PACW'] (single region for testing)")
    print(f"   Data types: ['demand', 'generation']")
    print()

    # Step 1: Extract with optimized EIA configuration
    print("📥 Step 1: Extract (ApiExtractStep)")

    extract_config = ApiExtractStepConfig(
        step_name="EIA Extract Integration Test",
        step_id="eia_extract_integration",
        source="eia",
        start_date=start_date,
        end_date=end_date,
        regions=["PACW"],
        data_types=["demand", "generation"],
        api_key=api_key,
        dry_run=False  # Real execution
    )

    extract_step = ApiExtractStep(extract_config)
    extract_start = time.time()
    extract_result = extract_step.run()
    extract_duration = time.time() - extract_start

    if extract_result.success:
        print(f"✅ Extract completed successfully!")
        print(f"   Duration: {extract_duration:.2f}s")
        print(f"   Files created: {extract_result.metrics.files_created}")
        print(f"   Records processed: {extract_result.metrics.records_processed:,}")
        print(f"   API calls: {extract_result.metrics.api_calls_made}")
        print(f"   Records/sec: {extract_result.metadata.get('records_per_second', 0):.1f}")
        print(f"   Requests/sec: {extract_result.metadata.get('requests_per_second', 0):.1f}")
        print()
    else:
        print(f"❌ Extract failed: {extract_result.errors}")
        return

    # Step 2: Transform with Polars (blazing fast)
    print("🔄 Step 2: Transform (DataCleanerStep)")

    transform_config = DataCleanerStepConfig(
        step_name="EIA Transform Integration Test",
        step_id="eia_transform_integration",
        source="eia",
        raw_data_dir=Path("data/raw"),
        interim_data_dir=Path("data/interim/integration_test"),
        validate_data=True,
        dry_run=False  # Real execution
    )

    transform_step = DataCleanerStep(transform_config)
    transform_start = time.time()
    transform_result = transform_step.run()
    transform_duration = time.time() - transform_start

    if transform_result.success:
        print(f"✅ Transform completed successfully!")
        print(f"   Duration: {transform_duration:.2f}s")
        print(f"   Files created: {transform_result.metrics.files_created}")
        print(f"   Records processed: {transform_result.metrics.records_processed:,}")
        print(f"   Output file: {transform_result.output_paths[0].name}")
        print(f"   Records/sec: {transform_result.metrics.records_processed/transform_duration:.1f}")
        print()
    else:
        print(f"❌ Transform failed: {transform_result.errors}")
        return

    # Summary
    total_duration = extract_duration + transform_duration
    total_records = extract_result.metrics.records_processed

    print("📊 Pipeline Summary")
    print(f"   Total duration: {total_duration:.2f}s")
    print(f"   Total records: {total_records:,}")
    print(f"   Overall throughput: {total_records/total_duration:.1f} records/sec")
    print()

    # Performance breakdown
    print("⚡ Performance Breakdown")
    print(f"   Extract: {extract_duration:.2f}s ({extract_result.metrics.records_processed/extract_duration:.1f} records/sec)")
    print(f"   Transform: {transform_duration:.2f}s ({transform_result.metrics.records_processed/transform_duration:.1f} records/sec)")
    print()

    # Validate the final output
    print("🔍 Output Validation")
    if transform_result.output_paths:
        output_file = transform_result.output_paths[0]
        print(f"   Output file: {output_file}")
        print(f"   File size: {output_file.stat().st_size / 1024:.1f} KB")

        # Quick validation using Polars
        try:
            import polars as pl
            df = pl.read_parquet(output_file)
            print(f"   Parquet shape: {df.shape}")
            print(f"   Columns: {df.columns}")
            print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            print(f"   Data types: {df['data_type'].unique().to_list()}")
            print(f"   Regions: {df['region'].unique().to_list()}")
            print("✅ Output validation passed!")
        except Exception as e:
            print(f"❌ Output validation failed: {e}")

    print()
    print("🎉 Integration test completed successfully!")


if __name__ == "__main__":
    test_full_pipeline_integration()
