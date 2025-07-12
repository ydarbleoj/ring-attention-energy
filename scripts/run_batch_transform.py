#!/usr/bin/env python3
"""Test script for batch transformation of all JSON files to Parquet.

This script runs the complete Transform stage on the entire dataset
using the BatchTransformOrchestrator for high-performance processing.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.pipeline.orchestrators.batch_transform_orchestrator import (
    BatchTransformOrchestrator,
    BatchTransformConfig
)


def run_full_batch_transform():
    """Run the complete batch transformation on all 548 files."""

    print("🚀 EIA BATCH TRANSFORMATION - FULL DATASET")
    print("=" * 60)
    print(f"🕐 Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Configure for aggressive performance
    config = BatchTransformConfig(
        files_per_batch=20,           # Larger batches for efficiency
        max_concurrent_batches=12,    # High concurrency
        validate_data=True,           # Keep data quality checks
        skip_empty_files=True,        # Skip files with 0 records
        overwrite_existing=False,     # Skip already processed files
        progress_update_interval=50   # Update every 50 files
    )

    # Initialize orchestrator
    orchestrator = BatchTransformOrchestrator(
        raw_data_path="data/raw/eia",
        interim_data_path="data/interim",
        config=config
    )

    # Run the complete transformation
    print("🔄 Starting full dataset transformation...")
    results = orchestrator.transform_all_files()

    if results["success"]:
        print("\n✅ TRANSFORMATION SUCCESSFUL!")

        # Show key metrics
        total_time = results["total_time_seconds"]
        files_processed = results["files_processed"]
        records_processed = results["total_output_records"]

        print(f"\n📈 KEY ACHIEVEMENTS:")
        print(f"  🎯 Target: Process 548 files in <5 minutes")
        print(f"  ✅ Actual: {files_processed} files in {total_time:.1f} seconds ({total_time/60:.2f} minutes)")
        print(f"  📊 Records: {records_processed:,} total records processed")

        if "performance_metrics" in results:
            perf = results["performance_metrics"]
            print(f"  ⚡ Speed: {perf['records_per_second']:,.0f} records/second")
            print(f"  📁 Throughput: {perf['files_per_second']:.1f} files/second")

        # Compare to target
        target_minutes = 5
        if total_time/60 < target_minutes:
            speedup = target_minutes / (total_time/60)
            print(f"  🏆 SUCCESS: {speedup:.1f}x faster than target!")
        else:
            print(f"  ⚠️  SLOWER: {(total_time/60)/target_minutes:.1f}x slower than target")

    else:
        print(f"\n❌ TRANSFORMATION FAILED: {results.get('error', 'Unknown error')}")

    return results


def run_test_subset():
    """Run a smaller test on a subset of files first."""

    print("🧪 EIA BATCH TRANSFORMATION - TEST SUBSET")
    print("=" * 50)

    # Test with just 2024 data first
    config = BatchTransformConfig(
        files_per_batch=10,
        max_concurrent_batches=6,
        validate_data=True,
        skip_empty_files=True,
        overwrite_existing=False,
        progress_update_interval=10
    )

    orchestrator = BatchTransformOrchestrator(
        raw_data_path="data/raw/eia",
        interim_data_path="data/interim",
        config=config
    )

    # Process only 2024 files as a test
    print("🔄 Testing with 2024 files only...")
    results = orchestrator.transform_all_files(
        years=[2024],  # Filter to 2024 only
        regions=["PACW", "ERCO"]  # Test with 2 regions
    )

    if results["success"]:
        print(f"✅ Test successful: {results['files_processed']} files in {results['total_time_seconds']:.1f}s")
        return True
    else:
        print(f"❌ Test failed: {results.get('error')}")
        return False


def run_progressive_test():
    """Run progressive testing: small → medium → full."""

    print("📈 PROGRESSIVE BATCH TRANSFORMATION TESTING")
    print("=" * 55)

    # Step 1: Test with 1 region, 1 year
    print("\n🔹 STEP 1: Single region, single year (PACW 2024)")
    config = BatchTransformConfig(
        files_per_batch=5,
        max_concurrent_batches=4,
        validate_data=True,
        skip_empty_files=True,
        overwrite_existing=False
    )

    orchestrator = BatchTransformOrchestrator(config=config)

    results1 = orchestrator.transform_all_files(
        regions=["PACW"],
        years=[2024]
    )

    if not results1["success"]:
        print(f"❌ Step 1 failed: {results1.get('error')}")
        return False

    print(f"✅ Step 1: {results1['files_processed']} files, {results1['total_time_seconds']:.1f}s")

    # Step 2: Test with 2 regions, 2 years
    print("\n🔹 STEP 2: Multiple regions and years (PACW+ERCO, 2024+2025)")

    results2 = orchestrator.transform_all_files(
        regions=["PACW", "ERCO"],
        years=[2024, 2025]
    )

    if not results2["success"]:
        print(f"❌ Step 2 failed: {results2.get('error')}")
        return False

    print(f"✅ Step 2: {results2['files_processed']} files, {results2['total_time_seconds']:.1f}s")

    # Step 3: Full dataset
    print("\n🔹 STEP 3: Full dataset (all regions, all years)")

    # Use more aggressive config for full run
    config.files_per_batch = 15
    config.max_concurrent_batches = 10
    orchestrator.batch_config = config

    results3 = orchestrator.transform_all_files()

    if results3["success"]:
        print(f"✅ FULL SUCCESS: {results3['files_processed']} files in {results3['total_time_seconds']:.1f}s")

        # Final performance analysis
        total_time_min = results3['total_time_seconds'] / 60
        if total_time_min < 5:
            print(f"🏆 ACHIEVEMENT UNLOCKED: Sub-5-minute transformation! ({total_time_min:.2f} minutes)")

        return results3
    else:
        print(f"❌ Full run failed: {results3.get('error')}")
        return False


if __name__ == "__main__":
    # Choose test mode based on command line argument
    import sys

    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
    else:
        mode = "progressive"  # Default

    if mode == "full":
        results = run_full_batch_transform()
    elif mode == "test":
        results = run_test_subset()
    elif mode == "progressive":
        results = run_progressive_test()
    else:
        print("Usage: python run_batch_transform.py [progressive|test|full]")
        print("  progressive: Small → Medium → Full testing (default)")
        print("  test: Quick test with subset of files")
        print("  full: Process complete 548-file dataset")
        sys.exit(1)

    print(f"\n🎯 NEXT STEPS:")
    print(f"1. Check data/interim/ for transformed Parquet files")
    print(f"2. Analyze data quality and completeness")
    print(f"3. Design Processed Stage aggregation strategy")
    print(f"4. Build ML/Ring-Attention optimized datasets")
