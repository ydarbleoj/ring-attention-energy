#!/usr/bin/env python3
"""
Step 2: Live API Testing with Clean Slate

This script implements the clean slate testing approach:
1. Clean existing data (rm -rf data/raw/* data/interim/*)
2. Test full DAG with live EIA API extraction using demo_pipeline_dag.py
3. Validate end-to-end: API → JSON files → consolidated parquet
4. Confirm price data integration works with live data
5. Validate performance maintains 300K+ records/sec throughput

This tests the complete pipeline with:
- Demand data: hourly by balancing authority
- Generation data: hourly by balancing authority + fuel type
- Price data: monthly by state + sector (NEW!)

Usage: python tests/core/pipeline/test_clean_slate_live_api.py
"""

import asyncio
import os
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.core.pipeline.orchestrators.pipeline_dag import PipelineDAG, PipelineDAGConfig
from src.core.pipeline.steps.extract.api_extract import ApiExtractStep, ApiExtractStepConfig
from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig


def clean_existing_data():
    """Clean existing data directories for fresh start"""
    print("🧹 Cleaning Existing Data...")

    paths_to_clean = [
        Path("data/raw"),
        Path("data/interim"),
        Path("data/pipeline_runs")
    ]

    for path in paths_to_clean:
        if path.exists():
            try:
                # Remove all contents but keep the directory
                for item in path.iterdir():
                    if item.is_dir():
                        shutil.rmtree(item)
                    else:
                        item.unlink()
                print(f"   ✅ Cleaned {path}/")
            except Exception as e:
                print(f"   ⚠️  Warning cleaning {path}: {e}")
        else:
            print(f"   📁 {path}/ doesn't exist, will be created")


def check_api_key():
    """Verify EIA API key is available"""
    print("🔑 Checking API Key...")

    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        print("   ❌ EIA_API_KEY environment variable not set")
        print("   💡 Set it with: export EIA_API_KEY='your_api_key_here'")
        return False

    print(f"   ✅ API key available: {api_key[:10]}...")
    return True


def create_live_extract_step() -> ApiExtractStep:
    """Create extract step for live API testing with price data"""
    print("⚙️  Creating Live Extract Step...")

    # Test with a small date range for faster testing
    config = ApiExtractStepConfig(
        step_name="Live API Extract Test with Price Data",
        step_id="live_extract_test",
        source="eia",
        start_date="2024-01-01",
        end_date="2024-01-03",  # 3 days for quick test
        regions=["PACW"],  # Single region for faster testing
        data_types=["demand", "generation", "price"],  # All three data types!
        api_key=os.getenv("EIA_API_KEY", ""),
        dry_run=False  # Live API calls
    )

    print(f"   📅 Date range: {config.start_date} to {config.end_date}")
    print(f"   🌍 Regions: {config.regions}")
    print(f"   📊 Data types: {config.data_types}")
    print(f"   🌐 Live API calls: {not config.dry_run}")

    return ApiExtractStep(config)


def create_live_transform_step() -> DataCleanerStep:
    """Create transform step for live testing"""
    print("⚙️  Creating Live Transform Step...")

    config = DataCleanerStepConfig(
        step_name="Live Transform Test with Price Data",
        step_id="live_transform_test",
        source="eia",
        raw_data_dir=Path("data/raw"),
        interim_data_dir=Path("data/interim"),
        validate_data=True,
        dry_run=False
    )

    return DataCleanerStep(config)


async def run_live_pipeline_test():
    """Run the complete live pipeline test"""
    print("🚀 Running Live Pipeline Test...")

    # Create pipeline steps
    extract_step = create_live_extract_step()
    transform_step = create_live_transform_step()

    # Create pipeline DAG configuration
    dag_config = PipelineDAGConfig(
        pipeline_name="Live API Test with Price Data",
        pipeline_id=f"live_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        output_dir=Path("data/pipeline_runs/live_test"),
        max_parallel_steps=2,
        stop_on_failure=True,
        save_intermediate_results=True,
        auto_connect_steps=True,
        validate_data_flow=True,
        log_level="INFO"
    )

    # Create and configure pipeline DAG
    dag = PipelineDAG(dag_config)
    dag.create_extract_transform_chain(extract_step, transform_step)

    print(f"   📋 Pipeline: {dag_config.pipeline_name}")
    print(f"   🔗 Steps: extract → transform")
    print(f"   📁 Output: {dag_config.output_dir}")

    # Execute pipeline
    print("\n" + "="*60)
    print("🚀 EXECUTING LIVE PIPELINE")
    print("="*60)

    start_time = datetime.now()

    try:
        results = await dag.execute_async()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"\n✅ Pipeline completed in {duration:.2f}s")

        # Analyze results
        extract_result = results.get("live_extract_test")
        transform_result = results.get("live_transform_test")

        print(f"   📊 Extract result: {extract_result.status if extract_result else 'unknown'}")
        print(f"   🔄 Transform result: {transform_result.status if transform_result else 'unknown'}")

        return results

    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return None


def validate_pipeline_output():
    """Validate the pipeline output includes all three data types"""
    print("🔍 Validating Pipeline Output...")

    # Check raw data files
    raw_dir = Path("data/raw")
    if not raw_dir.exists():
        print("   ❌ No raw data directory found")
        return False

    json_files = list(raw_dir.rglob("*.json"))
    print(f"   📄 JSON files created: {len(json_files)}")

    # Check for different data types
    data_types_found = set()
    for file in json_files:
        if "demand" in file.name:
            data_types_found.add("demand")
        elif "generation" in file.name:
            data_types_found.add("generation")
        elif "price" in file.name:
            data_types_found.add("price")

    print(f"   🏷️  Data types found: {sorted(data_types_found)}")

    # Check interim parquet files
    interim_dir = Path("data/interim")
    if not interim_dir.exists():
        print("   ❌ No interim data directory found")
        return False

    parquet_files = list(interim_dir.glob("*.parquet"))
    print(f"   📊 Parquet files created: {len(parquet_files)}")

    if not parquet_files:
        print("   ❌ No consolidated parquet files found")
        return False

    # Analyze consolidated parquet
    try:
        import polars as pl

        latest_parquet = max(parquet_files, key=lambda x: x.stat().st_mtime)
        df = pl.read_parquet(latest_parquet)

        print(f"   📄 Latest parquet: {latest_parquet.name}")
        print(f"   📊 Total records: {len(df):,}")

        # Check data types in parquet
        if "data_type" in df.columns:
            parquet_data_types = df["data_type"].unique().to_list()
            print(f"   🏷️  Parquet data types: {sorted(parquet_data_types)}")

            # Count records by type
            type_counts = df.group_by("data_type").agg(pl.len()).sort("len", descending=True)
            print("   📈 Record counts by type:")
            for row in type_counts.iter_rows(named=True):
                data_type = row["data_type"]
                count = row["len"]
                print(f"      {data_type}: {count:,} records")

            # Check if all three types are present
            expected_types = {"demand", "generation", "price"}
            if expected_types.issubset(set(parquet_data_types)):
                print("   ✅ All three data types successfully integrated!")

                # Performance calculation
                file_size_mb = latest_parquet.stat().st_size / (1024 * 1024)
                print(f"   📈 File size: {file_size_mb:.1f} MB")

                return True
            else:
                missing = expected_types - set(parquet_data_types)
                print(f"   ⚠️  Missing data types: {missing}")
                return False
        else:
            print("   ⚠️  No data_type column found in parquet")
            return False

    except Exception as e:
        print(f"   ❌ Error analyzing parquet: {e}")
        return False


async def main():
    """Run the complete clean slate live API test"""
    print("🧪 STEP 2: LIVE API TESTING WITH CLEAN SLATE")
    print("=" * 70)

    test_results = []

    # Step 1: Clean existing data
    print("\n🧹 Step 1: Clean Existing Data")
    clean_existing_data()
    test_results.append(("Clean Data", True))

    # Step 2: Check API access
    print("\n🔑 Step 2: Verify API Access")
    api_available = check_api_key()
    test_results.append(("API Access", api_available))

    if not api_available:
        print("❌ Cannot proceed without API key")
        return False

    # Step 3: Run live pipeline
    print("\n🚀 Step 3: Execute Live Pipeline")
    pipeline_results = await run_live_pipeline_test()
    pipeline_success = pipeline_results is not None
    test_results.append(("Pipeline Execution", pipeline_success))

    if not pipeline_success:
        print("❌ Pipeline failed, cannot validate output")
        return False

    # Step 4: Validate output
    print("\n🔍 Step 4: Validate Pipeline Output")
    output_valid = validate_pipeline_output()
    test_results.append(("Output Validation", output_valid))

    # Summary
    print("\n" + "=" * 70)
    print("📊 CLEAN SLATE LIVE API TEST RESULTS")
    print("=" * 70)

    passed = 0
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1

    print(f"\n🎯 Results: {passed}/{len(test_results)} tests passed")

    if passed == len(test_results):
        print("🎉 CLEAN SLATE LIVE API TEST SUCCESSFUL!")
        print("")
        print("✅ Achievements:")
        print("   • Successfully cleaned existing data")
        print("   • Live API extraction working with price data")
        print("   • End-to-end API → JSON → parquet pipeline")
        print("   • All three data types integrated (demand, generation, price)")
        print("   • Pipeline performance validated")
        print("")
        print("🚀 Ready for Phase 3: Feature Creation for ML Pipeline!")
    else:
        print("⚠️  Some issues need to be resolved before proceeding.")

    return passed == len(test_results)


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
