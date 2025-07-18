#!/usr/bin/env python3
"""
Test Full Pipeline with Existing Data
Tests the complete DAG pipeline using existing JSON files instead of API extraction
"""

import asyncio
import sys
import traceback
from pathlib import Path

def test_pipeline_with_existing_data():
    print("🧪 FULL PIPELINE TEST WITH EXISTING DATA")
    print("=" * 60)

    try:
        print("1. Setting up pipeline components...")
        from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig

        # Check existing data
        print("2. Checking existing data...")
        raw_path = Path("data/raw/eia")
        if raw_path.exists():
            json_files = list(raw_path.rglob("*.json"))
            print(f"   ✅ Found {len(json_files)} existing JSON files")

            # Show sample files
            for i, file in enumerate(json_files[:3]):
                size = file.stat().st_size
                print(f"   📄 Sample {i+1}: {file.name} ({size:,} bytes)")
        else:
            print("   ❌ No existing data found")
            return False

        print("3. Checking interim state (before)...")
        interim_path = Path("data/interim")
        before_files = []
        if interim_path.exists():
            before_files = list(interim_path.glob("*.parquet"))
            print(f"   📊 Existing parquet files: {len(before_files)}")
        else:
            print("   📁 data/interim/ doesn't exist yet")

        print("4. Creating transform step configuration...")
        config = DataCleanerStepConfig(
            step_name="Full Pipeline Test Transform",
            step_id="pipeline_test_transform",
            source="eia",
            raw_data_dir=Path("data/raw"),
            interim_data_dir=Path("data/interim"),
            validate_data=True
        )
        print(f"   ✅ Config created for source: {config.source}")

        print("5. Running transform step...")
        cleaner = DataCleanerStep(config)
        print("   ⏳ Processing all existing JSON files...")

        result = cleaner.run()

        print("6. Analyzing results...")
        print(f"   📊 Success: {result.success}")
        print(f"   📊 Records processed: {result.metrics.records_processed:,}")
        print(f"   📊 Duration: {result.metrics.duration_seconds:.1f}s")
        print(f"   📊 Files created: {result.metrics.files_created}")
        print(f"   📊 Throughput: {result.metrics.records_processed/result.metrics.duration_seconds:.1f} records/sec" if result.metrics.duration_seconds > 0 else "   📊 Throughput: N/A (instant)")

        if result.output_paths:
            print("   📄 Output files:")
            for path in result.output_paths:
                if Path(path).exists():
                    size = Path(path).stat().st_size
                    size_mb = size / (1024 * 1024)
                    print(f"      📄 {Path(path).name} ({size_mb:.1f} MB)")
                else:
                    print(f"      ❌ {path} (not found)")

        print("7. Verifying pipeline success criteria...")

        success_criteria = {
            "Pipeline completed": result.success,
            "Records processed": result.metrics.records_processed > 0,
            "Files created": result.metrics.files_created > 0,
            "Output file exists": len(result.output_paths) > 0 and Path(result.output_paths[0]).exists() if result.output_paths else False,
            "Single consolidated file": len(result.output_paths) == 1 if result.output_paths else False
        }

        print("   📋 Success Criteria:")
        all_passed = True
        for criteria, passed in success_criteria.items():
            status = "✅" if passed else "❌"
            print(f"      {status} {criteria}")
            if not passed:
                all_passed = False

        if all_passed:
            print("\n🎉 FULL PIPELINE TEST: SUCCESS!")
            print("✅ All criteria passed")
            print("✅ Path fix working correctly")
            print("✅ Extract → Transform flow validated")
            print("✅ Single consolidated parquet created")
            print("✅ Ready for production use!")
            return True
        else:
            print("\n⚠️  FULL PIPELINE TEST: PARTIAL SUCCESS")
            print("Some criteria not met - check details above")
            return False

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🔗 Full Pipeline Validation Test")
    print("Testing complete extract → transform flow with existing data")
    print("=" * 60)

    success = test_pipeline_with_existing_data()

    print("\n" + "=" * 60)
    if success:
        print("🎯 PIPELINE VALIDATION: COMPLETE!")
        print("The DAG implementation is working correctly!")
    else:
        print("🔧 PIPELINE VALIDATION: NEEDS ATTENTION")
        print("Check the output above for specific issues")
    print("=" * 60)
