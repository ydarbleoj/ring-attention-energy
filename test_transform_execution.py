#!/usr/bin/env python3
"""
Test Transform Execution - Full DataCleanerStep run
"""

import sys
import traceback
from pathlib import Path

def test_transform_execution():
    print("🧪 TRANSFORM EXECUTION TEST")
    print("=" * 50)

    try:
        print("1. Setting up...")
        from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig

        config = DataCleanerStepConfig(
            step_name="test_cleaner",
            step_id="test_cleaner_001",
            source="eia",
            raw_data_dir=Path("data/raw"),
            interim_data_dir=Path("data/interim"),
            validate_data=True
        )
        print(f"   ✅ Config created")

        cleaner = DataCleanerStep(config)
        print(f"   ✅ DataCleanerStep created")

        print("2. Checking before state...")
        interim_path = Path("data/interim")
        before_files = []
        if interim_path.exists():
            before_files = list(interim_path.glob("*.parquet"))
            print(f"   📊 Parquet files before: {len(before_files)}")
            for f in before_files:
                print(f"      📄 {f.name}")
        else:
            print(f"   📁 data/interim/ doesn't exist yet")

        print("3. Running transform execution...")
        print("   ⏳ This may take a moment to process 97 JSON files...")

        result = cleaner.run()
        print(f"   ✅ Transform completed!")
        print(f"   📊 Result success: {result.success}")
        print(f"   📊 Records processed: {result.metrics.records_processed:,}")
        print(f"   📊 Duration: {result.metrics.duration_seconds:.1f}s")
        print(f"   📊 Files created: {result.metrics.files_created}")

        if result.output_paths:
            print(f"   📄 Output files:")
            for path in result.output_paths:
                print(f"      {path}")

        if not result.success:
            print(f"   ❌ Errors: {result.errors}")
            return False

        print("4. Checking after state...")
        if interim_path.exists():
            after_files = list(interim_path.glob("*.parquet"))
            print(f"   📊 Parquet files after: {len(after_files)}")

            new_files = [f for f in after_files if f not in before_files]
            if new_files:
                print(f"   🎉 NEW FILES CREATED:")
                for f in new_files:
                    size = f.stat().st_size
                    print(f"      📄 {f.name} ({size:,} bytes)")
                return True
            else:
                print(f"   ⚠️  No new files created")
                return False
        else:
            print(f"   ❌ data/interim/ still doesn't exist")
            return False

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_transform_execution()

    print("\n" + "=" * 50)
    if success:
        print("🎉 TRANSFORM EXECUTION: SUCCESS!")
        print("✅ Files processed and parquet created")
    else:
        print("❌ TRANSFORM EXECUTION: FAILED!")
    print("=" * 50)
