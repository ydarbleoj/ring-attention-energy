#!/usr/bin/env python3
"""
Test Transform Step - Verify DataCleanerStep with Fixed Paths
Tests that the transform step can find files and create interim data after path fix
"""

import asyncio
import os
from pathlib import Path
from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig

async def test_transform_step():
    print("🧪 TESTING TRANSFORM STEP WITH FIXED PATHS")
    print("=" * 60)

    # 1. Check current file structure
    print("\n1. 📁 CHECKING CURRENT FILE STRUCTURE:")
    raw_eia_path = Path("data/raw/eia")
    if raw_eia_path.exists():
        json_files = list(raw_eia_path.rglob("*.json"))
        print(f"   Found {len(json_files)} JSON files in data/raw/eia/")
        if json_files:
            # Show sample files
            for i, file in enumerate(json_files[:3]):
                print(f"   📄 Sample {i+1}: {file}")
            if len(json_files) > 3:
                print(f"   ... and {len(json_files) - 3} more files")
        else:
            print("   ❌ No JSON files found!")
            return False
    else:
        print("   ❌ data/raw/eia/ directory not found!")
        return False

    # 2. Create config for transform step
    print("\n2. ⚙️  CREATING TRANSFORM CONFIGURATION:")
    config = DataCleanerStepConfig(
        step_name="test_transform_step",
        step_id="test_transform_001",
        source="eia",
        raw_data_dir=Path("data/raw"),  # Fixed path
        interim_data_dir=Path("data/interim"),
        validate_data=True
    )
    print(f"   ✅ Config step_name: {config.step_name}")
    print(f"   ✅ Config source: {config.source}")
    print(f"   ✅ Config raw_data_dir: {config.raw_data_dir}")
    print(f"   ✅ Config interim_data_dir: {config.interim_data_dir}")

    # 3. Create transform step instance
    print("\n3. 🔧 CREATING DATACLEANER STEP:")
    cleaner_step = DataCleanerStep(config)
    print(f"   ✅ Step name: {cleaner_step.config.step_name}")
    print(f"   ✅ Step config type: {type(cleaner_step.config).__name__}")

    # 4. Test file finding functionality
    print("\n4. 🔍 TESTING FILE DISCOVERY:")
    try:
        # Access the private method for testing
        found_files = cleaner_step._find_json_files()
        print(f"   ✅ DataCleanerStep found {len(found_files)} files")

        if found_files:
            for i, file in enumerate(found_files[:3]):
                print(f"   📄 Found {i+1}: {file}")
                # Verify file exists and has content
                if Path(file).exists():
                    size = Path(file).stat().st_size
                    print(f"      💾 Size: {size:,} bytes")
                else:
                    print(f"      ❌ File missing: {file}")
        else:
            print("   ❌ No files found by DataCleanerStep!")

            # Debug: Check what path it's looking in
            expected_path = Path(config.raw_data_dir) / config.source
            print(f"   🔍 Expected path: {expected_path}")
            print(f"   📁 Path exists: {expected_path.exists()}")
            if expected_path.exists():
                debug_files = list(expected_path.rglob("*.json"))
                print(f"   🐛 Debug - files in expected path: {len(debug_files)}")

            return False

    except Exception as e:
        print(f"   ❌ Error in file discovery: {e}")
        return False

    # 5. Check interim directory before transform
    print("\n5. 📂 CHECKING INTERIM DIRECTORY (BEFORE):")
    interim_path = Path("data/interim")
    if interim_path.exists():
        interim_files = list(interim_path.rglob("*"))
        print(f"   📊 Files in data/interim/: {len(interim_files)}")
        for file in interim_files:
            print(f"   📄 {file}")
    else:
        print("   📁 data/interim/ directory doesn't exist yet")

    # 6. Test actual transform execution (if we have files)
    if found_files:
        print("\n6. 🚀 TESTING TRANSFORM EXECUTION:")
        try:
            print("   ⏳ Running DataCleanerStep.execute()...")
            result = await cleaner_step.execute()
            print(f"   ✅ Transform result: {result}")

            # Check if interim files were created
            print("\n7. 📂 CHECKING INTERIM DIRECTORY (AFTER):")
            if interim_path.exists():
                interim_files_after = list(interim_path.rglob("*.parquet"))
                print(f"   📊 Parquet files in data/interim/: {len(interim_files_after)}")
                for file in interim_files_after:
                    size = file.stat().st_size
                    print(f"   📄 {file} ({size:,} bytes)")

                if interim_files_after:
                    print("   🎯 SUCCESS: Transform step created interim data!")
                    return True
                else:
                    print("   ❌ No parquet files created in interim/")
                    return False
            else:
                print("   ❌ data/interim/ still doesn't exist")
                return False

        except Exception as e:
            print(f"   ❌ Error during transform execution: {e}")
            import traceback
            traceback.print_exc()
            return False
    else:
        print("\n6. ⏭️  SKIPPING TRANSFORM EXECUTION (no files found)")
        return False

if __name__ == "__main__":
    print("🧪 TRANSFORM STEP TEST")
    print("Testing DataCleanerStep with fixed path configuration")
    print("=" * 60)

    success = asyncio.run(test_transform_step())

    print("\n" + "=" * 60)
    if success:
        print("🎉 TRANSFORM STEP TEST: SUCCESS!")
        print("✅ Path fix working")
        print("✅ Files discovered correctly")
        print("✅ Transform execution successful")
        print("✅ Interim data created")
    else:
        print("❌ TRANSFORM STEP TEST: FAILED!")
        print("💡 Next steps: Debug file discovery or execution issues")

    print("=" * 60)
