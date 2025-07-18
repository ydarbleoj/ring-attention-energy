#!/usr/bin/env python3
"""
Simple Transform Test - Debug DataCleanerStep
"""

import sys
import traceback
from pathlib import Path

print("🧪 SIMPLE TRANSFORM TEST")
print("=" * 40)

try:
    print("1. Testing imports...")
    from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig
    print("   ✅ DataCleanerStep imported successfully")

    print("2. Creating config...")
    config = DataCleanerStepConfig(
        step_name="test_cleaner",
        step_id="test_cleaner_001",
        source="eia",
        raw_data_dir=Path("data/raw"),
        interim_data_dir=Path("data/interim"),
        validate_data=True
    )
    print(f"   ✅ Config created: source={config.source}")

    print("3. Checking raw data...")
    raw_path = Path("data/raw/eia")
    if raw_path.exists():
        json_files = list(raw_path.rglob("*.json"))
        print(f"   ✅ Found {len(json_files)} JSON files")
    else:
        print(f"   ❌ Path not found: {raw_path}")
        sys.exit(1)

    print("4. Creating DataCleanerStep...")
    cleaner = DataCleanerStep(config)
    print(f"   ✅ DataCleanerStep created")

    print("5. Testing file discovery...")
    found_files = cleaner._find_json_files()
    print(f"   ✅ Found {len(found_files)} files via _find_json_files()")

    if found_files:
        print("   Sample files:")
        for i, file in enumerate(found_files[:3]):
            print(f"     {i+1}. {file}")

    print("\n🎉 SUCCESS: Transform step can find files!")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    traceback.print_exc()
    sys.exit(1)
