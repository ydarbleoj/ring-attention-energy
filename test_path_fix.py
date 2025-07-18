#!/usr/bin/env python3
"""
Quick test to verify the path fix for the DAG pipeline.
"""
import sys
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_path_fix():
    """Test that the path configuration is fixed."""
    print("🔍 Testing DAG Path Fix...")

    # Test 1: Check EIAExtractConfig default path
    from core.integrations.eia.schema import EIAExtractConfig
    config = EIAExtractConfig(
        step_name='Test Extract',
        step_id='test_extract',
        source='eia',
        start_date='2024-01-01',
        end_date='2024-01-02',
        regions=['PACW'],
        data_types=['demand', 'generation'],
        api_key='test_key'
    )

    print(f"✅ EIAExtractConfig default raw_data_path: {config.raw_data_path}")
    expected_path = "data/raw/eia"
    if config.raw_data_path == expected_path:
        print(f"❌ ISSUE: Still using old path: {config.raw_data_path}")
        print("   This will create double nesting: data/raw/eia/eia/")
    else:
        print(f"✅ FIXED: Using correct path: {config.raw_data_path}")

    # Test 2: Check ApiExtractStep path usage
    from core.pipeline.steps.extract.api_extract import ApiExtractStep, ApiExtractStepConfig

    step_config = ApiExtractStepConfig(
        step_name='Test Extract',
        step_id='test_extract',
        source='eia',
        start_date='2024-01-01',
        end_date='2024-01-02',
        regions=['PACW'],
        data_types=['demand'],
        api_key='test_key',
        dry_run=True  # Don't actually run
    )

    step = ApiExtractStep(step_config)
    print(f"✅ ApiExtractStep source_config.raw_data_path: {step.source_config.raw_data_path}")

    # Test 3: Check DataCleanerStep looks for files in right place
    from core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig

    transform_config = DataCleanerStepConfig(
        step_name='Test Transform',
        step_id='test_transform',
        source='eia',
        raw_data_dir=Path("data/raw"),
        interim_data_dir=Path("data/interim"),
        dry_run=True
    )

    transform_step = DataCleanerStep(transform_config)

    # Check where it looks for files
    source_dir = transform_config.raw_data_dir / transform_config.source
    print(f"✅ DataCleanerStep looks for files in: {source_dir}")

    # Check if there are actual files there
    existing_files = list(source_dir.rglob("*.json")) if source_dir.exists() else []
    print(f"📁 Found {len(existing_files)} JSON files in {source_dir}")

    # Check the problematic double-nested directory
    nested_dir = Path("data/raw/eia/eia")
    nested_files = list(nested_dir.rglob("*.json")) if nested_dir.exists() else []
    print(f"📁 Found {len(nested_files)} JSON files in problematic nested dir: {nested_dir}")

    if nested_files and not existing_files:
        print("🚨 CONFIRMED ISSUE: Files are in nested eia/eia/ but DataCleanerStep looks in eia/")
        print("   This explains why no parquet files are created in interim/")

        # Show some example files
        print("   Example nested files:")
        for f in nested_files[:3]:
            print(f"     {f}")

    # Test 4: Check price data type support
    try:
        price_config = EIAExtractConfig(
            step_name='Test Price Extract',
            step_id='test_price_extract',
            source='eia',
            start_date='2024-01-01',
            end_date='2024-01-02',
            regions=['PACW'],
            data_types=['demand', 'generation', 'price'],
            api_key='test_key'
        )
        print(f"✅ Price data type support: {price_config.data_types}")
    except Exception as e:
        print(f"❌ Price data type failed: {e}")

if __name__ == "__main__":
    test_path_fix()
