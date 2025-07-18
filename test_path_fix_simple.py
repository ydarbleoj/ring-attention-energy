#!/usr/bin/env python3
"""
Simple test to verify the path fix works correctly.
"""
import sys
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_path_fix():
    """Test that the path configuration fix works correctly."""
    print("🔧 Testing Path Fix...")

    try:
        from core.pipeline.steps.extract.api_extract import ApiExtractStep, ApiExtractStepConfig

        # Test 1: Default configuration (should NOT create double nesting)
        config = ApiExtractStepConfig(
            step_name='Test Extract',
            step_id='test_extract',
            source='eia',
            start_date='2024-01-01',
            end_date='2024-01-02',
            regions=['PACW'],
            data_types=['demand'],
            api_key='test_key',
            dry_run=True
        )

        print(f"✅ Step config raw_data_path: {config.raw_data_path}")

        # Create the step to see the internal source config
        step = ApiExtractStep(config)
        print(f"✅ Source config raw_data_path: {step.source_config.raw_data_path}")

        # Test 2: Check RawDataLoader will create correct path structure
        from core.integrations.eia.services.raw_data_loader import RawDataLoader

        # Test with our fixed path
        loader = RawDataLoader(step.source_config.raw_data_path)
        print(f"✅ RawDataLoader base path: {loader.raw_data_path}")

        # This is where files will be saved: base_path/eia/year/
        # So with our fix: data/raw/eia/year/ (correct!)
        # Before fix: data/raw/eia/eia/year/ (wrong!)

        expected_file_path = loader.raw_data_path / "eia" / "2024"
        print(f"✅ Files will be saved in: {expected_file_path}")

        if "eia/eia" in str(expected_file_path):
            print("❌ STILL BROKEN: Double nesting detected!")
            return False
        else:
            print("✅ FIXED: No double nesting - correct path structure!")
            return True

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_path_fix()
    if success:
        print("\n🎯 PATH FIX VERIFIED: Ready to test extraction!")
    else:
        print("\n🚨 PATH FIX FAILED: Need to investigate further")
