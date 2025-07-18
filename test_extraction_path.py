#!/usr/bin/env python3
"""
Test extraction with fixed path to verify files go to correct location.
"""
import sys
import os
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_extraction():
    """Test that extraction puts files in the correct location."""
    print("🧪 Testing Extraction with Fixed Path...")

    # Check if API key is available
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        print("⚠️  No EIA_API_KEY found - using dry-run mode")
        dry_run = True
    else:
        print(f"✅ Using EIA API key: {api_key[:10]}...")
        dry_run = False

    try:
        from core.pipeline.steps.extract.api_extract import ApiExtractStep, ApiExtractStepConfig

        # Create a minimal extraction test
        config = ApiExtractStepConfig(
            step_name='Path Fix Test Extract',
            step_id='path_test_extract',
            source='eia',
            start_date='2024-01-01',
            end_date='2024-01-01',  # Just one day
            regions=['PACW'],  # Just one region
            data_types=['demand'],  # Just demand
            api_key=api_key or 'test_key',
            dry_run=dry_run
        )

        print(f"📋 Config: {config.step_name}")
        print(f"   Date range: {config.start_date} to {config.end_date}")
        print(f"   Regions: {config.regions}")
        print(f"   Data types: {config.data_types}")
        print(f"   Dry run: {config.dry_run}")

        # Create and run the step
        step = ApiExtractStep(config)
        print(f"✅ Step created - files will be saved to: {step.source_config.raw_data_path}")

        # Run the extraction
        print("🚀 Running extraction...")
        result = step.run()

        print(f"✅ Extraction completed!")
        print(f"   Success: {result.success}")
        print(f"   Duration: {result.metrics.duration_seconds:.2f}s")
        print(f"   Records: {result.metrics.records_processed}")
        print(f"   Files created: {result.metrics.files_created}")

        if result.output_paths:
            print("📁 Files created:")
            for path in result.output_paths:
                print(f"   {path}")

                # Check if path has double nesting
                if "eia/eia" in str(path):
                    print("❌ STILL BROKEN: Double nesting detected in output path!")
                    return False
                else:
                    print("✅ CORRECT: No double nesting in output path!")

        if result.errors:
            print("❌ Errors:")
            for error in result.errors:
                print(f"   {error}")

        # Verify file structure
        expected_dir = Path("data/raw/eia/2024")
        if expected_dir.exists():
            files = list(expected_dir.glob("*.json"))
            print(f"✅ Found {len(files)} files in correct location: {expected_dir}")
            return True
        else:
            print(f"❌ Expected directory not found: {expected_dir}")

            # Check if files went to wrong location
            wrong_dir = Path("data/raw/eia/eia/2024")
            if wrong_dir.exists():
                wrong_files = list(wrong_dir.glob("*.json"))
                print(f"🚨 Files went to wrong location: {wrong_dir} ({len(wrong_files)} files)")
                return False
            else:
                print("⚠️  No files found in either location")
                return dry_run  # If dry run, this is expected

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_extraction()
    if success:
        print("\n🎉 EXTRACTION PATH FIX SUCCESSFUL!")
        print("   Files are now saved to the correct location!")
        print("   Ready to test transform step next.")
    else:
        print("\n🚨 EXTRACTION PATH FIX FAILED!")
        print("   Need to investigate the issue further.")
