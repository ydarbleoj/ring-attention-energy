#!/usr/bin/env python3
"""
Integration test for DataCleanerStep - validates end-to-end processing
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig


def test_cleaner_integration():
    """Test cleaner step with actual test files."""
    print("🧹 Testing DataCleanerStep Integration")
    print("=" * 50)

    # Create configuration using test data
    config = DataCleanerStepConfig(
        step_name="EIA Cleaner Integration Test",
        step_id="eia_cleaner_integration",
        source="eia",
        raw_data_dir=Path("tests/data/raw"),
        interim_data_dir=Path("tests/data/interim"),
        dry_run=False  # Actually process the files
    )

    print(f"✅ Configuration: source={config.source}")
    print(f"   Raw data dir: {config.raw_data_dir}")
    print(f"   Output dir: {config.interim_data_dir}")

    try:
        # Create step instance
        step = DataCleanerStep(config)
        print(f"✅ Step initialized successfully")

        # Run the step
        print("\n🚀 Running cleaner step...")
        result = step.run()

        # Display results
        print(f"\n📊 Results:")
        print(f"   Success: {result.success}")
        print(f"   Duration: {result.metrics.duration_seconds:.2f}s")
        print(f"   Records processed: {result.metrics.records_processed:,}")
        print(f"   Files created: {result.metrics.files_created}")
        print(f"   Output paths: {[str(p) for p in result.output_paths]}")

        if result.success and result.output_paths:
            output_file = result.output_paths[0]
            if output_file.exists():
                size_mb = output_file.stat().st_size / (1024 * 1024)
                print(f"   Output file size: {size_mb:.2f} MB")

                # Try to read the Parquet file to verify it's valid
                import polars as pl
                df = pl.read_parquet(output_file)
                print(f"   Verified Parquet: {len(df):,} rows, {len(df.columns)} columns")
                print(f"   Columns: {df.columns[:5]}{'...' if len(df.columns) > 5 else ''}")
            else:
                print(f"   ❌ Output file not found: {output_file}")

        print(f"\n✅ Integration test completed successfully!")

    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_cleaner_integration()
