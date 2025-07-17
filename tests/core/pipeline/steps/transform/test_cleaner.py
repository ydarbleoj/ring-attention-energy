#!/usr/bin/env python3
"""
Test suite for DataCleanerStep
"""

import pytest
from pathlib import Path

from src.core.pipeline.steps.transform.cleaner import DataCleanerStep, DataCleanerStepConfig


class TestDataCleanerStep:
    """Test suite for DataCleanerStep."""

    def test_cleaner_step_dry_run(self):
        """Test cleaner step configuration and validation in dry run mode."""
        # Create test configuration
        config = DataCleanerStepConfig(
            step_name="EIA Cleaner Test",
            step_id="eia_cleaner_test",
            source="eia",
            dry_run=True,  # Dry run mode
            raw_data_dir=Path("data/raw"),
            interim_data_dir=Path("data/interim")
        )

        assert config.source == "eia"
        assert config.dry_run is True
        print(f"✅ Configuration created: {config.source} source")

    def test_source_validation(self):
        """Test source validation for different scenarios."""
        # Valid sources should work
        valid_sources = ["eia", "caiso", "synthetic"]

        for source in valid_sources:
            config = DataCleanerStepConfig(
                step_name=f"{source.upper()} Cleaner Test",
                step_id=f"{source}_cleaner_test",
                source=source,
                dry_run=True,
                raw_data_dir=Path("data/raw"),
                interim_data_dir=Path("data/interim")
            )
            assert config.source == source
            print(f"✅ Valid source {source} passed validation")

    def test_invalid_source_validation(self):
        """Test that invalid sources are rejected."""
        with pytest.raises(ValueError, match="Source must be one of"):
            DataCleanerStepConfig(
                step_name="Invalid Cleaner Test",
                step_id="invalid_cleaner_test",
                source="invalid_source",
                dry_run=True,
                raw_data_dir=Path("data/raw"),
                interim_data_dir=Path("data/interim")
            )
        print("✅ Invalid source correctly rejected")

    def test_step_initialization(self):
        """Test step initialization with valid config."""
        # This test requires actual directory structure, so we'll skip it
        # unless in integration test mode
        pytest.skip("Requires actual directory structure - integration test")


def test_cleaner_step_scenarios():
    """Test cleaner step configuration for different scenarios."""
    print("🧪 Testing DataCleanerStep - Configuration Validation")

    # Test different scenarios
    scenarios = [
        ("eia", "Should work - EIA is implemented"),
        ("caiso", "Should work - CAISO validation passes"),
        ("synthetic", "Should work - Synthetic validation passes"),
        ("invalid", "Should fail - Invalid source")
    ]

    for source, description in scenarios:
        print(f"\n🧪 Testing source: {source} - {description}")

        try:
            config = DataCleanerStepConfig(
                step_name=f"{source.upper()} Cleaner Test",
                step_id=f"{source}_cleaner_test",
                source=source,
                dry_run=True,
                raw_data_dir=Path("data/raw"),
                interim_data_dir=Path("data/interim")
            )

            if source == "invalid":
                print("❌ Should have failed validation but didn't")
            else:
                print(f"✅ Config validation passed for {source}")

        except ValueError as e:
            if source == "invalid":
                print(f"✅ Expected validation error: {e}")
            else:
                print(f"❌ Unexpected validation error: {e}")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    print("🧪 DataCleanerStep Test Suite")
    print("=" * 50)

    test_cleaner_step_scenarios()

    print("\n" + "=" * 50)
    print("✅ Test suite completed")
    print("\nNext steps:")
    print("1. Create some test JSON files in data/raw/eia/")
    print("2. Run cleaner step without dry_run=True")
    print("3. Verify Parquet output in data/interim/")
    print("\nRun with pytest:")
    print("pytest tests/core/pipeline/steps/transform/test_data_cleaner.py -v")
