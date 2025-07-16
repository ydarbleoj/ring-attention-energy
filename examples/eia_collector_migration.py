"""
Migration Example: EIACollector to EIAExtractStep

This example shows how to migrate from the old EIACollector.collect_batch_data_sync()
method to the new EIAExtractStep that follows the BaseStep interface.

BEFORE (Phase 1): Using EIACollector directly
AFTER (Phase 2): Using EIAExtractStep with standardized interface
"""

import os
from datetime import date, timedelta
from pathlib import Path

# OLD APPROACH: Using EIACollector directly
def old_approach_example():
    """Example of the old approach using EIACollector."""
    print("OLD APPROACH: EIACollector")
    print("-" * 40)

    from src.core.pipeline.collectors.eia_collector import EIACollector

    api_key = os.getenv("EIA_API_KEY", "dummy_key_for_example")

    # Initialize collector
    collector = EIACollector(
        api_key=api_key,
        raw_data_path="data/old_approach"
    )

    # Prepare parameters - mixed concerns (dates, regions, config)
    start_date = date.today() - timedelta(days=2)
    end_date = date.today() - timedelta(days=1)
    regions = ["PACW", "ERCO"]
    data_type = "demand"
    delay = 0.2

    # Call the method - returns a complex dictionary
    print(f"Calling collect_batch_data_sync()...")
    print(f"  Data type: {data_type}")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Regions: {regions}")

    # Simulate the old result format
    old_result = {
        'success': True,
        'data_type': data_type,
        'start_date': start_date,
        'end_date': end_date,
        'regions_processed': regions,
        'file_paths': ['data/old_approach/eia_demand_20240101_PACW_ERCO.json'],
        'files_created': 1,
        'errors': [],
        'error_count': 0,
        'batch_duration_seconds': 2.5,
        'total_records': 1234,
        'total_bytes': 56789,
        'api_calls': 1
    }

    print(f"OLD RESULT STRUCTURE:")
    for key, value in old_result.items():
        print(f"  {key}: {value}")

    print(f"\n❌ Problems with old approach:")
    print(f"  - Mixed responsibilities (API calls + file I/O + orchestration)")
    print(f"  - No standardized configuration validation")
    print(f"  - Complex return dictionary structure")
    print(f"  - Hard to test individual components")
    print(f"  - No standard metrics collection")
    print(f"  - Inconsistent error handling")


# NEW APPROACH: Using EIAExtractStep
def new_approach_example():
    """Example of the new approach using EIAExtractStep."""
    print("\n" + "=" * 60)
    print("NEW APPROACH: EIAExtractStep")
    print("-" * 40)

    from src.core.pipeline.steps.extract import EIAExtractStep, EIAExtractStepConfig

    api_key = os.getenv("EIA_API_KEY", "dummy_key_for_example")

    # 1. Create type-safe configuration with validation
    config = EIAExtractStepConfig(
        step_name="Migrated EIA Extract",
        step_id="migrated_extract",
        api_key=api_key,
        data_type="demand",
        start_date=(date.today() - timedelta(days=2)).strftime("%Y-%m-%d"),
        end_date=(date.today() - timedelta(days=1)).strftime("%Y-%m-%d"),
        regions=["PACW", "ERCO"],
        delay_between_operations=0.2,
        output_dir=Path("data/new_approach"),
        raw_data_path="data/new_approach/raw"
    )

    print(f"✅ Configuration created and validated:")
    print(f"  Step Name: {config.step_name}")
    print(f"  Data Type: {config.data_type}")
    print(f"  Date Range: {config.start_date} to {config.end_date}")
    print(f"  Regions: {config.regions}")

    # 2. Create step instance
    try:
        step = EIAExtractStep(config)
        print(f"✅ Step created successfully")
    except Exception as e:
        print(f"❌ Step creation failed: {e}")
        return

    # 3. Execute step - returns standardized StepOutput
    print(f"\nExecuting step...")
    result = step.run()

    print(f"\nNEW RESULT STRUCTURE:")
    print(f"  Step ID: {result.step_id}")
    print(f"  Success: {result.success}")
    print(f"  Output Paths: {result.output_paths}")
    print(f"  Errors: {result.errors}")

    print(f"\n📊 Standardized Metrics:")
    print(f"  Duration: {result.metrics.duration_seconds:.2f}s")
    print(f"  Records Processed: {result.metrics.records_processed:,}")
    print(f"  Bytes Processed: {result.metrics.bytes_processed:,}")
    print(f"  API Calls: {result.metrics.api_calls_made}")
    print(f"  Files Created: {result.metrics.files_created}")
    print(f"  Start Time: {result.metrics.start_time}")
    print(f"  End Time: {result.metrics.end_time}")

    print(f"\n🔧 Step-specific Metadata:")
    relevant_metadata = {k: v for k, v in result.metadata.items()
                        if k not in ['dry_run']}
    for key, value in relevant_metadata.items():
        print(f"  {key}: {value}")

    print(f"\n✅ Benefits of new approach:")
    print(f"  - Single responsibility (only extraction)")
    print(f"  - Type-safe configuration with Pydantic validation")
    print(f"  - Standardized StepOutput with metrics")
    print(f"  - Compatible with StepRunner and pipeline DAGs")
    print(f"  - Easy to test with mocks")
    print(f"  - Consistent error handling and logging")
    print(f"  - Ready for production monitoring")


def migration_comparison():
    """Show side-by-side comparison of key differences."""
    print("\n" + "=" * 60)
    print("MIGRATION COMPARISON")
    print("=" * 60)

    comparison = [
        ("Configuration", "Manual parameters", "Pydantic models with validation"),
        ("Input Validation", "Mixed into collector", "Dedicated validate_input() method"),
        ("Return Type", "Complex dictionary", "Standardized StepOutput"),
        ("Error Handling", "Try/catch in collector", "BaseStep error handling"),
        ("Metrics", "Mixed into result dict", "Dedicated StepMetrics object"),
        ("Testing", "Hard to mock collector", "Easy to mock with step interface"),
        ("Composition", "Not composable", "Compatible with DAGs and runners"),
        ("Monitoring", "Custom logging", "Standard step monitoring"),
        ("Responsibilities", "Multiple (API + I/O + orchestration)", "Single (extraction only)"),
    ]

    print(f"{'Aspect':<20} {'OLD (EIACollector)':<30} {'NEW (EIAExtractStep)':<30}")
    print("-" * 80)
    for aspect, old, new in comparison:
        print(f"{aspect:<20} {old:<30} {new:<30}")

    print(f"\n🎯 Migration Path:")
    print(f"  1. Replace EIACollector.collect_batch_data_sync() calls")
    print(f"  2. Create EIAExtractStepConfig instead of manual parameters")
    print(f"  3. Use StepOutput instead of parsing result dictionaries")
    print(f"  4. Leverage StepRunner for monitoring and execution")
    print(f"  5. Chain steps into DAGs for complex workflows")


if __name__ == "__main__":
    # Show old approach (commented out to avoid actual API calls)
    old_approach_example()

    # Show new approach
    new_approach_example()

    # Show comparison
    migration_comparison()

    print(f"\n🚀 Ready for Phase 3: Pipeline Composition!")
    print(f"   Next: Implement PipelineDAG to chain extract → transform → load steps")
