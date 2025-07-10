"""Demo script for new orchestrator architecture.

This script demonstrates the complete ETL pipeline:
1. Extract: Raw data extraction with performance monitoring
2. Transform: Raw JSON to interim Parquet with Polars
3. Load: Interim to processed data combination

Tests both individual orchestrators and the full pipeline.
"""

import asyncio
import os
import logging
import sys
from datetime import date
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.pipeline.orchestrators import (
    ExtractOrchestrator,
    ExtractBatchConfig,
    TransformLoadOrchestrator,
    TransformLoadBatchConfig
)


def setup_logging():
    """Set up logging for the demo."""

    # Create logs directory
    logs_dir = Path('tests/logs')
    logs_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(logs_dir / 'orchestrator_demo.log')
        ]
    )


async def test_extract_orchestrator():
    """Test the Extract orchestrator."""

    print("🧪 Testing Extract Orchestrator")
    print("="*50)

    # Check API key
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        print("❌ EIA_API_KEY not found in environment")
        return False

    print(f"✅ Found EIA API key: {api_key[:10]}...")

    # Initialize extract orchestrator
    extract_orchestrator = ExtractOrchestrator(raw_data_path="data/raw")

    # Configure for conservative testing
    extract_config = ExtractBatchConfig(
        days_per_batch=7,  # 1 week batches
        max_concurrent_batches=2,  # Conservative concurrency
        delay_between_operations=0.5,  # 500ms delay
        max_operations_per_second=2.0,  # Conservative RPS
        log_individual_operations=True
    )

    extract_orchestrator.configure_batching(extract_config)

    # Test parameters
    region = "PACW"  # Pacific West (Oregon)
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 14)  # 2 weeks

    print(f"\n📊 Extract Test Parameters:")
    print(f"   • Region: {region}")
    print(f"   • Date Range: {start_date} to {end_date}")
    print(f"   • Data Types: demand, generation")

    try:
        # Run extraction
        extract_results = await extract_orchestrator.process_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            data_types=["demand", "generation"]
        )

        # Show results
        extract_metrics = extract_orchestrator.get_performance_metrics()

        print(f"\n✅ Extract Results:")
        print(f"   • Duration: {extract_metrics.duration_seconds:.2f}s")
        print(f"   • Operations: {extract_metrics.total_operations} total, {extract_metrics.successful_operations} successful")
        print(f"   • Success Rate: {extract_metrics.success_rate:.1f}%")
        print(f"   • Throughput: {extract_metrics.operations_per_second:.2f} ops/sec")
        print(f"   • Records: {extract_metrics.total_records_processed:,}")

        # List extracted files
        extracted_files = extract_orchestrator.list_extracted_files()
        print(f"\n📁 Extracted Files ({len(extracted_files)}):")
        for file_path in extracted_files[-5:]:  # Show last 5
            size_mb = file_path.stat().st_size / (1024 * 1024)
            print(f"   • {file_path.name} ({size_mb:.2f} MB)")

        return extract_metrics.success_rate > 80  # Success if >80% operations succeeded

    except Exception as e:
        print(f"❌ Extract test failed: {e}")
        return False


async def test_transform_load_orchestrator():
    """Test the Transform/Load orchestrator."""

    print("\n🧪 Testing Transform/Load Orchestrator")
    print("="*50)

    # Initialize transform/load orchestrator
    transform_load_orchestrator = TransformLoadOrchestrator(
        raw_data_path="data/raw",
        interim_data_path="data/interim",
        processed_data_path="data/processed"
    )

    # Configure for testing
    transform_config = TransformLoadBatchConfig(
        days_per_batch=30,  # Monthly batches
        max_concurrent_batches=3,
        validate_schema=True,
        create_data_quality_report=True,
        log_individual_operations=True
    )

    transform_load_orchestrator.configure_batching(transform_config)

    # Test parameters (matching extract test)
    region = "PACW"
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 31)  # January 2024

    print(f"\n📊 Transform/Load Test Parameters:")
    print(f"   • Region: {region}")
    print(f"   • Date Range: {start_date} to {end_date}")
    print(f"   • Data Types: demand, generation")

    try:
        # Test Phase 1: Raw to Interim
        print(f"\n🔄 Phase 1: Raw → Interim")
        interim_results = await transform_load_orchestrator.process_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            data_types=["demand", "generation"],
            stage="raw_to_interim"
        )

        interim_metrics = transform_load_orchestrator.get_performance_metrics()

        print(f"✅ Raw→Interim Results:")
        print(f"   • Duration: {interim_metrics.duration_seconds:.2f}s")
        print(f"   • Operations: {interim_metrics.successful_operations}/{interim_metrics.total_operations}")
        print(f"   • Records: {interim_metrics.total_records_processed:,}")
        print(f"   • Throughput: {interim_metrics.records_per_second:.0f} records/sec")

        # Test Phase 2: Interim to Processed
        print(f"\n🔄 Phase 2: Interim → Processed")
        processed_results = await transform_load_orchestrator.process_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            data_types=["demand", "generation"],
            stage="interim_to_processed"
        )

        processed_metrics = transform_load_orchestrator.get_performance_metrics()

        print(f"✅ Interim→Processed Results:")
        print(f"   • Duration: {processed_metrics.duration_seconds:.2f}s")
        print(f"   • Operations: {processed_metrics.successful_operations}/{processed_metrics.total_operations}")
        print(f"   • Records: {processed_metrics.total_records_processed:,}")

        # Show created files
        interim_files = list(Path("data/interim").rglob("*.parquet"))
        processed_files = list(Path("data/processed").rglob("*.parquet"))

        print(f"\n📁 Created Files:")
        print(f"   • Interim: {len(interim_files)} files")
        print(f"   • Processed: {len(processed_files)} files")

        for file_path in processed_files[-3:]:  # Show last 3
            size_mb = file_path.stat().st_size / (1024 * 1024)
            rel_path = file_path.relative_to(Path("data/processed"))
            print(f"     • {rel_path} ({size_mb:.2f} MB)")

        return interim_metrics.success_rate > 80 and processed_metrics.success_rate > 80

    except Exception as e:
        print(f"❌ Transform/Load test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_full_pipeline():
    """Test the complete ETL pipeline."""

    print("\n🚀 Testing Full ETL Pipeline")
    print("="*50)

    # Test Extract stage
    print("Stage 1: Extract")
    extract_success = await test_extract_orchestrator()

    if not extract_success:
        print("❌ Extract stage failed, stopping pipeline test")
        return False

    # Short delay between stages
    await asyncio.sleep(2)

    # Test Transform/Load stages
    print("\nStage 2+3: Transform & Load")
    transform_load_success = await test_transform_load_orchestrator()

    if not transform_load_success:
        print("❌ Transform/Load stages failed")
        return False

    print(f"\n🎉 Full ETL Pipeline Test Complete!")
    print(f"✅ All stages successful")

    # Show final data summary
    raw_files = list(Path("data/raw").rglob("*.json"))
    interim_files = list(Path("data/interim").rglob("*.parquet"))
    processed_files = list(Path("data/processed").rglob("*.parquet"))

    print(f"\n📊 Pipeline Summary:")
    print(f"   • Raw files: {len(raw_files)}")
    print(f"   • Interim files: {len(interim_files)}")
    print(f"   • Processed files: {len(processed_files)}")
    print(f"   • Data flow: JSON → Parquet → Combined Parquet")

    return True


async def test_individual_orchestrator(orchestrator_type: str):
    """Test an individual orchestrator."""

    if orchestrator_type == "extract":
        return await test_extract_orchestrator()
    elif orchestrator_type == "transform_load":
        return await test_transform_load_orchestrator()
    else:
        print(f"❌ Unknown orchestrator type: {orchestrator_type}")
        return False


if __name__ == "__main__":
    setup_logging()

    # Choose which test to run
    import sys
    if len(sys.argv) > 1:
        test_type = sys.argv[1]
        if test_type == "extract":
            success = asyncio.run(test_individual_orchestrator("extract"))
        elif test_type == "transform":
            success = asyncio.run(test_individual_orchestrator("transform_load"))
        elif test_type == "full":
            success = asyncio.run(test_full_pipeline())
        else:
            print("Usage: python demo_orchestrators.py [extract|transform|full]")
            sys.exit(1)
    else:
        # Default: run full pipeline
        success = asyncio.run(test_full_pipeline())

    if success:
        print("\n✅ Orchestrator demo completed successfully!")
        print("📂 Check data/ directories for output files")
        print("📄 Check tests/logs/orchestrator_demo.log for detailed logs")
    else:
        print("\n❌ Orchestrator demo failed")
        sys.exit(1)
