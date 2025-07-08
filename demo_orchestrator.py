"""Demo script showing how the DataLoadOrchestrator works with Option A storage."""

import asyncio
import logging
from datetime import date
from src.core.pipeline.orchestrator import DataLoadOrchestrator
from src.core.pipeline.config import EnergyPipelineConfig

# Setup logging to see what's happening
logging.basicConfig(level=logging.INFO)

async def demo_orchestrator():
    """Demo the DataLoadOrchestrator functionality."""

    # Create orchestrator
    config = EnergyPipelineConfig()
    orchestrator = DataLoadOrchestrator(config)

    print("=== DataLoadOrchestrator Demo ===\n")

    # 1. Show batch configurations
    print("1. Default Batch Configurations:")
    for name, batch_config in orchestrator.batch_configs.items():
        print(f"   {name}: {batch_config.batch_size_days} days per batch")
    print()

    # 2. Test date chunking for different collectors
    start_date = date(2023, 1, 1)
    end_date = date(2023, 12, 31)  # Full year

    print("2. Date Chunking for Full Year (2023):")
    for collector_name in ["eia", "caiso", "synthetic"]:
        chunks = orchestrator.generate_date_chunks(start_date, end_date, collector_name)
        print(f"   {collector_name}: {len(chunks)} chunks")
        print(f"      First chunk: {chunks[0]}")
        print(f"      Last chunk: {chunks[-1]}")
    print()

    # 3. Show storage filename generation
    print("3. Storage Filename Generation (Option A):")
    test_start = date(2023, 3, 1)
    test_end = date(2023, 3, 31)
    region = "PACW"

    for collector_name in ["eia", "caiso"]:
        for data_type in ["demand", "generation"]:
            filename = orchestrator._generate_storage_filename(
                collector_name, data_type, test_start, test_end, region
            )
            subfolder = orchestrator._generate_storage_subfolder(
                collector_name, data_type, test_start
            )
            full_path = f"data/processed/{subfolder}/{filename}.parquet"
            print(f"   {collector_name} {data_type}: {full_path}")
    print()

    # 4. Show progress tracking
    print("4. Progress Tracking:")
    summary = orchestrator.get_progress_summary()
    print(f"   Current progress: {summary}")
    print()

    # 5. Show historical load planning for 2000-2024
    massive_start = date(2000, 1, 1)
    massive_end = date(2024, 12, 31)

    print("5. Historical Load Planning (2000-2024):")
    for collector_name in ["eia", "caiso"]:
        chunks = orchestrator.generate_date_chunks(massive_start, massive_end, collector_name)
        total_api_calls = len(chunks) * 2  # 2 calls per chunk (demand + generation)
        print(f"   {collector_name}: {len(chunks)} chunks = {total_api_calls} total API calls")

        # Estimate time based on rate limits
        if collector_name == "eia":
            # 5,000 requests/hour limit
            estimated_hours = total_api_calls / 5000
            print(f"      Estimated time (respecting rate limits): {estimated_hours:.1f} hours")
        else:
            # No explicit limits for CAISO
            print(f"      No explicit rate limits")

    print("\n=== Demo Complete ===")

if __name__ == "__main__":
    asyncio.run(demo_orchestrator())
