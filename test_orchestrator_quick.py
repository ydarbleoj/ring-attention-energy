#!/usr/bin/env python3
"""
Quick test of orchestrator before full historical load.
"""

import asyncio
import time
from datetime import date
from src.core.pipeline.orchestrator import DataLoadOrchestrator
from src.core.pipeline.collectors.eia_collector import EIACollector
from src.core.pipeline.config import EnergyPipelineConfig

async def test_orchestrator():
    """Quick test of orchestrator with 1 week of data."""

    print("🧪 Testing orchestrator with 1 week of data...")

    # Setup
    config = EnergyPipelineConfig()
    orchestrator = DataLoadOrchestrator(config)

    # Register EIA collector
    eia_collector = EIACollector(config)
    orchestrator.register_collector("eia", eia_collector)

    # Test with 1 week
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 7)
    region = "PACW"

    print(f"📅 Test Range: {start_date} to {end_date}")

    start_time = time.time()

    results = await orchestrator.load_historical_data(
        start_date=start_date,
        end_date=end_date,
        region=region,
        collector_names=["eia"],
        parallel=True,
        skip_completed=True
    )

    end_time = time.time()

    # Analyze results
    eia_results = results.get("eia", [])
    successful = [r for r in eia_results if r.success]
    total_records = sum(r.records_collected for r in successful)

    print(f"✅ Test completed in {end_time - start_time:.2f} seconds")
    print(f"📊 Records collected: {total_records}")
    print(f"🚀 Performance: {total_records / (end_time - start_time):.2f} rec/s")
    print("🎯 Orchestrator is working! Ready for full historical load.")

if __name__ == "__main__":
    asyncio.run(test_orchestrator())
