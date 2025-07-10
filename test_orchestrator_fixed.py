#!/usr/bin/env python3
"""
Test orchestrator with proper configuration setup.
"""

import asyncio
import os
import time
from datetime import date
from src.core.pipeline.orchestrator import DataLoadOrchestrator
from src.core.pipeline.collectors.eia_collector import EIACollector
from src.core.pipeline.config import EnergyPipelineConfig

async def test_orchestrator_with_proper_config():
    """Test orchestrator with properly configured API key."""

    print("🧪 Testing orchestrator with proper configuration...")

    # Get API key from environment
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        print("❌ EIA_API_KEY not found in environment")
        return

    print(f"✅ Found EIA API key: {api_key[:10]}...")

    # Setup config with API key
    config = EnergyPipelineConfig()
    config.api_keys["eia"] = api_key

    # Initialize orchestrator
    orchestrator = DataLoadOrchestrator(config)

    # Create EIA collector with direct API key
    eia_collector = EIACollector(
        api_key=api_key,
        config={
            "storage_path": "data/cache",
            "timeout": 30,
            "retry": {
                "max_retries": 3,
                "initial_delay": 1.0
            }
        }
    )

    # Register collector
    orchestrator.register_collector("eia", eia_collector)

    # Test with 1 week
    start_date = date(2024, 1, 1)
    end_date = date(2024, 1, 7)
    region = "PACW"

    print(f"📅 Test Range: {start_date} to {end_date}")
    print(f"🌍 Region: {region}")

    start_time = time.time()

    try:
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
        failed = [r for r in eia_results if not r.success]
        total_records = sum(r.records_collected for r in successful)

        print(f"\n✅ Test completed in {end_time - start_time:.2f} seconds")
        print(f"📊 Records collected: {total_records}")
        print(f"🚀 Performance: {total_records / (end_time - start_time):.2f} rec/s")
        print(f"✅ Successful operations: {len(successful)}")
        print(f"❌ Failed operations: {len(failed)}")

        if failed:
            print("Failed operations:")
            for result in failed:
                print(f"  - {result.data_type}: {result.errors}")

        if total_records > 0:
            print("\n🎯 Orchestrator is working! Ready for full historical load.")
            return True
        else:
            print("\n⚠️  No data collected. Check API configuration.")
            return False

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_orchestrator_with_proper_config())
    if success:
        print("\n🚀 Ready to proceed with full historical benchmark!")
    else:
        print("\n🛠️  Need to fix configuration issues first.")
