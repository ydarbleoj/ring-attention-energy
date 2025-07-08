"""Quick test of EIA with 90-day batch size to see if we can beat 60 days."""

import asyncio
import time
from datetime import date
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.core.pipeline.orchestrator import DataLoadOrchestrator, BatchConfig
from src.core.pipeline.config import EnergyPipelineConfig
from src.core.pipeline.collectors.eia_collector import EIACollector
import os
from dotenv import load_dotenv

load_dotenv()

async def test_eia_90_days():
    """Test EIA with 90-day batch size."""

    print("🧪 Testing EIA with 90-day batch size")
    print("=" * 40)

    # Get API key
    eia_api_key = os.getenv("EIA_API_KEY")
    if not eia_api_key:
        print("❌ EIA_API_KEY not found")
        return

    # Test parameters
    test_start = date(2024, 1, 1)
    test_end = date(2024, 1, 7)  # 1 week test
    region = "PACW"

    # Create orchestrator with 90-day batch
    config = EnergyPipelineConfig()
    orchestrator = DataLoadOrchestrator(config)
    orchestrator.batch_configs["eia"] = BatchConfig("eia", batch_size_days=90)

    # Create and register EIA collector
    eia_collector = EIACollector(eia_api_key)
    orchestrator.register_collector("eia", eia_collector)

    # Time the operation
    print(f"🔄 Running EIA collection: {test_start} to {test_end}")
    start_time = time.time()

    results = await orchestrator.load_historical_data(
        start_date=test_start,
        end_date=test_end,
        region=region,
        collector_names=["eia"],
        parallel=False,
        skip_completed=False
    )

    end_time = time.time()

    # Calculate metrics
    collector_results = results["eia"]
    total_records = sum(r.records_collected for r in collector_results)
    total_time = end_time - start_time
    records_per_second = total_records / total_time if total_time > 0 else 0

    print("\n📊 RESULTS")
    print("-" * 20)
    print(f"Batch size:     90 days")
    print(f"Total time:     {total_time:.2f}s")
    print(f"Records:        {total_records}")
    print(f"Rec/second:     {records_per_second:.1f}")
    print(f"Success rate:   {len([r for r in collector_results if r.success])}/{len(collector_results)}")

    print("\n🏆 COMPARISON")
    print("-" * 20)
    print("Previous best (60 days): 202.4 rec/s")
    print(f"New result (90 days):    {records_per_second:.1f} rec/s")

    if records_per_second > 202.4:
        print("🎉 NEW WINNER! 90 days is faster!")
        improvement = ((records_per_second - 202.4) / 202.4) * 100
        print(f"   Improvement: +{improvement:.1f}%")
    else:
        print("📊 60 days still optimal")
        difference = ((202.4 - records_per_second) / 202.4) * 100
        print(f"   60 days faster by: {difference:.1f}%")

if __name__ == "__main__":
    asyncio.run(test_eia_90_days())
