"""
Performance optimization test for the orchestrator.

Target: 500 records/second (2.5x improvement from 202.4 rec/s baseline)

Key optimizations:
1. Parallel chunk processing (3 concurrent chunks for EIA)
2. Concurrent demand/generation collection within each chunk
3. Optimized batch configuration
"""

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

async def test_performance_optimization():
    """Test our performance optimizations."""

    print("🚀 Performance Optimization Test")
    print("=" * 50)
    print("Target: 500 records/second (2.5x baseline)")
    print("Baseline: 202.4 records/second")
    print()

    # Get API key
    eia_api_key = os.getenv("EIA_API_KEY")
    if not eia_api_key:
        print("❌ EIA_API_KEY not found")
        return

    # Test parameters - longer test for multiple chunks
    test_start = date(2024, 1, 1)
    test_end = date(2024, 7, 1)  # 6 months = 3 chunks at 60 days each
    region = "PACW"

    print(f"📅 Test period: {test_start} to {test_end} (6 months)")
    print(f"🌍 Region: {region}")

    # Create optimized orchestrator
    config = EnergyPipelineConfig()
    orchestrator = DataLoadOrchestrator(config)

    # The orchestrator now has optimized default configs:
    # - EIA: 3 parallel chunks, 3 concurrent requests
    batch_config = orchestrator.get_batch_config("eia")
    print(f"📊 Batch config: {batch_config.batch_size_days} days, "
          f"{batch_config.max_concurrent_requests} concurrent, "
          f"parallel={batch_config.enable_parallel_chunks}")

    # Create and register EIA collector
    eia_collector = EIACollector(eia_api_key)
    orchestrator.register_collector("eia", eia_collector)

    # Predict chunk count
    chunks = orchestrator.generate_date_chunks(test_start, test_end, "eia")
    print(f"📦 Will process {len(chunks)} chunks")

    # Run the performance test
    print("\n🔄 Running optimized data collection...")
    start_time = time.time()

    results = await orchestrator.load_historical_data(
        start_date=test_start,
        end_date=test_end,
        region=region,
        collector_names=["eia"],
        parallel=False,  # We're testing within-collector parallelization
        skip_completed=False
    )

    end_time = time.time()

    # Calculate metrics
    collector_results = results["eia"]
    total_records = sum(r.records_collected for r in collector_results)
    total_time = end_time - start_time
    records_per_second = total_records / total_time if total_time > 0 else 0

    print("\n📊 PERFORMANCE RESULTS")
    print("=" * 30)
    print(f"Total time:        {total_time:.2f}s")
    print(f"Records collected: {total_records}")
    print(f"Records/second:    {records_per_second:.1f}")
    print(f"Chunks processed:  {len(chunks)}")
    print(f"Success rate:      {len([r for r in collector_results if r.success])}/{len(collector_results)}")

    print("\n🎯 PERFORMANCE COMPARISON")
    print("=" * 30)
    baseline = 202.4
    print(f"Baseline:          {baseline:.1f} rec/s")
    print(f"Optimized:         {records_per_second:.1f} rec/s")

    if records_per_second > baseline:
        improvement = ((records_per_second - baseline) / baseline) * 100
        print(f"🎉 IMPROVEMENT:    +{improvement:.1f}%")

        if records_per_second >= 500:
            print(f"🏆 TARGET ACHIEVED! ({records_per_second:.1f} ≥ 500 rec/s)")
        else:
            target_gap = ((500 - records_per_second) / 500) * 100
            print(f"📈 Target progress: {target_gap:.1f}% to go")
    else:
        regression = ((baseline - records_per_second) / baseline) * 100
        print(f"📉 REGRESSION:     -{regression:.1f}%")

    print(f"\n💡 Performance Analysis:")
    print(f"   Parallel efficiency: {records_per_second / len(chunks):.1f} rec/s per chunk")
    print(f"   Time per chunk:      {total_time / len(chunks):.2f}s")
    print(f"   Theoretical max:     {len(chunks) * baseline / len(chunks):.1f} rec/s (if linear)")

if __name__ == "__main__":
    asyncio.run(test_performance_optimization())
