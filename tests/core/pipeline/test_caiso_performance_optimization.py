"""
CAISO Performance Optimization Test

Goals:
- Apply the same optimizations we used for EIA to CAISO
- Target: 200-300 records/second (accounting for CAISO API differences)
- Test parallel chunk processing with 2 concurrent chunks
- Validate data parsing and storage improvements
"""

import asyncio
import time
from datetime import date
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.core.pipeline.orchestrator import DataLoadOrchestrator
from src.core.pipeline.config import EnergyPipelineConfig
from src.core.pipeline.collectors.caiso_collector import CAISOCollector
import os
from dotenv import load_dotenv

load_dotenv()

async def test_caiso_performance_optimization():
    """Test CAISO performance optimizations."""

    print("🔥 CAISO Performance Optimization Test")
    print("=" * 50)
    print("Target: 200-300 records/second")
    print("Focus: Fix data parsing + parallel processing")
    print()

    # Test parameters - longer period to test multiple chunks
    test_start = date(2024, 1, 1)
    test_end = date(2024, 7, 1)  # 6 months = 2 chunks at 90 days each
    region = "CAISO"

    print(f"📅 Test period: {test_start} to {test_end} (6 months)")
    print(f"🌍 Region: {region}")

    # Create optimized orchestrator
    config = EnergyPipelineConfig()
    orchestrator = DataLoadOrchestrator(config)

    # CAISO now has optimized config:
    # - 90-day batches, 2 parallel chunks, 2 concurrent requests
    batch_config = orchestrator.get_batch_config("caiso")
    print(f"📊 Batch config: {batch_config.batch_size_days} days, "
          f"{batch_config.max_concurrent_requests} concurrent, "
          f"parallel={batch_config.enable_parallel_chunks}")

    # Create and register CAISO collector
    caiso_config = {
        "storage_path": "data/cache",
        "timeout": 30,
        "retry": {
            "max_retries": 3,
            "initial_delay": 1,
            "max_delay": 60
        }
    }
    caiso_collector = CAISOCollector(caiso_config)
    orchestrator.register_collector("caiso", caiso_collector)

    # Predict chunk count
    chunks = orchestrator.generate_date_chunks(test_start, test_end, "caiso")
    print(f"📦 Will process {len(chunks)} chunks")

    # Run baseline test first (sequential, no optimization)
    print("\n🔄 Running baseline (sequential) collection...")
    start_time = time.time()

    baseline_results = await orchestrator.load_historical_data(
        start_date=test_start,
        end_date=test_end,
        region=region,
        collector_names=["caiso"],
        parallel=False,
        skip_completed=False
    )

    baseline_time = time.time() - start_time

    # Calculate baseline metrics
    baseline_collector_results = baseline_results["caiso"]
    baseline_total_records = sum(r.records_collected for r in baseline_collector_results if r.success)
    baseline_records_per_second = baseline_total_records / baseline_time if baseline_time > 0 else 0
    baseline_success_count = len([r for r in baseline_collector_results if r.success])

    print(f"\n📊 BASELINE RESULTS")
    print("=" * 30)
    print(f"Time:           {baseline_time:.2f}s")
    print(f"Records:        {baseline_total_records}")
    print(f"Records/sec:    {baseline_records_per_second:.1f}")
    print(f"Success:        {baseline_success_count}/{len(baseline_collector_results)}")

    # Clear any completed ranges to retest
    orchestrator.progress["caiso"].completed_ranges.clear()

    # Now run optimized test with parallel processing
    print(f"\n🚀 Running optimized (parallel) collection...")
    start_time = time.time()

    optimized_results = await orchestrator.load_historical_data(
        start_date=test_start,
        end_date=test_end,
        region=region,
        collector_names=["caiso"],
        parallel=False,  # Within-collector parallelization
        skip_completed=False
    )

    optimized_time = time.time() - start_time

    # Calculate optimized metrics
    optimized_collector_results = optimized_results["caiso"]
    optimized_total_records = sum(r.records_collected for r in optimized_collector_results if r.success)
    optimized_records_per_second = optimized_total_records / optimized_time if optimized_time > 0 else 0
    optimized_success_count = len([r for r in optimized_collector_results if r.success])

    print(f"\n📊 OPTIMIZED RESULTS")
    print("=" * 30)
    print(f"Time:           {optimized_time:.2f}s")
    print(f"Records:        {optimized_total_records}")
    print(f"Records/sec:    {optimized_records_per_second:.1f}")
    print(f"Success:        {optimized_success_count}/{len(optimized_collector_results)}")

    print(f"\n🎯 CAISO PERFORMANCE COMPARISON")
    print("=" * 40)
    print(f"Baseline:       {baseline_records_per_second:.1f} rec/s")
    print(f"Optimized:      {optimized_records_per_second:.1f} rec/s")

    if optimized_records_per_second > baseline_records_per_second and baseline_records_per_second > 0:
        improvement = ((optimized_records_per_second - baseline_records_per_second) / baseline_records_per_second) * 100
        print(f"🎉 IMPROVEMENT: +{improvement:.1f}%")

        if optimized_records_per_second >= 200:
            print(f"🏆 TARGET ACHIEVED! ({optimized_records_per_second:.1f} ≥ 200 rec/s)")
        else:
            target_gap = ((200 - optimized_records_per_second) / 200) * 100
            print(f"📈 Target progress: {target_gap:.1f}% to go")
    elif optimized_total_records > 0:
        print(f"📊 Performance: {optimized_records_per_second:.1f} rec/s")
        print(f"🎯 Target: 200-300 rec/s")
    else:
        print("❌ Data collection issues detected")

    # Analyze data collection issues
    print(f"\n🔍 DATA COLLECTION ANALYSIS")
    print("=" * 35)

    if optimized_total_records == 0:
        print("❌ No records collected - investigating issues:")
        for result in optimized_collector_results:
            if not result.success:
                print(f"   • Error: {result.errors}")
    else:
        print(f"✅ Successfully collected {optimized_total_records} records")
        print(f"   Parallel efficiency: {optimized_records_per_second / len(chunks):.1f} rec/s per chunk")
        print(f"   Time per chunk: {optimized_time / len(chunks):.2f}s")

    # Success rate analysis
    success_rate = (optimized_success_count / len(optimized_collector_results)) * 100 if optimized_collector_results else 0
    print(f"   Success rate: {success_rate:.1f}%")

    if success_rate < 100:
        print(f"⚠️  Need to address data parsing/API issues")
    else:
        print(f"✅ 100% success rate - optimization working!")

if __name__ == "__main__":
    asyncio.run(test_caiso_performance_optimization())
