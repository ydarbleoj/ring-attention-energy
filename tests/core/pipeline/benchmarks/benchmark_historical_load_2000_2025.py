#!/usr/bin/env python3
"""
Historical Data Load Benchmark: EIA 2000-2025
Comprehensive benchmark of the DataLoadOrchestrator with 25 years of data.
"""

import asyncio
import time
import logging
from datetime import date, datetime
from pathlib import Path
import polars as pl

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('benchmark_historical_2000_2025.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def main():
    """Run comprehensive historical data benchmark."""

    print("🚀 Historical Data Load Benchmark: EIA 2000-2025")
    print("=" * 60)

    # Import components
    import os
    from src.core.pipeline.orchestrator import DataLoadOrchestrator
    from src.core.pipeline.collectors.eia_collector import EIACollector
    from src.core.pipeline.config import EnergyPipelineConfig

    # Get API key from environment
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        print("❌ EIA_API_KEY not found in environment")
        return

    print(f"✅ Found EIA API key: {api_key[:10]}...")

    # Configuration
    config = EnergyPipelineConfig()
    config.api_keys["eia"] = api_key

    start_date = date(2000, 1, 1)
    end_date = date(2025, 7, 8)  # Today
    region = "PACW"  # Pacific West region

    # Calculate scope
    total_days = (end_date - start_date).days
    total_years = (end_date.year - start_date.year)

    print(f"📊 Benchmark Scope:")
    print(f"   • Date Range: {start_date} to {end_date}")
    print(f"   • Total Days: {total_days:,} days")
    print(f"   • Total Years: {total_years} years")
    print(f"   • Region: {region}")
    print(f"   • Expected Records: ~{total_days * 24:,} hourly records")
    print()

    # Initialize orchestrator
    orchestrator = DataLoadOrchestrator(config)

    # Register EIA collector with proper API key
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
    orchestrator.register_collector("eia", eia_collector)

    # Get batch configuration info
    batch_config = orchestrator.get_batch_config("eia")
    chunks = orchestrator.generate_date_chunks(start_date, end_date, "eia")

    print(f"⚙️  Orchestrator Configuration:")
    print(f"   • Batch Size: {batch_config.batch_size_days} days")
    print(f"   • Parallel Chunks: {batch_config.enable_parallel_chunks}")
    print(f"   • Max Concurrent: {batch_config.max_concurrent_requests}")
    print(f"   • Total Chunks: {len(chunks)}")
    print(f"   • Estimated Time: {len(chunks) / 3 / 60:.1f} minutes (at 3 chunks/minute)")
    print()

    # Confirm before proceeding
    response = input("🤔 Proceed with full historical load? (y/N): ")
    if response.lower() != 'y':
        print("❌ Benchmark cancelled")
        return

    print("🏁 Starting historical data load...")
    start_time = time.time()

    try:
        # Load data with parallel processing
        results = await orchestrator.load_historical_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            collector_names=["eia"],
            parallel=True,  # Use parallel processing for maximum performance
            skip_completed=True
        )

        end_time = time.time()
        total_time = end_time - start_time

        # Analyze results
        eia_results = results.get("eia", [])
        successful_results = [r for r in eia_results if r.success]
        failed_results = [r for r in eia_results if not r.success]

        total_records = sum(r.records_collected for r in successful_results)

        # Calculate performance metrics
        records_per_second = total_records / total_time if total_time > 0 else 0

        print("\n" + "=" * 60)
        print("📈 BENCHMARK RESULTS")
        print("=" * 60)
        print(f"⏱️  Total Time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
        print(f"📊 Total Records: {total_records:,}")
        print(f"🚀 Performance: {records_per_second:.2f} records/second")
        print(f"✅ Successful Operations: {len(successful_results)}")
        print(f"❌ Failed Operations: {len(failed_results)}")

        if successful_results:
            print(f"📈 Success Rate: {len(successful_results)/(len(successful_results)+len(failed_results))*100:.1f}%")

        # Performance comparison with previous benchmarks
        baseline_performance = 202.4  # Original baseline
        target_performance = 500.0    # Target
        previous_best = 326.8         # Previous 6-month test

        improvement_vs_baseline = (records_per_second / baseline_performance - 1) * 100
        vs_target = (records_per_second / target_performance - 1) * 100
        vs_previous_best = (records_per_second / previous_best - 1) * 100

        print(f"\n🎯 Performance Analysis:")
        print(f"   • vs Baseline (202.4 rec/s): {improvement_vs_baseline:+.1f}%")
        print(f"   • vs Target (500 rec/s): {vs_target:+.1f}%")
        print(f"   • vs Previous Best (326.8 rec/s): {vs_previous_best:+.1f}%")

        # Storage analysis
        print(f"\n💾 Storage Analysis:")
        try:
            # Check cache files
            cache_path = Path("data/cache/eia")
            processed_path = Path("data/processed/eia")

            if cache_path.exists():
                cache_files = list(cache_path.rglob("*.parquet"))
                cache_size = sum(f.stat().st_size for f in cache_files)
                print(f"   • Cache Files: {len(cache_files)} parquet files")
                print(f"   • Cache Size: {cache_size / (1024**2):.1f} MB")

            if processed_path.exists():
                processed_files = list(processed_path.rglob("*.parquet"))
                processed_size = sum(f.stat().st_size for f in processed_files)
                print(f"   • Processed Files: {len(processed_files)} parquet files")
                print(f"   • Processed Size: {processed_size / (1024**2):.1f} MB")

        except Exception as e:
            print(f"   • Storage check failed: {e}")

        # MLX Integration Readiness
        print(f"\n🧠 MLX Integration Readiness:")
        expected_sequence_length = total_records
        ring_attention_capability = expected_sequence_length > 8760  # More than 1 year

        print(f"   • Sequence Length: {expected_sequence_length:,} timesteps")
        print(f"   • Ring Attention Needed: {'✅ Yes' if ring_attention_capability else '❌ No'}")
        print(f"   • Memory Complexity: O({expected_sequence_length:,}) → O({int(expected_sequence_length**0.5):,}) with Ring Attention")

        # Failure analysis
        if failed_results:
            print(f"\n❌ Failure Analysis:")
            for result in failed_results[:5]:  # Show first 5 failures
                print(f"   • {result.data_type}: {result.errors}")

        # Progress summary
        progress_summary = orchestrator.get_progress_summary()
        print(f"\n📊 Progress Summary:")
        for collector, stats in progress_summary.items():
            print(f"   • {collector}: {stats['completed']} completed, {stats['failed']} failed")

        print("\n🎉 Benchmark completed successfully!")
        print(f"📁 Logs saved to: benchmark_historical_2000_2025.log")

        # Save benchmark results
        benchmark_data = {
            "timestamp": datetime.now().isoformat(),
            "date_range": f"{start_date} to {end_date}",
            "total_time_seconds": total_time,
            "total_records": total_records,
            "records_per_second": records_per_second,
            "successful_operations": len(successful_results),
            "failed_operations": len(failed_results),
            "improvement_vs_baseline_percent": improvement_vs_baseline,
            "vs_target_percent": vs_target,
            "vs_previous_best_percent": vs_previous_best
        }

        # Save as JSON for future reference
        import json
        with open("benchmark_results_2000_2025.json", "w") as f:
            json.dump(benchmark_data, f, indent=2)

        print(f"💾 Benchmark data saved to: benchmark_results_2000_2025.json")

    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        logger.exception("Benchmark failed")
        raise

if __name__ == "__main__":
    asyncio.run(main())
