#!/usr/bin/env python3
"""
Historical Data Load Benchmark: EIA 2000-2025
Comprehensive benchmark using Ultimate Optimized ExtractOrchestrator with 25 years of data.

Performance Target: 3,300+ RPS with ultimate optimizations
Expected Time: ~1-2 hours for complete 25-year historical load
"""

import asyncio
import time
import logging
from datetime import date, datetime
from pathlib import Path
import sys
import os

# Add project root to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig

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
    """Run comprehensive historical data benchmark with ultimate optimizations."""

    print("� Historical Data Load Benchmark: EIA 2000-2025")
    print("⚡ Using Ultimate Optimized ExtractOrchestrator")
    print("🎯 Target Performance: 3,300+ RPS")
    print("=" * 70)

    # Configuration for ultimate performance
    start_date = date(2000, 1, 1)
    end_date = date(2025, 7, 10)  # Today
    region = "PACW"  # Pacific West region

    # Calculate scope
    total_days = (end_date - start_date).days
    total_years = (end_date.year - start_date.year)
    expected_records = total_days * 24 * 2  # 24 hours * 2 data types (demand + generation)

    print(f"📊 Benchmark Scope:")
    print(f"   • Date Range: {start_date} to {end_date}")
    print(f"   • Total Days: {total_days:,} days")
    print(f"   • Total Years: {total_years} years")
    print(f"   • Region: {region}")
    print(f"   • Expected Records: ~{expected_records:,} records")
    print(f"   • Data Types: demand + generation")
    print()

    # Initialize orchestrator with ultimate optimizations
    orchestrator = ExtractOrchestrator(raw_data_path="data/raw/eia")

    # Configure for ultimate performance (our best proven configuration)
    ultimate_config = ExtractBatchConfig(
        days_per_batch=45,                # Optimal batch size from benchmarks
        max_concurrent_batches=2,         # Proven stable concurrency
        delay_between_operations=0.8,     # Safe aggressive rate limiting
        max_operations_per_second=18.0,   # Conservative rate limiting
        adaptive_batch_sizing=False       # Consistent performance
    )

    orchestrator.batch_config = ultimate_config

    # Calculate estimated performance
    estimated_batches = (total_days / 45) * 2  # 45-day batches * 2 data types
    estimated_time_minutes = (estimated_batches * 0.8) / 60  # 0.8s per batch

    print(f"⚙️  Ultimate Orchestrator Configuration:")
    print(f"   • Batch Size: {ultimate_config.days_per_batch} days")
    print(f"   • Concurrency: {ultimate_config.max_concurrent_batches} batches")
    print(f"   • Rate Limiting: {ultimate_config.delay_between_operations}s delays")
    print(f"   • Expected Batches: {estimated_batches:.0f}")
    print(f"   • Estimated Time: {estimated_time_minutes:.1f} minutes")
    print(f"   • Storage Path: data/raw/eia/")
    print()

    # Confirm before proceeding
    response = input("🤔 Proceed with full 25-year historical load? (y/N): ")
    if response.lower() != 'y':
        print("❌ Benchmark cancelled")
        return

    print("🔥 Starting ultimate performance historical data load...")
    start_time = time.time()

    try:
        # Load data with ultimate optimized configuration
        results = await orchestrator.process_data(
            start_date=start_date,
            end_date=end_date,
            region=region,
            data_types=["demand", "generation"]  # Both data types for comprehensive dataset
        )

        end_time = time.time()
        total_time = end_time - start_time

        # Analyze results
        total_records = 0
        successful_operations = 0
        failed_operations = 0

        for data_type, batch_results in results.items():
            for batch_result in batch_results:
                if batch_result.success:
                    successful_operations += 1
                    total_records += batch_result.records_processed or 0
                else:
                    failed_operations += 1

        # Calculate performance metrics
        records_per_second = total_records / total_time if total_time > 0 else 0
        final_operations = orchestrator.metrics.total_operations
        api_calls_per_sec = final_operations / total_time if total_time > 0 else 0

        print("\n" + "=" * 70)
        print("� ULTIMATE BENCHMARK RESULTS")
        print("=" * 70)
        print(f"⏱️  Total Time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
        print(f"📊 Total Records: {total_records:,}")
        print(f"🚀 Performance: {records_per_second:.1f} RPS")
        print(f"🌐 API Rate: {api_calls_per_sec:.2f} requests/sec")
        print(f"✅ Successful Operations: {successful_operations}")
        print(f"❌ Failed Operations: {failed_operations}")

        if successful_operations > 0:
            success_rate = (successful_operations / (successful_operations + failed_operations)) * 100
            print(f"📈 Success Rate: {success_rate:.1f}%")

        # Performance comparison with our benchmark evolution
        baseline_performance = 1000      # Original optimized baseline
        previous_best = 3300            # Ultimate optimization peak
        target_performance = 5000       # 5K RPS target

        improvement_vs_baseline = (records_per_second / baseline_performance - 1) * 100
        vs_previous_best = (records_per_second / previous_best - 1) * 100
        vs_target = (records_per_second / target_performance - 1) * 100

        print(f"\n🎯 Performance Analysis:")
        print(f"   • vs Baseline (1,000 RPS): {improvement_vs_baseline:+.1f}%")
        print(f"   • vs Previous Best (3,300 RPS): {vs_previous_best:+.1f}%")
        print(f"   • vs 5K Target (5,000 RPS): {vs_target:+.1f}%")

        # Rate limiting compliance
        eia_limit = 1.389  # 5000/hour theoretical maximum
        utilization = (api_calls_per_sec / eia_limit) * 100 if eia_limit > 0 else 0
        safety_margin = ((eia_limit - api_calls_per_sec) / eia_limit) * 100 if eia_limit > 0 else 0

        print(f"\n⚡ Rate Limiting Analysis:")
        print(f"   • EIA Limit: {eia_limit:.2f} requests/sec")
        print(f"   • Our Rate: {api_calls_per_sec:.2f} requests/sec")
        print(f"   • Utilization: {utilization:.1f}%")
        print(f"   • Safety Margin: {safety_margin:.1f}%")

        # Storage analysis
        print(f"\n💾 Storage Analysis:")
        try:
            raw_path = Path("data/raw/eia")
            if raw_path.exists():
                raw_files = list(raw_path.rglob("*.json"))
                if raw_files:
                    total_size = sum(f.stat().st_size for f in raw_files)
                    print(f"   • Raw Files: {len(raw_files)} JSON files")
                    print(f"   • Total Size: {total_size / (1024**2):.1f} MB")
                    print(f"   • Avg File Size: {(total_size / len(raw_files)) / 1024:.1f} KB")
                else:
                    print(f"   • No raw files found yet")
            else:
                print(f"   • Raw data directory not created yet")

        except Exception as e:
            print(f"   • Storage check failed: {e}")

        # MLX Integration Readiness
        print(f"\n🧠 MLX Integration Readiness:")
        sequence_length = total_records
        ring_attention_needed = sequence_length > 8760  # More than 1 year of hourly data

        print(f"   • Sequence Length: {sequence_length:,} timesteps")
        print(f"   • Ring Attention Needed: {'✅ Yes' if ring_attention_needed else '❌ No'}")
        print(f"   • Memory Complexity: O({sequence_length:,}) → O({int(sequence_length**0.5):,}) with Ring Attention")
        print(f"   • Data Ready for Training: {'✅ Yes' if sequence_length > 1000 else '❌ No'}")

        # Data completeness analysis
        expected_records_per_year = 365 * 24 * 2  # 365 days * 24 hours * 2 data types
        expected_total = total_years * expected_records_per_year
        completeness = (total_records / expected_total) * 100 if expected_total > 0 else 0

        print(f"\n📊 Data Completeness Analysis:")
        print(f"   • Expected Records: {expected_total:,}")
        print(f"   • Actual Records: {total_records:,}")
        print(f"   • Completeness: {completeness:.1f}%")
        print(f"   • Missing Records: {expected_total - total_records:,}")

        print("\n🎉 Ultimate historical benchmark completed!")
        print(f"📁 Logs saved to: benchmark_historical_2000_2025.log")
        print(f"💾 Raw data saved to: data/raw/eia/")

        # Save comprehensive benchmark results
        benchmark_data = {
            "benchmark_type": "ultimate_historical_load",
            "timestamp": datetime.now().isoformat(),
            "date_range": f"{start_date} to {end_date}",
            "total_time_seconds": total_time,
            "total_records": total_records,
            "records_per_second": records_per_second,
            "api_calls_per_second": api_calls_per_sec,
            "successful_operations": successful_operations,
            "failed_operations": failed_operations,
            "success_rate_percent": success_rate if successful_operations > 0 else 0,
            "rate_limit_utilization_percent": utilization,
            "safety_margin_percent": safety_margin,
            "data_completeness_percent": completeness,
            "configuration": {
                "days_per_batch": ultimate_config.days_per_batch,
                "max_concurrent_batches": ultimate_config.max_concurrent_batches,
                "delay_between_operations": ultimate_config.delay_between_operations,
                "max_operations_per_second": ultimate_config.max_operations_per_second
            },
            "performance_vs_benchmarks": {
                "vs_baseline_percent": improvement_vs_baseline,
                "vs_previous_best_percent": vs_previous_best,
                "vs_5k_target_percent": vs_target
            }
        }

        # Save results
        import json
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"ultimate_historical_benchmark_{timestamp}.json"

        with open(results_file, "w") as f:
            json.dump(benchmark_data, f, indent=2)

        print(f"💾 Benchmark results saved to: {results_file}")

    except Exception as e:
        print(f"\n❌ Ultimate benchmark failed: {e}")
        logger.exception("Ultimate benchmark failed")
        raise

if __name__ == "__main__":
    asyncio.run(main())
