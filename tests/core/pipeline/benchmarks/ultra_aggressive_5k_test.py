#!/usr/bin/env python3
"""
Ultra Aggressive 5K RPS Test - Push towards ML/AI pipeline targets.

Based on findings:
- Best result: 2714.9 RPS with 45-day batches, 2 parallel
- Insight: Parallel processing + optimal batch sizes = high performance
- Target: 5,000 RPS for ML/AI workloads

Strategy: Test extreme configurations that push API limits responsibly.
"""

import asyncio
import time
from datetime import date
from pathlib import Path
import json
from typing import Dict, Any, List

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig


class UltraAggressive5KTest:
    """Push towards 5,000 RPS with carefully optimized configurations."""

    def __init__(self):
        # Use optimal date ranges based on findings
        self.test_configs = [
            {
                "name": "Optimal_45_Day_Range",
                "start_date": date(2024, 1, 1),
                "end_date": date(2024, 2, 15),  # 45 days - our proven sweet spot
                "description": "45-day proven optimal range"
            },
            {
                "name": "Extended_90_Day_Range",
                "start_date": date(2024, 1, 1),
                "end_date": date(2024, 4, 1),  # 90 days
                "description": "90-day extended range for scale testing"
            }
        ]

        self.region = "PACW"
        self.data_types = ["demand", "generation"]

        print("🔥 ULTRA AGGRESSIVE 5K RPS TEST")
        print("🎯 Target: 5,000 RPS for ML/AI Pipelines")
        print("🚀 Based on optimal 45-day batch findings")

    async def run_5k_tests(self):
        """Run ultra-aggressive configurations targeting 5K RPS."""

        all_results = []

        for test_config in self.test_configs:
            print(f"\n{'='*70}")
            print(f"🔥 Testing: {test_config['name']} - {test_config['description']}")
            print(f"📅 Date Range: {test_config['start_date']} to {test_config['end_date']}")
            print(f"{'='*70}")

            # Ultra-aggressive configurations
            ultra_configs = [
                {
                    "name": f"{test_config['name']}_Ultra_Parallel_Fast",
                    "days_per_batch": 15,  # Small batches for max parallelization
                    "max_concurrent_batches": 4,  # High concurrency
                    "delay_between_operations": 0.08,  # Very fast
                    "max_operations_per_second": 12.0,
                    "adaptive_batch_sizing": False
                },
                {
                    "name": f"{test_config['name']}_Optimal_Enhanced",
                    "days_per_batch": 22,  # Slightly smaller than our 45-day sweet spot
                    "max_concurrent_batches": 3,  # Moderate concurrency
                    "delay_between_operations": 0.05,  # Minimal delay
                    "max_operations_per_second": 15.0,
                    "adaptive_batch_sizing": False
                },
                {
                    "name": f"{test_config['name']}_Balanced_Speed",
                    "days_per_batch": 30,  # Good balance
                    "max_concurrent_batches": 3,
                    "delay_between_operations": 0.03,  # Near-minimal delay
                    "max_operations_per_second": 18.0,
                    "adaptive_batch_sizing": False
                },
                {
                    "name": f"{test_config['name']}_Maximum_Throughput",
                    "days_per_batch": 12,  # Very small for maximum API utilization
                    "max_concurrent_batches": 5,  # Maximum safe concurrency
                    "delay_between_operations": 0.06,  # Balanced for reliability
                    "max_operations_per_second": 16.0,
                    "adaptive_batch_sizing": False
                }
            ]

            for config in ultra_configs:
                print(f"\n🧪 Testing: {config['name']}")
                print(f"   📊 Batch size: {config['days_per_batch']} days")
                print(f"   ⚡ Concurrency: {config['max_concurrent_batches']}")
                print(f"   ⏱️  Delay: {config['delay_between_operations']}s")
                print(f"   🚀 Max ops/sec: {config['max_operations_per_second']}")

                result = await self._run_ultra_test(
                    test_config['start_date'],
                    test_config['end_date'],
                    config
                )

                all_results.append(result)

                print(f"   ✅ {result['rps']:.1f} RPS | "
                      f"{result['total_records']:,} records | "
                      f"{result['duration']:.2f}s | "
                      f"{result['success_rate']:.1%} success")

                # Check if we hit our targets
                if result['rps'] >= 5000:
                    print(f"   🎉 BREAKTHROUGH! 5K+ RPS ACHIEVED!")
                elif result['rps'] >= 4000:
                    print(f"   🚀 EXCELLENT! 4K+ RPS!")
                elif result['rps'] >= 3000:
                    print(f"   ✅ GREAT! 3K+ RPS!")

                await asyncio.sleep(2)

        # Ultimate analysis
        self._analyze_5k_results(all_results)

        return all_results

    async def _run_ultra_test(self, start_date: date, end_date: date, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run a single ultra-aggressive test."""

        test_start_time = time.time()

        try:
            # Create orchestrator
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/ultra_5k")

            # Apply config
            batch_config_dict = {k: v for k, v in config.items() if k != 'name'}
            batch_config = ExtractBatchConfig(**batch_config_dict)
            orchestrator.batch_config = batch_config

            # Run extraction
            results = await orchestrator.process_data(
                start_date=start_date,
                end_date=end_date,
                region=self.region,
                data_types=self.data_types
            )

            test_end_time = time.time()
            duration = test_end_time - test_start_time

            # Calculate comprehensive metrics
            total_records = 0
            total_batches = 0
            successful_batches = 0
            total_api_calls = 0

            for data_type, batch_results in results.items():
                for batch_result in batch_results:
                    total_batches += 1
                    total_api_calls += 1
                    if batch_result.success:
                        successful_batches += 1
                        total_records += batch_result.records_processed or 0

            rps = total_records / duration if duration > 0 else 0
            success_rate = successful_batches / total_batches if total_batches > 0 else 0
            api_calls_per_second = total_api_calls / duration if duration > 0 else 0

            return {
                "name": config['name'],
                "config": config,
                "start_date": str(start_date),
                "end_date": str(end_date),
                "date_range_days": (end_date - start_date).days,
                "duration": duration,
                "total_records": total_records,
                "rps": rps,
                "success_rate": success_rate,
                "total_batches": total_batches,
                "successful_batches": successful_batches,
                "total_api_calls": total_api_calls,
                "api_calls_per_second": api_calls_per_second
            }

        except Exception as e:
            test_end_time = time.time()
            duration = test_end_time - test_start_time
            print(f"   ❌ Failed: {e}")

            return {
                "name": config['name'],
                "config": config,
                "duration": duration,
                "total_records": 0,
                "rps": 0,
                "success_rate": 0,
                "error": str(e)
            }

    def _analyze_5k_results(self, results: List[Dict[str, Any]]):
        """Analyze ultra-aggressive test results."""

        print(f"\n{'='*80}")
        print("🎯 ULTRA-AGGRESSIVE 5K RPS ANALYSIS")
        print(f"{'='*80}")

        # Find best overall
        best = max(results, key=lambda x: x.get('rps', 0))

        print(f"\n🏆 BEST OVERALL PERFORMANCE:")
        print(f"   Test: {best['name']}")
        print(f"   🚀 RPS: {best['rps']:.1f}")
        print(f"   📊 Records: {best['total_records']:,}")
        print(f"   ⏱️  Duration: {best['duration']:.2f}s")
        print(f"   📅 Date Range: {best['date_range_days']} days")
        print(f"   ✅ Success Rate: {best['success_rate']:.1%}")
        print(f"   ⚡ API Calls/sec: {best.get('api_calls_per_second', 0):.2f}")

        # Performance categories
        ultra_high = [r for r in results if r.get('rps', 0) >= 5000]
        very_high = [r for r in results if 4000 <= r.get('rps', 0) < 5000]
        high = [r for r in results if 3000 <= r.get('rps', 0) < 4000]
        good = [r for r in results if 2000 <= r.get('rps', 0) < 3000]

        print(f"\n🎯 PERFORMANCE CATEGORIES:")
        print(f"   🔥 Ultra-High (5000+ RPS): {len(ultra_high)} tests")
        print(f"   🚀 Very High (4000-5000 RPS): {len(very_high)} tests")
        print(f"   ✅ High (3000-4000 RPS): {len(high)} tests")
        print(f"   📊 Good (2000-3000 RPS): {len(good)} tests")

        # Batch size analysis
        by_batch_size = {}
        for result in results:
            batch_size = result['config'].get('days_per_batch', 0)
            if batch_size not in by_batch_size:
                by_batch_size[batch_size] = []
            by_batch_size[batch_size].append(result.get('rps', 0))

        print(f"\n📊 PERFORMANCE BY BATCH SIZE:")
        for batch_size in sorted(by_batch_size.keys()):
            rpss = by_batch_size[batch_size]
            avg_rps = sum(rpss) / len(rpss)
            max_rps = max(rpss)
            print(f"   {batch_size:3d} days: {avg_rps:7.1f} RPS avg, {max_rps:7.1f} RPS max")

        # Target achievement
        if best['rps'] >= 5000:
            print(f"\n🎉 BREAKTHROUGH ACHIEVED!")
            print(f"   🏆 5,000+ RPS target reached: {best['rps']:.1f} RPS")
            print(f"   🤖 Ready for ML/AI pipeline workloads!")
        elif best['rps'] >= 4000:
            print(f"\n🚀 EXCELLENT PROGRESS!")
            print(f"   🎯 Very close to 5K target: {best['rps']:.1f} RPS")
            print(f"   📈 {((5000 - best['rps']) / 5000 * 100):.1f}% remaining to 5K goal")
        else:
            print(f"\n📈 SOLID PROGRESS!")
            print(f"   🎯 Current best: {best['rps']:.1f} RPS")
            print(f"   📊 {(best['rps'] / 5000 * 100):.1f}% of 5K target achieved")


async def main():
    """Run ultra-aggressive 5K RPS tests."""

    test = UltraAggressive5KTest()
    results = await test.run_5k_tests()

    # Save results
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = f"data/benchmarks/ultra_5k_performance_{timestamp}.json"

    Path("data/benchmarks").mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump({
            "timestamp": timestamp,
            "test_type": "ultra_aggressive_5k",
            "target_rps": 5000,
            "description": "Ultra-aggressive test pushing towards 5K RPS for ML/AI pipelines",
            "results": results
        }, f, indent=2, default=str)

    print(f"\n💾 Complete results saved to: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
