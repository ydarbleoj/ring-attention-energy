#!/usr/bin/env python3
"""
Ultimate Performance Test - Combining best configurations for maximum RPS.

Based on findings:
- 90-day batches: 3177.6 RPS (best single config)
- Sequential baseline: 2105 RPS with 60-day batches
- Parallel processing showed some limitations with smaller delays

This test combines optimal batch sizes with strategic parallel processing.
"""

import asyncio
import time
from datetime import date
from pathlib import Path
import json
from typing import Dict, Any, List

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig


class UltimatePerformanceTest:
    """Ultimate performance test combining best optimizations."""

    def __init__(self):
        # Test different date ranges to validate batch size impact
        self.test_configs = [
            {
                "name": "90_Day_Optimal",
                "start_date": date(2024, 1, 1),
                "end_date": date(2024, 4, 1),  # 90 days
                "description": "90-day optimal batch size"
            },
            {
                "name": "60_Day_Standard",
                "start_date": date(2024, 1, 1),
                "end_date": date(2024, 3, 1),  # 60 days
                "description": "60-day standard batch size"
            },
            {
                "name": "120_Day_Large",
                "start_date": date(2024, 1, 1),
                "end_date": date(2024, 5, 1),  # 120 days
                "description": "120-day large batch size"
            }
        ]

        self.region = "PACW"
        self.data_types = ["demand", "generation"]

        print("🎯 ULTIMATE PERFORMANCE TEST")
        print("🚀 Combining optimal configurations for maximum RPS")

    async def run_ultimate_tests(self):
        """Run ultimate performance test suite."""

        all_results = []

        for test_config in self.test_configs:
            print(f"\n{'='*60}")
            print(f"🔥 Testing: {test_config['name']} - {test_config['description']}")
            print(f"📅 Date Range: {test_config['start_date']} to {test_config['end_date']}")
            print(f"{'='*60}")

            # Test multiple configurations for this date range
            batch_configs = [
                {
                    "name": f"{test_config['name']}_Sequential_Optimized",
                    "days_per_batch": (test_config['end_date'] - test_config['start_date']).days,
                    "max_concurrent_batches": 1,
                    "delay_between_operations": 0.1,
                    "max_operations_per_second": 10.0,
                    "adaptive_batch_sizing": False
                },
                {
                    "name": f"{test_config['name']}_Parallel_Conservative",
                    "days_per_batch": (test_config['end_date'] - test_config['start_date']).days // 2,
                    "max_concurrent_batches": 2,
                    "delay_between_operations": 0.15,
                    "max_operations_per_second": 8.0,
                    "adaptive_batch_sizing": False
                },
                {
                    "name": f"{test_config['name']}_Parallel_Aggressive",
                    "days_per_batch": (test_config['end_date'] - test_config['start_date']).days // 3,
                    "max_concurrent_batches": 3,
                    "delay_between_operations": 0.1,
                    "max_operations_per_second": 10.0,
                    "adaptive_batch_sizing": False
                }
            ]

            for batch_config in batch_configs:
                print(f"\n🧪 Testing: {batch_config['name']}")
                print(f"   📊 Batch size: {batch_config['days_per_batch']} days")
                print(f"   ⚡ Concurrency: {batch_config['max_concurrent_batches']}")
                print(f"   ⏱️  Delay: {batch_config['delay_between_operations']}s")

                result = await self._run_test(
                    test_config['start_date'],
                    test_config['end_date'],
                    batch_config
                )

                all_results.append(result)

                print(f"   ✅ {result['rps']:.1f} RPS | "
                      f"{result['total_records']:,} records | "
                      f"{result['duration']:.2f}s | "
                      f"{result['success_rate']:.1%} success")

                await asyncio.sleep(2)

        # Analyze results
        self._analyze_ultimate_results(all_results)

        return all_results

    async def _run_test(self, start_date: date, end_date: date, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run a single ultimate performance test."""

        test_start_time = time.time()

        try:
            # Create orchestrator
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/ultimate")

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

            # Calculate metrics
            total_records = 0
            total_batches = 0
            successful_batches = 0
            total_api_calls = 0
            latencies = []

            for data_type, batch_results in results.items():
                for batch_result in batch_results:
                    total_batches += 1
                    total_api_calls += 1

                    if batch_result.success:
                        successful_batches += 1
                        total_records += batch_result.records_processed or 0
                        if hasattr(batch_result, 'operation_latency_seconds'):
                            latencies.append(batch_result.operation_latency_seconds)

            rps = total_records / duration if duration > 0 else 0
            success_rate = successful_batches / total_batches if total_batches > 0 else 0
            avg_latency = sum(latencies) / len(latencies) if latencies else 0

            date_range_days = (end_date - start_date).days

            return {
                "name": config['name'],
                "config": config,
                "start_date": start_date,
                "end_date": end_date,
                "date_range_days": date_range_days,
                "duration": duration,
                "total_records": total_records,
                "rps": rps,
                "success_rate": success_rate,
                "total_batches": total_batches,
                "successful_batches": successful_batches,
                "total_api_calls": total_api_calls,
                "avg_latency": avg_latency
            }

        except Exception as e:
            test_end_time = time.time()
            duration = test_end_time - test_start_time
            print(f"   ❌ Failed: {e}")

            return {
                "name": config['name'],
                "config": config,
                "start_date": start_date,
                "end_date": end_date,
                "duration": duration,
                "total_records": 0,
                "rps": 0,
                "success_rate": 0,
                "error": str(e)
            }

    def _analyze_ultimate_results(self, results: List[Dict[str, Any]]):
        """Analyze ultimate performance results."""

        print(f"\n{'='*80}")
        print("🏆 ULTIMATE PERFORMANCE ANALYSIS")
        print(f"{'='*80}")

        # Filter successful results
        successful_results = [r for r in results if r['rps'] > 0]

        if not successful_results:
            print("❌ No successful results to analyze")
            return

        # Find best overall
        best_result = max(successful_results, key=lambda x: x['rps'])

        print(f"\n🥇 BEST OVERALL PERFORMANCE:")
        print(f"   Test: {best_result['name']}")
        print(f"   RPS: {best_result['rps']:.1f}")
        print(f"   Records: {best_result['total_records']:,}")
        print(f"   Duration: {best_result['duration']:.2f}s")
        print(f"   Date Range: {best_result['date_range_days']} days")
        print(f"   Success Rate: {best_result['success_rate']:.1%}")
        print(f"   Config: {best_result['config']}")

        # Analyze by batch size
        print(f"\n📊 PERFORMANCE BY BATCH SIZE:")
        batch_size_performance = {}
        for result in successful_results:
            batch_size = result['config']['days_per_batch']
            if batch_size not in batch_size_performance:
                batch_size_performance[batch_size] = []
            batch_size_performance[batch_size].append(result['rps'])

        for batch_size in sorted(batch_size_performance.keys()):
            rps_values = batch_size_performance[batch_size]
            avg_rps = sum(rps_values) / len(rps_values)
            max_rps = max(rps_values)
            print(f"   {batch_size:3d} days: {avg_rps:7.1f} RPS avg, {max_rps:7.1f} RPS max")

        # Analyze by concurrency
        print(f"\n⚡ PERFORMANCE BY CONCURRENCY:")
        concurrency_performance = {}
        for result in successful_results:
            concurrency = result['config']['max_concurrent_batches']
            if concurrency not in concurrency_performance:
                concurrency_performance[concurrency] = []
            concurrency_performance[concurrency].append(result['rps'])

        for concurrency in sorted(concurrency_performance.keys()):
            rps_values = concurrency_performance[concurrency]
            avg_rps = sum(rps_values) / len(rps_values)
            max_rps = max(rps_values)
            print(f"   {concurrency} batch{'es' if concurrency > 1 else ''}: {avg_rps:7.1f} RPS avg, {max_rps:7.1f} RPS max")

        # Performance categories
        print(f"\n🎯 PERFORMANCE CATEGORIES:")
        excellent = [r for r in successful_results if r['rps'] >= 3000]
        good = [r for r in successful_results if 2000 <= r['rps'] < 3000]
        baseline = [r for r in successful_results if r['rps'] < 2000]

        print(f"   🚀 Excellent (3000+ RPS): {len(excellent)} tests")
        print(f"   ✅ Good (2000-3000 RPS): {len(good)} tests")
        print(f"   📊 Baseline (<2000 RPS): {len(baseline)} tests")

        if excellent:
            print(f"\n🔥 TOP PERFORMERS (3000+ RPS):")
            for result in sorted(excellent, key=lambda x: x['rps'], reverse=True):
                print(f"   {result['name']}: {result['rps']:.1f} RPS")

        # Recommendations
        print(f"\n💡 OPTIMIZATION INSIGHTS:")

        # Best batch size
        best_batch_size = best_result['config']['days_per_batch']
        print(f"   📅 Optimal batch size: {best_batch_size} days")

        # Concurrency analysis
        seq_results = [r for r in successful_results if r['config']['max_concurrent_batches'] == 1]
        par_results = [r for r in successful_results if r['config']['max_concurrent_batches'] > 1]

        if seq_results and par_results:
            best_seq = max(seq_results, key=lambda x: x['rps'])['rps']
            best_par = max(par_results, key=lambda x: x['rps'])['rps']

            if best_seq > best_par:
                print(f"   ⚡ Sequential processing outperforms parallel ({best_seq:.1f} vs {best_par:.1f} RPS)")
            else:
                print(f"   🚀 Parallel processing wins ({best_par:.1f} vs {best_seq:.1f} RPS)")

        print(f"   🎯 Target achievement: {'✅ EXCEEDED' if best_result['rps'] >= 3000 else '✅ ACHIEVED' if best_result['rps'] >= 2000 else '❌ NOT YET'}")


async def main():
    """Run ultimate performance tests."""

    test = UltimatePerformanceTest()
    results = await test.run_ultimate_tests()

    # Save comprehensive results
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = f"data/benchmarks/ultimate_performance_{timestamp}.json"

    Path("data/benchmarks").mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump({
            "timestamp": timestamp,
            "test_description": "Ultimate performance test combining optimal configurations",
            "target_rps": [2000, 3000],
            "results": results
        }, f, indent=2, default=str)

    print(f"\n💾 Complete results saved to: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
