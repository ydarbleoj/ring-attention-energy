#!/usr/bin/env python3
"""
Strategic 5K Test - Applying all learnings for maximum performance.

Key Findings Analysis:
1. Best result so far: 3177.6 RPS (90-day sequential)
2. Best parallel: 2714.9 RPS (45-day, 2 parallel batches)
3. Ultra-aggressive failed: Too much concurrency hurt performance
4. Sweet spot: 30-45 day batches with 2-3 parallel streams

Strategy: Find the perfect balance between batch size, parallelism, and timing.
"""

import asyncio
import time
from datetime import date
from pathlib import Path
import json
from typing import Dict, Any, List

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig


class Strategic5KTest:
    """Strategic approach to 5K RPS using all learnings."""

    def __init__(self):
        # Use multiple date ranges to test scalability
        self.test_ranges = [
            {
                "name": "Optimal_90_Day",
                "start": date(2024, 1, 1),
                "end": date(2024, 4, 1),  # 90 days - our proven winner
                "days": 90
            },
            {
                "name": "Extended_120_Day",
                "start": date(2024, 1, 1),
                "end": date(2024, 5, 1),  # 120 days - scale test
                "days": 120
            },
            {
                "name": "Large_180_Day",
                "start": date(2024, 1, 1),
                "end": date(2024, 7, 1),  # 180 days - big dataset
                "days": 180
            }
        ]

        self.region = "PACW"
        self.data_types = ["demand", "generation"]

        print("🎯 STRATEGIC 5K TEST")
        print("🧠 Applying all performance learnings")
        print("🚀 Target: Breakthrough to 5,000 RPS")

    async def run_strategic_tests(self):
        """Run strategic configurations based on all learnings."""

        all_results = []

        for test_range in self.test_ranges:
            print(f"\n{'='*75}")
            print(f"🔥 Testing: {test_range['name']} ({test_range['days']} days)")
            print(f"📅 Date Range: {test_range['start']} to {test_range['end']}")
            print(f"{'='*75}")

            # Strategic configurations based on learnings
            strategic_configs = [
                {
                    "name": f"{test_range['name']}_Single_Large_Batch",
                    "days_per_batch": test_range['days'],  # Single large batch
                    "max_concurrent_batches": 1,
                    "delay_between_operations": 0.1,  # Proven optimal
                    "max_operations_per_second": 10.0,
                    "adaptive_batch_sizing": False,
                    "description": "Single large batch - proven 3177 RPS technique"
                },
                {
                    "name": f"{test_range['name']}_Optimal_Dual_45",
                    "days_per_batch": 45,  # Our proven sweet spot
                    "max_concurrent_batches": 2,  # Proven parallel winner
                    "delay_between_operations": 0.15,  # Conservative for stability
                    "max_operations_per_second": 8.0,
                    "adaptive_batch_sizing": False,
                    "description": "Dual 45-day batches - proven 2714 RPS technique"
                },
                {
                    "name": f"{test_range['name']}_Enhanced_Dual_60",
                    "days_per_batch": 60,  # Larger dual batches
                    "max_concurrent_batches": 2,
                    "delay_between_operations": 0.12,  # Slightly faster
                    "max_operations_per_second": 9.0,
                    "adaptive_batch_sizing": False,
                    "description": "Enhanced dual 60-day batches"
                },
                {
                    "name": f"{test_range['name']}_Triple_30_Optimized",
                    "days_per_batch": 30,  # Balanced triple batches
                    "max_concurrent_batches": 3,
                    "delay_between_operations": 0.18,  # Conservative for 3-way
                    "max_operations_per_second": 7.0,
                    "adaptive_batch_sizing": False,
                    "description": "Triple 30-day optimized batches"
                },
                {
                    "name": f"{test_range['name']}_Quad_Small_Batch",
                    "days_per_batch": test_range['days'] // 4,  # Quarter batches
                    "max_concurrent_batches": 4,
                    "delay_between_operations": 0.2,  # Conservative for 4-way
                    "max_operations_per_second": 6.0,
                    "adaptive_batch_sizing": False,
                    "description": "Quad small batches for maximum parallelism"
                }
            ]

            for config in strategic_configs:
                print(f"\n🧪 {config['description']}")
                print(f"   📊 Batch: {config['days_per_batch']} days × {config['max_concurrent_batches']}")
                print(f"   ⏱️  Delay: {config['delay_between_operations']}s")
                print(f"   🚀 Max ops/sec: {config['max_operations_per_second']}")

                result = await self._run_strategic_test(
                    test_range['start'],
                    test_range['end'],
                    config
                )

                all_results.append(result)

                print(f"   ✅ {result['rps']:.1f} RPS | "
                      f"{result['total_records']:,} records | "
                      f"{result['duration']:.2f}s | "
                      f"{result['success_rate']:.1%}")

                # Real-time performance feedback
                if result['rps'] >= 5000:
                    print(f"   🎉 BREAKTHROUGH! 5K+ RPS ACHIEVED!")
                elif result['rps'] >= 4000:
                    print(f"   🚀 OUTSTANDING! 4K+ RPS!")
                elif result['rps'] >= 3500:
                    print(f"   🔥 EXCELLENT! Very close to our goal!")
                elif result['rps'] >= 3000:
                    print(f"   ✅ GREAT! Strong performance!")

                await asyncio.sleep(2)

        # Final comprehensive analysis
        self._analyze_strategic_results(all_results)
        return all_results

    async def _run_strategic_test(self, start_date: date, end_date: date, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run a strategic test with detailed metrics."""

        test_start_time = time.time()

        try:
            # Create orchestrator
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/strategic_5k")

            # Apply config
            batch_config_dict = {k: v for k, v in config.items() if k not in ['name', 'description']}
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

            # Detailed metrics calculation
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
            records_per_batch = total_records / successful_batches if successful_batches > 0 else 0
            efficiency_score = rps * success_rate  # Combined metric

            return {
                "name": config['name'],
                "description": config['description'],
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
                "records_per_batch": records_per_batch,
                "efficiency_score": efficiency_score
            }

        except Exception as e:
            test_end_time = time.time()
            duration = test_end_time - test_start_time
            print(f"   ❌ Failed: {e}")

            return {
                "name": config['name'],
                "duration": duration,
                "total_records": 0,
                "rps": 0,
                "success_rate": 0,
                "error": str(e)
            }

    def _analyze_strategic_results(self, results: List[Dict[str, Any]]):
        """Comprehensive analysis of strategic test results."""

        print(f"\n{'='*80}")
        print("🎯 STRATEGIC 5K PERFORMANCE ANALYSIS")
        print(f"{'='*80}")

        # Filter successful results
        successful_results = [r for r in results if r.get('rps', 0) > 0]

        if not successful_results:
            print("❌ No successful tests to analyze")
            return

        # Best overall performance
        best_rps = max(successful_results, key=lambda x: x['rps'])
        best_efficiency = max(successful_results, key=lambda x: x.get('efficiency_score', 0))

        print(f"\n🏆 BEST RPS PERFORMANCE:")
        print(f"   🚀 {best_rps['rps']:.1f} RPS - {best_rps['name']}")
        print(f"   📊 {best_rps['total_records']:,} records in {best_rps['duration']:.2f}s")
        print(f"   ✅ {best_rps['success_rate']:.1%} success rate")
        print(f"   📝 {best_rps['description']}")

        print(f"\n⚡ BEST EFFICIENCY (RPS × Success Rate):")
        print(f"   🎯 {best_efficiency.get('efficiency_score', 0):.1f} - {best_efficiency['name']}")

        # Performance categories
        ultra = [r for r in successful_results if r['rps'] >= 5000]
        very_high = [r for r in successful_results if 4000 <= r['rps'] < 5000]
        high = [r for r in successful_results if 3500 <= r['rps'] < 4000]
        good = [r for r in successful_results if 3000 <= r['rps'] < 3500]
        decent = [r for r in successful_results if 2000 <= r['rps'] < 3000]

        print(f"\n🎯 PERFORMANCE BREAKDOWN:")
        print(f"   🔥 Ultra (5000+ RPS): {len(ultra)} tests")
        print(f"   🚀 Very High (4000-5000 RPS): {len(very_high)} tests")
        print(f"   ✨ High (3500-4000 RPS): {len(high)} tests")
        print(f"   ✅ Good (3000-3500 RPS): {len(good)} tests")
        print(f"   📊 Decent (2000-3000 RPS): {len(decent)} tests")

        # Strategy effectiveness
        single_batch = [r for r in successful_results if r['config']['max_concurrent_batches'] == 1]
        dual_batch = [r for r in successful_results if r['config']['max_concurrent_batches'] == 2]
        triple_batch = [r for r in successful_results if r['config']['max_concurrent_batches'] == 3]
        quad_batch = [r for r in successful_results if r['config']['max_concurrent_batches'] == 4]

        print(f"\n📊 STRATEGY EFFECTIVENESS:")
        if single_batch:
            avg_single = sum(r['rps'] for r in single_batch) / len(single_batch)
            max_single = max(r['rps'] for r in single_batch)
            print(f"   Single batch: {avg_single:.1f} avg, {max_single:.1f} max RPS")

        if dual_batch:
            avg_dual = sum(r['rps'] for r in dual_batch) / len(dual_batch)
            max_dual = max(r['rps'] for r in dual_batch)
            print(f"   Dual batch:   {avg_dual:.1f} avg, {max_dual:.1f} max RPS")

        if triple_batch:
            avg_triple = sum(r['rps'] for r in triple_batch) / len(triple_batch)
            max_triple = max(r['rps'] for r in triple_batch)
            print(f"   Triple batch: {avg_triple:.1f} avg, {max_triple:.1f} max RPS")

        if quad_batch:
            avg_quad = sum(r['rps'] for r in quad_batch) / len(quad_batch)
            max_quad = max(r['rps'] for r in quad_batch)
            print(f"   Quad batch:   {avg_quad:.1f} avg, {max_quad:.1f} max RPS")

        # Final assessment
        best_rps_value = best_rps['rps']
        if best_rps_value >= 5000:
            print(f"\n🎉 MISSION ACCOMPLISHED!")
            print(f"   🏆 5,000+ RPS TARGET ACHIEVED!")
            print(f"   🤖 Ready for ML/AI production workloads!")
        elif best_rps_value >= 4500:
            print(f"\n🚀 SO CLOSE!")
            print(f"   🎯 {best_rps_value:.1f} RPS - Just {5000-best_rps_value:.0f} RPS from 5K target!")
            print(f"   📈 {(best_rps_value/5000*100):.1f}% of target achieved")
        elif best_rps_value >= 3500:
            print(f"\n✨ EXCELLENT PROGRESS!")
            print(f"   🎯 {best_rps_value:.1f} RPS - Strong performance achieved")
            print(f"   📊 {(best_rps_value/5000*100):.1f}% of 5K target")
        else:
            print(f"\n📈 SOLID FOUNDATION!")
            print(f"   🎯 {best_rps_value:.1f} RPS baseline established")
            print(f"   🔧 Room for further optimization")


async def main():
    """Run strategic 5K tests."""

    test = Strategic5KTest()
    results = await test.run_strategic_tests()

    # Save comprehensive results
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = f"data/benchmarks/strategic_5k_performance_{timestamp}.json"

    Path("data/benchmarks").mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump({
            "timestamp": timestamp,
            "test_type": "strategic_5k",
            "target_rps": 5000,
            "description": "Strategic test applying all learnings for 5K RPS breakthrough",
            "key_learnings": [
                "Best previous: 3177.6 RPS (90-day sequential)",
                "Best parallel: 2714.9 RPS (45-day dual)",
                "Optimal batch sizes: 30-90 days",
                "Sweet spot parallelism: 1-3 concurrent batches"
            ],
            "results": results
        }, f, indent=2, default=str)

    print(f"\n💾 Complete strategic analysis saved to: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
