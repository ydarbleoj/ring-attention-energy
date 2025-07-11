#!/usr/bin/env python3
"""
Optimized Performance Benchmark - Speed up our 45-day batch baseline.

Current Baseline: 1,853 RPS with 45-day batches (2 API calls per batch)
Goal: Optimize to reach closer to 5,000 RPS

Optimization Strategies:
1. Single data type (eliminate 2x API call overhead)
2. Increased request size limits
3. Network optimizations
4. Processing optimizations
"""

import asyncio
import time
from datetime import date, datetime, timedelta
from pathlib import Path
import json
from typing import Dict, Any, List
import statistics
import sys
import os

# Add project root to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig


class OptimizedBenchmark:
    """
    Benchmark optimizations to speed up our 45-day batch performance.

    Focus: Apply specific optimizations to push beyond 1,853 RPS baseline.
    """

    def __init__(self):
        # Stick with proven 45-day batches
        self.optimal_days = 45

        # Test focused date ranges
        self.test_ranges = [
            {
                "name": "Baseline_45_Days",
                "start": date(2024, 1, 1),
                "end": date(2024, 2, 15),  # 45 days
                "description": "Baseline 45-day performance"
            },
            {
                "name": "Extended_90_Days",
                "start": date(2024, 1, 1),
                "end": date(2024, 4, 1),   # 90 days for dual 45-day batches
                "description": "Extended range for optimization testing"
            }
        ]

        self.region = "PACW"

        print("🚀 OPTIMIZED PERFORMANCE BENCHMARK")
        print("🎯 Goal: Speed up 45-day batch beyond 1,853 RPS baseline")
        print("⚡ Testing specific optimizations")

    async def run_optimization_tests(self):
        """Run optimization tests to speed up performance."""

        all_results = []

        print(f"\n{'='*70}")
        print("🔍 OPTIMIZATION TESTING")
        print(f"{'='*70}")

        # Test 1: Single Data Type (eliminate 2x API call overhead)
        print(f"\n🧪 OPTIMIZATION 1: Single Data Type")
        print("   Strategy: Eliminate 2x API call overhead")
        print("-" * 50)

        single_type_results = await self._test_single_data_type()
        all_results.extend(single_type_results)

        await asyncio.sleep(2)

        # Test 2: Optimized Request Parameters
        print(f"\n🧪 OPTIMIZATION 2: Request Parameter Tuning")
        print("   Strategy: Optimize API request parameters")
        print("-" * 50)

        param_results = await self._test_optimized_parameters()
        all_results.extend(param_results)

        await asyncio.sleep(2)

        # Test 3: Processing Optimizations
        print(f"\n🧪 OPTIMIZATION 3: Minimal Processing Overhead")
        print("   Strategy: Reduce data processing time")
        print("-" * 50)

        processing_results = await self._test_processing_optimizations()
        all_results.extend(processing_results)

        # Analysis
        self._analyze_optimizations(all_results)
        return all_results

    async def _test_single_data_type(self) -> List[Dict[str, Any]]:
        """Test single data type extraction to eliminate 2x API call overhead."""

        results = []

        # Test both demand and generation separately
        for data_type in ["demand"]:  # Start with demand only
            print(f"\n   Testing {data_type} only:")

            for test_range in self.test_ranges:
                test_name = f"{test_range['name']}_Single_{data_type.title()}"

                config = ExtractBatchConfig(
                    days_per_batch=self.optimal_days,
                    max_concurrent_batches=1,
                    delay_between_operations=0.01,  # Minimal delay for optimization
                    max_operations_per_second=50.0,
                    adaptive_batch_sizing=False
                )

                print(f"     {test_range['name']}: ", end="", flush=True)

                result = await self._measure_extraction(
                    test_range['start'],
                    test_range['end'],
                    config,
                    test_name,
                    data_types=[data_type]  # Single data type
                )

                results.append(result)

                rps = result.get('rps', 0)
                latency_ms = result.get('avg_latency_ms', 0)
                improvement = ((rps / 1853.9) - 1) * 100 if rps > 0 else 0

                print(f"{rps:.1f} RPS ({improvement:+.1f}% vs baseline), {latency_ms:.0f}ms")

                await asyncio.sleep(1)

        return results

    async def _test_optimized_parameters(self) -> List[Dict[str, Any]]:
        """Test optimized API request parameters."""

        results = []

        # Different delay strategies
        delay_strategies = [
            {"name": "Ultra_Fast", "delay": 0.005, "ops_per_sec": 100.0},
            {"name": "Minimal_Delay", "delay": 0.01, "ops_per_sec": 80.0},
            {"name": "Conservative", "delay": 0.05, "ops_per_sec": 20.0}
        ]

        for strategy in delay_strategies:
            print(f"\n   Testing {strategy['name']} timing:")

            config = ExtractBatchConfig(
                days_per_batch=self.optimal_days,
                max_concurrent_batches=1,
                delay_between_operations=strategy['delay'],
                max_operations_per_second=strategy['ops_per_sec'],
                adaptive_batch_sizing=False
            )

            test_name = f"Param_Opt_{strategy['name']}_45_Days"

            print(f"     45-day batch: ", end="", flush=True)

            result = await self._measure_extraction(
                self.test_ranges[0]['start'],
                self.test_ranges[0]['end'],
                config,
                test_name,
                data_types=["demand"]  # Single type for speed
            )

            results.append(result)

            rps = result.get('rps', 0)
            latency_ms = result.get('avg_latency_ms', 0)
            improvement = ((rps / 1853.9) - 1) * 100 if rps > 0 else 0

            print(f"{rps:.1f} RPS ({improvement:+.1f}% vs baseline), {latency_ms:.0f}ms")

            await asyncio.sleep(1)

        return results

    async def _test_processing_optimizations(self) -> List[Dict[str, Any]]:
        """Test minimal processing optimizations."""

        results = []

        # Test larger batch sizes to maximize data per API call
        batch_sizes = [60, 75, 90]  # Larger than our 45-day optimum

        for batch_size in batch_sizes:
            print(f"\n   Testing {batch_size}-day batches:")

            config = ExtractBatchConfig(
                days_per_batch=batch_size,
                max_concurrent_batches=1,
                delay_between_operations=0.01,
                max_operations_per_second=50.0,
                adaptive_batch_sizing=False
            )

            test_name = f"Large_Batch_{batch_size}_Days"

            print(f"     {batch_size} days: ", end="", flush=True)

            result = await self._measure_extraction(
                date(2024, 1, 1),
                date(2024, 1, 1) + timedelta(days=batch_size),
                config,
                test_name,
                data_types=["demand"]
            )

            results.append(result)

            rps = result.get('rps', 0)
            latency_ms = result.get('avg_latency_ms', 0)
            records_per_call = result.get('records_per_api_call', 0)
            improvement = ((rps / 1853.9) - 1) * 100 if rps > 0 else 0

            print(f"{rps:.1f} RPS ({improvement:+.1f}%), {records_per_call:.0f} rec/call")

            await asyncio.sleep(1)

        return results

    async def _measure_extraction(
        self,
        start_date: date,
        end_date: date,
        config: ExtractBatchConfig,
        test_name: str,
        data_types: List[str] = ["demand"]
    ) -> Dict[str, Any]:
        """Measure extraction with precise timing."""

        extraction_start = time.time()

        try:
            # Create orchestrator
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/optimized")
            orchestrator.batch_config = config

            # Track operations
            initial_ops = orchestrator.metrics.total_operations

            # Run extraction
            results = await orchestrator.process_data(
                start_date=start_date,
                end_date=end_date,
                region=self.region,
                data_types=data_types
            )

            extraction_end = time.time()
            total_duration = extraction_end - extraction_start

            # Calculate metrics
            total_records = 0
            total_batches = 0
            successful_batches = 0
            final_ops = orchestrator.metrics.total_operations
            total_api_calls = final_ops - initial_ops

            for data_type, batch_results in results.items():
                for batch_result in batch_results:
                    total_batches += 1
                    if batch_result.success:
                        successful_batches += 1
                        total_records += batch_result.records_processed or 0

            # Performance metrics
            rps = total_records / total_duration if total_duration > 0 else 0
            avg_latency_ms = (total_duration / total_api_calls * 1000) if total_api_calls > 0 else 0
            records_per_api_call = total_records / total_api_calls if total_api_calls > 0 else 0
            success_rate = successful_batches / total_batches if total_batches > 0 else 0

            return {
                "test_name": test_name,
                "config": {
                    "days_per_batch": config.days_per_batch,
                    "delay_between_operations": config.delay_between_operations,
                    "max_operations_per_second": config.max_operations_per_second
                },
                "data_types": data_types,
                "total_duration": total_duration,
                "total_records": total_records,
                "total_api_calls": total_api_calls,
                "rps": rps,
                "avg_latency_ms": avg_latency_ms,
                "records_per_api_call": records_per_api_call,
                "success_rate": success_rate,
                "date_range_days": (end_date - start_date).days
            }

        except Exception as e:
            extraction_end = time.time()
            total_duration = extraction_end - extraction_start

            return {
                "test_name": test_name,
                "total_duration": total_duration,
                "total_records": 0,
                "rps": 0,
                "avg_latency_ms": 0,
                "success_rate": 0,
                "error": str(e)
            }

    def _analyze_optimizations(self, results: List[Dict[str, Any]]):
        """Analyze optimization results."""

        print(f"\n{'='*80}")
        print("🚀 OPTIMIZATION ANALYSIS")
        print(f"{'='*80}")

        successful_results = [r for r in results if r.get('total_records', 0) > 0]

        if not successful_results:
            print("❌ No successful optimizations to analyze")
            return

        # Best performance
        best_result = max(successful_results, key=lambda x: x.get('rps', 0))
        baseline_rps = 1853.9

        print(f"\n🏆 BEST OPTIMIZATION RESULT:")
        print(f"   🚀 {best_result['rps']:.1f} RPS")
        print(f"   🧪 Test: {best_result['test_name']}")
        print(f"   ⏱️  Latency: {best_result.get('avg_latency_ms', 0):.0f}ms")
        print(f"   📊 {best_result['total_records']:,} records, {best_result['total_api_calls']} API calls")
        print(f"   📈 Records per call: {best_result.get('records_per_api_call', 0):.0f}")

        # Improvement analysis
        improvement = ((best_result['rps'] / baseline_rps) - 1) * 100
        print(f"\n📈 IMPROVEMENT vs BASELINE:")
        print(f"   📊 Baseline: {baseline_rps:.1f} RPS")
        print(f"   🚀 Optimized: {best_result['rps']:.1f} RPS")
        print(f"   📈 Improvement: {improvement:+.1f}%")

        if improvement >= 50:
            print(f"   🎉 EXCELLENT! Major performance boost!")
        elif improvement >= 20:
            print(f"   ✅ GREAT! Significant improvement!")
        elif improvement >= 5:
            print(f"   👍 GOOD! Noticeable improvement!")
        else:
            print(f"   📊 Baseline maintained")

        # Optimization strategy analysis
        single_type_results = [r for r in successful_results if 'Single' in r['test_name']]
        param_opt_results = [r for r in successful_results if 'Param_Opt' in r['test_name']]
        large_batch_results = [r for r in successful_results if 'Large_Batch' in r['test_name']]

        print(f"\n🧪 OPTIMIZATION STRATEGY RESULTS:")

        if single_type_results:
            best_single = max(single_type_results, key=lambda x: x['rps'])
            single_improvement = ((best_single['rps'] / baseline_rps) - 1) * 100
            print(f"   📊 Single Data Type: {best_single['rps']:.1f} RPS ({single_improvement:+.1f}%)")

        if param_opt_results:
            best_param = max(param_opt_results, key=lambda x: x['rps'])
            param_improvement = ((best_param['rps'] / baseline_rps) - 1) * 100
            print(f"   ⚡ Parameter Tuning: {best_param['rps']:.1f} RPS ({param_improvement:+.1f}%)")

        if large_batch_results:
            best_large = max(large_batch_results, key=lambda x: x['rps'])
            large_improvement = ((best_large['rps'] / baseline_rps) - 1) * 100
            print(f"   📏 Larger Batches: {best_large['rps']:.1f} RPS ({large_improvement:+.1f}%)")

        # Goal progress
        goal_rps = 5000
        progress_to_goal = (best_result['rps'] / goal_rps) * 100

        print(f"\n🎯 PROGRESS TOWARD 5,000 RPS GOAL:")
        print(f"   🎯 Current: {best_result['rps']:.1f} RPS")
        print(f"   🏁 Goal: {goal_rps} RPS")
        print(f"   📊 Progress: {progress_to_goal:.1f}%")
        print(f"   📈 Still need: {((goal_rps / best_result['rps']) - 1) * 100:.0f}% more improvement")

        # Next steps
        print(f"\n💡 NEXT OPTIMIZATION OPPORTUNITIES:")
        if best_result.get('total_api_calls', 0) > 0:
            print(f"   🔄 Current API efficiency: {best_result.get('records_per_api_call', 0):.0f} records/call")
            print(f"   🎯 Target efficiency: {goal_rps / (best_result['rps'] / best_result.get('records_per_api_call', 1)):.0f} records/call needed")

        print(f"   🚀 Consider: Larger request limits, caching, different endpoints")


async def main():
    """Run optimization benchmark."""

    benchmark = OptimizedBenchmark()
    results = await benchmark.run_optimization_tests()

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"data/benchmarks/optimized_benchmark_{timestamp}.json"

    Path("data/benchmarks").mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump({
            "timestamp": timestamp,
            "test_type": "optimization_benchmark",
            "baseline_rps": 1853.9,
            "goal_rps": 5000,
            "description": "Performance optimizations for 45-day batch processing",
            "results": results
        }, f, indent=2, default=str)

    print(f"\n💾 Optimization results saved to: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
