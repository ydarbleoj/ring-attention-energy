#!/usr/bin/env python3
"""
API Call Performance Benchmark - Pure latency and throughput measurement.

This benchmark focuses on measuring actual API call performance without
artificial delays, to understand the real capabilities before applying
rate limiting in production.

Measurements:
- Latency: Start of API call → End of data loading (milliseconds)
- Throughput: Records processed per second
- Sequential vs Parallel performance comparison
"""

import asyncio
import time
from datetime import date, datetime
from pathlib import Path
import json
from typing import Dict, Any, List
import statistics
import sys
import os

# Add project root to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig


class APICallBenchmark:
    """
    Pure API call performance benchmark without rate limiting.

    Focus: Understand actual API response times and concurrent processing
    capabilities before applying production rate limits.
    """

    def __init__(self):
        # Small, focused test ranges for precise measurement
        self.test_date_ranges = [
            {
                "name": "Short_7_Days",
                "start": date(2024, 1, 1),
                "end": date(2024, 1, 8),  # 7 days
                "description": "7-day range for latency measurement"
            },
            {
                "name": "Medium_15_Days",
                "start": date(2024, 1, 1),
                "end": date(2024, 1, 16),  # 15 days
                "description": "15-day range for throughput measurement"
            }
        ]

        self.region = "PACW"
        self.data_types = ["demand", "generation"]

        print("⏱️  API CALL PERFORMANCE BENCHMARK")
        print("🎯 Measuring pure API latency and throughput")
        print("📊 No artificial rate limiting - pure performance")

    async def run_benchmark(self):
        """Run comprehensive API call performance benchmark."""

        all_results = []

        for date_range in self.test_date_ranges:
            print(f"\n{'='*70}")
            print(f"📅 Testing: {date_range['name']} - {date_range['description']}")
            print(f"🕐 Date Range: {date_range['start']} to {date_range['end']}")
            print(f"{'='*70}")

            # Test sequential performance
            print(f"\n🔄 SEQUENTIAL TESTS")
            print("-" * 40)

            sequential_results = await self._test_sequential_calls(date_range, runs=5)
            all_results.extend(sequential_results)

            await asyncio.sleep(2)  # Brief pause between test types

            # Test parallel performance
            print(f"\n⚡ PARALLEL TESTS")
            print("-" * 40)

            parallel_results = await self._test_parallel_calls(date_range)
            all_results.extend(parallel_results)

            await asyncio.sleep(3)  # Cool-down between date ranges

        # Comprehensive analysis
        self._analyze_api_performance(all_results)
        return all_results

    async def _test_sequential_calls(self, date_range: Dict[str, Any], runs: int = 5) -> List[Dict[str, Any]]:
        """Test sequential API calls to measure pure latency."""

        results = []

        print(f"🔄 Running {runs} sequential calls...")

        for run in range(1, runs + 1):
            print(f"   Run {run}/{runs}: ", end="", flush=True)

            # Configure for minimal overhead
            config = ExtractBatchConfig(
                days_per_batch=(date_range['end'] - date_range['start']).days,
                max_concurrent_batches=1,  # Sequential
                delay_between_operations=0.01,  # Minimal delay for measurement
                max_operations_per_second=50.0,  # High limit for pure testing
                adaptive_batch_sizing=False
            )

            result = await self._measure_single_extraction(
                date_range['start'],
                date_range['end'],
                config,
                f"{date_range['name']}_Sequential_Run_{run}"
            )

            results.append(result)

            # Display immediate results
            latency_ms = result.get('avg_latency_ms', 0)
            rps = result.get('rps', 0)
            print(f"{latency_ms:.0f}ms latency, {rps:.1f} RPS")

            await asyncio.sleep(0.5)  # Brief pause between runs

        # Calculate sequential statistics
        latencies = [r.get('avg_latency_ms', 0) for r in results if r.get('avg_latency_ms', 0) > 0]
        rpss = [r.get('rps', 0) for r in results if r.get('rps', 0) > 0]

        if latencies and rpss:
            print(f"\n📊 Sequential Summary:")
            print(f"   ⏱️  Latency: {statistics.mean(latencies):.0f}ms avg, {min(latencies):.0f}-{max(latencies):.0f}ms range")
            print(f"   🚀 RPS: {statistics.mean(rpss):.1f} avg, {min(rpss):.1f}-{max(rpss):.1f} range")

        return results

    async def _test_parallel_calls(self, date_range: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Test parallel API calls to measure concurrent performance."""

        results = []

        # Test different levels of concurrency
        concurrency_levels = [2, 3, 4]

        for concurrency in concurrency_levels:
            print(f"\n⚡ Testing {concurrency} concurrent batches:")

            # Run each concurrency level twice for consistency
            for run in range(1, 3):
                print(f"   Run {run}/2 ({concurrency} concurrent): ", end="", flush=True)

                # Calculate batch size for parallel processing
                total_days = (date_range['end'] - date_range['start']).days
                days_per_batch = max(total_days // concurrency, 1)

                config = ExtractBatchConfig(
                    days_per_batch=days_per_batch,
                    max_concurrent_batches=concurrency,
                    delay_between_operations=0.01,  # Minimal delay
                    max_operations_per_second=50.0,  # High limit
                    adaptive_batch_sizing=False
                )

                result = await self._measure_single_extraction(
                    date_range['start'],
                    date_range['end'],
                    config,
                    f"{date_range['name']}_Parallel_{concurrency}_Run_{run}"
                )

                results.append(result)

                # Display results
                latency_ms = result.get('avg_latency_ms', 0)
                rps = result.get('rps', 0)
                api_calls = result.get('total_api_calls', 0)
                print(f"{latency_ms:.0f}ms latency, {rps:.1f} RPS, {api_calls} calls")

                await asyncio.sleep(1)

        return results

    async def _measure_single_extraction(
        self,
        start_date: date,
        end_date: date,
        config: ExtractBatchConfig,
        test_name: str
    ) -> Dict[str, Any]:
        """Measure a single extraction with precise timing."""

        extraction_start_time = time.time()

        try:
            # Create orchestrator
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/api_performance")
            orchestrator.batch_config = config

            # Track initial state
            initial_operations = orchestrator.metrics.total_operations

            # Perform extraction with precise timing
            results = await orchestrator.process_data(
                start_date=start_date,
                end_date=end_date,
                region=self.region,
                data_types=self.data_types
            )

            extraction_end_time = time.time()
            total_duration = extraction_end_time - extraction_start_time

            # Calculate metrics
            total_records = 0
            total_batches = 0
            successful_batches = 0
            final_operations = orchestrator.metrics.total_operations
            total_api_calls = final_operations - initial_operations

            for data_type, batch_results in results.items():
                for batch_result in batch_results:
                    total_batches += 1
                    if batch_result.success:
                        successful_batches += 1
                        total_records += batch_result.records_processed or 0

            # Performance calculations
            rps = total_records / total_duration if total_duration > 0 else 0
            success_rate = successful_batches / total_batches if total_batches > 0 else 0

            # Average latency per API call in milliseconds
            avg_latency_ms = (total_duration / total_api_calls * 1000) if total_api_calls > 0 else 0

            # API calls per second
            api_calls_per_second = total_api_calls / total_duration if total_duration > 0 else 0

            return {
                "test_name": test_name,
                "config": {
                    "days_per_batch": config.days_per_batch,
                    "max_concurrent_batches": config.max_concurrent_batches
                },
                "start_date": str(start_date),
                "end_date": str(end_date),
                "date_range_days": (end_date - start_date).days,
                "total_duration": total_duration,
                "total_records": total_records,
                "total_api_calls": total_api_calls,
                "rps": rps,
                "avg_latency_ms": avg_latency_ms,
                "api_calls_per_second": api_calls_per_second,
                "success_rate": success_rate,
                "records_per_api_call": total_records / total_api_calls if total_api_calls > 0 else 0
            }

        except Exception as e:
            extraction_end_time = time.time()
            total_duration = extraction_end_time - extraction_start_time

            return {
                "test_name": test_name,
                "total_duration": total_duration,
                "total_records": 0,
                "rps": 0,
                "avg_latency_ms": 0,
                "success_rate": 0,
                "error": str(e)
            }

    def _analyze_api_performance(self, results: List[Dict[str, Any]]):
        """Analyze API call performance results."""

        print(f"\n{'='*80}")
        print("⏱️  API CALL PERFORMANCE ANALYSIS")
        print(f"{'='*80}")

        successful_results = [r for r in results if r.get('total_records', 0) > 0]

        if not successful_results:
            print("❌ No successful tests to analyze")
            return

        # Best performance
        best_rps = max(successful_results, key=lambda x: x.get('rps', 0))
        best_latency = min(successful_results, key=lambda x: x.get('avg_latency_ms', float('inf')))

        print(f"\n🏆 BEST RPS PERFORMANCE:")
        print(f"   🚀 {best_rps['rps']:.1f} RPS")
        print(f"   🧪 Test: {best_rps['test_name']}")
        print(f"   ⏱️  Latency: {best_rps.get('avg_latency_ms', 0):.0f}ms")
        print(f"   📊 {best_rps['total_records']:,} records, {best_rps['total_api_calls']} API calls")

        print(f"\n⚡ BEST LATENCY:")
        print(f"   ⏱️  {best_latency.get('avg_latency_ms', 0):.0f}ms")
        print(f"   🧪 Test: {best_latency['test_name']}")
        print(f"   🚀 {best_latency.get('rps', 0):.1f} RPS")

        # Sequential vs Parallel analysis
        sequential_results = [r for r in successful_results if 'Sequential' in r['test_name']]
        parallel_results = [r for r in successful_results if 'Parallel' in r['test_name']]

        print(f"\n📊 SEQUENTIAL vs PARALLEL COMPARISON:")

        if sequential_results:
            seq_avg_rps = statistics.mean([r['rps'] for r in sequential_results])
            seq_avg_latency = statistics.mean([r['avg_latency_ms'] for r in sequential_results])
            print(f"   🔄 Sequential: {seq_avg_rps:.1f} RPS avg, {seq_avg_latency:.0f}ms latency")

        if parallel_results:
            par_avg_rps = statistics.mean([r['rps'] for r in parallel_results])
            par_avg_latency = statistics.mean([r['avg_latency_ms'] for r in parallel_results])
            print(f"   ⚡ Parallel:   {par_avg_rps:.1f} RPS avg, {par_avg_latency:.0f}ms latency")

            if sequential_results:
                improvement = (par_avg_rps / seq_avg_rps - 1) * 100
                print(f"   📈 Parallel improvement: {improvement:+.1f}%")

        # Concurrency analysis
        by_concurrency = {}
        for result in parallel_results:
            concurrency = result['config']['max_concurrent_batches']
            if concurrency not in by_concurrency:
                by_concurrency[concurrency] = []
            by_concurrency[concurrency].append(result)

        if by_concurrency:
            print(f"\n⚡ PARALLEL PERFORMANCE BY CONCURRENCY:")
            for concurrency in sorted(by_concurrency.keys()):
                results_for_concurrency = by_concurrency[concurrency]
                avg_rps = statistics.mean([r['rps'] for r in results_for_concurrency])
                avg_latency = statistics.mean([r['avg_latency_ms'] for r in results_for_concurrency])
                print(f"   {concurrency} concurrent: {avg_rps:7.1f} RPS, {avg_latency:6.0f}ms latency")

        # Performance insights
        print(f"\n💡 PERFORMANCE INSIGHTS:")
        all_latencies = [r['avg_latency_ms'] for r in successful_results]
        all_rps = [r['rps'] for r in successful_results]

        print(f"   ⏱️  Latency range: {min(all_latencies):.0f}-{max(all_latencies):.0f}ms")
        print(f"   🚀 RPS range: {min(all_rps):.1f}-{max(all_rps):.1f}")
        print(f"   📊 Total tests: {len(successful_results)}")

        # Recommendations
        print(f"\n🎯 RECOMMENDATIONS:")
        if best_rps['rps'] >= 3000:
            print(f"   🏆 Excellent! Ready for high-performance workloads")
        elif best_rps['rps'] >= 2000:
            print(f"   ✅ Good performance, suitable for production")
        else:
            print(f"   📈 Baseline established, optimization opportunities exist")


async def main():
    """Run API call performance benchmark."""

    benchmark = APICallBenchmark()
    results = await benchmark.run_benchmark()

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"data/benchmarks/api_call_benchmark_{timestamp}.json"

    Path("data/benchmarks").mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump({
            "timestamp": timestamp,
            "test_type": "api_call_performance",
            "description": "Pure API call latency and throughput measurement",
            "results": results
        }, f, indent=2, default=str)

    print(f"\n💾 API call benchmark results saved to: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
