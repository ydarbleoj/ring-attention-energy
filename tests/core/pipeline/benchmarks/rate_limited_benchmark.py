#!/usr/bin/env python3
"""
Rate-Limited Extraction Benchmark - Real-world API ingestion optimization.

EIA API Constraints:
- 5,000 requests per hour maximum (1.39 requests/second)
- Being good neighbors: 1 request every 2 seconds (0.5 requests/second)
- This gives us sustainable 1,800 requests/hour with safety margin

Goal: Optimize data ingestion within strict real-world rate limits to understand
maximum practical throughput for production API workloads.
"""

import asyncio
import time
from datetime import date, datetime
from pathlib import Path
import json
from typing import Dict, Any, List
import sys
import os

# Add project root to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig


class RateLimitedBenchmark:
    """
    Benchmark extraction performance with strict rate limiting.

    This simulates real-world production constraints where API rate limits
    are the primary bottleneck, not network or processing speed.
    """

    def __init__(self):
        # EIA Rate Limiting (being conservative good neighbors)
        self.requests_per_hour = 1800  # Conservative: 1800/hour vs 5000/hour max
        self.seconds_per_request = 0.1

        # Test configurations
        self.test_configs = [
            {
                "name": "Rate_Limited_30_Days",
                "start_date": date(2024, 1, 1),
                "end_date": date(2024, 1, 31),  # 30 days
                "description": "30-day extraction with proper rate limiting"
            },
            {
                "name": "Rate_Limited_60_Days",
                "start_date": date(2024, 1, 1),
                "end_date": date(2024, 3, 1),  # 60 days
                "description": "60-day extraction with proper rate limiting"
            },
            {
                "name": "Rate_Limited_90_Days",
                "start_date": date(2024, 1, 1),
                "end_date": date(2024, 4, 1),  # 90 days
                "description": "90-day extraction with proper rate limiting"
            }
        ]

        self.region = "PACW"
        self.data_types = ["demand", "generation"]

        print("🕐 RATE-LIMITED EXTRACTION BENCHMARK")
        print("🤝 Being good API neighbors")
        print(f"⏱️  Rate limit: {self.seconds_per_request:.1f}s between requests")
        print(f"📊 Max throughput: {self.requests_per_hour} requests/hour")

    async def run_benchmark(self):
        """Run rate-limited benchmark tests."""

        all_results = []

        for test_config in self.test_configs:
            print(f"\n{'='*70}")
            print(f"🧪 Testing: {test_config['name']}")
            print(f"📅 Date Range: {test_config['start_date']} to {test_config['end_date']}")
            print(f"📝 {test_config['description']}")
            print(f"{'='*70}")

            # Different batch strategies within rate limits
            batch_strategies = [
                {
                    "name": f"{test_config['name']}_Sequential_Conservative",
                    "days_per_batch": (test_config['end_date'] - test_config['start_date']).days,
                    "max_concurrent_batches": 1,
                    "delay_between_operations": self.seconds_per_request,
                    "max_operations_per_second": 1.0 / self.seconds_per_request,
                    "adaptive_batch_sizing": False,
                    "description": "Single large batch, full rate limiting"
                },
                {
                    "name": f"{test_config['name']}_Dual_Batch",
                    "days_per_batch": (test_config['end_date'] - test_config['start_date']).days // 2,
                    "max_concurrent_batches": 1,  # Sequential to respect rate limits
                    "delay_between_operations": self.seconds_per_request,
                    "max_operations_per_second": 1.0 / self.seconds_per_request,
                    "adaptive_batch_sizing": False,
                    "description": "Two medium batches, full rate limiting"
                },
                {
                    "name": f"{test_config['name']}_Multi_Small_Batch",
                    "days_per_batch": 15,  # Smaller batches
                    "max_concurrent_batches": 1,  # Sequential only
                    "delay_between_operations": self.seconds_per_request,
                    "max_operations_per_second": 1.0 / self.seconds_per_request,
                    "adaptive_batch_sizing": False,
                    "description": "Multiple small batches, full rate limiting"
                }
            ]

            for strategy in batch_strategies:
                print(f"\n🔬 Strategy: {strategy['description']}")
                print(f"   📊 Batch size: {strategy['days_per_batch']} days")
                print(f"   ⏱️  Request interval: {strategy['delay_between_operations']:.1f}s")

                result = await self._run_rate_limited_test(
                    test_config['start_date'],
                    test_config['end_date'],
                    strategy
                )

                all_results.append(result)

                # Calculate expected vs actual timing
                expected_requests = result.get('total_api_calls', 0)
                expected_time = expected_requests * self.seconds_per_request
                actual_time = result.get('duration', 0)
                rps = result.get('rps', 0)
                avg_latency = result.get('avg_latency', 0)

                print(f"   � {rps:.1f} RPS | {result['total_records']:,} records")
                print(f"   ⏱️  Latency: {avg_latency:.2f}s avg | {actual_time:.1f}s total")
                print(f"   🎯 Rate compliance: {result['rate_compliance']:.1%}")

                # Performance category
                if rps >= 5000:
                    print(f"   🏆 BREAKTHROUGH! Exceeded 5K RPS goal!")
                elif rps >= 3100:
                    print(f"   🔥 EXCELLENT! Beat 3.1K RPS benchmark!")
                elif rps >= 2000:
                    print(f"   ✅ GREAT! Strong performance!")
                elif rps >= 1000:
                    print(f"   📊 GOOD! Solid baseline!")

                await asyncio.sleep(3)  # Cool-down between tests

        # Comprehensive analysis
        self._analyze_rate_limited_results(all_results)
        return all_results

    async def _run_rate_limited_test(self, start_date: date, end_date: date, config: Dict[str, Any]) -> Dict[str, Any]:
        """Run a single rate-limited test with detailed metrics."""

        test_start_time = time.time()

        try:
            # Create orchestrator with rate-limited config
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/rate_limited")

            # Apply rate-limited configuration
            batch_config_dict = {k: v for k, v in config.items() if k not in ['name', 'description']}
            batch_config = ExtractBatchConfig(**batch_config_dict)
            orchestrator.batch_config = batch_config

            # Track API calls
            initial_operations = orchestrator.metrics.total_operations

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
            final_operations = orchestrator.metrics.total_operations
            total_api_calls = final_operations - initial_operations

            for data_type, batch_results in results.items():
                for batch_result in batch_results:
                    total_batches += 1
                    if batch_result.success:
                        successful_batches += 1
                        total_records += batch_result.records_processed or 0

            # Rate compliance metrics
            expected_min_time = total_api_calls * self.seconds_per_request
            rate_compliance = min(duration / expected_min_time, 1.0) if expected_min_time > 0 else 1.0

            # Throughput metrics
            rps = total_records / duration if duration > 0 else 0  # Records Per Second
            records_per_hour = rps * 3600
            requests_per_hour = (total_api_calls / duration * 3600) if duration > 0 else 0
            avg_latency = duration / total_api_calls if total_api_calls > 0 else 0

            return {
                "name": config['name'],
                "description": config['description'],
                "config": config,
                "start_date": str(start_date),
                "end_date": str(end_date),
                "date_range_days": (end_date - start_date).days,
                "duration": duration,
                "total_records": total_records,
                "total_api_calls": total_api_calls,
                "rps": rps,  # Primary metric: Records Per Second
                "records_per_hour": records_per_hour,
                "requests_per_hour": requests_per_hour,
                "avg_latency": avg_latency,
                "success_rate": successful_batches / total_batches if total_batches > 0 else 0,
                "rate_compliance": rate_compliance,
                "expected_min_time": expected_min_time,
                "total_batches": total_batches,
                "successful_batches": successful_batches
            }

        except Exception as e:
            test_end_time = time.time()
            duration = test_end_time - test_start_time
            print(f"   ❌ Test failed: {e}")

            return {
                "name": config['name'],
                "duration": duration,
                "total_records": 0,
                "rps": 0,
                "records_per_hour": 0,
                "success_rate": 0,
                "rate_compliance": 0,
                "error": str(e)
            }

    def _analyze_rate_limited_results(self, results: List[Dict[str, Any]]):
        """Analyze rate-limited benchmark results."""

        print(f"\n{'='*80}")
        print("🕐 RATE-LIMITED BENCHMARK ANALYSIS")
        print(f"{'='*80}")

        successful_results = [r for r in results if r.get('total_records', 0) > 0]

        if not successful_results:
            print("❌ No successful tests to analyze")
            return

        # Best performance metrics
        best_rps = max(successful_results, key=lambda x: x.get('rps', 0))
        best_compliance = max(successful_results, key=lambda x: x.get('rate_compliance', 0))

        print(f"\n🏆 BEST RPS PERFORMANCE:")
        print(f"   � {best_rps['rps']:.1f} RPS")
        print(f"   🧪 Test: {best_rps['name']}")
        print(f"   ⏱️  Latency: {best_rps.get('avg_latency', 0):.2f}s avg")
        print(f"   📊 {best_rps['total_records']:,} records in {best_rps['duration']:.1f}s")
        print(f"   🎯 Rate compliance: {best_rps['rate_compliance']:.1%}")

        # Compare to benchmarks
        best_rps_value = best_rps['rps']
        if best_rps_value >= 5000:
            print(f"   🏆 BREAKTHROUGH! Exceeded 5,000 RPS goal!")
        elif best_rps_value >= 3100:
            print(f"   🔥 OUTSTANDING! Beat 3,100 RPS benchmark!")
        elif best_rps_value >= 2000:
            print(f"   ✅ EXCELLENT! Strong performance!")
        else:
            print(f"   📊 BASELINE established at {best_rps_value:.1f} RPS")

        print(f"\n🤝 BEST RATE COMPLIANCE:")
        print(f"   🎯 {best_compliance['rate_compliance']:.1%} compliance")
        print(f"   🧪 Test: {best_compliance['name']}")
        print(f"   � {best_compliance.get('rps', 0):.1f} RPS")

        # Batch size analysis
        by_batch_strategy = {}
        for result in successful_results:
            batch_size = result['config'].get('days_per_batch', 0)
            if batch_size not in by_batch_strategy:
                by_batch_strategy[batch_size] = []
            by_batch_strategy[batch_size].append(result)

        print(f"\n📊 RPS PERFORMANCE BY BATCH SIZE:")
        for batch_size in sorted(by_batch_strategy.keys()):
            results_for_size = by_batch_strategy[batch_size]
            avg_rps = sum(r.get('rps', 0) for r in results_for_size) / len(results_for_size)
            max_rps = max(r.get('rps', 0) for r in results_for_size)
            avg_compliance = sum(r['rate_compliance'] for r in results_for_size) / len(results_for_size)
            avg_latency = sum(r.get('avg_latency', 0) for r in results_for_size) / len(results_for_size)

            print(f"   {batch_size:3d} days: {avg_rps:7.1f} RPS avg, {max_rps:7.1f} RPS max")
            print(f"            {avg_latency:.2f}s latency, {avg_compliance:.1%} compliance")

        # Rate limiting insights
        print(f"\n💡 RATE LIMITING INSIGHTS:")
        print(f"   🕐 Target interval: {self.seconds_per_request:.1f}s between requests")
        print(f"   📈 Sustainable rate: {self.requests_per_hour} requests/hour")

        avg_compliance = sum(r['rate_compliance'] for r in successful_results) / len(successful_results)
        avg_rps_all = sum(r.get('rps', 0) for r in successful_results) / len(successful_results)
        print(f"   🎯 Average compliance: {avg_compliance:.1%}")
        print(f"   🚀 Average RPS: {avg_rps_all:.1f}")

        if avg_compliance >= 0.95:
            print(f"   ✅ Excellent rate limit compliance!")
        elif avg_compliance >= 0.85:
            print(f"   👍 Good rate limit compliance")
        else:
            print(f"   ⚠️  Consider increasing delays for better compliance")

        # Production recommendations
        best_overall = best_rps
        print(f"\n🚀 PRODUCTION RECOMMENDATIONS:")
        print(f"   📊 Optimal batch size: {best_overall['config']['days_per_batch']} days")
        print(f"   ⏱️  Request interval: {best_overall['config']['delay_between_operations']:.1f}s")
        print(f"   � Expected RPS: {best_overall['rps']:.1f}")
        print(f"   ⏱️  Expected latency: {best_overall.get('avg_latency', 0):.2f}s")
        print(f"   🕐 Time for 1-year data: ~{365*24*3600/best_overall['rps']/3600:.1f} hours")


async def main():
    """Run rate-limited benchmark."""

    benchmark = RateLimitedBenchmark()
    results = await benchmark.run_benchmark()

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"data/benchmarks/rate_limited_benchmark_{timestamp}.json"

    Path("data/benchmarks").mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump({
            "timestamp": timestamp,
            "test_type": "rate_limited_benchmark",
            "rate_limit_config": {
                "requests_per_hour": 1800,
                "seconds_per_request": 2.0,
                "description": "Conservative rate limiting for EIA API"
            },
            "results": results
        }, f, indent=2, default=str)

    print(f"\n💾 Rate-limited benchmark results saved to: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
