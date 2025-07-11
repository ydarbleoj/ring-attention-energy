#!/usr/bin/env python3
"""
Targeted EIA Client Performance Optimization

This benchmark focuses on the specific performance bottlenecks identified:
1. Rate limiting optimization based on EIA's actual limits
2. Connection pooling and session optimization
3. Bulk data processing instead of individual DataFrame operations
4. Memory-efficient data handling

The goal is to achieve maximum performance while staying within EIA's limits.
"""

import asyncio
import time
from datetime import date, datetime
from pathlib import Path
import json
from typing import Dict, Any, List
import sys
import os
import logging

# Add project root to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig
from src.core.integrations.eia.client import EIAClient
from src.core.integrations.config import get_config

logger = logging.getLogger(__name__)


class TargetedOptimizationBenchmark:
    """Test targeted optimizations for maximum EIA API performance."""

    def __init__(self):
        self.region = "PACW"
        self.test_date_range = {
            "start": date(2024, 1, 1),
            "end": date(2024, 4, 1)  # 90 days
        }

        print("🎯 TARGETED EIA OPTIMIZATION BENCHMARK")
        print("⚡ Focus: Rate limiting, session optimization, bulk processing")
        print("🏁 Goal: Maximum performance within EIA API limits")

    async def run_targeted_optimizations(self):
        """Run targeted optimization tests."""

        print(f"\n{'='*70}")
        print("🔧 TARGETED OPTIMIZATION TESTS")
        print(f"{'='*70}")

        results = []

        # Test 1: Current baseline for comparison
        print(f"\n📊 TEST 1: Current Baseline")
        baseline_result = await self._test_baseline()
        results.append(baseline_result)

        # Test 2: Optimized rate limiting (theoretical maximum)
        print(f"\n⚡ TEST 2: Theoretical Maximum Rate")
        max_rate_result = await self._test_maximum_rate_limiting()
        results.append(max_rate_result)

        # Test 3: Safe aggressive rate limiting
        print(f"\n🛡️  TEST 3: Safe Aggressive Rate")
        safe_aggressive_result = await self._test_safe_aggressive_rate()
        results.append(safe_aggressive_result)

        # Test 4: Larger batch sizes with optimized timing
        print(f"\n📦 TEST 4: Larger Batches + Optimized Timing")
        large_batch_result = await self._test_large_batches_optimized()
        results.append(large_batch_result)

        # Test 5: Ultra-aggressive with maximum concurrency
        print(f"\n🚀 TEST 5: Ultra-Aggressive Configuration")
        ultra_result = await self._test_ultra_aggressive()
        results.append(ultra_result)

        # Analysis
        self._analyze_results(results)

        return results

    async def _test_baseline(self) -> Dict[str, Any]:
        """Test current best-known configuration."""

        config = ExtractBatchConfig(
            days_per_batch=45,
            max_concurrent_batches=2,
            delay_between_operations=2.0,
            max_operations_per_second=8.0,
            adaptive_batch_sizing=False
        )

        print(f"   45-day batches, 2.0s delays (previous best): ", end="", flush=True)

        return await self._run_test_config(config, "Baseline_Current_Best")

    async def _test_maximum_rate_limiting(self) -> Dict[str, Any]:
        """Test with theoretical maximum rate (1.39 requests/sec)."""

        # EIA allows 5000/hour = 1.389/sec = 0.72s between requests
        config = ExtractBatchConfig(
            days_per_batch=45,
            max_concurrent_batches=2,
            delay_between_operations=0.72,  # Theoretical maximum
            max_operations_per_second=20.0,
            adaptive_batch_sizing=False
        )

        print(f"   45-day batches, 0.72s delays (theoretical max): ", end="", flush=True)

        return await self._run_test_config(config, "Theoretical_Maximum_Rate")

    async def _test_safe_aggressive_rate(self) -> Dict[str, Any]:
        """Test with safe but aggressive rate limiting."""

        # 10% safety margin: 0.72 * 1.1 = 0.8s
        config = ExtractBatchConfig(
            days_per_batch=45,
            max_concurrent_batches=2,
            delay_between_operations=0.8,  # Safe aggressive
            max_operations_per_second=18.0,
            adaptive_batch_sizing=False
        )

        print(f"   45-day batches, 0.8s delays (safe aggressive): ", end="", flush=True)

        return await self._run_test_config(config, "Safe_Aggressive_Rate")

    async def _test_large_batches_optimized(self) -> Dict[str, Any]:
        """Test larger batches with optimized timing."""

        # Larger batches with aggressive timing
        config = ExtractBatchConfig(
            days_per_batch=60,  # Larger batches
            max_concurrent_batches=2,
            delay_between_operations=0.9,  # Slightly more conservative
            max_operations_per_second=15.0,
            adaptive_batch_sizing=False
        )

        print(f"   60-day batches, 0.9s delays: ", end="", flush=True)

        return await self._run_test_config(config, "Large_Batches_Optimized")

    async def _test_ultra_aggressive(self) -> Dict[str, Any]:
        """Test ultra-aggressive configuration with maximum concurrency."""

        # Maximum concurrency with tight timing
        config = ExtractBatchConfig(
            days_per_batch=75,  # Very large batches
            max_concurrent_batches=3,  # More concurrency
            delay_between_operations=0.75,  # Very aggressive
            max_operations_per_second=25.0,
            adaptive_batch_sizing=False
        )

        print(f"   75-day batches, 3 concurrent, 0.75s delays: ", end="", flush=True)

        return await self._run_test_config(config, "Ultra_Aggressive")

    async def _run_test_config(self, config: ExtractBatchConfig, test_name: str) -> Dict[str, Any]:
        """Run a test with the given configuration."""

        start_time = time.time()

        try:
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/targeted")
            orchestrator.batch_config = config

            initial_ops = orchestrator.metrics.total_operations

            results = await orchestrator.process_data(
                start_date=self.test_date_range['start'],
                end_date=self.test_date_range['end'],
                region=self.region,
                data_types=["demand", "generation"]
            )

            end_time = time.time()
            total_duration = end_time - start_time

            total_records = 0
            final_ops = orchestrator.metrics.total_operations
            total_api_calls = final_ops - initial_ops

            for data_type, batch_results in results.items():
                for batch_result in batch_results:
                    if batch_result.success:
                        total_records += batch_result.records_processed or 0

            rps = total_records / total_duration if total_duration > 0 else 0

            # Calculate API calls per second for rate limiting analysis
            api_calls_per_sec = total_api_calls / total_duration if total_duration > 0 else 0

            print(f"{rps:.1f} RPS ({total_records:,} records, {api_calls_per_sec:.2f} API/s)")

            return {
                "test_name": test_name,
                "rps": rps,
                "total_records": total_records,
                "total_api_calls": total_api_calls,
                "api_calls_per_sec": api_calls_per_sec,
                "duration": total_duration,
                "config": {
                    "days_per_batch": config.days_per_batch,
                    "max_concurrent_batches": config.max_concurrent_batches,
                    "delay_between_operations": config.delay_between_operations,
                    "max_operations_per_second": config.max_operations_per_second
                }
            }

        except Exception as e:
            print(f"Failed: {e}")
            return {
                "test_name": test_name,
                "rps": 0,
                "error": str(e),
                "config": {
                    "days_per_batch": config.days_per_batch,
                    "max_concurrent_batches": config.max_concurrent_batches,
                    "delay_between_operations": config.delay_between_operations
                }
            }

    def _analyze_results(self, results: List[Dict[str, Any]]):
        """Analyze optimization results and provide insights."""

        print(f"\n{'='*70}")
        print("📈 TARGETED OPTIMIZATION ANALYSIS")
        print(f"{'='*70}")

        # Filter successful results
        successful_results = [r for r in results if r.get('rps', 0) > 0]

        if not successful_results:
            print("❌ No successful results to analyze")
            return

        # Sort by RPS
        successful_results.sort(key=lambda x: x['rps'], reverse=True)

        print(f"\n🏆 PERFORMANCE RANKING:")
        print(f"{'Rank':<4} {'Test':<25} {'RPS':<8} {'API/s':<8} {'Delay':<8} {'Batch':<8} {'Concur':<6}")
        print(f"{'-'*80}")

        for i, result in enumerate(successful_results, 1):
            config = result.get('config', {})
            print(f"{i:<4} {result['test_name']:<25} {result['rps']:<8.1f} "
                  f"{result.get('api_calls_per_sec', 0):<8.2f} "
                  f"{config.get('delay_between_operations', 0):<8.1f}s "
                  f"{config.get('days_per_batch', 0):<8}d "
                  f"{config.get('max_concurrent_batches', 0):<6}")

        # Analysis insights
        baseline_rps = next((r['rps'] for r in successful_results if 'Baseline' in r['test_name']), 0)
        best_rps = successful_results[0]['rps'] if successful_results else 0

        print(f"\n📊 KEY INSIGHTS:")
        if baseline_rps > 0:
            improvement = ((best_rps - baseline_rps) / baseline_rps) * 100
            print(f"   🚀 Best improvement: {improvement:+.1f}% ({baseline_rps:.1f} → {best_rps:.1f} RPS)")

        # Rate limiting analysis
        print(f"\n⏱️  RATE LIMITING ANALYSIS:")
        eia_theoretical_limit = 1.389  # 5000/hour = 1.389/sec

        for result in successful_results:
            api_rate = result.get('api_calls_per_sec', 0)
            if api_rate > 0:
                utilization = (api_rate / eia_theoretical_limit) * 100
                status = "⚠️  OVER LIMIT" if api_rate > eia_theoretical_limit else "✅ Within limit"
                print(f"   {result['test_name']}: {api_rate:.2f} API/s ({utilization:.1f}% of limit) {status}")

        # Best configuration
        best_result = successful_results[0]
        print(f"\n🏆 BEST CONFIGURATION:")
        print(f"   Test: {best_result['test_name']}")
        print(f"   Performance: {best_result['rps']:.1f} RPS")
        print(f"   API Rate: {best_result.get('api_calls_per_sec', 0):.2f} requests/sec")

        config = best_result.get('config', {})
        print(f"   Batch size: {config.get('days_per_batch', 0)} days")
        print(f"   Concurrency: {config.get('max_concurrent_batches', 0)} batches")
        print(f"   Delay: {config.get('delay_between_operations', 0):.1f}s")

        # Recommendations
        print(f"\n💡 PRODUCTION RECOMMENDATIONS:")

        # Find best configuration within rate limits
        within_limits = [r for r in successful_results
                        if r.get('api_calls_per_sec', 0) <= eia_theoretical_limit * 0.95]  # 5% safety margin

        if within_limits:
            best_safe = within_limits[0]
            print(f"   🛡️  Best safe configuration: {best_safe['test_name']}")
            print(f"      Performance: {best_safe['rps']:.1f} RPS")
            print(f"      API Rate: {best_safe.get('api_calls_per_sec', 0):.2f} requests/sec (safe)")

            safe_config = best_safe.get('config', {})
            print(f"      Configuration: {safe_config.get('days_per_batch', 0)}d batches, "
                  f"{safe_config.get('max_concurrent_batches', 0)} concurrent, "
                  f"{safe_config.get('delay_between_operations', 0):.1f}s delay")
        else:
            print(f"   ⚠️  All configurations exceed EIA rate limits - reduce concurrency or increase delays")

        # Save results
        self._save_results(results)

    def _save_results(self, results: List[Dict[str, Any]]):
        """Save detailed results to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        results_data = {
            "benchmark_type": "targeted_optimizations",
            "timestamp": timestamp,
            "test_region": self.region,
            "date_range": {
                "start": self.test_date_range['start'].isoformat(),
                "end": self.test_date_range['end'].isoformat()
            },
            "eia_rate_limit": {
                "theoretical_max_per_hour": 5000,
                "theoretical_max_per_second": 1.389
            },
            "results": results,
            "summary": {
                "total_tests": len(results),
                "successful_tests": len([r for r in results if r.get('rps', 0) > 0]),
                "best_rps": max((r.get('rps', 0) for r in results), default=0),
                "best_config": max(results, key=lambda x: x.get('rps', 0), default={}).get('test_name', 'none')
            }
        }

        results_dir = Path("data/benchmarks")
        results_dir.mkdir(parents=True, exist_ok=True)

        results_file = results_dir / f"targeted_optimizations_{timestamp}.json"

        with open(results_file, 'w') as f:
            json.dump(results_data, f, indent=2, default=str)

        print(f"\n💾 Results saved to: {results_file}")


async def main():
    """Run the targeted optimizations benchmark."""
    benchmark = TargetedOptimizationBenchmark()
    await benchmark.run_targeted_optimizations()


if __name__ == "__main__":
    asyncio.run(main())
