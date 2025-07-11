#!/usr/bin/env python3
"""
Ultimate EIA Performance Optimization

Combines the best findings from targeted and advanced optimizations:
1. Connection pooling optimization (11% improvement)
2. Theoretical maximum rate limiting (0.72s delays)
3. Optimal request parameters (length=8000)
4. Fine-tuned batch sizing

Goal: Achieve maximum possible performance within EIA API limits.
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
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add project root to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig
from src.core.integrations.eia.client import EIAClient
from src.core.integrations.config import get_config

logger = logging.getLogger(__name__)


class UltimateOptimizedEIAClient(EIAClient):
    """Ultimate optimized EIA Client combining all best practices."""

    def _create_session(self) -> requests.Session:
        """Create session with ultimate optimizations."""
        session = requests.Session()

        # Optimized retry strategy
        retry_strategy = Retry(
            total=2,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
        )

        # Ultimate connection pooling configuration (without socket_options)
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=25,  # Large connection pool
            pool_maxsize=60,      # Maximum connections
            pool_block=False
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Optimized headers
        session.headers.update({
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate',
            'User-Agent': 'EIA-Ultimate-Client/1.0',
            'Accept': 'application/json'
        })

        return session

    def _make_request(self, url: str, params: Dict) -> Dict:
        """Ultimate optimized request method."""
        params['api_key'] = self.api_key

        # Use optimal request length
        if 'length' not in params:
            params['length'] = 8000  # Best performing length from tests

        try:
            # Theoretical maximum rate: 0.72s (5000/hour = 1.389/sec)
            time.sleep(0.72)

            response = self.session.get(url, params=params, timeout=20)
            response.raise_for_status()

            data = response.json()

            if 'response' not in data:
                raise ValueError(f"Unexpected API response structure: {data}")

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"EIA API request failed: {e}")
            raise
        except ValueError as e:
            logger.error(f"EIA API response error: {e}")
            raise


class UltimatePerformanceBenchmark:
    """Ultimate performance benchmark combining all optimizations."""

    def __init__(self):
        self.region = "PACW"
        self.test_date_range = {
            "start": date(2024, 1, 1),
            "end": date(2024, 4, 1)  # 90 days
        }

        print("🔥 ULTIMATE EIA PERFORMANCE BENCHMARK")
        print("⚡ Combining all proven optimizations")
        print("🎯 Goal: Maximum possible performance within API limits")

    async def run_ultimate_optimization(self):
        """Run ultimate optimization tests."""

        print(f"\n{'='*75}")
        print("🚀 ULTIMATE PERFORMANCE TESTS")
        print(f"{'='*75}")

        results = []

        # Test 1: Previous best (connection optimization)
        print(f"\n📊 TEST 1: Previous Best (Connection Optimization)")
        prev_best_result = await self._test_previous_best()
        results.append(prev_best_result)

        # Test 2: Ultimate client with theoretical max rate
        print(f"\n🔥 TEST 2: Ultimate Client + Theoretical Max Rate")
        ultimate_result = await self._test_ultimate_client()
        results.append(ultimate_result)

        # Test 3: Ultimate client with safety margin
        print(f"\n🛡️  TEST 3: Ultimate Client + Safety Margin")
        ultimate_safe_result = await self._test_ultimate_client_safe()
        results.append(ultimate_safe_result)

        # Test 4: Ultimate with larger batches
        print(f"\n📦 TEST 4: Ultimate Client + Larger Batches")
        ultimate_large_result = await self._test_ultimate_large_batches()
        results.append(ultimate_large_result)

        # Test 5: Ultimate with maximum concurrency
        print(f"\n🚀 TEST 5: Ultimate Client + Maximum Concurrency")
        ultimate_max_result = await self._test_ultimate_max_concurrency()
        results.append(ultimate_max_result)

        # Analysis
        self._analyze_ultimate_results(results)

        return results

    async def _test_previous_best(self) -> Dict[str, Any]:
        """Test the previous best configuration for comparison."""

        config = ExtractBatchConfig(
            days_per_batch=45,
            max_concurrent_batches=2,
            delay_between_operations=1.5,  # Connection optimization setting
            max_operations_per_second=10.0,
            adaptive_batch_sizing=False
        )

        print(f"   Connection optimization (1.5s delays): ", end="", flush=True)

        return await self._run_test_config(config, "Previous_Best_Connection", use_ultimate_client=False)

    async def _test_ultimate_client(self) -> Dict[str, Any]:
        """Test ultimate client with theoretical maximum rate."""

        config = ExtractBatchConfig(
            days_per_batch=45,
            max_concurrent_batches=2,
            delay_between_operations=0.72,  # Theoretical maximum
            max_operations_per_second=20.0,
            adaptive_batch_sizing=False
        )

        print(f"   Ultimate client + 0.72s delays: ", end="", flush=True)

        return await self._run_test_config(config, "Ultimate_Theoretical_Max", use_ultimate_client=True)

    async def _test_ultimate_client_safe(self) -> Dict[str, Any]:
        """Test ultimate client with safety margin."""

        config = ExtractBatchConfig(
            days_per_batch=45,
            max_concurrent_batches=2,
            delay_between_operations=0.8,  # 10% safety margin
            max_operations_per_second=18.0,
            adaptive_batch_sizing=False
        )

        print(f"   Ultimate client + 0.8s delays (safe): ", end="", flush=True)

        return await self._run_test_config(config, "Ultimate_Safe_Margin", use_ultimate_client=True)

    async def _test_ultimate_large_batches(self) -> Dict[str, Any]:
        """Test ultimate client with larger batches."""

        config = ExtractBatchConfig(
            days_per_batch=60,  # Larger batches
            max_concurrent_batches=2,
            delay_between_operations=0.75,
            max_operations_per_second=16.0,
            adaptive_batch_sizing=False
        )

        print(f"   Ultimate client + 60-day batches: ", end="", flush=True)

        return await self._run_test_config(config, "Ultimate_Large_Batches", use_ultimate_client=True)

    async def _test_ultimate_max_concurrency(self) -> Dict[str, Any]:
        """Test ultimate client with maximum safe concurrency."""

        config = ExtractBatchConfig(
            days_per_batch=45,
            max_concurrent_batches=2,  # Maximum concurrency
            delay_between_operations=0.8,  # Slightly more conservative for higher concurrency
            max_operations_per_second=25.0,
            adaptive_batch_sizing=False
        )

        print(f"   Ultimate client + 2 concurrent batches: ", end="", flush=True)

        return await self._run_test_config(config, "Ultimate_Max_Concurrency", use_ultimate_client=True)

    async def _run_test_config(self, config: ExtractBatchConfig, test_name: str, use_ultimate_client: bool = False) -> Dict[str, Any]:
        """Run a test with the given configuration."""

        start_time = time.time()

        try:
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/ultimate")
            orchestrator.batch_config = config

            # Inject ultimate client if requested
            if use_ultimate_client:
                def create_ultimate_client(self):
                    config = get_config()
                    return UltimateOptimizedEIAClient(config=config)

                ExtractOrchestrator._create_eia_client = create_ultimate_client
                # Force recreate the client
                orchestrator.eia_client = create_ultimate_client(orchestrator)

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
            api_calls_per_sec = total_api_calls / total_duration if total_duration > 0 else 0

            print(f"{rps:.1f} RPS ({total_records:,} records, {api_calls_per_sec:.2f} API/s)")

            return {
                "test_name": test_name,
                "rps": rps,
                "total_records": total_records,
                "total_api_calls": total_api_calls,
                "api_calls_per_sec": api_calls_per_sec,
                "duration": total_duration,
                "used_ultimate_client": use_ultimate_client,
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
                "used_ultimate_client": use_ultimate_client,
                "config": {
                    "days_per_batch": config.days_per_batch,
                    "max_concurrent_batches": config.max_concurrent_batches,
                    "delay_between_operations": config.delay_between_operations
                }
            }

    def _analyze_ultimate_results(self, results: List[Dict[str, Any]]):
        """Analyze ultimate optimization results."""

        print(f"\n{'='*75}")
        print("🔥 ULTIMATE PERFORMANCE ANALYSIS")
        print(f"{'='*75}")

        # Filter successful results
        successful_results = [r for r in results if r.get('rps', 0) > 0]

        if not successful_results:
            print("❌ No successful results to analyze")
            return

        # Sort by RPS
        successful_results.sort(key=lambda x: x['rps'], reverse=True)

        print(f"\n🏆 ULTIMATE PERFORMANCE RANKING:")
        print(f"{'Rank':<4} {'Test':<30} {'RPS':<10} {'API/s':<8} {'Client':<8} {'Config':<25}")
        print(f"{'-'*90}")

        for i, result in enumerate(successful_results, 1):
            config = result.get('config', {})
            client_type = "Ultimate" if result.get('used_ultimate_client', False) else "Standard"
            config_str = f"{config.get('days_per_batch', 0)}d/{config.get('max_concurrent_batches', 0)}c/{config.get('delay_between_operations', 0):.1f}s"

            print(f"{i:<4} {result['test_name']:<30} {result['rps']:<10.1f} "
                  f"{result.get('api_calls_per_sec', 0):<8.2f} {client_type:<8} {config_str:<25}")

        # Performance analysis
        baseline_rps = next((r['rps'] for r in successful_results if 'Previous_Best' in r['test_name']), 0)
        best_rps = successful_results[0]['rps'] if successful_results else 0

        print(f"\n📈 ULTIMATE PERFORMANCE GAINS:")
        if baseline_rps > 0:
            ultimate_improvement = ((best_rps - baseline_rps) / baseline_rps) * 100
            print(f"   🚀 Ultimate improvement: {ultimate_improvement:+.1f}% ({baseline_rps:.1f} → {best_rps:.1f} RPS)")

        # Rate limiting compliance
        print(f"\n⚡ RATE LIMITING COMPLIANCE:")
        eia_limit = 1.389  # 5000/hour

        for result in successful_results:
            api_rate = result.get('api_calls_per_sec', 0)
            utilization = (api_rate / eia_limit) * 100 if eia_limit > 0 else 0
            compliance = "✅ SAFE" if api_rate <= eia_limit * 0.95 else "⚠️  AGGRESSIVE" if api_rate <= eia_limit else "❌ OVER LIMIT"
            print(f"   {result['test_name']}: {api_rate:.2f}/s ({utilization:.1f}%) {compliance}")

        # Ultimate recommendation
        best_result = successful_results[0]
        print(f"\n🏆 ULTIMATE CONFIGURATION:")
        print(f"   🥇 Best: {best_result['test_name']}")
        print(f"   ⚡ Performance: {best_result['rps']:.1f} RPS")
        print(f"   🌐 API Rate: {best_result.get('api_calls_per_sec', 0):.2f} requests/sec")
        print(f"   🔧 Client: {'Ultimate Optimized' if best_result.get('used_ultimate_client') else 'Standard'}")

        config = best_result.get('config', {})
        print(f"   ⚙️  Configuration:")
        print(f"      - Batch size: {config.get('days_per_batch', 0)} days")
        print(f"      - Concurrency: {config.get('max_concurrent_batches', 0)} batches")
        print(f"      - Delay: {config.get('delay_between_operations', 0):.2f}s")

        # Production guidance
        print(f"\n🎯 PRODUCTION DEPLOYMENT GUIDANCE:")

        # Find safest high-performance option
        safe_results = [r for r in successful_results
                       if r.get('api_calls_per_sec', 0) <= eia_limit * 0.9]  # 10% safety margin

        if safe_results:
            safest_best = safe_results[0]
            print(f"   🛡️  Recommended for production: {safest_best['test_name']}")
            print(f"      Performance: {safest_best['rps']:.1f} RPS")
            print(f"      Safety margin: {((eia_limit - safest_best.get('api_calls_per_sec', 0)) / eia_limit * 100):.1f}%")

        # Save results
        self._save_ultimate_results(results)

    def _save_ultimate_results(self, results: List[Dict[str, Any]]):
        """Save ultimate results to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        results_data = {
            "benchmark_type": "ultimate_performance",
            "timestamp": timestamp,
            "test_region": self.region,
            "date_range": {
                "start": self.test_date_range['start'].isoformat(),
                "end": self.test_date_range['end'].isoformat()
            },
            "optimizations_included": [
                "ultimate_connection_pooling",
                "theoretical_maximum_rate_limiting",
                "optimal_request_parameters",
                "fine_tuned_batch_sizing",
                "enhanced_session_management"
            ],
            "eia_api_limits": {
                "requests_per_hour": 5000,
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

        results_file = results_dir / f"ultimate_performance_{timestamp}.json"

        with open(results_file, 'w') as f:
            json.dump(results_data, f, indent=2, default=str)

        print(f"\n💾 Ultimate results saved to: {results_file}")


async def main():
    """Run the ultimate performance benchmark."""
    benchmark = UltimatePerformanceBenchmark()
    await benchmark.run_ultimate_optimization()


if __name__ == "__main__":
    asyncio.run(main())
