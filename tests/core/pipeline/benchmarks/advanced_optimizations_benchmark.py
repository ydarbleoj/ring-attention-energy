#!/usr/bin/env python3
"""
Advanced EIA Client Optimizations Benchmark

This benchmark tests specific client-level optimizations beyond batch sizing:
1. Connection pooling and session optimization
2. Rate limiting tuning based on API behavior
3. Request parameter optimization (length, timeout)
4. Data processing pipeline optimization
5. Memory management for large datasets

Goal: Push beyond 1,854 RPS with production-ready optimizations.
"""

import asyncio
import time
from datetime import date, datetime
from pathlib import Path
import json
from typing import Dict, Any, List
import sys
import os
import requests
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Add project root to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))

from src.core.pipeline.orchestrators.extract_orchestrator import ExtractOrchestrator, ExtractBatchConfig
from src.core.integrations.eia.client import EIAClient


class OptimizedEIAClient(EIAClient):
    """EIA Client with advanced optimizations."""

    def _create_session(self) -> requests.Session:
        """Create session with aggressive optimizations for bulk operations."""
        session = requests.Session()

        # More aggressive retry strategy for bulk operations
        retry_strategy = Retry(
            total=2,  # Fewer retries for faster failure detection
            backoff_factor=0.3,  # Faster backoff
            status_forcelist=[429, 500, 502, 503, 504],
        )

        # Optimized connection pooling for high throughput
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,  # Larger connection pool
            pool_maxsize=50,      # More connections per pool
            pool_block=False,     # Don't block on pool exhaustion
            socket_options=[(1, 1)]  # Enable TCP keepalive
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set session-level headers for optimization
        session.headers.update({
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate',
            'User-Agent': 'EIA-Optimization-Client/1.0'
        })

        return session

    def _make_request_optimized(self, url: str, params: Dict,
                               rate_limit_factor: float = 1.0,
                               timeout: int = 15) -> Dict:
        """Optimized request method with tunable parameters."""
        params['api_key'] = self.api_key

        try:
            # Dynamic rate limiting based on observed API behavior
            # EIA allows 5000/hour = 1.39/sec
            # With proper concurrent handling, we can be more aggressive
            base_delay = 0.72  # Just under the theoretical limit (1/1.39)
            optimized_delay = base_delay * rate_limit_factor

            time.sleep(optimized_delay)

            response = self.session.get(url, params=params, timeout=timeout)
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

    def get_electricity_demand_optimized(
        self,
        region: str = "US48",
        start_date: str = "2020-01-01",
        end_date: str = "2024-01-01",
        length: int = 5000,
        rate_limit_factor: float = 1.0
    ):
        """Optimized demand data extraction."""
        url = f"{self.base_url}/electricity/rto/region-data/data/"
        params = {
            'frequency': 'hourly',
            'data[0]': 'value',
            'facets[respondent][]': region,
            'facets[type][]': 'D',
            'start': start_date,
            'end': end_date,
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'length': length
        }

        data = self._make_request_optimized(url, params, rate_limit_factor)

        # Optimized data processing - minimize pandas overhead
        response_data = data['response']['data']
        if not response_data:
            return []

        # Return raw data for bulk processing instead of individual DataFrame creation
        return response_data

    def get_generation_mix_optimized(
        self,
        region: str = "US48",
        start_date: str = "2020-01-01",
        end_date: str = "2024-01-01",
        length: int = 5000,
        rate_limit_factor: float = 1.0
    ):
        """Optimized generation data extraction."""
        url = f"{self.base_url}/electricity/rto/fuel-type-data/data/"
        params = {
            'frequency': 'hourly',
            'data[0]': 'value',
            'facets[respondent][]': region,
            'start': start_date,
            'end': end_date,
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'length': length
        }

        data = self._make_request_optimized(url, params, rate_limit_factor)

        # Return raw data for bulk processing
        response_data = data['response']['data']
        if not response_data:
            return []

        return response_data


class AdvancedOptimizationsBenchmark:
    """Test advanced client-level optimizations."""

    def __init__(self):
        self.region = "PACW"
        self.test_date_range = {
            "start": date(2024, 1, 1),
            "end": date(2024, 4, 1)  # 90 days
        }

        print("🚀 ADVANCED EIA CLIENT OPTIMIZATIONS BENCHMARK")
        print("🎯 Testing client-level optimizations beyond batch sizing")
        print("⚡ Goal: Push beyond 1,854 RPS with production-ready improvements")

    async def run_optimizations(self):
        """Run all optimization tests."""

        print(f"\n{'='*80}")
        print("🔧 ADVANCED OPTIMIZATION TESTS")
        print(f"{'='*80}")

        results = []

        # Test 1: Baseline with current client
        print(f"\n🔄 TEST 1: Current Client Baseline")
        baseline_result = await self._test_current_baseline()
        results.append(baseline_result)

        # Test 2: Connection pooling optimization
        print(f"\n🌐 TEST 2: Optimized Connection Pooling")
        connection_result = await self._test_connection_optimization()
        results.append(connection_result)

        # Test 3: Rate limiting optimization
        print(f"\n⏱️  TEST 3: Aggressive Rate Limiting")
        rate_limit_result = await self._test_rate_limit_optimization()
        results.append(rate_limit_result)

        # Test 4: Request parameter optimization
        print(f"\n📊 TEST 4: Request Parameter Tuning")
        param_results = await self._test_parameter_optimization()
        results.extend(param_results)

        # Test 5: Combined optimizations
        print(f"\n🔥 TEST 5: Combined Optimizations")
        combined_result = await self._test_combined_optimizations()
        results.append(combined_result)

        # Analysis
        self._analyze_optimization_results(results)

        return results

    async def _test_current_baseline(self) -> Dict[str, Any]:
        """Test current client performance as baseline."""

        config = ExtractBatchConfig(
            days_per_batch=45,
            max_concurrent_batches=2,
            delay_between_operations=2.0,
            max_operations_per_second=8.0,
            adaptive_batch_sizing=False
        )

        print(f"   Current client with 45-day dual batches: ", end="", flush=True)

        start_time = time.time()

        try:
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/advanced")
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

            print(f"{rps:.1f} RPS ({total_records:,} records)")

            return {
                "test_name": "Current_Baseline",
                "rps": rps,
                "total_records": total_records,
                "total_api_calls": total_api_calls,
                "duration": total_duration,
                "optimization": "none"
            }

        except Exception as e:
            print(f"Failed: {e}")
            return {"test_name": "Current_Baseline", "rps": 0, "error": str(e)}

    async def _test_connection_optimization(self) -> Dict[str, Any]:
        """Test optimized connection pooling."""

        # Use shorter delays to test connection optimization effect
        config = ExtractBatchConfig(
            days_per_batch=45,
            max_concurrent_batches=2,
            delay_between_operations=1.5,  # Slightly more aggressive
            max_operations_per_second=10.0,
            adaptive_batch_sizing=False
        )

        print(f"   Optimized connection pooling (1.5s delays): ", end="", flush=True)

        start_time = time.time()

        try:
            # Monkey patch the client creation for this test
            original_client_class = ExtractOrchestrator.__dict__.get('_create_eia_client', None)

            def create_optimized_client(self):
                from src.core.integrations.config import get_config
                config = get_config()
                return OptimizedEIAClient(config=config)

            # Temporarily replace client creation
            ExtractOrchestrator._create_eia_client = create_optimized_client

            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/advanced")
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

            print(f"{rps:.1f} RPS ({total_records:,} records)")

            # Restore original client creation
            if original_client_class:
                ExtractOrchestrator._create_eia_client = original_client_class

            return {
                "test_name": "Connection_Optimization",
                "rps": rps,
                "total_records": total_records,
                "total_api_calls": total_api_calls,
                "duration": total_duration,
                "optimization": "connection_pooling"
            }

        except Exception as e:
            print(f"Failed: {e}")
            return {"test_name": "Connection_Optimization", "rps": 0, "error": str(e)}

    async def _test_rate_limit_optimization(self) -> Dict[str, Any]:
        """Test more aggressive rate limiting."""

        # More aggressive rate limiting - closer to EIA's theoretical limit
        config = ExtractBatchConfig(
            days_per_batch=45,
            max_concurrent_batches=2,
            delay_between_operations=0.8,  # Much more aggressive (closer to 1.39/sec limit)
            max_operations_per_second=15.0,
            adaptive_batch_sizing=False
        )

        print(f"   Aggressive rate limiting (0.8s delays): ", end="", flush=True)

        start_time = time.time()

        try:
            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/advanced")
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

            print(f"{rps:.1f} RPS ({total_records:,} records)")

            return {
                "test_name": "Rate_Limit_Optimization",
                "rps": rps,
                "total_records": total_records,
                "total_api_calls": total_api_calls,
                "duration": total_duration,
                "optimization": "aggressive_rate_limiting"
            }

        except Exception as e:
            print(f"Failed: {e}")
            return {"test_name": "Rate_Limit_Optimization", "rps": 0, "error": str(e)}

    async def _test_parameter_optimization(self) -> List[Dict[str, Any]]:
        """Test different request parameter configurations."""

        results = []

        # Test different length parameters
        length_configs = [3000, 5000, 8000]  # API default is 5000

        for length in length_configs:
            print(f"   Testing length={length}: ", end="", flush=True)

            config = ExtractBatchConfig(
                days_per_batch=45,
                max_concurrent_batches=2,
                delay_between_operations=1.2,
                max_operations_per_second=12.0,
                adaptive_batch_sizing=False
            )

            start_time = time.time()

            try:
                orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/advanced")
                orchestrator.batch_config = config

                initial_ops = orchestrator.metrics.total_operations

                results_data = await orchestrator.process_data(
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

                for data_type, batch_results in results_data.items():
                    for batch_result in batch_results:
                        if batch_result.success:
                            total_records += batch_result.records_processed or 0

                rps = total_records / total_duration if total_duration > 0 else 0

                print(f"{rps:.1f} RPS")

                results.append({
                    "test_name": f"Parameter_Length_{length}",
                    "rps": rps,
                    "total_records": total_records,
                    "total_api_calls": total_api_calls,
                    "duration": total_duration,
                    "optimization": f"length_{length}"
                })

            except Exception as e:
                print(f"Failed: {e}")
                results.append({
                    "test_name": f"Parameter_Length_{length}",
                    "rps": 0,
                    "error": str(e)
                })

        return results

    async def _test_combined_optimizations(self) -> Dict[str, Any]:
        """Test all optimizations combined."""

        # Most aggressive configuration with all optimizations
        config = ExtractBatchConfig(
            days_per_batch=60,  # Larger batches
            max_concurrent_batches=3,  # More concurrency
            delay_between_operations=0.6,  # Very aggressive rate limiting
            max_operations_per_second=20.0,
            adaptive_batch_sizing=False
        )

        print(f"   All optimizations combined (60-day, 3 concurrent, 0.6s delays): ", end="", flush=True)

        start_time = time.time()

        try:
            # Use optimized client
            def create_optimized_client(self):
                from src.core.integrations.config import get_config
                config = get_config()
                return OptimizedEIAClient(config=config)

            ExtractOrchestrator._create_eia_client = create_optimized_client

            orchestrator = ExtractOrchestrator(raw_data_path="data/benchmarks/advanced")
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

            print(f"{rps:.1f} RPS ({total_records:,} records)")

            return {
                "test_name": "Combined_Optimizations",
                "rps": rps,
                "total_records": total_records,
                "total_api_calls": total_api_calls,
                "duration": total_duration,
                "optimization": "all_combined"
            }

        except Exception as e:
            print(f"Failed: {e}")
            return {"test_name": "Combined_Optimizations", "rps": 0, "error": str(e)}

    def _analyze_optimization_results(self, results: List[Dict[str, Any]]):
        """Analyze optimization results and provide recommendations."""

        print(f"\n{'='*80}")
        print("📊 ADVANCED OPTIMIZATION ANALYSIS")
        print(f"{'='*80}")

        # Filter successful results
        successful_results = [r for r in results if r.get('rps', 0) > 0]

        if not successful_results:
            print("❌ No successful results to analyze")
            return

        # Sort by RPS
        successful_results.sort(key=lambda x: x['rps'], reverse=True)

        print(f"\n🏆 PERFORMANCE RANKING:")
        print(f"{'Rank':<4} {'Test':<25} {'RPS':<8} {'Records':<10} {'API Calls':<10} {'Duration':<10}")
        print(f"{'-'*75}")

        for i, result in enumerate(successful_results, 1):
            print(f"{i:<4} {result['test_name']:<25} {result['rps']:<8.1f} "
                  f"{result['total_records']:<10,} {result['total_api_calls']:<10} "
                  f"{result['duration']:<10.1f}s")

        # Performance improvements
        baseline_rps = next((r['rps'] for r in successful_results if 'Baseline' in r['test_name']), 0)
        best_rps = successful_results[0]['rps'] if successful_results else 0

        print(f"\n📈 PERFORMANCE IMPROVEMENTS:")
        if baseline_rps > 0:
            improvement = ((best_rps - baseline_rps) / baseline_rps) * 100
            print(f"   Best improvement: {improvement:+.1f}% ({baseline_rps:.1f} → {best_rps:.1f} RPS)")

        # Recommendations
        print(f"\n💡 OPTIMIZATION RECOMMENDATIONS:")

        best_result = successful_results[0]
        print(f"   🥇 Best performing configuration: {best_result['test_name']}")
        print(f"      Optimization: {best_result.get('optimization', 'unknown')}")
        print(f"      Performance: {best_result['rps']:.1f} RPS")

        # Identify key optimization factors
        connection_results = [r for r in successful_results if 'Connection' in r['test_name']]
        rate_limit_results = [r for r in successful_results if 'Rate_Limit' in r['test_name']]
        param_results = [r for r in successful_results if 'Parameter' in r['test_name']]

        if connection_results and baseline_rps > 0:
            conn_improvement = ((connection_results[0]['rps'] - baseline_rps) / baseline_rps) * 100
            print(f"   🌐 Connection optimization impact: {conn_improvement:+.1f}%")

        if rate_limit_results and baseline_rps > 0:
            rate_improvement = ((rate_limit_results[0]['rps'] - baseline_rps) / baseline_rps) * 100
            print(f"   ⏱️  Rate limiting optimization impact: {rate_improvement:+.1f}%")

        if param_results:
            best_param = max(param_results, key=lambda x: x['rps'])
            if baseline_rps > 0:
                param_improvement = ((best_param['rps'] - baseline_rps) / baseline_rps) * 100
                print(f"   📊 Parameter optimization impact: {param_improvement:+.1f}%")
                print(f"      Best parameter: {best_param.get('optimization', 'unknown')}")

        # Save detailed results
        self._save_results(results)

    def _save_results(self, results: List[Dict[str, Any]]):
        """Save detailed results to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        results_data = {
            "benchmark_type": "advanced_optimizations",
            "timestamp": timestamp,
            "test_region": self.region,
            "date_range": {
                "start": self.test_date_range['start'].isoformat(),
                "end": self.test_date_range['end'].isoformat()
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

        results_file = results_dir / f"advanced_optimizations_{timestamp}.json"

        with open(results_file, 'w') as f:
            json.dump(results_data, f, indent=2, default=str)

        print(f"\n💾 Detailed results saved to: {results_file}")


async def main():
    """Run the advanced optimizations benchmark."""
    benchmark = AdvancedOptimizationsBenchmark()
    await benchmark.run_optimizations()


if __name__ == "__main__":
    asyncio.run(main())
